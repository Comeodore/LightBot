import os
import re
import asyncio
import logging
import signal
import json
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from typing import Optional
from enum import Enum
from dotenv import load_dotenv
import aiohttp
import asyncpg
from telegram.ext import Application, AIORateLimiter

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PowerStatus(Enum):
    OK = "OK"
    ALARM = "ALARM"


class DtekCellStatus(Enum):
    POWER_ON = "power_on"
    POWER_OFF = "power_off"
    POWER_OFF_FIRST_30 = "power_off_first_30"
    POWER_OFF_SECOND_30 = "power_off_second_30"


class DtekOutageStatus(Enum):
    POWER_ON = "power_on"
    SCHEDULED = "scheduled"
    EMERGENCY = "emergency"


KYIV_TZ: ZoneInfo = ZoneInfo('Europe/Kyiv')
MINUTES_IN_DAY: int = 1440
UNPLANNED_OUTAGE_THRESHOLD_MINUTES: int = 60


def format_restoration_time(restoration_time: str) -> str:
    match = re.search(r'(\d{1,2}:\d{2})\s+(\d{2}\.\d{2}\.\d{4})', restoration_time)
    if not match:
        return restoration_time

    time_str = match.group(1)
    date_str = match.group(2)

    try:
        restoration_date = datetime.strptime(date_str, "%d.%m.%Y").date()
    except ValueError:
        return restoration_time

    now_kyiv = datetime.now(KYIV_TZ)
    today_date = now_kyiv.date()
    tomorrow_date = today_date + timedelta(days=1)

    if restoration_date == today_date:
        return time_str
    elif restoration_date == tomorrow_date:
        return f"tomorrow {time_str}"
    else:
        return f"{time_str} {date_str}"


@dataclass
class DtekCurrentOutage:
    status: DtekOutageStatus
    reason: str = ""
    start_time: str = ""
    restoration_time: str = ""

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> Optional['DtekCurrentOutage']:
        if not data:
            return None
        return cls(
            status=DtekOutageStatus(data.get('status', 'power_on')),
            reason=data.get('reason', ''),
            start_time=data.get('start_time', ''),
            restoration_time=data.get('restoration_time', '')
        )

    @property
    def is_emergency(self) -> bool:
        return self.status == DtekOutageStatus.EMERGENCY

    @property
    def is_outage(self) -> bool:
        return self.status != DtekOutageStatus.POWER_ON


@dataclass
class DtekHourSlot:
    hour: int
    status: DtekCellStatus

    @property
    def has_outage(self) -> bool:
        return self.status != DtekCellStatus.POWER_ON

    @property
    def outage_minutes(self) -> tuple[int, int]:
        if self.status == DtekCellStatus.POWER_OFF:
            return (0, 60)
        elif self.status == DtekCellStatus.POWER_OFF_FIRST_30:
            return (0, 30)
        elif self.status == DtekCellStatus.POWER_OFF_SECOND_30:
            return (30, 60)
        return (0, 0)


@dataclass
class DtekDaySchedule:
    date: str
    slots: tuple[DtekHourSlot, ...]
    last_updated: str

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> Optional['DtekDaySchedule']:
        if not data or 'slots' not in data:
            return None
        return cls(
            date=data.get('date', ''),
            slots=tuple(
                DtekHourSlot(hour=s['hour'], status=DtekCellStatus(s['status']))
                for s in data['slots']
            ),
            last_updated=data.get('last_updated', '')
        )

    def get_outage_periods(self) -> list[tuple[int, int]]:
        if not self.slots:
            return []

        periods = []
        current_start = None
        current_end = None

        for slot in self.slots:
            if slot.has_outage:
                start_offset, end_offset = slot.outage_minutes
                slot_start = slot.hour * 60 + start_offset
                slot_end = slot.hour * 60 + end_offset

                if current_start is None:
                    current_start = slot_start
                    current_end = slot_end
                elif slot_start == current_end:
                    current_end = slot_end
                else:
                    periods.append((current_start, current_end))
                    current_start = slot_start
                    current_end = slot_end
            else:
                if current_start is not None:
                    periods.append((current_start, current_end))
                    current_start = None
                    current_end = None

        if current_start is not None:
            periods.append((current_start, current_end))

        return periods

    def is_outage_at_minute(self, minute: int) -> bool:
        for slot in self.slots:
            if not slot.has_outage:
                continue
            start_offset, end_offset = slot.outage_minutes
            slot_start = slot.hour * 60 + start_offset
            slot_end = slot.hour * 60 + end_offset
            if slot_start <= minute < slot_end:
                return True
        return False

    def get_current_outage_end(self, minute: int) -> Optional[int]:
        periods = self.get_outage_periods()
        for start, end in periods:
            if start <= minute < end:
                return end
        return None


@dataclass(frozen=True, slots=True)
class Config:
    ws_url: str
    ha_token: str
    bot_token: str
    chat_ids: tuple[str, ...]
    database_url: str
    voltage_entity: str
    battery_entity: str

    @classmethod
    def from_env(cls) -> 'Config':
        required_vars = ['WS_URL', 'HA_TOKEN', 'BOT_TOKEN', 'CHAT_IDS', 'DATABASE_URL']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

        return cls(
            ws_url=os.getenv('WS_URL'),
            ha_token=os.getenv('HA_TOKEN'),
            bot_token=os.getenv('BOT_TOKEN'),
            chat_ids=tuple(c.strip() for c in os.getenv('CHAT_IDS', '').split(',') if c.strip()),
            database_url=os.getenv('DATABASE_URL'),
            voltage_entity='sensor.victron_vebus_activein_l1_voltage_228',
            battery_entity='sensor.victron_battery_soc'
        )


@dataclass(slots=True)
class PowerState:
    status: PowerStatus
    timestamp: datetime
    battery_level: str = "N/A"

    @property
    def state(self) -> str:
        return self.status.value

    @state.setter
    def state(self, value: str) -> None:
        self.status = PowerStatus(value)

    def is_power_on(self) -> bool:
        return self.status == PowerStatus.OK


class HomeAssistantClient:
    AUTH_TIMEOUT = 10
    HEARTBEAT_INTERVAL = 30
    WS_CLOSE_TIMEOUT = 30

    def __init__(self, url: str, token: str):
        self.url = url
        self.token = token
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._message_id = 1

    async def connect(self) -> None:
        await self.close()

        self._message_id = 1

        timeout = aiohttp.ClientTimeout(total=60)
        connector = aiohttp.TCPConnector(limit=10, keepalive_timeout=30)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)

        try:
            self.ws = await self.session.ws_connect(
                self.url,
                heartbeat=self.HEARTBEAT_INTERVAL,
                timeout=aiohttp.ClientWSTimeout(ws_close=self.WS_CLOSE_TIMEOUT),
                compress=15
            )

            auth_msg = await asyncio.wait_for(self.ws.receive_json(), timeout=self.AUTH_TIMEOUT)
            if auth_msg.get('type') != 'auth_required':
                raise ConnectionError(f"Unexpected auth message: {auth_msg.get('type')}")

            await self.ws.send_json({
                "type": "auth",
                "access_token": self.token
            })

            auth_result = await asyncio.wait_for(self.ws.receive_json(), timeout=self.AUTH_TIMEOUT)
            if auth_result.get('type') != 'auth_ok':
                error = auth_result.get('message', 'Unknown error')
                raise ConnectionError(f"Authentication failed: {error}")

            logger.info("✅ Connected to Home Assistant")
        except asyncio.TimeoutError:
            await self.close()
            raise ConnectionError("Authentication timeout")
        except Exception as e:
            await self.close()
            raise ConnectionError(f"Failed to connect: {e}") from e

    async def subscribe_events(self) -> None:
        await self._send_message("subscribe_events", event_type="state_changed")
        logger.info("📻 Subscribed to state changes")

    async def get_states(self) -> dict:
        if not self.ws or self.ws.closed:
            raise ConnectionError("WebSocket not connected")

        msg_id = await self._send_message("get_states")

        async for msg in self._receive_with_timeout(timeout=10):
            if msg.get('id') == msg_id:
                if msg.get('success'):
                    return {
                        state['entity_id']: state
                        for state in msg.get('result', [])
                    }
                else:
                    error = msg.get('error', {}).get('message', 'Unknown error')
                    raise RuntimeError(f"Failed to get states: {error}")

        raise TimeoutError("Failed to get states within timeout")

    async def _send_message(self, msg_type: str, **kwargs) -> int:
        if not self.ws or self.ws.closed:
            raise ConnectionError("WebSocket not connected")

        message = {"id": self._message_id, "type": msg_type, **kwargs}
        await self.ws.send_json(message)
        msg_id = self._message_id
        self._message_id += 1
        return msg_id

    async def _receive_with_timeout(self, timeout: float):
        start = asyncio.get_event_loop().time()
        async for msg in self.receive():
            yield msg
            if asyncio.get_event_loop().time() - start > timeout:
                break

    async def receive(self):
        if not self.ws:
            return

        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                yield msg.json()
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    async def close(self) -> None:
        if self.ws and not self.ws.closed:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("⚠️ WebSocket close timeout")
            except Exception as e:
                logger.warning(f"⚠️ Error closing WebSocket: {e}")
            finally:
                self.ws = None

        if self.session and not self.session.closed:
            try:
                await self.session.close()
                await asyncio.sleep(0.25)
            except Exception as e:
                logger.warning(f"⚠️ Error closing session: {e}")
            finally:
                self.session = None

        logger.info("✅ Home Assistant client closed")


class Database:
    MIN_POOL_SIZE = 2
    MAX_POOL_SIZE = 10
    COMMAND_TIMEOUT = 10

    def __init__(self, url: str):
        self.url = url
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self) -> None:
        try:
            self.pool = await asyncpg.create_pool(
                self.url,
                min_size=self.MIN_POOL_SIZE,
                max_size=self.MAX_POOL_SIZE,
                command_timeout=self.COMMAND_TIMEOUT,
                server_settings={'application_name': 'LightBot'}
            )

            async with self.pool.acquire() as conn:
                await conn.execute('SELECT 1')

            logger.info("✅ Database connected")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {e}")
            raise

    async def get_last_event(self) -> Optional[PowerState]:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT state, timestamp "
                    "FROM power_events ORDER BY timestamp DESC LIMIT 1"
                )
                if row:
                    return PowerState(
                        status=PowerStatus(row['state']),
                        timestamp=row['timestamp']
                    )
                return None
        except Exception as e:
            logger.error(f"❌ Failed to get last event: {e}")
            raise

    async def save_event(self, state: PowerState) -> None:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE power_events SET state = $1, timestamp = $2 WHERE id = 1",
                    state.state, state.timestamp
                )
            logger.info(f"💾 Saved: {state.state} at {state.timestamp}")
        except Exception as e:
            logger.error(f"❌ Failed to save event: {e}")
            raise

    async def get_dtek_schedule(self) -> Optional[dict]:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT current_outage, today_schedule, tomorrow_schedule, updated_at "
                    "FROM dtek_schedule WHERE id = 1"
                )
                if row:
                    return {
                        'current_outage': self._parse_json(row['current_outage']),
                        'today': self._parse_json(row['today_schedule']),
                        'tomorrow': self._parse_json(row['tomorrow_schedule']),
                        'updated_at': row['updated_at']
                    }
                return None
        except Exception as e:
            logger.error(f"❌ Failed to get dtek schedule: {e}")
            raise

    def _parse_json(self, value) -> Optional[dict]:
        if value is None:
            return None
        if isinstance(value, str):
            if not value:
                return None
            try:
                return json.loads(value)
            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse JSON: {e}")
                return None
        return value

    async def close(self) -> None:
        if self.pool:
            try:
                await asyncio.wait_for(self.pool.close(), timeout=10)
                logger.info("💾 Database closed")
            except asyncio.TimeoutError:
                logger.warning("⚠️ Database close timeout")
            except Exception as e:
                logger.warning(f"⚠️ Error closing database: {e}")
            finally:
                self.pool = None


class NotificationService:
    MAX_RETRIES = 3
    RETRY_DELAY = 2

    def __init__(self, bot_token: str, chat_ids: tuple[str, ...]):
        self.chat_ids = chat_ids
        self.app = (
            Application.builder()
            .token(bot_token)
            .rate_limiter(AIORateLimiter(max_retries=3))
            .build()
        )

    async def start(self) -> None:
        try:
            await self.app.initialize()
            await self.app.start()
            logger.info("✅ Notification service started")
        except Exception as e:
            logger.error(f"❌ Failed to start notification service: {e}")
            raise

    async def unpin_all_messages(self) -> None:
        for chat_id in self.chat_ids:
            for attempt in range(self.MAX_RETRIES):
                try:
                    await self.app.bot.unpin_all_chat_messages(chat_id=chat_id)
                    logger.info(f"📍 All messages unpinned in {chat_id}")
                    break
                except Exception as e:
                    if attempt < self.MAX_RETRIES - 1:
                        logger.warning(f"⚠️ Failed to unpin messages in {chat_id} (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}")
                        await asyncio.sleep(self.RETRY_DELAY)
                    else:
                        logger.warning(f"⚠️ Failed to unpin messages in {chat_id} after {self.MAX_RETRIES} attempts: {e}")

    async def edit_pinned_message(self, message: str) -> None:
        for chat_id in self.chat_ids:
            try:
                chat = await self.app.bot.get_chat(chat_id)
                if not chat.pinned_message:
                    logger.warning(f"⚠️ No pinned message found in {chat_id}, skipping edit")
                    continue

                message_id = chat.pinned_message.message_id
                logger.info(f"📌 Found pinned message in {chat_id}: {message_id}")

                for attempt in range(self.MAX_RETRIES):
                    try:
                        await self.app.bot.edit_message_text(
                            chat_id=chat_id,
                            message_id=message_id,
                            text=message,
                            parse_mode='Markdown',
                            disable_web_page_preview=True
                        )
                        logger.info(f"✏️ Pinned message edited in {chat_id}")
                        break
                    except Exception as e:
                        if attempt < self.MAX_RETRIES - 1:
                            logger.warning(f"⚠️ Failed to edit message in {chat_id} (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}")
                            await asyncio.sleep(self.RETRY_DELAY)
                        else:
                            logger.error(f"❌ Failed to edit message in {chat_id} after {self.MAX_RETRIES} attempts: {e}")
            except Exception as e:
                logger.error(f"❌ Failed to get chat info for {chat_id}: {e}")

    async def send_reply_to_pinned(self, message: str) -> None:
        for chat_id in self.chat_ids:
            try:
                chat = await self.app.bot.get_chat(chat_id)
                if not chat.pinned_message:
                    logger.warning(f"⚠️ No pinned message found in {chat_id}, sending regular message")
                    await self._send_to_chat(chat_id, message)
                    continue

                message_id = chat.pinned_message.message_id

                for attempt in range(self.MAX_RETRIES):
                    try:
                        await self.app.bot.send_message(
                            chat_id=chat_id,
                            text=message,
                            parse_mode='Markdown',
                            disable_web_page_preview=True,
                            reply_to_message_id=message_id
                        )
                        logger.info(f"📤 Reply sent to pinned message in {chat_id}")
                        break
                    except Exception as e:
                        if attempt < self.MAX_RETRIES - 1:
                            logger.warning(f"⚠️ Failed to send reply in {chat_id} (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}")
                            await asyncio.sleep(self.RETRY_DELAY)
                        else:
                            logger.error(f"❌ Failed to send reply in {chat_id} after {self.MAX_RETRIES} attempts: {e}")
            except Exception as e:
                logger.error(f"❌ Failed to get chat info for {chat_id}: {e}")

    async def _send_to_chat(self, chat_id: str, message: str, pin: bool = False) -> bool:
        for attempt in range(self.MAX_RETRIES):
            try:
                sent_message = await self.app.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
                logger.info(f"📤 Message sent to {chat_id}")

                if pin:
                    try:
                        await self.app.bot.pin_chat_message(
                            chat_id=chat_id,
                            message_id=sent_message.message_id,
                            disable_notification=True
                        )
                        logger.info(f"📌 Message pinned in {chat_id}")
                    except Exception as pin_error:
                        logger.warning(f"⚠️ Failed to pin message in {chat_id}: {pin_error}")

                return True
            except Exception as e:
                if attempt < self.MAX_RETRIES - 1:
                    logger.warning(f"⚠️ Failed to send to {chat_id} (attempt {attempt + 1}/{self.MAX_RETRIES}): {e}")
                    await asyncio.sleep(self.RETRY_DELAY * (attempt + 1))
                else:
                    logger.error(f"❌ Failed to send to {chat_id} after {self.MAX_RETRIES} attempts: {e}")
                    return False
        return False

    async def send(self, message: str, pin: bool = False, unpin_all_first: bool = False) -> None:
        if unpin_all_first:
            await self.unpin_all_messages()

        for chat_id in self.chat_ids:
            await self._send_to_chat(chat_id, message, pin)

    async def stop(self) -> None:
        try:
            await self.app.stop()
            await self.app.shutdown()
            logger.info("✅ Notification service stopped")
        except Exception as e:
            logger.warning(f"⚠️ Error stopping notification service: {e}")


class DtekScheduleMonitor:
    CHECK_INTERVAL = 60
    INITIAL_DELAY = 5

    def __init__(self, db: Database, notifier: NotificationService, power_monitor: 'PowerMonitor'):
        self.db = db
        self.notifier = notifier
        self.power_monitor = power_monitor
        self._shutdown = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._saved_current: Optional[DtekCurrentOutage] = None
        self._saved_today: Optional[DtekDaySchedule] = None
        self._saved_tomorrow: Optional[DtekDaySchedule] = None

    async def start(self) -> None:
        await self._load_initial_state()
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info("📅 DTEK schedule monitor started")

    async def stop(self) -> None:
        self._shutdown.set()
        if self._task and not self._task.done():
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except asyncio.TimeoutError:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
        logger.info("✅ DTEK schedule monitor stopped")

    async def _load_initial_state(self) -> None:
        data = await self.db.get_dtek_schedule()
        if data:
            self._saved_current = DtekCurrentOutage.from_dict(data.get('current_outage'))
            self._saved_today = DtekDaySchedule.from_dict(data.get('today'))
            self._saved_tomorrow = DtekDaySchedule.from_dict(data.get('tomorrow'))
            logger.info("📊 Initial DTEK schedule loaded from DB")
        else:
            logger.info("📊 No DTEK schedule in DB yet")

    async def _monitor_loop(self) -> None:
        try:
            await asyncio.wait_for(self._shutdown.wait(), timeout=self.INITIAL_DELAY)
            return
        except asyncio.TimeoutError:
            pass

        while not self._shutdown.is_set():
            try:
                await self._check_schedule()
            except Exception as e:
                logger.error(f"❌ Error checking DTEK schedule: {e}")

            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=self.CHECK_INTERVAL)
            except asyncio.TimeoutError:
                pass

    async def _check_schedule(self) -> None:
        data = await self.db.get_dtek_schedule()
        if not data:
            return

        current_outage = DtekCurrentOutage.from_dict(data.get('current_outage'))
        today = DtekDaySchedule.from_dict(data.get('today'))
        tomorrow = DtekDaySchedule.from_dict(data.get('tomorrow'))

        if not today:
            return

        if self._saved_today is None:
            self._saved_current = current_outage
            self._saved_today = today
            self._saved_tomorrow = tomorrow
            logger.info("📅 Initial DTEK schedule state set")
            return

        now_kyiv = datetime.now(KYIV_TZ)
        current_minute = now_kyiv.hour * 60 + now_kyiv.minute

        outage_changed = await self._check_outage_changes(current_outage, self._saved_current, today)

        is_new_day = self._saved_today and today and self._saved_today.date != today.date

        saved_tomorrow_had_outages = self._has_outages(self._saved_tomorrow) if not is_new_day else False
        tomorrow_has_outages = self._has_outages(tomorrow)

        tomorrow_published = tomorrow_has_outages and not saved_tomorrow_had_outages
        tomorrow_changed = tomorrow_has_outages and saved_tomorrow_had_outages and self._slots_differ(tomorrow, self._saved_tomorrow)
        tomorrow_cancelled = saved_tomorrow_had_outages and not tomorrow_has_outages

        if tomorrow_published:
            await self._handle_tomorrow_update(tomorrow, now_kyiv, current_minute, is_new=True)
            await self._check_today_changes(today, now_kyiv, current_minute, tomorrow_has_outages=True)
        elif tomorrow_cancelled:
            is_new_day = self._saved_today and today and self._saved_today.date != today.date
            if is_new_day and self._saved_tomorrow and not self._slots_differ(today, self._saved_tomorrow):
                logger.debug("📅 Day shifted, tomorrow became today (no cancellation)")
            else:
                await self._handle_tomorrow_cancelled(now_kyiv)
            await self._update_pinned_schedule(today, now_kyiv, use_tomorrow=False)
            await self._check_today_changes(today, now_kyiv, current_minute, tomorrow_has_outages=False)
        elif tomorrow_changed:
            await self._update_pinned_schedule(tomorrow, now_kyiv, use_tomorrow=True)
            await self._send_schedule_change_notification(tomorrow, now_kyiv, is_tomorrow=True)
            await self._check_today_changes(today, now_kyiv, current_minute, tomorrow_has_outages=True)
        elif tomorrow_has_outages:
            await self._update_pinned_schedule(tomorrow, now_kyiv, use_tomorrow=True)
            await self._check_today_changes(today, now_kyiv, current_minute, tomorrow_has_outages=True)
        else:
            await self._update_pinned_schedule(today, now_kyiv, use_tomorrow=False)
            await self._check_today_changes(today, now_kyiv, current_minute, tomorrow_has_outages=False)

        self._saved_current = current_outage
        self._saved_today = today
        self._saved_tomorrow = tomorrow

    async def _check_outage_changes(
        self,
        current: Optional[DtekCurrentOutage],
        saved: Optional[DtekCurrentOutage],
        today_schedule: Optional[DtekDaySchedule]
    ) -> bool:
        was_emergency = saved.is_emergency if saved else False
        is_emergency = current.is_emergency if current else False

        if is_emergency and was_emergency:
            power_is_off = self.power_monitor.is_power_off()
            if power_is_off and current.restoration_time != saved.restoration_time:
                formatted_time = format_restoration_time(current.restoration_time)
                message = (
                    f"🔄 **Restoration time updated**\n\n"
                    f"New expected time: **{formatted_time}**"
                )
                await self.notifier.send(message)
                logger.info(f"🔄 Restoration time changed: {formatted_time}")
                return True

        return False

    async def _handle_tomorrow_cancelled(self, now_kyiv: datetime) -> None:
        date_str = (now_kyiv + timedelta(days=1)).strftime("%d.%m")
        message = (
            f"📅 **Tomorrow ({date_str})**\n\n"
            f"✅ Outages cancelled"
        )
        await self.notifier.send(message)
        logger.info("📅 Tomorrow outages cancelled")

    def _has_outages(self, schedule: Optional[DtekDaySchedule]) -> bool:
        if not schedule:
            return False
        return bool(schedule.get_outage_periods())

    def _slots_differ(self, new: Optional[DtekDaySchedule], old: Optional[DtekDaySchedule]) -> bool:
        if new is None and old is None:
            return False
        if new is None or old is None:
            return True

        if len(new.slots) != len(old.slots):
            return True

        for new_slot, old_slot in zip(new.slots, old.slots):
            if new_slot.hour != old_slot.hour or new_slot.status != old_slot.status:
                return True

        return False

    async def _update_pinned_schedule(
        self,
        schedule: DtekDaySchedule,
        now_kyiv: datetime,
        use_tomorrow: bool = False
    ) -> None:
        saved_schedule = self._saved_tomorrow if use_tomorrow else self._saved_today

        if not self._slots_differ(schedule, saved_schedule):
            return

        day_label = "tomorrow" if use_tomorrow else "today"
        date_str = (now_kyiv + timedelta(days=1)).strftime("%d.%m") if use_tomorrow else now_kyiv.strftime("%d.%m")

        outage_periods = schedule.get_outage_periods()

        if not outage_periods:
            schedule_message = (
                f"📅 **{date_str} schedule**\n\n"
                f"✅ No outages scheduled"
            )
        else:
            schedule_message = self._format_schedule_message(date_str, outage_periods)

        await self.notifier.edit_pinned_message(schedule_message)
        logger.info(f"📅 Updated pinned with {day_label} schedule ({date_str})")

    async def _check_today_changes(
        self,
        today: DtekDaySchedule,
        now_kyiv: datetime,
        current_minute: int,
        tomorrow_has_outages: bool = False
    ) -> None:
        if not self._slots_differ(today, self._saved_today):
            return

        is_new_day = self._saved_today and today and self._saved_today.date != today.date
        if is_new_day and self._saved_tomorrow and not self._slots_differ(today, self._saved_tomorrow):
            logger.debug("📅 Day shifted, but schedule unchanged (tomorrow became today)")
            return

        if tomorrow_has_outages:
            logger.info("📅 Today schedule changed while tomorrow is active")
        else:
            logger.info("📅 Today schedule changed")

        current_next_outage = self._find_next_outage_period(today, current_minute)
        saved_next_outage = self._find_next_outage_period(self._saved_today, current_minute) if self._saved_today else None

        if current_next_outage == saved_next_outage:
            return

        date_str = today.date

        if not current_next_outage:
            message = (
                f"📅 **Today ({date_str}) schedule updated**\n\n"
                f"✅ No more outages scheduled"
            )
        else:
            start, end = current_next_outage
            start_str = f"{start // 60:02d}:{start % 60:02d}"
            end_h, end_m = (0, 0) if end == MINUTES_IN_DAY else (end // 60, end % 60)
            end_str = f"{end_h:02d}:{end_m:02d}"

            message = f"📅 **Today ({date_str}) schedule updated**\n\nNext outage: **{start_str} - {end_str}**"

        if tomorrow_has_outages:
            await self.notifier.send(message)
        else:
            await self.notifier.send_reply_to_pinned(message)
        logger.info("📅 Sent today schedule change notification")

    def _format_schedule_message(self, date_str: str, outage_periods: list[tuple[int, int]]) -> str:
        total_outage_minutes = sum(end - start for start, end in outage_periods)
        total_power_minutes = MINUTES_IN_DAY - total_outage_minutes

        outage_lines = []
        for start, end in outage_periods:
            start_time = f"{start // 60:02d}:{start % 60:02d}"
            end_h, end_m = (0, 0) if end == MINUTES_IN_DAY else (end // 60, end % 60)
            end_time = f"{end_h:02d}:{end_m:02d}"
            outage_lines.append(f"{start_time} - {end_time}")

        outage_duration = self._format_duration_hours(total_outage_minutes)
        power_duration = self._format_duration_hours(total_power_minutes)

        return (
            f"📅 **{date_str} schedule**\n\n"
            + "\n".join(outage_lines)
            + f"\n\n⚡️ Power: **{power_duration}**\n"
            + f"🔴 Outages: **{outage_duration}**"
        )

    def _format_duration_hours(self, minutes: int) -> str:
        hours = minutes // 60
        mins = minutes % 60
        return f"{hours}h {mins}m" if mins else f"{hours}h"

    async def _send_schedule_change_notification(
        self,
        schedule: DtekDaySchedule,
        now_kyiv: datetime,
        is_tomorrow: bool = False
    ) -> None:
        day_label = "Tomorrow" if is_tomorrow else "Today"
        current_minute = now_kyiv.hour * 60 + now_kyiv.minute

        next_outage = self._find_next_outage_period(schedule, current_minute if not is_tomorrow else 0)

        if not next_outage:
            message = (
                f"📅 **{day_label} schedule updated**\n\n"
                f"✅ No more outages scheduled"
            )
            await self.notifier.send_reply_to_pinned(message)
            logger.info(f"📅 Sent {day_label.lower()} schedule change notification: no more outages")
            return

        start, end = next_outage
        start_str = f"{start // 60:02d}:{start % 60:02d}"
        end_h, end_m = (0, 0) if end == MINUTES_IN_DAY else (end // 60, end % 60)
        end_str = f"{end_h:02d}:{end_m:02d}"

        day_prefix = "tomorrow " if is_tomorrow else ""

        message = (
            f"📅 **{day_label} schedule updated**\n\n"
            f"Next outage: **{day_prefix}{start_str} - {end_str}**"
        )

        await self.notifier.send_reply_to_pinned(message)
        logger.info(f"📅 Sent {day_label.lower()} schedule change notification: {day_prefix}{start_str} - {end_str}")

    async def _handle_tomorrow_update(
        self,
        tomorrow: DtekDaySchedule,
        now_kyiv: datetime,
        current_minute: int,
        is_new: bool = False
    ) -> None:
        outage_periods = tomorrow.get_outage_periods()

        if not outage_periods:
            logger.info("📅 Tomorrow has no outage slots")
            return

        tomorrow_date = now_kyiv + timedelta(days=1)
        date_str = tomorrow_date.strftime("%d.%m")

        schedule_message = self._format_schedule_message(date_str, outage_periods)

        await self.notifier.send(schedule_message, pin=True, unpin_all_first=True)
        total_minutes = sum(end - start for start, end in outage_periods)
        logger.info(f"📅 Sent tomorrow schedule update with {len(outage_periods)} outage periods ({self._format_duration_hours(total_minutes)} total)")

        next_outage = self._find_next_outage_period(tomorrow, 0)
        if next_outage:
            start, end = next_outage
            start_str = f"{start // 60:02d}:{start % 60:02d}"
            end_h, end_m = (0, 0) if end == MINUTES_IN_DAY else (end // 60, end % 60)
            end_str = f"{end_h:02d}:{end_m:02d}"

            message = (
                f"📅 **Tomorrow schedule published**\n\n"
                f"Next outage: **tomorrow {start_str} - {end_str}**"
            )

            await self.notifier.send_reply_to_pinned(message)
            logger.info(f"📅 Sent tomorrow schedule notification: tomorrow {start_str} - {end_str}")

    def _find_next_outage_period(
        self,
        schedule: Optional[DtekDaySchedule],
        current_minute: int
    ) -> Optional[tuple[int, int]]:
        if not schedule:
            return None

        periods = schedule.get_outage_periods()
        for start, end in periods:
            if end > current_minute:
                return (start, end)
        return None

    def get_current_schedule(self) -> tuple[Optional[DtekCurrentOutage], Optional[DtekDaySchedule], Optional[DtekDaySchedule]]:
        return self._saved_current, self._saved_today, self._saved_tomorrow


class PowerMonitor:
    RECONNECT_DELAY = 3
    SHUTDOWN_TIMEOUT = 10

    def __init__(self, config: Config):
        self.config = config
        self.ha = HomeAssistantClient(config.ws_url, config.ha_token)
        self.db = Database(config.database_url)
        self.notifier = NotificationService(config.bot_token, config.chat_ids)
        self.dtek_monitor: Optional[DtekScheduleMonitor] = None
        self.current_state: Optional[PowerState] = None
        self._shutdown = asyncio.Event()
        self._cleanup_done = False

    def is_power_off(self) -> bool:
        return self.current_state is not None and not self.current_state.is_power_on()

    async def run(self) -> None:
        logger.info("🚀 Power Monitor started")
        logger.info(f"👥 Recipients: {len(self.config.chat_ids)}")

        try:
            await self.db.connect()
            await self.notifier.start()

            self.dtek_monitor = DtekScheduleMonitor(self.db, self.notifier, self)
            await self.dtek_monitor.start()

            await self._main_loop()
        except Exception as e:
            logger.error(f"❌ Fatal error in run: {e}")
            raise
        finally:
            await self._cleanup()

    async def _main_loop(self) -> None:
        while not self._shutdown.is_set():
            try:
                await self.ha.connect()
                await self._initialize_state()
                await self.ha.subscribe_events()

                close_code, close_reason = await self._event_loop()

                if self._shutdown.is_set():
                    break

                if close_code:
                    logger.warning(f"⚠️ WebSocket disconnected: code={close_code}, reason={close_reason}")
                else:
                    logger.warning("⚠️ WebSocket disconnected")

                logger.info(f"🔄 Reconnecting in {self.RECONNECT_DELAY} seconds...")
                try:
                    await asyncio.wait_for(self._shutdown.wait(), timeout=self.RECONNECT_DELAY)
                except asyncio.TimeoutError:
                    pass

            except Exception as e:
                logger.error(f"❌ Connection error: {e}")

                if not self._shutdown.is_set():
                    logger.info(f"🔄 Reconnecting in {self.RECONNECT_DELAY} seconds...")
                    try:
                        await asyncio.wait_for(self._shutdown.wait(), timeout=self.RECONNECT_DELAY)
                    except asyncio.TimeoutError:
                        pass

    async def _initialize_state(self) -> None:
        last_event = await self.db.get_last_event()

        if not last_event:
            raise ValueError("No events in database. Please initialize the database with a starting event.")

        states = await self.ha.get_states()
        battery_state = states.get(self.config.battery_entity)
        current_battery = self._parse_battery_level(battery_state)

        last_event.battery_level = current_battery
        self.current_state = last_event
        logger.info(f"📊 Initial state: {self.current_state.state}, Battery: {self.current_state.battery_level}")

    async def _event_loop(self) -> tuple[Optional[int], Optional[str]]:
        async for event in self.ha.receive():
            if self._shutdown.is_set():
                break
            try:
                await self._process_event(event)
            except Exception as e:
                logger.error(f"❌ Error processing event: {e}")

        if self.ha.ws and self.ha.ws.closed:
            code = self.ha.ws.close_code
            exception = self.ha.ws.exception()
            return code, str(exception) if exception else 'Connection closed'
        return None, None

    async def _process_event(self, event: dict) -> None:
        if event.get('type') != 'event':
            return

        if not self.current_state:
            logger.warning("⚠️ Current state not initialized, skipping event")
            return

        event_data = event.get('event', {}).get('data', {})
        entity_id = event_data.get('entity_id')

        if entity_id == self.config.battery_entity:
            new_state = event_data.get('new_state', {})
            battery_level = self._parse_battery_level(new_state)
            self.current_state.battery_level = battery_level
            return

        if entity_id != self.config.voltage_entity:
            return

        new_state_data = event_data.get('new_state', {})
        voltage_value = new_state_data.get('state', 'unknown')

        try:
            new_power_status = self._parse_power_state(new_state_data)
        except ValueError as e:
            logger.warning(f"⚠️ Invalid power state: {e}")
            return

        if self._is_valid_transition(self.current_state.status, new_power_status):
            logger.info(f"⚡ Voltage: {voltage_value}V -> {new_power_status.value} (DB state: {self.current_state.state})")
            new_power = PowerState(
                status=new_power_status,
                timestamp=datetime.now(timezone.utc),
                battery_level=self.current_state.battery_level
            )
            await self._handle_state_change(new_power)

    def _is_valid_transition(self, old_status: PowerStatus, new_status: PowerStatus) -> bool:
        return (
            (old_status == PowerStatus.OK and new_status == PowerStatus.ALARM) or
            (old_status == PowerStatus.ALARM and new_status == PowerStatus.OK)
        )

    async def _handle_state_change(self, new_state: PowerState) -> None:
        duration = self._format_duration(self.current_state.timestamp, new_state.timestamp)

        is_unplanned_outage = False
        if not new_state.is_power_on():
            is_unplanned_outage = self._check_if_unplanned_outage()

        next_event_info = self._get_next_event_info(new_state.is_power_on())

        if new_state.is_power_on():
            message = (
                f"🟢 **POWER RESTORED**\n\n"
                f"⏳ Outage duration: **{duration}**\n"
                f"🔋 Battery level: **{new_state.battery_level}**"
            )
            if next_event_info:
                message += f"\n\n{next_event_info}"
            logger.info(f"✅ Power restored after {duration}")
        else:
            message = (
                f"🔴 **POWER OUTAGE**\n\n"
                f"⚡️ Uptime: **{duration}**\n"
                f"🔋 Battery level: **{new_state.battery_level}**"
            )

            if is_unplanned_outage:
                message += "\n\n⚠️ **Unscheduled outage**"
                logger.info(f"⚠️ Power lost (UNSCHEDULED) after {duration}")
            else:
                if next_event_info:
                    message += f"\n\n{next_event_info}"
                logger.info(f"⚠️ Power lost after {duration}")

        self.current_state = new_state
        await self.notifier.send(message)

        try:
            await self.db.save_event(new_state)
        except Exception as e:
            logger.error(f"❌ Failed to save event to DB: {e}")

    def _check_if_unplanned_outage(self) -> bool:
        if not self.dtek_monitor:
            return False

        current_outage, today, tomorrow = self.dtek_monitor.get_current_schedule()

        if current_outage and current_outage.is_emergency:
            return False

        if not today:
            return False

        now_kyiv = datetime.now(KYIV_TZ)
        current_minute = now_kyiv.hour * 60 + now_kyiv.minute

        if today.is_outage_at_minute(current_minute):
            return False

        next_outage_minute = None
        periods = today.get_outage_periods()
        for start, end in periods:
            if start > current_minute:
                next_outage_minute = start
                break

        if next_outage_minute is None and tomorrow:
            tomorrow_periods = tomorrow.get_outage_periods()
            if tomorrow_periods:
                next_outage_minute = MINUTES_IN_DAY + tomorrow_periods[0][0]

        if next_outage_minute is None:
            return True

        minutes_until_outage = next_outage_minute - current_minute

        if minutes_until_outage > UNPLANNED_OUTAGE_THRESHOLD_MINUTES:
            logger.info(f"🔍 Unplanned outage detected: next scheduled outage in {minutes_until_outage} minutes")
            return True

        return False

    def _get_next_event_info(self, power_is_on: bool) -> Optional[str]:
        if not self.dtek_monitor:
            return None

        current_outage, today, tomorrow = self.dtek_monitor.get_current_schedule()

        if current_outage and current_outage.is_emergency:
            if power_is_on:
                return "🚨 Emergency shutdowns in effect"
            else:
                if current_outage.restoration_time:
                    formatted_time = format_restoration_time(current_outage.restoration_time)
                    return f"🚨 Next possible connection: **{formatted_time}**"
                return "🚨 Emergency shutdowns in effect"

        if not today:
            return None

        now_kyiv = datetime.now(KYIV_TZ)
        current_minute = now_kyiv.hour * 60 + now_kyiv.minute

        if power_is_on:
            periods = today.get_outage_periods()
            for start, end in periods:
                if start > current_minute:
                    start_h, start_m = divmod(start, 60)
                    end_h, end_m = divmod(end, 60)
                    if end == MINUTES_IN_DAY:
                        end_h, end_m = 0, 0
                    return f"📅 Next outage: **{start_h:02d}:{start_m:02d}-{end_h:02d}:{end_m:02d}**"

            if tomorrow:
                tomorrow_periods = tomorrow.get_outage_periods()
                if tomorrow_periods:
                    start, end = tomorrow_periods[0]
                    start_h, start_m = divmod(start, 60)
                    end_h, end_m = divmod(end, 60)
                    return f"📅 Next outage: tomorrow **{start_h:02d}:{start_m:02d}-{end_h:02d}:{end_m:02d}**"

            return "✅ No more outages scheduled for today"
        else:
            outage_end = today.get_current_outage_end(current_minute)
            if outage_end:
                if outage_end == MINUTES_IN_DAY:
                    if tomorrow:
                        tomorrow_periods = tomorrow.get_outage_periods()
                        if tomorrow_periods and tomorrow_periods[0][0] == 0:
                            end = tomorrow_periods[0][1]
                            end_h, end_m = divmod(end, 60)
                            return f"📅 Next connection: tomorrow **{end_h:02d}:{end_m:02d}**"
                    return "📅 Next connection: **00:00**"
                else:
                    end_h, end_m = divmod(outage_end, 60)
                    return f"📅 Next connection: **{end_h:02d}:{end_m:02d}**"

            return None

    def _parse_power_state(self, state: dict) -> PowerStatus:
        try:
            voltage_value = state.get('state', 0)
            if voltage_value in ('unavailable', 'unknown', None):
                raise ValueError(f"Voltage state unavailable: {voltage_value}")
            voltage = float(voltage_value)
            return PowerStatus.OK if voltage > 0 else PowerStatus.ALARM
        except (ValueError, TypeError) as e:
            raise ValueError(f"Failed to parse voltage: {e}") from e

    def _parse_battery_level(self, state: Optional[dict]) -> str:
        if not state:
            return "N/A"
        try:
            battery_value = state.get('state')
            if battery_value is None or battery_value in ('unavailable', 'unknown'):
                return "N/A"
            return f"{round(float(battery_value))}%"
        except (ValueError, TypeError):
            return "N/A"

    def _format_duration(self, start: datetime, end: datetime) -> str:
        seconds = int((end - start).total_seconds())
        days, remainder = divmod(seconds, 86400)
        hours, remainder = divmod(remainder, 3600)
        minutes, _ = divmod(remainder, 60)

        parts = []
        if days > 0:
            parts.append(f"{days}d")
        if hours > 0:
            parts.append(f"{hours}h")
        if minutes > 0:
            parts.append(f"{minutes}m")

        return " ".join(parts) if parts else "less than a minute"

    async def _cleanup(self) -> None:
        if self._cleanup_done:
            return

        self._cleanup_done = True
        logger.info("🧹 Cleaning up...")

        cleanup_tasks = []

        if self.dtek_monitor:
            cleanup_tasks.append(('DTEK monitor', self.dtek_monitor.stop()))

        cleanup_tasks.extend([
            ('Home Assistant', self.ha.close()),
            ('Database', self.db.close()),
            ('Notifier', self.notifier.stop())
        ])

        for name, task in cleanup_tasks:
            try:
                await asyncio.wait_for(task, timeout=self.SHUTDOWN_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(f"⚠️ {name} cleanup timeout")
            except Exception as e:
                logger.warning(f"⚠️ Error cleaning up {name}: {e}")

        logger.info("✅ Cleanup complete")

    def shutdown(self) -> None:
        if not self._shutdown.is_set():
            logger.info("🛑 Shutdown requested")
            self._shutdown.set()


async def main() -> None:
    try:
        config = Config.from_env()
    except ValueError as e:
        logger.error(f"❌ Configuration error: {e}")
        return

    monitor = PowerMonitor(config)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, monitor.shutdown)

    try:
        await monitor.run()
    except KeyboardInterrupt:
        logger.info("👋 Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
    finally:
        logger.info("👋 Shutting down")


if __name__ == "__main__":
    asyncio.run(main())