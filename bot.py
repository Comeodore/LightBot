import os
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


class ScheduleStatus(Enum):
    SCHEDULE_APPLIES = "ScheduleApplies"
    EMERGENCY_SHUTDOWNS = "EmergencyShutdowns"
    WAITING_FOR_SCHEDULE = "WaitingForSchedule"


class SlotType(Enum):
    DEFINITE = "Definite"
    POSSIBLE = "Possible"


YASNO_REGION: str = '25'
YASNO_DSO: str = '902'
YASNO_GROUP: str = '2.2'
KYIV_TZ: ZoneInfo = ZoneInfo('Europe/Kyiv')
MINUTES_IN_DAY: int = 1440
UNPLANNED_OUTAGE_THRESHOLD_MINUTES: int = 60


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


@dataclass(frozen=True, slots=True)
class TimeSlot:
    start: int
    end: int
    type: str

    def is_power_off(self) -> bool:
        return self.type == SlotType.DEFINITE.value

    def contains_minute(self, minute: int) -> bool:
        return self.start <= minute < self.end


@dataclass(frozen=True, slots=True)
class DaySchedule:
    date: str
    slots: tuple[TimeSlot, ...]
    status: str

    def get_slot_at_minute(self, minute: int) -> Optional[TimeSlot]:
        for slot in self.slots:
            if slot.contains_minute(minute):
                return slot
        return None

    def get_next_transition(self, current_minute: int) -> Optional[tuple[int, bool]]:
        for i, slot in enumerate(self.slots):
            if slot.end > current_minute:
                if slot.start > current_minute:
                    return (slot.start, slot.is_power_off())
                if slot.end < MINUTES_IN_DAY and i + 1 < len(self.slots):
                    next_slot = self.slots[i + 1]
                    return (slot.end, next_slot.is_power_off())
        return None

    @classmethod
    def from_dict(cls, data: dict) -> Optional['DaySchedule']:
        if not data or not data.get('slots'):
            return None
        try:
            return cls(
                date=data.get('date', ''),
                slots=tuple(TimeSlot(**slot) for slot in data['slots']),
                status=data.get('status', '')
            )
        except (TypeError, KeyError) as e:
            logger.warning(f"⚠️ Failed to parse schedule: {e}")
            return None


@dataclass(frozen=True, slots=True)
class YasnoSchedule:
    today: DaySchedule
    tomorrow: Optional[DaySchedule]
    updated_on: datetime
    group: str


class YasnoAPIClient:
    BASE_URL = "https://app.yasno.ua/api/blackout-service/public/shutdowns"
    MAX_RETRIES = 3
    RETRY_DELAY = 2
    REQUEST_TIMEOUT = 10

    def __init__(self, region: str, dso: str, group: str):
        self.region = region
        self.dso = dso
        self.group = group
        self.session: Optional[aiohttp.ClientSession] = None

    async def connect(self) -> None:
        if self.session and not self.session.closed:
            return
        if self.session:
            await self.close()

        timeout = aiohttp.ClientTimeout(total=self.REQUEST_TIMEOUT)
        connector = aiohttp.TCPConnector(limit=10, limit_per_host=5, ttl_dns_cache=300)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def fetch_schedule(self) -> Optional[YasnoSchedule]:
        await self.connect()

        url = f"{self.BASE_URL}/regions/{self.region}/dsos/{self.dso}/planned-outages"

        for attempt in range(self.MAX_RETRIES):
            try:
                async with self.session.get(url) as response:
                    if response.status != 200:
                        logger.error(f"❌ Yasno API error: {response.status}")
                        if attempt < self.MAX_RETRIES - 1:
                            await asyncio.sleep(self.RETRY_DELAY * (2 ** attempt))
                            continue
                        return None

                    data = await response.json()

                    if self.group not in data:
                        logger.error(f"❌ Group {self.group} not found in response")
                        return None

                    group_data = data[self.group]
                    updated_on_str = group_data.get('updatedOn')

                    if not updated_on_str:
                        logger.error("❌ Missing updatedOn in response")
                        return None

                    today = DaySchedule.from_dict(group_data.get('today', {}))
                    if not today:
                        logger.error("❌ Failed to parse today schedule")
                        return None

                    tomorrow = DaySchedule.from_dict(group_data.get('tomorrow', {}))

                    try:
                        updated_on = datetime.fromisoformat(updated_on_str.replace('Z', '+00:00'))
                    except (ValueError, AttributeError) as e:
                        logger.error(f"❌ Failed to parse updated_on timestamp: {e}")
                        return None

                    return YasnoSchedule(
                        today=today,
                        tomorrow=tomorrow,
                        updated_on=updated_on,
                        group=self.group
                    )

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.warning(f"⚠️ Yasno API attempt {attempt + 1}/{self.MAX_RETRIES} failed: {e}")
                if attempt < self.MAX_RETRIES - 1:
                    await asyncio.sleep(self.RETRY_DELAY * (2 ** attempt))
                else:
                    logger.error(f"❌ Failed to fetch Yasno schedule after {self.MAX_RETRIES} attempts")
                    return None
            except Exception as e:
                logger.error(f"❌ Unexpected error fetching Yasno schedule: {e}")
                return None

        return None

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()
            await asyncio.sleep(0.25)
            logger.info("✅ Yasno API client closed")


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
                    "INSERT INTO power_events (state, timestamp) VALUES ($1, $2)",
                    state.state, state.timestamp
                )
            logger.info(f"💾 Saved: {state.state} at {state.timestamp}")
        except Exception as e:
            logger.error(f"❌ Failed to save event: {e}")
            raise

    async def get_yasno_schedule(self) -> Optional[dict]:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT updated_on, schedule_data "
                    "FROM yasno_schedule WHERE id = 1"
                )
                if row:
                    schedule_data = row['schedule_data']
                    if isinstance(schedule_data, str):
                        try:
                            schedule_data = json.loads(schedule_data)
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ Failed to parse schedule JSON: {e}")
                            return None
                    return {
                        'updated_on': row['updated_on'],
                        'schedule_data': schedule_data
                    }
                return None
        except Exception as e:
            logger.error(f"❌ Failed to get yasno schedule: {e}")
            raise

    async def save_yasno_schedule(
        self,
        updated_on: datetime,
        schedule_data: dict
    ) -> None:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        try:
            schedule_json = json.dumps(schedule_data)
        except (TypeError, ValueError) as e:
            logger.error(f"❌ Failed to serialize schedule data: {e}")
            raise

        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE yasno_schedule
                    SET updated_on = $1,
                        schedule_data = $2
                    WHERE id = 1
                    """,
                    updated_on, schedule_json
                )
                if result == "UPDATE 0":
                    logger.warning("⚠️ No rows updated in yasno_schedule table")
        except Exception as e:
            logger.error(f"❌ Failed to save yasno schedule: {e}")
            raise

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


class YasnoScheduleMonitor:
    CHECK_INTERVAL = 60
    INITIAL_DELAY = 5

    def __init__(
        self,
        yasno_client: YasnoAPIClient,
        db: Database,
        notifier: NotificationService,
        power_monitor: 'PowerMonitor'
    ):
        self.yasno = yasno_client
        self.db = db
        self.notifier = notifier
        self.power_monitor = power_monitor
        self._shutdown = asyncio.Event()
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info("📅 Yasno schedule monitor started")

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
        logger.info("✅ Yasno schedule monitor stopped")

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
                logger.error(f"❌ Error checking Yasno schedule: {e}")

            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=self.CHECK_INTERVAL)
            except asyncio.TimeoutError:
                pass

    async def _check_schedule(self) -> None:
        schedule = await self.yasno.fetch_schedule()
        if not schedule:
            return

        try:
            now_kyiv = datetime.now(KYIV_TZ)
            current_minute = now_kyiv.hour * 60 + now_kyiv.minute

            saved_schedule = await self.db.get_yasno_schedule()

            if saved_schedule:
                saved_updated_on = saved_schedule['updated_on']

                if schedule.updated_on > saved_updated_on:
                    await self._handle_schedule_update(
                        schedule,
                        saved_schedule,
                        now_kyiv,
                        current_minute
                    )
        except Exception as e:
            logger.error(f"❌ Error processing schedule: {e}")
            raise

        await self._save_schedule(schedule)

    async def _handle_schedule_update(
        self,
        schedule: YasnoSchedule,
        saved_schedule: dict,
        now_kyiv: datetime,
        current_minute: int
    ) -> None:
        try:
            logger.info(f"📅 Schedule updated: {saved_schedule['updated_on']} -> {schedule.updated_on}")

            schedule_data = saved_schedule.get('schedule_data')
            saved_today_status = schedule_data.get('today', {}).get('status', '') if schedule_data else ''

            if schedule.today.status == ScheduleStatus.EMERGENCY_SHUTDOWNS.value:
                if saved_today_status != ScheduleStatus.EMERGENCY_SHUTDOWNS.value:
                    message = (
                        f"🚨 **Emergency shutdowns**\n\n"
                        f"Scheduled outages are cancelled"
                    )
                    await self.notifier.send(message)
                    logger.info("🚨 Sent emergency shutdowns notification")
                else:
                    logger.info("🚨 Emergency shutdowns still in effect, skipping notification")
                return

            if (saved_today_status == ScheduleStatus.EMERGENCY_SHUTDOWNS.value and
                schedule.today.status != ScheduleStatus.EMERGENCY_SHUTDOWNS.value):
                message = (
                    f"✅ **Emergency shutdowns ended**\n\n"
                    f"Scheduled outages are active now"
                )
                await self.notifier.send(message)
                logger.info("✅ Sent emergency shutdowns ended notification")

            tomorrow_published = (
                schedule.tomorrow and
                schedule.tomorrow.status == ScheduleStatus.SCHEDULE_APPLIES.value and
                schedule_data and
                schedule_data.get('tomorrow', {}).get('status') != ScheduleStatus.SCHEDULE_APPLIES.value
            )

            tomorrow_is_active = (
                schedule.tomorrow and
                schedule.tomorrow.status == ScheduleStatus.SCHEDULE_APPLIES.value
            )

            if tomorrow_published:
                await self._handle_tomorrow_update(schedule, saved_schedule, now_kyiv, current_minute)
            else:
                await self._update_pinned_schedule(schedule, saved_schedule, now_kyiv, current_minute, use_tomorrow=tomorrow_is_active)
        except Exception as e:
            logger.error(f"❌ Error handling schedule update: {e}")

    async def _update_pinned_schedule(
        self,
        schedule: YasnoSchedule,
        saved_schedule: dict,
        now_kyiv: datetime,
        current_minute: int,
        use_tomorrow: bool = False
    ) -> None:
        try:
            day_schedule = schedule.tomorrow if use_tomorrow else schedule.today
            day_label = "tomorrow" if use_tomorrow else "today"

            if not day_schedule or not day_schedule.slots:
                logger.info(f"📅 {day_label.capitalize()} schedule update but no slots available")
                return

            if not use_tomorrow and schedule.today.status == ScheduleStatus.EMERGENCY_SHUTDOWNS.value:
                logger.info("📅 Today schedule updated but emergency shutdowns in effect - not updating pinned message")
                return

            schedule_data = saved_schedule.get('schedule_data') if saved_schedule else None
            saved_day_key = 'tomorrow' if use_tomorrow else 'today'
            saved_day_data = schedule_data.get(saved_day_key, {}) if schedule_data else {}
            saved_slots = saved_day_data.get('slots', [])

            current_slots = [
                {'start': s.start, 'end': s.end, 'type': s.type}
                for s in day_schedule.slots
            ]

            slots_changed = saved_slots != current_slots

            if not slots_changed:
                logger.debug(f"📅 {day_label.capitalize()} slots unchanged, skipping pinned message update")
                return

            outage_slots = [slot for slot in day_schedule.slots if slot.is_power_off()]

            date_str = (now_kyiv + timedelta(days=1)).strftime("%d.%m") if use_tomorrow else now_kyiv.strftime("%d.%m")

            if not outage_slots:
                schedule_message = (
                    f"📅 **{date_str} schedule**\n\n"
                    f"✅ No outages scheduled"
                )
            else:
                schedule_message = self._format_schedule_message(date_str, outage_slots)

            await self.notifier.edit_pinned_message(schedule_message)
            logger.info(f"📅 Updated pinned with {day_label} schedule ({date_str})")

            current_next_outage = self._find_next_outage_slot(schedule, now_kyiv, current_minute)
            saved_next_outage = self._get_saved_next_outage_slot(saved_schedule, now_kyiv, current_minute)

            next_slot_changed = self._outage_slots_differ(current_next_outage, saved_next_outage)

            saved_outage_minutes = self._calc_outage_minutes(saved_slots)
            current_outage_minutes = sum(s.end - s.start for s in outage_slots)

            await self._send_schedule_change_notification(
                schedule, now_kyiv, current_minute,
                is_tomorrow=use_tomorrow,
                next_slot_changed=next_slot_changed,
                old_outage_minutes=saved_outage_minutes,
                new_outage_minutes=current_outage_minutes
            )
        except Exception as e:
            logger.error(f"❌ Error updating pinned schedule: {e}")

    def _format_schedule_message(self, date_str: str, outage_slots: list[TimeSlot]) -> str:
        total_outage_minutes = sum(slot.end - slot.start for slot in outage_slots)
        total_power_minutes = MINUTES_IN_DAY - total_outage_minutes

        outage_lines = []
        for slot in outage_slots:
            start_time = f"{slot.start // 60:02d}:{slot.start % 60:02d}"
            end_h, end_m = (0, 0) if slot.end == MINUTES_IN_DAY else (slot.end // 60, slot.end % 60)
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

    def _calc_outage_minutes(self, slots: list) -> int:
        total = 0
        for slot in slots:
            if slot.get('type') == SlotType.DEFINITE.value:
                total += slot.get('end', 0) - slot.get('start', 0)
        return total

    def _format_duration_hours(self, minutes: int) -> str:
        hours = minutes // 60
        mins = minutes % 60
        return f"{hours}h {mins}m" if mins else f"{hours}h"

    async def _send_schedule_change_notification(
        self,
        schedule: YasnoSchedule,
        now_kyiv: datetime,
        current_minute: int,
        is_tomorrow: bool = False,
        next_slot_changed: bool = True,
        old_outage_minutes: int = 0,
        new_outage_minutes: int = 0
    ) -> None:
        try:
            day_label = "Tomorrow" if is_tomorrow else "Today"

            if next_slot_changed:
                next_outage = self._find_next_outage_slot(schedule, now_kyiv, current_minute)

                if not next_outage:
                    message = (
                        f"📅 **{day_label} schedule updated**\n\n"
                        f"✅ No more outages scheduled"
                    )
                    await self.notifier.send_reply_to_pinned(message)
                    logger.info(f"📅 Sent {day_label.lower()} schedule change notification: no more outages")
                    return

                start_time, end_time, _ = next_outage
                start_str = start_time.strftime("%H:%M")
                end_str = end_time.strftime("%H:%M")

                is_tomorrow_slot = start_time.date() > now_kyiv.date()
                day_prefix = "tomorrow " if is_tomorrow_slot else ""

                message = (
                    f"📅 **{day_label} schedule updated**\n\n"
                    f"Next outage: **{day_prefix}{start_str} - {end_str}**"
                )

                await self.notifier.send_reply_to_pinned(message)
                logger.info(f"📅 Sent {day_label.lower()} schedule change notification: {day_prefix}{start_str} - {end_str}")
            else:
                old_duration = self._format_duration_hours(old_outage_minutes)
                new_duration = self._format_duration_hours(new_outage_minutes)

                message = (
                    f"📅 **{day_label} schedule updated**\n\n"
                    f"Next outage slot unchanged\n"
                    f"Outage duration: **{old_duration}** → **{new_duration}**"
                )

                await self.notifier.send_reply_to_pinned(message)
                logger.info(f"📅 Sent {day_label.lower()} schedule change notification: {old_duration} -> {new_duration}")
        except Exception as e:
            logger.error(f"❌ Error sending schedule change notification: {e}")

    async def _handle_tomorrow_update(
        self,
        schedule: YasnoSchedule,
        saved_schedule: dict,
        now_kyiv: datetime,
        current_minute: int
    ) -> None:
        if not schedule.tomorrow or not schedule.tomorrow.slots:
            logger.warning("⚠️ Tomorrow schedule update called but no tomorrow data available")
            return

        outage_slots = [slot for slot in schedule.tomorrow.slots if slot.is_power_off()]

        if not outage_slots:
            logger.info("📅 Tomorrow has no outage slots")
            return

        tomorrow_date = now_kyiv + timedelta(days=1)
        date_str = tomorrow_date.strftime("%d.%m")

        schedule_message = self._format_schedule_message(date_str, outage_slots)

        await self.notifier.send(schedule_message, pin=True, unpin_all_first=True)
        total_minutes = sum(s.end - s.start for s in outage_slots)
        logger.info(f"📅 Sent tomorrow schedule update with {len(outage_slots)} outage slots ({self._format_duration_hours(total_minutes)} total)")

        await self._send_tomorrow_schedule_notification(schedule, saved_schedule, now_kyiv, current_minute)

    async def _send_tomorrow_schedule_notification(
        self,
        schedule: YasnoSchedule,
        saved_schedule: dict,
        now_kyiv: datetime,
        current_minute: int
    ) -> None:
        try:
            next_outage_slot = self._find_next_outage_slot(schedule, now_kyiv, current_minute)

            schedule_data = saved_schedule.get('schedule_data') if saved_schedule else None
            saved_next_outage_slot = None

            if schedule_data:
                saved_schedule_obj = self._reconstruct_schedule(schedule_data, saved_schedule.get('updated_on'))
                if saved_schedule_obj:
                    saved_next_outage_slot = self._find_next_outage_slot(saved_schedule_obj, now_kyiv, current_minute)

            if next_outage_slot == saved_next_outage_slot:
                logger.info("📅 Next outage slot unchanged, skipping notification")
                return

            if not next_outage_slot:
                message = (
                    f"📅 **Tomorrow schedule updated**\n\n"
                    f"✅ No more outages scheduled"
                )
                await self.notifier.send_reply_to_pinned(message)
                logger.info("📅 Sent tomorrow schedule notification: no outages")
                return

            start_time, end_time, _ = next_outage_slot
            start_str = start_time.strftime("%H:%M")
            end_str = end_time.strftime("%H:%M")

            is_tomorrow = start_time.date() > now_kyiv.date()
            day_prefix = "tomorrow " if is_tomorrow else ""

            message = (
                f"📅 **Tomorrow schedule updated**\n\n"
                f"Next outage: **{day_prefix}{start_str} - {end_str}**"
            )

            await self.notifier.send_reply_to_pinned(message)
            logger.info(f"📅 Sent tomorrow schedule notification: {day_prefix}{start_str} - {end_str}")
        except Exception as e:
            logger.error(f"❌ Error sending tomorrow schedule notification: {e}")

    def _reconstruct_schedule(self, schedule_data: dict, updated_on: Optional[datetime] = None) -> Optional[YasnoSchedule]:
        today = DaySchedule.from_dict(schedule_data.get('today', {}))
        if not today:
            return None

        tomorrow = DaySchedule.from_dict(schedule_data.get('tomorrow', {}))

        return YasnoSchedule(
            today=today,
            tomorrow=tomorrow,
            updated_on=updated_on or datetime.now(timezone.utc),
            group=YASNO_GROUP
        )

    def _find_next_outage_slot(
        self,
        schedule: YasnoSchedule,
        now_kyiv: datetime,
        current_minute: int
    ) -> Optional[tuple[datetime, datetime, bool]]:
        today_outage = None
        tomorrow_outage = None

        for slot in schedule.today.slots:
            if slot.is_power_off() and slot.end > current_minute:
                today_outage = slot
                break

        if schedule.tomorrow and schedule.tomorrow.status == ScheduleStatus.SCHEDULE_APPLIES.value:
            for slot in schedule.tomorrow.slots:
                if slot.is_power_off():
                    tomorrow_outage = slot
                    break

        if today_outage and tomorrow_outage:
            if today_outage.end == MINUTES_IN_DAY and tomorrow_outage.start == 0:
                start_time = now_kyiv.replace(
                    hour=today_outage.start // 60,
                    minute=today_outage.start % 60,
                    second=0,
                    microsecond=0
                )
                tomorrow_date = now_kyiv + timedelta(days=1)
                end_time = tomorrow_date.replace(
                    hour=tomorrow_outage.end // 60,
                    minute=tomorrow_outage.end % 60,
                    second=0,
                    microsecond=0
                )
                return (start_time, end_time, True)

        if today_outage:
            start_time = now_kyiv.replace(
                hour=today_outage.start // 60,
                minute=today_outage.start % 60,
                second=0,
                microsecond=0
            )
            if today_outage.end == MINUTES_IN_DAY:
                end_time = (now_kyiv + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                end_time = now_kyiv.replace(
                    hour=today_outage.end // 60,
                    minute=today_outage.end % 60,
                    second=0,
                    microsecond=0
                )
            return (start_time, end_time, False)

        if tomorrow_outage:
            tomorrow_date = now_kyiv + timedelta(days=1)
            start_time = tomorrow_date.replace(
                hour=tomorrow_outage.start // 60,
                minute=tomorrow_outage.start % 60,
                second=0,
                microsecond=0
            )
            if tomorrow_outage.end == MINUTES_IN_DAY:
                end_time = (tomorrow_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            else:
                end_time = tomorrow_date.replace(
                    hour=tomorrow_outage.end // 60,
                    minute=tomorrow_outage.end % 60,
                    second=0,
                    microsecond=0
                )
            return (start_time, end_time, False)

        return None

    def _get_saved_next_outage_slot(
        self,
        saved_schedule: dict,
        now_kyiv: datetime,
        current_minute: int
    ) -> Optional[tuple[datetime, datetime, bool]]:
        if not saved_schedule:
            return None

        schedule_data = saved_schedule.get('schedule_data')
        if not schedule_data:
            return None

        saved_schedule_obj = self._reconstruct_schedule(
            schedule_data,
            saved_schedule.get('updated_on', datetime.now(timezone.utc))
        )
        if not saved_schedule_obj:
            return None

        return self._find_next_outage_slot(saved_schedule_obj, now_kyiv, current_minute)

    def _outage_slots_differ(
        self,
        slot1: Optional[tuple[datetime, datetime, bool]],
        slot2: Optional[tuple[datetime, datetime, bool]]
    ) -> bool:
        if slot1 is None and slot2 is None:
            return False
        if slot1 is None or slot2 is None:
            return True

        start1, end1, _ = slot1
        start2, end2, _ = slot2

        return start1 != start2 or end1 != end2

    async def _save_schedule(self, schedule: YasnoSchedule) -> None:
        schedule_data = {
            'today': {
                'date': schedule.today.date,
                'status': schedule.today.status,
                'slots': [{'start': s.start, 'end': s.end, 'type': s.type} for s in schedule.today.slots]
            }
        }

        if schedule.tomorrow:
            schedule_data['tomorrow'] = {
                'date': schedule.tomorrow.date,
                'status': schedule.tomorrow.status,
                'slots': [{'start': s.start, 'end': s.end, 'type': s.type} for s in schedule.tomorrow.slots]
            }

        await self.db.save_yasno_schedule(
            schedule.updated_on,
            schedule_data
        )

    def _find_next_event(
        self,
        schedule: YasnoSchedule,
        now_kyiv: datetime,
        current_minute: int
    ) -> Optional[tuple[datetime, bool]]:
        next_transition = schedule.today.get_next_transition(current_minute)

        if next_transition:
            minute, is_outage = next_transition
            next_time = now_kyiv.replace(
                hour=minute // 60,
                minute=minute % 60,
                second=0,
                microsecond=0
            )
            return (next_time, is_outage)

        current_slot = schedule.today.get_slot_at_minute(current_minute)
        if not current_slot:
            return None

        if (schedule.tomorrow and
            schedule.tomorrow.status == ScheduleStatus.SCHEDULE_APPLIES.value and
            schedule.tomorrow.slots):

            first_tomorrow_slot = schedule.tomorrow.slots[0]

            if first_tomorrow_slot.start == 0:
                tomorrow_date = now_kyiv + timedelta(days=1)

                if current_slot.type == first_tomorrow_slot.type:
                    next_time = tomorrow_date.replace(
                        hour=first_tomorrow_slot.end // 60,
                        minute=first_tomorrow_slot.end % 60,
                        second=0,
                        microsecond=0
                    )
                    if len(schedule.tomorrow.slots) > 1:
                        next_slot = schedule.tomorrow.slots[1]
                        return (next_time, next_slot.is_power_off())
                    return None
                else:
                    next_time = tomorrow_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    return (next_time, first_tomorrow_slot.is_power_off())

        return None


class PowerMonitor:
    RECONNECT_DELAY = 3
    SHUTDOWN_TIMEOUT = 10

    def __init__(self, config: Config):
        self.config = config
        self.ha = HomeAssistantClient(config.ws_url, config.ha_token)
        self.db = Database(config.database_url)
        self.yasno = YasnoAPIClient(YASNO_REGION, YASNO_DSO, YASNO_GROUP)
        self.notifier = NotificationService(config.bot_token, config.chat_ids)
        self.yasno_monitor: Optional[YasnoScheduleMonitor] = None
        self.current_state: Optional[PowerState] = None
        self._shutdown = asyncio.Event()
        self._cleanup_done = False

    async def run(self) -> None:
        logger.info("🚀 Power Monitor started")
        logger.info(f"👥 Recipients: {len(self.config.chat_ids)}")

        try:
            await self.db.connect()
            await self.notifier.start()

            self.yasno_monitor = YasnoScheduleMonitor(
                self.yasno,
                self.db,
                self.notifier,
                self
            )
            await self.yasno_monitor.start()

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
            is_unplanned_outage = await self._check_if_unplanned_outage()

        next_event_info = await self._get_next_event_info(new_state.is_power_on())

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

    async def _check_if_unplanned_outage(self) -> bool:
        try:
            schedule = await self.yasno.fetch_schedule()
            if not schedule:
                return False

            if schedule.today.status == ScheduleStatus.EMERGENCY_SHUTDOWNS.value:
                return False

            if not schedule.today.slots:
                return False

            now_kyiv = datetime.now(KYIV_TZ)
            current_minute = now_kyiv.hour * 60 + now_kyiv.minute

            current_slot = schedule.today.get_slot_at_minute(current_minute)
            if current_slot and current_slot.is_power_off():
                return False

            next_outage_minute = None
            for slot in schedule.today.slots:
                if slot.start > current_minute and slot.is_power_off():
                    next_outage_minute = slot.start
                    break

            if next_outage_minute is None:
                if (schedule.tomorrow and
                    schedule.tomorrow.status == ScheduleStatus.SCHEDULE_APPLIES.value and
                    schedule.tomorrow.slots):
                    for slot in schedule.tomorrow.slots:
                        if slot.is_power_off():
                            next_outage_minute = MINUTES_IN_DAY + slot.start
                            break

            if next_outage_minute is None:
                return True

            minutes_until_outage = next_outage_minute - current_minute

            if minutes_until_outage > UNPLANNED_OUTAGE_THRESHOLD_MINUTES:
                logger.info(f"🔍 Unplanned outage detected: next scheduled outage in {minutes_until_outage} minutes")
                return True

            return False

        except Exception as e:
            logger.error(f"❌ Error checking if unplanned outage: {e}")
            return False

    async def _get_next_event_info(self, power_is_on: bool) -> Optional[str]:
        try:
            schedule = await self.yasno.fetch_schedule()
            if not schedule:
                return None

            if schedule.today.status == ScheduleStatus.EMERGENCY_SHUTDOWNS.value:
                return "🚨 Emergency shutdowns in effect"

            now_kyiv = datetime.now(KYIV_TZ)
            current_minute = now_kyiv.hour * 60 + now_kyiv.minute

            next_event = self._find_next_relevant_event(schedule, now_kyiv, current_minute, power_is_on)

            if next_event:
                next_time, is_outage = next_event

                if is_outage:
                    next_outage = self.yasno_monitor._find_next_outage_slot(schedule, now_kyiv, current_minute) if self.yasno_monitor else None
                    if next_outage:
                        start_time, end_time, _ = next_outage
                        start_str = start_time.strftime("%H:%M")
                        end_str = end_time.strftime("%H:%M")
                        return f"📅 Next outage: **{start_str}-{end_str}**"
                    else:
                        time_str = next_time.strftime("%H:%M")
                        return f"📅 Next outage: **{time_str}**"
                else:
                    time_str = next_time.strftime("%H:%M")
                    return f"📅 Next connection: **{time_str}**"

            if power_is_on:
                return "✅ No more outages scheduled for today"

            if not power_is_on and (not schedule.tomorrow or schedule.tomorrow.status == ScheduleStatus.WAITING_FOR_SCHEDULE.value):
                current_slot = schedule.today.get_slot_at_minute(current_minute)
                if current_slot and current_slot.is_power_off():
                    if current_slot.end == MINUTES_IN_DAY:
                        return "📅 Next connection: **00:00**"
                    else:
                        end_time = f"{current_slot.end // 60:02d}:{current_slot.end % 60:02d}"
                        return f"📅 Next connection: **{end_time}**"

            return None

        except Exception as e:
            logger.error(f"❌ Error getting next event info: {e}")
            return None

    def _find_next_relevant_event(
        self,
        schedule: YasnoSchedule,
        now_kyiv: datetime,
        current_minute: int,
        power_is_on: bool
    ) -> Optional[tuple[datetime, bool]]:
        for i, slot in enumerate(schedule.today.slots):
            if slot.end > current_minute:
                if slot.start > current_minute:
                    if (power_is_on and slot.is_power_off()) or (not power_is_on and not slot.is_power_off()):
                        next_time = now_kyiv.replace(
                            hour=slot.start // 60,
                            minute=slot.start % 60,
                            second=0,
                            microsecond=0
                        )
                        return (next_time, slot.is_power_off())

                if slot.end < MINUTES_IN_DAY and i + 1 < len(schedule.today.slots):
                    next_slot = schedule.today.slots[i + 1]
                    if (power_is_on and next_slot.is_power_off()) or (not power_is_on and not next_slot.is_power_off()):
                        next_time = now_kyiv.replace(
                            hour=slot.end // 60,
                            minute=slot.end % 60,
                            second=0,
                            microsecond=0
                        )
                        return (next_time, next_slot.is_power_off())

        current_slot = schedule.today.get_slot_at_minute(current_minute)
        if not current_slot:
            return None

        if (schedule.tomorrow and
            schedule.tomorrow.status == ScheduleStatus.SCHEDULE_APPLIES.value and
            schedule.tomorrow.slots):

            tomorrow_date = now_kyiv + timedelta(days=1)
            first_tomorrow_slot = schedule.tomorrow.slots[0]

            if first_tomorrow_slot.start == 0:
                if current_slot.type == first_tomorrow_slot.type:
                    if len(schedule.tomorrow.slots) > 1:
                        next_slot = schedule.tomorrow.slots[1]
                        if (power_is_on and next_slot.is_power_off()) or (not power_is_on and not next_slot.is_power_off()):
                            next_time = tomorrow_date.replace(
                                hour=first_tomorrow_slot.end // 60,
                                minute=first_tomorrow_slot.end % 60,
                                second=0,
                                microsecond=0
                            )
                            return (next_time, next_slot.is_power_off())
                else:
                    if (power_is_on and first_tomorrow_slot.is_power_off()) or (not power_is_on and not first_tomorrow_slot.is_power_off()):
                        next_time = tomorrow_date.replace(hour=0, minute=0, second=0, microsecond=0)
                        return (next_time, first_tomorrow_slot.is_power_off())

            for slot in schedule.tomorrow.slots:
                if (power_is_on and slot.is_power_off()) or (not power_is_on and not slot.is_power_off()):
                    next_time = tomorrow_date.replace(
                        hour=slot.start // 60,
                        minute=slot.start % 60,
                        second=0,
                        microsecond=0
                    )
                    return (next_time, slot.is_power_off())

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

        if self.yasno_monitor:
            cleanup_tasks.append(('Yasno monitor', self.yasno_monitor.stop()))

        cleanup_tasks.extend([
            ('Yasno client', self.yasno.close()),
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
