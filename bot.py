import os
import asyncio
import logging
import signal
import json
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
import aiohttp
import asyncpg
from telegram.ext import Application

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


YASNO_REGION = '25'
YASNO_DSO = '902'
YASNO_GROUP = '2.2'


@dataclass
class Config:
    ws_url: str
    ha_token: str
    bot_token: str
    chat_ids: list[str]
    database_url: str
    voltage_entity: str
    battery_entity: str
    
    @classmethod
    def from_env(cls):
        required_vars = ['WS_URL', 'HA_TOKEN', 'BOT_TOKEN', 'CHAT_IDS', 'DATABASE_URL']
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")
        
        return cls(
            ws_url=os.getenv('WS_URL'),
            ha_token=os.getenv('HA_TOKEN'),
            bot_token=os.getenv('BOT_TOKEN'),
            chat_ids=[c.strip() for c in os.getenv('CHAT_IDS', '').split(',') if c.strip()],
            database_url=os.getenv('DATABASE_URL'),
            voltage_entity='sensor.victron_vebus_activein_l1_voltage_228',
            battery_entity='sensor.victron_battery_soc'
        )


@dataclass
class PowerState:
    state: str
    timestamp: datetime
    battery_level: str = "N/A"
    
    def is_power_on(self) -> bool:
        return self.state == 'OK'


@dataclass
class TimeSlot:
    start: int
    end: int
    type: str
    
    def is_power_off(self) -> bool:
        return self.type == "Definite"
    
    def contains_minute(self, minute: int) -> bool:
        return self.start <= minute < self.end


@dataclass
class DaySchedule:
    date: str
    slots: list[TimeSlot]
    status: str
    
    def get_slot_at_minute(self, minute: int) -> Optional[TimeSlot]:
        for slot in self.slots:
            if slot.contains_minute(minute):
                return slot
        return None
    
    def get_next_transition(self, current_minute: int) -> Optional[tuple[int, bool]]:
        for slot in self.slots:
            if slot.end > current_minute:
                if slot.start > current_minute:
                    return (slot.start, slot.is_power_off())
                if slot.end < 1440:
                    next_slot_idx = self.slots.index(slot) + 1
                    if next_slot_idx < len(self.slots):
                        next_slot = self.slots[next_slot_idx]
                        return (slot.end, next_slot.is_power_off())
        return None


@dataclass
class YasnoSchedule:
    today: DaySchedule
    updated_on: datetime
    group: str


class YasnoAPIClient:
    BASE_URL = "https://app.yasno.ua/api/blackout-service/public/shutdowns"
    
    def __init__(self, region: str, dso: str, group: str):
        self.region = region
        self.dso = dso
        self.group = group
        self.session: Optional[aiohttp.ClientSession] = None
        
    async def connect(self) -> None:
        if self.session and not self.session.closed:
            return
        self.session = aiohttp.ClientSession()
        
    async def fetch_schedule(self) -> Optional[YasnoSchedule]:
        await self.connect()
        
        url = f"{self.BASE_URL}/regions/{self.region}/dsos/{self.dso}/planned-outages"
        
        try:
            async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status != 200:
                    logger.error(f"❌ Yasno API error: {response.status}")
                    return None
                    
                data = await response.json()
                
                if self.group not in data:
                    logger.error(f"❌ Group {self.group} not found in response")
                    return None
                    
                group_data = data[self.group]
                
                today_data = group_data.get('today', {})
                updated_on_str = group_data.get('updatedOn')
                
                if not updated_on_str:
                    logger.error("❌ Missing updatedOn in response")
                    return None
                
                today = DaySchedule(
                    date=today_data.get('date', ''),
                    slots=[TimeSlot(**slot) for slot in today_data.get('slots', [])],
                    status=today_data.get('status', '')
                )
                
                updated_on = datetime.fromisoformat(updated_on_str.replace('Z', '+00:00'))
                
                return YasnoSchedule(
                    today=today,
                    updated_on=updated_on,
                    group=self.group
                )
                
        except Exception as e:
            logger.error(f"❌ Failed to fetch Yasno schedule: {e}")
            return None
            
    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()


class HomeAssistantClient:
    def __init__(self, url: str, token: str):
        self.url = url
        self.token = token
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._message_id = 1
        
    async def connect(self) -> None:
        await self.close()
        
        self.session = aiohttp.ClientSession()
        try:
            self.ws = await self.session.ws_connect(
                self.url,
                heartbeat=30,
                timeout=aiohttp.ClientWSTimeout(ws_close=30)
            )
            
            auth_msg = await asyncio.wait_for(self.ws.receive_json(), timeout=10)
            if auth_msg.get('type') != 'auth_required':
                raise ConnectionError("Unexpected auth message")
            
            await self.ws.send_json({
                "type": "auth",
                "access_token": self.token
            })
            
            auth_result = await asyncio.wait_for(self.ws.receive_json(), timeout=10)
            if auth_result.get('type') != 'auth_ok':
                raise ConnectionError("Authentication failed")
            
            logger.info("✅ Connected to Home Assistant")
        except Exception:
            await self.close()
            raise
        
    async def subscribe_events(self) -> None:
        await self._send_message("subscribe_events", event_type="state_changed")
        logger.info("📻 Subscribed to state changes")
        
    async def get_states(self) -> dict:
        msg_id = await self._send_message("get_states")
        
        async for msg in self._receive_with_timeout(timeout=10):
            if msg.get('id') == msg_id and msg.get('success'):
                return {
                    state['entity_id']: state 
                    for state in msg.get('result', [])
                }
        
        raise TimeoutError("Failed to get states")
        
    async def _send_message(self, msg_type: str, **kwargs) -> int:
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
        if self.ws:
            if not self.ws.closed:
                await self.ws.close()
            self.ws = None
        if self.session:
            if not self.session.closed:
                await self.session.close()
            self.session = None


class Database:
    def __init__(self, url: str):
        self.url = url
        self.pool: Optional[asyncpg.Pool] = None
        
    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(self.url, min_size=2, max_size=10)
        logger.info("✅ Database connected")
        
    async def get_last_event(self) -> Optional[PowerState]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT state, timestamp FROM power_events ORDER BY timestamp DESC LIMIT 1"
            )
            if row:
                return PowerState(state=row['state'], timestamp=row['timestamp'])
            return None
            
    async def save_event(self, state: PowerState) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO power_events (state, timestamp) VALUES ($1, $2)",
                state.state, state.timestamp
            )
        logger.info(f"💾 Saved: {state.state} at {state.timestamp}")
        
    async def get_yasno_schedule(self) -> Optional[dict]:
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT updated_on, schedule_data "
                "FROM yasno_schedule WHERE id = 1"
            )
            if row:
                schedule_data = row['schedule_data']
                if isinstance(schedule_data, str):
                    schedule_data = json.loads(schedule_data)
                return {
                    'updated_on': row['updated_on'],
                    'schedule_data': schedule_data
                }
            return None
            
    async def save_yasno_schedule(
        self, 
        updated_on: datetime, 
        schedule_data: dict
    ) -> None:
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE yasno_schedule 
                SET updated_on = $1,
                    schedule_data = $2
                WHERE id = 1
                """,
                updated_on, json.dumps(schedule_data)
            )
        
    async def close(self) -> None:
        if self.pool:
            await self.pool.close()
            logger.info("💾 Database closed")


class NotificationService:
    def __init__(self, bot_token: str, chat_ids: list[str]):
        self.chat_ids = chat_ids
        self.app = Application.builder().token(bot_token).build()
        
    async def start(self) -> None:
        await self.app.initialize()
        await self.app.start()
        
    async def send(self, message: str) -> None:
        for chat_id in self.chat_ids:
            try:
                await self.app.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode='Markdown'
                )
            except Exception as e:
                logger.error(f"❌ Failed to send to {chat_id}: {e}")
                
    async def stop(self) -> None:
        await self.app.stop()
        await self.app.shutdown()


class YasnoScheduleMonitor:
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
        if self._task:
            await self._task
        
    async def _monitor_loop(self) -> None:
        await asyncio.sleep(5)
        
        while not self._shutdown.is_set():
            try:
                await self._check_schedule()
            except Exception as e:
                logger.error(f"❌ Error checking Yasno schedule: {e}")
            
            try:
                await asyncio.wait_for(self._shutdown.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass
                
    async def _check_schedule(self) -> None:
        schedule = await self.yasno.fetch_schedule()
        if not schedule:
            return
            
        now = datetime.now(timezone.utc)
        kyiv_offset = timedelta(hours=2)
        now_kyiv = now + kyiv_offset
        
        current_minute = now_kyiv.hour * 60 + now_kyiv.minute
        
        next_event = self._find_next_event(schedule, now_kyiv, current_minute)
        
        saved_schedule = await self.db.get_yasno_schedule()
        
        if saved_schedule:
            saved_updated_on = saved_schedule['updated_on']
            
            if schedule.updated_on > saved_updated_on:
                logger.info(f"📅 Schedule updated: {saved_updated_on} -> {schedule.updated_on}")
                
                saved_next_event = None
                if saved_schedule['schedule_data']:
                    saved_today_data = saved_schedule['schedule_data'].get('today', {})
                    if saved_today_data.get('slots'):
                        saved_day_schedule = DaySchedule(
                            date=saved_today_data.get('date', ''),
                            slots=[TimeSlot(**slot) for slot in saved_today_data['slots']],
                            status=saved_today_data.get('status', '')
                        )
                        saved_transition = saved_day_schedule.get_next_transition(current_minute)
                        if saved_transition:
                            saved_minute, saved_is_outage = saved_transition
                            saved_next_time = now_kyiv.replace(hour=saved_minute // 60, minute=saved_minute % 60, second=0, microsecond=0)
                            saved_next_event = (saved_next_time, saved_is_outage)
                
                if next_event and self.power_monitor.current_state:
                    next_time, is_outage = next_event
                    
                    event_changed = False
                    if saved_next_event is None:
                        event_changed = True
                    else:
                        saved_next_time, saved_is_outage = saved_next_event
                        if next_time != saved_next_time or is_outage != saved_is_outage:
                            event_changed = True
                    
                    if event_changed:
                        power_is_on = self.power_monitor.current_state.is_power_on()
                        
                        should_notify = False
                        if power_is_on and is_outage:
                            event_type = "outage"
                            should_notify = True
                        elif not power_is_on and not is_outage:
                            event_type = "connection"
                            should_notify = True
                        
                        if should_notify:
                            time_str = next_time.strftime("%H:%M")
                            
                            message = (
                                f"📅 **Schedule updated**\n\n"
                                f"Next {event_type}: **{time_str}**"
                            )
                            
                            await self.notifier.send(message)
                            logger.info(f"📅 Sent schedule update notification: {event_type} at {time_str}")
                        else:
                            logger.info(f"📅 Schedule updated but next event doesn't match current power state (power={'ON' if power_is_on else 'OFF'}, next_is_outage={is_outage})")
        
        schedule_data = {
            'today': {
                'date': schedule.today.date,
                'status': schedule.today.status,
                'slots': [{'start': s.start, 'end': s.end, 'type': s.type} for s in schedule.today.slots]
            }
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
            next_time = now_kyiv.replace(hour=minute // 60, minute=minute % 60, second=0, microsecond=0)
            return (next_time, is_outage)
        
        return None


class PowerMonitor:
    def __init__(self, config: Config):
        self.config = config
        self.ha = HomeAssistantClient(config.ws_url, config.ha_token)
        self.db = Database(config.database_url)
        self.notifier = NotificationService(config.bot_token, config.chat_ids)
        self.yasno = YasnoAPIClient(YASNO_REGION, YASNO_DSO, YASNO_GROUP)
        self.yasno_monitor: Optional[YasnoScheduleMonitor] = None
        self.current_state: Optional[PowerState] = None
        self._shutdown = asyncio.Event()
        
    async def run(self) -> None:
        logger.info("🚀 Power Monitor started")
        logger.info(f"👥 Recipients: {len(self.config.chat_ids)}")
        
        await self.db.connect()
        await self.notifier.start()
        
        self.yasno_monitor = YasnoScheduleMonitor(
            self.yasno,
            self.db,
            self.notifier,
            self
        )
        await self.yasno_monitor.start()
        
        try:
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
                    logger.info("🔄 Reconnecting in 3 seconds...")
                    try:
                        await asyncio.wait_for(self._shutdown.wait(), timeout=3)
                    except asyncio.TimeoutError:
                        pass
                        
                except Exception as e:
                    logger.error(f"❌ Connection error: {e}")
                    
                    if not self._shutdown.is_set():
                        logger.info("🔄 Reconnecting in 3 seconds...")
                        try:
                            await asyncio.wait_for(self._shutdown.wait(), timeout=3)
                        except asyncio.TimeoutError:
                            pass
        finally:
            await self._cleanup()
            
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
            
    async def _event_loop(self) -> None:
        async for event in self.ha.receive():
            if self._shutdown.is_set():
                break
            await self._process_event(event)
        
        if self.ha.ws and self.ha.ws.closed:
            code = self.ha.ws.close_code
            exception = self.ha.ws.exception()
            return code, str(exception) if exception else 'Connection closed'
        return None, None
                
    async def _process_event(self, event: dict) -> None:
        if event.get('type') != 'event':
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
        new_power_state = self._parse_power_state(new_state_data)
        
        if self._is_valid_transition(self.current_state.state, new_power_state):
            logger.info(f"⚡ Voltage: {voltage_value}V -> {new_power_state} (DB state: {self.current_state.state})")
            new_power = PowerState(
                state=new_power_state,
                timestamp=datetime.now(timezone.utc),
                battery_level=self.current_state.battery_level
            )
            await self._handle_state_change(new_power)
            
    def _is_valid_transition(self, old_state: str, new_state: str) -> bool:
        return (old_state == 'OK' and new_state == 'ALARM') or \
               (old_state == 'ALARM' and new_state == 'OK')
            
    async def _handle_state_change(self, new_state: PowerState) -> None:
        duration = self._format_duration(self.current_state.timestamp, new_state.timestamp)
        
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
            if next_event_info:
                message += f"\n\n{next_event_info}"
            logger.info(f"⚠️ Power lost after {duration}")
            
        await self.notifier.send(message)
        await self.db.save_event(new_state)
        self.current_state = new_state
    
    async def _get_next_event_info(self, power_is_on: bool) -> Optional[str]:
        try:
            schedule = await self.yasno.fetch_schedule()
            if not schedule:
                return None
            
            now = datetime.now(timezone.utc)
            kyiv_offset = timedelta(hours=2)
            now_kyiv = now + kyiv_offset
            current_minute = now_kyiv.hour * 60 + now_kyiv.minute
            
            next_transition = schedule.today.get_next_transition(current_minute)
            
            if next_transition:
                minute, is_outage = next_transition
                
                if power_is_on and is_outage:
                    time_str = f"{minute // 60:02d}:{minute % 60:02d}"
                    return f"📅 Next outage: **{time_str}**"
                elif not power_is_on and not is_outage:
                    time_str = f"{minute // 60:02d}:{minute % 60:02d}"
                    return f"📅 Next connection: **{time_str}**"
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Error getting next event info: {e}")
            return None
        
    def _parse_power_state(self, state: dict) -> str:
        try:
            voltage = float(state.get('state', 0))
            return 'OK' if voltage > 0 else 'ALARM'
        except (ValueError, TypeError) as e:
            raise ValueError(f"Failed to parse voltage: {e}")
            
    def _parse_battery_level(self, state: Optional[dict]) -> str:
        if not state:
            return "N/A"
        try:
            return f"{round(float(state.get('state')))}%"
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
        logger.info("🧹 Cleaning up...")
        if self.yasno_monitor:
            await self.yasno_monitor.stop()
        await self.yasno.close()
        await self.ha.close()
        await self.db.close()
        await self.notifier.stop()
        logger.info("✅ Cleanup complete")
        
    def shutdown(self) -> None:
        logger.info("🛑 Shutdown requested")
        self._shutdown.set()


async def main():
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
