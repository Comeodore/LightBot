import os
import asyncio
import logging
import signal
from datetime import datetime, timezone
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


class HomeAssistantClient:
    def __init__(self, url: str, token: str):
        self.url = url
        self.token = token
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._message_id = 1
        self._closing = False
        
    async def connect(self) -> None:
        self.session = aiohttp.ClientSession()
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
                logger.warning("⚠️ WebSocket closed")
                break
                
    async def close(self) -> None:
        self._closing = True
        if self.ws and not self.ws.closed:
            await self.ws.close()
        if self.session and not self.session.closed:
            await self.session.close()


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


class PowerMonitor:
    def __init__(self, config: Config):
        self.config = config
        self.ha = HomeAssistantClient(config.ws_url, config.ha_token)
        self.db = Database(config.database_url)
        self.notifier = NotificationService(config.bot_token, config.chat_ids)
        self.current_state: Optional[PowerState] = None
        self._shutdown = asyncio.Event()
        
    async def run(self) -> None:
        logger.info("🚀 Power Monitor started")
        logger.info(f"👥 Recipients: {len(self.config.chat_ids)}")
        
        await self.db.connect()
        await self.notifier.start()
        
        try:
            while not self._shutdown.is_set():
                try:
                    await self.ha.connect()
                    await self._initialize_state()
                    await self.ha.subscribe_events()
                    
                    await self._event_loop()
                    
                    if self._shutdown.is_set():
                        break
                        
                except Exception as e:
                    logger.error(f"❌ Connection error: {e}")
                    await self.ha.close()
                    
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
                
    async def _process_event(self, event: dict) -> None:
        if event.get('type') != 'event':
            return
            
        event_data = event.get('event', {}).get('data', {})
        entity_id = event_data.get('entity_id')
        
        if entity_id == self.config.battery_entity:
            new_state = event_data.get('new_state', {})
            battery_level = self._parse_battery_level(new_state)
            self.current_state.battery_level = battery_level
            logger.info(f"🔋 Battery updated: {battery_level}")
            return
            
        if entity_id != self.config.voltage_entity:
            return
            
        new_state_data = event_data.get('new_state', {})
        voltage_value = new_state_data.get('state', 'unknown')
        new_power_state = self._parse_power_state(new_state_data)
        
        logger.info(f"⚡ Voltage: {voltage_value}V -> {new_power_state} (DB state: {self.current_state.state})")
        
        if self._is_valid_transition(self.current_state.state, new_power_state):
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
        
        if new_state.is_power_on():
            message = (
                f"🟢 **POWER RESTORED**\n\n"
                f"⏳ Outage duration: **{duration}**\n"
                f"🔋 Battery level: **{new_state.battery_level}**"
            )
            logger.info(f"✅ Power restored after {duration}")
        else:
            message = (
                f"🔴 **POWER OUTAGE**\n\n"
                f"⚡️ Power was on for: **{duration}**\n"
                f"🔋 Battery level: **{new_state.battery_level}**"
            )
            logger.warning(f"⚠️ Power lost after {duration}")
            
        await self.notifier.send(message)
        await self.db.save_event(new_state)
        self.current_state = new_state
        
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
