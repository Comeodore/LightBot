import os
import asyncio
import logging
import json
import signal
from datetime import datetime, timezone
from typing import Optional
from dotenv import load_dotenv
import aiohttp
from telegram.ext import Application

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HomeAssistantWebSocket:
    def __init__(self, url: str, token: str, ping_interval: int = 30):
        self.url = url
        self.token = token
        self.ws = None
        self.session = None
        self.message_id = 1
        self.ping_interval = ping_interval
        self.ping_task = None
        self._closing = False
        
    async def connect(self):
        self.session = aiohttp.ClientSession()
        self.ws = await self.session.ws_connect(
            self.url,
            heartbeat=self.ping_interval,
            timeout=aiohttp.ClientWSTimeout(ws_close=30)
        )
        
        auth_required = await asyncio.wait_for(
            self.ws.receive_json(),
            timeout=10
        )
        if auth_required.get('type') != 'auth_required':
            raise ConnectionError("Unexpected auth message")
        
        await self.ws.send_json({
            "type": "auth",
            "access_token": self.token
        })
        
        auth_result = await asyncio.wait_for(
            self.ws.receive_json(),
            timeout=10
        )
        if auth_result.get('type') != 'auth_ok':
            raise ConnectionError("Authentication failed")
        
        logger.info("✅ WebSocket authenticated")
        
        self.ping_task = asyncio.create_task(self._ping_loop())
    
    async def _ping_loop(self):
        try:
            while not self._closing and self.ws and not self.ws.closed:
                await asyncio.sleep(self.ping_interval)
                if not self._closing and self.ws and not self.ws.closed:
                    await self.ws.ping()
                    logger.debug("🏓 WebSocket ping sent")
        except asyncio.CancelledError:
            logger.debug("Ping loop cancelled")
        except Exception as e:
            logger.error(f"Ping loop error: {e}")
        
    async def subscribe_events(self, event_type: str = "state_changed"):
        msg_id = await self._send_message("subscribe_events", event_type=event_type)
        logger.info(f"📻 Subscribed to {event_type} events")
        return msg_id
    
    async def get_states(self):
        return await self._send_message("get_states")
    
    async def _send_message(self, msg_type: str, **kwargs):
        message = {
            "id": self.message_id,
            "type": msg_type,
            **kwargs
        }
        self.message_id += 1
        await self.ws.send_json(message)
        return self.message_id - 1
    
    async def receive(self):
        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                yield json.loads(msg.data)
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.error(f"❌ WebSocket error: {self.ws.exception()}")
                break
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.warning("⚠️ WebSocket closed")
                break
    
    async def close(self):
        self._closing = True
        
        if self.ping_task and not self.ping_task.done():
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
        
        if self.ws and not self.ws.closed:
            await self.ws.close()
        
        if self.session and not self.session.closed:
            await self.session.close()


class PowerMonitor:
    def __init__(self):
        self.ws_url = os.getenv('WS_URL')
        self.ha_token = os.getenv('HA_TOKEN')
        self.bot_token = os.getenv('BOT_TOKEN')
        self.chat_ids = [c.strip() for c in os.getenv('CHAT_IDS', '').split(',') if c.strip()]
        
        self.alarm_entity = 'sensor.victron_vebus_alarm_gridlost_228'
        self.battery_entity = 'sensor.victron_battery_soc'
        
        self.application = Application.builder().token(self.bot_token).build()
        self.ha_ws = None
        
        self.battery_level = "N/A"
        self._shutdown_event = asyncio.Event()
        self._running = False
    
    def get_duration(self, old_state: dict, new_state: dict) -> str:
        if not old_state or not new_state:
            return "unknown"
        
        try:
            old_changed = datetime.fromisoformat(old_state['last_changed'].replace('Z', '+00:00'))
            new_changed = datetime.fromisoformat(new_state['last_changed'].replace('Z', '+00:00'))
            seconds = (new_changed - old_changed).total_seconds()
            
            days = int(seconds // 86400)
            hours = int((seconds % 86400) // 3600)
            minutes = int((seconds % 3600) // 60)
            
            parts = []
            if days > 0:
                parts.append(f"{days}d")
            if hours > 0:
                parts.append(f"{hours}h")
            if minutes > 0:
                parts.append(f"{minutes}m")
            
            return " ".join(parts) if parts else "less than a minute"
        except Exception:
            return "unknown"
    
    async def send_notification(self, message: str):
        for chat_id in self.chat_ids:
            try:
                await self.application.bot.send_message(
                    chat_id=chat_id,
                    text=message,
                    parse_mode='Markdown'
                )
                logger.info(f"✉️  Notification sent to {chat_id}")
            except Exception as e:
                logger.error(f"❌ Failed to send to {chat_id}: {e}")
    
    def request_shutdown(self):
        logger.info("🛑 Shutdown requested")
        self._shutdown_event.set()
    
    async def initialize_states(self):
        msg_id = await self.ha_ws.get_states()
        
        async for data in self.ha_ws.receive():
            if data.get('id') == msg_id and data.get('success'):
                states = data.get('result', [])
                
                for state in states:
                    entity_id = state.get('entity_id')
                    
                    if entity_id == self.alarm_entity:
                        alarm_state = state.get('state')
                        logger.info(f"📊 Initial alarm state: {alarm_state}")
                    
                    elif entity_id == self.battery_entity:
                        try:
                            self.battery_level = f"{round(float(state.get('state')))}%"
                        except (ValueError, TypeError):
                            self.battery_level = "N/A"
                        logger.info(f"🔋 Initial battery level: {self.battery_level}")
                
                break
    
    async def handle_state_change(self, entity_id: str, new_state: dict, old_state: dict):
        if entity_id == self.battery_entity:
            try:
                self.battery_level = f"{round(float(new_state.get('state')))}%"
            except (ValueError, TypeError):
                self.battery_level = "N/A"
            return
        
        if entity_id != self.alarm_entity or not old_state:
            return
        
        old_value = old_state.get('state')
        new_value = new_state.get('state')
        
        if old_value == 'OK' and new_value == 'ALARM':
            duration = self.get_duration(old_state, new_state)
            message = (
                f"🔴 **POWER OUTAGE**\n\n"
                f"⚡️ Power was on for: **{duration}**\n"
                f"🔋 Battery level: **{self.battery_level}**"
            )
            logger.warning(f"⚠️  Power lost! Was on for: {duration}")
            await self.send_notification(message)
        
        elif old_value == 'ALARM' and new_value == 'OK':
            duration = self.get_duration(old_state, new_state)
            message = (
                f"🟢 **POWER RESTORED**\n\n"
                f"⏳ Outage duration: **{duration}**\n"
                f"🔋 Battery level: **{self.battery_level}**"
            )
            logger.info(f"✅ Power restored! Outage lasted: {duration}")
            await self.send_notification(message)
    
    async def process_events(self):
        async for data in self.ha_ws.receive():
            if data.get('type') == 'event':
                event = data.get('event', {})
                
                if event.get('event_type') == 'state_changed':
                    event_data = event.get('data', {})
                    entity_id = event_data.get('entity_id')
                    
                    if entity_id in [self.alarm_entity, self.battery_entity]:
                        await self.handle_state_change(
                            entity_id,
                            event_data.get('new_state', {}),
                            event_data.get('old_state', {})
                        )
    
    async def run(self):
        logger.info("🚀 Starting Power Monitor Bot")
        logger.info(f"🔌 WebSocket: {self.ws_url}")
        logger.info(f"👥 Recipients: {len(self.chat_ids)}")
        logger.info(f"📡 Monitoring: {self.alarm_entity}")
        logger.info(f"🔋 Tracking: {self.battery_entity}")
        
        await self.application.initialize()
        await self.application.start()
        self._running = True
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    self.ha_ws = HomeAssistantWebSocket(self.ws_url, self.ha_token)
                    await self.ha_ws.connect()
                    
                    await self.initialize_states()
                    await self.ha_ws.subscribe_events()
                    
                    events_task = asyncio.create_task(self.process_events())
                    shutdown_task = asyncio.create_task(self._shutdown_event.wait())
                    
                    done, pending = await asyncio.wait(
                        {events_task, shutdown_task},
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    for task in pending:
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            pass
                    
                    if self._shutdown_event.is_set():
                        logger.info("Shutdown signal received, exiting")
                        break
                    
                except aiohttp.ClientError as e:
                    logger.error(f"❌ Connection error: {e}")
                except asyncio.TimeoutError:
                    logger.error("❌ Connection timeout")
                except Exception as e:
                    logger.error(f"❌ Unexpected error: {e}")
                finally:
                    if self.ha_ws:
                        await self.ha_ws.close()
                
                if not self._shutdown_event.is_set():
                    logger.info("🔄 Reconnecting in 10 seconds...")
                    try:
                        await asyncio.wait_for(
                            self._shutdown_event.wait(),
                            timeout=10
                        )
                    except asyncio.TimeoutError:
                        pass
        finally:
            self._running = False
            await self._cleanup()
    
    async def _cleanup(self):
        logger.info("🧹 Cleaning up resources...")
        
        if self.ha_ws:
            await self.ha_ws.close()
        
        if self.application:
            await self.application.stop()
            await self.application.shutdown()
        
        logger.info("✅ Cleanup complete")


async def main():
    monitor = PowerMonitor()
    
    loop = asyncio.get_running_loop()
    
    def signal_handler(sig):
        logger.info(f"📡 Received signal {sig.name}")
        monitor.request_shutdown()
    
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))
    
    try:
        await monitor.run()
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        raise
    finally:
        logger.info("👋 Shutting down gracefully")


if __name__ == "__main__":
    asyncio.run(main())
