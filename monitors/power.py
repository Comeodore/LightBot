from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, TYPE_CHECKING

from config import Config
from models.power import PowerStatus, PowerState
from models.dtek import DtekCurrentOutage, DtekDaySchedule
from services.home_assistant import HomeAssistantClient
from services.database import Database
from services.notifications import NotificationService
from utils.time_format import (
    KYIV_TZ,
    MINUTES_IN_DAY,
    minutes_to_time,
    format_duration,
)

if TYPE_CHECKING:
    from monitors.dtek_schedule import DtekScheduleMonitor

logger = logging.getLogger(__name__)

UNPLANNED_OUTAGE_THRESHOLD_MINUTES = 60


class PowerMessageBuilder:
    @staticmethod
    def power_restored(
        duration: str, battery: str, next_info: Optional[str]
    ) -> str:
        msg = (
            f"🟢 *POWER RESTORED*\n\n"
            f"⏱ Outage duration: *{duration}*\n"
            f"🔋 Battery: *{battery}*"
        )
        if next_info:
            msg += f"\n\n{next_info}"
        return msg

    @staticmethod
    def power_outage(
        uptime: str, battery: str, is_unplanned: bool, next_info: Optional[str]
    ) -> str:
        msg = (
            f"🔴 *POWER OUTAGE*\n\n"
            f"⏱ Uptime: *{uptime}*\n"
            f"🔋 Battery: *{battery}*"
        )
        if is_unplanned:
            msg += "\n\n⚠️ *Unscheduled outage*"
        elif next_info:
            msg += f"\n\n{next_info}"
        return msg


class NextEventInfoBuilder:
    def __init__(
        self,
        current_outage: Optional[DtekCurrentOutage],
        today: Optional[DtekDaySchedule],
        tomorrow: Optional[DtekDaySchedule],
        current_minute: int,
    ):
        self.current_outage = current_outage
        self.today = today
        self.tomorrow = tomorrow
        self.current_minute = current_minute

    def get_info(self, power_is_on: bool) -> Optional[str]:
        if power_is_on:
            return self._get_power_on_info()
        else:
            return self._get_power_off_info()

    def _get_power_on_info(self) -> Optional[str]:
        if self.current_outage and self.current_outage.is_emergency:
            next_outage = self._find_next_outage_string()
            if next_outage:
                return f"🚨 Emergency shutdowns in effect\n{next_outage}"
            return "🚨 Emergency shutdowns in effect"

        if not self.today:
            return None

        next_period = self.today.find_next_period(self.current_minute)
        if next_period:
            return f"📅 Next outage: *{self._format_period(*next_period)}*"

        if self.tomorrow:
            next_period = self.tomorrow.find_next_period(0)
            if next_period:
                return f"📅 Next outage: tomorrow *{self._format_period(*next_period)}*"

        return "✅ No more outages scheduled for today"

    def _get_power_off_info(self) -> Optional[str]:
        if self.current_outage and self.current_outage.is_emergency:
            return self._get_emergency_restoration_info()

        return self._get_scheduled_restoration_info()

    def _get_emergency_restoration_info(self) -> Optional[str]:
        if self.current_outage.restoration_time:
            return f"🚨 Emergency shutdown\n🕐 Restoration: *{self.current_outage.restoration_time}*"
        return "🚨 Emergency shutdown"

    def _get_scheduled_restoration_info(self) -> Optional[str]:
        slot_end = self._get_slot_end_time()
        if slot_end:
            return f"🕐 Next connection: *{slot_end}*"
        return None

    def _get_slot_end_time(self) -> Optional[str]:
        if not self.today:
            return None

        outage_end = self.today.get_current_outage_end(self.current_minute)
        if not outage_end:
            return None

        if outage_end == MINUTES_IN_DAY:
            if self.tomorrow:
                tomorrow_periods = self.tomorrow.get_outage_periods()
                if tomorrow_periods and tomorrow_periods[0][0] == 0:
                    end = tomorrow_periods[0][1]
                    return f"tomorrow {minutes_to_time(end)}"
            return "00:00"

        return minutes_to_time(outage_end)

    def _find_next_outage_string(self) -> Optional[str]:
        if self.today:
            next_period = self.today.find_next_period(self.current_minute)
            if next_period:
                start, end = next_period
                if end == MINUTES_IN_DAY and self.tomorrow:
                    tomorrow_periods = self.tomorrow.get_outage_periods()
                    if tomorrow_periods and tomorrow_periods[0][0] == 0:
                        return f"📅 Next outage: *{minutes_to_time(start)} - tomorrow {minutes_to_time(tomorrow_periods[0][1])}*"
                return f"📅 Next  outage: *{self._format_period(*next_period)}*"

        if self.tomorrow:
            next_period = self.tomorrow.find_next_period(0)
            if next_period:
                return f"📅 Next outage: tomorrow *{self._format_period(*next_period)}*"

        return None

    @staticmethod
    def _format_period(start: int, end: int) -> str:
        return f"{minutes_to_time(start)}-{minutes_to_time(end)}"


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
        self._msg = PowerMessageBuilder()

    def is_power_off(self) -> bool:
        return self.current_state is not None and not self.current_state.is_power_on()

    async def run(self) -> None:
        logger.info("🚀 Power Monitor started")
        logger.info(f"👥 Recipients: {len(self.config.chat_ids)}")

        try:
            await self.db.connect()
            await self.notifier.start()

            from monitors.dtek_schedule import DtekScheduleMonitor

            self.dtek_monitor = DtekScheduleMonitor(self.db, self.notifier, self)
            await self.dtek_monitor.start()

            await self._main_loop()
        except Exception as e:
            logger.error(f"Fatal error in run: {e}")
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
                    logger.warning(
                        f"WebSocket disconnected: code={close_code}, reason={close_reason}"
                    )
                else:
                    logger.warning("WebSocket disconnected")

                await self._wait_for_reconnect()

            except Exception as e:
                logger.error(f"Connection error: {e}")
                if not self._shutdown.is_set():
                    await self._wait_for_reconnect()

    async def _wait_for_reconnect(self) -> None:
        logger.info(f"Reconnecting in {self.RECONNECT_DELAY} seconds...")
        try:
            await asyncio.wait_for(
                self._shutdown.wait(), timeout=self.RECONNECT_DELAY
            )
        except asyncio.TimeoutError:
            pass

    async def _initialize_state(self) -> None:
        last_event = await self.db.get_last_event()
        if not last_event:
            raise ValueError(
                "No events in database. Please initialize the database with a starting event."
            )

        states = await self.ha.get_states()
        battery_state = states.get(self.config.battery_entity)
        current_battery = self._parse_battery_level(battery_state)

        last_event.battery_level = current_battery
        self.current_state = last_event
        logger.info(
            f"Initial state: {self.current_state.state}, Battery: {self.current_state.battery_level}"
        )

    async def _event_loop(self) -> tuple[Optional[int], Optional[str]]:
        async for event in self.ha.receive():
            if self._shutdown.is_set():
                break
            try:
                await self._process_event(event)
            except Exception as e:
                logger.error(f"Error processing event: {e}")

        if self.ha.ws and self.ha.ws.closed:
            code = self.ha.ws.close_code
            exception = self.ha.ws.exception()
            return code, str(exception) if exception else "Connection closed"
        return None, None

    async def _process_event(self, event: dict) -> None:
        if event.get("type") != "event":
            return

        if not self.current_state:
            logger.warning("Current state not initialized, skipping event")
            return

        event_data = event.get("event", {}).get("data", {})
        entity_id = event_data.get("entity_id")

        if entity_id == self.config.battery_entity:
            new_state = event_data.get("new_state", {})
            self.current_state.battery_level = self._parse_battery_level(new_state)
            return

        if entity_id != self.config.voltage_entity:
            return

        new_state_data = event_data.get("new_state", {})
        voltage_value = new_state_data.get("state", "unknown")

        try:
            new_power_status = self._parse_power_state(new_state_data)
        except ValueError as e:
            logger.warning(f"Invalid power state: {e}")
            return

        if self._is_valid_transition(self.current_state.status, new_power_status):
            logger.info(
                f"Voltage: {voltage_value}V -> {new_power_status.value} "
                f"(DB state: {self.current_state.state})"
            )
            new_power = PowerState(
                status=new_power_status,
                timestamp=datetime.now(timezone.utc),
                battery_level=self.current_state.battery_level,
            )
            await self._handle_state_change(new_power)

    @staticmethod
    def _is_valid_transition(old_status: PowerStatus, new_status: PowerStatus) -> bool:
        return (old_status == PowerStatus.OK and new_status == PowerStatus.ALARM) or (
            old_status == PowerStatus.ALARM and new_status == PowerStatus.OK
        )

    async def _handle_state_change(self, new_state: PowerState) -> None:
        duration = format_duration(self.current_state.timestamp, new_state.timestamp)

        is_unplanned = (
            self._check_if_unplanned_outage() if not new_state.is_power_on() else False
        )
        next_info = self._get_next_event_info(new_state.is_power_on())

        if new_state.is_power_on():
            message = self._msg.power_restored(
                duration, new_state.battery_level, next_info
            )
            logger.info(f"🟢 Power restored after {duration}")
        else:
            message = self._msg.power_outage(
                duration, new_state.battery_level, is_unplanned, next_info
            )
            log_suffix = "(UNSCHEDULED)" if is_unplanned else ""
            logger.info(f"🔴 Power lost {log_suffix} after {duration}")

        self.current_state = new_state
        await self.notifier.send(message)

        try:
            await self.db.save_event(new_state)
        except Exception as e:
            logger.error(f"Failed to save event to DB: {e}")

    def _check_if_unplanned_outage(self) -> bool:
        if not self.dtek_monitor:
            return False

        current_outage, today, tomorrow = self.dtek_monitor.get_current_schedule()

        if current_outage and current_outage.is_emergency:
            return False

        if not today:
            return False

        now = datetime.now(KYIV_TZ)
        current_minute = now.hour * 60 + now.minute

        if today.is_outage_at_minute(current_minute):
            return False

        next_outage_minute = self._find_next_outage_minute(
            today, tomorrow, current_minute
        )

        if next_outage_minute is None:
            return True

        minutes_until = next_outage_minute - current_minute
        if minutes_until > UNPLANNED_OUTAGE_THRESHOLD_MINUTES:
            logger.info(
                f"⚠️ Unplanned outage detected: next scheduled in {minutes_until} minutes"
            )
            return True

        return False

    def _find_next_outage_minute(
        self,
        today: DtekDaySchedule,
        tomorrow: Optional[DtekDaySchedule],
        current_minute: int,
    ) -> Optional[int]:
        periods = today.get_outage_periods()
        for start, _ in periods:
            if start > current_minute:
                return start

        if tomorrow:
            tomorrow_periods = tomorrow.get_outage_periods()
            if tomorrow_periods:
                return MINUTES_IN_DAY + tomorrow_periods[0][0]

        return None

    def _get_next_event_info(self, power_is_on: bool) -> Optional[str]:
        if not self.dtek_monitor:
            return None

        current_outage, today, tomorrow = self.dtek_monitor.get_current_schedule()
        now = datetime.now(KYIV_TZ)
        current_minute = now.hour * 60 + now.minute

        builder = NextEventInfoBuilder(current_outage, today, tomorrow, current_minute)
        return builder.get_info(power_is_on)

    @staticmethod
    def _parse_power_state(state: dict) -> PowerStatus:
        try:
            voltage_value = state.get("state", 0)
            if voltage_value in ("unavailable", "unknown", None):
                raise ValueError(f"Voltage state unavailable: {voltage_value}")
            voltage = float(voltage_value)
            return PowerStatus.OK if voltage > 0 else PowerStatus.ALARM
        except (ValueError, TypeError) as e:
            raise ValueError(f"Failed to parse voltage: {e}") from e

    @staticmethod
    def _parse_battery_level(state: Optional[dict]) -> str:
        if not state:
            return "N/A"
        try:
            battery_value = state.get("state")
            if battery_value is None or battery_value in ("unavailable", "unknown"):
                return "N/A"
            return f"{round(float(battery_value))}%"
        except (ValueError, TypeError):
            return "N/A"

    async def _cleanup(self) -> None:
        if self._cleanup_done:
            return

        self._cleanup_done = True
        logger.info("🧹 Cleaning up...")

        cleanup_tasks = []

        if self.dtek_monitor:
            cleanup_tasks.append(("DTEK monitor", self.dtek_monitor.stop()))

        cleanup_tasks.extend(
            [
                ("Home Assistant", self.ha.close()),
                ("Database", self.db.close()),
                ("Notifier", self.notifier.stop()),
            ]
        )

        for name, task in cleanup_tasks:
            try:
                await asyncio.wait_for(task, timeout=self.SHUTDOWN_TIMEOUT)
            except asyncio.TimeoutError:
                logger.warning(f"{name} cleanup timeout")
            except Exception as e:
                logger.warning(f"Error cleaning up {name}: {e}")

        logger.info("✅ Cleanup complete")

    def shutdown(self) -> None:
        if not self._shutdown.is_set():
            logger.info("🛑 Shutdown requested")
            self._shutdown.set()
