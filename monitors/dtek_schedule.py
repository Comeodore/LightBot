from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, TYPE_CHECKING

from models.dtek import DtekCurrentOutage, DtekDaySchedule, OutagePeriodFormatter
from services.database import Database
from services.notifications import NotificationService
from utils.time_format import KYIV_TZ

if TYPE_CHECKING:
    from monitors.power import PowerMonitor

logger = logging.getLogger(__name__)


@dataclass
class ScheduleState:
    current_outage: Optional[DtekCurrentOutage] = None
    today: Optional[DtekDaySchedule] = None
    tomorrow: Optional[DtekDaySchedule] = None

    def has_tomorrow_outages(self) -> bool:
        return self.tomorrow is not None and self.tomorrow.has_outages()


class ScheduleMessageBuilder:
    @staticmethod
    def tomorrow_cancelled(date_str: str) -> str:
        return f"📅 Tomorrow ({date_str}) outages cancelled"

    @staticmethod
    def tomorrow_published() -> str:
        return "📅 Tomorrow schedule published"

    @staticmethod
    def schedule_updated_with_info(day_label: str, date_str: str, info: str) -> str:
        return f"📅 {day_label} ({date_str}) schedule updated\n\n{info}"

    @staticmethod
    def emergency_restoration(restoration_time: str) -> str:
        return f"🚨 Emergency\n\n🕐 Restoration: *{restoration_time}*"

    @staticmethod
    def emergency_detected(restoration_time: str) -> str:
        msg = "🚨 *Emergency detected*"
        if restoration_time:
            msg += f"\n\n🕐 Restoration: *{restoration_time}*"
        return msg

    @staticmethod
    def emergency_cancelled() -> str:
        return "✅ Emergency cancelled"


class DtekScheduleMonitor:
    CHECK_INTERVAL = 60
    INITIAL_DELAY = 5

    def __init__(
        self,
        db: Database,
        notifier: NotificationService,
        power_monitor: PowerMonitor,
    ):
        self.db = db
        self.notifier = notifier
        self.power_monitor = power_monitor
        self._shutdown = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._state = ScheduleState()
        self._msg = ScheduleMessageBuilder()

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

    def get_current_schedule(
        self,
    ) -> tuple[Optional[DtekCurrentOutage], Optional[DtekDaySchedule], Optional[DtekDaySchedule]]:
        return self._state.current_outage, self._state.today, self._state.tomorrow

    async def _load_initial_state(self) -> None:
        data = await self.db.get_dtek_schedule()
        if data:
            now = datetime.now(KYIV_TZ)
            self._state = self._build_validated_state(data, now)
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
                logger.error(f"Error checking DTEK schedule: {e}")

            try:
                await asyncio.wait_for(
                    self._shutdown.wait(), timeout=self.CHECK_INTERVAL
                )
            except asyncio.TimeoutError:
                pass

    async def _check_schedule(self) -> None:
        data = await self.db.get_dtek_schedule()
        if not data:
            return

        now = datetime.now(KYIV_TZ)
        new_state = self._build_validated_state(data, now)

        await self._check_outage_changes(new_state)

        if not new_state.today:
            self._state = new_state
            return

        if self._state.today is None:
            self._state = new_state
            logger.info("📅 Initial DTEK schedule state set")
            return

        is_emergency = new_state.current_outage and new_state.current_outage.is_emergency
        if not is_emergency:
            await self._handle_schedule_changes(new_state, now)

        self._state = new_state

    def _build_validated_state(self, data: dict, now: datetime) -> ScheduleState:
        current_outage = DtekCurrentOutage.from_dict(data.get("current_outage"))
        today = DtekDaySchedule.from_dict(data.get("today"))
        tomorrow = DtekDaySchedule.from_dict(data.get("tomorrow"))

        expected_today = now.strftime("%d.%m.%y")
        expected_tomorrow = (now + timedelta(days=1)).strftime("%d.%m.%y")

        if today and today.date != expected_today:
            # logger.warning(f"Stale today schedule: {today.date} != {expected_today}, ignoring")
            today = None

        if tomorrow and tomorrow.date != expected_tomorrow:
            # logger.warning(f"Stale tomorrow schedule: {tomorrow.date} != {expected_tomorrow}, ignoring")
            tomorrow = None

        return ScheduleState(
            current_outage=current_outage,
            today=today,
            tomorrow=tomorrow,
        )

    async def _check_outage_changes(self, new_state: ScheduleState) -> None:
        self._log_status_mismatch_if_changed(new_state)

        old_outage = self._state.current_outage
        new_outage = new_state.current_outage

        was_emergency = old_outage and old_outage.is_emergency
        is_emergency = new_outage and new_outage.is_emergency

        if is_emergency and not was_emergency:
            await self._notify_emergency_detected(new_outage)
        elif was_emergency and not is_emergency:
            await self._notify_emergency_cancelled()
        elif is_emergency and was_emergency:
            await self._check_emergency_restoration_update(old_outage, new_outage)

        if new_outage and new_outage.is_scheduled:
            if not (old_outage and old_outage.is_scheduled):
                logger.debug(f"Site shows scheduled outage: {new_outage.restoration_time}")

    def _log_status_mismatch_if_changed(self, new_state: ScheduleState) -> None:
        # Only log when outage data actually changes
        if new_state.current_outage == self._state.current_outage:
            return
        
        power_is_off = self.power_monitor.is_power_off()
        site_has_outage = new_state.current_outage and new_state.current_outage.is_outage
        site_status = new_state.current_outage.status if new_state.current_outage else "power_on"
        
        if power_is_off and not site_has_outage:
            logger.info(f"⚠️ Status mismatch: power OFF but site shows '{site_status}', last updated: {new_state.current_outage.last_updated}")
        elif not power_is_off and site_has_outage:
            logger.info(f"⚠️ Status mismatch: power ON but site shows '{site_status}', last updated: {new_state.current_outage.last_updated}")

    async def _notify_emergency_detected(self, outage: DtekCurrentOutage) -> None:
        try:
            await self.notifier.send(self._msg.emergency_detected(outage.restoration_time))
            logger.info(f"🚨 Emergency detected: restoration_time={outage.restoration_time}")
        except Exception as e:
            logger.error(f"Failed to send emergency notification: {e}")

    async def _notify_emergency_cancelled(self) -> None:
        try:
            await self.notifier.send(self._msg.emergency_cancelled())
            logger.info("✅ Emergency cancelled")
        except Exception as e:
            logger.error(f"Failed to send emergency cancelled notification: {e}")

    async def _check_emergency_restoration_update(
        self, old_outage: DtekCurrentOutage, new_outage: DtekCurrentOutage
    ) -> None:
        if old_outage.restoration_time != new_outage.restoration_time:
            try:
                msg = self._msg.emergency_restoration(new_outage.restoration_time)
                await self.notifier.send(msg)
                logger.info(f"🔄 Emergency restoration time changed: {new_outage.restoration_time}")
            except Exception as e:
                logger.error(f"Failed to send restoration time update: {e}")

    async def _handle_schedule_changes(self, new_state: ScheduleState, now: datetime) -> None:
        is_new_day = self._is_new_day(new_state)
        tomorrow_changes = self._detect_tomorrow_changes(new_state, is_new_day)

        if tomorrow_changes == "published":
            await self._handle_tomorrow_published()
        elif tomorrow_changes == "cancelled":
            await self._handle_tomorrow_cancelled(now, is_new_day)
        elif tomorrow_changes == "changed":
            await self._handle_tomorrow_changed(new_state, now)

        await self._check_today_changes(new_state, now)

    def _is_new_day(self, new_state: ScheduleState) -> bool:
        return (
            self._state.today is not None
            and new_state.today is not None
            and self._state.today.date != new_state.today.date
        )

    def _detect_tomorrow_changes(self, new_state: ScheduleState, is_new_day: bool) -> Optional[str]:
        saved_had_outages = self._state.has_tomorrow_outages() if not is_new_day else False
        new_has_outages = new_state.has_tomorrow_outages()

        if new_has_outages and not saved_had_outages:
            return "published"
        if saved_had_outages and not new_has_outages:
            if is_new_day and self._state.tomorrow and not self._slots_differ(
                new_state.today, self._state.tomorrow
            ):
                return None
            return "cancelled"
        if new_has_outages and saved_had_outages:
            if self._slots_differ(new_state.tomorrow, self._state.tomorrow):
                return "changed"
        return None

    async def _handle_tomorrow_published(self) -> None:
        try:
            await self.notifier.send(self._msg.tomorrow_published())
            logger.info("📅 Tomorrow schedule published")
        except Exception as e:
            logger.error(f"Failed to send tomorrow published notification: {e}")

    async def _handle_tomorrow_cancelled(self, now: datetime, is_new_day: bool) -> None:
        if is_new_day:
            logger.debug("Day shifted, tomorrow became today")
            return

        tomorrow_date = (now + timedelta(days=1)).strftime("%d.%m")
        try:
            await self.notifier.send(self._msg.tomorrow_cancelled(tomorrow_date))
            logger.info("✅ Tomorrow outages cancelled")
        except Exception as e:
            logger.error(f"Failed to send tomorrow cancelled notification: {e}")

    async def _handle_tomorrow_changed(self, new_state: ScheduleState, now: datetime) -> None:
        tomorrow_date = (now + timedelta(days=1)).strftime("%d.%m")
        info = self._get_schedule_context_info(new_state, now)
        try:
            msg = self._msg.schedule_updated_with_info("Tomorrow", tomorrow_date, info)
            await self.notifier.send(msg)
            logger.info(f"📅 Tomorrow schedule changed: {info}")
        except Exception as e:
            logger.error(f"Failed to send tomorrow changed notification: {e}")

    async def _check_today_changes(self, new_state: ScheduleState, now: datetime) -> None:
        if not self._slots_differ(new_state.today, self._state.today):
            return

        is_new_day = self._is_new_day(new_state)
        if is_new_day and self._state.tomorrow and not self._slots_differ(
            new_state.today, self._state.tomorrow
        ):
            logger.debug("Day shifted, schedule unchanged")
            return

        date_str = now.strftime("%d.%m")
        info = self._get_schedule_context_info(new_state, now)
        try:
            msg = self._msg.schedule_updated_with_info("Today", date_str, info)
            await self.notifier.send(msg)
            logger.info(f"📅 Today schedule updated: {info}")
        except Exception as e:
            logger.error(f"Failed to send today schedule change notification: {e}")

    def _get_schedule_context_info(self, state: ScheduleState, now: datetime) -> str:
        current_minute = now.hour * 60 + now.minute
        formatter = OutagePeriodFormatter(state.today, state.tomorrow, current_minute)

        if self.power_monitor.is_power_off():
            slot_end = formatter.get_slot_end()
            if slot_end:
                return f"🕐 Current outage ends: *{slot_end}*"
            return "⚠️ Outage end unknown"
        else:
            next_outage = formatter.get_outage_string(power_is_off=False)
            if next_outage:
                return f"⚡ Next outage: *{next_outage}*"
            return "✅ No scheduled outages"

    @staticmethod
    def _slots_differ(
        new: Optional[DtekDaySchedule], old: Optional[DtekDaySchedule]
    ) -> bool:
        if new is None and old is None:
            return False
        if new is None or old is None:
            return True
        if len(new.slots) != len(old.slots):
            return True
        return any(
            ns.hour != os.hour or ns.status != os.status
            for ns, os in zip(new.slots, old.slots)
        )
