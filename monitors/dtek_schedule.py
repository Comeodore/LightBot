from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, TYPE_CHECKING

from models.dtek import DtekCurrentOutage, DtekDaySchedule
from services.database import Database
from services.notifications import NotificationService
from utils.time_format import (
    KYIV_TZ,
    MINUTES_IN_DAY,
    minutes_to_time,
    format_period,
    format_duration_hours,
)

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
    def format_schedule(date_str: str, schedule: DtekDaySchedule) -> str:
        periods = schedule.get_outage_periods()
        if not periods:
            return f"📅 *{date_str} schedule*\n\n✅ No outages scheduled"

        outage_lines = [format_period(s, e) for s, e in periods]
        outage_duration = format_duration_hours(schedule.total_outage_minutes())
        power_duration = format_duration_hours(schedule.total_power_minutes())

        return (
            f"📅 *{date_str} schedule*\n\n"
            + "\n".join(outage_lines)
            + f"\n\n⚡️ Power: *{power_duration}*\n"
            + f"🔴 Outages: *{outage_duration}*"
        )

    @staticmethod
    def tomorrow_cancelled(date_str: str) -> str:
        return f"📅 *Tomorrow ({date_str})*\n\n✅ Outages cancelled"

    @staticmethod
    def tomorrow_published(next_outage_str: str) -> str:
        return f"📅 *Tomorrow schedule published*\n\n🔴 Next outage: *{next_outage_str}*"

    @staticmethod
    def schedule_updated(day_label: str, date_str: str, next_outage_str: Optional[str]) -> str:
        if next_outage_str:
            return f"📅 *{day_label} ({date_str}) schedule updated*\n\n🔴 Next outage: *{next_outage_str}*"
        return f"📅 *{day_label} ({date_str}) schedule updated*\n\n✅ No more outages scheduled"

    @staticmethod
    def restoration_time_updated(restoration_time: str) -> str:
        return f"🔄 *Restoration time updated*\n\n🕐 New time: *{restoration_time}*"

    @staticmethod
    def slot_end_updated(slot_end: str) -> str:
        return f"📅 *Schedule updated*\n\n🕐 Outage ends: *{slot_end}*"

    @staticmethod
    def emergency_detected(restoration_time: str) -> str:
        msg = "🚨 *Emergency shutdown detected*"
        if restoration_time:
            msg += f"\n\n🕐 Restoration: *{restoration_time}*"
        return msg

    @staticmethod
    def outage_type_detected(outage_type: str, restoration_time: str) -> str:
        msg = f"⚡️ *{outage_type} detected*"
        if restoration_time:
            msg += f"\n\n🕐 Restoration: *{restoration_time}*"
        return msg


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

        current_minute = now.hour * 60 + now.minute
        await self._handle_schedule_changes(new_state, now, current_minute)

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
        if not self.power_monitor.is_power_off():
            self._log_outage_changes(new_state)
            return

        had_outage = self._state.current_outage and self._state.current_outage.is_outage
        has_outage = new_state.current_outage and new_state.current_outage.is_outage

        if has_outage and not had_outage:
            await self._handle_new_outage_info(new_state.current_outage)
            return

        if has_outage and had_outage:
            await self._handle_outage_info_update(
                self._state.current_outage, new_state.current_outage, new_state
            )

    def _log_outage_changes(self, new_state: ScheduleState) -> None:
        if new_state.current_outage != self._state.current_outage:
            logger.info(f"Current outage changed (power on): {new_state.current_outage}")

    async def _handle_new_outage_info(self, outage: DtekCurrentOutage) -> None:
        if outage.is_emergency:
            await self.notifier.send(
                self._msg.emergency_detected(outage.restoration_time)
            )
            logger.info(f"🚨 Emergency detected: restoration_time={outage.restoration_time}")
        elif outage.is_scheduled:
            await self.notifier.send(
                self._msg.outage_type_detected("Scheduled outage", outage.restoration_time)
            )
            logger.info(f"⚡️ Scheduled outage detected: restoration_time={outage.restoration_time}")

    async def _handle_outage_info_update(
        self,
        old_outage: DtekCurrentOutage,
        new_outage: DtekCurrentOutage,
        new_state: ScheduleState,
    ) -> None:
        if old_outage.restoration_time != new_outage.restoration_time:
            await self.notifier.send(
                self._msg.restoration_time_updated(new_outage.restoration_time)
            )
            logger.info(f"🔄 Restoration time changed: {new_outage.restoration_time}")

    async def _handle_schedule_changes(
        self, new_state: ScheduleState, now: datetime, current_minute: int
    ) -> None:
        is_new_day = self._is_new_day(new_state)
        tomorrow_changes = self._detect_tomorrow_changes(new_state, is_new_day)

        if tomorrow_changes == "published":
            await self._handle_tomorrow_published(new_state, now, current_minute)
        elif tomorrow_changes == "cancelled":
            await self._handle_tomorrow_cancelled(new_state, now, current_minute, is_new_day)
        elif tomorrow_changes == "changed":
            await self._handle_tomorrow_changed(new_state, now, current_minute)
        elif new_state.has_tomorrow_outages():
            await self._update_pinned_if_changed(new_state.tomorrow, now, is_tomorrow=True)
            await self._check_today_changes(new_state, now, current_minute, True)
        else:
            await self._update_pinned_if_changed(new_state.today, now, is_tomorrow=False)
            await self._check_today_changes(new_state, now, current_minute, False)

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

    async def _handle_tomorrow_published(
        self, new_state: ScheduleState, now: datetime, current_minute: int
    ) -> None:
        tomorrow_date = (now + timedelta(days=1)).strftime("%d.%m")
        schedule_msg = self._msg.format_schedule(tomorrow_date, new_state.tomorrow)
        await self.notifier.send(schedule_msg, pin=True, unpin_all_first=True)

        next_outage_str = self._get_next_outage_string(
            new_state.today, new_state.tomorrow, current_minute
        )
        if next_outage_str:
            await self.notifier.send_reply_to_pinned(
                self._msg.tomorrow_published(next_outage_str)
            )

        logger.info(f"📅 Sent tomorrow schedule: {tomorrow_date}")

    async def _handle_tomorrow_cancelled(
        self,
        new_state: ScheduleState,
        now: datetime,
        current_minute: int,
        is_new_day: bool,
    ) -> None:
        if is_new_day and self._state.tomorrow and not self._slots_differ(
            new_state.today, self._state.tomorrow
        ):
            logger.debug("Day shifted, tomorrow became today")
            return

        tomorrow_date = (now + timedelta(days=1)).strftime("%d.%m")
        await self.notifier.send(self._msg.tomorrow_cancelled(tomorrow_date))
        logger.info("✅ Tomorrow outages cancelled")

        await self._update_pinned_if_changed(new_state.today, now, is_tomorrow=False)
        await self._check_today_changes(new_state, now, current_minute, False)

    async def _handle_tomorrow_changed(
        self, new_state: ScheduleState, now: datetime, current_minute: int
    ) -> None:
        tomorrow_date = (now + timedelta(days=1)).strftime("%d.%m")
        schedule_msg = self._msg.format_schedule(tomorrow_date, new_state.tomorrow)
        await self.notifier.edit_pinned_message(schedule_msg)

        next_period = new_state.tomorrow.find_next_period(0)
        next_outage_str = (
            f"tomorrow {format_period(next_period[0], next_period[1])}"
            if next_period
            else None
        )
        await self.notifier.send_reply_to_pinned(
            self._msg.schedule_updated("Tomorrow", tomorrow_date, next_outage_str)
        )

        await self._check_today_changes(new_state, now, current_minute, True)
        logger.info(f"📅 Tomorrow schedule changed: {tomorrow_date}")

    async def _update_pinned_if_changed(
        self, schedule: DtekDaySchedule, now: datetime, is_tomorrow: bool
    ) -> None:
        saved_schedule = self._state.tomorrow if is_tomorrow else self._state.today
        if not self._slots_differ(schedule, saved_schedule):
            return

        date_str = (
            (now + timedelta(days=1)).strftime("%d.%m")
            if is_tomorrow
            else now.strftime("%d.%m")
        )
        schedule_msg = self._msg.format_schedule(date_str, schedule)
        await self.notifier.edit_pinned_message(schedule_msg)
        logger.info(f"📌 Updated pinned with {'tomorrow' if is_tomorrow else 'today'} schedule")

    async def _check_today_changes(
        self,
        new_state: ScheduleState,
        now: datetime,
        current_minute: int,
        tomorrow_has_outages: bool,
    ) -> None:
        if not self._slots_differ(new_state.today, self._state.today):
            return

        is_new_day = self._is_new_day(new_state)
        if is_new_day and self._state.tomorrow and not self._slots_differ(
            new_state.today, self._state.tomorrow
        ):
            logger.debug("Day shifted, schedule unchanged")
            return

        power_is_off = self.power_monitor.is_power_off()

        if power_is_off:
            await self._handle_schedule_change_power_off(
                new_state, current_minute, tomorrow_has_outages
            )
        else:
            await self._handle_schedule_change_power_on(
                new_state, current_minute, tomorrow_has_outages
            )

    async def _handle_schedule_change_power_off(
        self,
        new_state: ScheduleState,
        current_minute: int,
        tomorrow_has_outages: bool,
    ) -> None:
        old_slot_end = (
            self._state.today.get_current_outage_end(current_minute)
            if self._state.today
            else None
        )
        new_slot_end = (
            new_state.today.get_current_outage_end(current_minute)
            if new_state.today
            else None
        )

        if old_slot_end != new_slot_end and new_slot_end:
            slot_end_str = minutes_to_time(new_slot_end)
            await self.notifier.send(self._msg.slot_end_updated(slot_end_str))
            logger.info(f"🕐 Slot end changed: {slot_end_str}")
        else:
            logger.info("📅 Schedule changed during outage, but current slot unchanged")

    async def _handle_schedule_change_power_on(
        self,
        new_state: ScheduleState,
        current_minute: int,
        tomorrow_has_outages: bool,
    ) -> None:
        current_next = new_state.today.find_next_period(current_minute)
        saved_next = (
            self._state.today.find_next_period(current_minute)
            if self._state.today
            else None
        )

        logger.info(f"📅 Schedule changed: next period {saved_next} -> {current_next}")

        if current_next == saved_next:
            return

        date_str = new_state.today.date
        next_outage_str = format_period(*current_next) if current_next else None
        msg = self._msg.schedule_updated("Today", date_str, next_outage_str)

        if tomorrow_has_outages:
            await self.notifier.send(msg)
        else:
            await self.notifier.send_reply_to_pinned(msg)

        logger.info("📅 Sent today schedule change notification")

    def _get_next_outage_string(
        self,
        today: Optional[DtekDaySchedule],
        tomorrow: Optional[DtekDaySchedule],
        current_minute: int,
    ) -> Optional[str]:
        if today:
            next_today = today.find_next_period(current_minute)
            if next_today:
                start, end = next_today
                if end == MINUTES_IN_DAY:
                    periods = today.get_outage_periods()
                    for i, (p_start, p_end) in enumerate(periods):
                        if p_start == start and p_end == MINUTES_IN_DAY:
                            if i + 1 < len(periods) and periods[i + 1][0] == 0:
                                end = periods[i + 1][1]
                            break
                return f"{minutes_to_time(start)} - {minutes_to_time(end)}"

        if tomorrow:
            next_tomorrow = tomorrow.find_next_period(0)
            if next_tomorrow:
                return f"tomorrow {format_period(*next_tomorrow)}"

        return None

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
