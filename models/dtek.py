from dataclasses import dataclass
from enum import Enum
from typing import Optional

from utils.time_format import MINUTES_IN_DAY, format_period


class DtekCellStatus(Enum):
    POWER_ON = "power_on"
    POWER_OFF = "power_off"
    POWER_OFF_FIRST_30 = "power_off_first_30"
    POWER_OFF_SECOND_30 = "power_off_second_30"


class DtekOutageStatus(Enum):
    POWER_ON = "power_on"
    SCHEDULED = "scheduled"
    EMERGENCY = "emergency"


@dataclass
class DtekCurrentOutage:
    status: DtekOutageStatus
    reason: str = ""
    start_time: str = ""
    restoration_time: str = ""
    last_updated: str = ""

    @classmethod
    def from_dict(cls, data: Optional[dict]) -> Optional["DtekCurrentOutage"]:
        if not data:
            return None
        return cls(
            status=DtekOutageStatus(data.get("status", "power_on")),
            reason=data.get("reason", ""),
            start_time=data.get("start_time", ""),
            restoration_time=data.get("restoration_time", ""),
            last_updated=data.get("last_updated", ""),
        )

    @property
    def is_emergency(self) -> bool:
        return self.status == DtekOutageStatus.EMERGENCY

    @property
    def is_scheduled(self) -> bool:
        return self.status == DtekOutageStatus.SCHEDULED

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
    def from_dict(cls, data: Optional[dict]) -> Optional["DtekDaySchedule"]:
        if not data or "slots" not in data:
            return None
        return cls(
            date=data.get("date", ""),
            slots=tuple(
                DtekHourSlot(hour=s["hour"], status=DtekCellStatus(s["status"]))
                for s in data["slots"]
            ),
            last_updated=data.get("last_updated", ""),
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

    def find_next_period(self, after_minute: int = 0) -> Optional[tuple[int, int]]:
        """Find next outage period that starts after the given minute."""
        for start, end in self.get_outage_periods():
            if start > after_minute:
                return (start, end)
        return None

    def format_periods(self) -> list[str]:
        return [format_period(s, e) for s, e in self.get_outage_periods()]

    def has_outages(self) -> bool:
        return bool(self.get_outage_periods())

    def total_outage_minutes(self) -> int:
        return sum(end - start for start, end in self.get_outage_periods())

    def total_power_minutes(self) -> int:
        return MINUTES_IN_DAY - self.total_outage_minutes()
