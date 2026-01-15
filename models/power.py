from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class PowerStatus(Enum):
    OK = "OK"
    ALARM = "ALARM"


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
