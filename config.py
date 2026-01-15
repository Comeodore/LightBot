import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


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
    def from_env(cls) -> "Config":
        required_vars = ["WS_URL", "HA_TOKEN", "BOT_TOKEN", "CHAT_IDS", "DATABASE_URL"]
        missing = [var for var in required_vars if not os.getenv(var)]
        if missing:
            raise ValueError(f"Missing environment variables: {', '.join(missing)}")

        return cls(
            ws_url=os.getenv("WS_URL"),
            ha_token=os.getenv("HA_TOKEN"),
            bot_token=os.getenv("BOT_TOKEN"),
            chat_ids=tuple(
                c.strip() for c in os.getenv("CHAT_IDS", "").split(",") if c.strip()
            ),
            database_url=os.getenv("DATABASE_URL"),
            voltage_entity="sensor.victron_vebus_activein_l1_voltage_228",
            battery_entity="sensor.victron_battery_soc",
        )
