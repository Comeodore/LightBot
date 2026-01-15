import asyncio
import json
import logging
from typing import Optional

import asyncpg

from models.power import PowerStatus, PowerState

logger = logging.getLogger(__name__)


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
                server_settings={"application_name": "LightBot"},
            )

            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")

            logger.info("✅ Database connected")
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise

    async def get_last_event(self) -> Optional[PowerState]:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT state, timestamp FROM power_events ORDER BY timestamp DESC LIMIT 1"
                )
                if row:
                    return PowerState(
                        status=PowerStatus(row["state"]), timestamp=row["timestamp"]
                    )
                return None
        except Exception as e:
            logger.error(f"Failed to get last event: {e}")
            raise

    async def save_event(self, state: PowerState) -> None:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE power_events SET state = $1, timestamp = $2 WHERE id = 1",
                    state.state,
                    state.timestamp,
                )
            logger.info(f"💾 Saved: {state.state} at {state.timestamp}")
        except Exception as e:
            logger.error(f"Failed to save event: {e}")
            raise

    async def get_dtek_schedule(self) -> Optional[dict]:
        if not self.pool:
            raise RuntimeError("Database pool not initialized")

        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    "SELECT current_outage, today_schedule, tomorrow_schedule, updated_at "
                    "FROM dtek_schedule WHERE id = 1"
                )
                if row:
                    return {
                        "current_outage": self._parse_json(row["current_outage"]),
                        "today": self._parse_json(row["today_schedule"]),
                        "tomorrow": self._parse_json(row["tomorrow_schedule"]),
                        "updated_at": row["updated_at"],
                    }
                return None
        except Exception as e:
            logger.error(f"Failed to get dtek schedule: {e}")
            raise

    @staticmethod
    def _parse_json(value) -> Optional[dict]:
        if value is None:
            return None
        if isinstance(value, str):
            if not value:
                return None
            try:
                return json.loads(value)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
                return None
        return value

    async def close(self) -> None:
        if self.pool:
            try:
                await asyncio.wait_for(self.pool.close(), timeout=10)
                logger.info("💾 Database closed")
            except asyncio.TimeoutError:
                logger.warning("Database close timeout")
            except Exception as e:
                logger.warning(f"Error closing database: {e}")
            finally:
                self.pool = None
