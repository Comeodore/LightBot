import asyncio
import logging
import time
from typing import Optional, AsyncIterator

import aiohttp

logger = logging.getLogger(__name__)


class HomeAssistantClient:
    AUTH_TIMEOUT = 10
    HEARTBEAT_INTERVAL = 30
    WS_CLOSE_TIMEOUT = 30

    def __init__(self, url: str, token: str):
        self.url = url
        self.token = token
        self.session: Optional[aiohttp.ClientSession] = None
        self.ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._message_id = 1

    async def connect(self) -> None:
        await self.close()
        self._message_id = 1

        timeout = aiohttp.ClientTimeout(total=60)
        connector = aiohttp.TCPConnector(limit=10, keepalive_timeout=30)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)

        try:
            self.ws = await self.session.ws_connect(
                self.url,
                heartbeat=self.HEARTBEAT_INTERVAL,
                timeout=aiohttp.ClientWSTimeout(ws_close=self.WS_CLOSE_TIMEOUT),
                compress=15,
            )

            auth_msg = await asyncio.wait_for(
                self.ws.receive_json(), timeout=self.AUTH_TIMEOUT
            )
            if auth_msg.get("type") != "auth_required":
                raise ConnectionError(f"Unexpected auth message: {auth_msg.get('type')}")

            await self.ws.send_json({"type": "auth", "access_token": self.token})

            auth_result = await asyncio.wait_for(
                self.ws.receive_json(), timeout=self.AUTH_TIMEOUT
            )
            if auth_result.get("type") != "auth_ok":
                error = auth_result.get("message", "Unknown error")
                raise ConnectionError(f"Authentication failed: {error}")

            logger.info("✅ Connected to Home Assistant")
        except asyncio.TimeoutError:
            await self.close()
            raise ConnectionError("Authentication timeout")
        except Exception as e:
            await self.close()
            raise ConnectionError(f"Failed to connect: {e}") from e

    async def subscribe_events(self) -> None:
        await self._send_message("subscribe_events", event_type="state_changed")
        logger.info("📻 Subscribed to state changes")

    async def get_states(self) -> dict:
        if not self.ws or self.ws.closed:
            raise ConnectionError("WebSocket not connected")

        msg_id = await self._send_message("get_states")

        async for msg in self._receive_with_timeout(timeout=10):
            if msg.get("id") == msg_id:
                if msg.get("success"):
                    return {state["entity_id"]: state for state in msg.get("result", [])}
                else:
                    error = msg.get("error", {}).get("message", "Unknown error")
                    raise RuntimeError(f"Failed to get states: {error}")

        raise TimeoutError("Failed to get states within timeout")

    async def _send_message(self, msg_type: str, **kwargs) -> int:
        if not self.ws or self.ws.closed:
            raise ConnectionError("WebSocket not connected")

        message = {"id": self._message_id, "type": msg_type, **kwargs}
        await self.ws.send_json(message)
        msg_id = self._message_id
        self._message_id += 1
        return msg_id

    async def _receive_with_timeout(self, timeout: float) -> AsyncIterator[dict]:
        start = time.monotonic()
        async for msg in self.receive():
            yield msg
            if time.monotonic() - start > timeout:
                break

    async def receive(self) -> AsyncIterator[dict]:
        if not self.ws:
            return

        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                yield msg.json()
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

    async def close(self) -> None:
        if self.ws and not self.ws.closed:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=5)
            except asyncio.TimeoutError:
                logger.warning("WebSocket close timeout")
            except Exception as e:
                logger.warning(f"Error closing WebSocket: {e}")
            finally:
                self.ws = None

        if self.session and not self.session.closed:
            try:
                await self.session.close()
                await asyncio.sleep(0.25)
            except Exception as e:
                logger.warning(f"Error closing session: {e}")
            finally:
                self.session = None

        logger.info("✅ Home Assistant client closed")
