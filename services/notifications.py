import logging

from telegram import Message
from telegram.ext import Application, AIORateLimiter

from utils.retry import with_retry

logger = logging.getLogger(__name__)


class NotificationService:
    def __init__(self, bot_token: str, chat_ids: tuple[str, ...]):
        self.chat_ids = chat_ids
        self.app = (
            Application.builder()
            .token(bot_token)
            .rate_limiter(AIORateLimiter(max_retries=3))
            .build()
        )

    async def start(self) -> None:
        try:
            await self.app.initialize()
            await self.app.start()
            logger.info("✅ Notification service started")
        except Exception as e:
            logger.error(f"Failed to start notification service: {e}")
            raise

    async def send(self, message: str) -> None:
        for chat_id in self.chat_ids:
            await self._send_to_chat(chat_id, message)

    @with_retry(max_retries=3, base_delay=2.0)
    async def _send_message(self, chat_id: str, text: str) -> Message:
        return await self.app.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

    async def _send_to_chat(self, chat_id: str, message: str) -> bool:
        try:
            await self._send_message(chat_id, message)
            logger.info(f"📤 Message sent to {chat_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to send to {chat_id}: {e}")
            return False

    async def stop(self) -> None:
        try:
            await self.app.stop()
            await self.app.shutdown()
            logger.info("✅ Notification service stopped")
        except Exception as e:
            logger.warning(f"Error stopping notification service: {e}")
