import asyncio
import logging
from typing import Optional

from telegram import Message
from telegram.ext import Application, AIORateLimiter

from utils.retry import with_retry

logger = logging.getLogger(__name__)


class NotificationService:
    MAX_RETRIES = 3
    RETRY_DELAY = 2.0

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

    async def send(
        self, message: str, pin: bool = False, unpin_all_first: bool = False
    ) -> None:
        if unpin_all_first:
            await self._unpin_all_messages()

        for chat_id in self.chat_ids:
            await self._send_to_chat(chat_id, message, pin=pin)

    async def edit_pinned_message(self, message: str) -> None:
        for chat_id in self.chat_ids:
            pinned_message_id = await self._get_pinned_message_id(chat_id)
            if not pinned_message_id:
                logger.warning(f"No pinned message found in {chat_id}, skipping edit")
                continue

            await self._edit_message(chat_id, pinned_message_id, message)

    async def send_reply_to_pinned(self, message: str) -> None:
        for chat_id in self.chat_ids:
            pinned_message_id = await self._get_pinned_message_id(chat_id)
            if not pinned_message_id:
                logger.warning(
                    f"No pinned message found in {chat_id}, sending regular message"
                )
                await self._send_to_chat(chat_id, message)
                continue

            await self._send_reply(chat_id, message, pinned_message_id)

    async def _unpin_all_messages(self) -> None:
        for chat_id in self.chat_ids:
            try:
                await self._unpin_chat_messages(chat_id)
                logger.info(f"📍 All messages unpinned in {chat_id}")
            except Exception as e:
                logger.warning(f"Failed to unpin messages in {chat_id}: {e}")

    @with_retry(max_retries=3, base_delay=2.0)
    async def _unpin_chat_messages(self, chat_id: str) -> None:
        await self.app.bot.unpin_all_chat_messages(chat_id=chat_id)

    async def _get_pinned_message_id(self, chat_id: str) -> Optional[int]:
        try:
            chat = await self.app.bot.get_chat(chat_id)
            return chat.pinned_message.message_id if chat.pinned_message else None
        except Exception as e:
            logger.error(f"Failed to get chat info for {chat_id}: {e}")
            return None

    @with_retry(max_retries=3, base_delay=2.0)
    async def _edit_message(
        self, chat_id: str, message_id: int, text: str
    ) -> None:
        await self.app.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        logger.info(f"✏️ Pinned message edited in {chat_id}")

    @with_retry(max_retries=3, base_delay=2.0)
    async def _send_reply(
        self, chat_id: str, text: str, reply_to_message_id: int
    ) -> Message:
        message = await self.app.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=True,
            reply_to_message_id=reply_to_message_id,
        )
        logger.info(f"📤 Reply sent to pinned in {chat_id}")
        return message

    @with_retry(max_retries=3, base_delay=2.0)
    async def _send_message(self, chat_id: str, text: str) -> Message:
        return await self.app.bot.send_message(
            chat_id=chat_id,
            text=text,
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )

    async def _send_to_chat(
        self, chat_id: str, message: str, pin: bool = False
    ) -> bool:
        try:
            sent_message = await self._send_message(chat_id, message)
            logger.info(f"📤 Message sent to {chat_id}")

            if pin:
                try:
                    await self.app.bot.pin_chat_message(
                        chat_id=chat_id,
                        message_id=sent_message.message_id,
                        disable_notification=True,
                    )
                    logger.info(f"📌 Message pinned in {chat_id}")
                except Exception as pin_error:
                    logger.warning(f"Failed to pin message in {chat_id}: {pin_error}")

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
