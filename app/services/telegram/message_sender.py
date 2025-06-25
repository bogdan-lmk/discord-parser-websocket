from typing import List, Optional
from app.models.message import DiscordMessage

class MessageSender:
    """Utility mixin for sending messages to Telegram."""

    async def send_message(self, message: DiscordMessage) -> bool:
        """Send a single Discord message to Telegram."""
        topic_id = await self.get_or_create_server_topic(message.server_name)
        self.bot.send_message(
            chat_id=self.settings.telegram_chat_id,
            text=message.content,
            message_thread_id=topic_id if self.settings.use_topics else None,
            parse_mode=None,
        )
        return True

    async def send_messages_batch(self, messages: List[DiscordMessage]) -> int:
        """Send a batch of Discord messages."""
        count = 0
        for msg in messages:
            if await self.send_message(msg):
                count += 1
        return count
