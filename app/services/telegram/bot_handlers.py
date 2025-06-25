class BotHandlers:
    """Setup Telegram bot command and callback handlers."""

    def _setup_bot_handlers(self):
        if not self.bot:
            return

        @self.bot.message_handler(commands=["start", "help"])
        def start(message):
            self.bot.reply_to(message, "Bot is running")
