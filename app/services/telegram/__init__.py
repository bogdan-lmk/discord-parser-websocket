from ..telegram.message_sender import MessageSender
from ..telegram.topic_manager import TopicManager
from ..telegram.bot_handlers import BotHandlers

from ..models.message import DiscordMessage
from ..config import Settings
from ..utils.rate_limiter import RateLimiter

class TelegramService(TopicManager, MessageSender, BotHandlers):
    """Facade service combining topic management, message sending and bot handlers."""

    def __init__(self, settings: Settings, rate_limiter: RateLimiter, redis_client=None, logger=None):
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.redis_client = redis_client
        self.logger = logger
        self.bot = None
        self.server_topics = {}
        self.topic_name_cache = {}
        self.user_states = {}
        self.message_mappings = {}
        self.processed_messages = {}
        self.startup_verification_done = False
        self.bot_running = False
        self.discord_service = None

        self._initialize_bot()
        self._setup_bot_handlers()

    def _initialize_bot(self):
        import telebot
        if not self.settings.telegram_bot_token:
            return False
        self.bot = telebot.TeleBot(self.settings.telegram_bot_token, parse_mode=None, threaded=False)
        return True

    def set_discord_service(self, discord_service):
        self.discord_service = discord_service

    def start_bot_async(self):
        if not self.bot:
            return
        self.bot.polling(none_stop=True, interval=1, timeout=20, skip_pending=True)

    def stop_bot(self):
        if self.bot:
            self.bot.stop_polling()

    async def cleanup(self):
        self.stop_bot()
        self.startup_verification_done = False
        self.user_states.clear()
        self.message_mappings.clear()

    def get_enhanced_stats(self):
        return {
            "topics": len(self.server_topics),
            "messages": len(self.message_mappings),
            "bot_running": self.bot_running,
        }
