
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import List, Dict, Optional
from functools import lru_cache
import os
import logging
from dotenv import load_dotenv

# 1: Перезагружаем .env при каждом вызове
def reload_env():
    """Принудительная перезагрузка переменных окружения"""
    load_dotenv(override=True)  # override=True заставляет перезагрузить

class Settings(BaseSettings):
    """Application settings с автоматической перезагрузкой конфигурации"""
    
    # Application Settings
    app_name: str = "Discord Telegram Parser MVP"
    app_version: str = "2.1.0"
    debug: bool = Field(default=False, env="DEBUG")
    
    # Discord Configuration
    discord_auth_tokens: str = Field(..., env="DISCORD_AUTH_TOKENS")
    
    @property
    def discord_tokens(self) -> List[str]:
        """Parse Discord tokens from the environment variable"""
        if isinstance(self.discord_auth_tokens, str):
            tokens = [token.strip() for token in self.discord_auth_tokens.split(',') if token.strip()]
            return tokens
        return []
    
    # Telegram Configuration  
    telegram_bot_token: str = Field(..., env="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: int = Field(..., env="TELEGRAM_CHAT_ID")
    
    # Server/Channel Mappings (will be populated dynamically)
    server_channel_mappings: Dict[str, Dict[str, str]] = Field(default_factory=dict)
    
    # Лимиты 
    max_channels_per_server: int = Field(default=10, ge=1, le=50, env="MAX_CHANNELS_PER_SERVER")
    max_total_channels: int = Field(default=1000, ge=50, le=2000, env="MAX_TOTAL_CHANNELS")
    max_servers: int = Field(default=200, ge=10, le=500, env="MAX_SERVERS")
    
    # Rate Limiting
    discord_rate_limit_per_second: float = Field(default=1.5, ge=0.5, le=5.0, env="DISCORD_RATE_LIMIT_PER_SECOND")
    telegram_rate_limit_per_minute: int = Field(default=30, ge=5, le=100, env="TELEGRAM_RATE_LIMIT_PER_MINUTE")
    
    # Message Processing
    max_history_messages: int = Field(default=100, ge=10, le=500)
    max_concurrent_server_processing: int = Field(default=15, ge=5, le=50, env="MAX_CONCURRENT_SERVER_PROCESSING")
    server_discovery_batch_size: int = Field(default=20, ge=10, le=50, env="SERVER_DISCOVERY_BATCH_SIZE")
    
    # Message TTL for deduplication
    message_ttl_seconds: int = Field(
        default=86400,  # 1 day by default
        ge=3600,        # minimum 1 hour
        le=604800,      # maximum 1 week
        description="TTL for message deduplication in Redis"
    )
    
    # WebSocket Configuration
    websocket_heartbeat_interval: int = Field(default=45000, ge=30000)
    websocket_reconnect_delay: int = Field(default=60, ge=5, le=300)
    websocket_max_retries: int = Field(default=3, ge=1, le=10)
    
    # Memory Management
    cleanup_interval_minutes: int = Field(default=10, ge=1, le=60)
    max_memory_mb: int = Field(default=4096, ge=512, le=8192)
    
    # Telegram UI Preferences
    use_topics: bool = Field(default=True, env="TELEGRAM_USE_TOPICS")
    show_timestamps: bool = Field(default=True)
    show_server_in_message: bool = Field(default=True)

    # Channel Configuration
    channel_keywords: List[str] = Field(
        default=['announcement', 'announcements', 'announce', 'updates', 'news'],
        description="Keywords to identify announcement channels"
    )
    
    # Monitoring & Logging
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    enable_metrics: bool = Field(default=True)
    metrics_port: int = Field(default=9090, ge=1024, le=65535)
    
    # Redis Configuration (for caching)
    redis_url: Optional[str] = Field(default=None, env="REDIS_URL")
    cache_ttl_seconds: int = Field(default=600, ge=60, le=3600)
    
    # Health Check Configuration
    health_check_interval: int = Field(default=120, ge=30, le=300)
    
    # Специальные настройки для большого количества серверов
    channel_test_timeout: int = Field(default=5, ge=2, le=15, env="CHANNEL_TEST_TIMEOUT")
    
    @field_validator('discord_auth_tokens')
    @classmethod
    def validate_discord_tokens(cls, v):
        """Валидация пользовательских Discord токенов (не bot токенов)"""
        if isinstance(v, str):
            # Разделяем токены по запятым и очищаем
            tokens = []
            for token in v.split(','):
                clean_token = token.strip()
                if clean_token:  # Пропускаем пустые токены
                    # Убираем префикс "Bot " если кто-то случайно добавил
                    if clean_token.startswith('Bot '):
                        clean_token = clean_token[4:].strip()
                        print(f"⚠️ Удален префикс 'Bot ' из токена - используются пользовательские токены")
                    
                    if clean_token:
                        tokens.append(clean_token)
        else:
            tokens = v if isinstance(v, list) else []
        
        if not tokens or len(tokens) == 0:
            raise ValueError('At least one Discord user token is required')
        
        for i, token in enumerate(tokens):
            if not token or len(token.strip()) < 50:
                raise ValueError(f'Invalid Discord user token at position {i+1}: token too short (expected 70+ characters)')
            
            # Пользовательские Discord токены обычно начинаются с определенных префиксов
            # и имеют другой формат чем bot токены
            
            # Проверяем что это не явно bot токен (они имеют 3 части через точки)
            if '.' in token and len(token.split('.')) == 3:
                # Это может быть bot токен, предупреждаем
                try:
                    import base64
                    parts = token.split('.')
                    decoded = base64.b64decode(parts[0] + '===')
                    if decoded.decode('utf-8').isdigit():
                        raise ValueError(f'Token at position {i+1} appears to be a bot token. Please use user tokens instead.')
                except:
                    pass  # Если не удалось декодировать, продолжаем
            
            # Базовая проверка длины и символов
            if len(token) < 50 or len(token) > 100:
                raise ValueError(f'Invalid token length at position {i+1}: expected 50-100 characters, got {len(token)}')
            
            # Проверяем что токен содержит только допустимые символы
            allowed_chars = set('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.')
            if not all(c in allowed_chars for c in token):
                raise ValueError(f'Token at position {i+1} contains invalid characters')
        
        # Возвращаем очищенные токены как строку
        return ','.join(tokens)
    
    @field_validator('telegram_chat_id')
    @classmethod
    def validate_telegram_chat_id(cls, v):
        """Validate Telegram chat ID"""
        if v == 0:
            raise ValueError('Telegram chat ID cannot be 0')
        return v
    
    @field_validator('max_total_channels')
    @classmethod
    def validate_channel_limits(cls, v, info):
        """Гибкая валидация лимитов каналов"""
        if info.data:
            max_per_server = info.data.get('max_channels_per_server', 5)
            max_servers = info.data.get('max_servers', 50)
            
            theoretical_max = max_per_server * max_servers
            
            # Предупреждаем, но не блокируем, если лимит слишком низкий
            if v < max_per_server:
                logger = logging.getLogger(__name__)
                logger.warning(
                    f'max_total_channels ({v}) is less than max_channels_per_server ({max_per_server}). '
                    f'This may limit functionality.'
                )
            
            # Предупреждаем, если лимит намного превышает теоретический максимум
            if v > theoretical_max * 2:
                logger = logging.getLogger(__name__)
                logger.warning(
                    f'max_total_channels ({v}) is much higher than theoretical max ({theoretical_max}). '
                    f'Consider adjusting server or per-server limits.'
                )
        
        return v
    
    @property
    def discord_tokens_count(self) -> int:
        """Number of available Discord tokens"""
        return len(self.discord_tokens)
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return not self.debug
    
    @property
    def effective_max_servers(self) -> int:
        """Calculate effective max servers based on channel limits"""
        if self.max_channels_per_server > 0:
            return min(self.max_servers, self.max_total_channels // self.max_channels_per_server)
        return self.max_servers
    
    @property
    def log_config(self) -> dict:
        """Structured logging configuration"""
        return {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "json": {
                    "()": "structlog.stdlib.ProcessorFormatter",
                    "processor": "structlog.dev.ConsoleRenderer" if self.debug else "structlog.processors.JSONRenderer",
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "json",
                },
                "file": {
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": f"logs/{self.app_name.lower().replace(' ', '_')}.log",
                    "maxBytes": 10485760,  # 10MB
                    "backupCount": 5,
                    "formatter": "json",
                },
            },
            "loggers": {
                "": {
                    "handlers": ["console", "file"],
                    "level": self.log_level,
                    "propagate": True,
                },
            },
        }
    
    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "allow",  # Allow extra fields for dynamic server mappings
        "env_prefix": "",
        "populate_by_name": True
    }

# 2: Кеш с возможностью сброса
_settings_cache = None
_env_file_mtime = None

def clear_settings_cache():
    """Очистить кеш настроек (полезно для hot reload)"""
    global _settings_cache, _env_file_mtime
    _settings_cache = None
    _env_file_mtime = None
    print("🔄 Settings cache cleared")

def get_settings(force_reload: bool = False) -> Settings:
    """Get cached settings instance с улучшенной обработкой ошибок"""
    global _settings_cache, _env_file_mtime
    
    env_file_path = ".env"
    current_mtime = None
    
    # Проверяем время модификации .env файла
    try:
        if os.path.exists(env_file_path):
            current_mtime = os.path.getmtime(env_file_path)
    except Exception as e:
        print(f"⚠️ Warning: Could not check .env file modification time: {e}")
    
    # Принудительная перезагрузка или изменился .env файл
    if force_reload or _settings_cache is None or current_mtime != _env_file_mtime:
        print(f"🔄 {'Force reload' if force_reload else 'Auto reload'} - creating new settings instance")
        
        try:
            # Перезагружаем переменные окружения
            reload_env()
            
            # Создаем новый экземпляр настроек с обработкой ошибок
            _settings_cache = Settings()
            _env_file_mtime = current_mtime
            
            print(f"✅ New settings loaded:")
            print(f"   • Discord tokens: {_settings_cache.discord_tokens_count}")
            print(f"   • Telegram chat ID: {_settings_cache.telegram_chat_id}")
            
            # ИСПРАВЛЕНИЕ: Безопасная проверка токена
            if _settings_cache.telegram_bot_token:
                token_preview = _settings_cache.telegram_bot_token[:15] + "..." if len(_settings_cache.telegram_bot_token) > 15 else _settings_cache.telegram_bot_token
                print(f"   • Bot token preview: {token_preview}")
            else:
                print(f"   • Bot token: NOT SET")
            
            print(f"   • Use topics: {_settings_cache.use_topics}")
            
        except Exception as settings_error:
            print(f"❌ Error creating settings: {settings_error}")
            print(f"   Error type: {type(settings_error).__name__}")
            
            # Если есть старые настройки, используем их
            if _settings_cache is not None:
                print(f"⚠️ Using cached settings due to error")
                return _settings_cache
            else:
                # Критическая ошибка - не можем создать настройки
                print(f"💥 Critical: Cannot create settings and no cache available")
                raise RuntimeError(f"Failed to load settings: {settings_error}")
    
    return _settings_cache

def validate_settings(settings: Settings) -> tuple[bool, list[str]]:
    """Валидация настроек с подробными сообщениями об ошибках"""
    errors = []
    
    # Проверка Discord токенов
    if not settings.discord_tokens:
        errors.append("DISCORD_AUTH_TOKENS: No tokens provided")
    else:
        for i, token in enumerate(settings.discord_tokens):
            if not token or len(token.strip()) < 50:
                errors.append(f"DISCORD_AUTH_TOKENS: Token {i+1} is too short (expected 50+ characters)")
            elif token.startswith('Bot '):
                errors.append(f"DISCORD_AUTH_TOKENS: Token {i+1} appears to be a bot token (should be user token)")
    
    # Проверка Telegram настроек
    if not settings.telegram_bot_token:
        errors.append("TELEGRAM_BOT_TOKEN: Not provided")
    else:
        token_parts = settings.telegram_bot_token.split(':')
        if len(token_parts) != 2 or not token_parts[0].isdigit():
            errors.append("TELEGRAM_BOT_TOKEN: Invalid format (should be NUMBER:STRING)")
    
    if not settings.telegram_chat_id:
        errors.append("TELEGRAM_CHAT_ID: Not provided")
    elif settings.telegram_chat_id == 0:
        errors.append("TELEGRAM_CHAT_ID: Cannot be 0")
    
    # Проверка лимитов
    if settings.max_channels_per_server < 1:
        errors.append("MAX_CHANNELS_PER_SERVER: Must be at least 1")
    
    if settings.max_total_channels < settings.max_channels_per_server:
        errors.append("MAX_TOTAL_CHANNELS: Must be at least MAX_CHANNELS_PER_SERVER")
    
    # Проверка rate limiting
    if settings.discord_rate_limit_per_second < 0.1:
        errors.append("DISCORD_RATE_LIMIT_PER_SECOND: Must be at least 0.1")
    
    if settings.telegram_rate_limit_per_minute < 1:
        errors.append("TELEGRAM_RATE_LIMIT_PER_MINUTE: Must be at least 1")
    
    return len(errors) == 0, errors


# 3: Функции для принудительной перезагрузки
def reload_settings():
    """Принудительная перезагрузка настроек"""
    return get_settings(force_reload=True)

def get_fresh_settings() -> Settings:
    """Получить всегда свежие настройки (без кеша)"""
    reload_env()
    return Settings()

# 4: Дебаг функция для проверки текущих настроек
def debug_current_settings():
    """Показать текущие настройки для отладки"""
    settings = get_settings()
    
    print("🔍 Current Settings Debug:")
    print("=" * 40)
    print(f"Discord tokens count: {settings.discord_tokens_count}")
    print(f"Discord tokens preview: {[t[:10] + '...' for t in settings.discord_tokens]}")
    print(f"Telegram bot token: {settings.telegram_bot_token[:15]}...")
    print(f"Telegram chat ID: {settings.telegram_chat_id}")
    print(f"Use topics: {settings.use_topics}")
    print(f"Debug mode: {settings.debug}")
    print(f"Log level: {settings.log_level}")
    
    # Проверим также переменные окружения напрямую
    print("\n🔍 Environment Variables:")
    print("=" * 40)
    print(f"DISCORD_AUTH_TOKENS: {os.getenv('DISCORD_AUTH_TOKENS', 'NOT SET')[:20]}...")
    print(f"TELEGRAM_BOT_TOKEN: {os.getenv('TELEGRAM_BOT_TOKEN', 'NOT SET')[:15]}...")
    print(f"TELEGRAM_CHAT_ID: {os.getenv('TELEGRAM_CHAT_ID', 'NOT SET')}")
    print(f"TELEGRAM_USE_TOPICS: {os.getenv('TELEGRAM_USE_TOPICS', 'NOT SET')}")
    
    return settings

