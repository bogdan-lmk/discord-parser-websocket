# app/services/message_processor.py - ОБНОВЛЕННЫЙ для WebSocket
import asyncio
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Set, Any
import structlog

from ..models.message import DiscordMessage
from ..models.server import SystemStats, ServerStatus
from ..config import Settings
from .discord_service import DiscordService
from .telegram_service import TelegramService

class MessageProcessor:
    """Главный оркестратор - WebSocket ONLY (БЕЗ дублирования сообщений)"""
    
    def __init__(self,
                 settings: Settings,
                 discord_service: DiscordService,
                 telegram_service: TelegramService,
                 redis_client = None,
                 logger = None):
        self.settings = settings
        self.discord_service = discord_service
        self.telegram_service = telegram_service
        self.logger = logger or structlog.get_logger(__name__)
        
        # State management
        self.running = False
        self.start_time = datetime.now()
        
        # Statistics
        self.stats = SystemStats()
        
        # Background tasks
        self.tasks: List[asyncio.Task] = []
        
        # WebSocket message queue
        self.message_queue = asyncio.Queue(maxsize=1000)
        self.redis_client = redis_client
        
        # Channel initialization tracking
        self.last_processed_message_per_channel: Dict[str, datetime] = {}  # channel_id -> last_timestamp
        self.channel_initialization_done: Set[str] = set()  # каналы, для которых выполнена начальная инициализация
        
        # WebSocket-only дедупликация
        self.processed_message_hashes: Set[str] = set()  # Только хэши уже обработанных сообщений
        self.message_dedup_lock = asyncio.Lock()
        
        # Real-time synchronization state
        self.realtime_enabled = True
        
        # Server statistics
        self.server_message_counts: Dict[str, int] = {}  # server_name -> count
        self.server_last_activity: Dict[str, datetime] = {}  # server_name -> timestamp
        
        # Message rate tracking
        self.message_rate_tracker: Dict[str, List[datetime]] = {}  # server -> timestamps
        
        # Periodic cleanup
        self.last_cleanup = datetime.now()
        
        # Флаг завершения инициализации
        self.initial_sync_completed = False
        
        # WebSocket monitoring stats
        self.websocket_messages_received = 0
        self.websocket_messages_processed = 0
        self.websocket_last_message_time = None
        
    async def initialize(self) -> bool:
        """Initialize all services with enhanced error handling"""
        self.logger.info("🚀 Initializing Message Processor with WebSocket-only system")
        
        initialization_errors = []
        
        # Initialize Discord service with retry logic
        discord_initialized = False
        for attempt in range(3):
            try:
                self.logger.info(f"Initializing Discord service (attempt {attempt + 1}/3)")
                if await self.discord_service.initialize():
                    discord_initialized = True
                    self.logger.info("✅ Discord service initialized successfully")
                    break
                else:
                    error_msg = f"Discord service initialization returned False (attempt {attempt + 1})"
                    self.logger.warning(error_msg)
                    initialization_errors.append(error_msg)
            except Exception as e:
                error_msg = f"Discord service initialization error (attempt {attempt + 1}): {e}"
                self.logger.error(error_msg, error_type=type(e).__name__)
                initialization_errors.append(error_msg)
            
            if attempt < 2:  # Wait before retry
                await asyncio.sleep(5)
        
        if not discord_initialized:
            self.logger.error("❌ Discord service initialization failed after all attempts")
            self.logger.error("Discord initialization errors:")
            for error in initialization_errors[-3:]:  # Show last 3 errors
                self.logger.error(f"  • {error}")
            return False
        
        # Initialize Telegram service with retry logic
        telegram_initialized = False
        for attempt in range(3):
            try:
                self.logger.info(f"Initializing Telegram service (attempt {attempt + 1}/3)")
                if await self.telegram_service.initialize():
                    telegram_initialized = True
                    self.logger.info("✅ Telegram service initialized successfully")
                    break
                else:
                    error_msg = f"Telegram service initialization returned False (attempt {attempt + 1})"
                    self.logger.warning(error_msg)
                    initialization_errors.append(error_msg)
            except Exception as e:
                error_msg = f"Telegram service initialization error (attempt {attempt + 1}): {e}"
                self.logger.error(error_msg, error_type=type(e).__name__)
                initialization_errors.append(error_msg)
            
            if attempt < 2:  # Wait before retry
                await asyncio.sleep(3)
        
        if not telegram_initialized:
            self.logger.error("❌ Telegram service initialization failed after all attempts")
            self.logger.error("Telegram initialization errors:")
            for error in initialization_errors[-3:]:  # Show last 3 errors
                self.logger.error(f"  • {error}")
            return False
        
        # Set Discord service reference for channel management
        try:
            self.telegram_service.set_discord_service(self.discord_service)
            try:
                self.telegram_service.set_discord_service(self.discord_service)
                
                # ПРИНУДИТЕЛЬНОЕ создание топиков для всех серверов
                if hasattr(self.telegram_service, 'create_topics_for_all_servers'):
                    created_topics = await self.telegram_service.create_topics_for_all_servers()
                    
                    server_count = len(self.discord_service.servers)
                    topic_count = len(created_topics)
                    
                    self.logger.info("Topic creation results:")
                    self.logger.info(f"  Servers: {server_count}")
                    self.logger.info(f"  Topics created: {topic_count}")
                    
                    if topic_count < server_count:
                        missing = server_count - topic_count
                        self.logger.warning(f"  Missing topics: {missing}")
                    else:
                        self.logger.info("  Perfect coverage: ALL servers have topics")
                    
            except Exception as e:
                self.logger.error(f"Error in topic creation: {e}")
            self.logger.info("✅ Discord-Telegram service integration established")
        except Exception as e:
            self.logger.error("❌ Error setting Discord service reference", error=str(e))
            # Not critical, continue
        
        # Register WebSocket message callback
        try:
            self.discord_service.add_message_callback(self._handle_websocket_message)
            self.logger.info("✅ WebSocket message callback registered")
        except Exception as e:
            self.logger.error("❌ Error registering WebSocket callback", error=str(e))
            # Not critical for basic functionality
        
        # Initialize server tracking
        try:
            for server_name in self.discord_service.servers.keys():
                self.server_message_counts[server_name] = 0
                self.server_last_activity[server_name] = datetime.now()
            self.logger.info(f"✅ Server tracking initialized for {len(self.discord_service.servers)} servers")
        except Exception as e:
            self.logger.error("❌ Error initializing server tracking", error=str(e))
            # Initialize empty tracking
            self.server_message_counts = {}
            self.server_last_activity = {}
        
        # Update initial statistics
        try:
            await self._update_stats()
            self.logger.info("✅ Initial statistics updated")
        except Exception as e:
            self.logger.warning("⚠️ Error updating initial statistics", error=str(e))
            # Not critical
        
        self.logger.info("✅ Message Processor initialized successfully",
                        discord_servers=len(self.discord_service.servers),
                        telegram_topics=len(self.telegram_service.server_topics),
                        realtime_enabled=self.realtime_enabled,
                        monitored_channels=len(self.discord_service.monitored_announcement_channels),
                        mode="WebSocket-only",
                        anti_duplication="ACTIVE")
        
        return True
    
    async def _handle_websocket_message(self, message: DiscordMessage) -> None:
        """Handle WebSocket message with enhanced error handling"""
        try:
            self.websocket_messages_received += 1
            self.websocket_last_message_time = datetime.now()
            
            # Проверяем что сообщение не None и имеет необходимые атрибуты
            if not message:
                self.logger.warning("Received None message from WebSocket")
                return
            
            required_attrs = ['channel_id', 'server_name', 'channel_name', 'timestamp']
            missing_attrs = [attr for attr in required_attrs if not hasattr(message, attr)]
            if missing_attrs:
                self.logger.warning("Message missing required attributes", 
                                missing=missing_attrs)
                return
            
            # Check if channel is monitored
            if not hasattr(self.discord_service, 'monitored_announcement_channels'):
                self.logger.warning("Discord service missing monitored_announcement_channels")
                return
            
            if message.channel_id not in self.discord_service.monitored_announcement_channels:
                self.logger.debug("🚫 Non-monitored channel message ignored",
                                server=message.server_name,
                                channel=message.channel_name)
                return
            
            # Check if channel initialized (initial sync must be complete)
            if not self.initial_sync_completed or message.channel_id not in self.channel_initialization_done:
                self.logger.debug("⏳ Channel not initialized yet, queuing message",
                                server=message.server_name,
                                channel=message.channel_name)
                
                # Queue for processing after initialization
                try:
                    await asyncio.wait_for(self.message_queue.put(message), timeout=2.0)
                except asyncio.TimeoutError:
                    self.logger.warning("⚠️ Queue timeout during initialization for message from",
                                    server=message.server_name)
                except Exception as queue_error:
                    self.logger.error("Error queuing message during initialization",
                                    error=str(queue_error))
                return
            
            # Check if message is newer than last processed
            last_processed = self.last_processed_message_per_channel.get(message.channel_id)
            if last_processed and message.timestamp <= last_processed:
                self.logger.debug("🔄 Message not newer than last processed, skipping",
                                channel_id=message.channel_id,
                                message_timestamp=message.timestamp.isoformat(),
                                last_processed_timestamp=last_processed.isoformat())
                return
            
            # Deduplication check
            try:
                message_hash = self._create_message_hash(message)
                async with self.message_dedup_lock:
                    if message_hash in self.processed_message_hashes:
                        self.logger.debug("🔂 Duplicate WebSocket message ignored",
                                        message_hash=message_hash[:8],
                                        server=message.server_name)
                        return
                    self.processed_message_hashes.add(message_hash)
            except Exception as dedup_error:
                self.logger.error("Error in deduplication check", error=str(dedup_error))
                # Continue processing despite deduplication error
            
            # Rate limit check
            try:
                if not self._check_rate_limit(message.server_name, is_realtime=True):
                    self.logger.warning("🚦 Rate limit exceeded for WebSocket message", 
                                    server=message.server_name)
                    return
            except Exception as rate_error:
                self.logger.error("Error in rate limit check", error=str(rate_error))
                # Continue processing despite rate limit error
            
            # Queue the message for processing
            try:
                await asyncio.wait_for(self.message_queue.put(message), timeout=3.0)
                self.last_processed_message_per_channel[message.channel_id] = message.timestamp
                self._update_rate_tracking(message.server_name)
                self.server_last_activity[message.server_name] = datetime.now()
                
                channel_type = "announcement" if self._is_announcement_channel(message.channel_name) else "regular"
                self.logger.info("📨 NEW WebSocket message queued", 
                            server=message.server_name,
                            channel=message.channel_name,
                            channel_type=channel_type,
                            author=getattr(message, 'author', 'Unknown'),
                            queue_size=self.message_queue.qsize(),
                            message_hash=message_hash[:8] if 'message_hash' in locals() else 'unknown')
                
            except asyncio.TimeoutError:
                self.logger.error("❌ Message queue timeout - dropping WebSocket message",
                                server=message.server_name,
                                queue_size=self.message_queue.qsize())
                self.stats.errors_last_hour += 1
            except Exception as queue_error:
                self.logger.error("❌ Error queuing WebSocket message",
                                server=message.server_name,
                                error=str(queue_error),
                                error_type=type(queue_error).__name__)
                self.stats.errors_last_hour += 1
                    
        except Exception as e:
            self.logger.error("❌ Error handling WebSocket message",
                            server=getattr(message, 'server_name', 'unknown'),
                            error=str(e),
                            error_type=type(e).__name__)
            self.stats.errors_last_hour += 1
    
    def _create_message_hash(self, message: DiscordMessage) -> str:
        """Create unique hash for message"""
        hash_input = (
            f"{message.guild_id}:"
            f"{message.channel_id}:"
            f"{message.message_id}:"
            f"{message.timestamp.isoformat()}:"
            f"{message.author}:"
            f"{hashlib.md5(message.content.encode()).hexdigest()[:8]}"
        )
        return hashlib.sha256(hash_input.encode()).hexdigest()
    
    def _is_announcement_channel(self, channel_name: str) -> bool:
        """Проверка что канал является announcement"""
        channel_lower = channel_name.lower()
        return any(keyword in channel_lower for keyword in self.settings.channel_keywords)
    
    def _check_rate_limit(self, server_name: str, is_realtime: bool = False) -> bool:
        """Check if server is within rate limits"""
        now = datetime.now()
        window_start = now - timedelta(minutes=1)
        
        if server_name not in self.message_rate_tracker:
            self.message_rate_tracker[server_name] = []
        
        # Clean old timestamps
        self.message_rate_tracker[server_name] = [
            ts for ts in self.message_rate_tracker[server_name]
            if ts > window_start
        ]
        
        # More relaxed limits for WebSocket real-time messages
        limit = 90 if is_realtime else 60
        
        return len(self.message_rate_tracker[server_name]) < limit
    
    def _update_rate_tracking(self, server_name: str) -> None:
        """Update rate tracking for server"""
        if server_name not in self.message_rate_tracker:
            self.message_rate_tracker[server_name] = []
        
        self.message_rate_tracker[server_name].append(datetime.now())
        
        if server_name not in self.server_message_counts:
            self.server_message_counts[server_name] = 0
        self.server_message_counts[server_name] += 1
    
    async def start(self) -> None:
        """Start the message processor with enhanced error handling"""
        if self.running:
            self.logger.warning("⚠️ Message processor is already running")
            return
        
        self.running = True
        self.start_time = datetime.now()
        
        self.logger.info("🚀 Starting Message Processor with WebSocket-only system")
        
        # Start background tasks with individual error handling
        task_definitions = [
            ("websocket_processor", self._websocket_message_processor_loop),
            ("cleanup", self._cleanup_loop),
            ("stats", self._stats_update_loop),
            ("health", self._health_check_loop),
            ("rate_limit_cleanup", self._rate_limit_cleanup_loop),
            ("dedup_cleanup", self._deduplication_cleanup_loop),
            ("websocket_stats", self._websocket_stats_loop)
        ]
        
        self.tasks = []
        for task_name, task_func in task_definitions:
            try:
                task = asyncio.create_task(task_func(), name=task_name)
                self.tasks.append(task)
                self.logger.info(f"✅ Started background task: {task_name}")
            except Exception as e:
                self.logger.error(f"❌ Failed to start background task: {task_name}", error=str(e))
                # Continue with other tasks
        
        # Start Discord WebSocket monitoring
        try:
            discord_task = asyncio.create_task(
                self.discord_service.start_websocket_monitoring(), 
                name="discord_websocket"
            )
            self.tasks.append(discord_task)
            self.logger.info("✅ Discord WebSocket monitoring task started")
        except Exception as e:
            self.logger.error("❌ Failed to start Discord WebSocket monitoring", error=str(e))
            # Critical error for WebSocket-only system
            await self.stop()
            return
        
        # Start Telegram bot
        try:
            telegram_task = asyncio.create_task(
                self.telegram_service.start_bot_async(), 
                name="telegram_bot"
            )
            self.tasks.append(telegram_task)
            self.logger.info("✅ Telegram bot task started")
        except Exception as e:
            self.logger.error("❌ Failed to start Telegram bot", error=str(e))
            # Not critical for message processing, continue
        
        # Perform initial sync ТОЛЬКО ОДИН РАЗ (2 сообщения)
        try:
            await self._perform_initial_sync_once()
            self.logger.info("✅ Initial synchronization completed")
        except Exception as e:
            self.logger.error("❌ Error during initial sync", error=str(e))
            # Continue anyway
        
        self.logger.info("✅ Message Processor started successfully - WebSocket monitoring active")
        
        try:
            # Wait for all tasks with enhanced error handling
            results = await asyncio.gather(*self.tasks, return_exceptions=True)
            
            # Log any individual task failures
            for task, result in zip(self.tasks, results):
                if isinstance(result, Exception):
                    self.logger.error("❌ Task failed",
                                task_name=task.get_name(),
                                error=str(result),
                                error_type=type(result).__name__)
                elif task.done() and task.exception():
                    self.logger.error("❌ Task completed with exception",
                                task_name=task.get_name(),
                                error=str(task.exception()))
        except Exception as e:
            self.logger.error("❌ Critical error in message processor", 
                            error=str(e),
                            error_type=type(e).__name__)
        finally:
            await self.stop()
    
    async def stop(self) -> None:
        """Stop the message processor with enhanced cleanup"""
        if not self.running:
            self.logger.info("Message processor is not running")
            return
        
        self.running = False
        self.logger.info("🛑 Stopping Message Processor")
        
        # Cancel all background tasks
        cancelled_tasks = 0
        for task in self.tasks:
            if not task.done():
                task.cancel()
                cancelled_tasks += 1
        
        if cancelled_tasks > 0:
            self.logger.info(f"Cancelled {cancelled_tasks} background tasks")
        
        # Wait for tasks to complete with timeout
        if self.tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.tasks, return_exceptions=True),
                    timeout=30.0
                )
                self.logger.info("All background tasks completed")
            except asyncio.TimeoutError:
                self.logger.warning("Some background tasks did not complete within timeout")
            except Exception as e:
                self.logger.error("Error waiting for tasks to complete", error=str(e))
        
        # Cleanup services
        cleanup_errors = []
        
        try:
            await self.discord_service.cleanup()
            self.logger.info("Discord service cleaned up")
        except Exception as e:
            cleanup_errors.append(f"Discord cleanup error: {e}")
            self.logger.error("Error cleaning up Discord service", error=str(e))
        
        try:
            await self.telegram_service.cleanup()
            self.logger.info("Telegram service cleaned up")
        except Exception as e:
            cleanup_errors.append(f"Telegram cleanup error: {e}")
            self.logger.error("Error cleaning up Telegram service", error=str(e))
        
        # Final cleanup
        try:
            # Clear memory structures
            async with self.message_dedup_lock:
                self.processed_message_hashes.clear()
            
            self.channel_initialization_done.clear()
            self.last_processed_message_per_channel.clear()
            
            self.logger.info("Memory structures cleared")
        except Exception as e:
            cleanup_errors.append(f"Memory cleanup error: {e}")
            self.logger.error("Error during memory cleanup", error=str(e))
        
        if cleanup_errors:
            self.logger.warning(f"Message Processor stopped with {len(cleanup_errors)} cleanup errors")
            for error in cleanup_errors:
                self.logger.warning(f"  • {error}")
        else:
            self.logger.info("✅ Message Processor stopped cleanly")
    
    async def _perform_initial_sync_once(self) -> None:
        """Выполнить начальную синхронизацию ТОЛЬКО 2 сообщения"""
        if self.initial_sync_completed:
            self.logger.warning("⚠️ Initial sync already completed, skipping")
            return
            
        self.logger.info("🚀 Starting INITIAL synchronization (last 2 messages per channel, ONCE)")
        
        total_messages = 0
        
        for server_name, server_info in self.discord_service.servers.items():
            if server_info.status != ServerStatus.ACTIVE:
                continue
            
            server_messages = []
            
            # Get messages from ALL monitored channels
            for channel_id, channel_info in server_info.accessible_channels.items():
                if channel_id not in self.discord_service.monitored_announcement_channels:
                    continue
                
                try:
                    self.logger.info(f"📥 Getting initial messages from channel {channel_info.channel_name} ({channel_id})")
                    
                    # Get last 2 messages for initial sync
                    messages = await self.discord_service.get_recent_messages(
                        server_name,
                        channel_id,
                        limit=2
                    )
                    
                    if messages:
                        # Сортируем по времени (старые -> новые)
                        messages.sort(key=lambda x: x.timestamp, reverse=False)
                        
                        # Добавляем ВСЕ сообщения в initial sync (без дедупликации)
                        for msg in messages:
                            msg_hash = self._create_message_hash(msg)
                            # Добавляем хэш в processed, чтобы не обрабатывать эти сообщения повторно
                            self.processed_message_hashes.add(msg_hash)
                            server_messages.append(msg)
                        
                        # Запоминаем время последнего сообщения для этого канала
                        latest_message = max(messages, key=lambda x: x.timestamp)
                        self.last_processed_message_per_channel[channel_id] = latest_message.timestamp
                        
                        self.logger.info(f"✅ Channel {channel_info.channel_name}: {len(messages)} messages, latest: {latest_message.timestamp.isoformat()}")
                    else:
                        # Даже если сообщений нет, отмечаем канал как инициализированный
                        self.last_processed_message_per_channel[channel_id] = datetime.now()
                        self.logger.info(f"📭 Channel {channel_info.channel_name}: no messages found")
                    
                    # Отмечаем канал как инициализированный
                    self.channel_initialization_done.add(channel_id)
                    
                except Exception as e:
                    self.logger.error("❌ Error getting initial messages",
                                    server=server_name,
                                    channel_id=channel_id,
                                    error=str(e))
                    # Даже при ошибке отмечаем канал как инициализированный с текущим временем
                    self.last_processed_message_per_channel[channel_id] = datetime.now()
                    self.channel_initialization_done.add(channel_id)
            
            if server_messages:
                # Sort by timestamp and send to Telegram (oldest first)
                server_messages.sort(key=lambda x: x.timestamp, reverse=False)
                sent_count = await self.telegram_service.send_messages_batch(server_messages)
                
                total_messages += sent_count
                self.server_message_counts[server_name] = sent_count
                
                self.logger.info("✅ Initial sync for server complete",
                               server=server_name,
                               messages_sent=sent_count,
                               total_messages=len(server_messages),
                               oldest_first=True)
        
        # ВАЖНО: Отмечаем что начальная синхронизация завершена
        self.initial_sync_completed = True
        
        self.stats.messages_processed_total += total_messages
        
        self.logger.info("🎉 INITIAL synchronization COMPLETED - now monitoring only NEW WebSocket messages",
                        total_messages=total_messages,
                        servers_synced=len([s for s in self.discord_service.servers.values() 
                                          if s.status == ServerStatus.ACTIVE]),
                        initialized_channels=len(self.channel_initialization_done))
    
    async def _websocket_message_processor_loop(self) -> None:
        """WebSocket message processing loop - только НОВЫЕ сообщения"""
        self.logger.info("👂 Starting WebSocket message processor loop")
        
        while self.running:
            try:
                # Get message from queue with timeout
                try:
                    message = await asyncio.wait_for(self.message_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Process message immediately for real-time sync
                await self._process_websocket_message(message)
                
                # Mark task as done
                self.message_queue.task_done()
                
            except Exception as e:
                self.logger.error("❌ Error in WebSocket message processor loop", error=str(e))
                await asyncio.sleep(1)
    
    async def _process_websocket_message(self, message: DiscordMessage) -> None:
        """Process a WebSocket Discord message - только НОВЫЕ"""
        try:
            # Проверяем что канал мониторится
            if message.channel_id not in self.discord_service.monitored_announcement_channels:
                self.logger.debug("🚫 Non-monitored channel message skipped in processing",
                                server=message.server_name,
                                channel=message.channel_name)
                return
            
            # Send to Telegram with retry logic
            try:
                success = await self.telegram_service.send_message(message)
                
                if success:
                    self.stats.messages_processed_today += 1
                    self.stats.messages_processed_total += 1
                    self.websocket_messages_processed += 1
                    self.server_message_counts[message.server_name] += 1
                    
                    # Update last processed time for this channel
                    self.last_processed_message_per_channel[message.channel_id] = message.timestamp
                    
                    # Cache in Redis if available
                    if self.redis_client:
                        await self._cache_message_in_redis(message)
                    
                    channel_type = "announcement" if self._is_announcement_channel(message.channel_name) else "regular"
                    self.logger.info("✅ NEW WebSocket message processed and sent",
                                   server=message.server_name,
                                   channel=message.channel_name,
                                   channel_type=channel_type,
                                   author=message.author,
                                   total_today=self.stats.messages_processed_today,
                                   server_total=self.server_message_counts[message.server_name])
                else:
                    self.stats.errors_last_hour += 1
                    self.stats.last_error = "Failed to send WebSocket message to Telegram"
                    self.stats.last_error_time = datetime.now()
                    
                    self.logger.error("❌ Failed to process WebSocket message",
                                    server=message.server_name,
                                    channel=message.channel_name)
            
            except Exception as e:
                self.stats.errors_last_hour += 1
                self.stats.last_error = f"Telegram send error: {str(e)}"
                self.stats.last_error_time = datetime.now()
                
                if "message thread not found" in str(e).lower():
                    self.logger.warning("⚠️ Telegram topic not found for WebSocket message",
                                      server=message.server_name,
                                      channel=message.channel_name)
                else:
                    self.logger.error("❌ Error sending WebSocket message to Telegram",
                                    server=message.server_name,
                                    error=str(e))
            
        except Exception as e:
            self.logger.error("❌ Error processing WebSocket message", 
                            server=message.server_name,
                            error=str(e))
            
            self.stats.errors_last_hour += 1
            self.stats.last_error = str(e)
            self.stats.last_error_time = datetime.now()
    
    async def _cache_message_in_redis(self, message: DiscordMessage) -> None:
        """Cache message in Redis for deduplication"""
        if not self.redis_client:
            return
        
        try:
            message_hash = self._create_message_hash(message)
            await self.redis_client.setex(
                f"msg:{message_hash}",
                3600,  # 1 hour TTL
                message.model_dump_json()
            )
        except Exception as e:
            self.logger.error("❌ Failed to cache message in Redis", error=str(e))
    
    async def _cleanup_loop(self) -> None:
        """Periodic cleanup loop"""
        while self.running:
            try:
                await asyncio.sleep(self.settings.cleanup_interval_minutes * 60)
                
                # Memory cleanup
                import gc
                gc.collect()
                
                # Reset daily stats at midnight
                now = datetime.now()
                if now.date() > self.last_cleanup.date():
                    self.stats.messages_processed_today = 0
                    self.stats.errors_last_hour = 0
                    
                    # Reset daily server counters
                    for server_name in self.server_message_counts:
                        self.server_message_counts[server_name] = 0
                
                self.last_cleanup = now
                
                self.logger.info("🧹 Cleanup completed",
                               processed_hashes=len(self.processed_message_hashes),
                               initialized_channels=len(self.channel_initialization_done),
                               websocket_messages_received=self.websocket_messages_received,
                               websocket_messages_processed=self.websocket_messages_processed)
                
            except Exception as e:
                self.logger.error("❌ Error in cleanup loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _rate_limit_cleanup_loop(self) -> None:
        """Clean up old rate tracking data"""
        while self.running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                cutoff_time = datetime.now() - timedelta(minutes=5)
                
                for server_name in list(self.message_rate_tracker.keys()):
                    old_count = len(self.message_rate_tracker[server_name])
                    self.message_rate_tracker[server_name] = [
                        ts for ts in self.message_rate_tracker[server_name]
                        if ts > cutoff_time
                    ]
                    
                    new_count = len(self.message_rate_tracker[server_name])
                    
                    if old_count != new_count:
                        self.logger.debug("🧹 Cleaned rate tracking data",
                                        server=server_name,
                                        removed_count=old_count - new_count)
                
            except Exception as e:
                self.logger.error("❌ Error in rate limit cleanup loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _deduplication_cleanup_loop(self) -> None:
        """Periodic cleanup of deduplication data"""
        while self.running:
            try:
                await asyncio.sleep(600)  # Every 10 minutes
                
                # Limit the size of processed hashes (keep last 10000)
                async with self.message_dedup_lock:
                    if len(self.processed_message_hashes) > 10000:
                        # Convert to list, sort, and keep last 5000
                        hashes_list = list(self.processed_message_hashes)
                        # Keep only newer half (this is rough, but prevents unlimited growth)
                        self.processed_message_hashes = set(hashes_list[-5000:])
                        
                        self.logger.info("🧹 Cleaned old message hashes",
                                       removed_count=len(hashes_list) - 5000,
                                       remaining_count=len(self.processed_message_hashes))
                
                self.logger.debug("🧹 Deduplication cleanup completed",
                                processed_hashes=len(self.processed_message_hashes),
                                initialized_channels=len(self.channel_initialization_done))
                
            except Exception as e:
                self.logger.error("❌ Error in deduplication cleanup loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _stats_update_loop(self) -> None:
        """Statistics update loop"""
        while self.running:
            try:
                await asyncio.sleep(60)  # Update stats every minute
                await self._update_stats()
                
            except Exception as e:
                self.logger.error("❌ Error in stats update loop", error=str(e))
                await asyncio.sleep(60)
    
    async def _websocket_stats_loop(self) -> None:
        """WebSocket statistics update loop"""
        while self.running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                # Get WebSocket status
                ws_status = self.discord_service.get_websocket_status()
                
                self.logger.info("📊 WebSocket Stats Update",
                               active_connections=ws_status.get('active_connections', 0),
                               total_connections=ws_status.get('total_connections', 0),
                               messages_received=self.websocket_messages_received,
                               messages_processed=self.websocket_messages_processed,
                               last_message=self.websocket_last_message_time.isoformat() if self.websocket_last_message_time else None)
                
            except Exception as e:
                self.logger.error("❌ Error in WebSocket stats loop", error=str(e))
                await asyncio.sleep(300)
    
    async def _health_check_loop(self) -> None:
        """Health check loop"""
        while self.running:
            try:
                await asyncio.sleep(self.settings.health_check_interval)
                
                # Check Discord WebSocket health
                ws_status = self.discord_service.get_websocket_status()
                discord_healthy = ws_status.get('active_connections', 0) > 0
                
                # Check Telegram service health  
                telegram_healthy = self.telegram_service.bot_running
                
                # Check queue sizes
                queue_healthy = self.message_queue.qsize() < 500
                
                # Check deduplication health
                dedup_healthy = len(self.processed_message_hashes) < 50000
                
                # Check initialization status
                init_healthy = self.initial_sync_completed
                
                # Check WebSocket message flow
                message_flow_healthy = True
                if self.websocket_last_message_time:
                    time_since_last = datetime.now() - self.websocket_last_message_time
                    # Alert if no messages for 1 hour (this might be normal)
                    message_flow_healthy = time_since_last.total_seconds() < 3600
                
                if not (discord_healthy and telegram_healthy and queue_healthy and dedup_healthy and init_healthy):
                    self.logger.warning("⚠️ Health check failed",
                                      discord_websocket_healthy=discord_healthy,
                                      telegram_healthy=telegram_healthy,
                                      queue_healthy=queue_healthy,
                                      dedup_healthy=dedup_healthy,
                                      init_completed=init_healthy,
                                      message_flow_healthy=message_flow_healthy,
                                      queue_size=self.message_queue.qsize(),
                                      processed_hashes=len(self.processed_message_hashes),
                                      websocket_connections=ws_status.get('active_connections', 0))
                else:
                    self.logger.debug("✅ Health check passed",
                                    queue_size=self.message_queue.qsize(),
                                    processed_hashes=len(self.processed_message_hashes),
                                    initialized_channels=len(self.channel_initialization_done),
                                    websocket_connections=ws_status.get('active_connections', 0))
                
            except Exception as e:
                self.logger.error("❌ Error in health check loop", error=str(e))
                await asyncio.sleep(60)
    
    async def _update_stats(self) -> None:
        """Update system statistics"""
        try:
            # Discord stats
            discord_stats = self.discord_service.get_server_stats()
            
            self.stats.total_servers = discord_stats['total_servers']
            self.stats.active_servers = discord_stats['active_servers']
            self.stats.total_channels = discord_stats['total_channels']
            self.stats.active_channels = discord_stats['accessible_channels']
            
            # Memory usage
            import psutil
            process = psutil.Process()
            self.stats.memory_usage_mb = process.memory_info().rss / 1024 / 1024
            
            # Uptime
            self.stats.uptime_seconds = int((datetime.now() - self.start_time).total_seconds())
            
            # Rate limiting stats
            self.stats.discord_requests_per_hour = getattr(
                self.discord_service.rate_limiter, 'requests_last_hour', 0
            )
            self.stats.telegram_requests_per_hour = getattr(
                self.telegram_service.rate_limiter, 'requests_last_hour', 0
            )
            
        except Exception as e:
            self.logger.error("❌ Error updating stats", error=str(e))
    
    def get_status(self) -> Dict[str, any]:
        """Get comprehensive system status with WebSocket info"""
        discord_stats = self.discord_service.get_server_stats()
        
        # Enhanced статистики Telegram
        enhanced_features = {}
        try:
            enhanced_stats = self.telegram_service.get_enhanced_stats()
            enhanced_features = {
                "telegram_enhanced": enhanced_stats,
                "anti_duplicate_active": self.telegram_service.startup_verification_done,
                "bot_interface_active": self.telegram_service.bot_running,
                "user_states_count": len(getattr(self.telegram_service, 'user_states', {})),
                "processed_messages_cache": len(getattr(self.telegram_service, 'processed_messages', {}))
            }
        except Exception as e:
            enhanced_features = {"error": str(e), "available": False}
        
        # WebSocket статистики
        ws_status = self.discord_service.get_websocket_status()
        
        # Подсчет типов каналов
        announcement_channels = 0
        manually_added_channels = 0
        for server_info in self.discord_service.servers.values():
            for channel_id, channel_info in server_info.accessible_channels.items():
                if channel_id in self.discord_service.monitored_announcement_channels:
                    if self._is_announcement_channel(channel_info.channel_name):
                        announcement_channels += 1
                    else:
                        manually_added_channels += 1
        
        return {
            "system": {
                "running": self.running,
                "uptime_seconds": self.stats.uptime_seconds,
                "memory_usage_mb": self.stats.memory_usage_mb,
                "health_score": self.stats.health_score,
                "status": self.stats.status,
                "realtime_enabled": self.realtime_enabled,
                "initial_sync_completed": self.initial_sync_completed,
                "version": "WebSocket-only версия - real-time messages",
                "mode": "WebSocket Real-time"
            },
            "discord": {
                **discord_stats,
                "auto_discovered_announcement": announcement_channels,
                "manually_added_channels": manually_added_channels,
                "monitoring_strategy": "WebSocket Real-time"
            },
            "telegram": {
                "topics": len(self.telegram_service.server_topics),
                "bot_running": self.telegram_service.bot_running,
                "messages_tracked": len(self.telegram_service.message_mappings),
                "one_topic_per_server": True,
                "enhanced_interface": True
            },
            "processing": {
                "queue_size": self.message_queue.qsize(),
                "messages_today": self.stats.messages_processed_today,
                "messages_total": self.stats.messages_processed_total,
                "errors_last_hour": self.stats.errors_last_hour,
                "last_error": self.stats.last_error,
                "last_error_time": self.stats.last_error_time.isoformat() if self.stats.last_error_time else None,
                "processed_hashes": len(self.processed_message_hashes),
                "initialized_channels": len(self.channel_initialization_done),
                "anti_duplication": "ACTIVE"
            },
            "websocket": {
                "status": ws_status,
                "messages_received": self.websocket_messages_received,
                "messages_processed": self.websocket_messages_processed,
                "last_message_time": self.websocket_last_message_time.isoformat() if self.websocket_last_message_time else None,
                "processing_rate": round(self.websocket_messages_processed / max(1, self.websocket_messages_received) * 100, 2) if self.websocket_messages_received > 0 else 0
            },
            "rate_limiting": {
                "discord": self.discord_service.rate_limiter.get_stats(),
                "telegram": self.telegram_service.rate_limiter.get_stats()
            },
            "enhanced_features": enhanced_features,
            "servers": {
                server_name: {
                    "message_count": self.server_message_counts.get(server_name, 0),
                    "last_activity": self.server_last_activity.get(server_name, datetime.now()).isoformat(),
                    "announcement_channels": len([
                        ch for ch in self.discord_service.servers[server_name].accessible_channels.values()
                        if ch.channel_id in self.discord_service.monitored_announcement_channels
                        and self._is_announcement_channel(ch.channel_name)
                    ]) if server_name in self.discord_service.servers else 0,
                    "manually_added_channels": len([
                        ch for ch in self.discord_service.servers[server_name].accessible_channels.values()
                        if ch.channel_id in self.discord_service.monitored_announcement_channels
                        and not self._is_announcement_channel(ch.channel_name)
                    ]) if server_name in self.discord_service.servers else 0,
                    "initialized_channels": len([
                        ch_id for ch_id in self.discord_service.servers[server_name].accessible_channels.keys()
                        if ch_id in self.channel_initialization_done
                    ]) if server_name in self.discord_service.servers else 0
                }
                for server_name in self.discord_service.servers.keys()
            },
            "anti_duplication_info": {
                "initial_sync_completed": self.initial_sync_completed,
                "initialized_channels": len(self.channel_initialization_done),
                "processed_message_hashes": len(self.processed_message_hashes),
                "channel_timestamps": {
                    ch_id: timestamp.isoformat() 
                    for ch_id, timestamp in self.last_processed_message_per_channel.items()
                },
                "strategy": "WebSocket Real-time - no polling, no duplicates"
            }
        }
    
    # Остальные методы остаются без изменений
    def reset_channel_initialization(self, channel_id: str) -> bool:
        """Сброс инициализации канала (для debug/maintenance)"""
        try:
            if channel_id in self.channel_initialization_done:
                self.channel_initialization_done.remove(channel_id)
            
            if channel_id in self.last_processed_message_per_channel:
                del self.last_processed_message_per_channel[channel_id]
            
            self.logger.info("Channel initialization reset", channel_id=channel_id)
            return True
            
        except Exception as e:
            self.logger.error("Error resetting channel initialization", 
                            channel_id=channel_id, error=str(e))
            return False
    
    def force_reinitialize_all_channels(self) -> int:
        """Принудительная реинициализация всех каналов"""
        try:
            old_count = len(self.channel_initialization_done)
            
            self.channel_initialization_done.clear()
            self.last_processed_message_per_channel.clear()
            self.processed_message_hashes.clear()
            self.initial_sync_completed = False
            
            # Reset WebSocket stats
            self.websocket_messages_received = 0
            self.websocket_messages_processed = 0
            self.websocket_last_message_time = None
            
            self.logger.warning("🔄 Forced reinitialization of all channels (WebSocket mode)", 
                              previous_initialized_count=old_count)
            
            return old_count
            
        except Exception as e:
            self.logger.error("❌ Error in force reinitialize", error=str(e))
            return 0
    
    def get_channel_status(self, channel_id: str) -> Dict[str, any]:
        """Получить статус конкретного канала"""
        return {
            "channel_id": channel_id,
            "is_monitored": channel_id in self.discord_service.monitored_announcement_channels,
            "is_initialized": channel_id in self.channel_initialization_done,
            "last_processed_timestamp": self.last_processed_message_per_channel.get(channel_id),
            "last_processed_iso": self.last_processed_message_per_channel.get(channel_id).isoformat() 
                                 if channel_id in self.last_processed_message_per_channel else None,
            "in_server": any(
                channel_id in server_info.channels.keys() 
                for server_info in self.discord_service.servers.values()
            ),
            "websocket_coverage": True  # All monitored channels have WebSocket coverage
        }
    
    def get_anti_duplication_stats(self) -> Dict[str, any]:
        """Подробная статистика системы анти-дублирования"""
        return {
            "initialization": {
                "initial_sync_completed": self.initial_sync_completed,
                "initialized_channels_count": len(self.channel_initialization_done),
                "initialized_channels": list(self.channel_initialization_done),
                "pending_channels": [
                    ch_id for ch_id in self.discord_service.monitored_announcement_channels
                    if ch_id not in self.channel_initialization_done
                ]
            },
            "deduplication": {
                "processed_hashes_count": len(self.processed_message_hashes),
                "tracked_channels_count": len(self.last_processed_message_per_channel),
                "memory_usage_estimate_mb": len(self.processed_message_hashes) * 64 / (1024 * 1024)
            },
            "timestamps": {
                "channels_with_timestamps": len(self.last_processed_message_per_channel),
                "oldest_timestamp": min(self.last_processed_message_per_channel.values()).isoformat() 
                                  if self.last_processed_message_per_channel else None,
                "newest_timestamp": max(self.last_processed_message_per_channel.values()).isoformat() 
                                  if self.last_processed_message_per_channel else None
            },
            "websocket": {
                "messages_received": self.websocket_messages_received,
                "messages_processed": self.websocket_messages_processed,
                "last_message_time": self.websocket_last_message_time.isoformat() if self.websocket_last_message_time else None,
                "processing_efficiency": round(self.websocket_messages_processed / max(1, self.websocket_messages_received) * 100, 2) if self.websocket_messages_received > 0 else 100
            },
            "system_health": {
                "queue_size": self.message_queue.qsize(),
                "rate_tracking_active": len(self.message_rate_tracker),
                "system_running": self.running,
                "realtime_enabled": self.realtime_enabled,
                "websocket_status": self.discord_service.get_websocket_status()
            }
        }
    
    def get_diagnostic_info(self) -> Dict[str, Any]:
        """Получить подробную диагностическую информацию"""
        diagnostic = {
            "timestamp": datetime.now().isoformat(),
            "service_status": {
                "running": self.running,
                "initialized": hasattr(self, 'discord_service') and hasattr(self, 'telegram_service'),
                "start_time": self.start_time.isoformat() if self.start_time else None,
                "uptime_seconds": int((datetime.now() - self.start_time).total_seconds()) if self.start_time else 0
            },
            "tasks": {
                "total_tasks": len(self.tasks),
                "running_tasks": len([t for t in self.tasks if not t.done()]),
                "failed_tasks": len([t for t in self.tasks if t.done() and t.exception()]),
                "task_details": []
            },
            "message_processing": {
                "initial_sync_completed": self.initial_sync_completed,
                "websocket_messages_received": self.websocket_messages_received,
                "websocket_messages_processed": self.websocket_messages_processed,
                "queue_size": self.message_queue.qsize(),
                "processed_hashes": len(self.processed_message_hashes),
                "initialized_channels": len(self.channel_initialization_done)
            },
            "discord_service": {},
            "telegram_service": {},
            "errors_and_warnings": []
        }
        
        # Детали задач
        for task in self.tasks:
            task_info = {
                "name": task.get_name(),
                "done": task.done(),
                "cancelled": task.cancelled(),
                "exception": str(task.exception()) if task.done() and task.exception() else None
            }
            diagnostic["tasks"]["task_details"].append(task_info)
        
        # Discord service диагностика
        try:
            if hasattr(self.discord_service, 'get_service_health'):
                diagnostic["discord_service"] = self.discord_service.get_service_health()
            else:
                diagnostic["discord_service"] = {
                    "available": hasattr(self, 'discord_service'),
                    "sessions": len(getattr(self.discord_service, 'sessions', [])),
                    "servers": len(getattr(self.discord_service, 'servers', {})),
                    "monitored_channels": len(getattr(self.discord_service, 'monitored_announcement_channels', set()))
                }
        except Exception as e:
            diagnostic["discord_service"] = {"error": str(e)}
            diagnostic["errors_and_warnings"].append(f"Discord service diagnostic error: {e}")
        
        # Telegram service диагностика
        try:
            if hasattr(self.telegram_service, 'get_bot_health'):
                diagnostic["telegram_service"] = self.telegram_service.get_bot_health()
            else:
                diagnostic["telegram_service"] = {
                    "available": hasattr(self, 'telegram_service'),
                    "bot_running": getattr(self.telegram_service, 'bot_running', False),
                    "topics": len(getattr(self.telegram_service, 'server_topics', {}))
                }
        except Exception as e:
            diagnostic["telegram_service"] = {"error": str(e)}
            diagnostic["errors_and_warnings"].append(f"Telegram service diagnostic error: {e}")
        
        # Общие предупреждения
        if not self.running:
            diagnostic["errors_and_warnings"].append("Message processor is not running")
        
        if self.message_queue.qsize() > 100:
            diagnostic["errors_and_warnings"].append(f"High message queue size: {self.message_queue.qsize()}")
        
        if len(self.processed_message_hashes) > 10000:
            diagnostic["errors_and_warnings"].append(f"High processed hashes count: {len(self.processed_message_hashes)}")
        
        if not self.initial_sync_completed:
            diagnostic["errors_and_warnings"].append("Initial sync not completed")
        
        failed_tasks = [t for t in self.tasks if t.done() and t.exception()]
        if failed_tasks:
            diagnostic["errors_and_warnings"].append(f"{len(failed_tasks)} background tasks have failed")
        
        return diagnostic
    
    async def emergency_recovery(self) -> bool:
        """Emergency recovery procedure for critical failures"""
        self.logger.warning("🚨 Starting emergency recovery procedure")
        
        recovery_steps = []
        
        try:
            # Step 1: Stop current operations
            self.running = False
            recovery_steps.append("✅ Stopped current operations")
            
            # Step 2: Clear problematic state
            try:
                async with self.message_dedup_lock:
                    self.processed_message_hashes.clear()
                self.channel_initialization_done.clear()
                self.initial_sync_completed = False
                recovery_steps.append("✅ Cleared problematic state")
            except Exception as state_error:
                recovery_steps.append(f"❌ State cleanup error: {state_error}")
            
            # Step 3: Reset queue
            try:
                while not self.message_queue.empty():
                    try:
                        self.message_queue.get_nowait()
                    except:
                        break
                recovery_steps.append("✅ Reset message queue")
            except Exception as queue_error:
                recovery_steps.append(f"❌ Queue reset error: {queue_error}")
            
            # Step 4: Attempt service recovery
            try:
                if hasattr(self.discord_service, 'get_service_health'):
                    discord_health = self.discord_service.get_service_health()
                    if discord_health.get('status') != 'healthy':
                        self.logger.warning("Discord service unhealthy, attempting recovery")
                        # Could add Discord service recovery here
                recovery_steps.append("✅ Discord service check completed")
            except Exception as discord_error:
                recovery_steps.append(f"❌ Discord service recovery error: {discord_error}")
            
            try:
                if hasattr(self.telegram_service, 'get_bot_health'):
                    telegram_health = self.telegram_service.get_bot_health()
                    if telegram_health.get('status') != 'healthy':
                        self.logger.warning("Telegram service unhealthy, attempting recovery")
                        # Could add Telegram service recovery here
                recovery_steps.append("✅ Telegram service check completed")
            except Exception as telegram_error:
                recovery_steps.append(f"❌ Telegram service recovery error: {telegram_error}")
            
            # Step 5: Restart if possible
            try:
                self.logger.info("Attempting to restart after recovery")
                return await self.initialize()
            except Exception as restart_error:
                recovery_steps.append(f"❌ Restart error: {restart_error}")
                return False
            
        except Exception as e:
            self.logger.error("❌ Emergency recovery failed", error=str(e))
            recovery_steps.append(f"❌ Recovery procedure failed: {e}")
            return False
        
        finally:
            self.logger.info("🚨 Emergency recovery completed")
            for step in recovery_steps:
                self.logger.info(f"  {step}")
            
            return True
    
    def get_websocket_performance_stats(self) -> Dict[str, any]:
        """Получить статистику производительности WebSocket"""
        ws_status = self.discord_service.get_websocket_status()
        
        # Calculate message processing rate
        processing_rate = 0
        if self.websocket_messages_received > 0:
            processing_rate = round(self.websocket_messages_processed / self.websocket_messages_received * 100, 2)
        
        # Calculate uptime for each connection
        connection_uptimes = []
        for conn in ws_status.get('connections', []):
            if conn.get('connected') and conn.get('connected_at'):
                try:
                    connected_at = datetime.fromisoformat(conn['connected_at']) if isinstance(conn['connected_at'], str) else conn['connected_at']
                    uptime_seconds = (datetime.now() - connected_at).total_seconds()
                    connection_uptimes.append(uptime_seconds)
                except:
                    pass
        
        return {
            "websocket_connections": {
                "total": ws_status.get('total_connections', 0),
                "active": ws_status.get('active_connections', 0),
                "success_rate": round(ws_status.get('active_connections', 0) / max(1, ws_status.get('total_tokens', 1)) * 100, 2)
            },
            "message_processing": {
                "received": self.websocket_messages_received,
                "processed": self.websocket_messages_processed,
                "processing_rate_percent": processing_rate,
                "queue_size": self.message_queue.qsize(),
                "last_message": self.websocket_last_message_time.isoformat() if self.websocket_last_message_time else None
            },
            "connection_health": {
                "average_uptime_seconds": sum(connection_uptimes) / len(connection_uptimes) if connection_uptimes else 0,
                "total_connections_attempted": len(self.discord_service.gateway_urls),
                "successful_connections": ws_status.get('active_connections', 0),
                "monitored_channels": len(self.discord_service.monitored_announcement_channels)
            },
            "performance_indicators": {
                "real_time_delivery": processing_rate >= 95,
                "connection_stability": ws_status.get('active_connections', 0) >= len(self.discord_service.gateway_urls) * 0.8,
                "queue_health": self.message_queue.qsize() < 100,
                "deduplication_efficiency": len(self.processed_message_hashes) < 10000
            }
        }
        