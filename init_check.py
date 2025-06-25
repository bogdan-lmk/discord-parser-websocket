#!/usr/bin/env python3
"""
Детальный анализ серверов и каналов
Определяет почему у некоторых серверов нет топиков
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Dict, List, Set
import structlog
from dotenv import load_dotenv

# Настройка логирования
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(colors=True)
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)

class ServerChannelAnalyzer:
    """Анализатор серверов и каналов"""
    
    def __init__(self):
        load_dotenv()
        self.analysis = {
            "timestamp": datetime.now().isoformat(),
            "servers": {},
            "categories": {
                "with_topics": [],
                "without_topics": [],
                "with_monitored_channels": [],
                "without_monitored_channels": [],
                "with_announcement_channels": [],
                "without_announcement_channels": [],
                "active_servers": [],
                "inactive_servers": []
            },
            "summary": {},
            "recommendations": []
        }
    
    async def analyze_all_servers(self):
        """Анализ всех серверов"""
        logger.info("🔍 Начинаем детальный анализ серверов и каналов")
        
        # Инициализация сервисов
        discord_service, telegram_service = await self._initialize_services()
        
        if not discord_service or not telegram_service:
            logger.error("❌ Не удалось инициализировать сервисы")
            return False
        
        # Анализ каждого сервера
        for server_name, server_info in discord_service.servers.items():
            await self._analyze_server(
                server_name, 
                server_info, 
                discord_service, 
                telegram_service
            )
        
        # Категоризация серверов
        self._categorize_servers()
        
        # Генерация отчета
        self._generate_summary()
        
        # Cleanup
        await discord_service.cleanup()
        await telegram_service.cleanup()
        
        return True
    
    async def _initialize_services(self):
        """Инициализация сервисов"""
        try:
            from app.config import get_settings
            from app.services.discord_service import DiscordService
            from app.services.telegram_service import TelegramService
            from app.utils.rate_limiter import RateLimiter
            
            settings = get_settings()
            rate_limiter = RateLimiter()
            
            # Discord
            discord_service = DiscordService(settings, rate_limiter)
            if not await discord_service.initialize():
                return None, None
            
            # Telegram
            telegram_service = TelegramService(settings, rate_limiter)
            if not await telegram_service.initialize():
                return None, None
            
            # Связываем сервисы
            telegram_service.set_discord_service(discord_service)
            
            logger.info(f"✅ Сервисы инициализированы: {len(discord_service.servers)} серверов")
            
            return discord_service, telegram_service
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            return None, None
    
    async def _analyze_server(self, server_name: str, server_info, discord_service, telegram_service):
        """Детальный анализ одного сервера"""
        logger.debug(f"🔍 Анализ сервера: {server_name}")
        
        server_analysis = {
            "name": server_name,
            "guild_id": server_info.guild_id,
            "status": str(server_info.status),
            "channels": {
                "total": len(server_info.channels),
                "accessible": len(server_info.accessible_channels),
                "monitored": 0,
                "announcement": 0,
                "regular_monitored": 0
            },
            "telegram": {
                "has_topic": False,
                "topic_id": None,
                "topic_valid": False
            },
            "issues": [],
            "channel_details": []
        }
        
        # Анализ каналов
        announcement_channels = []
        monitored_channels = []
        
        for channel_id, channel_info in server_info.channels.items():
            channel_detail = {
                "id": channel_id,
                "name": channel_info.channel_name,
                "accessible": channel_info.http_accessible,
                "is_announcement": discord_service._is_announcement_channel(channel_info.channel_name),
                "is_monitored": channel_id in discord_service.monitored_announcement_channels,
                "message_count": getattr(channel_info, 'message_count', 0),
                "last_message": getattr(channel_info, 'last_message_time', None)
            }
            
            if channel_detail["is_announcement"]:
                announcement_channels.append(channel_detail)
                server_analysis["channels"]["announcement"] += 1
            
            if channel_detail["is_monitored"]:
                monitored_channels.append(channel_detail)
                server_analysis["channels"]["monitored"] += 1
                
                if not channel_detail["is_announcement"]:
                    server_analysis["channels"]["regular_monitored"] += 1
            
            server_analysis["channel_details"].append(channel_detail)
        
        # Анализ топика в Telegram
        topic_id = telegram_service.server_topics.get(server_name)
        if topic_id:
            server_analysis["telegram"]["has_topic"] = True
            server_analysis["telegram"]["topic_id"] = topic_id
            
            # Проверяем валидность топика
            try:
                topic_info = telegram_service.bot.get_forum_topic(
                    chat_id=telegram_service.settings.telegram_chat_id,
                    message_thread_id=topic_id
                )
                server_analysis["telegram"]["topic_valid"] = bool(topic_info)
                
                if not topic_info:
                    server_analysis["issues"].append("Топик в кеше, но не существует в Telegram")
                    
            except Exception as e:
                server_analysis["telegram"]["topic_valid"] = False
                server_analysis["issues"].append(f"Ошибка проверки топика: {str(e)}")
        
        # Выявление проблем
        self._identify_server_issues(server_analysis, announcement_channels, monitored_channels)
        
        # Сохраняем анализ
        self.analysis["servers"][server_name] = server_analysis
    
    def _identify_server_issues(self, server_analysis, announcement_channels, monitored_channels):
        """Выявление проблем сервера"""
        issues = server_analysis["issues"]
        
        # Проблемы с каналами
        if server_analysis["channels"]["total"] == 0:
            issues.append("Нет каналов на сервере")
        
        if server_analysis["channels"]["accessible"] == 0:
            issues.append("Нет доступных каналов")
        
        if server_analysis["channels"]["announcement"] == 0:
            issues.append("Нет announcement каналов")
        
        if server_analysis["channels"]["monitored"] == 0:
            issues.append("Нет мониторимых каналов")
        
        # Проблемы с топиками
        if not server_analysis["telegram"]["has_topic"]:
            if server_analysis["channels"]["monitored"] > 0:
                issues.append("Есть мониторимые каналы, но нет топика")
            else:
                issues.append("Нет топика (и нет мониторимых каналов)")
        
        # Проблемы со статусом
        if server_analysis["status"] != "ServerStatus.ACTIVE" and server_analysis["status"] != "active":
            issues.append(f"Сервер неактивен: {server_analysis['status']}")
        
        # Логическая проблема: почему топик не создается?
        if (server_analysis["channels"]["monitored"] > 0 and 
            not server_analysis["telegram"]["has_topic"]):
            issues.append("КРИТИЧНО: Есть мониторимые каналы, но топик не создан!")
        
        # Оценка качества сервера для мониторинга
        if (server_analysis["channels"]["announcement"] == 0 and 
            server_analysis["channels"]["monitored"] == 0):
            issues.append("Сервер не подходит для автоматического мониторинга")
    
    def _categorize_servers(self):
        """Категоризация серверов по различным критериям"""
        categories = self.analysis["categories"]
        
        for server_name, server_data in self.analysis["servers"].items():
            # По наличию топиков
            if server_data["telegram"]["has_topic"]:
                categories["with_topics"].append(server_name)
            else:
                categories["without_topics"].append(server_name)
            
            # По мониторимым каналам
            if server_data["channels"]["monitored"] > 0:
                categories["with_monitored_channels"].append(server_name)
            else:
                categories["without_monitored_channels"].append(server_name)
            
            # По announcement каналам
            if server_data["channels"]["announcement"] > 0:
                categories["with_announcement_channels"].append(server_name)
            else:
                categories["without_announcement_channels"].append(server_name)
            
            # По статусу активности
            if server_data["status"] in ["ServerStatus.ACTIVE", "active"]:
                categories["active_servers"].append(server_name)
            else:
                categories["inactive_servers"].append(server_name)
    
    def _generate_summary(self):
        """Генерация итогового резюме"""
        total_servers = len(self.analysis["servers"])
        categories = self.analysis["categories"]
        
        # Основная статистика
        summary = {
            "total_servers": total_servers,
            "with_topics": len(categories["with_topics"]),
            "without_topics": len(categories["without_topics"]),
            "with_monitored_channels": len(categories["with_monitored_channels"]),
            "without_monitored_channels": len(categories["without_monitored_channels"]),
            "with_announcement_channels": len(categories["with_announcement_channels"]),
            "without_announcement_channels": len(categories["without_announcement_channels"]),
            "active_servers": len(categories["active_servers"]),
            "inactive_servers": len(categories["inactive_servers"])
        }
        
        # Проблемные серверы
        problematic_servers = []
        good_servers_without_topics = []
        
        for server_name, server_data in self.analysis["servers"].items():
            if server_data["issues"]:
                # Серверы с проблемами
                critical_issues = [
                    issue for issue in server_data["issues"] 
                    if "КРИТИЧНО" in issue
                ]
                
                if critical_issues:
                    problematic_servers.append({
                        "name": server_name,
                        "issues": server_data["issues"],
                        "monitored_channels": server_data["channels"]["monitored"],
                        "has_topic": server_data["telegram"]["has_topic"]
                    })
                
                # Хорошие серверы без топиков
                if (server_data["channels"]["monitored"] > 0 and 
                    not server_data["telegram"]["has_topic"]):
                    good_servers_without_topics.append({
                        "name": server_name,
                        "monitored_channels": server_data["channels"]["monitored"],
                        "announcement_channels": server_data["channels"]["announcement"]
                    })
        
        summary["problematic_servers"] = problematic_servers
        summary["good_servers_without_topics"] = good_servers_without_topics
        
        # Анализ причин недостающих топиков
        missing_topic_analysis = self._analyze_missing_topics()
        summary["missing_topic_analysis"] = missing_topic_analysis
        
        # Генерация рекомендаций
        self._generate_recommendations(summary)
        
        self.analysis["summary"] = summary
    
    def _analyze_missing_topics(self):
        """Анализ причин отсутствующих топиков"""
        missing_analysis = {
            "servers_should_have_topics": 0,
            "servers_reasonably_without_topics": 0,
            "breakdown": {
                "no_channels": 0,
                "no_accessible_channels": 0,
                "no_monitored_channels": 0,
                "has_monitored_but_no_topic": 0,
                "inactive_server": 0
            }
        }
        
        for server_name in self.analysis["categories"]["without_topics"]:
            server_data = self.analysis["servers"][server_name]
            
            if server_data["channels"]["monitored"] > 0:
                # Сервер ДОЛЖЕН иметь топик
                missing_analysis["servers_should_have_topics"] += 1
                missing_analysis["breakdown"]["has_monitored_but_no_topic"] += 1
            else:
                # Разумные причины отсутствия топика
                missing_analysis["servers_reasonably_without_topics"] += 1
                
                if server_data["channels"]["total"] == 0:
                    missing_analysis["breakdown"]["no_channels"] += 1
                elif server_data["channels"]["accessible"] == 0:
                    missing_analysis["breakdown"]["no_accessible_channels"] += 1
                elif server_data["status"] not in ["ServerStatus.ACTIVE", "active"]:
                    missing_analysis["breakdown"]["inactive_server"] += 1
                else:
                    missing_analysis["breakdown"]["no_monitored_channels"] += 1
        
        return missing_analysis
    
    def _generate_recommendations(self, summary):
        """Генерация рекомендаций"""
        recommendations = []
        
        # Основные рекомендации на основе анализа
        if summary["missing_topic_analysis"]["servers_should_have_topics"] > 0:
            count = summary["missing_topic_analysis"]["servers_should_have_topics"]
            recommendations.append(
                f"🚨 КРИТИЧНО: {count} серверов с мониторимыми каналами НЕ имеют топиков!"
            )
            recommendations.append(
                "🔧 Рекомендация: Запустить принудительное создание топиков"
            )
        
        # Проблемы с автодискавери
        no_announcement = summary["without_announcement_channels"]
        if no_announcement > summary["total_servers"] * 0.3:  # Более 30%
            recommendations.append(
                f"📢 {no_announcement} серверов без announcement каналов"
            )
            recommendations.append(
                "💡 Рекомендация: Добавить каналы вручную через Telegram бот"
            )
        
        # Неактивные серверы
        if summary["inactive_servers"] > 0:
            recommendations.append(
                f"⚠️ {summary['inactive_servers']} неактивных серверов"
            )
            recommendations.append(
                "🔄 Рекомендация: Проверить доступность этих серверов"
            )
        
        # Общие рекомендации
        if summary["without_topics"] > 0:
            recommendations.append(
                "🔍 Используйте Telegram бот (/servers) для детального просмотра серверов"
            )
            recommendations.append(
                "⚡ Запустите 'Force Topic Verification' в боте"
            )
        
        # Успешные результаты
        if summary["with_topics"] == summary["total_servers"]:
            recommendations.append("🎉 Все серверы имеют топики!")
        elif summary["with_topics"] > summary["total_servers"] * 0.8:  # Более 80%
            recommendations.append("✅ Большинство серверов настроено корректно")
        
        self.analysis["recommendations"] = recommendations
    
    def print_analysis(self):
        """Вывод результатов анализа"""
        print("\n" + "="*80)
        print("🔍 ДЕТАЛЬНЫЙ АНАЛИЗ СЕРВЕРОВ И КАНАЛОВ")
        print("="*80)
        
        summary = self.analysis["summary"]
        
        # Основная статистика
        print(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
        print(f"   • Всего серверов: {summary['total_servers']}")
        print(f"   • С топиками: {summary['with_topics']}")
        print(f"   • Без топиков: {summary['without_topics']}")
        print(f"   • С мониторимыми каналами: {summary['with_monitored_channels']}")
        print(f"   • Без мониторимых каналов: {summary['without_monitored_channels']}")
        print(f"   • С announcement каналами: {summary['with_announcement_channels']}")
        print(f"   • Активных серверов: {summary['active_servers']}")
        
        # Анализ отсутствующих топиков
        missing_analysis = summary["missing_topic_analysis"]
        print(f"\n🎯 АНАЛИЗ ОТСУТСТВУЮЩИХ ТОПИКОВ:")
        print(f"   • Серверов, которые ДОЛЖНЫ иметь топики: {missing_analysis['servers_should_have_topics']}")
        print(f"   • Серверов, которые разумно НЕ имеют топики: {missing_analysis['servers_reasonably_without_topics']}")
        
        breakdown = missing_analysis["breakdown"]
        print(f"\n📋 ДЕТАЛИЗАЦИЯ ПРИЧИН:")
        print(f"   • Нет каналов вообще: {breakdown['no_channels']}")
        print(f"   • Нет доступных каналов: {breakdown['no_accessible_channels']}")
        print(f"   • Нет мониторимых каналов: {breakdown['no_monitored_channels']}")
        print(f"   • Есть мониторимые, но нет топика: {breakdown['has_monitored_but_no_topic']}")
        print(f"   • Неактивные серверы: {breakdown['inactive_server']}")
        
        # Проблемные серверы
        if summary["problematic_servers"]:
            print(f"\n🚨 СЕРВЕРЫ С КРИТИЧНЫМИ ПРОБЛЕМАМИ ({len(summary['problematic_servers'])}):")
            for server in summary["problematic_servers"][:10]:  # Показываем первые 10
                print(f"   • {server['name']}: {server['monitored_channels']} мониторимых каналов")
                for issue in server['issues']:
                    if "КРИТИЧНО" in issue:
                        print(f"     🚨 {issue}")
        
        # Хорошие серверы без топиков
        if summary["good_servers_without_topics"]:
            print(f"\n✨ СЕРВЕРЫ ГОТОВЫЕ К СОЗДАНИЮ ТОПИКОВ ({len(summary['good_servers_without_topics'])}):")
            for server in summary["good_servers_without_topics"][:10]:
                print(f"   • {server['name']}: {server['monitored_channels']} мониторимых, "
                      f"{server['announcement_channels']} announcement")
        
        # Рекомендации
        if self.analysis["recommendations"]:
            print(f"\n💡 РЕКОМЕНДАЦИИ:")
            for i, rec in enumerate(self.analysis["recommendations"], 1):
                print(f"   {i}. {rec}")
        
        # Итоговый диагноз
        print(f"\n🎯 ДИАГНОЗ ПРОБЛЕМЫ 20/46 ТОПИКОВ:")
        should_have = missing_analysis["servers_should_have_topics"]
        if should_have > 0:
            print(f"   ❌ {should_have} серверов с мониторимыми каналами не имеют топиков")
            print(f"   💡 Это объясняет недостачу {should_have} топиков")
            print(f"   🔧 Решение: Принудительное создание топиков")
        else:
            print(f"   ✅ Все серверы с мониторимыми каналами имеют топики")
            print(f"   📝 Недостающие топики относятся к серверам без мониторимых каналов")
            print(f"   💡 Это нормальное поведение системы")
        
        print(f"\n" + "="*80)
    
    def save_analysis(self):
        """Сохранение анализа в файл"""
        try:
            filename = f"server_channel_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.analysis, f, ensure_ascii=False, indent=2, default=str)
            logger.info(f"✅ Анализ сохранен в {filename}")
            
            # Создаем краткий CSV отчет для удобства
            csv_filename = f"servers_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self._create_csv_report(csv_filename)
            
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения анализа: {e}")
    
    def _create_csv_report(self, filename):
        """Создание CSV отчета"""
        try:
            import csv
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = [
                    'server_name', 'guild_id', 'status', 'total_channels', 
                    'accessible_channels', 'monitored_channels', 'announcement_channels',
                    'has_topic', 'topic_id', 'topic_valid', 'issues_count'
                ]
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                
                for server_name, server_data in self.analysis["servers"].items():
                    writer.writerow({
                        'server_name': server_name,
                        'guild_id': server_data['guild_id'],
                        'status': server_data['status'],
                        'total_channels': server_data['channels']['total'],
                        'accessible_channels': server_data['channels']['accessible'],
                        'monitored_channels': server_data['channels']['monitored'],
                        'announcement_channels': server_data['channels']['announcement'],
                        'has_topic': server_data['telegram']['has_topic'],
                        'topic_id': server_data['telegram']['topic_id'],
                        'topic_valid': server_data['telegram']['topic_valid'],
                        'issues_count': len(server_data['issues'])
                    })
            
            logger.info(f"✅ CSV отчет создан: {filename}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка создания CSV: {e}")

async def main():
    """Основная функция анализа"""
    print("🔍 Детальный анализ серверов и каналов")
    print("Определяем точную причину проблемы с  топиками")
    print()
    print("⚠️ ВНИМАНИЕ: Этот скрипт запускается параллельно с app.main")
    print("Убедитесь что:")
    print("• app.main работает в отдельном процессе")
    print("• Нет конфликтов доступа к файлам")
    print("• Достаточно ресурсов системы")
    print()
    
    analyzer = ServerChannelAnalyzer()
    
    try:
        success = await analyzer.analyze_all_servers()
        
        if success:
            analyzer.print_analysis()
            analyzer.save_analysis()
            
            print(f"\n✅ Анализ завершен!")
            print(f"📄 Результаты сохранены в JSON и CSV формате")
            print(f"💡 Используйте результаты для планирования исправлений")
        else:
            print(f"\n❌ Анализ не удался")
            
    except KeyboardInterrupt:
        print("\n🛑 Анализ прерван пользователем")
    except Exception as e:
        print(f"\n❌ Критическая ошибка анализа: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    print("🔍 Запуск анализа серверов и каналов...")
    print("Этот скрипт безопасен для запуска параллельно с основным приложением")
    print()
    
    asyncio.run(main())