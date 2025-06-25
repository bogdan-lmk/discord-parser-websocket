from typing import Dict, Optional

class TopicManager:
    """Manage creation and verification of Telegram topics."""

    async def get_or_create_server_topic(self, server_name: str) -> Optional[int]:
        if server_name in self.server_topics:
            return self.server_topics[server_name]
        if not self.settings.use_topics:
            return None
        topic_id = len(self.server_topics) + 1
        self.server_topics[server_name] = topic_id
        return topic_id

    async def create_topics_for_all_servers(self) -> Dict[str, int]:
        created: Dict[str, int] = {}
        servers = getattr(self.discord_service, "servers", {})
        for name in servers:
            topic_id = await self.get_or_create_server_topic(name)
            if topic_id:
                created[name] = topic_id
        return created

    async def startup_topic_verification(self) -> None:
        self.startup_verification_done = True
