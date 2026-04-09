from qdrant_client import AsyncQdrantClient

from app.conf.app_config import QdrantConfig, app_config


# from qdrant_client import QdrantClient


class QdrantClientManager:
    def __init__(self, config):
        self.client: AsyncQdrantClient | None = None
        self.config: QdrantConfig = config

    def _get_url(self):
        return f"http://{self.config.host}:{self.config.port}"

    def init(self):
        self.client = AsyncQdrantClient(url=self._get_url())
        return self.client

    async def close(self):
        await self.client.close()



qdrant_client_manager = QdrantClientManager(app_config.qdrant)
