import asyncio
from elasticsearch import  AsyncElasticsearch

from app.conf.app_config import ESConfig, app_config


class EsClientManager:
    def __init__(self, config: ESConfig):
        self.config:ESConfig = config
        self.client: AsyncElasticsearch | None = None

    def _get_config(self):
        return f'http://{self.config.host}:{self.config.port}'

    def init(self):
        self.client = AsyncElasticsearch( hosts= [self._get_config()])


    async def close(self):
        await self.client.close()

es_client_manager = EsClientManager(app_config.es)
if __name__ == '__main__':
    es_client_manager.init()
    client = es_client_manager.client

    async def test():
        # await client.indices.create(
        #     index='book'
        # )

        await  client.index(
            index="books",
            document={
                "name": "Snow Crash",
                "author": "Neal Stephenson",
                "release_date": "1992-06-01",
                "page_count": 470
            },
        )

        resp = await client.search(
            index="books",
        )
        print(resp)
        await client.close()
    asyncio.run(test())