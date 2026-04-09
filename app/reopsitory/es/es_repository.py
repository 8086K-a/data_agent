import uuid
from elasticsearch.helpers import async_bulk

from app.entities.value_info import ValueInfo


class ESRepository:
    index_mappings = {
        "dynamic": False,
        "properties": {
            "id": {"type": "keyword"},
            "value": {
                "type": "text",
                "analyzer": "ik_max_word",
                "search_analyzer": "ik_max_word",
                "fields": {
                    "keyword": {"type": "keyword"}
                }
            },
            "column_id": {"type": "keyword"}
        }
    }
    def __init__(self, client):
        self.client = client

    async def ensure_index(self, index_name):
        if not await self.client.indices.exists(index=index_name):
            await self.client.indices.create(
                index=index_name,
                mappings=self.index_mappings
            )

    async def save(self, index_name: str, info: list[dict], batch_size=500):

        for i in range(0, len(info), batch_size):
            batch = info[i:i + batch_size]

            actions = [
                {
                    "_index": index_name,

                    "_source": {
                        "id":item['id'],
                        "column_id": item["column_id"],
                        "value": item["value"],
                    }
                }
                for item in batch
            ]

            await async_bulk(self.client, actions)

    async def search(self,index_name, limit:int = 10 , keyword: str = "", score_threshold: float = 0.7) -> list[ValueInfo]:
        query = {
            "query": {
                "match": {
                    "value": keyword
                }
            },
            "size": limit,
            "min_score": score_threshold
        }

        response = await self.client.search(index=index_name, body=query)
        hits = response["hits"]["hits"]
        return [ValueInfo(**hit["_source"]) for hit in hits]