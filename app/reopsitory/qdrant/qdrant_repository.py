from qdrant_client import AsyncQdrantClient
from qdrant_client.http.models import PointStruct
from qdrant_client.models import VectorParams, Distance

from app.conf.app_config import app_config
from app.entities.column_info import ColumnInfo
from app.entities.metric_info import MetricInfo


class QdrantRepository:


        def __init__(self, client: AsyncQdrantClient):
            self.client = client



        async def ensure_collection(self, collection_name):
            if not await self.client.collection_exists(collection_name):
                await self.client.create_collection(
                    collection_name=collection_name,
                    vectors_config=VectorParams(size=app_config.qdrant.embedding_size, distance=Distance.COSINE)
                )

        async def upsert(self, qdrant_points,collection_name , batch_size: int = 100):
            if not qdrant_points:
                return
            for i in range(0, len(qdrant_points), batch_size):
                batch = qdrant_points[i:i + batch_size]

                points = [
                    PointStruct(
                        id=str(p["id"]),
                        vector=p["vector"],
                        payload=p.get("payload", {}),
                    )
                    for p in batch
                ]

                await self.client.upsert(
                    collection_name=collection_name,
                    points=points,
                )

        async def search(self,collection_name, embedding:list[float], score_threshold:float = 0.7, limit:int = 20 ) -> list:
            search_result = await self.client.query_points(
                collection_name=collection_name,
                query=embedding,
                limit=limit,
                score_threshold=score_threshold,
            )
            return [ColumnInfo(**item.payload) for item in search_result.points] if collection_name == 'column_info' else [MetricInfo(**item.payload) for item in search_result.points]


