from typing import TypedDict

from langchain_community.embeddings import DashScopeEmbeddings

from app.reopsitory.es.es_repository import ESRepository
from app.reopsitory.mysql.dw.dw_repository import DWRepository
from app.reopsitory.mysql.meta.meta_repository import MetaRepository
from app.reopsitory.qdrant.qdrant_repository import QdrantRepository


class DataAgentContext(TypedDict):
    qdrant_repository: QdrantRepository
    embedding_client: DashScopeEmbeddings
    es_repository: ESRepository
    meta_repository: MetaRepository
    dw_repository: DWRepository