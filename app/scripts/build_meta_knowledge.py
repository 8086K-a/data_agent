import argparse
import asyncio
import logging
from pathlib import Path

import qdrant_client

from app.client.embedding_client_manager import embeddingClientManager
from app.client.es_client_manager import es_client_manager
from app.client.mysql_client_manager import dw_db_manager, meta_db_manager
from app.client.qdrant_client_manager import qdrant_client_manager
from app.reopsitory.es.es_repository import ESRepository
from app.reopsitory.mysql.dw.dw_repository import DWRepository
from app.reopsitory.mysql.meta.meta_repository import MetaRepository
from app.reopsitory.qdrant.qdrant_repository import QdrantRepository
from app.service.meta_knowledge_service import MetaKnowledgeService


async def build(config_path: Path):
    dw_db_manager.init()
    meta_db_manager.init()
    qdrant_client_manager.init()
    embeddingClientManager.init()
    es_client_manager.init()

    logging.info("初始化成功")
    async with meta_db_manager.session_factory() as meat_session, dw_db_manager.session_factory() as dw_session:
        meta_repository = MetaRepository(meat_session)
        dw_repository = DWRepository(dw_session)
        qdrant_repository = QdrantRepository(qdrant_client_manager.client)
        es_repository = ESRepository(es_client_manager.client)

        meta_knowledge_service = MetaKnowledgeService(meta_repository=meta_repository, dw_repository=dw_repository, qdrant_repository=qdrant_repository,embedding_client=embeddingClientManager.client, es_repository = es_repository)
        await meta_knowledge_service.build(config_path)
    await qdrant_client_manager.close()
    await es_client_manager.close()


async def main():
    parser = argparse.ArgumentParser(description="读取配置文件")


    parser.add_argument(
        "-c", "--config",
        type=str,
        required=True,
        help="配置文件路径"
    )

    args = parser.parse_args()

    config_path = Path(args.config)

    await build(config_path)



if __name__ == "__main__":

    asyncio.run(main())