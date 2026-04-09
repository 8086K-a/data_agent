import logging
import uuid
from dataclasses import asdict
from pathlib import Path

from langchain_community.embeddings import DashScopeEmbeddings
from omegaconf import OmegaConf

from app.conf.meta_config import MetaConfig
from app.entities.column_info import ColumnInfo
from app.entities.column_metric import ColumnMetric
from app.entities.metric_info import MetricInfo
from app.entities.table_info import TableInfo
from app.models.column_info import ColumnInfoMySQL
from app.models.column_metric import ColumnMetricMySQL
from app.models.metric_info import MetricInfoMySQL
from app.models.table_info import TableInfoMySQL
from app.reopsitory.es.es_repository import ESRepository
from app.reopsitory.mysql.dw.dw_repository import DWRepository
from app.reopsitory.mysql.mappers.base_mapper import BaseMapper
from app.reopsitory.mysql.meta.meta_repository import MetaRepository
from app.reopsitory.qdrant.qdrant_repository import QdrantRepository


class MetaKnowledgeService:
    def __init__(self, meta_repository: MetaRepository, dw_repository: DWRepository, qdrant_repository: QdrantRepository, embedding_client: DashScopeEmbeddings, es_repository: ESRepository):
        self.meta_repository = meta_repository
        self.dw_repository = dw_repository
        self.qdrant_repository = qdrant_repository

        self.embedding_client = embedding_client
        self.es_repository = es_repository

    async def _embed_metrics_and_save_to_qdrant(self, metric_infos):
        collection_name = 'metric_info'
        await self.qdrant_repository.ensure_collection(collection_name= collection_name)

        texts = []
        payloads = []

        for metric in metric_infos:
            payload = asdict(metric)

            for text in [metric.name, metric.description, *metric.alias]:
                texts.append(text)
                payloads.append(payload)


        await self.embed_and_upsert(collection_name, texts, payloads)

    async def _save_columns_to_qdrant(self, columns: list[ColumnInfo]):

        collection_name = 'column_info'

        await self.qdrant_repository.ensure_collection(collection_name)

        # 1. 收集需要 embedding 的文本 + payload
        texts = []
        payloads = []

        for column in columns:
            payload = asdict(column)

            for text in [column.name, column.description, *column.alias]:
                texts.append(text)
                payloads.append(payload)

        await self.embed_and_upsert(collection_name ,texts, payloads)


    async def _save_metrics_to_meta_db(self, meta_config: MetaConfig):
        metric_infos: list[MetricInfo] = []
        column_metrics: list[ColumnMetric] = []


        for metric in meta_config.metrics:
            metric_info = MetricInfo(
                id=metric.name,
                name=metric.name,
                description=metric.description,
                relevant_columns=metric.relevant_columns,
                alias=metric.alias
            )
            column_metric = ColumnMetric(
                column_id=metric.relevant_columns[0],
                metric_id=metric.name
            )

            metric_infos.append(metric_info)
            column_metrics.append(column_metric)

        metrics_model = [BaseMapper.to_model(metric, MetricInfoMySQL) for metric in metric_infos]
        column_metrics_model = [BaseMapper.to_model(column_metric, ColumnMetricMySQL) for column_metric in column_metrics]
        async with self.meta_repository.session.begin():
            await self.meta_repository.save_all(metrics_model, column_metrics_model)
        return metric_infos


    async def embed_and_upsert(
            self,
            collection_name: str,
            texts: list[str],
            payloads: list[dict],
            batch_size: int = 500,
    ):
        ids = [str(uuid.uuid4()) for _ in texts]

        vectors = []

        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            batch_vectors = await self.embedding_client.aembed_documents(batch_texts)
            vectors.extend(batch_vectors)

        qdrant_points = [
            {
                "id": id_,
                "vector": vector,
                "payload": payload,
            }
            for id_, vector, payload in zip(ids, vectors, payloads)
        ]

        await self.qdrant_repository.upsert(qdrant_points, collection_name)




    async def _save_tables_to_meta_db(self, meta_config: MetaConfig)-> dict:
        tables: list[TableInfo] = []
        columns: list[ColumnInfo] = []

        sync_columns: list[str] = []
        for table in meta_config.tables:
            table_info = TableInfo(
                id=table.name,
                name=table.name,
                description=table.description,
                role=table.role
            )

            tables.append(table_info)
            column_type = await self.dw_repository.get_types_list(table.name)
            for column in table.columns:
                if column.sync:
                    sync_columns.append(f"{table.name}.{column.name}")

                example = await self.dw_repository.get_column_values(table.name, column.name)

                column_info = ColumnInfo(
                    id=f'{table.name}.{column.name}',
                    name=column.name,
                    description=column.description,
                    role=column.role,
                    alias=column.alias,
                    examples=example,
                    type=column_type[column.name],
                    table_id=table.name,
                )
                columns.append(column_info)

        # 2.将entity对象列表转化成 model对象列表，使用通用的mapper
        tables_model = [BaseMapper.to_model(table, TableInfoMySQL) for table in tables]
        columns_model = [BaseMapper.to_model(column, ColumnInfoMySQL) for column in columns]
        # 3.操作写入元数据库
        async with self.meta_repository.session.begin():
            await self.meta_repository.save_all(tables_model, columns_model)

        return {'columns': columns, 'sync_columns': sync_columns}



    async  def _save_tables_to_es(self, columns: list[ColumnInfo], sync_columns: list[str]):
        # 1.
        index_name = 'value_info'
        # 确保index存在es数据库中，如果没有则创建
        await self.es_repository.ensure_index(index_name)

        value_info: list[dict] = []
        for column in columns:
            if column.id not in sync_columns:
                continue
            # 从数据库根据表名字和字段名字查询出所有的值
            table_name = column.id.split('.')[0]

            # 一个方法，从该表的特定字段去重取值
            distinct_values = await self.dw_repository.get_distinct_column_values(table_name, column.name)
            for distinct_value in distinct_values:
                value_info.append({
                    'id': f'{column.id}::{distinct_value}',
                    'column_id': column.id,
                    'value': distinct_value
                })

        # 2.存到es数据库中，首先要有一个叫value_info表
        await  self.es_repository.save(index_name, value_info)

    async def build(self, config_path: Path):
    #目标1.同步表信息到元数据库
    #1.读取所有的表信息和字段信息（yaml）文件中,转化成entity对象列表
        schema = OmegaConf.structured(MetaConfig)
        context = OmegaConf.load(config_path)
        meta_config: MetaConfig = OmegaConf.to_object(OmegaConf.merge(schema, context))
        logging.info(f"配置文件 {config_path} 解析成功")
        if meta_config.tables:
            tmp = await self._save_tables_to_meta_db(meta_config)
            logging.info(f"表信息和字段信息保存到元数据库成功")
            await self._save_columns_to_qdrant(tmp['columns'])
            logging.info(f"字段信息保存到qdrant成功")
            #建立全文索引
            await self._save_tables_to_es(**tmp)
            logging.info(f"字段信息保存到es成功")



    #目标.指标信息到元数据库
    #写入指标信息
        if meta_config.metrics:

            metric_infos = await self._save_metrics_to_meta_db(meta_config)
            logging.info(f"指标信息保存到元数据库成功")
            await self._embed_metrics_and_save_to_qdrant(metric_infos)
            logging.info(f"指标信息保存到qdrant成功")



