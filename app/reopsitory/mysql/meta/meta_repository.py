from typing import List

from loguru import logger
from sqlalchemy import text

from app.entities.column_info import ColumnInfo
from app.entities.table_info import TableInfo


class MetaRepository:
    def __init__(self, session):
        self.session = session


    async def get_column_type_list(self, table_name):
        sql = f"SHOW COLUMNS FROM {table_name}"
        result =  await self.session.execute(text(sql))
        rows = result.mappings().fetchall()
        return rows


    async def save_all(self, *lst_list):
        for lst in lst_list:
            self.session.add_all(lst)

    async def get_column_info_by_id(self, id: str):
        sql = text("SELECT * FROM column_info WHERE id = :id")
        result = await self.session.execute(sql, {"id": id})
        row = result.mappings().fetchone()
        return ColumnInfo(
            id = row['id'],
            name = row['name'],
            type = row['type'],
            role = row['role'],
            examples = row['examples'],
            description = row['description'],
            alias = row['alias'],
            table_id = row['table_id']
        )

    async def get_table_info_by_id(self, id: str):
        sql = text("SELECT * FROM table_info WHERE id = :id")
        result = await self.session.execute(sql, {"id": id})
        row = result.mappings().fetchone()
        logger.info(row)

        return TableInfo(
            id = row['id'],
            name = row['name'],
            role = row['role'],
            description = row['description']
        )
    async def get_key_columns_by_table_id(self, table_id: str)->List[ColumnInfo]:
        sql = text("SELECT * FROM column_info WHERE table_id = :table_id AND role IN ('primary_key','foreign_key')")
        result = await self.session.execute(sql, {"table_id": table_id})
        rows = result.mappings().fetchall()
        key_columns = []
        for row in rows:
            key_columns.append(ColumnInfo(
            **dict(row)
            ))
        return key_columns