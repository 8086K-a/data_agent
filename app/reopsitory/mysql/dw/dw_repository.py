from sqlalchemy import text


class DWRepository:
    def __init__(self, session):
        self.session = session

    async def get_types_list(self, table_name):
        sql = f"SHOW COLUMNS FROM {table_name}"
        result = await self.session.execute(text(sql))
        rows = result.mappings().fetchall()
        return {row["Field"]: row["Type"] for row in rows}

    async def get_column_values(self,table_name, column_name):
        sql = f"SELECT distinct `{column_name}` FROM {table_name} limit 10"
        result = await self.session.execute(text(sql))
        rows = result.mappings().fetchall()
        return [row[column_name] for row in rows]

    async def get_distinct_column_values(self, table_name, column_name):
        sql = f"SELECT distinct `{column_name}` FROM {table_name}"
        result = await self.session.execute(text(sql))
        rows = result.mappings().fetchall()
        return [row[column_name] for row in rows]

    async def get_db(self):
        sql = "select version()"
        result = await self.session.execute(text(sql))
        version = result.scalar()
        dialect = self.session.bind.dialect.name
        return {
            "dialect": dialect,
            "version": version,
        }
    async def validate_sql(self, sql):
        sql = f"explain {sql}"
        await self.session.execute(text(sql))

    async def run(self, sql):
        result = await self.session.execute(text(sql))
        rows = result.mappings().fetchall()
        return [dict(row) for row in rows]
