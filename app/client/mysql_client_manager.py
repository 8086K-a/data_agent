from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


from app.conf.app_config import app_config, DBConfig


class MYSQLClientManager:
    def __init__(self,mysql_config: DBConfig):
            self.config = mysql_config
            self.engine = None
            self.session_factory = None

    def _get_url(self):
        return f"mysql+asyncmy://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}?charset=utf8mb4"
    def init(self):
        self.engine = create_async_engine(self._get_url(),pool_pre_ping=True,pool_size=10)
        self.session_factory = async_sessionmaker(self.engine, autoflush=True, expire_on_commit=False)

    async def close(self):
        await self.engine.dispose()

meta_db_manager = MYSQLClientManager(app_config.db_meta)
dw_db_manager = MYSQLClientManager(app_config.db_dw)