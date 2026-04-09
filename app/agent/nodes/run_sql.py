from langgraph.runtime import Runtime
from loguru import logger

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState
from app.reopsitory.mysql.dw.dw_repository import DWRepository


#使用state在节点中传递数据
async def run_sql(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("执行sql")

    sql = state['sql']

    dw_repository:DWRepository = runtime.context['dw_repository']

    result = await dw_repository.run(sql)
    logger.debug(f"sql执行结果: {result}")