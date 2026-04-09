from datetime import date

from langgraph.runtime import Runtime
from loguru import logger

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState, DateInfoState, DBInfoState


#使用state在节点中传递数据
async def add_extra_context(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("添加额外上下文")
    dw_repository = runtime.context['dw_repository']

    today = date.today()
    date_str = today.strftime('%Y-%m-%d')
    weekday = today.strftime('%A')
    quarter = f"Q{(today.month - 1)//3 + 1}"

    date_info = DateInfoState(
        date=date_str,
        weekday=weekday,
        quarter=quarter,
    )

    db = await dw_repository.get_db()

    db_info = DBInfoState(
        version=db['version'],
        dialect=db['dialect'],
    )

    logger.debug(f"db_info: {db_info}")
    return {"date_info": date_info, "db_info": db_info}