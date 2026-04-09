from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import init_llm
from app.agent.state import DataAgentState
from app.prompt.prompt_loader import load_prompt
from app.reopsitory.mysql.dw.dw_repository import DWRepository


#使用state在节点中传递数据
async def validate_sql(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("校验sql")
    dw_repository: DWRepository = runtime.context['dw_repository']

    sql = state['sql']
    try:
        await dw_repository.validate_sql(sql)
        return {"error": None}
    except Exception as e:
        error = str(e)
        return {"error": error}


