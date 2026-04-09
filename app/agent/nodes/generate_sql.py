import yaml
from langchain_core.output_parsers import JsonOutputParser, StrOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime
from loguru import logger

from app.agent.context import DataAgentContext
from app.agent.llm import init_llm
from app.agent.state import DataAgentState
from app.prompt.prompt_loader import load_prompt


#使用state在节点中传递数据
async def generate_sql(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("生成sql")
    query = state['query']
    table_infos = state['table_infos']
    metric_infos = state['metric_infos']
    date_info = state['date_info']
    db_info = state['db_info']


    llm = init_llm()
    prompt = PromptTemplate(
        template=load_prompt("generate_sql"),
        input_variables=["query", "table_infos", "metric_infos", "date_info", "db_info"],
    )
    output_parser = StrOutputParser()
    chain = prompt | llm | output_parser

    sql = chain.invoke({"query": query,
                  "table_infos": yaml.dump(table_infos, allow_unicode=True, sort_keys=False),
                  "metric_infos": yaml.dump(metric_infos, allow_unicode=True, sort_keys=False),
                  "date_info": yaml.dump(date_info, allow_unicode=True, sort_keys=False),
                  "db_info": yaml.dump(db_info, allow_unicode=True, sort_keys=False)})
    logger.debug(sql)
    return {'sql' : sql}