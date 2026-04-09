import yaml
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import init_llm
from app.agent.state import DataAgentState
from app.prompt.prompt_loader import load_prompt


#使用state在节点中传递数据
async def correct_sql(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("纠正SQL")
    query = state['query']
    table_infos = state['table_infos']
    metric_infos = state['metric_infos']
    date_info = state['date_info']
    db_info = state['db_info']
    error = state['error']
    sql = state['sql']

    llm = init_llm()
    prompt = PromptTemplate(
        template=load_prompt("correct_sql"),
        input_variables=["query", "table_infos", "metric_infos", "date_info", "db_info", "error", "sql"],
    )
    output_parser = StrOutputParser()
    chain = prompt | llm | output_parser

    sql1 = chain.invoke({"query": query,
                        "table_infos": yaml.dump(table_infos, allow_unicode=True, sort_keys=False),
                        "metric_infos": yaml.dump(metric_infos, allow_unicode=True, sort_keys=False),
                        "date_info": yaml.dump(date_info, allow_unicode=True, sort_keys=False),
                        "db_info": yaml.dump(db_info, allow_unicode=True, sort_keys=False),
                        "error": yaml.dump(error, allow_unicode=True, sort_keys=False),
                        "sql": yaml.dump(sql, allow_unicode=True, sort_keys=False)})

    return {'sql': sql1}
