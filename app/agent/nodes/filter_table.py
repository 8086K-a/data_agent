import yaml
from dns.e164 import query
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import init_llm
from app.agent.state import DataAgentState
from app.prompt.prompt_loader import load_prompt


#使用state在节点中传递数据
async def filter_table(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("过滤表信息")

    table_infos = state['table_infos']
    query = state['query']

    llm = init_llm()
    prompt = PromptTemplate(
        template=load_prompt("filter_table_info"),
        input_variables=["query", "table_infos"],
    )
    output_parser = JsonOutputParser()
    chain = prompt | llm | output_parser

    yaml_table_infos = yaml.dump(table_infos, sort_keys=False, allow_unicode=True)

    result =  chain.invoke({"query": query, "table_infos": yaml_table_infos})

    #  result = {'dim_region': ['region_id', 'region_name'], 'fact_order': ['order_amount', 'region_id']}

    filtered_table_infos = []
    for table_info in table_infos:
        if table_info['name'] in result:
           table_info['columns'] = [column_info for column_info in table_info['columns'] if column_info['name'] in result[table_info['name']]]
        filtered_table_infos.append(table_info)



    return {"table_infos": filtered_table_infos}