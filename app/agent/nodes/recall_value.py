from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime
from loguru import logger

from app.agent.context import DataAgentContext
from app.agent.llm import init_llm
from app.agent.state import DataAgentState
from app.entities.value_info import ValueInfo
from app.prompt.prompt_loader import load_prompt


#使用state在节点中传递数据
async def recall_value(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("召回指值")

    query = state['query']
    llm = init_llm()
    #获取es
    es_repository = runtime.context['es_repository']

    keywords = state['keywords']


    prompt = PromptTemplate(
        template=load_prompt("extend_keywords_for_value_recall"),
        input_variables=["query"],
    )
    output_parser = JsonOutputParser()
    chain = prompt | llm | output_parser
    result = await chain.ainvoke({"query": query})

    keywords = set(keywords + result)

    value_infos_map: dict[str, ValueInfo] = {}

    for keyword in keywords:
        value_infos: list[ValueInfo] = await es_repository.search(  # 根据embedding检索相关字段
           score_threshold=0.6, limit=10, keyword=keyword, index_name="value_info"
        )
        for info in value_infos:
            if info.id not in value_infos_map:
                value_infos_map[info.id] = info
    retrieved_values_infos: list[ValueInfo] = list(value_infos_map.values())


    logger.info(retrieved_values_infos)

    return {"retrieved_values_infos": retrieved_values_infos}
