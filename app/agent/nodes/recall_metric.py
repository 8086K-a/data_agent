from typing import List

from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime
from loguru import logger

from app.agent.context import DataAgentContext
from app.agent.llm import init_llm
from app.agent.state import DataAgentState
from app.entities.metric_info import MetricInfo
from app.prompt.prompt_loader import load_prompt


#使用state在节点中传递数据
async def recall_metric(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("召回指标信息")
    collection_name = "metric_info"
    #获取qdrant_repository召回信息
    qdrant_repository = runtime.context['qdrant_repository']
    embedding_client = runtime.context['embedding_client']
    #获取关键词
    keywords = state['keywords']

    #获取用户的问题
    query = state["query"]

    # llm拓展关键词
    llm = init_llm()
    prompt = PromptTemplate(
        template=load_prompt("extend_keywords_for_metric_recall"),
        input_variables=["query"],
    )
    output_parser = JsonOutputParser()
    chain = prompt | llm | output_parser
    result = await chain.ainvoke({"query": query})

    keywords = set(keywords + result)

    embeddings = await embedding_client.aembed_documents(list(keywords))
    # 去重
    column_info_map: dict[str, MetricInfo] = {}

    for embedding in embeddings:
        column_info: list[MetricInfo] = await qdrant_repository.search(  # 根据embedding检索相关字段
            embedding=embedding, score_threshold=0.6, limit=10, collection_name=collection_name
        )

        # colum_info可能会有重复的情况，所以要去重
        for info in column_info:
            column_info_map[info.id] = info

    retrieved_metric_infos: List[MetricInfo] = list(column_info_map.values())
    logger.info(retrieved_metric_infos)
    return {"retrieved_metric_infos": retrieved_metric_infos}


