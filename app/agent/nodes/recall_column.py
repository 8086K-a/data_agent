import logging
from typing import List

from elasticsearch.esql.functions import top
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime
from loguru import logger

from app.agent.context import DataAgentContext
from app.agent.llm import init_llm
from app.agent.state import DataAgentState
from app.entities.column_info import ColumnInfo
from app.prompt.prompt_loader import load_prompt


# 使用state在节点中传递数据
async def recall_column(state: DataAgentState, runtime: Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("召回字段")

    keywords = state["keywords"]
    query = state["query"]
    qdrant_repository = runtime.context["qdrant_repository"]
    embedding_client = runtime.context["embedding_client"]

    # llm拓展关键词
    llm = init_llm()
    prompt = PromptTemplate(
        template=load_prompt("extend_keywords_for_column_recall"),
        input_variables=["query"],
    )
    output_parser = JsonOutputParser()
    chain = prompt | llm | output_parser
    result = await chain.ainvoke({"query": query})


    keywords = set(keywords + result)

    # 得到召回结果，去检索信息丛qdrant
    embeddings = await embedding_client.aembed_documents(list(keywords))
    #去重
    column_info_map: dict[str, ColumnInfo] = {}

    for embedding in embeddings:
        column_info: list[ColumnInfo] = await qdrant_repository.search( #根据embedding检索相关字段
            embedding=embedding, score_threshold=0.6, limit=10, collection_name="column_info"
        )

        #colum_info可能会有重复的情况，所以要去重
        for info in column_info:
            column_info_map[info.id] = info

    retrieved_column_infos: List[ColumnInfo] = list(column_info_map.values())
    # logger.info(recall_column_infos)
    return {"retrieved_column_infos": retrieved_column_infos}

