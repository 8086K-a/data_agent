import loguru
import yaml
from langchain_core.output_parsers import JsonOutputParser
from langchain_core.prompts import PromptTemplate
from langgraph.runtime import Runtime

from app.agent.context import DataAgentContext
from app.agent.llm import init_llm
from app.agent.state import DataAgentState
from app.prompt.prompt_loader import load_prompt


#使用state在节点中传递数据
async def filter_metric(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("过滤指标信息")


    query = state['query']
    metric_infos = state['metric_infos']

    llm = init_llm()
    prompt = PromptTemplate(
        template=load_prompt("filter_metric_info"),
        input_variables=["query", "metric_infos"],
    )
    output_parser = JsonOutputParser()
    chain = prompt | llm | output_parser

    yaml_metric_infos = yaml.dump(metric_infos, sort_keys=False, allow_unicode=True)

    result = chain.invoke({"query": query, "metric_infos": yaml_metric_infos})

    filtered_metric_infos = [metric_info for metric_info in metric_infos if metric_info['name'] in result]


    loguru.logger.info(f"过滤后指标信息: {filtered_metric_infos}")
    return {"metric_infos": filtered_metric_infos}