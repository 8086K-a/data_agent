from langgraph.runtime import Runtime
from loguru import logger

from app.agent.context import DataAgentContext
from app.agent.state import DataAgentState, MetricInfoState, TableInfoState, ColumnInfoState
from app.entities.column_info import ColumnInfo
from app.entities.metric_info import MetricInfo
from app.entities.value_info import ValueInfo


#使用state在节点中传递数据
async def merge_retrieved_info(state:DataAgentState, runtime:Runtime[DataAgentContext]):
    writer = runtime.stream_writer
    writer("合并召回信息")


    #获取召回的指标信息，列信息，值信息
    retrieved_column_infos: list[ColumnInfo] = state['retrieved_column_infos']
    retrieved_metric_infos: list[MetricInfo] = state['retrieved_metric_infos']
    retrieved_values_infos: list[ValueInfo] = state['retrieved_values_infos']
    meta_repository = runtime.context['meta_repository']


    retrieved_column_infos_map = {col.id: col for col in retrieved_column_infos}
    #1.将指标信息中的列字段添加到列信息中
    for retrieved_metric_info in retrieved_metric_infos:
        relevant_columns_ids = retrieved_metric_info.relevant_columns
        # 根据列id，从元数据库中查出对应的列信息
        for relevant_columns_id in relevant_columns_ids:
            if relevant_columns_id not in retrieved_column_infos_map:
                column_info = await meta_repository.get_column_info_by_id(relevant_columns_id)
                retrieved_column_infos_map[relevant_columns_id] = column_info

    #2.将召回的字段取值，添加到列信息的example中
    for retrieved_values_info in retrieved_values_infos:
        value = retrieved_values_info.value
        column_id = retrieved_values_info.column_id
        #如果map
        if column_id not in retrieved_column_infos_map:
            column_info = await meta_repository.get_column_info_by_id(column_id)
            retrieved_column_infos_map[column_id] = column_info
        if value not in retrieved_column_infos_map:
            retrieved_column_infos_map[column_id].examples.append(value)



    """{
    "t_order": [
        ColumnInfo(name="province"),
        ColumnInfo(name="amount"),
    ],
    "t_user": [
        ColumnInfo(name="user_id"),
    ]
}"""
    table_to_columns_map:dict[str, list[ColumnInfo]] = {}
    for column_info in retrieved_column_infos_map.values():
        table_id = column_info.table_id
        if table_id not in table_to_columns_map:
            table_to_columns_map[table_id] = []
        table_to_columns_map[table_id].append(column_info)

    #手动添加主外键信息
    for table_id in table_to_columns_map.keys():
        key_columns: list[ColumnInfo] = await meta_repository.get_key_columns_by_table_id(table_id)
        column_ids = [column_info.id for column_info in table_to_columns_map[table_id]]
        for key_column in key_columns:
            if key_column.id not in column_ids:
                table_to_columns_map[table_id].append(key_column)


    #拼接成TableInfoState格式
    table_infos: list[TableInfoState] = []
    for table_id, column_infos in table_to_columns_map.items():
        logger.debug(f"table_id:{table_id}, column_infos:{column_infos}")
        table_info = await meta_repository.get_table_info_by_id(table_id)
        table_info_state = TableInfoState(
            name=table_info.name,
            role=table_info.role,
            description=table_info.description,
            columns=[
                ColumnInfoState(
                    name=column_info.name,
                    type=column_info.type,
                    role=column_info.role,
                    examples=column_info.examples,
                    description=column_info.description,
                    alias=column_info.alias
                ) for column_info in column_infos
            ]
        )
        table_infos.append(table_info_state)


    #从列信息的map中，找出对应的表id，去元数据库查出表的源信息

    # #将原指标信息转化为新格式
    metric_infos: list[MetricInfoState] = []
    for retrieved_metric_info in retrieved_metric_infos:
        metric_infos.append(MetricInfoState(
            name=retrieved_metric_info.name,
            description=retrieved_metric_info.description,
            alias=retrieved_metric_info.alias,
            retrieved_columns=retrieved_metric_info.relevant_columns
        ))



    logger.debug(f"table_infos:{table_infos}")
    logger.debug(f"metric_infos:{metric_infos}")
    return {
        'table_infos': table_infos,
        'metric_infos': metric_infos
    }




