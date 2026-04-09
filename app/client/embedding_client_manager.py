import asyncio

from langchain_community.embeddings import DashScopeEmbeddings
from app.conf.app_config import CNLLMConfig, app_config


class EmbeddingClientManager:
    def __init__(self, config:CNLLMConfig):
        self.client: DashScopeEmbeddings | None = None
        self.config: CNLLMConfig = config
    def init(self):
        self.client = DashScopeEmbeddings(
            model =self.config.embedding_model_name,
            dashscope_api_key=self.config.api_key

        )


embeddingClientManager = EmbeddingClientManager(app_config.cnllm)


if __name__ == "__main__":
    embeddingClientManager.init()
    client = embeddingClientManager.client


    async def test():
        text = "This is a test document."
        query_result = await client.aembed_query(text)

        print(query_result)
    asyncio.run(test())