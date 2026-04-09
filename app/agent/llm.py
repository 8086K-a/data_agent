from langchain.chat_models import init_chat_model

from app.conf.app_config import app_config

def init_llm():
    return init_chat_model(
    model=app_config.cnllm.model_name,
    model_provider='openai',
    api_key=app_config.cnllm.api_key,
    base_url=app_config.cnllm.base_url,
    temperature=0.0
)
