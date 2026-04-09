# 日志配置
from dataclasses import dataclass
from pathlib import Path

from omegaconf import OmegaConf

@dataclass
class File:
          enable: bool
          level: str
          path: str
          rotation: str
          retention: str

@dataclass
class Console:
          enable: bool
          level: str

@dataclass
class LoggingConfig:
          file: File
          console: Console

# 数据库配置
@dataclass
class DBConfig:
          host: str
          port: int
          user: str
          password: str
          database: str

@dataclass
class QdrantConfig:
          host: str
          port: int
          embedding_size: int

@dataclass
class EmbeddingConfig:
          host: str
          port: int
          model: str

@dataclass
class ESConfig:
          host: str
          port: int
          index_name: str

@dataclass
class LLMConfig:
          model_name: str
          api_key: str
          base_url: str

@dataclass
class CNLLMConfig:
          model_name: str
          embedding_model_name: str
          api_key: str
          base_url: str


@dataclass
class AppConfig:
          logging: LoggingConfig
          db_meta: DBConfig
          db_dw: DBConfig
          qdrant: QdrantConfig
          embedding: EmbeddingConfig
          es: ESConfig
          llm: LLMConfig
          cnllm: CNLLMConfig


config = Path(__file__).parents[2] / 'conf' / 'app_config.yaml'

schema = OmegaConf.structured(AppConfig)
context = OmegaConf.load(config)
app_config : AppConfig =  OmegaConf.to_object(OmegaConf.merge(schema, context))
