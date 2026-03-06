"""配置管理"""

import os
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel


class ClaudeConfig(BaseModel):
    api_key: str = ""
    base_url: str = "https://api.anthropic.com"
    model: str = "claude-opus-4-6"
    embedding_model: str = "bge-m3"
    temperature: float = 0.1
    top_p: float = 0.2


class ZhipuConfig(BaseModel):
    api_key: str = ""
    base_url: str = ""
    chat_model: str = "glm-4"
    embedding_model: str = "embedding-2"
    temperature: float = 0.1
    top_p: float = 0.2


class DeepSeekConfig(BaseModel):
    api_key: str = ""
    base_url: str = "https://api.deepseek.com"
    model: str = "deepseek-chat"
    embedding_model: str = "bge-m3"
    temperature: float = 0.1
    top_p: float = 0.2


class LLMConfig(BaseModel):
    default_provider: str = "deepseek"
    claude: ClaudeConfig = ClaudeConfig()
    zhipu: ZhipuConfig = ZhipuConfig()
    deepseek: DeepSeekConfig = DeepSeekConfig()


class ChunkConfig(BaseModel):
    chunk_size: int = 800
    chunk_overlap: int = 100


class StorageConfig(BaseModel):
    output_dir: str = "data/output"
    knowledge_dir: str = "data/knowledge"
    vectordb_dir: str = "data/vectordb"


class BloombergConfig(BaseModel):
    host: str = "localhost"
    port: int = 8194


class FMPConfig(BaseModel):
    api_key: str = ""


class FinnhubConfig(BaseModel):
    api_key: str = ""


class DataConfig(BaseModel):
    default_provider: str = "auto"  # auto / bloomberg / yfinance / fmp / finnhub
    bloomberg: BloombergConfig = BloombergConfig()
    fmp: FMPConfig = FMPConfig()
    finnhub: FinnhubConfig = FinnhubConfig()


class AppConfig(BaseModel):
    llm: LLMConfig = LLMConfig()
    chunking: ChunkConfig = ChunkConfig()
    storage: StorageConfig = StorageConfig()
    data: DataConfig = DataConfig()


def load_config(config_path: Optional[str] = None) -> AppConfig:
    """加载配置文件，支持环境变量覆盖"""
    if config_path is None:
        config_path = os.environ.get("BOOK_KB_CONFIG", "config.yaml")

    config_file = Path(config_path)
    data = {}

    if config_file.exists():
        with open(config_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

    config = AppConfig(**data)

    # 环境变量覆盖 API Key
    if env_key := os.environ.get("CLAUDE_API_KEY"):
        config.llm.claude.api_key = env_key
    if env_key := os.environ.get("ZHIPU_API_KEY"):
        config.llm.zhipu.api_key = env_key
    if env_key := os.environ.get("DEEPSEEK_API_KEY"):
        config.llm.deepseek.api_key = env_key
    if env_key := os.environ.get("FMP_API_KEY"):
        config.data.fmp.api_key = env_key
    if env_key := os.environ.get("FINNHUB_API_KEY"):
        config.data.finnhub.api_key = env_key

    return config
