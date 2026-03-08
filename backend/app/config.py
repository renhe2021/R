"""Application settings — loaded from .env via pydantic-settings."""

from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings

ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


class Settings(BaseSettings):
    # LLM 代理 (优先使用)
    llm_api_key: str = ""
    llm_base_url: str = ""
    llm_model: str = "claude-sonnet-4-5-20250929"

    # 模型降级链 (逗号分隔，按优先级排序)
    llm_fallback_models: str = "gemini-3.1-pro,gpt-5,gemini-3-flash"

    # 原始 OpenAI (备用)
    openai_api_key: str = ""
    openai_model: str = "gpt-4o"
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-sonnet-4-20250514"
    perplexity_api_key: str = ""

    # Data sources
    bloomberg_api_key: str = ""
    tushare_token: str = ""
    binance_api_key: str = ""
    binance_secret_key: str = ""

    # Database
    database_url: str = "sqlite:///./r_system.db"
    chroma_persist_dir: str = "./chroma_data"

    @property
    def effective_api_key(self) -> str:
        """优先使用代理 key，回退到 OpenAI key。"""
        return self.llm_api_key or self.openai_api_key

    @property
    def effective_base_url(self) -> str | None:
        """有代理地址则使用，否则 None（默认 OpenAI）。"""
        return self.llm_base_url or None

    @property
    def effective_model(self) -> str:
        """优先使用代理模型，回退到 OpenAI 模型。"""
        if self.llm_api_key and self.llm_model:
            return self.llm_model
        return self.openai_model or "gpt-4o"

    @property
    def model_chain(self) -> list[str]:
        """返回完整的模型降级链：[首选模型, 备选1, 备选2, ...]"""
        chain = [self.effective_model]
        if self.llm_fallback_models:
            fallbacks = [m.strip() for m in self.llm_fallback_models.split(",") if m.strip()]
            for m in fallbacks:
                if m not in chain:
                    chain.append(m)
        return chain

    model_config = {"env_file": str(ENV_FILE), "env_file_encoding": "utf-8", "extra": "ignore"}


@lru_cache()
def get_settings() -> Settings:
    return Settings()
