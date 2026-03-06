from .base import LLMProvider
from .claude_provider import ClaudeProvider
from .zhipu_provider import ZhipuProvider
from .deepseek_provider import DeepSeekProvider


def get_llm_provider(provider_name: str, config: dict) -> LLMProvider:
    """根据 provider 名称返回对应的 LLM 实例"""
    providers = {
        "claude": ClaudeProvider,
        "zhipu": ZhipuProvider,
        "deepseek": DeepSeekProvider,
    }
    provider_cls = providers.get(provider_name)
    if provider_cls is None:
        raise ValueError(f"不支持的 LLM provider: {provider_name}，支持: {', '.join(providers.keys())}")
    return provider_cls(config.get(provider_name, {}))
