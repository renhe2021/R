"""LLM Provider 抽象基类"""

from abc import ABC, abstractmethod


class LLMProvider(ABC):
    @abstractmethod
    def chat(self, messages: list[dict], system_prompt: str = "",
             temperature: float | None = None, top_p: float | None = None) -> str:
        """发送对话请求，返回模型回复文本"""
        ...

    @abstractmethod
    def embed(self, texts: list[str]) -> list[list[float]]:
        """批量生成文本 embedding 向量"""
        ...

    @property
    @abstractmethod
    def supports_embedding(self) -> bool:
        """是否支持 embedding 生成"""
        ...
