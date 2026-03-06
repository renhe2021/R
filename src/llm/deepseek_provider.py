"""DeepSeek Provider（OpenAI 兼容接口）"""

import logging
import time

from .base import LLMProvider

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


class DeepSeekProvider(LLMProvider):
    def __init__(self, config: dict):
        from openai import OpenAI
        self._client = OpenAI(
            api_key=config.get("api_key", ""),
            base_url=config.get("base_url", "https://api.deepseek.com"),
        )
        self._model = config.get("model", "deepseek-chat")
        self._embedding_model = config.get("embedding_model", "bge-m3")
        self._temperature = config.get("temperature", 0.1)
        self._top_p = config.get("top_p", 0.2)

    def chat(self, messages: list[dict], system_prompt: str = "",
             temperature: float | None = None, top_p: float | None = None) -> str:
        for attempt in range(MAX_RETRIES):
            try:
                msgs = list(messages)
                if system_prompt:
                    msgs.insert(0, {"role": "system", "content": system_prompt})
                response = self._client.chat.completions.create(
                    model=self._model,
                    temperature=temperature if temperature is not None else self._temperature,
                    top_p=top_p if top_p is not None else self._top_p,
                    messages=msgs,
                )
                return response.choices[0].message.content
            except Exception as e:
                logger.warning(f"DeepSeek API 调用失败 (尝试 {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def embed(self, texts: list[str]) -> list[list[float]]:
        results = []
        batch_size = 32
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            for attempt in range(MAX_RETRIES):
                try:
                    response = self._client.embeddings.create(
                        model=self._embedding_model,
                        input=batch,
                    )
                    results.extend([d.embedding for d in response.data])
                    break
                except Exception as e:
                    logger.warning(f"DeepSeek Embedding 失败 (尝试 {attempt + 1}): {e}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(2 ** attempt)
                    else:
                        raise
        return results

    @property
    def supports_embedding(self) -> bool:
        return True
