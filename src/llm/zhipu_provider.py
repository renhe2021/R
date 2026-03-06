"""智谱 GLM-4 Provider"""

import logging
import time

from .base import LLMProvider

logger = logging.getLogger(__name__)

MAX_RETRIES = 3


class ZhipuProvider(LLMProvider):
    def __init__(self, config: dict):
        from zhipuai import ZhipuAI
        self._client = ZhipuAI(api_key=config.get("api_key", ""))
        self._chat_model = config.get("chat_model", "glm-4")
        self._embedding_model = config.get("embedding_model", "embedding-2")
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
                    model=self._chat_model,
                    temperature=temperature if temperature is not None else self._temperature,
                    top_p=top_p if top_p is not None else self._top_p,
                    messages=msgs,
                )
                return response.choices[0].message.content
            except Exception as e:
                logger.warning(f"智谱 API 调用失败 (尝试 {attempt + 1}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def embed(self, texts: list[str]) -> list[list[float]]:
        results = []
        batch_size = 32
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            for text in batch:
                for attempt in range(MAX_RETRIES):
                    try:
                        response = self._client.embeddings.create(
                            model=self._embedding_model,
                            input=text,
                        )
                        results.append(response.data[0].embedding)
                        break
                    except Exception as e:
                        logger.warning(f"智谱 Embedding 失败 (尝试 {attempt + 1}): {e}")
                        if attempt < MAX_RETRIES - 1:
                            time.sleep(2 ** attempt)
                        else:
                            raise
        return results

    @property
    def supports_embedding(self) -> bool:
        return True
