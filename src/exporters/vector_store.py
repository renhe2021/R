"""ChromaDB 向量库构建器"""

import logging
from pathlib import Path

import chromadb

from ..models import TextChunk
from ..llm.base import LLMProvider

logger = logging.getLogger(__name__)


class VectorStoreBuilder:
    def __init__(self, persist_dir: str):
        self.persist_dir = Path(persist_dir)
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self._client = chromadb.PersistentClient(path=str(self.persist_dir))

    def build(self, chunks: list[TextChunk], collection_name: str,
              llm_provider: LLMProvider | None = None) -> None:
        """将文本块写入向量库"""
        # 清除旧数据（幂等）
        try:
            self._client.delete_collection(collection_name)
        except Exception:
            pass

        collection = self._client.get_or_create_collection(
            name=_safe_collection_name(collection_name),
            metadata={"hnsw:space": "cosine"},
        )

        if not chunks:
            logger.warning("没有文本块可写入向量库")
            return

        # 准备数据
        ids = [f"chunk_{c.chunk_index}" for c in chunks]
        documents = [c.content for c in chunks]
        metadatas = [
            {
                "chapter_title": c.chapter_title,
                "chunk_index": c.chunk_index,
                "book_title": c.metadata.get("book_title", ""),
                "investment_tags": ",".join(c.investment_tags) if c.investment_tags else "",
            }
            for c in chunks
        ]

        # 生成 embedding（如果有 LLM provider 且支持）
        embeddings = None
        if llm_provider and llm_provider.supports_embedding:
            logger.info(f"使用 LLM API 生成 embedding ({len(documents)} 个文本块)...")
            try:
                embeddings = llm_provider.embed(documents)
            except Exception as e:
                logger.warning(f"LLM embedding 生成失败，使用 ChromaDB 默认 embedding: {e}")
                embeddings = None

        # 分批写入（ChromaDB 单次最大 41666 条）
        batch_size = 500
        for i in range(0, len(ids), batch_size):
            end = min(i + batch_size, len(ids))
            kwargs = {
                "ids": ids[i:end],
                "documents": documents[i:end],
                "metadatas": metadatas[i:end],
            }
            if embeddings:
                kwargs["embeddings"] = embeddings[i:end]
            collection.add(**kwargs)

        logger.info(f"向量库构建完成: {len(ids)} 个文本块 -> collection '{collection_name}'")

    def get_collection(self, name: str):
        """获取已有的 collection"""
        return self._client.get_collection(_safe_collection_name(name))

    def list_collections(self) -> list[str]:
        """列出所有 collection"""
        return [c.name for c in self._client.list_collections()]


def _safe_collection_name(name: str) -> str:
    """ChromaDB collection 名称限制：3-63 字符，字母数字开头结尾"""
    import re
    name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
    name = name.strip("_-")[:63]
    if not name or not name[0].isalnum():
        name = "book_" + name
    if not name[-1].isalnum():
        name = name.rstrip("_-")
    return name or "default_collection"
