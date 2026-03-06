"""语义检索模块"""

import logging
from dataclasses import dataclass

import chromadb

from .llm.base import LLMProvider

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    content: str
    chapter_title: str
    book_title: str
    score: float
    metadata: dict


class KnowledgeSearcher:
    def __init__(self, persist_dir: str, llm_provider: LLMProvider | None = None):
        self._client = chromadb.PersistentClient(path=persist_dir)
        self._llm = llm_provider

    def search(self, query: str, collection_name: str,
               top_k: int = 5, filter_tags: list[str] | None = None) -> list[SearchResult]:
        """语义检索"""
        from .exporters.vector_store import _safe_collection_name

        try:
            collection = self._client.get_collection(_safe_collection_name(collection_name))
        except Exception:
            logger.error(f"Collection '{collection_name}' 不存在")
            return []

        # 构建查询参数
        kwargs: dict = {"n_results": top_k}

        # 如果有 LLM provider 且支持 embedding，用它生成 query embedding
        if self._llm and self._llm.supports_embedding:
            try:
                query_embedding = self._llm.embed([query])[0]
                kwargs["query_embeddings"] = [query_embedding]
            except Exception:
                kwargs["query_texts"] = [query]
        else:
            kwargs["query_texts"] = [query]

        # metadata 过滤
        if filter_tags:
            kwargs["where"] = {"investment_tags": {"$contains": filter_tags[0]}}

        results = collection.query(**kwargs)

        search_results = []
        if results and results["documents"]:
            docs = results["documents"][0]
            metas = results["metadatas"][0] if results["metadatas"] else [{}] * len(docs)
            distances = results["distances"][0] if results["distances"] else [0.0] * len(docs)

            for doc, meta, dist in zip(docs, metas, distances):
                search_results.append(SearchResult(
                    content=doc,
                    chapter_title=meta.get("chapter_title", ""),
                    book_title=meta.get("book_title", ""),
                    score=1.0 - dist,  # cosine distance -> similarity
                    metadata=meta,
                ))

        return search_results

    def list_books(self) -> list[str]:
        """列出所有已导入的书籍"""
        return [c.name for c in self._client.list_collections()]
