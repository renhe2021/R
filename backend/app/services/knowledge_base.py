"""Knowledge base service - ChromaDB vector store + theory management"""

import os
import logging
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from sqlalchemy.orm import Session

from app.models.knowledge import TheoryFramework, UserDocument, ResearchResult
from app.models.value_investing import InvestorLibrary
from app.services.perplexity_service import PerplexityService
from app.services.book_indexer import BookIndexer

logger = logging.getLogger(__name__)

THEORIES_DIR = Path(__file__).parent.parent / "knowledge" / "theories"
USER_DOCS_DIR = Path(__file__).parent.parent / "knowledge" / "user_docs"
RESEARCH_DIR = Path(__file__).parent.parent / "knowledge" / "research"

# Built-in theory metadata for structured storage
BUILTIN_THEORIES = [
    {
        "name": "Value Investing (Graham-Buffett)",
        "type": "value",
        "description": "Focuses on buying stocks below intrinsic value with a margin of safety. Core principles from Benjamin Graham and Warren Buffett.",
        "applicable_markets": ["us", "cn", "hk"],
        "core_indicators": ["P/E Ratio", "P/B Ratio", "Free Cash Flow Yield", "ROE", "Debt/Equity", "Dividend Yield"],
        "screening_rules": {
            "pe_ratio": {"max": 20, "weight": 0.2},
            "pb_ratio": {"max": 3, "weight": 0.15},
            "roe": {"min": 0.15, "weight": 0.25},
            "debt_to_equity": {"max": 1.0, "weight": 0.15},
            "dividend_yield": {"min": 0.02, "weight": 0.1},
            "fcf_yield": {"min": 0.05, "weight": 0.15},
        },
        "market_conditions": "Best applied in bear markets or when market valuations are stretched. Less effective in momentum-driven bull markets.",
        "source_file": "value_investing.md",
    },
    {
        "name": "Quantitative Multi-Factor Model",
        "type": "quantitative",
        "description": "Systematic factor-based stock selection using statistical models. Combines value, quality, momentum, and low-volatility factors.",
        "applicable_markets": ["us", "cn", "hk"],
        "core_indicators": ["Alpha", "Sharpe Ratio", "Factor Loading", "Information Coefficient", "Turnover"],
        "screening_rules": {
            "value_factor": {"metrics": ["pe_ratio", "pb_ratio", "ev_ebitda"], "weight": 0.25},
            "quality_factor": {"metrics": ["roe", "profit_margin", "debt_ratio"], "weight": 0.25},
            "momentum_factor": {"metrics": ["return_3m", "return_6m", "relative_strength"], "weight": 0.25},
            "volatility_factor": {"metrics": ["beta", "std_dev", "max_drawdown"], "weight": 0.25},
        },
        "market_conditions": "Works across market cycles with periodic factor rotation. Rebalance factors based on market regime.",
        "source_file": "quantitative_multifactor.md",
    },
    {
        "name": "Macro Economic Driven",
        "type": "macro",
        "description": "Top-down investment approach based on economic cycles, monetary policy, and sector rotation.",
        "applicable_markets": ["us", "cn", "hk", "crypto"],
        "core_indicators": ["PMI", "CPI", "Interest Rate", "Yield Curve", "Leading Economic Index", "GDP Growth"],
        "screening_rules": {
            "expansion_phase": {"sectors": ["technology", "consumer_discretionary", "industrials"], "weight": 0.3},
            "contraction_phase": {"sectors": ["utilities", "healthcare", "consumer_staples"], "weight": 0.3},
            "rate_sensitive": {"indicators": ["yield_curve_slope", "fed_funds_rate"], "weight": 0.2},
            "inflation_hedge": {"assets": ["commodities", "tips", "real_estate"], "weight": 0.2},
        },
        "market_conditions": "Critical during regime transitions (expansion to contraction). Pair with PMI and yield curve signals.",
        "source_file": "macro_driven.md",
    },
    {
        "name": "Momentum Trend Trading",
        "type": "momentum",
        "description": "Trend following strategy using price momentum, moving average systems, and breakout patterns.",
        "applicable_markets": ["us", "cn", "hk", "crypto"],
        "core_indicators": ["RSI", "MACD", "Moving Averages", "ATR", "Volume", "Relative Strength"],
        "screening_rules": {
            "trend_strength": {"metrics": ["ma_50_200_cross", "adx"], "weight": 0.3},
            "momentum_score": {"metrics": ["rsi", "macd_histogram", "return_momentum"], "weight": 0.3},
            "volume_confirmation": {"metrics": ["volume_ratio", "obv_trend"], "weight": 0.2},
            "breakout_signal": {"metrics": ["52w_high_proximity", "bollinger_breakout"], "weight": 0.2},
        },
        "market_conditions": "Most effective in trending markets. Reduce exposure in range-bound or high-volatility regimes.",
        "source_file": "momentum_trend.md",
    },
]


class KnowledgeBaseService:
    """Knowledge base core service.
    
    Manages ChromaDB vector store for semantic search,
    theory framework CRUD, user document processing,
    and Perplexity research integration.
    """

    def __init__(
        self,
        chroma_persist_dir: str = "./chroma_data",
        openai_api_key: str = "",
        perplexity_service: Optional[PerplexityService] = None,
    ):
        self.chroma_persist_dir = chroma_persist_dir
        self.openai_api_key = openai_api_key
        self.perplexity = perplexity_service
        self._client = None
        self._collection = None
        # Book full-text indexer (three-layer architecture)
        self._book_indexer = BookIndexer(chroma_persist_dir=chroma_persist_dir)

    def _ensure_chroma(self):
        """Lazily initialize ChromaDB client and collection."""
        if self._collection is not None:
            return
        try:
            import chromadb
            self._client = chromadb.PersistentClient(path=self.chroma_persist_dir)
            self._collection = self._client.get_or_create_collection(
                name="r_system_knowledge",
                metadata={"hnsw:space": "cosine"},
            )
            logger.info(f"ChromaDB initialized at {self.chroma_persist_dir}, "
                        f"collection has {self._collection.count()} documents")
        except Exception as e:
            logger.error(f"ChromaDB initialization failed: {e}")
            raise

    async def initialize_builtin_theories(self, db: Session):
        """Load built-in theory documents into ChromaDB and SQLite on first run."""
        self._ensure_chroma()

        for theory_data in BUILTIN_THEORIES:
            # Check if already in SQLite
            existing = db.query(TheoryFramework).filter_by(name=theory_data["name"]).first()
            if not existing:
                theory = TheoryFramework(
                    name=theory_data["name"],
                    type=theory_data["type"],
                    description=theory_data["description"],
                    applicable_markets=theory_data["applicable_markets"],
                    core_indicators=theory_data["core_indicators"],
                    screening_rules=theory_data["screening_rules"],
                    market_conditions=theory_data["market_conditions"],
                    is_active=True,
                    is_builtin=True,
                    source_file=theory_data["source_file"],
                )
                db.add(theory)
                logger.info(f"Added built-in theory: {theory_data['name']}")

            # Load theory document into ChromaDB
            source_file = THEORIES_DIR / theory_data["source_file"]
            if source_file.exists():
                content = source_file.read_text(encoding="utf-8")
                chunks = self._chunk_text(content, chunk_size=800, overlap=100)
                for i, chunk in enumerate(chunks):
                    doc_id = hashlib.md5(f"{theory_data['source_file']}_{i}".encode()).hexdigest()
                    self._collection.upsert(
                        ids=[doc_id],
                        documents=[chunk],
                        metadatas=[{
                            "source": theory_data["source_file"],
                            "theory_type": theory_data["type"],
                            "theory_name": theory_data["name"],
                            "chunk_index": i,
                        }],
                    )
        db.commit()
        logger.info(f"Built-in theories initialized. ChromaDB count: {self._collection.count()}")

    async def index_investor_library_books(self, db: Session):
        """Index all books from InvestorLibrary into ChromaDB for semantic search.
        
        This bridges the gap between stored book knowledge and the semantic search engine.
        Each book's principles, criteria, quotes, and summary are chunked and indexed.
        """
        self._ensure_chroma()
        
        books = db.query(InvestorLibrary).all()
        indexed_count = 0
        
        for book in books:
            # Build a rich text document from the book's structured knowledge
            parts = []
            parts.append(f"Book: {book.title} by {book.author}")
            if book.subcategory:
                parts.append(f"Category: {book.subcategory}")
            if book.difficulty:
                parts.append(f"Difficulty: {book.difficulty}")
            
            if book.summary:
                parts.append(f"\nSummary:\n{book.summary}")
            
            # Key principles
            principles = book.key_principles or []
            if isinstance(principles, str):
                try:
                    import json
                    principles = json.loads(principles)
                except Exception:
                    principles = []
            if principles:
                parts.append("\nKey Principles:")
                for p in principles:
                    if isinstance(p, dict):
                        parts.append(f"- {p.get('principle', '')}: {p.get('explanation', '')}")
                    elif isinstance(p, str):
                        parts.append(f"- {p}")
            
            # Actionable criteria
            criteria = book.actionable_criteria or []
            if isinstance(criteria, str):
                try:
                    import json
                    criteria = json.loads(criteria)
                except Exception:
                    criteria = []
            if criteria:
                parts.append("\nActionable Screening Criteria:")
                for c in criteria:
                    if isinstance(c, str):
                        parts.append(f"- {c}")
                    elif isinstance(c, dict):
                        parts.append(f"- {c.get('criteria', c.get('text', str(c)))}")
            
            # Notable quotes
            quotes = book.quotes or []
            if isinstance(quotes, str):
                try:
                    import json
                    quotes = json.loads(quotes)
                except Exception:
                    quotes = []
            if quotes:
                parts.append("\nNotable Quotes:")
                for q in quotes[:5]:
                    if isinstance(q, str):
                        parts.append(f'  "{q}"')
                    elif isinstance(q, dict):
                        parts.append(f'  "{q.get("quote", q.get("text", str(q)))}"')
            
            full_text = "\n".join(parts)
            if not full_text.strip():
                continue
            
            # Chunk and index into ChromaDB
            chunks = self._chunk_text(full_text, chunk_size=800, overlap=100)
            for i, chunk in enumerate(chunks):
                doc_id = hashlib.md5(f"book_{book.id}_{i}".encode()).hexdigest()
                self._collection.upsert(
                    ids=[doc_id],
                    documents=[chunk],
                    metadatas=[{
                        "source": f"book:{book.title}",
                        "doc_type": "investor_library_book",
                        "theory_type": "value",  # All books are value investing
                        "theory_name": f"Book: {book.title}",
                        "book_id": str(book.id),
                        "book_author": book.author or "",
                        "subcategory": book.subcategory or "",
                        "chunk_index": i,
                    }],
                )
            
            # Mark as indexed in DB
            book.is_indexed = True
            indexed_count += 1
        
        db.commit()
        logger.info(f"Indexed {indexed_count} books into ChromaDB. Total docs: {self._collection.count()}")

    async def search_theories(
        self, query: str, market: Optional[str] = None, top_k: int = 3, db: Optional[Session] = None
    ) -> List[Dict[str, Any]]:
        """Semantic search for matching investment theories AND book knowledge."""
        self._ensure_chroma()

        results = self._collection.query(
            query_texts=[query],
            n_results=top_k * 3,  # Get more to account for dedup
            include=["documents", "metadatas", "distances"],
        )

        seen_theories = set()
        output = []
        if results and results["documents"]:
            for doc, meta, dist in zip(
                results["documents"][0],
                results["metadatas"][0],
                results["distances"][0],
            ):
                theory_name = meta.get("theory_name", "")
                doc_type = meta.get("doc_type", "")
                if theory_name in seen_theories:
                    continue
                seen_theories.add(theory_name)

                # Get structured rules from SQLite if db provided
                structured = None
                if db and doc_type != "investor_library_book":
                    theory_record = db.query(TheoryFramework).filter_by(name=theory_name).first()
                    if theory_record:
                        if market and market not in (theory_record.applicable_markets or []):
                            continue
                        structured = {
                            "id": theory_record.id,
                            "screening_rules": theory_record.screening_rules,
                            "core_indicators": theory_record.core_indicators,
                            "market_conditions": theory_record.market_conditions,
                        }
                
                # For book results, include book metadata
                book_data = None
                if doc_type == "investor_library_book" and db:
                    book_id = meta.get("book_id")
                    if book_id:
                        try:
                            book_record = db.query(InvestorLibrary).filter_by(id=int(book_id)).first()
                            if book_record:
                                book_data = {
                                    "book_id": book_record.id,
                                    "title": book_record.title,
                                    "author": book_record.author,
                                    "subcategory": book_record.subcategory,
                                    "principles_count": len(book_record.key_principles or []),
                                    "criteria_count": len(book_record.actionable_criteria or []),
                                }
                        except Exception:
                            pass

                output.append({
                    "theory_name": theory_name,
                    "theory_type": meta.get("theory_type", ""),
                    "relevance_score": 1.0 - dist,
                    "context": doc[:500],
                    "structured": structured,
                    "book_data": book_data,
                    "source_type": doc_type or "builtin_theory",
                })

                if len(output) >= top_k:
                    break

        # Also search full-text book collection for deeper book knowledge
        try:
            fulltext_results = self._book_indexer.search(query, top_k=2, layer="paragraph")
            for r in fulltext_results:
                meta = r["metadata"]
                book_title = meta.get("book_title", "")
                # Avoid duplicating already-found theories/books
                if book_title and f"Book: {book_title}" not in seen_theories:
                    seen_theories.add(f"Book: {book_title}")
                    output.append({
                        "theory_name": f"Book: {book_title}",
                        "theory_type": "value",
                        "relevance_score": r["score"],
                        "context": r["content"][:500],
                        "structured": None,
                        "book_data": {
                            "title": book_title,
                            "author": meta.get("book_author", ""),
                            "chapter": meta.get("chapter_title", ""),
                        },
                        "source_type": "book_fulltext",
                    })
        except Exception as e:
            logger.warning(f"Book fulltext theory search failed (non-critical): {e}")

        return output

    async def search_knowledge(self, query: str, top_k: int = 5) -> List[Dict[str, Any]]:
        """General semantic search across all knowledge base documents.

        Searches both the summary collection (r_system_knowledge) and
        the full-text book collection (r_system_books_fulltext),
        then merges results by relevance score.
        """
        self._ensure_chroma()

        output = []

        # Search existing summary collection
        results = self._collection.query(
            query_texts=[query],
            n_results=top_k,
            include=["documents", "metadatas", "distances"],
        )
        if results and results["documents"]:
            for doc, meta, dist in zip(
                results["documents"][0],
                results["metadatas"][0],
                results["distances"][0],
            ):
                output.append({
                    "content": doc,
                    "source": meta.get("source", ""),
                    "score": 1.0 - dist,
                    "metadata": meta,
                })

        # Search full-text book collection (Layer 2 + 3)
        try:
            fulltext_results = self._book_indexer.search(query, top_k=top_k)
            for r in fulltext_results:
                meta = r["metadata"]
                output.append({
                    "content": r["content"],
                    "source": meta.get("source", ""),
                    "score": r["score"],
                    "metadata": meta,
                })
        except Exception as e:
            logger.warning(f"Book fulltext search failed (non-critical): {e}")

        # Sort by score descending and return top_k
        output.sort(key=lambda x: x["score"], reverse=True)
        return output[:top_k]

    async def add_document(self, file_path: str, db: Session) -> Dict[str, Any]:
        """Process and index a user-uploaded document."""
        self._ensure_chroma()
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        doc_type = path.suffix.lstrip(".").lower()
        content = ""

        if doc_type in ("md", "txt"):
            content = path.read_text(encoding="utf-8")
        elif doc_type == "pdf":
            try:
                import PyPDF2
                with open(path, "rb") as f:
                    reader = PyPDF2.PdfReader(f)
                    content = "\n".join(page.extract_text() or "" for page in reader.pages)
            except Exception as e:
                logger.error(f"PDF parsing failed: {e}")
                raise

        if not content.strip():
            raise ValueError("Document is empty")

        # Store in SQLite
        user_doc = UserDocument(
            filename=path.name,
            file_path=str(path),
            doc_type=doc_type,
            status="processing",
        )
        db.add(user_doc)
        db.commit()
        db.refresh(user_doc)

        # Chunk and index
        try:
            chunks = self._chunk_text(content, chunk_size=800, overlap=100)
            for i, chunk in enumerate(chunks):
                doc_id = hashlib.md5(f"user_{user_doc.id}_{i}".encode()).hexdigest()
                self._collection.upsert(
                    ids=[doc_id],
                    documents=[chunk],
                    metadatas=[{
                        "source": path.name,
                        "doc_type": "user_upload",
                        "user_doc_id": str(user_doc.id),
                        "chunk_index": i,
                    }],
                )
            user_doc.status = "indexed"
            user_doc.chunk_count = len(chunks)
            user_doc.indexed_at = datetime.utcnow()
        except Exception as e:
            user_doc.status = "error"
            user_doc.error_message = str(e)
            logger.error(f"Document indexing failed: {e}")

        db.commit()
        db.refresh(user_doc)
        return {
            "id": user_doc.id,
            "filename": user_doc.filename,
            "status": user_doc.status,
            "chunks": user_doc.chunk_count,
        }

    async def research_and_save(
        self, query: str, mode: str, db: Session
    ) -> Dict[str, Any]:
        """Execute Perplexity research and optionally save to knowledge base."""
        if not self.perplexity:
            return {"error": "Perplexity service not configured"}

        result = await self.perplexity.research(query, mode=mode)

        # Save to SQLite
        research = ResearchResult(
            query=query,
            mode=mode,
            content=result.get("content", ""),
            sources=result.get("sources", []),
            saved_to_kb=False,
        )
        db.add(research)
        db.commit()
        db.refresh(research)

        return {
            "id": research.id,
            "query": research.query,
            "mode": research.mode,
            "content": research.content,
            "sources": research.sources,
            "saved_to_kb": False,
        }

    async def save_research_to_kb(self, research_id: int, db: Session) -> bool:
        """Save a research result into the ChromaDB knowledge base."""
        self._ensure_chroma()
        research = db.query(ResearchResult).filter_by(id=research_id).first()
        if not research:
            return False

        chunks = self._chunk_text(research.content, chunk_size=800, overlap=100)
        for i, chunk in enumerate(chunks):
            doc_id = hashlib.md5(f"research_{research.id}_{i}".encode()).hexdigest()
            self._collection.upsert(
                ids=[doc_id],
                documents=[chunk],
                metadatas=[{
                    "source": f"perplexity_research_{research.id}",
                    "doc_type": "research",
                    "query": research.query,
                    "mode": research.mode,
                    "chunk_index": i,
                }],
            )

        research.saved_to_kb = True
        db.commit()
        logger.info(f"Research {research.id} saved to knowledge base ({len(chunks)} chunks)")
        return True

    def get_chroma_stats(self) -> Dict[str, Any]:
        """Get ChromaDB statistics for both collections."""
        try:
            self._ensure_chroma()
            summary_count = self._collection.count()
        except Exception:
            summary_count = 0

        try:
            book_stats = self._book_indexer.get_stats()
        except Exception:
            book_stats = {"total_documents": 0, "books_count": 0}

        return {
            "connected": True,
            "documents": summary_count,
            "fulltext_documents": book_stats.get("total_documents", 0),
            "books_indexed": book_stats.get("books_count", 0),
        }

    @property
    def book_indexer(self) -> BookIndexer:
        """Access the book full-text indexer."""
        return self._book_indexer

    @staticmethod
    def _chunk_text(text: str, chunk_size: int = 800, overlap: int = 100) -> List[str]:
        """Split text into overlapping chunks."""
        if len(text) <= chunk_size:
            return [text]
        chunks = []
        start = 0
        while start < len(text):
            end = start + chunk_size
            chunk = text[start:end]
            # Try to break at sentence boundary
            if end < len(text):
                last_period = chunk.rfind(".")
                last_newline = chunk.rfind("\n")
                break_point = max(last_period, last_newline)
                if break_point > chunk_size * 0.5:
                    chunk = chunk[: break_point + 1]
                    end = start + break_point + 1
            chunks.append(chunk.strip())
            start = end - overlap
        return [c for c in chunks if c]
