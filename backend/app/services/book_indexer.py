"""
Book Indexer Service — Four-Layer Knowledge Architecture

Layer 1: Book summaries (existing inject_deep_knowledge.py data)
Layer 2: Chapter-level documents (chapter title + full text as one doc)
Layer 3: Paragraph-level chunks (fine-grained, chunk_size=500, overlap=80)
Layer 4: Investment strategy extraction (structured screening logic per book)

Supports PDF (with TOC/outline) and EPUB (with spine/TOC).
Stores all layers in a dedicated ChromaDB collection: r_system_books_fulltext
"""

import os
import re
import hashlib
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

USER_DOCS_DIR = Path(__file__).parent.parent / "knowledge" / "user_docs"


@dataclass
class Chapter:
    """Represents a chapter or section extracted from a book."""
    title: str
    text: str
    chapter_number: int
    depth: int  # 0 = top-level chapter, 1 = section, 2 = subsection
    start_page: int = -1
    end_page: int = -1
    parent_chapter: str = ""


@dataclass
class BookDocument:
    """A fully parsed book with chapters and metadata."""
    title: str
    author: str
    file_path: str
    format: str  # "pdf" or "epub"
    total_pages: int = 0
    total_chars: int = 0
    chapters: List[Chapter] = field(default_factory=list)


# ── Book Metadata Registry ──
# Maps filename patterns to structured metadata
BOOK_REGISTRY: Dict[str, Dict[str, str]] = {
    "Warren Buffett Way": {"title": "The Warren Buffett Way", "author": "Robert G. Hagstrom"},
    "Intelligent Investor": {"title": "The Intelligent Investor", "author": "Benjamin Graham"},
    "Essays of Warren Buffett": {"title": "The Essays of Warren Buffett", "author": "Warren E. Buffett & Lawrence A. Cunningham"},
    "Investment Valuation": {"title": "Investment Valuation", "author": "Aswath Damodaran"},
    "Quality Investing": {"title": "Quality Investing", "author": "Lawrence A. Cunningham et al."},
    "Quantitative Value": {"title": "Quantitative Value", "author": "Wesley Gray & Tobias Carlisle"},
    "Education of a Value Investor": {"title": "The Education of a Value Investor", "author": "Guy Spier"},
    "Five Rules for Successful": {"title": "The Five Rules for Successful Stock Investing", "author": "Pat Dorsey"},
    "Little Book That Still Beats": {"title": "The Little Book That Still Beats the Market", "author": "Joel Greenblatt"},
    "Manual of Ideas": {"title": "The Manual of Ideas", "author": "John Mihaljevic"},
    "Value investing.*Graham": {"title": "Value Investing: From Graham to Buffett and Beyond", "author": "Bruce Greenwald et al."},
    "What Works on Wall Street": {"title": "What Works on Wall Street", "author": "James P. O'Shaughnessy"},
    "Expectations Investing": {"title": "Expectations Investing", "author": "Alfred Rappaport & Michael J. Mauboussin"},
}


def _match_book_metadata(filename: str) -> Dict[str, str]:
    """Match a filename to known book metadata."""
    for pattern, meta in BOOK_REGISTRY.items():
        if re.search(pattern, filename, re.IGNORECASE):
            return meta
    # Fallback: extract from filename
    name = Path(filename).stem.split("(")[0].strip()
    return {"title": name, "author": "Unknown"}


# ═══════════════════════════════════════════════════════════════
#  PDF Parsing with TOC-based Chapter Extraction
# ═══════════════════════════════════════════════════════════════

def _parse_pdf_with_chapters(file_path: str) -> BookDocument:
    """Parse a PDF using its outline/TOC to extract chapter-structured text."""
    import PyPDF2

    meta = _match_book_metadata(os.path.basename(file_path))
    reader = PyPDF2.PdfReader(open(file_path, "rb"))
    total_pages = len(reader.pages)

    # Extract all page texts
    page_texts: List[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        page_texts.append(text)

    total_chars = sum(len(t) for t in page_texts)

    # Extract outline entries with page numbers
    outline_entries: List[Tuple[int, str, int]] = []  # (depth, title, page_num)
    outline = reader.outline

    if outline:
        def _flatten_outline(items, depth=0):
            for item in items:
                if isinstance(item, list):
                    _flatten_outline(item, depth + 1)
                elif hasattr(item, "title"):
                    try:
                        page_num = reader.get_destination_page_number(item)
                    except Exception:
                        page_num = -1
                    outline_entries.append((depth, item.title, page_num))

        _flatten_outline(outline)

    # Filter to main content chapters (depth 0, numbered chapters)
    # Also include important non-numbered sections like Foreword, Preface
    main_chapters = []
    skip_sections = {"contents", "index", "about the website", "about the author"}

    for depth, title, page_num in outline_entries:
        title_lower = title.strip().lower()
        if title_lower in skip_sections:
            continue
        if page_num < 0:
            continue
        main_chapters.append((depth, title.strip(), page_num))

    if not main_chapters:
        # No outline: treat entire book as one chapter
        full_text = "\n".join(page_texts)
        chapters = [Chapter(
            title="Full Text",
            text=full_text,
            chapter_number=1,
            depth=0,
            start_page=0,
            end_page=total_pages - 1,
        )]
    else:
        # Build chapters from outline using page ranges
        chapters = []
        # Only use depth-0 entries as chapter boundaries for text extraction
        top_level = [(title, page_num) for depth, title, page_num in main_chapters if depth == 0]

        for i, (title, start_page) in enumerate(top_level):
            # End page is the page before next chapter starts (or last page)
            if i + 1 < len(top_level):
                end_page = top_level[i + 1][1] - 1
            else:
                # Last chapter extends to before Notes/Acknowledgments or end
                end_page = total_pages - 1
                for d, t, p in main_chapters:
                    t_lower = t.strip().lower()
                    if t_lower in ("notes", "acknowledgments", "bibliography", "appendix"):
                        if p > start_page:
                            end_page = p - 1
                            break

            # Extract text for this page range
            chapter_text = "\n".join(
                page_texts[p] for p in range(start_page, min(end_page + 1, total_pages))
                if page_texts[p].strip()
            )

            if not chapter_text.strip():
                continue

            # Determine chapter number
            ch_match = re.match(r"^(\d+)\s+", title)
            ch_num = int(ch_match.group(1)) if ch_match else i + 1

            chapters.append(Chapter(
                title=title,
                text=chapter_text,
                chapter_number=ch_num,
                depth=0,
                start_page=start_page,
                end_page=end_page,
            ))

        # Also build section-level entries (depth 1+) with parent tracking
        # These are stored as metadata but share the parent chapter's text range
        for depth, title, page_num in main_chapters:
            if depth >= 1:
                # Find parent chapter
                parent_title = ""
                for ch in chapters:
                    if ch.start_page <= page_num <= ch.end_page and ch.depth == 0:
                        parent_title = ch.title
                        break
                # Section entries are tracked for metadata but not as separate text chunks
                # (they'll get proper metadata during the chunking phase)

    logger.info(f"Parsed PDF '{meta['title']}': {total_pages} pages, {len(chapters)} chapters, {total_chars:,} chars")

    return BookDocument(
        title=meta["title"],
        author=meta["author"],
        file_path=file_path,
        format="pdf",
        total_pages=total_pages,
        total_chars=total_chars,
        chapters=chapters,
    )


# ═══════════════════════════════════════════════════════════════
#  EPUB Parsing with Spine-based Chapter Extraction
# ═══════════════════════════════════════════════════════════════

def _parse_epub_with_chapters(file_path: str) -> BookDocument:
    """Parse an EPUB using its spine to extract chapter-structured text."""
    import ebooklib
    from ebooklib import epub
    from bs4 import BeautifulSoup

    meta = _match_book_metadata(os.path.basename(file_path))
    book = epub.read_epub(file_path, options={"ignore_ncx": True})

    # Try to get TOC
    toc = book.toc
    toc_titles = []
    if toc:
        def _extract_toc(items):
            for item in items:
                if isinstance(item, tuple):
                    section, children = item
                    if hasattr(section, "title"):
                        toc_titles.append(section.title)
                    _extract_toc(children)
                elif hasattr(item, "title"):
                    toc_titles.append(item.title)
        _extract_toc(toc)

    # Extract chapters from spine documents
    chapters = []
    chapter_num = 0
    total_chars = 0
    skip_patterns = re.compile(
        r"(table of contents|copyright|cover|title page|dedication|also by)",
        re.IGNORECASE
    )

    for item in book.get_items():
        if item.get_type() != ebooklib.ITEM_DOCUMENT:
            continue

        soup = BeautifulSoup(item.get_content(), "html.parser")
        text = soup.get_text(separator="\n", strip=True)

        if not text.strip() or len(text.strip()) < 100:
            continue

        # Try to extract chapter title from headings
        heading = soup.find(["h1", "h2", "h3"])
        title = heading.get_text(strip=True) if heading else f"Section {chapter_num + 1}"

        if skip_patterns.search(title):
            continue

        chapter_num += 1
        total_chars += len(text)

        chapters.append(Chapter(
            title=title,
            text=text,
            chapter_number=chapter_num,
            depth=0,
        ))

    logger.info(f"Parsed EPUB '{meta['title']}': {len(chapters)} chapters, {total_chars:,} chars")

    return BookDocument(
        title=meta["title"],
        author=meta["author"],
        file_path=file_path,
        format="epub",
        total_pages=len(chapters),
        total_chars=total_chars,
        chapters=chapters,
    )


# ═══════════════════════════════════════════════════════════════
#  Text Splitting — RecursiveCharacterTextSplitter
# ═══════════════════════════════════════════════════════════════

def _clean_book_text(text: str, book_title: str = "") -> str:
    """Deep clean book text: remove headers/footers, page numbers, OCR artifacts."""
    # Remove page header patterns: "62 The Warren Buffett Way", "Chapter 3 Rules of the Game 31"
    text = re.sub(r"\n\d{1,4}\s+[A-Z][a-zA-Z\s]{5,40}\n", "\n", text)
    text = re.sub(r"\n[A-Z][a-z]+ [A-Z][a-z]+\s+\d{1,4}\n", "\n", text)
    # Remove "O'Shaughnessy 00  4/26/05  6:09 PM  Page xxx" PDF artifacts
    text = re.sub(r"O'Shaughnessy.*?Page\s+\w+", "", text)
    # Remove standalone page numbers
    text = re.sub(r"\n\s*\d{1,4}\s*\n", "\n", text)
    # Remove "This page intentionally left blank"
    text = re.sub(r"This page intentionally left blank\.?", "", text, flags=re.IGNORECASE)
    # Remove excessive whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{3,}", " ", text)
    return text.strip()


def _detect_section_boundaries(text: str) -> List[Tuple[str, str]]:
    """Detect sub-section boundaries within a chapter for semantic splitting.

    Returns list of (section_heading, section_text) tuples.
    """
    # Match ALL-CAPS headings, numbered sections, or bold-style headings
    heading_pattern = re.compile(
        r"\n\s*"
        r"("
        r"[A-Z][A-Z\s:—\-']{8,80}"           # ALL CAPS heading
        r"|(?:CHAPTER|PART|SECTION)\s+\d+"     # CHAPTER N
        r"|\d+\.\s+[A-Z][A-Za-z\s]{5,60}"     # 1. Title Case
        r"|[A-Z][a-z]+(?:\s+[A-Z][a-z]+){2,}" # Title Case Multi Word (3+ words)
        r")"
        r"\s*\n",
        re.MULTILINE,
    )
    sections = []
    matches = list(heading_pattern.finditer(text))

    if not matches:
        return [("", text)]

    # If there's text before the first heading
    if matches[0].start() > 50:
        sections.append(("", text[:matches[0].start()].strip()))

    for i, match in enumerate(matches):
        heading = match.group(1).strip()
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if body and len(body) > 30:
            sections.append((heading, body))

    return sections if sections else [("", text)]


def _split_chapter_into_chunks(
    chapter: Chapter,
    book_title: str,
    book_author: str,
    chunk_size: int = 500,
    chunk_overlap: int = 80,
) -> List[Dict[str, Any]]:
    """Split a chapter into fine-grained paragraph-level chunks with rich metadata.

    Uses semantic-aware splitting:
    1. First detects sub-section boundaries (ALL CAPS headings, etc.)
    2. Then splits each section into smaller chunks at sentence boundaries
    3. Tags each chunk with section_heading for better retrieval context

    Returns list of {text, metadata} dicts ready for ChromaDB upsert.
    """
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        separators=["\n\n", "\n", ". ", "? ", "! ", "; ", " "],
        length_function=len,
    )

    text = _clean_book_text(chapter.text, book_title)

    # Step 1: Detect sub-sections
    sections = _detect_section_boundaries(text)

    results = []
    global_idx = 0

    for section_heading, section_text in sections:
        # Step 2: Fine-grained splitting within each section
        chunks = splitter.split_text(section_text)

        for i, chunk_text in enumerate(chunks):
            chunk_text = chunk_text.strip()
            if not chunk_text or len(chunk_text) < 40:
                continue

            doc_id = hashlib.md5(
                f"v2_chunk:{book_title}:{chapter.title}:{section_heading}:idx{global_idx}".encode()
            ).hexdigest()

            metadata = {
                "source": f"book:{book_title}",
                "doc_type": "book_fulltext_chunk",
                "book_title": book_title,
                "book_author": book_author,
                "chapter_title": chapter.title,
                "chapter_number": chapter.chapter_number,
                "section_heading": section_heading or "",
                "chunk_index": global_idx,
                "section_chunk_index": i,
                "layer": "paragraph",  # Layer 3
            }
            if chapter.start_page >= 0:
                metadata["start_page"] = chapter.start_page
                metadata["end_page"] = chapter.end_page

            results.append({
                "id": doc_id,
                "text": chunk_text,
                "metadata": metadata,
            })
            global_idx += 1

    return results


def _build_chapter_doc(
    chapter: Chapter,
    book_title: str,
    book_author: str,
) -> Dict[str, Any]:
    """Build a chapter-level document (Layer 2) — full chapter as one doc.

    For very long chapters, we take the first 8000 chars as a representative summary.
    ChromaDB has a practical limit per document.
    """
    text = chapter.text.strip()
    # Truncate very long chapters for the chapter-level doc
    max_chapter_doc_len = 8000
    if len(text) > max_chapter_doc_len:
        text = text[:max_chapter_doc_len] + f"\n\n[... chapter continues, {len(chapter.text):,} total chars ...]"

    doc_id = hashlib.md5(
        f"chapter_doc:{book_title}:{chapter.title}".encode()
    ).hexdigest()

    metadata = {
        "source": f"book:{book_title}",
        "doc_type": "book_chapter",
        "book_title": book_title,
        "book_author": book_author,
        "chapter_title": chapter.title,
        "chapter_number": chapter.chapter_number,
        "chapter_char_count": len(chapter.text),
        "layer": "chapter",  # Layer 2
    }
    if chapter.start_page >= 0:
        metadata["start_page"] = chapter.start_page
        metadata["end_page"] = chapter.end_page

    return {
        "id": doc_id,
        "text": text,
        "metadata": metadata,
    }


# ═══════════════════════════════════════════════════════════════
#  BookIndexer — Main Service Class
# ═══════════════════════════════════════════════════════════════

class BookIndexer:
    """Indexes book full-text into ChromaDB with three-layer architecture.

    Uses a separate collection (r_system_books_fulltext) to avoid
    polluting the existing r_system_knowledge collection.
    """

    COLLECTION_NAME = "r_system_books_fulltext"

    def __init__(self, chroma_persist_dir: str = "./chroma_data"):
        self.chroma_persist_dir = chroma_persist_dir
        self._client = None
        self._collection = None

    def _ensure_chroma(self):
        """Lazily initialize ChromaDB client and collection."""
        if self._collection is not None:
            return
        import chromadb
        self._client = chromadb.PersistentClient(path=self.chroma_persist_dir)
        self._collection = self._client.get_or_create_collection(
            name=self.COLLECTION_NAME,
            metadata={"hnsw:space": "cosine"},
        )
        logger.info(
            f"BookIndexer: ChromaDB collection '{self.COLLECTION_NAME}' "
            f"has {self._collection.count()} documents"
        )

    def parse_book(self, file_path: str) -> BookDocument:
        """Parse a book file (PDF or EPUB) into structured chapters."""
        ext = os.path.splitext(file_path)[1].lower()
        if ext == ".pdf":
            return _parse_pdf_with_chapters(file_path)
        elif ext == ".epub":
            return _parse_epub_with_chapters(file_path)
        else:
            raise ValueError(f"Unsupported format: {ext}. Use .pdf or .epub")

    def index_book(
        self,
        file_path: str,
        chunk_size: int = 500,
        chunk_overlap: int = 80,
        force_reindex: bool = False,
    ) -> Dict[str, Any]:
        """Parse and index a book into ChromaDB (Layer 2 + Layer 3).

        Returns statistics about the indexing operation.
        """
        self._ensure_chroma()

        # Check if already indexed
        if not force_reindex:
            existing = self._collection.get(
                where={"book_title": _match_book_metadata(os.path.basename(file_path))["title"]},
                limit=1,
            )
            if existing and existing["ids"]:
                book_title = _match_book_metadata(os.path.basename(file_path))["title"]
                count = self._collection.count()
                logger.info(f"Book '{book_title}' already indexed. Use force_reindex=True to re-index.")
                return {
                    "status": "already_indexed",
                    "book_title": book_title,
                    "message": "Book already indexed. Use force_reindex=True to re-index.",
                    "collection_total": count,
                }

        # Parse book
        book = self.parse_book(file_path)
        logger.info(f"Indexing '{book.title}': {len(book.chapters)} chapters, {book.total_chars:,} chars")

        # If force_reindex, remove existing docs for this book
        if force_reindex:
            try:
                existing = self._collection.get(
                    where={"book_title": book.title},
                )
                if existing and existing["ids"]:
                    self._collection.delete(ids=existing["ids"])
                    logger.info(f"Removed {len(existing['ids'])} existing docs for '{book.title}'")
            except Exception as e:
                logger.warning(f"Could not remove existing docs: {e}")

        all_ids = []
        all_texts = []
        all_metadatas = []

        chapter_count = 0
        chunk_count = 0

        for chapter in book.chapters:
            if not chapter.text.strip():
                continue

            # Layer 2: Chapter-level document
            chapter_doc = _build_chapter_doc(chapter, book.title, book.author)
            all_ids.append(chapter_doc["id"])
            all_texts.append(chapter_doc["text"])
            all_metadatas.append(chapter_doc["metadata"])
            chapter_count += 1

            # Layer 3: Paragraph-level chunks
            chunks = _split_chapter_into_chunks(
                chapter, book.title, book.author,
                chunk_size=chunk_size, chunk_overlap=chunk_overlap,
            )
            for chunk in chunks:
                all_ids.append(chunk["id"])
                all_texts.append(chunk["text"])
                all_metadatas.append(chunk["metadata"])
                chunk_count += 1

        # Deduplicate IDs (keep first occurrence)
        seen_ids = set()
        dedup_ids, dedup_texts, dedup_metas = [], [], []
        for _id, _text, _meta in zip(all_ids, all_texts, all_metadatas):
            if _id not in seen_ids:
                seen_ids.add(_id)
                dedup_ids.append(_id)
                dedup_texts.append(_text)
                dedup_metas.append(_meta)
        all_ids, all_texts, all_metadatas = dedup_ids, dedup_texts, dedup_metas

        # Batch upsert to ChromaDB (in batches of 100 to avoid memory issues)
        batch_size = 100
        for start in range(0, len(all_ids), batch_size):
            end = min(start + batch_size, len(all_ids))
            self._collection.upsert(
                ids=all_ids[start:end],
                documents=all_texts[start:end],
                metadatas=all_metadatas[start:end],
            )

        total = self._collection.count()
        stats = {
            "status": "success",
            "book_title": book.title,
            "book_author": book.author,
            "format": book.format,
            "total_pages": book.total_pages,
            "total_chars": book.total_chars,
            "chapters_indexed": chapter_count,
            "chunks_indexed": chunk_count,
            "total_docs_indexed": chapter_count + chunk_count,
            "collection_total": total,
        }

        logger.info(
            f"Indexed '{book.title}': {chapter_count} chapters + {chunk_count} chunks = "
            f"{chapter_count + chunk_count} docs. Collection total: {total}"
        )
        return stats

    def index_all_books(
        self,
        docs_dir: Optional[str] = None,
        chunk_size: int = 500,
        chunk_overlap: int = 80,
        force_reindex: bool = False,
    ) -> List[Dict[str, Any]]:
        """Index all PDF and EPUB books in the user_docs directory."""
        if docs_dir is None:
            docs_dir = str(USER_DOCS_DIR)

        results = []
        supported = {".pdf", ".epub"}

        for fname in sorted(os.listdir(docs_dir)):
            ext = os.path.splitext(fname)[1].lower()
            if ext not in supported:
                continue

            file_path = os.path.join(docs_dir, fname)
            try:
                result = self.index_book(
                    file_path,
                    chunk_size=chunk_size,
                    chunk_overlap=chunk_overlap,
                    force_reindex=force_reindex,
                )
                results.append(result)
                print(f"  OK: {result['book_title']} — {result.get('total_docs_indexed', '?')} docs")
            except Exception as e:
                error = {"status": "error", "file": fname, "error": str(e)}
                results.append(error)
                print(f"  ERROR: {fname} — {e}")

        return results

    def search(
        self,
        query: str,
        top_k: int = 8,
        layer: Optional[str] = None,
        book_title: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Search the full-text book collection.

        Args:
            query: Search query text
            top_k: Number of results to return
            layer: Filter by layer ("chapter" or "paragraph"), or None for all
            book_title: Filter by specific book title, or None for all books

        Returns:
            List of {content, score, metadata} dicts
        """
        self._ensure_chroma()

        where_filter = {}
        conditions = []
        if layer:
            conditions.append({"layer": layer})
        if book_title:
            conditions.append({"book_title": book_title})

        if len(conditions) > 1:
            where_filter = {"$and": conditions}
        elif len(conditions) == 1:
            where_filter = conditions[0]

        kwargs = {
            "query_texts": [query],
            "n_results": top_k,
            "include": ["documents", "metadatas", "distances"],
        }
        if where_filter:
            kwargs["where"] = where_filter

        results = self._collection.query(**kwargs)

        output = []
        if results and results["documents"]:
            for doc, meta, dist in zip(
                results["documents"][0],
                results["metadatas"][0],
                results["distances"][0],
            ):
                output.append({
                    "content": doc,
                    "score": round(1.0 - dist, 4),
                    "metadata": meta,
                })
        return output

    def get_stats(self) -> Dict[str, Any]:
        """Get collection statistics."""
        self._ensure_chroma()
        total = self._collection.count()

        # Count by layer
        chapter_count = 0
        paragraph_count = 0
        books = set()

        try:
            all_meta = self._collection.get(include=["metadatas"])
            if all_meta and all_meta["metadatas"]:
                for m in all_meta["metadatas"]:
                    if m.get("layer") == "chapter":
                        chapter_count += 1
                    elif m.get("layer") == "paragraph":
                        paragraph_count += 1
                    books.add(m.get("book_title", "unknown"))
        except Exception:
            pass

        return {
            "collection": self.COLLECTION_NAME,
            "total_documents": total,
            "chapter_docs": chapter_count,
            "paragraph_chunks": paragraph_count,
            "books_indexed": sorted(books) if books else [],
            "books_count": len(books),
        }

    def delete_book(self, book_title: str) -> Dict[str, Any]:
        """Remove all indexed data for a specific book."""
        self._ensure_chroma()
        existing = self._collection.get(where={"book_title": book_title})
        if not existing or not existing["ids"]:
            return {"status": "not_found", "book_title": book_title}

        count = len(existing["ids"])
        self._collection.delete(ids=existing["ids"])
        return {
            "status": "deleted",
            "book_title": book_title,
            "documents_removed": count,
            "collection_total": self._collection.count(),
        }

    def get_all_chunks_for_book(self, book_title: str) -> List[Dict[str, Any]]:
        """Get all chunks for a specific book, ordered by chapter/chunk index."""
        self._ensure_chroma()
        results = self._collection.get(
            where={"$and": [{"book_title": book_title}, {"layer": "paragraph"}]},
            include=["documents", "metadatas"],
        )
        if not results or not results["ids"]:
            return []
        chunks = []
        for doc, meta in zip(results["documents"], results["metadatas"]):
            chunks.append({"text": doc, "metadata": meta})
        chunks.sort(key=lambda x: (
            x["metadata"].get("chapter_number", 0),
            x["metadata"].get("chunk_index", 0),
        ))
        return chunks


# ═══════════════════════════════════════════════════════════════
#  Strategy Extractor — Extract structured screening logic from books
# ═══════════════════════════════════════════════════════════════

# Known data fields in StockData (from src/analyzer.py)
AVAILABLE_DATA_FIELDS = {
    # 估值
    "pe", "forward_pe", "pb", "ps", "earnings_yield", "enterprise_value",
    # 盈利
    "roe", "eps", "revenue", "net_income", "ebit", "pretax_income",
    "profit_margin", "operating_margin",
    # 财务健康
    "current_ratio", "debt_to_equity", "debt_to_assets", "total_debt",
    "total_cash", "market_cap", "book_value", "tangible_book_value",
    "working_capital", "total_assets", "total_equity", "enterprise_value",
    "shares_outstanding", "current_assets", "current_liabilities",
    "long_term_debt", "total_liabilities", "interest_coverage_ratio",
    "free_cash_flow", "capex",
    # 股息
    "dividend_yield", "dividend_payout_ratio", "dividend_per_share",
    # 价格
    "price", "price_52w_high", "price_52w_low",
    # 增长
    "revenue_growth_rate", "eps_growth_rate", "eps_growth_5y",
    # 信用评级
    "sp_rating", "moody_rating", "sp_quality_ranking",
    # 技术
    "rsi_14d", "macd_line", "macd_signal", "macd_hist", "ma_200d",
    # 基准
    "market_pe", "industry_avg_pe", "aa_bond_yield", "treasury_yield_10y",
    # 历史
    "avg_eps_10y", "avg_eps_3y", "avg_eps_first_3y", "earnings_growth_10y",
    "profitable_years", "min_annual_eps_10y", "min_annual_eps_5y",
    "max_eps_decline", "consecutive_dividend_years",
    "consecutive_profitable_years", "revenue_cagr_10y",
    # 衍生
    "graham_number", "ncav_per_share", "intrinsic_value", "margin_of_safety",
    "net_current_assets", "book_value_equity", "eps_3yr_avg_to_price",
}

STRATEGY_EXTRACTION_PROMPT = """You are a quantitative investment analyst. Analyze the following book chapter text and extract ALL concrete, actionable stock screening rules.

For EACH rule, provide:
1. "name": Short rule name (e.g., "Graham P/E Filter", "Buffett ROE Threshold")
2. "description": Clear natural-language description
3. "expression": Python-style condition using these available variables:
   pe, forward_pe, pb, ps, earnings_yield, roe, eps, revenue, net_income, ebit,
   profit_margin, operating_margin, current_ratio, debt_to_equity, total_debt,
   market_cap, book_value, free_cash_flow, dividend_yield, dividend_payout_ratio,
   price, price_52w_high, price_52w_low, revenue_growth_rate, eps_growth_rate,
   interest_coverage_ratio, graham_number, intrinsic_value, margin_of_safety,
   avg_eps_10y, avg_eps_3y, earnings_growth_10y, profitable_years,
   consecutive_dividend_years, market_pe, aa_bond_yield, treasury_yield_10y,
   ncav_per_share, ma_200d, rsi_14d
   
   Use Python operators: <, >, <=, >=, ==, and, or, not
   Example: "pe < 15 and roe > 0.15 and debt_to_equity < 0.5"
   If a rule cannot be expressed with available variables, set expression to null.

4. "data_fields_required": List of variable names used in the expression
5. "category": One of ["valuation", "profitability", "financial_health", "growth", "dividend", "quality", "momentum", "composite"]
6. "confidence": "high" (explicit numeric threshold in text) / "medium" (qualitative with implied threshold) / "low" (general principle)

Also extract:
- "chapter_summary": One paragraph summarizing the chapter's core investment philosophy
- "key_concepts": List of key investment concepts mentioned (e.g., "margin of safety", "economic moat")

Return ONLY valid JSON:
{
  "strategies": [...],
  "chapter_summary": "...",
  "key_concepts": ["..."]
}

Be thorough. Extract ALL screening criteria, even if mentioned in passing. Include both explicit thresholds (like "P/E below 15") and implicit ones (like "strong balance sheet" → current_ratio > 2)."""


def _extract_strategies_from_text(
    text: str,
    chapter_title: str,
    book_title: str,
    llm_chat_fn,
) -> Optional[Dict[str, Any]]:
    """Use LLM to extract structured strategies from a chunk of text."""
    user_msg = f"Book: {book_title}\nChapter: {chapter_title}\n\nText:\n{text[:6000]}"
    try:
        response = llm_chat_fn(
            messages=[{"role": "user", "content": user_msg}],
            system_prompt=STRATEGY_EXTRACTION_PROMPT,
        )
        return _parse_strategy_json(response)
    except Exception as e:
        logger.warning(f"Strategy extraction failed for '{chapter_title}': {e}")
        return None


def _parse_strategy_json(text: str) -> Optional[Dict[str, Any]]:
    """Parse LLM response into structured strategy dict."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end])
            except json.JSONDecodeError:
                return None
    return None


def extract_book_strategies(
    book: BookDocument,
    llm_chat_fn,
    output_dir: str = "",
) -> Dict[str, Any]:
    """Extract all investment strategies from a parsed book.

    Args:
        book: Parsed BookDocument
        llm_chat_fn: Callable that takes (messages, system_prompt) kwargs
        output_dir: Directory to save the JSON output

    Returns:
        Structured dict with all strategies, data requirements, and summaries
    """
    all_strategies = []
    all_concepts = set()
    chapter_summaries = []
    data_fields_used = set()

    for i, chapter in enumerate(book.chapters):
        if not chapter.text.strip() or len(chapter.text.strip()) < 200:
            continue

        logger.info(f"  [{i+1}/{len(book.chapters)}] Extracting from: {chapter.title}")

        result = _extract_strategies_from_text(
            chapter.text,
            chapter.title,
            book.title,
            llm_chat_fn,
        )
        if not result:
            continue

        # Collect strategies
        for s in result.get("strategies", []):
            s["source_chapter"] = chapter.title
            s["source_book"] = book.title
            # Validate data fields
            fields = s.get("data_fields_required", [])
            valid_fields = [f for f in fields if f in AVAILABLE_DATA_FIELDS]
            s["data_fields_required"] = valid_fields
            s["data_fields_missing"] = [f for f in fields if f not in AVAILABLE_DATA_FIELDS]
            data_fields_used.update(valid_fields)
            all_strategies.append(s)

        # Collect concepts
        for c in result.get("key_concepts", []):
            all_concepts.add(c)

        # Collect chapter summary
        summary = result.get("chapter_summary", "")
        if summary:
            chapter_summaries.append({
                "chapter": chapter.title,
                "summary": summary,
            })

    # Deduplicate strategies by name + expression
    seen = set()
    unique_strategies = []
    for s in all_strategies:
        key = (s.get("name", ""), s.get("expression", ""))
        if key not in seen:
            seen.add(key)
            unique_strategies.append(s)

    # Build final report
    report = {
        "book_title": book.title,
        "book_author": book.author,
        "total_chapters_analyzed": len(book.chapters),
        "strategies": unique_strategies,
        "strategies_count": len(unique_strategies),
        "strategies_by_category": _group_by_category(unique_strategies),
        "data_fields_required": sorted(data_fields_used),
        "data_fields_available": sorted(data_fields_used & AVAILABLE_DATA_FIELDS),
        "data_fields_not_available": sorted(
            data_fields_used - AVAILABLE_DATA_FIELDS
        ),
        "key_concepts": sorted(all_concepts),
        "chapter_summaries": chapter_summaries,
    }

    # Save to file
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        safe_name = re.sub(r"[^\w\s-]", "", book.title).strip().replace(" ", "_")
        out_path = os.path.join(output_dir, f"{safe_name}_strategies.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"Strategies saved to {out_path}")
        report["output_file"] = out_path

    return report


def _group_by_category(strategies: List[Dict]) -> Dict[str, int]:
    """Group strategy count by category."""
    groups = {}
    for s in strategies:
        cat = s.get("category", "unknown")
        groups[cat] = groups.get(cat, 0) + 1
    return groups
