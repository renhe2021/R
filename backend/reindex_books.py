"""
Reindex All Books — Fine-grained chunking + Strategy extraction

Usage:
    python reindex_books.py                    # Reindex all 14 books + extract strategies
    python reindex_books.py --stats            # Show current collection stats
    python reindex_books.py --strategies-only  # Only run strategy extraction (skip reindex)
    python reindex_books.py --book "Intelligent Investor"  # Process a specific book

Output:
    - ChromaDB updated with fine-grained chunks (chunk_size=500)
    - JSON strategy files in backend/strategies/
"""

import sys
import os
import io
import json
import logging
import time

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Setup paths
sys.path.insert(0, os.path.dirname(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("reindex_books")

from app.services.book_indexer import (
    BookIndexer, extract_book_strategies, BOOK_REGISTRY
)

CHROMA_DIR = os.path.join(os.path.dirname(__file__), "chroma_data")
STRATEGIES_DIR = os.path.join(os.path.dirname(__file__), "strategies")


def _get_llm_chat_fn():
    """Build an LLM chat function from config.yaml."""
    try:
        import yaml
        config_path = os.path.join(PROJECT_ROOT, "config.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        llm_config = config.get("llm", {})
        provider = llm_config.get("default_provider", "claude")
        provider_conf = llm_config.get(provider, {})

        from openai import OpenAI
        client = OpenAI(
            api_key=provider_conf.get("api_key", ""),
            base_url=provider_conf.get("base_url", ""),
        )
        model = provider_conf.get("model", provider_conf.get("chat_model", "claude-opus-4-6"))

        def chat_fn(messages, system_prompt=""):
            msgs = []
            if system_prompt:
                msgs.append({"role": "system", "content": system_prompt})
            msgs.extend(messages)
            response = client.chat.completions.create(
                model=model,
                messages=msgs,
                temperature=0.1,
                max_tokens=4096,
            )
            return response.choices[0].message.content

        # Test
        logger.info(f"LLM ready: provider={provider}, model={model}")
        return chat_fn
    except Exception as e:
        logger.error(f"Failed to init LLM: {e}")
        return None


def show_stats(indexer):
    """Display current collection statistics."""
    stats = indexer.get_stats()
    print(f"\n{'='*70}")
    print(f"  Collection: {stats['collection']}")
    print(f"{'='*70}")
    print(f"  Total documents:     {stats['total_documents']}")
    print(f"  Chapter docs (L2):   {stats['chapter_docs']}")
    print(f"  Paragraph chunks (L3): {stats['paragraph_chunks']}")
    print(f"  Books indexed:       {stats['books_count']}")
    for b in stats.get("books_indexed", []):
        print(f"    • {b}")
    print(f"{'='*70}\n")


def reindex_all(indexer, book_filter=None):
    """Reindex all books with fine-grained chunking."""
    print(f"\n{'='*70}")
    print("  STEP 1: Reindexing books with fine-grained chunks (size=500)")
    print(f"{'='*70}\n")

    from app.services.book_indexer import USER_DOCS_DIR
    docs_dir = str(USER_DOCS_DIR)

    supported = {".pdf", ".epub"}
    files = sorted([
        f for f in os.listdir(docs_dir)
        if os.path.splitext(f)[1].lower() in supported
    ])

    if book_filter:
        files = [f for f in files if book_filter.lower() in f.lower()]
        if not files:
            print(f"  No books matching '{book_filter}' found.")
            return []

    print(f"  Found {len(files)} book(s) to process\n")

    results = []
    for i, fname in enumerate(files):
        file_path = os.path.join(docs_dir, fname)
        short_name = fname[:60] + "..." if len(fname) > 60 else fname
        print(f"  [{i+1}/{len(files)}] {short_name}")
        t0 = time.time()

        try:
            result = indexer.index_book(
                file_path,
                chunk_size=500,
                chunk_overlap=80,
                force_reindex=True,
            )
            elapsed = time.time() - t0
            docs = result.get("total_docs_indexed", "?")
            chunks = result.get("chunks_indexed", "?")
            print(f"         → {result['status']}: {docs} docs ({chunks} chunks), {elapsed:.1f}s")
            results.append(result)
        except Exception as e:
            elapsed = time.time() - t0
            print(f"         → ERROR: {e} ({elapsed:.1f}s)")
            results.append({"status": "error", "file": fname, "error": str(e)})

    return results


def extract_all_strategies(indexer, llm_chat_fn, book_filter=None):
    """Extract investment strategies from all indexed books."""
    print(f"\n{'='*70}")
    print("  STEP 2: Extracting investment strategies with LLM")
    print(f"{'='*70}\n")

    from app.services.book_indexer import USER_DOCS_DIR
    docs_dir = str(USER_DOCS_DIR)
    os.makedirs(STRATEGIES_DIR, exist_ok=True)

    supported = {".pdf", ".epub"}
    files = sorted([
        f for f in os.listdir(docs_dir)
        if os.path.splitext(f)[1].lower() in supported
    ])

    if book_filter:
        files = [f for f in files if book_filter.lower() in f.lower()]

    all_reports = []
    grand_total_strategies = 0
    grand_total_fields = set()

    for i, fname in enumerate(files):
        file_path = os.path.join(docs_dir, fname)
        short_name = fname[:60] + "..." if len(fname) > 60 else fname
        print(f"\n  [{i+1}/{len(files)}] Parsing: {short_name}")

        t0 = time.time()
        try:
            book = indexer.parse_book(file_path)
            print(f"         Parsed: {len(book.chapters)} chapters, {book.total_chars:,} chars")

            print(f"         Extracting strategies...")
            report = extract_book_strategies(
                book, llm_chat_fn, output_dir=STRATEGIES_DIR
            )

            elapsed = time.time() - t0
            n_strats = report.get("strategies_count", 0)
            n_fields = len(report.get("data_fields_required", []))
            grand_total_strategies += n_strats
            grand_total_fields.update(report.get("data_fields_required", []))

            print(f"         → {n_strats} strategies, {n_fields} data fields required ({elapsed:.1f}s)")

            # Show category breakdown
            by_cat = report.get("strategies_by_category", {})
            if by_cat:
                cats = ", ".join(f"{k}={v}" for k, v in sorted(by_cat.items()))
                print(f"           Categories: {cats}")

            all_reports.append(report)

        except Exception as e:
            elapsed = time.time() - t0
            print(f"         → ERROR: {e} ({elapsed:.1f}s)")
            all_reports.append({
                "book_title": fname,
                "status": "error",
                "error": str(e),
            })

    # Build combined report
    combined = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "books_processed": len(all_reports),
        "total_strategies": grand_total_strategies,
        "total_data_fields": sorted(grand_total_fields),
        "data_fields_count": len(grand_total_fields),
        "books": [],
    }

    for r in all_reports:
        combined["books"].append({
            "title": r.get("book_title", "?"),
            "strategies_count": r.get("strategies_count", 0),
            "categories": r.get("strategies_by_category", {}),
            "data_fields": r.get("data_fields_required", []),
        })

    # Save combined report
    combined_path = os.path.join(STRATEGIES_DIR, "_combined_strategies.json")
    with open(combined_path, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    # Print grand summary
    print(f"\n{'='*70}")
    print(f"  STRATEGY EXTRACTION COMPLETE")
    print(f"{'='*70}")
    print(f"  Books processed:     {len(all_reports)}")
    print(f"  Total strategies:    {grand_total_strategies}")
    print(f"  Unique data fields:  {len(grand_total_fields)}")
    print(f"  Output directory:    {STRATEGIES_DIR}")
    print(f"  Combined report:     {combined_path}")
    print(f"{'='*70}\n")

    # Print per-book summary
    print(f"  {'Book':<50} {'Strategies':>12} {'Fields':>8}")
    print(f"  {'-'*50} {'-'*12} {'-'*8}")
    for r in all_reports:
        title = r.get("book_title", "?")[:48]
        n = r.get("strategies_count", 0)
        f = len(r.get("data_fields_required", []))
        print(f"  {title:<50} {n:>12} {f:>8}")
    print()

    return combined


def main():
    args = set(sys.argv[1:])

    # Extract --book filter
    book_filter = None
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg == "--book" and i < len(sys.argv) - 1:
            book_filter = sys.argv[i + 1]
            break

    indexer = BookIndexer(chroma_persist_dir=CHROMA_DIR)

    if "--stats" in args:
        show_stats(indexer)
        return

    strategies_only = "--strategies-only" in args

    # Step 1: Reindex (unless --strategies-only)
    if not strategies_only:
        reindex_results = reindex_all(indexer, book_filter)
        show_stats(indexer)
    else:
        print("  Skipping reindex (--strategies-only)")

    # Step 2: Extract strategies
    llm_fn = _get_llm_chat_fn()
    if llm_fn:
        extract_all_strategies(indexer, llm_fn, book_filter)
    else:
        print("\n  WARNING: LLM not available. Skipping strategy extraction.")
        print("  Reindexing completed. Run with LLM configured to extract strategies.\n")


if __name__ == "__main__":
    main()
