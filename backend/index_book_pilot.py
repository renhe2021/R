"""
Pilot Script: Index "The Warren Buffett Way" with Three-Layer Architecture

Usage:
    python index_book_pilot.py              # Index the book
    python index_book_pilot.py --test       # Index + run search quality tests
    python index_book_pilot.py --stats      # Show collection stats
    python index_book_pilot.py --reindex    # Force re-index
"""
import sys
import os
import io
import logging

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Setup
sys.path.insert(0, os.path.dirname(__file__))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

from app.services.book_indexer import BookIndexer

CHROMA_DIR = os.path.join(os.path.dirname(__file__), "chroma_data")
BOOK_FILE = os.path.join(
    os.path.dirname(__file__), "app", "knowledge", "user_docs",
    "The Warren Buffett Way (Robert G. Hagstrom) (z-library.sk, 1lib.sk, z-lib.sk).pdf"
)


def main():
    args = set(sys.argv[1:])
    indexer = BookIndexer(chroma_persist_dir=CHROMA_DIR)

    if "--stats" in args:
        stats = indexer.get_stats()
        print(f"\n{'='*60}")
        print(f"Collection Stats: {stats['collection']}")
        print(f"{'='*60}")
        print(f"Total documents: {stats['total_documents']}")
        print(f"Chapter docs (Layer 2): {stats['chapter_docs']}")
        print(f"Paragraph chunks (Layer 3): {stats['paragraph_chunks']}")
        print(f"Books indexed ({stats['books_count']}): {', '.join(stats['books_indexed'])}")
        return

    # Step 1: Index the book
    print(f"\n{'='*60}")
    print("Step 1: Indexing 'The Warren Buffett Way'")
    print(f"{'='*60}")

    force = "--reindex" in args
    result = indexer.index_book(BOOK_FILE, force_reindex=force)
    print(f"\nResult:")
    for k, v in result.items():
        print(f"  {k}: {v}")

    # Step 2: Show stats
    stats = indexer.get_stats()
    print(f"\nCollection Stats:")
    print(f"  Total docs: {stats['total_documents']}")
    print(f"  Chapters: {stats['chapter_docs']}")
    print(f"  Chunks: {stats['paragraph_chunks']}")

    # Step 3: Search quality tests
    if "--test" in args or "--reindex" in args:
        print(f"\n{'='*60}")
        print("Step 2: Search Quality Tests")
        print(f"{'='*60}")

        test_queries = [
            # Specific concept queries
            ("What are owner earnings and how to calculate them?", "paragraph"),
            ("What is the Institutional Imperative?", "paragraph"),
            ("How does Buffett determine intrinsic value?", "paragraph"),

            # Chapter-level queries
            ("Twelve Immutable Tenets for buying a business", "chapter"),
            ("Psychology of investing and behavioral finance", "chapter"),

            # Case study queries
            ("Washington Post Company investment analysis", "paragraph"),
            ("GEICO Corporation valuation", "paragraph"),
            ("Coca-Cola investment thesis", "paragraph"),

            # Cross-cutting queries
            ("margin of safety and when to buy", "paragraph"),
            ("focus investing vs diversification", "paragraph"),
        ]

        for query, layer in test_queries:
            results = indexer.search(query, top_k=3, layer=layer)
            print(f"\n--- Query: \"{query}\" (layer={layer}) ---")
            if not results:
                print("  NO RESULTS FOUND")
                continue
            for i, r in enumerate(results):
                meta = r["metadata"]
                score = r["score"]
                preview = r["content"][:200].replace("\n", " ")
                ch = meta.get("chapter_title", "?")
                print(f"  [{i+1}] score={score:.3f} | Ch: {ch}")
                print(f"      {preview}...")

    print("\nDone!")


if __name__ == "__main__":
    main()
