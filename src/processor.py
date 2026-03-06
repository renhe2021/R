"""内容处理器：文本清洗 + 语义分块"""

import re
import logging

from langchain_text_splitters import RecursiveCharacterTextSplitter

from .models import BookContent, Chapter, TextChunk
from .config import ChunkConfig

logger = logging.getLogger(__name__)


def clean_text(text: str) -> str:
    """文本清洗"""
    # 去除多余空白行
    text = re.sub(r"\n{3,}", "\n\n", text)
    # 合并跨页断行（行尾非标点的短行 + 下一行首字母小写或中文）
    text = re.sub(r"(?<=[a-zA-Z\u4e00-\u9fff])\n(?=[a-z\u4e00-\u9fff])", " ", text)
    # 去除多余空格
    text = re.sub(r"[ \t]{2,}", " ", text)
    # 去除常见页码模式
    text = re.sub(r"\n\s*-?\s*\d+\s*-?\s*\n", "\n", text)
    return text.strip()


def process_book(book: BookContent, chunk_config: ChunkConfig | None = None) -> list[TextChunk]:
    """处理书籍：清洗文本 + 语义分块"""
    if chunk_config is None:
        chunk_config = ChunkConfig()

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_config.chunk_size,
        chunk_overlap=chunk_config.chunk_overlap,
        separators=["\n\n", "\n", "。", ".", " ", ""],
    )

    all_chunks: list[TextChunk] = []
    chunk_index = 0

    for chapter in book.chapters:
        cleaned = clean_text(chapter.content)
        if not cleaned:
            continue

        splits = splitter.split_text(cleaned)

        for split_text in splits:
            all_chunks.append(TextChunk(
                content=split_text,
                chapter_title=chapter.title,
                chunk_index=chunk_index,
                metadata={
                    "book_title": book.title,
                    "chapter_level": chapter.level,
                    "source_file": book.source_file,
                },
            ))
            chunk_index += 1

    logger.info(f"处理完成: {len(book.chapters)} 章节 -> {len(all_chunks)} 个文本块")
    return all_chunks
