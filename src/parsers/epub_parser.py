"""EPUB 文件解析器"""

import logging
import re
from pathlib import Path

import ebooklib
from ebooklib import epub
from bs4 import BeautifulSoup

from .base import BaseParser
from ..models import BookContent, Chapter

logger = logging.getLogger(__name__)


class EPUBParser(BaseParser):
    def supports(self, file_path: str) -> bool:
        return file_path.lower().endswith(".epub")

    def parse(self, file_path: str) -> BookContent:
        logger.info(f"开始解析 EPUB: {file_path}")
        path = Path(file_path)
        book = epub.read_epub(file_path)

        title = book.get_metadata("DC", "title")
        title = title[0][0] if title else path.stem

        author = book.get_metadata("DC", "creator")
        author = author[0][0] if author else ""

        chapters = []
        all_text_parts = []

        for item in book.get_items_of_type(ebooklib.ITEM_DOCUMENT):
            content = item.get_content().decode("utf-8", errors="replace")
            soup = BeautifulSoup(content, "lxml")

            # 提取章节标题
            heading = soup.find(re.compile(r"^h[1-6]$"))
            chapter_title = heading.get_text(strip=True) if heading else item.get_name()
            level = int(heading.name[1]) if heading else 2

            # 提取正文
            text = soup.get_text(separator="\n", strip=True)
            if text.strip():
                chapters.append(Chapter(
                    title=chapter_title,
                    level=min(level, 3),
                    content=text,
                ))
                all_text_parts.append(text)

        raw_text = "\n\n".join(all_text_parts)

        result = BookContent(
            title=title,
            author=author,
            chapters=chapters,
            raw_text=raw_text,
            source_file=str(path.absolute()),
        )
        logger.info(f"EPUB 解析完成: {len(chapters)} 个章节, {len(raw_text)} 字符")
        return result
