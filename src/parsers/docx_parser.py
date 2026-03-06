"""Word (.docx) 文件解析器"""

import logging
from pathlib import Path

from docx import Document

from .base import BaseParser
from ..models import BookContent, Chapter

logger = logging.getLogger(__name__)

# Word 内置标题样式到层级的映射
HEADING_STYLE_MAP = {
    "Heading 1": 1, "Heading 2": 2, "Heading 3": 3,
    "标题 1": 1, "标题 2": 2, "标题 3": 3,
    "Title": 1, "Subtitle": 2,
}


class DocxParser(BaseParser):
    def supports(self, file_path: str) -> bool:
        return file_path.lower().endswith(".docx")

    def parse(self, file_path: str) -> BookContent:
        logger.info(f"开始解析 Word: {file_path}")
        path = Path(file_path)
        doc = Document(file_path)

        chapters: list[Chapter] = []
        current_title = "正文"
        current_level = 1
        current_lines: list[str] = []
        all_text_parts: list[str] = []

        for para in doc.paragraphs:
            style_name = para.style.name if para.style else ""
            text = para.text.strip()

            if not text:
                continue

            all_text_parts.append(text)

            if style_name in HEADING_STYLE_MAP:
                # 遇到标题，保存上一个章节
                if current_lines:
                    content = "\n".join(current_lines).strip()
                    if content:
                        chapters.append(Chapter(
                            title=current_title,
                            level=current_level,
                            content=content,
                        ))
                current_title = text
                current_level = HEADING_STYLE_MAP[style_name]
                current_lines = []
            else:
                current_lines.append(text)

        # 保存最后一个章节
        if current_lines:
            content = "\n".join(current_lines).strip()
            if content:
                chapters.append(Chapter(
                    title=current_title,
                    level=current_level,
                    content=content,
                ))

        raw_text = "\n".join(all_text_parts)

        if not chapters and raw_text.strip():
            chapters.append(Chapter(title="全文", level=1, content=raw_text.strip()))

        result = BookContent(
            title=path.stem,
            chapters=chapters,
            raw_text=raw_text,
            source_file=str(path.absolute()),
        )
        logger.info(f"Word 解析完成: {len(chapters)} 个章节, {len(raw_text)} 字符")
        return result
