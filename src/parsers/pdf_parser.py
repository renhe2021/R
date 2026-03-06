"""PDF 文件解析器（支持文字版和扫描版 OCR）"""

import io
import re
import logging
from pathlib import Path

import pdfplumber

from .base import BaseParser
from ..models import BookContent, Chapter

logger = logging.getLogger(__name__)

# 章节标题正则模式
CHAPTER_PATTERNS = [
    re.compile(r"^第[一二三四五六七八九十百千\d]+[章部篇]", re.MULTILINE),
    re.compile(r"^Chapter\s+\d+", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^PART\s+[IVXLCDM\d]+", re.MULTILINE | re.IGNORECASE),
    re.compile(r"^\d+\.\s+\S", re.MULTILINE),
]


def _is_title_line(line: str, chars: list | None = None) -> bool:
    """判断一行是否为标题（基于内容模式）"""
    line = line.strip()
    if not line or len(line) > 100:
        return False
    for pattern in CHAPTER_PATTERNS:
        if pattern.match(line):
            return True
    return False


def _detect_title_level(line: str) -> int:
    """检测标题层级"""
    line = line.strip()
    if re.match(r"^第[一二三四五六七八九十百千\d]+[部篇]", line):
        return 1
    if re.match(r"^(第[一二三四五六七八九十百千\d]+章|Chapter\s+\d+|PART\s+)", line, re.IGNORECASE):
        return 2
    return 3


class PDFParser(BaseParser):
    def supports(self, file_path: str) -> bool:
        return file_path.lower().endswith(".pdf")

    def parse(self, file_path: str) -> BookContent:
        logger.info(f"开始解析 PDF: {file_path}")
        path = Path(file_path)

        # 先尝试用 pdfplumber 直接提取文字
        all_text_lines = self._extract_text_pdfplumber(file_path)
        raw_text = "\n".join(all_text_lines)

        # 如果提取的文字太少（可能是扫描版），回退到 OCR
        if len(raw_text.strip()) < 200:
            logger.info("文字提取结果过少，判断为扫描版 PDF，启动 OCR...")
            all_text_lines = self._extract_text_ocr(file_path)
            raw_text = "\n".join(all_text_lines)

        # 识别章节
        chapters = self._split_chapters(raw_text)
        title = path.stem

        book = BookContent(
            title=title,
            chapters=chapters,
            raw_text=raw_text,
            source_file=str(path.absolute()),
        )
        logger.info(f"PDF 解析完成: {len(chapters)} 个章节, {len(raw_text)} 字符")
        return book

    def _extract_text_pdfplumber(self, file_path: str) -> list[str]:
        """用 pdfplumber 提取文字（适用于文字版 PDF）"""
        lines: list[str] = []
        with pdfplumber.open(file_path) as pdf:
            total = len(pdf.pages)
            for i, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    lines.append(text)
                if (i + 1) % 50 == 0:
                    logger.info(f"  [pdfplumber] 已解析 {i + 1}/{total} 页")
        return lines

    def _extract_text_ocr(self, file_path: str) -> list[str]:
        """用 OCR 提取文字（适用于扫描版 PDF）"""
        try:
            import fitz
            from rapidocr_onnxruntime import RapidOCR
            from PIL import Image
        except ImportError as e:
            logger.error(f"OCR 依赖未安装: {e}")
            logger.error("请运行: pip install PyMuPDF rapidocr-onnxruntime Pillow")
            return []

        ocr = RapidOCR()
        lines: list[str] = []
        doc = fitz.open(file_path)
        total = len(doc)

        for i, page in enumerate(doc):
            pix = page.get_pixmap(dpi=200)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            result, _ = ocr(img)
            if result:
                page_text = "\n".join([line[1] for line in result])
                lines.append(page_text)

            if (i + 1) % 20 == 0:
                logger.info(f"  [OCR] 已处理 {i + 1}/{total} 页")

        doc.close()
        logger.info(f"  [OCR] 全部 {total} 页处理完成")
        return lines

    def _split_chapters(self, text: str) -> list[Chapter]:
        """根据标题模式切分章节"""
        lines = text.split("\n")
        chapters: list[Chapter] = []
        current_title = "前言"
        current_level = 1
        current_lines: list[str] = []

        for line in lines:
            if _is_title_line(line):
                # 保存上一个章节
                if current_lines:
                    content = "\n".join(current_lines).strip()
                    if content:
                        chapters.append(Chapter(
                            title=current_title,
                            level=current_level,
                            content=content,
                        ))
                current_title = line.strip()
                current_level = _detect_title_level(line)
                current_lines = []
            else:
                current_lines.append(line)

        # 保存最后一个章节
        if current_lines:
            content = "\n".join(current_lines).strip()
            if content:
                chapters.append(Chapter(
                    title=current_title,
                    level=current_level,
                    content=content,
                ))

        # 如果没识别到章节，整个文本作为一个章节
        if not chapters and text.strip():
            chapters.append(Chapter(title="全文", level=1, content=text.strip()))

        return chapters
