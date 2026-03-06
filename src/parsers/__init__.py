from .base import BaseParser
from .pdf_parser import PDFParser
from .epub_parser import EPUBParser
from .docx_parser import DocxParser


def get_parser(file_path: str) -> BaseParser:
    """根据文件扩展名返回对应的解析器实例"""
    ext = file_path.rsplit(".", 1)[-1].lower()
    parsers = {
        "pdf": PDFParser,
        "epub": EPUBParser,
        "docx": DocxParser,
    }
    parser_cls = parsers.get(ext)
    if parser_cls is None:
        raise ValueError(f"不支持的文件格式: .{ext}，支持: {', '.join(parsers.keys())}")
    return parser_cls()
