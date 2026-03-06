"""核心数据结构定义"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class IndicatorType(Enum):
    FUNDAMENTAL = "fundamental"  # 基本面：PE, PB, ROE 等
    TECHNICAL = "technical"      # 技术面：MACD, RSI, KDJ 等
    MIXED = "mixed"


@dataclass
class Chapter:
    """书籍章节"""
    title: str
    level: int  # 1=部分, 2=章, 3=节
    content: str
    sub_chapters: list["Chapter"] = field(default_factory=list)


@dataclass
class BookContent:
    """整本书的解析结果"""
    title: str
    author: str = ""
    chapters: list[Chapter] = field(default_factory=list)
    raw_text: str = ""
    source_file: str = ""


@dataclass
class TextChunk:
    """文本分块（用于向量库存储）"""
    content: str
    chapter_title: str
    chunk_index: int
    metadata: dict = field(default_factory=dict)
    investment_tags: list[str] = field(default_factory=list)


@dataclass
class StockRule:
    """选股规则"""
    description: str
    expression: Optional[str] = None  # 如 "PE < 15 AND ROE > 0.15"
    source_chapter: str = ""
    target_market: list[str] = field(default_factory=lambda: ["US", "HK"])


@dataclass
class Indicator:
    """财务/技术指标"""
    name: str  # 如 PE, ROE, MACD
    type: IndicatorType = IndicatorType.MIXED
    definition: str = ""


@dataclass
class InvestmentKnowledge:
    """从书中提取的投资知识"""
    book_title: str
    rules: list[StockRule] = field(default_factory=list)
    indicators: list[Indicator] = field(default_factory=list)
    data_requirements: list[str] = field(default_factory=list)
    summary: str = ""
