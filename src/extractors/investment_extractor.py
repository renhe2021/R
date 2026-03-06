"""LLM 驱动的投资知识提取器"""

import json
import logging
from typing import Optional

from ..models import (
    BookContent, Chapter, InvestmentKnowledge,
    StockRule, Indicator, IndicatorType,
)
from ..llm.base import LLMProvider

logger = logging.getLogger(__name__)

EXTRACTION_PROMPT = """你是一个专业的投资分析师。请仔细阅读以下书籍章节内容，提取其中的投资知识。

请以 JSON 格式返回以下信息（如果某项没有相关内容，返回空数组或空字符串）：

{
  "rules": [
    {
      "description": "选股规则的自然语言描述",
      "expression": "尝试转化为条件表达式，如 PE < 15 AND ROE > 0.15，无法转化则为 null"
    }
  ],
  "indicators": [
    {
      "name": "指标名称，如 PE, ROE, MACD",
      "type": "fundamental 或 technical 或 mixed",
      "definition": "指标的简要定义"
    }
  ],
  "data_requirements": ["实现这些规则需要的市场数据，如 '每日收盘价', '季度财报数据' 等"],
  "summary": "本章核心投资理念的一段话摘要"
}

重要：只返回 JSON，不要添加任何其他文字说明或 markdown 格式。"""


class InvestmentExtractor:
    def __init__(self, llm_provider: LLMProvider):
        self._llm = llm_provider

    def extract_from_book(self, book: BookContent) -> InvestmentKnowledge:
        """从整本书提取投资知识"""
        logger.info(f"开始提取投资知识: {book.title} ({len(book.chapters)} 个章节)")

        all_rules: list[StockRule] = []
        all_indicators: list[Indicator] = []
        all_data_reqs: list[str] = []
        all_summaries: list[str] = []

        for i, chapter in enumerate(book.chapters):
            logger.info(f"  提取章节 [{i + 1}/{len(book.chapters)}]: {chapter.title}")

            result = self._extract_chapter(chapter)
            if result is None:
                continue

            all_rules.extend(result.get("rules", []))
            all_indicators.extend(result.get("indicators", []))
            all_data_reqs.extend(result.get("data_requirements", []))
            if summary := result.get("summary", ""):
                all_summaries.append(f"**{chapter.title}**: {summary}")

        # 汇总去重
        knowledge = InvestmentKnowledge(
            book_title=book.title,
            rules=_deduplicate_rules(all_rules),
            indicators=_deduplicate_indicators(all_indicators),
            data_requirements=list(set(all_data_reqs)),
            summary="\n\n".join(all_summaries),
        )

        logger.info(
            f"知识提取完成: {len(knowledge.rules)} 条规则, "
            f"{len(knowledge.indicators)} 个指标, "
            f"{len(knowledge.data_requirements)} 项数据需求"
        )
        return knowledge

    def _extract_chapter(self, chapter: Chapter) -> Optional[dict]:
        """从单个章节提取知识"""
        # 截断过长内容（避免超出 context window）
        content = chapter.content[:8000]
        if len(chapter.content) > 8000:
            content += "\n...(内容已截断)"

        try:
            response = self._llm.chat(
                messages=[{"role": "user", "content": f"章节标题: {chapter.title}\n\n章节内容:\n{content}"}],
                system_prompt=EXTRACTION_PROMPT,
            )
            return _parse_json_response(response)
        except Exception as e:
            logger.warning(f"章节 '{chapter.title}' 知识提取失败: {e}")
            return None


def _parse_json_response(text: str) -> Optional[dict]:
    """从 LLM 响应中解析 JSON"""
    text = text.strip()
    # 去除 markdown code block
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        # 尝试找到 JSON 部分
        start = text.find("{")
        end = text.rfind("}") + 1
        if start >= 0 and end > start:
            try:
                data = json.loads(text[start:end])
            except json.JSONDecodeError:
                logger.warning("无法解析 LLM 返回的 JSON")
                return None
        else:
            return None

    return data


def _deduplicate_rules(raw_rules: list[dict]) -> list[StockRule]:
    """去重并转化为 StockRule"""
    seen = set()
    results = []
    for r in raw_rules:
        if isinstance(r, dict):
            desc = r.get("description", "")
            if desc and desc not in seen:
                seen.add(desc)
                results.append(StockRule(
                    description=desc,
                    expression=r.get("expression"),
                ))
    return results


def _deduplicate_indicators(raw_indicators: list[dict]) -> list[Indicator]:
    """去重并转化为 Indicator"""
    seen = set()
    results = []
    for ind in raw_indicators:
        if isinstance(ind, dict):
            name = ind.get("name", "")
            if name and name not in seen:
                seen.add(name)
                type_str = ind.get("type", "mixed")
                try:
                    ind_type = IndicatorType(type_str)
                except ValueError:
                    ind_type = IndicatorType.MIXED
                results.append(Indicator(
                    name=name,
                    type=ind_type,
                    definition=ind.get("definition", ""),
                ))
    return results
