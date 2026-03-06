"""Markdown 导出器"""

import logging
from pathlib import Path

from ..models import BookContent, InvestmentKnowledge

logger = logging.getLogger(__name__)


class MarkdownExporter:
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)

    def export(self, book: BookContent, knowledge: InvestmentKnowledge | None = None) -> Path:
        """按章节拆分导出 Markdown 文件"""
        book_dir = self.output_dir / _safe_name(book.title)
        book_dir.mkdir(parents=True, exist_ok=True)

        index_lines = [f"# {book.title}\n"]
        if book.author:
            index_lines.append(f"**作者**: {book.author}\n")
        index_lines.append(f"**来源**: {book.source_file}\n")
        index_lines.append("\n## 目录\n")

        for i, chapter in enumerate(book.chapters, 1):
            filename = f"{i:02d}_{_safe_name(chapter.title)}.md"
            chapter_path = book_dir / filename

            # 写章节文件
            md_content = f"{'#' * chapter.level} {chapter.title}\n\n{chapter.content}\n"

            # 如果有投资知识，附加到对应章节
            if knowledge:
                chapter_rules = [r for r in knowledge.rules if r.source_chapter == chapter.title]
                if chapter_rules:
                    md_content += "\n---\n\n## 📌 提取的选股规则\n\n"
                    for rule in chapter_rules:
                        md_content += f"- {rule.description}\n"
                        if rule.expression:
                            md_content += f"  - 表达式: `{rule.expression}`\n"

            chapter_path.write_text(md_content, encoding="utf-8")
            index_lines.append(f"{i}. [{chapter.title}]({filename})\n")

        # 写知识摘要
        if knowledge and knowledge.summary:
            summary_path = book_dir / "KNOWLEDGE_SUMMARY.md"
            summary_content = f"# {book.title} - 投资知识摘要\n\n{knowledge.summary}\n"

            if knowledge.indicators:
                summary_content += "\n## 涉及的指标\n\n"
                for ind in knowledge.indicators:
                    summary_content += f"- **{ind.name}** ({ind.type.value}): {ind.definition}\n"

            if knowledge.rules:
                summary_content += "\n## 选股规则\n\n"
                for rule in knowledge.rules:
                    summary_content += f"- {rule.description}\n"
                    if rule.expression:
                        summary_content += f"  - `{rule.expression}`\n"

            if knowledge.data_requirements:
                summary_content += "\n## 数据需求\n\n"
                for req in knowledge.data_requirements:
                    summary_content += f"- {req}\n"

            summary_path.write_text(summary_content, encoding="utf-8")
            index_lines.append(f"\n[投资知识摘要](KNOWLEDGE_SUMMARY.md)\n")

        # 写目录索引
        index_path = book_dir / "INDEX.md"
        index_path.write_text("".join(index_lines), encoding="utf-8")

        logger.info(f"Markdown 导出完成: {book_dir}")
        return book_dir


def _safe_name(name: str) -> str:
    """生成安全文件名"""
    import re
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    return name[:80].strip("_. ")
