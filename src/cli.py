"""CLI 入口"""

import json
import logging
import sys
from pathlib import Path

import click

from .config import load_config
from .models import InvestmentKnowledge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--config", "config_path", default=None, help="配置文件路径 (默认: config.yaml)")
@click.pass_context
def cli(ctx, config_path):
    """Book Knowledge Base - 投资选股书籍知识库工具"""
    ctx.ensure_object(dict)
    ctx.obj["config"] = load_config(config_path)


@cli.command()
@click.argument("file_path", type=click.Path(exists=True))
@click.option("--provider", default=None, help="LLM provider (claude/zhipu/deepseek)")
@click.pass_context
def ingest(ctx, file_path, provider):
    """导入书籍：解析 -> 分块 -> Markdown 导出 -> 向量库构建"""
    config = ctx.obj["config"]

    click.echo(f"[*] 开始导入: {file_path}")

    # 1. 解析
    from .parsers import get_parser
    parser = get_parser(file_path)
    book = parser.parse(file_path)
    click.echo(f"  [OK] 解析完成: {len(book.chapters)} 个章节, {len(book.raw_text)} 字符")

    # 2. 分块
    from .processor import process_book
    chunks = process_book(book, config.chunking)
    click.echo(f"  [OK] 分块完成: {len(chunks)} 个文本块")

    # 3. Markdown 导出
    from .exporters.markdown_exporter import MarkdownExporter
    exporter = MarkdownExporter(config.storage.output_dir)
    output_path = exporter.export(book)
    click.echo(f"  [OK] Markdown 导出: {output_path}")

    # 4. 向量库构建
    from .exporters.vector_store import VectorStoreBuilder
    llm_provider = None
    provider_name = provider or config.llm.default_provider
    try:
        from .llm import get_llm_provider
        llm_config = config.llm.model_dump()
        llm_provider = get_llm_provider(provider_name, llm_config)
    except Exception as e:
        click.echo(f"  [WARN] LLM provider 初始化失败 ({e})，使用默认 embedding")

    builder = VectorStoreBuilder(config.storage.vectordb_dir)
    builder.build(chunks, book.title, llm_provider)
    click.echo(f"  [OK] 向量库构建完成")

    click.echo(f"\n[DONE] 导入成功! 书名: {book.title}")


@cli.command()
@click.argument("book_name")
@click.option("--provider", default=None, help="LLM provider")
@click.pass_context
def extract(ctx, book_name, provider):
    """从已导入的书籍中提取投资知识（选股规则、指标、数据需求）"""
    config = ctx.obj["config"]

    # 查找已导出的 Markdown 文件，重新加载章节
    output_dir = Path(config.storage.output_dir) / book_name
    if not output_dir.exists():
        click.echo(f"[ERROR] 书籍 '{book_name}' 未找到，请先使用 ingest 命令导入")
        sys.exit(1)

    # 从 Markdown 恢复 BookContent
    from .models import BookContent, Chapter
    chapters = []
    for md_file in sorted(output_dir.glob("*.md")):
        if md_file.name in ("INDEX.md", "KNOWLEDGE_SUMMARY.md"):
            continue
        content = md_file.read_text(encoding="utf-8")
        # 第一行是标题
        lines = content.split("\n", 1)
        title = lines[0].lstrip("#").strip()
        body = lines[1] if len(lines) > 1 else ""
        chapters.append(Chapter(title=title, level=2, content=body.strip()))

    book = BookContent(title=book_name, chapters=chapters)
    click.echo(f"[*] 加载 {len(chapters)} 个章节，开始提取投资知识...")

    # LLM 提取
    provider_name = provider or config.llm.default_provider
    from .llm import get_llm_provider
    llm_config = config.llm.model_dump()
    llm_provider = get_llm_provider(provider_name, llm_config)

    from .extractors import InvestmentExtractor
    extractor = InvestmentExtractor(llm_provider)
    knowledge = extractor.extract_from_book(book)

    # 保存结果
    knowledge_dir = Path(config.storage.knowledge_dir) / book_name
    knowledge_dir.mkdir(parents=True, exist_ok=True)

    _save_knowledge(knowledge, knowledge_dir)
    click.echo(f"\n[DONE] 知识提取完成!")
    click.echo(f"  - {len(knowledge.rules)} 条选股规则")
    click.echo(f"  - {len(knowledge.indicators)} 个指标")
    click.echo(f"  - {len(knowledge.data_requirements)} 项数据需求")
    click.echo(f"  - 保存至: {knowledge_dir}")


@cli.command()
@click.argument("query")
@click.option("--book", default=None, help="指定搜索的书名")
@click.option("--top-k", default=5, help="返回结果数量")
@click.option("--provider", default=None, help="LLM provider")
@click.pass_context
def search(ctx, query, book, top_k, provider):
    """语义检索：输入自然语言问题，返回最相关的书籍内容"""
    config = ctx.obj["config"]

    from .search import KnowledgeSearcher
    from .llm import get_llm_provider

    llm_provider = None
    provider_name = provider or config.llm.default_provider
    try:
        llm_config = config.llm.model_dump()
        llm_provider = get_llm_provider(provider_name, llm_config)
    except Exception:
        pass

    searcher = KnowledgeSearcher(config.storage.vectordb_dir, llm_provider)

    # 搜索所有书或指定书
    books = [book] if book else searcher.list_books()
    if not books:
        click.echo("[ERROR] 没有已导入的书籍，请先使用 ingest 命令导入")
        return

    all_results = []
    for b in books:
        results = searcher.search(query, b, top_k=top_k)
        all_results.extend(results)

    # 按分数排序取 top_k
    all_results.sort(key=lambda r: r.score, reverse=True)
    all_results = all_results[:top_k]

    if not all_results:
        click.echo("[*] 未找到相关内容")
        return

    click.echo(f"\n[*] 搜索结果 (共 {len(all_results)} 条):\n")
    for i, r in enumerate(all_results, 1):
        click.echo(f"--- [{i}] 相似度: {r.score:.3f} | 章节: {r.chapter_title} ---")
        click.echo(f"{r.content[:300]}{'...' if len(r.content) > 300 else ''}")
        click.echo()


@cli.command("list")
@click.pass_context
def list_books(ctx):
    """列出所有已导入的书籍"""
    config = ctx.obj["config"]

    from .search import KnowledgeSearcher
    searcher = KnowledgeSearcher(config.storage.vectordb_dir)
    books = searcher.list_books()

    if not books:
        click.echo("[*] 暂无已导入的书籍")
        return

    click.echo(f"\n[*] 已导入 {len(books)} 本书:\n")
    for i, name in enumerate(books, 1):
        # 检查是否有知识提取结果
        knowledge_dir = Path(config.storage.knowledge_dir) / name
        has_knowledge = knowledge_dir.exists()
        status = "[OK] 已提取知识" if has_knowledge else "[--] 待提取知识"
        click.echo(f"  {i}. {name}  [{status}]")


@cli.command()
@click.option("--dir", "scan_dir", default=None, help="扫描目录 (默认: data/books)")
@click.option("--provider", default=None, help="LLM provider (claude/zhipu/deepseek)")
@click.option("--yes", "-y", is_flag=True, help="跳过确认，自动导入所有新书")
@click.pass_context
def scan(ctx, scan_dir, provider, yes):
    """扫描目录中未导入的书籍，批量导入"""
    config = ctx.obj["config"]
    supported_exts = {".pdf", ".epub", ".docx"}

    # 确定扫描目录
    books_dir = Path(scan_dir) if scan_dir else Path("data/books")
    if not books_dir.exists():
        books_dir.mkdir(parents=True, exist_ok=True)
        click.echo(f"[*] 已创建目录: {books_dir}")
        click.echo(f"[*] 请将书籍文件放入 {books_dir} 后重新运行")
        return

    # 扫描支持的文件
    all_files = []
    for ext in supported_exts:
        all_files.extend(books_dir.glob(f"*{ext}"))
    all_files.sort(key=lambda f: f.name)

    if not all_files:
        click.echo(f"[*] 目录 {books_dir} 中没有找到支持的书籍文件")
        click.echo(f"    支持格式: {', '.join(supported_exts)}")
        return

    # 检查哪些已经导入过
    from .search import KnowledgeSearcher
    searcher = KnowledgeSearcher(config.storage.vectordb_dir)
    imported_books = set(searcher.list_books())

    new_files = []
    skip_files = []
    for f in all_files:
        book_name = f.stem
        if book_name in imported_books:
            skip_files.append(f)
        else:
            new_files.append(f)

    # 显示扫描结果
    click.echo(f"\n[*] 扫描 {books_dir}:")
    click.echo(f"    找到 {len(all_files)} 个文件, {len(new_files)} 个待导入, {len(skip_files)} 个已导入\n")

    if skip_files:
        click.echo("  已导入 (跳过):")
        for f in skip_files:
            click.echo(f"    - {f.name}")
        click.echo()

    if not new_files:
        click.echo("[*] 没有新书需要导入")
        return

    click.echo("  待导入:")
    for i, f in enumerate(new_files, 1):
        size_mb = f.stat().st_size / (1024 * 1024)
        click.echo(f"    {i}. {f.name}  ({size_mb:.1f} MB)")
    click.echo()

    # 确认
    if not yes:
        if not click.confirm(f"确认导入以上 {len(new_files)} 本书?"):
            click.echo("[*] 已取消")
            return

    # 批量导入
    success = 0
    failed = []
    for i, f in enumerate(new_files, 1):
        click.echo(f"\n{'='*50}")
        click.echo(f"[{i}/{len(new_files)}] 导入: {f.name}")
        click.echo(f"{'='*50}")
        try:
            ctx.invoke(ingest, file_path=str(f), provider=provider)
            success += 1
        except Exception as e:
            click.echo(f"  [ERROR] 导入失败: {e}")
            failed.append((f.name, str(e)))

    # 汇总
    click.echo(f"\n{'='*50}")
    click.echo(f"[DONE] 批量导入完成: {success} 成功, {len(failed)} 失败")
    if failed:
        click.echo("\n  失败列表:")
        for name, err in failed:
            click.echo(f"    - {name}: {err}")


@cli.command()
@click.argument("symbol")
@click.option("--book", default=None, help="指定使用某本书的规则 (默认: 使用全部)")
@click.option("--provider", default=None, help="LLM provider")
@click.option("--data-source", "-d", default="auto",
              type=click.Choice(["auto", "bloomberg", "yfinance"]),
              help="数据源 (默认: auto，优先 Bloomberg)")
@click.option("--llm-report", "-l", is_flag=True, help="使用 LLM 生成深度分析报告")
@click.pass_context
def analyze(ctx, symbol, book, provider, data_source, llm_report):
    """分析单只股票：用知识库中的规则评估个股

    示例:
      book-kb analyze NVDA              # 自动选数据源
      book-kb analyze NVDA -d bloomberg  # 强制用彭博
      book-kb analyze NVDA -d yfinance   # 强制用 yfinance
      book-kb analyze NVDA -l            # 带 LLM 深度分析
    """
    config = ctx.obj["config"]

    from .analyzer import (
        load_knowledge_rules,
        evaluate_rules, generate_analysis_report,
    )
    from .data_providers import get_data_provider

    # 1. 获取股票数据
    click.echo(f"\n[*] 获取 {symbol.upper()} 的市场数据...")
    try:
        dp = get_data_provider(data_source)
        click.echo(f"  [数据源] {dp.name}")
        stock = dp.fetch(symbol.upper())
    except Exception as e:
        click.echo(f"[ERROR] 获取数据失败: {e}")
        return
    click.echo(f"  [OK] {stock.name} ({stock.symbol}) - {stock.sector}/{stock.industry}")

    # 2. 加载规则
    click.echo(f"[*] 加载知识库规则...")
    rules = load_knowledge_rules(config.storage.knowledge_dir, book)
    if not rules:
        click.echo("[ERROR] 未找到可用规则，请先用 extract 命令提取知识")
        return
    click.echo(f"  [OK] 加载 {len(rules)} 条量化规则")

    # 3. 评估规则
    click.echo(f"[*] 评估规则...")
    results = evaluate_rules(stock, rules)

    # 4. 输出报告
    report = generate_analysis_report(stock, results)
    click.echo(report)

    # 5. 可选：LLM 深度分析
    if llm_report:
        click.echo(f"\n[*] 使用 LLM 生成深度分析报告...")
        provider_name = provider or config.llm.default_provider
        from .llm import get_llm_provider
        llm_config = config.llm.model_dump()
        llm_provider = get_llm_provider(provider_name, llm_config)

        passed = [r for r in results if r.passed is True]
        failed = [r for r in results if r.passed is False]

        prompt = f"""你是一位资深投资分析师。请基于以下数据和规则评估结果，为 {stock.name} ({stock.symbol}) 生成一份简洁的投资分析报告。

## 股票基础数据
- 行业: {stock.sector} / {stock.industry}
- 股价: ${stock.price:.2f}, 市值: ${stock.market_cap/1e9:.1f}B
- PE: {stock.pe:.1f}, Forward PE: {stock.forward_pe:.1f}, PB: {stock.pb:.1f}
- ROE: {stock.roe*100:.1f}%, EPS: ${stock.eps:.2f}
- 负债权益比: {stock.debt_to_equity:.2f}, 流动比率: {stock.current_ratio:.2f}
- 股息率: {stock.dividend_yield:.2f}%

## 规则评估结果
- 通过 {len(passed)} 条，不通过 {len(failed)} 条

### 通过的规则 (前10条):
{chr(10).join(f"- [{r.expression}] {r.description[:60]}" for r in passed[:10])}

### 不通过的规则 (前10条):
{chr(10).join(f"- [{r.expression}] {r.description[:60]}" for r in failed[:10])}

请用中文输出，包含：
1. 一句话总结（买入/持有/观望/回避）
2. 核心优势（基于通过的规则）
3. 主要风险（基于不通过的规则）
4. 从价值投资角度的建议
"""
        try:
            analysis = llm_provider.chat(
                messages=[{"role": "user", "content": prompt}],
            )
            click.echo(f"\n{'='*60}")
            click.echo(f"  LLM 深度分析报告")
            click.echo(f"{'='*60}")
            click.echo(analysis)
        except Exception as e:
            click.echo(f"[ERROR] LLM 分析失败: {e}")


# --- 辅助函数 ---

def _save_knowledge(knowledge: InvestmentKnowledge, output_dir: Path):
    """保存知识提取结果为 JSON"""
    import dataclasses

    def _serialize(obj):
        if dataclasses.is_dataclass(obj):
            return dataclasses.asdict(obj)
        if hasattr(obj, "value"):
            return obj.value
        return str(obj)

    data = {
        "book_title": knowledge.book_title,
        "rules": [dataclasses.asdict(r) for r in knowledge.rules],
        "indicators": [
            {**dataclasses.asdict(ind), "type": ind.type.value}
            for ind in knowledge.indicators
        ],
        "data_requirements": knowledge.data_requirements,
        "summary": knowledge.summary,
    }

    (output_dir / "knowledge.json").write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
