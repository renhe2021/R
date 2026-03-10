"""Knowledge base API — book list, rules, cross-validate.

Provides endpoints previously in src/web_app.py (Flask) for the FastAPI backend:
  - GET  /books           — list imported books (from data/knowledge/)
  - GET  /rules/{book}    — get rules for a specific book
  - POST /cross-validate  — multi-source cross-validation for a stock
"""

import json
import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

logger = logging.getLogger(__name__)

router = APIRouter(tags=["knowledge"])

# Project root: backend/app/api/knowledge_routes.py → ../../.. = project root
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_KNOWLEDGE_DIR = _PROJECT_ROOT / "data" / "knowledge"


# ── GET /api/books ──────────────────────────────────────────────

@router.get("/books")
async def list_books():
    """获取已导入书籍列表（从 data/knowledge/ 读取）"""
    books = []
    try:
        if _KNOWLEDGE_DIR.exists():
            for d in sorted(_KNOWLEDGE_DIR.iterdir()):
                if d.is_dir():
                    kfile = d / "knowledge.json"
                    info = {
                        "name": d.name,
                        "has_knowledge": kfile.exists(),
                        "rule_count": 0,
                        "indicator_count": 0,
                        "summary": "",
                    }
                    if kfile.exists():
                        try:
                            kdata = json.loads(kfile.read_text(encoding="utf-8"))
                            info["rule_count"] = len(kdata.get("rules", []))
                            info["indicator_count"] = len(kdata.get("indicators", []))
                            info["summary"] = (kdata.get("summary", "") or "")[:200]
                        except Exception as e:
                            logger.warning(f"Failed to parse {kfile}: {e}")
                    books.append(info)
    except Exception as e:
        logger.error(f"Failed to list books: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    return {"books": books}


# ── GET /api/rules/{book_name} ──────────────────────────────────

@router.get("/rules/{book_name:path}")
async def get_book_rules(book_name: str):
    """获取某本书的规则详情"""
    kfile = _KNOWLEDGE_DIR / book_name / "knowledge.json"
    if not kfile.exists():
        raise HTTPException(status_code=404, detail="书籍知识未找到")

    try:
        kdata = json.loads(kfile.read_text(encoding="utf-8"))
        return kdata
    except Exception as e:
        logger.error(f"Failed to read rules for {book_name}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── POST /api/cross-validate ────────────────────────────────────

class CrossValidateRequest(BaseModel):
    symbol: str
    sources: Optional[list[str]] = None


@router.post("/cross-validate")
async def cross_validate(req: CrossValidateRequest):
    """用多个数据源拉取同一只股票，交叉对比数据"""
    import asyncio

    symbol = req.symbol.strip().upper()
    if not symbol:
        raise HTTPException(status_code=400, detail="请输入股票代码")

    sources = req.sources or ["yfinance", "yahoo_direct"]

    results = {}

    def _fetch_source(src: str):
        from src.data_providers.factory import get_data_provider
        from src.symbol_resolver import resolve_for_provider
        try:
            provider = get_data_provider(src)
            resolved = resolve_for_provider(symbol, provider.name)
            stock = provider.fetch(resolved)
            if stock is None:
                return src, {"status": "error", "error": "获取数据为空"}
            stock_dict = stock.to_dict() if hasattr(stock, 'to_dict') else {}
            # Remove internal/private fields
            stock_dict = {k: v for k, v in stock_dict.items()
                         if not k.startswith("_") and v is not None and v != "" and v != []}
            cov = stock.data_coverage() if hasattr(stock, 'data_coverage') else {}
            return src, {
                "status": "ok",
                "data": stock_dict,
                "data_quality": cov,
            }
        except Exception as e:
            return src, {"status": "error", "error": str(e)}

    loop = asyncio.get_running_loop()
    tasks = [loop.run_in_executor(None, _fetch_source, src) for src in sources]
    fetched = await asyncio.gather(*tasks, return_exceptions=True)

    for item in fetched:
        if isinstance(item, Exception):
            logger.error(f"Cross-validate error: {item}")
            continue
        src, result = item
        results[src] = result

    # Compute comparison
    comparison = _compute_comparison(results)

    return {
        "symbol": symbol,
        "sources": results,
        "comparison": comparison,
    }


def _compute_comparison(results: dict) -> dict:
    """对比多个数据源的数值差异"""
    key_fields = [
        ("price", "股价", "$"),
        ("pe", "PE (TTM)", ""),
        ("forward_pe", "Forward PE", ""),
        ("pb", "PB", ""),
        ("ps", "PS", ""),
        ("eps", "EPS", "$"),
        ("roe", "ROE", "%"),
        ("revenue", "营收", "$"),
        ("net_income", "净利润", "$"),
        ("current_ratio", "流动比率", ""),
        ("debt_to_equity", "负债权益比", ""),
        ("market_cap", "市值", "$"),
        ("dividend_yield", "股息率", "%"),
        ("book_value", "每股账面值", "$"),
        ("profit_margin", "净利润率", "%"),
        ("operating_margin", "营业利润率", "%"),
        ("free_cash_flow", "自由现金流", "$"),
    ]

    ok_sources = {k: v.get("data", {}) for k, v in results.items()
                  if v.get("status") in ("ok", "partial")}
    if len(ok_sources) < 2:
        return {"fields": [], "message": "需要至少 2 个数据源才能交叉对比"}

    fields = []
    source_names = list(ok_sources.keys())
    for field_key, field_name, unit in key_fields:
        values = {}
        for src in source_names:
            val = ok_sources[src].get(field_key)
            if val is not None:
                try:
                    values[src] = float(val)
                except (TypeError, ValueError):
                    values[src] = val

        if len(values) >= 2:
            numeric_vals = [v for v in values.values() if isinstance(v, (int, float))]
            deviation = ""
            if len(numeric_vals) >= 2 and numeric_vals[0] != 0:
                max_dev = max(abs(v - numeric_vals[0]) / abs(numeric_vals[0]) * 100
                             for v in numeric_vals[1:])
                deviation = f"{max_dev:.1f}%"

            fields.append({
                "field": field_key,
                "name": field_name,
                "unit": unit,
                "values": values,
                "deviation": deviation,
            })

    return {"fields": fields, "source_names": source_names}
