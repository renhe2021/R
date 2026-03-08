"""Old Charlie's financial analysis tools.

Eight async tool functions consumed by:
  - graph.py  (pipeline nodes)
  - llm.py    (OpenAI function-calling execute_tool)

All heavy lifting is done by reusing the existing src/ data providers
and the backend knowledge_base service.
"""

import json
import logging
import math
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

# ── Ensure the project root src/ package is importable ──
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # R/
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ════════════════════════════════════════════════════════════════
#  1. scan_fundamentals — Get comprehensive stock fundamentals
# ════════════════════════════════════════════════════════════════

async def scan_fundamentals(symbol: str) -> str:
    """Fetch 60+ fundamental indicators for a stock (BBG 优先, yfinance 兜底).

    Returns a formatted string suitable for LLM consumption.
    """
    import asyncio

    def _fetch():
        from src.symbol_resolver import resolve_for_provider
        from src.data_providers.factory import get_data_provider
        provider = get_data_provider("auto")
        resolved_symbol = resolve_for_provider(symbol, provider.name)
        logger.info(f"[scan_fundamentals] {symbol} → {provider.name}")
        stock = provider.fetch(resolved_symbol)
        return stock, resolved_symbol

    loop = asyncio.get_running_loop()
    stock, resolved_symbol = await asyncio.wait_for(loop.run_in_executor(None, _fetch), timeout=60)
    data = stock.to_dict()
    coverage = stock.data_coverage()

    display_symbol = resolved_symbol if resolved_symbol != symbol.upper() else symbol.upper()

    # Build human-readable summary
    lines = [
        f"=== {data.get('name', symbol)} ({display_symbol}) ===",
        f"行业: {data.get('sector', 'N/A')} / {data.get('industry', 'N/A')}",
        f"数据覆盖率: 核心 {coverage['core']['pct']}%, 扩展 {coverage['extended']['pct']}%, 历史 {coverage['historical']['pct']}%",
        "",
        "--- 估值 ---",
        f"股价: ${data.get('price', 0):.2f}  市值: ${data.get('market_cap', 0)/1e9:.1f}B",
        f"PE(TTM): {_fmt(data.get('pe'))}  Forward PE: {_fmt(data.get('forward_pe'))}",
        f"PB: {_fmt(data.get('pb'))}  PS: {_fmt(data.get('ps'))}",
        "",
        "--- 盈利 ---",
        f"ROE: {_pct(data.get('roe'))}  EPS: ${_fmt(data.get('eps'))}",
        f"净利润率: {_pct(data.get('profit_margin'))}  营业利润率: {_pct(data.get('operating_margin'))}",
        f"营收: ${_big(data.get('revenue'))}  净利润: ${_big(data.get('net_income'))}",
        "",
        "--- 财务健康 ---",
        f"流动比率: {_fmt(data.get('current_ratio'))}  负债权益比: {_fmt(data.get('debt_to_equity'))}",
        f"自由现金流: ${_big(data.get('free_cash_flow'))}",
        f"利息覆盖率: {_fmt(data.get('interest_coverage_ratio'))}",
        "",
        "--- 股息 ---",
        f"股息率: {_fmt(data.get('dividend_yield'))}%  连续分红: {data.get('consecutive_dividend_years', 'N/A')} 年",
        "",
        "--- Graham 估值 ---",
        f"Graham Number: ${_fmt(data.get('graham_number'))}",
        f"内在价值: ${_fmt(data.get('intrinsic_value'))}",
        f"安全边际: {_pct(data.get('margin_of_safety'))}",
        f"NCAV/股: ${_fmt(data.get('ncav_per_share'))}",
        "",
        "--- 技术指标 ---",
        f"RSI(14): {_fmt(data.get('rsi_14d'))}  MA(200): ${_fmt(data.get('ma_200d'))}",
        f"MACD: {_fmt(data.get('macd_line'))}  Signal: {_fmt(data.get('macd_signal'))}",
        "",
        "--- 历史数据 ---",
        f"10年平均EPS: ${_fmt(data.get('avg_eps_10y'))}  3年平均EPS: ${_fmt(data.get('avg_eps_3y'))}",
        f"EPS 10年CAGR: {_pct(data.get('earnings_growth_10y'))}",
        f"盈利年数: {data.get('profitable_years', 'N/A')}  最大EPS下降: {_pct(data.get('max_eps_decline'))}",
    ]
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════
#  2. scan_history — Get historical price data
# ════════════════════════════════════════════════════════════════

async def scan_history(symbol: str, days: int = 365) -> str:
    """Fetch historical price data and return a summary string (BBG 优先)."""
    import asyncio

    def _fetch():
        from datetime import datetime, timedelta
        from src.symbol_resolver import resolve_for_provider
        from src.data_providers.factory import get_data_provider

        # 历史价格：先尝试 BBG，失败则 yfinance
        try:
            from src.data_providers.bloomberg import BloombergProvider
            bp = BloombergProvider()
            if bp.is_available():
                resolved = resolve_for_provider(symbol, "bloomberg")
                end = datetime.now()
                start = end - timedelta(days=days)
                import blpapi  # noqa: F401
                # BBG 历史数据请求
                stock = bp.fetch(resolved)
                name = stock.name or resolved
                # 用 BBG 获取 OHLCV
                hist = bp.fetch_history(resolved, start, end) if hasattr(bp, 'fetch_history') else None
                if hist is not None and not hist.empty:
                    return hist, name, resolved
        except Exception as e:
            logger.debug(f"[scan_history] BBG 历史数据回退: {e}")

        # Fallback: yfinance
        import yfinance as yf
        resolved = resolve_for_provider(symbol, "yfinance")
        ticker = yf.Ticker(resolved)
        end = datetime.now()
        start = end - timedelta(days=days)
        hist = ticker.history(start=start.strftime("%Y-%m-%d"), end=end.strftime("%Y-%m-%d"))
        return hist, ticker.info.get("shortName", resolved), resolved

    loop = asyncio.get_running_loop()
    hist, name, resolved = await asyncio.wait_for(loop.run_in_executor(None, _fetch), timeout=60)

    if hist is None or hist.empty:
        return f"无法获取 {symbol} 的历史数据"

    close = hist["Close"]
    lines = [
        f"=== {name} ({symbol.upper()}) 过去 {days} 天价格历史 ===",
        f"数据点: {len(hist)} 天",
        f"最新价: ${close.iloc[-1]:.2f}",
        f"最高价: ${close.max():.2f} ({close.idxmax().strftime('%Y-%m-%d')})",
        f"最低价: ${close.min():.2f} ({close.idxmin().strftime('%Y-%m-%d')})",
        f"平均价: ${close.mean():.2f}",
        f"区间涨幅: {(close.iloc[-1] / close.iloc[0] - 1) * 100:.1f}%",
        "",
        "--- 最近10个交易日 ---",
    ]
    for date, row in hist.tail(10).iterrows():
        lines.append(
            f"  {date.strftime('%Y-%m-%d')}: "
            f"O=${row['Open']:.2f} H=${row['High']:.2f} "
            f"L=${row['Low']:.2f} C=${row['Close']:.2f} "
            f"V={row['Volume']/1e6:.1f}M"
        )
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════
#  3. detect_shenanigans — Financial fraud detection
# ════════════════════════════════════════════════════════════════

async def detect_shenanigans(symbol: str) -> Dict[str, Any]:
    """Run Beneish M-Score, Altman Z-Score, and Piotroski F-Score.

    Returns dict with: redFlags, riskLevel, mScore, zScore, fScore, summary.
    """
    import asyncio

    def _compute():
        from src.data_providers.factory import get_data_provider
        from src.symbol_resolver import resolve_for_provider
        import yfinance as yf

        provider = get_data_provider("auto")
        resolved = resolve_for_provider(symbol, provider.name)
        logger.info(f"[detect_shenanigans] {symbol} → {provider.name}")
        stock = provider.fetch(resolved)
        # M-Score 需要 yfinance 的 balance_sheet/income_stmt
        yf_resolved = resolve_for_provider(symbol, "yfinance")
        ticker = yf.Ticker(yf_resolved)

        red_flags: List[Dict[str, str]] = []

        # ── Piotroski F-Score (0-9, higher = better) ──
        f_score = 0
        f_reasons = []
        if stock.net_income and stock.net_income > 0:
            f_score += 1
            f_reasons.append("净利润为正 +1")
        if stock.roe and stock.roe > 0:
            f_score += 1
            f_reasons.append("ROE为正 +1")
        if stock.free_cash_flow and stock.free_cash_flow > 0:
            f_score += 1
            f_reasons.append("自由现金流为正 +1")
        if stock.free_cash_flow and stock.net_income and stock.free_cash_flow > stock.net_income:
            f_score += 1
            f_reasons.append("FCF > 净利润 (现金质量) +1")
        if stock.current_ratio and stock.current_ratio > 1.0:
            f_score += 1
            f_reasons.append("流动比率 > 1 +1")
        if stock.debt_to_equity is not None and stock.debt_to_equity < 0.5:
            f_score += 1
            f_reasons.append("负债权益比 < 0.5 +1")
        if stock.profit_margin and stock.profit_margin > 0:
            f_score += 1
            f_reasons.append("利润率为正 +1")
        if stock.revenue_growth_rate and stock.revenue_growth_rate > 0:
            f_score += 1
            f_reasons.append("营收正增长 +1")
        if stock.roe and stock.roe > 0.1:
            f_score += 1
            f_reasons.append("ROE > 10% +1")

        if f_score <= 3:
            red_flags.append({
                "name": "低 Piotroski F-Score",
                "category": "financial_strength",
                "severity": "HIGH",
                "detail": f"F-Score = {f_score}/9，财务实力严重不足",
            })

        # ── Altman Z-Score (>2.99 安全, 1.81-2.99 灰色, <1.81 危险) ──
        z_score = None
        if (stock.total_assets and stock.total_assets > 0 and
                stock.working_capital is not None and stock.market_cap and
                stock.revenue and stock.total_liabilities is not None):
            ta = stock.total_assets
            wc = stock.working_capital or 0
            re_val = (stock.net_income or 0) * 0.7  # rough retained earnings
            ebit = stock.ebit or (stock.net_income or 0)
            mv_equity = stock.market_cap
            tl = stock.total_liabilities or 0
            sales = stock.revenue

            a = 1.2 * (wc / ta)
            b = 1.4 * (re_val / ta)
            c = 3.3 * (ebit / ta) if ebit else 0
            d = 0.6 * (mv_equity / tl) if tl > 0 else 3.0
            e = 1.0 * (sales / ta)
            z_score = round(a + b + c + d + e, 2)

            if z_score < 1.81:
                red_flags.append({
                    "name": "低 Altman Z-Score",
                    "category": "bankruptcy_risk",
                    "severity": "CRITICAL",
                    "detail": f"Z-Score = {z_score}，处于破产危险区 (<1.81)",
                })
            elif z_score < 2.99:
                red_flags.append({
                    "name": "Altman Z-Score 灰色区",
                    "category": "bankruptcy_risk",
                    "severity": "MEDIUM",
                    "detail": f"Z-Score = {z_score}，处于灰色区 (1.81-2.99)",
                })

        # ── Beneish M-Score (> -1.78 可能操纵盈利) ──
        m_score = None
        try:
            bs = ticker.balance_sheet
            inc = ticker.income_stmt
            if bs is not None and not bs.empty and inc is not None and not inc.empty and bs.shape[1] >= 2:
                rev_cur = _safe_val(inc, "Total Revenue", 0)
                rev_prev = _safe_val(inc, "Total Revenue", 1)
                rec_cur = _safe_val(bs, "Receivables", 0) or _safe_val(bs, "Net Receivables", 0) or 0
                rec_prev = _safe_val(bs, "Receivables", 1) or _safe_val(bs, "Net Receivables", 1) or 0
                ta_cur = _safe_val(bs, "Total Assets", 0)
                ta_prev = _safe_val(bs, "Total Assets", 1)
                dep_cur = _safe_val(inc, "Depreciation And Amortization", 0) or 0
                dep_prev = _safe_val(inc, "Depreciation And Amortization", 1) or 0
                sga_cur = _safe_val(inc, "Selling General And Administration", 0) or 0
                sga_prev = _safe_val(inc, "Selling General And Administration", 1) or 0
                ni_cur = _safe_val(inc, "Net Income", 0) or 0
                cfo = stock.free_cash_flow or 0

                if rev_cur and rev_prev and rev_prev > 0 and ta_cur and ta_prev and ta_prev > 0:
                    dsri = (rec_cur / rev_cur) / (rec_prev / rev_prev) if rec_prev > 0 and rev_cur > 0 else 1.0
                    gmi = ((rev_prev - (stock.net_income or 0)) / rev_prev) / \
                          ((rev_cur - ni_cur) / rev_cur) if rev_cur > 0 else 1.0
                    aqi = (1 - ((_safe_val(bs, "Current Assets", 0) or 0) +
                                (_safe_val(bs, "Net Fixed Assets", 0) or _safe_val(bs, "Property Plant Equipment", 0) or 0)) / ta_cur) / \
                          (1 - ((_safe_val(bs, "Current Assets", 1) or 0) +
                                (_safe_val(bs, "Net Fixed Assets", 1) or _safe_val(bs, "Property Plant Equipment", 1) or 0)) / ta_prev) \
                        if ta_cur > 0 and ta_prev > 0 else 1.0
                    sgi = rev_cur / rev_prev if rev_prev > 0 else 1.0
                    sgai = (sga_cur / rev_cur) / (sga_prev / rev_prev) if sga_prev > 0 and rev_cur > 0 and rev_prev > 0 else 1.0
                    lvgi = ((stock.total_liabilities or 0) / ta_cur) / \
                           ((_safe_val(bs, "Total Liabilities Net Minority Interest", 1) or 0) / ta_prev) \
                        if ta_cur > 0 and ta_prev > 0 else 1.0
                    tata = (ni_cur - cfo) / ta_cur if ta_cur > 0 else 0

                    m_score = round(
                        -4.84 + 0.920 * dsri + 0.528 * gmi + 0.404 * aqi +
                        0.892 * sgi + 0.115 * 1.0 - 0.172 * sgai +
                        4.679 * tata - 0.327 * lvgi,
                        2,
                    )

                    if m_score > -1.78:
                        red_flags.append({
                            "name": "高 Beneish M-Score",
                            "category": "earnings_manipulation",
                            "severity": "HIGH",
                            "detail": f"M-Score = {m_score}，超过 -1.78 警戒线，可能存在盈利操纵",
                        })
        except Exception as e:
            logger.warning(f"M-Score calculation failed for {symbol}: {e}")

        # ── Additional red flags ──
        if stock.debt_to_equity and stock.debt_to_equity > 2.0:
            red_flags.append({
                "name": "高负债率",
                "category": "leverage",
                "severity": "HIGH",
                "detail": f"负债权益比 = {stock.debt_to_equity:.2f}，远超安全线",
            })
        if stock.pe and stock.pe > 50:
            red_flags.append({
                "name": "极高估值",
                "category": "overvaluation",
                "severity": "MEDIUM",
                "detail": f"PE = {stock.pe:.1f}，估值偏高",
            })
        if stock.free_cash_flow and stock.net_income and stock.free_cash_flow < 0 < stock.net_income:
            red_flags.append({
                "name": "现金流与利润背离",
                "category": "cash_quality",
                "severity": "HIGH",
                "detail": "净利润为正但自由现金流为负，盈利质量堪忧",
            })

        # ── Determine risk level ──
        critical_count = sum(1 for f in red_flags if f["severity"] == "CRITICAL")
        high_count = sum(1 for f in red_flags if f["severity"] == "HIGH")

        if critical_count > 0:
            risk_level = "CRITICAL"
        elif high_count >= 3:
            risk_level = "HIGH"
        elif high_count >= 1 or len(red_flags) >= 3:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        summary_parts = [f"Z-Score: {z_score or 'N/A'}", f"F-Score: {f_score}/9", f"M-Score: {m_score or 'N/A'}"]
        summary = f"风险等级: {risk_level}, {', '.join(summary_parts)}, {len(red_flags)} 个红旗"

        return {
            "symbol": symbol.upper(),
            "redFlags": red_flags,
            "riskLevel": risk_level,
            "zScore": z_score,
            "fScore": f_score,
            "mScore": m_score,
            "fReasons": f_reasons,
            "summary": summary,
        }

    import asyncio
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _compute)


# ════════════════════════════════════════════════════════════════
#  4. run_full_valuation — 7 valuation models
# ════════════════════════════════════════════════════════════════

async def run_full_valuation(symbol: str) -> Dict[str, Any]:
    """Run 7 valuation models and return consolidated results."""
    import asyncio

    def _compute():
        from src.data_providers.factory import get_data_provider
        from src.symbol_resolver import resolve_for_provider
        provider = get_data_provider("auto")
        resolved = resolve_for_provider(symbol, provider.name)
        logger.info(f"[run_full_valuation] {symbol} → {provider.name}")
        stock = provider.fetch(resolved)

        valuations = {}
        errors = []

        # 1. Graham Number
        if stock.eps and stock.eps > 0 and stock.book_value and stock.book_value > 0:
            gn = math.sqrt(22.5 * stock.eps * stock.book_value)
            valuations["grahamNumber"] = round(gn, 2)

        # 2. Graham Intrinsic Value V = EPS * (8.5 + 2g) * 4.4/Y
        if stock.eps and stock.eps > 0:
            g = (stock.eps_growth_5y or stock.earnings_growth_10y or 0.05) * 100
            y = stock.aa_bond_yield or stock.treasury_yield_10y or 4.4
            if y > 0:
                iv = stock.eps * (8.5 + 2 * g) * 4.4 / y
                valuations["grahamIntrinsicValue"] = round(iv, 2)

        # 3. EPV (Earnings Power Value)
        if stock.ebit and stock.ebit > 0 and stock.total_debt is not None:
            wacc = 0.10  # assume 10% WACC
            tax_rate = 0.21
            epv = (stock.ebit * (1 - tax_rate)) / wacc
            if stock.shares_outstanding and stock.shares_outstanding > 0:
                epv_per_share = (epv - (stock.total_debt or 0) + (stock.total_cash or 0)) / stock.shares_outstanding
                valuations["epv"] = round(epv_per_share, 2)

        # 4. DCF (simplified 2-stage)
        if stock.free_cash_flow and stock.free_cash_flow > 0 and stock.shares_outstanding:
            fcf = stock.free_cash_flow
            g1 = min(stock.revenue_growth_rate / 100 if stock.revenue_growth_rate else 0.08, 0.20)
            g2 = 0.03  # terminal growth
            wacc = 0.10
            # Stage 1: 5 years
            pv_stage1 = sum(fcf * (1 + g1) ** y / (1 + wacc) ** y for y in range(1, 6))
            # Stage 2: terminal value
            terminal_fcf = fcf * (1 + g1) ** 5 * (1 + g2)
            terminal_value = terminal_fcf / (wacc - g2)
            pv_terminal = terminal_value / (1 + wacc) ** 5
            total_value = pv_stage1 + pv_terminal
            dcf_per_share = total_value / stock.shares_outstanding
            valuations["dcfValue"] = round(dcf_per_share, 2)

        # 5. DDM (Dividend Discount Model)
        if stock.dividend_per_share and stock.dividend_per_share > 0:
            g = (stock.earnings_growth_10y or 0.03)
            r = 0.10
            if r > g:
                ddm = stock.dividend_per_share * (1 + g) / (r - g)
                valuations["ddmValue"] = round(ddm, 2)

        # 6. Net-Net (NCAV)
        if stock.ncav_per_share is not None:
            valuations["netNetValue"] = round(stock.ncav_per_share, 2)

        # 7. Owner Earnings (Buffett)
        if stock.net_income and stock.capex:
            owner_earnings = stock.net_income - abs(stock.capex)
            if stock.shares_outstanding and stock.shares_outstanding > 0:
                oe_per_share = owner_earnings / stock.shares_outstanding
                oe_value = oe_per_share / 0.10  # capitalize at 10%
                valuations["ownerEarningsValue"] = round(oe_value, 2)

        # ── Consensus intrinsic value ──
        valid_vals = [v for v in valuations.values() if isinstance(v, (int, float)) and v > 0]
        intrinsic_value = None
        if valid_vals:
            valid_vals.sort()
            mid = len(valid_vals) // 2
            intrinsic_value = valid_vals[mid]  # median

        # ── Margin of safety ──
        margin_of_safety = None
        if intrinsic_value and stock.price and stock.price > 0:
            margin_of_safety = (intrinsic_value - stock.price) / intrinsic_value

        # ── Quality assessment ──
        quality = {}
        if stock.roe and stock.roe > 0.15 and stock.profit_margin and stock.profit_margin > 0.1:
            quality["moatType"] = "Wide"
        elif stock.roe and stock.roe > 0.10:
            quality["moatType"] = "Narrow"
        else:
            quality["moatType"] = "None"

        return {
            "symbol": symbol.upper(),
            "price": stock.price,
            "valuations": valuations,
            "intrinsicValue": intrinsic_value,
            "marginOfSafety": round(margin_of_safety, 4) if margin_of_safety else None,
            "quality": quality,
            "grahamNumber": valuations.get("grahamNumber"),
            "epv": valuations.get("epv"),
            "dcfValue": valuations.get("dcfValue"),
            "margin_of_safety": round(margin_of_safety, 4) if margin_of_safety else None,
        }

    import asyncio
    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(loop.run_in_executor(None, _compute), timeout=60)


# ════════════════════════════════════════════════════════════════
#  5. search_knowledge — Semantic search across knowledge base
# ════════════════════════════════════════════════════════════════

async def search_knowledge(query: str) -> str:
    """Search the ChromaDB knowledge base (theories + book full-text).

    Returns a formatted string with the most relevant results.
    """
    from app.config import get_settings
    from app.services.knowledge_base import KnowledgeBaseService

    settings = get_settings()
    kb = KnowledgeBaseService(chroma_persist_dir=settings.chroma_persist_dir)

    try:
        results = await kb.search_knowledge(query, top_k=5)
    except Exception as e:
        logger.warning(f"Knowledge search failed: {e}")
        results = []

    if not results:
        return f"未找到与「{query}」相关的知识库内容。"

    lines = [f"=== 知识库搜索: {query} ===\n"]
    for i, r in enumerate(results, 1):
        source = r.get("source", r.get("metadata", {}).get("source", ""))
        score = r.get("score", 0)
        content = r.get("content", "")[:600]
        lines.append(f"[{i}] 相似度: {score:.3f} | 来源: {source}")
        lines.append(content)
        lines.append("")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════
#  6. search_book_library — Search investment book full-text
# ════════════════════════════════════════════════════════════════

async def search_book_library(query: str) -> str:
    """Search the full-text indexed investment books via BookIndexer."""
    from app.config import get_settings
    from app.services.book_indexer import BookIndexer

    settings = get_settings()
    indexer = BookIndexer(chroma_persist_dir=settings.chroma_persist_dir)

    try:
        results = indexer.search(query, top_k=5, layer="paragraph")
    except Exception as e:
        logger.warning(f"Book library search failed: {e}")
        results = []

    if not results:
        return f"未在书籍库中找到与「{query}」相关的内容。"

    lines = [f"=== 投资书籍搜索: {query} ===\n"]
    for i, r in enumerate(results, 1):
        meta = r.get("metadata", {})
        book = meta.get("book_title", "")
        chapter = meta.get("chapter_title", "")
        score = r.get("score", 0)
        content = r.get("content", "")[:500]
        lines.append(f"[{i}] {book} — {chapter} (相似度: {score:.3f})")
        lines.append(content)
        lines.append("")
    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════
#  7. analyze_news — News fetch + sentiment
# ════════════════════════════════════════════════════════════════

async def analyze_news(query: str, limit: int = 5) -> str:
    """Fetch recent news for a stock/topic using yfinance.

    Returns a JSON string with news items and basic sentiment.
    """
    import asyncio

    def _fetch():
        import yfinance as yf

        ticker = yf.Ticker(query.upper())
        news = getattr(ticker, "news", None) or []

        items = []
        for n in news[:limit]:
            title = n.get("title", "")
            publisher = n.get("publisher", "")
            link = n.get("link", "")
            pub_date = n.get("providerPublishTime", "")

            # Simple keyword sentiment
            positive_words = {"beat", "surge", "rise", "gain", "profit", "growth", "upgrade", "record", "bullish"}
            negative_words = {"miss", "fall", "drop", "loss", "decline", "downgrade", "crash", "bearish", "cut"}
            title_lower = title.lower()
            pos = sum(1 for w in positive_words if w in title_lower)
            neg = sum(1 for w in negative_words if w in title_lower)
            if pos > neg:
                sentiment = "positive"
            elif neg > pos:
                sentiment = "negative"
            else:
                sentiment = "neutral"

            items.append({
                "title": title,
                "publisher": publisher,
                "sentiment": sentiment,
                "link": link,
            })

        return {
            "query": query.upper(),
            "count": len(items),
            "articles": items,
            "overallSentiment": _overall_sentiment(items),
        }

    loop = asyncio.get_running_loop()
    result = await asyncio.wait_for(loop.run_in_executor(None, _fetch), timeout=30)
    return json.dumps(result, ensure_ascii=False)


# ════════════════════════════════════════════════════════════════
#  8. research_realtime — Perplexity AI research (graceful fallback)
# ════════════════════════════════════════════════════════════════

async def research_realtime(query: str) -> str:
    """Use Perplexity AI for real-time research.

    If Perplexity API key is not configured, falls back to a web search prompt.
    """
    from app.config import get_settings

    settings = get_settings()
    if not settings.perplexity_api_key:
        return (
            f"Perplexity API 未配置。\n"
            f"请在 backend/.env 中设置 PERPLEXITY_API_KEY。\n"
            f"查询内容: {query}\n\n"
            f"建议手动搜索以获取最新信息。"
        )

    try:
        import httpx

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.perplexity.ai/chat/completions",
                headers={
                    "Authorization": f"Bearer {settings.perplexity_api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "llama-3.1-sonar-large-128k-online",
                    "messages": [
                        {"role": "system", "content": "You are a financial research assistant. Provide accurate, well-sourced answers."},
                        {"role": "user", "content": query},
                    ],
                    "max_tokens": 2000,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            content = data["choices"][0]["message"]["content"]
            return f"=== Perplexity 研究: {query} ===\n\n{content}"
    except Exception as e:
        logger.error(f"Perplexity research failed: {e}")
        return f"Perplexity 研究失败: {str(e)[:200]}\n查询: {query}"


# ════════════════════════════════════════════════════════════════
#  9. evaluate_stock_rules — Multi-school value investing evaluation
# ════════════════════════════════════════════════════════════════

async def evaluate_stock_rules(symbol: str, school: str = "all") -> str:
    """Evaluate a stock against the distilled investment rules from 14 books.

    7 schools: graham, buffett, quantitative, quality, valuation, contrarian, garp
    65+ rules covering valuation, profitability, quality, growth, financial health.

    Returns formatted multi-school evaluation with scores and recommendations.
    """
    import asyncio

    def _evaluate():
        from src.data_providers.factory import get_data_provider
        from src.symbol_resolver import resolve_for_provider
        from app.agent.distilled_rules import (
            evaluate_stock_all_schools, evaluate_stock_against_school, SCHOOLS,
        )

        provider = get_data_provider("auto")
        resolved = resolve_for_provider(symbol, provider.name)
        logger.info(f"[evaluate_stock_rules] {symbol} → {provider.name}")
        stock = provider.fetch(resolved)
        data = stock.to_dict()

        if school == "all":
            results = evaluate_stock_all_schools(data)
        else:
            if school not in SCHOOLS:
                return f"未知流派: {school}。可选: {', '.join(SCHOOLS.keys())}"
            results = {"schools": {school: evaluate_stock_against_school(data, school)}}

        return _format_school_evaluation(symbol, data, results)

    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(loop.run_in_executor(None, _evaluate), timeout=60)


def _format_school_evaluation(symbol: str, data: dict, results: dict) -> str:
    """Format multi-school evaluation into readable text."""
    lines = [f"═══ {symbol.upper()} 七流派投资评估 ═══\n"]

    # Stock basics
    price = data.get("price", 0)
    pe = data.get("pe")
    roe = data.get("roe")
    lines.append(f"股价: ${price:.2f}  PE: {pe or 'N/A'}  ROE: {f'{roe*100:.1f}%' if roe else 'N/A'}\n")

    # Best fit
    best = results.get("best_fit_school")
    if best:
        from app.agent.distilled_rules import SCHOOLS
        school_obj = SCHOOLS.get(best)
        lines.append(f"⭐ 最佳适配流派: {school_obj.name_cn if school_obj else best}\n")

    strong = results.get("strong_pass_schools", [])
    if strong:
        from app.agent.distilled_rules import SCHOOLS as S
        names = [S[s].name_cn for s in strong if s in S]
        lines.append(f"🏆 优秀评级流派: {', '.join(names)}\n")

    # Per-school details
    for school_name, eval_data in results.get("schools", {}).items():
        verdict = eval_data.get("verdict_cn", "")
        rec = eval_data.get("recommendation", "")
        score = eval_data.get("score", 0)
        max_score = eval_data.get("max_score", 0)
        pass_rate = eval_data.get("pass_rate", 0)
        school_cn = eval_data.get("school_cn", school_name)

        emoji = {"STRONG_PASS": "🟢", "PASS": "🟡", "MARGINAL": "🟠", "FAIL": "🔴", "REJECT": "⛔"}.get(rec, "⚪")
        lines.append(f"\n{emoji} 【{school_cn}】 {verdict} ({score}/{max_score}, 通过率 {pass_rate:.0%})")

        # Show passed rules
        for p in eval_data.get("passed", []):
            lines.append(f"  ✓ {p['rule']}")

        # Show failed rules
        for f in eval_data.get("failed", []):
            marker = "⚡✗" if f.get("is_eliminatory") else "  ✗"
            lines.append(f"  {marker} {f['rule']}: {f['description']}")

        # Show skipped
        skipped = eval_data.get("skipped", [])
        if skipped:
            lines.append(f"  ⊘ 跳过 {len(skipped)} 条规则 (缺少数据)")

    # Overall
    overall_score = results.get("overall_score")
    overall_max = results.get("overall_max")
    if overall_score is not None:
        lines.append(f"\n══ 总分: {overall_score}/{overall_max} ══")

    return "\n".join(lines)


# ════════════════════════════════════════════════════════════════
#  Helpers
# ════════════════════════════════════════════════════════════════

def _fmt(val, digits=2) -> str:
    if val is None:
        return "N/A"
    return f"{val:.{digits}f}"


def _pct(val) -> str:
    if val is None:
        return "N/A"
    return f"{val * 100:.1f}%"


def _big(val) -> str:
    if val is None:
        return "N/A"
    abs_val = abs(val)
    sign = "-" if val < 0 else ""
    if abs_val >= 1e12:
        return f"{sign}{abs_val/1e12:.1f}T"
    if abs_val >= 1e9:
        return f"{sign}{abs_val/1e9:.1f}B"
    if abs_val >= 1e6:
        return f"{sign}{abs_val/1e6:.1f}M"
    return f"{sign}{abs_val:,.0f}"


def _overall_sentiment(articles: list) -> str:
    if not articles:
        return "unknown"
    sentiments = [a["sentiment"] for a in articles]
    pos = sentiments.count("positive")
    neg = sentiments.count("negative")
    if pos > neg:
        return "positive"
    elif neg > pos:
        return "negative"
    return "neutral"


def _safe_val(df, field: str, col_idx: int = 0):
    """Safely extract a value from a pandas DataFrame."""
    try:
        if field in df.index and col_idx < df.shape[1]:
            val = df.loc[field].iloc[col_idx]
            if val is not None and not (isinstance(val, float) and math.isnan(val)):
                return float(val)
    except Exception:
        pass
    return None
