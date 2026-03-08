"""SEC Filing Fetcher — Download and parse 10-K/10-Q/Annual Reports.

Uses SEC EDGAR API (free, no key required) + yfinance earnings data.
Extracts key financial statements and management discussion sections
for LLM-powered deep analysis.

For non-US stocks (HK/CN), falls back to yfinance financial statements.
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger(__name__)

SEC_HEADERS = {
    "User-Agent": "R-System-ValueInvesting research@rsystem.dev",
    "Accept-Encoding": "gzip, deflate",
}
SEC_BASE = "https://efts.sec.gov/LATEST"
EDGAR_FILINGS = "https://data.sec.gov/submissions"


async def fetch_sec_filings(symbol: str, filing_type: str = "10-K", count: int = 1) -> List[Dict[str, Any]]:
    """Fetch SEC filings metadata for a US stock.

    Args:
        symbol: Stock ticker (e.g., AAPL)
        filing_type: "10-K" (annual) or "10-Q" (quarterly)
        count: Number of recent filings to fetch

    Returns:
        List of filing metadata dicts with urls
    """
    try:
        async with httpx.AsyncClient(timeout=15, headers=SEC_HEADERS) as client:
            # Step 1: Get CIK from ticker
            resp = await client.get(f"https://efts.sec.gov/LATEST/search-index?q=%22{symbol}%22&dateRange=custom&startdt=2024-01-01&forms={filing_type}")
            if resp.status_code != 200:
                # Try company tickers mapping
                resp2 = await client.get("https://www.sec.gov/files/company_tickers.json")
                if resp2.status_code == 200:
                    tickers = resp2.json()
                    cik = None
                    for entry in tickers.values():
                        if entry.get("ticker", "").upper() == symbol.upper():
                            cik = str(entry["cik_str"]).zfill(10)
                            break
                    if not cik:
                        return []
                else:
                    return []
            else:
                cik = None

            # Step 2: Get filings from EDGAR
            if not cik:
                # Use full-text search API
                search_resp = await client.get(
                    f"{SEC_BASE}/search-index",
                    params={
                        "q": f'"{symbol}"',
                        "forms": filing_type,
                        "dateRange": "custom",
                        "startdt": "2023-01-01",
                    },
                )
                if search_resp.status_code == 200:
                    data = search_resp.json()
                    hits = data.get("hits", {}).get("hits", [])
                    if hits:
                        cik = str(hits[0].get("_source", {}).get("entity_id", "")).zfill(10)

            if not cik:
                # Last resort: company tickers
                resp3 = await client.get("https://www.sec.gov/files/company_tickers.json")
                if resp3.status_code == 200:
                    for entry in resp3.json().values():
                        if entry.get("ticker", "").upper() == symbol.upper():
                            cik = str(entry["cik_str"]).zfill(10)
                            break

            if not cik:
                logger.warning(f"[FilingFetcher] Cannot find CIK for {symbol}")
                return []

            # Step 3: Fetch submission history
            sub_resp = await client.get(f"{EDGAR_FILINGS}/CIK{cik}.json")
            if sub_resp.status_code != 200:
                return []

            sub_data = sub_resp.json()
            recent = sub_data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            dates = recent.get("filingDate", [])
            accessions = recent.get("accessionNumber", [])
            primary_docs = recent.get("primaryDocument", [])
            descriptions = recent.get("primaryDocDescription", [])

            filings = []
            for i, form in enumerate(forms):
                if form == filing_type and len(filings) < count:
                    acc_clean = accessions[i].replace("-", "")
                    doc_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc_clean}/{primary_docs[i]}"
                    filings.append({
                        "form": form,
                        "filingDate": dates[i],
                        "accessionNumber": accessions[i],
                        "primaryDocument": primary_docs[i],
                        "description": descriptions[i] if i < len(descriptions) else "",
                        "url": doc_url,
                        "cik": cik,
                    })

            return filings

    except Exception as e:
        logger.error(f"[FilingFetcher] SEC API error for {symbol}: {e}")
        return []


async def fetch_filing_text(url: str, max_chars: int = 30000) -> str:
    """Download and extract text from a SEC filing document.

    Handles HTML filings by stripping tags and extracting key sections.
    """
    try:
        async with httpx.AsyncClient(timeout=30, headers=SEC_HEADERS, follow_redirects=True) as client:
            resp = await client.get(url)
            if resp.status_code != 200:
                return ""

            content = resp.text
            content_type = resp.headers.get("content-type", "")

            if "html" in content_type or content.strip().startswith("<"):
                text = _strip_html(content)
            else:
                text = content

            # Truncate if too long
            if len(text) > max_chars:
                text = text[:max_chars] + f"\n\n[... truncated at {max_chars:,} chars ...]"

            return text.strip()

    except Exception as e:
        logger.error(f"[FilingFetcher] Failed to fetch filing: {e}")
        return ""


def _strip_html(html: str) -> str:
    """Strip HTML tags and clean up SEC filing text."""
    # Remove script/style blocks
    html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML comments
    html = re.sub(r"<!--.*?-->", "", html, flags=re.DOTALL)
    # Remove XBRL tags
    html = re.sub(r"<[^>]*ix:[^>]*>", "", html)
    # Replace br/p with newlines
    html = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</p>|</div>|</tr>|</li>", "\n", html, flags=re.IGNORECASE)
    html = re.sub(r"</td>", "\t", html, flags=re.IGNORECASE)
    # Remove remaining tags
    html = re.sub(r"<[^>]+>", "", html)
    # Clean up entities
    html = html.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    html = html.replace("&quot;", '"').replace("&#8217;", "'").replace("&#8220;", '"').replace("&#8221;", '"')
    # Remove excessive whitespace
    html = re.sub(r"[ \t]+", " ", html)
    html = re.sub(r"\n{3,}", "\n\n", html)
    return html.strip()


def _extract_key_sections(text: str) -> Dict[str, str]:
    """Extract key sections from a 10-K/10-Q filing text.

    Looks for: MD&A, Risk Factors, Financial Statements, Business Overview.
    """
    sections = {}
    section_markers = {
        "business": [
            r"(?:ITEM\s*1\.?\s*(?:—|-)?\s*BUSINESS)",
            r"(?:Part\s+I\s*,?\s*Item\s*1)",
        ],
        "risk_factors": [
            r"(?:ITEM\s*1A\.?\s*(?:—|-)?\s*RISK\s*FACTORS)",
        ],
        "mda": [
            r"(?:ITEM\s*7\.?\s*(?:—|-)?\s*MANAGEMENT)",
            r"(?:MANAGEMENT'?S?\s*DISCUSSION\s*AND\s*ANALYSIS)",
        ],
        "financial_statements": [
            r"(?:ITEM\s*8\.?\s*(?:—|-)?\s*FINANCIAL\s*STATEMENTS)",
            r"(?:CONSOLIDATED\s*(?:BALANCE\s*SHEETS?|STATEMENTS?\s*OF\s*(?:INCOME|OPERATIONS)))",
        ],
    }

    text_upper = text.upper()
    for section_name, patterns in section_markers.items():
        for pattern in patterns:
            match = re.search(pattern, text_upper)
            if match:
                start = match.start()
                # Find the end (next ITEM or end of text)
                end_match = re.search(r"ITEM\s*\d", text_upper[start + len(match.group()):])
                end = start + len(match.group()) + end_match.start() if end_match else min(start + 15000, len(text))
                sections[section_name] = text[start:end].strip()[:8000]
                break

    return sections


async def get_financial_statements_yfinance(symbol: str) -> Dict[str, Any]:
    """Get financial statements from yfinance as fallback for non-US stocks.

    Returns income statement, balance sheet, cash flow statement data.
    """
    def _fetch():
        import yfinance as yf
        ticker = yf.Ticker(symbol)

        result = {
            "symbol": symbol,
            "source": "yfinance",
            "timestamp": datetime.now().isoformat(),
        }

        # Income Statement
        try:
            inc = ticker.income_stmt
            if inc is not None and not inc.empty:
                # Get latest 2 years
                cols = min(2, inc.shape[1])
                inc_data = {}
                for row in inc.index:
                    vals = []
                    for c in range(cols):
                        v = inc.iloc[inc.index.get_loc(row), c]
                        vals.append(float(v) if v is not None and str(v) != 'nan' else None)
                    inc_data[str(row)] = vals
                result["income_statement"] = inc_data
                result["income_periods"] = [str(c.date()) for c in inc.columns[:cols]]
        except Exception as e:
            logger.debug(f"Income stmt failed for {symbol}: {e}")

        # Balance Sheet
        try:
            bs = ticker.balance_sheet
            if bs is not None and not bs.empty:
                cols = min(2, bs.shape[1])
                bs_data = {}
                for row in bs.index:
                    vals = []
                    for c in range(cols):
                        v = bs.iloc[bs.index.get_loc(row), c]
                        vals.append(float(v) if v is not None and str(v) != 'nan' else None)
                    bs_data[str(row)] = vals
                result["balance_sheet"] = bs_data
                result["balance_periods"] = [str(c.date()) for c in bs.columns[:cols]]
        except Exception as e:
            logger.debug(f"Balance sheet failed for {symbol}: {e}")

        # Cash Flow
        try:
            cf = ticker.cashflow
            if cf is not None and not cf.empty:
                cols = min(2, cf.shape[1])
                cf_data = {}
                for row in cf.index:
                    vals = []
                    for c in range(cols):
                        v = cf.iloc[cf.index.get_loc(row), c]
                        vals.append(float(v) if v is not None and str(v) != 'nan' else None)
                    cf_data[str(row)] = vals
                result["cash_flow"] = cf_data
                result["cashflow_periods"] = [str(c.date()) for c in cf.columns[:cols]]
        except Exception as e:
            logger.debug(f"Cash flow failed for {symbol}: {e}")

        # Earnings history
        try:
            earnings = ticker.earnings_history
            if earnings is not None and not earnings.empty:
                result["earnings_history"] = earnings.to_dict(orient="records")[:8]
        except Exception:
            pass

        return result

    loop = asyncio.get_running_loop()
    return await asyncio.wait_for(loop.run_in_executor(None, _fetch), timeout=60)


async def fetch_comprehensive_financials(symbol: str) -> Dict[str, Any]:
    """Fetch comprehensive financial data: SEC filings + yfinance statements.

    For US stocks: tries SEC EDGAR 10-K first, then yfinance.
    For non-US stocks: yfinance only.

    Returns a structured dict with:
    - filing_metadata: SEC filing info (if available)
    - key_sections: Extracted MD&A, risk factors, etc. (if available)
    - financial_statements: Income/Balance/CashFlow data
    - summary: A formatted text summary for LLM consumption
    """
    is_us = not any(marker in symbol.upper() for marker in [".HK", ".SS", ".SZ", ".T", ".L", ".DE"])

    result = {
        "symbol": symbol,
        "timestamp": datetime.now().isoformat(),
        "has_sec_filing": False,
        "has_financial_statements": False,
    }

    # Parallel fetch: SEC filing + yfinance statements
    tasks = [get_financial_statements_yfinance(symbol)]
    if is_us:
        tasks.append(fetch_sec_filings(symbol, "10-K", count=1))

    fetched = await asyncio.gather(*tasks, return_exceptions=True)

    # yfinance statements
    yf_data = fetched[0] if not isinstance(fetched[0], Exception) else {}
    if yf_data and (yf_data.get("income_statement") or yf_data.get("balance_sheet")):
        result["financial_statements"] = yf_data
        result["has_financial_statements"] = True

    # SEC filing
    if is_us and len(fetched) > 1 and not isinstance(fetched[1], Exception):
        sec_filings = fetched[1]
        if sec_filings:
            filing = sec_filings[0]
            result["filing_metadata"] = filing
            result["has_sec_filing"] = True

            # Try to download and extract key sections
            try:
                filing_text = await fetch_filing_text(filing["url"], max_chars=50000)
                if filing_text:
                    sections = _extract_key_sections(filing_text)
                    result["key_sections"] = sections
            except Exception as e:
                logger.warning(f"[FilingFetcher] Section extraction failed: {e}")

    # Build summary for LLM
    result["summary"] = _build_financial_summary(symbol, result)

    return result


def _build_financial_summary(symbol: str, data: Dict[str, Any]) -> str:
    """Build a formatted financial summary text for LLM analysis."""
    lines = [f"=== {symbol} 最新财报数据 ===\n"]

    # Filing info
    if data.get("has_sec_filing"):
        meta = data.get("filing_metadata", {})
        lines.append(f"SEC 文件: {meta.get('form', 'N/A')} | 提交日期: {meta.get('filingDate', 'N/A')}")
        lines.append(f"链接: {meta.get('url', 'N/A')}\n")

    fs = data.get("financial_statements", {})

    # Income Statement
    inc = fs.get("income_statement", {})
    if inc:
        periods = fs.get("income_periods", ["Latest", "Prior"])
        lines.append(f"--- 利润表 ({' / '.join(periods[:2])}) ---")
        key_items = [
            "Total Revenue", "Cost Of Revenue", "Gross Profit",
            "Operating Income", "Net Income", "EBITDA",
            "Basic EPS", "Diluted EPS",
        ]
        for item in key_items:
            if item in inc:
                vals = inc[item]
                formatted = " / ".join(_fmt_big(v) for v in vals[:2])
                lines.append(f"  {item}: {formatted}")
        lines.append("")

    # Balance Sheet
    bs = fs.get("balance_sheet", {})
    if bs:
        periods = fs.get("balance_periods", ["Latest", "Prior"])
        lines.append(f"--- 资产负债表 ({' / '.join(periods[:2])}) ---")
        key_items = [
            "Total Assets", "Total Liabilities Net Minority Interest",
            "Stockholders Equity", "Total Debt", "Cash And Cash Equivalents",
            "Current Assets", "Current Liabilities", "Working Capital",
        ]
        for item in key_items:
            if item in bs:
                vals = bs[item]
                formatted = " / ".join(_fmt_big(v) for v in vals[:2])
                lines.append(f"  {item}: {formatted}")
        lines.append("")

    # Cash Flow
    cf = fs.get("cash_flow", {})
    if cf:
        periods = fs.get("cashflow_periods", ["Latest", "Prior"])
        lines.append(f"--- 现金流量表 ({' / '.join(periods[:2])}) ---")
        key_items = [
            "Operating Cash Flow", "Free Cash Flow",
            "Capital Expenditure", "Repurchase Of Capital Stock",
            "Cash Dividends Paid",
        ]
        for item in key_items:
            if item in cf:
                vals = cf[item]
                formatted = " / ".join(_fmt_big(v) for v in vals[:2])
                lines.append(f"  {item}: {formatted}")
        lines.append("")

    # Key sections from SEC filing
    sections = data.get("key_sections", {})
    if sections.get("mda"):
        lines.append("--- 管理层讨论与分析 (MD&A) 摘要 ---")
        lines.append(sections["mda"][:3000])
        lines.append("")
    if sections.get("risk_factors"):
        lines.append("--- 主要风险因素 ---")
        lines.append(sections["risk_factors"][:2000])
        lines.append("")

    if len(lines) <= 2:
        lines.append("(未能获取财报数据)")

    return "\n".join(lines)


def _fmt_big(val) -> str:
    """Format a big number for display."""
    if val is None:
        return "N/A"
    try:
        v = float(val)
    except (TypeError, ValueError):
        return str(val)

    abs_v = abs(v)
    sign = "-" if v < 0 else ""
    if abs_v >= 1e12:
        return f"{sign}${abs_v/1e12:.1f}T"
    if abs_v >= 1e9:
        return f"{sign}${abs_v/1e9:.1f}B"
    if abs_v >= 1e6:
        return f"{sign}${abs_v/1e6:.1f}M"
    if abs_v >= 1e3:
        return f"{sign}${abs_v/1e3:.1f}K"
    return f"{sign}${abs_v:.2f}"
