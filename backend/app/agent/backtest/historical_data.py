"""Point-in-Time historical data fetcher.

Uses yfinance quarterly financial statements to build snapshots of each stock's
fundamental data **as it was known** at each historical point in time, ensuring
no look-ahead bias.

Key design choices
──────────────────
1. Only data from reports with dates **<= rebalance_date** are used.
2. ``ticker.quarterly_income_stmt`` / ``quarterly_balance_sheet`` / ``quarterly_cashflow``
   give ~16-20 quarters of history (4-5 years).
3. Price history comes from ``ticker.history()``.
4. LRU-style in-memory cache avoids re-fetching the same ticker within a run.
5. Concurrency controlled via ``asyncio.Semaphore`` (default 5).
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

import yfinance as yf
import pandas as pd
import numpy as np

from app.agent.backtest.models import HistoricalSnapshot

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  Raw data cache  (ticker → raw DataFrames)
# ═══════════════════════════════════════════════════════════════

_RAW_CACHE: Dict[str, Dict[str, Any]] = {}
_PRICE_CACHE: Dict[str, pd.DataFrame] = {}
_CACHE_LOCK = None  # Lock created lazily in async context if needed


def _safe_float(val: Any) -> Optional[float]:
    """Convert pandas/numpy value to Python float or None."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if math.isnan(val) or math.isinf(val):
            return None
        return float(val)
    try:
        f = float(val)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except (TypeError, ValueError):
        return None


def _get_latest_col_before(df: pd.DataFrame, cutoff: date) -> Optional[pd.Timestamp]:
    """Return the latest column date in *df* that is <= *cutoff*."""
    if df is None or df.empty:
        return None
    cutoff_ts = pd.Timestamp(cutoff)
    valid = [c for c in df.columns if c <= cutoff_ts]
    return max(valid) if valid else None


def _get_earliest_col(df: pd.DataFrame) -> Optional[pd.Timestamp]:
    """Return the earliest column date in *df* (fallback for pre-data snapshots)."""
    if df is None or df.empty:
        return None
    return min(df.columns)


def _iloc_safe(df: pd.DataFrame, row_label: str, col) -> Optional[float]:
    """Safely extract a cell from a DataFrame with row-label index."""
    if df is None or df.empty:
        return None
    for label in [row_label]:
        if label in df.index:
            try:
                return _safe_float(df.loc[label, col])
            except (KeyError, IndexError):
                return None
    return None


def _try_rows(df: pd.DataFrame, col, *labels) -> Optional[float]:
    """Try multiple row labels in order, return first non-None."""
    for lab in labels:
        v = _iloc_safe(df, lab, col)
        if v is not None:
            return v
    return None


# ═══════════════════════════════════════════════════════════════
#  Synchronous raw-data loader
# ═══════════════════════════════════════════════════════════════

def _fetch_raw(symbol: str, lookback_start: date) -> Dict[str, Any]:
    """Fetch and cache raw quarterly financials + price history for *symbol*."""
    if symbol in _RAW_CACHE:
        return _RAW_CACHE[symbol]

    logger.info(f"[PIT-Data] Fetching raw data for {symbol}")
    ticker = yf.Ticker(symbol)
    time.sleep(0.3)  # rate-limit courtesy

    try:
        inc = ticker.quarterly_income_stmt
    except Exception:
        inc = pd.DataFrame()
    try:
        bs = ticker.quarterly_balance_sheet
    except Exception:
        bs = pd.DataFrame()
    try:
        cf = ticker.quarterly_cashflow
    except Exception:
        cf = pd.DataFrame()
    try:
        info = ticker.info or {}
    except Exception:
        info = {}

    raw = {"income": inc, "balance": bs, "cashflow": cf, "info": info}
    _RAW_CACHE[symbol] = raw
    return raw


def _fetch_price_history(symbol: str, start: date, end: date) -> pd.DataFrame:
    """Fetch daily Close prices for *symbol* between [start, end]."""
    cache_key = symbol
    if cache_key in _PRICE_CACHE:
        df = _PRICE_CACHE[cache_key]
        mask = (df.index.date >= start) & (df.index.date <= end)
        sub = df.loc[mask]
        if not sub.empty:
            return sub

    logger.debug(f"[PIT-Data] Fetching price history for {symbol}")
    ticker = yf.Ticker(symbol)
    time.sleep(0.2)
    start_str = (start - timedelta(days=30)).strftime("%Y-%m-%d")
    end_str = (end + timedelta(days=5)).strftime("%Y-%m-%d")
    try:
        hist = ticker.history(start=start_str, end=end_str, auto_adjust=True)
    except Exception:
        hist = pd.DataFrame()

    if hist is not None and not hist.empty:
        _PRICE_CACHE[cache_key] = hist
    return hist if hist is not None else pd.DataFrame()


def _price_on_date(symbol: str, target: date, tolerance_days: int = 5) -> Optional[float]:
    """Get the closing price nearest to *target* (within tolerance)."""
    hist = _PRICE_CACHE.get(symbol)
    if hist is None or hist.empty:
        return None
    target_ts = pd.Timestamp(target)
    for delta in range(0, tolerance_days + 1):
        for d in [target_ts + pd.Timedelta(days=delta), target_ts - pd.Timedelta(days=delta)]:
            if d in hist.index:
                return _safe_float(hist.loc[d, "Close"])
    # fallback: nearest
    idx = hist.index.get_indexer([target_ts], method="nearest")
    if idx[0] >= 0 and abs((hist.index[idx[0]] - target_ts).days) <= tolerance_days:
        return _safe_float(hist.iloc[idx[0]]["Close"])
    return None


# ═══════════════════════════════════════════════════════════════
#  Piotroski F-Score (9 binary criteria)
# ═══════════════════════════════════════════════════════════════

def _compute_f_score(
    inc: pd.DataFrame,
    bs: pd.DataFrame,
    cf: pd.DataFrame,
    inc_col: pd.Timestamp,
    bs_col: pd.Timestamp,
    cf_col: pd.Timestamp,
    as_of: date,
) -> Optional[int]:
    """Compute Piotroski F-Score (0-9) from quarterly financial statements.

    Uses current and year-ago data for YoY comparisons.
    Returns None if insufficient data.
    """
    if inc_col is None and bs_col is None:
        return None

    score = 0

    # --- Helpers: get TTM sums for a given set of columns ---
    def _ttm_sum(df, cols, *labels):
        return sum(_try_rows(df, c, *labels) or 0 for c in cols)

    # Current and prior year columns (TTM approximation)
    inc_cols = sorted([c for c in inc.columns if c <= pd.Timestamp(as_of)], reverse=True) if not inc.empty else []
    bs_cols = sorted([c for c in bs.columns if c <= pd.Timestamp(as_of)], reverse=True) if not bs.empty else []
    cf_cols = sorted([c for c in cf.columns if c <= pd.Timestamp(as_of)], reverse=True) if not cf.empty else []

    cur_inc = inc_cols[:4]
    prev_inc = inc_cols[4:8]
    cur_bs = bs_cols[0] if bs_cols else None
    prev_bs = bs_cols[4] if len(bs_cols) > 4 else None
    cur_cf = cf_cols[:4]

    # 1. ROA positive (net income TTM > 0)
    if len(cur_inc) >= 2:
        ni_ttm = _ttm_sum(inc, cur_inc, "Net Income", "Net Income Common Stockholders")
        if ni_ttm > 0:
            score += 1

    # 2. Operating Cash Flow positive
    if len(cur_cf) >= 2:
        ocf_ttm = _ttm_sum(cf, cur_cf, "Operating Cash Flow", "Cash Flow From Continuing Operating Activities")
        if ocf_ttm > 0:
            score += 1

    # 3. ROA increasing (current vs prior year)
    if len(cur_inc) >= 2 and len(prev_inc) >= 2:
        ni_cur = _ttm_sum(inc, cur_inc, "Net Income", "Net Income Common Stockholders")
        ni_prev = _ttm_sum(inc, prev_inc, "Net Income", "Net Income Common Stockholders")
        ta_cur = _try_rows(bs, cur_bs, "Total Assets") if cur_bs else None
        ta_prev = _try_rows(bs, prev_bs, "Total Assets") if prev_bs else None
        if ta_cur and ta_cur > 0 and ta_prev and ta_prev > 0:
            roa_cur = ni_cur / ta_cur
            roa_prev = ni_prev / ta_prev
            if roa_cur > roa_prev:
                score += 1

    # 4. Cash flow > Net Income (accrual quality)
    if len(cur_inc) >= 2 and len(cur_cf) >= 2:
        ni_ttm = _ttm_sum(inc, cur_inc, "Net Income", "Net Income Common Stockholders")
        ocf_ttm = _ttm_sum(cf, cur_cf, "Operating Cash Flow", "Cash Flow From Continuing Operating Activities")
        if ocf_ttm > ni_ttm:
            score += 1

    # 5. Leverage decreasing (long-term debt / total assets)
    if cur_bs and prev_bs:
        debt_cur = _try_rows(bs, cur_bs, "Total Debt", "Long Term Debt") or 0
        debt_prev = _try_rows(bs, prev_bs, "Total Debt", "Long Term Debt") or 0
        ta_cur = _try_rows(bs, cur_bs, "Total Assets") or 1
        ta_prev = _try_rows(bs, prev_bs, "Total Assets") or 1
        if ta_cur > 0 and ta_prev > 0:
            if (debt_cur / ta_cur) <= (debt_prev / ta_prev):
                score += 1

    # 6. Current ratio increasing
    if cur_bs and prev_bs:
        ca_cur = _try_rows(bs, cur_bs, "Current Assets", "Total Current Assets") or 0
        cl_cur = _try_rows(bs, cur_bs, "Current Liabilities", "Total Current Liabilities") or 1
        ca_prev = _try_rows(bs, prev_bs, "Current Assets", "Total Current Assets") or 0
        cl_prev = _try_rows(bs, prev_bs, "Current Liabilities", "Total Current Liabilities") or 1
        cr_cur = ca_cur / cl_cur if cl_cur > 0 else 0
        cr_prev = ca_prev / cl_prev if cl_prev > 0 else 0
        if cr_cur > cr_prev:
            score += 1

    # 7. No new shares issued (shares outstanding not increased)
    if cur_bs and prev_bs:
        shares_cur = _try_rows(bs, cur_bs, "Ordinary Shares Number", "Share Issued")
        shares_prev = _try_rows(bs, prev_bs, "Ordinary Shares Number", "Share Issued")
        if shares_cur and shares_prev and shares_cur <= shares_prev:
            score += 1

    # 8. Gross margin increasing
    if len(cur_inc) >= 2 and len(prev_inc) >= 2:
        rev_cur = _ttm_sum(inc, cur_inc, "Total Revenue", "Revenue")
        cogs_cur = _ttm_sum(inc, cur_inc, "Cost Of Revenue")
        rev_prev = _ttm_sum(inc, prev_inc, "Total Revenue", "Revenue")
        cogs_prev = _ttm_sum(inc, prev_inc, "Cost Of Revenue")
        if rev_cur > 0 and rev_prev > 0:
            gm_cur = (rev_cur - cogs_cur) / rev_cur
            gm_prev = (rev_prev - cogs_prev) / rev_prev
            if gm_cur > gm_prev:
                score += 1

    # 9. Asset turnover increasing
    if len(cur_inc) >= 2 and len(prev_inc) >= 2:
        rev_cur = _ttm_sum(inc, cur_inc, "Total Revenue", "Revenue")
        rev_prev = _ttm_sum(inc, prev_inc, "Total Revenue", "Revenue")
        ta_cur = _try_rows(bs, cur_bs, "Total Assets") if cur_bs else None
        ta_prev = _try_rows(bs, prev_bs, "Total Assets") if prev_bs else None
        if ta_cur and ta_cur > 0 and ta_prev and ta_prev > 0:
            at_cur = rev_cur / ta_cur
            at_prev = rev_prev / ta_prev
            if at_cur > at_prev:
                score += 1

    return score


# ═══════════════════════════════════════════════════════════════
#  Altman Z-Score (5-factor model for manufacturing firms)
# ═══════════════════════════════════════════════════════════════

def _compute_z_score(
    inc: pd.DataFrame,
    bs: pd.DataFrame,
    inc_cols_before: list,
    bs_col: pd.Timestamp,
    market_cap: Optional[float],
) -> Optional[float]:
    """Compute Altman Z-Score from quarterly financial data.

    Z = 1.2*X1 + 1.4*X2 + 3.3*X3 + 0.6*X4 + 1.0*X5
    Where:
      X1 = Working Capital / Total Assets
      X2 = Retained Earnings / Total Assets
      X3 = EBIT / Total Assets
      X4 = Market Cap / Total Liabilities
      X5 = Revenue / Total Assets

    Returns None if insufficient data.
    """
    if bs_col is None:
        return None

    ta = _try_rows(bs, bs_col, "Total Assets")
    if not ta or ta <= 0:
        return None

    ca = _try_rows(bs, bs_col, "Current Assets", "Total Current Assets") or 0
    cl = _try_rows(bs, bs_col, "Current Liabilities", "Total Current Liabilities") or 0
    tl = _try_rows(bs, bs_col, "Total Liabilities Net Minority Interest", "Total Liabilities") or 0
    re = _try_rows(bs, bs_col, "Retained Earnings") or 0

    # TTM EBIT and Revenue
    ebit_ttm = 0
    rev_ttm = 0
    if inc_cols_before and len(inc_cols_before) >= 2:
        ebit_ttm = sum(_try_rows(inc, c, "EBIT", "Operating Income") or 0 for c in inc_cols_before[:4])
        rev_ttm = sum(_try_rows(inc, c, "Total Revenue", "Revenue") or 0 for c in inc_cols_before[:4])

    x1 = (ca - cl) / ta
    x2 = re / ta
    x3 = ebit_ttm / ta
    x4 = (market_cap / tl) if (market_cap and tl > 0) else 0
    x5 = rev_ttm / ta

    z = 1.2 * x1 + 1.4 * x2 + 3.3 * x3 + 0.6 * x4 + 1.0 * x5
    return round(z, 2)


# ═══════════════════════════════════════════════════════════════
#  Beneish M-Score (8-variable model for earnings manipulation)
# ═══════════════════════════════════════════════════════════════

def _compute_m_score(
    inc: pd.DataFrame,
    bs: pd.DataFrame,
    cf: pd.DataFrame,
    inc_cols_before: list,
    bs_col: pd.Timestamp,
    as_of: date,
) -> Optional[float]:
    """Compute Beneish M-Score from quarterly financial data.

    M = -4.84 + 0.920*DSRI + 0.528*GMI + 0.404*AQI + 0.892*SGI
        + 0.115*DEPI - 0.172*SGAI + 4.679*TATA - 0.327*LVGI

    Requires current and year-ago data. Returns None if insufficient.
    A score > -1.78 suggests earnings manipulation.
    """
    if not inc_cols_before or len(inc_cols_before) < 8:
        return None  # need at least 2 years of quarterly data

    bs_cols = sorted([c for c in bs.columns if c <= pd.Timestamp(as_of)], reverse=True) if not bs.empty else []
    if len(bs_cols) < 5:
        return None

    cur_bs = bs_cols[0]
    prev_bs = bs_cols[4] if len(bs_cols) > 4 else None
    if prev_bs is None:
        return None

    cur_inc = inc_cols_before[:4]
    prev_inc = inc_cols_before[4:8]

    def _ttm(df, cols, *labels):
        return sum(_try_rows(df, c, *labels) or 0 for c in cols)

    # Current year
    rev_cur = _ttm(inc, cur_inc, "Total Revenue", "Revenue")
    cogs_cur = _ttm(inc, cur_inc, "Cost Of Revenue")
    ni_cur = _ttm(inc, cur_inc, "Net Income", "Net Income Common Stockholders")
    sga_cur = _ttm(inc, cur_inc, "Selling General And Administration", "General And Administrative Expense")
    dep_cur = _ttm(inc, cur_inc, "Depreciation And Amortization In Income Statement",
                    "Reconciled Depreciation", "Depreciation")

    # Prior year
    rev_prev = _ttm(inc, prev_inc, "Total Revenue", "Revenue")
    cogs_prev = _ttm(inc, prev_inc, "Cost Of Revenue")
    sga_prev = _ttm(inc, prev_inc, "Selling General And Administration", "General And Administrative Expense")
    dep_prev = _ttm(inc, prev_inc, "Depreciation And Amortization In Income Statement",
                     "Reconciled Depreciation", "Depreciation")

    # Balance sheet
    recv_cur = _try_rows(bs, cur_bs, "Net Receivable", "Accounts Receivable") or 0
    recv_prev = _try_rows(bs, prev_bs, "Net Receivable", "Accounts Receivable") or 0
    ta_cur = _try_rows(bs, cur_bs, "Total Assets") or 0
    ta_prev = _try_rows(bs, prev_bs, "Total Assets") or 0
    ca_cur = _try_rows(bs, cur_bs, "Current Assets", "Total Current Assets") or 0
    ppe_cur = _try_rows(bs, cur_bs, "Net PPE", "Gross PPE") or 0
    ca_prev = _try_rows(bs, prev_bs, "Current Assets", "Total Current Assets") or 0
    ppe_prev = _try_rows(bs, prev_bs, "Net PPE", "Gross PPE") or 0
    cl_cur = _try_rows(bs, cur_bs, "Current Liabilities", "Total Current Liabilities") or 0
    tl_cur = _try_rows(bs, cur_bs, "Total Liabilities Net Minority Interest", "Total Liabilities") or 0
    cl_prev = _try_rows(bs, prev_bs, "Current Liabilities", "Total Current Liabilities") or 0
    tl_prev = _try_rows(bs, prev_bs, "Total Liabilities Net Minority Interest", "Total Liabilities") or 0

    # Cash flow for TATA
    cf_cols = sorted([c for c in cf.columns if c <= pd.Timestamp(as_of)], reverse=True) if not cf.empty else []
    ocf_ttm = sum(_try_rows(cf, c, "Operating Cash Flow", "Cash Flow From Continuing Operating Activities") or 0
                  for c in cf_cols[:4]) if len(cf_cols) >= 2 else 0

    # Guard divisions
    if rev_prev <= 0 or rev_cur <= 0 or ta_cur <= 0 or ta_prev <= 0:
        return None

    gm_cur = (rev_cur - cogs_cur) / rev_cur
    gm_prev = (rev_prev - cogs_prev) / rev_prev

    # 1. DSRI — Days Sales in Receivables Index
    dsri = ((recv_cur / rev_cur) / (recv_prev / rev_prev)) if recv_prev > 0 and rev_prev > 0 else 1.0

    # 2. GMI — Gross Margin Index
    gmi = (gm_prev / gm_cur) if gm_cur != 0 else 1.0

    # 3. AQI — Asset Quality Index (non-current, non-PPE assets / TA)
    aq_cur = 1 - ((ca_cur + ppe_cur) / ta_cur) if ta_cur > 0 else 0
    aq_prev = 1 - ((ca_prev + ppe_prev) / ta_prev) if ta_prev > 0 else 0
    aqi = (aq_cur / aq_prev) if aq_prev != 0 else 1.0

    # 4. SGI — Sales Growth Index
    sgi = rev_cur / rev_prev

    # 5. DEPI — Depreciation Index
    dep_rate_cur = dep_cur / (dep_cur + ppe_cur) if (dep_cur + ppe_cur) > 0 else 0
    dep_rate_prev = dep_prev / (dep_prev + ppe_prev) if (dep_prev + ppe_prev) > 0 else 0
    depi = (dep_rate_prev / dep_rate_cur) if dep_rate_cur != 0 else 1.0

    # 6. SGAI — SGA Expense Index
    sga_rate_cur = sga_cur / rev_cur if rev_cur > 0 else 0
    sga_rate_prev = sga_prev / rev_prev if rev_prev > 0 else 0
    sgai = (sga_rate_cur / sga_rate_prev) if sga_rate_prev != 0 else 1.0

    # 7. TATA — Total Accruals to Total Assets
    tata = (ni_cur - ocf_ttm) / ta_cur if ta_cur > 0 else 0

    # 8. LVGI — Leverage Index
    lev_cur = tl_cur / ta_cur if ta_cur > 0 else 0
    lev_prev = tl_prev / ta_prev if ta_prev > 0 else 0
    lvgi = (lev_cur / lev_prev) if lev_prev != 0 else 1.0

    m = (-4.84 + 0.920 * dsri + 0.528 * gmi + 0.404 * aqi + 0.892 * sgi
         + 0.115 * depi - 0.172 * sgai + 4.679 * tata - 0.327 * lvgi)

    return round(m, 2)


# ═══════════════════════════════════════════════════════════════
#  Snapshot builder
# ═══════════════════════════════════════════════════════════════

def build_snapshot(symbol: str, as_of: date, raw: Dict[str, Any]) -> Optional[HistoricalSnapshot]:
    """Build a HistoricalSnapshot from raw quarterly data, using only data known as of *as_of*.

    For dates that precede all available quarterly data (e.g. requesting a snapshot
    from 8 years ago when only 5 years of financials exist), the function falls back
    to the EARLIEST available financial column while still using the as_of-date price.
    This lets us run long-horizon backtests (10 yr+) even with limited financial history
    — the fundamentals for the earliest periods will be approximate but the price-driven
    P&L is accurate.
    """
    inc: pd.DataFrame = raw.get("income", pd.DataFrame())
    bs: pd.DataFrame = raw.get("balance", pd.DataFrame())
    cf: pd.DataFrame = raw.get("cashflow", pd.DataFrame())
    info: dict = raw.get("info", {})

    inc_col = _get_latest_col_before(inc, as_of)
    bs_col = _get_latest_col_before(bs, as_of)
    cf_col = _get_latest_col_before(cf, as_of)

    # ── Fallback: if as_of is before all financial data, use earliest available ──
    _used_fallback = False
    if inc_col is None and bs_col is None:
        # Try earliest financial column as fallback (for long-lookback scenarios)
        inc_col = _get_earliest_col(inc)
        bs_col = _get_earliest_col(bs)
        cf_col = _get_earliest_col(cf)
        if inc_col is None and bs_col is None:
            return None  # truly no usable financial data
        _used_fallback = True

    # ── Price ──
    price = _price_on_date(symbol, as_of) or _safe_float(info.get("currentPrice"))
    if not price:
        price = _safe_float(info.get("regularMarketPrice"))

    # ── Income statement fields ──
    revenue = _try_rows(inc, inc_col, "Total Revenue", "Revenue") if inc_col else None
    net_income = _try_rows(inc, inc_col, "Net Income", "Net Income Common Stockholders") if inc_col else None
    ebit = _try_rows(inc, inc_col, "EBIT", "Operating Income") if inc_col else None
    eps_val = _try_rows(inc, inc_col, "Basic EPS", "Diluted EPS") if inc_col else None
    interest_expense = _try_rows(inc, inc_col, "Interest Expense", "Interest Expense Non Operating") if inc_col else None

    # Annualise quarterly data (TTM — sum last 4 quarters, with safety valve)
    _data_quality = "normal"
    _ttm_quarters = 0
    if inc_col is not None and not inc.empty:
        cols_before = sorted([c for c in inc.columns if c <= pd.Timestamp(as_of)], reverse=True)[:4]
        _ttm_quarters = len(cols_before)
        if _ttm_quarters >= 2:
            revenue_raw = sum(_try_rows(inc, c, "Total Revenue", "Revenue") or 0 for c in cols_before)
            ni_raw = sum(_try_rows(inc, c, "Net Income", "Net Income Common Stockholders") or 0 for c in cols_before)
            ebit_raw = sum(_try_rows(inc, c, "EBIT", "Operating Income") or 0 for c in cols_before)
            eps_raw = sum(_try_rows(inc, c, "Basic EPS", "Diluted EPS") or 0 for c in cols_before)

            # TTM safety valve: if < 4 quarters, annualise by scaling up
            if _ttm_quarters < 4:
                scale = 4.0 / _ttm_quarters
                revenue_raw *= scale
                ni_raw *= scale
                ebit_raw *= scale
                eps_raw *= scale
                _data_quality = "low"

            revenue_ttm = _safe_float(revenue_raw)
            net_income_ttm = _safe_float(ni_raw)
            ebit_ttm = _safe_float(ebit_raw)
            eps_ttm = _safe_float(eps_raw)
            if revenue_ttm and revenue_ttm > 0:
                revenue = revenue_ttm
            if net_income_ttm:
                net_income = net_income_ttm
            if ebit_ttm:
                ebit = ebit_ttm
            if eps_ttm:
                eps_val = eps_ttm

    # ── Balance sheet fields ──
    total_assets = _try_rows(bs, bs_col, "Total Assets") if bs_col else None
    total_liabilities = _try_rows(bs, bs_col, "Total Liabilities Net Minority Interest", "Total Liabilities") if bs_col else None
    stockholders_equity = _try_rows(bs, bs_col, "Stockholders Equity", "Total Equity Gross Minority Interest") if bs_col else None
    current_assets = _try_rows(bs, bs_col, "Current Assets", "Total Current Assets") if bs_col else None
    current_liabilities = _try_rows(bs, bs_col, "Current Liabilities", "Total Current Liabilities") if bs_col else None
    total_debt = _try_rows(bs, bs_col, "Total Debt", "Long Term Debt") if bs_col else None
    total_cash = _try_rows(bs, bs_col, "Cash And Cash Equivalents", "Cash Cash Equivalents And Short Term Investments") if bs_col else None
    shares_out = _try_rows(bs, bs_col, "Ordinary Shares Number", "Share Issued") if bs_col else None
    if not shares_out:
        shares_out = _safe_float(info.get("sharesOutstanding"))

    # ── Cash flow fields ──
    fcf = _try_rows(cf, cf_col, "Free Cash Flow") if cf_col else None
    ocf = _try_rows(cf, cf_col, "Operating Cash Flow", "Cash Flow From Continuing Operating Activities") if cf_col else None
    capex = _try_rows(cf, cf_col, "Capital Expenditure") if cf_col else None

    # Annualise cash flow (TTM — sum last 4 quarters, with safety valve)
    if cf_col is not None and cf is not None and not cf.empty:
        cf_cols_before = sorted([c for c in cf.columns if c <= pd.Timestamp(as_of)], reverse=True)[:4]
        cf_q_count = len(cf_cols_before)
        if cf_q_count >= 2:
            fcf_raw = sum(_try_rows(cf, c, "Free Cash Flow") or 0 for c in cf_cols_before)
            ocf_raw = sum(
                _try_rows(cf, c, "Operating Cash Flow", "Cash Flow From Continuing Operating Activities") or 0
                for c in cf_cols_before
            )
            # Scale up if fewer than 4 quarters
            if cf_q_count < 4:
                cf_scale = 4.0 / cf_q_count
                fcf_raw *= cf_scale
                ocf_raw *= cf_scale
            fcf_ttm = _safe_float(fcf_raw)
            ocf_ttm = _safe_float(ocf_raw)
            if fcf_ttm:
                fcf = fcf_ttm
            if ocf_ttm:
                ocf = ocf_ttm

    # ── Derived ratios ──
    pe = None
    if price and eps_val and eps_val > 0:
        pe = price / eps_val

    pb = None
    book_value_ps = None
    if stockholders_equity and shares_out and shares_out > 0:
        book_value_ps = stockholders_equity / shares_out
        if price and book_value_ps > 0:
            pb = price / book_value_ps

    ps = None
    if price and revenue and shares_out and shares_out > 0:
        rev_per_share = revenue / shares_out
        if rev_per_share > 0:
            ps = price / rev_per_share

    market_cap = price * shares_out if price and shares_out else _safe_float(info.get("marketCap"))

    roe = None
    if net_income and stockholders_equity and stockholders_equity > 0:
        roe = net_income / stockholders_equity

    operating_margin = None
    if ebit and revenue and revenue > 0:
        operating_margin = ebit / revenue

    profit_margin = None
    if net_income and revenue and revenue > 0:
        profit_margin = net_income / revenue

    current_ratio = None
    if current_assets and current_liabilities and current_liabilities > 0:
        current_ratio = current_assets / current_liabilities

    debt_to_equity = None
    if total_liabilities and stockholders_equity and stockholders_equity > 0:
        debt_to_equity = total_liabilities / stockholders_equity

    interest_coverage = None
    if ebit and interest_expense and interest_expense != 0:
        interest_coverage = abs(ebit / interest_expense)

    dividend_yield = _safe_float(info.get("dividendYield"))
    dividends_per_share = _safe_float(info.get("dividendRate"))

    # earnings_yield: stored as DECIMAL (0.08 = 8%), matching distilled_rules convention
    earnings_yield = (1.0 / pe) if pe and pe > 0 else None

    # eps_growth: compare latest quarter EPS to year-ago quarter
    eps_growth = None
    if inc_col is not None and not inc.empty:
        cols_sorted = sorted([c for c in inc.columns if c <= pd.Timestamp(as_of)], reverse=True)
        if len(cols_sorted) >= 5:
            eps_now = _try_rows(inc, cols_sorted[0], "Basic EPS", "Diluted EPS")
            eps_ago = _try_rows(inc, cols_sorted[4], "Basic EPS", "Diluted EPS")
            if eps_now is not None and eps_ago is not None and eps_ago != 0:
                eps_growth = (eps_now - eps_ago) / abs(eps_ago)

    # revenue_growth
    revenue_growth = None
    if inc_col is not None and not inc.empty:
        cols_sorted = sorted([c for c in inc.columns if c <= pd.Timestamp(as_of)], reverse=True)
        if len(cols_sorted) >= 5:
            rev_now = _try_rows(inc, cols_sorted[0], "Total Revenue", "Revenue")
            rev_ago = _try_rows(inc, cols_sorted[4], "Total Revenue", "Revenue")
            if rev_now is not None and rev_ago is not None and rev_ago != 0:
                revenue_growth = (rev_now - rev_ago) / abs(rev_ago)

    # peg
    peg = None
    if pe and eps_growth and eps_growth > 0:
        growth_pct = eps_growth * 100
        if growth_pct > 0:
            peg = pe / growth_pct

    # 52-week high (store ABSOLUTE price, not ratio)
    price_52w_high = None
    price_vs_52w_high = None
    hist = _PRICE_CACHE.get(symbol)
    if hist is not None and not hist.empty and price:
        year_ago = as_of - timedelta(days=365)
        mask = (hist.index.date >= year_ago) & (hist.index.date <= as_of)
        sub = hist.loc[mask]
        if not sub.empty:
            high_52w = sub["Close"].max()
            if high_52w and high_52w > 0:
                price_52w_high = float(high_52w)  # absolute price for distilled_rules
                price_vs_52w_high = price / high_52w  # ratio for snapshot field

    # working capital & ncav
    working_capital = None
    ncav_per_share = None
    if current_assets is not None and total_liabilities is not None:
        ncav = current_assets - total_liabilities
        if shares_out and shares_out > 0:
            ncav_per_share = ncav / shares_out
        working_capital = current_assets - (current_liabilities or 0)

    # graham_number
    graham_number = None
    if eps_val and eps_val > 0 and book_value_ps and book_value_ps > 0:
        graham_number = math.sqrt(22.5 * eps_val * book_value_ps)

    # intrinsic_value (Graham formula: V = EPS × (8.5 + 2g) × 4.4 / Y)
    # g = expected growth rate as percentage (e.g. 10 for 10%)
    # Y = current AA corporate bond yield (use historical avg ~5% as fallback)
    # Note: Using y=4.4 when it's the numerator constant makes 4.4/y=1.0, defeating
    # the purpose. We use a realistic historical AA bond yield estimate instead.
    intrinsic_value = None
    margin_of_safety_val = None
    if eps_val and eps_val > 0 and eps_growth is not None:
        g = max(eps_growth * 100, 0)  # convert decimal to percentage
        y = 5.0  # realistic AA corporate bond yield estimate (historical avg)
        iv = eps_val * (8.5 + 2 * g) * 4.4 / y
        if iv > 0:
            intrinsic_value = iv
            if price and price > 0:
                margin_of_safety_val = (iv - price) / iv

    # enterprise_value
    enterprise_value = None
    if market_cap is not None:
        ev = market_cap + (total_debt or 0) - (total_cash or 0)
        if ev > 0:
            enterprise_value = ev

    # ── Historical derived fields for distilled_rules ──
    # avg_eps_3y, avg_eps_10y: average EPS over past 3/10 years (from quarterly data)
    # max_eps_decline: maximum year-over-year EPS decline seen in history
    # profitable_years: number of years with positive net income
    # earnings_growth_10y: CAGR of EPS over all available years
    avg_eps_3y = None
    avg_eps_10y = None
    max_eps_decline = None
    profitable_years_val = None
    earnings_growth_10y = None
    available_years_val = None

    if inc_col is not None and not inc.empty:
        cols_sorted = sorted([c for c in inc.columns if c <= pd.Timestamp(as_of)], reverse=True)
        # Collect annual EPS figures (every 4th quarter for annualised)
        annual_eps_list = []
        for i in range(0, len(cols_sorted), 4):
            chunk = cols_sorted[i:i + 4]
            if len(chunk) >= 2:  # at least 2 quarters for reasonable annual estimate
                # Scale up if partial year
                raw_eps = sum(
                    _try_rows(inc, c, "Basic EPS", "Diluted EPS") or 0
                    for c in chunk
                )
                if len(chunk) < 4:
                    raw_eps = raw_eps * (4.0 / len(chunk))
                annual_eps_list.append(raw_eps)

        available_years_val = len(annual_eps_list)

        if len(annual_eps_list) >= 1:
            avg_eps_3y = sum(annual_eps_list[:3]) / min(len(annual_eps_list), 3)
        if len(annual_eps_list) >= 3:
            avg_eps_10y = sum(annual_eps_list[:min(len(annual_eps_list), 10)]) / min(len(annual_eps_list), 10)

        # max EPS decline (year-over-year)
        if len(annual_eps_list) >= 2:
            declines = []
            for i in range(len(annual_eps_list) - 1):
                if annual_eps_list[i + 1] != 0:
                    yoy = (annual_eps_list[i] - annual_eps_list[i + 1]) / abs(annual_eps_list[i + 1])
                    if yoy < 0:
                        declines.append(yoy)
            if declines:
                max_eps_decline = max(declines)  # least negative decline (closest to 0)
            else:
                max_eps_decline = 0.0  # no declines

        # profitable_years: count years with positive annual EPS
        profitable_years_val = sum(1 for e in annual_eps_list if e > 0)

        # earnings_growth_10y: CAGR using first and last available annual EPS
        if len(annual_eps_list) >= 2:
            newest_eps = annual_eps_list[0]
            oldest_eps = annual_eps_list[-1]
            n_years = len(annual_eps_list) - 1
            if oldest_eps > 0 and newest_eps > 0 and n_years > 0:
                earnings_growth_10y = (newest_eps / oldest_eps) ** (1.0 / n_years) - 1.0

    # ── F-Score / Z-Score / M-Score ──
    # Compute using dedicated helper functions from quarterly data
    inc_cols_all = sorted([c for c in inc.columns if c <= pd.Timestamp(as_of)], reverse=True) if not inc.empty else []
    f_score_val = _compute_f_score(inc, bs, cf, inc_col, bs_col, cf_col, as_of)
    z_score_val = _compute_z_score(inc, bs, inc_cols_all, bs_col, market_cap)
    m_score_val = _compute_m_score(inc, bs, cf, inc_cols_all, bs_col, as_of)

    # Mark data_quality as extrapolated if we used fallback
    if _used_fallback:
        _data_quality = "extrapolated"

    snap = HistoricalSnapshot(
        symbol=symbol,
        as_of_date=as_of,
        price=price or 0.0,
        pe=pe,
        pb=pb,
        ps=ps,
        market_cap=market_cap,
        roe=roe,
        operating_margin=operating_margin,
        profit_margin=profit_margin,
        current_ratio=current_ratio,
        debt_to_equity=debt_to_equity,
        interest_coverage=interest_coverage,
        dividend_yield=dividend_yield,
        eps=eps_val,
        eps_growth=eps_growth,
        revenue_growth=revenue_growth,
        earnings_growth_10y=earnings_growth_10y,
        peg=peg,
        earnings_yield=earnings_yield,
        f_score=f_score_val,
        z_score=z_score_val,
        m_score=m_score_val,
        data_quality=_data_quality,
        available_years=available_years_val,
        price_vs_52w_high=price_vs_52w_high,
        total_assets=total_assets,
        total_liabilities=total_liabilities,
        total_current_assets=current_assets,
        total_current_liabilities=current_liabilities,
        shares_outstanding=shares_out,
        free_cash_flow=fcf,
        operating_cash_flow=ocf,
        capex=capex,
        revenue=revenue,
        ebit=ebit,
        net_income=net_income,
        dividends_per_share=dividends_per_share,
        book_value=book_value_ps,
        total_debt=total_debt,
        total_cash=total_cash,
        enterprise_value=enterprise_value,
        working_capital=working_capital,
        ncav_per_share=ncav_per_share,
        intrinsic_value=intrinsic_value,
        margin_of_safety=margin_of_safety_val,
        graham_number=graham_number,
        price_52w_high=price_52w_high,
        avg_eps_3y=avg_eps_3y,
        avg_eps_10y=avg_eps_10y,
        max_eps_decline=max_eps_decline,
        profitable_years=profitable_years_val,
    )
    return snap


# ═══════════════════════════════════════════════════════════════
#  Public API — HistoricalDataFetcher
# ═══════════════════════════════════════════════════════════════

class HistoricalDataFetcher:
    """Fetch & cache Point-in-Time fundamental snapshots for a list of symbols.

    Usage::

        fetcher = HistoricalDataFetcher(symbols, lookback_years=3.0)
        snapshots = await fetcher.fetch_all(progress_callback=...)
        # snapshots: Dict[str, Dict[date, HistoricalSnapshot]]
        #            symbol  →  {rebalance_date → snapshot}
    """

    def __init__(
        self,
        symbols: List[str],
        lookback_years: float = 3.0,
        holding_months: int = 6,
        benchmark: str = "SPY",
        max_concurrent: int = 5,
    ):
        self.symbols = [s.upper() for s in symbols]
        self.lookback_years = lookback_years
        self.holding_months = holding_months
        self.benchmark = benchmark.upper()
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._rebalance_dates: List[date] = []

        if lookback_years > 5:
            logger.info(
                f"[PIT-Data] Long lookback: {lookback_years:.1f} years. "
                f"Note: yfinance quarterly financials cover ~4-5 years. "
                f"Snapshots beyond that will use earliest available data + price history."
            )

    def compute_rebalance_dates(self) -> List[date]:
        """Generate rebalance dates going back *lookback_years* from today."""
        today = date.today()
        dates: List[date] = []
        d = today
        months_back = int(self.lookback_years * 12)
        for _ in range(0, months_back, self.holding_months):
            d = _subtract_months(d, self.holding_months)
            dates.append(d)
        dates.sort()
        self._rebalance_dates = dates
        return dates

    async def fetch_all(
        self,
        progress_callback=None,
    ) -> Dict[str, Dict[date, HistoricalSnapshot]]:
        """Fetch all snapshots for all symbols at all rebalance dates.

        Returns {symbol: {date: HistoricalSnapshot}}.

        Post-processing:
        - Extracts benchmark PE (market_pe) from SPY snapshots
        - Injects market_pe into all stock snapshots for Quantitative rules
        """
        if not self._rebalance_dates:
            self.compute_rebalance_dates()

        all_symbols = list(set(self.symbols + [self.benchmark]))
        total = len(all_symbols)
        result: Dict[str, Dict[date, HistoricalSnapshot]] = {}

        earliest = self._rebalance_dates[0] - timedelta(days=60)
        latest = date.today() + timedelta(days=5)

        for idx, sym in enumerate(all_symbols):
            try:
                await self._fetch_one(sym, earliest, latest, result)
            except Exception as e:
                logger.warning(f"[PIT-Data] Failed to fetch {sym}: {e}")

            if progress_callback:
                await progress_callback(
                    stage="data_fetch",
                    current=idx + 1,
                    total=total,
                    symbol=sym,
                )

        # ── Post-processing: inject market_pe from benchmark ──
        bench_snaps = result.get(self.benchmark, {})
        if bench_snaps:
            for rd, bench_snap in bench_snaps.items():
                bench_pe = bench_snap.pe
                if bench_pe and bench_pe > 0:
                    # Inject market_pe into all non-benchmark stocks at this date
                    for sym, sym_snaps in result.items():
                        if sym == self.benchmark:
                            continue
                        snap = sym_snaps.get(rd)
                        if snap:
                            snap.market_pe = bench_pe

        return result

    async def _fetch_one(
        self,
        symbol: str,
        price_start: date,
        price_end: date,
        result: Dict[str, Dict[date, HistoricalSnapshot]],
    ):
        """Fetch raw data and build snapshots for one symbol."""
        async with self._semaphore:
            loop = asyncio.get_running_loop()
            raw = await loop.run_in_executor(None, _fetch_raw, symbol, price_start)
            await loop.run_in_executor(
                None, _fetch_price_history, symbol, price_start, price_end
            )

        snapshots: Dict[date, HistoricalSnapshot] = {}
        for rd in self._rebalance_dates:
            snap = build_snapshot(symbol, rd, raw)
            if snap and snap.price > 0:
                snapshots[rd] = snap

        if snapshots:
            result[symbol] = snapshots

    async def fetch_benchmark_prices(self) -> pd.DataFrame:
        """Return daily Close prices for the benchmark (e.g. SPY)."""
        if not self._rebalance_dates:
            self.compute_rebalance_dates()
        earliest = self._rebalance_dates[0] - timedelta(days=30)
        latest = date.today() + timedelta(days=5)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, _fetch_price_history, self.benchmark, earliest, latest
        )
        return _PRICE_CACHE.get(self.benchmark, pd.DataFrame())

    def get_price_on_date(self, symbol: str, target: date) -> Optional[float]:
        """Get cached closing price for *symbol* near *target*."""
        return _price_on_date(symbol, target)

    def get_price_series(self, symbol: str, start: date, end: date) -> pd.Series:
        """Return daily Close prices for *symbol* between [start, end]."""
        hist = _PRICE_CACHE.get(symbol, pd.DataFrame())
        if hist.empty:
            return pd.Series(dtype=float)
        mask = (hist.index.date >= start) & (hist.index.date <= end)
        return hist.loc[mask, "Close"]

    @staticmethod
    def clear_cache():
        """Clear all in-memory caches (useful between runs)."""
        _RAW_CACHE.clear()
        _PRICE_CACHE.clear()


# ═══════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════

def _subtract_months(d: date, months: int) -> date:
    """Subtract *months* from date *d*."""
    month = d.month - months
    year = d.year
    while month <= 0:
        month += 12
        year -= 1
    day = min(d.day, 28)  # safe day
    return date(year, month, day)
