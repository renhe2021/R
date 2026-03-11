"""底层原始数据获取抽象层 — 统一回测与实时分析的数据源。

设计目标：
    HistoricalDataFetcher（回测）和 DataProvider（实时分析）共享底层数据获取逻辑。
    build_snapshot() 需要的 DataFrame 格式统一（行标签一致），不管底层是 Bloomberg 还是 yfinance。

数据源优先级：
    Bloomberg（BDH QUARTERLY）优先，yfinance 作为 fallback。

线程安全：
    BloombergRawSource 复用 BloombergProvider 的单例 session 和 _lock，
    所有 BDP/BDH 请求自动串行化。
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from datetime import date, timedelta
from typing import Any, Dict, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  Bloomberg BDH 字段 → yfinance DataFrame 行标签 映射
# ═══════════════════════════════════════════════════════════════

# Income Statement (季度)
_BBG_INCOME_FIELDS = [
    "SALES_REV_TURN",           # → Total Revenue
    "NET_INCOME",               # → Net Income
    "IS_OPER_INC",              # → EBIT (Operating Income)
    "IS_EPS",                   # → Basic EPS
    "IS_INT_EXPENSE",           # → Interest Expense
    "IS_COGS_TO_FE_AND_PP_AND_G",  # → Cost Of Revenue
]

_BBG_INCOME_TO_YF = {
    "SALES_REV_TURN": "Total Revenue",
    "NET_INCOME": "Net Income",
    "IS_OPER_INC": "EBIT",
    "IS_EPS": "Basic EPS",
    "IS_INT_EXPENSE": "Interest Expense",
    "IS_COGS_TO_FE_AND_PP_AND_G": "Cost Of Revenue",
}

# Balance Sheet (季度)
_BBG_BALANCE_FIELDS = [
    "BS_TOT_ASSET",             # → Total Assets
    "BS_CUR_ASSET_REPORT",      # → Current Assets
    "BS_CUR_LIAB",              # → Current Liabilities
    "BS_TOT_LIAB2",             # → Total Liabilities
    "TOTAL_EQUITY",             # → Stockholders Equity
    "BS_LT_BORROW",            # → Long Term Debt
    "EQY_SH_OUT",               # → Ordinary Shares Number
    "BS_RETAIN_EARN",           # → Retained Earnings
    "BS_TOT_LIAB2",             # → Total Liabilities Net Minority Interest
]

_BBG_BALANCE_TO_YF = {
    "BS_TOT_ASSET": "Total Assets",
    "BS_CUR_ASSET_REPORT": "Current Assets",
    "BS_CUR_LIAB": "Current Liabilities",
    "BS_TOT_LIAB2": "Total Liabilities",
    "TOTAL_EQUITY": "Stockholders Equity",
    "BS_LT_BORROW": "Long Term Debt",
    "EQY_SH_OUT": "Ordinary Shares Number",
    "BS_RETAIN_EARN": "Retained Earnings",
}

# Cash Flow (季度)
_BBG_CASHFLOW_FIELDS = [
    "CF_FREE_CASH_FLOW",            # → Free Cash Flow
    "CF_CASH_FROM_OPER",            # → Operating Cash Flow
    "CAPITAL_EXPEND",               # → Capital Expenditure
]

_BBG_CASHFLOW_TO_YF = {
    "CF_FREE_CASH_FLOW": "Free Cash Flow",
    "CF_CASH_FROM_OPER": "Operating Cash Flow",
    "CAPITAL_EXPEND": "Capital Expenditure",
}

# 去重后的 Bloomberg 字段列表
_BBG_BALANCE_FIELDS_UNIQUE = list(dict.fromkeys(_BBG_BALANCE_FIELDS))

# ── Bloomberg BDH 字段单位缩放 ──
# Bloomberg BDH 返回的财务报表数据通常以"百万"为单位（如 Total Assets = 379,297 = $379.3B），
# 而 yfinance 返回的是实际金额（如 Total Assets = 379,297,000,000）。
# 以下字段是"每股"或"比率"类型，**不需要** ×1,000,000 缩放：
_BBG_NO_SCALE_FIELDS = {
    "IS_EPS",           # 每股收益（实际美元）
    "PX_LAST",          # 价格（实际美元）
    "EQY_DVD_YLD_IND",  # 股息率（百分比）
    "DVD_SH_LAST",      # 每股股息（实际美元）
}
# 所有其他财务报表字段（Total Assets、Revenue、Net Income、EQY_SH_OUT 等）
# 都以"百万"为单位，需要乘以 1,000,000 转换为实际值。


class RawDataSource(ABC):
    """回测和实时分析共用的底层原始数据获取接口。

    返回的 DataFrame 格式遵循 yfinance 的约定：
    - 行标签 = 字段名（如 "Total Revenue", "Net Income"）
    - 列标签 = 日期（pd.Timestamp，最新在左）
    """

    @abstractmethod
    def fetch_quarterly_financials(
        self, symbol: str, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """获取季度财务报表。

        Returns:
            dict with keys:
                "income":   pd.DataFrame (rows=字段, cols=季度日期)
                "balance":  pd.DataFrame
                "cashflow": pd.DataFrame
                "info":     dict (基本信息如 currentPrice, sharesOutstanding 等)
        """
        ...

    @abstractmethod
    def fetch_price_history(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """获取日线收盘价。

        Returns:
            pd.DataFrame with DatetimeIndex, columns containing "Close".
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """数据源名称，用于日志和缓存键。"""
        ...


# ═══════════════════════════════════════════════════════════════
#  Bloomberg 实现
# ═══════════════════════════════════════════════════════════════

class BloombergRawSource(RawDataSource):
    """通过 Bloomberg BDH (QUARTERLY) 获取季度财报，BDP 获取 info。

    复用已有的 BloombergProvider 单例的 session 和 _lock，
    不会创建额外的 blpapi 连接。
    """

    def __init__(self, bloomberg_provider):
        """
        Args:
            bloomberg_provider: BloombergProvider 实例（通常从 factory 单例获取）。
                                调用其 _bdh / _bdp 方法。
        """
        self._provider = bloomberg_provider

    @property
    def name(self) -> str:
        return "bloomberg"

    def fetch_quarterly_financials(
        self, symbol: str, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """用 BDH QUARTERLY 拉季度财报，BDP 拉 info，返回兼容 yfinance 格式的 DataFrame。"""
        security = self._provider._to_bbg_ticker(symbol)
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        logger.info(f"[Bloomberg-Raw] 获取 {security} 季度财报 {start_date}~{end_date}")

        # ── 在 Bloomberg session lock 下执行所有 BDH/BDP 请求 ──
        with self._provider._lock:
            # 1. Income Statement (QUARTERLY)
            inc_rows = self._provider._bdh(
                security, _BBG_INCOME_FIELDS, start_str, end_str,
                periodicity="QUARTERLY"
            )

            # 2. Balance Sheet (QUARTERLY)
            bs_rows = self._provider._bdh(
                security, _BBG_BALANCE_FIELDS_UNIQUE, start_str, end_str,
                periodicity="QUARTERLY"
            )

            # 3. Cash Flow (QUARTERLY)
            cf_rows = self._provider._bdh(
                security, _BBG_CASHFLOW_FIELDS, start_str, end_str,
                periodicity="QUARTERLY"
            )

            # 4. Info (BDP — 当前快照)
            from .bloomberg import BBG_FIELD_MAP
            bdp_fields = ["SECURITY_NAME", "PX_LAST", "EQY_SH_OUT",
                          "CUR_MKT_CAP", "EQY_DVD_YLD_IND", "DVD_SH_LAST"]
            ref_data = self._provider._bdp(security, bdp_fields)

        # ── 转换为 yfinance 兼容 DataFrame ──
        income_df = self._rows_to_df(inc_rows, _BBG_INCOME_TO_YF)
        balance_df = self._rows_to_df(bs_rows, _BBG_BALANCE_TO_YF)
        cashflow_df = self._rows_to_df(cf_rows, _BBG_CASHFLOW_TO_YF)

        # ── 构建 info dict ──
        # EQY_SH_OUT 单位修复：Bloomberg BDP 返回百万股 → 乘以 1,000,000 转为实际股数
        shares_raw = ref_data.get("EQY_SH_OUT")
        shares_actual = shares_raw * 1_000_000 if shares_raw is not None else None

        info = {
            "shortName": ref_data.get("SECURITY_NAME", symbol),
            "currentPrice": ref_data.get("PX_LAST"),
            "regularMarketPrice": ref_data.get("PX_LAST"),
            "sharesOutstanding": shares_actual,
            "marketCap": ref_data.get("CUR_MKT_CAP"),
            "dividendYield": ref_data.get("EQY_DVD_YLD_IND"),
            "dividendRate": ref_data.get("DVD_SH_LAST"),
        }

        logger.info(
            f"[Bloomberg-Raw] {security}: "
            f"income={len(inc_rows)}q, balance={len(bs_rows)}q, "
            f"cashflow={len(cf_rows)}q"
        )

        return {
            "income": income_df,
            "balance": balance_df,
            "cashflow": cashflow_df,
            "info": info,
        }

    def fetch_price_history(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """用 BDH DAILY 获取日线收盘价。"""
        security = self._provider._to_bbg_ticker(symbol)
        start_str = start_date.strftime("%Y%m%d")
        end_str = end_date.strftime("%Y%m%d")

        logger.debug(f"[Bloomberg-Raw] 获取 {security} 价格历史 {start_date}~{end_date}")

        with self._provider._lock:
            rows = self._provider._bdh(
                security, ["PX_LAST"], start_str, end_str,
                periodicity="DAILY"
            )

        if not rows:
            logger.warning(f"[Bloomberg-Raw] {security} 价格历史为空")
            return pd.DataFrame()

        # 转为 DataFrame，index=date, columns=[Close]
        records = []
        for r in rows:
            d = r.get("date")
            px = r.get("PX_LAST")
            if d and px is not None:
                records.append({"date": pd.Timestamp(d), "Close": float(px)})

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records).set_index("date").sort_index()
        return df

    @staticmethod
    def _rows_to_df(
        rows: list, field_map: dict
    ) -> pd.DataFrame:
        """将 BDH 返回的 [{date, field1, field2, ...}, ...] 转为 yfinance 格式 DataFrame。

        yfinance 格式：行=字段名, 列=日期（最新在左）。

        **关键修复**：Bloomberg BDH QUARTERLY 可能把同一个季度的不同字段返回在不同日期上。
        例如 Balance Sheet 的大多数字段返回在 2025-12-27（实际季末日期），
        但 ``EQY_SH_OUT`` 返回在 2025-12-31（月末标准日期）。
        本方法将 ±7 天内的日期合并为同一列（以拥有最多字段的日期为锚点），
        确保 ``_get_latest_col_before()`` 能拿到同一季度的所有字段。

        **单位修复**：Bloomberg BDH 的财务报表字段（Total Assets、Revenue、
        Net Income、EQY_SH_OUT 等）以"百万"为单位，此处全部乘以 1,000,000
        转换为实际值，与 yfinance 约定一致。
        每股类字段（IS_EPS 等）不缩放。
        """
        if not rows:
            return pd.DataFrame()

        # ── Step 1: 收集每条 BDH row 的日期和映射后的字段 ──
        raw_entries: list = []  # [(pd.Timestamp, {yf_label: value})]
        for r in rows:
            d = r.get("date")
            if not d:
                continue
            ts = pd.Timestamp(d)
            mapped = {}
            for bbg_field, yf_label in field_map.items():
                val = r.get(bbg_field)
                if val is not None:
                    try:
                        fval = float(val)
                        # 单位缩放：Bloomberg 百万 → 实际值
                        # 除了 per-share/ratio 字段外，全部 ×1,000,000
                        if bbg_field not in _BBG_NO_SCALE_FIELDS:
                            fval *= 1_000_000
                        mapped[yf_label] = fval
                    except (TypeError, ValueError):
                        pass
            if mapped:
                raw_entries.append((ts, mapped))

        if not raw_entries:
            return pd.DataFrame()

        # ── Step 2: 合并同季度的临近日期（±7天） ──
        # 按日期排序，然后将 7 天内的日期聚合到同一个 "锚点" 日期
        raw_entries.sort(key=lambda x: x[0])

        merged: Dict[pd.Timestamp, Dict[str, float]] = {}
        # anchor_map: 每个原始日期 → 合并后的锚点日期
        anchor_for: Dict[pd.Timestamp, pd.Timestamp] = {}

        for ts, fields in raw_entries:
            # 找到是否有已存在的锚点在 ±7 天内
            best_anchor = None
            for existing_anchor in merged:
                if abs((ts - existing_anchor).days) <= 7:
                    best_anchor = existing_anchor
                    break

            if best_anchor is not None:
                # 合并到已有锚点：新字段补充进去（不覆盖已有的）
                for label, val in fields.items():
                    if label not in merged[best_anchor]:
                        merged[best_anchor][label] = val
                anchor_for[ts] = best_anchor
            else:
                # 新锚点
                merged[ts] = dict(fields)
                anchor_for[ts] = ts

        if not merged:
            return pd.DataFrame()

        # ── Step 3: 构建 DataFrame：行=字段名，列=日期（最新在左） ──
        all_labels = list(dict.fromkeys(
            label for fields in merged.values() for label in fields
        ))
        dates_sorted = sorted(merged.keys(), reverse=True)  # 最新在左

        df = pd.DataFrame(index=all_labels, columns=dates_sorted, dtype=float)
        for ts, fields in merged.items():
            for label, val in fields.items():
                df.loc[label, ts] = val

        return df


# ═══════════════════════════════════════════════════════════════
#  yfinance 实现
# ═══════════════════════════════════════════════════════════════

class YFinanceRawSource(RawDataSource):
    """通过 yfinance 获取季度财报和价格历史。

    封装了原 historical_data.py 中 _fetch_raw() 和 _fetch_price_history() 的逻辑。
    """

    @property
    def name(self) -> str:
        return "yfinance"

    def fetch_quarterly_financials(
        self, symbol: str, start_date: date, end_date: date
    ) -> Dict[str, Any]:
        """获取 yfinance 季度财报。"""
        import yfinance as yf

        logger.info(f"[YFinance-Raw] 获取 {symbol} 季度财报")
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

        q_count = max(
            len(inc.columns) if inc is not None and not inc.empty else 0,
            len(bs.columns) if bs is not None and not bs.empty else 0,
            0,
        )
        logger.info(f"[YFinance-Raw] {symbol}: {q_count} quarters available")

        return {
            "income": inc if inc is not None else pd.DataFrame(),
            "balance": bs if bs is not None else pd.DataFrame(),
            "cashflow": cf if cf is not None else pd.DataFrame(),
            "info": info,
        }

    def fetch_price_history(
        self, symbol: str, start_date: date, end_date: date
    ) -> pd.DataFrame:
        """获取 yfinance 日线价格历史。"""
        import yfinance as yf

        logger.debug(f"[YFinance-Raw] 获取 {symbol} 价格历史 {start_date}~{end_date}")
        ticker = yf.Ticker(symbol)
        time.sleep(0.2)

        start_str = (start_date - timedelta(days=30)).strftime("%Y-%m-%d")
        end_str = (end_date + timedelta(days=5)).strftime("%Y-%m-%d")

        try:
            hist = ticker.history(start=start_str, end=end_str, auto_adjust=True)
        except Exception:
            hist = pd.DataFrame()

        return hist if hist is not None else pd.DataFrame()


# ═══════════════════════════════════════════════════════════════
#  Factory：获取最佳 RawDataSource（Bloomberg 优先）
# ═══════════════════════════════════════════════════════════════

_raw_source_instance: Optional[RawDataSource] = None
_raw_source_name: Optional[str] = None


def get_raw_source(preferred: str = "auto") -> RawDataSource:
    """获取 RawDataSource 实例（Bloomberg 优先，yfinance fallback）。

    Args:
        preferred: "bloomberg" / "yfinance" / "auto"
                   auto = 尝试 Bloomberg，不可用则降级到 yfinance

    Returns:
        RawDataSource 实例（进程级单例）
    """
    global _raw_source_instance, _raw_source_name

    # 如果已有实例且类型匹配，直接复用
    if _raw_source_instance is not None:
        if preferred == "auto" or preferred == _raw_source_name:
            return _raw_source_instance

    if preferred == "bloomberg" or preferred == "auto":
        try:
            from .factory import get_data_provider
            from .cache import CachingProvider

            provider = get_data_provider("bloomberg")
            # 解包 CachingProvider
            inner = provider
            if hasattr(inner, '_inner'):
                inner = inner._inner

            source = BloombergRawSource(inner)
            _raw_source_instance = source
            _raw_source_name = "bloomberg"
            logger.info("[RawSource] 使用 Bloomberg 数据源（BDH QUARTERLY）")
            return source
        except Exception as e:
            if preferred == "bloomberg":
                raise
            logger.info(f"[RawSource] Bloomberg 不可用: {e}，降级到 yfinance")

    # yfinance fallback
    source = YFinanceRawSource()
    _raw_source_instance = source
    _raw_source_name = "yfinance"
    logger.info("[RawSource] 使用 yfinance 数据源")
    return source


def clear_raw_source_cache():
    """清除 RawDataSource 单例缓存。"""
    global _raw_source_instance, _raw_source_name
    _raw_source_instance = None
    _raw_source_name = None
