"""Yahoo Finance Direct HTTP API 数据源 — 不依赖 yfinance 包

作为独立的第二数据源，直接通过 HTTP 调用 Yahoo Finance API，
与 yfinance 包走完全不同的代码路径，用于交叉验证。

使用 Yahoo Finance v10 quoteSummary API + cookie/crumb 认证。
"""

import logging
import math
import re
from typing import Optional

import requests

from .base import DataProvider
from ..analyzer import StockData

logger = logging.getLogger(__name__)

# Yahoo Finance API 端点
YF_BASE = "https://query2.finance.yahoo.com"
YF_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class YahooDirectProvider(DataProvider):
    """直接调用 Yahoo Finance HTTP API，不经过 yfinance 包。

    这是一个完全独立的实现，用于交叉验证 yfinance provider 的数据。
    两者都从 Yahoo Finance 拉数据，但代码路径完全不同：
    - yfinance provider: 依赖 yfinance 包的解析逻辑
    - yahoo_direct: 直接 HTTP 请求 + 自行解析 JSON + cookie/crumb 认证
    """

    def __init__(self):
        self._session: Optional[requests.Session] = None
        self._crumb: Optional[str] = None

    @property
    def name(self) -> str:
        return "yahoo_direct"

    def is_available(self) -> bool:
        try:
            self._ensure_session()
            return self._crumb is not None
        except Exception:
            return False

    def _ensure_session(self):
        """建立带 cookie 的 session 并获取 crumb"""
        if self._session is not None and self._crumb is not None:
            return

        self._session = requests.Session()
        self._session.headers.update(YF_HEADERS)

        try:
            # 步骤 1: 访问 Yahoo Finance 获取 cookie
            resp = self._session.get(
                "https://fc.yahoo.com",
                timeout=10,
                allow_redirects=True,
            )
            # 即使返回 404 也能获取 cookie

            # 步骤 2: 用 cookie 获取 crumb
            resp = self._session.get(
                "https://query2.finance.yahoo.com/v1/test/getcrumb",
                timeout=10,
            )
            if resp.ok and resp.text:
                self._crumb = resp.text.strip()
                logger.info(f"[yahoo_direct] Got crumb: {self._crumb[:8]}...")
            else:
                logger.warning(f"[yahoo_direct] Failed to get crumb: HTTP {resp.status_code}")
                self._crumb = None
        except Exception as e:
            logger.warning(f"[yahoo_direct] Session init failed: {e}")
            self._crumb = None

    def fetch(self, symbol: str) -> StockData:
        self._ensure_session()
        logger.info(f"[yahoo_direct] Fetching data for {symbol}...")
        stock = StockData(symbol=symbol)

        if not self._crumb:
            logger.warning("[yahoo_direct] No crumb available, skipping quoteSummary")
            return stock

        # 1. quoteSummary — 包含几乎所有基本面数据
        modules = [
            "price", "summaryDetail", "defaultKeyStatistics",
            "financialData", "balanceSheetHistory",
            "incomeStatementHistory", "cashflowStatementHistory",
            "earningsHistory", "earningsTrend",
        ]
        summary = self._get_quote_summary(symbol, modules)
        if summary:
            self._populate_from_summary(stock, summary)

        # 2. 历史价格（技术指标）
        self._compute_technical_indicators(stock, symbol)

        # 3. 市场基准
        self._fetch_market_benchmarks(stock)

        # 4. 衍生指标
        self._compute_derived_metrics(stock)

        logger.info(f"[yahoo_direct] Done: {stock.name} ({stock.symbol})")
        return stock

    def _get_quote_summary(self, symbol: str, modules: list) -> Optional[dict]:
        """调用 Yahoo Finance quoteSummary API (带 crumb 认证)"""
        try:
            url = f"{YF_BASE}/v10/finance/quoteSummary/{symbol}"
            resp = self._session.get(
                url,
                params={
                    "modules": ",".join(modules),
                    "crumb": self._crumb,
                },
                timeout=15,
            )
            if not resp.ok:
                logger.warning(f"[yahoo_direct] quoteSummary failed: HTTP {resp.status_code}")
                # crumb 可能过期，尝试刷新
                if resp.status_code in (401, 403):
                    self._session = None
                    self._crumb = None
                    self._ensure_session()
                    if self._crumb:
                        resp = self._session.get(
                            url,
                            params={
                                "modules": ",".join(modules),
                                "crumb": self._crumb,
                            },
                            timeout=15,
                        )
                        if not resp.ok:
                            return None
                    else:
                        return None
                else:
                    return None

            data = resp.json()
            result = data.get("quoteSummary", {}).get("result", [])
            return result[0] if result else None
        except Exception as e:
            logger.warning(f"[yahoo_direct] quoteSummary error: {e}")
            return None

    def _populate_from_summary(self, stock: StockData, summary: dict):
        """从 quoteSummary 结果填充所有字段"""
        price = summary.get("price", {})
        detail = summary.get("summaryDetail", {})
        stats = summary.get("defaultKeyStatistics", {})
        fin_data = summary.get("financialData", {})

        # === 基本信息 ===
        stock.name = _raw_str(price.get("shortName")) or _raw_str(price.get("longName")) or stock.symbol
        stock.sector = ""
        stock.industry = ""

        # === 价格 ===
        stock.price = _num(price.get("regularMarketPrice"))
        stock.market_cap = _num(price.get("marketCap"))

        # === 估值 ===
        stock.pe = _num(detail.get("trailingPE"))
        stock.forward_pe = _num(detail.get("forwardPE")) or _num(stats.get("forwardPE"))
        stock.pb = _num(stats.get("priceToBook"))
        stock.ps = _num(detail.get("priceToSalesTrailing12Months"))
        if stock.pe and stock.pe > 0:
            stock.earnings_yield = 100.0 / stock.pe

        # === 盈利 ===
        stock.eps = _num(stats.get("trailingEps"))
        stock.roe = _num(fin_data.get("returnOnEquity"))
        stock.revenue = _num(fin_data.get("totalRevenue"))
        stock.profit_margin = _num(fin_data.get("profitMargins"))
        stock.operating_margin = _num(fin_data.get("operatingMargins"))
        stock.ebit = _num(fin_data.get("ebitda"))

        # === 财务健康 ===
        stock.current_ratio = _num(fin_data.get("currentRatio"))
        de_raw = _num(fin_data.get("debtToEquity"))
        stock.debt_to_equity = (de_raw / 100.0) if de_raw is not None else None
        stock.total_debt = _num(fin_data.get("totalDebt"))
        stock.total_cash = _num(fin_data.get("totalCash"))
        stock.book_value = _num(stats.get("bookValue"))
        stock.enterprise_value = _num(stats.get("enterpriseValue"))
        stock.shares_outstanding = _num(stats.get("sharesOutstanding")) or _num(stats.get("impliedSharesOutstanding"))
        stock.free_cash_flow = _num(fin_data.get("freeCashflow"))

        # === 股息 ===
        # Yahoo v10 API dividendYield 已经是百分比值 (如 0.004 = 0.4%)
        # 但在 raw 格式中通常是小数 → 需 * 100
        dy_raw = _num(detail.get("dividendYield"))
        if dy_raw is not None:
            # 如果 < 1，则是小数形式，需要 *100；否则已经是百分比
            stock.dividend_yield = (dy_raw * 100) if dy_raw < 1 else dy_raw
        pr_raw = _num(stats.get("payoutRatio"))
        stock.dividend_payout_ratio = (pr_raw * 100) if pr_raw is not None and pr_raw else None
        stock.dividend_per_share = _num(detail.get("dividendRate"))

        # === 价格区间 ===
        stock.price_52w_high = _num(detail.get("fiftyTwoWeekHigh"))
        stock.price_52w_low = _num(detail.get("fiftyTwoWeekLow"))

        # === 增长 ===
        rg_raw = _num(fin_data.get("revenueGrowth"))
        stock.revenue_growth_rate = (rg_raw * 100) if rg_raw is not None else None
        eg_raw = _num(fin_data.get("earningsGrowth"))
        stock.eps_growth_rate = (eg_raw * 100) if eg_raw is not None else None

        # === MA 200 ===
        stock.ma_200d = _num(detail.get("twoHundredDayAverage"))

        stock.annual_sales = stock.revenue

        # === 从财务报表补充 ===
        self._populate_from_statements(stock, summary)

    def _populate_from_statements(self, stock: StockData, summary: dict):
        """从历史财务报表补充数据"""
        # 资产负债表
        bs_data = summary.get("balanceSheetHistory", {}).get("balanceSheetStatements", [])
        if bs_data:
            latest = bs_data[0]
            stock.total_assets = stock.total_assets or _num(latest.get("totalAssets"))
            stock.total_equity = _num(latest.get("totalStockholderEquity"))
            stock.current_assets = _num(latest.get("totalCurrentAssets"))
            stock.current_liabilities = _num(latest.get("totalCurrentLiabilities"))
            stock.long_term_debt = _num(latest.get("longTermDebt"))
            stock.total_liabilities = _num(latest.get("totalLiab"))

            if stock.current_assets is not None and stock.current_liabilities is not None:
                stock.working_capital = stock.current_assets - stock.current_liabilities

        # 利润表
        inc_data = summary.get("incomeStatementHistory", {}).get("incomeStatementHistory", [])
        if inc_data:
            latest = inc_data[0]
            stock.net_income = _num(latest.get("netIncome"))
            stock.ebit = stock.ebit or _num(latest.get("ebit"))
            stock.pretax_income = _num(latest.get("incomeBeforeTax"))
            interest_expense = _num(latest.get("interestExpense"))
            if stock.ebit and interest_expense and abs(interest_expense) > 0:
                stock.interest_coverage_ratio = stock.ebit / abs(interest_expense)

            # 历史 EPS 计算
            eps_series = []
            for stmt in reversed(inc_data):
                ni = _num(stmt.get("netIncome"))
                if ni is not None and stock.shares_outstanding and stock.shares_outstanding > 0:
                    eps_series.append(ni / stock.shares_outstanding)

            if eps_series:
                stock.eps_history = eps_series
                stock.avg_eps_10y = sum(eps_series) / len(eps_series)
                if len(eps_series) >= 3:
                    stock.avg_eps_3y = sum(eps_series[-3:]) / 3
                if len(eps_series) >= 2 and eps_series[0] > 0 and eps_series[-1] > 0:
                    years = len(eps_series) - 1
                    stock.earnings_growth_10y = (eps_series[-1] / eps_series[0]) ** (1.0 / years) - 1
                stock.profitable_years = sum(1 for e in eps_series if e and e > 0)
                stock.min_annual_eps_10y = min(eps_series) if eps_series else None

                consec = 0
                for e in reversed(eps_series):
                    if e and e > 0:
                        consec += 1
                    else:
                        break
                stock.consecutive_profitable_years = consec

        # 现金流
        cf_data = summary.get("cashflowStatementHistory", {}).get("cashflowStatements", [])
        if cf_data:
            latest = cf_data[0]
            stock.free_cash_flow = stock.free_cash_flow or _num(latest.get("totalCashFromOperatingActivities"))
            stock.capex = _num(latest.get("capitalExpenditures"))
            if stock.capex:
                stock.capex = abs(stock.capex)

            div_years = 0
            for stmt in cf_data:
                div_paid = _num(stmt.get("dividendsPaid"))
                if div_paid is not None and abs(div_paid) > 0:
                    div_years += 1
                else:
                    break
            stock.consecutive_dividend_years = div_years or stock.consecutive_dividend_years

        # 负债比率
        if stock.total_debt and stock.total_assets and stock.total_assets > 0:
            stock.debt_to_assets = stock.total_debt / stock.total_assets

    def _compute_technical_indicators(self, stock: StockData, symbol: str):
        """从 chart API 计算 RSI、MACD"""
        try:
            url = f"{YF_BASE}/v8/finance/chart/{symbol}"
            resp = self._session.get(
                url,
                params={
                    "range": "1y",
                    "interval": "1d",
                    "crumb": self._crumb,
                },
                timeout=15,
            )
            if not resp.ok:
                return

            data = resp.json()
            result = data.get("chart", {}).get("result", [])
            if not result:
                return

            indicators = result[0].get("indicators", {})
            quotes = indicators.get("quote", [{}])[0]
            closes = quotes.get("close", [])
            closes = [c for c in closes if c is not None]

            if len(closes) < 26:
                return

            # RSI (14天)
            if len(closes) >= 15:
                deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
                gains = [d if d > 0 else 0 for d in deltas[-14:]]
                losses = [-d if d < 0 else 0 for d in deltas[-14:]]
                avg_gain = sum(gains) / 14
                avg_loss = sum(losses) / 14
                if avg_loss > 0:
                    rs = avg_gain / avg_loss
                    stock.rsi_14d = 100 - (100 / (1 + rs))
                else:
                    stock.rsi_14d = 100.0

            # MACD (12, 26, 9)
            if len(closes) >= 35:
                ema12 = _ema(closes, 12)
                ema26 = _ema(closes, 26)
                macd_line = [e12 - e26 for e12, e26 in zip(ema12[-len(ema26):], ema26)]
                signal_line = _ema(macd_line, 9) if len(macd_line) >= 9 else []
                if macd_line:
                    stock.macd_line = macd_line[-1]
                if signal_line:
                    stock.macd_signal = signal_line[-1]
                    stock.macd_hist = macd_line[-1] - signal_line[-1]

        except Exception as e:
            logger.warning(f"[yahoo_direct] Technical indicators failed: {e}")

    def _fetch_market_benchmarks(self, stock: StockData):
        """市场基准"""
        try:
            spy_summary = self._get_quote_summary("SPY", ["summaryDetail"])
            if spy_summary:
                detail = spy_summary.get("summaryDetail", {})
                stock.market_pe = _num(detail.get("trailingPE"))
        except Exception:
            pass

        try:
            tnx_summary = self._get_quote_summary("^TNX", ["price"])
            if tnx_summary:
                price_mod = tnx_summary.get("price", {})
                stock.treasury_yield_10y = _num(price_mod.get("regularMarketPrice"))
                if stock.treasury_yield_10y:
                    stock.aa_bond_yield = stock.treasury_yield_10y + 1.0
        except Exception:
            pass

    def _compute_derived_metrics(self, stock: StockData):
        """衍生指标"""
        if stock.eps is not None and stock.eps > 0 and stock.book_value is not None and stock.book_value > 0:
            stock.graham_number = math.sqrt(22.5 * stock.eps * stock.book_value)

        if stock.current_assets and stock.total_liabilities and stock.shares_outstanding:
            stock.net_current_assets = stock.current_assets - stock.total_liabilities
            stock.ncav_per_share = stock.net_current_assets / stock.shares_outstanding

        if stock.eps is not None and stock.eps > 0:
            g = (stock.eps_growth_5y or stock.earnings_growth_10y or 0.0) * 100
            y = stock.aa_bond_yield or stock.treasury_yield_10y or 4.4
            if y > 0:
                stock.intrinsic_value = stock.eps * (8.5 + 2 * g) * 4.4 / y

        if stock.intrinsic_value is not None and stock.intrinsic_value > 0 and stock.price is not None and stock.price > 0:
            stock.margin_of_safety = (stock.intrinsic_value - stock.price) / stock.intrinsic_value

        stock.book_value_equity = stock.total_equity
        stock.annual_sales = stock.revenue

        if stock.avg_eps_3y is not None and stock.avg_eps_3y > 0 and stock.price is not None and stock.price > 0:
            stock.eps_3yr_avg_to_price = stock.price / stock.avg_eps_3y


def _raw_str(val) -> Optional[str]:
    """从 Yahoo API 的 {raw: ..., fmt: ...} 格式或直接字符串中提取字符串值"""
    if val is None:
        return None
    if isinstance(val, dict):
        return str(val.get("raw", "")) or val.get("fmt", "")
    return str(val) if val else None


def _num(val, default: float = None) -> Optional[float]:
    """安全数值转换，支持 Yahoo API 的 {raw: 123.45} 格式"""
    if val is None:
        return default
    if isinstance(val, dict):
        val = val.get("raw")
        if val is None:
            return default
    try:
        result = float(val)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ValueError, TypeError):
        return default


def _ema(data: list, period: int) -> list:
    """指数移动平均"""
    if len(data) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = [sum(data[:period]) / period]
    for val in data[period:]:
        ema.append((val - ema[-1]) * multiplier + ema[-1])
    return ema
