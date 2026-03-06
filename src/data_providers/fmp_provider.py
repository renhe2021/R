"""Financial Modeling Prep (FMP) 数据源 - 免费/低成本的金融数据 API

FMP 提供全球股票的基本面、财务报表、历史价格等数据。
免费 tier 每天 250 次请求，覆盖 US 股票。

API Key 申请: https://financialmodelingprep.com/developer/docs/
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Optional

from .base import DataProvider
from ..analyzer import StockData

logger = logging.getLogger(__name__)

# FMP API 基础 URL
FMP_BASE_URL = "https://financialmodelingprep.com/api/v3"


class FMPProvider(DataProvider):
    """通过 Financial Modeling Prep API 获取股票数据"""

    def __init__(self, api_key: str = ""):
        self._api_key = api_key

    @property
    def name(self) -> str:
        return "fmp"

    def is_available(self) -> bool:
        try:
            import requests  # noqa: F401
            return bool(self._api_key)
        except ImportError:
            return False

    def fetch(self, symbol: str) -> StockData:
        import requests

        logger.info(f"[FMP] Fetching data for {symbol}...")
        stock = StockData(symbol=symbol)

        # 1. 公司概况 + 实时报价
        profile = self._get(f"/profile/{symbol}")
        quote = self._get(f"/quote/{symbol}")
        if profile:
            self._populate_profile(stock, profile[0] if isinstance(profile, list) and profile else profile)
        if quote:
            self._populate_quote(stock, quote[0] if isinstance(quote, list) and quote else quote)

        # 2. 关键指标 (TTM)
        ratios_ttm = self._get(f"/ratios-ttm/{symbol}")
        if ratios_ttm:
            self._populate_ratios(stock, ratios_ttm[0] if isinstance(ratios_ttm, list) and ratios_ttm else ratios_ttm)

        # 3. 关键指标 (年度) - 用于历史分析
        key_metrics = self._get(f"/key-metrics/{symbol}", params={"period": "annual", "limit": 10})
        if key_metrics:
            self._populate_key_metrics(stock, key_metrics)

        # 4. 财务报表 (资产负债表、利润表)
        balance_sheet = self._get(f"/balance-sheet-statement/{symbol}", params={"period": "annual", "limit": 10})
        income_stmt = self._get(f"/income-statement/{symbol}", params={"period": "annual", "limit": 10})
        cash_flow = self._get(f"/cash-flow-statement/{symbol}", params={"period": "annual", "limit": 10})

        if balance_sheet:
            self._populate_balance_sheet(stock, balance_sheet)
        if income_stmt:
            self._populate_income_stmt(stock, income_stmt)
        if cash_flow:
            self._populate_cash_flow(stock, cash_flow)

        # 5. 历史指标计算
        if income_stmt:
            self._compute_historical_metrics(stock, income_stmt, balance_sheet or [], cash_flow or [])

        # 6. 技术指标
        self._compute_technical_indicators(stock)

        # 7. 市场基准
        self._fetch_market_benchmarks(stock)

        # 8. 衍生指标
        self._compute_derived_metrics(stock)

        logger.info(f"[FMP] Data fetched: {stock.name} ({stock.symbol})")
        return stock

    def _get(self, endpoint: str, params: Optional[dict] = None) -> Optional[dict]:
        """发送 FMP API 请求"""
        import requests

        url = f"{FMP_BASE_URL}{endpoint}"
        p = {"apikey": self._api_key}
        if params:
            p.update(params)
        try:
            resp = requests.get(url, params=p, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            # FMP 错误格式: {"Error Message": "..."}
            if isinstance(data, dict) and "Error Message" in data:
                logger.warning(f"[FMP] API error: {data['Error Message']}")
                return None
            return data
        except Exception as e:
            logger.warning(f"[FMP] Request failed {endpoint}: {e}")
            return None

    def _populate_profile(self, stock: StockData, p: dict):
        """从公司概况填充"""
        stock.name = p.get("companyName", stock.symbol)
        stock.sector = p.get("sector", "")
        stock.industry = p.get("industry", "")
        stock.market_cap = _num(p.get("mktCap"))
        stock.price = _num(p.get("price"))
        stock.price_52w_high = _num(p.get("range", "").split("-")[-1]) if p.get("range") else 0.0
        stock.price_52w_low = _num(p.get("range", "").split("-")[0]) if p.get("range") else 0.0
        stock.book_value = _num(p.get("bookValue"))  # may be None in profile
        stock.dividend_yield = (_num(p.get("lastDiv")) or 0) / stock.price * 100 if stock.price and p.get("lastDiv") else None

    def _populate_quote(self, stock: StockData, q: dict):
        """从实时报价填充"""
        stock.price = _num(q.get("price")) or stock.price
        stock.pe = _num(q.get("pe"))
        stock.eps = _num(q.get("eps"))
        stock.market_cap = _num(q.get("marketCap")) or stock.market_cap
        stock.shares_outstanding = _num(q.get("sharesOutstanding"))
        stock.price_52w_high = _num(q.get("yearHigh")) or stock.price_52w_high
        stock.price_52w_low = _num(q.get("yearLow")) or stock.price_52w_low
        stock.revenue = _num(q.get("revenue"))

    def _populate_ratios(self, stock: StockData, r: dict):
        """从 TTM 比率填充"""
        stock.pe = _num(r.get("peRatioTTM")) or stock.pe
        stock.forward_pe = _num(r.get("forwardPeRatioTTM")) or stock.forward_pe
        stock.pb = _num(r.get("priceToBookRatioTTM"))
        stock.ps = _num(r.get("priceToSalesRatioTTM"))
        stock.roe = _num(r.get("returnOnEquityTTM"))
        stock.current_ratio = _num(r.get("currentRatioTTM"))
        stock.debt_to_equity = _num(r.get("debtEquityRatioTTM"))
        stock.profit_margin = _num(r.get("netProfitMarginTTM"))
        stock.operating_margin = _num(r.get("operatingProfitMarginTTM"))
        stock.dividend_yield = (_num(r.get("dividendYieldTTM")) or 0) * 100 if r.get("dividendYieldTTM") else stock.dividend_yield
        stock.dividend_payout_ratio = (_num(r.get("payoutRatioTTM")) or 0) * 100 if r.get("payoutRatioTTM") else None
        if stock.pe and stock.pe > 0:
            stock.earnings_yield = 100.0 / stock.pe
        stock.interest_coverage_ratio = _num(r.get("interestCoverageTTM"))
        stock.free_cash_flow = (_num(r.get("freeCashFlowPerShareTTM")) or 0) * stock.shares_outstanding if r.get("freeCashFlowPerShareTTM") and stock.shares_outstanding else stock.free_cash_flow

    def _populate_key_metrics(self, stock: StockData, metrics: list):
        """从关键指标填充额外数据"""
        if not metrics:
            return
        latest = metrics[0]
        stock.book_value = _num(latest.get("bookValuePerShare")) or stock.book_value
        stock.tangible_book_value = _num(latest.get("tangibleBookValuePerShare"))
        stock.revenue_growth_rate = _num(latest.get("revenuePerShare"))  # not exactly growth rate
        stock.enterprise_value = _num(latest.get("enterpriseValue")) or stock.enterprise_value
        stock.dividend_per_share = (_num(latest.get("dividendYield")) or 0) * stock.price if latest.get("dividendYield") and stock.price else None

    def _populate_balance_sheet(self, stock: StockData, bs_list: list):
        """从资产负债表填充"""
        if not bs_list:
            return
        latest = bs_list[0]
        stock.total_assets = _num(latest.get("totalAssets")) or stock.total_assets
        stock.total_equity = _num(latest.get("totalStockholdersEquity"))
        stock.current_assets = _num(latest.get("totalCurrentAssets"))
        stock.current_liabilities = _num(latest.get("totalCurrentLiabilities"))
        stock.long_term_debt = _num(latest.get("longTermDebt"))
        stock.total_liabilities = _num(latest.get("totalLiabilities"))
        stock.total_debt = _num(latest.get("totalDebt")) or stock.total_debt
        stock.total_cash = _num(latest.get("cashAndCashEquivalents"))
        stock.net_income = _num(latest.get("retainedEarnings"))  # approximate

        if stock.current_assets and stock.current_liabilities:
            stock.working_capital = stock.current_assets - stock.current_liabilities

        if stock.total_debt and stock.total_assets and stock.total_assets > 0:
            stock.debt_to_assets = stock.total_debt / stock.total_assets

    def _populate_income_stmt(self, stock: StockData, inc_list: list):
        """从利润表填充"""
        if not inc_list:
            return
        latest = inc_list[0]
        stock.revenue = _num(latest.get("revenue")) or stock.revenue
        stock.annual_sales = stock.revenue
        stock.net_income = _num(latest.get("netIncome")) or stock.net_income
        stock.ebit = _num(latest.get("operatingIncome"))
        stock.pretax_income = _num(latest.get("incomeBeforeTax"))
        interest_expense = _num(latest.get("interestExpense"))
        if stock.ebit and interest_expense and interest_expense > 0:
            stock.interest_coverage_ratio = stock.interest_coverage_ratio or (stock.ebit / interest_expense)

    def _populate_cash_flow(self, stock: StockData, cf_list: list):
        """从现金流表填充"""
        if not cf_list:
            return
        latest = cf_list[0]
        stock.free_cash_flow = _num(latest.get("freeCashFlow")) or stock.free_cash_flow
        stock.capex = _num(latest.get("capitalExpenditure"))

    def _compute_historical_metrics(self, stock: StockData, income_stmts: list, balance_sheets: list, cash_flows: list):
        """从历史报表计算10年指标"""
        try:
            # EPS 序列 (从旧到新)
            eps_series = []
            for stmt in reversed(income_stmts):
                eps_val = _num(stmt.get("eps")) or _num(stmt.get("epsdiluted"))
                if eps_val is not None:
                    eps_series.append(eps_val)

            if eps_series:
                stock.avg_eps_10y = sum(eps_series) / len(eps_series)
                stock.eps_history = eps_series

                # EPS CAGR
                if len(eps_series) >= 2 and eps_series[0] > 0 and eps_series[-1] > 0:
                    years = len(eps_series) - 1
                    stock.earnings_growth_10y = (eps_series[-1] / eps_series[0]) ** (1.0 / years) - 1

                # EPS 5年增长
                if len(eps_series) >= 6:
                    e_now, e_5y = eps_series[-1], eps_series[-6]
                    if e_5y > 0 and e_now > 0:
                        stock.eps_growth_5y = (e_now / e_5y) ** (1.0 / 5) - 1
                elif len(eps_series) >= 5:
                    e_now, e_5y = eps_series[-1], eps_series[0]
                    yrs = len(eps_series) - 1
                    if e_5y > 0 and e_now > 0:
                        stock.eps_growth_5y = (e_now / e_5y) ** (1.0 / yrs) - 1

                # 3年平均
                if len(eps_series) >= 3:
                    stock.avg_eps_3y = sum(eps_series[-3:]) / 3
                if len(eps_series) >= 6:
                    stock.avg_eps_first_3y = sum(eps_series[:3]) / 3

                # 盈利年数
                stock.profitable_years = sum(1 for e in eps_series if e and e > 0)
                stock.min_annual_eps_10y = min(eps_series) if eps_series else 0.0
                if len(eps_series) >= 5:
                    stock.min_annual_eps_5y = min(eps_series[-5:])

                # 连续盈利
                consec = 0
                for e in reversed(eps_series):
                    if e and e > 0:
                        consec += 1
                    else:
                        break
                stock.consecutive_profitable_years = consec

                # 最大下降
                max_decline = 0.0
                for i in range(1, len(eps_series)):
                    if eps_series[i - 1] and eps_series[i - 1] > 0:
                        decline = (eps_series[i - 1] - eps_series[i]) / eps_series[i - 1]
                        max_decline = max(max_decline, decline)
                stock.max_eps_decline = max_decline

                # 5年前 EPS
                if len(eps_series) >= 6:
                    stock.__dict__['eps_5y_ago'] = eps_series[-6]

            # 收入序列
            revenue_series = []
            for stmt in reversed(income_stmts):
                rev = _num(stmt.get("revenue"))
                if rev and rev > 0:
                    revenue_series.append(rev)
            if revenue_series and len(revenue_series) >= 2:
                if revenue_series[0] > 0 and revenue_series[-1] > 0:
                    years = len(revenue_series) - 1
                    stock.revenue_cagr_10y = (revenue_series[-1] / revenue_series[0]) ** (1.0 / years) - 1

            # 分红连续年数
            dividend_series = []
            for stmt in reversed(income_stmts):
                # FMP income stmt doesn't have dividends directly
                # We'll use cash flow dividendsPaid
                pass
            for cf in reversed(cash_flows):
                div_paid = abs(_num(cf.get("dividendsPaid")))
                dividend_series.append(div_paid)
            if dividend_series:
                consec_div = 0
                for d in reversed(dividend_series):
                    if d and d > 0:
                        consec_div += 1
                    else:
                        break
                stock.consecutive_dividend_years = consec_div

            # 账面价值增长
            bv_series = []
            for bs in reversed(balance_sheets):
                eq = _num(bs.get("totalStockholdersEquity"))
                shares = _num(bs.get("commonStock")) or stock.shares_outstanding
                if eq and shares and shares > 0:
                    bv_series.append(eq / shares)
            if bv_series and len(bv_series) >= 2:
                if bv_series[0] > 0 and bv_series[-1] > 0:
                    years = len(bv_series) - 1
                    stock.book_value_growth = (bv_series[-1] / bv_series[0]) ** (1.0 / years) - 1

            # 利息覆盖率历史
            ic_series = []
            for stmt in reversed(income_stmts):
                ebit = _num(stmt.get("operatingIncome"))
                interest = _num(stmt.get("interestExpense"))
                if ebit and interest and interest > 0:
                    ic_series.append(ebit / interest)
            if ic_series:
                if len(ic_series) >= 7:
                    recent7 = ic_series[-7:]
                    stock.avg_7y_pretax_interest_coverage = sum(recent7) / len(recent7)
                    stock.worst_year_pretax_interest_coverage = min(recent7)
                else:
                    stock.avg_7y_pretax_interest_coverage = sum(ic_series) / len(ic_series)
                    stock.worst_year_pretax_interest_coverage = min(ic_series)

        except Exception as e:
            logger.warning(f"[FMP] Historical metrics computation failed: {e}")

    def _compute_technical_indicators(self, stock: StockData):
        """计算技术指标 (RSI, MACD, MA200)"""
        import requests

        try:
            # 获取1年历史价格
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            data = self._get(
                f"/historical-price-full/{stock.symbol}",
                params={"from": start_date, "to": end_date}
            )
            if not data or "historical" not in data:
                return

            prices = data["historical"]
            if len(prices) < 26:
                return

            # 按日期排序 (旧到新)
            prices.sort(key=lambda x: x["date"])
            closes = [p["close"] for p in prices]

            # MA 200
            if len(closes) >= 200:
                stock.ma_200d = sum(closes[-200:]) / 200
            elif len(closes) >= 50:
                stock.ma_200d = sum(closes) / len(closes)

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
                ema12 = self._ema(closes, 12)
                ema26 = self._ema(closes, 26)
                macd_line = [e12 - e26 for e12, e26 in zip(ema12[-len(ema26):], ema26)]
                signal_line = self._ema(macd_line, 9) if len(macd_line) >= 9 else []

                if macd_line:
                    stock.macd_line = macd_line[-1]
                if signal_line:
                    stock.macd_signal = signal_line[-1]
                    stock.macd_hist = macd_line[-1] - signal_line[-1]

        except Exception as e:
            logger.warning(f"[FMP] Technical indicators failed: {e}")

    def _fetch_market_benchmarks(self, stock: StockData):
        """获取市场基准"""
        try:
            # S&P 500 PE
            spy_quote = self._get("/quote/SPY")
            if spy_quote and isinstance(spy_quote, list) and spy_quote:
                stock.market_pe = _num(spy_quote[0].get("pe"))

            # 10年国债
            treasury = self._get("/treasury", params={"from": datetime.now().strftime("%Y-%m-%d"), "to": datetime.now().strftime("%Y-%m-%d")})
            if treasury and isinstance(treasury, list) and treasury:
                stock.treasury_yield_10y = _num(treasury[0].get("year10"))
                if stock.treasury_yield_10y:
                    stock.aa_bond_yield = stock.treasury_yield_10y + 1.0
        except Exception as e:
            logger.warning(f"[FMP] Market benchmarks failed: {e}")

    def _compute_derived_metrics(self, stock: StockData):
        """计算衍生指标"""
        # Graham Number
        if stock.eps is not None and stock.eps > 0 and stock.book_value is not None and stock.book_value > 0:
            stock.graham_number = math.sqrt(22.5 * stock.eps * stock.book_value)

        # NCAV per share
        if stock.current_assets and stock.total_liabilities and stock.shares_outstanding:
            stock.net_current_assets = stock.current_assets - stock.total_liabilities
            stock.ncav_per_share = stock.net_current_assets / stock.shares_outstanding

        # 内在价值 (Graham: V = EPS * (8.5 + 2g) * 4.4 / Y)
        if stock.eps is not None and stock.eps > 0:
            g = (stock.eps_growth_5y or stock.earnings_growth_10y or 0.0) * 100
            y = stock.aa_bond_yield or stock.treasury_yield_10y or 4.4
            if y > 0:
                stock.intrinsic_value = stock.eps * (8.5 + 2 * g) * 4.4 / y

        # 安全边际
        if stock.intrinsic_value is not None and stock.intrinsic_value > 0 and stock.price is not None and stock.price > 0:
            stock.margin_of_safety = (stock.intrinsic_value - stock.price) / stock.intrinsic_value

        stock.book_value_equity = stock.total_equity
        stock.annual_sales = stock.revenue

        if stock.avg_eps_3y is not None and stock.avg_eps_3y > 0 and stock.price is not None and stock.price > 0:
            stock.eps_3yr_avg_to_price = stock.price / stock.avg_eps_3y

    @staticmethod
    def _ema(data: list, period: int) -> list:
        """计算指数移动平均"""
        if len(data) < period:
            return []
        multiplier = 2 / (period + 1)
        ema = [sum(data[:period]) / period]
        for val in data[period:]:
            ema.append((val - ema[-1]) * multiplier + ema[-1])
        return ema


def _num(val, default: float = None) -> Optional[float]:
    """安全数值转换 — 返回 None 而非 0.0 来表示缺失"""
    if val is None:
        return default
    try:
        result = float(val)
        if math.isnan(result) or math.isinf(result):
            return default
        return result
    except (ValueError, TypeError):
        # 可能是字符串如 "123.45 - 234.56"
        if isinstance(val, str):
            val = val.strip()
            try:
                return float(val)
            except ValueError:
                return default
        return default
