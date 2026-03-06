"""Finnhub 数据源 - 免费金融数据 API (60次/分钟)

Finnhub 提供全球股票的基本面指标、财务报表、估值、分红等数据。
免费 tier 60 requests/minute，无每日限制。

API Key 申请: https://finnhub.io/register
文档: https://finnhub.io/docs/api
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Optional

from .base import DataProvider
from ..analyzer import StockData

logger = logging.getLogger(__name__)


class FinnhubProvider(DataProvider):
    """通过 Finnhub API 获取股票数据"""

    def __init__(self, api_key: str = ""):
        self._api_key = api_key
        self._client = None

    @property
    def name(self) -> str:
        return "finnhub"

    def is_available(self) -> bool:
        try:
            import finnhub  # noqa: F401
            return bool(self._api_key)
        except ImportError:
            return False

    def _get_client(self):
        if self._client is None:
            import finnhub
            self._client = finnhub.Client(api_key=self._api_key)
        return self._client

    def fetch(self, symbol: str) -> StockData:
        logger.info(f"[Finnhub] Fetching data for {symbol}...")
        stock = StockData(symbol=symbol)
        client = self._get_client()

        # 1. 公司概况
        self._fetch_profile(stock, client)

        # 2. 实时报价
        self._fetch_quote(stock, client)

        # 3. 基本面指标 (PE, PB, ROE, EPS, etc.)
        self._fetch_basic_financials(stock, client)

        # 4. 财务报表 (年度)
        self._fetch_financials(stock, client)

        # 5. 分红数据
        self._fetch_dividends(stock, client)

        # 6. 技术指标
        self._compute_technical_indicators(stock, client)

        # 7. 市场基准
        self._fetch_market_benchmarks(stock, client)

        # 8. 衍生指标
        self._compute_derived_metrics(stock)

        logger.info(f"[Finnhub] Data fetched: {stock.name} ({stock.symbol})")
        return stock

    def _fetch_profile(self, stock: StockData, client):
        """公司概况"""
        try:
            p = client.company_profile2(symbol=stock.symbol)
            if p:
                stock.name = p.get("name", stock.symbol)
                stock.sector = p.get("finnhubIndustry", "")
                stock.industry = p.get("finnhubIndustry", "")
                stock.market_cap = (_num(p.get("marketCapitalization")) or 0) * 1e6  # Finnhub 返回百万
                stock.shares_outstanding = (_num(p.get("shareOutstanding")) or 0) * 1e6
        except Exception as e:
            logger.warning(f"[Finnhub] Profile failed: {e}")

    def _fetch_quote(self, stock: StockData, client):
        """实时报价"""
        try:
            q = client.quote(stock.symbol)
            if q:
                stock.price = _num(q.get("c"))  # current price
                stock.price_52w_high = _num(q.get("h"))  # day high (52w from basic_financials)
                stock.price_52w_low = _num(q.get("l"))   # day low
        except Exception as e:
            logger.warning(f"[Finnhub] Quote failed: {e}")

    def _fetch_basic_financials(self, stock: StockData, client):
        """基本面指标 — Finnhub 的核心 API，返回大量指标"""
        try:
            data = client.company_basic_financials(stock.symbol, 'all')
            if not data:
                return

            metric = data.get("metric", {})
            if not metric:
                return

            # 估值指标
            stock.pe = _num(metric.get("peNormalizedAnnual")) or _num(metric.get("peTTM"))
            stock.forward_pe = _num(metric.get("forwardPE"))
            stock.pb = _num(metric.get("pbQuarterly")) or _num(metric.get("pbAnnual"))
            stock.ps = _num(metric.get("psTTM")) or _num(metric.get("psAnnual"))
            stock.enterprise_value = _num(metric.get("enterpriseValue"))

            if stock.pe and stock.pe > 0:
                stock.earnings_yield = 100.0 / stock.pe

            # 盈利指标
            stock.eps = _num(metric.get("epsNormalizedAnnual")) or _num(metric.get("epsTTM"))
            stock.roe = _num(metric.get("roeTTM")) or _num(metric.get("roeRfy"))
            if stock.roe and abs(stock.roe) > 1:
                stock.roe = stock.roe / 100.0  # 有时返回百分比
            stock.profit_margin = _num(metric.get("netProfitMarginTTM")) or _num(metric.get("netProfitMarginAnnual"))
            if stock.profit_margin and abs(stock.profit_margin) > 1:
                stock.profit_margin = stock.profit_margin / 100.0
            stock.operating_margin = _num(metric.get("operatingMarginTTM")) or _num(metric.get("operatingMarginAnnual"))
            if stock.operating_margin and abs(stock.operating_margin) > 1:
                stock.operating_margin = stock.operating_margin / 100.0

            # 财务健康
            stock.current_ratio = _num(metric.get("currentRatioQuarterly")) or _num(metric.get("currentRatioAnnual"))
            stock.debt_to_equity = _num(metric.get("totalDebt/totalEquityQuarterly")) or _num(metric.get("totalDebt/totalEquityAnnual"))
            if stock.debt_to_equity and stock.debt_to_equity > 10:
                stock.debt_to_equity = stock.debt_to_equity / 100.0  # 归一化

            # 股息
            stock.dividend_yield = _num(metric.get("dividendYieldIndicatedAnnual")) or _num(metric.get("dividendYield5Y"))
            if stock.dividend_yield and stock.dividend_yield < 1:
                stock.dividend_yield = stock.dividend_yield * 100  # 转百分比
            stock.dividend_per_share = _num(metric.get("dividendPerShareAnnual"))
            stock.dividend_payout_ratio = _num(metric.get("payoutRatioTTM")) or _num(metric.get("payoutRatioAnnual"))

            # 账面价值
            stock.book_value = _num(metric.get("bookValuePerShareQuarterly")) or _num(metric.get("bookValuePerShareAnnual"))
            stock.tangible_book_value = _num(metric.get("tangibleBookValuePerShareQuarterly")) or _num(metric.get("tangibleBookValuePerShareAnnual"))

            # 价格区间
            stock.price_52w_high = _num(metric.get("52WeekHigh")) or stock.price_52w_high
            stock.price_52w_low = _num(metric.get("52WeekLow")) or stock.price_52w_low

            # 收入 / 市值
            stock.revenue = (_num(metric.get("revenuePerShareTTM")) or 0) * stock.shares_outstanding if metric.get("revenuePerShareTTM") and stock.shares_outstanding else stock.revenue
            stock.market_cap = stock.market_cap or ((_num(metric.get("marketCapitalization")) or 0) * 1e6)

            # 现金流
            stock.free_cash_flow = (_num(metric.get("freeCashFlowPerShareTTM")) or 0) * stock.shares_outstanding if metric.get("freeCashFlowPerShareTTM") and stock.shares_outstanding else None

            # 增长
            stock.revenue_growth_rate = _num(metric.get("revenueGrowthQuarterlyYoy")) or _num(metric.get("revenueGrowth3Y"))
            if stock.revenue_growth_rate and abs(stock.revenue_growth_rate) > 5:
                stock.revenue_growth_rate = stock.revenue_growth_rate  # 已是百分比
            stock.eps_growth_rate = _num(metric.get("epsGrowthQuarterlyYoy")) or _num(metric.get("epsGrowth3Y"))
            stock.eps_growth_5y = _num(metric.get("epsGrowth5Y"))
            if stock.eps_growth_5y and abs(stock.eps_growth_5y) > 5:
                stock.eps_growth_5y = stock.eps_growth_5y / 100.0

            # Interest coverage
            stock.interest_coverage_ratio = _num(metric.get("interestCoverageAnnual")) or _num(metric.get("interestCoverageQuarterly"))

            # 历史 EPS 和增长
            # Finnhub basic_financials 包含历史 series
            series = data.get("series", {})
            annual = series.get("annual", {})

            # EPS 序列
            eps_data = annual.get("eps", [])
            if eps_data:
                eps_series = sorted(eps_data, key=lambda x: x.get("period", ""))
                eps_values = [_num(e.get("v")) for e in eps_series if e.get("v") is not None]
                if eps_values:
                    stock.eps_history = eps_values
                    stock.avg_eps_10y = sum(eps_values) / len(eps_values)

                    if len(eps_values) >= 3:
                        stock.avg_eps_3y = sum(eps_values[-3:]) / 3

                    if len(eps_values) >= 6:
                        stock.avg_eps_first_3y = sum(eps_values[:3]) / 3

                    # EPS CAGR
                    if len(eps_values) >= 2 and eps_values[0] > 0 and eps_values[-1] > 0:
                        years = len(eps_values) - 1
                        stock.earnings_growth_10y = (eps_values[-1] / eps_values[0]) ** (1.0 / years) - 1

                    # 盈利年数
                    stock.profitable_years = sum(1 for e in eps_values if e and e > 0)
                    stock.min_annual_eps_10y = min(eps_values) if eps_values else 0.0
                    if len(eps_values) >= 5:
                        stock.min_annual_eps_5y = min(eps_values[-5:])

                    # 连续盈利
                    consec = 0
                    for e in reversed(eps_values):
                        if e and e > 0:
                            consec += 1
                        else:
                            break
                    stock.consecutive_profitable_years = consec

                    # 最大 EPS 下降
                    max_decline = 0.0
                    for i in range(1, len(eps_values)):
                        if eps_values[i - 1] and eps_values[i - 1] > 0:
                            decline = (eps_values[i - 1] - eps_values[i]) / eps_values[i - 1]
                            max_decline = max(max_decline, decline)
                    stock.max_eps_decline = max_decline

                    # 5年前 EPS
                    if len(eps_values) >= 6:
                        stock.__dict__['eps_5y_ago'] = eps_values[-6]

            # 收入序列
            rev_data = annual.get("revenue", [])
            if rev_data and len(rev_data) >= 2:
                rev_series = sorted(rev_data, key=lambda x: x.get("period", ""))
                rev_values = [_num(r.get("v")) for r in rev_series if r.get("v") is not None and _num(r.get("v")) > 0]
                if len(rev_values) >= 2 and rev_values[0] > 0:
                    years = len(rev_values) - 1
                    stock.revenue_cagr_10y = (rev_values[-1] / rev_values[0]) ** (1.0 / years) - 1

            # ROE 序列 (用于历史分析)
            roe_data = annual.get("roe", [])
            if roe_data:
                roe_series = sorted(roe_data, key=lambda x: x.get("period", ""))
                roe_values = [_num(r.get("v")) for r in roe_series if r.get("v") is not None]
                if roe_values:
                    stock.roe = stock.roe or (roe_values[-1] / 100.0 if abs(roe_values[-1]) > 1 else roe_values[-1])

        except Exception as e:
            logger.warning(f"[Finnhub] Basic financials failed: {e}")

    def _fetch_financials(self, stock: StockData, client):
        """从年度财务报表补充数据"""
        try:
            # 标准化财务报表
            data = client.financials_reported(symbol=stock.symbol, freq='annual')
            if not data or not data.get("data"):
                return

            reports = data["data"]
            if not reports:
                return

            # 按时间排序（新到旧）
            reports.sort(key=lambda x: x.get("year", 0), reverse=True)
            latest = reports[0] if reports else None

            if latest and latest.get("report"):
                report = latest["report"]

                # 资产负债表
                bs = report.get("bs", [])
                bs_dict = {item.get("concept", ""): _num(item.get("value")) for item in bs if item.get("value") is not None}

                stock.total_assets = stock.total_assets or bs_dict.get("us-gaap_Assets", 0)
                stock.total_equity = stock.total_equity or bs_dict.get("us-gaap_StockholdersEquity", 0) or bs_dict.get("us-gaap_StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", 0)
                stock.current_assets = stock.current_assets or bs_dict.get("us-gaap_AssetsCurrent", 0)
                stock.current_liabilities = stock.current_liabilities or bs_dict.get("us-gaap_LiabilitiesCurrent", 0)
                stock.long_term_debt = stock.long_term_debt or bs_dict.get("us-gaap_LongTermDebtNoncurrent", 0) or bs_dict.get("us-gaap_LongTermDebt", 0)
                stock.total_liabilities = stock.total_liabilities or bs_dict.get("us-gaap_Liabilities", 0)
                stock.total_debt = stock.total_debt or bs_dict.get("us-gaap_LongTermDebt", 0)
                stock.total_cash = stock.total_cash or bs_dict.get("us-gaap_CashAndCashEquivalentsAtCarryingValue", 0)

                if stock.current_assets and stock.current_liabilities:
                    stock.working_capital = stock.current_assets - stock.current_liabilities
                if stock.total_debt and stock.total_assets and stock.total_assets > 0:
                    stock.debt_to_assets = stock.total_debt / stock.total_assets

                # 利润表
                ic = report.get("ic", [])
                ic_dict = {item.get("concept", ""): _num(item.get("value")) for item in ic if item.get("value") is not None}

                stock.revenue = stock.revenue or ic_dict.get("us-gaap_RevenueFromContractWithCustomerExcludingAssessedTax", 0) or ic_dict.get("us-gaap_Revenues", 0) or ic_dict.get("us-gaap_SalesRevenueNet", 0)
                stock.annual_sales = stock.revenue
                stock.net_income = stock.net_income or ic_dict.get("us-gaap_NetIncomeLoss", 0)
                stock.ebit = stock.ebit or ic_dict.get("us-gaap_OperatingIncomeLoss", 0)
                stock.pretax_income = stock.pretax_income or ic_dict.get("us-gaap_IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", 0)

                interest_expense = ic_dict.get("us-gaap_InterestExpense", 0)
                if stock.ebit and interest_expense and interest_expense > 0:
                    stock.interest_coverage_ratio = stock.interest_coverage_ratio or (stock.ebit / interest_expense)

                # 现金流
                cf = report.get("cf", [])
                cf_dict = {item.get("concept", ""): _num(item.get("value")) for item in cf if item.get("value") is not None}

                stock.free_cash_flow = stock.free_cash_flow or (
                    cf_dict.get("us-gaap_NetCashProvidedByUsedInOperatingActivities", 0) -
                    abs(cf_dict.get("us-gaap_PaymentsToAcquirePropertyPlantAndEquipment", 0))
                ) or 0.0
                stock.capex = stock.capex or abs(cf_dict.get("us-gaap_PaymentsToAcquirePropertyPlantAndEquipment", 0))

            # 利息覆盖率历史 (从多年报表)
            ic_series = []
            for r in reports[:10]:
                rpt = r.get("report", {})
                ic_items = rpt.get("ic", [])
                ic_d = {item.get("concept", ""): _num(item.get("value")) for item in ic_items if item.get("value") is not None}
                ebit = ic_d.get("us-gaap_OperatingIncomeLoss", 0)
                ie = ic_d.get("us-gaap_InterestExpense", 0)
                if ebit and ie and ie > 0:
                    ic_series.append(ebit / ie)

            if ic_series:
                ic_series.reverse()
                if len(ic_series) >= 7:
                    recent7 = ic_series[-7:]
                    stock.avg_7y_pretax_interest_coverage = sum(recent7) / len(recent7)
                    stock.worst_year_pretax_interest_coverage = min(recent7)
                else:
                    stock.avg_7y_pretax_interest_coverage = sum(ic_series) / len(ic_series)
                    stock.worst_year_pretax_interest_coverage = min(ic_series)

            # 分红连续年数 (从现金流)
            div_years = 0
            for r in reports:
                rpt = r.get("report", {})
                cf_items = rpt.get("cf", [])
                cf_d = {item.get("concept", ""): _num(item.get("value")) for item in cf_items if item.get("value") is not None}
                div_paid = abs(cf_d.get("us-gaap_PaymentsOfDividendsCommonStock", 0) or cf_d.get("us-gaap_PaymentsOfDividends", 0))
                if div_paid > 0:
                    div_years += 1
                else:
                    break
            stock.consecutive_dividend_years = stock.consecutive_dividend_years or div_years

            # 账面价值增长
            bv_series = []
            for r in reversed(reports[:10]):
                rpt = r.get("report", {})
                bs_items = rpt.get("bs", [])
                bs_d = {item.get("concept", ""): _num(item.get("value")) for item in bs_items if item.get("value") is not None}
                eq = bs_d.get("us-gaap_StockholdersEquity", 0) or bs_d.get("us-gaap_StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", 0)
                if eq and stock.shares_outstanding and stock.shares_outstanding > 0:
                    bv_series.append(eq / stock.shares_outstanding)
            if bv_series and len(bv_series) >= 2 and bv_series[0] > 0 and bv_series[-1] > 0:
                years = len(bv_series) - 1
                stock.book_value_growth = (bv_series[-1] / bv_series[0]) ** (1.0 / years) - 1

        except Exception as e:
            logger.warning(f"[Finnhub] Financials reported failed: {e}")

    def _fetch_dividends(self, stock: StockData, client):
        """获取分红数据"""
        try:
            end = datetime.now().strftime("%Y-%m-%d")
            start = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            divs = client.stock_dividends(stock.symbol, _from=start, to=end)
            if divs:
                annual_div = sum(_num(d.get("amount")) for d in divs)
                if annual_div > 0 and stock.price > 0:
                    stock.dividend_yield = stock.dividend_yield or (annual_div / stock.price * 100)
                stock.dividend_per_share = stock.dividend_per_share or annual_div
        except Exception as e:
            logger.warning(f"[Finnhub] Dividends failed: {e}")

    def _compute_technical_indicators(self, stock: StockData, client):
        """计算 RSI、MACD、MA200"""
        try:
            import time
            end = int(time.time())
            start = end - 365 * 24 * 3600

            candles = client.stock_candles(stock.symbol, 'D', start, end)
            if not candles or candles.get("s") != "ok":
                return

            closes = candles.get("c", [])
            if len(closes) < 26:
                return

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
            logger.warning(f"[Finnhub] Technical indicators failed: {e}")

    def _fetch_market_benchmarks(self, stock: StockData, client):
        """市场基准"""
        try:
            # S&P 500 PE
            spy_metrics = client.company_basic_financials("SPY", 'all')
            if spy_metrics and spy_metrics.get("metric"):
                stock.market_pe = _num(spy_metrics["metric"].get("peTTM"))
        except Exception:
            pass

        try:
            # 10年国债 — Finnhub economic data
            import requests
            resp = requests.get(
                "https://finnhub.io/api/v1/economic",
                params={"code": "DGS10", "token": self._api_key},
                timeout=10
            )
            if resp.ok:
                data = resp.json()
                if data and isinstance(data, list) and data:
                    stock.treasury_yield_10y = _num(data[-1].get("value"))
                    if stock.treasury_yield_10y:
                        stock.aa_bond_yield = stock.treasury_yield_10y + 1.0
        except Exception:
            pass

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
        return default


def _ema(data: list, period: int) -> list:
    """计算指数移动平均"""
    if len(data) < period:
        return []
    multiplier = 2 / (period + 1)
    ema = [sum(data[:period]) / period]
    for val in data[period:]:
        ema.append((val - ema[-1]) * multiplier + ema[-1])
    return ema
