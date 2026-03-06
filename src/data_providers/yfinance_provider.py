"""yfinance 数据源 - 免费备选方案（含历史数据和衍生指标计算）"""

import logging
import math
from datetime import datetime, timedelta

from .base import DataProvider
from ..analyzer import StockData

logger = logging.getLogger(__name__)


class YfinanceProvider(DataProvider):
    """通过 yfinance 获取股票数据（免费，无需终端）"""

    @property
    def name(self) -> str:
        return "yfinance"

    def is_available(self) -> bool:
        try:
            import yfinance  # noqa: F401
            return True
        except ImportError:
            return False

    def fetch(self, symbol: str) -> StockData:
        import yfinance as yf

        logger.info(f"[yfinance] 获取 {symbol} 的市场数据...")
        ticker = yf.Ticker(symbol)
        info = ticker.info

        stock = StockData(symbol=symbol)
        self._populate_from_info(stock, info)
        self._populate_financials(stock, ticker)
        self._compute_historical_metrics(stock, ticker)
        self._compute_technical_indicators(stock, ticker)
        self._fetch_market_benchmarks(stock)
        self._compute_derived_metrics(stock)

        logger.info(f"[yfinance] 数据获取完成: {stock.name} ({stock.symbol})")
        return stock

    def _populate_from_info(self, stock: StockData, info: dict):
        """从 ticker.info 填充基础字段"""
        stock.name = info.get("shortName", stock.symbol)
        stock.sector = info.get("sector", "")
        stock.industry = info.get("industry", "")

        # 估值
        stock.pe = info.get("trailingPE") or None
        stock.forward_pe = info.get("forwardPE") or None
        stock.pb = info.get("priceToBook") or None
        stock.ps = info.get("priceToSalesTrailing12Months") or None
        if stock.pe and stock.pe > 0:
            stock.earnings_yield = 100.0 / stock.pe

        # 盈利
        stock.roe = info.get("returnOnEquity") or None
        stock.eps = info.get("trailingEps") or None
        stock.revenue = info.get("totalRevenue") or None
        stock.net_income = info.get("netIncomeToCommon") or None
        stock.ebit = info.get("ebitda") or None  # yfinance uses ebitda
        stock.profit_margin = info.get("profitMargins") or None
        stock.operating_margin = info.get("operatingMargins") or None

        # 财务健康
        stock.current_ratio = info.get("currentRatio") or None
        de_raw = info.get("debtToEquity")
        stock.debt_to_equity = (de_raw / 100.0) if de_raw else None
        stock.total_debt = info.get("totalDebt") or None
        stock.total_cash = info.get("totalCash") or None
        stock.market_cap = info.get("marketCap") or None
        stock.book_value = info.get("bookValue") or None
        stock.enterprise_value = info.get("enterpriseValue") or None
        stock.shares_outstanding = info.get("sharesOutstanding") or None
        stock.total_assets = info.get("totalAssets") or None
        stock.free_cash_flow = info.get("freeCashflow") or None

        # 股息 — 注意: dividendYield=0 是有效的（不分红的公司）
        # yfinance 返回的 dividendYield 已经是百分比值 (如 0.4 表示 0.4%)
        dy_raw = info.get("dividendYield")
        stock.dividend_yield = dy_raw if dy_raw is not None else None
        pr_raw = info.get("payoutRatio")
        stock.dividend_payout_ratio = (pr_raw * 100) if pr_raw is not None and pr_raw else None
        stock.dividend_per_share = info.get("dividendRate") or None

        # 价格
        stock.price = info.get("currentPrice") or info.get("regularMarketPrice") or None
        stock.price_52w_high = info.get("fiftyTwoWeekHigh") or None
        stock.price_52w_low = info.get("fiftyTwoWeekLow") or None

        # 增长
        rg_raw = info.get("revenueGrowth")
        stock.revenue_growth_rate = (rg_raw * 100) if rg_raw is not None else None
        eg_raw = info.get("earningsGrowth")
        stock.eps_growth_rate = (eg_raw * 100) if eg_raw is not None else None

        # 技术指标 - MA200
        stock.ma_200d = info.get("twoHundredDayAverage") or None

        # annual_sales = revenue
        stock.annual_sales = stock.revenue

    def _populate_financials(self, stock: StockData, ticker):
        """从财务报表提取更多字段"""
        try:
            bs = ticker.balance_sheet
            if bs is not None and not bs.empty:
                latest = bs.iloc[:, 0]  # 最新一期
                stock.total_assets = stock.total_assets if stock.total_assets is not None else self._safe_get(latest, "Total Assets")
                stock.total_equity = self._safe_get(latest, "Stockholders Equity") or self._safe_get(latest, "Total Equity Gross Minority Interest")
                stock.current_assets = self._safe_get(latest, "Current Assets")
                stock.current_liabilities = self._safe_get(latest, "Current Liabilities")
                stock.long_term_debt = self._safe_get(latest, "Long Term Debt") or self._safe_get(latest, "Long Term Debt And Capital Lease Obligation")
                stock.total_liabilities = self._safe_get(latest, "Total Liabilities Net Minority Interest") or self._safe_get(latest, "Total Liabilities")
                stock.tangible_book_value = self._safe_get(latest, "Tangible Book Value")
                if stock.tangible_book_value and stock.shares_outstanding:
                    stock.tangible_book_value = stock.tangible_book_value / stock.shares_outstanding

                # 营运资金
                if stock.current_assets is not None and stock.current_liabilities is not None:
                    stock.working_capital = stock.current_assets - stock.current_liabilities
        except Exception as e:
            logger.warning(f"[yfinance] 获取资产负债表失败: {e}")

        try:
            inc = ticker.income_stmt
            if inc is not None and not inc.empty:
                latest = inc.iloc[:, 0]
                stock.ebit = stock.ebit if stock.ebit is not None else self._safe_get(latest, "EBIT")
                stock.pretax_income = self._safe_get(latest, "Pretax Income")
                interest_expense = self._safe_get(latest, "Interest Expense")
                if stock.ebit and interest_expense and interest_expense > 0:
                    stock.interest_coverage_ratio = stock.ebit / interest_expense
        except Exception as e:
            logger.warning(f"[yfinance] 获取利润表失败: {e}")

        try:
            cf = ticker.cashflow
            if cf is not None and not cf.empty:
                latest = cf.iloc[:, 0]
                stock.free_cash_flow = stock.free_cash_flow if stock.free_cash_flow is not None else self._safe_get(latest, "Free Cash Flow")
                stock.capex = self._safe_get(latest, "Capital Expenditure")
        except Exception as e:
            logger.warning(f"[yfinance] 获取现金流表失败: {e}")

        # 负债率
        if stock.total_debt and stock.total_assets and stock.total_assets > 0:
            stock.debt_to_assets = stock.total_debt / stock.total_assets

    def _compute_historical_metrics(self, stock: StockData, ticker):
        """从历史财务数据计算10年指标"""
        try:
            # 获取历史EPS
            eps_series = self._get_annual_eps(ticker)
            revenue_series = self._get_annual_revenue(ticker)
            dividend_series = self._get_annual_dividends(ticker)
            bv_series = self._get_annual_book_value(ticker)
            ic_series = self._get_annual_interest_coverage(ticker)

            # === EPS 相关 ===
            if eps_series:
                stock.avg_eps_10y = sum(eps_series) / len(eps_series)
                stock.eps_history = eps_series

                # EPS CAGR (全期)
                if len(eps_series) >= 2 and eps_series[0] > 0 and eps_series[-1] > 0:
                    years = len(eps_series) - 1
                    stock.earnings_growth_10y = (eps_series[-1] / eps_series[0]) ** (1.0 / years) - 1

                # EPS 5年增长率
                if len(eps_series) >= 6:
                    eps_now = eps_series[-1]
                    eps_5y = eps_series[-6]
                    if eps_5y > 0 and eps_now > 0:
                        stock.eps_growth_5y = (eps_now / eps_5y) ** (1.0 / 5) - 1
                elif len(eps_series) >= 5:
                    eps_now = eps_series[-1]
                    eps_5y = eps_series[0]
                    years = len(eps_series) - 1
                    if eps_5y > 0 and eps_now > 0:
                        stock.eps_growth_5y = (eps_now / eps_5y) ** (1.0 / years) - 1

                # 最近 3 年平均 EPS
                if len(eps_series) >= 3:
                    stock.avg_eps_3y = sum(eps_series[-3:]) / 3

                # 最早 3 年平均 EPS
                if len(eps_series) >= 6:
                    stock.avg_eps_first_3y = sum(eps_series[:3]) / 3

                # 盈利年数
                stock.profitable_years = sum(1 for e in eps_series if e and e > 0)

                # 最小年度 EPS
                stock.min_annual_eps_10y = min(eps_series) if eps_series else 0.0
                if len(eps_series) >= 5:
                    stock.min_annual_eps_5y = min(eps_series[-5:])

                # 连续盈利年数
                consec = 0
                for e in reversed(eps_series):
                    if e and e > 0:
                        consec += 1
                    else:
                        break
                stock.consecutive_profitable_years = consec

                # 最大 EPS 下降
                max_decline = 0.0
                for i in range(1, len(eps_series)):
                    if eps_series[i - 1] and eps_series[i - 1] > 0:
                        decline = (eps_series[i - 1] - eps_series[i]) / eps_series[i - 1]
                        max_decline = max(max_decline, decline)
                stock.max_eps_decline = max_decline

                # 5年前 EPS
                if len(eps_series) >= 6:
                    stock.__dict__['eps_5y_ago'] = eps_series[-6]
                elif len(eps_series) >= 5:
                    stock.__dict__['eps_5y_ago'] = eps_series[0]

            # === 分红 ===
            if dividend_series:
                consecutive = 0
                for d in reversed(dividend_series):
                    if d and d > 0:
                        consecutive += 1
                    else:
                        break
                stock.consecutive_dividend_years = consecutive
                stock.dividend_history = dividend_series

            # === 收入增长 ===
            if revenue_series and len(revenue_series) >= 2:
                if revenue_series[0] > 0 and revenue_series[-1] > 0:
                    years = len(revenue_series) - 1
                    stock.revenue_cagr_10y = (revenue_series[-1] / revenue_series[0]) ** (1.0 / years) - 1

            # === 账面价值增长 ===
            if bv_series and len(bv_series) >= 2:
                if bv_series[0] > 0 and bv_series[-1] > 0:
                    years = len(bv_series) - 1
                    stock.book_value_growth = (bv_series[-1] / bv_series[0]) ** (1.0 / years) - 1

            # === 利息覆盖率历史 ===
            if ic_series:
                if len(ic_series) >= 7:
                    recent7 = ic_series[-7:]
                    stock.avg_7y_pretax_interest_coverage = sum(recent7) / len(recent7)
                    stock.worst_year_pretax_interest_coverage = min(recent7)
                elif ic_series:
                    stock.avg_7y_pretax_interest_coverage = sum(ic_series) / len(ic_series)
                    stock.worst_year_pretax_interest_coverage = min(ic_series)

        except Exception as e:
            logger.warning(f"[yfinance] 历史数据计算失败: {e}")

    def _get_annual_eps(self, ticker) -> list[float]:
        """从历史利润表提取年度 EPS 序列"""
        try:
            inc = ticker.income_stmt
            if inc is None or inc.empty:
                return []
            shares = None
            bs = ticker.balance_sheet
            if bs is not None and not bs.empty:
                for col_name in ["Ordinary Shares Number", "Share Issued"]:
                    if col_name in bs.index:
                        shares = bs.loc[col_name]
                        break

            eps_list = []
            if "Basic EPS" in inc.index:
                row = inc.loc["Basic EPS"]
                for val in reversed(row.values):
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        eps_list.append(float(val))
            elif "Diluted EPS" in inc.index:
                row = inc.loc["Diluted EPS"]
                for val in reversed(row.values):
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        eps_list.append(float(val))
            elif "Net Income" in inc.index and shares is not None:
                ni_row = inc.loc["Net Income"]
                for i, val in enumerate(reversed(ni_row.values)):
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        sh = list(reversed(shares.values))[i] if i < len(shares) else None
                        if sh and sh > 0:
                            eps_list.append(float(val) / float(sh))
            return eps_list
        except Exception as e:
            logger.warning(f"[yfinance] 获取历史EPS失败: {e}")
            return []

    def _get_annual_revenue(self, ticker) -> list[float]:
        """从历史利润表提取年度收入序列"""
        try:
            inc = ticker.income_stmt
            if inc is None or inc.empty:
                return []
            for field in ["Total Revenue", "Revenue"]:
                if field in inc.index:
                    row = inc.loc[field]
                    return [float(v) for v in reversed(row.values)
                            if v is not None and not (isinstance(v, float) and math.isnan(v))]
            return []
        except Exception:
            return []

    def _get_annual_dividends(self, ticker) -> list[float]:
        """获取年度股息序列"""
        try:
            divs = ticker.dividends
            if divs is None or divs.empty:
                return []
            annual = divs.resample("YE").sum()
            return [float(v) for v in annual.values if not (isinstance(v, float) and math.isnan(v))]
        except Exception:
            return []

    def _get_annual_book_value(self, ticker) -> list[float]:
        """从历史资产负债表提取年度每股账面价值序列"""
        try:
            bs = ticker.balance_sheet
            if bs is None or bs.empty:
                return []
            for field in ["Stockholders Equity", "Total Equity Gross Minority Interest"]:
                if field in bs.index:
                    row = bs.loc[field]
                    shares_row = None
                    for sfield in ["Ordinary Shares Number", "Share Issued"]:
                        if sfield in bs.index:
                            shares_row = bs.loc[sfield]
                            break
                    if shares_row is not None:
                        bv_list = []
                        for eq, sh in zip(reversed(row.values), reversed(shares_row.values)):
                            if eq is not None and sh is not None and sh > 0:
                                if not (isinstance(eq, float) and math.isnan(eq)):
                                    bv_list.append(float(eq) / float(sh))
                        return bv_list
            return []
        except Exception:
            return []

    def _get_annual_interest_coverage(self, ticker) -> list[float]:
        """从历史利润表提取年度利息覆盖率序列 (EBIT / Interest Expense)"""
        try:
            inc = ticker.income_stmt
            if inc is None or inc.empty:
                return []
            if "EBIT" in inc.index and "Interest Expense" in inc.index:
                ebit_row = inc.loc["EBIT"]
                int_row = inc.loc["Interest Expense"]
                ic_list = []
                for eb, ie in zip(reversed(ebit_row.values), reversed(int_row.values)):
                    if eb is not None and ie is not None:
                        if not (isinstance(eb, float) and math.isnan(eb)):
                            if not (isinstance(ie, float) and math.isnan(ie)):
                                ie_abs = abs(float(ie))  # interest expense often negative
                                if ie_abs > 0:
                                    ic_list.append(float(eb) / ie_abs)
                return ic_list
            return []
        except Exception:
            return []

    def _compute_technical_indicators(self, stock: StockData, ticker):
        """从价格历史计算技术指标 (RSI, MACD)"""
        try:
            hist = ticker.history(period="1y")
            if hist is None or hist.empty or len(hist) < 26:
                return
            close = hist["Close"]

            # RSI (14天)
            delta = close.diff()
            gain = delta.clip(lower=0)
            loss = (-delta.clip(upper=0))
            avg_gain = gain.rolling(14).mean()
            avg_loss = loss.rolling(14).mean()
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))
            if len(rsi.dropna()) > 0:
                stock.rsi_14d = float(rsi.dropna().iloc[-1])

            # MACD (12, 26, 9)
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            if len(macd_line.dropna()) > 0:
                stock.macd_line = float(macd_line.iloc[-1])
                stock.macd_signal = float(signal_line.iloc[-1])
                stock.macd_hist = float((macd_line - signal_line).iloc[-1])
        except Exception as e:
            logger.warning(f"[yfinance] 技术指标计算失败: {e}")

    def _fetch_market_benchmarks(self, stock: StockData):
        """获取市场基准数据"""
        import yfinance as yf

        try:
            # S&P 500 PE (via SPY)
            spy = yf.Ticker("SPY")
            spy_info = spy.info
            stock.market_pe = spy_info.get("trailingPE") or None
        except Exception:
            logger.warning("[yfinance] 获取 S&P 500 PE 失败")

        try:
            # 10年国债收益率 (^TNX)
            tnx = yf.Ticker("^TNX")
            tnx_info = tnx.info
            stock.treasury_yield_10y = tnx_info.get("regularMarketPrice") or tnx_info.get("previousClose") or None
            if stock.treasury_yield_10y:
                stock.aa_bond_yield = stock.treasury_yield_10y + 1.0
        except Exception:
            logger.warning("[yfinance] 获取国债收益率失败")

    def _compute_derived_metrics(self, stock: StockData):
        """计算衍生指标: Graham Number, NCAV, 内在价值, 安全边际等"""
        # Graham Number = sqrt(22.5 * EPS * Book Value per Share)
        if stock.eps is not None and stock.eps > 0 and stock.book_value is not None and stock.book_value > 0:
            stock.graham_number = math.sqrt(22.5 * stock.eps * stock.book_value)

        # NCAV per share = (Current Assets - Total Liabilities) / Shares Outstanding
        if stock.current_assets and stock.total_liabilities and stock.shares_outstanding:
            stock.net_current_assets = stock.current_assets - stock.total_liabilities
            stock.ncav_per_share = stock.net_current_assets / stock.shares_outstanding

        # 内在价值 (Graham 公式 V = EPS * (8.5 + 2g) * 4.4 / Y)
        if stock.eps is not None and stock.eps > 0:
            g = (stock.eps_growth_5y or stock.earnings_growth_10y or 0.0) * 100
            y = stock.aa_bond_yield or stock.treasury_yield_10y or 4.4
            if y > 0:
                stock.intrinsic_value = stock.eps * (8.5 + 2 * g) * 4.4 / y

        # 安全边际 = (内在价值 - 价格) / 内在价值
        if stock.intrinsic_value is not None and stock.intrinsic_value > 0 and stock.price is not None and stock.price > 0:
            stock.margin_of_safety = (stock.intrinsic_value - stock.price) / stock.intrinsic_value

        # book_value_equity = total_equity
        stock.book_value_equity = stock.total_equity

        # EPS 3年均值对价格比
        if stock.avg_eps_3y is not None and stock.avg_eps_3y > 0 and stock.price is not None and stock.price > 0:
            stock.eps_3yr_avg_to_price = stock.price / stock.avg_eps_3y

    @staticmethod
    def _safe_get(series, key, default=None):
        """安全获取 pandas Series 中的值"""
        try:
            if key in series.index:
                val = series[key]
                if val is not None and not (isinstance(val, float) and math.isnan(val)):
                    return float(val)
        except Exception:
            pass
        return default
