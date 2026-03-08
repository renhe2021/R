"""Bloomberg Terminal 数据源 - 通过 blpapi 连接本地彭博终端"""

import logging
import math
from datetime import datetime, timedelta
from typing import Any

from .base import DataProvider
from ..analyzer import StockData

logger = logging.getLogger(__name__)

# Bloomberg 字段 -> StockData 属性映射 (BDP 实时/静态)
BBG_FIELD_MAP = {
    # 基础信息
    "SECURITY_NAME": "name",
    "GICS_SECTOR_NAME": "sector",
    "GICS_INDUSTRY_NAME": "industry",
    # 价格
    "PX_LAST": "price",
    "HIGH_52WEEK": "price_52w_high",
    "LOW_52WEEK": "price_52w_low",
    # 估值
    "PE_RATIO": "pe",
    "BEST_PE_RATIO": "forward_pe",
    "PX_TO_BOOK_RATIO": "pb",
    "PX_TO_SALES_RATIO": "ps",
    "EARN_YLD": "earnings_yield",
    # 盈利
    "TRAIL_12M_EPS": "eps",
    "RETURN_COM_EQY": "roe",
    "SALES_REV_TURN": "revenue",
    "NET_INCOME": "net_income",
    "EBIT": "ebit",
    "PRETAX_INC": "pretax_income",
    "PROF_MARGIN": "profit_margin",
    "OPER_MARGIN": "operating_margin",
    # 财务健康
    "CUR_RATIO": "current_ratio",
    "TOT_DEBT_TO_TOT_EQY": "debt_to_equity",
    "TOT_DEBT_TO_TOT_ASSET": "debt_to_assets",
    "BS_TOT_LIAB2": "total_liabilities",
    "BS_CASH_NEAR_CASH_ITEM": "total_cash",
    "CUR_MKT_CAP": "market_cap",
    "BOOK_VAL_PER_SH": "book_value",
    "TANGIBLE_BV_PER_SH": "tangible_book_value",
    "WORKING_CAPITAL": "working_capital",
    "BS_TOT_ASSET": "total_assets",
    "TOTAL_EQUITY": "total_equity",
    "BS_CUR_ASSET_REPORT": "current_assets",
    "BS_CUR_LIAB": "current_liabilities",
    "BS_LT_BORROW": "long_term_debt",
    "IS_INT_EXPENSE": "_interest_expense",  # 用于计算利息覆盖率
    "CF_FREE_CASH_FLOW": "free_cash_flow",
    "CAPITAL_EXPEND": "capex",
    # 股息
    "EQY_DVD_YLD_IND": "dividend_yield",
    "DVD_PAYOUT_RATIO": "dividend_payout_ratio",
    "DVD_SH_LAST": "dividend_per_share",
    # 增长
    "SALES_GROWTH": "revenue_growth_rate",
    "EPS_GROWTH": "eps_growth_rate",
    # 信用评级
    "RTG_SP_LT_LC_ISSUER_CREDIT": "sp_rating",
    "RTG_MOODY": "moody_rating",
    # 股本
    "EQY_SH_OUT": "shares_outstanding",
    "ENTERPRISE_VALUE": "enterprise_value",
    # 技术指标
    "RSI_14D": "rsi_14d",
    "MACD_SIGNAL": "macd_signal",
    "MOV_AVG_200D": "ma_200d",
}

# 历史字段 (BDH) - 需要拉取多年数据来计算
BBG_HISTORICAL_FIELDS = [
    "IS_EPS",              # 年度 EPS
    "SALES_REV_TURN",      # 年度收入
    "NET_INCOME",          # 年度净利润
    "DVD_SH_LAST",         # 年度每股股息
    "BOOK_VAL_PER_SH",     # 年度每股账面价值
    "RETURN_COM_EQY",      # 年度 ROE
    "IS_INT_EXPENSE",      # 年度利息支出 (用于计算利息覆盖率)
    "EBIT",                # 年度 EBIT (用于计算利息覆盖率)
]

# 市场基准证券
MARKET_BENCHMARKS = {
    "SPX Index": ["PE_RATIO"],                    # S&P 500 PE
    "USGG10YR Index": ["PX_LAST"],                # 美国10年国债收益率
}


class BloombergProvider(DataProvider):
    """通过 blpapi 连接 Bloomberg Terminal 获取数据"""

    def __init__(self, host: str = "localhost", port: int = 8194):
        self._host = host
        self._port = port
        self._session = None

    @property
    def name(self) -> str:
        return "bloomberg"

    def is_available(self) -> bool:
        try:
            import blpapi  # noqa: F401
            return True
        except ImportError:
            return False

    def _get_session(self):
        """获取或创建 blpapi session"""
        if self._session is not None:
            return self._session

        import blpapi

        options = blpapi.SessionOptions()
        options.setServerHost(self._host)
        options.setServerPort(self._port)

        session = blpapi.Session(options)
        if not session.start():
            raise ConnectionError(
                f"无法连接 Bloomberg Terminal ({self._host}:{self._port})。"
                "请确保 Bloomberg Terminal 桌面版已启动并登录。"
            )
        if not session.openService("//blp/refdata"):
            raise ConnectionError("无法打开 Bloomberg //blp/refdata 服务")

        self._session = session
        logger.info(f"[Bloomberg] 已连接终端 {self._host}:{self._port}")
        return session

    def _bdp(self, security: str, fields: list[str]) -> dict[str, Any]:
        """BDP - 获取实时/静态参考数据"""
        import blpapi

        session = self._get_session()
        service = session.getService("//blp/refdata")
        request = service.createRequest("ReferenceDataRequest")

        request.getElement("securities").appendValue(security)
        for f in fields:
            request.getElement("fields").appendValue(f)

        session.sendRequest(request)

        result = {}
        while True:
            event = session.nextEvent(500)
            for msg in event:
                if msg.hasElement("securityData"):
                    sec_data = msg.getElement("securityData")
                    for i in range(sec_data.numValues()):
                        sec = sec_data.getValueAsElement(i)
                        if sec.hasElement("fieldData"):
                            field_data = sec.getElement("fieldData")
                            for f in fields:
                                if field_data.hasElement(f):
                                    try:
                                        val = field_data.getElementAsFloat(f)
                                        result[f] = val
                                    except Exception:
                                        try:
                                            result[f] = field_data.getElementAsString(f)
                                        except Exception:
                                            pass
            if event.eventType() == blpapi.Event.RESPONSE:
                break

        return result

    def _bdh(self, security: str, fields: list[str],
             start_date: str, end_date: str,
             periodicity: str = "YEARLY") -> list[dict]:
        """BDH - 获取历史数据"""
        import blpapi

        session = self._get_session()
        service = session.getService("//blp/refdata")
        request = service.createRequest("HistoricalDataRequest")

        request.getElement("securities").appendValue(security)
        for f in fields:
            request.getElement("fields").appendValue(f)
        request.set("startDate", start_date)
        request.set("endDate", end_date)
        request.set("periodicitySelection", periodicity)

        logger.debug(f"[Bloomberg] BDH 请求: {security}, "
                     f"fields={fields}, {start_date}~{end_date}, {periodicity}")
        session.sendRequest(request)

        rows = []
        while True:
            event = session.nextEvent(5000)  # 增大超时到 5s，防止大量数据时超时
            for msg in event:
                # 检查是否有请求级别的错误
                if msg.hasElement("responseError"):
                    err = msg.getElement("responseError")
                    logger.error(f"[Bloomberg] BDH responseError: "
                                 f"{err.getElementAsString('message')}")
                    return rows

                if msg.hasElement("securityData"):
                    sec_data = msg.getElement("securityData")

                    # 检查 securityError（如 ticker 无效）
                    if sec_data.hasElement("securityError"):
                        sec_err = sec_data.getElement("securityError")
                        logger.error(
                            f"[Bloomberg] BDH securityError for '{security}': "
                            f"{sec_err.getElementAsString('message')} "
                            f"(category={sec_err.getElementAsString('category')})"
                        )
                        return rows

                    # 检查 fieldExceptions（如字段名无效）
                    if sec_data.hasElement("fieldExceptions"):
                        fe = sec_data.getElement("fieldExceptions")
                        for j in range(fe.numValues()):
                            exc = fe.getValueAsElement(j)
                            fid = exc.getElementAsString("fieldId") if exc.hasElement("fieldId") else "?"
                            ei = exc.getElement("errorInfo") if exc.hasElement("errorInfo") else None
                            emsg = ei.getElementAsString("message") if ei and ei.hasElement("message") else "unknown"
                            logger.warning(f"[Bloomberg] BDH fieldException: "
                                           f"field={fid}, error={emsg}")

                    if sec_data.hasElement("fieldData"):
                        field_data = sec_data.getElement("fieldData")
                        for i in range(field_data.numValues()):
                            row_elem = field_data.getValueAsElement(i)
                            row = {}
                            if row_elem.hasElement("date"):
                                row["date"] = row_elem.getElementAsString("date")
                            for f in fields:
                                if row_elem.hasElement(f):
                                    try:
                                        row[f] = row_elem.getElementAsFloat(f)
                                    except Exception:
                                        try:
                                            row[f] = row_elem.getElementAsString(f)
                                        except Exception:
                                            pass
                            rows.append(row)
                    else:
                        logger.warning(f"[Bloomberg] BDH 响应中无 fieldData 元素 "
                                       f"(security={security})")

            if event.eventType() == blpapi.Event.RESPONSE:
                break

        if not rows:
            logger.warning(f"[Bloomberg] BDH 返回 0 行数据: security={security}, "
                           f"fields={fields}, range={start_date}~{end_date}")

        return rows

    def fetch(self, symbol: str) -> StockData:
        """获取完整股票数据（实时 + 10年历史 + 市场基准）"""
        security = self._to_bbg_ticker(symbol)
        logger.info(f"[Bloomberg] 获取 {security} ...")

        # 1. 拉取实时/静态字段
        bdp_fields = list(BBG_FIELD_MAP.keys())
        ref_data = self._bdp(security, bdp_fields)
        logger.info(f"[Bloomberg] BDP 获取 {len(ref_data)} 个字段")

        # 2. 拉取 10 年历史数据
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=365 * 10)).strftime("%Y%m%d")
        hist_data = self._bdh(security, BBG_HISTORICAL_FIELDS, start_date, end_date)

        # 如果 BDH 返回空，尝试缩短时间范围重试一次
        if not hist_data:
            logger.warning(f"[Bloomberg] BDH 10年数据为空，尝试 5 年范围重试...")
            start_date_5y = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y%m%d")
            hist_data = self._bdh(security, BBG_HISTORICAL_FIELDS, start_date_5y, end_date)

        if not hist_data:
            logger.warning(f"[Bloomberg] BDH 历史数据仍为空! "
                           f"security={security}, 所有历史衍生指标将为 None。"
                           f"请确认: 1) ticker 格式正确 2) 该证券确实有年度财务数据")
        else:
            logger.info(f"[Bloomberg] BDH 获取 {len(hist_data)} 年历史数据")

        # 3. 拉取市场基准数据
        benchmarks = self._fetch_market_benchmarks()

        # 4. 构建 StockData
        stock = StockData(symbol=symbol)
        self._populate_from_bdp(stock, ref_data)
        self._populate_benchmarks(stock, benchmarks)

        # 5. 历史数据计算
        if hist_data:
            self._compute_historical_metrics(stock, hist_data)

        # 6. 计算衍生指标 (Graham Number, NCAV, 内在价值等)
        self._compute_derived_metrics(stock)

        logger.info(f"[Bloomberg] 数据获取完成: {stock.name} ({stock.symbol})")
        return stock

    def _populate_from_bdp(self, stock: StockData, ref_data: dict):
        """从 BDP 数据填充 StockData"""
        stock.name = ref_data.get("SECURITY_NAME", stock.symbol)
        stock.sector = ref_data.get("GICS_SECTOR_NAME", "")
        stock.industry = ref_data.get("GICS_INDUSTRY_NAME", "")

        # 价格
        stock.price = ref_data.get("PX_LAST") or None
        stock.price_52w_high = ref_data.get("HIGH_52WEEK") or None
        stock.price_52w_low = ref_data.get("LOW_52WEEK") or None

        # 估值
        stock.pe = ref_data.get("PE_RATIO") or None
        stock.forward_pe = ref_data.get("BEST_PE_RATIO") or None
        stock.pb = ref_data.get("PX_TO_BOOK_RATIO") or None
        stock.ps = ref_data.get("PX_TO_SALES_RATIO") or None
        stock.earnings_yield = ref_data.get("EARN_YLD") or None

        # 盈利
        stock.eps = ref_data.get("TRAIL_12M_EPS") or None
        roe_raw = ref_data.get("RETURN_COM_EQY")
        stock.roe = (roe_raw / 100.0) if roe_raw else None
        stock.revenue = ref_data.get("SALES_REV_TURN") or None
        stock.net_income = ref_data.get("NET_INCOME") or None
        stock.ebit = ref_data.get("EBIT") or None
        stock.pretax_income = ref_data.get("PRETAX_INC") or None
        pm_raw = ref_data.get("PROF_MARGIN")
        stock.profit_margin = (pm_raw / 100.0) if pm_raw else None
        om_raw = ref_data.get("OPER_MARGIN")
        stock.operating_margin = (om_raw / 100.0) if om_raw else None

        # 财务健康
        stock.current_ratio = ref_data.get("CUR_RATIO") or None
        de_raw = ref_data.get("TOT_DEBT_TO_TOT_EQY")
        stock.debt_to_equity = (de_raw / 100.0) if de_raw else None
        da_raw = ref_data.get("TOT_DEBT_TO_TOT_ASSET")
        stock.debt_to_assets = (da_raw / 100.0) if da_raw else None
        stock.total_liabilities = ref_data.get("BS_TOT_LIAB2") or None
        stock.total_debt = stock.total_liabilities  # 同义
        stock.total_cash = ref_data.get("BS_CASH_NEAR_CASH_ITEM") or None
        stock.market_cap = ref_data.get("CUR_MKT_CAP") or None
        stock.book_value = ref_data.get("BOOK_VAL_PER_SH") or None
        stock.tangible_book_value = ref_data.get("TANGIBLE_BV_PER_SH") or None
        stock.working_capital = ref_data.get("WORKING_CAPITAL") or None
        stock.total_assets = ref_data.get("BS_TOT_ASSET") or None
        stock.total_equity = ref_data.get("TOTAL_EQUITY") or None
        stock.current_assets = ref_data.get("BS_CUR_ASSET_REPORT") or None
        stock.current_liabilities = ref_data.get("BS_CUR_LIAB") or None
        stock.long_term_debt = ref_data.get("BS_LT_BORROW") or None
        stock.free_cash_flow = ref_data.get("CF_FREE_CASH_FLOW") or None
        stock.capex = ref_data.get("CAPITAL_EXPEND") or None

        # 利息覆盖率 = EBIT / 利息支出
        interest_expense = ref_data.get("IS_INT_EXPENSE") or None
        if stock.ebit and interest_expense and interest_expense > 0:
            stock.interest_coverage_ratio = stock.ebit / interest_expense

        # 股息
        stock.dividend_yield = ref_data.get("EQY_DVD_YLD_IND") or None
        stock.dividend_payout_ratio = ref_data.get("DVD_PAYOUT_RATIO") or None
        stock.dividend_per_share = ref_data.get("DVD_SH_LAST") or None

        # 增长
        stock.revenue_growth_rate = ref_data.get("SALES_GROWTH") or None
        stock.eps_growth_rate = ref_data.get("EPS_GROWTH") or None

        # 信用评级
        stock.sp_rating = ref_data.get("RTG_SP_LT_LC_ISSUER_CREDIT", "")
        stock.moody_rating = ref_data.get("RTG_MOODY", "")

        # 股本
        stock.shares_outstanding = ref_data.get("EQY_SH_OUT") or None
        stock.enterprise_value = ref_data.get("ENTERPRISE_VALUE") or None

        # 技术指标
        stock.rsi_14d = ref_data.get("RSI_14D") or None
        stock.macd_signal = ref_data.get("MACD_SIGNAL") or None
        stock.ma_200d = ref_data.get("MOV_AVG_200D") or None

        # annual_sales = revenue
        stock.annual_sales = stock.revenue

    def _fetch_market_benchmarks(self) -> dict:
        """拉取市场基准数据（S&P 500 PE, 国债利率, AA 债券利率）"""
        benchmarks = {}
        for security, fields in MARKET_BENCHMARKS.items():
            try:
                data = self._bdp(security, fields)
                benchmarks[security] = data
                logger.info(f"[Bloomberg] 基准 {security}: {data}")
            except Exception as e:
                logger.warning(f"[Bloomberg] 获取基准 {security} 失败: {e}")
        return benchmarks

    def _populate_benchmarks(self, stock: StockData, benchmarks: dict):
        """填充市场基准数据到 StockData"""
        # S&P 500 PE
        spx = benchmarks.get("SPX Index", {})
        stock.market_pe = spx.get("PE_RATIO") or None

        # 10年国债收益率
        ust = benchmarks.get("USGG10YR Index", {})
        stock.treasury_yield_10y = ust.get("PX_LAST") or None

        # AA 企业债收益率 ≈ 国债 + 1% 信用利差（简化估算）
        if stock.treasury_yield_10y:
            stock.aa_bond_yield = stock.treasury_yield_10y + 1.0

    def _compute_historical_metrics(self, stock: StockData, hist_data: list[dict]):
        """从历史数据计算衍生指标"""
        eps_series = [r.get("IS_EPS") for r in hist_data if r.get("IS_EPS") is not None]
        revenue_series = [r.get("SALES_REV_TURN") for r in hist_data if r.get("SALES_REV_TURN") is not None]
        dividend_series = [r.get("DVD_SH_LAST") for r in hist_data if r.get("DVD_SH_LAST") is not None]
        bv_series = [r.get("BOOK_VAL_PER_SH") for r in hist_data if r.get("BOOK_VAL_PER_SH") is not None]

        # 历史利息覆盖率: EBIT / IS_INT_EXPENSE
        ic_series = []
        for r in hist_data:
            ebit_val = r.get("EBIT")
            int_exp = r.get("IS_INT_EXPENSE")
            if ebit_val is not None and int_exp is not None and int_exp > 0:
                ic_series.append(ebit_val / int_exp)

        # === EPS 相关 ===
        if eps_series:
            stock.avg_eps_10y = sum(eps_series) / len(eps_series)
            stock.eps_history = eps_series

            # EPS CAGR (10 年)
            if len(eps_series) >= 2 and eps_series[0] and eps_series[0] > 0:
                years = len(eps_series) - 1
                if eps_series[-1] > 0 and eps_series[0] > 0:
                    stock.earnings_growth_10y = (eps_series[-1] / eps_series[0]) ** (1.0 / years) - 1

            # 最近 3 年平均 EPS
            if len(eps_series) >= 3:
                stock.avg_eps_3y = sum(eps_series[-3:]) / 3

            # 最早 3 年平均 EPS (用于 Graham 的增长对比)
            if len(eps_series) >= 6:
                stock.avg_eps_first_3y = sum(eps_series[:3]) / 3

            # 盈利年数 (正 EPS)
            positive_years = sum(1 for e in eps_series if e and e > 0)
            stock.profitable_years = positive_years

            # 10年中最小年度 EPS
            stock.min_annual_eps_10y = min(eps_series) if eps_series else 0.0

            # 5年中最小年度 EPS
            if len(eps_series) >= 5:
                stock.min_annual_eps_5y = min(eps_series[-5:])

            # 连续盈利年数（从最近往回数）
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

            # EPS 5年增长率 (CAGR)
            if len(eps_series) >= 6:
                eps_now = eps_series[-1]
                eps_5y = eps_series[-6]
                if eps_5y and eps_5y > 0 and eps_now and eps_now > 0:
                    stock.eps_growth_5y = (eps_now / eps_5y) ** (1.0 / 5) - 1
            elif len(eps_series) >= 5:
                eps_now = eps_series[-1]
                eps_5y = eps_series[0]
                years = len(eps_series) - 1
                if eps_5y and eps_5y > 0 and eps_now and eps_now > 0:
                    stock.eps_growth_5y = (eps_now / eps_5y) ** (1.0 / years) - 1

            # 5年前的 EPS (动态字段，存在 __dict__ 中供规则引用)
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

        # === 收入增长 (CAGR) ===
        if revenue_series and len(revenue_series) >= 2:
            if revenue_series[0] and revenue_series[0] > 0 and revenue_series[-1] and revenue_series[-1] > 0:
                years = len(revenue_series) - 1
                stock.revenue_cagr_10y = (revenue_series[-1] / revenue_series[0]) ** (1.0 / years) - 1

        # === 账面价值增长 ===
        if bv_series and len(bv_series) >= 2:
            if bv_series[0] and bv_series[0] > 0 and bv_series[-1] and bv_series[-1] > 0:
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
        # g = 预期增长率, Y = 当前 AA 债券收益率
        if stock.eps is not None and stock.eps > 0:
            g = (stock.eps_growth_5y or stock.earnings_growth_10y or 0.0) * 100
            y = stock.aa_bond_yield or stock.treasury_yield_10y or 4.4
            if y > 0:
                stock.intrinsic_value = stock.eps * (8.5 + 2 * g) * 4.4 / y

        # 安全边际 = (内在价值 - 价格) / 内在价值
        if stock.intrinsic_value is not None and stock.intrinsic_value > 0 and stock.price is not None and stock.price > 0:
            stock.margin_of_safety = (stock.intrinsic_value - stock.price) / stock.intrinsic_value

        # book_value_equity = total_equity (方便规则引用)
        stock.book_value_equity = stock.total_equity

        # EPS 3年均值对价格比
        if stock.avg_eps_3y is not None and stock.avg_eps_3y > 0 and stock.price is not None and stock.price > 0:
            stock.eps_3yr_avg_to_price = stock.price / stock.avg_eps_3y

    def _to_bbg_ticker(self, symbol: str) -> str:
        """将简单 ticker 转换为 Bloomberg 格式"""
        symbol = symbol.strip()
        # 已经是 Bloomberg 格式（包含 Equity/Index），直接返回
        sym_upper = symbol.upper()
        if " EQUITY" in sym_upper:
            return symbol
        if " INDEX" in sym_upper:
            return symbol
        symbol = sym_upper
        if symbol.isdigit() and len(symbol) <= 5:
            return f"{symbol} HK Equity"
        if symbol.isdigit() and len(symbol) == 6:
            if symbol.startswith(("6", "9")):
                return f"{symbol} CH Equity"
            else:
                return f"{symbol} CH Equity"
        return f"{symbol} US Equity"

    def close(self):
        """关闭连接"""
        if self._session:
            self._session.stop()
            self._session = None
            logger.info("[Bloomberg] 连接已关闭")

    def __del__(self):
        self.close()
