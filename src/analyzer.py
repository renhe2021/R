"""单股分析引擎 - 用知识库中的规则评估个股"""

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class StockData:
    """股票基础数据 + 彭博扩展字段

    重要：数值字段使用 Optional[float] = None 而非 float = 0.0
    - None 表示"数据未获取到/不可用"
    - 0.0 表示"该指标的真实值为零"
    这样可以区分"没拿到数据"和"数据确实是0"（如无分红公司的 dividend_yield=0.0）
    """
    symbol: str
    name: str = ""
    sector: str = ""
    industry: str = ""
    # 估值指标
    pe: Optional[float] = None
    forward_pe: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    earnings_yield: Optional[float] = None
    # 盈利指标
    roe: Optional[float] = None
    eps: Optional[float] = None
    revenue: Optional[float] = None
    net_income: Optional[float] = None
    ebit: Optional[float] = None
    pretax_income: Optional[float] = None
    profit_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    # 财务健康
    current_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    debt_to_assets: Optional[float] = None
    total_debt: Optional[float] = None
    total_cash: Optional[float] = None
    market_cap: Optional[float] = None
    book_value: Optional[float] = None
    tangible_book_value: Optional[float] = None
    working_capital: Optional[float] = None
    total_assets: Optional[float] = None
    total_equity: Optional[float] = None
    enterprise_value: Optional[float] = None
    shares_outstanding: Optional[float] = None
    current_assets: Optional[float] = None
    current_liabilities: Optional[float] = None
    long_term_debt: Optional[float] = None
    total_liabilities: Optional[float] = None
    interest_coverage_ratio: Optional[float] = None
    free_cash_flow: Optional[float] = None
    capex: Optional[float] = None
    # 股息
    dividend_yield: Optional[float] = None
    dividend_payout_ratio: Optional[float] = None
    dividend_per_share: Optional[float] = None
    # 价格
    price: Optional[float] = None
    price_52w_high: Optional[float] = None
    price_52w_low: Optional[float] = None
    # 增长
    revenue_growth_rate: Optional[float] = None
    eps_growth_rate: Optional[float] = None
    eps_growth_5y: Optional[float] = None
    # 信用评级 / 质量
    sp_rating: str = ""
    moody_rating: str = ""
    sp_quality_ranking: str = ""
    # 技术指标
    rsi_14d: Optional[float] = None
    macd_line: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_hist: Optional[float] = None
    ma_200d: Optional[float] = None
    # 市场基准
    market_pe: Optional[float] = None
    industry_avg_pe: Optional[float] = None
    aa_bond_yield: Optional[float] = None
    treasury_yield_10y: Optional[float] = None
    # === 历史衍生指标 (彭博 BDH 计算) ===
    avg_eps_10y: Optional[float] = None
    avg_eps_3y: Optional[float] = None
    avg_eps_first_3y: Optional[float] = None
    earnings_growth_10y: Optional[float] = None
    profitable_years: Optional[int] = None
    min_annual_eps_10y: Optional[float] = None
    min_annual_eps_5y: Optional[float] = None
    max_eps_decline: Optional[float] = None
    consecutive_dividend_years: Optional[int] = None
    consecutive_profitable_years: Optional[int] = None
    revenue_cagr_10y: Optional[float] = None
    book_value_growth: Optional[float] = None
    eps_history: list = field(default_factory=list)
    dividend_history: list = field(default_factory=list)
    # === 计算衍生指标 ===
    graham_number: Optional[float] = None
    ncav_per_share: Optional[float] = None
    intrinsic_value: Optional[float] = None
    margin_of_safety: Optional[float] = None
    net_current_assets: Optional[float] = None
    annual_sales: Optional[float] = None
    book_value_equity: Optional[float] = None
    eps_3yr_avg_to_price: Optional[float] = None
    # 历史利息覆盖
    avg_7y_pretax_interest_coverage: Optional[float] = None
    worst_year_pretax_interest_coverage: Optional[float] = None

    # === 数据质量追踪 ===
    _data_quality: dict = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()
                if v is not None and v != "" and v != [] and not k.startswith("_")}

    def data_coverage(self) -> dict:
        """计算数据覆盖率 — 多少核心字段真正有值"""
        core_fields = [
            "price", "pe", "eps", "roe", "pb", "market_cap",
            "revenue", "net_income", "book_value", "current_ratio",
            "debt_to_equity", "dividend_yield",
        ]
        extended_fields = [
            "forward_pe", "ps", "earnings_yield", "profit_margin",
            "operating_margin", "total_assets", "total_equity",
            "enterprise_value", "free_cash_flow", "ebit",
        ]
        historical_fields = [
            "avg_eps_10y", "avg_eps_3y", "earnings_growth_10y",
            "profitable_years", "consecutive_dividend_years",
            "graham_number", "intrinsic_value",
        ]

        def _count(fields):
            total = len(fields)
            filled = sum(1 for f in fields if getattr(self, f, None) is not None)
            return filled, total

        core_filled, core_total = _count(core_fields)
        ext_filled, ext_total = _count(extended_fields)
        hist_filled, hist_total = _count(historical_fields)
        all_filled = core_filled + ext_filled + hist_filled
        all_total = core_total + ext_total + hist_total

        return {
            "core": {"filled": core_filled, "total": core_total,
                     "pct": round(core_filled / core_total * 100, 1) if core_total else 0},
            "extended": {"filled": ext_filled, "total": ext_total,
                         "pct": round(ext_filled / ext_total * 100, 1) if ext_total else 0},
            "historical": {"filled": hist_filled, "total": hist_total,
                           "pct": round(hist_filled / hist_total * 100, 1) if hist_total else 0},
            "overall": {"filled": all_filled, "total": all_total,
                        "pct": round(all_filled / all_total * 100, 1) if all_total else 0},
            "has_price": self.price is not None and self.price > 0,
            "has_name": bool(self.name and self.name != self.symbol),
            "missing_core": [f for f in core_fields if getattr(self, f, None) is None],
        }

    def is_valid(self) -> bool:
        """检查数据是否至少有最基本的有效性（股价和名称）"""
        return (self.price is not None and self.price > 0 and
                bool(self.name and self.name != self.symbol))


def fetch_stock_data(symbol: str) -> StockData:
    """通过 yfinance 获取股票数据"""
    import yfinance as yf

    logger.info(f"获取 {symbol} 的市场数据...")
    ticker = yf.Ticker(symbol)
    info = ticker.info

    data = StockData(
        symbol=symbol,
        name=info.get("shortName", symbol),
        sector=info.get("sector", ""),
        industry=info.get("industry", ""),
        pe=info.get("trailingPE") or None,
        forward_pe=info.get("forwardPE") or None,
        pb=info.get("priceToBook") or None,
        ps=info.get("priceToSalesTrailing12Months") or None,
        roe=info.get("returnOnEquity") or None,
        eps=info.get("trailingEps") or None,
        revenue=info.get("totalRevenue") or None,
        net_income=info.get("netIncomeToCommon") or None,
        current_ratio=info.get("currentRatio") or None,
        debt_to_equity=(info.get("debtToEquity") / 100.0) if info.get("debtToEquity") else None,
        total_debt=info.get("totalDebt") or None,
        total_cash=info.get("totalCash") or None,
        market_cap=info.get("marketCap") or None,
        book_value=info.get("bookValue") or None,
        dividend_yield=(info.get("dividendYield")) if info.get("dividendYield") is not None else None,
        price=info.get("currentPrice") or info.get("regularMarketPrice") or None,
        price_52w_high=info.get("fiftyTwoWeekHigh") or None,
        price_52w_low=info.get("fiftyTwoWeekLow") or None,
    )
    logger.info(f"数据获取完成: {data.name} ({data.symbol})")
    return data


# --- 规则评估变量映射 ---
VARIABLE_MAP = {
    # === 估值 ===
    "PE": "pe", "P/E": "pe", "pe": "pe", "trailing_pe": "pe", "PE_RATIO": "pe",
    "forward_pe": "forward_pe", "FORWARD_PE": "forward_pe",
    "PB": "pb", "P/B": "pb", "pb": "pb", "price_to_book": "pb", "price_to_book_value": "pb",
    "PRICE_TO_BOOK": "pb", "Price_to_Book": "pb",
    "PS": "ps", "P/S": "ps",
    "EARNINGS_YIELD": "earnings_yield", "earnings_yield": "earnings_yield",
    "E/P": "earnings_yield",
    "ENTERPRISE_VALUE": "enterprise_value", "EV": "enterprise_value",
    # === 盈利 ===
    "ROE": "roe", "roe": "roe", "return_on_equity": "roe", "RETURN_COM_EQY": "roe",
    "EPS": "eps", "eps": "eps", "trailing_eps": "eps",
    "CURRENT_EPS": "eps", "current_EPS": "eps", "primary_EPS": "eps",
    "EBIT": "ebit", "ebit": "ebit",
    "net_income": "net_income", "NET_INCOME": "net_income",
    "PRETAX_EARNINGS": "pretax_income", "pretax_income": "pretax_income",
    "pretax_earnings": "pretax_income",
    "profit_margin": "profit_margin", "PROFIT_MARGIN": "profit_margin",
    "net_profit_margin": "profit_margin",
    "operating_margin": "operating_margin", "OPERATING_MARGIN": "operating_margin",
    "EARNINGS_TO_BOOK_VALUE": "roe",
    # === 财务健康 ===
    "current_ratio": "current_ratio", "CURRENT_RATIO": "current_ratio",
    "debt_to_equity": "debt_to_equity", "DE_RATIO": "debt_to_equity",
    "DEBT_TO_EQUITY": "debt_to_equity", "D/E": "debt_to_equity",
    "total_debt": "total_debt", "TOTAL_DEBT": "total_debt", "DEBT": "total_debt",
    "total_cash": "total_cash",
    "market_cap": "market_cap", "MARKET_CAP": "market_cap", "MARKET_CAP_USD": "market_cap",
    "book_value": "book_value", "BOOK_VALUE": "book_value",
    "BOOK_VALUE_PER_SHARE": "book_value", "BookValue": "book_value", "Book_Value": "book_value",
    "book_value_per_share": "book_value",
    "tangible_book_value_per_share": "tangible_book_value",
    "TANGIBLE_BV_PER_SH": "tangible_book_value",
    "NetTangibleAssetPerShare": "tangible_book_value",
    "NET_TANGIBLE_ASSETS_PER_SHARE": "tangible_book_value",
    "TangibleBookValue": "tangible_book_value",
    "working_capital": "working_capital", "WORKING_CAPITAL": "working_capital",
    "total_assets": "total_assets", "TOTAL_ASSETS": "total_assets",
    "total_equity": "total_equity", "TOTAL_EQUITY": "total_equity",
    "shareholders_equity": "total_equity", "stock_equity": "total_equity",
    "book_value_equity": "total_equity", "tangible_equity": "total_equity",
    "total_capital": "total_assets",
    "current_assets": "current_assets", "CURRENT_ASSETS": "current_assets",
    "current_liabilities": "current_liabilities", "CURRENT_LIABILITIES": "current_liabilities",
    "long_term_debt": "long_term_debt", "LONG_TERM_DEBT": "long_term_debt",
    "bonded_debt": "long_term_debt",
    "total_liabilities": "total_liabilities", "TOTAL_LIABILITIES": "total_liabilities",
    "shares_outstanding": "shares_outstanding", "SharesOutstanding": "shares_outstanding",
    "SHARES_OUTSTANDING": "shares_outstanding",
    "interest_coverage_ratio": "interest_coverage_ratio",
    "interest_coverage": "interest_coverage_ratio",
    "INTEREST_COVERAGE": "interest_coverage_ratio",
    "interest_coverage_before_tax": "interest_coverage_ratio",
    "interest_coverage_after_tax": "interest_coverage_ratio",
    "interest_charges": "interest_coverage_ratio",  # 近似: 利息覆盖率
    "free_cash_flow": "free_cash_flow", "FCF": "free_cash_flow",
    "net_current_asset_value_per_share": "ncav_per_share",
    "NET_CURRENT_ASSET_VALUE_PER_SHARE": "ncav_per_share",
    "NetCurrentAssetValuePerShare": "ncav_per_share",
    "PRICE_NET_CURRENT_ASSET_VALUE": "ncav_per_share",
    # === 股息 ===
    "dividend_yield": "dividend_yield", "DIVIDEND_YIELD": "dividend_yield",
    "DividendYield": "dividend_yield", "stock_dividend_yield": "dividend_yield",
    "DIVIDEND_PAYOUT_RATIO": "dividend_payout_ratio",
    "Dividend_Payout_Ratio": "dividend_payout_ratio",
    "dividend_payout_ratio": "dividend_payout_ratio",
    "dividend_per_share": "dividend_per_share", "dividend": "dividend_per_share",
    "MARGIN_OF_SAFETY": "margin_of_safety",
    "earned_per_share": "eps",  # 近似
    "earnings_growth_yoy": "eps_growth_rate",  # 近似
    "EARNINGS_GROWTH_RATE_PRETAX": "eps_growth_rate",  # 近似
    "CONSECUTIVE_DIVIDEND_YEARS": "consecutive_dividend_years",
    "consecutive_dividend_years": "consecutive_dividend_years",
    "DIVIDEND_CONTINUITY_YEARS": "consecutive_dividend_years",
    "DIVIDEND_PAYOUT_YEARS": "consecutive_dividend_years",
    "CONTINUOUS_DIVIDEND_RECORD": "consecutive_dividend_years",
    "DIVIDEND_GROWTH_RATE": "dividend_yield",  # 近似
    # === 价格 ===
    "price": "price", "PRICE": "price", "CURRENT_PRICE": "price",
    "Price": "price", "market_price": "price", "MARKET_PRICE": "price",
    "price_current": "price", "stock_price": "price", "PX_LAST": "price",
    "Common_Stock_Price": "price", "price_per_share": "price",
    "Market_Price_Per_Share": "price",
    "price_52w_high": "price_52w_high", "HIGH_52WEEK": "price_52w_high",
    "price_52w_low": "price_52w_low", "LOW_52WEEK": "price_52w_low",
    # === 收入/增长 ===
    "annual_revenue": "revenue", "REVENUE": "revenue", "revenue": "revenue",
    "revenues": "revenue", "annual_sales": "revenue",
    "revenue_growth_rate": "revenue_growth_rate", "SALES_GROWTH": "revenue_growth_rate",
    "EARNINGS_GROWTH_RATE": "eps_growth_rate", "EPS_GROWTH": "eps_growth_rate",
    "earnings_growth_rate": "eps_growth_rate",
    "EPS_ANNUAL_GROWTH_RATE_5Y": "eps_growth_5y",
    "debt_growth_rate": "revenue_growth_rate",  # 近似
    # === 历史衍生 ===
    "AVG_EPS_10Y": "avg_eps_10y", "avg_EPS_10Y": "avg_eps_10y",
    "AvgEPS_3Y": "avg_eps_3y", "AVG_EARNINGS_3Y": "avg_eps_3y",
    "AvgEarnings_5Y": "avg_eps_3y",
    "EPS_3yr_avg": "avg_eps_3y", "EPS_recent_3yr_avg": "avg_eps_3y",
    "avg_earnings_3yr": "avg_eps_3y", "AVG_EARNINGS_3Y_RECENT": "avg_eps_3y",
    "PRICE_AVG_EARNINGS_3Y": "avg_eps_3y",
    "EPS_earliest_3yr_avg": "avg_eps_first_3y",
    "THREE_YEAR_AVG_GROWTH_OVER_DECADE": "earnings_growth_10y",  # 近似
    "EARNINGS_GROWTH_10Y": "earnings_growth_10y", "CAGR_10Y": "earnings_growth_10y",
    "earnings_growth_10yr": "earnings_growth_10y",
    "TOTAL_GROWTH_10Y": "earnings_growth_10y",
    "profitable_years_in_last_5": "profitable_years",
    "EARNINGS_STABILITY": "profitable_years",
    "DJIA_MAX_EPS_DECLINE": "max_eps_decline",
    "min_annual_earnings_last_10_years": "min_annual_eps_10y",
    "min_annual_EPS_last_5_years": "min_annual_eps_5y",
    "MIN_EPS_10Y": "min_annual_eps_10y",
    "consecutive_profitable_years": "consecutive_profitable_years",
    "annual_deficit_count_last_10_years": "consecutive_profitable_years",  # 近似: 盈利年数
    "NO_ANNUAL_DEFICIT_IN_10Y": "min_annual_eps_10y",  # min EPS > 0 即无亏损
    "EPS_5_years_ago": "eps_5y_ago",
    # === 计算衍生 ===
    "intrinsic_value": "intrinsic_value", "INTRINSIC_VALUE": "intrinsic_value",
    "graham_number": "graham_number", "Graham_Number": "graham_number",
    "net_current_assets": "net_current_assets", "NET_CURRENT_ASSETS": "net_current_assets",
    "ncav_per_share": "ncav_per_share", "NCAV": "ncav_per_share",
    "margin_of_safety": "margin_of_safety",
    # === 信用评级 / 质量 ===
    "SP_RATING": "sp_rating", "credit_rating": "sp_rating",
    "BOND_RATING": "sp_rating", "bond_rating": "sp_rating",
    "SP_Ranking": "sp_quality_ranking", "SP_RANKING": "sp_quality_ranking",
    # === 负债比率 ===
    "TOT_DEBT_TO_TOT_ASSET": "debt_to_assets",
    "debt_to_assets": "debt_to_assets",
    "TOTAL_DEBT_TOTAL_ASSETS": "debt_to_assets",
    # === 技术指标 ===
    "RSI": "rsi_14d", "RSI_14D": "rsi_14d", "rsi": "rsi_14d",
    "MACD_LINE": "macd_line", "MACD": "macd_line",
    "SIGNAL_LINE": "macd_signal", "MACD_SIGNAL": "macd_signal",
    "MACD_HIST": "macd_hist",
    "MA_200": "ma_200d", "MA_200D": "ma_200d", "SMA_200": "ma_200d",
    # === 市场基准 ===
    "MARKET_PE": "market_pe", "market_PE": "market_pe",
    "MARKET_AVERAGE_PE": "market_pe", "MARKET_AVG_PE": "market_pe",
    "INDUSTRY_AVG_PE": "industry_avg_pe", "industry_average_PE": "industry_avg_pe",
    "industry_average": "industry_avg_pe",
    "AA_corporate_bond_yield": "aa_bond_yield", "AA_BOND_YIELD": "aa_bond_yield",
    "bond_yield": "aa_bond_yield", "BOND_YIELD": "aa_bond_yield",
    "treasury_yield": "treasury_yield_10y", "TREASURY_YIELD": "treasury_yield_10y",
    # === 行业/板块 (字符串) ===
    "sector": "sector", "SECTOR": "sector",
    "industry": "industry", "INDUSTRY": "industry",
    # === 历史利息覆盖 ===
    "AVG_7Y_PRETAX_INTEREST_COVERAGE": "avg_7y_pretax_interest_coverage",
    "WORST_YEAR_PRETAX_INTEREST_COVERAGE": "worst_year_pretax_interest_coverage",
    "AVG_7Y_AFTERTAX_INTEREST_COVERAGE": "avg_7y_pretax_interest_coverage",
    "WORST_YEAR_AFTERTAX_INTEREST_COVERAGE": "worst_year_pretax_interest_coverage",
    # === 操作比率 ===
    "operating_ratio": "operating_margin",  # 近似
    "market_capitalization": "market_cap",
    "IntrinsicValue": "intrinsic_value",
    "INDUSTRY_AVERAGE": "industry_avg_pe",  # 近似
}

# 不应作为个股评估的规则 — 这些是资产配置/组合管理/债券/基金/并购规则
_NON_STOCK_KEYWORDS = {
    # 资产配置
    "stock_allocation", "bond_allocation", "equity_allocation",
    "INDEX_FUND_ALLOCATION", "INDIVIDUAL_STOCK_ALLOCATION",
    "STOCK_ALLOCATION", "BOND_ALLOCATION",
    "portfolio_stock_count", "DIVERSIFIED_GROUP_COUNT",
    # 债券/基金
    "convertible_bond", "CONVERTIBLE_BOND", "convertible_issues",
    "closed_end_fund", "closed_end_fund_price",
    "expense_ratio", "bond_fund",
    "CONVERSION_PREMIUM", "bond_face_value", "bond_market_price",
    "par_value", "PAR_VALUE", "bond_price", "bond_type",
    "bond_price_pct_of_par", "BOND_PRICE",
    "convertible_bond_fund_annual_expense_ratio",
    # 并购/特殊事件
    "ACQUISITION_OFFER_PRICE", "DEAL_SUCCESS_PROBABILITY",
    "acquisition_driven_revenue_growth_pct",
    "investment_amount", "use_convertible_bond_fund",
    "alternative_bond", "current_bond",
    "alternative_bond_interest_coverage",
    "SUFFICIENT_NUMBER",
    # 公式定义（不是筛选条件）
    "Corrected_Stock_Price", "True_Market_Price",
    "Warrant_Value_Per_Share", "Total_Warrant_Market_Value",
    "real_tangible_equity", "dubious_assets", "omitted_equity_items",
    "warrants_market_value", "acquirer_total_market_cap",
    "BANK_LOAN_GROWTH_YOY",
    # 不可量化的定性变量
    "PROXY_STATEMENT_REVIEWED", "YEARS_OF_FINANCIAL_DATA",
    "FinancialCondition", "IS_NEW_ISSUE", "QUALITY",
    "interest_coverage_trend", "price_difference_minimal",
    "industry_peer_transportation_ratio", "transportation_ratio",
    "capital_deficit", "net_assets",
    "LIQUID_ASSETS_RATIO",
    "INCOME_TAX_PAID", "INCOME_TAX_EXPENSE",
    "company_results",
    # 债券具体属性
    "BondPrice", "ParValue", "ACCUMULATED_DIVIDENDS",
    "investment_grade", "bond_coupon_rate",
    # 并购/特殊事件具体属性
    "implied_value_per_share_via_parent", "direct_purchase_price_per_share",
    "acquisition_funded_by_stock", "stock_overvaluation_flag",
    "intrinsic_value_under_competent_management",
    "reinvestment_return", "shareholder_alternative_return",
    "market_average_growth",
    "PROFITABILITY_RANK_IN_INDUSTRY", "COMPETITIVENESS_RANK_IN_INDUSTRY",
    "profitability_forecast",
    "DIVERSIFIED_PORTFOLIO", "industry_peer_PE",
    # 公式中的中间变量
    "book_value_of_common", "debt_types_count",
    "special_items_impact", "deficit_count_last_5_years",
    "moderate_threshold",
    # 税务/资本利得/特殊主题
    "ANNUAL_CAPITAL_LOSS_DEDUCTION", "HOLDING_PERIOD",
    "capital_gains_tax_rate", "INSIDER_CONFLICT_OF_INTEREST",
    "COMPANY_PROFITABILITY", "stock_options_dilution_pct",
    "TOTAL_FIXED_CHARGES", "SAFE_BOND_ISSUANCE_CAPACITY",
    "LARGE_CAP_THRESHOLD", "PORTFOLIO_STOCK_COUNT",
    "business_value",
}

# 这些 token 是字符串字面量/常量/Python关键字，不应被当作变量
_LITERAL_TOKENS = {
    "true", "false", "TRUE", "FALSE", "True", "False",
    "IN", "FOR", "for", "implies", "REJECT",
    # 字符串比较中的字面值
    "industrial", "utility", "utilities", "financial", "technology",
    "strong", "weak", "declining", "high", "low", "moderate",
    "unsatisfactory", "satisfactory",
    # S&P 评级字面量
    "Aaa", "Aa", "Baa",
    # 数学/聚合函数
    "MIN", "MAX", "AVG", "MEDIAN", "SUM",
    # 自然语言动词/形容词（LLM生成的伪代码中常见）
    "PREFER", "IS", "REASONABLE", "INVESTIGATE",
    # 其它常量
    "RED_FLAG", "FAIL", "PASS",
    "never", "profitable",
}


@dataclass
class RuleResult:
    """单条规则评估结果"""
    description: str
    expression: str
    passed: Optional[bool] = None  # True=通过, False=不通过, None=无法评估
    reason: str = ""
    values_used: dict = field(default_factory=dict)


def evaluate_rules(stock: StockData, rules: list[dict]) -> list[RuleResult]:
    """用规则列表评估一只股票"""
    results = []
    stock_dict = stock.__dict__

    for rule in rules:
        expr = rule.get("expression")
        desc = rule.get("description", "")

        if not expr or expr == "None" or expr == "null":
            continue

        result = _evaluate_single(expr, stock_dict, desc)
        results.append(result)

    return results


def _evaluate_single(expression: str, stock_data: dict, description: str) -> RuleResult:
    """评估单条规则表达式"""
    result = RuleResult(description=description, expression=expression)

    try:
        # ── 第 0 步: 含中文的伪表达式直接跳过 ──
        if re.search(r'[\u4e00-\u9fff]', expression):
            result.passed = None
            result.reason = "含中文描述的非标准表达式"
            return result

        # ── 第 0.5 步: 含 => 箭头、% 符号、≈ 等非 Python 语法 → 跳过 ──
        if '=>' in expression or '\u2248' in expression or '\u2265' in expression or '\u2264' in expression:
            result.passed = None
            result.reason = "非标准表达式语法"
            return result
        # 含括号注释如 "(substantial margin required)" → 跳过
        if re.search(r'\([a-z]+\s+[a-z]+\s+[a-z]+', expression):
            result.passed = None
            result.reason = "含自然语言注释的表达式"
            return result
        # EARNINGS_STABILITY = 100% → 跳过
        if '%' in expression:
            result.passed = None
            result.reason = "含百分号的非标准表达式"
            return result

        # ── 第 1 步: 跳过伪代码/策略型规则 ──
        skip_keywords = {"THEN", "IF", "WHEN", "WHERE", "AVOID", "BUY", "SELL",
                         "EXCLUDE", "WARNING", "HIGH_RISK", "UNIVERSE", "RANK",
                         "INVESTIGATE", "REJECT"}
        all_tokens_set = set(re.findall(r'[A-Za-z_][A-Za-z0-9_]*', expression))
        if all_tokens_set & skip_keywords:
            result.passed = None
            result.reason = "非数值表达式 (策略/指令型规则)"
            return result

        # ── 第 2 步: 跳过非个股规则（资产配置/债券/基金） ──
        if all_tokens_set & _NON_STOCK_KEYWORDS:
            result.passed = None
            result.reason = "非个股规则 (资产配置/债券/基金类)"
            return result

        # ── 第 2.5 步: 跳过含 "抽象占位符" 的规则 ──
        _placeholder_tokens = {
            "threshold", "large_cap_threshold", "conservative_threshold",
            "reasonable_threshold", "high_threshold", "LowMultiplier",
            "moderate_level", "margin_of_safety_threshold",
            "SUFFICIENT_NUMBER", "required_coverage_ratio",
        }
        if all_tokens_set & _placeholder_tokens:
            result.passed = None
            result.reason = "含抽象占位符的规则 (需人工判断)"
            return result

        # ── 第 3 步: 表达式预处理/标准化 ──
        work_expr = expression

        # 3a. 布尔变量 = TRUE/true 的规则重写
        work_expr = re.sub(r'NO_ANNUAL_DEFICIT_IN_10Y\s*[==]+\s*(TRUE|true|True)',
                           'min_annual_earnings_last_10_years > 0', work_expr)
        work_expr = re.sub(r'CONTINUOUS_DIVIDEND_RECORD\s*[==]+\s*(TRUE|true|True)',
                           'consecutive_dividend_years > 0', work_expr)

        # 3b. P/B 和 P/E 是特殊的复合变量名，替换为单一 token
        work_expr = re.sub(r'\bP/B\b', '_PB_', work_expr)
        work_expr = re.sub(r'\bP/E\b', '_PE_', work_expr)
        work_expr = re.sub(r'\bP/S\b', '_PS_', work_expr)
        work_expr = re.sub(r'\bD/E\b', '_DE_', work_expr)
        work_expr = re.sub(r'\bE/P\b', '_EP_', work_expr)

        # 3b. 函数调用 AVG(...), MIN(...) 替换为已有变量
        work_expr = re.sub(r'MIN\s*\(EPS[^)]*10[^)]*\)', 'min_annual_earnings_last_10_years', work_expr)
        work_expr = re.sub(r'MIN\s*\(EPS[^)]*5[^)]*\)', 'min_annual_EPS_last_5_years', work_expr)
        work_expr = re.sub(r'AVG\s*\(EPS[^)]*last_3[^)]*\)', 'EPS_recent_3yr_avg', work_expr)
        work_expr = re.sub(r'AVG\s*\(EPS[^)]*first_3[^)]*\)', 'EPS_earliest_3yr_avg', work_expr)
        work_expr = re.sub(r'AVG\s*\(EPS,\s*3Y?\)', 'EPS_3yr_avg', work_expr)
        work_expr = re.sub(r'Price\s*/\s*AVG\s*\(EPS,\s*3Y?\)', 'price / EPS_3yr_avg', work_expr)
        work_expr = re.sub(r'PRICE\s*/\s*AVG_EARNINGS_3Y', 'price / avg_earnings_3yr', work_expr)

        # 3c. "IN (...)" 语法转 Python "in (...)"
        work_expr = re.sub(r'\bIN\s*\(', ' in (', work_expr)

        # ── 第 4 步: 先移除引号内的字符串字面量，避免被提取为变量 ──
        expr_no_strings = re.sub(r"'[^']*'", "''", work_expr)
        expr_no_strings = re.sub(r'"[^"]*"', '""', expr_no_strings)

        # ── 第 5 步: 提取变量 token ──
        tokens = re.findall(r'[A-Za-z_][A-Za-z0-9_/]*', expr_no_strings)

        # 过滤掉关键字、字面量、函数名
        _builtin_names = {"AND", "OR", "NOT", "and", "or", "not", "in",
                          "sqrt", "abs", "max", "min", "len", "sum"}
        tokens = [t for t in tokens if t not in _builtin_names and t not in _LITERAL_TOKENS]

        # ── 第 6 步: 变量解析 ──
        _internal_aliases = {
            "_PB_": "pb", "_PE_": "pe", "_PS_": "ps",
            "_DE_": "debt_to_equity", "_EP_": "earnings_yield",
        }

        safe_vars = {}
        used_vars = {}
        missing = []

        for token in tokens:
            mapped = _internal_aliases.get(token) or VARIABLE_MAP.get(token)
            if mapped and mapped in stock_data:
                val = stock_data[mapped]
                if val is None or val == '' or val == []:
                    missing.append(token)
                else:
                    safe_vars[token] = val
                    used_vars[token] = val
            elif token not in _LITERAL_TOKENS:
                missing.append(token)

        if missing:
            result.passed = None
            result.reason = f"缺少数据: {', '.join(sorted(set(missing)))}"
            result.values_used = used_vars
            return result

        # ── 第 7 步: 表达式转 Python 并执行 ──
        py_expr = work_expr
        py_expr = py_expr.replace(" AND ", " and ").replace(" OR ", " or ").replace(" NOT ", " not ")
        # = (赋值) → == (比较)，但保留 >=, <=, !=, ==
        py_expr = re.sub(r'(?<![<>!=])=(?!=)', '==', py_expr)

        import math
        safe_builtins = {
            "__builtins__": {},
            "sqrt": math.sqrt,
            "abs": abs,
            "max": max,
            "min": min,
            "len": len,
            "sum": sum,
            "True": True,
            "False": False,
        }
        safe_builtins.update(safe_vars)

        result.passed = bool(eval(py_expr, {"__builtins__": {}}, safe_builtins))
        result.values_used = used_vars
        result.reason = "\u2713 通过" if result.passed else "\u2717 不通过"

    except Exception as e:
        result.passed = None
        result.reason = f"表达式解析失败: {e}"

    return result


def load_knowledge_rules(knowledge_dir: str, book_name: str = None) -> list[dict]:
    """加载知识库中的规则"""
    kdir = Path(knowledge_dir)
    all_rules = []

    if book_name:
        dirs = [kdir / book_name]
    else:
        dirs = [d for d in kdir.iterdir() if d.is_dir()]

    for d in dirs:
        kfile = d / "knowledge.json"
        if kfile.exists():
            data = json.loads(kfile.read_text(encoding="utf-8"))
            rules = data.get("rules", [])
            # 只要有表达式的
            rules = [r for r in rules if r.get("expression") and r["expression"] not in ("None", "null", None)]
            all_rules.extend(rules)
            logger.info(f"从 {d.name} 加载了 {len(rules)} 条规则")

    return all_rules


def generate_analysis_report(stock: StockData, results: list[RuleResult]) -> str:
    """生成分析报告文本"""
    passed = [r for r in results if r.passed is True]
    failed = [r for r in results if r.passed is False]
    unknown = [r for r in results if r.passed is None]

    lines = []
    lines.append(f"{'='*60}")
    lines.append(f"  股票分析报告: {stock.name} ({stock.symbol})")
    lines.append(f"{'='*60}")

    # 数据质量报告
    coverage = stock.data_coverage()
    lines.append(f"  [数据质量] 核心: {coverage['core']['pct']}% ({coverage['core']['filled']}/{coverage['core']['total']})  "
                 f"扩展: {coverage['extended']['pct']}%  历史: {coverage['historical']['pct']}%")
    if coverage['missing_core']:
        lines.append(f"  [缺失核心] {', '.join(coverage['missing_core'])}")
    if not stock.is_valid():
        lines.append(f"  ⚠ 警告: 数据可能不完整（缺少股价或公司名称）")
    lines.append(f"")

    lines.append(f"  行业: {stock.sector} / {stock.industry}")
    lines.append(f"  股价: ${(stock.price or 0):.2f}  |  市值: ${(stock.market_cap or 0)/1e9:.1f}B")
    lines.append(f"")
    lines.append(f"  --- 核心估值 ---")
    lines.append(f"  PE (TTM):  {(stock.pe or 0):.1f}    |  Forward PE: {(stock.forward_pe or 0):.1f}")
    lines.append(f"  PB:        {(stock.pb or 0):.1f}    |  PS:         {(stock.ps or 0):.1f}")
    if stock.earnings_yield:
        lines.append(f"  盈利收益率: {stock.earnings_yield:.2f}%")
    if stock.market_pe:
        lines.append(f"  市场PE (S&P500): {stock.market_pe:.1f}  |  行业PE: {(stock.industry_avg_pe or 0):.1f}")
    lines.append(f"")
    lines.append(f"  --- 盈利能力 ---")
    lines.append(f"  ROE:       {(stock.roe or 0)*100:.1f}%   |  EPS:        ${(stock.eps or 0):.2f}")
    if stock.ebit:
        lines.append(f"  EBIT:      ${stock.ebit/1e9:.2f}B")
    if stock.pretax_income:
        lines.append(f"  税前利润:  ${stock.pretax_income/1e9:.2f}B")
    if stock.profit_margin:
        lines.append(f"  净利润率:  {stock.profit_margin*100:.1f}%  |  营业利润率: {(stock.operating_margin or 0)*100:.1f}%")
    lines.append(f"")
    lines.append(f"  --- 财务健康 ---")
    lines.append(f"  负债权益比: {(stock.debt_to_equity or 0):.2f}  |  流动比率:   {(stock.current_ratio or 0):.2f}")
    if stock.current_assets:
        lines.append(f"  流动资产:   ${stock.current_assets/1e9:.2f}B  |  流动负债:   ${(stock.current_liabilities or 0)/1e9:.2f}B")
    if stock.long_term_debt:
        lines.append(f"  长期负债:   ${stock.long_term_debt/1e9:.2f}B")
    if stock.working_capital:
        lines.append(f"  营运资金:   ${stock.working_capital/1e9:.2f}B")
    if stock.interest_coverage_ratio:
        lines.append(f"  利息覆盖率: {stock.interest_coverage_ratio:.1f}x")
    if stock.tangible_book_value:
        lines.append(f"  有形账面值: ${stock.tangible_book_value:.2f}/股")
    if stock.free_cash_flow:
        lines.append(f"  自由现金流: ${stock.free_cash_flow/1e9:.2f}B")
    lines.append(f"  股息率:    {(stock.dividend_yield or 0):.2f}%")
    if stock.dividend_payout_ratio:
        lines.append(f"  派息率:    {stock.dividend_payout_ratio:.1f}%")
    lines.append(f"  52周范围:  ${(stock.price_52w_low or 0):.2f} - ${(stock.price_52w_high or 0):.2f}")

    # Graham 衍生指标
    if stock.graham_number or stock.intrinsic_value or stock.ncav_per_share:
        lines.append(f"")
        lines.append(f"  --- Graham 估值 ---")
        if stock.graham_number:
            lines.append(f"  Graham Number: ${stock.graham_number:.2f}")
        if stock.intrinsic_value:
            lines.append(f"  内在价值:      ${stock.intrinsic_value:.2f}")
        if stock.margin_of_safety:
            pct = stock.margin_of_safety * 100
            tag = "溢价" if pct < 0 else "折价"
            lines.append(f"  安全边际:      {abs(pct):.1f}% ({tag})")
        if stock.ncav_per_share:
            lines.append(f"  NCAV/股:       ${stock.ncav_per_share:.2f}")

    # 技术指标
    if stock.rsi_14d or stock.ma_200d:
        lines.append(f"")
        lines.append(f"  --- 技术指标 ---")
        if stock.rsi_14d:
            lines.append(f"  RSI(14):   {stock.rsi_14d:.1f}")
        if stock.ma_200d:
            lines.append(f"  MA(200):   ${stock.ma_200d:.2f}")
        if stock.macd_line:
            lines.append(f"  MACD:      {stock.macd_line:.2f}  |  Signal: {stock.macd_signal:.2f}")

    # 历史数据
    if stock.avg_eps_10y or stock.consecutive_dividend_years or stock.profitable_years:
        lines.append(f"")
        lines.append(f"  --- 历史数据 (10年) ---")
        if stock.avg_eps_10y:
            lines.append(f"  10年平均EPS: ${stock.avg_eps_10y:.2f}")
        if stock.avg_eps_3y:
            lines.append(f"  3年平均EPS:  ${stock.avg_eps_3y:.2f}")
        if stock.avg_eps_first_3y:
            lines.append(f"  最早3年EPS:  ${stock.avg_eps_first_3y:.2f}")
        if stock.earnings_growth_10y:
            lines.append(f"  EPS 10年CAGR: {stock.earnings_growth_10y*100:.1f}%")
        if stock.eps_growth_5y:
            lines.append(f"  EPS 5年增长:  {stock.eps_growth_5y*100:.1f}%")
        if stock.profitable_years:
            lines.append(f"  盈利年数:     {stock.profitable_years}/10年")
        if stock.consecutive_profitable_years:
            lines.append(f"  连续盈利:     {stock.consecutive_profitable_years} 年")
        if stock.min_annual_eps_10y:
            lines.append(f"  10年最低EPS:  ${stock.min_annual_eps_10y:.2f}")
        if stock.max_eps_decline:
            lines.append(f"  最大EPS下降:  {stock.max_eps_decline*100:.1f}%")
        if stock.consecutive_dividend_years:
            lines.append(f"  连续分红:     {stock.consecutive_dividend_years} 年")
        if stock.revenue_cagr_10y:
            lines.append(f"  收入10年CAGR: {stock.revenue_cagr_10y*100:.1f}%")
        if stock.avg_7y_pretax_interest_coverage:
            lines.append(f"  7年均利息覆盖: {stock.avg_7y_pretax_interest_coverage:.1f}x")
            lines.append(f"  最差年利息覆盖: {stock.worst_year_pretax_interest_coverage:.1f}x")

    # 信用评级
    if stock.sp_rating or stock.moody_rating:
        lines.append(f"")
        lines.append(f"  --- 信用评级 ---")
        if stock.sp_rating:
            lines.append(f"  S&P:   {stock.sp_rating}")
        if stock.moody_rating:
            lines.append(f"  Moody: {stock.moody_rating}")
        if stock.sp_quality_ranking:
            lines.append(f"  S&P Quality: {stock.sp_quality_ranking}")

    # 市场基准
    if stock.treasury_yield_10y or stock.aa_bond_yield:
        lines.append(f"")
        lines.append(f"  --- 市场基准 ---")
        if stock.treasury_yield_10y:
            lines.append(f"  10年国债: {stock.treasury_yield_10y:.2f}%")
        if stock.aa_bond_yield:
            lines.append(f"  AA企业债: {stock.aa_bond_yield:.2f}%")

    lines.append(f"")
    lines.append(f"  --- 规则评估汇总 ---")
    lines.append(f"  可评估规则: {len(passed)+len(failed)} 条")
    lines.append(f"  ✓ 通过: {len(passed)} 条  |  ✗ 不通过: {len(failed)} 条  |  ? 无法评估: {len(unknown)} 条")
    lines.append(f"  通过率: {len(passed)/(len(passed)+len(failed))*100:.0f}%" if (passed or failed) else "  通过率: N/A")
    lines.append(f"")

    if passed:
        lines.append(f"  --- ✓ 通过的规则 (前20条) ---")
        for r in passed[:20]:
            vals = ", ".join(f"{k}={v}" for k, v in r.values_used.items())
            lines.append(f"    ✓ [{r.expression}]")
            lines.append(f"      {r.description[:80]}")
            lines.append(f"      实际值: {vals}")
        if len(passed) > 20:
            lines.append(f"      ... 还有 {len(passed)-20} 条")
        lines.append(f"")

    if failed:
        lines.append(f"  --- ✗ 不通过的规则 (前20条) ---")
        for r in failed[:20]:
            vals = ", ".join(f"{k}={v}" for k, v in r.values_used.items())
            lines.append(f"    ✗ [{r.expression}]")
            lines.append(f"      {r.description[:80]}")
            lines.append(f"      实际值: {vals}")
        if len(failed) > 20:
            lines.append(f"      ... 还有 {len(failed)-20} 条")

    lines.append(f"\n{'='*60}")
    return "\n".join(lines)
