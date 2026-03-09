"""Distilled Investment Rules — Extracted & refined from 14 classic value investing books.

1,253 raw strategies → ~80 high-confidence, actionable screening rules organized into
7 investment "schools" (流派). Each rule includes:
- Python expression using StockData fields
- Source book(s) and page concepts
- Required data fields (all available in our system)
- Category and confidence level

These rules power:
1. The enhanced screener (screener.py) — multi-school stock evaluation
2. The LLM persona — giving Old Charlie deep investment knowledge
3. The evaluate_stock_rules tool — per-stock rule evaluation
"""

from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from app.agent.investment_params import params as _P

# ═══════════════════════════════════════════════════════════════
#  Data Structures
# ═══════════════════════════════════════════════════════════════

@dataclass
class InvestmentRule:
    """A single quantitative investment rule."""
    name: str
    expression: str                     # Python expression using StockData field names
    description: str
    category: str                       # valuation|profitability|financial_health|growth|dividend|quality|momentum|composite
    school: str                         # graham|buffett|quantitative|quality|momentum_value|contrarian|growth_at_value
    source_books: List[str]
    data_fields: List[str]
    weight: float = 1.0                 # Importance weight within its school
    confidence: str = "high"            # high|medium
    is_eliminatory: bool = False        # If True, failing this rule = automatic rejection


@dataclass
class InvestmentSchool:
    """An investment school/approach with its rules."""
    name: str
    name_cn: str
    description: str
    philosophy: str
    key_figures: List[str]
    rules: List[InvestmentRule]
    min_pass_rate: float = 0.6          # Minimum % of rules to pass for school approval


# ═══════════════════════════════════════════════════════════════
#  School 1: BENJAMIN GRAHAM — Deep Value / Net-Net
#  Sources: The Intelligent Investor, Value Investing: Graham to Buffett
# ═══════════════════════════════════════════════════════════════

GRAHAM_RULES = [
    InvestmentRule(
        name="Graham PE上限",
        expression="pe is not None and pe > 0 and pe < 15",
        description="P/E不超过15倍——过去12个月盈利的15倍是Graham的硬性上限",
        category="valuation", school="graham",
        source_books=["The Intelligent Investor", "Value Investing: Graham to Buffett"],
        data_fields=["pe"], weight=1.5, confidence="high", is_eliminatory=True,
    ),
    InvestmentRule(
        name="Graham PE×PB复合",
        expression="pe is not None and pb is not None and pe > 0 and pb > 0 and pe * pb < 22.5",
        description="PE × PB 乘积不超过22.5——Graham Number的推导基础",
        category="valuation", school="graham",
        source_books=["The Intelligent Investor"],
        data_fields=["pe", "pb"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="Graham 安全边际≥33%",
        expression="margin_of_safety is not None and margin_of_safety >= 0.33",
        description="股价低于内在价值至少33%——Graham的最低安全边际要求",
        category="valuation", school="graham",
        source_books=["Value Investing: Graham to Buffett", "The Intelligent Investor"],
        data_fields=["margin_of_safety"], weight=2.0, confidence="high",
    ),
    InvestmentRule(
        name="Graham 流动比率",
        expression="current_ratio is not None and current_ratio >= 2.0",
        description="流动比率≥2——Graham要求的财务安全底线",
        category="financial_health", school="graham",
        source_books=["The Intelligent Investor"],
        data_fields=["current_ratio"], weight=1.2, confidence="high",
    ),
    InvestmentRule(
        name="Graham 负债权益比",
        expression="debt_to_equity is not None and debt_to_equity < 1.0",
        description="负债权益比<1——Graham要求总负债不超过净资产",
        category="financial_health", school="graham",
        source_books=["The Intelligent Investor"],
        data_fields=["debt_to_equity"], weight=1.2, confidence="high", is_eliminatory=True,
    ),
    InvestmentRule(
        name="Graham 连续盈利",
        expression="profitable_years is not None and profitable_years >= 10",
        description="连续10年以上盈利——Graham对防御型投资者的要求",
        category="quality", school="graham",
        source_books=["The Intelligent Investor"],
        data_fields=["profitable_years"], weight=1.3, confidence="high",
    ),
    InvestmentRule(
        name="Graham 连续分红",
        expression="consecutive_dividend_years is not None and consecutive_dividend_years >= 20",
        description="至少连续20年分红——Graham防御型投资者标准",
        category="dividend", school="graham",
        source_books=["The Intelligent Investor"],
        data_fields=["consecutive_dividend_years"], weight=1.0, confidence="high",
    ),
    InvestmentRule(
        name="Graham 盈利增长",
        expression="avg_eps_10y is not None and avg_eps_3y is not None and avg_eps_10y > 0 and avg_eps_3y > avg_eps_10y",
        description="近3年平均EPS高于近10年平均EPS——Graham要求盈利有适度增长",
        category="growth", school="graham",
        source_books=["The Intelligent Investor"],
        data_fields=["avg_eps_10y", "avg_eps_3y"], weight=1.0, confidence="high",
    ),
    InvestmentRule(
        name="Graham 市值门槛",
        expression="market_cap is not None and market_cap >= 1e9",
        description="市值≥10亿美元——Graham排除小盘投机股",
        category="quality", school="graham",
        source_books=["The Intelligent Investor"],
        data_fields=["market_cap"], weight=0.8, confidence="medium",
    ),
    InvestmentRule(
        name="Graham NCAV净流动资产价值",
        expression="ncav_per_share is not None and price is not None and ncav_per_share > 0 and price < ncav_per_share * 0.67",
        description="股价低于NCAV的2/3——Graham最经典的「捡烟蒂」策略",
        category="valuation", school="graham",
        source_books=["The Intelligent Investor", "Value Investing: Graham to Buffett"],
        data_fields=["ncav_per_share", "price"], weight=2.0, confidence="high",
    ),
]

# ═══════════════════════════════════════════════════════════════
#  School 2: WARREN BUFFETT — Quality Moat Investing
#  Sources: The Warren Buffett Way, The Essays of Warren Buffett
# ═══════════════════════════════════════════════════════════════

BUFFETT_RULES = [
    InvestmentRule(
        name="Buffett ROE护城河",
        expression="roe is not None and roe > 0.15",
        description="ROE持续>15%——Buffett判断企业是否具备经济护城河的核心指标",
        category="profitability", school="buffett",
        source_books=["The Warren Buffett Way", "The Essays of Warren Buffett"],
        data_fields=["roe"], weight=2.0, confidence="high", is_eliminatory=True,
    ),
    InvestmentRule(
        name="Buffett 净利润率",
        expression="profit_margin is not None and profit_margin > 0.10",
        description="净利润率>10%——高利润率是定价权和护城河的体现",
        category="profitability", school="buffett",
        source_books=["The Warren Buffett Way", "Quality Investing"],
        data_fields=["profit_margin"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="Buffett 低负债",
        expression="debt_to_equity is not None and debt_to_equity < 0.5",
        description="负债权益比<0.5——Buffett偏好几乎无债的优质企业",
        category="financial_health", school="buffett",
        source_books=["The Warren Buffett Way", "The Essays of Warren Buffett"],
        data_fields=["debt_to_equity"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="Buffett 自由现金流为正",
        expression="free_cash_flow is not None and free_cash_flow > 0",
        description="自由现金流为正——Buffett看重「Owner Earnings」(所有者盈余)",
        category="quality", school="buffett",
        source_books=["The Warren Buffett Way", "The Essays of Warren Buffett"],
        data_fields=["free_cash_flow"], weight=1.8, confidence="high", is_eliminatory=True,
    ),
    InvestmentRule(
        name="Buffett 盈利一致性",
        expression="profitable_years is not None and profitable_years >= 10",
        description="至少10年连续盈利——Buffett只投资有长期稳定盈利记录的企业",
        category="quality", school="buffett",
        source_books=["The Warren Buffett Way"],
        data_fields=["profitable_years"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="Buffett 安全边际",
        expression="margin_of_safety is not None and margin_of_safety >= 0.25",
        description="安全边际≥25%——Buffett要求价格大幅低于内在价值",
        category="valuation", school="buffett",
        source_books=["The Warren Buffett Way", "The Essays of Warren Buffett"],
        data_fields=["margin_of_safety"], weight=2.0, confidence="high",
    ),
    InvestmentRule(
        name="Buffett 营业利润率",
        expression="operating_margin is not None and operating_margin > 0.15",
        description="营业利润率>15%——反映企业运营效率和竞争优势",
        category="profitability", school="buffett",
        source_books=["The Warren Buffett Way", "Quality Investing"],
        data_fields=["operating_margin"], weight=1.3, confidence="medium",
    ),
    InvestmentRule(
        name="Buffett 盈利增长",
        expression="earnings_growth_10y is not None and earnings_growth_10y > 0.03",
        description="10年盈利CAGR>3%——Buffett要求企业盈利持续增长",
        category="growth", school="buffett",
        source_books=["The Warren Buffett Way", "The Essays of Warren Buffett"],
        data_fields=["earnings_growth_10y"], weight=1.2, confidence="medium",
    ),
    InvestmentRule(
        name="Buffett 内在价值折扣",
        expression="intrinsic_value is not None and price is not None and intrinsic_value > 0 and price < intrinsic_value",
        description="当前股价低于内在价值估算——Buffett永远不会为企业多付钱",
        category="valuation", school="buffett",
        source_books=["The Warren Buffett Way"],
        data_fields=["intrinsic_value", "price"], weight=1.8, confidence="high",
    ),
]

# ═══════════════════════════════════════════════════════════════
#  School 3: QUANTITATIVE VALUE — Systematic Factor Investing
#  Sources: Quantitative Value, What Works on Wall Street
# ═══════════════════════════════════════════════════════════════

QUANT_VALUE_RULES = [
    InvestmentRule(
        name="QV 盈利收益率(EBIT/EV)",
        expression="earnings_yield is not None and earnings_yield > 0.08",
        description="盈利收益率>8%——Quantitative Value的首选估值指标，赢得了「价格比率赛马」",
        category="valuation", school="quantitative",
        source_books=["Quantitative Value", "What Works on Wall Street"],
        data_fields=["earnings_yield"], weight=2.0, confidence="high", is_eliminatory=True,
    ),
    InvestmentRule(
        name="QV 避免魅力股",
        expression="pe is not None and market_pe is not None and pe < market_pe",
        description="PE低于市场平均——避开高估值魅力股，它们长期跑输市场",
        category="valuation", school="quantitative",
        source_books=["Quantitative Value", "What Works on Wall Street"],
        data_fields=["pe", "market_pe"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="QV 高质量盈利（FCF > NI）",
        expression="free_cash_flow is not None and net_income is not None and net_income > 0 and free_cash_flow > net_income",
        description="自由现金流>净利润——低应计比率，盈利质量高，不易操纵",
        category="quality", school="quantitative",
        source_books=["Quantitative Value"],
        data_fields=["free_cash_flow", "net_income"], weight=1.8, confidence="high",
    ),
    InvestmentRule(
        name="QV ROE质量门槛",
        expression="roe is not None and roe > 0.15",
        description="ROE>15%——辨别特许经营权质量的企业",
        category="profitability", school="quantitative",
        source_books=["Quantitative Value"],
        data_fields=["roe"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="QV 财务健康(利息覆盖)",
        expression="interest_coverage_ratio is not None and interest_coverage_ratio > 3",
        description="利息覆盖率>3——排除财务困境风险",
        category="financial_health", school="quantitative",
        source_books=["Quantitative Value", "The Five Rules"],
        data_fields=["interest_coverage_ratio"], weight=1.3, confidence="high",
    ),
    InvestmentRule(
        name="QV 市值>1亿美元",
        expression="market_cap is not None and market_cap >= 1e8",
        description="排除微盘股——避免流动性陷阱和做空成本",
        category="quality", school="quantitative",
        source_books=["Quantitative Value", "What Works on Wall Street"],
        data_fields=["market_cap"], weight=0.8, confidence="high",
    ),
    InvestmentRule(
        name="QV 低P/S比",
        expression="ps is not None and ps < 1.5",
        description="P/S<1.5——O'Shaughnessy发现低市销率是最有效的单因子之一",
        category="valuation", school="quantitative",
        source_books=["What Works on Wall Street"],
        data_fields=["ps"], weight=1.3, confidence="high",
    ),
    InvestmentRule(
        name="QV 营收正增长",
        expression="revenue_growth_rate is not None and revenue_growth_rate > 0",
        description="营收正增长——基本的业务增长要求",
        category="growth", school="quantitative",
        source_books=["What Works on Wall Street"],
        data_fields=["revenue_growth_rate"], weight=1.0, confidence="medium",
    ),
    InvestmentRule(
        name="QV Greenblatt 魔法公式",
        expression="earnings_yield is not None and roe is not None and earnings_yield > 0.08 and roe > 0.20",
        description="高盈利收益率+高资本回报率——Joel Greenblatt「击败市场的小书」的核心",
        category="composite", school="quantitative",
        source_books=["The Little Book That Still Beats the Market", "Quantitative Value"],
        data_fields=["earnings_yield", "roe"], weight=2.0, confidence="high",
    ),
]

# ═══════════════════════════════════════════════════════════════
#  School 4: QUALITY INVESTING — Durable Competitive Advantage
#  Sources: Quality Investing, The Five Rules for Successful Stock Investing
# ═══════════════════════════════════════════════════════════════

QUALITY_RULES = [
    InvestmentRule(
        name="Quality ROE持续性",
        expression="roe is not None and roe > 0.15 and profitable_years is not None and profitable_years >= 8",
        description="ROE>15%且持续盈利8年+——长期高资本回报是竞争优势的核心证据",
        category="quality", school="quality",
        source_books=["Quality Investing", "The Five Rules"],
        data_fields=["roe", "profitable_years"], weight=2.0, confidence="high", is_eliminatory=True,
    ),
    InvestmentRule(
        name="Quality 营业利润率",
        expression="operating_margin is not None and operating_margin > 0.15",
        description="营业利润率>15%——Morningstar认为这是宽护城河的关键信号",
        category="profitability", school="quality",
        source_books=["The Five Rules", "Quality Investing"],
        data_fields=["operating_margin"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="Quality 自由现金流正",
        expression="free_cash_flow is not None and free_cash_flow > 0",
        description="自由现金流为正——质量投资的基本要求",
        category="quality", school="quality",
        source_books=["Quality Investing"],
        data_fields=["free_cash_flow"], weight=1.5, confidence="high", is_eliminatory=True,
    ),
    InvestmentRule(
        name="Quality 低财务杠杆",
        expression="debt_to_equity is not None and debt_to_equity < 0.5",
        description="负债权益比<0.5——高质量企业不需要大量举债",
        category="financial_health", school="quality",
        source_books=["Quality Investing", "The Five Rules"],
        data_fields=["debt_to_equity"], weight=1.3, confidence="high",
    ),
    InvestmentRule(
        name="Quality 营收增长",
        expression="revenue_growth_rate is not None and revenue_growth_rate > 0.05",
        description="营收增长>5%——Quality Investing强调可持续增长",
        category="growth", school="quality",
        source_books=["Quality Investing"],
        data_fields=["revenue_growth_rate"], weight=1.2, confidence="medium",
    ),
    InvestmentRule(
        name="Quality 盈利稳定性",
        expression="max_eps_decline is not None and max_eps_decline > -0.30",
        description="最大EPS下降幅度不超过30%——高质量企业盈利波动小",
        category="quality", school="quality",
        source_books=["Quality Investing"],
        data_fields=["max_eps_decline"], weight=1.0, confidence="medium",
    ),
    InvestmentRule(
        name="Quality EPS增长",
        expression="eps_growth_rate is not None and eps_growth_rate > 0.05",
        description="EPS增长率>5%——盈利持续增长是质量投资的核心诉求",
        category="growth", school="quality",
        source_books=["Quality Investing", "The Five Rules"],
        data_fields=["eps_growth_rate"], weight=1.2, confidence="medium",
    ),
    InvestmentRule(
        name="Quality 合理估值",
        expression="pe is not None and pe > 0 and pe < 25",
        description="PE<25——即便是优质企业，也不应该为之付出过高代价",
        category="valuation", school="quality",
        source_books=["Quality Investing"],
        data_fields=["pe"], weight=1.0, confidence="medium",
    ),
]

# ═══════════════════════════════════════════════════════════════
#  School 5: DAMODARAN — Valuation-Centric
#  Sources: Investment Valuation, Expectations Investing
# ═══════════════════════════════════════════════════════════════

VALUATION_RULES = [
    InvestmentRule(
        name="Damodaran Forward PE折扣",
        expression="forward_pe is not None and pe is not None and forward_pe > 0 and forward_pe < pe",
        description="前瞻PE低于当前PE——暗示盈利增长预期，估值有望改善",
        category="valuation", school="valuation",
        source_books=["Investment Valuation", "Expectations Investing"],
        data_fields=["forward_pe", "pe"], weight=1.3, confidence="medium",
    ),
    InvestmentRule(
        name="Damodaran 盈利收益率>无风险利率",
        expression="earnings_yield is not None and treasury_yield_10y is not None and earnings_yield > treasury_yield_10y",
        description="盈利收益率>10年国债收益率——股票应该比国债提供更高的回报",
        category="valuation", school="valuation",
        source_books=["Investment Valuation"],
        data_fields=["earnings_yield", "treasury_yield_10y"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="Damodaran PEG合理",
        expression="pe is not None and eps_growth_rate is not None and eps_growth_rate > 0 and pe > 0 and (pe / (eps_growth_rate * 100)) < 1.5",
        description="PEG<1.5——增长调整后的估值合理",
        category="valuation", school="valuation",
        source_books=["Investment Valuation"],
        data_fields=["pe", "eps_growth_rate"], weight=1.3, confidence="medium",
    ),
    InvestmentRule(
        name="Expectations 营业利润率>10%",
        expression="operating_margin is not None and operating_margin > 0.10",
        description="营业利润率>10%——企业有足够的盈利缓冲",
        category="profitability", school="valuation",
        source_books=["Expectations Investing"],
        data_fields=["operating_margin"], weight=1.2, confidence="medium",
    ),
    InvestmentRule(
        name="Damodaran 正FCF",
        expression="free_cash_flow is not None and free_cash_flow > 0",
        description="自由现金流为正——DCF估值的基础要求",
        category="quality", school="valuation",
        source_books=["Investment Valuation"],
        data_fields=["free_cash_flow"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="Expectations 营收增长预期",
        expression="revenue_growth_rate is not None and revenue_growth_rate > 0",
        description="营收正增长——Rappaport认为预期修正驱动股价",
        category="growth", school="valuation",
        source_books=["Expectations Investing"],
        data_fields=["revenue_growth_rate"], weight=1.0, confidence="medium",
    ),
]

# ═══════════════════════════════════════════════════════════════
#  School 6: CONTRARIAN VALUE — Buy the Unloved
#  Sources: What Works on Wall Street, The Education of a Value Investor
# ═══════════════════════════════════════════════════════════════

CONTRARIAN_RULES = [
    InvestmentRule(
        name="逆向 52周低位接近",
        expression="price is not None and price_52w_high is not None and price < price_52w_high * 0.70",
        description="股价低于52周高点30%+——逆向投资者的买入区间",
        category="momentum", school="contrarian",
        source_books=["What Works on Wall Street", "Value Investing: Graham to Buffett"],
        data_fields=["price", "price_52w_high"], weight=1.5, confidence="medium",
    ),
    InvestmentRule(
        name="逆向 低PS深度价值",
        expression="ps is not None and ps < 0.75",
        description="P/S<0.75——O'Shaughnessy发现极低市销率股票显著跑赢",
        category="valuation", school="contrarian",
        source_books=["What Works on Wall Street"],
        data_fields=["ps"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="逆向 低PE逆向",
        expression="pe is not None and pe > 0 and pe < 10",
        description="PE<10——极低估值通常意味着市场过度悲观",
        category="valuation", school="contrarian",
        source_books=["What Works on Wall Street", "Value Investing: Graham to Buffett"],
        data_fields=["pe"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="逆向 高股息收益率",
        expression="dividend_yield is not None and dividend_yield > 0.04",
        description="股息率>4%——高股息通常出现在被抛弃的股票中",
        category="dividend", school="contrarian",
        source_books=["What Works on Wall Street"],
        data_fields=["dividend_yield"], weight=1.3, confidence="high",
    ),
    InvestmentRule(
        name="逆向 基本面健康",
        expression="roe is not None and roe > 0.08 and free_cash_flow is not None and free_cash_flow > 0",
        description="ROE>8%且FCF为正——逆向投资也要确保基本面没有恶化",
        category="quality", school="contrarian",
        source_books=["The Education of a Value Investor"],
        data_fields=["roe", "free_cash_flow"], weight=1.5, confidence="medium",
    ),
]

# ═══════════════════════════════════════════════════════════════
#  School 7: GROWTH AT REASONABLE PRICE (GARP)
#  Sources: The Five Rules, Quality Investing, Expectations Investing
# ═══════════════════════════════════════════════════════════════

GARP_RULES = [
    InvestmentRule(
        name="GARP EPS增长>10%",
        expression="eps_growth_rate is not None and eps_growth_rate > 0.10",
        description="EPS增长率>10%——GARP要求显著的盈利增长",
        category="growth", school="garp",
        source_books=["The Five Rules", "Quality Investing"],
        data_fields=["eps_growth_rate"], weight=1.5, confidence="medium",
    ),
    InvestmentRule(
        name="GARP PE合理",
        expression="pe is not None and pe > 0 and pe < 20",
        description="PE<20——增长要以合理价格获得",
        category="valuation", school="garp",
        source_books=["The Five Rules"],
        data_fields=["pe"], weight=1.3, confidence="medium",
    ),
    InvestmentRule(
        name="GARP PEG<1",
        expression="pe is not None and eps_growth_rate is not None and eps_growth_rate > 0 and pe > 0 and (pe / (eps_growth_rate * 100)) < 1.0",
        description="PEG<1——Peter Lynch的经典指标：增长调整后仍然便宜",
        category="composite", school="garp",
        source_books=["The Five Rules"],
        data_fields=["pe", "eps_growth_rate"], weight=2.0, confidence="high",
    ),
    InvestmentRule(
        name="GARP ROE>15%",
        expression="roe is not None and roe > 0.15",
        description="ROE>15%——增长必须伴随高资本效率",
        category="profitability", school="garp",
        source_books=["Quality Investing", "The Five Rules"],
        data_fields=["roe"], weight=1.5, confidence="high",
    ),
    InvestmentRule(
        name="GARP 营收持续增长",
        expression="revenue_growth_rate is not None and revenue_growth_rate > 0.08",
        description="营收增长>8%——GARP要求顶线增长驱动",
        category="growth", school="garp",
        source_books=["Expectations Investing", "Quality Investing"],
        data_fields=["revenue_growth_rate"], weight=1.3, confidence="medium",
    ),
    InvestmentRule(
        name="GARP 低负债",
        expression="debt_to_equity is not None and debt_to_equity < 0.8",
        description="负债权益比<0.8——增长不应建立在杠杆之上",
        category="financial_health", school="garp",
        source_books=["The Five Rules"],
        data_fields=["debt_to_equity"], weight=1.0, confidence="medium",
    ),
]

# ═══════════════════════════════════════════════════════════════
#  Build Schools
# ═══════════════════════════════════════════════════════════════

SCHOOLS: Dict[str, InvestmentSchool] = {
    "graham": InvestmentSchool(
        name="Graham Deep Value",
        name_cn="格雷厄姆深度价值",
        description="买入价格远低于清算价值或保守估算内在价值的股票，极度强调安全边际和财务稳健",
        philosophy="宁可在精确的范围内大致正确，也不要在错误的范围内精确无误。安全边际是投资的基石。",
        key_figures=["Benjamin Graham", "David Dodd"],
        rules=GRAHAM_RULES,
        min_pass_rate=0.5,
    ),
    "buffett": InvestmentSchool(
        name="Buffett Quality Moat",
        name_cn="巴菲特护城河投资",
        description="以合理价格买入具有持久竞争优势(护城河)的优质企业，长期持有享受复利",
        philosophy="以合理的价格买入一家伟大的公司，远胜于以伟大的价格买入一家合理的公司。",
        key_figures=["Warren Buffett", "Charlie Munger"],
        rules=BUFFETT_RULES,
        min_pass_rate=0.6,
    ),
    "quantitative": InvestmentSchool(
        name="Quantitative Value",
        name_cn="量化价值",
        description="用系统化因子排名选股：高盈利收益率 + 高资本回报 + 低财务风险，避免行为偏差",
        philosophy="系统化投资消除人类认知偏差。让数据说话，不要让情绪主导决策。",
        key_figures=["Joel Greenblatt", "James O'Shaughnessy", "Wesley Gray"],
        rules=QUANT_VALUE_RULES,
        min_pass_rate=0.5,
    ),
    "quality": InvestmentSchool(
        name="Quality Investing",
        name_cn="品质投资",
        description="寻找具有持久竞争优势、高资本回报、可持续增长的优质企业",
        philosophy="时间是优质企业的朋友。持有高质量企业让复利为你工作。",
        key_figures=["Pat Dorsey (Morningstar)", "Lawrence Cunningham"],
        rules=QUALITY_RULES,
        min_pass_rate=0.6,
    ),
    "valuation": InvestmentSchool(
        name="Damodaran Valuation-Centric",
        name_cn="达摩达兰估值派",
        description="从估值角度出发，评估市场预期与实际价值之间的差距",
        philosophy="估值既是科学也是艺术。任何资产都有价格，但不是每个价格都合理。",
        key_figures=["Aswath Damodaran", "Alfred Rappaport"],
        rules=VALUATION_RULES,
        min_pass_rate=0.5,
    ),
    "contrarian": InvestmentSchool(
        name="Contrarian Value",
        name_cn="逆向价值",
        description="在市场恐慌时买入被过度抛售但基本面健康的股票",
        philosophy="别人恐惧时我贪婪，别人贪婪时我恐惧。",
        key_figures=["Guy Spier", "David Dreman", "John Templeton"],
        rules=CONTRARIAN_RULES,
        min_pass_rate=0.6,
    ),
    "garp": InvestmentSchool(
        name="GARP (Growth at Reasonable Price)",
        name_cn="合理价格成长",
        description="寻找高成长但估值合理(PEG<1)的企业，兼顾成长性和安全性",
        philosophy="既要成长，也要价值。PEG是衡量性价比的最佳指标。",
        key_figures=["Peter Lynch", "Pat Dorsey"],
        rules=GARP_RULES,
        min_pass_rate=0.5,
    ),
}

# All rules flattened
ALL_RULES: List[InvestmentRule] = []
for school in SCHOOLS.values():
    ALL_RULES.extend(school.rules)


# ═══════════════════════════════════════════════════════════════
#  Dynamic Rule Builder — rebuilds expressions from current params
# ═══════════════════════════════════════════════════════════════

# Mapping: (param_key) → list of (school_rules_var, rule_index, expression_template)
# expression_template uses {param_key} placeholders that get formatted with current values.
_PARAM_TO_RULE_MAP: Dict[str, List[Dict[str, Any]]] = {
    # Graham
    "graham.pe_max":              [{"school": "graham", "rule_idx": 0, "tpl": "pe is not None and pe > 0 and pe < {v}"}],
    "graham.pe_pb_product_max":   [{"school": "graham", "rule_idx": 1, "tpl": "pe is not None and pb is not None and pe > 0 and pb > 0 and pe * pb < {v}"}],
    "graham.margin_of_safety_min":[{"school": "graham", "rule_idx": 2, "tpl": "margin_of_safety is not None and margin_of_safety >= {v}"}],
    "graham.current_ratio_min":   [{"school": "graham", "rule_idx": 3, "tpl": "current_ratio is not None and current_ratio >= {v}"}],
    "graham.debt_to_equity_max":  [{"school": "graham", "rule_idx": 4, "tpl": "debt_to_equity is not None and debt_to_equity < {v}"}],
    "graham.profitable_years_min":[{"school": "graham", "rule_idx": 5, "tpl": "profitable_years is not None and profitable_years >= {v}"}],
    "graham.dividend_years_min":  [{"school": "graham", "rule_idx": 6, "tpl": "consecutive_dividend_years is not None and consecutive_dividend_years >= {v}"}],
    "graham.market_cap_min":      [{"school": "graham", "rule_idx": 8, "tpl": "market_cap is not None and market_cap >= {v}"}],
    "graham.ncav_discount":       [{"school": "graham", "rule_idx": 9, "tpl": "ncav_per_share is not None and price is not None and ncav_per_share > 0 and price < ncav_per_share * {v}"}],
    # Buffett
    "buffett.roe_min":            [{"school": "buffett", "rule_idx": 0, "tpl": "roe is not None and roe > {v}"}],
    "buffett.profit_margin_min":  [{"school": "buffett", "rule_idx": 1, "tpl": "profit_margin is not None and profit_margin > {v}"}],
    "buffett.debt_to_equity_max": [{"school": "buffett", "rule_idx": 2, "tpl": "debt_to_equity is not None and debt_to_equity < {v}"}],
    "buffett.margin_of_safety_min":[{"school": "buffett", "rule_idx": 5, "tpl": "margin_of_safety is not None and margin_of_safety >= {v}"}],
    "buffett.operating_margin_min":[{"school": "buffett", "rule_idx": 6, "tpl": "operating_margin is not None and operating_margin > {v}"}],
    "buffett.earnings_growth_10y_min":[{"school": "buffett", "rule_idx": 7, "tpl": "earnings_growth_10y is not None and earnings_growth_10y > {v}"}],
    "buffett.profitable_years_min":[{"school": "buffett", "rule_idx": 4, "tpl": "profitable_years is not None and profitable_years >= {v}"}],
    # Quantitative
    "quantitative.earnings_yield_min":[{"school": "quantitative", "rule_idx": 0, "tpl": "earnings_yield is not None and earnings_yield > {v}"}],
    "quantitative.roe_min":       [{"school": "quantitative", "rule_idx": 3, "tpl": "roe is not None and roe > {v}"}],
    "quantitative.interest_coverage_min":[{"school": "quantitative", "rule_idx": 4, "tpl": "interest_coverage_ratio is not None and interest_coverage_ratio > {v}"}],
    "quantitative.market_cap_min":[{"school": "quantitative", "rule_idx": 5, "tpl": "market_cap is not None and market_cap >= {v}"}],
    "quantitative.ps_max":        [{"school": "quantitative", "rule_idx": 6, "tpl": "ps is not None and ps < {v}"}],
    "quantitative.greenblatt_roe_min":[{"school": "quantitative", "rule_idx": 8, "tpl": "earnings_yield is not None and roe is not None and earnings_yield > 0.08 and roe > {v}"}],
    # Quality
    "quality.roe_min":            [{"school": "quality", "rule_idx": 0, "tpl_fn": lambda v: f"roe is not None and roe > {v} and profitable_years is not None and profitable_years >= {_P.get('quality.profitable_years_min', 8)}"}],
    "quality.profitable_years_min":[{"school": "quality", "rule_idx": 0, "tpl_fn": lambda v: f"roe is not None and roe > {_P.get('quality.roe_min', 0.15)} and profitable_years is not None and profitable_years >= {v}"}],
    "quality.operating_margin_min":[{"school": "quality", "rule_idx": 1, "tpl": "operating_margin is not None and operating_margin > {v}"}],
    "quality.debt_to_equity_max": [{"school": "quality", "rule_idx": 3, "tpl": "debt_to_equity is not None and debt_to_equity < {v}"}],
    "quality.revenue_growth_min": [{"school": "quality", "rule_idx": 4, "tpl": "revenue_growth_rate is not None and revenue_growth_rate > {v}"}],
    "quality.max_eps_decline":    [{"school": "quality", "rule_idx": 5, "tpl": "max_eps_decline is not None and max_eps_decline > {v}"}],
    "quality.eps_growth_min":     [{"school": "quality", "rule_idx": 6, "tpl": "eps_growth_rate is not None and eps_growth_rate > {v}"}],
    "quality.pe_max":             [{"school": "quality", "rule_idx": 7, "tpl": "pe is not None and pe > 0 and pe < {v}"}],
    # Valuation
    "valuation.peg_max":          [{"school": "valuation", "rule_idx": 2, "tpl": "pe is not None and eps_growth_rate is not None and eps_growth_rate > 0 and pe > 0 and (pe / (eps_growth_rate * 100)) < {v}"}],
    "valuation.operating_margin_min":[{"school": "valuation", "rule_idx": 3, "tpl": "operating_margin is not None and operating_margin > {v}"}],
    # Contrarian
    "contrarian.price_vs_52w_high_max":[{"school": "contrarian", "rule_idx": 0, "tpl": "price is not None and price_52w_high is not None and price < price_52w_high * {v}"}],
    "contrarian.ps_max":          [{"school": "contrarian", "rule_idx": 1, "tpl": "ps is not None and ps < {v}"}],
    "contrarian.pe_max":          [{"school": "contrarian", "rule_idx": 2, "tpl": "pe is not None and pe > 0 and pe < {v}"}],
    "contrarian.dividend_yield_min":[{"school": "contrarian", "rule_idx": 3, "tpl": "dividend_yield is not None and dividend_yield > {v}"}],
    "contrarian.roe_min":         [{"school": "contrarian", "rule_idx": 4, "tpl": "roe is not None and roe > {v} and free_cash_flow is not None and free_cash_flow > 0"}],
    # GARP
    "garp.eps_growth_min":        [{"school": "garp", "rule_idx": 0, "tpl": "eps_growth_rate is not None and eps_growth_rate > {v}"}],
    "garp.pe_max":                [{"school": "garp", "rule_idx": 1, "tpl": "pe is not None and pe > 0 and pe < {v}"}],
    "garp.peg_max":               [{"school": "garp", "rule_idx": 2, "tpl": "pe is not None and eps_growth_rate is not None and eps_growth_rate > 0 and pe > 0 and (pe / (eps_growth_rate * 100)) < {v}"}],
    "garp.roe_min":               [{"school": "garp", "rule_idx": 3, "tpl": "roe is not None and roe > {v}"}],
    "garp.revenue_growth_min":    [{"school": "garp", "rule_idx": 4, "tpl": "revenue_growth_rate is not None and revenue_growth_rate > {v}"}],
    "garp.debt_to_equity_max":    [{"school": "garp", "rule_idx": 5, "tpl": "debt_to_equity is not None and debt_to_equity < {v}"}],
}

# School key → SCHOOLS dict key mapping
_SCHOOL_RULES_MAP = {
    "graham": GRAHAM_RULES,
    "buffett": BUFFETT_RULES,
    "quantitative": QUANT_VALUE_RULES,
    "quality": QUALITY_RULES,
    "valuation": VALUATION_RULES,
    "contrarian": CONTRARIAN_RULES,
    "garp": GARP_RULES,
}


def _apply_param_overrides():
    """Apply current param values to rule expressions and school pass rates.

    Called before every evaluation to ensure rules reflect latest params.
    """
    # 1. Update rule expressions
    for param_key, mappings in _PARAM_TO_RULE_MAP.items():
        value = _P.get(param_key)
        if value is None:
            continue
        for m in mappings:
            rules_list = _SCHOOL_RULES_MAP.get(m["school"])
            if rules_list and m["rule_idx"] < len(rules_list):
                if "tpl_fn" in m:
                    rules_list[m["rule_idx"]].expression = m["tpl_fn"](value)
                else:
                    rules_list[m["rule_idx"]].expression = m["tpl"].format(v=value)

    # 2. Update school pass rates
    for school_key, school in SCHOOLS.items():
        rate = _P.get(f"school_pass_rate.{school_key}")
        if rate is not None:
            school.min_pass_rate = rate


# ═══════════════════════════════════════════════════════════════
#  Rule Evaluation Engine
# ═══════════════════════════════════════════════════════════════

def evaluate_stock_against_school(
    stock_data: Dict[str, Any],
    school_name: str,
) -> Dict[str, Any]:
    """Evaluate a stock against all rules of a specific school.

    Dynamically applies any parameter overrides before evaluation.

    Args:
        stock_data: Dict of field_name -> value (from StockData.to_dict())
        school_name: Key in SCHOOLS dict

    Returns:
        Dict with: school, passed, failed, skipped, score, max_score, pass_rate, recommendation
    """
    # Apply latest param overrides to rule expressions
    _apply_param_overrides()

    school = SCHOOLS.get(school_name)
    if not school:
        return {"error": f"Unknown school: {school_name}"}

    passed = []
    failed = []
    skipped = []
    score = 0.0
    max_score = 0.0
    has_eliminatory_fail = False

    for rule in school.rules:
        max_score += rule.weight

        # Check if required data is available
        missing = [f for f in rule.data_fields if stock_data.get(f) is None]
        if missing:
            skipped.append({
                "rule": rule.name,
                "reason": f"缺少数据: {', '.join(missing)}",
            })
            continue

        try:
            result = eval(rule.expression, {"__builtins__": {}}, stock_data)
            if result:
                passed.append({
                    "rule": rule.name,
                    "description": rule.description,
                    "weight": rule.weight,
                })
                score += rule.weight
            else:
                failed.append({
                    "rule": rule.name,
                    "description": rule.description,
                    "weight": rule.weight,
                    "is_eliminatory": rule.is_eliminatory,
                })
                if rule.is_eliminatory:
                    has_eliminatory_fail = True
        except Exception:
            skipped.append({"rule": rule.name, "reason": "表达式计算错误"})

    evaluated = len(passed) + len(failed)
    pass_rate = len(passed) / evaluated if evaluated > 0 else 0

    # Determine recommendation
    if has_eliminatory_fail:
        recommendation = "REJECT"
        verdict_cn = "不合格（淘汰性指标未通过）"
    elif pass_rate >= school.min_pass_rate and score >= max_score * 0.6:
        recommendation = "STRONG_PASS"
        verdict_cn = "优秀"
    elif pass_rate >= school.min_pass_rate * 0.8:
        recommendation = "PASS"
        verdict_cn = "合格"
    elif pass_rate >= 0.3:
        recommendation = "MARGINAL"
        verdict_cn = "边缘"
    else:
        recommendation = "FAIL"
        verdict_cn = "不合格"

    return {
        "school": school.name,
        "school_cn": school.name_cn,
        "philosophy": school.philosophy,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "score": round(score, 1),
        "max_score": round(max_score, 1),
        "pass_rate": round(pass_rate, 3),
        "total_rules": len(school.rules),
        "evaluated": evaluated,
        "recommendation": recommendation,
        "verdict_cn": verdict_cn,
    }


def evaluate_stock_all_schools(stock_data: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate a stock against ALL 7 investment schools.

    Returns comprehensive multi-school evaluation.
    """
    results = {}
    best_school = None
    best_score = -1

    for school_name in SCHOOLS:
        result = evaluate_stock_against_school(stock_data, school_name)
        results[school_name] = result
        if result.get("score", 0) > best_score and result.get("recommendation") not in ("REJECT", "FAIL"):
            best_score = result["score"]
            best_school = school_name

    # Summary
    strong_pass = [k for k, v in results.items() if v.get("recommendation") == "STRONG_PASS"]
    passes = [k for k, v in results.items() if v.get("recommendation") == "PASS"]
    rejects = [k for k, v in results.items() if v.get("recommendation") == "REJECT"]

    return {
        "schools": results,
        "best_fit_school": best_school,
        "strong_pass_schools": strong_pass,
        "pass_schools": passes,
        "reject_schools": rejects,
        "overall_score": round(sum(v.get("score", 0) for v in results.values()), 1),
        "overall_max": round(sum(v.get("max_score", 0) for v in results.values()), 1),
    }


def get_school_summary() -> str:
    """Get a formatted summary of all schools for the LLM persona."""
    lines = []
    for name, school in SCHOOLS.items():
        lines.append(f"\n### {school.name_cn} ({school.name})")
        lines.append(f"**代表人物**: {', '.join(school.key_figures)}")
        lines.append(f"**核心理念**: {school.philosophy}")
        lines.append(f"**选股规则 ({len(school.rules)} 条)**:")
        for r in school.rules:
            marker = "⚡" if r.is_eliminatory else "•"
            lines.append(f"  {marker} {r.name}: {r.description}")
    return "\n".join(lines)
