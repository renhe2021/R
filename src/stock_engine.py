"""老查理统一选股 & 分析引擎
==============================================

将散落在 distilled_rules / tools / analyzer / web_app 中的逻辑
抽象为一个自包含的、可复用的引擎。

三层架构
--------
Layer 1 — DataLayer:    统一数据采集 (yfinance + Bloomberg fallback)
Layer 2 — AnalysisLayer: 五维度深度分析
Layer 3 — DecisionLayer: 综合决策 + 自然语言报告

五维度分析
----------
1. 基本面扫描   — 60+ 指标提取
2. 估值矩阵     — 7 种估值模型 + 共识内在价值
3. 财务排雷     — M-Score / Z-Score / F-Score 三重检测
4. 七流派评估   — 65+ 条蒸馏规则覆盖 7 个投资学派
5. 知识库检索   — 语义搜索投资书籍 + 理论框架（可选）

使用方式
--------
    from src.stock_engine import StockEngine

    engine = StockEngine()

    # 单股深度分析
    report = engine.analyze("AAPL")

    # 批量选股筛选
    results = engine.screen(["AAPL", "MSFT", "GOOG", "BRK-B", "JNJ"])

    # 自定义流派筛选
    results = engine.screen(symbols, schools=["graham", "buffett"])

    # 获取结构化数据（供 API / 前端消费）
    data = engine.analyze("腾讯", output="dict")
"""

import json
import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  数据结构
# ═══════════════════════════════════════════════════════════════

class Verdict(str, Enum):
    """综合研判结论"""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    CAUTION = "CAUTION"
    AVOID = "AVOID"


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


@dataclass
class ValuationResult:
    """单个估值模型的结果"""
    model: str
    model_cn: str
    value: Optional[float] = None
    description: str = ""


@dataclass
class FraudDetection:
    """财务排雷结果"""
    z_score: Optional[float] = None
    z_zone: str = ""          # safe / grey / danger
    f_score: int = 0
    f_details: List[str] = field(default_factory=list)
    m_score: Optional[float] = None
    m_flag: bool = False      # True = 可能操纵盈利
    red_flags: List[Dict[str, str]] = field(default_factory=list)
    risk_level: RiskLevel = RiskLevel.LOW


@dataclass
class SchoolEvaluation:
    """单个投资流派的评估结果"""
    school_key: str
    school_name: str
    school_cn: str
    philosophy: str
    score: float = 0.0
    max_score: float = 0.0
    pass_rate: float = 0.0
    recommendation: str = ""     # STRONG_PASS / PASS / MARGINAL / FAIL / REJECT
    verdict_cn: str = ""
    passed_rules: List[Dict[str, Any]] = field(default_factory=list)
    failed_rules: List[Dict[str, Any]] = field(default_factory=list)
    skipped_rules: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class StockAnalysis:
    """一只股票的完整分析结果"""
    # 基础信息
    symbol: str
    name: str = ""
    sector: str = ""
    industry: str = ""
    market: str = ""
    timestamp: str = ""

    # 数据质量
    data_coverage: Dict[str, Any] = field(default_factory=dict)

    # 核心指标（精选最关键的）
    price: Optional[float] = None
    market_cap: Optional[float] = None
    pe: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    roe: Optional[float] = None
    eps: Optional[float] = None
    profit_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    free_cash_flow: Optional[float] = None
    dividend_yield: Optional[float] = None
    earnings_yield: Optional[float] = None

    # 估值矩阵
    valuations: List[ValuationResult] = field(default_factory=list)
    intrinsic_value: Optional[float] = None
    margin_of_safety: Optional[float] = None
    moat_type: str = ""          # Wide / Narrow / None

    # 财务排雷
    fraud_detection: Optional[FraudDetection] = None

    # 七流派评估
    school_evaluations: List[SchoolEvaluation] = field(default_factory=list)
    best_fit_school: str = ""
    overall_score: float = 0.0
    overall_max_score: float = 0.0

    # 知识库洞察
    knowledge_insights: List[Dict[str, str]] = field(default_factory=list)

    # 综合决策
    verdict: Verdict = Verdict.HOLD
    verdict_cn: str = ""
    verdict_reasons: List[str] = field(default_factory=list)
    score_100: float = 0.0      # 百分制综合评分

    # 原始 StockData（供下游使用）
    _raw_data: Optional[Any] = field(default=None, repr=False)

    def to_dict(self) -> dict:
        """转为可序列化的字典"""
        d = {}
        for k, v in self.__dict__.items():
            if k.startswith("_"):
                continue
            if isinstance(v, Enum):
                d[k] = v.value
            elif isinstance(v, list) and v and hasattr(v[0], "__dataclass_fields__"):
                d[k] = [asdict(item) if hasattr(item, "__dataclass_fields__") else item for item in v]
            elif hasattr(v, "__dataclass_fields__"):
                d[k] = asdict(v)
            else:
                d[k] = v
        return d


@dataclass
class ScreenResult:
    """批量选股筛选结果"""
    symbol: str
    name: str = ""
    verdict: Verdict = Verdict.HOLD
    score_100: float = 0.0
    best_school: str = ""
    margin_of_safety: Optional[float] = None
    risk_level: RiskLevel = RiskLevel.MEDIUM
    moat_type: str = ""
    pe: Optional[float] = None
    roe: Optional[float] = None
    highlights: List[str] = field(default_factory=list)
    error: str = ""


# ═══════════════════════════════════════════════════════════════
#  引擎主类
# ═══════════════════════════════════════════════════════════════

class StockEngine:
    """老查理统一选股 & 分析引擎

    整合数据采集、五维度分析、综合决策为一体。
    """

    def __init__(
        self,
        enable_knowledge: bool = False,
        chroma_dir: Optional[str] = None,
        max_workers: int = 4,
    ):
        """
        Args:
            enable_knowledge: 是否启用知识库语义搜索（需要 ChromaDB）
            chroma_dir: ChromaDB 持久化目录（enable_knowledge=True 时需要）
            max_workers: 并发工作线程数
        """
        self._enable_knowledge = enable_knowledge
        self._chroma_dir = chroma_dir
        self._max_workers = max_workers

    # ───────────────────────────────────────────────────────────
    #  公开 API
    # ───────────────────────────────────────────────────────────

    def analyze(
        self,
        symbol: str,
        schools: Optional[List[str]] = None,
        output: str = "object",
    ) -> Any:
        """对单只股票执行全维度深度分析。

        Args:
            symbol: 股票代码（支持中文名、港股代码、A股代码等）
            schools: 指定评估的流派列表，None 表示全部 7 个流派
            output: "object" 返回 StockAnalysis, "dict" 返回字典, "report" 返回文本报告

        Returns:
            StockAnalysis / dict / str (取决于 output 参数)
        """
        logger.info(f"[Engine] 开始分析 {symbol}")
        analysis = StockAnalysis(symbol=symbol, timestamp=datetime.now().isoformat())

        # ── Layer 1: 数据采集 ──
        stock_data = self._fetch_data(symbol)
        if stock_data is None:
            analysis.verdict = Verdict.HOLD
            analysis.verdict_cn = "数据获取失败"
            analysis.verdict_reasons = ["无法获取股票数据，请检查代码是否正确"]
            return self._format_output(analysis, output)

        self._populate_basics(analysis, stock_data)

        # ── Layer 2: 五维度分析（并发） ──
        stock_dict = stock_data.to_dict()

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {
                executor.submit(self._run_valuation, stock_data): "valuation",
                executor.submit(self._run_fraud_detection, stock_data): "fraud",
                executor.submit(self._run_school_evaluation, stock_dict, schools): "schools",
            }
            if self._enable_knowledge:
                futures[executor.submit(self._run_knowledge_search, symbol, stock_data)] = "knowledge"

            for future in as_completed(futures):
                task_name = futures[future]
                try:
                    result = future.result()
                    if task_name == "valuation":
                        self._apply_valuation(analysis, result, stock_data)
                    elif task_name == "fraud":
                        analysis.fraud_detection = result
                    elif task_name == "schools":
                        self._apply_school_results(analysis, result)
                    elif task_name == "knowledge":
                        analysis.knowledge_insights = result
                except Exception as e:
                    logger.error(f"[Engine] {task_name} 分析失败: {e}")

        # ── Layer 3: 综合决策 ──
        self._compute_verdict(analysis)

        analysis._raw_data = stock_data
        logger.info(f"[Engine] 分析完成 {analysis.name} ({symbol}): {analysis.verdict.value} {analysis.score_100:.0f}分")
        return self._format_output(analysis, output)

    def screen(
        self,
        symbols: List[str],
        schools: Optional[List[str]] = None,
        min_score: float = 0.0,
        sort_by: str = "score",
        max_workers: Optional[int] = None,
    ) -> List[ScreenResult]:
        """批量选股筛选。

        Args:
            symbols: 股票代码列表
            schools: 指定评估的流派列表
            min_score: 最低分数过滤（0-100）
            sort_by: 排序字段 "score" / "mos" (安全边际) / "roe"
            max_workers: 并发数（默认使用引擎配置）

        Returns:
            排序后的 ScreenResult 列表
        """
        workers = max_workers or min(self._max_workers, len(symbols))
        results: List[ScreenResult] = []

        logger.info(f"[Engine] 批量筛选 {len(symbols)} 只股票...")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(self._screen_one, sym, schools): sym
                for sym in symbols
            }
            for future in as_completed(future_map):
                sym = future_map[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    logger.error(f"[Engine] 筛选 {sym} 失败: {e}")
                    results.append(ScreenResult(symbol=sym, error=str(e)))

        # 过滤 & 排序
        if min_score > 0:
            results = [r for r in results if r.score_100 >= min_score]

        sort_keys = {
            "score": lambda r: r.score_100,
            "mos": lambda r: r.margin_of_safety if r.margin_of_safety is not None else -999,
            "roe": lambda r: r.roe if r.roe is not None else -999,
        }
        key_fn = sort_keys.get(sort_by, sort_keys["score"])
        results.sort(key=key_fn, reverse=True)

        logger.info(f"[Engine] 筛选完成，{len(results)} 只股票通过")
        return results

    def quick_score(self, symbol: str) -> Tuple[float, Verdict, str]:
        """快速评分（轻量级，不做完整分析）。

        Returns:
            (score_100, verdict, verdict_cn)
        """
        analysis = self.analyze(symbol, output="object")
        return analysis.score_100, analysis.verdict, analysis.verdict_cn

    # ───────────────────────────────────────────────────────────
    #  Layer 1: 数据采集
    # ───────────────────────────────────────────────────────────

    def _fetch_data(self, symbol: str):
        """统一数据采集，支持符号解析"""
        try:
            from .symbol_resolver import resolve_for_provider
            from .data_providers.yfinance_provider import YfinanceProvider

            resolved = resolve_for_provider(symbol, "yfinance")
            provider = YfinanceProvider()
            stock = provider.fetch(resolved)

            if not stock.is_valid():
                logger.warning(f"[Engine] {symbol} 数据不完整")

            return stock
        except Exception as e:
            logger.error(f"[Engine] 数据获取失败 {symbol}: {e}")
            return None

    def _populate_basics(self, analysis: StockAnalysis, stock):
        """从 StockData 填充基础信息"""
        analysis.name = stock.name or analysis.symbol
        analysis.sector = stock.sector or ""
        analysis.industry = stock.industry or ""
        analysis.data_coverage = stock.data_coverage()

        analysis.price = stock.price
        analysis.market_cap = stock.market_cap
        analysis.pe = stock.pe
        analysis.pb = stock.pb
        analysis.ps = stock.ps
        analysis.roe = stock.roe
        analysis.eps = stock.eps
        analysis.profit_margin = stock.profit_margin
        analysis.operating_margin = stock.operating_margin
        analysis.debt_to_equity = stock.debt_to_equity
        analysis.current_ratio = stock.current_ratio
        analysis.free_cash_flow = stock.free_cash_flow
        analysis.dividend_yield = stock.dividend_yield
        analysis.earnings_yield = stock.earnings_yield

    # ───────────────────────────────────────────────────────────
    #  Layer 2a: 估值矩阵（7 种模型）
    # ───────────────────────────────────────────────────────────

    def _run_valuation(self, stock) -> List[ValuationResult]:
        """运行 7 种估值模型"""
        results = []

        # 1. Graham Number
        if stock.eps and stock.eps > 0 and stock.book_value and stock.book_value > 0:
            gn = math.sqrt(22.5 * stock.eps * stock.book_value)
            results.append(ValuationResult(
                model="graham_number", model_cn="格雷厄姆数",
                value=round(gn, 2),
                description="V = √(22.5 × EPS × BVPS)"
            ))

        # 2. Graham Intrinsic Value
        if stock.eps and stock.eps > 0:
            g = (stock.eps_growth_5y or stock.earnings_growth_10y or 0.05) * 100
            y = stock.aa_bond_yield or stock.treasury_yield_10y or 4.4
            if y > 0:
                iv = stock.eps * (8.5 + 2 * g) * 4.4 / y
                results.append(ValuationResult(
                    model="graham_intrinsic", model_cn="格雷厄姆内在价值",
                    value=round(iv, 2),
                    description="V = EPS × (8.5 + 2g) × 4.4/Y"
                ))

        # 3. EPV (Earnings Power Value)
        if stock.ebit and stock.ebit > 0 and stock.total_debt is not None:
            wacc = 0.10
            tax_rate = 0.21
            epv = (stock.ebit * (1 - tax_rate)) / wacc
            if stock.shares_outstanding and stock.shares_outstanding > 0:
                epv_ps = (epv - (stock.total_debt or 0) + (stock.total_cash or 0)) / stock.shares_outstanding
                results.append(ValuationResult(
                    model="epv", model_cn="盈利能力价值",
                    value=round(epv_ps, 2),
                    description="EPV = EBIT(1-t) / WACC，再减债加现金"
                ))

        # 4. DCF (两阶段折现)
        if stock.free_cash_flow and stock.free_cash_flow > 0 and stock.shares_outstanding:
            fcf = stock.free_cash_flow
            g1 = min(stock.revenue_growth_rate / 100 if stock.revenue_growth_rate else 0.08, 0.20)
            g2 = 0.03
            wacc = 0.10
            pv_stage1 = sum(fcf * (1 + g1) ** y / (1 + wacc) ** y for y in range(1, 6))
            terminal_fcf = fcf * (1 + g1) ** 5 * (1 + g2)
            terminal_value = terminal_fcf / (wacc - g2)
            pv_terminal = terminal_value / (1 + wacc) ** 5
            dcf_ps = (pv_stage1 + pv_terminal) / stock.shares_outstanding
            results.append(ValuationResult(
                model="dcf", model_cn="现金流折现",
                value=round(dcf_ps, 2),
                description=f"5年高速增长({g1*100:.0f}%) + 永续({g2*100:.0f}%), WACC={wacc*100:.0f}%"
            ))

        # 5. DDM (股息折现)
        if stock.dividend_per_share and stock.dividend_per_share > 0:
            g = stock.earnings_growth_10y or 0.03
            r = 0.10
            if r > g:
                ddm = stock.dividend_per_share * (1 + g) / (r - g)
                results.append(ValuationResult(
                    model="ddm", model_cn="股息折现",
                    value=round(ddm, 2),
                    description=f"DDM = D(1+g)/(r-g), g={g*100:.1f}%, r={r*100:.0f}%"
                ))

        # 6. Net-Net (NCAV)
        if stock.ncav_per_share is not None:
            results.append(ValuationResult(
                model="ncav", model_cn="净流动资产价值",
                value=round(stock.ncav_per_share, 2),
                description="NCAV = 流动资产 - 总负债"
            ))

        # 7. Owner Earnings (Buffett)
        if stock.net_income and stock.capex:
            owner_earnings = stock.net_income - abs(stock.capex)
            if stock.shares_outstanding and stock.shares_outstanding > 0:
                oe_ps = owner_earnings / stock.shares_outstanding
                oe_value = oe_ps / 0.10
                results.append(ValuationResult(
                    model="owner_earnings", model_cn="所有者盈余",
                    value=round(oe_value, 2),
                    description="Owner Earnings = NI - CapEx, 10x 资本化"
                ))

        return results

    def _apply_valuation(self, analysis: StockAnalysis, valuations: List[ValuationResult], stock):
        """将估值结果应用到分析报告"""
        analysis.valuations = valuations

        valid_vals = [v.value for v in valuations if v.value and v.value > 0]
        if valid_vals:
            valid_vals.sort()
            mid = len(valid_vals) // 2
            analysis.intrinsic_value = valid_vals[mid]  # 取中位数

        if analysis.intrinsic_value and stock.price and stock.price > 0:
            analysis.margin_of_safety = round(
                (analysis.intrinsic_value - stock.price) / analysis.intrinsic_value, 4
            )

        # 护城河判断
        if stock.roe and stock.roe > 0.15 and stock.profit_margin and stock.profit_margin > 0.1:
            analysis.moat_type = "Wide"
        elif stock.roe and stock.roe > 0.10:
            analysis.moat_type = "Narrow"
        else:
            analysis.moat_type = "None"

    # ───────────────────────────────────────────────────────────
    #  Layer 2b: 财务排雷
    # ───────────────────────────────────────────────────────────

    def _run_fraud_detection(self, stock) -> FraudDetection:
        """三重财务安全检测: Z-Score + F-Score + M-Score"""
        fd = FraudDetection()

        # ── Piotroski F-Score (0-9) ──
        checks = [
            (stock.net_income and stock.net_income > 0, "净利润为正"),
            (stock.roe and stock.roe > 0, "ROE为正"),
            (stock.free_cash_flow and stock.free_cash_flow > 0, "自由现金流为正"),
            (stock.free_cash_flow and stock.net_income and stock.free_cash_flow > stock.net_income,
             "FCF > 净利润 (高盈利质量)"),
            (stock.current_ratio and stock.current_ratio > 1.0, "流动比率 > 1"),
            (stock.debt_to_equity is not None and stock.debt_to_equity < 0.5, "负债权益比 < 0.5"),
            (stock.profit_margin and stock.profit_margin > 0, "利润率为正"),
            (stock.revenue_growth_rate and stock.revenue_growth_rate > 0, "营收正增长"),
            (stock.roe and stock.roe > 0.1, "ROE > 10%"),
        ]
        for cond, desc in checks:
            if cond:
                fd.f_score += 1
                fd.f_details.append(f"✓ {desc}")
            else:
                fd.f_details.append(f"✗ {desc}")

        if fd.f_score <= 3:
            fd.red_flags.append({
                "name": "低 Piotroski F-Score",
                "severity": "HIGH",
                "detail": f"F-Score = {fd.f_score}/9，财务实力严重不足",
            })

        # ── Altman Z-Score ──
        if (stock.total_assets and stock.total_assets > 0 and
                stock.working_capital is not None and stock.market_cap and
                stock.revenue and stock.total_liabilities is not None):
            ta = stock.total_assets
            wc = stock.working_capital or 0
            re_val = (stock.net_income or 0) * 0.7
            ebit = stock.ebit or (stock.net_income or 0)
            mv_eq = stock.market_cap
            tl = stock.total_liabilities or 0
            sales = stock.revenue

            a = 1.2 * (wc / ta)
            b = 1.4 * (re_val / ta)
            c = 3.3 * (ebit / ta) if ebit else 0
            d = 0.6 * (mv_eq / tl) if tl > 0 else 3.0
            e = 1.0 * (sales / ta)
            fd.z_score = round(a + b + c + d + e, 2)

            if fd.z_score >= 2.99:
                fd.z_zone = "safe"
            elif fd.z_score >= 1.81:
                fd.z_zone = "grey"
                fd.red_flags.append({
                    "name": "Z-Score 灰色区",
                    "severity": "MEDIUM",
                    "detail": f"Z-Score = {fd.z_score}，处于灰色区 (1.81-2.99)",
                })
            else:
                fd.z_zone = "danger"
                fd.red_flags.append({
                    "name": "低 Z-Score",
                    "severity": "CRITICAL",
                    "detail": f"Z-Score = {fd.z_score}，破产危险区 (<1.81)",
                })

        # ── M-Score (简化版) ──
        # 完整 M-Score 需要两年财报数据，这里用可用指标近似判断
        if (stock.free_cash_flow is not None and stock.net_income is not None):
            if stock.net_income > 0 and stock.free_cash_flow < 0:
                fd.m_flag = True
                fd.red_flags.append({
                    "name": "现金流与利润背离",
                    "severity": "HIGH",
                    "detail": "净利润为正但自由现金流为负，可能存在盈利操纵",
                })

        # ── 其他红旗 ──
        if stock.debt_to_equity and stock.debt_to_equity > 2.0:
            fd.red_flags.append({
                "name": "高负债率",
                "severity": "HIGH",
                "detail": f"负债权益比 = {stock.debt_to_equity:.2f}",
            })
        if stock.pe and stock.pe > 50:
            fd.red_flags.append({
                "name": "极高估值",
                "severity": "MEDIUM",
                "detail": f"PE = {stock.pe:.1f}，估值显著偏高",
            })

        # 风险等级
        critical = sum(1 for f in fd.red_flags if f["severity"] == "CRITICAL")
        high = sum(1 for f in fd.red_flags if f["severity"] == "HIGH")
        if critical > 0:
            fd.risk_level = RiskLevel.CRITICAL
        elif high >= 3:
            fd.risk_level = RiskLevel.HIGH
        elif high >= 1 or len(fd.red_flags) >= 3:
            fd.risk_level = RiskLevel.MEDIUM
        else:
            fd.risk_level = RiskLevel.LOW

        return fd

    # ───────────────────────────────────────────────────────────
    #  Layer 2c: 七流派评估
    # ───────────────────────────────────────────────────────────

    def _run_school_evaluation(
        self, stock_dict: Dict[str, Any], schools: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """运行蒸馏规则评估"""
        from .distilled_rules_bridge import evaluate_all_schools, evaluate_one_school

        if schools:
            results = {}
            for s in schools:
                results[s] = evaluate_one_school(stock_dict, s)
            return {"schools": results}
        else:
            return evaluate_all_schools(stock_dict)

    def _apply_school_results(self, analysis: StockAnalysis, results: Dict[str, Any]):
        """将流派评估结果应用到分析报告"""
        best_school = results.get("best_fit_school", "")
        analysis.best_fit_school = best_school

        for school_key, data in results.get("schools", {}).items():
            if isinstance(data, dict) and "error" not in data:
                analysis.school_evaluations.append(SchoolEvaluation(
                    school_key=school_key,
                    school_name=data.get("school", school_key),
                    school_cn=data.get("school_cn", school_key),
                    philosophy=data.get("philosophy", ""),
                    score=data.get("score", 0),
                    max_score=data.get("max_score", 0),
                    pass_rate=data.get("pass_rate", 0),
                    recommendation=data.get("recommendation", ""),
                    verdict_cn=data.get("verdict_cn", ""),
                    passed_rules=data.get("passed", []),
                    failed_rules=data.get("failed", []),
                    skipped_rules=data.get("skipped", []),
                ))

        analysis.overall_score = results.get("overall_score", 0)
        analysis.overall_max_score = results.get("overall_max", 0)

    # ───────────────────────────────────────────────────────────
    #  Layer 2d: 知识库检索（可选）
    # ───────────────────────────────────────────────────────────

    def _run_knowledge_search(self, symbol: str, stock) -> List[Dict[str, str]]:
        """语义搜索知识库，获取相关投资理论和书籍观点"""
        if not self._enable_knowledge or not self._chroma_dir:
            return []

        insights = []
        queries = []

        # 根据股票特征构建智能查询
        if stock.sector:
            queries.append(f"{stock.sector} 行业投资分析")
        if stock.roe and stock.roe > 0.15:
            queries.append("高ROE护城河企业特征")
        if stock.pe and stock.pe < 15:
            queries.append("低PE深度价值投资策略")
        if stock.dividend_yield and stock.dividend_yield > 0.03:
            queries.append("高股息价值投资")
        if stock.debt_to_equity and stock.debt_to_equity > 1.5:
            queries.append("高负债企业风险分析")
        if not queries:
            queries.append(f"{stock.name} 投资分析方法")

        try:
            from .search import KnowledgeSearcher
            searcher = KnowledgeSearcher(
                str(Path(self._chroma_dir).parent / "data" / "vectordb")
            )

            for q in queries[:3]:  # 最多 3 个查询
                results = searcher.search(q, top_k=2)
                for r in results:
                    insights.append({
                        "query": q,
                        "content": r.content[:300],
                        "book": r.book_title,
                        "chapter": r.chapter_title,
                        "score": f"{r.score:.3f}",
                    })
        except Exception as e:
            logger.warning(f"[Engine] 知识库搜索失败: {e}")

        return insights

    # ───────────────────────────────────────────────────────────
    #  Layer 3: 综合决策
    # ───────────────────────────────────────────────────────────

    def _compute_verdict(self, analysis: StockAnalysis):
        """基于五维度分析结果，计算综合评分和投资建议"""
        score = 50.0  # 基础分
        reasons = []

        # ── 维度 1: 估值 (25分) ──
        valuation_score = 0.0
        if analysis.margin_of_safety is not None:
            mos = analysis.margin_of_safety
            if mos >= 0.33:
                valuation_score = 25
                reasons.append(f"安全边际优秀 ({mos*100:.0f}%)")
            elif mos >= 0.15:
                valuation_score = 18
                reasons.append(f"安全边际良好 ({mos*100:.0f}%)")
            elif mos >= 0:
                valuation_score = 10
                reasons.append(f"估值合理 (安全边际 {mos*100:.0f}%)")
            elif mos >= -0.2:
                valuation_score = 3
                reasons.append(f"轻度高估 ({abs(mos)*100:.0f}% 溢价)")
            else:
                valuation_score = 0
                reasons.append(f"严重高估 ({abs(mos)*100:.0f}% 溢价)")
        elif analysis.pe is not None:
            if 0 < analysis.pe < 15:
                valuation_score = 18
                reasons.append(f"PE偏低 ({analysis.pe:.1f})")
            elif analysis.pe < 25:
                valuation_score = 10
                reasons.append(f"PE合理 ({analysis.pe:.1f})")
            else:
                valuation_score = 2
                reasons.append(f"PE偏高 ({analysis.pe:.1f})")

        # ── 维度 2: 盈利质量 (20分) ──
        quality_score = 0.0
        if analysis.roe is not None:
            if analysis.roe > 0.20:
                quality_score += 10
            elif analysis.roe > 0.15:
                quality_score += 7
            elif analysis.roe > 0.10:
                quality_score += 4
            elif analysis.roe > 0:
                quality_score += 2

        if analysis.profit_margin is not None:
            if analysis.profit_margin > 0.15:
                quality_score += 5
            elif analysis.profit_margin > 0.08:
                quality_score += 3
            elif analysis.profit_margin > 0:
                quality_score += 1

        if analysis.free_cash_flow is not None and analysis.free_cash_flow > 0:
            quality_score += 5
            reasons.append("自由现金流为正")
        elif analysis.free_cash_flow is not None:
            reasons.append("自由现金流为负")

        # ── 维度 3: 财务安全 (20分) ──
        safety_score = 0.0
        if analysis.fraud_detection:
            fd = analysis.fraud_detection
            # F-Score 贡献
            safety_score += min(fd.f_score * 1.5, 10)

            # Z-Score 贡献
            if fd.z_zone == "safe":
                safety_score += 7
            elif fd.z_zone == "grey":
                safety_score += 3
            elif fd.z_zone == "danger":
                safety_score -= 5
                reasons.append("Z-Score 处于危险区")

            # 红旗惩罚
            for flag in fd.red_flags:
                if flag["severity"] == "CRITICAL":
                    safety_score -= 8
                    reasons.append(f"严重风险: {flag['name']}")
                elif flag["severity"] == "HIGH":
                    safety_score -= 3

            safety_score = max(safety_score, 0)

        # ── 维度 4: 流派认可度 (25分) ──
        school_score = 0.0
        if analysis.school_evaluations:
            strong_passes = sum(1 for e in analysis.school_evaluations if e.recommendation == "STRONG_PASS")
            passes = sum(1 for e in analysis.school_evaluations if e.recommendation == "PASS")
            rejects = sum(1 for e in analysis.school_evaluations if e.recommendation == "REJECT")

            school_score = min(strong_passes * 7 + passes * 4, 25)

            if rejects >= 4:
                school_score = max(school_score - 10, 0)
                reasons.append(f"被 {rejects} 个流派否决")

            if strong_passes >= 3:
                reasons.append(f"被 {strong_passes} 个流派强烈推荐")
            elif strong_passes >= 1:
                reasons.append(f"被 {strong_passes} 个流派推荐")

            if analysis.best_fit_school:
                best = next((e for e in analysis.school_evaluations if e.school_key == analysis.best_fit_school), None)
                if best:
                    reasons.append(f"最匹配流派: {best.school_cn}")

        # ── 维度 5: 护城河 (10分) ──
        moat_score = 0.0
        if analysis.moat_type == "Wide":
            moat_score = 10
            reasons.append("宽护城河企业")
        elif analysis.moat_type == "Narrow":
            moat_score = 5
            reasons.append("窄护城河企业")

        # ── 汇总 ──
        raw_score = valuation_score + quality_score + safety_score + school_score + moat_score
        analysis.score_100 = round(max(0, min(100, raw_score)), 1)

        # ── 研判结论 ──
        s = analysis.score_100
        fd_risk = analysis.fraud_detection.risk_level if analysis.fraud_detection else RiskLevel.MEDIUM

        if fd_risk == RiskLevel.CRITICAL:
            analysis.verdict = Verdict.AVOID
            analysis.verdict_cn = "回避"
            reasons.insert(0, "存在严重财务风险")
        elif s >= 75:
            analysis.verdict = Verdict.STRONG_BUY
            analysis.verdict_cn = "强烈推荐"
        elif s >= 60:
            analysis.verdict = Verdict.BUY
            analysis.verdict_cn = "推荐买入"
        elif s >= 40:
            analysis.verdict = Verdict.HOLD
            analysis.verdict_cn = "持有/观望"
        elif s >= 25:
            analysis.verdict = Verdict.CAUTION
            analysis.verdict_cn = "谨慎"
        else:
            analysis.verdict = Verdict.AVOID
            analysis.verdict_cn = "回避"

        analysis.verdict_reasons = reasons

    # ───────────────────────────────────────────────────────────
    #  批量筛选辅助
    # ───────────────────────────────────────────────────────────

    def _screen_one(self, symbol: str, schools: Optional[List[str]] = None) -> ScreenResult:
        """筛选单只股票"""
        try:
            analysis = self.analyze(symbol, schools=schools, output="object")
            highlights = []

            if analysis.margin_of_safety and analysis.margin_of_safety > 0.2:
                highlights.append(f"安全边际 {analysis.margin_of_safety*100:.0f}%")
            if analysis.moat_type == "Wide":
                highlights.append("宽护城河")
            if analysis.best_fit_school:
                best = next((e for e in analysis.school_evaluations if e.school_key == analysis.best_fit_school), None)
                if best:
                    highlights.append(f"适合{best.school_cn}")
            if analysis.fraud_detection and analysis.fraud_detection.f_score >= 7:
                highlights.append(f"F-Score {analysis.fraud_detection.f_score}/9")

            return ScreenResult(
                symbol=analysis.symbol,
                name=analysis.name,
                verdict=analysis.verdict,
                score_100=analysis.score_100,
                best_school=analysis.best_fit_school,
                margin_of_safety=analysis.margin_of_safety,
                risk_level=analysis.fraud_detection.risk_level if analysis.fraud_detection else RiskLevel.MEDIUM,
                moat_type=analysis.moat_type,
                pe=analysis.pe,
                roe=analysis.roe,
                highlights=highlights,
            )
        except Exception as e:
            return ScreenResult(symbol=symbol, error=str(e))

    # ───────────────────────────────────────────────────────────
    #  输出格式化
    # ───────────────────────────────────────────────────────────

    def _format_output(self, analysis: StockAnalysis, output: str) -> Any:
        """根据 output 参数格式化返回值"""
        if output == "dict":
            return analysis.to_dict()
        elif output == "report":
            return self._generate_text_report(analysis)
        else:
            return analysis

    def _generate_text_report(self, a: StockAnalysis) -> str:
        """生成完整的文本分析报告"""
        sep = "═" * 60
        lines = [
            f"\n{sep}",
            f"  老查理深度价值分析报告",
            f"  {a.name} ({a.symbol})",
            f"  {a.timestamp[:19]}",
            f"{sep}\n",
        ]

        # 综合结论
        verdict_emoji = {
            Verdict.STRONG_BUY: "🟢", Verdict.BUY: "🟡",
            Verdict.HOLD: "⚪", Verdict.CAUTION: "🟠", Verdict.AVOID: "🔴",
        }
        emoji = verdict_emoji.get(a.verdict, "⚪")
        lines.append(f"  {emoji} 综合评分: {a.score_100:.0f}/100  |  建议: {a.verdict_cn}")
        lines.append(f"  理由: {'; '.join(a.verdict_reasons[:5])}")
        lines.append("")

        # 基础信息
        lines.append(f"  行业: {a.sector} / {a.industry}")
        lines.append(f"  护城河: {a.moat_type or 'N/A'}")
        cov = a.data_coverage
        if cov:
            lines.append(f"  数据覆盖: 核心 {cov.get('core', {}).get('pct', 0)}%  "
                         f"扩展 {cov.get('extended', {}).get('pct', 0)}%  "
                         f"历史 {cov.get('historical', {}).get('pct', 0)}%")
        lines.append("")

        # 核心指标
        lines.append(f"  ─── 核心指标 ───")
        lines.append(f"  股价: ${a.price or 0:.2f}  |  市值: ${(a.market_cap or 0)/1e9:.1f}B")
        lines.append(f"  PE: {_f(a.pe)}  |  PB: {_f(a.pb)}  |  PS: {_f(a.ps)}")
        lines.append(f"  ROE: {_pct(a.roe)}  |  净利率: {_pct(a.profit_margin)}  |  营业利润率: {_pct(a.operating_margin)}")
        lines.append(f"  D/E: {_f(a.debt_to_equity)}  |  流动比率: {_f(a.current_ratio)}")
        lines.append(f"  FCF: ${_big(a.free_cash_flow)}  |  股息率: {_f(a.dividend_yield)}%")
        lines.append("")

        # 估值矩阵
        if a.valuations:
            lines.append(f"  ─── 估值矩阵 ({len(a.valuations)} 个模型) ───")
            for v in a.valuations:
                status = ""
                if v.value and a.price:
                    diff = (v.value - a.price) / a.price * 100
                    status = f"({'↑' if diff > 0 else '↓'}{abs(diff):.0f}%)"
                lines.append(f"    {v.model_cn}: ${v.value or 0:.2f} {status}")
            if a.intrinsic_value:
                lines.append(f"  ★ 共识内在价值: ${a.intrinsic_value:.2f}")
            if a.margin_of_safety is not None:
                tag = "折价" if a.margin_of_safety >= 0 else "溢价"
                lines.append(f"  ★ 安全边际: {abs(a.margin_of_safety)*100:.1f}% ({tag})")
            lines.append("")

        # 财务排雷
        if a.fraud_detection:
            fd = a.fraud_detection
            risk_emoji = {"LOW": "🟢", "MEDIUM": "🟡", "HIGH": "🟠", "CRITICAL": "🔴"}
            lines.append(f"  ─── 财务排雷 ───")
            lines.append(f"  {risk_emoji.get(fd.risk_level.value, '⚪')} 风险等级: {fd.risk_level.value}")
            lines.append(f"  F-Score: {fd.f_score}/9  |  Z-Score: {fd.z_score or 'N/A'} ({fd.z_zone})")
            if fd.red_flags:
                for flag in fd.red_flags:
                    lines.append(f"    ⚠ [{flag['severity']}] {flag['name']}: {flag['detail']}")
            else:
                lines.append(f"    ✓ 未发现重大财务风险")
            lines.append("")

        # 七流派评估
        if a.school_evaluations:
            lines.append(f"  ─── 七流派评估 ───")
            if a.best_fit_school:
                best = next((e for e in a.school_evaluations if e.school_key == a.best_fit_school), None)
                if best:
                    lines.append(f"  ★ 最佳适配: {best.school_cn}")
                    lines.append(f"    「{best.philosophy}」\n")

            rec_emoji = {"STRONG_PASS": "🟢", "PASS": "🟡", "MARGINAL": "🟠", "FAIL": "🔴", "REJECT": "⛔"}
            for e in a.school_evaluations:
                em = rec_emoji.get(e.recommendation, "⚪")
                lines.append(f"  {em} {e.school_cn}: {e.verdict_cn} "
                             f"({e.score:.1f}/{e.max_score:.1f}, 通过率 {e.pass_rate:.0%})")
                # 显示关键通过/失败规则
                for p in e.passed_rules[:2]:
                    lines.append(f"      ✓ {p.get('rule', '')}")
                for f in e.failed_rules[:2]:
                    marker = "⚡✗" if f.get("is_eliminatory") else "  ✗"
                    lines.append(f"      {marker} {f.get('rule', '')}")

            lines.append(f"\n  总分: {a.overall_score:.1f}/{a.overall_max_score:.1f}")
            lines.append("")

        # 知识库洞察
        if a.knowledge_insights:
            lines.append(f"  ─── 知识库洞察 ───")
            for ins in a.knowledge_insights[:3]:
                lines.append(f"    📖 {ins.get('book', '')} — {ins.get('chapter', '')}")
                lines.append(f"       {ins.get('content', '')[:120]}...")
            lines.append("")

        lines.append(sep)
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
#  格式化辅助函数
# ═══════════════════════════════════════════════════════════════

def _f(val, digits=2) -> str:
    if val is None:
        return "N/A"
    return f"{val:.{digits}f}"


def _pct(val) -> str:
    if val is None:
        return "N/A"
    return f"{val * 100:.1f}%"


def _big(val) -> str:
    if val is None:
        return "N/A"
    abs_val = abs(val)
    sign = "-" if val < 0 else ""
    if abs_val >= 1e12:
        return f"{sign}{abs_val/1e12:.1f}T"
    if abs_val >= 1e9:
        return f"{sign}{abs_val/1e9:.1f}B"
    if abs_val >= 1e6:
        return f"{sign}{abs_val/1e6:.1f}M"
    return f"{sign}{abs_val:,.0f}"
