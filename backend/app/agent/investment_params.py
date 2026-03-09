"""Investment Parameters Registry — Centralized, auditable, dynamically adjustable.

All hardcoded screening/scoring thresholds across the system are extracted here.
Supports:
1. View all parameters by category/school with descriptions
2. Runtime override (in-memory) via API
3. External YAML config override (investment_params.yaml)
4. Reset to defaults
5. Parameter audit trail (who changed what, when)

Usage:
    from app.agent.investment_params import params
    pe_limit = params.get("graham.pe_max")                  # 15
    params.override("graham.pe_max", 18, reason="市场均值偏高")  # runtime override
    params.reset("graham.pe_max")                             # back to default
"""

import copy
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  Parameter definition
# ═══════════════════════════════════════════════════════════════

class ParamDef:
    """A single parameter definition with metadata."""
    __slots__ = (
        "key", "default", "value", "description", "category",
        "school", "unit", "min_val", "max_val", "source",
    )

    def __init__(
        self,
        key: str,
        default: Any,
        description: str,
        category: str,
        school: str = "system",
        unit: str = "",
        min_val: Any = None,
        max_val: Any = None,
        source: str = "default",
    ):
        self.key = key
        self.default = default
        self.value = default  # current value — may differ from default after override
        self.description = description
        self.category = category
        self.school = school
        self.unit = unit
        self.min_val = min_val
        self.max_val = max_val
        self.source = source  # "default" | "yaml" | "api_override"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "value": self.value,
            "default": self.default,
            "description": self.description,
            "category": self.category,
            "school": self.school,
            "unit": self.unit,
            "min": self.min_val,
            "max": self.max_val,
            "source": self.source,
            "is_overridden": self.value != self.default,
        }


# ═══════════════════════════════════════════════════════════════
#  Default parameter definitions — ALL system thresholds
# ═══════════════════════════════════════════════════════════════

_DEFAULTS: List[ParamDef] = [
    # ── Graham Deep Value ──────────────────────────────────────
    ParamDef("graham.pe_max", 15, "PE上限——Graham防御型投资者", "valuation", "graham", "x", 5, 30),
    ParamDef("graham.pe_pb_product_max", 22.5, "PE×PB乘积上限——Graham Number", "valuation", "graham", "x", 10, 50),
    ParamDef("graham.margin_of_safety_min", 0.33, "最低安全边际", "valuation", "graham", "%", 0.1, 0.6),
    ParamDef("graham.current_ratio_min", 2.0, "最低流动比率", "financial_health", "graham", "x", 1.0, 4.0),
    ParamDef("graham.debt_to_equity_max", 1.0, "最大负债权益比", "financial_health", "graham", "x", 0.3, 3.0),
    ParamDef("graham.profitable_years_min", 10, "最低连续盈利年数", "quality", "graham", "年", 3, 20),
    ParamDef("graham.dividend_years_min", 20, "最低连续分红年数", "dividend", "graham", "年", 5, 30),
    ParamDef("graham.market_cap_min", 1e9, "最低市值(美元)", "quality", "graham", "$", 1e8, 1e11),
    ParamDef("graham.ncav_discount", 0.67, "NCAV折扣比例(股价<NCAV*此值)", "valuation", "graham", "x", 0.3, 1.0),

    # ── Buffett Quality Moat ────────────────────────────────────
    ParamDef("buffett.roe_min", 0.15, "最低ROE——护城河指标", "profitability", "buffett", "%", 0.05, 0.40),
    ParamDef("buffett.profit_margin_min", 0.10, "最低净利润率", "profitability", "buffett", "%", 0.03, 0.30),
    ParamDef("buffett.debt_to_equity_max", 0.5, "最大负债权益比", "financial_health", "buffett", "x", 0.1, 2.0),
    ParamDef("buffett.margin_of_safety_min", 0.25, "最低安全边际", "valuation", "buffett", "%", 0.1, 0.5),
    ParamDef("buffett.operating_margin_min", 0.15, "最低营业利润率", "profitability", "buffett", "%", 0.05, 0.30),
    ParamDef("buffett.earnings_growth_10y_min", 0.03, "10年盈利CAGR最低要求", "growth", "buffett", "%", 0.0, 0.15),
    ParamDef("buffett.profitable_years_min", 10, "最低连续盈利年数", "quality", "buffett", "年", 3, 20),

    # ── Quantitative Value ──────────────────────────────────────
    ParamDef("quantitative.earnings_yield_min", 0.08, "最低盈利收益率(EBIT/EV)", "valuation", "quantitative", "%", 0.03, 0.20),
    ParamDef("quantitative.roe_min", 0.15, "最低ROE", "profitability", "quantitative", "%", 0.05, 0.30),
    ParamDef("quantitative.interest_coverage_min", 3, "最低利息覆盖率", "financial_health", "quantitative", "x", 1, 10),
    ParamDef("quantitative.market_cap_min", 1e8, "最低市值(美元)", "quality", "quantitative", "$", 1e7, 1e10),
    ParamDef("quantitative.ps_max", 1.5, "最大P/S比", "valuation", "quantitative", "x", 0.5, 5.0),
    ParamDef("quantitative.greenblatt_roe_min", 0.20, "Greenblatt魔法公式最低ROE", "composite", "quantitative", "%", 0.10, 0.40),

    # ── Quality Investing ───────────────────────────────────────
    ParamDef("quality.roe_min", 0.15, "最低ROE", "profitability", "quality", "%", 0.05, 0.30),
    ParamDef("quality.profitable_years_min", 8, "ROE持续最低年数", "quality", "quality", "年", 3, 15),
    ParamDef("quality.operating_margin_min", 0.15, "最低营业利润率", "profitability", "quality", "%", 0.05, 0.30),
    ParamDef("quality.debt_to_equity_max", 0.5, "最大负债权益比", "financial_health", "quality", "x", 0.1, 2.0),
    ParamDef("quality.revenue_growth_min", 0.05, "最低营收增长率", "growth", "quality", "%", 0.0, 0.20),
    ParamDef("quality.max_eps_decline", -0.30, "最大EPS下降幅度", "quality", "quality", "%", -0.60, 0.0),
    ParamDef("quality.eps_growth_min", 0.05, "最低EPS增长率", "growth", "quality", "%", 0.0, 0.20),
    ParamDef("quality.pe_max", 25, "最大PE", "valuation", "quality", "x", 10, 50),

    # ── Damodaran Valuation ─────────────────────────────────────
    ParamDef("valuation.peg_max", 1.5, "最大PEG", "valuation", "valuation", "x", 0.5, 3.0),
    ParamDef("valuation.operating_margin_min", 0.10, "最低营业利润率", "profitability", "valuation", "%", 0.03, 0.25),

    # ── Contrarian Value ────────────────────────────────────────
    ParamDef("contrarian.price_vs_52w_high_max", 0.70, "股价/52周高点最大比例", "momentum", "contrarian", "x", 0.4, 0.9),
    ParamDef("contrarian.ps_max", 0.75, "最大P/S", "valuation", "contrarian", "x", 0.3, 2.0),
    ParamDef("contrarian.pe_max", 10, "最大PE", "valuation", "contrarian", "x", 3, 20),
    ParamDef("contrarian.dividend_yield_min", 0.04, "最低股息率", "dividend", "contrarian", "%", 0.01, 0.10),
    ParamDef("contrarian.roe_min", 0.08, "基本面最低ROE", "profitability", "contrarian", "%", 0.03, 0.20),

    # ── GARP ────────────────────────────────────────────────────
    ParamDef("garp.eps_growth_min", 0.10, "最低EPS增长率", "growth", "garp", "%", 0.03, 0.30),
    ParamDef("garp.pe_max", 20, "最大PE", "valuation", "garp", "x", 10, 40),
    ParamDef("garp.peg_max", 1.0, "最大PEG", "composite", "garp", "x", 0.5, 2.0),
    ParamDef("garp.roe_min", 0.15, "最低ROE", "profitability", "garp", "%", 0.05, 0.30),
    ParamDef("garp.revenue_growth_min", 0.08, "最低营收增长率", "growth", "garp", "%", 0.03, 0.20),
    ParamDef("garp.debt_to_equity_max", 0.8, "最大负债权益比", "financial_health", "garp", "x", 0.3, 2.0),

    # ── School Pass Rates ───────────────────────────────────────
    ParamDef("school_pass_rate.graham", 0.5, "Graham流派最低通过率", "system", "graham", "%", 0.2, 0.9),
    ParamDef("school_pass_rate.buffett", 0.6, "Buffett流派最低通过率", "system", "buffett", "%", 0.2, 0.9),
    ParamDef("school_pass_rate.quantitative", 0.5, "量化价值流派最低通过率", "system", "quantitative", "%", 0.2, 0.9),
    ParamDef("school_pass_rate.quality", 0.6, "品质投资流派最低通过率", "system", "quality", "%", 0.2, 0.9),
    ParamDef("school_pass_rate.valuation", 0.5, "估值派最低通过率", "system", "valuation", "%", 0.2, 0.9),
    ParamDef("school_pass_rate.contrarian", 0.6, "逆向价值流派最低通过率", "system", "contrarian", "%", 0.2, 0.9),
    ParamDef("school_pass_rate.garp", 0.5, "GARP流派最低通过率", "system", "garp", "%", 0.2, 0.9),

    # ── School Weights (Stage 5 consensus) ──────────────────────
    ParamDef("school_weight.graham", 1.5, "Graham流派权重", "scoring", "graham", "x", 0.0, 5.0),
    ParamDef("school_weight.buffett", 2.0, "Buffett流派权重", "scoring", "buffett", "x", 0.0, 5.0),
    ParamDef("school_weight.quantitative", 1.5, "量化价值流派权重", "scoring", "quantitative", "x", 0.0, 5.0),
    ParamDef("school_weight.quality", 2.0, "品质投资流派权重", "scoring", "quality", "x", 0.0, 5.0),
    ParamDef("school_weight.valuation", 1.5, "估值派权重", "scoring", "valuation", "x", 0.0, 5.0),
    ParamDef("school_weight.contrarian", 0.5, "逆向价值流派权重", "scoring", "contrarian", "x", 0.0, 5.0),
    ParamDef("school_weight.garp", 1.0, "GARP流派权重", "scoring", "garp", "x", 0.0, 5.0),

    # ── Risk Manager Veto Thresholds ────────────────────────────
    ParamDef("risk.z_score_danger", 1.81, "Z-Score否决阈值(低于此值=破产危险区)", "risk", "risk_manager", "", 1.0, 3.0),
    ParamDef("risk.m_score_danger", -1.78, "M-Score否决阈值(高于此值=盈利操纵嫌疑)", "risk", "risk_manager", "", -3.0, 0.0),
    ParamDef("risk.f_score_danger", 3, "F-Score否决阈值(≤此值=财务极度虚弱)", "risk", "risk_manager", "", 1, 5),

    # ── Quick Screener (Stage 1) ────────────────────────────────
    ParamDef("screener.pe_max", 25, "快速筛选PE上限", "valuation", "screener", "x", 10, 60),
    ParamDef("screener.debt_to_equity_max", 2.0, "快速筛选最大负债权益比", "financial_health", "screener", "x", 0.5, 5.0),
    ParamDef("screener.market_cap_min", 5e8, "快速筛选最低市值(美元)", "quality", "screener", "$", 1e7, 1e11),

    # ── Forensics (Stage 4) ─────────────────────────────────────
    ParamDef("forensics.f_score_red_flag", 3, "F-Score红旗阈值(≤此值触发)", "risk", "forensics", "", 1, 5),
    ParamDef("forensics.z_score_danger", 1.81, "Z-Score破产危险线", "risk", "forensics", "", 1.0, 3.0),
    ParamDef("forensics.z_score_grey", 2.99, "Z-Score灰色区上限", "risk", "forensics", "", 2.0, 4.0),
    ParamDef("forensics.m_score_danger", -1.78, "M-Score警戒线", "risk", "forensics", "", -3.0, 0.0),
    ParamDef("forensics.high_debt_flag", 2.0, "高负债率红旗阈值", "risk", "forensics", "x", 1.0, 5.0),
    ParamDef("forensics.high_pe_flag", 50, "极高估值红旗PE阈值", "risk", "forensics", "x", 30, 100),
    ParamDef("forensics.high_red_flags_eliminate", 3, "HIGH级红旗数量淘汰阈值", "risk", "forensics", "", 1, 5),

    # ── Risk Tier (Stage 4) ─────────────────────────────────────
    ParamDef("risk_tier.fortress_f_score_min", 7, "FORTRESS级最低F-Score", "risk", "risk_tier", "", 5, 9),
    ParamDef("risk_tier.solid_f_score_min", 5, "SOLID级最低F-Score", "risk", "risk_tier", "", 3, 8),

    # ── Data Quality (Stage 2) ──────────────────────────────────
    ParamDef("data.core_coverage_min", 40, "核心数据最低覆盖率(%)", "system", "data", "%", 10, 80),
    ParamDef("data.good_enough_coverage", 60, "数据源切换阈值——达到此覆盖率即可", "system", "data", "%", 30, 90),

    # ── Stage 8: Conviction Scoring ─────────────────────────────
    ParamDef("scoring.valuation_max_points", 30, "估值维度满分", "scoring", "conviction", "分", 10, 50),
    ParamDef("scoring.school_consensus_max_points", 25, "流派共识维度满分", "scoring", "conviction", "分", 10, 50),
    ParamDef("scoring.financial_safety_max_points", 20, "财务安全维度满分", "scoring", "conviction", "分", 10, 40),
    ParamDef("scoring.moat_max_points", 10, "护城河维度满分", "scoring", "conviction", "分", 5, 20),
    ParamDef("scoring.llm_max_points", 10, "LLM分析维度满分", "scoring", "conviction", "分", 5, 20),
    ParamDef("scoring.committee_max_points", 15, "投委会辩论维度满分", "scoring", "conviction", "分", 5, 30),
    ParamDef("scoring.veto_penalty", 20, "否决权罚分", "scoring", "conviction", "分", 10, 40),

    # ── Margin of Safety Score Bands ────────────────────────────
    ParamDef("scoring.mos_band_excellent", 0.33, "安全边际满分线", "scoring", "conviction", "%", 0.2, 0.5),
    ParamDef("scoring.mos_band_good", 0.20, "安全边际良好线", "scoring", "conviction", "%", 0.1, 0.4),
    ParamDef("scoring.mos_band_fair", 0.10, "安全边际合格线", "scoring", "conviction", "%", 0.0, 0.3),

    # ── Verdict Thresholds ──────────────────────────────────────
    ParamDef("verdict.strong_buy_score", 75, "STRONG_BUY最低分", "scoring", "verdict", "分", 60, 95),
    ParamDef("verdict.buy_score", 55, "BUY最低分", "scoring", "verdict", "分", 40, 80),
    ParamDef("verdict.hold_score", 35, "HOLD最低分", "scoring", "verdict", "分", 20, 60),
    ParamDef("verdict.avoid_score", 20, "AVOID最低分", "scoring", "verdict", "分", 10, 40),

    # ── Conviction Level Thresholds ─────────────────────────────
    ParamDef("conviction.highest_strong_schools", 3, "HIGHEST级最低强推流派数", "scoring", "conviction", "", 2, 5),
    ParamDef("conviction.highest_mos_min", 0.30, "HIGHEST级最低安全边际", "scoring", "conviction", "%", 0.2, 0.5),
    ParamDef("conviction.high_strong_schools", 2, "HIGH级最低强推流派数", "scoring", "conviction", "", 1, 4),
    ParamDef("conviction.high_mos_min", 0.15, "HIGH级最低安全边际", "scoring", "conviction", "%", 0.1, 0.4),

    # ── Timing Score ────────────────────────────────────────────
    ParamDef("timing.rsi_oversold", 30, "RSI超卖线", "timing", "timing", "", 15, 40),
    ParamDef("timing.rsi_overbought", 70, "RSI超买线", "timing", "timing", "", 60, 85),
    ParamDef("timing.buy_now_threshold", 65, "BUY_NOW时机分", "timing", "timing", "分", 50, 80),
    ParamDef("timing.caution_threshold", 40, "CAUTION时机分", "timing", "timing", "分", 25, 55),

    # ── Position Sizing ─────────────────────────────────────────
    ParamDef("position.highest_pct", 12.0, "HIGHEST信念仓位(%)", "position", "position", "%", 5.0, 20.0),
    ParamDef("position.high_pct", 8.0, "HIGH信念仓位(%)", "position", "position", "%", 3.0, 15.0),
    ParamDef("position.medium_pct", 5.0, "MEDIUM信念仓位(%)", "position", "position", "%", 2.0, 10.0),
    ParamDef("position.low_pct", 3.0, "LOW信念仓位(%)", "position", "position", "%", 1.0, 8.0),
    ParamDef("position.none_pct", 2.0, "NONE信念仓位(%)", "position", "position", "%", 0.5, 5.0),
    ParamDef("position.buy_now_multiplier", 1.2, "BUY_NOW时机仓位乘数", "position", "position", "x", 1.0, 2.0),
    ParamDef("position.caution_multiplier", 0.7, "CAUTION时机仓位乘数", "position", "position", "x", 0.3, 1.0),
    ParamDef("position.max_single_pct", 15.0, "单只股票最大仓位(%)", "position", "position", "%", 5.0, 25.0),

    # ── Stop Loss ───────────────────────────────────────────────
    ParamDef("stoploss.highest_pct", 0.25, "HIGHEST信念止损比例", "position", "stoploss", "%", 0.10, 0.40),
    ParamDef("stoploss.high_pct", 0.20, "HIGH信念止损比例", "position", "stoploss", "%", 0.10, 0.35),
    ParamDef("stoploss.medium_pct", 0.15, "MEDIUM信念止损比例", "position", "stoploss", "%", 0.08, 0.30),
    ParamDef("stoploss.low_pct", 0.12, "LOW信念止损比例", "position", "stoploss", "%", 0.05, 0.25),
    ParamDef("stoploss.none_pct", 0.10, "NONE信念止损比例", "position", "stoploss", "%", 0.05, 0.20),

    # ── Buy Price Range ─────────────────────────────────────────
    ParamDef("buy_price.low_multiplier", 0.67, "理想买入价下限=内在价值×此值", "position", "buy_price", "x", 0.4, 0.9),
    ParamDef("buy_price.high_multiplier", 0.85, "理想买入价上限=内在价值×此值", "position", "buy_price", "x", 0.6, 1.0),

    # ── Valuation Model Parameters ──────────────────────────────
    ParamDef("valuation_model.graham_constant", 22.5, "Graham Number常数(PE×PB上限)", "valuation_model", "valuation_model", "", 15, 30),
    ParamDef("valuation_model.graham_iv_base_pe", 8.5, "Graham内在价值无增长PE", "valuation_model", "valuation_model", "", 6, 12),
    ParamDef("valuation_model.epv_wacc", 0.10, "EPV模型WACC", "valuation_model", "valuation_model", "%", 0.05, 0.20),
    ParamDef("valuation_model.epv_tax_rate", 0.21, "EPV模型税率", "valuation_model", "valuation_model", "%", 0.10, 0.35),
    ParamDef("valuation_model.dcf_terminal_growth", 0.03, "DCF终端增长率", "valuation_model", "valuation_model", "%", 0.01, 0.05),
    ParamDef("valuation_model.dcf_wacc", 0.10, "DCF折现率(WACC)", "valuation_model", "valuation_model", "%", 0.05, 0.20),
    ParamDef("valuation_model.dcf_forecast_years", 5, "DCF预测期(年)", "valuation_model", "valuation_model", "年", 3, 10),
    ParamDef("valuation_model.dcf_max_growth", 0.20, "DCF最大增长率上限", "valuation_model", "valuation_model", "%", 0.10, 0.40),
    ParamDef("valuation_model.ddm_required_return", 0.10, "DDM必要回报率", "valuation_model", "valuation_model", "%", 0.05, 0.20),
    ParamDef("valuation_model.owner_earnings_cap_rate", 0.10, "Owner Earnings资本化率", "valuation_model", "valuation_model", "%", 0.05, 0.20),

    # ── Moat Detection ──────────────────────────────────────────
    ParamDef("moat.wide_roe_min", 0.15, "Wide Moat最低ROE", "quality", "moat", "%", 0.10, 0.25),
    ParamDef("moat.wide_margin_min", 0.10, "Wide Moat最低利润率", "quality", "moat", "%", 0.05, 0.20),
    ParamDef("moat.narrow_roe_min", 0.10, "Narrow Moat最低ROE", "quality", "moat", "%", 0.05, 0.20),

    # ── Committee Debate PM Thresholds ──────────────────────────
    ParamDef("committee.strong_buy_threshold", 0.70, "PM裁决STRONG_BUY加权均分", "scoring", "committee", "", 0.5, 0.9),
    ParamDef("committee.buy_threshold", 0.55, "PM裁决BUY加权均分", "scoring", "committee", "", 0.4, 0.8),
    ParamDef("committee.hold_threshold", 0.35, "PM裁决HOLD加权均分", "scoring", "committee", "", 0.2, 0.6),

    # ── Strategy Templates ──────────────────────────────────────
    ParamDef("strategy.conservative.min_market_cap", 2e9, "保守型最低市值", "strategy", "strategy", "$", 1e8, 1e11),
    ParamDef("strategy.conservative.max_pe", 15, "保守型最大PE", "strategy", "strategy", "x", 5, 30),
    ParamDef("strategy.conservative.max_de", 1.0, "保守型最大D/E", "strategy", "strategy", "x", 0.3, 3.0),
    ParamDef("strategy.conservative.min_current_ratio", 2.0, "保守型最低流动比率", "strategy", "strategy", "x", 1.0, 4.0),
    ParamDef("strategy.conservative.min_margin_of_safety", 0.33, "保守型最低安全边际", "strategy", "strategy", "%", 0.1, 0.6),
    ParamDef("strategy.conservative.max_holdings", 20, "保守型最大持仓数", "strategy", "strategy", "", 5, 50),

    ParamDef("strategy.balanced.min_market_cap", 5e8, "均衡型最低市值", "strategy", "strategy", "$", 1e7, 1e11),
    ParamDef("strategy.balanced.max_pe", 25, "均衡型最大PE", "strategy", "strategy", "x", 10, 50),
    ParamDef("strategy.balanced.max_de", 2.0, "均衡型最大D/E", "strategy", "strategy", "x", 0.5, 5.0),
    ParamDef("strategy.balanced.min_current_ratio", 1.0, "均衡型最低流动比率", "strategy", "strategy", "x", 0.5, 3.0),
    ParamDef("strategy.balanced.min_margin_of_safety", 0.15, "均衡型最低安全边际", "strategy", "strategy", "%", 0.0, 0.5),
    ParamDef("strategy.balanced.max_holdings", 15, "均衡型最大持仓数", "strategy", "strategy", "", 5, 50),

    ParamDef("strategy.aggressive.min_market_cap", 1e8, "进取型最低市值", "strategy", "strategy", "$", 1e6, 1e10),
    ParamDef("strategy.aggressive.max_pe", 40, "进取型最大PE", "strategy", "strategy", "x", 15, 80),
    ParamDef("strategy.aggressive.max_de", 3.0, "进取型最大D/E", "strategy", "strategy", "x", 1.0, 10.0),
    ParamDef("strategy.aggressive.min_current_ratio", 0.5, "进取型最低流动比率", "strategy", "strategy", "x", 0.2, 2.0),
    ParamDef("strategy.aggressive.min_margin_of_safety", 0.0, "进取型最低安全边际", "strategy", "strategy", "%", 0.0, 0.3),
    ParamDef("strategy.aggressive.max_holdings", 30, "进取型最大持仓数", "strategy", "strategy", "", 5, 100),

    # ── PIT Backtest Parameters ────────────────────────────────
    ParamDef("backtest.holding_months", 6, "回测持仓周期(月)", "backtest", "backtest", "月", 1, 24),
    ParamDef("backtest.lookback_years", 3.0, "回测回溯年数", "backtest", "backtest", "年", 1.0, 5.0),
    ParamDef("backtest.commission_rate", 0.001, "佣金率(0.1%)", "backtest", "backtest", "%", 0.0, 0.01),
    ParamDef("backtest.slippage_rate", 0.0005, "滑点率(0.05%)", "backtest", "backtest", "%", 0.0, 0.005),
    ParamDef("backtest.stop_loss_pct", 0.15, "止损比例(15%)", "backtest", "backtest", "%", 0.05, 0.40),
    ParamDef("backtest.max_holdings", 15, "最大持仓数", "backtest", "backtest", "", 3, 50),
    ParamDef("backtest.initial_capital", 1_000_000, "初始资金(美元)", "backtest", "backtest", "$", 10_000, 100_000_000),
    ParamDef("backtest.benchmark", "SPY", "基准指数", "backtest", "backtest", "", None, None),
    ParamDef("backtest.validated_win_rate", 0.60, "VALIDATED判定最低胜率", "backtest", "backtest", "%", 0.40, 0.80),
    ParamDef("backtest.validated_avg_alpha", 0.02, "VALIDATED判定最低平均Alpha", "backtest", "backtest", "%", 0.0, 0.10),
    ParamDef("backtest.mixed_win_rate", 0.40, "MIXED判定最低胜率", "backtest", "backtest", "%", 0.20, 0.60),
    ParamDef("backtest.mixed_avg_alpha", -0.02, "MIXED判定最低平均Alpha", "backtest", "backtest", "%", -0.10, 0.05),
]


# ═══════════════════════════════════════════════════════════════
#  Parameter Registry — singleton
# ═══════════════════════════════════════════════════════════════

class InvestmentParamsRegistry:
    """Thread-safe, singleton investment parameter registry."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        if self._initialized:
            return
        self._params: Dict[str, ParamDef] = {}
        self._change_log: List[Dict[str, Any]] = []
        self._load_defaults()
        self._load_yaml_overrides()
        self._initialized = True

    def _load_defaults(self):
        """Load all default parameter definitions."""
        for p in _DEFAULTS:
            self._params[p.key] = copy.deepcopy(p)

    def _load_yaml_overrides(self):
        """Load overrides from investment_params.yaml if it exists."""
        yaml_path = Path(__file__).resolve().parent.parent.parent.parent / "investment_params.yaml"
        if not yaml_path.exists():
            # Also check config directory
            yaml_path = Path(__file__).resolve().parent.parent.parent / "investment_params.yaml"
        if not yaml_path.exists():
            return

        try:
            import yaml
            with open(yaml_path, "r", encoding="utf-8") as f:
                overrides = yaml.safe_load(f)

            if not isinstance(overrides, dict):
                return

            count = 0
            for key, value in self._flatten_dict(overrides):
                if key in self._params:
                    self._params[key].value = value
                    self._params[key].source = "yaml"
                    count += 1
                    self._log_change(key, self._params[key].default, value, "yaml_load")

            if count:
                logger.info(f"[InvestmentParams] Loaded {count} overrides from {yaml_path}")

        except Exception as e:
            logger.warning(f"[InvestmentParams] Failed to load YAML overrides: {e}")

    @staticmethod
    def _flatten_dict(d: dict, prefix: str = "") -> List[Tuple[str, Any]]:
        """Flatten nested dict: {a: {b: 1}} → [('a.b', 1)]."""
        items = []
        for k, v in d.items():
            new_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                items.extend(InvestmentParamsRegistry._flatten_dict(v, new_key))
            else:
                items.append((new_key, v))
        return items

    # ── Public API ──

    def get(self, key: str, default: Any = None) -> Any:
        """Get a parameter value by key."""
        p = self._params.get(key)
        return p.value if p else default

    def get_def(self, key: str) -> Optional[ParamDef]:
        """Get the full parameter definition."""
        return self._params.get(key)

    def override(self, key: str, value: Any, reason: str = "") -> bool:
        """Override a parameter value at runtime.

        Returns True if successful, False if key not found or value out of range.
        """
        p = self._params.get(key)
        if not p:
            return False

        # Validate range
        if p.min_val is not None and value < p.min_val:
            logger.warning(f"[Params] {key}={value} below min {p.min_val}")
            return False
        if p.max_val is not None and value > p.max_val:
            logger.warning(f"[Params] {key}={value} above max {p.max_val}")
            return False

        old_value = p.value
        p.value = value
        p.source = "api_override"
        self._log_change(key, old_value, value, "api_override", reason)
        logger.info(f"[Params] Override: {key} = {old_value} → {value} ({reason})")
        return True

    def reset(self, key: str) -> bool:
        """Reset a parameter to its default value."""
        p = self._params.get(key)
        if not p:
            return False
        old = p.value
        p.value = p.default
        p.source = "default"
        self._log_change(key, old, p.default, "reset")
        return True

    def reset_all(self):
        """Reset all parameters to defaults."""
        for p in self._params.values():
            p.value = p.default
            p.source = "default"
        self._log_change("*", None, None, "reset_all")

    def batch_override(self, overrides: Dict[str, Any], reason: str = "") -> Dict[str, bool]:
        """Override multiple parameters. Returns {key: success}."""
        results = {}
        for key, value in overrides.items():
            results[key] = self.override(key, value, reason)
        return results

    def list_all(self) -> List[Dict[str, Any]]:
        """List all parameters as dicts."""
        return [p.to_dict() for p in self._params.values()]

    def list_by_school(self, school: str) -> List[Dict[str, Any]]:
        """List parameters for a specific school."""
        return [p.to_dict() for p in self._params.values() if p.school == school]

    def list_by_category(self, category: str) -> List[Dict[str, Any]]:
        """List parameters for a specific category."""
        return [p.to_dict() for p in self._params.values() if p.category == category]

    def list_overridden(self) -> List[Dict[str, Any]]:
        """List only parameters that differ from their defaults."""
        return [p.to_dict() for p in self._params.values() if p.value != p.default]

    def get_change_log(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get the parameter change audit trail."""
        return self._change_log[-limit:]

    def get_schools(self) -> List[str]:
        """Get all unique school names."""
        return sorted(set(p.school for p in self._params.values()))

    def get_categories(self) -> List[str]:
        """Get all unique category names."""
        return sorted(set(p.category for p in self._params.values()))

    def export_yaml(self) -> str:
        """Export current (non-default) parameters as YAML string."""
        try:
            import yaml
        except ImportError:
            return "# PyYAML not installed — export as flat key=value\n" + "\n".join(
                f"{p.key}: {p.value}" for p in self._params.values() if p.value != p.default
            )

        nested: Dict[str, Any] = {}
        for p in self._params.values():
            if p.value == p.default:
                continue
            parts = p.key.split(".")
            d = nested
            for part in parts[:-1]:
                d = d.setdefault(part, {})
            d[parts[-1]] = p.value

        if not nested:
            return "# All parameters at default values — nothing to export\n"

        return yaml.dump(nested, default_flow_style=False, allow_unicode=True, sort_keys=True)

    def summary(self) -> Dict[str, Any]:
        """Get a high-level summary of the parameter registry."""
        total = len(self._params)
        overridden = sum(1 for p in self._params.values() if p.value != p.default)
        schools = {}
        for p in self._params.values():
            schools.setdefault(p.school, 0)
            schools[p.school] += 1

        return {
            "total_parameters": total,
            "overridden_count": overridden,
            "parameters_by_school": schools,
            "categories": self.get_categories(),
            "change_log_size": len(self._change_log),
        }

    # ── Internal ──

    def _log_change(self, key: str, old_value: Any, new_value: Any, source: str, reason: str = ""):
        self._change_log.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "key": key,
            "old_value": old_value,
            "new_value": new_value,
            "source": source,
            "reason": reason,
        })
        # Keep log bounded
        if len(self._change_log) > 500:
            self._change_log = self._change_log[-300:]

    def reload_yaml(self):
        """Re-load YAML overrides (useful after file edit)."""
        # First reset all to defaults
        for p in self._params.values():
            p.value = p.default
            p.source = "default"
        self._load_yaml_overrides()


# ═══════════════════════════════════════════════════════════════
#  Module-level singleton — importable as `from ... import params`
# ═══════════════════════════════════════════════════════════════

params = InvestmentParamsRegistry()
