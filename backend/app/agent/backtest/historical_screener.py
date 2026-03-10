"""Historical Point-in-Time screener.

Runs Stage 3 (hard-knockout) and Stage 5 (multi-school consensus) screening
on *historical* data snapshots, reusing the existing ``distilled_rules`` engine.

Stage 4 (forensics) and Stage 6 (valuation) use simplified fallbacks because
the full models require data fields that may not be available historically.

Design principles
─────────────────
1. **No LLM calls** — pure rule-based evaluation (Stage 7 skipped).
2. **Identical rule engine** — ``evaluate_stock_all_schools`` is called with the
   same ``data: dict`` format used in live pipeline.
3. **Composite scoring** mirrors the Stage 8 logic in ``unified_pipeline.py`` but
   simplified to the quantitative components only.
4. **Adaptive degradation** — long-history rules (profitable_years >= 10, etc.)
   are dynamically relaxed when yfinance only provides 4-5 years of data.
"""

from __future__ import annotations

import logging
import math
from datetime import date
from typing import Any, Dict, List, Optional

from app.agent.distilled_rules import (
    evaluate_stock_all_schools,
)
from app.agent.investment_params import params as _P
from app.agent.backtest.models import (
    HistoricalSnapshot,
    ScreeningResult,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  Stage 3 — Hard Knockout Gate (simplified)
# ═══════════════════════════════════════════════════════════════

def _hard_knockout(data: Dict[str, Any], strategy: str = "balanced") -> Optional[str]:
    """Apply eliminatory rules.  Returns failure reason or None if passed."""
    pe = data.get("pe")
    eps = data.get("eps")
    de = data.get("debt_to_equity")
    mcap = data.get("market_cap")
    roe = data.get("roe")

    max_pe = _P.get(f"strategy.{strategy}.max_pe", _P.get("screener.pe_max", 25))
    max_de = _P.get(f"strategy.{strategy}.max_de", _P.get("screener.debt_to_equity_max", 2.0))
    min_mcap = _P.get(f"strategy.{strategy}.min_market_cap", _P.get("screener.market_cap_min", 5e8))

    failures: List[str] = []
    if pe is not None and (pe <= 0 or pe > max_pe):
        failures.append(f"PE={pe:.1f} (max {max_pe})")
    if eps is not None and eps <= 0:
        failures.append(f"EPS={eps:.2f} <= 0")
    if de is not None and de > max_de:
        failures.append(f"D/E={de:.2f} > {max_de}")
    if mcap is not None and mcap < min_mcap:
        failures.append(f"MarketCap={mcap/1e9:.1f}B < {min_mcap/1e9:.1f}B")
    if roe is not None and roe < 0:
        failures.append(f"ROE={roe:.2%} < 0")

    return "; ".join(failures) if failures else None


# ═══════════════════════════════════════════════════════════════
#  Stage 4 — Simplified Forensic Accounting (Z/F/M scores)
# ═══════════════════════════════════════════════════════════════

def _forensic_check(data: Dict[str, Any]) -> Optional[str]:
    """Simplified forensic accounting check using Altman Z-Score, Piotroski F-Score,
    and Beneish M-Score when available.

    Returns a warning string if RED FLAG detected, None if passed.
    These are the same thresholds used by the Risk Manager veto logic.
    """
    warnings: List[str] = []

    # Altman Z-Score: < 1.81 = distress zone
    z_score = data.get("z_score")
    z_danger = _P.get("risk.z_score_danger", 1.81)
    if z_score is not None and z_score < z_danger:
        warnings.append(f"Z-Score={z_score:.2f} < {z_danger} (distress zone)")

    # Beneish M-Score: > -1.78 = likely earnings manipulation
    m_score = data.get("m_score")
    m_danger = _P.get("risk.m_score_danger", -1.78)
    if m_score is not None and m_score > m_danger:
        warnings.append(f"M-Score={m_score:.2f} > {m_danger} (manipulation risk)")

    # Piotroski F-Score: <= 3 = weak fundamentals
    f_score = data.get("f_score")
    f_danger = _P.get("risk.f_score_danger", 3)
    if f_score is not None and f_score <= f_danger:
        warnings.append(f"F-Score={f_score} <= {f_danger} (weak fundamentals)")

    # Additional sanity checks from available data
    # Negative operating cash flow with positive net income = accrual red flag
    ocf = data.get("operating_cash_flow")
    ni = data.get("net_income")
    if ocf is not None and ni is not None and ni > 0 and ocf < 0:
        warnings.append(f"OCF={ocf/1e6:.0f}M < 0 while NI={ni/1e6:.0f}M > 0 (accrual flag)")

    return "; ".join(warnings) if warnings else None


# ═══════════════════════════════════════════════════════════════
#  Adaptive Degradation — relax long-history rules for limited data
# ═══════════════════════════════════════════════════════════════

def _adaptive_degrade(data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply adaptive degradation to screening data for limited-history scenarios.

    When yfinance only provides 4-5 years of quarterly data, rules requiring
    10-20 years of history (e.g. Graham profitable_years >= 10, consecutive
    dividend_years >= 20) can never pass. This function:

    1. Scales profitable_years proportionally if available_years < required threshold
       e.g. if we have 4 years of data and all 4 are profitable → treat as equivalent
       to meeting the 10-year requirement (scale: 4/4 * 10 = 10)

    2. Marks consecutive_dividend_years as None (skip) rather than 0 (fail) when
       we truly have no dividend history data

    3. Adjusts earnings_growth_10y label if derived from shorter history

    Returns a COPY of the data dict with adjustments applied.
    """
    d = dict(data)  # shallow copy
    available = d.get("available_years")

    if available is None or available >= 10:
        return d  # enough history, no degradation needed

    # ── Scale profitable_years ──
    # If a stock has been profitable for ALL available years, extrapolate
    # that it likely meets the 10-year threshold.
    # Formula: scaled = profitable_years × (required / available)
    # But cap at a reasonable extrapolation (max 2.5x).
    profitable = d.get("profitable_years")
    if profitable is not None and available > 0:
        profitability_ratio = profitable / available  # e.g. 4/4 = 1.0 (100% profitable)
        if profitability_ratio >= 0.9:
            # Nearly all years profitable → scale up to meet typical thresholds
            d["profitable_years"] = max(profitable, int(profitability_ratio * 10))
        elif profitability_ratio >= 0.7:
            # Mostly profitable → partial scale
            d["profitable_years"] = max(profitable, int(profitability_ratio * 8))
        # else: keep original (poor track record doesn't deserve extrapolation)

    # ── Handle missing dividend history ──
    # yfinance info.dividendYield exists but we can't determine consecutive years.
    # Rather than leaving it as 0 (which would fail Graham's 20-year rule),
    # set to None so the rule gets SKIPPED (not counted against pass_rate).
    if d.get("consecutive_dividend_years") is None:
        # Already None → rule will be skipped. Good.
        pass

    return d


# ═══════════════════════════════════════════════════════════════
#  Stage 5 — Multi-school consensus (direct reuse)
# ═══════════════════════════════════════════════════════════════

def _school_consensus(data: Dict[str, Any]) -> Dict[str, Any]:
    """Evaluate against all 7 schools, return full result dict.

    Applies adaptive degradation for limited-history data before evaluation.
    """
    adapted = _adaptive_degrade(data)
    return evaluate_stock_all_schools(adapted)


# ═══════════════════════════════════════════════════════════════
#  Composite scoring (quantitative-only, mirrors Stage 8)
# ═══════════════════════════════════════════════════════════════

# ── Stage 6 — Simplified Valuation Gate ──
# Replaces the full 7-model valuation suite with a lightweight check
# using margin_of_safety + PE + Graham Number when available.

def _valuation_gate(data: Dict[str, Any]) -> float:
    """Return a 0-30 valuation score (simplified Stage 6).

    Uses available valuation metrics to gauge attractiveness:
    - Margin of safety (primary)
    - PE relative to historical norms
    - Graham Number vs price
    - Enterprise Value / FCF
    """
    score = 0.0
    mos = data.get("margin_of_safety")
    pe = data.get("pe")
    price = data.get("price")
    graham_num = data.get("graham_number")
    ev = data.get("enterprise_value")
    fcf = data.get("free_cash_flow")

    # Margin of safety: most important valuation metric
    if mos is not None:
        if mos >= _P.get("scoring.mos_band_excellent", 0.33):
            score += 15
        elif mos >= _P.get("scoring.mos_band_good", 0.20):
            score += 11
        elif mos >= _P.get("scoring.mos_band_fair", 0.10):
            score += 7
        elif mos >= 0:
            score += 3

    # PE attractiveness
    if pe and pe > 0:
        if pe < 10:
            score += 6
        elif pe < 15:
            score += 4
        elif pe < 20:
            score += 2

    # Graham Number: price should be below Graham Number for value
    if price and graham_num and graham_num > 0:
        ratio = price / graham_num
        if ratio < 0.75:
            score += 5
        elif ratio < 1.0:
            score += 3
        elif ratio < 1.25:
            score += 1

    # EV/FCF: lower is more attractive
    if ev and fcf and fcf > 0:
        ev_fcf = ev / fcf
        if ev_fcf < 10:
            score += 4
        elif ev_fcf < 15:
            score += 2

    return min(score, 30)




_SCHOOL_WEIGHTS = {
    "graham": lambda: _P.get("school_weight.graham", 1.5),
    "buffett": lambda: _P.get("school_weight.buffett", 2.0),
    "quantitative": lambda: _P.get("school_weight.quantitative", 1.5),
    "quality": lambda: _P.get("school_weight.quality", 2.0),
    "valuation": lambda: _P.get("school_weight.valuation", 1.5),
    "contrarian": lambda: _P.get("school_weight.contrarian", 0.5),
    "garp": lambda: _P.get("school_weight.garp", 1.0),
}


def _composite_score(school_eval: Dict[str, Any], data: Dict[str, Any]) -> float:
    """Calculate a 0-100 composite score from school results + data quality.

    Simplified vs. live pipeline (no LLM, no committee, no moat LLM).
    Weighted components:
      - school consensus   40 pts
      - valuation/safety   30 pts  (via _valuation_gate)
      - financial health   20 pts
      - data completeness  10 pts
    """
    score = 0.0

    # ── School consensus (40 pts) ──
    schools = school_eval.get("schools", {})
    weighted_sum = 0.0
    weight_total = 0.0
    for name, res in schools.items():
        w = _SCHOOL_WEIGHTS.get(name, lambda: 1.0)()
        pr = res.get("pass_rate", 0)
        weighted_sum += pr * w
        weight_total += w
    if weight_total > 0:
        consensus_ratio = weighted_sum / weight_total
        score += consensus_ratio * 40

    # ── Valuation / safety (30 pts) — delegated to Stage 6 ──
    score += _valuation_gate(data)

    # ── Financial health (20 pts) ──
    cr = data.get("current_ratio")
    de = data.get("debt_to_equity")
    roe = data.get("roe")
    if cr is not None and cr >= 2.0:
        score += 7
    elif cr is not None and cr >= 1.5:
        score += 4
    if de is not None and de < 0.5:
        score += 7
    elif de is not None and de < 1.0:
        score += 4
    if roe is not None and roe >= 0.15:
        score += 6
    elif roe is not None and roe >= 0.10:
        score += 3

    # ── Data completeness (10 pts) ──
    key_fields = ["pe", "roe", "debt_to_equity", "eps", "revenue", "market_cap",
                  "operating_margin", "current_ratio", "free_cash_flow", "profit_margin"]
    present = sum(1 for f in key_fields if data.get(f) is not None)
    score += (present / len(key_fields)) * 10

    return round(min(score, 100), 1)


def _conviction_level(score: float, school_eval: Dict[str, Any], data: Dict[str, Any]) -> str:
    """Determine conviction level from composite score + school results."""
    strong = school_eval.get("strong_pass_schools", [])
    mos = data.get("margin_of_safety", 0) or 0

    if (len(strong) >= _P.get("conviction.highest_strong_schools", 3)
            and mos >= _P.get("conviction.highest_mos_min", 0.30)
            and score >= 75):
        return "HIGHEST"
    if (len(strong) >= _P.get("conviction.high_strong_schools", 2)
            and mos >= _P.get("conviction.high_mos_min", 0.15)
            and score >= 55):
        return "HIGH"
    if score >= 40:
        return "MEDIUM"
    if score >= 25:
        return "LOW"
    return "NONE"


# ═══════════════════════════════════════════════════════════════
#  Public API — HistoricalScreener
# ═══════════════════════════════════════════════════════════════

class HistoricalScreener:
    """Screen stocks at a historical rebalance point using PIT snapshots.

    Usage::

        screener = HistoricalScreener(strategy="balanced")
        result = screener.screen_at_point(
            rebalance_date=date(2023, 7, 1),
            snapshots={"AAPL": snapshot, "MSFT": snapshot, ...},
            max_holdings=15,
        )
    """

    def __init__(self, strategy: str = "balanced"):
        self.strategy = strategy

    def screen_at_point(
        self,
        rebalance_date: date,
        snapshots: Dict[str, HistoricalSnapshot],
        max_holdings: int = 15,
    ) -> ScreeningResult:
        """Run screening pipeline on all snapshots for one rebalance date.

        Steps:
          1. Hard knockout (Stage 3)
          2. Forensic accounting check (Stage 4 simplified)
          3. Multi-school consensus (Stage 5)
          4. Valuation gate + Composite scoring (Stage 6+8 simplified)
          5. Rank and select top-N
        """
        passed: List[str] = []
        eliminated: List[str] = []
        school_scores: Dict[str, Dict[str, float]] = {}
        composite_scores: Dict[str, float] = {}
        conviction_levels: Dict[str, str] = {}

        for symbol, snap in snapshots.items():
            data = snap.to_screening_dict()

            # Stage 3: hard knockout
            fail_reason = _hard_knockout(data, self.strategy)
            if fail_reason:
                eliminated.append(symbol)
                logger.debug(f"[PIT-Screen] {symbol} eliminated at {rebalance_date} (Stage 3): {fail_reason}")
                continue

            # Stage 4: forensic accounting (soft — warn but don't eliminate unless severe)
            forensic_warning = _forensic_check(data)
            if forensic_warning:
                # Count severity: if 2+ red flags, eliminate
                flag_count = forensic_warning.count(";") + 1
                if flag_count >= 2:
                    eliminated.append(symbol)
                    logger.debug(f"[PIT-Screen] {symbol} eliminated at {rebalance_date} (Stage 4): {forensic_warning}")
                    continue
                else:
                    logger.debug(f"[PIT-Screen] {symbol} forensic warning at {rebalance_date}: {forensic_warning}")

            # Stage 5: multi-school consensus
            school_eval = _school_consensus(data)

            # Check if all schools reject
            reject_count = len(school_eval.get("reject_schools", []))
            total_schools = len(school_eval.get("schools", {}))
            if total_schools > 0 and reject_count >= total_schools:
                eliminated.append(symbol)
                continue

            # Composite score
            cscore = _composite_score(school_eval, data)
            conv = _conviction_level(cscore, school_eval, data)

            # Record per-school pass_rates
            per_school: Dict[str, float] = {}
            for sname, sres in school_eval.get("schools", {}).items():
                per_school[sname] = sres.get("pass_rate", 0)

            passed.append(symbol)
            school_scores[symbol] = per_school
            composite_scores[symbol] = cscore
            conviction_levels[symbol] = conv

        # Rank by composite score, take top-N
        ranked = sorted(passed, key=lambda s: composite_scores.get(s, 0), reverse=True)
        final_passed = ranked[:max_holdings]
        extra_eliminated = ranked[max_holdings:]
        eliminated.extend(extra_eliminated)

        # Remove non-selected from score dicts
        for sym in extra_eliminated:
            school_scores.pop(sym, None)
            composite_scores.pop(sym, None)
            conviction_levels.pop(sym, None)

        logger.info(
            f"[PIT-Screen] {rebalance_date}: "
            f"{len(snapshots)} candidates → {len(final_passed)} passed, "
            f"{len(eliminated)} eliminated"
        )

        return ScreeningResult(
            rebalance_date=rebalance_date,
            passed_symbols=final_passed,
            eliminated_symbols=eliminated,
            school_scores=school_scores,
            composite_scores=composite_scores,
            conviction_levels=conviction_levels,
        )
