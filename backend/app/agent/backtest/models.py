"""Data models for the Point-in-Time backtesting engine.

All dataclasses used by the PIT backtest pipeline live here:
  - PITBacktestConfig  — input configuration
  - HistoricalSnapshot — one stock's financial data at a point in time
  - ScreeningResult    — screening outcome at a single rebalance point
  - Trade              — a single buy/sell transaction
  - HoldingRecord      — per-stock per-period holding info
  - PeriodSummary      — one rebalance-window's performance summary
  - PerformanceMetrics — final aggregate risk-adjusted metrics
  - MonthlyReturn      — year × month return cell
  - PITBacktestResult  — top-level result envelope
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import date
from typing import Any, Dict, List, Optional


# ═══════════════════════════════════════════════════════════════
#  Input config
# ═══════════════════════════════════════════════════════════════

@dataclass
class PITBacktestConfig:
    """Configuration for a single PIT backtest run."""
    symbols: List[str]
    holding_months: int = 6
    lookback_years: float = 3.0
    commission_rate: float = 0.001      # 0.1 %
    slippage_rate: float = 0.0005       # 0.05 %
    stop_loss_pct: float = 0.15         # sell if price drops 15 % from buy
    max_holdings: int = 15
    initial_capital: float = 1_000_000.0
    weighting: str = "equal"            # "equal" | "conviction"
    benchmark: str = "SPY"
    strategy: str = "balanced"          # "conservative" | "balanced" | "aggressive"
    rebalance_day: int = 1              # day-of-month trigger (approx.)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "PITBacktestConfig":
        allowed = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in allowed})


# ═══════════════════════════════════════════════════════════════
#  Historical data snapshot
# ═══════════════════════════════════════════════════════════════

@dataclass
class HistoricalSnapshot:
    """One stock's financial data at a specific historical point.

    Fields mirror the ``data: dict`` expected by ``distilled_rules.evaluate_stock_against_school``.
    """
    symbol: str
    as_of_date: date                    # report date we're using
    price: float = 0.0
    pe: Optional[float] = None
    pb: Optional[float] = None
    ps: Optional[float] = None
    market_cap: Optional[float] = None
    roe: Optional[float] = None
    operating_margin: Optional[float] = None
    profit_margin: Optional[float] = None
    current_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    interest_coverage: Optional[float] = None
    dividend_yield: Optional[float] = None
    eps: Optional[float] = None
    eps_growth: Optional[float] = None
    revenue_growth: Optional[float] = None
    earnings_growth_10y: Optional[float] = None
    peg: Optional[float] = None
    earnings_yield: Optional[float] = None
    profitable_years: Optional[int] = None
    dividend_years: Optional[int] = None
    f_score: Optional[int] = None
    z_score: Optional[float] = None
    m_score: Optional[float] = None
    price_vs_52w_high: Optional[float] = None
    # raw balance-sheet fields for valuation models
    total_assets: Optional[float] = None
    total_liabilities: Optional[float] = None
    total_current_assets: Optional[float] = None
    total_current_liabilities: Optional[float] = None
    shares_outstanding: Optional[float] = None
    free_cash_flow: Optional[float] = None
    operating_cash_flow: Optional[float] = None
    capex: Optional[float] = None
    revenue: Optional[float] = None
    ebit: Optional[float] = None
    net_income: Optional[float] = None
    dividends_per_share: Optional[float] = None
    # additional derived fields for compatibility
    book_value: Optional[float] = None
    total_debt: Optional[float] = None
    total_cash: Optional[float] = None
    enterprise_value: Optional[float] = None
    working_capital: Optional[float] = None
    ncav_per_share: Optional[float] = None
    intrinsic_value: Optional[float] = None
    margin_of_safety: Optional[float] = None
    graham_number: Optional[float] = None
    # 52-week high absolute price (for distilled_rules "price_52w_high")
    price_52w_high: Optional[float] = None
    # Historical derived fields for rule engine
    avg_eps_3y: Optional[float] = None      # average annual EPS over 3 years
    avg_eps_10y: Optional[float] = None     # average annual EPS over available history
    max_eps_decline: Optional[float] = None # worst YoY EPS decline (negative decimal)


    def to_screening_dict(self) -> Dict[str, Any]:
        """Convert to the flat ``data`` dict consumed by distilled_rules.

        Handles field-name mapping differences between HistoricalSnapshot
        and what distilled_rules / tools.py expect.

        UNIT CONVENTIONS (must match distilled_rules expressions):
        ─────────────────────────────────────────────────────────
        - pe, pb, ps, current_ratio, debt_to_equity: raw ratios
        - roe, profit_margin, operating_margin: DECIMAL (0.15 = 15%)
        - eps_growth_rate, revenue_growth_rate: DECIMAL (0.10 = 10%)
        - earnings_yield: DECIMAL (0.08 = 8%)
        - dividend_yield: DECIMAL (0.04 = 4%)
        - margin_of_safety: DECIMAL (0.33 = 33%)
        - price, price_52w_high, ncav_per_share: USD per share
        - market_cap, revenue, net_income, free_cash_flow: USD absolute
        """
        d = asdict(self)
        d.pop("as_of_date", None)

        # field name mappings: snapshot name → distilled_rules name
        _RENAMES = {
            "interest_coverage": "interest_coverage_ratio",
            "eps_growth": "eps_growth_rate",
            "revenue_growth": "revenue_growth_rate",
            "dividend_years": "consecutive_dividend_years",
            "total_current_assets": "current_assets",
            "total_current_liabilities": "current_liabilities",
            "dividends_per_share": "dividend_per_share",
        }
        for old_key, new_key in _RENAMES.items():
            if old_key in d and d[old_key] is not None:
                d[new_key] = d[old_key]

        # eps_growth_rate / revenue_growth_rate:
        # These are stored as DECIMALS in the snapshot (0.10 = 10%).
        # distilled_rules expressions compare against decimals
        # (e.g. "eps_growth_rate > 0.05", "revenue_growth_rate > 0.08")
        # and PEG formula does "pe / (eps_growth_rate * 100)".
        # So we pass them through AS-IS in decimal form. No conversion.

        # earnings_yield: distilled_rules expects DECIMAL (e.g. 0.08 = 8%)
        # NOT the percentage number. So 1/pe, not 100/pe.
        if d.get("pe") and d["pe"] > 0:
            if d.get("earnings_yield") is None:
                d["earnings_yield"] = 1.0 / d["pe"]

        # price_52w_high: distilled_rules expects ABSOLUTE price (USD)
        # If price_52w_high is already set directly, use it.
        # Otherwise derive from price_vs_52w_high ratio: 52w_high = price / ratio
        price = d.get("price")
        ratio = d.pop("price_vs_52w_high", None)
        if not d.get("price_52w_high") and price and ratio and 0 < ratio <= 1.0:
            d["price_52w_high"] = price / ratio

        return {k: v for k, v in d.items() if v is not None}


# ═══════════════════════════════════════════════════════════════
#  Screening result at one time point
# ═══════════════════════════════════════════════════════════════

@dataclass
class ScreeningResult:
    """Outcome of running Stage 3-6 rules at a single rebalance date."""
    rebalance_date: date
    passed_symbols: List[str] = field(default_factory=list)
    eliminated_symbols: List[str] = field(default_factory=list)
    school_scores: Dict[str, Dict[str, float]] = field(default_factory=dict)
    # symbol → {school → pass_rate}
    composite_scores: Dict[str, float] = field(default_factory=dict)
    # symbol → composite score (0-100)
    conviction_levels: Dict[str, str] = field(default_factory=dict)
    # symbol → "HIGHEST" / "HIGH" / "MEDIUM" / "LOW" / "NONE"


# ═══════════════════════════════════════════════════════════════
#  Trade record
# ═══════════════════════════════════════════════════════════════

@dataclass
class Trade:
    """A single buy or sell transaction."""
    trade_date: date
    symbol: str
    action: str                          # "BUY" | "SELL" | "STOP_LOSS"
    shares: float
    price: float
    commission: float = 0.0
    slippage_cost: float = 0.0
    proceeds: float = 0.0               # negative for buys, positive for sells
    reason: str = ""


# ═══════════════════════════════════════════════════════════════
#  Holding record
# ═══════════════════════════════════════════════════════════════

@dataclass
class HoldingRecord:
    """One stock's position within a holding period."""
    symbol: str
    buy_date: date
    buy_price: float
    shares: float
    weight: float                        # target weight in portfolio
    sell_date: Optional[date] = None
    sell_price: Optional[float] = None
    sell_reason: str = ""                # "REBALANCE" | "STOP_LOSS" | "FINAL"
    return_pct: Optional[float] = None


# ═══════════════════════════════════════════════════════════════
#  Period summary
# ═══════════════════════════════════════════════════════════════

@dataclass
class PeriodSummary:
    """Performance for one rebalance window."""
    period_index: int
    start_date: date
    end_date: date
    start_nav: float
    end_nav: float
    portfolio_return: float              # (end_nav - start_nav) / start_nav
    benchmark_return: float
    alpha: float
    holdings_count: int
    trades: List[Trade] = field(default_factory=list)
    holdings: List[HoldingRecord] = field(default_factory=list)
    stop_losses_triggered: int = 0
    commission_paid: float = 0.0
    slippage_paid: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["start_date"] = str(self.start_date)
        d["end_date"] = str(self.end_date)
        for t in d.get("trades", []):
            t["trade_date"] = str(t["trade_date"])
        for h in d.get("holdings", []):
            h["buy_date"] = str(h["buy_date"])
            if h.get("sell_date"):
                h["sell_date"] = str(h["sell_date"])
        return d


# ═══════════════════════════════════════════════════════════════
#  Monthly return cell (for heat-map)
# ═══════════════════════════════════════════════════════════════

@dataclass
class MonthlyReturn:
    year: int
    month: int
    portfolio_return: float
    benchmark_return: float


# ═══════════════════════════════════════════════════════════════
#  Aggregate performance metrics
# ═══════════════════════════════════════════════════════════════

@dataclass
class PerformanceMetrics:
    """Final risk-adjusted performance figures."""
    total_return: float = 0.0
    benchmark_total_return: float = 0.0
    annualized_return: float = 0.0
    annualized_benchmark: float = 0.0
    alpha: float = 0.0
    sharpe_ratio: Optional[float] = None
    sortino_ratio: Optional[float] = None
    calmar_ratio: Optional[float] = None
    max_drawdown: float = 0.0
    max_drawdown_duration_days: int = 0
    win_rate: float = 0.0               # % of periods that beat benchmark
    profit_factor: Optional[float] = None
    total_trades: int = 0
    total_commission_paid: float = 0.0
    total_slippage_cost: float = 0.0
    avg_turnover: float = 0.0
    best_period_return: float = 0.0
    worst_period_return: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ═══════════════════════════════════════════════════════════════
#  Top-level backtest result
# ═══════════════════════════════════════════════════════════════

@dataclass
class PITBacktestResult:
    """Complete result of a Point-in-Time backtest run."""
    run_id: str
    config: PITBacktestConfig
    metrics: PerformanceMetrics
    periods: List[PeriodSummary] = field(default_factory=list)
    nav_series: List[Dict[str, Any]] = field(default_factory=list)
    # [{date: "2023-01-15", nav: 1012345.0, benchmark_nav: 1008000.0}, ...]
    monthly_returns: List[MonthlyReturn] = field(default_factory=list)
    verdict: str = "UNKNOWN"            # "VALIDATED" | "MIXED" | "FAILED"
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "config": self.config.to_dict(),
            "metrics": self.metrics.to_dict(),
            "periods": [p.to_dict() for p in self.periods],
            "nav_series": self.nav_series,
            "monthly_returns": [asdict(m) for m in self.monthly_returns],
            "verdict": self.verdict,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, default=str)
