"""Performance metrics calculator for PIT backtest results.

Computes risk-adjusted returns and generates monthly return matrices
from a sequence of PeriodSummary objects and NAV history.

Metrics produced
────────────────
- Total / annualised return (portfolio + benchmark)
- Alpha
- Sharpe Ratio (annualised, risk-free = 0)
- Sortino Ratio (downside deviation)
- Calmar Ratio (annualised return / max drawdown)
- Maximum drawdown (depth + duration)
- Win rate (% of periods beating benchmark)
- Profit factor (gross wins / gross losses)
- Total transaction costs
- Average turnover
- Monthly return heat-map data
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date
from typing import Any, Dict, List, Optional

from app.agent.backtest.models import (
    PerformanceMetrics,
    PeriodSummary,
    MonthlyReturn,
)

TRADING_DAYS_PER_YEAR = 252


def calculate_metrics(
    periods: List[PeriodSummary],
    nav_series: List[Dict[str, Any]],
    initial_capital: float,
    risk_free_rate: float = 0.0,
    holding_months: int = 6,
) -> PerformanceMetrics:
    """Calculate all performance metrics from backtest results.

    Args:
        periods: List of per-rebalance-period summaries.
        nav_series: [{date, nav, benchmark_nav}, ...] weekly/daily points.
        initial_capital: Starting portfolio value.
        risk_free_rate: Annualised risk-free rate (default 0).
        holding_months: Length of each holding period in months (for annualisation).

    Returns:
        PerformanceMetrics dataclass.
    """
    if not periods:
        return PerformanceMetrics()

    # ── Total return ──
    final_nav = periods[-1].end_nav
    total_return = (final_nav - initial_capital) / initial_capital if initial_capital > 0 else 0

    # Benchmark total return (chain period returns)
    bench_cum = 1.0
    for p in periods:
        bench_cum *= (1 + p.benchmark_return)
    benchmark_total_return = bench_cum - 1

    # ── Annualised returns ──
    first_start = periods[0].start_date
    last_end = periods[-1].end_date
    years = max((last_end - first_start).days / 365.25, 0.1)

    annualized_return = _annualize(total_return, years)
    annualized_benchmark = _annualize(benchmark_total_return, years)
    alpha = annualized_return - annualized_benchmark

    # ── Period returns array ──
    port_returns = [p.portfolio_return for p in periods]
    bench_returns = [p.benchmark_return for p in periods]

    # ── Sharpe Ratio ──
    # periods_per_year derived from actual holding period
    ppy = 12 / holding_months  # e.g. 6 months → 2 periods/year
    sharpe = _sharpe(port_returns, risk_free_rate, periods_per_year=ppy)

    # ── Sortino Ratio ──
    sortino = _sortino(port_returns, risk_free_rate, periods_per_year=ppy)

    # ── Max drawdown from NAV series ──
    max_dd, max_dd_duration = _max_drawdown(nav_series)

    # ── Calmar Ratio ──
    calmar = None
    if max_dd and max_dd > 0:
        calmar = round(annualized_return / max_dd, 3)

    # ── Win rate ──
    wins = sum(1 for p in periods if p.portfolio_return > p.benchmark_return)
    win_rate = wins / len(periods) if periods else 0

    # ── Profit factor ──
    gross_wins = sum(p.portfolio_return for p in periods if p.portfolio_return > 0)
    gross_losses = abs(sum(p.portfolio_return for p in periods if p.portfolio_return < 0))
    profit_factor = round(gross_wins / gross_losses, 3) if gross_losses > 0 else None

    # ── Transaction costs ──
    total_commission = sum(p.commission_paid for p in periods)
    total_slippage = sum(p.slippage_paid for p in periods)

    # ── Total trades ──
    total_trades = sum(len(p.trades) for p in periods)

    # ── Average turnover ──
    turnovers = []
    for p in periods:
        buy_value = sum(abs(t.proceeds) for t in p.trades if t.action == "BUY")
        if p.start_nav > 0:
            turnovers.append(buy_value / p.start_nav)
    avg_turnover = sum(turnovers) / len(turnovers) if turnovers else 0

    # ── Best / worst period ──
    best_period = max(port_returns) if port_returns else 0
    worst_period = min(port_returns) if port_returns else 0

    return PerformanceMetrics(
        total_return=round(total_return, 6),
        benchmark_total_return=round(benchmark_total_return, 6),
        annualized_return=round(annualized_return, 6),
        annualized_benchmark=round(annualized_benchmark, 6),
        alpha=round(alpha, 6),
        sharpe_ratio=sharpe,
        sortino_ratio=sortino,
        calmar_ratio=calmar,
        max_drawdown=round(max_dd, 6) if max_dd else 0,
        max_drawdown_duration_days=max_dd_duration,
        win_rate=round(win_rate, 4),
        profit_factor=profit_factor,
        total_trades=total_trades,
        total_commission_paid=round(total_commission, 2),
        total_slippage_cost=round(total_slippage, 2),
        avg_turnover=round(avg_turnover, 4),
        best_period_return=round(best_period, 6),
        worst_period_return=round(worst_period, 6),
    )


def calculate_monthly_returns(
    nav_series: List[Dict[str, Any]],
) -> List[MonthlyReturn]:
    """Aggregate NAV series into monthly portfolio & benchmark returns.

    Returns a list of MonthlyReturn objects suitable for heat-map rendering.
    """
    if not nav_series or len(nav_series) < 2:
        return []

    # Group by (year, month)
    monthly: Dict[tuple, List[Dict]] = defaultdict(list)
    for point in nav_series:
        d = _parse_date(point["date"])
        if d:
            monthly[(d.year, d.month)].append(point)

    results: List[MonthlyReturn] = []
    sorted_months = sorted(monthly.keys())

    for i, (year, month) in enumerate(sorted_months):
        points = sorted(monthly[(year, month)], key=lambda x: x["date"])
        if len(points) < 2:
            # Use first point of this month and first of next
            if i + 1 < len(sorted_months):
                next_key = sorted_months[i + 1]
                next_points = sorted(monthly[next_key], key=lambda x: x["date"])
                start_nav = points[0]["nav"]
                end_nav = next_points[0]["nav"]
                start_bench = points[0].get("benchmark_nav", start_nav)
                end_bench = next_points[0].get("benchmark_nav", end_nav)
            else:
                continue
        else:
            start_nav = points[0]["nav"]
            end_nav = points[-1]["nav"]
            start_bench = points[0].get("benchmark_nav", start_nav)
            end_bench = points[-1].get("benchmark_nav", end_nav)

        port_ret = (end_nav - start_nav) / start_nav if start_nav > 0 else 0
        bench_ret = (end_bench - start_bench) / start_bench if start_bench > 0 else 0

        results.append(MonthlyReturn(
            year=year,
            month=month,
            portfolio_return=round(port_ret, 6),
            benchmark_return=round(bench_ret, 6),
        ))

    return results


# ═══════════════════════════════════════════════════════════════
#  Internal helpers
# ═══════════════════════════════════════════════════════════════

def _annualize(total_return: float, years: float) -> float:
    """Convert total return to annualised CAGR."""
    if years <= 0:
        return 0
    if total_return <= -1:
        return -1.0
    return (1 + total_return) ** (1 / years) - 1


def _sharpe(returns: List[float], rf: float = 0, periods_per_year: float = 2) -> Optional[float]:
    """Annualised Sharpe ratio from period returns."""
    if len(returns) < 2:
        return None
    mean_r = sum(returns) / len(returns)
    excess = mean_r - rf / periods_per_year
    std = _std(returns)
    if std <= 0:
        return None
    return round(excess / std * math.sqrt(periods_per_year), 3)


def _sortino(returns: List[float], rf: float = 0, periods_per_year: float = 2) -> Optional[float]:
    """Sortino ratio — uses downside deviation only."""
    if len(returns) < 2:
        return None
    mean_r = sum(returns) / len(returns)
    downside = [min(r - rf, 0) ** 2 for r in returns]
    dd = math.sqrt(sum(downside) / len(downside))
    if dd <= 0:
        return None
    return round((mean_r - rf) / dd * math.sqrt(periods_per_year), 3)


def _std(values: List[float]) -> float:
    """Sample standard deviation."""
    if len(values) < 2:
        return 0
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(var)


def _max_drawdown(nav_series: List[Dict[str, Any]]) -> tuple:
    """Calculate maximum drawdown depth and duration in days."""
    if not nav_series:
        return 0.0, 0

    navs = [(p.get("date", ""), p.get("nav", 0)) for p in nav_series if p.get("nav")]
    if not navs:
        return 0.0, 0

    peak = navs[0][1]
    max_dd = 0.0
    dd_start_date = navs[0][0]
    max_dd_duration = 0
    current_dd_start = navs[0][0]

    for date_str, nav in navs:
        if nav >= peak:
            peak = nav
            current_dd_start = date_str
        else:
            dd = (peak - nav) / peak
            if dd > max_dd:
                max_dd = dd
                dd_start_date = current_dd_start
                d1 = _parse_date(dd_start_date)
                d2 = _parse_date(date_str)
                if d1 and d2:
                    max_dd_duration = (d2 - d1).days

    return max_dd, max_dd_duration


def _parse_date(s) -> Optional[date]:
    """Parse date from string or return as-is if already date."""
    if isinstance(s, date):
        return s
    if isinstance(s, str):
        try:
            return date.fromisoformat(s[:10])
        except (ValueError, IndexError):
            return None
    return None
