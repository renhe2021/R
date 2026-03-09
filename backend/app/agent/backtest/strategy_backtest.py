"""Strategy Backtest Engine — Rolling 6-month holding period backtester.

Simulates:
  "If we had bought stocks selected by our screening rules N months ago
   and held for 6 months, what would the returns be?"

Uses yfinance for historical price data.
Compares against S&P 500 (SPY) benchmark.

Key Metrics:
  - Per-period return (absolute + vs benchmark)
  - Win rate (% of periods beating benchmark)
  - Annualized return
  - Sharpe Ratio
  - Maximum drawdown
  - Alpha (excess return over benchmark)
"""

import asyncio
import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class PeriodResult:
    """Result for one backtest period (e.g., one 6-month window)."""
    period_start: str          # YYYY-MM-DD
    period_end: str
    stocks: List[str]          # Stocks held in this period
    portfolio_return: float    # Equal-weight portfolio return
    benchmark_return: float    # SPY return in same period
    alpha: float               # portfolio_return - benchmark_return
    beat_benchmark: bool
    per_stock_returns: Dict[str, float]  # symbol -> return

    def to_dict(self) -> Dict[str, Any]:
        return {
            "period_start": self.period_start,
            "period_end": self.period_end,
            "stocks": self.stocks,
            "portfolio_return": round(self.portfolio_return, 4),
            "benchmark_return": round(self.benchmark_return, 4),
            "alpha": round(self.alpha, 4),
            "beat_benchmark": self.beat_benchmark,
            "per_stock_returns": {
                k: round(v, 4) for k, v in self.per_stock_returns.items()
            },
        }


@dataclass
class BacktestResult:
    """Complete backtest result across all rolling periods."""
    holding_months: int = 6
    lookback_years: float = 2.5
    total_periods: int = 0
    periods: List[PeriodResult] = field(default_factory=list)

    # Aggregate metrics
    avg_period_return: float = 0.0
    avg_benchmark_return: float = 0.0
    avg_alpha: float = 0.0
    win_rate: float = 0.0              # % of periods beating benchmark
    annualized_return: float = 0.0
    annualized_benchmark: float = 0.0
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    total_cumulative_return: float = 0.0
    benchmark_cumulative_return: float = 0.0

    # Verdict
    verdict: str = ""  # "VALIDATED" / "MIXED" / "FAILED"
    summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "holding_months": self.holding_months,
            "lookback_years": self.lookback_years,
            "total_periods": self.total_periods,
            "periods": [p.to_dict() for p in self.periods],
            "avg_period_return": round(self.avg_period_return, 4),
            "avg_benchmark_return": round(self.avg_benchmark_return, 4),
            "avg_alpha": round(self.avg_alpha, 4),
            "win_rate": round(self.win_rate, 4),
            "annualized_return": round(self.annualized_return, 4),
            "annualized_benchmark": round(self.annualized_benchmark, 4),
            "sharpe_ratio": round(self.sharpe_ratio, 2) if self.sharpe_ratio else None,
            "max_drawdown": round(self.max_drawdown, 4) if self.max_drawdown else None,
            "total_cumulative_return": round(self.total_cumulative_return, 4),
            "benchmark_cumulative_return": round(self.benchmark_cumulative_return, 4),
            "verdict": self.verdict,
            "summary": self.summary,
        }


class StrategyBacktester:
    """Rolling 6-month holding period backtester.

    Strategy: Equal-weight portfolio of selected stocks, rebalanced every
    `holding_months` months. Looks back `lookback_years` years.
    """

    def __init__(
        self,
        holding_months: int = 6,
        lookback_years: float = 2.5,
    ):
        self.holding_months = holding_months
        self.lookback_years = lookback_years

    async def run_backtest(self, symbols: List[str]) -> BacktestResult:
        """Run rolling backtest for the given stock basket.

        Downloads historical prices for all symbols + SPY benchmark,
        then simulates equal-weight holding across rolling windows.
        """
        if not symbols:
            return BacktestResult(verdict="FAILED", summary="No symbols provided")

        loop = asyncio.get_running_loop()

        try:
            price_data = await asyncio.wait_for(
                loop.run_in_executor(None, self._fetch_all_prices, symbols),
                timeout=60,
            )
        except asyncio.TimeoutError:
            logger.error("Backtest price fetch timed out (60s)")
            return BacktestResult(verdict="FAILED", summary="Data fetch timeout")
        except Exception as e:
            logger.error(f"Backtest price fetch error: {e}")
            return BacktestResult(verdict="FAILED", summary=f"Data error: {str(e)[:200]}")

        if not price_data or "SPY" not in price_data:
            return BacktestResult(verdict="FAILED", summary="Insufficient price data")

        return self._compute_rolling_backtest(symbols, price_data)

    def _fetch_all_prices(self, symbols: List[str]) -> Dict[str, Any]:
        """Fetch historical daily close prices for all symbols + SPY.

        Returns: {symbol: pandas.Series of daily close prices}
        """
        import yfinance as yf

        # Determine date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=int(self.lookback_years * 365 + 30))

        all_symbols = list(set(symbols + ["SPY"]))
        price_data: Dict[str, Any] = {}

        # Resolve symbols for yfinance
        try:
            from src.symbol_resolver import resolve_for_provider
            resolved_map = {}
            for s in all_symbols:
                try:
                    resolved_map[s] = resolve_for_provider(s, "yfinance")
                except Exception:
                    resolved_map[s] = s
        except ImportError:
            resolved_map = {s: s for s in all_symbols}

        for symbol in all_symbols:
            try:
                resolved = resolved_map.get(symbol, symbol)
                ticker = yf.Ticker(resolved)
                hist = ticker.history(
                    start=start_date.strftime("%Y-%m-%d"),
                    end=end_date.strftime("%Y-%m-%d"),
                )
                if hist is not None and not hist.empty and len(hist) >= 20:
                    price_data[symbol] = hist["Close"]
                else:
                    logger.warning(f"[Backtest] Insufficient data for {symbol}")
            except Exception as e:
                logger.warning(f"[Backtest] Failed to fetch {symbol}: {e}")

        return price_data

    def _compute_rolling_backtest(
        self,
        symbols: List[str],
        price_data: Dict[str, Any],
    ) -> BacktestResult:
        """Compute rolling backtest across all periods."""
        import pandas as pd

        spy_prices = price_data.get("SPY")
        if spy_prices is None or len(spy_prices) < 20:
            return BacktestResult(verdict="FAILED", summary="No SPY benchmark data")

        holding_days = self.holding_months * 21  # ~21 trading days/month
        lookback_days = int(self.lookback_years * 252)

        # Generate period start dates (rolling every holding_months months)
        all_dates = spy_prices.index
        if len(all_dates) < holding_days + 20:
            return BacktestResult(verdict="FAILED", summary="Not enough history for backtest")

        # Start from lookback_days ago, step by holding_days
        start_idx = max(0, len(all_dates) - lookback_days)
        period_starts = list(range(start_idx, len(all_dates) - holding_days, holding_days))

        if not period_starts:
            return BacktestResult(verdict="FAILED", summary="Not enough periods for backtest")

        periods: List[PeriodResult] = []

        for ps_idx in period_starts:
            pe_idx = min(ps_idx + holding_days, len(all_dates) - 1)
            period_start_date = all_dates[ps_idx]
            period_end_date = all_dates[pe_idx]

            # Benchmark return
            spy_start = float(spy_prices.iloc[ps_idx])
            spy_end = float(spy_prices.iloc[pe_idx])
            benchmark_return = (spy_end / spy_start - 1) if spy_start > 0 else 0.0

            # Per-stock returns
            per_stock_returns: Dict[str, float] = {}
            valid_returns: List[float] = []

            for sym in symbols:
                sym_prices = price_data.get(sym)
                if sym_prices is None:
                    continue

                # Find closest dates in this stock's price series
                try:
                    # Get prices at period boundaries
                    mask_start = sym_prices.index <= period_start_date
                    mask_end = sym_prices.index <= period_end_date

                    if not mask_start.any() or not mask_end.any():
                        continue

                    s_price = float(sym_prices[mask_start].iloc[-1])
                    e_price = float(sym_prices[mask_end].iloc[-1])

                    if s_price > 0:
                        ret = e_price / s_price - 1
                        per_stock_returns[sym] = ret
                        valid_returns.append(ret)
                except Exception:
                    continue

            if not valid_returns:
                continue

            # Equal-weight portfolio return
            portfolio_return = sum(valid_returns) / len(valid_returns)
            alpha = portfolio_return - benchmark_return

            periods.append(PeriodResult(
                period_start=str(period_start_date.date()),
                period_end=str(period_end_date.date()),
                stocks=[s for s in symbols if s in per_stock_returns],
                portfolio_return=portfolio_return,
                benchmark_return=benchmark_return,
                alpha=alpha,
                beat_benchmark=alpha > 0,
                per_stock_returns=per_stock_returns,
            ))

        if not periods:
            return BacktestResult(verdict="FAILED", summary="No valid periods computed")

        # Aggregate metrics
        result = BacktestResult(
            holding_months=self.holding_months,
            lookback_years=self.lookback_years,
            total_periods=len(periods),
            periods=periods,
        )

        returns = [p.portfolio_return for p in periods]
        bench_returns = [p.benchmark_return for p in periods]
        alphas = [p.alpha for p in periods]

        result.avg_period_return = sum(returns) / len(returns)
        result.avg_benchmark_return = sum(bench_returns) / len(bench_returns)
        result.avg_alpha = sum(alphas) / len(alphas)
        result.win_rate = sum(1 for p in periods if p.beat_benchmark) / len(periods)

        # Cumulative return (compounding)
        cum = 1.0
        cum_bench = 1.0
        cum_values = [1.0]
        for p in periods:
            cum *= (1 + p.portfolio_return)
            cum_bench *= (1 + p.benchmark_return)
            cum_values.append(cum)

        result.total_cumulative_return = cum - 1
        result.benchmark_cumulative_return = cum_bench - 1

        # Annualize
        total_years = len(periods) * self.holding_months / 12
        if total_years > 0 and cum > 0:
            result.annualized_return = cum ** (1 / total_years) - 1
        if total_years > 0 and cum_bench > 0:
            result.annualized_benchmark = cum_bench ** (1 / total_years) - 1

        # Sharpe ratio (using period returns)
        if len(returns) > 1:
            mean_r = sum(returns) / len(returns)
            var_r = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
            std_r = math.sqrt(var_r) if var_r > 0 else 0
            periods_per_year = 12 / self.holding_months
            if std_r > 0:
                result.sharpe_ratio = (mean_r / std_r) * math.sqrt(periods_per_year)

        # Max drawdown (from cumulative values)
        peak = cum_values[0]
        max_dd = 0.0
        for v in cum_values:
            if v > peak:
                peak = v
            dd = (v - peak) / peak if peak > 0 else 0
            if dd < max_dd:
                max_dd = dd
        result.max_drawdown = max_dd

        # Verdict
        if result.win_rate >= 0.6 and result.avg_alpha > 0.02:
            result.verdict = "VALIDATED"
            result.summary = (
                f"策略验证通过: {result.total_periods} 个 {self.holding_months} 个月持仓周期，"
                f"胜率 {result.win_rate:.0%}，平均 Alpha {result.avg_alpha:.1%}，"
                f"Sharpe {result.sharpe_ratio:.2f}" if result.sharpe_ratio else
                f"策略验证通过: 胜率 {result.win_rate:.0%}，平均 Alpha {result.avg_alpha:.1%}"
            )
        elif result.win_rate >= 0.4 and result.avg_alpha > -0.02:
            result.verdict = "MIXED"
            result.summary = (
                f"策略表现一般: 胜率 {result.win_rate:.0%}，"
                f"平均 Alpha {result.avg_alpha:.1%}，需进一步观察"
            )
        else:
            result.verdict = "FAILED"
            result.summary = (
                f"策略验证失败: 胜率 {result.win_rate:.0%}，"
                f"平均 Alpha {result.avg_alpha:.1%}，"
                f"未能持续跑赢基准"
            )

        return result
