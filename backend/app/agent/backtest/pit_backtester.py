"""Point-in-Time Backtester — main orchestration engine.

Coordinates:
  HistoricalDataFetcher  → data snapshots
  HistoricalScreener     → stock selection at each rebalance date
  TradeSimulator         → execution with costs & stop-loss
  MetricsCalculator      → risk-adjusted performance stats

Produces SSE-compatible progress events and a serialisable PITBacktestResult.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional

from app.agent.backtest.models import (
    PITBacktestConfig,
    PITBacktestResult,
    PerformanceMetrics,
    PeriodSummary,
)
from app.agent.backtest.historical_data import HistoricalDataFetcher
from app.agent.backtest.historical_screener import HistoricalScreener
from app.agent.backtest.trade_simulator import TradeSimulator
from app.agent.backtest.metrics import calculate_metrics, calculate_monthly_returns
from app.agent.investment_params import params as _P

logger = logging.getLogger(__name__)


class PointInTimeBacktester:
    """Run a complete PIT backtest and yield SSE progress events.

    Usage::

        bt = PointInTimeBacktester(config)
        async for event in bt.run():
            # event is a dict: {"type": "...", "data": {...}}
            yield f"data: {json.dumps(event)}\\n\\n"
    """

    def __init__(self, config: PITBacktestConfig, raw_source=None):
        self.config = config
        self.run_id = uuid.uuid4().hex[:16]
        self._result: Optional[PITBacktestResult] = None
        self._raw_source = raw_source  # RawDataSource: Bloomberg/yfinance/auto

    async def run(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Execute the full backtest pipeline, yielding SSE events."""
        started = datetime.now(timezone.utc)

        yield _evt("backtest_start", {
            "run_id": self.run_id,
            "config": self.config.to_dict(),
            "message": f"开始 Point-in-Time 回测 — {len(self.config.symbols)} 只股票, "
                       f"回溯 {self.config.lookback_years} 年, "
                       f"持仓周期 {self.config.holding_months} 个月",
        })

        try:
            # ── Phase 1: Fetch historical data ──
            yield _evt("backtest_phase", {"phase": "data_fetch", "message": "正在获取历史数据..."})

            fetcher = HistoricalDataFetcher(
                symbols=self.config.symbols,
                lookback_years=self.config.lookback_years,
                holding_months=self.config.holding_months,
                benchmark=self.config.benchmark,
                raw_source=self._raw_source,
            )
            rebalance_dates = fetcher.compute_rebalance_dates()

            async def _progress_cb(**kwargs):
                pass  # we yield below instead

            snapshots = await fetcher.fetch_all()

            yield _evt("backtest_progress", {
                "phase": "data_fetch",
                "progress": 100,
                "message": f"数据获取完成 — {len(snapshots)} 只股票有历史数据, "
                           f"{len(rebalance_dates)} 个再平衡时点",
            })

            if not snapshots:
                yield _evt("backtest_error", {"message": "未能获取任何历史数据"})
                return

            # ── Phase 2: Historical screening at each rebalance point ──
            yield _evt("backtest_phase", {"phase": "screening", "message": "正在各时点执行筛选规则..."})

            screener = HistoricalScreener(strategy=self.config.strategy)
            screening_results = {}

            for idx, rd in enumerate(rebalance_dates):
                # Gather snapshots for this date across all symbols
                point_snaps = {}
                for sym, sym_snaps in snapshots.items():
                    if sym == self.config.benchmark.upper():
                        continue
                    if rd in sym_snaps:
                        point_snaps[sym] = sym_snaps[rd]

                if not point_snaps:
                    continue

                sr = screener.screen_at_point(
                    rebalance_date=rd,
                    snapshots=point_snaps,
                    max_holdings=self.config.max_holdings,
                )
                screening_results[rd] = sr

                yield _evt("backtest_progress", {
                    "phase": "screening",
                    "progress": round((idx + 1) / len(rebalance_dates) * 100),
                    "message": f"时点 {rd}: {len(sr.passed_symbols)} 只通过筛选",
                    "date": str(rd),
                    "passed_count": len(sr.passed_symbols),
                    "symbols": sr.passed_symbols[:10],
                })

            if not screening_results:
                yield _evt("backtest_error", {"message": "所有时点均无股票通过筛选"})
                return

            # ── Phase 3: Trade simulation ──
            yield _evt("backtest_phase", {"phase": "trading", "message": "正在模拟交易执行..."})

            simulator = TradeSimulator(
                config=self.config,
                price_getter=fetcher.get_price_on_date,
                price_series_getter=fetcher.get_price_series,
            )

            sorted_dates = sorted(screening_results.keys())
            simulator.initialise(sorted_dates[0])

            periods: List[PeriodSummary] = []

            for idx, rd in enumerate(sorted_dates):
                # Period end = next rebalance date or today
                if idx + 1 < len(sorted_dates):
                    period_end = sorted_dates[idx + 1]
                else:
                    period_end = min(
                        rd + timedelta(days=self.config.holding_months * 30),
                        date.today(),
                    )

                bench_start = fetcher.get_price_on_date(self.config.benchmark, rd)
                bench_end = fetcher.get_price_on_date(self.config.benchmark, period_end)

                summary = simulator.execute_rebalance(
                    period_idx=idx,
                    screening=screening_results[rd],
                    period_start=rd,
                    period_end=period_end,
                    benchmark_start_price=bench_start,
                    benchmark_end_price=bench_end,
                )
                periods.append(summary)

                yield _evt("backtest_progress", {
                    "phase": "trading",
                    "progress": round((idx + 1) / len(sorted_dates) * 100),
                    "message": f"窗口 {rd} → {period_end}: "
                               f"收益 {summary.portfolio_return:+.2%} vs 基准 {summary.benchmark_return:+.2%}",
                    "period": summary.to_dict(),
                })

            # ── Phase 4: Calculate metrics ──
            yield _evt("backtest_phase", {"phase": "metrics", "message": "正在计算风险调整指标..."})

            nav_series = simulator.get_nav_series()
            metrics = calculate_metrics(
                periods=periods,
                nav_series=nav_series,
                initial_capital=self.config.initial_capital,
                holding_months=self.config.holding_months,
            )
            monthly = calculate_monthly_returns(nav_series)

            # ── Verdict ──
            verdict = self._determine_verdict(metrics, periods)

            finished = datetime.now(timezone.utc)
            duration = (finished - started).total_seconds()

            self._result = PITBacktestResult(
                run_id=self.run_id,
                config=self.config,
                metrics=metrics,
                periods=periods,
                nav_series=nav_series,
                monthly_returns=monthly,
                verdict=verdict,
                started_at=started.isoformat(),
                finished_at=finished.isoformat(),
                duration_seconds=round(duration, 1),
            )

            yield _evt("backtest_complete", {
                "run_id": self.run_id,
                "verdict": verdict,
                "metrics": metrics.to_dict(),
                "monthly_returns": [{"year": m.year, "month": m.month,
                                     "portfolio": m.portfolio_return,
                                     "benchmark": m.benchmark_return} for m in monthly],
                "nav_series_length": len(nav_series),
                "periods_count": len(periods),
                "duration_seconds": round(duration, 1),
                "message": f"回测完成 — {verdict} | "
                           f"年化收益 {metrics.annualized_return:+.2%} | "
                           f"Alpha {metrics.alpha:+.2%} | "
                           f"Sharpe {metrics.sharpe_ratio or 'N/A'} | "
                           f"最大回撤 {metrics.max_drawdown:.2%} | "
                           f"胜率 {metrics.win_rate:.0%}",
            })

        except Exception as e:
            logger.exception(f"[PIT-Backtest] Error: {e}")
            yield _evt("backtest_error", {
                "run_id": self.run_id,
                "message": f"回测执行失败: {str(e)[:200]}",
            })

    def get_result(self) -> Optional[PITBacktestResult]:
        return self._result

    def _determine_verdict(
        self, metrics: PerformanceMetrics, periods: List[PeriodSummary]
    ) -> str:
        """Classify the backtest outcome."""
        validated_wr = _P.get("backtest.validated_win_rate", 0.60)
        validated_alpha = _P.get("backtest.validated_avg_alpha", 0.02)
        mixed_wr = _P.get("backtest.mixed_win_rate", 0.40)
        mixed_alpha = _P.get("backtest.mixed_avg_alpha", -0.02)

        avg_alpha = metrics.alpha  # already annualised
        wr = metrics.win_rate

        if wr >= validated_wr and avg_alpha >= validated_alpha:
            return "VALIDATED"
        if wr >= mixed_wr and avg_alpha >= mixed_alpha:
            return "MIXED"
        return "FAILED"


def _evt(event_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """Create a standardised SSE event dict."""
    return {"type": event_type, "data": data}
