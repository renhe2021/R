"""Backtesting engine for strategy validation."""

from app.agent.backtest.strategy_backtest import StrategyBacktester, BacktestResult
from app.agent.backtest.models import (
    PITBacktestConfig,
    PITBacktestResult,
    PerformanceMetrics,
    HistoricalSnapshot,
    ScreeningResult,
    Trade,
    HoldingRecord,
    PeriodSummary,
    MonthlyReturn,
)
from app.agent.backtest.pit_backtester import PointInTimeBacktester
from app.agent.backtest.historical_data import HistoricalDataFetcher
from app.agent.backtest.historical_screener import HistoricalScreener
from app.agent.backtest.trade_simulator import TradeSimulator
from app.agent.backtest.metrics import calculate_metrics, calculate_monthly_returns

__all__ = [
    "StrategyBacktester",
    "BacktestResult",
    "PITBacktestConfig",
    "PITBacktestResult",
    "PerformanceMetrics",
    "HistoricalSnapshot",
    "ScreeningResult",
    "Trade",
    "HoldingRecord",
    "PeriodSummary",
    "MonthlyReturn",
    "PointInTimeBacktester",
    "HistoricalDataFetcher",
    "HistoricalScreener",
    "TradeSimulator",
    "calculate_metrics",
    "calculate_monthly_returns",
]
