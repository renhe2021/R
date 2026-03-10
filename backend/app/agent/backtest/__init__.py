"""Backtesting engine for strategy validation and threshold optimisation."""

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
from app.agent.backtest.threshold_optimizer import (
    ThresholdOptimizer,
    OptimizationConfig,
    OptimizationResult,
    DEFAULT_OPTIMIZE_KEYS,
)
from app.agent.backtest.walk_forward import (
    WalkForwardValidator,
    WalkForwardConfig,
    WalkForwardResult,
)

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
    # Threshold Optimizer
    "ThresholdOptimizer",
    "OptimizationConfig",
    "OptimizationResult",
    "DEFAULT_OPTIMIZE_KEYS",
    # Walk-Forward Validator
    "WalkForwardValidator",
    "WalkForwardConfig",
    "WalkForwardResult",
]
