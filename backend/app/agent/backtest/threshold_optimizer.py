"""Threshold Optimizer — Automated parameter tuning via backtesting.

Uses a Train/Test (or Walk-Forward) framework to find optimal screening
thresholds that maximise risk-adjusted returns (Sharpe Ratio ≥ 1.0).

Architecture
────────────
1. **Parameter Space**: Extract tuneable parameters from InvestmentParamsRegistry
   with their min/max ranges.
2. **Search Strategy**:
   - Grid Search (default, exhaustive with adaptive step counts)
   - Random Search (efficient, for very large spaces)
   - Bayesian Optimisation (optional, requires scikit-optimize)
3. **Train / Test Split — Fixed Test Period**:
   - Default: lookback_years=10, test_years=3
   - Train set: first 7 years of historical data → Grid Search for best thresholds
   - Test set: last 3 years → validate the whole stock-picking pipeline
   - No future data leakage — strict temporal split
   - test_years must be ≥ 3 years for robust validation
4. **Objective Function**: Maximise Sharpe Ratio on the TRAIN set,
   then validate on TEST set.
5. **Output**: Best parameter set + train/test metrics + all trial records.

Usage::

    optimizer = ThresholdOptimizer(config)
    async for event in optimizer.run():
        yield sse_event(event)
"""

from __future__ import annotations

import asyncio
import copy
import itertools
import json
import logging
import math
import random
import uuid
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from app.agent.investment_params import InvestmentParamsRegistry, params as _P
from app.agent.backtest.historical_data import HistoricalDataFetcher
from app.agent.backtest.historical_screener import HistoricalScreener
from app.agent.backtest.trade_simulator import TradeSimulator
from app.agent.backtest.metrics import calculate_metrics, calculate_monthly_returns
from app.agent.backtest.models import (
    PITBacktestConfig,
    PITBacktestResult,
    PerformanceMetrics,
    PeriodSummary,
)

logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════════
#  Data models
# ═══════════════════════════════════════════════════════════════

@dataclass
class OptimizableParam:
    """A parameter that can be optimised."""
    key: str
    current: Any
    default: Any
    min_val: float
    max_val: float
    step: Optional[float] = None  # grid step size (None = auto)
    description: str = ""
    school: str = ""


@dataclass
class TrialResult:
    """Result of evaluating one parameter combination."""
    trial_id: int
    params: Dict[str, Any]
    # Train metrics
    train_sharpe: Optional[float] = None
    train_return: float = 0.0
    train_alpha: float = 0.0
    train_win_rate: float = 0.0
    train_max_drawdown: float = 0.0
    train_periods: int = 0
    # Test metrics
    test_sharpe: Optional[float] = None
    test_return: float = 0.0
    test_alpha: float = 0.0
    test_win_rate: float = 0.0
    test_max_drawdown: float = 0.0
    test_periods: int = 0
    # Metadata
    duration_seconds: float = 0.0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def is_valid(self) -> bool:
        return self.error is None and self.train_sharpe is not None

    @property
    def objective_value(self) -> float:
        """Primary objective: train Sharpe, with penalty for overfitting."""
        if not self.is_valid:
            return -999.0
        ts = self.train_sharpe or 0.0
        # Penalise if test performance degrades significantly vs train
        overfit_penalty = 0.0
        if self.test_sharpe is not None and self.train_sharpe:
            gap = self.train_sharpe - self.test_sharpe
            if gap > 0.5:  # big train-test gap
                overfit_penalty = gap * 0.3
        return ts - overfit_penalty


@dataclass
class OptimizationConfig:
    """Configuration for a threshold optimisation run.

    Time Split Model (train_years + test_years = lookback_years)
    ─────────────────────────────────────────────────────────────
    • lookback_years: Total historical data length (default 10 years).
    • test_years:     Fixed test period at the END of the data (default 3 years).
    •                 The remaining (lookback - test) years form the training set.
    •                 This is mandatory >= 3 years to ensure robust validation.
    •
    • train_ratio is kept for backward compatibility; if test_years > 0 it takes
    •   priority.  train_ratio is only used when test_years is None/0.
    •
    • Example:  lookback=10, test=3  ⟹  Train: 7 years, Test: 3 years
    """
    symbols: List[str]
    # Parameters to optimise (dotted keys from InvestmentParamsRegistry)
    param_keys: List[str] = field(default_factory=list)
    # Search settings
    search_method: str = "grid"  # "grid" | "random" | "bayesian"
    max_trials: int = 200
    # Backtest settings
    holding_months: int = 6
    lookback_years: float = 10.0
    strategy: str = "balanced"
    benchmark: str = "SPY"
    initial_capital: float = 1_000_000.0
    max_holdings: int = 15
    commission_rate: float = 0.001
    slippage_rate: float = 0.0005
    stop_loss_pct: float = 0.15
    # Train / Test split — NEW: fixed test years take priority
    test_years: float = 3.0          # FIXED test period (years), must be >= 3
    train_ratio: float = 0.70        # Fallback: only used when test_years == 0
    # Target
    target_sharpe: float = 1.0
    # Random seed for reproducibility
    seed: int = 42

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def effective_train_years(self) -> float:
        """Actual training period length in years."""
        if self.test_years > 0:
            return self.lookback_years - self.test_years
        return self.lookback_years * self.train_ratio

    @property
    def effective_test_years(self) -> float:
        """Actual test period length in years."""
        if self.test_years > 0:
            return self.test_years
        return self.lookback_years * (1 - self.train_ratio)


@dataclass
class OptimizationResult:
    """Complete result of a threshold optimisation run."""
    run_id: str
    config: OptimizationConfig
    best_params: Dict[str, Any] = field(default_factory=dict)
    best_train_sharpe: Optional[float] = None
    best_test_sharpe: Optional[float] = None
    best_train_metrics: Optional[Dict[str, Any]] = None
    best_test_metrics: Optional[Dict[str, Any]] = None
    baseline_train_sharpe: Optional[float] = None
    baseline_test_sharpe: Optional[float] = None
    all_trials: List[TrialResult] = field(default_factory=list)
    total_trials: int = 0
    successful_trials: int = 0
    target_achieved: bool = False
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "config": self.config.to_dict(),
            "best_params": self.best_params,
            "best_train_sharpe": self.best_train_sharpe,
            "best_test_sharpe": self.best_test_sharpe,
            "best_train_metrics": self.best_train_metrics,
            "best_test_metrics": self.best_test_metrics,
            "baseline_train_sharpe": self.baseline_train_sharpe,
            "baseline_test_sharpe": self.baseline_test_sharpe,
            "total_trials": self.total_trials,
            "successful_trials": self.successful_trials,
            "target_achieved": self.target_achieved,
            "all_trials": [t.to_dict() for t in self.all_trials],
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
        }


# ═══════════════════════════════════════════════════════════════
#  Default optimisable parameters
# ═══════════════════════════════════════════════════════════════

# These are the most impactful screening parameters based on domain knowledge.
# Users can override by providing their own param_keys list.
DEFAULT_OPTIMIZE_KEYS = [
    # Hard knockout gates (Stage 3)
    "screener.pe_max",
    "screener.debt_to_equity_max",
    "screener.market_cap_min",
    # School weights (Stage 5 consensus)
    "school_weight.graham",
    "school_weight.buffett",
    "school_weight.quantitative",
    "school_weight.quality",
    # Risk thresholds (Stage 4 forensics)
    "forensics.high_red_flags_eliminate",
    # Scoring bands
    "scoring.mos_band_excellent",
    "scoring.mos_band_good",
    # Verdict cutoffs
    "verdict.strong_buy_score",
    "verdict.buy_score",
]


# ═══════════════════════════════════════════════════════════════
#  Core Optimizer
# ═══════════════════════════════════════════════════════════════

class ThresholdOptimizer:
    """Automated threshold tuning engine with Train/Test validation.

    Usage::

        cfg = OptimizationConfig(
            symbols=["AAPL", "MSFT", ...],
            param_keys=["screener.pe_max", "school_weight.buffett"],
            search_method="random",
            max_trials=30,
        )
        optimizer = ThresholdOptimizer(cfg)
        async for event in optimizer.run():
            yield f"data: {json.dumps(event)}\\n\\n"
    """

    def __init__(self, config: OptimizationConfig):
        self.config = config
        self.run_id = uuid.uuid4().hex[:16]
        self._result: Optional[OptimizationResult] = None
        self._rng = random.Random(config.seed)

    async def run(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Execute the full optimisation pipeline, yielding SSE events."""
        started = datetime.now(timezone.utc)

        yield _evt("optimize_start", {
            "run_id": self.run_id,
            "config": self.config.to_dict(),
            "message": f"开始阈值优化 — {len(self.config.symbols)} 只股票, "
                       f"回溯 {self.config.lookback_years:.0f}年 "
                       f"(训练 ~{self.config.effective_train_years:.0f}年 + "
                       f"测试 ~{self.config.effective_test_years:.0f}年), "
                       f"搜索方法: {self.config.search_method}, "
                       f"最大试验数: {self.config.max_trials}",
        })

        try:
            # ── Phase 1: Resolve optimisable parameters ──
            yield _evt("optimize_phase", {
                "phase": "param_resolve",
                "message": "正在解析可优化参数空间...",
            })

            param_keys = self.config.param_keys or DEFAULT_OPTIMIZE_KEYS
            opt_params = self._resolve_params(param_keys)

            if not opt_params:
                yield _evt("optimize_error", {
                    "message": "未找到有效的可优化参数",
                })
                return

            yield _evt("optimize_progress", {
                "phase": "param_resolve",
                "message": f"找到 {len(opt_params)} 个可优化参数: "
                           + ", ".join(p.key for p in opt_params),
                "params": [
                    {
                        "key": p.key,
                        "current": p.current,
                        "min": p.min_val,
                        "max": p.max_val,
                        "description": p.description,
                    }
                    for p in opt_params
                ],
            })

            # ── Phase 2: Fetch historical data (once, shared across all trials) ──
            yield _evt("optimize_phase", {
                "phase": "data_fetch",
                "message": "正在获取历史数据（所有试验共享）...",
            })

            fetcher = HistoricalDataFetcher(
                symbols=self.config.symbols,
                lookback_years=self.config.lookback_years,
                holding_months=self.config.holding_months,
                benchmark=self.config.benchmark,
            )
            rebalance_dates = fetcher.compute_rebalance_dates()
            snapshots = await fetcher.fetch_all()

            if not snapshots or not rebalance_dates:
                yield _evt("optimize_error", {
                    "message": "未能获取足够的历史数据",
                })
                return

            yield _evt("optimize_progress", {
                "phase": "data_fetch",
                "progress": 100,
                "message": f"数据获取完成 — {len(snapshots)} 只股票, "
                           f"{len(rebalance_dates)} 个再平衡时点",
            })

            # ── Phase 3: Train / Test temporal split ──
            yield _evt("optimize_phase", {
                "phase": "split",
                "message": "正在进行 Train/Test 时间分割...",
            })

            # Use fixed test_years if specified (priority), otherwise train_ratio
            if self.config.test_years > 0:
                # Fixed test period: last N years = test, everything before = train
                test_cutoff_date = rebalance_dates[-1] - timedelta(
                    days=int(self.config.test_years * 365)
                )
                # Find the split index: first date >= test_cutoff_date goes to test set
                split_idx = 0
                for i, rd in enumerate(rebalance_dates):
                    if rd >= test_cutoff_date:
                        split_idx = i
                        break
                # Safety: ensure at least 2 train periods and 1 test period
                if split_idx < 2:
                    split_idx = 2
                if split_idx >= len(rebalance_dates) - 1:
                    split_idx = len(rebalance_dates) - 2
            else:
                # Fallback: ratio-based split
                split_idx = int(len(rebalance_dates) * self.config.train_ratio)

            if split_idx < 2 or (len(rebalance_dates) - split_idx) < 1:
                yield _evt("optimize_error", {
                    "message": f"再平衡时点不足以分割 Train/Test "
                               f"(需要至少 3 个时点, 实际 {len(rebalance_dates)} 个)",
                })
                return

            train_dates = rebalance_dates[:split_idx]
            test_dates = rebalance_dates[split_idx:]

            train_years_actual = (train_dates[-1] - train_dates[0]).days / 365.25
            test_years_actual = (test_dates[-1] - test_dates[0]).days / 365.25

            yield _evt("optimize_progress", {
                "phase": "split",
                "message": f"Train: {train_dates[0]} → {train_dates[-1]} "
                           f"({len(train_dates)} 个窗口, ~{train_years_actual:.1f}年) | "
                           f"Test: {test_dates[0]} → {test_dates[-1]} "
                           f"({len(test_dates)} 个窗口, ~{test_years_actual:.1f}年)",
                "train_start": str(train_dates[0]),
                "train_end": str(train_dates[-1]),
                "train_periods": len(train_dates),
                "train_years": round(train_years_actual, 1),
                "test_start": str(test_dates[0]),
                "test_end": str(test_dates[-1]),
                "test_periods": len(test_dates),
                "test_years": round(test_years_actual, 1),
            })

            # ── Phase 4: Run baseline (current parameters) ──
            yield _evt("optimize_phase", {
                "phase": "baseline",
                "message": "正在运行基线回测（当前参数）...",
            })

            baseline_current = {p.key: p.current for p in opt_params}
            baseline_train = self._evaluate_params(
                baseline_current, snapshots, train_dates, fetcher,
            )
            baseline_test = self._evaluate_params(
                baseline_current, snapshots, test_dates, fetcher,
            )

            yield _evt("optimize_progress", {
                "phase": "baseline",
                "message": f"基线结果 — "
                           f"Train Sharpe: {_fmt_sharpe(baseline_train.train_sharpe)} | "
                           f"Test Sharpe: {_fmt_sharpe(baseline_test.train_sharpe)}",
                "baseline_train_sharpe": baseline_train.train_sharpe,
                "baseline_test_sharpe": baseline_test.train_sharpe,
            })

            # ── Phase 5: Search ──
            yield _evt("optimize_phase", {
                "phase": "search",
                "message": f"开始参数搜索 ({self.config.search_method})...",
            })

            candidates = self._generate_candidates(opt_params)
            total_trials = min(len(candidates), self.config.max_trials)

            all_trials: List[TrialResult] = []
            best_trial: Optional[TrialResult] = None

            for idx, candidate in enumerate(candidates[:total_trials]):
                trial_start = datetime.now(timezone.utc)

                # Apply candidate params
                self._apply_params(candidate)

                # Evaluate on TRAIN set
                train_result = self._evaluate_params(
                    candidate, snapshots, train_dates, fetcher,
                )

                # Evaluate on TEST set
                test_result = self._evaluate_params(
                    candidate, snapshots, test_dates, fetcher,
                )

                trial = TrialResult(
                    trial_id=idx + 1,
                    params=candidate,
                    train_sharpe=train_result.train_sharpe,
                    train_return=train_result.train_return,
                    train_alpha=train_result.train_alpha,
                    train_win_rate=train_result.train_win_rate,
                    train_max_drawdown=train_result.train_max_drawdown,
                    train_periods=train_result.train_periods,
                    test_sharpe=test_result.train_sharpe,  # using train_* fields from test evaluation
                    test_return=test_result.train_return,
                    test_alpha=test_result.train_alpha,
                    test_win_rate=test_result.train_win_rate,
                    test_max_drawdown=test_result.train_max_drawdown,
                    test_periods=test_result.train_periods,
                    duration_seconds=round(
                        (datetime.now(timezone.utc) - trial_start).total_seconds(), 1
                    ),
                    error=train_result.error or test_result.error,
                )
                all_trials.append(trial)

                # Track best
                if best_trial is None or trial.objective_value > best_trial.objective_value:
                    best_trial = trial

                yield _evt("optimize_trial", {
                    "trial": idx + 1,
                    "total": total_trials,
                    "progress": round((idx + 1) / total_trials * 100),
                    "params": candidate,
                    "train_sharpe": trial.train_sharpe,
                    "test_sharpe": trial.test_sharpe,
                    "train_return": round(trial.train_return, 4),
                    "test_return": round(trial.test_return, 4),
                    "is_best": trial is best_trial,
                    "best_so_far_sharpe": best_trial.train_sharpe if best_trial else None,
                    "message": f"Trial {idx + 1}/{total_trials}: "
                               f"Train Sharpe={_fmt_sharpe(trial.train_sharpe)} | "
                               f"Test Sharpe={_fmt_sharpe(trial.test_sharpe)}"
                               + (" ★ New Best!" if trial is best_trial and idx > 0 else ""),
                })

                # Restore defaults after each trial
                self._restore_params(opt_params)

            # ── Phase 6: Final validation — re-run best on full period ──
            yield _evt("optimize_phase", {
                "phase": "validation",
                "message": "正在验证最优参数组合...",
            })

            # Apply best params and run on FULL period
            if best_trial and best_trial.is_valid:
                self._apply_params(best_trial.params)
                full_result = self._evaluate_params(
                    best_trial.params, snapshots, rebalance_dates, fetcher,
                )
                self._restore_params(opt_params)

                best_train_metrics = {
                    "sharpe": best_trial.train_sharpe,
                    "return": round(best_trial.train_return, 4),
                    "alpha": round(best_trial.train_alpha, 4),
                    "win_rate": round(best_trial.train_win_rate, 4),
                    "max_drawdown": round(best_trial.train_max_drawdown, 4),
                    "periods": best_trial.train_periods,
                }
                best_test_metrics = {
                    "sharpe": best_trial.test_sharpe,
                    "return": round(best_trial.test_return, 4),
                    "alpha": round(best_trial.test_alpha, 4),
                    "win_rate": round(best_trial.test_win_rate, 4),
                    "max_drawdown": round(best_trial.test_max_drawdown, 4),
                    "periods": best_trial.test_periods,
                }
            else:
                best_train_metrics = None
                best_test_metrics = None

            target_achieved = (
                best_trial is not None
                and best_trial.test_sharpe is not None
                and best_trial.test_sharpe >= self.config.target_sharpe
            )

            finished = datetime.now(timezone.utc)
            duration = (finished - started).total_seconds()

            successful = sum(1 for t in all_trials if t.is_valid)

            self._result = OptimizationResult(
                run_id=self.run_id,
                config=self.config,
                best_params=best_trial.params if best_trial else {},
                best_train_sharpe=best_trial.train_sharpe if best_trial else None,
                best_test_sharpe=best_trial.test_sharpe if best_trial else None,
                best_train_metrics=best_train_metrics,
                best_test_metrics=best_test_metrics,
                baseline_train_sharpe=baseline_train.train_sharpe,
                baseline_test_sharpe=baseline_test.train_sharpe,
                all_trials=all_trials,
                total_trials=len(all_trials),
                successful_trials=successful,
                target_achieved=target_achieved,
                started_at=started.isoformat(),
                finished_at=finished.isoformat(),
                duration_seconds=round(duration, 1),
            )

            # Improvement summary
            baseline_s = baseline_test.train_sharpe or 0
            best_s = best_trial.test_sharpe if best_trial and best_trial.test_sharpe else 0
            improvement = best_s - baseline_s

            yield _evt("optimize_complete", {
                "run_id": self.run_id,
                "target_achieved": target_achieved,
                "best_params": best_trial.params if best_trial else {},
                "best_train_sharpe": best_trial.train_sharpe if best_trial else None,
                "best_test_sharpe": best_trial.test_sharpe if best_trial else None,
                "baseline_train_sharpe": baseline_train.train_sharpe,
                "baseline_test_sharpe": baseline_test.train_sharpe,
                "improvement": round(improvement, 3),
                "total_trials": len(all_trials),
                "successful_trials": successful,
                "duration_seconds": round(duration, 1),
                "best_train_metrics": best_train_metrics,
                "best_test_metrics": best_test_metrics,
                "message": self._build_summary(
                    target_achieved, best_trial, baseline_s, best_s,
                    len(all_trials), duration,
                ),
            })

        except Exception as e:
            logger.exception(f"[Optimizer] Error: {e}")
            yield _evt("optimize_error", {
                "run_id": self.run_id,
                "message": f"优化执行失败: {str(e)[:300]}",
            })

    def get_result(self) -> Optional[OptimizationResult]:
        return self._result

    # ──────────────────────────────────────────────────────────
    #  Parameter space resolution
    # ──────────────────────────────────────────────────────────

    def _resolve_params(self, keys: List[str]) -> List[OptimizableParam]:
        """Resolve parameter keys to OptimizableParam objects."""
        result = []
        for key in keys:
            p = _P.get_def(key)
            if p is None:
                logger.warning(f"[Optimizer] Unknown parameter: {key}")
                continue
            if p.min_val is None or p.max_val is None:
                logger.warning(f"[Optimizer] {key} has no min/max range, skipping")
                continue
            # Skip non-numeric parameters
            if not isinstance(p.value, (int, float)):
                logger.warning(f"[Optimizer] {key} is not numeric ({type(p.value).__name__}), skipping")
                continue
            result.append(OptimizableParam(
                key=key,
                current=p.value,
                default=p.default,
                min_val=float(p.min_val),
                max_val=float(p.max_val),
                description=p.description,
                school=p.school,
            ))
        return result

    # ──────────────────────────────────────────────────────────
    #  Candidate generation
    # ──────────────────────────────────────────────────────────

    def _generate_candidates(
        self, params: List[OptimizableParam]
    ) -> List[Dict[str, Any]]:
        """Generate parameter combinations based on search method."""
        if self.config.search_method == "grid":
            return self._grid_candidates(params)
        elif self.config.search_method == "bayesian":
            return self._random_candidates(params)  # fallback to random
        else:  # "random" (default)
            return self._random_candidates(params)

    def _grid_candidates(self, params: List[OptimizableParam]) -> List[Dict[str, Any]]:
        """Generate grid search candidates with adaptive step size.

        Step granularity adapts to the number of parameters to keep total
        combinations manageable:
          ≤ 3 params → 7 steps per param (high resolution)
          4-6 params → 5 steps per param
          7+ params  → 4 steps per param

        The current value of each parameter is always included in its grid
        to ensure the baseline is covered.
        """
        grid_values: Dict[str, List[Any]] = {}

        # Adaptive step count based on search space dimensionality
        if len(params) <= 3:
            n_steps = 7
        elif len(params) <= 6:
            n_steps = 5
        else:
            n_steps = 4

        for p in params:
            if isinstance(p.default, int) and p.key not in (
                "school_weight.graham", "school_weight.buffett",
                "school_weight.quantitative", "school_weight.quality",
            ):
                vals = _int_linspace(int(p.min_val), int(p.max_val), n_steps)
            else:
                vals = _float_linspace(p.min_val, p.max_val, n_steps)

            # Always include the current value in the grid
            if isinstance(p.default, int):
                current = int(p.current)
                if current not in vals:
                    vals.append(current)
                    vals.sort()
            else:
                current = round(float(p.current), 3)
                if current not in vals:
                    vals.append(current)
                    vals.sort()

            grid_values[p.key] = vals

        # Cartesian product
        keys = list(grid_values.keys())
        values = list(grid_values.values())
        combos = list(itertools.product(*values))

        total_combos = len(combos)
        logger.info(
            f"[Optimizer] Grid search: {len(params)} params × "
            f"{n_steps} steps = {total_combos} combinations "
            f"(cap at {self.config.max_trials})"
        )

        # Shuffle to avoid systematic bias if we hit max_trials early
        self._rng.shuffle(combos)

        return [{k: v for k, v in zip(keys, combo)} for combo in combos]

    def _random_candidates(self, params: List[OptimizableParam]) -> List[Dict[str, Any]]:
        """Generate random search candidates."""
        candidates = []
        for _ in range(self.config.max_trials):
            combo = {}
            for p in params:
                if isinstance(p.default, int) and p.key not in (
                    "school_weight.graham", "school_weight.buffett",
                    "school_weight.quantitative", "school_weight.quality",
                    "school_weight.valuation", "school_weight.contrarian",
                    "school_weight.garp",
                ):
                    combo[p.key] = self._rng.randint(int(p.min_val), int(p.max_val))
                else:
                    # For floats, use 2 decimal places
                    val = self._rng.uniform(p.min_val, p.max_val)
                    combo[p.key] = round(val, 2)
            candidates.append(combo)
        return candidates

    # ──────────────────────────────────────────────────────────
    #  Parameter application / restoration
    # ──────────────────────────────────────────────────────────

    def _apply_params(self, param_dict: Dict[str, Any]):
        """Apply a parameter set to the global registry."""
        for key, value in param_dict.items():
            _P.override(key, value, reason=f"optimizer_trial_{self.run_id}")

    def _restore_params(self, params: List[OptimizableParam]):
        """Restore parameters to their original values."""
        for p in params:
            _P.override(p.key, p.current, reason="optimizer_restore")

    # ──────────────────────────────────────────────────────────
    #  Single-trial evaluation (synchronous, CPU-bound)
    # ──────────────────────────────────────────────────────────

    def _evaluate_params(
        self,
        param_dict: Dict[str, Any],
        snapshots: Dict[str, Dict[date, Any]],
        rebalance_dates: List[date],
        fetcher: HistoricalDataFetcher,
    ) -> TrialResult:
        """Evaluate a parameter set against the given rebalance dates.

        Returns a TrialResult with metrics computed from the date range.
        """
        try:
            screener = HistoricalScreener(strategy=self.config.strategy)

            # Screen at each date
            screening_results = {}
            for rd in rebalance_dates:
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

            if not screening_results:
                return TrialResult(
                    trial_id=-1,
                    params=param_dict,
                    error="No stocks passed screening at any rebalance point",
                )

            # Simulate trades
            bt_config = PITBacktestConfig(
                symbols=self.config.symbols,
                holding_months=self.config.holding_months,
                lookback_years=self.config.lookback_years,
                commission_rate=self.config.commission_rate,
                slippage_rate=self.config.slippage_rate,
                stop_loss_pct=self.config.stop_loss_pct,
                max_holdings=self.config.max_holdings,
                initial_capital=self.config.initial_capital,
                benchmark=self.config.benchmark,
                strategy=self.config.strategy,
            )

            simulator = TradeSimulator(
                config=bt_config,
                price_getter=fetcher.get_price_on_date,
                price_series_getter=fetcher.get_price_series,
            )

            sorted_dates = sorted(screening_results.keys())
            simulator.initialise(sorted_dates[0])

            periods: List[PeriodSummary] = []
            for idx, rd in enumerate(sorted_dates):
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

            if not periods:
                return TrialResult(
                    trial_id=-1,
                    params=param_dict,
                    error="No periods could be simulated",
                )

            # Calculate metrics
            nav_series = simulator.get_nav_series()
            metrics = calculate_metrics(
                periods=periods,
                nav_series=nav_series,
                initial_capital=self.config.initial_capital,
                holding_months=self.config.holding_months,
            )

            return TrialResult(
                trial_id=-1,
                params=param_dict,
                train_sharpe=metrics.sharpe_ratio,
                train_return=metrics.total_return,
                train_alpha=metrics.alpha,
                train_win_rate=metrics.win_rate,
                train_max_drawdown=metrics.max_drawdown,
                train_periods=len(periods),
            )

        except Exception as e:
            logger.error(f"[Optimizer] Trial evaluation error: {e}")
            return TrialResult(
                trial_id=-1,
                params=param_dict,
                error=str(e)[:200],
            )

    # ──────────────────────────────────────────────────────────
    #  Summary builder
    # ──────────────────────────────────────────────────────────

    def _build_summary(
        self,
        target_achieved: bool,
        best_trial: Optional[TrialResult],
        baseline_sharpe: float,
        best_sharpe: float,
        total_trials: int,
        duration: float,
    ) -> str:
        """Build a human-readable summary."""
        if not best_trial:
            return "优化失败: 没有有效的试验结果"

        improvement = best_sharpe - baseline_sharpe
        parts = [
            f"优化完成 — {total_trials} 次试验, 耗时 {duration:.0f}s",
        ]

        if target_achieved:
            parts.append(
                f"✅ 目标达成! Test Sharpe = {_fmt_sharpe(best_trial.test_sharpe)} "
                f"(目标 ≥ {self.config.target_sharpe})"
            )
        else:
            parts.append(
                f"⚠️ 未达目标: Test Sharpe = {_fmt_sharpe(best_trial.test_sharpe)} "
                f"(目标 ≥ {self.config.target_sharpe})"
            )

        parts.append(
            f"基线 Sharpe: {_fmt_sharpe(baseline_sharpe)} → 最优: {_fmt_sharpe(best_sharpe)} "
            f"(提升 {improvement:+.3f})"
        )

        # List changed params
        changed = []
        for key, val in best_trial.params.items():
            p = _P.get_def(key)
            if p and val != p.default:
                changed.append(f"  {key}: {p.default} → {val}")
        if changed:
            parts.append("最优参数调整:")
            parts.extend(changed)

        return "\n".join(parts)


# ═══════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════

def _evt(event_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    return {"type": event_type, "data": data}


def _fmt_sharpe(s: Optional[float]) -> str:
    return f"{s:.3f}" if s is not None else "N/A"


def _float_linspace(start: float, end: float, n: int) -> List[float]:
    """Generate n evenly spaced floats from start to end."""
    if n <= 1:
        return [start]
    step = (end - start) / (n - 1)
    return [round(start + i * step, 3) for i in range(n)]


def _int_linspace(start: int, end: int, n: int) -> List[int]:
    """Generate n evenly spaced ints from start to end."""
    if n <= 1:
        return [start]
    vals = _float_linspace(float(start), float(end), n)
    return sorted(set(int(round(v)) for v in vals))
