"""Walk-Forward Validation — Rolling train/test windows for robust threshold testing.

Unlike a single Train/Test split, Walk-Forward uses multiple overlapping windows:

    |---- Train 1 (4yr) ----|---- Test 1 (3yr) ----|
              |---- Train 2 (4yr) ----|---- Test 2 (3yr) ----|
                        |---- Train 3 (4yr) ----|---- Test 3 (3yr) ----|

Each test window is at least 3 years long to ensure robust out-of-sample
validation of the entire stock-picking logic.

Design
──────
1. Divide the full history (default 10 years) into K folds (default 3).
2. Each fold: Grid Search on the train window (4 yr) → evaluate on test window (3 yr).
3. Report the AVERAGE out-of-sample Sharpe across all folds.
4. Only parameters that perform well across ALL folds are considered robust.

This module wraps the ThresholdOptimizer for the inner optimisation loop.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Dict, List, Optional

from app.agent.backtest.threshold_optimizer import (
    ThresholdOptimizer,
    OptimizationConfig,
    OptimizableParam,
    TrialResult,
    _evt,
    _fmt_sharpe,
    DEFAULT_OPTIMIZE_KEYS,
)
from app.agent.backtest.historical_data import HistoricalDataFetcher
from app.agent.investment_params import params as _P

logger = logging.getLogger(__name__)


@dataclass
class WalkForwardConfig:
    """Configuration for Walk-Forward validation.

    Each fold: Train on N months, then test on M months (≥ 36 = 3 years).
    The folds slide forward so each uses a different test period.
    """
    symbols: List[str]
    # Parameters to optimise
    param_keys: List[str] = field(default_factory=list)
    # Walk-Forward settings
    n_folds: int = 3              # number of rolling windows
    train_window_months: int = 48  # training window size (4 years)
    test_window_months: int = 36   # test window size (3 years, must be ≥ 36)
    # Inner optimiser settings
    search_method: str = "grid"
    max_trials_per_fold: int = 100  # trials per fold
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
    # Targets
    target_sharpe: float = 1.0
    seed: int = 42

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class FoldResult:
    """Result of one walk-forward fold."""
    fold_index: int
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    best_params: Dict[str, Any] = field(default_factory=dict)
    train_sharpe: Optional[float] = None
    test_sharpe: Optional[float] = None
    test_return: float = 0.0
    test_alpha: float = 0.0
    test_win_rate: float = 0.0
    test_max_drawdown: float = 0.0
    trials_run: int = 0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class WalkForwardResult:
    """Complete result of Walk-Forward validation."""
    run_id: str
    config: WalkForwardConfig
    folds: List[FoldResult] = field(default_factory=list)
    # Aggregates across folds
    avg_oos_sharpe: Optional[float] = None    # average out-of-sample Sharpe
    min_oos_sharpe: Optional[float] = None
    max_oos_sharpe: Optional[float] = None
    avg_oos_return: float = 0.0
    avg_oos_alpha: float = 0.0
    # Consensus best params (most frequent across folds)
    consensus_params: Dict[str, Any] = field(default_factory=dict)
    # Metadata
    target_achieved: bool = False
    total_trials: int = 0
    started_at: str = ""
    finished_at: str = ""
    duration_seconds: float = 0.0
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "config": self.config.to_dict(),
            "folds": [f.to_dict() for f in self.folds],
            "avg_oos_sharpe": self.avg_oos_sharpe,
            "min_oos_sharpe": self.min_oos_sharpe,
            "max_oos_sharpe": self.max_oos_sharpe,
            "avg_oos_return": round(self.avg_oos_return, 4),
            "avg_oos_alpha": round(self.avg_oos_alpha, 4),
            "consensus_params": self.consensus_params,
            "target_achieved": self.target_achieved,
            "total_trials": self.total_trials,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "error": self.error,
        }


class WalkForwardValidator:
    """Rolling Walk-Forward validation for threshold robustness.

    Usage::

        cfg = WalkForwardConfig(
            symbols=["AAPL", "MSFT", ...],
            n_folds=3,
            max_trials_per_fold=20,
        )
        validator = WalkForwardValidator(cfg)
        async for event in validator.run():
            yield f"data: {json.dumps(event)}\\n\\n"
    """

    def __init__(self, config: WalkForwardConfig):
        self.config = config
        self.run_id = uuid.uuid4().hex[:16]
        self._result: Optional[WalkForwardResult] = None

    async def run(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Execute walk-forward validation, yielding SSE events."""
        started = datetime.now(timezone.utc)

        yield _evt("walkforward_start", {
            "run_id": self.run_id,
            "config": self.config.to_dict(),
            "message": f"开始 Walk-Forward 验证 — "
                       f"{self.config.n_folds} 折, "
                       f"每折 {self.config.max_trials_per_fold} 次试验, "
                       f"训练窗口 {self.config.train_window_months}月 "
                       f"(~{self.config.train_window_months / 12:.1f}年), "
                       f"测试窗口 {self.config.test_window_months}月 "
                       f"(~{self.config.test_window_months / 12:.1f}年)",
        })

        try:
            # ── Phase 1: Fetch all historical data upfront ──
            yield _evt("walkforward_phase", {
                "phase": "data_fetch",
                "message": "正在获取完整历史数据...",
            })

            fetcher = HistoricalDataFetcher(
                symbols=self.config.symbols,
                lookback_years=self.config.lookback_years,
                holding_months=self.config.holding_months,
                benchmark=self.config.benchmark,
            )
            all_rebalance_dates = fetcher.compute_rebalance_dates()
            snapshots = await fetcher.fetch_all()

            if not snapshots or len(all_rebalance_dates) < 3:
                yield _evt("walkforward_error", {
                    "message": f"历史数据不足 — "
                               f"需要至少 3 个再平衡时点, 实际 {len(all_rebalance_dates)}",
                })
                return

            yield _evt("walkforward_progress", {
                "phase": "data_fetch",
                "progress": 100,
                "message": f"数据获取完成 — {len(snapshots)} 只股票, "
                           f"{len(all_rebalance_dates)} 个时点 "
                           f"({all_rebalance_dates[0]} → {all_rebalance_dates[-1]})",
            })

            # ── Phase 2: Generate fold windows ──
            folds = self._generate_folds(all_rebalance_dates)

            if not folds:
                yield _evt("walkforward_error", {
                    "message": "无法生成足够的训练/测试窗口",
                })
                return

            yield _evt("walkforward_phase", {
                "phase": "folds",
                "message": f"生成 {len(folds)} 个 Walk-Forward 折",
                "folds": [
                    {
                        "fold": i + 1,
                        "train": f"{train[0]} → {train[-1]} ({len(train)} 窗口)",
                        "test": f"{test[0]} → {test[-1]} ({len(test)} 窗口)",
                    }
                    for i, (train, test) in enumerate(folds)
                ],
            })

            # ── Phase 3: Run optimisation for each fold ──
            fold_results: List[FoldResult] = []
            total_trials = 0

            for fold_idx, (train_dates, test_dates) in enumerate(folds):
                yield _evt("walkforward_fold_start", {
                    "fold": fold_idx + 1,
                    "total_folds": len(folds),
                    "train_dates": f"{train_dates[0]} → {train_dates[-1]}",
                    "test_dates": f"{test_dates[0]} → {test_dates[-1]}",
                    "message": f"Fold {fold_idx + 1}/{len(folds)}: "
                               f"训练 {train_dates[0]}→{train_dates[-1]} | "
                               f"测试 {test_dates[0]}→{test_dates[-1]}",
                })

                # Create inner optimiser config for this fold
                # The fold already has train_dates and test_dates split,
                # so we pass test_years=0 and use train_ratio to match the fold split
                inner_config = OptimizationConfig(
                    symbols=self.config.symbols,
                    param_keys=self.config.param_keys or DEFAULT_OPTIMIZE_KEYS,
                    search_method=self.config.search_method,
                    max_trials=self.config.max_trials_per_fold,
                    holding_months=self.config.holding_months,
                    lookback_years=self.config.lookback_years,
                    strategy=self.config.strategy,
                    benchmark=self.config.benchmark,
                    initial_capital=self.config.initial_capital,
                    max_holdings=self.config.max_holdings,
                    commission_rate=self.config.commission_rate,
                    slippage_rate=self.config.slippage_rate,
                    stop_loss_pct=self.config.stop_loss_pct,
                    test_years=0,  # fold-level split is handled externally
                    train_ratio=len(train_dates) / (len(train_dates) + len(test_dates)),
                    target_sharpe=self.config.target_sharpe,
                    seed=self.config.seed + fold_idx,
                )

                # Run inner optimiser (reuse pre-fetched data)
                inner_opt = ThresholdOptimizer(inner_config)

                # Use the inner optimizer's parameter resolution and search
                param_keys = inner_config.param_keys or DEFAULT_OPTIMIZE_KEYS
                opt_params = inner_opt._resolve_params(param_keys)

                if not opt_params:
                    fold_results.append(FoldResult(
                        fold_index=fold_idx + 1,
                        train_start=str(train_dates[0]),
                        train_end=str(train_dates[-1]),
                        test_start=str(test_dates[0]),
                        test_end=str(test_dates[-1]),
                        error="No optimisable parameters",
                    ))
                    continue

                # Generate candidates
                candidates = inner_opt._generate_candidates(opt_params)
                n_trials = min(len(candidates), self.config.max_trials_per_fold)

                best_fold_trial: Optional[TrialResult] = None

                for trial_idx, candidate in enumerate(candidates[:n_trials]):
                    # Apply params
                    inner_opt._apply_params(candidate)

                    # Evaluate on train dates
                    train_result = inner_opt._evaluate_params(
                        candidate, snapshots, train_dates, fetcher,
                    )

                    # Evaluate on test dates
                    test_result = inner_opt._evaluate_params(
                        candidate, snapshots, test_dates, fetcher,
                    )

                    # Restore
                    inner_opt._restore_params(opt_params)

                    trial = TrialResult(
                        trial_id=trial_idx + 1,
                        params=candidate,
                        train_sharpe=train_result.train_sharpe,
                        train_return=train_result.train_return,
                        train_alpha=train_result.train_alpha,
                        train_win_rate=train_result.train_win_rate,
                        train_max_drawdown=train_result.train_max_drawdown,
                        train_periods=train_result.train_periods,
                        test_sharpe=test_result.train_sharpe,
                        test_return=test_result.train_return,
                        test_alpha=test_result.train_alpha,
                        test_win_rate=test_result.train_win_rate,
                        test_max_drawdown=test_result.train_max_drawdown,
                        test_periods=test_result.train_periods,
                    )

                    if best_fold_trial is None or trial.objective_value > best_fold_trial.objective_value:
                        best_fold_trial = trial

                    total_trials += 1

                    # Yield progress for every 5 trials
                    if (trial_idx + 1) % 5 == 0 or trial_idx == n_trials - 1:
                        yield _evt("walkforward_fold_progress", {
                            "fold": fold_idx + 1,
                            "trial": trial_idx + 1,
                            "total_trials": n_trials,
                            "progress": round((trial_idx + 1) / n_trials * 100),
                            "best_train_sharpe": best_fold_trial.train_sharpe if best_fold_trial else None,
                            "best_test_sharpe": best_fold_trial.test_sharpe if best_fold_trial else None,
                        })

                # Record fold result
                fold_res = FoldResult(
                    fold_index=fold_idx + 1,
                    train_start=str(train_dates[0]),
                    train_end=str(train_dates[-1]),
                    test_start=str(test_dates[0]),
                    test_end=str(test_dates[-1]),
                    best_params=best_fold_trial.params if best_fold_trial else {},
                    train_sharpe=best_fold_trial.train_sharpe if best_fold_trial else None,
                    test_sharpe=best_fold_trial.test_sharpe if best_fold_trial else None,
                    test_return=best_fold_trial.test_return if best_fold_trial else 0,
                    test_alpha=best_fold_trial.test_alpha if best_fold_trial else 0,
                    test_win_rate=best_fold_trial.test_win_rate if best_fold_trial else 0,
                    test_max_drawdown=best_fold_trial.test_max_drawdown if best_fold_trial else 0,
                    trials_run=n_trials,
                )
                fold_results.append(fold_res)

                yield _evt("walkforward_fold_complete", {
                    "fold": fold_idx + 1,
                    "total_folds": len(folds),
                    "train_sharpe": fold_res.train_sharpe,
                    "test_sharpe": fold_res.test_sharpe,
                    "test_return": round(fold_res.test_return, 4),
                    "test_alpha": round(fold_res.test_alpha, 4),
                    "best_params": fold_res.best_params,
                    "message": f"Fold {fold_idx + 1} 完成: "
                               f"Train Sharpe={_fmt_sharpe(fold_res.train_sharpe)} | "
                               f"Test Sharpe={_fmt_sharpe(fold_res.test_sharpe)}",
                })

            # ── Phase 4: Aggregate results ──
            valid_folds = [f for f in fold_results if f.test_sharpe is not None]

            if valid_folds:
                oos_sharpes = [f.test_sharpe for f in valid_folds]
                avg_oos = sum(oos_sharpes) / len(oos_sharpes)
                min_oos = min(oos_sharpes)
                max_oos = max(oos_sharpes)
                avg_return = sum(f.test_return for f in valid_folds) / len(valid_folds)
                avg_alpha = sum(f.test_alpha for f in valid_folds) / len(valid_folds)
            else:
                avg_oos = min_oos = max_oos = None
                avg_return = avg_alpha = 0.0

            # Consensus params — median of each parameter across folds
            consensus = self._compute_consensus(fold_results)

            target_ok = avg_oos is not None and avg_oos >= self.config.target_sharpe

            finished = datetime.now(timezone.utc)
            duration = (finished - started).total_seconds()

            self._result = WalkForwardResult(
                run_id=self.run_id,
                config=self.config,
                folds=fold_results,
                avg_oos_sharpe=round(avg_oos, 3) if avg_oos is not None else None,
                min_oos_sharpe=round(min_oos, 3) if min_oos is not None else None,
                max_oos_sharpe=round(max_oos, 3) if max_oos is not None else None,
                avg_oos_return=avg_return,
                avg_oos_alpha=avg_alpha,
                consensus_params=consensus,
                target_achieved=target_ok,
                total_trials=total_trials,
                started_at=started.isoformat(),
                finished_at=finished.isoformat(),
                duration_seconds=round(duration, 1),
            )

            yield _evt("walkforward_complete", {
                "run_id": self.run_id,
                "target_achieved": target_ok,
                "avg_oos_sharpe": avg_oos,
                "min_oos_sharpe": min_oos,
                "max_oos_sharpe": max_oos,
                "avg_oos_return": round(avg_return, 4),
                "avg_oos_alpha": round(avg_alpha, 4),
                "consensus_params": consensus,
                "folds_summary": [
                    {
                        "fold": f.fold_index,
                        "test_sharpe": f.test_sharpe,
                        "test_return": round(f.test_return, 4),
                    }
                    for f in fold_results
                ],
                "total_trials": total_trials,
                "duration_seconds": round(duration, 1),
                "message": self._build_summary(target_ok, avg_oos, min_oos, max_oos, fold_results, duration),
            })

        except Exception as e:
            logger.exception(f"[WalkForward] Error: {e}")
            yield _evt("walkforward_error", {
                "run_id": self.run_id,
                "message": f"Walk-Forward 验证失败: {str(e)[:300]}",
            })

    def get_result(self) -> Optional[WalkForwardResult]:
        return self._result

    # ──────────────────────────────────────────────────────────
    #  Fold generation
    # ──────────────────────────────────────────────────────────

    def _generate_folds(
        self, dates: List[date]
    ) -> List[tuple]:
        """Generate (train_dates, test_dates) pairs for walk-forward.

        Each fold slides forward by test_window_months, maintaining
        train_window_months of training data.  Test window is typically
        36 months (3 years) to ensure robust out-of-sample validation.
        """
        if len(dates) < 3:
            return []

        # Convert months to approximate number of rebalance periods
        total_months = (dates[-1] - dates[0]).days / 30.44
        periods_per_month = len(dates) / total_months if total_months > 0 else 0

        train_periods = max(2, int(self.config.train_window_months * periods_per_month))
        test_periods = max(1, int(self.config.test_window_months * periods_per_month))

        logger.info(
            f"[WalkForward] Fold parameters: "
            f"train={self.config.train_window_months}mo ({train_periods} periods), "
            f"test={self.config.test_window_months}mo ({test_periods} periods), "
            f"total dates={len(dates)}"
        )

        folds = []
        start = 0

        for _ in range(self.config.n_folds):
            train_end = start + train_periods
            test_end = train_end + test_periods

            if test_end > len(dates):
                break

            train_dates = dates[start:train_end]
            test_dates = dates[train_end:test_end]

            if len(train_dates) >= 2 and len(test_dates) >= 1:
                folds.append((train_dates, test_dates))

            # Slide forward by test_periods
            start += test_periods

        return folds

    # ──────────────────────────────────────────────────────────
    #  Consensus parameters
    # ──────────────────────────────────────────────────────────

    def _compute_consensus(self, fold_results: List[FoldResult]) -> Dict[str, Any]:
        """Compute consensus (median) parameter values across folds."""
        valid_folds = [f for f in fold_results if f.best_params]
        if not valid_folds:
            return {}

        all_keys = set()
        for f in valid_folds:
            all_keys.update(f.best_params.keys())

        consensus = {}
        for key in sorted(all_keys):
            values = [f.best_params[key] for f in valid_folds if key in f.best_params]
            if not values:
                continue
            # Median
            values.sort()
            mid = len(values) // 2
            if len(values) % 2 == 0:
                median_val = (values[mid - 1] + values[mid]) / 2
            else:
                median_val = values[mid]

            # Round to match the parameter's type
            p = _P.get_def(key)
            if p and isinstance(p.default, int):
                consensus[key] = int(round(median_val))
            else:
                consensus[key] = round(median_val, 3)

        return consensus

    # ──────────────────────────────────────────────────────────
    #  Summary
    # ──────────────────────────────────────────────────────────

    def _build_summary(
        self,
        target_ok: bool,
        avg_oos: Optional[float],
        min_oos: Optional[float],
        max_oos: Optional[float],
        folds: List[FoldResult],
        duration: float,
    ) -> str:
        parts = [f"Walk-Forward 验证完成 — {len(folds)} 折, 耗时 {duration:.0f}s"]

        if target_ok:
            parts.append(
                f"✅ 目标达成! 平均样本外 Sharpe = {_fmt_sharpe(avg_oos)} "
                f"(目标 ≥ {self.config.target_sharpe})"
            )
        else:
            parts.append(
                f"⚠️ 未达目标: 平均样本外 Sharpe = {_fmt_sharpe(avg_oos)} "
                f"(目标 ≥ {self.config.target_sharpe})"
            )

        if min_oos is not None and max_oos is not None:
            parts.append(f"Sharpe 分布: min={_fmt_sharpe(min_oos)}, max={_fmt_sharpe(max_oos)}")

        for f in folds:
            parts.append(
                f"  Fold {f.fold_index}: {f.test_start}→{f.test_end} | "
                f"Test Sharpe={_fmt_sharpe(f.test_sharpe)}"
            )

        return "\n".join(parts)
