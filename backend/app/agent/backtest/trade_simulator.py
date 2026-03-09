"""Trade simulator with transaction costs, stop-loss, and rebalancing.

Simulates actual portfolio management:
  1. At each rebalance date, sell positions no longer in recommended list.
  2. Buy new recommendations with equal-weight (or conviction-weight) allocation.
  3. Daily stop-loss monitoring within each holding period.
  4. Deducts commission and slippage on every trade.

All monetary amounts are in the portfolio's base currency (USD).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from app.agent.backtest.models import (
    PITBacktestConfig,
    ScreeningResult,
    Trade,
    HoldingRecord,
    PeriodSummary,
)

logger = logging.getLogger(__name__)


class TradeSimulator:
    """Simulate trades with realistic cost model.

    Usage::

        sim = TradeSimulator(config, price_getter)
        sim.initialise(start_nav)
        summary = sim.execute_rebalance(period_idx, screening_result, ...)
    """

    def __init__(
        self,
        config: PITBacktestConfig,
        price_getter,
        price_series_getter=None,
    ):
        """
        Args:
            config: Backtest configuration
            price_getter: callable(symbol, date) → Optional[float]
            price_series_getter: callable(symbol, start, end) → pd.Series
        """
        self.cfg = config
        self._price = price_getter
        self._price_series = price_series_getter
        self.cash: float = config.initial_capital
        self.nav: float = config.initial_capital
        self.positions: Dict[str, HoldingRecord] = {}  # symbol → HoldingRecord
        self.all_trades: List[Trade] = []
        self.nav_history: List[Dict[str, Any]] = []  # [{date, nav, benchmark_nav}]

    def initialise(self, start_date: date):
        """Reset state for a fresh run."""
        self.cash = self.cfg.initial_capital
        self.nav = self.cfg.initial_capital
        self.positions = {}
        self.all_trades = []
        self.nav_history = [
            {"date": str(start_date), "nav": self.nav, "benchmark_nav": self.cfg.initial_capital}
        ]

    # ──────────────────────────────────────────────────────────
    #  Core: execute one rebalance window
    # ──────────────────────────────────────────────────────────

    def execute_rebalance(
        self,
        period_idx: int,
        screening: ScreeningResult,
        period_start: date,
        period_end: date,
        benchmark_start_price: Optional[float] = None,
        benchmark_end_price: Optional[float] = None,
    ) -> PeriodSummary:
        """Execute a full rebalance cycle.

        1. Sell holdings not in new recommendation.
        2. Buy new recommendations.
        3. Run daily stop-loss monitoring.
        4. Record period summary.
        """
        start_nav = self._calc_nav(period_start)
        trades: List[Trade] = []
        stop_losses = 0

        new_symbols = set(screening.passed_symbols)
        old_symbols = set(self.positions.keys())

        # ── Step 1: Sell positions no longer recommended ──
        to_sell = old_symbols - new_symbols
        for sym in to_sell:
            t = self._sell(sym, period_start, reason="REBALANCE")
            if t:
                trades.append(t)

        # ── Step 2: Determine target weights ──
        target_weights = self._compute_weights(screening)

        # ── Step 3: Rebalance existing + buy new ──
        total_value = self._calc_nav(period_start)
        for sym in screening.passed_symbols:
            target_val = total_value * target_weights.get(sym, 0)
            current_val = self._position_value(sym, period_start)

            if sym in self.positions:
                diff = target_val - current_val
                if abs(diff) > total_value * 0.02:  # >2% drift → trade
                    if diff > 0:
                        t = self._buy(sym, period_start, diff)
                    else:
                        t = self._sell_partial(sym, period_start, abs(diff))
                    if t:
                        trades.append(t)
            else:
                t = self._buy(sym, period_start, target_val)
                if t:
                    trades.append(t)

        # ── Step 4: Daily stop-loss monitoring ──
        if self._price_series and self.cfg.stop_loss_pct > 0:
            sl_trades, sl_count = self._run_stop_loss(period_start, period_end)
            trades.extend(sl_trades)
            stop_losses = sl_count

        # ── Step 5: Build daily NAV within period ──
        self._build_daily_nav(period_start, period_end, benchmark_start_price, benchmark_end_price)

        end_nav = self._calc_nav(period_end)
        port_ret = (end_nav - start_nav) / start_nav if start_nav > 0 else 0

        bench_ret = 0.0
        if benchmark_start_price and benchmark_end_price and benchmark_start_price > 0:
            bench_ret = (benchmark_end_price - benchmark_start_price) / benchmark_start_price

        alpha = port_ret - bench_ret

        commission_paid = sum(t.commission for t in trades)
        slippage_paid = sum(t.slippage_cost for t in trades)

        holdings_list = list(self.positions.values())
        for h in holdings_list:
            if h.sell_date is None:
                p = self._price(h.symbol, period_end)
                if p and h.buy_price > 0:
                    h.return_pct = (p - h.buy_price) / h.buy_price

        summary = PeriodSummary(
            period_index=period_idx,
            start_date=period_start,
            end_date=period_end,
            start_nav=round(start_nav, 2),
            end_nav=round(end_nav, 2),
            portfolio_return=round(port_ret, 6),
            benchmark_return=round(bench_ret, 6),
            alpha=round(alpha, 6),
            holdings_count=len(self.positions),
            trades=trades,
            holdings=[_clone_holding(h) for h in holdings_list],
            stop_losses_triggered=stop_losses,
            commission_paid=round(commission_paid, 2),
            slippage_paid=round(slippage_paid, 2),
        )

        self.all_trades.extend(trades)
        self.nav = end_nav
        return summary

    # ──────────────────────────────────────────────────────────
    #  Weight computation
    # ──────────────────────────────────────────────────────────

    def _compute_weights(self, screening: ScreeningResult) -> Dict[str, float]:
        """Compute target portfolio weights."""
        n = len(screening.passed_symbols)
        if n == 0:
            return {}

        if self.cfg.weighting == "conviction":
            # Weight by composite score
            total_score = sum(screening.composite_scores.get(s, 50) for s in screening.passed_symbols)
            if total_score <= 0:
                total_score = n * 50
            weights = {
                s: screening.composite_scores.get(s, 50) / total_score
                for s in screening.passed_symbols
            }
        else:
            # Equal weight
            w = 1.0 / n
            weights = {s: w for s in screening.passed_symbols}

        return weights

    # ──────────────────────────────────────────────────────────
    #  Trading primitives
    # ──────────────────────────────────────────────────────────

    def _buy(self, symbol: str, trade_date: date, amount: float) -> Optional[Trade]:
        """Buy *amount* worth of *symbol*.

        Caps the buy amount to available cash to prevent negative cash balance.
        """
        price = self._price(symbol, trade_date)
        if not price or price <= 0:
            return None

        # Cap amount to available cash — never go negative
        amount = min(amount, self.cash)
        if amount <= 0:
            return None

        commission = amount * self.cfg.commission_rate
        slippage = amount * self.cfg.slippage_rate
        effective_amount = amount - commission - slippage
        if effective_amount <= 0:
            return None

        shares = effective_amount / price
        self.cash -= amount

        if symbol in self.positions:
            pos = self.positions[symbol]
            total_shares = pos.shares + shares
            pos.buy_price = (pos.buy_price * pos.shares + price * shares) / total_shares
            pos.shares = total_shares
        else:
            weight = amount / max(self.nav, 1)
            self.positions[symbol] = HoldingRecord(
                symbol=symbol,
                buy_date=trade_date,
                buy_price=price,
                shares=shares,
                weight=weight,
            )

        return Trade(
            trade_date=trade_date,
            symbol=symbol,
            action="BUY",
            shares=round(shares, 4),
            price=round(price, 2),
            commission=round(commission, 2),
            slippage_cost=round(slippage, 2),
            proceeds=round(-amount, 2),
            reason="REBALANCE",
        )

    def _sell(self, symbol: str, trade_date: date, reason: str = "REBALANCE") -> Optional[Trade]:
        """Sell entire position of *symbol*."""
        pos = self.positions.get(symbol)
        if not pos:
            return None

        price = self._price(symbol, trade_date)
        if not price or price <= 0:
            return None

        gross = pos.shares * price
        commission = gross * self.cfg.commission_rate
        slippage = gross * self.cfg.slippage_rate
        net = gross - commission - slippage
        self.cash += net

        pos.sell_date = trade_date
        pos.sell_price = price
        pos.sell_reason = reason
        pos.return_pct = (price - pos.buy_price) / pos.buy_price if pos.buy_price > 0 else 0

        trade = Trade(
            trade_date=trade_date,
            symbol=symbol,
            action="SELL" if reason == "REBALANCE" else "STOP_LOSS",
            shares=round(pos.shares, 4),
            price=round(price, 2),
            commission=round(commission, 2),
            slippage_cost=round(slippage, 2),
            proceeds=round(net, 2),
            reason=reason,
        )

        del self.positions[symbol]
        return trade

    def _sell_partial(self, symbol: str, trade_date: date, amount: float) -> Optional[Trade]:
        """Sell *amount* worth of *symbol* (partial trim)."""
        pos = self.positions.get(symbol)
        if not pos:
            return None

        price = self._price(symbol, trade_date)
        if not price or price <= 0:
            return None

        shares_to_sell = min(amount / price, pos.shares)
        gross = shares_to_sell * price
        commission = gross * self.cfg.commission_rate
        slippage = gross * self.cfg.slippage_rate
        net = gross - commission - slippage
        self.cash += net

        pos.shares -= shares_to_sell
        if pos.shares < 0.001:
            del self.positions[symbol]

        return Trade(
            trade_date=trade_date,
            symbol=symbol,
            action="SELL",
            shares=round(shares_to_sell, 4),
            price=round(price, 2),
            commission=round(commission, 2),
            slippage_cost=round(slippage, 2),
            proceeds=round(net, 2),
            reason="TRIM",
        )

    # ──────────────────────────────────────────────────────────
    #  Stop-loss
    # ──────────────────────────────────────────────────────────

    def _run_stop_loss(
        self, start: date, end: date
    ) -> Tuple[List[Trade], int]:
        """Check stop-loss daily for all positions."""
        trades: List[Trade] = []
        triggered = 0
        symbols = list(self.positions.keys())

        for sym in symbols:
            if sym not in self.positions:
                continue
            pos = self.positions[sym]
            stop_price = pos.buy_price * (1 - self.cfg.stop_loss_pct)

            if not self._price_series:
                continue

            series = self._price_series(sym, start, end)
            if series is None or series.empty:
                continue

            for idx_date, close in series.items():
                d = idx_date.date() if hasattr(idx_date, 'date') else idx_date
                if d <= start:
                    continue
                if close <= stop_price and sym in self.positions:
                    t = self._sell(sym, d, reason="STOP_LOSS")
                    if t:
                        trades.append(t)
                        triggered += 1
                    break

        return trades, triggered

    # ──────────────────────────────────────────────────────────
    #  NAV calculation
    # ──────────────────────────────────────────────────────────

    def _calc_nav(self, as_of: date) -> float:
        """Calculate total portfolio value (cash + all position values)."""
        total = self.cash
        for sym, pos in self.positions.items():
            total += self._position_value(sym, as_of)
        return total

    def _position_value(self, symbol: str, as_of: date) -> float:
        """Value of a single position at *as_of*."""
        pos = self.positions.get(symbol)
        if not pos:
            return 0.0
        price = self._price(symbol, as_of)
        if not price:
            price = pos.buy_price  # fallback
        return pos.shares * price

    def _build_daily_nav(
        self,
        start: date,
        end: date,
        bench_start_price: Optional[float],
        bench_end_price: Optional[float],
    ):
        """Append approximate daily NAV points for charting."""
        bench_initial = self.nav_history[-1]["benchmark_nav"] if self.nav_history else self.cfg.initial_capital
        d = start
        while d <= end:
            nav = self._calc_nav(d)
            # benchmark NAV approximation (linear interpolation if no daily data)
            bench_nav = bench_initial
            if bench_start_price and bench_end_price and bench_start_price > 0:
                total_days = max((end - start).days, 1)
                elapsed = (d - start).days
                bench_ret = (bench_end_price / bench_start_price - 1) * (elapsed / total_days)
                bench_nav = bench_initial * (1 + bench_ret)

            self.nav_history.append({
                "date": str(d),
                "nav": round(nav, 2),
                "benchmark_nav": round(bench_nav, 2),
            })
            d += timedelta(days=7)  # weekly sampling for chart

    def get_nav_series(self) -> List[Dict[str, Any]]:
        """Return the full NAV history."""
        return self.nav_history

    def get_total_trades(self) -> List[Trade]:
        return self.all_trades


def _clone_holding(h: HoldingRecord) -> HoldingRecord:
    """Shallow clone a HoldingRecord."""
    return HoldingRecord(
        symbol=h.symbol,
        buy_date=h.buy_date,
        buy_price=h.buy_price,
        shares=h.shares,
        weight=h.weight,
        sell_date=h.sell_date,
        sell_price=h.sell_price,
        sell_reason=h.sell_reason,
        return_pct=h.return_pct,
    )
