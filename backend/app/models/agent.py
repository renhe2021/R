"""Agent-related ORM models: sessions, messages, verdicts, backtest runs."""

import json
from datetime import datetime, timezone

from sqlalchemy import Column, Integer, Float, String, Text, DateTime

from app.database import Base


class AgentSession(Base):
    __tablename__ = "agent_sessions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(64), unique=True, nullable=False, index=True)
    title = Column(String(256), default="")
    mode = Column(String(32), default="chat")
    stock_symbols = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AgentMessage(Base):
    __tablename__ = "agent_messages"

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(64), nullable=False, index=True)
    role = Column(String(16), nullable=False)  # human / ai / system
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AgentVerdict(Base):
    __tablename__ = "agent_verdicts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(32), unique=True, nullable=False, index=True)
    input_stocks = Column(Text, nullable=True)
    verdict_json = Column(Text, nullable=True)
    final_picks_count = Column(Integer, default=0)
    red_flags_count = Column(Integer, default=0)
    status = Column(String(32), default="completed")
    charlie_summary = Column(Text, nullable=True)
    debate_json = Column(Text, nullable=True)       # NEW: Full committee debate records (JSON)
    committee_votes = Column(Text, nullable=True)    # NEW: Vote summary (JSON)
    strategy_backtest = Column(Text, nullable=True)  # NEW: Strategy backtest result (JSON)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class BacktestRun(Base):
    """Persists each PIT backtest run for history / comparison."""
    __tablename__ = "backtest_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(64), unique=True, nullable=False, index=True)
    status = Column(String(32), default="running")      # running | completed | failed
    config_json = Column(Text, nullable=True)            # PITBacktestConfig serialised
    result_json = Column(Text, nullable=True)            # PITBacktestResult serialised
    verdict = Column(String(32), nullable=True)          # VALIDATED | MIXED | FAILED
    total_return = Column(Float, nullable=True)
    annualized_return = Column(Float, nullable=True)
    alpha = Column(Float, nullable=True)
    sharpe_ratio = Column(Float, nullable=True)
    max_drawdown = Column(Float, nullable=True)
    win_rate = Column(Float, nullable=True)
    duration_seconds = Column(Float, nullable=True)
    symbols_csv = Column(Text, nullable=True)            # comma-separated input symbols
    strategy = Column(String(64), nullable=True)
    error = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
