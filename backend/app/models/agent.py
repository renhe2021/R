"""Agent-related ORM models: sessions, messages, verdicts."""

import json
from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String, Text, DateTime

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
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
