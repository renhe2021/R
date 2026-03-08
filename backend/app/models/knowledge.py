"""Knowledge base ORM models."""

from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, JSON

from app.database import Base


class TheoryFramework(Base):
    __tablename__ = "theory_frameworks"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(256), unique=True, nullable=False)
    type = Column(String(64), default="")
    description = Column(Text, default="")
    applicable_markets = Column(JSON, default=list)
    core_indicators = Column(JSON, default=list)
    screening_rules = Column(JSON, default=dict)
    market_conditions = Column(Text, default="")
    is_active = Column(Boolean, default=True)
    is_builtin = Column(Boolean, default=False)
    source_file = Column(String(256), default="")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class UserDocument(Base):
    __tablename__ = "user_documents"

    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(String(512), nullable=False)
    file_path = Column(Text, default="")
    doc_type = Column(String(32), default="")
    status = Column(String(32), default="pending")
    chunk_count = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    indexed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class ResearchResult(Base):
    __tablename__ = "research_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    query = Column(Text, nullable=False)
    mode = Column(String(32), default="concise")
    content = Column(Text, default="")
    sources = Column(JSON, default=list)
    saved_to_kb = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
