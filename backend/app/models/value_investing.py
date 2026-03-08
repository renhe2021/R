"""Value investing ORM models."""

from datetime import datetime, timezone

from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime, JSON

from app.database import Base


class InvestorLibrary(Base):
    __tablename__ = "investor_library"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(512), nullable=False)
    author = Column(String(256), default="")
    subcategory = Column(String(128), default="")
    difficulty = Column(String(32), default="")
    summary = Column(Text, default="")
    key_principles = Column(JSON, default=list)
    actionable_criteria = Column(JSON, default=list)
    quotes = Column(JSON, default=list)
    is_indexed = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
