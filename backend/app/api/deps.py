"""FastAPI dependency injection — singletons for AgentService, KnowledgeBaseService, etc."""

from functools import lru_cache

from app.agent.service import AgentService
from app.config import get_settings


# ── Singletons ──

_agent_service: AgentService | None = None


def get_agent_service() -> AgentService:
    """Return the singleton AgentService instance."""
    global _agent_service
    if _agent_service is None:
        _agent_service = AgentService()
    return _agent_service


def get_data_router():
    """Placeholder data router.

    The pipeline currently fetches data directly via yfinance in tools.py.
    This is kept for interface compatibility with graph.py's screener_node.
    """
    return None
