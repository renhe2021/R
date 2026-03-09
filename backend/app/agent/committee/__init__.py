"""Investment Committee — Multi-Agent debate & decision engine.

Simulates a professional fund investment committee with:
- 7 School Agents (one per investment philosophy)
- 5 Role Agents (Research/Quant/Risk/Macro/Sector)
- 1 Portfolio Manager (final arbiter)
- Structured 2-round debate with veto mechanism
"""

from app.agent.committee.models import (
    Stance, AgentOpinion, DebateRecord, VetoDecision,
)
from app.agent.committee.base_agent import InvestmentAgent
from app.agent.committee.debate import DebateEngine

__all__ = [
    "Stance", "AgentOpinion", "DebateRecord", "VetoDecision",
    "InvestmentAgent", "DebateEngine",
]
