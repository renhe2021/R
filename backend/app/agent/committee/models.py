"""Data models for the Investment Committee multi-agent system.

All structured data passed between agents, the debate engine,
and the pipeline is defined here.
"""

from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from enum import Enum


class Stance(str, Enum):
    """Agent's investment stance on a stock."""
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    AVOID = "AVOID"
    REJECT = "REJECT"  # Risk Manager only — triggers veto check


# Numeric weights for aggregation
STANCE_WEIGHTS: Dict[str, float] = {
    "STRONG_BUY": 1.0,
    "BUY": 0.7,
    "HOLD": 0.4,
    "AVOID": 0.15,
    "REJECT": 0.0,
}


@dataclass
class AgentOpinion:
    """A single Agent's independent analysis on one stock."""
    agent_name: str
    agent_type: str           # "school" | "role"
    symbol: str
    stance: str               # Stance value as string
    confidence: float         # 0.0 - 1.0
    key_reasons: List[str]    # Top 3 reasons for stance
    risks: List[str]          # Top 3 risks identified
    data_points: Dict[str, str]  # Key metrics cited {metric_name: value_string}
    analysis_text: str        # Full analysis narrative (500-1500 chars)
    veto: bool = False        # Only Risk Manager can set True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent_name": self.agent_name,
            "agent_type": self.agent_type,
            "symbol": self.symbol,
            "stance": self.stance,
            "confidence": self.confidence,
            "key_reasons": self.key_reasons,
            "risks": self.risks,
            "data_points": self.data_points,
            "analysis_text": self.analysis_text,
            "veto": self.veto,
        }


@dataclass
class VetoDecision:
    """Risk Manager's veto decision."""
    triggered: bool = False
    reason: str = ""
    quantitative_triggers: List[str] = field(default_factory=list)
    # Which hard rules fired: e.g. ["Z-Score=1.5 < 1.81", "F-Score=2 <= 3"]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "triggered": self.triggered,
            "reason": self.reason,
            "quantitative_triggers": self.quantitative_triggers,
        }


@dataclass
class DebateRecord:
    """Complete record of one investment committee session for one stock."""
    symbol: str
    stock_name: str = ""

    # Round 1: independent opinions
    round1_opinions: List[AgentOpinion] = field(default_factory=list)

    # Round 2: debate & verdict
    debate_summary: str = ""          # LLM-generated debate narrative
    dissent_points: List[str] = field(default_factory=list)  # Key disagreements

    # Votes
    votes: Dict[str, str] = field(default_factory=dict)  # agent_name -> stance
    vote_tally: Dict[str, int] = field(default_factory=dict)  # stance -> count

    # Veto
    veto: VetoDecision = field(default_factory=VetoDecision)

    # Final verdict
    final_verdict: str = "HOLD"       # Stance value
    final_confidence: float = 0.0
    final_reasoning: str = ""         # PM's reasoning
    portfolio_manager_summary: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "stock_name": self.stock_name,
            "round1_opinions": [o.to_dict() for o in self.round1_opinions],
            "debate_summary": self.debate_summary,
            "dissent_points": self.dissent_points,
            "votes": self.votes,
            "vote_tally": self.vote_tally,
            "veto": self.veto.to_dict(),
            "final_verdict": self.final_verdict,
            "final_confidence": self.final_confidence,
            "final_reasoning": self.final_reasoning,
            "portfolio_manager_summary": self.portfolio_manager_summary,
        }

    @property
    def opinions_by_type(self) -> Dict[str, List[AgentOpinion]]:
        """Group opinions by agent_type."""
        groups: Dict[str, List[AgentOpinion]] = {"school": [], "role": []}
        for op in self.round1_opinions:
            groups.setdefault(op.agent_type, []).append(op)
        return groups

    @property
    def majority_stance(self) -> str:
        """The stance with the most votes."""
        if not self.vote_tally:
            return "HOLD"
        return max(self.vote_tally, key=self.vote_tally.get)  # type: ignore

    @property
    def consensus_ratio(self) -> float:
        """Fraction of agents agreeing with majority."""
        if not self.vote_tally:
            return 0.0
        total = sum(self.vote_tally.values())
        majority_count = max(self.vote_tally.values())
        return majority_count / total if total > 0 else 0.0
