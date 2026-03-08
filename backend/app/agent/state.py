"""Agent state definitions used across the pipeline."""

from dataclasses import dataclass, field
from typing import TypedDict, Optional, List, Dict, Any


@dataclass
class AgentMessage:
    """A single message in the agent conversation."""
    role: str  # "user" | "assistant" | "system"
    content: str = ""
    tool_calls: list = field(default_factory=list)


class AgentState(TypedDict, total=False):
    """Mutable state passed through the pipeline nodes."""
    messages: List[AgentMessage]
    session_id: str
    mode: str  # "chat" | "analyze"
    input_stocks: List[str]
    data_source: Optional[str]
    screening_results: Optional[Dict[str, Any]]
    interrogation_results: Optional[Dict[str, Any]]
    appraisal_results: Optional[Dict[str, Any]]
    final_report: Optional[Dict[str, Any]]
    sink_results: Optional[Dict[str, Any]]
