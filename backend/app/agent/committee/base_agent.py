"""Base class for all Investment Committee agents.

Each agent:
1. Receives a shared stock data snapshot (StockResult + data_map from Stages 1-6)
2. Runs independent LLM analysis with its own role-specific system prompt
3. Produces a structured AgentOpinion (stance, confidence, reasons, risks)
"""

import json
import logging
import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from app.agent.committee.models import AgentOpinion, Stance

logger = logging.getLogger(__name__)


class InvestmentAgent(ABC):
    """Abstract base class for all investment committee agents."""

    def __init__(self):
        self._name: str = ""
        self._agent_type: str = ""  # "school" | "role"
        self._system_prompt: str = ""

    @property
    def name(self) -> str:
        return self._name

    @property
    def agent_type(self) -> str:
        return self._agent_type

    @property
    def system_prompt(self) -> str:
        return self._system_prompt

    @abstractmethod
    def build_analysis_prompt(self, symbol: str, stock_snapshot: str) -> str:
        """Build the user-facing analysis prompt for this agent.

        Args:
            symbol: Stock ticker
            stock_snapshot: Pre-formatted text containing all Stage 1-6 data

        Returns:
            The user message to send to the LLM
        """
        ...

    async def analyze(self, symbol: str, stock_snapshot: str) -> AgentOpinion:
        """Run independent analysis and return structured opinion.

        Calls LLM with this agent's system prompt + analysis prompt,
        then parses the JSON response into an AgentOpinion.
        """
        from app.agent.llm import simple_completion

        user_prompt = self.build_analysis_prompt(symbol, stock_snapshot)

        messages = [
            {"role": "system", "content": self._system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        try:
            raw = await simple_completion(
                messages,
                temperature=0.4,
                max_tokens=2000,
                timeout=120.0,
            )

            if raw.startswith("[LLM Error") or raw.startswith("[LLM Timeout"):
                return self._fallback_opinion(symbol, f"LLM failure: {raw[:100]}")

            return self._parse_opinion(symbol, raw)

        except Exception as e:
            logger.error(f"[{self._name}] Analysis failed for {symbol}: {e}")
            return self._fallback_opinion(symbol, str(e))

    def _parse_opinion(self, symbol: str, raw_text: str) -> AgentOpinion:
        """Parse LLM JSON output into AgentOpinion, with robust fallback."""
        parsed = self._extract_json(raw_text)

        if parsed:
            stance_raw = str(parsed.get("stance", "HOLD")).upper().replace(" ", "_")
            # Validate stance
            valid_stances = {s.value for s in Stance}
            if stance_raw not in valid_stances:
                stance_raw = "HOLD"

            confidence = parsed.get("confidence", 0.5)
            if not isinstance(confidence, (int, float)):
                confidence = 0.5
            confidence = max(0.0, min(1.0, float(confidence)))

            return AgentOpinion(
                agent_name=self._name,
                agent_type=self._agent_type,
                symbol=symbol,
                stance=stance_raw,
                confidence=confidence,
                key_reasons=self._ensure_list(parsed.get("key_reasons", [])),
                risks=self._ensure_list(parsed.get("risks", [])),
                data_points=parsed.get("data_points", {}),
                analysis_text=str(parsed.get("analysis_text", raw_text[:1000])),
                veto=bool(parsed.get("veto", False)),
            )
        else:
            # Fallback: extract stance from text heuristically
            stance = self._infer_stance_from_text(raw_text)
            return AgentOpinion(
                agent_name=self._name,
                agent_type=self._agent_type,
                symbol=symbol,
                stance=stance,
                confidence=0.4,
                key_reasons=["(Auto-extracted from unstructured response)"],
                risks=[],
                data_points={},
                analysis_text=raw_text[:1500],
                veto=False,
            )

    def _extract_json(self, text: str) -> Optional[Dict[str, Any]]:
        """Robustly extract JSON from LLM output."""
        # Try markdown code block first
        code_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
        if code_match:
            try:
                return json.loads(code_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try to find outermost { ... }
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
        if json_match:
            candidate = json_match.group()
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                # Clean common LLM issues
                cleaned = re.sub(r'//[^\n]*', '', candidate)
                cleaned = re.sub(r',\s*([}\]])', r'\1', cleaned)
                try:
                    return json.loads(cleaned)
                except json.JSONDecodeError:
                    pass

        return None

    def _infer_stance_from_text(self, text: str) -> str:
        """Heuristic stance extraction from free text."""
        text_lower = text.lower()
        if any(w in text_lower for w in ["strong buy", "strong_buy", "强烈买入"]):
            return "STRONG_BUY"
        elif any(w in text_lower for w in ["avoid", "回避", "不建议"]):
            return "AVOID"
        elif any(w in text_lower for w in ["reject", "否决"]):
            return "REJECT"
        elif any(w in text_lower for w in ["buy", "买入", "建议买入"]):
            return "BUY"
        else:
            return "HOLD"

    def _fallback_opinion(self, symbol: str, error_msg: str) -> AgentOpinion:
        """Generate a neutral fallback opinion on error."""
        return AgentOpinion(
            agent_name=self._name,
            agent_type=self._agent_type,
            symbol=symbol,
            stance="HOLD",
            confidence=0.1,
            key_reasons=[f"Analysis unavailable: {error_msg[:200]}"],
            risks=["Unable to complete analysis"],
            data_points={},
            analysis_text=f"[{self._name}] Error: {error_msg[:500]}",
            veto=False,
        )

    @staticmethod
    def _ensure_list(val: Any) -> List[str]:
        """Ensure value is a list of strings."""
        if isinstance(val, list):
            return [str(v) for v in val[:5]]
        elif isinstance(val, str):
            return [val]
        return []

    # ── Shared prompt building blocks ──

    @staticmethod
    def json_output_instruction() -> str:
        """Standard JSON output instruction appended to all agent prompts."""
        return """

请严格按以下 JSON 格式输出你的分析（不要输出其他内容）：

```json
{
  "stance": "STRONG_BUY|BUY|HOLD|AVOID",
  "confidence": 0.0-1.0,
  "key_reasons": ["理由1", "理由2", "理由3"],
  "risks": ["风险1", "风险2", "风险3"],
  "data_points": {"指标名": "数值"},
  "analysis_text": "你的完整分析文本（300-800字）"
}
```"""
