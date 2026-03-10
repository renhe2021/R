"""Investment Committee Debate Engine.

Orchestrates the 2-round structured debate:
  Round 1: All agents analyze independently in parallel
  Round 2: Portfolio Manager synthesizes opinions, simulates debate,
           checks veto, and delivers final verdict

Yields SSE-compatible events for real-time monitoring.
"""

import asyncio
import json
import logging
from typing import Any, AsyncGenerator, Callable, Dict, List, Optional

from app.agent.committee.models import (
    AgentOpinion, DebateRecord, Stance, VetoDecision,
    STANCE_WEIGHTS,
)
from app.agent.committee.base_agent import InvestmentAgent
from app.agent.committee.school_agents import create_all_school_agents
from app.agent.committee.role_agents import (
    create_all_role_agents, RiskManagerAgent,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  Portfolio Manager debate prompt
# ═══════════════════════════════════════════════════════════════

_PM_SYSTEM = """你是投资委员会主席（Portfolio Manager / Chief Investment Officer）。

你的职责：
1. 综合所有分析师（7个流派 + 5个角色）的独立意见
2. 识别关键分歧点和争议焦点
3. 模拟投委会质询：对持看多意见的分析师提出反面质疑，对持看空意见的分析师提出正面反驳
4. 形成最终裁决（STRONG_BUY / BUY / HOLD / AVOID）

你的决策原则：
- 多数投票权重大，但不是简单的人头数——高确信度意见权重更高
- Risk Manager 的风险警告必须认真对待
- 如果分歧极大（如 60/40 分裂），最终裁决应偏保守（HOLD 而非 BUY）
- 辩论记录必须完整，便于事后追溯

"投资不是关于买好东西，而是关于买得好。" — Howard Marks"""


def _build_pm_debate_prompt(
    symbol: str,
    stock_name: str,
    opinions: List[AgentOpinion],
    veto: VetoDecision,
) -> str:
    """Build the Portfolio Manager's debate synthesis prompt."""
    # Group opinions
    school_opinions = [o for o in opinions if o.agent_type == "school"]
    role_opinions = [o for o in opinions if o.agent_type == "role"]

    # Format opinions
    lines = [f"## 投委会分析汇总 — {symbol} ({stock_name})\n"]

    lines.append("### 一、七流派独立意见\n")
    for op in school_opinions:
        lines.append(f"**{op.agent_name}**: {op.stance} (置信度 {op.confidence:.0%})")
        for r in op.key_reasons[:2]:
            lines.append(f"  - {r}")
        if op.risks:
            lines.append(f"  ⚠ 主要风险: {op.risks[0]}")
        lines.append("")

    lines.append("### 二、五角色独立意见\n")
    for op in role_opinions:
        veto_tag = " [⚡否决]" if op.veto else ""
        lines.append(f"**{op.agent_name}**: {op.stance} (置信度 {op.confidence:.0%}){veto_tag}")
        for r in op.key_reasons[:2]:
            lines.append(f"  - {r}")
        if op.risks:
            lines.append(f"  ⚠ 主要风险: {op.risks[0]}")
        lines.append("")

    # Vote tally
    tally: Dict[str, int] = {}
    for op in opinions:
        tally[op.stance] = tally.get(op.stance, 0) + 1
    lines.append(f"### 三、投票统计: {dict(tally)}\n")

    # Veto status
    if veto.triggered:
        lines.append(f"### ⚡ Risk Manager 否决已触发\n理由: {veto.reason}\n量化触发: {', '.join(veto.quantitative_triggers)}\n")
    else:
        lines.append("### Risk Manager 否决未触发\n")

    opinion_text = "\n".join(lines)

    return f"""{opinion_text}

---

作为投委会主席，请完成以下工作：

## 1. 辩论纪要
模拟投委会质询过程：
- 对看多方提出最尖锐的反驳（"为什么你忽视了..."）
- 对看空方提出最有力的质疑（"但数据显示..."）
- 记录关键分歧点

## 2. 最终裁决
综合所有意见，给出你的最终判断。

{"⚠️ 注意：Risk Manager 已触发否决，你必须将最终 stance 设为 AVOID 或更低。" if veto.triggered else ""}

请严格按以下 JSON 格式输出：

```json
{{
  "debate_summary": "投委会辩论纪要（300-600字，记录关键质询和分歧）",
  "dissent_points": ["分歧点1", "分歧点2", "分歧点3"],
  "final_verdict": "STRONG_BUY|BUY|HOLD|AVOID",
  "final_confidence": 0.0-1.0,
  "final_reasoning": "最终裁决理由（200-400字）",
  "portfolio_manager_summary": "给投资者的一句话总结"
}}
```"""


# ═══════════════════════════════════════════════════════════════
#  Debate Engine
# ═══════════════════════════════════════════════════════════════

class DebateEngine:
    """Orchestrates the full investment committee debate for one stock.

    Usage:
        engine = DebateEngine()
        async for event in engine.run_debate(symbol, stock_name, snapshot, stock_result):
            # event is a dict suitable for SSE streaming
            yield event
    """

    def __init__(self):
        self.school_agents = create_all_school_agents()
        self.role_agents = create_all_role_agents()
        self._risk_manager: Optional[RiskManagerAgent] = None
        for agent in self.role_agents:
            if isinstance(agent, RiskManagerAgent):
                self._risk_manager = agent
                break

    async def run_debate(
        self,
        symbol: str,
        stock_name: str,
        stock_snapshot: str,
        z_score: float | None = None,
        m_score: float | None = None,
        f_score: int | None = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Run full debate, yielding SSE events for real-time monitoring.

        Yields events:
        - debate_start
        - agent_opinion (one per agent, as they complete)
        - veto_check
        - debate_round_2
        - final_verdict
        - debate_end

        Returns the final DebateRecord via the last event.
        """
        record = DebateRecord(symbol=symbol, stock_name=stock_name)

        # ── Debate Start ──
        yield {
            "type": "debate_start",
            "symbol": symbol,
            "stock_name": stock_name,
            "total_agents": len(self.school_agents) + len(self.role_agents),
            "message": f"投委会开始分析 {symbol} ({stock_name})...",
        }

        # ── Round 1: All agents analyze with controlled concurrency ──
        all_agents: List[InvestmentAgent] = self.school_agents + self.role_agents
        opinions: List[AgentOpinion] = []

        # Limit agent-level concurrency per stock to avoid rate limit storms
        agent_semaphore = asyncio.Semaphore(3)  # Max 3 agents per stock at once

        async def _run_agent(agent: InvestmentAgent) -> AgentOpinion:
            async with agent_semaphore:
                return await agent.analyze(symbol, stock_snapshot)

        # Run agents concurrently (semaphore-limited), yield each result as it completes
        tasks = {
            asyncio.create_task(_run_agent(agent)): agent
            for agent in all_agents
        }

        for coro in asyncio.as_completed(tasks.keys()):
            try:
                opinion = await coro
                opinions.append(opinion)

                yield {
                    "type": "agent_opinion",
                    "symbol": symbol,
                    "agent_name": opinion.agent_name,
                    "agent_type": opinion.agent_type,
                    "stance": opinion.stance,
                    "confidence": opinion.confidence,
                    "key_reasons": opinion.key_reasons[:3],
                    "risks": opinion.risks[:2],
                    "veto": opinion.veto,
                    "message": (
                        f"[{opinion.agent_name}] {opinion.stance} "
                        f"(置信度 {opinion.confidence:.0%})"
                    ),
                }

            except Exception as e:
                logger.error(f"Agent task failed: {e}")

        record.round1_opinions = opinions

        # Build vote tally
        for op in opinions:
            record.votes[op.agent_name] = op.stance
            record.vote_tally[op.stance] = record.vote_tally.get(op.stance, 0) + 1

        # ── Veto Check ──
        # 1. Quantitative (rule-based, non-negotiable)
        veto = VetoDecision()
        if self._risk_manager:
            veto = self._risk_manager.check_quantitative_veto(z_score, m_score, f_score)

        # 2. LLM-based Risk Manager opinion
        risk_opinion = next(
            (o for o in opinions if o.agent_name == "Risk Manager"), None
        )
        if risk_opinion and risk_opinion.veto and not veto.triggered:
            veto = VetoDecision(
                triggered=True,
                reason=f"Risk Manager LLM判断否决: {risk_opinion.key_reasons[0] if risk_opinion.key_reasons else 'N/A'}",
                quantitative_triggers=[],
            )

        record.veto = veto

        yield {
            "type": "veto_check",
            "symbol": symbol,
            "triggered": veto.triggered,
            "reason": veto.reason,
            "quantitative_triggers": veto.quantitative_triggers,
            "message": (
                f"⚡ 否决触发: {veto.reason}" if veto.triggered
                else f"✓ 否决未触发: {veto.reason}"
            ),
        }

        # ── Round 2: PM debate synthesis ──
        yield {
            "type": "debate_round_2",
            "symbol": symbol,
            "message": "Portfolio Manager 综合所有意见，模拟投委会辩论...",
        }

        pm_result = await self._run_pm_synthesis(
            symbol, stock_name, opinions, veto
        )

        record.debate_summary = pm_result.get("debate_summary", "")
        record.dissent_points = pm_result.get("dissent_points", [])
        record.final_verdict = pm_result.get("final_verdict", "HOLD")
        record.final_confidence = pm_result.get("final_confidence", 0.5)
        record.final_reasoning = pm_result.get("final_reasoning", "")
        record.portfolio_manager_summary = pm_result.get("portfolio_manager_summary", "")

        # If veto triggered, override verdict
        if veto.triggered:
            if record.final_verdict in ("STRONG_BUY", "BUY"):
                record.final_verdict = "AVOID"
                record.final_reasoning = (
                    f"[否决覆盖] 原裁决为 {pm_result.get('final_verdict', 'N/A')}，"
                    f"但 Risk Manager 否决已触发: {veto.reason}。"
                    f"最终裁决降级为 AVOID。\n\n"
                    f"原始理由: {record.final_reasoning}"
                )

        yield {
            "type": "final_verdict",
            "symbol": symbol,
            "verdict": record.final_verdict,
            "confidence": record.final_confidence,
            "reasoning": record.final_reasoning,
            "portfolio_manager_summary": record.portfolio_manager_summary,
            "vote_tally": record.vote_tally,
            "consensus_ratio": record.consensus_ratio,
            "veto_triggered": veto.triggered,
            "message": (
                f"最终裁决: {record.final_verdict} "
                f"(置信度 {record.final_confidence:.0%}, "
                f"共识度 {record.consensus_ratio:.0%})"
            ),
        }

        # ── Debate End ──
        yield {
            "type": "debate_end",
            "symbol": symbol,
            "debate_record": record.to_dict(),
            "message": f"投委会分析完成: {symbol} → {record.final_verdict}",
        }

    async def _run_pm_synthesis(
        self,
        symbol: str,
        stock_name: str,
        opinions: List[AgentOpinion],
        veto: VetoDecision,
    ) -> Dict[str, Any]:
        """Run Portfolio Manager's debate synthesis via LLM."""
        from app.agent.llm import simple_completion

        prompt = _build_pm_debate_prompt(symbol, stock_name, opinions, veto)

        messages = [
            {"role": "system", "content": _PM_SYSTEM},
            {"role": "user", "content": prompt},
        ]

        try:
            raw = await simple_completion(
                messages,
                temperature=0.3,
                max_tokens=3000,
                timeout=120.0,
            )

            if raw.startswith("[LLM Error") or raw.startswith("[LLM Timeout"):
                return self._fallback_pm_result(opinions, veto)

            parsed = self._extract_json(raw)
            if parsed:
                # Validate verdict
                valid_stances = {s.value for s in Stance}
                verdict = str(parsed.get("final_verdict", "HOLD")).upper()
                if verdict not in valid_stances:
                    verdict = "HOLD"
                parsed["final_verdict"] = verdict

                confidence = parsed.get("final_confidence", 0.5)
                if not isinstance(confidence, (int, float)):
                    confidence = 0.5
                parsed["final_confidence"] = max(0.0, min(1.0, float(confidence)))

                return parsed
            else:
                return self._fallback_pm_result(opinions, veto, raw_text=raw)

        except Exception as e:
            logger.error(f"PM synthesis failed for {symbol}: {e}")
            return self._fallback_pm_result(opinions, veto)

    def _fallback_pm_result(
        self,
        opinions: List[AgentOpinion],
        veto: VetoDecision,
        raw_text: str = "",
    ) -> Dict[str, Any]:
        """Compute verdict from vote tallying when LLM fails."""
        if veto.triggered:
            return {
                "debate_summary": f"PM synthesis failed. Veto active: {veto.reason}",
                "dissent_points": [],
                "final_verdict": "AVOID",
                "final_confidence": 0.3,
                "final_reasoning": f"Risk Manager veto: {veto.reason}",
                "portfolio_manager_summary": f"否决: {veto.reason}",
            }

        # Weighted vote
        total_weight = 0.0
        for op in opinions:
            w = STANCE_WEIGHTS.get(op.stance, 0.4) * op.confidence
            total_weight += w

        avg_weight = total_weight / max(len(opinions), 1)

        if avg_weight >= 0.7:
            verdict = "STRONG_BUY"
        elif avg_weight >= 0.55:
            verdict = "BUY"
        elif avg_weight >= 0.35:
            verdict = "HOLD"
        else:
            verdict = "AVOID"

        return {
            "debate_summary": raw_text[:500] if raw_text else "PM synthesis unavailable (LLM error)",
            "dissent_points": [],
            "final_verdict": verdict,
            "final_confidence": round(avg_weight, 2),
            "final_reasoning": f"Fallback: weighted average stance = {avg_weight:.2f}",
            "portfolio_manager_summary": f"自动裁决: {verdict} (加权 {avg_weight:.2f})",
        }

    @staticmethod
    def _extract_json(text: str) -> Optional[Dict[str, Any]]:
        """Extract JSON from LLM output."""
        import re
        # Try markdown code block
        code_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
        if code_match:
            try:
                return json.loads(code_match.group(1))
            except json.JSONDecodeError:
                pass

        # Try outermost { ... }
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
        if json_match:
            candidate = json_match.group()
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                import re as _re
                cleaned = _re.sub(r'//[^\n]*', '', candidate)
                cleaned = _re.sub(r',\s*([}\]])', r'\1', cleaned)
                try:
                    return json.loads(cleaned)
                except json.JSONDecodeError:
                    pass

        return None
