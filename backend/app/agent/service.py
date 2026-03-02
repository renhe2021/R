"""AgentService — Public API for Old Charlie agent.

Pure Python implementation, no LangGraph/LangChain dependency.
Exposes: chat(), chat_stream(), analyze(), get_verdicts(), get_sessions()
"""

import json
import logging
import uuid
from typing import AsyncGenerator, Dict, Any, List, Optional

from app.agent.state import AgentState, AgentMessage
from app.agent.graph import run_pipeline
from app.agent.llm import is_llm_available
from app.agent.persona import CHARLIE_SYSTEM_PROMPT
from app.database import SessionLocal

logger = logging.getLogger(__name__)


class AgentService:
    """Old Charlie Agent Service — singleton managed by deps.py.

    Two chat modes:
    - chat(): Legacy pipeline-based chat (routes to analyze)
    - chat_stream(): Streaming advisor chat with LLM + tool-calling + conversation history
    """

    def __init__(self):
        # In-memory conversation store: session_id -> list of messages
        self._conversations: Dict[str, List[Dict[str, str]]] = {}
        # Keep at most this many sessions in memory
        self._max_sessions = 50

    def _get_conversation(self, session_id: str) -> List[Dict[str, str]]:
        """Get or create conversation history for a session."""
        if session_id not in self._conversations:
            self._conversations[session_id] = [
                {"role": "system", "content": CHARLIE_SYSTEM_PROMPT}
            ]
            # Evict oldest sessions if too many
            if len(self._conversations) > self._max_sessions:
                oldest = next(iter(self._conversations))
                del self._conversations[oldest]
        return self._conversations[session_id]

    async def chat_stream(
        self,
        message: str,
        session_id: Optional[str] = None,
        history: Optional[List[Dict[str, str]]] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Streaming advisor chat — LLM with tool-calling and conversation memory.

        Yields SSE events:
        - {"event": "session", "sessionId": "..."}
        - {"event": "tool_call", "name": "...", "arguments": {...}}
        - {"event": "tool_result", "name": "...", "result": "..."}
        - {"event": "token", "content": "..."}
        - {"event": "done"}
        """
        from app.agent.llm import chat_completion_stream

        if not session_id:
            session_id = str(uuid.uuid4())[:8]

        yield {"event": "session", "sessionId": session_id}

        try:
            # Build conversation
            if history is not None:
                # Client sent full history — use it (with system prompt prepended)
                conv = [{"role": "system", "content": CHARLIE_SYSTEM_PROMPT}]
                for h in history:
                    if h.get("role") in ("user", "assistant") and h.get("content"):
                        conv.append({"role": h["role"], "content": h["content"]})
                conv.append({"role": "user", "content": message})
            else:
                # Use server-side conversation memory
                conv = self._get_conversation(session_id)
                conv.append({"role": "user", "content": message})

            # Stream LLM response
            full_response = ""
            async for event in chat_completion_stream(conv):
                evt_type = event.get("type", "")
                if evt_type == "token":
                    full_response += event["content"]
                    yield {"event": "token", "content": event["content"]}
                elif evt_type == "tool_call":
                    yield {
                        "event": "tool_call",
                        "name": event["name"],
                        "arguments": event.get("arguments", {}),
                    }
                elif evt_type == "tool_result":
                    yield {
                        "event": "tool_result",
                        "name": event["name"],
                        "result": event.get("result", ""),
                    }
                elif evt_type == "done":
                    # Save assistant response to conversation history
                    if full_response:
                        if history is None:
                            conv.append({"role": "assistant", "content": full_response})
                    yield {"event": "done"}

            # Persist session
            self._save_session(session_id, title=f"Chat: {message[:30]}...", mode="chat")
            self._save_message(session_id, "human", message)
            if full_response:
                self._save_message(session_id, "ai", full_response)

        except Exception as e:
            logger.error(f"Agent chat_stream error: {e}", exc_info=True)
            yield {"event": "error", "message": str(e)[:500]}

    async def chat(self, message: str, session_id: Optional[str] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Legacy chat mode — routes through pipeline."""
        if not session_id:
            session_id = str(uuid.uuid4())[:8]

        yield {"event": "session", "sessionId": session_id}

        try:
            state: AgentState = {
                "messages": [AgentMessage(role="user", content=message)],
                "session_id": session_id,
                "mode": "chat",
                "input_stocks": [],
                "screening_results": None,
                "interrogation_results": None,
                "appraisal_results": None,
                "final_report": None,
                "sink_results": None,
            }

            async for event in run_pipeline(state):
                yield event

            # Save session
            self._save_session(session_id, title=f"Chat: {message[:30]}...", mode="chat")

        except Exception as e:
            logger.error(f"Agent chat error: {e}", exc_info=True)
            yield {"event": "error", "message": str(e)[:500]}

    async def analyze(self, stocks: List[str], session_id: Optional[str] = None, data_source: Optional[str] = None) -> AsyncGenerator[Dict[str, Any], None]:
        """Analyze mode — Run the three-stage SOP on a list of stocks."""
        if not session_id:
            session_id = str(uuid.uuid4())[:8]

        yield {"event": "session", "sessionId": session_id}

        try:
            state: AgentState = {
                "messages": [AgentMessage(role="user", content=f"Analyze: {', '.join(stocks)}")],
                "session_id": session_id,
                "mode": "analyze",
                "input_stocks": stocks,
                "data_source": data_source,
                "screening_results": None,
                "interrogation_results": None,
                "appraisal_results": None,
                "final_report": None,
                "sink_results": None,
            }

            async for event in run_pipeline(state):
                yield event

            # Save session
            self._save_session(
                session_id,
                title=f"Analysis: {', '.join(stocks[:5])}",
                mode="analyze",
                stocks=stocks,
            )

        except Exception as e:
            logger.error(f"Agent analyze error: {e}", exc_info=True)
            yield {"event": "error", "message": str(e)[:500]}

    def _save_session(self, session_id: str, title: str = "", mode: str = "chat",
                      stocks: Optional[List[str]] = None):
        """Persist session to DB (upsert — skip if already exists)."""
        db = SessionLocal()
        try:
            from app.models.agent import AgentSession
            existing = db.query(AgentSession).filter(
                AgentSession.session_id == session_id
            ).first()
            if not existing:
                session = AgentSession(
                    session_id=session_id,
                    title=title,
                    mode=mode,
                    stock_symbols=json.dumps(stocks) if stocks else None,
                )
                db.add(session)
                db.commit()
        except Exception as e:
            db.rollback()
            logger.warning(f"Failed to save session: {e}")
        finally:
            db.close()

    def _save_message(self, session_id: str, role: str, content: str):
        """Persist a chat message to DB."""
        db = SessionLocal()
        try:
            from app.models.agent import AgentMessage as DBAgentMessage
            msg = DBAgentMessage(
                session_id=session_id,
                role=role,
                content=content[:10000],
            )
            db.add(msg)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.warning(f"Failed to save message: {e}")
        finally:
            db.close()

    async def get_chat_history(self, session_id: str) -> List[Dict[str, str]]:
        """Get conversation history for a session."""
        db = SessionLocal()
        try:
            from app.models.agent import AgentMessage as DBAgentMessage
            messages = db.query(DBAgentMessage).filter(
                DBAgentMessage.session_id == session_id
            ).order_by(DBAgentMessage.created_at.asc()).all()
            return [
                {
                    "role": "user" if m.role == "human" else "assistant",
                    "content": m.content,
                }
                for m in messages
                if m.role in ("human", "ai")
            ]
        finally:
            db.close()

    async def get_sessions(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent agent sessions."""
        db = SessionLocal()
        try:
            from app.models.agent import AgentSession
            sessions = db.query(AgentSession).order_by(
                AgentSession.created_at.desc()
            ).limit(limit).all()
            return [
                {
                    "sessionId": s.session_id,
                    "title": s.title,
                    "mode": s.mode,
                    "createdAt": s.created_at.isoformat() if s.created_at else None,
                }
                for s in sessions
            ]
        finally:
            db.close()

    async def get_verdicts(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent verdicts."""
        db = SessionLocal()
        try:
            from app.models.agent import AgentVerdict
            verdicts = db.query(AgentVerdict).order_by(
                AgentVerdict.created_at.desc()
            ).limit(limit).all()
            return [
                {
                    "runId": v.run_id,
                    "inputStocks": json.loads(v.input_stocks) if v.input_stocks else [],
                    "finalPicksCount": v.final_picks_count,
                    "redFlagsCount": v.red_flags_count,
                    "status": v.status,
                    "charlieSummary": v.charlie_summary[:500] if v.charlie_summary else "",
                    "createdAt": v.created_at.isoformat() if v.created_at else None,
                }
                for v in verdicts
            ]
        finally:
            db.close()

    async def get_verdict(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific verdict by run_id."""
        db = SessionLocal()
        try:
            from app.models.agent import AgentVerdict
            v = db.query(AgentVerdict).filter(AgentVerdict.run_id == run_id).first()
            if not v:
                return None
            return {
                "runId": v.run_id,
                "verdictJson": json.loads(v.verdict_json) if v.verdict_json else {},
                "inputStocks": json.loads(v.input_stocks) if v.input_stocks else [],
                "finalPicksCount": v.final_picks_count,
                "redFlagsCount": v.red_flags_count,
                "status": v.status,
                "charlieSummary": v.charlie_summary,
                "createdAt": v.created_at.isoformat() if v.created_at else None,
            }
        finally:
            db.close()
