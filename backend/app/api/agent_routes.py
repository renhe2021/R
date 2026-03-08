"""Agent API routes — Old Charlie chat, analyze, advisor, sessions, verdicts."""

import json
import logging
from typing import Optional, List

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.api.deps import get_agent_service
from app.agent.service import AgentService
from app.agent.llm import is_llm_available

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agent", tags=["agent"])


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=5000)
    sessionId: Optional[str] = None


class AdvisorChatRequest(BaseModel):
    message: str = Field(..., min_length=1, max_length=5000)
    sessionId: Optional[str] = None
    history: Optional[List[dict]] = None  # Optional client-side history


class AnalyzeRequest(BaseModel):
    stocks: list[str] = Field(..., min_length=1, max_length=100)
    sessionId: Optional[str] = None
    dataSource: Optional[str] = None


class PipelineRequest(BaseModel):
    """Request for the unified 8-stage pipeline."""
    stocks: list[str] = Field(default_factory=list, max_length=100)
    universe: Optional[str] = None  # Preset universe name
    strategy: str = Field(default="balanced")
    enableLlm: bool = Field(default=True)


async def _sse_generator(async_gen):
    """Convert async generator to SSE format."""
    try:
        async for event in async_gen:
            # Ensure each event has an 'event' key for frontend parsing
            if "type" in event and "event" not in event:
                event["event"] = event.pop("type")
            data = json.dumps(event, default=str, ensure_ascii=False)
            yield f"data: {data}\n\n"
    except Exception as e:
        logger.error(f"SSE stream error: {e}")
        yield f"data: {json.dumps({'event': 'error', 'message': str(e)[:500]})}\n\n"


@router.post("/advisor")
async def advisor_chat(
    req: AdvisorChatRequest,
    agent: AgentService = Depends(get_agent_service),
):
    """Streaming advisor chat with Old Charlie — LLM + tool-calling + book knowledge.

    This is the main chat endpoint for the advisor mode. Supports:
    - Multi-turn conversation with memory
    - Automatic tool calling (stock data, valuation, book search, etc.)
    - Streaming token-by-token response
    """
    gen = agent.chat_stream(
        message=req.message,
        session_id=req.sessionId,
        history=req.history,
    )
    return StreamingResponse(
        _sse_generator(gen),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/advisor/status")
async def advisor_status():
    """Check if the advisor LLM is available."""
    available = is_llm_available()
    return {
        "llmAvailable": available,
        "mode": "llm" if available else "not_configured",
    }


@router.get("/advisor/history/{session_id}")
async def get_advisor_history(
    session_id: str,
    agent: AgentService = Depends(get_agent_service),
):
    """Get conversation history for a session."""
    history = await agent.get_chat_history(session_id)
    return {"sessionId": session_id, "messages": history}


@router.post("/chat")
async def agent_chat(
    req: ChatRequest,
    agent: AgentService = Depends(get_agent_service),
):
    """Chat with Old Charlie (SSE stream) — legacy pipeline mode."""
    gen = agent.chat(message=req.message, session_id=req.sessionId)
    return StreamingResponse(
        _sse_generator(gen),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/analyze")
async def agent_analyze(
    req: AnalyzeRequest,
    agent: AgentService = Depends(get_agent_service),
):
    """Analyze a list of stocks through Old Charlie's three-stage SOP (SSE stream)."""
    stocks = [s.upper().strip() for s in req.stocks if s.strip()]
    if not stocks:
        raise HTTPException(status_code=422, detail="No valid stock symbols provided")

    gen = agent.analyze(stocks=stocks, session_id=req.sessionId, data_source=req.dataSource)
    return StreamingResponse(
        _sse_generator(gen),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.post("/pipeline")
async def agent_pipeline(req: PipelineRequest):
    """Run the unified 8-stage R-System pipeline (SSE stream).

    This is the main analysis endpoint — replaces the legacy /analyze endpoint.
    Supports: single/multi stock analysis, preset universes, strategy templates.
    """
    from app.agent.unified_pipeline import UnifiedPipeline

    stocks = [s.upper().strip() for s in req.stocks if s.strip()]
    if not stocks and not req.universe:
        raise HTTPException(status_code=422, detail="Provide stocks or a universe name")

    pipeline = UnifiedPipeline()

    async def _run():
        try:
            async for event in pipeline.run(
                symbols=stocks,
                strategy=req.strategy,
                universe=req.universe,
                enable_llm=req.enableLlm,
            ):
                data = json.dumps(event, default=str, ensure_ascii=False)
                yield f"data: {data}\n\n"
        except Exception as e:
            logger.error(f"Pipeline SSE error: {e}", exc_info=True)
            error_event = json.dumps({
                "event": "error",
                "stage": "pipeline",
                "message": f"管线执行出错: {str(e)[:500]}",
            }, ensure_ascii=False)
            yield f"data: {error_event}\n\n"

    return StreamingResponse(
        _run(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/pipeline/strategies")
async def get_strategies():
    """Get available strategy templates and preset universes."""
    from app.agent.unified_pipeline import STRATEGY_TEMPLATES, PRESET_UNIVERSES
    return {
        "strategies": {k: {"name": v.get("name", k)} for k, v in STRATEGY_TEMPLATES.items()},
        "universes": {k: {"count": len(v)} for k, v in PRESET_UNIVERSES.items()},
    }


@router.get("/sessions")
async def get_sessions(
    limit: int = Query(20, ge=1, le=100),
    agent: AgentService = Depends(get_agent_service),
):
    """Get recent agent sessions."""
    sessions = await agent.get_sessions(limit=limit)
    return {"sessions": sessions}


@router.get("/verdicts")
async def get_verdicts(
    limit: int = Query(20, ge=1, le=100),
    agent: AgentService = Depends(get_agent_service),
):
    """Get recent verdicts."""
    verdicts = await agent.get_verdicts(limit=limit)
    return {"verdicts": verdicts}


@router.get("/verdicts/{run_id}")
async def get_verdict(
    run_id: str,
    agent: AgentService = Depends(get_agent_service),
):
    """Get a specific verdict by run_id."""
    verdict = await agent.get_verdict(run_id)
    if not verdict:
        raise HTTPException(status_code=404, detail=f"Verdict {run_id} not found")
    return verdict
