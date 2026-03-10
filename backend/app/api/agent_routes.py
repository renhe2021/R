"""Agent API routes — Old Charlie chat, analyze, advisor, sessions, verdicts, backtest."""

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
    dataSource: Optional[str] = Field(
        default=None,
        description="Data source override. None=Bloomberg first (ask user if unavailable). "
                    "'bloomberg'/'yfinance'/'fmp'/'finnhub'/'yahoo_direct'=use directly.",
    )
    resumeFrom: Optional[int] = Field(
        default=None,
        description="Stage number to resume from (e.g. 3 to skip data checkpoint). "
                    "Requires resumeRunId to restore cached state.",
    )
    resumeRunId: Optional[str] = Field(
        default=None,
        description="The run_id from the paused run, used to look up cached pipeline state.",
    )


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
                data_source=req.dataSource,
                resume_from=req.resumeFrom,
                resume_run_id=req.resumeRunId,
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


@router.get("/pipeline/data-sources")
async def probe_data_sources():
    """Probe available data sources (Bloomberg first).

    Returns Bloomberg availability and list of alternative data sources.
    Frontend can use this to pre-check before starting the pipeline.
    """
    import asyncio

    def _probe():
        from src.data_providers.factory import probe_bloomberg_first
        return probe_bloomberg_first()

    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, _probe)

    source_labels = {
        "bloomberg": "Bloomberg Terminal — 最高质量机构级数据",
        "yfinance": "Yahoo Finance (yfinance) — 免费，覆盖面广",
        "fmp": "Financial Modeling Prep (FMP) — 需API Key，数据质量高",
        "finnhub": "Finnhub — 需API Key，实时数据",
        "yahoo_direct": "Yahoo Direct — 免费备选",
    }

    sources = []
    if result.bloomberg_available:
        sources.append({"id": "bloomberg", "label": source_labels["bloomberg"], "available": True, "primary": True})

    for alt in result.available_alternatives:
        sources.append({"id": alt, "label": source_labels.get(alt, alt), "available": True, "primary": False})

    return {
        "bloombergAvailable": result.bloomberg_available,
        "bloombergError": result.error_message if not result.bloomberg_available else None,
        "sources": sources,
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


# ═══════════════════════════════════════════════════════════════
#  Investment Parameters — View / Override / Reset / Audit
# ═══════════════════════════════════════════════════════════════


class ParamOverrideRequest(BaseModel):
    """Request to override one or more parameters."""
    overrides: dict = Field(..., description="Dict of param_key -> new_value")
    reason: str = Field(default="", description="Reason for the override")


class ParamResetRequest(BaseModel):
    """Request to reset one or more parameters."""
    keys: List[str] = Field(default_factory=list, description="Keys to reset; empty = reset all")


@router.get("/params")
async def get_all_params(
    school: Optional[str] = Query(None, description="Filter by school"),
    category: Optional[str] = Query(None, description="Filter by category"),
):
    """Get all investment parameters, optionally filtered by school or category.

    Returns the full registry including current value, default, description,
    valid range, and whether the value has been overridden.
    """
    from app.agent.investment_params import params

    if school:
        items = params.list_by_school(school)
    elif category:
        items = params.list_by_category(category)
    else:
        items = params.list_all()

    return {
        "parameters": items,
        "summary": params.summary(),
    }


@router.get("/params/overridden")
async def get_overridden_params():
    """Get only parameters that differ from their defaults."""
    from app.agent.investment_params import params
    return {
        "overridden": params.list_overridden(),
        "count": len(params.list_overridden()),
    }


@router.get("/params/schools")
async def get_param_schools():
    """Get available school names and their parameter counts."""
    from app.agent.investment_params import params
    schools = params.get_schools()
    return {
        "schools": [
            {"name": s, "parameters": params.list_by_school(s)}
            for s in schools
        ],
    }


@router.get("/params/audit")
async def get_param_audit(limit: int = Query(50, ge=1, le=500)):
    """Get the parameter change audit trail."""
    from app.agent.investment_params import params
    return {
        "changes": params.get_change_log(limit),
    }


@router.post("/params/override")
async def override_params(req: ParamOverrideRequest):
    """Override one or more investment parameters at runtime.

    Changes take effect immediately for the next pipeline run.
    All changes are logged in the audit trail.

    Example body:
    {
        "overrides": {"graham.pe_max": 18, "risk.z_score_danger": 1.50},
        "reason": "当前市场PE偏高，放宽Graham PE上限"
    }
    """
    from app.agent.investment_params import params

    results = params.batch_override(req.overrides, req.reason)

    succeeded = {k: v for k, v in results.items() if v}
    failed = {k: v for k, v in results.items() if not v}

    if failed:
        # Return partial success with details
        return {
            "status": "partial" if succeeded else "failed",
            "succeeded": list(succeeded.keys()),
            "failed": list(failed.keys()),
            "message": f"{len(succeeded)} 个参数修改成功, {len(failed)} 个失败 (参数不存在或值超出范围)",
        }

    return {
        "status": "success",
        "succeeded": list(succeeded.keys()),
        "message": f"已成功修改 {len(succeeded)} 个参数",
    }


@router.post("/params/reset")
async def reset_params(req: ParamResetRequest):
    """Reset parameters to their default values.

    If keys is empty, resets ALL parameters.
    """
    from app.agent.investment_params import params

    if not req.keys:
        params.reset_all()
        return {"status": "success", "message": "所有参数已重置为默认值"}

    results = {}
    for key in req.keys:
        results[key] = params.reset(key)

    return {
        "status": "success",
        "reset": [k for k, v in results.items() if v],
        "not_found": [k for k, v in results.items() if not v],
    }


@router.post("/params/reload")
async def reload_params_yaml():
    """Reload parameters from investment_params.yaml.

    Use after editing the YAML file — no restart needed.
    """
    from app.agent.investment_params import params
    params.reload_yaml()
    overridden = params.list_overridden()
    return {
        "status": "success",
        "message": f"YAML已重新加载, {len(overridden)} 个参数被覆盖",
        "overridden": overridden,
    }


@router.get("/params/export")
async def export_params_yaml():
    """Export current non-default parameters as YAML string.

    Can be saved to investment_params.yaml for persistence.
    """
    from app.agent.investment_params import params
    return {
        "yaml": params.export_yaml(),
        "overridden_count": len(params.list_overridden()),
    }


@router.get("/params/{key}")
async def get_single_param(key: str):
    """Get a single parameter by its dotted key (e.g., 'graham.pe_max')."""
    from app.agent.investment_params import params
    p = params.get_def(key)
    if not p:
        raise HTTPException(status_code=404, detail=f"Parameter '{key}' not found")
    return p.to_dict()


@router.get("/debates/{run_id}")
async def get_debate_records(run_id: str):
    """Get committee debate records for a specific pipeline run.

    Returns the full debate record including all agent opinions,
    vote tally, veto decisions, and PM final verdict for each stock.
    """
    from app.database import get_session
    from app.models.agent import AgentVerdict

    async with get_session() as session:
        from sqlalchemy import select
        result = await session.execute(
            select(AgentVerdict).where(AgentVerdict.run_id == run_id)
        )
        row = result.scalars().first()

    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    debate_data = None
    if row.debate_json:
        try:
            debate_data = json.loads(row.debate_json)
        except json.JSONDecodeError:
            debate_data = {"raw": row.debate_json}

    backtest_data = None
    if row.strategy_backtest:
        try:
            backtest_data = json.loads(row.strategy_backtest)
        except json.JSONDecodeError:
            backtest_data = None

    return {
        "runId": run_id,
        "debates": debate_data,
        "strategyBacktest": backtest_data,
        "committeeVotes": json.loads(row.committee_votes) if row.committee_votes else None,
    }


# ═══════════════════════════════════════════════════════════════
#  Point-in-Time Backtest API
# ═══════════════════════════════════════════════════════════════


class BacktestRequest(BaseModel):
    """Request body for launching a PIT backtest."""
    stocks: list[str] = Field(default_factory=list, max_length=100)
    universe: Optional[str] = None
    holdingMonths: int = Field(default=6, ge=1, le=24)
    lookbackYears: float = Field(default=3.0, ge=1.0, le=5.0)
    commissionRate: float = Field(default=0.001, ge=0, le=0.01)
    slippageRate: float = Field(default=0.0005, ge=0, le=0.005)
    stopLossPct: float = Field(default=0.15, ge=0.05, le=0.40)
    maxHoldings: int = Field(default=15, ge=3, le=50)
    initialCapital: float = Field(default=1_000_000, ge=10_000, le=100_000_000)
    weighting: str = Field(default="equal")
    benchmark: str = Field(default="SPY")
    strategy: str = Field(default="balanced")
    dataSource: Optional[str] = Field(
        default=None,
        description="Data source for backtest. None=yfinance (backtest needs historical quarterly data). "
                    "Backtest currently only supports yfinance for historical financials.",
    )


@router.post("/backtest/run")
async def run_backtest(req: BacktestRequest):
    """Launch a Point-in-Time backtest (SSE stream).

    Returns real-time progress events:
      - backtest_start / backtest_phase / backtest_progress / backtest_complete / backtest_error

    The full result is also persisted to the database for later retrieval.
    """
    from app.agent.backtest.pit_backtester import PointInTimeBacktester
    from app.agent.backtest.models import PITBacktestConfig

    # Resolve symbols
    stocks = [s.upper().strip() for s in req.stocks if s.strip()]
    if not stocks and req.universe:
        from app.agent.unified_pipeline import PRESET_UNIVERSES
        stocks = PRESET_UNIVERSES.get(req.universe, [])

    if not stocks:
        raise HTTPException(status_code=422, detail="Provide stocks or a universe name")

    config = PITBacktestConfig(
        symbols=stocks,
        holding_months=req.holdingMonths,
        lookback_years=req.lookbackYears,
        commission_rate=req.commissionRate,
        slippage_rate=req.slippageRate,
        stop_loss_pct=req.stopLossPct,
        max_holdings=req.maxHoldings,
        initial_capital=req.initialCapital,
        weighting=req.weighting,
        benchmark=req.benchmark,
        strategy=req.strategy,
    )

    backtester = PointInTimeBacktester(config)

    async def _stream():
        try:
            async for event in backtester.run():
                if "type" in event and "event" not in event:
                    event["event"] = event.pop("type")
                data = json.dumps(event, default=str, ensure_ascii=False)
                yield f"data: {data}\n\n"

            # Persist result
            result = backtester.get_result()
            if result:
                _persist_backtest(result)

        except Exception as e:
            logger.error(f"Backtest SSE error: {e}", exc_info=True)
            yield f"data: {json.dumps({'event': 'backtest_error', 'message': str(e)[:500]})}\n\n"

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/backtest/results")
async def list_backtest_results(
    limit: int = Query(20, ge=1, le=100),
    strategy: Optional[str] = Query(None),
):
    """List recent backtest runs."""
    from app.database import SessionLocal
    from app.models.agent import BacktestRun
    from sqlalchemy import select

    db = SessionLocal()
    try:
        q = select(BacktestRun).order_by(BacktestRun.created_at.desc())
        if strategy:
            q = q.where(BacktestRun.strategy == strategy)
        q = q.limit(limit)
        rows = db.execute(q).scalars().all()

        return {
            "results": [
                {
                    "run_id": r.run_id,
                    "status": r.status,
                    "verdict": r.verdict,
                    "strategy": r.strategy,
                    "symbols": r.symbols_csv,
                    "total_return": r.total_return,
                    "annualized_return": r.annualized_return,
                    "alpha": r.alpha,
                    "sharpe_ratio": r.sharpe_ratio,
                    "max_drawdown": r.max_drawdown,
                    "win_rate": r.win_rate,
                    "duration_seconds": r.duration_seconds,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
                for r in rows
            ],
            "count": len(rows),
        }
    finally:
        db.close()


@router.get("/backtest/results/{run_id}")
async def get_backtest_result(run_id: str):
    """Get the full result of a specific backtest run."""
    from app.database import SessionLocal
    from app.models.agent import BacktestRun
    from sqlalchemy import select

    db = SessionLocal()
    try:
        row = db.execute(
            select(BacktestRun).where(BacktestRun.run_id == run_id)
        ).scalars().first()

        if not row:
            raise HTTPException(status_code=404, detail=f"Backtest run {run_id} not found")

        result_data = None
        if row.result_json:
            try:
                result_data = json.loads(row.result_json)
            except json.JSONDecodeError:
                result_data = {"raw": row.result_json}

        return {
            "run_id": row.run_id,
            "status": row.status,
            "verdict": row.verdict,
            "strategy": row.strategy,
            "symbols": row.symbols_csv,
            "config": json.loads(row.config_json) if row.config_json else None,
            "result": result_data,
            "created_at": row.created_at.isoformat() if row.created_at else None,
        }
    finally:
        db.close()


@router.get("/backtest/compare")
async def compare_backtests(
    run_ids: str = Query(..., description="Comma-separated run IDs to compare"),
):
    """Compare multiple backtest runs side-by-side."""
    from app.database import SessionLocal
    from app.models.agent import BacktestRun
    from sqlalchemy import select

    ids = [rid.strip() for rid in run_ids.split(",") if rid.strip()]
    if not ids:
        raise HTTPException(status_code=422, detail="Provide at least one run_id")

    db = SessionLocal()
    try:
        rows = db.execute(
            select(BacktestRun).where(BacktestRun.run_id.in_(ids))
        ).scalars().all()

        if not rows:
            raise HTTPException(status_code=404, detail="No matching backtest runs found")

        comparisons = []
        for r in rows:
            result_data = None
            if r.result_json:
                try:
                    result_data = json.loads(r.result_json)
                except json.JSONDecodeError:
                    pass

            comparisons.append({
                "run_id": r.run_id,
                "strategy": r.strategy,
                "symbols": r.symbols_csv,
                "verdict": r.verdict,
                "total_return": r.total_return,
                "annualized_return": r.annualized_return,
                "alpha": r.alpha,
                "sharpe_ratio": r.sharpe_ratio,
                "max_drawdown": r.max_drawdown,
                "win_rate": r.win_rate,
                "duration_seconds": r.duration_seconds,
                "nav_series": result_data.get("nav_series") if result_data else None,
                "monthly_returns": result_data.get("monthly_returns") if result_data else None,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            })

        return {"comparisons": comparisons, "count": len(comparisons)}
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════
#  Threshold Optimization API
# ═══════════════════════════════════════════════════════════════


class OptimizeRequest(BaseModel):
    """Request body for launching threshold optimisation.

    Time-split model: lookback_years = train + test.
    Default: 10 years total → 7 years train + 3 years test.
    The test period is used to validate the entire stock-picking logic
    with the thresholds found during training.
    """
    stocks: list[str] = Field(default_factory=list, max_length=100)
    universe: Optional[str] = None
    paramKeys: List[str] = Field(
        default_factory=list,
        description="Parameter keys to optimise (e.g. ['screener.pe_max', 'school_weight.buffett']). "
                    "Empty = use default optimisable set (~12 key parameters).",
    )
    searchMethod: str = Field(default="grid", description="'grid' | 'random'")
    maxTrials: int = Field(default=200, ge=5, le=2000)
    holdingMonths: int = Field(default=6, ge=1, le=24)
    lookbackYears: float = Field(
        default=10.0, ge=3.0, le=20.0,
        description="Total data period (years). Default 10. Need long history for meaningful train/test.",
    )
    testYears: float = Field(
        default=3.0, ge=2.0, le=10.0,
        description="Fixed test period at the END of the data (years). Must be ≥ 3. "
                    "Remaining (lookback - test) years form the training set for Grid Search.",
    )
    strategy: str = Field(default="balanced")
    benchmark: str = Field(default="SPY")
    initialCapital: float = Field(default=1_000_000, ge=10_000, le=100_000_000)
    maxHoldings: int = Field(default=15, ge=3, le=50)
    trainRatio: float = Field(
        default=0.70, ge=0.5, le=0.9,
        description="Fallback train ratio (only used when testYears=0).",
    )
    targetSharpe: float = Field(default=1.0, ge=0.0, le=5.0)
    seed: int = Field(default=42)


@router.post("/backtest/optimize")
async def run_optimization(req: OptimizeRequest):
    """Launch automated threshold optimisation (SSE stream).

    Uses Train/Test temporal split to find the best parameter combination
    that maximises Sharpe Ratio on in-sample data and validates on
    out-of-sample data.

    Returns real-time progress events:
      - optimize_start / optimize_phase / optimize_trial / optimize_complete / optimize_error

    Target: Sharpe Ratio ≥ 1.0 on the test set.
    """
    from app.agent.backtest.threshold_optimizer import (
        ThresholdOptimizer,
        OptimizationConfig,
    )

    # Resolve symbols
    stocks = [s.upper().strip() for s in req.stocks if s.strip()]
    if not stocks and req.universe:
        from app.agent.unified_pipeline import PRESET_UNIVERSES
        stocks = PRESET_UNIVERSES.get(req.universe, [])

    if not stocks:
        raise HTTPException(status_code=422, detail="Provide stocks or a universe name")

    config = OptimizationConfig(
        symbols=stocks,
        param_keys=req.paramKeys,
        search_method=req.searchMethod,
        max_trials=req.maxTrials,
        holding_months=req.holdingMonths,
        lookback_years=req.lookbackYears,
        strategy=req.strategy,
        benchmark=req.benchmark,
        initial_capital=req.initialCapital,
        max_holdings=req.maxHoldings,
        test_years=req.testYears,
        train_ratio=req.trainRatio,
        target_sharpe=req.targetSharpe,
        seed=req.seed,
    )

    optimizer = ThresholdOptimizer(config)

    async def _stream():
        try:
            async for event in optimizer.run():
                if "type" in event and "event" not in event:
                    event["event"] = event.pop("type")
                data = json.dumps(event, default=str, ensure_ascii=False)
                yield f"data: {data}\n\n"

            # Persist result
            result = optimizer.get_result()
            if result:
                _persist_optimization(result)

        except Exception as e:
            logger.error(f"Optimize SSE error: {e}", exc_info=True)
            yield f"data: {json.dumps({'event': 'optimize_error', 'message': str(e)[:500]})}\n\n"

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


class WalkForwardRequest(BaseModel):
    """Request body for launching Walk-Forward validation.

    Walk-Forward uses multiple rolling windows, each with a training
    period followed by a fixed test period (≥3 years).
    """
    stocks: list[str] = Field(default_factory=list, max_length=100)
    universe: Optional[str] = None
    paramKeys: List[str] = Field(default_factory=list)
    nFolds: int = Field(default=3, ge=2, le=10)
    trainWindowMonths: int = Field(default=48, ge=12, le=120)
    testWindowMonths: int = Field(default=36, ge=12, le=60)
    searchMethod: str = Field(default="grid")
    maxTrialsPerFold: int = Field(default=100, ge=5, le=500)
    holdingMonths: int = Field(default=6, ge=1, le=24)
    lookbackYears: float = Field(
        default=10.0, ge=3.0, le=20.0,
        description="Total data period. Default 10 years.",
    )
    strategy: str = Field(default="balanced")
    benchmark: str = Field(default="SPY")
    initialCapital: float = Field(default=1_000_000, ge=10_000, le=100_000_000)
    maxHoldings: int = Field(default=15, ge=3, le=50)
    targetSharpe: float = Field(default=1.0, ge=0.0, le=5.0)
    seed: int = Field(default=42)


@router.post("/backtest/walk-forward")
async def run_walk_forward(req: WalkForwardRequest):
    """Launch Walk-Forward validation (SSE stream).

    Unlike a single Train/Test split, Walk-Forward uses K rolling windows
    to validate that the optimised parameters work across different market
    regimes, not just one lucky period.

    Returns real-time progress events:
      - walkforward_start / walkforward_phase / walkforward_fold_start /
        walkforward_fold_progress / walkforward_fold_complete / walkforward_complete

    Target: Average out-of-sample Sharpe ≥ 1.0 across all folds.
    """
    from app.agent.backtest.walk_forward import (
        WalkForwardValidator,
        WalkForwardConfig,
    )

    # Resolve symbols
    stocks = [s.upper().strip() for s in req.stocks if s.strip()]
    if not stocks and req.universe:
        from app.agent.unified_pipeline import PRESET_UNIVERSES
        stocks = PRESET_UNIVERSES.get(req.universe, [])

    if not stocks:
        raise HTTPException(status_code=422, detail="Provide stocks or a universe name")

    config = WalkForwardConfig(
        symbols=stocks,
        param_keys=req.paramKeys,
        n_folds=req.nFolds,
        train_window_months=req.trainWindowMonths,
        test_window_months=req.testWindowMonths,
        search_method=req.searchMethod,
        max_trials_per_fold=req.maxTrialsPerFold,
        holding_months=req.holdingMonths,
        lookback_years=req.lookbackYears,
        strategy=req.strategy,
        benchmark=req.benchmark,
        initial_capital=req.initialCapital,
        max_holdings=req.maxHoldings,
        target_sharpe=req.targetSharpe,
        seed=req.seed,
    )

    validator = WalkForwardValidator(config)

    async def _stream():
        try:
            async for event in validator.run():
                if "type" in event and "event" not in event:
                    event["event"] = event.pop("type")
                data = json.dumps(event, default=str, ensure_ascii=False)
                yield f"data: {data}\n\n"

            # Persist result
            result = validator.get_result()
            if result:
                _persist_walk_forward(result)

        except Exception as e:
            logger.error(f"WalkForward SSE error: {e}", exc_info=True)
            yield f"data: {json.dumps({'event': 'walkforward_error', 'message': str(e)[:500]})}\n\n"

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


class ApplyOptimizedRequest(BaseModel):
    """Request to apply optimised parameters to the live system."""
    params: dict = Field(..., description="Optimised parameter dict from optimize/walk-forward result")
    reason: str = Field(default="Applied from threshold optimizer")


@router.post("/backtest/apply-optimized")
async def apply_optimized_params(req: ApplyOptimizedRequest):
    """Apply optimised parameters to the live system.

    Convenience endpoint: takes the best_params or consensus_params from
    an optimisation run and applies them via the parameter override system.
    """
    from app.agent.investment_params import params as _P

    if not req.params:
        raise HTTPException(status_code=422, detail="No parameters provided")

    results = _P.batch_override(req.params, req.reason)
    succeeded = {k: v for k, v in results.items() if v}
    failed = {k: v for k, v in results.items() if not v}

    return {
        "status": "success" if not failed else "partial",
        "applied": list(succeeded.keys()),
        "failed": list(failed.keys()),
        "message": f"已应用 {len(succeeded)} 个优化参数" + (
            f", {len(failed)} 个失败" if failed else ""
        ),
    }


def _persist_optimization(result):
    """Save an OptimizationResult to the database."""
    from app.database import SessionLocal
    from app.models.agent import BacktestRun

    db = SessionLocal()
    try:
        run = BacktestRun(
            run_id=f"opt_{result.run_id}",
            status="completed" if not result.error else "failed",
            config_json=json.dumps(result.config.to_dict(), default=str),
            result_json=json.dumps(result.to_dict(), default=str),
            verdict="OPTIMIZED" if result.target_achieved else "SUBOPTIMAL",
            total_return=0,
            annualized_return=0,
            alpha=0,
            sharpe_ratio=result.best_test_sharpe,
            max_drawdown=0,
            win_rate=0,
            duration_seconds=result.duration_seconds,
            symbols_csv=",".join(result.config.symbols),
            strategy=result.config.strategy,
            error=result.error,
        )
        db.add(run)
        db.commit()
        logger.info(f"[Optimizer] Persisted run opt_{result.run_id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Optimizer] Failed to persist: {e}")
    finally:
        db.close()


def _persist_walk_forward(result):
    """Save a WalkForwardResult to the database."""
    from app.database import SessionLocal
    from app.models.agent import BacktestRun

    db = SessionLocal()
    try:
        run = BacktestRun(
            run_id=f"wf_{result.run_id}",
            status="completed" if not result.error else "failed",
            config_json=json.dumps(result.config.to_dict(), default=str),
            result_json=json.dumps(result.to_dict(), default=str),
            verdict="WF_VALIDATED" if result.target_achieved else "WF_SUBOPTIMAL",
            total_return=0,
            annualized_return=0,
            alpha=0,
            sharpe_ratio=result.avg_oos_sharpe,
            max_drawdown=0,
            win_rate=0,
            duration_seconds=result.duration_seconds,
            symbols_csv=",".join(result.config.symbols),
            strategy=result.config.strategy,
            error=result.error,
        )
        db.add(run)
        db.commit()
        logger.info(f"[WalkForward] Persisted run wf_{result.run_id}")
    except Exception as e:
        db.rollback()
        logger.error(f"[WalkForward] Failed to persist: {e}")
    finally:
        db.close()


def _persist_backtest(result):
    """Save a PITBacktestResult to the database."""
    from app.database import SessionLocal
    from app.models.agent import BacktestRun

    db = SessionLocal()
    try:
        m = result.metrics
        run = BacktestRun(
            run_id=result.run_id,
            status="completed" if not result.error else "failed",
            config_json=json.dumps(result.config.to_dict(), default=str),
            result_json=result.to_json(),
            verdict=result.verdict,
            total_return=m.total_return,
            annualized_return=m.annualized_return,
            alpha=m.alpha,
            sharpe_ratio=m.sharpe_ratio,
            max_drawdown=m.max_drawdown,
            win_rate=m.win_rate,
            duration_seconds=result.duration_seconds,
            symbols_csv=",".join(result.config.symbols),
            strategy=result.config.strategy,
            error=result.error,
        )
        db.add(run)
        db.commit()
        logger.info(f"[Backtest] Persisted run {result.run_id} — {result.verdict}")
    except Exception as e:
        db.rollback()
        logger.error(f"[Backtest] Failed to persist run: {e}")
    finally:
        db.close()
