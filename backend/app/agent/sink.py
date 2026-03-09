"""Sink node — Persist pipeline verdict to SQLite database."""

import json
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


async def run_sink(verdict: Dict[str, Any], run_id: str) -> Dict[str, Any]:
    """Write the verdict to the agent_verdicts table.

    Args:
        verdict: Full verdict dict from reporter_node
        run_id: Unique run identifier

    Returns:
        Dict with: dbWritten, runId, error (if any)
    """
    try:
        from app.database import SessionLocal
        from app.models.agent import AgentVerdict

        db = SessionLocal()
        try:
            existing = db.query(AgentVerdict).filter(AgentVerdict.run_id == run_id).first()
            if existing:
                return {"dbWritten": False, "runId": run_id, "error": "Verdict already exists"}

            picks = verdict.get("final_picks", [])
            avoids = verdict.get("final_avoids", [])

            # Count total red flags
            red_flags_count = 0
            interrogation = verdict.get("interrogation", {})
            for sym_data in interrogation.values():
                if isinstance(sym_data, dict):
                    red_flags_count += len(sym_data.get("redFlags", []))

            record = AgentVerdict(
                run_id=run_id,
                input_stocks=json.dumps(verdict.get("input_stocks", [])),
                verdict_json=json.dumps(verdict, default=str),
                final_picks_count=len(picks),
                red_flags_count=red_flags_count,
                status="completed",
                charlie_summary=verdict.get("charlie_summary", ""),
                debate_json=json.dumps(verdict.get("committeeDebates"), default=str) if verdict.get("committeeDebates") else None,
                committee_votes=json.dumps(
                    {sym: d.get("vote_tally", {}) for sym, d in (verdict.get("committeeDebates") or {}).items()},
                    default=str,
                ) if verdict.get("committeeDebates") else None,
                strategy_backtest=json.dumps(verdict.get("strategyBacktest"), default=str) if verdict.get("strategyBacktest") else None,
            )
            db.add(record)
            db.commit()
            logger.info(f"Verdict {run_id} persisted: {len(picks)} picks, {red_flags_count} red flags")
            return {"dbWritten": True, "runId": run_id}

        except Exception as e:
            db.rollback()
            logger.error(f"Sink DB error: {e}")
            return {"dbWritten": False, "runId": run_id, "error": str(e)[:200]}
        finally:
            db.close()

    except Exception as e:
        logger.error(f"Sink error: {e}")
        return {"dbWritten": False, "runId": run_id, "error": str(e)[:200]}
