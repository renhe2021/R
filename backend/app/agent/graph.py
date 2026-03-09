"""Old Charlie's multi-stage analysis pipeline — BBG 优先 + LLM 深度分析.

Pipeline: router → screener → interrogator → appraiser → reporter(+LLM) → sink
Chat mode: router → chat (LLM with tool-calling)

数据层: Bloomberg → FMP → Finnhub → yfinance (自动降级)
分析层: 量化规则引擎 + LLM 深度推理 (LLM 可选增强)
"""

import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, AsyncGenerator

from app.agent.state import AgentState, AgentMessage
from app.agent.persona import CHARLIE_SYSTEM_PROMPT
from app.agent.llm import is_llm_available
from app.agent.tools import (
    scan_fundamentals, detect_shenanigans, run_full_valuation,
    search_knowledge, analyze_news, search_book_library,
    evaluate_stock_rules,
)
from app.agent.screener import run_screening
from app.agent.sink import run_sink

logger = logging.getLogger(__name__)


# ─── Node Functions ───

async def router_node(state: AgentState) -> Dict[str, Any]:
    """Determine intent: analyze stocks or general chat."""
    last_msg = state["messages"][-1] if state["messages"] else None
    if not last_msg:
        return {"mode": "chat"}

    content = last_msg.content if hasattr(last_msg, "content") else str(last_msg)
    content_lower = content.lower()

    stock_pattern = r'\b[A-Z]{1,5}\b'
    potential_tickers = re.findall(stock_pattern, content)
    common_words = {
        "I", "A", "THE", "AND", "OR", "IS", "IT", "IN", "ON", "AT", "TO",
        "FOR", "OF", "BY", "DO", "IF", "MY", "AM", "AN", "AS", "BE", "SO",
        "NO", "UP", "OK", "RUN", "GET", "SET", "ALL", "OLD",
    }
    tickers = [t for t in potential_tickers if t not in common_words and len(t) >= 2]

    analyze_keywords = [
        "分析", "analyze", "analyse", "screen", "筛选", "排雷",
        "估值", "valuat", "check", "检查", "look at", "看看", "评估",
    ]
    has_analyze_intent = any(kw in content_lower for kw in analyze_keywords)

    if has_analyze_intent and len(tickers) >= 1:
        return {"mode": "analyze", "input_stocks": tickers}

    if state.get("input_stocks") and len(state["input_stocks"]) > 0:
        return {"mode": "analyze"}

    return {"mode": "chat"}


async def screener_node(state: AgentState) -> Dict[str, Any]:
    """Stage 1: Multi-school value investing screening, no LLM."""
    stocks = state.get("input_stocks", [])
    if not stocks:
        return {"screening_results": {"passed": [], "eliminated": [], "totalInput": 0}}

    from app.api.deps import get_data_router
    router = get_data_router()
    source_pref = state.get("data_source")
    results = await run_screening(stocks, router, source_pref=source_pref, mode="quick")

    msg = (
        f"**阶段1初筛完成** — 输入 {results['totalInput']} 只，"
        f"通过 {results['totalPassed']} 只，淘汰 {results['totalEliminated']} 只。\n"
    )
    if results["eliminated"]:
        msg += "淘汰名单：\n"
        for e in results["eliminated"][:10]:
            msg += f"- {e['symbol']}: {e['reason']}\n"
    if results["passed"]:
        msg += f"\n通过初筛的股票：{', '.join(results['passed'])}"

    return {
        "screening_results": results,
        "messages": [AgentMessage(role="assistant", content=msg)],
    }


async def interrogator_node(state: AgentState) -> Dict[str, Any]:
    """Stage 2: Direct tool calls for fraud detection — no LLM required.

    For each passed stock, directly call detect_shenanigans (pure Python computation)
    and scan_fundamentals, then aggregate results with a template summary.
    """
    screening = state.get("screening_results", {})
    passed = screening.get("passed", [])

    if not passed:
        return {
            "interrogation_results": {},
            "messages": [AgentMessage(role="assistant", content="没有通过初筛的股票，跳过排雷阶段。")],
        }

    interrogation_results = {}
    all_red_flags = []

    async def _interrogate_one(symbol: str) -> tuple:
        """Interrogate a single stock — runs concurrently via gather."""
        logger.info(f"Interrogating {symbol}...")
        shenanigans = await detect_shenanigans(symbol)

        red_flags = shenanigans.get("redFlags", [])
        risk_level = shenanigans.get("riskLevel", "LOW")
        z_score = shenanigans.get("zScore")
        f_score = shenanigans.get("fScore")
        m_score = shenanigans.get("mScore")

        if risk_level == "CRITICAL":
            verdict = "FAIL"
        elif risk_level == "HIGH" and len(red_flags) >= 3:
            verdict = "FAIL"
        else:
            verdict = "PASS"

        result = {
            "verdict": verdict,
            "riskLevel": risk_level,
            "redFlags": red_flags,
            "zScore": z_score,
            "fScore": f_score,
            "mScore": m_score,
            "summary": shenanigans.get("summary", ""),
        }
        return symbol, result

    # Run all interrogations concurrently
    results_list = await asyncio.gather(
        *[_interrogate_one(s) for s in passed],
        return_exceptions=True,
    )

    for item in results_list:
        if isinstance(item, Exception):
            logger.error(f"Interrogation failed: {item}")
            continue
        symbol, result = item
        interrogation_results[symbol] = result
        for rf in result.get("redFlags", []):
            all_red_flags.append({**rf, "symbol": symbol})

    # Build summary message
    survivors = [s for s, v in interrogation_results.items() if v["verdict"] != "FAIL"]
    failed = [s for s, v in interrogation_results.items() if v["verdict"] == "FAIL"]

    msg = f"**阶段2排雷完成** — 审问了 {len(passed)} 只股票。\n\n"

    if failed:
        msg += f"🚫 未通过排雷 ({len(failed)}): {', '.join(failed)}\n"
        for s in failed:
            r = interrogation_results[s]
            msg += f"  - {s}: 风险等级 {r['riskLevel']}, {len(r['redFlags'])} 个红旗\n"
            for rf in r["redFlags"][:3]:
                msg += f"    ⚠ {rf['name']} ({rf['severity']}): {rf['detail']}\n"

    if survivors:
        msg += f"\n✅ 通过排雷 ({len(survivors)}): {', '.join(survivors)}\n"
        for s in survivors:
            r = interrogation_results[s]
            msg += f"  - {s}: Z={r['zScore']}, F={r['fScore']}, M={r['mScore']}, 风险={r['riskLevel']}\n"

    return {
        "interrogation_results": interrogation_results,
        "messages": [AgentMessage(role="assistant", content=msg)],
    }


async def appraiser_node(state: AgentState) -> Dict[str, Any]:
    """Stage 3: Direct tool calls for valuation — no LLM required.

    For each survivor, directly call run_full_valuation (wraps ValueInvestingService.full_analysis)
    and analyze_news, then aggregate results with a template summary.
    """
    interrogation = state.get("interrogation_results", {})
    survivors = [s for s, v in interrogation.items() if v.get("verdict") != "FAIL"]

    if not survivors:
        return {
            "appraisal_results": {},
            "messages": [AgentMessage(role="assistant", content="没有通过排雷的股票，跳过估值阶段。")],
        }

    appraisal_results = {}

    async def _appraise_one(symbol: str) -> tuple:
        """Appraise a single stock — runs concurrently via gather."""
        logger.info(f"Appraising {symbol}...")

        # Run all three data fetches concurrently for each stock
        valuation_task = run_full_valuation(symbol)
        school_task = evaluate_stock_rules(symbol, school="all")
        news_task = _safe_analyze_news(symbol)

        valuation, school_eval_text, news_data = await asyncio.gather(
            valuation_task, school_task, news_task
        )

        valuations = valuation.get("valuations", {}) if isinstance(valuation, dict) else {}
        quality = valuation.get("quality", {}) if isinstance(valuation, dict) else {}
        margin_of_safety = valuation.get("marginOfSafety", valuation.get("margin_of_safety")) if isinstance(valuation, dict) else None

        recommendation = _determine_recommendation(valuation, interrogation.get(symbol, {}))

        reasoning_parts = []
        if isinstance(valuation, dict):
            if valuation.get("grahamNumber"):
                reasoning_parts.append(f"Graham Number: ${valuation['grahamNumber']:.2f}")
            if valuation.get("epv"):
                reasoning_parts.append(f"EPV: ${valuation['epv']:.2f}")
            if margin_of_safety is not None:
                reasoning_parts.append(f"Margin of Safety: {margin_of_safety:.1%}" if isinstance(margin_of_safety, (int, float)) else f"MoS: {margin_of_safety}")
            moat = quality.get("moatType") or quality.get("moat")
            if moat:
                reasoning_parts.append(f"Moat: {moat}")

        intrinsic_value = None
        if isinstance(valuation, dict):
            for key in ["intrinsicValue", "epv", "grahamNumber", "dcfValue"]:
                v = valuation.get(key)
                if v and isinstance(v, (int, float)) and v > 0:
                    intrinsic_value = v
                    break

        result = {
            "valuation": valuation,
            "news": news_data,
            "recommendation": recommendation,
            "intrinsicValue": intrinsic_value,
            "marginOfSafety": margin_of_safety if isinstance(margin_of_safety, (int, float)) else None,
            "reasoning": "; ".join(reasoning_parts) if reasoning_parts else "基于纯代码估值模型分析",
            "school_evaluation": school_eval_text,
        }
        return symbol, result

    # Run all appraisals concurrently
    results_list = await asyncio.gather(
        *[_appraise_one(s) for s in survivors],
        return_exceptions=True,
    )

    for item in results_list:
        if isinstance(item, Exception):
            logger.error(f"Appraisal failed: {item}")
            continue
        symbol, result = item
        appraisal_results[symbol] = result

    # Build summary message
    msg = f"**阶段3估值完成** — 评估了 {len(survivors)} 只股票。\n\n"
    for symbol, data in appraisal_results.items():
        rec = data["recommendation"]
        rec_emoji = {"strong_buy": "🟢", "buy": "🟢", "hold": "🟡", "avoid": "🔴"}.get(rec, "⚪")
        msg += f"{rec_emoji} **{symbol}**: {rec.upper()}\n"
        if data["intrinsicValue"]:
            msg += f"  内在价值: ${data['intrinsicValue']:.2f}\n"
        if data["marginOfSafety"] and isinstance(data["marginOfSafety"], (int, float)):
            msg += f"  安全边际: {data['marginOfSafety']:.1%}\n"
        msg += f"  {data['reasoning']}\n\n"

    return {
        "appraisal_results": appraisal_results,
        "messages": [AgentMessage(role="assistant", content=msg)],
    }


async def _safe_analyze_news(symbol: str, limit: int = 5) -> dict:
    """Fetch news with error handling — used inside gather."""
    try:
        news_raw = await analyze_news(symbol, limit=limit)
        return json.loads(news_raw) if isinstance(news_raw, str) else news_raw
    except Exception:
        return {"error": "Could not fetch news"}


def _determine_recommendation(valuation: Any, interrogation_data: Dict) -> str:
    """Determine stock recommendation based on valuation data and risk profile."""
    if not isinstance(valuation, dict) or valuation.get("error"):
        return "hold"

    risk_level = interrogation_data.get("riskLevel", "LOW")
    f_score = interrogation_data.get("fScore")
    mos = valuation.get("marginOfSafety", valuation.get("margin_of_safety"))

    # Conservative rules (Old Charlie style)
    if risk_level in ("HIGH", "CRITICAL"):
        return "avoid"

    if isinstance(mos, (int, float)):
        if mos >= 0.4:
            return "strong_buy" if (f_score and f_score >= 7) else "buy"
        elif mos >= 0.2:
            return "buy" if risk_level == "LOW" else "hold"

    if f_score and f_score >= 7:
        return "buy"
    elif f_score and f_score >= 5:
        return "hold"

    return "hold"


async def reporter_node(state: AgentState) -> Dict[str, Any]:
    """Generate final CharlieVerdict report — LLM-enhanced when available, template fallback."""
    run_id = str(uuid.uuid4())[:8]
    screening = state.get("screening_results", {})
    interrogation = state.get("interrogation_results", {})
    appraisal = state.get("appraisal_results", {})

    # Build structured final picks
    final_picks = []
    for symbol, data in appraisal.items():
        final_picks.append({
            "symbol": symbol,
            "recommendation": data.get("recommendation", "hold"),
            "intrinsicValue": data.get("intrinsicValue"),
            "marginOfSafety": data.get("marginOfSafety"),
            "reasoning": data.get("reasoning", ""),
        })

    final_avoids = [
        {"symbol": e["symbol"], "reason": e["reason"]}
        for e in screening.get("eliminated", [])
    ]

    # Build template-based Charlie summary (always available)
    template_summary = _build_charlie_summary(screening, interrogation, appraisal, final_picks, final_avoids)

    # ── LLM 深度分析（如果 LLM 可用） ──
    charlie_summary = template_summary
    llm_analysis = None
    if is_llm_available() and (final_picks or final_avoids):
        try:
            llm_analysis = await _llm_deep_analysis(
                screening, interrogation, appraisal, final_picks, final_avoids
            )
            if llm_analysis:
                charlie_summary = template_summary + "\n\n" + llm_analysis
        except Exception as e:
            logger.warning(f"LLM 深度分析失败，使用模板: {e}")

    verdict = {
        "runId": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "input_stocks": state.get("input_stocks", []),
        "screening": screening,
        "interrogation": interrogation,
        "appraisals": appraisal,
        "final_picks": final_picks,
        "final_avoids": final_avoids,
        "charlie_summary": charlie_summary,
        "llm_analysis": llm_analysis,
        "debate_records": state.get("debate_records"),
        "strategy_backtest": state.get("strategy_backtest"),
    }

    return {
        "final_report": verdict,
        "messages": [AgentMessage(role="assistant", content=f"**老查理的最终报告**\n\n{charlie_summary}")],
    }


async def _llm_deep_analysis(
    screening: Dict, interrogation: Dict, appraisal: Dict,
    picks: List[Dict], avoids: List[Dict],
) -> Optional[str]:
    """Use LLM to generate deep investment analysis based on collected data.

    Sends all quantitative results to LLM and asks for:
    1. 投资逻辑深度解读
    2. 风险因素综合评估
    3. 比较分析（如多只股票）
    4. 具体买入策略建议
    """
    from app.agent.llm import simple_completion
    from app.agent.persona import CHARLIE_SYSTEM_PROMPT

    # Compile data snapshot for LLM
    data_lines = ["以下是量化分析引擎的完整输出数据：\n"]

    # Screening summary
    data_lines.append(f"== 初筛 ==")
    data_lines.append(f"输入: {screening.get('totalInput', 0)} 只")
    data_lines.append(f"通过: {screening.get('totalPassed', 0)} 只")
    for e in screening.get("eliminated", [])[:10]:
        data_lines.append(f"  淘汰: {e.get('symbol', '?')} — {e.get('reason', '?')}")

    # Interrogation
    data_lines.append(f"\n== 排雷 ==")
    for sym, data in interrogation.items():
        if isinstance(data, dict):
            data_lines.append(
                f"  {sym}: verdict={data.get('verdict')}, risk={data.get('riskLevel')}, "
                f"Z={data.get('zScore')}, F={data.get('fScore')}, M={data.get('mScore')}"
            )
            for rf in data.get("redFlags", []):
                data_lines.append(f"    ⚠ {rf.get('name', '')}: {rf.get('detail', '')}")

    # Appraisal
    data_lines.append(f"\n== 估值与流派评估 ==")
    for sym, data in appraisal.items():
        if isinstance(data, dict):
            val = data.get("valuation", {})
            if isinstance(val, dict):
                data_lines.append(
                    f"  {sym}: price=${val.get('price', '?')}, "
                    f"intrinsic=${data.get('intrinsicValue', '?')}, "
                    f"MoS={data.get('marginOfSafety', '?')}, "
                    f"rec={data.get('recommendation', '?')}"
                )
                valuations = val.get("valuations", {})
                if valuations:
                    data_lines.append(f"    估值模型: {json.dumps(valuations, default=str)}")
            school = data.get("school_evaluation", "")
            if school:
                # 截取前 800 字
                data_lines.append(f"    七流派评估:\n{school[:800]}")

    data_snapshot = "\n".join(data_lines)

    prompt = f"""你是「老查理」——一位严格的价值投资顾问。
现在你已经拿到了量化分析引擎对以下股票的完整分析数据。

{data_snapshot}

请你基于以上数据，提供一份**深度投资分析报告**，包含以下部分：

1. **核心发现**（2-3句话概括最重要的发现）
2. **逐股深度分析**（对每只通过排雷的股票，分析其投资逻辑、竞争优势、风险因素）
3. **比较分析**（如果有多只股票，进行横向对比，指出各自优劣）
4. **关键风险警示**（综合所有红旗信号和风险，给出务实的风险评估）
5. **买入策略建议**（具体的买入价位区间、仓位建议、时间窗口）
6. **老查理寄语**（一段简短的投资智慧总结）

要求：
- 所有分析必须基于数据，不允许编造数字
- 引用具体的估值模型结果（Graham Number, DCF, EPV等）
- 用安全边际的概念来评估每只股票
- 风格严谨、保守，宁可错失也不要冒险
- 如果数据不充分，明确指出而不是猜测
"""

    messages = [
        {"role": "system", "content": CHARLIE_SYSTEM_PROMPT},
        {"role": "user", "content": prompt},
    ]

    result = await simple_completion(messages, temperature=0.5, max_tokens=4096)

    if result and not result.startswith("[LLM Error"):
        return f"═══ 老查理 AI 深度分析 ═══\n\n{result}"
    return None


def _build_charlie_summary(screening: Dict, interrogation: Dict, appraisal: Dict,
                           picks: List[Dict], avoids: List[Dict]) -> str:
    """Build a structured summary in Old Charlie's style — no LLM needed."""
    total_input = screening.get("totalInput", 0)
    total_passed = screening.get("totalPassed", 0)

    survivors = [s for s, v in interrogation.items() if v.get("verdict") != "FAIL"]
    failed_interrog = [s for s, v in interrogation.items() if v.get("verdict") == "FAIL"]

    lines = [
        "═══════════════════════════════════════",
        "         老查理的投资审判书",
        "═══════════════════════════════════════",
        "",
        f"📊 输入 {total_input} 只股票",
        f"🔍 阶段1初筛：{total_passed} 只通过，{total_input - total_passed} 只淘汰",
    ]

    if avoids:
        lines.append("  淘汰原因：")
        for a in avoids[:5]:
            lines.append(f"    ✘ {a['symbol']}: {a['reason']}")

    lines.append(f"🕵 阶段2排雷：{len(survivors)} 只存活，{len(failed_interrog)} 只毙命")
    if failed_interrog:
        for s in failed_interrog:
            r = interrogation[s]
            lines.append(f"    ✘ {s}: {r.get('summary', '风险过高')}")

    lines.append(f"⚖ 阶段3估值：{len(picks)} 只最终裁决")
    if picks:
        for p in picks:
            rec_map = {"strong_buy": "强力买入", "buy": "买入", "hold": "持有观望", "avoid": "回避"}
            rec_cn = rec_map.get(p["recommendation"], p["recommendation"])
            line = f"    → {p['symbol']}: {rec_cn}"
            if p.get("intrinsicValue"):
                line += f" | 内在价值 ${p['intrinsicValue']:.2f}"
            if p.get("marginOfSafety") and isinstance(p["marginOfSafety"], (int, float)):
                line += f" | 安全边际 {p['marginOfSafety']:.1%}"
            lines.append(line)
    else:
        lines.append("    没有股票通过老查理的严格筛选。")

    lines.extend([
        "",
        "───────────────────────────────────────",
        "「宁可错杀一千，不可放过一个。」— 老查理",
        "───────────────────────────────────────",
    ])

    return "\n".join(lines)


async def sink_node(state: AgentState) -> Dict[str, Any]:
    """Persist verdict to DB and dispatch to downstream consumers."""
    verdict = state.get("final_report")
    if not verdict:
        return {"sink_results": {"dbWritten": False, "error": "No verdict to persist"}}

    run_id = verdict.get("runId", str(uuid.uuid4())[:8])
    results = await run_sink(verdict, run_id)
    return {"sink_results": results}


async def chat_node(state: AgentState) -> Dict[str, Any]:
    """Chat mode — uses LLM with tool-calling. Requires OPENAI_API_KEY."""
    from app.agent.llm import chat_completion

    last_msg = state["messages"][-1] if state["messages"] else None
    content = last_msg.content if hasattr(last_msg, "content") else str(last_msg)

    if not is_llm_available():
        error_msg = (
            "⚠️ **OPENAI_API_KEY 未配置**\n\n"
            "要使用老查理顾问模式，请完成以下设置：\n\n"
            "1. 在 `backend/.env` 文件中添加：\n"
            "   ```\n"
            "   OPENAI_API_KEY=sk-your-key-here\n"
            "   ```\n"
            "2. 重启后端服务\n\n"
            "配置完成后即可与老查理对话。"
        )
        return {"messages": [AgentMessage(role="assistant", content=error_msg)]}

    # Build conversation history for LLM
    llm_messages = [{"role": "system", "content": CHARLIE_SYSTEM_PROMPT}]
    for msg in state["messages"]:
        role = msg.role if hasattr(msg, "role") else "user"
        msg_content = msg.content if hasattr(msg, "content") else str(msg)
        if role in ("user", "assistant", "system"):
            llm_messages.append({"role": role, "content": msg_content})

    response_text = await chat_completion(llm_messages)
    return {"messages": [AgentMessage(role="assistant", content=response_text)]}


# ─── Pipeline Runner ───

async def run_pipeline(state: AgentState) -> AsyncGenerator[Dict[str, Any], None]:
    """Run the full Old Charlie pipeline as an async generator, yielding events.

    This replaces LangGraph's StateGraph with a simple sequential pipeline.
    """
    # Step 1: Route
    route_result = await router_node(state)
    state.update(route_result)

    if state.get("mode") == "chat":
        yield {"type": "phase_start", "phase": "chat"}
        chat_result = await chat_node(state)
        for msg in chat_result.get("messages", []):
            yield {"type": "message", "content": msg.content}
        yield {"type": "done"}
        return

    # Analysis mode — three-stage pipeline
    input_stocks = state.get("input_stocks", [])

    # Stage 1: Screening
    yield {"type": "phase_start", "phase": "screening", "total": len(input_stocks)}
    screening_result = await screener_node(state)
    state.update(screening_result)
    for msg in screening_result.get("messages", []):
        yield {"type": "message", "content": msg.content}

    screening = state.get("screening_results", {})
    yield {
        "type": "screening_result",
        "passed": screening.get("passed", []),
        "eliminated": [e.get("symbol", "") for e in screening.get("eliminated", [])],
        "criteria": screening.get("criteriaUsed", []),
    }

    # Stage 2: Interrogation
    passed = screening.get("passed", [])
    yield {"type": "phase_start", "phase": "interrogation", "total": len(passed)}
    interrogation_result = await interrogator_node(state)
    state.update(interrogation_result)
    for msg in interrogation_result.get("messages", []):
        yield {"type": "message", "content": msg.content}

    # Yield individual red flags
    interrogation = state.get("interrogation_results", {})
    for symbol, data in interrogation.items():
        if isinstance(data, dict):
            for flag in data.get("redFlags", []):
                yield {
                    "type": "red_flag",
                    "symbol": symbol,
                    "category": flag.get("category", ""),
                    "description": flag.get("detail", flag.get("name", "")),
                    "severity": flag.get("severity", "MEDIUM"),
                }

    # Stage 3: Appraisal
    survivors = [s for s, v in interrogation.items() if v.get("verdict") != "FAIL"]
    yield {"type": "phase_start", "phase": "appraisal", "total": len(survivors)}
    appraisal_result = await appraiser_node(state)
    state.update(appraisal_result)
    for msg in appraisal_result.get("messages", []):
        yield {"type": "message", "content": msg.content}

    # Reporter
    yield {"type": "phase_start", "phase": "reporting"}
    report_result = await reporter_node(state)
    state.update(report_result)

    if state.get("final_report"):
        yield {"type": "report", "data": state["final_report"]}

    # Sink
    sink_result = await sink_node(state)
    state.update(sink_result)

    # Done
    summary = {
        "total": len(input_stocks),
        "passedScreening": len(passed),
        "passedInterrogation": len(survivors),
        "final": len(state.get("final_report", {}).get("final_picks", [])),
    }
    yield {"type": "done", "summary": summary}
