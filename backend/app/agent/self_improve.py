"""Agent Self-Improvement Engine — Reflection/Reflexion pattern implementation.

Workflow:
1. Run eval suite → get EvaluationReport
2. Analyze weaknesses → identify improvement areas
3. Generate targeted improvements to persona/rules/tools
4. Apply improvements
5. Re-run eval → verify improvement
6. Repeat until convergence or max iterations

This implements the academic "Reflexion" agent pattern from:
- Shinn et al. 2023 "Reflexion: Language Agents with Verbal Reinforcement Learning"
- Madaan et al. 2023 "Self-Refine: Iterative Refinement with Self-Feedback"
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

from app.agent.evaluator import (
    EvaluationReport, DimensionReport, TestResult,
    run_evaluation, format_report, report_to_dict,
)
from app.agent.eval_suite import ALL_TESTS, get_quick_eval_suite, get_stats

logger = logging.getLogger(__name__)

# Where to store evaluation history
EVAL_HISTORY_DIR = Path(__file__).resolve().parent.parent.parent / "eval_history"


# ═══════════════════════════════════════════════════════════════
#  Improvement Actions — What can be changed
# ═══════════════════════════════════════════════════════════════

IMPROVEMENT_CATALOG = {
    "persona_tool_emphasis": {
        "area": "persona",
        "description": "在 persona 中强调工具使用的重要性",
        "trigger_weakness": ["工具", "tool", "未调用"],
    },
    "persona_citation_depth": {
        "area": "persona",
        "description": "增加 persona 中的书籍引用和经典案例要求",
        "trigger_weakness": ["引用", "名言", "知识引用"],
    },
    "persona_answer_structure": {
        "area": "persona",
        "description": "强化回答结构化要求",
        "trigger_weakness": ["简短", "深度不够", "结构"],
    },
    "persona_behavioral_knowledge": {
        "area": "persona",
        "description": "增加行为金融学知识到 persona",
        "trigger_weakness": ["行为金融", "认知偏差"],
    },
    "persona_edge_case_handling": {
        "area": "persona",
        "description": "增加边界情况处理指引",
        "trigger_weakness": ["边界", "edge_case", "特殊情况"],
    },
    "rules_coverage": {
        "area": "rules",
        "description": "增加 distilled_rules 的覆盖范围",
        "trigger_weakness": ["规则", "覆盖", "遗漏"],
    },
    "tools_enhancement": {
        "area": "tools",
        "description": "增强工具能力（如增加比较分析工具）",
        "trigger_weakness": ["对比", "比较", "comparative"],
    },
}


# ═══════════════════════════════════════════════════════════════
#  Analyze Weaknesses → Generate Improvements
# ═══════════════════════════════════════════════════════════════

def analyze_weaknesses(report: EvaluationReport) -> List[Dict[str, Any]]:
    """Analyze evaluation report and determine needed improvements.

    Returns list of improvement actions with priority and details.
    """
    actions = []
    weakness_text = " ".join(report.top_weaknesses).lower()

    # Check each improvement in catalog
    for action_id, action_info in IMPROVEMENT_CATALOG.items():
        for trigger in action_info["trigger_weakness"]:
            if trigger.lower() in weakness_text:
                actions.append({
                    "id": action_id,
                    "area": action_info["area"],
                    "description": action_info["description"],
                    "priority": "HIGH",
                })
                break

    # Check per-dimension weaknesses
    for dim, dim_report in report.dimension_reports.items():
        if dim_report.priority in ("critical", "high"):
            actions.append({
                "id": f"dim_{dim}_improvement",
                "area": "persona",
                "description": f"改进 {dim} 维度能力 (当前得分: {dim_report.avg_score}/10)",
                "priority": dim_report.priority.upper(),
                "dimension": dim,
                "weaknesses": dim_report.weaknesses,
            })

    # Deduplicate by area
    seen = set()
    unique_actions = []
    for a in actions:
        key = a["id"]
        if key not in seen:
            seen.add(key)
            unique_actions.append(a)

    return sorted(unique_actions, key=lambda x: {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2}.get(x["priority"], 3))


def generate_persona_patches(
    report: EvaluationReport,
    actions: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    """Generate specific patches to apply to persona.py based on weakness analysis.

    Returns list of {location, old_text, new_text} patches.
    """
    patches = []
    weakness_areas = {a["id"] for a in actions}

    # 1. Tool emphasis patch
    if "persona_tool_emphasis" in weakness_areas:
        patches.append({
            "id": "tool_emphasis",
            "description": "强化工具调用指令",
            "addition": """
### 重要工具使用规则 ⚠️
1. **每次分析股票时，必须首先调用 evaluate_stock_rules** — 这不是可选的，是必须的
2. **需要数据支撑时，必须调用 get_stock_fundamentals** — 不要凭空回答
3. **涉及估值时，必须调用 run_valuation_analysis** — 用7模型估值
4. **涉及风险时，必须调用 detect_financial_shenanigans** — 三重排雷
5. 不要在没有调用任何工具的情况下给出投资建议
""",
            "location": "after_toolbox",
        })

    # 2. Citation depth patch
    if "persona_citation_depth" in weakness_areas:
        patches.append({
            "id": "citation_depth",
            "description": "扩充经典引用库",
            "addition": """
### 扩展引用库
你的回答中应至少包含 1-2 条经典引用来增强说服力：

**格雷厄姆系列**:
- 「聪明的投资者通过自律和勇气，将熊市转化为他的优势。」
- 「投资最重要的品质是理性，而非智力。」
- 「对于防御型投资者，我们强调分散化、高质量、审慎的价格。」

**巴菲特/芒格系列**:
- 「我们的投资方法：找到好的企业，以合理的价格买入，然后耐心持有。」
- 「如果你不愿意持有一只股票十年，那就不要持有十分钟。」
- 「护城河不是静态的，它要么在加宽，要么在收窄。」
- 「永远不要低估一个过于宽泛的能力圈所带来的破坏力。」— 芒格

**量化系列**:
- 「最好的投资策略是你能坚持执行的策略。」— O'Shaughnessy
- 「投资中最大的风险不是波动性，而是你自己。」— Gray

**行为金融**:
- 「投资成功的关键不是智商，而是情商。」— Guy Spier
- 「所有可怕的投资都有一个共同点：在最初的时候看起来很有吸引力。」
""",
            "location": "after_quotes",
        })

    # 3. Answer structure enforcement
    if "persona_answer_structure" in weakness_areas:
        patches.append({
            "id": "answer_structure",
            "description": "强制回答结构",
            "addition": """
### 回答深度要求 ⚠️
- 每次回答不少于 300 字
- 必须包含**具体数字**（PE、ROE、安全边际等）
- 必须引用**至少一位大师的观点**
- 如果被问到个股，必须给出**明确的结论**（买入/持有/回避），不能含糊
- 对比分析时，必须列出**量化对比表**
""",
            "location": "after_style",
        })

    # 4. Behavioral finance knowledge
    if "persona_behavioral_knowledge" in weakness_areas or any(
        a.get("dimension") == "behavioral" for a in actions
    ):
        patches.append({
            "id": "behavioral_knowledge",
            "description": "增加行为金融知识",
            "addition": """
### 行为金融学知识
你深谙以下认知偏差及其对投资决策的影响：

**主要认知偏差**:
1. **锚定效应**: 过度依赖最初的信息（如52周高点）做判断
2. **损失厌恶**: 对损失的恐惧约是获利快乐的2.5倍，导致过早卖出赢家
3. **确认偏差**: 只关注支持已有观点的信息
4. **从众心理**: 跟随大众而非独立思考
5. **过度自信**: 高估自己预测市场的能力
6. **近因偏差**: 过度关注最近发生的事件
7. **禀赋效应**: 高估已持有的股票价值

**应对策略** (来自 Guy Spier 《一个价值投资者的教育》):
- 使用清单（checklist）来对抗情绪化决策
- 远离市场噪音，减少看盘频率
- 建立投资日记，记录买入/卖出的理由
- 寻找「投资部落」—— 与理性投资者为伍
""",
            "location": "after_philosophy",
        })

    # 5. Edge case handling
    if "persona_edge_case_handling" in weakness_areas or any(
        a.get("dimension") == "edge_case" for a in actions
    ):
        patches.append({
            "id": "edge_case_handling",
            "description": "增加边界情况处理",
            "addition": """
### 特殊情况处理指引
当遇到以下情况时，不要生硬套用规则，而是灵活分析：

1. **高PE成长股**: PE>50 不等于不值得投资。改用 PEG、FCF/EV、营收增长等指标
2. **银行/金融股**: Graham 的流动比率、P/B 标准需要调整。用 P/TBV、净息差、不良率
3. **周期性行业**: 在周期底部PE会很高（甚至亏损），要看正常化盈利
4. **零利率环境**: 利率接近零时，"盈利收益率>国债利率"几乎所有股票都通过，需要加入风险溢价
5. **中概股/ADR**: 考虑 VIE 结构风险、监管风险、信息不对称
6. **高增长亏损公司**: 用 PS、营收增长、单位经济模型（Unit Economics）替代 PE
7. **REIT/MLP**: 用 FFO/AFFO 替代 EPS，分红覆盖率替代 PE

**核心原则**: Graham 的规则是指导方针，不是不可违背的教条。理解规则背后的逻辑比机械执行更重要。
""",
            "location": "after_sop",
        })

    # 6. Portfolio construction knowledge
    if any(a.get("dimension") == "portfolio" for a in actions):
        patches.append({
            "id": "portfolio_knowledge",
            "description": "增加组合构建知识",
            "addition": """
### 组合构建原则
当被问及投资组合时，你应该运用以下知识：

**Graham 防御型组合**: 15-30只大盘价值股，50/50 股债配置
**Buffett 集中型组合**: 5-10只最高确信度的持仓，坚持能力圈
**Greenblatt 量化组合**: 30只魔法公式排名最高的股票，年度调仓
**Kelly 仓位管理**: f* = (bp - q) / b，实践中用半Kelly降低风险

**调仓触发条件**:
- 股价达到内在价值（卖出）
- 基本面恶化（卖出）
- 发现更好的机会（换仓）
- 定期再平衡（年度/半年度）
""",
            "location": "after_philosophy",
        })

    return patches


# ═══════════════════════════════════════════════════════════════
#  Apply Improvements to Persona
# ═══════════════════════════════════════════════════════════════

def apply_persona_improvements(
    patches: List[Dict[str, str]],
    dry_run: bool = True,
) -> Dict[str, Any]:
    """Apply improvement patches to persona.py.

    Args:
        patches: List of patches to apply
        dry_run: If True, only show what would change without modifying files

    Returns:
        Dict with applied patches and resulting persona.
    """
    from app.agent.persona import CHARLIE_SYSTEM_PROMPT

    current_prompt = CHARLIE_SYSTEM_PROMPT
    applied = []
    skipped = []

    for patch in patches:
        addition = patch.get("addition", "")
        if not addition:
            skipped.append({"id": patch["id"], "reason": "no content"})
            continue

        # Check if already applied (fuzzy check)
        first_line = addition.strip().split("\n")[0]
        if first_line in current_prompt:
            skipped.append({"id": patch["id"], "reason": "already applied"})
            continue

        # Determine insertion point
        location = patch.get("location", "end")
        insert_after = _find_insertion_point(current_prompt, location)

        if insert_after >= 0:
            current_prompt = current_prompt[:insert_after] + "\n" + addition + current_prompt[insert_after:]
            applied.append(patch["id"])
        else:
            # Append at end before closing quote
            current_prompt = current_prompt.rstrip() + "\n" + addition + "\n"
            applied.append(patch["id"])

    result = {
        "applied": applied,
        "skipped": skipped,
        "new_prompt_length": len(current_prompt),
        "original_prompt_length": len(CHARLIE_SYSTEM_PROMPT),
        "dry_run": dry_run,
    }

    if not dry_run and applied:
        # Write updated persona
        _write_updated_persona(current_prompt)
        result["written"] = True
        logger.info(f"Applied {len(applied)} persona patches: {applied}")

    return result


def _find_insertion_point(prompt: str, location: str) -> int:
    """Find the position to insert new content based on location hint."""
    markers = {
        "after_toolbox": "第四章：你的工具箱",
        "after_quotes": "经典格言库",
        "after_style": "回答结构",
        "after_sop": "第二章：你的分析框架",
        "after_philosophy": "第一章：你的投资哲学体系",
    }

    marker = markers.get(location, "")
    if marker:
        idx = prompt.find(marker)
        if idx >= 0:
            # Find end of the section (next ═══ or end of prompt)
            next_section = prompt.find("═══════", idx + len(marker))
            if next_section > 0:
                return next_section
            return len(prompt)
    return -1


def _write_updated_persona(new_prompt: str):
    """Write the updated persona to persona.py."""
    persona_path = Path(__file__).resolve().parent / "persona.py"

    content = f'''"""Old Charlie persona — Deep Value Investing Agent system prompt.

Knowledge distilled from 14 classic investment books, 1,253 strategies extracted.
Organized into 7 investment schools with 65+ quantitative screening rules.
Auto-improved via self-evaluation at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.
"""

CHARLIE_SYSTEM_PROMPT = """{new_prompt}"""
'''
    persona_path.write_text(content, encoding="utf-8")


# ═══════════════════════════════════════════════════════════════
#  Self-Improvement Loop
# ═══════════════════════════════════════════════════════════════

async def run_self_improvement(
    max_iterations: int = 3,
    target_score: float = 8.0,
    quick: bool = True,
    dry_run: bool = True,
) -> Dict[str, Any]:
    """Run the full self-improvement loop.

    1. Evaluate Agent
    2. Analyze weaknesses
    3. Generate improvements
    4. Apply (if not dry_run)
    5. Re-evaluate
    6. Repeat until target_score or max_iterations

    Args:
        max_iterations: Max improvement rounds
        target_score: Stop when avg score >= this
        quick: Use quick eval suite (faster)
        dry_run: If True, don't modify files

    Returns:
        Dict with iteration history and final results.
    """
    history = []
    best_score = 0

    for iteration in range(1, max_iterations + 1):
        logger.info(f"═══ Self-Improvement Iteration {iteration}/{max_iterations} ═══")

        # Step 1: Evaluate
        report = await run_evaluation(quick=quick)
        report_text = format_report(report)
        logger.info(f"Iteration {iteration} score: {report.avg_score}/10 (grade: {report.overall_grade})")

        # Step 2: Analyze
        actions = analyze_weaknesses(report)
        logger.info(f"Identified {len(actions)} improvement actions")

        # Step 3: Generate patches
        patches = generate_persona_patches(report, actions)
        logger.info(f"Generated {len(patches)} persona patches")

        # Step 4: Apply
        apply_result = apply_persona_improvements(patches, dry_run=dry_run)

        # Step 5: Record
        history.append({
            "iteration": iteration,
            "score": report.avg_score,
            "grade": report.overall_grade,
            "actions_identified": len(actions),
            "patches_applied": len(apply_result.get("applied", [])),
            "patches_skipped": len(apply_result.get("skipped", [])),
            "report_summary": report_text[:1000],
            "recommendations": report.improvement_recommendations[:3],
        })

        # Save report to file
        _save_report(report, iteration)

        # Step 6: Check convergence
        if report.avg_score >= target_score:
            logger.info(f"Target score {target_score} reached! Stopping.")
            break

        if report.avg_score <= best_score and iteration > 1:
            logger.info("No improvement detected. Stopping.")
            break

        best_score = max(best_score, report.avg_score)

    return {
        "iterations": len(history),
        "history": history,
        "final_score": history[-1]["score"] if history else 0,
        "final_grade": history[-1]["grade"] if history else "F",
        "improved": len(history) > 1 and history[-1]["score"] > history[0]["score"],
        "dry_run": dry_run,
    }


def _save_report(report: EvaluationReport, iteration: int):
    """Save evaluation report to file."""
    EVAL_HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"eval_{timestamp}_iter{iteration}.json"
    filepath = EVAL_HISTORY_DIR / filename

    data = report_to_dict(report)
    data["iteration"] = iteration

    filepath.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"Saved report to {filepath}")


# ═══════════════════════════════════════════════════════════════
#  Offline Evaluation (No LLM required)
# ═══════════════════════════════════════════════════════════════

def run_offline_evaluation(responses: Dict[str, str]) -> EvaluationReport:
    """Run offline evaluation on pre-collected responses.

    Args:
        responses: Dict of test_id -> agent_response_text

    Useful for evaluating without real-time LLM calls.
    """
    from app.agent.eval_suite import get_test
    from app.agent.evaluator import score_response_rule_based, _build_report

    results = []
    for test_id, response in responses.items():
        test = get_test(test_id)
        if test:
            result = score_response_rule_based(test, response)
            results.append(result)

    return _build_report(results)


# ═══════════════════════════════════════════════════════════════
#  Quick Self-Check (run from command line)
# ═══════════════════════════════════════════════════════════════

def print_eval_suite_stats():
    """Print statistics about the evaluation suite."""
    stats = get_stats()
    print(f"\n{'='*50}")
    print(f"  老查理 Agent 评估套件统计")
    print(f"{'='*50}")
    print(f"  总测试数: {stats['total_tests']}")
    print(f"\n  按维度:")
    dim_names = {
        "conceptual": "理论知识", "stock_analysis": "个股分析",
        "comparative": "对比分析", "risk": "风险识别",
        "edge_case": "边界情况", "behavioral": "行为金融",
        "portfolio": "组合构建", "master": "大师级压力测试",
    }
    for dim, count in stats["by_dimension"].items():
        print(f"    {dim_names.get(dim, dim)}: {count} 题")
    print(f"\n  按难度:")
    for diff, count in stats["by_difficulty"].items():
        print(f"    {diff}: {count} 题")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    print_eval_suite_stats()
