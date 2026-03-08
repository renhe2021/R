"""Agent Evaluator — Automated scoring of Old Charlie's responses.

Implements the Reflection pattern:
1. Run Agent on test questions
2. Score responses against rubrics
3. Identify weakness patterns
4. Generate improvement recommendations

Scoring Modes:
- Rule-based: Fast, no LLM required — checks for expected elements
- LLM-as-Judge: Uses a second LLM to grade quality (when API available)
- Hybrid: Rule-based first, then LLM refinement
"""

import json
import logging
import re
import time
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, field

from app.agent.eval_suite import (
    TestCase, ALL_TESTS, TESTS_BY_DIMENSION, get_quick_eval_suite, get_stats,
)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  Evaluation Result Data Structures
# ═══════════════════════════════════════════════════════════════

@dataclass
class TestResult:
    """Result of evaluating one test case."""
    test_id: str
    dimension: str
    difficulty: str
    question: str
    response: str
    score: float  # 0-10
    max_score: float  # typically 10
    rubric_scores: Dict[str, Tuple[float, float]]  # criterion -> (scored, max)
    element_hits: List[str]  # expected elements found
    element_misses: List[str]  # expected elements NOT found
    anti_pattern_violations: List[str]  # anti-patterns found in response
    tool_calls_made: List[str]  # tools the agent called
    tool_calls_expected: List[str]  # tools that should have been called
    tool_calls_missed: List[str]  # expected tools not called
    strengths: List[str]
    weaknesses: List[str]
    response_time_s: float = 0.0


@dataclass
class DimensionReport:
    """Aggregated report for one dimension."""
    dimension: str
    avg_score: float
    max_possible: float
    test_count: int
    pass_rate: float  # % of tests scoring >= 6/10
    strengths: List[str]
    weaknesses: List[str]
    priority: str  # "critical" | "high" | "medium" | "low"


@dataclass
class EvaluationReport:
    """Full evaluation report across all tests."""
    timestamp: str
    total_tests: int
    avg_score: float
    overall_grade: str  # A/B/C/D/F
    dimension_reports: Dict[str, DimensionReport]
    test_results: List[TestResult]
    top_strengths: List[str]
    top_weaknesses: List[str]
    improvement_recommendations: List[Dict[str, str]]  # priority, area, action


# ═══════════════════════════════════════════════════════════════
#  Rule-Based Scorer (No LLM required)
# ═══════════════════════════════════════════════════════════════

def score_response_rule_based(
    test: TestCase,
    response: str,
    tool_calls_made: Optional[List[str]] = None,
    response_time_s: float = 0.0,
) -> TestResult:
    """Score an agent response using rule-based evaluation.

    Checks:
    1. Expected element coverage (keywords/concepts present)
    2. Anti-pattern violations (bad answers)
    3. Tool call correctness
    4. Rubric scoring (estimated from element presence)
    5. Response quality heuristics (length, structure, citations)
    """
    tool_calls_made = tool_calls_made or []
    response_lower = response.lower()

    # 1. Check expected elements
    element_hits = []
    element_misses = []
    for elem in test.expected_elements:
        # Check both Chinese and English, case-insensitive
        if elem.lower() in response_lower:
            element_hits.append(elem)
        else:
            element_misses.append(elem)

    element_coverage = len(element_hits) / max(len(test.expected_elements), 1)

    # 2. Check anti-patterns
    anti_violations = []
    for ap in test.anti_patterns:
        if ap.lower() in response_lower:
            anti_violations.append(ap)

    anti_penalty = len(anti_violations) * 1.0  # -1 point per violation

    # 3. Check tool calls
    tool_calls_expected = test.expected_tool_calls
    tool_calls_missed = [t for t in tool_calls_expected if t not in tool_calls_made]
    tool_coverage = 1.0
    if tool_calls_expected:
        tool_coverage = (len(tool_calls_expected) - len(tool_calls_missed)) / len(tool_calls_expected)

    # 4. Estimate rubric scores from element presence + heuristics
    rubric_scores = {}
    total_rubric_points = 0
    scored_points = 0

    for criterion, max_pts in test.rubric.items():
        total_rubric_points += max_pts
        # Estimate score for this criterion
        est_score = _estimate_criterion_score(criterion, response, element_hits, tool_calls_made, max_pts)
        rubric_scores[criterion] = (est_score, max_pts)
        scored_points += est_score

    # 5. Quality heuristics
    quality_bonus = 0
    # Length check (not too short, not too long)
    if 200 <= len(response) <= 3000:
        quality_bonus += 0.3
    elif len(response) > 3000:
        quality_bonus += 0.1  # verbose but at least detailed
    # Structure check (headers, bullet points)
    if "**" in response or "##" in response or "---" in response:
        quality_bonus += 0.2
    # Citation check (quotes, book references)
    if "「" in response or "」" in response or "——" in response:
        quality_bonus += 0.3
    # Number/data check
    numbers_found = len(re.findall(r'\d+\.?\d*%|\$\d+|[A-Z]\d+\.?\d*', response))
    if numbers_found >= 3:
        quality_bonus += 0.2

    # 6. Calculate final score (0-10)
    if total_rubric_points > 0:
        rubric_pct = scored_points / total_rubric_points
    else:
        rubric_pct = element_coverage

    raw_score = (
        rubric_pct * 7.0 +        # 70% from rubric
        element_coverage * 1.5 +    # 15% from element coverage
        tool_coverage * 1.0 +       # 10% from tool calls
        quality_bonus               # bonus for quality
    )
    raw_score -= anti_penalty
    final_score = max(0, min(10, round(raw_score, 1)))

    # 7. Identify strengths and weaknesses
    strengths = []
    weaknesses = []

    if element_coverage >= 0.8:
        strengths.append(f"优秀的知识覆盖 ({element_coverage:.0%} 关键概念)")
    elif element_coverage < 0.4:
        weaknesses.append(f"知识覆盖不足 ({element_coverage:.0%} 关键概念), 缺少: {', '.join(element_misses[:5])}")

    if anti_violations:
        weaknesses.append(f"包含错误内容: {', '.join(anti_violations)}")

    if tool_calls_missed:
        weaknesses.append(f"未调用关键工具: {', '.join(tool_calls_missed)}")

    if len(response) < 100:
        weaknesses.append("回答过于简短")
    elif len(response) > 4000:
        weaknesses.append("回答过于冗长")

    if quality_bonus >= 0.7:
        strengths.append("回答结构化且有数据支撑")

    for crit, (scored, max_pts) in rubric_scores.items():
        if scored >= max_pts * 0.8:
            strengths.append(f"'{crit}' 评分优秀")
        elif scored < max_pts * 0.3:
            weaknesses.append(f"'{crit}' 评分不足")

    return TestResult(
        test_id=test.id,
        dimension=test.dimension,
        difficulty=test.difficulty,
        question=test.question,
        response=response[:2000],  # truncate for storage
        score=final_score,
        max_score=10.0,
        rubric_scores=rubric_scores,
        element_hits=element_hits,
        element_misses=element_misses,
        anti_pattern_violations=anti_violations,
        tool_calls_made=tool_calls_made,
        tool_calls_expected=tool_calls_expected,
        tool_calls_missed=tool_calls_missed,
        strengths=strengths,
        weaknesses=weaknesses,
        response_time_s=response_time_s,
    )


def _estimate_criterion_score(
    criterion: str,
    response: str,
    element_hits: List[str],
    tool_calls: List[str],
    max_pts: float,
) -> float:
    """Heuristically estimate score for a rubric criterion.

    Uses keyword matching, tool call presence, and response structure.
    """
    response_lower = response.lower()
    score = 0.0

    # Tool-related criteria
    if "调用" in criterion and ("tool" in criterion.lower() or "工具" in criterion):
        for tool in ["evaluate_stock_rules", "get_stock_fundamentals",
                      "run_valuation_analysis", "detect_financial_shenanigans"]:
            if tool in " ".join(tool_calls):
                score += max_pts * 0.5
                break
        return min(score, max_pts)

    # Count how many relevant keywords from the criterion appear
    criterion_words = re.findall(r'[\u4e00-\u9fff]+|[a-zA-Z]+', criterion)
    matches = sum(1 for w in criterion_words if w.lower() in response_lower)
    word_coverage = matches / max(len(criterion_words), 1)

    # Also check element hits relevant to this criterion
    relevant_hits = sum(1 for h in element_hits if h.lower() in criterion.lower() or criterion.lower() in h.lower())

    score = max_pts * (0.5 * word_coverage + 0.3 * min(relevant_hits / 2, 1.0))

    # Bonus for structured answers
    if ("解释" in criterion or "分析" in criterion) and len(response) > 300:
        score += max_pts * 0.2

    # Bonus for citations when expected
    if ("引用" in criterion or "名言" in criterion):
        if "「" in response or "——" in response or '"' in response:
            score += max_pts * 0.3

    return min(round(score, 1), max_pts)


# ═══════════════════════════════════════════════════════════════
#  LLM-as-Judge Scorer (when API available)
# ═══════════════════════════════════════════════════════════════


def _parse_judge_json(text: str) -> Optional[Dict[str, Any]]:
    """Robustly parse JSON from LLM judge output.

    Handles common LLM JSON issues: trailing commas, comments,
    markdown code blocks, extra text before/after JSON.
    """
    # Try to extract JSON from markdown code block first
    code_match = re.search(r'```(?:json)?\s*(\{.*?\})\s*```', text, re.DOTALL)
    if code_match:
        text_to_parse = code_match.group(1)
    else:
        # Find outermost { ... }
        json_match = re.search(r'\{.*\}', text, re.DOTALL)
        if not json_match:
            return None
        text_to_parse = json_match.group()

    # Try direct parse first
    try:
        return json.loads(text_to_parse)
    except json.JSONDecodeError:
        pass

    # Clean up common LLM JSON issues
    cleaned = text_to_parse
    # Remove single-line comments (// ...)
    cleaned = re.sub(r'//[^\n]*', '', cleaned)
    # Remove trailing commas before } or ]
    cleaned = re.sub(r',\s*([}\]])', r'\1', cleaned)
    # Fix unquoted keys (simple cases)
    cleaned = re.sub(r'(\{|,)\s*(\w+)\s*:', r'\1 "\2":', cleaned)

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        pass

    # Last resort: try to extract just total_score
    score_match = re.search(r'"total_score"\s*:\s*([\d.]+)', text)
    if score_match:
        score = float(score_match.group(1))
        # Extract strengths and weaknesses lists
        strengths = re.findall(r'"([\u4e00-\u9fff][^"]{5,})"', text)
        return {
            "total_score": score,
            "rubric_scores": {},
            "strengths": strengths[:3],
            "weaknesses": [],
        }

    return None




LLM_JUDGE_PROMPT = """你是一位严格的投资教育评审官。你需要对一个价值投资AI Agent的回答进行评分。

## 测试问题
{question}

## Agent回答
{response}

## 评分标准 (Rubric)
{rubric}

## 期望包含的关键概念
{expected_elements}

## 不应出现的内容 (Anti-patterns)
{anti_patterns}

## 评分要求
请对每个评分标准打分，并给出总体评价。

请严格按以下JSON格式输出：
{{
  "rubric_scores": {{"标准名": {{"score": 分数, "max": 满分, "reason": "原因"}}}},
  "total_score": 总分(0-10),
  "strengths": ["优点1", "优点2"],
  "weaknesses": ["不足1", "不足2"],
  "improvement_suggestions": ["建议1", "建议2"]
}}
"""


async def score_response_llm_judge(
    test: TestCase,
    response: str,
    tool_calls_made: Optional[List[str]] = None,
) -> Optional[TestResult]:
    """Score using LLM-as-Judge pattern. Returns None if LLM unavailable."""
    try:
        from app.agent.llm import is_llm_available, simple_completion

        if not is_llm_available():
            return None

        rubric_text = "\n".join(f"- {k}: {v}分" for k, v in test.rubric.items())
        elements_text = ", ".join(test.expected_elements)
        anti_text = ", ".join(test.anti_patterns) if test.anti_patterns else "无"

        prompt = LLM_JUDGE_PROMPT.format(
            question=test.question,
            response=response[:3000],
            rubric=rubric_text,
            expected_elements=elements_text,
            anti_patterns=anti_text,
        )

        judge_text = await simple_completion(
            messages=[
                {"role": "system", "content": "你是严格的投资知识评审官。按JSON格式输出评分。"},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=1500,
        )

        if judge_text.startswith("[LLM Error:"):
            return None

        # Parse JSON from response — with robust error handling
        judge_data = _parse_judge_json(judge_text)
        if not judge_data:
            return None

        # Build TestResult from LLM judge
        rubric_scores = {}
        for crit, data in judge_data.get("rubric_scores", {}).items():
            rubric_scores[crit] = (data.get("score", 0), data.get("max", 0))

        return TestResult(
            test_id=test.id,
            dimension=test.dimension,
            difficulty=test.difficulty,
            question=test.question,
            response=response[:2000],
            score=judge_data.get("total_score", 5.0),
            max_score=10.0,
            rubric_scores=rubric_scores,
            element_hits=[],
            element_misses=[],
            anti_pattern_violations=[],
            tool_calls_made=tool_calls_made or [],
            tool_calls_expected=test.expected_tool_calls,
            tool_calls_missed=[],
            strengths=judge_data.get("strengths", []),
            weaknesses=judge_data.get("weaknesses", []),
        )
    except Exception as e:
        logger.warning(f"LLM judge failed for {test.id}: {e}")
        return None


# ═══════════════════════════════════════════════════════════════
#  Run Agent & Evaluate
# ═══════════════════════════════════════════════════════════════

async def run_single_test(test: TestCase, use_llm_judge: bool = False) -> TestResult:
    """Run the Agent on a single test case and score the response.

    Uses simple_completion (no tool calling) to avoid HTTP 400 issues
    with proxies. The Agent still demonstrates tool-calling knowledge
    in its textual response, which is evaluated by the rubric.
    """
    from app.agent.llm import is_llm_available, simple_completion
    from app.agent.persona import CHARLIE_SYSTEM_PROMPT

    if not is_llm_available():
        return TestResult(
            test_id=test.id, dimension=test.dimension, difficulty=test.difficulty,
            question=test.question, response="[LLM未配置，无法测试]",
            score=0, max_score=10, rubric_scores={}, element_hits=[], element_misses=[],
            anti_pattern_violations=[], tool_calls_made=[], tool_calls_expected=test.expected_tool_calls,
            tool_calls_missed=test.expected_tool_calls, strengths=[], weaknesses=["LLM未配置"],
        )

    start = time.time()

    # Build messages — add instruction to demonstrate knowledge without actual tool calls
    eval_instruction = (
        "注意：这是一个评估测试。请直接根据你的知识体系回答问题。"
        "请在回答中自然地提及你会使用的工具名称（如 evaluate_stock_rules、get_stock_fundamentals 等），"
        "并给出你的专业分析和明确结论。请控制回答长度在 2000 字以内，确保完整性。"
    )
    messages = [
        {"role": "system", "content": CHARLIE_SYSTEM_PROMPT},
        {"role": "user", "content": f"{test.question}\n\n{eval_instruction}"},
    ]

    try:
        response = await simple_completion(messages, temperature=0.7, max_tokens=8192)
    except Exception as e:
        response = f"[Agent 错误: {str(e)[:200]}]"

    elapsed = time.time() - start

    # Detect tool mentions in response text (Agent describes what tools it would use)
    mentioned_tools = []
    tool_names = [
        "evaluate_stock_rules", "get_stock_fundamentals",
        "run_valuation_analysis", "detect_financial_shenanigans",
        "search_investment_books", "get_stock_news", "research_topic",
    ]
    for tool_name in tool_names:
        if tool_name in response.lower() or tool_name.replace("_", " ") in response.lower():
            mentioned_tools.append(tool_name)

    # Score
    result = score_response_rule_based(test, response, mentioned_tools, elapsed)

    # Optionally refine with LLM judge
    if use_llm_judge:
        llm_result = await score_response_llm_judge(test, response, mentioned_tools)
        if llm_result:
            # Blend scores: 40% rule-based + 60% LLM judge
            result.score = round(result.score * 0.4 + llm_result.score * 0.6, 1)
            result.strengths = list(set(result.strengths + llm_result.strengths))
            result.weaknesses = list(set(result.weaknesses + llm_result.weaknesses))

    return result


async def run_evaluation(
    tests: Optional[List[TestCase]] = None,
    use_llm_judge: bool = False,
    quick: bool = False,
) -> EvaluationReport:
    """Run full evaluation suite and generate report.

    Args:
        tests: Specific tests to run (default: all)
        use_llm_judge: Whether to use LLM for scoring
        quick: If True, use quick eval suite (7 tests, one per dimension)
    """
    from datetime import datetime, timezone

    if quick:
        tests = get_quick_eval_suite()
    elif tests is None:
        tests = ALL_TESTS

    results = []
    for test in tests:
        logger.info(f"Running test {test.id}: {test.question[:50]}...")
        result = await run_single_test(test, use_llm_judge)
        results.append(result)
        logger.info(f"  Score: {result.score}/{result.max_score}")

    # Generate report
    return _build_report(results)


def _build_report(results: List[TestResult]) -> EvaluationReport:
    """Build comprehensive evaluation report from test results."""
    from datetime import datetime, timezone

    if not results:
        return EvaluationReport(
            timestamp=datetime.now(timezone.utc).isoformat(),
            total_tests=0, avg_score=0, overall_grade="F",
            dimension_reports={}, test_results=[],
            top_strengths=[], top_weaknesses=[], improvement_recommendations=[],
        )

    # Overall stats
    avg_score = sum(r.score for r in results) / len(results)
    grade = _score_to_grade(avg_score)

    # Per-dimension analysis
    dim_results: Dict[str, List[TestResult]] = {}
    for r in results:
        dim_results.setdefault(r.dimension, []).append(r)

    dimension_reports = {}
    for dim, dim_tests in dim_results.items():
        dim_avg = sum(r.score for r in dim_tests) / len(dim_tests)
        dim_pass_rate = sum(1 for r in dim_tests if r.score >= 6) / len(dim_tests)

        all_strengths = [s for r in dim_tests for s in r.strengths]
        all_weaknesses = [w for r in dim_tests for w in r.weaknesses]

        # Determine priority
        if dim_avg < 4:
            priority = "critical"
        elif dim_avg < 6:
            priority = "high"
        elif dim_avg < 8:
            priority = "medium"
        else:
            priority = "low"

        dimension_reports[dim] = DimensionReport(
            dimension=dim,
            avg_score=round(dim_avg, 1),
            max_possible=10.0,
            test_count=len(dim_tests),
            pass_rate=round(dim_pass_rate, 2),
            strengths=_deduplicate_list(all_strengths)[:3],
            weaknesses=_deduplicate_list(all_weaknesses)[:3],
            priority=priority,
        )

    # Collect top strengths/weaknesses across all tests
    all_strengths = [s for r in results for s in r.strengths]
    all_weaknesses = [w for r in results for w in r.weaknesses]

    # Generate improvement recommendations
    recommendations = _generate_recommendations(dimension_reports, results)

    return EvaluationReport(
        timestamp=datetime.now(timezone.utc).isoformat(),
        total_tests=len(results),
        avg_score=round(avg_score, 1),
        overall_grade=grade,
        dimension_reports=dimension_reports,
        test_results=results,
        top_strengths=_deduplicate_list(all_strengths)[:5],
        top_weaknesses=_deduplicate_list(all_weaknesses)[:5],
        improvement_recommendations=recommendations,
    )


def _score_to_grade(score: float) -> str:
    """Convert 0-10 score to letter grade."""
    if score >= 9:
        return "A+"
    elif score >= 8:
        return "A"
    elif score >= 7:
        return "B"
    elif score >= 6:
        return "C"
    elif score >= 5:
        return "D"
    else:
        return "F"


def _deduplicate_list(items: List[str]) -> List[str]:
    """Remove near-duplicates from a list while preserving order."""
    seen = set()
    result = []
    for item in items:
        key = item[:50].lower()  # fuzzy dedup
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def _generate_recommendations(
    dim_reports: Dict[str, DimensionReport],
    results: List[TestResult],
) -> List[Dict[str, str]]:
    """Generate actionable improvement recommendations."""
    recommendations = []

    # Sort dimensions by priority
    critical_dims = [d for d, r in dim_reports.items() if r.priority == "critical"]
    high_dims = [d for d, r in dim_reports.items() if r.priority == "high"]

    dim_names = {
        "conceptual": "理论知识",
        "stock_analysis": "个股分析",
        "comparative": "对比分析",
        "risk": "风险识别",
        "edge_case": "边界情况",
        "behavioral": "行为金融",
        "portfolio": "组合构建",
        "master": "大师级压力测试",
    }

    for dim in critical_dims:
        report = dim_reports[dim]
        recommendations.append({
            "priority": "CRITICAL",
            "area": dim_names.get(dim, dim),
            "score": f"{report.avg_score}/10",
            "action": f"需要立即改进{dim_names.get(dim, dim)}能力。",
            "details": "; ".join(report.weaknesses[:3]),
        })

    for dim in high_dims:
        report = dim_reports[dim]
        recommendations.append({
            "priority": "HIGH",
            "area": dim_names.get(dim, dim),
            "score": f"{report.avg_score}/10",
            "action": f"应优先改进{dim_names.get(dim, dim)}能力。",
            "details": "; ".join(report.weaknesses[:3]),
        })

    # Check for common cross-cutting issues
    all_weaknesses = [w for r in results for w in r.weaknesses]
    weakness_text = " ".join(all_weaknesses)

    if "工具" in weakness_text or "tool" in weakness_text.lower():
        recommendations.append({
            "priority": "HIGH",
            "area": "工具调用",
            "action": "Agent 未能主动调用分析工具，需要在 persona 中强化工具使用指令。",
            "details": "建议在 system prompt 中添加更明确的工具使用规则。",
        })

    if "简短" in weakness_text or "不足" in weakness_text:
        recommendations.append({
            "priority": "MEDIUM",
            "area": "回答深度",
            "action": "回答深度不够，需要增强分析的全面性。",
            "details": "建议在 persona 中添加'回答必须包含xxx'的结构化要求。",
        })

    if "引用" in weakness_text or "名言" in weakness_text:
        recommendations.append({
            "priority": "MEDIUM",
            "area": "知识引用",
            "action": "未能充分引用14本书中的知识和名言。",
            "details": "建议在 persona 中扩充经典格言库和书中案例引用。",
        })

    return recommendations


# ═══════════════════════════════════════════════════════════════
#  Report Formatting
# ═══════════════════════════════════════════════════════════════

def format_report(report: EvaluationReport) -> str:
    """Format evaluation report as readable text."""
    lines = [
        "═══════════════════════════════════════════════════",
        "     老查理 Agent 自我评估报告",
        "═══════════════════════════════════════════════════",
        "",
        f"时间: {report.timestamp}",
        f"测试总数: {report.total_tests}",
        f"平均分: {report.avg_score}/10",
        f"总体等级: {report.overall_grade}",
        "",
        "─── 各维度得分 ───",
    ]

    grade_emoji = {"critical": "🔴", "high": "🟠", "medium": "🟡", "low": "🟢"}

    for dim, dr in sorted(report.dimension_reports.items(), key=lambda x: x[1].avg_score):
        emoji = grade_emoji.get(dr.priority, "⚪")
        dim_names = {
            "conceptual": "理论知识", "stock_analysis": "个股分析",
            "comparative": "对比分析", "risk": "风险识别",
            "edge_case": "边界情况", "behavioral": "行为金融",
            "portfolio": "组合构建", "master": "大师级压力测试",
        }
        name = dim_names.get(dim, dim)
        lines.append(
            f"  {emoji} {name}: {dr.avg_score}/10 "
            f"(通过率: {dr.pass_rate:.0%}, 优先级: {dr.priority})"
        )
        for w in dr.weaknesses[:2]:
            lines.append(f"     ✗ {w}")

    lines.extend(["", "─── 总体优势 ───"])
    for s in report.top_strengths[:5]:
        lines.append(f"  ✓ {s}")

    lines.extend(["", "─── 总体弱点 ───"])
    for w in report.top_weaknesses[:5]:
        lines.append(f"  ✗ {w}")

    lines.extend(["", "─── 改进建议 ───"])
    for rec in report.improvement_recommendations:
        lines.append(f"  [{rec['priority']}] {rec['area']}: {rec['action']}")
        if rec.get("details"):
            lines.append(f"     → {rec['details']}")

    lines.extend([
        "",
        "═══════════════════════════════════════════════════",
    ])

    return "\n".join(lines)


def report_to_dict(report: EvaluationReport) -> Dict[str, Any]:
    """Convert report to serializable dict."""
    return {
        "timestamp": report.timestamp,
        "total_tests": report.total_tests,
        "avg_score": report.avg_score,
        "overall_grade": report.overall_grade,
        "dimensions": {
            k: {
                "avg_score": v.avg_score,
                "pass_rate": v.pass_rate,
                "test_count": v.test_count,
                "priority": v.priority,
                "strengths": v.strengths,
                "weaknesses": v.weaknesses,
            }
            for k, v in report.dimension_reports.items()
        },
        "test_results": [
            {
                "test_id": r.test_id,
                "dimension": r.dimension,
                "score": r.score,
                "strengths": r.strengths,
                "weaknesses": r.weaknesses,
                "element_hits": r.element_hits,
                "element_misses": r.element_misses,
                "tool_calls_missed": r.tool_calls_missed,
            }
            for r in report.test_results
        ],
        "top_strengths": report.top_strengths,
        "top_weaknesses": report.top_weaknesses,
        "recommendations": report.improvement_recommendations,
    }
