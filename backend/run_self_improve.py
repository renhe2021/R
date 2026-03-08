"""Run self-improvement loop: evaluate Agent → analyze weaknesses → improve persona → repeat.

Usage:
    python run_self_improve.py              # Quick eval (7 tests, 1 per dimension)
    python run_self_improve.py --full       # Full eval (36 tests)
    python run_self_improve.py --apply      # Actually apply persona patches (default: dry-run)
"""

import asyncio
import json
import logging
import sys
import os
import time

# Ensure backend is on path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("self_improve")


async def run_single_test_with_report(test, iteration: int):
    """Run a single test and print detailed results."""
    from app.agent.evaluator import run_single_test

    logger.info(f"  [{test.id}] {test.question[:60]}...")
    start = time.time()
    result = await run_single_test(test, use_llm_judge=True)
    elapsed = time.time() - start

    # Score emoji
    if result.score >= 8:
        emoji = "🟢"
    elif result.score >= 6:
        emoji = "🟡"
    elif result.score >= 4:
        emoji = "🟠"
    else:
        emoji = "🔴"

    print(f"\n  {emoji} [{test.id}] {test.dimension} ({test.difficulty}) — {result.score}/10  [{elapsed:.1f}s]")
    print(f"     Q: {test.question[:80]}")

    if result.response and result.response != "[LLM未配置，无法测试]":
        # Show first 200 chars of response
        resp_preview = result.response[:200].replace("\n", " ")
        print(f"     A: {resp_preview}...")
    else:
        print(f"     A: {result.response}")

    if result.element_hits:
        print(f"     ✓ 命中概念: {', '.join(result.element_hits[:8])}")
    if result.element_misses:
        print(f"     ✗ 缺失概念: {', '.join(result.element_misses[:8])}")
    if result.anti_pattern_violations:
        print(f"     ⚠ 违规内容: {', '.join(result.anti_pattern_violations)}")
    if result.tool_calls_made:
        print(f"     🔧 调用工具: {', '.join(result.tool_calls_made)}")
    if result.tool_calls_missed:
        print(f"     ✗ 未调用: {', '.join(result.tool_calls_missed)}")

    for s in result.strengths[:2]:
        print(f"     ✓ {s}")
    for w in result.weaknesses[:2]:
        print(f"     ✗ {w}")

    return result


async def run_evaluation_round(iteration: int, quick: bool = True, use_llm_judge: bool = True):
    """Run one evaluation round with detailed output."""
    from app.agent.eval_suite import ALL_TESTS, get_quick_eval_suite
    from app.agent.evaluator import _build_report, format_report

    tests = get_quick_eval_suite() if quick else ALL_TESTS
    mode = "Quick" if quick else "Full"

    print(f"\n{'='*60}")
    print(f"  第 {iteration} 轮评估 ({mode} 模式, {len(tests)} 题)")
    print(f"{'='*60}")

    results = []
    for test in tests:
        result = await run_single_test_with_report(test, iteration)
        results.append(result)

    report = _build_report(results)

    # Print summary
    print(f"\n{'─'*60}")
    print(format_report(report))

    return report


async def analyze_and_improve(report, dry_run: bool = True):
    """Analyze weaknesses and generate improvements."""
    from app.agent.self_improve import analyze_weaknesses, generate_persona_patches, apply_persona_improvements

    print(f"\n{'='*60}")
    print(f"  弱点分析 & 改进建议")
    print(f"{'='*60}")

    actions = analyze_weaknesses(report)
    if not actions:
        print("  没有发现需要改进的地方！Agent 表现优秀。")
        return None

    print(f"\n  发现 {len(actions)} 个改进点:")
    for a in actions:
        print(f"    [{a['priority']}] {a['description']}")
        if a.get("weaknesses"):
            for w in a["weaknesses"][:2]:
                print(f"       → {w}")

    patches = generate_persona_patches(report, actions)
    if patches:
        print(f"\n  生成了 {len(patches)} 个 Persona 补丁:")
        for p in patches:
            print(f"    • {p['id']}: {p['description']}")

        result = apply_persona_improvements(patches, dry_run=dry_run)
        print(f"\n  应用结果:")
        print(f"    Applied: {result.get('applied', [])}")
        print(f"    Skipped: {result.get('skipped', [])}")
        print(f"    Dry run: {result.get('dry_run', True)}")
        print(f"    Prompt length: {result.get('original_prompt_length')} → {result.get('new_prompt_length')}")
        return result
    else:
        print("  无需生成补丁。")
        return None


async def run_llm_judge_improvement(report):
    """Use LLM to generate targeted persona improvements based on evaluation results."""
    from app.agent.llm import is_llm_available, simple_completion

    if not is_llm_available():
        return None

    # Build a summary of weaknesses for the LLM
    weak_dims = []
    for dim, dr in report.dimension_reports.items():
        if dr.avg_score < 7.0:
            weak_dims.append({
                "dimension": dim,
                "score": dr.avg_score,
                "weaknesses": dr.weaknesses,
                "priority": dr.priority,
            })

    if not weak_dims:
        return None

    # Get the worst test results
    worst_tests = sorted(report.test_results, key=lambda r: r.score)[:5]
    worst_details = []
    for t in worst_tests:
        worst_details.append({
            "id": t.test_id,
            "question": t.question[:100],
            "score": t.score,
            "misses": t.element_misses[:5],
            "weaknesses": t.weaknesses[:3],
        })

    prompt = f"""你是一个 AI Agent 自我改进专家。

以下是一个价值投资 Agent「老查理」的评估结果：
- 总体得分: {report.avg_score}/10
- 总体等级: {report.overall_grade}

## 薄弱维度
{json.dumps(weak_dims, ensure_ascii=False, indent=2)}

## 最差的测试
{json.dumps(worst_details, ensure_ascii=False, indent=2)}

## 任务
请针对以上弱点，生成需要添加到 Agent 的 System Prompt 中的**具体改进内容**。
要求：
1. 每条改进都是可以直接粘贴到 prompt 中的文本
2. 用中文
3. 要包含具体的投资知识、公式、案例
4. 不要泛泛而谈，要精确和专业

请输出 JSON 格式：
[
  {{"area": "改进领域", "content": "要添加到prompt中的具体文本", "reason": "为什么要加这个"}}
]
"""

    try:
        text = await simple_completion(
            messages=[
                {"role": "system", "content": "你是 AI Agent 优化专家，专注于投资领域。严格输出合法 JSON 数组，不要添加注释或多余文本。"},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=4000,
        )
        if text.startswith("[LLM Error:"):
            return None
        # Extract JSON — handle markdown code blocks and raw JSON
        import re
        # Try markdown code block first
        code_match = re.search(r'```(?:json)?\s*(\[.*?\])\s*```', text, re.DOTALL)
        if code_match:
            raw_json = code_match.group(1)
        else:
            json_match = re.search(r'\[.*\]', text, re.DOTALL)
            raw_json = json_match.group() if json_match else None

        if raw_json:
            # Clean up common LLM JSON issues
            raw_json = raw_json.replace('\n', ' ').replace('\r', '')
            # Remove trailing commas before ] or }
            raw_json = re.sub(r',\s*([}\]])', r'\1', raw_json)
            improvements = json.loads(raw_json)
            return improvements
    except Exception as e:
        logger.warning(f"LLM improvement generation failed: {e}")
        # Log the raw text for debugging
        if 'text' in locals():
            logger.debug(f"Raw LLM output: {text[:500]}")
    return None


async def apply_llm_improvements(improvements: list):
    """Apply LLM-generated improvements to persona.

    Strategy: Replace the '第十章：精炼执行补充规则（自动生成）' section
    instead of appending new blocks. This prevents persona bloat.
    """
    from app.agent.persona import CHARLIE_SYSTEM_PROMPT
    from pathlib import Path
    from datetime import datetime, timezone

    if not improvements:
        return

    print(f"\n  LLM 生成了 {len(improvements)} 条改进:")
    for i, imp in enumerate(improvements):
        print(f"    {i+1}. [{imp.get('area', '?')}] {imp.get('reason', '')[:60]}")

    # Build additions — filter out content already present
    additions = []
    for imp in improvements:
        content = imp.get("content", "")
        if content and content.strip() not in CHARLIE_SYSTEM_PROMPT:
            additions.append({"area": imp.get("area", "通用"), "content": content.strip()})

    if not additions:
        print("  所有改进已经存在于 persona 中，跳过。")
        return

    # Strategy: merge new improvements into 第十章 (replace the auto-generated section)
    # Find the 第十章 marker and everything after it until end
    marker = "═══════════════════════════════════════════\n 第十章：精炼执行补充规则（自动生成）"
    marker_idx = CHARLIE_SYSTEM_PROMPT.find("第十章：精炼执行补充规则")

    if marker_idx > 0:
        # Find the ═══ line before 第十章
        pre_marker = CHARLIE_SYSTEM_PROMPT.rfind("═══════════════════════════════════════════", 0, marker_idx)
        if pre_marker > 0:
            base_prompt = CHARLIE_SYSTEM_PROMPT[:pre_marker].rstrip()
        else:
            base_prompt = CHARLIE_SYSTEM_PROMPT[:marker_idx].rstrip()

        # Extract existing 第十章 content to merge with new improvements
        old_section = CHARLIE_SYSTEM_PROMPT[marker_idx:]
    else:
        base_prompt = CHARLIE_SYSTEM_PROMPT.rstrip()
        old_section = ""

    # Build new 第十章 section: keep old content + merge new
    new_section = "\n\n═══════════════════════════════════════════\n"
    new_section += " 第十章：精炼执行补充规则（自动生成）\n"
    new_section += "═══════════════════════════════════════════\n\n"

    # Keep the old subsections that still exist
    if old_section:
        # Extract content after the header
        header_end = old_section.find("\n═══════════════════════════════════════════\n")
        if header_end < 0:
            # Single section, take everything after the header line
            lines = old_section.split("\n", 2)
            if len(lines) > 2:
                new_section += lines[2].strip() + "\n\n"
        else:
            content_part = old_section[old_section.find("\n")+1:]
            new_section += content_part.strip() + "\n\n"

    # Add new improvements
    new_section += f"### 本轮新增改进 ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})\n\n"
    for add in additions:
        new_section += add["content"] + "\n\n"

    new_prompt = base_prompt + new_section.rstrip() + "\n"

    # Safety check: don't let persona grow too much (> 30000 chars = warning)
    if len(new_prompt) > 30000:
        print(f"  ⚠️ Persona 过长 ({len(new_prompt)} chars)，截断新增内容...")
        # Keep only base + old section, skip new additions
        new_prompt = base_prompt
        if old_section:
            new_prompt += "\n\n═══════════════════════════════════════════\n"
            new_prompt += " 第十章：精炼执行补充规则（自动生成）\n"
            new_prompt += "═══════════════════════════════════════════\n\n"
            lines = old_section.split("\n", 2)
            if len(lines) > 2:
                new_prompt += lines[2].strip()
        new_prompt += "\n"

    persona_path = Path(__file__).resolve().parent / "app" / "agent" / "persona.py"
    file_content = f'''"""Old Charlie persona — Deep Value Investing Agent system prompt.

Knowledge distilled from 14 classic investment books, 1,253 strategies extracted.
Organized into 7 investment schools with 65+ quantitative screening rules.
Auto-improved via self-evaluation at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}.
"""

CHARLIE_SYSTEM_PROMPT = """{new_prompt}"""
'''
    persona_path.write_text(file_content, encoding="utf-8")
    print(f"  ✅ 已将 {len(additions)} 条改进合并到第十章 (总长: {len(new_prompt)} chars)")


async def main():
    args = sys.argv[1:]
    quick = "--full" not in args
    dry_run = "--apply" not in args
    max_rounds = 3

    from app.agent.llm import is_llm_available
    if not is_llm_available():
        print("❌ LLM 未配置！请确保 backend/.env 中设置了 LLM_API_KEY")
        print("   当前配置路径: backend/.env")
        return

    print("╔══════════════════════════════════════════════════════╗")
    print("║    老查理 Agent 自我提升循环                          ║")
    print("║    Reflexion Pattern: Evaluate → Analyze → Improve   ║")
    print("╚══════════════════════════════════════════════════════╝")
    print(f"\n  模式: {'Quick (8题)' if quick else 'Full (36题)'}")
    print(f"  干跑: {'是 (不修改文件)' if dry_run else '否 (会修改 persona.py)'}")
    print(f"  最大轮数: {max_rounds}")

    best_score = 0
    target_score = 8.0

    for round_num in range(1, max_rounds + 1):
        # Step 1: Evaluate
        report = await run_evaluation_round(round_num, quick=quick)

        current_score = report.avg_score

        # Step 2: Check if we reached target
        if current_score >= target_score:
            best_score = max(best_score, current_score)
            print(f"\n  目标分数 {target_score} 已达成！(当前: {current_score})")
            break

        # Check for no improvement (only after round 1, and only if we've done improvements)
        if round_num > 1 and current_score <= best_score - 0.5:
            print(f"\n  未检测到改进 (当前: {current_score}, 之前最佳: {best_score})，停止迭代。")
            best_score = max(best_score, current_score)
            break

        best_score = max(best_score, current_score)

        # Step 3: Analyze and improve
        # First: rule-based patches
        improve_result = await analyze_and_improve(report, dry_run=dry_run)

        # Second: LLM-generated improvements (more targeted)
        if not dry_run:
            print(f"\n{'─'*60}")
            print("  使用 LLM 生成针对性改进...")
            llm_improvements = await run_llm_judge_improvement(report)
            if llm_improvements:
                await apply_llm_improvements(llm_improvements)
                # Reload persona module
                import importlib
                import app.agent.persona
                importlib.reload(app.agent.persona)
                print("  ✅ Persona 模块已重新加载")
            else:
                print("  ℹ️ LLM 未生成额外改进")

        if round_num < max_rounds:
            print(f"\n  ⏳ 准备第 {round_num + 1} 轮评估...")

    # Final summary
    print(f"\n{'='*60}")
    print(f"  自我提升完成！")
    print(f"  最终得分: {best_score}/10")
    print(f"  完成轮数: {round_num}")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())
