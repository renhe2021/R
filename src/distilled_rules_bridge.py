"""蒸馏规则桥接模块

将 backend/app/agent/distilled_rules.py 中的评估逻辑桥接到 src/ 包中，
使 StockEngine 无需依赖 backend 包即可调用七流派评估。
"""

import sys
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# 确保 backend 可导入
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_BACKEND_DIR = _PROJECT_ROOT / "backend"
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))


def _get_distilled():
    """延迟导入蒸馏规则模块"""
    try:
        from backend.app.agent.distilled_rules import (
            evaluate_stock_all_schools,
            evaluate_stock_against_school,
            SCHOOLS,
        )
        return evaluate_stock_all_schools, evaluate_stock_against_school, SCHOOLS
    except ImportError:
        # 如果 backend 包结构不匹配，尝试直接路径导入
        try:
            from app.agent.distilled_rules import (
                evaluate_stock_all_schools,
                evaluate_stock_against_school,
                SCHOOLS,
            )
            return evaluate_stock_all_schools, evaluate_stock_against_school, SCHOOLS
        except ImportError:
            logger.warning("无法导入蒸馏规则模块，将使用内置精简规则")
            return None, None, None


def evaluate_all_schools(stock_data: Dict[str, Any]) -> Dict[str, Any]:
    """对股票数据执行七流派全评估。

    Args:
        stock_data: StockData.to_dict() 的输出

    Returns:
        包含 schools, best_fit_school, overall_score 等字段的字典
    """
    fn_all, fn_one, schools = _get_distilled()

    if fn_all is not None:
        return fn_all(stock_data)

    # 降级: 使用内置精简规则
    return _fallback_evaluate(stock_data)


def evaluate_one_school(stock_data: Dict[str, Any], school_name: str) -> Dict[str, Any]:
    """对股票数据执行单个流派评估。"""
    fn_all, fn_one, schools = _get_distilled()

    if fn_one is not None:
        return fn_one(stock_data, school_name)

    # 降级
    result = _fallback_evaluate(stock_data)
    return result.get("schools", {}).get(school_name, {"error": f"Unknown school: {school_name}"})


def get_available_schools() -> List[str]:
    """获取可用的流派列表"""
    _, _, schools = _get_distilled()
    if schools:
        return list(schools.keys())
    return ["graham", "buffett", "quantitative", "quality", "valuation", "contrarian", "garp"]


def get_school_info(school_name: str) -> Optional[Dict[str, str]]:
    """获取流派基本信息"""
    _, _, schools = _get_distilled()
    if schools and school_name in schools:
        s = schools[school_name]
        return {
            "name": s.name,
            "name_cn": s.name_cn,
            "description": s.description,
            "philosophy": s.philosophy,
            "key_figures": s.key_figures,
            "rule_count": len(s.rules),
        }
    return None


# ═══════════════════════════════════════════════════════════════
#  降级方案: 内置精简规则（当 backend 不可用时）
# ═══════════════════════════════════════════════════════════════

_FALLBACK_RULES = {
    "graham": {
        "name": "Graham Deep Value",
        "name_cn": "格雷厄姆深度价值",
        "philosophy": "安全边际是投资的基石",
        "rules": [
            ("pe is not None and pe > 0 and pe < 15", "PE < 15", 1.5, True),
            ("pb is not None and pb > 0 and pe is not None and pe > 0 and pe * pb < 22.5", "PE×PB < 22.5", 1.5, False),
            ("current_ratio is not None and current_ratio >= 2.0", "流动比率 ≥ 2", 1.2, False),
            ("debt_to_equity is not None and debt_to_equity < 1.0", "负债权益比 < 1", 1.2, True),
            ("market_cap is not None and market_cap >= 1e9", "市值 ≥ 10亿", 0.8, False),
        ],
        "min_pass_rate": 0.5,
    },
    "buffett": {
        "name": "Buffett Quality Moat",
        "name_cn": "巴菲特护城河投资",
        "philosophy": "以合理的价格买入一家伟大的公司",
        "rules": [
            ("roe is not None and roe > 0.15", "ROE > 15%", 2.0, True),
            ("profit_margin is not None and profit_margin > 0.10", "净利润率 > 10%", 1.5, False),
            ("debt_to_equity is not None and debt_to_equity < 0.5", "负债权益比 < 0.5", 1.5, False),
            ("free_cash_flow is not None and free_cash_flow > 0", "自由现金流为正", 1.8, True),
            ("operating_margin is not None and operating_margin > 0.15", "营业利润率 > 15%", 1.3, False),
        ],
        "min_pass_rate": 0.6,
    },
    "quantitative": {
        "name": "Quantitative Value",
        "name_cn": "量化价值",
        "philosophy": "系统化投资消除人类认知偏差",
        "rules": [
            ("earnings_yield is not None and earnings_yield > 0.08", "盈利收益率 > 8%", 2.0, True),
            ("roe is not None and roe > 0.15", "ROE > 15%", 1.5, False),
            ("free_cash_flow is not None and net_income is not None and net_income > 0 and free_cash_flow > net_income",
             "FCF > 净利润", 1.8, False),
            ("market_cap is not None and market_cap >= 1e8", "市值 > 1亿", 0.8, False),
            ("ps is not None and ps < 1.5", "P/S < 1.5", 1.3, False),
        ],
        "min_pass_rate": 0.5,
    },
    "quality": {
        "name": "Quality Investing",
        "name_cn": "品质投资",
        "philosophy": "时间是优质企业的朋友",
        "rules": [
            ("roe is not None and roe > 0.15", "ROE > 15%", 2.0, True),
            ("operating_margin is not None and operating_margin > 0.15", "营业利润率 > 15%", 1.5, False),
            ("free_cash_flow is not None and free_cash_flow > 0", "自由现金流为正", 1.5, True),
            ("debt_to_equity is not None and debt_to_equity < 0.5", "低财务杠杆", 1.3, False),
            ("pe is not None and pe > 0 and pe < 25", "PE < 25", 1.0, False),
        ],
        "min_pass_rate": 0.6,
    },
    "valuation": {
        "name": "Damodaran Valuation",
        "name_cn": "达摩达兰估值派",
        "philosophy": "估值既是科学也是艺术",
        "rules": [
            ("forward_pe is not None and pe is not None and forward_pe > 0 and forward_pe < pe",
             "前瞻PE < 当前PE", 1.3, False),
            ("operating_margin is not None and operating_margin > 0.10", "营业利润率 > 10%", 1.2, False),
            ("free_cash_flow is not None and free_cash_flow > 0", "自由现金流为正", 1.5, False),
        ],
        "min_pass_rate": 0.5,
    },
    "contrarian": {
        "name": "Contrarian Value",
        "name_cn": "逆向价值",
        "philosophy": "别人恐惧时我贪婪",
        "rules": [
            ("price is not None and price_52w_high is not None and price < price_52w_high * 0.70",
             "股价低于52周高点30%+", 1.5, False),
            ("pe is not None and pe > 0 and pe < 10", "PE < 10", 1.5, False),
            ("ps is not None and ps < 0.75", "P/S < 0.75", 1.5, False),
            ("dividend_yield is not None and dividend_yield > 0.04", "股息率 > 4%", 1.3, False),
            ("roe is not None and roe > 0.08 and free_cash_flow is not None and free_cash_flow > 0",
             "基本面健康", 1.5, False),
        ],
        "min_pass_rate": 0.6,
    },
    "garp": {
        "name": "GARP",
        "name_cn": "合理价格成长",
        "philosophy": "既要成长，也要价值",
        "rules": [
            ("eps_growth_rate is not None and eps_growth_rate > 0.10", "EPS增长 > 10%", 1.5, False),
            ("pe is not None and pe > 0 and pe < 20", "PE < 20", 1.3, False),
            ("roe is not None and roe > 0.15", "ROE > 15%", 1.5, False),
            ("debt_to_equity is not None and debt_to_equity < 0.8", "低负债", 1.0, False),
        ],
        "min_pass_rate": 0.5,
    },
}


def _fallback_evaluate(stock_data: Dict[str, Any]) -> Dict[str, Any]:
    """使用精简内置规则评估（当 backend 模块不可用时）"""
    results = {}
    best_school = None
    best_score = -1

    for school_key, school_def in _FALLBACK_RULES.items():
        passed = []
        failed = []
        skipped = []
        score = 0.0
        max_score = 0.0
        has_eliminatory_fail = False

        for expr, name, weight, is_elim in school_def["rules"]:
            max_score += weight
            try:
                result = eval(expr, {"__builtins__": {}}, stock_data)
                if result:
                    passed.append({"rule": name, "weight": weight})
                    score += weight
                else:
                    failed.append({"rule": name, "weight": weight, "is_eliminatory": is_elim})
                    if is_elim:
                        has_eliminatory_fail = True
            except Exception:
                skipped.append({"rule": name, "reason": "数据缺失或计算错误"})

        evaluated = len(passed) + len(failed)
        pass_rate = len(passed) / evaluated if evaluated > 0 else 0
        min_pr = school_def["min_pass_rate"]

        if has_eliminatory_fail:
            recommendation = "REJECT"
            verdict_cn = "不合格（淘汰性指标未通过）"
        elif pass_rate >= min_pr and score >= max_score * 0.6:
            recommendation = "STRONG_PASS"
            verdict_cn = "优秀"
        elif pass_rate >= min_pr * 0.8:
            recommendation = "PASS"
            verdict_cn = "合格"
        elif pass_rate >= 0.3:
            recommendation = "MARGINAL"
            verdict_cn = "边缘"
        else:
            recommendation = "FAIL"
            verdict_cn = "不合格"

        result_data = {
            "school": school_def["name"],
            "school_cn": school_def["name_cn"],
            "philosophy": school_def["philosophy"],
            "passed": passed,
            "failed": failed,
            "skipped": skipped,
            "score": round(score, 1),
            "max_score": round(max_score, 1),
            "pass_rate": round(pass_rate, 3),
            "total_rules": len(school_def["rules"]),
            "evaluated": evaluated,
            "recommendation": recommendation,
            "verdict_cn": verdict_cn,
        }
        results[school_key] = result_data

        if score > best_score and recommendation not in ("REJECT", "FAIL"):
            best_score = score
            best_school = school_key

    strong_pass = [k for k, v in results.items() if v["recommendation"] == "STRONG_PASS"]
    passes = [k for k, v in results.items() if v["recommendation"] == "PASS"]
    rejects = [k for k, v in results.items() if v["recommendation"] == "REJECT"]

    return {
        "schools": results,
        "best_fit_school": best_school,
        "strong_pass_schools": strong_pass,
        "pass_schools": passes,
        "reject_schools": rejects,
        "overall_score": round(sum(v["score"] for v in results.values()), 1),
        "overall_max": round(sum(v["max_score"] for v in results.values()), 1),
    }
