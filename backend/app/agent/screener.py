"""Stage 1 Screener — Multi-school value investing screening.

Upgraded from 5 hardcoded rules to 65+ distilled rules across 7 investment schools.
No LLM required — pure code evaluation using StockData.

Two modes:
1. Quick screen (default): Basic Graham-Buffett eliminatory rules for fast filtering
2. Full screen: All 7 schools evaluated for comprehensive scoring
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional

from app.agent.distilled_rules import (
    SCHOOLS, ALL_RULES, InvestmentRule,
    evaluate_stock_against_school, evaluate_stock_all_schools,
)
from app.agent.investment_params import params as _P

logger = logging.getLogger(__name__)


def _build_eliminatory_rules():
    """Build eliminatory rules dynamically from current params."""
    return [
        {"name": f"PE合理 (PE < {_P.get('screener.pe_max', 25)} 且 > 0)",
         "check": lambda d, _mx=_P.get("screener.pe_max", 25): d.get("pe") is not None and 0 < d["pe"] < _mx},
        {"name": "正盈利 (EPS > 0)",
         "check": lambda d: d.get("eps") is not None and d["eps"] > 0},
        {"name": f"负债可控 (D/E < {_P.get('screener.debt_to_equity_max', 2.0)})",
         "check": lambda d, _mx=_P.get("screener.debt_to_equity_max", 2.0): d.get("debt_to_equity") is None or d["debt_to_equity"] < _mx},
        {"name": f"市值 > ${_P.get('screener.market_cap_min', 5e8)/1e8:.0f}亿",
         "check": lambda d, _mn=_P.get("screener.market_cap_min", 5e8): d.get("market_cap") is not None and d["market_cap"] >= _mn},
        {"name": "ROE > 0",
         "check": lambda d: d.get("roe") is None or d["roe"] > 0},
    ]


async def run_screening(
    stocks: List[str],
    data_router=None,
    source_pref: Optional[str] = None,
    mode: str = "quick",
) -> Dict[str, Any]:
    """Screen a list of stocks through value investing criteria.

    Args:
        stocks: List of ticker symbols
        data_router: Optional (unused, kept for interface compat)
        source_pref: Preferred data source (unused)
        mode: "quick" (eliminatory only) or "full" (all 7 schools)

    Returns:
        Dict with screening results including school evaluations in full mode.
    """
    passed = []
    eliminated = []
    school_evaluations = {}

    def _screen_one(symbol: str) -> Dict[str, Any]:
        try:
            from src.data_providers.factory import get_data_provider
            from src.symbol_resolver import resolve_for_provider, resolve_symbol
            from app.agent.tools import get_active_data_source

            resolved = resolve_symbol(symbol)
            display = resolved.canonical

            # Use the active data source (Bloomberg if available, else auto)
            active_source = get_active_data_source()
            provider = get_data_provider(active_source)
            provider_symbol = resolve_for_provider(symbol, provider.name)
            stock = provider.fetch(provider_symbol)

            if not stock.is_valid():
                return {"symbol": display, "status": "eliminated", "reason": "无法获取有效数据"}

            data = stock.to_dict()

            # Quick screen: eliminatory rules
            ELIMINATORY_RULES = _build_eliminatory_rules()
            failures = []
            for rule in ELIMINATORY_RULES:
                try:
                    if not rule["check"](data):
                        failures.append(rule["name"])
                except Exception:
                    pass

            if failures:
                return {
                    "symbol": display,
                    "status": "eliminated",
                    "reason": "未通过基本面门槛: " + ", ".join(failures),
                }

            result = {"symbol": display, "status": "passed", "data": data}

            # Full mode: evaluate against all schools
            if mode == "full":
                school_eval = evaluate_stock_all_schools(data)
                result["school_evaluation"] = school_eval
                result["best_school"] = school_eval.get("best_fit_school")
                result["strong_schools"] = school_eval.get("strong_pass_schools", [])

                # In full mode, reject if ALL schools reject
                reject_count = len(school_eval.get("reject_schools", []))
                if reject_count == len(SCHOOLS):
                    return {
                        "symbol": display,
                        "status": "eliminated",
                        "reason": "所有7大投资流派均判定不合格",
                        "school_evaluation": school_eval,
                    }

            return result

        except Exception as e:
            return {"symbol": symbol.upper(), "status": "eliminated", "reason": f"数据获取失败: {str(e)[:100]}"}

    loop = asyncio.get_event_loop()
    results = await asyncio.gather(
        *[loop.run_in_executor(None, _screen_one, s) for s in stocks]
    )

    for r in results:
        if r["status"] == "passed":
            passed.append(r["symbol"])
            if "school_evaluation" in r:
                school_evaluations[r["symbol"]] = r["school_evaluation"]
        else:
            eliminated.append(r)

    # Build criteria description
    if mode == "full":
        criteria = [f"7大投资流派 ({len(ALL_RULES)} 条规则)"] + [
            f"{s.name_cn}: {len(s.rules)} 条" for s in SCHOOLS.values()
        ]
    else:
        criteria = [r["name"] for r in _build_eliminatory_rules()]

    return {
        "passed": passed,
        "eliminated": eliminated,
        "totalInput": len(stocks),
        "totalPassed": len(passed),
        "totalEliminated": len(eliminated),
        "criteriaUsed": criteria,
        "mode": mode,
        "school_evaluations": school_evaluations if school_evaluations else None,
    }
