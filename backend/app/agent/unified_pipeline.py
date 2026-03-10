"""R-System Unified Pipeline v2.0 — 八阶段系统化价值投资分析引擎

将项目中三条割裂的管线 (analyzer.py / graph.py / screening_pipeline.py)
统一为一条完整的专业分析流水线，最大化利用全部知识资产。

八阶段流程:
  Stage 1: Universe Construction   — 构建股票池
  Stage 2: Data Acquisition        — 数据采集 & 质量门控
  Stage 3: Hard Knockout Gate      — Graham 硬性门槛淘汰
  Stage 4: Financial Forensics     — 三重排雷 + Schilit 七大诡计
  Stage 5: Multi-School Consensus  — 七流派 65+ 规则共识评估
  Stage 6: Multi-Model Valuation   — 7 种估值模型 + 安全边际
  Stage 7: LLM-Enhanced Analysis   — 知识库 + Perplexity + 老查理深度分析
  Stage 8: Conviction Ranking      — 信念排名 + 仓位分配 + 持久化

知识资产:
  14 本投资经典 → 1,253 策略 → 65+ 蒸馏规则 → 7 大流派
  7 种估值模型 → 3 重排雷 → Perplexity 实时研究 → 老查理 persona
"""

import asyncio
import json
import logging
import math
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple

from app.agent.investment_params import params as _P

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  Enums & Data Structures
# ═══════════════════════════════════════════════════════════════

class StageStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    SKIPPED = "skipped"
    FAILED = "failed"


class Verdict(str, Enum):
    STRONG_BUY = "STRONG_BUY"
    BUY = "BUY"
    HOLD = "HOLD"
    AVOID = "AVOID"
    REJECT = "REJECT"


class ConvictionLevel(str, Enum):
    HIGHEST = "HIGHEST"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    NONE = "NONE"


class RiskTier(str, Enum):
    FORTRESS = "FORTRESS"
    SOLID = "SOLID"
    NEUTRAL = "NEUTRAL"
    FRAGILE = "FRAGILE"
    DANGER = "DANGER"


@dataclass
class StockResult:
    """A single stock's complete analysis result across all 8 stages."""
    symbol: str
    name: str = ""
    sector: str = ""
    industry: str = ""
    price: Optional[float] = None

    # Stage 2: Data quality
    data_quality: float = 0.0  # 0-100
    data_source: str = ""

    # Stage 3: Knockout gate
    gate_passed: bool = True
    gate_failures: List[str] = field(default_factory=list)
    eliminated_at_stage: int = 0  # 0 = not eliminated

    # Stage 4: Forensics
    f_score: int = 0
    f_score_details: List[str] = field(default_factory=list)
    z_score: Optional[float] = None
    z_zone: str = ""
    m_score: Optional[float] = None
    red_flags: List[Dict[str, str]] = field(default_factory=list)
    risk_tier: str = "NEUTRAL"
    forensics_verdict: str = "PASS"

    # Stage 5: Multi-school
    school_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    best_school: str = ""
    strong_schools: List[str] = field(default_factory=list)
    school_consensus_score: float = 0.0

    # Stage 6: Valuation
    valuations: Dict[str, float] = field(default_factory=dict)
    intrinsic_value: Optional[float] = None
    margin_of_safety: Optional[float] = None
    moat: str = "None"

    # Stage 6.5: Filing analysis
    filing_summary: str = ""
    has_sec_filing: bool = False
    has_financial_statements: bool = False

    # Stage 7: LLM analysis
    knowledge_snippets: List[str] = field(default_factory=list)
    realtime_research: str = ""
    llm_analysis: str = ""
    per_school_opinions: Dict[str, str] = field(default_factory=dict)  # school -> opinion text
    comparative_matrix: str = ""  # Cross-stock comparison text

    # Stage 7 (NEW): Investment Committee debate
    debate_record: Optional[Dict[str, Any]] = None  # Full DebateRecord.to_dict()
    committee_verdict: str = ""      # Final verdict from committee (STRONG_BUY/BUY/HOLD/AVOID)
    committee_confidence: float = 0.0
    committee_vote_tally: Dict[str, int] = field(default_factory=dict)
    committee_veto_triggered: bool = False

    # Stage 9 (NEW): Strategy backtest
    strategy_backtest: Optional[Dict[str, Any]] = None  # BacktestResult.to_dict()

    # Stage 8: Final
    composite_score: float = 0.0
    conviction: str = "NONE"
    verdict: str = "HOLD"
    position_weight: float = 0.0
    selection_reasons: List[str] = field(default_factory=list)

    # Timing signals (new)
    rsi_14d: Optional[float] = None
    macd_signal_str: str = ""  # "BULLISH" / "BEARISH" / "NEUTRAL"
    ma200_position: str = ""  # "ABOVE" / "BELOW"
    week52_position: Optional[float] = None  # 0-100, position within 52w range
    volume_anomaly: str = ""  # "HIGH" / "NORMAL" / "LOW"
    timing_score: float = 0.0  # 0-100 overall timing attractiveness
    timing_verdict: str = ""  # "BUY_NOW" / "WAIT" / "CAUTION"
    price_52w_high: Optional[float] = None
    price_52w_low: Optional[float] = None

    # Backtest (new)
    backtest_return_1y: Optional[float] = None  # actual 1y return
    backtest_return_2y: Optional[float] = None
    backtest_return_3y: Optional[float] = None
    backtest_sp500_1y: Optional[float] = None  # benchmark return
    backtest_sp500_2y: Optional[float] = None
    backtest_sp500_3y: Optional[float] = None
    backtest_alpha_1y: Optional[float] = None  # excess return
    backtest_alpha_2y: Optional[float] = None
    backtest_alpha_3y: Optional[float] = None
    backtest_max_drawdown: Optional[float] = None
    backtest_sharpe: Optional[float] = None
    backtest_verdict: str = ""  # "VALIDATED" / "MIXED" / "FAILED"

    # Position advice (new)
    buy_price_low: Optional[float] = None  # ideal entry range
    buy_price_high: Optional[float] = None
    stop_loss_price: Optional[float] = None
    position_size_pct: float = 0.0  # suggested % of portfolio
    next_review_date: str = ""  # 3 months from now
    rebalance_action: str = ""  # "INITIATE" / "ADD" / "HOLD" / "TRIM" / "EXIT"

    def is_alive(self) -> bool:
        return self.eliminated_at_stage == 0

    def eliminate(self, stage: int, reason: str):
        self.eliminated_at_stage = stage
        self.gate_failures.append(reason)

    def to_dict(self) -> dict:
        d = {}
        for k, v in self.__dict__.items():
            if isinstance(v, Enum):
                d[k] = v.value
            else:
                d[k] = v
        return d


# ═══════════════════════════════════════════════════════════════
#  Strategy Templates
# ═══════════════════════════════════════════════════════════════

def _build_strategy_templates() -> Dict[str, Dict[str, Any]]:
    """Build strategy templates from current params — dynamic, not hardcoded."""
    return {
        "conservative": {
            "name": "保守型 (Graham 防御型)",
            "min_market_cap": _P.get("strategy.conservative.min_market_cap", 2e9),
            "max_pe": _P.get("strategy.conservative.max_pe", 15),
            "max_de": _P.get("strategy.conservative.max_de", 1.0),
            "min_current_ratio": _P.get("strategy.conservative.min_current_ratio", 2.0),
            "require_positive_fcf": True,
            "min_margin_of_safety": _P.get("strategy.conservative.min_margin_of_safety", 0.33),
            "max_holdings": _P.get("strategy.conservative.max_holdings", 20),
        },
        "balanced": {
            "name": "均衡型 (Buffett + Quality)",
            "min_market_cap": _P.get("strategy.balanced.min_market_cap", 5e8),
            "max_pe": _P.get("strategy.balanced.max_pe", 25),
            "max_de": _P.get("strategy.balanced.max_de", 2.0),
            "min_current_ratio": _P.get("strategy.balanced.min_current_ratio", 1.0),
            "require_positive_fcf": False,
            "min_margin_of_safety": _P.get("strategy.balanced.min_margin_of_safety", 0.15),
            "max_holdings": _P.get("strategy.balanced.max_holdings", 15),
        },
        "aggressive": {
            "name": "进取型 (Greenblatt + GARP)",
            "min_market_cap": _P.get("strategy.aggressive.min_market_cap", 1e8),
            "max_pe": _P.get("strategy.aggressive.max_pe", 40),
            "max_de": _P.get("strategy.aggressive.max_de", 3.0),
            "min_current_ratio": _P.get("strategy.aggressive.min_current_ratio", 0.5),
            "require_positive_fcf": False,
            "min_margin_of_safety": _P.get("strategy.aggressive.min_margin_of_safety", 0.0),
            "max_holdings": _P.get("strategy.aggressive.max_holdings", 30),
        },
    }

# Keep module-level reference for backward compatibility (used by agent_routes.py)
STRATEGY_TEMPLATES = _build_strategy_templates()

PRESET_UNIVERSES = {
    "value_30": [
        "BRK-B", "JNJ", "PG", "KO", "PEP", "MCD", "WMT", "MMM", "CL",
        "EMR", "SWK", "GPC", "ADP", "SHW", "ITW", "ABT", "TGT", "ABBV", "LOW",
        "CAT", "UNP", "GD", "ED", "XOM", "CVX", "T", "VZ", "IBM", "INTC", "HD",
    ],
    "sp500_top50": [
        "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "BRK-B", "TSLA", "UNH", "XOM",
        "JNJ", "JPM", "V", "PG", "MA", "HD", "CVX", "MRK", "ABBV", "LLY",
        "PEP", "KO", "COST", "AVGO", "WMT", "MCD", "CSCO", "TMO", "ACN", "ABT",
        "DHR", "CRM", "NKE", "LIN", "ORCL", "TXN", "NEE", "UPS", "PM", "MS",
        "BMY", "RTX", "SCHW", "AMGN", "HON", "LOW", "QCOM", "SPGI", "BA", "GS",
    ],
    "dividend_kings": [
        "JNJ", "PG", "KO", "PEP", "MMM", "EMR", "CL", "ABT", "GPC", "SWK",
        "ADP", "SHW", "ITW", "ED", "TGT", "ABBV", "LOW", "MCD", "WMT", "BDX",
    ],
    "buffett_portfolio": [
        "AAPL", "BAC", "AXP", "KO", "CVX", "OXY", "KHC", "MCO",
        "CB", "DVA", "VRSN", "MA", "V", "NU", "AMZN", "C",
    ],
    "hk_value": [
        "0700.HK", "0005.HK", "0003.HK", "1299.HK", "0001.HK",
        "2318.HK", "0388.HK", "0939.HK", "1398.HK", "0941.HK",
        "0011.HK", "0002.HK", "0016.HK", "0027.HK", "2628.HK",
    ],
    "faang_plus": [
        "AAPL", "AMZN", "GOOGL", "META", "MSFT", "NFLX", "NVDA", "TSLA",
    ],
}


# ═══════════════════════════════════════════════════════════════
#  Unified Pipeline Engine
# ═══════════════════════════════════════════════════════════════

class UnifiedPipeline:
    """R-System 八阶段统一分析管线。

    最大化利用全部知识资产:
    - 65+ 蒸馏规则 × 7 投资流派 (distilled_rules.py)
    - 7 种估值模型 (tools.py run_full_valuation)
    - 3 重排雷: F-Score / Z-Score / M-Score (tools.py detect_shenanigans)
    - 14 本书 × 4 层 ChromaDB 索引 (book_indexer.py)
    - Perplexity 实时研究 (perplexity_service.py)
    - 老查理 persona 10 章深度分析 (persona.py)
    """

    STAGES = [
        {"id": 1, "name": "Universe Construction", "name_cn": "构建股票池"},
        {"id": 2, "name": "Data Acquisition", "name_cn": "数据采集"},
        {"id": 3, "name": "Hard Knockout Gate", "name_cn": "门槛淘汰"},
        {"id": 4, "name": "Financial Forensics", "name_cn": "财务排雷"},
        {"id": 5, "name": "Multi-School Consensus", "name_cn": "七流派共识"},
        {"id": 6, "name": "Multi-Model Valuation", "name_cn": "多模型估值"},
        {"id": 7, "name": "LLM-Enhanced Analysis", "name_cn": "AI深度分析"},
        {"id": 8, "name": "Conviction Ranking", "name_cn": "信念排名"},
        {"id": 9, "name": "Backtest & Position", "name_cn": "回测验证 & 调仓"},
    ]

    def __init__(self):
        self.results: List[StockResult] = []
        self.run_id: str = ""
        self.strategy: str = "balanced"
        self.config: Dict = {}
        self.stage_stats: Dict[int, Dict] = {}
        self._data_source: Optional[str] = None  # user-selected data source

    async def _probe_data_source(self) -> Optional[Dict[str, Any]]:
        """Probe Bloomberg availability. If unavailable, return an SSE event for user choice.

        Returns:
            None if Bloomberg is available (pipeline should continue).
            Dict (SSE event) if Bloomberg is unavailable (pipeline should pause).
        """
        import asyncio

        def _probe():
            from src.data_providers.factory import probe_bloomberg_first
            return probe_bloomberg_first()

        loop = asyncio.get_running_loop()
        probe_result = await loop.run_in_executor(None, _probe)

        if probe_result.bloomberg_available:
            # Bloomberg ready — set as active source and continue
            self._data_source = "bloomberg"
            logger.info("[Pipeline] Bloomberg 可用，使用 Bloomberg 数据源")
            return None

        # Bloomberg unavailable — emit event asking user to choose
        alternatives = probe_result.available_alternatives
        logger.info(f"[Pipeline] Bloomberg 不可用 ({probe_result.error_message})，"
                     f"等待用户选择数据源: {alternatives}")

        # Map provider names to display labels
        source_labels = {
            "yfinance": "Yahoo Finance (yfinance) — 免费，覆盖面广",
            "fmp": "Financial Modeling Prep (FMP) — 需API Key，数据质量高",
            "finnhub": "Finnhub — 需API Key，实时数据",
            "yahoo_direct": "Yahoo Direct — 免费备选",
        }

        return {
            "event": "data_source_required",
            "runId": self.run_id,
            "stage": 2,
            "message": f"Bloomberg 不可用: {probe_result.error_message}。请选择备选数据源：",
            "bloomberg_error": probe_result.error_message,
            "available_sources": [
                {"id": src, "label": source_labels.get(src, src)}
                for src in alternatives
            ],
            "symbols": [r.symbol for r in self.results],
            "strategy": self.strategy,
        }

    # ── Checkpoint cache for resume ──
    # Class-level cache: run_id -> (results, data_map, config, strategy, data_source)
    _checkpoint_cache: Dict[str, Any] = {}

    async def run(
        self,
        symbols: List[str],
        strategy: str = "balanced",
        universe: Optional[str] = None,
        enable_llm: bool = True,
        data_source: Optional[str] = None,
        resume_from: Optional[int] = None,
        resume_run_id: Optional[str] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Run the full 9-stage pipeline as an async generator, yielding SSE events.

        Args:
            symbols: Stock symbols to analyze (overrides universe if provided)
            strategy: Strategy template name
            universe: Preset universe name (used if symbols is empty)
            enable_llm: Whether to run Stage 7 LLM analysis
            data_source: User-selected data source. None = Bloomberg first (ask if unavailable).
                         "bloomberg" / "yfinance" / "fmp" / "finnhub" / "yahoo_direct" = use directly.
            resume_from: Stage number to resume from (skips earlier stages).
                         Use 3 to skip data checkpoint and continue from Stage 3.
            resume_run_id: The run_id from the previous paused run (for cache lookup).

        Yields:
            SSE events with pipeline progress and results.
            - ``data_source_required``: Bloomberg unavailable, user must choose source (pauses).
            - ``checkpoint``: Data quality review point, user can continue or abort (pauses).
            - ``stage_update``, ``substep_update``: Normal progress events.
            - ``pipeline_done``: Final report.
        """
        # ── Handle resume from checkpoint ──
        if resume_from and resume_run_id and resume_run_id in UnifiedPipeline._checkpoint_cache:
            cached = UnifiedPipeline._checkpoint_cache.pop(resume_run_id)
            self.run_id = resume_run_id
            self.results = cached["results"]
            data_map = cached["data_map"]
            self.config = cached["config"]
            self.strategy = cached["strategy"]
            self._data_source = cached["data_source"]

            logger.info(f"[Pipeline] 从检查点恢复: run_id={self.run_id}, "
                        f"resume_from=Stage {resume_from}, "
                        f"{len(self.results)} stocks, {len(data_map)} with data")

            # Set the active data source for tools.py
            from app.agent.tools import set_active_data_source
            set_active_data_source(self._data_source or "auto")

            # Emit resumed event
            yield {
                "event": "checkpoint_resumed",
                "runId": self.run_id,
                "resumeFrom": resume_from,
                "message": f"从 Stage {resume_from} 恢复分析...",
            }

        else:
            # ── Fresh run: Stage 1 + Stage 2 ──
            self.run_id = str(uuid.uuid4())[:8]
            self.strategy = strategy
            self._data_source = data_source
            templates = _build_strategy_templates()
            self.config = templates.get(strategy, templates["balanced"])

            # ── Stage 1: Universe Construction ──
            yield self._stage_event(1, "running")
            resolved_symbols = self._stage1_universe(symbols, universe)
            self.results = [StockResult(symbol=s) for s in resolved_symbols]
            yield self._stage_event(1, "completed", {
                "count": len(self.results),
                "symbols": resolved_symbols,
            })

            # ── Stage 2: Data Acquisition (Bloomberg-first + user choice) ──
            yield self._stage_event(2, "running")

            # If no data_source specified, probe Bloomberg first
            if data_source is None:
                probe_event = await self._probe_data_source()
                if probe_event is not None:
                    yield probe_event
                    return  # Pipeline paused — frontend re-submits with chosen data_source

            data_map = await self._stage2_data_acquisition()
            alive_count = sum(1 for r in self.results if r.is_alive())
            yield self._stage_event(2, "completed", {
                "fetched": len(data_map),
                "alive": alive_count,
                "data_insufficient": len(self.results) - alive_count,
                "data_source": self._data_source or "bloomberg",
            })

            # Set the active data source for tools.py
            from app.agent.tools import set_active_data_source
            set_active_data_source(self._data_source or "auto")

            # ── Data Checkpoint: pause for user review ──
            checkpoint_report = self._build_data_checkpoint(data_map)

            # Cache state for resume
            UnifiedPipeline._checkpoint_cache[self.run_id] = {
                "results": self.results,
                "data_map": data_map,
                "config": self.config,
                "strategy": self.strategy,
                "data_source": self._data_source,
            }

            yield {
                "event": "checkpoint",
                "runId": self.run_id,
                "stage": 2,
                "checkpointType": "data_quality",
                "message": "数据采集完成 — 请检查数据质量后继续",
                "report": checkpoint_report,
            }
            return  # Pipeline paused — frontend shows review modal

        # ── Stage 3: Hard Knockout Gate ──
        yield self._stage_event(3, "running")
        self._stage3_knockout(data_map)
        alive = [r for r in self.results if r.is_alive()]
        eliminated = [r for r in self.results if not r.is_alive() and r.eliminated_at_stage == 3]
        yield self._stage_event(3, "completed", {
            "alive": len(alive),
            "eliminated": len(eliminated),
            "details": [
                {"symbol": r.symbol, "reason": r.gate_failures[-1] if r.gate_failures else ""}
                for r in eliminated
            ],
        })

        # ── Stage 4: Financial Forensics ──
        yield self._stage_event(4, "running")
        await self._stage4_forensics([r for r in self.results if r.is_alive()], data_map)
        alive_after = [r for r in self.results if r.is_alive()]
        failed = [r for r in self.results if not r.is_alive() and r.eliminated_at_stage == 4]
        yield self._stage_event(4, "completed", {
            "alive": len(alive_after),
            "eliminated": len(failed),
            "details": [
                {
                    "symbol": r.symbol,
                    "fScore": r.f_score, "zScore": r.z_score, "mScore": r.m_score,
                    "riskTier": r.risk_tier,
                    "redFlags": r.red_flags[:5],
                    "verdict": r.forensics_verdict,
                }
                for r in self.results if r.is_alive() or r.eliminated_at_stage == 4
            ],
        })

        # ── Stage 5: Multi-School Consensus ──
        yield self._stage_event(5, "running")
        await self._stage5_multi_school(data_map, [r for r in self.results if r.is_alive()])
        yield self._stage_event(5, "completed", {
            "details": [
                {
                    "symbol": r.symbol,
                    "bestSchool": r.best_school,
                    "strongSchools": r.strong_schools,
                    "consensusScore": r.school_consensus_score,
                    "schools": {
                        k: {
                            "score": v.get("score", 0),
                            "maxScore": v.get("max_score", 0),
                            "passRate": v.get("pass_rate", 0),
                            "recommendation": v.get("recommendation", ""),
                            "verdictCn": v.get("verdict_cn", ""),
                        }
                        for k, v in r.school_results.items()
                    },
                }
                for r in self.results if r.is_alive()
            ],
        })

        # ── Stage 6: Multi-Model Valuation ──
        yield self._stage_event(6, "running")
        await self._stage6_valuation(data_map, [r for r in self.results if r.is_alive()])
        yield self._stage_event(6, "completed", {
            "details": [
                {
                    "symbol": r.symbol,
                    "price": r.price,
                    "valuations": r.valuations,
                    "intrinsicValue": r.intrinsic_value,
                    "marginOfSafety": r.margin_of_safety,
                    "moat": r.moat,
                }
                for r in self.results if r.is_alive()
            ],
        })

        # ── Stage 6.5 (Timing Signals) — runs parallel with Stage 7 ──
        yield self._substep_event(6, "timing", "running", "采集技术面时机信号...")
        alive_for_timing = [r for r in self.results if r.is_alive()]
        await self._stage_timing_signals(alive_for_timing)
        yield self._substep_event(6, "timing", "completed", "时机信号采集完成", {
            "details": [
                {
                    "symbol": r.symbol,
                    "timingScore": r.timing_score,
                    "timingVerdict": r.timing_verdict,
                    "rsi": r.rsi_14d,
                    "macd": r.macd_signal_str,
                    "ma200": r.ma200_position,
                    "week52Pct": r.week52_position,
                    "volumeAnomaly": r.volume_anomaly,
                }
                for r in alive_for_timing
            ],
        })

        # ── Stage 7: LLM-Enhanced Deep Analysis (with sub-step events) ──
        if enable_llm:
            yield self._stage_event(7, "running")

            # 7a: Fetch financial statements / SEC filings
            alive_for_llm = [r for r in self.results if r.is_alive()]
            yield self._substep_event(7, "7a", "running", "下载 SEC 财报数据...")
            await self._fetch_filing_data(alive_for_llm)
            filed_count = sum(1 for r in alive_for_llm if r.has_sec_filing or r.has_financial_statements)
            yield self._substep_event(7, "7a", "completed",
                                      f"财报数据就绪 ({filed_count}/{len(alive_for_llm)} 只有财报)")

            # 7b-7e: Full LLM analysis with per-substep progress
            # 7b: Knowledge base search
            yield self._substep_event(7, "7b", "running", "搜索投资书籍知识库...")
            await self._stage7b_knowledge(alive_for_llm)
            kb_count = sum(1 for r in alive_for_llm if r.knowledge_snippets)
            yield self._substep_event(7, "7b", "completed",
                                      f"知识库搜索完成 ({kb_count} 只匹配到书籍知识)")

            # 7c: Perplexity realtime
            yield self._substep_event(7, "7c", "running", "Perplexity 实时研究最新动态...")
            await self._stage7c_realtime(alive_for_llm)
            rt_count = sum(1 for r in alive_for_llm if r.realtime_research)
            yield self._substep_event(7, "7c", "completed",
                                      f"实时研究完成 ({rt_count} 只获取到最新动态)")

            # 7d: Investment Committee Multi-Agent Debate (NEW)
            yield self._substep_event(7, "7d", "running",
                                      f"投资委员会 Multi-Agent 辩论分析 ({len(alive_for_llm)} 只)...")
            await self._stage7d_committee_debate(alive_for_llm, data_map)
            debate_count = sum(1 for r in alive_for_llm if r.debate_record)
            yield self._substep_event(7, "7d", "completed",
                                      f"投委会辩论完成 ({debate_count}/{len(alive_for_llm)} 只完成辩论)")

            # 7e: Per-stock LLM deep analysis (retained as fallback / enrichment)
            yield self._substep_event(7, "7e", "running",
                                      f"老查理逐股深度分析 ({len(alive_for_llm)} 只)...")
            await self._stage7d_per_stock_llm(alive_for_llm, data_map)
            llm_count = sum(1 for r in alive_for_llm if r.llm_analysis)
            yield self._substep_event(7, "7e", "completed",
                                      f"逐股分析完成 ({llm_count}/{len(alive_for_llm)} 只生成深度报告)")

            # 7f: Comparative matrix
            if len(alive_for_llm) >= 2:
                yield self._substep_event(7, "7f", "running", "横向对比 Basket 分析...")
                await self._stage7e_comparative(alive_for_llm)
                yield self._substep_event(7, "7f", "completed", "Basket 对比分析完成")

            yield self._stage_event(7, "completed", {
                "details": [
                    {
                        "symbol": r.symbol,
                        "hasKnowledge": len(r.knowledge_snippets) > 0,
                        "hasRealtime": bool(r.realtime_research),
                        "hasLlmAnalysis": bool(r.llm_analysis),
                        "hasFilingData": r.has_financial_statements or r.has_sec_filing,
                        "hasSchoolOpinions": len(r.per_school_opinions) > 0,
                        "hasDebateRecord": r.debate_record is not None,
                        "committeeVerdict": r.committee_verdict,
                        "committeeConfidence": r.committee_confidence,
                        "vetoTriggered": r.committee_veto_triggered,
                    }
                    for r in self.results if r.is_alive()
                ],
            })
        else:
            yield self._stage_event(7, "skipped")

        # ── Stage 8: Conviction Ranking ──
        yield self._stage_event(8, "running")
        self._stage8_conviction([r for r in self.results if r.is_alive()])
        yield self._stage_event(8, "completed", {
            "portfolio": [
                {
                    "symbol": r.symbol, "name": r.name,
                    "compositeScore": r.composite_score,
                    "conviction": r.conviction, "verdict": r.verdict,
                    "positionWeight": r.position_weight,
                    "marginOfSafety": r.margin_of_safety,
                    "moat": r.moat, "riskTier": r.risk_tier,
                    "bestSchool": r.best_school,
                    "selectionReasons": r.selection_reasons,
                }
                for r in self.results if r.is_alive()
            ],
        })

        # ── Stage 9: Historical Backtest + Strategy Backtest + Position Advice ──
        yield self._stage_event(9, "running")
        alive_final = [r for r in self.results if r.is_alive()]

        # 9a: Per-stock historical backtest (existing)
        yield self._substep_event(9, "backtest", "running", "回溯 2-3 年历史数据验证方法论...")
        await self._stage9_backtest(alive_final)
        yield self._substep_event(9, "backtest", "completed", "历史验证完成")

        # 9b: Strategy rolling backtest (NEW — 6-month holding period)
        yield self._substep_event(9, "strategy_backtest", "running",
                                  "滚动策略回测 (6个月持仓周期，回溯2.5年)...")
        strategy_bt = await self._stage9_strategy_backtest(alive_final)
        bt_verdict = strategy_bt.get("verdict", "N/A") if strategy_bt else "N/A"
        yield self._substep_event(9, "strategy_backtest", "completed",
                                  f"策略回测完成: {bt_verdict}",
                                  {"strategy_backtest": strategy_bt} if strategy_bt else None)

        # 9c: Position advice
        yield self._substep_event(9, "position", "running", "生成调仓建议...")
        self._stage9_position_advice(alive_final)
        yield self._substep_event(9, "position", "completed", "调仓建议就绪")

        yield self._stage_event(9, "completed", {
            "strategy_backtest": strategy_bt,
            "details": [
                {
                    "symbol": r.symbol,
                    "backtest": {
                        "return1y": r.backtest_return_1y,
                        "return2y": r.backtest_return_2y,
                        "return3y": r.backtest_return_3y,
                        "sp5001y": r.backtest_sp500_1y,
                        "sp5002y": r.backtest_sp500_2y,
                        "sp5003y": r.backtest_sp500_3y,
                        "alpha1y": r.backtest_alpha_1y,
                        "alpha2y": r.backtest_alpha_2y,
                        "alpha3y": r.backtest_alpha_3y,
                        "maxDrawdown": r.backtest_max_drawdown,
                        "sharpe": r.backtest_sharpe,
                        "verdict": r.backtest_verdict,
                    },
                    "position": {
                        "buyPriceLow": r.buy_price_low,
                        "buyPriceHigh": r.buy_price_high,
                        "stopLoss": r.stop_loss_price,
                        "sizePct": r.position_size_pct,
                        "nextReview": r.next_review_date,
                        "action": r.rebalance_action,
                    },
                }
                for r in alive_final
            ],
        })

        # Build final report
        final_report = self._build_final_report()

        # ── Persist ──
        await self._persist_verdict(final_report)

        # ── Done ──
        yield {
            "event": "pipeline_done",
            "runId": self.run_id,
            "report": final_report,
        }

    # ═══════════════════════════════════════════════════════════════
    #  Data Checkpoint Builder
    # ═══════════════════════════════════════════════════════════════

    def _build_data_checkpoint(self, data_map: Dict[str, Any]) -> Dict[str, Any]:
        """Build a comprehensive data quality report for user review.

        Returns a dict that the frontend renders as a checkpoint review modal:
        - summary: overall stats
        - stocks: per-stock data coverage & quality details
        - warnings: issues requiring attention
        - strategy_impact: which strategy filters might eliminate stocks
        """
        cfg = self.config
        stocks_report = []
        warnings = []
        total_core_coverage = 0.0
        stocks_with_data = 0

        for result in self.results:
            stock_info: Dict[str, Any] = {
                "symbol": result.symbol,
                "name": result.name or result.symbol,
                "alive": result.is_alive(),
                "dataSource": result.data_source or self._data_source or "unknown",
                "price": result.price,
            }

            if not result.is_alive():
                # Eliminated at Stage 2 — show why
                stock_info["status"] = "eliminated"
                stock_info["reason"] = result.gate_failures[-1] if result.gate_failures else "数据获取失败"
                stock_info["coverage"] = {"core": 0, "extended": 0, "historical": 0, "overall": 0}
                stock_info["missingCore"] = []
                warnings.append({
                    "type": "data_fail",
                    "symbol": result.symbol,
                    "message": f"{result.symbol}: {stock_info['reason']}",
                })
            elif result.symbol in data_map:
                stocks_with_data += 1
                stock_obj = data_map[result.symbol].get("stock")
                d = data_map[result.symbol].get("dict", {})

                # Coverage breakdown
                if hasattr(stock_obj, 'data_coverage'):
                    cov = stock_obj.data_coverage()
                else:
                    cov = {"core": {"pct": 0}, "extended": {"pct": 0},
                           "historical": {"pct": 0}, "overall": {"pct": 0},
                           "missing_core": []}

                core_pct = cov.get("core", {}).get("pct", 0)
                total_core_coverage += core_pct

                stock_info["status"] = "ok" if core_pct >= 70 else ("warning" if core_pct >= 40 else "poor")
                stock_info["coverage"] = {
                    "core": cov.get("core", {}).get("pct", 0),
                    "extended": cov.get("extended", {}).get("pct", 0),
                    "historical": cov.get("historical", {}).get("pct", 0),
                    "overall": cov.get("overall", {}).get("pct", 0),
                }
                stock_info["missingCore"] = cov.get("missing_core", [])

                # ── Historical data depth ──
                eps_history = getattr(stock_obj, 'eps_history', []) or []
                div_history = getattr(stock_obj, 'dividend_history', []) or []
                stock_info["historicalDepth"] = {
                    "epsYears": len(eps_history),
                    "dividendYears": len(div_history),
                    "hasGrowthData": d.get("earnings_growth_10y") is not None,
                    "has10YAvg": d.get("avg_eps_10y") is not None,
                    "profitableYears": d.get("profitable_years"),
                    "consecutiveProfitable": d.get("consecutive_profitable_years"),
                    "consecutiveDividend": d.get("consecutive_dividend_years"),
                }

                # ── Data freshness ──
                fetched_at = getattr(stock_obj, '_fetched_at', None)
                cache_info = None
                try:
                    from src.data_providers.cache import CachingProvider
                    from src.data_providers.factory import get_data_provider
                    provider = get_data_provider(self._data_source or "auto")
                    if isinstance(provider, CachingProvider):
                        cache_info = provider.get_cache_info(result.symbol)
                except Exception:
                    pass

                stock_info["freshness"] = {
                    "fetchedAt": fetched_at,
                    "isCached": cache_info.get("is_cached", False) if cache_info else False,
                    "cacheAge": cache_info.get("cache_age_str", "") if cache_info else "",
                    "source": result.data_source or self._data_source or "unknown",
                }

                # ── Data anomaly detection ──
                anomalies = []
                pe_val = d.get("pe")
                if pe_val is not None and pe_val > 200:
                    anomalies.append({"field": "PE", "value": pe_val,
                                      "issue": "PE 异常偏高，可能是一次性损益或微利导致"})
                if pe_val is not None and pe_val < 0:
                    anomalies.append({"field": "PE", "value": pe_val,
                                      "issue": "PE 为负，公司亏损"})
                de_val = d.get("debt_to_equity")
                if de_val is not None and de_val < 0:
                    anomalies.append({"field": "D/E", "value": de_val,
                                      "issue": "D/E 为负，可能是负股东权益"})
                if de_val is not None and de_val > 5:
                    anomalies.append({"field": "D/E", "value": de_val,
                                      "issue": "D/E 极高，杠杆风险突出"})
                fcf_val = d.get("free_cash_flow")
                ni_val = d.get("net_income")
                if (fcf_val is not None and ni_val is not None
                        and fcf_val < 0 and ni_val > 0):
                    anomalies.append({"field": "FCF vs 净利润", "value": f"FCF={fcf_val}, NI={ni_val}",
                                      "issue": "现金流与利润背离，盈利质量存疑"})
                roe_val = d.get("roe")
                if roe_val is not None and roe_val > 1.0:
                    anomalies.append({"field": "ROE", "value": f"{roe_val*100:.0f}%",
                                      "issue": "ROE 超过 100%，可能是低权益或负权益"})
                stock_info["anomalies"] = anomalies
                if anomalies:
                    warnings.append({
                        "type": "anomaly",
                        "symbol": result.symbol,
                        "message": f"{result.symbol}: {len(anomalies)} 个数据异常 — {anomalies[0]['issue']}",
                    })

                # Key values for user to verify
                stock_info["keyMetrics"] = {
                    "marketCap": d.get("market_cap"),
                    "pe": d.get("pe"),
                    "pb": d.get("pb"),
                    "roe": d.get("roe"),
                    "debtToEquity": d.get("debt_to_equity"),
                    "currentRatio": d.get("current_ratio"),
                    "dividendYield": d.get("dividend_yield"),
                    "eps": d.get("eps"),
                    "revenue": d.get("revenue"),
                    "freeCashFlow": d.get("free_cash_flow"),
                    "profitMargin": d.get("profit_margin"),
                    "sector": d.get("sector", result.sector),
                    "industry": d.get("industry", result.industry),
                }

                # Strategy impact preview: would this stock pass Stage 3?
                preview_fails = []
                mc = d.get("market_cap")
                if mc is not None and cfg.get("min_market_cap") and mc < cfg["min_market_cap"]:
                    preview_fails.append({
                        "gate": "市值门槛",
                        "actual": f"${mc/1e9:.1f}B",
                        "threshold": f"≥ ${cfg['min_market_cap']/1e9:.1f}B",
                        "severity": "hard",
                        "suggestion": "切换到进取型策略（最低市值 $0.1B）" if cfg.get("min_market_cap", 0) > 1e8 else "",
                    })
                pe = d.get("pe")
                if pe is not None and cfg.get("max_pe") and pe > cfg["max_pe"]:
                    preview_fails.append({
                        "gate": "PE合理",
                        "actual": f"{pe:.1f}",
                        "threshold": f"< {cfg['max_pe']}",
                        "severity": "hard",
                        "suggestion": f"切换到进取型策略（max_pe={40}）" if cfg.get("max_pe", 99) < 40 else "",
                    })
                de = d.get("debt_to_equity")
                if de is not None and cfg.get("max_de") and de > cfg["max_de"]:
                    preview_fails.append({
                        "gate": "负债可控",
                        "actual": f"{de:.2f}",
                        "threshold": f"< {cfg['max_de']}",
                        "severity": "hard",
                        "suggestion": "",
                    })
                cr = d.get("current_ratio")
                if cr is not None and cfg.get("min_current_ratio") and cr < cfg["min_current_ratio"]:
                    preview_fails.append({
                        "gate": "流动性",
                        "actual": f"{cr:.2f}",
                        "threshold": f"≥ {cfg['min_current_ratio']}",
                        "severity": "hard",
                        "suggestion": "",
                    })
                fcf = d.get("free_cash_flow")
                if cfg.get("require_positive_fcf") and (fcf is None or fcf <= 0):
                    preview_fails.append({
                        "gate": "正自由现金流",
                        "actual": f"${fcf/1e6:.0f}M" if fcf is not None else "N/A",
                        "threshold": "> 0",
                        "severity": "hard",
                        "suggestion": "切换到均衡/进取型策略（不强制要求正 FCF）",
                    })

                stock_info["stage3Preview"] = {
                    "willPass": len(preview_fails) == 0,
                    "failCount": len(preview_fails),
                    "failures": preview_fails,
                    # Keep backward-compatible flat list for existing frontend
                    "potentialFailures": [
                        f"{f['gate']}: {f['actual']} vs {f['threshold']}"
                        for f in preview_fails
                    ],
                }

                if preview_fails:
                    first_fail = preview_fails[0]
                    warnings.append({
                        "type": "stage3_risk",
                        "symbol": result.symbol,
                        "message": f"{result.symbol}: 可能在 Stage 3 被淘汰 — {first_fail['gate']}({first_fail['actual']} vs {first_fail['threshold']})",
                    })

                if stock_info["missingCore"]:
                    missing = stock_info["missingCore"]
                    if len(missing) >= 4:
                        warnings.append({
                            "type": "low_coverage",
                            "symbol": result.symbol,
                            "message": f"{result.symbol}: 核心字段缺失较多 ({len(missing)}/{12}) — {', '.join(missing[:4])}...",
                        })
            else:
                stock_info["status"] = "no_data"
                stock_info["coverage"] = {"core": 0, "extended": 0, "historical": 0, "overall": 0}
                stock_info["missingCore"] = []

            stocks_report.append(stock_info)

        avg_core_coverage = total_core_coverage / max(stocks_with_data, 1)

        return {
            "summary": {
                "totalStocks": len(self.results),
                "withData": stocks_with_data,
                "eliminated": len(self.results) - sum(1 for r in self.results if r.is_alive()),
                "avgCoreCoverage": round(avg_core_coverage, 1),
                "dataSource": self._data_source or "bloomberg",
                "strategy": self.strategy,
                "strategyName": self.config.get("name", self.strategy),
            },
            "stocks": stocks_report,
            "warnings": warnings,
        }

    # ═══════════════════════════════════════════════════════════════
    #  Stage Implementations
    # ═══════════════════════════════════════════════════════════════

    def _stage1_universe(self, symbols: List[str], universe: Optional[str]) -> List[str]:
        """Stage 1: Resolve symbols from input or preset universe."""
        if symbols and len(symbols) > 0:
            return [s.upper().strip() for s in symbols if s.strip()]

        if universe and universe in PRESET_UNIVERSES:
            return list(PRESET_UNIVERSES[universe])

        return symbols or []

    async def _stage2_data_acquisition(self) -> Dict[str, Any]:
        """Stage 2: Fetch data for all symbols using the selected data source.

        数据源策略:
          - self._data_source = "bloomberg": 仅用 Bloomberg
          - self._data_source = "yfinance" / "fmp" / etc: 仅用指定源
          - self._data_source = None: 本不应到达此处 (应在 run() 中被 probe 拦截)，
                                       但作为安全兜底用 auto 模式

        性能优化:
          - provider 在循环外创建一次（单例），所有股票复用同一个连接
          - 对 Bloomberg 等有状态连接的 provider 尤其重要，避免连接风暴
        """
        import asyncio

        data_map = {}
        chosen_source = self._data_source or "auto"

        # ── 创建 provider 一次，所有股票共享 ──
        from src.data_providers.factory import get_data_provider
        shared_provider = get_data_provider(chosen_source)
        provider_name = shared_provider.name
        logger.info(f"[Stage2] 使用共享 provider: {provider_name} (chosen: {chosen_source})")

        def _fetch_single(symbol: str):
            """Fetch data for one symbol using the shared provider."""
            from src.symbol_resolver import resolve_for_provider

            resolved = resolve_for_provider(symbol, provider_name)
            logger.info(f"[Stage2] {symbol} → {provider_name} (共享连接)")

            stock = shared_provider.fetch(resolved)
            if stock is None or not hasattr(stock, 'to_dict'):
                return None, provider_name, 0

            cov = stock.data_coverage() if hasattr(stock, 'data_coverage') else {}
            pct = cov.get('core', {}).get('pct', 0) if cov else 0
            return stock, provider_name, pct

        async def _fetch_one(result: StockResult) -> Tuple[str, Any]:
            loop = asyncio.get_running_loop()
            return result.symbol, await asyncio.wait_for(
                loop.run_in_executor(None, _fetch_single, result.symbol),
                timeout=60,
            )

        tasks = [_fetch_one(r) for r in self.results]
        fetched = await asyncio.gather(*tasks, return_exceptions=True)

        for item in fetched:
            if isinstance(item, Exception):
                logger.error(f"Data fetch failed: {item}")
                continue
            symbol, (stock, source_name, core_pct) = item
            result = next((r for r in self.results if r.symbol == symbol), None)
            if not result:
                continue

            if stock is None or not hasattr(stock, 'to_dict'):
                result.eliminate(2, f"数据源 {chosen_source} 获取失败")
                continue

            d = stock.to_dict()

            result.name = getattr(stock, 'name', symbol) or symbol
            result.sector = getattr(stock, 'sector', '') or ''
            result.industry = getattr(stock, 'industry', '') or ''
            result.price = getattr(stock, 'price', None)
            result.data_quality = core_pct
            result.data_source = source_name

            # Quality gate: core coverage must be >= threshold
            min_coverage = _P.get("data.core_coverage_min", 40)
            if core_pct < min_coverage:
                result.eliminate(2, f"数据质量不足 (核心覆盖率 {core_pct}%)")
                continue

            data_map[symbol] = {"stock": stock, "dict": d}

        return data_map

    def _stage3_knockout(self, data_map: Dict[str, Any]):
        """Stage 3: Hard knockout based on Graham defensive investor standards."""
        cfg = self.config

        for result in self.results:
            if not result.is_alive():
                continue

            if result.symbol not in data_map:
                result.eliminate(3, "无有效数据")
                continue

            d = data_map[result.symbol]["dict"]

            # Gate checks
            gates = [
                ("市值门槛", lambda: d.get("market_cap") is not None and d["market_cap"] >= cfg.get("min_market_cap", 5e8),
                 f"市值 < ${cfg.get('min_market_cap', 5e8)/1e9:.1f}B"),
                ("PE合理", lambda: d.get("pe") is None or (0 < d["pe"] < cfg.get("max_pe", 25)),
                 f"PE 不在 0-{cfg.get('max_pe', 25)} 范围"),
                ("正盈利", lambda: d.get("eps") is not None and d["eps"] > 0,
                 "EPS ≤ 0"),
                ("负债可控", lambda: d.get("debt_to_equity") is None or d["debt_to_equity"] < cfg.get("max_de", 2.0),
                 f"D/E > {cfg.get('max_de', 2.0)}"),
                ("流动性", lambda: d.get("current_ratio") is None or d["current_ratio"] >= cfg.get("min_current_ratio", 1.0),
                 f"流动比率 < {cfg.get('min_current_ratio', 1.0)}"),
                ("ROE为正", lambda: d.get("roe") is None or d["roe"] > 0,
                 "ROE ≤ 0"),
            ]

            if cfg.get("require_positive_fcf"):
                gates.append(
                    ("正自由现金流", lambda: d.get("free_cash_flow") is None or d["free_cash_flow"] > 0,
                     "FCF < 0")
                )

            failures = []
            for name, check_fn, fail_msg in gates:
                try:
                    if not check_fn():
                        failures.append(f"{name}: {fail_msg}")
                except Exception:
                    pass

            if failures:
                result.gate_passed = False
                result.gate_failures = failures
                result.eliminate(3, "; ".join(failures[:3]))

    async def _stage4_forensics(self, alive_results: List[StockResult],
                               data_map: Optional[Dict[str, Any]] = None):
        """Stage 4: Financial forensics — F-Score, Z-Score, M-Score, Schilit detection.

        Args:
            alive_results: Stocks that survived previous stages.
            data_map: Stage 2 data cache. When provided, avoids redundant fetch in tools.
        """
        from app.agent.tools import detect_shenanigans

        async def _check_one(result: StockResult):
            try:
                # Pass pre-fetched StockData to avoid redundant API calls
                stock_obj = None
                if data_map and result.symbol in data_map:
                    stock_obj = data_map[result.symbol].get("stock")
                data = await detect_shenanigans(result.symbol, stock_data=stock_obj)
                result.f_score = data.get("fScore", 0)
                result.f_score_details = data.get("fReasons", [])
                result.z_score = data.get("zScore")
                result.m_score = data.get("mScore")
                result.red_flags = data.get("redFlags", [])

                # Z-Score zone
                if result.z_score is not None:
                    if result.z_score >= 2.99:
                        result.z_zone = "safe"
                    elif result.z_score >= 1.81:
                        result.z_zone = "grey"
                    else:
                        result.z_zone = "danger"

                # Risk tier
                risk = data.get("riskLevel", "LOW")
                if risk == "CRITICAL":
                    result.risk_tier = RiskTier.DANGER.value
                elif risk == "HIGH":
                    result.risk_tier = RiskTier.FRAGILE.value
                elif risk == "MEDIUM":
                    result.risk_tier = RiskTier.NEUTRAL.value
                else:
                    # Refine based on F-Score and Z-Score
                    fortress_f = _P.get("risk_tier.fortress_f_score_min", 7)
                    solid_f = _P.get("risk_tier.solid_f_score_min", 5)
                    if result.f_score >= fortress_f and result.z_zone == "safe" and len(result.red_flags) == 0:
                        result.risk_tier = RiskTier.FORTRESS.value
                    elif result.f_score >= solid_f and result.z_zone in ("safe", "grey"):
                        result.risk_tier = RiskTier.SOLID.value
                    else:
                        result.risk_tier = RiskTier.NEUTRAL.value

                # Forensics verdict
                if risk == "CRITICAL":
                    result.forensics_verdict = "FAIL"
                    result.eliminate(4, f"排雷失败: 风险等级 CRITICAL, {len(result.red_flags)} 个红旗")
                elif risk == "HIGH" and len(result.red_flags) >= 3:
                    result.forensics_verdict = "FAIL"
                    result.eliminate(4, f"排雷失败: 风险等级 HIGH + {len(result.red_flags)} 个红旗")
                else:
                    result.forensics_verdict = "PASS"

            except Exception as e:
                logger.error(f"Forensics failed for {result.symbol}: {e}")
                result.forensics_verdict = "PASS"  # Err on the side of not eliminating

        await asyncio.gather(*[_check_one(r) for r in alive_results], return_exceptions=True)

    async def _stage5_multi_school(self, data_map: Dict[str, Any], alive_results: List[StockResult]):
        """Stage 5: Seven investment school consensus evaluation."""
        import asyncio

        def _evaluate_one(result: StockResult):
            if result.symbol not in data_map:
                return
            d = data_map[result.symbol]["dict"]

            try:
                from app.agent.distilled_rules import evaluate_stock_all_schools, SCHOOLS
                eval_result = evaluate_stock_all_schools(d)

                result.school_results = eval_result.get("schools", {})
                result.best_school = eval_result.get("best_fit_school", "")
                result.strong_schools = eval_result.get("strong_pass_schools", [])

                # Weighted consensus score (0-100)
                school_weights = {
                    "graham": _P.get("school_weight.graham", 1.5),
                    "buffett": _P.get("school_weight.buffett", 2.0),
                    "quantitative": _P.get("school_weight.quantitative", 1.5),
                    "quality": _P.get("school_weight.quality", 2.0),
                    "valuation": _P.get("school_weight.valuation", 1.5),
                    "contrarian": _P.get("school_weight.contrarian", 0.5),
                    "garp": _P.get("school_weight.garp", 1.0),
                }
                total_w, max_w = 0.0, 0.0
                for sk, sd in result.school_results.items():
                    w = school_weights.get(sk, 1.0)
                    score = sd.get("score", 0)
                    max_score = sd.get("max_score", 1)
                    if max_score > 0:
                        total_w += (score / max_score) * w * 100
                        max_w += w * 100
                result.school_consensus_score = round(total_w / max_w * 100, 1) if max_w > 0 else 0

            except Exception as e:
                logger.warning(f"[Stage5] School eval failed for {result.symbol}: {e}")

        loop = asyncio.get_running_loop()
        await asyncio.wait_for(
            loop.run_in_executor(None, lambda: [_evaluate_one(r) for r in alive_results]),
            timeout=60,
        )

    async def _stage6_valuation(self, data_map: Dict[str, Any], alive_results: List[StockResult]):
        """Stage 6: 7 valuation models + margin of safety."""
        from app.agent.tools import run_full_valuation

        async def _value_one(result: StockResult):
            try:
                # Pass pre-fetched StockData to avoid redundant API calls
                stock_obj = None
                if result.symbol in data_map:
                    stock_obj = data_map[result.symbol].get("stock")
                val = await run_full_valuation(result.symbol, stock_data=stock_obj)
                if isinstance(val, dict) and not val.get("error"):
                    result.valuations = val.get("valuations", {})
                    result.intrinsic_value = val.get("intrinsicValue")
                    result.margin_of_safety = val.get("marginOfSafety")
                    result.price = val.get("price", result.price)

                    quality = val.get("quality", {})
                    result.moat = quality.get("moatType", "None")
            except Exception as e:
                logger.warning(f"[Stage6] Valuation failed for {result.symbol}: {e}")

        await asyncio.gather(*[_value_one(r) for r in alive_results], return_exceptions=True)

    async def _fetch_filing_data(self, alive_results: List[StockResult]):
        """Fetch financial statements and SEC filings for each stock."""
        try:
            from app.agent.filing_fetcher import fetch_comprehensive_financials

            async def _fetch_one(result: StockResult):
                try:
                    data = await fetch_comprehensive_financials(result.symbol)
                    result.filing_summary = data.get("summary", "")
                    result.has_sec_filing = data.get("has_sec_filing", False)
                    result.has_financial_statements = data.get("has_financial_statements", False)
                    logger.info(
                        f"[Filing] {result.symbol}: SEC={result.has_sec_filing}, "
                        f"FS={result.has_financial_statements}"
                    )
                except Exception as e:
                    logger.warning(f"[Filing] {result.symbol} failed: {e}")

            await asyncio.gather(*[_fetch_one(r) for r in alive_results[:10]], return_exceptions=True)
        except Exception as e:
            logger.warning(f"[Filing] Import or batch error: {e}")

    async def _stage7b_knowledge(self, alive_results: List[StockResult]):
        """Stage 7b: Knowledge base search."""
        try:
            from app.agent.tools import search_book_library

            async def _kb_one(result):
                try:
                    query = f"{result.name} {result.sector} value investing analysis moat"
                    kb_result = await asyncio.wait_for(search_book_library(query), timeout=15)
                    if kb_result and "未在书籍库中找到" not in kb_result:
                        result.knowledge_snippets = [kb_result[:1500]]
                except asyncio.TimeoutError:
                    logger.warning(f"[Stage7b] Knowledge search timeout for {result.symbol}")
                except Exception:
                    pass

            await asyncio.gather(*[_kb_one(r) for r in alive_results], return_exceptions=True)
        except Exception as e:
            logger.warning(f"[Stage7b] Knowledge search failed: {e}")

    async def _stage7c_realtime(self, alive_results: List[StockResult]):
        """Stage 7c: Perplexity realtime research."""
        try:
            from app.agent.tools import research_realtime
            symbols_str = ", ".join(r.symbol for r in alive_results[:5])
            query = (
                f"Latest news, earnings results, and financial analysis for {symbols_str}. "
                f"Include: recent quarterly earnings surprises, analyst consensus, "
                f"major risks, catalysts, competitive position changes, and management guidance."
            )
            realtime = await asyncio.wait_for(research_realtime(query), timeout=30)
            if realtime and "未配置" not in realtime and "失败" not in realtime:
                for result in alive_results:
                    result.realtime_research = realtime[:2000]
        except asyncio.TimeoutError:
            logger.warning("[Stage7c] Perplexity research timed out (30s)")
        except Exception as e:
            logger.warning(f"[Stage7c] Perplexity research failed: {e}")

    async def _stage7d_per_stock_llm(self, alive_results: List[StockResult], data_map: Dict[str, Any]):
        """Stage 7d: Per-stock LLM analysis with school opinions — PARALLEL."""
        try:
            from app.agent.llm import is_llm_available, simple_completion
            from app.agent.persona import CHARLIE_SYSTEM_PROMPT

            if not is_llm_available():
                return

            async def _analyze_one(result):
                stock_data_text = self._build_single_stock_snapshot(result, data_map)

                prompt = f"""你是「老查理」，融合了 Graham、Buffett、Greenblatt、Damodaran、Dorsey 等大师智慧的深度价值投资分析师。

以下是 {result.symbol} ({result.name}) 的完整量化分析数据 + 最新财报：

{stock_data_text}

请提供 **逐流派深度分析**（这是最重要的输出）：

## 一、七大流派分别怎么看 {result.symbol}

对以下每个流派，请用 2-3 段文字说明该流派对此股票的看法（不是只给评分，而是解释为什么），包括该流派关注的核心指标是否达标、该流派会不会买入、买入的条件是什么：

1. **Graham 深度价值派**: PE、PB、负债比、流动比率、连续盈利等
2. **Buffett 护城河派**: ROE、利润率、竞争优势、管理层品质、自由现金流
3. **量化价值派 (Greenblatt/O'Shaughnessy)**: 盈利收益率、ROIC、排名方法
4. **品质投资派 (Dorsey/Cunningham)**: 持续高ROE、低资本需求、护城河类型
5. **Damodaran 估值派**: DCF 合理性、增长假设、WACC 敏感性
6. **逆向价值派 (Spier/Templeton)**: 市场情绪、逆向指标、被忽视程度
7. **GARP 成长派 (Lynch)**: PEG、成长可持续性、合理价格

## 二、关键财务分析

基于最新财报数据（利润表、资产负债表、现金流量表），分析：
- 收入和利润趋势（同比变化）
- 资产负债表健康度
- 现金流质量（经营现金流 vs 净利润）
- 资本分配策略（回购、分红、再投资）

## 三、核心投资论点

- 为什么应该买入？（3个最强理由）
- 为什么不应该买入？（Munger 反向思维，3个最大风险）
- 合理买入价格区间

## 四、老查理寄语

一段话总结，直接给出买/不买的建议和理由。"""

                messages = [
                    {"role": "system", "content": CHARLIE_SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ]

                try:
                    llm_result = await simple_completion(messages, temperature=0.5, max_tokens=4096, timeout=150)
                    if llm_result and not llm_result.startswith("[LLM Error") and not llm_result.startswith("[LLM Timeout"):
                        result.llm_analysis = llm_result
                        result.per_school_opinions = self._parse_school_opinions(llm_result)
                    else:
                        logger.warning(f"[Stage7d] LLM for {result.symbol}: {llm_result[:100]}")
                except Exception as e:
                    logger.warning(f"[Stage7d] LLM analysis for {result.symbol} failed: {e}")

            llm_semaphore = asyncio.Semaphore(2)  # Max 2 concurrent LLM analyses

            async def _analyze_with_limit(r):
                async with llm_semaphore:
                    await _analyze_one(r)

            await asyncio.gather(
                *[_analyze_with_limit(r) for r in alive_results[:8]],
                return_exceptions=True,
            )
        except Exception as e:
            logger.warning(f"[Stage7d] LLM analysis batch failed: {e}")

    async def _stage7e_comparative(self, alive_results: List[StockResult]):
        """Stage 7e: Cross-basket comparative matrix."""
        try:
            from app.agent.llm import simple_completion
            from app.agent.persona import CHARLIE_SYSTEM_PROMPT

            comparison_data = self._build_comparison_snapshot(alive_results)

            prompt = f"""你是「老查理」。以下是一个投资组合篮子（basket）中所有存活股票的横向对比数据：

{comparison_data}

请提供 **Basket 横向对比分析**：

## 一、对比矩阵

用表格形式比较各股票的关键维度（安全边际、护城河、七流派共识度、估值吸引力、风险等级）。

## 二、Best Pick — 最佳价值投资标的

从纯价值投资角度，哪只股票是当前最应该买入的？为什么？引用具体数据。

## 三、组合建议

如果要构建一个集中型价值投资组合（5-10只），你会如何分配仓位？考虑：
- 行业分散度
- 风险回报比
- 流派覆盖度（不同股票适合不同流派）
- Kelly 仓位建议

## 四、避免哪些？

哪些股票虽然通过了筛选但仍需警惕？为什么？

## 五、Howard Marks 第二层思维

市场对这些股票的共识是什么？我们的差异化见解在哪里？"""

            messages = [
                {"role": "system", "content": CHARLIE_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ]

            comparative_result = await simple_completion(messages, temperature=0.5, max_tokens=4096, timeout=150)
            if comparative_result and not comparative_result.startswith("[LLM Error") and not comparative_result.startswith("[LLM Timeout"):
                for result in alive_results:
                    result.comparative_matrix = comparative_result

        except Exception as e:
            logger.warning(f"[Stage7e] Comparative analysis failed: {e}")

    async def _stage7d_committee_debate(
        self,
        alive_results: List[StockResult],
        data_map: Dict[str, Any],
    ):
        """Stage 7d (NEW): Multi-Agent Investment Committee debate.

        For each stock, runs 12 agents (7 school + 5 role) in parallel,
        then PM synthesizes a debate and delivers the final verdict.
        Committee debate events are logged but not yielded (parent yields substeps).
        """
        try:
            from app.agent.committee.debate import DebateEngine

            engine = DebateEngine()

            async def _debate_one(result: StockResult):
                try:
                    stock_snapshot = self._build_single_stock_snapshot(result, data_map)

                    debate_events = []
                    async for event in engine.run_debate(
                        symbol=result.symbol,
                        stock_name=result.name,
                        stock_snapshot=stock_snapshot,
                        z_score=result.z_score,
                        m_score=result.m_score,
                        f_score=result.f_score,
                    ):
                        debate_events.append(event)
                        logger.info(
                            f"[Committee] {result.symbol} — {event.get('type')}: "
                            f"{event.get('message', '')[:80]}"
                        )

                    # Extract final results from events
                    for evt in debate_events:
                        if evt.get("type") == "debate_end":
                            record = evt.get("debate_record", {})
                            result.debate_record = record
                            result.committee_verdict = record.get("final_verdict", "")
                            result.committee_confidence = record.get("final_confidence", 0.0)
                            result.committee_vote_tally = record.get("vote_tally", {})
                            result.committee_veto_triggered = record.get("veto", {}).get("triggered", False)

                            # Also populate per_school_opinions from debate
                            for op in record.get("round1_opinions", []):
                                if op.get("agent_type") == "school":
                                    result.per_school_opinions[op["agent_name"]] = (
                                        f"{op['stance']} ({op['confidence']:.0%}): "
                                        f"{op.get('analysis_text', '')[:300]}"
                                    )

                except Exception as e:
                    logger.warning(f"[Committee] Debate failed for {result.symbol}: {e}")

            # Run debates for stocks with limited concurrency
            # (Each stock runs 12 agents internally; limit stock-level parallelism
            #  to avoid overwhelming the API rate limit)
            stock_semaphore = asyncio.Semaphore(2)  # Max 2 stocks debating at once

            async def _debate_with_limit(result: StockResult):
                async with stock_semaphore:
                    await _debate_one(result)

            await asyncio.gather(
                *[_debate_with_limit(r) for r in alive_results[:8]],
                return_exceptions=True,
            )
        except Exception as e:
            logger.warning(f"[Committee] Batch debate failed: {e}")

    async def _stage9_strategy_backtest(
        self, alive_results: List[StockResult]
    ) -> Optional[Dict[str, Any]]:
        """Stage 9b (NEW): Rolling strategy backtest with 6-month holding periods.

        Tests: "If we had bought these stocks 6 months ago, would we have beaten S&P500?"
        Uses rolling windows over 2.5 years.
        """
        try:
            from app.agent.backtest.strategy_backtest import StrategyBacktester

            symbols = [r.symbol for r in alive_results if r.is_alive()]
            if not symbols:
                return None

            backtester = StrategyBacktester(holding_months=6, lookback_years=2.5)
            bt_result = await backtester.run_backtest(symbols)

            # Store in each stock result
            bt_dict = bt_result.to_dict()
            for r in alive_results:
                r.strategy_backtest = bt_dict

            return bt_dict

        except Exception as e:
            logger.warning(f"[Strategy Backtest] Failed: {e}")
            return None

    # ═══════════════════════════════════════════════════════════════
    #  Timing Signals (new)
    # ═══════════════════════════════════════════════════════════════

    async def _stage_timing_signals(self, alive_results: List[StockResult]):
        """Fetch RSI, MACD, MA200, 52-week range, volume anomaly for timing signals."""
        import asyncio

        async def _timing_one(result: StockResult):
            try:
                loop = asyncio.get_running_loop()

                def _fetch_timing():
                    import yfinance as yf
                    from src.symbol_resolver import resolve_for_provider
                    resolved = resolve_for_provider(result.symbol, "yfinance")
                    ticker = yf.Ticker(resolved)
                    hist = ticker.history(period="1y")
                    if hist is None or hist.empty or len(hist) < 20:
                        return None
                    return hist

                hist = await asyncio.wait_for(
                    loop.run_in_executor(None, _fetch_timing), timeout=30
                )
                if hist is None:
                    return

                close = hist["Close"]
                volume = hist["Volume"]
                current = close.iloc[-1]

                # 52-week high/low position
                high_52w = close.max()
                low_52w = close.min()
                result.price_52w_high = float(high_52w)
                result.price_52w_low = float(low_52w)
                range_52w = high_52w - low_52w
                if range_52w > 0:
                    result.week52_position = round((current - low_52w) / range_52w * 100, 1)
                else:
                    result.week52_position = 50.0

                # RSI(14)
                delta = close.diff()
                gain = delta.where(delta > 0, 0).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                rsi = 100 - (100 / (1 + rs))
                if not rsi.empty and not rsi.isna().iloc[-1]:
                    result.rsi_14d = round(float(rsi.iloc[-1]), 1)

                # MACD
                ema12 = close.ewm(span=12, adjust=False).mean()
                ema26 = close.ewm(span=26, adjust=False).mean()
                macd_line = ema12 - ema26
                signal_line = macd_line.ewm(span=9, adjust=False).mean()
                if not macd_line.empty:
                    macd_val = float(macd_line.iloc[-1])
                    sig_val = float(signal_line.iloc[-1])
                    if macd_val > sig_val and macd_val > 0:
                        result.macd_signal_str = "BULLISH"
                    elif macd_val < sig_val and macd_val < 0:
                        result.macd_signal_str = "BEARISH"
                    else:
                        result.macd_signal_str = "NEUTRAL"

                # MA200 position
                if len(close) >= 200:
                    ma200 = close.rolling(200).mean().iloc[-1]
                    result.ma200_position = "ABOVE" if current > ma200 else "BELOW"
                elif len(close) >= 50:
                    ma50 = close.rolling(50).mean().iloc[-1]
                    result.ma200_position = "ABOVE" if current > ma50 else "BELOW"

                # Volume anomaly (last 5 days vs 20-day avg)
                if len(volume) >= 20:
                    avg_vol_20 = volume.iloc[-20:].mean()
                    avg_vol_5 = volume.iloc[-5:].mean()
                    if avg_vol_20 > 0:
                        vol_ratio = avg_vol_5 / avg_vol_20
                        if vol_ratio > 1.5:
                            result.volume_anomaly = "HIGH"
                        elif vol_ratio < 0.5:
                            result.volume_anomaly = "LOW"
                        else:
                            result.volume_anomaly = "NORMAL"

                # Compute timing score (0-100)
                score = 50.0

                # RSI: oversold = good for buying, overbought = caution
                if result.rsi_14d is not None:
                    if result.rsi_14d < 30:
                        score += 20  # Oversold — great timing
                    elif result.rsi_14d < 40:
                        score += 10
                    elif result.rsi_14d > 70:
                        score -= 15  # Overbought — wait
                    elif result.rsi_14d > 60:
                        score -= 5

                # 52-week position: near low = better timing
                if result.week52_position is not None:
                    if result.week52_position < 25:
                        score += 15  # Near 52w low
                    elif result.week52_position < 40:
                        score += 5
                    elif result.week52_position > 90:
                        score -= 10  # Near 52w high

                # MACD
                if result.macd_signal_str == "BULLISH":
                    score += 10
                elif result.macd_signal_str == "BEARISH":
                    score -= 10

                # MA200
                if result.ma200_position == "ABOVE":
                    score += 5  # Uptrend
                elif result.ma200_position == "BELOW":
                    score += 5  # Contrarian value opportunity

                # Volume
                if result.volume_anomaly == "HIGH":
                    score += 5  # Attention/catalyst

                result.timing_score = round(max(0, min(100, score)), 1)

                # Timing verdict
                buy_now_th = _P.get("timing.buy_now_threshold", 65)
                caution_th = _P.get("timing.caution_threshold", 40)
                if result.timing_score >= buy_now_th:
                    result.timing_verdict = "BUY_NOW"
                elif result.timing_score >= caution_th:
                    result.timing_verdict = "WAIT"
                else:
                    result.timing_verdict = "CAUTION"

            except Exception as e:
                logger.warning(f"[Timing] {result.symbol}: {e}")

        await asyncio.gather(*[_timing_one(r) for r in alive_results], return_exceptions=True)

    # ═══════════════════════════════════════════════════════════════
    #  Stage 9: Backtest + Position Advice (new)
    # ═══════════════════════════════════════════════════════════════

    async def _stage9_backtest(self, alive_results: List[StockResult]):
        """Fetch 3-year historical returns and compare to S&P 500 benchmark."""
        import asyncio

        async def _backtest_one(result: StockResult):
            try:
                loop = asyncio.get_running_loop()

                def _fetch_history():
                    import yfinance as yf
                    from datetime import datetime, timedelta
                    from src.symbol_resolver import resolve_for_provider
                    resolved = resolve_for_provider(result.symbol, "yfinance")
                    end = datetime.now()
                    start = end - timedelta(days=1100)  # ~3 years

                    ticker = yf.Ticker(resolved)
                    hist = ticker.history(start=start.strftime("%Y-%m-%d"),
                                          end=end.strftime("%Y-%m-%d"))

                    # S&P 500 benchmark
                    spy = yf.Ticker("SPY")
                    spy_hist = spy.history(start=start.strftime("%Y-%m-%d"),
                                           end=end.strftime("%Y-%m-%d"))

                    return hist, spy_hist

                hist, spy_hist = await asyncio.wait_for(
                    loop.run_in_executor(None, _fetch_history), timeout=45
                )

                if hist is None or hist.empty or len(hist) < 20:
                    return

                close = hist["Close"]
                current = float(close.iloc[-1])

                def _calc_return(series, days_ago):
                    if len(series) < days_ago:
                        return None
                    target_idx = max(0, len(series) - days_ago)
                    past_price = float(series.iloc[target_idx])
                    if past_price <= 0:
                        return None
                    return round((current / past_price - 1), 4)

                # Stock returns
                result.backtest_return_1y = _calc_return(close, 252)
                result.backtest_return_2y = _calc_return(close, 504)
                result.backtest_return_3y = _calc_return(close, 756)

                # S&P 500 returns
                if spy_hist is not None and not spy_hist.empty:
                    spy_close = spy_hist["Close"]
                    spy_current = float(spy_close.iloc[-1])

                    def _calc_spy(days_ago):
                        if len(spy_close) < days_ago:
                            return None
                        idx = max(0, len(spy_close) - days_ago)
                        past = float(spy_close.iloc[idx])
                        return round((spy_current / past - 1), 4) if past > 0 else None

                    result.backtest_sp500_1y = _calc_spy(252)
                    result.backtest_sp500_2y = _calc_spy(504)
                    result.backtest_sp500_3y = _calc_spy(756)

                    # Alpha (excess return)
                    if result.backtest_return_1y is not None and result.backtest_sp500_1y is not None:
                        result.backtest_alpha_1y = round(result.backtest_return_1y - result.backtest_sp500_1y, 4)
                    if result.backtest_return_2y is not None and result.backtest_sp500_2y is not None:
                        result.backtest_alpha_2y = round(result.backtest_return_2y - result.backtest_sp500_2y, 4)
                    if result.backtest_return_3y is not None and result.backtest_sp500_3y is not None:
                        result.backtest_alpha_3y = round(result.backtest_return_3y - result.backtest_sp500_3y, 4)

                # Max drawdown (from 3y data)
                running_max = close.cummax()
                drawdown = (close - running_max) / running_max
                result.backtest_max_drawdown = round(float(drawdown.min()), 4)

                # Annualized Sharpe (simple: daily returns, 252 trading days)
                daily_returns = close.pct_change().dropna()
                if len(daily_returns) > 50:
                    mean_r = daily_returns.mean()
                    std_r = daily_returns.std()
                    if std_r > 0:
                        result.backtest_sharpe = round(float(mean_r / std_r * (252 ** 0.5)), 2)

                # Verdict based on alpha and drawdown
                alpha_1y = result.backtest_alpha_1y
                alpha_3y = result.backtest_alpha_3y or result.backtest_alpha_2y
                dd = result.backtest_max_drawdown

                if alpha_1y is not None and alpha_1y > 0.05 and (alpha_3y is None or alpha_3y > 0):
                    result.backtest_verdict = "VALIDATED"
                elif alpha_1y is not None and alpha_1y > -0.05:
                    result.backtest_verdict = "MIXED"
                else:
                    result.backtest_verdict = "FAILED"

                # Override if huge drawdown
                if dd is not None and dd < -0.5:
                    if result.backtest_verdict == "VALIDATED":
                        result.backtest_verdict = "MIXED"

            except Exception as e:
                logger.warning(f"[Backtest] {result.symbol}: {e}")

        await asyncio.gather(*[_backtest_one(r) for r in alive_results], return_exceptions=True)

    def _stage9_position_advice(self, alive_results: List[StockResult]):
        """Generate actionable position advice for quarterly rebalancing."""
        from datetime import datetime, timedelta

        next_review = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d")

        for r in alive_results:
            r.next_review_date = next_review

            # Buy price range based on intrinsic value and safety margin
            if r.intrinsic_value and r.intrinsic_value > 0 and r.price and r.price > 0:
                # Ideal buy: below intrinsic value
                r.buy_price_low = round(r.intrinsic_value * _P.get("buy_price.low_multiplier", 0.67), 2)
                r.buy_price_high = round(r.intrinsic_value * _P.get("buy_price.high_multiplier", 0.85), 2)

                # Stop loss: below current price depending on conviction
                stop_pct = {
                    "HIGHEST": _P.get("stoploss.highest_pct", 0.25),
                    "HIGH": _P.get("stoploss.high_pct", 0.20),
                    "MEDIUM": _P.get("stoploss.medium_pct", 0.15),
                    "LOW": _P.get("stoploss.low_pct", 0.12),
                    "NONE": _P.get("stoploss.none_pct", 0.10),
                }.get(r.conviction, 0.15)
                r.stop_loss_price = round(r.price * (1 - stop_pct), 2)
            elif r.price and r.price > 0:
                # Fallback if no intrinsic value
                r.buy_price_low = round(r.price * 0.85, 2)
                r.buy_price_high = round(r.price * 0.95, 2)
                r.stop_loss_price = round(r.price * 0.85, 2)

            # Position size (% of portfolio) based on conviction
            base_sizes = {
                "HIGHEST": _P.get("position.highest_pct", 12.0),
                "HIGH": _P.get("position.high_pct", 8.0),
                "MEDIUM": _P.get("position.medium_pct", 5.0),
                "LOW": _P.get("position.low_pct", 3.0),
                "NONE": _P.get("position.none_pct", 2.0),
            }
            r.position_size_pct = base_sizes.get(r.conviction, 5.0)

            # Adjust by timing
            if r.timing_verdict == "BUY_NOW":
                r.position_size_pct = min(
                    r.position_size_pct * _P.get("position.buy_now_multiplier", 1.2),
                    _P.get("position.max_single_pct", 15.0),
                )
            elif r.timing_verdict == "CAUTION":
                r.position_size_pct = r.position_size_pct * _P.get("position.caution_multiplier", 0.7)

            r.position_size_pct = round(r.position_size_pct, 1)

            # Rebalance action
            if r.verdict in ("STRONG_BUY", "BUY") and r.timing_verdict == "BUY_NOW":
                r.rebalance_action = "INITIATE"
            elif r.verdict in ("STRONG_BUY", "BUY"):
                r.rebalance_action = "ADD"
            elif r.verdict == "HOLD":
                r.rebalance_action = "HOLD"
            elif r.verdict == "AVOID":
                r.rebalance_action = "TRIM"
            else:
                r.rebalance_action = "EXIT"

    def _stage8_conviction(self, alive_results: List[StockResult]):
        """Stage 8: Conviction ranking + position allocation."""

        for r in alive_results:
            score = 0.0

            # Valuation dimension (30 points)
            if r.margin_of_safety is not None:
                mos = r.margin_of_safety
                mos_excellent = _P.get("scoring.mos_band_excellent", 0.33)
                mos_good = _P.get("scoring.mos_band_good", 0.20)
                mos_fair = _P.get("scoring.mos_band_fair", 0.10)
                val_max = _P.get("scoring.valuation_max_points", 30)
                if mos >= mos_excellent:
                    score += val_max
                elif mos >= mos_good:
                    score += val_max * 0.8
                elif mos >= mos_fair:
                    score += val_max * 0.53
                elif mos >= 0:
                    score += val_max * 0.27

            # School consensus (25 points)
            score += r.school_consensus_score * 0.25

            # Financial safety (20 points)
            tier_scores = {
                "FORTRESS": 20, "SOLID": 14, "NEUTRAL": 8, "FRAGILE": 3, "DANGER": 0,
            }
            score += tier_scores.get(r.risk_tier, 8)

            # Moat (10 points)
            moat_scores = {"Wide": 10, "Narrow": 5, "None": 0}
            score += moat_scores.get(r.moat, 0)

            # LLM qualitative adjustment (10 points)
            if r.llm_analysis:
                score += 5  # Base bonus for having LLM analysis
                if r.realtime_research:
                    score += 3  # Bonus for realtime info
                if r.knowledge_snippets:
                    score += 2  # Bonus for book knowledge

            # Committee debate adjustment (NEW — up to 15 points)
            if r.debate_record:
                cv = r.committee_verdict
                cc = r.committee_confidence
                if cv in ("STRONG_BUY", "BUY") and cc >= 0.6:
                    score += 15 * cc  # Strong committee endorsement
                elif cv == "HOLD":
                    score += 5 * cc
                elif cv == "AVOID":
                    score -= 10 * cc  # Committee concern reduces score
                if r.committee_veto_triggered:
                    score -= 20  # Veto is a major penalty

            # Sentiment bonus (5 points) — from school strong count
            strong_bonus = min(len(r.strong_schools) * 1.5, 5)
            score += strong_bonus

            r.composite_score = round(max(0, min(100, score)), 1)

            # Conviction level
            strong_count = len(r.strong_schools)
            mos = r.margin_of_safety or -1

            if strong_count >= _P.get("conviction.highest_strong_schools", 3) and mos >= _P.get("conviction.highest_mos_min", 0.30) and r.risk_tier == "FORTRESS" and r.moat == "Wide":
                r.conviction = ConvictionLevel.HIGHEST.value
            elif strong_count >= _P.get("conviction.high_strong_schools", 2) and mos >= _P.get("conviction.high_mos_min", 0.15) and r.risk_tier in ("FORTRESS", "SOLID"):
                r.conviction = ConvictionLevel.HIGH.value
            elif strong_count >= 1 or (mos is not None and mos >= 0):
                r.conviction = ConvictionLevel.MEDIUM.value
            elif r.gate_passed:
                r.conviction = ConvictionLevel.LOW.value
            else:
                r.conviction = ConvictionLevel.NONE.value

            # Verdict
            if r.composite_score >= _P.get("verdict.strong_buy_score", 75) and r.conviction in ("HIGHEST", "HIGH"):
                r.verdict = Verdict.STRONG_BUY.value
            elif r.composite_score >= _P.get("verdict.buy_score", 55) and r.conviction in ("HIGHEST", "HIGH", "MEDIUM"):
                r.verdict = Verdict.BUY.value
            elif r.composite_score >= _P.get("verdict.hold_score", 35):
                r.verdict = Verdict.HOLD.value
            elif r.composite_score >= _P.get("verdict.avoid_score", 20):
                r.verdict = Verdict.AVOID.value
            else:
                r.verdict = Verdict.REJECT.value

            # Selection reasons
            r.selection_reasons = []
            if r.best_school:
                r.selection_reasons.append(f"最适合{r.best_school}流派")
            if r.strong_schools:
                r.selection_reasons.append(f"{len(r.strong_schools)}个流派强推")
            if r.margin_of_safety and r.margin_of_safety > 0:
                r.selection_reasons.append(f"安全边际{r.margin_of_safety*100:.0f}%")
            if r.moat == "Wide":
                r.selection_reasons.append("宽护城河")
            if r.risk_tier == "FORTRESS":
                r.selection_reasons.append("堡垒级财务")
            if r.f_score >= 7:
                r.selection_reasons.append(f"F-Score {r.f_score}/9")

        # Sort by composite score
        alive_results.sort(key=lambda x: x.composite_score, reverse=True)

        # Position allocation (信念加权)
        if alive_results:
            raw_weights = []
            for r in alive_results:
                conv_mult = {
                    "HIGHEST": 2.0, "HIGH": 1.5, "MEDIUM": 1.0, "LOW": 0.7, "NONE": 0.3,
                }.get(r.conviction, 1.0)
                raw_weights.append(r.composite_score * conv_mult)

            total_raw = sum(raw_weights) or 1
            for r, rw in zip(alive_results, raw_weights):
                r.position_weight = round(rw / total_raw * 100, 1)

    # ═══════════════════════════════════════════════════════════════
    #  Helpers
    # ═══════════════════════════════════════════════════════════════

    def _build_single_stock_snapshot(self, r: StockResult, data_map: Dict[str, Any]) -> str:
        """Build comprehensive data snapshot for a single stock (for per-stock LLM analysis)."""
        lines = []
        lines.append(f"=== {r.symbol} ({r.name}) ===")
        lines.append(f"行业: {r.sector}/{r.industry}  价格: ${r.price or 0:.2f}")
        lines.append(f"数据源: {r.data_source}  数据质量: {r.data_quality:.0f}%\n")

        # Fundamentals from data_map
        if r.symbol in data_map:
            d = data_map[r.symbol]["dict"]
            lines.append("--- 核心指标 ---")
            lines.append(f"PE(TTM): {d.get('pe', 'N/A')}  Forward PE: {d.get('forward_pe', 'N/A')}")
            lines.append(f"PB: {d.get('pb', 'N/A')}  PS: {d.get('ps', 'N/A')}")
            lines.append(f"ROE: {_fmt_pct(d.get('roe'))}  EPS: ${d.get('eps', 'N/A')}")
            lines.append(f"净利润率: {_fmt_pct(d.get('profit_margin'))}  营业利润率: {_fmt_pct(d.get('operating_margin'))}")
            lines.append(f"市值: ${_fmt_big(d.get('market_cap'))}  营收: ${_fmt_big(d.get('revenue'))}")
            lines.append(f"流动比率: {d.get('current_ratio', 'N/A')}  D/E: {d.get('debt_to_equity', 'N/A')}")
            lines.append(f"自由现金流: ${_fmt_big(d.get('free_cash_flow'))}  净利润: ${_fmt_big(d.get('net_income'))}")
            lines.append(f"利息覆盖率: {d.get('interest_coverage_ratio', 'N/A')}")
            lines.append(f"股息率: {d.get('dividend_yield', 'N/A')}%  连续分红: {d.get('consecutive_dividend_years', 'N/A')} 年")
            lines.append(f"10年平均EPS: ${d.get('avg_eps_10y', 'N/A')}  EPS CAGR: {_fmt_pct(d.get('earnings_growth_10y'))}")
            lines.append(f"盈利年数: {d.get('profitable_years', 'N/A')}/10\n")

        # Forensics
        lines.append("--- 财务排雷 ---")
        lines.append(f"F-Score: {r.f_score}/9  Z-Score: {r.z_score or 'N/A'}  M-Score: {r.m_score or 'N/A'}")
        lines.append(f"风险层级: {r.risk_tier}")
        if r.red_flags:
            for rf in r.red_flags[:5]:
                lines.append(f"  红旗: {rf.get('name', '')}: {rf.get('detail', '')}")
        lines.append("")

        # School evaluations
        lines.append("--- 七流派量化评分 ---")
        school_names = {
            "graham": "Graham深度价值", "buffett": "Buffett护城河",
            "quantitative": "量化价值", "quality": "品质投资",
            "valuation": "Damodaran估值", "contrarian": "逆向价值", "garp": "GARP成长",
        }
        for sk, sd in r.school_results.items():
            name = school_names.get(sk, sk)
            score = sd.get("score", 0)
            max_s = sd.get("max_score", 1)
            pct = score / max_s * 100 if max_s else 0
            rec = sd.get("recommendation", "N/A")
            lines.append(f"  {name}: {score}/{max_s} ({pct:.0f}%) — {rec}")
        lines.append(f"共识度: {r.school_consensus_score:.0f}/100  最佳流派: {r.best_school}")
        lines.append(f"强推流派: {', '.join(r.strong_schools) if r.strong_schools else '无'}\n")

        # Valuations
        lines.append("--- 七模型估值 ---")
        model_names = {
            "grahamNumber": "Graham Number", "grahamIntrinsicValue": "Graham 内在价值",
            "epv": "EPV", "dcfValue": "DCF", "ddmValue": "DDM",
            "netNetValue": "Net-Net", "ownerEarningsValue": "Owner Earnings",
        }
        for mk, mv in r.valuations.items():
            name = model_names.get(mk, mk)
            diff = ((mv - r.price) / r.price * 100) if r.price and mv else 0
            lines.append(f"  {name}: ${mv:.2f} ({'+' if diff > 0 else ''}{diff:.0f}% vs 股价)")
        lines.append(f"共识内在价值: ${r.intrinsic_value or 0:.2f}  安全边际: {(r.margin_of_safety or 0)*100:.1f}%")
        lines.append(f"护城河: {r.moat}\n")

        # Filing data
        if r.filing_summary:
            lines.append("--- 最新财报数据 ---")
            lines.append(r.filing_summary[:4000])
            lines.append("")

        # Knowledge snippets
        if r.knowledge_snippets:
            lines.append("--- 书籍知识库参考 ---")
            lines.append(r.knowledge_snippets[0][:800])
            lines.append("")

        # Realtime research
        if r.realtime_research:
            lines.append("--- Perplexity 实时研究 ---")
            lines.append(r.realtime_research[:1000])

        return "\n".join(lines)

    def _build_comparison_snapshot(self, results: List[StockResult]) -> str:
        """Build a compact cross-stock comparison for basket analysis."""
        lines = [f"=== Basket 横向对比 ({len(results)} 只股票) ===\n"]

        # Summary table header
        lines.append(f"{'Stock':<10} {'Price':>8} {'PE':>6} {'ROE':>7} {'MoS':>7} {'F-Score':>8} {'Moat':>8} "
                      f"{'Schools':>8} {'Best School':>15} {'Verdict':>10}")
        lines.append("-" * 100)

        for r in results:
            roe_str = f"{r.school_consensus_score:.0f}%" if r.school_consensus_score else "N/A"
            mos_str = f"{(r.margin_of_safety or 0)*100:.0f}%" if r.margin_of_safety else "N/A"
            strong = len(r.strong_schools)
            lines.append(
                f"{r.symbol:<10} ${r.price or 0:>7.2f} "
                f"{'N/A':>6} "
                f"{roe_str:>7} {mos_str:>7} "
                f"{r.f_score:>4}/9   {r.moat:>8} "
                f"{strong:>4}强推  {r.best_school:>15} "
                f"{r.verdict:>10}"
            )

        lines.append("")

        # Detail per stock
        for r in results:
            lines.append(f"\n--- {r.symbol} ({r.name}) ---")
            lines.append(f"综合评分: {r.composite_score:.0f}/100 | 信念: {r.conviction} | 仓位: {r.position_weight:.1f}%")
            lines.append(f"估值: 7模型共识 ${r.intrinsic_value or 0:.2f} vs 股价 ${r.price or 0:.2f} = 安全边际 {(r.margin_of_safety or 0)*100:.0f}%")
            lines.append(f"排雷: F={r.f_score}/9, Z={r.z_score or 'N/A'}, M={r.m_score or 'N/A'}, 风险={r.risk_tier}")
            lines.append(f"流派共识: {r.school_consensus_score:.0f}/100, 强推: {', '.join(r.strong_schools) or '无'}")
            if r.selection_reasons:
                lines.append(f"选择理由: {'; '.join(r.selection_reasons)}")

        return "\n".join(lines)

    @staticmethod
    def _parse_school_opinions(llm_text: str) -> Dict[str, str]:
        """Extract per-school opinions from LLM analysis text."""
        schools = {}
        school_markers = {
            "graham": ["Graham", "格雷厄姆", "深度价值"],
            "buffett": ["Buffett", "巴菲特", "护城河"],
            "quantitative": ["量化", "Greenblatt", "O'Shaughnessy"],
            "quality": ["品质", "Quality", "Dorsey", "Cunningham"],
            "valuation": ["Damodaran", "达摩达兰", "估值派"],
            "contrarian": ["逆向", "Contrarian", "Spier", "Templeton"],
            "garp": ["GARP", "Lynch", "成长派"],
        }

        lines = llm_text.split("\n")
        current_school = None
        current_text = []

        for line in lines:
            # Check if this line starts a new school section
            found_school = None
            for school_key, markers in school_markers.items():
                if any(m in line for m in markers) and ("**" in line or "###" in line or "##" in line):
                    found_school = school_key
                    break

            if found_school:
                # Save previous school
                if current_school and current_text:
                    schools[current_school] = "\n".join(current_text).strip()
                current_school = found_school
                current_text = [line]
            elif current_school:
                # Check if we've hit a non-school section header
                if line.strip().startswith("## ") and not any(
                    m in line for markers in school_markers.values() for m in markers
                ):
                    # Save and reset
                    if current_text:
                        schools[current_school] = "\n".join(current_text).strip()
                    current_school = None
                    current_text = []
                else:
                    current_text.append(line)

        # Save last school
        if current_school and current_text:
            schools[current_school] = "\n".join(current_text).strip()

        return schools

    def _stage_event(self, stage_id: int, status: str, data: Optional[Dict] = None) -> Dict:
        """Build a stage progress SSE event."""
        stage_info = self.STAGES[stage_id - 1]
        event = {
            "event": "stage_update",
            "runId": self.run_id,
            "stage": stage_id,
            "stageName": stage_info["name"],
            "stageNameCn": stage_info["name_cn"],
            "status": status,
            "totalStages": 9,
        }
        if data:
            event["data"] = data

        # Record stats
        self.stage_stats[stage_id] = {
            "status": status,
            "alive": sum(1 for r in self.results if r.is_alive()),
            "eliminated": sum(1 for r in self.results if not r.is_alive()),
        }
        return event

    def _substep_event(self, stage_id: int, substep: str, status: str,
                       message: str, data: Optional[Dict] = None) -> Dict:
        """Build a sub-step progress SSE event for fine-grained Stage 7/9 updates."""
        event = {
            "event": "substep_update",
            "runId": self.run_id,
            "stage": stage_id,
            "substep": substep,
            "status": status,
            "message": message,
        }
        if data:
            event["data"] = data
        return event

    def _build_data_snapshot(self, results: List[StockResult]) -> str:
        """Build a compact data snapshot for LLM consumption."""
        lines = []
        for r in results:
            lines.append(f"\n=== {r.symbol} ({r.name}) ===")
            lines.append(f"行业: {r.sector}/{r.industry}  价格: ${r.price or 0:.2f}")
            lines.append(f"排雷: F={r.f_score}/9, Z={r.z_score or 'N/A'}, M={r.m_score or 'N/A'}, 风险={r.risk_tier}")
            if r.red_flags:
                for rf in r.red_flags[:3]:
                    lines.append(f"  ⚠ {rf.get('name','')}: {rf.get('detail','')}")
            lines.append(f"七流派: 共识度={r.school_consensus_score:.0f}/100, 最佳={r.best_school}, 强推={r.strong_schools}")
            if r.valuations:
                lines.append(f"估值: {json.dumps(r.valuations, default=str)}")
            lines.append(f"内在价值: ${r.intrinsic_value or 0:.2f}, 安全边际: {r.margin_of_safety or 0:.1%}, 护城河: {r.moat}")
            if r.knowledge_snippets:
                lines.append(f"书籍知识: {r.knowledge_snippets[0][:300]}...")
            if r.realtime_research:
                lines.append(f"实时研究: {r.realtime_research[:400]}...")
        return "\n".join(lines)

    def _build_final_report(self) -> Dict[str, Any]:
        """Build the final structured report."""
        alive = [r for r in self.results if r.is_alive()]
        eliminated = [r for r in self.results if not r.is_alive()]

        return {
            "runId": self.run_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "strategy": self.strategy,
            "strategyName": self.config.get("name", self.strategy),
            "totalInput": len(self.results),
            "totalAlive": len(alive),
            "totalEliminated": len(eliminated),
            "stageStats": self.stage_stats,
            "portfolio": [r.to_dict() for r in alive],
            "eliminated": [
                {
                    "symbol": r.symbol, "name": r.name,
                    "eliminatedAtStage": r.eliminated_at_stage,
                    "reason": r.gate_failures[-1] if r.gate_failures else "",
                }
                for r in eliminated
            ],
            "funnel": {
                "input": len(self.results),
                "afterData": sum(1 for r in self.results if r.eliminated_at_stage != 2),
                "afterGate": sum(1 for r in self.results if r.eliminated_at_stage not in (2, 3)),
                "afterForensics": sum(1 for r in self.results if r.is_alive()),
                "final": len(alive),
            },
            "llmAnalysis": alive[0].llm_analysis if alive and alive[0].llm_analysis else None,
            "comparativeMatrix": alive[0].comparative_matrix if alive and alive[0].comparative_matrix else None,
            "perStockAnalysis": {
                r.symbol: {
                    "analysis": r.llm_analysis,
                    "schoolOpinions": r.per_school_opinions,
                    "filingSummary": r.filing_summary if r.filing_summary else None,
                }
                for r in alive if r.llm_analysis
            },
            "timingSignals": {
                r.symbol: {
                    "timingScore": r.timing_score,
                    "timingVerdict": r.timing_verdict,
                    "rsi": r.rsi_14d,
                    "macd": r.macd_signal_str,
                    "ma200": r.ma200_position,
                    "week52Pct": r.week52_position,
                    "week52High": r.price_52w_high,
                    "week52Low": r.price_52w_low,
                    "volumeAnomaly": r.volume_anomaly,
                }
                for r in alive
            },
            "backtestResults": {
                r.symbol: {
                    "return1y": r.backtest_return_1y,
                    "return2y": r.backtest_return_2y,
                    "return3y": r.backtest_return_3y,
                    "sp5001y": r.backtest_sp500_1y,
                    "sp5002y": r.backtest_sp500_2y,
                    "sp5003y": r.backtest_sp500_3y,
                    "alpha1y": r.backtest_alpha_1y,
                    "alpha2y": r.backtest_alpha_2y,
                    "alpha3y": r.backtest_alpha_3y,
                    "maxDrawdown": r.backtest_max_drawdown,
                    "sharpe": r.backtest_sharpe,
                    "verdict": r.backtest_verdict,
                }
                for r in alive
            },
            "positionAdvice": {
                r.symbol: {
                    "buyPriceLow": r.buy_price_low,
                    "buyPriceHigh": r.buy_price_high,
                    "stopLoss": r.stop_loss_price,
                    "sizePct": r.position_size_pct,
                    "nextReview": r.next_review_date,
                    "action": r.rebalance_action,
                }
                for r in alive
            },
            # NEW: Committee debate records
            "committeeDebates": {
                r.symbol: r.debate_record
                for r in alive if r.debate_record
            },
            # NEW: Strategy rolling backtest
            "strategyBacktest": alive[0].strategy_backtest if alive and alive[0].strategy_backtest else None,
        }

    async def _persist_verdict(self, report: Dict):
        """Persist the verdict to database."""
        try:
            from app.agent.sink import run_sink
            await run_sink(report, self.run_id)
        except Exception as e:
            logger.warning(f"Failed to persist verdict: {e}")


# ═══════════════════════════════════════════════════════════════
#  Module-level helpers
# ═══════════════════════════════════════════════════════════════

def _fmt_pct(val) -> str:
    if val is None:
        return "N/A"
    return f"{val * 100:.1f}%"


def _fmt_big(val) -> str:
    if val is None:
        return "N/A"
    abs_v = abs(val)
    sign = "-" if val < 0 else ""
    if abs_v >= 1e12:
        return f"{sign}{abs_v/1e12:.1f}T"
    if abs_v >= 1e9:
        return f"{sign}{abs_v/1e9:.1f}B"
    if abs_v >= 1e6:
        return f"{sign}{abs_v/1e6:.1f}M"
    return f"{sign}{abs_v:,.0f}"
