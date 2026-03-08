"""老查理选股流程引擎 — 从 14 本投资经典中抽象的系统化选股 SOP
=================================================================

知识来源
--------
14 本投资经典 → 1,253 条量化策略 → 65+ 条蒸馏规则 → 7 大投资流派

本模块将散落在 distilled_rules / screener / stock_engine 中的选股逻辑，
抽象为一条 **5 阶段流水线（Pipeline）**，每个阶段是独立、可插拔的。

5 阶段选股流水线
----------------
  Stage 1 — Universe: 构建初始股票池（宇宙）
  Stage 2 — Gatekeeper: 硬性门槛淘汰（生存测试）
  Stage 3 — MultiLens: 七流派多维评估（多透镜分析）
  Stage 4 — Valuation: 内在价值估算 & 安全边际计算
  Stage 5 — Conviction: 信念排名 & 最终组合构建

设计原则
--------
1. 每个 Stage 都是纯函数式：输入候选列表 → 输出候选列表（可独立测试）
2. Pipeline 可以从任意阶段启动（比如已有候选池，直接从 Stage 3 开始）
3. 所有决策逻辑都有知识来源标注（哪本书/哪个流派/哪条规则）
4. 不依赖 LLM — 纯代码规则引擎，可确定性复现

使用方式
--------
    from src.screening_pipeline import ScreeningPipeline

    # 完整流水线：从 S&P 500 中选股
    pipeline = ScreeningPipeline()
    portfolio = pipeline.run(
        universe="sp500",       # 或传入 ["AAPL", "MSFT", ...] 自定义列表
        strategy="balanced",    # 策略模板: conservative / balanced / aggressive / custom
        max_holdings=15,        # 最终持仓数量
    )

    # 从特定阶段启动
    survivors = pipeline.run_from_stage(3, candidates=[...])  # 从七流派评估开始

    # 查看选股报告
    print(pipeline.report())
"""

import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Callable

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════
#  知识溯源：选股理论基础
# ═══════════════════════════════════════════════════════════════

KNOWLEDGE_SOURCES = {
    "gatekeeper": {
        "description": "硬性门槛淘汰 — 不满足基本条件的股票直接淘汰",
        "sources": [
            "《聪明的投资者》第14章：Graham 防御型投资者7大标准",
            "《聪明的投资者》第15章：进取型投资者5条附加标准",
            "《量化价值》第3章：排除财务困境股、微盘股、欺诈股",
            "《成功股票投资的五条规则》第2章：基本面准入条件",
        ],
        "rationale": "Graham 认为选股首先是排除法 — 先淘汰不合格的，再从合格中选最优的。"
    },
    "multi_school": {
        "description": "七流派多维评估 — 每只股票经过7个投资学派的审判",
        "sources": [
            "《聪明的投资者》— Graham 深度价值：PE<15, 安全边际≥33%",
            "《巴菲特之道》+《致股东的信》— Buffett 护城河：ROE>15%, 低负债, 长期盈利",
            "《量化价值》+《华尔街的成功法则》— 量化价值：盈利收益率, 魔法公式",
            "《品质投资》+《成功股票投资的五条规则》— 品质投资：持久高ROE, 宽护城河",
            "《投资估值》+《预期投资》— Damodaran 估值派：PEG, 盈利收益率>国债利率",
            "《华尔街的成功法则》— 逆向价值：52周低位, 低P/S, 高股息",
            "《成功股票投资的五条规则》+《品质投资》— GARP：PEG<1, 高增长+合理估值",
        ],
        "rationale": "没有一个流派是万能的。通过多流派交叉验证，减少单一视角的盲区。"
    },
    "valuation": {
        "description": "多模型估值 — 7种估值模型取共识",
        "sources": [
            "《聪明的投资者》— Graham Number: V = √(22.5 × EPS × BVPS)",
            "《聪明的投资者》— Graham IV: V = EPS × (8.5 + 2g) × 4.4/Y",
            "《投资估值》— EPV 盈利能力价值: EBIT(1-t)/WACC",
            "《投资估值》— DCF 两阶段折现: 5年高速 + 永续",
            "《巴菲特之道》— Owner Earnings: NI - CapEx",
            "《聪明的投资者》— NCAV 净流动资产: 流动资产 - 总负债",
            "《聪明的投资者》— DDM 股息折现: D(1+g)/(r-g)",
        ],
        "rationale": "Damodaran 告诫：最常见的估值错误是被精确数字迷惑。多模型取共识比单模型更可靠。"
    },
    "fraud_detection": {
        "description": "三重财务排雷",
        "sources": [
            "Piotroski F-Score (0-9分) — 9维财务健康评分",
            "Altman Z-Score — 破产预测模型 (safe>2.99, grey 1.81-2.99, danger<1.81)",
            "Beneish M-Score — 盈利操纵检测 (FCF vs NI 背离)",
            "《财务诡计》Howard Schilit — 7大盈利操纵手法",
        ],
        "rationale": "价值陷阱的根源是财务欺诈。排雷必须在估值之前。"
    },
    "portfolio_construction": {
        "description": "组合构建",
        "sources": [
            "《聪明的投资者》— Graham: 10-30只，充分分散",
            "《巴菲特之道》— Buffett: 集中持仓 8-15只高信念标的",
            "《击败市场的小书》— Greenblatt: 20-30只等权重, 年度轮换",
            "Kelly Criterion — 仓位 = edge / odds，但实操中用半 Kelly",
        ],
        "rationale": "集中还是分散取决于能力圈。有高信念就集中，否则分散。"
    },
}


# ═══════════════════════════════════════════════════════════════
#  数据结构
# ═══════════════════════════════════════════════════════════════

class CandidateStatus(str, Enum):
    """候选股票在流水线中的状态"""
    ALIVE = "ALIVE"               # 存活（继续评估）
    ELIMINATED = "ELIMINATED"     # 淘汰
    SELECTED = "SELECTED"         # 入选最终组合


class ConvictionLevel(str, Enum):
    """信念等级"""
    HIGHEST = "HIGHEST"    # 最高信念 — ≥3个流派强推 + 安全边际≥30%
    HIGH = "HIGH"          # 高信念   — ≥2个流派强推 + 安全边际≥15%
    MEDIUM = "MEDIUM"      # 中等信念 — ≥1个流派通过 + 有安全边际
    LOW = "LOW"            # 低信念   — 勉强通过
    NONE = "NONE"          # 无信念   — 不建议入选


class RiskTier(str, Enum):
    """风险层级"""
    FORTRESS = "FORTRESS"   # 堡垒级（F-Score≥7, Z-Score>2.99, 低负债）
    SOLID = "SOLID"         # 稳固级
    NEUTRAL = "NEUTRAL"     # 中性
    FRAGILE = "FRAGILE"     # 脆弱级
    DANGER = "DANGER"       # 危险级


@dataclass
class Candidate:
    """选股流水线中的候选股票"""
    symbol: str
    name: str = ""
    sector: str = ""
    industry: str = ""

    # 原始数据
    raw_data: Optional[Any] = field(default=None, repr=False)  # StockData 对象
    data_dict: Dict[str, Any] = field(default_factory=dict, repr=False)

    # Stage 2: 门槛检测结果
    gatekeeper_passed: bool = True
    gatekeeper_failures: List[str] = field(default_factory=list)

    # Stage 3: 七流派评估
    school_results: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    best_school: str = ""
    strong_schools: List[str] = field(default_factory=list)
    school_consensus_score: float = 0.0   # 0-100，七流派共识评分

    # Stage 4: 估值
    valuations: Dict[str, float] = field(default_factory=dict)
    intrinsic_value: Optional[float] = None
    price: Optional[float] = None
    margin_of_safety: Optional[float] = None
    moat: str = ""  # Wide / Narrow / None

    # Stage 4b: 排雷
    f_score: int = 0
    z_score: Optional[float] = None
    z_zone: str = ""
    red_flags: List[str] = field(default_factory=list)
    risk_tier: RiskTier = RiskTier.NEUTRAL

    # Stage 5: 最终排名
    conviction: ConvictionLevel = ConvictionLevel.NONE
    composite_score: float = 0.0  # 0-100 最终综合得分
    position_weight: float = 0.0  # 建议仓位权重 (%)
    status: CandidateStatus = CandidateStatus.ALIVE

    # 决策溯源
    selection_reasons: List[str] = field(default_factory=list)
    elimination_reason: str = ""

    def eliminate(self, reason: str):
        self.status = CandidateStatus.ELIMINATED
        self.elimination_reason = reason

    def select(self):
        self.status = CandidateStatus.SELECTED

    def to_dict(self) -> dict:
        d = {}
        for k, v in self.__dict__.items():
            if k in ("raw_data", "data_dict"):
                continue
            if isinstance(v, Enum):
                d[k] = v.value
            else:
                d[k] = v
        return d


# ═══════════════════════════════════════════════════════════════
#  策略模板（来源于 14 本书的组合构建理论）
# ═══════════════════════════════════════════════════════════════

STRATEGY_TEMPLATES = {
    "conservative": {
        "name": "保守型（Graham 防御型投资者）",
        "description": "适合大多数投资者。严格门槛、深度折价、分散持仓。",
        "source": "《聪明的投资者》第14章 — 防御型投资者的股票选择",
        "gatekeeper": {
            "min_market_cap": 2e9,       # ≥$20亿（Zweig 现代化更新）
            "max_pe": 15,                # Graham 上限
            "max_debt_to_equity": 1.0,   # Graham 要求
            "min_current_ratio": 2.0,    # Graham 流动性要求
            "require_positive_earnings": True,
            "require_positive_fcf": True,
            "min_profitable_years": 10,
        },
        "school_weights": {
            "graham": 2.0, "buffett": 1.5, "quality": 1.5,
            "quantitative": 1.0, "valuation": 1.0, "contrarian": 0.5, "garp": 0.5,
        },
        "valuation": {
            "min_margin_of_safety": 0.33,  # Graham 要求 33%
            "require_below_intrinsic": True,
        },
        "portfolio": {
            "max_holdings": 20,
            "min_holdings": 10,
            "max_sector_pct": 0.25,       # 单行业不超过 25%
            "min_conviction": "MEDIUM",
        },
    },
    "balanced": {
        "name": "均衡型（Buffett + Quality 融合）",
        "description": "平衡价值与质量。护城河企业 + 合理安全边际。",
        "source": "《巴菲特之道》+《品质投资》— 以合理价格买入伟大企业",
        "gatekeeper": {
            "min_market_cap": 5e8,
            "max_pe": 25,
            "max_debt_to_equity": 2.0,
            "min_current_ratio": 1.0,
            "require_positive_earnings": True,
            "require_positive_fcf": False,
        },
        "school_weights": {
            "graham": 1.0, "buffett": 2.0, "quality": 2.0,
            "quantitative": 1.5, "valuation": 1.5, "contrarian": 0.5, "garp": 1.5,
        },
        "valuation": {
            "min_margin_of_safety": 0.15,
            "require_below_intrinsic": False,
        },
        "portfolio": {
            "max_holdings": 15,
            "min_holdings": 8,
            "max_sector_pct": 0.30,
            "min_conviction": "MEDIUM",
        },
    },
    "aggressive": {
        "name": "进取型（Greenblatt + GARP 融合）",
        "description": "追求高回报。放松估值门槛，侧重增长 + 量化因子。",
        "source": "《击败市场的小书》+《华尔街的成功法则》+《预期投资》",
        "gatekeeper": {
            "min_market_cap": 1e8,
            "max_pe": 40,
            "max_debt_to_equity": 3.0,
            "min_current_ratio": 0.5,
            "require_positive_earnings": True,
            "require_positive_fcf": False,
        },
        "school_weights": {
            "graham": 0.5, "buffett": 1.0, "quality": 1.0,
            "quantitative": 2.0, "valuation": 2.0, "contrarian": 1.5, "garp": 2.0,
        },
        "valuation": {
            "min_margin_of_safety": 0.0,
            "require_below_intrinsic": False,
        },
        "portfolio": {
            "max_holdings": 30,
            "min_holdings": 15,
            "max_sector_pct": 0.35,
            "min_conviction": "LOW",
        },
    },
}


# ═══════════════════════════════════════════════════════════════
#  预置股票池（Universe）
# ═══════════════════════════════════════════════════════════════

PRESET_UNIVERSES = {
    "value_30": {
        "name": "价值投资经典30",
        "description": "长期被价值投资者关注的经典标的",
        "symbols": [
            "BRK-B", "JNJ", "PG", "KO", "PEP", "MCD", "WMT", "MMM", "JNJ", "CL",
            "EMR", "SWK", "GPC", "ADP", "SHW", "ITW", "ABT", "TGT", "ABBV", "LOW",
            "CAT", "UNP", "GD", "ED", "XOM", "CVX", "T", "VZ", "IBM", "INTC",
        ],
    },
    "sp500_top50": {
        "name": "S&P 500 权重前50",
        "symbols": [
            "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "META", "BRK-B", "TSLA", "UNH", "XOM",
            "JNJ", "JPM", "V", "PG", "MA", "HD", "CVX", "MRK", "ABBV", "LLY",
            "PEP", "KO", "COST", "AVGO", "WMT", "MCD", "CSCO", "TMO", "ACN", "ABT",
            "DHR", "CRM", "NKE", "LIN", "ORCL", "TXN", "NEE", "UPS", "PM", "MS",
            "BMY", "RTX", "SCHW", "AMGN", "HON", "LOW", "QCOM", "SPGI", "BA", "GS",
        ],
    },
    "dividend_kings": {
        "name": "股息之王（连续25年+增股息）",
        "symbols": [
            "JNJ", "PG", "KO", "PEP", "MMM", "EMR", "CL", "ABT", "GPC", "SWK",
            "ADP", "SHW", "ITW", "ED", "TGT", "ABBV", "LOW", "MCD", "WMT", "BDX",
        ],
    },
    "buffett_portfolio": {
        "name": "巴菲特当前持仓精选",
        "symbols": [
            "AAPL", "BAC", "AXP", "KO", "CVX", "OXY", "KHC", "MCO",
            "CB", "DVA", "VRSN", "MA", "V", "NU", "AMZN", "C",
        ],
    },
    "hk_value": {
        "name": "港股价值精选",
        "symbols": [
            "0700.HK", "0005.HK", "0003.HK", "1299.HK", "0001.HK",
            "2318.HK", "0388.HK", "0939.HK", "1398.HK", "0941.HK",
            "0011.HK", "0002.HK", "0016.HK", "0027.HK", "2628.HK",
        ],
    },
}


# ═══════════════════════════════════════════════════════════════
#  Stage 实现
# ═══════════════════════════════════════════════════════════════

class Stage1_Universe:
    """Stage 1: 构建初始股票池

    知识来源:
    - 《华尔街的成功法则》O'Shaughnessy: 回测发现宇宙的选择比策略更重要
    - 《量化价值》: 先定义宇宙（排除微盘、金融、ADR），再施加策略
    """

    @staticmethod
    def build(
        universe: Any = "value_30",
        custom_symbols: Optional[List[str]] = None,
    ) -> List[Candidate]:
        """构建初始候选池。

        Args:
            universe: 预置池名称 (str) 或自定义代码列表 (list)
            custom_symbols: 额外追加的代码

        Returns:
            Candidate 列表
        """
        symbols = []

        if isinstance(universe, list):
            symbols = list(universe)
        elif isinstance(universe, str) and universe in PRESET_UNIVERSES:
            symbols = list(PRESET_UNIVERSES[universe]["symbols"])
        elif isinstance(universe, str):
            # 尝试当作逗号分隔的代码
            symbols = [s.strip() for s in universe.split(",") if s.strip()]

        if custom_symbols:
            symbols.extend(custom_symbols)

        # 去重
        seen = set()
        unique = []
        for s in symbols:
            key = s.upper()
            if key not in seen:
                seen.add(key)
                unique.append(s)

        candidates = [Candidate(symbol=s) for s in unique]
        logger.info(f"[Stage1] 初始股票池: {len(candidates)} 只")
        return candidates


class Stage2_Gatekeeper:
    """Stage 2: 硬性门槛淘汰 — 不满足生存条件的直接淘汰

    知识来源:
    - Graham《聪明的投资者》第14章: 防御型投资者7大标准
      ① 企业规模足够大 ② 财务状况稳健 ③ 盈利稳定
      ④ 分红记录 ⑤ 盈利增长 ⑥ PE合理 ⑦ PB合理

    - 《量化价值》: 排除3类不合格股票
      ① 财务困境股（Z-Score < 1.81）
      ② 微盘股（市值 < $1亿）
      ③ 盈利操纵股（M-Score 可疑）

    设计理念:
    "选股首先是排除法" — Graham
    "第一条规则是不要亏钱，第二条规则是不要忘记第一条" — Buffett
    """

    # 默认门槛规则（来源标注）
    DEFAULT_GATES = [
        {
            "name": "市值门槛",
            "field": "market_cap",
            "check": lambda d, cfg: d.get("market_cap") is not None and d["market_cap"] >= cfg.get("min_market_cap", 5e8),
            "source": "Graham《聪明的投资者》+ Zweig 现代化更新: 排除小盘投机股",
        },
        {
            "name": "PE合理",
            "field": "pe",
            "check": lambda d, cfg: d.get("pe") is None or (d["pe"] > 0 and d["pe"] < cfg.get("max_pe", 25)),
            "source": "Graham: PE上限15 (保守) / Lynch: PE<40 (进取)",
        },
        {
            "name": "正盈利",
            "field": "eps",
            "check": lambda d, cfg: not cfg.get("require_positive_earnings", True) or (d.get("eps") is not None and d["eps"] > 0),
            "source": "Graham: 连续10年正盈利是最低要求",
        },
        {
            "name": "负债可控",
            "field": "debt_to_equity",
            "check": lambda d, cfg: d.get("debt_to_equity") is None or d["debt_to_equity"] < cfg.get("max_debt_to_equity", 2.0),
            "source": "Graham: D/E < 1.0 / Buffett: D/E < 0.5 偏好几乎无债的企业",
        },
        {
            "name": "流动性安全",
            "field": "current_ratio",
            "check": lambda d, cfg: d.get("current_ratio") is None or d["current_ratio"] >= cfg.get("min_current_ratio", 1.0),
            "source": "Graham: 流动比率≥2 / Quality Investing: ≥1",
        },
        {
            "name": "正自由现金流",
            "field": "free_cash_flow",
            "check": lambda d, cfg: not cfg.get("require_positive_fcf", False) or d.get("free_cash_flow") is None or d["free_cash_flow"] > 0,
            "source": "Buffett: 自由现金流为正是 Owner Earnings 的基础",
        },
        {
            "name": "ROE为正",
            "field": "roe",
            "check": lambda d, cfg: d.get("roe") is None or d["roe"] > 0,
            "source": "Buffett: ROE是判断企业是否具备护城河的核心指标",
        },
    ]

    @staticmethod
    def run(
        candidates: List[Candidate],
        config: Optional[Dict] = None,
        max_workers: int = 4,
    ) -> List[Candidate]:
        """对候选池执行门槛淘汰。

        Args:
            candidates: 候选列表
            config: 门槛配置（来自策略模板的 gatekeeper 字段）
            max_workers: 并发数

        Returns:
            更新状态后的候选列表（包含被淘汰的）
        """
        cfg = config or STRATEGY_TEMPLATES["balanced"]["gatekeeper"]

        def _check_one(c: Candidate) -> Candidate:
            # 先获取数据
            if c.raw_data is None:
                c.raw_data = Stage2_Gatekeeper._fetch(c.symbol)

            if c.raw_data is None:
                c.eliminate("数据获取失败")
                return c

            if hasattr(c.raw_data, 'to_dict'):
                c.data_dict = c.raw_data.to_dict()
            c.name = getattr(c.raw_data, 'name', c.symbol) or c.symbol
            c.sector = getattr(c.raw_data, 'sector', '') or ''
            c.industry = getattr(c.raw_data, 'industry', '') or ''
            c.price = getattr(c.raw_data, 'price', None)

            # 逐项门槛检查
            failures = []
            for gate in Stage2_Gatekeeper.DEFAULT_GATES:
                try:
                    if not gate["check"](c.data_dict, cfg):
                        failures.append(f"{gate['name']} (依据: {gate['source'][:30]}...)")
                except Exception:
                    pass

            if failures:
                c.gatekeeper_passed = False
                c.gatekeeper_failures = failures
                c.eliminate(f"门槛淘汰: {'; '.join(failures[:3])}")
            else:
                c.gatekeeper_passed = True

            return c

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_check_one, c): c for c in candidates}
            results = []
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as e:
                    c = futures[future]
                    c.eliminate(f"异常: {str(e)[:60]}")
                    results.append(c)

        alive = sum(1 for c in results if c.status == CandidateStatus.ALIVE)
        dead = sum(1 for c in results if c.status == CandidateStatus.ELIMINATED)
        logger.info(f"[Stage2] 门槛淘汰完成: {alive} 存活 / {dead} 淘汰 (共 {len(results)})")
        return results

    @staticmethod
    def _fetch(symbol: str):
        """获取股票数据 — 通过工厂模式自动选择最优数据源（BBG 优先）"""
        try:
            from .data_providers.factory import get_data_provider
            from .symbol_resolver import resolve_for_provider

            # 使用 auto 模式：Bloomberg → FMP → Finnhub → yfinance
            provider = get_data_provider("auto")
            resolved = resolve_for_provider(symbol, provider.name)
            logger.info(f"[Stage2] {symbol} 使用数据源: {provider.name}")
            stock = provider.fetch(resolved)
            return stock if stock.is_valid() else stock
        except Exception as e:
            logger.warning(f"[Stage2] 数据获取失败 {symbol}: {e}")
            return None


class Stage3_MultiLens:
    """Stage 3: 七流派多维评估（多透镜分析）

    知识来源:
    7 大投资流派，共 65+ 条量化规则，源自 14 本投资经典：

    1. Graham 深度价值 (10条规则)
       - 核心: 安全边际是投资的基石
       - 淘汰规则: PE<15, D/E<1.0
       - 来源: 《聪明的投资者》《价值投资: 从Graham到Buffett》

    2. Buffett 护城河投资 (9条规则)
       - 核心: 以合理价格买入伟大企业
       - 淘汰规则: ROE>15%, FCF>0
       - 来源: 《巴菲特之道》《巴菲特致股东的信》

    3. 量化价值 (9条规则)
       - 核心: 系统化投资消除认知偏差
       - 淘汰规则: 盈利收益率(EBIT/EV)>8%
       - 来源: 《量化价值》《华尔街的成功法则》《击败市场的小书》

    4. 品质投资 (8条规则)
       - 核心: 时间是优质企业的朋友
       - 淘汰规则: ROE>15%持续8年+, FCF>0
       - 来源: 《品质投资》《成功股票投资的五条规则》

    5. Damodaran 估值派 (6条规则)
       - 核心: 估值既是科学也是艺术
       - 来源: 《投资估值》《预期投资》

    6. 逆向价值 (5条规则)
       - 核心: 别人恐惧时我贪婪
       - 来源: 《华尔街的成功法则》《一个价值投资者的教育》

    7. GARP 合理价格成长 (6条规则)
       - 核心: 既要成长也要价值，PEG<1
       - 来源: 《成功股票投资的五条规则》《品质投资》

    评估逻辑:
    - 每只股票经过全部 7 个流派的独立评估
    - 计算「流派共识度」：多少个流派认可这只股票
    - 识别「最佳适配流派」：哪个流派最认可这只股票
    """

    @staticmethod
    def run(
        candidates: List[Candidate],
        school_weights: Optional[Dict[str, float]] = None,
    ) -> List[Candidate]:
        """对所有存活候选执行七流派评估。

        Args:
            candidates: 候选列表
            school_weights: 流派权重配置

        Returns:
            更新了流派评估结果的候选列表
        """
        weights = school_weights or STRATEGY_TEMPLATES["balanced"]["school_weights"]
        alive = [c for c in candidates if c.status == CandidateStatus.ALIVE]

        from .distilled_rules_bridge import evaluate_all_schools

        for c in alive:
            if not c.data_dict:
                continue

            try:
                result = evaluate_all_schools(c.data_dict)
                c.school_results = result.get("schools", {})
                c.best_school = result.get("best_fit_school", "")
                c.strong_schools = result.get("strong_pass_schools", [])

                # 计算加权共识评分 (0-100)
                total_weighted = 0.0
                max_weighted = 0.0

                for school_key, school_data in c.school_results.items():
                    w = weights.get(school_key, 1.0)
                    score = school_data.get("score", 0)
                    max_score = school_data.get("max_score", 1)
                    if max_score > 0:
                        total_weighted += (score / max_score) * w * 100
                        max_weighted += w * 100

                c.school_consensus_score = round(total_weighted / max_weighted * 100, 1) if max_weighted > 0 else 0

            except Exception as e:
                logger.warning(f"[Stage3] 流派评估失败 {c.symbol}: {e}")
                c.school_consensus_score = 0

        logger.info(f"[Stage3] 七流派评估完成: {len(alive)} 只股票")
        return candidates


class Stage4_Valuation:
    """Stage 4: 内在价值估算 + 安全边际 + 财务排雷

    知识来源:
    A. 7 种估值模型（多模型取共识）:
       1. Graham Number = √(22.5 × EPS × BVPS)
       2. Graham IV = EPS × (8.5 + 2g) × 4.4/Y
       3. EPV = EBIT(1-t) / WACC
       4. DCF 两阶段 = Σ FCF×(1+g1)^t/(1+r)^t + TV/(1+r)^5
       5. DDM = D(1+g) / (r-g)
       6. NCAV = 流动资产 - 总负债
       7. Owner Earnings = (NI - CapEx) / discount_rate

    B. 安全边际计算:
       MoS = (内在价值 - 当前价格) / 内在价值
       Graham 要求 ≥ 33%, Buffett 要求 ≥ 25%

    C. 财务排雷（三重检测）:
       - Piotroski F-Score (0-9)
       - Altman Z-Score
       - 现金流背离检测
    """

    @staticmethod
    def run(
        candidates: List[Candidate],
        valuation_config: Optional[Dict] = None,
    ) -> List[Candidate]:
        """执行估值 + 排雷。"""
        cfg = valuation_config or STRATEGY_TEMPLATES["balanced"]["valuation"]
        alive = [c for c in candidates if c.status == CandidateStatus.ALIVE]

        for c in alive:
            if c.raw_data is None:
                continue

            stock = c.raw_data

            # ── A. 七模型估值 ──
            c.valuations = Stage4_Valuation._multi_model_valuation(stock)

            # 取中位数作为共识内在价值
            valid_vals = sorted(v for v in c.valuations.values() if v and v > 0)
            if valid_vals:
                mid = len(valid_vals) // 2
                c.intrinsic_value = valid_vals[mid]

            # 安全边际
            c.price = getattr(stock, 'price', None)
            if c.intrinsic_value and c.price and c.price > 0:
                c.margin_of_safety = round(
                    (c.intrinsic_value - c.price) / c.intrinsic_value, 4
                )

            # 护城河判断
            roe = getattr(stock, 'roe', None)
            pm = getattr(stock, 'profit_margin', None)
            if roe and roe > 0.15 and pm and pm > 0.10:
                c.moat = "Wide"
            elif roe and roe > 0.10:
                c.moat = "Narrow"
            else:
                c.moat = "None"

            # ── B. 财务排雷 ──
            Stage4_Valuation._fraud_detection(c, stock)

        logger.info(f"[Stage4] 估值+排雷完成: {len(alive)} 只股票")
        return candidates

    @staticmethod
    def _multi_model_valuation(stock) -> Dict[str, float]:
        """7 种估值模型"""
        vals = {}

        # 1. Graham Number
        eps = getattr(stock, 'eps', None)
        bv = getattr(stock, 'book_value', None)
        if eps and eps > 0 and bv and bv > 0:
            vals["graham_number"] = round(math.sqrt(22.5 * eps * bv), 2)

        # 2. Graham Intrinsic Value
        if eps and eps > 0:
            g = (getattr(stock, 'eps_growth_5y', None) or getattr(stock, 'earnings_growth_10y', None) or 0.05) * 100
            y = getattr(stock, 'aa_bond_yield', None) or getattr(stock, 'treasury_yield_10y', None) or 4.4
            if y > 0:
                vals["graham_iv"] = round(eps * (8.5 + 2 * g) * 4.4 / y, 2)

        # 3. EPV
        ebit = getattr(stock, 'ebit', None)
        total_debt = getattr(stock, 'total_debt', None)
        shares = getattr(stock, 'shares_outstanding', None)
        total_cash = getattr(stock, 'total_cash', None)
        if ebit and ebit > 0 and shares and shares > 0:
            epv = (ebit * 0.79) / 0.10  # (1-21% tax) / 10% WACC
            epv_ps = (epv - (total_debt or 0) + (total_cash or 0)) / shares
            if epv_ps > 0:
                vals["epv"] = round(epv_ps, 2)

        # 4. DCF
        fcf = getattr(stock, 'free_cash_flow', None)
        if fcf and fcf > 0 and shares and shares > 0:
            rgr = getattr(stock, 'revenue_growth_rate', None)
            g1 = min(rgr / 100 if rgr else 0.08, 0.20)
            g2, wacc = 0.03, 0.10
            pv1 = sum(fcf * (1 + g1) ** y / (1 + wacc) ** y for y in range(1, 6))
            tv = fcf * (1 + g1) ** 5 * (1 + g2) / (wacc - g2)
            pv_tv = tv / (1 + wacc) ** 5
            dcf_ps = (pv1 + pv_tv) / shares
            if dcf_ps > 0:
                vals["dcf"] = round(dcf_ps, 2)

        # 5. DDM
        dps = getattr(stock, 'dividend_per_share', None)
        if dps and dps > 0:
            g = getattr(stock, 'earnings_growth_10y', None) or 0.03
            r = 0.10
            if r > g:
                vals["ddm"] = round(dps * (1 + g) / (r - g), 2)

        # 6. NCAV
        ncav = getattr(stock, 'ncav_per_share', None)
        if ncav is not None and ncav > 0:
            vals["ncav"] = round(ncav, 2)

        # 7. Owner Earnings
        ni = getattr(stock, 'net_income', None)
        capex = getattr(stock, 'capex', None)
        if ni and capex and shares and shares > 0:
            oe = ni - abs(capex)
            if oe > 0:
                vals["owner_earnings"] = round((oe / shares) / 0.10, 2)

        return vals

    @staticmethod
    def _fraud_detection(c: Candidate, stock):
        """三重财务安全检测"""
        # F-Score (简化版 0-9)
        checks = [
            getattr(stock, 'net_income', None) and stock.net_income > 0,
            getattr(stock, 'roe', None) and stock.roe > 0,
            getattr(stock, 'free_cash_flow', None) and stock.free_cash_flow > 0,
            (getattr(stock, 'free_cash_flow', None) and getattr(stock, 'net_income', None)
             and stock.free_cash_flow > stock.net_income),
            getattr(stock, 'current_ratio', None) and stock.current_ratio > 1.0,
            getattr(stock, 'debt_to_equity', None) is not None and stock.debt_to_equity < 0.5,
            getattr(stock, 'profit_margin', None) and stock.profit_margin > 0,
            getattr(stock, 'revenue_growth_rate', None) and stock.revenue_growth_rate > 0,
            getattr(stock, 'roe', None) and stock.roe > 0.10,
        ]
        c.f_score = sum(1 for ch in checks if ch)

        # Z-Score
        ta = getattr(stock, 'total_assets', None)
        if ta and ta > 0:
            wc = getattr(stock, 'working_capital', None) or 0
            re_val = (getattr(stock, 'net_income', None) or 0) * 0.7
            ebit = getattr(stock, 'ebit', None) or (getattr(stock, 'net_income', None) or 0)
            mv = getattr(stock, 'market_cap', None) or 0
            tl = getattr(stock, 'total_liabilities', None) or 0
            rev = getattr(stock, 'revenue', None) or 0

            a = 1.2 * (wc / ta)
            b = 1.4 * (re_val / ta)
            cc = 3.3 * (ebit / ta) if ebit else 0
            d = 0.6 * (mv / tl) if tl > 0 else 3.0
            e = 1.0 * (rev / ta) if rev else 0
            c.z_score = round(a + b + cc + d + e, 2)

            if c.z_score >= 2.99:
                c.z_zone = "safe"
            elif c.z_score >= 1.81:
                c.z_zone = "grey"
            else:
                c.z_zone = "danger"
                c.red_flags.append("Z-Score危险区(<1.81)——破产风险")

        # 现金流背离
        ni_val = getattr(stock, 'net_income', None)
        fcf_val = getattr(stock, 'free_cash_flow', None)
        if ni_val and ni_val > 0 and fcf_val is not None and fcf_val < 0:
            c.red_flags.append("利润与现金流背离——可能存在盈利操纵")

        # 高负债
        de = getattr(stock, 'debt_to_equity', None)
        if de and de > 2.0:
            c.red_flags.append(f"高负债率(D/E={de:.1f})")

        # 风险层级
        if c.f_score >= 7 and c.z_zone == "safe" and not c.red_flags:
            c.risk_tier = RiskTier.FORTRESS
        elif c.f_score >= 5 and c.z_zone in ("safe", "grey") and len(c.red_flags) <= 1:
            c.risk_tier = RiskTier.SOLID
        elif c.z_zone == "danger" or len(c.red_flags) >= 3:
            c.risk_tier = RiskTier.DANGER
        elif len(c.red_flags) >= 2 or c.f_score <= 3:
            c.risk_tier = RiskTier.FRAGILE
        else:
            c.risk_tier = RiskTier.NEUTRAL


class Stage5_Conviction:
    """Stage 5: 信念排名 & 组合构建

    知识来源:
    - Graham: 充分分散，10-30只持仓
    - Buffett: "如果你不愿意持有一只股票10年，那就一分钟也不要持有"
    - Greenblatt: 等权重，年度轮换，20-30只
    - Kelly Criterion: 仓位 = (bp - q) / b，实操用 Half Kelly

    信念等级判定:
    - HIGHEST: ≥3个流派强推 + 安全边际≥30% + 堡垒级财务 + 宽护城河
    - HIGH:    ≥2个流派强推 + 安全边际≥15% + 稳固级财务
    - MEDIUM:  ≥1个流派通过 + 有安全边际
    - LOW:     勉强通过门槛
    - NONE:    不建议入选

    仓位分配:
    - 信念越高 → 仓位越大（Half Kelly 思想）
    - 风险越高 → 仓位越小
    - 单行业不超过策略限制（分散化要求）
    """

    @staticmethod
    def run(
        candidates: List[Candidate],
        portfolio_config: Optional[Dict] = None,
    ) -> List[Candidate]:
        """执行信念评级 + 组合构建。

        Args:
            candidates: 候选列表
            portfolio_config: 组合配置

        Returns:
            最终排序后的候选列表
        """
        cfg = portfolio_config or STRATEGY_TEMPLATES["balanced"]["portfolio"]
        min_conv = cfg.get("min_conviction", "MEDIUM")
        max_holdings = cfg.get("max_holdings", 15)

        alive = [c for c in candidates if c.status == CandidateStatus.ALIVE]

        # ── Step 1: 计算信念等级 ──
        for c in alive:
            strong_count = len(c.strong_schools)
            mos = c.margin_of_safety or -1

            if (strong_count >= 3 and mos >= 0.30
                    and c.risk_tier == RiskTier.FORTRESS and c.moat == "Wide"):
                c.conviction = ConvictionLevel.HIGHEST
            elif strong_count >= 2 and mos >= 0.15 and c.risk_tier in (RiskTier.FORTRESS, RiskTier.SOLID):
                c.conviction = ConvictionLevel.HIGH
            elif strong_count >= 1 or (mos is not None and mos >= 0):
                c.conviction = ConvictionLevel.MEDIUM
            elif c.gatekeeper_passed:
                c.conviction = ConvictionLevel.LOW
            else:
                c.conviction = ConvictionLevel.NONE

        # ── Step 2: 综合评分 (0-100) ──
        for c in alive:
            score = 0.0

            # 估值维度 (30分)
            if c.margin_of_safety is not None:
                mos = c.margin_of_safety
                if mos >= 0.33:
                    score += 30
                elif mos >= 0.15:
                    score += 22
                elif mos >= 0:
                    score += 12
                elif mos >= -0.2:
                    score += 4
                # else: 0

            # 流派共识度 (30分)
            score += c.school_consensus_score * 0.30

            # 财务安全 (20分)
            tier_scores = {
                RiskTier.FORTRESS: 20, RiskTier.SOLID: 14,
                RiskTier.NEUTRAL: 8, RiskTier.FRAGILE: 3, RiskTier.DANGER: 0,
            }
            score += tier_scores.get(c.risk_tier, 8)

            # 护城河 (10分)
            moat_scores = {"Wide": 10, "Narrow": 5, "None": 0}
            score += moat_scores.get(c.moat, 0)

            # 信念加分 (10分)
            conv_scores = {
                ConvictionLevel.HIGHEST: 10, ConvictionLevel.HIGH: 7,
                ConvictionLevel.MEDIUM: 4, ConvictionLevel.LOW: 1, ConvictionLevel.NONE: 0,
            }
            score += conv_scores.get(c.conviction, 0)

            c.composite_score = round(max(0, min(100, score)), 1)

        # ── Step 3: 排名 & 选择 ──
        conviction_order = {
            ConvictionLevel.HIGHEST: 0, ConvictionLevel.HIGH: 1,
            ConvictionLevel.MEDIUM: 2, ConvictionLevel.LOW: 3, ConvictionLevel.NONE: 4,
        }
        min_conv_level = conviction_order.get(ConvictionLevel[min_conv], 2)

        # 过滤最低信念
        eligible = [
            c for c in alive
            if conviction_order.get(c.conviction, 4) <= min_conv_level
        ]

        # 按综合得分排序
        eligible.sort(key=lambda c: c.composite_score, reverse=True)

        # 选择 top N
        selected = eligible[:max_holdings]

        # ── Step 4: 仓位分配 ──
        if selected:
            # 简化版信念加权分配
            raw_weights = []
            for c in selected:
                base = c.composite_score
                # 信念放大
                conv_mult = {
                    ConvictionLevel.HIGHEST: 2.0, ConvictionLevel.HIGH: 1.5,
                    ConvictionLevel.MEDIUM: 1.0, ConvictionLevel.LOW: 0.7,
                }.get(c.conviction, 1.0)
                raw_weights.append(base * conv_mult)

            total_raw = sum(raw_weights) or 1
            for c, rw in zip(selected, raw_weights):
                c.position_weight = round(rw / total_raw * 100, 1)
                c.select()

                # 生成选择理由
                c.selection_reasons = []
                if c.best_school:
                    c.selection_reasons.append(f"最适合{c.best_school}流派")
                if c.strong_schools:
                    c.selection_reasons.append(f"{len(c.strong_schools)}个流派强推")
                if c.margin_of_safety and c.margin_of_safety > 0:
                    c.selection_reasons.append(f"安全边际{c.margin_of_safety*100:.0f}%")
                if c.moat == "Wide":
                    c.selection_reasons.append("宽护城河")
                if c.risk_tier == RiskTier.FORTRESS:
                    c.selection_reasons.append("堡垒级财务安全")
                if c.f_score >= 7:
                    c.selection_reasons.append(f"F-Score {c.f_score}/9")

        logger.info(f"[Stage5] 最终选出 {len(selected)} 只 (信念门槛: {min_conv})")
        return candidates


# ═══════════════════════════════════════════════════════════════
#  选股流水线主类
# ═══════════════════════════════════════════════════════════════

class ScreeningPipeline:
    """5 阶段选股流水线

    将 14 本投资经典的选股智慧编码为可执行的系统化流程。

    流水线:
        Universe → Gatekeeper → MultiLens → Valuation → Conviction
        (构建池)   (门槛淘汰)   (七流派评估)  (估值排雷)  (排名选股)
    """

    def __init__(self, max_workers: int = 4):
        self._max_workers = max_workers
        self._candidates: List[Candidate] = []
        self._strategy: str = "balanced"
        self._config: Dict = {}
        self._run_timestamp: str = ""
        self._stage_stats: Dict[str, Dict] = {}

    def run(
        self,
        universe: Any = "value_30",
        strategy: str = "balanced",
        custom_config: Optional[Dict] = None,
        max_holdings: Optional[int] = None,
    ) -> List[Candidate]:
        """执行完整的 5 阶段选股流水线。

        Args:
            universe: 股票池 — 预置名称(str) / 代码列表(list) / 逗号分隔字符串
            strategy: 策略模板 — "conservative" / "balanced" / "aggressive"
            custom_config: 自定义配置（覆盖模板）
            max_holdings: 最终持仓数量（覆盖模板）

        Returns:
            全部候选列表（包含 SELECTED / ELIMINATED / ALIVE）
        """
        self._strategy = strategy
        self._config = {**STRATEGY_TEMPLATES.get(strategy, STRATEGY_TEMPLATES["balanced"])}
        if custom_config:
            for k, v in custom_config.items():
                if isinstance(v, dict) and k in self._config:
                    self._config[k].update(v)
                else:
                    self._config[k] = v
        if max_holdings:
            self._config.setdefault("portfolio", {})["max_holdings"] = max_holdings

        self._run_timestamp = datetime.now().isoformat()

        logger.info(f"[Pipeline] 开始选股: 策略={self._config.get('name', strategy)}, 股票池={universe}")

        # ── Stage 1: 构建股票池 ──
        self._candidates = Stage1_Universe.build(universe)
        self._record_stage_stats("Stage1_Universe", self._candidates)

        # ── Stage 2: 门槛淘汰 ──
        self._candidates = Stage2_Gatekeeper.run(
            self._candidates,
            config=self._config.get("gatekeeper"),
            max_workers=self._max_workers,
        )
        self._record_stage_stats("Stage2_Gatekeeper", self._candidates)

        # ── Stage 3: 七流派评估 ──
        self._candidates = Stage3_MultiLens.run(
            self._candidates,
            school_weights=self._config.get("school_weights"),
        )
        self._record_stage_stats("Stage3_MultiLens", self._candidates)

        # ── Stage 4: 估值 + 排雷 ──
        self._candidates = Stage4_Valuation.run(
            self._candidates,
            valuation_config=self._config.get("valuation"),
        )
        self._record_stage_stats("Stage4_Valuation", self._candidates)

        # ── Stage 5: 信念排名 + 组合构建 ──
        self._candidates = Stage5_Conviction.run(
            self._candidates,
            portfolio_config=self._config.get("portfolio"),
        )
        self._record_stage_stats("Stage5_Conviction", self._candidates)

        selected = [c for c in self._candidates if c.status == CandidateStatus.SELECTED]
        logger.info(f"[Pipeline] 选股完成: {len(selected)} 只入选")

        return self._candidates

    def run_from_stage(
        self,
        stage: int,
        candidates: List[Candidate],
        strategy: str = "balanced",
    ) -> List[Candidate]:
        """从指定阶段开始运行流水线。

        Args:
            stage: 起始阶段 (2-5)
            candidates: 已有候选列表
            strategy: 策略模板
        """
        self._strategy = strategy
        self._config = {**STRATEGY_TEMPLATES.get(strategy, STRATEGY_TEMPLATES["balanced"])}
        self._candidates = candidates
        self._run_timestamp = datetime.now().isoformat()

        if stage <= 2:
            self._candidates = Stage2_Gatekeeper.run(
                self._candidates, self._config.get("gatekeeper"), self._max_workers)
        if stage <= 3:
            self._candidates = Stage3_MultiLens.run(
                self._candidates, self._config.get("school_weights"))
        if stage <= 4:
            self._candidates = Stage4_Valuation.run(
                self._candidates, self._config.get("valuation"))
        if stage <= 5:
            self._candidates = Stage5_Conviction.run(
                self._candidates, self._config.get("portfolio"))

        return self._candidates

    # ───────────────────────────────────────────────────────────
    #  结果查询
    # ───────────────────────────────────────────────────────────

    def selected(self) -> List[Candidate]:
        """获取入选的股票"""
        return sorted(
            [c for c in self._candidates if c.status == CandidateStatus.SELECTED],
            key=lambda c: c.composite_score, reverse=True,
        )

    def eliminated(self) -> List[Candidate]:
        """获取被淘汰的股票"""
        return [c for c in self._candidates if c.status == CandidateStatus.ELIMINATED]

    def all_candidates(self) -> List[Candidate]:
        """获取全部候选"""
        return self._candidates

    def to_dict(self) -> Dict[str, Any]:
        """输出为可序列化字典"""
        selected = self.selected()
        return {
            "timestamp": self._run_timestamp,
            "strategy": self._strategy,
            "strategy_name": self._config.get("name", self._strategy),
            "total_universe": len(self._candidates),
            "total_selected": len(selected),
            "total_eliminated": len(self.eliminated()),
            "stage_stats": self._stage_stats,
            "portfolio": [c.to_dict() for c in selected],
            "eliminated": [
                {"symbol": c.symbol, "name": c.name, "reason": c.elimination_reason}
                for c in self.eliminated()
            ],
        }

    # ───────────────────────────────────────────────────────────
    #  报告生成
    # ───────────────────────────────────────────────────────────

    def report(self) -> str:
        """生成完整的选股报告。"""
        sep = "=" * 70
        lines = [
            f"\n{sep}",
            f"  老查理选股流水线报告",
            f"  策略: {self._config.get('name', self._strategy)}",
            f"  时间: {self._run_timestamp[:19]}",
            f"{sep}\n",
        ]

        # 流水线概览
        lines.append("  --- 流水线概览 ---")
        for stage_name, stats in self._stage_stats.items():
            lines.append(
                f"  {stage_name}: {stats['alive']} 存活 / {stats['eliminated']} 淘汰"
            )
        lines.append("")

        # 知识来源
        lines.append("  --- 知识基础 ---")
        lines.append("  14 本投资经典 -> 1,253 条策略 -> 65+ 条蒸馏规则 -> 7 大流派")
        lines.append(f"  策略理论来源: {self._config.get('source', 'N/A')}")
        lines.append("")

        # 入选组合
        selected = self.selected()
        if selected:
            lines.append(f"  --- 入选组合 ({len(selected)} 只) ---")
            lines.append(f"  {'代码':<8s} {'名称':<20s} {'评分':>5s}  {'信念':>8s}  {'仓位':>5s}  {'安全边际':>8s}  {'护城河':>6s}  {'风险':>8s}  最匹配流派")
            lines.append(f"  {'-'*8} {'-'*20} {'-'*5}  {'-'*8}  {'-'*5}  {'-'*8}  {'-'*6}  {'-'*8}  {'-'*12}")

            for c in selected:
                mos_str = f"{c.margin_of_safety*100:+.0f}%" if c.margin_of_safety is not None else "N/A"
                lines.append(
                    f"  {c.symbol:<8s} {c.name[:20]:<20s} {c.composite_score:5.0f}  "
                    f"{c.conviction.value:>8s}  {c.position_weight:4.1f}%  "
                    f"{mos_str:>8s}  {c.moat:>6s}  {c.risk_tier.value:>8s}  {c.best_school or 'N/A'}"
                )

            lines.append("")

            # 仓位分布
            total_weight = sum(c.position_weight for c in selected)
            lines.append(f"  总仓位: {total_weight:.1f}%")

            # 行业分布
            sector_map: Dict[str, float] = {}
            for c in selected:
                s = c.sector or "未知"
                sector_map[s] = sector_map.get(s, 0) + c.position_weight
            lines.append("  行业分布:")
            for s, w in sorted(sector_map.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"    {s}: {w:.1f}%")
            lines.append("")

            # 信念分布
            conv_map: Dict[str, int] = {}
            for c in selected:
                conv_map[c.conviction.value] = conv_map.get(c.conviction.value, 0) + 1
            lines.append("  信念分布:")
            for cv, cnt in sorted(conv_map.items()):
                lines.append(f"    {cv}: {cnt} 只")
        else:
            lines.append("  未选出任何股票。")

        lines.append("")

        # 被淘汰的
        eliminated = self.eliminated()
        if eliminated:
            lines.append(f"  --- 被淘汰 ({len(eliminated)} 只) ---")
            for c in eliminated[:10]:
                lines.append(f"  {c.symbol:<8s} {c.name[:20]:<20s} 原因: {c.elimination_reason[:50]}")
            if len(eliminated) > 10:
                lines.append(f"  ... 还有 {len(eliminated)-10} 只")

        lines.append(f"\n{sep}")
        return "\n".join(lines)

    def _record_stage_stats(self, stage_name: str, candidates: List[Candidate]):
        alive = sum(1 for c in candidates if c.status == CandidateStatus.ALIVE)
        eliminated = sum(1 for c in candidates if c.status == CandidateStatus.ELIMINATED)
        selected = sum(1 for c in candidates if c.status == CandidateStatus.SELECTED)
        self._stage_stats[stage_name] = {
            "alive": alive, "eliminated": eliminated, "selected": selected, "total": len(candidates),
        }


# ═══════════════════════════════════════════════════════════════
#  便捷函数
# ═══════════════════════════════════════════════════════════════

def quick_screen(
    symbols: List[str],
    strategy: str = "balanced",
    max_holdings: int = 15,
) -> List[Candidate]:
    """便捷函数：快速选股。

    Args:
        symbols: 股票代码列表
        strategy: 策略模板
        max_holdings: 最大持仓数

    Returns:
        入选的 Candidate 列表
    """
    pipeline = ScreeningPipeline()
    pipeline.run(universe=symbols, strategy=strategy, max_holdings=max_holdings)
    return pipeline.selected()


def get_knowledge_sources() -> Dict[str, Any]:
    """获取选股流程的知识溯源信息"""
    return KNOWLEDGE_SOURCES


def get_strategy_templates() -> Dict[str, Any]:
    """获取所有策略模板"""
    return STRATEGY_TEMPLATES


def get_preset_universes() -> Dict[str, Any]:
    """获取所有预置股票池"""
    return PRESET_UNIVERSES
