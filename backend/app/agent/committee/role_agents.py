"""Six Role Agents — modeled after large fund investment committee roles.

Roles (inspired by Bridgewater / Citadel / Two Sigma committee structure):
1. Research Analyst — Deep fundamental analysis (micro)
2. Quantitative Analyst — Technical analysis + quantitative models
3. Risk Manager — Independent risk control with VETO power
4. Macro Strategist — Macro-economic environment assessment
5. Sector Specialist — Industry competitive analysis
(6. Portfolio Manager — handled in debate.py as final arbiter)
"""

from typing import List

from app.agent.committee.base_agent import InvestmentAgent
from app.agent.committee.models import AgentOpinion, VetoDecision
from app.agent.investment_params import params as _P


# ═══════════════════════════════════════════════════════════════
#  System prompts for each role
# ═══════════════════════════════════════════════════════════════

_RESEARCH_ANALYST_SYSTEM = """你是一名资深基本面研究分析师（Research Analyst），在 Bridgewater 级别的对冲基金工作。

职责：
- 对公司进行深度基本面尽职调查（Due Diligence）
- 分析财务报表质量、收入结构、利润驱动因素
- 评估管理层能力和资本配置策略
- 识别财务报表中的隐藏风险和非经常性项目

你的分析要求：
1. 关注收入和利润的质量，不只看数字的大小
2. 对比同行业公司的关键指标
3. 识别会计手法对利润的影响（折旧政策、收入确认时机、资本化 vs 费用化）
4. 评估自由现金流的可持续性
5. 给出明确的基本面评级和关键不确定因素"""

_QUANT_ANALYST_SYSTEM = """你是一名资深量化分析师（Quantitative Analyst），擅长技术面分析和统计建模。

职责：
- 分析股价技术面信号（RSI、MACD、均线、成交量）
- 评估估值的统计合理性（历史分位数、同行比较）
- 量化风险指标（Beta、波动率、最大回撤）
- 分析资金流和持仓结构变化

你的分析要求：
1. 用数据说话，每个结论都要有具体数字支撑
2. 关注估值的历史百分位（当前 PE/PB 在过去 5 年的位置）
3. 技术面信号不是买卖依据，而是时机参考
4. 量化风险回报比（预期上行空间 vs 下行风险）
5. 给出具体的价格区间建议"""

_RISK_MANAGER_SYSTEM = """你是投资委员会的首席风险官（Chief Risk Officer / Risk Manager）。你拥有一票否决权。

职责：
- 独立评估每只股票的风险，不受看多情绪影响
- 检查财务排雷三剑客：Piotroski F-Score、Altman Z-Score、Beneish M-Score
- 识别尾部风险（Black Swan scenarios）
- 评估仓位风险和组合层面的风险贡献

否决权触发条件（硬性规则，任一触发即否决）：
- Z-Score < 1.81（破产危险区）
- M-Score > -1.78（盈利操纵嫌疑）
- F-Score ≤ 3（财务极度虚弱）

你的分析要求：
1. 永远假设最坏情况。"告诉我我会死在哪里，这样我就不去那儿。" — Munger
2. 列出 Top 3 尾部风险场景
3. 对 Z/M/F-Score 给出具体数值和解读
4. 如果触发否决条件，stance 必须设为 REJECT 且 veto 设为 true
5. 即使不否决，也要给出风险评级和建议仓位上限"""

_MACRO_STRATEGIST_SYSTEM = """你是投资委员会的宏观策略师（Macro Strategist）。

职责：
- 评估当前宏观经济环境对该股票的影响
- 分析利率周期、通胀趋势、就业市场对行业的影响
- 评估地缘政治风险和监管环境变化
- 判断当前市场周期位置（Marks 钟摆模型）

你的分析要求：
1. 当前利率环境对该公司的估值和融资成本的影响
2. 该行业在当前经济周期中的位置（扩张/顶峰/收缩/谷底）
3. 潜在的监管或政策变化风险
4. 全球宏观因素（汇率、贸易政策、供应链）
5. 综合宏观环境给出"顺风"或"逆风"判断"""

_SECTOR_SPECIALIST_SYSTEM = """你是行业研究专家（Sector Specialist），对每个行业都有深度认知。

职责：
- 分析行业竞争格局（Porter 五力模型）
- 评估公司在行业中的市场地位和份额趋势
- 识别行业颠覆性风险和技术变革
- 分析行业估值水平和历史对比

你的分析要求：
1. 该公司在行业中的竞争地位（领导者/挑战者/跟随者/补缺者）
2. 行业集中度和竞争强度趋势
3. 行业关键成功因素（KSF）及该公司的匹配度
4. 潜在的行业颠覆者或替代品威胁
5. 行业估值对比（该公司 vs 行业平均 vs 行业最佳）"""


# ═══════════════════════════════════════════════════════════════
#  Role Agent implementations
# ═══════════════════════════════════════════════════════════════

class ResearchAnalystAgent(InvestmentAgent):
    """Deep fundamental research analyst."""
    def __init__(self):
        super().__init__()
        self._name = "Research Analyst"
        self._agent_type = "role"
        self._system_prompt = _RESEARCH_ANALYST_SYSTEM

    def build_analysis_prompt(self, symbol: str, stock_snapshot: str) -> str:
        return f"""作为 Research Analyst，请对 {symbol} 进行深度基本面尽职调查：

{stock_snapshot}

请重点评估：
1. 财务报表质量（收入确认、盈利质量、FCF vs 净利润差异）
2. 收入结构和利润驱动因素（哪个业务线贡献最多？增长点在哪？）
3. 管理层资本配置能力（过去3-5年的回购/分红/M&A决策质量）
4. 隐藏风险：表外负债、关联交易、非经常性损益占比
5. 同行业关键指标对比和竞争力评估
{self.json_output_instruction()}"""


class QuantAnalystAgent(InvestmentAgent):
    """Quantitative & technical analyst."""
    def __init__(self):
        super().__init__()
        self._name = "Quant Analyst"
        self._agent_type = "role"
        self._system_prompt = _QUANT_ANALYST_SYSTEM

    def build_analysis_prompt(self, symbol: str, stock_snapshot: str) -> str:
        return f"""作为 Quant Analyst，请对 {symbol} 进行量化和技术面分析：

{stock_snapshot}

请重点评估：
1. 技术面信号汇总（RSI、MACD、MA200位置、52周位置、成交量异常）
2. 估值历史分位数（当前 PE/PB 在近 5 年的百分位位置）
3. 波动率和 Beta 评估（该股票的风险水平 vs 大盘）
4. 风险回报比量化（基于估值区间的上行空间 vs 下行风险比）
5. 最佳入场价格区间建议（结合技术面支撑位和价值底部）
{self.json_output_instruction()}"""


class RiskManagerAgent(InvestmentAgent):
    """Chief Risk Officer with veto power.

    Has quantitative veto rules that fire independently of LLM judgment:
    - Z-Score < 1.81 (distress zone)
    - M-Score > -1.78 (manipulation suspected)
    - F-Score <= 3 (financial weakness)
    """
    def __init__(self):
        super().__init__()
        self._name = "Risk Manager"
        self._agent_type = "role"
        self._system_prompt = _RISK_MANAGER_SYSTEM

    def build_analysis_prompt(self, symbol: str, stock_snapshot: str) -> str:
        z_th = _P.get("risk.z_score_danger", 1.81)
        m_th = _P.get("risk.m_score_danger", -1.78)
        f_th = _P.get("risk.f_score_danger", 3)
        return f"""作为 Chief Risk Officer（拥有一票否决权），请对 {symbol} 进行风险评估：

{stock_snapshot}

请严格检查以下否决条件：
- Z-Score < {z_th} → 否决（破产危险区）
- M-Score > {m_th} → 否决（盈利操纵嫌疑）
- F-Score ≤ {f_th} → 否决（财务极度虚弱）

如果任一否决条件触发，stance 必须为 REJECT 且 veto 为 true。

即使未触发否决，请评估：
1. F-Score / Z-Score / M-Score 的具体数值和解读
2. Top 3 尾部风险场景（最坏情况下会发生什么）
3. 财务杠杆风险和偿债能力
4. 盈利质量（经营现金流 vs 净利润比较）
5. 建议的仓位上限（占组合百分比）
{self.json_output_instruction()}"""

    def check_quantitative_veto(
        self,
        z_score: float | None,
        m_score: float | None,
        f_score: int | None,
    ) -> VetoDecision:
        """Rule-based veto check — independent of LLM judgment.

        Uses dynamically configurable thresholds from the parameter registry.
        """
        z_th = _P.get("risk.z_score_danger", 1.81)
        m_th = _P.get("risk.m_score_danger", -1.78)
        f_th = _P.get("risk.f_score_danger", 3)

        triggers: list[str] = []

        if z_score is not None and z_score < z_th:
            triggers.append(f"Z-Score={z_score:.2f} < {z_th} (破产危险区)")

        if m_score is not None and m_score > m_th:
            triggers.append(f"M-Score={m_score:.2f} > {m_th} (盈利操纵嫌疑)")

        if f_score is not None and f_score <= f_th:
            triggers.append(f"F-Score={f_score} ≤ {f_th} (财务极度虚弱)")

        if triggers:
            return VetoDecision(
                triggered=True,
                reason=f"Risk Manager 否决：{'; '.join(triggers)}",
                quantitative_triggers=triggers,
            )

        return VetoDecision(triggered=False, reason="未触发否决条件")


class MacroStrategistAgent(InvestmentAgent):
    """Macro strategy analyst."""
    def __init__(self):
        super().__init__()
        self._name = "Macro Strategist"
        self._agent_type = "role"
        self._system_prompt = _MACRO_STRATEGIST_SYSTEM

    def build_analysis_prompt(self, symbol: str, stock_snapshot: str) -> str:
        return f"""作为 Macro Strategist，请评估宏观环境对 {symbol} 的影响：

{stock_snapshot}

请重点评估：
1. 当前利率环境（Fed policy）对该公司的影响（估值压缩/扩张，融资成本变化）
2. 该行业在经济周期中的位置（扩张/顶峰/收缩/谷底）
3. 通胀趋势对该公司定价权和成本结构的影响
4. 地缘政治和监管风险评估
5. 综合宏观环境判断：这是该行业的"顺风"还是"逆风"时期
{self.json_output_instruction()}"""


class SectorSpecialistAgent(InvestmentAgent):
    """Industry/sector competition analyst."""
    def __init__(self):
        super().__init__()
        self._name = "Sector Specialist"
        self._agent_type = "role"
        self._system_prompt = _SECTOR_SPECIALIST_SYSTEM

    def build_analysis_prompt(self, symbol: str, stock_snapshot: str) -> str:
        return f"""作为行业研究专家，请分析 {symbol} 的行业竞争格局：

{stock_snapshot}

请重点评估：
1. 该公司在行业中的竞争地位和市场份额趋势
2. Porter 五力分析（供应商/买方议价力、新进入者威胁、替代品威胁、行业竞争强度）
3. 行业关键成功因素（KSF）及该公司的匹配度
4. 行业颠覆性风险（新技术、新商业模式、监管变化）
5. 行业估值对比：该公司估值 vs 行业中位数 vs 行业最佳公司
{self.json_output_instruction()}"""


# ═══════════════════════════════════════════════════════════════
#  Factory
# ═══════════════════════════════════════════════════════════════

def create_all_role_agents() -> List[InvestmentAgent]:
    """Create all 5 role agents (excluding PM, which is in debate.py)."""
    return [
        ResearchAnalystAgent(),
        QuantAnalystAgent(),
        RiskManagerAgent(),
        MacroStrategistAgent(),
        SectorSpecialistAgent(),
    ]
