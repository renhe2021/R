"""Agent Evaluation Suite — Test questions to assess Old Charlie's investment knowledge.

This suite covers 7 dimensions of value investing expertise:
1. Conceptual Knowledge — Do you really understand the masters' philosophies?
2. Stock Analysis — Can you apply multi-school rules to real stocks?
3. Comparative Analysis — Can you compare stocks and rank them?
4. Risk Detection — Can you spot financial red flags?
5. Edge Cases — Can you handle tricky scenarios?
6. Behavioral Finance — Can you overcome cognitive biases?
7. Portfolio Construction — Can you build a value portfolio?

Each test has:
- question: The prompt to send to the Agent
- dimension: Which skill is being tested
- difficulty: easy / medium / hard / expert
- expected_elements: Key elements that a correct answer MUST contain
- anti_patterns: Things a correct answer should NOT contain
- rubric: Detailed scoring criteria (0-10)
"""

from typing import List, Dict, Any
from dataclasses import dataclass, field


@dataclass
class TestCase:
    """A single evaluation test case."""
    id: str
    question: str
    dimension: str
    difficulty: str  # easy | medium | hard | expert
    expected_elements: List[str]  # Must-have keywords/concepts
    anti_patterns: List[str] = field(default_factory=list)  # Should NOT appear
    rubric: Dict[str, int] = field(default_factory=dict)  # criterion -> max_points
    expected_tool_calls: List[str] = field(default_factory=list)  # Tools that should be invoked
    reference_answer_notes: str = ""  # Notes for the evaluator


# ═══════════════════════════════════════════════════════════════
#  Dimension 1: CONCEPTUAL KNOWLEDGE (理论知识)
# ═══════════════════════════════════════════════════════════════

CONCEPTUAL_TESTS = [
    TestCase(
        id="C01",
        question="什么是安全边际？为什么 Graham 认为它是投资中最重要的概念？请举例说明。",
        dimension="conceptual",
        difficulty="easy",
        expected_elements=["内在价值", "市场价格", "差额", "Graham", "保护", "下行风险", "33%"],
        anti_patterns=["止损", "技术分析", "短线"],
        rubric={
            "正确定义安全边际": 3,
            "引用Graham原文/理念": 2,
            "给出具体数值(如33%)": 2,
            "举例说明": 2,
            "语言流畅专业": 1,
        },
        reference_answer_notes="安全边际 = 内在价值与市场价格之间的差额。Graham要求至少33%。"
    ),
    TestCase(
        id="C02",
        question="请比较 Graham 和 Buffett 的投资哲学差异。为什么 Buffett 后来超越了 Graham 的框架？",
        dimension="conceptual",
        difficulty="medium",
        expected_elements=[
            "捡烟蒂", "低于清算价值", "护城河", "优质企业", "合理价格",
            "芒格", "ROE", "定性", "定量",
        ],
        anti_patterns=["没有区别", "完全一样"],
        rubric={
            "准确描述Graham的Deep Value方法": 2,
            "准确描述Buffett的Quality Moat方法": 2,
            "解释Buffett如何超越Graham": 3,
            "提及Munger的影响": 1,
            "引用相关书籍或名言": 2,
        },
        reference_answer_notes="Graham=纯数量化低价买入清算品; Buffett=合理价格买伟大企业; Munger影响了转变。"
    ),
    TestCase(
        id="C03",
        question="解释 Greenblatt 的魔法公式（Magic Formula）。它为什么有效？它的局限性是什么？",
        dimension="conceptual",
        difficulty="medium",
        expected_elements=[
            "盈利收益率", "资本回报率", "EBIT/EV", "ROIC", "排名",
            "系统化", "行为偏差", "小书",
        ],
        rubric={
            "正确解释两个因子": 3,
            "解释为什么有效": 2,
            "指出局限性": 2,
            "引用Greenblatt原著": 1,
            "给出实际使用建议": 2,
        },
    ),
    TestCase(
        id="C04",
        question="什么是 Owner Earnings（所有者盈余）？为什么 Buffett 认为它比净利润更重要？",
        dimension="conceptual",
        difficulty="hard",
        expected_elements=[
            "自由现金流", "净利润", "折旧", "摊销", "资本支出",
            "维护性资本支出", "会计利润", "现金",
        ],
        rubric={
            "正确定义Owner Earnings公式": 3,
            "解释与净利润的区别": 2,
            "解释为什么更可靠": 2,
            "引用Buffett原文": 2,
            "举例说明": 1,
        },
    ),
    TestCase(
        id="C05",
        question="Damodaran 认为估值中最常见的错误是什么？如何避免？",
        dimension="conceptual",
        difficulty="hard",
        expected_elements=[
            "偏见", "精确", "假设", "折现率", "增长率",
            "终值", "敏感性分析", "叙事",
        ],
        rubric={
            "列出至少3个常见错误": 3,
            "解释每个错误为什么危险": 2,
            "给出避免方法": 3,
            "引用Damodaran": 2,
        },
    ),
    TestCase(
        id="C06",
        question="什么是 Piotroski F-Score？它的9个组成部分分别是什么？为什么它对价值投资特别重要？",
        dimension="conceptual",
        difficulty="medium",
        expected_elements=[
            "9分", "盈利", "杠杆", "效率", "ROA", "现金流",
            "流动比率", "毛利率", "资产周转",
        ],
        rubric={
            "正确列出9个组成部分": 4,
            "解释每个部分的含义": 2,
            "解释对价值投资的意义": 2,
            "给出分数解读(如>7为好)": 2,
        },
    ),
    TestCase(
        id="C07",
        question="请解释 Beneish M-Score 是什么？-1.78 这个临界值的含义是什么？",
        dimension="conceptual",
        difficulty="hard",
        expected_elements=[
            "盈利操纵", "Beneish", "-1.78", "8个变量", "DSRI", "GMI",
            "应收账款", "毛利率",
        ],
        rubric={
            "正确定义M-Score": 3,
            "列出关键变量": 2,
            "解释-1.78临界值": 2,
            "实际应用建议": 2,
            "引用案例": 1,
        },
    ),
]


# ═══════════════════════════════════════════════════════════════
#  Dimension 2: STOCK ANALYSIS (个股分析)
# ═══════════════════════════════════════════════════════════════

STOCK_ANALYSIS_TESTS = [
    TestCase(
        id="S01",
        question="分析 BRK-B（伯克希尔·哈撒韦），用7大投资流派来评估它。",
        dimension="stock_analysis",
        difficulty="medium",
        expected_elements=["护城河", "ROE", "估值", "巴菲特", "保险", "浮存金"],
        expected_tool_calls=["evaluate_stock_rules", "get_stock_fundamentals"],
        rubric={
            "调用了evaluate_stock_rules": 2,
            "给出7个流派的评分": 2,
            "分析护城河和竞争优势": 2,
            "给出明确的买入/持有建议": 2,
            "引用相关投资智慧": 2,
        },
    ),
    TestCase(
        id="S02",
        question="分析苹果公司（AAPL），你会给它什么投资建议？",
        dimension="stock_analysis",
        difficulty="medium",
        expected_elements=["品牌", "生态系统", "自由现金流", "PE", "回购", "护城河"],
        expected_tool_calls=["evaluate_stock_rules", "get_stock_fundamentals", "run_valuation_analysis"],
        rubric={
            "获取了实时基本面数据": 2,
            "运行了多模型估值": 2,
            "分析了定性因素(品牌/生态)": 2,
            "给出了具体估值区间": 2,
            "给出明确建议和理由": 2,
        },
    ),
    TestCase(
        id="S03",
        question="用格雷厄姆深度价值的标准来评估 JPM（摩根大通）。它符合 Graham 的防御型投资者标准吗？",
        dimension="stock_analysis",
        difficulty="hard",
        expected_elements=[
            "PE < 15", "PE×PB", "连续盈利", "分红", "流动比率",
            "银行股", "特殊行业", "监管",
        ],
        expected_tool_calls=["evaluate_stock_rules"],
        rubric={
            "用Graham标准逐条评估": 3,
            "指出银行股的特殊性": 2,
            "解释为什么Graham标准不完全适用": 2,
            "建议更合适的评估框架": 2,
            "数据驱动的结论": 1,
        },
    ),
    TestCase(
        id="S04",
        question="帮我分析 COST（好市多），它的护城河是什么类型？值得长期持有吗？",
        dimension="stock_analysis",
        difficulty="medium",
        expected_elements=["会员制", "规模优势", "成本优势", "转换成本", "忠诚度", "周转率"],
        expected_tool_calls=["evaluate_stock_rules", "get_stock_fundamentals"],
        rubric={
            "正确识别护城河类型": 3,
            "分析竞争优势可持续性": 2,
            "估值分析": 2,
            "长期投资价值判断": 2,
            "引用Dorsey/Morningstar的护城河框架": 1,
        },
    ),
    TestCase(
        id="S05",
        question="分析 META（Meta Platforms），作为一个价值投资者，你怎么看科技股？",
        dimension="stock_analysis",
        difficulty="hard",
        expected_elements=[
            "网络效应", "自由现金流", "AI", "元宇宙", "广告",
            "用户基数", "盈利增长",
        ],
        expected_tool_calls=["evaluate_stock_rules", "run_valuation_analysis"],
        rubric={
            "从价值投资角度分析科技股": 3,
            "识别定量和定性因素": 2,
            "讨论增长与估值的平衡": 2,
            "风险分析": 2,
            "引用大师对科技股的观点": 1,
        },
    ),
]


# ═══════════════════════════════════════════════════════════════
#  Dimension 3: COMPARATIVE ANALYSIS (对比分析)
# ═══════════════════════════════════════════════════════════════

COMPARATIVE_TESTS = [
    TestCase(
        id="CP01",
        question="比较 KO（可口可乐）和 PEP（百事可乐），从价值投资角度，哪个更值得买入？",
        dimension="comparative",
        difficulty="medium",
        expected_elements=["品牌", "多元化", "ROE", "股息", "护城河", "估值", "PE"],
        expected_tool_calls=["evaluate_stock_rules", "get_stock_fundamentals"],
        rubric={
            "两只股票都获取了数据": 2,
            "逐项对比关键指标": 3,
            "分析各自护城河": 2,
            "给出明确的相对偏好": 2,
            "引用Buffett对KO的经典投资案例": 1,
        },
    ),
    TestCase(
        id="CP02",
        question="如果我只能选一只银行股长期持有，BRK-B、JPM 和 BAC 你推荐哪个？为什么？",
        dimension="comparative",
        difficulty="hard",
        expected_elements=["资产质量", "ROE", "分红", "护城河", "管理层", "估值"],
        expected_tool_calls=["evaluate_stock_rules"],
        rubric={
            "三只股票都做了评估": 2,
            "注意到BRK-B不是银行股": 2,
            "逐项比较关键指标": 2,
            "给出明确推荐及原因": 2,
            "讨论银行股的特殊风险": 2,
        },
    ),
    TestCase(
        id="CP03",
        question="从 GARP 角度比较 MSFT 和 GOOGL，哪个更像 Peter Lynch 会买的股票？",
        dimension="comparative",
        difficulty="hard",
        expected_elements=["PEG", "EPS增长", "PE", "营收增长", "ROE", "Lynch"],
        expected_tool_calls=["evaluate_stock_rules"],
        rubric={
            "正确应用GARP/Lynch标准": 3,
            "计算和比较PEG": 2,
            "分析增长可持续性": 2,
            "给出明确推荐": 2,
            "引用Lynch的选股原则": 1,
        },
    ),
]


# ═══════════════════════════════════════════════════════════════
#  Dimension 4: RISK DETECTION (风险识别)
# ═══════════════════════════════════════════════════════════════

RISK_TESTS = [
    TestCase(
        id="R01",
        question="如何识别一家公司是否在操纵盈利？请用 Schilit 的 7 大类财务诡计来解释。",
        dimension="risk",
        difficulty="hard",
        expected_elements=[
            "提前确认收入", "虚构收入", "一次性项目", "费用资本化",
            "M-Score", "Beneish", "现金流与利润背离",
        ],
        rubric={
            "列出至少5种财务诡计": 3,
            "每种给出识别方法": 2,
            "提到M-Score工具": 2,
            "引用实际案例(如安然)": 2,
            "给出防范建议": 1,
        },
    ),
    TestCase(
        id="R02",
        question="什么是「价值陷阱」(Value Trap)？如何区分真正的低估股和价值陷阱？",
        dimension="risk",
        difficulty="medium",
        expected_elements=[
            "低PE", "持续下跌", "基本面恶化", "行业衰退",
            "F-Score", "自由现金流", "竞争优势",
        ],
        rubric={
            "正确定义价值陷阱": 2,
            "列出区分标准": 3,
            "引用相关投资大师": 2,
            "给出实际识别方法": 2,
            "举例说明": 1,
        },
    ),
    TestCase(
        id="R03",
        question="一家公司连续3年净利润增长20%，但自由现金流持续为负且应收账款大幅增长。你怎么看？",
        dimension="risk",
        difficulty="hard",
        expected_elements=[
            "盈利质量", "应计利润", "现金流", "操纵", "M-Score",
            "警惕", "回避",
        ],
        anti_patterns=["买入", "看好", "推荐"],
        rubric={
            "识别出现金流与利润背离的危险": 3,
            "解释应收账款增长的含义": 2,
            "引用盈利质量检测方法": 2,
            "给出明确的回避建议": 2,
            "引用大师语录": 1,
        },
    ),
    TestCase(
        id="R04",
        question="帮我排雷 TSLA（特斯拉），它有哪些财务风险？",
        dimension="risk",
        difficulty="medium",
        expected_elements=["估值", "PE", "波动", "竞争", "利润率"],
        expected_tool_calls=["detect_financial_shenanigans", "get_stock_fundamentals"],
        rubric={
            "调用了排雷工具": 2,
            "分析了Z-Score/F-Score/M-Score": 2,
            "识别了估值风险": 2,
            "讨论了行业竞争风险": 2,
            "给出了明确的风险等级": 2,
        },
    ),
]


# ═══════════════════════════════════════════════════════════════
#  Dimension 5: EDGE CASES (边界情况)
# ═══════════════════════════════════════════════════════════════

EDGE_CASE_TESTS = [
    TestCase(
        id="E01",
        question="亚马逊长期PE超过100倍，但它是一笔伟大的投资。价值投资框架如何解释这种现象？",
        dimension="edge_case",
        difficulty="expert",
        expected_elements=[
            "自由现金流", "再投资", "护城河", "增长", "Bezos",
            "长期", "PE不适用",
        ],
        rubric={
            "承认传统PE指标的局限性": 2,
            "解释亚马逊的特殊商业模式": 3,
            "用FCF/增长视角重新评估": 2,
            "讨论价值投资框架的演进": 2,
            "保持诚实(不否认也不盲目接受)": 1,
        },
    ),
    TestCase(
        id="E02",
        question="如果一只股票 PE=5，PB=0.3，股息率8%，但 F-Score 只有 2/9，你会买吗？",
        dimension="edge_case",
        difficulty="hard",
        expected_elements=[
            "价值陷阱", "F-Score", "财务困境", "不会买", "回避",
            "基本面恶化",
        ],
        anti_patterns=["买入", "便宜", "推荐"],
        rubric={
            "识别这是价值陷阱而非低估": 3,
            "解释低F-Score的危险": 2,
            "引用Graham对'便宜不等于好'的警告": 2,
            "给出明确的回避建议": 2,
            "引用实际案例": 1,
        },
    ),
    TestCase(
        id="E03",
        question="一只中国A股在美国也有ADR上市，两边价格差异20%。怎么分析？",
        dimension="edge_case",
        difficulty="hard",
        expected_elements=[
            "套利", "流动性", "汇率", "VIE结构", "监管",
            "风险溢价", "信息不对称",
        ],
        rubric={
            "解释价差的可能原因": 3,
            "讨论A股/港股/ADR的差异": 2,
            "分析套利可行性": 2,
            "指出VIE结构等特殊风险": 2,
            "给出实际建议": 1,
        },
    ),
    TestCase(
        id="E04",
        question="在零利率环境下，Graham的'盈利收益率>国债利率'规则还有意义吗？",
        dimension="edge_case",
        difficulty="expert",
        expected_elements=[
            "利率环境", "风险溢价", "TINA", "估值膨胀",
            "调整", "仍有参考价值",
        ],
        rubric={
            "理解规则的本质(机会成本比较)": 3,
            "讨论低利率对估值的影响": 2,
            "提出调整建议": 2,
            "保持Graham精神但灵活应用": 2,
            "引用Damodaran对利率的观点": 1,
        },
    ),
]


# ═══════════════════════════════════════════════════════════════
#  Dimension 6: BEHAVIORAL FINANCE (行为金融)
# ═══════════════════════════════════════════════════════════════

BEHAVIORAL_TESTS = [
    TestCase(
        id="B01",
        question="一只股票从$200跌到$100，很多人说'打折了，该买入'。这种想法对吗？",
        dimension="behavioral",
        difficulty="medium",
        expected_elements=[
            "锚定效应", "内在价值", "价格≠价值", "基本面",
            "下跌原因", "不一定",
        ],
        anti_patterns=["赶紧买入", "抄底"],
        rubric={
            "识别锚定偏差": 3,
            "强调价格vs价值的区别": 2,
            "建议先分析下跌原因": 2,
            "引用Graham的市场先生比喻": 2,
            "给出正确的分析框架": 1,
        },
    ),
    TestCase(
        id="B02",
        question="为什么价值投资策略在理论上有效，但大多数人执行不了？",
        dimension="behavioral",
        difficulty="medium",
        expected_elements=[
            "耐心", "逆向", "从众心理", "损失厌恶",
            "短期思维", "情绪", "纪律",
        ],
        rubric={
            "列出至少3种行为偏差": 3,
            "解释每种如何破坏投资": 2,
            "引用Guy Spier/Munger的行为金融观点": 2,
            "给出克服建议": 2,
            "引用经典名言": 1,
        },
    ),
    TestCase(
        id="B03",
        question="Quantitative Value 这本书的核心观点是什么？为什么系统化投资能打败人类判断？",
        dimension="behavioral",
        difficulty="medium",
        expected_elements=[
            "认知偏差", "系统化", "因子", "回测", "纪律",
            "情绪", "过度自信",
        ],
        rubric={
            "正确总结书的核心论点": 3,
            "解释人类判断的系统性偏差": 2,
            "引用书中的关键数据/回测结果": 2,
            "讨论系统化方法的局限性": 2,
            "引用Gray/Carlisle": 1,
        },
    ),
]


# ═══════════════════════════════════════════════════════════════
#  Dimension 7: PORTFOLIO CONSTRUCTION (组合构建)
# ═══════════════════════════════════════════════════════════════

PORTFOLIO_TESTS = [
    TestCase(
        id="P01",
        question="如果我有100万美元，想构建一个 Graham 风格的防御型投资组合，你会怎么配置？",
        dimension="portfolio",
        difficulty="hard",
        expected_elements=[
            "分散", "15-30只", "大盘", "低PE", "分红",
            "债券", "安全边际", "行业分散",
        ],
        rubric={
            "给出具体配置方案": 3,
            "包含分散化建议": 2,
            "符合Graham防御型标准": 2,
            "提到债券比例": 1,
            "给出具体选股标准": 2,
        },
    ),
    TestCase(
        id="P02",
        question="如何用 Kelly 公式来决定仓位大小？Buffett 的集中持仓策略合理吗？",
        dimension="portfolio",
        difficulty="expert",
        expected_elements=[
            "Kelly", "概率", "赔率", "集中", "能力圈",
            "确信度", "半Kelly",
        ],
        rubric={
            "正确解释Kelly公式": 3,
            "解释Buffett集中策略的逻辑": 2,
            "讨论集中vs分散的利弊": 2,
            "给出实际建议(如半Kelly)": 2,
            "引用Buffett/Munger对集中投资的观点": 1,
        },
    ),
    TestCase(
        id="P03",
        question="一个价值投资组合应该每隔多久调仓？依据什么标准？",
        dimension="portfolio",
        difficulty="medium",
        expected_elements=[
            "年度", "季度", "催化剂", "目标价", "基本面变化",
            "税务", "交易成本",
        ],
        rubric={
            "给出合理的调仓频率": 2,
            "列出触发调仓的条件": 3,
            "讨论税务影响": 1,
            "引用不同大师的做法": 2,
            "区分持有vs卖出的标准": 2,
        },
    ),
]


# ═══════════════════════════════════════════════════════════════
#  Dimension 8: MASTER-LEVEL STRESS TESTS (大师级压力测试)
# ═══════════════════════════════════════════════════════════════

MASTER_TESTS = [
    TestCase(
        id="M01",
        question="NVDA（英伟达）过去2年涨了10倍，PE超过60。Howard Marks 会如何看待这种'确定性溢价'？价值投资者应该怎么参与？",
        dimension="master",
        difficulty="expert",
        expected_elements=[
            "第二层思维", "风险", "周期", "预期", "估值",
            "护城河", "AI", "均值回归", "共识",
        ],
        anti_patterns=["赶紧买入", "必涨", "错过了"],
        rubric={
            "运用Howard Marks第二层思维分析": 3,
            "区分'好公司'和'好投资'": 2,
            "分析AI周期风险和估值透支": 2,
            "给出有条件的参与策略(而非简单买/不买)": 2,
            "引用大师对热门股的忠告": 1,
        },
        reference_answer_notes="Marks的核心观点:好公司≠好投资。价格已经反映了乐观预期。第二层思维要问'共识错在哪?'"
    ),
    TestCase(
        id="M02",
        question="BABA（阿里巴巴）从$300跌到$70，PE不到10，FCF丰富。Seth Klarman 式的深度价值分析师会怎么看？这是不是'捡黄金'的机会？",
        dimension="master",
        difficulty="expert",
        expected_elements=[
            "VIE结构", "监管风险", "折价", "政策", "安全边际",
            "不可量化风险", "资本配置", "回购",
        ],
        anti_patterns=["稳赚", "必涨"],
        rubric={
            "识别不可量化的政治/监管风险": 3,
            "用Klarman的风险框架分析": 2,
            "讨论VIE结构对股东权益的影响": 2,
            "给出仓位控制建议(非全仓)": 2,
            "引用Klarman对不可量化风险的观点": 1,
        },
        reference_answer_notes="Klarman强调:安全边际无法保护你免受不可量化的风险。政治风险不可对冲。仓位必须控制。"
    ),
    TestCase(
        id="M03",
        question="2025年AI泡沫论甚嚣尘上。请用Howard Marks的'市场周期'框架分析当前科技股市场处于周期的哪个位置。",
        dimension="master",
        difficulty="expert",
        expected_elements=[
            "周期", "钟摆", "极端", "均值回归", "情绪",
            "资本流入", "估值扩张", "风险态度",
        ],
        rubric={
            "准确运用Marks周期理论(钟摆模型)": 3,
            "分析当前市场的情绪指标": 2,
            "给出周期位置判断(上半程/下半程)": 2,
            "提出应对策略(增减仓/防御性配置)": 2,
            "引用《周期》或《投资最重要的事》": 1,
        },
        reference_answer_notes="Marks钟摆模型:市场在恐惧和贪婪之间摆动，很少在中点停留。当前是否'太高了?'"
    ),
    TestCase(
        id="M04",
        question="WFC（富国银行）经历了虚假账户丑闻后被巴菲特减持。从'管理层诚信'角度，一个深度价值投资者应该如何重新评估护城河受损的公司？",
        dimension="master",
        difficulty="hard",
        expected_elements=[
            "管理层", "诚信", "护城河", "品牌受损", "监管",
            "合规成本", "文化", "恢复",
        ],
        expected_tool_calls=["evaluate_stock_rules", "detect_financial_shenanigans"],
        rubric={
            "分析管理层失信对护城河的永久性影响": 3,
            "引用Buffett对管理层诚信的强调": 2,
            "评估品牌恢复可能性和时间框架": 2,
            "给出是否值得重新买入的结论": 2,
            "讨论合规成本对未来盈利的影响": 1,
        },
    ),
    TestCase(
        id="M05",
        question="假设你管理一个10亿美元的价值基金，现在市场整体CAPE(席勒市盈率)达到35倍。你会采取什么策略？请给出具体的资产配置方案。",
        dimension="master",
        difficulty="expert",
        expected_elements=[
            "CAPE", "均值回归", "现金", "防御", "债券",
            "国际分散", "对冲", "耐心",
        ],
        rubric={
            "正确解读高CAPE的历史含义": 2,
            "给出具体配置比例(股/债/现金)": 3,
            "讨论是否需要国际分散": 2,
            "引用GMO/Marks/Grantham的观点": 1,
            "给出'如果市场下跌30%'的备选方案": 2,
        },
        reference_answer_notes="高CAPE历史上预示着低回报。但CAPE>30不等于立即崩盘。需要耐心等待机会+保持流动性。"
    ),
    TestCase(
        id="M06",
        question="Charlie Munger 的'反向思维'(Inversion)如何应用于投资决策？请用一个具体案例说明如何'反过来想'来避免灾难性错误。",
        dimension="master",
        difficulty="hard",
        expected_elements=[
            "反向", "避免愚蠢", "失败", "清单", "认知偏差",
            "能力圈", "Munger",
        ],
        rubric={
            "正确解释Inversion方法论": 3,
            "给出具体的投资案例": 2,
            "连接到投资清单的实际应用": 2,
            "引用Munger的经典论述": 2,
            "讨论如何制度化反向思维": 1,
        },
        reference_answer_notes="Munger: '告诉我我会死在哪里，这样我就不去那儿。' 先问'这笔投资怎么会亏光?'"
    ),
    TestCase(
        id="M07",
        question="对比分析：如果2026年你只能买入一只股票持有10年不动，在 V（Visa）、UNH（联合健康）和 BRK-B 中选一只。请给出你的选择和完整论证。",
        dimension="master",
        difficulty="expert",
        expected_elements=[
            "护城河", "持久性", "ROIC", "增长", "管理层",
            "估值", "风险", "能力圈",
        ],
        expected_tool_calls=["evaluate_stock_rules"],
        rubric={
            "三只股票都做了深入分析": 2,
            "评估了10年护城河持久性": 3,
            "给出了明确且有说服力的选择": 2,
            "量化了预期回报率": 2,
            "讨论了最大的下行风险": 1,
        },
    ),
]


# ═══════════════════════════════════════════════════════════════
#  All Tests Combined
# ═══════════════════════════════════════════════════════════════

ALL_TESTS: List[TestCase] = (
    CONCEPTUAL_TESTS +
    STOCK_ANALYSIS_TESTS +
    COMPARATIVE_TESTS +
    RISK_TESTS +
    EDGE_CASE_TESTS +
    BEHAVIORAL_TESTS +
    PORTFOLIO_TESTS +
    MASTER_TESTS
)

# Index by dimension
TESTS_BY_DIMENSION: Dict[str, List[TestCase]] = {}
for t in ALL_TESTS:
    TESTS_BY_DIMENSION.setdefault(t.dimension, []).append(t)

# Index by difficulty
TESTS_BY_DIFFICULTY: Dict[str, List[TestCase]] = {}
for t in ALL_TESTS:
    TESTS_BY_DIFFICULTY.setdefault(t.difficulty, []).append(t)


def get_test(test_id: str) -> TestCase | None:
    """Get a test case by ID."""
    for t in ALL_TESTS:
        if t.id == test_id:
            return t
    return None


def get_quick_eval_suite() -> List[TestCase]:
    """Get a quick evaluation suite (one from each dimension).

    Picks an easy/medium test from each of the 8 dimensions.
    For master dimension (all expert), picks the first one.
    """
    quick = []
    seen_dims = set()
    # First pass: easy/medium
    for t in ALL_TESTS:
        if t.dimension not in seen_dims and t.difficulty in ("easy", "medium"):
            quick.append(t)
            seen_dims.add(t.dimension)
    # Second pass: for dimensions without easy/medium (like master), pick first
    for t in ALL_TESTS:
        if t.dimension not in seen_dims:
            quick.append(t)
            seen_dims.add(t.dimension)
    return quick


def get_stats() -> Dict[str, Any]:
    """Get statistics about the test suite."""
    return {
        "total_tests": len(ALL_TESTS),
        "by_dimension": {k: len(v) for k, v in TESTS_BY_DIMENSION.items()},
        "by_difficulty": {k: len(v) for k, v in TESTS_BY_DIFFICULTY.items()},
        "dimensions": list(TESTS_BY_DIMENSION.keys()),
    }
