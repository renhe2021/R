# R · 多流派 AI 投资委员会选股平台

基于 14 本经典投资著作构建的 **12-Agent 投资委员会**系统。7 大投资流派 + 5 个专业角色独立分析、结构化辩论、Portfolio Manager 最终裁决。

## 核心特性

### 🏛️ 12-Agent 投资委员会

| 类型 | Agent | 方法论 |
|------|-------|--------|
| 流派 | Graham 深度价值 | 安全边际、净流动资产 |
| 流派 | Buffett 护城河 | 经济护城河、ROE 持续性 |
| 流派 | 量化价值 | F-Score、Magic Formula |
| 流派 | 品质投资 | ROIC、资本效率 |
| 流派 | Damodaran 估值 | DCF、EV/EBITDA |
| 流派 | 逆向价值 | 均值回归、情绪偏离 |
| 流派 | GARP 成长价值 | PEG、营收加速 |
| 角色 | Research Analyst | 基本面综合 |
| 角色 | Quant Analyst | 量化因子 |
| 角色 | Risk Manager | 否决权（一票否决） |
| 角色 | Macro Strategist | 宏观周期 |
| 角色 | Sector Specialist | 行业对标 |

### 📊 8 阶段统一管线 (Unified Pipeline)

1. **Stage 1** — 市场筛选器（多条件 screener）
2. **Stage 2** — 多数据源财务数据拉取（Bloomberg / yfinance / FMP / Finnhub）
3. **Stage 3** — 7 流派独立规则评估
4. **Stage 4** — 书籍知识库 RAG 增强
5. **Stage 5** — AI 深度分析（LLM 逐股研报）
6. **Stage 6** — 择时信号 + 回测验证
7. **Stage 7** — **投资委员会辩论**
   - 7a: 12 Agent 独立出具意见（stance + confidence + reasons + risks）
   - 7b: 结构化辩论（质询、反驳、分歧聚焦）
   - 7c: 投票 + Risk Manager 否决权
   - 7d: Portfolio Manager 最终裁决
8. **Stage 8** — 仓位建议 + 报告汇总

### 🖥️ 投委会辩论可视化

前端「投委会辩论」Tab 展示完整的 AI 辩论过程：

- **最终裁决横幅** — PM 判决、置信度、共识度、否决状态
- **投票分布条** — 12 Agent 投票的彩色堆叠条形图
- **否决警报** — Risk Manager 一票否决时的红色脉冲动画
- **流派意见卡片** — 7 流派各自的立场、置信度进度条、核心理由、风险警告
- **角色分析卡片** — 5 角色的专业视角分析
- **关键分歧点** — 辩论中最大的争议焦点
- **Round 2 辩论纪要** — PM 模拟投委会质询的完整叙述
- **PM 最终判断** — 裁决理由 + 一句话投资者总结

### 📡 数据源

| 数据源 | 说明 | 费用 |
|--------|------|------|
| Bloomberg Terminal | 专业终端（优先） | 付费 |
| yfinance | Yahoo Finance 开源包 | 免费 |
| FMP | Financial Modeling Prep API | 免费 Key |
| Finnhub | 实时行情 + 基本面 | 免费 Key |

系统自动选择最优数据源，支持 BRK-B 等特殊 ticker 自动映射（`BRK-B` → `BRK/B US Equity`）。

## 快速启动

```bash
# 1. 克隆仓库
git clone https://github.com/renhe2021/R.git
cd R

# 2. 创建虚拟环境
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
# source venv/bin/activate

# 3. 安装依赖
pip install -r requirements.txt

# 4. 配置
cp config.example.yaml config.yaml
# 编辑 config.yaml，填入 LLM API Key + 数据源 Key

# 5. 启动后端服务
cd backend
python -m uvicorn app.main:app --host 0.0.0.0 --port 8000
# 浏览器打开 http://localhost:8000
```

## 项目结构

```
backend/
├── app/
│   ├── main.py                 # FastAPI 主入口
│   ├── agent/
│   │   ├── unified_pipeline.py # 8 阶段统一管线
│   │   ├── persona.py          # 12 Agent 人设定义
│   │   ├── distilled_rules.py  # 7 流派蒸馏规则
│   │   ├── screener.py         # 市场筛选器
│   │   └── committee/
│   │       ├── debate.py       # 投委会辩论引擎
│   │       └── models.py       # 辩论数据模型
│   ├── routes/                 # API 路由（SSE 实时推送）
│   └── db/                     # SQLite 持久化
src/
├── data_providers/             # 数据源适配器
│   ├── bloomberg.py            # Bloomberg Terminal
│   ├── yfinance_provider.py    # yfinance
│   ├── fmp_provider.py         # FMP
│   └── finnhub_provider.py     # Finnhub
├── extractors/                 # 书籍知识提取
├── llm/                        # LLM 提供者（Claude/DeepSeek/Zhipu）
└── search.py                   # ChromaDB 语义搜索
web/
├── index.html                  # 单页应用
├── assets/
│   ├── app.js                  # 前端逻辑（SSE + 渲染）
│   └── style.css               # 暗色主题样式
data/
└── knowledge/                  # 14 本投资书籍蒸馏知识库
```

## 配置说明

复制 `config.example.yaml` 为 `config.yaml`，主要配置项：

- **LLM**: 支持 Claude / DeepSeek / Zhipu（用于深度分析 + 辩论）
- **数据源**: Bloomberg（优先）> yfinance（免费）> FMP / Finnhub（需 Key）
- **投资参数**: `investment_params.yaml` 控制流派权重、筛选阈值、否决条件

## 最近更新

### v0.4 — 投委会辩论可视化 & Bug 修复
- ✅ 新增「投委会辩论」详情 Tab，完整可视化 12 Agent 辩论过程
- ✅ 投票分布条、Agent 意见卡片、否决警报、PM 裁决面板
- ✅ 管线完成状态栏显示数据源名称和辩论股票数量
- ✅ 修复 `agent_verdicts` 等数据库表缺失导致的启动错误
- ✅ 修复 BRK-B ticker 在 Bloomberg 中的映射问题（`-` → `/`）
- ✅ Screener 数据源从硬编码 yfinance 改为工厂模式（优先 Bloomberg）

### v0.3 — Bloomberg 优先 + 多 Agent 委员会 + 回测引擎
- Bloomberg Terminal 数据源集成
- 12-Agent 投资委员会辩论系统
- PIT (Point-in-Time) 回测引擎

### v0.2 — Agent SOP 管线
- 8 阶段统一管线（SSE 实时推送）
- 7 流派蒸馏规则评估
- 前端暗色主题 UI

### v0.1 — 基础框架
- 多数据源股票分析（yfinance + Yahoo Direct）
- 14 本投资书籍知识提取
- ChromaDB 语义搜索
