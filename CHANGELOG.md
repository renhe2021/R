# Changelog

所有重大改动记录。格式遵循 [Keep a Changelog](https://keepachangelog.com/)。

---

## [0.4.0] — 2026-03-09 (957c3a0)

### 投委会辩论可视化 + Bug 修复

#### 新增 (Added)
- **前端「投委会辩论」Tab** — 在股票详情面板新增完整的 AI 辩论可视化
  - 最终裁决横幅：PM 判决、置信度、共识度、否决状态
  - 投票分布条：12 Agent 投票的彩色堆叠条形图（STRONG_BUY / BUY / HOLD / AVOID / REJECT）
  - 否决警报：Risk Manager 一票否决时红色脉冲动画 + 否决原因 + 量化触发条件
  - 7 流派意见卡片：立场标签、置信度进度条、核心理由（3条）、风险警告（2条）
  - 5 角色分析卡片：Research/Quant/Risk/Macro/Sector 各自独立分析
  - 关键分歧点列表
  - Round 2 辩论纪要（PM 质询叙述）
  - PM 最终判断 + 一句话投资者总结
- **管线完成状态栏** — 显示数据源名称（Bloomberg/yfinance/FMP/Finnhub）和投委会辩论股票数量
- **数据源追踪** — `window._pipelineDataSource` 全程存储当前使用的数据源

#### 修复 (Fixed)
- **DB 表缺失错误** — `backend/app/main.py` startup 中增加缺失表检测与强制创建逻辑，确保 `agent_verdicts` / `backtest_results` 等表在首次启动时自动创建
- **BRK-B ticker 映射** — `src/data_providers/bloomberg.py` 中 `-` 自动转为 `/`（`BRK-B` → `BRK/B US Equity`），解决 Bloomberg Terminal 查不到数据的问题
- **Screener 数据源硬编码** — `backend/app/agent/screener.py` 从硬编码 `YfinanceProvider` 改为使用 `get_data_provider()` 工厂函数，优先使用 Bloomberg

#### 改动文件
| 文件 | 改动说明 |
|------|----------|
| `web/index.html` | +1 行：投委会辩论 Tab 按钮 |
| `web/assets/app.js` | +212 行：`renderCommitteeDebateDetail()` + `renderAgentOpinionCard()` + 数据存储 + 状态栏增强 |
| `web/assets/style.css` | +231 行：投委会辩论全套 CSS（verdict banner, vote bar, agent cards, veto alert, dissent, narrative） |
| `backend/app/main.py` | +12 行：startup 中缺失表检测与强制 CREATE TABLE |
| `backend/app/agent/screener.py` | +13 行：数据源工厂模式替换硬编码 |
| `src/data_providers/bloomberg.py` | +6 行：BRK-B → BRK/B 的 ticker 映射逻辑 |
| `README.md` | 全面重写：12-Agent 架构表、8 阶段管线、数据源表格、版本更新日志 |

---

## [0.3.0] — 2026-03-09 (3f2924b)

### Bloomberg 优先数据源 + 多 Agent 投资委员会 + PIT 回测引擎

#### 新增 (Added)
- **Bloomberg-first 数据源架构** — 用户可控的 fallback 机制（SSE pause-resume），BloombergProvider.is_available() 验证 Terminal 实际连接状态
- **12-Agent 投资委员会辩论引擎** — 7 投资流派 + 5 专业角色，结构化辩论 + 否决权 + PM 裁决
  - 辩论模型：`backend/app/agent/committee/models.py`
  - 辩论引擎：`backend/app/agent/committee/debate.py`
- **PIT 回测引擎** — Point-in-Time 回测，交易模拟器 + 指标计算器
- **投资参数注册中心** — `investment_params.yaml` YAML 覆盖 + 审计追踪
- **前端增强** — 数据源选择弹窗、回测页面、管线 UI 改进
- **新 API 端点** — 数据源探测、回测运行/结果/对比、参数 CRUD

#### 关键架构决策
- 数据源优先级：Bloomberg > yfinance > FMP > Finnhub
- 投委会采用两轮辩论制：Round 1 独立意见 → Round 2 结构化辩论
- Risk Manager 拥有一票否决权，可基于量化触发条件（如 debt_to_equity > 阈值）否决

---

## [0.2.0] — 2026-03-09 (e759ccd)

### Agent SOP 管线 — 后端 + 前端

#### 新增 (Added)
- **8 阶段统一管线** (`backend/app/agent/unified_pipeline.py`) — SSE 实时推送各阶段进度
  1. 市场筛选器
  2. 多数据源财务数据拉取
  3. 7 流派独立规则评估
  4. 书籍知识库 RAG 增强
  5. AI 深度分析（LLM 逐股研报）
  6. 择时信号 + 回测验证
  7. 投资委员会辩论
  8. 仓位建议 + 报告汇总
- **7 流派蒸馏规则** (`backend/app/agent/distilled_rules.py`) — 850 行，从 14 本投资书籍中提炼的量化评估规则
- **12 Agent 人设** (`backend/app/agent/persona.py`) — 605 行，每个 Agent 的背景、方法论、决策框架
- **前端暗色主题 UI** — 实时进度条、阶段卡片、股票详情面板

---

## [0.1.1] — 2026-03-06 (debb639)

### 便携式私有仓库配置

#### 改动 (Changed)
- 包含 `config.yaml` 和所有数据文件，确保克隆即可运行

---

## [0.1.0] — 2026-03-06 (195c8fe)

### 初始平台

#### 新增 (Added)
- **多数据源股票分析** — yfinance + Yahoo Direct HTTP 双源并行获取，交叉验证 22 项指标
- **知识库规则评估** — 从 14 本投资书籍中提取的选股规则自动评估
- **智能数据合并** — 一致取中位数、有差异取主源、冲突取最佳覆盖源
- **书籍知识提取** — PDF / EPUB / DOCX 格式，LLM 驱动的规则提取
- **ChromaDB 语义搜索** — 向量数据库支持的知识库搜索
- **LLM 集成** — Claude / DeepSeek / Zhipu 三选一
- **Flask Web UI** — 基础分析界面

---

## 如何使用此文件

在新电脑上 `git pull` 后，查看此文件即可了解：
1. **做了什么** — 每个版本的功能/修复清单
2. **为什么改** — 每条改动都有原因说明
3. **改了哪些文件** — 关键版本附带文件级改动表
4. **架构决策** — 重要的设计选择记录在案

如果你需要定位某个具体功能的代码位置，可以在改动文件表中找到对应的文件路径。
