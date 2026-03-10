# 全量阈值参考手册 — R System Investment Thresholds

> 系统中 **100+ 个量化阈值**的完整定义、计算公式、触发逻辑。
> 所有阈值均可通过 `investment_params.yaml` 或 API `POST /agent/params` 运行时调整。

---

## 目录

1. [Risk Manager 否决阈值（一票否决权）](#1-risk-manager-否决阈值一票否决权)
2. [七大流派筛选规则](#2-七大流派筛选规则)
3. [流派评判系统](#3-流派评判系统)
4. [投委会辩论裁决](#4-投委会辩论裁决)
5. [Forensics 财务排雷](#5-forensics-财务排雷)
6. [评分系统 (Conviction Scoring)](#6-评分系统-conviction-scoring)
7. [估值模型参数](#7-估值模型参数)
8. [护城河检测](#8-护城河检测)
9. [择时信号](#9-择时信号)
10. [仓位管理](#10-仓位管理)
11. [回测判定](#11-回测判定)
12. [策略模板](#12-策略模板)
13. [数据质量](#13-数据质量)
14. [指标计算公式汇总](#14-指标计算公式汇总)

---

## 1. Risk Manager 否决阈值（一票否决权）

Risk Manager 拥有**一票否决权**，以下为硬性规则，任一触发即否决（stance 强制降为 AVOID）：

| 参数 Key | 指标 | 默认值 | 触发条件 | 含义 | 公式 |
|----------|------|--------|----------|------|------|
| `risk.z_score_danger` | Altman Z-Score | **1.81** | Z < 1.81 | 破产危险区 | Z = 1.2×(WC/TA) + 1.4×(RE/TA) + 3.3×(EBIT/TA) + 0.6×(MV/TL) + 1.0×(Sales/TA) |
| `risk.m_score_danger` | Beneish M-Score | **-1.78** | M > -1.78 | 盈利操纵嫌疑 | M = -4.84 + 0.92×DSRI + 0.528×GMI + 0.404×AQI + 0.892×SGI + 0.115×DEPI - 0.172×SGAI + 4.679×TATA - 0.327×LVGI |
| `risk.f_score_danger` | Piotroski F-Score | **3** | F ≤ 3 | 财务极度虚弱 | 0-9 分（9 项二元检验，每项 0 或 1） |

### Z-Score 分区解读
| 区间 | 含义 |
|------|------|
| Z > 2.99 | 安全区（Safe Zone） |
| 1.81 < Z < 2.99 | 灰色区（Grey Zone） |
| Z < 1.81 | **危险区 → 触发否决** |

### F-Score 9 项检验
| # | 检验项 | 条件 → 得 1 分 |
|---|--------|---------------|
| 1 | ROA 正 | Net Income / Total Assets > 0 |
| 2 | 经营现金流正 | CFO > 0 |
| 3 | ROA 改善 | 本年 ROA > 上年 ROA |
| 4 | 盈利质量 | CFO > Net Income |
| 5 | 杠杆下降 | 本年长期负债率 < 上年 |
| 6 | 流动性改善 | 本年流动比率 > 上年 |
| 7 | 无稀释 | 本年股数 ≤ 上年股数 |
| 8 | 毛利率改善 | 本年毛利率 > 上年 |
| 9 | 资产周转率改善 | 本年 Sales/Assets > 上年 |

### M-Score 8 因子
| 因子 | 名称 | 含义 |
|------|------|------|
| DSRI | 应收账款指数 | 应收增速 > 营收增速 → 虚增收入 |
| GMI | 毛利率指数 | 毛利率恶化 → 盈利压力 |
| AQI | 资产质量指数 | 非流动资产占比上升 → 资本化费用 |
| SGI | 营收增长指数 | 高增长公司更易操纵 |
| DEPI | 折旧指数 | 折旧率下降 → 延长资产寿命 |
| SGAI | SGA 费用指数 | 管理费率变化 |
| TATA | 应计比率 | (NI - CFO) / TA，越高越可疑 |
| LVGI | 杠杆指数 | 杠杆率变化 |

---

## 2. 七大流派筛选规则

### 2.1 Graham 深度价值（10 条规则）

> **代表人物**: Benjamin Graham, David Dodd
> **核心理念**: "安全边际是投资的基石"
> **最低通过率**: 50%

| # | 规则名 | 条件 | 默认值 | 淘汰性 | 计算公式/说明 |
|---|--------|------|--------|--------|--------------|
| 1 | PE 上限 | PE < N | **15** | ⚡ 是 | PE = Price / EPS(TTM) |
| 2 | PE×PB 复合 | PE × PB < N | **22.5** | 否 | Graham Number² = 22.5 × EPS × BVPS |
| 3 | 安全边际 | MoS ≥ N | **33%** | 否 | MoS = (Intrinsic Value - Price) / Intrinsic Value |
| 4 | 流动比率 | CR ≥ N | **2.0** | 否 | CR = Current Assets / Current Liabilities |
| 5 | 负债权益比 | D/E < N | **1.0** | ⚡ 是 | D/E = Total Debt / Total Equity |
| 6 | 连续盈利 | ≥ N 年 | **10** | 否 | 过去 N 年 EPS 均 > 0 |
| 7 | 连续分红 | ≥ N 年 | **20** | 否 | 连续 N 年支付股息 |
| 8 | 盈利增长 | 近 3 年 EPS > 近 10 年 EPS | — | 否 | avg_eps_3y > avg_eps_10y |
| 9 | 市值门槛 | 市值 ≥ N | **$10 亿** | 否 | Market Cap = Price × Shares Outstanding |
| 10 | NCAV 净流动资产 | Price < NCAV × N | **0.67** | 否 | NCAV = (Current Assets - Total Liabilities) / Shares |

### 2.2 Buffett 护城河投资（9 条规则）

> **代表人物**: Warren Buffett, Charlie Munger
> **核心理念**: "以合理价格买入伟大企业"
> **最低通过率**: 60%

| # | 规则名 | 条件 | 默认值 | 淘汰性 | 说明 |
|---|--------|------|--------|--------|------|
| 1 | ROE 护城河 | ROE > N | **15%** | ⚡ 是 | ROE = Net Income / Shareholder Equity |
| 2 | 净利润率 | NPM > N | **10%** | 否 | NPM = Net Income / Revenue |
| 3 | 低负债 | D/E < N | **0.5** | 否 | 比 Graham 更严格 |
| 4 | FCF 为正 | FCF > 0 | — | ⚡ 是 | FCF = Operating CF - CapEx |
| 5 | 盈利一致性 | ≥ N 年 | **10** | 否 | 连续盈利年数 |
| 6 | 安全边际 | MoS ≥ N | **25%** | 否 | 比 Graham 的 33% 稍低 |
| 7 | 营业利润率 | OPM > N | **15%** | 否 | OPM = Operating Income / Revenue |
| 8 | 盈利增长 | 10 年 CAGR > N | **3%** | 否 | CAGR = (EPS_now/EPS_10y)^(1/10) - 1 |
| 9 | 内在价值折扣 | Price < IV | — | 否 | IV 基于 DCF/EPV/Graham Number |

### 2.3 量化价值（9 条规则）

> **代表人物**: Joel Greenblatt, James O'Shaughnessy, Wesley Gray
> **核心理念**: "系统化投资消除认知偏差"
> **最低通过率**: 50%

| # | 规则名 | 条件 | 默认值 | 淘汰性 | 说明 |
|---|--------|------|--------|--------|------|
| 1 | 盈利收益率 | EY > N | **8%** | ⚡ 是 | EY = EBIT / Enterprise Value |
| 2 | 避免魅力股 | PE < 市场 PE | — | 否 | 低于 S&P500 平均 PE |
| 3 | 高质量盈利 | FCF > NI | — | 否 | 自由现金流 > 净利润 → 低应计 |
| 4 | ROE 质量门槛 | ROE > N | **15%** | 否 | |
| 5 | 利息覆盖 | ICR > N | **3x** | 否 | ICR = EBIT / Interest Expense |
| 6 | 市值门槛 | 市值 ≥ N | **$1 亿** | 否 | 排除微盘股 |
| 7 | 低 P/S | P/S < N | **1.5** | 否 | PS = Price / Revenue per Share |
| 8 | 营收正增长 | Revenue Growth > 0 | — | 否 | |
| 9 | 魔法公式 | EY > 8% 且 ROE > N | **20%** | 否 | Greenblatt 双因子排名 |

### 2.4 品质投资（8 条规则）

> **代表人物**: Pat Dorsey (Morningstar), Lawrence Cunningham
> **核心理念**: "时间是优质企业的朋友"
> **最低通过率**: 60%

| # | 规则名 | 条件 | 默认值 | 淘汰性 | 说明 |
|---|--------|------|--------|--------|------|
| 1 | ROE 持续性 | ROE > N 且盈利 ≥ M 年 | **15%, 8 年** | ⚡ 是 | 长期高 ROE 是核心标准 |
| 2 | 营业利润率 | OPM > N | **15%** | 否 | 宽护城河信号 |
| 3 | FCF 为正 | FCF > 0 | — | ⚡ 是 | |
| 4 | 低财务杠杆 | D/E < N | **0.5** | 否 | |
| 5 | 营收增长 | Revenue Growth > N | **5%** | 否 | 可持续增长 |
| 6 | 盈利稳定性 | 最大 EPS 下降 > N | **-30%** | 否 | 波动小 |
| 7 | EPS 增长 | EPS Growth > N | **5%** | 否 | |
| 8 | 合理估值 | PE < N | **25** | 否 | 不为优质付过高代价 |

### 2.5 Damodaran 估值派（6 条规则）

> **代表人物**: Aswath Damodaran, Alfred Rappaport
> **核心理念**: "估值既是科学也是艺术"
> **最低通过率**: 50%

| # | 规则名 | 条件 | 默认值 | 说明 |
|---|--------|------|--------|------|
| 1 | Forward PE 折扣 | Forward PE < Trailing PE | — | 盈利增长预期 |
| 2 | 盈利收益率 > 无风险利率 | EY > 10Y Treasury | — | 风险溢价为正 |
| 3 | PEG 合理 | PEG < N | **1.5** | PEG = PE / (EPS Growth% × 100) |
| 4 | 营业利润率 | OPM > N | **10%** | |
| 5 | FCF 为正 | FCF > 0 | — | DCF 的基础 |
| 6 | 营收正增长 | Revenue Growth > 0 | — | |

### 2.6 逆向价值（5 条规则）

> **代表人物**: Howard Marks, Seth Klarman, Guy Spier, John Templeton
> **核心理念**: "别人恐惧时我贪婪"
> **最低通过率**: 60%

| # | 规则名 | 条件 | 默认值 | 说明 |
|---|--------|------|--------|------|
| 1 | 52 周低位 | Price < 52W High × N | **0.70** | 跌 30%+ |
| 2 | 深度低 P/S | P/S < N | **0.75** | 极低市销率 |
| 3 | 极低 PE | PE < N | **10** | 市场过度悲观 |
| 4 | 高股息 | Div Yield > N | **4%** | 高股息=被抛弃信号 |
| 5 | 基本面健康 | ROE > N 且 FCF > 0 | **8%** | 确保不是价值陷阱 |

### 2.7 GARP 合理价格成长（6 条规则）

> **代表人物**: Peter Lynch, Jeremy Siegel
> **核心理念**: "PEG < 1 = 折扣价购买增长"
> **最低通过率**: 50%

| # | 规则名 | 条件 | 默认值 | 说明 |
|---|--------|------|--------|------|
| 1 | EPS 增长 | EPS Growth > N | **10%** | 显著盈利增长 |
| 2 | PE 合理 | PE < N | **20** | |
| 3 | PEG < 1 | PEG < N | **1.0** | Lynch 核心指标 |
| 4 | ROE | ROE > N | **15%** | 高资本效率 |
| 5 | 营收增长 | Revenue Growth > N | **8%** | 顶线驱动 |
| 6 | 低负债 | D/E < N | **0.8** | |

---

## 3. 流派评判系统

每只股票对每个流派跑完规则后，按以下逻辑判定：

| 判定 | 条件 | 含义 |
|------|------|------|
| **REJECT** | 任一 `is_eliminatory=True` 规则失败 | 不合格（淘汰性指标未通过） |
| **STRONG_PASS** | pass_rate ≥ min_pass_rate 且 score ≥ 60% max_score | 优秀 |
| **PASS** | pass_rate ≥ 80% × min_pass_rate | 合格 |
| **MARGINAL** | pass_rate ≥ 30% | 边缘 |
| **FAIL** | pass_rate < 30% | 不合格 |

**流派权重**（用于综合打分）：

| 流派 | 权重 | 参数 Key |
|------|------|----------|
| Buffett 护城河 | **2.0** | `school_weight.buffett` |
| 品质投资 | **2.0** | `school_weight.quality` |
| Graham 深度价值 | **1.5** | `school_weight.graham` |
| 量化价值 | **1.5** | `school_weight.quantitative` |
| Damodaran 估值 | **1.5** | `school_weight.valuation` |
| GARP 成长 | **1.0** | `school_weight.garp` |
| 逆向价值 | **0.5** | `school_weight.contrarian` |

---

## 4. 投委会辩论裁决

### 投票权重

| Stance | 权重 |
|--------|------|
| STRONG_BUY | 1.0 |
| BUY | 0.7 |
| HOLD | 0.4 |
| AVOID | 0.15 |
| REJECT | 0.0 |

### PM Fallback 裁决（LLM 失败时）

| 条件 | 裁决 |
|------|------|
| 加权均分 ≥ 0.70 | STRONG_BUY |
| 加权均分 ≥ 0.55 | BUY |
| 加权均分 ≥ 0.35 | HOLD |
| 加权均分 < 0.35 | AVOID |

| 参数 Key | 默认值 |
|----------|--------|
| `committee.strong_buy_threshold` | 0.70 |
| `committee.buy_threshold` | 0.55 |
| `committee.hold_threshold` | 0.35 |

### 否决覆盖
如果 Risk Manager 否决已触发，PM 即使裁决 BUY/STRONG_BUY，也会被强制降为 AVOID。

---

## 5. Forensics 财务排雷

Stage 4 的财务安全检查：

| 参数 Key | 默认值 | 触发条件 | 结果 |
|----------|--------|----------|------|
| `forensics.f_score_red_flag` | 3 | F-Score ≤ 3 | 红旗 |
| `forensics.z_score_danger` | 1.81 | Z-Score < 1.81 | 红旗 |
| `forensics.z_score_grey` | 2.99 | Z-Score < 2.99 | 警告 |
| `forensics.m_score_danger` | -1.78 | M-Score > -1.78 | 红旗 |
| `forensics.high_debt_flag` | 2.0 | D/E > 2.0 | 红旗 |
| `forensics.high_pe_flag` | 50 | PE > 50 | 红旗 |
| `forensics.high_red_flags_eliminate` | 3 | HIGH 级红旗 ≥ 3 | **淘汰** |

### 风险分级

| 参数 Key | 默认值 | 含义 |
|----------|--------|------|
| `risk_tier.fortress_f_score_min` | 7 | F-Score ≥ 7 → FORTRESS 级 |
| `risk_tier.solid_f_score_min` | 5 | F-Score ≥ 5 → SOLID 级 |

---

## 6. 评分系统 (Conviction Scoring)

Stage 8 最终信念评分（满分 100+）：

| 维度 | 满分 | 参数 Key |
|------|------|----------|
| 估值 | **30** | `scoring.valuation_max_points` |
| 流派共识 | **25** | `scoring.school_consensus_max_points` |
| 财务安全 | **20** | `scoring.financial_safety_max_points` |
| 投委会辩论 | **15** | `scoring.committee_max_points` |
| 护城河 | **10** | `scoring.moat_max_points` |
| LLM 分析 | **10** | `scoring.llm_max_points` |
| **否决罚分** | **-20** | `scoring.veto_penalty` |

### 安全边际评分阶梯

| 安全边际 | 得分 | 参数 Key |
|----------|------|----------|
| MoS ≥ 33% | 满分 | `scoring.mos_band_excellent` |
| MoS ≥ 20% | 良好 | `scoring.mos_band_good` |
| MoS ≥ 10% | 合格 | `scoring.mos_band_fair` |
| MoS < 10% | 低分 | — |

### 最终裁决阈值

| 裁决 | 最低分 | 参数 Key |
|------|--------|----------|
| STRONG_BUY | **75** | `verdict.strong_buy_score` |
| BUY | **55** | `verdict.buy_score` |
| HOLD | **35** | `verdict.hold_score` |
| AVOID | **20** | `verdict.avoid_score` |
| REJECT | < 20 | — |

### 信念等级

| 等级 | 条件 | 参数 Key |
|------|------|----------|
| HIGHEST | ≥ 3 流派 STRONG_PASS 且 MoS ≥ 30% | `conviction.highest_*` |
| HIGH | ≥ 2 流派 STRONG_PASS 且 MoS ≥ 15% | `conviction.high_*` |
| MEDIUM | 其他合格 | — |
| LOW | 勉强通过 | — |
| NONE | 不建议 | — |

---

## 7. 估值模型参数

| 参数 Key | 默认值 | 说明 | 公式 |
|----------|--------|------|------|
| `valuation_model.graham_constant` | 22.5 | Graham Number 常数 | GN = √(22.5 × EPS × BVPS) |
| `valuation_model.graham_iv_base_pe` | 8.5 | Graham 内在价值无增长 PE | IV = EPS × (8.5 + 2g) × 4.4/Y |
| `valuation_model.epv_wacc` | 10% | EPV 折现率 | EPV = EBIT(1-t) / WACC |
| `valuation_model.epv_tax_rate` | 21% | EPV 税率 | |
| `valuation_model.dcf_wacc` | 10% | DCF 折现率 | |
| `valuation_model.dcf_terminal_growth` | 3% | DCF 永续增长率 | Terminal = FCF×(1+g) / (WACC-g) |
| `valuation_model.dcf_forecast_years` | 5 年 | DCF 预测期 | |
| `valuation_model.dcf_max_growth` | 20% | 增长率上限 | 防止 DCF 膨胀 |
| `valuation_model.ddm_required_return` | 10% | DDM 必要回报率 | V = D₁ / (r - g) |
| `valuation_model.owner_earnings_cap_rate` | 10% | 所有者盈余资本化率 | OE = NI + D&A - CapEx(维护) |

---

## 8. 护城河检测

| 参数 Key | 默认值 | 说明 |
|----------|--------|------|
| `moat.wide_roe_min` | 15% | Wide Moat: ROE ≥ 15% |
| `moat.wide_margin_min` | 10% | Wide Moat: 利润率 ≥ 10% |
| `moat.narrow_roe_min` | 10% | Narrow Moat: ROE ≥ 10% |

**五大护城河来源** (Dorsey)：无形资产(品牌/专利) | 转换成本 | 网络效应 | 成本优势 | 有效规模

---

## 9. 择时信号

| 参数 Key | 默认值 | 说明 |
|----------|--------|------|
| `timing.rsi_oversold` | 30 | RSI < 30 → 超卖 |
| `timing.rsi_overbought` | 70 | RSI > 70 → 超买 |
| `timing.buy_now_threshold` | 65 | 时机分 ≥ 65 → BUY_NOW |
| `timing.caution_threshold` | 40 | 时机分 < 40 → CAUTION |

**RSI 公式**: RSI = 100 - 100/(1 + RS), RS = 14 日平均涨幅 / 14 日平均跌幅

---

## 10. 仓位管理

### 信念仓位

| 信念等级 | 仓位 % | 参数 Key |
|----------|--------|----------|
| HIGHEST | **12%** | `position.highest_pct` |
| HIGH | **8%** | `position.high_pct` |
| MEDIUM | **5%** | `position.medium_pct` |
| LOW | **3%** | `position.low_pct` |
| NONE | **2%** | `position.none_pct` |

### 时机调整

| 时机 | 乘数 | 参数 Key |
|------|------|----------|
| BUY_NOW | **×1.2** | `position.buy_now_multiplier` |
| CAUTION | **×0.7** | `position.caution_multiplier` |

### 约束

| 参数 Key | 默认值 |
|----------|--------|
| `position.max_single_pct` | 15% — 单只最大仓位 |

### 止损比例

| 信念等级 | 止损 % | 说明 |
|----------|--------|------|
| HIGHEST | 25% | 高信念容忍更大波动 |
| HIGH | 20% | |
| MEDIUM | 15% | |
| LOW | 12% | |
| NONE | 10% | 低信念紧止损 |

### 买入价格区间

| 参数 Key | 默认值 | 说明 |
|----------|--------|------|
| `buy_price.low_multiplier` | 0.67 | 理想买入下限 = 内在价值 × 0.67 |
| `buy_price.high_multiplier` | 0.85 | 理想买入上限 = 内在价值 × 0.85 |

---

## 11. 回测判定

| 参数 Key | 默认值 | 说明 |
|----------|--------|------|
| `backtest.holding_months` | 6 | 持仓周期 |
| `backtest.lookback_years` | 3.0 | 回溯年数 |
| `backtest.commission_rate` | 0.1% | 佣金 |
| `backtest.slippage_rate` | 0.05% | 滑点 |
| `backtest.stop_loss_pct` | 15% | 止损 |
| `backtest.max_holdings` | 15 | 最大持仓数 |
| `backtest.initial_capital` | $1,000,000 | 初始资金 |
| `backtest.benchmark` | SPY | 基准 |

### 回测结果判定

| 判定 | 胜率条件 | Alpha 条件 |
|------|----------|------------|
| **VALIDATED** | ≥ 60% | ≥ 2% |
| **MIXED** | ≥ 40% | ≥ -2% |
| **FAILED** | < 40% | < -2% |

---

## 12. 策略模板

| 策略 | 最低市值 | PE 上限 | D/E 上限 | 流动比率 | 安全边际 | 持仓数 |
|------|----------|---------|----------|----------|----------|--------|
| **保守型** | $20 亿 | 15 | 1.0 | 2.0 | 33% | 20 |
| **均衡型** | $5 亿 | 25 | 2.0 | 1.0 | 15% | 15 |
| **进取型** | $1 亿 | 40 | 3.0 | 0.5 | 0% | 30 |

---

## 13. 数据质量

| 参数 Key | 默认值 | 说明 |
|----------|--------|------|
| `data.core_coverage_min` | 40% | 核心字段（price/pe/roe等12项）最低覆盖率 |
| `data.good_enough_coverage` | 60% | 达到此覆盖率即停止切换数据源 |

---

## 14. 指标计算公式汇总

| 指标 | 公式 | 字段名 |
|------|------|--------|
| **PE (Trailing)** | Price / EPS(TTM) | `pe` |
| **Forward PE** | Price / EPS(Forward) | `forward_pe` |
| **PB** | Price / Book Value per Share | `pb` |
| **PS** | Price / Revenue per Share | `ps` |
| **PEG** | PE / (EPS Growth% × 100) | 计算型 |
| **ROE** | Net Income / Shareholder Equity | `roe` |
| **Earnings Yield** | EBIT / Enterprise Value | `earnings_yield` |
| **D/E** | Total Debt / Total Equity | `debt_to_equity` |
| **Current Ratio** | Current Assets / Current Liabilities | `current_ratio` |
| **Interest Coverage** | EBIT / Interest Expense | `interest_coverage_ratio` |
| **FCF** | Operating Cash Flow - CapEx | `free_cash_flow` |
| **Margin of Safety** | (Intrinsic Value - Price) / Intrinsic Value | `margin_of_safety` |
| **Graham Number** | √(22.5 × EPS × BVPS) | `graham_number` |
| **NCAV/Share** | (Current Assets - Total Liabilities) / Shares | `ncav_per_share` |
| **Owner Earnings** | NI + D&A - Maintenance CapEx | 计算型 |
| **EPV** | EBIT × (1-t) / WACC | 计算型 |
| **RSI(14)** | 100 - 100/(1 + avg_gain_14d/avg_loss_14d) | `rsi_14d` |

---

## 如何调整阈值

### 方法 1：YAML 文件（推荐）
编辑 `investment_params.yaml`：
```yaml
risk:
  m_score_danger: -2.22    # 放宽 M-Score 阈值
graham:
  pe_max: 18               # 放宽 Graham PE 上限
```
然后调用 `POST /agent/params/reload` 热加载。

### 方法 2：API 运行时调整
```bash
POST /agent/params
{
  "overrides": {"risk.m_score_danger": -2.22},
  "reason": "学术界更常用的 Beneish 阈值"
}
```

### 方法 3：查看当前值
```bash
GET /agent/params              # 全量参数
GET /agent/params/overridden   # 仅查看已覆盖项
```
