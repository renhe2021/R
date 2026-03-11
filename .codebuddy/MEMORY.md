# 老查理 (Old Charlie) — 项目长期记忆

> 最后更新: 2026-03-11

## 项目简介

"老查理"是一个深度价值投资分析平台，包含 8 阶段选股管线 (Pipeline)、Multi-Agent 投委会辩论、回测验证、阈值优化。

## 技术栈

- **后端**: FastAPI (Python), SQLAlchemy, SSE 流式推送
- **前端**: 原生 HTML/CSS/JS (`web/index.html`, `web/assets/app.js`)
- **数据源**: Bloomberg Terminal (优先) / yfinance (fallback) / FMP / Finnhub
- **LLM**: 统一走 `app/agent/llm.py` 的 `simple_completion`

## 项目结构

```
backend/app/
├── agent/
│   ├── backtest/           # 回测引擎
│   │   ├── historical_data.py      # 历史数据获取 + F/Z/M Score
│   │   ├── historical_screener.py  # 历史选股器 (8阶段)
│   │   ├── pit_backtester.py       # Point-in-Time 回测器
│   │   ├── threshold_optimizer.py  # 阈值优化 (Grid/Random/Stepwise)
│   │   ├── walk_forward.py         # Walk-Forward K折验证
│   │   ├── trade_simulator.py      # 交易模拟器
│   │   ├── metrics.py              # 指标计算 (Sharpe, Alpha等)
│   │   └── models.py               # 数据模型
│   ├── investment_params.py  # 投资参数注册表 (可热修改)
│   ├── unified_pipeline.py   # 8阶段统一管线
│   ├── service.py             # AgentService
│   └── llm.py                 # LLM 调用封装
│   
├── api/
│   └── agent_routes.py     # 所有 API 端点
│
src/data_providers/
├── factory.py              # 数据源工厂 (probe_bloomberg_first)
├── raw_source.py           # RawDataSource 抽象层 (Bloomberg/yfinance)
├── bloomberg_provider.py   # Bloomberg Terminal 封装
└── __init__.py

web/
├── index.html              # 单页应用
└── assets/app.js           # 前端逻辑
```

## 关键架构决策

1. **数据源**: `RawDataSource` 抽象层统一 Bloomberg/yfinance，回测和主管线共用同一数据源
2. **Multi-Agent**: 不用框架，Python 类 + 角色 prompt 实现，5+1 模式 (Value/Growth/Quant/Contrarian/Risk + PM)
3. **参数系统**: `InvestmentParamsRegistry` 支持 YAML 热加载、运行时 override、审计日志
4. **前后端通信**: SSE 事件流，事件名格式 `{module}_{action}` (如 `optimize_progress`)

## 当前状态 (2026-03-11)

### 已完成
- ✅ 8阶段选股管线 (Stage 1-8)
- ✅ Point-in-Time 回测
- ✅ 阈值优化 (Grid / Random / **Stepwise**)
- ✅ Walk-Forward K折验证
- ✅ Bloomberg 数据集成 (BDH quarterly + BDP)
- ✅ RawDataSource 抽象层 (Bloomberg 优先, yfinance fallback)
- ✅ 前端完整 UI (分析面板 + 回测 + 优化 + Walk-Forward)

### 已修复的重要 Bug
- **Bloomberg BDH 日期分裂**: 同一季度的 EQY_SH_OUT 和 BS 字段返回在不同日期 → ±7天合并
- **Bloomberg BDH 单位**: 所有 BDH 财务字段是百万单位 → ×1,000,000 (除 per-share 字段)
- **SSE 事件格式不匹配**: 后端 `optimize_trial` → 前端只识别 `optimize_progress`；`optimize_complete` 数据结构需嵌套对象

### Stepwise 搜索 (最新功能)
- 分组坐标下降: 12 个参数按功能分 4 组，逐组 mini grid search
- `max_trials` = 每组预算上限 (默认 200)，不是全局上限
- 最多 3 轮外循环，收敛 (ΔSharpe < 0.01) 提前退出
- 每组用组内局部最优锁定参数值

## 数据源注意事项

- **Bloomberg 机器**: 有 Bloomberg Terminal 的机器会自动优先用 Bloomberg
- **非 Bloomberg 机器**: 自动 fallback 到 yfinance，功能完全一致
- `_BBG_NO_SCALE_FIELDS = {"IS_EPS", "PX_LAST", "EQY_DVD_YLD_IND", "DVD_SH_LAST"}` — 这些 per-share/ratio 字段不做 ×1M 缩放

## 用户偏好

- Perplexity API Key: 已配置 (存于 agent memory，不写入 git)，OpenAI 兼容格式, base_url: https://api.perplexity.ai
- 偏好中文交流
- 重视可解释性和可追溯性 (辩论记录持久化)
