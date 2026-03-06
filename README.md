# 投资选股分析平台

基于投资书籍知识库的股票分析系统，支持多数据源交叉验证。

## 快速启动

```bash
# 1. 克隆仓库
git clone https://github.com/renhe2021/R.git
cd R

# 2. 创建虚拟环境（推荐）
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
# source venv/bin/activate

# 3. 安装依赖
pip install -r requirements.txt

# 4. 配置
cp config.example.yaml config.yaml
# 编辑 config.yaml，填入你的 LLM API Key

# 5. 启动 Web 服务
python -m src.web_app
# 浏览器打开 http://localhost:5001
```

## 功能

- **多数据源股票分析**: yfinance + Yahoo Direct HTTP 双源并行获取，交叉验证 22 项指标
- **知识库规则评估**: 从投资书籍中提取的选股规则自动评估
- **智能数据合并**: 一致取中位数、有差异取主源、冲突取最佳覆盖源
- **书籍知识提取**: 支持 PDF / EPUB / DOCX 格式，LLM 驱动的规则提取
- **语义搜索**: ChromaDB 向量数据库支持的知识库搜索

## 项目结构

```
src/
├── web_app.py              # Flask Web 服务（主入口）
├── analyzer.py             # 股票数据模型 + 规则评估引擎
├── config.py               # 配置管理
├── data_providers/         # 数据源
│   ├── yfinance_provider.py    # yfinance 包
│   ├── yahoo_direct_provider.py # Yahoo Finance HTTP API（独立第二源）
│   ├── fmp_provider.py         # Financial Modeling Prep
│   ├── finnhub_provider.py     # Finnhub
│   └── bloomberg.py            # Bloomberg Terminal
├── extractors/             # 书籍知识提取
├── parsers/                # 文件解析（PDF/EPUB/DOCX）
├── llm/                    # LLM 提供者（Claude/DeepSeek/Zhipu）
├── exporters/              # 导出（Markdown/向量库）
└── search.py               # 语义搜索
web/                        # 前端（HTML/CSS/JS）
data/
└── knowledge/              # 提取的投资规则知识库
```

## 配置说明

复制 `config.example.yaml` 为 `config.yaml`，主要配置项：

- **LLM**: 用于书籍知识提取，支持 Claude / DeepSeek / Zhipu
- **数据源**: yfinance 和 yahoo_direct 免费可用，FMP 和 Finnhub 需申请免费 API Key
- **存储路径**: 知识库、向量库、输出目录

## CLI 命令

```bash
# 提取书籍知识
python -m src.cli extract <书籍文件路径>

# 搜索知识库
python -m src.cli search "价值投资"

# 分析股票
python -m src.cli analyze AAPL
```
