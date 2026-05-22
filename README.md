# Portfolio Analysis — 多平台持仓穿透分析工具

## 项目背景

个人投资往往分散在多个 App（支付宝、雪球、券商、富途等），每个平台只能看到自己账户内的持仓，无法一眼看清：

- **整体资产到底配了多少股、多少债、多少现金？**
- **跨平台是否重复持仓了同一只股票？**
- **行业集中度如何？是不是全押在制造业？**

本项目的目标就是：**从各平台导出持仓文件，自动解析 → 基金穿透 → 跨平台聚合**，最终产出一份统一的资产配置报告。

## 核心能力

| 能力 | 说明 |
|------|------|
| 多平台解析 | 支持支付宝/且慢/雪球(PDF) + 华泰(Excel) + 富途(PDF) |
| 基金穿透 | 把"一只基金"拆解为底层的股票/债券/现金/商品比例 |
| 币种统一 | HKD/USD 自动换算为 CNY |
| 历史行情缓存 | 基金 NAV、ETF/股票行情、行业配置均缓存到本地 SQLite |
| 跨平台聚合 | 合并所有平台数据，输出按资产类型和股债商现两个维度的汇总 |

## 全流程架构

```
┌─────────────────────────────────────────────────────────────────┐
│  输入：data/ 目录下的持仓文件                                      │
│  alipay_*.pdf | qieman_*.pdf | snowball_*.pdf                   │
│  huatai_*.xlsx | futu_*.pdf                                     │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
              ┌────────────────────────┐
              │  Step 0: 清除缓存        │
              │  删除 cache/ 子目录下     │
              │  的历史 JSON 缓存文件     │
              └────────────┬───────────┘
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step 1-5: 逐平台穿透分析（5 个平台依次执行）                        │
│                                                                 │
│  Parse(解析) → Classify(分类) → Convert(换算) → Penetrate(穿透)   │
│                                                                 │
│  穿透过程中调用 akshare API，数据缓存到 cache/fund_nav.db (SQLite)  │
│  每个平台的穿透结果保存到 cache/{platform}/*.json                   │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
              ┌────────────────────────┐
              │  Step 6: 跨平台聚合      │
              │  合并 5 个平台数据        │
              │  汇总全局资产配置         │
              └────────────┬───────────┘
                           ▼
              ┌────────────────────────────────────────┐
              │  输出：output/aggregated_summary_YYYYMMDD.json  │
              └────────────────────────────────────────┘
```

## 执行步骤详解

### Step 0 — 清除缓存

| 项目 | 内容 |
|------|------|
| 做什么 | 删除 `cache/` 下每个平台子目录中的 JSON 缓存文件 |
| 为什么 | 确保每次运行基于最新的持仓数据，不会混入历史结果 |
| 产出 | 干净的 cache 目录（子目录结构保留，文件清空） |

> 注意：`cache/fund_nav.db`（SQLite 数据库）不会被清除，行情数据持久保留、增量更新。

### Step 1-5 — 逐平台穿透分析

对 5 个平台依次执行以下 4 步流水线：

```
Parse(解析原始文件) → Classify(资产分类) → Convert(币种换算) → Penetrate(基金穿透)
```

#### 1. Parse — 解析持仓文件

| 平台 | 解析器 | 输入格式 | 解析方式 |
|------|--------|----------|----------|
| 支付宝 | `AlipayParser` | 多页 PDF，6 列表格 | pdfplumber 逐页提取表格，处理跨页续表 |
| 且慢 | `QiemanParser` | 多页 PDF，6 列表格 | pdfplumber 提取，过滤汇总行 |
| 雪球 | `SnowballParser` | 多页 PDF，6/9 列混合 | 区分自选基金(6列)和投顾组合(9列) |
| 华泰 | `HuataiParser` | Excel (.xlsx) | pandas 读取，分别解析股票区和基金区 |
| 富途 | `FutuParser` | 每日结单 PDF | 正则匹配文本行，解析港股/美股/基金 |

**产出**：`list[HoldingRecord]` — 统一格式的持仓记录列表

#### 2. Classify — 资产分类

根据代码格式和名称关键词，为每条持仓标注类型：

| 分类维度 | 可能的值 |
|----------|---------|
| `asset_class` (L1) | equity_fund / bond_fund / mixed_fund / money_fund / index_fund / etf / commodity_fund / stock_cn / stock_hk / stock_us / bond |
| `sub_type` (L2) | 更细粒度的二级分类 |

**分类优先级**：先查 akshare 基金数据库 → 再按代码正则匹配 → 最后按名称关键词修正

#### 3. Convert — 币种换算

按 `config.yaml` 中配置的汇率（HKD→0.87, USD→6.84），将所有资产统一换算为 CNY 市值。

#### 4. Penetrate — 基金穿透

这是核心步骤。把每只基金"拆开看"，还原其底层的真实资产配置：

| 子步骤 | 做什么 | 数据来源 |
|--------|--------|----------|
| 5.1 资产配置 | 查询每只基金的股/债/现金/商品比例 | 雪球 API `fund_individual_detail_hold_xq` |
| 5.2 持仓明细 | 获取基金的前 N 大重仓股/债 (deep 模式) | akshare 基金持仓 API |
| 5.3 多维度数据 | 收集行业分类、地区、波动率 (deep 模式) | akshare 行业配置 API |
| 5.4 生成汇总 | 构建 L1(基金类型) + L2(股债商现) 两级汇总 | 计算汇总 |

**产出**：每个平台生成 `cache/{platform}/penetration_summary.json`

### Step 6 — 跨平台聚合

| 项目 | 内容 |
|------|------|
| 做什么 | 扫描所有平台的穿透结果，合并同名股票，汇总全局资产配置 |
| 为什么 | 得到跨所有平台的统一全景视图 |
| 产出 | `output/aggregated_summary_YYYYMMDD.json` |

**输出文件结构**：

```json
{
  "snapshot_time": "YYYY-MM-DDTHH:MM:SS",
  "total_market_value_cny": "xxx",
  "app_breakdown": {
    "futu": "xxx",
    "huatai": "xxx",
    "alipay": "xxx",
    "qieman": "xxx",
    "snowball": "xxx"
  },
  "level1_summary": {
    "bond_fund": {"pct": "xxx"},
    "mixed_fund": {"pct": "xxx"},
    "...": "..."
  },
  "level2_summary": {
    "equity": {"pct": "xxx"},
    "bond": {"pct": "xxx"},
    "cash": {"pct": "xxx"},
    "commodity": {"pct": "xxx"}
  }
}
```

## 本地数据缓存

### SQLite 数据库 (`cache/fund_nav.db`)

行情和行业数据持久化存储，支持增量更新，避免重复调用 API：

| 表名 | 主键 | 存储内容 | 写入策略 |
|------|------|----------|----------|
| `fund_info` | fund_code | 基金基本信息（名称、类型、NAV 时间范围） | INSERT OR IGNORE |
| `fund_daily_nav` | (fund_code, nav_date) | 场外基金每日净值 | INSERT OR IGNORE |
| `etf_daily_hist` | (symbol, trade_date) | 场内 ETF 每日行情 | INSERT OR IGNORE |
| `stock_daily_hist` | (symbol, trade_date) | A股/港股/美股每日行情 | INSERT OR IGNORE |
| `fund_industry_allocation` | (fund_code, report_date, industry_name) | 基金行业配置比例 | INSERT OR REPLACE |
| `stock_industry` | stock_code | 个股行业分类（申万一级） | INSERT OR REPLACE |

> `INSERT OR IGNORE` = 不覆盖已有数据，安全追加  
> `INSERT OR REPLACE` = 允许更新（行业数据可能修正）

### JSON 缓存 (`cache/{platform}/*.json`)

每次运行前清除，存储当次运行的中间结果：

```
cache/
├── fund_nav.db                      # SQLite（持久保留）
├── alipay/
│   ├── fund_allocation.json         # 基金资产配置
│   ├── fund_info.json               # 基金类型分类
│   └── penetration_summary.json     # 穿透汇总结果
├── futu/
│   ├── classified_holdings.json     # 分类后的持仓
│   └── penetration_summary.json
├── huatai/                          # 结构同 futu
├── qieman/                          # 结构同 alipay
└── snowball/                        # 结构同 alipay
```

## 环境准备

```bash
# 安装依赖
pip3 install -r requirements.txt
```

主要依赖：

| 包 | 用途 |
|----|------|
| `pdfplumber` | PDF 表格/文本提取 |
| `pandas` + `openpyxl` | 数据处理 + Excel 读取 |
| `akshare` | A股/基金/ETF 数据 API |
| `PyYAML` | 配置文件解析 |
| `tabulate` | 终端表格格式化输出 |

## 使用方式

### 1. 准备数据文件

将各平台导出的持仓文件放入 `data/` 目录，文件名需以对应平台名为前缀：

| 文件名模式 | 来源 | 格式 |
|------------|------|------|
| `alipay_*.pdf` | 支付宝 | PDF |
| `qieman_*.pdf` | 且慢 | PDF |
| `snowball_*.pdf` | 雪球 | PDF |
| `huatai_*.xlsx` | 华泰证券 | Excel |
| `futu_*.pdf` | 富途证券 | PDF |

### 2. 检查数据提取结果（可选）

正式运行前，可先用 `inspect_data.py` 检查文件解析是否正常：

```bash
# 扫描整个 data/ 目录
python3 inspect_data.py --data-dir data

# 只检查单个文件
python3 inspect_data.py --file data/alipay_20260225.pdf

# 输出保存到文件
python3 inspect_data.py --data-dir data 2>&1 | tee output/inspect_result.log
```

**关注点**：
- 表格列名是否正确识别（基金代码、份额、市值等）
- 跨页表格数据是否完整
- Excel 中股票/基金区的行范围是否准确

### 3. 运行主程序（一键执行完整流水线）

```bash
# 对每个基金查询资产配置、行业分布、汇总输出
python3 main.py --deep=False

# 对每个基金查询资产配置、持股明细、行业分布、汇总输出
python3 main.py --deep=True
```

程序会自动执行完整流水线（Step 0 → Step 8），最终在 `output/` 目录下生成汇总报告和指标文件。

### 4. 完整执行顺序

```
┌─────────────────────────────────────────────────────────────────┐
│  完整执行顺序 (main.py 一键编排)                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Step 0: 清除 JSON 缓存                                         │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  删除 cache/{platform}/*.json                          │     │
│  │  保留 cache/fund_nav.db (SQLite 行情数据库不清除)        │     │
│  └───────────────────────────────────────────────────────┘     │
│         |                                                       │
│         v                                                       │
│  Step 1-5: 逐平台解析 + 分类 + 穿透                              │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  对每个平台执行：                                        │     │
│  │    Parse(解析PDF/Excel)                                 │     │
│  │    -> Classify(资产分类)                                 │     │
│  │    -> Convert(币种换算)                                  │     │
│  │    -> Penetrate(基金穿透，拆解底层资产)                    │     │
│  │                                                         │     │
│  │  平台: alipay / futu / huatai / qieman / snowball       │     │
│  └───────────────────────────────────────────────────────┘     │
│         |                                                       │
│         v                                                       │
│  Step 6: 跨平台聚合                                              │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  合并所有平台穿透结果                                     │     │
│  │  输出: output/aggregated_summary_YYYYMMDD.json          │     │
│  └───────────────────────────────────────────────────────┘     │
│         |                                                       │
│         v                                                       │
│  Step 7: 抓取历史行情数据                                         │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  全量抓取各类资产的历史价格：                               │     │
│  │    场外基金 NAV (alipay/qieman/snowball)                 │     │
│  │    场内 ETF 行情 (huatai)                                │     │
│  │    A股/港股/美股行情 (huatai/futu)                        │     │
│  │  数据写入: cache/fund_nav.db                             │     │
│  └───────────────────────────────────────────────────────┘     │
│         |                                                       │
│         v                                                       │
│  Step 8: 计算量化指标                                             │
│  ┌───────────────────────────────────────────────────────┐     │
│  │  基于历史行情计算：                                       │     │
│  │    单资产: 收益率/波动率/最大回撤/夏普比率                  │     │
│  │    Portfolio: 整体收益/波动/相关性矩阵                     │     │
│  │  时间窗口: 1M / 3M / 6M / 1Y / 3Y / 5Y                 │     │
│  │  输出: output/indicators_YYYYMMDD.json                  │     │
│  └───────────────────────────────────────────────────────┘     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 5. 分步手动执行

也可以单独运行各个步骤：

| 步骤 | 对应脚本 | 运行命令 |
|------|---------|---------|
| 解析+穿透(单平台) | `script/get_xxx_penetration.py` | `python3 -m script.get_alipay_penetration --file alipay_20260225.pdf --date 20260225` |
| 获取历史价格 | `script/fetch_nav_data.py` | `python3 -m script.fetch_nav_data --all` |
| 计算指标 | `script/compute_indicator.py` | `python3 -m script.compute_indicator` |

```bash
# 支付宝（基金类，支持 deep 穿透）
python3 -m script.get_alipay_penetration --file alipay_20260225.pdf --date 20260225

# 华泰（券商类）
python3 -m script.get_huatai_penetration --file huatai_20250213.xlsx --date 20250213

# 富途（券商类）
python3 -m script.get_futu_penetration --file futu_20260225.pdf --date 20260225

# 抓取全量历史行情（基金历史净值和个股历史价格）
python3 -m script.fetch_nav_data --all

# 计算量化指标
python3 -m script.compute_indicator

# 获取个股的行业分布，和基金的行业穿透数据
python3 -m script.test_industry_to_sqlite.py

# 可视化
streamlit run dashboard/app.py
```

## 项目结构

```
portfolio_analysis/
├── main.py                     # 主程序入口，编排全流程
├── config.yaml                 # 配置文件（平台/汇率/默认配置/路径）
├── models.py                   # 数据模型（HoldingRecord / FundAllocation / BaseParser）
├── requirements.txt            # Python 依赖
├── token.json                  # API Token 配置
│
├── data/                       # 原始持仓文件（PDF/Excel），需手动放入
│   ├── alipay_*.pdf            #   支付宝持仓 PDF
│   ├── qieman_*.pdf            #   且慢持仓 PDF
│   ├── snowball_*.pdf          #   雪球持仓 PDF
│   ├── huatai_*.xlsx           #   华泰持仓 Excel
│   └── futu_*.pdf              #   富途每日结单 PDF
│
├── parsers/                    # 各平台文件解析器
│   ├── __init__.py
│   ├── alipay_parser.py        #   支付宝 PDF 解析
│   ├── qieman_parser.py        #   且慢 PDF 解析
│   ├── snowball_parser.py      #   雪球 PDF 解析
│   ├── huatai_parser.py        #   华泰 Excel 解析
│   └── futu_parser.py          #   富途 PDF 解析
│
├── analyzers/                  # 分析引擎
│   ├── __init__.py
│   ├── classifier.py           #   资产分类器（按代码/名称判定类型）
│   ├── fund_penetration.py     #   基金穿透（拆解底层资产配置）
│   ├── fund_nav_db.py          #   SQLite 数据库管理（行情/行业数据）
│   ├── aggregator.py           #   跨平台聚合器
│   ├── indicators.py           #   量化指标计算（收益率/波动率/夏普/相关性）
│   └── cache_utils.py          #   JSON 缓存工具函数
│
├── script/                     # 可执行脚本（python3 -m script.xxx）
│   ├── __init__.py
│   ├── get_alipay_penetration.py   # 支付宝穿透
│   ├── get_qieman_penetration.py   # 且慢穿透
│   ├── get_snowball_penetration.py # 雪球穿透
│   ├── get_huatai_penetration.py   # 华泰穿透
│   ├── get_futu_penetration.py     # 富途穿透
│   ├── fetch_nav_data.py           # 抓取历史行情数据
│   ├── compute_indicator.py        # 计算量化指标
│   ├── inspect_data.py             # 数据文件结构检查工具
│   ├── test_parsers.py             # 解析器单元测试
│   └── test_industry_to_sqlite.py  # 行业数据入库测试
│
├── dashboard/                  # Streamlit 可视化看板
│   ├── __init__.py
│   ├── app.py                  #   看板主入口（streamlit run dashboard/app.py）
│   ├── pages/                  #   多页面模块
│   │   ├── __init__.py
│   │   ├── overview.py         #     总览页（资产配置饼图/平台分布）
│   │   ├── holdings.py         #     持仓明细页
│   │   ├── portfolio.py        #     组合分析页（加权收益/波动/夏普）
│   │   └── asset_metrics.py    #     个股体检页（单资产指标详情）
│   └── utils/                  #   工具模块
│       ├── __init__.py
│       ├── data_loader.py      #     数据加载器（读取缓存/SQLite）
│       └── chart_helper.py     #     图表绘制辅助函数
│
│
├── develop_log/                # 开发日志与设计文档
│   ├── akshare_api.md          #   akshare API 调研笔记
│   ├── dashboard.md            #   看板设计文档
│   ├── extract_data.md         #   数据提取设计
│   └── indicator_items.md      #   指标项设计说明
│
├── cache/                      # 缓存目录
│   ├── fund_nav.db             #   SQLite 数据库（持久化，行情/行业数据）
│   ├── indicator_results.json  #   量化指标计算结果缓存
│   ├── alipay/                 #   支付宝穿透中间结果
│   ├── futu/                   #   富途穿透中间结果
│   ├── huatai/                 #   华泰穿透中间结果
│   ├── qieman/                 #   且慢穿透中间结果
│   └── snowball/               #   雪球穿透中间结果
│
└── output/                     # 输出目录
    └── aggregated_summary_YYYYMMDD.json  # 最终汇总报告
```
