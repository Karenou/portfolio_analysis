# 历史行情数据抓取脚本设计文档

## 1. 为什么要做

### 现状问题

当前数据库 `cache/fund_nav.db` 中仅有 **7 个资产**有历史行情数据（5 只基金 + 2 只 ETF），但实际各平台合计有 **130 个唯一资产**需要行情数据：

| 数据来源 | 待抓取 | 已抓取 | 完成率 |
|---------|--------|--------|--------|
| alipay 场外基金 | 36 | 5 | 14% |
| qieman 场外基金 | 69 | (共用) | - |
| snowball 场外基金 | 42 | (共用) | - |
| huatai ETF | 9 | 2 | 22% |
| huatai A股 | 1 | 0 | 0% |
| futu 港股 | 6 | 0 | 0% |
| futu 美股 | 2 | 0 | 0% |
| **去重合计** | **~130** | **7** | **5%** |

### 根本原因

1. 场外基金 NAV 抓取入口分散在 `script/get_xxx_penetration.py --fetch-nav`，需要手动逐平台执行
2. ETF / 股票 / 港股 / 美股抓取函数写在 `fund_nav_db.py` 中，但**没有任何入口脚本调用它们**
3. 之前执行时可能因网络超时/API 限流中途中断，没有自动重试

### 目标

提供一个**独立的、一键式数据抓取脚本**：
- 用户按需手动执行（低频操作，每周/每月一次）
- 执行前先展示诊断信息（待抓取 vs 已抓取）
- 支持选择性抓取（只抓某个平台 / 某种类型）
- 失败时有清晰日志，方便排查

---

## 2. 怎么做

### 整体架构

```
用户执行: python -m scripts.fetch_nav_data
│
├── --check ──────► 诊断模式（只展示各平台待抓取/已抓取数量）
│
├── --all ────────► 全量抓取（所有平台所有类型）
│                   │
│                   ├── Step 1: 场外基金 NAV (alipay → qieman → snowball) ──► fund_daily_nav 表
│                   ├── Step 2: 场内 ETF (huatai ETF 列表) ────────────────► etf_daily_hist 表
│                   ├── Step 3: A股 (huatai 股票列表) ──────────────────────► stock_daily_hist 表
│                   ├── Step 4: 港股 (futu 港股列表) ───────────────────────► stock_daily_hist 表
│                   └── Step 5: 美股 (futu 美股列表) ───────────────────────► stock_daily_hist 表
│                                                                              │
│                                                                              ▼
├── --platform xxx ► 指定平台（只抓某个平台）                              输出汇总报告
│
└── --type xxx ───► 指定类型（只抓 fund/etf/stock）
```

### 与现有代码的关系

```
┌─────────────────────────────────────┐       ┌──────────────────────────────────────────────────┐
│  新增脚本                            │       │  现有模块（不修改）                                │
│                                     │       │                                                  │
│  scripts/fetch_nav_data.py          │──────►│  fund_nav_db.py                                  │
│  （调度层 + 诊断层）                  │ 调用   │    ├── fetch_and_store_fund_nav(platform)         │
│                                     │       │    ├── fetch_and_store_etf_hist()                 │
│                                     │       │    ├── fetch_and_store_stock_hist()               │
│                                     │       │    ├── fetch_and_store_stock_hk_hist()            │
│                                     │       │    └── fetch_and_store_stock_us_hist()            │
└─────────────────────────────────────┘       └──────────────────────────────────────────────────┘
```

**设计原则**：新脚本只做**调度和诊断**，实际抓取逻辑复用 `fund_nav_db.py` 中已有的函数，不重复实现。

---

## 3. 具体步骤

### Step 1: 创建 `script/fetch_nav_data.py`

文件位置：`portfolio_analysis/script/fetch_nav_data.py`

#### 3.1 核心功能模块

| 模块 | 功能 | 说明 |
|------|------|------|
| `check_status()` | 诊断检查 | 读取各平台 fund_info.json / classified_holdings.json，对比数据库中已有数据，输出表格 |
| `fetch_all()` | 全量抓取 | 按顺序调用所有抓取函数 |
| `fetch_by_platform()` | 按平台抓取 | 只抓指定平台的场外基金 |
| `fetch_by_type()` | 按类型抓取 | 只抓 fund / etf / stock_a / stock_hk / stock_us |

#### 3.2 命令行接口设计

```bash
# 模式 1: 诊断检查（默认行为，不执行抓取）
python -m script.fetch_nav_data --check

# 模式 2: 全量抓取
python -m script.fetch_nav_data --all

# 模式 3: 只抓某个平台的基金
python -m script.fetch_nav_data --platform alipay
python -m script.fetch_nav_data --platform qieman
python -m script.fetch_nav_data --platform snowball

# 模式 4: 只抓某种类型
python -m script.fetch_nav_data --type etf
python -m script.fetch_nav_data --type stock_a
python -m script.fetch_nav_data --type stock_hk
python -m script.fetch_nav_data --type stock_us
```

#### 3.3 诊断输出示例

```
╔══════════════════════════════════════════════════════════════╗
║          历史行情数据抓取状态诊断                              ║
╚══════════════════════════════════════════════════════════════╝

平台         类型        待抓取   已抓取   缺失    最新日期
─────────────────────────────────────────────────────────────
alipay       fund         36       5      31     2026-05-19
qieman       fund         69      12      57     2026-05-19
snowball     fund         42       8      34     2026-05-19
huatai       etf           9       2       7     2026-05-19
huatai       stock_a       1       0       1     -
futu         stock_hk      6       0       6     -
futu         stock_us      2       0       2     -
─────────────────────────────────────────────────────────────
合计（去重）               130       7     123

⚠️ 当前数据覆盖率: 5.4%，建议执行 --all 进行全量抓取
预估耗时: ~3-5 分钟（130 个资产 × 0.3s API 间隔 + 网络耗时）
```

#### 3.4 抓取执行流程

```
用户 ──► fetch_nav_data.py ──► fund_nav_db.py ──► akshare API
 │              │                      │                │
 │   1. --all   │                      │                │
 │──────────────►                      │                │
 │              │  check_status()      │                │
 │◄─── 显示诊断 ─┤                      │                │
 │              │  确认继续？[Y/n]       │                │
 │── Y ────────►│                      │                │
 │              │                      │                │
 │              │  ┌─ loop: alipay/qieman/snowball ─┐   │
 │              │  │ fetch_and_store_fund_nav(plat) ─┼──►│ fund_open_fund_info_em
 │              │  │                                │◄──┤ DataFrame
 │              │  │ 写入 fund_daily_nav 表         │   │
 │◄─ 进度反馈 ──┤  └───────────────────────────────────┘   │
 │              │                      │                │
 │              │  fetch_and_store_etf_hist() ──────────►│ fund_etf_fund_info_em
 │              │  fetch_and_store_stock_hist() ────────►│ stock_zh_a_daily
 │              │  fetch_and_store_stock_hk_hist() ─────►│ stock_hk_daily
 │              │  fetch_and_store_stock_us_hist() ─────►│ stock_us_daily
 │              │                      │                │
 │              │  再次 check_status()  │                │
 │◄─ 汇总报告 ──┤                      │                │
```

#### 3.5 错误处理策略

| 场景 | 处理方式 |
|------|---------|
| 单个资产 API 失败 | 记录日志，继续下一个（已在 fund_nav_db.py 中实现） |
| 整个平台 fund_info.json 缺失 | 跳过该平台，日志 WARNING |
| 数据库文件不存在 | 自动创建（fund_nav_db 已支持） |
| 用户中断 (Ctrl+C) | 已提交的数据保留，打印中断位置 |
| 增量判断（3天内有数据） | 自动跳过，避免重复抓取 |

---

## 4. 文件变动清单

| 操作 | 文件 | 说明 |
|------|------|------|
| **新增** | `script/fetch_nav_data.py` | 主脚本（~220 行） |
| 不修改 | `analyzers/fund_nav_db.py` | 复用已有抓取函数 |
| 不修改 | `config.yaml` | 无需额外配置 |

---

## 5. 预估耗时

| 阶段 | 资产数 | 单次 API 耗时 | 预估总耗时 |
|------|--------|--------------|-----------|
| 场外基金 NAV | ~120 只（去重） | 0.3s 间隔 + 1~2s 请求 | ~2-3 分钟 |
| ETF 历史 | 9 只 | 0.3s 间隔 + 1s 请求 | ~10 秒 |
| A股 | 1 只 | 即时 | ~2 秒 |
| 港股 | 6 只 | 0.3s + 全量返回 ~3s | ~20 秒 |
| 美股 | 2 只 | 0.3s + 全量返回 ~3s | ~7 秒 |
| **合计** | | | **~3-5 分钟** |

---

## 6. 后续可扩展

- 支持 `--force` 参数强制刷新（忽略 3 天增量判断）
- 支持 `--code xxx` 只抓特定代码
- 加入进度条 (tqdm)
- 加入重试机制（单个资产失败后 retry 一次）
