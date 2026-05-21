# 项目中 akshare API 使用说明

本文档记录项目中所有调用 akshare 库的 API 函数，包括用途、参数、返回结果及使用位置。

---

## 总览

| # | API 函数名 | 所在文件 | 用途 |
|---|-----------|---------|------|
| 1 | `fund_individual_detail_hold_xq` | `analyzers/fund_penetration.py` | 基金大类资产配置 |
| 2 | `fund_portfolio_hold_em` | `analyzers/fund_penetration.py` | 基金重仓股票 |
| 3 | `fund_portfolio_bond_hold_em` | `analyzers/fund_penetration.py` | 基金重仓债券 |
| 4 | `index_stock_cons_csindex` | `analyzers/fund_penetration.py` | 中证系列指数成分股 |
| 5 | `index_stock_cons` | `analyzers/fund_penetration.py` | 指数成分股（fallback） |
| 6 | `fund_portfolio_industry_allocation_em` | `analyzers/fund_penetration.py` | 基金行业配置分布 |
| 7 | `fund_name_em` | `analyzers/classifier.py` | 全量基金名称列表 |
| 8 | `fund_money_fund_info_em` | `analyzers/fund_nav_db.py` | 货币基金收益数据 |
| 9 | `fund_open_fund_info_em` | `analyzers/fund_nav_db.py` | 场外开放式基金净值 |
| 10 | `fund_etf_fund_info_em` | `analyzers/fund_nav_db.py` | 场内 ETF 净值 |
| 11 | `stock_zh_a_daily` | `analyzers/fund_nav_db.py` | A 股日线行情 |
| 12 | `stock_hk_daily` | `analyzers/fund_nav_db.py` | 港股日线行情 |
| 13 | `stock_us_daily` | `analyzers/fund_nav_db.py` | 美股日线行情 |
| 14 | `stock_individual_basic_info_xq` | `script/test_industry_to_sqlite.py` | A股个股基本信息（雪球） |
| 15 | `stock_individual_basic_info_us_xq` | `script/test_industry_to_sqlite.py` | 美股个股基本信息（雪球） |
| 16 | `stock_hk_company_profile_em` | `script/test_industry_to_sqlite.py` | 港股个股公司概况（东方财富） |

---

## 详细说明

---

### 1. `ak.fund_individual_detail_hold_xq`

- **用途**: 查询基金的大类资产配置比例（股票/债券/现金/其他）
- **所在文件**: `analyzers/fund_penetration.py` (行 137)
- **调用方式**:
  ```python
  df = ak.fund_individual_detail_hold_xq(symbol=code, date=query_date)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 基金代码（6位） | `"000628"` |
  | date | str | 查询日期，格式 YYYYMMDD | `"20251231"` |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 资产类型 | 资产分类名称（股票/债券/现金/其他） |
  | 仓位占比 | 该类资产占基金净值的百分比 |
- **项目中的使用逻辑**: 遍历各行，将"股票"映射为 equity、"债券"映射为 bond、"现金"映射为 cash，用于计算基金的资产分配比例

---

### 2. `ak.fund_portfolio_hold_em`

- **用途**: 查询基金的重仓股票持仓明细（Top10 股票）
- **所在文件**: `analyzers/fund_penetration.py` (行 299)
- **调用方式**:
  ```python
  df = ak.fund_portfolio_hold_em(symbol=code, date=year)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 基金代码（6位） | `"000628"` |
  | date | str | 年份，格式 YYYY | `"2025"` |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 股票代码 | A 股代码 |
  | 股票名称 | 股票简称 |
  | 占净值比例 | 该股票市值占基金净值的百分比 |
  | 季度 | 报告期标识（如"2025年1季度报告"） |
- **项目中的使用逻辑**: 通过 `_get_latest_season` 取最新一期，提取代码、名称、占比，构建持仓列表用于穿透分析
- **注意**: 当 date 年份无数据时，会自动尝试 fallback 到上一年

---

### 3. `ak.fund_portfolio_bond_hold_em`

- **用途**: 查询基金的重仓债券持仓明细（Top5 债券）
- **所在文件**: `analyzers/fund_penetration.py` (行 324)
- **调用方式**:
  ```python
  df = ak.fund_portfolio_bond_hold_em(symbol=code, date=year)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 基金代码（6位） | `"110027"` |
  | date | str | 年份，格式 YYYY | `"2025"` |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 债券代码 | 债券代码 |
  | 债券名称 | 债券简称 |
  | 占净值比例 | 该债券市值占基金净值的百分比 |
- **项目中的使用逻辑**: 取最新一期，提取债券代码、名称、占比，构建债券持仓列表
- **注意**: 同样有年份 fallback 机制

---

### 4. `ak.index_stock_cons_csindex`

- **用途**: 查询中证系列指数的成分股及权重（首选方案）
- **所在文件**: `analyzers/fund_penetration.py` (行 349)
- **调用方式**:
  ```python
  df = ak.index_stock_cons_csindex(symbol=idx_code)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 指数代码 | `"000300"` (沪深300) |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 成分券代码 | 成分股代码 |
  | 成分券名称 | 成分股名称 |
  | 权重 | 该成分股在指数中的权重(%) |
- **项目中的使用逻辑**: 获取被动指数基金跟踪的指数成分股，用于穿透分析被动基金的实际持仓

---

### 5. `ak.index_stock_cons`

- **用途**: 查询指数成分股（当 `index_stock_cons_csindex` 失败时的备用方案）
- **所在文件**: `analyzers/fund_penetration.py` (行 353)
- **调用方式**:
  ```python
  df = ak.index_stock_cons(symbol=idx_code)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 指数代码 | `"000300"` |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 品种代码 | 成分股代码 |
  | 品种名称 | 成分股名称 |
  | 权重 | 权重(%) |
- **项目中的使用逻辑**: 作为 `index_stock_cons_csindex` 的 fallback，功能相同但数据源不同

---

### 6. `ak.fund_portfolio_industry_allocation_em`

- **用途**: 查询基金的行业配置分布（按证监会行业分类）
- **所在文件**: `analyzers/fund_penetration.py` (行 435)、`script/test_industry_to_sqlite.py` (行 73)
- **调用方式**:
  ```python
  df = ak.fund_portfolio_industry_allocation_em(symbol=fund_code, date=year)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 基金代码（6位） | `"000628"` |
  | date | str | 年份，格式 YYYY | `"2025"` |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 截止时间 | 报告期截止日期 |
  | 行业类别 | 证监会行业分类名称 |
  | 占净值比例 | 该行业投资占基金净值比例(%) |
- **项目中的使用逻辑**: 按"截止时间"取最新一期，遍历提取行业名和占比，写入 SQLite `fund_industry_allocation` 表
- **注意**: 只接受基金代码，不能传股票代码；货币基金/商品基金/QDII 通常无数据

---

### 7. `ak.fund_name_em`

- **用途**: 获取全市场基金名称列表（用于根据代码查询基金类型/子类型）
- **所在文件**: `analyzers/classifier.py` (行 96)
- **调用方式**:
  ```python
  df = ak.fund_name_em()
  ```
- **参数**: 无
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 基金代码 | 6位基金代码 |
  | 基金简称 | 基金全称 |
  | 基金类型 | 分类（如股票型、混合型、债券型等） |
- **项目中的使用逻辑**: 程序启动时加载一次，缓存为全局变量，后续通过基金代码匹配查询基金的子类型分类
- **注意**: 此接口返回全量数据（约1万+条），首次调用较慢，建议全局缓存

---

### 8. `ak.fund_money_fund_info_em`

- **用途**: 获取货币基金的历史收益数据（万份收益 + 7日年化）
- **所在文件**: `analyzers/fund_nav_db.py` (行 200)
- **调用方式**:
  ```python
  df = ak.fund_money_fund_info_em(symbol=code)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 货币基金代码（6位） | `"000198"` |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 净值日期 | 日期 |
  | 万份收益 | 每万份基金当日收益(元) |
  | 7日年化收益率(%) | 7日滚动年化收益率 |
- **项目中的使用逻辑**: 7日年化收益率存为 unit_nav，万份收益存为 acc_nav，写入 `fund_daily_nav` 表
- **注意**: 仅适用于货币型基金（sub_type = "money_fund"）

---

### 9. `ak.fund_open_fund_info_em`

- **用途**: 获取场外开放式基金的历史单位净值走势
- **所在文件**: `analyzers/fund_nav_db.py` (行 202)
- **调用方式**:
  ```python
  df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 基金代码（6位） | `"000628"` |
  | indicator | str | 数据类型标识 | `"单位净值走势"` |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 净值日期 | 日期 |
  | 单位净值 | 当日单位净值 |
  | 日增长率 | 当日涨跌幅(%) |
- **项目中的使用逻辑**: 存入 `fund_daily_nav` 表，用于后续计算收益率、最大回撤等指标
- **注意**: 适用于非货币型场外基金（股票型、混合型、债券型、QDII 等）

---

### 10. `ak.fund_etf_fund_info_em`

- **用途**: 获取场内 ETF 的历史净值数据
- **所在文件**: `analyzers/fund_nav_db.py` (行 390)
- **调用方式**:
  ```python
  df = ak.fund_etf_fund_info_em(fund=code, start_date=start_date, end_date=end_date)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | fund | str | ETF 基金代码（6位） | `"510300"` |
  | start_date | str | 起始日期，格式 YYYYMMDD | `"20200101"` |
  | end_date | str | 结束日期，格式 YYYYMMDD | `"20260101"` |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | 净值日期 | 日期 |
  | 单位净值 | 当日单位净值 |
  | 累计净值 | 累计净值 |
  | 日增长率 | 当日涨跌幅(%) |
  | 申购状态 | 场内申购状态 |
  | 赎回状态 | 场内赎回状态 |
- **项目中的使用逻辑**: 单位净值存为 close，日增长率存为 pct_change，写入 `etf_daily_hist` 表
- **注意**: 该接口支持日期范围参数，注意参数名是 `fund` 而非 `symbol`

---

### 11. `ak.stock_zh_a_daily`

- **用途**: 获取 A 股个股的日线行情数据（前复权）
- **所在文件**: `analyzers/fund_nav_db.py` (行 544)
- **调用方式**:
  ```python
  df = ak.stock_zh_a_daily(symbol=f"{prefix}{code}", start_date=start_date, end_date=end_date, adjust="qfq")
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 交易所前缀+代码 | `"sh600519"`, `"sz000001"` |
  | start_date | str | 起始日期，格式 YYYYMMDD | `"20200101"` |
  | end_date | str | 结束日期，格式 YYYYMMDD | `"20260101"` |
  | adjust | str | 复权方式 | `"qfq"` (前复权) |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | date | 交易日期 |
  | open | 开盘价 |
  | high | 最高价 |
  | low | 最低价 |
  | close | 收盘价 |
  | volume | 成交量 |
- **项目中的使用逻辑**: 存入 `stock_daily_hist` 表，market="A-share"
- **注意**: symbol 需要加交易所前缀——6开头为"sh"，其余为"sz"

---

### 12. `ak.stock_hk_daily`

- **用途**: 获取港股个股的日线行情数据（前复权）
- **所在文件**: `analyzers/fund_nav_db.py` (行 608)
- **调用方式**:
  ```python
  df = ak.stock_hk_daily(symbol=code, adjust="qfq")
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 港股代码（5位，含前导0） | `"00700"` (腾讯) |
  | adjust | str | 复权方式 | `"qfq"` (前复权) |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | date | 交易日期 |
  | open | 开盘价 |
  | high | 最高价 |
  | low | 最低价 |
  | close | 收盘价 |
  | volume | 成交量 |
- **项目中的使用逻辑**: 本地过滤日期范围后存入 `stock_daily_hist` 表，market="H-share"
- **注意**: 不支持 start_date/end_date 参数，返回全量历史数据，需本地过滤

---

### 13. `ak.stock_us_daily`

- **用途**: 获取美股个股的日线行情数据（前复权）
- **所在文件**: `analyzers/fund_nav_db.py` (行 675)
- **调用方式**:
  ```python
  df = ak.stock_us_daily(symbol=code, adjust="qfq")
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 美股代码（如雪球格式） | `"AAPL"`, `".IXIC"` |
  | adjust | str | 复权方式 | `"qfq"` (前复权) |
- **返回 DataFrame 字段**:
  | 列名 | 说明 |
  |------|------|
  | date | 交易日期 |
  | open | 开盘价 |
  | high | 最高价 |
  | low | 最低价 |
  | close | 收盘价 |
  | volume | 成交量 |
- **项目中的使用逻辑**: 本地过滤日期范围后存入 `stock_daily_hist` 表，market="US"
- **注意**: 不支持 start_date/end_date 参数，返回全量历史数据，需本地过滤

---

### 14. `ak.stock_individual_basic_info_xq`

- **用途**: 查询 A 股个股基本信息（数据源：雪球），主要用于获取个股所属行业
- **所在文件**: `script/test_industry_to_sqlite.py` (行 268)
- **调用方式**:
  ```python
  df = ak.stock_individual_basic_info_xq(symbol="SH600519", token=xq_a_token)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 雪球格式代码（SH/SZ + 6位数字） | `"SH600519"`, `"SZ000001"` |
  | token | str | 雪球 xq_a_token（从浏览器 Cookie 获取） | 登录雪球后获取 |
  | timeout | float | 请求超时时间（秒），可选 | `None` |
- **返回 DataFrame 字段**（item/value 两列格式）:
  | item 值 | 说明 |
  |---------|------|
  | org_name_cn | 公司全称 |
  | org_short_name_cn | 公司简称 |
  | main_operation_business | 主营业务描述 |
  | affiliate_industry | 所属行业，格式为 `{'ind_code': 'BK0088', 'ind_name': '白酒'}` |
  | staff_num | 员工人数 |
  | listed_date | 上市日期（时间戳） |
  | actual_controller | 实际控制人 |
- **项目中的使用逻辑**: 提取 `affiliate_industry` 字段中的 `ind_name` 作为个股行业分类，存入 SQLite `stock_industry` 表
- **注意**:
  - symbol 格式要求：6开头加 `SH` 前缀，0/3开头加 `SZ` 前缀
  - 需要有效的雪球 token，通过命令行 `--token` 参数传入
  - token 获取方式：登录雪球网页版 → 浏览器开发者工具 → Application → Cookies → `xq_a_token`

---

### 15. `ak.stock_individual_basic_info_us_xq`

- **用途**: 查询美股个股基本信息（数据源：雪球），主要用于获取主营业务描述
- **所在文件**: `script/test_industry_to_sqlite.py` (行 305)
- **调用方式**:
  ```python
  df = ak.stock_individual_basic_info_us_xq(symbol="NVDA", token=xq_a_token)
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 美股 ticker 代码 | `"NVDA"`, `"PDD"`, `"BRK.B"` |
  | token | str | 雪球 xq_a_token（从浏览器 Cookie 获取） | 登录雪球后获取 |
  | timeout | float | 请求超时时间（秒），可选 | `None` |
- **返回 DataFrame 字段**（item/value 两列格式）:
  | item 值 | 说明 |
  |---------|------|
  | org_name_cn | 公司中文全称 |
  | org_short_name_en | 公司英文简称 |
  | main_operation_business | 主营业务描述（中文） |
  | operating_scope | 经营范围详细描述 |
  | org_cn_introduction | 公司简介 |
  | staff_num | 员工人数 |
  | td_mkt | 上市交易所 |
  | mainholder | 主要股东 |
- **项目中的使用逻辑**: 提取 `main_operation_business` 字段作为美股个股行业/业务描述，存入 SQLite `stock_industry` 表
- **注意**:
  - symbol 直接使用美股 ticker（如 `NVDA`、`PDD`），无需加前缀
  - 需要有效的雪球 token，与 A 股接口共用同一个 token
  - 美股没有 `affiliate_industry` 字段，只能通过 `main_operation_business` 获取业务信息

---

### 16. `ak.stock_hk_company_profile_em`

- **用途**: 查询港股个股公司概况（数据源：东方财富），主要用于获取个股所属行业
- **所在文件**: `script/test_industry_to_sqlite.py` (行 286)
- **调用方式**:
  ```python
  df = ak.stock_hk_company_profile_em(symbol="01919")
  ```
- **参数**:
  | 参数 | 类型 | 说明 | 示例 |
  |------|------|------|------|
  | symbol | str | 港股代码（5位，含前导零） | `"01919"`, `"00700"`, `"02020"` |
- **返回 DataFrame 字段**（列名即字段名，通常只有1行数据）:
  | 列名 | 说明 |
  |------|------|
  | 公司名称 | 公司中文全称 |
  | 英文名称 | 公司英文全称 |
  | 注册地 | 公司注册地 |
  | 所属行业 | 行业分类名称（如"工用运输"、"零售业"） |
  | 董事长 | 董事长姓名 |
  | 员工人数 | 员工总数 |
  | 公司成立日期 | 成立日期 |
  | 公司网址 | 官网地址 |
  | 公司介绍 | 公司简介 |
- **项目中的使用逻辑**: 提取 `所属行业` 列的值作为港股个股行业分类，存入 SQLite `stock_industry` 表
- **注意**:
  - symbol 必须是 5 位数字字符串（不足5位需前导补零），如腾讯为 `"00700"`
  - 不需要 token，数据源为东方财富免费接口
  - 代码中先通过 `stock_code.replace("HK", "").lstrip("0").zfill(5)` 统一转换格式

---

## 使用注意事项

1. **请求频率**: akshare 底层调用东方财富等网站接口，建议调用间隔 ≥ 0.5s，批量抓取时用 `time.sleep()` 避免被封 IP
2. **数据延迟**: 基金持仓数据按季度披露，通常滞后 1-2 个月；行业配置同理
3. **fallback 机制**: 项目中对 `fund_portfolio_hold_em`、`fund_portfolio_bond_hold_em` 设置了年份 fallback（当前年无数据时尝试上一年）
4. **异常处理**: 所有 API 调用均包裹在 try-except 中，失败时返回 None/空列表，不阻断流程
5. **代码前缀**:
   - A 股: 需加 "sh"/"sz" 前缀（`stock_zh_a_daily`）；雪球接口需加 "SH"/"SZ" 前缀（`stock_individual_basic_info_xq`）
   - 港股: 5位代码含前导零（`stock_hk_daily`）
   - 美股: 直接用 ticker（`stock_us_daily`、`stock_individual_basic_info_us_xq`）
   - 基金: 均为6位纯数字代码
6. **雪球 token**: `stock_individual_basic_info_xq` 和 `stock_individual_basic_info_us_xq` 需要雪球登录 token，通过命令行 `--token` 参数传入，不要硬编码到代码中。token 有时效性，过期需重新从浏览器获取
