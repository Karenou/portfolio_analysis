# Portfolio Analysis - 综合持仓分析工具

穿透多个理财/券商 App 的持仓数据，整合分析股债商比例、行业分布、波动率等维度。

## 环境准备

```bash
# 安装依赖
pip3 install -r requirements.txt
```

## 数据文件

1. 需要手动将各 App 导出的持仓文件放在 `data/` 目录下，并且把文件名字前缀改为对应app的英文。目前支持：

| 文件 | 来源 | 格式 |
|------|------|------|
| `alipay_*.pdf` | 支付宝 | PDF |
| `qieman_*.pdf` | 且慢 | PDF |
| `snowball_*.pdf` | 雪球 | PDF |
| `futu_*.pdf` | 富途 | PDF |
| `huatai_*.xlsx` | 华泰 | Excel |

## 使用步骤

### Step 1：检查数据提取结果（inspect_data.py）

在正式运行分析前，可以离线先用 `inspect_data.py` 检查各文件的表格结构是否被正确提取：

```bash
# 扫描整个 data/ 目录，输出所有文件的表格结构
python3 inspect_data.py --data-dir data

# 只检查单个文件
python3 inspect_data.py --file data/alipay_20260225.pdf

# 控制每个表格显示的最大行数（默认10行）
python3 inspect_data.py --max-rows 20

# 将日志保存到文件，方便翻阅
python3 inspect_data.py --data-dir data 2>&1 | tee output/inspect_result.log
```

**关注点**：
- 检查表格列名是否正确识别（基金代码、基金名称、份额、市值等）
- 检查跨页的表格数据是否完整（如支付宝PDF有36条基金记录分布在3页）
- 检查 Excel 文件中股票持仓区和基金持仓区的行范围是否准确

### Step 2：运行主程序分析（main.py）

程序执行步骤
1. 删除cache文件夹下各个子文件夹的历史缓存文件
2. 逐个解析data文件夹下的资产持仓数据，然后进行持仓分析
3. 把分析结果以json格式保存到output文件夹内，输出文件名为`aggregated_summary_YYYYMMDD.json`，YYYYMMDD为程序执行当日日期。

```bash

python3 main.py

```

## 项目结构

```
portfolio_analysis/
├── data/                # 原始持仓数据文件（PDF/Excel）
├── output/              # 输出结果和日志
├── parsers/             # 各 App 的文件解析器
├── analyzers/           # 持仓分析模块
├── cache/               # 缓存数据
├── config.yaml          # 配置文件
├── models.py            # 数据模型定义
├── main.py              # 主程序入口
├── test/                # 测试代码
└── requirements.txt     # Python 依赖
```
