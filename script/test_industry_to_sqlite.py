"""测试程序：抓取当前持仓基金 & 个股的行业分布，并保存到 SQLite。

完整流程：
Part A - 基金行业分布：
  1. 从所有平台 fund_info.json 加载持仓基金（去重）
  2. 过滤掉货币基金和商品基金
  3. 调用 akshare fund_portfolio_industry_allocation_em API 获取行业分布
  4. 保存到 SQLite fund_industry_allocation 表
  5. 从 SQLite 读取验证数据完整性

Part B - 个股行业分布：
  6. 从所有平台 penetrated_details.json 加载持仓个股（去重）
  7. 过滤掉已有行业数据的个股（SQLite 缓存命中）
  8. 根据市场调用不同 akshare API：
     - A股: stock_individual_info_em → "行业"
     - 港股: stock_hk_company_profile_em → "所属行业"
     - 美股: stock_individual_basic_info_us_xq → "main_operation_business"
  9. 保存到 SQLite stock_industry 表
 10. 从 SQLite 读取验证

Part C - 汇总统计
"""

import argparse
import ast
import json
import os
import re
import sys
import time
from datetime import datetime
from typing import Optional

# 确保项目根目录在 path 中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import akshare as ak

from analyzers.fund_nav_db import (
    save_fund_industry_allocation,
    get_fund_industry_allocation,
    save_stock_industry,
    get_stock_industry,
    _get_conn,
    _DB_PATH,
)

# ──────────────────────────────────────────────
# 配置
# ──────────────────────────────────────────────
PLATFORMS = ["alipay", "qieman", "snowball", "huatai", "futu"]
CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache")
API_DELAY = 0.5  # API 调用间隔（秒）
SKIP_SUB_TYPES = {"money_fund", "commodity_fund"}  # 不查行业分布的基金类型
XQ_A_TOKEN = None  # 雪球 token，通过命令行参数 --token 传入


# ──────────────────────────────────────────────
# Part A: 基金行业分布
# ──────────────────────────────────────────────

def load_all_funds() -> dict:
    """从所有平台 fund_info.json 加载基金信息（去重）。

    Returns:
        {fund_code: {"name": ..., "sub_type": ..., "platforms": [...]}}
    """
    all_funds = {}
    for platform in PLATFORMS:
        path = os.path.join(CACHE_DIR, platform, "fund_info.json")
        if not os.path.exists(path):
            print(f"  [跳过] {platform}/fund_info.json 不存在")
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for code, info in data.items():
            if code not in all_funds:
                all_funds[code] = {
                    "name": info.get("name", ""),
                    "sub_type": info.get("sub_type", ""),
                    "platforms": [platform],
                }
            else:
                all_funds[code]["platforms"].append(platform)
    return all_funds


def query_fund_industry(fund_code: str, year: str) -> Optional[list]:
    """调用 akshare API 获取基金行业配置。

    Returns:
        [{"industry_name": "制造业", "industry_pct": 45.2}, ...] 或 None
    """
    try:
        df = ak.fund_portfolio_industry_allocation_em(symbol=fund_code, date=year)
        if df is None or df.empty:
            return None
        # 取最新一期
        latest_date = df["截止时间"].max()
        df_latest = df[df["截止时间"] == latest_date]
        rows = []
        for _, r in df_latest.iterrows():
            ind_name = str(r.get("行业类别", "")).strip()
            pct = float(r.get("占净值比例", 0) or 0)
            if ind_name and pct > 0:
                rows.append({"industry_name": ind_name, "industry_pct": pct})
        return rows if rows else None
    except Exception as e:
        return None


def run_fund_industry():
    """Part A: 基金行业分布抓取。"""
    print("\n" + "=" * 70)
    print("  Part A: 持仓基金行业分布抓取")
    print("=" * 70)

    # Step 1: 加载所有持仓基金
    print("\n[A-1] 加载持仓基金...")
    all_funds = load_all_funds()
    print(f"  共加载 {len(all_funds)} 只基金（去重后）")

    # Step 2: 过滤
    target_funds = {
        code: info for code, info in all_funds.items()
        if info["sub_type"] not in SKIP_SUB_TYPES
    }
    skipped = len(all_funds) - len(target_funds)
    print(f"  过滤掉货币/商品基金 {skipped} 只，剩余 {len(target_funds)} 只待查询")

    # 排除场内 ETF（场内ETF代码通常为6位且以51/15/16/52开头）
    otc_funds = {}
    etf_onexchange = {}
    for code, info in target_funds.items():
        if len(code) == 6 and code[:2] in ("51", "15", "16", "52"):
            etf_onexchange[code] = info
        else:
            otc_funds[code] = info
    print(f"  场内ETF {len(etf_onexchange)} 只（跳过，API不支持），场外基金 {len(otc_funds)} 只")

    # Step 3: 调用 API 抓取行业分布
    print(f"\n[A-2] 开始调用 akshare API 获取基金行业分布（预计耗时 {len(otc_funds) * API_DELAY:.0f}s）...")
    year = str(datetime.now().year)
    success_count = 0
    fail_count = 0
    results = {}

    for i, (code, info) in enumerate(otc_funds.items(), 1):
        time.sleep(API_DELAY)
        rows = query_fund_industry(code, year)
        if rows:
            results[code] = rows
            success_count += 1
            top = max(rows, key=lambda x: x["industry_pct"])
            status = f"✓ {len(rows)}个行业, TOP={top['industry_name']}({top['industry_pct']:.1f}%)"
        else:
            fail_count += 1
            status = "✗ 无数据"
        print(f"  [{i}/{len(otc_funds)}] {code} {info['name'][:20]:<22} {status}")

    print(f"\n  抓取完成: 成功 {success_count} 只, 失败/无数据 {fail_count} 只")

    # Step 4: 保存到 SQLite
    print(f"\n[A-3] 写入 SQLite ({_DB_PATH})...")
    saved_count = 0
    for code, rows in results.items():
        save_fund_industry_allocation(
            fund_code=code,
            report_date=year,
            industry_data=rows,
            data_source="fund_portfolio_industry_allocation_em",
        )
        saved_count += 1
    print(f"  写入完成: {saved_count} 只基金的行业数据已保存")

    # Step 5: 验证
    print(f"\n[A-4] 从 SQLite 读取验证...")
    verify_ok = 0
    verify_fail = 0
    for code in results:
        db_rows = get_fund_industry_allocation(code)
        if db_rows and len(db_rows) > 0:
            verify_ok += 1
        else:
            verify_fail += 1
            print(f"  ⚠ {code} 写入后读取失败!")
    print(f"  验证结果: 成功 {verify_ok}, 失败 {verify_fail}")

    return results


# ──────────────────────────────────────────────
# Part B: 个股行业分布
# ──────────────────────────────────────────────

def _classify_region(code: str) -> str:
    """根据股票代码格式判断市场：CN / HK / US。"""
    if re.match(r"^\d{6}$", code):
        return "CN"
    if re.match(r"^HK\d+$", code, re.I) or re.match(r"^\d{5}$", code):
        return "HK"
    # 其他（字母开头）视为美股
    if re.match(r"^[A-Za-z]", code):
        return "US"
    return "CN"


def load_all_stocks() -> dict:
    """从所有平台 classified_holdings.json 加载持仓个股（去重）。

    筛选条件：asset_class == "equity"
    市场判断：优先用 sub_type 字段（stock_hk / stock_us），否则通过代码格式推断

    Returns:
        {stock_code: {"name": ..., "region": ...}}
    """
    all_stocks = {}
    for platform in PLATFORMS:
        path = os.path.join(CACHE_DIR, platform, "classified_holdings.json")
        if not os.path.exists(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for item in data:
            code = item.get("code", "")
            # 只取 asset_class 为 equity 的记录
            if item.get("asset_class") != "equity":
                continue
            if not code or code in all_stocks:
                continue
            # 通过 sub_type 判断市场
            sub_type = item.get("sub_type", "")
            if sub_type == "stock_hk":
                region = "HK"
            elif sub_type == "stock_us":
                region = "US"
            elif sub_type in ("stock_cn", "stock_a"):
                region = "CN"
            else:
                region = _classify_region(code)
            all_stocks[code] = {
                "name": item.get("name", ""),
                "region": region,
            }
    return all_stocks


def query_stock_industry(stock_code: str, region: str) -> Optional[str]:
    """根据市场类型调用不同 akshare API 查询个股行业。

    Returns:
        行业名称字符串，或 None（查询失败时）
    """
    try:
        if region == "CN":
            if not XQ_A_TOKEN:
                print("  [警告] 未提供 --token 参数，跳过A股查询。请通过 --token 传入雪球 xq_a_token")
                return None
            # 纯数字代码转雪球格式：6开头为SH，其余为SZ
            if stock_code.isdigit():
                prefix = "SH" if stock_code.startswith("6") else "SZ"
                xq_symbol = f"{prefix}{stock_code}"
            else:
                xq_symbol = stock_code
            df = ak.stock_individual_basic_info_xq(symbol=xq_symbol, token=XQ_A_TOKEN)
            if df is not None and not df.empty:
                # 优先取 affiliate_industry 字段
                if "item" in df.columns:
                    row = df[df["item"] == "affiliate_industry"]
                    if not row.empty:
                        val = str(row.iloc[0]["value"]).strip()
                        # 解析 {'ind_code': 'BK0088', 'ind_name': '白酒'} 格式
                        if "ind_name" in val:
                            try:
                                ind_dict = ast.literal_eval(val)
                                return ind_dict.get("ind_name", val)
                            except Exception:
                                return val
                        if val and val not in ("nan", "None", ""):
                            return val

        elif region == "HK":
            # 港股代码统一处理为5位纯数字
            hk_code = stock_code.replace("HK", "").lstrip("0") or "0"
            hk_code = hk_code.zfill(5)
            df = ak.stock_hk_company_profile_em(symbol=hk_code)
            if df is not None and not df.empty:
                # 返回格式为列名直接是字段名，如 "所属行业" 是一个 column
                if "所属行业" in df.columns:
                    val = str(df["所属行业"].iloc[0]).strip()
                    if val and val not in ("nan", "None", ""):
                        return val
                # 兼容旧版 item/value 格式
                elif "item" in df.columns:
                    row = df[df["item"] == "所属行业"]
                    if not row.empty:
                        val = str(row.iloc[0]["value"]).strip()
                        if val and val not in ("nan", "None"):
                            return val

        elif region == "US":
            if not XQ_A_TOKEN:
                print("  [警告] 未提供 --token 参数，跳过美股查询。请通过 --token 传入雪球 xq_a_token")
                return None
            df = ak.stock_individual_basic_info_us_xq(symbol=stock_code, token=XQ_A_TOKEN)
            if df is not None and not df.empty:
                if "item" in df.columns:
                    row = df[df["item"] == "main_operation_business"]
                    if not row.empty:
                        val = str(row.iloc[0]["value"]).strip()
                        if val and val not in ("nan", "None"):
                            return val
                elif "main_operation_business" in df.columns:
                    val = df["main_operation_business"].iloc[0]
                    if val:
                        val = str(val).strip()
                        if val and val not in ("nan", "None"):
                            return val

    except Exception as e:
        return None

    return None


def run_stock_industry():
    """Part B: 个股行业分布抓取。"""
    print("\n" + "=" * 70)
    print("  Part B: 持仓个股行业分布抓取")
    print("=" * 70)

    # Step 6: 加载所有持仓个股
    print("\n[B-1] 加载持仓个股...")
    all_stocks = load_all_stocks()
    print(f"  共加载 {len(all_stocks)} 只个股（去重后）")

    # 按市场分类统计
    cn_stocks = {c: i for c, i in all_stocks.items() if i["region"] == "CN"}
    hk_stocks = {c: i for c, i in all_stocks.items() if i["region"] == "HK"}
    us_stocks = {c: i for c, i in all_stocks.items() if i["region"] == "US"}
    print(f"  A股: {len(cn_stocks)} 只, 港股: {len(hk_stocks)} 只, 美股: {len(us_stocks)} 只")

    # Step 7: 过滤已有缓存的个股
    print("\n[B-2] 检查 SQLite 缓存...")
    to_query = {}
    cached_count = 0
    for code, info in all_stocks.items():
        cached = get_stock_industry(code)
        if cached and cached.get("industry_l1") not in ("未知", "个股", "海外", None, ""):
            cached_count += 1
        else:
            to_query[code] = info
    print(f"  已有缓存: {cached_count} 只, 待查询: {len(to_query)} 只")

    # Step 8: 调用 API 查询
    print(f"\n[B-3] 开始调用 akshare API 查询个股行业（预计耗时 {len(to_query) * API_DELAY:.0f}s）...")
    success_count = 0
    fail_count = 0
    stock_results = {}  # {code: industry_name}

    for i, (code, info) in enumerate(to_query.items(), 1):
        time.sleep(API_DELAY)
        region = info["region"]
        industry_name = query_stock_industry(code, region)

        if industry_name:
            stock_results[code] = industry_name
            success_count += 1
            status = f"✓ {industry_name}"
        else:
            fail_count += 1
            status = "✗ 无数据"

        name_display = info["name"][:16] if info["name"] else code
        print(f"  [{i}/{len(to_query)}] [{region}] {code} {name_display:<18} {status}")

    print(f"\n  抓取完成: 成功 {success_count} 只, 失败/无数据 {fail_count} 只")

    # Step 9: 保存到 SQLite
    print(f"\n[B-4] 写入 SQLite stock_industry 表...")
    saved_count = 0
    for code, industry_name in stock_results.items():
        data_source_map = {
            "CN": "stock_individual_info_em",
            "HK": "stock_hk_company_profile_em",
            "US": "stock_individual_basic_info_us_xq",
        }
        region = to_query[code]["region"]
        save_stock_industry(
            stock_code=code,
            stock_name=to_query[code].get("name", ""),
            industry_l1=industry_name,
            industry_l2=industry_name,
            data_source=data_source_map.get(region, ""),
        )
        saved_count += 1
    print(f"  写入完成: {saved_count} 只个股的行业数据已保存")

    # Step 10: 验证
    print(f"\n[B-5] 从 SQLite 读取验证...")
    verify_ok = 0
    verify_fail = 0
    for code in stock_results:
        db_row = get_stock_industry(code)
        if db_row and db_row.get("industry_l1"):
            verify_ok += 1
        else:
            verify_fail += 1
            print(f"  ⚠ {code} 写入后读取失败!")
    print(f"  验证结果: 成功 {verify_ok}, 失败 {verify_fail}")

    return stock_results


# ──────────────────────────────────────────────
# Part C: 汇总统计
# ──────────────────────────────────────────────

def print_summary(fund_results: dict, stock_results: dict):
    """打印汇总统计信息。"""
    print("\n" + "=" * 70)
    print("  Part C: 汇总统计")
    print("=" * 70)

    # 基金行业汇总
    print(f"\n[C-1] 基金行业分布汇总")
    print("-" * 70)
    industry_summary = {}
    for code, rows in fund_results.items():
        for r in rows:
            name = r["industry_name"]
            if name not in industry_summary:
                industry_summary[name] = {"count": 0, "total_pct": 0.0}
            industry_summary[name]["count"] += 1
            industry_summary[name]["total_pct"] += r["industry_pct"]

    sorted_industries = sorted(industry_summary.items(),
                               key=lambda x: x[1]["count"], reverse=True)
    print(f"  {'行业名称':<16} {'出现基金数':>10} {'平均占比':>10}")
    print(f"  {'-'*16} {'-'*10} {'-'*10}")
    for name, stats in sorted_industries[:20]:
        avg_pct = stats["total_pct"] / stats["count"]
        print(f"  {name:<16} {stats['count']:>10} {avg_pct:>9.2f}%")

    # 个股行业汇总
    print(f"\n[C-2] 个股行业分布汇总")
    print("-" * 70)
    stock_industry_summary = {}
    for code, industry_name in stock_results.items():
        if industry_name not in stock_industry_summary:
            stock_industry_summary[industry_name] = 0
        stock_industry_summary[industry_name] += 1

    sorted_stock_ind = sorted(stock_industry_summary.items(),
                              key=lambda x: x[1], reverse=True)
    print(f"  {'行业名称':<20} {'个股数量':>10}")
    print(f"  {'-'*20} {'-'*10}")
    for name, count in sorted_stock_ind[:20]:
        print(f"  {name:<20} {count:>10}")

    # SQLite 表统计
    print(f"\n[C-3] SQLite 数据库统计")
    print("-" * 70)
    conn = _get_conn(_DB_PATH)

    row = conn.execute("SELECT COUNT(*) FROM fund_industry_allocation").fetchone()
    print(f"  fund_industry_allocation 总记录: {row[0]}")
    row2 = conn.execute(
        "SELECT COUNT(DISTINCT fund_code) FROM fund_industry_allocation").fetchone()
    print(f"  涉及基金: {row2[0]} 只")

    row3 = conn.execute("SELECT COUNT(*) FROM stock_industry").fetchone()
    print(f"  stock_industry 总记录: {row3[0]}")
    # 按 data_source 分组统计
    rows = conn.execute(
        "SELECT data_source, COUNT(*) FROM stock_industry GROUP BY data_source"
    ).fetchall()
    for source, cnt in rows:
        print(f"    - {source or '未知来源'}: {cnt} 条")

    conn.close()


# ──────────────────────────────────────────────
# 主入口
# ──────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  行业分布数据抓取 & SQLite 存储 （基金 + 个股）")
    print("=" * 70)

    # 切换到项目根目录（确保 SQLite 路径正确）
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    # Part A: 基金行业
    fund_results = run_fund_industry()

    # Part B: 个股行业
    stock_results = run_stock_industry()

    # Part C: 汇总
    print_summary(fund_results, stock_results)

    print("\n" + "=" * 70)
    print("  全部完成!")
    print("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="行业分布数据抓取 & SQLite 存储")
    parser.add_argument("--token", type=str, default=None,
                        help="雪球 xq_a_token，用于查询美股行业信息（登录雪球后从浏览器 Cookie 获取）")
    args = parser.parse_args()
    # 需要 global 声明，否则模块级 XQ_A_TOKEN 不会被更新
    XQ_A_TOKEN = args.token  # noqa: F811 - 覆盖模块级变量
    main()
