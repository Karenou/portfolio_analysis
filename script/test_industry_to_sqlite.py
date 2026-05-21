"""测试程序：抓取当前持仓基金的行业分布，并保存到 SQLite。

完整流程：
1. 从所有平台 fund_info.json 加载持仓基金（去重）
2. 过滤掉货币基金和商品基金
3. 调用 akshare fund_portfolio_industry_allocation_em API 获取行业分布
4. 保存到 SQLite fund_industry_allocation 表
5. 从 SQLite 读取验证数据完整性
6. 打印汇总统计
"""

import json
import os
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


def main():
    print("=" * 70)
    print("  持仓基金行业分布抓取 & SQLite 存储 测试程序")
    print("=" * 70)

    # Step 1: 加载所有持仓基金
    print("\n[Step 1] 加载持仓基金...")
    all_funds = load_all_funds()
    print(f"  共加载 {len(all_funds)} 只基金（去重后）")

    # Step 2: 过滤
    target_funds = {
        code: info for code, info in all_funds.items()
        if info["sub_type"] not in SKIP_SUB_TYPES
    }
    skipped = len(all_funds) - len(target_funds)
    print(f"  过滤掉货币/商品基金 {skipped} 只，剩余 {len(target_funds)} 只待查询")

    # 排除场内 ETF（6位以上代码的场内ETF，API不支持直接查行业）
    # 场内ETF代码通常为6位且以51/15/16/52开头
    otc_funds = {}
    etf_onexchange = {}
    for code, info in target_funds.items():
        if len(code) == 6 and code[:2] in ("51", "15", "16", "52"):
            etf_onexchange[code] = info
        else:
            otc_funds[code] = info
    print(f"  场内ETF {len(etf_onexchange)} 只（跳过，API不支持），场外基金 {len(otc_funds)} 只")

    # Step 3: 调用 API 抓取行业分布
    print(f"\n[Step 3] 开始调用 akshare API 获取行业分布（预计耗时 {len(otc_funds) * API_DELAY:.0f}s）...")
    year = str(datetime.now().year)
    success_count = 0
    fail_count = 0
    results = {}  # {code: [industry_rows]}

    for i, (code, info) in enumerate(otc_funds.items(), 1):
        time.sleep(API_DELAY)
        rows = query_fund_industry(code, year)
        status = ""
        if rows:
            results[code] = rows
            success_count += 1
            top = max(rows, key=lambda x: x["industry_pct"])
            status = f"✓ {len(rows)}个行业, TOP={top['industry_name']}({top['industry_pct']:.1f}%)"
        else:
            fail_count += 1
            status = "✗ 无数据"
        # 打印进度
        print(f"  [{i}/{len(otc_funds)}] {code} {info['name'][:20]:<22} {status}")

    print(f"\n  抓取完成: 成功 {success_count} 只, 失败/无数据 {fail_count} 只")

    # Step 4: 保存到 SQLite
    print(f"\n[Step 4] 写入 SQLite ({_DB_PATH})...")
    os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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

    # Step 5: 从 SQLite 读取验证
    print(f"\n[Step 5] 从 SQLite 读取验证...")
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

    # Step 6: 汇总统计
    print(f"\n[Step 6] 行业分布汇总统计")
    print("-" * 70)

    # 统计所有基金涉及的行业
    industry_summary = {}  # {industry_name: [fund_count, total_pct]}
    for code, rows in results.items():
        for r in rows:
            name = r["industry_name"]
            if name not in industry_summary:
                industry_summary[name] = {"count": 0, "total_pct": 0.0}
            industry_summary[name]["count"] += 1
            industry_summary[name]["total_pct"] += r["industry_pct"]

    # 按出现次数排序
    sorted_industries = sorted(industry_summary.items(),
                               key=lambda x: x[1]["count"], reverse=True)
    print(f"  {'行业名称':<16} {'出现基金数':>10} {'平均占比':>10}")
    print(f"  {'-'*16} {'-'*10} {'-'*10}")
    for name, stats in sorted_industries[:20]:
        avg_pct = stats["total_pct"] / stats["count"]
        print(f"  {name:<16} {stats['count']:>10} {avg_pct:>9.2f}%")

    # 打印 SQLite 表中的总记录数
    print(f"\n[统计] SQLite fund_industry_allocation 表总记录数:")
    conn = _get_conn(_DB_PATH)
    row = conn.execute("SELECT COUNT(*) FROM fund_industry_allocation").fetchone()
    print(f"  总记录: {row[0]}")
    row2 = conn.execute(
        "SELECT COUNT(DISTINCT fund_code) FROM fund_industry_allocation").fetchone()
    print(f"  涉及基金: {row2[0]} 只")
    conn.close()

    print("\n" + "=" * 70)
    print("  测试完成!")
    print("=" * 70)


if __name__ == "__main__":
    main()
