"""历史行情数据一键抓取脚本。

功能：
  1. --check: 诊断模式，展示各平台待抓取/已抓取数量
  2. --all: 全量抓取所有平台所有类型
  3. --platform xxx: 只抓指定平台的场外基金
  4. --type xxx: 只抓指定类型 (fund/etf/stock_a/stock_hk/stock_us)

运行方式：
    cd portfolio_analysis
    python -m script.fetch_nav_data --check
    python -m script.fetch_nav_data --all
    python -m script.fetch_nav_data --platform alipay
    python -m script.fetch_nav_data --type etf
"""

import argparse
import logging
import os
import sqlite3
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from analyzers.cache_utils import load_json
from analyzers.fund_nav_db import (
    fetch_and_store_fund_nav,
    fetch_and_store_etf_hist,
    fetch_and_store_stock_hist,
    fetch_and_store_stock_hk_hist,
    fetch_and_store_stock_us_hist,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

_DB_PATH = os.path.join("cache", "fund_nav.db")

# 各平台配置
_FUND_PLATFORMS = ["alipay", "qieman", "snowball"]


# ══════════════════════════════════════════════════════════════
# 诊断模块
# ══════════════════════════════════════════════════════════════

def _get_db_fund_codes() -> set:
    """从数据库获取已有净值数据的基金代码集合。"""
    if not os.path.isfile(_DB_PATH):
        return set()
    conn = sqlite3.connect(_DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT fund_code FROM fund_daily_nav WHERE unit_nav IS NOT NULL"
    ).fetchall()
    conn.close()
    return {r[0] for r in rows}


def _get_db_etf_codes() -> set:
    """从数据库获取已有历史数据的 ETF 代码集合。"""
    if not os.path.isfile(_DB_PATH):
        return set()
    conn = sqlite3.connect(_DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM etf_daily_hist WHERE close IS NOT NULL"
    ).fetchall()
    conn.close()
    return {r[0] for r in rows}


def _get_db_stock_codes() -> set:
    """从数据库获取已有历史数据的股票代码集合。"""
    if not os.path.isfile(_DB_PATH):
        return set()
    conn = sqlite3.connect(_DB_PATH)
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM stock_daily_hist WHERE close IS NOT NULL"
    ).fetchall()
    conn.close()
    return {r[0] for r in rows}


def _get_platform_fund_codes(platform: str) -> dict:
    """读取某平台的 fund_info.json，返回 {code: name} 字典。"""
    path = os.path.join("cache", platform, "fund_info.json")
    data = load_json(path)
    if not data:
        return {}
    # fund_info.json 格式: {"code": {"name": "xxx", "sub_type": "xxx"}}
    return {code: info.get("name", "") for code, info in data.items()}


def _get_etf_codes() -> dict:
    """读取 huatai ETF 列表，返回 {code: name}。"""
    holdings = load_json("cache/huatai/classified_holdings.json")
    if not holdings:
        return {}
    if isinstance(holdings, dict):
        holdings = list(holdings.values())
    return {h["code"]: h.get("name", "") for h in holdings if h.get("sub_type") == "etf"}


def _get_stock_a_codes() -> dict:
    """读取 huatai A股列表。"""
    holdings = load_json("cache/huatai/classified_holdings.json")
    if not holdings:
        return {}
    if isinstance(holdings, dict):
        holdings = list(holdings.values())
    return {h["code"]: h.get("name", "") for h in holdings
            if h.get("sub_type") in ("stock_a", "stock_cn")}


def _get_stock_hk_codes() -> dict:
    """读取 futu 港股列表。"""
    holdings = load_json("cache/futu/classified_holdings.json")
    if not holdings:
        return {}
    if isinstance(holdings, dict):
        holdings = list(holdings.values())
    return {h["code"]: h.get("name", "") for h in holdings if h.get("sub_type") == "stock_hk"}


def _get_stock_us_codes() -> dict:
    """读取 futu 美股列表。"""
    holdings = load_json("cache/futu/classified_holdings.json")
    if not holdings:
        return {}
    if isinstance(holdings, dict):
        holdings = list(holdings.values())
    return {h["code"]: h.get("name", "") for h in holdings if h.get("sub_type") == "stock_us"}


def check_status():
    """诊断模式：展示各平台待抓取/已抓取/缺失数量。"""
    print()
    print("=" * 62)
    print("          历史行情数据抓取状态诊断")
    print("=" * 62)
    print()
    print(f"{'平台':<12}{'类型':<12}{'待抓取':>6}{'已抓取':>8}{'缺失':>6}")
    print("-" * 62)

    db_fund_codes = _get_db_fund_codes()
    db_etf_codes = _get_db_etf_codes()
    db_stock_codes = _get_db_stock_codes()

    total_need = set()
    total_have = set()

    # 各平台场外基金
    for platform in _FUND_PLATFORMS:
        codes = _get_platform_fund_codes(platform)
        need = len(codes)
        have = len(set(codes.keys()) & db_fund_codes)
        miss = need - have
        print(f"{platform:<12}{'fund':<12}{need:>6}{have:>8}{miss:>6}")
        total_need.update(codes.keys())
        total_have.update(set(codes.keys()) & db_fund_codes)

    # ETF
    etf_codes = _get_etf_codes()
    etf_have = len(set(etf_codes.keys()) & db_etf_codes)
    etf_miss = len(etf_codes) - etf_have
    print(f"{'huatai':<12}{'etf':<12}{len(etf_codes):>6}{etf_have:>8}{etf_miss:>6}")
    total_need.update(etf_codes.keys())
    total_have.update(set(etf_codes.keys()) & db_etf_codes)

    # A股
    stock_a_codes = _get_stock_a_codes()
    stock_a_have = len(set(stock_a_codes.keys()) & db_stock_codes)
    stock_a_miss = len(stock_a_codes) - stock_a_have
    print(f"{'huatai':<12}{'stock_a':<12}{len(stock_a_codes):>6}{stock_a_have:>8}{stock_a_miss:>6}")
    total_need.update(stock_a_codes.keys())
    total_have.update(set(stock_a_codes.keys()) & db_stock_codes)

    # 港股
    hk_codes = _get_stock_hk_codes()
    hk_have = len(set(hk_codes.keys()) & db_stock_codes)
    hk_miss = len(hk_codes) - hk_have
    print(f"{'futu':<12}{'stock_hk':<12}{len(hk_codes):>6}{hk_have:>8}{hk_miss:>6}")
    total_need.update(hk_codes.keys())
    total_have.update(set(hk_codes.keys()) & db_stock_codes)

    # 美股
    us_codes = _get_stock_us_codes()
    us_have = len(set(us_codes.keys()) & db_stock_codes)
    us_miss = len(us_codes) - us_have
    print(f"{'futu':<12}{'stock_us':<12}{len(us_codes):>6}{us_have:>8}{us_miss:>6}")
    total_need.update(us_codes.keys())
    total_have.update(set(us_codes.keys()) & db_stock_codes)

    # 汇总
    print("-" * 62)
    coverage = len(total_have) / len(total_need) * 100 if total_need else 0
    print(f"{'合计(去重)':<24}{len(total_need):>6}{len(total_have):>8}"
          f"{len(total_need) - len(total_have):>6}")
    print()
    print(f"数据覆盖率: {coverage:.1f}%")

    if coverage < 80:
        print(f"建议执行: python -m script.fetch_nav_data --all")
        est_minutes = len(total_need) * 1.5 / 60  # 粗估每个资产 1.5s
        print(f"预估耗时: ~{est_minutes:.0f} 分钟")

    print()
    return len(total_need) - len(total_have)


# ══════════════════════════════════════════════════════════════
# 抓取执行模块
# ══════════════════════════════════════════════════════════════

def fetch_all():
    """全量抓取所有平台所有类型。"""
    print("\n开始全量抓取...")
    start_time = time.time()

    # 1) 场外基金
    for platform in _FUND_PLATFORMS:
        print(f"\n>>> 抓取 {platform} 场外基金 NAV...")
        try:
            fetch_and_store_fund_nav(platform)
        except Exception as e:
            logger.error(f"  {platform} 抓取出错: {e}")

    # 2) ETF
    print(f"\n>>> 抓取 huatai ETF 历史行情...")
    try:
        fetch_and_store_etf_hist()
    except Exception as e:
        logger.error(f"  ETF 抓取出错: {e}")

    # 3) A股
    print(f"\n>>> 抓取 huatai A股历史行情...")
    try:
        fetch_and_store_stock_hist()
    except Exception as e:
        logger.error(f"  A股抓取出错: {e}")

    # 4) 港股
    print(f"\n>>> 抓取 futu 港股历史行情...")
    try:
        fetch_and_store_stock_hk_hist()
    except Exception as e:
        logger.error(f"  港股抓取出错: {e}")

    # 5) 美股
    print(f"\n>>> 抓取 futu 美股历史行情...")
    try:
        fetch_and_store_stock_us_hist()
    except Exception as e:
        logger.error(f"  美股抓取出错: {e}")

    elapsed = time.time() - start_time
    print(f"\n全量抓取完成，耗时 {elapsed:.1f} 秒")

    # 抓取后再诊断一次
    print("\n--- 抓取后状态 ---")
    check_status()


def fetch_by_platform(platform: str):
    """只抓取指定平台的场外基金。"""
    if platform not in _FUND_PLATFORMS:
        print(f"不支持的平台: {platform}，可选: {_FUND_PLATFORMS}")
        return
    print(f"\n>>> 抓取 {platform} 场外基金 NAV...")
    fetch_and_store_fund_nav(platform)
    print("完成")


def fetch_by_type(asset_type: str):
    """只抓取指定类型。"""
    type_map = {
        "fund": lambda: [fetch_and_store_fund_nav(p) for p in _FUND_PLATFORMS],
        "etf": fetch_and_store_etf_hist,
        "stock_a": fetch_and_store_stock_hist,
        "stock_hk": fetch_and_store_stock_hk_hist,
        "stock_us": fetch_and_store_stock_us_hist,
    }

    if asset_type not in type_map:
        print(f"不支持的类型: {asset_type}，可选: {list(type_map.keys())}")
        return

    print(f"\n>>> 抓取类型: {asset_type}...")
    func = type_map[asset_type]
    func()
    print("完成")


# ══════════════════════════════════════════════════════════════
# 命令行入口
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="历史行情数据抓取工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python -m script.fetch_nav_data --check          # 只看诊断
  python -m script.fetch_nav_data --all            # 全量抓取
  python -m script.fetch_nav_data --platform qieman  # 只抓且慢基金
  python -m script.fetch_nav_data --type etf       # 只抓ETF
        """)

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--check", action="store_true", help="诊断模式：只展示抓取状态")
    group.add_argument("--all", action="store_true", help="全量抓取所有平台所有类型")
    group.add_argument("--platform", type=str, help="只抓指定平台 (alipay/qieman/snowball)")
    group.add_argument("--type", type=str, help="只抓指定类型 (fund/etf/stock_a/stock_hk/stock_us)")

    args = parser.parse_args()

    # 默认行为：诊断
    if not args.all and not args.platform and not args.type:
        args.check = True

    if args.check:
        check_status()
    elif args.all:
        # 先展示诊断
        missing = check_status()
        if missing == 0:
            print("所有数据已是最新，无需抓取。")
            return
        print(f"\n即将开始抓取 {missing} 个缺失资产的历史行情数据...")
        print("按 Ctrl+C 可随时中断（已抓取的数据会保留）\n")
        fetch_all()
    elif args.platform:
        fetch_by_platform(args.platform)
    elif args.type:
        fetch_by_type(args.type)


if __name__ == "__main__":
    main()
