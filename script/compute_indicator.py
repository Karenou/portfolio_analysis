"""指标计算脚本 - 计算所有持仓的量化指标并保存到 JSON + SQLite。

功能：
  1. 计算单资产指标（收益率、波动率、最大回撤、夏普比率）
  2. 计算 Portfolio 整体指标（加权收益率、加权波动率）
  3. 计算大类资产相关性矩阵
  4. 结果保存到 cache/indicator_results.json
  5. 结果同步保存到 SQLite (fund_nav.db) indicator_results 表

运行方式：
    cd portfolio_analysis
    python -m script.compute_indicator
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import yaml

from analyzers.indicators import (
    resolve_window,
    get_price_series,
    calc_return,
    calc_volatility,
    calc_max_drawdown,
    calc_sharpe,
    calc_return_series,
    calc_all_single,
    calc_portfolio_return,
    calc_portfolio_volatility,
    calc_correlation_custom,
    calc_correlation_by_asset_class,
    compute_all_indicators,
    _load_holdings_with_weights,
)
from analyzers.fund_nav_db import save_indicator_results_to_db

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def test_resolve_window():
    """测试时间窗口解析。"""
    print("\n" + "=" * 60)
    print("测试 1: resolve_window()")
    print("=" * 60)

    for label in ["1M", "3M", "6M", "1Y", "3Y", "5Y"]:
        start, end = resolve_window(label)
        print(f"  {label}: {start} ~ {end}")

    # 测试无效标签
    try:
        resolve_window("2W")
        print("  ❌ 应该抛出 ValueError")
    except ValueError as e:
        print(f"  ✓ 无效标签正确报错: {e}")


def test_get_price_series():
    """测试价格序列读取（需要有数据库数据）。"""
    print("\n" + "=" * 60)
    print("测试 2: get_price_series()")
    print("=" * 60)

    start, end = resolve_window("1Y")

    # 从数据库读几个已知的代码
    holdings = _load_holdings_with_weights()
    if not holdings:
        print("  ⚠️ 无法加载持仓数据，跳过")
        return []

    # 取前 5 个代码试试
    test_codes = [h["code"] for h in holdings[:5]]
    valid_codes = []

    for code in test_codes:
        series = get_price_series(code, start, end)
        if series is not None:
            print(f"  ✓ {code}: {len(series)} 条数据, "
                  f"范围 {series.index[0].date()} ~ {series.index[-1].date()}, "
                  f"首 {series.iloc[0]:.4f} → 末 {series.iloc[-1]:.4f}")
            valid_codes.append(code)
        else:
            print(f"  - {code}: 无数据")

    return valid_codes


def test_single_asset_metrics(codes: list[str]):
    """测试单资产指标。"""
    print("\n" + "=" * 60)
    print("测试 3: 单资产指标（calc_all_single）")
    print("=" * 60)

    if not codes:
        print("  ⚠️ 无有效代码，跳过")
        return

    start, end = resolve_window("1Y")
    for code in codes[:3]:
        result = calc_all_single(code, start, end)
        if result:
            print(f"\n  [{code}] 1Y 指标:")
            print(f"    区间收益率:   {result['return']:.2%}")
            print(f"    年化收益率:   {result['annualized_return']:.2%}")
            print(f"    年化波动率:   {result['volatility']:.2%}" if result['volatility'] else "    年化波动率: N/A")
            print(f"    最大回撤:     {result['max_drawdown']:.2%}")
            print(f"    夏普比率:     {result['sharpe']:.4f}" if result['sharpe'] else "    夏普比率: N/A")
            print(f"    数据点数:     {result['data_points']}")


def test_portfolio_metrics():
    """测试 Portfolio 整体指标。"""
    print("\n" + "=" * 60)
    print("测试 4: Portfolio 整体指标")
    print("=" * 60)

    holdings = _load_holdings_with_weights()
    if not holdings:
        print("  ⚠️ 无法加载持仓数据，跳过")
        return

    print(f"  加载了 {len(holdings)} 个持仓")

    start, end = resolve_window("1Y")

    # 加权收益率
    port_ret = calc_portfolio_return(holdings, start, end)
    print(f"  Portfolio 加权收益率 (1Y): {port_ret:.2%}" if port_ret else "  Portfolio 加权收益率: N/A")

    # 加权波动率
    port_vol = calc_portfolio_volatility(holdings, start, end)
    print(f"  Portfolio 加权波动率 (1Y): {port_vol:.2%}" if port_vol else "  Portfolio 加权波动率: N/A")

    # Portfolio 夏普
    if port_ret and port_vol and port_vol > 0:
        sharpe = (port_ret - 0.02) / port_vol
        print(f"  Portfolio 夏普比率 (1Y): {sharpe:.4f}")


def test_correlation():
    """测试相关性矩阵。"""
    print("\n" + "=" * 60)
    print("测试 5: 相关性矩阵")
    print("=" * 60)

    holdings = _load_holdings_with_weights()
    if not holdings:
        print("  ⚠️ 无法加载持仓数据，跳过")
        return

    start, end = resolve_window("1Y")

    # 模式 A: 用户自选（取前 10 个有数据的代码）
    codes = [h["code"] for h in holdings[:10]]
    corr = calc_correlation_custom(codes, start, end)
    if corr:
        print(f"\n  模式 A（自选持仓）: {len(corr['labels'])} 个资产")
        print(f"    资产: {corr['labels']}")
        print(f"    数据点: {corr['data_points']}")
        for i, row in enumerate(corr["matrix"]):
            print(f"    {corr['labels'][i]:>8}: {['%.2f' % v for v in row]}")
    else:
        print("  模式 A: 数据不足")

    # 模式 B: 大类资产相关性
    corr_class = calc_correlation_by_asset_class(holdings, start, end)
    if corr_class:
        print(f"\n  模式 B（大类资产）: {corr_class['labels']}")
        print(f"    数据点: {corr_class['data_points']}")
        for i, row in enumerate(corr_class["matrix"]):
            print(f"    {corr_class['labels'][i]:>10}: {['%.2f' % v for v in row]}")
    else:
        print("  模式 B: 数据不足")


def compute_and_save(windows: list[str] = ["1M", "1Y"]):
    """全量计算指标并保存到 JSON + SQLite。"""
    print("\n" + "=" * 60)
    print("全量计算: compute_all_indicators() + 保存到 SQLite")
    print("=" * 60)

    config_path = "config.yaml"
    if not os.path.isfile(config_path):
        print("  ⚠️ config.yaml 不存在，跳过")
        return

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # 计算所有窗口的指标（保存到 JSON）
    result = compute_all_indicators(config, windows=windows)

    if result:
        print(f"  计算时间: {result['computed_at']}")
        print(f"  资产总数: {result['asset_count']}")
        print(f"  有效单资产: {len(result['single_asset_metrics'])}")
        print(f"  Portfolio 窗口: {list(result['portfolio_metrics'].keys())}")
        if result.get("correlation_by_asset_class"):
            print(f"  大类相关性: {result['correlation_by_asset_class']['labels']}")

        # 同步保存到 SQLite
        save_indicator_results_to_db(result)
        print(f"  ✓ 指标结果已同步保存到 SQLite")
    else:
        print("  ⚠️ 计算结果为空")


if __name__ == "__main__":
    print("╔══════════════════════════════════════════════════════════╗")
    print("║        indicators.py 指标计算模块                        ║")
    print("╚══════════════════════════════════════════════════════════╝")

    test_resolve_window()
    valid_codes = test_get_price_series()
    test_single_asset_metrics(valid_codes)
    test_portfolio_metrics()
    test_correlation()

    windows =  ["1M", "3M", "6M", "1Y", "5Y", "10Y"]
    compute_and_save(windows=windows)

    print("\n" + "=" * 60)
    print("全部完成!")
    print("=" * 60)
