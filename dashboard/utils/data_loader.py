"""
统一数据加载层
从 cache/ 目录读取 JSON + SQLite 数据，供各 Tab 页面使用
"""

import json
import sqlite3
import os
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd

# 项目根目录和缓存目录
PROJECT_ROOT = Path(__file__).parent.parent.parent
CACHE_DIR = PROJECT_ROOT / "cache"

# 支持的平台列表
PLATFORMS = ["futu", "huatai", "alipay", "qieman", "snowball"]


def _load_json(filepath: Path) -> Optional[Union[dict, list]]:
    """加载 JSON 文件，不存在则返回 None"""
    if filepath.exists():
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def load_indicator_results() -> dict:
    """加载全局指标计算结果"""
    data = _load_json(CACHE_DIR / "indicator_results.json")
    if data is None:
        raise FileNotFoundError("indicator_results.json 不存在")
    return data


def load_classified_holdings() -> pd.DataFrame:
    """
    加载所有平台的 classified_holdings，合并为一个 DataFrame
    增加 platform 列标识来源平台
    """
    all_holdings = []
    for platform in PLATFORMS:
        filepath = CACHE_DIR / platform / "classified_holdings.json"
        holdings = _load_json(filepath)
        if holdings:
            for item in holdings:
                item["platform"] = platform
            all_holdings.extend(holdings)
    
    if not all_holdings:
        return pd.DataFrame()
    
    df = pd.DataFrame(all_holdings)
    return df


def load_penetration_summaries() -> Dict[str, dict]:
    """加载所有平台的 penetration_summary（兼容两种文件名）"""
    summaries = {}
    for platform in PLATFORMS:
        # 优先读 penetration_summary.json，没有则尝试 penetrated_holdings.json
        filepath = CACHE_DIR / platform / "penetration_summary.json"
        data = _load_json(filepath)
        if data is None:
            filepath = CACHE_DIR / platform / "penetrated_holdings.json"
            data = _load_json(filepath)
        if data:
            summaries[platform] = data
    return summaries


def get_total_market_value(summaries: Dict[str, dict]) -> float:
    """从各平台 penetration_summary 计算总市值"""
    total = 0.0
    for platform, data in summaries.items():
        total += data.get("total_market_value_cny", 0)
    return total


def get_level2_aggregated(summaries: Dict[str, dict]) -> Dict[str, float]:
    """聚合所有平台的 level2_summary（equity/bond/commodity/cash）"""
    aggregated = {}
    for platform, data in summaries.items():
        level2 = data.get("level2_summary", {})
        for category, info in level2.items():
            if category not in aggregated:
                aggregated[category] = 0.0
            aggregated[category] += info.get("market_value_cny", 0)
    return aggregated


def get_currency_aggregated(summaries: Dict[str, dict]) -> Dict[str, float]:
    """聚合所有平台的币种敞口（转换为 CNY 等值后的占比）"""
    currency_values = {}
    
    for platform, data in summaries.items():
        curr_summary = data.get("currency_summary", {})
        total_mv = data.get("total_market_value_cny", 0)
        
        if not curr_summary:
            # 没有 currency_summary 的平台默认全部是 CNY
            currency_values["CNY"] = currency_values.get("CNY", 0) + total_mv
            continue
        
        # 计算各币种的原始金额占该平台总额的比例，然后乘以 CNY 总市值
        all_original = {}
        for currency, values in curr_summary.items():
            for cur_code, amount in values.items():
                all_original[currency] = all_original.get(currency, 0) + amount
        
        # 用简单的比例分配 CNY 市值
        total_original = sum(all_original.values()) if all_original else 1
        for currency, amount in all_original.items():
            ratio = amount / total_original if total_original > 0 else 0
            cny_equivalent = total_mv * ratio
            currency_values[currency] = currency_values.get(currency, 0) + cny_equivalent
    
    return currency_values


def get_platform_breakdown(summaries: Dict[str, dict]) -> Dict[str, Dict[str, float]]:
    """获取每个平台的资产大类分布（用于堆叠条形图）"""
    result = {}
    for platform, data in summaries.items():
        level2 = data.get("level2_summary", {})
        result[platform] = {
            cat: info.get("market_value_cny", 0)
            for cat, info in level2.items()
        }
    return result


def get_portfolio_metrics(indicator_data: dict, window: str = "1Y") -> dict:
    """获取组合级别指标"""
    portfolio = indicator_data.get("portfolio_metrics", {})
    return portfolio.get(window, {})


def get_correlation_matrix(indicator_data: dict, window: str = "1Y") -> dict:
    """获取资产大类相关性矩阵"""
    portfolio = indicator_data.get("portfolio_metrics", {})
    window_data = portfolio.get(window, {})
    return window_data.get("correlation_by_asset_class", {})


def get_single_asset_metrics(indicator_data: dict) -> pd.DataFrame:
    """将单资产指标转为 DataFrame，方便排序和展示"""
    metrics = indicator_data.get("single_asset_metrics", {})
    rows = []
    for code, info in metrics.items():
        name = info.get("name", code)
        windows = info.get("windows", {})
        for window, vals in windows.items():
            rows.append({
                "code": code,
                "name": name,
                "window": window,
                "return": vals.get("return"),
                "annualized_return": vals.get("annualized_return"),
                "volatility": vals.get("volatility"),
                "max_drawdown": vals.get("max_drawdown"),
                "sharpe": vals.get("sharpe"),
                "data_points": vals.get("data_points"),
            })
    return pd.DataFrame(rows)


def load_nav_history(code: str) -> Optional[pd.DataFrame]:
    """从 SQLite 加载某只资产的净值历史"""
    db_path = CACHE_DIR / "fund_nav.db"
    if not db_path.exists():
        return None
    
    # 各表的列名映射：(表名, 代码列, 日期列)
    TABLE_CONFIG = [
        ("etf_daily_hist", "symbol", "trade_date"),
        ("fund_daily_nav", "fund_code", "nav_date"),
        ("stock_daily_hist", "symbol", "trade_date"),
    ]
    
    conn = sqlite3.connect(str(db_path))
    try:
        for table, code_col, date_col in TABLE_CONFIG:
            try:
                df = pd.read_sql_query(
                    f"SELECT * FROM {table} WHERE {code_col} = ? ORDER BY {date_col}",
                    conn,
                    params=[code]
                )
                if not df.empty:
                    return df
            except Exception:
                continue
        return None
    finally:
        conn.close()


def get_db_tables() -> List[str]:
    """获取 SQLite 数据库中的所有表名"""
    db_path = CACHE_DIR / "fund_nav.db"
    if not db_path.exists():
        return []
    
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()
