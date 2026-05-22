"""指标计算模块 - 独立于 fund_nav_db.py 的量化指标计算层。

分两层计算：
  Layer 1: 单资产指标（收益率、波动率、最大回撤、夏普比率、累计净值曲线）
  Layer 2: Portfolio 整体指标（加权收益率、加权波动率、相关性矩阵）

所有函数统一支持灵活的 start_date / end_date 参数，方便可视化看板的时间滑块消费。
"""

import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from analyzers.cache_utils import load_json, save_json

logger = logging.getLogger(__name__)

_DB_PATH = os.path.join("cache", "fund_nav.db")

# ══════════════════════════════════════════════════════════════
# 时间窗口工具
# ══════════════════════════════════════════════════════════════

# 预设窗口：标签 → 往前推的天数
_WINDOW_DAYS = {
    "1M": 30,
    "3M": 90,
    "6M": 180,
    "1Y": 365,
    "3Y": 365 * 3,
    "5Y": 365 * 5,
    "10Y": 365 * 10
}


def resolve_window(window_label: str) -> tuple[str, str]:
    """将窗口标签转换为 (start_date, end_date) 日期字符串对。

    Args:
        window_label: 预设窗口，如 "1M", "3M", "6M", "1Y", "3Y", "5Y"

    Returns:
        (start_date, end_date)，格式 "YYYY-MM-DD"
    """
    days = _WINDOW_DAYS.get(window_label.upper())
    if days is None:
        raise ValueError(f"不支持的窗口标签: {window_label}，可用: {list(_WINDOW_DAYS.keys())}")
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    return start_date, end_date


# ══════════════════════════════════════════════════════════════
# 数据读取层（统一入口）
# ══════════════════════════════════════════════════════════════

def _get_conn(db_path: str = _DB_PATH) -> sqlite3.Connection:
    """获取 SQLite 连接。"""
    return sqlite3.connect(db_path)


def get_price_series(code: str, start_date: str, end_date: str,
                     db_path: str = _DB_PATH) -> Optional[pd.Series]:
    """统一的价格/净值序列读取入口，自动判断资产类型。

    查找顺序：fund_daily_nav → etf_daily_hist → stock_daily_hist

    Args:
        code: 资产代码
        start_date: 起始日期 "YYYY-MM-DD"
        end_date: 结束日期 "YYYY-MM-DD"
        db_path: 数据库路径

    Returns:
        pd.Series，DatetimeIndex，值为价格/净值；数据不足时返回 None
    """
    conn = _get_conn(db_path)
    series = None

    # 1) 尝试 fund_daily_nav
    series = _read_nav_series_by_date(conn, code, start_date, end_date)
    if series is not None:
        conn.close()
        return series

    # 2) 尝试 etf_daily_hist
    series = _read_close_series_by_date(conn, "etf_daily_hist", code, start_date, end_date)
    if series is not None:
        conn.close()
        return series

    # 3) 尝试 stock_daily_hist
    series = _read_close_series_by_date(conn, "stock_daily_hist", code, start_date, end_date)
    conn.close()
    return series


def _read_nav_series_by_date(conn: sqlite3.Connection, fund_code: str,
                             start_date: str, end_date: str) -> Optional[pd.Series]:
    """按日期范围读取基金净值序列。"""
    rows = conn.execute("""
        SELECT nav_date, unit_nav FROM fund_daily_nav
        WHERE fund_code = ? AND nav_date >= ? AND nav_date <= ?
              AND unit_nav IS NOT NULL
        ORDER BY nav_date
    """, (fund_code, start_date, end_date)).fetchall()

    if not rows or len(rows) < 2:
        return None
    dates = [r[0] for r in rows]
    values = [r[1] for r in rows]
    return pd.Series(values, index=pd.to_datetime(dates), name=fund_code)


def _read_close_series_by_date(conn: sqlite3.Connection, table: str,
                               symbol: str, start_date: str,
                               end_date: str) -> Optional[pd.Series]:
    """按日期范围读取 ETF/股票收盘价序列。"""
    rows = conn.execute(f"""
        SELECT trade_date, close FROM {table}
        WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
              AND close IS NOT NULL
        ORDER BY trade_date
    """, (symbol, start_date, end_date)).fetchall()

    if not rows or len(rows) < 2:
        return None
    dates = [r[0] for r in rows]
    values = [r[1] for r in rows]
    return pd.Series(values, index=pd.to_datetime(dates), name=symbol)


# ══════════════════════════════════════════════════════════════
# Layer 1: 单资产指标
# ══════════════════════════════════════════════════════════════

def calc_return(code: str, start_date: str, end_date: str,
                db_path: str = _DB_PATH) -> Optional[dict]:
    """计算单资产的区间收益率和年化收益率。

    Returns:
        {"return": 区间收益率, "annualized_return": 年化收益率} 或 None
    """
    series = get_price_series(code, start_date, end_date, db_path)
    if series is None or len(series) < 2:
        return None

    start_val = series.iloc[0]
    end_val = series.iloc[-1]
    if start_val <= 0:
        return None

    # 区间收益率
    period_return = end_val / start_val - 1
    # 年化收益率
    trading_days = len(series)
    annualized = (end_val / start_val) ** (250 / trading_days) - 1

    return {
        "return": round(float(period_return), 6),
        "annualized_return": round(float(annualized), 6),
    }


def calc_volatility(code: str, start_date: str, end_date: str,
                    db_path: str = _DB_PATH) -> Optional[float]:
    """计算单资产的年化波动率。

    公式：日收益率标准差 × √252

    Returns:
        年化波动率（float）或 None
    """
    series = get_price_series(code, start_date, end_date, db_path)
    if series is None or len(series) < 20:
        return None

    daily_returns = series.pct_change().dropna()
    if len(daily_returns) < 10:
        return None

    vol = float(daily_returns.std() * np.sqrt(252))
    return round(vol, 6)


def calc_max_drawdown(code: str, start_date: str, end_date: str,
                      db_path: str = _DB_PATH) -> Optional[float]:
    """计算单资产的最大回撤。

    公式：min((price - cummax) / cummax)

    Returns:
        最大回撤（负数）或 None
    """
    series = get_price_series(code, start_date, end_date, db_path)
    if series is None or len(series) < 2:
        return None

    cummax = series.cummax()
    drawdown = (series - cummax) / cummax
    mdd = float(drawdown.min())
    return round(mdd, 6)


def calc_sharpe(code: str, start_date: str, end_date: str,
                risk_free_rate: float = 0.02,
                db_path: str = _DB_PATH) -> Optional[float]:
    """计算单资产的夏普比率。

    公式：(年化收益率 - 无风险利率) / 年化波动率

    Args:
        code: 资产代码
        start_date/end_date: 日期范围
        risk_free_rate: 无风险利率（年化），默认 2%

    Returns:
        夏普比率（float）或 None
    """
    ret_info = calc_return(code, start_date, end_date, db_path)
    vol = calc_volatility(code, start_date, end_date, db_path)

    if ret_info is None or vol is None or vol == 0:
        return None

    sharpe = (ret_info["annualized_return"] - risk_free_rate) / vol
    return round(float(sharpe), 4)


def calc_return_series(code: str, start_date: str, end_date: str,
                       db_path: str = _DB_PATH) -> Optional[dict]:
    """计算累计收益率序列（净值曲线），供前端画折线图用。

    公式：每日 price / price[0] - 1

    Returns:
        {"dates": [...], "values": [...]} 或 None
    """
    series = get_price_series(code, start_date, end_date, db_path)
    if series is None or len(series) < 2:
        return None

    base_val = series.iloc[0]
    if base_val <= 0:
        return None

    cum_return = (series / base_val - 1)
    return {
        "dates": [d.strftime("%Y-%m-%d") for d in cum_return.index],
        "values": [round(float(v), 6) for v in cum_return.values],
    }


def calc_all_single(code: str, start_date: str, end_date: str,
                    risk_free_rate: float = 0.02,
                    db_path: str = _DB_PATH) -> Optional[dict]:
    """一次性计算单资产的所有指标。

    Returns:
        包含 return, annualized_return, volatility, max_drawdown, sharpe 的 dict
    """
    series = get_price_series(code, start_date, end_date, db_path)
    if series is None or len(series) < 2:
        return None

    start_val = series.iloc[0]
    end_val = series.iloc[-1]
    if start_val <= 0:
        return None

    # 收益率
    period_return = end_val / start_val - 1
    trading_days = len(series)
    annualized_return = (end_val / start_val) ** (250 / trading_days) - 1

    # 波动率
    daily_returns = series.pct_change().dropna()
    volatility = None
    if len(daily_returns) >= 10:
        volatility = float(daily_returns.std() * np.sqrt(252))

    # 最大回撤
    cummax = series.cummax()
    drawdown = (series - cummax) / cummax
    max_drawdown = float(drawdown.min())

    # 夏普比率
    sharpe = None
    if volatility and volatility > 0:
        sharpe = (annualized_return - risk_free_rate) / volatility

    return {
        "return": round(float(period_return), 6),
        "annualized_return": round(float(annualized_return), 6),
        "volatility": round(volatility, 6) if volatility else None,
        "max_drawdown": round(max_drawdown, 6),
        "sharpe": round(float(sharpe), 4) if sharpe else None,
        "data_points": trading_days,
    }


# ══════════════════════════════════════════════════════════════
# Layer 2: Portfolio 整体指标
# ══════════════════════════════════════════════════════════════

def calc_portfolio_return(holdings: list[dict], start_date: str, end_date: str,
                          db_path: str = _DB_PATH) -> Optional[float]:
    """计算 Portfolio 加权收益率。

    公式：Σ(单资产收益率 × 权重)

    Args:
        holdings: 持仓列表，每项需含 "code" 和 "weight"（市值占比 0~1）
        start_date/end_date: 日期范围

    Returns:
        加权收益率（float）或 None
    """
    weighted_sum = 0.0
    total_weight = 0.0

    for h in holdings:
        code = h.get("code", "")
        weight = h.get("weight", 0)
        if not code or weight <= 0:
            continue

        ret_info = calc_return(code, start_date, end_date, db_path)
        if ret_info is not None:
            weighted_sum += ret_info["annualized_return"] * weight
            total_weight += weight

    if total_weight == 0:
        return None

    # 归一化（如果部分资产没数据，权重不满 1）
    return round(weighted_sum / total_weight, 6)


def calc_portfolio_volatility(holdings: list[dict], start_date: str, end_date: str,
                              db_path: str = _DB_PATH) -> Optional[float]:
    """计算 Portfolio 加权波动率。

    优先用协方差矩阵法：√(w^T · Σ · w)
    如果数据不足，降级为简单加权：Σ(单资产波动率 × 权重)

    Args:
        holdings: 持仓列表，每项需含 "code" 和 "weight"

    Returns:
        Portfolio 年化波动率（float）或 None
    """
    # 收集每个资产的日收益率序列
    return_series_map = {}
    weights_map = {}

    for h in holdings:
        code = h.get("code", "")
        weight = h.get("weight", 0)
        if not code or weight <= 0:
            continue

        series = get_price_series(code, start_date, end_date, db_path)
        if series is not None and len(series) >= 20:
            daily_ret = series.pct_change().dropna()
            if len(daily_ret) >= 10:
                return_series_map[code] = daily_ret
                weights_map[code] = weight

    if len(return_series_map) < 2:
        # 降级为简单加权
        return _calc_simple_weighted_vol(holdings, start_date, end_date, db_path)

    # 构造 DataFrame，对齐日期
    df = pd.DataFrame(return_series_map)
    df = df.dropna()

    if len(df) < 20:
        return _calc_simple_weighted_vol(holdings, start_date, end_date, db_path)

    # 构造权重向量（归一化）
    codes = list(df.columns)
    raw_weights = np.array([weights_map[c] for c in codes])
    w = raw_weights / raw_weights.sum()

    # 协方差矩阵（年化）
    cov_matrix = df.cov() * 252
    # 检查协方差矩阵是否包含异常值
    if cov_matrix.isnull().any().any() or np.isinf(cov_matrix.values).any():
        return _calc_simple_weighted_vol(holdings, start_date, end_date, db_path)

    # Portfolio 波动率 = √(w^T · Σ · w)
    port_var = float(w @ cov_matrix.values @ w)
    if np.isnan(port_var) or np.isinf(port_var) or port_var < 0:
        return _calc_simple_weighted_vol(holdings, start_date, end_date, db_path)
    port_vol = np.sqrt(port_var)

    return round(float(port_vol), 6)


def _calc_simple_weighted_vol(holdings: list[dict], start_date: str, end_date: str,
                              db_path: str = _DB_PATH) -> Optional[float]:
    """降级版：简单加权波动率 = Σ(单资产波动率 × 权重)。"""
    weighted_sum = 0.0
    total_weight = 0.0

    for h in holdings:
        code = h.get("code", "")
        weight = h.get("weight", 0)
        if not code or weight <= 0:
            continue

        vol = calc_volatility(code, start_date, end_date, db_path)
        if vol is not None:
            weighted_sum += vol * weight
            total_weight += weight

    if total_weight == 0:
        return None
    return round(weighted_sum / total_weight, 6)


def calc_correlation_custom(codes: list[str], start_date: str, end_date: str,
                            db_path: str = _DB_PATH) -> Optional[dict]:
    """用户自选持仓的相关性矩阵。

    Args:
        codes: 资产代码列表，如 ["000001", "161725", "00700"]
        start_date/end_date: 日期范围

    Returns:
        {"labels": [...], "matrix": [[...]]} 或 None
    """
    return_series_map = {}

    for code in codes:
        series = get_price_series(code, start_date, end_date, db_path)
        if series is not None and len(series) >= 20:
            daily_ret = series.pct_change().dropna()
            if len(daily_ret) >= 10:
                return_series_map[code] = daily_ret

    if len(return_series_map) < 2:
        return None

    df = pd.DataFrame(return_series_map)
    df = df.dropna()

    if len(df) < 20:
        return None

    corr = df.corr(method="pearson")
    labels = list(corr.columns)
    matrix = [[round(float(v), 4) for v in row] for row in corr.values]

    return {
        "labels": labels,
        "matrix": matrix,
        "data_points": len(df),
    }


def calc_correlation_by_asset_class(holdings: list[dict], start_date: str, end_date: str,
                                    db_path: str = _DB_PATH) -> Optional[dict]:
    """按大类资产（股/债/商）计算相关性矩阵。

    做法：将每类资产下的持仓按权重合成一条加权日收益率序列，然后计算类间相关系数。

    Args:
        holdings: 持仓列表，每项需含 "code", "weight", "true_asset_class"

    Returns:
        {"labels": ["equity","bond","commodity"], "matrix": [[...]]} 或 None
    """
    # 按大类分组
    class_groups = {"equity": [], "bond": [], "commodity": []}
    for h in holdings:
        ac = h.get("true_asset_class", "")
        if ac in class_groups:
            class_groups[ac].append(h)

    # 为每个大类构建加权日收益率序列
    class_return_series = {}
    for ac, group in class_groups.items():
        if not group:
            continue
        weighted_series = _build_weighted_return_series(group, start_date, end_date, db_path)
        if weighted_series is not None:
            class_return_series[ac] = weighted_series

    if len(class_return_series) < 2:
        return None

    df = pd.DataFrame(class_return_series)
    df = df.dropna()

    if len(df) < 20:
        return None

    corr = df.corr(method="pearson")
    labels = list(corr.columns)
    matrix = [[round(float(v), 4) for v in row] for row in corr.values]

    return {
        "labels": labels,
        "matrix": matrix,
        "data_points": len(df),
    }


def _build_weighted_return_series(holdings: list[dict], start_date: str, end_date: str,
                                  db_path: str = _DB_PATH) -> Optional[pd.Series]:
    """为一组持仓构建加权日收益率序列。"""
    return_series_list = []
    weight_list = []

    for h in holdings:
        code = h.get("code", "")
        weight = h.get("weight", h.get("market_value_cny", 0))
        if not code or weight <= 0:
            continue

        series = get_price_series(code, start_date, end_date, db_path)
        if series is not None and len(series) >= 20:
            daily_ret = series.pct_change().dropna()
            if len(daily_ret) >= 10:
                return_series_list.append(daily_ret)
                weight_list.append(weight)

    if not return_series_list:
        return None

    # 对齐日期，加权合成
    df = pd.concat(return_series_list, axis=1)
    df = df.dropna()

    if len(df) < 20:
        return None

    weights = np.array(weight_list)
    weights = weights / weights.sum()

    weighted_return = (df.values * weights).sum(axis=1)
    return pd.Series(weighted_return, index=df.index)


def calc_all_portfolio(holdings: list[dict], start_date: str, end_date: str,
                       risk_free_rate: float = 0.02,
                       db_path: str = _DB_PATH) -> Optional[dict]:
    """一次性计算所有 Portfolio 整体指标。

    Args:
        holdings: 持仓列表，每项需含 "code", "weight", "true_asset_class"

    Returns:
        包含加权收益率、加权波动率、Portfolio 夏普、大类相关性的 dict
    """
    port_return = calc_portfolio_return(holdings, start_date, end_date, db_path)
    port_vol = calc_portfolio_volatility(holdings, start_date, end_date, db_path)
    corr_class = calc_correlation_by_asset_class(holdings, start_date, end_date, db_path)

    # Portfolio 夏普
    port_sharpe = None
    if port_return is not None and port_vol is not None and port_vol > 0:
        port_sharpe = round((port_return - risk_free_rate) / port_vol, 4)

    return {
        "weighted_return": port_return,
        "weighted_volatility": port_vol,
        "portfolio_sharpe": port_sharpe,
        "correlation_by_asset_class": corr_class,
    }


# ══════════════════════════════════════════════════════════════
# 主入口：全量计算 + 保存 JSON
# ══════════════════════════════════════════════════════════════

def _load_holdings_with_weights(cache_dir: str = "cache") -> list[dict]:
    """从各平台的 penetrated_holdings.json 和 classified_holdings.json 加载持仓并计算权重。

    数据来源：
      1. penetrated_holdings.json — 场外基金穿透后的底层持仓
      2. classified_holdings.json — 个股、场内ETF等直接持仓（futu、huatai等券商平台）

    Returns:
        持仓列表，每项含 code, name, weight, true_asset_class, market_value_cny
    """
    all_details = []
    total_mv = 0.0

    # 扫描各 app 子目录
    if not os.path.isdir(cache_dir):
        return []

    for entry in os.listdir(cache_dir):
        subdir = os.path.join(cache_dir, entry)
        if not os.path.isdir(subdir):
            continue

        # 来源1: penetrated_holdings.json（场外基金穿透）
        ph_path = os.path.join(subdir, "penetrated_holdings.json")
        if os.path.isfile(ph_path):
            data = load_json(ph_path)
            details = data.get("penetrated_details", [])
            all_details.extend(details)
            total_mv += data.get("total_market_value_cny", 0)

        # 来源2: classified_holdings.json（个股/场内ETF等直接持仓）
        ch_path = os.path.join(subdir, "classified_holdings.json")
        if os.path.isfile(ch_path):
            classified = load_json(ch_path)
            if isinstance(classified, list):
                for item in classified:
                    mv = item.get("market_value_cny", 0)
                    # 映射 asset_class → true_asset_class
                    asset_class = item.get("asset_class", "other")
                    sub_type = item.get("sub_type", "")
                    true_ac = _map_asset_class(asset_class, sub_type, item.get("level2_allocation", {}))
                    all_details.append({
                        "code": item.get("code", ""),
                        "name": item.get("name", ""),
                        "market_value_cny": mv,
                        "true_asset_class": true_ac,
                    })
                    total_mv += mv

    if total_mv <= 0 or not all_details:
        return []

    # 合并同一 code 的持仓（跨平台去重求和）
    def _strip_part_suffix(name: str) -> str:
        """去掉穿透产生的后缀，如 ' (股票部分)' ' (债券部分)' 等"""
        return re.sub(r'\s*\((股票|债券|现金|商品|其他)部分\)\s*$', '', name)

    merged = {}
    for d in all_details:
        code = d.get("code", "")
        if not code:
            continue
        if code in merged:
            merged[code]["market_value_cny"] += d.get("market_value_cny", 0)
        else:
            merged[code] = {
                "code": code,
                "name": _strip_part_suffix(d.get("name", "")),
                "true_asset_class": d.get("true_asset_class", "other"),
                "market_value_cny": d.get("market_value_cny", 0),
            }

    # 计算权重
    holdings = []
    for info in merged.values():
        weight = info["market_value_cny"] / total_mv if total_mv > 0 else 0
        info["weight"] = weight
        holdings.append(info)

    return holdings


def _map_asset_class(asset_class: str, sub_type: str, level2: dict) -> str:
    """将 classified_holdings 的 asset_class/sub_type 映射为统一的 true_asset_class。

    映射规则：
      - equity / stock_* → "equity"
      - bond / bond_fund → "bond"
      - commodity_fund / commodity → "commodity"
      - fund + level2_allocation 判断主要成分
      - 其余 → "other"
    """
    if asset_class == "equity":
        return "equity"
    if asset_class == "bond":
        return "bond"
    if asset_class == "commodity":
        return "commodity"

    # fund 类需看 sub_type 或 level2_allocation
    if sub_type in ("bond_fund",):
        return "bond"
    if sub_type in ("commodity_fund",):
        return "commodity"
    if sub_type in ("money_fund",):
        return "other"

    # 通过 level2_allocation 判断
    if level2:
        equity_pct = level2.get("equity_pct", 0)
        bond_pct = level2.get("bond_pct", 0)
        commodity_pct = level2.get("commodity_pct", 0)
        cash_pct = level2.get("cash_pct", 0)
        max_pct = max(equity_pct, bond_pct, commodity_pct, cash_pct)
        if max_pct == cash_pct and cash_pct >= 0.8:
            return "other"
        if max_pct == equity_pct:
            return "equity"
        if max_pct == bond_pct:
            return "bond"
        if max_pct == commodity_pct:
            return "commodity"

    # ETF 默认归为 equity
    if sub_type == "etf":
        return "equity"

    return "other"


def compute_all_indicators(config: dict, windows: list[str] = None,
                           db_path: str = _DB_PATH) -> dict:
    """全量计算所有指标并保存到 cache/indicator_results.json。

    Args:
        config: 配置字典（来自 config.yaml）
        windows: 要计算的时间窗口列表，默认 ["1M", "3M", "6M", "1Y", "3Y", "5Y"]
        db_path: 数据库路径

    Returns:
        完整的指标计算结果 dict
    """
    if windows is None:
        windows = ["1M", "3M", "6M", "1Y", "3Y", "5Y"]

    cache_dir = config.get("paths", {}).get("cache_dir", "cache")
    risk_free_rate = config.get("risk_free_rate", 0.02)

    # 加载持仓
    holdings = _load_holdings_with_weights(cache_dir)
    if not holdings:
        logger.warning("无法加载持仓数据，跳过指标计算")
        return {}

    logger.info(f"开始计算指标: {len(holdings)} 个资产, 窗口 {windows}")

    # ── 单资产指标 ──
    single_asset_metrics = {}
    for h in holdings:
        code = h["code"]
        name = h.get("name", "")
        asset_metrics = {"name": name, "windows": {}}

        for w in windows:
            try:
                start_date, end_date = resolve_window(w)
                result = calc_all_single(code, start_date, end_date, risk_free_rate, db_path)
                if result:
                    asset_metrics["windows"][w] = result
            except Exception as e:
                logger.debug(f"  {code} 窗口 {w} 计算失败: {e}")

        # 只保留有数据的资产
        if asset_metrics["windows"]:
            single_asset_metrics[code] = asset_metrics

    logger.info(f"单资产指标: {len(single_asset_metrics)} 个资产有效")

    # ── Portfolio 整体指标 ──
    portfolio_metrics = {}
    for w in windows:
        try:
            start_date, end_date = resolve_window(w)
            port_result = calc_all_portfolio(holdings, start_date, end_date,
                                            risk_free_rate, db_path)
            if port_result:
                portfolio_metrics[w] = port_result
        except Exception as e:
            logger.debug(f"  Portfolio 窗口 {w} 计算失败: {e}")

    # ── 大类资产相关性（用 1Y 窗口）──
    corr_by_class = None
    try:
        start_1y, end_1y = resolve_window("1Y")
        corr_by_class = calc_correlation_by_asset_class(holdings, start_1y, end_1y, db_path)
    except Exception as e:
        logger.debug(f"  大类资产相关性计算失败: {e}")

    # 组装最终结果
    result = {
        "computed_at": datetime.now().isoformat(),
        "asset_count": len(holdings),
        "windows": windows,
        "risk_free_rate": risk_free_rate,
        "single_asset_metrics": single_asset_metrics,
        "portfolio_metrics": portfolio_metrics,
        "correlation_by_asset_class": corr_by_class,
    }

    # 保存到 JSON
    out_path = os.path.join(cache_dir, "indicator_results.json")
    save_json(result, out_path)
    logger.info(f"指标计算完成，已保存到 {out_path}")

    return result
