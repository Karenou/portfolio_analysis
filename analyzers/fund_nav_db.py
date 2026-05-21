"""SQLite database module for fund NAV, ETF/stock history, and industry data.

Manages all SQLite operations for:
  - fund_info: Fund basic info
  - fund_daily_nav: Daily NAV for OTC funds (场外基金)
  - etf_daily_hist: Daily price history for on-exchange ETFs (场内基金)
  - stock_daily_hist: Daily price history for A-share stocks
  - fund_industry_allocation: Fund industry allocation
  - stock_industry: Stock industry classification

Database file: cache/fund_nav.db
"""

import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Optional

import akshare as ak
import numpy as np
import pandas as pd

from analyzers.cache_utils import load_json

logger = logging.getLogger(__name__)

_API_DELAY = 0.3
_DB_PATH = os.path.join("cache", "fund_nav.db")

# ──────────────────────────────────────────────
# Database initialisation
# ──────────────────────────────────────────────

_CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS fund_info (
    fund_code    TEXT PRIMARY KEY,
    fund_name    TEXT,
    fund_type    TEXT,
    source_platforms TEXT,
    nav_start_date TEXT,
    nav_end_date   TEXT,
    total_records  INTEGER DEFAULT 0,
    last_updated   TEXT
);

CREATE TABLE IF NOT EXISTS fund_daily_nav (
    fund_code    TEXT NOT NULL,
    nav_date     TEXT NOT NULL,
    unit_nav     REAL,
    acc_nav      REAL,
    daily_return REAL,
    PRIMARY KEY (fund_code, nav_date)
);
CREATE INDEX IF NOT EXISTS idx_nav_code ON fund_daily_nav(fund_code);
CREATE INDEX IF NOT EXISTS idx_nav_date ON fund_daily_nav(nav_date);

CREATE TABLE IF NOT EXISTS etf_daily_hist (
    symbol       TEXT NOT NULL,
    trade_date   TEXT NOT NULL,
    open         REAL,
    close        REAL,
    high         REAL,
    low          REAL,
    volume       REAL,
    amount       REAL,
    amplitude    REAL,
    pct_change   REAL,
    change_amount REAL,
    turnover     REAL,
    PRIMARY KEY (symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS stock_daily_hist (
    symbol       TEXT NOT NULL,
    trade_date   TEXT NOT NULL,
    market       TEXT DEFAULT 'A-share',
    open         REAL,
    close        REAL,
    high         REAL,
    low          REAL,
    volume       REAL,
    amount       REAL,
    amplitude    REAL,
    pct_change   REAL,
    change_amount REAL,
    turnover     REAL,
    PRIMARY KEY (symbol, trade_date)
);

CREATE TABLE IF NOT EXISTS fund_industry_allocation (
    fund_code    TEXT NOT NULL,
    report_date  TEXT NOT NULL,
    industry_name TEXT NOT NULL,
    industry_pct REAL,
    data_source  TEXT,
    last_updated TEXT,
    PRIMARY KEY (fund_code, report_date, industry_name)
);

CREATE TABLE IF NOT EXISTS stock_industry (
    stock_code   TEXT PRIMARY KEY,
    stock_name   TEXT,
    industry_l1  TEXT,
    industry_l2  TEXT,
    data_source  TEXT,
    last_updated TEXT
);
"""


def _get_conn(db_path: str = _DB_PATH) -> sqlite3.Connection:
    """Get a connection and ensure all tables exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.executescript(_CREATE_TABLES_SQL)
    # 迁移：为旧数据库的 stock_daily_hist 补上 market 字段
    _migrate_stock_daily_hist(conn)
    return conn


def _migrate_stock_daily_hist(conn: sqlite3.Connection):
    """检查 stock_daily_hist 是否有 market 列，没有则新增并将已有数据标记为 'A-share'。"""
    cursor = conn.execute("PRAGMA table_info(stock_daily_hist)")
    columns = [row[1] for row in cursor.fetchall()]
    if "market" not in columns:
        conn.execute("ALTER TABLE stock_daily_hist ADD COLUMN market TEXT DEFAULT 'A-share'")
        conn.execute("UPDATE stock_daily_hist SET market = 'A-share' WHERE market IS NULL")
        conn.commit()
        logger.info("Migrated stock_daily_hist: added 'market' column")


# ──────────────────────────────────────────────
# Fund info helpers
# ──────────────────────────────────────────────

def _upsert_fund_info(conn: sqlite3.Connection, code: str, name: str,
                      fund_type: str, source: str):
    """Insert or update fund_info row.

    Fixes:
      - source_platforms: NULL/empty safe, exact-match (no substring false hit)
      - fund_type: first-writer-wins (preserve existing non-empty value)
    """
    conn.execute("""
        INSERT INTO fund_info (fund_code, fund_name, fund_type,
                               source_platforms, last_updated)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(fund_code) DO UPDATE SET
            fund_name = excluded.fund_name,
            fund_type = CASE
                WHEN fund_info.fund_type IS NOT NULL AND fund_info.fund_type != ''
                THEN fund_info.fund_type
                ELSE excluded.fund_type
            END,
            source_platforms = CASE
                WHEN fund_info.source_platforms IS NULL OR fund_info.source_platforms = ''
                THEN excluded.source_platforms
                WHEN (',' || fund_info.source_platforms || ',')
                     LIKE ('%,' || excluded.source_platforms || ',%')
                THEN fund_info.source_platforms
                ELSE fund_info.source_platforms || ',' || excluded.source_platforms
            END,
            last_updated = excluded.last_updated
    """, (code, name, fund_type, source, datetime.now().isoformat()))


def _update_fund_info_stats(conn: sqlite3.Connection, code: str):
    """Update nav_start_date, nav_end_date, total_records from fund_daily_nav."""
    row = conn.execute("""
        SELECT MIN(nav_date), MAX(nav_date), COUNT(*)
        FROM fund_daily_nav WHERE fund_code = ?
    """, (code,)).fetchone()
    if row and row[2] > 0:
        conn.execute("""
            UPDATE fund_info
            SET nav_start_date = ?, nav_end_date = ?, total_records = ?,
                last_updated = ?
            WHERE fund_code = ?
        """, (row[0], row[1], row[2], datetime.now().isoformat(), code))


# ──────────────────────────────────────────────
# 需求 8: OTC fund NAV fetch & store
# ──────────────────────────────────────────────

def _get_latest_nav_date(conn: sqlite3.Connection, code: str) -> Optional[str]:
    """Return the latest nav_date for a fund, or None."""
    row = conn.execute(
        "SELECT MAX(nav_date) FROM fund_daily_nav WHERE fund_code = ?",
        (code,)).fetchone()
    return row[0] if row and row[0] else None


def _fetch_otc_fund_nav(code: str, fund_type: str) -> Optional[pd.DataFrame]:
    """Fetch OTC fund NAV from akshare. Returns DataFrame or None."""
    try:
        if fund_type == "money_fund":
            df = ak.fund_money_fund_info_em(symbol=code)
        else:
            df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
        return df if df is not None and not df.empty else None
    except Exception as e:
        logger.warning(f"Failed to fetch NAV for {code}: {e}")
        return None


def _store_nav_rows(conn: sqlite3.Connection, code: str,
                    df: pd.DataFrame, fund_type: str):
    """Parse akshare NAV DataFrame and insert rows into fund_daily_nav."""
    rows = []
    if fund_type == "money_fund":
        # money fund columns: 净值日期, 万份收益, 7日年化收益率(%)
        for _, r in df.iterrows():
            date_str = str(r.iloc[0])[:10]
            unit_nav = float(r.iloc[2]) if pd.notna(r.iloc[2]) else None  # 7-day annualised
            acc_nav = float(r.iloc[1]) if pd.notna(r.iloc[1]) else None   # 万份收益
            rows.append((code, date_str, unit_nav, acc_nav, None))
    else:
        # normal fund columns: 净值日期, 单位净值, 日增长率
        for _, r in df.iterrows():
            date_str = str(r.iloc[0])[:10]
            unit_nav = float(r.iloc[1]) if pd.notna(r.iloc[1]) else None
            daily_ret = float(r.iloc[2]) if len(r) > 2 and pd.notna(r.iloc[2]) else None
            rows.append((code, date_str, unit_nav, None, daily_ret))

    if rows:
        conn.executemany("""
            INSERT OR IGNORE INTO fund_daily_nav
            (fund_code, nav_date, unit_nav, acc_nav, daily_return)
            VALUES (?, ?, ?, ?, ?)
        """, rows)


def fetch_and_store_fund_nav(platform: str, db_path: str = _DB_PATH):
    """Fetch and store NAV for all OTC funds of a given platform.

    Reads fund list from cache/{platform}/fund_info.json.
    Supports incremental update.

    Args:
        platform: One of 'alipay', 'qieman', 'snowball'.
        db_path: Path to SQLite database file.
    """
    fund_info_path = os.path.join("cache", platform, "fund_info.json")
    fund_map = load_json(fund_info_path)
    if not fund_map:
        logger.warning(f"No fund_info.json found for {platform}")
        return

    conn = _get_conn(db_path)
    total = len(fund_map)
    success = 0
    skipped = 0

    logger.info(f"---------------------[{platform}] Starting NAV fetch for {total} funds... ---------------------")

    for code, info in fund_map.items():
        name = info.get("name", "")
        fund_type = info.get("sub_type", "")

        # Skip non-OTC types
        if fund_type == "etf":
            # OTC ETF-linked funds still have 6-digit codes starting with 0/1/2
            # but on alipay/qieman/snowball they are treated as OTC
            pass

        _upsert_fund_info(conn, code, name, fund_type, platform)

        # Incremental: check latest date in DB
        latest = _get_latest_nav_date(conn, code)
        if latest:
            # If data is recent enough (within 3 days), skip
            try:
                latest_dt = datetime.strptime(latest, "%Y-%m-%d")
                if (datetime.now() - latest_dt).days <= 3:
                    skipped += 1
                    continue
            except ValueError:
                pass

        time.sleep(_API_DELAY)
        df = _fetch_otc_fund_nav(code, fund_type)
        if df is not None:
            _store_nav_rows(conn, code, df, fund_type)
            _update_fund_info_stats(conn, code)
            success += 1
            logger.info(f"  [{success}/{total}] {code} ({name}): {len(df)} rows")
        else:
            logger.warning(f"  {code} ({name}): fetch failed, skipped")

    conn.commit()
    conn.close()
    logger.info(f"[{platform}] NAV fetch done: {success} fetched, "
                f"{skipped} skipped (up-to-date), "
                f"{total - success - skipped} failed \n\n")


# ──────────────────────────────────────────────
# 需求 11: ETF + A-stock history fetch & store
# ──────────────────────────────────────────────

def _get_latest_hist_date(conn: sqlite3.Connection, table: str,
                          symbol: str) -> Optional[str]:
    """Return latest trade_date for a symbol in the given table."""
    row = conn.execute(
        f"SELECT MAX(trade_date) FROM {table} WHERE symbol = ?",
        (symbol,)).fetchone()
    return row[0] if row and row[0] else None


def _store_hist_rows(conn: sqlite3.Connection, table: str,
                     symbol: str, df: pd.DataFrame,
                     market: str = "A-share"):
    """Parse akshare history DataFrame and insert into table.

    Args:
        conn: SQLite 连接
        table: 目标表名
        symbol: 股票/ETF代码
        df: akshare 返回的 DataFrame（支持中文或英文列名）
        market: 市场标识 'A-share' / 'H-share' / 'US'
    """
    # 自动检测列名格式：中文(stock_zh_a_hist) vs 英文(stock_xx_daily)
    cols = df.columns.tolist()
    is_cn = "日期" in cols

    rows = []
    for _, r in df.iterrows():
        if is_cn:
            date_str = str(r.get("日期", ""))[:10]
            rows.append((
                symbol, date_str, market,
                _safe_float(r.get("开盘")), _safe_float(r.get("收盘")),
                _safe_float(r.get("最高")), _safe_float(r.get("最低")),
                _safe_float(r.get("成交量")), _safe_float(r.get("成交额")),
                _safe_float(r.get("振幅")), _safe_float(r.get("涨跌幅")),
                _safe_float(r.get("涨跌额")), _safe_float(r.get("换手率")),
            ))
        else:
            # 英文列名：stock_zh_a_daily / stock_hk_daily / stock_us_daily
            date_str = str(r.get("date", ""))[:10]
            rows.append((
                symbol, date_str, market,
                _safe_float(r.get("open")), _safe_float(r.get("close")),
                _safe_float(r.get("high")), _safe_float(r.get("low")),
                _safe_float(r.get("volume")), _safe_float(r.get("amount")),
                None,  # amplitude (daily API 无此字段)
                None,  # pct_change (daily API 无此字段)
                None,  # change_amount (daily API 无此字段)
                _safe_float(r.get("turnover")),
            ))
    if rows:
        if table == "stock_daily_hist":
            conn.executemany(f"""
                INSERT OR IGNORE INTO {table}
                (symbol, trade_date, market, open, close, high, low,
                 volume, amount, amplitude, pct_change, change_amount, turnover)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
        else:
            # etf_daily_hist 没有 market 字段，去掉 market
            rows_no_market = [(r[0], r[1]) + r[3:] for r in rows]
            conn.executemany(f"""
                INSERT OR IGNORE INTO {table}
                (symbol, trade_date, open, close, high, low,
                 volume, amount, amplitude, pct_change, change_amount, turnover)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows_no_market)


def _safe_float(val) -> Optional[float]:
    """Safely convert to float, return None on failure."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _fetch_etf_nav(code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """用 fund_etf_fund_info_em 获取场内ETF历史单位净值。

    该接口参数为 fund, start_date, end_date。
    返回列: ['净值日期', '单位净值', '累计净值', '日增长率', '申购状态', '赎回状态']
    """
    try:
        df = ak.fund_etf_fund_info_em(fund=code, start_date=start_date, end_date=end_date)
        return df if df is not None and not df.empty else None
    except Exception as e:
        logger.warning(f"fund_etf_fund_info_em failed for {code}: {e}")
        return None


def _store_etf_nav_rows(conn: sqlite3.Connection, code: str, df: pd.DataFrame):
    """将 fund_etf_fund_info_em 返回的 DataFrame 存入 etf_daily_hist 表。

    注意：该API返回的是净值而非行情，所以 open/high/low/volume 等字段为空，
    只填充 close(=单位净值) 和 pct_change(=日增长率)。
    """
    rows = []
    for _, r in df.iterrows():
        date_str = str(r.get("净值日期", ""))[:10]
        unit_nav = _safe_float(r.get("单位净值"))
        daily_ret = _safe_float(r.get("日增长率"))
        rows.append((
            code, date_str,
            None,       # open
            unit_nav,   # close = 单位净值
            None,       # high
            None,       # low
            None,       # volume
            None,       # amount
            None,       # amplitude
            daily_ret,  # pct_change = 日增长率
            None,       # change_amount
            None,       # turnover
        ))
    if rows:
        conn.executemany("""
            INSERT OR IGNORE INTO etf_daily_hist
            (symbol, trade_date, open, close, high, low,
             volume, amount, amplitude, pct_change, change_amount, turnover)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, rows)


def fetch_and_store_etf_hist(start_date: str = "", end_date: str = "",
                             db_path: str = _DB_PATH):
    """获取并存储场内ETF历史净值（使用 fund_etf_fund_info_em）。

    读取 cache/huatai/classified_holdings.json 中的 ETF 列表。

    Args:
        start_date: 起始日期 YYYYMMDD，默认近10年。
        end_date: 结束日期 YYYYMMDD，默认今天。
        db_path: SQLite 数据库路径。
    """
    holdings = load_json("cache/huatai/classified_holdings.json")
    if not holdings:
        logger.warning("No Huatai classified_holdings.json found")
        return

    if isinstance(holdings, dict):
        holdings = list(holdings.values())

    etfs = [h for h in holdings if h.get("sub_type") == "etf"]
    if not etfs:
        logger.info("No ETF holdings found in Huatai")
        return

    if not end_date:
        end_date = datetime.now().strftime("%Y%m%d")
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365 * 10)).strftime("%Y%m%d")

    conn = _get_conn(db_path)
    success = 0

    logger.info(f"Fetching ETF NAV for {len(etfs)} ETFs ({start_date}~{end_date})...")

    for h in etfs:
        code = h["code"]
        name = h.get("name", "")

        # 增量检查
        latest = _get_latest_hist_date(conn, "etf_daily_hist", code)
        if latest:
            try:
                latest_dt = datetime.strptime(latest, "%Y-%m-%d")
                if (datetime.now() - latest_dt).days <= 3:
                    logger.debug(f"  {code} ({name}): up-to-date, skipped")
                    continue
                start_date_inc = (latest_dt + timedelta(days=1)).strftime("%Y%m%d")
            except ValueError:
                start_date_inc = start_date
        else:
            start_date_inc = start_date

        time.sleep(_API_DELAY)
        df = _fetch_etf_nav(code, start_date_inc, end_date)
        if df is not None:
            _store_etf_nav_rows(conn, code, df)
            success += 1
            logger.info(f"  {code} ({name}): {len(df)} rows fetched")
        else:
            logger.warning(f"  {code} ({name}): empty result")

    conn.commit()
    conn.close()
    logger.info(f"ETF history done: {success}/{len(etfs)} fetched")


def fetch_and_store_stock_hist(db_path: str = _DB_PATH):
    """Fetch and store daily history for Huatai A-share stock holdings.

    Reads stock list from cache/huatai/classified_holdings.json.
    """
    holdings = load_json("cache/huatai/classified_holdings.json")
    if not holdings:
        logger.warning("No Huatai classified_holdings.json found")
        return

    if isinstance(holdings, dict):
        holdings = list(holdings.values())

    stocks = [h for h in holdings
              if h.get("sub_type") in ("stock_a", "stock_cn")]
    if not stocks:
        logger.info("No A-share stock holdings found in Huatai")
        return

    conn = _get_conn(db_path)
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y%m%d")
    success = 0

    logger.info(f"Fetching stock history for {len(stocks)} stocks...")

    for h in stocks:
        code = h["code"]
        name = h.get("name", "")

        # Incremental check
        latest = _get_latest_hist_date(conn, "stock_daily_hist", code)
        if latest:
            try:
                latest_dt = datetime.strptime(latest, "%Y-%m-%d")
                if (datetime.now() - latest_dt).days <= 3:
                    logger.debug(f"  {code} ({name}): up-to-date, skipped")
                    continue
                start_date_inc = (latest_dt + timedelta(days=1)).strftime("%Y%m%d")
            except ValueError:
                start_date_inc = start_date
        else:
            start_date_inc = start_date

        time.sleep(_API_DELAY)
        try:
            # stock_zh_a_daily 的 symbol 需要加交易所前缀: sh/sz
            prefix = "sh" if code.startswith("6") else "sz"
            df = ak.stock_zh_a_daily(
                symbol=f"{prefix}{code}",
                start_date=start_date_inc, end_date=end_date,
                adjust="qfq")
            if df is not None and not df.empty:
                _store_hist_rows(conn, "stock_daily_hist", code, df, market="A-share")
                success += 1
                logger.info(f"  {code} ({name}): {len(df)} rows fetched")
            else:
                logger.warning(f"  {code} ({name}): empty result")
        except Exception as e:
            logger.warning(f"  {code} ({name}): fetch failed - {e}")

    conn.commit()
    conn.close()
    logger.info(f"A-share stock history done: {success}/{len(stocks)} fetched")


def fetch_and_store_stock_hk_hist(db_path: str = _DB_PATH):
    """获取并存储富途港股持仓的历史行情数据。

    读取 cache/futu/classified_holdings.json 中 sub_type='stock_hk' 的记录。
    使用 akshare 的 stock_hk_daily 接口（新浪数据源，全量返回后本地过滤）。
    """
    holdings = load_json("cache/futu/classified_holdings.json")
    if not holdings:
        logger.warning("No Futu classified_holdings.json found")
        return

    if isinstance(holdings, dict):
        holdings = list(holdings.values())

    hk_stocks = [h for h in holdings if h.get("sub_type") == "stock_hk"]
    if not hk_stocks:
        logger.info("No HK stock holdings found in Futu")
        return

    conn = _get_conn(db_path)
    start_date = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y-%m-%d")
    success = 0

    logger.info(f"Fetching HK stock history for {len(hk_stocks)} stocks...")

    for h in hk_stocks:
        code = h["code"]
        name = h.get("name", "")

        # 增量检查
        latest = _get_latest_hist_date(conn, "stock_daily_hist", code)
        if latest:
            try:
                latest_dt = datetime.strptime(latest, "%Y-%m-%d")
                if (datetime.now() - latest_dt).days <= 3:
                    logger.debug(f"  {code} ({name}): up-to-date, skipped")
                    continue
                filter_start = (latest_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            except ValueError:
                filter_start = start_date
        else:
            filter_start = start_date

        time.sleep(_API_DELAY)
        try:
            # stock_hk_daily 不支持日期范围参数，返回全量数据
            df = ak.stock_hk_daily(symbol=code, adjust="qfq")
            if df is not None and not df.empty:
                # 本地过滤日期范围
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                df = df[df["date"] >= filter_start]
                if not df.empty:
                    _store_hist_rows(conn, "stock_daily_hist", code, df, market="H-share")
                    success += 1
                    logger.info(f"  {code} ({name}): {len(df)} rows fetched")
                else:
                    logger.debug(f"  {code} ({name}): no new data after filtering")
            else:
                logger.warning(f"  {code} ({name}): empty result")
        except Exception as e:
            logger.warning(f"  {code} ({name}): fetch failed - {e}")

    conn.commit()
    conn.close()
    logger.info(f"HK stock history done: {success}/{len(hk_stocks)} fetched")


def fetch_and_store_stock_us_hist(db_path: str = _DB_PATH):
    """获取并存储富途美股持仓的历史行情数据。

    读取 cache/futu/classified_holdings.json 中 sub_type='stock_us' 的记录。
    使用 akshare 的 stock_us_daily 接口（新浪数据源，全量返回后本地过滤）。
    """
    holdings = load_json("cache/futu/classified_holdings.json")
    if not holdings:
        logger.warning("No Futu classified_holdings.json found")
        return

    if isinstance(holdings, dict):
        holdings = list(holdings.values())

    us_stocks = [h for h in holdings if h.get("sub_type") == "stock_us"]
    if not us_stocks:
        logger.info("No US stock holdings found in Futu")
        return

    conn = _get_conn(db_path)
    start_date = (datetime.now() - timedelta(days=365 * 5)).strftime("%Y-%m-%d")
    success = 0

    logger.info(f"Fetching US stock history for {len(us_stocks)} stocks...")

    for h in us_stocks:
        code = h["code"]
        name = h.get("name", "")

        # 增量检查
        latest = _get_latest_hist_date(conn, "stock_daily_hist", code)
        if latest:
            try:
                latest_dt = datetime.strptime(latest, "%Y-%m-%d")
                if (datetime.now() - latest_dt).days <= 3:
                    logger.debug(f"  {code} ({name}): up-to-date, skipped")
                    continue
                filter_start = (latest_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            except ValueError:
                filter_start = start_date
        else:
            filter_start = start_date

        time.sleep(_API_DELAY)
        try:
            # stock_us_daily 不支持日期范围参数，返回全量数据
            df = ak.stock_us_daily(symbol=code, adjust="qfq")
            if df is not None and not df.empty:
                # 本地过滤日期范围
                df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
                df = df[df["date"] >= filter_start]
                if not df.empty:
                    _store_hist_rows(conn, "stock_daily_hist", code, df, market="US")
                    success += 1
                    logger.info(f"  {code} ({name}): {len(df)} rows fetched")
                else:
                    logger.debug(f"  {code} ({name}): no new data after filtering")
            else:
                logger.warning(f"  {code} ({name}): empty result")
        except Exception as e:
            logger.warning(f"  {code} ({name}): fetch failed - {e}")

    conn.commit()
    conn.close()
    logger.info(f"US stock history done: {success}/{len(us_stocks)} fetched")


# ──────────────────────────────────────────────
# 需求 9: Industry data SQLite read/write
# ──────────────────────────────────────────────

def save_stock_industry(stock_code: str, stock_name: str,
                        industry_l1: str, industry_l2: str,
                        data_source: str = "",
                        db_path: str = _DB_PATH):
    """Save a single stock's industry classification to SQLite."""
    conn = _get_conn(db_path)
    conn.execute("""
        INSERT OR REPLACE INTO stock_industry
        (stock_code, stock_name, industry_l1, industry_l2, data_source, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (stock_code, stock_name, industry_l1, industry_l2,
          data_source, datetime.now().isoformat()))
    conn.commit()
    conn.close()


def get_stock_industry(stock_code: str,
                       db_path: str = _DB_PATH) -> Optional[dict]:
    """Read a stock's industry classification from SQLite."""
    conn = _get_conn(db_path)
    row = conn.execute(
        "SELECT industry_l1, industry_l2 FROM stock_industry WHERE stock_code = ?",
        (stock_code,)).fetchone()
    conn.close()
    if row:
        return {"industry_l1": row[0], "industry_l2": row[1]}
    return None


def save_fund_industry_allocation(fund_code: str, report_date: str,
                                  industry_data: list[dict],
                                  data_source: str = "",
                                  db_path: str = _DB_PATH):
    """Save a fund's industry allocation to SQLite.

    Args:
        fund_code: Fund code.
        report_date: Report date/quarter (e.g. '2024Q4').
        industry_data: List of {'industry_name': ..., 'industry_pct': ...}.
        data_source: API name used to fetch data.
    """
    conn = _get_conn(db_path)
    now = datetime.now().isoformat()
    rows = [
        (fund_code, report_date, d["industry_name"], d.get("industry_pct", 0),
         data_source, now)
        for d in industry_data
    ]
    conn.executemany("""
        INSERT OR REPLACE INTO fund_industry_allocation
        (fund_code, report_date, industry_name, industry_pct,
         data_source, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()


def get_fund_industry_allocation(fund_code: str,
                                 db_path: str = _DB_PATH) -> Optional[list[dict]]:
    """Read a fund's industry allocation from SQLite."""
    conn = _get_conn(db_path)
    rows = conn.execute("""
        SELECT industry_name, industry_pct, report_date
        FROM fund_industry_allocation
        WHERE fund_code = ?
        ORDER BY report_date DESC, industry_pct DESC
    """, (fund_code,)).fetchall()
    conn.close()
    if rows:
        return [{"industry_name": r[0], "industry_pct": r[1],
                 "report_date": r[2]} for r in rows]
    return None


def batch_save_stock_industry(industry_dict: dict,
                              db_path: str = _DB_PATH):
    """Batch save stock industry data from a dict.

    Args:
        industry_dict: {stock_code: {industry_l1, industry_l2, ...}, ...}
    """
    conn = _get_conn(db_path)
    now = datetime.now().isoformat()
    rows = []
    for code, info in industry_dict.items():
        l1 = info.get("industry_l1", "未知")
        l2 = info.get("industry_l2", "未知")
        rows.append((code, "", l1, l2, "batch_import", now))
    conn.executemany("""
        INSERT OR REPLACE INTO stock_industry
        (stock_code, stock_name, industry_l1, industry_l2, data_source, last_updated)
        VALUES (?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    logger.info(f"Batch saved {len(rows)} stock industry records to SQLite")


# ──────────────────────────────────────────────
# 需求 10: Quant metrics computation
# ──────────────────────────────────────────────

def _read_nav_series(conn: sqlite3.Connection, fund_code: str,
                     days: int = 0) -> Optional[pd.Series]:
    """Read NAV series from fund_daily_nav as a pandas Series indexed by date.

    Args:
        conn: SQLite connection.
        fund_code: Fund code.
        days: If > 0, only read the last N calendar days.

    Returns:
        pd.Series with DatetimeIndex and NAV values, or None.
    """
    if days > 0:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT nav_date, unit_nav FROM fund_daily_nav
            WHERE fund_code = ? AND nav_date >= ? AND unit_nav IS NOT NULL
            ORDER BY nav_date
        """, (fund_code, cutoff)).fetchall()
    else:
        rows = conn.execute("""
            SELECT nav_date, unit_nav FROM fund_daily_nav
            WHERE fund_code = ? AND unit_nav IS NOT NULL
            ORDER BY nav_date
        """, (fund_code,)).fetchall()

    if not rows or len(rows) < 2:
        return None

    dates = [r[0] for r in rows]
    values = [r[1] for r in rows]
    return pd.Series(values, index=pd.to_datetime(dates), name=fund_code)


def _read_close_series(conn: sqlite3.Connection, table: str,
                       symbol: str, days: int = 0) -> Optional[pd.Series]:
    """Read close price series from etf_daily_hist or stock_daily_hist."""
    if days > 0:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        rows = conn.execute(f"""
            SELECT trade_date, close FROM {table}
            WHERE symbol = ? AND trade_date >= ? AND close IS NOT NULL
            ORDER BY trade_date
        """, (symbol, cutoff)).fetchall()
    else:
        rows = conn.execute(f"""
            SELECT trade_date, close FROM {table}
            WHERE symbol = ? AND close IS NOT NULL
            ORDER BY trade_date
        """, (symbol,)).fetchall()

    if not rows or len(rows) < 2:
        return None

    dates = [r[0] for r in rows]
    values = [r[1] for r in rows]
    return pd.Series(values, index=pd.to_datetime(dates), name=symbol)


def _calc_annualised_return(series: pd.Series) -> Optional[float]:
    """Calculate annualised return from a price/NAV series."""
    if series is None or len(series) < 30:
        return None
    start_val = series.iloc[0]
    end_val = series.iloc[-1]
    if start_val <= 0:
        return None
    trading_days = len(series)
    return (end_val / start_val) ** (250 / trading_days) - 1


def _calc_annualised_volatility(series: pd.Series) -> Optional[float]:
    """Calculate annualised volatility from a price/NAV series."""
    if series is None or len(series) < 30:
        return None
    returns = series.pct_change().dropna()
    if len(returns) < 20:
        return None
    return float(returns.std() * (252 ** 0.5))


def _calc_max_drawdown(series: pd.Series) -> Optional[float]:
    """Calculate maximum drawdown from a price/NAV series."""
    if series is None or len(series) < 30:
        return None
    cummax = series.cummax()
    drawdown = (series - cummax) / cummax
    return float(drawdown.min())


def compute_quant_metrics(db_path: str = _DB_PATH) -> dict:
    """Compute quantitative metrics for all assets in the database.

    Computes:
      - Annualised return (1Y, 3Y, 5Y)
      - Annualised volatility (1Y, 3Y, 5Y)
      - Max drawdown (1Y, 3Y, 5Y)
      - Correlation matrix

    Returns:
        Dict with 'fund_metrics', 'etf_metrics', 'stock_metrics',
        'correlation_matrix'.
    """
    conn = _get_conn(db_path)
    result = {
        "fund_metrics": {},
        "etf_metrics": {},
        "stock_metrics": {},
        "correlation_matrix": {},
    }

    windows = {"1Y": 365, "3Y": 365 * 3, "5Y": 365 * 5}
    all_return_series = {}  # symbol → daily return Series (for correlation)

    # 1) OTC fund metrics
    fund_codes = [r[0] for r in conn.execute(
        "SELECT DISTINCT fund_code FROM fund_daily_nav").fetchall()]
    for code in fund_codes:
        metrics = _compute_single_metrics(conn, "fund_daily_nav", code,
                                          is_nav=True, windows=windows)
        if metrics:
            result["fund_metrics"][code] = metrics
            # Collect return series for correlation (use 1Y)
            s = _read_nav_series(conn, code, days=365)
            if s is not None and len(s) >= 30:
                all_return_series[code] = s.pct_change().dropna()

    # 2) ETF metrics
    etf_symbols = [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM etf_daily_hist").fetchall()]
    for sym in etf_symbols:
        metrics = _compute_single_metrics(conn, "etf_daily_hist", sym,
                                          is_nav=False, windows=windows)
        if metrics:
            result["etf_metrics"][sym] = metrics
            s = _read_close_series(conn, "etf_daily_hist", sym, days=365)
            if s is not None and len(s) >= 30:
                all_return_series[sym] = s.pct_change().dropna()

    # 3) Stock metrics
    stock_symbols = [r[0] for r in conn.execute(
        "SELECT DISTINCT symbol FROM stock_daily_hist").fetchall()]
    for sym in stock_symbols:
        metrics = _compute_single_metrics(conn, "stock_daily_hist", sym,
                                          is_nav=False, windows=windows)
        if metrics:
            result["stock_metrics"][sym] = metrics
            s = _read_close_series(conn, "stock_daily_hist", sym, days=365)
            if s is not None and len(s) >= 30:
                all_return_series[sym] = s.pct_change().dropna()

    # 4) Correlation matrix
    if len(all_return_series) >= 2:
        result["correlation_matrix"] = _compute_correlation_matrix(
            all_return_series)

    conn.close()
    logger.info(f"Quant metrics: {len(result['fund_metrics'])} funds, "
                f"{len(result['etf_metrics'])} ETFs, "
                f"{len(result['stock_metrics'])} stocks, "
                f"correlation {len(all_return_series)}x{len(all_return_series)}")
    return result


def _compute_single_metrics(conn: sqlite3.Connection, table: str,
                            code: str, *, is_nav: bool,
                            windows: dict) -> Optional[dict]:
    """Compute return/volatility/drawdown for a single asset across windows."""
    metrics = {}
    has_any = False

    for label, days in windows.items():
        if is_nav:
            s = _read_nav_series(conn, code, days=days)
        else:
            s = _read_close_series(conn, table, code, days=days)

        if s is None or len(s) < 30:
            metrics[label] = {"status": "insufficient_data"}
            continue

        ret = _calc_annualised_return(s)
        vol = _calc_annualised_volatility(s)
        mdd = _calc_max_drawdown(s)
        metrics[label] = {
            "annualised_return": round(ret, 4) if ret is not None else None,
            "annualised_volatility": round(vol, 4) if vol is not None else None,
            "max_drawdown": round(mdd, 4) if mdd is not None else None,
            "data_points": len(s),
        }
        has_any = True

    return metrics if has_any else None


def _compute_correlation_matrix(return_series: dict) -> dict:
    """Compute Pearson correlation matrix from daily return series.

    Args:
        return_series: {symbol: pd.Series of daily returns}

    Returns:
        Dict with 'symbols' (list) and 'matrix' (list of lists).
    """
    # Align all series by date intersection
    df = pd.DataFrame(return_series)
    df = df.dropna()

    if len(df) < 20 or len(df.columns) < 2:
        return {}

    corr = df.corr(method="pearson")
    symbols = list(corr.columns)
    matrix = corr.values.tolist()

    # Round values
    matrix = [[round(v, 4) for v in row] for row in matrix]

    return {
        "symbols": symbols,
        "matrix": matrix,
        "data_points": len(df),
    }
