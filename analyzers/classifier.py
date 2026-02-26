"""Asset Classifier - Step 4: First-level asset categorization.

Classifies each HoldingRecord into asset_class + sub_type based on:
- Securities code pattern matching (A-shares, HK stocks, US stocks, ETFs, bonds)
- AKShare fund type query with local cache
"""

import json
import logging
import os
import re
from typing import Optional

from models import HoldingRecord

logger = logging.getLogger(__name__)

# --- Code pattern rules ---
# A-share stocks: 60xxxx (SH main), 00xxxx (SZ main), 30xxxx (ChiNext), 68xxxx (STAR)
_A_SHARE_RE = re.compile(r"^(60\d{4}|00\d{4}|30\d{4}|68\d{4})$")

# A-share ETFs: 51xxxx (SH), 15xxxx / 16xxxx (SZ)
_ETF_RE = re.compile(r"^(51\d{4}|15\d{4}|16\d{4})$")

# Open-end fund: 6-digit codes not matching stock/ETF patterns
_FUND_CODE_RE = re.compile(r"^\d{6}$")

# Bonds: 10xxxx / 11xxxx / 12xxxx (convertible bonds, enterprise bonds, etc.)
_BOND_RE = re.compile(r"^(10\d{4}|11\d{4}|12\d{4})$")

# HK stock: 5-digit pure number or has .HK suffix, or starts with HK prefix
_HK_STOCK_RE = re.compile(r"^(\d{5}|.+\.HK)$", re.IGNORECASE)

# US stock: alphabetic code (e.g. AAPL, BRK.B, GLD)
_US_STOCK_RE = re.compile(r"^[A-Za-z][A-Za-z0-9.]*$")

# HK fund code pattern (from Futu): HK followed by digits
_HK_FUND_RE = re.compile(r"^HK\d{10,}$", re.IGNORECASE)

# Fund sub_type mapping from akshare fund_name_em
# Simplified: only 6 types (equity/mixed/bond/money/commodity/etf)
# Index, FOF, ETF-场内, ETF联接 → all mapped to "etf"
# QDII → not mapped here; resolved by _refine_sub_type_by_name()
_FUND_TYPE_MAP = {
    "股票型": "equity_fund",
    "混合型": "mixed_fund",
    "债券型": "bond_fund",
    "货币型": "money_fund",
    "商品型": "commodity_fund",
    "指数型": "etf",
    "FOF型": "etf",
    "FOF": "etf",
    "ETF-场内": "etf",
    "ETF联接": "etf",
    "QDII型": "qdii_pending",
    "QDII": "qdii_pending",
}

# Keywords for name-based refinement (priority order matters)
_COMMODITY_KEYWORDS = ["黄金", "原油", "商品", "豆粕", "白银", "有色金属", "能源"]
_BOND_KEYWORDS = ["债", "債", "纯债", "純債", "信用", "利率", "国开行", "國開行"]
_EQUITY_KEYWORDS = ["股票", "成长", "价值", "蓝筹", "消费", "医疗", "医药",
                    "科技", "互联网", "环保", "养老产业", "红利", "健康",
                    "产业", "驱动", "精选", "主题"]


def _load_fund_cache(cache_path: str) -> dict:
    """Load fund info cache from JSON file."""
    if os.path.exists(cache_path):
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            logger.warning(f"Failed to load fund cache from {cache_path}, starting fresh")
    return {}


def _save_fund_cache(cache: dict, cache_path: str):
    """Save fund info cache to JSON file."""
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


# In-memory fund list cache (loaded once per session)
_fund_list_df = None


def _load_fund_list():
    """Load akshare fund_name_em into memory (once per session)."""
    global _fund_list_df
    if _fund_list_df is not None:
        return _fund_list_df
    try:
        import akshare as ak
        _fund_list_df = ak.fund_name_em()
        logger.info(f"Loaded fund list: {len(_fund_list_df)} funds")
    except Exception as e:
        logger.warning(f"Failed to load fund list: {e}")
        _fund_list_df = None
    return _fund_list_df


def _query_fund_type_akshare(code: str) -> Optional[str]:
    """Query fund type from the in-memory fund list.

    Returns the sub_type string or None if not found.
    """
    df = _load_fund_list()
    if df is None:
        return None
    try:
        row = df[df["基金代码"] == code]
        if not row.empty:
            fund_type_raw = row.iloc[0]["基金类型"]
            for key, val in _FUND_TYPE_MAP.items():
                if key in str(fund_type_raw):
                    return val
            logger.info(f"Fund {code} type '{fund_type_raw}' not in mapping, using 'other'")
            return "other"
    except Exception as e:
        logger.warning(f"Failed to query fund type for {code}: {e}")
    return None


def _refine_sub_type_by_name(name: str, coarse_type: str) -> str:
    """Refine the coarse sub_type from akshare by analyzing fund name.

    The underlying asset type (equity/bond/commodity) takes priority
    over region/wrapper type (QDII/FOF/index). For example:
    - '国泰黄金ETF联接A' → commodity_fund (not etf)
    - '广发中债1-3年国开行债券指数A' → bond_fund (not etf)
    - '富国全球债券(QDII)A' → bond_fund (not qdii)
    - '广发纳斯达克100ETF联接A' → etf (no commodity/bond keyword)
    """
    # Step 1: Commodity keywords have highest priority
    if any(k in name for k in _COMMODITY_KEYWORDS):
        return "commodity_fund"

    # Step 2: Bond keywords override etf/qdii/mixed
    if any(k in name for k in _BOND_KEYWORDS):
        return "bond_fund"

    # Step 3: For QDII-pending, determine by equity keywords or default to etf
    if coarse_type == "qdii_pending":
        if any(k in name for k in _EQUITY_KEYWORDS):
            return "equity_fund"
        # QDII with ETF/指数/联接 in name → etf
        if any(k in name for k in ["ETF", "etf", "指数", "联接"]):
            return "etf"
        # Default QDII: treat as mixed_fund
        return "mixed_fund"

    # Step 4: Return the coarse type as-is for non-ambiguous types
    return coarse_type


def _classify_single(record: HoldingRecord, fund_cache: dict) -> HoldingRecord:
    """Classify a single HoldingRecord based on its code and source.

    Strategy: For 6-digit numeric codes, query fund database FIRST,
    then fall back to regex-based stock/bond matching.
    This avoids misclassifying fund codes (e.g. 000628) as stocks.

    Mutates record.asset_class and record.sub_type in place.
    """
    code = record.code.strip()

    # --- 1. HK fund (from Futu, code starts with 'HK') ---
    if _HK_FUND_RE.match(code):
        # Check cache first
        cached = fund_cache.get(code)
        if cached and cached.get("sub_type") and cached.get("sub_type") != "other":
            record.asset_class = "fund"
            record.sub_type = cached["sub_type"]
            return record
        # Guess by name keywords (commodity > bond > money > equity > etf > mixed)
        sub_type = _guess_fund_type_by_name(record.name)
        # Special: bond_fund should have asset_class = "bond"
        if sub_type == "bond_fund":
            record.asset_class = "bond"
        elif sub_type == "commodity_fund":
            record.asset_class = "commodity"
        else:
            record.asset_class = "fund"
        record.sub_type = sub_type
        fund_cache[code] = {"name": record.name, "sub_type": sub_type}
        return record

    # --- 2. 6-digit numeric code: fund-first strategy ---
    if _FUND_CODE_RE.match(code):
        # 2a. Check fund cache first
        cached = fund_cache.get(code)
        if cached:
            coarse = cached.get("sub_type", "other")
            record.asset_class = "fund"
            record.sub_type = _refine_sub_type_by_name(record.name, coarse)
            # Update cache if refined type differs
            if record.sub_type != coarse:
                fund_cache[code]["sub_type"] = record.sub_type
            return record

        # 2b. Query akshare fund database
        coarse_type = _query_fund_type_akshare(code)
        if coarse_type:
            refined = _refine_sub_type_by_name(record.name, coarse_type)
            fund_cache[code] = {"name": record.name, "sub_type": refined}
            record.asset_class = "fund"
            record.sub_type = refined
            return record

        # 2c. Not found in fund DB → fall back to regex matching
        # ETF (51/15/16 prefix, only if not found in fund DB)
        if _ETF_RE.match(code):
            record.asset_class = "fund"
            record.sub_type = "etf"
            return record

        # Bond (convertible bonds: 10xxxx/11xxxx/12xxxx or name has "转债")
        if _BOND_RE.match(code) or "转债" in record.name:
            record.asset_class = "bond"
            record.sub_type = "bond"
            return record

        # A-share stock (only if NOT a fund)
        if _A_SHARE_RE.match(code):
            record.asset_class = "equity"
            record.sub_type = "stock_cn"
            return record

        # 2e. Still unmatched 6-digit → guess as fund by name
        record.asset_class = "fund"
        record.sub_type = _guess_fund_type_by_name(record.name)
        fund_cache[code] = {"name": record.name, "sub_type": record.sub_type}
        return record

    # --- 3. HK stock/ETF (from Futu or other platforms) ---
    # Check for ETF first by name keyword
    if "ETF" in record.name or "etf" in record.name.lower():
        record.asset_class = "fund"
        record.sub_type = "etf"
        return record

    if record.source == "futu" and record.raw_info.get("region") == "HK":
        record.asset_class = "equity"
        record.sub_type = "stock_hk"
        return record

    if _HK_STOCK_RE.match(code) and record.currency == "HKD":
        record.asset_class = "equity"
        record.sub_type = "stock_hk"
        return record

    # --- 4. US stock/ETF ---
    if record.source == "futu" and record.raw_info.get("region") == "US":
        record.asset_class = "equity"
        record.sub_type = "stock_us"
        return record

    if _US_STOCK_RE.match(code) and record.currency == "USD":
        record.asset_class = "equity"
        record.sub_type = "stock_us"
        return record

    # --- 5. Fallback: unclassified ---
    record.asset_class = "other"
    record.sub_type = "other"
    logger.warning(f"Unclassified: code={code}, name={record.name}, source={record.source}")
    return record


def _guess_fund_type_by_name(name: str) -> str:
    """Guess fund sub_type from its Chinese name when API query fails.

    Priority: commodity > bond > money > equity > etf > mixed (default).
    No QDII/FOF as standalone types; they are resolved by asset keywords.
    """
    # Commodity first (highest priority)
    if any(k in name for k in _COMMODITY_KEYWORDS):
        return "commodity_fund"
    # Bond
    if any(k in name for k in _BOND_KEYWORDS):
        return "bond_fund"
    # Money market
    if any(k in name for k in ["货币", "现金", "天天", "余额"]):
        return "money_fund"
    # ETF / index
    if any(k in name.lower() for k in ["指数", "etf", "联接", "沪深300", "中证500",
                                         "中证1000", "创业板", "纳斯达克", "恒生",
                                         "标普"]):
        return "etf"
    # Equity
    if any(k in name for k in _EQUITY_KEYWORDS):
        return "equity_fund"
    # Mixed / default
    if any(k in name for k in ["混合", "灵活配置", "平衡"]):
        return "mixed_fund"
    return "mixed_fund"


def classify_records(records: list[HoldingRecord], config: dict) -> list[HoldingRecord]:
    """Classify all holding records with asset_class and sub_type.

    Args:
        records: List of parsed HoldingRecord (from all platforms).
        config: Configuration dict from config.yaml.

    Returns:
        The same list with asset_class and sub_type populated.
    """
    cache_dir = config["paths"]["cache_dir"]
    cache_path = os.path.join(cache_dir, "fund_info.json")
    fund_cache = _load_fund_cache(cache_path)

    classified_count = {"equity": 0, "fund": 0, "bond": 0, "other": 0}

    for record in records:
        _classify_single(record, fund_cache)
        classified_count[record.asset_class] = classified_count.get(record.asset_class, 0) + 1

    # Save updated cache
    _save_fund_cache(fund_cache, cache_path)

    logger.info(f"Classification summary: {classified_count}")
    return records
