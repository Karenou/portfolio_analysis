"""Test script: Run Level 1 + Level 2 classification for Huatai holdings.

Steps:
  1. Parse Huatai Excel (stock + fund holdings)
  2. Classify records (Level 1)
  3. Currency conversion (original + CNY)
  4. Level 2 penetration (equity/bond/commodity/cash)
  5. Summarize and output

Level 2 Rules:
  - stock_a, stock_hk, stock_us → equity
  - etf (non-gold, non-bond) → equity
  - gold etf → commodity
  - bond etf → bond
  - bond_fund → bond
  - mixed_fund, equity_fund → call API for allocation
  - money_fund → cash

Usage:
  cd portfolio_analysis
  python -m test.test_huatai_penetration                        # default file
  python -m test.test_huatai_penetration --file huatai_20260225.xlsx  # specify file
"""

import argparse
import sys
import os
import json
import logging
import yaml
from collections import defaultdict
from datetime import datetime

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test")


# Parse command-line arguments
ap = argparse.ArgumentParser(description="Test Huatai holdings classification (Level 1 + Level 2)")
ap.add_argument("--file", type=str, required=True, help="Excel file name")
ap.add_argument("--date", type=str, required=True, help="Holdings date in YYYYMMDD format")


def is_gold_etf(name: str) -> bool:
    """Check if ETF is a gold/commodity ETF."""
    gold_keywords = ["黄金", "黃金", "gold", "GLD", "IAU", "商品", "commodity", "贵金属", "貴金屬"]
    name_lower = name.lower()
    return any(kw.lower() in name_lower for kw in gold_keywords)


def is_bond_etf(name: str, code: str = "") -> bool:
    """Check if ETF is a bond/treasury ETF."""
    bond_keywords = [
        "国债", "國債", "債券", "债券",
        "bond", "treasury", "treasuries",
        "short-term treasury", "long-term treasury",
    ]
    bond_codes = ["SGOV", "SHV", "BND", "TLT", "IEF", "SHY", "GOVT", "TIP", "LQD"]
    
    name_lower = name.lower()
    code_upper = code.upper()
    
    if code_upper in bond_codes:
        return True
    
    return any(kw.lower() in name_lower for kw in bond_keywords)


def classify_level2(record, config: dict, date: str = "") -> dict:
    """Classify a single holding into Level 2 asset class.

    Returns:
        dict with keys: equity_pct, bond_pct, commodity_pct, cash_pct, other_pct
    """
    sub_type = record.sub_type or ""
    name = record.name or ""
    
    # Rule 0: Cash equivalents (标准券等) → 100% cash
    raw_info = getattr(record, "raw_info", {}) or {}
    if raw_info.get("is_cash_equivalent"):
        return {"equity_pct": 0.0, "bond_pct": 0.0, "commodity_pct": 0.0,
                "cash_pct": 1.0, "other_pct": 0.0}
    
    # Rule 1: Individual stocks (A-share, HK, US) → 100% equity
    if sub_type in ("stock_a", "stock_cn", "stock_hk", "stock_us"):
        return {"equity_pct": 1.0, "bond_pct": 0.0, "commodity_pct": 0.0,
                "cash_pct": 0.0, "other_pct": 0.0}
    
    # Rule 2: ETF
    if sub_type == "etf":
        if is_gold_etf(name):
            return {"equity_pct": 0.0, "bond_pct": 0.0, "commodity_pct": 1.0,
                    "cash_pct": 0.0, "other_pct": 0.0}
        elif is_bond_etf(name, record.code):
            return {"equity_pct": 0.0, "bond_pct": 1.0, "commodity_pct": 0.0,
                    "cash_pct": 0.0, "other_pct": 0.0}
        else:
            return {"equity_pct": 1.0, "bond_pct": 0.0, "commodity_pct": 0.0,
                    "cash_pct": 0.0, "other_pct": 0.0}
    
    # Rule 3: Bond fund → 100% bond
    if sub_type == "bond_fund":
        return {"equity_pct": 0.0, "bond_pct": 1.0, "commodity_pct": 0.0,
                "cash_pct": 0.0, "other_pct": 0.0}
    
    # Rule 4: Commodity fund → 100% commodity
    if sub_type == "commodity_fund":
        return {"equity_pct": 0.0, "bond_pct": 0.0, "commodity_pct": 1.0,
                "cash_pct": 0.0, "other_pct": 0.0}
    
    # Rule 5: Mixed fund / Equity fund / Index fund → call API
    if sub_type in ("mixed_fund", "equity_fund", "index_fund"):
        from analyzers.fund_penetration import _query_fund_allocation_api
        is_etf_linked = "ETF联接" in name
        alloc = _query_fund_allocation_api(record.code, date=date, is_etf_linked=is_etf_linked)
        
        if alloc:
            return alloc
        else:
            defaults = config.get("default_fund_allocation", {})
            key = "hybrid_fund" if sub_type == "mixed_fund" else "equity_fund"
            a = defaults.get(key, defaults.get("other", {}))
            return {
                "equity_pct": a.get("equity_pct", 0.5),
                "bond_pct": a.get("bond_pct", 0.2),
                "commodity_pct": a.get("commodity_pct", 0.0),
                "cash_pct": a.get("cash_pct", 0.2),
                "other_pct": a.get("other_pct", 0.1),
            }
    
    # Rule 6: Money fund → 100% cash
    if sub_type == "money_fund":
        return {"equity_pct": 0.0, "bond_pct": 0.0, "commodity_pct": 0.0,
                "cash_pct": 1.0, "other_pct": 0.0}
    
    # Default: other
    return {"equity_pct": 0.0, "bond_pct": 0.0, "commodity_pct": 0.0,
            "cash_pct": 0.0, "other_pct": 1.0}



if __name__ == "__main__":
    args = ap.parse_args()
    file_name = args.file
    date_str = args.date or datetime.now().strftime("%Y%m%d")

    # Step 1: Parse Huatai Excel
    from parsers.huatai_parser import HuataiParser

    parser = HuataiParser()
    file_path = f"data/{file_name}"
    
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        sys.exit(1)
    
    records = parser.parse(file_path)
    logger.info(f"Step 1 done: {len(records)} records parsed from Huatai")
    
    # Count stocks vs funds
    stock_count = sum(1 for r in records if r.asset_class == "equity" or 
                      (r.code.isdigit() and len(r.code) == 6 and 
                       r.code.startswith(("60", "00", "30", "68"))))
    fund_count = len(records) - stock_count
    logger.info(f"  Estimated: {stock_count} stocks, {fund_count} funds")

    # Load config
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Use huatai-specific cache subdirectory
    cache_dir = "cache/huatai"
    os.makedirs(cache_dir, exist_ok=True)
    config["paths"]["cache_dir"] = cache_dir

    # Step 2: Classify (Level 1)
    from analyzers.classifier import classify_records

    records = classify_records(records, config)
    logger.info("Step 2 done: classification complete")

    # Print classification result
    cls_summary: dict[str, int] = {}
    for r in records:
        key = f"{r.asset_class}/{r.sub_type}"
        cls_summary[key] = cls_summary.get(key, 0) + 1
    for k, v in sorted(cls_summary.items()):
        logger.info(f"  {k}: {v} records")

    # Step 3: Currency conversion
    fx = config["exchange_rates"]
    for r in records:
        r.market_value_cny = r.market_value * fx.get(r.currency, 1.0)
    
    logger.info("Step 3 done: currency conversion complete")

    # Step 4: Level 2 classification
    logger.info("Starting Step 4: Level 2 penetration...")
    
    # Store level2 allocation for each record
    level2_allocs = {}
    for r in records:
        alloc = classify_level2(r, config, date=date_str)
        level2_allocs[r.code] = alloc
        if r.sub_type in ("mixed_fund", "equity_fund", "index_fund"):
            logger.info(f"  {r.code} ({r.name}): eq={alloc['equity_pct']:.1%} "
                        f"bd={alloc['bond_pct']:.1%} cash={alloc['cash_pct']:.1%}")
    
    logger.info("Step 4 done: Level 2 penetration complete")

    # Step 5: Summarize
    # 5.1 Original currency summary
    currency_summary: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for r in records:
        currency_summary[r.currency][r.currency] += r.market_value

    # 5.2 Level 1 summary (by sub_type)
    level1_summary: dict[str, dict] = defaultdict(lambda: {
        "original_values": defaultdict(float),
        "market_value_cny": 0.0,
        "count": 0
    })
    
    for r in records:
        sub_type = r.sub_type
        level1_summary[sub_type]["original_values"][r.currency] += r.market_value
        level1_summary[sub_type]["market_value_cny"] += r.market_value_cny
        level1_summary[sub_type]["count"] += 1

    # 5.3 Level 2 summary (equity/bond/commodity/cash)
    level2_summary: dict[str, dict] = defaultdict(lambda: {
        "original_values": defaultdict(float),
        "market_value_cny": 0.0,
        "count": 0
    })
    
    for r in records:
        alloc = level2_allocs.get(r.code, {"other_pct": 1.0})
        mv_cny = r.market_value_cny
        
        # Distribute market value by Level 2 allocation
        for cls, pct in [
            ("equity", alloc.get("equity_pct", 0)),
            ("bond", alloc.get("bond_pct", 0)),
            ("commodity", alloc.get("commodity_pct", 0)),
            ("cash", alloc.get("cash_pct", 0)),
            ("other", alloc.get("other_pct", 0)),
        ]:
            if pct > 0:
                level2_summary[cls]["market_value_cny"] += mv_cny * pct
                level2_summary[cls]["original_values"][r.currency] += r.market_value * pct
                level2_summary[cls]["count"] += pct  # Fractional count

    # Calculate total CNY market value
    total_cny = sum(v["market_value_cny"] for v in level1_summary.values())

    # Calculate percentages
    for sub_type, data in level1_summary.items():
        data["pct"] = data["market_value_cny"] / total_cny if total_cny > 0 else 0
    
    for cls, data in level2_summary.items():
        data["pct"] = data["market_value_cny"] / total_cny if total_cny > 0 else 0

    logger.info("Step 5 done: summary complete")

    # Output results
    logger.info("=" * 60)
    logger.info("=== 总资产 ===")
    logger.info("  原始货币统计:")
    for currency in sorted(currency_summary.keys()):
        total = currency_summary[currency][currency]
        logger.info(f"    {currency}: {total:,.2f} {currency}")
    logger.info(f"  CNY 总市值: {total_cny:,.2f} CNY")

    logger.info("")
    logger.info("=== Level 1 汇总 ===")
    sorted_items = sorted(level1_summary.items(), key=lambda x: -x[1]["market_value_cny"])
    for sub_type, data in sorted_items:
        if data["market_value_cny"] > 0:
            orig_parts = []
            for curr, val in sorted(data["original_values"].items()):
                orig_parts.append(f"{val:,.2f} {curr}")
            orig_str = " + ".join(orig_parts) if orig_parts else "0"
            
            logger.info(
                f"  {sub_type}: {orig_str} → {data['market_value_cny']:,.2f} CNY "
                f"({data['pct'] * 100:.1f}%)"
            )

    logger.info("")
    logger.info("=== Level 2 汇总 (股债商现) ===")
    sorted_l2 = sorted(level2_summary.items(), key=lambda x: -x[1]["market_value_cny"])
    for cls, data in sorted_l2:
        if data["market_value_cny"] > 0:
            orig_parts = []
            for curr, val in sorted(data["original_values"].items()):
                orig_parts.append(f"{val:,.2f} {curr}")
            orig_str = " + ".join(orig_parts) if orig_parts else "0"
            
            logger.info(
                f"  {cls}: {orig_str} → {data['market_value_cny']:,.2f} CNY "
                f"({data['pct'] * 100:.1f}%)"
            )

    # Save to cache
    # Save classified holdings with Level 2 allocation
    classified_holdings = []
    for r in records:
        alloc = level2_allocs.get(r.code, {})
        classified_holdings.append({
            "code": r.code,
            "name": r.name,
            "quantity": r.quantity,
            "price": r.price,
            "market_value": r.market_value,
            "currency": r.currency,
            "market_value_cny": r.market_value_cny,
            "asset_class": r.asset_class,
            "sub_type": r.sub_type,
            "level2_allocation": alloc,
        })
    
    output_path = os.path.join(cache_dir, "classified_holdings.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(classified_holdings, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved classified holdings to {output_path}")

    # Save summary
    summary_output = {
        "snapshot_time": datetime.now().isoformat(),
        "total_market_value_cny": total_cny,
        "currency_summary": {k: dict(v) for k, v in currency_summary.items()},
        "level1_summary": {k: {**v, "original_values": dict(v["original_values"])} for k, v in level1_summary.items()},
        "level2_summary": {k: {**v, "original_values": dict(v["original_values"])} for k, v in level2_summary.items()},
    }
    
    summary_path = os.path.join(cache_dir, "penetration_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_output, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved penetration summary to {summary_path}")

    # List cache files
    logger.info("--- Cache files ---")
    for fname in sorted(os.listdir(cache_dir)):
        sz = os.path.getsize(os.path.join(cache_dir, fname))
        logger.info(f"  {fname}: {sz / 1024:.1f} KB")

