"""Test script: Run Steps 1-5 for Qieman holdings.

Steps:
  1. Parse qieman PDF
  4. Classify records
  4.5 Currency conversion
  5. Fund penetration analysis

Usage:
  cd portfolio_analysis
  python -m test.test_qieman_penetration                        # shallow mode (fast)
  python -m test.test_qieman_penetration --deep --file qieman_20260224.pdf               # deep mode (full stock-level)
  python -m test.test_qieman_penetration --date 20251231 --file qieman_20260224.pdf      # specify holdings date
  python -m test.test_qieman_penetration --deep --date 20251231 --file qieman_20260224.pdf # deep + date
"""

import argparse
import sys
import os
import json
import logging
import yaml

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test")


# Parse command-line arguments
ap = argparse.ArgumentParser(description="Test qieman fund penetration")
ap.add_argument("--deep", action="store_true", help="Enable deep penetration mode")
ap.add_argument("--date", type=str, required=True, help="Holdings date in YYYYMMDD format, e.g. 20251231")
ap.add_argument("--file", type=str, required=True, help="file name")



if __name__ == "__main__":
    args = ap.parse_args()
    deep_mode = args.deep
    date_str = args.date
    file_name = args.file

    # Step 1: Parse qieman only
    from parsers.qieman_parser import QiemanParser

    parser = QiemanParser()
    records = parser.parse(f"data/{file_name}")
    logger.info(f"Step 1 done: {len(records)} records parsed from qieman")

    # Load config
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # Use qieman-specific cache subdirectory
    config["paths"]["cache_dir"] = "cache/qieman"

    # Step 4: Classify
    from analyzers.classifier import classify_records

    records = classify_records(records, config)
    logger.info("Step 4 done: classification complete")

    # Print classification result
    cls_summary: dict[str, int] = {}
    for r in records:
        key = f"{r.asset_class}/{r.sub_type}"
        cls_summary[key] = cls_summary.get(key, 0) + 1
    for k, v in sorted(cls_summary.items()):
        logger.info(f"  {k}: {v} records")

    # Step 4.5: Currency conversion
    fx = config["exchange_rates"]
    for r in records:
        r.market_value_cny = r.market_value * fx.get(r.currency, 1.0)

    # Step 5: Fund penetration
    logger.info(
        f"Starting Step 5: Fund Penetration (deep={deep_mode})..."
    )
    from analyzers.fund_penetration import penetrate_funds

    result = penetrate_funds(records, config, deep_penetration=deep_mode, date=date_str)

    # Print summary
    logger.info("=== PENETRATION RESULT ===")
    logger.info(
        f"Total market value: {result['total_market_value_cny']:,.2f} CNY"
    )

    logger.info("--- Level 1 (个股/基金类型) ---")
    for k, v in sorted(
        result["level1_summary"].items(),
        key=lambda x: -x[1]["market_value_cny"],
    ):
        if v["market_value_cny"] > 0:
            logger.info(
                f"  {k}: {v['market_value_cny']:,.2f} CNY "
                f"({v['pct'] * 100:.1f}%)"
            )

    logger.info("--- Level 2 (股债商) ---")
    for k, v in sorted(
        result["level2_summary"].items(),
        key=lambda x: -x[1]["market_value_cny"],
    ):
        if v["market_value_cny"] > 0:
            logger.info(
                f"  {k}: {v['market_value_cny']:,.2f} CNY "
                f"({v['pct'] * 100:.1f}%)"
            )

    logger.info(
        f"Total penetrated details: {len(result['penetrated_details'])}"
    )

    # List cache files
    cache_dir = config["paths"]["cache_dir"]
    logger.info("--- Cache files ---")
    if os.path.isdir(cache_dir):
        for fname in sorted(os.listdir(cache_dir)):
            sz = os.path.getsize(os.path.join(cache_dir, fname))
            logger.info(f"  {fname}: {sz / 1024:.1f} KB")
    else:
        logger.warning(f"Cache dir not found: {cache_dir}")

