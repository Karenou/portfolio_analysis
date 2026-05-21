#!/usr/bin/env python3
"""Test all parsers against real data files and print results."""

import logging
import sys
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parsers.alipay_parser import AlipayParser
from parsers.qieman_parser import QiemanParser
from parsers.snowball_parser import SnowballParser
from parsers.huatai_parser import HuataiParser
from parsers.futu_parser import FutuParser


def main():
    parsers_files = [
        (AlipayParser(), "data/alipay_20260225.pdf"),
        (QiemanParser(), "data/qieman_20260224.pdf"),
        (SnowballParser(), "data/snowball_20260224.pdf"),
        (HuataiParser(), "data/huatai_20260225.xlsx"),
        (FutuParser(), "data/futu_20260206.pdf"),
    ]

    total = 0
    for parser, path in parsers_files:
        print(f"\n{'=' * 60}")
        print(f"  {parser.platform_name.upper()} - {path}")
        print(f"{'=' * 60}")

        records = parser.parse(path)
        total += len(records)

        for i, r in enumerate(records, 1):
            name_display = r.name[:18].ljust(18)
            print(
                f"  {i:>3d}. {r.code:>8s}  {name_display}  "
                f"qty={r.quantity:>12.2f}  price={r.price:>10.4f}  "
                f"mv={r.market_value:>12.2f}  {r.currency}"
            )

        print(f"  --- Subtotal: {len(records)} records ---")

    print(f"\n{'=' * 60}")
    print(f"  GRAND TOTAL: {total} records across {len(parsers_files)} platforms")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
