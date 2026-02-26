#!/usr/bin/env python3
"""Inspect data files - Extract and display table structures from PDF and Excel files.

This standalone script scans the data/ directory, extracts table structures from
each file, and prints them in a human-readable format for verification.

Usage:
    python inspect_data.py                  # Scan default data/ directory
    python inspect_data.py --data-dir path  # Scan custom directory
    python inspect_data.py --file path      # Inspect a single file
"""

import argparse
import logging
import os
import re
import sys
from typing import Optional

import pandas as pd

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("inspect_data")

SUPPORTED_EXTENSIONS = {".pdf", ".xlsx", ".xls"}
SEPARATOR = "=" * 80
SUB_SEPARATOR = "-" * 60


# ---------------------------------------------------------------------------
# Futu-specific cash balance extraction
# ---------------------------------------------------------------------------
def extract_futu_cash_balance(page) -> Optional[dict]:
    """Extract cash balance from Futu PDF page 4.

    Expected format in text:
        期末資產淨值總覽 合計(HKD) 港幣資產 美元資產 人民幣資產 日元資產 新加坡元資產
        現金結餘 73,566.13 46,714.37 3,436.63 0.00 0.00 0.00

    Returns:
        dict with keys: hkd_cash, usd_cash (original currency amounts)
        or None if not found.
    """
    text = page.extract_text() or ""
    lines = text.split("\n")

    # Find the cash balance line
    for line in lines:
        # Match: 現金結餘 followed by numbers
        # Format: 現金結餘 合計(HKD) 港幣 美元 人民幣 日元 新加坡元
        if "現金結餘" in line or "现金结余" in line:
            # Extract all numbers from the line
            numbers = re.findall(r"[\d,]+\.\d{2}", line)
            if len(numbers) >= 3:
                # numbers[1] = HKD, numbers[2] = USD
                hkd_cash = float(numbers[1].replace(",", ""))
                usd_cash = float(numbers[2].replace(",", ""))
                return {
                    "hkd_cash": hkd_cash,
                    "usd_cash": usd_cash,
                }
    return None


# ---------------------------------------------------------------------------
# PDF inspection
# ---------------------------------------------------------------------------
def inspect_pdf(filepath: str, max_rows: int = 10) -> None:
    """Extract and display all tables from a PDF file using pdfplumber.

    Args:
        filepath: Path to the PDF file.
        max_rows: Maximum number of rows to display per table.
    """
    try:
        import pdfplumber
    except ImportError:
        logger.error("pdfplumber is not installed. Run: pip install pdfplumber")
        return

    logger.info(f"Opening PDF: {filepath}")
    try:
        with pdfplumber.open(filepath) as pdf:
            total_pages = len(pdf.pages)
            logger.info(f"  Total pages: {total_pages}")

            # Check if this is a Futu PDF (check filename)
            is_futu = "futu" in os.path.basename(filepath).lower()

            tables_found = 0
            for page_idx, page in enumerate(pdf.pages):
                page_num = page_idx + 1
                tables = page.extract_tables()

                # Futu-specific: extract cash balance from page 4
                if is_futu and page_num == 4:
                    cash_balance = extract_futu_cash_balance(page)
                    if cash_balance:
                        logger.info(f"  Page {page_num}: Futu Cash Balance detected:")
                        logger.info(f"    HKD Cash: {cash_balance['hkd_cash']:,.2f} HKD")
                        logger.info(f"    USD Cash: {cash_balance['usd_cash']:,.2f} USD")

                if not tables:
                    # Also try extracting text to show non-tabular content
                    text = page.extract_text()
                    if text and text.strip():
                        logger.info(f"  Page {page_num}: No tables found, raw text preview:")
                        for line in text.strip().split("\n")[:5]:
                            logger.info(f"    | {line}")
                        if len(text.strip().split("\n")) > 5:
                            logger.info(f"    | ... ({len(text.strip().split(chr(10)))} lines total)")
                    else:
                        logger.info(f"  Page {page_num}: Empty or no extractable content")
                    continue

                for tbl_idx, table in enumerate(tables):
                    tables_found += 1
                    tbl_num = tbl_idx + 1
                    total_rows = len(table)

                    logger.info(f"  Page {page_num}, Table {tbl_num}: {total_rows} rows x {len(table[0]) if table else 0} cols")

                    # Display header row
                    if total_rows > 0:
                        header = table[0]
                        logger.info(f"    Header: {header}")

                    # Display data rows (up to max_rows)
                    display_rows = min(max_rows, total_rows - 1)
                    for row_idx in range(1, display_rows + 1):
                        logger.info(f"    Row {row_idx}: {table[row_idx]}")

                    if total_rows - 1 > max_rows:
                        logger.info(f"    ... ({total_rows - 1 - max_rows} more data rows)")

            logger.info(f"  Summary: {tables_found} tables extracted from {total_pages} pages")

    except Exception as e:
        logger.error(f"  Failed to read PDF: {e}")


# ---------------------------------------------------------------------------
# Excel inspection
# ---------------------------------------------------------------------------
def inspect_excel(filepath: str, max_rows: int = 10) -> None:
    """Extract and display structure from an Excel file.

    Args:
        filepath: Path to the Excel file.
        max_rows: Maximum number of rows to display per sheet.
    """
    try:
        import openpyxl
    except ImportError:
        logger.error("openpyxl is not installed. Run: pip install openpyxl")
        return

    logger.info(f"Opening Excel: {filepath}")
    try:
        xls = pd.ExcelFile(filepath, engine="openpyxl")
        sheet_names = xls.sheet_names
        logger.info(f"  Sheet names: {sheet_names}")

        for sheet_name in sheet_names:
            logger.info(f"  {SUB_SEPARATOR}")
            logger.info(f"  Sheet: '{sheet_name}'")

            # Read the sheet (all data, no header assumption first)
            df_raw = pd.read_excel(filepath, sheet_name=sheet_name, header=None, engine="openpyxl")
            total_rows, total_cols = df_raw.shape
            logger.info(f"    Raw shape: {total_rows} rows x {total_cols} cols")

            if total_rows == 0:
                logger.info(f"    (empty sheet)")
                continue

            # Show first few rows to help identify header location
            display_count = min(max_rows + 3, total_rows)  # +3 to show potential header area
            logger.info(f"    First {display_count} rows (raw, 0-indexed):")
            for idx in range(display_count):
                row_data = df_raw.iloc[idx].tolist()
                logger.info(f"      Row {idx}: {row_data}")

            if total_rows > display_count:
                logger.info(f"      ... ({total_rows - display_count} more rows)")

            # Also try reading with auto-detected header for a cleaner view
            try:
                df = pd.read_excel(filepath, sheet_name=sheet_name, engine="openpyxl")
                logger.info(f"    Auto-header columns: {df.columns.tolist()}")
                logger.info(f"    Dtypes: {df.dtypes.to_dict()}")
            except Exception:
                pass

    except Exception as e:
        logger.error(f"  Failed to read Excel: {e}")


# ---------------------------------------------------------------------------
# Main dispatcher
# ---------------------------------------------------------------------------
def inspect_file(filepath: str, max_rows: int = 10) -> None:
    """Inspect a single file based on its extension.

    Args:
        filepath: Path to the file.
        max_rows: Maximum rows to display per table.
    """
    ext = os.path.splitext(filepath)[1].lower()
    filename = os.path.basename(filepath)

    print(f"\n{SEPARATOR}")
    print(f"  FILE: {filename}")
    print(f"  PATH: {filepath}")
    print(f"  SIZE: {os.path.getsize(filepath) / 1024:.1f} KB")
    print(f"  TYPE: {ext}")
    print(SEPARATOR)

    if ext == ".pdf":
        inspect_pdf(filepath, max_rows)
    elif ext in (".xlsx", ".xls"):
        inspect_excel(filepath, max_rows)
    else:
        logger.warning(f"Unsupported file type: {ext}")


def scan_directory(data_dir: str, max_rows: int = 10) -> None:
    """Scan a directory and inspect all supported files.

    Args:
        data_dir: Path to the data directory.
        max_rows: Maximum rows to display per table.
    """
    if not os.path.isdir(data_dir):
        logger.error(f"Directory not found: {data_dir}")
        sys.exit(1)

    files = sorted(
        f for f in os.listdir(data_dir)
        if os.path.isfile(os.path.join(data_dir, f))
        and os.path.splitext(f)[1].lower() in SUPPORTED_EXTENSIONS
    )

    if not files:
        logger.warning(f"No PDF or Excel files found in {data_dir}")
        return

    logger.info(f"Found {len(files)} data file(s) in '{data_dir}':")
    for f in files:
        logger.info(f"  - {f}")

    for filename in files:
        filepath = os.path.join(data_dir, filename)
        inspect_file(filepath, max_rows)

    # Final summary
    print(f"\n{SEPARATOR}")
    print(f"  INSPECTION COMPLETE: {len(files)} files processed")
    print(SEPARATOR)


def main():
    parser = argparse.ArgumentParser(
        description="Inspect PDF and Excel data files - extract and display table structures."
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Path to the data directory (default: data/)",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Path to a single file to inspect (overrides --data-dir)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=10,
        help="Max rows to display per table (default: 10)",
    )
    args = parser.parse_args()

    if args.file:
        if not os.path.isfile(args.file):
            logger.error(f"File not found: {args.file}")
            sys.exit(1)
        inspect_file(args.file, args.max_rows)
    else:
        scan_directory(args.data_dir, args.max_rows)


if __name__ == "__main__":
    main()
