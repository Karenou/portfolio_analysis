"""Futu Securities (富途证券) PDF parser.

Futu exports a PDF daily statement. The full holdings are on the page
containing "期末概覽-股票和股票期權" (stocks) and "期末概覽-基金" (funds).

Stock line format (text-based):
  01919(中遠海控) SEHK HKD 2,500 13.9400 - 34,850.00 13,940.00 10,455.00 0.3000

Fund line format (text-based):
  HK0000369188(泰康開泰海外短期債券基金) HKD 14,632.707868 13.9240 2026/02/05 0.00 203,745.82

Special case: FREQ.CVR name spans two lines:
  FREQ.CVR(Frequency Therapeutics Inc US USD 224 0.0000 - 0.00 ...
  Contingent Value Right)
"""

import logging
import re
from typing import Optional

import pdfplumber

from models import BaseParser, HoldingRecord

logger = logging.getLogger(__name__)


def _parse_number(text: str) -> float:
    """Parse a numeric string, removing commas."""
    cleaned = text.strip().replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


# Regex for stock lines: code(name) market currency quantity price multiplier market_value ...
# e.g. "01919(中遠海控) SEHK HKD 2,500 13.9400 - 34,850.00 13,940.00 10,455.00 0.3000"
# e.g. "BRK.B(伯克希爾-B) US USD 6 508.0900 - 3,048.54 ..."
_STOCK_RE = re.compile(
    r"^([A-Za-z0-9.]+)"       # code
    r"\((.+?)\)\s+"            # (name)
    r"(SEHK|US|NYSE|NASDAQ)\s+"  # market
    r"(HKD|USD|CNY)\s+"       # currency
    r"([\d,]+(?:\.\d+)?)\s+"  # quantity
    r"([\d,]+\.\d+)\s+"       # price
    r"-\s+"                    # multiplier placeholder '-'
    r"([\d,]+\.\d+)"          # market_value
)

# Regex for stock lines where the name has a closing ')' that was NOT captured
# because the name contains spaces and wraps to next line.
# e.g. "FREQ.CVR(Frequency Therapeutics Inc US USD 224 0.0000 - 0.00 ..."
_STOCK_PARTIAL_RE = re.compile(
    r"^([A-Za-z0-9.]+)"       # code
    r"\((.+)\s+"               # partial name (no closing paren)
    r"(SEHK|US|NYSE|NASDAQ)\s+"
    r"(HKD|USD|CNY)\s+"
    r"([\d,]+(?:\.\d+)?)\s+"
    r"([\d,]+\.\d+)\s+"
    r"-\s+"
    r"([\d,]+\.\d+)"
)

# Regex for fund lines: code(name) currency quantity price price_date pending market_value
# e.g. "HK0000369188(泰康開泰海外短期債券基金) HKD 14,632.707868 13.9240 2026/02/05 0.00 203,745.82"
_FUND_RE = re.compile(
    r"^([A-Za-z0-9]+)"        # code
    r"\((.+?)\)\s+"            # (name)
    r"(HKD|USD|CNY)\s+"       # currency
    r"([\d,]+\.\d+)\s+"       # quantity
    r"([\d,]+\.\d+)\s+"       # price (NAV)
    r"\d{4}/\d{2}/\d{2}\s+"   # price_date (skip)
    r"[\d,]+\.\d+\s+"         # pending amount (skip)
    r"([\d,]+\.\d+)"          # market_value
)


def _find_overview_page(pdf) -> Optional[object]:
    """Find the page that contains '期末概覽-股票和股票期權'."""
    for page in pdf.pages:
        text = page.extract_text() or ""
        if "期末概覽-股票和股票期權" in text or "期末概览-股票和股票期权" in text:
            return page
    return None


def _parse_stocks_from_text(lines: list[str]) -> list[HoldingRecord]:
    """Parse stock holdings from text lines in the stock section."""
    records = []
    in_stock_section = False
    pending_partial_name: Optional[str] = None

    for line in lines:
        stripped = line.strip()

        # Detect section boundaries
        if "期末概覽-股票和股票期權" in stripped or "期末概览-股票和股票期权" in stripped:
            in_stock_section = True
            continue
        if "期末概覽-基金" in stripped or "期末概览-基金" in stripped:
            in_stock_section = False
            continue
        if stripped.startswith("代碼名稱") or stripped.startswith("代码名称"):
            continue  # Skip header line

        if not in_stock_section:
            continue

        # Handle continuation line for multi-line name (e.g. "Contingent Value Right)")
        if pending_partial_name is not None:
            # This line is a continuation; skip it (record already created)
            pending_partial_name = None
            continue

        # Try full stock line match
        m = _STOCK_RE.match(stripped)
        if m:
            code = m.group(1)
            name = m.group(2)
            market = m.group(3)
            currency = m.group(4)
            quantity = _parse_number(m.group(5))
            price = _parse_number(m.group(6))
            market_value = _parse_number(m.group(7))
            region = "HK" if market == "SEHK" else "US"

            records.append(HoldingRecord(
                code=code,
                name=name,
                quantity=quantity,
                price=price,
                market_value=market_value,
                currency=currency,
                source="futu",
                raw_info={"market": market, "region": region},
            ))
            continue

        # Try partial match (name wraps to next line)
        m2 = _STOCK_PARTIAL_RE.match(stripped)
        if m2:
            code = m2.group(1)
            name = m2.group(2).strip()
            market = m2.group(3)
            currency = m2.group(4)
            quantity = _parse_number(m2.group(5))
            price = _parse_number(m2.group(6))
            market_value = _parse_number(m2.group(7))
            region = "HK" if market == "SEHK" else "US"

            records.append(HoldingRecord(
                code=code,
                name=name,
                quantity=quantity,
                price=price,
                market_value=market_value,
                currency=currency,
                source="futu",
                raw_info={"market": market, "region": region},
            ))
            pending_partial_name = code  # Next line is the name continuation
            continue

    return records


def _parse_funds_from_text(lines: list[str]) -> list[HoldingRecord]:
    """Parse fund holdings from text lines in the fund section."""
    records = []
    in_fund_section = False

    for line in lines:
        stripped = line.strip()

        if "期末概覽-基金" in stripped or "期末概览-基金" in stripped:
            in_fund_section = True
            continue
        if stripped.startswith("代碼名稱") or stripped.startswith("代码名称"):
            continue  # Skip header line
        # End of fund section indicators
        if stripped.startswith("製備日期") or stripped.startswith("制备日期"):
            break

        if not in_fund_section:
            continue

        m = _FUND_RE.match(stripped)
        if m:
            code = m.group(1)
            name = m.group(2)
            currency = m.group(3)
            quantity = _parse_number(m.group(4))
            price = _parse_number(m.group(5))
            market_value = _parse_number(m.group(6))

            records.append(HoldingRecord(
                code=code,
                name=name,
                quantity=quantity,
                price=price,
                market_value=market_value,
                currency=currency,
                source="futu",
                raw_info={"market": "FUND", "region": "HK"},
            ))

    return records


class FutuParser(BaseParser):
    """Parser for Futu Securities PDF holding files."""

    @property
    def platform_name(self) -> str:
        return "futu"

    def parse(self, file_path: str) -> list[HoldingRecord]:
        """Parse Futu PDF and extract holdings from the overview page.

        Extracts from:
        - 期末概覽-股票和股票期權 (stocks & ETFs, 14 items)
        - 期末概覽-基金 (funds, 1 item)
        Total: 15 holdings.
        """
        records: list[HoldingRecord] = []

        with pdfplumber.open(file_path) as pdf:
            overview_page = _find_overview_page(pdf)
            if overview_page is None:
                logger.warning(f"[{self.platform_name}] Could not find overview page in {file_path}")
                return records

            page_num = overview_page.page_number
            logger.info(f"[{self.platform_name}] Found overview on page {page_num}")

            text = overview_page.extract_text() or ""
            lines = text.split("\n")

            stock_records = _parse_stocks_from_text(lines)
            fund_records = _parse_funds_from_text(lines)
            all_records = stock_records + fund_records

            # Filter out records with zero market value
            records = [r for r in all_records if r.market_value != 0]
            skipped = len(all_records) - len(records)
            if skipped:
                logger.info(f"[{self.platform_name}] Skipped {skipped} records with zero market value")

        logger.info(f"[{self.platform_name}] Parsed {len(records)} holdings "
                     f"({len(stock_records)} stocks + {len(fund_records)} funds, "
                     f"after filtering)")
        for r in records:
            logger.debug(f"  {r.code:20s} {r.name:30s} {r.currency} {r.market_value:>12,.2f}")

        return records
