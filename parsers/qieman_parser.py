"""Qieman (且慢) PDF parser.

Qieman exports a PDF with fund holdings across multiple pages.
Table columns: 基金代码 | 基金名称 | 基金份额 | 基金净值 | 净值日期 | 基金市值（元）

Page 1 has investor info rows before the actual data header at row index ~2.
Pages 2-3 are continuations without header rows.
Last row on page 3 is a summary row: "人民币合计（SUM）：".
"""

import logging
import re
from typing import Optional

import pdfplumber

from models import BaseParser, HoldingRecord

logger = logging.getLogger(__name__)


def _clean_text(text: Optional[str]) -> str:
    """Remove newlines and extra whitespace from cell text."""
    if text is None:
        return ""
    return re.sub(r"\s+", "", text.strip())


def _parse_number(text: Optional[str]) -> float:
    """Parse a numeric string, removing commas and whitespace."""
    if text is None:
        return 0.0
    cleaned = _clean_text(text).replace(",", "").replace("，", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _is_fund_code(text: str) -> bool:
    """Check if text looks like a 6-digit fund code."""
    return bool(re.match(r"^\d{6}$", text))


def _is_header_or_meta_row(row: list) -> bool:
    """Check if a row is a header, bilingual label, or investor info row."""
    if not row or len(row) < 6:
        return True
    first_cell = _clean_text(row[0])
    # Skip Chinese/English header rows
    if "基金代码" in first_cell or "FundCode" in first_cell:
        return True
    # Skip investor info rows
    if "投资人" in first_cell or "截止日期" in first_cell:
        return True
    return False


def _is_summary_row(row: list) -> bool:
    """Check if a row is the summary total row."""
    if not row:
        return False
    first_cell = _clean_text(row[0]) if row[0] else ""
    return "合计" in first_cell or "SUM" in first_cell


class QiemanParser(BaseParser):
    """Parser for Qieman fund holding PDF files."""

    @property
    def platform_name(self) -> str:
        return "qieman"

    def parse(self, file_path: str) -> list[HoldingRecord]:
        """Parse Qieman PDF and extract fund holdings.

        The PDF has 6 columns:
          [0] 基金代码  [1] 基金名称  [2] 基金份额
          [3] 基金净值  [4] 净值日期  [5] 基金市值（元）
        """
        records: list[HoldingRecord] = []

        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if _is_header_or_meta_row(row):
                            continue
                        if _is_summary_row(row):
                            continue

                        code = _clean_text(row[0])
                        if not _is_fund_code(code):
                            continue

                        name = _clean_text(row[1])
                        quantity = _parse_number(row[2])
                        price = _parse_number(row[3])
                        market_value = _parse_number(row[5])

                        if market_value == 0.0:
                            logger.debug(f"Skipping row with zero market value: {row}")
                            continue

                        record = HoldingRecord(
                            code=code,
                            name=name,
                            quantity=quantity,
                            price=price,
                            market_value=market_value,
                            currency="CNY",
                            source=self.platform_name,
                        )
                        records.append(record)

        logger.info(f"[{self.platform_name}] Parsed {len(records)} fund holdings")
        return records
