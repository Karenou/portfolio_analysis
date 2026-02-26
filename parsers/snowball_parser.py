"""Snowball (雪球) PDF parser.

Snowball exports a PDF with two types of fund tables:
1. Page 1 - Self-selected funds (自选基金): 6 columns
   基金代码 | 基金名称 | 持有份额 | 单位净值(元) | 单位净值日期 | 持有市值(元)
2. Pages 2-4 - Advisor portfolios (投顾组合): 9 columns
   投顾代码 | 投顾名称 | 基金代码 | 基金名称 | 持有份额 | 单位净值(元) | 单位净值日期 | 持有市值(元) | 投顾管理人
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


def _is_header_row(row: list) -> bool:
    """Check if a row is a Chinese/English header row."""
    if not row:
        return True
    joined = "".join(_clean_text(c) for c in row if c)
    return ("基金代码" in joined or "FundCode" in joined
            or "投顾代码" in joined or "AdviserCode" in joined)


def _parse_6col_row(row: list) -> Optional[HoldingRecord]:
    """Parse a 6-column self-selected fund row.

    Columns: [0] 基金代码 [1] 基金名称 [2] 持有份额
             [3] 单位净值(元) [4] 单位净值日期 [5] 持有市值(元)
    """
    if len(row) < 6:
        return None

    code = _clean_text(row[0])
    if not _is_fund_code(code):
        return None

    name = _clean_text(row[1])
    quantity = _parse_number(row[2])
    price = _parse_number(row[3])
    market_value = _parse_number(row[5])

    if market_value == 0.0:
        return None

    return HoldingRecord(
        code=code,
        name=name,
        quantity=quantity,
        price=price,
        market_value=market_value,
        currency="CNY",
        source="snowball",
        raw_info={"account_type": "self_selected"},
    )


def _parse_9col_row(row: list) -> Optional[HoldingRecord]:
    """Parse a 9-column advisor portfolio row.

    Columns: [0] 投顾代码 [1] 投顾名称 [2] 基金代码 [3] 基金名称
             [4] 持有份额 [5] 单位净值(元) [6] 单位净值日期
             [7] 持有市值(元) [8] 投顾管理人
    """
    if len(row) < 9:
        return None

    code = _clean_text(row[2])
    if not _is_fund_code(code):
        return None

    name = _clean_text(row[3])
    quantity = _parse_number(row[4])
    price = _parse_number(row[5])
    market_value = _parse_number(row[7])
    advisor_code = _clean_text(row[0])
    advisor_name = _clean_text(row[1])

    if market_value == 0.0:
        return None

    return HoldingRecord(
        code=code,
        name=name,
        quantity=quantity,
        price=price,
        market_value=market_value,
        currency="CNY",
        source="snowball",
        raw_info={
            "account_type": "advisor",
            "advisor_code": advisor_code,
            "advisor_name": advisor_name,
            "advisor_manager": _clean_text(row[8]),
        },
    )


class SnowballParser(BaseParser):
    """Parser for Snowball fund holding PDF files."""

    @property
    def platform_name(self) -> str:
        return "snowball"

    def parse(self, file_path: str) -> list[HoldingRecord]:
        """Parse Snowball PDF and extract fund holdings.

        Handles both 6-column (self-selected) and 9-column (advisor) table layouts.
        """
        records: list[HoldingRecord] = []

        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table:
                        continue

                    # Determine table width from the first non-empty row
                    col_count = max(len(r) for r in table if r)

                    for row in table:
                        if _is_header_row(row):
                            continue

                        record = None
                        if col_count >= 9:
                            record = _parse_9col_row(row)
                        elif col_count >= 6:
                            record = _parse_6col_row(row)

                        if record:
                            records.append(record)

        logger.info(f"[{self.platform_name}] Parsed {len(records)} fund holdings")
        return records
