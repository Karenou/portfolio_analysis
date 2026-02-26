"""Alipay (支付宝) PDF parser.

Alipay exports a PDF with a table spanning multiple pages.
Table columns: 序号 | 基金交易账号 | 基金名称 | 基金代码 | 总份额 | 单位净值 | 单位净值日期 | 资产小计
"""

import logging
import re
from typing import Optional

import pdfplumber

from models import BaseParser, HoldingRecord

logger = logging.getLogger(__name__)

# Expected column headers for validation
EXPECTED_HEADERS = {"序号", "基金名称", "基金代码", "总份额", "单位净值", "资产小计"}


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


def _is_header_row(row: list) -> bool:
    """Check if a row is the table header."""
    if not row or len(row) < 6:
        return False
    joined = "".join(_clean_text(c) for c in row if c)
    return "序号" in joined and "基金代码" in joined


def _is_data_row(row: list) -> bool:
    """Check if a row contains valid fund data (starts with a numeric index)."""
    if not row or len(row) < 8:
        return False
    first_cell = _clean_text(row[0])
    return first_cell.isdigit()


class AlipayParser(BaseParser):
    """Parser for Alipay fund holding PDF files."""

    @property
    def platform_name(self) -> str:
        return "alipay"

    def parse(self, file_path: str) -> list[HoldingRecord]:
        """Parse Alipay PDF and extract fund holdings.

        The PDF has a table across pages with 8 columns:
          [0] 序号  [1] 基金交易账号  [2] 基金名称  [3] 基金代码
          [4] 总份额  [5] 单位净值  [6] 单位净值日期  [7] 资产小计
        Page 3 table may lack a header row (continuation from page 2).
        """
        records: list[HoldingRecord] = []

        with pdfplumber.open(file_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        if _is_header_row(row):
                            continue
                        if not _is_data_row(row):
                            # On page 3 the "header" is actually a data row
                            # (the first row is data, not a real header)
                            # Check if col[3] looks like a fund code (6 digits)
                            code_cell = _clean_text(row[3]) if len(row) > 3 else ""
                            if not re.match(r"^\d{6}$", code_cell):
                                continue

                        code = _clean_text(row[3])
                        name = _clean_text(row[2])
                        quantity = _parse_number(row[4])
                        price = _parse_number(row[5])
                        market_value = _parse_number(row[7])

                        if not code or market_value == 0.0:
                            logger.debug(f"Skipping row with empty code or zero value: {row}")
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
