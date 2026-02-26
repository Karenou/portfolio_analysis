"""Huatai Securities (华泰证券) Excel parser.

Huatai exports an Excel file (Sheet1) with multiple sections:
  - Personal info area (rows 0-6)
  - Total assets area (rows 7-12)
  - Transaction details area (rows 14-21)
  - **Stock holdings** (starts at row with "股票持仓", header row follows)
    Columns: 日期 | 股东账号 | - | 股票代码 | 股票名称 | - | 持仓数 | 市值 | - | 成本价 | 现价 | 持仓盈亏 | 币种
  - **Fund holdings** (starts at row with "基金持仓", header row follows)
    Columns: 基金代码 | 基金名称 | - | 基金份额 | - | 单位净值 | - | 参考市值 | - | 摊薄成本价 | - | 持仓盈亏 | -
"""

import logging
import math
import re
from typing import Optional

import pandas as pd

from models import BaseParser, HoldingRecord

logger = logging.getLogger(__name__)

# Securities to keep but classify specially
CASH_CODES = {"888880"}  # 标准券 (standard bond collateral for repo) -> cash


def _is_nan(val) -> bool:
    """Check if a value is NaN or None."""
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    return False


def _clean_str(val) -> str:
    """Convert cell value to a clean string."""
    if _is_nan(val):
        return ""
    s = str(val).strip()
    # Remove newlines
    s = re.sub(r"\s+", "", s)
    return s


def _parse_number(val) -> float:
    """Parse numeric value from cell."""
    if _is_nan(val):
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    cleaned = _clean_str(val).replace(",", "").replace("，", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_stock_code(val) -> str:
    """Parse stock/ETF code, ensuring it's zero-padded to 6 digits."""
    s = _clean_str(val)
    if s.isdigit():
        return s.zfill(6)
    return s


def _parse_nav_value(val) -> float:
    """Parse NAV value that may contain date info like '1.00000000[20260213]'."""
    s = _clean_str(val)
    # Remove bracketed date info
    s = re.sub(r"\[.*?\]", "", s)
    try:
        return float(s)
    except ValueError:
        return 0.0


class HuataiParser(BaseParser):
    """Parser for Huatai Securities Excel holding files."""

    @property
    def platform_name(self) -> str:
        return "huatai"

    def parse(self, file_path: str) -> list[HoldingRecord]:
        """Parse Huatai Excel and extract stock + fund holdings."""
        df = pd.read_excel(file_path, sheet_name=0, header=None, engine="openpyxl")
        records: list[HoldingRecord] = []

        # Locate section start rows
        stock_start = None
        fund_start = None

        for idx, row in df.iterrows():
            cell = _clean_str(row.iloc[0])
            if cell == "股票持仓":
                stock_start = idx
            elif cell == "基金持仓":
                fund_start = idx

        if stock_start is not None:
            stock_records = self._parse_stock_section(df, stock_start)
            records.extend(stock_records)
        else:
            logger.warning(f"[{self.platform_name}] Stock holdings section not found")

        if fund_start is not None:
            fund_records = self._parse_fund_section(df, fund_start)
            records.extend(fund_records)
        else:
            logger.warning(f"[{self.platform_name}] Fund holdings section not found")

        logger.info(f"[{self.platform_name}] Parsed {len(records)} holdings "
                     f"(stocks: {len(records) - (len(records) if fund_start is None else 0)})")
        return records

    def _parse_stock_section(self, df: pd.DataFrame, start_row: int) -> list[HoldingRecord]:
        """Parse the stock holdings section.

        Layout (0-indexed columns within the section):
          [0] 日期  [1] 股东账号  [2] -  [3] 股票代码  [4] 股票名称
          [5] -  [6] 持仓数  [7] 市值  [8] -  [9] 成本价
          [10] 现价  [11] 持仓盈亏  [12] 币种
        """
        records: list[HoldingRecord] = []
        # Data rows start 3 rows after section header (header + empty row + data)
        data_start = start_row + 3

        for idx in range(data_start, len(df)):
            row = df.iloc[idx]
            first_cell = _clean_str(row.iloc[0])

            # Stop at empty rows or summary rows
            if first_cell in ("", "合计"):
                if first_cell == "合计":
                    break
                # Check if this is just a gap row or end of section
                next_cell = _clean_str(row.iloc[1]) if len(row) > 1 else ""
                if not next_cell:
                    break
                continue

            code = _parse_stock_code(row.iloc[3])
            name = _clean_str(row.iloc[4])
            quantity = _parse_number(row.iloc[6])
            market_value = _parse_number(row.iloc[7])
            cost_price = _parse_number(row.iloc[9])
            current_price = _parse_number(row.iloc[10])
            currency_str = _clean_str(row.iloc[12])

            # Mark special entries for later classification
            is_special_cash = code in CASH_CODES
            if is_special_cash:
                logger.debug(f"Found cash-equivalent code: {code} ({name})")

            # Map currency
            currency = "CNY"
            if "美元" in currency_str:
                currency = "USD"
            elif "港币" in currency_str:
                currency = "HKD"

            record = HoldingRecord(
                code=code,
                name=name,
                quantity=quantity,
                price=current_price,
                market_value=market_value,
                currency=currency,
                source=self.platform_name,
                raw_info={"cost_price": cost_price, "is_cash_equivalent": is_special_cash},
            )
            records.append(record)

        logger.info(f"[{self.platform_name}] Stock section: {len(records)} records")
        return records

    def _parse_fund_section(self, df: pd.DataFrame, start_row: int) -> list[HoldingRecord]:
        """Parse the fund holdings section.

        Layout (0-indexed columns):
          [0] 基金代码  [1] 基金名称  [2] -  [3] 基金份额  [4] -
          [5] 单位净值  [6] -  [7] 参考市值  [8] -
          [9] 摊薄成本价  [10] -  [11] 持仓盈亏  [12] -
        """
        records: list[HoldingRecord] = []
        # Data rows start 3 rows after section header
        data_start = start_row + 3

        for idx in range(data_start, len(df)):
            row = df.iloc[idx]
            first_cell = _clean_str(row.iloc[0])

            if first_cell in ("", "合计"):
                if first_cell == "合计":
                    break
                next_cell = _clean_str(row.iloc[1]) if len(row) > 1 else ""
                if not next_cell:
                    break
                continue

            code = _clean_str(row.iloc[0])
            name = _clean_str(row.iloc[1])
            quantity = _parse_number(row.iloc[3])
            price = _parse_nav_value(row.iloc[5])
            market_value = _parse_number(row.iloc[7])

            if not code or market_value == 0.0:
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

        logger.info(f"[{self.platform_name}] Fund section: {len(records)} records")
        return records
