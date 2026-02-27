"""Core data models for portfolio analysis."""

from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Optional


@dataclass
class HoldingRecord:
    """Unified holding record across all platforms."""
    code: str                          # Security code (e.g. '000001', '00700')
    name: str                          # Security name
    quantity: float                     # Shares or fund units
    price: float                       # Current price or NAV
    market_value: float                # Total market value in original currency
    currency: str = "CNY"              # CNY / HKD / USD
    source: str = ""                   # Platform name (alipay / qieman / snowball / huatai / futu)
    asset_class: str = ""              # Major class: equity / bond / commodity / money / other
    sub_type: str = ""                 # Sub type: stock_cn / stock_hk / equity_fund / bond_fund / etc.
    market_value_cny: float = 0.0      # Market value converted to CNY
    is_estimated: bool = False         # Whether fund penetration uses default ratios
    raw_info: dict = field(default_factory=dict)  # Extra raw fields from source


@dataclass
class FundAllocation:
    """Fund asset allocation breakdown after penetration."""
    code: str
    name: str
    total_market_value_cny: float
    equity_pct: float = 0.0           # Stock portion percentage
    bond_pct: float = 0.0             # Bond portion percentage
    cash_pct: float = 0.0             # Cash portion percentage
    commodity_pct: float = 0.0        # Commodity portion percentage
    other_pct: float = 0.0            # Other portion percentage
    is_estimated: bool = False         # Whether using default ratios


class BaseParser(ABC):
    """Abstract base class for platform-specific parsers."""

    @abstractmethod
    def parse(self, file_path: str) -> list[HoldingRecord]:
        """Parse a holding file and return a list of HoldingRecord.

        Args:
            file_path: Path to the holding file (PDF or Excel).

        Returns:
            List of parsed HoldingRecord objects.
        """
        pass

    @property
    @abstractmethod
    def platform_name(self) -> str:
        """Return the platform identifier string."""
        pass
