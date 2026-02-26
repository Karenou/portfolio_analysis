"""Cache and utility functions shared across analyzer modules."""

import json
import logging
import os

logger = logging.getLogger(__name__)


def load_json(path: str) -> dict:
    """Load a JSON file, return empty dict if not found."""
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            logger.warning(f"Failed to load JSON from {path}")
    return {}


def save_json(data, path: str):
    """Save data to a JSON file with pretty print."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def safe_pct(val) -> float:
    """Convert percentage value to float.

    Values > 1.0 are treated as raw percentages (e.g. 65.0 -> 0.65).
    """
    if val is None:
        return 0.0
    try:
        v = float(val)
        if v > 1.0:
            return v / 100.0
        return max(v, 0.0)
    except (ValueError, TypeError):
        return 0.0
