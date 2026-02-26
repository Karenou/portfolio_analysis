"""Multi-dimension Aggregation - Step 6.

Reads cached penetration results from Step 5, performs four-dimension
aggregation WITHOUT calling any akshare API:

  Dim 1: Asset class (equity / bond / commodity / cash / other)
  Dim 2: Region (CN / HK / US / OTHER)
  Dim 3: Industry (SW L1 classification)
  Dim 4: Risk level (high / medium / low)

Outputs: cache/aggregation_result.json
"""

import logging
import os
from collections import defaultdict

from analyzers.cache_utils import load_json, save_json

logger = logging.getLogger(__name__)


def _group_by(details: list[dict], key: str) -> dict[str, float]:
    """Sum market_value_cny grouped by a given key."""
    groups: dict[str, float] = defaultdict(float)
    for d in details:
        groups[d.get(key, "未知")] += d.get("market_value_cny", 0)
    return dict(groups)


def _to_pct_table(groups: dict[str, float], total: float) -> list[dict]:
    """Convert {label: mv} dict to sorted list of {label, mv, pct}."""
    rows = []
    for label, mv in sorted(groups.items(), key=lambda x: -x[1]):
        rows.append({
            "label": label,
            "market_value_cny": round(mv, 2),
            "pct": round(mv / total, 4) if total else 0,
        })
    return rows


def aggregate_all(penetration_result: dict, config: dict) -> dict:
    """Run all four aggregation dimensions.

    Args:
        penetration_result: Return value from penetrate_funds(),
            or loaded from cache/penetrated_holdings.json.
        config: Configuration dict from config.yaml.

    Returns:
        Aggregation result dict saved to cache/aggregation_result.json.
    """
    cache_dir = config["paths"]["cache_dir"]

    # If penetration_result is None or empty, load from cache
    if not penetration_result or "penetrated_details" not in penetration_result:
        path = os.path.join(cache_dir, "penetrated_holdings.json")
        penetration_result = load_json(path)
        if not penetration_result:
            logger.error("No penetrated holdings data found!")
            return {}

    details = penetration_result.get("penetrated_details", [])
    total_mv = penetration_result.get("total_market_value_cny", 0)
    if total_mv <= 0:
        total_mv = sum(d.get("market_value_cny", 0) for d in details)

    logger.info(f"Aggregating {len(details)} penetrated detail rows, "
                f"total={total_mv:,.0f} CNY")

    # --- Dimension 1: True asset class (equity/bond/commodity/cash/other) ---
    dim1 = _group_by(details, "true_asset_class")
    dim1_table = _to_pct_table(dim1, total_mv)

    # --- Dimension 2: Region (CN/HK/US/OTHER) ---
    dim2 = _group_by(details, "region")
    dim2_table = _to_pct_table(dim2, total_mv)

    # --- Dimension 3: Industry (SW L1) - only equity portion ---
    equity_details = [d for d in details
                      if d.get("true_asset_class") == "equity"]
    equity_total = sum(d.get("market_value_cny", 0)
                       for d in equity_details)
    dim3_raw = _group_by(equity_details, "industry_l1")
    dim3_table = _to_pct_table(dim3_raw, equity_total)

    # --- Dimension 4: Risk level (high/medium/low) ---
    dim4 = _group_by(details, "risk_level")
    dim4_table = _to_pct_table(dim4, total_mv)

    # --- Cross-dimension: original_type breakdown ---
    dim_orig = _group_by(details, "original_type")
    dim_orig_table = _to_pct_table(dim_orig, total_mv)

    # --- Merge duplicate stocks across platforms ---
    stock_agg = _merge_same_stock(equity_details)

    result = {
        "total_market_value_cny": total_mv,
        "dim_asset_class": dim1_table,
        "dim_region": dim2_table,
        "dim_industry": dim3_table,
        "dim_industry_equity_total": equity_total,
        "dim_risk_level": dim4_table,
        "dim_original_type": dim_orig_table,
        "top_stock_holdings": stock_agg[:30],
        "level1_summary": penetration_result.get("level1_summary", {}),
        "level2_summary": penetration_result.get("level2_summary", {}),
    }

    out_path = os.path.join(cache_dir, "aggregation_result.json")
    save_json(result, out_path)

    # Log summaries
    _log_dim("Asset Class", dim1_table)
    _log_dim("Region", dim2_table)
    _log_dim("Industry (Top 10)", dim3_table[:10])
    _log_dim("Risk Level", dim4_table)

    logger.info(f"Aggregation saved to {out_path}")
    return result


def _merge_same_stock(equity_details: list[dict]) -> list[dict]:
    """Merge same stock code across platforms, sum market values."""
    merged: dict[str, dict] = {}
    for d in equity_details:
        code = d.get("code", "")
        if code in merged:
            merged[code]["market_value_cny"] += d.get(
                "market_value_cny", 0)
            # Track sources
            src = d.get("source", "")
            if src and src not in merged[code].get("sources", []):
                merged[code].setdefault("sources", []).append(src)
        else:
            merged[code] = {
                "code": code,
                "name": d.get("name", ""),
                "market_value_cny": d.get("market_value_cny", 0),
                "region": d.get("region", ""),
                "industry_l1": d.get("industry_l1", ""),
                "sources": [d.get("source", "")],
            }

    rows = sorted(merged.values(),
                  key=lambda x: -x["market_value_cny"])
    # Add pct
    total = sum(r["market_value_cny"] for r in rows) or 1
    for r in rows:
        r["pct"] = round(r["market_value_cny"] / total, 4)
        r["market_value_cny"] = round(r["market_value_cny"], 2)
    return rows


def _log_dim(name: str, table: list[dict]):
    """Log a dimension summary."""
    parts = [f"{r['label']}={r['pct']:.1%}" for r in table]
    logger.info(f"[{name}] {' | '.join(parts)}")
