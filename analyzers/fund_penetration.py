"""Fund Penetration Analysis - Step 5.

Sub-steps:
  5.1 - Fund asset allocation (equity/bond/cash/commodity split)
  5.2 - Fund holding detail (top-N stocks, index constituents)
  5.3 - Multi-dimension data collection (industry, region, volatility)
  5.4 - Penetrated result summary with all dimension labels -> cache
"""

import logging
import os
import re
import time
from datetime import datetime, timedelta
from typing import Optional

from models import HoldingRecord, FundAllocation
from analyzers.cache_utils import load_json, save_json, safe_pct

logger = logging.getLogger(__name__)

_API_DELAY = 0.3

# Well-known index name -> code mapping
_INDEX_MAP = {
    "沪深300": "000300", "中证500": "000905", "中证1000": "000852",
    "上证50": "000016", "创业板": "399006", "科创50": "000688",
    "中证红利": "000922", "中证全指": "000985", "国证2000": "399303",
    "中证800": "000906", "中证A50": "930050", "中证A500": "000510",
}


# ===========================================================================
# 5.1  Fund Asset Allocation
# ===========================================================================

def _get_latest_season(df, season_col: str = "季度"):
    """Return the subset of *df* that belongs to the latest season."""
    if season_col not in df.columns:
        return df
    seasons = sorted(df[season_col].unique())
    return df[df[season_col] == seasons[-1]]


def _query_fund_allocation_api(
    code: str, date: str = "", *, is_etf_linked: bool = False,
) -> Optional[dict]:
    """Query fund asset allocation via Xueqiu (fund_individual_detail_hold_xq).

    Returns a normalised dict with equity/bond/cash/commodity/other ratios
    that sum to 1.0.  For ETF-linked funds the "其他" category (which
    represents the held ETF shares) is re-classified as equity.

    Args:
        code: Fund code.
        date: Date string in YYYYMMDD format.
        is_etf_linked: If True, treat "其他" as equity.
    """
    try:
        import akshare as ak
        query_date = date if len(date) == 8 else datetime.now().strftime("%Y%m%d")
        df = ak.fund_individual_detail_hold_xq(
            symbol=code, date=query_date)
        if df is None or df.empty:
            return None

        # Parse API result: columns = ["资产类型", "仓位占比"]
        raw: dict[str, float] = {}
        for _, row in df.iterrows():
            raw[str(row["资产类型"]).strip()] = float(row["仓位占比"])

        eq = raw.get("股票", 0.0)
        bd = raw.get("债券", 0.0)
        cash = raw.get("现金", 0.0)
        other = raw.get("其他", 0.0)
        # API does not return a commodity row; keep 0
        commodity = 0.0

        # ETF-linked: "其他" is actually the ETF position → equity
        if is_etf_linked:
            eq += other
            other = 0.0

        # Normalise so that ratios sum to 1.0 (bond funds may exceed 100% due to leverage)
        total = eq + bd + cash + commodity + other
        if total <= 0:
            return None
        # API returns percentages (e.g. 股票=94.01 means 94.01%), divide by total to normalize
        eq_r = eq / total
        bd_r = bd / total
        cash_r = cash / total
        commodity_r = commodity / total
        other_r = other / total

        return {
            "equity_pct": eq_r, "bond_pct": bd_r,
            "cash_pct": cash_r, "commodity_pct": commodity_r,
            "other_pct": other_r,
        }
    except Exception as e:
        logger.debug(f"fund_individual_detail_hold_xq failed for {code}: {e}")
        return None


def _default_allocation(sub_type: str, config: dict) -> dict:
    """Get default allocation from config by sub_type."""
    defaults = config.get("default_fund_allocation", {})
    key_map = {
        "equity_fund": "equity_fund", "bond_fund": "bond_fund",
        "mixed_fund": "hybrid_fund", "money_fund": "money_fund",
        "index_fund": "index_fund", "etf": "index_fund",
        "qdii_fund": "qdii", "commodity_fund": "commodity_fund",
        "fof_fund": "fof",
    }
    a = defaults.get(key_map.get(sub_type, "other"),
                     defaults.get("other", {}))
    return {"equity_pct": a.get("equity_pct", 0.5),
            "bond_pct": a.get("bond_pct", 0.2),
            "cash_pct": a.get("cash_pct", 0.2),
            "commodity_pct": a.get("commodity_pct", 0.0),
            "other_pct": a.get("other_pct", 0.1)}


def _penetrate_allocation(
    fund_records: list[HoldingRecord], config: dict, cache_dir: str,
    date: str = "",
) -> dict[str, FundAllocation]:
    """Step 5.1: Query fund asset allocation for each fund."""
    path = os.path.join(cache_dir, "fund_allocation.json")
    cached = load_json(path)
    result: dict[str, FundAllocation] = {}

    for rec in fund_records:
        code = rec.code
        # Money fund = 100% cash
        if rec.sub_type == "money_fund":
            result[code] = FundAllocation(
                code=code, name=rec.name,
                total_market_value_cny=rec.market_value_cny,
                cash_pct=1.0)
            continue

        # Commodity fund = 100% commodity, no API needed
        if rec.sub_type == "commodity_fund":
            result[code] = FundAllocation(
                code=code, name=rec.name,
                total_market_value_cny=rec.market_value_cny,
                commodity_pct=1.0)
            logger.info(f"Fund {code} ({rec.name}): commodity_fund -> 100% commodity")
            continue

        # Cache hit
        if code in cached:
            d = cached[code]
            result[code] = FundAllocation(
                code=code, name=rec.name,
                total_market_value_cny=rec.market_value_cny,
                equity_pct=d["equity_pct"], bond_pct=d["bond_pct"],
                cash_pct=d["cash_pct"],
                commodity_pct=d.get("commodity_pct", 0),
                other_pct=d.get("other_pct", 0),
                is_estimated=d.get("is_estimated", False))
            continue

        # API query via Xueqiu
        is_etf_linked = "ETF联接" in rec.name
        time.sleep(_API_DELAY)
        alloc = _query_fund_allocation_api(
            code, date=date, is_etf_linked=is_etf_linked)
        if alloc:
            estimated = False
            logger.info(f"Fund {code} ({rec.name}): XQ API eq={alloc['equity_pct']:.1%} "
                        f"bond={alloc['bond_pct']:.1%} cash={alloc['cash_pct']:.1%}"
                        f"{' (ETF-linked, 其他→equity)' if is_etf_linked else ''}")
        else:
            alloc = _default_allocation(rec.sub_type, config)
            estimated = True
            logger.info(f"Fund {code} ({rec.name}): default (type={rec.sub_type})")

        alloc["is_estimated"] = estimated
        cached[code] = alloc
        result[code] = FundAllocation(
            code=code, name=rec.name,
            total_market_value_cny=rec.market_value_cny,
            equity_pct=alloc["equity_pct"], bond_pct=alloc["bond_pct"],
            cash_pct=alloc["cash_pct"],
            commodity_pct=alloc.get("commodity_pct", 0),
            other_pct=alloc.get("other_pct", 0),
            is_estimated=estimated)

    save_json(cached, path)
    logger.info(f"5.1 done: {len(result)} fund allocations processed")
    return result


# ===========================================================================
# 5.2  Fund Holdings Detail
# ===========================================================================

def _guess_tracked_index(fund_name: str) -> Optional[str]:
    for kw, code in _INDEX_MAP.items():
        if kw in fund_name:
            return code
    return None


def _query_fund_holdings_api(code: str, date: str = "") -> Optional[list[dict]]:
    """Query fund top stock holdings via akshare."""
    try:
        import akshare as ak
        year = date[:4] if len(date) >= 4 else str(datetime.now().year)
        fallback_year = str(int(year) - 1)
        df = ak.fund_portfolio_hold_em(symbol=code, date=year)
        if df is None or df.empty:
            df = ak.fund_portfolio_hold_em(symbol=code, date=fallback_year)
        if df is None or df.empty:
            return None
        latest = _get_latest_season(df)
        rows = []
        for _, r in latest.iterrows():
            sc = str(r.get("股票代码", "")).strip()
            sn = str(r.get("股票名称", "")).strip()
            hp = safe_pct(r.get("占净值比例", 0))
            if sc and hp > 0:
                rows.append({"stock_code": sc, "stock_name": sn,
                             "hold_pct": hp, "asset_type": "equity"})
        return rows or None
    except Exception as e:
        logger.debug(f"fund_portfolio_hold_em failed for {code}: {e}")
        return None


def _query_fund_bond_holdings_api(code: str, date: str = "") -> Optional[list[dict]]:
    """Query fund top bond holdings via akshare."""
    try:
        import akshare as ak
        year = date[:4] if len(date) >= 4 else str(datetime.now().year)
        fallback_year = str(int(year) - 1)
        df = ak.fund_portfolio_bond_hold_em(symbol=code, date=year)
        if df is None or df.empty:
            df = ak.fund_portfolio_bond_hold_em(symbol=code, date=fallback_year)
        if df is None or df.empty:
            return None
        latest = _get_latest_season(df)
        rows = []
        for _, r in latest.iterrows():
            bc = str(r.get("债券代码", "")).strip()
            bn = str(r.get("债券名称", "")).strip()
            hp = safe_pct(r.get("占净值比例", 0))
            if bc and hp > 0:
                rows.append({"bond_code": bc, "bond_name": bn,
                             "hold_pct": hp, "asset_type": "bond"})
        return rows or None
    except Exception as e:
        logger.debug(f"fund_portfolio_bond_hold_em failed for {code}: {e}")
        return None


def _query_index_constituents_api(idx_code: str) -> Optional[list[dict]]:
    """Query index constituents from akshare."""
    try:
        import akshare as ak
        df = None
        try:
            df = ak.index_stock_cons_csindex(symbol=idx_code)
        except Exception:
            pass
        if df is None or df.empty:
            df = ak.index_stock_cons(symbol=idx_code)
        if df is None or df.empty:
            return None
        rows = []
        for _, r in df.iterrows():
            sc = str(r.get("成分券代码",
                           r.get("品种代码", ""))).strip()
            sn = str(r.get("成分券名称",
                           r.get("品种名称", ""))).strip()
            w = safe_pct(r.get("权重", 0))
            if sc:
                rows.append({"stock_code": sc, "stock_name": sn,
                             "weight": w or None})
        return rows or None
    except Exception as e:
        logger.debug(f"index constituents failed for {idx_code}: {e}")
        return None


def _penetrate_holdings(
    fund_records: list[HoldingRecord], config: dict, cache_dir: str,
    date: str = "",
) -> tuple[dict, dict, dict]:
    """Step 5.2: Fetch fund stock + bond holdings and index constituents.

    Returns:
        (fund_stock_holdings, fund_bond_holdings, index_constituents)
    """
    sh_path = os.path.join(cache_dir, "fund_holdings.json")
    bh_path = os.path.join(cache_dir, "fund_bond_holdings.json")
    i_path = os.path.join(cache_dir, "index_constituents.json")
    fund_sh = load_json(sh_path)
    fund_bh = load_json(bh_path)
    idx_c = load_json(i_path)

    for rec in fund_records:
        if rec.sub_type in ("money_fund", "commodity_fund"):
            continue

        # Stock holdings
        if rec.code not in fund_sh:
            time.sleep(_API_DELAY)
            h = _query_fund_holdings_api(rec.code, date=date)
            fund_sh[rec.code] = h or []
            logger.info(f"Fund {rec.code}: {len(h or [])} stock holdings")

        # Bond holdings
        if rec.code not in fund_bh:
            time.sleep(_API_DELAY)
            bh = _query_fund_bond_holdings_api(rec.code, date=date)
            fund_bh[rec.code] = bh or []
            logger.info(f"Fund {rec.code}: {len(bh or [])} bond holdings")

        # Index constituents
        if rec.sub_type in ("index_fund", "etf"):
            idx = _guess_tracked_index(rec.name)
            if idx and idx not in idx_c:
                time.sleep(_API_DELAY)
                c = _query_index_constituents_api(idx)
                idx_c[idx] = c or []
                logger.info(f"Index {idx}: {len(c or [])} constituents")

    save_json(fund_sh, sh_path)
    save_json(fund_bh, bh_path)
    save_json(idx_c, i_path)
    logger.info(f"5.2 done: {len(fund_sh)} funds (stock), "
                f"{len(fund_bh)} funds (bond), {len(idx_c)} indices")
    return fund_sh, fund_bh, idx_c


# ===========================================================================
# 5.3  Multi-dimension Data Collection
# ===========================================================================

def _query_stock_industry(code: str) -> Optional[dict]:
    try:
        import akshare as ak
        df = ak.stock_individual_info_em(symbol=code)
        if df is None or df.empty:
            return None
        info = {}
        for _, r in df.iterrows():
            item = str(r.get("item", ""))
            val  = str(r.get("value", ""))
            if "行业" in item:
                info["industry_l1"] = val
            if "板块" in item:
                info["industry_l2"] = val
        return info if "industry_l1" in info else None
    except Exception as e:
        logger.debug(f"stock_individual_info_em failed for {code}: {e}")
        return None


def _classify_region(code: str, currency: str, raw_info: dict) -> str:
    """Determine region: CN / HK / US."""
    region = raw_info.get("region", "")
    if region == "HK":
        return "HK"
    if region in ("US", "NYSE", "NASDAQ"):
        return "US"
    if re.match(r"^\d{6}$", code):
        return "CN"
    if re.match(r"^HK\d+$", code, re.I) or re.match(r"^\d{5}$", code):
        return "HK"
    if currency == "HKD":
        return "HK"
    if currency == "USD":
        return "US"
    return "CN"


def _compute_volatility(code: str, market: str = "cn") -> Optional[float]:
    """Compute annualized volatility from 1-year daily returns."""
    try:
        import akshare as ak
        end = datetime.now().strftime("%Y%m%d")
        start = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        if market == "hk":
            df = ak.stock_hk_hist(symbol=code, period="daily",
                                  start_date=start, end_date=end,
                                  adjust="qfq")
        else:
            df = ak.stock_zh_a_hist(symbol=code, period="daily",
                                    start_date=start, end_date=end,
                                    adjust="qfq")
        if df is None or len(df) < 30:
            return None
        col = "收盘" if "收盘" in df.columns else "Close"
        if col not in df.columns:
            return None
        ret = df[col].pct_change().dropna()
        return float(ret.std() * (252 ** 0.5))
    except Exception as e:
        logger.debug(f"Volatility failed for {code}: {e}")
        return None


def _default_risk_level(sub_type: str, config: dict) -> str:
    rules = config.get("volatility_rules", {})
    for lvl in ("high", "medium", "low"):
        if sub_type in rules.get(lvl, []):
            return lvl
    return "medium"


def _collect_dimensions(
    all_records: list[HoldingRecord], fund_holdings: dict,
    config: dict, cache_dir: str,
) -> tuple[dict, dict, dict]:
    """Step 5.3: Collect industry, region, volatility."""
    ind_path = os.path.join(cache_dir, "stock_industry.json")
    reg_path = os.path.join(cache_dir, "security_region.json")
    vol_path = os.path.join(cache_dir, "volatility.json")

    industry  = load_json(ind_path)
    regions   = load_json(reg_path)
    vol_data  = load_json(vol_path)

    # Region for every record
    for rec in all_records:
        if rec.code not in regions:
            regions[rec.code] = _classify_region(
                rec.code, rec.currency, rec.raw_info)

    # Collect unique stock codes
    stock_codes: set[tuple[str, str]] = set()
    for rec in all_records:
        if rec.asset_class == "equity":
            stock_codes.add((rec.code, rec.sub_type))
    for _, holdings in fund_holdings.items():
        for h in holdings:
            sc = h.get("stock_code", "")
            if sc:
                stock_codes.add((sc, "stock_cn"))
                if sc not in regions:
                    regions[sc] = "CN"

    # Industry
    logger.info(f"Collecting industry for {len(stock_codes)} stocks...")
    n_queried = 0
    for code, _ in stock_codes:
        if code in industry:
            continue
        if not re.match(r"^\d{6}$", code):
            industry[code] = {"industry_l1": "海外", "industry_l2": "海外"}
            continue
        time.sleep(_API_DELAY)
        info = _query_stock_industry(code)
        industry[code] = info or {"industry_l1": "未知", "industry_l2": "未知"}
        if info:
            n_queried += 1
    logger.info(f"Industry: queried {n_queried} new stocks")

    # Volatility
    logger.info("Collecting volatility data...")
    vol_n = 0
    for rec in all_records:
        if rec.code in vol_data:
            continue
        if rec.asset_class == "fund":
            vol_data[rec.code] = {
                "annual_volatility": None,
                "risk_level": _default_risk_level(rec.sub_type, config)}
            continue
        if (rec.asset_class == "equity"
                and rec.sub_type == "stock_cn" and vol_n < 20):
            time.sleep(_API_DELAY)
            vol = _compute_volatility(rec.code)
            if vol is not None:
                rl = ("high" if vol > 0.35
                      else ("medium" if vol > 0.20 else "low"))
                vol_data[rec.code] = {
                    "annual_volatility": round(vol, 4),
                    "risk_level": rl}
                vol_n += 1
                continue
        vol_data[rec.code] = {
            "annual_volatility": None,
            "risk_level": _default_risk_level(
                rec.sub_type or "other", config)}
    logger.info(f"Volatility: computed {vol_n} stocks")

    save_json(industry, ind_path)
    save_json(regions, reg_path)
    save_json(vol_data, vol_path)
    return industry, regions, vol_data


# ===========================================================================
# 5.4  Build Penetrated Holdings Summary
# ===========================================================================

def _sub_type_to_l1(sub_type: str) -> str:
    m = {"equity_fund": "equity_fund", "mixed_fund": "mixed_fund",
         "bond_fund": "bond_fund", "money_fund": "money_fund",
         "etf": "index_fund", "commodity_fund": "commodity_fund"}
    return m.get(sub_type, "other")


def _detail(rec, mv, orig, cls, rgn, ind, rl, vol, suffix=""):
    """Build a single penetrated detail dict."""
    return {
        "code": rec.code, "name": rec.name + suffix,
        "source": rec.source,
        "original_type": orig, "true_asset_class": cls,
        "market_value_cny": mv, "region": rgn,
        "industry_l1": ind.get("industry_l1", "未知"),
        "industry_l2": ind.get("industry_l2", "未知"),
        "risk_level": rl, "annual_volatility": vol,
    }


def _distribute_by_constituents(
    rec, eq_mv, constituents, industry, regions, details,
):
    """Distribute equity MV proportionally to index constituents."""
    tw = sum(c.get("weight") or 0 for c in constituents)
    equal = tw <= 0
    we = 1.0 / len(constituents) if equal and constituents else None
    for c in constituents:
        w = we if equal else ((c.get("weight") or 0) / tw)
        smv = eq_mv * w
        ind = industry.get(c["stock_code"], {})
        details.append({
            "code": c["stock_code"], "name": c["stock_name"],
            "source": rec.source,
            "original_type": f"via_index_{rec.sub_type}",
            "true_asset_class": "equity",
            "market_value_cny": smv,
            "region": regions.get(c["stock_code"], "CN"),
            "industry_l1": ind.get("industry_l1", "未知"),
            "industry_l2": ind.get("industry_l2", "未知"),
            "risk_level": "high", "annual_volatility": None,
        })


def _add_bond_details(
    rec, bond_mv, fund_bond_holdings, regions, details,
):
    """Add bond detail rows for a fund's bond portion."""
    holdings = fund_bond_holdings.get(rec.code, [])
    if not holdings:
        # No bond-level detail → keep as one aggregated row
        details.append({
            "code": rec.code,
            "name": f"{rec.name} (债券部分)",
            "source": rec.source,
            "original_type": rec.sub_type,
            "true_asset_class": "bond",
            "market_value_cny": bond_mv,
            "region": regions.get(rec.code, "CN"),
            "industry_l1": "债券", "industry_l2": "债券",
            "risk_level": "low", "annual_volatility": None,
        })
        return

    # Distribute by bond top holdings proportionally
    tp = sum(h["hold_pct"] for h in holdings) or 1.0
    for h in holdings:
        smv = bond_mv * (h["hold_pct"] / tp)
        details.append({
            "code": h["bond_code"], "name": h["bond_name"],
            "source": rec.source,
            "original_type": f"via_{rec.sub_type}",
            "true_asset_class": "bond",
            "market_value_cny": smv,
            "region": regions.get(rec.code, "CN"),
            "industry_l1": "债券", "industry_l2": "债券",
            "risk_level": "low", "annual_volatility": None,
        })


def _add_equity_details(
    rec, eq_mv, fund_holdings, idx_cons,
    industry, regions, details,
):
    """Add equity detail rows for a fund's stock portion."""
    holdings = fund_holdings.get(rec.code, [])
    if not holdings:
        details.append({
            "code": rec.code,
            "name": f"{rec.name} (股票部分)",
            "source": rec.source,
            "original_type": rec.sub_type,
            "true_asset_class": "equity",
            "market_value_cny": eq_mv,
            "region": regions.get(rec.code, "CN"),
            "industry_l1": "未知", "industry_l2": "未知",
            "risk_level": "high", "annual_volatility": None,
        })
        return

    # Index fund → try constituents
    if rec.sub_type in ("index_fund", "etf"):
        idx = _guess_tracked_index(rec.name)
        if idx and idx in idx_cons and idx_cons[idx]:
            _distribute_by_constituents(
                rec, eq_mv, idx_cons[idx],
                industry, regions, details)
            return

    # Distribute by fund top holdings
    tp = sum(h["hold_pct"] for h in holdings) or 1.0
    for h in holdings:
        smv = eq_mv * (h["hold_pct"] / tp)
        ind = industry.get(h["stock_code"], {})
        details.append({
            "code": h["stock_code"], "name": h["stock_name"],
            "source": rec.source,
            "original_type": f"via_{rec.sub_type}",
            "true_asset_class": "equity",
            "market_value_cny": smv,
            "region": regions.get(h["stock_code"], "CN"),
            "industry_l1": ind.get("industry_l1", "未知"),
            "industry_l2": ind.get("industry_l2", "未知"),
            "risk_level": "high", "annual_volatility": None,
        })


def _build_summary(
    all_records, allocs, fund_h, fund_bh, idx_c,
    industry, regions, vol_data, config, cache_dir,
    *, deep: bool = True,
) -> dict:
    """Step 5.4: Build the two-level penetration summary."""
    fx = config.get("exchange_rates",
                    {"CNY": 1.0, "HKD": 0.92, "USD": 7.25})

    l1 = {"individual_stock": 0.0, "equity_fund": 0.0,
          "mixed_fund": 0.0, "bond_fund": 0.0,
          "index_fund": 0.0, "money_fund": 0.0,
          "commodity_fund": 0.0, "other": 0.0}
    l2 = {"equity": 0.0, "bond": 0.0, "commodity": 0.0,
          "cash": 0.0, "other": 0.0}
    details: list[dict] = []

    for rec in all_records:
        mv = (rec.market_value_cny
              or rec.market_value * fx.get(rec.currency, 1.0))
        rgn = regions.get(rec.code, "CN")
        vi = vol_data.get(rec.code, {})

        if rec.asset_class == "equity":
            l1["individual_stock"] += mv
            l2["equity"] += mv
            ind = industry.get(rec.code, {})
            details.append(_detail(
                rec, mv, "individual_stock", "equity", rgn,
                ind, vi.get("risk_level", "high"),
                vi.get("annual_volatility")))

        elif rec.asset_class == "bond":
            l1["other"] += mv
            l2["bond"] += mv
            details.append(_detail(
                rec, mv, "bond", "bond", rgn,
                {"industry_l1": "债券", "industry_l2": "债券"},
                "low", None))

        elif rec.asset_class == "fund":
            alloc = allocs.get(rec.code)
            if not alloc:
                l1["other"] += mv
                l2["other"] += mv
                details.append(_detail(
                    rec, mv, rec.sub_type, "other", rgn,
                    {}, vi.get("risk_level", "medium"), None))
                continue

            l1[_sub_type_to_l1(rec.sub_type)] += mv

            parts = [
                ("equity",    alloc.equity_pct),
                ("bond",      alloc.bond_pct),
                ("cash",      alloc.cash_pct),
                ("commodity", alloc.commodity_pct),
                ("other",     alloc.other_pct),
            ]
            for cls, pct in parts:
                l2[cls] += mv * pct

            # Equity portion
            eq_mv = mv * alloc.equity_pct
            if deep:
                # Deep mode: expand to individual stock holdings
                _add_equity_details(
                    rec, eq_mv, fund_h, idx_c,
                    industry, regions, details)
            elif eq_mv > 0:
                # Shallow mode: keep as one fund-level equity row
                details.append(_detail(
                    rec, eq_mv, rec.sub_type, "equity", rgn,
                    {"industry_l1": "基金股票部分",
                     "industry_l2": "基金股票部分"},
                    vi.get("risk_level", "high"), None,
                    suffix=" (股票部分)"))

            # Bond portion
            bond_mv = mv * alloc.bond_pct
            if deep:
                _add_bond_details(
                    rec, bond_mv, fund_bh, regions, details)
            elif bond_mv > 0:
                details.append(_detail(
                    rec, bond_mv, rec.sub_type, "bond", rgn,
                    {"industry_l1": "债券", "industry_l2": "债券"},
                    "low", None, suffix=" (债券部分)"))

            # Non-equity/bond portions
            _LABELS = [
                ("现金", "cash", alloc.cash_pct, "low"),
                ("商品", "commodity", alloc.commodity_pct, "medium"),
                ("其他", "other", alloc.other_pct, "medium"),
            ]
            for label, cls, pct, rl in _LABELS:
                pmv = mv * pct
                if pmv > 0:
                    details.append(_detail(
                        rec, pmv, rec.sub_type, cls, rgn,
                        {"industry_l1": label, "industry_l2": label},
                        rl, None, suffix=f" ({label}部分)"))
        else:
            l1["other"] += mv
            l2["other"] += mv
            details.append(_detail(
                rec, mv, "other", "other", rgn,
                {"industry_l1": "其他", "industry_l2": "其他"},
                vi.get("risk_level", "medium"), None))

    total = sum(l1.values())

    def _pct_dict(d):
        return {k: {"market_value_cny": v,
                     "pct": v / total if total else 0}
                for k, v in d.items()}

    result = {
        "total_market_value_cny": total,
        "level1_summary": _pct_dict(l1),
        "level2_summary": _pct_dict(l2),
        "penetrated_details": details,
        "snapshot_time": datetime.now().isoformat(),
    }

    save_json(result,
              os.path.join(cache_dir, "penetrated_holdings.json"))
    save_json({
        "snapshot_time": result["snapshot_time"],
        "total_records": len(all_records),
        "total_details": len(details),
        "total_market_value_cny": total,
        "cache_files": [
            "fund_allocation.json", "fund_holdings.json",
            "fund_bond_holdings.json", "index_constituents.json",
            "stock_industry.json", "security_region.json",
            "volatility.json", "penetrated_holdings.json"],
    }, os.path.join(cache_dir, "data_snapshot_meta.json"))

    def _fmt(d):
        parts = []
        for k, v in sorted(d.items(), key=lambda x: -x[1]):
            if v > 0:
                parts.append(f"{k}={v:,.0f}({v/total*100:.1f}%)"
                             if total else f"{k}={v:,.0f}")
        return " | ".join(parts)

    logger.info(f"Penetration done: total={total:,.0f} CNY")
    logger.info(f"L1: {_fmt(l1)}")
    logger.info(f"L2: {_fmt(l2)}")
    return result


# ===========================================================================
# Public API
# ===========================================================================

def penetrate_funds(
    records: list[HoldingRecord], config: dict,
    deep_penetration: bool = False,
    date: str = "",
) -> dict:
    """Run fund penetration (Steps 5.1-5.4).

    Args:
        records: Classified HoldingRecord list.
        config: Configuration dict from config.yaml.
        deep_penetration: If True, run full individual stock-level
            penetration (5.2 holdings + 5.3 industry/volatility).
            If False (default), only run 5.1 allocation and produce
            fund-level summary without expanding to individual stocks.
            Can also be set via config["deep_penetration"].
        date: Date string in YYYYMMDD format (e.g. '20251231').
            Used to determine which year's fund holdings to query.

    Returns:
        Penetration result dict with level1/level2 summaries
        and penetrated_details.
    """
    cache_dir = config["paths"]["cache_dir"]
    os.makedirs(cache_dir, exist_ok=True)

    # Config can override the parameter
    deep = deep_penetration or config.get("deep_penetration", False)

    funds = [r for r in records if r.asset_class == "fund"]
    logger.info(f"Penetration start: {len(funds)} funds / "
                f"{len(records)} total (deep={deep})")

    # 5.1 - Always run: fund asset allocation
    allocs = _penetrate_allocation(funds, config, cache_dir, date=date)

    if deep:
        # 5.2 - Deep mode: fetch individual stock + bond holdings
        fund_h, fund_bh, idx_c = _penetrate_holdings(
            funds, config, cache_dir, date=date)
        # 5.3 - Deep mode: collect industry / region / volatility
        industry, regions, vol = _collect_dimensions(
            records, fund_h, config, cache_dir)
    else:
        # Shallow mode: skip 5.2/5.3, use empty placeholders
        fund_h, fund_bh, idx_c = {}, {}, {}
        industry, regions, vol = {}, {}, {}
        # Still classify region for top-level records
        for rec in records:
            regions[rec.code] = _classify_region(
                rec.code, rec.currency, rec.raw_info)
            vol[rec.code] = {
                "annual_volatility": None,
                "risk_level": _default_risk_level(
                    rec.sub_type or "other", config)}
        logger.info("5.2/5.3 skipped (deep_penetration=False)")

    return _build_summary(
        records, allocs, fund_h, fund_bh, idx_c,
        industry, regions, vol, config, cache_dir,
        deep=deep)
