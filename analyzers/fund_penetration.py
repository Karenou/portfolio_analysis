"""Fund Penetration Analysis - Step 5.

Sub-steps:
  5.1 - Fund asset allocation (equity/bond/cash/commodity split)
  5.2 - Fund holding detail (top-N stocks, index constituents)
  5.3 - Multi-dimension data collection (industry, region)
  5.4 - Penetrated result summary with all dimension labels -> cache
"""

import logging
import os
import re
import time
from datetime import datetime
from typing import Optional
import akshare as ak

from models import HoldingRecord, FundAllocation
from analyzers.cache_utils import load_json, save_json, safe_pct
from analyzers.fund_nav_db import (
    batch_save_stock_industry,
    save_fund_industry_allocation,
)

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


def _should_reclassify_other_as_equity(
    sub_type: str, name: str, raw: dict[str, float],
) -> bool:
    """判断是否应将"其他"重分类为"股票"。

    适用场景：ETF联接基金、指数基金、QDII股票型FOF 等通过持有ETF份额间接持有
    股票的基金，API 返回的"其他"实际上代表底层 ETF/基金份额的股票仓位。

    判断逻辑（前提：其他>50% 且 股票<=50%）：
    1. sub_type 为 etf/index_fund → 重分类为股票
       （含国内ETF联接、QDII股票型ETF联接、QDII-FOF股票型等）
    2. 名称含 "ETF联接" / "联接" / "指数" → 重分类为股票
    3. 股票型基金(equity_fund)且"其他" > 80% → 重分类为股票

    不触发重分类的情况（由 _should_reclassify_other_as_bond 处理）：
    - 债券型 QDII（sub_type=bond_fund）的"其他"由债券重分类逻辑处理
    - 混合型基金（sub_type=mixed_fund）的"其他"保持原样不重分类
    """
    other_pct = raw.get("其他", 0.0)
    eq_pct = raw.get("股票", 0.0)

    # 如果"其他"很小，没必要重分类
    if other_pct <= 50:
        return False

    # 如果本身股票已经占大头，不需要重分类
    if eq_pct > 50:
        return False

    # 规则1: ETF/指数类基金 - "其他"就是底层ETF份额 = 股票
    # 含：国内ETF联接、QDII股票ETF联接、QDII-FOF(标普500/纳斯达克/恒生等)
    if sub_type in ("etf", "index_fund"):
        return True

    # 规则2: 名称关键词判断 - ETF联接、联接、指数
    etf_keywords = ("ETF联接", "联接", "指数")
    if any(kw in name for kw in etf_keywords):
        return True

    # 规则3: 股票型基金且"其他"异常高 - 可能是通过ETF方式配置
    if sub_type == "equity_fund" and other_pct > 80:
        return True

    return False


def _should_reclassify_other_as_bond(
    sub_type: str, name: str, raw: dict[str, float],
) -> bool:
    """判断是否应将"其他"重分类为"债券"。

    适用场景：债券型基金通过持有资管计划、ABS等间接持有债券类资产。
    """
    other_pct = raw.get("其他", 0.0)
    bond_pct = raw.get("债券", 0.0)

    if other_pct <= 50:
        return False

    # 债券型基金，"其他"很可能是 ABS/资管计划等固收资产
    if sub_type == "bond_fund":
        return True

    # 名称含债券关键词
    bond_keywords = ("债", "信用", "利率", "纯债", "短融")
    if any(kw in name for kw in bond_keywords):
        return True

    return False


def _query_fund_allocation_api(
    code: str, date: str = "", *, sub_type: str = "", name: str = "",
) -> Optional[dict]:
    """Query fund asset allocation via Xueqiu (fund_individual_detail_hold_xq).

    Returns a normalised dict with equity/bond/cash/commodity/other ratios
    that sum to 1.0. 自动根据基金类型和名称判断是否需要将"其他"重新归类。

    Args:
        code: Fund code.
        date: Date string in YYYYMMDD format.
        sub_type: Fund sub type (etf/index_fund/equity_fund/bond_fund...).
        name: Fund name for keyword-based reclassification.
    """
    try:
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

        # 智能重分类"其他"
        reclassified = ""
        if _should_reclassify_other_as_equity(sub_type, name, raw):
            eq += other
            other = 0.0
            reclassified = "equity"
        elif _should_reclassify_other_as_bond(sub_type, name, raw):
            bd += other
            other = 0.0
            reclassified = "bond"

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
            "_reclassified": reclassified,  # 标记重分类来源（debug用）
            "_raw": raw,  # 保留原始数据（debug用）
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

        # API query via Xueqiu（智能重分类"其他"为实际资产类型）
        time.sleep(_API_DELAY)
        alloc = _query_fund_allocation_api(
            code, date=date, sub_type=rec.sub_type, name=rec.name)
        if alloc:
            estimated = False
            reclassified = alloc.pop("_reclassified", "")
            alloc.pop("_raw", None)  # 移除debug字段，不写入缓存
            reclassify_msg = ""
            if reclassified:
                reclassify_msg = f" (其他→{reclassified})"
            logger.info(f"Fund {code} ({rec.name}): XQ API eq={alloc['equity_pct']:.1%} "
                        f"bond={alloc['bond_pct']:.1%} cash={alloc['cash_pct']:.1%}"
                        f"{reclassify_msg}")
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

def _query_fund_industry(fund_code: str, date: str) -> Optional[list[dict]]:
    """查询基金的行业配置分布（fund_portfolio_industry_allocation_em）。

    返回最新一期的行业列表，每项包含 industry_name 和 pct（占净值比例）。
    注意：此 API 接受基金代码，不能传股票代码。
    """
    try:
        year = date[:4] if len(date) >= 4 else str(datetime.now().year)
        df = ak.fund_portfolio_industry_allocation_em(symbol=fund_code, date=year)
        if df is None or df.empty:
            return None
        # 取最新一期（截止时间最大）
        latest_date = df["截止时间"].max()
        df_latest = df[df["截止时间"] == latest_date]
        rows = []
        for _, r in df_latest.iterrows():
            ind_name = str(r.get("行业类别", "")).strip()
            pct = float(r.get("占净值比例", 0) or 0)
            if ind_name and pct > 0:
                rows.append({"industry_name": ind_name, "pct": pct})
        return rows if rows else None
    except Exception as e:
        logger.debug(f"fund_portfolio_industry_allocation_em failed for {fund_code}: {e}")
        return None


def _get_top_industry(industry_rows: list[dict]) -> tuple[str, str]:
    """从基金行业分布列表中取占比最高的一个行业名，映射为 l1/l2。"""
    if not industry_rows:
        return "未知", "未知"
    top = max(industry_rows, key=lambda x: x["pct"])
    name = top["industry_name"]
    return name, name


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


def _collect_dimensions(
    all_records: list[HoldingRecord], fund_holdings: dict,
    config: dict, cache_dir: str, date: str
) -> tuple[dict, dict]:
    """Step 5.3: Collect industry, region.

    行业数据采集策略（修正版）：
    - 基金持仓：直接用基金代码调 fund_portfolio_industry_allocation_em，
      获取该基金的行业分布（正确用途），写入 SQLite fund_industry_allocation 表
    - 直接持有的 A 股个股：标记为"个股-直接持有"
    - 海外股票：直接标记为"海外"
    """
    reg_path = os.path.join(cache_dir, "security_region.json")

    industry: dict = {}
    regions   = load_json(reg_path)

    # 地区标注：所有记录
    for rec in all_records:
        if rec.code not in regions:
            regions[rec.code] = _classify_region(
                rec.code, rec.currency, rec.raw_info)

    # 直接持有的个股行业标注
    for rec in all_records:
        if rec.asset_class == "equity" and rec.code not in industry:
            if not re.match(r"^\d{6}$", rec.code):
                # 海外股票
                industry[rec.code] = {"industry_l1": "海外", "industry_l2": "海外"}
            else:
                # A股个股：标记 region，行业留空等待后续补充
                industry[rec.code] = {"industry_l1": "个股", "industry_l2": "个股"}

    # 基金行业分布：用基金代码直接查，写入 SQLite
    fund_records = [r for r in all_records if r.asset_class == "fund"
                    and r.sub_type not in ("money_fund", "commodity_fund")]
    logger.info(f"Collecting fund industry for {len(fund_records)} funds...")
    n_queried = 0
    for rec in fund_records:
        if rec.code in industry:
            continue
        time.sleep(_API_DELAY)
        rows = _query_fund_industry(rec.code, date)
        if rows:
            # 取 top 行业写入 industry dict（供 penetrated_details 展示）
            top_l1, top_l2 = _get_top_industry(rows)
            industry[rec.code] = {"industry_l1": top_l1, "industry_l2": top_l2}
            n_queried += 1

            # 写入 SQLite fund_industry_allocation 表
            industry_data = [{"industry_name": r["industry_name"],
                              "industry_pct": r["pct"]} for r in rows]
            report_date = date[:4] if date else str(datetime.now().year)
            save_fund_industry_allocation(
                rec.code, report_date, industry_data,
                data_source="fund_portfolio_industry_allocation_em")

            logger.info(f"Fund {rec.code} ({rec.name}): top industry={top_l1} "
                        f"({len(rows)} sectors)")
        else:
            industry[rec.code] = {"industry_l1": "未知", "industry_l2": "未知"}
    logger.info(f"Fund industry: queried {n_queried} new funds")

    # 同步 fund_holdings 里的底层股票行业（已知来源：fund_holdings，标记为"基金持股"）
    for _, holdings in fund_holdings.items():
        for h in holdings:
            sc = h.get("stock_code", "")
            if sc and sc not in industry:
                if not re.match(r"^\d{6}$", sc):
                    industry[sc] = {"industry_l1": "海外", "industry_l2": "海外"}
                else:
                    industry[sc] = {"industry_l1": "个股", "industry_l2": "个股"}
            if sc and sc not in regions:
                regions[sc] = "CN"

    n_queried_stocks = sum(1 for v in industry.values()
                           if v.get("industry_l1") not in ("未知", "海外", "个股"))
    logger.info(f"Industry: {len(industry)} total, {n_queried_stocks} with known sector")

    save_json(regions, reg_path)

    # Batch persist stock industry data to SQLite
    batch_save_stock_industry(industry)

    return industry, regions


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
    industry, regions, config, cache_dir,
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

        if rec.asset_class == "equity":
            l1["individual_stock"] += mv
            l2["equity"] += mv
            ind = industry.get(rec.code, {})
            details.append(_detail(
                rec, mv, "individual_stock", "equity", rgn,
                ind, "high", None))

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
                    {}, "medium", None))
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
                    "high", None,
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
                "medium", None))

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
            "security_region.json", "penetrated_holdings.json"],
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
            penetration (5.2 holdings + 5.3 industry).
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
        # 5.3 - Deep mode: collect industry / region（含底层持股行业标注）
        industry, regions = _collect_dimensions(
            records, fund_h, config, cache_dir, date)
    else:
        # Shallow mode: skip 5.2, use empty placeholders
        fund_h, fund_bh, idx_c = {}, {}, {}
        # 5.3 - 即使浅模式也执行行业分布采集，写入 SQLite
        industry, regions = _collect_dimensions(
            records, fund_h, config, cache_dir, date)
        logger.info("5.2 skipped (deep_penetration=False), 5.3 industry executed")

    return _build_summary(
        records, allocs, fund_h, fund_bh, idx_c,
        industry, regions, config, cache_dir,
        deep=deep)
