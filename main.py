"""Portfolio Analysis - Main Entry Point.

Orchestrates the full pipeline: clean cache -> penetrate all platforms -> aggregate -> report.
"""

import os
import sys
import glob
import subprocess
import logging
import yaml
from datetime import datetime

from models import HoldingRecord, BaseParser

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clear_cache_files(cache_dir: str) -> None:
    """Clear all cache files in subdirectories, but keep subdirectories.
    
    Args:
        cache_dir: Path to the cache directory.
    """
    if not os.path.isdir(cache_dir):
        logger.warning(f"Cache directory not found: {cache_dir}")
        return
    
    cleared_count = 0
    for subdir_name in os.listdir(cache_dir):
        subdir_path = os.path.join(cache_dir, subdir_name)
        if os.path.isdir(subdir_path):
            for file_path in glob.glob(os.path.join(subdir_path, "*")):
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    cleared_count += 1
                    logger.debug(f"Removed: {file_path}")
    
    logger.info(f"Cleared {cleared_count} cache files from {cache_dir}")


def find_data_file(data_dir: str, prefix: str) -> str:
    """Find the first file matching the prefix in data directory.
    
    Args:
        data_dir: Path to the data directory.
        prefix: File name prefix to match (e.g., 'alipay_').
    
    Returns:
        File name (not full path) of the matched file.
    
    Raises:
        FileNotFoundError: If no matching file is found.
    """
    for filename in os.listdir(data_dir):
        if filename.lower().startswith(prefix):
            return filename
    raise FileNotFoundError(f"No file found with prefix '{prefix}' in {data_dir}")


def run_penetration_test(platform: str, data_file: str, date_str: str, deep: bool = False) -> bool:
    """Run a penetration test script for a specific platform.
    
    Args:
        platform: Platform name (alipay, futu, huatai, qieman, snowball).
        data_file: Data file name to process.
        date_str: Holdings date in YYYYMMDD format.
        deep: Whether to enable deep penetration mode.
    
    Returns:
        True if successful, False otherwise.
    """
    module_name = f"script.get_{platform}_penetration"
    cmd = [sys.executable, "-m", module_name, "--file", data_file, "--date", date_str]
    
    if deep:
        cmd.insert(3, "--deep")
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to run {module_name}: {e}")
        logger.error(e.stderr[-1000:] if len(e.stderr) > 1000 else e.stderr)
        return False


def run_aggregator(cache_dir: str) -> bool:
    """Run cross-platform aggregation.
    
    Args:
        cache_dir: Path to the cache directory.
    
    Returns:
        True if successful, False otherwise.
    """
    from analyzers.aggregator import aggregate_cross_platform
    
    logger.info("Running cross-platform aggregation...")
    result = aggregate_cross_platform(cache_dir)
    
    if result:
        logger.info(f"Aggregation complete. Total: {result['total_market_value_cny']:,.2f} CNY")
        return True
    else:
        logger.error("Aggregation failed.")
        return False


def run_fetch_nav_data() -> bool:
    """抓取所有平台的历史行情数据，写入 SQLite。
    
    Returns:
        True if successful, False otherwise.
    """
    from analyzers.fund_nav_db import (
        fetch_and_store_fund_nav,
        fetch_and_store_etf_hist,
        fetch_and_store_stock_hist,
        fetch_and_store_stock_hk_hist,
        fetch_and_store_stock_us_hist,
    )
    
    logger.info("Fetching historical price data...")
    
    # 场外基金 NAV
    fund_platforms = ["alipay", "qieman", "snowball"]
    for platform in fund_platforms:
        try:
            logger.info(f"  Fetching {platform} fund NAV...")
            fetch_and_store_fund_nav(platform)
        except Exception as e:
            logger.error(f"  {platform} fund NAV fetch failed: {e}")
    
    # ETF 行情
    try:
        logger.info("  Fetching ETF historical data...")
        fetch_and_store_etf_hist()
    except Exception as e:
        logger.error(f"  ETF fetch failed: {e}")
    
    # A股行情
    try:
        logger.info("  Fetching A-share historical data...")
        fetch_and_store_stock_hist()
    except Exception as e:
        logger.error(f"  A-share fetch failed: {e}")
    
    # 港股行情
    try:
        logger.info("  Fetching HK stock historical data...")
        fetch_and_store_stock_hk_hist()
    except Exception as e:
        logger.error(f"  HK stock fetch failed: {e}")
    
    # 美股行情
    try:
        logger.info("  Fetching US stock historical data...")
        fetch_and_store_stock_us_hist()
    except Exception as e:
        logger.error(f"  US stock fetch failed: {e}")
    
    logger.info("Historical price data fetch complete.")
    return True


def run_indicators(config: dict, output_dir: str, date_str: str) -> bool:
    """计算量化指标（收益率/波动率/回撤/夏普/相关性）。
    
    Args:
        config: 配置字典。
        output_dir: 输出目录。
        date_str: 日期字符串 YYYYMMDD。
    
    Returns:
        True if successful, False otherwise.
    """
    import json
    from analyzers.indicators import compute_all_indicators
    
    logger.info("Computing quantitative indicators...")
    
    try:
        result = compute_all_indicators(config, windows=["1M", "3M", "6M", "1Y", "3Y", "5Y"])
        
        if result:
            # 保存指标结果到 output 目录
            output_path = os.path.join(output_dir, f"indicators_{date_str}.json")
            os.makedirs(output_dir, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Indicators saved to: {output_path}")
            logger.info(f"  Asset count: {result.get('asset_count', 'N/A')}")
            logger.info(f"  Single asset metrics: {len(result.get('single_asset_metrics', []))}")
            logger.info(f"  Portfolio windows: {list(result.get('portfolio_metrics', {}).keys())}")
            return True
        else:
            logger.warning("Indicator computation returned empty result.")
            return False
    except Exception as e:
        logger.error(f"Indicator computation failed: {e}")
        return False


if __name__ == "__main__":
    """Main pipeline entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Portfolio Analysis Pipeline")
    parser.add_argument("--deep", action="store_true", default=False,
                        help="启用深度穿透模式（获取持股明细）")
    args = parser.parse_args()
    
    config = load_config()
    cache_dir = config["paths"]["cache_dir"]
    data_dir = config["paths"]["data_dir"]
    deep_mode = args.deep or config.get("deep_penetration", False)
    
    # Get current date in YYYYMMDD format
    date_str = datetime.now().strftime("%Y%m%d")
    logger.info(f"Pipeline started. Date: {date_str}, deep={deep_mode}")
    
    # Step 0: Clear cache files (keep subdirectories)
    logger.info("=== Step 0: Clearing cache files ===")
    clear_cache_files(cache_dir)
    
    # Platform configurations: (platform_name, file_prefix)
    platforms = [
        ("alipay", "alipay_"),
        ("futu", "futu_"),
        ("huatai", "huatai_"),
        ("qieman", "qieman_"),
        ("snowball", "snowball_"),
    ]
    
    # Step 1-5: Run penetration tests for each platform
    logger.info("=== Step 1-5: Running penetration tests ===")
    success_count = 0
    for platform, prefix in platforms:
        try:
            data_file = find_data_file(data_dir, prefix)
            logger.info(f"\n--- Processing [{platform}] with file: {data_file} ---")
            
            if run_penetration_test(platform, data_file, date_str, deep=deep_mode):
                success_count += 1
            else:
                logger.warning(f"Skipping {platform} due to error")
        except FileNotFoundError as e:
            logger.warning(f"Skipping {platform}: {e}")
    
    logger.info(f"Penetration tests completed: {success_count}/{len(platforms)} successful")
    
    if success_count == 0:
        logger.error("No platforms processed successfully. Aborting.")
    
    # Step 6: Run cross-platform aggregation
    logger.info("\n=== Step 6: Cross-platform aggregation ===")
    run_aggregator(cache_dir)
    
    # Step 7: Fetch historical price data
    logger.info("\n=== Step 7: Fetching historical price data ===")
    run_fetch_nav_data()
    
    # Step 8: Compute quantitative indicators
    logger.info("\n=== Step 8: Computing quantitative indicators ===")
    output_dir = config["paths"]["output_dir"]
    run_indicators(config, output_dir, date_str)
    
    # Done
    logger.info(f"\n✅ Pipeline complete! Output dir: {output_dir}/")
    logger.info(f"   Summary file: output/aggregated_summary_{date_str}.json")
    logger.info(f"   Indicators file: output/indicators_{date_str}.json")
