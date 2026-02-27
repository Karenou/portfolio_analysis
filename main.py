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
    module_name = f"test.get_{platform}_penetration"
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


if __name__ == "__main__":
    """Main pipeline entry point."""
    config = load_config()
    cache_dir = config["paths"]["cache_dir"]
    data_dir = config["paths"]["data_dir"]
    
    # Get current date in YYYYMMDD format
    date_str = datetime.now().strftime("%Y%m%d")
    logger.info(f"Pipeline started. Date: {date_str}")
    
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
            
            if run_penetration_test(platform, data_file, date_str, deep=False):
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
    
    # Done
    output_dir = config["paths"]["output_dir"]
    logger.info(f"\n✅ Pipeline complete! Output dir: {output_dir}/")
    logger.info(f"   Summary file: output/aggregated_summary_{date_str}.json")
