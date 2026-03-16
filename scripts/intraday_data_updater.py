#!/usr/bin/env python3
"""
SEBI-Compliant Intraday Data Updater

Manages intraday (1-minute) OHLCV parquet files with a sliding window
approach while maintaining SEBI compliance.

Window Logic:
- Window Start: today - 61 days (older data is deleted)
- Window End: today - 31 days (SEBI compliance limit)
- Data is fetched for the entire window and old data is pruned

Example:
    Today = February 9, 2025
    Window Start = December 10, 2024 (today - 61)
    Window End = January 9, 2025 (today - 31 = SEBI compliance)

    Data older than Dec 10 is deleted
    Data is fetched/updated from Dec 10 to Jan 9
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time as time_module
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, time
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Tuple

import pandas as pd
import asyncio

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.data_providers import fetch_intraday_ohlcv, fetch_intraday_ohlcv_async
from backend.data_compliance import INTRADAY_DIRECTORY, get_intraday_parquet_path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("intraday_update.log"),
    ],
)
logger = logging.getLogger(__name__)

# Constants
SEBI_LAG_DAYS = 30
WINDOW_DAYS = 30  # 30 days of intraday data (from day 31 to day 61)
WINDOW_START_OFFSET = 61  # today - 61 days
REPORT_FILE = "intraday_update_report.json"


class IntradayDataUpdater:
    """
    Manages SEBI-compliant intraday data updates with sliding window.
    """

    def __init__(self, intraday_dir: Path = None, max_workers: int = 20):
        self.intraday_dir = (
            Path(intraday_dir) if intraday_dir else Path(INTRADAY_DIRECTORY)
        )
        self.max_workers = max_workers
        self.updated_stocks: List[Dict] = []
        self.skipped_stocks: List[Dict] = []
        self.deleted_files: List[Dict] = []
        self.errors: List[Dict] = []
        self._lock = Lock()

    def get_window_dates(self) -> Tuple[datetime, datetime]:
        """
        Calculate the intraday data window.

        Yahoo only provides last 30 days of 1-min data.
        We fetch that window and let pruning handle the 61-day sliding window.

        Returns:
            Tuple of (window_start, window_end)
            - window_start: today - 30 days (00:00:00) - what Yahoo can provide
            - window_end: now (the latest data available)
        """
        today = datetime.now()

        # Yahoo provides last 30 days
        start_date = today - timedelta(days=30)
        end_date = today

        window_start = datetime.combine(start_date.date(), time(0, 0, 0))
        window_end = end_date

        return window_start, window_end

    def get_all_intraday_files(self) -> List[Path]:
        """
        Get all intraday parquet files.
        Returns sorted list of file paths.
        """
        parquet_files = []

        if not self.intraday_dir.exists():
            return parquet_files

        for subdir in sorted(self.intraday_dir.iterdir()):
            if subdir.is_dir():
                for parquet_file in sorted(subdir.glob("*.parquet")):
                    parquet_files.append(parquet_file)

        return parquet_files

    def get_symbols_from_daily_data(self) -> List[str]:
        """
        Get list of symbols from daily data directory.
        These are the symbols we should fetch intraday data for.
        """
        data_dir = Path(__file__).parent.parent / "data"
        symbols = []

        if not data_dir.exists():
            return symbols

        for subdir in data_dir.iterdir():
            if subdir.is_dir():
                for parquet_file in subdir.glob("*.parquet"):
                    symbols.append(parquet_file.stem.upper())

        return sorted(set(symbols))

    def prune_old_data(self, file_path: Path, window_start: datetime) -> bool:
        """
        Remove data older than window_start from a parquet file.

        Returns:
            True if file was modified, False otherwise
        """
        try:
            df = pd.read_parquet(file_path)

            if df.empty:
                return False

            # Ensure datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # Remove timezone info if present
            if hasattr(df.index, "tz") and df.index.tz is not None:
                df.index = df.index.tz_convert(None)

            original_len = len(df)
            df_filtered = df[df.index >= window_start]

            if len(df_filtered) < original_len:
                if df_filtered.empty:
                    # All data is old, delete file
                    file_path.unlink()
                    logger.info(
                        f"Deleted {file_path.stem} (all data older than window)"
                    )
                    return True

                # Save filtered data
                df_filtered.to_parquet(file_path, compression="snappy")
                logger.info(
                    f"Pruned {file_path.stem}: {original_len - len(df_filtered)} old rows removed"
                )
                return True

            return False

        except Exception as e:
            logger.error(f"Error pruning {file_path}: {e}")
            return False

    def prune_all_old_data(self, window_start: datetime) -> int:
        """
        Prune old data from all intraday files.

        Returns:
            Number of files modified/deleted
        """
        files = self.get_all_intraday_files()
        modified_count = 0

        logger.info(f"Pruning data older than {window_start.date()}...")

        for file_path in files:
            symbol = file_path.stem
            original_size = file_path.stat().st_size if file_path.exists() else 0

            if self.prune_old_data(file_path, window_start):
                modified_count += 1
                self.deleted_files.append(
                    {
                        "symbol": symbol,
                        "action": "pruned",
                        "original_size": original_size,
                    }
                )

        logger.info(f"Pruned {modified_count} files")
        return modified_count

    def normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize intraday dataframe columns and index.
        """
        if df is None or df.empty:
            return df

        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        # Remove timezone info
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_convert(None)

        # Standardize column names
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
        df.columns = [col.title().replace("_", "") for col in df.columns]
        df.index.name = "Date"

        return df

    def filter_to_window(
        self, df: pd.DataFrame, window_start: datetime, window_end: datetime
    ) -> pd.DataFrame:
        """
        Filter dataframe to only include data within the window.
        """
        if df is None or df.empty:
            return df

        return df[(df.index >= window_start) & (df.index <= window_end)].copy()

    def fetch_intraday_data(
        self, symbol: str, window_start: datetime, window_end: datetime
    ) -> Optional[pd.DataFrame]:
        """
        Fetch intraday data for a symbol within the window.

        Returns:
            DataFrame with OHLCV data, or None if fetch fails
        """
        try:
            logger.info(f"Fetching intraday data for {symbol}")

            # Use async version for faster parallel chunk fetching
            df = asyncio.run(
                fetch_intraday_ohlcv_async(symbol, window_start, window_end)
            )

            if df is None or df.empty:
                logger.warning(f"No intraday data returned for {symbol}")
                return None

            df = self.normalize_dataframe(df)

            if df.empty:
                logger.warning(f"No data in window for {symbol}")
                return None

            logger.info(f"Fetched {len(df)} rows for {symbol}")
            return df

        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return None

    def update_intraday_file(
        self,
        symbol: str,
        window_start: datetime,
        window_end: datetime,
        min_rows: int = 0,
    ) -> bool:
        """
        Update intraday data file for a single symbol.

        Returns:
            True if update successful, False otherwise
        """
        try:
            # Fetch new data
            df = self.fetch_intraday_data(symbol, window_start, window_end)

            if df is None or df.empty:
                return False

            # Check minimum rows requirement
            if min_rows > 0 and len(df) < min_rows:
                logger.warning(f"{symbol} skipped: {len(df)} rows < {min_rows} minimum")
                return False

            # Get output path
            out_path = get_intraday_parquet_path(symbol)
            out_path = Path(out_path)

            # Ensure parent directory exists
            out_path.parent.mkdir(parents=True, exist_ok=True)

            # Save to parquet
            df.to_parquet(out_path, compression="snappy")

            logger.info(f"Updated {symbol}: {len(df)} rows saved")
            return True

        except Exception as e:
            logger.error(f"Error updating {symbol}: {e}")
            self.errors.append({"symbol": symbol, "error": str(e), "type": "update"})
            return False

    def validate_intraday_file(
        self, file_path: Path, window_start: datetime, window_end: datetime
    ) -> Dict:
        """
        Validate an intraday parquet file.

        Returns:
            Dictionary with validation results
        """
        symbol = file_path.stem
        result = {"symbol": symbol, "valid": True, "errors": []}

        try:
            df = pd.read_parquet(file_path)

            if df.empty:
                result["valid"] = False
                result["errors"].append("empty_file")
                return result

            # Ensure datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                df.index = pd.to_datetime(df.index)

            # Check required columns
            required_cols = {"Open", "High", "Low", "Close", "Volume"}
            df.columns = [
                col.title().replace("_", "")
                for col in [c.lower().replace(" ", "_") for c in df.columns]
            ]

            if not required_cols.issubset(set(df.columns)):
                result["valid"] = False
                result["errors"].append("missing_columns")
                return result

            # Check date range
            min_dt = df.index.min()
            max_dt = df.index.max()

            if min_dt < window_start:
                result["errors"].append(f"data_older_than_window_start")

            if max_dt > window_end:
                result["errors"].append(f"data_newer_than_window_end")

            result["rows"] = len(df)
            result["min_date"] = min_dt.isoformat()
            result["max_date"] = max_dt.isoformat()

        except Exception as e:
            result["valid"] = False
            result["errors"].append(f"read_error: {str(e)}")

        return result

    def run_update(
        self,
        symbols: List[str] = None,
        max_symbols: int = None,
        min_rows: int = 0,
        sleep_seconds: float = 1.0,
        prune_old: bool = True,
        validate: bool = True,
    ) -> Dict:
        """
        Run the intraday data update pipeline.

        Args:
            symbols: Specific symbols to update (None = all from daily data)
            max_symbols: Maximum number of symbols to process
            min_rows: Minimum rows required to save file
            sleep_seconds: Sleep between fetches to avoid rate limits
            prune_old: Whether to prune old data before fetching
            validate: Whether to validate files after update

        Returns:
            Dictionary with update statistics
        """
        logger.info("=" * 60)
        logger.info("Starting Intraday Data Update Pipeline")
        logger.info("=" * 60)

        # Calculate window dates
        window_start, window_end = self.get_window_dates()
        today = datetime.now()

        logger.info(f"Current date: {today.date()}")
        logger.info(
            f"Window start: {window_start.date()} (today - {WINDOW_START_OFFSET} days)"
        )
        logger.info(f"Window end: {window_end.date()} (today - {SEBI_LAG_DAYS} days)")
        logger.info(f"Window span: {(window_end - window_start).days + 1} days")
        logger.info("-" * 60)

        # Ensure intraday directory exists
        self.intraday_dir.mkdir(parents=True, exist_ok=True)

        # Prune old data
        if prune_old:
            pruned = self.prune_all_old_data(window_start)
            logger.info(f"Pruned {pruned} files with old data")

        # Get symbols to process
        if symbols is None:
            symbols = self.get_symbols_from_daily_data()

        if max_symbols:
            symbols = symbols[:max_symbols]

        if not symbols:
            logger.error("No symbols found to process")
            return {"success": False, "error": "No symbols found"}

        logger.info(
            f"Processing {len(symbols)} symbols with {self.max_workers} workers"
        )
        logger.info("-" * 60)

        # Process each symbol with thread pool
        attempted = 0
        succeeded = 0
        skipped = 0
        failed = 0

        def process_symbol(symbol: str) -> Tuple[str, bool]:
            """Thread-safe function to update a symbol."""
            success = self.update_intraday_file(
                symbol, window_start, window_end, min_rows
            )
            with self._lock:
                if success:
                    self.updated_stocks.append({"symbol": symbol})
                else:
                    self.skipped_stocks.append({"symbol": symbol})
            return symbol, success

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(process_symbol, sym): sym for sym in symbols}

            for i, future in enumerate(as_completed(futures), 1):
                symbol = futures[future]
                attempted += 1
                try:
                    _, success = future.result()
                    if success:
                        succeeded += 1
                    else:
                        failed += 1
                except Exception as e:
                    logger.error(f"Error processing {symbol}: {e}")
                    failed += 1

                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{len(symbols)} processed")

        # Validate files if requested
        validation_results = []
        if validate:
            logger.info("-" * 60)
            logger.info("Validating intraday files...")

            files = self.get_all_intraday_files()
            for file_path in files:
                result = self.validate_intraday_file(
                    file_path, window_start, window_end
                )
                validation_results.append(result)

                if not result["valid"]:
                    self.errors.append(
                        {
                            "symbol": result["symbol"],
                            "error": result["errors"],
                            "type": "validation",
                        }
                    )

        # Generate report
        report = {
            "timestamp": datetime.now().isoformat(),
            "pipeline": "intraday",
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "window_days": (window_end - window_start).days + 1,
            "total_symbols": len(symbols),
            "attempted": attempted,
            "succeeded": succeeded,
            "skipped": skipped,
            "failed": failed,
            "pruned_files": len(self.deleted_files),
            "validation_errors": len([r for r in validation_results if not r["valid"]]),
            "updated": self.updated_stocks,
            "skipped_list": self.skipped_stocks,
            "pruned": self.deleted_files,
            "error_details": self.errors,
            "validation_results": validation_results if validate else [],
        }

        # Save report
        with open(REPORT_FILE, "w") as f:
            json.dump(report, f, indent=2)

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Intraday Update Complete")
        logger.info("=" * 60)
        logger.info(f"Attempted: {attempted}")
        logger.info(f"Succeeded: {succeeded}")
        logger.info(f"Skipped: {skipped}")
        logger.info(f"Failed: {failed}")
        logger.info(f"Pruned files: {len(self.deleted_files)}")
        logger.info(
            f"Validation errors: {len([r for r in validation_results if not r['valid']])}"
        )
        logger.info(f"Report saved to: {REPORT_FILE}")

        return report


def main():
    parser = argparse.ArgumentParser(
        description="SEBI-Compliant Intraday Data Updater for NSE Stocks"
    )
    parser.add_argument(
        "--symbols", type=str, nargs="+", help="Specific symbols to update"
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=None,
        help="Maximum number of symbols to process",
    )
    parser.add_argument(
        "--min-rows", type=int, default=0, help="Minimum rows required to save file"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=20,
        help="Number of parallel workers (default: 20)",
    )
    parser.add_argument(
        "--no-prune", action="store_true", help="Disable pruning of old data"
    )
    parser.add_argument(
        "--no-validate", action="store_true", help="Disable validation after update"
    )
    parser.add_argument(
        "--intraday-dir",
        type=str,
        default=None,
        help="Override intraday data directory",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Number of symbols per batch (default: 200)",
    )
    parser.add_argument(
        "--batch-delay",
        type=int,
        default=45,
        help="Seconds to wait between batches (default: 45)",
    )

    args = parser.parse_args()

    # Create updater
    intraday_dir = Path(args.intraday_dir) if args.intraday_dir else None
    updater = IntradayDataUpdater(
        intraday_dir=intraday_dir, max_workers=args.max_workers
    )

    # Get list of symbols to process
    if args.symbols:
        all_symbols = args.symbols
    else:
        all_symbols = updater.get_symbols_from_daily_data()

    if args.max_symbols:
        all_symbols = all_symbols[: args.max_symbols]

    total_symbols = len(all_symbols)
    batch_size = args.batch_size
    batch_delay = args.batch_delay

    logger.info(f"Total symbols to process: {total_symbols}")
    logger.info(f"Batch size: {batch_size}, Batch delay: {batch_delay}s")

    # Process in batches
    total_attempted = 0
    total_succeeded = 0
    total_failed = 0
    batch_num = 0

    for i in range(0, total_symbols, batch_size):
        batch = all_symbols[i : i + batch_size]
        batch_num += 1

        logger.info(
            f"Processing batch {batch_num}: symbols {i + 1} to {min(i + batch_size, total_symbols)}"
        )

        # Run update for this batch
        report = updater.run_update(
            symbols=batch,
            max_symbols=None,
            min_rows=args.min_rows,
            prune_old=not args.no_prune,
            validate=not args.no_validate,
        )

        total_attempted += report.get("attempted", 0)
        total_succeeded += report.get("succeeded", 0)
        total_failed += report.get("failed", 0)

        logger.info(
            f"Batch {batch_num} complete: {report.get('succeeded', 0)}/{report.get('attempted', 0)} succeeded"
        )

        # Wait before next batch (except for last batch)
        if i + batch_size < total_symbols:
            logger.info(f"Waiting {batch_delay}s before next batch...")
            time_module.sleep(batch_delay)

    # Final report
    logger.info("=" * 60)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total attempted: {total_attempted}")
    logger.info(f"Total succeeded: {total_succeeded}")
    logger.info(f"Total failed: {total_failed}")
    logger.info(f"Batches processed: {batch_num}")

    # Exit with error code if there were failures
    if total_failed > 0:
        logger.warning(f"Pipeline completed with {total_failed} errors")
        sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()
