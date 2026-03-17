#!/usr/bin/env python3
"""
SEBI-Compliant Daily Data Updater

Updates daily OHLCV parquet files for NSE stocks while maintaining
SEBI compliance (31-day lag).

Logic:
- SEBI requires 31-day lag on all market data
- We maintain a 7-day buffer before the deadline
- Data is updated when: last_date < (today - 31 days + 7 days)
- Updates fetch from last_date to SEBI compliance date

Example:
    Today = February 9, 2025
    SEBI Compliance Date = January 9, 2025 (today - 31)
    Update Threshold = January 16, 2025 (SEBI date + 7)
    
    If stock.last_date = January 20, 2025:
        → Data is fresh, skip
    
    If stock.last_date = January 10, 2025:
        → Fetch from Jan 10 to Jan 9 (SEBI limit)
"""

import argparse
import json
import logging
import os
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.data_providers import fetch_daily_ohlcv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('daily_update.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = Path(__file__).parent.parent / 'data'
METADATA_DIR = Path(__file__).parent.parent / 'data' / 'metadata'
INCEPTION_DATES_FILE = METADATA_DIR / 'inception_dates.json'
SEBI_LAG_DAYS = 31
UPDATE_BUFFER_DAYS = 7
REPORT_FILE = 'daily_update_report.json'


class SEBIComplianceError(Exception):
    """Raised when data violates SEBI compliance rules."""
    pass


class DailyDataUpdater:
    """
    Manages SEBI-compliant daily data updates for NSE stock data.
    """
    
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = Path(data_dir)
        self.updated_stocks: List[Dict] = []
        self.skipped_stocks: List[Dict] = []
        self.errors: List[Dict] = []
        self.inception_date_issues: List[Dict] = []  # NEW: Track inception date issues
        self.inception_dates_cache: Dict[str, str] = {}  # NEW: Cache of symbol -> inception date ISO str
        self._load_inception_dates_cache()  # NEW: Load cache from disk

    # ==================== INCEPTION DATE CACHE METHODS ====================

    def _load_inception_dates_cache(self):
        """Load cached inception dates from metadata JSON file."""
        try:
            METADATA_DIR.mkdir(parents=True, exist_ok=True)
            if INCEPTION_DATES_FILE.exists():
                with open(INCEPTION_DATES_FILE, 'r') as f:
                    self.inception_dates_cache = json.load(f)
                logger.info(f"Loaded {len(self.inception_dates_cache)} cached inception dates")
            else:
                logger.info("No inception dates cache file found, starting fresh")
        except Exception as e:
            logger.warning(f"Failed to load inception dates cache: {e}")
            self.inception_dates_cache = {}

    def _save_inception_dates_cache(self):
        """Save inception dates cache to metadata JSON file."""
        try:
            METADATA_DIR.mkdir(parents=True, exist_ok=True)
            with open(INCEPTION_DATES_FILE, 'w') as f:
                json.dump(self.inception_dates_cache, f, indent=2)
            logger.info(f"Saved {len(self.inception_dates_cache)} inception dates to cache")
        except Exception as e:
            logger.error(f"Failed to save inception dates cache: {e}")

    def get_cached_inception_date(self, symbol: str) -> Optional[datetime]:
        """Get inception date from cache, returning None if not cached."""
        if symbol in self.inception_dates_cache:
            try:
                return datetime.fromisoformat(self.inception_dates_cache[symbol])
            except (ValueError, TypeError):
                return None
        return None

    def fetch_and_cache_inception_date(self, symbol: str) -> Optional[datetime]:
        """
        Fetch inception date from data provider and cache it.

        Returns the inception date or None if unavailable.
        """
        # Check cache first
        cached = self.get_cached_inception_date(symbol)
        if cached is not None:
            return cached

        # Import here to avoid circular import
        from backend.data_providers import fetch_stock_inception_date

        # Fetch from provider
        inception = fetch_stock_inception_date(symbol)
        if inception is not None:
            self.inception_dates_cache[symbol] = inception.isoformat()
        return inception

    def get_first_date_from_parquet(self, file_path: Path) -> Optional[datetime]:
        """
        Read the first (earliest) date from a parquet file.
        Returns None if file is corrupted or empty.
        """
        try:
            df = pd.read_parquet(file_path)

            if df.empty:
                return None

            # Handle DatetimeIndex
            if isinstance(df.index, pd.DatetimeIndex):
                first_date = df.index.min()
                return first_date.to_pydatetime() if hasattr(first_date, 'to_pydatetime') else first_date

            # Handle date column
            date_col = next((c for c in df.columns if c.lower() == 'date'), None)
            if date_col:
                df[date_col] = pd.to_datetime(df[date_col])
                return df[date_col].min().to_pydatetime()

            # Try to parse index as datetime
            df.index = pd.to_datetime(df.index)
            first_date = df.index.min()
            return first_date.to_pydatetime() if hasattr(first_date, 'to_pydatetime') else first_date

        except Exception as e:
            logger.error(f"Error reading first date from {file_path}: {e}")
            return None

    def get_sebi_compliance_date(self) -> datetime:
        """
        Calculate the SEBI compliance date (today - 31 days).
        No data newer than this date should be stored.
        """
        return datetime.now() - timedelta(days=SEBI_LAG_DAYS)
    
    def get_update_threshold_date(self) -> datetime:
        """
        Calculate the update threshold date.
        Stocks with data older than this need updating.
        """
        return self.get_sebi_compliance_date() + timedelta(days=UPDATE_BUFFER_DAYS)
    
    def get_all_stock_files(self) -> List[Path]:
        """
        Get all parquet files in the data directory.
        Returns sorted list of file paths.
        """
        parquet_files = []
        
        if not self.data_dir.exists():
            logger.warning(f"Data directory does not exist: {self.data_dir}")
            return parquet_files
        
        # Data is organized in subdirectories (0-9, A, B, C, etc.)
        for subdir in sorted(self.data_dir.iterdir()):
            if subdir.is_dir():
                for parquet_file in sorted(subdir.glob('*.parquet')):
                    parquet_files.append(parquet_file)
        
        return parquet_files
    
    def get_last_date_from_parquet(self, file_path: Path) -> Optional[datetime]:
        """
        Read the last (most recent) date from a parquet file.
        Returns None if file is corrupted or empty.
        """
        try:
            df = pd.read_parquet(file_path)
            
            if df.empty:
                return None
            
            # Handle DatetimeIndex
            if isinstance(df.index, pd.DatetimeIndex):
                last_date = df.index.max()
                return last_date.to_pydatetime() if hasattr(last_date, 'to_pydatetime') else last_date
            
            # Handle date column
            date_col = next((c for c in df.columns if c.lower() == 'date'), None)
            if date_col:
                df[date_col] = pd.to_datetime(df[date_col])
                return df[date_col].max().to_pydatetime()
            
            # Try to parse index as datetime
            df.index = pd.to_datetime(df.index)
            last_date = df.index.max()
            return last_date.to_pydatetime() if hasattr(last_date, 'to_pydatetime') else last_date
            
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return None
    
    def check_stock_needs_update(self, file_path: Path, check_inception: bool = True) -> Dict:
        """
        Check if a stock needs data update, including inception date validation.

        Args:
            file_path: Path to the parquet file
            check_inception: Whether to validate inception date (slower, requires API call)

        Returns:
            Dict with keys:
                - needs_update: bool - whether SEBI compliance update needed
                - needs_full_refetch: bool - whether historical data is missing
                - last_date: Optional[datetime] - last date in parquet
                - first_date: Optional[datetime] - first date in parquet
                - inception_date: Optional[datetime] - actual market inception
                - sebi_compliance_date: datetime - SEBI compliance date
                - reason: str - human-readable reason for status
        """
        symbol = file_path.stem
        last_date = self.get_last_date_from_parquet(file_path)
        first_date = self.get_first_date_from_parquet(file_path)
        sebi_date = self.get_sebi_compliance_date()

        result = {
            'needs_update': False,
            'needs_full_refetch': False,
            'last_date': last_date,
            'first_date': first_date,
            'inception_date': None,
            'sebi_compliance_date': sebi_date,
            'reason': ''
        }

        # Check 1: SEBI compliance (last_date freshness)
        if last_date is None:
            result['needs_update'] = True
            result['needs_full_refetch'] = True
            result['reason'] = 'Unreadable or empty file'
            return result

        update_threshold = self.get_update_threshold_date()
        if last_date < update_threshold:
            result['needs_update'] = True
            result['reason'] = f'Last date {last_date.date()} is before threshold {update_threshold.date()}'

        # Check 2: Inception date completeness
        if check_inception and first_date is not None:
            inception_date = self.fetch_and_cache_inception_date(symbol)
            result['inception_date'] = inception_date

            if inception_date is not None:
                # Allow 5-day tolerance for inception dates (market holidays, listing delays)
                tolerance = timedelta(days=5)
                if first_date > (inception_date + tolerance):
                    result['needs_full_refetch'] = True
                    result['reason'] = (
                        f"Missing historical data: file starts {first_date.date()}, "
                        f"but stock inception is {inception_date.date()}"
                    )
                    logger.warning(
                        f"[{symbol}] Inception gap detected: "
                        f"file starts {first_date.date()} vs inception {inception_date.date()}"
                    )

        return result
    
    def fetch_stock_data(self, symbol: str, up_to_date: datetime) -> Optional[pd.DataFrame]:
        """
        Fetch stock data from data providers up to the SEBI compliance date.
        
        Args:
            symbol: Stock symbol (e.g., 'RELIANCE.NS')
            up_to_date: Maximum date to fetch (SEBI compliance date)
            
        Returns:
            DataFrame with OHLCV data, or None if fetch fails
        """
        try:
            logger.info(f"Fetching {symbol} up to {up_to_date.date()}")
            
            # Fetch using data providers fallback chain
            df = fetch_daily_ohlcv(symbol, period="2y")
            
            if df is None or df.empty:
                logger.warning(f"No data returned for {symbol}")
                return None
            
            # Ensure datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                date_col = next((c for c in df.columns if c.lower() == 'date'), None)
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df.set_index(date_col, inplace=True)
            
            # Remove timezone info if present
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            
            # Trim to SEBI compliance date
            if df.index.max() > up_to_date:
                logger.warning(f"Trimming {symbol} to SEBI limit {up_to_date.date()}")
                df = df[df.index <= up_to_date]
            
            # Standardize column names
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            logger.info(f"Successfully fetched {len(df)} rows for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return None

    def fetch_full_history(self, symbol: str, inception_date: datetime) -> Optional[pd.DataFrame]:
        """
        Fetch complete historical data from inception to SEBI compliance date.

        Used when parquet file is missing early data (inception gap detected).

        Args:
            symbol: Stock symbol (e.g., 'RELIANCE.NS')
            inception_date: Stock's market inception date

        Returns:
            DataFrame with complete OHLCV data, or None if fetch fails
        """
        try:
            sebi_date = self.get_sebi_compliance_date()
            logger.info(
                f"Fetching full history for {symbol} "
                f"from {inception_date.date()} to {sebi_date.date()}"
            )

            # Fetch maximum available history
            df = fetch_daily_ohlcv(symbol, period="max")

            if df is None or df.empty:
                logger.warning(f"No data returned for full history of {symbol}")
                return None

            # Ensure datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                date_col = next((c for c in df.columns if c.lower() == 'date'), None)
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df.set_index(date_col, inplace=True)

            # Remove timezone info if present
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df.index = df.index.tz_localize(None)

            # Trim to SEBI compliance date
            if df.index.max() > sebi_date:
                logger.info(f"Trimming {symbol} to SEBI limit {sebi_date.date()}")
                df = df[df.index <= sebi_date]

            # Standardize column names
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]

            logger.info(
                f"Successfully fetched {len(df)} rows for {symbol} "
                f"(full history: {df.index.min().date()} to {df.index.max().date()})"
            )
            return df

        except Exception as e:
            logger.error(f"Error fetching full history for {symbol}: {e}")
            return None

    def _perform_full_refetch(self, file_path: Path, symbol: str,
                              inception_date: datetime) -> bool:
        """
        Perform a complete refetch of a stock's historical data.

        Called when inception gap is detected - overwrites existing parquet
        with full historical data from inception to SEBI compliance date.

        Args:
            file_path: Path to the parquet file
            symbol: Stock symbol
            inception_date: Stock's market inception date

        Returns:
            True if refetch successful, False otherwise
        """
        try:
            # Fetch full history
            new_data = self.fetch_full_history(symbol, inception_date)

            if new_data is None or new_data.empty:
                logger.warning(f"No data available for full refetch of {symbol}")
                return False

            # Validate SEBI compliance
            sebi_date = self.get_sebi_compliance_date()
            max_date = new_data.index.max()
            if max_date > sebi_date:
                raise SEBIComplianceError(
                    f"Data violation: {symbol} has data from {max_date} "
                    f"which is newer than SEBI compliance date {sebi_date}"
                )

            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Save to parquet (overwrites existing)
            new_data.to_parquet(file_path, compression='snappy')

            first_date = new_data.index.min()
            logger.info(
                f"✅ Full refetch complete for {symbol}: {len(new_data)} rows, "
                f"from {first_date.date()} to {max_date.date()}"
            )
            return True

        except SEBIComplianceError as e:
            logger.error(f"SEBI Compliance Error during full refetch for {symbol}: {e}")
            self.errors.append({'symbol': symbol, 'error': str(e), 'type': 'compliance'})
            return False
        except Exception as e:
            logger.error(f"Error during full refetch for {symbol}: {e}")
            self.errors.append({'symbol': symbol, 'error': str(e), 'type': 'full_refetch'})
            return False

    def update_stock_data(self, file_path: Path, symbol: str, up_to_date: datetime) -> bool:
        """
        Update a single stock's parquet file with fresh data.
        
        Returns:
            True if update successful, False otherwise
        """
        try:
            # Fetch new data
            new_data = self.fetch_stock_data(symbol, up_to_date)
            
            if new_data is None or new_data.empty:
                logger.warning(f"No new data available for {symbol}")
                return False
            
            # Validate SEBI compliance
            max_date = new_data.index.max()
            if max_date > up_to_date:
                raise SEBIComplianceError(
                    f"Data violation: {symbol} has data from {max_date} "
                    f"which is newer than SEBI compliance date {up_to_date}"
                )
            
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to parquet
            new_data.to_parquet(file_path, compression='snappy')
            
            logger.info(f"✅ Updated {symbol}: {len(new_data)} rows, up to {max_date.date()}")
            return True
            
        except SEBIComplianceError as e:
            logger.error(f"SEBI Compliance Error for {symbol}: {e}")
            self.errors.append({'symbol': symbol, 'error': str(e), 'type': 'compliance'})
            return False
        except Exception as e:
            logger.error(f"Error updating {symbol}: {e}")
            self.errors.append({'symbol': symbol, 'error': str(e), 'type': 'update'})
            return False
    
    def run_update(self, sample_size: int = None, force_update: bool = False,
                   symbols: List[str] = None, check_only: bool = False,
                   check_inception: bool = True) -> Dict:
        """
        Run the daily data update pipeline.

        Args:
            sample_size: Number of random stocks to check (None = all)
            force_update: If True, update all stocks regardless of date
            symbols: Specific symbols to update (overrides sample_size)
            check_only: If True, report issues without making updates
            check_inception: If True, validate inception dates (slower)

        Returns:
            Dictionary with update statistics
        """
        logger.info("=" * 60)
        logger.info("Starting Daily Data Update Pipeline")
        logger.info("=" * 60)

        # Calculate key dates
        today = datetime.now()
        sebi_date = self.get_sebi_compliance_date()
        threshold_date = self.get_update_threshold_date()

        logger.info(f"Current date: {today.date()}")
        logger.info(f"SEBI compliance date: {sebi_date.date()}")
        logger.info(f"Update threshold: {threshold_date.date()}")
        logger.info(f"Force update: {force_update}")
        logger.info(f"Check only: {check_only}")
        logger.info(f"Check inception: {check_inception}")
        logger.info("-" * 60)

        # Get stock files to process
        all_files = self.get_all_stock_files()
        logger.info(f"Total stocks in database: {len(all_files)}")

        if len(all_files) == 0:
            logger.error("No stock files found!")
            return {'success': False, 'error': 'No stock files found'}

        # Determine which files to process
        if symbols:
            # Filter to specific symbols
            files_to_process = [f for f in all_files if f.stem in symbols]
            logger.info(f"Processing {len(files_to_process)} specified symbols")
        elif sample_size:
            # Random sample
            files_to_process = random.sample(all_files, min(sample_size, len(all_files)))
            logger.info(f"Checking {len(files_to_process)} random stocks")
        else:
            # Process all
            files_to_process = all_files
            logger.info(f"Processing all {len(files_to_process)} stocks")

        # Process each stock
        for i, file_path in enumerate(files_to_process, 1):
            symbol = file_path.stem

            logger.info(f"\n[{i}/{len(files_to_process)}] Processing {symbol}...")

            # Check if update needed (returns dict with detailed status)
            status = self.check_stock_needs_update(file_path, check_inception=check_inception)

            if status['last_date']:
                logger.info(f"  Last date: {status['last_date'].date()}")
            if status['first_date']:
                logger.info(f"  First date: {status['first_date'].date()}")
            if status['inception_date']:
                logger.info(f"  Inception: {status['inception_date'].date()}")

            # Handle full refetch (inception gap)
            if status['needs_full_refetch']:
                if check_only:
                    logger.warning(f"  ⚠️  {status['reason']}")
                    self.inception_date_issues.append({
                        'symbol': symbol,
                        'first_date': status['first_date'].isoformat() if status['first_date'] else None,
                        'inception_date': status['inception_date'].isoformat() if status['inception_date'] else None,
                        'reason': status['reason'],
                        'action_needed': 'full_refetch'
                    })
                else:
                    logger.info(f"  🔄 Performing full refetch...")
                    inception = status['inception_date']
                    if inception:
                        success = self._perform_full_refetch(file_path, symbol, inception)
                        if success:
                            self.updated_stocks.append({
                                'symbol': symbol,
                                'update_type': 'full_refetch',
                                'previous_first_date': status['first_date'].isoformat() if status['first_date'] else None,
                                'new_first_date': inception.isoformat(),
                                'reason': status['reason']
                            })
                        else:
                            self.inception_date_issues.append({
                                'symbol': symbol,
                                'reason': status['reason'],
                                'action_needed': 'full_refetch',
                                'error': 'Full refetch failed'
                            })
                    else:
                        logger.warning(f"  ⚠️  Cannot refetch: inception date unavailable")
                        self.inception_date_issues.append({
                            'symbol': symbol,
                            'reason': status['reason'],
                            'action_needed': 'full_refetch',
                            'error': 'Inception date unavailable'
                        })
                continue

            # Handle SEBI compliance update
            if status['needs_update'] or force_update:
                if check_only:
                    logger.info(f"  ⚠️  {status['reason']} (check-only mode)")
                    self.updated_stocks.append({
                        'symbol': symbol,
                        'update_type': 'incremental',
                        'last_date': status['last_date'].isoformat() if status['last_date'] else None,
                        'sebi_compliance_date': status['sebi_compliance_date'].isoformat(),
                        'reason': status['reason'],
                        'skipped': 'check_only_mode'
                    })
                else:
                    logger.info(f"  🔄 Updating...")
                    success = self.update_stock_data(
                        file_path, symbol, status['sebi_compliance_date']
                    )
                    if success:
                        self.updated_stocks.append({
                            'symbol': symbol,
                            'update_type': 'incremental',
                            'previous_date': status['last_date'].isoformat() if status['last_date'] else None,
                            'new_date': status['sebi_compliance_date'].isoformat()
                        })
            else:
                logger.info(f"  ⏭️  Skipping (data is fresh)")
                self.skipped_stocks.append({
                    'symbol': symbol,
                    'last_date': status['last_date'].isoformat() if status['last_date'] else None
                })

        # Save inception dates cache
        if check_inception:
            self._save_inception_dates_cache()

        # Generate report
        report = {
            'timestamp': datetime.now().isoformat(),
            'pipeline': 'daily',
            'check_only': check_only,
            'check_inception': check_inception,
            'sebi_compliance_date': sebi_date.isoformat(),
            'update_threshold': threshold_date.isoformat(),
            'total_stocks': len(all_files),
            'checked_stocks': len(files_to_process),
            'updated_stocks': len(self.updated_stocks),
            'skipped_stocks': len(self.skipped_stocks),
            'inception_date_issues': len(self.inception_date_issues),
            'errors': len(self.errors),
            'updated': self.updated_stocks,
            'skipped': self.skipped_stocks,
            'inception_issues': self.inception_date_issues,
            'error_details': self.errors
        }

        # Save report
        with open(REPORT_FILE, 'w') as f:
            json.dump(report, f, indent=2)

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Daily Update Complete")
        logger.info("=" * 60)
        logger.info(f"Total checked: {len(files_to_process)}")
        logger.info(f"Updated: {len(self.updated_stocks)}")
        logger.info(f"Skipped (fresh): {len(self.skipped_stocks)}")
        logger.info(f"Inception issues: {len(self.inception_date_issues)}")
        logger.info(f"Errors: {len(self.errors)}")
        logger.info(f"Report saved to: {REPORT_FILE}")

        return report


def main():
    parser = argparse.ArgumentParser(
        description='SEBI-Compliant Daily Data Updater for NSE Stocks'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=None,
        help='Number of random stocks to check (default: all)'
    )
    parser.add_argument(
        '--force-update',
        action='store_true',
        help='Force update all stocks regardless of date'
    )
    parser.add_argument(
        '--symbols',
        type=str,
        nargs='+',
        help='Specific symbols to update'
    )
    parser.add_argument(
        '--data-dir',
        type=str,
        default=None,
        help='Override data directory'
    )
    parser.add_argument(
        '--check-only',
        action='store_true',
        help='Report issues without making updates (audit mode)'
    )
    parser.add_argument(
        '--no-check-inception',
        action='store_true',
        help='Skip inception date checking (faster)'
    )

    args = parser.parse_args()

    # Create updater
    data_dir = Path(args.data_dir) if args.data_dir else DATA_DIR
    updater = DailyDataUpdater(data_dir=data_dir)

    # Run update
    report = updater.run_update(
        sample_size=args.sample_size,
        force_update=args.force_update,
        symbols=args.symbols,
        check_only=args.check_only,
        check_inception=not args.no_check_inception
    )
    
    # Exit with error code if there were failures
    if report.get('errors', 0) > 0:
        logger.warning(f"Pipeline completed with {report['errors']} errors")
        sys.exit(1)
    
    sys.exit(0)


if __name__ == '__main__':
    main()
