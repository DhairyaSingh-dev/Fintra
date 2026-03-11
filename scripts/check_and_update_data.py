#!/usr/bin/env python3
"""
SEBI-Compliant Data Update Pipeline

This script checks the freshness of Parquet data files and updates them
when they approach the SEBI 31-day lag deadline.

Logic:
- SEBI requires 31-day lag on all market data
- We maintain a 7-day buffer before the deadline
- Data is updated only when: last_date < (today - 31 days + 7 days)
- Updates are done in batches to minimize API calls

Example:
    Today = February 9, 2025
    SEBI Deadline = January 9, 2025
    Update Threshold = January 16, 2025 (7-day buffer)
    
    If stock.last_date = January 20, 2025:
        → Data is fresh, skip
    
    If stock.last_date = January 10, 2025:
        → Data needs update (fetch up to January 9, 2025)
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
import sys
import os

# Add parent directory to path so we can import from the main app
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_providers import fetch_daily_ohlcv
from data_compliance import get_intraday_window, INTRADAY_DIRECTORY

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('data_update.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
DATA_DIR = Path(__file__).parent.parent / 'data'
INTRADAY_DIR = Path(INTRADAY_DIRECTORY)
SEBI_LAG_DAYS = 31
UPDATE_BUFFER_DAYS = 7
REPORT_FILE = 'data_update_report.json'
ERROR_LOG = 'data_update_errors.log'


class SEBIComplianceError(Exception):
    """Raised when data violates SEBI compliance rules."""
    pass


class DataUpdatePipeline:
    """
    Manages the SEBI-compliant data update pipeline for NSE stock data.
    """
    
    def __init__(self, data_dir: Path = DATA_DIR):
        self.data_dir = data_dir
        self.updated_stocks = []
        self.errors = []
        self.skipped_stocks = []
        self.intraday_errors = []
        
    def get_sebi_compliance_date(self) -> datetime:
        """
        Calculate the SEBI compliance date (today - 31 days).
        No data newer than this date should be stored or displayed.
        """
        today = datetime.now()
        sebi_date = today - timedelta(days=SEBI_LAG_DAYS)
        return sebi_date
    
    def get_update_threshold_date(self) -> datetime:
        """
        Calculate the update threshold date.
        Stocks with data older than this need updating.
        """
        sebi_date = self.get_sebi_compliance_date()
        threshold_date = sebi_date + timedelta(days=UPDATE_BUFFER_DAYS)
        return threshold_date
    
    def get_all_stock_files(self) -> List[Path]:
        """
        Get all parquet files in the data directory.
        Returns list of file paths.
        """
        parquet_files = []
        
        # Data is organized in subdirectories (0-9, A, B, C, etc.)
        for subdir in self.data_dir.iterdir():
            if subdir.is_dir():
                for parquet_file in subdir.glob('*.parquet'):
                    parquet_files.append(parquet_file)
        
        return sorted(parquet_files)
    
    def get_last_date_from_parquet(self, file_path: Path) -> Optional[datetime]:
        """
        Read the last (most recent) date from a parquet file.
        Returns None if file is corrupted or empty.
        """
        try:
            df = pd.read_parquet(file_path)
            
            # Ensure datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                date_col = next((c for c in df.columns if c.lower() == 'date'), None)
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df.set_index(date_col, inplace=True)
                else:
                    df.index = pd.to_datetime(df.index)
            
            if df.empty:
                return None
                
            last_date = df.index.max()
            return last_date.to_pydatetime() if hasattr(last_date, 'to_pydatetime') else last_date
            
        except Exception as e:
            logger.error(f"Error reading {file_path}: {e}")
            return None
    
    def check_stock_needs_update(self, file_path: Path) -> Tuple[bool, Optional[datetime], Optional[datetime]]:
        """
        Check if a stock needs data update.
        
        Returns:
            Tuple of (needs_update, last_date, sebi_compliance_date)
        """
        last_date = self.get_last_date_from_parquet(file_path)
        
        if last_date is None:
            logger.warning(f"Could not read {file_path}, marking for update")
            return True, None, self.get_sebi_compliance_date()
        
        update_threshold = self.get_update_threshold_date()
        
        # Check if data is within our update buffer
        needs_update = last_date < update_threshold
        
        return needs_update, last_date, self.get_sebi_compliance_date()
    
    def fetch_stock_data(self, symbol: str, up_to_date: datetime) -> Optional[pd.DataFrame]:
        """
        Fetch stock data from yfinance up to the SEBI compliance date.
        
        Args:
            symbol: Stock symbol (e.g., 'RELIANCE.NS')
            up_to_date: Maximum date to fetch (SEBI compliance date)
            
        Returns:
            DataFrame with OHLCV data, or None if fetch fails
        """
        try:
            # Calculate start date (fetch last 2 years of data)
            end_date = up_to_date.strftime('%Y-%m-%d')
            start_date = (up_to_date - timedelta(days=730)).strftime('%Y-%m-%d')
            
            logger.info(f"Fetching {symbol} from {start_date} to {end_date}")
            
            # Fetch using data providers fallback chain
            df = fetch_daily_ohlcv(symbol, period="2y")
            
            if df is None or df.empty:
                logger.warning(f"No data returned for {symbol}")
                return None
            
            # Validate data doesn't violate SEBI compliance
            if len(df) > 0:
                max_date = df.index.max()
                if max_date > up_to_date:
                    logger.warning(f"Data violation detected for {symbol}, trimming to SEBI limit")
                    df = df[df.index <= up_to_date]
            
            # Standardize column names (yfinance uses PascalCase)
            df.columns = [col.lower().replace(' ', '_') for col in df.columns]
            
            logger.info(f"Successfully fetched {len(df)} rows for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            return None
    
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
            
            # Validate SEBI compliance one more time
            max_date = new_data.index.max()
            if max_date > up_to_date:
                raise SEBIComplianceError(
                    f"Data violation: {symbol} has data from {max_date} which is newer than "
                    f"SEBI compliance date {up_to_date}"
                )
            
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

    def validate_intraday_data(self, sample_size: int = 100) -> Dict:
        if not INTRADAY_DIR.exists():
            return {
                'available': False,
                'message': f'Intraday directory not found: {INTRADAY_DIR}',
                'checked_files': 0,
                'errors': 0
            }
        files = []
        for subdir in INTRADAY_DIR.iterdir():
            if subdir.is_dir():
                files.extend(subdir.glob('*.parquet'))
        if not files:
            return {
                'available': False,
                'message': 'No intraday parquet files found',
                'checked_files': 0,
                'errors': 0
            }
        sample_files = random.sample(files, min(sample_size, len(files)))
        window_start, window_end = get_intraday_window()
        required_cols = {'Open', 'High', 'Low', 'Close', 'Volume'}
        for file_path in sample_files:
            symbol = file_path.stem
            try:
                df = pd.read_parquet(file_path)
                if not isinstance(df.index, pd.DatetimeIndex):
                    date_col = next((c for c in df.columns if c.lower() == 'date'), None)
                    if date_col:
                        df[date_col] = pd.to_datetime(df[date_col])
                        df.set_index(date_col, inplace=True)
                    else:
                        df.index = pd.to_datetime(df.index)
                if df.empty:
                    self.intraday_errors.append({'symbol': symbol, 'error': 'empty'})
                    continue
                df.columns = [col.lower().replace(' ', '_') for col in df.columns]
                df.columns = [col.title().replace('_', '') for col in df.columns]
                if not required_cols.issubset(set(df.columns)):
                    self.intraday_errors.append({'symbol': symbol, 'error': 'missing_columns'})
                    continue
                min_dt = df.index.min()
                max_dt = df.index.max()
                if min_dt < window_start or max_dt > window_end:
                    self.intraday_errors.append({
                        'symbol': symbol,
                        'error': 'out_of_window',
                        'min': min_dt.isoformat(),
                        'max': max_dt.isoformat(),
                        'window_start': window_start.isoformat(),
                        'window_end': window_end.isoformat()
                    })
            except Exception as e:
                self.intraday_errors.append({'symbol': symbol, 'error': str(e)})
        return {
            'available': True,
            'message': 'Intraday validation complete',
            'checked_files': len(sample_files),
            'errors': len(self.intraday_errors),
            'error_details': self.intraday_errors,
            'window_start': window_start.isoformat(),
            'window_end': window_end.isoformat()
        }
    
    def run_pipeline(self, sample_size: int = 100, force_update: bool = False,
                     validate_intraday: bool = True, intraday_sample_size: int = 100) -> Dict:
        """
        Run the complete data update pipeline.
        
        Args:
            sample_size: Number of random stocks to check
            force_update: If True, update all stocks regardless of date
            
        Returns:
            Dictionary with update statistics
        """
        logger.info("=" * 60)
        logger.info("Starting SEBI-Compliant Data Update Pipeline")
        logger.info("=" * 60)
        
        # Calculate key dates
        today = datetime.now()
        sebi_date = self.get_sebi_compliance_date()
        threshold_date = self.get_update_threshold_date()
        
        logger.info(f"Current date: {today.date()}")
        logger.info(f"SEBI compliance date: {sebi_date.date()}")
        logger.info(f"Update threshold: {threshold_date.date()}")
        logger.info(f"Sample size: {sample_size}")
        logger.info(f"Force update: {force_update}")
        logger.info("-" * 60)
        
        # Get all stock files
        all_files = self.get_all_stock_files()
        logger.info(f"Total stocks in database: {len(all_files)}")
        
        if len(all_files) == 0:
            logger.error("No stock files found!")
            return {'success': False, 'error': 'No stock files found'}
        
        # Sample random stocks
        sample_files = random.sample(all_files, min(sample_size, len(all_files)))
        logger.info(f"Checking {len(sample_files)} random stocks")
        
        # Process each stock
        for i, file_path in enumerate(sample_files, 1):
            symbol = file_path.stem  # e.g., 'RELIANCE.NS'
            
            logger.info(f"\n[{i}/{len(sample_files)}] Processing {symbol}...")
            
            # Check if update needed
            needs_update, last_date, sebi_compliance = self.check_stock_needs_update(file_path)
            
            if last_date:
                logger.info(f"  Last date: {last_date.date()}")
            
            if not needs_update and not force_update:
                logger.info(f"  ⏭️  Skipping (data is fresh)")
                self.skipped_stocks.append({
                    'symbol': symbol,
                    'last_date': last_date.isoformat() if last_date else None
                })
                continue
            
            # Update the stock
            logger.info(f"  🔄 Updating...")
            success = self.update_stock_data(file_path, symbol, sebi_compliance)
            
            if success:
                self.updated_stocks.append({
                    'symbol': symbol,
                    'previous_date': last_date.isoformat() if last_date else None,
                    'new_date': sebi_compliance.isoformat()
                })
        
        intraday_report = None
        if validate_intraday:
            intraday_report = self.validate_intraday_data(sample_size=intraday_sample_size)

        total_errors = len(self.errors)
        if intraday_report:
            total_errors += intraday_report.get('errors', 0)

        # Generate report
        report = {
            'timestamp': datetime.now().isoformat(),
            'sebi_compliance_date': sebi_date.isoformat(),
            'update_threshold': threshold_date.isoformat(),
            'sample_size': sample_size,
            'total_stocks': len(all_files),
            'checked_stocks': len(sample_files),
            'updated_stocks': len(self.updated_stocks),
            'skipped_stocks': len(self.skipped_stocks),
            'errors': total_errors,
            'updated': self.updated_stocks,
            'skipped': self.skipped_stocks,
            'error_details': self.errors,
            'intraday_validation': intraday_report
        }
        
        # Save report
        with open(REPORT_FILE, 'w') as f:
            json.dump(report, f, indent=2)
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Pipeline Complete")
        logger.info("=" * 60)
        logger.info(f"Total checked: {len(sample_files)}")
        logger.info(f"Updated: {len(self.updated_stocks)}")
        logger.info(f"Skipped (fresh): {len(self.skipped_stocks)}")
        logger.info(f"Errors: {len(self.errors)}")
        logger.info(f"Report saved to: {REPORT_FILE}")
        
        return report


def main():
    parser = argparse.ArgumentParser(
        description='SEBI-Compliant Data Update Pipeline for NSE Stocks'
    )
    parser.add_argument(
        '--sample-size',
        type=int,
        default=100,
        help='Number of random stocks to check (default: 100)'
    )
    parser.add_argument(
        '--force-update',
        type=str,
        default='false',
        help='Force update all stocks regardless of date (true/false)'
    )
    parser.add_argument(
        '--validate-intraday',
        type=str,
        default='true',
        help='Validate intraday dataset integrity (true/false)'
    )
    parser.add_argument(
        '--intraday-sample-size',
        type=int,
        default=100,
        help='Number of intraday files to check (default: 100)'
    )
    
    args = parser.parse_args()
    
    force_update = args.force_update.lower() == 'true'
    validate_intraday = args.validate_intraday.lower() == 'true'
    
    # Run pipeline
    pipeline = DataUpdatePipeline()
    report = pipeline.run_pipeline(
        sample_size=args.sample_size,
        force_update=force_update,
        validate_intraday=validate_intraday,
        intraday_sample_size=args.intraday_sample_size
    )
    
    # Exit with error code if there were failures
    if report.get('errors', 0) > 0:
        logger.warning(f"Pipeline completed with {report['errors']} errors")
        sys.exit(1)
    
    sys.exit(0)


if __name__ == '__main__':
    main()
