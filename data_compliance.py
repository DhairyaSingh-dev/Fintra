"""
Data Compliance Module
Handles SEBI compliance requirements including 31-day data lag
and data availability tracking.
"""
import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# Constants for SEBI compliance
DATA_LAG_DAYS = 31
DATA_DIRECTORY = os.path.join(os.path.dirname(__file__), 'data')


class DataComplianceManager:
    """
    Manages SEBI compliance requirements for data handling.
    Ensures 31-day lag and tracks data availability.
    """
    
    def __init__(self):
        self.data_lag_days = DATA_LAG_DAYS
        self.data_dir = DATA_DIRECTORY
        self._cache_data_availability = None
        self._cache_timestamp = None
        
    def get_current_date_with_lag(self) -> datetime:
        """
        Get the current effective date with 31-day lag applied.
        This ensures no data newer than 31 days is displayed.
        """
        today = datetime.now()
        lag_date = today - timedelta(days=self.data_lag_days)
        return lag_date
    
    def check_data_availability(self) -> Dict:
        """
        Check the availability of parquet data files.
        Returns information about the data range available.
        """
        # Return cached result if less than 5 minutes old
        if self._cache_data_availability and self._cache_timestamp:
            if (datetime.now() - self._cache_timestamp).seconds < 300:
                return self._cache_data_availability
        
        try:
            # Find any parquet file to check date range
            # All parquet files should have the same date range
            sample_files = []
            for letter_dir in os.listdir(self.data_dir):
                letter_path = os.path.join(self.data_dir, letter_dir)
                if os.path.isdir(letter_path):
                    files = [f for f in os.listdir(letter_path) if f.endswith('.parquet')]
                    if files:
                        sample_files.append(os.path.join(letter_path, files[0]))
                        if len(sample_files) >= 3:  # Check a few files to ensure consistency
                            break
            
            if not sample_files:
                return {
                    'available': False,
                    'message': 'No parquet data files found',
                    'last_date': None,
                    'first_date': None,
                    'lag_date': self.get_current_date_with_lag().strftime('%Y-%m-%d'),
                    'days_lag': self.data_lag_days
                }
            
            # Read first file to get date range
            df = pd.read_parquet(sample_files[0])
            
            # Ensure datetime index
            if not isinstance(df.index, pd.DatetimeIndex):
                date_col = next((c for c in df.columns if c.lower() == 'date'), None)
                if date_col:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df.set_index(date_col, inplace=True)
                else:
                    df.index = pd.to_datetime(df.index)
            
            first_date = df.index.min()
            last_date = df.index.max()
            lag_date = self.get_current_date_with_lag()
            
            # Check if we need to enforce additional lag
            effective_last_date = min(last_date, lag_date)
            needs_manual_lag = lag_date > last_date
            
            result = {
                'available': True,
                'first_date': first_date.strftime('%Y-%m-%d'),
                'last_date': last_date.strftime('%Y-%m-%d'),
                'lag_date': lag_date.strftime('%Y-%m-%d'),
                'effective_last_date': effective_last_date.strftime('%Y-%m-%d'),
                'days_lag': self.data_lag_days,
                'needs_manual_lag': needs_manual_lag,
                'total_days_available': (last_date - first_date).days,
                'data_freshness_days': (datetime.now() - last_date).days,
                'message': self._generate_availability_message(first_date, last_date, lag_date, needs_manual_lag)
            }
            
            self._cache_data_availability = result
            self._cache_timestamp = datetime.now()
            
            return result
            
        except Exception as e:
            logger.error(f"Error checking data availability: {e}")
            return {
                'available': False,
                'message': f'Error checking data: {str(e)}',
                'last_date': None,
                'first_date': None,
                'lag_date': self.get_current_date_with_lag().strftime('%Y-%m-%d'),
                'days_lag': self.data_lag_days
            }
    
    def _generate_availability_message(self, first_date: datetime, last_date: datetime, 
                                      lag_date: datetime, needs_manual_lag: bool) -> str:
        """Generate a user-friendly message about data availability."""
        data_range = (last_date - first_date).days
        
        if needs_manual_lag:
            days_behind = (lag_date - last_date).days
            return (
                f"📊 Data Range: {first_date.strftime('%Y-%m-%d')} to {last_date.strftime('%Y-%m-%d')} "
                f"({data_range} days)\n"
                f"⏱️ SEBI Compliance: 31-day lag enforced (effective date: {lag_date.strftime('%Y-%m-%d')})\n"
                f"⚠️  Data is {days_behind} days behind the lag requirement"
            )
        else:
            return (
                f"📊 Data Range: {first_date.strftime('%Y-%m-%d')} to {last_date.strftime('%Y-%m-%d')} "
                f"({data_range} days)\n"
                f"✅ SEBI Compliance: 31-day lag active (effective date: {lag_date.strftime('%Y-%m-%d')})"
            )
    
    def filter_data_with_lag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter a DataFrame to only include data up to the lag date.
        Ensures SEBI compliance by excluding recent data.
        """
        if df.empty:
            return df
        
        # Ensure datetime index
        if not isinstance(df.index, pd.DatetimeIndex):
            date_col = next((c for c in df.columns if c.lower() == 'date'), None)
            if date_col:
                df[date_col] = pd.to_datetime(df[date_col])
                df = df.set_index(date_col)
            else:
                df.index = pd.to_datetime(df.index)
        
        lag_date = self.get_current_date_with_lag()
        
        # Filter to only include data up to lag date
        filtered_df = df[df.index <= lag_date].copy()
        
        if len(filtered_df) < len(df):
            logger.info(f"Applied {self.data_lag_days}-day lag: {len(df) - len(filtered_df)} rows excluded")
        
        return filtered_df
    
    def get_informatics_html(self) -> str:
        """Generate HTML for displaying data informatics."""
        info = self.check_data_availability()
        
        if not info['available']:
            return f"""
            <div class="data-informatics warning">
                <h4>📊 Data Availability</h4>
                <p class="warning-text">{info['message']}</p>
                <p>SEBI Compliance: {info['days_lag']}-day lag enforced</p>
            </div>
            """
        
        status_class = 'warning' if info.get('needs_manual_lag') else 'success'
        lag_indicator = '⚠️' if info.get('needs_manual_lag') else '✅'
        
        return f"""
        <div class="data-informatics {status_class}">
            <h4>📊 Data Informatics</h4>
            <div class="info-grid">
                <div class="info-item">
                    <span class="label">Data Range:</span>
                    <span class="value">{info['first_date']} to {info['last_date']}</span>
                </div>
                <div class="info-item">
                    <span class="label">Total History:</span>
                    <span class="value">{info['total_days_available']} trading days</span>
                </div>
                <div class="info-item">
                    <span class="label">SEBI Compliance:</span>
                    <span class="value">{info['days_lag']}-day mandatory lag</span>
                </div>
                <div class="info-item">
                    <span class="label">Effective Date:</span>
                    <span class="value">{info['effective_last_date']}</span>
                </div>
                <div class="info-item">
                    <span class="label">Data Freshness:</span>
                    <span class="value">{info['data_freshness_days']} days old</span>
                </div>
                <div class="info-item status">
                    <span class="label">Status:</span>
                    <span class="value">{lag_indicator} Compliant</span>
                </div>
            </div>
            <p class="compliance-notice">
                🔒 This platform maintains a strict {info['days_lag']}-day data lag 
                in accordance with SEBI regulations. No current or recent data is displayed.
            </p>
        </div>
        """


# Global instance
data_compliance = DataComplianceManager()


def get_parquet_path(symbol: str) -> Optional[str]:
    """Get the parquet file path for a symbol."""
    if not symbol or len(symbol) == 0:
        return None
    first_char = symbol[0].upper()
    if first_char.isdigit():
        first_char = '0-9'
    file_path = os.path.join(DATA_DIRECTORY, first_char, f"{symbol}.parquet")
    return file_path if os.path.exists(file_path) else None


def load_stock_data_with_compliance(symbol: str) -> Tuple[Optional[pd.DataFrame], Dict]:
    """
    Load stock data with SEBI compliance applied.
    Returns both the filtered DataFrame and compliance info.
    """
    file_path = get_parquet_path(symbol)
    
    if not file_path:
        return None, {'error': f'No data found for symbol {symbol}'}
    
    try:
        df = pd.read_parquet(file_path)
        
        # Apply compliance filtering
        filtered_df = data_compliance.filter_data_with_lag(df)
        
        compliance_info = {
            'original_rows': len(df),
            'filtered_rows': len(filtered_df),
            'rows_excluded': len(df) - len(filtered_df),
            'lag_days': DATA_LAG_DAYS,
            'effective_last_date': filtered_df.index.max().strftime('%Y-%m-%d') if not filtered_df.empty else None
        }
        
        return filtered_df, compliance_info
        
    except Exception as e:
        logger.error(f"Error loading data for {symbol}: {e}")
        return None, {'error': str(e)}


# Flask route helper
def get_data_availability_endpoint():
    """Endpoint helper for data availability check."""
    return data_compliance.check_data_availability()
