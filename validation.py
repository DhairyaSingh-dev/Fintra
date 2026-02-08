"""
Input Validation Module
Provides comprehensive backend validation for all API endpoints.
Protects against XSS, injection attacks, and invalid data.
"""
import logging
import os
import re
from datetime import datetime, timezone
from html import escape
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# XSS and injection patterns to reject
XSS_PATTERNS = [
    '<script',
    '<img',
    'onerror',
    'onclick',
    'onload',
    'onmouseover',
    'javascript:',
    'data:text/html',
    'data:application/javascript',
    'eval(',
    'expression(',
    'alert(',
    'document.cookie',
    'document.write',
    'window.location',
    '.innerhtml',
    'fromcharcode',
]

# HTML tags to strip for sanitization
HTML_TAGS = re.compile(r'<[^>]+>')

# Symbol validation pattern - allows alphanumeric, dots, and dashes
SYMBOL_PATTERN = re.compile(r'^[A-Z0-9][A-Z0-9.-]*$', re.IGNORECASE)


def get_available_symbols() -> List[str]:
    """
    Get list of available stock symbols from the data directory.
    Returns cached list for performance.
    """
    symbols = []
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    
    try:
        if os.path.exists(data_dir):
            for letter_dir in os.listdir(data_dir):
                letter_path = os.path.join(data_dir, letter_dir)
                if os.path.isdir(letter_path):
                    for file in os.listdir(letter_path):
                        if file.endswith('.parquet'):
                            # Extract symbol from filename (e.g., "RELIANCE.NS.parquet" -> "RELIANCE.NS")
                            symbol = file.replace('.parquet', '')
                            symbols.append(symbol.upper())
    except Exception as e:
        logger.error(f"Error loading symbol whitelist: {e}")
    
    return symbols


# Cache the symbol whitelist
_SYMBOL_WHITELIST: Optional[List[str]] = None

def get_symbol_whitelist() -> List[str]:
    """Get cached symbol whitelist."""
    global _SYMBOL_WHITELIST
    if _SYMBOL_WHITELIST is None:
        try:
            _SYMBOL_WHITELIST = get_available_symbols()
            logger.info(f"Loaded {len(_SYMBOL_WHITELIST)} symbols into whitelist")
        except Exception as e:
            logger.warning(f"Could not load symbol whitelist: {e}. Using permissive validation.")
            _SYMBOL_WHITELIST = []
    return _SYMBOL_WHITELIST


def validate_symbol(symbol: str) -> Tuple[bool, str]:
    """
    Validate stock symbol against backend whitelist.
    
    Args:
        symbol: Stock symbol to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not symbol or not isinstance(symbol, str):
        return False, "Symbol is required and must be a string"
    
    symbol = symbol.strip().upper()
    
    if len(symbol) < 1 or len(symbol) > 50:
        return False, "Symbol must be between 1 and 50 characters"
    
    if not SYMBOL_PATTERN.match(symbol):
        return False, "Symbol contains invalid characters. Only alphanumeric, dots, and dashes allowed"
    
    # Check against whitelist (if available)
    try:
        whitelist = get_symbol_whitelist()
        if whitelist and symbol not in whitelist:
            return False, f"Symbol '{symbol}' is not available in our database. Please check the symbol and try again."
    except Exception as e:
        logger.warning(f"Could not validate symbol against whitelist: {e}")
        # If whitelist can't be loaded, allow the symbol (basic validation passed)
        pass
    
    return True, ""


def sanitize_string(value: str, max_length: int = 500, allow_html: bool = False) -> Tuple[str, Optional[str]]:
    """
    Sanitize string input to prevent XSS and injection attacks.
    
    Args:
        value: String to sanitize
        max_length: Maximum allowed length
        allow_html: Whether to allow HTML tags (default: False - strips all HTML)
        
    Returns:
        Tuple of (sanitized_value, error_message)
    """
    if value is None:
        return "", None
    
    if not isinstance(value, str):
        return "", "Value must be a string"
    
    # Check for XSS patterns
    value_lower = value.lower()
    for pattern in XSS_PATTERNS:
        if pattern in value_lower:
            return "", f"Invalid characters detected in input (XSS attempt)"
    
    # Strip HTML if not allowed
    if not allow_html:
        value = HTML_TAGS.sub('', value)
    
    # Trim whitespace
    value = value.strip()
    
    # Check length
    if len(value) > max_length:
        return "", f"Input exceeds maximum length of {max_length} characters"
    
    # Escape any remaining HTML entities
    value = escape(value)
    
    return value, None


def validate_float(value: Any, field_name: str, min_val: Optional[float] = None, 
                   max_val: Optional[float] = None) -> Tuple[Optional[float], Optional[str]]:
    """
    Validate and convert value to float with range checking.
    
    Args:
        value: Value to validate
        field_name: Name of field for error messages
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        
    Returns:
        Tuple of (float_value, error_message)
    """
    if value is None:
        return None, f"{field_name} is required"
    
    try:
        # Handle both int and float
        if isinstance(value, bool):
            return None, f"{field_name} must be a number, not a boolean"
        
        float_val = float(value)
        
        # Check for NaN or Infinity
        if not (float_val == float_val):  # NaN check
            return None, f"{field_name} must be a valid number"
        if float_val == float('inf') or float_val == float('-inf'):
            return None, f"{field_name} must be a finite number"
        
        # Range checks
        if min_val is not None and float_val < min_val:
            return None, f"{field_name} must be at least {min_val}"
        
        if max_val is not None and float_val > max_val:
            return None, f"{field_name} must not exceed {max_val}"
        
        return float_val, None
        
    except (ValueError, TypeError) as e:
        return None, f"{field_name} must be a valid number"


def validate_int(value: Any, field_name: str, min_val: Optional[int] = None, 
                 max_val: Optional[int] = None) -> Tuple[Optional[int], Optional[str]]:
    """
    Validate and convert value to integer with range checking.
    
    Args:
        value: Value to validate
        field_name: Name of field for error messages
        min_val: Minimum allowed value
        max_val: Maximum allowed value
        
    Returns:
        Tuple of (int_value, error_message)
    """
    if value is None:
        return None, f"{field_name} is required"
    
    try:
        # Handle both int and float (if float, check it's whole number)
        if isinstance(value, bool):
            return None, f"{field_name} must be a number, not a boolean"
        
        float_val = float(value)
        
        # Check if it's a whole number
        if not float_val.is_integer():
            return None, f"{field_name} must be a whole number"
        
        int_val = int(float_val)
        
        # Range checks
        if min_val is not None and int_val < min_val:
            return None, f"{field_name} must be at least {min_val}"
        
        if max_val is not None and int_val > max_val:
            return None, f"{field_name} must not exceed {max_val}"
        
        return int_val, None
        
    except (ValueError, TypeError) as e:
        return None, f"{field_name} must be a valid integer"


def validate_date(date_str: str, field_name: str, allow_future: bool = False) -> Tuple[Optional[datetime], Optional[str]]:
    """
    Validate date string format and constraints.
    
    Args:
        date_str: Date string to validate (expected format: YYYY-MM-DD)
        field_name: Name of field for error messages
        allow_future: Whether to allow future dates
        
    Returns:
        Tuple of (datetime_value, error_message)
    """
    if not date_str:
        return None, f"{field_name} is required"
    
    if not isinstance(date_str, str):
        return None, f"{field_name} must be a string in YYYY-MM-DD format"
    
    # Validate format using regex
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return None, f"{field_name} must be in YYYY-MM-DD format (e.g., 2024-01-15)"
    
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()
        
        # Check if date is in the future
        today = datetime.now(timezone.utc).date()
        if not allow_future and date_obj > today:
            return None, f"{field_name} cannot be in the future"
        
        return datetime.combine(date_obj, datetime.min.time()), None
        
    except ValueError as e:
        return None, f"{field_name} is not a valid date"


def validate_date_range(start_date: str, end_date: str) -> Tuple[bool, str]:
    """
    Validate that start_date <= end_date.
    
    Args:
        start_date: Start date string (YYYY-MM-DD)
        end_date: End date string (YYYY-MM-DD)
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    start_val, start_err = validate_date(start_date, "Start date", allow_future=False)
    if start_err:
        return False, start_err
    
    end_val, end_err = validate_date(end_date, "End date", allow_future=False)
    if end_err:
        return False, end_err
    
    if start_val.date() > end_val.date():
        return False, "Start date cannot be after end date"
    
    return True, ""


def validate_required_fields(data: Dict, required_fields: List[str]) -> Tuple[bool, str]:
    """
    Validate that all required fields are present and not None/empty.
    
    Args:
        data: Dictionary containing the data
        required_fields: List of required field names
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not isinstance(data, dict):
        return False, "Request data must be a JSON object"
    
    missing = [field for field in required_fields if field not in data or data[field] is None]
    
    if missing:
        return False, f"Missing required fields: {', '.join(missing)}"
    
    return True, ""


def validate_strategy(strategy: str) -> Tuple[bool, str]:
    """
    Validate backtesting strategy name.
    
    Args:
        strategy: Strategy name to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    valid_strategies = ['golden_cross', 'rsi', 'macd', 'composite', 'momentum', 'mean_reversion', 'breakout']
    
    if not strategy or not isinstance(strategy, str):
        return False, f"Strategy is required and must be one of: {', '.join(valid_strategies)}"
    
    strategy = strategy.lower().strip()
    
    if strategy not in valid_strategies:
        return False, f"Invalid strategy. Must be one of: {', '.join(valid_strategies)}"
    
    return True, ""


# Portfolio validation constants
POSITION_QUANTITY_MIN = 0
POSITION_QUANTITY_MAX = 100000
POSITION_PRICE_MIN = 0
POSITION_PRICE_MAX = 1000000
POSITION_NOTES_MAX_LENGTH = 500

# Backtest validation constants
BACKTEST_BALANCE_MIN = 1000
BACKTEST_BALANCE_MAX = 10000000
BACKTEST_ATR_MULTIPLIER_MIN = 0.5
BACKTEST_ATR_MULTIPLIER_MAX = 20.0
BACKTEST_RISK_PER_TRADE_MIN = 0.001
BACKTEST_RISK_PER_TRADE_MAX = 0.5


def create_validation_error(errors: List[str]) -> Dict[str, Any]:
    """
    Create standardized validation error response.
    
    Args:
        errors: List of error messages
        
    Returns:
        Error response dictionary
    """
    return {
        "error": "Validation failed",
        "errors": errors,
        "message": "Please check your input and try again"
    }
