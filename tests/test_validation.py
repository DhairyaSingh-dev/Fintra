"""
Unit tests for validation module
Tests comprehensive backend validation including symbol validation,
XSS prevention, and data type validation.
"""
import pytest
import sys
from pathlib import Path
from datetime import datetime, timezone

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from validation import (
    validate_symbol, sanitize_string, validate_float, validate_int,
    validate_date, validate_date_range, validate_required_fields, validate_strategy,
    create_validation_error, XSS_PATTERNS
)


class TestSymbolValidation:
    """Tests for symbol validation"""
    
    def test_valid_symbol(self):
        """Test valid symbol formats - returns (is_valid, error_message)"""
        is_valid, error = validate_symbol('RELIANCE.NS')
        assert is_valid is True
        assert error == ""
        
        is_valid, error = validate_symbol('TCS.NS')
        assert is_valid is True
        
        is_valid, error = validate_symbol('INFY.NS')
        assert is_valid is True
    
    def test_invalid_symbol_too_long(self):
        """Test symbol too long - returns (False, error_message)"""
        is_valid, error = validate_symbol('A' * 51)
        assert is_valid is False
        assert 'between 1 and 50 characters' in error
    
    def test_invalid_symbol_chars(self):
        """Test invalid characters in symbol - returns (False, error_message)"""
        is_valid, error = validate_symbol('REL@NCE')
        assert is_valid is False
        assert 'invalid characters' in error.lower()
    
    def test_empty_symbol(self):
        """Test empty symbol - returns (False, error_message)"""
        is_valid, error = validate_symbol('')
        assert is_valid is False
        assert 'Symbol is required' in error
        
        # Test None
        is_valid, error = validate_symbol(None)
        assert is_valid is False


class TestStringValidation:
    """Tests for string sanitization - returns (sanitized_value, error_message)"""
    
    def test_sanitize_basic_string(self):
        """Test basic string sanitization"""
        result, error = sanitize_string('Hello World', max_length=100)
        assert result == 'Hello World'
        assert error is None
    
    def test_sanitize_with_html(self):
        """Test HTML removal - XSS pattern detection"""
        # Test with XSS pattern
        result, error = sanitize_string('<script>alert("xss")</script>Hello', allow_html=False)
        assert result == ""
        assert 'XSS attempt' in error
        
        # Test normal HTML (should be stripped but not flagged as error)
        result, error = sanitize_string('<b>Hello</b> World', allow_html=False)
        assert result == "Hello World"
        assert error is None
    
    def test_sanitize_too_long(self):
        """Test string exceeding max length"""
        result, error = sanitize_string('A' * 101, max_length=100)
        assert result == ""
        assert 'exceeds maximum length' in error
    
    def test_sanitize_xss_patterns(self):
        """Test XSS pattern detection from XSS_PATTERNS list"""
        # Test first few XSS patterns
        for pattern in XSS_PATTERNS[:3]:
            test_input = f'test{pattern}test'
            result, error = sanitize_string(test_input)
            assert result == "", f"Pattern {pattern} should be rejected"
            assert error is not None


class TestNumericValidation:
    """Tests for numeric validation - returns (value, error_message)"""
    
    def test_validate_float_valid(self):
        """Test valid float validation"""
        result, error = validate_float('123.45', 'price')
        assert result == 123.45
        assert error is None
        
        result, error = validate_float('0.01', 'price')
        assert result == 0.01
    
    def test_validate_float_invalid(self):
        """Test invalid float"""
        result, error = validate_float('abc', 'price')
        assert result is None
        assert 'must be a valid number' in error
    
    def test_validate_float_range(self):
        """Test float range validation"""
        # Above max
        result, error = validate_float('150', 'price', min_val=0, max_val=100)
        assert result is None
        assert 'must not exceed' in error
        
        # Below min
        result, error = validate_float('-10', 'price', min_val=0)
        assert result is None
        assert 'at least' in error
    
    def test_validate_int_valid(self):
        """Test valid integer"""
        result, error = validate_int('10', 'quantity')
        assert result == 10
        assert error is None
    
    def test_validate_int_invalid(self):
        """Test invalid integer"""
        result, error = validate_int('10.5', 'quantity')
        assert result is None
        assert 'must be a whole number' in error


class TestDateValidation:
    """Tests for date validation - returns (datetime_object, error_message)"""
    
    def test_validate_date_valid(self):
        """Test valid date - returns datetime object, not string"""
        result, error = validate_date('2024-01-15', 'entry_date')
        assert isinstance(result, datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert error is None
    
    def test_validate_date_invalid_format(self):
        """Test invalid date format"""
        result, error = validate_date('15/01/2024', 'entry_date')
        assert result is None
        assert 'YYYY-MM-DD format' in error
    
    def test_validate_date_range_valid(self):
        """Test valid date range - returns (is_valid, error_message)"""
        is_valid, error = validate_date_range('2024-01-01', '2024-12-31')
        assert is_valid is True
        assert error == ""
    
    def test_validate_date_range_invalid(self):
        """Test end date before start date"""
        is_valid, error = validate_date_range('2024-12-31', '2024-01-01')
        assert is_valid is False
        assert 'cannot be after' in error


class TestStrategyValidation:
    """Tests for strategy validation - returns (is_valid, error_message)"""
    
    def test_valid_strategy(self):
        """Test valid strategy"""
        valid_strategies = ['golden_cross', 'rsi', 'macd', 'composite', 'momentum', 'mean_reversion', 'breakout']
        
        for strategy in valid_strategies:
            is_valid, error = validate_strategy(strategy)
            assert is_valid is True, f"Strategy {strategy} should be valid"
            assert error == ""
    
    def test_invalid_strategy(self):
        """Test invalid strategy"""
        is_valid, error = validate_strategy('invalid_strategy')
        assert is_valid is False
        assert 'Invalid strategy' in error


class TestRequiredFields:
    """Tests for required fields validation - returns (is_valid, error_message)"""
    
    def test_all_fields_present(self):
        """Test with all required fields"""
        data = {'name': 'John', 'email': 'john@example.com'}
        is_valid, error = validate_required_fields(data, ['name', 'email'])
        assert is_valid is True
        assert error == ""
    
    def test_missing_fields(self):
        """Test with missing fields"""
        data = {'name': 'John'}
        is_valid, error = validate_required_fields(data, ['name', 'email'])
        assert is_valid is False
        assert 'Missing required fields' in error
        assert 'email' in error


class TestValidationError:
    """Tests for validation error helper - takes list of errors"""
    
    def test_create_validation_error(self):
        """Test error creation with list of errors"""
        errors = ['Invalid symbol format', 'Price must be positive']
        error = create_validation_error(errors)
        assert error['error'] == 'Validation failed'
        assert error['errors'] == errors
        assert 'message' in error