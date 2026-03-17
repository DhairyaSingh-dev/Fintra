"""
Unit tests for data_providers module.

Tests focus on inception date fetching with multiple fallback strategies.
"""
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from backend.data_providers import fetch_stock_inception_date


class TestFetchStockInceptionDate:
    """Tests for fetch_stock_inception_date function."""

    def test_returns_none_when_all_fallbacks_fail(self):
        """Test returns None when yfinance fails completely."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            mock_ticker.info = {}
            mock_ticker.history.return_value = None
            mock_ticker_class.return_value = mock_ticker

            result = fetch_stock_inception_date('TEST.NS')
            assert result is None

    def test_returns_none_when_history_empty(self):
        """Test returns None when max history fetch returns empty."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            mock_ticker.info = {}  # No firstTradeDateEpoch or startDate
            empty_df = MagicMock()
            empty_df.empty = True
            mock_ticker.history.return_value = empty_df
            mock_ticker_class.return_value = mock_ticker

            result = fetch_stock_inception_date('TEST.NS')
            assert result is None

    def test_fetches_inception_from_firsttrade_date_epoch(self):
        """Test primary fallback: firstTradeDateEpoch."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            # Simulate Unix timestamp for Jan 15, 2020
            mock_ticker.info = {'firstTradeDateEpoch': 1579046400}
            mock_ticker_class.return_value = mock_ticker

            result = fetch_stock_inception_date('RELIANCE.NS')

            assert result is not None
            assert result.year == 2020
            assert result.month == 1
            assert result.day == 15

    def test_fetches_inception_from_start_date(self):
        """Test secondary fallback: startDate."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            mock_ticker.info = {
                'firstTradeDateEpoch': None,
                'startDate': 1579046400  # Same date
            }
            mock_ticker_class.return_value = mock_ticker

            result = fetch_stock_inception_date('TCS.NS')

            assert result is not None
            assert result.year == 2020
            assert result.month == 1
            assert result.day == 15

    def test_uses_max_history_when_no_epoch_fields(self):
        """Test tertiary fallback: fetch max history."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            mock_ticker.info = {}  # No inception fields
            # Create mock dataframe with datetime index
            dates = pd.DatetimeIndex([
                datetime(2019, 5, 10),
                datetime(2019, 5, 11),
                datetime(2019, 5, 12)
            ])
            mock_df = MagicMock()
            mock_df.empty = False
            mock_df.index = dates
            mock_ticker.history.return_value = mock_df
            mock_ticker_class.return_value = mock_ticker

            result = fetch_stock_inception_date('INFY.NS')

            assert result is not None
            assert result.year == 2019
            assert result.month == 5
            assert result.day == 10

    def test_strips_timezone_from_history_date(self):
        """Test timezone info is removed from inception date."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            mock_ticker.info = {}
            # Create timezone-aware datetime
            from datetime import timezone, timedelta
            tz_aware = datetime(2019, 5, 10, tzinfo=timezone(timedelta(hours=5)))
            dates = pd.DatetimeIndex([tz_aware])
            mock_df = MagicMock()
            mock_df.empty = False
            mock_df.index = dates
            mock_ticker.history.return_value = mock_df
            mock_ticker_class.return_value = mock_ticker

            result = fetch_stock_inception_date('WIPRO.NS')

            assert result is not None
            assert result.tzinfo is None  # Timezone should be stripped

    def test_adds_ns_suffix_when_missing(self):
        """Test .NS suffix is automatically added for Indian stocks."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            mock_ticker.info = {'firstTradeDateEpoch': 1579046400}
            mock_ticker_class.return_value = mock_ticker

            fetch_stock_inception_date('RELIANCE')  # No .NS suffix

            # Should have been called with RELIANCE.NS
            mock_ticker_class.assert_called_once_with('RELIANCE.NS', session=None)

    def test_doesnt_add_suffix_when_present(self):
        """Test .NS suffix is not doubled."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            mock_ticker.info = {'firstTradeDateEpoch': 1579046400}
            mock_ticker_class.return_value = mock_ticker

            fetch_stock_inception_date('RELIANCE.NS')  # Already has .NS

            mock_ticker_class.assert_called_once_with('RELIANCE.NS', session=None)

    def test_handles_exception_gracefully(self):
        """Test exceptions are caught and returns None."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker_class.side_effect = Exception("Network error")

            result = fetch_stock_inception_date('TEST.NS')
            assert result is None

    def test_prefers_firsttrade_date_epoch_over_startdate(self):
        """Test firstTradeDateEpoch is preferred when both are present."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            mock_ticker.info = {
                'firstTradeDateEpoch': 1579046400,  # Jan 15, 2020
                'startDate': 1576780800  # Dec 19, 2019 (earlier)
            }
            mock_ticker_class.return_value = mock_ticker

            result = fetch_stock_inception_date('TEST.NS')

            # Should use firstTradeDateEpoch
            assert result is not None
            assert result.year == 2020

    def test_handles_non_dict_info(self):
        """Test handles when ticker.info is not a dict-like."""
        with patch('backend.data_providers.yf.Ticker') as mock_ticker_class:
            mock_ticker = MagicMock()
            # Simulate None info (unlikely but possible)
            mock_ticker.info = None
            mock_ticker_class.return_value = mock_ticker

            result = fetch_stock_inception_date('TEST.NS')
            assert result is None
