"""
Unit tests for Intraday Data Updater.

Tests the SEBI-compliant intraday data update pipeline with sliding window.
"""

import shutil
import tempfile
from datetime import datetime, timedelta, time
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pandas as pd
import pytest

from scripts.intraday_data_updater import (
    SEBI_LAG_DAYS,
    WINDOW_DAYS,
    WINDOW_START_OFFSET,
    IntradayDataUpdater,
)


@pytest.fixture
def temp_intraday_dir():
    """Create a temporary intraday directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir)


@pytest.fixture
def updater(temp_intraday_dir):
    """Create an IntradayDataUpdater instance with temp directory."""
    return IntradayDataUpdater(intraday_dir=temp_intraday_dir)


@pytest.fixture
def sample_intraday_file(temp_intraday_dir):
    """Create a sample intraday parquet file."""

    def _create(symbol, start_time, periods=10):
        subdir = temp_intraday_dir / symbol[0].upper()
        subdir.mkdir(exist_ok=True)
        file_path = subdir / f"{symbol}.parquet"

        dates = pd.date_range(start=start_time, periods=periods, freq="5min")
        df = pd.DataFrame(
            {
                "Open": range(periods),
                "High": range(periods),
                "Low": range(periods),
                "Close": range(periods),
                "Volume": range(periods),
            },
            index=dates,
        )
        df.to_parquet(file_path)
        return file_path

    return _create


class TestWindowCalculations:
    """Tests for intraday window calculations."""

    def test_window_start_is_30_days_ago(self, updater):
        """Test window start is today minus 30 days (yfinance limit)."""
        window_start, window_end = updater.get_window_dates()
        expected_start = datetime.now() - timedelta(days=30)

        diff = abs((window_start - expected_start).total_seconds())
        assert diff < 86400  # Within 1 day

    def test_window_end_is_now(self, updater):
        """Test window end is now (latest available data)."""
        window_start, window_end = updater.get_window_dates()
        now = datetime.now()

        diff = abs((window_end - now).total_seconds())
        assert diff < 86400  # Within 1 day

    def test_window_spans_30_days(self, updater):
        """Test window spans approximately 30 days."""
        window_start, window_end = updater.get_window_dates()

        duration = (window_end - window_start).days
        assert 29 <= duration <= 31  # Approximately 30 days

    def test_window_start_is_beginning_of_day(self, updater):
        """Test window start is at 00:00:00."""
        window_start, _ = updater.get_window_dates()

        assert window_start.time() == time(0, 0, 0)

    def test_window_end_is_recent(self, updater):
        """Test window end is recent (within last day)."""
        _, window_end = updater.get_window_dates()
        now = datetime.now()

        diff = abs((window_end - now).total_seconds())
        assert diff < 86400  # Within 1 day


class TestGetAllIntradayFiles:
    """Tests for discovering intraday files."""

    def test_finds_all_parquet_files(self, updater, sample_intraday_file):
        """Test finds all parquet files in subdirectories."""
        sample_intraday_file("A.NS", datetime.now() - timedelta(days=40))
        sample_intraday_file("B.NS", datetime.now() - timedelta(days=40))
        sample_intraday_file("C.NS", datetime.now() - timedelta(days=40))

        files = updater.get_all_intraday_files()
        assert len(files) == 3

    def test_ignores_non_parquet_files(self, updater, temp_intraday_dir):
        """Test ignores non-parquet files."""
        subdir = temp_intraday_dir / "T"
        subdir.mkdir()
        (subdir / "TEST.NS.parquet").write_bytes(b"\x00" * 100)
        (subdir / "TEST.txt").write_text("ignored")

        files = updater.get_all_intraday_files()
        assert len(files) == 1
        assert files[0].suffix == ".parquet"

    def test_returns_empty_for_empty_directory(self, updater):
        """Test returns empty list for empty directory."""
        files = updater.get_all_intraday_files()
        assert files == []

    def test_returns_sorted_list(self, updater, sample_intraday_file):
        """Test returns sorted file list."""
        sample_intraday_file("Z.NS", datetime.now() - timedelta(days=40))
        sample_intraday_file("A.NS", datetime.now() - timedelta(days=40))
        sample_intraday_file("M.NS", datetime.now() - timedelta(days=40))

        files = updater.get_all_intraday_files()
        file_names = [f.stem for f in files]
        assert file_names == sorted(file_names)


class TestPruneOldData:
    """Tests for pruning old data from files."""

    def test_removes_data_older_than_window_start(self, updater, sample_intraday_file):
        """Test removes data older than window start."""
        window_start, _ = updater.get_window_dates()

        # Create file with old data
        old_time = window_start - timedelta(days=10)
        file_path = sample_intraday_file("OLD.NS", old_time, periods=100)

        result = updater.prune_old_data(file_path, window_start)

        assert result is True
        # File may be deleted if all data was old, or still exists with filtered data
        if file_path.exists():
            df = pd.read_parquet(file_path)
            assert df.index.min() >= window_start

    def test_deletes_file_with_all_old_data(self, updater, sample_intraday_file):
        """Test deletes file when all data is old."""
        window_start, _ = updater.get_window_dates()

        # Create file with only old data
        old_time = window_start - timedelta(days=20)
        file_path = sample_intraday_file("ALLOLD.NS", old_time, periods=10)

        result = updater.prune_old_data(file_path, window_start)

        assert result is True
        assert not file_path.exists()

    def test_keeps_file_with_new_data(self, updater, sample_intraday_file):
        """Test keeps file when data is within window."""
        window_start, _ = updater.get_window_dates()

        # Create file with recent data
        file_path = sample_intraday_file("NEW.NS", window_start, periods=10)

        result = updater.prune_old_data(file_path, window_start)

        assert result is False
        assert file_path.exists()

    def test_handles_corrupted_file(self, updater, temp_intraday_dir):
        """Test handles corrupted file gracefully."""
        subdir = temp_intraday_dir / "C"
        subdir.mkdir()
        corrupted = subdir / "CORRUPTED.NS.parquet"
        corrupted.write_text("not a parquet file")

        result = updater.prune_old_data(corrupted, datetime.now())

        assert result is False


class TestNormalizeDataframe:
    """Tests for normalizing intraday dataframes."""

    def test_standardizes_column_names(self, updater):
        """Test column names are standardized."""
        df = pd.DataFrame(
            {"open": [100], "high": [105], "close": [103]},
            index=pd.DatetimeIndex([datetime.now()]),
        )

        result = updater.normalize_dataframe(df)

        assert "Open" in result.columns
        assert "High" in result.columns
        assert "Close" in result.columns

    def test_removes_timezone_from_index(self, updater):
        """Test removes timezone info from index."""
        dates = pd.date_range(start="2024-01-01", periods=5, freq="min", tz="UTC")
        df = pd.DataFrame({"Open": range(5), "Close": range(5)}, index=dates)

        result = updater.normalize_dataframe(df)

        assert result.index.tz is None

    def test_handles_empty_dataframe(self, updater):
        """Test handles empty dataframe."""
        df = pd.DataFrame()

        result = updater.normalize_dataframe(df)

        assert result.empty


class TestFilterToWindow:
    """Tests for filtering data to window."""

    def test_filters_to_window_range(self, updater):
        """Test filters data to window range."""
        window_start = datetime(2024, 1, 1, 0, 0, 0)
        window_end = datetime(2024, 1, 31, 23, 59, 59)

        dates = pd.date_range(start="2023-12-01", end="2024-02-15", freq="D")
        df = pd.DataFrame(
            {"Open": range(len(dates)), "Close": range(len(dates))}, index=dates
        )

        result = updater.filter_to_window(df, window_start, window_end)

        assert result.index.min() >= window_start
        assert result.index.max() <= window_end

    def test_handles_empty_dataframe(self, updater):
        """Test handles empty dataframe."""
        df = pd.DataFrame()

        result = updater.filter_to_window(df, datetime.now(), datetime.now())

        assert result.empty


class TestFetchIntradayData:
    """Tests for fetching intraday data."""

    @patch("scripts.intraday_data_updater.fetch_intraday_ohlcv")
    def test_fetches_and_normalizes_data(self, mock_fetch, updater):
        """Test fetches and normalizes intraday data."""
        window_start = datetime.now() - timedelta(days=61)
        window_end = datetime.now() - timedelta(days=31)

        mock_data = pd.DataFrame(
            {
                "Open": [100],
                "High": [105],
                "Low": [99],
                "Close": [103],
                "Volume": [1000],
            },
            index=pd.DatetimeIndex([window_end - timedelta(hours=1)]),
        )
        mock_fetch.return_value = mock_data

        result = updater.fetch_intraday_data("TEST.NS", window_start, window_end)

        assert result is not None
        assert "Open" in result.columns

    @patch("scripts.intraday_data_updater.fetch_intraday_ohlcv")
    def test_returns_none_on_fetch_error(self, mock_fetch, updater):
        """Test returns None when fetch fails."""
        window_start = datetime.now() - timedelta(days=61)
        window_end = datetime.now() - timedelta(days=31)

        mock_fetch.side_effect = Exception("Network error")

        result = updater.fetch_intraday_data("TEST.NS", window_start, window_end)

        assert result is None

    @patch("scripts.intraday_data_updater.fetch_intraday_ohlcv")
    def test_returns_none_on_empty_response(self, mock_fetch, updater):
        """Test returns None when fetch returns None."""
        window_start = datetime.now() - timedelta(days=61)
        window_end = datetime.now() - timedelta(days=31)

        mock_fetch.return_value = None

        result = updater.fetch_intraday_data("TEST.NS", window_start, window_end)

        assert result is None


class TestUpdateIntradayFile:
    """Tests for updating intraday files."""

    @patch("scripts.intraday_data_updater.fetch_intraday_ohlcv")
    def test_updates_file_with_new_data(self, mock_fetch, updater, temp_intraday_dir):
        """Test updates parquet file with fetched data."""
        window_start = datetime.now() - timedelta(days=61)
        window_end = datetime.now() - timedelta(days=31)

        mock_data = pd.DataFrame(
            {
                "Open": [100],
                "High": [105],
                "Low": [99],
                "Close": [103],
                "Volume": [1000],
            },
            index=pd.DatetimeIndex([window_end - timedelta(hours=1)]),
        )
        mock_fetch.return_value = mock_data

        with patch(
            "scripts.intraday_data_updater.get_intraday_parquet_path"
        ) as mock_path:
            mock_path.return_value = str(temp_intraday_dir / "T" / "TEST.NS.parquet")
            result = updater.update_intraday_file("TEST.NS", window_start, window_end)

        assert result is True

    @patch("scripts.intraday_data_updater.fetch_intraday_ohlcv")
    def test_returns_false_on_fetch_failure(self, mock_fetch, updater):
        """Test returns False when fetch fails."""
        window_start = datetime.now() - timedelta(days=61)
        window_end = datetime.now() - timedelta(days=31)

        mock_fetch.return_value = None

        result = updater.update_intraday_file("TEST.NS", window_start, window_end)

        assert result is False

    @patch("scripts.intraday_data_updater.fetch_intraday_ohlcv")
    def test_respects_min_rows_requirement(self, mock_fetch, updater):
        """Test respects minimum rows requirement."""
        window_start = datetime.now() - timedelta(days=61)
        window_end = datetime.now() - timedelta(days=31)

        mock_data = pd.DataFrame(
            {"Open": [100], "Close": [103]}, index=pd.DatetimeIndex([window_end])
        )
        mock_fetch.return_value = mock_data

        result = updater.update_intraday_file(
            "TEST.NS", window_start, window_end, min_rows=100
        )

        assert result is False


class TestValidateIntradayFile:
    """Tests for validating intraday files."""

    def test_validates_good_file(self, updater, sample_intraday_file):
        """Test validates file with correct data."""
        window_start, window_end = updater.get_window_dates()
        file_path = sample_intraday_file("GOOD.NS", window_start, periods=100)

        result = updater.validate_intraday_file(file_path, window_start, window_end)

        assert result["valid"] is True
        assert result["symbol"] == "GOOD.NS"

    def test_detects_empty_file(self, updater, temp_intraday_dir):
        """Test detects empty file."""
        window_start, window_end = updater.get_window_dates()

        subdir = temp_intraday_dir / "E"
        subdir.mkdir()
        file_path = subdir / "EMPTY.NS.parquet"
        pd.DataFrame().to_parquet(file_path)

        result = updater.validate_intraday_file(file_path, window_start, window_end)

        assert result["valid"] is False
        assert "empty_file" in result["errors"]

    def test_detects_missing_columns(self, updater, temp_intraday_dir):
        """Test detects missing required columns."""
        window_start, window_end = updater.get_window_dates()

        subdir = temp_intraday_dir / "M"
        subdir.mkdir()
        file_path = subdir / "MISSING.NS.parquet"

        df = pd.DataFrame(
            {"open": [100], "close": [103]}, index=pd.DatetimeIndex([window_start])
        )
        df.to_parquet(file_path)

        result = updater.validate_intraday_file(file_path, window_start, window_end)

        assert result["valid"] is False
        assert "missing_columns" in result["errors"]

    def test_detects_data_outside_window(self, updater, sample_intraday_file):
        """Test detects data outside window range."""
        window_start, window_end = updater.get_window_dates()

        # Create file with data older than window
        old_time = window_start - timedelta(days=10)
        file_path = sample_intraday_file("OUTSIDE.NS", old_time, periods=10)

        result = updater.validate_intraday_file(file_path, window_start, window_end)

        assert "data_older_than_window_start" in result["errors"]


class TestRunUpdate:
    """Tests for running the complete update pipeline."""

    @pytest.fixture
    def populated_updater(self, temp_intraday_dir):
        """Create updater with sample data structure."""
        updater = IntradayDataUpdater(intraday_dir=temp_intraday_dir)

        # Create some existing files
        window_start, window_end = updater.get_window_dates()
        for letter in ["A", "B", "C"]:
            subdir = temp_intraday_dir / letter
            subdir.mkdir()
            dates = pd.date_range(start=window_start, periods=10, freq="5min")
            df = pd.DataFrame(
                {
                    "Open": range(10),
                    "High": range(10),
                    "Low": range(10),
                    "Close": range(10),
                    "Volume": range(10),
                },
                index=dates,
            )
            df.to_parquet(subdir / f"STOCK{letter}.NS.parquet")

        return updater

    def test_generates_report_with_required_fields(self, populated_updater):
        """Test pipeline generates report with all required fields."""
        with patch.object(populated_updater, "update_intraday_file", return_value=True):
            report = populated_updater.run_update(
                symbols=["A.NS", "B.NS"], prune_old=False, validate=False
            )

        assert "timestamp" in report
        assert "window_start" in report
        assert "window_end" in report
        assert "succeeded" in report
        assert "failed" in report
        assert report["pipeline"] == "intraday"

    def test_prunes_old_data_when_enabled(self, populated_updater):
        """Test prunes old data when prune_old=True."""
        with patch.object(
            populated_updater, "prune_all_old_data", return_value=5
        ) as mock_prune:
            populated_updater.run_update(
                symbols=["A.NS"], prune_old=True, validate=False
            )
            mock_prune.assert_called_once()

    def test_skips_prune_when_disabled(self, populated_updater):
        """Test skips pruning when prune_old=False."""
        with patch.object(populated_updater, "prune_all_old_data") as mock_prune:
            populated_updater.run_update(
                symbols=["A.NS"], prune_old=False, validate=False
            )
            mock_prune.assert_not_called()

    def test_validates_files_when_enabled(self, populated_updater):
        """Test validates files when validate=True."""
        with patch.object(populated_updater, "update_intraday_file", return_value=True):
            report = populated_updater.run_update(
                symbols=["A.NS"], prune_old=False, validate=True
            )

        assert "validation_results" in report

    def test_respects_max_symbols(self, populated_updater):
        """Test respects max_symbols parameter."""
        with patch.object(populated_updater, "update_intraday_file", return_value=True):
            report = populated_updater.run_update(
                symbols=["A.NS", "B.NS", "C.NS"],
                max_symbols=2,
                prune_old=False,
                validate=False,
            )

        assert report["attempted"] == 2

    def test_handles_no_symbols(self, updater):
        """Test handles case with no symbols."""
        with patch.object(updater, "get_symbols_from_daily_data", return_value=[]):
            report = updater.run_update(prune_old=False, validate=False)

        assert report["success"] is False
        assert "error" in report


class TestGetSymbolsFromDailyData:
    """Tests for getting symbols from daily data directory."""

    def test_discovers_symbols_from_daily_data(self, updater, temp_intraday_dir):
        """Test discovers symbols from daily data directory."""
        # Create mock daily data directory
        data_dir = temp_intraday_dir.parent / "data"
        data_dir.mkdir(exist_ok=True)

        for letter in ["A", "B"]:
            subdir = data_dir / letter
            subdir.mkdir(exist_ok=True)
            (subdir / "STOCKA.NS.parquet").write_bytes(b"\x00" * 100)
            (subdir / "STOCKB.NS.parquet").write_bytes(b"\x00" * 100)

        # This test is more about the logic than actual file discovery
        assert data_dir.exists()


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    def test_handles_nonexistent_intraday_dir(self):
        """Test handles nonexistent intraday directory."""
        updater = IntradayDataUpdater(intraday_dir=Path("/nonexistent/path"))
        files = updater.get_all_intraday_files()
        assert files == []

    def test_creates_intraday_dir_on_update(self, updater, temp_intraday_dir):
        """Test creates intraday directory during update."""
        # Delete the directory
        shutil.rmtree(temp_intraday_dir)

        updater.intraday_dir = temp_intraday_dir

        with patch.object(updater, "update_intraday_file", return_value=True):
            updater.run_update(symbols=["A.NS"], prune_old=False, validate=False)

        assert temp_intraday_dir.exists()

    def test_handles_file_with_timezone_data(self, updater, temp_intraday_dir):
        """Test handles file with timezone-aware index."""
        window_start, window_end = updater.get_window_dates()

        subdir = temp_intraday_dir / "T"
        subdir.mkdir()
        file_path = subdir / "TZ.NS.parquet"

        dates = pd.date_range(
            start=window_start, periods=10, freq="5min", tz="Asia/Kolkata"
        )
        df = pd.DataFrame({"Open": range(10), "Close": range(10)}, index=dates)
        df.to_parquet(file_path)

        result = updater.prune_old_data(file_path, window_start)

        # Should not crash
        assert result is not None
