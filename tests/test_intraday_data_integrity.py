import os
import random
from pathlib import Path

import pandas as pd
import pytest

from data_compliance import INTRADAY_DIRECTORY, get_intraday_window


def _get_parquet_files(base_dir: Path):
    files = []
    if not base_dir.exists():
        return files
    for subdir in base_dir.iterdir():
        if subdir.is_dir():
            files.extend(subdir.glob("*.parquet"))
    return files


def test_intraday_directory_exists():
    assert os.path.exists(INTRADAY_DIRECTORY)


def test_intraday_parquet_integrity():
    pytest.importorskip("pyarrow")
    base_dir = Path(INTRADAY_DIRECTORY)
    files = _get_parquet_files(base_dir)
    if not files:
        pytest.skip("No intraday parquet files found")
    sample_files = random.sample(files, min(50, len(files)))
    window_start, window_end = get_intraday_window()
    required_cols = {"Open", "High", "Low", "Close", "Volume"}

    for file_path in sample_files:
        df = pd.read_parquet(file_path)
        if not isinstance(df.index, pd.DatetimeIndex):
            date_col = next((c for c in df.columns if c.lower() == "date"), None)
            if date_col:
                df[date_col] = pd.to_datetime(df[date_col])
                df.set_index(date_col, inplace=True)
            else:
                df.index = pd.to_datetime(df.index)
        assert not df.empty
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
        df.columns = [col.title().replace("_", "") for col in df.columns]
        assert required_cols.issubset(set(df.columns))
        assert df.index.min() >= window_start
        assert df.index.max() <= window_end
