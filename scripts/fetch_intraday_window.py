"""
Fetch 1-minute intraday data for the SEBI-compliant window and store as parquet.
"""
import argparse
import logging
import os
import sys
import time as time_module
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_compliance import get_intraday_window, get_intraday_parquet_path, INTRADAY_DIRECTORY
from data_providers import fetch_intraday_ohlcv

logger = logging.getLogger(__name__)


def parse_start(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    if "T" in value:
        return datetime.fromisoformat(value)
    return datetime.fromisoformat(f"{value}T00:00:00")


def parse_end(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    if "T" in value:
        return datetime.fromisoformat(value)
    return datetime.fromisoformat(f"{value}T23:59:59")


def discover_symbols(data_dir: Path) -> List[str]:
    symbols = []
    if not data_dir.exists():
        return symbols
    for subdir in data_dir.iterdir():
        if not subdir.is_dir():
            continue
        for parquet_file in subdir.glob("*.parquet"):
            symbols.append(parquet_file.stem.upper())
    return sorted(set(symbols))


def read_symbols_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    symbols = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if s:
            symbols.append(s.upper())
    return symbols


def normalize_intraday_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_localize(None)
    df = df.copy()
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    df.columns = [col.title().replace("_", "") for col in df.columns]
    df.index.name = "Date"
    return df


def filter_window(df: pd.DataFrame, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df[(df.index >= start_dt) & (df.index <= end_dt)].copy()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols-file", type=str, default=None)
    parser.add_argument("--max-symbols", type=int, default=None)
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument("--min-rows", type=int, default=0)
    parser.add_argument("--sleep-seconds", type=float, default=1.0)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    start_dt, end_dt = get_intraday_window()
    override_start = parse_start(args.start_date)
    override_end = parse_end(args.end_date)
    if override_start:
        start_dt = override_start
    if override_end:
        end_dt = override_end
    if end_dt <= start_dt:
        logger.error("Invalid window: end must be after start")
        return 2

    data_dir = Path(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) / "data"
    if args.symbols_file:
        symbols = read_symbols_file(Path(args.symbols_file))
    else:
        symbols = discover_symbols(data_dir)

    if args.max_symbols:
        symbols = symbols[: args.max_symbols]

    if not symbols:
        logger.error("No symbols found")
        return 1

    Path(INTRADAY_DIRECTORY).mkdir(parents=True, exist_ok=True)

    attempted = 0
    succeeded = 0
    skipped = 0
    failed = 0

    for symbol in symbols:
        attempted += 1
        try:
            df = fetch_intraday_ohlcv(symbol, start_dt, end_dt)
            df = normalize_intraday_df(df)
            df = filter_window(df, start_dt, end_dt)
            if df is None or df.empty:
                logger.warning(f"{symbol} returned no intraday data")
                skipped += 1
                continue
            if args.min_rows and len(df) < args.min_rows:
                logger.warning(f"{symbol} skipped due to low rows: {len(df)}")
                skipped += 1
                continue
            out_path = get_intraday_parquet_path(symbol)
            Path(out_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(out_path, compression="snappy")
            succeeded += 1
            logger.info(f"{symbol} saved {len(df)} rows to {out_path}")
        except Exception as exc:
            failed += 1
            logger.error(f"{symbol} failed: {exc}")
        if args.sleep_seconds:
            time_module.sleep(args.sleep_seconds)

    logger.info(
        f"Done. attempted={attempted} succeeded={succeeded} skipped={skipped} failed={failed}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
