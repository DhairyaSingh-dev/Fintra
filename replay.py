import logging
import pandas as pd
import datetime
import os
from config import Config
from redis_client import redis_client, init_redis
from data_providers import fetch_intraday_ohlcv
from data_compliance import get_intraday_window, get_intraday_parquet_path

logger = logging.getLogger(__name__)

def _cache_key(symbol: str, start: str, end: str) -> str:
    return f"replay:{symbol}:{start}:{end}"

def _is_valid_window(start_dt: datetime.datetime, end_dt: datetime.datetime) -> bool:
    # Must be <= 60 minutes
    return (end_dt - start_dt).total_seconds() <= 60 * 60 and end_dt > start_dt

def _enforce_sebi_lag(end_dt: datetime.datetime) -> bool:
    _, window_end = get_intraday_window()
    return end_dt <= window_end


def _load_intraday_parquet(symbol: str, start_dt: datetime.datetime, end_dt: datetime.datetime) -> pd.DataFrame:
    path = get_intraday_parquet_path(symbol)
    if not path or not os.path.exists(path):
        return None
    df = pd.read_parquet(path)
    if not isinstance(df.index, pd.DatetimeIndex):
        date_col = next((c for c in df.columns if c.lower() == 'date'), None)
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col])
            df.set_index(date_col, inplace=True)
        else:
            df.index = pd.to_datetime(df.index)
    df = df[(df.index >= start_dt) & (df.index <= end_dt)].copy()
    if df.empty:
        return None
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    df.columns = [col.title().replace('_', '') for col in df.columns]
    df.index.name = 'Date'
    return df

def get_one_min_candles(symbol: str, start_iso: str, end_iso: str) -> pd.DataFrame:
    """Return a DataFrame with 1‑minute OHLCV for the given symbol.
    * start_iso / end_iso are ISO‑8601 strings (UTC) received from the client.
    * Enforces a 30‑day SEBI lag and a max 60‑minute window.
    * Caches the raw list of dicts in Redis for 12 h to avoid repeated yfinance calls.
    """
    try:
        start_dt = datetime.datetime.fromisoformat(start_iso.rstrip('Z'))
        end_dt = datetime.datetime.fromisoformat(end_iso.rstrip('Z'))
    except Exception as e:
        logger.error(f"Invalid datetime format: {e}")
        raise ValueError("Invalid start or end datetime format")

    if not _is_valid_window(start_dt, end_dt):
        raise ValueError("Time window must be <= 60 minutes and end after start")

    if not _enforce_sebi_lag(end_dt):
        raise ValueError("End datetime must be at least 30 days in the past (SEBI lag)")

    cache_key = _cache_key(symbol.upper(), start_iso, end_iso)
    client = None
    if redis_client.is_connected():
        client = redis_client.get_client()
    if client:
        cached = client.get(cache_key)
        if cached:
            logger.info("Replay data cache hit for %s", cache_key)
            # Cached as JSON string of list of dicts
            import json
            data = json.loads(cached)
            return pd.DataFrame(data)

    window_start, window_end = get_intraday_window()
    df = None
    if start_dt >= window_start and end_dt <= window_end:
        df = _load_intraday_parquet(symbol, start_dt, end_dt)
    if df is None or df.empty:
        df = fetch_intraday_ohlcv(symbol, start_dt, end_dt)
    
    if df is None or df.empty:
        logger.warning("Empty 1‑min data for %s", symbol)
        raise ValueError("No minute‑level data available for the requested period. "
                         "Ensure the date is a trading day (weekday, not a holiday).")

    # Ensure required columns exist
    required = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required:
        if col not in df.columns:
            df[col] = None

    # Reset index to a datetime column named 'timestamp'
    # yfinance uses 'Datetime' as index name for intraday data
    df = df.reset_index()
    # Rename whatever the index column is to 'timestamp'
    if 'Datetime' in df.columns:
        df.rename(columns={'Datetime': 'timestamp'}, inplace=True)
    elif 'Date' in df.columns:
        df.rename(columns={'Date': 'timestamp'}, inplace=True)
    elif 'index' in df.columns:
        df.rename(columns={'index': 'timestamp'}, inplace=True)

    # Convert to list of dicts for JSON caching
    records = df.to_dict(orient='records')
    if client:
        import json
        # store for 12 hours (43200 seconds)
        try:
            # Serialise timestamps for JSON
            import math
            serialisable = []
            for r in records:
                row = {}
                for k, v in r.items():
                    if hasattr(v, 'isoformat'):
                        row[k] = v.isoformat()
                    elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        row[k] = 0
                    else:
                        row[k] = v
                serialisable.append(row)
            client.setex(cache_key, 43200, json.dumps(serialisable))
            logger.info("Cached replay data for %s", cache_key)
        except Exception as e:
            logger.error("Failed to cache replay data: %s", e)
    return df
