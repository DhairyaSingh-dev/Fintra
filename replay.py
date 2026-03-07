import logging
import pandas as pd
import datetime
from config import Config
from redis_client import redis_client, init_redis
import yfinance as yf

logger = logging.getLogger(__name__)

def _cache_key(symbol: str, start: str, end: str) -> str:
    return f"replay:{symbol}:{start}:{end}"

def _is_valid_window(start_dt: datetime.datetime, end_dt: datetime.datetime) -> bool:
    # Must be <= 60 minutes
    return (end_dt - start_dt).total_seconds() <= 60 * 60 and end_dt > start_dt

def _enforce_sebi_lag(end_dt: datetime.datetime) -> bool:
    # End must be at least 30 days before now
    lag = datetime.datetime.utcnow() - datetime.timedelta(days=30)
    return end_dt <= lag

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

    # Cache miss – fetch via yfinance (interval=1m)
    logger.info("Fetching 1‑min data for %s from %s to %s", symbol, start_iso, end_iso)
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(interval='1m', start=start_dt, end=end_dt, auto_adjust=False)
    except Exception as e:
        logger.error(f"Failed to fetch 1‑min data for {symbol}: {e}")
        raise

    if df.empty:
        logger.warning("Empty 1‑min data for %s", symbol)
        raise ValueError("No minute‑level data available for the requested period")

    # Ensure required columns exist
    required = ['Open', 'High', 'Low', 'Close', 'Volume']
    for col in required:
        if col not in df.columns:
            df[col] = None

    # Reset index to a datetime column named 'timestamp'
    df = df.reset_index()
    df.rename(columns={'index': 'timestamp'}, inplace=True)
    # Convert to list of dicts for JSON caching
    records = df.to_dict(orient='records')
    if client:
        import json, math
        # store for 12 hours (43200 seconds)
        try:
            client.setex(cache_key, 43200, json.dumps(records))
            logger.info("Cached replay data for %s", cache_key)
        except Exception as e:
            logger.error("Failed to cache replay data: %s", e)
    return df
