import asyncio
import logging
import random
import requests
import time
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import yfinance as yf
from backend.config import Config

logger = logging.getLogger(__name__)

# Rate limiting for Yahoo Finance
_yf_request_count = 0
_yf_rate_limit_delay = 0.0
YF_RATE_LIMIT_THRESHOLD = 30  # Add delay after this many requests
YF_RATE_LIMIT_PAUSE = 3.0  # Seconds to pause when rate limited

# ----- User-Agent rotation to avoid Yahoo Finance fingerprinting -----
_UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 OPR/114.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:130.0) Gecko/20100101 Firefox/130.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
]


def _yf_session():
    """Create a session for yfinance. Now uses curl_cffi as Yahoo requires it."""
    try:
        from curl_cffi import requests as curl_requests

        return curl_requests.Session(impersonate="chrome")
    except ImportError:
        # Fallback to requests if curl_cffi not available
        s = requests.Session()
        s.headers["User-Agent"] = random.choice(_UA_POOL)
        return s


class DataProviderError(Exception):
    pass


def fetch_daily_ohlcv(
    symbol: str, period: str = "90d", providers: list = None
) -> pd.DataFrame:
    """Fetch daily OHLCV data with fallback chain: yfinance -> Polygon -> AlphaVantage -> Finnhub

    For NSE stocks (.NS): yfinance -> AlphaVantage (with .BSE suffix).
    Polygon/Finnhub don't carry NSE data on free tier.
    """
    if providers is None:
        providers = ["yfinance", "polygon", "alphavantage", "finnhub"]

    is_nse = symbol.upper().endswith(".NS") or symbol.upper().endswith(".BO")
    yf_symbol = symbol if "." in symbol else f"{symbol}.NS"
    base_symbol = symbol.replace(".NS", "").replace(".BO", "")
    # Alpha Vantage uses .BSE suffix for Indian stocks
    av_symbol = f"{base_symbol}.BSE" if is_nse else base_symbol

    # Try 1: yFinance
    if "yfinance" in providers:
        try:
            logger.info(f"[yFinance] Fetching daily data for {yf_symbol}")
            ticker = yf.Ticker(yf_symbol, session=_yf_session())
            df = ticker.history(period=period, interval="1d", auto_adjust=False)
            if df is not None and not df.empty:
                logger.info(f"[yFinance] Success: {len(df)} rows")
                return _standardize_df(df)
            logger.warning("[yFinance] Returned empty dataframe")
        except Exception as e:
            logger.warning(f"[yFinance] Failed: {e}")

    # Calculate dates for API requests
    days = 90
    if period.endswith("d"):
        days = int(period[:-1])
    elif period.endswith("y"):
        days = int(period[:-1]) * 365

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    # Try 2: Polygon.io (skip for NSE — no Indian stock coverage on free tier)
    if "polygon" in providers and Config.POLYGON_API_KEY and not is_nse:
        try:
            logger.info(f"[Polygon] Fetching daily data for {base_symbol}")
            url = f"https://api.polygon.io/v2/aggs/ticker/{base_symbol}/range/1/day/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
            res = requests.get(
                url, params={"apiKey": Config.POLYGON_API_KEY, "adjusted": "true"}
            )
            if res.status_code == 200:
                data = res.json()
                if data.get("results"):
                    df = pd.DataFrame(data["results"])
                    df["Date"] = pd.to_datetime(df["t"], unit="ms")
                    df = df.rename(
                        columns={
                            "o": "Open",
                            "h": "High",
                            "l": "Low",
                            "c": "Close",
                            "v": "Volume",
                        }
                    )
                    df = df.set_index("Date")[
                        ["Open", "High", "Low", "Close", "Volume"]
                    ]
                    logger.info(f"[Polygon] Success: {len(df)} rows")
                    return df
            logger.warning(f"[Polygon] No results for {base_symbol}")
        except Exception as e:
            logger.warning(f"[Polygon] Exception: {e}")
    elif "polygon" in providers and is_nse:
        logger.info(f"[Polygon] Skipping — no NSE coverage on free tier")

    # Try 3: Alpha Vantage (use .BSE suffix for Indian stocks)
    if "alphavantage" in providers and Config.ALPHA_VANTAGE_API_KEY:
        try:
            logger.info(f"[AlphaVantage] Fetching daily data for {av_symbol}")
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "TIME_SERIES_DAILY",
                "symbol": av_symbol,
                "outputsize": "compact" if days <= 100 else "full",
                "apikey": Config.ALPHA_VANTAGE_API_KEY,
            }
            res = requests.get(url, params=params)
            if res.status_code == 200:
                data = res.json()
                # Check for premium gate or error messages
                if "Information" in data or "Note" in data:
                    msg = data.get("Information", data.get("Note", ""))
                    logger.warning(f"[AlphaVantage] API message: {msg[:120]}")
                else:
                    ts_key = "Time Series (Daily)"
                    if ts_key in data:
                        df = pd.DataFrame.from_dict(data[ts_key], orient="index")
                        df.index = pd.to_datetime(df.index)
                        df = df.rename(
                            columns={
                                "1. open": "Open",
                                "2. high": "High",
                                "3. low": "Low",
                                "4. close": "Close",
                                "5. volume": "Volume",
                            }
                        ).astype(float)
                        df = df[df.index >= start_date]
                        df = df.sort_index()
                        if not df.empty:
                            logger.info(f"[AlphaVantage] Success: {len(df)} rows")
                            return df
                    logger.warning(f"[AlphaVantage] No data for {av_symbol}")
            else:
                logger.warning(f"[AlphaVantage] HTTP {res.status_code}")
        except Exception as e:
            logger.warning(f"[AlphaVantage] Exception: {e}")

    # Try 4: Finnhub (skip for NSE — no Indian stock coverage on free tier)
    if "finnhub" in providers and Config.FINNHUB_API_KEY and not is_nse:
        try:
            logger.info(f"[Finnhub] Fetching daily data for {base_symbol}")
            url = "https://finnhub.io/api/v1/stock/candle"
            params = {
                "symbol": base_symbol,
                "resolution": "D",
                "from": int(start_date.timestamp()),
                "to": int(end_date.timestamp()),
                "token": Config.FINNHUB_API_KEY,
            }
            res = requests.get(url, params=params)
            if res.status_code == 200:
                data = res.json()
                if data.get("s") == "ok":
                    df = pd.DataFrame(data)
                    df["Date"] = pd.to_datetime(df["t"], unit="s")
                    df = df.rename(
                        columns={
                            "o": "Open",
                            "h": "High",
                            "l": "Low",
                            "c": "Close",
                            "v": "Volume",
                        }
                    )
                    df = df.set_index("Date")[
                        ["Open", "High", "Low", "Close", "Volume"]
                    ]
                    logger.info(f"[Finnhub] Success: {len(df)} rows")
                    return df
            logger.warning(f"[Finnhub] No data for {base_symbol}")
        except Exception as e:
            logger.warning(f"[Finnhub] Exception: {e}")
    elif "finnhub" in providers and is_nse:
        logger.info(f"[Finnhub] Skipping — no NSE coverage on free tier")

    logger.error(f"All specified data providers failed for daily data: {symbol}")
    return None


def _date_range_chunks(start_dt: datetime, end_dt: datetime, chunk_days: int = 7):
    """Yield (start, end) tuples for date ranges, each chunk_days apart."""
    current = start_dt
    while current < end_dt:
        chunk_end = current + timedelta(days=chunk_days)
        if chunk_end > end_dt:
            chunk_end = end_dt
        yield current, chunk_end
        current = chunk_end


def fetch_intraday_ohlcv(
    symbol: str, start_dt: datetime, end_dt: datetime
) -> pd.DataFrame:
    """Fetch 1-min OHLCV data with fallback chain.

    For NSE stocks (.NS): only yfinance works (Polygon/AV don't carry NSE intraday on free tier).
    For US stocks: yfinance -> Polygon -> AlphaVantage.

    Note: yfinance limits 1-min data to ~30 days per request, so we chunk requests.
    """
    is_nse = symbol.upper().endswith(".NS")
    yf_symbol = symbol if is_nse else f"{symbol}.NS" if "." not in symbol else symbol
    base_symbol = symbol.replace(".NS", "").replace(".BO", "")

    # yfinance only has last 30 days of 1-min data
    # We fetch what we can (last 30 days to now) and let the storage maintain the sliding window
    max_yf_date = datetime.now() - timedelta(days=30)

    # Fetch from the later of (start_dt or 30 days ago) up to now
    effective_start_dt = max(start_dt, max_yf_date)
    effective_end_dt = datetime.now()

    # If the effective range is inverted (requested data too old), return empty
    if effective_start_dt >= effective_end_dt:
        logger.warning(
            f"[yFinance] Requested range outside 30-day limit, skipping {symbol}"
        )
        return None

    logger.info(
        f"[yFinance] Fetching available data from last 30 days: "
        f"{effective_start_dt.date()} to {effective_end_dt.date()}"
    )

    # Try 1: yFinance (with retry + longer cooldown + chunking for >7 days)
    max_retries = 3
    date_range_days = (effective_end_dt - effective_start_dt).days

    # If range > 7 days, we need to chunk the requests
    if date_range_days > 7:
        logger.info(
            f"[yFinance] Date range {date_range_days} days > 7, chunking requests..."
        )
        all_dfs = []
        for chunk_start, chunk_end in _date_range_chunks(
            effective_start_dt, effective_end_dt, chunk_days=7
        ):
            for attempt in range(max_retries):
                try:
                    logger.info(
                        f"[yFinance] Fetching {yf_symbol} {chunk_start.date()} to {chunk_end.date()} "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    ticker = yf.Ticker(yf_symbol, session=_yf_session())
                    df = ticker.history(
                        interval="1m",
                        start=chunk_start,
                        end=chunk_end,
                        auto_adjust=False,
                    )
                    if df is not None and not df.empty:
                        logger.info(f"[yFinance] Chunk success: {len(df)} rows")
                        all_dfs.append(_standardize_df(df))
                        break
                    else:
                        logger.warning(
                            f"[yFinance] Empty result for chunk {chunk_start.date()}"
                        )
                        break
                except Exception as e:
                    err_str = str(e).lower()
                    logger.warning(f"[yFinance] Chunk error attempt {attempt + 1}: {e}")
                    is_rate_limit = any(
                        kw in err_str
                        for kw in ["rate", "too many", "429", "timeout", "timed out"]
                    )
                    if is_rate_limit and attempt < max_retries - 1:
                        wait = 5 * (attempt + 1)
                        logger.warning(f"[yFinance] Rate limit, waiting {wait}s...")
                        time.sleep(wait)
                    elif attempt == max_retries - 1:
                        logger.warning(f"[yFinance] All retries exhausted for chunk")

            # Small delay between chunks to avoid rate limits
            time.sleep(1)

        if all_dfs:
            combined = pd.concat(all_dfs).sort_index()
            # Don't filter by SEBI window - return all fetched data
            # The storage layer will handle the sliding window
            logger.info(f"[yFinance] Combined {len(combined)} rows from chunks")
            return combined
        logger.warning(f"[yFinance] No data from any chunk")
    else:
        # Original single-request logic for short ranges
        for attempt in range(max_retries):
            try:
                logger.info(
                    f"[yFinance] Fetching 1-min data for {yf_symbol} (attempt {attempt + 1}/{max_retries})"
                )
                ticker = yf.Ticker(yf_symbol, session=_yf_session())
                df = ticker.history(
                    interval="1m",
                    start=effective_start_dt,
                    end=effective_end_dt,
                    auto_adjust=False,
                )
                if df is not None and not df.empty:
                    logger.info(f"[yFinance] Success: {len(df)} rows")
                    return _standardize_df(df)
                logger.warning(f"[yFinance] Returned empty dataframe for {yf_symbol}")
                break  # empty df, break to fallback
            except Exception as e:
                err_str = str(e).lower()
                logger.warning(f"[yFinance] Error on attempt {attempt + 1}: {e}")
                is_rate_limit = any(
                    kw in err_str
                    for kw in ["rate", "too many", "429", "timeout", "timed out"]
                )
                if is_rate_limit and attempt < max_retries - 1:
                    wait = 5 * (attempt + 1)  # 5s, 10s, 15s — longer cooldown
                    logger.warning(
                        f"[yFinance] Possible rate limit, waiting {wait}s..."
                    )
                    time.sleep(wait)
                elif is_rate_limit and attempt == max_retries - 1:
                    logger.warning("[yFinance] Rate limits exhausted after all retries")
                else:
                    logger.warning(f"[yFinance] Non-retryable error: {e}")
                    break

    # For NSE stocks, external providers don't carry Indian intraday data on free tiers
    if is_nse:
        logger.warning(
            f"[Fallback] Skipping Polygon/AlphaVantage for NSE stock {symbol} "
            f"(no Indian intraday support on free tiers)"
        )
        logger.error(f"All data providers failed for intraday data: {symbol}")
        return None

    # --- US / International stocks only below this point ---

    # Try 2: Polygon.io (uses date strings, not ms timestamps)
    if Config.POLYGON_API_KEY:
        try:
            start_str = start_dt.strftime("%Y-%m-%d")
            end_str = end_dt.strftime("%Y-%m-%d")
            logger.info(f"[Polygon] Fetching 1-min data for {base_symbol}")
            url = f"https://api.polygon.io/v2/aggs/ticker/{base_symbol}/range/1/minute/{start_str}/{end_str}"
            res = requests.get(
                url,
                params={
                    "apiKey": Config.POLYGON_API_KEY,
                    "adjusted": "true",
                    "limit": "5000",
                },
            )
            if res.status_code == 200:
                data = res.json()
                if data.get("results"):
                    df = pd.DataFrame(data["results"])
                    df["Date"] = pd.to_datetime(df["t"], unit="ms")
                    df = df.rename(
                        columns={
                            "o": "Open",
                            "h": "High",
                            "l": "Low",
                            "c": "Close",
                            "v": "Volume",
                        }
                    )
                    df = df.set_index("Date")[
                        ["Open", "High", "Low", "Close", "Volume"]
                    ]
                    # Filter to exact time range
                    df = df[(df.index >= start_dt) & (df.index <= end_dt)]
                    if not df.empty:
                        logger.info(f"[Polygon] Success: {len(df)} rows")
                        return df
                logger.warning(f"[Polygon] No results for {base_symbol}")
            else:
                logger.warning(f"[Polygon] HTTP {res.status_code}: {res.text[:200]}")
        except Exception as e:
            logger.warning(f"[Polygon] Exception: {e}")

    # Try 3: Alpha Vantage (intraday is premium — may fail on free keys)
    if Config.ALPHA_VANTAGE_API_KEY:
        try:
            logger.info(f"[AlphaVantage] Fetching 1-min data for {base_symbol}")
            url = "https://www.alphavantage.co/query"
            params = {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": base_symbol,
                "interval": "1min",
                "outputsize": "full",
                "apikey": Config.ALPHA_VANTAGE_API_KEY,
            }
            res = requests.get(url, params=params)
            if res.status_code == 200:
                data = res.json()
                # Check for premium gate or error messages
                if "Information" in data or "Note" in data:
                    msg = data.get("Information", data.get("Note", ""))
                    logger.warning(f"[AlphaVantage] API message: {msg[:120]}")
                else:
                    ts_key = "Time Series (1min)"
                    if ts_key in data:
                        df = pd.DataFrame.from_dict(data[ts_key], orient="index")
                        df.index = pd.to_datetime(df.index)
                        df = df.rename(
                            columns={
                                "1. open": "Open",
                                "2. high": "High",
                                "3. low": "Low",
                                "4. close": "Close",
                                "5. volume": "Volume",
                            }
                        ).astype(float)
                        df = df[(df.index >= start_dt) & (df.index <= end_dt)]
                        df = df.sort_index()
                        if not df.empty:
                            logger.info(f"[AlphaVantage] Success: {len(df)} rows")
                            return df
                    logger.warning(f"[AlphaVantage] No matching data for {base_symbol}")
            else:
                logger.warning(f"[AlphaVantage] HTTP {res.status_code}")
        except Exception as e:
            logger.warning(f"[AlphaVantage] Exception: {e}")

    logger.error(f"All data providers failed for intraday data: {symbol}")
    return None


def _standardize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure standard Index and Columns for yFinance DataFrame"""
    if getattr(df.index, "tz", None) is not None:
        df.index = df.index.tz_convert(None)
    df.columns = [col.lower().replace(" ", "_") for col in df.columns]
    df.columns = [col.title().replace("_", "") for col in df.columns]
    df.index.name = "Date"
    return df


# ===== Async versions for faster parallel fetching =====


async def _fetch_yfinance_chunk_async(
    yf_symbol: str, chunk_start: datetime, chunk_end: datetime
) -> Optional[pd.DataFrame]:
    """Async helper to fetch a single chunk from yfinance with rate limiting."""
    global _yf_request_count, _yf_rate_limit_delay

    # Apply rate limiting delay if needed
    if _yf_rate_limit_delay > 0:
        await asyncio.sleep(_yf_rate_limit_delay)
        _yf_rate_limit_delay = 0.0

    loop = asyncio.get_event_loop()
    try:
        ticker = yf.Ticker(yf_symbol)
        df = await loop.run_in_executor(
            None,
            lambda: ticker.history(
                interval="1m", start=chunk_start, end=chunk_end, auto_adjust=False
            ),
        )

        _yf_request_count += 1

        # Check if we need to rate limit
        if _yf_request_count >= YF_RATE_LIMIT_THRESHOLD:
            logger.warning(
                f"Rate limit threshold reached ({YF_RATE_LIMIT_THRESHOLD}), pausing..."
            )
            _yf_rate_limit_delay = YF_RATE_LIMIT_PAUSE
            _yf_request_count = 0

        if df is not None and not df.empty:
            return _standardize_df(df)
    except Exception as e:
        err_str = str(e).lower()
        if "rate" in err_str or "429" in err_str or "too many" in err_str:
            logger.warning(f"[yFinance] Rate limited, backing off...")
            _yf_rate_limit_delay = YF_RATE_LIMIT_PAUSE * 2
            _yf_request_count = 0
        logger.warning(f"[yFinance] Chunk error: {e}")
    return None


async def fetch_intraday_ohlcv_async(
    symbol: str, start_dt: datetime, end_dt: datetime
) -> Optional[pd.DataFrame]:
    """Async version: Fetch 1-min OHLCV data with parallel chunk fetching.

    Fetches all chunks in parallel for a single symbol.
    """
    is_nse = symbol.upper().endswith(".NS")
    yf_symbol = symbol if is_nse else f"{symbol}.NS" if "." not in symbol else symbol

    max_yf_date = datetime.now() - timedelta(days=30)
    effective_start_dt = max(start_dt, max_yf_date)
    effective_end_dt = datetime.now()

    if effective_start_dt >= effective_end_dt:
        logger.warning(
            f"[yFinance] Requested range outside 30-day limit, skipping {symbol}"
        )
        return None

    date_range_days = (effective_end_dt - effective_start_dt).days

    if date_range_days > 5:
        logger.info(
            f"[yFinance] Async: fetching {yf_symbol} {effective_start_dt.date()} to {effective_end_dt.date()}"
        )

        tasks = []
        for chunk_start, chunk_end in _date_range_chunks(
            effective_start_dt, effective_end_dt, chunk_days=5
        ):
            task = _fetch_yfinance_chunk_async(yf_symbol, chunk_start, chunk_end)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_dfs = []
        for i, result in enumerate(results):
            if isinstance(result, pd.DataFrame) and not result.empty:
                all_dfs.append(result)
            elif isinstance(result, Exception):
                logger.warning(f"[yFinance] Chunk {i} failed: {result}")

        if all_dfs:
            combined = pd.concat(all_dfs).sort_index()
            logger.info(f"[yFinance] Async combined {len(combined)} rows from chunks")
            return combined

        logger.warning(f"[yFinance] No data from any chunk")
        return None
    else:
        return fetch_intraday_ohlcv(symbol, effective_start_dt, effective_end_dt)
