import logging
import os
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Try to import yfinance for fallback
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    logger.warning("yfinance not available, local data only")

# SEBI Compliance Constants
DATA_LAG_DAYS = 31


def get_data_lag_date() -> datetime:
    """Get the effective date with 31-day SEBI compliance lag."""
    return datetime.now() - pd.Timedelta(days=DATA_LAG_DAYS)


def check_data_availability(symbol: str = None) -> Dict:
    """
    Check data availability across parquet files.
    Returns information about data freshness and range.
    """
    try:
        data_dir = os.path.join(os.path.dirname(__file__), 'data')
        
        # Sample files to check date range
        sample_files = []
        for letter_dir in os.listdir(data_dir):
            letter_path = os.path.join(data_dir, letter_dir)
            if os.path.isdir(letter_path):
                files = [f for f in os.listdir(letter_path) if f.endswith('.parquet')]
                if files:
                    sample_files.append(os.path.join(letter_path, files[0]))
                if len(sample_files) >= 3:
                    break
        
        if not sample_files:
            return {
                'available': False,
                'message': 'No data files found',
                'data_range': None,
                'lag_days': DATA_LAG_DAYS
            }
        
        # Check first file for date range
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
        lag_date = get_data_lag_date()
        effective_date = min(last_date, lag_date)
        
        # Calculate if manual lag is needed
        needs_lag = lag_date > last_date
        days_behind = (lag_date - last_date).days if needs_lag else 0
        
        return {
            'available': True,
            'first_date': first_date.strftime('%Y-%m-%d'),
            'last_date': last_date.strftime('%Y-%m-%d'),
            'lag_date': lag_date.strftime('%Y-%m-%d'),
            'effective_last_date': effective_date.strftime('%Y-%m-%d'),
            'needs_manual_lag': needs_lag,
            'days_behind_lag': days_behind,
            'total_days': (last_date - first_date).days,
            'data_freshness_days': (datetime.now() - last_date).days,
            'lag_days': DATA_LAG_DAYS,
            'message': f"Data available from {first_date.strftime('%Y-%m-%d')} to {last_date.strftime('%Y-%m-%d')} "
                      f"with {DATA_LAG_DAYS}-day SEBI compliance lag"
        }
        
    except Exception as e:
        logger.error(f"Error checking data availability: {e}")
        return {
            'available': False,
            'message': f'Error: {str(e)}',
            'data_range': None,
            'lag_days': DATA_LAG_DAYS
        }


def apply_sebi_lag(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply 31-day SEBI compliance lag to DataFrame.
    Removes any data newer than 31 days.
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
    
    lag_date = get_data_lag_date()
    original_count = len(df)
    filtered_df = df[df.index <= lag_date].copy()
    
    if len(filtered_df) < original_count:
        logger.info(f"Applied {DATA_LAG_DAYS}-day SEBI lag: excluded {original_count - len(filtered_df)} rows")
    
    return filtered_df

class BacktestEngine:
    def __init__(self, df):
        """
        Initialize with a DataFrame. 
        Expects columns: 'Open', 'High', 'Low', 'Close', 'Volume' (case insensitive).
        """
        self.df = df.copy()
        # Normalize column names to lowercase for consistency
        self.df.columns = [c.lower() for c in self.df.columns]
        self.signals = pd.DataFrame(index=self.df.index)
        self.signals['signal'] = 0  # 0: Hold, 1: Buy, -1: Sell

    def add_moving_averages(self, short_window=50, long_window=200):
        """Calculates Simple Moving Averages."""
        self.df['sma_short'] = self.df['close'].rolling(window=short_window).mean()
        self.df['sma_long'] = self.df['close'].rolling(window=long_window).mean()

    def add_rsi(self, window=14):
        """Calculates Relative Strength Index."""
        delta = self.df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()

        rs = gain / loss
        self.df['rsi'] = 100 - (100 / (1 + rs))

    def add_macd(self, span_short=12, span_long=26, span_signal=9):
        """Calculates MACD and Signal line."""
        ema_short = self.df['close'].ewm(span=span_short, adjust=False).mean()
        ema_long = self.df['close'].ewm(span=span_long, adjust=False).mean()
        
        self.df['macd'] = ema_short - ema_long
        self.df['macd_signal'] = self.df['macd'].ewm(span=span_signal, adjust=False).mean()

    def add_atr(self, period=14):
        """Calculates Average True Range for Volatility Sizing."""
        high_low = self.df['high'] - self.df['low']
        high_close = np.abs(self.df['high'] - self.df['close'].shift())
        low_close = np.abs(self.df['low'] - self.df['close'].shift())
        
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = ranges.max(axis=1)
        
        self.df['atr'] = true_range.rolling(window=period).mean()

    def add_adx(self, period=14):
        """Calculates ADX and Directional Indicators (+DI, -DI)."""
        plus_dm = self.df['high'].diff()
        minus_dm = -self.df['low'].diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0
        
        tr = self.df['atr'] # Assumes ATR is already calculated
        
        # Using simple ewm for smoothing (Wilder's approximation)
        self.df['plus_di'] = 100 * (plus_dm.ewm(alpha=1/period, adjust=False).mean() / tr)
        self.df['minus_di'] = 100 * (minus_dm.ewm(alpha=1/period, adjust=False).mean() / tr)
        
        plus_di = self.df['plus_di']
        minus_di = self.df['minus_di']
        
        dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        self.df['adx'] = dx.rolling(window=period).mean()

    def add_volume_analysis(self, window=20):
        """Calculates Volume Moving Average to detect spikes."""
        self.df['vol_ma'] = self.df['volume'].rolling(window=window).mean()
        # Flag if volume is 2x the average
        self.df['vol_spike'] = self.df['volume'] > (self.df['vol_ma'] * 2.0)

    def add_momentum(self, period=10):
        """Calculates momentum for momentum strategy."""
        self.df['momentum'] = self.df['close'] - self.df['close'].shift(period)
        self.df['momentum_pct'] = ((self.df['close'] - self.df['close'].shift(period)) / self.df['close'].shift(period)) * 100

    def add_bollinger_bands(self, period=20, std_dev=2):
        """Calculates Bollinger Bands for mean reversion strategy."""
        self.df['bb_middle'] = self.df['close'].rolling(window=period).mean()
        self.df['bb_std'] = self.df['close'].rolling(window=period).std()
        self.df['bb_upper'] = self.df['bb_middle'] + (self.df['bb_std'] * std_dev)
        self.df['bb_lower'] = self.df['bb_middle'] - (self.df['bb_std'] * std_dev)

    def run_strategy(self, strategy_name="composite"):
        """
        Executes the pattern matching logic based on the selected strategy.
        Returns the DataFrame with signals and indicators.
        Available strategies: 'golden_cross', 'rsi', 'macd', 'composite', 'momentum', 'mean_reversion', 'breakout'
        """
        # Ensure indicators are present
        self.add_moving_averages()
        self.add_rsi()
        self.add_macd()
        self.add_atr()
        self.add_adx()
        self.add_volume_analysis()
        self.add_momentum()
        self.add_bollinger_bands()

        # Use a temporary Series for triggers (1=Buy, -1=Sell)
        triggers = pd.Series(0, index=self.df.index)

        # --- Pattern Logic ---
        
        if strategy_name == "golden_cross":
            # Buy: Short MA crosses above Long MA
            # Sell: Short MA crosses below Long MA
            buy_cond = (self.df['sma_short'] > self.df['sma_long']) & \
                            (self.df['sma_short'].shift(1) <= self.df['sma_long'].shift(1))
            
            sell_cond = (self.df['sma_short'] < self.df['sma_long']) & \
                             (self.df['sma_short'].shift(1) >= self.df['sma_long'].shift(1))

        elif strategy_name == "rsi":
            # Buy: RSI crosses below 30 (Oversold entry)
            # Sell: RSI crosses above 70 (Overbought exit)
            buy_cond = (self.df['rsi'] < 30) & (self.df['rsi'].shift(1) >= 30)
            sell_cond = (self.df['rsi'] > 70) & (self.df['rsi'].shift(1) <= 70)

        elif strategy_name == "macd":
            # Buy: MACD crosses above Signal
            # Sell: MACD crosses below Signal
            buy_cond = (self.df['macd'] > self.df['macd_signal']) & \
                            (self.df['macd'].shift(1) <= self.df['macd_signal'].shift(1))
            
            sell_cond = (self.df['macd'] < self.df['macd_signal']) & \
                             (self.df['macd'].shift(1) >= self.df['macd_signal'].shift(1))

        elif strategy_name == "composite":
            # Enhanced Logic:
            # Buy: (Golden Cross OR MACD Cross) AND Volume > Average AND (ADX > 20 & +DI > -DI)
            # Sell: Trend Breaks (+DI < -DI)
            ma_buy = (self.df['sma_short'] > self.df['sma_long']) & \
                     (self.df['sma_short'].shift(1) <= self.df['sma_long'].shift(1))
            
            macd_buy = (self.df['macd'] > self.df['macd_signal']) & \
                       (self.df['macd'].shift(1) <= self.df['macd_signal'].shift(1))
            
            vol_confirmation = self.df['volume'] > self.df['vol_ma']
            
            # Regime Filter: Strong Trend (ADX > 20) AND Bullish Direction (+DI > -DI)
            trend_confirmation = (self.df['adx'] > 20) & (self.df['plus_di'] > self.df['minus_di'])

            buy_cond = (ma_buy | macd_buy) & vol_confirmation & trend_confirmation
            
            # Sell when the bullish trend breaks
            sell_cond = self.df['plus_di'] < self.df['minus_di']

        elif strategy_name == "momentum":
            # Buy: Price crossover with positive momentum
            # Sell: Negative momentum or trend reversal
            price_above_ma = self.df['close'] > self.df['sma_short']
            positive_momentum = self.df['momentum_pct'] > 2.0
            vol_conf = self.df['volume'] > self.df['vol_ma']
            
            buy_cond = price_above_ma & positive_momentum & vol_conf
            
            sell_cond = (self.df['momentum_pct'] < -1.0) | \
                       (self.df['close'] < self.df['sma_short'])

        elif strategy_name == "mean_reversion":
            # Buy: Close below lower Bollinger Band AND RSI < 35
            # Sell: Close above upper Bollinger Band OR RSI > 65
            buy_cond = (self.df['close'] < self.df['bb_lower']) & (self.df['rsi'] < 35)
            
            sell_cond = (self.df['close'] > self.df['bb_upper']) | (self.df['rsi'] > 65)

        elif strategy_name == "breakout":
            # Buy: Price breaks above MA with high volume
            # Sell: Price breaks below MA or momentum reversal
            ma_breakout = (self.df['close'] > self.df['sma_short']) & \
                         (self.df['close'].shift(1) <= self.df['sma_short'].shift(1))
            vol_surge = self.df['volume'] > (self.df['vol_ma'] * 1.5)
            adx_strong = self.df['adx'] > 25
            
            buy_cond = ma_breakout & vol_surge & adx_strong
            
            sell_cond = (self.df['close'] < self.df['sma_short']) & \
                       (self.df['minus_di'] > self.df['plus_di'])
        
        else:
            raise ValueError(f"Unknown strategy: {strategy_name}")

        # Apply Triggers
        triggers.loc[buy_cond] = 1
        triggers.loc[sell_cond] = -1

        # Signal Persistence (State Flag)
        # We ffill() the state so we stay Long (1) until a Sell (-1) happens.
        self.signals['signal'] = triggers.replace(0, np.nan).ffill().fillna(0)
        
        # Convert to Binary State: 1 = Long, 0 = Cash
        # (If signal was -1, it becomes 0)
        self.signals['signal'] = self.signals['signal'].apply(lambda x: 1 if x > 0 else 0)

        # Merge signals back into main DF for visualization/export
        result = self.df.join(self.signals['signal'])
        
        return result

    def get_performance_summary(self, initial_capital=100000.0, is_long_only=True, start_date=None, end_date=None, 
                                atr_multiplier=3.0, tax_rate=0.002):
        """
        Event-Driven Backtest Simulator.
        Handles: Next Day Open Execution, Volatility Sizing, Dynamic ATR Stops, Gaps, Taxes.
        """
        # Prepare Data
        df_sim = self.df.join(self.signals['signal'])
        if start_date:
            df_sim = df_sim.loc[start_date:]
        if end_date:
            df_sim = df_sim.loc[:end_date]

        if df_sim.empty:
            return {"error": True, "message": "No data found for the specified date range."}

        # Simulation State
        cash = initial_capital
        shares = 0
        portfolio_values = []
        highest_price_since_entry = 0
        
        trades = []
        entry_date = None
        entry_price = 0
        
        # We iterate through the DataFrame. 
        # Logic: Decisions made on Day T (using Close/Signals) are executed on Day T+1 Open.
        
        dates = df_sim.index
        for i in range(len(df_sim) - 1):
            today = df_sim.iloc[i]
            next_day = df_sim.iloc[i+1]
            
            # 1. Update Portfolio Value (Mark to Market)
            current_val = cash + (shares * today['close'])
            portfolio_values.append(current_val)
            
            # 2. Determine Execution for Next Day
            signal = today['signal'] # 1 = Hold/Buy, 0 = Cash/Sell
            
            exit_price = None
            exit_reason = None
            
            # A) Check Stop Loss (Dynamic ATR)
            if shares > 0 and atr_multiplier > 0:
                # Update highest price seen during trade (using Today's High)
                highest_price_since_entry = max(highest_price_since_entry, today['high'])
                
                atr = today['atr']
                if pd.notna(atr) and atr > 0:
                    stop_price = highest_price_since_entry - (atr * atr_multiplier)
                    
                    # Check Gap Down (Open < Stop)
                    if next_day['open'] < stop_price:
                        exit_price = next_day['open']
                        exit_reason = "Stop Loss (Gap)"
                    # Check Intraday Breach (Low < Stop)
                    elif next_day['low'] < stop_price:
                        exit_price = stop_price
                        exit_reason = "Stop Loss (Intraday)"
            
            # B) Check Strategy Signal (State = 0 means Sell)
            # Signal exit happens at Open. Overrides Intraday stop if Open is valid.
            if shares > 0 and signal == 0:
                # If we haven't already gapped down below stop, we sell at Open
                if exit_price is None or exit_reason == "Stop Loss (Intraday)":
                    exit_price = next_day['open']
                    exit_reason = "Signal Exit"
            
            # --- EXECUTE SELL ---
            if shares > 0 and exit_price is not None:
                exec_price = exit_price
                
                # Revenue
                revenue = shares * exec_price
                # Tax/Slippage
                cost = revenue * tax_rate
                
                cash += (revenue - cost)
                
                # Record Trade
                pnl_pct = ((exec_price - entry_price) / entry_price) * 100
                trades.append({
                    'entry_date': entry_date.strftime('%Y-%m-%d') if entry_date else 'N/A',
                    'exit_date': next_day.name.strftime('%Y-%m-%d'),
                    'entry_price': entry_price,
                    'exit_price': exec_price,
                    'pnl_pct': pnl_pct,
                    'result': 'Win' if pnl_pct > 0 else 'Loss',
                    'reason': exit_reason
                })
                
                shares = 0
                highest_price_since_entry = 0
            
            # --- EXECUTE BUY ---
            elif shares == 0 and signal == 1:
                # Execute Buy at Next Day Open
                exec_price = next_day['open']
                
                # Volatility Sizing (ATR)
                # Rule: Risk 2% of capital per trade. Stop distance is 2 * ATR.
                # Position Size = (Capital * 0.02) / (2 * ATR) -> Simplified: Capital * 0.01 / ATR
                # If ATR is missing, default to 100% equity.
                atr = today['atr']
                if pd.notna(atr) and atr > 0:
                    risk_per_trade = current_val * 0.02 # 2% Risk
                    stop_distance = 2 * atr
                    target_shares = risk_per_trade / stop_distance
                    
                    # Cap shares so we don't spend more than cash
                    max_affordable = cash / (exec_price * (1 + tax_rate))
                    shares_to_buy = min(target_shares, max_affordable)
                else:
                    # Fallback: 100% Equity
                    shares_to_buy = cash / (exec_price * (1 + tax_rate))
                
                # Execute
                cost_basis = shares_to_buy * exec_price
                fees = cost_basis * tax_rate
                
                if cash >= (cost_basis + fees):
                    cash -= (cost_basis + fees)
                    shares = shares_to_buy
                    highest_price_since_entry = exec_price # Initialize with entry price
                    entry_price = exec_price
                    entry_date = next_day.name

        # Append last day value
        last_val = cash + (shares * df_sim.iloc[-1]['close'])
        portfolio_values.append(last_val)
        
        # Create Series for Metrics
        equity_curve = pd.Series(portfolio_values, index=df_sim.index)
        daily_returns = equity_curve.pct_change().fillna(0)
        
        # Calculate Percentage Returns (ROI)
        strategy_roi = ((equity_curve.iloc[-1] - initial_capital) / initial_capital) * 100
        
        # Benchmark (Buy and Hold) - Simple calculation
        market_curve = (df_sim['close'] / df_sim['close'].iloc[0]) * initial_capital
        market_roi = ((market_curve.iloc[-1] - initial_capital) / initial_capital) * 100

        # Risk Metrics
        sharpe_ratio = (daily_returns.mean() / daily_returns.std()) * np.sqrt(252) if daily_returns.std() != 0 else 0
        
        rolling_max = equity_curve.cummax()
        drawdown = (equity_curve - rolling_max) / rolling_max
        max_drawdown = drawdown.min() * 100

        return {
            "error": False,
            "final_portfolio_value": float(equity_curve.iloc[-1]),
            "market_buy_hold_value": float(market_curve.iloc[-1]),
            "strategy_return_pct": float(strategy_roi),
            "market_return_pct": float(market_roi),
            "sharpe_ratio": float(sharpe_ratio),
            "max_drawdown_pct": float(max_drawdown),
            "trades": trades,
            "trades_df": pd.DataFrame(trades).assign(result=lambda df: df["pnl_pct"].apply(lambda x: "Win" if x > 0 else "Loss"))
        }


def get_parquet_path(symbol: str) -> Optional[str]:
    """
    Get the absolute file path for a stock symbol's parquet data file.
    Automatically adds .NS suffix for NSE stocks if not present.
    """
    # Use absolute path relative to this file's directory
    base_dir = os.path.dirname(__file__)
    if not symbol or len(symbol) == 0:
        logger.error(f"Invalid symbol: {symbol}")
        return None
    
    # Ensure symbol has .NS suffix for NSE stocks
    symbol_upper = symbol.upper().strip()
    if not symbol_upper.endswith('.NS'):
        symbol_with_suffix = f"{symbol_upper}.NS"
    else:
        symbol_with_suffix = symbol_upper
    
    first_char = symbol_with_suffix[0].upper()
    file_path = os.path.join(base_dir, 'data', first_char, f"{symbol_with_suffix}.parquet")
    return file_path


# Cache for loaded stock data to reduce latency
_stock_data_cache: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
CACHE_TTL_SECONDS = 300  # Cache data for 5 minutes


def _get_cached_stock_data(symbol: str) -> Optional[pd.DataFrame]:
    """Get stock data from cache if available and not expired."""
    if symbol in _stock_data_cache:
        df, timestamp = _stock_data_cache[symbol]
        if datetime.now() - timestamp < timedelta(seconds=CACHE_TTL_SECONDS):
            logger.info(f"Cache hit for {symbol}")
            return df.copy()
        else:
            logger.info(f"Cache expired for {symbol}")
            del _stock_data_cache[symbol]
    return None


def _set_cached_stock_data(symbol: str, df: pd.DataFrame):
    """Store stock data in cache."""
    _stock_data_cache[symbol] = (df.copy(), datetime.now())
    logger.info(f"Cached data for {symbol}")

def load_stock_data(symbol: str, apply_lag: bool = True) -> Tuple[Optional[pd.DataFrame], Dict]:
    """
    Load parquet data for a given stock symbol with optional SEBI compliance lag.
    Uses caching to reduce latency for repeated requests.
    
    Args:
        symbol: Stock symbol to load
        apply_lag: Whether to apply 31-day SEBI compliance lag (default: True)
    
    Returns:
        Tuple of (DataFrame or None, compliance_info dict)
    """
    try:
        symbol_upper = symbol.upper().strip()
        
        # Try to get from cache first
        cached_df = _get_cached_stock_data(symbol_upper)
        if cached_df is not None:
            compliance_info = {
                'symbol': symbol_upper,
                'original_rows': len(cached_df),
                'date_range': {
                    'start': cached_df.index.min().strftime('%Y-%m-%d'),
                    'end': cached_df.index.max().strftime('%Y-%m-%d')
                },
                'lag_applied': apply_lag,
                'lag_days': DATA_LAG_DAYS if apply_lag else 0,
                'cached': True
            }
            
            # Apply SEBI compliance lag if requested
            if apply_lag:
                df = apply_sebi_lag(cached_df.copy())
                compliance_info['filtered_rows'] = len(df)
                compliance_info['rows_excluded'] = compliance_info['original_rows'] - len(df)
                compliance_info['effective_end_date'] = df.index.max().strftime('%Y-%m-%d') if not df.empty else None
                return df, compliance_info
            else:
                return cached_df.copy(), compliance_info
        
        file_path = get_parquet_path(symbol_upper)
        if not file_path:
            return None, {'error': f'No data found for symbol {symbol_upper}'}
        
        if not os.path.exists(file_path):
            logger.error(f"Parquet file not found: {file_path}")
            return None, {'error': f'Data file not found for {symbol_upper}'}
        
        logger.info(f"Loading data from {file_path}")
        df = pd.read_parquet(file_path, engine='pyarrow')
        
        # Ensure the DataFrame has a DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            # Check if there is a 'Date' column to use as index
            date_col = next((c for c in df.columns if c.lower() == 'date'), None)
            if date_col:
                logger.info(f"Converting column '{date_col}' to Datetime Index...")
                df[date_col] = pd.to_datetime(df[date_col])
                df.set_index(date_col, inplace=True)
            else:
                # Attempt to convert the existing index to datetime
                df.index = pd.to_datetime(df.index)
        
        # Standardize column names to PascalCase for consistency across the application
        # This avoids repeated column name conversions in routes
        df.columns = [col.title().replace('_', '') for col in df.columns]
        df.index.name = 'Date'
        
        # Cache the standardized data before applying lag
        _set_cached_stock_data(symbol_upper, df)
        
        compliance_info = {
            'symbol': symbol_upper,
            'original_rows': len(df),
            'date_range': {
                'start': df.index.min().strftime('%Y-%m-%d'),
                'end': df.index.max().strftime('%Y-%m-%d')
            },
            'lag_applied': apply_lag,
            'lag_days': DATA_LAG_DAYS if apply_lag else 0,
            'cached': False
        }
        
        # Apply SEBI compliance lag if requested
        if apply_lag:
            df = apply_sebi_lag(df)
            compliance_info['filtered_rows'] = len(df)
            compliance_info['rows_excluded'] = compliance_info['original_rows'] - len(df)
            compliance_info['effective_end_date'] = df.index.max().strftime('%Y-%m-%d') if not df.empty else None
        
        return df, compliance_info
    
    except Exception as e:
        logger.error(f"Error loading stock data for {symbol}: {e}")
        return None, {'error': str(e)}


def fetch_from_yfinance(symbol: str, period: str = "90d", interval: str = "1d") -> Optional[pd.DataFrame]:
    """
    Fetch stock data from yfinance as fallback.
    Automatically adds .NS suffix for NSE stocks if not present.
    
    Args:
        symbol: Stock symbol to fetch
        period: Time period to fetch (e.g., '90d', '1y')
        interval: Data interval (e.g., '1d', '1h')
    
    Returns:
        DataFrame with OHLCV data or None if fetch fails
    """
    if not YFINANCE_AVAILABLE:
        logger.warning("yfinance not available, cannot fetch from API")
        return None
    
    try:
        # Ensure symbol has .NS suffix for NSE stocks
        symbol_upper = symbol.upper().strip()
        if not symbol_upper.endswith('.NS'):
            symbol_with_suffix = f"{symbol_upper}.NS"
        else:
            symbol_with_suffix = symbol_upper
        
        logger.info(f"Fetching {symbol_with_suffix} from yfinance (period={period}, interval={interval})")
        ticker = yf.Ticker(symbol_with_suffix)
        df = ticker.history(period=period, interval=interval)
        
        if df.empty:
            logger.warning(f"No data returned from yfinance for {symbol_with_suffix}")
            return None
        
        # Standardize column names to lowercase
        df.columns = [col.lower().replace(' ', '_') for col in df.columns]
        
        # Convert to PascalCase for consistency with local data
        df.columns = [col.title().replace('_', '') for col in df.columns]
        df.index.name = 'Date'
        
        logger.info(f"Successfully fetched {len(df)} rows from yfinance for {symbol_with_suffix}")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching {symbol_with_suffix if 'symbol_with_suffix' in locals() else symbol} from yfinance: {e}")
        return None


def save_to_local_data(symbol: str, df: pd.DataFrame) -> bool:
    """
    Save fetched data to local parquet file for future use.
    
    Args:
        symbol: Stock symbol
        df: DataFrame with OHLCV data
    
    Returns:
        True if saved successfully, False otherwise
    """
    try:
        file_path = get_parquet_path(symbol)
        if not file_path:
            return False
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Standardize column names before saving
        df_to_save = df.copy()
        df_to_save.columns = [col.lower().replace(' ', '_') for col in df_to_save.columns]
        
        # Save to parquet
        df_to_save.to_parquet(file_path, compression='snappy')
        logger.info(f"Saved {len(df_to_save)} rows to local data: {file_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error saving data for {symbol}: {e}")
        return False


def get_stock_data_with_fallback(
    symbol: str,
    period: str = "90d",
    interval: str = "1d",
    apply_lag: bool = True,
    min_rows: int = 30
) -> Tuple[Optional[pd.DataFrame], Dict]:
    """
    Get stock data with local data as primary source and yfinance as fallback.
    
    This function implements a priority-based data fetching strategy:
    1. First, try to load from local parquet files
    2. If local data is insufficient or missing, fetch from yfinance
    3. Save yfinance data to local storage for future use
    
    Args:
        symbol: Stock symbol to load
        period: Time period for yfinance fallback (e.g., '90d', '1y')
        interval: Data interval for yfinance fallback (e.g., '1d', '1h')
        apply_lag: Whether to apply 31-day SEBI compliance lag
        min_rows: Minimum number of rows required for local data to be considered sufficient
    
    Returns:
        Tuple of (DataFrame or None, metadata dict with source info)
    """
    symbol_upper = symbol.upper().strip()
    metadata = {
        'symbol': symbol_upper,
        'source': None,
        'local_available': False,
        'yfinance_available': False,
        'data_completeness': {},
        'lag_applied': apply_lag
    }
    
    # Step 1: Try to load from local data
    logger.info(f"Attempting to load {symbol_upper} from local data (primary source)")
    local_df, local_info = load_stock_data(symbol_upper, apply_lag=apply_lag)
    
    if local_df is not None and not local_df.empty:
        metadata['local_available'] = True
        metadata['data_completeness']['local_rows'] = len(local_df)
        metadata['data_completeness']['local_date_range'] = local_info.get('date_range', {})
        
        # Check if local data is sufficient
        if len(local_df) >= min_rows:
            logger.info(f"Using local data for {symbol_upper}: {len(local_df)} rows")
            metadata['source'] = 'local'
            metadata['yfinance_fallback'] = False
            return local_df, metadata
        else:
            logger.warning(f"Local data insufficient for {symbol_upper}: {len(local_df)} rows (min: {min_rows})")
            metadata['data_completeness']['local_insufficient'] = True
    else:
        logger.warning(f"No local data available for {symbol_upper}")
        metadata['data_completeness']['local_missing'] = True
    
    # Step 2: Fallback to yfinance
    logger.info(f"Falling back to yfinance for {symbol_upper}")
    yf_df = fetch_from_yfinance(symbol_upper, period=period, interval=interval)
    
    if yf_df is not None and not yf_df.empty:
        metadata['yfinance_available'] = True
        metadata['data_completeness']['yfinance_rows'] = len(yf_df)
        metadata['data_completeness']['yfinance_date_range'] = {
            'start': yf_df.index.min().strftime('%Y-%m-%d'),
            'end': yf_df.index.max().strftime('%Y-%m-%d')
        }
        
        # Apply SEBI lag if requested
        if apply_lag:
            yf_df = apply_sebi_lag(yf_df)
            metadata['data_completeness']['yfinance_rows_after_lag'] = len(yf_df)
        
        # Save to local storage for future use
        save_success = save_to_local_data(symbol_upper, yf_df)
        metadata['data_completeness']['saved_to_local'] = save_success
        
        logger.info(f"Using yfinance data for {symbol_upper}: {len(yf_df)} rows")
        metadata['source'] = 'yfinance'
        metadata['yfinance_fallback'] = True
        return yf_df, metadata
    
    # Step 3: Neither source available
    logger.error(f"No data available for {symbol_upper} from any source")
    metadata['source'] = None
    metadata['error'] = 'No data available from local storage or yfinance'
    return None, metadata


def batch_fetch_prices(symbols: List[str], period: str = "60d") -> Dict[str, Optional[pd.DataFrame]]:
    """
    Batch fetch prices for multiple symbols with local data priority.
    
    Args:
        symbols: List of stock symbols
        period: Time period to fetch
    
    Returns:
        Dictionary mapping symbol to DataFrame or None
    """
    results = {}
    
    for symbol in symbols:
        df, metadata = get_stock_data_with_fallback(
            symbol,
            period=period,
            apply_lag=True,
            min_rows=1  # For price fetching, even 1 row is sufficient
        )
        results[symbol] = df
    
    return results


def get_current_price(symbol: str) -> Optional[float]:
    """
    Get current/latest price for a symbol with local data priority.
    
    Args:
        symbol: Stock symbol
    
    Returns:
        Latest price or None if unavailable
    """
    df, metadata = get_stock_data_with_fallback(
        symbol,
        period="5d",  # Short period for price check
        apply_lag=True,
        min_rows=1
    )
    
    if df is not None and not df.empty:
        # Get the latest close price
        if 'close' in df.columns:
            return float(df['close'].iloc[-1])
    
    return None