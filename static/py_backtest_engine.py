import pandas as pd
import numpy as np

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

    def run_strategy(self, config):
        """
        Executes the pattern matching logic based on the selected strategy.
        Returns the DataFrame with signals and indicators.
        Available strategies: 'golden_cross', 'rsi', 'macd', 'composite', 'momentum', 'mean_reversion', 'breakout'
        """
        strategy_name = config.get('strategy', 'composite')
        
        # Extract custom parameters from config
        sma_short = int(config.get('sma_short', 50))
        sma_long = int(config.get('sma_long', 200))
        rsi_window = int(config.get('rsi_window', 14))
        rsi_oversold = int(config.get('rsi_oversold', 30))
        rsi_overbought = int(config.get('rsi_overbought', 70))
        macd_short = int(config.get('macd_short', 12))
        macd_long = int(config.get('macd_long', 26))
        
        # Ensure indicators are present with custom parameters where applicable
        self.add_moving_averages(short_window=sma_short, long_window=sma_long)
        self.add_rsi(window=rsi_window)
        self.add_macd(span_short=macd_short, span_long=macd_long)
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
            # Buy: RSI crosses below oversold (entry)
            # Sell: RSI crosses above overbought (exit)
            buy_cond = (self.df['rsi'] < rsi_oversold) & (self.df['rsi'].shift(1) >= rsi_oversold)
            sell_cond = (self.df['rsi'] > rsi_overbought) & (self.df['rsi'].shift(1) <= rsi_overbought)

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
                                atr_multiplier=3.0, risk_per_trade=0.02, tax_rate=0.002):
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
                if pd.notna(atr) and atr > 0:
                    risk_amount = current_val * risk_per_trade
                    stop_distance = 2 * atr
                    target_shares = risk_amount / stop_distance
                    
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

import json
import math

def _sanitize_value(v):
    """Replace NaN/Inf floats with 0 so json.dumps produces valid JSON."""
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return 0.0
    return v

def _sanitize_dict(d):
    """Recursively sanitize a dict, replacing NaN/Inf with 0."""
    cleaned = {}
    for k, v in d.items():
        if isinstance(v, dict):
            cleaned[k] = _sanitize_dict(v)
        elif isinstance(v, list):
            cleaned[k] = [_sanitize_dict(item) if isinstance(item, dict) else _sanitize_value(item) for item in v]
        else:
            cleaned[k] = _sanitize_value(v)
    return cleaned

def run_backtest_browser(data_json, config):
    try:
        data = json.loads(data_json)
        df = pd.DataFrame(data)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])
            df.set_index('Date', inplace=True)
            
        engine = BacktestEngine(df)
        engine.run_strategy(config)
        
        perf = engine.get_performance_summary(
            initial_capital=float(config.get('initial_balance', 100000)),
            is_long_only=True,
            start_date=config.get('start_date'),
            end_date=config.get('end_date'),
            atr_multiplier=float(config.get('atr_multiplier', 3.0)),
            risk_per_trade=float(config.get('risk_per_trade', 0.02)),
            tax_rate=0.002
        )
        
        # We drop the dataframe for JSON serialization
        if 'trades_df' in perf:
            del perf['trades_df']
        
        # Sanitize NaN/Inf values to prevent invalid JSON
        perf = _sanitize_dict(perf)
            
        return json.dumps(perf)
    except Exception as e:
        return json.dumps({'error': True, 'message': str(e)})
