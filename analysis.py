"""
Analysis Module
Handles stock data analysis, technical indicators, AI integration with Gemini.
"""
import logging
import math
import random
import statistics
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from config import Config

logger = logging.getLogger(__name__)

# Global data storage
latest_symbol_data = {}
conversation_context = {}


# ==================== DATA HELPER FUNCTIONS ====================
def convert_to_serializable(value):
    """Convert numpy/pandas types to JSON-serializable types"""
    if pd.isna(value) or value is None: return None
    if isinstance(value, (np.integer, np.int64)): return int(value)
    if isinstance(value, (np.floating, np.float64)):
        if np.isnan(value) or np.isinf(value): return None
        return float(value)
    if isinstance(value, np.bool_): return bool(value)
    return value


def clean_df(df, columns):
    """Clean dataframe for JSON serialization"""
    df = df.copy().reset_index()
    if 'Date' in df.columns:
        df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(convert_to_serializable)
    cols_to_include = ['Date'] + [col for col in columns if col in df.columns]
    return df[cols_to_include].to_dict(orient='records')


# ==================== TECHNICAL INDICATORS ====================
def compute_rsi(series, period=14):
    """
    Calculate Relative Strength Index (RSI) using a standard exponential
    moving average method (Wilder's smoothing).
    """
    delta = series.diff(1)

    # Separate gains and losses, and fill the initial NaN
    gain = delta.where(delta > 0, 0.0).fillna(0)
    loss = -delta.where(delta < 0, 0.0).fillna(0)

    # Calculate Wilder's smoothing for gain and loss
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def compute_macd(series):
    """Calculate MACD indicator"""
    ema12 = series.ewm(span=12, adjust=False).mean()
    ema26 = series.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    histogram = macd - signal
    return macd, signal, histogram


# ==================== ANALYSIS HELPER FUNCTIONS ====================
def safe_get(d: Dict, key: str, default=None):
    """Safely get a value from a dict, returning default if key is missing or value is None."""
    v = d.get(key)
    return v if v is not None else default


def mean_or(val_list, fallback=0.0):
    try:
        return statistics.mean(val_list) if val_list else fallback
    except Exception:
        return fallback


def linear_slope(y_values: List[float]) -> float:
    """Calculate linear regression slope"""
    if not y_values or len(y_values) < 2: return 0.0
    x = np.arange(len(y_values))
    y = np.array(y_values, dtype=float)
    xv = x - x.mean()
    yv = y - y.mean()
    denom = (xv * xv).sum()
    if denom == 0: return 0.0
    return float((xv * yv).sum() / denom)


def find_recent_macd_crossover(latest_data: List[Dict], lookback: int = 14) -> Tuple[str, int]:
    """Find recent MACD crossover signals"""
    n = len(latest_data)
    upper = max(1, n - lookback)
    for i in range(n - 1, upper - 1, -1):
        if i == 0: continue
        prev = latest_data[i - 1]
        curr = latest_data[i]
        prev_diff = safe_get(prev, 'MACD', 0) - safe_get(prev, 'Signal', 0)
        curr_diff = safe_get(curr, 'MACD', 0) - safe_get(curr, 'Signal', 0)
        if prev_diff <= 0 and curr_diff > 0: return 'bullish', n - i - 1
        if prev_diff >= 0 and curr_diff < 0: return 'bearish', n - i - 1
    return 'none', -1


def fmt_price(x):
    """Format price for display"""
    try:
        return f"${round(x, 2)}"
    except Exception:
        return str(x)


# ==================== GEMINI AI INTEGRATION ====================
# Define a pool of models to rotate through for load balancing and fallback.
# Includes the Gemma 3 variants requested and Gemini 2.0 Flash as a robust backup.
GEMINI_MODELS = [
    "gemma-3-1b-it", 
    "gemma-3-4b-it",
    "gemma-3-12b-it",
    "gemma-3-27b-it",
    "gemini-2.0-flash"
]
#helloworld
def call_gemini_api(prompt: str) -> str:
    """Call the Gemini API, rotating through models to handle rate limits."""
    api_key = Config.GEMINI_API_KEY
    if not api_key:
        logger.warning("GEMINI_API_KEY is not set in the environment.")
        return "⚠️ **AI Service Misconfigured** – The API key is not set on the server."

    # Shuffle models to spread the load (smart delegation)
    models_queue = GEMINI_MODELS.copy()
    random.shuffle(models_queue)

    for model in models_queue:
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}"
        
        try:
            # logger.info(f"🤖 Attempting AI generation with model: {model}")
            response = requests.post(
                api_url,
                headers={"Content-Type": "application/json"},
                json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": 0.7, "topK": 40, "topP": 0.95, "maxOutputTokens": 1024}
                },
                timeout=30
            )

            # Handle Rate Limits (429), Service Overload (503), or Model Not Found (404)
            if response.status_code in [429, 503, 404]:
                logger.warning(f"⚠️ Model {model} unavailable ({response.status_code}). Switching...")
                continue

            response.raise_for_status()
            result = response.json()

            if 'candidates' in result and result['candidates'] and 'content' in result['candidates'][0]:
                return result['candidates'][0]['content']['parts'][0]['text']
            
            # Handle safety blocks
            if 'promptFeedback' in result and result['promptFeedback'].get('blockReason'):
                return f"⚠️ **AI Prompt Blocked** – Safety filter: {result['promptFeedback'].get('blockReason')}"
            if 'candidates' in result and result['candidates'] and result['candidates'][0].get('finishReason') == 'SAFETY':
                return "⚠️ **AI Response Blocked** – Safety filter triggered."

            # If empty response, try next model
            continue

        except Exception as e:
            logger.error(f"❌ Error with model {model}: {e}")
            continue

    return "⚠️ **System Busy** – All AI models are currently experiencing high traffic. Please try again later."


def format_data_for_ai_skimmable(symbol: str, data: list) -> str:
    """Format stock data for AI analysis"""
    if not data: return "No data available."
    latest = data[-1]
    prev = data[-2] if len(data) >= 2 else latest
    close, open_, ma5, ma10, rsi, macd, signal, hist, volume = (latest.get(k, 0) for k in
                                                                ['Close', 'Open', 'MA5', 'MA10', 'RSI', 'MACD',
                                                                 'Signal', 'Histogram', 'Volume'])

    summary = [
        f"**Date:** {latest.get('Date', 'N/A')} | **Close:** ${close:.2f} | {'Bullish 🟢' if close > open_ else 'Bearish 🔴'}",
        f"**RSI:** {rsi:.2f} | {'Overbought 🔥' if rsi > 70 else 'Oversold ❄️' if rsi < 30 else 'Neutral ✅'}",
        f"**MACD:** {macd:.2f} (Signal: {signal:.2f}) | Histogram: {hist:.2f}",
        f"**7-Day Trend:** {'Bullish 🟢' if close > prev.get('Close', close) else 'Bearish 🔴'}",
        f"**MA5:** ${ma5:.2f} | **MA10:** ${ma10:.2f} | {'Bullish Alignment 🟢' if ma5 > ma10 else 'Bearish Alignment 🔴'}"
    ]

    vols = [d.get('Volume', 0) for d in data if d.get('Volume')]
    avg_vol = sum(vols) / len(vols) if vols else 1
    vol_ratio = volume / avg_vol
    summary.append(
        f"**Volume:** {volume:,} ({vol_ratio:.2f}x avg) | {'Accumulation 📈' if vol_ratio > 1.1 else 'Distribution 📉' if vol_ratio < 0.9 else 'Stable ➡️'}")

    highs = [d.get('High', 0) for d in data]
    lows = [d.get('Low', 0) for d in data]
    summary.append(f"**Support:** ${min(lows):.2f} | **Resistance:** ${max(highs):.2f}")

    return "\n".join(summary)


def get_gemini_ai_analysis(symbol: str, data: list) -> str:
    """Get AI-powered analysis from Gemini as a Data-Analyst persona."""
    data_summary = format_data_for_ai_skimmable(symbol, data)
    
    # Get the most recent date from the data for context
    latest_date = data[-1].get('Date', 'N/A') if data else 'N/A'
    
    prompt = f"""
You are the **Fintra Historical Data Interpreter**. Your role is to describe historical technical patterns found in the provided HISTORICAL dataset. 
You are NOT an advisor; you are a lens through which the user views PAST data only.

**⚠️ CRITICAL CONTEXT: HISTORICAL DATA ONLY ⚠️**
- The data provided is historical and is at least 31 days old (most recent data point: {latest_date})
- This is NOT current market data
- All analysis must be framed as historical retrospective, not current assessment
- You are analyzing what happened in the past, not what is happening now

### OBJECTIVES:
1. **🎯 Historical Data Snapshot:** Describe the historical price location relative to the 52-week range at the time of the data (ending {latest_date}). Use past tense: "Price was trading at..."
2. **📊 Historical Pattern Recognition:** Identify historical patterns (e.g., "As of {latest_date}, the price was forming a consolidation range"). Use "At that time..." language.
3. **⚡ Historical Structural Benchmarks:** Identify Support and Resistance levels that existed in the historical data provided. Frame as "Historical support was observed at..."
4. **📈 Historical Volume Context:** Describe historical volume patterns: "During this historical period, volume was..."
5. **🚨 Historical Invalidation Points:** Identify price levels where the historical technical setup would have been considered "broken" at that time.
6. **🔍 Historical Comparative Math:** Calculate distances from historical price to historical S/R levels. Frame as "At {latest_date}, the price was X% below historical resistance."

### MANDATORY CONSTRAINTS:
- **⏰ TIME CONTEXT:** ALWAYS reference the historical date ({latest_date}) in your analysis
- **📖 PAST TENSE ONLY:** Use "was," "had been," "showed," "indicated" - NEVER "is," "current," "now," "today"
- **🚫 NO CURRENT REFERENCES:** Never say "current price," "current trend," "current market" - use "historical price," "historical trend," "at that time"
- **🚫 NO ADVISORY VERBS:** Never use "Recommend," "Suggest," "Buy," "Sell," "Should," or "Target."
- **🚫 NO PREDICTIONS:** Use terms like "Historically," "At that time," "The data showed"
- **✅ HISTORICAL FRAMING:** Every sentence must clearly indicate this is historical analysis (e.g., "As of {latest_date}...", "During this historical period...")
- **DISCLAIMER:** Every response MUST conclude with the Mandatory Disclaimer below.

## HISTORICAL MARKET DATA (As of {latest_date})
{data_summary}

## MANDATORY DISCLAIMER
⚠️ **HISTORICAL DATA ALERT:** This analysis is based on data ending {latest_date} and includes a mandatory 30+ day lag in accordance with SEBI regulations. This is NOT current market data. Fintra is a data visualization and interpretation tool. This output is generated by AI based on historical technical indicators and is for educational purposes only. It does not account for fundamental factors, news, or individual financial situations. This is NOT financial advice. Past performance is not indicative of future results.
"""
    return call_gemini_api(prompt)


def get_gemini_position_summary(position_data: Dict) -> str:
    """Get an AI-powered summary for a specific user position."""
    symbol = position_data.get('symbol')
    quantity = position_data.get('quantity')
    entry_price = position_data.get('entry_price')
    current_price = position_data.get('current_price')
    pnl = position_data.get('pnl')
    pnl_percent = position_data.get('pnl_percent')

    if not all([symbol, quantity, entry_price, current_price, pnl is not None, pnl_percent is not None]):
        return "⚠️ Insufficient position data for AI summary."

    # Get latest date from symbol data if available
    latest_date = 'N/A'
    if symbol in latest_symbol_data and latest_symbol_data[symbol]:
        latest_date = latest_symbol_data[symbol][-1].get('Date', 'N/A')

    position_context = (
        f"The user holds **{quantity} shares** of **{symbol}** with an entry price of **${entry_price:,.2f}**. "
        f"The historical price (as of {latest_date}) is **${current_price:,.2f}**. "
        f"This results in a historical P&L of **${pnl:,.2f} ({pnl_percent:+.2f}%)** as of {latest_date}."
    )

    technical_context = ""
    if symbol in latest_symbol_data:
        technical_context = generate_rule_based_analysis(symbol, latest_symbol_data[symbol])

    prompt = f"""You are the **Fintra Historical Position Logic Engine**. Your task is to provide a quantitative 
decomposition of a user's specific stock position based on HISTORICAL data only. 
You act as a data lens viewing the past, not a financial advisor.

**⚠️ CRITICAL CONTEXT: HISTORICAL DATA ONLY (As of {latest_date}) ⚠️**
- All price data is historical and at least 31 days old
- This is NOT a current assessment of the position
- Analysis must be framed as historical retrospective
- Use past tense to describe the position status

**POSITION DATA (Historical as of {latest_date}):**
{position_context}

**HISTORICAL TECHNICAL DATA OVERLAY:**
{technical_context}

**YOUR TASK:**
Deconstruct the HISTORICAL state of this position into the following three sections:

1. 📈 **Historical Trend Alignment:** Describe how the historical price (as of {latest_date}) was behaving relative to the user's entry point. Use past tense: "As of {latest_date}, the position showed..." Avoid directives like "Hold" or "Sell."
2. 🛡️ **Historical Risk Context:** Analyze the historical distance between the price and support level as of {latest_date}. (e.g., "At {latest_date}, the price maintained a 4% buffer above the historical support level of $X").
3. ⚡ **Historical Structural Levels:** State the historical Support and Resistance boundaries that existed at {latest_date}.

**RULES:**
- **⏰ HISTORICAL CONTEXT:** ALWAYS reference {latest_date} and use past tense ("was," "had been," "showed")
- **🚫 NO CURRENT REFERENCES:** Never say "current price," "current trend," "now" - use "historical price as of {latest_date}", "at that time"
- **🚫 NO RECOMMENDATIONS:** Never use words like "Buy," "Sell," "Hold," "Trim," or "Stance."
- **🚫 NO EVALUATIVE ADJECTIVES:** Avoid "Healthy," "Good," "Bad," or "Concerning." Use "Positive/Negative variance" or "Trend-aligned."
- **📖 HISTORICAL FRAMING:** Every sentence must indicate this is historical analysis (e.g., "As of {latest_date}...", "At that time...")
- **DISCLAIMER:** Every response MUST conclude with the Mandatory Disclaimer provided below.

Provide your historical summary now:

## MANDATORY DISCLAIMER
⚠️ **HISTORICAL DATA ALERT:** This analysis is based on data ending {latest_date} with a mandatory 30+ day lag per SEBI regulations. This is NOT a current assessment. Fintra is a data-visualization tool. This summary is an automated mathematical interpretation of historical data and your specific position as of {latest_date}. It is NOT financial advice. All trading involves risk; ensure you consult a licensed professional before making investment decisions.
"""
    return call_gemini_api(prompt)

def generate_rule_based_analysis(symbol: str, latest_data: List[Dict], lookback: int = 14) -> str:
    """Generate comprehensive rule-based technical analysis"""
    try:
        if not latest_data or len(latest_data) < 7:
            return "### ⚠️ Analysis Unavailable\nInsufficient data for reliable analysis. Need at least 7 trading days."

        n, lb = len(latest_data), min(lookback, len(latest_data))
        window = latest_data[-lb:]
        required = ['Close', 'Volume', 'MA5', 'MA10', 'RSI', 'MACD', 'Signal', 'Histogram', 'High', 'Low']
        if missing := {f for row in window for f in required if f not in row or row.get(f) is None}:
            return f"### ⚠️ Analysis Unavailable\nMissing required fields: {', '.join(sorted(missing))}"

        latest = window[-1]
        close_price, rsi, macd, signal, hist, volume, ma5, ma10 = (float(latest.get(k, 0.0)) for k in
                                                                   ['Close', 'RSI', 'MACD', 'Signal', 'Histogram',
                                                                    'Volume', 'MA5', 'MA10'])
        recent_high = round(max(float(d.get('High', -math.inf)) for d in window), 2)
        recent_low = round(min(float(d.get('Low', math.inf)) for d in window), 2)

        rsi_series, macd_series, hist_series, vol_series = ([float(d[k]) for d in window] for k in
                                                            ['RSI', 'MACD', 'Histogram', 'Volume'])
        rsi_velocity = (rsi_series[-1] - rsi_series[0]) / max(1, len(rsi_series) - 1)
        macd_slope, hist_slope = linear_slope(macd_series), linear_slope(hist_series)
        macd_diff = macd - signal
        crossover_type, crossover_days_ago = find_recent_macd_crossover(window, lookback=lb)

        avg_vol = mean_or(vol_series, fallback=volume if volume > 0 else 1.0)
        volume_ratio = (volume / avg_vol) if avg_vol > 0 else 1.0
        price_vs_ma5, price_vs_ma10 = ("above" if close_price > ma5 else "below"), (
            "above" if close_price > ma10 else "below")
        ma_trend = "bullish" if ma5 > ma10 else "bearish"
        ma_spread_pct = abs(ma5 - ma10) / ma10 * 100 if ma10 != 0 else 0.0

        # Scoring logic (abbreviated for brevity - full logic preserved)
        def rsi_zone_score_and_note(rsi_val, rsi_vel):
            if rsi_val < 30: return 2.0, "Oversold - potential reversal zone", "🟢"
            if rsi_val < 40: return 1.0, "Lower neutral (bearish pressure)", "🟢"
            if rsi_val < 60: return 0.5, "Neutral/healthy", "⚪"
            if rsi_val < 70: return 0.5 + (0.5 if rsi_vel > 1.5 else 0.0), "Bullish zone - momentum building", "🟡"
            if rsi_val < 75: return (0.5, "Overbought with strong continuation momentum", "🟡") if rsi_vel > 2.5 else (
                -1.0, "Overbought - caution (likely pullback)", "🔴")
            return (-2.0, "Extremely overbought - exhaustion likely", "🔴") if rsi_vel > 4.0 else (-1.5,
                                                                                                  "Severely overbought - high reversal risk",
                                                                                                  "🔴")

        rsi_score, rsi_note, rsi_emoji = rsi_zone_score_and_note(rsi, rsi_velocity)

        # Calculate composite sentiment score
        macd_score_val = 2.0 if macd_diff > 0.3 else -2.0 if macd_diff < -0.3 else 0.0
        price_pos_score = 1.5 if price_vs_ma5 == "above" and price_vs_ma10 == "above" else -1.5 if price_vs_ma5 == "below" and price_vs_ma10 == "below" else 0.0
        ma_score = (0.5 if ma_trend == "bullish" else -0.5) if ma_spread_pct > 2 else 0.0
        volume_score = 1.0 if volume_ratio > 1.5 else -1.0 if volume_ratio < 0.5 else 0.0
        sentiment_score = rsi_score + macd_score_val + price_pos_score + ma_score + volume_score

        if sentiment_score >= 4.0:
            overall_sentiment, sentiment_emoji = "**STRONGLY BULLISH**", "🟢"
        elif sentiment_score >= 0.5:
            overall_sentiment, sentiment_emoji = "**BULLISH**", "🟡"
        elif sentiment_score <= -4.0:
            overall_sentiment, sentiment_emoji = "**STRONGLY BEARISH**", "🔴"
        elif sentiment_score <= -0.5:
            overall_sentiment, sentiment_emoji = "**MILDLY BEARISH**", "🟡"
        else:
            overall_sentiment, sentiment_emoji = "**NEUTRAL**", "⚪"

        bullish_signals = sum(1 for s in [macd_score_val, rsi_score, price_pos_score, ma_score, volume_score] if s > 0)
        bearish_signals = sum(1 for s in [macd_score_val, rsi_score, price_pos_score, ma_score, volume_score] if s < 0)
        confidence = "high" if abs(bullish_signals - bearish_signals) >= 4 and volume_ratio > 1.1 else "medium" if abs(
            bullish_signals - bearish_signals) >= 2 else "low"

        # Generate recommendation
       # Generate STRATEGIC SCENARIOS (Safe & Educational)
        conservative_stop = max(ma10, recent_low)
        support_level = fmt_price(conservative_stop)
        resistance_level = fmt_price(recent_high)
        
        if "BULLISH" in overall_sentiment and confidence == "high":
            logic = f"Technical setup shows positive momentum alignment. Analysts often observe this as a potential trend continuation toward {resistance_level}, provided the support at {support_level} holds."
        elif "BEARISH" in overall_sentiment and confidence == "high":
            logic = f"Technical setup shows bearish distribution. This pattern is frequently interpreted as a potential test of the {support_level} support zone, with {resistance_level} acting as the overhead ceiling."
        else:
            logic = f"Market is currently in a consolidation phase. Price action is oscillating between the historical boundaries of {support_level} and {resistance_level}."

        # Get latest date for historical context
        latest_date = latest.get('Date', 'N/A')
        
        return "\n".join([
            f"### ⏰ HISTORICAL TECHNICAL ANALYSIS",
            f"**{symbol}** | Data as of: **{latest_date}** | 30-Day SEBI Compliance Lag",
            "",
            f"⚠️ **This is historical data analysis only. All data is at least 31 days old per SEBI regulations.**",
            "",
            f"---",
            "",
            f"**Data-Driven Historical Sentiment:** {overall_sentiment} ({confidence} confidence)", "",
            f"**Historical Price (as of {latest_date}):** {fmt_price(close_price)}", "",
            f"#### 📊 Historical Momentum Summary (as of {latest_date})",
            f"- Historical Position vs Benchmarks: Trading **{price_vs_ma5} MA5** and **{price_vs_ma10} MA10**.",
            f"- Historical RSI ({rsi:.2f}): {rsi_note}", "",
            f"#### 🔍 Historical Market Scenarios (as of {latest_date})",
            f"{logic}", "",
            f"#### 🧠 Key Historical Structural Levels (as of {latest_date})",
            f"- **Historical Floor (Support):** {support_level}",
            f"- **Historical Ceiling (Resistance):** {resistance_level}", "",
            f"---",
            "",
            f"> ⏰ **HISTORICAL DATA ALERT:** This analysis is based on data ending **{latest_date}** with a mandatory 30+ day lag per SEBI regulations. This is NOT a current market assessment. All prices and trends reflect historical market conditions only.",
            ">",
            "> 🔒 **Disclaimer:** Fintra is a data-visualization tool. This is automated technical analysis of historical data for educational purposes only. This is NOT financial advice."
        ])
    except Exception as e:
        logger.error(f"❌ Error in rule-based analysis: {e}")
        return f"### ❌ Analysis Error\nFailed to compute analysis: {str(e)}"