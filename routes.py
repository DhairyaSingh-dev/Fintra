"""
Routes Module
Defines all Flask routes and API endpoints.
"""
import logging
import os
import os
import re
import secrets
import traceback
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import jwt
import pandas as pd
import requests
from flask import Blueprint, jsonify, make_response, redirect, request, session

# Google Auth imports for secure ID token verification
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token

from analysis import (
    call_gemini_api,
    clean_df,
    compute_macd,
    compute_rsi,
    conversation_context,
    find_recent_macd_crossover,
    generate_rule_based_analysis,
    get_gemini_ai_analysis,
    get_gemini_position_summary,
    latest_symbol_data,
)
from auth import generate_jwt_token, require_auth, set_token_cookies, verify_jwt_token
from backtesting import (
    DATA_LAG_DAYS,
    YFINANCE_AVAILABLE,
    BacktestEngine,
    batch_fetch_prices,
    check_data_availability,
    get_current_price,
    get_stock_data_with_fallback,
    load_stock_data,
)
from chatbot_validation import (
    ChatbotSafetyEnforcer,
    ConversationStateTracker,
    FrameworkValidator,
    get_conversation_state,
    validate_chat_input,
)
from config import Config
from database import db
from mc_engine import MonteCarloEngine, SimulationConfig
from models import Position, User
from validation import (
    BACKTEST_ATR_MULTIPLIER_MAX,
    BACKTEST_ATR_MULTIPLIER_MIN,
    BACKTEST_BALANCE_MAX,
    BACKTEST_BALANCE_MIN,
    BACKTEST_RISK_PER_TRADE_MAX,
    BACKTEST_RISK_PER_TRADE_MIN,
    POSITION_NOTES_MAX_LENGTH,
    POSITION_PRICE_MAX,
    POSITION_PRICE_MIN,
    POSITION_QUANTITY_MAX,
    POSITION_QUANTITY_MIN,
    create_validation_error,
    sanitize_string,
    validate_date,
    validate_date_range,
    validate_float,
    validate_int,
    validate_required_fields,
    validate_strategy,
    validate_symbol,
)

# Redis and RAG imports with explicit feature flags
try:
    from rag_engine import init_rag, rag_engine
    from redis_client import ChatCache, DataCache, RateLimiter, init_redis, redis_client
    IMPORT_OK = True
except ImportError as e:
    logging.warning(f"Redis/RAG modules not available: {e}")
    IMPORT_OK = False

# Feature flags (override availability if explicitly disabled)
REDIS_FLAG = os.getenv("ENABLE_REDIS", "true").lower()
RAG_FLAG = os.getenv("ENABLE_RAG", "true").lower()
REDIS_ENABLED = REDIS_FLAG in ("1", "true", "yes", "on")
RAG_ENABLED = RAG_FLAG in ("1", "true", "yes", "on")

# Determine actual availability
if not IMPORT_OK or not REDIS_ENABLED:
    REDIS_AVAILABLE = False
else:
    try:
        # Ping Redis to verify connectivity
        REDIS_AVAILABLE = redis_client.is_connected()
    except Exception:
        REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)

# Create Blueprint for all routes
api = Blueprint('api', __name__)

# SocketIO namespace for live replay
from flask_socketio import emit, Namespace, disconnect


@api.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify(status='ok', timestamp=datetime.now(timezone.utc).isoformat()), 200


# ==================== Admin Flags ====================
@api.route('/admin/flags', methods=['GET', 'POST'])
def admin_flags():
    """Admin endpoint to view or update feature flags at runtime."""
    from flask import request
    # Current flags (read from environment to reflect runtime defaults)
    flags = {
        'ENABLE_REDIS': os.getenv('ENABLE_REDIS', 'true'),
        'ENABLE_RAG': os.getenv('ENABLE_RAG', 'true'),
    }
    if request.method == 'GET':
        return jsonify(flags), 200
    else:
        data = request.get_json(silent=True) or {}
        updated = {}
        for key in ('ENABLE_REDIS','ENABLE_RAG'):
            if key in data:
                val = str(data[key]).lower()
                if val in ('1','true','yes','on'):
                    os.environ[key] = 'true'
                    updated[key] = 'true'
                elif val in ('0','false','no','off'):
                    os.environ[key] = 'false'
                    updated[key] = 'false'
        return jsonify({ 'updated': updated, 'current': flags }), 200


# ==================== OAUTH STATE MANAGEMENT ====================
class OAuthStateManager:
    """Manages OAuth state tokens for CSRF protection using Redis"""
    
    @staticmethod
    def store_state(state: str, ttl: int = 300):
        """Store state token in Redis with 5-minute TTL"""
        try:
            if REDIS_AVAILABLE:
                client = redis_client.get_client()
                if client:
                    key = f"oauth:state:{state}"
                    client.setex(key, ttl, "pending")
                    logger.debug(f"OAuth state stored: {state[:16]}...")
                    return True
        except Exception as e:
            logger.error(f"Failed to store OAuth state: {e}")
        return False
    
    @staticmethod
    def validate_and_clear_state(state: str) -> bool:
        """Validate state token and clear it from Redis"""
        try:
            if REDIS_AVAILABLE:
                client = redis_client.get_client()
                if client:
                    key = f"oauth:state:{state}"
                    # Get and delete in one operation
                    value = client.get(key)
                    if value:
                        client.delete(key)
                        logger.debug(f"OAuth state validated and cleared: {state[:16]}...")
                        return True
                    else:
                        logger.warning(f"OAuth state not found or expired: {state[:16]}...")
                        return False
            # If Redis is not available, we can't validate state
            logger.warning("Redis not available for OAuth state validation")
            return False
        except Exception as e:
            logger.error(f"Failed to validate OAuth state: {e}")
            return False


# ==================== PORTFOLIO HELPERS ====================
def get_user_from_token():
    """Helper to get user_id and db_user from access token."""
    access_token = request.cookies.get('access_token')

    # Fallback: Check Authorization Header if cookie is missing
    if not access_token:
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith("Bearer "):
            access_token = auth_header.split(" ")[1]

    if not access_token:
        return None, None

    payload = verify_jwt_token(access_token, Config.ACCESS_TOKEN_JWT_SECRET)
    if not payload:
        return None, None

    user_id = payload.get('user_id')
    if not user_id:
        return None, None

    db_user = User.query.filter_by(google_user_id=user_id).first()
    if not db_user:
        logger.warning(f"⚠️ Auth Debug: Token valid for user_id '{user_id}', but User not found in DB.")
    return user_id, db_user


# ==================== AUTHENTICATION ROUTES ====================
@api.route('/auth/login', methods=['GET', 'OPTIONS'])
def auth_login():
    """Initiate Google OAuth flow."""
    logger.info("📥 /auth/login endpoint called")
    try:
        state = secrets.token_urlsafe(32)
        logger.info(f"   Generated state: {state[:16]}...")
        
        # Store state in Redis for CSRF protection
        store_result = OAuthStateManager.store_state(state)
        logger.info(f"   OAuth state stored: {store_result}")
        if not store_result:
            logger.warning("⚠️ Failed to store OAuth state in Redis - continuing without state validation")
        
        logger.info(f"Generating auth URL with redirect_uri: {Config.REDIRECT_URI}")

        auth_params = {
            'client_id': Config.GOOGLE_CLIENT_ID,
            'redirect_uri': Config.REDIRECT_URI,
            'response_type': 'code',
            'scope': ' '.join(Config.SCOPES),
            'access_type': 'offline',
            'prompt': 'select_account',
            'state': state
        }
        auth_url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(auth_params)}"

        resp = jsonify(success=True, auth_url=auth_url, state_token=state)
        resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return resp, 200
    except Exception as e:
        logger.error(f"❌ OAuth initiation error: {e}")
        logger.error(traceback.format_exc())
        return jsonify(error=f"Failed to initiate OAuth: {str(e)}"), 500


@api.route('/oauth2callback', methods=['GET'])
def oauth_callback():
    """Handle OAuth callback safely with detailed logging."""
    try:
        logger.info("--- OAuth Callback Start ---")
        code = request.args.get('code')
        state = request.args.get('state')
        error = request.args.get('error')

        if error:
            logger.error(f"Google returned error: {error}")
            return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=auth_failed&reason={error}')

        if not code:
            logger.error("No 'code' parameter found in callback.")
            return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=no_code')

        # Validate state parameter to prevent CSRF attacks
        if not state:
            logger.error("No 'state' parameter found in callback.")
            return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=missing_state')
        
        # Only validate state if Redis is available
        # If Redis is down, we log a warning but allow the OAuth flow to continue
        # This is a trade-off between security and availability
        if REDIS_AVAILABLE:
            if not OAuthStateManager.validate_and_clear_state(state):
                logger.error(f"Invalid or expired state parameter: {state[:16]}...")
                return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=invalid_state')
        else:
            logger.warning("⚠️ Redis not available - skipping OAuth state validation for availability")

        logger.info("Exchanging code for tokens...")
        token_data = {
            'code': code,
            'client_id': Config.GOOGLE_CLIENT_ID,
            'client_secret': Config.GOOGLE_CLIENT_SECRET,
            'redirect_uri': Config.REDIRECT_URI,
            'grant_type': 'authorization_code'
        }
        
        try:
            token_response = requests.post("https://oauth2.googleapis.com/token", data=token_data, timeout=10)
            token_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Token exchange request failed: {e}")
            return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=token_exchange_failed&reason=network_error')

        tokens = token_response.json()
        id_token = tokens.get('id_token')
        if not id_token:
            logger.error(f"Token exchange response did not include an id_token: {tokens}")
            return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=missing_id_token')

        logger.info("Tokens received. Verifying ID token signature...")

        try:
            # Verify ID token signature using Google's public keys
            # This ensures the token was actually issued by Google and hasn't been tampered with
            user_info = google_id_token.verify_oauth2_token(
                id_token,
                google_requests.Request(),
                Config.GOOGLE_CLIENT_ID,
                clock_skew_in_seconds=10
            )
            logger.info("ID token signature verified successfully")
        except google_id_token.InvalidTokenError as e:
            logger.error(f"Invalid ID token: {e}")
            return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=invalid_id_token')
        except Exception as e:
            logger.error(f"Failed to verify ID token: {e}")
            return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=invalid_id_token')

        user_id = user_info.get('sub')
        if not user_id:
            logger.error("No 'sub' (user_id) in ID token.")
            return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=missing_user_id')
        logger.info(f"OAuth callback processing for Google User ID: {user_id}")

        # --- NEW: Sync with database ---
        # Find user in our DB or create them if they are new.
        db_user = User.query.filter_by(google_user_id=user_id).first()
        if not db_user:
            db_user = User(
                google_user_id=user_id,
                email=user_info.get('email'),
                name=user_info.get('name'),
                picture=user_info.get('picture')
            )
            db.session.add(db_user)
        else: # Update user info if it has changed
            db_user.email = user_info.get('email')
            db_user.name = user_info.get('name')
            db_user.picture = user_info.get('picture')
        db.session.commit()

        logger.info(f"User '{user_info.get('email')}' authenticated. Storing session.")
        # This user_data is only for JWT generation, not for in-memory session state.
        user_data_for_jwt = {
            'user_id': user_id,
            'email': user_info.get('email'),
            'name': user_info.get('name')
        }

        jwt_access = generate_jwt_token(user_data_for_jwt, Config.ACCESS_TOKEN_JWT_SECRET,
                                        Config.ACCESS_TOKEN_EXPIRETIME)
        jwt_refresh = generate_jwt_token(user_data_for_jwt, Config.REFRESH_TOKEN_JWT_SECRET,
                                         Config.REFRESH_TOKEN_EXPIRETIME)

        # Generate redirect URL with tokens as query params (Fallback for when cookies are blocked)
        # This allows the frontend to grab tokens from URL if Set-Cookie fails.
        redirect_params = {
            'access_token': jwt_access,
            'refresh_token': jwt_refresh
        }
        redirect_url = f"{Config.CLIENT_REDIRECT_URL}?{urlencode(redirect_params)}"

        html_content = f"""
        <!DOCTYPE html>
        <html>
            <head><title>Redirecting...</title></head>
            <body>
                <p>Authentication successful. Redirecting you to the app...</p>
                <script>window.location.href = "{redirect_url}";</script>
            </body>
        </html>
        """
        response = make_response(html_content)
        response.headers['Content-Type'] = 'text/html'
        set_token_cookies(response, jwt_access, jwt_refresh)

        logger.info("--- OAuth Callback End: Success ---")
        return response

    except Exception as e:
        logger.error(f"CRITICAL ERROR in /oauth2callback: {e}")
        logger.error(traceback.format_exc()) # Log the full traceback for debugging
        # Create a safe, user-friendly error message without newlines
        error_reason = "db_connection_failed" if "OperationalError" in str(e) else "internal_error"
        return redirect(f'{Config.CLIENT_REDIRECT_URL}?error=callback_crash&reason={error_reason}')


@api.route('/auth/token/refresh', methods=['POST'])
def refresh_token():
    """Refresh JWT access token"""
    # This endpoint is now deprecated. The `require_auth` decorator handles token refresh automatically.
    return jsonify(error="This endpoint is deprecated. Token refresh is handled by the auth middleware."), 410


@api.route('/auth/logout', methods=['POST', 'OPTIONS'])
def logout():
    """Logout user and clear session"""
    try:
        logger.info("User logout initiated.")
        response = jsonify(success=True, message="Logged out")
        response.set_cookie('access_token', '', max_age=0, path='/')
        response.set_cookie('refresh_token', '', max_age=0, path='/')
        return response, 200
    except Exception as e:
        logger.error(f"❌ Logout error: {e}")
        return jsonify(error="Logout failed"), 500


@api.route('/auth/status', methods=['GET'])
def auth_status():
    """Check authentication status with robust token handling"""
    logger.info("🔍 /auth/status called - Checking for tokens...")
    try:
        # Prioritize cookie, but fall back to Authorization header.
        # This makes the endpoint compatible with both cookie-based sessions and the URL token fallback.
        access_token = request.cookies.get('access_token')
        refresh_token = request.cookies.get('refresh_token')

        if not access_token:
            auth_header = request.headers.get('Authorization')
            if auth_header and auth_header.startswith("Bearer "):
                logger.info("Auth status: No cookie found, using 'Authorization: Bearer' header.")
                access_token = auth_header.split(" ")[1]
        
        # 1. Try Access Token
        if access_token:
            payload = verify_jwt_token(access_token, Config.ACCESS_TOKEN_JWT_SECRET)
            if payload:
                user_id = payload.get('user_id')
                if user_id:
                    db_user = User.query.filter_by(google_user_id=user_id).first()
                    if db_user:
                        expires_at = datetime.fromtimestamp(payload['exp'], tz=timezone.utc)
                        response = jsonify(
                            authenticated=True,
                            user={
                                "email": db_user.email,
                                "name": db_user.name,
                                "picture": db_user.picture,
                                "expires_in": int((expires_at - datetime.now(timezone.utc)).total_seconds())
                            }
                        )
                        # Prevent caching of auth status
                        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
                        return response, 200
                    else:
                        logger.warning(f"Auth status: Valid access token for user_id {user_id}, but user not found in DB.")
                else:
                    logger.warning("Auth status: Access token payload missing user_id.")
        
        # 2. Fallback: Try Refresh Token (if access token missing or invalid)
        if refresh_token:
            payload = verify_jwt_token(refresh_token, Config.REFRESH_TOKEN_JWT_SECRET)
            if payload:
                user_id = payload.get('user_id')
                if user_id:
                    db_user = User.query.filter_by(google_user_id=user_id).first()
                    if db_user:
                        # Generate new access token
                        user_data = {'user_id': db_user.google_user_id, 'email': db_user.email, 'name': db_user.name}
                        new_access_token = generate_jwt_token(user_data, Config.ACCESS_TOKEN_JWT_SECRET, Config.ACCESS_TOKEN_EXPIRETIME)
                        
                        # Return authenticated with new cookie
                        response = jsonify(
                            authenticated=True,
                            user={
                                "email": db_user.email,
                                "name": db_user.name,
                                "picture": db_user.picture,
                                "expires_in": Config.parse_time_to_seconds(Config.ACCESS_TOKEN_EXPIRETIME)
                            }
                        )
                        set_token_cookies(response, new_access_token, refresh_token)
                        logger.info(f"🔄 Auth status recovered session via refresh token for {db_user.email}")
                        # Prevent caching
                        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
                        return response, 200
                    else:
                        logger.warning(f"Auth status: Valid refresh token for user_id {user_id}, but user not found in DB.")
                else:
                    logger.warning("Auth status: Refresh token payload missing user_id.")

        logger.info(f"Auth status check failed. Access cookie present: {bool(access_token)}, Refresh cookie present: {bool(refresh_token)}")
        response = jsonify(authenticated=False)
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        return response, 200
    except Exception as e:
        logger.error(f"❌ Auth status error: {e}")
        logger.error(traceback.format_exc())
        return jsonify(authenticated=False), 200

@api.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    # Check local data availability
    try:
        data_status = check_data_availability()
        local_data_available = data_status.get('available', False)
        data_freshness = data_status.get('data_freshness_days', 'unknown')
    except Exception as e:
        local_data_available = False
        data_freshness = f"error: {str(e)}"
    
    return jsonify(
        status="healthy",
        services={
            "local_data": "available" if local_data_available else "unavailable",
            "data_freshness_days": data_freshness,
            "yfinance": "fallback_available",
            "rule_based_analysis": "operational",
            "oauth_authentication": "enabled"
        },
        data_source_priority="local_first",
        version="5.1-LocalPriority",
        env="production"
    ), 200


@api.route('/ping')
def ping():
    return "ok", 200


@api.route('/data/availability', methods=['GET'])
def get_data_availability():
    """
    Get data availability information including SEBI compliance lag status.
    Returns information about available data range and compliance.
    """
    try:
        availability = check_data_availability()
        return jsonify(availability), 200
    except Exception as e:
        logger.error(f"Error getting data availability: {e}")
        return jsonify({
            'available': False,
            'error': str(e),
            'lag_days': DATA_LAG_DAYS
        }), 500


@api.route('/stock/<symbol>/date_range', methods=['GET'])
def get_stock_date_range(symbol):
    """
    Get available date range for a specific stock symbol.
    Returns first and last available dates from parquet data.
    """
    try:
        if not symbol:
            return jsonify(error="Symbol is required"), 400
        
        symbol = symbol.upper().strip()
        df, compliance_info = load_stock_data(symbol, apply_lag=True)
        
        if df is None:
            return jsonify(error=f"Data not found for symbol {symbol}"), 404
        
        return jsonify({
            'symbol': symbol,
            'first_date': df.index.min().strftime('%Y-%m-%d'),
            'last_date': df.index.max().strftime('%Y-%m-%d'),
            'total_days': len(df),
            'lag_days': DATA_LAG_DAYS,
            'lag_date': (datetime.now() - timedelta(days=DATA_LAG_DAYS)).strftime('%Y-%m-%d')
        }), 200
        
    except Exception as e:
        logger.error(f"Error getting date range for {symbol}: {e}")
        return jsonify(error=str(e)), 500


# ==================== DATA & ANALYSIS ROUTES ====================
@api.route('/get_data', methods=['POST'])
def get_data():
    """Fetch and analyze stock data"""
    auth_response = require_auth()
    if auth_response:
        return auth_response
    logger.info("Received request for /api/get_data") # This log is now reachable
    data = request.get_json()
    if not data:
        return jsonify(error="Request body is required"), 400
    
    # Validate symbol against whitelist
    symbol = data.get('symbol', '').strip()
    is_valid, error_msg = validate_symbol(symbol)
    if not is_valid:
        return jsonify(error=error_msg), 400
    
    user_id, _ = get_user_from_token()
    
    symbol = symbol.upper()

    try:
        # Use unified data fetching with local data priority
        hist, metadata = get_stock_data_with_fallback(
            symbol,
            period="90d",
            interval="1d",
            apply_lag=True,
            min_rows=30
        )

        if hist is None or hist.empty:
            # Provide detailed error message about fallback attempts
            error_details = []
            if metadata:
                if metadata.get('local_available'):
                    error_details.append("local data insufficient")
                else:
                    error_details.append("no local data")
                
                if metadata.get('yfinance_available'):
                    error_details.append("yfinance fallback attempted but failed")
                elif not YFINANCE_AVAILABLE:
                    error_details.append("yfinance not available")
                else:
                    error_details.append("yfinance fallback attempted but no data returned")
                
                if metadata.get('error'):
                    error_details.append(f"error: {metadata.get('error')}")
            
            error_msg = f"Could not retrieve data for {symbol}. " + "; ".join(error_details) if error_details else f"No data available for {symbol}"
            return jsonify(error=error_msg), 404
        
        # Log data source for debugging
        logger.info(f"Data loaded for {symbol}: source={metadata.get('source')}, "
                   f"yfinance_fallback={metadata.get('yfinance_fallback', False)}, "
                   f"rows={len(hist)}, cached={metadata.get('data_completeness', {}).get('cached', False)}")

        # Note: Column names are already standardized to PascalCase in load_stock_data
        # hist.columns = [col.title().replace('_', '') for col in hist.columns]
        
        # Check if we have enough data to calculate indicators
        # RSI requires 14 days minimum, so we need at least 14 rows
        if len(hist) < 14:
            return jsonify(
                error=f"Insufficient data for {symbol}. Found {len(hist)} rows, need at least 14 for technical indicators. "
                      f"Data source: {metadata.get('source', 'unknown')}. "
                      f"Please try again later or contact support if the issue persists."
            ), 422
        
        hist['Ma5'] = hist['Close'].rolling(window=5).mean()
        hist['Ma10'] = hist['Close'].rolling(window=10).mean()
        hist['Rsi'] = compute_rsi(hist['Close'])
        hist['Macd'], hist['Signal'], hist['Histogram'] = compute_macd(hist['Close'])

        # For AI analysis, use last 30 days of data that has all indicators calculated
        # (RSI needs 14 days, so we need at least 14 days of history)
        hist_with_indicators = hist.dropna(subset=['Ma5', 'Ma10', 'Rsi', 'Macd', 'Signal', 'Histogram'])
        
        # Check if we have any data with indicators after dropping NaN
        if hist_with_indicators.empty:
            logger.warning(f"No valid indicator data for {symbol} after calculating. Data has {len(hist)} rows but indicators are all NaN.")
            return jsonify(
                error=f"Could not calculate technical indicators for {symbol}. The data may be insufficient or corrupted. "
                      f"Data source: {metadata.get('source', 'unknown')}, rows: {len(hist)}. "
                      f"Please try a different symbol or contact support."
            ), 422
        
        latest_data_list = clean_df(hist_with_indicators.tail(30),
                                    ['Open', 'High', 'Low', 'Close', 'Volume', 'Ma5', 'Ma10', 'Rsi', 'Macd', 'Signal',
                                     'Histogram'])

        latest_symbol_data[symbol] = latest_data_list

        rule_based_text = generate_rule_based_analysis(symbol, latest_data_list)
        gemini_analysis = get_gemini_ai_analysis(symbol, latest_data_list)

        if user_id not in conversation_context:
            conversation_context[user_id] = {
                "current_symbol": symbol,
                "conversation_history": [],
                "last_active": datetime.now(timezone.utc).isoformat(),
                "user_positions": {}
            }
        else:
            conversation_context[user_id]["current_symbol"] = symbol
            conversation_context[user_id]["last_active"] = datetime.now(timezone.utc).isoformat()

        # Prepare compliance information
        lag_date = datetime.now() - timedelta(days=DATA_LAG_DAYS)
        effective_date = hist.index.max().strftime('%Y-%m-%d') if not hist.empty else None
        
        # For display tables, use data that has the specific indicators available
        # Don't require ALL indicators to be present - just the ones needed for each table
        hist_ohlcv = hist.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'])
        hist_ma = hist.dropna(subset=['Ma5', 'Ma10'])
        hist_rsi = hist.dropna(subset=['Rsi'])
        hist_macd = hist.dropna(subset=['Macd', 'Signal', 'Histogram'])
        
        return jsonify(
            ticker=symbol,
            OHLCV=clean_df(hist_ohlcv, ['Open', 'High', 'Low', 'Close', 'Volume']),
            MA=clean_df(hist_ma, ['Ma5', 'Ma10']),
            RSI=clean_df(hist_rsi, ['Rsi']),
            MACD=clean_df(hist_macd, ['Macd', 'Signal', 'Histogram']),
            AI_Review=gemini_analysis,
            Rule_Based_Analysis=rule_based_text,
            data_source={
                'primary': metadata.get('source', 'unknown'),
                'yfinance_fallback': metadata.get('yfinance_fallback', False),
                'local_available': metadata.get('local_available', False),
                'yfinance_available': metadata.get('yfinance_available', False)
            },
            sebi_compliance={
                'data_lag_days': DATA_LAG_DAYS,
                'effective_last_date': effective_date,
                'lag_date': lag_date.strftime('%Y-%m-%d'),
                'compliance_notice': f"This analysis uses historical data with a mandatory {DATA_LAG_DAYS}-day lag in accordance with SEBI regulations. No current market data is included."
            }
        ), 200
    except Exception as e:
        logger.error(f"❌ Error in /api/get_data: {e}")
        return jsonify(error=f"Server error: {str(e)}"), 500


@api.route('/chat', methods=['POST', 'OPTIONS'])
def chat():
    """
    Enhanced chatbot endpoint with framework validation, safety enforcement,
    conversation memory management, and educational value protection.
    """
    if request.method == 'OPTIONS':
        return jsonify(success=True), 200

    auth_response = require_auth()
    if auth_response:
        return auth_response

    # Get user ID from token for conversation tracking
    _, db_user = get_user_from_token()
    if db_user:
        user_id = str(db_user.id)
    else:
        # Fallback to access token for anonymous users
        user_id = request.cookies.get('access_token', 'anonymous')
    
    # Rate limiting check (30 requests per minute)
    if REDIS_AVAILABLE and RateLimiter.is_allowed(user_id, 'chat', max_requests=30):
        remaining = RateLimiter.get_remaining(user_id, 'chat', max_requests=30)
    else:
        return jsonify(
            error="Rate limit exceeded. Please wait a moment before sending more messages.",
            retry_after=60
        ), 429

    data = request.get_json()
    if not data:
        return jsonify(error="No data provided"), 400

    # Validate and sanitize query (XSS protection)
    raw_query = data.get('query', '')
    query, error_msg = sanitize_string(raw_query, max_length=500, allow_html=False)
    if error_msg:
        return jsonify(error=f"Invalid query: {error_msg}"), 400
    
    if not query:
        return jsonify(error="No query provided"), 400

    # Get context mode and related data
    mode = data.get('mode', 'none')  # 'none', 'market', or 'portfolio'
    symbol = data.get('symbol')  # For market mode
    selected_position_id = data.get('position_id')  # For portfolio mode

    # Build cache key context
    cache_context = {
        "mode": mode,
        "symbol": symbol,
        "position_id": selected_position_id
    }

    try:
        # ============================================
        # STEP 1: FRAMEWORK VALIDATION & SAFETY CHECKS
        # ============================================
        
        # Validate chat input for framework correctness and safety
        is_valid, processed_query, validation_metadata = validate_chat_input(query, mode, user_id)
        
        if not is_valid:
            # Input was blocked by pre-validation
            logger.warning(f"Chat input blocked for user {user_id}: {validation_metadata}")
            return jsonify(
                response=processed_query,  # This contains the error message
                context={"mode": mode, "blocked": True},
                validation={"blocked": True, "reason": validation_metadata.get("reason", "validation_failed")},
                rate_limit_remaining=remaining
            ), 200
        
        # Check for suspicious mode transition
        conv_state = get_conversation_state(user_id)
        is_transition_suspicious, transition_msg = conv_state.is_transition_suspicious(mode)
        mode_warning = ""
        if is_transition_suspicious:
            mode_warning = f"\n\n⚠️ **Mode Transition Warning:** {transition_msg}"
            logger.warning(f"Suspicious mode transition for user {user_id}: {transition_msg}")
        
        # ============================================
        # STEP 2: CHECK CACHE
        # ============================================
        
        if REDIS_AVAILABLE:
            cached_response = ChatCache.get(query, cache_context)
            if cached_response:
                logger.debug("Chat cache hit - returning cached response")
                return jsonify(
                    response=cached_response + mode_warning if mode_warning else cached_response,
                    context=cache_context,
                    cached=True,
                    rate_limit_remaining=remaining,
                    validation={"framework_validated": True}
                ), 200

        # ============================================
        # STEP 3: LOAD & ENHANCE SYSTEM PROMPT
        # ============================================
        
        import os
        BASE_DIR = os.path.dirname(os.path.dirname(__file__))
        system_prompt_path = os.path.join(BASE_DIR, 'system_prompt.txt')
        
        try:
            with open(system_prompt_path, 'r') as f:
                base_system_prompt = f.read().strip()
        except Exception as e:
            logger.warning(f"Could not load system prompt: {e}")
            base_system_prompt = "You are Fintra, a friendly AI assistant helping users learn about stock market technical analysis. Be conversational and educational."
        
        # Enhance system prompt with safety rules
        system_prompt = ChatbotSafetyEnforcer.build_enhanced_system_prompt(base_system_prompt, conv_state)

        # ============================================
        # STEP 4: BUILD CONTEXT BASED ON MODE
        # ============================================
        
        context_str = ""
        current_context = {
            "mode": mode,
            "framework_validated": True,
            "suspicious_score": validation_metadata.get("suspicious_score", 0)
        }

        if mode == 'market' and symbol:
            context_str = f"\n[MARKET CONTEXT]: The user is asking about {symbol}. This is historical data (31-day lag per SEBI). Current stock: {symbol}"
            current_context["symbol"] = symbol
            
        elif mode == 'portfolio' and selected_position_id:
            if db_user:
                position = Position.query.filter_by(id=selected_position_id, user_id=db_user.id).first()
                if position:
                    context_str = f"\n[PORTFOLIO CONTEXT]: The user has a position in {position.symbol} - {position.quantity} shares at entry price ₹{position.entry_price}. This is historical performance data (31-day lag per SEBI)."
                    current_context["position"] = {
                        "symbol": position.symbol,
                        "quantity": position.quantity,
                        "entry_price": float(position.entry_price)
                    }
        else:
            context_str = "\n[GENERAL CHAT MODE]: Educational discussion only. No specific stock context."
            current_context["mode"] = "educational"

        # ============================================
        # STEP 5: RAG KNOWLEDGE RETRIEVAL
        # ============================================
        
        rag_context = ""
        sources = []
        if REDIS_AVAILABLE and rag_engine.model:
            try:
                retrieved_docs = rag_engine.search(query, top_k=3)
                if retrieved_docs:
                    rag_context = rag_engine.assemble_context(query, retrieved_docs)
                    sources = [doc['title'] for doc in retrieved_docs]
                    logger.debug(f"RAG retrieved {len(retrieved_docs)} documents")
            except Exception as e:
                logger.warning(f"RAG retrieval failed: {e}")

        # ============================================
        # STEP 6: BUILD ENHANCED PROMPT
        # ============================================
        
        safe_query = processed_query[:500]  # Use validated query
        
        # Check if this is a correction scenario
        if validation_metadata.get("correction_needed"):
            # Use the correction prompt built by the validator
            full_prompt = safe_query  # The validator already built the full correction prompt
            current_context["framework_correction"] = True
            current_context["corrected_framework"] = validation_metadata.get("framework", {}).get("framework", "")
        else:
            # Standard prompt construction
            if rag_context:
                full_prompt = f"{system_prompt}\n\n{rag_context}\n{context_str}\n\nUser: {safe_query}\n\nUse the knowledge base information above to provide an accurate, educational response that corrects any misconceptions."
            elif mode == 'none':
                full_prompt = f"{system_prompt}\n\nUser: {safe_query}\n\nProvide an educational response. If the user has any misconceptions about technical analysis, politely correct them with the proper definitions."
            else:
                full_prompt = f"{system_prompt}{context_str}\n\nUser: {safe_query}\n\nAnalyze based on the context provided. Correct any wrong frameworks or definitions."
        
        full_prompt = full_prompt.replace('\r', ' ')

        # ============================================
        # STEP 7: CALL API WITH VALIDATION
        # ============================================
        
        assistant_response = call_gemini_api(full_prompt)
        
        if not assistant_response:
            assistant_response = "I'm sorry, I couldn't generate a response. Please try asking in a different way."
        elif len(assistant_response) > 1000:
            assistant_response = assistant_response[:997] + "..."
        
        # Add mode transition warning if present
        if mode_warning:
            assistant_response = mode_warning + "\n\n" + assistant_response

        # ============================================
        # STEP 8: POST-PROCESS & CACHE
        # ============================================
        
        # Only cache if no personal/sensitive data
        if REDIS_AVAILABLE and not validation_metadata.get("correction_needed"):
            ChatCache.set(query, cache_context, assistant_response)

        # Build validation info for response
        validation_info = {
            "framework_validated": True,
            "suspicious_score": validation_metadata.get("suspicious_score", 0),
            "corrections_made": validation_metadata.get("corrections_made", 0)
        }
        
        if validation_metadata.get("correction_needed"):
            validation_info["correction_applied"] = True
            validation_info["corrected_framework"] = validation_metadata.get("framework", {}).get("framework", "")
        
        if validation_metadata.get("mode_transition_warning"):
            validation_info["mode_transition_warning"] = True

        return jsonify(
            response=assistant_response,
            context=current_context,
            sources=sources if sources else None,
            rate_limit_remaining=remaining,
            cached=False,
            validation=validation_info
        ), 200

    except Exception as e:
        logger.error(f"Chat error: {e}")
        logger.error(traceback.format_exc())
        return jsonify(error="Unable to process request"), 500


@api.route('/chat/reset', methods=['POST'])
def reset_chat_context():
    """
    Reset conversation context and memory.
    Useful after completing educational exercises or when switching topics.
    """
    auth_response = require_auth()
    if auth_response:
        return auth_response

    try:
        # Get user ID
        _, db_user = get_user_from_token()
        if db_user:
            user_id = str(db_user.id)
        else:
            user_id = request.cookies.get('access_token', 'anonymous')
        
        # Clear conversation state
        from chatbot_validation import clear_conversation_state
        clear_conversation_state(user_id)
        
        logger.info(f"Chat context reset for user {user_id}")
        
        return jsonify(
            success=True,
            message="Conversation context has been reset. You can start fresh now.",
            timestamp=datetime.now(timezone.utc).isoformat()
        ), 200
        
    except Exception as e:
        logger.error(f"Error resetting chat context: {e}")
        return jsonify(error="Failed to reset context"), 500


@api.route('/chat/validation-status', methods=['GET'])
def get_chat_validation_status():
    """
    Get current conversation validation status and suspicious activity score.
    Useful for debugging and monitoring.
    """
    auth_response = require_auth()
    if auth_response:
        return auth_response

    try:
        # Get user ID
        _, db_user = get_user_from_token()
        if db_user:
            user_id = str(db_user.id)
        else:
            user_id = request.cookies.get('access_token', 'anonymous')
        
        # Get conversation state
        from chatbot_validation import get_conversation_state
        conv_state = get_conversation_state(user_id)
        
        return jsonify(
            user_id=user_id[:8] + "..." if len(user_id) > 8 else user_id,
            suspicious_score=conv_state.suspicious_score,
            corrections_made=conv_state.correction_count,
            last_correction=conv_state.last_correction,
            mode_history_count=len(conv_state.mode_history),
            strict_mode_active=conv_state.should_enforce_strict_mode(),
            user_frameworks_introduced=conv_state.user_frameworks_introduced
        ), 200
        
    except Exception as e:
        logger.error(f"Error getting validation status: {e}")
        return jsonify(error="Failed to get status"), 500


@api.route('/price/<symbol>', methods=['GET'])
def get_current_price_endpoint(symbol):
    """Get current price for a symbol using local data with yfinance fallback."""
    if not symbol:
        return jsonify(error="Symbol required"), 400
    
    try:
        # Use unified data fetching with local priority
        price = get_current_price(symbol)
        
        if price is not None:
            return jsonify(price=price, source="local"), 200
        
        return jsonify(error="Price not found"), 404
    except Exception as e:
        logger.error(f"Price fetch error: {e}")
        if "Too Many Requests" in str(e) or "Rate limited" in str(e):
            return jsonify(error="Rate limit exceeded. Please try again later."), 429
        return jsonify(error="Server error"), 500


# ==================== PORTFOLIO ROUTES ====================
@api.route('/portfolio', methods=['GET'])
def get_portfolio():
    """Fetch all positions for the current user."""
    auth_response = require_auth()
    if auth_response:
        return auth_response

    _, db_user = get_user_from_token()
    if not db_user:
        return jsonify(error="Database user not found for this session."), 401

    try:
        positions = Position.query.filter_by(user_id=db_user.id).order_by(Position.symbol).all()
        
        if not positions:
            return jsonify([]), 200

        # Batch fetch current prices using local data with yfinance fallback
        symbols = [p.symbol for p in positions]
        
        # Use batch_fetch_prices for unified data fetching
        symbols_data = batch_fetch_prices(symbols, period="60d")
        
        portfolio_data = []
        for p in positions:
            latest_date = None  # Initialize for transparency
            data_source = "unknown"
            try:
                # Get data from batch fetch
                hist = symbols_data.get(p.symbol)
                
                if hist is None or hist.empty:
                    raise ValueError(f"No valid history for {p.symbol}")
                
                # Track data source
                data_source = "local" if hist is not None else "unavailable"
                
                # Note: Column names are already standardized to PascalCase in load_stock_data
                # hist.columns = [col.title().replace('_', '') for col in hist.columns]
                
                # Validate and Clean
                if 'Close' not in hist.columns:
                    raise ValueError(f"No 'Close' column in data for {p.symbol}")
                
                hist = hist.copy()
                hist = hist.dropna(subset=['Close'])
                
                if hist.empty:
                    raise ValueError(f"History is empty after dropping NaNs for {p.symbol}")

                # Calculate indicators
                hist['Rsi'] = compute_rsi(hist['Close'])
                hist['Ma5'] = hist['Close'].rolling(window=5).mean()
                hist['Ma10'] = hist['Close'].rolling(window=10).mean()
                hist['Macd'], hist['Signal'], hist['Histogram'] = compute_macd(hist['Close'])
                
                latest = hist.iloc[-1]
                current_price = latest['Close']
                
                hist_list_for_macd = clean_df(hist.dropna(subset=['Macd', 'Signal']), ['Macd', 'Signal'])
                crossover_type, crossover_days_ago = find_recent_macd_crossover(hist_list_for_macd, lookback=7)
                macd_status = "None"
                if crossover_type != 'none':
                    macd_status = f"{crossover_type.capitalize()} {crossover_days_ago}d ago"
                
                current_value = p.quantity * current_price
                entry_value = p.quantity * p.entry_price
                pnl = current_value - entry_value
                pnl_percent = (pnl / entry_value) * 100 if entry_value != 0 else 0

                # Reset index to make Date a column (clean_df expects 'Date' column)
                chart_df = hist.tail(30).reset_index()
                chart_df.columns = [col.title() if col.lower() == 'date' else col for col in chart_df.columns]
                chart_data = clean_df(chart_df, ['Close'])
                
                # Get the latest date for transparency
                latest_date = latest.name.strftime('%Y-%m-%d') if hasattr(latest.name, 'strftime') else str(latest.name)

                position_payload = {
                    "id": p.id,
                    "symbol": p.symbol,
                    "quantity": p.quantity,
                    "entry_price": p.entry_price,
                    "entry_date": p.entry_date.strftime('%Y-%m-%d'),
                    "notes": p.notes,
                    "current_price": current_price,
                    "current_value": current_value,
                    "pnl": pnl,
                    "pnl_percent": pnl_percent,
                    "rsi": latest.get('Rsi'),
                    "ma5": latest.get('Ma5'),
                    "ma10": latest.get('Ma10'),
                    "macd_status": macd_status,
                    "chart_data": chart_data,
                    "data_date": latest_date,  # Include the date of the data for transparency
                    "data_source": data_source  # Track where data came from
                }

                portfolio_data.append(position_payload)

            except Exception as e:
                logger.error(f"❌ CRITICAL ERROR processing {p.symbol}: {str(e)}")
                logger.error(traceback.format_exc())
                # Append with data we have, even if live price fails
                portfolio_data.append({
                    "id": p.id, "symbol": p.symbol, "quantity": p.quantity,
                    "entry_price": p.entry_price, "entry_date": p.entry_date.strftime('%Y-%m-%d'),
                    "notes": p.notes, "current_price": p.entry_price, "current_value": p.quantity * p.entry_price,
                    "pnl": 0, "pnl_percent": 0,
                    "rsi": None, "ma5": None, "ma10": None, "macd_status": "N/A", "chart_data": [],
                    "data_date": None,
                    "data_source": data_source
                })

        return jsonify(portfolio_data), 200
    except Exception as e:
        logger.error(f"❌ Error fetching portfolio: {e}")
        return jsonify(error="Failed to fetch portfolio data."), 500


@api.route('/portfolio/positions/list', methods=['GET'])
def get_portfolio_positions_list():
    """Get simplified list of portfolio positions for chat selection."""
    auth_response = require_auth()
    if auth_response:
        return auth_response

    _, db_user = get_user_from_token()
    if not db_user:
        return jsonify(error="Database user not found."), 401

    try:
        positions = Position.query.filter_by(user_id=db_user.id).order_by(Position.symbol).all()
        
        positions_list = []
        for p in positions:
            positions_list.append({
                "id": p.id,
                "symbol": p.symbol,
                "quantity": p.quantity,
                "entry_price": p.entry_price,
                "entry_date": p.entry_date.strftime('%Y-%m-%d')
            })

        return jsonify(positions_list), 200
    except Exception as e:
        logger.error(f"❌ Error fetching positions list: {e}")
        return jsonify(error="Failed to fetch positions."), 500


@api.route('/positions', methods=['POST'])
def add_position():
    """Add a new position to the user's portfolio with comprehensive validation."""
    auth_response = require_auth()
    if auth_response: return auth_response

    _, db_user = get_user_from_token()
    if not db_user: return jsonify(error="Database user not found."), 401

    data = request.get_json()
    if not data:
        return jsonify(error="Request body is required"), 400
    
    # Validate required fields
    is_valid, error_msg = validate_required_fields(data, ['symbol', 'quantity', 'entry_price'])
    if not is_valid:
        return jsonify(create_validation_error([error_msg])), 400

    validation_errors = []
    
    # Validate symbol against whitelist
    symbol = data.get('symbol', '').strip()
    is_valid, error_msg = validate_symbol(symbol)
    if not is_valid:
        validation_errors.append(error_msg)
    
    # Validate quantity (0 to 100,000)
    quantity, error_msg = validate_float(
        data.get('quantity'), 'Quantity',
        min_val=POSITION_QUANTITY_MIN, max_val=POSITION_QUANTITY_MAX
    )
    if error_msg:
        validation_errors.append(error_msg)
    
    # Validate entry_price (0 to 1,000,000)
    entry_price, error_msg = validate_float(
        data.get('entry_price'), 'Entry price',
        min_val=POSITION_PRICE_MIN, max_val=POSITION_PRICE_MAX
    )
    if error_msg:
        validation_errors.append(error_msg)
    
    # Validate and sanitize notes (max 500 chars, strip HTML, XSS protection)
    notes = data.get('notes', '')
    sanitized_notes, error_msg = sanitize_string(notes, max_length=POSITION_NOTES_MAX_LENGTH, allow_html=False)
    if error_msg:
        validation_errors.append(f"Notes: {error_msg}")
    
    # Validate entry_date (YYYY-MM-DD, cannot be in future)
    entry_date_str = data.get('entry_date')
    entry_date = None
    if entry_date_str:
        entry_date_val, error_msg = validate_date(entry_date_str, 'Entry date', allow_future=False)
        if error_msg:
            validation_errors.append(error_msg)
        else:
            entry_date = entry_date_val.date()
    else:
        entry_date = datetime.now(timezone.utc).date()
    
    # Return all validation errors if any
    if validation_errors:
        logger.warning(f"Position validation failed: {validation_errors}")
        return jsonify(create_validation_error(validation_errors)), 400

    try:
        new_position = Position(
            symbol=symbol.upper(),
            quantity=quantity,
            entry_price=entry_price,
            entry_date=entry_date,
            notes=sanitized_notes,
            user_id=db_user.id
        )
        db.session.add(new_position)
        db.session.commit()
        logger.info(f"✅ Position added: {symbol} x {quantity} for user {db_user.id}")
        return jsonify(
            id=new_position.id,
            message="Position added successfully",
            symbol=symbol.upper(),
            quantity=quantity,
            entry_price=entry_price
        ), 201
    except Exception as e:
        logger.error(f"❌ Error adding position: {e}")
        db.session.rollback()
        return jsonify(error="Failed to add position due to database error"), 500


@api.route('/positions/<int:position_id>', methods=['DELETE'])
def delete_position(position_id):
    """Delete a position."""
    auth_response = require_auth()
    if auth_response: return auth_response
    
    _, db_user = get_user_from_token()
    if not db_user: return jsonify(error="Database user not found."), 401

    position = Position.query.get_or_404(position_id)
    if position.user_id != db_user.id:
        return jsonify(error="Forbidden: You do not own this position."), 403

    db.session.delete(position)
    db.session.commit()
    return jsonify(message="Position deleted successfully"), 200

@api.route('/backtest', methods=['POST'])
def run_backtest():
    """
    Run backtest strategy on historical data with comprehensive input validation.
    """
    auth_response = require_auth()
    if auth_response:
        return auth_response

    data = request.get_json()
    if not data:
        return jsonify(error="Request body is required"), 400

    validation_errors = []
    
    # Validate symbol against whitelist
    symbol = data.get('symbol', '').strip()
    is_valid, error_msg = validate_symbol(symbol)
    if not is_valid:
        validation_errors.append(error_msg)
    
    # Validate strategy
    strategy = data.get('strategy', 'composite')
    is_valid, error_msg = validate_strategy(strategy)
    if not is_valid:
        validation_errors.append(error_msg)
    
    # Validate initial_balance (1,000 to 10,000,000)
    initial_balance, error_msg = validate_float(
        data.get('initial_balance', 100000), 'Initial balance',
        min_val=BACKTEST_BALANCE_MIN, max_val=BACKTEST_BALANCE_MAX
    )
    if error_msg:
        validation_errors.append(error_msg)
    
    # Validate atr_multiplier (0.5 to 20)
    atr_multiplier, error_msg = validate_float(
        data.get('atr_multiplier', 3.0), 'ATR multiplier',
        min_val=BACKTEST_ATR_MULTIPLIER_MIN, max_val=BACKTEST_ATR_MULTIPLIER_MAX
    )
    if error_msg:
        validation_errors.append(error_msg)
    
    # Validate risk_per_trade (0.001 to 0.5)
    risk_per_trade, error_msg = validate_float(
        data.get('risk_per_trade', 0.02), 'Risk per trade',
        min_val=BACKTEST_RISK_PER_TRADE_MIN, max_val=BACKTEST_RISK_PER_TRADE_MAX
    )
    if error_msg:
        validation_errors.append(error_msg)
    
    # Get date strings for validation
    start_date = data.get('start_date')
    end_date = data.get('end_date')
    
    # Validate date range (not in the future, start <= end)
    if start_date or end_date:
        if start_date and end_date:
            is_valid, error_msg = validate_date_range(start_date, end_date)
            if not is_valid:
                validation_errors.append(error_msg)
        elif start_date:
            _, error_msg = validate_date(start_date, 'Start date', allow_future=False)
            if error_msg:
                validation_errors.append(error_msg)
        elif end_date:
            _, error_msg = validate_date(end_date, 'End date', allow_future=False)
            if error_msg:
                validation_errors.append(error_msg)
    
    # Return all validation errors if any
    if validation_errors:
        logger.warning(f"Backtest validation failed: {validation_errors}")
        return jsonify(create_validation_error(validation_errors)), 400

    try:
        df, compliance_info = load_stock_data(symbol, apply_lag=True)
        if df is None:
            return jsonify(error=f"Data not found for symbol {symbol}. Please check if it's a valid Indian stock."), 404
        
        # Get available date range from the data
        available_first_date = df.index.min().strftime('%Y-%m-%d')
        available_last_date = df.index.max().strftime('%Y-%m-%d')
        
        # Validate dates
        if start_date and start_date < available_first_date:
            return jsonify(error=f"Start date {start_date} is before available data start ({available_first_date})"), 400
        if end_date and end_date > available_last_date:
            return jsonify(error=f"End date {end_date} is after available data end ({available_last_date})"), 400
        if start_date and end_date and start_date > end_date:
            return jsonify(error="Start date cannot be after end date"), 400

        engine = BacktestEngine(df)
        result_df = engine.run_strategy(strategy)

        performance = engine.get_performance_summary(
            initial_capital=initial_balance,
            is_long_only=True,
            start_date=start_date,
            end_date=end_date,
            atr_multiplier=atr_multiplier,
            tax_rate=0.002
        )

        # Add AI analysis using existing Gemini integration
        if not performance.get("error"):
            trades_df = performance.get('trades_df', pd.DataFrame())
            if not trades_df.empty:
                if 'result' not in trades_df.columns:
                    trades_df['result'] = trades_df.get('result', pd.Series(['Loss'] * len(trades_df)))

                performance_summary = f"""
                Backtest Results Summary for {symbol}:
                - Strategy: {strategy}
                - Period: {start_date} to {end_date}
                - Initial Capital: ₹{initial_balance:,.2f}
                - Final Value: ₹{performance['final_portfolio_value']:,.2f}
                - Total Return: {performance['strategy_return_pct']:.2f}%
                - Buy & Hold Return: {performance['market_return_pct']:.2f}%
                - Sharpe Ratio: {performance['sharpe_ratio']:.2f}
                - Max Drawdown: {performance['max_drawdown_pct']:.2f}%
                - Total Trades: {len(trades_df)}
                - Win Rate: {(len(trades_df[trades_df['result'] == 'Win']) / len(trades_df) * 100):.1f}%

                Trade Details:
                {trades_df[['entry_date', 'exit_date', 'entry_price', 'exit_price', 'pnl_pct', 'result', 'reason']].to_string(index=False)}
                """

                ai_prompt = f"""
You are the **Fintra Historical Strategy Analysis Engine**. Your role is to provide a neutral, 
quantitative decomposition of HISTORICAL backtest data. This is pure historical analysis, not current market assessment.

**⚠️ CRITICAL CONTEXT: HISTORICAL BACKTEST ONLY ⚠️**
- This is a backtest of historical data from {start_date} to {end_date}
- Data includes a mandatory 31-day SEBI compliance lag
- This analysis examines what happened in the past, not what to do now
- All performance metrics are hypothetical historical simulations

### HISTORICAL INPUT DATA FOR {symbol} (Period: {start_date} to {end_date}):
{performance_summary}

### OBJECTIVES - HISTORICAL ANALYSIS ONLY:
1. **📊 Historical Statistical Performance:** Compare the Strategy Final Value against the Buy & Hold benchmark during the historical period {start_date} to {end_date}. State the historical delta objectively using past tense.
2. **📉 Historical Risk Attribution:** Describe the historical relationship between the Max Drawdown and Sharpe Ratio. (e.g., "During the backtest period from {start_date} to {end_date}, the strategy experienced a drawdown of X while maintaining a reward-to-risk metric of Y").
3. **🔍 Historical Variable Sensitivity:** Identify which historical parameters (like Exit Reasons or Stop Loss frequency) most heavily influenced the total historical P&L during this period. 
4. **📅 Historical Market Regime Context:** Note how the strategy performed during specific historical market conditions (high-volatility vs. low-volatility periods) found in the data from {start_date} to {end_date}.
5. **🧩 Historical Edge Case Analysis:** Identify the single largest historical win and loss during {start_date} to {end_date}; describe the technical conditions of those specific historical events.

### MANDATORY CONSTRAINTS:
- **⏰ TIME CONTEXT:** ALWAYS reference the historical period ({start_date} to {end_date}) and use past tense
- **📖 HISTORICAL FRAMING:** Use "During the backtest period...", "In this historical simulation...", "From {start_date} to {end_date}..."
- **🚫 NO CURRENT REFERENCES:** Never imply this is current or applicable to today's market
- **🚫 NO PRESCRIPTIONS:** Do not suggest "improvements," "next steps," or "adjustments." Instead, use "Historical data suggests sensitivity to [Variable] during this period."
- **📊 OBJECTIVE TONE:** Avoid evaluative words like "Concerning," "Good," "Bad," "Successful," or "Failed." Use "Underperformed benchmark" or "Exceeded historical volatility."
- **🚫 NO DIRECTIVES:** Never use "Buy," "Sell," "Hold," "Trade," or "Traders should."
- **DISCLAIMER:** Conclude with the Mandatory Disclaimer below.

### FORMATTING:
- Use ## for Headers.
- Use **Bold** for all numerical values.
- Use Code Blocks (```) for any data comparisons.
- Include date range ({start_date} to {end_date}) in section headers.

## MANDATORY DISCLAIMER
⚠️ **HISTORICAL BACKTEST ALERT:** This analysis is based on historical data from {start_date} to {end_date} with a mandatory 30+ day lag per SEBI regulations. This is a hypothetical historical simulation, NOT a current market assessment, NOT financial advice, and NOT a recommendation to trade. Past results do not predict future returns. All trading involves substantial risk.
"""
                try:
                    ai_analysis = call_gemini_api(ai_prompt)
                    performance['ai_analysis'] = ai_analysis
                except Exception as e:
                    logger.error(f"AI analysis failed: {e}")
                    performance['ai_analysis'] = "AI analysis temporarily unavailable. Please try again later."

        # Remove the DataFrame from the response to avoid serialization issues
        performance.pop('trades_df', None)
        
        # Add SEBI compliance information to response
        performance['sebi_compliance'] = {
            'data_lag_days': DATA_LAG_DAYS,
            'data_range': compliance_info.get('date_range'),
            'rows_excluded_for_compliance': compliance_info.get('rows_excluded', 0),
            'effective_end_date': compliance_info.get('effective_end_date'),
            'compliance_notice': f"This analysis uses historical data with a mandatory {DATA_LAG_DAYS}-day lag in accordance with SEBI regulations. No current market data is included."
        }

        return jsonify(performance)

    except ValueError as e:
        return jsonify(error=str(e)), 400
    except Exception as e:
        logger.error(f"❌ Backtest error: {e}")
        return jsonify(error=f"Server error: {str(e)}"), 500  
    


@api.route('/backtest/monte_carlo', methods=['POST'])
def run_monte_carlo():
    """
    Run Monte Carlo simulation analysis on backtest results with input validation.

    This endpoint analyzes whether backtest results are due to luck or skill
    by running thousands of randomized simulations.
    """
    auth_response = require_auth()
    if auth_response:
        return auth_response

    data = request.get_json()
    if not data:
        return jsonify(error="Request body is required"), 400

    validation_errors = []
    
    # Required parameters
    trades = data.get('trades', [])
    prices = data.get('prices', [])
    
    # Validate trades is a list with at least 2 items
    if not isinstance(trades, list):
        validation_errors.append("Trades must be an array")
    elif len(trades) < 2:
        validation_errors.append("At least 2 trades required for Monte Carlo analysis")
    elif len(trades) > 10000:
        validation_errors.append("Maximum 10,000 trades allowed")
    
    # Validate prices is a list if provided
    if prices and not isinstance(prices, list):
        validation_errors.append("Prices must be an array")
    elif prices and len(prices) > 100000:
        validation_errors.append("Maximum 100,000 price points allowed")
    
    # Validate num_simulations (100 to 100000)
    num_simulations, error_msg = validate_int(
        data.get('num_simulations', 10000), 'Number of simulations',
        min_val=100, max_val=100000
    )
    if error_msg:
        validation_errors.append(error_msg)
    
    # Validate seed
    seed_val = data.get('seed', 0)
    try:
        seed = int(seed_val)
        if seed < 0:
            validation_errors.append("Seed must be a non-negative integer")
    except (ValueError, TypeError):
        validation_errors.append("Seed must be a valid integer")
        seed = 0
    
    # Validate initial_capital
    initial_capital, error_msg = validate_float(
        data.get('initial_capital', 100000), 'Initial capital',
        min_val=1000, max_val=100000000
    )
    if error_msg:
        validation_errors.append(error_msg)
    
    # Validate numeric metrics
    try:
        original_return = float(data.get('original_return', 0))
        if not (-1000 <= original_return <= 1000):
            validation_errors.append("Original return must be between -1000% and 1000%")
    except (ValueError, TypeError):
        validation_errors.append("Original return must be a valid number")
        original_return = 0
    
    try:
        original_sharpe = float(data.get('original_sharpe', 0))
        if not (-100 <= original_sharpe <= 100):
            validation_errors.append("Original Sharpe ratio must be between -100 and 100")
    except (ValueError, TypeError):
        validation_errors.append("Original Sharpe ratio must be a valid number")
        original_sharpe = 0
    
    try:
        original_max_dd = float(data.get('original_max_dd', 0))
        if not (-100 <= original_max_dd <= 0):
            validation_errors.append("Original max drawdown must be between -100% and 0%")
    except (ValueError, TypeError):
        validation_errors.append("Original max drawdown must be a valid number")
        original_max_dd = 0
    
    # Return validation errors if any
    if validation_errors:
        logger.warning(f"Monte Carlo validation failed: {validation_errors}")
        return jsonify(create_validation_error(validation_errors)), 400
    
    try:
        logger.info(f"🎲 Starting Monte Carlo analysis: {num_simulations} simulations")
        
        # Initialize Monte Carlo engine
        mc_engine = MonteCarloEngine(seed=seed)
        mc_engine.set_trades(trades)
        
        # Set daily returns if prices provided
        if prices and len(prices) > 1:
            import pandas as pd
            price_series = pd.Series(prices)
            mc_engine.set_daily_returns(price_series)
        
        # Configure simulation
        config = SimulationConfig(
            num_simulations=num_simulations,
            seed=mc_engine.seed,
            initial_capital=initial_capital
        )
        
        # Run analysis
        start_time = datetime.now()
        analysis = mc_engine.run_analysis(config)
        
        # Calculate p-values and update interpretation
        analysis = mc_engine.calculate_p_values(
            analysis, 
            original_return, 
            original_sharpe
        )
        
        elapsed_time = (datetime.now() - start_time).total_seconds()
        
        # Prepare response
        response = analysis.to_dict()
        response['performance'] = {
            'elapsed_time_seconds': elapsed_time,
            'simulations_per_second': round(num_simulations / elapsed_time, 2)
        }
        
        logger.info(f"✅ Monte Carlo analysis complete in {elapsed_time:.2f}s")
        
        return jsonify(response), 200
        
    except Exception as e:
        logger.error(f"❌ Monte Carlo analysis error: {e}")
        logger.error(traceback.format_exc())
        return jsonify(error=f"Monte Carlo analysis failed: {str(e)}"), 500


@api.route('/backtest/quick_mc', methods=['POST'])
def run_quick_monte_carlo():
    """
    Quick Monte Carlo analysis (1,000 simulations) for fast preview.
    """
    auth_response = require_auth()
    if auth_response:
        return auth_response
    
    data = request.get_json()
    data['num_simulations'] = 1000  # Force 1k simulations
    
    # Forward to main endpoint
    return run_monte_carlo()


@api.route('/admin/init-redis', methods=['POST'])
def admin_init_redis():
    """
    Admin endpoint to initialize Redis and index knowledge base.
    This is a workaround for Render free tier which doesn't support Shell access.
    
    Usage:
    - POST to /api/admin/init-redis with admin key
    - Or access via browser: GET /api/admin/init-redis?key=YOUR_ADMIN_KEY
    
    Security: Requires ADMIN_KEY environment variable
    """
    # Check admin key
    admin_key = request.args.get('key') or request.headers.get('X-Admin-Key')
    expected_key = os.getenv('ADMIN_KEY')
    
    if not expected_key:
        return jsonify(
            error="ADMIN_KEY not configured",
            message="Set ADMIN_KEY environment variable to use this endpoint"
        ), 500
    
    if admin_key != expected_key:
        logger.warning(f"Unauthorized admin access attempt from {request.remote_addr}")
        return jsonify(error="Unauthorized"), 401
    
    try:
        results = {
            "timestamp": datetime.now().isoformat(),
            "steps": []
        }
        
        # Step 1: Initialize Redis
        results["steps"].append({"step": 1, "action": "Initialize Redis connection"})
        if REDIS_AVAILABLE:
            if redis_client.is_connected():
                results["steps"][-1]["status"] = "✅ Already connected"
            else:
                try:
                    redis_client.connect()
                    results["steps"][-1]["status"] = "✅ Connected"
                except Exception as e:
                    results["steps"][-1]["status"] = f"❌ Failed: {str(e)}"
                    return jsonify(results), 500
        else:
            results["steps"][-1]["status"] = "❌ Redis module not available"
            return jsonify(results), 500
        
        # Step 2: Initialize RAG index
        results["steps"].append({"step": 2, "action": "Create vector search index"})
        try:
            if rag_engine.create_index():
                results["steps"][-1]["status"] = "✅ Index ready"
            else:
                results["steps"][-1]["status"] = "⚠️ Index may already exist"
        except Exception as e:
            results["steps"][-1]["status"] = f"⚠️ {str(e)}"
        
        # Step 3: Index knowledge base
        results["steps"].append({"step": 3, "action": "Index knowledge base documents"})
        try:
            import subprocess
            import sys

            # Run indexing script
            result = subprocess.run(
                [sys.executable, "scripts/index_knowledge.py"],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                results["steps"][-1]["status"] = "✅ Knowledge base indexed"
                results["steps"][-1]["output"] = result.stdout[-500:]  # Last 500 chars
            else:
                results["steps"][-1]["status"] = f"❌ Indexing failed"
                results["steps"][-1]["error"] = result.stderr[-500:]
                
        except Exception as e:
            results["steps"][-1]["status"] = f"❌ Error: {str(e)}"
        
        # Step 4: Verify index
        results["steps"].append({"step": 4, "action": "Verify index contents"})
        try:
            stats = rag_engine.get_stats()
            results["steps"][-1]["status"] = f"✅ {stats.get('document_count', 0)} documents indexed"
            results["stats"] = stats
        except Exception as e:
            results["steps"][-1]["status"] = f"⚠️ {str(e)}"
        
        # Overall status
        success = all("❌" not in step.get("status", "") for step in results["steps"])
        results["success"] = success
        
        return jsonify(results), 200 if success else 500
        
    except Exception as e:
        logger.error(f"Admin init error: {e}")
        return jsonify(
            error="Initialization failed",
            message=str(e)
        ), 500


@api.route('/admin/redis-status', methods=['GET'])
def admin_redis_status():
    """
    Check Redis and RAG status.
    Can be accessed without auth to verify deployment.
    """
    status = {
        "timestamp": datetime.now().isoformat(),
        "redis_available": REDIS_AVAILABLE,
        "redis_connected": False,
        "rag_ready": False,
        "knowledge_base": {}
    }
    
    if REDIS_AVAILABLE:
        try:
            status["redis_connected"] = redis_client.is_connected()
            if status["redis_connected"]:
                status["rag_ready"] = rag_engine.model is not None
                status["knowledge_base"] = rag_engine.get_stats()
        except Exception as e:
            status["error"] = str(e)
    
    return jsonify(status), 200
