"""
Authentication Module
Handles OAuth, JWT tokens, session management, and authentication middleware.
"""
import logging
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Dict, Optional

import jwt
import requests
from flask import current_app, jsonify, request, session

from config import Config

logger = logging.getLogger(__name__)

def generate_jwt_token(user_data: dict, secret: str, expires_in: str) -> str:
    """Generate JWT token"""
    if not secret:
        logger.error("Cannot generate JWT token: secret is None or empty")
        raise ValueError("JWT secret is not configured")
    
    expiry_seconds = Config.parse_time_to_seconds(expires_in)
    payload = {
        'user_id': user_data['user_id'],
        'email': user_data['email'],
        'name': user_data.get('name', ''),
        'exp': datetime.now(timezone.utc) + timedelta(seconds=expiry_seconds),
        'iat': datetime.now(timezone.utc)
    }
    return jwt.encode(payload, secret, algorithm='HS256')

def verify_jwt_token(token: str, secret: str) -> Optional[dict]:
    """Verify JWT token"""
    if not secret:
        logger.error("Cannot verify JWT token: secret is None or empty")
        return None
    
    try:
        # Add a 10-second leeway to account for minor clock skew.
        return jwt.decode(token, secret, algorithms=['HS256'], leeway=timedelta(seconds=10))
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError) as e:
        logger.warning(f"JWT verification failed: {e}")
        return None

def set_token_cookies(response, access_token: str, refresh_token: str):
    """Set cookies safely so browser actually stores them."""
    is_production = current_app.config.get('SESSION_COOKIE_SECURE', False)
    samesite_mode = current_app.config.get('SESSION_COOKIE_SAMESITE', 'Lax')

    # Safety Net: Browsers reject SameSite=None if Secure is False.
    if samesite_mode == 'None' and not is_production:
        logger.warning("⚠️ Configuration Mismatch: Forcing Secure=True because SameSite='None'.")
        is_production = True

    # Calculate max_age in seconds
    access_max_age = Config.parse_time_to_seconds(Config.ACCESS_TOKEN_EXPIRETIME)
    refresh_max_age = Config.parse_time_to_seconds(Config.REFRESH_TOKEN_EXPIRETIME)

    logger.info(f"🍪 Setting cookies: Secure={is_production}, SameSite={samesite_mode}")
    logger.info(f"   Access token expires in {access_max_age}s, Refresh token expires in {refresh_max_age}s")

    # Explicitly set path='/' to ensure cookies are sent for all API routes
    response.set_cookie(
        'access_token',
        access_token,
        httponly=True,
        secure=is_production,
        samesite=samesite_mode,
        max_age=access_max_age,
        domain=None,
        path='/' 
    )

    response.set_cookie(
        'refresh_token',
        refresh_token,
        httponly=True,
        secure=is_production,
        samesite=samesite_mode,
        max_age=refresh_max_age,
        domain=None,
        path='/'
    )

    # --- FIX: Add 'Partitioned' attribute for Cross-Site Tracking (CHIPS) ---
    # Modern browsers (Chrome 110+) require cookies in cross-site contexts (Vercel -> Render)
    # to be 'Partitioned' if third-party cookies are restricted.
    if is_production and samesite_mode == 'None':
        cookie_headers = response.headers.getlist("Set-Cookie")
        new_cookie_headers = []
        for header in cookie_headers:
            if "Partitioned" not in header:
                new_cookie_headers.append(header + "; Partitioned")
            else:
                new_cookie_headers.append(header)
        
        del response.headers["Set-Cookie"]
        for header in new_cookie_headers:
            response.headers.add("Set-Cookie", header)
        logger.info("🍪 Added 'Partitioned' attribute to cookies for CHIPS support.")

    return response

def require_auth():
    """
    Stateless authentication check. Relies only on JWTs and the database.
    Returns a Flask Response object if auth fails, otherwise returns None.
    """
    logger.debug("--- Stateless Auth check initiated ---")
    access_token = request.cookies.get('access_token')

    # 1a. Fallback: Check Authorization Header if cookie is missing
    if not access_token:
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith("Bearer "):
            access_token = auth_header.split(" ")[1]

    # 1. Try access token
    if access_token:
        payload = verify_jwt_token(access_token, Config.ACCESS_TOKEN_JWT_SECRET)
        if payload:
            user_id = payload.get('user_id')
            if user_id:
                logger.debug(f"Access token is valid for user {user_id}. Granting access.")
                return None  # Success
        else:
            logger.info("Access token invalid or expired. Falling back to refresh token.")
    else:
        logger.info("No access_token cookie found in request.")

    # 2. Try refresh token from cookie or Authorization header
    refresh_token = request.cookies.get('refresh_token')
    
    # Fallback: Check Authorization Header for refresh token
    if not refresh_token:
        auth_header = request.headers.get('Authorization')
        if auth_header and auth_header.startswith("Bearer "):
            # Try to parse two tokens: access_token:refresh_token
            token_parts = auth_header.split(" ")[1].split(':')
            if len(token_parts) >= 2:
                refresh_token = token_parts[1]
                logger.debug("Using refresh token from Authorization header")
    
    if refresh_token:
        payload = verify_jwt_token(refresh_token, Config.REFRESH_TOKEN_JWT_SECRET)
        if payload:
            user_id = payload.get('user_id')
            if user_id:
                # We need user data from DB to generate a new access token.
                from models import User
                db_user = User.query.filter_by(google_user_id=user_id).first()
                if db_user:
                    logger.info(f"Refresh token valid for user {user_id}. Issuing new access token.")
                    user_data = {'user_id': db_user.google_user_id, 'email': db_user.email, 'name': db_user.name}
                    new_access_token = generate_jwt_token(user_data, Config.ACCESS_TOKEN_JWT_SECRET, Config.ACCESS_TOKEN_EXPIRETIME)
                    new_refresh_token = generate_jwt_token(user_data, Config.REFRESH_TOKEN_JWT_SECRET, Config.REFRESH_TOKEN_EXPIRETIME)
                    
                    # Return both tokens in response for frontend to store
                    response = jsonify({
                        "error": "Access token refreshed",
                        "access_token": new_access_token,
                        "refresh_token": new_refresh_token
                    })
                    # Also try to set cookies as backup
                    set_token_cookies(response, new_access_token, new_refresh_token)
                    return response, 401  # Signal client to retry
                else:
                    logger.error(f"Refresh token is for a user ({user_id}) that does not exist in DB.")
        else:
            logger.warning("Refresh token found but verification failed.")
    else:
        logger.info("No refresh_token found in request (cookie or header).")

    logger.warning("--- Auth check failed: No valid tokens found. Denying access. ---")
    # Clear potentially bad cookies on the client
    response = jsonify({"error": "Not authenticated. Please sign in."})
    response.set_cookie('access_token', '', max_age=0)
    response.set_cookie('refresh_token', '', max_age=0)
    return response, 401
