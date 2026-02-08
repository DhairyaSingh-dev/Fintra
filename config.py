"""
Configuration Management Module
Handles environment variables, secrets, and application settings.
"""
import os
from datetime import timedelta


class Config:
    """Base configuration class"""

    # Flask Settings
    SECRET_KEY = os.getenv('FLASK_SECRET_KEY')

    # Detect environment: Assume production if RENDER env var is set or FLASK_ENV is production
    # We default to True if not explicitly development to ensure security headers on cloud deployments
    IS_PRODUCTION = os.getenv('RENDER') is not None or os.getenv('FLASK_ENV') == 'production' or os.getenv('FLASK_ENV') != 'development'

    # Force secure cookies in production for cross-site usage (Vercel -> Render)
    SESSION_COOKIE_SECURE = IS_PRODUCTION
    SESSION_COOKIE_SAMESITE = 'None' if IS_PRODUCTION else 'Lax'

    # Define a data directory, configurable via environment variable. Defaults to the project root.
    DATA_DIR = os.getenv('DATA_DIR', os.path.dirname(os.path.abspath(__file__)))

    # Database Configuration
    # Check for DATABASE_URL. If provided by Render/Neon, it might start with 'postgres://'
    # SQLAlchemy requires 'postgresql://', so we fix it if necessary.
    _db_url = os.getenv('DATABASE_URL')

    # Robust cleaning: strip whitespace and quotes that might have been pasted in
    if _db_url:
        _db_url = _db_url.strip().strip('"').strip("'")

    if _db_url and _db_url.startswith('postgres://'):
        _db_url = _db_url.replace('postgres://', 'postgresql://', 1)

    # Fix for Neon/Render: Ensure sslmode is set to require
    if _db_url and 'postgresql://' in _db_url and 'sslmode' not in _db_url:
        separator = '&' if '?' in _db_url else '?'
        _db_url = f"{_db_url}{separator}sslmode=require"

    SQLALCHEMY_DATABASE_URI = _db_url if _db_url else f"sqlite:///{os.path.join(DATA_DIR, 'portfolio.db')}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    # Engine options to improve connection pool reliability, especially for serverless DBs like Neon.
    # pool_pre_ping checks if a connection is alive before using it, preventing OperationalError
    # on stale connections.
    SQLALCHEMY_ENGINE_OPTIONS = {
        "pool_pre_ping": True,
    }

    # Google OAuth Configuration
    GOOGLE_CLIENT_ID = os.getenv('GOOGLE_CLIENT_ID')
    GOOGLE_CLIENT_SECRET = os.getenv('GOOGLE_CLIENT_SECRET')

    # Gemini API Key
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')

    # Centralized URL Configuration
    # Set these in Render/Vercel to control CORS and Redirects
    # Default to production URLs if running on Render (IS_PRODUCTION is True)
    _default_client = 'https://fintraio.vercel.app' if IS_PRODUCTION else 'http://localhost:5000'
    CLIENT_ORIGIN = os.getenv('CLIENT_ORIGIN', _default_client).rstrip('/')

    _default_backend = 'https://stock-dashboard-fqtn.onrender.com' if IS_PRODUCTION else 'http://localhost:5000'
    BACKEND_ORIGIN = os.getenv('BACKEND_ORIGIN', _default_backend).rstrip('/')

    # Production OAuth Redirect URI
    REDIRECT_URI = f"{BACKEND_ORIGIN}/api/oauth2callback"

    # Production Frontend URL
    CLIENT_REDIRECT_URL = f"{CLIENT_ORIGIN}/dashboard.html"

    # JWT Configuration
    # Secrets MUST be set via environment variables for security
    ACCESS_TOKEN_JWT_SECRET = os.getenv('ACCESS_TOKEN_JWT_SECRET')
    REFRESH_TOKEN_JWT_SECRET = os.getenv('REFRESH_TOKEN_JWT_SECRET')
    ACCESS_TOKEN_EXPIRETIME = '15m'
    REFRESH_TOKEN_EXPIRETIME = '7d'
    
    # Validate that secrets are set
    @classmethod
    def validate_secrets(cls):
        """Validate that required secrets are configured"""
        missing = []
        if not cls.ACCESS_TOKEN_JWT_SECRET:
            missing.append('ACCESS_TOKEN_JWT_SECRET')
        if not cls.REFRESH_TOKEN_JWT_SECRET:
            missing.append('REFRESH_TOKEN_JWT_SECRET')
        if not cls.GOOGLE_CLIENT_ID:
            missing.append('GOOGLE_CLIENT_ID')
        if not cls.GOOGLE_CLIENT_SECRET:
            missing.append('GOOGLE_CLIENT_SECRET')
        
        if missing:
            import logging
            logger = logging.getLogger(__name__)
            logger.error("=" * 60)
            logger.error("❌ MISSING REQUIRED ENVIRONMENT VARIABLES:")
            for var in missing:
                logger.error(f"   - {var}")
            logger.error("=" * 60)
            logger.error("Please set these variables in your Render Dashboard:")
            logger.error("https://dashboard.render.com")
            # Don't raise - let the app start so we can see the logs
            return False
        
        return True

    # OAuth Scopes
    SCOPES = [
        'openid',
        'https://www.googleapis.com/auth/userinfo.email',
        'https://www.googleapis.com/auth/userinfo.profile'
    ]

    # CORS Configuration
    CORS_ORIGINS = [
        "http://localhost:5000",
        "http://127.0.0.1:5000",
        CLIENT_ORIGIN
    ]

    # Cookie Domain
    # In production, set this to your parent domain (e.g., ".yourdomain.com") if frontend and backend are on subdomains.
    # If frontend (Vercel) and backend (Render) are on different domains, leave this as None to default to the backend host.
    COOKIE_DOMAIN = None

    @staticmethod
    def parse_time_to_seconds(time_str: str) -> int:
        """Convert time string like '15m' or '7d' to seconds"""
        if time_str.endswith('m'):
            return int(time_str[:-1]) * 60
        elif time_str.endswith('h'):
            return int(time_str[:-1]) * 3600
        elif time_str.endswith('d'):
            return int(time_str[:-1]) * 86400
        return 900
