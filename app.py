"""
Main Application Entry Point
Initializes Flask app, configures middleware, registers blueprints.
"""
import logging
import os
import traceback
from sys import stdout

from flask import Flask, jsonify, request
from flask_cors import CORS
# SocketIO removed — replay uses REST endpoint instead
from sqlalchemy import text

from config import Config
from database import db
from routes import api

#easter egg
# ==================== LOGGING SETUP ====================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
    handlers=[logging.StreamHandler(stdout)]
)
logger = logging.getLogger(__name__)

#DevEasterEgg
# ==================== APPLICATION FACTORY ====================
def create_app():
    """Application factory pattern"""
    # Define the static folder using an absolute path for reliability, especially in Docker.
    # This ensures Flask knows exactly where to find files like main.js and styles.css.
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    app = Flask(__name__, 
                static_folder=static_dir, 
                static_url_path='')

    # Load configuration
    app.config.from_object(Config)
    
    # Validate required secrets are set
    secrets_valid = Config.validate_secrets()
    if not secrets_valid:
        logger.error("⚠️  Some required secrets are missing. The app may not function correctly.")

    # Initialize extensions
    db.init_app(app)

    # CORS is handled manually via before_request/after_request hooks below.
    # Do NOT use Flask-CORS here — it conflicts with our manual headers.
    # CORS(app, supports_credentials=True, origins=Config.CORS_ORIGINS, ...)

    # Note: SocketIO removed — replay/forward-test use REST endpoints

    # Register blueprints
    app.register_blueprint(api, url_prefix='/api')

    # ==================== INITIALIZE REDIS & RAG (RENDER FREE TIER) ====================
    # Auto-initialize on startup with retry logic for Render's ephemeral Redis
    def init_services_background():
        """Initialize Redis and index knowledge base in background thread"""
        import threading
        import time
        
        def init_worker():
            """Worker thread to initialize services without blocking startup"""
            try:
                # Import here to avoid circular imports
                from rag_engine import init_rag, rag_engine
                from redis_client import init_redis, redis_client
                
                logger.info("🔄 Background initialization started...")
                
                # Retry logic for Redis connection (Render Redis takes time to be ready)
                for attempt in range(5):
                    try:
                        if init_redis():
                            logger.info("✅ Redis connected")
                            break
                    except Exception as e:
                        logger.warning(f"⚠️ Redis connection attempt {attempt + 1}/5 failed: {e}")
                        if attempt < 4:
                            time.sleep(5)  # Wait 5 seconds before retry
                
                # Initialize RAG index
                try:
                    if init_rag():
                        logger.info("✅ RAG index ready")
                except Exception as e:
                    logger.warning(f"⚠️ RAG initialization: {e}")
                
                # Check if knowledge base needs indexing
                try:
                    if redis_client.is_connected():
                        stats = rag_engine.get_stats()
                        doc_count = stats.get('document_count', 0)
                        
                        if doc_count == 0:
                            logger.info("📚 Knowledge base empty, indexing documents...")
                            # Run indexing sequentially in this same thread to save memory
                            # instead of spawning a new Python subprocess
                            try:
                                import sys
                                project_root = os.path.dirname(os.path.abspath(__file__))
                                if project_root not in sys.path:
                                    sys.path.insert(0, project_root)
                                    
                                from scripts.index_knowledge import index_documents
                                success = index_documents()
                                
                                if success:
                                    logger.info("✅ Knowledge base indexed successfully (Sequential)")
                                else:
                                    logger.error("❌ Knowledge base indexing failed")
                            except Exception as idx_err:
                                logger.error(f"❌ Error during sequential indexing: {idx_err}")
                        else:
                            logger.info(f"✅ Knowledge base already indexed ({doc_count} documents)")
                            
                except Exception as e:
                    logger.error(f"❌ Knowledge base check/index error: {e}")
                    
                logger.info("🎉 Background initialization complete!")
                
            except ImportError as e:
                logger.warning(f"⚠️ Redis/RAG modules not available: {e}")
            except Exception as e:
                logger.error(f"❌ Background initialization error: {e}")
        
        # Start initialization in background thread so it doesn't block app startup
        thread = threading.Thread(target=init_worker, daemon=True)
        thread.start()
        logger.info("🚀 Background service initialization started (non-blocking)")
    
    init_services_background()

    # ==================== BULLETPROOF CORS ====================
    # Flask-CORS can silently fail if middleware order or error handlers interfere.
    # These hooks guarantee CORS headers on EVERY response, no matter what.
    
    allowed_origins = set(Config.CORS_ORIGINS)

    @app.before_request
    def handle_preflight_and_logging():
        """Handle OPTIONS preflight immediately + log requests."""
        origin = request.headers.get('Origin', '')
        
        if request.method == 'OPTIONS':
            # Immediately respond to preflight — don't let it reach any route or error handler
            logger.info(f"✈️ PREFLIGHT {request.path} | Origin: {origin}")
            resp = app.make_default_options_response()
            if origin in allowed_origins:
                resp.headers['Access-Control-Allow-Origin'] = origin
                resp.headers['Access-Control-Allow-Credentials'] = 'true'
                resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, DELETE, PUT'
                resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'
                resp.headers['Access-Control-Max-Age'] = '3600'
            return resp
        
        if not request.path.endswith('/health'):
            logger.info(f"📥 [{request.method}] {request.path} | Origin: {origin or 'None'}")
            logger.info(f"   🔑 Incoming Cookies: {list(request.cookies.keys())}")

    @app.after_request
    def ensure_cors_and_tokens(response):
        """Stamp CORS headers + inject refreshed auth cookies on EVERY response."""
        # 1. CORS headers
        origin = request.headers.get('Origin', '')
        if origin in allowed_origins:
            response.headers['Access-Control-Allow-Origin'] = origin
            response.headers['Access-Control-Allow-Credentials'] = 'true'
            response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS, DELETE, PUT'
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization, X-Requested-With'
        
        # 2. If require_auth() silently refreshed tokens, set cookies on this response
        from flask import g
        pending_access = getattr(g, 'pending_access_token', None)
        pending_refresh = getattr(g, 'pending_refresh_token', None)
        if pending_access and pending_refresh:
            from auth import set_token_cookies
            set_token_cookies(response, pending_access, pending_refresh)
            logger.info("🔄 Silently refreshed auth tokens on response")
        
        return response

    # Error handlers
    @app.errorhandler(Exception)
    def handle_exception(e):
        """Global exception handler with enhanced logging."""
        tb_str = traceback.format_exc()
        
        request_details = {}
        try:
            request_details = {
                "method": request.method,
                "path": request.path,
                "headers": dict(request.headers),
            }
        except Exception as req_exc:
            logger.error(f"Could not extract request details during exception handling: {req_exc}")

        logger.error(f"--- Unhandled Exception ---")
        logger.error(f"Request: {request_details}")
        logger.error(f"Exception: {e}\n{tb_str}")
        logger.error(f"--- End Exception ---")
        
        response = jsonify(
            error="An internal server error occurred.",
            details=None # In production, do not expose internal error details
        )
        response.status_code = 500
        return response

    @app.route('/')
    def landing_page():
        return app.send_static_file("index.html")

    @app.route('/dashboard')
    def dashboard_page():
        return app.send_static_file("dashboard.html")

    @app.errorhandler(404)
    def not_found(e):
        # If the path starts with /api, it's a genuine API 404 error.
        if request.path.startswith('/api/'):
            return jsonify(error="API endpoint not found"), 404
        else:
            # Otherwise, redirect to landing
            return app.send_static_file("index.html")

    # Add a startup log to display critical configuration
    with app.app_context():
        # Create database tables if they don't exist
        db.create_all()

        # --- SCHEMA MIGRATION ---
        # db.create_all() does not update existing tables. We manually ensure the 'picture' column exists.
        try:
            with db.engine.connect() as conn:
                conn.execute(text('ALTER TABLE "user" ADD COLUMN IF NOT EXISTS picture VARCHAR(512)'))
                conn.commit()
        except Exception as e:
            logger.warning(f"Schema migration check failed: {e}")

        logger.info(" 🗃️  Database tables ensured.")

        logger.info("=" * 70)
        logger.info(" 🚀 BACKEND SERVER STARTING UP")
        logger.info(f" 🌍 Environment Config: IS_PRODUCTION={app.config.get('IS_PRODUCTION')}")
        logger.info(f" 🍪 Cookie Config: Secure={app.config.get('SESSION_COOKIE_SECURE')}, SameSite={app.config.get('SESSION_COOKIE_SAMESITE')}")
        logger.info(f" 🔐 Google Client ID: {Config.GOOGLE_CLIENT_ID[:10] if Config.GOOGLE_CLIENT_ID else 'NOT SET'}{'...' if Config.GOOGLE_CLIENT_ID else ''}")
        logger.info(f" ↪️ Google Redirect URI: {Config.REDIRECT_URI}")
        logger.info(f" 🌐 Frontend Redirect URL: {Config.CLIENT_REDIRECT_URL}")
        logger.info(f" 🔑 JWT Secrets Loaded: {'✅' if Config.ACCESS_TOKEN_JWT_SECRET and Config.REFRESH_TOKEN_JWT_SECRET else '❌ NOT FOUND'}")
        logger.info("=" * 70)

    # Replay uses REST endpoint /api/replay/candles (no WebSocket needed)

    return app
    
app = create_app()
