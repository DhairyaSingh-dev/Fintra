"""
Application startup script
Initializes Redis, RAG, and other services
Feature flags: ENABLE_REDIS and ENABLE_RAG can override automatic init behavior.
"""
import logging
import os

from rag_engine import init_rag
from redis_client import init_redis

logger = logging.getLogger(__name__)

def init_services():
    """Initialize all services on application startup"""
    logger.info("=" * 60)
    logger.info("Initializing Fintra Services")
    logger.info("=" * 60)
    
    # Feature flags (explicit enable/disable)
    redis_flag = os.getenv("ENABLE_REDIS", "true").lower()
    rag_flag = os.getenv("ENABLE_RAG", "true").lower()

    redis_enabled = redis_flag in ("1", "true", "yes", "on")
    rag_enabled = rag_flag in ("1", "true", "yes", "on")

    # Initialize Redis (if enabled)
    if redis_enabled:
        redis_ok = init_redis()
    else:
        logger.info("🔧 Redis disabled by ENABLE_REDIS flag; skipping initialization.")
        redis_ok = False
    
    # Initialize RAG (may require Redis; allow explicit disable via flag)
    if rag_enabled:
        rag_ok = init_rag() if redis_ok or True else False
    else:
        logger.info("🎛️ RAG disabled by ENABLE_RAG flag; skipping initialization.")
        rag_ok = False
    
    logger.info("=" * 60)
    logger.info(f"Redis: {'✅ Connected' if redis_ok else '❌ Not Available'}")
    logger.info(f"RAG: {'✅ Ready' if rag_ok else '❌ Not Available'}")
    logger.info("=" * 60)
    
    return {
        'redis': redis_ok,
        'rag': rag_ok
    }

if __name__ == "__main__":
    init_services()
