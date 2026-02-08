"""
Application startup script
Initializes Redis, RAG, and other services
"""
import logging

from rag_engine import init_rag
from redis_client import init_redis

logger = logging.getLogger(__name__)

def init_services():
    """Initialize all services on application startup"""
    logger.info("=" * 60)
    logger.info("Initializing Fintra Services")
    logger.info("=" * 60)
    
    # Initialize Redis
    redis_ok = init_redis()
    
    # Initialize RAG (requires Redis)
    if redis_ok:
        rag_ok = init_rag()
    else:
        logger.warning("⚠️ Skipping RAG initialization (Redis not available)")
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
