"""
Redis Configuration and Client
Handles Redis connection, caching, and vector search
"""
import hashlib
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import redis
from redisvl.index import SearchIndex
from redisvl.query import VectorQuery

logger = logging.getLogger(__name__)

class RedisConfig:
    """Redis configuration settings"""
    HOST = os.getenv('REDIS_HOST', 'localhost')
    PORT = int(os.getenv('REDIS_PORT', 6379))
    DB = int(os.getenv('REDIS_DB', 0))
    PASSWORD = os.getenv('REDIS_PASSWORD', None)
    
    # Vector search settings
    # Gemini embedding-001 uses 3072 dimensions, local models use 384
    VECTOR_DIM = 3072 if os.getenv('GEMINI_API_KEY') else 384
    VECTOR_INDEX_NAME = "fintra_knowledge"
    SIMILARITY_THRESHOLD = 0.75
    
    # Cache TTL settings (in seconds)
    CHAT_CACHE_TTL = 3600  # 1 hour
    DATA_CACHE_TTL = 300   # 5 minutes
    SESSION_TTL = 86400    # 24 hours
    RATE_LIMIT_WINDOW = 60 # 1 minute

class RedisClient:
    """Singleton Redis client wrapper"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._client = None
            cls._instance._vector_index = None
        return cls._instance
    
    def connect(self) -> Any:
        """Initialize Redis connection with Upstash & Render support"""
        if self._client is None:
            try:
                # 0. Fast-track Upstash REST API if provided (fixes DNS/IP blocking issues)
                raw_host = os.getenv('REDIS_HOST')
                # Explicitly grab the API Key for the Bearer token header as requested
                api_key_bearer = os.getenv('REDIS_PASSWORD')
                
                if raw_host and api_key_bearer and 'upstash.io' in raw_host:
                    # Ensure the host has the required https protocol for the REST client
                    rest_url = raw_host if raw_host.startswith('http') else f"https://{raw_host}"
                    
                    try:
                        from upstash_redis import Redis as UpstashRedis
                        logger.info(f"🔒 Using native upstash-redis REST client with REDIS_API_KEY Bearer token at {rest_url}")
                        # The 'token' param exactly translates to: Authorization: Bearer <api_key_bearer>
                        self._client = UpstashRedis(url=rest_url, token=api_key_bearer)
                        # Test connection
                        self._client.ping()
                        logger.info("✅ Redis connection established (REST HTTP)")
                        return self._client
                    except ImportError:
                        logger.warning("upstash-redis package not found, falling back to standard redis-py")

                # 1. Prefer REDIS_URL if available (It handles username, pass, and rediss:// SSL natively)
                redis_url = os.getenv('REDIS_URL')
                
                kwargs = {
                    'decode_responses': True,
                    'socket_connect_timeout': 5,
                    'socket_timeout': 5,
                    'health_check_interval': 30,
                    'socket_keepalive': True,
                    'retry_on_timeout': True,
                }

                if redis_url:
                    if redis_url.startswith('rediss://'):
                        kwargs['ssl_cert_reqs'] = "none"
                        logger.info("🔒 Using native SSL Rediss URL for connection")
                    self._client = redis.from_url(redis_url, **kwargs)
                else:    
                    # 2. Fallback to manual pieces
                    is_upstash = "upstash.io" in RedisConfig.HOST
                    use_ssl = is_upstash or RedisConfig.PORT == 6379 

                    connection_params = {
                        'host': RedisConfig.HOST,
                        'port': RedisConfig.PORT,
                        'db': RedisConfig.DB,
                        'password': RedisConfig.PASSWORD,
                        **kwargs
                    }

                    if is_upstash:
                        # Upstash requires the explicit 'default' username via ACLs if not using a URL
                        connection_params['username'] = 'default'

                    if use_ssl:
                        connection_params.update({
                            'ssl': True,
                            'ssl_cert_reqs': "none",
                        })
                        logger.info(f"🔒 SSL enabled for Redis at {RedisConfig.HOST}")

                    self._client = redis.Redis(**connection_params)
                
                # Test connection
                self._client.ping()
                logger.info("✅ Redis connection established")
                
            except Exception as e:
                logger.error(f"❌ Redis connection failed: {e}")
                self._client = None
                raise
        return self._client
    
    def get_client(self) -> Optional[redis.Redis]:
        """Get Redis client, attempt reconnect if needed"""
        try:
            if self._client:
                self._client.ping()
                return self._client
        except:
            logger.warning("Redis connection lost, attempting reconnect...")
            self._client = None
        
        try:
            return self.connect()
        except:
            return None
    
    def is_connected(self) -> bool:
        """Check if Redis is connected"""
        try:
            client = self.get_client()
            return client is not None and client.ping()
        except:
            return False

# Global Redis client instance
redis_client = RedisClient()

class ChatCache:
    """Chat response caching with Redis"""
    
    @staticmethod
    def _generate_key(query: str, context: Dict) -> str:
        """Generate cache key from query and context using SHA-256"""
        key_data = f"{query}:{json.dumps(context, sort_keys=True)}"
        return f"chat:cache:{hashlib.sha256(key_data.encode()).hexdigest()[:64]}"
    
    @staticmethod
    def get(query: str, context: Dict) -> Optional[str]:
        """Get cached chat response"""
        try:
            client = redis_client.get_client()
            if not client:
                return None
            
            key = ChatCache._generate_key(query, context)
            cached = client.get(key)
            
            if cached:
                logger.debug(f"Chat cache hit: {key}")
                return json.loads(cached)['response']
            return None
        except Exception as e:
            logger.error(f"Chat cache get error: {e}")
            return None
    
    @staticmethod
    def set(query: str, context: Dict, response: str, ttl: int = None):
        """Cache chat response"""
        try:
            client = redis_client.get_client()
            if not client:
                return
            
            key = ChatCache._generate_key(query, context)
            ttl = ttl or RedisConfig.CHAT_CACHE_TTL
            
            data = {
                'query': query,
                'context': context,
                'response': response,
                'cached_at': datetime.now().isoformat()
            }
            
            client.set(key, json.dumps(data), ex=ttl)
            logger.debug(f"Chat response cached: {key}")
        except Exception as e:
            logger.error(f"Chat cache set error: {e}")
    
    @staticmethod
    def invalidate_pattern(pattern: str = "chat:cache:*"):
        """Invalidate chat cache by pattern"""
        try:
            client = redis_client.get_client()
            if not client:
                return
            
            keys = client.keys(pattern)
            if keys:
                client.delete(*keys)
                logger.info(f"Invalidated {len(keys)} chat cache entries")
        except Exception as e:
            logger.error(f"Chat cache invalidation error: {e}")

class RateLimiter:
    """Rate limiting for API endpoints"""
    
    @staticmethod
    def is_allowed(user_id: str, endpoint: str, max_requests: int = 30) -> bool:
        """
        Check if user is within rate limit
        Returns True if allowed, False if rate limited
        """
        try:
            client = redis_client.get_client()
            if not client:
                # If Redis is down, allow the request (fail open)
                return True
            
            key = f"rate_limit:{user_id}:{endpoint}"
            window = RedisConfig.RATE_LIMIT_WINDOW
            
            # Get current count
            current = client.get(key)
            
            if current is None:
                # First request in window
                client.set(key, 1, ex=window)
                return True
            
            current_count = int(current)
            if current_count >= max_requests:
                logger.warning(f"Rate limit exceeded: {user_id} - {endpoint}")
                return False
            
            # Increment counter
            client.incr(key)
            return True
            
        except Exception as e:
            logger.error(f"Rate limiter error: {e}")
            return True  # Fail open
    
    @staticmethod
    def get_remaining(user_id: str, endpoint: str, max_requests: int = 30) -> int:
        """Get remaining requests in current window"""
        try:
            client = redis_client.get_client()
            if not client:
                return max_requests
            
            key = f"rate_limit:{user_id}:{endpoint}"
            current = client.get(key)
            
            if current is None:
                return max_requests
            
            return max(0, max_requests - int(current))
        except Exception as e:
            logger.error(f"Rate limiter get remaining error: {e}")
            return max_requests

class SessionManager:
    """Session management with Redis"""
    
    @staticmethod
    def store_session(session_id: str, data: Dict, ttl: int = None):
        """Store session data"""
        try:
            client = redis_client.get_client()
            if not client:
                return
            
            key = f"session:{session_id}"
            ttl = ttl or RedisConfig.SESSION_TTL
            
            client.set(key, json.dumps(data), ex=ttl)
            logger.debug(f"Session stored: {session_id}")
        except Exception as e:
            logger.error(f"Session store error: {e}")
    
    @staticmethod
    def get_session(session_id: str) -> Optional[Dict]:
        """Get session data"""
        try:
            client = redis_client.get_client()
            if not client:
                return None
            
            key = f"session:{session_id}"
            data = client.get(key)
            
            if data:
                # Refresh TTL on access
                client.expire(key, RedisConfig.SESSION_TTL)
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Session get error: {e}")
            return None
    
    @staticmethod
    def delete_session(session_id: str):
        """Delete session"""
        try:
            client = redis_client.get_client()
            if not client:
                return
            
            key = f"session:{session_id}"
            client.delete(key)
            logger.debug(f"Session deleted: {session_id}")
        except Exception as e:
            logger.error(f"Session delete error: {e}")

class DataCache:
    """General data caching"""
    
    @staticmethod
    def get(key: str) -> Optional[Any]:
        """Get cached data"""
        try:
            client = redis_client.get_client()
            if not client:
                return None
            
            data = client.get(f"data:{key}")
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Data cache get error: {e}")
            return None
    
    @staticmethod
    def set(key: str, data: Any, ttl: int = None):
        """Cache data"""
        try:
            client = redis_client.get_client()
            if not client:
                return
            
            ttl = ttl or RedisConfig.DATA_CACHE_TTL
            client.set(f"data:{key}", json.dumps(data), ex=ttl)
        except Exception as e:
            logger.error(f"Data cache set error: {e}")
    
    @staticmethod
    def delete(key: str):
        """Delete cached data"""
        try:
            client = redis_client.get_client()
            if not client:
                return
            client.delete(f"data:{key}")
        except Exception as e:
            logger.error(f"Data cache delete error: {e}")

def init_redis():
    """Initialize Redis connection on startup"""
    try:
        client = redis_client.connect()
        logger.info("✅ Redis initialized successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Redis initialization failed: {e}")
        logger.warning("⚠️ Application will run without Redis caching")
        return False
