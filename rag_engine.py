"""
RAG (Retrieval-Augmented Generation) Engine
Implements vector search using Redis for knowledge retrieval

NOTE: Upstash REST client does NOT support RediSearch (FT.CREATE, FT.SEARCH)
or low-level Redis commands (execute_command, scan_iter, hset with bytes).
This engine uses a pure-Python cosine similarity approach instead, storing
document embeddings as JSON and performing similarity search in application code.
"""
import json
import logging
import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from redis_client import RedisConfig, redis_client

logger = logging.getLogger(__name__)

# Try lightweight embedding libraries in order of preference
EMBEDDING_BACKEND = None

# Option 0: Gemini API via new google.genai SDK (Zero local memory, prioritize on free tier)
if os.getenv('GEMINI_API_KEY'):
    try:
        from google import genai as google_genai
        EMBEDDING_BACKEND = 'gemini'
        logger.info("✅ google.genai SDK available - using 0-RAM Gemini embeddings")
    except ImportError:
        logger.warning("google-genai package not installed, trying legacy google-generativeai")
        try:
            import google.generativeai as genai_legacy
            genai_legacy.configure(api_key=os.getenv('GEMINI_API_KEY'))
            EMBEDDING_BACKEND = 'gemini_legacy'
            logger.info("✅ Legacy google.generativeai configured (consider upgrading to google-genai)")
        except ImportError:
            pass

# Option 1: FastEmbed (lightweight, ~200MB, no PyTorch)
if EMBEDDING_BACKEND is None:
    try:
        from fastembed import TextEmbedding
        EMBEDDING_BACKEND = 'fastembed'
        logger.info("✅ FastEmbed available - using lightweight embeddings")
    except ImportError:
        pass

# Option 2: sentence-transformers (heavy, fallback)
if EMBEDDING_BACKEND is None:
    try:
        from sentence_transformers import SentenceTransformer
        EMBEDDING_BACKEND = 'sentence_transformers'
        logger.info("✅ sentence-transformers available")
    except ImportError:
        pass

if EMBEDDING_BACKEND is None:
    logger.warning("⚠️ No embedding library available. RAG features disabled. Install fastembed for lightweight option.")

# numpy is required for all backends
try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    NUMPY_AVAILABLE = False
    logger.error("❌ NumPy not available - RAG features disabled")


def _cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    """Pure-Python cosine similarity (no numpy required)."""
    dot = sum(a * b for a, b in zip(vec_a, vec_b))
    norm_a = math.sqrt(sum(a * a for a in vec_a))
    norm_b = math.sqrt(sum(b * b for b in vec_b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


class RAGEngine:
    """
    Retrieval-Augmented Generation Engine
    Uses Redis for document storage and pure-Python cosine similarity search.
    Compatible with Upstash REST client (no RediSearch commands needed).
    """
    
    def __init__(self):
        self.model = None
        self.backend = EMBEDDING_BACKEND
        # Lazy Loading: DO NOT call self._load_model() here!
        self.index_name = RedisConfig.VECTOR_INDEX_NAME
        self.vector_dim = RedisConfig.VECTOR_DIM
        self.similarity_threshold = RedisConfig.SIMILARITY_THRESHOLD
        
    def _load_model(self):
        """Lazy load the embedding model"""
        if self.model is not None:
            return
            
        if self.backend == 'gemini':
            self.model = 'gemini_api'
            return
        
        if self.backend == 'gemini_legacy':
            self.model = 'gemini_legacy_api'
            return
            
        if not NUMPY_AVAILABLE:
            logger.warning("NumPy not available. RAG features disabled.")
            return
            
        if self.backend is None:
            logger.warning("No embedding library available.")
            return
            
        try:
            logger.info(f"Loading embedding model (backend: {self.backend})...")
            
            if self.backend == 'fastembed':
                # FastEmbed - lightweight, no PyTorch
                self.model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
                logger.info("✅ FastEmbed model loaded successfully")
            elif self.backend == 'sentence_transformers':
                # sentence-transformers - heavy fallback
                self.model = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("✅ SentenceTransformer model loaded successfully")
                
        except Exception as e:
            logger.error(f"❌ Failed to load embedding model: {e}")
            self.model = None
    
    def embed_text(self, text: str) -> Optional[List[float]]:
        """Generate embedding vector for text"""
        self._load_model()
        if not self.model:
            logger.error("Embedding model not available")
            return None
        
        try:
            if self.backend == 'gemini':
                client = google_genai.Client(api_key=os.getenv('GEMINI_API_KEY'))
                response = client.models.embed_content(
                    model='gemini-embedding-001',
                    contents=text,
                )
                return list(response.embeddings[0].values)
            elif self.backend == 'gemini_legacy':
                import google.generativeai as genai_legacy
                result = genai_legacy.embed_content(
                    model="models/embedding-001",
                    content=text,
                    task_type="retrieval_document"
                )
                return result['embedding']
            elif self.backend == 'fastembed':
                # FastEmbed returns generator, convert to list
                embedding = list(self.model.embed([text]))[0]
                return embedding.tolist() if hasattr(embedding, 'tolist') else list(embedding)
            elif self.backend == 'sentence_transformers':
                # sentence-transformers
                embedding = self.model.encode(text, convert_to_tensor=False)
                return embedding.tolist()
            else:
                return None
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return None
    
    def embed_query(self, query: str) -> Optional[List[float]]:
        """Generate embedding vector for a query"""
        self._load_model()
        if self.backend == 'gemini':
            try:
                client = google_genai.Client(api_key=os.getenv('GEMINI_API_KEY'))
                response = client.models.embed_content(
                    model='gemini-embedding-001',
                    contents=query,
                )
                return list(response.embeddings[0].values)
            except Exception as e:
                logger.error(f"Error generating Gemini query embedding: {e}")
                return None
        elif self.backend == 'gemini_legacy':
            try:
                import google.generativeai as genai_legacy
                result = genai_legacy.embed_content(
                    model="models/embedding-001",
                    content=query,
                    task_type="retrieval_query"
                )
                return result['embedding']
            except Exception as e:
                logger.error(f"Error generating legacy Gemini query embedding: {e}")
                return None
        
        # For other backends, query embedding == text embedding
        return self.embed_text(query)
    
    def create_index(self):
        """
        'Create' the index — for Upstash REST, this just verifies Redis connectivity.
        No RediSearch FT.CREATE is needed since we use pure-Python cosine similarity.
        """
        try:
            client = redis_client.get_client()
            if not client:
                logger.error("Redis not connected")
                return False
            
            # Verify connectivity with a simple ping
            client.ping()
            logger.info(f"✅ RAG index '{self.index_name}' ready (pure-Python cosine similarity mode)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error verifying Redis for RAG: {e}")
            return False
    
    def add_document(self, doc_id: str, content: str, title: str = "", 
                     category: str = "general", tags: List[str] = None,
                     metadata: Dict = None):
        """
        Add a document to the vector index.
        Stores embedding as a JSON list (compatible with Upstash REST).
        """
        if not self.backend:
            logger.warning("Cannot add document: no embedding backend available")
            return False
            
        try:
            client = redis_client.get_client()
            if not client:
                return False
            
            # Generate embedding
            embedding = self.embed_text(content)
            if not embedding:
                return False
            
            # Store as JSON (Upstash REST compatible — no raw bytes)
            doc_key = f"doc:{doc_id}"
            doc_data = {
                "content": content,
                "title": title or doc_id,
                "category": category,
                "tags": ",".join(tags) if tags else "",
                "embedding": embedding,  # stored as JSON list
            }
            
            if metadata:
                doc_data["metadata"] = metadata
            
            # Store the whole document as a single JSON string
            client.set(doc_key, json.dumps(doc_data))
            logger.debug(f"Document indexed: {doc_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding document {doc_id}: {e}")
            return False
    
    def _get_all_doc_keys(self, client) -> List[str]:
        """Get all document keys, compatible with both standard Redis and Upstash REST."""
        try:
            # Try scan_iter first (standard redis-py)
            if hasattr(client, 'scan_iter'):
                return list(client.scan_iter(match="doc:*", count=100))
            
            # Fallback: use SCAN command via Upstash REST
            keys = []
            cursor = 0
            while True:
                result = client.scan(cursor, match="doc:*", count=100)
                cursor, batch = result
                keys.extend(batch)
                if cursor == 0:
                    break
            return keys
        except Exception as e:
            logger.error(f"Error scanning doc keys: {e}")
            return []

    def search(self, query: str, top_k: int = 3) -> List[Dict]:
        """
        Search for similar documents using pure-Python cosine similarity.
        Returns list of documents with similarity scores.
        """
        if not self.backend:
            logger.warning("Cannot search: no embedding backend available")
            return []
            
        try:
            client = redis_client.get_client()
            if not client:
                logger.warning("Redis not available, returning empty results")
                return []
            
            # Generate query embedding
            query_embedding = self.embed_query(query)
            if not query_embedding:
                return []
            
            # Get all document keys
            doc_keys = self._get_all_doc_keys(client)
            
            if not doc_keys:
                logger.debug("No documents in index")
                return []
            
            # Score each document
            scored_docs = []
            for key in doc_keys:
                try:
                    raw = client.get(key)
                    if not raw:
                        continue
                    
                    doc_data = json.loads(raw) if isinstance(raw, str) else json.loads(raw.decode())
                    doc_embedding = doc_data.get('embedding')
                    if not doc_embedding:
                        continue
                    
                    similarity = _cosine_similarity(query_embedding, doc_embedding)
                    
                    if similarity >= self.similarity_threshold:
                        scored_docs.append({
                            'id': key.replace('doc:', '') if isinstance(key, str) else key.decode().replace('doc:', ''),
                            'content': doc_data.get('content', ''),
                            'title': doc_data.get('title', ''),
                            'category': doc_data.get('category', 'general'),
                            'similarity': similarity
                        })
                except Exception as e:
                    logger.debug(f"Error processing doc {key}: {e}")
                    continue
            
            # Sort by similarity descending, take top_k
            scored_docs.sort(key=lambda d: d['similarity'], reverse=True)
            results = scored_docs[:top_k]
            
            logger.debug(f"Search found {len(results)} relevant documents")
            return results
            
        except Exception as e:
            logger.error(f"Error in vector search: {e}")
            return []
    
    def assemble_context(self, query: str, retrieved_docs: List[Dict]) -> str:
        """Build context string from retrieved documents"""
        if not retrieved_docs:
            return ""
        
        context_parts = ["\n=== RELEVANT KNOWLEDGE BASE ===\n"]
        
        for i, doc in enumerate(retrieved_docs, 1):
            context_parts.append(
                f"[{i}] {doc['title']} (Category: {doc['category']}, Relevance: {doc['similarity']:.2%})\n"
                f"{doc['content']}\n"
            )
        
        context_parts.append("=== END KNOWLEDGE BASE ===\n")
        return "\n".join(context_parts)
    
    def get_stats(self) -> Dict:
        """Get index statistics"""
        try:
            client = redis_client.get_client()
            if not client:
                return {"error": "Redis not connected"}
            
            doc_keys = self._get_all_doc_keys(client)
            
            return {
                "index_name": self.index_name,
                "document_count": len(doc_keys),
                "vector_dimension": self.vector_dim,
                "similarity_threshold": self.similarity_threshold,
                "backend": self.backend or "none"
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {"error": str(e)}
    
    def delete_document(self, doc_id: str):
        """Delete a document from the index"""
        try:
            client = redis_client.get_client()
            if not client:
                return False
            
            doc_key = f"doc:{doc_id}"
            client.delete(doc_key)
            logger.debug(f"Document deleted: {doc_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting document {doc_id}: {e}")
            return False
    
    def clear_index(self):
        """Clear all documents from the index"""
        try:
            client = redis_client.get_client()
            if not client:
                return False
            
            doc_keys = self._get_all_doc_keys(client)
            for key in doc_keys:
                try:
                    client.delete(key)
                except Exception:
                    pass
            
            logger.info(f"Cleared {len(doc_keys)} documents from index")
            return True
        except Exception as e:
            logger.error(f"Error clearing index: {e}")
            return False

# Global RAG engine instance
rag_engine = RAGEngine()

def init_rag():
    """Initialize RAG engine on startup"""
    try:
        # Create vector index
        if rag_engine.create_index():
            logger.info("✅ RAG engine initialized successfully")
            return True
        else:
            logger.warning("⚠️ RAG engine initialization failed")
            return False
    except Exception as e:
        logger.error(f"❌ RAG initialization error: {e}")
        return False
