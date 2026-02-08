"""
RAG (Retrieval-Augmented Generation) Engine
Implements vector search using Redis for knowledge retrieval
"""
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from redis_client import RedisConfig, redis_client

logger = logging.getLogger(__name__)

# Try lightweight embedding libraries in order of preference
EMBEDDING_BACKEND = None

# Option 1: FastEmbed (lightweight, ~10MB, no PyTorch)
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

class RAGEngine:
    """
    Retrieval-Augmented Generation Engine
    Uses Redis for vector storage and similarity search
    """
    
    def __init__(self):
        self.model = None
        self._load_model()
        self.index_name = RedisConfig.VECTOR_INDEX_NAME
        self.vector_dim = RedisConfig.VECTOR_DIM
        self.similarity_threshold = RedisConfig.SIMILARITY_THRESHOLD
        
    def _load_model(self):
        """Load the embedding model (FastEmbed or sentence-transformers)"""
        if not NUMPY_AVAILABLE:
            logger.warning("NumPy not available. RAG features disabled.")
            self.model = None
            return
            
        if EMBEDDING_BACKEND is None:
            logger.warning("No embedding library available. Install fastembed or sentence-transformers.")
            self.model = None
            return
            
        try:
            logger.info(f"Loading embedding model (backend: {EMBEDDING_BACKEND})...")
            
            if EMBEDDING_BACKEND == 'fastembed':
                # FastEmbed - lightweight, no PyTorch
                self.model = TextEmbedding(model_name="BAAI/bge-small-en-v1.5")
                logger.info("✅ FastEmbed model loaded successfully")
            elif EMBEDDING_BACKEND == 'sentence_transformers':
                # sentence-transformers - heavy fallback
                self.model = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("✅ SentenceTransformer model loaded successfully")
            else:
                self.model = None
                
        except Exception as e:
            logger.error(f"❌ Failed to load embedding model: {e}")
            self.model = None
    
    def embed_text(self, text: str) -> Optional[List[float]]:
        """Generate embedding vector for text"""
        if not self.model or not NUMPY_AVAILABLE:
            logger.error("Embedding model not available")
            return None
        
        try:
            if EMBEDDING_BACKEND == 'fastembed':
                # FastEmbed returns generator, convert to list
                embedding = list(self.model.embed([text]))[0]
                return embedding.tolist() if hasattr(embedding, 'tolist') else list(embedding)
            elif EMBEDDING_BACKEND == 'sentence_transformers':
                # sentence-transformers
                embedding = self.model.encode(text, convert_to_tensor=False)
                return embedding.tolist()
            else:
                return None
        except Exception as e:
            logger.error(f"Error generating embedding: {e}")
            return None
    
    def embed_query(self, query: str) -> Optional[List[float]]:
        """Alias for embed_text - for query embedding"""
        return self.embed_text(query)
    
    def create_index(self):
        """Create Redis vector search index"""
        try:
            client = redis_client.get_client()
            if not client:
                logger.error("Redis not connected")
                return False
            
            # Check if index already exists
            try:
                client.execute_command("FT.INFO", self.index_name)
                logger.info(f"Vector index '{self.index_name}' already exists")
                return True
            except:
                pass  # Index doesn't exist, create it
            
            # Create vector index schema
            schema = [
                "FT.CREATE", self.index_name,
                "ON", "HASH",
                "PREFIX", "1", "doc:",
                "SCHEMA",
                "content", "TEXT",
                "title", "TEXT",
                "category", "TAG",
                "tags", "TAG",
                "embedding", "VECTOR", "FLAT",
                "6",
                "TYPE", "FLOAT32",
                "DIM", str(self.vector_dim),
                "DISTANCE_METRIC", "COSINE"
            ]
            
            client.execute_command(*schema)
            logger.info(f"✅ Vector index '{self.index_name}' created successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creating vector index: {e}")
            return False
    
    def add_document(self, doc_id: str, content: str, title: str = "", 
                     category: str = "general", tags: List[str] = None,
                     metadata: Dict = None):
        """Add a document to the vector index"""
        if not NUMPY_AVAILABLE or not self.model:
            logger.warning("Cannot add document: numpy or sentence-transformers not available")
            return False
            
        try:
            client = redis_client.get_client()
            if not client:
                return False
            
            # Generate embedding
            embedding = self.embed_text(content)
            if not embedding:
                return False
            
            # Convert embedding to bytes
            embedding_bytes = np.array(embedding, dtype=np.float32).tobytes()
            
            # Prepare document data
            doc_key = f"doc:{doc_id}"
            doc_data = {
                "content": content,
                "title": title or doc_id,
                "category": category,
                "tags": ",".join(tags) if tags else "",
                "embedding": embedding_bytes
            }
            
            if metadata:
                doc_data["metadata"] = json.dumps(metadata)
            
            # Store in Redis
            client.hset(doc_key, mapping=doc_data)
            logger.debug(f"Document indexed: {doc_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error adding document {doc_id}: {e}")
            return False
    
    def search(self, query: str, top_k: int = 3) -> List[Dict]:
        """
        Search for similar documents
        Returns list of documents with similarity scores
        """
        if not NUMPY_AVAILABLE or not self.model:
            logger.warning("Cannot search: numpy or sentence-transformers not available")
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
            
            # Convert to bytes
            query_bytes = np.array(query_embedding, dtype=np.float32).tobytes()
            
            # Perform vector search
            search_query = [
                "FT.SEARCH", self.index_name,
                "*=>[KNN {} @embedding $vec AS score]".format(top_k),
                "PARAMS", "2", "vec", query_bytes,
                "RETURN", "4", "content", "title", "category", "score",
                "SORTBY", "score", "ASC",
                "DIALECT", "2"
            ]
            
            results = client.execute_command(*search_query)
            
            # Parse results
            documents = []
            if results and len(results) > 1:
                # Skip the first element (total count)
                for i in range(1, len(results), 2):
                    if i + 1 < len(results):
                        doc_key = results[i]
                        fields = results[i + 1]
                        
                        # Convert fields list to dict
                        field_dict = {}
                        for j in range(0, len(fields), 2):
                            if j + 1 < len(fields):
                                field_dict[fields[j]] = fields[j + 1]
                        
                        # Calculate similarity (1 - distance for cosine)
                        score = float(field_dict.get('score', 1.0))
                        similarity = 1 - score
                        
                        # Only include if above threshold
                        if similarity >= self.similarity_threshold:
                            documents.append({
                                'id': doc_key.replace('doc:', ''),
                                'content': field_dict.get('content', ''),
                                'title': field_dict.get('title', ''),
                                'category': field_dict.get('category', 'general'),
                                'similarity': similarity
                            })
            
            logger.debug(f"Search found {len(documents)} relevant documents")
            return documents
            
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
            
            # Count documents
            doc_count = len(client.keys("doc:*"))
            
            return {
                "index_name": self.index_name,
                "document_count": doc_count,
                "vector_dimension": self.vector_dim,
                "similarity_threshold": self.similarity_threshold
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
            
            doc_keys = client.keys("doc:*")
            if doc_keys:
                client.delete(*doc_keys)
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
