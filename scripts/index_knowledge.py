#!/usr/bin/env python3
"""
Knowledge Base Indexing Script
Indexes all educational documents into Redis Vector Store for RAG
Usage: python scripts/index_knowledge.py
"""
import json
import os
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging

from rag_engine import init_rag, rag_engine
from redis_client import init_redis

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_knowledge_documents():
    """Load all documents from knowledge_base/"""
    docs = []
    kb_path = Path("knowledge_base")
    
    if not kb_path.exists():
        logger.error(f"Knowledge base directory not found: {kb_path}")
        return docs
    
    for category_dir in kb_path.iterdir():
        if category_dir.is_dir():
            for file in category_dir.glob("*.json"):
                try:
                    with open(file, 'r', encoding='utf-8') as f:
                        doc = json.load(f)
                        doc['category'] = category_dir.name
                        docs.append(doc)
                        logger.debug(f"Loaded document: {doc.get('id', file.stem)}")
                except Exception as e:
                    logger.error(f"Error loading {file}: {e}")
    
    return docs

def index_documents():
    """Index all documents into Redis Vector Store"""
    logger.info("=" * 60)
    logger.info("Fintra Knowledge Base Indexing")
    logger.info("=" * 60)
    
    # Initialize Redis and RAG
    logger.info("\n1. Initializing Redis connection...")
    if not init_redis():
        logger.error("❌ Redis initialization failed. Exiting.")
        return False
    
    logger.info("\n2. Initializing RAG engine...")
    if not init_rag():
        logger.error("❌ RAG engine initialization failed. Exiting.")
        return False
    
    # Load documents
    logger.info("\n3. Loading knowledge documents...")
    documents = load_knowledge_documents()
    
    if not documents:
        logger.error("❌ No documents found to index.")
        return False
    
    logger.info(f"✅ Loaded {len(documents)} documents")
    
    # Clear existing index
    logger.info("\n4. Clearing existing index...")
    rag_engine.clear_index()
    
    # Index documents
    logger.info("\n5. Indexing documents...")
    success_count = 0
    failed_count = 0
    
    for i, doc in enumerate(documents, 1):
        try:
            doc_id = doc.get('id', f"doc_{i}")
            title = doc.get('title', 'Untitled')
            content = doc.get('content', '')
            category = doc.get('category', 'general')
            tags = doc.get('tags', [])
            
            logger.info(f"  [{i}/{len(documents)}] Indexing: {title}")
            
            if rag_engine.add_document(
                doc_id=doc_id,
                content=content,
                title=title,
                category=category,
                tags=tags
            ):
                success_count += 1
            else:
                failed_count += 1
                logger.warning(f"  ⚠️ Failed to index: {title}")
                
        except Exception as e:
            failed_count += 1
            logger.error(f"  ❌ Error indexing document {i}: {e}")
    
    # Show stats
    logger.info("\n" + "=" * 60)
    logger.info("Indexing Complete!")
    logger.info("=" * 60)
    logger.info(f"✅ Successfully indexed: {success_count} documents")
    logger.info(f"❌ Failed to index: {failed_count} documents")
    
    # Get and display index stats
    stats = rag_engine.get_stats()
    logger.info("\n📊 Index Statistics:")
    logger.info(f"  - Index name: {stats.get('index_name', 'N/A')}")
    logger.info(f"  - Total documents: {stats.get('document_count', 'N/A')}")
    logger.info(f"  - Vector dimension: {stats.get('vector_dimension', 'N/A')}")
    logger.info(f"  - Similarity threshold: {stats.get('similarity_threshold', 'N/A')}")
    
    logger.info("\n✨ Knowledge base is ready for RAG queries!")
    logger.info("=" * 60)
    
    return success_count > 0

def test_search():
    """Test the search functionality"""
    logger.info("\n🧪 Testing search functionality...")
    
    test_queries = [
        "What is RSI?",
        "How does MACD work?",
        "Explain support and resistance",
        "What are SEBI regulations?",
        "How to manage risk in trading?"
    ]
    
    for query in test_queries:
        logger.info(f"\n  Query: '{query}'")
        results = rag_engine.search(query, top_k=2)
        
        if results:
            for i, doc in enumerate(results, 1):
                logger.info(f"    [{i}] {doc['title']} (similarity: {doc['similarity']:.2%})")
        else:
            logger.info("    No results found")

if __name__ == "__main__":
    try:
        if index_documents():
            # Optionally test the search
            test_search()
            sys.exit(0)
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\n\n⚠️ Indexing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n❌ Unexpected error: {e}")
        sys.exit(1)
