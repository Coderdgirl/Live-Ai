"""
Vector database integration for LiveGuard AI
Handles storage and retrieval of vector embeddings
"""
import os
import logging
import numpy as np
from dotenv import load_dotenv
import json
import time
from datetime import datetime

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
VECTOR_DB_HOST = os.getenv('VECTOR_DB_HOST', 'localhost')
VECTOR_DB_PORT = os.getenv('VECTOR_DB_PORT', '6333')
VECTOR_DB_COLLECTION = os.getenv('VECTOR_DB_COLLECTION', 'liveguard_embeddings')
EMBEDDING_DIMENSION = int(os.getenv('EMBEDDING_DIMENSION', '384'))

# In-memory vector store for demo purposes
# In a production environment, this would be replaced with a proper vector database like Qdrant, Milvus, etc.
class InMemoryVectorStore:
    def __init__(self):
        self.vectors = []
        self.metadata = []
        logger.info("Initialized in-memory vector store")
    
    def add_vector(self, vector, metadata):
        """Add a vector and its metadata to the store"""
        self.vectors.append(vector)
        self.metadata.append(metadata)
        logger.info(f"Added vector with metadata: {metadata.get('text_representation', '')[:50]}...")
        return len(self.vectors) - 1  # Return the index as ID
    
    def search(self, query_vector, limit=5):
        """Search for similar vectors"""
        if not self.vectors:
            return []
        
        # Convert to numpy for efficient computation
        query_vector = np.array(query_vector)
        vectors = np.array(self.vectors)
        
        # Compute cosine similarity
        similarities = np.dot(vectors, query_vector) / (
            np.linalg.norm(vectors, axis=1) * np.linalg.norm(query_vector)
        )
        
        # Get top matches
        top_indices = np.argsort(similarities)[-limit:][::-1]
        
        results = []
        for idx in top_indices:
            results.append({
                'id': idx,
                'similarity': float(similarities[idx]),
                'metadata': self.metadata[idx]
            })
        
        return results
    
    def get_all(self, limit=100):
        """Get all vectors and metadata"""
        result = []
        for i, (vector, metadata) in enumerate(zip(self.vectors, self.metadata)):
            if i >= limit:
                break
            result.append({
                'id': i,
                'vector': vector,
                'metadata': metadata
            })
        return result

# Singleton instance
_vector_store = None

def get_vector_store():
    """Get or initialize the vector store"""
    global _vector_store
    if _vector_store is None:
        _vector_store = InMemoryVectorStore()
    return _vector_store

def store_embedding(embedding, metadata):
    """Store an embedding vector with metadata"""
    store = get_vector_store()
    
    # Ensure embedding is a list
    if isinstance(embedding, np.ndarray):
        embedding = embedding.tolist()
    
    # Add timestamp if not present
    if 'timestamp' not in metadata:
        metadata['timestamp'] = datetime.now().isoformat()
    
    # Store in vector database
    vector_id = store.add_vector(embedding, metadata)
    
    return vector_id

def search_similar(query_embedding, limit=5):
    """Search for similar embeddings"""
    store = get_vector_store()
    
    # Ensure query is a list
    if isinstance(query_embedding, np.ndarray):
        query_embedding = query_embedding.tolist()
    
    # Search vector database
    results = store.search(query_embedding, limit=limit)
    
    return results

def get_recent_embeddings(limit=100):
    """Get recent embeddings for visualization"""
    store = get_vector_store()
    return store.get_all(limit=limit)