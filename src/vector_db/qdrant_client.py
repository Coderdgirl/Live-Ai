"""
Qdrant Vector Database Client for LiveGuard AI
Handles vector storage and similarity search operations
"""

import os
import json
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http import models

# Load environment variables
load_dotenv()

# Qdrant configuration
QDRANT_HOST = os.getenv('QDRANT_HOST', 'localhost')
QDRANT_PORT = int(os.getenv('QDRANT_PORT', '6333'))
QDRANT_COLLECTION_FINANCIAL = os.getenv('QDRANT_COLLECTION_FINANCIAL', 'financial_vectors')
QDRANT_COLLECTION_SECURITY = os.getenv('QDRANT_COLLECTION_SECURITY', 'security_vectors')

class VectorDBClient:
    """Client for interacting with Qdrant vector database"""
    
    def __init__(self):
        """Initialize the Qdrant client"""
        self.client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        self._ensure_collections_exist()
    
    def _ensure_collections_exist(self):
        """Ensure that required collections exist in Qdrant"""
        # Check and create financial collection if needed
        try:
            self.client.get_collection(QDRANT_COLLECTION_FINANCIAL)
            print(f"Collection {QDRANT_COLLECTION_FINANCIAL} exists")
        except Exception:
            print(f"Creating collection {QDRANT_COLLECTION_FINANCIAL}")
            self.client.create_collection(
                collection_name=QDRANT_COLLECTION_FINANCIAL,
                vectors_config=models.VectorParams(size=7, distance=models.Distance.COSINE),
            )
        
        # Check and create security collection if needed
        try:
            self.client.get_collection(QDRANT_COLLECTION_SECURITY)
            print(f"Collection {QDRANT_COLLECTION_SECURITY} exists")
        except Exception:
            print(f"Creating collection {QDRANT_COLLECTION_SECURITY}")
            self.client.create_collection(
                collection_name=QDRANT_COLLECTION_SECURITY,
                vectors_config=models.VectorParams(size=12, distance=models.Distance.COSINE),
            )
    
    def search_financial_patterns(self, vector, limit=10):
        """Search for similar financial transaction patterns"""
        results = self.client.search(
            collection_name=QDRANT_COLLECTION_FINANCIAL,
            query_vector=vector,
            limit=limit
        )
        
        # Process and return results
        processed_results = []
        for result in results:
            # Parse metadata from payload
            metadata = json.loads(result.payload.get('metadata', '{}'))
            
            processed_results.append({
                'id': result.id,
                'score': result.score,
                'metadata': metadata
            })
        
        return processed_results
    
    def search_security_patterns(self, vector, limit=10):
        """Search for similar security event patterns"""
        results = self.client.search(
            collection_name=QDRANT_COLLECTION_SECURITY,
            query_vector=vector,
            limit=limit
        )
        
        # Process and return results
        processed_results = []
        for result in results:
            # Parse metadata from payload
            metadata = json.loads(result.payload.get('metadata', '{}'))
            
            processed_results.append({
                'id': result.id,
                'score': result.score,
                'metadata': metadata
            })
        
        return processed_results
    
    def get_financial_transaction(self, transaction_id):
        """Get a specific financial transaction by ID"""
        results = self.client.scroll(
            collection_name=QDRANT_COLLECTION_FINANCIAL,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="metadata.transaction_id",
                        match=models.MatchValue(value=transaction_id)
                    )
                ]
            ),
            limit=1
        )
        
        if results[0]:
            record = results[0][0]
            metadata = json.loads(record.payload.get('metadata', '{}'))
            return {
                'id': record.id,
                'vector': record.vector,
                'metadata': metadata
            }
        
        return None
    
    def get_security_event(self, event_id):
        """Get a specific security event by ID"""
        results = self.client.scroll(
            collection_name=QDRANT_COLLECTION_SECURITY,
            scroll_filter=models.Filter(
                must=[
                    models.FieldCondition(
                        key="metadata.event_id",
                        match=models.MatchValue(value=event_id)
                    )
                ]
            ),
            limit=1
        )
        
        if results[0]:
            record = results[0][0]
            metadata = json.loads(record.payload.get('metadata', '{}'))
            return {
                'id': record.id,
                'vector': record.vector,
                'metadata': metadata
            }
        
        return None
    
    def get_financial_anomalies(self, limit=20):
        """Get financial transactions marked as anomalies"""
        results = self.client.scroll(
            collection_name=QDRANT_COLLECTION_FINANCIAL,
            scroll_filter=models.Filter(
                should=[
                    models.FieldCondition(
                        key="metadata.is_flagged",
                        match=models.MatchValue(value=True)
                    ),
                    models.FieldCondition(
                        key="metadata.anomaly_indicator",
                        match=models.IsNotNullCondition()
                    )
                ]
            ),
            limit=limit
        )
        
        processed_results = []
        for record in results[0]:
            metadata = json.loads(record.payload.get('metadata', '{}'))
            processed_results.append({
                'id': record.id,
                'vector': record.vector,
                'metadata': metadata
            })
        
        return processed_results
    
    def get_security_anomalies(self, limit=20):
        """Get security events marked as anomalies"""
        results = self.client.scroll(
            collection_name=QDRANT_COLLECTION_SECURITY,
            scroll_filter=models.Filter(
                should=[
                    models.FieldCondition(
                        key="metadata.is_flagged",
                        match=models.MatchValue(value=True)
                    ),
                    models.FieldCondition(
                        key="metadata.anomaly_indicator",
                        match=models.IsNotNullCondition()
                    ),
                    models.FieldCondition(
                        key="metadata.severity",
                        match=models.MatchValue(value="critical")
                    )
                ]
            ),
            limit=limit
        )
        
        processed_results = []
        for record in results[0]:
            metadata = json.loads(record.payload.get('metadata', '{}'))
            processed_results.append({
                'id': record.id,
                'vector': record.vector,
                'metadata': metadata
            })
        
        return processed_results

if __name__ == "__main__":
    # Simple test of the vector DB client
    client = VectorDBClient()
    print("Vector DB client initialized successfully")
    
    # Test financial anomalies retrieval
    financial_anomalies = client.get_financial_anomalies(limit=5)
    print(f"Retrieved {len(financial_anomalies)} financial anomalies")
    
    # Test security anomalies retrieval
    security_anomalies = client.get_security_anomalies(limit=5)
    print(f"Retrieved {len(security_anomalies)} security anomalies")