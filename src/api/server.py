"""
FastAPI server for LiveGuard AI
Provides API endpoints for analytics and alerts
"""
import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from dotenv import load_dotenv

# Import vector store
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from models.vector_store import search_similar, get_recent_embeddings

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
API_HOST = os.getenv('API_HOST', '0.0.0.0')
API_PORT = int(os.getenv('API_PORT', '8000'))

# Create FastAPI app
app = FastAPI(title="LiveGuard AI API", description="Real-time intelligence API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development only, restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory store for alerts
alerts = []

# Models
class Alert(BaseModel):
    id: str
    timestamp: str
    type: str
    severity: str
    message: str
    data: Dict[str, Any]

class SearchQuery(BaseModel):
    query: List[float]
    limit: Optional[int] = 5

# Routes
@app.get("/")
async def root():
    return {"message": "LiveGuard AI API is running"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/alerts", response_model=List[Alert])
async def get_alerts(limit: int = 10):
    """Get recent alerts"""
    return alerts[-limit:]

@app.post("/alerts")
async def create_alert(alert: Alert):
    """Create a new alert"""
    alerts.append(alert)
    logger.info(f"New alert created: {alert.message}")
    return {"status": "success", "id": alert.id}

@app.post("/search")
async def search(query: SearchQuery):
    """Search for similar vectors"""
    try:
        results = search_similar(query.query, limit=query.limit)
        return {"results": results}
    except Exception as e:
        logger.error(f"Search error: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/embeddings")
async def get_embeddings(limit: int = 100):
    """Get recent embeddings for visualization"""
    try:
        embeddings = get_recent_embeddings(limit=limit)
        return {"embeddings": embeddings}
    except Exception as e:
        logger.error(f"Error getting embeddings: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def start_server():
    """Start the FastAPI server"""
    logger.info(f"Starting API server at http://{API_HOST}:{API_PORT}")
    uvicorn.run(app, host=API_HOST, port=API_PORT)

if __name__ == "__main__":
    # Configure logging when run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    start_server()