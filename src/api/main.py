"""
FastAPI Server for LiveGuard AI
Provides API endpoints for frontend integration
"""

import os
import sys
import json
import os
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.csv_utils import financial_data_to_csv, security_data_to_csv, generate_filename
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from vector_db.qdrant_client import VectorDBClient

# Load environment variables
load_dotenv()

# API configuration
API_HOST = os.getenv('API_HOST', 'localhost')
API_PORT = int(os.getenv('API_PORT', '8000'))
FRONTEND_URL = os.getenv('FRONTEND_URL', 'http://localhost:3000')

# Initialize FastAPI app
app = FastAPI(
    title="LiveGuard AI API",
    description="Real-Time Intelligence for Finance, Security, and Beyond",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[FRONTEND_URL],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize vector DB client
vector_db = VectorDBClient()

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# Pydantic models for API
class FinancialTransaction(BaseModel):
    transaction_id: str
    timestamp: str
    account_id: str
    transaction_type: str
    amount: float
    currency: str
    location: Optional[str] = None
    device_id: Optional[str] = None
    is_flagged: bool = False
    anomaly_indicator: Optional[str] = None

class SecurityEvent(BaseModel):
    event_id: str
    timestamp: str
    event_type: str
    user_id: str
    source_ip: str
    resource_id: Optional[str] = None
    severity: str
    status: str
    is_flagged: bool = False
    anomaly_indicator: Optional[str] = None

class SearchQuery(BaseModel):
    vector: List[float]
    limit: int = 10

# API routes
@app.get("/")
async def root():
    return {"message": "Welcome to LiveGuard AI API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy"}

# Financial data endpoints
@app.get("/financial/anomalies", response_model=List[dict])
async def get_financial_anomalies(limit: int = 20):
    """Get financial transactions marked as anomalies"""
    anomalies = vector_db.get_financial_anomalies(limit=limit)
    return anomalies

@app.get("/financial/anomalies/csv")
async def get_financial_anomalies_csv(limit: int = 20):
    """Get financial transactions marked as anomalies in CSV format"""
    anomalies = vector_db.get_financial_anomalies(limit=limit)
    csv_data = financial_data_to_csv(anomalies)
    filename = generate_filename("financial_anomalies")
    
    response = Response(content=csv_data)
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = "text/csv"
    return response

@app.get("/financial/transaction/{transaction_id}")
async def get_financial_transaction(transaction_id: str):
    """Get a specific financial transaction by ID"""
    transaction = vector_db.get_financial_transaction(transaction_id)
    if transaction:
        return transaction
    return {"error": "Transaction not found"}

@app.post("/financial/search")
async def search_financial_patterns(query: SearchQuery):
    """Search for similar financial transaction patterns"""
    results = vector_db.search_financial_patterns(query.vector, limit=query.limit)
    return results

# Security data endpoints
@app.get("/security/anomalies", response_model=List[dict])
async def get_security_anomalies(limit: int = 20):
    """Get security events marked as anomalies"""
    anomalies = vector_db.get_security_anomalies(limit=limit)
    return anomalies

@app.get("/security/anomalies/csv")
async def get_security_anomalies_csv(limit: int = 20):
    """Get security events marked as anomalies in CSV format"""
    anomalies = vector_db.get_security_anomalies(limit=limit)
    csv_data = security_data_to_csv(anomalies)
    filename = generate_filename("security_anomalies")
    
    response = Response(content=csv_data)
    response.headers["Content-Disposition"] = f"attachment; filename={filename}"
    response.headers["Content-Type"] = "text/csv"
    return response

@app.get("/security/event/{event_id}")
async def get_security_event(event_id: str):
    """Get a specific security event by ID"""
    event = vector_db.get_security_event(event_id)
    if event:
        return event
    return {"error": "Event not found"}

@app.post("/security/search")
async def search_security_patterns(query: SearchQuery):
    """Search for similar security event patterns"""
    results = vector_db.search_security_patterns(query.vector, limit=query.limit)
    return results

# WebSocket for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            # Echo back for now, in production would process commands
            await websocket.send_text(f"Message received: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    print(f"Starting LiveGuard AI API on {API_HOST}:{API_PORT}")
    uvicorn.run("main:app", host=API_HOST, port=API_PORT, reload=True)