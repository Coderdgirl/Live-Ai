import os
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import threading
from typing import List

# Load environment variables
load_dotenv()

# --- Connection Manager for WebSocket ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# --- FastAPI App ---
app = FastAPI()

# CORS Middleware
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API Routes ---
@app.get("/")
def read_root():
    return {"message": "LiveGuard AI Backend is running"}

@app.get("/download/financial_anomalies")
def download_financial_anomalies():
    return FileResponse("financial_anomalies.csv", media_type='text/csv', filename="financial_anomalies.csv")

@app.get("/download/security_events")
def download_security_events():
    return FileResponse("security_events.csv", media_type='text/csv', filename="security_events.csv")

# --- WebSocket Endpoint ---
@app.websocket("/ws/chatbot")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    print("A client has connected.")
    await manager.send_personal_message("You are connected to the chatbot.", websocket)
    try:
        while True:
            data = await websocket.receive_text()
            print(f"Received message: {data}")
            response = f"Bot says: You wrote '{data}'"
            await manager.send_personal_message(response, websocket)
            print(f"Sent response: {response}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("A client has left the chat")

# --- Main Entry Point ---
if __name__ == "__main__":
    host = os.getenv("BACKEND_HOST", "127.0.0.1")
    port = int(os.getenv("BACKEND_PORT", 8000))
    uvicorn.run(app, host=host, port=port)