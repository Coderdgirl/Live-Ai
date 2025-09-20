from fastapi import FastAPI, WebSocket
import random

app = FastAPI()

async def ai_generate_answer(question: str) -> str:
    # Simple simulated AI/ML logic for demonstration
    responses = {
        "hello": ["Hello! How can I assist you today?", "Hi there! What can I do for you?"],
        "finance": ["Finance involves managing money and investments.", "Would you like to know about personal finance or corporate finance?"],
        "security": ["Security is crucial for protecting assets and information.", "Are you interested in cybersecurity or physical security?"]
    }
    key = question.lower().strip()
    for topic in responses:
        if topic in key:
            return random.choice(responses[topic])
    return "I'm an AI assistant. Could you please rephrase or ask another question?"

@app.websocket("/ws/chatbot")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            answer = await ai_generate_answer(data)
            await websocket.send_text(answer)
    except Exception as e:
        await websocket.close()
        print(f"Error: {e}")