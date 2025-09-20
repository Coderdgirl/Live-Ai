"""
Pathway data processing pipeline for LiveGuard AI
Processes real-time data streams and performs analytics
"""
import os
import json
import logging
import pathway as pw
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
import numpy as np

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'liveguard-data')
EMBEDDING_MODEL = os.getenv('EMBEDDING_MODEL', 'all-MiniLM-L6-v2')
ALERT_THRESHOLD = float(os.getenv('ALERT_THRESHOLD', '0.85'))
ALERT_WINDOW_SECONDS = int(os.getenv('ALERT_WINDOW_SECONDS', '300'))

# Global embedding model
embedding_model = None

def get_embedding_model():
    """Get or initialize the embedding model"""
    global embedding_model
    if embedding_model is None:
        logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
        embedding_model = SentenceTransformer(EMBEDDING_MODEL)
    return embedding_model

def generate_embedding(text):
    """Generate embedding vector for text"""
    model = get_embedding_model()
    return model.encode(text)

def text_to_embedding(data):
    """Convert data to text and generate embedding"""
    # Create a text representation of the data
    if data.get('type') == 'financial':
        text = (f"Transaction: {data.get('transaction_type', '')} "
                f"Amount: {data.get('amount', 0)} "
                f"Merchant: {data.get('merchant', '')} "
                f"Country: {data.get('country', '')} "
                f"User: {data.get('user_id', '')}")
    elif data.get('type') == 'security':
        text = (f"Security event: {data.get('event_type', '')} "
                f"Status: {data.get('status', '')} "
                f"IP: {data.get('ip_address', '')} "
                f"User: {data.get('user_id', '')}")
    else:
        text = json.dumps(data)
    
    # Generate embedding
    embedding = generate_embedding(text)
    
    # Add embedding to data
    data['embedding'] = embedding.tolist()
    data['text_representation'] = text
    
    return data

def start_pipeline():
    """Start the Pathway data processing pipeline"""
    logger.info("Starting Pathway pipeline...")
    
    # Initialize Pathway
    pw.debug.set_log_level("INFO")
    
    # Define Kafka input connector
    input_data = pw.io.kafka.read(
        servers=KAFKA_BOOTSTRAP_SERVERS,
        topics=[KAFKA_TOPIC],
        value_deserializer=json.loads,
        autocommit=True,
        group_id="liveguard-pathway",
        auto_offset_reset="latest"
    )
    
    # Process data stream
    processed_data = input_data.select(
        pw.this.value,
        timestamp=pw.this.timestamp
    )
    
    # Apply embeddings
    embedded_data = processed_data.select(
        data=pw.apply(text_to_embedding, pw.this.value),
        timestamp=pw.this.timestamp
    )
    
    # Split by data type
    financial_data = embedded_data.filter(pw.this.data["type"] == "financial")
    security_data = embedded_data.filter(pw.this.data["type"] == "security")
    
    # Detect anomalies based on embeddings
    def detect_anomalies(data_stream, window_seconds=ALERT_WINDOW_SECONDS):
        # Time window for anomaly detection
        windowed_data = data_stream.windowby(
            pw.temporal.tumbling(duration=f"{window_seconds}s")
        ).select(
            data=pw.this.data,
            count=pw.count(),
            timestamp=pw.this.timestamp
        )
        
        # Detect anomalies
        return windowed_data.filter(
            # Filter known anomalies or detect new ones
            (pw.this.data["is_anomaly"] == True) | 
            (pw.apply(lambda x: x.get("amount", 0) > 5000 if x.get("type") == "financial" else False, pw.this.data))
        )
    
    # Apply anomaly detection
    financial_anomalies = detect_anomalies(financial_data)
    security_anomalies = detect_anomalies(security_data)
    
    # Combine anomalies
    all_anomalies = pw.union(financial_anomalies, security_anomalies)
    
    # Output to vector database
    all_anomalies.sink(
        pw.io.python.write(lambda data: store_vector_embedding(data))
    )
    
    # Output to alerts API
    all_anomalies.sink(
        pw.io.python.write(lambda data: send_alert(data))
    )
    
    # Run the pipeline
    pw.run()

def store_vector_embedding(data):
    """Store embedding in vector database"""
    # This would connect to your vector DB
    # For now, we'll just log it
    logger.info(f"Storing embedding for anomaly: {data['data']['text_representation']}")
    
def send_alert(data):
    """Send alert for detected anomaly"""
    logger.warning(f"ALERT: Anomaly detected - {data['data']['text_representation']}")
    # In a real system, this would send to an alerting service

if __name__ == "__main__":
    # Configure logging when run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    start_pipeline()