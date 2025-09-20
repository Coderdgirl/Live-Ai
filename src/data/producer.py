"""
Kafka producer for LiveGuard AI
Generates simulated financial and security data for demonstration
"""
import os
import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'liveguard-data')

def create_producer():
    """Create and return a Kafka producer instance"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        return producer
    except Exception as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        raise

def generate_financial_data():
    """Generate simulated financial transaction data"""
    transaction_types = ['purchase', 'withdrawal', 'transfer', 'deposit']
    merchants = ['Amazon', 'Walmart', 'Target', 'Starbucks', 'Apple Store', 'Gas Station', 'Restaurant', 'Hotel']
    countries = ['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'CN']
    
    # Generate random transaction
    transaction_type = random.choice(transaction_types)
    amount = round(random.uniform(1.0, 1000.0), 2)
    merchant = random.choice(merchants)
    country = random.choice(countries)
    user_id = f"user_{random.randint(1000, 9999)}"
    
    # Add anomaly in 5% of transactions
    is_anomaly = random.random() < 0.05
    
    if is_anomaly:
        # Create anomalous transaction
        if random.random() < 0.5:
            # Unusually large amount
            amount = round(random.uniform(5000.0, 20000.0), 2)
        else:
            # Unusual location
            country = random.choice(['RU', 'NG', 'BR', 'KP'])
    
    return {
        'timestamp': datetime.now().isoformat(),
        'type': 'financial',
        'transaction_type': transaction_type,
        'amount': amount,
        'merchant': merchant,
        'country': country,
        'user_id': user_id,
        'is_anomaly': is_anomaly
    }

def generate_security_data():
    """Generate simulated security event data"""
    event_types = ['login', 'logout', 'file_access', 'admin_action', 'password_change', 'api_access']
    status_codes = ['success', 'failure', 'timeout', 'unauthorized']
    ip_addresses = [
        '192.168.1.1', '10.0.0.1', '172.16.0.1',  # Internal IPs
        '203.0.113.1', '198.51.100.1', '8.8.8.8'  # External IPs
    ]
    
    # Generate random security event
    event_type = random.choice(event_types)
    status = random.choice(status_codes)
    ip_address = random.choice(ip_addresses)
    user_id = f"user_{random.randint(1000, 9999)}"
    
    # Add anomaly in 5% of events
    is_anomaly = random.random() < 0.05
    
    if is_anomaly:
        # Create anomalous security event
        if random.random() < 0.5:
            # Failed login attempts
            event_type = 'login'
            status = 'failure'
            # Generate suspicious IP
            ip_parts = [str(random.randint(1, 255)) for _ in range(4)]
            ip_address = '.'.join(ip_parts)
        else:
            # Unusual admin action
            event_type = 'admin_action'
            # Unusual time (3am-4am)
            
    return {
        'timestamp': datetime.now().isoformat(),
        'type': 'security',
        'event_type': event_type,
        'status': status,
        'ip_address': ip_address,
        'user_id': user_id,
        'is_anomaly': is_anomaly
    }

def start_producer():
    """Start the Kafka producer and generate data"""
    try:
        producer = create_producer()
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Producing messages to topic: {KAFKA_TOPIC}")
        
        while True:
            # Generate either financial or security data
            if random.random() < 0.6:  # 60% financial, 40% security
                data = generate_financial_data()
            else:
                data = generate_security_data()
            
            # Send data to Kafka
            producer.send(KAFKA_TOPIC, data)
            
            # Log anomalies
            if data.get('is_anomaly', False):
                logger.warning(f"Anomaly detected: {data}")
            
            # Sleep for a random interval (0.5-2 seconds)
            time.sleep(random.uniform(0.5, 2.0))
            
    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Producer error: {e}")
    finally:
        if 'producer' in locals():
            producer.flush()
            producer.close()
            logger.info("Producer closed")

if __name__ == "__main__":
    # Configure logging when run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    start_producer()