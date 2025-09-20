"""
Kafka Producer for LiveGuard AI
Generates and sends sample data to Kafka topics for financial and security events
"""

import json
import time
import random
import os
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables
load_dotenv()

# Kafka configuration
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_FINANCIAL = os.getenv('KAFKA_TOPIC_FINANCIAL', 'financial_data')
TOPIC_SECURITY = os.getenv('KAFKA_TOPIC_SECURITY', 'security_events')

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None
)

def generate_financial_data():
    """Generate sample financial transaction data"""
    transaction_types = ['deposit', 'withdrawal', 'transfer', 'payment']
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CNY']
    
    data = {
        'transaction_id': f"tx-{random.randint(10000, 99999)}",
        'timestamp': datetime.now().isoformat(),
        'account_id': f"acc-{random.randint(1000, 9999)}",
        'transaction_type': random.choice(transaction_types),
        'amount': round(random.uniform(10.0, 10000.0), 2),
        'currency': random.choice(currencies),
        'location': f"{random.uniform(-90, 90):.6f},{random.uniform(-180, 180):.6f}",
        'device_id': f"dev-{random.randint(100, 999)}",
        'is_flagged': random.random() < 0.05  # 5% chance of being flagged
    }
    
    # Add some anomaly indicators for demo purposes
    if random.random() < 0.1:  # 10% chance of anomaly
        anomaly_type = random.choice([
            'unusual_location', 
            'large_amount', 
            'high_frequency', 
            'unusual_time'
        ])
        data['anomaly_indicator'] = anomaly_type
    
    return data

def generate_security_event():
    """Generate sample security event data"""
    event_types = ['login', 'logout', 'file_access', 'permission_change', 'system_alert']
    severity_levels = ['low', 'medium', 'high', 'critical']
    status_codes = ['success', 'failure', 'warning', 'error']
    
    data = {
        'event_id': f"evt-{random.randint(10000, 99999)}",
        'timestamp': datetime.now().isoformat(),
        'event_type': random.choice(event_types),
        'user_id': f"user-{random.randint(100, 999)}",
        'source_ip': f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        'resource_id': f"res-{random.randint(100, 999)}",
        'severity': random.choice(severity_levels),
        'status': random.choice(status_codes),
        'is_flagged': random.random() < 0.08  # 8% chance of being flagged
    }
    
    # Add some anomaly indicators for demo purposes
    if random.random() < 0.15:  # 15% chance of anomaly
        anomaly_type = random.choice([
            'unusual_access_pattern', 
            'multiple_failures', 
            'privilege_escalation', 
            'unusual_time'
        ])
        data['anomaly_indicator'] = anomaly_type
    
    return data

def send_to_kafka(topic, data, key=None):
    """Send data to Kafka topic"""
    future = producer.send(topic, key=key, value=data)
    try:
        record_metadata = future.get(timeout=10)
        print(f"Sent to {record_metadata.topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")

def run_producer(interval=1.0):
    """Run the producer to continuously generate and send data"""
    try:
        while True:
            # Generate and send financial data
            financial_data = generate_financial_data()
            send_to_kafka(TOPIC_FINANCIAL, financial_data, key=financial_data['transaction_id'])
            
            # Generate and send security event
            security_event = generate_security_event()
            send_to_kafka(TOPIC_SECURITY, security_event, key=security_event['event_id'])
            
            # Wait before generating next batch
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Producer stopped by user")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed")

if __name__ == "__main__":
    print(f"Starting LiveGuard AI Kafka Producer")
    print(f"Sending financial data to topic: {TOPIC_FINANCIAL}")
    print(f"Sending security events to topic: {TOPIC_SECURITY}")
    run_producer()