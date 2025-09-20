"""
Kafka Consumer for LiveGuard AI
Consumes data from Kafka topics for financial and security events
"""

import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer

# Load environment variables
load_dotenv()

# Kafka configuration
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_FINANCIAL = os.getenv('KAFKA_TOPIC_FINANCIAL', 'financial_data')
TOPIC_SECURITY = os.getenv('KAFKA_TOPIC_SECURITY', 'security_events')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'liveguard_consumer')

class LiveGuardConsumer:
    """Kafka consumer for LiveGuard AI data streams"""
    
    def __init__(self, topics=None, group_id=GROUP_ID):
        """Initialize the consumer with specified topics"""
        if topics is None:
            topics = [TOPIC_FINANCIAL, TOPIC_SECURITY]
        
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None
        )
    
    def consume_batch(self, timeout_ms=1000, max_records=100):
        """Consume a batch of messages with timeout"""
        messages = []
        message_count = 0
        
        # Poll for messages
        message_batch = self.consumer.poll(timeout_ms=timeout_ms, max_records=max_records)
        
        # Process the batch
        for tp, records in message_batch.items():
            for record in records:
                messages.append({
                    'topic': tp.topic,
                    'partition': tp.partition,
                    'offset': record.offset,
                    'key': record.key,
                    'value': record.value,
                    'timestamp': record.timestamp
                })
                message_count += 1
        
        return messages
    
    def consume_stream(self, callback):
        """Stream messages and process with callback function"""
        try:
            for message in self.consumer:
                # Extract message data
                data = {
                    'topic': message.topic,
                    'partition': message.partition,
                    'offset': message.offset,
                    'key': message.key,
                    'value': message.value,
                    'timestamp': message.timestamp
                }
                
                # Process with callback
                callback(data)
                
        except KeyboardInterrupt:
            print("Consumer stopped by user")
        finally:
            self.close()
    
    def close(self):
        """Close the consumer connection"""
        self.consumer.close()
        print("Consumer closed")

def print_message(message):
    """Simple callback to print message data"""
    topic = message['topic']
    value = message['value']
    
    if topic == TOPIC_FINANCIAL:
        print(f"Financial Transaction: {value['transaction_id']} - {value['transaction_type']} - {value['amount']} {value['currency']}")
        if 'anomaly_indicator' in value:
            print(f"⚠️ ANOMALY DETECTED: {value['anomaly_indicator']}")
    
    elif topic == TOPIC_SECURITY:
        print(f"Security Event: {value['event_id']} - {value['event_type']} - {value['severity']} severity")
        if 'anomaly_indicator' in value:
            print(f"⚠️ ANOMALY DETECTED: {value['anomaly_indicator']}")

if __name__ == "__main__":
    print(f"Starting LiveGuard AI Kafka Consumer")
    print(f"Consuming from topics: {TOPIC_FINANCIAL}, {TOPIC_SECURITY}")
    
    # Create consumer and start streaming
    consumer = LiveGuardConsumer()
    consumer.consume_stream(print_message)