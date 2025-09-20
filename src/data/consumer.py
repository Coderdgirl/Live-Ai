"""
Kafka consumer for LiveGuard AI
Consumes data from Kafka topic and forwards to Pathway pipeline
"""
import os
import json
import logging
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'liveguard-data')

def create_consumer():
    """Create and return a Kafka consumer instance"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='liveguard-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        raise

def consume_messages(callback):
    """
    Consume messages from Kafka topic and process them with the callback
    
    Args:
        callback: Function to call with each message
    """
    try:
        consumer = create_consumer()
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Consuming messages from topic: {KAFKA_TOPIC}")
        
        for message in consumer:
            try:
                data = message.value
                callback(data)
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            logger.info("Consumer closed")

if __name__ == "__main__":
    # Configure logging when run directly
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Example callback function
    def print_message(data):
        print(f"Received: {data}")
    
    consume_messages(print_message)