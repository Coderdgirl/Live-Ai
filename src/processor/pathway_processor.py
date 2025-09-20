"""
Pathway Processor for LiveGuard AI
Processes streaming data from Kafka using Pathway for real-time analytics
"""

import os
import json
import pathway as pw
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# Kafka configuration
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC_FINANCIAL = os.getenv('KAFKA_TOPIC_FINANCIAL', 'financial_data')
TOPIC_SECURITY = os.getenv('KAFKA_TOPIC_SECURITY', 'security_events')
GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'liveguard_pathway_processor')

# Qdrant configuration
QDRANT_HOST = os.getenv('QDRANT_HOST', 'localhost')
QDRANT_PORT = int(os.getenv('QDRANT_PORT', '6333'))
QDRANT_COLLECTION_FINANCIAL = os.getenv('QDRANT_COLLECTION_FINANCIAL', 'financial_vectors')
QDRANT_COLLECTION_SECURITY = os.getenv('QDRANT_COLLECTION_SECURITY', 'security_vectors')

class LiveGuardProcessor:
    """Real-time data processor using Pathway"""
    
    def __init__(self):
        """Initialize the Pathway processor"""
        self.pw_context = pw.io.kafka.read(
            brokers=BOOTSTRAP_SERVERS,
            topics=[TOPIC_FINANCIAL, TOPIC_SECURITY],
            group_id=GROUP_ID,
            auto_offset_reset="latest",
            poll_timeout=1.0,
        )
    
    def parse_messages(self):
        """Parse Kafka messages into structured data"""
        # Parse JSON messages
        parsed = self.pw_context.select(
            topic=pw.this.topic,
            data=pw.apply(json.loads, pw.this.value)
        )
        
        # Split streams by topic
        financial_stream = parsed.filter(pw.this.topic == TOPIC_FINANCIAL)
        security_stream = parsed.filter(pw.this.topic == TOPIC_SECURITY)
        
        return financial_stream, security_stream
    
    def process_financial_data(self, stream):
        """Process financial transaction data"""
        # Extract fields from financial data
        transactions = stream.select(
            transaction_id=pw.apply(lambda x: x.get("transaction_id"), pw.this.data),
            timestamp=pw.apply(lambda x: x.get("timestamp"), pw.this.data),
            account_id=pw.apply(lambda x: x.get("account_id"), pw.this.data),
            transaction_type=pw.apply(lambda x: x.get("transaction_type"), pw.this.data),
            amount=pw.apply(lambda x: x.get("amount"), pw.this.data),
            currency=pw.apply(lambda x: x.get("currency"), pw.this.data),
            location=pw.apply(lambda x: x.get("location"), pw.this.data),
            device_id=pw.apply(lambda x: x.get("device_id"), pw.this.data),
            is_flagged=pw.apply(lambda x: x.get("is_flagged", False), pw.this.data),
            anomaly_indicator=pw.apply(lambda x: x.get("anomaly_indicator", None), pw.this.data),
        )
        
        # Detect high-value transactions
        high_value_txns = transactions.filter(pw.this.amount > 5000)
        
        # Detect potential fraud patterns
        potential_fraud = transactions.filter(
            (pw.this.is_flagged == True) | 
            (pw.this.anomaly_indicator != None)
        )
        
        # Time-window aggregation for account activity
        account_activity = transactions \
            .groupby(pw.this.account_id) \
            .reduce(
                account_id=pw.this.account_id,
                transaction_count=pw.count(),
                total_amount=pw.sum(pw.this.amount),
                last_transaction=pw.max(pw.this.timestamp),
                high_value_count=pw.count(pw.this.amount > 1000),
                flagged_count=pw.count(pw.this.is_flagged == True)
            )
        
        return transactions, high_value_txns, potential_fraud, account_activity
    
    def process_security_events(self, stream):
        """Process security event data"""
        # Extract fields from security events
        events = stream.select(
            event_id=pw.apply(lambda x: x.get("event_id"), pw.this.data),
            timestamp=pw.apply(lambda x: x.get("timestamp"), pw.this.data),
            event_type=pw.apply(lambda x: x.get("event_type"), pw.this.data),
            user_id=pw.apply(lambda x: x.get("user_id"), pw.this.data),
            source_ip=pw.apply(lambda x: x.get("source_ip"), pw.this.data),
            resource_id=pw.apply(lambda x: x.get("resource_id"), pw.this.data),
            severity=pw.apply(lambda x: x.get("severity"), pw.this.data),
            status=pw.apply(lambda x: x.get("status"), pw.this.data),
            is_flagged=pw.apply(lambda x: x.get("is_flagged", False), pw.this.data),
            anomaly_indicator=pw.apply(lambda x: x.get("anomaly_indicator", None), pw.this.data),
        )
        
        # Detect high-severity events
        high_severity = events.filter(pw.this.severity.in_(["high", "critical"]))
        
        # Detect potential security incidents
        security_incidents = events.filter(
            (pw.this.is_flagged == True) | 
            (pw.this.anomaly_indicator != None) |
            (pw.this.severity.in_(["high", "critical"]) & (pw.this.status == "failure"))
        )
        
        # Time-window aggregation for user activity
        user_activity = events \
            .groupby(pw.this.user_id) \
            .reduce(
                user_id=pw.this.user_id,
                event_count=pw.count(),
                failure_count=pw.count(pw.this.status == "failure"),
                last_event=pw.max(pw.this.timestamp),
                high_severity_count=pw.count(pw.this.severity.in_(["high", "critical"])),
                flagged_count=pw.count(pw.this.is_flagged == True)
            )
        
        return events, high_severity, security_incidents, user_activity
    
    def prepare_vector_embeddings(self, financial_data, security_data):
        """Prepare data for vector embeddings"""
        # For financial data, create a feature vector
        financial_vectors = financial_data.select(
            id=pw.this.transaction_id,
            vector=pw.apply(
                lambda tx_type, amount, is_flagged, has_anomaly: 
                    [
                        1.0 if tx_type == "deposit" else 0.0,
                        1.0 if tx_type == "withdrawal" else 0.0,
                        1.0 if tx_type == "transfer" else 0.0,
                        1.0 if tx_type == "payment" else 0.0,
                        float(amount) / 10000.0,  # Normalize amount
                        1.0 if is_flagged else 0.0,
                        1.0 if has_anomaly else 0.0
                    ],
                pw.this.transaction_type,
                pw.this.amount,
                pw.this.is_flagged,
                pw.this.anomaly_indicator != None
            ),
            metadata=pw.apply(
                lambda tx: json.dumps(tx),
                pw.record(
                    transaction_id=pw.this.transaction_id,
                    timestamp=pw.this.timestamp,
                    account_id=pw.this.account_id,
                    transaction_type=pw.this.transaction_type,
                    amount=pw.this.amount,
                    currency=pw.this.currency,
                    is_flagged=pw.this.is_flagged,
                    anomaly_indicator=pw.this.anomaly_indicator
                )
            )
        )
        
        # For security data, create a feature vector
        security_vectors = security_data.select(
            id=pw.this.event_id,
            vector=pw.apply(
                lambda event_type, severity, status, is_flagged, has_anomaly: 
                    [
                        1.0 if event_type == "login" else 0.0,
                        1.0 if event_type == "logout" else 0.0,
                        1.0 if event_type == "file_access" else 0.0,
                        1.0 if event_type == "permission_change" else 0.0,
                        1.0 if event_type == "system_alert" else 0.0,
                        1.0 if severity == "low" else 0.0,
                        1.0 if severity == "medium" else 0.0,
                        1.0 if severity == "high" else 0.0,
                        1.0 if severity == "critical" else 0.0,
                        1.0 if status == "failure" else 0.0,
                        1.0 if is_flagged else 0.0,
                        1.0 if has_anomaly else 0.0
                    ],
                pw.this.event_type,
                pw.this.severity,
                pw.this.status,
                pw.this.is_flagged,
                pw.this.anomaly_indicator != None
            ),
            metadata=pw.apply(
                lambda evt: json.dumps(evt),
                pw.record(
                    event_id=pw.this.event_id,
                    timestamp=pw.this.timestamp,
                    event_type=pw.this.event_type,
                    user_id=pw.this.user_id,
                    source_ip=pw.this.source_ip,
                    severity=pw.this.severity,
                    status=pw.this.status,
                    is_flagged=pw.this.is_flagged,
                    anomaly_indicator=pw.this.anomaly_indicator
                )
            )
        )
        
        return financial_vectors, security_vectors
    
    def send_to_vector_db(self, financial_vectors, security_vectors):
        """Send processed data to vector database"""
        # Send financial vectors to Qdrant
        pw.io.qdrant.write(
            financial_vectors,
            host=QDRANT_HOST,
            port=QDRANT_PORT,
            collection_name=QDRANT_COLLECTION_FINANCIAL,
            create_collection=True,
            vector_size=7,  # Match the size of our feature vector
        )
        
        # Send security vectors to Qdrant
        pw.io.qdrant.write(
            security_vectors,
            host=QDRANT_HOST,
            port=QDRANT_PORT,
            collection_name=QDRANT_COLLECTION_SECURITY,
            create_collection=True,
            vector_size=12,  # Match the size of our feature vector
        )
    
    def run(self):
        """Run the Pathway processor pipeline"""
        # Parse Kafka messages
        financial_stream, security_stream = self.parse_messages()
        
        # Process financial data
        financial_data, high_value_txns, financial_fraud, account_activity = self.process_financial_data(financial_stream)
        
        # Process security events
        security_data, high_severity, security_incidents, user_activity = self.process_security_events(security_stream)
        
        # Prepare vector embeddings
        financial_vectors, security_vectors = self.prepare_vector_embeddings(financial_data, security_data)
        
        # Send to vector database
        self.send_to_vector_db(financial_vectors, security_vectors)
        
        # Output alerts to console for monitoring
        pw.io.csv.write(high_value_txns, "stdout:")
        pw.io.csv.write(financial_fraud, "stdout:")
        pw.io.csv.write(high_severity, "stdout:")
        pw.io.csv.write(security_incidents, "stdout:")
        
        # Run the Pathway engine
        pw.run()

if __name__ == "__main__":
    print(f"Starting LiveGuard AI Pathway Processor")
    processor = LiveGuardProcessor()
    processor.run()