# LiveGuard AI: Real-Time Intelligence for Finance, Security, and Beyond

A real-time data processing and analytics platform built with Pathway, Kafka, and vector embeddings for intelligent monitoring and alerting.

## Features

- Real-time data ingestion via Kafka
- Intelligent data processing with Pathway
- Vector embeddings for similarity search and anomaly detection
- Real-time analytics dashboard
- Alerting system for critical events

## Setup Instructions

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Set up Kafka:
   - Download and install Kafka from [kafka.apache.org](https://kafka.apache.org/downloads)
   - Start Zookeeper: `bin/zookeeper-server-start.sh config/zookeeper.properties`
   - Start Kafka: `bin/kafka-server-start.sh config/server.properties`
   - Create topic: `bin/kafka-topics.sh --create --topic liveguard-data --bootstrap-server localhost:9092`

3. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Update the variables as needed

4. Run the application:
   ```
   python src/main.py
   ```

5. Access the dashboard:
   - Open your browser and navigate to `http://localhost:8050`

## Project Structure

- `src/`: Source code
  - `data/`: Data producers and consumers
  - `pipeline/`: Pathway data processing pipelines
  - `models/`: Vector embedding models
  - `api/`: FastAPI endpoints
  - `dashboard/`: Dash visualization dashboard
- `config/`: Configuration files
- `tests/`: Unit and integration tests