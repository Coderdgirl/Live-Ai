"""
LiveGuard AI: Main application entry point
"""
import os
import logging
from dotenv import load_dotenv
import threading
import time

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def start_kafka_producer():
    """Start the Kafka producer in a separate thread"""
    from data.producer import start_producer
    producer_thread = threading.Thread(target=start_producer)
    producer_thread.daemon = True
    producer_thread.start()
    return producer_thread

def start_pathway_pipeline():
    """Start the Pathway data processing pipeline"""
    from pipeline.processor import start_pipeline
    pipeline_thread = threading.Thread(target=start_pipeline)
    pipeline_thread.daemon = True
    pipeline_thread.start()
    return pipeline_thread

def start_api_server():
    """Start the FastAPI server"""
    from api.server import start_server
    api_thread = threading.Thread(target=start_server)
    api_thread.daemon = True
    api_thread.start()
    return api_thread

def start_dashboard():
    """Start the Dash dashboard"""
    from dashboard.app import start_dashboard
    dashboard_thread = threading.Thread(target=start_dashboard)
    dashboard_thread.daemon = True
    dashboard_thread.start()
    return dashboard_thread

def main():
    """Main entry point for the application"""
    logger.info("Starting LiveGuard AI...")
    
    # Start components
    producer_thread = start_kafka_producer()
    logger.info("Kafka producer started")
    
    time.sleep(2)  # Give producer time to connect
    
    pipeline_thread = start_pathway_pipeline()
    logger.info("Pathway pipeline started")
    
    api_thread = start_api_server()
    logger.info("API server started")
    
    dashboard_thread = start_dashboard()
    logger.info("Dashboard started")
    
    logger.info("LiveGuard AI is running!")
    logger.info(f"Dashboard available at http://{os.getenv('DASHBOARD_HOST', '0.0.0.0')}:{os.getenv('DASHBOARD_PORT', '8050')}")
    
    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down LiveGuard AI...")

if __name__ == "__main__":
    main()