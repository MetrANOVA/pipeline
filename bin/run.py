import os
import logging
from metranova.pipelines.krc import KRCPipeline
from metranova.pipelines.json import JSONPipeline
from metranova.consumers import KafkaConsumer

# Configure logging
log_level = logging.INFO
if os.getenv('DEBUG', 'false').lower() == 'true' or os.getenv('DEBUG') == '1':
    log_level = logging.DEBUG
    
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main entry point"""
    logger.info("Starting ClickHouse Writer Service")
    
    output = None
    kafka_consumer = None
    
    try:
        #determine output method
        output_method = os.getenv('PIPELINE_TYPE', 'json').lower()
        if output_method == 'clickhouse':
            output = KRCPipeline()
        else:
            output = JSONPipeline()

        # Initialize Kafka consumer
        kafka_consumer = KafkaConsumer(pipeline=output)

        # Start consuming messages
        kafka_consumer.consume_messages()
        
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise
    finally:
        # Clean shutdown
        if hasattr(output, 'close'):
            output.close()
        if kafka_consumer:
            kafka_consumer.close()


if __name__ == "__main__":
    main()