import os
import logging
from metranova.ch_writer import KafkaSSLConsumer,JSONOutput
from metranova.flow_edge_output import FlowEdgeOutput
from metranova.raw_msg_output import RawMsgOutput

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
        output_method = os.getenv('OUTPUT_METHOD', 'json').lower()
        if output_method == 'raw_msg':
            output = RawMsgOutput()
        elif output_method == 'flow_edge':
            output = FlowEdgeOutput()
        else:
            output = JSONOutput()

        # Initialize Kafka consumer
        kafka_consumer = KafkaSSLConsumer(output=output)

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
            kafka_consumer._close_consumer()


if __name__ == "__main__":
    main()