import os
import logging
from metranova.pipelines.metadata import CRMetadataPipeline
from metranova.pipelines.krc import KRCPipeline
from metranova.pipelines.json import KafkaToJSONPipeline

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
    logger.info("Starting MetrANova Pipeline")
    pipeline = None
    try:
        #determine output method
        pipeline_type = os.getenv('PIPELINE_TYPE', 'json').lower()
        if pipeline_type == 'clickhouse':
            pipeline = KRCPipeline()
        elif pipeline_type == 'metadata':
            pipeline = CRMetadataPipeline()
        else:
            pipeline = KafkaToJSONPipeline()

        # Start the pipeline
        pipeline.start()
    except KeyboardInterrupt:
        logger.info("Shutting down due to keyboard interrupt")
    except Exception as e:
        logger.error(f"Application error: {e}")
        raise 
    finally:
        # Clean shutdown
        if pipeline:
            pipeline.close()

if __name__ == "__main__":
    main()