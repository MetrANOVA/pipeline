import argparse
import os
import logging
from metranova.pipelines.base import YAMLPipeline

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
    logger.info("Starting MetrANOVA Pipeline")
    pipeline = None
    try:
        #Get YAML file from args or env
        parser = argparse.ArgumentParser(description="MetrANOVA Pipeline Runner")
        parser.add_argument('--pipeline', type=str, required=False, help='YAML file defining the pipeline configuration')
        args = parser.parse_args()
        pipeline_yaml = args.pipeline or os.getenv('PIPELINE_YAML', None)
        if not pipeline_yaml:
            raise ValueError("Pipeline YAML configuration file must be provided via --pipeline argument or PIPELINE_YAML environment variable")

        #Load pipeline from YAML
        pipeline = YAMLPipeline(yaml_file=pipeline_yaml)
        logger.info(f"Loaded pipeline {pipeline}")

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