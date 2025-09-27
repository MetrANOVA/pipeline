import logging
import os
import time
from metranova.meta_loader import MetaLoader

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
    logger.info("Starting Meta Loader")
    #get update interval
    update_interval = int(os.getenv('META_UPDATE_INTERVAL', -1))
    meta_loader = None
    try:
        while True:
            # Initialize meta loader
            meta_loader = MetaLoader()
            
            # Load all configured tables
            meta_loader.load_all_tables()
            
            if update_interval <= 0:
                break  # Run once if no interval is set
            
            logger.info(f"Sleeping for {update_interval} seconds before next update")
            time.sleep(update_interval)
    except Exception as e:
        logger.error(f"Meta loader error: {e}")
        raise
    finally:
        if meta_loader:
            meta_loader.close()

if __name__ == "__main__":
    main()