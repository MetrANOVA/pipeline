import logging
import os
import clickhouse_connect
from metranova.connectors.base import BaseConnector

logger = logging.getLogger(__name__)

class ClickHouseConnector(BaseConnector):
    def __init__(self):
        # setup logger
        self.logger = logger

        # ClickHouse configuration from environment
        self.host = os.getenv('CLICKHOUSE_HOST', 'localhost')
        self.port = int(os.getenv('CLICKHOUSE_PORT', '8123'))
        self.database = os.getenv('CLICKHOUSE_DATABASE', 'default')
        self.username = os.getenv('CLICKHOUSE_USERNAME', 'default')
        self.password = os.getenv('CLICKHOUSE_PASSWORD', '')

        # Initialize ClickHouse connection
        self.client = None

        # Connect to ClickHouse 
        self.connect()

    def connect(self):
        # Initialize ClickHouse connection
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                secure=os.getenv('CLICKHOUSE_SECURE', 'false').lower() == 'true',
                verify=False
            )
            # Test connection
            self.client.ping()

            #Create database
            skip_db_creation = os.getenv('CLICKHOUSE_SKIP_DB_CREATE', 'false').lower() in ['1', 'true', 'yes']
            if not skip_db_creation:
                #setup database
                self.create_database()
            #switch to target database
            self.client.command(f"USE {self.database}")
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}, database: {self.database}")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
    
    def create_database(self):
        """Create the target database if it doesn't exist"""
        if self.database is None:
            self.logger.warning("No database name specified, skipping database creation")
            return
        try:
            self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            self.logger.info(f"Database {self.database} is ready")
        except Exception as e:
            self.logger.error(f"Failed to create database {self.database}: {e}")
            raise

    def close(self):
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed")
