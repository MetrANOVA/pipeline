import logging
import os
import time
from metranova.connectors.clickhouse import ClickHouseConnector
from metranova.consumers.base import TimedIntervalConsumer
from metranova.pipelines.base import BasePipeline

logger = logging.getLogger(__name__)

class BaseClickHouseConsumer(TimedIntervalConsumer):
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.datasource = ClickHouseConnector()
        self.update_interval = -1
        self.tables = []

    def consume_messages(self):
        if not self.tables:
            self.logger.error("No tables specified for metadata loading")
            return
    
        # Load from tables serially
        self.logger.info("Starting metadata loading from ClickHouse")
        for table in self.tables:
            try:
                msg = self.query_table(table)
                self.pipeline.process_message(msg)
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")

    def query_table(self, table: str) -> dict:
        raise NotImplementedError("Subclasses must implement query_table method")

class MetadataClickHouseConsumer(BaseClickHouseConsumer):
    """ClickHouse consumer for loading metadata tables"""
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.update_interval = int(os.getenv('CLICKHOUSE_CONSUMER_UPDATE_INTERVAL', -1))
        # Load tables from environment variable - can be in format table or table:field[:field...]
        tables_str = os.getenv('CLICKHOUSE_CONSUMER_TABLES', '')
        if tables_str:
            self.tables = [table.strip() for table in tables_str.split(',') if table.strip()]
            logger.info(f"Found {len(self.tables)} meta lookup tables: {self.tables}")
        else:
            logger.warning("CLICKHOUSE_CONSUMER_TABLES environment variable is empty")

    def build_query(self, table: str) -> str:
        """Build the metadata query for a specific table"""
        (table_name, *fields) = table.split(':') if ':' in table else (table, None)
        query = "SELECT argMax(id, insert_time) as latest_id,argMax(ref, insert_time) as latest_ref"
        for field in fields:
            if field is not None:
                query += f", argMax({field}, insert_time) as latest_{field}"
        query += f" FROM {table_name} WHERE id IS NOT NULL AND ref IS NOT NULL"
        for field in fields:
            if field is not None:
                query += f" AND {field} IS NOT NULL"
        query += " GROUP BY id ORDER BY id"
        return query

    def query_table(self, table: str) -> dict:
        """Load metadata from a single ClickHouse table"""
        logger.info(f"Loading metadata from table: {table}")
        
        try:
            # Build and execute query
            query = self.build_query(table)
            logger.debug(f"Executing query: {query}")
            
            start_time = time.time()
            result = self.datasource.client.query(query)
            query_time = time.time() - start_time
            
            # Get the data
            rows = result.result_rows
            logger.info(f"Query completed in {query_time:.2f}s, found {len(rows)} records")
            
            if not rows:
                logger.warning(f"No data found in table {table}")
                return {}

            return {"table": table, "rows": rows}
            
        except Exception as e:
            logger.error(f"Failed to load metadata from table {table}: {e}")
            raise

class IPMetadataClickHouseConsumer(MetadataClickHouseConsumer):
    def build_query(self, table: str) -> str:
        """Build the metadata query for a specific table"""
        query = f"""
        SELECT argMax(id, insert_time) as latest_id, 
               argMax(ref, insert_time) as latest_ref,
               argMax(ip_subnet, insert_time) as latest_ip_subnet
        FROM {table} 
        GROUP BY id
        ORDER BY id
        """
        return query