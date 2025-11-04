from ipaddress import IPv6Address
import logging
import os
import time
from typing import Optional
from metranova.cachers.base import BaseCacher
from metranova.connectors.clickhouse import ClickHouseConnector

logger = logging.getLogger(__name__)

class ClickHouseCacher(BaseCacher):

    def __init__(self):
        super().__init__()
        self.logger = logger
        self.cache = ClickHouseConnector()
        self.local_cache = {}
        tables_str = os.getenv('CLICKHOUSE_CACHER_TABLES', '')
        self.cache_refresh_interval = int(os.getenv('CLICKHOUSE_CACHER_REFRESH_INTERVAL', '600'))
        self.tables = [t.strip() for t in tables_str.split(',') if t.strip()]
        self.primary_columns = ['id', 'ref', 'hash']
        if not self.tables:
            self.logger.warning("No tables specified for ClickHouse cacher")
        
        #Prime cache and start refresh thread if needed
        self.start_refresh_thread()

    def prime(self):
        for table in self.tables:
            self.prime_table(table)
    
    def prime_table(self, table):
        """Preload any necessary data into local cache"""
        # Example: preload some frequently accessed keys
        if self.cache.client is None:
            logger.debug("ClickHouse client not available, skipping priming")
            return
    
        # Pull data into local cache
        try:
            rows = self.query_table(table)
            if not rows:
                logger.debug(f"No rows returned for table {table}, skipping priming")
                return
            tmp_cache = {}
            for row in rows:
                if len(row) < 4:
                    self.logger.debug(f"Skipping row with insufficient columns: {row}")
                    return
                max_insert_time, latest_id, latest_ref, latest_hash = row[0], row[1], row[2], row[3]
                # If there are additional fields (e.g., flow_index), then index by those fields instead
                table_key = str(latest_id)
                if len(row) > 4:
                    table_key = ""
                    for latest_field in row[4:]:
                        if latest_field is None:
                            continue
                        #if latest field is an IPv6Address, make it is not a mapped IPv4 address (i.e. starts with ::ffff:)
                        if isinstance(latest_field, IPv6Address) and latest_field.ipv4_mapped:
                            latest_field = str(latest_field.ipv4_mapped)
                        if table_key:
                            table_key += f":"
                        table_key += f"{latest_field}"
                tmp_cache[table_key] = {
                    'id': latest_id,
                    'ref': latest_ref,
                    'hash': latest_hash,
                    'max_insert_time': max_insert_time
                }
            self.local_cache[table] = tmp_cache
            logger.info(f"Loaded {len(tmp_cache)} existing records from {table}")
        except Exception as e:
            logger.info(f"Could not load existing data (table might be empty): {e}")

    def lookup(self, table, key: str) -> Optional[str]:
        #lookup in local cache
        if key is None or table is None:
            return None
        return self.local_cache.get(table, {}).get(str(key), None)
    
    def lookup_dict_key(self, table: str, key: str, dict_key: str) -> Optional[str]:
        """Lookup a specific key from the cached dict for a given table and key"""
        record = self.lookup(table, key)
        if record is None:
            return None
        return record.get(dict_key, None)

    def build_query(self, table: str) -> str:
        """Build the metadata query for a specific table
        Example table formats:
        - meta_device - Grabs id and ref only
        - meta_interface:device_id:flow_index - Grabs id, ref, device_id and flow_index
        - meta_interface:@loopback_ip - Grabs the array field loopback_ip and expands it via ARRAY JOIN
        """
        (table_name, *fields) = table.split(':') if ':' in table else (table, None)
        #query = "SELECT argMax(id, insert_time) as latest_id, argMax(ref, insert_time) as latest_ref"
        #start building query with self.primary_columns
        query = "SELECT MAX(insert_time) as max_insert_time, " + ", ".join([f"argMax({col}, insert_time) as latest_{col}" for col in self.primary_columns])
        array_joins = []
        latest_fields = ["max_insert_time"] + [ f"latest_{col}" for col in self.primary_columns ]
        for field in fields:
            if field is None:
                continue
            if field.startswith('@'):
                array_field = field[1:]
                latest_field = f"latest_{array_field}"
                latest_fields.append(latest_field)
                array_joins.append(latest_field)
                query += f", argMax({array_field}, insert_time) as {latest_field}"
            else:
                latest_field = f"latest_{field}"
                latest_fields.append(latest_field)
                query += f", argMax({field}, insert_time) as {latest_field}"
        query += f" FROM {table_name} "
        query += "WHERE id IS NOT NULL AND ref IS NOT NULL"
        for field in fields:
            if field is not None:
                query += f" AND {field.lstrip('@')} IS NOT NULL"
        query += " GROUP BY id ORDER BY id"
        if len(array_joins) > 0:
            query = "SELECT " + ", ".join(latest_fields) + f" FROM ({query}) ARRAY JOIN " + ", ".join(array_joins)

        return query

    def query_table(self, table: str):
        """Load metadata from a single ClickHouse table"""
        logger.info(f"Loading metadata from table: {table}")
        
        try:
            # Build and execute query
            query = self.build_query(table)
            logger.debug(f"Executing query: {query}")
            
            start_time = time.time()
            result = self.cache.client.query(query)
            query_time = time.time() - start_time
            if not result:
                logger.warning(f"No result returned from query for table {table}")
                return None
            
            # Get the data
            rows = result.result_rows
            logger.info(f"Query completed in {query_time:.2f}s, found {len(rows)} records")
            
            if not rows:
                logger.warning(f"No data found in table {table}")
                return None

            return rows
            
        except Exception as e:
            logger.error(f"Failed to load metadata from table {table}: {e}")
            raise