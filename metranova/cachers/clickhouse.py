from ipaddress import IPv6Address
import logging
import os
import re
import time
from cachetools import TTLCache
from typing import Optional
from metranova.cachers.base import BaseCacher
from metranova.connectors.clickhouse import ClickHouseConnector

logger = logging.getLogger(__name__)

class ClickHouseCacher(BaseCacher):

    def __init__(self):
        super().__init__()
        self.logger = logger
        self.cache = ClickHouseConnector()
        self.cache_max_size = int(os.getenv('CLICKHOUSE_CACHER_MAX_SIZE', '100000000')) #defaults to large 100 million entries
        self.cache_max_ttl = int(os.getenv('CLICKHOUSE_CACHER_MAX_TTL', '86400')) #defaults to large 1 hour TTL
        self.local_cache = TTLCache(maxsize=self.cache_max_size, ttl=self.cache_max_ttl)
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

class ClickHouseRangedCacher(ClickHouseCacher):
    """Caches range-based mappings as key -> range_val -> id per configured table."""

    def __init__(self):
        BaseCacher.__init__(self)
        self.logger = logger
        self.cache = ClickHouseConnector()
        self.cache_max_size = int(os.getenv('CLICKHOUSE_CACHER_MAX_SIZE', '100000000'))
        self.cache_max_ttl = int(os.getenv('CLICKHOUSE_CACHER_MAX_TTL', '86400'))
        self.local_cache = TTLCache(maxsize=self.cache_max_size, ttl=self.cache_max_ttl)
        self.cache_refresh_interval = int(os.getenv('CLICKHOUSE_CACHER_REFRESH_INTERVAL', '600'))
        self.range_configs = self._parse_range_configs()
        if not self.range_configs:
            self.logger.warning("No valid ranged cacher configurations were found")

        # Keep this for compatibility with parent cacher interface expectations.
        self.tables = [cfg['lookup_table'] for cfg in self.range_configs]

        # Prime cache and start refresh thread if needed.
        self.start_refresh_thread()

    @staticmethod
    def _identifier_is_safe(identifier: str) -> bool:
        return bool(re.match(r'^[A-Za-z_][A-Za-z0-9_\.]*$', identifier or ''))

    def _parse_range_configs(self):
        """Parse range cacher sources from env.

        Format for CLICKHOUSE_RANGED_CACHER_CONFIGS:
        lookup_table:clickhouse_table:min_col:max_col:key_col:val_col
        [,lookup_table:clickhouse_table:min_col:max_col:key_col:val_col...]

        Backward compatible format:
        clickhouse_table:min_col:max_col:key_col:val_col
        """
        configs_raw = os.getenv('CLICKHOUSE_RANGED_CACHER_CONFIGS', '').strip()
        parsed_configs = []

        if configs_raw:
            chunks = [chunk.strip() for chunk in configs_raw.split(',') if chunk.strip()]
            for chunk in chunks:
                parts = [part.strip() for part in chunk.split(':')]
                if len(parts) == 6:
                    lookup_table, table, min_col, max_col, key_col, val_col = parts
                elif len(parts) == 5:
                    table, min_col, max_col, key_col, val_col = parts
                    lookup_table = table
                else:
                    self.logger.warning(
                        f"Skipping invalid CLICKHOUSE_RANGED_CACHER_CONFIGS entry: {chunk}"
                    )
                    continue
                parsed_configs.append({
                    'lookup_table': lookup_table,
                    'table': table,
                    'min_col': min_col,
                    'max_col': max_col,
                    'key_col': key_col,
                    'val_col': val_col,
                })
        else:
            table = os.getenv('CLICKHOUSE_RANGED_CACHER_TABLE', 'meta_application_dict').strip()
            parsed_configs.append({
                'lookup_table': os.getenv('CLICKHOUSE_RANGED_CACHER_LOOKUP_TABLE', table).strip(),
                'table': table,
                'min_col': os.getenv('CLICKHOUSE_RANGED_CACHER_MIN_COLUMN', 'port_range_min').strip(),
                'max_col': os.getenv('CLICKHOUSE_RANGED_CACHER_MAX_COLUMN', 'port_range_max').strip(),
                'key_col': os.getenv(
                    'CLICKHOUSE_RANGED_CACHER_KEY_COLUMN',
                    os.getenv('CLICKHOUSE_RANGED_CACHER_KEY_COLUMN', 'protocol')
                ).strip(),
                'val_col': os.getenv(
                    'CLICKHOUSE_RANGED_CACHER_VAL_COLUMN',
                    os.getenv('CLICKHOUSE_RANGED_CACHER_ID_COLUMN', 'id')
                ).strip(),
            })

        valid_configs = []
        for cfg in parsed_configs:
            identifiers = [
                cfg['lookup_table'],
                cfg['table'],
                cfg['min_col'],
                cfg['max_col'],
                cfg['key_col'],
                cfg['val_col'],
            ]
            if all(self._identifier_is_safe(name) for name in identifiers):
                valid_configs.append(cfg)
            else:
                self.logger.warning(f"Skipping ranged cacher config with invalid identifiers: {cfg}")
        return valid_configs

    def build_query(self, config) -> str:
        return (
            f"SELECT DISTINCT {config['min_col']}, {config['max_col']}, "
            f"{config['key_col']}, {config['val_col']} "
            f"FROM {config['table']}"
        )

    def query_table(self, config):
        """Load range metadata from a configured ClickHouse table."""
        logger.info(f"Loading ranged metadata from table: {config['table']}")

        try:
            query = self.build_query(config)
            logger.debug(f"Executing ranged query: {query}")

            start_time = time.time()
            result = self.cache.client.query(query)
            query_time = time.time() - start_time
            if not result:
                logger.warning(f"No result returned from ranged query for table {config['table']}")
                return None

            rows = result.result_rows
            logger.info(
                f"Ranged query completed in {query_time:.2f}s, found {len(rows)} records for {config['table']}"
            )

            if not rows:
                logger.warning(f"No ranged data found in table {config['table']}")
                return None

            return rows

        except Exception as e:
            logger.error(f"Failed to load ranged metadata from table {config['table']}: {e}")
            raise

    def prime(self):
        for config in self.range_configs:
            self.prime_table(config)

    def prime_table(self, config):
        """Preload range records as key -> range_val -> id."""
        if self.cache.client is None:
            logger.debug("ClickHouse client not available, skipping ranged cache priming")
            return

        table = config['table']
        lookup_table = config['lookup_table']
        try:
            rows = self.query_table(config)
            if not rows:
                logger.debug(f"No rows returned for ranged table {table}, skipping priming")
                return

            table_cache = {}
            expanded_range_vals = 0
            for row in rows:
                if len(row) < 4:
                    self.logger.debug(f"Skipping ranged row with insufficient columns: {row}")
                    continue

                range_min, range_max, row_key, row_val = row[0], row[1], row[2], row[3]
                if range_min is None or range_max is None or row_key is None:
                    continue

                try:
                    start_range = int(range_min)
                    end_range = int(range_max)
                except (TypeError, ValueError):
                    self.logger.debug(f"Skipping ranged row with invalid range vals: {row}")
                    continue

                if end_range < start_range:
                    start_range, end_range = end_range, start_range

                lookup_key = str(row_key).lower()
                key_cache = table_cache.setdefault(lookup_key, {})
                for range_val in range(start_range, end_range + 1):
                    key_cache[range_val] = row_val
                    expanded_range_vals += 1

            self.local_cache[lookup_table] = table_cache
            logger.info(
                "Loaded ranged cache for "
                f"{lookup_table} (source={table}): keys={len(table_cache)}, expanded_ranges={expanded_range_vals}"
            )
        except Exception as e:
            logger.info(f"Could not load ranged metadata for table {table}: {e}")

    def lookup(self, table, key: str) -> Optional[str]:
        if key is None or table is None:
            return None

        lookup_key = None
        range_val = None

        if isinstance(key, (tuple, list)) and len(key) == 2:
            lookup_key, range_val = key[0], key[1]
        elif isinstance(key, str) and ':' in key:
            lookup_key, range_val = key.split(':', 1)
        else:
            return None

        try:
            range_val = int(range_val)
        except (TypeError, ValueError):
            return None

        lookup_key = str(lookup_key).lower()
        table_cache = self.local_cache.get(table, {})
        return table_cache.get(lookup_key, {}).get(range_val, None)

    def lookup_key_range(self, table: str, key: str, range_val: int) -> Optional[str]:
        return self.lookup(table, (key, range_val))
