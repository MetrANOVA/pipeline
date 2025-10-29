
from collections import defaultdict
import csv
import logging
import time
import orjson
import pycountry
import pycountry_convert
import pytricia
import os

import yaml
from metranova.consumers.base import TimedIntervalConsumer
from metranova.consumers.file import YAMLFileConsumer

logger = logging.getLogger(__name__)

class YAMLFileConsumer(YAMLFileConsumer):
    def __init__(self, pipeline, env_prefix = ''):
        super().__init__(pipeline, env_prefix)
        self.logger = logger

    def handle_file_data(self, file_path, data):
        if data is None:
            return
        # Process metadata
        table = data.get('table', None)
        if table is None:
            self.logger.error(f"No table found in file: {file_path}")
            return
        self.pipeline.process_message({'table': table, 'data': data.get('data', [])})

class CAIDAOrgASConsumer(TimedIntervalConsumer):
    """Consumer to load CAIDA AS to Org mapping and PeeringDB data from files and format as meta_organization metadata and meta_as data"""
    def __init__(self, pipeline):
        super().__init__(pipeline)
        # Initial values
        self.logger = logger
        self.datasource = None  # No external datasource needed for file reading
        self.update_interval = int(os.getenv(f'CAIDA_ORG_AS_CONSUMER_UPDATE_INTERVAL', -1))
        #load table name from env
        self.as_table = os.getenv('CAIDA_ORG_AS_CONSUMER_AS_TABLE', 'meta_as')
        self.org_table = os.getenv('CAIDA_ORG_AS_CONSUMER_ORG_TABLE', 'meta_organization')
        #load files from env
        self.as2org_file = os.getenv('CAIDA_ORG_AS_CONSUMER_AS2ORG_FILE', '/app/caches/caida_as_org2info.jsonl')
        self.peeringdb_file = os.getenv('CAIDA_ORG_AS_CONSUMER_PEERINGDB_FILE', '/app/caches/caida_peeringdb.json')
        self.custom_org_file = os.getenv('CAIDA_ORG_AS_CONSUMER_CUSTOM_ORG_FILE', None)  # Optional custom additions
        self.custom_as_file = os.getenv('CAIDA_ORG_AS_CONSUMER_CUSTOM_AS_FILE', None)  # Optional custom additions  
    
    def _lookup_country_name(self, country_code):
        """Helper function to lookup country name from country code"""
        if not country_code:
            return None
        try:
            country = pycountry.countries.get(alpha_2=country_code)
            return country.name if country else None
        except Exception as e:
            self.logger.debug(f"Error looking up country name for code {country_code}: {e}")
            return None

    def _lookup_continent_name(self, country_code):
        """Helper function to lookup continent name from country code"""
        if not country_code:
            return None
        continent_name = None
        try:
            continent_code = pycountry_convert.country_alpha2_to_continent_code(country_code)
            continent_name = pycountry_convert.convert_continent_code_to_continent_name(continent_code)
        except Exception as e:
            self.logger.debug(f"Error looking up continent name for country code {country_code}: {e}")
        
        return continent_name

    def _lookup_subdivision_name(self, country_code, subdivision_code):
        """Helper function to lookup subdivision name from country and subdivision codes"""
        try:
            subdivisions = pycountry.subdivisions.get(code=f"{country_code}-{subdivision_code}")
            return subdivisions.name if subdivisions else None
        except Exception as e:
            self.logger.error(f"Error looking up subdivision name for code {country_code}-{subdivision_code}: {e}")
            return None

    def _load_caida_data(self, as_objs, org_objs):
        """Load CAIDA AS to Org mapping data"""
        try:
            with open(self.as2org_file, 'r') as f:
                for line in f:
                    line_json = orjson.loads(line)
                    if line_json.get('type') == 'Organization':
                        if line_json.get('organizationId', None) is None:
                            continue
                        org_id = f"caida:{line_json['organizationId']}"
                        org_objs[org_id] = {
                            'id': org_id,
                            'name': line_json.get('name', "Delegated"),  # when CAIDA doesn't have a name then mark "Delegated" - this is meaning of @del tag in orgId
                            'ext': {"data_source": ["CAIDA"]}
                        }
                        if line_json.get('country', None):
                            org_objs[org_id]['country_code'] = line_json['country']
                            country_name = self._lookup_country_name(line_json['country'])
                            if country_name:
                                org_objs[org_id]['country_name'] = country_name
                            continent_name = self._lookup_continent_name(line_json['country'])
                            if continent_name:
                                org_objs[org_id]['continent_name'] = continent_name
                    elif line_json.get('type') == 'ASN':
                        if line_json.get('asn', None) is None or line_json.get('organizationId', None) is None or line_json.get('name', None) is None:
                            continue
                        try:
                            as_id = int(line_json['asn'])
                        except ValueError:
                            continue
                        as_objs[as_id] = {
                            'id': as_id,
                            'organization_id': f"caida:{line_json['organizationId']}",
                            'name': line_json.get('name', None),
                            'ext': {"data_source": ["CAIDA"]}
                        }
        except FileNotFoundError as e:
            self.logger.error(f"AS to Org file not found: {self.as2org_file}. Error: {e}")
        except Exception as e:
            self.logger.error(f"Error processing AS to Org file {self.as2org_file}: {e}")

    def _load_peeringdb_data(self):
        """Load PeeringDB data from file"""
        peeringdb_data = None
        try:
            with open(self.peeringdb_file, 'r') as f:
                peeringdb_data = orjson.loads(f.read())
        except FileNotFoundError as e:
            self.logger.error(f"PeeringDB file not found: {self.peeringdb_file}. Error: {e}")
        except Exception as e:
            self.logger.error(f"Error processing PeeringDB file {self.peeringdb_file}: {e}")
        return peeringdb_data

    def _process_peeringdb_organizations(self, peeringdb_data):
        """Build index of PeeringDB organizations by org id"""
        peeringdb_org_objs = {}
        if peeringdb_data:
            for org_record in peeringdb_data.get('org', {}).get('data', []):
                org_id = org_record.get('id', None)
                if org_id is None:
                    continue
                peeringdb_org_objs[f"peeringdb:{org_id}"] = org_record
        return peeringdb_org_objs

    def _process_peeringdb_networks(self, peeringdb_data, peeringdb_org_objs, as_objs, org_objs):
        """Process PeeringDB network data and supplement AS/org objects"""
        if not peeringdb_data:
            return

        for as_record in peeringdb_data.get('net', {}).get('data', []):
            as_id = as_record.get('asn', None)
            org_id = as_record.get('org_id', None)
            if as_id is None or org_id is None:
                continue
            
            # Build proper org_id and make sure it exists
            org_id = f"peeringdb:{org_id}"
            if org_id not in peeringdb_org_objs:
                continue
            
            try:
                as_id = int(as_id)
            except ValueError:
                continue
            
            # Create AS record if it doesn't exist
            if as_id not in as_objs:
                if as_record.get('name', None) is None:
                    continue
                as_objs[as_id] = {
                    'id': as_id,
                    'organization_id': org_id,
                    'name': as_record.get('name', None),
                    'ext': {"data_source": []}
                }
                
                # Create organization record if it doesn't exist
                if org_id not in org_objs:
                    org_objs[org_id] = {
                        'id': org_id,
                        'name': peeringdb_org_objs[org_id].get('name', None),
                        'ext': {"data_source": []}
                    }
                    if peeringdb_org_objs[org_id].get('country', None):
                        org_objs[org_id]['country_code'] = peeringdb_org_objs[org_id]['country']
                        country_name = self._lookup_country_name(peeringdb_org_objs[org_id]['country'])
                        if country_name:
                            org_objs[org_id]['country_name'] = country_name
                        continent_name = self._lookup_continent_name(peeringdb_org_objs[org_id]['country'])
                        if continent_name:
                            org_objs[org_id]['continent_name'] = continent_name

            # Update AS with PeeringDB data
            if "PeeringDB" not in as_objs[as_id]['ext']['data_source']:
                as_objs[as_id]['ext']['data_source'].append("PeeringDB")
                as_objs[as_id]['name'] = as_record['name']  # replace name if peering db has a different name
                as_objs[as_id]['ext'].update({
                    'peeringdb_ipv6': as_record.get('info_ipv6', None),
                    'peeringdb_prefixes4': as_record.get('info_prefixes4', None),
                    'peeringdb_prefixes6': as_record.get('info_prefixes6', None),
                    'peeringdb_ratio': as_record.get('info_ratio', None),
                    'peeringdb_scope': as_record.get('info_scope', None),
                    'peeringdb_traffic': as_record.get('info_traffic', None),
                    'peeringdb_type': as_record.get('info_types', None)
                })
            
            # Update organization with PeeringDB data
            current_org = org_objs[as_objs[as_id]['organization_id']]
            if not current_org or current_org.get('ext', {}).get('data_source', None) is None:
                self.logger.warning(f"Organization object missing for AS {as_id} with organization ID {as_objs[as_id]['organization_id']}")
                continue
            
            if "PeeringDB" not in current_org['ext']['data_source']:
                current_org['ext']['data_source'].append("PeeringDB")
                # Set organization location data if not already set
                if not current_org.get('city_name', None):
                    current_org['city_name'] = peeringdb_org_objs[org_id].get('city', None)
                if not current_org.get('country_code', None) and peeringdb_org_objs[org_id].get('country', None):
                    current_org['country_code'] = peeringdb_org_objs[org_id]['country']
                    country_name = self._lookup_country_name(peeringdb_org_objs[org_id]['country'])
                    if country_name:
                        current_org['country_name'] = country_name
                    continent_name = self._lookup_continent_name(peeringdb_org_objs[org_id]['country'])
                    if continent_name:
                        current_org['continent_name'] = continent_name
                if not current_org.get('latitude', None):
                    current_org['latitude'] = peeringdb_org_objs[org_id].get('latitude', None)
                if not current_org.get('longitude', None):
                    current_org['longitude'] = peeringdb_org_objs[org_id].get('longitude', None)
                if not current_org.get('country_sub_code', None) and peeringdb_org_objs[org_id].get('state', None):
                    current_org['country_sub_code'] = peeringdb_org_objs[org_id]['state']
                    if current_org.get('country_code', None) is not None:
                        subdivision_name = self._lookup_subdivision_name(current_org['country_code'], current_org['country_sub_code'])
                        if subdivision_name:
                            current_org['country_sub_name'] = subdivision_name

    def _load_custom_data(self, as_objs, org_objs):
        """Load custom organization and AS additions from YAML files"""
        # Handle org additions from YAML
        if self.custom_org_file:
            try:
                with open(self.custom_org_file, 'r') as f:
                    custom_org_data = yaml.safe_load(f)
                    for record in custom_org_data.get('data', []):
                        org_id = record.get('id', None)
                        if org_id is None:
                            continue
                        org_objs[org_id].update(record)
            except FileNotFoundError as e:
                self.logger.error(f"Custom organization file not found: {self.custom_org_file}. Error: {e}")
            except Exception as e:
                self.logger.error(f"Error processing custom organization file {self.custom_org_file}: {e}")
        
        # Handle AS additions from YAML
        if self.custom_as_file:
            try:
                with open(self.custom_as_file, 'r') as f:
                    custom_as_data = yaml.safe_load(f)
                    for record in custom_as_data.get('data', []):
                        as_id = int(record.get('id', None))
                        if as_id is None:
                            continue
                        as_objs[as_id].update(record)
            except FileNotFoundError as e:
                self.logger.error(f"Custom AS file not found: {self.custom_as_file}. Error: {e}")
            except Exception as e:
                self.logger.error(f"Error processing custom AS file {self.custom_as_file}: {e}")

    def _emit_messages(self, as_objs, org_objs):
        """Emit organization and AS messages to the pipeline"""
        # Emit organization messages first
        self.pipeline.process_message({'table': self.org_table, 'data': list(org_objs.values())})
        
        # Wait to emit AS messages until after organization messages so that any foreign key references to organization_id will resolve
        # TODO: Add a way to flush writers and wait for completion instead of fixed sleep
        self.logger.info("Waiting 10 seconds before emitting AS messages to allow organization metadata to be processed first...")
        time.sleep(10)
        
        # Prime clickhouse cacher for organization metadata to avoid foreign key issues
        self.pipeline.cacher("clickhouse").prime()

        # Emit AS messages
        self.pipeline.process_message({'table': self.as_table, 'data': list(as_objs.values())})

    def consume_messages(self):
        """Main method to consume and process CAIDA and PeeringDB data"""
        # Initialize data containers
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        
        # Load CAIDA AS to Org mapping
        self._load_caida_data(as_objs, org_objs)
        
        # Load and process PeeringDB data
        peeringdb_data = self._load_peeringdb_data()
        peeringdb_org_objs = self._process_peeringdb_organizations(peeringdb_data)
        self._process_peeringdb_networks(peeringdb_data, peeringdb_org_objs, as_objs, org_objs)
        
        # Load custom data additions
        self._load_custom_data(as_objs, org_objs)
        
        # Emit messages to pipeline
        self._emit_messages(as_objs, org_objs)

class IPGeolocationCSVConsumer(TimedIntervalConsumer):
    """Consumer to load geolocation data from a series of CSV files and store in ClickHouse meta_ip table"""
    def __init__(self, pipeline):
        super().__init__(pipeline)
        # Initial values
        self.logger = logger
        self.datasource = None  # No external datasource needed for file reading
        self.update_interval = int(os.getenv(f'IP_GEO_CSV_CONSUMER_UPDATE_INTERVAL', -1))
        #load table name from env
        self.table = os.getenv('IP_GEO_CSV_CONSUMER_TABLE', 'meta_ip')
        #load files from env
        self.asn_files = self.parse_file_list(os.getenv('IP_GEO_CSV_CONSUMER_ASN_FILES', '/app/caches/ip_geo_asn.csv'))
        self.location_files = self.parse_file_list(os.getenv('IP_GEO_CSV_CONSUMER_LOCATION_FILES', '/app/caches/ip_geo_location.csv'))
        self.ip_block_files = self.parse_file_list(os.getenv('IP_GEO_CSV_CONSUMER_IP_BLOCK_FILES', '/app/caches/ip_geo_ip_blocks.csv'))
        self.custom_ip_file = os.getenv('IP_GEO_CSV_CONSUMER_CUSTOM_IP_FILE', None)  # Optional custom additions

    def parse_file_list(self, file_list_str: str):
        """Parse a comma-separated string of file paths into a list."""
        if not file_list_str:
            return []
        return [file_path.strip() for file_path in file_list_str.split(',') if file_path.strip()]

    def ip_subnet_to_str(self, ip_subnet: list):
        if not ip_subnet:
            return None
        if not isinstance(ip_subnet, (list, tuple)) or len(ip_subnet) != 2:
            self.logger.error("IP subnet must be a list of [prefix, length]")
            return None
        return f"{ip_subnet[0]}/{ip_subnet[1]}"

    def ip_subnet_to_tuple(self, ip_subnet: str):
        if not ip_subnet:
            return None
        parts = ip_subnet.split('/')
        if len(parts) != 2:
            self.logger.error("IP subnet must be in the format 'prefix/length'")
            return None
        return (parts[0], int(parts[1]))

    def consume_messages(self):
        #Open the asn files for reading
        ip_to_asn_trie = pytricia.PyTricia(128)
        for asn_file in self.asn_files:
            try:
                with open(asn_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        net = row.get('network', None)
                        asn = row.get('autonomous_system_number', None)
                        if net and asn:
                            ip_to_asn_trie[net] = int(asn)
            except FileNotFoundError as e:
                self.logger.error(f"ASN file not found: {asn_file}. Error: {e}")
            except Exception as e:
                self.logger.error(f"Error processing ASN file {asn_file}: {e}")
        #open location files for reading and build location code mapping
        #CSV columns: geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name,subdivision_1_iso_code,subdivision_1_name,subdivision_2_iso_code,subdivision_2_name,city_name,metro_code,time_zone,is_in_european_union
        location_code_map = {}
        for location_file in self.location_files:
            try:
                with open(location_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        geo_id = row.get('geoname_id', None)
                        if not geo_id:
                            continue
                        location_code_map[geo_id] = {
                            'continent_name': row.get('continent_name', None),
                            'country_code': row.get('country_iso_code', None),
                            'country_name': row.get('country_name', None),
                            'country_sub_code': row.get('subdivision_1_iso_code', None),
                            'country_sub_name': row.get('subdivision_1_name', None),
                            'city_name': row.get('city_name', None)
                        }
            except FileNotFoundError as e:
                self.logger.error(f"Location file not found: {location_file}. Error: {e}")
            except Exception as e:
                self.logger.error(f"Error processing Location file {location_file}: {e}")

        # Load custom ip additions from YAML file
        custom_ip_data = defaultdict(dict)
        if self.custom_ip_file:
            try:
                with open(self.custom_ip_file, 'r') as f:
                    custom_ip_data = yaml.safe_load(f)
                    for record in custom_ip_data.get('data', []):
                        record_id = record.get('id', None)
                        if record_id is None:
                            continue
                        custom_ip_data[record_id] = record
            except FileNotFoundError as e:
                self.logger.error(f"Custom IP file not found: {self.custom_ip_file}. Error: {e}")
            except Exception as e:
                self.logger.error(f"Error processing custom IP file {self.custom_ip_file}: {e}")

        # Open ip_block files for reading and build ip objects
        # CSV Column names: network,geoname_id,registered_country_geoname_id,represented_country_geoname_id,is_anonymous_proxy,is_satellite_provider,postal_code,latitude,longitude,accuracy_radius,is_anycast
        for ip_block_file in self.ip_block_files:
            ip_obj = {}
            try:
                with open(ip_block_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        #set ip_subnet - ned to format as list of tuple for database
                        network = row.get('network', None)
                        if not network:
                            continue
                        ip_obj['id'] = network  # ensure entry exists
                        ip_subnet_tuple = self.ip_subnet_to_tuple(network)
                        ip_obj['ip_subnet'] = [ip_subnet_tuple]
                        #lookup asn info in trie
                        asn = ip_to_asn_trie.get(network, None)
                        if asn:
                            ip_obj['as_id'] = asn
                        #lookup location info
                        geo_id = row.get('geoname_id', None)
                        if geo_id and geo_id in location_code_map:
                            ip_obj.update(location_code_map[geo_id])
                        # add remaining fields directly from ip block file
                        ip_obj.update({
                            'latitude': row.get('latitude', None),
                            'longitude': row.get('longitude', None)
                        })
                        #merge with any custom data
                        if custom_ip_data.get(network, None):
                            ip_obj.update(custom_ip_data[network])
                            del custom_ip_data[network]  # remove from custom data so we can add any remaining later
                        #now process the record - do this here to avoid memory issues with large files
                        #TODO: Possibly speed up imports by batching records instead of one at a time
                        self.pipeline.process_message({'table': self.table, 'data': [ip_obj]})
            except FileNotFoundError as e:
                self.logger.error(f"IP Block file not found: {ip_block_file}. Error: {e}")
            except Exception as e:
                self.logger.error(f"Error processing IP Block file {ip_block_file}: {e}")
        
        # Process any remaining custom IP records that were not in the ip block files
        self.pipeline.process_message({'table': self.table, 'data': list(custom_ip_data.values())})