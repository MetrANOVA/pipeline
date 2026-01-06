
from collections import defaultdict
import csv
import gzip
import logging
import time
import orjson
import pycountry
import pycountry_convert
import pytricia
import os
import requests
from datetime import datetime, timedelta
from io import BytesIO
import zipfile
import shutil
import glob

import yaml
from metranova.consumers.base import TimedIntervalConsumer
from metranova.consumers.file import YAMLFileConsumer

from metranova.connectors.http import HTTPConnector
from metranova.pipelines.base import BasePipeline

logger = logging.getLogger(__name__)

class YAMLFileConsumer(YAMLFileConsumer):
    def __init__(self, pipeline):
        super().__init__(pipeline)
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
        
        # Determine whether to use URLs or local files
        self.use_url = os.getenv('CAIDA_ORG_AS_CONSUMER_USE_URL', 'true').lower() in ['true', '1', 'yes']
        
        # URL configurations
        self.as2org_url_template = os.getenv('CAIDA_ORG_AS_CONSUMER_AS2ORG_URL', 'https://publicdata.caida.org/datasets/as-organizations/{date}.as-org2info.jsonl.gz')
        self.peeringdb_url_template = os.getenv('CAIDA_ORG_AS_CONSUMER_PEERINGDB_URL', 'https://publicdata.caida.org/datasets/peeringdb/{year}/{month}/peeringdb_2_dump_{year}_{month}_{day}.json')
        
        #load files from env (fallback to local files)
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
    
    def _download_file_with_fallback(self, url, max_retries=3):
        """Download a file from a URL with retries"""
        for attempt in range(max_retries):
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    return response.content
                elif response.status_code == 404:
                    self.logger.warning(f"File not found at {url}")
                    return None
                else:
                    self.logger.warning(f"Attempt {attempt + 1}/{max_retries}: Got status {response.status_code} from {url}")
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1}/{max_retries}: Error downloading from {url}: {e}")
        
        return None
    
    def _get_caida_as2org_url(self, date):
        """Generate CAIDA AS2Org URL for a given date"""
        date_str = date.strftime('%Y%m%d')
        return self.as2org_url_template.format(date=date_str)
    
    def _get_peeringdb_url(self, date):
        """Generate PeeringDB URL for a given date"""
        return self.peeringdb_url_template.format(
            year=date.strftime('%Y'),
            month=date.strftime('%m'),
            day=date.strftime('%d')
        )
    
    def _fetch_caida_data_from_url(self):
        """Fetch CAIDA AS2Org data from URL with monthly fallback"""
        now = datetime.now()
        # Try current month (1st day) first, then previous month
        for months_back in range(2):
            target_date = (now.replace(day=1) - timedelta(days=months_back * 30)).replace(day=1)
            url = self._get_caida_as2org_url(target_date)
            self.logger.info(f"Attempting to download CAIDA AS2Org data from {url}")
            
            content = self._download_file_with_fallback(url)
            if content:
                try:
                    # Decompress gzip data
                    decompressed = gzip.decompress(content)
                    return decompressed.decode('utf-8')
                except Exception as e:
                    self.logger.error(f"Error decompressing CAIDA data from {url}: {e}")
        
        self.logger.error("Failed to fetch CAIDA AS2Org data from URL after trying current and previous month")
        return None
    
    def _fetch_peeringdb_data_from_url(self):
        """Fetch PeeringDB data from URL with daily fallback"""
        now = datetime.now()
        # Try today, yesterday, and day before
        for days_back in range(3):
            target_date = now - timedelta(days=days_back)
            url = self._get_peeringdb_url(target_date)
            self.logger.info(f"Attempting to download PeeringDB data from {url}")
            
            content = self._download_file_with_fallback(url)
            if content:
                try:
                    return orjson.loads(content)
                except Exception as e:
                    self.logger.error(f"Error parsing PeeringDB JSON from {url}: {e}")
        
        self.logger.error("Failed to fetch PeeringDB data from URL after trying last 3 days")
        return None

    def _load_caida_data(self, as_objs, org_objs):
        """Load CAIDA AS to Org mapping data from URL or local file"""
        data_content = None
        
        # Try to load from URL if configured
        if self.use_url:
            data_content = self._fetch_caida_data_from_url()
            if data_content:
                self.logger.info("Successfully fetched CAIDA AS2Org data from URL")
        
        # Fallback to local file if URL fetch failed or not configured
        if not data_content:
            self.logger.info(f"Loading CAIDA AS2Org data from local file: {self.as2org_file}")
            try:
                with open(self.as2org_file, 'r') as f:
                    data_content = f.read()
            except FileNotFoundError as e:
                self.logger.error(f"AS to Org file not found: {self.as2org_file}. Error: {e}")
                return
            except Exception as e:
                self.logger.error(f"Error reading AS to Org file {self.as2org_file}: {e}")
                return
        
        # Process the data (same logic regardless of source)
        if not data_content:
            self.logger.error("No CAIDA AS2Org data available to process")
            return
        
        try:
            for line in data_content.splitlines():
                if not line.strip():
                    continue
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
        except Exception as e:
            self.logger.error(f"Error processing CAIDA AS2Org data: {e}")

    def _load_peeringdb_data(self):
        """Load PeeringDB data from URL or local file"""
        peeringdb_data = None
        
        # Try to load from URL if configured
        if self.use_url:
            peeringdb_data = self._fetch_peeringdb_data_from_url()
            if peeringdb_data:
                self.logger.info("Successfully fetched PeeringDB data from URL")
        
        # Fallback to local file if URL fetch failed or not configured
        if not peeringdb_data:
            self.logger.info(f"Loading PeeringDB data from local file: {self.peeringdb_file}")
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
        
        # Determine whether to download from MaxMind or use local files
        self.use_maxmind_download = os.getenv('IP_GEO_CSV_CONSUMER_USE_MAXMIND_DOWNLOAD', 'true').lower() in ['true', '1', 'yes']
        
        # MaxMind credentials
        self.maxmind_account_id = os.getenv('IP_GEO_CSV_CONSUMER_MAXMIND_ACCOUNT_ID', None)
        self.maxmind_license_key = os.getenv('IP_GEO_CSV_CONSUMER_MAXMIND_LICENSE_KEY', None)
        
        # Cache directory for downloads
        self.cache_dir = os.getenv('IP_GEO_CSV_CONSUMER_CACHE_DIR', '/app/caches')
        
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
    
    def _download_maxmind_file(self, database_name):
        """Download a MaxMind GeoLite2 database file"""
        if not self.maxmind_account_id or not self.maxmind_license_key:
            self.logger.error(f"MaxMind credentials not configured for {database_name}")
            return None
        
        url = f'https://download.maxmind.com/geoip/databases/{database_name}/download?suffix=zip'
        zip_path = os.path.join(self.cache_dir, f'{database_name}.zip')
        
        self.logger.info(f"Downloading {database_name} from MaxMind...")
        
        try:
            response = requests.get(
                url,
                auth=(self.maxmind_account_id, self.maxmind_license_key),
                timeout=300,  # 5 minute timeout for large files
                stream=True
            )
            
            if response.status_code == 200:
                with open(zip_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                self.logger.info(f"Successfully downloaded {database_name} to {zip_path}")
                return zip_path
            elif response.status_code == 401:
                self.logger.error(f"MaxMind authentication failed for {database_name}. Check credentials.")
                return None
            else:
                self.logger.error(f"Failed to download {database_name}: HTTP {response.status_code}")
                return None
        except Exception as e:
            self.logger.error(f"Error downloading {database_name}: {e}")
            return None
    
    def _extract_maxmind_zip(self, zip_path, database_name, file_mappings):
        """Extract specific files from MaxMind zip archive"""
        extract_dir = os.path.join(self.cache_dir, database_name)
        
        try:
            # Extract zip file
            self.logger.info(f"Extracting {zip_path} to {extract_dir}")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Find the versioned directory (e.g., GeoLite2-ASN-CSV_20231215)
            subdirs = glob.glob(os.path.join(extract_dir, f'{database_name}_*'))
            if not subdirs:
                self.logger.error(f"No versioned subdirectory found in {extract_dir}")
                return False
            
            versioned_dir = subdirs[0]
            self.logger.info(f"Found versioned directory: {versioned_dir}")
            
            # Copy files to target locations
            for source_filename, target_path in file_mappings.items():
                source_path = os.path.join(versioned_dir, source_filename)
                if os.path.exists(source_path):
                    shutil.copy2(source_path, target_path)
                    self.logger.info(f"Copied {source_filename} to {target_path}")
                else:
                    self.logger.warning(f"File not found: {source_path}")
            
            # Cleanup: remove zip and extracted directory
            os.remove(zip_path)
            shutil.rmtree(extract_dir)
            self.logger.info(f"Cleaned up temporary files for {database_name}")
            
            return True
        except Exception as e:
            self.logger.error(f"Error extracting {database_name}: {e}")
            return False
    
    def _download_maxmind_asn_data(self):
        """Download and extract MaxMind GeoLite2 ASN data"""
        database_name = 'GeoLite2-ASN-CSV'
        zip_path = self._download_maxmind_file(database_name)
        
        if not zip_path:
            return False
        
        file_mappings = {
            'GeoLite2-ASN-Blocks-IPv4.csv': os.path.join(self.cache_dir, 'GeoLite2-ASN-Blocks-IPv4.csv'),
            'GeoLite2-ASN-Blocks-IPv6.csv': os.path.join(self.cache_dir, 'GeoLite2-ASN-Blocks-IPv6.csv')
        }
        
        return self._extract_maxmind_zip(zip_path, database_name, file_mappings)
    
    def _download_maxmind_city_data(self):
        """Download and extract MaxMind GeoLite2 City data"""
        database_name = 'GeoLite2-City-CSV'
        zip_path = self._download_maxmind_file(database_name)
        
        if not zip_path:
            return False
        
        file_mappings = {
            'GeoLite2-City-Blocks-IPv4.csv': os.path.join(self.cache_dir, 'GeoLite2-City-Blocks-IPv4.csv'),
            'GeoLite2-City-Blocks-IPv6.csv': os.path.join(self.cache_dir, 'GeoLite2-City-Blocks-IPv6.csv'),
            'GeoLite2-City-Locations-en.csv': os.path.join(self.cache_dir, 'GeoLite2-City-Locations-en.csv')
        }
        
        return self._extract_maxmind_zip(zip_path, database_name, file_mappings)
    
    def _ensure_maxmind_data(self):
        """Ensure MaxMind data files are available, downloading if necessary"""
        if not self.use_maxmind_download:
            self.logger.info("MaxMind download disabled, using existing local files")
            return True
        
        if not self.maxmind_account_id or not self.maxmind_license_key:
            self.logger.warning("MaxMind credentials not configured, using existing local files")
            return True
        
        # Create cache directory if it doesn't exist
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Download ASN data
        self.logger.info("Downloading MaxMind ASN data...")
        if not self._download_maxmind_asn_data():
            self.logger.error("Failed to download MaxMind ASN data")
            return False
        
        # Download City data
        self.logger.info("Downloading MaxMind City data...")
        if not self._download_maxmind_city_data():
            self.logger.error("Failed to download MaxMind City data")
            return False
        
        self.logger.info("Successfully downloaded and extracted all MaxMind data files")
        return True

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

    def _load_asn_data(self):
        """Load ASN data from CSV files into a PyTricia trie"""
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
        return ip_to_asn_trie

    def _load_location_data(self):
        """Load location data from CSV files and build location code mapping"""
        # CSV columns: geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name,subdivision_1_iso_code,subdivision_1_name,subdivision_2_iso_code,subdivision_2_name,city_name,metro_code,time_zone,is_in_european_union
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
        return location_code_map

    def _load_custom_ip_data(self):
        """Load custom IP additions from YAML file"""
        custom_ip_data = defaultdict(dict)
        if self.custom_ip_file:
            try:
                with open(self.custom_ip_file, 'r') as f:
                    yaml_data = yaml.safe_load(f)
                    for record in yaml_data.get('data', []):
                        record_id = record.get('id', None)
                        if record_id is None:
                            continue
                        custom_ip_data[record_id] = record
            except FileNotFoundError as e:
                self.logger.error(f"Custom IP file not found: {self.custom_ip_file}. Error: {e}")
            except Exception as e:
                self.logger.error(f"Error processing custom IP file {self.custom_ip_file}: {e}")
        return custom_ip_data

    def _build_ip_object(self, row, ip_to_asn_trie, location_code_map, custom_ip_data):
        """Build an IP object from CSV row data and lookup tables"""
        network = row.get('network', None)
        if not network:
            return None, custom_ip_data
        
        ip_obj = {'id': network}
        
        # Set ip_subnet - need to format as list of tuple for database
        ip_subnet_tuple = self.ip_subnet_to_tuple(network)
        ip_obj['ip_subnet'] = [ip_subnet_tuple]
        
        # Lookup ASN info in trie
        asn = ip_to_asn_trie.get(network, None)
        if asn:
            ip_obj['as_id'] = asn
        
        # Lookup location info
        geo_id = row.get('geoname_id', None)
        if geo_id and geo_id in location_code_map:
            ip_obj.update(location_code_map[geo_id])
        
        # Add remaining fields directly from ip block file
        ip_obj.update({
            'latitude': row.get('latitude', None),
            'longitude': row.get('longitude', None)
        })
        
        # Merge with any custom data
        if custom_ip_data.get(network, None):
            ip_obj.update(custom_ip_data[network])
            del custom_ip_data[network]  # remove from custom data so we can add any remaining later
        
        return ip_obj, custom_ip_data

    def _process_ip_block_files(self, ip_to_asn_trie, location_code_map, custom_ip_data):
        """Process IP block files and emit IP objects to pipeline"""
        # CSV Column names: network,geoname_id,registered_country_geoname_id,represented_country_geoname_id,is_anonymous_proxy,is_satellite_provider,postal_code,latitude,longitude,accuracy_radius,is_anycast
        for ip_block_file in self.ip_block_files:
            try:
                with open(ip_block_file, 'r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        ip_obj, custom_ip_data = self._build_ip_object(row, ip_to_asn_trie, location_code_map, custom_ip_data)
                        if ip_obj:
                            # Process the record - do this here to avoid memory issues with large files
                            # TODO: Possibly speed up imports by batching records instead of one at a time
                            self.pipeline.process_message({'table': self.table, 'data': [ip_obj]})
            except FileNotFoundError as e:
                self.logger.error(f"IP Block file not found: {ip_block_file}. Error: {e}")
            except Exception as e:
                self.logger.error(f"Error processing IP Block file {ip_block_file}: {e}")
        
        return custom_ip_data

    def consume_messages(self):
        """Main method to consume and process IP geolocation data from CSV files"""
        # Ensure MaxMind data is available (download if configured)
        if not self._ensure_maxmind_data():
            self.logger.error("Failed to ensure MaxMind data availability")
            return
        
        # Load ASN data
        ip_to_asn_trie = self._load_asn_data()
        
        # Load location data
        location_code_map = self._load_location_data()
        
        # Load custom IP data
        custom_ip_data = self._load_custom_ip_data()
        
        # Process IP block files
        remaining_custom_data = self._process_ip_block_files(ip_to_asn_trie, location_code_map, custom_ip_data)
        
        # Process any remaining custom IP records that were not in the ip block files
        if remaining_custom_data:
            self.pipeline.process_message({'table': self.table, 'data': list(remaining_custom_data.values())})

class CRICIPConsumer(TimedIntervalConsumer):
    """Consumer to load CRIC IP data from CERN API and store in ClickHouse meta_ip_cric table"""
 
    def __init__(self, pipeline: BasePipeline):
        super().__init__(pipeline)
        self.logger = logger
        self.datasource = HTTPConnector()

        # Configuration from environment
        env_prefix = os.getenv('CRIC_IP_CONSUMER_ENV_PREFIX', '')
        if env_prefix and not env_prefix.endswith('_'):
            env_prefix += '_'

        self.update_interval = int(os.getenv(f'{env_prefix}CRIC_IP_CONSUMER_UPDATE_INTERVAL', -1))
        self.table = os.getenv(f'{env_prefix}CRIC_IP_CONSUMER_TABLE', 'meta_ip_cric')
        self.cric_url = os.getenv(
            f'{env_prefix}CRIC_IP_CONSUMER_URL',
            'https://wlcg-cric.cern.ch/api/core/rcsite/query/list/?json'
        )

        # Custom additions file (optional)
        self.custom_ip_file = os.getenv(f'{env_prefix}CRIC_IP_CONSUMER_CUSTOM_IP_FILE', None)
        
        self.logger.info(f"CRIC IP Consumer initialized with URL: {self.cric_url}")
    
    def _fetch_cric_data(self):
        """Fetch CRIC data from API"""
        try:
            self.logger.info(f"Fetching CRIC data from {self.cric_url}")
            response = self.datasource.client.get(self.cric_url, timeout=60)
            response.raise_for_status()

            data = response.json()
            self.logger.info(f"Successfully fetched CRIC data with {len(data)} sites")
            return data
            
        except Exception as e:
            self.logger.error(f"Error fetching CRIC data: {e}")
            return None
    
    def _load_custom_ip_data(self):
        """Load custom IP additions from YML file"""
        custom_ip_data = {}
        if self.custom_ip_file:
            try:
                with open(self.custom_ip_file, 'r') as f:
                    yaml_data = yaml.safe_load(f)
                    for record in yaml_data.get('data', []):
                        record_id = record.get('id', None)
                        if record_id is None:
                            continue
                        custom_ip_data[record_id] = record
                self.logger.info(f"Loaded {len(custom_ip_data)} custom IP records")
            except FileNotFoundError:
                self.logger.warning(f"Custom IP file not found: {self.custom_ip_file}")
            except Exception as e:
                self.logger.error(f"Error loading custom IP file {self.custom_ip_file}: {e}")
        return custom_ip_data
    
    def _parse_ip_subnet(self, ip_subnet_str):
        """Parse IP subnet string into tuple format"""
        if not ip_subnet_str:
            return None
        try:
            parts = ip_subnet_str.split('/')
            if len(parts) != 2:
                self.logger.warning(f"Invalid IP subnet format: {ip_subnet_str}")
                return None
            return (parts[0], int(parts[1]))
        except Exception as e:
            self.logger.error(f"Error parsing IP subnet {ip_subnet_str}: {e}")
            return None
    
    def _extract_ip_records(self, cric_data, custom_ip_data):
        """Extract IP records from CRIC data - simplified to 6 core fields"""
        ip_records = []
        
        if not cric_data:
            self.logger.warning("No CRIC data to process")
            return ip_records
        
        for site_name, site_data in cric_data.items():
            try:
                # Get only the 6 core fields we need
                site_info = {
                    'name': site_name,                             # name field
                    'tier': site_data.get('rc_tier_level', None),  # tier field
                    'country': site_data.get('country', None),     # country field
                    'latitude': site_data.get('latitude', None),   # latitude field
                    'longitude': site_data.get('longitude', None), # longitude field
                }
                
                # Process netroutes (network routes)
                netroutes = site_data.get('netroutes', {})
                if not netroutes:
                    self.logger.debug(f"No netroutes found for site {site_name}")
                    continue
                
                # Iterate through each network route
                for netroute_name, netroute_data in netroutes.items():
                    try:
                        # Get net_site
                        net_site = netroute_data.get('netsite', None)
                        
                        # Get networks (contains IPv4 and IPv6 ranges)
                        networks = netroute_data.get('networks', {})
                        if not networks:
                            self.logger.debug(f"No networks found for netroute {netroute_name} in site {site_name}")
                            continue
                        
                        # Process IPv4 ranges
                        ipv4_ranges = networks.get('ipv4', [])
                        for ip_range in ipv4_ranges:
                            ip_record = self._build_ip_record(
                                ip_range, 
                                site_info,
                                net_site,
                                custom_ip_data
                            )
                            if ip_record:
                                ip_records.append(ip_record)
                        
                        # Process IPv6 ranges
                        ipv6_ranges = networks.get('ipv6', [])
                        for ip_range in ipv6_ranges:
                            ip_record = self._build_ip_record(
                                ip_range,
                                site_info,
                                net_site,
                                custom_ip_data
                            )
                            if ip_record:
                                ip_records.append(ip_record)
                    
                    except Exception as e:
                        self.logger.error(f"Error processing netroute {netroute_name} in site {site_name}: {e}")
                        continue
                    
            except Exception as e:
                self.logger.error(f"Error processing site {site_name}: {e}")
                continue
        
        # Add any remaining custom IP records not in CRIC data
        for record_id, custom_record in custom_ip_data.items():
            ip_records.append(custom_record)
        
        self.logger.info(f"Extracted {len(ip_records)} IP records from CRIC data")
        return ip_records
    
    def _build_ip_record(self, ip_range, site_info, net_site, custom_ip_data):
        """Build an IP record with only the 6 core fields"""
        try:
            # Parse IP subnet
            ip_subnet_tuple = self._parse_ip_subnet(ip_range)
            if not ip_subnet_tuple:
                return None
            
            # Use IP range as the record ID
            record_id = ip_range
            
            # Build the IP record with only required fields
            ip_record = {
                'id': record_id,
                'ip_subnet': [ip_subnet_tuple],
                'name': site_info['name'],    
                'latitude': site_info['latitude'],
                'longitude': site_info['longitude'],
                'country': site_info['country'],
                'net_site': net_site,
                'tier': site_info['tier'],
            }
            
            # Merge with custom data if exists
            if record_id in custom_ip_data:
                ip_record.update(custom_ip_data[record_id])
                del custom_ip_data[record_id]
            
            return ip_record
            
        except Exception as e:
            self.logger.error(f"Error building IP record for {ip_range}: {e}")
            return None
    
    def consume_messages(self):
        """Main method to consume and process CRIC IP data"""
        # Fetch CRIC data from API
        cric_data = self._fetch_cric_data()
        if not cric_data:
            self.logger.error("Failed to fetch CRIC data, skipping this cycle")
            return
        
        # Load custom IP data
        custom_ip_data = self._load_custom_ip_data()
        
        # Extract and process IP records
        ip_records = self._extract_ip_records(cric_data, custom_ip_data)
        
        if not ip_records:
            self.logger.warning("No IP records to process")
            return
        
        # Send to pipeline for processing
        self.pipeline.process_message({
            'table': self.table,
            'data': ip_records
        })
        
        self.logger.info(f"Successfully processed {len(ip_records)} CRIC IP records")