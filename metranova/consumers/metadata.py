
from collections import defaultdict
import logging
import time
import orjson
import pycountry
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
    
    def consume_messages(self):
        # Load AS to Org mapping
        as_objs = defaultdict(dict)
        org_objs = defaultdict(dict)
        try:
            with open(self.as2org_file, 'r') as f:
                for line in f:
                    line_json = orjson.loads(line)
                    if line_json.get('type') == 'Organization':
                        if line_json.get('organizationId', None) is None:
                            continue
                        org_id = f"caida:{line_json['organizationId']}"
                        org_objs[org_id] = {}
                        org_objs[org_id]['id'] = org_id
                        #when CAIDA doesn't have a name then mark "Delegated" - this is meaning of @del tag in orgId
                        org_objs[org_id]['name'] = line_json.get('name', "Delegated") 
                        org_objs[org_id]['ext'] = {"data_source": ["CAIDA"]}
                        if line_json.get('country', None):
                            org_objs[org_id]['country_code'] = line_json['country']
                            #lookup alpha2 code to full country name using pycountry
                            try:
                                country = pycountry.countries.get(alpha_2=line_json['country'])
                                if country:
                                    org_objs[org_id]['country_name'] = country.name
                            except Exception as e:
                                self.logger.error(f"Error looking up country name for code {line_json['country']}: {e}")
                    elif line_json.get('type') == 'ASN':
                        if line_json.get('asn', None) is None or line_json.get('organizationId', None) is None or line_json.get('name', None) is None:
                            continue
                        try:
                            as_id = int(line_json['asn'])
                        except ValueError:
                            continue
                        as_objs[as_id] = {}
                        as_objs[as_id]['id'] = as_id
                        as_objs[as_id]['organization_id'] = f"caida:{line_json['organizationId']}"
                        as_objs[as_id]['name'] = line_json.get('name', None)
                        as_objs[as_id]['ext'] = {"data_source": ["CAIDA"]}
        except FileNotFoundError as e:
            self.logger.error(f"AS to Org file not found: {self.as2org_file}. Error: {e}")
        except Exception as e:
            self.logger.error(f"Error processing AS to Org file {self.as2org_file}: {e}")

        # Load PeeringDB data - supplement the organization data and as data
        ## Open File
        peeringdb_data = None
        try:
            with open(self.peeringdb_file, 'r') as f:
                peeringdb_data = orjson.loads(f.read())
        except FileNotFoundError as e:
            self.logger.error(f"PeeringDB file not found: {self.peeringdb_file}. Error: {e}")
        except Exception as e:
            self.logger.error(f"Error processing PeeringDB file {self.peeringdb_file}: {e}")
        
        ## Process PeeringDB data
        peeringdb_org_objs = {}
        #first build and index of organizations by org id
        for org_record in peeringdb_data.get('org', {}).get('data', []):
            org_id = org_record.get('id', None)
            if org_id is None:
                continue
            peeringdb_org_objs[f"peeringdb:{org_id}"] = org_record
        # now for each as in 'net', grab the org_id and update/create as and organization objects
        for as_record in peeringdb_data.get('net', {}).get('data', []):
            as_id = as_record.get('asn', None)
            org_id = as_record.get('org_id', None)
            if as_id is None or org_id is None:
                continue
            # Build proper org_id and make sure it exists
            org_id = f"peeringdb:{org_id}"
            if org_id not in peeringdb_org_objs:
                continue
            # Now supplement as and org data
            try:
                as_id = int(as_id)
            except ValueError:
                continue
            if as_id not in as_objs:
                if as_record.get('name', None) is None:
                    continue
                #AS was not in CAIDA data, create new
                as_objs[as_id] = {}
                as_objs[as_id]['id'] = as_id
                as_objs[as_id]['organization_id'] = org_id
                as_objs[as_id]['name'] = as_record.get('name', None)
                # Initialize ext data source, will set later
                as_objs[as_id]['ext'] = {"data_source": []}
                # check if org exists and create if not
                if as_objs[as_id]['organization_id'] not in org_objs:
                    org_objs[org_id] = {}
                    org_objs[org_id]['id'] = as_objs[as_id]['organization_id']
                    org_objs[org_id]['name'] = peeringdb_org_objs[org_id].get('name', None)
                    # Init data source, will get updated later 
                    org_objs[org_id]['ext'] = {"data_source": []}
                    if peeringdb_org_objs[org_id].get('country', None):
                        org_objs[org_id]['country_code'] = peeringdb_org_objs[org_id]['country']
                        #lookup alpha2 code to full country name using pycountry
                        try:
                            country = pycountry.countries.get(alpha_2=peeringdb_org_objs[org_id]['country'])
                            if country:
                                org_objs[org_id]['country_name'] = country.name
                        except Exception as e:
                            self.logger.error(f"Error looking up country name for code {peeringdb_org_objs[org_id]['country']}: {e}")
            if "PeeringDB" not in as_objs[as_id]['ext']['data_source']:
                # Update source if exists since we'll be adding PeeringDB
                as_objs[as_id]['ext']['data_source'].append("PeeringDB")
                #replace name if peering db has a different name
                as_objs[as_id]['name'] = as_record['name']
                #add add info_ipv6, info_prefixes4, info_prefixes6, info_scope, info_traffic, and info_types, info_ratio to ext
                as_objs[as_id]['ext'].update({
                    'peeringdb_ipv6': as_record.get('info_ipv6', None),
                    'peeringdb_prefixes4': as_record.get('info_prefixes4', None),
                    'peeringdb_prefixes6': as_record.get('info_prefixes6', None),
                    'peeringdb_ratio': as_record.get('info_ratio', None),
                    'peeringdb_scope': as_record.get('info_scope', None),
                    'peeringdb_traffic': as_record.get('info_traffic', None),
                    'peeringdb_type': as_record.get('info_types', None)
                })
            #get organization object and supplement
            current_org = org_objs[as_objs[as_id]['organization_id']]
            if not current_org or current_org.get('ext', {}).get('data_source', None) is None:
                self.logger.warning(f"Organization object missing for AS {as_id} with organization ID {as_objs[as_id]['organization_id']}")
                continue
            if "PeeringDB" not in current_org['ext']['data_source']:
                current_org['ext']['data_source'].append("PeeringDB")
                # set org city, country(and lookup country_name), latitude, longitude and state (i.e. country_sub_code...also map to country_sub_name) if not already set
                if not current_org.get('city', None):
                    current_org['city'] = peeringdb_org_objs[org_id].get('city', None)
                if not current_org.get('country_code', None) and peeringdb_org_objs[org_id].get('country', None):
                    current_org['country_code'] = peeringdb_org_objs[org_id]['country']
                    #lookup alpha2 code to full country name using pycountry
                    try:
                        country = pycountry.countries.get(alpha_2=peeringdb_org_objs[org_id]['country'])
                        if country:
                            current_org['country_name'] = country.name
                    except Exception as e:
                        self.logger.error(f"Error looking up country name for code {peeringdb_org_objs[org_id]['country']}: {e}")
                if not current_org.get('latitude', None):
                    current_org['latitude'] = peeringdb_org_objs[org_id].get('latitude', None)
                if not current_org.get('longitude', None):
                    current_org['longitude'] = peeringdb_org_objs[org_id].get('longitude', None)
                if not current_org.get('country_sub_code', None) and peeringdb_org_objs[org_id].get('state', None):
                    current_org['country_sub_code'] = peeringdb_org_objs[org_id]['state']
                    #lookup country_sub_name using pycountry
                    try:
                        if current_org.get('country_code', None) is not None:
                            subdivisions = pycountry.subdivisions.get(code=f"{current_org['country_code']}-{current_org['country_sub_code']}")
                            if subdivisions:
                                current_org['country_sub_name'] = subdivisions.name
                    except Exception as e:
                        self.logger.error(f"Error looking up subdivision name for code {current_org['country_code']}-{current_org['state']}: {e}")
        
        #Handle org additions from YAML
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
        #Handle as additions from YAML
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

        #now emit organization messages
        self.pipeline.process_message({'table': self.org_table, 'data': list(org_objs.values())})
        
        #wait to emit AS messages until after organization messages so that any foreign key references to organization_id will resolve
        #TODO: Add a way to flush writers and wait for completion instead of fixed sleep
        self.logger.info("Waiting 10 seconds before emitting AS messages to allow organization metadata to be processed first...")
        time.sleep(10)
        #prime clickhouse cacher for organization metadata to avoid foreign key issues
        self.pipeline.cacher("clickhouse").prime()

        #now emit as messages
        self.pipeline.process_message({'table': self.as_table, 'data': list(as_objs.values())})
