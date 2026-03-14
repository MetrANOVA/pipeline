
import os
import re
from metranova.processors.redis.base import BaseRedisProcessor

class IRIBaseRedisProcessor(BaseRedisProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.required_fields = [['id'], ['self_uri']]
        # override this with pattern to match in URL.
        # Example `/status/resources`, `status/inicidents/.+/events`
        self.expires = int(os.getenv('REDIS_IRI_EXPIRES', '86400'))  # default 1 day
        self.val_id_field = 'id'
        self.url_match = None
        self.table_name = None # override with redis table name
        self.ref_field = None #Override with field containing array of references
    
    def match_message(self, value):
        if self.url_match is not None:
            #apply regex match on message url (value.get('url'))
            url = value.get('url', '')
            #remove trailing query params
            url = url.split('?', 1)[0]
            #remove trailing slash
            url = url.rstrip('/')
            if not url or not re.search(self.url_match, url):
                return False
        return self.has_match_field(value)
    
    def build_message(self, value: dict, msg_metadata: dict) -> list[dict]:
        """Build lookup table entries from the message."""
        #Grab the data field or return if doesn;t exist
        value_data = value.get('data', None)
        if not value_data:
            self.logger.warning(f"No 'data' field in message: {value}")
            return []
        #init results array
        results = []
        #check if value_data is a list, if not make it a list so we can process uniformly
        if not isinstance(value_data, list):
            value_data = [value_data]
        for val in value_data:
            if not self.has_required_fields(val):
                return []

            #Extract the resource id
            resource_id = val.get(self.val_id_field, None)
            if not resource_id:
                self.logger.warning(f"Could not extract resource id using path {self.val_id_field} from message: {val}")
                return []

            #Walkthrough refences and map ids
            ref_urls = val.get(self.ref_field, []) if self.ref_field else []
            for ref_url in ref_urls:
                #Extract a UUID from the end of the URL path (removing any trailing slash)
                ref_url = ref_url.rstrip('/')
                #split url by '/' and take last part as key
                ref_uuid = ref_url.split('/')[-1]
                #check if looks like a UUID, if not skip
                if not re.match(r'^[0-9a-fA-F-]{36}$', ref_uuid):
                    continue
                # Create the lookup table entry
                formatted_record = {
                    "table": self.table_name,
                    "key": ref_uuid,
                    "value": resource_id,
                    "expires": self.expires
                }
                results.append(formatted_record)
                self.logger.debug(f"Built lookup entry: {formatted_record}")

        return results

class SiteToFacilityProcessor(IRIBaseRedisProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.url_match = r'/facility$'
        self.table_name = 'iri_site_to_facility'
        self.ref_field = 'site_uris'

class ResourceToSiteProcessor(IRIBaseRedisProcessor):
    def __init__(self, pipeline):
        super().__init__(pipeline)
        self.url_match = r'/facility/sites$'
        self.table_name = 'iri_resource_to_site'
        self.ref_field = 'resource_uris'