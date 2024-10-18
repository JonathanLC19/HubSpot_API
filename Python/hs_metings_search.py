import hubspot
from pprint import pprint
from hubspot.crm.objects.meetings import PublicObjectSearchRequest, ApiException

import credentials as creds

client = hubspot.Client.create(access_token=creds.hs_sb_key)

public_object_search_request = PublicObjectSearchRequest(limit=10, properties=["hubspot_owner_id", "hs_meeting_title", "hs_meeting_external_URL", "hs_meeting_start_time", "hs_meeting_end_time"])
try:
    api_response = client.crm.objects.meetings.search_api.do_search(public_object_search_request=public_object_search_request)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling search_api->do_search: %s\n" % e)