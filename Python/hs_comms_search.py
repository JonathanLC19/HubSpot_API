import hubspot
from pprint import pprint
from hubspot.crm.objects.communications import PublicObjectSearchRequest, ApiException
from datetime import datetime, timedelta

import credentials as creds

#region DATES SCRIPTS
#–––––––––––––––––––––––––––––––––––––––––– DATES SCRIPTS ------------------------------------------#
# Obtén la fecha actual y su día de la semana
today = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
today_str = today.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + 'Z'


current_weekday = today.weekday()

yesterday = (today - timedelta(current_weekday - 2)).replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_str = yesterday.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + 'Z'

client = hubspot.Client.create(access_token=creds.hs_prod_key)

public_object_search_request = PublicObjectSearchRequest(limit=100, properties=["hs_communication_channel_type", "hs_communication_body", "hs_communication_logged_from", "hs_timestamp"], 
                                                         filter_groups=[{"filters":[
                                                             {
                                                                #  "highValue":"string",
                                                                 "propertyName":"hs_timestamp",
                                                                #  "values":["string"],
                                                                 "value":yesterday_str,
                                                                 "operator":"GT"
                                                                 },
                                                                 {
                                                                    "value": "CONVERSATIONS",
                                                                    "propertyName": "hs_communication_logged_from",
                                                                    "operator": "EQ"
                                                                }]}])
try:
    api_response = client.crm.objects.communications.search_api.do_search(public_object_search_request=public_object_search_request)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling search_api->do_search: %s\n" % e)