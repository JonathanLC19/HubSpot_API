import requests
import wf_creds
from pprint import pprint

url = "https://api.hubapi.com/automation/v4/flows"

querystring = {"limit":"20"}

headers = {
    'accept': "application/json",
    'authorization': f"Bearer {wf_creds.hs_sb_key}"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

# for res in response.json()['results']:
#     if res['objectTypeId'] == '0-3':
#         print('' * 20)
#         print(res['name'])
#         print('-----------------------------------------------------------')
#         # print a separator line
#         print('' * 20)
#         pprint(res)

# print(len(response.json()['results']))

wf_ids = [res['id'] for res in response.json()['results']]
print(wf_ids)