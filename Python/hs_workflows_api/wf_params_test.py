import wf_creds
import requests
from pprint import pprint

def getWF(wf_id, hs_id):
    wf_id = str(wf_id)
    url = f"https://api.hubapi.com/automation/v4/flows/{wf_id}"

    headers = {
        'accept': "application/json",
        'authorization': f"Bearer {hs_id}"
        }

    response = requests.request("GET", url, headers=headers)

    try:
        resp_js = response.json()
    except requests.exceptions.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        resp_js = None

    return resp_js

data = getWF(1550961861, wf_creds.hs_sb_key)
pprint(data)
