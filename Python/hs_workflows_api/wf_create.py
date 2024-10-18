import requests
import wf_creds
from pprint import pprint
import json
import wf_read

def replace_single_quotes(text):
    """
    Reemplaza las comillas simples en un texto por comillas dobles escapadas.

    :param text: Texto de entrada.
    :return: Texto con las comillas simples reemplazadas.
    """
    text = text.replace("'", r'"')
    text = text.replace("\n", r'')
    return text

def convert_booleans_to_lowercase(data):
    """
    Reemplaza los valores booleanos True y False en el diccionario por 'true' y 'false' en minúsculas.
    
    :param data: Diccionario de entrada.
    :return: Diccionario con los valores booleanos convertidos.
    """
    if isinstance(data, dict):
        return {k: convert_booleans_to_lowercase(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_booleans_to_lowercase(item) for item in data]
    elif data is True:
        return 'true'
    elif data is False:
        return 'false'
    else:
        return data

#########################################################################################################################################

url = "https://api.hubapi.com/automation/v4/flows"
wf_data = wf_read.getWF(1574262982, wf_creds.hs_sb_key)

# wf_data['name'] = 'GX GENERAL | TICKETS - Update Todays Date [PART TWO]'
wf_data['isEnabled'] = 'false'
data_lc = convert_booleans_to_lowercase(wf_data)
data_str = json.dumps(data_lc, indent=4)


data = replace_single_quotes(data_str)

pprint(data)
print("Data type:", type(data))

headers = {
    'accept': "application/json",
    'content-type': "application/json",
    'authorization': f"Bearer {wf_creds.hs_prod_key}"
    }

response = requests.request("POST", url, data=data, headers=headers)

print(response.text)

#########################################################################################################################################

# import requests

# url = "https://api.hubapi.com/automation/v4/flows"

# payload = {
#     "isEnabled": False,
#     "flowType": "WORKFLOW",
#     "name": "Set Lifecycle Stage to Lead on Contact Creation",
#     "startActionId": "1",
#     "nextAvailableActionId": "2",
#     "actions": [
#         {
#             "type": "SINGLE_CONNECTION",
#             "actionId": "1",
#             "actionTypeVersion": 0,
#             "actionTypeId": "0-5",
#             "fields": {
#                 "property_name": "lifecyclestage",
#                 "value": {
#                     "staticValue": "lead",
#                     "type": "STATIC_VALUE"
#                 }
#             }
#         }
#     ],
#     "enrollmentCriteria": {
#         "shouldReEnroll": False,
#         "type": "EVENT_BASED",
#         "eventFilterBranches": [
#             {
#                 "eventTypeId": "4-1463224",
#                 "operator": "HAS_COMPLETED",
#                 "filterBranchType": "UNIFIED_EVENTS",
#                 "filterBranchOperator": "AND",
#                 "filterBranches": [],
#                 "filters": []
#             }
#         ],
#         "listMembershipFilterBranches": []
#     },
#     "timeWindows": [],
#     "blockedDates": [],
#     "customProperties": {},
#     "crmObjectCreationStatus": "COMPLETE",
#     "type": "CONTACT_FLOW",
#     "objectTypeId": "0-1",
#     "suppressionListIds": [],
#     "canEnrollFromSalesforce": False
# }

# headers = {
#     'accept': "application/json",
#     'content-type': "application/json",
#     'authorization': f"Bearer {wf_creds.hs_sb_key}"
# }

# response = requests.post(url, json=payload, headers=headers)

# print(response.text)
