
import requests
import credentials as creds
from pprint import pprint
from openai import OpenAI
import json

client = OpenAI(api_key=creds.openai_key)

# openai_key = creds.openai_key
# openai.api_key = openai_key

GPT_MODEL = "gpt-4o-2024-08-06"

"""--------------------------------------- GET THREADS ---------------------------------------------"""

def get_all_threads(key, latest_message_timestamp_after):
    url = "https://api.hubapi.com/conversations/v3/conversations/threads"
    
    headers = {
        'accept': "application/json",
        'authorization': f"Bearer {key}"
    }

    all_threads = []
    querystring = {"limit": 100, 
                #    "sort": "-latestMessageTimestamp",
                #    "latestMessageTimestampAfter": latest_message_timestamp_after,
                   "properties": ["id", "assignedTo", "status", "inboxId"]}
    after = None

    while True:
        if after:
            querystring["after"] = after

        response = requests.get(url, headers=headers, params=querystring)
        res = response.json()

        # Iterar sobre los resultados actuales y renderear uno por uno
        for thread in res.get('results', []):
            yield thread  # Devuelve cada resultado uno por uno

        # Verifica si hay más páginas de resultados
        paging = res.get('paging', {}).get('next', None)
        if paging:
            after = paging.get('after')
        else:
            # No hay más resultados, salir del bucle
            break

    return all_threads

for thread in get_all_threads(creds.hs_sb_key, "2024-10-01T00:00:00Z"):
    pprint(thread)
    # if thread['inboxId'] == '4734555589':
    #     pprint(thread)
    # else:
    #     print(thread['inboxId'])    
# """--------------------------------------- GET THREADS BY OBJECT ID ---------------------------------------------"""

# def get_threads_id(object_id):
#     url = f"https://api.hubapi.com/conversations/v3/conversations/threads?associatedContactId={object_id}"

#     headers = {
#         'accept': "application/json",
#         'authorization': f'Bearer {creds.hs_sb_key}',
#         }

#     response = requests.request("GET", url, headers=headers)
#     res = response.json()

#     threads = [i['id'] for i in res['results']]
#     print(threads)
    
#     for thread_id in threads:
#         url = f"https://api.hubapi.com/conversations/v3/conversations/threads/{thread_id}/messages"

#         headers = {
#             'accept': "application/json",
#             'authorization': f"Bearer {creds.hs_sb_key}",
#             }

#         try:
#             response = requests.request("GET", url, headers=headers)
#             if response.status_code == 200:
#                 res = json.dumps(response.json(), indent=4)
#                 threads_json = json.loads(res)

#                 data = threads_json

#                 # Access all the fields == loop through
#                 for result in data['results']:
#                     thr_type =  result['type']
#                     print(thr_type)
                    
#                     # thr_type =  result['type']
#                     # thr_id = result['id']
#                     # thr_status = result['newStatus']
#                     # thr_create_date = result['createdAt']
#                     # thr_created_by = result['createdBy']
#                     # thr_recipients = result['recipients']
#                     # thr_senders = result['senders']
#                     # if result['type'] == 'MESSAGE':
#                     #     thr_channel = result['channelId']
#                     #     thr_create_date = result['createdAt']
#                     #     thr_created_by = result['createdBy']
#                     #     thr_direction = result['direction']
#                     #     thr_recipients = result['recipients']
#                     #     thr_text = result['text']
#                     # thr_description = f"""
#                     #     Type: {thr_type},
#                     #     ID: {thr_id},
#                     #     Status: {thr_status},
#                     #     Channel: {thr_channel},
#                     #     Created: {thr_create_date},
#                     #     Created by: {thr_created_by},
#                     #     Direction: {thr_direction},
#                     #     Recipients: {thr_recipients},
#                     #     Senders: {thr_senders},
#                     #     Text: {thr_text}
#                     # """
#                     # threads_list = [thr_description[i] for i in range(len(thr_description))]
#             return res
                    
#         except requests.exceptions.RequestException as e:
#             print(f"Error occurred during API request: {e}")


# """--------------------------------------- GET ACTORS ---------------------------------------------"""

# def get_actors(key):
#     url = "https://api.hubapi.com/conversations/v3/conversations/actors/batch/read"

#     payload = "{\"inputs\":[\"string\"]}"
#     headers = {
#         'accept': "application/json",
#         'content-type': "application/json",
#         'authorization': f"Bearer {key}"
#         }

#     response = requests.request("POST", url, data=payload, headers=headers)

#     print(response.text)

# if __name__ == "__main__":
#     pprint(get_actors(creds.hs_sb_key))