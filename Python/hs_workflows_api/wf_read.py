import wf_creds
import requests
from pprint import pprint
# from dotenv import load_dotenv
# import os
# import openai
# import json

# load_dotenv()
# openai_key = os.environ['openai_key']
# GPT_MODEL = "gpt-4o-mini"
# openai.api_key = openai_key

def getWF(wf_id, hs_id):
    wf_id = str(wf_id)
    url = f"https://api.hubapi.com/automation/v4/flows/{wf_id}"

    headers = {
        'accept': "application/json",
        'authorization': f"Bearer {hs_id}"
        }

    response = requests.request("GET", url, headers=headers)
    resp_js = response.json()

    return resp_js

data = getWF(1550961861, wf_creds.hs_prod_key)
# pprint(data)

# text = """read and understand the context of this HubSpot workflows API responses I'm going to share. 
#           This workflows are related to an automated system based on managing and updating HubSpot tickets information based on the 'All Issue Types' property values.
#           Give me a description of the process this workflow executes. Don't need you to split it into the different workflow actions/branches.
#           Always begin the response with the workflow name."""

# print("Write your prompt:")
# user_input = input("Read this HubSpot workflow API response and ")

# def wf_summary(wf):
#     # Realiza una solicitud a la API de OpenAI
#     summary = openai.chat.completions.create(
#         model=GPT_MODEL,
#         messages=[
#             {"role": "system", "content": text},
#             {"role": "user", "content": json.dumps(wf)}
#         ],
#         max_tokens=1000
#     )

#     # Devuelve la respuesta de la API
#     return summary.choices[0].message.content

# print(wf_summary(data))