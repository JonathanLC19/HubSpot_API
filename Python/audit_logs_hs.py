import logging
import requests
import json
from dotenv import load_dotenv
import os

import credentials as creds
from pprint import pprint
import pandas as pd
import hubspot
from hubspot.crm.deals.exceptions import ApiException as DealsApiException
from hubspot.crm.contacts.exceptions import ApiException as ContactsApiException
from hubspot.crm.tickets.exceptions import ApiException as TicketsApiException, NotFoundException as TicketsNotFoundException
from hubspot.crm.objects.tasks.exceptions import ApiException as TasksApiException
from datetime import datetime, timedelta

import openai
from tenacity import retry, wait_random_exponential, stop_after_attempt
from termcolor import colored 
#region CONFIG
#–––––––––––––––––––––––––––––––––––––––––– CONFIG ------------------------------------------#
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

url = "https://api.hubapi.com/account-info/v3/activity/audit-logs"
access_token = creds.hs_prod_key
client_hs = hubspot.Client.create(access_token=creds.hs_prod_key)

load_dotenv()
openai_key = os.environ['openai_key']
GPT_MODEL = "gpt-4o-mini"
openai.api_key = openai_key

#region DATES SCRIPTS
#–––––––––––––––––––––––––––––––––––––––––– DATES SCRIPTS ------------------------------------------#
# Obtén la fecha actual y su día de la semana
today = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
today_str = today.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + 'Z'


current_weekday = today.weekday()

yesterday = (today - timedelta(current_weekday - 2)).replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_str = yesterday.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + 'Z'

print(yesterday_str)
print(today_str)

# tomorrow = timedelta(current_weekday + 1)

# # Calcula el inicio y fin de la semana pasada
# start_of_last_week = (today - timedelta(days=current_weekday + 7)).replace(hour=0, minute=0, second=0, microsecond=0)
# end_of_last_week = (start_of_last_week + timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)

#region LOGS DATAFRAME
#–––––––––––––––––––––––––––––––––––––––––– LOGS DATAFRAME ------------------------------------------#
headers = {
    'accept': "application/json",
    'authorization': f"Bearer {access_token}"
}

def get_activity_logs(after=None):
    querystring = {
        "limit": "100",
        "actingUserId": "51109567",
        "occurredAfter":str(yesterday_str),
        "occurredBefore":str(today_str)
    }
    if after:
        querystring["after"] = after
    
    response = requests.get(url, headers=headers, params=querystring)
    return response.json()

# Primera solicitud
response = get_activity_logs()
logs = response.get('results', [])
paging = response.get('paging', {})
# print(paging)
# print(logs)

# Si hay más resultados, usa el cursor 'after' para la siguiente solicitud
while 'next' in paging:
    after = paging['next']['after']
    response = get_activity_logs(after)
    logs.extend(response.get('results', []))
    paging = response.get('paging', {})

category = []
subcategory = []
action = []
object_id = []
date = []
asset = []
info = []
sources = []

# Procesar los logs de actividad
for log in logs:
    category.append(log['category'])
    subcategory.append(log['subCategory'])
    action.append(log['action'])
    object_id.append(log['targetObjectId'])
    date.append(log['occurredAt'])


logs = pd.DataFrame(data={
    'category' : category,
    'subcategory' : subcategory,
    'object_id' : object_id, 
    'action' : action,
    'date' : date
    })   

# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#     print(logs)

for index, row in logs.iterrows():
    if row['category'] == 'WORKFLOWS':
        url = f"https://api.hubapi.com/automation/v4/flows/{row['object_id']}"
        headers = {
            'accept': "application/json",
            'authorization': f"Bearer {access_token}"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            resp_js = response.json()
            info.append(resp_js)
            sources.append(resp_js.get('name', ''))
        else:
            # logger.error("Failed to get workflow info for object_id %s. Status code: %d", row['object_id'], response.status_code)
            info.append("")
            sources.append("")

    elif row['category'] in {'CRM_OBJECT', 'PROPERTY_VALUE', 'CRM_OBJECT_ASSOCIATION'}:
        if row['subcategory'] == 'DEAL':
            try:
                api_response = client_hs.crm.deals.basic_api.get_by_id(deal_id=f"{row['object_id']}", archived=False)
                source = api_response.properties.get('dealname', '')
                info.append(api_response)
                sources.append(source)
            except DealsApiException as e:
                # logger.error("Failed to get deal info for object_id %s. Exception: %s", row['object_id'], e)
                info.append("")
                sources.append("")

        elif row['subcategory'] == 'CONTACT':
            try:
                api_response = client_hs.crm.contacts.basic_api.get_by_id(contact_id=f"{row['object_id']}", archived=False)
                source = api_response.properties.get('email', '')
                info.append(api_response)
                sources.append(source)
            except ContactsApiException as e:
                print("Exception when calling basic_api->get_by_id: %s\n" % e)
                # logger.error("Failed to get contact info for object_id %s. Exception: %s", row['object_id'], e)
                info.append("")
                sources.append("")

        elif row['subcategory'] == 'TICKET':
            try:
                api_response = client_hs.crm.tickets.basic_api.get_by_id(ticket_id=f"{row['object_id']}", archived=False)
                source = api_response.properties.get('subject', '')
                info.append(api_response)
                sources.append(source)
            except TicketsNotFoundException as e:
                # logger.error("Ticket with ID %s not found. Exception: %s", row['object_id'], e)
                info.append("")
                sources.append("")
            except TicketsApiException as e:
                # logger.error("Failed to get ticket info for object_id %s. Exception: %s", row['object_id'], e)
                info.append("")
                sources.append("")

        elif row['subcategory'] == 'TASK':
            try:
                api_response = client_hs.crm.objects.tasks.basic_api.get_by_id(task_id=f"{row['object_id']}", archived=False)
                info.append(api_response)
                sources.append("")
            except TasksApiException as e:
                # logger.error("Failed to get task info for object_id %s. Exception: %s", row['object_id'], e)
                info.append("")
                sources.append("")

        else:
            info.append("")
            sources.append("")

    else:
        info.append("")
        sources.append("")

logs['info'] = info
logs['source'] = sources

# with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#     print(logs)

# # extraer la info del log

# for index, row in logs.iterrows():
#     # print(index)
#     if row['subcategory'] == 'DEAL':
#         print(row['info'])


#region OPENAI
#–––––––––––––––––––––––––––––––––––––––––– OPENAI ------------------------------------------#

def show_json(obj):
    # Suponiendo que obj.model_dump_json() devuelve una cadena JSON
    json_str = obj.model_dump_json()
    
    # Convertir la cadena JSON a un diccionario
    json_obj = json.loads(json_str)
    
    # Imprimir el JSON de manera formateada
    print(json.dumps(json_obj, indent=4))

# Paso 3: Definir una función para procesar los datos del DataFrame
def summary_info(fila):
    # Convierte la fila del DataFrame a un formato de texto adecuado
    texto = (
        f"Don't extend too much in the description, be more breefy. Please process the following information and provide a single bulletpointed descriptive summary structured by subcategories and actions of the activity performed by a HubSpot user based on the information in the following table of action logs in HubSpot. Give a one line summary of each info column value when exists and also show the source column value. The id of each object is in Object_id column. Tell also how many lines does the whole table has and what date actions were executed:\n"
        f"Categoría: {fila['category']}\n"
        f"Subcategoría: {fila['subcategory']}\n"
        f"ID del objeto: {fila['object_id']}\n"
        f"Acción: {fila['action']}\n"
        f"Fecha: {fila['date']}\n"
        f"Información: {fila['info']}\n"
        f"Fuente: {fila['source']}\n"
    )
    
    # Realiza una solicitud a la API de OpenAI
    summary = openai.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {"role": "system", "content": "You are a HubSpot CRM expert which duty is to check, audit and summarize an actions log table that entails the info of all the actions executed by a portal user during a certain timeframe."},
            {"role": "user", "content": texto}
        ],
        max_tokens=1000
    )
    
    # Devuelve la respuesta de la API
    return summary.choices[0].message.content

def record_details(fila):
    # Convierte la fila del DataFrame a un formato de texto adecuado
    texto = (
        f"Don't extend too much in the description, be more breefy. Please process the following information, read each content into info column for WORKFLOWS category and provide a summary the executed actions history made in that actual record. The column info refers to a HubSpot API call that retrieves the main information of the record the action log is related to. Read each info and identify the branches, actions, properties, etc. added or modified in each log finding them in the API call response placed in info column. Associate the answer with the workflow name which is the source column value:\n"
        f"Categoría: {fila['category']}\n"
        f"Subcategoría: {fila['subcategory']}\n"
        f"ID del objeto: {fila['object_id']}\n"
        f"Acción: {fila['action']}\n"
        f"Fecha: {fila['date']}\n"
        f"Información: {fila['info']}\n"
        f"Fuente: {fila['source']}\n"
    )
    
    # Realiza una solicitud a la API de OpenAI
    details = openai.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {"role": "system", "content": "You are a HubSpot CRM expert which duty is to check, audit and summarize an actions log table that entails the info of all the actions executed by a portal user during a certain timeframe."},
            {"role": "user", "content": texto}
        ],
        max_tokens=1000
    )
    
    # Devuelve la respuesta de la API
    return details.choices[0].message.content

pprint(summary_info(logs))
print("\n#################################################################\n")
pprint(record_details(logs))
# # Paso 4: Iterar sobre las filas del DataFrame y procesar cada una
# for index, fila in logs.iterrows():
#     # print(fila)
#     respuesta = procesar_fila_con_openai(fila)
#     print(f"Fila {index}:\n{respuesta}\n")