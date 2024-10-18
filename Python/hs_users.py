import requests
import json
import time
import schedule
import credentials as crd
from pprint import pprint
from datetime import datetime

import hubspot
from hubspot.crm.properties import PropertyUpdate, ApiException
from hubspot.crm.objects import SimplePublicObjectInputForCreate, ApiException

import gx_shift as gxs
import hs_properties as hsp


#region GET USERS
# Get users info from HubSpot to match with agents
def get_users(after=None):
        url = "https://api.hubapi.com/crm/v3/objects/users"

        headers = {
            'accept': "application/json",
            'authorization': f"Bearer {crd.hs_sb_key}"
            }

        querystring = {"limit":"100",
                       "properties":["hs_availability_status", 
                                     "hs_working_hours",
                                     "hs_job_title",
                                     "hs_given_name",
                                     "hs_family_name",
                                     "hs_email",
                                     "hs_out_of_office_hours",
                                     "hubspot_team_id",
                                     "hubspot_owner_id"]
                       ,"archived":"false"}
        if after:
            querystring["after"] = after
        
        response = requests.get(url, headers=headers, params=querystring)
        response = response.json()
        res = response.get('results')
        return res

users = get_users()
user_props = [user['properties'] for user in users]

# for user in user_props:
#     if user['hubspot_team_id'] == '133123965':
#         pprint(user)

#region GET AGENTS

# Get agents info from HubSpot to match with users
def get_agents(after=None):
        url = "https://api.hubapi.com/crm/v3/objects/agents"

        headers = {
            'accept': "application/json",
            'authorization': f"Bearer {crd.hs_sb_key}"
            }

        querystring = {"limit":"100",
                       "properties":["availability_status",
                                     "hubspot_team_id",
                                     "hubspot_owner_id"]
                       ,"archived":"false"}
        if after:
            querystring["after"] = after
        
        response = requests.get(url, headers=headers, params=querystring)
        response = response.json()
        res = response.get('results')
        return res

agents = get_agents()
agent_props = [agent['properties'] for agent in agents]

# for agent in agent_props:
#     pprint(agent)

#region CREATE AGENT

# Create agent in HubSpot with the user data
def create_c_o(key, agent_name, hubspot_owner_id, availability_status):
    client = hubspot.Client.create(access_token=key)

    properties = {
        "agent_name": agent_name,
        "hubspot_owner_id": hubspot_owner_id,
        "availability_status": availability_status
    }
    simple_public_object_input_for_create = SimplePublicObjectInputForCreate(properties=properties)
    try:
        api_response = client.crm.objects.basic_api.create(object_type="agents", simple_public_object_input_for_create=simple_public_object_input_for_create)
        # pprint(api_response)
    except ApiException as e:
        print("Exception when calling basic_api->create: %s\n" % e)


# Extraer los hubspot_owner_id de users y agents
user_owner_ids =  {user['hubspot_owner_id'] for user in user_props if 'hubspot_team_id' in user and user['hubspot_team_id'] == '133123965'}

# print(user_owner_ids)

agent_owner_ids = {agent['hubspot_owner_id'] for agent in agent_props if 'hubspot_owner_id' in agent}

# Encontrar coincidencias de hubspot_owner_id
matching_owner_ids = user_owner_ids.intersection(agent_owner_ids)

# pprint(matching_owner_ids)

# Identificar los agentes que ya existen en HubSpot y crearlo si no existe
for user in user_props:
    if user['hubspot_owner_id'] in user_owner_ids:
        if user['hubspot_team_id'] == '133123965':
            if user['hubspot_owner_id'] in matching_owner_ids:
                pprint(f"{user['hs_given_name']} {user['hs_family_name']} - Agent already exists ✅")
            else:
                create_c_o(crd.hs_sb_key, f"{user['hs_given_name']} {user['hs_family_name']}", user['hubspot_owner_id'], user['hs_availability_status'])
                pprint(f"{user['hs_given_name']} {user['hs_family_name']} - New Agent Created 🦾")

#     else:
#         pprint(f"{user['hs_given_name']} {user['hs_family_name']} - Does not belong to the team - {user['hubspot_team_id']}")

#region UPDATE AGENT

# Actualizar agente
def update_agent(id, properties):
        url = f"https://api.hubapi.com/crm/v3/objects/agents/{id}"

        headers = {
            'accept': "application/json",
            'authorization': f"Bearer {crd.hs_sb_key}"
            }

        querystring ={
                        "properties": properties
                        }

        response = requests.request("PATCH", url, headers=headers, json=querystring)
        res = response.json()

        return res

# Comparar disponibilidad del agente con el user y actualizar
for agent in agent_props:
    # print(agent['availability_status'])  # Mostrar disponibilidad del agente

    # Filtrar usuarios por hubspot_team_id y hubspot_owner_id coincidente con el agente
    matching_users = [
        user for user in user_props
        if user['hubspot_team_id'] == '133123965' and user['hubspot_owner_id'] == agent['hubspot_owner_id']
    ]
    # pprint(matching_users)

    for user in matching_users:
        # print(user['hs_availability_status'])  # Mostrar disponibilidad del usuario

        # Comparar disponibilidad y actualizar si es necesario
        if user['hs_availability_status'] != agent['availability_status']:
            # Actualización comentada
            properties = {
                "availability_status": user['hs_availability_status']
            }
            update_agent(agent['hs_object_id'], properties)
            pprint(f"Availability updated: {user['hs_availability_status']} -> {agent['availability_status']} - user: {user['hs_given_name']} {user['hs_family_name']}")
        else:
            pprint(f"{user['hs_given_name']} {user['hs_family_name']} - Availability coincide: {user['hs_availability_status']} / {agent['availability_status']}")
