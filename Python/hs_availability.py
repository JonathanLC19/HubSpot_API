import requests
import credentials as crd
from pprint import pprint
from datetime import datetime

import hubspot
from hubspot.crm.properties import PropertyUpdate, ApiException
from hubspot.crm.objects import SimplePublicObjectInputForCreate, ApiException
from hubspot.crm.tickets import SimplePublicObjectInput, PublicObjectSearchRequest, ApiException

import schedule
import time

#region GET USERS
# Get users info from HubSpot to match with agents
def get_users():
    """
    Gets users info from HubSpot to match with agents.

    Parameters:
    after (str): The ID of the last user to be returned in the previous page of results. This is used to paginate through large result sets.

    Returns:
    list: A list of dictionaries containing user information.
    """
    url = "https://api.hubapi.com/crm/v3/objects/users/search"

    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'authorization': f"Bearer {crd.hs_prod_key}"
        }

    querystring = {"limit": 100,
                    "properties":["hs_availability_status", 
                                    "hs_working_hours",
                                    "hs_job_title",
                                    "hs_given_name",
                                    "hs_family_name",
                                    "hs_email",
                                    "hs_out_of_office_hours",
                                    "hubspot_team_id",
                                    "hubspot_owner_id",
                                    "hs_deactivated"
                                    ],
                    "filters": [
                                {
                                    "value": 'false',
                                    "propertyName": "hs_deactivated",
                                    "operator": "EQ"
                                },
                                {
                                    "values": [147649404, 147601142, 147649414],
                                    # "values": [95422169],
                                    "propertyName": "hubspot_team_id",
                                    "operator": "IN"
                                }
                            ]
                    ,"archived":"false"}
    all_users = []
    after = None
    
    try:
        while True:
            if after:
                querystring["after"] = after

            response = requests.post(url, headers=headers, json=querystring)
            response.raise_for_status()  # Levanta un error si la respuesta HTTP indica fallo

            response_json = response.json()

            # Añadir los usuarios de la respuesta actual a la lista total
            all_users.extend(response_json.get('results', []))

            # Obtener el valor de "after" para la siguiente página
            paging = response_json.get('paging')
            if paging and paging.get('next'):
                after = paging['next']['after']
            else:
                break  # Si no hay más páginas, salir del bucle

    except requests.exceptions.HTTPError as http_err:
        pprint(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        pprint(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        pprint(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        pprint(f"An error occurred: {req_err}")
    except Exception as e:
        pprint(f"An unexpected error occurred: {e}")

    return all_users

users = get_users()
user_props = [user['properties'] for user in users]

# pprint(len(user_props))

# for user in user_props:
#     pprint(f"{user['hs_email']} - {user['hubspot_team_id']} - {user['hubspot_owner_id']}")

#region GET TICKETS
def get_tickets(key, agent_id):

    """
    Gets tickets associated with a given agent_id.

    Parameters:
    key (str): HubSpot API key
    agent_id (int): HubSpot agent ID

    Returns:
    list: List of tickets associated with the agent
    """
    client = hubspot.Client.create(access_token=key)

    public_object_search_request = PublicObjectSearchRequest(properties= [ "gx_agent_availability", "hubspot_owner_id", "hs_pipeline"],
        filter_groups=[
            {
                "filters": [
                    {
                        "value": agent_id,
                        "propertyName": "hubspot_owner_id",
                        "operator": "EQ"
                    },
                    {
                        "value": "2024-09-01T00:00:00.000Z",
                        "propertyName": "createdate",
                        "operator": "GTE"
                    },
                    {
                        "values": [442510297, 442510298, 442510300],
                        # "values": [95422169],
                        "propertyName": "hs_pipeline",
                        "operator": "IN"
                    }
                ]
            }
        ],
        limit=100,
        after = None # Cambiado de offset=offset a after=None
    )
    all_tickets = []
    try:
        while True:
            # Ejecutar la búsqueda de tickets
            api_response = client.crm.tickets.search_api.do_search(public_object_search_request=public_object_search_request)
            results = api_response.results
            
            # Agregar los resultados actuales a la lista de tickets
            if results:
                all_tickets.extend(results)

            # Verificar si hay más páginas de resultados
            if api_response.paging and api_response.paging.next:
                public_object_search_request.after = api_response.paging.next.after
            else:
                break  # Si no hay más páginas, salir del bucle
        
    except ApiException as e:
        print("Exception when calling search_api->do_search: %s\n" % e)
        return []

    return all_tickets

# for user in user_props:
#     pprint(f"{user['hs_email']} - {user['hubspot_owner_id']}")
#     pprint("---------")
#     tickets = get_tickets(crd.hs_prod_key, user['hubspot_owner_id'])
#     ticket_props = [ticket.properties for ticket in tickets]
#     for t in ticket_props:
#         pprint(f"{t['hs_object_id']} - {t['createdate']}")

#region UPDATE TICKETS
# Actualizar el campo de disponibilidad en un ticket
def update_ticket_availability(ticket_id, availability_status, key):
    """
    Actualiza el campo de disponibilidad en un ticket asignado a un agente.

    Parameters:
    ticket_id (int): El ID del ticket a actualizar.
    availability_status (str): El valor a asignar al campo de disponibilidad del ticket.
    key (str): La clave de autenticación para el API de HubSpot.
    """
    client = hubspot.Client.create(access_token=key)
    
    properties = {
        "gx_agent_availability": availability_status
    }
    simple_public_object_input = SimplePublicObjectInput(properties=properties)
    
    try:
        client.crm.tickets.basic_api.update(ticket_id, simple_public_object_input)
        # pprint(f"Ticket {ticket_id} - Availability updated to {availability_status}")
    except ApiException as e:
        print(f"Exception when calling tickets.basic_api.update: {e}")

# Función principal para sincronizar la disponibilidad en los tickets asignados
# def sync_tickets_with_user_availability():
#     """
#     Sincroniza la disponibilidad de los agentes en los tickets asignados a cada agente.

#     Itera sobre los usuarios, obtiene los tickets asignados a cada usuario y
#     actualiza el campo de disponibilidad en cada ticket si es necesario.
#     """
#     users = get_users()

#     # Iterar sobre cada usuario
#     for user in users:
#         user_properties = user['properties']
        
#         owner_id = user_properties['hubspot_owner_id']
#         availability_status = user_properties['hs_availability_status']
#         user_given_name = user_properties['hs_given_name']
#         user_family_name = user_properties['hs_family_name']

#         # Obtener los tickets asignados al usuario
#         tickets = get_tickets(crd.hs_prod_key, owner_id)

#         # Verificar si no hay tickets asignados
#         if not tickets:
#             pprint(f"User {user_given_name} {user_family_name} has no tickets assigned.")
#             continue  # Pasar al siguiente usuario si no hay tickets

#         # Contador para los tickets actualizados
#         updated_tickets_count = 0

#         # Iterar sobre los tickets asignados y actualizar la disponibilidad
#         for ticket in tickets:
#             ticket_id = ticket.id
#             ticket_availability = ticket.properties.get('gx_agent_availability')

#             # Solo actualizar si el status de disponibilidad es diferente
#             if ticket_availability != availability_status:
#                 update_ticket_availability(ticket_id, availability_status, crd.hs_prod_key)
#                 updated_tickets_count += 1

#         # Loggear resumen por usuario cuando todos sus tickets se hayan procesado
#         pprint(f"User {user_given_name} {user_family_name}: {updated_tickets_count} tickets updated.")

# # Ejecutar la sincronización
# sync_tickets_with_user_availability()

# schedule.every(30).seconds.do(sync_tickets_with_user_availability)

# while True:
#     schedule.run_pending()
#     # timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     # print(f"{timestamp} - Execution of sync_users_and_agents")
#     time.sleep(1)  # Espera 1 segundo entre cada iteraci n para evitar uso excesivo de recursos