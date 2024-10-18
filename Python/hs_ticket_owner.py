import requests
import credentials as crd
from pprint import pprint
from datetime import datetime, timedelta
import hubspot
from hubspot.crm.properties import PropertyUpdate, ApiException
from hubspot.crm.objects import SimplePublicObjectInputForCreate, ApiException
from hubspot.crm.tickets import SimplePublicObjectInput, PublicObjectSearchRequest, ApiException

import schedule
import time

# Current date and SLA date (24 hours back)
current_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
sla_date = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

#region GET USERS
# Get users with availability status
def get_users(key):
    """
    Gets users info from HubSpot to match with agents.
    """
    url = "https://api.hubapi.com/crm/v3/objects/users/search"
    headers = {
        'accept': "application/json",
        'content-type': "application/json",
        'authorization': f"Bearer {key}"
    }
    querystring = {
        "limit": 100,
        "properties": ["hs_availability_status", "hubspot_owner_id", "hs_family_name", "hs_given_name", "hs_email"],         
        "filters": [
            {
                "value": 'false',
                "propertyName": "hs_deactivated",
                "operator": "EQ"
            },
            {
                "values": [133123965],
                "propertyName": "hubspot_team_id",
                "operator": "IN"
            }
        ],
        "archived": "false"
    }

    all_users = []
    after = None
    try:
        while True:
            if after:
                querystring["after"] = after

            response = requests.post(url, headers=headers, json=querystring)
            response.raise_for_status()
            response_json = response.json()
            all_users.extend(response_json.get('results', []))

            paging = response_json.get('paging')
            if paging and paging.get('next'):
                after = paging['next']['after']
            else:
                break

    except requests.exceptions.RequestException as e:
        pprint(f"An error occurred: {e}")
    return all_users

#region GET TICKETS
# Get tickets where agent availability is "away"
def get_tickets(key):
    """
    Fetches tickets where 'gx_agent_availability' is 'away' and matches time conditions.
    """
    client = hubspot.Client.create(access_token=key)

    public_object_search_request = PublicObjectSearchRequest(
        properties=["gx_agent_availability", "hubspot_owner_id", "time_since_last_available", "time_since_last_away", "subject"],
        filter_groups=[
            {
                "filters": [
                    # {
                    #     "propertyName": "hubspot_owner_id",
                    #     "operator": "NOT_HAS_PROPERTY"
                    # },
                    {
                        "values": [379059177],
                        # "values": [95422169],
                        "propertyName": "hs_pipeline",
                        "operator": "IN"
                    },
                    # {
                    #     "value": sla_date,
                    #     "propertyName": "time_since_last_away",
                    #     "operator": "LTE"
                    # }
                ]
            }
        ],
        limit=100,
        after=None
    )
    all_tickets = []
    try:
        while True:
            api_response = client.crm.tickets.search_api.do_search(public_object_search_request=public_object_search_request)
            results = api_response.results

            if results:
                all_tickets.extend(results)

            if api_response.paging and api_response.paging.next:
                public_object_search_request.after = api_response.paging.next.after
            else:
                break

    except ApiException as e:
        print("Exception when calling search_api->do_search: %s\n" % e)

    return all_tickets

#region ASSIGN TICKET OWNER
# Update ticket with new hubspot_owner_id
def update_ticket_owner(ticket_id, new_hubspot_owner_id, key):
    """
    Update ticket owner to the available agent.
    """
    client = hubspot.Client.create(access_token=key)
    
    properties = {
        "hubspot_owner_id": new_hubspot_owner_id
    }
    simple_public_object_input = SimplePublicObjectInput(properties=properties)
    
    try:
        client.crm.tickets.basic_api.update(ticket_id, simple_public_object_input)
        # pprint(f"Ticket {ticket_id} updated with new owner ID: {new_hubspot_owner_id}")
    except ApiException as e:
        print(f"Exception when updating ticket: {e}")

#region COUNT TICKETS PER USER
def count_tickets_per_user(key):
    """
    Counts the number of tickets for each user (agent) and prints the result.
    """
    users = get_users(key)
    tickets = get_tickets(key)

    tickets_by_user = {user['properties']['hubspot_owner_id']: 0 for user in users}

    for ticket in tickets:
        user_id = ticket.properties.get('hubspot_owner_id')
        if user_id and user_id in tickets_by_user:
            tickets_by_user[user_id] += 1

    return tickets_by_user

# Call the function to count tickets per user
# count_tickets_per_user(crd.hs_sb_key)

def print_ticket_counts(ticket_count_by_user, users, description):
    """
    Helper function to print the number of tickets assigned to each agent.
    
    Parameters:
    ticket_count_by_user (dict): Dictionary containing the number of tickets per user.
    users (list): List of user dictionaries with properties.
    description (str): A message to describe the current state (e.g., "before" or "after").
    """
    print(f"Ticket counts {description} assignment:")
    for user in users:
        user_id = user['properties']['hubspot_owner_id']
        availability = user['properties'].get('hs_availability_status', 'unknown')
        name = user['properties']['hs_given_name']
        family_name = user['properties']['hs_family_name']
        ticket_count = ticket_count_by_user.get(user_id, 0)
        print(f"User ID: {name} {family_name} | Availability: {availability} | Ticket count: {ticket_count}")
    print("\n")  # Para una mejor legibilidad

#region MAIN FUNCT
# Main sync function
def assign_ticket_owner(key):
    """
    Syncs tickets based on agent availability and ticket count.
    Assigns tickets to the available agent with the least number of tickets.
    """
    users = get_users(key)
    ticket_count_by_user = count_tickets_per_user(key)

    # Imprimir el número de tickets por agente antes de la asignación
    print_ticket_counts(ticket_count_by_user, users, "before")

    # Filtrar usuarios disponibles
    available_users = [user for user in users if user['properties'].get('hs_availability_status') == 'available']
    away_users = [user for user in users if user['properties'].get('hs_availability_status') == 'away']

    if available_users:
        # Obtener los tickets a asignar
        tickets = get_tickets(key)

        for ticket in tickets:
            ticket_id = ticket.id
            ticket_owner = ticket.properties.get('hubspot_owner_id')

            if not ticket_owner:
                # Ordenar usuarios disponibles por menor cantidad de tickets
                available_users.sort(key=lambda user: ticket_count_by_user.get(user['properties']['hubspot_owner_id'], 0))
                selected_user = available_users[0]
                available_user_id = selected_user['properties']['hubspot_owner_id']

                # Verificar y mostrar cuántos tickets tiene el usuario antes de la asignación
                print(f"Assigning ticket {ticket_id} to {selected_user['properties']['hs_email']}. Current ticket count: {ticket_count_by_user.get(available_user_id, 0)}")

                # Actualizar el dueño del ticket
                update_ticket_owner(ticket_id, available_user_id, key)

                # Actualizar el conteo de tickets manualmente después de la asignación
                ticket_count_by_user[available_user_id] = ticket_count_by_user.get(available_user_id, 0) + 1

                # Verificar cuántos tickets tiene después de la asignación
                print(f"Ticket {ticket_id} updated with new owner ID: {available_user_id}. New ticket count: {ticket_count_by_user.get(available_user_id, 0)}")

            else:
                pprint(f"Ticket {ticket_id} already has owner {ticket_owner}. Skipping assignment.")

        # Advertencia sobre los agentes que están away
        if away_users:
            away_user_emails = [user['properties']['hs_email'] for user in away_users]
            print("Warning: The following agents are away and did not receive any ticket assignments:")
            print(", ".join(away_user_emails))

    else:
        pprint("No available users found.")

    # Imprimir el número de tickets por agente después de la asignación
    print_ticket_counts(ticket_count_by_user, users, "after")

# Run the synchronization
assign_ticket_owner(crd.hs_sb_key)

def redistribute_tickets_equally(key):
    """
    Redistribute tickets among available agents to ensure they have an equal number of tickets.
    """
    users = get_users(key)
    ticket_count_by_user = count_tickets_per_user(key)

    # Imprimir el número de tickets por agente antes de la redistribución
    print_ticket_counts(ticket_count_by_user, users, "before redistribution")

    # Filtrar usuarios disponibles
    available_users = [user for user in users if user['properties'].get('hs_availability_status') == 'available']
    
    # Obtener el número total de tickets y calcular el objetivo por usuario
    total_tickets = sum(ticket_count_by_user.get(user['properties']['hubspot_owner_id'], 0) for user in available_users)
    num_available_users = len(available_users)
    
    if num_available_users == 0:
        print("No available users to redistribute tickets.")
        return

    # El número ideal de tickets por usuario disponible
    ideal_ticket_count = total_tickets // num_available_users
    print(f"Ideal ticket count per user: {ideal_ticket_count}")

    # Obtener los tickets que necesitan ser redistribuidos
    tickets = get_tickets(key)

    # Clasificar los usuarios por su cantidad de tickets
    available_users.sort(key=lambda user: ticket_count_by_user.get(user['properties']['hubspot_owner_id'], 0))

    # Redistribuir tickets
    for ticket in tickets:
        ticket_id = ticket.id
        ticket_owner_id = ticket.properties.get('hubspot_owner_id')

        # Si el ticket ya tiene dueño
        if ticket_owner_id:
            # Encontrar al usuario con más tickets
            highest_user = available_users[-1]
            highest_user_id = highest_user['properties']['hubspot_owner_id']
            highest_user_ticket_count = ticket_count_by_user.get(highest_user_id, 0)

            # Encontrar al usuario con menos tickets
            lowest_user = available_users[0]
            lowest_user_id = lowest_user['properties']['hubspot_owner_id']
            lowest_user_ticket_count = ticket_count_by_user.get(lowest_user_id, 0)

            # Si el usuario con más tickets tiene más que el ideal y el usuario con menos tiene menos que el ideal
            if highest_user_ticket_count > ideal_ticket_count and lowest_user_ticket_count < ideal_ticket_count:
                # Transferir el ticket al usuario con menos tickets
                print(f"Transferring ticket {ticket_id} from {highest_user['properties']['hs_email']} to {lowest_user['properties']['hs_email']}.")
                
                # Actualizar el dueño del ticket
                update_ticket_owner(ticket_id, lowest_user_id, key)

                # Actualizar los conteos de tickets manualmente
                ticket_count_by_user[highest_user_id] -= 1
                ticket_count_by_user[lowest_user_id] += 1

                # Reordenar la lista de usuarios disponibles después de la transferencia
                available_users.sort(key=lambda user: ticket_count_by_user.get(user['properties']['hubspot_owner_id'], 0))

    # Imprimir el número de tickets por agente después de la redistribución
    print_ticket_counts(ticket_count_by_user, users, "after redistribution")

# Run the redistribution
# redistribute_tickets_equally(crd.hs_sb_key)