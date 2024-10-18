import requests
from pprint import pprint

import hubspot
from hubspot.crm.properties import PropertyUpdate, ApiException
from hubspot.crm.objects import SimplePublicObjectInputForCreate, ApiException
from hubspot.crm.tickets import SimplePublicObjectInput, ApiException
import credentials as crd

import schedule
import time
from datetime import datetime
import logging
import psycopg2


# Configurar la conexión a PostgreSQL
def connect_db():
    try:
        connection = psycopg2.connect(
            host=crd.rds_host,
            port=5432,
            database=crd.rds_database,
            user=crd.rds_user,
            password=crd.rds_password
        )
        print("Conexión a la base de datos exitosa.")
        return connection
    except Exception as e:
        print(f"Error en la conexión a la base de datos: {e}")
        return None

# Función para insertar logs en la tabla agents_logs
def log_to_db(level, message):
    conn = None
    try:
        conn = connect_db()
        if conn is None:
            raise Exception("No se pudo establecer la conexión a la base de datos.")
        cur = conn.cursor()

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]
        insert_query = """
        INSERT INTO agents_logs (date, response)
        VALUES (%s, %s)
        """
        cur.execute(insert_query, (timestamp, f"{level} - {message}"))
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(f"Error al insertar en la base de datos: {error}")
    finally:
        if conn is not None:
            conn.close()

# Modificar logging para incluir base de datos
def log_message(level, message):
    logging.log(level, message)
    logging.info(f"Logging to DB: {level} - {message}")
    log_to_db(level, message)

# Models for Users and Agents
class User:
    def __init__(self, properties):
        self.given_name = properties.get('hs_given_name')
        self.family_name = properties.get('hs_family_name')
        self.email = properties.get('hs_email')
        self.availability_status = properties.get('hs_availability_status')
        self.working_hours = properties.get('hs_working_hours')
        self.out_of_office_hours = properties.get('hs_out_of_office_hours')
        self.hubspot_team_id = properties.get('hubspot_team_id')
        self.hubspot_owner_id = properties.get('hubspot_owner_id')

class Agent:
    def __init__(self, properties):
        self.hubspot_owner_id = properties.get('hubspot_owner_id')
        self.availability_status = properties.get('availability_status')
        self.team_id = properties.get('hubspot_team_id')
        self.object_id = properties.get('hs_object_id')  # Assuming there's an object_id for update

class Ticket:
    def __init__(self, properties):
        self.object_id = properties.get('hs_object_id')
        self.pipeline = properties.get('hs_pipeline')
        self.hs_pipeline_stage = properties.get('hs_pipeline_stage')
        self.gx_agent_availability = properties.get('gx_agent_availability')
        self.hubspot_owner_id = properties.get('hubspot_owner_id')

# Function to fetch users from HubSpot
def get_users(after=None):
    url = "https://api.hubapi.com/crm/v3/objects/users"
    headers = {
        'accept': "application/json",
        'authorization': f"Bearer {crd.hs_sb_key}"
    }
    querystring = {
        "limit": "100",
        "properties": ["hs_availability_status", "hs_given_name", "hs_family_name", "hs_email", "hubspot_team_id", "hubspot_owner_id"],
        "archived": "false"
    }
    if after:
        querystring["after"] = after

    response = requests.get(url, headers=headers, params=querystring).json()
    return [User(user['properties']) for user in response.get('results', [])]


# Function to fetch agents from HubSpot
def get_agents(after=None):
    url = "https://api.hubapi.com/crm/v3/objects/agents"
    headers = {
        'accept': "application/json",
        'authorization': f"Bearer {crd.hs_sb_key}"
    }
    querystring = {
        "limit": "100",
        "properties": ["availability_status", "hubspot_team_id", "hubspot_owner_id"],
        "archived": "false"
    }
    if after:
        querystring["after"] = after

    response = requests.get(url, headers=headers, params=querystring).json()
    return [Agent(agent['properties']) for agent in response.get('results', [])]

#region GET TICKETS
#Function to get tickets
def get_tickets(key, agent_id):
    # HUBSPOT API
    client = hubspot.Client.create(access_token=key)
    after = None
    results = []

    while True:
        try:
            api_response = client.crm.associations.v4.basic_api.get_page(object_type="agents", object_id=agent_id, to_object_type="tickets", limit=500, after=after)
            results = (api_response.results)
            ticket_ids = [res.to_object_id for res in results]
            if not api_response.paging or not api_response.paging.next:
                break
            after = api_response.paging.next.after
        except ApiException as e:
            print("Exception when calling basic_api->get_page: %s\n" % e)
            break
    return ticket_ids

# Function to create a new agent
def create_agent(key, user):
    client = hubspot.Client.create(access_token=key)
    properties = {
        "agent_name": f"{user.given_name} {user.family_name}",
        "hubspot_owner_id": user.hubspot_owner_id,
        "availability_status": user.availability_status
    }
    simple_public_object_input_for_create = SimplePublicObjectInputForCreate(properties=properties)
    try:
        client.crm.objects.basic_api.create(object_type="agents", simple_public_object_input_for_create=simple_public_object_input_for_create)
        (f"{user.given_name} {user.family_name} - New Agent Created 🦾")
    except ApiException as e:
        logging.info(f"Error creating agent: {e}")


# Function to update an agent
def update_agent(agent, user):
    url = f"https://api.hubapi.com/crm/v3/objects/agents/{agent.object_id}"
    headers = {
        'accept': "application/json",
        'authorization': f"Bearer {crd.hs_sb_key}"
    }
    querystring = {
        "properties": {"availability_status": user.availability_status}
    }
    response = requests.request("PATCH", url, headers=headers, json=querystring)
    logging.info(f"Availability updated: {user.availability_status} -> {agent.availability_status} - user: {user.given_name} {user.family_name}")

#region UPDATE TICKETS
#Function to update agent's tickets
def update_tickets(key, agent, ticket):
    client = hubspot.Client.create(access_token=key)

    properties = {
        "gx_agent_availability": f"{agent.availability_status}"
    }
    simple_public_object_input = SimplePublicObjectInput(object_write_trace_id="string", properties=properties)
    try:
        api_response = client.crm.tickets.basic_api.update(ticket_id={ticket.object_id}, simple_public_object_input=simple_public_object_input)
        logging.info(f"Ticket Availability updated: {ticket.gx_agent_availability} -> {agent.availability_status}")
    except ApiException as e:
        logging.info("Exception when calling basic_api->update: %s\n" % e)

#region MAIN FUNCT
# Main function to sync users and agents
def sync_users_and_agents():
    users = get_users()
    agents = get_agents()

    # Map hubspot_owner_id to agent for faster lookup
    agent_map = {agent.hubspot_owner_id: agent for agent in agents}

    # Iterate through users to create or update agents
    for user in users:
        if user.hubspot_team_id == '133123965':
            agent = agent_map.get(user.hubspot_owner_id)
            ticket = get_tickets(crd.hs_sb_key, user.hubspot_owner_id)
            
            if agent:
                # Compare and update if needed
                if user.availability_status != agent.availability_status:
                    update_agent(agent, user)
                    get_tickets(crd.hs_sb_key, agent.object_id)
                    update_tickets(crd.hs_sb_key, agent, ticket)
                else:
                    logging.info(f"{user.given_name} {user.family_name} - Availability coincide: {user.availability_status} / {agent.availability_status}")
            else:
                # Create new agent if not found
                create_agent(crd.hs_sb_key, user)


# Run the sync process
sync_users_and_agents()

schedule.every(30).seconds.do(sync_users_and_agents)

while True:
    schedule.run_pending()
    # timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # print(f"{timestamp} - Execution of sync_users_and_agents")
    time.sleep(1)  # Espera 1 segundo entre cada iteraci n para evitar uso excesivo de recursos