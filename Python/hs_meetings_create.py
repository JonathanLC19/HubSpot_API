#region CREATE MEETINGS - CALENDAR

from datetime import datetime, timedelta
import os.path
import pickle
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import hubspot
from pprint import pprint
from hubspot.crm.objects.meetings import SimplePublicObjectInputForCreate, ApiException

# Reemplaza por tu archivo de credenciales
import credentials as creds
import dotenv

dotenv.load_dotenv()

today = (datetime.now()).replace(hour=0, minute=0, second=0, microsecond=0)
today_str = today.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + 'Z'

# Google Calendar SCOPES
SCOPES = ['https://www.googleapis.com/auth/calendar']

def get_calendar_service():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # Si no hay credenciales válidas disponibles, permite que el usuario inicie sesión
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'calendar_creds.json', SCOPES)
            creds = flow.run_local_server(port=8080)
        # Guarda las credenciales para la próxima ejecución
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    service = build('calendar', 'v3', credentials=creds)
    return service

def create_event(service, summary, location, description, start_time, end_time):
    event = {
        'summary': summary,
        'location': location,
        'description': description,
        'start': {
            'dateTime': start_time,
            'timeZone': 'Europe/London',  # Cambia a tu zona horaria
        },
        'end': {
            'dateTime': end_time,
            'timeZone': 'Europe/London',  # Cambia a tu zona horaria
        },
        'reminders': {
            'useDefault': False,
            'overrides': [
                {'method': 'email', 'minutes': 24 * 60},
                {'method': 'popup', 'minutes': 10},
            ],
        },
    }

    event = service.events().insert(calendarId='primary', body=event).execute()
    print(f"Event created: {event.get('htmlLink')}")
    return event.get('htmlLink')

if __name__ == '__main__':
    service = get_calendar_service()

    # Formulario simple en consola
    summary = input("Ingrese el título del evento: ")
    location = input("Ingrese la ubicación del evento: ")
    description = input("Ingrese la descripción del evento: ")
    start_time = input("Ingrese la fecha y hora de inicio (YYYY-MM-DDTHH:MM:SS): ")
    end_time = input("Ingrese la fecha y hora de fin (YYYY-MM-DDTHH:MM:SS): ")

    # Crea el evento en Google Calendar
    event_link = create_event(service, summary, location, description, start_time, end_time)

#region CREATE MEETINGS - HUBSPOT

    # Configura la autenticación en HubSpot
    client = hubspot.Client.create(access_token=creds.hs_sb_key)

    # Formato de fecha y hora para HubSpot
    start_time_hs = f"{start_time}.000Z"
    end_time_hs = f"{end_time}.000Z"

    # Crea las propiedades del meeting en HubSpot
    properties = {
        "hs_meeting_title": summary,
        "hubspot_owner_id": 897783247,  # Reemplaza con tu ID de propietario
        "hs_internal_meeting_notes": description,
        "hs_meeting_start_time": start_time_hs,
        "hs_meeting_end_time": end_time_hs,
        'hs_meeting_external_url': event_link,
        'hs_timestamp': today_str
    }

    simple_public_object_input_for_create = SimplePublicObjectInputForCreate(
        associations=[
            {"types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 200}],
             "to": {"id": 10886146539}}  # Reemplaza con tu ID de contacto u objeto relacionado
        ],
        properties=properties
    )

    try:
        api_response = client.crm.objects.meetings.basic_api.create(
            simple_public_object_input_for_create=simple_public_object_input_for_create
        )
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling basic_api->create: %s\n" % e)