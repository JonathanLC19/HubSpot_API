import streamlit as st
from streamlit_datetime_range_picker import datetime_range_picker
import datetime
import os.path
import pickle
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import hubspot
from hubspot.crm.objects.meetings import SimplePublicObjectInputForCreate, ApiException
from dotenv import load_dotenv
import os
load_dotenv()

# Google Calendar SCOPES
SCOPES = ['https://www.googleapis.com/auth/calendar']

# Initialize session state for available slots
if 'slots' not in st.session_state:
    st.session_state.slots = {
        'morning': 0,
        'afternoon': 0
    }

def get_calendar_service():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('../calendar_creds.json', SCOPES)
            creds = flow.run_local_server(port=8080)
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
            'timeZone': 'Europe/Madrid',
        },
        'end': {
            'dateTime': end_time,
            'timeZone': 'Europe/Madrid',
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
    return event.get('htmlLink')

def book_slot():
    service = get_calendar_service()
    summary = st.session_state.summary
    location = st.session_state.location
    description = st.session_state.description
    start_time = st.session_state.start_time.isoformat()
    end_time = st.session_state.end_time.isoformat()

    today = datetime.date.today()
    today_str = today.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-3] + 'Z'

    event_link = create_event(service, summary, location, description, start_time, end_time)

    client = hubspot.Client.create(access_token=os.environ['hs_sb_key'])
    properties = {
        "hs_meeting_title": summary,
        "hubspot_owner_id": 897783247,
        "hs_internal_meeting_notes": description,
        "hs_meeting_start_time": f"{start_time}Z",
        "hs_meeting_end_time": f"{end_time}Z",
        'hs_meeting_external_url': event_link,
        'hs_timestamp': today_str
    }

    simple_public_object_input_for_create = SimplePublicObjectInputForCreate(
        associations=[{"types": [{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 200}],
                       "to": {"id": 10886146539}}],
        properties=properties
    )

    try:
        client.crm.objects.meetings.basic_api.create(
            simple_public_object_input_for_create=simple_public_object_input_for_create
        )
        st.success("Cita reservada con éxito.")
    except ApiException as e:
        st.error(f"Error al reservar la cita: {e}")

# Streamlit UI
st.title("Reserva de Citas")

st.date_input("Seleccione una fecha", key='date')

slot_options = []
if st.session_state.slots['morning'] < 2:
    slot_options.append("Mañana (09:00 - 14:00)")
if st.session_state.slots['afternoon'] < 2:
    slot_options.append("Tarde (17:00 - 20:00)")

slot_choice = st.selectbox("Seleccione una franja horaria", slot_options)

if slot_choice == "Mañana (09:00 - 14:00)":
    start_time = datetime.datetime.combine(st.session_state.date, datetime.time(9, 0))
    end_time = datetime.datetime.combine(st.session_state.date, datetime.time(14, 0))
elif slot_choice == "Tarde (17:00 - 20:00)":
    start_time = datetime.datetime.combine(st.session_state.date, datetime.time(17, 0))
    end_time = datetime.datetime.combine(st.session_state.date, datetime.time(20, 0))
else:
    st.stop()

st.text_input("Título de la cita", key='summary')
st.text_input("Ubicación", key='location')
st.text_area("Descripción", key='description')

st.session_state.start_time = start_time
st.session_state.end_time = end_time

if st.button("Reservar cita"):
    if slot_choice == "Mañana (09:00 - 14:00)":
        st.session_state.slots['morning'] += 1
    elif slot_choice == "Tarde (17:00 - 20:00)":
        st.session_state.slots['afternoon'] += 1
    book_slot()