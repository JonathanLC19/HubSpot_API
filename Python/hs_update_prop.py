import hubspot
from pprint import pprint
from hubspot.crm.properties import PropertyUpdate, ApiException

from credentials import hs_sb_key, hs_prod_key

import pandas as pd

""" ----------------------------------- SH DATAFRAME ---------------------------------------"""

# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import os

# def update_apt_of_interest():
#     #CREATE DF
#     # If modifying these scopes, delete the file token.json.
#     SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

#     # The ID and sheet name of a sample spreadsheet.
#     SAMPLE_SPREADSHEET_ID = "1V-soPvvFGxNNaAFMrBPS90FXOUKpKfxIJy9_u9mrIQg"
#     SHEET_NAME = "Sheet2"  # Change this to your sheet name


#     """Shows basic usage of the Sheets API.
#     Prints values from a sample spreadsheet.
#     """
#     creds = None
#     # The file token.json stores the user's access and refresh tokens and is
#     # created automatically when the authorization flow completes for the first
#     # time.
#     if os.path.exists("token.json"):
#         creds = Credentials.from_authorized_user_file("token.json", SCOPES)
#     # If there are no (valid) credentials available, let the user log in.
#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             flow = InstalledAppFlow.from_client_secrets_file(
#                 "gd_client.json", SCOPES
#             )
#             creds = flow.run_local_server(port=0)
#         # Save the credentials for the next run
#         with open("token.json", "w") as token:
#             token.write(creds.to_json())

#     try:
#         service = build("sheets", "v4", credentials=creds)

#         # Call the Sheets API to get the sheet ID by title
#         spreadsheet = service.spreadsheets().get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()
#         sheets = spreadsheet.get('sheets', [])
#         sheet_id = None
#         for sheet in sheets:
#             if sheet['properties']['title'] == SHEET_NAME:
#                 sheet_id = sheet['properties']['sheetId']
#                 break

#         if sheet_id is None:
#             print(f"Sheet '{SHEET_NAME}' not found.")
            

#         # Call the Sheets API to get values from the entire sheet
#         result = (
#             service.spreadsheets()
#             .values()
#             .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=f"{SHEET_NAME}")
#             .execute()
#         )
#         values = result.get("values", [])

#         if not values:
#             print("No data found.")
            

#         # Create a DataFrame from the values
#         columns = values[0]
#         data = values[1:]
#         df_wks = pd.DataFrame(data, columns=columns)
#         # df_wks['hours to touch'] = pd.to_numeric(df_wks['hours to touch'], errors = 'coerce').fillna(0).astype(int)

#         # Print the DataFrame or perform further operations
#         # print(df_wks['hours to touch'])

#     except HttpError as err:
#         print(err)

#     # print(df_wks['Name'])

""" ----------------------------------- METABASE ---------------------------------------"""
import requests
import metabase as mb

#connect metabase
response = requests.post('http://prod-metabase-lb-834391402.eu-central-1.elb.amazonaws.com/api/session',
                         json={'username': mb.mb_user,
                               'password': mb.mb_pass})
session_id = response.json()['id']
headers = {'X-Metabase-Session': session_id}

mb_url_card = mb.mb_url_card
mb_url = mb.mb_url

# Find public questions
response = requests.get(mb_url_card,
                         headers=headers).json()
questions = [q for q in response if q['public_uuid']]
print(f'{len(questions)} public of {len(response)} questions')

# get columns
uuid_1 = questions[7]['public_uuid']
response = requests.get(f'{mb_url}/api/public/card/{uuid_1}/query',
                        headers=headers)
response = response.json()
res = response['data']
type(res)

columns = [v['name'] for i, v in enumerate(response['data']['cols'])]

# Get Backoffice data
uuid_1 = questions[7]['public_uuid']
response = requests.get(f'{mb_url}/api/public/card/{uuid_1}/query',
                        headers=headers)
res = response.json()
rows = res['data']['rows']
cols = [v['name'] for i, v in enumerate(res['data']['cols'])]
bo_db = pd.DataFrame(rows, columns = cols)
bo_db = bo_db.dropna(subset=['codename'])
print(bo_db)

""" ----------------------------------- UPDATE PROPERTY ---------------------------------------"""


# #Get property values


# options_list = []
# for index, row in df_wks.iterrows():
#     n = 0
#     nr = n+1
#     options_list.append({'hidden': False, 'label': str(row['Name']), 'value': str(row['Name'])})
# # pprint(options_list)

# #Remove duplicates from options_list
# options_list_2 = []

# for dicc in options_list:
#     if dicc not in options_list_2:
#         options_list_2.append(dicc)

# # pprint(len(options_list_2))

# client = hubspot.Client.create(access_token= hs_sb_key)

# try:
#     read_prop = client.crm.properties.core_api.get_by_name(object_type="deals", property_name="apartment_of_interest___list", archived=False)
#     prop_options = read_prop.options
#     labels = [option.label for option in prop_options]

#     # pprint(labels)
# except ApiException as e:
#     print("Exception when calling core_api->get_by_name: %s\n" % e)



# # Manage property update


# if len(options_list_2) > len(read_prop.options):
#     property_update = PropertyUpdate(group_name="dealinformation", hidden=False, options=options_list_2)
#     try:
#         api_response = client.crm.properties.core_api.update(object_type="deals", property_name="apartment_of_interest___list", property_update=property_update)
#         # pprint(api_response)
#         pprint(f"se ha añadido un  nuevo apartamento: {df_wks['Name'].iloc[-1]}")
#     except ApiException as e:
#         print("Exception when calling core_api->update: %s\n" % e)

# elif len(options_list_2) < len(read_prop.options):
#     property_update = PropertyUpdate(group_name="dealinformation", hidden=False, options=options_list_2)
#     try:
#         api_response = client.crm.properties.core_api.update(object_type="deals", property_name="apartment_of_interest___list", property_update=property_update)
#         # pprint(api_response)
#         pprint(f"se ha actualizado la lista de apt. porque la liste de Key info es menor que los valores de la propiedad")
#     except ApiException as e:
#         print("Exception when calling core_api->update: %s\n" % e)

# elif len(options_list_2) == len(read_prop.options):
#     updated = False
#     for label in labels:
#         if label not in [option['label'] for option in options_list_2]:
#             property_update = PropertyUpdate(group_name="dealinformation", hidden=False, options=options_list_2)
#             try:
#                 api_response = client.crm.properties.core_api.update(object_type="deals", property_name="apartment_of_interest___list", property_update=property_update)
#                 pprint(f"Se ha cambiado el nombre del apt. {label}")
#                 # pprint(api_response)
#                 updated = True
#             except ApiException as e:
#                 print("Exception when calling core_api->update: %s\n" % e)
#     if not updated:
#         pprint(f"No new apartments in the list. HubSpot property: {len(read_prop.options)} -> Key Info {len(options_list_2)}")

# else:
#     pprint(f"No new apartments in the list. HubSpot property: {len(read_prop.options)} -> Key Info {len(options_list_2)}")




# """ ----------------------------------- SCHEDULE FUNCTION ---------------------------------------"""

# import schedule
# import time

# # def job():
# #     print("I'm working...")

# # schedule.every(5).seconds.do(job)
# # schedule.every(10).minutes.do(job)
# # schedule.every().hour.do(job)
# # schedule.every().day.at("10:30").do(job)
# # schedule.every(5).to(10).minutes.do(job)
# # schedule.every().monday.do(job)
# # schedule.every().wednesday.at("13:15").do(job)
# # schedule.every().day.at("12:42", "Europe/Amsterdam").do(job)
# # schedule.every().minute.at(":17").do(job)

# s = schedule.every().day.at("07:40", "Europe/London").do(update_apt_of_interest)
# # print(s.next_run)
# print(repr(s))

# while True:
#     schedule.run_pending()
#     time.sleep(1)