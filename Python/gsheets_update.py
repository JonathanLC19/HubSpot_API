import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

import functions
import creds2
import get_deals

props = ['hs_record_id', 'dealname', 'hs_created_by_user_id', 'pipeline']

dataframe = get_deals.get_all_deals_df(creds2.hs_test, props)
dropcol = ['properties','properties_with_history']
# dataframe = dataframe.drop(columns = dropcol)

# # Convertir las columnas de tipo Timestamp a strings
# dataframe['created_at'] = dataframe['created_at'].dt.strftime('%Y-%m-%d %H:%M:%S')
# dataframe['updated_at'] = dataframe['updated_at'].dt.strftime('%Y-%m-%d %H:%M:%S')

gc = gspread.oauth()

sh = gc.open("My new spreadsheet")
worksheet = sh.get_worksheet(0)

# worksheet.update(values = [[3, 4], [5, 6]], range_name='A1:B2')
worksheet.update([dataframe.columns.values.tolist()] + dataframe.values.tolist())



print(worksheet.get("A1"))




# dataframe = pd.DataFrame({
#     'Nombre': ['Juan', 'María', 'Pedro'],
#     'Edad': [25, 27, 30]
# })


# # credentials = ServiceAccountCredentials.from_service_account_file('gsheets_key.json')
# # gc = gspread.authorize(credentials)
# # sheet = gc.open_by_key('1SDNa3OVsONWX5NDHHrgKzZBaNedXelLFcWWp6yC42Hw')

# # range = 'A1:Z'

# # data = dataframe.values.tolist()
# # range.update(data)


# # If modifying these scopes, delete the file token.json.
# SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# # The ID and sheet name of a sample spreadsheet.
# SAMPLE_SPREADSHEET_ID = "1SDNa3OVsONWX5NDHHrgKzZBaNedXelLFcWWp6yC42Hw"
# SHEET_NAME = "tests"  # Change this to your sheet name

# # Your data to update the spreadsheet
# new_values = [
#     ["New Value 1", "New Value 2", "New Value 3"],
#     ["Another New Value 1", "Another New Value 2", "Another New Value 3"],
# ]

# creds = None

# # The file token.json stores the user's access and refresh tokens and is
# # created automatically when the authorization flow completes for the first
# # time.
# if os.path.exists("token.json"):
#     creds = Credentials.from_authorized_user_file("token.json", SCOPES)
# # If there are no (valid) credentials available, let the user log in.
# if not creds or not creds.valid:
#     if creds and creds.expired and creds.refresh_token:
#         creds.refresh(Request())
#     else:
#         flow = InstalledAppFlow.from_client_secrets_file(
#             "oauth.json", SCOPES
#         )
#         creds = flow.run_local_server(port=0)
#     # Save the credentials for the next run
#     with open("token.json", "w") as token:
#         token.write(creds.to_json())

# try:
#     service = build("sheets", "v4", credentials=creds)

#     # Specify the range where you want to update the values
#     update_range = f"{SHEET_NAME}!A1:C{len(new_values) + 1}"  # Assuming 3 columns

#     # Call the Sheets API to update the values in the specified range
#     update_body = {"values": new_values}
#     update_result = (
#         service.spreadsheets()
#         .values()
#         .update(
#             spreadsheetId=SAMPLE_SPREADSHEET_ID,
#             range=update_range,
#             body=update_body,
#             valueInputOption="RAW",
#         )
#         .execute()
#     )

#     print(f"Updated {update_result['updatedCells']} cells.")

# except HttpError as err:
#     print(err)

