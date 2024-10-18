import pandas as pd
from pandasai import SmartDataframe, SmartDatalake
from pandasai.llm import OpenAI

import credentials as credentials
import metabase

""" ----------------------------------- SH DATAFRAME ---------------------------------------"""

# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# import os

# #CREATE DF
# # If modifying these scopes, delete the file token.json.
# SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# # The ID and sheet name of a sample spreadsheet.
# SAMPLE_SPREADSHEET_ID = "1_gsQIM5Bc-SBYA9laTgd35J9UJDfZJXxNcDIJiNXbNc"
# SHEET_NAME = "bcn"  # Change this to your sheet name


# """Shows basic usage of the Sheets API.
# Prints values from a sample spreadsheet.
# """
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
#             "gd_client.json", SCOPES
#         )
#         creds = flow.run_local_server(port=0)
#     # Save the credentials for the next run
#     with open("token.json", "w") as token:
#         token.write(creds.to_json())

# try:
#     service = build("sheets", "v4", credentials=creds)

#     # Call the Sheets API to get the sheet ID by title
#     spreadsheet = service.spreadsheets().get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()
#     sheets = spreadsheet.get('sheets', [])
#     sheet_id = None
#     for sheet in sheets:
#         if sheet['properties']['title'] == SHEET_NAME:
#             sheet_id = sheet['properties']['sheetId']
#             break

#     if sheet_id is None:
#         print(f"Sheet '{SHEET_NAME}' not found.")
        

#     # Call the Sheets API to get values from the entire sheet
#     result = (
#         service.spreadsheets()
#         .values()
#         .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=f"{SHEET_NAME}")
#         .execute()
#     )
#     values = result.get("values", [])

#     if not values:
#         print("No data found.")
        

#     # Create a DataFrame from the values
#     columns = values[0]
#     data = values[1:]
#     df_wks = pd.DataFrame(data, columns=columns)
#     # df_wks['hours to touch'] = pd.to_numeric(df_wks['hours to touch'], errors = 'coerce').fillna(0).astype(int)

#     # Print the DataFrame or perform further operations
#     # print(df_wks['hours to touch'])

# except HttpError as err:
#     print(err)

""" ----------------------------------- READ DATABASE ---------------------------------------"""

# import psycopg2

# # Establece la conexión a la base de datos PostgreSQL
# conn = psycopg2.connect(
#     dbname=credentials.db_database,
#     user=credentials.db_user,
#     password=credentials.db_password,
#     host=credentials.db_host  # O la dirección de tu servidor PostgreSQL
# )

# # Define la consulta SQL para seleccionar los datos de la tabla
# sql_query = "SELECT * FROM hs_deals_q4_tests"

# # Lee los datos de la tabla en un DataFrame de pandas
# df = pd.read_sql_query(sql_query, conn)

# # Cierra la conexión a la base de datos
# conn.close()

from sqlalchemy import create_engine, text

# Conecta a la base de datos PostgreSQL usando SQLAlchemy
engine = create_engine(f'postgresql+psycopg2://{credentials.db_user}:{credentials.db_password}@{credentials.db_host}:5432/{credentials.db_database}')

# # Define la consulta SQL para seleccionar los datos de la tabla
# sql_query = "SELECT * FROM hs_deals_q4_tests"

# # Lee los datos de la tabla en un DataFrame de pandas usando SQLAlchemy
# df = pd.read_sql_query(sql_query, engine)

with engine.begin() as conn:
    query = text("""SELECT * FROM hs_deals_q4_tests""")
    df = pd.read_sql_query(query, conn)

# Cierra la conexión a la base de datos
engine.dispose()


pd.set_option('display.max_rows', None)



###################  BOOKINGS TABLE ##########################

import requests

# Find public questions
response = requests.get(metabase.mb_url_card,
                         headers=metabase.headers).json()
questions = [q for q in response if q['public_uuid']]

# Get Backoffice data
uuid_1 = questions[0]['public_uuid']
response = requests.get(f'{metabase.mb_url}/api/public/card/{uuid_1}/query',
                        headers=metabase.headers)
rows = response.json()['data']['rows']
bo_db = pd.DataFrame(rows, columns =['ID','Check In','Check Out','Apartment ID','Guest ID','Inserted At','Updated At','Monthly Price','Nights',
                                    'Country','State','Pets','Source','Imported Pre Be','Check In Done','Check Out Done','Datocms ID','Code','Canceled At',
                                    'Stay Reason','Temporary Moving','Guests → ID','Guests → Name','Guests → Email','Guests → Phone',
                                    'Guests → Inserted At','Guests → Updated At','Guests → Last Name',
                                    'Apartments → ID','Apartments → City','Apartments → Name','Apartments → Codename',
                                    'Apartments → Unit Number','Apartments → Number Of Rooms','Apartments → Number Of Bedrooms',
                                    'Apartments → Property Floor','Apartments → Max Guests','Apartments → Latitude','Apartments → Longitude',
                                    'Apartments → Interior Sqm','Apartments → Exterior Sqm','Apartments → Monthly Rent','Apartments → Landlord Rent',
                                    'Apartments → Design','Apartments → Number Of Bathrooms','Apartments → Inserted At','Apartments → Updated At',
                                    'Apartments → Amenities','Apartments → Neighbourhood',
                                    'Apartments → Parent Listing ID','Apartments → Contract Start Date','Apartments → Contract End Date',
                                    'Apartments → First Landlord Rent Payment Date','Apartments → Guest Ready Date','Apartments → Total Sqm','Apartments → Building Utilities',
                                    'Booking Events → Data → Booking From ID','Booking Events → Data → Booking To ID','Booking Events → Data → New Check Out',
                                    'Booking Events → Data → Old Check Out','Booking Events → ID','Booking Events → Name','Booking Events → Booking ID',
                                    'Booking Events → Inserted At','Booking Events → Updated At'
                                    ])

bo_db['ID'] = bo_db['ID'].astype(int)
""" --------------------------------------- AI ---------------------------------------------"""
# Sample DataFrame
# df = pd.DataFrame({
#     "country": ["United States", "United Kingdom", "France", "Germany", "Italy", "Spain", "Canada", "Australia", "Japan", "China"],
#     "gdp": [19294482071552, 2891615567872, 2411255037952, 3435817336832, 1745433788416, 1181205135360, 1607402389504, 1490967855104, 4380756541440, 14631844184064],
#     "happiness_index": [6.94, 7.16, 6.66, 7.07, 6.38, 6.4, 7.23, 7.22, 5.87, 5.12]
# })

# Instantiate a LLM
llm = OpenAI(api_token=credentials.openai_key)

df = SmartDataframe(df, config={"llm": llm})
print("Write your prompt:")
input = input()
response = df.chat(input)

# print(response)

# df = SmartDatalake([df, bo_db], config={"llm": llm})
# # booked = df.chat("Find the bookings which booking_id does not appear in deals and return booking_id, Inserted at, Source, State, Booking Events → Name, Guests → Name, Guests → Last Name, Guests → Email, Guests → Phone, Apartments → Codename")

# print("Write your prompt:")
# input = input()
# response = df.chat(input)

print(response)
# Output: United Kingdom, Canada, Australia, United States, Germany