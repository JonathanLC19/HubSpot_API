import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from credentials import db_password, db_user, db_database, db_host

import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

def escape_quotes(value):
    """
    Escapa el símbolo de comilla simple en una cadena de texto.
    """
    return value.replace("'", "''") if isinstance(value, str) else value


# # If modifying these scopes, delete the file token.json.
# SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# # The ID and range of a sample spreadsheet.
# SAMPLE_SPREADSHEET_ID = "1LuKkgplMFDJSevHet2lCO14_0q-gaY3CGbAzzEndHPw"
# SAMPLE_RANGE_NAME = "NORMALIZED"


# def main():
#   """Shows basic usage of the Sheets API.
#   Prints values from a sample spreadsheet.
#   """
#   creds = None
#   # The file token.json stores the user's access and refresh tokens, and is
#   # created automatically when the authorization flow completes for the first
#   # time.
#   if os.path.exists("token.json"):
#     creds = Credentials.from_authorized_user_file("token.json", SCOPES)
#   # If there are no (valid) credentials available, let the user log in.
#   if not creds or not creds.valid:
#     if creds and creds.expired and creds.refresh_token:
#       creds.refresh(Request())
#     else:
#       flow = InstalledAppFlow.from_client_secrets_file(
#           "gd_client.json", SCOPES
#       )
#       creds = flow.run_local_server(port=0)
#     # Save the credentials for the next run
#     with open("token.json", "w") as token:
#       token.write(creds.to_json())

#   try:
#     service = build("sheets", "v4", credentials=creds)

#     # Call the Sheets API
#     sheet = service.spreadsheets()
#     result = (
#         sheet.values()
#         .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=SAMPLE_RANGE_NAME)
#         .execute()
#     )
#     values = result.get("values", [])

#     if not values:
#       print("No data found.")
#       return

#     print("Name, Major:")
#     for row in values:
#       # Print columns A and E, which correspond to indices 0 and 4.
#       print(values)
#   except HttpError as err:
#     print(err)


# if __name__ == "__main__":
#   main()


#CREATE DF
# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and sheet name of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1Ct2XT-dWMtZd0qCLD0z3pzylmphcb6J_RFeV-44SMvA"
SHEET_NAME = "NORMALIZED"  # Change this to your sheet name


"""Shows basic usage of the Sheets API.
Prints values from a sample spreadsheet.
"""
creds = None
# The file token.json stores the user's access and refresh tokens and is
# created automatically when the authorization flow completes for the first
# time.
if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
# If there are no (valid) credentials available, let the user log in.
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            "gd_client.json", SCOPES
        )
        creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
        token.write(creds.to_json())

try:
    service = build("sheets", "v4", credentials=creds)

    # Call the Sheets API to get the sheet ID by title
    spreadsheet = service.spreadsheets().get(spreadsheetId=SAMPLE_SPREADSHEET_ID).execute()
    sheets = spreadsheet.get('sheets', [])
    sheet_id = None
    for sheet in sheets:
        if sheet['properties']['title'] == SHEET_NAME:
            sheet_id = sheet['properties']['sheetId']
            break

    if sheet_id is None:
        print(f"Sheet '{SHEET_NAME}' not found.")
        

    # Call the Sheets API to get values from the entire sheet
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SAMPLE_SPREADSHEET_ID, range=f"{SHEET_NAME}")
        .execute()
    )
    values = result.get("values", [])

    if not values:
        print("No data found.")
        

    # Create a DataFrame from the values
    columns = values[0]
    data = values[1:]
    df_wks = pd.DataFrame(data, columns=columns)

    # Print the DataFrame or perform further operations
    # print(df_wks['booking_type'])

except HttpError as err:
    print(err)



# Conectar a la base de datos
conn_details = psycopg2.connect(
    host=db_host,
    database=db_database,
    user=db_user,
    password=db_password,
    port=5432
)

cursor = conn_details.cursor()

# # Recorre las filas del DataFrame y ejecuta consultas INSERT o UPDATE
# for index, row in df_wks.iterrows():
#     owner = row["owner"]
#     activity_type = escape_quotes(row['activity_type'])
#     activity_id = row['activity_id']
#     activity_date = row['activity_date']

#     # Inserta una nueva fila
#     insert_query = f'''
#         INSERT INTO new_activities_q4 (owner, activity_type, activity_id, activity_date)
#         VALUES ('{owner}', '{activity_type}', {activity_id}, '{activity_date}');
#     '''
#     cursor.execute(insert_query)
#     conn_details.commit()
#     print(f"Se insertó la fila {index + 1} en la tabla new_activities_q4. ID: {activity_id}")

# cursor.close()
# conn_details.close()

"""--------------------------------------- EXECUTION PERFORMANCE ENHANCED --------------------------------------------"""

try:
    # Iniciar transacción
    conn_details.autocommit = False
    cursor = conn_details.cursor()

    # Recorre las filas del DataFrame y prepara los datos para la inserción
    rows = []
    for index, row in df_wks.iterrows():
        owner = row["owner"]
        activity_type = escape_quotes(row['activity_type'])
        activity_id = row['activity_id']
        activity_date = row['activity_date']

        # Agrega datos a la lista de filas
        rows.append((owner, activity_type, activity_id, activity_date))
        print(f"Se insertó la fila {index + 1} en la tabla new_activities_q4. ID: {activity_id}")
    # Inserta o actualiza las filas usando execute_values
    execute_values(
        cursor,
        '''
        INSERT INTO new_activities_q4 (owner, activity_type, activity_id, activity_date)
        VALUES %s;
        ''',
        rows
    )

    # Confirmar la transacción
    conn_details.commit()
    print(f"Se insertaron las filas en la tabla new_activities_q4.")

except Exception as e:
    # Si hay algún error, realiza un rollback
    print(f"Error: {e}")
    conn_details.rollback()

finally:
    # Restablecer el modo de autocommit y cerrar el cursor y la conexión
    conn_details.autocommit = True
    cursor.close()
    conn_details.close()
