from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import os.path
import pandas as pd
from datetime import datetime, timedelta

# Función para convertir una fecha y una hora en timestamp
def obtener_timestamp(fecha_str, hora_str):
    fecha = datetime.strptime(fecha_str, "%d/%m/%Y")  # convertir el string de fecha a datetime
    hora_inicio, hora_fin = map(int, hora_str.split('-'))
    
    # Ajustamos si la hora de fin es menor que la de inicio (turno que pasa de medianoche)
    if hora_fin <= hora_inicio:
        hora_fin += 24

    # Calculamos los timestamps en milisegundos
    ts_inicio = int((fecha + timedelta(hours=hora_inicio)).timestamp()) * 1000
    ts_fin = int((fecha + timedelta(hours=hora_fin)).timestamp()) * 1000
    
    return ts_inicio, ts_fin

# Función para calcular las horas fuera de la oficina
def horas_fuera(shift, fecha):
    if shift == "REST":
        # Si está en descanso todo el día, fuera de la oficina todo el día
        ts_dia_inicio = int(datetime.strptime(fecha, "%d/%m/%Y").timestamp()) * 1000
        ts_dia_fin = int((datetime.strptime(fecha, "%d/%m/%Y") + timedelta(hours=23, minutes=59)).timestamp()) * 1000
        return f'[{{"startTimestamp":{ts_dia_inicio},"endTimestamp":{ts_dia_fin}}}]'
    
    try:
        # Obtener timestamps del turno
        ts_inicio, ts_fin = obtener_timestamp(fecha, shift)
        
        # Timestamp del inicio y fin del día
        ts_dia_inicio = int(datetime.strptime(fecha, "%d/%m/%Y").timestamp()) * 1000
        ts_dia_fin = int((datetime.strptime(fecha, "%d/%m/%Y") + timedelta(hours=23, minutes=59)).timestamp()) * 1000
        
        # Horas fuera de la oficina son antes del inicio del turno y después del fin del turno
        return f'[{{"startTimestamp":{ts_dia_inicio},"endTimestamp":{ts_inicio}}},{{"startTimestamp":{ts_fin},"endTimestamp":{ts_dia_fin}}}]'
    
    except:
        return '[]'  # En caso de que haya algún valor que no entendamos

def escape_quotes(value):
    """
    Escapa el símbolo de comilla simple en una cadena de texto.
    """
    return value.replace("'", "''") if isinstance(value, str) else value

#region CREATE DF
# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and sheet name of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1OQhD6sp3YQM289yMTs_fN3TfFjAx7Ob5KLJ5n4VSTlg"
SHEET_NAME = "GX - December 2023"  # Change this to your sheet name


"""Shows basic usage of the Sheets API.
Prints values from a sample spreadsheet.
"""
def get_shifts():
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
        # print(values)

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

    # new_df = df_wks[df_wks['booking_date'].notna()].copy()
    # print(df_wks)

    # for index, row in df_wks.iterrows():
    #     if row['Sep'][index] == "9-18":
    #         print(row['User'])
    #     else:
    #         print("None")


    # Suponiendo que ya tienes cargado tu DataFrame con los datos

    # Usamos melt para cambiar de formato ancho a largo
    df_melted = pd.melt(df_wks, 
                        id_vars=['User', 'Email', 'Position'],  # columnas que no queremos derretir
                        var_name='Day',                         # el nombre que le daremos a las columnas (días/meses)
                        value_name='Shift')                     # el nombre que le daremos a los valores (turnos)

    # Aplicamos la función a cada fila para calcular las horas fuera de la oficina
    df_melted['OutOfOffice'] = df_melted.apply(lambda row: horas_fuera(row['Shift'], row['Day']), axis=1)

    return df_melted

    # Mostramos el resultado
    # print(df_melted['OutOfOffice'][2010])

    # Mostramos los primeros registros para verificar
# print(get_shifts())