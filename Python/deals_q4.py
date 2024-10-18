import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from credentials import db_password, db_user, db_database, db_host

import os.path
import time

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

start = time.perf_counter()

def escape_quotes(value):
    """
    Escapa el símbolo de comilla simple en una cadena de texto.
    """
    return value.replace("'", "''") if isinstance(value, str) else value

#region CREATE DF
# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

# The ID and sheet name of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = "1LuKkgplMFDJSevHet2lCO14_0q-gaY3CGbAzzEndHPw"
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

# new_df = df_wks[df_wks['booking_date'].notna()].copy()
# print(df_wks.head())

#region DDBB CONNECTION

# Conectar a la base de datos
conn_details = psycopg2.connect(
    host=db_host,
    database=db_database,
    user=db_user,
    password=db_password,
    port=5432
)

cursor = conn_details.cursor()

#region DDBB INSERT
"""--------------------------------- ALL DATA (df_wks) --------------------------------------------------"""


# Iniciar transacción
try:
    # Iniciar transacción
    conn_details.autocommit = False
    cursor = conn_details.cursor()

    # Recorre las filas del DataFrame y prepara los datos para la inserción o actualización
    rows = []
    for index, row in df_wks.iterrows():
        deal_id = row["Deal ID"]
        deal_name = escape_quotes(row["Deal Name"])
        create_date = row["Create Date"] if row['Create Date'] else '1997-01-01'
        close_date = row["Close Date"] if row['Close Date'] else '1997-01-01'
        pipeline = row["Pipeline"]
        deal_stage = row["Deal Stage"]
        deal_owner = row["Deal Owner"]
        billing_type = None if row["Billing Type"] == '' else row["Billing Type"]
        guest_type = None if row["Guest Type"] == '' else row["Guest Type"]
        check_in_date = row["Check in Date"] if row["Check in Date"] else '1997-01-01'
        check_out_date = row["Check out Date"] if row["Check out Date"] else '1997-01-01'
        lenght_of_stay = row["Lenght of Stay"] if row["Lenght of Stay"] else '0'
        budget = row["Monthly Rent"]
        apt_of_interest = row["Apartment of Interest"]
        apt_booked = row["Apartment Booked"]
        neighborhood = row["Neighborhood"]
        purpose_of_rental = row["Purpose of Rental"]
        company_sponsored = row["Company Sponsored"]
        pet_fee_required = row["Pet Friendly Apt. Required"]
        source = row["Deal Source"]
        source_drilldown = row["Deal Source Drill Down"]
        contact_email = row["Associated Contact Email"]
        contact_phone = row["Associated Contact Phone"]
        timestamp = row["Timestamp"]
        inbound_outbound = row["b2b_source"]
        time_to_close = None if row["time_to_close"] == '' else row["time_to_close"]
        is_won = row["is_won"]
        booking_type = None if row["booking_type"] == '' else row["booking_type"]
        time_to_won = None if row["time_to_won"] == '' else row["time_to_won"]
        backoffice_id = None if row["backoffice_id"] == '' else row["backoffice_id"]
        city = None if row["city"] == '' else row["city"]
        booking_date = None if row["booking_date"] == '' else row["booking_date"]
        last_update = row['last_update'] if row['last_update'] != '' else 'NULL'
        margin_x_stay = None if row["margin_x_stay"] == '' else row["margin_x_stay"]
        rent_amount = None if row["rent_amount"] == '' else row["rent_amount"]
        sadmin_utilities = None if row["sadmin_utilities"] == '' else row["sadmin_utilities"]
        sadmin_final_cleaning = None if row["sadmin_final_cleaning"] == '' else row["sadmin_final_cleaning"]
        sadmin_pet_fee = None if row["sadmin_pet_fee"] == '' else row["sadmin_pet_fee"]
        b2c_entered_new_deal_stage = None if row["b2c_entered_new_deal_stage"] == '' else row["b2c_entered_new_deal_stage"]	
        b2c_entered_attempt_stage = None if row["b2c_entered_attempt_stage"] == '' else row["b2c_entered_attempt_stage"]	
        b2c_entered_connected_stage = None if row["b2c_entered_connected_stage"] == '' else row["b2c_entered_connected_stage"]	
        b2c_entered_proposal_stage = None if row["b2c_entered_proposal_stage"] == '' else row["b2c_entered_proposal_stage"]	
        b2c_entered_negotiation_stage = None if row["b2c_entered_negotiation_stage"] == '' else row["b2c_entered_negotiation_stage"]
        b2b_entered_discovery_stage = None if row["b2b_entered_discovery_stage"] == '' else row["b2b_entered_discovery_stage"]
        b2b_entered_proposal_stage = None if row["b2b_entered_proposal_stage"] == '' else row["b2b_entered_proposal_stage"]
        b2b_entered_negotiation_stage = None if row["b2b_entered_negotiation_stage"] == '' else row["b2b_entered_negotiation_stage"]
        kam_entered_enquiry_stage = None if row["kam_entered_enquiry_stage"] == '' else row["kam_entered_enquiry_stage"]
        kam_entered_proposal_stage = None if row["kam_entered_proposal_stage"] == '' else row["kam_entered_proposal_stage"]
        kam_entered_engaged_stage = None if row["kam_entered_engaged_stage"] == '' else row["kam_entered_engaged_stage"]
        kam_entered_block_stage = None if row["kam_entered_block_stage"] == '' else row["kam_entered_block_stage"]
        nr_of_bedrooms = None if row["nr_of_bedrooms"] == '' else row["nr_of_bedrooms"]
        lost_reason = None if row["lost_reason"] == '' else row["lost_reason"]
        lost_reason_dd =None if row["lost_reason_dd"] == '' else row["lost_reason_dd"]


        # Verifica si el deal_id existe en la tabla hs_deals_q4
        cursor.execute(f"SELECT * FROM hs_deals_q4 WHERE deal_id = {deal_id}")
        existing_deal = cursor.fetchone()

        if existing_deal:
            # Si existe, actualiza la fila
            rows.append((
                deal_id, deal_name, create_date, close_date, pipeline, deal_stage, deal_owner,
                billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
                budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
                company_sponsored, pet_fee_required, source, source_drilldown,
                contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
                is_won, booking_type, time_to_won, backoffice_id, city, booking_date,
                last_update, margin_x_stay, rent_amount, sadmin_utilities, sadmin_final_cleaning,
                sadmin_pet_fee, b2c_entered_new_deal_stage,	b2c_entered_attempt_stage, b2c_entered_connected_stage,
                b2c_entered_proposal_stage,	b2c_entered_negotiation_stage,	b2b_entered_discovery_stage,
                b2b_entered_proposal_stage,	b2b_entered_negotiation_stage, kam_entered_enquiry_stage,
                kam_entered_proposal_stage, kam_entered_engaged_stage, kam_entered_block_stage, nr_of_bedrooms,
                lost_reason, lost_reason_dd
            ))
            print(f"Se actualizó la fila {index + 1} en la tabla hs_deals_q4. ID: {deal_id}")
        else:
            # Si no existe, inserta una nueva fila
            rows.append((
                deal_id, deal_name, create_date, close_date, pipeline, deal_stage, deal_owner,
                billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
                budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
                company_sponsored, pet_fee_required, source, source_drilldown,
                contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
                is_won, booking_type, time_to_won, backoffice_id, city, booking_date,
                last_update, margin_x_stay, rent_amount, sadmin_utilities, sadmin_final_cleaning,
                sadmin_pet_fee, b2c_entered_new_deal_stage,	b2c_entered_attempt_stage, b2c_entered_connected_stage,
                b2c_entered_proposal_stage,	b2c_entered_negotiation_stage,	b2b_entered_discovery_stage,
                b2b_entered_proposal_stage,	b2b_entered_negotiation_stage, kam_entered_enquiry_stage,
                kam_entered_proposal_stage, kam_entered_engaged_stage, kam_entered_block_stage, nr_of_bedrooms,
                lost_reason, lost_reason_dd
            ))
            print(f"Se insertó la fila {index + 1} en la tabla hs_deals_q4. ID: {deal_id}")

    # Inserta o actualiza las filas usando execute_values
    execute_values(
        cursor,
        '''
        INSERT INTO hs_deals_q4 (
            deal_id, deal_name, create_date, close_date, pipeline, deal_stage, deal_owner,
            billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
            budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
            company_sponsored, pet_fee_required, source, source_drilldown,
            contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
            is_won, booking_type, time_to_won, backoffice_id, city, booking_date,
            last_update, margin_x_stay, rent_amount, sadmin_utilities, sadmin_final_cleaning,
            sadmin_pet_fee, b2c_entered_new_deal_stage,	b2c_entered_attempt_stage, b2c_entered_connected_stage,
            b2c_entered_proposal_stage,	b2c_entered_negotiation_stage,	b2b_entered_discovery_stage,
            b2b_entered_proposal_stage,	b2b_entered_negotiation_stage, kam_entered_enquiry_stage,
            kam_entered_proposal_stage, kam_entered_engaged_stage, kam_entered_block_stage, nr_of_bedrooms,
            lost_reason, lost_reason_dd
        )
        VALUES %s
        ON CONFLICT (deal_id) DO UPDATE SET
            deal_name = EXCLUDED.deal_name,
            create_date = EXCLUDED.create_date,
            close_date = EXCLUDED.close_date,
            pipeline = EXCLUDED.pipeline,
            deal_stage = EXCLUDED.deal_stage,
            deal_owner = EXCLUDED.deal_owner,
            billing_type = EXCLUDED.billing_type,
            guest_type = EXCLUDED.guest_type,
            check_in_date = EXCLUDED.check_in_date,
            check_out_date = EXCLUDED.check_out_date,
            lenght_of_stay = EXCLUDED.lenght_of_stay,
            budget = EXCLUDED.budget,
            apt_of_interest = EXCLUDED.apt_of_interest,
            apt_booked = EXCLUDED.apt_booked,
            neighborhood = EXCLUDED.neighborhood,
            purpose_of_rental = EXCLUDED.purpose_of_rental,
            company_sponsored = EXCLUDED.company_sponsored,
            pet_fee_required = EXCLUDED.pet_fee_required,
            source = EXCLUDED.source,
            source_drilldown = EXCLUDED.source_drilldown,
            contact_email = EXCLUDED.contact_email,
            contact_phone = EXCLUDED.contact_phone,
            timestamp = EXCLUDED.timestamp,
            inbound_outbound = EXCLUDED.inbound_outbound,
            time_to_close = EXCLUDED.time_to_close,
            is_won = EXCLUDED.is_won,
            booking_type = EXCLUDED.booking_type,
            time_to_won = EXCLUDED.time_to_won,
            backoffice_id = EXCLUDED.backoffice_id,
            city = EXCLUDED.city,
            booking_date = EXCLUDED.booking_date,
            last_update = EXCLUDED.last_update,
            margin_x_stay = EXCLUDED.margin_x_stay,
            rent_amount = EXCLUDED.rent_amount,
            sadmin_utilities = EXCLUDED.sadmin_utilities,
            sadmin_final_cleaning = EXCLUDED.sadmin_final_cleaning,
            sadmin_pet_fee = EXCLUDED.sadmin_pet_fee, 
            b2c_entered_new_deal_stage = EXCLUDED.b2c_entered_new_deal_stage,	
            b2c_entered_attempt_stage = EXCLUDED.b2c_entered_attempt_stage, 
            b2c_entered_connected_stage = EXCLUDED.b2c_entered_connected_stage,
            b2c_entered_proposal_stage = EXCLUDED.b2c_entered_proposal_stage,	
            b2c_entered_negotiation_stage = EXCLUDED.b2c_entered_negotiation_stage,	
            b2b_entered_discovery_stage = EXCLUDED.b2b_entered_discovery_stage,
            b2b_entered_proposal_stage = EXCLUDED.b2b_entered_proposal_stage,	
            b2b_entered_negotiation_stage = EXCLUDED.b2b_entered_negotiation_stage, 
            kam_entered_enquiry_stage = EXCLUDED.kam_entered_enquiry_stage,
            kam_entered_proposal_stage = EXCLUDED.kam_entered_proposal_stage, 
            kam_entered_engaged_stage = EXCLUDED.kam_entered_engaged_stage, 
            kam_entered_block_stage = EXCLUDED.kam_entered_block_stage,
            nr_of_bedrooms = EXCLUDED.nr_of_bedrooms,
            lost_reason = EXCLUDED.lost_reason,
            lost_reason_dd = EXCLUDED.lost_reason_dd

        ;
        ''',
        rows
    )

    # Confirmar la transacción
    conn_details.commit()

except ValueError as e:
    # Si hay algún error, realiza un rollback
    print(f"Error: {e}")
    conn_details.rollback()

finally:
    # Cierra el cursor y la conexión
    cursor.close()
    conn_details.close()


"""--------------------------------- FILTERED DATA (new_df) --------------------------------------------------"""

# # Iniciar transacción
# def update_deals_ddbb(ddbb):
#     start = time.perf_counter()
#     try:
#         # Iniciar transacción
#         conn_details.autocommit = False
#         cursor = conn_details.cursor()

#         # Recorre las filas del DataFrame y prepara los datos para la inserción o actualización
#         rows = []
#         for index, row in ddbb.iterrows():
#             deal_id = row["Deal ID"]
#             deal_name = escape_quotes(row["Deal Name"])
#             create_date = row["Create Date"] if row['Create Date'] else '1997-01-01'
#             close_date = row["Close Date"] if row['Close Date'] else '1997-01-01'
#             pipeline = row["Pipeline"]
#             deal_stage = row["Deal Stage"]
#             deal_owner = row["Deal Owner"]
#             billing_type = None if row["Billing Type"] == '' else row["Billing Type"]
#             guest_type = None if row["Guest Type"] == '' else row["Guest Type"]
#             check_in_date = row["Check in Date"] if row["Check in Date"] else '1997-01-01'
#             check_out_date = row["Check out Date"] if row["Check out Date"] else '1997-01-01'
#             lenght_of_stay = row["Lenght of Stay"] if row["Lenght of Stay"] else '0'
#             budget = row["Monthly Rent"]
#             apt_of_interest = row["Apartment of Interest"]
#             apt_booked = row["Apartment Booked"]
#             neighborhood = row["Neighborhood"]
#             purpose_of_rental = row["Purpose of Rental"]
#             company_sponsored = row["Company Sponsored"]
#             pet_fee_required = row["Pet Friendly Apt. Required"]
#             source = row["Deal Source"]
#             source_drilldown = row["Deal Source Drill Down"]
#             contact_email = row["Associated Contact Email"]
#             contact_phone = row["Associated Contact Phone"]
#             timestamp = row["Timestamp"]
#             inbound_outbound = row["b2b_source"]
#             time_to_close = None if row["time_to_close"] == '' else row["time_to_close"]
#             is_won = row["is_won"]
#             booking_type = None if row["booking_type"] == '' else row["booking_type"]
#             time_to_won = None if row["time_to_won"] == '' else row["time_to_won"]
#             backoffice_id = None if row["backoffice_id"] == '' else row["backoffice_id"]
#             city = None if row["city"] == '' else row["city"]
#             booking_date = row['booking_date'] if row['booking_date'] != '' else 'NULL'

#             # Verifica si el deal_id existe en la tabla hs_deals_q4
#             cursor.execute(f"SELECT * FROM hs_deals_q4 WHERE deal_id = {deal_id}")
#             existing_deal = cursor.fetchone()

#             if existing_deal:
#                 # Si existe, actualiza la fila
#                 rows.append((
#                     deal_id, deal_name, create_date, close_date, pipeline, deal_stage, deal_owner,
#                     billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
#                     budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
#                     company_sponsored, pet_fee_required, source, source_drilldown,
#                     contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
#                     is_won, booking_type, time_to_won, backoffice_id, city, booking_date
#                 ))
#                 print(f"Se actualizó la fila {index + 1} en la tabla hs_deals_q4. ID: {deal_id} \n {booking_date}")
#             else:
#                 # Si no existe, inserta una nueva fila
#                 rows.append((
#                     deal_id, deal_name, create_date, close_date, pipeline, deal_stage, deal_owner,
#                     billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
#                     budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
#                     company_sponsored, pet_fee_required, source, source_drilldown,
#                     contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
#                     is_won, booking_type, time_to_won, backoffice_id, city, booking_date
#                 ))
#                 print(f"Se insertó la fila {index + 1} en la tabla hs_deals_q4. ID: {deal_id} \n {booking_date}")

#         # Inserta o actualiza las filas usando execute_values
#         execute_values(
#             cursor,
#             '''
#             INSERT INTO hs_deals_q4 (
#                 deal_id, deal_name, create_date, close_date, pipeline, deal_stage, deal_owner,
#                 billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
#                 budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
#                 company_sponsored, pet_fee_required, source, source_drilldown,
#                 contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
#                 is_won, booking_type, time_to_won, backoffice_id, city, booking_date
#             )
#             VALUES %s
#             ON CONFLICT (deal_id) DO UPDATE SET
#                 deal_name = EXCLUDED.deal_name,
#                 create_date = EXCLUDED.create_date,
#                 close_date = EXCLUDED.close_date,
#                 pipeline = EXCLUDED.pipeline,
#                 deal_stage = EXCLUDED.deal_stage,
#                 deal_owner = EXCLUDED.deal_owner,
#                 billing_type = EXCLUDED.billing_type,
#                 guest_type = EXCLUDED.guest_type,
#                 check_in_date = EXCLUDED.check_in_date,
#                 check_out_date = EXCLUDED.check_out_date,
#                 lenght_of_stay = EXCLUDED.lenght_of_stay,
#                 budget = EXCLUDED.budget,
#                 apt_of_interest = EXCLUDED.apt_of_interest,
#                 apt_booked = EXCLUDED.apt_booked,
#                 neighborhood = EXCLUDED.neighborhood,
#                 purpose_of_rental = EXCLUDED.purpose_of_rental,
#                 company_sponsored = EXCLUDED.company_sponsored,
#                 pet_fee_required = EXCLUDED.pet_fee_required,
#                 source = EXCLUDED.source,
#                 source_drilldown = EXCLUDED.source_drilldown,
#                 contact_email = EXCLUDED.contact_email,
#                 contact_phone = EXCLUDED.contact_phone,
#                 timestamp = EXCLUDED.timestamp,
#                 inbound_outbound = EXCLUDED.inbound_outbound,
#                 time_to_close = EXCLUDED.time_to_close,
#                 is_won = EXCLUDED.is_won,
#                 booking_type = EXCLUDED.booking_type,
#                 time_to_won = EXCLUDED.time_to_won,
#                 backoffice_id = EXCLUDED.backoffice_id,
#                 city = EXCLUDED.city,
#                 booking_date = EXCLUDED.booking_date
#             ;
#             ''',
#             rows
#         )

#         # Confirmar la transacción
#         conn_details.commit()

#     except ValueError as e:
#         # Si hay algún error, realiza un rollback
#         print(f"Error: {e}")
#         conn_details.rollback()

#     finally:
#         # Cierra el cursor y la conexión
#         cursor.close()
#         conn_details.close()
    
#     end = time.perf_counter()
#     ttime = end - start
#     print(f'Tiempo total de ejecución: {ttime} segundos')



# end = time.perf_counter()
# ttime = end - start

# print(f'Tiempo total de ejecución: {ttime} segundos')





"""---------------------------------------------------------------------------------------------------------"""

# # Recorre las filas del DataFrame y ejecuta consultas INSERT o UPDATE
# for index, row in df_wks.iterrows():
#     deal_id = row["Deal ID"]
#     deal_name = escape_quotes(row["Deal Name"])
#     create_date = row["Create Date"] if row['Create Date'] else '1997-01-01'
#     close_date = row["Close Date"] if row['Close Date'] else '1997-01-01'
#     pipeline = row["Pipeline"]
#     deal_stage = row["Deal Stage"]
#     deal_owner = row["Deal Owner"]
#     billing_type = row["Billing Type"]
#     guest_type = row["Guest Type"]
#     check_in_date = row["Check in Date"] if row["Check in Date"] else '1997-01-01'
#     check_out_date = row["Check out Date"] if row["Check out Date"] else '1997-01-01'
#     lenght_of_stay = row["Lenght of Stay"] if row["Lenght of Stay"] else '0'
#     budget = row["Monthly Rent"]
#     apt_of_interest = row["Apartment of Interest"]
#     apt_booked = row["Apartment Booked"]
#     neighborhood = row["Neighborhood"]
#     purpose_of_rental = row["Purpose of Rental"]
#     company_sponsored = row["Company Sponsored"]
#     pet_fee_required = row["Pet Friendly Apt. Required"]
#     source = row["Deal Source"]
#     source_drilldown = row["Deal Source Drill Down"]
#     contact_email = row["Associated Contact Email"]
#     contact_phone = row["Associated Contact Phone"]
#     timestamp = row["Timestamp"]
#     inbound_outbound = row["b2b_source"]
#     time_to_close = None if row["time_to_close"] == '' else row["time_to_close"]
#     is_won = row["is_won"]
#     booking_type = None if row["booking_type"] == '' else row["booking_type"]
#     time_to_won = None if row["time_to_won"] == '' else row["time_to_won"]
#     backoffice_id = None if row["backoffice_id"] == '' else row["backoffice_id"]
#     city = None if row["city"] == '' else row["city"]


#     # Verifica si el deal_id existe en la tabla hs_deals_q4
#     cursor.execute(f"SELECT * FROM hs_deals_q4 WHERE deal_id = {deal_id}")
#     existing_deal = cursor.fetchone()
#     if existing_deal:
#         # Si existe, actualiza la fila
#         update_query = '''
#             UPDATE hs_deals_q4
#             SET deal_name = %s,
#                 create_date = %s,
#                 close_date = %s,
#                 pipeline = %s,
#                 deal_stage = %s,
#                 deal_owner = %s,
#                 billing_type = %s,
#                 guest_type = %s,
#                 check_in_date = %s,
#                 check_out_date = %s,
#                 lenght_of_stay = %s,
#                 budget = %s,
#                 apt_of_interest = %s,
#                 apt_booked = %s,
#                 neighborhood = %s,
#                 purpose_of_rental = %s,
#                 company_sponsored = %s,
#                 pet_fee_required = %s,
#                 source = %s,
#                 source_drilldown = %s,
#                 contact_email = %s,
#                 contact_phone = %s,
#                 timestamp = %s,
#                 inbound_outbound = %s,
#                 time_to_close = %s,
#                 is_won = %s,
#                 booking_type = %s,
#                 backoffice_id = %s,
#                 city = %s
#             WHERE deal_id = %s;
#         '''
#         cursor.execute(update_query, (deal_name, create_date, close_date, pipeline, deal_stage, deal_owner,
#                                       billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
#                                       budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
#                                       company_sponsored, pet_fee_required, source, source_drilldown,
#                                       contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
#                                       is_won, booking_type, backoffice_id, city, deal_id))
#         conn_details.commit()
#         print(f"Se actualizó la fila {index + 1} en la tabla hs_deals_q4. ID: {deal_id}")
#     else:
#         # Si no existe, inserta una nueva fila
#         insert_query = '''
#             INSERT INTO hs_deals_q4 (deal_id, deal_name, create_date, close_date, pipeline, deal_stage,
#                                      billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
#                                      budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
#                                      company_sponsored, pet_fee_required, source, source_drilldown,
#                                      contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
#                                      is_won, booking_type, time_to_won, backoffice_id, city)
#             VALUES (%s, %s, %s, %s, %s, %s,
#                     %s, %s, %s, %s, %s, %s,
#                     %s, %s, %s, %s, %s, %s,
#                     %s, %s, %s, %s, %s, %s,
#                     %s, %s, %s, %s, %s);
#         '''
#         cursor.execute(insert_query, (deal_id, deal_name, create_date, close_date, pipeline, deal_stage,
#                                       billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
#                                       budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
#                                       company_sponsored, pet_fee_required, source, source_drilldown,
#                                       contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
#                                       is_won, booking_type, time_to_won, backoffice_id, city))
#         conn_details.commit()
#         print(f"Se insertó la fila {index + 1} en la tabla hs_deals_q4. ID: {deal_id}")

# # Cierra el cursor y la conexión
# cursor.close()
# conn_details.close()

# # Conectar a la base de datos
# conn_details = psycopg2.connect(
#     host=db_host,
#     database=db_database,
#     user=db_user,
#     password=db_password,
#     port=5432
# )

# cursor = conn_details.cursor()