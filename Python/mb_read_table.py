import psycopg2
import requests
from metabase import headers, mb_url_card, mb_url
import pandas as pd
from datetime import datetime
import pytz

from credentials import db_database, db_host, db_password, db_user

def escape_quotes(value):
    """
    Escapa el símbolo de comilla simple en una cadena de texto.
    """
    return value.replace("'", "''") if isinstance(value, str) else value

# Define una función para formatear las fechas en el formato adecuado para PostgreSQL
def format_date(date_str):
    if date_str:
        dt = datetime.fromisoformat(date_str)
        dt_utc = dt.astimezone(pytz.utc)
        return dt_utc.strftime("%Y-%m-%d %H:%M:%S %z")
    else:
        return None


# Find public questions
response = requests.get(mb_url_card,
                         headers=headers).json()
questions = [q for q in response if q['public_uuid']]
# print(f'{len(questions)} public of {len(response)} questions')



# # Get the required question
# uuid = questions[0]['public_uuid']
# response = requests.get(f'http://prod-metabase-lb-834391402.eu-central-1.elb.amazonaws.com/api/public/card/{uuid}',
#                         headers=headers)
# print(f'table 0: {response.json()["name"]}')

# # Get the required question
# uuid = questions[1]['public_uuid']
# response = requests.get(f'http://prod-metabase-lb-834391402.eu-central-1.elb.amazonaws.com/api/public/card/{uuid}',
#                         headers=headers)
# print(f'table 1: {response.json()["name"]}')

# # Get the required question
# uuid = questions[2]['public_uuid']
# response = requests.get(f'http://prod-metabase-lb-834391402.eu-central-1.elb.amazonaws.com/api/public/card/{uuid}',
#                         headers=headers)
# print(f'table 2: {response.json()["name"]}')

# # Get the required question
# uuid = questions[3]['public_uuid']
# response = requests.get(f'http://prod-metabase-lb-834391402.eu-central-1.elb.amazonaws.com/api/public/card/{uuid}',
#                         headers=headers)
# print(f'table 3: {response.json()["name"]}')

# # Get the required question
# uuid = questions[4]['public_uuid']
# response = requests.get(f'http://prod-metabase-lb-834391402.eu-central-1.elb.amazonaws.com/api/public/card/{uuid}',
#                         headers=headers)
# print(f'table 4: {response.json()["name"]}')

"""
table 0: BackOffice Data - Q4
table 1: Bookings
table 2: Bookings + Guests + Apartments + Events
table 3: Hs Deals Q4
table 4: Hs Deals Q4 | "Backoffice ID" is known
"""


# Get Backoffice data
uuid_1 = questions[0]['public_uuid']
response = requests.get(f'{mb_url}/api/public/card/{uuid_1}/query',
                        headers=headers)
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

# print('BackOffice DDBB')
# # # pd.set_option('display.max_columns', None)
# print(bo_db.info())

# Get HubSpot data
uuid_2 = questions[4]['public_uuid']
response = requests.get(f'http://prod-metabase-lb-834391402.eu-central-1.elb.amazonaws.com/api/public/card/{uuid_2}/query',
                        headers=headers)
rows = response.json()['data']['rows']
hs_db = pd.DataFrame(rows, columns = [
    'Deal ID','Deal Name','Create Date','Close Date','Deal Stage','Deal Owner','Billing Type','Guest Type',
    'Check In Date','Check Out Date','Lenght Of Stay','Budget','Apt Of Interest','Apt Booked','Neighborhood','Purpose Of Rental',
    'Company Sponsored','Pet Fee Required','Source','Source Drilldown','Contact Email','Contact Phone','Timestamp','Pipeline',
    'Inbound Outbound','Time To Close','Is Won','Booking Type','Time To Won','Backoffice ID', 'city', 'booking_date', 'last_update',
    'margin_x_stay', 'rent_amount', 'sadmin_utilities', 'sadmin_pet_fee', 'sadmin_final_cleaning'
])

# hs_db["Backoffice ID"] = hs_db["Backoffice ID"].astype(int)
# hs_db["Backoffice ID"] = hs_db["Backoffice ID"].astype(object)

# # Filtra las filas donde 'Backoffice ID' no sea 0
# hs_db = hs_db[hs_db['Backoffice ID'] > 0].copy()

# print('HubSpot DDBB')
# # # Configurar pandas para mostrar todas las columnas
# # # pd.set_option('display.max_columns', None)
print(hs_db.info())

# # Asigna las columnas de interés a nuevas variables para mayor claridad
# bo_id_column = 'ID'
# hs_boid_column = 'Backoffice ID'

# # Asegúrate de que las columnas de correo electrónico no tengan valores nulos
# bo_df = bo_db.dropna(subset=[bo_id_column])
# hs_df = hs_db.dropna(subset=[hs_boid_column])

# print("Valores únicos en 'ID' de df_backoffice:", sorted(bo_db['ID'].unique()))
# print("Valores únicos en 'Backoffice ID' de df_hubspot:", sorted(hs_db['Backoffice ID'].unique()))

# Realiza la combinación de los DataFrames en función de las columnas de correo electrónico
merged_df = pd.merge(bo_db, hs_db, left_on='ID', right_on='Backoffice ID', how='inner')

# Imprime el nuevo DataFrame combinado
# print(merged_df)

# Selecciona las columnas deseadas de merged_df
selected_columns = [
    'ID','Check In','Check Out','Apartment ID','Apartments → Codename','Guest ID','Guests → Name','Guests → Email','Inserted At','Updated At','Monthly Price','Nights',
    'Country','State','Pets','Deal ID','Deal Name','Contact Email','Create Date','booking_date','Deal Stage','Deal Owner',
    'Check In Date','Check Out Date','Lenght Of Stay','Budget','Apt Of Interest','Apt Booked', 'Backoffice ID','Booking Events → Data → Booking From ID',
    'Booking Events → Data → Booking To ID','Booking Events → Data → New Check Out','Booking Events → Data → Old Check Out',
    'Booking Events → ID','Booking Events → Name','Booking Events → Booking ID','Booking Events → Inserted At','Booking Events → Updated At']

# Crea el nuevo DataFrame final_df con las columnas seleccionadas
final_df = merged_df[selected_columns]
final_df = final_df.fillna('')

# Aplica la función a las columnas correspondientes
final_df['Booking Events → Data → New Check Out'] = final_df['Booking Events → Data → New Check Out'].apply(format_date)
final_df['Booking Events → Data → Old Check Out'] = final_df['Booking Events → Data → Old Check Out'].apply(format_date)
final_df["Booking Events → Inserted At"] = final_df["Booking Events → Inserted At"].apply(format_date)
final_df["Booking Events → Updated At"] = final_df["Booking Events → Updated At"].apply(format_date)

final_df['Booking Events → Data → New Check Out'].fillna('', inplace=True)
final_df['Booking Events → Data → Old Check Out'].fillna('', inplace=True)
final_df["Booking Events → Inserted At"].fillna('', inplace=True)
final_df["Booking Events → Updated At"].fillna('', inplace=True)


# # Configurar pandas para mostrar todas las columnas
# pd.set_option('display.max_columns', None)

# Imprime el nuevo DataFrame final_df
# pd.set_option('display.max_columns', None)

# filtered_columns = [
#     'Booking Events → Data → New Check Out','Booking Events → Data → Old Check Out','Booking Events → Inserted At','Booking Events → Updated At']

# print(final_df[filtered_columns])
# print(final_df.iloc[[9, 10, 11], :])




# Conectar a la base de datos
conn_details = psycopg2.connect(
    host=db_host,
    database=db_database,
    user=db_user,
    password=db_password,
    port=5432
)

cursor = conn_details.cursor()

# Recorre las filas del DataFrame y ejecuta consultas INSERT o UPDATE
for index, row in final_df.iterrows():
    booking_id = row['ID']
    bo_checkin = row['Check In']
    bo_checkout = row['Check Out']
    bo_apt_id = row ['Apartment ID']
    bo_guest_id = row['Guest ID']
    bo_create_date = row['Inserted At']
    bo_update_date = row['Updated At']
    bo_monthly_rent = 'NULL' if row['Monthly Price'] == '' else row['Monthly Price']
    bo_los = row['Nights']
    bo_country = row['Country']
    bo_state = row['State']
    bo_pet = row['Pets']
    deal_id = row['Deal ID']
    deal_name = row['Deal Name']
    create_date = row['Create Date'] if row['Create Date'] else '1997-01-01'
    close_date = row['booking_date'] if row['booking_date'] else '1997-01-01'
    deal_stage = row['Deal Stage']
    deal_owner = row['Deal Owner']
    check_in_date = row['Check In Date'] if row['Check In Date'] else '1997-01-01'
    check_out_date = row['Check Out Date'] if row['Check Out Date'] else '1997-01-01'
    lenght_of_stay = row['Lenght Of Stay'] if row['Lenght Of Stay'] else '0'
    budget = 'NULL' if row['Budget'] == '' else row['Budget']
    apt_of_interest = row['Apt Of Interest']
    apt_booked = row['Apt Booked']
    bo_apt_name = row['Apartments → Codename']
    bo_guest_name = row['Guests → Name']
    bo_guest_email = '' if row['Guests → Email'] == '' else row['Guests → Email']
    hs_deal_email = 'NULL' if row ['Contact Email'] == '' else row ['Contact Email']
    backoffice_id = '' if row['Backoffice ID'] == '' else row['Backoffice ID']
    boevent_from_bo_id = 'NULL' if row['Booking Events → Data → Booking From ID'] == '' else row['Booking Events → Data → Booking From ID']
    boevent_to_bo_id = 'NULL' if row['Booking Events → Data → Booking To ID'] == '' else row['Booking Events → Data → Booking To ID']
    boevent_new_checkout = "'"+row['Booking Events → Data → New Check Out']+"'" if row['Booking Events → Data → New Check Out'] != '' else 'NULL'
    boevent_old_checkout = "'"+row['Booking Events → Data → Old Check Out']+"'" if row['Booking Events → Data → Old Check Out'] != '' else 'NULL'
    boevent_bo_id  = 'NULL' if row['Booking Events → ID'] == '' else row['Booking Events → ID']
    boevent_name = row['Booking Events → Name']
    boevent_create_date = "'"+row['Booking Events → Inserted At']+"'" if row['Booking Events → Inserted At'] != '' else 'NULL'
    boevent_update_date = "'"+row['Booking Events → Updated At']+"'" if row['Booking Events → Updated At'] != '' else 'NULL'

    # Verifica si el deal_id existe en la tabla hs_deals_q4
    cursor.execute(f"SELECT * FROM bo_hs_merge_test WHERE deal_id = {deal_id}")
    existing_deal = cursor.fetchone()

    if existing_deal:
        # Si existe, actualiza la fila
        update_query = f'''
            UPDATE bo_hs_merge_test
            SET bo_checkin = '{bo_checkin}',
                bo_checkout = '{bo_checkout}',
                bo_apt_id = {bo_apt_id},
                bo_guest_id = {bo_guest_id},
                bo_create_date = '{bo_create_date}',
                bo_update_date = '{bo_update_date}',
                bo_monthly_rent = {bo_monthly_rent},
                bo_los = {bo_los},
                bo_country = '{bo_country}',
                bo_state = '{bo_state}',
                bo_pet = {bo_pet},
                deal_id = {deal_id},
                hs_dealname = '{deal_name}',
                hs_create_date = '{create_date}',
                hs_close_date = '{close_date}',
                hs_stage = '{deal_stage}',
                hs_owner = '{deal_owner}',
                hs_checkin = '{check_in_date}',
                hs_checkout = '{check_out_date}',
                hs_los = {lenght_of_stay},
                hs_budget = '{budget}',
                hs_apt_interest = '{apt_of_interest}',
                hs_apt_booked = '{apt_booked}',
                bo_apt_name = '{bo_apt_name}',
                bo_guest_name = '{bo_guest_name}',
                bo_guest_email = '{bo_guest_email}',
                hs_deal_email = '{hs_deal_email}',
                hs_backoffice_id = {backoffice_id},
                boevent_from_bo_id = {boevent_from_bo_id},
                boevent_to_bo_id = {boevent_to_bo_id},
                boevent_new_checkout = {boevent_new_checkout},
                boevent_old_checkout = {boevent_old_checkout},
                boevent_bo_id = {boevent_bo_id},
                boevent_create_date = {boevent_create_date}::TIMESTAMPTZ,
                boevent_update_date = {boevent_update_date}::TIMESTAMPTZ
            WHERE booking_id = {booking_id};
        '''
        cursor.execute(update_query)
        conn_details.commit()
        print(f"Se actualizó la fila {index + 1} en la tabla bo_hs_merge_test. ID: {booking_id}")
    else:
        # Si no existe, inserta una nueva fila
        insert_query = f'''
            INSERT INTO bo_hs_merge_test (booking_id, bo_checkin, bo_checkout, bo_apt_id, bo_guest_id, bo_create_date, bo_update_date, bo_monthly_rent, bo_los, bo_country,
                bo_state, bo_pet, deal_id, hs_dealname, hs_create_date, hs_close_date, hs_stage, hs_owner, hs_checkin, hs_checkout, hs_los, hs_budget, hs_apt_interest, hs_apt_booked,
                bo_apt_name, bo_guest_name, bo_guest_email, hs_backoffice_id, boevent_from_bo_id, boevent_to_bo_id, boevent_new_checkout, boevent_old_checkout, boevent_bo_id, 
                boevent_name, boevent_create_date, boevent_update_date)
            VALUES ({booking_id}, '{bo_checkin}', '{bo_checkout}', {bo_apt_id}, {bo_guest_id}, '{bo_create_date}', '{bo_update_date}', 
                {bo_monthly_rent}, {bo_los}, '{bo_country}', '{bo_state}', {bo_pet}, {deal_id}, '{deal_name}', '{create_date}', 
                '{close_date}', '{deal_stage}', '{deal_owner}', '{check_in_date}', '{check_out_date}', {lenght_of_stay}, '{budget}', 
                '{apt_of_interest}', '{apt_booked}', '{bo_apt_name}', '{bo_guest_name}', '{bo_guest_email}', {backoffice_id},
                {boevent_from_bo_id}, {boevent_to_bo_id}, {boevent_new_checkout}::TIMESTAMPTZ, {boevent_old_checkout}::TIMESTAMPTZ, {boevent_bo_id},
                '{boevent_name}', {boevent_create_date}::TIMESTAMPTZ, {boevent_update_date}::TIMESTAMPTZ);
        '''
        # print("Consulta SQL para INSERT:")
        # print(insert_query)
        cursor.execute(insert_query)
        conn_details.commit()
        print(f"Se insertó la fila {index + 1} en la tabla bo_hs_merge_test. ID: {booking_id}")

cursor.close()
conn_details.close()