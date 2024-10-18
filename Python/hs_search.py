import hubspot
from pprint import pprint
from hubspot.crm.deals import PublicObjectSearchRequest, ApiException
# from hubspot.crm.contacts import PublicObjectSearchRequest, ApiException

from dateutil import parser
import pandas as pd
# from concurrent.futures import ThreadPoolExecutor
import time

import psycopg2
from psycopg2.extras import execute_values

from functions import escape_quotes
from credentials import hs_sb_key, hs_prod_key, db_database, db_host, db_password, db_user, openai_key

# from pandasai import SmartDataframe
# from pandasai.llm import OpenAI


def search_deals(hs_key, st_date, en_date):
    st = time.perf_counter()
    api_client = hubspot.Client.create(access_token=hs_key)

    # Timestamp en milisegundos de la fecha proporcionada
    start = str(int(parser.isoparse(f"{st_date}T00:00:00.000Z").timestamp() * 1000))
    end = str(int(parser.isoparse(f"{en_date}T00:00:00.000Z").timestamp() * 1000))

    properties=[
            'hs_object_id', 'dealname', 'createdate', 'closedate', 'hs_lastmodifieddate', 'pipeline', 'dealstage', 'hubspot_owner_id', 'billing_type',
                'deal___guest_type', 'check_in_date', 'check_out_date', 'length_of_stay_in_days_test', 'monthly_budget__temp_', 'apartment_of_interest___list',
                'apartment_booked___list', 'deal___neighborhood', 'deal___purpose_of_rental', 'company_sponsored__new_', 'deal____pet_friendly__apt__required',
                'deal_contacted_through__new_', 'deal_contacted_through_drilldown_1__new_', 'associated_contact_email', 'associated_contact_phone_nr',
                'spreadsheet_timestamp', 'b2b_source', 'time_from_creation_to_booking', 'timetoclose', 'hs_is_closed_won', 'booking_type', 'backoffice_id',
                'booking_city', 'booking___stage_enrollment_date', 'total_margin_x_stay', 'sadmin___rent_amount', 'sadmin___utilities', 
                'sadmin___final_cleaning', 'sadmin___pet_fee', 'notes_last_updated', 'last_activity_date___associated_contact'
        ]

    # Lista para almacenar todos los resultados
    all_results = []

    # Objeto de búsqueda para filtrar oportunidades creadas después de la fecha dada
    public_object_search_request = PublicObjectSearchRequest(
        properties= properties,
        filter_groups=[
            {
                "filters": [
                    {
                        "propertyName": "createdate",
                        "operator": "BETWEEN",
                        "highValue": end,
                        "value": start      
                    },
                    {
                        "values": [95422169, 135831272, 303370699],
                        # "values": [95422169],
                        "propertyName": "pipeline",
                        "operator": "IN"
                    }
                ]
            }
        ],
        limit=100,
        after=None  # Cambiado de offset=offset a after=None
    )


    try:
        # Bucle para recuperar todas las páginas de resultados
        while True:
            # Ejecutar la búsqueda de oportunidades
            api_response = api_client.crm.deals.search_api.do_search(public_object_search_request=public_object_search_request)
            results = api_response.results
            all_results.extend(results)
            
            # Si hay más páginas, actualizar el objeto de búsqueda con el after
            if api_response.paging and api_response.paging.next:
                public_object_search_request.after = api_response.paging.next.after
            else:
                break  # Si no hay más páginas, salir del bucle

        # Convertir todos los resultados en un DataFrame
        df = pd.DataFrame(all_results)
        # Imprimir los resultados o realizar cualquier otro procesamiento necesario
        # print(df)

    except ApiException as e:
        print("Exception when calling search_api->do_search: %s\n" % e)


    # Crear DataFrame
    properties_list = []
    for row in df[0]:
        properties_list.append(row.properties)

    results = pd.DataFrame(properties_list)

    # pd.set_option('display.max_columns', None)
    results = results[properties]
    en = time.perf_counter()
    tt = en - st
    print(f'Total time = {tt}' )
    return results

oct_nov = search_deals(hs_prod_key, '2023-10-01', '2024-04-01')
print(oct_nov)

dec_jan = search_deals(hs_prod_key, '2023-12-01', '2024-02-01')
# print(dec_jan)

feb_mar = search_deals(hs_prod_key, '2024-02-01', '2024-04-30')
# print(feb_mar)

df = pd.concat([oct_nov, dec_jan, feb_mar], axis= 0)
  

df['conversion_rate'] = df['booking___stage_enrollment_date'].apply(lambda x: 1 if pd.notna(x) else 0)


""" --------------------------------------------- DDBB ----------------------------------------------------"""

# Conectar a la base de datos
conn_details = psycopg2.connect(
    host=db_host,
    database=db_database,
    user=db_user,
    password=db_password,
    port=5432
)

cursor = conn_details.cursor()

# Iniciar transacción
def update_deals_ddbb(ddbb):
    start = time.perf_counter()
    try:
        # Iniciar transacción
        conn_details.autocommit = False
        cursor = conn_details.cursor()

        # Borrar todas las filas existentes en la tabla
        cursor.execute("TRUNCATE TABLE hs_deals_q4_tests")

        # Recorre las filas del DataFrame y prepara los datos para la inserción o actualización
        rows = []
        for index, row in ddbb.iterrows():
            deal_id = row["hs_object_id"]
            deal_name = escape_quotes(row["dealname"])
            create_date = None if row["createdate"] == '' else row["createdate"]
            close_date = None if row["closedate"] == '' else row["closedate"]
            last_update = row['hs_lastmodifieddate'] if row['hs_lastmodifieddate'] != '' else 'NULL'
            pipeline = row["pipeline"]
            deal_stage = row["dealstage"]
            deal_owner = None if row["hubspot_owner_id"] == '' else row["hubspot_owner_id"]
            billing_type = None if row["billing_type"] == '' else row["billing_type"]
            guest_type = None if row["deal___guest_type"] == '' else row["deal___guest_type"]
            check_in_date = None if row["check_in_date"] == '' else row["check_in_date"]
            check_out_date = None if row["check_out_date"] == '' else row["check_out_date"]
            lenght_of_stay = None if row["length_of_stay_in_days_test"] == '' else row["length_of_stay_in_days_test"]
            budget = row["monthly_budget__temp_"]
            apt_of_interest = row["apartment_of_interest___list"]
            apt_booked = row["apartment_booked___list"]
            neighborhood = row["deal___neighborhood"]
            purpose_of_rental = row["deal___purpose_of_rental"]
            company_sponsored = row["company_sponsored__new_"]
            pet_fee_required = row["deal____pet_friendly__apt__required"]
            source = row["deal_contacted_through__new_"]
            source_drilldown = row["deal_contacted_through_drilldown_1__new_"]
            contact_email = row["associated_contact_email"]
            contact_phone = row["associated_contact_phone_nr"]
            timestamp = row["spreadsheet_timestamp"]
            inbound_outbound = row["b2b_source"]
            time_to_close = None if row["timetoclose"] == '' else row["timetoclose"]
            is_won = row["hs_is_closed_won"]
            booking_type = None if row["booking_type"] == '' else row["booking_type"]
            time_to_won = None if row["time_from_creation_to_booking"] == '' else row["time_from_creation_to_booking"]
            backoffice_id = None if row["backoffice_id"] == '' else row["backoffice_id"]
            city = None if row["booking_city"] == '' else row["booking_city"]
            booking_date = None if row["booking___stage_enrollment_date"] == '' else row["booking___stage_enrollment_date"]
            margin_x_stay = None if row["total_margin_x_stay"] == '' else row["total_margin_x_stay"]
            rent_amount = None if row["sadmin___rent_amount"] == '' else row["sadmin___rent_amount"]
            sadmin_utilities = None if row["sadmin___utilities"] == '' else row["sadmin___utilities"]
            sadmin_final_cleaning = None if row["sadmin___final_cleaning"] == '' else row["sadmin___final_cleaning"]
            sadmin_pet_fee = None if row["sadmin___pet_fee"] == '' else row["sadmin___pet_fee"]
            notes_last_updated = None if row["notes_last_updated"] == '' else row["notes_last_updated"]
            last_activity_date___associated_contact = None if row["last_activity_date___associated_contact"] == '' else row["last_activity_date___associated_contact"]
            conversion_rate = row['conversion_rate']


                        # Agregar los datos a la lista de filas
            rows.append((
                deal_id, deal_name, create_date, close_date, last_update, pipeline, deal_stage, 
                deal_owner, billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
                budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
                company_sponsored, pet_fee_required, source, source_drilldown,
                contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
                is_won, booking_type, time_to_won, backoffice_id, city, booking_date,
                margin_x_stay, rent_amount, sadmin_utilities, sadmin_final_cleaning, sadmin_pet_fee, notes_last_updated,
                last_activity_date___associated_contact, conversion_rate
            ))
            print(f"Se insertó la fila {index + 1} en la tabla hs_deals_q4_tests. ID: {deal_id} \n {conversion_rate}")

        # Insertar todas las filas usando execute_values
        execute_values(
            cursor,
            '''
            INSERT INTO hs_deals_q4_tests (
                deal_id, deal_name, create_date, close_date, last_update, pipeline, deal_stage, 
                deal_owner, billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
                budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
                company_sponsored, pet_fee_required, source, source_drilldown,
                contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
                is_won, booking_type, time_to_won, backoffice_id, city, booking_date,
                margin_x_stay, rent_amount, sadmin_utilities, sadmin_final_cleaning, sadmin_pet_fee, notes_last_updated,
                last_activity_date___associated_contact, conversion_rate
            )
            VALUES %s;
            ''',
            rows
        )

        # Confirmar la transacción
        conn_details.commit()

    except Exception as e:
        # Si hay algún error, realiza un rollback
        print(f"Error: {e}")
        conn_details.rollback()

    finally:
        # Cierra el cursor y la conexión
        cursor.close()
        conn_details.close()
    
    end = time.perf_counter()
    ttime = end - start
    print(f'Tiempo total de ejecución: {ttime} segundos')



# update_deals_ddbb(df)