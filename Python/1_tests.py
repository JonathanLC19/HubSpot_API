from datetime import datetime, timedelta
import pandas as pd
import time 
import numpy as np
import psycopg2
from psycopg2.extras import execute_values

import hubspot
from hubspot.crm.deals import PublicObjectSearchRequest, ApiException

# from creds2 import hs_test, host, database, user, passw, port
from credentials import hs_prod_key, db_database, db_host, db_password, db_user


from functions import HubspotDealsFetcher, escape_quotes, eliminate_point_nr
from decorators import memoize_decorator
import schedule


# from pandasai import PandasAI, SmartDataframe
#  from pandasai.llm.openai import OpenAI



# # Obtén la fecha actual y su día de la semana
# today = datetime.now()
# current_weekday = today.weekday()

# tomorrow = timedelta(current_weekday + 1)

# # Calcula el inicio y fin de la semana pasada
# start_of_last_week = today - timedelta(days=current_weekday + 7)
# end_of_last_week = start_of_last_week + timedelta(days=6)

# # print(f'{today}\n{current_weekday}')
# # print(f'{start_of_last_week}\n{end_of_last_week}')
# # print(tomorrow)

""" ------------------------- MILISEG A SEG ---------------------------- """

# # Crear un DataFrame de ejemplo
# data = {'tiempo1': [8121600000, 1209600000, 2678400000],
#         'tiempo2': [13219200000, 345600000, 2678400000],
#         'otra_columna': [10, 20, 30]}
# df = pd.DataFrame(data)

# # Iterar sobre todas las columnas del DataFrame
# for col in df.columns:
#     # Verificar si la columna contiene milisegundos
#     if 'tiempo' in col:
#         # Convertir milisegundos a segundos
#         df[col] = df[col] / 86400000

# # Mostrar el DataFrame resultante
# print(df)


""" ------------------------- VECTORIZATION VS "FOR" LOOP ---------------------------- """

# start = time.perf_counter()
# # iterative sum
# total = 0
# # iterating through 1.5 Million numbers
# for item in range(0, 1500000):
#     total = total + item

# print('sum is:' + str(total))
# end = time.perf_counter()
# print(end - start)


# start = time.perf_counter()
# # vectorized sum - using numpy for vectorization
# # np.arange create the sequence of numbers from 0 to 1499999
# print(np.sum(np.arange(1500000)))
# end = time.perf_counter()
# print(end - start)


""" ------------------------- PANDASAI ---------------------------- """

# class MyPandasAI(PandasAI):
#     def chat(self, df, prompt):
#         # Get the answer from the OpenAI API
#         answer = self.llm.chat(prompt)
#         # Return the answer
#         return answer

# OPENAI_API_KEY = ""
# llm = OpenAI(api_token=OPENAI_API_KEY)
# pandas_ai = MyPandasAI(llm)

# data_dict = {
#     "country": ["Delhi", "Mumbai", "Kolkata", "Chennai", "Jaipur", "Lucknow", "Pune", "Bengaluru", "Amritsar", "Agra", "Kola"],
#     "annual tax collected": [19294482072, 28916155672, 24112550372, 34358173362, 17454337886, 11812051350, 16074023894, 14909678554, 43807565410, 146318441864, np.nan],
#     "happiness_index": [9.94, 7.16, 6.35, 8.07, 6.98, 6.1, 4.23, 8.22, 6.87, 3.36, np.nan],
# }
# df = pd.DataFrame(data_dict)

# response = pandas_ai(df, "Show the first 5 rows of data in tabular form")
# print(response)

# df = pd.DataFrame({
#     "country": [
#         "United States", "United Kingdom", "France", "Germany", "Italy", "Spain", "Canada", "Australia", "Japan", "China"],
#     "gdp": [
#         19294482071552, 2891615567872, 2411255037952, 3435817336832, 1745433788416, 1181205135360, 1607402389504, 1490967855104, 4380756541440, 14631844184064
#     ],
# })

# df = SmartDataframe(df)
# df.chat('Which are the countries with GDP greater than 3000000000000?')


# """ ------------------------- FUNCTOOLS CACHE ---------------------------- """

# from functools import cache

# @cache
# def fib(n):
#     if n <= 1:
#         return n
#     return fib(n - 1) + fib(n - 2)


# def main(test_times=50):
#     start = time.time()
#     for _ in range(test_times):
#         fib(30)
#     print(f"Total time spent: {time.time() - start} s")


# main()
""" ------------------------- DUPLICATES ---------------------------- """

# import pandas as pd

# # Ejemplo de DataFrame
# data = {'Columna1': [1, 2, 3, 4, 1, 2],
#         'Columna2': ['A', 'B', 'C', 'D', 'A', 'B']}
# df = pd.DataFrame(data)

# # Encontrar duplicados
# duplicados = df[df.duplicated(keep=False)]

# # Crear un nuevo DataFrame con filas duplicadas
# df_duplicados = df[df.isin(duplicados)].dropna()

# # Mostrar el nuevo DataFrame con filas duplicadas
# print("Nuevo DataFrame con filas duplicadas:")
# print(df_duplicados)


""" ------------------------------------- DEALS DDBB ---------------------------------------"""

# props = ['hs_object_id', 'dealname', 'pipeline']

# df = get_all_deals_df(hs_test, props)

# import psycopg2

# # Conectar a la base de datos
# conn_details = psycopg2.connect(
#     host=host,
#     database=database,
#     user=user,
#     password=passw,
#     port=5432
# )

# cursor = conn_details.cursor()

# # Recorre las filas del DataFrame y ejecuta consultas INSERT o UPDATE
# for index, row in df.iterrows():
#     createdate = row['createdate']
#     dealname = escape_quotes(row['dealname'])
#     hs_lastmodifieddate = row["hs_lastmodifieddate"]
#     hs_object_id = row["hs_object_id"]
#     pipeline = row['pipeline']

#     # Verifica si el deal_id existe en la tabla hs_deals_q4
#     cursor.execute(f"SELECT * FROM tests WHERE hs_object_id = {hs_object_id}")
#     existing_deal = cursor.fetchone()

#     if existing_deal:
#         # Si existe, actualiza la fila
#         update_query = f'''
#             UPDATE tests
#             SET createdate = '{createdate}'::TIMESTAMPTZ,
#                 dealname = '{dealname}',
#                 hs_lastmodifieddate = '{hs_lastmodifieddate}'::TIMESTAMPTZ,
#                 hs_object_id = '{hs_object_id}',
#                 pipeline = '{pipeline}'
#             WHERE hs_object_id = {hs_object_id};
#         '''
#         cursor.execute(update_query)
#         conn_details.commit()
#         print(f"Se actualizó la fila {index + 1} en la tabla tests. ID: {hs_object_id}")
#     else:
#         # Si no existe, inserta una nueva fila
#         insert_query = f'''
#             INSERT INTO tests (createdate, dealname, hs_lastmodifieddate, hs_object_id, pipeline)
#             VALUES ('{createdate}'::TIMESTAMPTZ, '{dealname}', '{hs_lastmodifieddate}'::TIMESTAMPTZ, '{hs_object_id}', '{pipeline}');
#         '''
#         cursor.execute(insert_query)
#         conn_details.commit()
#         print(f"Se insertó la fila {index + 1} en la tabla tests. ID: {hs_object_id}")

# cursor.close()
# conn_details.close()


"""----------------------------- ddbb object ---------------------------------"""

# props = ['hs_object_id', 'dealname', 'pipeline']

# df = get_all_deals_df(hs_test, props)

# class ProcesadorDataFrame:
#     def __init__(self, conn_details):
#         self.conn_details = conn_details

#     def procesar_dataframe(self, df):
#         cursor = self.conn_details.cursor()

#         for index, row in df.iterrows():
#             createdate = row['createdate']
#             dealname = escape_quotes(row['dealname'])
#             hs_lastmodifieddate = row["hs_lastmodifieddate"]
#             hs_object_id = row["hs_object_id"]
#             pipeline = row['pipeline']

#             # Verifica si el deal_id existe en la tabla hs_deals_q4
#             cursor.execute(f"SELECT * FROM tests WHERE hs_object_id = {hs_object_id}")
#             existing_deal = cursor.fetchone()

#             if existing_deal:
#                 # Si existe, actualiza la fila
#                 update_query = f'''
#                     UPDATE tests
#                     SET createdate = '{createdate}'::TIMESTAMPTZ,
#                         dealname = '{dealname}',
#                         hs_lastmodifieddate = '{hs_lastmodifieddate}'::TIMESTAMPTZ,
#                         hs_object_id = '{hs_object_id}',
#                         pipeline = '{pipeline}'
#                     WHERE hs_object_id = {hs_object_id};
#                 '''
#                 cursor.execute(update_query)
#                 conn_details.commit()
#                 print(f"Se actualizó la fila {index + 1} en la tabla tests. ID: {hs_object_id}")
#             else:
#                 # Si no existe, inserta una nueva fila
#                 insert_query = f'''
#                     INSERT INTO tests (createdate, dealname, hs_lastmodifieddate, hs_object_id, pipeline)
#                     VALUES ('{createdate}'::TIMESTAMPTZ, '{dealname}', '{hs_lastmodifieddate}'::TIMESTAMPTZ, '{hs_object_id}', '{pipeline}');
#                 '''
#                 cursor.execute(insert_query)
#                 conn_details.commit()
#                 print(f"Se insertó la fila {index + 1} en la tabla tests. ID: {hs_object_id}")

#         cursor.close()
#         conn_details.close()


# conn_details = psycopg2.connect(
#     host= host,
#     database= database,
#     user= user,
#     password= passw,
#     port= 5432
# )

# # Suponiendo que df es tu DataFrame
# # Reemplaza df con el nombre real de tu DataFrame
# procesador = ProcesadorDataFrame(conn_details)
# procesador.procesar_dataframe(df)



"""----------------------------- get deals object ---------------------------------"""
# def schedule_ddbb():
#     props = ['hs_object_id', 'dealname', 'pipeline']

#     df_result = HubspotDealsFetcher(hs_test).get_all_deals_df(props)
#     print(df_result)

#     conn_details = psycopg2.connect(
#         host= host,
#         database= database,
#         user= user,
#         password= passw,
#         port= 5432
#     )

#     # Suponiendo que df es tu DataFrame
#     # Reemplaza df con el nombre real de tu DataFrame
#     procesador = RefreshDataBase(conn_details).procesar_dataframe(df_result)

# schedule_ddbb()
# # Programar la tarea para que se ejecute cada minuto
# schedule.every(10).seconds.do(schedule_ddbb)

# # Ejecutar el programa
# while True:
#     schedule.run_pending()
#     time.sleep(1)

# props = ['hs_object_id', 'dealname', 'createdate', 'closedate', 'hs_lastmodifieddate', 'pipeline', 'dealstage', 'hubspot_owner_id', 'billing_type',
#               'deal___guest_type', 'check_in_date', 'check_out_date', 'length_of_stay_in_days_test', 'monthly_budget__temp_', 'apartment_of_interest___list',
#               'apartment_booked___list', 'deal___neighborhood', 'deal___purpose_of_rental', 'company_sponsored__new_', 'deal____pet_friendly__apt__required',
#               'deal_contacted_through__new_', 'deal_contacted_through_drilldown_1__new_', 'associated_contact_email', 'associated_contact_phone_nr',
#               'spreadsheet_timestamp', 'b2b_source', 'time_from_creation_to_booking', 'timetoclose', 'hs_is_closed_won', 'booking_type', 'backoffice_id',
#               'booking_city', 'booking___stage_enrollment_date', 'total_margin_x_stay', 'sadmin___rent_amount', 'sadmin___utilities', 
#               'sadmin___final_cleaning', 'sadmin___pet_fee', 'deals_api___create_date']


# results = HubspotDealsFetcher(hs_prod_key).get_all_deals_df(props)
# results = results[props]

# # Filtro combinando condiciones en la columna "pipeline" y "createdate"
# fil_pipeline = results['pipeline'].isin(['95422169', '135831272'])
# # # fil_date = (pd.to_datetime(results['createdate']).dt.tz_localize(None) > pd.to_datetime('2023-10-01').tz_localize(None))

# # Obtener la fecha de ayer
# fecha_ayer = datetime.now() - timedelta(days=1)
# fecha_ayer = fecha_ayer.replace(hour=23, minute=59, second=59, microsecond=0)  # Establecer la hora a las 00:00:00 para comparar fechas

# # Aplicar filtro para las filas creadas ayer
# fil_date = (pd.to_datetime(results['createdate']).dt.tz_localize(None) < fecha_ayer)

# results = results[fil_pipeline & fil_date].copy()

# print(results['deals_api___create_date'])


"""-------------------------------------------- DDBB UPDATE/INSERT -------------------------------------------"""

# # Conectar a la base de datos
# conn_details = psycopg2.connect(
#     host=db_host,
#     database=db_database,
#     user=db_user,
#     password=db_password,
#     port=5432
# )

# cursor = conn_details.cursor()

# # Iniciar transacción
# def update_deals_ddbb(ddbb):
#     start = time.perf_counter()
#     try:
#         # Iniciar transacción
#         conn_details.autocommit = False
#         cursor = conn_details.cursor()

#         # Borrar todas las filas existentes en la tabla
#         cursor.execute("TRUNCATE TABLE hs_deals_q4_tests")

#         # Recorre las filas del DataFrame y prepara los datos para la inserción o actualización
#         rows = []
#         for index, row in ddbb.iterrows():
#             deal_id = row["hs_object_id"]
#             deal_name = escape_quotes(row["dealname"])
#             create_date = None if row["createdate"] == '' else row["createdate"]
#             close_date = None if row["closedate"] == '' else row["closedate"]
#             last_update = row['hs_lastmodifieddate'] if row['hs_lastmodifieddate'] != '' else 'NULL'
#             pipeline = row["pipeline"]
#             deal_stage = row["dealstage"]
#             deal_owner = None if row["hubspot_owner_id"] == '' else row["hubspot_owner_id"]
#             billing_type = None if row["billing_type"] == '' else row["billing_type"]
#             guest_type = None if row["deal___guest_type"] == '' else row["deal___guest_type"]
#             check_in_date = None if row["check_in_date"] == '' else row["check_in_date"]
#             check_out_date = None if row["check_out_date"] == '' else row["check_out_date"]
#             lenght_of_stay = None if row["length_of_stay_in_days_test"] == '' else row["length_of_stay_in_days_test"]
#             budget = row["monthly_budget__temp_"]
#             apt_of_interest = row["apartment_of_interest___list"]
#             apt_booked = row["apartment_booked___list"]
#             neighborhood = row["deal___neighborhood"]
#             purpose_of_rental = row["deal___purpose_of_rental"]
#             company_sponsored = row["company_sponsored__new_"]
#             pet_fee_required = row["deal____pet_friendly__apt__required"]
#             source = row["deal_contacted_through__new_"]
#             source_drilldown = row["deal_contacted_through_drilldown_1__new_"]
#             contact_email = row["associated_contact_email"]
#             contact_phone = row["associated_contact_phone_nr"]
#             timestamp = row["spreadsheet_timestamp"]
#             inbound_outbound = row["b2b_source"]
#             time_to_close = None if row["timetoclose"] == '' else row["timetoclose"]
#             is_won = row["hs_is_closed_won"]
#             booking_type = None if row["booking_type"] == '' else row["booking_type"]
#             time_to_won = None if row["time_from_creation_to_booking"] == '' else row["time_from_creation_to_booking"]
#             backoffice_id = None if row["backoffice_id"] == '' else row["backoffice_id"]
#             city = None if row["booking_city"] == '' else row["booking_city"]
#             booking_date = None if row["booking___stage_enrollment_date"] == '' else row["booking___stage_enrollment_date"]
#             margin_x_stay = None if row["total_margin_x_stay"] == '' else row["total_margin_x_stay"]
#             rent_amount = None if row["sadmin___rent_amount"] == '' else row["sadmin___rent_amount"]
#             sadmin_utilities = None if row["sadmin___utilities"] == '' else row["sadmin___utilities"]
#             sadmin_final_cleaning = None if row["sadmin___final_cleaning"] == '' else row["sadmin___final_cleaning"]
#             sadmin_pet_fee = None if row["sadmin___pet_fee"] == '' else row["sadmin___pet_fee"]
#             deals_api___create_date = None if row["deals_api___create_date"] == '' else row["deals_api___create_date"]


#                         # Agregar los datos a la lista de filas
#             rows.append((
#                 deal_id, deal_name, create_date, close_date, last_update, pipeline, deal_stage, 
#                 deal_owner, billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
#                 budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
#                 company_sponsored, pet_fee_required, source, source_drilldown,
#                 contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
#                 is_won, booking_type, time_to_won, backoffice_id, city, booking_date,
#                 margin_x_stay, rent_amount, sadmin_utilities, sadmin_final_cleaning, sadmin_pet_fee, deals_api___create_date
#             ))
#             print(f"Se insertó la fila {index + 1} en la tabla hs_deals_q4_tests. ID: {deal_id} \n {booking_date}")

#         # Insertar todas las filas usando execute_values
#         execute_values(
#             cursor,
#             '''
#             INSERT INTO hs_deals_q4_tests (
#                 deal_id, deal_name, create_date, close_date, last_update, pipeline, deal_stage, 
#                 deal_owner, billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
#                 budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
#                 company_sponsored, pet_fee_required, source, source_drilldown,
#                 contact_email, contact_phone, timestamp, inbound_outbound, time_to_close,
#                 is_won, booking_type, time_to_won, backoffice_id, city, booking_date,
#                 margin_x_stay, rent_amount, sadmin_utilities, sadmin_final_cleaning, sadmin_pet_fee, deals_api___create_date
#             )
#             VALUES %s;
#             ''',
#             rows
#         )

#         # Confirmar la transacción
#         conn_details.commit()

#     except Exception as e:
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

# results2 = results

# update_deals_ddbb(results2)

"""----------------------------- search deals object ---------------------------------"""

from hubspot.crm.deals import PublicObjectSearchRequest, ApiException
from dateutil import parser

api_client = hubspot.Client.create(access_token=hs_prod_key)

def search_deals(st_date):
    st = time.perf_counter()
    # Timestamp en milisegundos de la fecha proporcionada
    date = str(int(parser.isoparse(f"{st_date}T00:00:00.000Z").timestamp() * 1000))

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
                        "operator": "GT",
                        "value": date      
                    },
                    {
                        "values": [95422169, 135831272],
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

search = search_deals('2023-10-01')
print(search)