import pandas as pd
import hubspot
from hubspot.crm.deals import BasicApi, ApiException, PublicObjectSearchRequest
from pprint import pprint
import json
from concurrent.futures import ThreadPoolExecutor
from decorators import calcular_tiempo_ejecucion
import psycopg2
import time
from dateutil import parser

import requests
from metabase import headers, mb_url_card, mb_url
import credentials as crd

import os
from dotenv import load_dotenv

load_dotenv()

# from credentials import hs_prod_key
# from creds2 import host, database, user, passw, port


# Función para obtener todos los registros de deals
"""------------------------------------ USE GENERATORS ------------------------------------"""

def fetch_page(client, limit, after, df_columns):
    return client.crm.deals.basic_api.get_page(limit=limit, after=after, properties=df_columns, archived=False)

def get_deals_generator(client, limit, properties):
    after = None

    while True:
        try:
            api_response = fetch_page(client, limit, after, properties)

            if not api_response.paging or not api_response.paging.next:
                break

            after = api_response.paging.next.after

            yield pd.DataFrame([deal.properties for deal in api_response.results])

        except ApiException as e:
            print("Exception when calling basic_api->get_page: %s\n" % e)
            break

@calcular_tiempo_ejecucion
def get_all_deals_df(access_token, properties, progress_bar=None):
    client = hubspot.Client.create(access_token=access_token)
    limit = 100
    df_list = []

    with ThreadPoolExecutor(max_workers=8) as executor:
        for page_df in executor.map(lambda page: page, get_deals_generator(client, limit, properties)):
            df_list.append(page_df)

    df = pd.concat(df_list, ignore_index=True)
    return df

"""----------------------- LIST FORMAT --------------------------------"""

# def fetch_page(client, limit, after, df_columns):
#     return client.crm.deals.basic_api.get_page(limit=limit, after=after, properties=df_columns, archived=False)

# def get_deals_generator(client, limit, properties):
#     after = None

#     while True:
#         try:
#             api_response = fetch_page(client, limit, after, properties)

#             if not api_response.paging or not api_response.paging.next:
#                 break

#             after = api_response.paging.next.after

#             yield api_response  # Retorna directamente el objeto api_response

#         except ApiException as e:
#             print("Exception when calling basic_api->get_page: %s\n" % e)
#             break

# @calcular_tiempo_ejecucion
# def get_all_deals_df(access_token, properties):
#     client = hubspot.Client.create(access_token=access_token)
#     limit = 100
#     df_list = []

#     with ThreadPoolExecutor(max_workers=5) as executor:
#         for page in executor.map(lambda page: page.results, get_deals_generator(client, limit, properties)):
#             # Trabaja directamente con 'page', que es una lista de resultados
#             df_list.extend(page)

#     # Retorna la lista extendida, que contiene todos los resultados
#     return df_list

"""-------------------------------- DDBB --------------------------------"""

# def sql_ddbb(db_host, db_database, db_user, db_password):
#     try:
#         # Conectar a la base de datos
#         conn_details = psycopg2.connect(
#             host=db_host,
#             database=db_database,
#             user=db_user,
#             password=db_password,
#             port=port
#         )

#         # Crear y devolver el objeto cursor
#         return conn_details.cursor()

#     except psycopg2.Error as e:
#         print(f"Error al conectar a la base de datos: {e}")
#         return None
    


"""-------------------------------- ESCAPE QUOTES IN TEXT --------------------------------"""
def escape_quotes(value):
    """
    Escapa el símbolo de comilla simple en una cadena de texto.
    """
    return value.replace("'", "''") if isinstance(value, str) else value



"""--------------------------------- get deals class --------------------------------------"""

class HubspotDealsFetcher:
    def __init__(self, access_token, limit=100, max_workers=8):
        self.client = hubspot.Client.create(access_token=access_token)
        self.limit = limit
        self.max_workers = max_workers

    def fetch_page(self, after=None, df_columns=None):
        return self.client.crm.deals.basic_api.get_page(
            limit=self.limit,
            after=after,
            properties=df_columns,
            archived=False
        )

    def get_deals_generator(self, properties):
        after = None

        while True:
            try:
                api_response = self.fetch_page(after=after, df_columns=properties)

                if not api_response.paging or not api_response.paging.next:
                    break

                after = api_response.paging.next.after

                yield pd.DataFrame([deal.properties for deal in api_response.results])

            except ApiException as e:
                print("Exception when calling basic_api->get_page: %s\n" % e)
                break

    @staticmethod
    def calcular_tiempo_ejecucion(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            print(f"Tiempo de ejecución: {end_time - start_time} segundos")
            return result

        return wrapper

    @calcular_tiempo_ejecucion
    def get_all_deals_df(self, properties):
        df_list = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for page_df in executor.map(lambda page: page, self.get_deals_generator(properties)):
                df_list.append(page_df)

        df = pd.concat(df_list, ignore_index=True)
        return df



"""--------------------------------- update/insert ddbb class --------------------------------------"""
# conn_details = psycopg2.connect(
#     host= ,
#     database= database,
#     user= user,
#     password= passw,
#     port= 5432
# )

# class ProcesadorDataFrame:
#     def __init__(self, conn_details):

#         self.conn_details = conn_details

    # def procesar_dataframe(self, df):
    #     cursor = self.conn_details.cursor()

    #     for index, row in df.iterrows():
    #         createdate = row['createdate']
    #         dealname = escape_quotes(row['dealname'])
    #         hs_lastmodifieddate = row["hs_lastmodifieddate"]
    #         hs_object_id = row["hs_object_id"]
    #         pipeline = row['pipeline']

    #         # Verifica si el deal_id existe en la tabla hs_deals_q4
    #         cursor.execute(f"SELECT * FROM tests WHERE hs_object_id = {hs_object_id}")
    #         existing_deal = cursor.fetchone()

    #         if existing_deal:
    #             # Si existe, actualiza la fila
    #             update_query = f'''
    #                 UPDATE tests
    #                 SET createdate = '{createdate}'::TIMESTAMPTZ,
    #                     dealname = '{dealname}',
    #                     hs_lastmodifieddate = '{hs_lastmodifieddate}'::TIMESTAMPTZ,
    #                     hs_object_id = '{hs_object_id}',
    #                     pipeline = '{pipeline}'
    #                 WHERE hs_object_id = {hs_object_id};
    #             '''
    #             cursor.execute(update_query)
    #             conn_details.commit()
    #             print(f"Se actualizó la fila {index + 1} en la tabla tests. ID: {hs_object_id}")
    #         else:
    #             # Si no existe, inserta una nueva fila
    #             insert_query = f'''
    #                 INSERT INTO tests (createdate, dealname, hs_lastmodifieddate, hs_object_id, pipeline)
    #                 VALUES ('{createdate}'::TIMESTAMPTZ, '{dealname}', '{hs_lastmodifieddate}'::TIMESTAMPTZ, '{hs_object_id}', '{pipeline}');
    #             '''
    #             cursor.execute(insert_query)
    #             conn_details.commit()
    #             print(f"Se insertó la fila {index + 1} en la tabla tests. ID: {hs_object_id}")

    #     cursor.close()
    #     conn_details.close()


#region REFRESH DDBB
"""------------------------------------ refresh ddbb -------------------------------------------"""

class RefreshDataBase:
    def __init__(self, conn_details):
        self.conn_details = conn_details

    def procesar_dataframe(self, df):
        cursor = self.conn_details.cursor()

        # Truncar la tabla antes de insertar nuevos datos
        cursor.execute("TRUNCATE TABLE tests;")
        self.conn_details.commit()
        print('DDBB Deleted')

        for index, row in df.iterrows():
            createdate = row['createdate']
            dealname = escape_quotes(row['dealname'])
            hs_lastmodifieddate = row["hs_lastmodifieddate"]
            hs_object_id = row["hs_object_id"]
            pipeline = row['pipeline']

            # Insertar una nueva fila
            insert_query = f'''
                INSERT INTO tests (createdate, dealname, hs_lastmodifieddate, hs_object_id, pipeline)
                VALUES ('{createdate}'::TIMESTAMPTZ, '{dealname}', '{hs_lastmodifieddate}'::TIMESTAMPTZ, '{hs_object_id}', '{pipeline}');
            '''
            cursor.execute(insert_query)
            self.conn_details.commit()
            print(f"Se insertó la fila {index + 1} en la tabla tests. ID: {hs_object_id}")

        cursor.close()


"""--------------------------------------- BO ID - ELIMINATE POINT ---------------------------------"""


def eliminate_point_nr(nr):
    if nr is None:
        return ''  # Retorna None si el valor es None
    # Convierte el número a cadena y elimina el punto decimal si está presente
    numero_sin_punto = str(nr).replace('.', '')
    
    # Convierte la cadena resultante de nuevo a un entero
    no_point = int(numero_sin_punto)
        
    return no_point



#region SEARCH DEALS
"""--------------------------------------- SEARCH DEALS ---------------------------------"""
def search_deals(hs_key, st_date,df2):
    st_date = st_date
    current_date = pd.Timestamp.now().strftime('%Y-%m-%d')

    # Loop through months starting from 2023-10-01 up to current month
    while st_date < current_date:
        en_date = pd.to_datetime(st_date) + pd.offsets.MonthEnd(1)
        en_date = en_date + pd.DateOffset(days=1)
        en_date = en_date.strftime('%Y-%m-%d')
        print(f"Searching deals for month: {st_date} to {en_date}")

        st = time.perf_counter()
        api_client = hubspot.Client.create(access_token=hs_key)

        start = str(int(parser.isoparse(f"{st_date}T00:00:00.000Z").timestamp() * 1000))
        end = str(int(parser.isoparse(f"{en_date}T00:00:00.000Z").timestamp() * 1000))

        properties = [
            'hs_object_id', 'dealname', 'createdate', 'closedate', 'hs_lastmodifieddate', 'pipeline', 'dealstage', 'hubspot_owner_id', 'billing_type',
            'deal___guest_type', 'check_in_date', 'check_out_date', 'length_of_stay_in_days_test', 'monthly_budget__temp_', 'apartment_of_interest___list',
            'apartment_booked___list', 'deal___neighborhood', 'deal___purpose_of_rental', 'company_sponsored__new_', 'deal____pet_friendly__apt__required',
            'deal_contacted_through__new_', 'deal_contacted_through_drilldown_1__new_', 'associated_contact_email', 'associated_contact_phone_nr',
            'spreadsheet_timestamp', 'b2b_source', 'time_from_creation_to_booking', 'timetoclose', 'hs_is_closed_won', 'booking_type', 'backoffice_id',
            'booking_city', 'booking___stage_enrollment_date', 'total_margin_x_stay', 'sadmin___rent_amount', 'sadmin___utilities', 
            'sadmin___final_cleaning', 'sadmin___pet_fee', 'notes_last_updated', 'last_activity_date___associated_contact'
        ]

        all_results = []

        public_object_search_request = PublicObjectSearchRequest(
            properties=properties,
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
                            "propertyName": "pipeline",
                            "operator": "IN"
                        }
                    ]
                }
            ],
            limit=100,
            after=None
        )

        try:
            while True:
                time.sleep(0.1)
                api_response = api_client.crm.deals.search_api.do_search(public_object_search_request=public_object_search_request)
                results = api_response.results
                all_results.extend(results)

                if api_response.paging and api_response.paging.next:
                    public_object_search_request.after = api_response.paging.next.after
                else:
                    break

            df1 = pd.DataFrame(all_results)
            df2 = pd.concat([df2, df1], ignore_index=True)

        except ApiException as e:
            print("Exception when calling search_api->do_search: %s\n" % e)
            
        # Crear DataFrame
        properties_list = []
        for row in df2[0]:
            properties_list.append(row.properties)

        results = pd.DataFrame(properties_list)
        results = results[properties]
        en = time.perf_counter()
        tt = en - st
        print(f'Time taken for current search: {tt}')

        # Move to the next month
        st_date = pd.to_datetime(st_date) + pd.DateOffset(months=1)
        st_date = st_date.strftime('%Y-%m-%d')

    return results




#region METABASE
"""------------------------------------ METABASE ------------------------------------"""
def metabase_q(question):
    # Find public questions
    response = requests.get(mb_url_card,
                            headers=headers).json()
    questions = [q for q in response if q['public_uuid']]
    # print(f'{len(questions)} public of {len(response)} questions')

    # Get Backoffice data
    uuid_1 = questions[question]['public_uuid']
    response = requests.get(f'{mb_url}/api/public/card/{uuid_1}/query',
                            headers=headers)
    res = response.json()
    rows = res['data']['rows']
    cols = [v['name'] for i, v in enumerate(res['data']['cols'])]
    bo_db = pd.DataFrame(rows, columns = cols)
    return bo_db





#region READ SQL
"""------------------------------------ READ SQL ------------------------------------"""
def read_ddbb(table):
    host = os.environ['db_host']
    port = os.environ['db_port']
    name = os.environ['db_database']
    user = os.environ['db_user']
    pw = os.environ['db_password']

    with psycopg2.connect(f"host={host} port={port} dbname={name} user={user} password={pw}") as conn:
        sql = f"select * from {table};"
        dat = pd.read_sql_query(sql, conn)
        return dat
    

#region READ CALLS
"""--------------------------------------- READ DEALS---------------------------------------------"""

def readCalls(api_key):
    client = hubspot.Client.create(access_token=api_key)

    try:
        api_response = client.crm.objects.calls.basic_api.get_page(limit=10, properties=["hs_call_direction"], archived=False)
        # pprint(api_response)
    except ApiException as e:
        print("Exception when calling basic_api->get_page: %s\n" % e)

    return api_response