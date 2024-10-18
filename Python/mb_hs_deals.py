from profilehooks import profile

import psycopg2
from psycopg2.extras import execute_values

# from hubspot.crm.associations.v4 import ApiException
from hubspot.crm.deals import BasicApi
from get_deals import get_all_deals_df
from functions import calcular_tiempo_ejecucion


import pandas as pd

from credentials import db_password, db_user, db_database, db_host, hs_prod_key, hs_sb_key
# import creds2

import time
# from functools import cache

props = ['hs_object_id', 'dealname', 'createdate', 'hs_lastmodifieddate', 'pipeline', 'dealstage', 'hubspot_owner_id', 'billing_type',
              'deal___guest_type', 'check_in_date', 'check_out_date', 'test_jun_23___length_of_stay', 'monthly_budget__temp_', 'apartment_of_interest___list',
              'apartment_booked___list', 'deal___neighborhood', 'deal___purpose_of_rental', 'company_sponsored__new_', 'deal____pet_friendly__apt__required',
              'deal_contacted_through__new_', 'deal_contacted_through_drilldown_1__new_', 'associated_contact_email', 'associated_contact_phone_nr',
              'spreadsheet_timestamp', 'b2b_source', 'time_from_creation_to_booking', 'timetoclose', 'hs_is_closed_won', 'booking_type', 'backoffice_id',
              'booking_city', 'total_margin_x_stay', 'sadmin___rent_amount', 'length_of_stay_in_days_test', 'sadmin___utilities', 'sadmin___utilities', 
              'sadmin___pet_fee', 'sadmin___final_cleaning']

@calcular_tiempo_ejecucion
def mb_hs_deals(token, prop):
    start = time.perf_counter()

    deals_df_tests = get_all_deals_df(token, prop)
    rows = deals_df_tests.shape[0]


    # Lista de valores por los cuales quieres filtrar
    valores_a_filtrar = ['95422169', '135831272']

    # Filtro combinando condiciones en la columna "pipeline" y "createdate"
    filtro_pipeline = deals_df_tests['pipeline'].isin(valores_a_filtrar)

    # Convierte la columna 'createdate' a datetime y luego a objeto sin información de zona horaria
    deals_df_tests['createdate'] = deals_df_tests['createdate'].apply(lambda x: pd.to_datetime(x, errors='coerce', format='mixed', dayfirst=True).tz_localize(None))

    # Realiza la comparación después de eliminar la información de zona horaria
    filtro_fecha = deals_df_tests['createdate'] > pd.to_datetime('2023-10-01')

    # Aplica el filtro combinado
    new_df = deals_df_tests[filtro_pipeline].copy()
    # new_df = deals_df_tests[filtro_pipeline & filtro_fecha].copy()
    # Convierte la columna 'createdate' a datetime y luego a objeto sin información de zona horaria
    deals_df_tests['createdate'] = deals_df_tests['createdate'].apply(lambda x: pd.to_datetime(x, errors='coerce', format='mixed', dayfirst=True).tz_localize(None))

    # Realiza la comparación después de eliminar la información de zona horaria
    filtro_fecha = deals_df_tests['createdate'] > pd.to_datetime('2023-10-01')

    # Aplica el filtro combinado
    new_df = deals_df_tests[filtro_pipeline & filtro_fecha].copy()



    # Especifica el nombre de la columna que deseas dividir
    los = 'test_jun_23___length_of_stay'

    # Verifica y convierte los valores de la columna a números (float) y maneja valores nulos
    new_df[los] = pd.to_numeric(new_df[los], errors='coerce')

    # Divide los valores de la columna por 86400000
    new_df[los] = new_df[los] / 86400000


    # # Iterar sobre todas las columnas del DataFrame
    # for col in new_df.columns:
    #     # Verificar si la columna contiene fechas
    #     if 'fecha' in col:
    #         # Convertir las cadenas de fecha al formato datetime
    #         new_df[col] = pd.to_datetime(new_df[col], errors='coerce')
    #         # Aplicar el formato deseado
    #         new_df[col] = new_df[col].dt.strftime('%Y-%m-%d')

    end = time.perf_counter()
    ttime = end - start
    print(f'Tiempo total de ejecución para {rows} filas: {ttime} segundos')
    return new_df
    
df = mb_hs_deals(hs_prod_key, props)
print(df.sample(10, axis=1).sample(10, axis=0))

# print(mb_hs_deals(, props))

# # Conectar a la base de datos
# conn_details = psycopg2.connect(
#     host=creds2.host,
#     database=creds2.database,
#     user=creds2.user,
#     password=creds2.passw,
#     port=5432
# )

# cursor = conn_details.cursor()