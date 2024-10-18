from get_all_deals import get_all_deals_df
from credentials import hs_sb_key, hs_prod_key
from functions import calcular_tiempo_ejecucion

from concurrent.futures import ThreadPoolExecutor

import hubspot
from hubspot.crm.associations.v4 import ApiException
import pandas as pd

# df = get_all_deals_df(hs_sb_key)
# # print(df)

# for hs_object_id in df['hs_object_id']:
#     try:
#         # Crear el cliente de HubSpot
#         client = hubspot.Client.create(access_token=hs_sb_key)

#         # Realizar la solicitud para obtener las asociaciones
#         api_response = client.crm.associations.v4.basic_api.get_page(object_type="deals", object_id=hs_object_id, to_object_type="companies", limit=500)
        
#         # Procesar la respuesta y almacenar los resultados en otro DataFrame
#         associations_data = {
#             "deal_object_id": [hs_object_id] * len(api_response.results),
#             "company_object_id": [association["to_object_id"] for association in api_response.results]
#         }

#         associations_df = pd.DataFrame(associations_data)

#         # Guardar el DataFrame de asociaciones en otro archivo o realizar otras acciones según sea necesario
#         # Por ejemplo, concatenar todos los DataFrames en uno grande
#         # Si es la primera iteración, crea el DataFrame 'all_associations_df', de lo contrario, concaténalo
#         if 'all_associations_df' not in locals():
#             all_associations_df = associations_df
#         else:
#             all_associations_df = pd.concat([all_associations_df, associations_df], ignore_index=True)

#     except ApiException as e:
#         print(f"Exception when calling basic_api->get_page for hs_object_id {hs_object_id}: {e}")

# # Mostrar el DataFrame con todas las asociaciones
# print(all_associations_df)

# import time
# import pandas as pd
# from concurrent.futures import ThreadPoolExecutor
# import hubspot  # Asegúrate de importar el módulo correcto
# from hubspot.crm.associations.v4 import ApiException

# # from credentials import hs_sb_key, hs_prod_key

# from profilehooks import profile


# def fetch_page(client, limit, after, df_columns):
#     return client.crm.deals.basic_api.get_page(limit=limit, after=after, properties=df_columns, archived=False)

# @calcular_tiempo_ejecucion
# def get_all_deals_df(access_token, properties):
#     # Crear el cliente de HubSpot
#     client = hubspot.Client.create(access_token=access_token)

#     # Configurar variables para la paginación
#     limit = 100  # Ajusta este valor según lo que permita HubSpot
#     after = None

#     # # Definir las columnas del DataFrame
#     # df_columns = ['dealname', 'createdate', 'pipeline', 'dealstage', 'cp___latest_source', 'cp___latest_source__2_', 'cp___latest_source_timestamp', 'associated_company', 'associated_contact']

#     # Crear una lista para almacenar los DataFrames de cada página
#     df_list = []

#     with ThreadPoolExecutor(max_workers=5) as executor:
#         while True:
#             # time.sleep(0.5)
#             try:
#                 # Obtener la página actual
#                 api_response = executor.submit(fetch_page, client, limit, after, properties).result()

#                 # Verificar si hay más páginas
#                 if not api_response.paging or not api_response.paging.next:
#                     break

#                 # Actualizar el 'after' para la próxima página
#                 after = api_response.paging.next.after

#                 # Convertir los resultados en un DataFrame y agregarlos a la lista
#                 page_df = pd.DataFrame([deal.properties for deal in api_response.results])
#                 df_list.append(page_df)

#             except ApiException as e:
#                 print("Exception when calling basic_api->get_page: %s\n" % e)
#                 break

#     # Concatenar todos los DataFrames de la lista en uno solo
#     df = pd.concat(df_list, ignore_index=True)
#     return df


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
def get_all_deals_df(access_token, properties):
    client = hubspot.Client.create(access_token=access_token)
    limit = 100
    df_list = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        for page_df in executor.map(lambda page: page, get_deals_generator(client, limit, properties)):
            df_list.append(page_df)

    df = pd.concat(df_list, ignore_index=True)
    return df


props = ['hs_object_id', 'dealname', 'createdate', 'hs_lastmodifieddate', 'pipeline', 'dealstage', 'hubspot_owner_id', 'billing_type',
              'deal___guest_type', 'check_in_date', 'check_out_date', 'test_jun_23___length_of_stay', 'monthly_budget__temp_', 'apartment_of_interest___list',
              'apartment_booked___list', 'deal___neighborhood', 'deal___purpose_of_rental', 'company_sponsored__new_', 'deal____pet_friendly__apt__required',
              'deal_contacted_through__new_', 'deal_contacted_through_drilldown_1__new_', 'associated_contact_email', 'associated_contact_phone_nr',
              'spreadsheet_timestamp', 'b2b_source', 'time_from_creation_to_booking', 'timetoclose', 'hs_is_closed_won', 'booking_type', 'backoffice_id',
              'booking_city']

deals = get_all_deals_df(hs_prod_key, props)

# Lista de valores por los cuales quieres filtrar
valores_a_filtrar = ['95422169', '135831272']

# Filtro combinando condiciones en la columna "pipeline" y "createdate"
filtro_pipeline = deals['pipeline'].isin(valores_a_filtrar)

new_df = deals[filtro_pipeline].copy()

print(new_df.iloc[:5, 9:13])