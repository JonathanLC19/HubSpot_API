import hubspot
from hubspot.crm.deals import ApiException

import pandas as pd
import time
from credentials import hs_prod_key, hs_sb_key
from datetime import datetime

from pprint import pprint


# # Crear el cliente de HubSpot
# client = hubspot.Client.create(access_token=hs_sb_key)

# # Configurar variables para la paginación
# limit = 100
# after = None

# # Crear un DataFrame vacío para almacenar los resultados
# df_columns = ['dealname', 'createdate', 'pipeline', 'dealstage', 'cp___latest_source', 'cp___latest_source__2_', 'cp___latest_source_timestamp']
# df = pd.DataFrame(columns=df_columns)

# while True:
#     time.sleep(0.5)
#     try:
#         # Obtener la página actual
#         api_response = client.crm.deals.basic_api.get_page(limit=limit, after=after, properties=['dealname', 'createdate', 'pipeline', 'dealstage', 'cp___latest_source', 'cp___latest_source__2_', 'cp___latest_source_timestamp'], archived=False)
        
#         # Imprimir o procesar los resultados de la página actual
#         pprint(api_response.results)

#         # Verificar si hay más páginas
#         if not api_response.paging or not api_response.paging.next:
#             break

#         # Actualizar el 'after' para la próxima página
#         after = api_response.paging.next.after

#         # Convertir los resultados en un DataFrame y agregarlos al DataFrame principal
#         page_df = pd.DataFrame([deal.properties for deal in api_response.results])
#         df = pd.concat([df, page_df], ignore_index=True)
#         print(df)

#     except ApiException as e:
#         print("Exception when calling basic_api->get_page: %s\n" % e)
#         break  # Puedes decidir cómo manejar errores, aquí simplemente rompemos el bucle


def get_all_deals_df(access_token):
    # Crear el cliente de HubSpot
    client = hubspot.Client.create(access_token=access_token)

    # Configurar variables para la paginación
    limit = 100
    after = None

    # Crear un DataFrame vacío para almacenar los resultados
    df_columns = ['dealname', 'createdate', 'pipeline', 'dealstage', 'cp___latest_source', 'cp___latest_source__2_', 'cp___latest_source_timestamp', 'associated_company', 'associated_contact']
    df = pd.DataFrame(columns=df_columns)

    while True:
        time.sleep(0.5)
        try:
            # Obtener la página actual
            api_response = client.crm.deals.basic_api.get_page(limit=limit, after=after, properties=df_columns, archived=False)

            # Verificar si hay más páginas
            if not api_response.paging or not api_response.paging.next:
                break

            # Actualizar el 'after' para la próxima página
            after = api_response.paging.next.after

            # Convertir los resultados en un DataFrame y agregarlos al DataFrame principal
            page_df = pd.DataFrame([deal.properties for deal in api_response.results])
            df = pd.concat([df, page_df], ignore_index=True)

        except ApiException as e:
            print("Exception when calling basic_api->get_page: %s\n" % e)
            break  # Puedes decidir cómo manejar errores, aquí simplemente rompemos el bucle

    return df



# client = hubspot.Client.create(access_token=hs_prod_key)

# try:
#     api_response = client.crm.deals.basic_api.get_page(limit=100, archived=False)
#     pprint(api_response)
# except ApiException as e:
#     print("Exception when calling basic_api->get_page: %s\n" % e)



# def getAllDeals(secret_key):
#     client = hubspot.Client.create(access_token=secret_key)
#     deals_data = []
#     next_page = None
#     count = 0

#     while True:
#         time.sleep(1)
#         try:
#             print("Realizando solicitud...")
#             if next_page is None:
#                 api_response = client.crm.deals.basic_api.get_page(limit=100, archived=False)
#             else:
#                 api_response = client.crm.deals.basic_api.get_page(limit=100, archived=False, after=next_page)
#             print("Solicitud completada.")

#             deals_data.extend(api_response.results)

#             # Verificar si estamos en la última página
#             if api_response.paging.next is None:
#                 break

#             next_page = api_response.paging.next.after

#         except Exception as e:
#             print(f"Exception when calling basic_api->get_page: {e}")
#             break

#     deals_df = pd.json_normalize(deals_data)
#     return deals_df

# # Ejemplo de cómo usar la función:
# deals_df = getAllDeals(hs_prod_key)
# print(deals_df)

"""-------------------------------------------------------- SAMPLE TEST --------------------------------------------------------"""

# def getAllDeals(secret_key):
#     client = hubspot.Client.create(access_token=secret_key)
#     deals_data = []
#     next_page = 1
#     request_count = 0  # Variable para contar las solicitudes

#     while request_count < 110:
#         time.sleep(0.5)
#         if next_page == 1:
#             api_response = client.crm.deals.basic_api.get_page(limit=100, archived=False)
#             deals_data.extend(api_response.results)
#             next_page = api_response.paging.next.after
#         else:
#             api_response = client.crm.deals.basic_api.get_page(limit=100, archived=False, after=next_page)
#             if len(api_response.results) > 0:
#                 deals_data.extend(api_response.results)
#                 try:
#                     print("Realizando solicitud...")
#                     next_page = api_response.paging.next.after
#                     api_response.paging.next.link
#                     print("Solicitud completada.")
#                 except AttributeError:
#                     break

#     deals_df = pd.json_normalize(deals_data)
#     return deals_data

# # Ejemplo de cómo usar la función:
# deals_df = getAllDeals(hs_prod_key)

# def getDealsCreatedAfter(secret_key):
#     client = hubspot.Client.create(access_token=secret_key)
#     props = ['dealname', 'createdate', 'pipeline', 'dealstage', 'cp___latest_source', 'cp___latest_source__2_', 'cp___latest_source_timestamp']
#     deals = {}
#     next_page = 1

#     while True:
#         time.sleep(0.5)
#         if next_page == 1:
#             api_response = client.crm.deals.basic_api.get_page(
#                 limit=100, archived=False, properties=props)
#         else:
#             api_response = client.crm.deals.basic_api.get_page(
#                 limit=100, archived=False, properties=props, after=next_page)

#         for item in api_response.results:
#             deals[item.id] = item

#         if len(api_response.results) > 0:
#             try:
#                 next_page = api_response.paging.next.after
#                 api_response.paging.next.link
#             except AttributeError:
#                 break
#         else:
#             break

#     return deals

# # Ejemplo de cómo usar la función:
# deals_created_after_date = getDealsCreatedAfter(hs_sb_key)

