# import hubspot
# from pprint import pprint
# from hubspot.crm.owners import ApiException
# from credentials import hs_prod_key

# client = hubspot.Client.create(access_token= hs_prod_key)

# try:
#     api_response = client.crm.owners.owners_api.get_page(limit=100, archived=False)
#     pprint(api_response)
# except ApiException as e:
#     print("Exception when calling owners_api->get_page: %s\n" % e)

import pandas as pd
import hubspot
from hubspot.crm.owners import ApiException
import credentials as creds

import psycopg2
from psycopg2.extras import execute_values

class HubspotOwnersFetcher:
    def __init__(self, access_token, limit=100):
        self.client = hubspot.Client.create(access_token=access_token)
        self.limit = limit

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

    def fetch_all_owners(self):
        owners = []
        after = None
        while True:
            try:
                api_response = self.client.crm.owners.owners_api.get_page(limit=self.limit, after=after, archived=False)
                owners.extend(api_response.results)
                if not api_response.paging or not api_response.paging.next:
                    break
                after = api_response.paging.next.after
               
            except ApiException as e:
                print("Exception when calling owners_api->get_page: %s\n" % e)
                break
        return owners

    @calcular_tiempo_ejecucion
    def get_all_owners_df(self):
        owners = self.fetch_all_owners()
        owner_data = []
        for owner in owners:
            owner_data.append({
                "owner_id": owner.id,
                "email": owner.email,
                "first_name": owner.first_name,
                "last_name": owner.last_name
            })
        df = pd.DataFrame(owner_data)
        return df


# Uso de la clase HubspotOwnersFetcher
fetcher = HubspotOwnersFetcher(access_token=creds.hs_prod_key)
owners_df = fetcher.get_all_owners_df()
owners_df = owners_df[owners_df['email'].str.contains('ukio.com')]
owners_df['hs_user'] = owners_df['first_name'] + ' ' + owners_df['last_name']

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
print(owners_df)

""" ------------------------ UPDATE DDBB ----------------------------- """
# Conectar a la base de datos
# conn_details = psycopg2.connect(
#     host=db_host,
#     database=db_database,
#     user=db_user,
#     password=db_password,
#     port=5432
# )

# cursor = conn_details.cursor()


# # Iniciar transacción
# try:
#     # Iniciar transacción
#     conn_details.autocommit = False
#     cursor = conn_details.cursor()

#     # Borrar todas las filas existentes en la tabla
#     cursor.execute("TRUNCATE TABLE hs_users")

#     # Recorre las filas del DataFrame y prepara los datos para la inserción o actualización
#     rows = []
#     for index, row in owners_df.iterrows():
#         owner_id = row["owner_id"]
#         email = row["email"]
#         first_name = row["first_name"]
#         last_name = row["last_name"]
#         hs_user = row["hs_user"]


#         # Verifica si el deal_id existe en la tabla hs_deals_q4
#         cursor.execute(f"SELECT * FROM hs_users WHERE owner_id = {owner_id}")
#         existing_deal = cursor.fetchone()

#         if existing_deal:
#             # Si existe, actualiza la fila
#             rows.append((
#                 owner_id, email, first_name, last_name, hs_user
#             ))
#             print(f"Se actualizó la fila {index + 1} en la tabla hs_users. ID: {owner_id}")
#         else:
#             # Si no existe, inserta una nueva fila
#             rows.append((
#                 owner_id, email, first_name, last_name, hs_user
#             ))
#             print(f"Se insertó la fila {index + 1} en la tabla hs_users. ID: {owner_id}")

#     # Inserta o actualiza las filas usando execute_values
#     execute_values(
#         cursor,
#         '''
#         INSERT INTO hs_users (
#             owner_id, email, first_name, last_name, hs_user
#         )
#         VALUES %s
#         ON CONFLICT (owner_id) DO UPDATE SET
#             email = EXCLUDED.email,
#             first_name = EXCLUDED.first_name,
#             last_name = EXCLUDED.last_name,
#             hs_user = EXCLUDED.hs_user
#         ;
#         ''',
#         rows
#     )

#     # Confirmar la transacción
#     conn_details.commit()

# except ValueError as e:
#     # Si hay algún error, realiza un rollback
#     print(f"Error: {e}")
#     conn_details.rollback()

# finally:
#     # Cierra el cursor y la conexión
#     cursor.close()
#     conn_details.close()