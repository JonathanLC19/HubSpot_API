import hubspot
from pprint import pprint
from hubspot.crm.pipelines import ApiException
import pandas as pd

from credentials import hs_prod_key, db_database, db_host, db_password, db_user

import psycopg2
from psycopg2.extras import execute_values


client = hubspot.Client.create(access_token= hs_prod_key)

try:
    api_response = client.crm.pipelines.pipelines_api.get_all(object_type="deals")
    results = api_response.results
    pprint(results)
except ApiException as e:
    print("Exception when calling pipelines_api->get_all: %s\n" % e)

pipes = []
for pipe in results:
    pipes.append({
        "pipe_id": pipe.id,
        "name": pipe.label
    })
df = pd.DataFrame(pipes)

print(df)

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

#     # # Borrar todas las filas existentes en la tabla
#     # cursor.execute("TRUNCATE TABLE hs_pipelines")

#     # Recorre las filas del DataFrame y prepara los datos para la inserción o actualización
#     rows = []
#     for index, row in df.iterrows():
#         pipe_id = row["pipe_id"]
#         name = row["name"]


#         # Verifica si el deal_id existe en la tabla hs_deals_q4
#         cursor.execute(f"SELECT * FROM hs_pipelines WHERE pipe_id = {pipe_id}")
#         existing_deal = cursor.fetchone()

#         if existing_deal:
#             # Si existe, actualiza la fila
#             rows.append((
#                 pipe_id, name
#             ))
#             print(f"Se actualizó la fila {index + 1} en la tabla hs_pipelines. ID: {pipe_id}")
#         else:
#             # Si no existe, inserta una nueva fila
#             rows.append((
#                 pipe_id, name
#             ))
#             print(f"Se insertó la fila {index + 1} en la tabla hs_pipelines. ID: {pipe_id}")

#     # Inserta o actualiza las filas usando execute_values
#     execute_values(
#         cursor,
#         '''
#         INSERT INTO hs_pipelines (
#             pipe_id, name
#         )
#         VALUES %s
#         ON CONFLICT (pipe_id) DO UPDATE SET
#             name = EXCLUDED.name
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

""" ------------------------  CREATE PIPELINE ----------------------------- """

import hubspot
from pprint import pprint
from hubspot.crm.pipelines import PipelineInput, ApiException

client = hubspot.Client.create(access_token="YOUR_ACCESS_TOKEN")

pipeline_input = PipelineInput(display_order=0, stages=[{"label":"In Progress","metadata":{"ticketState":"OPEN"},"displayOrder":0},{"label":"Done","metadata":{"ticketState":"CLOSED"},"displayOrder":1}], label="My replaced pipeline")
try:
    api_response = client.crm.pipelines.pipelines_api.create(object_type="objectType", pipeline_input=pipeline_input)
    pprint(api_response)
except ApiException as e:
    print("Exception when calling pipelines_api->create: %s\n" % e)