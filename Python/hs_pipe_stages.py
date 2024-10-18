import hubspot
from pprint import pprint
from hubspot.crm.pipelines import ApiException
import pandas as pd

from credentials import hs_prod_key, db_database, db_host, db_password, db_user, hs_sb_key

import psycopg2
from psycopg2.extras import execute_values


client = hubspot.Client.create(access_token= hs_prod_key)

try:
    api_response = client.crm.pipelines.pipelines_api.get_all(object_type="tickets")
    results = api_response.results
    # pprint(results)
except ApiException as e:
    print("Exception when calling pipelines_api->get_all: %s\n" % e)


# Creamos listas para almacenar los datos
pipe_ids = []
stage_ids = []
stage_labels = []

# Iteramos sobre la lista de diccionarios
for item in results:
    # Obtenemos el ID principal
    main_id = item.id
    # Iteramos sobre los diccionarios de 'stages'
    for stage in item.stages:
        # Guardamos el ID principal, ID del stage y su label
        pipe_ids.append(main_id)
        stage_ids.append(stage.id)
        stage_labels.append(stage.label)

# Creamos el DataFrame
df = pd.DataFrame({
    'pipe_id': pipe_ids,
    'stage_id': stage_ids,
    'stage_label': stage_labels
})

# Mostramos el DataFrame
print(results)


""" ------------------------ UPDATE DDBB ----------------------------- """
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
# try:
#     # Iniciar transacción
#     conn_details.autocommit = False
#     cursor = conn_details.cursor()

#     # # Borrar todas las filas existentes en la tabla
#     # cursor.execute("TRUNCATE TABLE hs_pipe_stages")

#     # Recorre las filas del DataFrame y prepara los datos para la inserción o actualización
#     rows = []
#     for index, row in df.iterrows():
#         stage_id = row["stage_id"]
#         stage_label = row["stage_label"]
#         pipe_id = row["pipe_id"]


#         # Verifica si el deal_id existe en la tabla hs_deals_q4
#         cursor.execute(f"SELECT * FROM hs_pipe_stages WHERE stage_id = {stage_id}")
#         existing_deal = cursor.fetchone()

#         if existing_deal:
#             # Si existe, actualiza la fila
#             rows.append((
#                 stage_id, stage_label, pipe_id
#             ))
#             print(f"Se actualizó la fila {index + 1} en la tabla hs_pipe_stages. ID: {stage_id}")
#         else:
#             # Si no existe, inserta una nueva fila
#             rows.append((
#                 stage_id, stage_label, pipe_id
#             ))
#             print(f"Se insertó la fila {index + 1} en la tabla hs_pipe_stages. ID: {stage_id}")

#     # Inserta o actualiza las filas usando execute_values
#     execute_values(
#         cursor,
#         '''
#         INSERT INTO hs_pipe_stages (
#             stage_id, stage_label, pipe_id
#         )
#         VALUES %s
#         ON CONFLICT (stage_id) DO UPDATE SET
#             stage_label = EXCLUDED.stage_label,
#             pipe_id = EXCLUDED.pipe_id
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