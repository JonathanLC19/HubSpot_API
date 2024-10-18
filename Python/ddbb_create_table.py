import psycopg2
from credentials import db_database, db_host, db_password, db_user

import psycopg2
from psycopg2 import Error


""" -------------------------- CREATE TABLE ----------------------------------"""
# try:
#     # Establecer conexión con la base de datos
#     connection = psycopg2.connect(
#         user=db_user,
#         password=db_password,
#         host=db_host,
#         database=db_database
#     )

#     # Crear un cursor para ejecutar comandos SQL
#     cursor = connection.cursor()

#     # Definir la sentencia SQL para crear la tabla hs_users
#     create_table_query = '''
#         CREATE TABLE hs_pipe_stages (
#             stage_id SERIAL PRIMARY KEY,
#             stage_label VARCHAR(100) UNIQUE NOT NULL,
#             pipe_id INT
#         );
#     '''

#     # Ejecutar la sentencia SQL
#     cursor.execute(create_table_query)
#     connection.commit()
#     print("La tabla hs_pipe_stages ha sido creada exitosamente.")

# except (Exception, Error) as error:
#     print("Error al conectar o crear la tabla:", error)

# finally:
#     # Cerrar el cursor y la conexión con la base de datos
#     if connection:
#         cursor.close()
#         connection.close()
#         print("Conexión cerrada.")


""" -------------------------- UPDATE COLUMN ----------------------------------"""
# try:
#     # Establecer conexión con la base de datos
#     connection = psycopg2.connect(
#         user=db_user,
#         password=db_password,
#         host=db_host,
#         database=db_database
#     )

#     # Crear un cursor para ejecutar comandos SQL
#     cursor = connection.cursor()

#      # Cambiar el tipo de datos de la columna deal_owner a integer
#     alter_column_query = '''
#         ALTER TABLE hs_pipe_stages DROP CONSTRAINT hs_pipe_stages_stage_label_key;
#     '''

#     # Ejecutar la sentencia SQL
#     cursor.execute(alter_column_query)
#     connection.commit()
#     print("La columna stage_label ha sido actualizada.")

# except (Exception, Error) as error:
#     print("Error al actualizar la columna stage_label:", error)

# finally:
#     # Cerrar el cursor y la conexión con la base de datos
#     if connection:
#         cursor.close()
#         connection.close()
#         print("Conexión cerrada.")

""" -------------------------- UPDATE FOREIGN KEY COLUMN ----------------------------------"""
# try:
#     # Establecer conexión con la base de datos
#     connection = psycopg2.connect(
#         user=db_user,
#         password=db_password,
#         host=db_host,
#         database=db_database
#     )

#     # Crear un cursor para ejecutar comandos SQL
#     cursor = connection.cursor()

#     # Limpiar los datos: asignar un valor predeterminado a los valores vacíos
#     update_query = '''
#         UPDATE hs_deals_q4_tests
#         SET deal_stage = DEFAULT
#         WHERE deal_stage = '';
#     '''

#     cursor.execute(update_query)
#     connection.commit()
#     print("Valores vacíos en la columna deal_stage han sido actualizados.")

#     # Cambiar el tipo de datos de la columna deal_owner a integer
#     alter_column_query = '''
#         ALTER TABLE hs_deals_q4_tests
#         ALTER COLUMN deal_stage TYPE INTEGER USING deal_stage::integer;
#     '''

#     # Ejecutar la sentencia SQL
#     cursor.execute(alter_column_query)
#     connection.commit()
#     print("El tipo de datos de la columna deal_stage ha sido cambiado a integer.")

# except (Exception, Error) as error:
#     print("Error al cambiar el tipo de datos de la columna deal_stage:", error)

# finally:
#     # Cerrar el cursor y la conexión con la base de datos
#     if connection:
#         cursor.close()
#         connection.close()
#         print("Conexión cerrada.")


""" -------------------------- CREATE COLUMN ----------------------------------"""
# try:
#     # Establecer conexión con la base de datos
#     connection = psycopg2.connect(
#         user=db_user,
#         password=db_password,
#         host=db_host,
#         database=db_database
#     )

#     # Crear un cursor para ejecutar comandos SQL
#     cursor = connection.cursor()

#     # Definir la sentencia SQL para crear la tabla hs_users
#     create_columns_query = '''
#         ALTER TABLE hs_deals_q4_tests
#         ADD COLUMN notes_last_updated TIMESTAMPTZ NULL,
#         ADD COLUMN last_activity_date___associated_contact TIMESTAMPTZ NULL;
#     '''

#     # Ejecutar la sentencia SQL
#     cursor.execute(create_columns_query)
#     connection.commit()
#     print("Las nuevas colunmas han sido creadas exitosamente.")

# except (Exception, Error) as error:
#     print("Error al conectar o crear la tabla:", error)

# finally:
#     # Cerrar el cursor y la conexión con la base de datos
#     if connection:
#         cursor.close()
#         connection.close()
#         print("Conexión cerrada.")
