import psycopg2
from credentials import db_password
import pandas as pd

# # Conectar a la base de datos
# conn_details = psycopg2.connect(
#     host="localhost",
#     database="tu_basede_datos",
#     user="tu_usuario",
#     password=db_password,
#     port=5432
# )

# cursor = conn_details.cursor()

# # Recorre las filas del DataFrame y ejecuta consultas INSERT o UPDATE
# for index, row in df_wks.iterrows():
#     deal_id = row["Deal ID"]
#     deal_name = row["Deal Name"]
#     create_date = row["Create Date"] if row['Create Date'] else '1997-01-01'
#     close_date = row["Close Date"] if row['Close Date'] else '1997-01-01'
#     pipeline = row["Pipeline"]
#     deal_stage = row["Deal Stage"]
#     deal_owner = row["Deal Owner"]
#     billing_type = row["Billing Type"]
#     guest_type = row["Guest Type"]
#     check_in_date = row["Check in Date"] if row["Check in Date"] else '1997-01-01'
#     check_out_date = row["Check out Date"] if row["Check out Date"] else '1997-01-01'
#     lenght_of_stay = row["Lenght of Stay"] if row["Lenght of Stay"] else '0'
#     budget = row["Monthly Rent"]
#     apt_of_interest = row["Apartment of Interest"]
#     apt_booked = row["Apartment Booked"]
#     neighborhood = row["Neighborhood"]
#     purpose_of_rental = row["Purpose of Rental"]
#     company_sponsored = row["Company Sponsored"]
#     pet_fee_required = row["Pet Friendly Apt. Required"]
#     source = row["Deal Source"]
#     source_drilldown = row["Deal Source Drill Down"]
#     contact_email = row["Associated Contact Email"]
#     contact_phone = row["Associated Contact Phone"]
#     timestamp = row["Timestamp"]
#     inbound_outbound = row["b2b_source"]

#     # Verifica si el deal_id existe en la tabla hs_deals_q4
#     cursor.execute(f"SELECT * FROM hs_deals_q4 WHERE deal_id = {deal_id}")
#     existing_deal = cursor.fetchone()

#     if existing_deal:
#         # Si existe, actualiza la fila
#         update_query = f'''
#             UPDATE hs_deals_q4
#             SET deal_name = '{deal_name}',
#                 create_date = '{create_date}',
#                 close_date = '{close_date}',
#                 pipeline = '{pipeline}',
#                 deal_stage = '{deal_stage}',
#                 deal_owner = '{deal_owner}',
#                 billing_type = '{billing_type}',
#                 guest_type = '{guest_type}',
#                 check_in_date = '{check_in_date}',
#                 check_out_date = '{check_out_date}',
#                 lenght_of_stay = {lenght_of_stay},
#                 budget = '{budget}',
#                 apt_of_interest = '{apt_of_interest}',
#                 apt_booked = '{apt_booked}',
#                 neighborhood = '{neighborhood}',
#                 purpose_of_rental = '{purpose_of_rental}',
#                 company_sponsored = '{company_sponsored}',
#                 pet_fee_required = '{pet_fee_required}',
#                 source = '{source}',
#                 source_drilldown = '{source_drilldown}',
#                 contact_email = '{contact_email}',
#                 contact_phone = '{contact_phone}',
#                 timestamp = '{timestamp}',
#                 inbound_outbound = '{inbound_outbound}'
#             WHERE deal_id = {deal_id};
#         '''
#         cursor.execute(update_query)
#         conn_details.commit()
#         print(f"Se actualizó la fila {index + 1} en la tabla hs_deals_q4. ID: {deal_id}")
#     else:
#         # Si no existe, inserta una nueva fila
#         insert_query = f'''
#             INSERT INTO hs_deals_q4 (deal_id, deal_name, create_date, close_date, pipeline, deal_stage, deal_owner,
#                                      billing_type, guest_type, check_in_date, check_out_date, lenght_of_stay,
#                                      budget, apt_of_interest, apt_booked, neighborhood, purpose_of_rental,
#                                      company_sponsored, pet_fee_required, source, source_drilldown, contact_email,
#                                      contact_phone, timestamp, inbound_outbound)
#             VALUES ({deal_id}, '{deal_name}', '{create_date}', '{close_date}', '{pipeline}', '{deal_stage}',
#                     '{deal_owner}', '{billing_type}', '{guest_type}', '{check_in_date}', '{check_out_date}',
#                     {lenght_of_stay}, '{budget}', '{apt_of_interest}', '{apt_booked}', '{neighborhood}',
#                     '{purpose_of_rental}', '{company_sponsored}', '{pet_fee_required}', '{source}',
#                     '{source_drilldown}', '{contact_email}', '{contact_phone}', '{timestamp}',
#                     '{inbound_outbound}');
#         '''
#         cursor.execute(insert_query)
#         conn_details.commit()
#         print(f"Se insertó la fila {index + 1} en la tabla hs_deals_q4. ID: {deal_id}")

# cursor.close()
# conn_details.close()
