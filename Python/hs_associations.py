import hubspot
from pprint import pprint
from hubspot.crm.associations.v4 import ApiException
# import pandas as pd
# import os.path
import time

from credentials import hs_prod_key, hs_sb_key
# from get_deals import df
# from get_deals import get_all_deals_df

# from google.auth.transport.requests import Request
# from google.oauth2.credentials import Credentials
# from google_auth_oauthlib.flow import InstalledAppFlow
# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError

# from concurrent.futures import ThreadPoolExecutor

# start = time.perf_counter()

# df = get_all_deals_df(hs_sb_key)
# rows = df.shape[0]

# # Filtra el DataFrame original
# new_df = df[df['pipeline'] == '40914676'].copy()

# end = time.perf_counter()
# ttime = end - start

# print(new_df)
# print(f'Tiempo total de ejecución para {rows} filas: {ttime} segundos')


"""---------------------------------------------- GET ASSOCIATIONS --------------------------------------------------------"""

# def associations(secret_key):
#     # HUBSPOT API
#     client = hubspot.Client.create(access_token=secret_key)

#     try:
#         api_response = client.crm.associations.v4.basic_api.get_page(object_type="agents", object_id="12323909826", to_object_type="tickets", limit=500, after=None)
#         # pprint(api_response)
#     except ApiException as e:
#         print("Exception when calling basic_api->get_page: %s\n" % e)
#     return api_response.results
# start = time.perf_counter()
# ass = associations(hs_sb_key)
# end = time.perf_counter()
# ttime = end - start
# print (ass)
# print(ttime)

# for i in ass:
#     print(i.to_object_id)

def get_tickets(key, agent_id):
    # HUBSPOT API
    client = hubspot.Client.create(access_token=key)
    after = None
    results = []

    while True:
        try:
            api_response = client.crm.associations.v4.basic_api.get_page(object_type="agents", object_id=agent_id, to_object_type="tickets", limit=500, after=after)
            results = (api_response.results)
            ticket_ids = [res.to_object_id for res in results]
            if not api_response.paging or not api_response.paging.next:
                break
            after = api_response.paging.next.after
        except ApiException as e:
            print("Exception when calling basic_api->get_page: %s\n" % e)
            break
    return ticket_ids # [Ticket(ticket.properties) for ticket in results]

pprint(get_tickets(hs_sb_key, '12325702586'))
"""---------------------------------------------- GET ASSOCIATIONS: ENHANCED EXECUTION TIME --------------------------------------------------------"""


# def associations(secret_key, df, batch_size=100):
#     # HUBSPOT API
#     client = hubspot.Client.create(access_token=secret_key)

#     # Crear un diccionario para almacenar los resultados
#     results_dict = {"deal_id": [], "company_id": []}

#     start_time = time.perf_counter()

#     def fetch_associations(row):
#         deal_id = row["hs_object_id"]
#         try:
#             api_response = client.crm.associations.v4.basic_api.get_page(object_type="deals", object_id=deal_id, to_object_type="companies", limit=500)
#             results = api_response.results
#             if results:
#                 company_id = results[0].to_object_id
#                 return deal_id, company_id
#         except ApiException as e:
#             print(f"Exception when calling basic_api->get_page for deal_id {deal_id}: {e}")
#         return None

#     for i in range(0, len(df), batch_size):
#         batch_df = df.iloc[i:i+batch_size]
#         results_list = batch_df.apply(fetch_associations, axis=1)
#         results_list = [result for result in results_list if result]

#         for deal_id, company_id in results_list:
#             results_dict["deal_id"].append(deal_id)
#             results_dict["company_id"].append(company_id)
#             pprint(f"The associated company ID for {deal_id} is {company_id}")

#     end_time = time.perf_counter()
#     elapsed_time = end_time - start_time
#     print(f'Tiempo total de ejecución: {elapsed_time} segundos')

#     return results_dict


# ass = associations(hs_prod_key, df)
# print (ass)


