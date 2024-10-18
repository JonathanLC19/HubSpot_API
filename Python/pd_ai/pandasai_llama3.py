from pandasai.llm.local_llm import LocalLLM
from pandasai.llm import OpenAI
import streamlit as st
import pandas as pd
from pandasai import SmartDataframe
from dotenv import load_dotenv
import os

import hubspot
from hubspot.crm.deals import BasicApi, ApiException, PublicObjectSearchRequest
import time
from dateutil import parser

load_dotenv()

openai_key = os.environ['openai_key']
hs_key = os.environ['hs_prod_key']

# model = LocalLLM(
#     api_base='http://127.0.0.1:11434/v1',
#     model='llama3'
# )

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

st.title("MCU Box Office")
df2 = pd.DataFrame()
data = search_deals(hs_key, "2024-04-01", df2)

st.write(data)

llm = OpenAI(openai_api_key=openai_key, model = "gpt-4o", max_tokens=1000)
df = SmartDataframe(data, config={'llm' : llm})
prompt = st.text_area("Enter your prompt:")

if st.button("Generate"):
    if prompt:
        with st.spinner("Generating response..."):
            st.write(df.chat(prompt))