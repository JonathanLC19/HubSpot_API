import os

import pandas as pd

from pandasai import Agent
from pandasai.responses.streamlit_response import StreamlitResponse
import credentials as creds
import functions as func

import streamlit as st

import functions as func


st.title("🦾 AI Test APP")

pandasai_key = creds.pandasai_key

def deals():
    # Initialize an empty DataFrame to hold all results
    df2 = pd.DataFrame()

    # Call search_deals with your HubSpot API key and df2
    # df2 will be updated with results for each month from 2023-10-01 until current month
    df2 = func.search_deals(creds.hs_prod_key, "2024-03-01", df2)
    return df2

deals_df = deals()
with st.expander("🔎 Employees Preview"):
    st.write(deals_df)

# Get your FREE API key signing up at https://pandabi.ai.
# You can also configure it in your .env file.
os.environ["PANDASAI_API_KEY"] = pandasai_key

query = st.text_area("🗣️ Chat with Dataframe")

if st.button("Generate", type='primary'):
    if query:
        with st.spinner("Generating response..."):
            agent = Agent(
                deals_df,
                config={"verbose": True, "response_parser": StreamlitResponse},
            )

            st.write(agent.chat(query))
    else: st.warning("Please, enter a query.")