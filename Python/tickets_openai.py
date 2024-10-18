from dotenv import load_dotenv, find_dotenv
import os
from pprint import pprint
import pandas as pd

from openai import OpenAI
import openai
from langchain_openai import ChatOpenAI
from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
# from langchain.agents import AgentType, initialize_agent, load_tools

from pydantic import BaseModel, Field

from hs_search_tickets import search_tickets

load_dotenv(find_dotenv())

hs_api = os.getenv('hs_prod_key')
openai.api_key = os.getenv("openai_key")
MODEL = "gpt-4o-2024-08-06"

def tickets(date):
    # Initialize an empty DataFrame to hold all results
    df2 = pd.DataFrame()

    # Call search_deals with your HubSpot API key and df2
    # df2 will be updated with results for each month from 2023-10-01 until current month
    df2 = search_tickets(hs_api, date, df2)
    return df2

tickets_df = tickets("2024-08-01")
# print(tickets_df.info())

llm = ChatOpenAI(model=MODEL, temperature=0, openai_api_key=openai.api_key)
agent = create_pandas_dataframe_agent(llm, tickets_df, agent_type="tool-calling", verbose=True)

agent.run("how many rows and columns are there in the dataset?")

# --------------------------------------------------------------

query = """
Analyze the ticket data and provide insights based on the following:
1. Average time between ticket creation and last modification across all tickets.
2. Count of tickets in each pipeline stage.
3. Identify the ticket owner with the most assigned tickets.
4. List all unique subjects for tickets handled by a specific owner (provided by owner ID).
"""

# --------------------------------------------------------------
# Providing a JSON Schema
# --------------------------------------------------------------

system_prompt = """
You are an AI assistant specialized in HubSpot CRM and ticket management. You will receive a query along with a set of ticket data. 
Your role is to analyze the ticket data and provide structured responses, including calculations, insights, and any relevant patterns or trends.

When responding, you should:
1. Parse the ticket data provided via the API.
2. Perform calculations such as averages, counts, and aggregations.
3. Identify key insights, such as trends in ticket stages, most active owners, or common subjects.
4. Structure your response clearly, with actionable insights that directly answer the query.

Always ensure your responses are accurate, data-driven, and aligned with the analysis requested in the query.
"""
