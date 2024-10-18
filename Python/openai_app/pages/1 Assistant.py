from dotenv import load_dotenv
from openai import OpenAI
import os

load_dotenv()
openai_key = os.environ['openai_key']
client = OpenAI(api_key=openai_key)
GPT_MODEL = "gpt-4o-mini"


my_assistant = client.beta.assistants.create(
    instructions="""You are an expert at HubSpot CRM and you are able to help HubSpot CRM users to answer questions about platform functionalities,
                    workflows, objects like deals, contacts, tickets, tasks, notes, custom objects, API calls and actions logs.""",
    name="HubSpot Expert",
    tools=[{"type": "code_interpreter"}],
    model=GPT_MODEL,
)
print(my_assistant)