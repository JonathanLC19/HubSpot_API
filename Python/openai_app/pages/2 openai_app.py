import streamlit as st
import openai
from dotenv import load_dotenv
import os

with open('wf_api.txt', 'r') as f:
    wf_api = f.read()

with open('openai_app/pages/wf_api_endpoints.txt', 'r') as f:
    wf_endpoints = f.read()

with open('sb_contact_props.txt', 'r') as f:
    contact_props = f.read()

with open('sb_ticket_props.txt', 'r') as f:
    ticket_props = f.read()

with open('sb_deal_props.txt', 'r') as f:
    deal_props = f.read()

with open('sb_reservations_props.txt', 'r') as f:
    reservation_props = f.read()

load_dotenv()
openai_key = os.environ['openai_key']
GPT_MODEL = "gpt-4o"

openai.api_key = openai_key


code = """
        You are an AI assistant that is responsible for generating code based on human instructions. 
        You will be given information about the methods, functions, and classes closest to the user's position. 
        If the user's instruction does not explicitly refer to any particular target, 
        reply with code that is correct and functional based on the information provided. 
        If there is no clear way to follow the instruction, do not reply at all.
        """

agent = """
        You are an AI assistant that is responsible for generating answers based on human instructions. 
        You are an expert at HubSpot CRM and you are able to help HubSpot CRM users to answer questions about platform functionalities,
        workflows, objects like deals, contacts, tickets, tasks, notes, custom objects, API calls and actions logs.
        You will be given information about the user's request. 
        If the user's instruction does not explicitly refer to any particular target, 
        reply with a text that is correct and functional based on the information provided. 
        If there is no clear way to follow the instruction, do not reply at all.
        """
def chatbot(user_input, system_content):
    response = openai.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {"role": "system", "content": f"""
                {system_content}

                Contact properties: {contact_props}
                Ticket properties: {ticket_props}
                Deal properties: {deal_props}
                Reservation properties: {reservation_props}
                Workflow API Endpoints and Schemas information: {wf_endpoints}
                
            """},
            {"role": "user", "content": user_input}
        ],
        max_tokens=1000
    )

    return response.choices[0].message.content

def main():
    st.title("Chatbot")

    user_input = st.text_input("Type your message ")
    system_content = st.selectbox("Select an AI agent", ("HubSpot Developer", "HubSpot Consultant"), index=None)
    
    with st.spinner('Wait for it...'):
        if st.button("Send"):
            response = chatbot(user_input, code if system_content == "HubSpot Developer" else agent)
            st.write(f"Chatbot: {response}")

if __name__ == "__main__":
    main()
