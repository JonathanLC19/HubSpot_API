import streamlit as st
import os
import requests
import json
import openai
from dotenv import load_dotenv

load_dotenv()
openai_key = os.environ['openai_key']
GPT_MODEL = "gpt-4o"
openai.api_key = openai_key

def getWF(wf_id, hs_id):
    wf_id = str(wf_id)
    url = f"https://api.hubapi.com/automation/v4/flows/{wf_id}"

    headers = {
        'accept': "application/json",
        'authorization': f"Bearer {hs_id}"
    }

    response = requests.request("GET", url, headers=headers)
    resp_js = response.json()

    return resp_js

summary = """read and understand the context of this HubSpot workflow API responses I'm going to share. 
            Give me a summary of the process this workflow executes. Don't you split it into the different workflow actions/branches.
            Just give me a description of the process the workflow is related to.
            Always begin the response with the workflow name."""

schema = """read and understand the context of this HubSpot workflow API responses I'm going to share. 
                Give me a description of the process this workflow executes. Split it into the different workflow actions/branches following this schema:
                - Head 1: Trigger
                - Head 2: Action
                - Bulletpoints: Actions description
                - Head 3: Sub-Actions (if applies)
                - Bulletpoints: Sub-Actions description (if applies)
                If there are no sub-actions, don't show them. If there are more than one sub-action, follow the same schema.
                Nest the sub-actions under the parent action.
                Always begin the response with the workflow name."""
def wf_summary(wf, system):
    summary = openai.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(wf)}
        ],
        max_tokens=1000
    )

    return summary.choices[0].message.content

def main():
    st.title("Workflow Summary")

    wf_id = st.text_input("Enter Workflow ID:")
    hs_id = st.selectbox("Enter HubSpot API Key:",
                         ("hs_prod_key", "hs_sb_key"),
                         index=None,
                         placeholder="Select HubSpot account..."
                         )
    
    system_content = st.selectbox("Enter Respone type:",
                         ("summary", "schema"),
                         index=None,
                         placeholder="Select Respone type..."
                         )

    with st.spinner('🪄 Making Magic happen...'):
        if st.button("Get Summary"):
            data = getWF(wf_id, os.environ[hs_id])
            summary = wf_summary(data, summary if system_content == "summary" else schema)
            st.write(summary)

if __name__ == "__main__":
    main()

wf_schema_example = """
Name: "Le Figaró V.1. Paris | Zapier Create Deal".

1. Overview
ID & Revision Details: The workflow has a unique identifier (id) and a revisionId indicating its version.
Status: The isEnabled property indicates that the workflow is currently active (true).
Name: The workflow is named "Le Figaró V.1. Paris | Zapier Create Deal".
Timestamps: The workflow was creted on 2022-08-31 and last updated on 2022-08-31.

2. Workflow Structure
Enrollment Criteria:
The workflow supports manual enrollment with the option to re-enroll contacts.
Properties Handled:
There are several properties being set in deals such as:
dealname, dealstage, booking_city, etc.
These involve both static values and object properties derived from the enrolled object (like firstname, apartment_of_interest, etc.).

2. Action Details
Branching Actions:

There are three main branches determining the next action based on filtering criteria concerning deals:
Branch 1 - Deal Active: This branch checks if deals are active based on dealstage and num_associated_deals.
    Branch 1 actions - (triggered if active deals exist): Sends a notification to a specific user with the details of the deal.

Branch 2 - Deal in Closed but NO ACTIVE DEALS: This checks for deals that are closed yet have no active deals associated.
    Branch 2 actions - (triggered if there are closed deals but no active ones): Creates a new deal with specific properties.

Branch 3 - Deal No Exists: This branch captures cases where no deals exist.
    Branch 3 actions - (action to log a timestamp): Records when the workflow executed.

Subsequent Actions:

Based on the evaluation of branches:
Action ID 2 (triggered if active deals exist): Sends a notification to a specific user with the details of the deal.
Action ID 3 (triggered if there are closed deals but no active ones): Creates a new deal with specific properties.
Action ID 4 (action to log a timestamp): Records when the workflow executed.
Enrollment Criteria
The workflow supports manual enrollment with the option to re-enroll contacts.
Properties Handled
There are several properties being set in deals such as:
dealname, dealstage, booking_city, etc.
These involve both static values and object properties derived from the enrolled object (like firstname, apartment_of_interest, etc.).
Conclusion
This workflow is designed to manage deal creation based on interactive criteria and automates notifications concerning the deal statuses. 
It would be beneficial for sales or customer service teams handling deal management in a structured way within the CRM environment.
"""