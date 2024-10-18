import streamlit as st
import os
import requests
import json
import openai
from dotenv import load_dotenv

load_dotenv()
openai_key = os.environ['openai_key']
GPT_MODEL = "gpt-4o-mini"
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

def wf_process(wf_ids, hs_id):
    process = []
    for wf_id in wf_ids:
        data = getWF(wf_id, hs_id)
        process.append(data)
    return process

def process_summary(wfs, user_input):
    summary = openai.chat.completions.create(
        model=GPT_MODEL,
        messages=[
            {"role": "system", "content": f"""You are a HubSpot platform expert. Read and understand the context of this content which is
                      related to a list of HubSpot Workflows API calls. All of them are related to the same process. Unite all of them with a summary of the process
                      related to the user input. Don't summarize the workflow actions and structure, just use them as the context to understand the process they stand for. The process is:{user_input}"""},
            {"role": "user", "content": json.dumps(wfs)}
        ],
        max_tokens=1000
    )

    return summary.choices[0].message.content

import streamlit as st
import os

def main():
    # Inicializar la lista de IDs de workflows en el estado de sesión si no existe
    if 'workflow_ids_list' not in st.session_state:
        st.session_state.workflow_ids_list = []

    # Input para el Workflow ID
    workflow_id = st.text_input("Workflow ID",
                                key="workflow_id_input",
                                value=st.session_state.workflow_id)

    # Botón para insertar el ID
    if st.button("Insert ID", key="insert_id"):
        if st.session_state.workflow_id not in st.session_state.workflow_ids_list and workflow_id:
            st.session_state.workflow_ids_list.append(workflow_id)

    # Mostrar la tabla de Workflow IDs con botones para eliminar
    st.write("### Workflow IDs List")
    for i, wf_id in enumerate(st.session_state.workflow_ids_list):
        cols = st.columns((3, 1))
        cols[0].write(wf_id)
        if cols[1].button("Eliminar", key=f"delete_{i}"):
            st.session_state.workflow_ids_list.pop(i)

    # Input para el HubSpot API Key y el prompt del usuario
    hs_id = st.selectbox("Enter HubSpot API Key:",
                         ("hs_prod_key", "hs_sb_key"),
                         index=None,
                         placeholder="Select HubSpot account..."
                         )
    user_input = st.text_input("Write your prompt:")

    # Botón para ejecutar el prompt
    with st.spinner('🪄 Making Magic happen...'):
        if st.button("Execute prompt", key="execute_prompt"):
            if hs_id in os.environ:  # Verificar si hs_id está en las variables de entorno
                data = wf_process(st.session_state.workflow_ids_list, os.environ[hs_id])
                st.write(process_summary(data, user_input))
            else:
                st.error("HubSpot API Key no encontrada en las variables de entorno.")

if __name__ == '__main__':
    main()
