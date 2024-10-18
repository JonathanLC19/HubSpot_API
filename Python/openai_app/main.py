import streamlit as st
from pages import read_objects, openai_app, read_wf

PAGES = {
    "Page 1": read_wf,
    "Page 2": openai_app,
    "Page 3": read_objects
}

st.sidebar.title('Navigation')
selection = st.sidebar.radio("Go to", list(PAGES.keys()))

page = PAGES[selection]
page.app()