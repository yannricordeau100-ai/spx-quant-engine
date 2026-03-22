import os, json
import streamlit as st

ROOT=os.path.dirname(os.path.dirname(__file__))
PROJECT_ROOT=os.path.dirname(ROOT)
TRACE_JSON=os.path.join(PROJECT_ROOT,"processed","LAST_QUERY_SOURCE_TRACE.json")

st.set_page_config(page_title="Source Trace", page_icon="🧭", layout="wide")
st.title("Source Trace")

if os.path.exists(TRACE_JSON):
    try:
        with open(TRACE_JSON,"r",encoding="utf-8") as f:
            data=json.load(f)
        st.json(data)
    except Exception as e:
        st.error(repr(e))
else:
    st.info("Aucune trace disponible pour le moment.")
