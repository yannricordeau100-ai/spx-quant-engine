import os, json
import pandas as pd
import streamlit as st

ROOT=os.path.dirname(os.path.dirname(__file__))
PROJECT_ROOT=os.path.dirname(ROOT)
HIST_CSV=os.path.join(PROJECT_ROOT,"processed","QUERY_HISTORY","query_history_index.csv")

st.set_page_config(page_title="Historique", page_icon="📚", layout="wide")
st.title("Historique")

if os.path.exists(HIST_CSV):
    try:
        df=pd.read_csv(HIST_CSV)
        st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(repr(e))
else:
    st.info("Aucun historique disponible pour le moment.")
