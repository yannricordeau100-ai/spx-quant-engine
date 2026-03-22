import streamlit as st
import os
import pandas as pd

st.set_page_config(layout="wide")

st.title("SPX Quant Engine")

DATA_PATH = "./data"

@st.cache_data
def load_csvs():
    data = {}
    for root, dirs, files in os.walk(DATA_PATH):
        for f in files:
            if f.endswith(".csv"):
                path = os.path.join(root, f)
                try:
                    data[f] = pd.read_csv(path)
                except:
                    pass
    return data

data = load_csvs()

st.write("CSV loaded:", len(data))

if len(data) == 0:
    st.warning("No data loaded")
else:
    st.success("Data OK")

st.write(list(data.keys())[:20])