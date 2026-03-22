import streamlit as st
import pandas as pd
import requests
import re

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

DRIVE_FOLDER_URL = "https://drive.google.com/drive/folders/1szXsXMH_SfrLn6dBF2DQ5mhJ8LpN7dZ_?usp=drive_link"

def extract_file_ids(folder_url):
    try:
        html = requests.get(folder_url, timeout=20).text
        ids = re.findall(r"[\"']([a-zA-Z0-9_-]{25,})[\"']", html)
        return sorted(list(set(ids)))
    except:
        return []

@st.cache_data
def load_csv(file_id):
    try:
        url = f"https://drive.google.com/uc?id={file_id}"
        return pd.read_csv(url)
    except:
        return None

st.write("Scanning Drive...")
file_ids = extract_file_ids(DRIVE_FOLDER_URL)
st.write("Files detected:", len(file_ids))

if len(file_ids) == 0:
    st.warning("No files detected (check Drive sharing and folder link)")
    st.stop()

selected_id = st.selectbox("Select file", file_ids)

if st.button("Load file"):
    df = load_csv(selected_id)
    if df is None:
        st.error("Failed to load file")
    else:
        st.success("Loaded")
        st.dataframe(df.head())
