import streamlit as st
import pandas as pd
import requests
import re

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

DRIVE_FOLDER_URL = "https://drive.google.com/drive/folders/1szXsXMH_SfrLn6dBF2DQ5mhJ8LpN7dZ_?usp=drive_link"

def extract_files(folder_url):
    try:
        html = requests.get(folder_url, timeout=20).text
        
        ids = re.findall(r'([a-zA-Z0-9_-]{25,})', html)
        names = re.findall(r'aria-label="([^"]+)"', html)

        files = []
        for i, fid in enumerate(ids[:len(names)]):
            files.append((names[i], fid))
        
        return files
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
files = extract_files(DRIVE_FOLDER_URL)

st.write("Files detected:", len(files))

if len(files) == 0:
    st.warning("No files detected")
    st.stop()

file_names = [f[0] for f in files]
selected_name = st.selectbox("Select dataset", file_names)

selected_id = dict(files)[selected_name]

if st.button("Load file"):
    df = load_csv(selected_id)
    if df is None:
        st.error("Failed to load file")
    else:
        st.success(f"{selected_name} loaded")
        st.dataframe(df.head())
