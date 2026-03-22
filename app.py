import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

CATALOG_PATH = "data/selected_catalog.csv"
BASE_PATH = "/Users/yann/Library/CloudStorage/GoogleDrive-yannricordeau100@gmail.com/Mon Drive/IA (ancien)"

@st.cache_data
def load_catalog():
    if not os.path.exists(CATALOG_PATH):
        return pd.DataFrame()
    return pd.read_csv(CATALOG_PATH)

@st.cache_data
def load_csv(rel_path):
    full_path = os.path.join(BASE_PATH, rel_path)
    if not os.path.exists(full_path):
        return None
    try:
        return pd.read_csv(full_path)
    except:
        return None

catalog = load_catalog()

st.write("Selected catalog rows:", len(catalog))

if len(catalog) == 0:
    st.stop()

assets = sorted(catalog["asset"].dropna().unique())
asset = st.selectbox("Asset", assets)

view = catalog[catalog["asset"] == asset].copy()

q = st.text_input("Search dataset", "")
if q:
    ql = q.lower()
    view = view[
        view["file_name"].str.lower().str.contains(ql) |
        view["freq_guess"].str.lower().str.contains(ql)
    ]

st.write("Matched rows:", len(view))

if len(view) == 0:
    st.stop()

file = st.selectbox("Select file", view["file_name"].values)
row = view[view["file_name"] == file].iloc[0]

st.write("Path:", row["relative_path"])

if st.button("Load CSV"):
    df = load_csv(row["relative_path"])
    if df is None:
        st.error("File not found on server (normal on HF)")
    else:
        st.success("Loaded")
        st.write("Shape:", df.shape)
        st.dataframe(df.head(200), width="stretch")
