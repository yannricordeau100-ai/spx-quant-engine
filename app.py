import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

CATALOG_PATH = "data/catalog.csv"

@st.cache_data
def load_catalog():
    if not os.path.exists(CATALOG_PATH):
        return pd.DataFrame(columns=["file_name", "relative_path"])
    try:
        return pd.read_csv(CATALOG_PATH)
    except:
        return pd.DataFrame(columns=["file_name", "relative_path"])

catalog = load_catalog()

st.write("Catalog rows:", len(catalog))

if len(catalog) == 0:
    st.warning("No catalog loaded")
    st.stop()

q = st.text_input("Search dataset", "")
view = catalog.copy()

if q.strip():
    ql = q.strip().lower()
    mask = (
        view["file_name"].astype(str).str.lower().str.contains(ql, na=False) |
        view["relative_path"].astype(str).str.lower().str.contains(ql, na=False)
    )
    view = view[mask]

st.write("Matched rows:", len(view))
st.dataframe(view.head(200), use_container_width=True)
