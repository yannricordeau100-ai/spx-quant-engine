import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

CATALOG_PATH = "data/selected_catalog.csv"

@st.cache_data
def load_catalog():
    if not os.path.exists(CATALOG_PATH):
        return pd.DataFrame(columns=["asset", "file_name", "relative_path", "size_bytes", "freq_guess", "tz_guess"])
    try:
        return pd.read_csv(CATALOG_PATH)
    except Exception:
        return pd.DataFrame(columns=["asset", "file_name", "relative_path", "size_bytes", "freq_guess", "tz_guess"])

catalog = load_catalog()

st.write("Selected catalog rows:", len(catalog))

if len(catalog) == 0:
    st.warning("selected_catalog.csv not found")
    st.stop()

assets = sorted(catalog["asset"].dropna().astype(str).unique().tolist())
selected_asset = st.selectbox("Asset", assets)

view = catalog[catalog["asset"] == selected_asset].copy()

q = st.text_input("Search dataset", "")
if q.strip():
    ql = q.strip().lower()
    mask = (
        view["file_name"].astype(str).str.lower().str.contains(ql, na=False) |
        view["relative_path"].astype(str).str.lower().str.contains(ql, na=False) |
        view["freq_guess"].astype(str).str.lower().str.contains(ql, na=False) |
        view["tz_guess"].astype(str).str.lower().str.contains(ql, na=False)
    )
    view = view[mask]

st.write("Matched rows:", len(view))
st.dataframe(view.head(300), width="stretch")
