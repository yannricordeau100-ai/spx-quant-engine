cd ~/spx-quant-engine
cat > app.py <<'PY'
import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

CATALOG_PATH = "data/catalog.csv"

TARGET_ASSETS = ["SPX", "SPY", "VIX", "VIX1D", "Or+petrole", "Or+pétrole"]

EXCLUDE_KEYWORDS = [
    "ERROR",
    "TEST",
    "BAD",
    "TEMP"
]

@st.cache_data
def load_catalog():
    if not os.path.exists(CATALOG_PATH):
        return pd.DataFrame(columns=["file_name", "relative_path"])
    return pd.read_csv(CATALOG_PATH)

def filter_catalog(df):
    if len(df) == 0:
        return df

    df = df.copy()

    # filtre exclusion
    for k in EXCLUDE_KEYWORDS:
        df = df[~df["file_name"].str.contains(k, case=False, na=False)]

    # filtre actifs
    mask = False
    for asset in TARGET_ASSETS:
        mask = mask | df["relative_path"].str.contains(asset, case=False, na=False)

    df = df[mask]

    return df

catalog = load_catalog()
filtered = filter_catalog(catalog)

st.write("Total catalog:", len(catalog))
st.write("Filtered (target assets):", len(filtered))

if len(filtered) == 0:
    st.warning("No matching datasets")
    st.stop()

# sélection dataset
selected = st.selectbox(
    "Select dataset",
    filtered["file_name"].values
)

row = filtered[filtered["file_name"] == selected].iloc[0]

st.write("Selected path:", row["relative_path"])
PY
