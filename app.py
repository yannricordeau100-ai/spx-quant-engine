python3 - <<'PY'
content = """import streamlit as st
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
    try:
        return pd.read_csv(CATALOG_PATH)
    except:
        return pd.DataFrame(columns=["file_name", "relative_path"])

def filter_catalog(df):
    if len(df) == 0:
        return df

    df = df.copy()

    for k in EXCLUDE_KEYWORDS:
        df = df[~df["file_name"].astype(str).str.contains(k, case=False, na=False)]

    mask = False
    for asset in TARGET_ASSETS:
        mask = mask | df["relative_path"].astype(str).str.contains(asset, case=False, na=False)

    return df[mask]

catalog = load_catalog()
filtered = filter_catalog(catalog)

st.write("Total catalog:", len(catalog))
st.write("Filtered (target assets):", len(filtered))

if len(filtered) == 0:
    st.warning("No matching datasets")
    st.stop()

selected = st.selectbox("Select dataset", filtered["file_name"].values)
row = filtered[filtered["file_name"] == selected].iloc[0]

st.write("Selected path:", row["relative_path"])
st.dataframe(filtered.head(200), width='stretch')
"""
with open("app.py", "w", encoding="utf-8") as f:
    f.write(content)
print("app.py rewritten")
PY
