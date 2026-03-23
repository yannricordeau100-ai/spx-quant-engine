import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

CATALOG_PATH = "data/selected_catalog.csv"

BAD_PATH_KEYWORDS = [
    "portable_backup_temp",
    "streamlit_community_cloud_pack",
    "exports/",
    "processed/",
    "derived/",
    "backup",
]

BAD_FILE_KEYWORDS = [
    "ratio__",
    "zscore_",
    "spread__",
    "rolling_ratio",
    "copie de",
]

ASSET_ORDER = ["SPX", "SPY", "VIX", "VIX1D", "Or+pétrole"]

@st.cache_data
def load_catalog():
    if not os.path.exists(CATALOG_PATH):
        return pd.DataFrame(columns=["asset", "file_name", "relative_path", "size_bytes", "freq_guess", "tz_guess"])
    try:
        return pd.read_csv(CATALOG_PATH)
    except Exception:
        return pd.DataFrame(columns=["asset", "file_name", "relative_path", "size_bytes", "freq_guess", "tz_guess"])

def clean_catalog(df):
    if len(df) == 0:
        return df

    out = df.copy()

    rp = out["relative_path"].astype(str).str.lower()
    fn = out["file_name"].astype(str).str.lower()

    bad_path_mask = False
    for k in BAD_PATH_KEYWORDS:
        bad_path_mask = bad_path_mask | rp.str.contains(k, na=False)

    bad_file_mask = False
    for k in BAD_FILE_KEYWORDS:
        bad_file_mask = bad_file_mask | fn.str.contains(k, na=False)

    out = out[~bad_path_mask]
    out = out[~bad_file_mask]

    out = out.sort_values(
        by=["asset", "file_name", "size_bytes"],
        ascending=[True, True, False]
    )

    out = out.drop_duplicates(subset=["asset", "file_name"], keep="first")
    return out.reset_index(drop=True)

catalog = load_catalog()
cleaned = clean_catalog(catalog)

st.write("Selected catalog rows:", len(catalog))
st.write("Canonical rows:", len(cleaned))

if len(cleaned) == 0:
    st.warning("No canonical datasets found")
    st.stop()

assets = [a for a in ASSET_ORDER if a in cleaned["asset"].astype(str).unique().tolist()]
selected_asset = st.selectbox("Asset", assets)

view = cleaned[cleaned["asset"] == selected_asset].copy()

freqs = ["ALL"] + sorted(view["freq_guess"].dropna().astype(str).unique().tolist())
selected_freq = st.selectbox("Frequency", freqs)

if selected_freq != "ALL":
    view = view[view["freq_guess"].astype(str) == selected_freq]

tzs = ["ALL"] + sorted(view["tz_guess"].dropna().astype(str).unique().tolist())
selected_tz = st.selectbox("Timezone", tzs)

if selected_tz != "ALL":
    view = view[view["tz_guess"].astype(str) == selected_tz]

q = st.text_input("Search dataset", "")
if q.strip():
    ql = q.strip().lower()
    mask = (
        view["file_name"].astype(str).str.lower().str.contains(ql, na=False) |
        view["relative_path"].astype(str).str.lower().str.contains(ql, na=False)
    )
    view = view[mask]

st.write("Matched rows:", len(view))
st.dataframe(view[["asset", "file_name", "relative_path", "size_bytes", "freq_guess", "tz_guess"]].head(300), width="stretch")
