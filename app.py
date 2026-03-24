import streamlit as st
import pandas as pd
import os

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

CATALOG_PATH = "data/selected_catalog.csv"
LOCAL_ROOT = "/Users/yann/Library/CloudStorage/GoogleDrive-yannricordeau100@gmail.com/Mon Drive/IA"

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

@st.cache_data
def load_real_csv(relative_path):
    full_path = os.path.join(LOCAL_ROOT, relative_path)
    if not os.path.exists(full_path):
        return None
    try:
        return pd.read_csv(full_path)
    except Exception:
        return None

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
st.dataframe(
    view[["asset", "file_name", "relative_path", "size_bytes", "freq_guess", "tz_guess"]].head(300),
    width="stretch"
)

if len(view) == 0:
    st.stop()

selected_file = st.selectbox("Dataset preview", view["file_name"].tolist())
row = view[view["file_name"] == selected_file].iloc[0]

st.subheader("Dataset summary")
st.write({
    "asset": row["asset"],
    "file_name": row["file_name"],
    "relative_path": row["relative_path"],
    "size_bytes": int(row["size_bytes"]) if pd.notna(row["size_bytes"]) else None,
    "freq_guess": row["freq_guess"],
    "tz_guess": row["tz_guess"],
})

st.subheader("Dataset structure preview (LOCAL TEST)")

df = load_real_csv(row["relative_path"])

if df is None:
    st.error("File not accessible on HF server. This is expected for now.")
else:
    st.success("Loaded real CSV")
    st.write("Shape:", df.shape)
    st.write("Columns:", list(df.columns))

    time_col = None
    for c in df.columns:
        cl = str(c).lower()
        if "time" in cl or "date" in cl:
            time_col = c
            break

    if time_col:
        try:
            ts = pd.to_datetime(df[time_col], errors="coerce")
            st.write("Time column:", time_col)
            st.write("Min:", ts.min())
            st.write("Max:", ts.max())
        except Exception:
            pass

    st.write("Head")
    st.dataframe(df.head(50), width="stretch")

    st.write("Tail")
    st.dataframe(df.tail(50), width="stretch")
