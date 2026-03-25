import streamlit as st
import pandas as pd
import os
import numpy as np

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

CATALOG_PATH = "data/selected_catalog.csv"
LIVE_ROOT = "data/live_selected"

BAD_PATH_KEYWORDS = [
    "portable_backup_temp",
    "streamlit_community_cloud_pack",
    "exports/",
    "processed/",
    "derived/",
    "backup",
    "spx_open_engine_project/",
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

    bad_path_mask = pd.Series(False, index=out.index)
    for k in BAD_PATH_KEYWORDS:
        bad_path_mask = bad_path_mask | rp.str.contains(k, na=False)

    bad_file_mask = pd.Series(False, index=out.index)
    for k in BAD_FILE_KEYWORDS:
        bad_file_mask = bad_file_mask | fn.str.contains(k, na=False)

    out = out.loc[~bad_path_mask]
    out = out.loc[~bad_file_mask]

    out = out.sort_values(
        by=["asset", "file_name", "size_bytes"],
        ascending=[True, True, False]
    )

    out = out.drop_duplicates(subset=["asset", "file_name"], keep="first")
    return out.reset_index(drop=True)

def guess_time_column(cols):
    exact = ["time", "datetime", "date", "timestamp"]
    for c in cols:
        cl = str(c).lower()
        if cl in exact:
            return c
    for c in cols:
        cl = str(c).lower()
        if "time" in cl or "date" in cl:
            return c
    return None

def guess_price_columns(cols):
    preferred = ["close", "open", "high", "low", "price", "last"]
    out = []
    lower_map = {str(c).lower(): c for c in cols}
    for p in preferred:
        if p in lower_map:
            out.append(lower_map[p])
    for c in cols:
        cl = str(c).lower()
        if cl not in [str(x).lower() for x in out]:
            if any(k in cl for k in preferred):
                out.append(c)
    return out

def freq_to_minutes(freq_guess):
    m = {
        "1min": 1,
        "5min": 5,
        "15min": 15,
        "30min": 30,
        "1h": 60,
        "daily": 1440,
    }
    return m.get(str(freq_guess), None)

@st.cache_data
def load_real_csv(file_name):
    full_path = os.path.join(LIVE_ROOT, file_name)
    if not os.path.exists(full_path):
        return None, "missing"

    try:
        df = pd.read_csv(full_path, sep=None, engine="python")
        if df is not None and len(df.columns) > 1:
            return df, "auto"
    except Exception:
        pass

    for sep, label in [(";", "semicolon"), (",", "comma"), ("\t", "tab"), ("|", "pipe")]:
        try:
            df = pd.read_csv(full_path, sep=sep)
            if df is not None and len(df.columns) >= 1:
                return df, label
        except Exception:
            continue

    return None, "failed"

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

st.subheader("Dataset structure preview (REAL ON HF)")

df, sep_mode = load_real_csv(row["file_name"])

if df is None:
    st.error("CSV not found or unreadable in data/live_selected")
    st.stop()

st.success("Loaded real CSV")
st.write("Separator detection:", sep_mode)
st.write("Shape:", df.shape)
st.write("Columns:", list(df.columns))

time_col = guess_time_column(df.columns)
if time_col:
    try:
        ts = pd.to_datetime(df[time_col], errors="coerce")
        st.write("Time column:", time_col)
        st.write("Min:", ts.min())
        st.write("Max:", ts.max())
    except Exception:
        ts = None
else:
    ts = None

st.write("Head")
st.dataframe(df.head(50), width="stretch")

st.write("Tail")
st.dataframe(df.tail(50), width="stretch")

st.subheader("Simple Query Engine v2")

price_candidates = guess_price_columns(df.columns)
if len(price_candidates) == 0:
    st.warning("No price-like columns detected")
    st.stop()

default_price_idx = 0
if "close" in [str(c).lower() for c in price_candidates]:
    default_price_idx = [str(c).lower() for c in price_candidates].index("close")

price_col = st.selectbox("Price column", price_candidates, index=default_price_idx)

freq_guess = str(row["freq_guess"])
freq_minutes = freq_to_minutes(freq_guess)

if freq_minutes is not None and freq_minutes < 1440:
    horizon_minutes = st.selectbox("Horizon (minutes)", [5, 10, 15, 30, 60, 120], index=3)
    horizon_steps = max(1, int(round(horizon_minutes / freq_minutes)))
else:
    horizon_minutes = None
    horizon_steps = st.selectbox("Horizon (rows)", [1, 2, 3, 5, 10], index=1)

move_mode = st.selectbox("Move mode", ["absolute", "percent"])
threshold = st.number_input("Threshold", value=5.0, min_value=0.0)
direction = st.selectbox("Direction", ["up", "down", "abs"])

df_q = df.copy()

try:
    price_series = pd.to_numeric(df_q[price_col], errors="coerce")
except Exception:
    st.error("Selected price column is not numeric")
    st.stop()

df_q["future_price"] = price_series.shift(-horizon_steps)

if move_mode == "absolute":
    df_q["move"] = df_q["future_price"] - price_series
else:
    df_q["move"] = (df_q["future_price"] - price_series) / price_series * 100.0

valid = df_q["move"].notna()

if direction == "up":
    cond = df_q["move"] > threshold
elif direction == "down":
    cond = df_q["move"] < -threshold
else:
    cond = df_q["move"].abs() > threshold

total = int(valid.sum())
success = int((cond & valid).sum())
prob = (success / total) if total > 0 else 0.0

st.write({
    "dataset": row["file_name"],
    "price_column": price_col,
    "move_mode": move_mode,
    "direction": direction,
    "threshold": threshold,
    "horizon_steps": int(horizon_steps),
    "horizon_minutes": horizon_minutes,
    "total_samples": total,
    "success": success,
    "probability": round(prob, 4),
})

preview_cols = [price_col, "future_price", "move"]
if time_col and time_col in df_q.columns:
    preview_cols = [time_col] + preview_cols

st.write("Query preview")
st.dataframe(df_q[preview_cols].head(50), width="stretch")
