import streamlit as st
import pandas as pd
import os
import re
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
    out = out.sort_values(by=["asset", "file_name", "size_bytes"], ascending=[True, True, False])
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

def parse_simple_query(text):
    t = str(text).lower().strip()
    if not t:
        return {}

    direction = None
    if any(x in t for x in ["hausse", "up", "monte", "rise"]):
        direction = "up"
    elif any(x in t for x in ["baisse", "down", "drop", "chute"]):
        direction = "down"
    elif any(x in t for x in ["absolu", "absolute", "abs", "move"]):
        direction = "abs"

    move_mode = "absolute"
    if "%" in t or "percent" in t or "pourcent" in t:
        move_mode = "percent"

    m_threshold = re.search(r'(\d+(?:[.,]\d+)?)\s*%', t)
    if m_threshold:
        threshold = float(m_threshold.group(1).replace(",", "."))
        move_mode = "percent"
    else:
        m_threshold = re.search(r'([<>]=?|sup[ée]rieur|plus de|moins de)?\s*(\d+(?:[.,]\d+)?)', t)
        threshold = float(m_threshold.group(2).replace(",", ".")) if m_threshold else None

    m_horizon = re.search(r'(\d+)\s*(min|minute|minutes|h|heure|heures|day|daily|jour|jours)', t)
    horizon_minutes = None
    if m_horizon:
        value = int(m_horizon.group(1))
        unit = m_horizon.group(2)
        if unit.startswith("min"):
            horizon_minutes = value
        elif unit in ["h", "heure", "heures"]:
            horizon_minutes = value * 60
        elif unit in ["day", "daily", "jour", "jours"]:
            horizon_minutes = value * 1440

    return {
        "direction": direction,
        "move_mode": move_mode,
        "threshold": threshold,
        "horizon_minutes": horizon_minutes,
    }

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

df, sep_mode = load_real_csv(row["file_name"])
if df is None:
    st.error("CSV not found or unreadable in data/live_selected")
    st.stop()

st.subheader("Dataset structure preview (REAL ON HF)")
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
st.dataframe(df.head(20), width="stretch")

st.subheader("Simple Query Engine v3")

question = st.text_input(
    "Natural language query",
    value="hausse > 5 points en 30 min"
)
parsed = parse_simple_query(question)

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

default_move_mode = parsed.get("move_mode") or "absolute"
move_mode = st.selectbox("Move mode", ["absolute", "percent"], index=0 if default_move_mode == "absolute" else 1)

if freq_minutes is not None and freq_minutes < 1440:
    minute_options = [5, 10, 15, 30, 60, 120]
    default_horizon = parsed.get("horizon_minutes") if parsed.get("horizon_minutes") in minute_options else 30
    horizon_minutes = st.selectbox("Horizon (minutes)", minute_options, index=minute_options.index(default_horizon))
    horizon_steps = max(1, int(round(horizon_minutes / freq_minutes)))
else:
    horizon_minutes = None
    row_options = [1, 2, 3, 5, 10]
    horizon_steps = st.selectbox("Horizon (rows)", row_options, index=1)

default_threshold = parsed.get("threshold") if parsed.get("threshold") is not None else 5.0
threshold = st.number_input("Threshold", value=float(default_threshold), min_value=0.0)

default_direction = parsed.get("direction") if parsed.get("direction") in ["up", "down", "abs"] else "up"
direction = st.selectbox("Direction", ["up", "down", "abs"], index=["up", "down", "abs"].index(default_direction))

st.write("Parsed query", parsed)

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
st.dataframe(df_q[preview_cols].head(30), width="stretch")
