import streamlit as st
import pandas as pd
import os
import re

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

ASSET_ALIASES = {
    "SPX": ["spx", "s&p", "s&p500", "sp 500", "es1", "es future", "spooz"],
    "SPY": ["spy"],
    "VIX": ["vix", "vix cash", "vix open", "vix ouverture"],
    "VIX1D": ["vix1d", "vix 1d", "1-day vix"],
    "Or+pétrole": ["or+pétrole", "or+petrole", "gold+oil", "gold oil", "or", "petrole", "pétrole", "oil"],
}

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

def detect_assets_from_query(text):
    t = str(text).lower()
    found = []
    for asset, aliases in ASSET_ALIASES.items():
        if any(alias in t for alias in aliases):
            found.append(asset)
    # preserve project order
    ordered = [a for a in ASSET_ORDER if a in found]
    return ordered[:5]

def parse_simple_query(text):
    t = str(text).lower().strip()
    if not t:
        return {}

    direction = None
    if any(x in t for x in ["hausse", "up", "monte", "rise"]):
        direction = "up"
    elif any(x in t for x in ["baisse", "down", "drop", "chute"]):
        direction = "down"
    elif any(x in t for x in ["absolu", "absolute", "abs", "bouge", "move"]):
        direction = "abs"

    move_mode = "absolute"
    if "%" in t or "percent" in t or "pourcent" in t:
        move_mode = "percent"

    threshold = None
    m_threshold_pct = re.search(r'(\d+(?:[.,]\d+)?)\s*%', t)
    if m_threshold_pct:
        threshold = float(m_threshold_pct.group(1).replace(",", "."))
        move_mode = "percent"
    else:
        m_threshold = re.search(r'(\d+(?:[.,]\d+)?)', t)
        if m_threshold:
            threshold = float(m_threshold.group(1).replace(",", "."))

    horizon_minutes = None
    m_horizon = re.search(r'(\d+)\s*(min|minute|minutes|h|heure|heures|day|daily|jour|jours)', t)
    if m_horizon:
        value = int(m_horizon.group(1))
        unit = m_horizon.group(2)
        if unit.startswith("min"):
            horizon_minutes = value
        elif unit in ["h", "heure", "heures"]:
            horizon_minutes = value * 60
        elif unit in ["day", "daily", "jour", "jours"]:
            horizon_minutes = value * 1440

    condition_flag = any(x in t for x in ["si ", "when ", "condition", "à condition", "if "])

    return {
        "direction": direction,
        "move_mode": move_mode,
        "threshold": threshold,
        "horizon_minutes": horizon_minutes,
        "condition_flag": condition_flag,
        "date_scope": "entire selected dataset history",
    }

def score_dataset_row(row):
    score = 0
    fn = str(row["file_name"]).lower()
    freq = str(row["freq_guess"]).lower()
    tz = str(row["tz_guess"]).lower()

    if "1min" in freq:
        score += 50
    elif "5min" in freq:
        score += 35
    elif "30min" in freq:
        score += 20
    elif "daily" in freq:
        score += 10

    if "new_york" in tz or "unknown" in tz:
        score += 5

    if "future" not in fn:
        score += 3

    return score

def build_candidates(cleaned, assets):
    rows = []
    for asset in assets:
        sub = cleaned[cleaned["asset"] == asset].copy()
        if len(sub) == 0:
            rows.append({
                "asset": asset,
                "status": "missing",
                "file_name": None,
                "freq_guess": None,
                "tz_guess": None,
                "relative_path": None,
                "score": None,
            })
            continue

        sub["score"] = sub.apply(score_dataset_row, axis=1)
        sub = sub.sort_values(by=["score", "size_bytes"], ascending=[False, False])

        best = sub.iloc[0]
        rows.append({
            "asset": asset,
            "status": "ok",
            "file_name": best["file_name"],
            "freq_guess": best["freq_guess"],
            "tz_guess": best["tz_guess"],
            "relative_path": best["relative_path"],
            "score": int(best["score"]),
        })
    return pd.DataFrame(rows)

catalog = load_catalog()
cleaned = clean_catalog(catalog)

if len(cleaned) == 0:
    st.warning("No canonical datasets found")
    st.stop()

st.subheader("Question")

question = st.text_input(
    "Ask in French or English",
    value="hausse > 5 points en 30 min sur SPX et VIX"
)

parsed = parse_simple_query(question)
detected_assets = detect_assets_from_query(question)

manual_assets = st.multiselect(
    "Manual asset override (optional)",
    options=ASSET_ORDER,
    default=detected_assets if len(detected_assets) > 0 else ["SPX"]
)

final_assets = manual_assets[:5]
candidates = build_candidates(cleaned, final_assets)

st.subheader("Result")
st.write({
    "question": question,
    "detected_assets": detected_assets,
    "final_assets": final_assets,
    "direction": parsed.get("direction"),
    "move_mode": parsed.get("move_mode"),
    "threshold": parsed.get("threshold"),
    "horizon_minutes": parsed.get("horizon_minutes"),
    "condition_flag": parsed.get("condition_flag"),
    "date_scope": parsed.get("date_scope"),
})

if len(candidates) > 0:
    st.dataframe(candidates, width="stretch")

with st.expander("Dataset browser details", expanded=False):
    st.write("Selected catalog rows:", len(catalog))
    st.write("Canonical rows:", len(cleaned))
    st.dataframe(
        cleaned[["asset", "file_name", "relative_path", "size_bytes", "freq_guess", "tz_guess"]].head(300),
        width="stretch"
    )

with st.expander("Query parsing details", expanded=False):
    st.write("Parsed query", parsed)
    st.write("Detected assets", detected_assets)
