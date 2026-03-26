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
    "SPX": ["spx", "s&p", "sp 500", "s&p500", "es1", "es future"],
    "SPY": ["spy"],
    "VIX": ["vix", "vix cash", "vix open", "vix ouverture"],
    "VIX1D": ["vix1d", "vix 1d"],
    "Or+pétrole": ["or+pétrole", "or+petrole", "gold+oil", "gold oil", "or", "gold", "pétrole", "petrole", "oil"],
}

# Tu pourras me donner les cas ambigus plus tard, on les fixera ici.
MANUAL_TZ_OVERRIDES = {
    "VIX_9H30_CET_SPX_OPENING_daily.csv": "Europe/Paris",
}

TZ_PATH_HINTS = {
    "Europe/Paris": ["cet", "paris", "9h30_cet"],
    "America/New_York": ["new_york", "ny", "_et", " et ", "opening"],
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
    elif any(x in t for x in ["absolu", "absolute", "abs", "move", "mouvement"]):
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
        "unknown": None,
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

def infer_timezone(row):
    fn = str(row["file_name"])
    rel = str(row["relative_path"]).lower()
    cat_tz = str(row["tz_guess"])

    if fn in MANUAL_TZ_OVERRIDES:
        return MANUAL_TZ_OVERRIDES[fn]

    if cat_tz and cat_tz != "unknown":
        return cat_tz

    for tz, hints in TZ_PATH_HINTS.items():
        if any(h in rel for h in hints):
            return tz

    return "unknown"

def score_dataset_row(row, parsed):
    freq = str(row["freq_guess"])
    freq_minutes = freq_to_minutes(freq)
    tz = infer_timezone(row)

    base = 0
    if freq == "1min":
        base = 100
    elif freq == "5min":
        base = 85
    elif freq == "30min":
        base = 65
    elif freq == "1h":
        base = 55
    elif freq == "daily":
        base = 40
    else:
        base = 10

    threshold = parsed.get("threshold")
    horizon_minutes = parsed.get("horizon_minutes")

    if horizon_minutes is not None and freq_minutes is not None:
        if freq_minutes > horizon_minutes:
            base -= 200
        else:
            base += 15

    if tz != "unknown":
        base += 3

    return base

def build_candidates(cleaned, assets, parsed):
    rows = []
    for asset in assets:
        sub = cleaned[cleaned["asset"] == asset].copy()
        if len(sub) == 0:
            rows.append({
                "asset": asset,
                "status": "missing",
                "file_name": None,
                "freq_guess": None,
                "tz": None,
                "relative_path": None,
                "score": None,
            })
            continue

        sub["resolved_tz"] = sub.apply(infer_timezone, axis=1)
        sub["score"] = sub.apply(lambda r: score_dataset_row(r, parsed), axis=1)
        sub = sub.sort_values(by=["score", "size_bytes"], ascending=[False, False])

        best = sub.iloc[0]
        rows.append({
            "asset": asset,
            "status": "ok" if best["score"] > -100 else "insufficient_granularity",
            "file_name": best["file_name"],
            "freq_guess": best["freq_guess"],
            "tz": best["resolved_tz"],
            "relative_path": best["relative_path"],
            "score": int(best["score"]),
        })
    return pd.DataFrame(rows)

def compute_asset_result(asset, candidate_row, parsed):
    if candidate_row["status"] != "ok":
        return {
            "asset": asset,
            "status": candidate_row["status"],
            "message": "No dataset with sufficient granularity for this question.",
        }

    df, sep_mode = load_real_csv(candidate_row["file_name"])
    if df is None:
        return {
            "asset": asset,
            "status": "unreadable",
            "message": "Dataset could not be loaded on HF.",
        }

    price_candidates = guess_price_columns(df.columns)
    if len(price_candidates) == 0:
        return {
            "asset": asset,
            "status": "no_price_column",
            "message": "No price-like column detected.",
        }

    price_col = price_candidates[0]
    if "close" in [str(c).lower() for c in price_candidates]:
        price_col = price_candidates[[str(c).lower() for c in price_candidates].index("close")]

    freq_minutes = freq_to_minutes(candidate_row["freq_guess"])
    horizon_minutes = parsed.get("horizon_minutes")
    if horizon_minutes is not None and freq_minutes is not None:
        if freq_minutes > horizon_minutes:
            return {
                "asset": asset,
                "status": "insufficient_granularity",
                "message": f"{candidate_row['file_name']} is too coarse for {horizon_minutes} minutes.",
            }
        horizon_steps = max(1, int(round(horizon_minutes / freq_minutes)))
    else:
        horizon_steps = 1

    try:
        price_series = pd.to_numeric(df[price_col], errors="coerce")
    except Exception:
        return {
            "asset": asset,
            "status": "bad_price_column",
            "message": "Selected price column is not numeric.",
        }

    move_mode = parsed.get("move_mode") or "absolute"
    threshold = parsed.get("threshold")
    direction = parsed.get("direction") or "up"

    if threshold is None:
        threshold = 5.0 if move_mode == "absolute" else 0.2

    future_price = price_series.shift(-horizon_steps)
    if move_mode == "absolute":
        move = future_price - price_series
    else:
        move = (future_price - price_series) / price_series * 100.0

    valid = move.notna()

    if direction == "up":
        cond = move > threshold
    elif direction == "down":
        cond = move < -threshold
    else:
        cond = move.abs() > threshold

    total = int(valid.sum())
    success = int((cond & valid).sum())
    prob = (success / total) if total > 0 else 0.0

    time_col = guess_time_column(df.columns)
    if time_col:
        try:
            ts = pd.to_datetime(df[time_col], errors="coerce")
            min_ts = str(ts.min())
            max_ts = str(ts.max())
        except Exception:
            min_ts = None
            max_ts = None
    else:
        min_ts = None
        max_ts = None

    return {
        "asset": asset,
        "status": "ok",
        "file_name": candidate_row["file_name"],
        "freq_guess": candidate_row["freq_guess"],
        "tz": candidate_row["tz"],
        "price_column": price_col,
        "separator": sep_mode,
        "move_mode": move_mode,
        "direction": direction,
        "threshold": threshold,
        "horizon_minutes": horizon_minutes,
        "horizon_steps": horizon_steps,
        "total_samples": total,
        "success": success,
        "probability": round(prob, 4),
        "time_min": min_ts,
        "time_max": max_ts,
    }

def format_llm_answer(question, parsed, results):
    lines = []
    lines.append("### Answer")
    lines.append("")
    lines.append(f"I interpreted your question as: **{question}**.")
    lines.append("")
    lines.append("I evaluated the request on the **entire history of each selected dataset**, not on a single date.")
    lines.append("")

    for r in results:
        asset = r["asset"]
        if r["status"] == "ok":
            mode_label = "points" if r["move_mode"] == "absolute" else "%"
            horizon_text = f"{r['horizon_minutes']} minutes" if r["horizon_minutes"] is not None else f"{r['horizon_steps']} rows"
            direction_text = {"up": "up move", "down": "down move", "abs": "absolute move"}[r["direction"]]
            lines.append(
                f"**{asset}** — using `{r['file_name']}` ({r['freq_guess']}, {r['tz']}). "
                f"Probability of a **{direction_text} > {r['threshold']} {mode_label}** over **{horizon_text}**: "
                f"**{r['probability']:.2%}** "
                f"({r['success']} successes over {r['total_samples']} valid observations)."
            )
        else:
            lines.append(
                f"**{asset}** — I cannot answer this cleanly with the currently selected datasets: {r['message']}"
            )

    lines.append("")
    lines.append("### Interpretation notes")
    lines.append("")
    lines.append("- The result is computed over the **full available history of the chosen dataset**.")
    lines.append("- It is **not** restricted to the open, a single day, or a conditional event yet.")
    lines.append("- Multi-asset **joint conditions** will be the next step; this version computes one result **per asset**.")
    return "\n".join(lines)

catalog = load_catalog()
cleaned = clean_catalog(catalog)

if len(cleaned) == 0:
    st.warning("No canonical datasets found")
    st.stop()

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
candidates = build_candidates(cleaned, final_assets, parsed)

results = []
for _, cand in candidates.iterrows():
    results.append(compute_asset_result(cand["asset"], cand, parsed))

st.markdown(format_llm_answer(question, parsed, results))

with st.expander("Datasets used", expanded=False):
    st.dataframe(candidates, width="stretch")

with st.expander("Query parsing details", expanded=False):
    st.write(parsed)
    st.write("Detected assets:", detected_assets)
    st.write("Final assets:", final_assets)

with st.expander("Catalog browser", expanded=False):
    st.write("Selected catalog rows:", len(catalog))
    st.write("Canonical rows:", len(cleaned))
    st.dataframe(
        cleaned[["asset", "file_name", "relative_path", "size_bytes", "freq_guess", "tz_guess"]].head(300),
        width="stretch"
    )
