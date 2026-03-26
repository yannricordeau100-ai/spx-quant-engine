import streamlit as st
import pandas as pd
import os
import re

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

CATALOG_PATH = "data/selected_catalog.csv"
LIVE_ROOT = "data/live_selected"

ASSET_ORDER = ["SPX", "SPY", "VIX", "VIX1D", "Or+pétrole"]

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
    "future",
]

ASSET_ALIASES = {
    "SPX": ["spx", "s&p", "sp 500", "s&p500"],
    "SPY": ["spy"],
    "VIX": ["vix", "vix cash", "vix open", "vix ouverture"],
    "VIX1D": ["vix1d", "vix 1d"],
    "Or+pétrole": ["or+pétrole", "or+petrole", "gold+oil", "gold oil", "or", "gold", "pétrole", "petrole", "oil"],
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
    return ordered

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
            if df is not None and len(df.columns) > 1:
                return df, label
        except Exception:
            continue

    return None, "failed"

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

def guess_close_column(cols):
    for c in cols:
        if str(c).lower() == "close":
            return c
    for c in cols:
        if "close" in str(c).lower():
            return c
    return None

def choose_best_dataset(asset, cleaned):
    sub = cleaned[cleaned["asset"] == asset].copy()
    if len(sub) == 0:
        return None

    # priorité stricte voulue pour le moteur actuel
    priority = {
        "1min": 100,
        "30min": 80,
        "daily": 60,
        "5min": 50,
        "1h": 40,
        "unknown": 0
    }

    sub["prio"] = sub["freq_guess"].astype(str).map(lambda x: priority.get(x, 0))
    sub = sub.sort_values(by=["prio", "size_bytes"], ascending=[False, False])

    return sub.iloc[0]

def parse_weekly_drop_question(text):
    t = str(text).lower()

    if "semaine" not in t and "week" not in t:
        return None

    if not any(x in t for x in ["baissé", "baisse", "down", "drop", "chute"]):
        return None

    m_pct = re.search(r'(\d+(?:[.,]\d+)?)\s*%', t)
    if not m_pct:
        return None

    threshold_pct = float(m_pct.group(1).replace(",", "."))

    m_times = re.search(r'(\d+)\s*fois', t)
    n_times = int(m_times.group(1)) if m_times else 2

    last_flag = any(x in t for x in ["dernière", "dernier", "last"])

    return {
        "type": "weekly_drop_count",
        "threshold_pct": threshold_pct,
        "n_times": n_times,
        "last_flag": last_flag,
    }

def answer_weekly_drop(asset, dataset_row):
    df, sep_mode = load_real_csv(dataset_row["file_name"])
    if df is None:
        return {
            "ok": False,
            "text": f"Je n’ai pas pu charger le dataset `{dataset_row['file_name']}`.",
        }

    time_col = guess_time_column(df.columns)
    close_col = guess_close_column(df.columns)

    if time_col is None or close_col is None:
        return {
            "ok": False,
            "text": f"Le dataset `{dataset_row['file_name']}` n’a pas les colonnes minimales attendues (`time` et `close`).",
        }

    out = df.copy()
    out[time_col] = pd.to_datetime(out[time_col], errors="coerce")
    out[close_col] = pd.to_numeric(out[close_col], errors="coerce")
    out = out.dropna(subset=[time_col, close_col]).sort_values(time_col)

    if len(out) < 3:
        return {
            "ok": False,
            "text": f"Le dataset `{dataset_row['file_name']}` n’a pas assez de lignes exploitables.",
        }

    out["ret_pct"] = out[close_col].pct_change() * 100.0
    out["year_week"] = out[time_col].dt.strftime("%G-W%V")

    threshold = -1.0
    week_counts = (
        out.assign(hit=out["ret_pct"] <= threshold)
           .groupby("year_week", as_index=False)["hit"]
           .sum()
           .rename(columns={"hit": "drop_count"})
    )

    qualified = week_counts[week_counts["drop_count"] >= 2].copy()

    if len(qualified) == 0:
        return {
            "ok": True,
            "text": (
                f"Je n’ai trouvé aucune semaine où **{asset}** a baissé d’au moins **1%** "
                f"au moins **2 fois** dans le dataset `{dataset_row['file_name']}`."
            ),
        }

    last_week = qualified.iloc[-1]["year_week"]
    last_count = int(qualified.iloc[-1]["drop_count"])

    matching_rows = out[
        (out["year_week"] == last_week) &
        (out["ret_pct"] <= threshold)
    ][[time_col, close_col, "ret_pct"]].copy()

    return {
        "ok": True,
        "text": (
            f"La dernière semaine où **{asset}** a baissé d’au moins **1%** "
            f"au moins **2 fois** est **{last_week}**.\n\n"
            f"J’ai utilisé le dataset **`{dataset_row['file_name']}`** "
            f"(fréquence : **{dataset_row['freq_guess']}**). "
            f"Dans cette semaine, j’ai compté **{last_count}** occurrences répondant au critère."
        ),
        "details": matching_rows,
    }

catalog = load_catalog()
cleaned = clean_catalog(catalog)

question = st.text_input(
    "Question",
    value="Quand est ce que SPX a baissé d'au moins 1% 2 fois dans la même semaine la dernière fois ?"
)

assets = detect_assets_from_query(question)
weekly_rule = parse_weekly_drop_question(question)

if len(assets) == 0:
    st.warning("Je n’ai pas détecté d’actif clairement dans la question.")
    st.stop()

if weekly_rule is None:
    st.info("Cette version traite pour l’instant surtout la question test hebdomadaire sur les baisses >= 1%.")
    with st.expander("Ce que j’ai compris", expanded=False):
        st.write({"assets": assets, "question": question})
    st.stop()

asset = assets[0]
dataset_row = choose_best_dataset(asset, cleaned)

if dataset_row is None:
    st.error(f"Aucun dataset exploitable trouvé pour {asset}.")
    st.stop()

answer = answer_weekly_drop(asset, dataset_row)

st.markdown("### Réponse")
st.markdown(answer["text"])

if answer.get("details") is not None and len(answer["details"]) > 0:
    with st.expander("Voir les lignes correspondantes", expanded=False):
        st.dataframe(answer["details"], width="stretch")

with st.expander("Détails techniques", expanded=False):
    st.write({
        "actif_detecté": asset,
        "dataset_utilisé": dataset_row["file_name"],
        "fréquence": dataset_row["freq_guess"],
        "chemin": dataset_row["relative_path"],
    })
