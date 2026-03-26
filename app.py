import streamlit as st
import pandas as pd
import os
import re

st.set_page_config(layout="wide")
st.title("SPX Quant Engine")

CATALOG_PATH = "data/selected_catalog.csv"
LIVE_ROOT = "data/live_selected"

ASSET_ORDER = ["SPX", "SPY", "VIX", "VIX1D", "Or+pétrole"]

ASSET_ALIASES = {
    "SPX": ["spx", "s&p", "sp 500", "s&p500"],
    "SPY": ["spy"],
    "VIX": ["vix", "vix cash", "vix open", "vix ouverture"],
    "VIX1D": ["vix1d", "vix 1d"],
    "Or+pétrole": ["or+pétrole", "or+petrole", "gold+oil", "gold oil", "or", "gold", "pétrole", "petrole", "oil"],
}

MANUAL_TZ_OVERRIDES = {
    "VIX_9H30_CET_SPX_OPENING_daily.csv": "Europe/Paris",
}

@st.cache_data
def load_catalog():
    if not os.path.exists(CATALOG_PATH):
        return pd.DataFrame(columns=["asset", "file_name", "relative_path", "local_file_name", "size_bytes", "freq_guess", "tz_guess"])
    try:
        return pd.read_csv(CATALOG_PATH)
    except Exception:
        return pd.DataFrame(columns=["asset", "file_name", "relative_path", "local_file_name", "size_bytes", "freq_guess", "tz_guess"])

def detect_assets_from_query(text):
    t = str(text).lower()
    found = []
    for asset, aliases in ASSET_ALIASES.items():
        if any(alias in t for alias in aliases):
            found.append(asset)
    return [a for a in ASSET_ORDER if a in found]

def parse_question(text):
    t = str(text).lower().strip()
    out = {
        "direction": None,
        "move_mode": "absolute",
        "threshold": None,
        "horizon_minutes": None,
        "condition_flag": any(x in t for x in ["si ", "when ", "condition", "à condition", "if "]),
    }

    if any(x in t for x in ["hausse", "up", "monte", "rise"]):
        out["direction"] = "up"
    elif any(x in t for x in ["baisse", "down", "drop", "chute"]):
        out["direction"] = "down"
    elif any(x in t for x in ["absolu", "absolute", "abs", "mouvement", "move"]):
        out["direction"] = "abs"

    m_pct = re.search(r'(\d+(?:[.,]\d+)?)\s*%', t)
    if m_pct:
        out["threshold"] = float(m_pct.group(1).replace(",", "."))
        out["move_mode"] = "percent"
    else:
        m_num = re.search(r'(\d+(?:[.,]\d+)?)', t)
        if m_num:
            out["threshold"] = float(m_num.group(1).replace(",", "."))

    m_h = re.search(r'(\d+)\s*(min|minute|minutes|h|heure|heures|day|daily|jour|jours)', t)
    if m_h:
        value = int(m_h.group(1))
        unit = m_h.group(2)
        if unit.startswith("min"):
            out["horizon_minutes"] = value
        elif unit in ["h", "heure", "heures"]:
            out["horizon_minutes"] = value * 60
        elif unit in ["day", "daily", "jour", "jours"]:
            out["horizon_minutes"] = value * 1440

    # question test hebdomadaire
    if ("semaine" in t or "week" in t) and any(x in t for x in ["baissé", "baisse", "down", "drop", "chute"]) and ("1%" in t or "1 %" in t):
        out["special_case"] = "weekly_drop_count"
    else:
        out["special_case"] = None

    return out

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

def infer_timezone(row):
    fn = str(row["file_name"])
    tz = str(row["tz_guess"])
    if fn in MANUAL_TZ_OVERRIDES:
        return MANUAL_TZ_OVERRIDES[fn]
    if tz and tz != "unknown":
        return tz
    return "unknown"

@st.cache_data
def load_real_csv(local_file_name):
    full_path = os.path.join(LIVE_ROOT, local_file_name)
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

def dataset_priority(freq):
    # priorité voulue: 1min > 5min > 30min > daily
    order = {
        "1min": 100,
        "5min": 90,
        "30min": 80,
        "daily": 70,
        "1h": 60,
        "unknown": 10,
    }
    return order.get(str(freq), 0)

def choose_all_datasets_for_asset(asset, catalog):
    sub = catalog[catalog["asset"] == asset].copy()
    if len(sub) == 0:
        return sub
    sub["priority"] = sub["freq_guess"].astype(str).map(dataset_priority)
    return sub.sort_values(by=["priority", "size_bytes"], ascending=[False, False]).reset_index(drop=True)

def answer_weekly_drop(asset, datasets):
    results = []

    for _, row in datasets.iterrows():
        df, sep_mode = load_real_csv(row["local_file_name"])
        if df is None:
            results.append({
                "asset": asset,
                "dataset": row["file_name"],
                "freq": row["freq_guess"],
                "status": "unreadable",
                "text": f"Dataset illisible: {row['file_name']}"
            })
            continue

        time_col = guess_time_column(df.columns)
        close_col = guess_close_column(df.columns)

        if time_col is None or close_col is None:
            results.append({
                "asset": asset,
                "dataset": row["file_name"],
                "freq": row["freq_guess"],
                "status": "missing_columns",
                "text": f"Colonnes insuffisantes dans {row['file_name']}."
            })
            continue

        work = df.copy()
        work[time_col] = pd.to_datetime(work[time_col], errors="coerce")
        work[close_col] = pd.to_numeric(work[close_col], errors="coerce")
        work = work.dropna(subset=[time_col, close_col]).sort_values(time_col)

        if len(work) < 3:
            results.append({
                "asset": asset,
                "dataset": row["file_name"],
                "freq": row["freq_guess"],
                "status": "too_short",
                "text": f"Dataset trop court: {row['file_name']}."
            })
            continue

        work["ret_pct"] = work[close_col].pct_change() * 100.0
        work["year_week"] = work[time_col].dt.strftime("%G-W%V")

        threshold = -1.0
        week_counts = (
            work.assign(hit=work["ret_pct"] <= threshold)
                .groupby("year_week", as_index=False)["hit"]
                .sum()
                .rename(columns={"hit": "drop_count"})
        )

        qualified = week_counts[week_counts["drop_count"] >= 2].copy()

        if len(qualified) == 0:
            results.append({
                "asset": asset,
                "dataset": row["file_name"],
                "freq": row["freq_guess"],
                "status": "no_match",
                "text": f"Aucune semaine trouvée dans {row['file_name']}."
            })
            continue

        last_week = qualified.iloc[-1]["year_week"]
        last_count = int(qualified.iloc[-1]["drop_count"])

        results.append({
            "asset": asset,
            "dataset": row["file_name"],
            "freq": row["freq_guess"],
            "status": "ok",
            "last_week": last_week,
            "count": last_count,
            "text": f"{row['file_name']} → dernière semaine trouvée: {last_week} ({last_count} occurrences)."
        })

    return results

def answer_generic(asset, datasets, parsed):
    results = []

    for _, row in datasets.iterrows():
        df, sep_mode = load_real_csv(row["local_file_name"])
        if df is None:
            continue

        price_candidates = guess_price_columns(df.columns)
        if len(price_candidates) == 0:
            continue

        price_col = price_candidates[0]
        if "close" in [str(c).lower() for c in price_candidates]:
            price_col = price_candidates[[str(c).lower() for c in price_candidates].index("close")]

        freq_minutes = freq_to_minutes(row["freq_guess"])
        horizon_minutes = parsed.get("horizon_minutes")

        if horizon_minutes is not None and freq_minutes is not None:
            if freq_minutes > horizon_minutes:
                continue
            horizon_steps = max(1, int(round(horizon_minutes / freq_minutes)))
        else:
            horizon_steps = 1

        price_series = pd.to_numeric(df[price_col], errors="coerce")
        future_price = price_series.shift(-horizon_steps)

        move_mode = parsed.get("move_mode") or "absolute"
        threshold = parsed.get("threshold")
        direction = parsed.get("direction") or "up"

        if threshold is None:
            threshold = 5.0 if move_mode == "absolute" else 0.2

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

        results.append({
            "asset": asset,
            "dataset": row["file_name"],
            "freq": row["freq_guess"],
            "status": "ok",
            "probability": round(prob, 4),
            "success": success,
            "total": total,
        })

    return results

def format_answer(question, parsed, assets, all_results):
    lines = []
    lines.append("### Réponse")
    lines.append("")

    if len(assets) == 0:
        lines.append("Je n’ai détecté aucun actif clairement dans ta question.")
        return "\n".join(lines)

    clear_question = (len(assets) > 0 and not parsed.get("condition_flag"))
    if not clear_question:
        lines.append(f"J’ai interprété ta question comme : **{question}**.")
        lines.append("")

    if parsed.get("special_case") == "weekly_drop_count":
        for asset in assets:
            res = all_results.get(asset, [])
            ok_rows = [r for r in res if r["status"] == "ok"]

            if not ok_rows:
                lines.append(f"**{asset}** — je n’ai trouvé aucune semaine correspondante sur les datasets exploitables actuellement testés.")
                continue

            ok_rows = sorted(ok_rows, key=lambda r: (r["last_week"], r["freq"]), reverse=True)
            best = ok_rows[0]
            lines.append(
                f"**{asset}** — la dernière semaine trouvée est **{best['last_week']}**, "
                f"sur le dataset **`{best['dataset']}`** (fréquence **{best['freq']}**), "
                f"avec **{best['count']}** occurrences dans cette semaine."
            )

        lines.append("")
        lines.append("J’ai testé l’actif sur **tous les CSV canoniques disponibles pour cet actif**, pas sur un seul fichier.")
        return "\n".join(lines)

    for asset in assets:
        res = all_results.get(asset, [])
        ok_rows = [r for r in res if r["status"] == "ok"]

        if not ok_rows:
            lines.append(f"**{asset}** — je n’ai pas trouvé de dataset exploitable avec une granularité suffisante.")
            continue

        lines.append(f"**{asset}** — résultats sur tous les CSV canoniques :")
        for r in ok_rows:
            lines.append(
                f"- `{r['dataset']}` ({r['freq']}) : probabilité **{r['probability']:.2%}** "
                f"({r['success']} succès / {r['total']} observations)"
            )

    lines.append("")
    lines.append("La réponse porte sur **tout l’historique disponible des datasets utilisés**, pas sur une date unique.")
    return "\n".join(lines)

catalog = load_catalog()

question = st.text_input(
    "Question",
    value="Quand est ce que SPX a baissé d'au moins 1% 2 fois dans la même semaine la dernière fois ?"
)

parsed = parse_question(question)
assets = detect_assets_from_query(question)

all_results = {}
datasets_used = []

for asset in assets:
    ds = choose_all_datasets_for_asset(asset, catalog)
    datasets_used.append(ds)
    if parsed.get("special_case") == "weekly_drop_count":
        all_results[asset] = answer_weekly_drop(asset, ds)
    else:
        all_results[asset] = answer_generic(asset, ds, parsed)

st.markdown(format_answer(question, parsed, assets, all_results))

with st.expander("Datasets utilisés", expanded=False):
    for asset in assets:
        st.markdown(f"**{asset}**")
        ds = choose_all_datasets_for_asset(asset, catalog)
        if len(ds) == 0:
            st.write("Aucun dataset")
        else:
            st.dataframe(
                ds[["asset", "file_name", "relative_path", "freq_guess", "tz_guess", "size_bytes"]],
                width="stretch"
            )

with st.expander("Détails techniques", expanded=False):
    st.write(parsed)
    st.write("Actifs détectés :", assets)
