import streamlit as st
import pandas as pd
import os
import re
from datetime import date

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

MONTHS_FR = {
    1: "janvier", 2: "février", 3: "mars", 4: "avril", 5: "mai", 6: "juin",
    7: "juillet", 8: "août", 9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"
}

WEEKDAYS_FR = {
    0: "lundi", 1: "mardi", 2: "mercredi", 3: "jeudi", 4: "vendredi", 5: "samedi", 6: "dimanche"
}

MANUAL_TZ_OVERRIDES = {
    "VIX_9H30_CET_SPX_OPENING_daily.csv": "Europe/Paris",
}

@st.cache_data
def load_catalog():
    if not os.path.exists(CATALOG_PATH):
        return pd.DataFrame(columns=[
            "asset", "doc_type", "file_name", "relative_path", "local_file_name",
            "size_bytes", "freq_guess", "tz_guess", "vix_snapshot"
        ])
    try:
        return pd.read_csv(CATALOG_PATH)
    except Exception:
        return pd.DataFrame(columns=[
            "asset", "doc_type", "file_name", "relative_path", "local_file_name",
            "size_bytes", "freq_guess", "tz_guess", "vix_snapshot"
        ])

def detect_assets_from_query(text):
    t = str(text).lower()
    found = []
    for asset, aliases in ASSET_ALIASES.items():
        if any(alias in t for alias in aliases):
            found.append(asset)
    ordered = [a for a in ASSET_ORDER if a in found]
    return ordered if ordered else ["SPX"]

def parse_question(text):
    t = str(text).lower().strip()
    out = {
        "question_raw": text,
        "assets": detect_assets_from_query(text),
        "direction": None,
        "move_mode": "absolute",
        "threshold": None,
        "horizon_minutes": None,
        "condition_flag": any(x in t for x in ["si ", "when ", "condition", "à condition", "if "]),
        "special_case": None,
        "options_case": False,
        "need_interpretation_notice": False,
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

    if ("semaine" in t or "week" in t) and any(x in t for x in ["baissé", "baisse", "down", "drop", "chute"]) and ("1%" in t or "1 %" in t):
        out["special_case"] = "weekly_drop_count"

    if any(x in t for x in ["reverse iron condor", "ric", " iron condor", " ic ", "coût d'un ic", "coût d’un ic", "coût d'un ric", "coût d’un ric"]):
        out["options_case"] = True

    m_vix = re.search(r'vix(?:\s*de)?\s*(\d+(?:[.,]\d+)?)', t)
    out["target_vix"] = float(m_vix.group(1).replace(",", ".")) if m_vix else None

    m_points = re.search(r'aile[s]?\s*(?:de)?\s*(\d+(?:[.,]\d+)?)\s*(point|points)\b', t)
    m_pct_wing = re.search(r'aile[s]?\s*(?:de)?\s*(\d+(?:[.,]\d+)?)\s*%', t)

    out["wing_mode"] = None
    out["wing_value"] = None
    if m_points:
        out["wing_mode"] = "points"
        out["wing_value"] = float(m_points.group(1).replace(",", "."))
    elif m_pct_wing:
        out["wing_mode"] = "percent"
        out["wing_value"] = float(m_pct_wing.group(1).replace(",", "."))

    if out["options_case"] and out["wing_mode"] is None:
        out["wing_mode"] = "points"
        out["wing_value"] = 10.0

    if "ric" in t or "reverse iron condor" in t:
        out["structure"] = "RIC"
    elif "ic" in t or "iron condor" in t:
        out["structure"] = "IC"
    else:
        out["structure"] = None

    clear_enough = (
        len(out["assets"]) > 0 and (
            out["special_case"] is not None or
            out["options_case"] or
            (out["direction"] is not None and out["threshold"] is not None)
        )
    )
    out["need_interpretation_notice"] = not clear_enough
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
        "snapshot": None,
    }
    return m.get(str(freq_guess), None)

def iso_week_to_date_range_fr(year_week):
    try:
        year_str, week_str = year_week.split("-W")
        year = int(year_str)
        week = int(week_str)
        start = date.fromisocalendar(year, week, 1)
        end = date.fromisocalendar(year, week, 7)
        start_txt = f"{WEEKDAYS_FR[start.weekday()]} {start.strftime('%d-%m-%Y')}"
        end_txt = f"{WEEKDAYS_FR[end.weekday()]} {end.strftime('%d-%m-%Y')}"
        return f"du {start_txt} au {end_txt}"
    except Exception:
        return year_week

def infer_timezone(row):
    fn = str(row["file_name"])
    if fn in MANUAL_TZ_OVERRIDES:
        return MANUAL_TZ_OVERRIDES[fn]
    tz = str(row["tz_guess"])
    return tz if tz and tz != "nan" else "unknown"

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
            if df is not None and len(df.columns) >= 1:
                return df, label
        except Exception:
            continue
    return None, "failed"

def choose_all_standard_datasets(asset, catalog):
    sub = catalog[(catalog["asset"] == asset) & (catalog["doc_type"] == "standard")].copy()
    if len(sub) == 0:
        return sub
    priority = {"1min": 100, "5min": 90, "30min": 80, "daily": 70, "1h": 60, "unknown": 0}
    sub["priority"] = sub["freq_guess"].astype(str).map(lambda x: priority.get(x, 0))
    return sub.sort_values(by=["priority", "size_bytes"], ascending=[False, False]).reset_index(drop=True)

def load_option_chain_row(df):
    if df is None or len(df) == 0:
        return None

    if len(df.columns) == 1 and ";" in str(df.columns[0]):
        col_name = df.columns[0]
        new_cols = [x.strip() for x in col_name.split(";")]
        split_values = df.iloc[:, 0].astype(str).str.split(";", expand=True)
        split_values.columns = new_cols
        df = split_values.copy()

    rename_map = {}
    for c in df.columns:
        cl = str(c).strip().lower().replace(" ", "").replace("_", "")
        if cl in ["strike", "strikeprice"]:
            rename_map[c] = "Strike"
        elif cl in ["callbid"]:
            rename_map[c] = "CallBid"
        elif cl in ["callask"]:
            rename_map[c] = "CallAsk"
        elif cl in ["calldelta"]:
            rename_map[c] = "CallDelta"
        elif cl in ["putbid"]:
            rename_map[c] = "PutBid"
        elif cl in ["putask"]:
            rename_map[c] = "PutAsk"
        elif cl in ["putdelta"]:
            rename_map[c] = "PutDelta"

    df = df.rename(columns=rename_map)

    needed = ["Strike", "CallBid", "CallAsk", "CallDelta", "PutBid", "PutAsk", "PutDelta"]
    if not all(c in df.columns for c in needed):
        return None

    for c in needed:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=needed).sort_values("Strike").reset_index(drop=True)
    return df

def choose_nearest_option_file(catalog, target_vix):
    sub = catalog[catalog["doc_type"] == "options"].copy()
    if len(sub) == 0:
        return None
    sub["vix_snapshot_num"] = pd.to_numeric(sub["vix_snapshot"], errors="coerce")
    sub = sub.dropna(subset=["vix_snapshot_num"])
    if len(sub) == 0:
        return None
    if target_vix is None:
        return sub.sort_values("vix_snapshot_num").iloc[0]
    sub["dist"] = (sub["vix_snapshot_num"] - target_vix).abs()
    return sub.sort_values(["dist", "size_bytes"], ascending=[True, False]).iloc[0]

def nearest_strike(strikes, target, side=None):
    strikes = sorted([float(x) for x in strikes])
    if side == "lower":
        candidates = [s for s in strikes if s <= target]
        if len(candidates) == 0:
            return min(strikes, key=lambda x: abs(x - target))
        return min(candidates, key=lambda x: abs(x - target))
    if side == "upper":
        candidates = [s for s in strikes if s >= target]
        if len(candidates) == 0:
            return min(strikes, key=lambda x: abs(x - target))
        return min(candidates, key=lambda x: abs(x - target))
    return min(strikes, key=lambda x: abs(x - target))

def compute_ic_ric(question_parsed, catalog):
    row = choose_nearest_option_file(catalog, question_parsed.get("target_vix"))
    if row is None:
        return {"ok": False, "text": "Aucun fichier d’options exploitable n’a été trouvé."}

    raw_df, sep_mode = load_real_csv(row["local_file_name"])
    chain = load_option_chain_row(raw_df)
    if chain is None or len(chain) == 0:
        return {"ok": False, "text": f"Le tableau d’options `{row['file_name']}` n’est pas exploitable."}

    work = chain.copy()
    work["center_score"] = (work["CallDelta"] - 0.5).abs() + (work["PutDelta"] + 0.5).abs()
    center = work.sort_values("center_score").iloc[0]
    center_strike = float(center["Strike"])

    wing_mode = question_parsed.get("wing_mode") or "points"
    wing_value = float(question_parsed.get("wing_value") or 10.0)

    if wing_mode == "percent":
        wing_points_raw = center_strike * wing_value / 100.0
    else:
        wing_points_raw = wing_value

    strikes = work["Strike"].tolist()
    lower_target = center_strike - wing_points_raw
    upper_target = center_strike + wing_points_raw

    lower_strike = nearest_strike(strikes, lower_target, side="lower")
    upper_strike = nearest_strike(strikes, upper_target, side="upper")

    row_center = work[work["Strike"] == center_strike].iloc[0]
    row_lower = work[work["Strike"] == lower_strike].iloc[0]
    row_upper = work[work["Strike"] == upper_strike].iloc[0]

    width_put = center_strike - lower_strike
    width_call = upper_strike - center_strike
    width = max(width_put, width_call)

    # exécution réaliste bid/ask
    ic_credit = (
        row_center["PutBid"] - row_lower["PutAsk"] +
        row_center["CallBid"] - row_upper["CallAsk"]
    )
    ric_debit = (
        row_center["PutAsk"] - row_lower["PutBid"] +
        row_center["CallAsk"] - row_upper["CallBid"]
    )

    # mid
    center_put_mid = (row_center["PutBid"] + row_center["PutAsk"]) / 2
    lower_put_mid = (row_lower["PutBid"] + row_lower["PutAsk"]) / 2
    center_call_mid = (row_center["CallBid"] + row_center["CallAsk"]) / 2
    upper_call_mid = (row_upper["CallBid"] + row_upper["CallAsk"]) / 2

    ic_mid = (center_put_mid - lower_put_mid) + (center_call_mid - upper_call_mid)
    ric_mid = (center_put_mid - lower_put_mid) + (center_call_mid - upper_call_mid)

    ic_max_gain = ic_credit
    ic_max_loss = width - ic_credit
    ic_be_low = center_strike - ic_credit
    ic_be_high = center_strike + ic_credit

    ric_max_loss = ric_debit
    ric_max_gain = width - ric_debit
    ric_be_low = center_strike - ric_debit
    ric_be_high = center_strike + ric_debit

    structure = question_parsed.get("structure")
    if structure == "IC":
        text = (
            f"### Réponse\n\n"
            f"Pour un **IC** avec un **VIX cible de {question_parsed.get('target_vix')}**, "
            f"j’ai utilisé le fichier **`{row['file_name']}`** "
            f"(VIX observé le plus proche : **{row['vix_snapshot']}**).\n\n"
            f"Le **strike central** retenu est **{int(center_strike)}**, car c’est celui qui est le plus proche de "
            f"**delta +0,5 côté call** et **delta -0,5 côté put**.\n\n"
            f"Les ailes retenues sont **{int(lower_strike)} / {int(center_strike)} / {int(upper_strike)}**.\n\n"
            f"- **Crédit IC (bid/ask réaliste)** : **{ic_credit:.2f}**\n"
            f"- **Valeur mid** : **{ic_mid:.2f}**\n"
            f"- **Max gain** : **{ic_max_gain:.2f}**\n"
            f"- **Max loss** : **{ic_max_loss:.2f}**\n"
            f"- **Break-even bas** : **{ic_be_low:.2f}**\n"
            f"- **Break-even haut** : **{ic_be_high:.2f}**"
        )
    elif structure == "RIC":
        text = (
            f"### Réponse\n\n"
            f"Pour un **RIC** avec un **VIX cible de {question_parsed.get('target_vix')}**, "
            f"j’ai utilisé le fichier **`{row['file_name']}`** "
            f"(VIX observé le plus proche : **{row['vix_snapshot']}**).\n\n"
            f"Le **strike central** retenu est **{int(center_strike)}**, car c’est celui qui est le plus proche de "
            f"**delta +0,5 côté call** et **delta -0,5 côté put**.\n\n"
            f"Les ailes retenues sont **{int(lower_strike)} / {int(center_strike)} / {int(upper_strike)}**.\n\n"
            f"- **Débit RIC (bid/ask réaliste)** : **{ric_debit:.2f}**\n"
            f"- **Valeur mid** : **{ric_mid:.2f}**\n"
            f"- **Max gain** : **{ric_max_gain:.2f}**\n"
            f"- **Max loss** : **{ric_max_loss:.2f}**\n"
            f"- **Break-even bas** : **{ric_be_low:.2f}**\n"
            f"- **Break-even haut** : **{ric_be_high:.2f}**"
        )
    else:
        text = (
            f"### Réponse\n\n"
            f"J’ai utilisé le fichier **`{row['file_name']}`** "
            f"(VIX observé le plus proche : **{row['vix_snapshot']}**).\n\n"
            f"Centre retenu : **{int(center_strike)}**. "
            f"Ailes retenues : **{int(lower_strike)} / {int(center_strike)} / {int(upper_strike)}**.\n\n"
            f"- **IC** : crédit **{ic_credit:.2f}**, max gain **{ic_max_gain:.2f}**, max loss **{ic_max_loss:.2f}**\n"
            f"- **RIC** : débit **{ric_debit:.2f}**, max gain **{ric_max_gain:.2f}**, max loss **{ric_max_loss:.2f}**"
        )

    details = {
        "fichier": row["file_name"],
        "vix_snapshot_utilisé": row["vix_snapshot"],
        "sep_mode": sep_mode,
        "center_strike": center_strike,
        "lower_strike": lower_strike,
        "upper_strike": upper_strike,
        "wing_mode": wing_mode,
        "wing_value": wing_value,
        "wing_points_raw": wing_points_raw,
        "ic_credit": round(ic_credit, 4),
        "ric_debit": round(ric_debit, 4),
        "ic_mid": round(ic_mid, 4),
        "ric_mid": round(ric_mid, 4),
    }

    return {"ok": True, "text": text, "details": details}

def answer_weekly_drop(asset, datasets):
    results = []

    for _, row in datasets.iterrows():
        df, sep_mode = load_real_csv(row["local_file_name"])
        if df is None:
            continue

        time_col = guess_time_column(df.columns)
        close_col = guess_close_column(df.columns)

        if time_col is None or close_col is None:
            continue

        work = df.copy()
        work[time_col] = pd.to_datetime(work[time_col], errors="coerce")
        work[close_col] = pd.to_numeric(work[close_col], errors="coerce")
        work = work.dropna(subset=[time_col, close_col]).sort_values(time_col)

        if len(work) < 3:
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
            continue

        last_week = qualified.iloc[-1]["year_week"]
        last_count = int(qualified.iloc[-1]["drop_count"])

        results.append({
            "asset": asset,
            "dataset": row["file_name"],
            "freq": row["freq_guess"],
            "status": "ok",
            "last_week": last_week,
            "week_label_fr": iso_week_to_date_range_fr(last_week),
            "count": last_count,
        })

    if len(results) == 0:
        return {
            "ok": False,
            "text": f"Je n’ai trouvé aucune semaine où **{asset}** a baissé d’au moins **1%** au moins **2 fois** dans les datasets standards actuellement disponibles."
        }

    results = sorted(results, key=lambda r: r["last_week"], reverse=True)
    best = results[0]
    return {
        "ok": True,
        "text": (
            f"### Réponse\n\n"
            f"Pour **{asset}**, la dernière période trouvée est **{best['week_label_fr']}**.\n\n"
            f"J’ai retenu le dataset **`{best['dataset']}`** (fréquence **{best['freq']}**), "
            f"dans lequel j’ai compté **{best['count']}** occurrences répondant au critère dans cette semaine.\n\n"
            f"J’ai testé **tous les CSV canoniques disponibles pour {asset}**, pas un seul."
        ),
        "details": results,
    }

def answer_generic(asset, datasets, parsed):
    rows = []

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
            unit_label = "points"
        else:
            move = (future_price - price_series) / price_series * 100.0
            unit_label = "%"

        valid = move.notna()

        if direction == "up":
            cond = move > threshold
            dir_txt = "hausse"
        elif direction == "down":
            cond = move < -threshold
            dir_txt = "baisse"
        else:
            cond = move.abs() > threshold
            dir_txt = "mouvement absolu"

        total = int(valid.sum())
        success = int((cond & valid).sum())
        prob = (success / total) if total > 0 else 0.0

        rows.append({
            "dataset": row["file_name"],
            "freq": row["freq_guess"],
            "probability": prob,
            "success": success,
            "total": total,
            "dir_txt": dir_txt,
            "threshold": threshold,
            "unit_label": unit_label,
            "horizon_minutes": horizon_minutes,
        })

    if len(rows) == 0:
        return {
            "ok": False,
            "text": f"Je n’ai trouvé aucun dataset standard exploitable pour répondre proprement à la question sur **{asset}**."
        }

    rows = sorted(rows, key=lambda x: ({"1min": 4, "5min": 3, "30min": 2, "daily": 1}.get(x["freq"], 0), x["total"]), reverse=True)
    best = rows[0]

    horizon_txt = f"{best['horizon_minutes']} minutes" if best["horizon_minutes"] is not None else "l’horizon demandé"
    text = (
        f"### Réponse\n\n"
        f"Pour **{asset}**, la meilleure réponse disponible dans les datasets standards testés est la suivante :\n\n"
        f"- condition évaluée : **{best['dir_txt']} > {best['threshold']} {best['unit_label']}**\n"
        f"- horizon : **{horizon_txt}**\n"
        f"- dataset principal retenu : **`{best['dataset']}`** (**{best['freq']}**)\n"
        f"- probabilité observée : **{best['probability']:.2%}**\n"
        f"- occurrences : **{best['success']}** sur **{best['total']}** observations valides\n\n"
        f"J’ai testé **tous les CSV canoniques disponibles pour {asset}**, puis retenu la meilleure granularité exploitable."
    )
    return {"ok": True, "text": text, "details": rows}

catalog = load_catalog()

question = st.text_input(
    "Question",
    value="Quand est ce que SPX a baissé d'au moins 1% 2 fois dans la même semaine la dernière fois ?"
)

parsed = parse_question(question)
assets = parsed["assets"]

# petite mémoire de session pour la suite
st.session_state["last_question"] = question
st.session_state["last_assets"] = assets

if parsed["options_case"]:
    result = compute_ic_ric(parsed, catalog)
    st.markdown(result["text"])
    if result.get("details"):
        with st.expander("Détails techniques", expanded=False):
            st.write(result["details"])
else:
    final_blocks = []

    if parsed["need_interpretation_notice"]:
        final_blocks.append("### Réponse\n\nJ’ai dû interpréter partiellement la question, car elle n’était pas complètement explicite sur tous les paramètres.")

    for asset in assets:
        ds = choose_all_standard_datasets(asset, catalog)

        if parsed.get("special_case") == "weekly_drop_count":
            result = answer_weekly_drop(asset, ds)
        else:
            result = answer_generic(asset, ds, parsed)

        final_blocks.append(result["text"])

    st.markdown("\n\n".join(final_blocks))

    with st.expander("Datasets utilisés", expanded=False):
        for asset in assets:
            st.markdown(f"**{asset}**")
            ds = choose_all_standard_datasets(asset, catalog)
            if len(ds) == 0:
                st.write("Aucun dataset standard.")
            else:
                st.dataframe(
                    ds[["asset", "doc_type", "file_name", "relative_path", "freq_guess", "tz_guess", "size_bytes"]],
                    width="stretch"
                )

    with st.expander("Détails techniques", expanded=False):
        st.write(parsed)

with st.expander("Jeux de données spéciaux chargés dans la V1", expanded=False):
    st.write("Calendar économique : chargé")
    st.write("Tableaux d’options SPX pour IC / RIC : chargés")
    st.write("SPX_FUTURE : exclu du moteur actuel")
