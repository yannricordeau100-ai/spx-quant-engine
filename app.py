import streamlit as st
import pandas as pd
import os
import re
import numpy as np
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
    "calendar_events_daily.csv": "America/New_York",
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
        "list_mode": False,
    }

    if any(x in t for x in ["hausse", "up", "monte", "rise"]):
        out["direction"] = "up"
    elif any(x in t for x in ["baissé", "baisse", "down", "drop", "chute"]):
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

    if any(x in t for x in ["dernières fois", "dernieres fois", "last times", "où ", "ou "]):
        out["list_mode"] = True

    clear_enough = (
        len(out["assets"]) > 0 and (
            out["special_case"] is not None or
            out["options_case"] or
            (out["direction"] is not None and out["threshold"] is not None)
        )
    )
    out["need_interpretation_notice"] = not clear_enough
    return out

def format_date_fr(dt):
    if pd.isna(dt):
        return None
    return f"{WEEKDAYS_FR[dt.weekday()]} {dt.day} {MONTHS_FR[dt.month]} {dt.year}"

def iso_week_to_range_fr(year_week):
    try:
        year_str, week_str = year_week.split("-W")
        year = int(year_str)
        week = int(week_str)
        start = date.fromisocalendar(year, week, 1)  # lundi
        end = date.fromisocalendar(year, week, 5)    # vendredi
        start_txt = f"{WEEKDAYS_FR[start.weekday()]} {start.day} {MONTHS_FR[start.month]} {start.year}"
        end_txt = f"{WEEKDAYS_FR[end.weekday()]} {end.day} {MONTHS_FR[end.month]} {end.year}"
        return {
            "start": start,
            "end": end,
            "label": f"du {start_txt} au {end_txt}",
        }
    except Exception:
        return {"start": None, "end": None, "label": year_week}

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

def normalize_option_chain(df):
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
        elif cl == "callbid":
            rename_map[c] = "CallBid"
        elif cl == "callask":
            rename_map[c] = "CallAsk"
        elif cl == "calldelta":
            rename_map[c] = "CallDelta"
        elif cl == "putbid":
            rename_map[c] = "PutBid"
        elif cl == "putask":
            rename_map[c] = "PutAsk"
        elif cl == "putdelta":
            rename_map[c] = "PutDelta"

    df = df.rename(columns=rename_map)

    needed = ["Strike", "CallBid", "CallAsk", "CallDelta", "PutBid", "PutAsk", "PutDelta"]
    if not all(c in df.columns for c in needed):
        return None

    for c in needed:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=needed).sort_values("Strike").reset_index(drop=True)
    return df

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

def compute_chain_metrics(row, wing_mode, wing_value):
    raw_df, sep_mode = load_real_csv(row["local_file_name"])
    chain = normalize_option_chain(raw_df)
    if chain is None or len(chain) == 0:
        return None

    work = chain.copy()
    work["center_score"] = (work["CallDelta"] - 0.5).abs() + (work["PutDelta"] + 0.5).abs()
    center = work.sort_values("center_score").iloc[0]
    center_strike = float(center["Strike"])

    if wing_mode == "percent":
        wing_points_raw = center_strike * float(wing_value) / 100.0
    else:
        wing_points_raw = float(wing_value)

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

    ic_credit = (
        row_center["PutBid"] - row_lower["PutAsk"] +
        row_center["CallBid"] - row_upper["CallAsk"]
    )
    ric_debit = (
        row_center["PutAsk"] - row_lower["PutBid"] +
        row_center["CallAsk"] - row_upper["CallBid"]
    )

    center_put_mid = (row_center["PutBid"] + row_center["PutAsk"]) / 2
    lower_put_mid = (row_lower["PutBid"] + row_lower["PutAsk"]) / 2
    center_call_mid = (row_center["CallBid"] + row_center["CallAsk"]) / 2
    upper_call_mid = (row_upper["CallBid"] + row_upper["CallAsk"]) / 2

    ic_mid = (center_put_mid - lower_put_mid) + (center_call_mid - upper_call_mid)
    ric_mid = (row_center["PutAsk"] - row_lower["PutBid"]) + (row_center["CallAsk"] - row_upper["CallBid"])
    ric_mid_fair = (center_put_mid - lower_put_mid) + (center_call_mid - upper_call_mid)

    metrics = {
        "file_name": row["file_name"],
        "vix_snapshot": float(row["vix_snapshot"]) if str(row["vix_snapshot"]) not in ["", "nan"] else None,
        "sep_mode": sep_mode,
        "center_strike": center_strike,
        "lower_strike": lower_strike,
        "upper_strike": upper_strike,
        "wing_points_raw": wing_points_raw,
        "ic_credit": float(ic_credit),
        "ric_debit": float(ric_debit),
        "ic_mid": float(ic_mid),
        "ric_mid": float(ric_mid_fair),
        "ic_max_gain": float(ic_credit),
        "ic_max_loss": float(width - ic_credit),
        "ic_be_low": float(center_strike - ic_credit),
        "ic_be_high": float(center_strike + ic_credit),
        "ric_max_gain": float(width - ric_debit),
        "ric_max_loss": float(ric_debit),
        "ric_be_low": float(center_strike - ric_debit),
        "ric_be_high": float(center_strike + ric_debit),
    }
    return metrics

def choose_option_rows_for_interpolation(catalog, target_vix):
    sub = catalog[catalog["doc_type"] == "options"].copy()
    if len(sub) == 0:
        return sub
    sub["vix_snapshot_num"] = pd.to_numeric(sub["vix_snapshot"], errors="coerce")
    sub = sub.dropna(subset=["vix_snapshot_num"]).sort_values("vix_snapshot_num").reset_index(drop=True)
    if target_vix is None or len(sub) == 0:
        return sub.head(1)

    exact = sub[np.isclose(sub["vix_snapshot_num"], target_vix)]
    if len(exact) > 0:
        return exact.head(1)

    below = sub[sub["vix_snapshot_num"] < target_vix].sort_values("vix_snapshot_num", ascending=False).head(2)
    above = sub[sub["vix_snapshot_num"] > target_vix].sort_values("vix_snapshot_num", ascending=True).head(2)
    out = pd.concat([below, above]).sort_values("vix_snapshot_num").drop_duplicates(subset=["file_name"])
    if len(out) == 0:
        sub["dist"] = (sub["vix_snapshot_num"] - target_vix).abs()
        return sub.sort_values("dist").head(4)
    return out

def interpolate_metric(points_x, points_y, target_x):
    if len(points_x) == 0:
        return None
    if len(points_x) == 1:
        return float(points_y[0])

    x = np.array(points_x, dtype=float)
    y = np.array(points_y, dtype=float)

    deg = 2 if len(x) >= 3 else 1
    try:
        coeffs = np.polyfit(x, y, deg=deg)
        val = np.polyval(coeffs, target_x)
        return float(val)
    except Exception:
        # fallback inverse distance
        dist = np.abs(x - float(target_x))
        dist = np.where(dist == 0, 1e-9, dist)
        w = 1.0 / dist
        return float(np.sum(w * y) / np.sum(w))

def compute_ic_ric(question_parsed, catalog):
    rows = choose_option_rows_for_interpolation(catalog, question_parsed.get("target_vix"))
    if rows is None or len(rows) == 0:
        return {"ok": False, "text": "Aucun fichier d’options exploitable n’a été trouvé."}

    metrics_list = []
    for _, row in rows.iterrows():
        m = compute_chain_metrics(row, question_parsed.get("wing_mode"), question_parsed.get("wing_value"))
        if m is not None and m.get("vix_snapshot") is not None:
            metrics_list.append(m)

    if len(metrics_list) == 0:
        return {"ok": False, "text": "Les tableaux d’options trouvés ne sont pas exploitables."}

    target_vix = question_parsed.get("target_vix")
    if target_vix is None:
        target_vix = metrics_list[0]["vix_snapshot"]

    xs = [m["vix_snapshot"] for m in metrics_list]

    interpolated = {}
    for key in [
        "center_strike", "lower_strike", "upper_strike", "wing_points_raw",
        "ic_credit", "ric_debit", "ic_mid", "ric_mid",
        "ic_max_gain", "ic_max_loss", "ic_be_low", "ic_be_high",
        "ric_max_gain", "ric_max_loss", "ric_be_low", "ric_be_high"
    ]:
        ys = [m[key] for m in metrics_list]
        interpolated[key] = interpolate_metric(xs, ys, target_vix)

    used_files = [m["file_name"] for m in metrics_list]
    structure = question_parsed.get("structure")

    if structure == "IC":
        text = (
            f"### Réponse\n\n"
            f"Pour un **IC** avec un **VIX cible de {target_vix:.2f}**, j’ai utilisé les fichiers d’options les plus proches "
            f"et effectué une **interpolation locale non linéaire**.\n\n"
            f"Le **strike central estimé** est **{round(interpolated['center_strike'])}**.\n"
            f"Les ailes estimées sont **{round(interpolated['lower_strike'])} / {round(interpolated['center_strike'])} / {round(interpolated['upper_strike'])}**.\n\n"
            f"- **Crédit IC estimé** : **{interpolated['ic_credit']:.2f}**\n"
            f"- **Valeur mid estimée** : **{interpolated['ic_mid']:.2f}**\n"
            f"- **Max gain estimé** : **{interpolated['ic_max_gain']:.2f}**\n"
            f"- **Max loss estimé** : **{interpolated['ic_max_loss']:.2f}**\n"
            f"- **Break-even bas estimé** : **{interpolated['ic_be_low']:.2f}**\n"
            f"- **Break-even haut estimé** : **{interpolated['ic_be_high']:.2f}**"
        )
    elif structure == "RIC":
        text = (
            f"### Réponse\n\n"
            f"Pour un **RIC** avec un **VIX cible de {target_vix:.2f}**, j’ai utilisé les fichiers d’options les plus proches "
            f"et effectué une **interpolation locale non linéaire**.\n\n"
            f"Le **strike central estimé** est **{round(interpolated['center_strike'])}**.\n"
            f"Les ailes estimées sont **{round(interpolated['lower_strike'])} / {round(interpolated['center_strike'])} / {round(interpolated['upper_strike'])}**.\n\n"
            f"- **Débit RIC estimé** : **{interpolated['ric_debit']:.2f}**\n"
            f"- **Valeur mid estimée** : **{interpolated['ric_mid']:.2f}**\n"
            f"- **Max gain estimé** : **{interpolated['ric_max_gain']:.2f}**\n"
            f"- **Max loss estimé** : **{interpolated['ric_max_loss']:.2f}**\n"
            f"- **Break-even bas estimé** : **{interpolated['ric_be_low']:.2f}**\n"
            f"- **Break-even haut estimé** : **{interpolated['ric_be_high']:.2f}**"
        )
    else:
        text = (
            f"### Réponse\n\n"
            f"J’ai effectué une interpolation locale non linéaire autour d’un **VIX cible de {target_vix:.2f}**.\n\n"
            f"- **IC estimé** : **{interpolated['ic_credit']:.2f}**\n"
            f"- **RIC estimé** : **{interpolated['ric_debit']:.2f}**"
        )

    return {
        "ok": True,
        "text": text,
        "details": {
            "fichiers_utilisés": used_files,
            "target_vix": target_vix,
            "wing_mode": question_parsed.get("wing_mode"),
            "wing_value": question_parsed.get("wing_value"),
            **{k: round(v, 4) if isinstance(v, (int, float, np.floating)) else v for k, v in interpolated.items()}
        }
    }

def answer_weekly_drop(asset, datasets, list_mode=False):
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

        for _, qrow in qualified.iterrows():
            rng = iso_week_to_range_fr(qrow["year_week"])
            results.append({
                "asset": asset,
                "dataset": row["file_name"],
                "freq": row["freq_guess"],
                "last_week": qrow["year_week"],
                "week_label_fr": rng["label"],
                "count": int(qrow["drop_count"]),
            })

    if len(results) == 0:
        return {
            "ok": False,
            "text": f"Je n’ai trouvé aucune semaine où **{asset}** a baissé d’au moins **1%** au moins **2 fois** dans les datasets standards actuellement disponibles."
        }

    results = sorted(results, key=lambda r: r["last_week"], reverse=True)

    if not list_mode:
        best = results[0]
        return {
            "ok": True,
            "text": (
                f"### Réponse\n\n"
                f"Pour **{asset}**, la dernière semaine trouvée a commencé **{best['week_label_fr'].replace('du ', '')}**.\n\n"
                f"J’ai retenu le dataset **`{best['dataset']}`** (fréquence **{best['freq']}**), "
                f"dans lequel j’ai compté **{best['count']}** occurrences répondant au critère dans cette semaine.\n\n"
                f"J’ai testé **tous les CSV canoniques disponibles pour {asset}**, pas un seul."
            ),
            "details": results,
        }

    top = results[:5]
    lines = [f"### Réponse\n", f"Pour **{asset}**, les dernières semaines trouvées sont les suivantes :\n"]
    for r in top:
        lines.append(
            f"- **{r['week_label_fr']}** — dataset **`{r['dataset']}`** ({r['freq']}), "
            f"avec **{r['count']}** occurrences"
        )
    lines.append("\nJ’ai testé **tous les CSV canoniques disponibles pour cet actif**.")
    return {"ok": True, "text": "\n".join(lines), "details": results}

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

# mémoire minimale de session pour la future étape
st.session_state["last_question"] = question
st.session_state["last_assets"] = assets

if parsed["options_case"]:
    result = compute_ic_ric(parsed, catalog)
    st.markdown(result["text"])
    if result.get("details"):
        with st.expander("Détails techniques", expanded=False):
            st.write(result["details"])
else:
    blocks = []

    if parsed["need_interpretation_notice"]:
        blocks.append("### Réponse\n\nJ’ai dû interpréter partiellement la question, car elle n’était pas complètement explicite sur tous les paramètres.")

    for asset in assets:
        ds = choose_all_standard_datasets(asset, catalog)

        if parsed.get("special_case") == "weekly_drop_count":
            result = answer_weekly_drop(asset, ds, list_mode=parsed.get("list_mode", False))
        else:
            result = answer_generic(asset, ds, parsed)

        blocks.append(result["text"])

    st.markdown("\n\n".join(blocks))

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
