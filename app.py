import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

st.set_page_config(page_title="SPX Quant Engine", layout="wide")
st.title("SPX Quant Engine")

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR / "data" / "live_selected"
APP_RUNTIME_DIR = BASE_DIR / "app_runtime"
APP_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
FEEDBACK_CSV = APP_RUNTIME_DIR / "question_feedback.csv"

DEFAULT_Q = "SPX quand VIX1D/VIX > 1.2"

MONTHS_FR = {
    1: "janvier", 2: "février", 3: "mars", 4: "avril", 5: "mai", 6: "juin",
    7: "juillet", 8: "août", 9: "septembre", 10: "octobre", 11: "novembre", 12: "décembre"
}
WEEKDAYS_FR = {
    0: "lundi", 1: "mardi", 2: "mercredi", 3: "jeudi", 4: "vendredi", 5: "samedi", 6: "dimanche"
}

def norm(t: Any) -> str:
    t = str(t).strip().lower()
    repl = {
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "à": "a", "â": "a",
        "î": "i", "ï": "i",
        "ô": "o",
        "ù": "u", "û": "u", "ü": "u",
        "ç": "c",
        "\u2018": "'", "\u2019": "'",
        "≥": ">=", "≤": "<="
    }
    for k, v in repl.items():
        t = t.replace(k, v)
    t = re.sub(r"\s+", " ", t)
    return t

def fmt_date_fr(d: Any) -> str:
    try:
        d = pd.to_datetime(d).date()
        return f"{WEEKDAYS_FR[d.weekday()]} {d.day} {MONTHS_FR[d.month]} {d.year}"
    except Exception:
        return str(d)

def fmt_week_fr(year_week: str) -> str:
    try:
        y, w = year_week.split("-W")
        y, w = int(y), int(w)
        start = date.fromisocalendar(y, w, 1)
        end = date.fromisocalendar(y, w, 5)
        label = f"du {WEEKDAYS_FR[start.weekday()]} {start.day} {MONTHS_FR[start.month]} {start.year} au {WEEKDAYS_FR[end.weekday()]} {end.day} {MONTHS_FR[end.month]} {end.year}"
        return label[:1].upper() + label[1:]
    except Exception:
        return year_week

def read_csv_any(path: Path):
    try:
        df = pd.read_csv(path, sep=None, engine="python")
        if df is not None and len(df.columns) > 1:
            return df, "auto"
    except Exception:
        pass
    for sep, label in [(";", "semicolon"), (",", "comma"), ("\t", "tab"), ("|", "pipe")]:
        try:
            df = pd.read_csv(path, sep=sep)
            if df is not None and len(df.columns) > 1:
                return df, label
        except Exception:
            continue
    raise ValueError(f"Lecture CSV impossible: {path}")

def guess_time_column(cols):
    cols = list(cols)
    lower = {str(c).lower(): c for c in cols}
    for exact in ["time", "datetime", "date", "timestamp"]:
        if exact in lower:
            return lower[exact]
    for c in cols:
        cl = str(c).lower()
        if "time" in cl or "date" in cl:
            return c
    return None

def guess_open_column(cols):
    cols = list(cols)
    lower = {str(c).lower(): c for c in cols}
    if "open" in lower:
        return lower["open"]
    for c in cols:
        if "open" in str(c).lower():
            return c
    return None

def guess_close_column(cols):
    cols = list(cols)
    lower = {str(c).lower(): c for c in cols}
    if "close" in lower:
        return lower["close"]
    for c in cols:
        if "close" in str(c).lower():
            return c
    return None

def guess_value_column(cols):
    cols = list(cols)
    priorities = ["ratio_vix1d_vix", "ratio", "close", "open", "value", "last"]
    for p in priorities:
        for c in cols:
            if p in str(c).lower():
                return c
    for c in cols:
        cl = str(c).lower()
        if all(x not in cl for x in ["time", "date", "timestamp"]):
            return c
    return None

def detect_freq(file_name: str) -> str:
    n = norm(file_name)
    if "1min" in n or "1_min" in n or "_1min" in n:
        return "1min"
    if "5min" in n or "5_min" in n or "_5min" in n:
        return "5min"
    if "15min" in n or "15_min" in n:
        return "15min"
    if "30min" in n or "30_min" in n or "_30min" in n or "_30_min" in n:
        return "30min"
    if "4hour" in n or "4hours" in n or "4h" in n or "4_hour" in n:
        return "4h"
    if "1hour" in n or "1_hour" in n or "_1h" in n or "hour" in n:
        return "1h"
    if "daily" in n or n.endswith("_daily.csv") or n.endswith("_day.csv") or ", 1d" in n:
        return "daily"
    return "unknown"

def clean_display_filename(file_name: str) -> str:
    name = str(file_name)
    if "__" not in name:
        return name
    parts = [p for p in name.split("__") if p]
    last = parts[-1] if parts else name
    return last if last.lower().endswith(".csv") else name

def detect_dataset_kind(folder_name: str, file_name: str) -> str:
    folder = norm(folder_name)
    file_n = norm(file_name)
    if "option_chain" in file_n or "option chain" in file_n:
        return "options"
    if "data brut option spx pour ric" in folder or "option" in file_n or "ric" in file_n:
        return "options"
    if "autres actions upload a preparer" in folder:
        return "excluded"
    if "5j et 20 j move average" in folder or "average range" in file_n:
        return "move_average"
    if "correlation" in folder or "correlation" in file_n:
        return "correlation"
    if "vix1d_vix_ratio" in file_n or ("vix1d" in file_n and "vix" in file_n and "ratio" in file_n):
        return "ratio"
    if folder == "ratio" or "ratio" in file_n or "put call" in file_n or "put_call" in file_n:
        return "ratio"
    return "standard"

def build_aliases(path: Path, dataset_kind: str) -> list:
    file_n = norm(path.name)
    aliases = set()

    if dataset_kind == "standard":
        if "spx_daily" in file_n:
            aliases.update({"spx", "s&p", "s&p500", "sp 500", "sp500"})
        elif "spy_daily" in file_n:
            aliases.add("spy")
        elif "qqq_daily" in file_n:
            aliases.add("qqq")
        elif "iwm_daily" in file_n:
            aliases.add("iwm")
        elif file_n == "vix_daily.csv":
            aliases.add("vix")
        elif "vix1d_daily" in file_n:
            aliases.update({"vix1d", "vix 1d"})
        elif "vvix" in file_n and "daily" in file_n:
            aliases.add("vvix")
        elif "skew" in file_n and "daily" in file_n:
            aliases.update({"skew", "skew index"})
        elif "dxy" in file_n:
            aliases.add("dxy")
        elif "gold" in file_n and "daily" in file_n:
            aliases.update({"gold", "or"})
        elif "vix3m" in file_n:
            aliases.add("vix3m")
        elif "vix6m" in file_n:
            aliases.add("vix6m")
        elif "vix9d" in file_n:
            aliases.add("vix9d")

    if dataset_kind == "ratio" and "vix1d" in file_n and "vix" in file_n:
        aliases.update({"vix1d/vix", "ratio vix1d/vix", "vix1d vix"})

    return sorted(aliases)

@st.cache_data(show_spinner=False)
def scan_catalog() -> pd.DataFrame:
    rows = []
    if not DATA_ROOT.exists():
        return pd.DataFrame(columns=["file", "display_file", "path", "folder", "freq", "aliases", "dataset_kind", "eligible_detection"])

    for f in sorted(DATA_ROOT.rglob("*.csv")):
        rel_n = norm(str(f.relative_to(DATA_ROOT)))
        if "__macosx" in rel_n:
            continue
        folder = f.parent.name
        kind = detect_dataset_kind(folder, f.name)
        freq = detect_freq(f.name)
        rows.append({
            "file": f.name,
            "display_file": clean_display_filename(f.name),
            "path": str(f),
            "folder": folder,
            "freq": freq,
            "aliases": build_aliases(f, kind),
            "dataset_kind": kind,
            "eligible_detection": (freq == "daily" and kind in {"standard", "ratio"}),
        })
    return pd.DataFrame(rows)

CATALOG = scan_catalog()

def get_dataset_by_exact_file(file_name: str):
    if CATALOG.empty:
        return None
    hit = CATALOG[CATALOG["file"] == file_name]
    if hit.empty:
        return None
    return hit.iloc[0].to_dict()

def find_ratio_dataset():
    """Trouve le CSV ratio VIX1D/VIX — priorité absolue à VIX1D_VIX_ratio_daily.csv"""
    if CATALOG.empty:
        return None
    # Priorité 1 : nom exact
    exact = get_dataset_by_exact_file("VIX1D_VIX_ratio_daily.csv")
    if exact is not None:
        return exact
    # Priorité 2 : cherche dans les ratios daily contenant vix1d et vix
    tmp = CATALOG[
        (CATALOG["dataset_kind"] == "ratio") &
        (CATALOG["freq"] == "daily")
    ].copy()
    if tmp.empty:
        return None
    file_n = tmp["file"].map(norm)
    tmp = tmp[file_n.str.contains("vix1d", na=False) & file_n.str.contains("vix", na=False)]
    if tmp.empty:
        return None
    tmp = tmp.copy()
    tmp["score"] = 0
    tmp.loc[tmp["file"].map(norm).str.contains("ratio", na=False), "score"] += 10
    tmp.loc[tmp["file"].map(norm).str.contains("daily", na=False), "score"] += 5
    tmp = tmp.sort_values(["score", "file"], ascending=[False, True])
    return tmp.iloc[0].to_dict()

def detect_datasets_in_question(q: str, max_count: int = 3):
    qn = norm(q)
    out = []

    ratio_hit = None
    if "vix1d/vix" in qn or "vix1d vix" in qn:
        ratio_hit = find_ratio_dataset()
        if ratio_hit is not None:
            matched = "vix1d/vix" if "vix1d/vix" in qn else "vix1d vix"
            out.append({**ratio_hit, "_matched_alias": matched, "_matched_pos": qn.find(matched), "_matched_len": len(matched)})

    standards = [
        ("spx", get_dataset_by_exact_file("SPX_daily.csv")),
        ("spy", get_dataset_by_exact_file("SPY_daily.csv")),
        ("qqq", get_dataset_by_exact_file("QQQ_daily.csv")),
        ("iwm", get_dataset_by_exact_file("IWM_daily.csv")),
        ("vix", get_dataset_by_exact_file("VIX_daily.csv")),
        ("vix1d", get_dataset_by_exact_file("VIX1D_VIX_ratio_daily.csv") if "vix1d/vix" not in qn else None),
        ("gold", get_dataset_by_exact_file("Gold_daily.csv")),
        ("dxy", get_dataset_by_exact_file("DXY_daily.csv")),
        ("vix3m", get_dataset_by_exact_file("VIX3M_daily.csv")),
        ("vix9d", get_dataset_by_exact_file("VIX9D_daily.csv")),
    ]

    occupied = []
    if ratio_hit is not None:
        matched = "vix1d/vix" if "vix1d/vix" in qn else "vix1d vix"
        occupied.append((qn.find(matched), qn.find(matched) + len(matched)))

    for alias, hit in standards:
        if hit is None:
            continue
        pos = qn.find(alias)
        if pos == -1:
            continue
        overlap = any(not (pos + len(alias) <= a or b <= pos) for a, b in occupied)
        if overlap:
            continue
        out.append({**hit, "_matched_alias": alias, "_matched_pos": pos, "_matched_len": len(alias)})
        occupied.append((pos, pos + len(alias)))

    out = sorted(out, key=lambda r: (r["_matched_pos"], -r["_matched_len"], r["file"]))
    dedup = []
    seen = set()
    for r in out:
        if r["file"] in seen:
            continue
        dedup.append(r)
        seen.add(r["file"])
    return dedup[:max_count]

@st.cache_data(show_spinner=False)
def load_daily_value_df(path_str: str):
    path = Path(path_str)
    df, sep_used = read_csv_any(path)
    time_col = guess_time_column(df.columns)
    value_col = guess_value_column(df.columns)
    if time_col is None:
        raise Exception(f"Colonne date introuvable dans {path.name} | cols={list(df.columns)}")
    if value_col is None:
        raise Exception(f"Colonne valeur introuvable dans {path.name} | cols={list(df.columns)}")

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[time_col, value_col]).sort_values(time_col)

    if df.empty:
        raise Exception(f"Aucune ligne exploitable dans {path.name}")

    out = df[[time_col, value_col]].rename(columns={time_col: "time", value_col: "value"}).copy()
    out["date"] = out["time"].dt.date
    return out, path.name, value_col

@st.cache_data(show_spinner=False)
def load_price_daily_df(asset: str):
    file_name = f"{asset}_daily.csv"
    hit = get_dataset_by_exact_file(file_name)
    if hit is None:
        raise FileNotFoundError(f"ERREUR : {file_name} introuvable dans data/live_selected — dataset manquant, pas de fallback.")

    path = Path(hit["path"])
    df, sep_used = read_csv_any(path)

    time_col = guess_time_column(df.columns)
    open_col = guess_open_column(df.columns)
    close_col = guess_close_column(df.columns)

    if time_col is None or open_col is None or close_col is None:
        raise Exception(f"Colonnes introuvables dans {path.name} | cols={list(df.columns)}")

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df[open_col] = pd.to_numeric(df[open_col], errors="coerce")
    df[close_col] = pd.to_numeric(df[close_col], errors="coerce")
    df = df.dropna(subset=[time_col, open_col, close_col]).sort_values(time_col)

    if df.empty:
        raise Exception(f"Aucune ligne exploitable dans {path.name}")

    out = df[[time_col, open_col, close_col]].rename(columns={time_col: "time", open_col: "open", close_col: "close"}).copy()
    out["date"] = out["time"].dt.date
    out["week"] = out["time"].dt.strftime("%G-W%V")
    return out, path.name

def extract_threshold_pct(q: str) -> float:
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*%", q)
    return float(m.group(1).replace(",", ".")) if m else 1.0

def extract_times(q: str) -> int:
    m = re.search(r"(\d+)\s*fois", norm(q))
    return int(m.group(1)) if m else 2

def extract_generic_threshold(q: str):
    qn = norm(q)
    patterns = [
        r"(>=)\s*(\d+(?:[.,]\d+)?)",
        r"(<=)\s*(\d+(?:[.,]\d+)?)",
        r"(>)\s*(\d+(?:[.,]\d+)?)",
        r"(<)\s*(\d+(?:[.,]\d+)?)",
    ]
    for p in patterns:
        m = re.search(p, qn)
        if m:
            return m.group(1), float(m.group(2).replace(",", "."))
    return None, None

def is_supported_spx_weekly_question(q: str) -> bool:
    qn = norm(q)
    return (
        "spx" in qn and
        any(x in qn for x in ["baisse", "baiss"]) and
        bool(re.search(r"\d+(?:[.,]\d+)?\s*%", qn)) and
        "fois" in qn and
        ("semaine" in qn or "week" in qn)
    )

def is_single_ratio_question(q: str) -> bool:
    qn = norm(q)
    hits = detect_datasets_in_question(q, max_count=3)
    return ("vix1d/vix" in qn or "vix1d vix" in qn) and (len(hits) == 1)

def is_multi_dataset_question(q: str) -> bool:
    return len(detect_datasets_in_question(q, max_count=3)) >= 2

def append_feedback(question: str, answer: str, kind: str, extra_choice: str = ""):
    row = pd.DataFrame([{
        "SOURCE": "SPX_QUANT_ENGINE_FEEDBACK_V1",
        "timestamp_utc": pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "question": question,
        "answer": answer,
        "type": kind,
        "choice": extra_choice
    }])
    if FEEDBACK_CSV.exists():
        prev = pd.read_csv(FEEDBACK_CSV)
        out = pd.concat([prev, row], ignore_index=True)
    else:
        out = row
    out.to_csv(FEEDBACK_CSV, index=False)

def build_weekly_drop_answer(q: str):
    threshold = extract_threshold_pct(q)
    times = extract_times(q)
    df, dataset_name = load_price_daily_df("SPX")
    df["ret_pct"] = ((df["close"] - df["open"]) / df["open"]) * 100.0
    df["hit"] = df["ret_pct"] <= -threshold
    wk = df.groupby("week", as_index=False)["hit"].sum().rename(columns={"hit": "count"})
    wk = wk[wk["count"] >= times].copy()

    if wk.empty:
        return "Je n'ai trouvé aucune semaine correspondant au critère demandé.", pd.DataFrame()

    total_weeks = int(df["week"].nunique())
    qualified_weeks = int(len(wk))
    avg_per_year = round((qualified_weeks / total_weeks) * 52.0, 2) if total_weeks > 0 else 0.0

    wk["Semaine"] = wk["week"].apply(fmt_week_fr)
    wk["Dataset retenu"] = dataset_name
    wk["Fréquence"] = "daily"
    wk["Occurrences validées"] = wk["count"].astype(int)
    wk = wk.sort_values("week", ascending=False).reset_index(drop=True)
    best = wk.iloc[0]

    txt = (
        f"**{fmt_week_fr(best['week'])}**\n\n"
        f"En moyenne, ce type de semaine arrive **{avg_per_year} fois par an** sur l'historique disponible.\n\n"
        f"---\n"
        f"Dataset utilisé : {dataset_name} (daily)"
    )

    export_df = wk[["Semaine", "week", "Dataset retenu", "Fréquence", "Occurrences validées"]].copy()
    export_df.insert(0, "Question", [""] * len(export_df))
    export_df.loc[0, "Question"] = q
    return txt, export_df

def build_single_ratio_frequency_answer(q: str):
    operator, threshold = extract_generic_threshold(q)
    if operator is None or threshold is None:
        return "Condition numérique introuvable dans la question.", pd.DataFrame()

    ratio_hit = find_ratio_dataset()
    if ratio_hit is None:
        return "ERREUR : Dataset VIX1D_VIX_ratio_daily.csv introuvable dans data/live_selected.", pd.DataFrame()

    ratio_df, ratio_file, _ = load_daily_value_df(ratio_hit["path"])
    ratio_df = ratio_df.sort_values("date")

    if operator == ">":
        filtered = ratio_df[ratio_df["value"] > threshold].copy()
    elif operator == "<":
        filtered = ratio_df[ratio_df["value"] < threshold].copy()
    elif operator == ">=":
        filtered = ratio_df[ratio_df["value"] >= threshold].copy()
    else:
        filtered = ratio_df[ratio_df["value"] <= threshold].copy()

    total_days = int(len(ratio_df))
    matching_days = int(len(filtered))
    pct = round((matching_days / total_days) * 100.0, 2) if total_days > 0 else 0.0

    start_date = fmt_date_fr(ratio_df["date"].min()) if total_days > 0 else ""
    end_date = fmt_date_fr(ratio_df["date"].max()) if total_days > 0 else ""
    period_label = f"Période analysée : du {start_date} au {end_date}." if total_days > 0 else "Période analysée : non disponible."

    txt = (
        f"**La fréquence est de {pct}% sur la période analysée.**\n\n"
        f"Cela représente **{matching_days} jours** sur **{total_days} jours** de l'historique du ratio.\n\n"
        f"{period_label}\n\n"
        f"---\n"
        f"Dataset utilisé : {ratio_file}"
    )

    if filtered.empty:
        return txt, pd.DataFrame()

    filtered["Date"] = pd.to_datetime(filtered["date"]).dt.strftime("%Y-%m-%d")
    export_df = filtered[["Date", "value"]].copy()
    export_df.columns = ["Date", ratio_file]
    export_df.insert(0, "Question", [""] * len(export_df))
    export_df.loc[0, "Question"] = q
    return txt, export_df

def build_multi_dataset_frequency_answer(q: str):
    datasets = detect_datasets_in_question(q, max_count=3)
    if len(datasets) < 2:
        return "Pas assez de datasets détectés dans la question.", pd.DataFrame()

    operator, threshold = extract_generic_threshold(q)
    if operator is None or threshold is None:
        return "Condition numérique introuvable dans la question.", pd.DataFrame()

    qn = norm(q)
    op_positions = [qn.find(op) for op in [">=", "<=", ">", "<"] if qn.find(op) != -1]
    op_pos = min(op_positions) if op_positions else -1
    if op_pos == -1:
        return "Position de la condition introuvable.", pd.DataFrame()

    condition_ds = None
    for ds in datasets:
        pos = qn.find(ds["_matched_alias"])
        if pos != -1 and pos < op_pos:
            condition_ds = ds
    if condition_ds is None:
        condition_ds = datasets[-1]

    subject_ds = None
    for ds in datasets:
        if ds["file"] != condition_ds["file"]:
            subject_ds = ds
            break
    if subject_ds is None:
        return "Dataset principal introuvable.", pd.DataFrame()

    cond_df, cond_file, _ = load_daily_value_df(condition_ds["path"])

    subject_alias_n = norm(subject_ds["_matched_alias"])
    subject_file_n = norm(subject_ds["file"])
    subject_is_price_asset = any(x in subject_alias_n for x in ["spx", "spy", "qqq", "iwm"]) and subject_file_n in {
        "spx_daily.csv", "spy_daily.csv", "qqq_daily.csv", "iwm_daily.csv"
    }

    if subject_is_price_asset:
        subject_asset = "SPX"
        for candidate in ["SPY", "QQQ", "IWM", "SPX"]:
            if candidate.lower() in subject_alias_n:
                subject_asset = candidate
                break
        subj_df, subj_file = load_price_daily_df(subject_asset)
        subj_df["subject_open"] = subj_df["open"]
        subj_df["subject_close"] = subj_df["close"]
        subj_df["subject_var_pct"] = ((subj_df["subject_close"] - subj_df["subject_open"]) / subj_df["subject_open"]) * 100.0
        subj_base = subj_df[["date", "subject_open", "subject_close", "subject_var_pct"]].copy()
    else:
        subj_raw, subj_file, _ = load_daily_value_df(subject_ds["path"])
        subj_base = subj_raw[["date", "value"]].rename(columns={"value": "subject_value"}).copy()

    cond_base = cond_df[["date", "value"]].rename(columns={"value": "condition_value"}).copy()
    merged = pd.merge(subj_base, cond_base, on="date", how="inner").sort_values("date")

    if merged.empty:
        return "Aucune date commune entre les datasets.", pd.DataFrame()

    if operator == ">":
        filtered = merged[merged["condition_value"] > threshold].copy()
    elif operator == "<":
        filtered = merged[merged["condition_value"] < threshold].copy()
    elif operator == ">=":
        filtered = merged[merged["condition_value"] >= threshold].copy()
    else:
        filtered = merged[merged["condition_value"] <= threshold].copy()

    total_common_days = int(len(merged))
    matching_days = int(len(filtered))
    pct_common = round((matching_days / total_common_days) * 100.0, 2) if total_common_days > 0 else 0.0

    start_date = fmt_date_fr(merged["date"].min()) if total_common_days > 0 else ""
    end_date = fmt_date_fr(merged["date"].max()) if total_common_days > 0 else ""
    period_label = f"Période commune analysée : du {start_date} au {end_date}." if total_common_days > 0 else "Période commune analysée : non disponible."

    txt = (
        f"**La fréquence est de {pct_common}% sur la période analysée.**\n\n"
        f"Cela représente **{matching_days} jours** sur **{total_common_days} jours** de l'historique commun.\n\n"
        f"{period_label}\n\n"
        f"---\n"
        f"Datasets utilisés : {subj_file} + {cond_file}"
    )

    if filtered.empty:
        return txt, pd.DataFrame()

    filtered["Date"] = pd.to_datetime(filtered["date"]).dt.strftime("%Y-%m-%d")

    if "subject_open" in filtered.columns:
        export_df = filtered[["Date", "subject_open", "subject_close", "subject_var_pct", "condition_value"]].copy()
        export_df.columns = [
            "Date",
            f"{subject_ds['display_file']} Open",
            f"{subject_ds['display_file']} Close",
            f"{subject_ds['display_file']} Var %",
            f"{condition_ds['display_file']} Value",
        ]
    else:
        export_df = filtered[["Date", "subject_value", "condition_value"]].copy()
        export_df.columns = [
            "Date",
            f"{subject_ds['display_file']} Value",
            f"{condition_ds['display_file']} Value",
        ]

    export_df.insert(0, "Question", [""] * len(export_df))
    export_df.loc[0, "Question"] = q
    return txt, export_df

def build_clarification_intro() -> str:
    return "Question reconnue mais ambiguë.\n\nChoisis ce que tu veux analyser :"

# ── UI ──────────────────────────────────────────────────────────────────────
q = st.text_input("Question", value=DEFAULT_Q, key="main_question")

if "clarif_choice" not in st.session_state:
    st.session_state["clarif_choice"] = ""
if "prev_q" not in st.session_state:
    st.session_state["prev_q"] = q

if st.session_state["prev_q"] != q:
    st.session_state["clarif_choice"] = ""
    st.session_state["prev_q"] = q

resp_col, sig_col = st.columns([5.4, 1.2])

with resp_col:
    st.markdown("## Réponse")
    txt = ""
    export_df = pd.DataFrame()

    if is_supported_spx_weekly_question(q):
        st.session_state["clarif_choice"] = ""
        try:
            txt, export_df = build_weekly_drop_answer(q)
            st.write(txt)
        except Exception as e:
            st.error(str(e))

    elif is_single_ratio_question(q):
        choice = st.session_state.get("clarif_choice", "")
        if choice == "frequence":
            try:
                txt, export_df = build_single_ratio_frequency_answer(q)
                st.write(txt)
            except Exception as e:
                st.error(str(e))
        elif choice == "variation":
            txt = "Analyse variation ratio : à implémenter."
            st.write(txt)
        elif choice == "direction":
            txt = "Analyse direction ratio : à implémenter."
            st.write(txt)
        elif choice == "horizon":
            txt = "Analyse horizon ratio : à implémenter."
            st.write(txt)
        else:
            st.write(build_clarification_intro())

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Variation"):
                st.session_state["clarif_choice"] = "variation"
                append_feedback(q, "Choix variation", "clarification_choice", "variation")
                st.rerun()
            if st.button("Direction"):
                st.session_state["clarif_choice"] = "direction"
                append_feedback(q, "Choix direction", "clarification_choice", "direction")
                st.rerun()
        with c2:
            if st.button("Fréquence"):
                st.session_state["clarif_choice"] = "frequence"
                append_feedback(q, "Choix frequence", "clarification_choice", "frequence")
                st.rerun()
            if st.button("Horizon"):
                st.session_state["clarif_choice"] = "horizon"
                append_feedback(q, "Choix horizon", "clarification_choice", "horizon")
                st.rerun()

    elif is_multi_dataset_question(q):
        choice = st.session_state.get("clarif_choice", "")
        if choice == "frequence":
            try:
                txt, export_df = build_multi_dataset_frequency_answer(q)
                st.write(txt)
            except Exception as e:
                st.error(str(e))
        elif choice == "variation":
            txt = "Analyse variation multi-datasets : à implémenter."
            st.write(txt)
        elif choice == "direction":
            txt = "Analyse direction multi-datasets : à implémenter."
            st.write(txt)
        elif choice == "horizon":
            txt = "Analyse horizon multi-datasets : à implémenter."
            st.write(txt)
        else:
            st.write(build_clarification_intro())

        c1, c2 = st.columns(2)
        with c1:
            if st.button("Variation", key="mv"):
                st.session_state["clarif_choice"] = "variation"
                append_feedback(q, "Choix variation", "clarification_choice", "variation")
                st.rerun()
            if st.button("Direction", key="md"):
                st.session_state["clarif_choice"] = "direction"
                append_feedback(q, "Choix direction", "clarification_choice", "direction")
                st.rerun()
        with c2:
            if st.button("Fréquence", key="mf"):
                st.session_state["clarif_choice"] = "frequence"
                append_feedback(q, "Choix frequence", "clarification_choice", "frequence")
                st.rerun()
            if st.button("Horizon", key="mh"):
                st.session_state["clarif_choice"] = "horizon"
                append_feedback(q, "Choix horizon", "clarification_choice", "horizon")
                st.rerun()

    else:
        txt = "Question reconnue mais pas encore implémentée dans cette version."
        st.write(txt)

with sig_col:
    st.markdown("## Signaler")
    if st.button("Réponse fausse"):
        append_feedback(q, txt, "false_answer")
        st.success("Enregistré")
    if st.button("Question non gérée"):
        append_feedback(q, txt, "not_handled")
        st.success("Enregistré")

if not export_df.empty:
    st.markdown("## Tableau résultat")
    st.dataframe(export_df, hide_index=True)
    st.download_button(
        "Télécharger le résultat en CSV",
        export_df.to_csv(index=False).encode("utf-8"),
        "resultat_spx_quant_engine.csv",
        "text/csv",
    )
