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
        "à": "a", "â": "a", "î": "i", "ï": "i",
        "ô": "o", "ù": "u", "û": "u", "ü": "u",
        "ç": "c", "\u2018": "'", "\u2019": "'",
        "≥": ">=", "≤": "<="
    }
    for k, v in repl.items():
        t = t.replace(k, v)
    return re.sub(r"\s+", " ", t)

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
    lower = {str(c).lower(): c for c in cols}
    for exact in ["time", "datetime", "date", "timestamp"]:
        if exact in lower:
            return lower[exact]
    for c in cols:
        if "time" in str(c).lower() or "date" in str(c).lower():
            return c
    return None

def guess_open_column(cols):
    lower = {str(c).lower(): c for c in cols}
    if "open" in lower:
        return lower["open"]
    for c in cols:
        if "open" in str(c).lower():
            return c
    return None

def guess_close_column(cols):
    lower = {str(c).lower(): c for c in cols}
    if "close" in lower:
        return lower["close"]
    for c in cols:
        if "close" in str(c).lower():
            return c
    return None

def guess_value_column(cols):
    for p in ["ratio_vix1d_vix", "ratio", "close", "open", "value", "last"]:
        for c in cols:
            if p in str(c).lower():
                return c
    for c in cols:
        if all(x not in str(c).lower() for x in ["time", "date", "timestamp"]):
            return c
    return None

def detect_freq(file_name: str) -> str:
    n = norm(file_name)
    if "1min" in n or "1_min" in n or "_1min" in n: return "1min"
    if "5min" in n or "5_min" in n or "_5min" in n: return "5min"
    if "15min" in n or "15_min" in n: return "15min"
    if "30min" in n or "30_min" in n or "_30min" in n: return "30min"
    if "4hour" in n or "4h" in n: return "4h"
    if "1hour" in n or "hour" in n: return "1h"
    if "daily" in n or n.endswith("_daily.csv") or ", 1d" in n: return "daily"
    return "unknown"

def detect_dataset_kind(folder_name: str, file_name: str) -> str:
    file_n = norm(file_name)
    if "option_chain" in file_n: return "options"
    if "average range" in file_n: return "move_average"
    if "correlation" in file_n: return "correlation"
    if "vix1d_vix_ratio" in file_n or ("vix1d" in file_n and "vix" in file_n and "ratio" in file_n): return "ratio"
    if "ratio" in file_n or "put_call" in file_n or "put call" in file_n: return "ratio"
    return "standard"

def build_aliases(path: Path, dataset_kind: str) -> list:
    file_n = norm(path.name)
    aliases = set()
    if dataset_kind == "standard":
        if "spx_daily" in file_n: aliases.update({"spx", "s&p500", "sp500"})
        elif "spy_daily" in file_n: aliases.add("spy")
        elif "qqq_daily" in file_n: aliases.add("qqq")
        elif "iwm_daily" in file_n: aliases.add("iwm")
        elif file_n == "vix_daily.csv": aliases.add("vix")
        elif "vvix" in file_n: aliases.add("vvix")
        elif "skew" in file_n: aliases.update({"skew", "skew index"})
        elif "dxy" in file_n: aliases.add("dxy")
        elif "gold" in file_n and "daily" in file_n: aliases.update({"gold", "or"})
        elif "vix3m" in file_n: aliases.add("vix3m")
        elif "vix6m" in file_n: aliases.add("vix6m")
        elif "vix9d" in file_n: aliases.add("vix9d")
    if dataset_kind == "ratio" and "vix1d" in file_n and "vix" in file_n:
        aliases.update({"vix1d/vix", "ratio vix1d/vix"})
    return sorted(aliases)

@st.cache_data(show_spinner=False)
def scan_catalog() -> pd.DataFrame:
    rows = []
    if not DATA_ROOT.exists():
        return pd.DataFrame(columns=["file","display_file","path","folder","freq","aliases","dataset_kind","eligible_detection"])
    for f in sorted(DATA_ROOT.rglob("*.csv")):
        if "__macosx" in norm(str(f.relative_to(DATA_ROOT))):
            continue
        folder = f.parent.name
        kind = detect_dataset_kind(folder, f.name)
        freq = detect_freq(f.name)
        rows.append({
            "file": f.name, "display_file": f.name, "path": str(f),
            "folder": folder, "freq": freq, "aliases": build_aliases(f, kind),
            "dataset_kind": kind,
            "eligible_detection": (freq == "daily" and kind in {"standard", "ratio"}),
        })
    return pd.DataFrame(rows)

CATALOG = scan_catalog()

def get_by_file(file_name: str):
    if CATALOG.empty: return None
    hit = CATALOG[CATALOG["file"] == file_name]
    return hit.iloc[0].to_dict() if not hit.empty else None

def find_ratio_dataset():
    exact = get_by_file("VIX1D_VIX_ratio_daily.csv")
    if exact: return exact
    tmp = CATALOG[(CATALOG["dataset_kind"]=="ratio") & (CATALOG["freq"]=="daily")].copy()
    if tmp.empty: return None
    fn = tmp["file"].map(norm)
    tmp = tmp[fn.str.contains("vix1d") & fn.str.contains("vix")]
    if tmp.empty: return None
    tmp = tmp.copy()
    tmp["score"] = tmp["file"].map(norm).str.contains("ratio").astype(int) * 10
    return tmp.sort_values("score", ascending=False).iloc[0].to_dict()

# Actifs prix (sujet)
PRICE_ASSETS = {"spx": "SPX_daily.csv", "spy": "SPY_daily.csv", "qqq": "QQQ_daily.csv", "iwm": "IWM_daily.csv"}

# Actifs condition (triés du plus long au plus court pour éviter collision vix1d/vix vs vix)
CONDITION_ASSETS_ORDER = ["vix1d/vix", "vix1d vix", "vvix", "vix3m", "vix9d", "vix6m", "skew", "dxy", "gold", "vix"]
CONDITION_FILES = {
    "vix1d/vix": None,  # géré par find_ratio_dataset()
    "vix1d vix": None,
    "vix": "VIX_daily.csv",
    "vvix": "VVIX_daily.csv",
    "vix3m": "VIX3M_daily.csv",
    "vix9d": "VIX9D_daily.csv",
    "vix6m": "VIX6M_daily.csv",
    "skew": "SKEW_INDEX_daily.csv",
    "dxy": "DXY_daily.csv",
    "gold": "Gold_daily.csv",
}

def extract_threshold(q: str):
    qn = norm(q)
    for p in [r"(>=)\s*(\d+(?:[.,]\d+)?)", r"(<=)\s*(\d+(?:[.,]\d+)?)",
              r"(>)\s*(\d+(?:[.,]\d+)?)", r"(<)\s*(\d+(?:[.,]\d+)?)"]:
        m = re.search(p, qn)
        if m:
            return m.group(1), float(m.group(2).replace(",", "."))
    return None, None

def parse_question(q: str):
    qn = norm(q)
    subject = next((a for a in PRICE_ASSETS if a in qn), None)
    condition_asset = next((a for a in CONDITION_ASSETS_ORDER if a in qn), None)
    operator, threshold = extract_threshold(q)
    if condition_asset is None or operator is None:
        return None
    return {"subject": subject, "condition_asset": condition_asset, "operator": operator, "threshold": threshold}

@st.cache_data(show_spinner=False)
def load_value_df(path_str: str):
    path = Path(path_str)
    df, _ = read_csv_any(path)
    tc = guess_time_column(df.columns)
    vc = guess_value_column(df.columns)
    if tc is None: raise Exception(f"Colonne date introuvable dans {path.name}")
    if vc is None: raise Exception(f"Colonne valeur introuvable dans {path.name}")
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df[vc] = pd.to_numeric(df[vc], errors="coerce")
    df = df.dropna(subset=[tc, vc]).sort_values(tc)
    if df.empty: raise Exception(f"Aucune ligne exploitable dans {path.name}")
    out = df[[tc, vc]].rename(columns={tc: "time", vc: "value"}).copy()
    out["date"] = out["time"].dt.date
    return out, path.name

@st.cache_data(show_spinner=False)
def load_price_df(asset_alias: str):
    fn = PRICE_ASSETS[asset_alias]
    hit = get_by_file(fn)
    if hit is None: raise FileNotFoundError(f"ERREUR : {fn} introuvable.")
    path = Path(hit["path"])
    df, _ = read_csv_any(path)
    tc = guess_time_column(df.columns)
    oc = guess_open_column(df.columns)
    cc = guess_close_column(df.columns)
    if None in (tc, oc, cc): raise Exception(f"Colonnes introuvables dans {fn}")
    df[tc] = pd.to_datetime(df[tc], errors="coerce")
    df[oc] = pd.to_numeric(df[oc], errors="coerce")
    df[cc] = pd.to_numeric(df[cc], errors="coerce")
    df = df.dropna(subset=[tc, oc, cc]).sort_values(tc)
    if df.empty: raise Exception(f"Aucune ligne exploitable dans {fn}")
    out = df[[tc, oc, cc]].rename(columns={tc:"time", oc:"open", cc:"close"}).copy()
    out["date"] = out["time"].dt.date
    out["week"] = out["time"].dt.strftime("%G-W%V")
    return out, fn

def load_cond_df(condition_asset: str):
    if condition_asset in ("vix1d/vix", "vix1d vix"):
        hit = find_ratio_dataset()
        if hit is None: raise FileNotFoundError("ERREUR : VIX1D_VIX_ratio_daily.csv introuvable.")
        return load_value_df(hit["path"])
    fn = CONDITION_FILES.get(condition_asset)
    if fn is None: raise FileNotFoundError(f"Actif '{condition_asset}' non reconnu.")
    hit = get_by_file(fn)
    if hit is None: raise FileNotFoundError(f"ERREUR : {fn} introuvable.")
    return load_value_df(hit["path"])

def apply_op(df, col, op, thr):
    ops = {">": df[col] > thr, "<": df[col] < thr, ">=": df[col] >= thr, "<=": df[col] <= thr}
    return df[ops[op]].copy()

def build_answer(q: str):
    parsed = parse_question(q)
    if parsed is None:
        return "Question non reconnue. Exemple : **SPX quand VIX > 18** ou **SPX quand VIX1D/VIX > 1.2**", pd.DataFrame()

    subject = parsed["subject"]
    ca = parsed["condition_asset"]
    op = parsed["operator"]
    thr = parsed["threshold"]

    cond_df, cond_file = load_cond_df(ca)
    filtered = apply_op(cond_df, "value", op, thr)
    total = len(cond_df)
    match = len(filtered)
    pct = round(match / total * 100, 2) if total > 0 else 0

    txt = (
        f"**{ca.upper()} {op} {thr} : {pct}% des jours** ({match} jours sur {total})\n\n"
        f"Période : du {fmt_date_fr(cond_df['date'].min())} au {fmt_date_fr(cond_df['date'].max())}.\n\n"
        f"Dataset condition : {cond_file}"
    )

    if subject is None:
        exp = filtered[["date","value"]].copy()
        exp.columns = ["Date", cond_file]
        exp["Date"] = pd.to_datetime(exp["Date"]).dt.strftime("%Y-%m-%d")
        exp.insert(0, "Question", "")
        if not exp.empty: exp.iloc[0, 0] = q
        return txt, exp

    subj_df, subj_file = load_price_df(subject)
    merged = pd.merge(
        filtered[["date","value"]].rename(columns={"value":"cond_value"}),
        subj_df[["date","open","close"]], on="date", how="inner"
    ).sort_values("date")

    if merged.empty:
        return txt + "\n\nAucune date commune entre les deux datasets.", pd.DataFrame()

    merged["var_pct"] = ((merged["close"] - merged["open"]) / merged["open"]) * 100.0
    n = len(merged)
    avg = round(merged["var_pct"].mean(), 3)
    pos = round((merged["var_pct"] > 0).sum() / n * 100, 1)
    neg = round((merged["var_pct"] < 0).sum() / n * 100, 1)

    txt += (
        f"\n\n---\n**{subject.upper()} ces {n} jours :**\n\n"
        f"- Variation moyenne open→close : **{avg:+.3f}%**\n"
        f"- Jours haussiers : **{pos}%** | Jours baissiers : **{neg}%**\n\n"
        f"Dataset : {subj_file}"
    )

    exp = merged[["date","open","close","var_pct","cond_value"]].copy()
    exp.columns = ["Date", f"{subject.upper()} Open", f"{subject.upper()} Close",
                   f"{subject.upper()} Var%", f"{ca.upper()} Value"]
    exp["Date"] = pd.to_datetime(exp["Date"]).dt.strftime("%Y-%m-%d")
    exp.insert(0, "Question", "")
    if not exp.empty: exp.iloc[0, 0] = q
    return txt, exp

def is_weekly_drop(q: str) -> bool:
    qn = norm(q)
    return ("spx" in qn and any(x in qn for x in ["baisse","baiss"])
            and bool(re.search(r"\d+(?:[.,]\d+)?\s*%", qn))
            and "fois" in qn and ("semaine" in qn or "week" in qn))

def build_weekly_drop(q: str):
    thr = float(re.search(r"(\d+(?:[.,]\d+)?)\s*%", q).group(1).replace(",",".")) if re.search(r"(\d+(?:[.,]\d+)?)\s*%", q) else 1.0
    times = int(re.search(r"(\d+)\s*fois", norm(q)).group(1)) if re.search(r"(\d+)\s*fois", norm(q)) else 2
    df, dname = load_price_df("spx")
    df["ret"] = ((df["close"] - df["open"]) / df["open"]) * 100.0
    df["hit"] = df["ret"] <= -thr
    wk = df.groupby("week", as_index=False)["hit"].sum().rename(columns={"hit":"count"})
    wk = wk[wk["count"] >= times].copy()
    if wk.empty: return "Aucune semaine correspondant au critère.", pd.DataFrame()
    total_wk = df["week"].nunique()
    avg = round(len(wk) / total_wk * 52, 2)
    wk["Semaine"] = wk["week"].apply(fmt_week_fr)
    wk = wk.sort_values("week", ascending=False)
    txt = f"**{fmt_week_fr(wk.iloc[0]['week'])}**\n\nEn moyenne **{avg} fois par an**.\n\nDataset : {dname}"
    exp = wk[["Semaine","week"]].copy()
    exp.insert(0, "Question", ""); exp.iloc[0,0] = q
    return txt, exp

def append_feedback(question, answer, kind, choice=""):
    row = pd.DataFrame([{"SOURCE":"SPX_QUANT_ENGINE_FEEDBACK_V1",
        "timestamp_utc": pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "question": question, "answer": answer, "type": kind, "choice": choice}])
    if FEEDBACK_CSV.exists():
        row = pd.concat([pd.read_csv(FEEDBACK_CSV), row], ignore_index=True)
    row.to_csv(FEEDBACK_CSV, index=False)

# ── UI ───────────────────────────────────────────────────────────────────────
q = st.text_input("Question", value=DEFAULT_Q, key="main_question")
resp_col, sig_col = st.columns([5.4, 1.2])

with resp_col:
    st.markdown("## Réponse")
    txt, export_df = "", pd.DataFrame()
    try:
        if is_weekly_drop(q):
            txt, export_df = build_weekly_drop(q)
        else:
            txt, export_df = build_answer(q)
        st.markdown(txt)
    except Exception as e:
        st.error(f"Erreur : {e}")

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
    st.download_button("Télécharger CSV",
        export_df.to_csv(index=False).encode("utf-8"),
        "resultat_spx_quant_engine.csv", "text/csv")
