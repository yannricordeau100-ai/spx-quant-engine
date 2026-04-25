# app_local.py — SPX Quant Engine LOCAL (CLI + Streamlit UI)
# Couche 1 : regex (logique identique à app.py, sans @st.cache_data)
# Couche 2 : Ollama (llama3.2:3b) + DuckDB (tous les CSV de data/live_selected/)
#
# Usage CLI : python3 app_local.py "question"
# Usage UI  : python3 -m streamlit run app_local.py --server.port 8503

import json
import math
import os
import re
import sys
import threading
import time
import unicodedata
import urllib.request
from functools import lru_cache
from pathlib import Path

import duckdb
import pandas as pd
import pytz

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "live_selected"
TICKERS_DIR = DATA_DIR / "tickers"
TICKERS_DIR.mkdir(exist_ok=True)

# Import optionnel du module patterns (non bloquant)
try:
    from patterns_v2 import launch_background as _patterns_launch
    _PATTERNS_AVAILABLE = True
except ImportError:
    _PATTERNS_AVAILABLE = False

# Import module ticker_analysis
try:
    from ticker_analysis import analyze_ticker as _ticker_analyze
    _TICKER_AVAILABLE = True
except ImportError:
    _TICKER_AVAILABLE = False

# Import interpréteur sémantique + exécuteur
try:
    from query_interpreter import interpret_query as _interpret
    from query_executor import execute_query as _execute
    _INTERPRETER_AVAILABLE = True
except ImportError:
    _INTERPRETER_AVAILABLE = False

# Import SPX patterns overnight
try:
    from spx_patterns import find_active_patterns as _spx_active, get_all_patterns as _spx_all
    _SPX_PATTERNS_AVAILABLE = True
except Exception:
    _SPX_PATTERNS_AVAILABLE = False

VERSION_LOCAL   = "v2.21.3"
_MAX_FOLLOWUP_TURNS = 5
HISTORY_FILE      = BASE_DIR / "data" / "history.json"
TOKEN_FLAG_FILE   = BASE_DIR / ".token_warning"
PATTERNS_FLAG_FILE = BASE_DIR / "data" / ".patterns_ready"
OLLAMA_URL      = "http://localhost:11434/api/generate"
OLLAMA_MODEL    = "sqlcoder:latest"
OLLAMA_IDLE_SEC = 5

# ─── Few-shot prompt (statique) ───────────────────────────────────────────

_DOW_FR = {"0":"Dimanche","1":"Lundi","2":"Mardi","3":"Mercredi","4":"Jeudi","5":"Vendredi","6":"Samedi"}
_DOW_CASE = ("CASE strftime('%w',time)"
             " WHEN '1' THEN 'Lundi' WHEN '2' THEN 'Mardi' WHEN '3' THEN 'Mercredi'"
             " WHEN '4' THEN 'Jeudi' WHEN '5' THEN 'Vendredi' END")

_SYSTEM_PROMPT = """\
Tu es un expert SQL/DuckDB. Génère UNIQUEMENT du SQL valide DuckDB sans explication ni commentaire.
Règles : variation daily = (close - LAG(close) OVER (ORDER BY time)) / LAG(close) OVER (ORDER BY time) * 100. Jamais de colonnes inexistantes, ABS() pour variation absolue, noms de jours en français via CASE strftime('%w',time).

Q: variation SPX en 2022 ?
SQL: SELECT (last.close - first.open)/first.open*100 as perf_pct FROM (SELECT close FROM spx_daily WHERE strftime('%Y',time)='2022' ORDER BY time DESC LIMIT 1) last, (SELECT open FROM spx_daily WHERE strftime('%Y',time)='2022' ORDER BY time ASC LIMIT 1) first

Q: meilleur jour de la semaine pour le SPX ?
SQL: WITH d AS (SELECT *, LAG(close) OVER (ORDER BY time) as prev_close FROM spx_daily) SELECT CASE strftime('%w',time) WHEN '1' THEN 'Lundi' WHEN '2' THEN 'Mardi' WHEN '3' THEN 'Mercredi' WHEN '4' THEN 'Jeudi' WHEN '5' THEN 'Vendredi' END as jour, AVG((close-prev_close)/prev_close*100) as avg_var FROM d WHERE prev_close IS NOT NULL AND strftime('%w',time) IN ('1','2','3','4','5') GROUP BY strftime('%w',time) ORDER BY avg_var DESC LIMIT 1

Q: put/call ratio moyen du SPX quand VIX > 20 ?
SQL: SELECT AVG(p.close) as avg_pc_ratio, COUNT(*) as nb_jours FROM spx_put_call_ratio_daily p JOIN vix_daily v ON p.time = v.time WHERE v.close > 20

Q: combien de jours de bourse par année pour le SPX ?
SQL: SELECT strftime('%Y',time) as annee, COUNT(*) as nb_jours FROM spx_daily GROUP BY annee ORDER BY annee

Q: range moyen en points du SPX sur barres 30min ?
SQL: SELECT DATE(time) as date, ROUND(MAX(high)-MIN(low),2) as range_pts FROM spx_30min GROUP BY DATE(time) ORDER BY range_pts DESC LIMIT 10

Q: performance SPX par jour de la semaine ?
SQL: WITH d AS (SELECT *, LAG(close) OVER (ORDER BY time) as prev_close FROM spx_daily) SELECT CASE strftime('%w',time) WHEN '1' THEN 'Lundi' WHEN '2' THEN 'Mardi' WHEN '3' THEN 'Mercredi' WHEN '4' THEN 'Jeudi' WHEN '5' THEN 'Vendredi' END as jour, AVG((close-prev_close)/prev_close*100) as avg_var, COUNT(*) as nb_jours FROM d WHERE prev_close IS NOT NULL AND strftime('%w',time) IN ('1','2','3','4','5') GROUP BY strftime('%w',time) ORDER BY strftime('%w',time)

Q: jours où SPX est resté dans un range +/-15pts dans les 30min après ouverture quand VIX1D/VIX > 1.0 en 2025 ?
SQL: SELECT COUNT(*) as nb_jours FROM (SELECT date(s.time) FROM spx_30min s JOIN vix1d_vix_ratio_daily r ON date(s.time)=date(r.time) WHERE strftime('%Y',s.time)='2025' AND strftime('%H:%M',s.time) BETWEEN '09:30' AND '10:00' AND r.open > 1.0 GROUP BY date(s.time) HAVING MAX(s.high)-MIN(s.low) <= 15)

Q: VVIX moyen les lundis et mardis ?
SQL: SELECT CASE strftime('%w',time) WHEN '1' THEN 'Lundi' WHEN '2' THEN 'Mardi' END as jour, ROUND(AVG(close),2) as avg_vvix, COUNT(*) as nb_jours FROM vvix_daily WHERE strftime('%w',time) IN ('1','2') GROUP BY strftime('%w',time) ORDER BY strftime('%w',time)

Q: combien de séances SPX en 2025 avec VIX supérieur à 18 ?
SQL: SELECT COUNT(*) as nb_seances FROM spx_daily s JOIN vix_daily v ON date(s.time)=date(v.time) WHERE strftime('%Y',s.time)='2025' AND v.close > 18

Q: VVIX moyen les lundis et mardis quand VIX > 20 ?
SQL: SELECT CASE strftime('%w',v.time) WHEN '1' THEN 'Lundi' WHEN '2' THEN 'Mardi' END as jour, ROUND(AVG(v.close),2) as avg_vvix, COUNT(*) as nb_jours FROM vvix_daily v JOIN vix_daily vx ON v.time=vx.time WHERE strftime('%w',v.time) IN ('1','2') AND vx.close > 20 GROUP BY strftime('%w',v.time) ORDER BY strftime('%w',v.time)

Q: variation moyenne clôture J vers open J+1 du SPX par mois ?
SQL: SELECT strftime('%Y-%m',s1.time) as mois, ROUND(AVG((s2.open - s1.close)/s1.close*100),4) as overnight_pct, COUNT(*) as nb_jours FROM spx_daily s1 JOIN spx_daily s2 ON date(s2.time) = date(s1.time, '+1 day') GROUP BY mois ORDER BY mois

Q: dates où AAOI a perdu 5% ou plus en daily et % de jours positifs le lendemain ?
SQL: WITH drops AS (SELECT time, close, LAG(close) OVER (ORDER BY time) as prev_close FROM aaoi), next_day AS (SELECT d.time, d.close, d.prev_close, LEAD(d.close) OVER (ORDER BY d.time) as next_close FROM drops d WHERE (d.close-d.prev_close)/d.prev_close*100 <= -5) SELECT COUNT(*) as nb_fois, ROUND(100.0*SUM(CASE WHEN next_close > close THEN 1 ELSE 0 END)/COUNT(*),1) as pct_positif_lendemain FROM next_day WHERE next_close IS NOT NULL
"""

# ─── Timezone ─────────────────────────────────────────────────────────────

_TZ_PARIS = pytz.timezone("Europe/Paris")
_TZ_NY    = pytz.timezone("America/New_York")

_PARIS_FILES = {
    "DAX40_daily.csv", "FTSE100_daily.csv", "NIKKEI225_daily.csv",
    "Gold_daily.csv", "Gold_1hour.csv", "DXY_daily.csv",
    "OANDA_USB02YUSD, 1D.csv", "OANDA_USB10YUSD, 1D.csv",
    "Yield_Curve_Spread_10Y_2Y.csv",
    "SPX_1min.csv", "SPX_5min.csv", "SPX_30min.csv",
    "SPY_1min.csv", "SPY_30min.csv",
    "QQQ_1_min.csv", "QQQ_30min.csv",
    "IWM_30_min.csv",
    "VIX1D_1min.csv", "VIX1D_30min.csv",
    "SPX_FUTURE_1min.csv", "SPX_FUTURE_5min.csv", "SPX_FUTURE_30min.csv",
    "oil_5min.csv", "TICK_4hours.csv",
}

# ─── Couche 1 : registry + parsing ────────────────────────────────────────

_HC_CONDITIONS = {
    "vix1d/vix":    ("VIX1D_VIX_ratio_daily.csv",                            "open"),
    "vix1d_vix":    ("VIX1D_VIX_ratio_daily.csv",                            "open"),
    "vix9d":        ("VIX9D_daily.csv",                                       "close"),
    "vix3m":        ("VIX3M_daily.csv",                                       "close"),
    "vix6m":        ("VIX6M_daily.csv",                                       "close"),
    "vvix":         ("VVIX_daily.csv",                                        "close"),
    "vix":          ("VIX_daily.csv",                                         "close"),
    "skew":         ("SKEW_INDEX_daily.csv",                                  "close"),
    "dxy":          ("DXY_daily.csv",                                         "close"),
    "gold":         ("Gold_daily.csv",                                        "close"),
    "nikkei":       ("NIKKEI225_daily.csv",                                   "close"),
    "dax":          ("DAX40_daily.csv",                                       "close"),
    "ftse":         ("FTSE100_daily.csv",                                     "close"),
    "spx put-call": ("SPX_Put_Call_Ratio_daily.csv",                          "close"),
    "spx put/call": ("SPX_Put_Call_Ratio_daily.csv",                          "close"),
    "spx pcr":      ("SPX_Put_Call_Ratio_daily.csv",                          "close"),
    "qqq put-call": ("QQQ_Put_Call_Ratio_daily.csv",                          "close"),
    "qqq put/call": ("QQQ_Put_Call_Ratio_daily.csv",                          "close"),
    "qqq pcr":      ("QQQ_Put_Call_Ratio_daily.csv",                          "close"),
    "spy put-call": ("SPY_Put_Call_Ratio_daily.csv",                          "close"),
    "spy put/call": ("SPY_Put_Call_Ratio_daily.csv",                          "close"),
    "spy pcr":      ("SPY_Put_Call_Ratio_daily.csv",                          "close"),
    "iwm put-call": ("IWM_Put_Call_Ratio_daily.csv",                          "close"),
    "iwm put/call": ("IWM_Put_Call_Ratio_daily.csv",                          "close"),
    "iwm pcr":      ("IWM_Put_Call_Ratio_daily.csv",                          "close"),
    "vix put-call": ("VIX_Put_Call_Ratio_daily.csv",                          "close"),
    "vix put/call": ("VIX_Put_Call_Ratio_daily.csv",                          "close"),
    "vix pcr":      ("VIX_Put_Call_Ratio_daily.csv",                          "close"),
    "yield curve":  ("Yield_Curve_Spread_10Y_2Y.csv", "spread_10Y_minus_2Y"),
    "yield_curve":  ("Yield_Curve_Spread_10Y_2Y.csv", "spread_10Y_minus_2Y"),
    "us10y":        ("US_10_years_bonds_daily.csv",                           "close"),
    "us 10y":       ("US_10_years_bonds_daily.csv",                           "close"),
    "10y":          ("US_10_years_bonds_daily.csv",                           "close"),
    "bonds":        ("US_10_years_bonds_daily.csv",                           "close"),
    "advance-decline": ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
    "advance decline": ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
    "adv-dec":         ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
    "adv dec":         ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
}

_HC_SUBJECTS = {
    "spx": "SPX_daily.csv", "spy": "SPY_daily.csv",
    "qqq": "QQQ_daily.csv", "iwm": "IWM_daily.csv",
    "aapl": "AAPL.csv",     "aaoi": "AAOI.csv",
}

_SKIP_RE = re.compile(
    r"_1min\.csv$|_5min\.csv$|_30min\.csv$|_1hour\.csv$|_4hours\.csv$"
    r"|option_chain|calendar_events", re.IGNORECASE,
)

_WEEKDAYS = {
    "lundi":0,"lundis":0,"mardi":1,"mardis":1,"mercredi":2,"mercredis":2,
    "jeudi":3,"jeudis":3,"vendredi":4,"vendredis":4,
    "monday":0,"tuesday":1,"wednesday":2,"thursday":3,"friday":4,
}
_WEEKDAY_LABELS = {0:"lundis",1:"mardis",2:"mercredis",3:"jeudis",4:"vendredis"}

_OVERNIGHT_POS_RE = re.compile(
    r"ouvre\s+en\s+positif|ouverture\s+positive|ouverture\s+en\s+hausse"
    r"|gap[\s-]?up|open\s+sup[eé]rieur|gap\s+haussier", re.IGNORECASE)
_OVERNIGHT_NEG_RE = re.compile(
    r"ouvre\s+en\s+n[eé]gatif|ouverture\s+n[eé]gative|ouverture\s+en\s+baisse"
    r"|gap[\s-]?down|open\s+inf[eé]rieur|gap\s+baissier", re.IGNORECASE)
_INTRADAY_RE = re.compile(
    r"(\d+)\s*min(?:utes?)?\s*apr[eè]s|open\s*\+\s*(\d+)|(\d+)\s*h(?:eure)?\s*apr[eè]s",
    re.IGNORECASE)
_CAL_EVENTS = [
    (r"\bemploi\b|\bchômage\b|\bchomage\b|\bnfp\b|\bjobless\b|\bpayroll\b",
     ["jobless","nfp","employment","payroll","labor"]),
    (r"\bcpi\b|\binflation\b", ["cpi","inflation","consumer price"]),
    (r"\bfomc\b|\bfed\b", ["fomc","fed funds","federal reserve"]),
    (r"\bpmi\b", ["pmi"]),
    (r"\bism\b", ["ism"]),
    (r"\bpce\b", ["pce"]),
    (r"\bearnings\b|\brésultats\b|\bresultats\b", ["earnings"]),
]
_SURPRISE_POS_RE = re.compile(
    r"meilleures?\s+qu[e']?\s*annonc[eé]|better\s+than\s+expected", re.IGNORECASE)
_SURPRISE_NEG_RE = re.compile(
    r"moins\s+bonnes?\s+qu[e']?\s*annonc[eé]|worse\s+than\s+expected", re.IGNORECASE)
_KEYWORD_RE = re.compile(
    r"\b(?:quand|si|when|if|avec|après|apres|sur|pour|lors)\b", re.IGNORECASE)


def _ticker_from_path(path: Path) -> str:
    name = path.stem
    name = re.sub(r"_daily$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[,\s]+1D$", "", name, flags=re.IGNORECASE)
    return re.sub(r"[\s_]+", "_", name.lower().strip()).strip("_")


def _peek_columns(path: Path) -> list:
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            header = f.readline().strip().lstrip("\ufeff")
        return [c.strip().lower() for c in header.split(";")]
    except Exception:
        return []


@lru_cache(maxsize=1)
def _build_dynamic_registry() -> tuple:
    cond, subj = {}, {}
    if not DATA_DIR.exists():
        return cond, subj
    for path in sorted(DATA_DIR.glob("*.csv")):
        if _SKIP_RE.search(path.name):
            continue
        ticker = _ticker_from_path(path)
        if not ticker:
            continue
        cols = _peek_columns(path)
        if not cols or "time" not in cols:
            continue
        val_col = next((c for c in ("close","open") if c in cols),
                       next((c for c in cols if c != "time"), None))
        if val_col is None:
            continue
        cond[ticker] = (path.name, val_col)
        if "open" in cols and "close" in cols:
            subj[ticker] = path.name
    return cond, subj


@lru_cache(maxsize=1)
def get_effective_registries() -> tuple:
    dyn_cond, dyn_subj = _build_dynamic_registry()
    return {**dyn_cond, **_HC_CONDITIONS}, {**dyn_subj, **_HC_SUBJECTS}


def _paris_to_ny(df: pd.DataFrame) -> pd.DataFrame:
    if df["time"].dt.tz is not None or df["time"].dt.hour.max() == 0:
        return df
    try:
        df = df.copy()
        df["time"] = (df["time"]
                      .dt.tz_localize(_TZ_PARIS, ambiguous="NaT", nonexistent="NaT")
                      .dt.tz_convert(_TZ_NY).dt.tz_localize(None))
    except Exception:
        pass
    return df


@lru_cache(maxsize=64)
def _load_csv(fname: str) -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / fname, sep=";")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    if fname in _PARIS_FILES:
        df = _paris_to_ny(df)
    return df


def _to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s.astype(str).str.replace(",", ".").str.strip(), errors="coerce")


def _detect_weekday(q):
    for name, day in _WEEKDAYS.items():
        if re.search(rf"\b(?:les?|the)\s+{name}\b", q, re.IGNORECASE):
            return day
    return None


def _detect_subject(q, eff_subj):
    m = _KEYWORD_RE.search(q)
    prefix = q[:m.start()] if m else q
    for s in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", prefix, re.IGNORECASE):
            return s
    for s in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", q, re.IGNORECASE):
            return s
    return "spx"


def _detect_overnight(q, eff_subj):
    if _OVERNIGHT_POS_RE.search(q):   direction = "positive"
    elif _OVERNIGHT_NEG_RE.search(q): direction = "negative"
    else:                              return None
    asset = None
    for t in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(t)}\b\s+(?:ouvre|ouverture|open|gap)", q, re.IGNORECASE):
            asset = t; break
    return {"direction": direction, "asset": asset}


def _detect_intraday(q):
    m = _INTRADAY_RE.search(q)
    if not m: return None
    if m.group(1): return int(m.group(1))
    if m.group(2): return int(m.group(2))
    if m.group(3): return int(m.group(3)) * 60
    return None


def _detect_calendar(q):
    for pattern, keywords in _CAL_EVENTS:
        if re.search(pattern, q, re.IGNORECASE):
            surprise = ("positive" if _SURPRISE_POS_RE.search(q)
                        else "negative" if _SURPRISE_NEG_RE.search(q) else None)
            return {"keywords": keywords, "surprise": surprise}
    return None


def _parse_single_condition(chunk, eff_cond):
    for asset in sorted(eff_cond, key=len, reverse=True):
        pat = re.escape(asset) + r"\s*(>=|<=|>|<|=)\s*([\d.,]+)"
        m = re.search(pat, chunk, re.IGNORECASE)
        if m:
            return asset, m.group(1), float(m.group(2).replace(",", "."))
    return None


def parse_query(query: str):
    eff_cond, eff_subj = get_effective_registries()
    q = query.strip()
    subject      = _detect_subject(q, eff_subj)
    overnight    = _detect_overnight(q, eff_subj)
    weekday      = _detect_weekday(q)
    intraday_min = _detect_intraday(q)
    calendar     = _detect_calendar(q)
    chunks = re.split(r"\s+(?:ET|AND)\s+", q, flags=re.IGNORECASE)
    conditions = []
    for chunk in chunks:
        r = _parse_single_condition(chunk, eff_cond)
        if r:
            asset, op, thr = r
            conditions.append({"asset": asset, "op": op, "threshold": thr})
    if not conditions and overnight is None and intraday_min is None and calendar is None:
        return None
    return {"subject": subject, "conditions": conditions, "weekday": weekday,
            "overnight": overnight, "intraday_min": intraday_min, "calendar": calendar}


def _apply_op(series, op, threshold):
    return {">":series>threshold,"<":series<threshold,">=":series>=threshold,
            "<=":series<=threshold,"=":series==threshold}.get(op, pd.Series(False, index=series.index))


def _overnight_dates(df, direction="positive"):
    d = df[["open","close"]].dropna().sort_index().copy()
    d["prev_close"] = d["close"].shift(1)
    d = d.dropna(subset=["prev_close"])
    mask = d["open"] > d["prev_close"] if direction=="positive" else d["open"] < d["prev_close"]
    return set(d[mask].index.normalize())


def _stats(df_filtered):
    df = df_filtered.dropna(subset=["close","prev_close"]).copy()
    df["var_pct"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100
    df["var_pts"] = df["close"] - df["prev_close"]
    n = len(df)
    if n == 0: return {}
    bull = int((df["var_pct"] > 0).sum())
    best, worst = df["var_pct"].idxmax(), df["var_pct"].idxmin()
    return {"n":n,"mean_var":float(df["var_pct"].mean()),
            "mean_pts":float(df["var_pts"].mean()),
            "pct_bull":bull/n*100,"pct_bear":(n-bull)/n*100,
            "best_date":best.strftime("%Y-%m-%d"),"best_val":float(df.loc[best,"var_pct"]),
            "worst_date":worst.strftime("%Y-%m-%d"),"worst_val":float(df.loc[worst,"var_pct"]),
            "df":df}


# ─── Couche 1 ─────────────────────────────────────────────────────────────

def layer1_structured(query: str):
    """Returns None (not a C1 query), {"type":"C1_EMPTY",...} or {"type":"C1",...}."""
    parsed = parse_query(query)
    if parsed is None:
        return None
    eff_cond, eff_subj = get_effective_registries()
    subject    = parsed["subject"]
    conditions = parsed["conditions"]
    weekday    = parsed["weekday"]
    overnight  = parsed["overnight"]
    if subject not in eff_subj:
        return None
    df_raw = _load_csv(eff_subj[subject]).copy()
    for col in ("open","close"):
        if col in df_raw.columns:
            df_raw[col] = _to_numeric(df_raw[col])
    subject_df  = df_raw.set_index("time")
    valid_dates = set(subject_df.dropna(subset=["open","close"]).index.normalize())
    for cond in conditions:
        fname, col = eff_cond[cond["asset"]]
        df_c = _load_csv(fname).copy()
        col_l = col.lower().replace(" ","_")
        if col_l not in df_c.columns:
            col_l = next((c for c in df_c.columns if c != "time"), None)
        if col_l is None:
            return None
        df_c[col_l] = _to_numeric(df_c[col_l])
        s    = df_c.set_index("time")[col_l].dropna()
        mask = _apply_op(s, cond["op"], cond["threshold"])
        valid_dates &= set(s[mask].index.normalize())
    if overnight:
        ov = overnight["asset"] or subject
        df_ov = _load_csv(eff_subj.get(ov, eff_subj["spx"])).copy()
        for col in ("open","close"):
            if col in df_ov.columns:
                df_ov[col] = _to_numeric(df_ov[col])
        valid_dates &= _overnight_dates(df_ov.set_index("time"), overnight["direction"])
    if weekday is not None:
        valid_dates = {d for d in valid_dates if d.weekday() == weekday}
    subj = subject_df.copy()
    subj.index = subj.index.normalize()
    subj["prev_close"] = subj["close"].shift(1)
    stats = _stats(subj[subj.index.isin(valid_dates)])
    total = len(subject_df.dropna(subset=["open","close"]))
    n     = stats.get("n", 0) if stats else 0
    pct   = n / total * 100 if total else 0
    # Build human-readable condition string
    cond_str = " ET ".join(f"{c['asset'].upper()} {c['op']} {c['threshold']}" for c in conditions)
    if overnight:
        ov_label  = (overnight["asset"] or subject).upper() + " "
        dir_label = "positive" if overnight["direction"] == "positive" else "négative"
        flag = f"{ov_label}ouverture {dir_label} vs veille"
        cond_str = (cond_str + "  ·  " if cond_str else "") + flag
    if weekday is not None:
        flag = f"les {_WEEKDAY_LABELS.get(weekday, str(weekday))}"
        cond_str = (cond_str + "  ·  " if cond_str else "") + flag
    if not stats:
        return {"type": "C1_EMPTY", "msg": f"Aucun jour ne correspond pour {subject.upper()}."}
    return {
        "type":         "C1",
        "subject":      subject,
        "cond_str":     cond_str,
        "n":            n,
        "total":        total,
        "pct":          pct,
        "window_label": "close J-1 → close J",
        "stats":        stats,
        "conditions":   conditions,
    }


def layer1(query: str):
    result = layer1_structured(query)
    if result is None:
        return None
    if result["type"] == "C1_EMPTY":
        return f"[C1] {result['msg']}"
    st = result["stats"]
    cond_str = result["cond_str"] or "(aucune numérique)"
    pts_sign = "+" if st["mean_pts"] >= 0 else ""
    return "\n".join([
        "[Couche 1 — regex]",
        f"Sujet : {result['subject'].upper()}  |  Condition : {cond_str}",
        f"Période : {result['n']} jours",
        f"Variation moy close J-1→J : {'+' if st['mean_var']>=0 else ''}{st['mean_var']:.2f}%  ({pts_sign}{st['mean_pts']:.1f} pts)",
        f"Jours haussiers : {st['pct_bull']:.1f}%   Jours baissiers : {st['pct_bear']:.1f}%",
        f"Meilleur jour  : {st['best_date']}  {'+' if st['best_val']>=0 else ''}{st['best_val']:.2f}%",
        f"Pire jour      : {st['worst_date']}  {st['worst_val']:.2f}%",
    ])


# ─── Couche 2 : DuckDB dynamique + Ollama ─────────────────────────────────

def _tbl(fname: str) -> str:
    stem = Path(fname).stem
    s = stem.lower()
    s = re.sub(r"[\s\-,.()/]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _load_for_duckdb(fname: str) -> pd.DataFrame:
    df = pd.read_csv(DATA_DIR / fname, sep=";")
    df.columns = [
        re.sub(r"_+", "_", re.sub(r"[()]+", "", c.strip().lower().replace(" ","_").replace("-","_")))
        for c in df.columns
    ]
    df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
    df = df.dropna(subset=["time"])
    if fname in _PARIS_FILES and df["time"].dt.hour.max() > 0:
        df["time"] = (df["time"]
                      .dt.tz_localize(_TZ_PARIS, ambiguous="NaT", nonexistent="NaT")
                      .dt.tz_convert(_TZ_NY).dt.tz_localize(None))
    for col in df.columns:
        if col == "time":
            continue
        converted = pd.to_numeric(df[col].astype(str).str.replace(",",".").str.strip(), errors="coerce")
        if converted.notna().mean() >= 0.3:
            df[col] = converted
    return df


_duckdb_tables: list[tuple[str, pd.DataFrame]] | None = None
_duckdb_schema: str | None = None
_duckdb_lock = threading.Lock()


def _ensure_duckdb():
    global _duckdb_tables, _duckdb_schema
    if _duckdb_tables is not None:
        return
    with _duckdb_lock:
        if _duckdb_tables is not None:
            return
        print("[DuckDB] Chargement de tous les CSV...", flush=True)
        tables = []
        for path in sorted(DATA_DIR.glob("*.csv")):
            tname = _tbl(path.name)
            try:
                df = _load_for_duckdb(path.name)
                tables.append((tname, df))
            except Exception as e:
                print(f"  [skip] {path.name}: {e}", flush=True)
        _duckdb_tables = tables
        lines = ["Tables DuckDB (noms exacts à utiliser dans le SQL) :"]
        for tname, df in sorted(tables):
            cols = list(df.columns)
            shown = ", ".join(cols[:6])
            if len(cols) > 6:
                shown += f" (+{len(cols)-6})"
            lines.append(f"  {tname}: {shown}")
        _duckdb_schema = "\n".join(lines)
        print(f"[DuckDB] {len(tables)} tables chargées.", flush=True)


def _fresh_conn() -> duckdb.DuckDBPyConnection:
    _ensure_duckdb()
    conn = duckdb.connect(":memory:")
    for tname, df in _duckdb_tables:
        conn.register(tname, df)
    return conn


_SCHEMA_TABLES = {
    "spx_daily", "vix_daily", "vix1d_vix_ratio_daily", "spx_30min",
    "vix1d_30min", "calendar_events_daily", "spx_put_call_ratio_daily",
    "gold_daily", "dxy_daily", "skew_index_daily",
}

def get_schema() -> str:
    _ensure_duckdb()
    lines = ["Tables disponibles :"]
    for tname, df in sorted(_duckdb_tables):
        if tname in _SCHEMA_TABLES:
            cols = list(df.columns)
            shown = ", ".join(cols[:6])
            if len(cols) > 6:
                shown += f" (+{len(cols)-6})"
            lines.append(f"  {tname}: {shown}")
    return "\n".join(lines)


_COMPLEX_PERIOD_RE = re.compile(
    r"\bpar\s+(mois|trimestre|semaine|ann[eé]e|jour)\b", re.IGNORECASE
)


def _query_complexity(question: str) -> tuple[str, int, float]:
    """Retourne (niveau, num_predict, temperature)."""
    words = question.split()
    nb_words = len(words)
    has_period_group = bool(_COMPLEX_PERIOD_RE.search(question))
    # Compte les actifs connus présents dans la question
    q_low = question.lower()
    known_assets = {"spx","spy","qqq","iwm","aapl","aaoi","vix","vvix","dxy","gold","dax","ftse","nikkei"}
    nb_assets = sum(1 for a in known_assets if re.search(rf"\b{a}\b", q_low))
    is_complex = nb_words > 15 or has_period_group or nb_assets >= 2
    if is_complex:
        return "complexe", 1200, 0.1
    return "simple", 400, 0.0


def _clean_sql(raw: str) -> str:
    raw = re.sub(r"```sql\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"```", "", raw)
    # Truncate at continuation markers sqlcoder emits after the SQL
    for stop_token in ("\nQuestion:", "\nQ:", "\n--", "\nSQL:"):
        idx = raw.find(stop_token)
        if idx != -1:
            raw = raw[:idx]
    lines = []
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        lines.append(line)
        if line.rstrip().endswith(";"):
            break
    sql = " ".join(lines).strip().rstrip(";")
    if not re.match(r"^\s*SELECT", sql, re.IGNORECASE):
        m = re.search(r"(SELECT\b.*)", sql, re.IGNORECASE | re.DOTALL)
        if m:
            sql = m.group(1).strip()
    return sql


def layer2_structured(query: str) -> dict:
    return {"type": "ERROR", "ok": False, "df": None, "sql": "",
            "error": "Question non reconnue — reformulez simplement."}


def layer2(query: str) -> str:
    return "[Erreur] Question non reconnue — reformulez simplement."



# ─── Moteur principal ─────────────────────────────────────────────────────

def answer(query: str) -> str:
    result = layer1(query)
    if result is not None:
        return result
    return layer2(query)


# ─── Historique persistant JSON ───────────────────────────────────────────

def _result_to_serializable(result: dict) -> dict:
    """Convertit un result dict en version JSON-sérialisable (DataFrames → records)."""
    rtype = result.get("type", "")
    if rtype == "C1":
        stats = result["stats"]
        df = stats["df"]
        cols = [c for c in ["prev_close", "close", "var_pct", "var_pts"] if c in df.columns]
        df_save = df[cols].copy()
        df_save.index = df_save.index.strftime("%Y-%m-%d")
        df_records = json.loads(df_save.reset_index().to_json(orient="records"))
        s = {k: v for k, v in result.items() if k != "stats"}
        s["stats"] = {k: v for k, v in stats.items() if k != "df"}
        s["stats"]["df_records"] = df_records
        return s
    if rtype == "C2" and result.get("df") is not None:
        s = {k: v for k, v in result.items() if k != "df"}
        s["df_records"] = json.loads(result["df"].to_json(orient="records"))
        return s
    return {k: v for k, v in result.items() if k != "df"}


def _result_from_serializable(result: dict) -> dict:
    """Reconstruit les DataFrames depuis les records JSON."""
    rtype = result.get("type", "")
    if rtype == "C1":
        stats = result.get("stats", {})
        if "df_records" in stats:
            records = stats.pop("df_records")
            df = pd.DataFrame(records)
            if "time" in df.columns:
                df["time"] = pd.to_datetime(df["time"])
                df = df.set_index("time")
            stats["df"] = df
        result["stats"] = stats
    elif rtype == "C2" and "df_records" in result:
        records = result.pop("df_records")
        result["df"] = pd.DataFrame(records) if records else pd.DataFrame()
    return result


def _save_history(history: list) -> None:
    try:
        data = [{"q": item["q"], "result": _result_to_serializable(item["result"])}
                for item in history[-20:]
                if item.get("result", {}).get("type") != "PATTERNS_LAUNCHED"]
        HISTORY_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[history] Erreur sauvegarde : {e}", flush=True)


def _load_history() -> list:
    try:
        if not HISTORY_FILE.exists():
            return []
        data = json.loads(HISTORY_FILE.read_text(encoding="utf-8"))
        return [{"q": item["q"], "result": _result_from_serializable(item["result"])}
                for item in data
                if item.get("result", {}).get("type") != "PATTERNS_LAUNCHED"]
    except Exception as e:
        print(f"[history] Erreur chargement : {e}", flush=True)
        return []


# ─── Alerte tokens ────────────────────────────────────────────────────────

def _check_token_warning() -> bool:
    """Retourne True si l'usage tokens est signalé > 80%."""
    if os.environ.get("SPX_TOKEN_WARNING"):
        return True
    if TOKEN_FLAG_FILE.exists():
        return True
    return False


# ─── Streamlit : helpers ──────────────────────────────────────────────────

# ─── Module IC / RIC (Option Chains) ──────────────────────────────────────

import re as _re

_IC_RIC_RE = _re.compile(
    r"\b(ic|iron\s*condor|ric|reverse\s*iron\s*condor)\b.*?aile\s*(\d+)",
    _re.IGNORECASE
)
_VIX_LEVEL_RE = _re.compile(r"\bvix\s*[\s_]?([\d]+[,.]?[\d]*)\b", _re.IGNORECASE)

def _find_option_chain_file(vix_level: str) -> tuple[Path | None, bool]:
    """Retourne (fichier, is_exact). is_exact=True si le VIX demandé est dans le nom."""
    vix_str = vix_level.replace(".", "_").replace(",", "_")
    for f in DATA_DIR.glob("*option_chain*.csv"):
        if vix_str in f.name.replace(".", "_"):
            return f, True
    candidates = list(DATA_DIR.glob("*option_chain*.csv"))
    if not candidates:
        return None, False
    target = float(vix_level.replace(",", "."))
    def extract_vix(p: Path):
        m = _re.search(r"VIX_([\d]+[_][\d]+)", p.name)
        if m:
            return float(m.group(1).replace("_", "."))
        return 999.0
    return min(candidates, key=lambda p: abs(extract_vix(p) - target)), False

def _compute_ic_ric(query: str) -> dict | None:
    """Calcule IC ou RIC depuis un CSV option chain. Retourne dict ou None."""
    m_type = _IC_RIC_RE.search(query)
    if not m_type:
        return None

    is_ric = "ric" in m_type.group(1).lower() or "reverse" in m_type.group(1).lower()
    wing = int(m_type.group(2))

    # Niveau VIX dans la question
    m_vix = _VIX_LEVEL_RE.search(query)
    vix_requested = None
    exact_match = True
    if not m_vix:
        candidates = list(DATA_DIR.glob("*option_chain*.csv"))
        if not candidates:
            return {"type": "IC_RIC", "ok": False, "error": "Aucun fichier option chain trouvé."}
        chain_file = candidates[0]
    else:
        vix_requested = m_vix.group(1)
        chain_file, exact_match = _find_option_chain_file(vix_requested)
        if chain_file is None:
            return {"type": "IC_RIC", "ok": False, "error": f"Fichier option chain VIX {vix_requested} introuvable."}

    try:
        df = pd.read_csv(chain_file, sep=None, engine="python")
        df.columns = [c.strip() for c in df.columns]
        # Normalise noms colonnes
        col_map = {}
        for c in df.columns:
            cl = c.lower().replace(" ", "")
            if cl in ("strike",): col_map[c] = "Strike"
            elif cl in ("callbid","call_bid"): col_map[c] = "CallBid"
            elif cl in ("callask","call_ask"): col_map[c] = "CallAsk"
            elif cl in ("calldelta","call_delta"): col_map[c] = "CallDelta"
            elif cl in ("putbid","put_bid"): col_map[c] = "PutBid"
            elif cl in ("putask","put_ask"): col_map[c] = "PutAsk"
            elif cl in ("putdelta","put_delta"): col_map[c] = "PutDelta"
        df = df.rename(columns=col_map)
        for col in ["Strike","CallBid","CallAsk","CallDelta","PutBid","PutAsk","PutDelta"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["Strike","CallDelta"])

        # ATM = strike avec CallDelta le plus proche de 0.5
        df["_delta_diff"] = (df["CallDelta"] - 0.5).abs()
        atm_idx = df["_delta_diff"].idxmin()
        atm_strike = df.loc[atm_idx, "Strike"]

        def get_row(target_strike):
            idx = (df["Strike"] - target_strike).abs().idxmin()
            return df.loc[idx]

        atm = get_row(atm_strike)

        if not is_ric:
            # IC : Short ATM Call + Short ATM Put + Long (ATM+wing) Call + Long (ATM-wing) Put
            lc = get_row(atm_strike + wing)
            lp = get_row(atm_strike - wing)
            credit = round(
                (atm["CallBid"] + atm["PutBid"]) - (lc["CallAsk"] + lp["PutAsk"]), 2
            )
            label = f"IC aile {wing}"
            legs = [
                {"jambe": "Short Call", "strike": atm_strike, "prix": atm["CallBid"]},
                {"jambe": f"Long Call", "strike": atm_strike + wing, "prix": -lc["CallAsk"]},
                {"jambe": "Short Put", "strike": atm_strike, "prix": atm["PutBid"]},
                {"jambe": f"Long Put", "strike": atm_strike - wing, "prix": -lp["PutAsk"]},
            ]
        else:
            # RIC : Long ATM Call + Long ATM Put + Short (ATM+wing) Call + Short (ATM-wing) Put
            sc_r = get_row(atm_strike + wing)
            sp_r = get_row(atm_strike - wing)
            credit = round(
                (sc_r["CallBid"] + sp_r["PutBid"]) - (atm["CallAsk"] + atm["PutAsk"]), 2
            )
            label = f"RIC aile {wing}"
            legs = [
                {"jambe": "Long Call", "strike": atm_strike, "prix": -atm["CallAsk"]},
                {"jambe": f"Short Call", "strike": atm_strike + wing, "prix": sc_r["CallBid"]},
                {"jambe": "Long Put", "strike": atm_strike, "prix": -atm["PutAsk"]},
                {"jambe": f"Short Put", "strike": atm_strike - wing, "prix": sp_r["PutBid"]},
            ]

        return {
            "type": "IC_RIC",
            "ok": True,
            "label": label,
            "is_ric": is_ric,
            "wing": wing,
            "atm_strike": atm_strike,
            "atm_delta": round(float(atm["CallDelta"]), 3),
            "credit": credit,
            "legs": legs,
            "source": chain_file.name,
            "exact_match": exact_match,
            "vix_requested": vix_requested,
        }
    except Exception as e:
        return {"type": "IC_RIC", "ok": False, "error": str(e)}


# ─── Comparaison A vs B ───────────────────────────────────────────────────

_VS_RE = re.compile(
    r"\bvs\.?\b|\bversus\b",
    re.IGNORECASE,
)

# Regex pour extraire un "slot" de comparaison :
# - une année seule : "2022"
# - un actif seul   : "SPX", "QQQ" …
# - un mois+année   : "mars 2024"
# - "performance/perf/variation ACTIF MOIS? ANNÉE?"
_COMPARE_SLOT_RE = re.compile(
    r"(?:"
    r"(?:performance|perf|variation|rendement|retour)\s+(?P<slot_asset>\w+)(?:\s+(?P<slot_month>[a-zàâéèêëîïôùûüç]+))?\s+(?P<slot_year>20\d{2})"
    r"|(?P<bare_year>20\d{2})"
    r"|(?P<bare_asset>SPX|SPY|QQQ|IWM|AAPL|AAOI|VIX|VVIX|DXY|GOLD|DAX|FTSE|NIKKEI)"
    r")",
    re.IGNORECASE,
)


def _parse_compare_slot(text: str, default_asset: str | None = None) -> str | None:
    """
    Transforme un fragment de question en une question normalisée pour _compute_lookup
    ou layer1_structured. Retourne None si le slot n'est pas reconnu.
    """
    text = text.strip().rstrip("?").strip()
    m = _COMPARE_SLOT_RE.search(text)
    if not m:
        return None

    # Slot complet "performance ACTIF [MOIS] ANNÉE"
    if m.group("slot_asset"):
        asset = m.group("slot_asset")
        year  = m.group("slot_year")
        month = m.group("slot_month") or ""
        if month:
            return f"performance {asset} {month} {year}"
        return f"performance {asset} {year}"

    # Année seule → utilise l'actif par défaut détecté dans la question globale
    if m.group("bare_year"):
        year = m.group("bare_year")
        asset = default_asset or "SPX"
        return f"performance {asset} {year}"

    # Actif seul → pas assez d'info pour un lookup, retourne None
    return None


def _compute_compare(query: str) -> dict | None:
    """
    Détecte "A vs B" / "A versus B" dans la question.
    Retourne type="C1_COMPARE" si les deux slots sont résolus, sinon None.
    """
    if not _VS_RE.search(query):
        return None

    parts = _VS_RE.split(query, maxsplit=1)
    if len(parts) != 2:
        return None

    left_raw, right_raw = parts[0], parts[1]

    # Actif par défaut : premier actif mentionné dans la question complète
    _, eff_subj = get_effective_registries()
    default_asset = None
    for s in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", query, re.IGNORECASE):
            default_asset = s.upper()
            break

    left_q  = _parse_compare_slot(left_raw,  default_asset)
    right_q = _parse_compare_slot(right_raw, default_asset)

    if not left_q or not right_q:
        return None

    # Calcule chaque slot via _compute_lookup (priorité) ou layer1_structured
    def _resolve(q: str) -> dict | None:
        r = _compute_lookup(q)
        if r is not None:
            return r
        r = layer1_structured(q)
        return r

    res_left  = _resolve(left_q)
    res_right = _resolve(right_q)

    if res_left is None or res_right is None:
        return None
    if not res_left.get("ok", True) or not res_right.get("ok", True):
        return None

    return {
        "type":   "C1_COMPARE",
        "ok":     True,
        "left":   {"query": left_q,  "result": res_left},
        "right":  {"query": right_q, "result": res_right},
    }


_EXPLORE_RE = re.compile(
    r"\bexplorer?\s+patterns?\b(.*)",
    re.IGNORECASE,
)

# Tickers individuels (actions) → route vers ticker_analysis
_INDIVIDUAL_TICKERS = {"aaoi", "aapl"}
# Auto-scan tickers/ directory
if TICKERS_DIR.exists():
    for _tf in TICKERS_DIR.glob("*.csv"):
        _ts = _tf.stem.lower().replace("_daily", "")
        if len(_ts) <= 5 and _ts.isalpha() and "earning" not in _ts:
            _INDIVIDUAL_TICKERS.add(_ts)

def _detect_individual_ticker(query: str) -> str | None:
    """Détecte si la question mentionne un ticker individuel (action, pas indice)."""
    _SYNONYMS = {
        "apple": "AAPL", "microsoft": "MSFT", "google": "GOOG",
        "alphabet": "GOOG", "amazon": "AMZN", "tesla": "TSLA",
        "nvidia": "NVDA", "meta": "META", "netflix": "NFLX",
        "reddit": "RDDT", "robinhood": "HOOD", "mercadolibre": "MELI",
        "applovin": "APP", "ondas": "ONDS", "iren": "IREN",
        "micron": "MU", "coherent": "COHR",
    }
    q_lower = query.lower()
    for name, sym in _SYNONYMS.items():
        if name in q_lower:
            return sym
    q = q_lower
    # D'abord les tickers connus statiques
    for t in sorted(_INDIVIDUAL_TICKERS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(t)}\b", q):
            return t
    # Puis les CSV *_daily.csv ou *.csv qui ne sont pas des indices/ratios
    _skip = {"spx","spy","qqq","iwm","vix","vvix","vix1d","vix3m","vix6m","vix9d",
             "dxy","gold","dax","dax40","ftse","ftse100","nikkei","nikkei225","skew"}
    _, eff_subj = get_effective_registries()
    for s in sorted(eff_subj, key=len, reverse=True):
        if s in _skip or "/" in s or "put" in s or "call" in s or "ratio" in s:
            continue
        if re.search(rf"\b{re.escape(s)}\b", q):
            return s
    return None


def _compute_ticker_analysis(query: str, session_state=None) -> dict | None:
    """Route vers ticker_analysis.py si un ticker individuel est détecté."""
    if not _TICKER_AVAILABLE:
        return None
    ticker = _detect_individual_ticker(query)
    if ticker is None:
        return None
    # Ne pas intercepter les lookup simples (clôture X le DATE)
    if _LOOKUP_FIELD_RE.search(query.lower()) and _parse_date_from_query(query):
        return None
    # Ne pas intercepter les comparaisons
    if _VS_RE.search(query):
        return None
    # Ne pas intercepter les IC/RIC
    if _IC_RIC_RE.search(query):
        return None
    # Follow-up contextuel
    ctx_dates = None
    if session_state and session_state.get("ticker_context"):
        ctx = session_state["ticker_context"]
        if _CONTEXT_REF_RE.search(query) and ctx.get("dates"):
            ctx_dates = ctx["dates"]
            print(f"[followup-context] ticker={ticker} using {len(ctx_dates)} dates from context", flush=True)
    return _ticker_analyze(ticker, query, context_dates=ctx_dates)


def _compute_result(query: str, session_state=None) -> dict:
    # Priorité -2 : interpréteur sémantique (regex + LLM classifier → pandas)
    if _INTERPRETER_AVAILABLE:
        try:
            _ctx = session_state.get("follow_up_context", {}) if session_state else {}
            _active_ticker = (_ctx.get("ticker")
                              or (session_state.get("ticker_context", {}).get("ticker")
                                  if session_state else None))
            _last_cat = _ctx.get("last_category")
            _last_params = _ctx.get("last_params")
            interp = _interpret(query, active_ticker=_active_ticker,
                                last_category=_last_cat, last_params=_last_params)
            interp["be_seuil"] = session_state.get("be_seuil", 2.0) if session_state else 2.0
            interp["bull_seuil"] = session_state.get("bull_seuil", 2.0) if session_state else 2.0
            skip_cats = {"UNKNOWN", None}
            # FILTER_STATS: only handle "abs" here, rest → existing pipeline
            if interp.get("category") == "FILTER_STATS" and interp.get("criterion") != "abs":
                skip_cats.add("FILTER_STATS")
            if interp.get("category") not in skip_cats:
                result = _execute(interp, query)
                if result is not None and result.get("ok") is not False:
                    if result.get("ok"):
                        print(f"[routing] INTERPRETED/{interp['category']} | {query[:60]}", flush=True)
                        # Store follow-up context
                        if session_state is not None:
                            depth = _ctx.get("depth", 0) if _ctx else 0
                            session_state["follow_up_context"] = {
                                "ticker": result.get("ticker", _active_ticker),
                                "last_category": interp.get("category"),
                                "last_params": {
                                    "pattern": interp.get("pattern"),
                                    "direction": interp.get("direction"),
                                    "criterion": interp.get("criterion"),
                                },
                                "depth": min(depth + 1, 2),
                            }
                        return result
        except Exception as e:
            print(f"[interpreter] error: {e}", flush=True)
    # Priorité -1.5 : SPX overnight patterns
    if _SPX_PATTERNS_AVAILABLE and re.search(
            r"\b(ouverture\s+spx|open\s+spx|overnight\s+spx|patterns?\s+spx"
            r"|que\s+fera\s+spx|spx\s+demain|patterns?\s+overnight)\b", query, re.IGNORECASE):
        active = _spx_active()
        all_p = _spx_all()[:15]
        print(f"[routing] SPX_OVERNIGHT | {query[:60]}", flush=True)
        return {"type": "INTERPRETED", "ok": True, "sub_type": "spx_overnight",
                "active_patterns": active, "all_patterns": all_p}
    # Priorité -1 : commande "explorer patterns X, Y, Z"
    m_exp = _EXPLORE_RE.search(query)
    if m_exp and _PATTERNS_AVAILABLE:
        raw_assets = m_exp.group(1).strip().strip(",").strip()
        assets = [a.strip().upper() for a in re.split(r"[,\s]+", raw_assets) if a.strip()]
        if not assets:
            assets = ["VIX", "VIX1D/VIX"]
        _patterns_launch(assets, target="SPX", max_combos=1000,
                         session_state=None)
        # Flag dans session_state pour le polling (écrit côté UI thread)
        return {
            "type": "PATTERNS_LAUNCHED",
            "ok": True,
            "assets": assets,
            "set_running": True,
            "msg": f"Exploration en arrière-plan lancée pour : {', '.join(assets)}. Résultats dans quelques secondes.",
        }

    # Priorité 0 : IC/RIC option chain
    ic_ric = _compute_ic_ric(query)
    if ic_ric is not None:
        print(f"[routing] IC_RIC | {query[:60]}", flush=True)
        return ic_ric
    # Priorité 1 : comparaison A vs B
    compare = _compute_compare(query)
    if compare is not None:
        print(f"[routing] COMPARE | {query[:60]}", flush=True)
        return compare
    # Priorité 2 : lookup direct (valeur ponctuelle ou perf mois/année)
    lookup = _compute_lookup(query)
    if lookup is not None:
        print(f"[routing] LOOKUP | {query[:60]}", flush=True)
        return lookup
    # Priorité 2b : drop/gain X% + lendemain (multi-seuils + follow-up)
    drop_next = _compute_drop_next_day(query, session_state=session_state)
    if drop_next is not None:
        print(f"[routing] DROP_NEXT | {query[:60]}", flush=True)
        return drop_next
    # Priorité 3 : ticker individuel → ticker_analysis.py
    ticker_r = _compute_ticker_analysis(query, session_state=session_state)
    if ticker_r is not None:
        print(f"[routing] TICKER | {query[:60]}", flush=True)
        return ticker_r
    # Couche 1 : regex conditions
    result = layer1_structured(query)
    if result is not None:
        print(f"[routing] C1 | {query[:60]}", flush=True)
        return result
    # Couche 2 : sqlcoder + DuckDB — SEULEMENT pour les indices/macro
    # Si un ticker individuel est détecté, ne jamais appeler Ollama
    _c2_ticker = _detect_individual_ticker(query)
    if _c2_ticker is not None:
        return {"type": "ERROR", "ok": False,
                "error": f"Question sur {_c2_ticker.upper()} non reconnue — reformulez plus simplement."}
    print(f"[routing] C2 | {query[:60]}", flush=True)
    return layer2_structured(query)


_COL_LABELS: dict[str, str] = {
    "dow": "Jour", "jour": "Jour", "annee": "Année",
    "perf_pct": "Variation %", "avg_var": "Var. moy. %", "avg_pc_ratio": "P/C ratio moy.",
    "nb_jours": "Nb jours", "nb_seances": "Nb séances", "nb_sessions": "Nb séances",
    "range_pts": "Range (pts)", "date": "Date", "count_star()": "Nombre",
    "avg_open": "Open moy.", "avg_close": "Close moy.", "avg_vvix": "VVIX moy.",
    "avg_vix": "VIX moy.", "min_open": "Open min.", "max_close": "Close max.",
}

def _humanize_col(name: str) -> str:
    low = name.lower()
    if low in _COL_LABELS:
        return _COL_LABELS[low]
    if low.startswith("count") or low in ("n", "total"):
        return "Nombre"
    if "pct" in low or "perf" in low or "var" in low or "ret" in low:
        return name.replace("_", " ").title() + " %"
    return name.replace("_", " ").title()

def _fmt_c2_val(col_name: str, val) -> str:
    """Formate une valeur C2 pour st.metric ou tableau."""
    low = col_name.lower()
    if low in ("dow",) and isinstance(val, (int, float, str)):
        return _DOW_FR.get(str(int(float(val))), str(val))
    if not isinstance(val, (int, float)):
        return str(val)
    is_pct = any(k in low for k in ("pct", "var", "perf", "change", "ret"))
    if is_pct:
        sign = "+" if val >= 0 else ""
        return f"{sign}{val:.2f}%"
    if low in ("range_pts",) or "pts" in low:
        return f"{val:.1f} pts"
    if any(k in low for k in ("count", "nb_jours", "nb_seances", "nb_sessions")) or low in ("n", "total"):
        return f"{int(val):,}"
    return f"{val:.4f}" if abs(val) < 1e4 else f"{val:,.2f}"


# ─── Lookup direct C1 (clôture/open/high/low ACTIF le DATE) ──────────────

_LOOKUP_FIELD_RE = re.compile(
    r"\b(cl[oô]ture|close|open|ouverture|high|haut|low|bas)\b",
    re.IGNORECASE,
)
# Dates numériques : 15/03/2024 ou 2024-03-15
_LOOKUP_DATE_NUM_RE = re.compile(
    r"\b(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\d{4}[/\-]\d{2}[/\-]\d{2})\b",
)
# Dates texte français : "9 octobre 2025", "le 15 mars 2024"
_LOOKUP_DATE_TEXT_RE = re.compile(
    r"\b(?:le\s+)?(\d{1,2})\s+"
    r"(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre)"
    r"\s+(\d{4})\b",
    re.IGNORECASE,
)
_LOOKUP_MONTH_RE = re.compile(
    r"\b(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre"
    r"|january|february|march|april|june|july|august|september|october|november|december)"
    r"\s+(\d{4})\b",
    re.IGNORECASE,
)
_PERF_RE = re.compile(r"\b(performance|perf|variation|rendement|retour)\b", re.IGNORECASE)
_MONTH_MAP = {
    "janvier":1,"février":2,"fevrier":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"août":8,"aout":8,"septembre":9,"octobre":10,"novembre":11,"décembre":12,"decembre":12,
    "january":1,"february":2,"march":3,"april":4,"june":6,"july":7,
    "august":8,"september":9,"october":10,"november":11,"december":12,
}


def _parse_date_from_query(query: str) -> pd.Timestamp | None:
    """Parse une date depuis une question : texte français ou format numérique."""
    # Essai texte français : "9 octobre 2025"
    m = _LOOKUP_DATE_TEXT_RE.search(query)
    if m:
        day = int(m.group(1))
        month = _MONTH_MAP.get(m.group(2).lower().replace("é","e").replace("û","u").replace("ô","o"), None)
        year = int(m.group(3))
        if month:
            try:
                return pd.Timestamp(year=year, month=month, day=day)
            except Exception:
                return None
    # Essai numérique : 15/03/2024
    m2 = _LOOKUP_DATE_NUM_RE.search(query)
    if m2:
        try:
            return pd.Timestamp(m2.group(1).replace("/", "-"))
        except Exception:
            return None
    return None


def _detect_lookup(query: str) -> dict | None:
    """Détecte une demande de valeur ponctuelle (open/close/high/low + date ou perf mois/année)."""
    query = unicodedata.normalize("NFC", query)
    q = query.lower()
    _, eff_subj = get_effective_registries()

    # Détection actif
    asset = None
    for s in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", q):
            asset = s
            break

    # Détection champ (close/open/high/low)
    fm = _LOOKUP_FIELD_RE.search(q)
    # Détection date (texte ou numérique) — même sans champ explicite
    target_date = _parse_date_from_query(query)

    print(f"[_detect_lookup] q={q!r} | asset={asset} | field={fm.group(1) if fm else None} | date={target_date}", flush=True)

    if asset is None:
        return None

    # Performance sur un mois entier
    if _PERF_RE.search(q):
        mm = _LOOKUP_MONTH_RE.search(query)
        if mm:
            month_key = mm.group(1).lower()
            month = _MONTH_MAP.get(month_key, None)
            year = int(mm.group(2))
            return {"kind": "perf_month", "asset": asset, "month": month, "year": year}
        # Performance sur une année
        ym = re.search(r"\b(20\d{2})\b", query)
        if ym and "mois" not in q:
            return {"kind": "perf_year", "asset": asset, "year": int(ym.group(1))}

    # Valeur ponctuelle : date détectée (champ explicite ou close par défaut)
    if target_date is not None:
        if fm:
            field_raw = fm.group(1).lower()
            field_norm = field_raw.replace("ô", "o").replace("é", "e")
            field = ("close" if field_norm in ("cloture", "close")
                     else "open" if field_norm in ("open", "ouverture")
                     else "high" if field_norm in ("high", "haut")
                     else "low")
        else:
            # Pas de champ précisé → clôture par défaut (règle métier)
            field = "close"
        return {"kind": "value_date", "asset": asset, "field": field, "target_date": target_date}
    return None


# ─── Pattern C1 : drop/gain X% + lendemain ───────────────────────────────

_DROP_RE = re.compile(
    r"\b(perdu|perd|chut[eé]|baiss[eé]|gagn[eé]|mont[eé]|hauss[eé])"
    r"\s+(?:de\s+)?(\d+[\.,]?\d*)\s*%"
    r"\s*(ou\s+plus|ou\s+moins|\+|minimum|min)?",
    re.IGNORECASE,
)
_DROP_THRESHOLD_RE = re.compile(r"(\d+[\.,]?\d*)\s*%", re.IGNORECASE)
_NEXT_DAY_RE = re.compile(
    r"(positif|haussier|n[eé]gatif|baissier|rebond)\s*(le\s+)?lendemain",
    re.IGNORECASE,
)
_FOLLOWUP_THRESHOLD_RE = re.compile(
    r"(?:pareil|idem|même\s*chose|et)\s+(?:mais\s+)?-?\s*(\d+[\.,]?\d*)\s*%",
    re.IGNORECASE,
)
_DROP_COND_RE = re.compile(
    r"\bquand\s+(vix|vvix|dxy|skew|gold)\s*(>=?|<=?|=)\s*([\d.,]+)",
    re.IGNORECASE,
)


def _drop_next_single(df: pd.DataFrame, threshold: float, is_drop: bool,
                       next_positive: bool) -> dict:
    """Calcule drop/gain pour un seuil unique sur un df déjà préparé."""
    if is_drop:
        mask = df["var_pct"] <= -threshold
    else:
        mask = df["var_pct"] >= threshold
    filtered = df[mask].copy()
    n = len(filtered)
    if n == 0:
        return {"n": 0, "threshold": threshold, "pct_next": 0.0, "dates": []}
    if next_positive:
        hits = int((filtered["next_close"] > filtered["close"]).sum())
    else:
        hits = int((filtered["next_close"] < filtered["close"]).sum())
    pct = float(hits / n * 100)
    dates_list = [{"date": idx.strftime("%Y-%m-%d"),
                   "var": round(float(row["var_pct"]), 2),
                   "next_var": round(float((row["next_close"] - row["close"]) / row["close"] * 100), 2)}
                  for idx, row in filtered.iterrows()]
    return {"n": n, "threshold": threshold, "pct_next": pct, "dates": dates_list}


def _compute_drop_next_day(query: str, session_state=None) -> dict | None:
    """Détecte 'ACTIF a perdu X% ou plus … lendemain' → pandas direct.
    Supporte multi-seuils (5%, 7%, 10%), condition VIX, et follow-up."""
    dm = _DROP_RE.search(query)
    nm = _NEXT_DAY_RE.search(query)

    # Follow-up contextuel : "pareil mais -7%", "et -10%"
    if dm is None and session_state is not None:
        fm = _FOLLOWUP_THRESHOLD_RE.search(query)
        if fm and session_state.get("_drop_next_ctx"):
            ctx = session_state["_drop_next_ctx"]
            new_thr = float(fm.group(1).replace(",", "."))
            print(f"[followup-context] drop_next seuil={new_thr}% "
                  f"asset={ctx['asset']} is_drop={ctx['is_drop']}", flush=True)
            return _compute_drop_next_day_core(
                ctx["asset"], [new_thr], ctx["is_drop"], ctx["next_positive"],
                ctx.get("cond_asset"), ctx.get("cond_op"), ctx.get("cond_thr"))
        return None

    if dm is None or nm is None:
        return None

    q = query.lower()
    _, eff_subj = get_effective_registries()
    asset = None
    for s in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", q):
            asset = s
            break
    if asset is None or asset not in eff_subj:
        return None

    verb = dm.group(1).lower()
    is_drop = verb in ("perdu", "perd", "chute", "chuté", "baisse", "baissé")
    next_dir = nm.group(1).lower().replace("é", "e")
    next_positive = next_dir in ("positif", "haussier", "rebond")

    # Condition additionnelle : "quand VIX > X"
    cm = _DROP_COND_RE.search(query)
    cond_asset = cm.group(1).lower() if cm else None
    cond_op = cm.group(2) if cm else None
    cond_thr = float(cm.group(3).replace(",", ".")) if cm else None

    # Extraire tous les seuils de la question
    thresholds = sorted(set(float(m.group(1).replace(",", "."))
                            for m in _DROP_THRESHOLD_RE.finditer(query)), reverse=False)
    # Exclure le seuil de la condition VIX s'il a été capturé
    if cond_thr is not None:
        thresholds = [t for t in thresholds if t != cond_thr]
    if not thresholds:
        return None

    return _compute_drop_next_day_core(
        asset, thresholds, is_drop, next_positive, cond_asset, cond_op, cond_thr)


def _compute_drop_next_day_core(asset: str, thresholds: list[float],
                                 is_drop: bool, next_positive: bool,
                                 cond_asset: str | None = None,
                                 cond_op: str | None = None,
                                 cond_thr: float | None = None) -> dict | None:
    """Cœur du calcul drop/gain multi-seuils avec condition optionnelle."""
    _, eff_subj = get_effective_registries()
    if asset not in eff_subj:
        return None
    df = _load_csv(eff_subj[asset]).copy()
    for col in ("open", "close"):
        if col in df.columns:
            df[col] = _to_numeric(df[col])
    df = df.set_index("time").sort_index()
    df["prev_close"] = df["close"].shift(1)
    df["var_pct"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100
    df["next_close"] = df["close"].shift(-1)
    df = df.dropna(subset=["prev_close", "next_close"])

    # Filtre condition additionnelle (ex: VIX > 20)
    cond_label = None
    if cond_asset and cond_op and cond_thr is not None:
        eff_cond, _ = get_effective_registries()
        if cond_asset in eff_cond:
            fname, col = eff_cond[cond_asset]
            df_c = _load_csv(fname).copy()
            col_l = col.lower().replace(" ", "_")
            if col_l in df_c.columns:
                df_c[col_l] = _to_numeric(df_c[col_l])
                cond_s = df_c.set_index("time")[col_l].dropna()
                cond_s.index = cond_s.index.normalize()
                cond_mask = _apply_op(cond_s, cond_op, cond_thr)
                cond_dates = set(cond_s[cond_mask].index)
                df = df[df.index.normalize().isin(cond_dates)]
                cond_label = f"{cond_asset.upper()} {cond_op} {cond_thr}"

    results = [_drop_next_single(df, thr, is_drop, next_positive) for thr in thresholds]

    return {"type": "C1_DROP_NEXT", "ok": True, "asset": asset.upper(),
            "is_drop": is_drop, "next_positive": next_positive,
            "cond_label": cond_label,
            "cond_asset": cond_asset, "cond_op": cond_op, "cond_thr": cond_thr,
            "results": results}


def _compute_lookup(query: str) -> dict | None:
    info = _detect_lookup(query)
    if info is None:
        return None
    _, eff_subj = get_effective_registries()
    asset = info["asset"]
    if asset not in eff_subj:
        return None
    df = _load_csv(eff_subj[asset]).copy()
    for col in ("open", "close", "high", "low"):
        if col in df.columns:
            df[col] = _to_numeric(df[col])
    df = df.set_index("time")
    df.index = df.index.normalize()

    if info["kind"] == "value_date":
        target = info["target_date"]
        if target not in df.index:
            return {"type": "C1_LOOKUP", "ok": False,
                    "error": f"Pas de données pour {asset.upper()} le {target.date()}."}
        field = info["field"]
        if field not in df.columns:
            return {"type": "C1_LOOKUP", "ok": False,
                    "error": f"Colonne '{field}' absente pour {asset.upper()}."}
        val = float(df.loc[target, field])
        label_map = {"close": "Clôture", "open": "Open", "high": "Haut", "low": "Bas"}
        return {"type": "C1_LOOKUP", "ok": True,
                "label": f"{asset.upper()} — {label_map.get(field, field)} du {target.strftime('%d/%m/%Y')}",
                "value": val, "unit": "pts"}

    if info["kind"] == "perf_month":
        month, year = info["month"], info["year"]
        mask = (df.index.year == year) & (df.index.month == month)
        sub = df[mask].dropna(subset=["open", "close"])
        if sub.empty:
            return {"type": "C1_LOOKUP", "ok": False,
                    "error": f"Pas de données pour {asset.upper()} {month}/{year}."}
        first_open = float(sub["open"].iloc[0])
        last_close = float(sub["close"].iloc[-1])
        perf = (last_close - first_open) / first_open * 100
        mname = [k for k, v in _MONTH_MAP.items() if v == month and len(k) > 4]
        mstr = mname[0].capitalize() if mname else str(month)
        return {"type": "C1_LOOKUP", "ok": True,
                "label": f"Performance {asset.upper()} {mstr} {year}",
                "value": perf, "unit": "%"}

    if info["kind"] == "perf_year":
        year = info["year"]
        mask = df.index.year == year
        sub = df[mask].dropna(subset=["open", "close"])
        if sub.empty:
            return {"type": "C1_LOOKUP", "ok": False,
                    "error": f"Pas de données pour {asset.upper()} en {year}."}
        first_open = float(sub["open"].iloc[0])
        last_close = float(sub["close"].iloc[-1])
        perf = (last_close - first_open) / first_open * 100
        return {"type": "C1_LOOKUP", "ok": True,
                "label": f"Performance {asset.upper()} {year}",
                "value": perf, "unit": "%"}
    return None


def _render_compare_side(label: str, r: dict) -> tuple[str, float, str]:
    """Extrait (label_affiché, valeur, unité) depuis un résultat lookup ou C1."""
    rtype = r.get("type", "")
    if rtype == "C1_LOOKUP" and r.get("ok"):
        return label, r["value"], r.get("unit", "")
    if rtype == "C1" and r.get("stats"):
        st = r["stats"]
        return label, st["mean_var"], "%"
    return label, float("nan"), ""


_QUANT_CSS = """<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=JetBrains+Mono:wght@400;500;700&family=DM+Sans:wght@300;400;500;600&display=swap');
:root{--bg-base:#09090f;--bg-card:#111119;--bg-card-hover:#16161f;--bg-input:#0d0d14;
--border:rgba(0,212,255,.12);--border-strong:rgba(0,212,255,.35);--accent:#00d4ff;
--accent-dim:rgba(0,212,255,.08);--positive:#00e676;--negative:#ff3d3d;--warning:#f5a623;
--text-primary:#e8e8f0;--text-secondary:#8888a8;--text-muted:#44445a;
--font-display:'Syne',sans-serif;--font-body:'DM Sans',sans-serif;--font-mono:'JetBrains Mono',monospace;
--radius:8px}
html,body,[data-testid="stAppViewContainer"]{background:var(--bg-base)!important;color:var(--text-primary)!important;font-family:var(--font-body)!important}
[data-testid="stSidebar"]{background:#0c0c14!important;border-right:1px solid var(--border)!important}
h1{font-family:var(--font-display)!important;font-size:28px!important;font-weight:800!important;letter-spacing:-.5px!important;color:var(--text-primary)!important}
h2,h3{font-family:var(--font-display)!important;font-weight:700!important;color:var(--text-primary)!important}
.stTextArea textarea{background:var(--bg-input)!important;border:1px solid var(--border)!important;border-radius:var(--radius)!important;color:var(--text-primary)!important;font-family:var(--font-mono)!important;font-size:13px!important;padding:12px 14px!important}
.stTextArea textarea:focus{border-color:var(--border-strong)!important;box-shadow:0 0 0 2px rgba(0,212,255,.08)!important}
.stButton>button{background:transparent!important;border:1px solid var(--border-strong)!important;border-radius:var(--radius)!important;color:var(--accent)!important;font-family:var(--font-body)!important;font-size:13px!important;font-weight:500!important;padding:8px 18px!important;transition:all .15s ease!important}
.stButton>button:hover{background:var(--accent-dim)!important;border-color:var(--accent)!important}
[data-testid="stMetric"]{background:var(--bg-card)!important;border:1px solid var(--border)!important;border-radius:var(--radius)!important;padding:14px 16px!important}
[data-testid="stMetricLabel"]{font-family:var(--font-body)!important;font-size:11px!important;text-transform:uppercase!important;letter-spacing:1px!important;color:var(--text-secondary)!important}
[data-testid="stMetricValue"]{font-family:var(--font-mono)!important;font-size:22px!important;font-weight:700!important;color:var(--text-primary)!important}
[data-testid="stMetricDelta"]{font-family:var(--font-mono)!important;font-size:12px!important}
[data-testid="stDataFrame"]{border:1px solid var(--border)!important;border-radius:var(--radius)!important;overflow:hidden!important}
[data-testid="stDataFrame"] th{background:#0c0c17!important;color:var(--text-secondary)!important;font-size:11px!important;text-transform:uppercase!important;letter-spacing:.8px!important;border-bottom:1px solid var(--border)!important}
[data-testid="stDataFrame"] td{color:var(--text-primary)!important;border-bottom:1px solid rgba(0,212,255,.05)!important}
[data-testid="stAlert"]{border-radius:var(--radius)!important;border-left-width:3px!important;font-size:13px!important}
[data-testid="stVegaLiteChart"],[data-testid="stArrowVegaLiteChart"]{border:1px solid var(--border)!important;border-radius:var(--radius)!important;background:var(--bg-card)!important}
[data-testid="stDownloadButton"]>button{background:transparent!important;border:1px solid var(--border)!important;color:var(--text-secondary)!important;font-size:12px!important}
[data-testid="stDownloadButton"]>button:hover{border-color:var(--accent)!important;color:var(--accent)!important}
hr{border:none!important;border-top:1px solid var(--border)!important;margin:16px 0!important}
[data-testid="stSidebar"] .stButton>button{background:var(--bg-card)!important;border:1px solid var(--border)!important;color:var(--text-secondary)!important;font-size:12px!important;text-align:left!important;width:100%!important;margin-bottom:3px!important}
[data-testid="stSidebar"] .stButton>button:hover{background:var(--bg-card-hover)!important;border-color:var(--border-strong)!important;color:var(--text-primary)!important}
.version-badge{display:inline-block;background:rgba(0,212,255,.08);border:1px solid var(--border);border-radius:4px;padding:2px 8px;font-family:var(--font-mono);font-size:11px;color:var(--accent)}
.question-display{background:var(--bg-input);border:1px solid var(--border);border-left:3px solid var(--accent);border-radius:var(--radius);padding:12px 16px;font-family:var(--font-mono);font-size:13px;color:var(--text-secondary);margin:12px 0}
.section-header{display:flex;align-items:center;gap:10px;margin:16px 0 10px}
.section-header .title{font-family:var(--font-display);font-size:16px;font-weight:700;color:var(--text-primary)}
.section-header .badge{background:rgba(0,212,255,.08);border:1px solid var(--border);border-radius:4px;padding:2px 8px;font-size:11px;font-family:var(--font-mono);color:var(--accent)}
.section-header .meta{font-size:11px;color:var(--text-muted);margin-left:auto}
.stat-card{background:var(--bg-card);border:1px solid var(--border);border-radius:var(--radius);padding:14px 16px;text-align:center}
.stat-card .label{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--text-muted);margin-bottom:6px}
.stat-card .value{font-family:var(--font-mono);font-size:22px;font-weight:700;color:var(--text-primary)}
.stat-card .sub{font-size:11px;color:var(--text-secondary);margin-top:3px}
.stat-card.positive{border-color:rgba(0,230,118,.3)}.stat-card.negative{border-color:rgba(255,61,61,.3)}.stat-card.accent{border-color:rgba(0,212,255,.3)}
.stat-card .value.pos{color:#00e676}.stat-card .value.neg{color:#ff3d3d}.stat-card .value.acc{color:#00d4ff}
@keyframes fadeInUp{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
.result-block{animation:fadeInUp .25s ease forwards}
</style>"""


def _stat_card(label: str, value: str, sub: str = "", sentiment: str = "neutral") -> str:
    vc = {"positive": "pos", "negative": "neg", "accent": "acc"}.get(sentiment, "")
    cc = sentiment if sentiment in ("positive", "negative", "accent") else ""
    return (f'<div class="stat-card {cc}"><div class="label">{label}</div>'
            f'<div class="value {vc}">{value}</div>'
            + (f'<div class="sub">{sub}</div>' if sub else "") + "</div>")


def _section_header(title: str, badge: str = "", meta: str = "") -> str:
    return (f'<div class="section-header"><span class="title">{title}</span>'
            + (f'<span class="badge">{badge}</span>' if badge else "")
            + (f'<span class="meta">{meta}</span>' if meta else "") + "</div>")


def _render_stat_row(cards: list[dict]) -> None:
    import streamlit as st
    html = "".join(
        f'<div style="flex:1;">{_stat_card(c["label"], c["value"], c.get("sub", ""), c.get("sentiment", "neutral"))}</div>'
        for c in cards)
    st.markdown(f'<div class="result-block" style="display:flex;gap:10px;margin:10px 0;">{html}</div>',
                unsafe_allow_html=True)


def _add_download(df: pd.DataFrame, label: str = "Télécharger",
                   filename: str = "export") -> None:
    """Bouton CSV sous un tableau."""
    import streamlit as st
    if df is None or df.empty:
        return
    csv = df.to_csv(index=True, sep=";", decimal=",").encode("utf-8-sig")
    st.download_button(f"⬇ {label} (CSV)", data=csv,
                       file_name=f"{filename}.csv", mime="text/csv")


def _result_to_text(result: dict, query: str) -> str:
    from datetime import datetime
    lines = ["SPX Quant Engine — Export",
             f"Date : {datetime.now().strftime('%d/%m/%Y %H:%M')}",
             f"Question : {query}", "=" * 60, ""]
    _SKIP = {"type", "ok", "_models", "_clf", "_reg", "dates_detail", "_feature_names", "_query"}
    def _fmt(k, v, indent=0):
        if str(k).startswith("_"):
            return
        p = "  " * indent
        if isinstance(v, list) and v and isinstance(v[0], dict):
            lines.append(f"{p}{k}:")
            for item in v[:30]:
                lines.append(p + "  " + " | ".join(f"{ik}: {iv}" for ik, iv in item.items() if not str(ik).startswith("_")))
        elif isinstance(v, dict):
            lines.append(f"{p}{k}:")
            for rk, rv in v.items():
                _fmt(rk, rv, indent + 1)
        elif isinstance(v, list):
            lines.append(f"{p}{k}: {', '.join(str(x) for x in v[:20])}")
        else:
            lines.append(f"{p}{k}: {v}")
    for k, v in result.items():
        if k not in _SKIP:
            _fmt(k, v)
    return "\n".join(lines)


def _add_download_response(result: dict) -> None:
    """Bouton téléchargement réponse complète."""
    import streamlit as st
    try:
        q = st.session_state.get("last_query", "")
        txt = _result_to_text(result, q)
        sub = result.get("sub_type", "export")
        st.download_button("⬇ Télécharger réponse", data=txt.encode("utf-8"),
                           file_name=f"spx_{sub}.txt", mime="text/plain",
                           key=f"dl_{sub}_{hash(txt) % 99999}")
    except Exception:
        pass


def _render_engulfing_cards(dates_detail: list) -> None:
    """Cards HTML pour N ≤ 5 engulfing occurrences."""
    import streamlit as st
    for d in dates_detail:
        vj = d.get("var_j", d.get("var", 0)) or 0
        vj1 = d.get("next_var", 0) or 0
        ok = d.get("success", False)
        bd = "#26a269" if ok else "#e01b24"
        cj = "#26a269" if vj > 0 else "#e01b24"
        cj1 = "#26a269" if vj1 > 0 else "#e01b24"
        cl = d.get("close", 0)
        st.markdown(f"""
        <div style="background:#1a1a2e;border-left:4px solid {bd};border-radius:8px;
             padding:12px 16px;margin:5px 0;display:flex;justify-content:space-between;
             align-items:center;flex-wrap:wrap;gap:8px;">
          <span style="font-size:14px;font-weight:600;color:#fff;min-width:90px;">{d.get('date','')}</span>
          <div style="text-align:center;min-width:70px;">
            <div style="font-size:10px;color:#888;">VAR J</div>
            <div style="font-size:15px;font-weight:700;color:{cj};">{vj:+.2f}%</div></div>
          <div style="text-align:center;min-width:70px;">
            <div style="font-size:10px;color:#888;">CLOSE</div>
            <div style="font-size:15px;font-weight:600;color:#fff;">{cl:.2f}</div></div>
          <div style="text-align:center;min-width:70px;">
            <div style="font-size:10px;color:#888;">VAR J+1</div>
            <div style="font-size:15px;font-weight:700;color:{cj1};">{vj1:+.2f}%</div></div>
          <span style="background:{'#1a3a2a' if ok else '#2e1a1a'};border-radius:16px;
                padding:4px 12px;font-size:12px;color:{bd};font-weight:600;">
            {'Succès' if ok else 'Échec'}</span>
        </div>""", unsafe_allow_html=True)


def _render_result(result: dict) -> None:
    import streamlit as st

    rtype = result.get("type", "")

    if rtype == "PATTERNS_RESULTS":
        data = result.get("data", {})
        n = data.get("n_patterns", 0)
        st.metric("Patterns retenus (p<0.05, OOS validé)", n)
        st.caption(
            f"Actifs : {', '.join(data.get('assets',[]))}  ·  "
            f"{data.get('n_combos_tested','?')} combos testés  ·  "
            f"{data.get('elapsed_sec','?')}s  ·  "
            f"Base rate : {data.get('base_rate','?')}"
        )
        patterns = data.get("patterns", [])
        if patterns:
            rows = []
            for p in patterns:
                rows.append({
                    "Condition": p.get("condition_str",""),
                    "Horizon": p.get("horizon",""),
                    "N (IS)": p["is"]["n"],
                    "% Haussiers (IS)": p["is"]["pct_bull"],
                    "Rend. moy IS (%)": p["is"]["mean_ret"],
                    "p-value IS": p["is"]["p_value"],
                    "N (OOS)": p["oos"]["n"],
                    "% Haussiers (OOS)": p["oos"]["pct_bull"],
                    "Invalidé par": p.get("invalidated_by", {}).get("condition","") if p.get("invalidated_by") else "",
                })
            df_pat = pd.DataFrame(rows)
            st.dataframe(df_pat, use_container_width=True)
            csv_bytes = df_pat.to_csv(index=False).encode("utf-8")
            st.download_button("Télécharger patterns CSV", data=csv_bytes,
                               file_name="patterns_results.csv", mime="text/csv")
        else:
            st.info("Aucun pattern statistiquement significatif trouvé.")
        return

    if rtype == "PATTERNS_LAUNCHED":
        if result.get("set_running"):
            st.session_state["patterns_running"] = True
            st.session_state["patterns_ready"] = False
        # Ne pas afficher le message d'attente si l'exploration est déjà terminée
        if not st.session_state.get("patterns_ready"):
            st.info(result.get("msg", "Exploration lancée en arrière-plan."))
            st.caption(f"Actifs : {', '.join(result.get('assets', []))}")
        return

    if rtype == "C1_COMPARE":
        if not result.get("ok"):
            st.error("Impossible de comparer ces deux éléments.")
            return
        left_r  = result["left"]["result"]
        right_r = result["right"]["result"]
        left_q  = result["left"]["query"]
        right_q = result["right"]["query"]

        lbl_l, val_l, unit_l = _render_compare_side(left_q,  left_r)
        lbl_r, val_r, unit_r = _render_compare_side(right_q, right_r)

        def _fmt(v, u):
            if pd.isna(v):
                return "—"
            sign = "+" if u == "%" and v > 0 else ""
            if u == "%":
                return f"{sign}{v:.2f}%"
            return f"{v:,.2f} {u}".strip()

        col_l, col_r = st.columns(2)
        col_l.metric(lbl_l, _fmt(val_l, unit_l))
        col_r.metric(lbl_r, _fmt(val_r, unit_r))

        # Graphique barre toujours affiché, colonnes Hausse/Baisse selon signe
        short_l = left_q[:40]
        short_r = right_q[:40]
        idx = [short_l, short_r]
        chart_df = pd.DataFrame({
            "Hausse (%)": [val_l if (not pd.isna(val_l) and val_l >= 0) else float("nan"),
                           val_r if (not pd.isna(val_r) and val_r >= 0) else float("nan")],
            "Baisse (%)": [val_l if (not pd.isna(val_l) and val_l < 0)  else float("nan"),
                           val_r if (not pd.isna(val_r) and val_r < 0)  else float("nan")],
        }, index=idx)
        st.bar_chart(chart_df, color=["#26a269", "#e01b24"],
                     height=260, use_container_width=True)
        return

    if rtype == "IC_RIC":
        if not result["ok"]:
            st.error(result.get("error", "Erreur calcul IC/RIC."))
            return
        if not result.get("exact_match", True) and result.get("vix_requested"):
            st.info(f"Données basées sur {result['source']} (VIX le plus proche disponible pour VIX {result['vix_requested']})")
        credit = result["credit"]
        credit_label = f"+{credit:.2f} pts (crédit)" if credit >= 0 else f"{credit:.2f} pts (débit)"
        c1, c2, c3 = st.columns(3)
        c1.metric(result["label"], credit_label)
        c2.metric("Strike ATM", f"{result['atm_strike']:.0f}")
        c3.metric("Delta ATM", f"{result['atm_delta']:.3f}")
        st.markdown("---")
        legs_df = pd.DataFrame(result["legs"])
        legs_df.columns = ["Jambe", "Strike", "Prix"]
        legs_df["Prix"] = legs_df["Prix"].apply(lambda x: f"{x:+.2f}")
        st.dataframe(legs_df, use_container_width=True, hide_index=True)
        st.caption(f"Source : {result['source']}")
        return

    if rtype == "C1_DROP_NEXT":
        asset = result["asset"]
        is_drop = result["is_drop"]
        next_pos = result["next_positive"]
        direction = "perdu" if is_drop else "gagné"
        next_label = "positif" if next_pos else "négatif"
        subs = result.get("results", [])
        cond_label = result.get("cond_label")
        if not subs:
            st.warning("Aucun résultat.")
            return
        header = f"**{asset}** — jours ayant {direction} ≥ X% (close J-1 → close J)"
        if cond_label:
            header += f"  ·  filtre : {cond_label}"
        st.markdown(header)
        # Metrics côte à côte (max 3 colonnes par ligne)
        cols = st.columns(min(len(subs), 3))
        for i, sub in enumerate(subs):
            with cols[i % 3]:
                st.metric(f"≥ {sub['threshold']}%", f"{sub['n']} fois")
                st.metric(f"% {next_label} J+1", f"{sub['pct_next']:.1f}%")
        # Scatter chart : Var J vs Var J+1 (premier seuil)
        first = subs[0]
        dates = first.get("dates", [])
        if dates and len(dates) > 1:
            df_sc = pd.DataFrame(dates)
            df_sc.columns = ["Date", "Var J (%)", "Var J+1 (%)"]
            df_sc["Couleur"] = df_sc["Var J+1 (%)"].apply(
                lambda x: "Positif J+1" if x >= 0 else "Négatif J+1")
            st.markdown(f"---\n**Scatter : Var J vs Var J+1** (seuil ≥ {first['threshold']}%)")
            pos = df_sc[df_sc["Couleur"] == "Positif J+1"]
            neg = df_sc[df_sc["Couleur"] == "Négatif J+1"]
            scatter_df = pd.DataFrame(index=range(len(df_sc)))
            scatter_df["x"] = df_sc["Var J (%)"].values
            if not pos.empty:
                s = pd.Series(dtype=float, index=scatter_df.index)
                s.iloc[pos.index] = pos["Var J+1 (%)"].values
                scatter_df["Positif J+1"] = s
            if not neg.empty:
                s = pd.Series(dtype=float, index=scatter_df.index)
                s.iloc[neg.index] = neg["Var J+1 (%)"].values
                scatter_df["Négatif J+1"] = s
            scatter_df = scatter_df.set_index("x")
            colors = []
            if "Positif J+1" in scatter_df.columns:
                colors.append("#26a269")
            if "Négatif J+1" in scatter_df.columns:
                colors.append("#e01b24")
            st.scatter_chart(scatter_df, color=colors, height=300, use_container_width=True)
            # Barres horizontales compactes (résumé visuel des dates)
            bar_df = pd.DataFrame({
                "Hausse J+1 (%)": df_sc["Var J+1 (%)"].where(df_sc["Var J+1 (%)"] >= 0),
                "Baisse J+1 (%)": df_sc["Var J+1 (%)"].where(df_sc["Var J+1 (%)"] < 0),
            }, index=df_sc["Date"])
            st.bar_chart(bar_df, color=["#26a269", "#e01b24"],
                         height=250, use_container_width=True)
            # CSV export
            csv_bytes = df_sc[["Date","Var J (%)","Var J+1 (%)"]].to_csv(index=False).encode("utf-8")
            st.download_button("Télécharger CSV", data=csv_bytes,
                               file_name=f"{asset}_drop_next.csv", mime="text/csv")
        # Stocker contexte pour follow-up
        all_dates = [d["date"] for sub in subs for d in sub.get("dates", [])]
        st.session_state["last_dates"] = sorted(set(all_dates))
        st.session_state["last_asset"] = asset.lower()
        st.session_state["_drop_next_ctx"] = {
            "asset": asset.lower(), "is_drop": is_drop, "next_positive": next_pos,
            "cond_asset": result.get("cond_asset"),
            "cond_op": result.get("cond_op"),
            "cond_thr": result.get("cond_thr"),
        }
        return

    if rtype == "INTERPRETED":
        if not result.get("ok"):
            st.error(result.get("error", "Erreur."))
            return

        # Dispatch vers render_engine.py (HTML/Chart.js) pour TOUS les types
        try:
            from render_engine import dispatch_render
            html_str, height = dispatch_render(result)
            st.components.v1.html(html_str, height=height, scrolling=False)
        except Exception as e:
            st.warning(f"Erreur rendu: {e}")

        # Engulfing analysis/by_year : ajouter curseurs interactifs APRÈS le HTML
        _sub_check = result.get("sub_type") or result.get("sub", "")
        if _sub_check not in ("engulfing_analysis", "engulfing_by_year"):
            _add_download_response(result)
            return

        sub = _sub_check

        if sub == "single_value":
            val = result["value"]
            unit = result.get("unit", "")
            sign = "+" if unit == "%" and val > 0 else ""
            fmt = f"{sign}{val:.2f}{unit}" if unit == "%" else f"{val:,.2f} {unit}"
            st.metric(result["label"], fmt)
            return

        if sub == "single_value_enriched":
            val = result["value"]
            unit = result.get("unit", "")
            sign = "+" if unit == "%" and val > 0 else ""
            fmt = f"{sign}{val:.2f}{unit}" if unit == "%" else f"{val:,.2f} {unit}"
            ctx = result.get("context", {})
            c1, c2, c3 = st.columns(3)
            c1.metric(result["label"], fmt)
            c2.metric("Variation J", f"{ctx['var_pct']:+.2f}%" if ctx.get("var_pct") is not None else "—")
            c3.metric("Volume", f"{ctx['volume_ratio']:.1f}x moy" if ctx.get("volume_ratio") else "—")
            caps = []
            if ctx.get("pattern"):
                caps.append(f"Pattern : **{ctx['pattern']}**")
            if ctx.get("vix"):
                caps.append(f"VIX : {ctx['vix']:.1f}")
            if caps:
                st.caption(" · ".join(caps))
            return

        if sub == "text_explanation_general":
            st.markdown(f"**{result.get('subject', '').upper()} — Explication**")
            for line in result.get("text", "").split("\n"):
                if line.strip():
                    st.markdown(line)
            return

        if sub == "intraday_best_time":
            st.markdown("**SPX/SPY — Meilleur moment pour acheter**")
            b = result["best"]
            c1, c2, c3 = st.columns(3)
            c1.metric("Meilleure heure", b["entry_time"])
            c2.metric("% positif→clôture", f"{b['pct_positive']:.1f}%")
            c3.metric("Rendement moyen", f"{b['mean_ret']:+.3f}%")
            rows = result.get("results", [])
            if rows:
                bar_data = pd.DataFrame({"% positif": [r["pct_positive"] for r in rows]},
                                        index=[r["entry_time"] for r in rows])
                st.bar_chart(bar_data, height=220, color="#26a269")
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

        if sub == "intraday_overnight":
            st.markdown("**SPX — Analyse overnight (futures)**")
            c1, c2, c3 = st.columns(3)
            c1.metric("Corrélation overnight→jour", f"{result['corr_overnight_day']:+.3f}")
            c2.metric("Même direction", f"{result['pct_same_direction']:.1f}%")
            c3.metric("Ret overnight moyen", f"{result['mean_overnight_ret']:+.3f}%")
            if result.get("conclusion"):
                st.info(result["conclusion"])
            return

        if sub == "intraday_conditional":
            cond = result.get("condition", "condition")
            st.markdown(f"**SPX/SPY — {cond} ({result.get('n_sessions',0)} sessions)**")
            rows = result.get("horizon_results", [])
            if rows:
                df_r = pd.DataFrame(rows)
                bar_data = pd.DataFrame({"% positif": [r["% positif"] for r in rows]},
                                        index=[r["Horizon"] for r in rows])
                st.bar_chart(bar_data, height=220, color="#1c71d8")
                st.dataframe(df_r, use_container_width=True, hide_index=True)
                _add_download(df_r, "Télécharger", f"SPX_intraday_{cond.replace(' ','_')}")
            if result.get("conclusion"):
                st.info(result["conclusion"])
            return

        if sub == "intraday_general":
            st.markdown(f"**SPX/SPY — {result['horizon'].replace('ret_','').replace('min',' min')}**")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("% positif", f"{result['pct_positive']:.1f}%")
            c2.metric("Moy", f"{result['mean']:+.3f}%")
            c3.metric("Médiane", f"{result['median']:+.3f}%")
            c4.metric("N sessions", result["n"])
            if result.get("conclusion"):
                st.info(result["conclusion"])
            return

        if sub in ("ml_prediction", "ml_amplitude"):
            pred = result.get("prediction", {})
            stats = result.get("model_stats", {})
            entry = result.get("entry_point", "9h30")
            cat = pred.get("amplitude_category", pred.get("direction", "?"))
            cat_colors = {"FORT": "#26a269", "FAIBLE": "#1c71d8", "INCERTAIN": "#e5a50a",
                          "hausse": "#26a269", "baisse": "#e01b24"}
            cc = cat_colors.get(cat, "#888")
            st.markdown(f"**SPX — Prédiction ML ({entry})**")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Signal", cat, delta_color="off")
            amp_pct = pred.get("amplitude_pct", 0)
            amp_pts = pred.get("amplitude_pts", 0)
            c2.metric("Amplitude", f"{amp_pct:.2f}%", delta=f"~{amp_pts:.0f} pts" if amp_pts else None, delta_color="off")
            c3.metric("Précision", f"{stats.get('category_accuracy', stats.get('direction_accuracy', 0)):.1f}%")
            c4.metric("MAE", f"{stats.get('amplitude_mae', 0):.4f}%")
            probas = pred.get("probabilities", {})
            if probas:
                st.caption(f"Probabilités — FORT:{probas.get('fort',0):.0f}% · INCERTAIN:{probas.get('incertain',0):.0f}% · FAIBLE:{probas.get('faible',0):.0f}%")
            if pred.get("ric_signal"):
                st.success(f"Signal RIC — amplitude ≥ 0.45% depuis {entry}")
            elif pred.get("ic_signal"):
                st.success(f"Signal IC — amplitude ≤ 0.23% depuis {entry}")
            st.caption(f"Modèle: {stats.get('best_model','')} | Train:{stats.get('n_train',0)}j | Test:{stats.get('n_test',0)}j")
            top_f = result.get("top_features", {})
            if top_f:
                st.markdown("**Features prédictives**")
                st.bar_chart(pd.DataFrame({"Importance": list(top_f.values())},
                             index=list(top_f.keys())), height=200, color="#9141ac")
            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

        if sub == "engulfing_duration":
            st.markdown(f"**{result['ticker']}** — Durée de la baisse après {result.get('pattern', 'engulfing')}")
            c1, c2, c3 = st.columns(3)
            c1.metric("Durée médiane", f"{result['median_days']} j")
            c2.metric("Durée moyenne", f"{result['mean_days']:.1f} j")
            c3.metric("Occurrences", result["n"])
            dist = result.get("distribution", [])
            if dist:
                df_d = pd.DataFrame(dist)
                st.bar_chart(df_d.set_index("jours")["count"], height=200, color="#1c71d8")
            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

        if sub == "streak_analysis":
            direction = result.get("direction", "up")
            label = "haussière" if direction == "up" else "baissière"
            st.markdown(f"**{result['ticker']}** — Séquences {label} consécutives")
            best = result.get("best")
            if best:
                c1, c2, c3 = st.columns(3)
                c1.metric("Record", f"{best['length']} jours")
                c2.metric("Du", best["start"])
                c3.metric("Au", best["end"])
            st.metric("Durée moyenne", f"{result['avg_streak']:.1f} jours")
            top5 = result.get("top5", [])
            if top5:
                df_t = pd.DataFrame(top5)
                df_t.columns = ["Durée", "Début", "Fin"]
                st.dataframe(df_t, use_container_width=True, hide_index=True)
            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

        if sub == "engulfing_volume_threshold":
            st.markdown(f"**{result['ticker']}** — Seuil volume pour {result['target_rate']}% de succès (BE)")
            opt = result.get("optimal")
            if opt:
                c1, c2, c3 = st.columns(3)
                c1.metric("Vol ratio minimum", f"{opt['Vol ratio min']:.2f}x")
                c2.metric("Taux succès", f"{opt['Taux succès %']:.1f}%")
                c3.metric("Occurrences", opt["N occurrences"])
            rows = result.get("results", [])
            if rows:
                df_r = pd.DataFrame(rows)
                bar_data = pd.DataFrame({"Taux succès %": [r["Taux succès %"] for r in rows]},
                                        index=[str(r["Vol ratio min"]) for r in rows])
                st.bar_chart(bar_data, height=220, color="#26a269")
                st.dataframe(df_r, use_container_width=True, hide_index=True)
            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

        if sub == "engulfing_avg_perf":
            st.markdown(f"**{result['ticker']}** — Performance moyenne après {result.get('pattern', 'engulfing')}")
            c1, c2, c3 = st.columns(3)
            c1.metric("Var moy J+1", f"{result['avg_next']:+.2f}%")
            c2.metric("Var médiane J+1", f"{result['med_next']:+.2f}%")
            c3.metric("% positif J+1", f"{result['pct_pos_next']:.1f}%")
            st.caption(f"Sur {result['n']} occurrences")
            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

        if sub == "bias_analysis":
            biais = result["biais"]
            icon = "+" if biais == "haussier" else "-" if biais == "baissier" else "="
            st.markdown(f"**{result['ticker']}** — Analyse de biais")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("% jours positifs", f"{result['pct_pos']:.1f}%")
            c2.metric("Var. moyenne", f"{result['mean_var']:+.3f}%")
            c3.metric("Var. médiane", f"{result['median_var']:+.3f}%")
            c4.metric("Biais", f"{biais.upper()} {icon}")
            spx = result.get("spx_context", {})
            if spx:
                st.caption(f"SPX même période : {spx.get('pct_pos',0)}% positif, moy {spx.get('mean_var',0):+.3f}%")
            st.info(f"{result['ticker']} a un biais **{biais}** : {result['pct_pos']:.1f}% de jours positifs sur {result['n']} séances (skewness: {result['skew']}).")
            _add_download_response(result)
            return

        if sub == "correlation_scan":
            st.markdown(f"**{result['ticker']}** — Corrélations avec {result['n_assets']} actifs")
            rows = result.get("results", [])
            pos = [r for r in rows if r["Corrélation"] > 0][:5]
            neg = [r for r in rows if r["Corrélation"] < 0][:5]
            if pos or neg:
                c1, c2 = st.columns(2)
                with c1:
                    st.markdown("**Positives**")
                    for r in pos:
                        st.metric(r["Actif"], f"{r['Corrélation']:+.4f}",
                                  delta=r["Force"], delta_color="normal")
                with c2:
                    st.markdown("**Négatives**")
                    for r in neg:
                        st.metric(r["Actif"], f"{r['Corrélation']:+.4f}",
                                  delta=r["Force"], delta_color="inverse")
            if rows:
                st.divider()
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

        if sub == "correlation":
            c = result["corr"]
            interp = "forte positive" if c > 0.7 else "modérée positive" if c > 0.3 else "faible" if c > -0.3 else "modérée négative" if c > -0.7 else "forte négative"
            st.markdown(f"**Corrélation {result['ticker']} / {result['ticker_2']}**")
            c1, c2 = st.columns(2)
            c1.metric("Coefficient de Pearson", f"{c:.4f}")
            c2.metric("Interprétation", interp)
            st.caption(f"Calculé sur {result['n']} séances communes (var_pct journalières)")
            _add_download_response(result)
            return

        if sub == "multi_condition":
            r = result
            if r["n"] == 0:
                st.info(f"Aucun jour trouvé pour {r.get('asset_1','')} + {r.get('asset_2','')} avec ces conditions.")
                _add_download_response(result)
            return
            st.markdown(f"**{r.get('asset_2','')}** quand {r.get('cond_1','')} ET {r.get('cond_2','')}")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Occurrences", r["n"])
            c2.metric("% positif J+1", f"{r['pct_pos_next']:.1f}%")
            c3.metric("Var moy J+1", f"{r['mean_next']:+.2f}%")
            c4.metric("Var méd J+1", f"{r['median_next']:+.2f}%")
            dates = r.get("dates", [])
            if dates:
                df_d = pd.DataFrame(dates).rename(columns={"date": "Date", "var_J": "Var J (%)", "next_var": "Var J+1 (%)"})
                st.dataframe(df_d, use_container_width=True, hide_index=True)
            return

        if sub == "spx_overnight":
            active = result.get("active_patterns", [])
            all_p = result.get("all_patterns", [])
            st.markdown("**SPX Overnight — Patterns actifs aujourd'hui**")
            if active:
                for p in active:
                    tag = "ACTIONNABLE" if p.get("actionnable") else "observé"
                    arr = "+" if p["direction"] == "hausse" else "-"
                    st.metric(f"{tag} — {p['label']}",
                              f"{p['taux_is']:.1f}% {arr}",
                              delta=f"méd. {p['median_amp']:+.3f}% | n={p['n']} | OOS: {p.get('taux_oos') or '—'}%",
                              delta_color="off")
            else:
                st.info("Aucun pattern overnight actif aujourd'hui.")
            if all_p:
                st.divider()
                st.markdown("**Top 15 patterns historiques**")
                df_p = pd.DataFrame(all_p)
                df_p["Statut"] = df_p["actionnable"].map({True: "Actionnable", False: "Observé"})
                df_p["Actif"] = df_p["active_today"].map({True: "OUI", False: ""})
                display_cols = ["Actif", "Statut", "label", "direction", "taux_is", "taux_oos", "n", "median_amp"]
                rename = {"label": "Signal", "direction": "Dir.", "taux_is": "IS %",
                          "taux_oos": "OOS %", "n": "N", "median_amp": "Amp. méd. %"}
                st.dataframe(df_p[[c for c in display_cols if c in df_p.columns]].rename(columns=rename),
                             use_container_width=True, hide_index=True)
            return

        if sub == "best_single":
            st.markdown(f"**{result.get('ticker', '')}** — {result['label']}")
            c1, c2, c3 = st.columns(3)
            c1.metric("Variation", f"{'+' if result['var']>0 else ''}{result['var']:.2f}%")
            c2.metric("Date", result["date"])
            c3.metric("Clôture", f"{result['close']:,.2f}")
            return

        if sub == "best_multi":
            label = result.get("label", "meilleur jour")
            st.markdown(f"**{result['ticker']} — {label} par année**")
            cols = st.columns(len(result["results"]))
            for i, r in enumerate(result["results"]):
                with cols[i]:
                    st.metric(str(r["year"]),
                              f"{'+' if r['var']>0 else ''}{r['var']:.2f}%",
                              delta=f"{r['date']}", delta_color="off")
            return

        if sub == "pattern_last":
            st.markdown(f"**{result['ticker']}** — Dernier {result['pattern']}")
            c1, c2 = st.columns(2)
            c1.metric("Date", result["date"])
            c2.metric("Var J", f"{result['var']:+.2f}%",
                      delta=f"close {result['close']:,.2f}", delta_color="off")
            if result.get("next_var") is not None:
                st.metric("Var J+1", f"{result['next_var']:+.2f}%")
            st.caption(f"Total {result['pattern']} dans l'historique : {result['n_total']}")
            return

        if sub == "pattern_all":
            st.markdown(f"**{result['ticker']}** — {result['pattern']} ({result['n']} occurrences)")
            if result.get("pct_neg_next"):
                st.metric("% baisse J+1", f"{result['pct_neg_next']:.1f}%")
            dates = result.get("dates", [])
            if dates:
                df_d = pd.DataFrame(dates)
                cols_rename = {"date": "Date", "var": "Var J (%)", "close": "Close"}
                if "next_var" in df_d.columns:
                    cols_rename["next_var"] = "Var J+1 (%)"
                df_d = df_d.rename(columns=cols_rename)
                st.dataframe(df_d, use_container_width=True, hide_index=True)
            return

        if sub == "weekday":
            st.markdown(f"**{result['ticker']} — Performance par jour de la semaine**")
            rows = result.get("rows", [])
            if rows:
                df_wd = pd.DataFrame(rows)
                df_wd.columns = ["Jour", "Var moy %", "Var méd %", "Nb séances", "% positif"]
                bar_data = pd.DataFrame({"Var moy %": [r["var_moy"] for r in rows]},
                                        index=[r["jour"] for r in rows])
                st.bar_chart(bar_data, color="#1c71d8", height=250, use_container_width=True)
                st.dataframe(df_wd, use_container_width=True, hide_index=True)
            if result.get("conclusion"):
                st.info(result["conclusion"])
            return

        if sub == "monthly":
            st.markdown(f"**{result['ticker']} — Performance par mois**")
            rows = result.get("rows", [])
            if rows:
                df_mo = pd.DataFrame(rows)
                df_mo.columns = ["Mois", "Var moy %", "Var méd %", "Nb séances", "% positif"]
                bar_data = pd.DataFrame({"Var moy %": [r["var_moy"] for r in rows]},
                                        index=[r["mois"] for r in rows])
                st.bar_chart(bar_data, color="#1c71d8", height=250, use_container_width=True)
                st.dataframe(df_mo, use_container_width=True, hide_index=True)
            if result.get("conclusion"):
                st.info(result["conclusion"])
            return

        if sub == "annual_multi":
            st.markdown(f"**{result['ticker']} — Performance annuelle**")
            cols = st.columns(len(result["results"]))
            for i, r in enumerate(result["results"]):
                with cols[i]:
                    sign = "+" if r["perf"] > 0 else ""
                    st.metric(str(r["year"]), f"{sign}{r['perf']:.2f}%")
            return

        if sub == "count":
            c1, c2 = st.columns(2)
            c1.metric(result.get("label", "Jours"), result["count"])
            c2.metric("Sur total", f"{result['pct']:.1f}%",
                      delta=f"{result['total']} séances", delta_color="off")
            return

        if sub == "filter_abs":
            st.markdown(f"**{result['ticker']}** — jours avec |variation| ≥ {result['threshold']}%")
            c1, c2, c3 = st.columns(3)
            c1.metric("Nb jours", result["n"])
            c2.metric("% positif J+1", f"{result['pct_positive_next']:.1f}%")
            c3.metric("Var moy J+1", f"{result['mean_next']:+.2f}%")
            dates = result.get("dates", [])
            if dates:
                df_d = pd.DataFrame(dates)
                cols_map = {"date": "Date", "var": "Var J (%)", "next_var": "Var J+1 (%)"}
                df_d = df_d.rename(columns=cols_map)
                st.dataframe(df_d, use_container_width=True, hide_index=True)
            return

        if sub == "engulfing_analysis":
            _tk_key = result.get('ticker', 'unk').replace('.', '_')
            _pat_r = result.get('pattern', 'bearish')

            _sl1, _sl2 = st.columns(2)
            with _sl1:
                _jend_r = st.select_slider(
                    "Fenêtre J+",
                    options=[1, 2, 3, 4, 5], value=5,
                    format_func=lambda x: f"J+{x}",
                    key=f"jend_r_{_tk_key}"
                )
            with _sl2:
                _months_r = st.slider(
                    "Historique (mois)",
                    min_value=3, max_value=60, value=36, step=3,
                    key=f"months_r_{_tk_key}"
                )

            _all_rows = result.get("rows", [])
            _cutoff_r = pd.Timestamp.now() - pd.DateOffset(months=_months_r)
            _rows_filtered = [
                r for r in _all_rows
                if pd.Timestamp(r["date"]) >= _cutoff_r
            ]

            from ticker_analysis import _find_ticker_csv, load_earnings_dates
            _tk_name = result.get('ticker', '')
            _tk_csv = _find_ticker_csv(_tk_name)
            _rows_recalc = []
            if _tk_csv and _tk_csv.exists():
                _df_r2 = pd.read_csv(_tk_csv, sep=";")
                _df_r2.columns = [c.strip().lower() for c in _df_r2.columns]
                _df_r2["time"] = pd.to_datetime(
                    _df_r2["time"].astype(str).str.strip(), errors="coerce")
                _df_r2 = _df_r2.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
                for _nc in ["open", "high", "low", "close", "volume"]:
                    if _nc in _df_r2.columns:
                        _df_r2[_nc] = pd.to_numeric(
                            _df_r2[_nc].astype(str).str.replace(",", ".").str.replace(" ", ""),
                            errors="coerce")
                _td_r = sorted(_df_r2["time"].dt.normalize().unique())
                _tidx_r = {d: i for i, d in enumerate(_td_r)}

                _df_spx = _cached_load_csv(str(DATA_DIR / "SPX_daily.csv"))
                _td_spx = None
                _tidx_spx = None
                if _df_spx is not None:
                    _td_spx = sorted(_df_spx["time"].dt.normalize().unique())
                    _tidx_spx = {d: i for i, d in enumerate(_td_spx)}

                for _row in _rows_filtered:
                    _sdt = pd.Timestamp(_row["date"]).normalize()
                    _sidx = _tidx_r.get(_sdt)
                    if _sidx is None:
                        continue
                    _cj = _row["close"]
                    _ok = False
                    _best = None
                    for _jj in range(1, _jend_r + 1):
                        if _sidx + _jj >= len(_td_r):
                            break
                        _fr = _df_r2[_df_r2["time"].dt.normalize() == _td_r[_sidx + _jj]]
                        if len(_fr) == 0:
                            continue
                        _fc = float(_fr.iloc[0]["close"])
                        _fl = float(_fr.iloc[0]["low"]) if "low" in _fr.columns else _fc
                        _fh = float(_fr.iloc[0]["high"]) if "high" in _fr.columns else _fc
                        if _pat_r == "bearish":
                            _cand = min(_fl, _fc)
                            _pf = (_cand - _cj) / _cj * 100
                            if _best is None or _pf < _best:
                                _best = _pf
                            if _cand < _cj:
                                _ok = True
                        else:
                            _cand = max(_fh, _fc)
                            _pf = (_cand - _cj) / _cj * 100
                            if _best is None or _pf > _best:
                                _best = _pf
                            if _cand > _cj:
                                _ok = True

                    _spx_j1 = None
                    if not _ok and _df_spx is not None:
                        _si2 = _tidx_spx.get(_sdt)
                        if _si2 is not None and _si2 + 1 < len(_td_spx):
                            _sr0 = _df_spx[_df_spx["time"].dt.normalize() == _td_spx[_si2]]
                            _sr1 = _df_spx[_df_spx["time"].dt.normalize() == _td_spx[_si2 + 1]]
                            if len(_sr0) > 0 and len(_sr1) > 0:
                                _sc0 = float(_sr0.iloc[0]["close"])
                                _sc1 = float(_sr1.iloc[0]["close"])
                                _spx_j1 = round((_sc1 - _sc0) / _sc0 * 100, 2)

                    _rows_recalc.append({
                        **_row,
                        "success": _ok,
                        "best_move": round(_best, 2) if _best else None,
                        "spx_j1": _spx_j1,
                    })

            _n = len(_rows_recalc)
            _ns = sum(1 for r in _rows_recalc if r["success"])
            _nf = _n - _ns
            _wr = _ns / _n * 100 if _n > 0 else 0

            st.markdown(f"**{result['ticker']}** — {_pat_r} engulfing strict")
            _wrc = "#26a269" if _wr >= 70 else "#f6d32d" if _wr >= 45 else "#e01b24"
            st.markdown(
                f"<div style='background:#1a1a2e;border-radius:10px;"
                f"padding:12px;margin:8px 0;text-align:center;'>"
                f"<span style='color:#888;font-size:13px;'>Win rate J+{_jend_r} — "
                f"{_n} signaux ({_months_r} mois)</span><br>"
                f"<span style='font-size:30px;font-weight:700;color:{_wrc};'>"
                f"{_wr:.0f}%</span> "
                f"<span style='font-size:14px;color:#888;'>({_ns}✅ {_nf}❌)</span>"
                f"</div>", unsafe_allow_html=True
            )

            for _sv in reversed(_rows_recalc):
                _cb = "#26a269" if _sv["success"] else "#e01b24"
                _vc = "#e01b24" if _sv["var_j"] < 0 else "#26a269"
                _bc_v = _sv.get("best_move") or 0
                _bc = "#26a269" if (_bc_v < 0 and _pat_r == "bearish") or (_bc_v > 0 and _pat_r != "bearish") else "#e01b24"
                _spx_txt = ""
                if not _sv["success"] and _sv.get("spx_j1") is not None:
                    _sc = "#e01b24" if _sv["spx_j1"] < 0 else "#26a269"
                    _spx_txt = f"&nbsp;|&nbsp; SPX J+1 : <b style='color:{_sc};'>{_sv['spx_j1']:+.2f}%</b>"
                _bm = f"{_bc_v:+.2f}%" if _bc_v else "—"
                st.markdown(f"""
<div style="background:#1a1a2e;border-left:4px solid {_cb};
     border-radius:8px;padding:10px 14px;margin:4px 0;">
  <div style="display:flex;justify-content:space-between;">
    <b style="color:#fff;">{_sv['date']}</b>
    <span style="color:{_cb};font-weight:600;">{'✅' if _sv['success'] else '❌'}</span>
  </div>
  <div style="font-size:13px;color:#ccc;margin-top:4px;">
    Close J:<b style="color:#fff;"> {_sv['close']:.2f}</b>&nbsp;|&nbsp;
    Var J:<b style="color:{_vc};"> {_sv['var_j']:+.2f}%</b>&nbsp;|&nbsp;
    Best J+1..{_jend_r}:<b style="color:{_bc};"> {_bm}</b>{_spx_txt}
  </div>
</div>""", unsafe_allow_html=True)

            if result.get("earn_count"):
                st.caption(f"📅 Earnings ±5j exclus ({result['earn_count']} dates)")
            _add_download_response(result)
            return

        if sub == "engulfing_thresholds":
            st.markdown(f"**{result['ticker']}** — {result['pattern']} · Seuil → Taux de réussite")
            table = result.get("table", [])
            if table:
                df_t = pd.DataFrame(table)[["seuil", "taux", "n"]]
                df_t.columns = ["Seuil", "Taux %", "N succès"]
                st.dataframe(df_t, use_container_width=True, hide_index=True)
            return

        if sub == "engulfing_vix":
            st.markdown(f"**{result['ticker']}** — {result['pattern']} · Taux par niveau VIX")
            table = result.get("table", [])
            if table:
                df_v = pd.DataFrame(table)
                df_v.columns = ["VIX range", "N total", "N succès", "Taux %"]
                st.dataframe(df_v, use_container_width=True, hide_index=True)
            return

        if sub == "multi_threshold":
            st.markdown(f"**{result['ticker']}** — Occurrences par seuil")
            rows = result.get("results", [])
            if rows:
                df_t = pd.DataFrame(rows)
                has_j1 = any(r.get("% positif J+1") is not None for r in rows)
                if has_j1:
                    bar_data = pd.DataFrame({"% positif J+1": [r.get("% positif J+1", 0) or 0 for r in rows]},
                                            index=[r["Seuil"] for r in rows])
                    st.bar_chart(bar_data, color="#26a269", height=220, use_container_width=True)
                else:
                    bar_data = pd.DataFrame({"Occurrences": [r["Occurrences"] for r in rows]},
                                            index=[r["Seuil"] for r in rows])
                    st.bar_chart(bar_data, color="#1c71d8", height=220, use_container_width=True)
                st.dataframe(df_t, use_container_width=True, hide_index=True)
            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

        if sub == "engulfing_by_year":
            _tk_name_by = result.get('ticker', 'UNK')
            _pat_by = result.get('pattern', 'bearish')
            _is_bear_by = "bearish" in _pat_by.lower()

            st.markdown(f"**{_tk_name_by}** — {_pat_by} engulfing · Par année")

            _tk_key2 = _tk_name_by.replace('.', '_') + "_by"
            _sl1b, _sl2b = st.columns(2)
            with _sl1b:
                _jend_by = st.select_slider(
                    "Fenêtre J+", options=[1, 2, 3, 4, 5], value=5,
                    format_func=lambda x: f"J+{x}",
                    key=f"jend_by_{_tk_key2}"
                )
            with _sl2b:
                _months_by = st.slider(
                    "Historique (mois)", min_value=3, max_value=60,
                    value=60, step=3, key=f"months_by_{_tk_key2}"
                )

            # Données depuis result (toujours disponibles)
            _dd = result.get("dates_detail", [])
            _yr = result.get("year_rows", [])

            # Filtrer par mois
            _cutoff_by = pd.Timestamp.now() - pd.DateOffset(months=_months_by)
            _dd_filtered = []
            for _d in _dd:
                try:
                    _dt = pd.Timestamp(_d["date"], dayfirst=True)
                except Exception:
                    _dt = pd.Timestamp(_d["date"])
                if _dt >= _cutoff_by:
                    _dd_filtered.append({**_d, "_dt": _dt})

            # Toujours recalculer depuis le CSV (fenêtre J+jend variable)
            _rows_final = []
            try:
                from ticker_analysis import _find_ticker_csv, load_earnings_dates, detect_engulfing_strict
                _tk_csv = _find_ticker_csv(_tk_name_by)
                if _tk_csv and _tk_csv.exists():
                    _df_full = pd.read_csv(_tk_csv, sep=";")
                    _df_full.columns = [c.strip().lower() for c in _df_full.columns]
                    _df_full["time"] = pd.to_datetime(
                        _df_full["time"].astype(str).str.strip(), errors="coerce")
                    _df_full = _df_full.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
                    for _nc in ["open", "high", "low", "close", "volume"]:
                        if _nc in _df_full.columns:
                            _df_full[_nc] = pd.to_numeric(
                                _df_full[_nc].astype(str).str.replace(",", ".").str.replace(" ", ""),
                                errors="coerce")

                    _td = sorted(_df_full["time"].dt.normalize().unique())
                    _tidx = {d: i for i, d in enumerate(_td)}

                    for _d in _dd_filtered:
                        _sdt = _d["_dt"].normalize()
                        _idx = _tidx.get(_sdt)
                        if _idx is None:
                            continue
                        _cj = _d["close"]
                        _ok = False
                        _best = None
                        for _jj in range(1, _jend_by + 1):
                            if _idx + _jj >= len(_td):
                                break
                            _fr = _df_full[_df_full["time"].dt.normalize() == _td[_idx + _jj]]
                            if len(_fr) == 0:
                                continue
                            _fc = float(_fr.iloc[0]["close"])
                            _fl = float(_fr.iloc[0]["low"]) if "low" in _fr.columns else _fc
                            _fh = float(_fr.iloc[0]["high"]) if "high" in _fr.columns else _fc
                            if _is_bear_by:
                                _cand = min(_fl, _fc)
                                _pf = (_cand - _cj) / _cj * 100
                                if _best is None or _pf < _best:
                                    _best = _pf
                                if _cand < _cj:
                                    _ok = True
                            else:
                                _cand = max(_fh, _fc)
                                _pf = (_cand - _cj) / _cj * 100
                                if _best is None or _pf > _best:
                                    _best = _pf
                                if _cand > _cj:
                                    _ok = True
                        _rows_final.append({
                            "date": _d["date"], "var_j": _d["var_j"],
                            "close": _cj, "success": _ok,
                            "best_move": round(_best, 2) if _best is not None else None,
                            "year": _sdt.year,
                        })
            except Exception as _e:
                st.caption(f"Recalcul J+{_jend_by} indisponible: {_e}")

            if not _rows_final:
                # Fallback si CSV introuvable
                for _d in _dd_filtered:
                    _rows_final.append({
                        "date": _d["date"], "var_j": _d.get("var_j", 0),
                        "close": _d.get("close", 0),
                        "success": _d.get("success", False),
                        "best_move": _d.get("best_move"),
                        "year": _d["_dt"].year,
                    })

            # Year rows
            _year_data = {}
            for _r in _rows_final:
                _y = _r["year"]
                _year_data.setdefault(_y, {"n": 0, "ns": 0})
                _year_data[_y]["n"] += 1
                if _r["success"]:
                    _year_data[_y]["ns"] += 1
            _year_rows_calc = [
                {"Année": y, "Occ.": d["n"], "Succès": d["ns"],
                 "Échecs": d["n"] - d["ns"],
                 "Taux %": round(d["ns"] / d["n"] * 100, 1) if d["n"] > 0 else 0}
                for y, d in sorted(_year_data.items())
            ]

            _n = len(_rows_final)
            _ns = sum(1 for r in _rows_final if r["success"])
            _wr = _ns / _n * 100 if _n > 0 else 0

            # 4 blocs métriques HTML
            def _mhtml(label, value, sub_text, color, bc="#1f2937"):
                return (
                    f"<div style='background:#0d1117;border-radius:12px;padding:20px;"
                    f"border:1px solid {bc};text-align:center;'>"
                    f"<div style='color:#9ca3af;font-size:11px;letter-spacing:2px;"
                    f"text-transform:uppercase;margin-bottom:8px;'>{label}</div>"
                    f"<div style='font-size:32px;font-weight:700;color:{color};'>{value}</div>"
                    f"<div style='color:#6b7280;font-size:12px;margin-top:4px;'>{sub_text}</div>"
                    f"</div>"
                )

            _avg_var = round(sum(r["var_j"] for r in _rows_final) / _n, 2) if _n > 0 else 0
            _avg_bm = round(sum(r.get("best_move") or 0 for r in _rows_final) / _n, 2) if _n > 0 else 0
            _hc1, _hc2, _hc3, _hc4 = st.columns(4)
            _hc1.markdown(_mhtml("OCCURRENCES", str(_n), f"{_months_by} mois", "#06b6d4"), unsafe_allow_html=True)
            _hc2.markdown(_mhtml("TAUX SUCCÈS", f"{_wr:.1f}%", f"{_ns}/{_n}", "#22c55e", bc="#22c55e" if _wr >= 60 else "#1f2937"), unsafe_allow_html=True)
            _hc3.markdown(_mhtml("VAR MOY J", f"{_avg_var:+.2f}%", "jour du signal", "#22c55e" if _avg_var < 0 and _is_bear_by else "#ef4444"), unsafe_allow_html=True)
            _hc4.markdown(_mhtml("BEST MOVE MOY", f"{_avg_bm:+.2f}%", f"J+1..J+{_jend_by}", "#22c55e" if (_avg_bm < 0 and _is_bear_by) or (_avg_bm > 0 and not _is_bear_by) else "#ef4444"), unsafe_allow_html=True)

            # Win rate
            _wrc = "#26a269" if _wr >= 70 else "#f6d32d" if _wr >= 45 else "#e01b24"
            st.markdown(
                f"<div style='background:#1a1a2e;border-radius:10px;"
                f"padding:12px;margin:8px 0;text-align:center;'>"
                f"<span style='color:#888;font-size:13px;'>Win rate J+{_jend_by} — "
                f"{_n} signaux ({_months_by} mois)</span><br>"
                f"<span style='font-size:30px;font-weight:700;color:{_wrc};'>"
                f"{_wr:.0f}%</span> "
                f"<span style='font-size:14px;color:#888;'>({_ns}✅ {_n - _ns}❌)</span>"
                f"</div>", unsafe_allow_html=True
            )

            # Tableau par année
            if _year_rows_calc:
                st.dataframe(pd.DataFrame(_year_rows_calc),
                             use_container_width=True, hide_index=True)
                bar_data = pd.DataFrame(
                    {"Taux %": [r["Taux %"] for r in _year_rows_calc]},
                    index=[str(r["Année"]) for r in _year_rows_calc]
                )
                st.bar_chart(bar_data, height=220, color="#26a269")

            # Scatter
            _valid_sc = [r for r in _rows_final if r.get("best_move") is not None]
            if len(_valid_sc) >= 3:
                sc = pd.DataFrame(index=range(len(_valid_sc)))
                sc["Var J (%)"] = [d["var_j"] for d in _valid_sc]
                sc["Succès"] = [d["best_move"] if d["success"] else None for d in _valid_sc]
                sc["Échec"] = [d["best_move"] if not d["success"] else None for d in _valid_sc]
                sc = sc.set_index("Var J (%)")
                st.scatter_chart(sc, color=["#26a269", "#e01b24"],
                                 height=350, use_container_width=True)

            # Cards
            for _sv in reversed(_rows_final):
                _cb = "#26a269" if _sv["success"] else "#e01b24"
                _vc = "#e01b24" if _sv["var_j"] < 0 else "#26a269"
                _bv = _sv.get("best_move") or 0
                _bc = "#26a269" if (_bv < 0 and _is_bear_by) or (_bv > 0 and not _is_bear_by) else "#e01b24"
                _bm = f"{_bv:+.2f}%" if _bv else "—"
                st.markdown(f"""
<div style="background:#1a1a2e;border-left:4px solid {_cb};
     border-radius:8px;padding:10px 14px;margin:4px 0;">
  <div style="display:flex;justify-content:space-between;">
    <b style="color:#fff;">{_sv['date']}</b>
    <span style="color:{_cb};font-weight:600;">{'✅' if _sv['success'] else '❌'}</span>
  </div>
  <div style="font-size:13px;color:#ccc;margin-top:4px;">
    Close:<b style="color:#fff;"> {_sv['close']:.2f}</b>&nbsp;|&nbsp;
    Var J:<b style="color:{_vc};"> {_sv['var_j']:+.2f}%</b>&nbsp;|&nbsp;
    Best J+1..{_jend_by}:<b style="color:{_bc};"> {_bm}</b>
  </div>
</div>""", unsafe_allow_html=True)

            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

            st.markdown(f"**{result['ticker']}** — Points communs aux {result['n_failures']} échecs")
            corr = result.get("correlations", {})
            if corr:
                df_c = pd.DataFrame([{"Métrique": k, "Valeur": v} for k, v in corr.items()])
                st.dataframe(df_c, use_container_width=True, hide_index=True)
            if result.get("conclusion"):
                st.info(result["conclusion"])
            _add_download_response(result)
            return

        if sub == "text_explanation":
            st.markdown(f"**{result.get('pattern', 'Engulfing')} — Configuration · {result['ticker']}**")
            for ligne in result.get("lignes", []):
                st.markdown(f"— {ligne}")
            return

        # Fallback propre
        st.warning(f"Résultat reçu (type: {sub}) — affichage simplifié")
        for key in ("ticker", "n", "label", "value", "conclusion"):
            if key in result:
                st.write(f"**{key}** : {result[key]}")
        return

    if rtype == "TICKER_ANALYSIS":
        if not result.get("ok"):
            st.error(result.get("error", "Erreur analyse ticker."))
            return

        # BBE handler from ticker_analysis.py returns sub="engulfing_analysis"
        _ta_sub = result.get("sub", "")
        if _ta_sub in ("engulfing_analysis", "engulfing_by_year"):
            # Normalize to sub_type for the INTERPRETED renderer
            result["sub_type"] = _ta_sub
            sub = _ta_sub
            # Jump to the engulfing renderers below (same code as INTERPRETED)
            if sub == "engulfing_analysis":
                _tk_key = result.get('ticker', 'unk').replace('.', '_')
                _pat_r = result.get('pattern', 'bearish')

                _sl1, _sl2 = st.columns(2)
                with _sl1:
                    _jend_r = st.select_slider(
                        "Fenêtre J+",
                        options=[1, 2, 3, 4, 5], value=5,
                        format_func=lambda x: f"J+{x}",
                        key=f"jend_ta_{_tk_key}"
                    )
                with _sl2:
                    _months_r = st.slider(
                        "Historique (mois)",
                        min_value=3, max_value=60, value=36, step=3,
                        key=f"months_ta_{_tk_key}"
                    )

                _all_rows = result.get("rows", [])
                _cutoff_r = pd.Timestamp.now() - pd.DateOffset(months=_months_r)
                _rows_filtered = [
                    r for r in _all_rows
                    if pd.Timestamp(r["date"]) >= _cutoff_r
                ]

                from ticker_analysis import _find_ticker_csv, load_earnings_dates
                _tk_name = result.get('ticker', '')
                _tk_csv = _find_ticker_csv(_tk_name)
                _rows_recalc = []
                if _tk_csv and _tk_csv.exists():
                    _df_r2 = pd.read_csv(_tk_csv, sep=";")
                    _df_r2.columns = [c.strip().lower() for c in _df_r2.columns]
                    _df_r2["time"] = pd.to_datetime(
                        _df_r2["time"].astype(str).str.strip(), errors="coerce")
                    _df_r2 = _df_r2.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
                    for _nc in ["open", "high", "low", "close", "volume"]:
                        if _nc in _df_r2.columns:
                            _df_r2[_nc] = pd.to_numeric(
                                _df_r2[_nc].astype(str).str.replace(",", ".").str.replace(" ", ""),
                                errors="coerce")
                    _td_r = sorted(_df_r2["time"].dt.normalize().unique())
                    _tidx_r = {d: i for i, d in enumerate(_td_r)}

                    for _row in _rows_filtered:
                        _sdt = pd.Timestamp(_row["date"]).normalize()
                        _sidx = _tidx_r.get(_sdt)
                        if _sidx is None:
                            continue
                        _cj = _row["close"]
                        _ok = False
                        _best = None
                        for _jj in range(1, _jend_r + 1):
                            if _sidx + _jj >= len(_td_r):
                                break
                            _fr = _df_r2[_df_r2["time"].dt.normalize() == _td_r[_sidx + _jj]]
                            if len(_fr) == 0:
                                continue
                            _fc = float(_fr.iloc[0]["close"])
                            _fl = float(_fr.iloc[0]["low"]) if "low" in _fr.columns else _fc
                            _fh = float(_fr.iloc[0]["high"]) if "high" in _fr.columns else _fc
                            if _pat_r == "bearish":
                                _cand = min(_fl, _fc)
                                _pf = (_cand - _cj) / _cj * 100
                                if _best is None or _pf < _best:
                                    _best = _pf
                                if _cand < _cj:
                                    _ok = True
                            else:
                                _cand = max(_fh, _fc)
                                _pf = (_cand - _cj) / _cj * 100
                                if _best is None or _pf > _best:
                                    _best = _pf
                                if _cand > _cj:
                                    _ok = True
                        _rows_recalc.append({
                            **_row,
                            "success": _ok,
                            "best_move": round(_best, 2) if _best else None,
                        })

                _n = len(_rows_recalc)
                _ns = sum(1 for r in _rows_recalc if r["success"])
                _wr = _ns / _n * 100 if _n > 0 else 0

                st.markdown(f"**{result['ticker']}** — {_pat_r} engulfing strict")
                _wrc = "#26a269" if _wr >= 70 else "#f6d32d" if _wr >= 45 else "#e01b24"
                st.markdown(
                    f"<div style='background:#1a1a2e;border-radius:10px;"
                    f"padding:12px;margin:8px 0;text-align:center;'>"
                    f"<span style='color:#888;font-size:13px;'>Win rate J+{_jend_r} — "
                    f"{_n} signaux ({_months_r} mois)</span><br>"
                    f"<span style='font-size:30px;font-weight:700;color:{_wrc};'>"
                    f"{_wr:.0f}%</span> "
                    f"<span style='font-size:14px;color:#888;'>({_ns}✅ {_n - _ns}❌)</span>"
                    f"</div>", unsafe_allow_html=True
                )

                for _sv in reversed(_rows_recalc):
                    _cb = "#26a269" if _sv["success"] else "#e01b24"
                    _vc = "#e01b24" if _sv["var_j"] < 0 else "#26a269"
                    _bc_v = _sv.get("best_move") or 0
                    _bc = "#26a269" if (_bc_v < 0 and _pat_r == "bearish") or (_bc_v > 0 and _pat_r != "bearish") else "#e01b24"
                    _bm = f"{_bc_v:+.2f}%" if _bc_v else "—"
                    st.markdown(f"""
<div style="background:#1a1a2e;border-left:4px solid {_cb};
     border-radius:8px;padding:10px 14px;margin:4px 0;">
  <div style="display:flex;justify-content:space-between;">
    <b style="color:#fff;">{_sv['date']}</b>
    <span style="color:{_cb};font-weight:600;">{'✅' if _sv['success'] else '❌'}</span>
  </div>
  <div style="font-size:13px;color:#ccc;margin-top:4px;">
    Close:<b style="color:#fff;"> {_sv['close']:.2f}</b>&nbsp;|&nbsp;
    Var J:<b style="color:{_vc};"> {_sv['var_j']:+.2f}%</b>&nbsp;|&nbsp;
    Best J+1..{_jend_r}:<b style="color:{_bc};"> {_bm}</b>
  </div>
</div>""", unsafe_allow_html=True)

                if result.get("earn_count"):
                    st.caption(f"📅 Earnings ±5j exclus ({result['earn_count']} dates)")
                if result.get("conclusion"):
                    st.info(result["conclusion"])
                return

        ticker = result["ticker"]
        metrics = result.get("metrics", {})
        sub_type = result.get("sub_type", result.get("sub", ""))
        conclusion = result.get("conclusion", "")
        period = metrics.get("period", "")

        st.markdown(f"### {ticker} — Analyse")
        if period:
            st.caption(f"Période : {period}")

        # ── Sub-type : weekday stats ──
        if sub_type == "weekday":
            rows = result.get("weekday_stats", [])
            if rows:
                st.markdown("**Performance par jour de la semaine**")
                df_wd = pd.DataFrame(rows)
                df_wd.columns = ["Jour", "Var moy %", "Var méd %", "Nb séances", "% positif"]
                bar_data = pd.DataFrame(
                    {"Var moy %": [r["var_moy"] for r in rows]},
                    index=[r["jour"] for r in rows])
                st.bar_chart(bar_data, color="#1c71d8", height=250, use_container_width=True)
                st.dataframe(df_wd, use_container_width=True, hide_index=True)
            if conclusion:
                st.info(conclusion)
            return

        # ── Sub-type : monthly stats ──
        if sub_type == "monthly":
            rows = result.get("monthly_stats", [])
            if rows:
                st.markdown("**Performance par mois**")
                df_mo = pd.DataFrame(rows)
                df_mo.columns = ["Mois", "Var moy %", "Var méd %", "Nb séances", "% positif"]
                bar_data = pd.DataFrame(
                    {"Var moy %": [r["var_moy"] for r in rows]},
                    index=[r["mois"] for r in rows])
                st.bar_chart(bar_data, color="#1c71d8", height=250, use_container_width=True)
                st.dataframe(df_mo, use_container_width=True, hide_index=True)
            if conclusion:
                st.info(conclusion)
            return

        # ── Sub-type : count ──
        if sub_type == "count":
            c1, c2 = st.columns(2)
            c1.metric(metrics.get("label", "Jours"), metrics["n"])
            c2.metric("Sur total", f"{metrics.get('pct', 0):.1f}%",
                      delta=f"{metrics.get('total', 0)} séances", delta_color="off")
            if conclusion:
                st.info(conclusion)
            return

        # ── Default : analyse filtrée ──
        next_day = result.get("next_day", {})
        distribution = result.get("distribution", [])
        patterns = result.get("patterns", [])
        n = metrics["n"]

        if n == 0:
            st.warning("Aucune occurrence pour ce critère.")
            return

        # 4 blocs métriques HTML
        _mean_var = metrics.get("mean_var")
        _pct_pos = next_day.get("pct_positive", 0) if next_day else 0
        _mean_next = next_day.get("mean_next", 0) if next_day else 0
        _mc1, _mc2, _mc3, _mc4 = st.columns(4)

        def _metric_html(label, value, sub_text, color, border_color="#1f2937"):
            return (
                f"<div style='background:#0d1117;border-radius:12px;padding:20px;"
                f"border:1px solid {border_color};text-align:center;'>"
                f"<div style='color:#9ca3af;font-size:11px;letter-spacing:2px;"
                f"text-transform:uppercase;margin-bottom:8px;'>{label}</div>"
                f"<div style='font-size:32px;font-weight:700;color:{color};'>{value}</div>"
                f"<div style='color:#6b7280;font-size:12px;margin-top:4px;'>{sub_text}</div>"
                f"</div>"
            )

        _mc1.markdown(_metric_html(
            "OCCURRENCES", str(n),
            f"sur {metrics.get('total', '?')} séances" if metrics.get('total') else "",
            "#06b6d4"
        ), unsafe_allow_html=True)

        _taux = round(metrics.get("pct", 0), 1) if metrics.get("pct") else ""
        _mc2.markdown(_metric_html(
            "VAR MOYENNE", f"{_mean_var:+.2f}%" if _mean_var is not None else "—",
            f"médiane {metrics.get('median_var', 0):+.2f}%" if metrics.get("median_var") is not None else "",
            "#22c55e" if (_mean_var or 0) >= 0 else "#ef4444"
        ), unsafe_allow_html=True)

        _mc3.markdown(_metric_html(
            "VAR MOY J+1", f"{_mean_next:+.2f}%" if next_day else "—",
            f"médiane {next_day.get('median_next', 0):+.2f}%" if next_day else "",
            "#22c55e" if _mean_next >= 0 else "#ef4444"
        ), unsafe_allow_html=True)

        _mc4.markdown(_metric_html(
            "% POSITIF J+1", f"{_pct_pos:.1f}%" if next_day else "—",
            f"{next_day.get('n_positive', '?')}/{next_day.get('n_total', '?')}" if next_day else "",
            "#e5e7eb", border_color="#22c55e" if _pct_pos >= 55 else "#1f2937"
        ), unsafe_allow_html=True)

        if metrics.get("best"):
            _bc1, _bc2 = st.columns(2)
            _bc1.metric("Meilleur jour", f"{metrics['best']['val']:+.2f}%",
                        delta=metrics["best"]["date"], delta_color="off")
            _bc2.metric("Pire jour", f"{metrics['worst']['val']:.2f}%",
                        delta=metrics["worst"]["date"], delta_color="off")

        if next_day:
            st.markdown("---")
            st.markdown("**Lendemain (J+1)**")
            nc1, nc2, nc3 = st.columns(3)
            nc1.metric("% positif J+1", f"{next_day.get('pct_positive', 0):.1f}%")
            nc2.metric("Var. moy. J+1", f"{next_day.get('mean_next', 0):+.2f}%")
            nc3.metric("Var. médiane J+1", f"{next_day.get('median_next', 0):+.2f}%")

        dates_detail = result.get("dates", [])
        if dates_detail and any("next_var" in d for d in dates_detail):
            df_sc = pd.DataFrame(dates_detail)
            if "next_var" in df_sc.columns:
                df_sc = df_sc.dropna(subset=["next_var"])
                if len(df_sc) > 1:
                    scatter = pd.DataFrame(index=range(len(df_sc)))
                    scatter["Var J (%)"] = df_sc["var"].values
                    pos_mask = df_sc["next_var"].values >= 0
                    s_pos = pd.Series(dtype=float, index=scatter.index)
                    s_neg = pd.Series(dtype=float, index=scatter.index)
                    s_pos[pos_mask] = df_sc.loc[df_sc.index[pos_mask], "next_var"].values
                    s_neg[~pos_mask] = df_sc.loc[df_sc.index[~pos_mask], "next_var"].values
                    scatter["Positif J+1"] = s_pos
                    scatter["Négatif J+1"] = s_neg
                    scatter = scatter.set_index("Var J (%)")
                    st.scatter_chart(scatter, color=["#26a269", "#e01b24"],
                                     height=300, use_container_width=True)

        if distribution:
            import altair as alt
            st.markdown("---")
            st.markdown("**Distribution par palier**")
            dist_df = pd.DataFrame(distribution)
            dist_df.columns = ["Palier", "Nb", "Mois dominant", "Jour dominant"]
            chart = alt.Chart(dist_df).mark_bar(color="#1c71d8").encode(
                x=alt.X("Palier:N", sort=None,
                         axis=alt.Axis(labelAngle=-45, labelFontSize=10)),
                y=alt.Y("Nb:Q", title="Occurrences"),
                tooltip=["Palier", "Nb", "Mois dominant", "Jour dominant"],
            ).properties(height=280)
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(dist_df, use_container_width=True, hide_index=True)

        if patterns:
            n_act = sum(1 for p in patterns if p.get("actionnable"))
            n_obs = len(patterns) - n_act
            st.markdown("---")
            n_strong = sum(1 for p in patterns if p.get("taux", 0) >= 80)
            st.markdown(f"**Patterns détectés** ({n_act} actionnables ≥95%, {n_strong - n_act} forts 80-94%, {n_obs - (n_strong - n_act)} tendances 65-79%)")
            rows = []
            for p in patterns:
                tag = ("ACTIONNABLE" if p.get("actionnable")
                       else "fort" if p.get("taux", 0) >= 80 and p.get("oos_valid")
                       else "tendance" if p.get("oos_valid")
                       else "IS only")
                rows.append({
                    "Statut": tag,
                    "Condition": p["label"],
                    "Horizon": p["horizon"],
                    "Direction": p["direction"],
                    "Taux IS": f"{p['taux']}%",
                    "N (IS)": p["n"],
                    "Amp. méd.": f"{p['median_amp']:+.2f}%",
                    "N (OOS)": p.get("n_oos", "—"),
                    "OOS %": f"{p['oos_pct']:.1f}%" if p.get("oos_pct", -1) >= 0 else "—",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        if conclusion:
            st.markdown("---")
            st.info(conclusion)

        st.session_state["ticker_context"] = {
            "ticker": ticker.lower(),
            "dates": [d["date"] for d in dates_detail],
            "last_result": result,
        }
        st.session_state["last_dates"] = [d["date"] for d in dates_detail]
        st.session_state["last_asset"] = ticker.lower()
        return

    if rtype == "C1_LOOKUP":
        if not result["ok"]:
            st.error(result.get("error", "Données introuvables."))
            return
        val = result["value"]
        unit = result.get("unit", "")
        sign = "+" if (unit == "%" and val > 0) else ""
        fmt_val = f"{sign}{val:.2f}{unit}" if unit == "%" else f"{val:,.2f} {unit}"
        st.metric(result["label"], fmt_val)
        return

    if rtype == "C1_EMPTY":
        st.warning(result["msg"])
        return

    if rtype == "C1":
        stats        = result["stats"]
        subject      = result["subject"]
        window_label = result["window_label"]

        st.caption(
            f"{result['n']} jours sur {result['total']} ({result['pct']:.1f}% de l'historique)"
            f"  ·  fenêtre : {window_label}"
        )
        st.markdown("")

        # 5 stat cards (identiques à app.py)
        # Correction #3 : delta du 1er metric = variation en points absolus
        c1, c2, c3, c4, c5 = st.columns(5)
        sign     = "+" if stats["mean_var"] >= 0 else ""
        pts_sign = "+" if stats["mean_pts"] >= 0 else ""
        c1.metric("Variation moy.", f"{sign}{stats['mean_var']:.2f}%",
                  delta=f"{pts_sign}{stats['mean_pts']:.1f} pts", delta_color="off")
        c2.metric("Jours haussiers", f"{stats['pct_bull']:.1f}%")
        c3.metric("Jours baissiers", f"{stats['pct_bear']:.1f}%")
        c4.metric("Meilleur jour", f"+{stats['best_val']:.2f}%",
                  delta=stats["best_date"], delta_color="off")
        c5.metric("Pire jour", f"{stats['worst_val']:.2f}%",
                  delta=stats["worst_date"], delta_color="off")

        st.markdown("---")

        # Bar chart vert/rouge — source : SPX_daily.csv canonique via _HC_SUBJECTS
        # (le registre HC prend toujours priorité sur le dynamique pour "spx")
        var = stats["df"]["var_pct"].copy()
        var.index = var.index.strftime("%Y-%m-%d")
        chart_df = pd.DataFrame({
            "Hausse (%)": var.where(var >= 0),
            "Baisse (%)": var.where(var < 0),
        })
        st.bar_chart(chart_df, color=["#26a269", "#e01b24"],
                     height=300, use_container_width=True)

        # Tableau exportable CSV (inclut var_pts)
        avail_cols = [c for c in ["prev_close", "close", "var_pct", "var_pts"] if c in stats["df"].columns]
        export_df = stats["df"][avail_cols].copy()
        export_df.index = export_df.index.strftime("%Y-%m-%d")
        export_df = export_df.reset_index().rename(columns={"index": "Date", "time": "Date"})
        for col in ["var_pct", "var_pts"]:
            if col in export_df.columns:
                export_df[col] = export_df[col].round(4)
        csv_bytes = export_df.to_csv(index=False).encode("utf-8")
        col_dl, _ = st.columns([1, 4])
        with col_dl:
            st.download_button(
                "Télécharger CSV", data=csv_bytes,
                file_name=f"{subject}_{result.get('cond_str','')[:30].replace(' ','_')}.csv",
                mime="text/csv",
            )
        st.dataframe(export_df, use_container_width=True, height=300)
        return

    if rtype == "C2":
        if result["ok"]:
            df = result["df"]
            if df.empty:
                st.info(result.get("empty_msg", "0 résultat pour ces critères."))
                return
            if len(df) <= 3:
                for _, row in df.iterrows():
                    cols = st.columns(max(1, len(df.columns)))
                    for ci, col_name in enumerate(df.columns):
                        cols[ci].metric(_humanize_col(col_name),
                                        _fmt_c2_val(col_name, row[col_name]))
            else:
                display_df = df.rename(columns={c: _humanize_col(c) for c in df.columns})
                csv_bytes = df.to_csv(index=False).encode("utf-8")
                col_dl, _ = st.columns([1, 4])
                with col_dl:
                    st.download_button(
                        "Télécharger CSV", data=csv_bytes,
                        file_name="resultat_c2.csv", mime="text/csv",
                    )
                st.dataframe(display_df, use_container_width=True)

            # ── Charts enrichis (Point 5) ──────────────────────────────────
            # Colonnes numériques (hors index et labels texte)
            num_cols = [c for c in df.columns
                        if pd.api.types.is_numeric_dtype(df[c])
                        and c not in ("time",)]
            # Série temporelle → line chart
            is_timeseries = "time" in df.columns and len(df) > 5
            # var_pct : bar chart vert/rouge (filtre outliers > 5 std dev)
            if "var_pct" in df.columns and len(df) > 1:
                series = df["var_pct"].copy()
                if len(series) > 10:
                    mu, sigma = series.mean(), series.std()
                    series = series[((series - mu).abs() <= 5 * sigma)]
                if is_timeseries:
                    chart_idx = pd.to_datetime(df.loc[series.index, "time"]).dt.strftime("%Y-%m-%d")
                else:
                    chart_idx = series.index.astype(str)
                chart_data = pd.DataFrame({
                    "Hausse (%)": series.where(series >= 0).values,
                    "Baisse (%)": series.where(series < 0).values,
                }, index=chart_idx)
                st.bar_chart(chart_data, color=["#26a269", "#e01b24"],
                             height=280, use_container_width=True)
            elif is_timeseries and num_cols:
                # Série temporelle sans var_pct → line chart
                chart_df = df.set_index(pd.to_datetime(df["time"]))[num_cols].copy()
                chart_df.index = chart_df.index.strftime("%Y-%m-%d")
                chart_df.columns = [_humanize_col(c) for c in chart_df.columns]
                st.line_chart(chart_df, height=280, use_container_width=True)
            elif num_cols and len(df) > 1:
                # Groupé (jour, mois, etc.) → bar chart bleu
                label_col = next((c for c in df.columns
                                  if c not in num_cols and c != "time"), None)
                chart_num = num_cols[0]
                series = df[chart_num].copy()
                if len(series) > 10:
                    mu, sigma = series.mean(), series.std()
                    series = series[((series - mu).abs() <= 5 * sigma)]
                idx = (df.loc[series.index, label_col].astype(str)
                       if label_col else series.index.astype(str))
                chart_data = pd.DataFrame(
                    {_humanize_col(chart_num): series.values}, index=idx
                )
                st.bar_chart(chart_data, color="#1c71d8", height=280,
                             use_container_width=True)
        else:
            st.warning("Question trop complexe — essayez de la reformuler en plusieurs questions simples.")
        return

    # Fallback (ne devrait pas arriver)
    st.warning("Résultat inconnu.")


# ─── Follow-up query builder (Points 2 & 3) ──────────────────────────────

_FR_MONTHS_RE = re.compile(
    r"\b(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre)\b",
    re.IGNORECASE,
)


_CONTEXT_REF_RE = re.compile(
    r"\bces\s+jours\b|\bparmi\s+ces\s+dates\b|\bsur\s+cette\s+p[eé]riode\b"
    r"|\bmême\s+chose\s+mais\b|\bpareil\s+mais\b",
    re.IGNORECASE,
)


def _build_followup_query(parent_q: str, parent_result: dict, followup: str) -> str:
    """
    Construit une question de suivi intelligente depuis le contexte parent.
    Priorité :
      0. C1_DROP_NEXT + nouveau seuil → passe tel quel (géré par _compute_drop_next_day)
      0b. Référence contextuelle "ces jours" → passe tel quel avec contexte
      1. Follow-up contient une nouvelle année → remplace l'année dans parent_q
      2. Follow-up contient un nouveau mois → remplace le mois dans parent_q
      3. Follow-up contient un nouvel actif seul → remplace l'actif dans parent_q
      4. Fallback : préfixe minimal (actif + condition, sans l'année parente)
    """
    f = followup.strip()

    # ── 0. C1_DROP_NEXT : follow-up avec nouveau seuil ──────────────────
    if parent_result.get("type") == "C1_DROP_NEXT":
        fm = _FOLLOWUP_THRESHOLD_RE.search(f)
        if fm:
            print(f"[followup-context] drop_next threshold passthrough: {f!r}", flush=True)
            return f
        # Référence contextuelle ("ces jours", "parmi ces dates")
        if _CONTEXT_REF_RE.search(f):
            print(f"[followup-context] date-ref passthrough: {f!r}", flush=True)
            return f
    _, eff_subj = get_effective_registries()

    # ── 1. Nouveau year dans le follow-up ─────────────────────────────────
    new_year_m = re.search(r"\b(20\d{2})\b", f)
    parent_has_year = bool(re.search(r"\b20\d{2}\b", parent_q))
    if new_year_m:
        if parent_has_year:
            # Remplace l'année dans la question parente → garde toute la structure C1
            result = re.sub(r"\b20\d{2}\b", new_year_m.group(1), parent_q)
            print(f"[followup] year-replace: {result!r}", flush=True)
            return result
        else:
            result = f"{parent_q.rstrip('?').rstrip()} en {new_year_m.group(1)}"
            print(f"[followup] year-append: {result!r}", flush=True)
            return result

    # ── 2. Nouveau mois dans le follow-up ────────────────────────────────
    new_month_m = _FR_MONTHS_RE.search(f)
    parent_month_m = _FR_MONTHS_RE.search(parent_q)
    if new_month_m and parent_month_m:
        result = (parent_q[:parent_month_m.start()]
                  + new_month_m.group(0)
                  + parent_q[parent_month_m.end():])
        print(f"[followup] month-replace: {result!r}", flush=True)
        return result

    # ── 3. Nouvel actif seul dans le follow-up ───────────────────────────
    parent_asset = None
    for s in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", parent_q, re.IGNORECASE):
            parent_asset = s
            break
    followup_asset = None
    for s in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", f, re.IGNORECASE):
            followup_asset = s
            break
    if followup_asset and parent_asset and followup_asset != parent_asset:
        # Vérifie que le follow-up ne dit que l'actif (+ mots de liaison)
        leftover = re.sub(rf"\b{re.escape(followup_asset)}\b", "", f, flags=re.IGNORECASE)
        leftover = re.sub(r"\b(et|pour|avec|aussi|même chose|idem|pareil)\b", "", leftover, flags=re.IGNORECASE)
        leftover = re.sub(r"[?!\s]+", "", leftover)
        if not leftover:
            result = re.sub(rf"\b{re.escape(parent_asset)}\b", followup_asset.upper(),
                            parent_q, flags=re.IGNORECASE)
            print(f"[followup] asset-replace: {result!r}", flush=True)
            return result

    # ── 4. Fallback : préfixe minimal sans l'année parente ───────────────
    prefix_parts = []
    if parent_result.get("type") == "C1" and parent_result.get("subject"):
        prefix_parts.append(parent_result["subject"].upper())
    km = re.search(r"(quand\s+\w+[\s><=!]+[\d,\.]+)", parent_q, re.IGNORECASE)
    if km:
        prefix_parts.append(km.group(1))
    prefix = (" ".join(prefix_parts) + " — ") if prefix_parts else ""
    result = f"{prefix}{f}"
    print(f"[followup] fallback prefix: {result!r}", flush=True)
    return result


# ─── Streamlit UI ─────────────────────────────────────────────────────────

def _human_readable_feature(feat: str) -> str:
    """Traduit un nom de feature technique en texte lisible."""
    replacements = [
        # Composites & ratios
        ("vix1d_vix_ratio", "Ratio VIX1j/VIX"),
        ("ratio_vix9d_vix3m", "Ratio VIX9j/VIX3m"),
        ("ratio_vix9d_vix", "Ratio VIX9j/VIX"),
        ("ratio_skew_vix", "Ratio SKEW/VIX"),
        ("vts_vix9d_vix3m", "VTS VIX9j/VIX3m"),
        ("vts_vix9d_vix6m", "VTS VIX9j/VIX6m"),
        ("vts_vix9d_vix", "VTS VIX9j/VIX"),
        ("vts_vix_vix3m", "VTS VIX/VIX3m"),
        ("vts_vix_vix6m", "VTS VIX/VIX6m"),
        ("vts_vix3m_vix6m", "VTS VIX3m/VIX6m"),
        ("vts_vvix_vix", "VTS VVIX/VIX"),
        ("vts_skew_vix", "VTS SKEW/VIX"),
        ("vts_composite_stress", "Stress vol composite"),
        ("interact_vvix_vix9d", "Interaction VVIX×VIX9j"),
        ("interact_vix_skew", "Interaction VIX×SKEW"),
        ("pcf_spx_pc_x_vix", "P/C SPX × VIX"),
        ("pcf_composite", "P/C composite"),
        ("pcf_convergence_high", "Convergence P/C"),
        ("pcf_double_fear", "Double peur P/C×VIX"),
        ("div_triple_high_dispersion", "Dispersion triple haute"),
        ("div_triple_all_down", "Triple baisse SPX/IWM/QQQ"),
        ("div_spx_bonds_flight", "Flight to quality SPX→Bonds"),
        ("div_nikkei_crash", "Nikkei crash (<-2%)"),
        ("div_nikkei_spx", "Divergence Nikkei/SPX"),
        ("div_spx_iwm", "Divergence SPX/IWM"),
        ("div_spx_gold", "Divergence SPX/Or"),
        ("div_qqq_spx", "Divergence QQQ/SPX"),
        ("div_spx_dax", "Divergence SPX/DAX"),
        ("refuge_ftq_all", "Flight to quality complet"),
        ("refuge_gold_dxy", "Ratio Or/Dollar refuge"),
        ("refuge_composite", "Score refuge composite"),
        ("cfm_spx_or30_coherent", "Cohérence SPX daily×OR30"),
        ("cfm_spx_all_aligned", "SPX momentum aligné"),
        ("cfm_vix_quad", "Quadrant VIX (mom×niveau)"),
        ("cfm_vix_spike_from_low", "VIX spike depuis bas"),
        ("cfm_vix_crush_from_high", "VIX crush depuis haut"),
        ("regime_stress", "Régime stress"),
        ("regime_vol", "Régime volatilité"),
        ("regime_spx", "Régime SPX momentum"),
        ("regime_calm", "Régime calme"),
        ("advance_decline_ratio_net_ratio_put_call", "A/D ratio × P/C net"),
        ("advance_decline_ratio", "Ratio Advance/Decline"),
        ("ad_above_ma10", "Breadth au-dessus MA10"),
        # Assets prefixes
        ("vix_spx_open_plus_30", "VIX@10h"),
        ("vix_put_call_ratio", "VIX P/C"),
        ("vix9d", "VIX9j"),
        ("vix6m", "VIX6m"),
        ("vix3m", "VIX3m"),
        ("vvix", "VVIX"),
        ("skew", "SKEW"),
        ("vix_open_j", "VIX open J"),
        ("vix_open", "VIX open"),
        ("vix_close", "VIX clôture"),
        ("vix_high", "VIX haut"),
        ("vix_low", "VIX bas"),
        ("spx_williams_vix_fix", "SPX Williams VIX Fix"),
        ("spx_iv_rank", "SPX IV Rank"),
        ("spx_iv_percentile", "SPX IV Percentile"),
        ("spx_put_call_ratio", "SPX P/C"),
        ("spx_dist_ma", "SPX écart MA"),
        ("spx_dist_ath", "SPX écart ATH"),
        ("spx_in_drawdown", "SPX en drawdown"),
        ("spx_recovering", "SPX en rebond"),
        ("spx_strong_bear", "SPX forte baisse"),
        ("spx_strong_bull", "SPX forte hausse"),
        ("spx_streak", "SPX streak"),
        ("spx_rvol", "SPX vol réalisée"),
        ("spx_above_ma", "SPX au-dessus MA"),
        ("spx_rsi_based_ma", "SPX RSI MA"),
        ("spx_rsi", "SPX RSI"),
        ("spx_plot", "SPX Williams Plot"),
        ("spy_put_call_ratio", "SPY P/C"),
        ("spy_rsi", "SPY RSI"),
        ("spy_930", "SPY barre 9h30"),
        ("spy", "SPY"),
        ("iwm_put_call_ratio", "IWM P/C"),
        ("iwm_micro", "IWM micro"),
        ("iwm", "IWM"),
        ("qqq_put_call_ratio", "QQQ P/C"),
        ("qqq", "QQQ"),
        ("equity_put_call_ratio", "Equity P/C"),
        ("equity_put_call_rati", "Equity P/C"),
        ("equity_pc", "Equity P/C"),
        ("gold_micro", "Or micro"),
        ("gold", "Or"),
        ("dxy_micro", "DXY micro"),
        ("dxy", "DXY"),
        ("dax40", "DAX40"),
        ("nikkei225", "Nikkei225"),
        ("ftse100", "FTSE100"),
        ("us_10_years_bonds", "Oblig 10 ans"),
        ("us_bonds_30_days_con", "T-Bonds 30j"),
        ("yield_curve", "Courbe taux"),
        # Intraday J-1
        ("spx5_jm1_amplitude", "SPX5min J-1 amplitude"),
        ("spx5_jm1_close_pos", "SPX5min J-1 position clôture"),
        ("spx5_jm1_close_top_third", "SPX5min J-1 clôture tiers sup"),
        ("spx5_jm1_close_bottom_third", "SPX5min J-1 clôture tiers inf"),
        ("spx5_jm1_last30_ret", "SPX5min J-1 rendement 30min finales"),
        ("spx5_jm1_first60_ret", "SPX5min J-1 rendement 1ère heure"),
        ("spx5_jm1_or30", "SPX5min J-1 OR30"),
        ("spx5_jm1_rsi_close", "SPX5min J-1 RSI"),
        ("spx5_jm1_wvf_close", "SPX5min J-1 Williams VIX Fix"),
        ("fut_jm1_ov_ret", "Futures J-1 retour overnight"),
        ("fut_jm1_ov_range", "Futures J-1 range overnight"),
        ("fut_jm1_premkt_ret", "Futures J-1 retour pré-market"),
        ("fut_jm1_ov_vol", "Futures J-1 volume overnight"),
        ("fut_jm1_gap", "Futures J-1 gap"),
        ("fut_jm1_large_overnight", "Futures J-1 grande nuit"),
        ("spy30_jm1_vol_ratio", "SPY30min J-1 ratio volume"),
        ("spy30_jm1_close_vs_vwap", "SPY30min J-1 vs VWAP"),
        ("spy30_jm1_bb_pos", "SPY30min J-1 Bollinger"),
        # Intraday J
        ("or30", "OR30"),
        ("fut_overnight", "Futures overnight"),
        ("bar_1000", "Barre 10h00"),
        # Calendar
        ("cal_days_since_opex", "Jours depuis OpEx"),
        ("cal_days_to_opex", "Jours jusqu'à OpEx"),
        ("cal_is_opex", "Jour OpEx"),
        ("cal_is_fomc", "Jour FOMC"),
        ("cal_is_nfp", "Jour NFP"),
        ("cal_is_cpi", "Jour CPI"),
        ("cal_n_high_events", "Nb événements high impact"),
        ("cal_n_medium_events", "Nb événements medium"),
        ("cal_macro_surprise", "Surprise macro"),
        # Temporal
        ("day_of_week", "Jour de la semaine"),
        ("month", "Mois"),
        ("week_of_year", "Semaine"),
        ("is_monday", "Lundi"),
        ("is_friday", "Vendredi"),
        ("is_month_start", "Début de mois"),
        ("is_month_end", "Fin de mois"),
        ("vix_regime", "Régime VIX"),
        ("vix_compression", "VIX compression"),
        ("vix_expansion", "VIX expansion"),
        ("vix_extreme", "VIX extrême"),
        ("vix_spike", "VIX spike"),
        ("vix_crush", "VIX crush"),
        ("vix_ts_inverted", "VIX structure inversée"),
        ("vix_ts_deeply", "VIX fortement inversé"),
        ("vix_ts_inversion_new", "Nouvelle inversion VIX"),
        ("vix_ts_spread_accel", "Accélération spread VIX"),
        ("vix_cross_above", "VIX franchit à la hausse"),
        ("vix_cross_below", "VIX franchit à la baisse"),
        ("skew_above", "SKEW au-dessus"),
        ("skew_cross", "SKEW croisement"),
    ]

    f = feat
    for old, new in replacements:
        if old in f:
            f = f.replace(old, new)
            break

    # Suffixes courants
    f = f.replace("_z20", " (z20)").replace("_z60", " (z60)")
    f = f.replace("_lag2", " lag2j").replace("_lag3", " lag3j")
    f = f.replace("_lag5", " lag5j").replace("_lag10", " lag10j")
    f = f.replace("_mom1d", " mom1j").replace("_mom3d", " mom3j")
    f = f.replace("_mom5d", " mom5j").replace("_mom10d", " mom10j")
    f = f.replace("_mom20d", " mom20j")
    f = f.replace("_pct252", " pct252j")
    f = f.replace("_accel5", " accél5j")
    f = f.replace("_distma20", " écart MA20").replace("_distma50", " écart MA50")
    f = f.replace("_vol5", " vol5j").replace("_vol10", " vol10j")
    f = f.replace("_vol20", " vol20j")
    f = f.replace("_diff_1_3", " diff J-1/J-3").replace("_diff_1_5", " diff J-1/J-5")
    f = f.replace("_diff_3_10", " diff J-3/J-10").replace("_diff_5_10", " diff J-5/J-10")
    f = f.replace("_streak_bull", " streak haussier").replace("_streak_bear", " streak baissier")
    f = f.replace("_cross_ma5_up", " croisement MA5↑")
    f = f.replace("_cross_ma5_down", " croisement MA5↓")
    f = f.replace("_cross_ma20_up", " croisement MA20↑")
    f = f.replace("_cross_ma20_down", " croisement MA20↓")
    f = f.replace("_breakout_up", " breakout↑").replace("_breakout_down", " breakout↓")
    f = f.replace("_body_pct", " corps bougie %")
    f = f.replace("_body_dir", " direction bougie")
    f = f.replace("_close_position", " position clôture")
    f = f.replace("_true_range", " true range").replace("_atr", " ATR")
    f = f.replace("_body_ratio", " ratio corps").replace("_is_doji", " doji")
    f = f.replace("_gap_pct", " gap %").replace("_gap_up", " gap↑").replace("_gap_down", " gap↓")
    f = f.replace("_upper_shadow", " mèche haute").replace("_lower_shadow", " mèche basse")
    f = f.replace("_bull_candles", " bougies haussières").replace("_bear_candles", " bougies baissières")
    f = f.replace("_effort_result", " effort×résultat")
    f = f.replace("_spread_accel3", " spread accél3j")
    f = f.replace("_inverted", " inversé").replace("_inversion_new", " inversion nouvelle")
    f = f.replace("_ratio_z", " ratio z").replace("_ratio", " ratio")
    f = f.replace("_spread", " spread")
    f = f.replace("_open", " open").replace("_close", " close")
    f = f.replace("_high", " high").replace("_low", " low")
    f = f.replace("_rsi_based_ma", " RSI MA").replace("_rsi", " RSI")
    f = f.replace("_volume", " volume")
    f = f.replace("_micro_", " micro ").replace("_micro", " micro")
    f = f.replace("_", " ").strip()
    return f[:65]


def _streamlit_app():
    import streamlit as st

    st.set_page_config(page_title="SPX Quant Engine", layout="wide")

    # CSS pour sliders gris (st.markdown fonctionne pour les éléments internes)
    st.markdown("""
<style>
div[data-testid="stSlider"] > div > div > div > div {
    background: #6b7280 !important;
}
div[data-testid="stSlider"] > div > div > div > div > div {
    background: #374151 !important;
}
</style>
""", unsafe_allow_html=True)

    # JS injection via components.html — bypasse le shadow DOM Streamlit
    import streamlit.components.v1 as _stc
    _stc.html("""
<script>
(function() {
    function hideElements() {
        var selectors = [
            '[data-testid="stToolbar"]',
            '[data-testid="stAppDeployButton"]',
            '[data-testid="stMainMenu"]',
            '[data-testid="stMainMenuButton"]',
            '[data-testid="stHeader"]',
            '[data-testid="stDecoration"]',
            '[data-testid="stStatusWidget"]'
        ];
        var parent = window.parent.document;
        selectors.forEach(function(sel) {
            var els = parent.querySelectorAll(sel);
            els.forEach(function(el) { el.style.display = 'none'; });
        });
        // Aussi masquer par tag
        var headers = parent.querySelectorAll('header');
        headers.forEach(function(h) { h.style.display = 'none'; });
        var footers = parent.querySelectorAll('footer');
        footers.forEach(function(f) { f.style.display = 'none'; });
        // Remonter le contenu
        var container = parent.querySelector('[data-testid="stMainBlockContainer"]');
        if (container) { container.style.paddingTop = '0.5rem'; }
        var blockContainer = parent.querySelector('.block-container');
        if (blockContainer) { blockContainer.style.paddingTop = '0.5rem'; }
        // Réduire taille tableaux
        var tables = parent.querySelectorAll('[data-testid="stDataFrame"] *');
        tables.forEach(function(el) { el.style.fontSize = '0.8rem'; });
    }
    hideElements();
    setTimeout(hideElements, 500);
    setTimeout(hideElements, 1500);
    setTimeout(hideElements, 3000);
    var obs = new MutationObserver(hideElements);
    obs.observe(window.parent.document.body, {childList:true, subtree:true});
})();
</script>
""", height=0)

    st.title(f"SPX Quant Engine {VERSION_LOCAL}")

    @st.cache_data(ttl=300)
    def _cached_load_csv(path_str, sep=";"):
        """Cache les lectures CSV lourdes (VIX, SPX daily)."""
        p = Path(path_str)
        if not p.exists():
            return None
        df = pd.read_csv(p, sep=sep)
        df.columns = [c.strip().lower() for c in df.columns]
        df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
        df = df.dropna(subset=["time"]).sort_values("time")
        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c].astype(str).str.replace(",", "."), errors="coerce")
        return df

    # ── Correction #4 : alerte tokens > 80% (variable env SPX_TOKEN_WARNING ou flag fichier)
    if _check_token_warning():
        st.warning("Tokens > 80% — contexte proche de la limite. Les réponses peuvent être tronquées.")

    # ── Correction #1 : historique persistant — chargé une seule fois au démarrage
    if "history_loaded" not in st.session_state:
        st.session_state.history = _load_history()
        st.session_state.history_loaded = True
        st.session_state.active_idx = len(st.session_state.history) - 1

    if "be_seuil" not in st.session_state:
        st.session_state["be_seuil"] = 2.0
    if "bull_seuil" not in st.session_state:
        st.session_state["bull_seuil"] = 2.0
    if "active_idx" not in st.session_state:
        st.session_state.active_idx = -1

    # ── Polling fichier flag patterns (vérifié à chaque rerun)
    if PATTERNS_FLAG_FILE.exists() and not st.session_state.get("patterns_ready"):
        import json as _j2
        try:
            flag_data = _j2.loads(PATTERNS_FLAG_FILE.read_text())
            st.session_state["patterns_ready"] = True
            st.session_state["patterns_count"] = flag_data.get("n_patterns", "?")
            st.session_state["patterns_running"] = False
            PATTERNS_FLAG_FILE.unlink()
            st.rerun()
        except Exception:
            pass
    # Si exploration en cours et flag pas encore là → rerun automatique après 3s
    elif st.session_state.get("patterns_running") and not st.session_state.get("patterns_ready"):
        time.sleep(3)
        st.rerun()

        # ── Sidebar : notification patterns prêts
    with st.sidebar:
        if st.session_state.get("patterns_ready"):
            n_pat = st.session_state.get("patterns_count", "?")
            if st.button(f"📊 Voir {n_pat} patterns trouvés", key="btn_patterns",
                         use_container_width=True):
                st.session_state["patterns_ready"] = False
                import json as _json
                pat_file = BASE_DIR / "data" / "patterns_results.json"
                if pat_file.exists():
                    data = _json.loads(pat_file.read_text())
                    st.session_state["_pending_q"] = f"Patterns ({n_pat} trouvés)"
                    st.session_state["_pending_result"] = {
                        "type": "PATTERNS_RESULTS", "ok": True, "data": data,
                    }
                st.rerun()

    # ── Sidebar : historique 20 dernières questions cliquables
    with st.sidebar:
        st.markdown("### Seuil Engulfing")

        # Bearish E
        st.caption("Bearish E (baisse cible)")
        _bear_opts = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
        _bear_labels = [f"-{s}%" for s in _bear_opts] + ["Perso."]
        _cur_bear = st.session_state.get("be_seuil", 2.0)
        _def_bear = _bear_opts.index(_cur_bear) if _cur_bear in _bear_opts else len(_bear_opts)
        _sel_bear = st.selectbox("Bearish seuil", _bear_labels, index=_def_bear,
                                  key="be_seuil_select", label_visibility="collapsed")
        if _sel_bear == "Perso.":
            _pb = st.text_input("Perso. bearish", value=str(_cur_bear) if _cur_bear not in _bear_opts else "2.0",
                                key="be_seuil_perso", placeholder="ex: 1.7", label_visibility="collapsed")
            try:
                _v = abs(float(_pb.replace(",", ".").replace("-", "").strip()))
                if 0.1 <= _v <= 20.0:
                    st.session_state["be_seuil"] = _v
            except ValueError:
                pass
        else:
            st.session_state["be_seuil"] = float(_sel_bear.replace("-", "").replace("%", ""))

        # Bullish E
        st.caption("Bullish E (hausse cible)")
        _bull_opts = [1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
        _bull_labels = [f"+{s}%" for s in _bull_opts] + ["Perso."]
        _cur_bull = st.session_state.get("bull_seuil", 2.0)
        _def_bull = _bull_opts.index(_cur_bull) if _cur_bull in _bull_opts else len(_bull_opts)
        _sel_bull = st.selectbox("Bullish seuil", _bull_labels, index=_def_bull,
                                  key="bull_seuil_select", label_visibility="collapsed")
        if _sel_bull == "Perso.":
            _pb2 = st.text_input("Perso. bullish", value=str(_cur_bull) if _cur_bull not in _bull_opts else "2.0",
                                 key="bull_seuil_perso", placeholder="ex: 1.7", label_visibility="collapsed")
            try:
                _v2 = abs(float(_pb2.replace(",", ".").replace("+", "").strip()))
                if 0.1 <= _v2 <= 20.0:
                    st.session_state["bull_seuil"] = _v2
            except ValueError:
                pass
        else:
            st.session_state["bull_seuil"] = float(_sel_bull.replace("+", "").replace("%", ""))

        st.caption(f"Actif : bearish -{st.session_state['be_seuil']}% / bullish +{st.session_state['bull_seuil']}%")

        if st.button("↺ Appliquer", key="apply_seuil", use_container_width=True):
            _active_idx = st.session_state.get("active_idx", -1)
            _hist = st.session_state.get("history", [])
            if 0 <= _active_idx < len(_hist):
                _active_q = _hist[_active_idx]["q"]
                with st.spinner("Recalcul…"):
                    _new_result = _compute_result(_active_q, session_state=st.session_state)
                st.session_state["history"][_active_idx]["result"] = _new_result
                _save_history(st.session_state["history"])
                st.rerun()

        st.markdown("---")
        st.markdown("### Historique")
        if not st.session_state.history:
            st.caption("Aucune question posée.")
        else:
            for i, item in enumerate(reversed(st.session_state.history[-20:])):
                real_i = len(st.session_state.history) - 1 - i
                label = item["q"][:50] + ("…" if len(item["q"]) > 50 else "")
                col_q, col_del = st.columns([5, 1])
                with col_q:
                    if st.button(label, key=f"h{real_i}", use_container_width=True):
                        st.session_state.active_idx = real_i
                        st.rerun()
                with col_del:
                    if st.button("×", key=f"del{real_i}"):
                        st.session_state.history.pop(real_i)
                        if st.session_state.active_idx >= len(st.session_state.history):
                            st.session_state.active_idx = len(st.session_state.history) - 1
                        _save_history(st.session_state.history)
                        st.rerun()
            st.markdown("---")
            if st.button("Effacer tout", key="clear_all", use_container_width=True):
                st.session_state.history = []
                st.session_state.active_idx = -1
                _save_history([])
                st.rerun()

        # ── Status permanent sidebar ──
        st.markdown("---")
        _token_warn = _check_token_warning()
        _status_icon = "🔴" if _token_warn else "🟢"
        st.caption(
            f"{_status_icon} {VERSION_LOCAL} | "
            f"{'⚠ Tokens > 80%' if _token_warn else 'Tokens OK'}"
        )

    # ── Main

    with st.expander("🔬 ML · Patterns SPX Edge", expanded=False):
        import json as _json_ml
        from pathlib import Path as _Path_ml

        _DATA_ML = BASE_DIR / "data"
        _tab_overview, _tab_ric, _tab_ric_p1, _tab_ic, _tab_grid, _tab_options, _tab_val2025, _tab_today = st.tabs([
            "Vue d'ensemble",
            "RIC — Tous régimes",
            "RIC — VIX ≤ 19",
            "IC — VIX ≤ 19",
            "🎯 Grille RIC",
            "💰 Options",
            "📅 Validation 2025",
            "Signal aujourd'hui"
        ])

        # ── Sélecteurs ──────────────────────────────────────
        _col_ep2, _col_hz2 = st.columns(2)
        with _col_ep2:
            _ep2 = st.selectbox("Point d'entrée",
                                 ["9h30", "10h00", "10h30"],
                                 key="ml_ep2")
        with _col_hz2:
            _hz2 = st.selectbox("Horizon",
                                 ["360min", "240min", "180min"],
                                 key="ml_hz2")

        def _load_pat(fname):
            p = _DATA_ML / fname
            if p.exists():
                with open(p) as _f:
                    return _json_ml.load(_f)
            return None

        def _show_patterns(pat_data, label=""):
            if not pat_data:
                st.info(f"Aucune donnée disponible pour {label}.")
                return
            _m1, _m2, _m3, _m4 = st.columns(4)
            _m1.metric("Sessions", pat_data.get("n_sessions", "?"))
            _m2.metric("Actionnables", pat_data.get("n_actionable", "?"))
            _m3.metric("Robustes WF", pat_data.get("n_robust", "?"))
            _m4.metric("Features", pat_data.get("n_features", "?"))
            st.markdown("---")
            _all_pats = pat_data.get("all_patterns", [])
            # Trier : 100% OOS en premier, puis robuste, puis OOS%, puis occ
            _all_pats_sorted = sorted(
                _all_pats,
                key=lambda p: (
                    1 if p.get("precision_oos", 0) == 100.0 else 0,
                    1 if p.get("is_robust") else 0,
                    p.get("precision_oos", 0),
                    p.get("n_oos", 0)
                ),
                reverse=True
            )
            _robust = [p for p in _all_pats_sorted if p.get("is_robust")]
            if not _robust:
                _robust = _all_pats_sorted[:10]
            else:
                _robust = _robust[:20]
            st.markdown(f"**{len(_robust)} patterns robustes**")
            for _i2, _pat2 in enumerate(_robust[:20]):
                _po2 = _pat2.get("precision_oos", 0)
                _col2 = "🟢" if _po2 >= 90 else "🟡" if _po2 >= 82 else "🔴"
                _feats2 = " + ".join(
                    _human_readable_feature(f)
                    for f in _pat2.get("features", [])
                )
                _wf_v = _pat2.get("n_windows_valid", 0)
                _wf_t = _pat2.get("n_windows_tested", 0)
                _rob_flag = "✓ ROBUSTE" if _pat2.get("is_robust") else ""
                with st.expander(
                    f"{_col2} #{_i2+1} {_rob_flag} | OOS={_po2:.1f}% | "
                    f"~{_pat2.get('per_quarter_oos',0):.1f}/trim | "
                    f"WF={_wf_v}/{_wf_t}",
                    expanded=(_i2 < 2)
                ):
                    _c1, _c2, _c3, _c4 = st.columns(4)
                    _c1.metric("Précision IS", f"{_pat2.get('precision_is',0):.1f}%")
                    _c2.metric("Précision OOS", f"{_po2:.1f}%")
                    _c3.metric("Occurrences OOS", _pat2.get("n_oos", 0))
                    _c4.metric("Walk-Forward", f"{_wf_v}/{_wf_t}")
                    st.markdown("**Conditions (données clôture J-1) :**")
                    for _cond2 in _pat2.get("conditions", []):
                        _sym2 = "≥" if _cond2["direction"] == "above" else "≤"
                        _pct2 = _cond2.get("percentile",
                                            _cond2.get("percentile_is", "?"))
                        _hr = _human_readable_feature(_cond2["feature"])
                        st.markdown(
                            f"• **{_hr}** {_sym2} `{_cond2['threshold']}` "
                            f"*(percentile {_pct2} sur données d'apprentissage)*"
                        )
                    _wf2 = _pat2.get("precisions_by_window", [])
                    if _wf2:
                        _wf_str = " | ".join(
                            f"{_p2:.0f}%" if _p2 is not None else "n/a"
                            for _p2 in _wf2
                        )
                        st.caption(f"Précision par fenêtre temporelle : {_wf_str}")

        # ── Tab 1 : Vue d'ensemble ───────────────────────────
        with _tab_overview:
            st.markdown("### Comment lire ces résultats ?")
            st.info(
                "**IS (In-Sample)** — 70% des données historiques utilisées "
                "pour *apprendre* les patterns (2023–fin 2025 environ). "
                "La précision IS mesure si le pattern existe dans le passé.\n\n"
                "**OOS (Out-of-Sample)** — 30% des données *jamais vues* "
                "pendant l'apprentissage, utilisées pour *valider* les patterns "
                "(début 2025–février 2026). C'est le vrai test. "
                "Un pattern à 94% OOS signifie que sur 17 jours de test, "
                "15 ou 16 ont effectivement produit une amplitude ≥ 0.45%.\n\n"
                "**Walk-Forward (WF=3/3)** — Le pattern a tenu sur 3 fenêtres "
                "temporelles indépendantes testées séparément. "
                "C'est la preuve de stabilité dans le temps.\n\n"
                "**RIC** — Stratégie qui vise une amplitude ≥ 0.45% "
                "(le SPX bouge d'au moins 0.45% depuis l'entrée).\n\n"
                "**IC (Iron Condor)** — Stratégie qui vise une amplitude ≤ 0.23% "
                "(le SPX reste dans une plage étroite). "
                "Requiert VIX ≤ 19 pour que la prime soit suffisante."
            )
            st.markdown("---")
            st.markdown("### Patterns prioritaires actuels")
            _priority_pats = [
                {
                    "title": "Pattern A — RIC / 9h30 / Tous régimes VIX",
                    "oos": 100.0, "occ": 11, "trim": 2.9, "wf": "4/6",
                    "lift": "44% → 100%",
                    "conditions": [
                        ("Ratio VIX9j / VIX3m (tension court terme vs moyen terme)",
                         "≥", "0.9904", "P90",
                         "Le VIX très court terme est aussi élevé ou plus élevé que le VIX 3 mois — structure de termes inversée"),
                        ("Ratio Advance/Decline ajusté put/call (plus haut) en écart-type 60j",
                         "≤", "-0.33", "P50",
                         "La breadth du marché est dégradée par rapport à sa normale sur 60 jours"),
                    ]
                },
                {
                    "title": "Pattern B — RIC / 9h30 / Tous régimes VIX",
                    "oos": 100.0, "occ": 10, "trim": 2.6, "wf": "3/4",
                    "lift": "38% → 100%",
                    "conditions": [
                        ("Open VIX en percentile sur 1 an",
                         "≥", "87.7", "P90",
                         "Le VIX à l'ouverture est dans son top 12% sur l'année écoulée"),
                        ("Ratio VIX1j/VIX (ouverture) en écart-type sur 60j",
                         "≥", "1.07", "P90",
                         "Le VIX très court terme est anormalement élevé par rapport au VIX standard"),
                    ]
                },
                {
                    "title": "Pattern C — RIC / 9h30 / VIX 19-22",
                    "oos": 83.3, "occ": 12, "trim": 12.0, "wf": "6/8",
                    "lift": "60% → 83%",
                    "conditions": [
                        ("Variation 5j du RSI put/call actions",
                         "≥", "4.22", "P70",
                         "Le RSI du put/call sur les actions individuelles est en hausse depuis 5 jours — signal de couverture croissant"),
                    ]
                },
                {
                    "title": "Pattern D — RIC / 10h00 / Tous régimes VIX",
                    "oos": 100.0, "occ": 9, "trim": 2.4, "wf": "3/4",
                    "lift": "29% → 100%",
                    "conditions": [
                        ("Variation du plus bas SPX sur 5 jours",
                         "≤", "-2.12%", "P10",
                         "Le SPX a atteint des plus bas nettement inférieurs à il y a 5 jours"),
                        ("Variation du plus bas VIX 3 mois sur 10 jours",
                         "≥", "+12.97%", "P90",
                         "Le VIX 3 mois monte fortement depuis 10 jours — stress moyen terme en hausse"),
                        ("VIX plus haut J-1",
                         "≥", "22.04", "P90",
                         "Le VIX a touché un niveau élevé hier — confirmation du stress"),
                    ]
                },
                {
                    "title": "Pattern IC-E — IC / 10h00 / VIX ≤ 19",
                    "oos": 87.5, "occ": 8, "trim": 2.9, "wf": "4/6",
                    "lift": "38% → 88%",
                    "conditions": [
                        ("Variation 3j du RSI put/call SPX",
                         "≤", "-5.24", "P30",
                         "Le put/call SPX décélère — les traders cessent de se couvrir"),
                        ("Interaction VVIX × VIX9j en écart-type",
                         "≤", "-1.04", "P20",
                         "Le stress combiné volatilité-de-la-volatilité × VIX court terme est faible — marché serein"),
                    ]
                },
            ]
            for _pp in _priority_pats:
                _color = "🟢" if _pp["oos"] >= 93 else "🟡"
                with st.expander(
                    f"{_color} {_pp['title']} | OOS={_pp['oos']:.1f}% | "
                    f"{_pp['occ']} occ | ~{_pp['trim']:.1f}/trim | WF={_pp['wf']}",
                    expanded=True
                ):
                    st.caption(f"Lift probabiliste : {_pp['lift']}")
                    for (_feat, _sym, _val, _pct, _explication) in _pp["conditions"]:
                        st.markdown(
                            f"• **{_feat}** {_sym} `{_val}` "
                            f"*(percentile {_pct})* — {_explication}"
                        )

        # ── Tab 2 : RIC Tous régimes ─────────────────────────
        with _tab_ric:
            st.markdown("### Patterns RIC — Tous régimes de VIX")
            st.caption(
                "Ces patterns prédisent une amplitude SPX ≥ 0.45% "
                "depuis le point d'entrée, sans filtre sur le VIX."
            )
            _pat_ric = _load_pat(f"patterns_{_ep2}_{_hz2}.json")
            _show_patterns(_pat_ric, f"RIC {_ep2}/{_hz2}")

        # ── Tab 3 : RIC Paramètre 1 (VIX ≤ 19) ─────────────
        with _tab_ric_p1:
            st.markdown("### Patterns RIC — Paramètre 1 : VIX ≤ 19 à l'entrée")
            st.info(
                "**Pourquoi VIX ≤ 19 ?** En régime calme, les ailes du RIC "
                "sont moins chères → le gain potentiel est plus élevé par "
                "rapport au coût → meilleur ratio risque/récompense, même "
                "avec une précision légèrement plus faible."
            )
            _pat_ric_p1 = _load_pat(f"patterns_{_ep2}_{_hz2}_vix_le19.json")
            if not _pat_ric_p1 or not _pat_ric_p1.get("all_patterns"):
                st.warning(
                    "Aucun pattern RIC robuste trouvé en régime VIX ≤ 19. "
                    "En régime très calme (VIX < 19), le SPX ne produit pas "
                    "d'amplitude ≥ 0.45% de façon prévisible. "
                    "Ce régime est plus adapté à la stratégie IC."
                )
            else:
                _show_patterns(_pat_ric_p1, f"RIC VIX≤19 {_ep2}/{_hz2}")

        # ── Tab 4 : IC Paramètre 1 (VIX ≤ 19) ──────────────
        with _tab_ic:
            st.markdown("### Patterns IC — Paramètre 1 : VIX ≤ 19 à l'entrée")
            st.info(
                "**IC (Iron Condor)** = stratégie où le SPX reste "
                "dans une plage étroite (amplitude ≤ 0.23% sur 6h). "
                "On profite de la décroissance du temps (theta).\n\n"
                "**Pourquoi VIX ≤ 19 ?** La prime collectée doit couvrir "
                "le risque. Avec VIX < 18, la prime est trop faible. "
                "Avec VIX > 22, le risque de gros mouvement est trop élevé. "
                "La zone 18-19 est le sweet spot."
            )
            _pat_ic = _load_pat(
                f"patterns_{_ep2}_{_hz2}_ic_23bps_vix_le19.json"
            )
            if not _pat_ic or not _pat_ic.get("all_patterns"):
                st.warning(
                    f"Aucun pattern IC disponible pour {_ep2}/{_hz2} en VIX ≤ 19. "
                    "Le meilleur résultat IC est disponible pour 10h00/360min."
                )
                _pat_ic_best = _load_pat(
                    "patterns_10h00_360min_ic_23bps_vix_le19.json"
                )
                if _pat_ic_best and _pat_ic_best.get("all_patterns"):
                    st.markdown(
                        "**Résultat IC disponible — 10h00 / 360min / VIX ≤ 19 :**"
                    )
                    _show_patterns(_pat_ic_best, "IC 10h00/360min VIX≤19")
            else:
                _n_ic_s = _pat_ic.get("n_sessions_ic", "?")
                _n_ic_tot = _pat_ic.get("n_sessions", "?")
                if _n_ic_tot and _n_ic_s:
                    try:
                        _ic_pct = int(_n_ic_s) / int(_n_ic_tot) * 100
                        st.caption(
                            f"En régime VIX ≤ 19, le SPX reste calme "
                            f"(amplitude ≤ 0.23%) dans {_ic_pct:.1f}% des cas "
                            f"({_n_ic_s}/{_n_ic_tot} sessions)."
                        )
                    except Exception:
                        pass
                _show_patterns(_pat_ic, f"IC VIX≤19 {_ep2}/{_hz2}")

        # ── Tab Grille RIC ────────────────────────────────────
        with _tab_grid:
            st.markdown("### 🎯 Explorateur de combinaisons RIC")
            st.caption(
                "Ajuste les paramètres pour explorer les combinaisons "
                "réalisables. Les résultats sont triés par fiabilité décroissante."
            )

            _grid_data = _load_pat("grid_results.json")

            if not _grid_data:
                st.warning(
                    "Grille non calculée. Lancer : "
                    "`from spx_pattern_search import run_grid_search; run_grid_search()`"
                )
            else:
                # ── Sliders ──────────────────────────────────
                _col_s1, _col_s2, _col_s3 = st.columns(3)

                with _col_s1:
                    _sel_ric = st.select_slider(
                        "📊 Seuil RIC minimum",
                        options=[0.30, 0.35, 0.40, 0.45],
                        value=0.35,
                        format_func=lambda x: f"≥ {x:.2f}%",
                        key="expl_ric"
                    )

                with _col_s2:
                    _sel_vix = st.select_slider(
                        "🌡️ VIX maximum",
                        options=[19, 20, 21, 22],
                        value=21,
                        format_func=lambda x: f"≤ {x}",
                        key="expl_vix"
                    )

                with _col_s3:
                    _sel_wr = st.select_slider(
                        "🎯 Win rate minimum",
                        options=[80, 85, 87, 90, 95, 100],
                        value=90,
                        format_func=lambda x: f"≥ {x}%",
                        key="expl_wr"
                    )

                st.markdown("---")

                # ── Collecter tous les résultats valides ──────
                _all_valid = []
                _cell_key = f"{int(_sel_ric * 100)}bps_vix{_sel_vix}"

                for _ep in ["9h30", "10h00"]:
                    for _hz in ["360min", "240min", "180min"]:
                        _cell = _grid_data.get(_ep, {}).get(_hz, {}).get(_cell_key, {})
                        if not _cell.get("ok"):
                            continue
                        _oos = _cell.get("best_oos", 0)
                        if _oos >= _sel_wr:
                            _all_valid.append({
                                "ep": _ep, "hz": _hz,
                                "oos": _oos,
                                "occ": _cell.get("best_occ", 0),
                                "wf": _cell.get("best_wf", "?"),
                                "n_sessions": _cell.get("n_sessions", 0),
                                "n_ric": _cell.get("n_ric", 0),
                                "n_actionable": _cell.get("n_actionable_90", 0),
                                "robust": _cell.get("cell_status") == "green",
                                "pattern": _cell.get("best_pattern"),
                            })

                # Trier : 100% en premier, puis OOS décroissant, puis occurrences
                _all_valid.sort(
                    key=lambda x: (
                        1 if x["oos"] == 100.0 else 0,
                        x["oos"],
                        x["occ"]
                    ),
                    reverse=True
                )

                # ── Résultats ────────────────────────────────
                if _all_valid:
                    st.markdown(
                        f"**{len(_all_valid)} combinaison(s) valide(s)** "
                        f"pour RIC≥{_sel_ric:.2f}% / VIX≤{_sel_vix} / "
                        f"Win rate≥{_sel_wr}%"
                    )
                    st.caption(
                        f"Données : {_all_valid[0]['n_sessions']} sessions "
                        f"| Jours RIC : {_all_valid[0]['n_ric']} "
                        f"({_all_valid[0]['n_ric']/_all_valid[0]['n_sessions']*100:.1f}%)"
                    )

                    for _res in _all_valid:
                        _oos_v = _res["oos"]
                        _emoji = "🟢" if _oos_v == 100.0 else \
                                 "🟢" if _oos_v >= 90 and _res["robust"] else \
                                 "🟡" if _oos_v >= 87 else "🔴"
                        _rob_flag = " ✓ ROBUSTE" if _res["robust"] else ""

                        with st.expander(
                            f"{_emoji} {_res['ep']} / {_res['hz']} — "
                            f"**{_oos_v:.0f}% OOS** | "
                            f"{_res['occ']} occ | "
                            f"WF {_res['wf']}"
                            f"{_rob_flag}",
                            expanded=(_oos_v == 100.0 and _res == _all_valid[0])
                        ):
                            _c1, _c2, _c3, _c4 = st.columns(4)
                            _c1.metric("Win rate OOS", f"{_oos_v:.0f}%")
                            _c2.metric("Occurrences OOS", _res["occ"])
                            _c3.metric("Walk-Forward", _res["wf"])
                            _c4.metric("Patterns ≥90%", _res["n_actionable"])

                            _bp = _res.get("pattern")
                            if _bp:
                                st.markdown("**Conditions à respecter :**")
                                for _cond in _bp.get("conditions", []):
                                    _sym = "≥" if _cond["direction"] == "above" else "≤"
                                    _pct = _cond.get("percentile",
                                                      _cond.get("percentile_is", "?"))
                                    _hr = _human_readable_feature(_cond["feature"])
                                    st.markdown(
                                        f"• **{_hr}** {_sym} `{_cond['threshold']}` "
                                        f"*(percentile {_pct})*"
                                    )
                                _wf_list = _bp.get("precisions_by_window", [])
                                if _wf_list:
                                    _wf_str = " | ".join(
                                        f"{_p:.0f}%" if _p is not None else "n/a"
                                        for _p in _wf_list
                                    )
                                    st.caption(f"Walk-forward par fenêtre : {_wf_str}")
                else:
                    st.error(
                        f"❌ Aucune combinaison valide pour "
                        f"RIC≥{_sel_ric:.2f}% / VIX≤{_sel_vix} / "
                        f"Win rate≥{_sel_wr}%"
                    )

                # ── Contraintes détectées ────────────────────
                st.markdown("---")
                st.markdown("### ⚠️ Contraintes structurelles détectées")

                _impossible = []
                _degraded = []

                if _sel_ric >= 0.45 and _sel_vix <= 19:
                    _impossible.append(
                        "**RIC≥0.45% + VIX≤19** : structurellement impossible. "
                        "En régime très calme (VIX≤19), seulement ~30% des sessions "
                        "produisent une amplitude ≥0.45% sur 360min. "
                        "La base rate est trop faible pour atteindre 90%+ OOS."
                    )

                if _sel_ric >= 0.45 and _sel_vix <= 20:
                    _degraded.append(
                        "**RIC≥0.45% + VIX≤20** : zone difficile (~34% base rate). "
                        "Les patterns trouvés ont peu d'occurrences OOS (6-8). "
                        "Considère RIC≥0.35% ou VIX≤22 pour plus de robustesse."
                    )

                if _sel_ric >= 0.35:
                    _degraded.append(
                        "**10h00/180min** : horizon trop court pour RIC≥0.35%. "
                        "En 3h depuis 10h00, l'amplitude ≥0.35% n'est atteinte "
                        "que dans ~35% des cas même en VIX élevé. "
                        "Préférer 360min ou 240min."
                    )

                st.info(
                    "📅 **Contrainte de données** : Le CSV VIX1D (VIX très court terme) "
                    "n'existe que depuis fin 2022, ce qui limite toute la recherche "
                    "à **791 sessions** (2023-2026) au lieu des ~1481 disponibles. "
                    "Ce CSV est pourtant l'un des signaux les plus prédictifs "
                    "(ratio VIX1j/VIX → Pattern B, 100% OOS). "
                    "Sans lui, le scénario idéal RIC≥0.45%+VIX≤19 pourrait "
                    "bénéficier de 2× plus de données pour valider les patterns."
                )

                if _impossible:
                    for _msg in _impossible:
                        st.error(_msg)

                if _degraded:
                    for _msg in _degraded:
                        st.warning(_msg)

                if not _impossible and not _degraded:
                    st.success(
                        "✅ Aucune contrainte structurelle détectée "
                        "pour cette combinaison."
                    )

                # ── Vue d'ensemble de la grille complète ─────
                st.markdown("---")
                st.markdown("### Vue d'ensemble — toutes combinaisons")
                st.caption(
                    "Chaque cellule montre le meilleur résultat "
                    "pour (entry=9h30, horizon=360min). "
                    "Vert=≥90%+robuste | Jaune=87-90% | Rouge=pas de pattern."
                )

                _gep_ov = st.selectbox(
                    "Point d'entrée", ["9h30", "10h00"],
                    key="grid_ov_ep"
                )
                _ghz_ov = st.selectbox(
                    "Horizon", ["360min", "240min", "180min"],
                    key="grid_ov_hz"
                )

                _ric_thrs_ov = [0.45, 0.40, 0.35, 0.30]
                _vix_maxs_ov = [22, 21, 20, 19]

                _hdr = st.columns([2, 1, 1, 1, 1])
                _hdr[0].markdown("**RIC seuil →**")
                for _vi, _vm in enumerate(_vix_maxs_ov):
                    _hdr[_vi + 1].markdown(f"**VIX ≤ {_vm}**")

                for _rt in _ric_thrs_ov:
                    _row = st.columns([2, 1, 1, 1, 1])
                    _row[0].markdown(f"**≥ {_rt:.2f}%**")
                    for _vi, _vm in enumerate(_vix_maxs_ov):
                        _ck = f"{int(_rt * 100)}bps_vix{_vm}"
                        _c = _grid_data.get(_gep_ov, {}).get(_ghz_ov, {}).get(_ck, {})
                        if not _c.get("ok"):
                            _row[_vi + 1].markdown("❌")
                        else:
                            _s = _c.get("cell_status", "red")
                            _em = "🟢" if _s == "green" else \
                                  "🟡" if _s == "yellow" else "🔴"
                            _o = _c.get("best_oos", 0)
                            _oc = _c.get("best_occ", 0)
                            _w = _c.get("best_wf", "?")
                            _row[_vi + 1].markdown(
                                f"{_em} `{_o:.0f}%`\n\n"
                                f"`{_oc}` occ\n\n"
                                f"WF`{_w}`"
                            )

        # ── Tab Options ──────────────────────────────────────
        with _tab_options:
            st.markdown("### 💰 Simulateur économique options 0DTE SPX")
            st.caption(
                "Croise les patterns détectés avec les mouvements réels du SPX "
                "et les primes options réelles. "
                "Chaque occurrence historique d'un pattern est simulée avec "
                "la structure options correspondante."
            )

            try:
                import sys as _sys
                _sys.path.insert(0, str(BASE_DIR))
                from options_validator import (
                    interpolate_gains, simulate_pattern_economics,
                    get_spx_amplitude_distribution,
                    MIN_GAIN_EXCELLENT, MIN_GAIN_VIABLE, MIN_GAIN_MARGINAL
                )
                _opts_available = True
            except ImportError as _e:
                _opts_available = False
                st.error(f"options_validator.py non disponible : {_e}")

            if _opts_available:
                # ── Section 1 : Calculateur VIX → primes ──
                with st.expander("📊 Primes par structure selon le VIX ouverture", expanded=False):
                    _vix_input = st.slider(
                        "🌡️ VIX à l'ouverture",
                        min_value=12.0, max_value=35.0,
                        value=18.0, step=0.5, key="opts_vix_slider"
                    )
                    _gains = interpolate_gains(_vix_input)

                    def _gain_badge(g):
                        if g >= MIN_GAIN_EXCELLENT:
                            return f"✅✅ **+{g:.1f} pts**"
                        elif g >= MIN_GAIN_VIABLE:
                            return f"✅ **+{g:.1f} pts**"
                        elif g >= MIN_GAIN_MARGINAL:
                            return f"🟡 +{g:.1f} pts"
                        else:
                            return f"❌ {g:.1f} pts"

                    _oc1, _oc2 = st.columns(2)
                    with _oc1:
                        st.markdown("**📈 Marché QUI BOUGE (RIC/RIB)**")
                        st.markdown(f"RIC pur ±30pts : {_gain_badge(_gains['RIC_pur30'])}")
                        st.markdown(f"RIC pur ±40pts : {_gain_badge(_gains['RIC_pur40'])}")
                        st.markdown(f"RIB ±20→±40pts : {_gain_badge(_gains['RIB_20_40'])}")
                    with _oc2:
                        st.markdown("**📉 Marché CALME (IC/IB)**")
                        st.markdown(f"IC pur ±30pts : {_gain_badge(_gains['IC_pur30'])}")
                        st.markdown(f"IC pur ±40pts : {_gain_badge(_gains['IC_pur40'])}")
                        st.markdown(f"IB ±20→±40pts : {_gain_badge(_gains['IB_20_40'])}")

                    _vix_levels_chart = [14.16, 15.09, 16.02, 16.30, 17.16, 18.80, 19.04,
                                         22.36, 24.74, 24.90, 25.91, 27.03, 30.80]
                    import pandas as _pd_opts
                    _chart_data = [{'VIX': v,
                                    'RIC±40': interpolate_gains(v)['RIC_pur40'],
                                    'IC±40': interpolate_gains(v)['IC_pur40'],
                                    'RIB±20→40': interpolate_gains(v)['RIB_20_40'],
                                    'IB±20→40': interpolate_gains(v)['IB_20_40']}
                                   for v in _vix_levels_chart]
                    st.line_chart(_pd_opts.DataFrame(_chart_data).set_index('VIX'))

                # ── Section 2 : Distribution amplitudes SPX par régime VIX ──
                st.markdown("---")
                st.markdown("#### 📐 Amplitudes réelles du SPX par régime VIX")
                st.caption("Combien de fois le SPX bouge de ≥X pts selon le VIX — basé sur l'historique réel.")

                # Définir _gain_badge ici si pas dans expander expanded
                def _gain_badge_main(g):
                    if g >= MIN_GAIN_EXCELLENT:
                        return f"✅✅ **+{g:.1f} pts**"
                    elif g >= MIN_GAIN_VIABLE:
                        return f"✅ **+{g:.1f} pts**"
                    elif g >= MIN_GAIN_MARGINAL:
                        return f"🟡 +{g:.1f} pts"
                    else:
                        return f"❌ {g:.1f} pts"

                _amp_cols = st.columns(3)
                _amp_configs = [
                    ("VIX ≤ 17", None, 17.0),
                    ("VIX 17-22", 17.0, 22.0),
                    ("VIX ≥ 22", 22.0, None),
                ]

                for _ci, (_label, _vmin, _vmax) in enumerate(_amp_configs):
                    with _amp_cols[_ci]:
                        st.markdown(f"**{_label}**")
                        _amp = get_spx_amplitude_distribution(
                            BASE_DIR / "data" / "live_selected",
                            vix_min=_vmin, vix_max=_vmax
                        )
                        if _amp:
                            st.caption(f"{_amp['n_sessions']} sessions | "
                                       f"HL médian : {_amp['hl_median']}pts")
                            for _thr in [20, 30, 40, 50]:
                                _phl = _amp['pct_hl_above'].get(_thr, 0)
                                _pco = _amp['pct_co_above'].get(_thr, 0)
                                _bar = "🟢" if _phl > 60 else "🟡" if _phl > 30 else "🔴"
                                st.markdown(
                                    f"{_bar} ≥{_thr}pts : "
                                    f"H-L {_phl}% | C-O {_pco}%"
                                )

                            st.markdown("*Rentabilité RIC±40 dans ce régime :*")
                            _vmid = (_vmin or 14) + ((_vmax or 35) - (_vmin or 14)) / 2
                            _g = interpolate_gains(_vmid)
                            _ric_g = _g['RIC_pur40']
                            _phl40 = _amp['pct_hl_above'].get(40, 0)
                            _expected = round(_phl40 / 100 * _ric_g - (1 - _phl40 / 100) * _ric_g * 0.8, 1)
                            st.caption(f"SPX bouge ≥40pts dans {_phl40}% des cas → "
                                       f"P&L attendu ≈ {_expected:+.1f}pts/trade")
                        else:
                            st.warning("Données insuffisantes")

                # ── Section 3 : Simulateur par pattern ──
                st.markdown("---")
                st.markdown("#### 🎯 Simulateur P&L par pattern")
                st.caption(
                    "Sélectionne un pattern — le simulateur calcule le P&L réel "
                    "sur chaque occurrence historique en utilisant le VIX open du jour "
                    "et l'amplitude réelle du SPX."
                )

                import json as _json_opts
                import glob as _glob
                _pat_files = sorted(_glob.glob(str(_DATA_ML / "patterns_*.json")))
                _pat_choices = {}
                for _pf in _pat_files:
                    _pname = Path(_pf).stem.replace("patterns_", "")
                    try:
                        with open(_pf) as _f:
                            _pd_tmp = _json_opts.load(_f)
                        if _pd_tmp.get('all_patterns'):
                            _pat_choices[_pname] = (_pf, _pd_tmp)
                    except Exception:
                        pass

                if not _pat_choices:
                    st.warning("Aucun fichier pattern trouvé dans data/.")
                else:
                    _sel_pat_name = st.selectbox(
                        "Pattern à simuler",
                        list(_pat_choices.keys()),
                        key="sim_pat_select"
                    )
                    _sel_file, _sel_data = _pat_choices[_sel_pat_name]

                    _all_pats = _sel_data.get('all_patterns', [])
                    _pat_labels = [
                        f"#{i+1} OOS={p.get('precision_oos',0):.0f}% "
                        f"({p.get('n_oos',0)} occ) — "
                        f"{' + '.join(c['feature'][:25] for c in p.get('conditions',[])[:2])}"
                        for i, p in enumerate(_all_pats[:10])
                    ]

                    if _pat_labels:
                        _sel_pat_idx = st.selectbox(
                            "Pattern spécifique",
                            range(len(_pat_labels)),
                            format_func=lambda i: _pat_labels[i],
                            key="sim_pat_idx"
                        )
                        _sel_pat = _all_pats[_sel_pat_idx]

                        _sim_strat = st.radio(
                            "Structure à simuler",
                            ["RIC ±40", "IC ±40", "RIB ±20→±40", "IB ±20→±40"],
                            horizontal=True,
                            key="sim_strat"
                        )

                        _strat_map = {
                            "RIC ±40": ('RIC', 40, None),
                            "IC ±40":  ('IC',  40, None),
                            "RIB ±20→±40": ('RIB', 20, 40),
                            "IB ±20→±40":  ('IB',  20, 40),
                        }

                        _oos_dates = _sel_pat.get('oos_dates', [])

                        if not _oos_dates:
                            st.info(
                                "Les dates OOS ne sont pas stockées dans ce fichier. "
                                "Relance run_full_search() pour les générer."
                            )
                        else:
                            import pandas as _pd_sim
                            _dates = [_pd_sim.Timestamp(d) for d in _oos_dates]

                            _sim_result = simulate_pattern_economics(
                                pattern_occurrences=_dates,
                                daily_data_dir=BASE_DIR / "data" / "live_selected",
                                chain_data_dir=BASE_DIR / "data" / "live_selected",
                                structure_configs=[_strat_map[_sim_strat]],
                            )

                            _key = (_strat_map[_sim_strat][0] +
                                    (f"_{_strat_map[_sim_strat][1]}"
                                     if _strat_map[_sim_strat][2] is None
                                     else f"_{_strat_map[_sim_strat][1]}_"
                                          f"{_strat_map[_sim_strat][2]}"))

                            _s = _sim_result['stats'].get(_key, {})
                            if _s:
                                _r1, _r2, _r3, _r4 = st.columns(4)
                                _r1.metric("Win rate", f"{_s['win_rate']:.0f}%")
                                _r2.metric("P&L moyen/trade", f"{_s['avg_pnl']:+.1f}pts")
                                _r3.metric("P&L total", f"{_s['total_pnl']:+.1f}pts")
                                _r4.metric("Trades simulés", _s['n_trades'])

                                st.markdown("**Détail par session :**")
                                for _sess in _sim_result['sessions']:
                                    _pnl_val = _sess.get(f'pnl_{_key}', 0)
                                    _emoji = "✅" if _pnl_val > 0 else "❌"
                                    st.markdown(
                                        f"{_emoji} {_sess['date'].date()} | "
                                        f"VIX={_sess['vix_open']:.1f} | "
                                        f"SPX H-L={_sess['spx_amplitude_hl']:.0f}pts "
                                        f"C-O={_sess['spx_move_abs']:.0f}pts | "
                                        f"P&L={_pnl_val:+.1f}pts"
                                    )

        # ── Tab Validation 2025 ──────────────────────────────
        with _tab_val2025:
            st.markdown("### 📅 Validation des patterns sur 2025")
            st.caption(
                "Vérifie que les patterns découverts sur 2023-2026 "
                "tiennent sur les données 2025 uniquement. "
                "Un delta négatif indique une dégradation récente."
            )

            _v2025_col1, _v2025_col2, _v2025_col3 = st.columns([2, 2, 1])
            with _v2025_col1:
                _v2025_ep = st.selectbox(
                    "Point d'entrée",
                    ["9h30", "10h00"],
                    key="v2025_ep"
                )
            with _v2025_col2:
                _v2025_hz = st.selectbox(
                    "Horizon",
                    ["360min", "240min", "180min"],
                    key="v2025_hz"
                )
            with _v2025_col3:
                st.markdown("&nbsp;", unsafe_allow_html=True)
                _run_v2025 = st.button(
                    "▶ Valider",
                    key="v2025_run",
                    use_container_width=True
                )

            _v2025_file = _DATA_ML / f"validation_2025_{_v2025_ep}_{_v2025_hz}.json"
            _v2025_existing = None
            if _v2025_file.exists():
                try:
                    import json as _jv25
                    with open(_v2025_file) as _f:
                        _v2025_existing = _jv25.load(_f)
                except Exception:
                    pass

            if _run_v2025:
                with st.spinner(f"Validation 2025 en cours pour {_v2025_ep}/{_v2025_hz}..."):
                    try:
                        from spx_pattern_search import run_validation_2025
                        _v2025_result = run_validation_2025(
                            entry_point=_v2025_ep,
                            horizon=_v2025_hz,
                        )
                        if _v2025_result.get("ok"):
                            _v2025_existing = _v2025_result["results"]
                            st.success(
                                f"✅ {_v2025_result['n_sessions_2025']} sessions 2025 "
                                f"analysées | {len(_v2025_existing)} patterns évalués"
                            )
                        else:
                            st.error(_v2025_result.get("error"))
                    except Exception as _e:
                        st.error(f"Erreur : {_e}")

            if _v2025_existing:
                st.markdown(f"**{len(_v2025_existing)} patterns évalués**")
                st.markdown(
                    "🟢 ≥90% sur 2025 | 🟡 80-90% | 🔴 <80% | "
                    "Delta = précision 2025 − précision OOS globale"
                )

                for _r25 in _v2025_existing[:15]:
                    _p25 = _r25.get("precision_2025", 0)
                    _pg = _r25.get("oos_global", 0)
                    _delta = _r25.get("delta", 0)
                    _n = _r25.get("n_trigger_2025", 0)
                    _rob = "✓" if _r25.get("is_robust") else ""

                    _flag = "🟢" if _p25 >= 90 else "🟡" if _p25 >= 80 else "🔴"
                    _delta_str = f"+{_delta:.1f}%" if _delta >= 0 else f"{_delta:.1f}%"

                    with st.expander(
                        f"{_flag} {_r25['source']} {_rob} | "
                        f"2025={_p25:.0f}% ({_n} occ) | "
                        f"OOS global={_pg:.0f}% | "
                        f"Δ={_delta_str}",
                        expanded=False
                    ):
                        for _c in _r25.get("conditions", []):
                            _sym = "≥" if _c["direction"] == "above" else "≤"
                            _hr = _human_readable_feature(_c["feature"])
                            st.markdown(
                                f"• **{_hr}** {_sym} `{_c['threshold']}` "
                                f"*(P{_c.get('percentile', '?')})*"
                            )

            elif not _run_v2025:
                st.info(
                    "Clique sur **Valider** pour lancer l'analyse. "
                    "Les résultats sont sauvegardés et rechargés automatiquement."
                )

        # ── Tab Signal aujourd'hui ─────────────────────────
        with _tab_today:
            st.markdown("### 📡 Signal actif aujourd'hui ?")
            st.caption(
                "Évalue les patterns prioritaires sur les données J-1. "
                "Si un pattern matche, indique la meilleure structure options "
                "selon le VIX d'ouverture."
            )

            try:
                from spx_pattern_search import check_today_signals as _cts
                _today_available = True
            except Exception as _e:
                _today_available = False
                st.error(f"check_today_signals non disponible : {_e}")

            if _today_available:
                _col_ep, _col_vix, _col_btn = st.columns([2, 2, 1])
                with _col_ep:
                    _today_ep = st.selectbox(
                        "Point d'entrée",
                        ["9h30", "10h00"],
                        key="today_ep"
                    )
                with _col_vix:
                    _today_vix = st.number_input(
                        "VIX à l'ouverture (optionnel)",
                        min_value=0.0, max_value=80.0,
                        value=0.0, step=0.1,
                        key="today_vix",
                        help="Laisse à 0 si pas encore connu"
                    )
                with _col_btn:
                    st.markdown("&nbsp;", unsafe_allow_html=True)
                    _run_today = st.button(
                        "🔍 Vérifier",
                        key="today_run",
                        use_container_width=True
                    )

                if _run_today:
                    with st.spinner("Calcul des features et évaluation des patterns..."):
                        _vix_arg = float(_today_vix) if _today_vix > 0 else None
                        _today_result = _cts(
                            entry_point=_today_ep,
                            vix_open_today=_vix_arg,
                            verbose=False,
                        )

                    if not _today_result.get("ok"):
                        st.error(_today_result.get("error", "Erreur inconnue"))
                    elif _today_result["no_trade"]:
                        st.success(
                            f"✅ **Aucun signal actif** pour {_today_ep} "
                            f"(données au {_today_result['evaluation_date']}). "
                            f"Pas de trade aujourd'hui."
                        )
                    else:
                        st.warning(
                            f"🔴 **{_today_result['n_signals']} signal(s) actif(s)** "
                            f"pour {_today_ep} — données au "
                            f"{_today_result['evaluation_date']}"
                        )
                        for _sig in _today_result["signals"]:
                            _rob = " ✓ ROBUSTE" if _sig["is_robust"] else ""
                            with st.expander(
                                f"🔴 {_sig['source']}{_rob} — "
                                f"OOS={_sig['oos_pct']:.0f}% / "
                                f"{_sig['n_oos']} occ",
                                expanded=True
                            ):
                                st.markdown("**Conditions vérifiées :**")
                                for _c in _sig["conditions"]:
                                    _sym = "≥" if _c["direction"] == "above" else "≤"
                                    _hr = _human_readable_feature(_c["feature"])
                                    st.markdown(
                                        f"• **{_hr}** {_sym} "
                                        f"`{_c['threshold']}` "
                                        f"*(P{_c.get('percentile', _c.get('percentile_is', '?'))})*"
                                    )

                                if "options_recommendation" in _sig:
                                    _rec = _sig["options_recommendation"]
                                    st.markdown("---")
                                    st.markdown(
                                        f"**Recommandation options :** "
                                        f"{_rec['strategy']} → "
                                        f"{_rec['verdict']} "
                                        f"(max gain **{_rec['max_gain_pts']:.1f}pts** "
                                        f"@ VIX {_rec['vix_used']:.1f})"
                                    )
                                    _all_g = _rec.get("all_gains", {})
                                    if _all_g:
                                        _gc1, _gc2 = st.columns(2)
                                        with _gc1:
                                            st.caption(
                                                f"RIC±40: {_all_g.get('RIC_pur40',0):+.1f}pts | "
                                                f"RIC±30: {_all_g.get('RIC_pur30',0):+.1f}pts | "
                                                f"RIB±20→40: {_all_g.get('RIB_20_40',0):+.1f}pts"
                                            )
                                        with _gc2:
                                            st.caption(
                                                f"IC±40: {_all_g.get('IC_pur40',0):+.1f}pts | "
                                                f"IC±30: {_all_g.get('IC_pur30',0):+.1f}pts | "
                                                f"IB±20→40: {_all_g.get('IB_20_40',0):+.1f}pts"
                                            )
                else:
                    st.info(
                        "Clique sur **Vérifier** pour évaluer les patterns "
                        "sur les données les plus récentes. "
                        "Si le VIX est connu (après 9h30 NY), entre-le "
                        "pour obtenir la recommandation options complète."
                    )

    with st.expander("🕯️ BBE — Analyse multi-ticker", expanded=False):
        st.caption(
            "Détection BBE stricte : confirmation volume + corps significatif. "
            "Les earnings ±5j sont automatiquement exclus si un fichier "
            "ticker_earnings.csv est présent."
        )

        _bbe_tickers_available = []
        _INDICES_EXCLUDE = {
            "SPX", "SPY", "VIX", "VIX1D", "VIX3M", "VIX6M", "VIX9D", "VVIX",
            "SKEW", "IWM", "QQQ", "DAX40", "NIKKEI225", "GOLD", "DXY",
            "FTSE100", "ADVANCE_DECLINE_RATIO_NET_RATIO_PUT_CALL",
            "EQUITY_PUT_CALL_RATIO", "SPX_PUT_CALL_RATIO",
            "SPY_PUT_CALL_RATIO", "IWM_PUT_CALL_RATIO",
            "QQQ_PUT_CALL_RATIO", "VIX_PUT_CALL_RATIO",
            "US_10_YEARS_BONDS", "US_BONDS_30_DAYS_CONTRACT",
            "VIX_SPX_OPEN", "VIX_SPX_OPEN_PLUS_30",
            "YIELD_CURVE_SPREAD_10Y_2Y", "VIX1D_VIX_RATIO",
            "SPX_IWM_CORRELATION_20DAYS", "SPX_QQQ_CORRELATION_20DAYS",
            "SPX_20_DAYS_AVERAGE_RANGE", "SPX_5_DAYS_AVERAGE_RANGE",
            "CALENDAR_EVENTS", "OANDA_USB02YUSD_1D", "OANDA_USB10YUSD_1D",
            "VX_FUTURE_VX1", "VX_FUTURE_VX2",
        }
        for _bbe_dir in [DATA_DIR, TICKERS_DIR]:
            if _bbe_dir.exists():
                for _f in sorted(_bbe_dir.glob("*.csv")):
                    _stem = _f.stem.upper()
                    if any(x in _stem for x in ["OPTION", "CHAIN", "1MIN",
                                                 "5MIN", "30MIN", "1HOUR",
                                                 "4HOUR", "EARNINGS", "OIL",
                                                 "TICK"]):
                        continue
                    _tk = _stem.replace("_DAILY", "")
                    if _tk not in _INDICES_EXCLUDE and len(_tk) <= 5:
                        _bbe_tickers_available.append((_tk, _f))

        _seen = set()
        _bbe_tickers_dedup = []
        for _tk, _p in _bbe_tickers_available:
            if _tk not in _seen:
                _seen.add(_tk)
                _bbe_tickers_dedup.append((_tk, _p))
        _bbe_tickers_available = _bbe_tickers_dedup

        if not _bbe_tickers_available:
            st.info("Aucun ticker individuel trouvé.")
        else:
            _bbe_ticker_names = [t[0] for t in _bbe_tickers_available]
            _bbe_ticker_paths = {t[0]: t[1] for t in _bbe_tickers_available}

            _bbe_col1, _bbe_col2, _bbe_col3 = st.columns([3, 2, 2])
            with _bbe_col1:
                _bbe_selected = st.multiselect(
                    "Tickers (max 5)", _bbe_ticker_names,
                    default=_bbe_ticker_names[:1] if _bbe_ticker_names else [],
                    max_selections=5, key="bbe_tickers"
                )
            with _bbe_col2:
                _bbe_pattern = st.radio(
                    "Pattern", ["Bearish", "Bullish", "Les deux"],
                    horizontal=True, key="bbe_pattern"
                )
            with _bbe_col3:
                _bbe_n_months = st.slider(
                    "Mois d'historique", min_value=3, max_value=36,
                    value=12, step=3, key="bbe_months"
                )

            _bbe_jend_col, _bbe_filter_col = st.columns([3, 3])
            with _bbe_jend_col:
                _bbe_jend = st.select_slider(
                    "📅 Fenêtre de validation",
                    options=[1, 2, 3, 4, 5], value=5,
                    format_func=lambda x: f"J+{x} ouvrés",
                    key="bbe_jend"
                )
            with _bbe_filter_col:
                _bbe_vix_min = st.slider(
                    "🌡️ VIX min (bearish)", min_value=0.0, max_value=30.0,
                    value=0.0, step=0.5, key="bbe_vix_min",
                    help="0 = pas de filtre VIX"
                )

            with st.expander("⚙️ Filtres avancés", expanded=False):
                _bbe_adv1, _bbe_adv2 = st.columns(2)
                with _bbe_adv1:
                    _bbe_ma_filter = st.checkbox(
                        "📉 Bearish seulement si cours < MA20",
                        value=False, key="bbe_ma_filter"
                    )
                    _bbe_min_body_pct = st.slider(
                        "Corps min (% du range H-L)", min_value=0, max_value=80,
                        value=30, step=5, key="bbe_body_pct"
                    )
                with _bbe_adv2:
                    _bbe_require_gap = st.checkbox(
                        "⬆️ Bearish : gap up/neutre à l'open",
                        value=True, key="bbe_gap"
                    )
                    _bbe_vol_ma = st.checkbox(
                        "📊 Volume J > moyenne 20j",
                        value=False, key="bbe_vol_ma"
                    )
                    _bbe_rsi_min = st.slider(
                        "RSI J-1 min (bearish en surachat)",
                        min_value=40, max_value=75,
                        value=55, step=5, key="bbe_rsi_min",
                        help="Bearish plus fiable si RSI élevé avant"
                    )

            # Charger VIX (cached)
            _df_vix = _cached_load_csv(str(DATA_DIR / "VIX_daily.csv"))

            if _bbe_selected:
                patterns_to_run = []
                if _bbe_pattern in ("Bearish", "Les deux"):
                    patterns_to_run.append("bearish")
                if _bbe_pattern in ("Bullish", "Les deux"):
                    patterns_to_run.append("bullish")

                for _tk in _bbe_selected:
                    st.markdown(f"#### {_tk}")
                    _tk_path = _bbe_ticker_paths[_tk]
                    try:
                        _df_tk = pd.read_csv(_tk_path, sep=";")
                        _df_tk.columns = [c.strip().lower() for c in _df_tk.columns]
                        _df_tk["time"] = pd.to_datetime(
                            _df_tk["time"].astype(str).str.strip(), errors="coerce"
                        )
                        _df_tk = _df_tk.dropna(subset=["time"]).sort_values("time")
                        for _nc in ["open", "high", "low", "close", "volume"]:
                            if _nc in _df_tk.columns:
                                _df_tk[_nc] = pd.to_numeric(
                                    _df_tk[_nc].astype(str).str.replace(",", "."),
                                    errors="coerce"
                                )

                        _cutoff = pd.Timestamp.now() - pd.DateOffset(months=_bbe_n_months)
                        _df_bbe_full = _df_tk.copy()  # toutes les données pour J+1..J+5
                        _df_tk = _df_tk[_df_tk["time"] >= _cutoff]

                        _earn_dates = set()
                        for _ed in [DATA_DIR, TICKERS_DIR]:
                            for _ef in [f"{_tk}_earnings.csv", f"{_tk.lower()}_earnings.csv"]:
                                _ep = _ed / _ef
                                if _ep.exists():
                                    try:
                                        _fl = _ep.read_text().split("\n")[0]
                                        _sep = ";" if ";" in _fl else ","
                                        _edf = pd.read_csv(_ep, sep=_sep)
                                        _ecol = next((c for c in _edf.columns
                                                      if "date" in c.lower()), _edf.columns[0])
                                        _earn_dates = set(
                                            pd.to_datetime(_edf[_ecol].astype(str).str.strip(),
                                                           format="mixed", errors="coerce")
                                            .dropna().dt.normalize()
                                        )
                                        st.caption(f"📅 {len(_earn_dates)} earnings — ±5j exclus")
                                    except Exception:
                                        pass
                                    break

                        if len(_df_tk) < 10:
                            st.warning(f"Pas assez de données pour {_tk}")
                            continue

                        _has_vol = "volume" in _df_tk.columns
                        _pat_cols = st.columns(len(patterns_to_run))
                        for _pi, _pat in enumerate(patterns_to_run):
                            with _pat_cols[_pi]:
                                _pat_label = "🔴 Bearish" if _pat == "bearish" else "🟢 Bullish"
                                st.markdown(f"**{_pat_label}**")

                                _df_bbe = _df_tk.copy().reset_index(drop=True)
                                _df_bbe["body"] = (_df_bbe["close"] - _df_bbe["open"]).abs()
                                _df_bbe["is_green"] = _df_bbe["close"] > _df_bbe["open"]
                                _df_bbe["is_red"] = _df_bbe["close"] < _df_bbe["open"]
                                if _bbe_ma_filter:
                                    _df_bbe["ma20"] = _df_bbe["close"].rolling(20).mean()

                                _trading_dates = sorted(_df_bbe_full["time"].dt.normalize().unique())
                                _trading_idx = {d: i for i, d in enumerate(_trading_dates)}

                                _signals = []
                                for _i in range(1, len(_df_bbe)):
                                    _p = _df_bbe.iloc[_i - 1]
                                    _c = _df_bbe.iloc[_i]
                                    _dt = pd.Timestamp(_c["time"]).normalize()

                                    if _earn_dates and any(
                                        abs((_dt - _e).days) <= 5 for _e in _earn_dates
                                    ):
                                        continue

                                    if _pat == "bearish":
                                        if not (_p["is_green"] and _c["is_red"]):
                                            continue
                                        if _bbe_require_gap and _c["open"] < _p["close"] * 0.999:
                                            continue
                                        if _c["close"] >= _p["open"]:
                                            continue
                                        if _c["body"] < _p["body"] * 1.1:
                                            continue
                                        if _has_vol and _p["volume"] > 0:
                                            if _c["volume"] < _p["volume"]:
                                                continue
                                        if _bbe_ma_filter and "ma20" in _df_bbe.columns:
                                            _ma_val = _df_bbe.iloc[_i].get("ma20")
                                            if not pd.isna(_ma_val) and _c["close"] > _ma_val:
                                                continue
                                    else:
                                        if not (_p["is_red"] and _c["is_green"]):
                                            continue
                                        if _c["open"] > _p["close"] * 1.001:
                                            continue
                                        if _c["close"] <= _p["open"]:
                                            continue
                                        if _c["body"] < _p["body"] * 1.1:
                                            continue
                                        if _has_vol and _p["volume"] > 0:
                                            if _c["volume"] < _p["volume"]:
                                                continue

                                    # Filtre corps % du range
                                    _range_hl = _c["high"] - _c["low"]
                                    if _bbe_min_body_pct > 0 and _range_hl > 0:
                                        if _c["body"] / _range_hl * 100 < _bbe_min_body_pct:
                                            continue

                                    # Filtre VIX (bearish)
                                    if _pat == "bearish" and _bbe_vix_min > 0 and _df_vix is not None:
                                        _vix_match = _df_vix[_df_vix["time"].dt.normalize() == _dt]
                                        if len(_vix_match) > 0:
                                            _vix_day = float(_vix_match.iloc[0].get("open", 0))
                                            if _vix_day < _bbe_vix_min:
                                                continue

                                    # Volume > MA20
                                    if _bbe_vol_ma and _has_vol:
                                        _vol_ma20 = _df_bbe["volume"].rolling(20).mean().iloc[_i]
                                        if not pd.isna(_vol_ma20) and _c["volume"] < _vol_ma20:
                                            continue

                                    # RSI minimum (bearish)
                                    if _pat == "bearish" and "rsi" in _df_bbe.columns:
                                        _rsi_val = _df_bbe["rsi"].iloc[_i - 1]
                                        if not pd.isna(_rsi_val) and _rsi_val < _bbe_rsi_min:
                                            continue

                                    _var = (_c["close"] / _p["close"] - 1) * 100 if _p["close"] > 0 else 0
                                    _vr = (_c["volume"] / _p["volume"]
                                           if _has_vol and _p["volume"] > 0 else None)
                                    _br = (_c["body"] / _p["body"]
                                           if _p["body"] > 0 else None)
                                    _signals.append({
                                        "date": _dt.strftime("%Y-%m-%d"),
                                        "close": round(float(_c["close"]), 2),
                                        "var_j": round(float(_var), 2),
                                        "vol_ratio": round(float(_vr), 2) if _vr else "—",
                                        "body_ratio": round(float(_br), 2) if _br else "—",
                                    })

                                # Calc meilleure perf J+1..J+jend
                                for _sig in _signals:
                                    _sig_dt = pd.Timestamp(_sig["date"]).normalize()
                                    _idx = _trading_idx.get(_sig_dt)
                                    if _idx is None:
                                        _sig["perf_jend"] = None
                                        _sig["success_strict"] = False
                                        continue

                                    _close_j = _sig["close"]
                                    _success = False
                                    _best_perf = None

                                    # Seuils sidebar (valeurs positives : 2.0 = -2% bearish, +2% bullish)
                                    _be_seuil_pct = float(st.session_state.get("be_seuil", 2.0))
                                    _bull_seuil_pct = float(st.session_state.get("bull_seuil", 2.0))
                                    _bear_target = _close_j * (1 - _be_seuil_pct / 100.0)
                                    _bull_target = _close_j * (1 + _bull_seuil_pct / 100.0)

                                    if _pat == "bearish":
                                        for _jj in range(1, _bbe_jend + 1):
                                            _fidx = _idx + _jj
                                            if _fidx >= len(_trading_dates):
                                                break
                                            _fdt = _trading_dates[_fidx]
                                            _frow = _df_bbe_full[_df_bbe_full["time"].dt.normalize() == _fdt]
                                            if len(_frow) == 0:
                                                continue
                                            _fclose = float(_frow.iloc[0]["close"])
                                            _flow = float(_frow.iloc[0]["low"]) if "low" in _frow.columns else _fclose
                                            _candidate = min(_flow, _fclose)
                                            _perf = (_candidate - _close_j) / _close_j * 100
                                            if _best_perf is None or _perf < _best_perf:
                                                _best_perf = _perf
                                            if _candidate <= _bear_target:
                                                _success = True
                                    else:  # bullish
                                        for _jj in range(1, _bbe_jend + 1):
                                            _fidx = _idx + _jj
                                            if _fidx >= len(_trading_dates):
                                                break
                                            _fdt = _trading_dates[_fidx]
                                            _frow = _df_bbe_full[_df_bbe_full["time"].dt.normalize() == _fdt]
                                            if len(_frow) == 0:
                                                continue
                                            _fclose = float(_frow.iloc[0]["close"])
                                            _fhigh = float(_frow.iloc[0]["high"]) if "high" in _frow.columns else _fclose
                                            _candidate = max(_fhigh, _fclose)
                                            _perf = (_candidate - _close_j) / _close_j * 100
                                            if _best_perf is None or _perf > _best_perf:
                                                _best_perf = _perf
                                            if _candidate >= _bull_target:
                                                _success = True

                                    _sig["perf_jend"] = round(_best_perf, 2) if _best_perf is not None else None
                                    _sig["success_strict"] = _success

                                # Win rate
                                _valid_sigs = [s for s in _signals if s.get("perf_jend") is not None]
                                _wins = [s for s in _valid_sigs if s.get("success_strict")]
                                _wr = len(_wins) / len(_valid_sigs) * 100 if _valid_sigs else 0

                                if _valid_sigs:
                                    _wr_color = "#26a269" if _wr >= 60 else "#f6d32d" if _wr >= 45 else "#e01b24"
                                    st.markdown(
                                        f"<div style='background:#1a1a2e;border-radius:10px;"
                                        f"padding:12px 16px;margin:8px 0;text-align:center;'>"
                                        f"<span style='font-size:13px;color:#888;'>Win rate J+{_bbe_jend} "
                                        f"— {len(_valid_sigs)} signaux</span><br>"
                                        f"<span style='font-size:28px;font-weight:700;color:{_wr_color};'>"
                                        f"{_wr:.0f}%</span>"
                                        f"</div>",
                                        unsafe_allow_html=True
                                    )

                                if _signals:
                                    for _sig in reversed(_signals):
                                        _col_sig = "#e01b24" if _pat == "bearish" else "#26a269"
                                        _var_col = "#e01b24" if _sig["var_j"] < 0 else "#26a269"
                                        _perf_val = _sig.get("perf_jend")
                                        _perf_str = f"{_perf_val:+.2f}%" if _perf_val is not None else "—"
                                        _perf_col = "#e01b24" if (_perf_val or 0) < 0 else "#26a269"
                                        _succ = _sig.get("success_strict", False)
                                        _succ_str = "✅ Succès" if _succ else "❌ Échec" if _perf_val is not None else ""
                                        st.markdown(f"""
<div style="background:#1a1a2e;border-left:4px solid {_col_sig};
     border-radius:8px;padding:10px 14px;margin:4px 0;">
  <div style="display:flex;justify-content:space-between;align-items:center;">
    <span style="font-weight:700;color:#fff;font-size:14px;">{_sig['date']}</span>
    <span style="font-size:12px;color:{'#26a269' if _succ else '#e01b24'};
          font-weight:600;">{_succ_str}</span>
  </div>
  <div style="font-size:13px;color:#ccc;margin-top:6px;">
    Close : <b style="color:#fff;">{_sig['close']}</b> &nbsp;|&nbsp;
    Var J : <b style="color:{_var_col};">{_sig['var_j']:+.2f}%</b> &nbsp;|&nbsp;
    Best J+1..J+{_bbe_jend} : <b style="color:{_perf_col};">{_perf_str}</b>
  </div>
  <div style="font-size:11px;color:#666;margin-top:4px;">
    Vol×{_sig['vol_ratio']} &nbsp;|&nbsp; Corps×{_sig['body_ratio']}
  </div>
</div>""", unsafe_allow_html=True)
                                else:
                                    st.info("Aucun BBE strict sur la période")

                    except Exception as _bbe_err:
                        st.error(f"Erreur {_tk} : {_bbe_err}")

    # ── PEAD — Backtest & Scan quotidien (univers Russell 1000 large caps) ─
    try:
        from pead_ui import render_pead_tab
        render_pead_tab()
    except Exception as _e_pead:
        st.error(f"Erreur PEAD tab : {_e_pead}")

    with st.form("qform", clear_on_submit=False):
        query = st.text_area(
            "Question", value="", label_visibility="collapsed",
            placeholder="ex: SPX quand VIX > 18  |  QQQ si VIX < 20 les lundis",
            height=68,
        )
        submitted = st.form_submit_button("🔍 Rechercher", use_container_width=True)

    if submitted and query.strip():
        q = query.strip()
        st.session_state["last_query"] = q
        with st.spinner("Calcul…"):
            result = _compute_result(q, session_state=st.session_state)
        st.session_state.history.append({"q": q, "result": result})
        if len(st.session_state.history) > 20:
            st.session_state.history = st.session_state.history[-20:]
        _save_history(st.session_state.history)
        st.session_state.active_idx = len(st.session_state.history) - 1
        st.session_state["_pending_q"] = q
        st.session_state["_pending_result"] = _result_to_serializable(result)
        st.rerun()

    # ── Affichage du résultat actif ────────────────────────────────────────
    # Priorité 1 : résultat frais stocké au submit (bypasse active_idx)
    if st.session_state.get("_pending_result") is not None:
        fresh_q = st.session_state.pop("_pending_q", "")
        fresh_result = _result_from_serializable(st.session_state.pop("_pending_result"))
        active_i = st.session_state.active_idx
        item = {"q": fresh_q, "result": fresh_result}
    # Priorité 2 : navigation sidebar
    elif 0 <= st.session_state.active_idx < len(st.session_state.history):
        active_i = st.session_state.active_idx
        item = st.session_state.history[active_i]
    else:
        item = None

    if item:
        st.markdown("**Question :**")
        st.code(item["q"], language=None)
        _render_result(item["result"])

        # ── Follow-up conversation widget (Point 4) ────────────────────────
        st.markdown("")
        with st.form(f"followup_{active_i}", clear_on_submit=True):
            followup_col, btn_col = st.columns([6, 1])
            with followup_col:
                followup_q = st.text_area(
                    "Suivi", height=60, label_visibility="collapsed",
                    placeholder="Question de suivi sur ce résultat…",
                )
            with btn_col:
                followup_sent = st.form_submit_button("→ Suivi")

        if followup_sent and followup_q.strip():
            parent_q = item["q"]
            parent_result = item["result"]
            enriched_q = _build_followup_query(parent_q, parent_result, followup_q.strip())
            recent_qs = {h["q"] for h in st.session_state.history[-20:]}
            if enriched_q not in recent_qs:
                with st.spinner("Calcul…"):
                    new_result = _compute_result(enriched_q, session_state=st.session_state)
                st.session_state.history.append({"q": enriched_q, "result": new_result})
                if len(st.session_state.history) > 20:
                    st.session_state.history = st.session_state.history[-20:]
                _save_history(st.session_state.history)
                st.session_state.active_idx = len(st.session_state.history) - 1
                st.session_state["_pending_q"] = enriched_q
                st.session_state["_pending_result"] = _result_to_serializable(new_result)
            else:
                for idx in range(len(st.session_state.history) - 1, -1, -1):
                    if st.session_state.history[idx]["q"] == enriched_q:
                        st.session_state.active_idx = idx
                        st.session_state["_pending_q"] = enriched_q
                        st.session_state._pending_result = _result_to_serializable(
                            st.session_state.history[idx]["result"]
                        )
                        break
            st.rerun()

        # ── Graphique combiné si 2 entrées partagent le même actif (Point 4) ─
        if len(st.session_state.history) >= 2 and active_i > 0:
            prev_item = st.session_state.history[active_i - 1]
            curr_r = item["result"]
            prev_r = prev_item["result"]
            if (curr_r.get("type") == "C1" and prev_r.get("type") == "C1"
                    and curr_r.get("subject") == prev_r.get("subject")):
                curr_df = curr_r["stats"]["df"]["var_pct"].copy()
                prev_df = prev_r["stats"]["df"]["var_pct"].copy()
                curr_df.index = curr_df.index.strftime("%Y-%m-%d")
                prev_df.index = prev_df.index.strftime("%Y-%m-%d")
                cond_a = (prev_r.get("cond_str") or "Critère A")[:30]
                cond_b = (curr_r.get("cond_str") or "Critère B")[:30]
                combined = pd.DataFrame({cond_a: prev_df, cond_b: curr_df}).dropna(how="all")
                if not combined.empty:
                    st.markdown("**Comparaison des deux derniers résultats**")
                    st.bar_chart(combined, height=260, use_container_width=True)


# ─── CLI ──────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) > 1:
        print(answer(" ".join(sys.argv[1:])))
    else:
        print("SPX Quant Engine LOCAL — Couche 1 (regex) + Couche 2 (Ollama+DuckDB)")
        print("Tape 'exit' pour quitter.\n")
        while True:
            try:
                q = input(">>> ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not q or q.lower() in ("exit", "quit"):
                break
            print(answer(q))
            print()


# ─── Entrée ───────────────────────────────────────────────────────────────

try:
    import streamlit as st
    if st.runtime.exists():
        _streamlit_app()
    elif __name__ == "__main__":
        main()
except Exception:
    if __name__ == "__main__":
        main()
