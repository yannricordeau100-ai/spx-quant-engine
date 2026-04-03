# SPX_QUANT_ENGINE | v2.0
import re
from pathlib import Path
import pandas as pd
import pytz
import streamlit as st

st.set_page_config(page_title="SPX Quant Engine", layout="wide")

VERSION = "v2.0"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "live_selected"
DEFAULT_Q = "SPX quand VIX1D/VIX > 1.2"

# ─── Hardcoded registry (aliases + special columns — take priority) ────────

_HC_CONDITIONS = {
    # VIX family
    "vix1d/vix":    ("VIX1D_VIX_ratio_daily.csv",                            "open"),
    "vix1d_vix":    ("VIX1D_VIX_ratio_daily.csv",                            "open"),
    "vix9d":        ("VIX9D_daily.csv",                                       "close"),
    "vix3m":        ("VIX3M_daily.csv",                                       "close"),
    "vix6m":        ("VIX6M_daily.csv",                                       "close"),
    "vvix":         ("VVIX_daily.csv",                                        "close"),
    "vix":          ("VIX_daily.csv",                                         "close"),
    # Sentiment
    "skew":         ("SKEW_INDEX_daily.csv",                                  "close"),
    # Macro
    "dxy":          ("DXY_daily.csv",                                         "close"),
    "gold":         ("Gold_daily.csv",                                        "close"),
    "nikkei":       ("NIKKEI225_daily.csv",                                   "close"),
    "dax":          ("DAX40_daily.csv",                                       "close"),
    "ftse":         ("FTSE100_daily.csv",                                     "close"),
    # Put-Call ratios
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
    # Rates (special column name)
    "yield curve":  ("Yield_Curve_Spread_10Y_2Y.csv", "spread_10Y_minus_2Y"),
    "yield_curve":  ("Yield_Curve_Spread_10Y_2Y.csv", "spread_10Y_minus_2Y"),
    "us10y":        ("US_10_years_bonds_daily.csv",                           "close"),
    "us 10y":       ("US_10_years_bonds_daily.csv",                           "close"),
    "10y":          ("US_10_years_bonds_daily.csv",                           "close"),
    "bonds":        ("US_10_years_bonds_daily.csv",                           "close"),
    # Breadth
    "advance-decline": ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
    "advance decline": ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
    "adv-dec":         ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
    "adv dec":         ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
}

_HC_SUBJECTS = {
    "spx":  "SPX_daily.csv",
    "spy":  "SPY_daily.csv",
    "qqq":  "QQQ_daily.csv",
    "iwm":  "IWM_daily.csv",
    "aapl": "AAPL.csv",
    "aaoi": "AAOI.csv",
}

# ─── Timezone conversion ──────────────────────────────────────────────────

_TZ_PARIS = pytz.timezone("Europe/Paris")
_TZ_NY    = pytz.timezone("America/New_York")

_PARIS_FILES = {
    "DAX40_daily.csv", "FTSE100_daily.csv", "NIKKEI225_daily.csv",
    "Gold_daily.csv", "Gold_1hour.csv", "DXY_daily.csv",
    "OANDA_USB02YUSD, 1D.csv", "OANDA_USB10YUSD, 1D.csv",
    "Yield_Curve_Spread_10Y_2Y.csv",
}

_TICK_OFFSET = pd.Timedelta("1h30min")
_TICK_FILE   = "TICK_4hours.csv"

# Intraday / special files to skip in dynamic scan
_SKIP_RE = re.compile(
    r"_1min\.csv$|_5min\.csv$|_30min\.csv$|_1hour\.csv$|_4hours\.csv$"
    r"|option_chain|calendar_events",
    re.IGNORECASE,
)


def paris_to_ny(df: pd.DataFrame) -> pd.DataFrame:
    """Convert naive Paris (CET/CEST) timestamps → NY (ET). No-op if date-only."""
    if df["time"].dt.tz is not None:
        return df
    if df["time"].dt.hour.max() == 0:
        return df
    try:
        df = df.copy()
        df["time"] = (
            df["time"]
            .dt.tz_localize(_TZ_PARIS, ambiguous="NaT", nonexistent="NaT")
            .dt.tz_convert(_TZ_NY)
            .dt.tz_localize(None)
        )
    except Exception:
        pass
    return df


def tick_correction(df: pd.DataFrame) -> pd.DataFrame:
    """Apply +1h30 to TICK timestamps (known export offset vs real NY time)."""
    df = df.copy()
    df["time"] = df["time"] + _TICK_OFFSET
    return df

# ─── Dynamic registry ─────────────────────────────────────────────────────

def _ticker_from_path(path: Path) -> str:
    """Extract normalized ticker from CSV filename, stripping frequency suffixes."""
    name = path.stem
    name = re.sub(r"_daily$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[,\s]+1D$", "", name, flags=re.IGNORECASE)
    return re.sub(r"[\s_]+", "_", name.lower().strip()).strip("_")


def _peek_columns(path: Path) -> list:
    """Read only the header line of a CSV file."""
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            header = f.readline().strip().lstrip("\ufeff")  # strip BOM
        return [c.strip().lower() for c in header.split(";")]
    except Exception:
        return []


@st.cache_data
def _build_dynamic_registry() -> tuple:
    """
    Scan DATA_DIR for daily CSVs.
    Returns (cond_dict, subj_dict):
      cond_dict: {ticker: (filename, value_col)}
      subj_dict: {ticker: filename}  — only files with open+close
    """
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
        # Choose value column for conditions: prefer close, then open, then first non-time
        val_col = next(
            (c for c in ("close", "open") if c in cols),
            next((c for c in cols if c != "time"), None),
        )
        if val_col is None:
            continue
        cond[ticker] = (path.name, val_col)
        if "open" in cols and "close" in cols:
            subj[ticker] = path.name
    return cond, subj


@st.cache_data
def get_effective_registries() -> tuple:
    """
    Merge dynamic registry with hardcoded (HC takes priority for aliases).
    Returns (eff_conditions, eff_subjects).
    """
    dyn_cond, dyn_subj = _build_dynamic_registry()
    return {**dyn_cond, **_HC_CONDITIONS}, {**dyn_subj, **_HC_SUBJECTS}

# ─── Data loading ─────────────────────────────────────────────────────────

@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    if path.name in _PARIS_FILES:
        df = paris_to_ny(df)
    if path.name == _TICK_FILE:
        df = tick_correction(df)
    return df


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.astype(str).str.replace(",", ".").str.strip(), errors="coerce"
    )


def get_condition_series(asset_key: str):
    eff_cond, _ = get_effective_registries()
    key = asset_key.lower().strip()
    if key not in eff_cond:
        return None
    fname, col = eff_cond[key]
    path = DATA_DIR / fname
    if not path.exists():
        return None
    df = load_csv(path)
    col_l = col.lower().replace(" ", "_")
    if col_l not in df.columns:
        for c in df.columns:
            if c != "time":
                col_l = c
                break
    df[col_l] = _to_numeric(df[col_l])
    return df.set_index("time")[col_l].dropna()


def get_subject_df(asset_key: str):
    _, eff_subj = get_effective_registries()
    key = asset_key.lower().strip()
    if key not in eff_subj:
        return None
    path = DATA_DIR / eff_subj[key]
    if not path.exists():
        return None
    df = load_csv(path)
    for col in ("open", "close"):
        if col in df.columns:
            df[col] = _to_numeric(df[col])
    return df.set_index("time")

# ─── Query parsing ────────────────────────────────────────────────────────

_WEEKDAYS = {
    "lundi": 0,    "lundis": 0,
    "mardi": 1,    "mardis": 1,
    "mercredi": 2, "mercredis": 2,
    "jeudi": 3,    "jeudis": 3,
    "vendredi": 4, "vendredis": 4,
    "samedi": 5,   "samedis": 5,
    "dimanche": 6, "dimanches": 6,
    "monday": 0,   "tuesday": 1,   "wednesday": 2,
    "thursday": 3, "friday": 4,    "saturday": 5, "sunday": 6,
}

_OVERNIGHT_POS_RE = re.compile(
    r"ouvre\s+en\s+positif|ouverture\s+positive|ouverture\s+en\s+hausse"
    r"|overnight\s+positif|gap[\s-]?up|open\s+sup[eé]rieur|ouvre\s+en\s+hausse"
    r"|gap\s+haussier",
    re.IGNORECASE,
)

_OVERNIGHT_NEG_RE = re.compile(
    r"ouvre\s+en\s+n[eé]gatif|ouverture\s+n[eé]gative|ouverture\s+en\s+baisse"
    r"|overnight\s+n[eé]gatif|gap[\s-]?down|open\s+inf[eé]rieur|ouvre\s+en\s+baisse"
    r"|gap\s+baissier",
    re.IGNORECASE,
)

_KEYWORD_RE = re.compile(
    r"\b(?:quand|si|when|if|avec|après|apres|sur|pour)\b",
    re.IGNORECASE,
)


def _detect_weekday(q: str):
    """Requires explicit 'les/le/the' prefix to avoid false positives."""
    for name, day in _WEEKDAYS.items():
        if re.search(rf"\b(?:les?|the)\s+{name}\b", q, re.IGNORECASE):
            return day
    return None


def _detect_subject(q: str, eff_subj: dict) -> str:
    """
    Subject = first recognized asset BEFORE keywords like 'quand/si/when/if'.
    Falls back to full query search, then defaults to 'spx'.
    """
    m = _KEYWORD_RE.search(q)
    prefix = q[: m.start()] if m else q
    for s in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", prefix, re.IGNORECASE):
            return s
    # Fallback: full query
    for s in sorted(eff_subj, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", q, re.IGNORECASE):
            return s
    return "spx"


def _detect_overnight(q: str, eff_subj: dict):
    """
    Returns {"direction": "positive"|"negative", "asset": str|None} or None.
    Detects optional asset before the overnight keyword: "AAPL ouvre en positif".
    """
    if _OVERNIGHT_POS_RE.search(q):
        direction = "positive"
    elif _OVERNIGHT_NEG_RE.search(q):
        direction = "negative"
    else:
        return None
    # Optional: detect which asset the overnight applies to
    overnight_asset = None
    for ticker in sorted(eff_subj, key=len, reverse=True):
        if re.search(
            rf"\b{re.escape(ticker)}\b\s+(?:ouvre|ouverture|open|gap)",
            q, re.IGNORECASE,
        ):
            overnight_asset = ticker
            break
    return {"direction": direction, "asset": overnight_asset}


def _parse_single_condition(chunk: str, eff_cond: dict):
    """Extract (asset_key, op, threshold) from a query fragment, or None.
    Requires ASSET immediately followed by OP NUMBER (whitespace only between them).
    """
    for asset in sorted(eff_cond, key=len, reverse=True):
        pat = re.escape(asset) + r"\s*(>=|<=|>|<|=)\s*([\d.,]+)"
        m = re.search(pat, chunk, re.IGNORECASE)
        if m:
            return asset, m.group(1), float(m.group(2).replace(",", "."))
    return None


def parse_query(query: str):
    """
    Returns {subject, conditions, weekday, overnight} or None on failure.

    Examples:
      "SPX quand VIX1D/VIX > 1.2"
      "SPX quand VIX > 18 ET VIX1D/VIX > 1.2 les lundis"
      "QQQ si VIX < 20 et ouverture positive"
      "SPX quand AAPL ouvre en négatif"
      "AAOI quand AAOI > 50"
    """
    eff_cond, eff_subj = get_effective_registries()
    q = query.strip()

    subject   = _detect_subject(q, eff_subj)
    overnight = _detect_overnight(q, eff_subj)
    weekday   = _detect_weekday(q)

    chunks = re.split(r"\s+(?:ET|AND)\s+", q, flags=re.IGNORECASE)
    conditions = []
    for chunk in chunks:
        result = _parse_single_condition(chunk, eff_cond)
        if result:
            asset, op, threshold = result
            conditions.append({"asset": asset, "op": op, "threshold": threshold})

    if not conditions and overnight is None:
        return None

    return {
        "subject":    subject,
        "conditions": conditions,
        "weekday":    weekday,
        "overnight":  overnight,
    }

# ─── Filters & stats ──────────────────────────────────────────────────────

def _apply_op(series: pd.Series, op: str, threshold: float) -> pd.Series:
    ops = {
        ">":  series > threshold,
        "<":  series < threshold,
        ">=": series >= threshold,
        "<=": series <= threshold,
        "=":  series == threshold,
    }
    return ops.get(op, pd.Series(False, index=series.index))


def overnight_dates(df: pd.DataFrame, direction: str = "positive") -> set:
    """
    Dates where open[J] > close[J-1] (positive) or open[J] < close[J-1] (negative).
    Works on any daily DataFrame with 'open' and 'close' columns.
    """
    d = df[["open", "close"]].dropna().sort_index().copy()
    d["prev_close"] = d["close"].shift(1)
    d = d.dropna(subset=["prev_close"])
    mask = d["open"] > d["prev_close"] if direction == "positive" else d["open"] < d["prev_close"]
    return set(d[mask].index.normalize())


def compute_stats(subject_df: pd.DataFrame, valid_dates: set) -> dict:
    subj = subject_df.copy()
    subj.index = subj.index.normalize()
    df = subj[subj.index.isin(valid_dates)].dropna(subset=["open", "close"])
    if df.empty:
        return {}
    df["var_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    n         = len(df)
    bull      = int((df["var_pct"] > 0).sum())
    bear      = int((df["var_pct"] < 0).sum())
    best_idx  = df["var_pct"].idxmax()
    worst_idx = df["var_pct"].idxmin()
    return {
        "n":          n,
        "mean_var":   float(df["var_pct"].mean()),
        "pct_bull":   bull / n * 100,
        "pct_bear":   bear / n * 100,
        "best_date":  best_idx.strftime("%Y-%m-%d"),
        "best_val":   float(df.loc[best_idx, "var_pct"]),
        "worst_date": worst_idx.strftime("%Y-%m-%d"),
        "worst_val":  float(df.loc[worst_idx, "var_pct"]),
        "df":         df,
    }

# ─── Analysis renderer ────────────────────────────────────────────────────

_WEEKDAY_LABELS = {
    0: "lundis", 1: "mardis", 2: "mercredis",
    3: "jeudis", 4: "vendredis", 5: "samedis", 6: "dimanches",
}


def run_analysis(query: str) -> None:
    parsed = parse_query(query)
    if parsed is None:
        st.error(
            "Requête non reconnue. Exemples :\n"
            "- `SPX quand VIX1D/VIX > 1.2`\n"
            "- `SPX quand VIX > 18 ET VIX1D/VIX > 1.2`\n"
            "- `QQQ si VIX < 20 les lundis`\n"
            "- `SPX quand VIX > 18 et ouverture positive`\n"
            "- `SPX quand AAPL ouvre en négatif`"
        )
        return

    subject    = parsed["subject"]
    conditions = parsed["conditions"]
    weekday    = parsed["weekday"]
    overnight  = parsed["overnight"]

    subject_df = get_subject_df(subject)
    if subject_df is None:
        st.error(f"Fichier introuvable pour le sujet : **{subject}**")
        return

    # Build intersection of valid dates starting from subject dates
    valid_dates = set(subject_df.dropna(subset=["open", "close"]).index.normalize())

    for cond in conditions:
        series = get_condition_series(cond["asset"])
        if series is None:
            st.error(f"Fichier introuvable pour la condition : **{cond['asset']}**")
            return
        mask = _apply_op(series, cond["op"], cond["threshold"])
        valid_dates &= set(series[mask].index.normalize())

    if overnight:
        ov_asset = overnight["asset"] or subject
        ov_df = get_subject_df(ov_asset)
        if ov_df is None:
            st.error(f"Données overnight introuvables pour : **{ov_asset}**")
            return
        valid_dates &= overnight_dates(ov_df, overnight["direction"])

    if weekday is not None:
        valid_dates = {d for d in valid_dates if d.weekday() == weekday}

    stats = compute_stats(subject_df, valid_dates)
    if not stats:
        st.warning("Aucun jour ne correspond à cette combinaison de conditions.")
        return

    # ── Build display title
    cond_str = " ET ".join(
        f"{c['asset'].upper()} {c['op']} {c['threshold']}" for c in conditions
    )
    flags = []
    if overnight:
        ov_label  = (overnight["asset"] or subject).upper() + " "
        dir_label = "positive" if overnight["direction"] == "positive" else "négative"
        flags.append(f"{ov_label}ouverture {dir_label} vs veille")
    if weekday is not None:
        flags.append(f"les {_WEEKDAY_LABELS[weekday]}")
    if flags:
        sep = "  ·  "
        cond_str = (cond_str + sep if cond_str else "") + sep.join(flags)

    n     = stats["n"]
    total = len(subject_df.dropna(subset=["open", "close"]))
    pct   = n / total * 100 if total else 0

    st.markdown(f"### {subject.upper()} — {cond_str}")
    st.caption(f"{n} jours sur {total} ({pct:.1f}% de l'historique)")
    st.markdown("")

    # ── 5 stat cards
    c1, c2, c3, c4, c5 = st.columns(5)
    sign = "+" if stats["mean_var"] >= 0 else ""
    c1.metric("Variation moy. open→close", f"{sign}{stats['mean_var']:.2f}%")
    c2.metric("Jours haussiers",           f"{stats['pct_bull']:.1f}%")
    c3.metric("Jours baissiers",           f"{stats['pct_bear']:.1f}%")
    c4.metric("Meilleur jour", f"+{stats['best_val']:.2f}%",
              delta=stats["best_date"], delta_color="off")
    c5.metric("Pire jour",    f"{stats['worst_val']:.2f}%",
              delta=stats["worst_date"], delta_color="off")

    st.markdown("---")

    # ── Bar chart (vert = hausse, rouge = baisse)
    var = stats["df"]["var_pct"].copy()
    var.index = var.index.strftime("%Y-%m-%d")
    chart_df = pd.DataFrame({
        "Hausse (%)": var.where(var >= 0),
        "Baisse (%)": var.where(var < 0),
    })
    st.bar_chart(chart_df, color=["#26a269", "#e01b24"],
                 height=300, use_container_width=True)

    # ── Raw condition series (expanders)
    for cond in conditions:
        series = get_condition_series(cond["asset"])
        if series is None:
            continue
        with st.expander(f"Série brute : {cond['asset'].upper()}", expanded=False):
            ctx = series.reset_index()
            ctx.columns = ["Date", cond["asset"].upper()]
            ctx["Date"] = ctx["Date"].dt.strftime("%Y-%m-%d")
            st.dataframe(ctx.sort_values("Date", ascending=False),
                         use_container_width=True, height=260)

# ─── UI ───────────────────────────────────────────────────────────────────

st.markdown(
    f'<div style="position:fixed;top:0.55rem;right:1.1rem;'
    f'color:#999;font-size:0.68rem;z-index:9999;pointer-events:none">'
    f'{VERSION}</div>',
    unsafe_allow_html=True,
)

st.markdown("## SPX Quant Engine")

# Build registry once for UI display
_eff_cond, _eff_subj = get_effective_registries()
_n_subj = len(_eff_subj)
_n_cond = len(_eff_cond)

st.markdown(
    f"<div style='color:#888;font-size:0.8rem;margin-top:-0.8rem;margin-bottom:0.5rem'>"
    f"<b>{_n_subj}</b> actifs sujets · <b>{_n_cond}</b> actifs conditions détectés automatiquement"
    f"&nbsp;|&nbsp; Filtres : jour de semaine · ouverture +/- vs veille · multi-ET"
    f"</div>",
    unsafe_allow_html=True,
)

with st.expander("Actifs disponibles", expanded=False):
    col_s, col_c = st.columns(2)
    with col_s:
        st.markdown("**Sujets**")
        st.markdown("  ".join(f"`{a}`" for a in sorted(_eff_subj)))
    with col_c:
        st.markdown("**Conditions (extrait)**")
        shown = sorted(_eff_cond)[:40]
        st.markdown("  ".join(f"`{a}`" for a in shown)
                    + (f"  … +{_n_cond - 40} autres" if _n_cond > 40 else ""))

query = st.text_input(
    label="query",
    value=DEFAULT_Q,
    label_visibility="collapsed",
    placeholder=(
        "ex: SPX quand VIX > 18 ET VIX1D/VIX > 1.2 les lundis  |  "
        "AAPL quand AAPL ouvre en négatif  |  QQQ si VIX < 20"
    ),
)

st.markdown("")
run_analysis(query)
