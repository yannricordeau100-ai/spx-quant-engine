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

# ─── Asset registry ───────────────────────────────────────────────────────

CONDITION_ASSETS = {
    # VIX family (longest keys first avoids "vix" eating "vix3m" etc.)
    "vix1d/vix":    ("VIX1D_VIX_ratio_daily.csv",                          "open"),
    "vix1d_vix":    ("VIX1D_VIX_ratio_daily.csv",                          "open"),
    "vix9d":        ("VIX9D_daily.csv",                                     "close"),
    "vix3m":        ("VIX3M_daily.csv",                                     "close"),
    "vix6m":        ("VIX6M_daily.csv",                                     "close"),
    "vvix":         ("VVIX_daily.csv",                                      "close"),
    "vix":          ("VIX_daily.csv",                                       "close"),
    # Sentiment
    "skew":         ("SKEW_INDEX_daily.csv",                                "close"),
    # Macro
    "dxy":          ("DXY_daily.csv",                                       "close"),
    "gold":         ("Gold_daily.csv",                                      "close"),
    "nikkei":       ("NIKKEI225_daily.csv",                                 "close"),
    "dax":          ("DAX40_daily.csv",                                     "close"),
    "ftse":         ("FTSE100_daily.csv",                                   "close"),
    # Put-Call ratios
    "spx put-call": ("SPX_Put_Call_Ratio_daily.csv",                        "close"),
    "spx put/call": ("SPX_Put_Call_Ratio_daily.csv",                        "close"),
    "spx pcr":      ("SPX_Put_Call_Ratio_daily.csv",                        "close"),
    "qqq put-call": ("QQQ_Put_Call_Ratio_daily.csv",                        "close"),
    "qqq put/call": ("QQQ_Put_Call_Ratio_daily.csv",                        "close"),
    "qqq pcr":      ("QQQ_Put_Call_Ratio_daily.csv",                        "close"),
    "spy put-call": ("SPY_Put_Call_Ratio_daily.csv",                        "close"),
    "spy put/call": ("SPY_Put_Call_Ratio_daily.csv",                        "close"),
    "spy pcr":      ("SPY_Put_Call_Ratio_daily.csv",                        "close"),
    "iwm put-call": ("IWM_Put_Call_Ratio_daily.csv",                        "close"),
    "iwm put/call": ("IWM_Put_Call_Ratio_daily.csv",                        "close"),
    "iwm pcr":      ("IWM_Put_Call_Ratio_daily.csv",                        "close"),
    "vix put-call": ("VIX_Put_Call_Ratio_daily.csv",                        "close"),
    "vix put/call": ("VIX_Put_Call_Ratio_daily.csv",                        "close"),
    "vix pcr":      ("VIX_Put_Call_Ratio_daily.csv",                        "close"),
    # Rates
    "yield curve":  ("Yield_Curve_Spread_10Y_2Y.csv",  "spread_10Y_minus_2Y"),
    "yield_curve":  ("Yield_Curve_Spread_10Y_2Y.csv",  "spread_10Y_minus_2Y"),
    "us10y":        ("US_10_years_bonds_daily.csv",                         "close"),
    "us 10y":       ("US_10_years_bonds_daily.csv",                         "close"),
    "10y":          ("US_10_years_bonds_daily.csv",                         "close"),
    "bonds":        ("US_10_years_bonds_daily.csv",                         "close"),
    # Breadth
    "advance-decline": ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
    "advance decline": ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
    "adv-dec":         ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
    "adv dec":         ("advance_decline_ratio_net_ratio_put_call_daily.csv", "close"),
}

SUBJECT_ASSETS = {
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

# Files exported in Paris local time (CET/CEST)
_PARIS_FILES = {
    "DAX40_daily.csv", "FTSE100_daily.csv", "NIKKEI225_daily.csv",
    "Gold_daily.csv", "Gold_1hour.csv", "DXY_daily.csv",
    "OANDA_USB02YUSD, 1D.csv", "OANDA_USB10YUSD, 1D.csv",
    "Yield_Curve_Spread_10Y_2Y.csv",
}

# TICK has a known +1h30 export offset vs actual NY session time
_TICK_OFFSET = pd.Timedelta("1h30min")
_TICK_FILE   = "TICK_4hours.csv"


def paris_to_ny(df: pd.DataFrame) -> pd.DataFrame:
    """Convert naive Paris (CET/CEST) timestamps → NY (ET). No-op if already date-only."""
    if df["time"].dt.tz is not None:
        return df
    if df["time"].dt.hour.max() == 0:
        return df  # date-only rows, no intraday offset to apply
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
    """Apply +1h30 offset to TICK timestamps (known export offset vs real NY time)."""
    df = df.copy()
    df["time"] = df["time"] + _TICK_OFFSET
    return df

# ─── Data loading ─────────────────────────────────────────────────────────

@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    # Apply timezone corrections for Paris-exported files
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
    key = asset_key.lower().strip()
    if key not in CONDITION_ASSETS:
        return None
    fname, col = CONDITION_ASSETS[key]
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
    key = asset_key.lower().strip()
    if key not in SUBJECT_ASSETS:
        return None
    path = DATA_DIR / SUBJECT_ASSETS[key]
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

_OVERNIGHT_RE = re.compile(
    r"ouvre\s+en\s+positif|ouverture\s+positive|ouverture\s+en\s+hausse"
    r"|overnight\s+positif|gap\s+up|open\s+sup[eé]rieur",
    re.IGNORECASE,
)


def _detect_weekday(q: str):
    for name, day in _WEEKDAYS.items():
        if re.search(rf"\b{name}\b", q, re.IGNORECASE):
            return day
    return None


def _parse_single_condition(chunk: str):
    """Extract (asset_key, op, threshold) from a query fragment, or None."""
    for asset in sorted(CONDITION_ASSETS, key=len, reverse=True):
        if re.search(re.escape(asset), chunk, re.IGNORECASE):
            m = re.search(r"(>=|<=|>|<|=)\s*([\d.,]+)", chunk)
            if m:
                op = m.group(1)
                threshold = float(m.group(2).replace(",", "."))
                return asset, op, threshold
    return None


def parse_query(query: str):
    """
    Returns {
        "subject":    str,
        "conditions": list of {"asset": str, "op": str, "threshold": float},
        "weekday":    int | None,   # 0=Mon…6=Sun
        "overnight":  bool,
    } or None on failure.

    Examples:
      "SPX quand VIX1D/VIX > 1.2"
      "SPX quand VIX > 18 ET VIX1D/VIX > 1.2 les lundis"
      "QQQ si VIX < 20 et ouverture positive"
    """
    q = query.strip()

    # Subject
    subject = "spx"
    for s in sorted(SUBJECT_ASSETS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", q, re.IGNORECASE):
            subject = s
            break

    # Flags
    overnight = bool(_OVERNIGHT_RE.search(q))
    weekday = _detect_weekday(q)

    # Multi-condition split on " ET " / " AND " (case-insensitive)
    # We only split on standalone ET/AND between numeric conditions; "overnight" uses "en" not "ET"
    chunks = re.split(r"\s+(?:ET|AND)\s+", q, flags=re.IGNORECASE)

    conditions = []
    for chunk in chunks:
        result = _parse_single_condition(chunk)
        if result:
            asset, op, threshold = result
            conditions.append({"asset": asset, "op": op, "threshold": threshold})

    if not conditions:
        return None

    return {
        "subject":    subject,
        "conditions": conditions,
        "weekday":    weekday,
        "overnight":  overnight,
    }

# ─── Filters & stats ──────────────────────────────────────────────────────

def _apply_op(series: pd.Series, op: str, threshold: float) -> pd.Series:
    ops = {">": series > threshold, "<": series < threshold,
           ">=": series >= threshold, "<=": series <= threshold,
           "=": series == threshold}
    return ops.get(op, pd.Series(False, index=series.index))


def overnight_dates(subject_df: pd.DataFrame) -> set:
    """Dates where open[J] > close[J-1]."""
    df = subject_df[["open", "close"]].dropna().sort_index().copy()
    df["prev_close"] = df["close"].shift(1)
    return set(df[df["open"] > df["prev_close"]].index.normalize())


def compute_stats(subject_df: pd.DataFrame, valid_dates: set) -> dict:
    subj = subject_df.copy()
    subj.index = subj.index.normalize()
    df = subj[subj.index.isin(valid_dates)].dropna(subset=["open", "close"])
    if df.empty:
        return {}
    df["var_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    n = len(df)
    bull = int((df["var_pct"] > 0).sum())
    bear = int((df["var_pct"] < 0).sum())
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
            "- `SPX quand VIX > 18 et ouverture positive`"
        )
        return

    subject    = parsed["subject"]
    conditions = parsed["conditions"]
    weekday    = parsed["weekday"]
    overnight  = parsed["overnight"]

    subject_df = get_subject_df(subject)
    if subject_df is None:
        st.error(f"Fichier introuvable pour : **{subject}**")
        return

    # Build intersection of valid dates
    valid_dates = set(subject_df.dropna(subset=["open", "close"]).index.normalize())

    for cond in conditions:
        series = get_condition_series(cond["asset"])
        if series is None:
            st.error(f"Fichier introuvable pour : **{cond['asset']}**")
            return
        mask = _apply_op(series, cond["op"], cond["threshold"])
        valid_dates &= set(series[mask].index.normalize())

    if overnight:
        valid_dates &= overnight_dates(subject_df)

    if weekday is not None:
        valid_dates = {d for d in valid_dates if d.weekday() == weekday}

    stats = compute_stats(subject_df, valid_dates)
    if not stats:
        st.warning("Aucun jour ne correspond à cette combinaison de conditions.")
        return

    # ── Build title
    cond_str = " ET ".join(
        f"{c['asset'].upper()} {c['op']} {c['threshold']}" for c in conditions
    )
    flags = []
    if overnight:
        flags.append("ouverture positive vs veille")
    if weekday is not None:
        flags.append(f"les {_WEEKDAY_LABELS[weekday]}")
    if flags:
        cond_str += "  ·  " + "  ·  ".join(flags)

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

    # ── Bar chart
    chart_df = (
        stats["df"][["var_pct"]]
        .rename(columns={"var_pct": "Variation open→close (%)"})
    )
    chart_df.index = chart_df.index.strftime("%Y-%m-%d")
    st.bar_chart(chart_df, height=300, width="stretch")

    # ── Raw condition series (expander)
    for cond in conditions:
        series = get_condition_series(cond["asset"])
        if series is None:
            continue
        with st.expander(f"Série brute : {cond['asset'].upper()}", expanded=False):
            ctx = series.reset_index()
            ctx.columns = ["Date", cond["asset"].upper()]
            ctx["Date"] = ctx["Date"].dt.strftime("%Y-%m-%d")
            st.dataframe(ctx.sort_values("Date", ascending=False),
                         width="stretch", height=260)

# ─── UI ───────────────────────────────────────────────────────────────────

st.markdown(
    f'<div style="position:fixed;top:0.55rem;right:1.1rem;'
    f'color:#999;font-size:0.68rem;z-index:9999;pointer-events:none">'
    f'{VERSION}</div>',
    unsafe_allow_html=True,
)

st.markdown("## SPX Quant Engine")
st.markdown(
    "<div style='color:#888;font-size:0.8rem;margin-top:-0.8rem;margin-bottom:1rem'>"
    "Conditions : VIX · VIX1D/VIX · VIX9D · VIX3M · VIX6M · VVIX · SKEW · "
    "DXY · Gold · Nikkei · DAX · FTSE · SPX/QQQ/SPY/IWM/VIX PCR · "
    "US10Y · Yield Curve · Advance-Decline &nbsp;|&nbsp; "
    "Filtres : jour de semaine · ouverture positive vs veille · multi-ET"
    "</div>",
    unsafe_allow_html=True,
)

query = st.text_input(
    label="query",
    value=DEFAULT_Q,
    label_visibility="collapsed",
    placeholder=(
        "ex: SPX quand VIX > 18 ET VIX1D/VIX > 1.2 les lundis  |  "
        "QQQ si VIX < 20 et ouverture positive"
    ),
)

st.markdown("")
run_analysis(query)
