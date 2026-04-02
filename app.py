# SPX_QUANT_ENGINE | v2.0
import re
from pathlib import Path
import pandas as pd
import streamlit as st

st.set_page_config(page_title="SPX Quant Engine", layout="wide")

VERSION = "v2.0"
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "live_selected"
DEFAULT_Q = "SPX quand VIX1D/VIX > 1.2"

# ─── Asset registry ───────────────────────────────────────────────────────
# Keys sorted longest-first at query-parse time to prevent "vix" eating "vix3m"

CONDITION_ASSETS = {
    # VIX family
    "vix1d/vix":    ("VIX1D_VIX_ratio_daily.csv",                          "open"),
    "vix1d_vix":    ("VIX1D_VIX_ratio_daily.csv",                          "open"),
    "vix9d":        ("VIX9D_daily.csv",                                     "close"),
    "vix3m":        ("VIX3M_daily.csv",                                     "close"),
    "vix6m":        ("VIX6M_daily.csv",                                     "close"),
    "vvix":         ("VVIX_daily.csv",                                      "close"),
    "vix":          ("VIX_daily.csv",                                       "close"),
    # Sentiment / structure
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

# ─── Data loading ─────────────────────────────────────────────────────────

@st.cache_data
def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=";")
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    return df


def _to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.astype(str).str.replace(",", ".").str.strip(), errors="coerce")


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
        # fallback: first non-time column
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

# ─── Query parser ─────────────────────────────────────────────────────────

def parse_query(query: str):
    """
    Returns (subject, cond_asset, op, threshold) or None.
    Handles e.g. "SPX quand VIX1D/VIX > 1.2", "QQQ si VIX < 20", "AAPL SKEW >= 145"
    """
    q = query.strip()

    # subject (longest key first, word-boundary match)
    subject = "spx"
    for s in sorted(SUBJECT_ASSETS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(s)}\b", q, re.IGNORECASE):
            subject = s
            break

    # condition asset (longest key first, substring match)
    cond_asset = None
    for asset in sorted(CONDITION_ASSETS, key=len, reverse=True):
        if re.search(re.escape(asset), q, re.IGNORECASE):
            cond_asset = asset
            break
    if cond_asset is None:
        return None

    # operator + threshold
    m = re.search(r"(>=|<=|>|<|=)\s*([\d.,]+)", q)
    if not m:
        return None
    op = m.group(1)
    threshold = float(m.group(2).replace(",", "."))

    return subject, cond_asset, op, threshold

# ─── Stats ────────────────────────────────────────────────────────────────

def compute_stats(subject_df: pd.DataFrame, cond_dates: pd.DatetimeIndex) -> dict:
    """Filter subject_df to days matching cond_dates (date-only match) and compute stats."""
    subj_norm = subject_df.index.normalize()
    cond_norm = cond_dates.normalize()
    df = subject_df[subj_norm.isin(cond_norm)].copy()
    df = df.dropna(subset=["open", "close"])
    if df.empty:
        return {}
    df["var_pct"] = (df["close"] - df["open"]) / df["open"] * 100
    n = len(df)
    bull = int((df["var_pct"] > 0).sum())
    bear = int((df["var_pct"] < 0).sum())
    best_idx = df["var_pct"].idxmax()
    worst_idx = df["var_pct"].idxmin()
    return {
        "n": n,
        "mean_var": float(df["var_pct"].mean()),
        "pct_bull": bull / n * 100,
        "pct_bear": bear / n * 100,
        "best_date": best_idx.strftime("%Y-%m-%d"),
        "best_val": float(df.loc[best_idx, "var_pct"]),
        "worst_date": worst_idx.strftime("%Y-%m-%d"),
        "worst_val": float(df.loc[worst_idx, "var_pct"]),
        "df": df,
    }

# ─── Analysis renderer ────────────────────────────────────────────────────

def run_analysis(query: str) -> None:
    parsed = parse_query(query)
    if parsed is None:
        st.error("Requête non reconnue. Exemple : `SPX quand VIX1D/VIX > 1.2`")
        return

    subject, cond_asset, op, threshold = parsed
    cond_series = get_condition_series(cond_asset)
    subject_df = get_subject_df(subject)

    if cond_series is None:
        st.error(f"Fichier introuvable pour : **{cond_asset}**")
        return
    if subject_df is None:
        st.error(f"Fichier introuvable pour : **{subject}**")
        return

    ops_map = {
        ">":  cond_series > threshold,
        "<":  cond_series < threshold,
        ">=": cond_series >= threshold,
        "<=": cond_series <= threshold,
        "=":  cond_series == threshold,
    }
    mask = ops_map.get(op, pd.Series(False, index=cond_series.index))
    filtered_dates = cond_series[mask].index

    stats = compute_stats(subject_df, filtered_dates)
    if not stats:
        st.warning("Aucun jour ne correspond à cette condition (ou pas de données communes).")
        return

    n = stats["n"]
    total = len(subject_df.dropna(subset=["open", "close"]))
    pct_hist = n / total * 100 if total > 0 else 0

    # ── Title
    st.markdown(f"### {subject.upper()} — {cond_asset.upper()} {op} {threshold}")
    st.caption(f"{n} jours sur {total} disponibles ({pct_hist:.1f}% de l'historique)")

    st.markdown("")

    # ── 5 stat cards
    c1, c2, c3, c4, c5 = st.columns(5)

    sign = "+" if stats["mean_var"] >= 0 else ""
    c1.metric(
        label="Variation moy. open→close",
        value=f"{sign}{stats['mean_var']:.2f}%",
    )
    c2.metric(
        label="Jours haussiers",
        value=f"{stats['pct_bull']:.1f}%",
    )
    c3.metric(
        label="Jours baissiers",
        value=f"{stats['pct_bear']:.1f}%",
    )
    c4.metric(
        label="Meilleur jour",
        value=f"+{stats['best_val']:.2f}%",
        delta=stats["best_date"],
        delta_color="off",
    )
    c5.metric(
        label="Pire jour",
        value=f"{stats['worst_val']:.2f}%",
        delta=stats["worst_date"],
        delta_color="off",
    )

    st.markdown("---")

    # ── Bar chart: variation open→close on each matched day
    chart_df = (
        stats["df"][["var_pct"]]
        .rename(columns={"var_pct": "Variation open→close (%)"})
    )
    chart_df.index = chart_df.index.strftime("%Y-%m-%d")
    st.bar_chart(chart_df, height=300, use_container_width=True)

    # ── Condition series context
    with st.expander(f"Série brute : {cond_asset.upper()}", expanded=False):
        ctx = cond_series.reset_index()
        ctx.columns = ["Date", cond_asset.upper()]
        ctx["Date"] = ctx["Date"].dt.strftime("%Y-%m-%d")
        st.dataframe(ctx.sort_values("Date", ascending=False), use_container_width=True, height=260)

# ─── UI layout ────────────────────────────────────────────────────────────

# Discrete version tag fixed top-right
st.markdown(
    f'<div style="position:fixed;top:0.55rem;right:1.1rem;'
    f'color:#999;font-size:0.68rem;z-index:9999;pointer-events:none">'
    f'{VERSION}</div>',
    unsafe_allow_html=True,
)

st.markdown("## SPX Quant Engine")

st.markdown(
    "<div style='color:#888;font-size:0.8rem;margin-top:-0.8rem;margin-bottom:1rem'>"
    "Conditions disponibles : VIX · VIX1D/VIX · VIX9D · VIX3M · VIX6M · VVIX · SKEW · "
    "DXY · Gold · Nikkei · DAX · FTSE · SPX/QQQ/SPY/IWM/VIX PCR · "
    "US10Y · Yield Curve · Advance-Decline"
    "</div>",
    unsafe_allow_html=True,
)

query = st.text_input(
    label="query",
    value=DEFAULT_Q,
    label_visibility="collapsed",
    placeholder="ex: SPX quand VIX1D/VIX > 1.2  |  QQQ si VIX < 20  |  AAPL SKEW >= 145",
)

st.markdown("")
run_analysis(query)
