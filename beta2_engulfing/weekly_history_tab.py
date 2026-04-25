"""
Onglet "Historique BBE — cette semaine"
Liste TOUS les BBE de la semaine (lundi → dimanche) sur l'univers complet.
Détection LIVE depuis les CSV (n'attend pas J+1 disponible) → les signaux
du jour de clôture apparaissent immédiatement.
Colonne "Gardé" indique si le ticker est dans les 45 retenus.
Tri : jour le plus récent en haut.
"""
from __future__ import annotations
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

sys.path.insert(0, "/Users/yann/spx-quant-engine")
from ticker_analysis import _find_ticker_csv, load_earnings_dates, detect_engulfing_strict

BASE = Path(__file__).parent
SIGNALS = BASE / "bbe_signals.csv"
META = BASE / "bbe_meta.csv"
SCREEN = BASE / "beta_gt2_midlarge.csv"
TICKERS_DIR = Path("/Users/yann/spx-quant-engine/data/live_selected/tickers")


@st.cache_data(show_spinner=False)
def _kept_tickers() -> set[str]:
    """Recalcule dynamiquement les 45 tickers gardés (WR≥70%, n≥8)."""
    sig = pd.read_csv(SIGNALS)
    g = sig.groupby("ticker")
    n = g.size()
    wr = g["return_lowmin_j1_pct"].apply(lambda s: (s <= -2.0).mean() * 100).round(1)
    keep = set(n[(wr >= 70.0) & (n >= 8)].index)
    return keep


@st.cache_data(show_spinner=False)
def _load_data():
    sig = pd.read_csv(SIGNALS)
    meta = pd.read_csv(META)
    sig["date_j0"] = pd.to_datetime(sig["date_j0"])
    return sig, meta


def _detect_week_signals(monday: pd.Timestamp, sunday: pd.Timestamp) -> pd.DataFrame:
    """Détecte LIVE les BBE de la semaine sur tout l'univers.
    N'exige pas J+1 — les signaux du jour de clôture apparaissent."""
    tickers = sorted(pd.read_csv(SCREEN)["Ticker"].dropna().astype(str).unique())
    rows = []
    for t in tickers:
        p = _find_ticker_csv(t)
        if not p: continue
        df = pd.read_csv(p, sep=";")
        df.columns = [c.strip().lower() for c in df.columns]
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        for c in ("open","high","low","close"):
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(",",".").str.replace(r"\s+","",regex=True), errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"].astype(str).str.replace(r"\s+","",regex=True), errors="coerce")
        df = df.dropna(subset=["time","open","close"]).sort_values("time").reset_index(drop=True)
        earn = load_earnings_dates(t)
        bear = detect_engulfing_strict(df, pattern="bearish", earnings_dates=earn)
        for _, sig in bear.iterrows():
            d0 = pd.Timestamp(sig["date"]).normalize()
            if monday <= d0 <= sunday:
                # Calcule J+1 si dispo
                idx = df.index[df["time"].dt.normalize() == d0]
                close_j0 = float(sig["close"])
                ret_j1 = lowmin_j5 = None
                if len(idx) and idx[0] + 1 < len(df):
                    c1 = float(df.iloc[idx[0]+1]["close"])
                    ret_j1 = (c1 - close_j0) / close_j0 * 100
                    # lowmin J+1..J+5
                    end = min(idx[0]+5, len(df)-1)
                    if end > idx[0]:
                        lowmin = df.iloc[idx[0]+1:end+1]["low"].min()
                        lowmin_j5 = (lowmin - close_j0) / close_j0 * 100
                rows.append({
                    "ticker": t, "date_j0": d0,
                    "close_j0": close_j0,
                    "return_close_j1_pct": ret_j1,
                    "return_lowmin_j5_pct": lowmin_j5,
                })
    return pd.DataFrame(rows)


def render_weekly_history(key_prefix: str = "wkhist") -> None:
    today = pd.Timestamp.now().normalize()
    monday = today - timedelta(days=today.dayofweek)  # 0=lundi
    sunday = monday + timedelta(days=6)

    st.caption(
        f"Bearish Engulfing détectés du **{monday.date()}** au "
        f"**{sunday.date()}** sur l'univers complet (164). "
        f"Colonne **Gardé ✅** = ticker dans les 45 retenus (WR J+1 ≥ 70%)."
    )

    kept = _kept_tickers()
    _, meta = _load_data()

    # Détection LIVE depuis CSV — inclut signaux du jour sans J+1
    week = _detect_week_signals(monday, sunday)

    if week.empty:
        st.info("Aucun BBE détecté cette semaine.")
        return

    # Colonne flag : ticker dans les 45 gardés ou pas
    week["Gardé"] = week["ticker"].apply(lambda t: "✅" if t in kept else "—")

    # Win rate historique pour contexte
    wr_j1 = sig.groupby("ticker")["return_lowmin_j1_pct"].apply(
        lambda s: (s <= -2.0).mean() * 100
    ).round(1).to_dict()
    wr_j5 = sig.groupby("ticker")["return_lowmin_j5_pct"].apply(
        lambda s: (s.dropna() <= -2.0).mean() * 100
    ).round(1).to_dict()
    company = dict(zip(meta["Ticker"], meta["Company"]))
    mcap = dict(zip(meta["Ticker"], meta["MCap_USD"]))

    def fmt_cap(x):
        if pd.isna(x): return "-"
        if x >= 1e9: return f"{x/1e9:.1f}B"
        if x >= 1e6: return f"{x/1e6:.0f}M"
        return str(int(x))

    week["Date"] = week["date_j0"].dt.strftime("%a %Y-%m-%d")
    week["Company"] = week["ticker"].map(lambda t: (company.get(t) or "")[:32])
    week["MktCap"] = week["ticker"].map(mcap).map(fmt_cap)
    week["WR J+1"] = week["ticker"].map(lambda t: f"{wr_j1.get(t, 0):.1f}%")
    week["WR J+5"] = week["ticker"].map(lambda t: f"{wr_j5.get(t, 0):.1f}%")
    week = week.rename(columns={
        "ticker": "Ticker", "close_j0": "Close J0",
        "return_close_j1_pct": "Var J+1 %", "return_lowmin_j5_pct": "Plus-bas J+5 %",
    })
    week["Var J+1 %"] = week["Var J+1 %"].apply(lambda v: round(v,2) if pd.notna(v) else "—")
    week["Plus-bas J+5 %"] = week["Plus-bas J+5 %"].apply(lambda v: round(v,2) if pd.notna(v) else "—")

    out = week[[
        "Date", "Gardé", "Ticker", "Company", "MktCap", "Close J0",
        "Var J+1 %", "Plus-bas J+5 %", "WR J+1", "WR J+5",
    ]].sort_values(["Date", "Gardé"], ascending=[False, False]).reset_index(drop=True)

    st.markdown(f"### {len(out)} signaux cette semaine")
    st.dataframe(out, use_container_width=True, height=420, hide_index=True)
    st.download_button(
        "📥 Télécharger CSV",
        data=out.to_csv(index=False).encode("utf-8"),
        file_name=f"bbe_week_{monday.date()}_{sunday.date()}.csv",
        mime="text/csv",
        key=f"{key_prefix}_dl",
    )
