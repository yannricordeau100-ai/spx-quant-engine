"""
Onglet "Conditions custom" : pour 1 ticker, sélectionne 2 plages de variation
(J0 et J+1) et affiche les statistiques de cooccurrence.

Logique :
  - var J0 = (close J0 - close J-1) / close J-1 × 100
  - var J+1 = (close J+1 - close J0) / close J0 × 100
  - Trouve tous les jours où var J0 ∈ [seuil_j0_min, seuil_j0_max]
  - Pour ces jours, compte combien ont var J+1 ∈ [seuil_j1_min, seuil_j1_max]
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import streamlit as st

BASE = Path(__file__).parent
SCREEN = BASE / "beta_gt2_midlarge.csv"
TICKERS_DIR = Path("/Users/yann/spx-quant-engine/data/live_selected/tickers")


@st.cache_data(show_spinner=False)
def _load_universe() -> list[str]:
    return sorted(pd.read_csv(SCREEN)["Ticker"].dropna().astype(str).unique())


@st.cache_data(show_spinner=False)
def _load_ohlcv(ticker: str) -> pd.DataFrame | None:
    p = TICKERS_DIR / f"{ticker}.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, sep=";")
    df.columns = [c.strip().lower() for c in df.columns]
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    for c in ("open", "high", "low", "close"):
        df[c] = pd.to_numeric(
            df[c].astype(str).str.replace(",", ".").str.replace(r"\s+", "", regex=True),
            errors="coerce",
        )
    df = df.dropna(subset=["time", "close"]).sort_values("time").reset_index(drop=True)
    df["var_j0_pct"] = df["close"].pct_change() * 100
    df["var_j1_pct"] = df["var_j0_pct"].shift(-1)
    return df


def render_conditions_custom(key_prefix: str = "ccust") -> None:
    st.caption(
        "Sélectionne un ticker, règle 2 plages de variation (J0 et J+1), "
        "et obtiens la statistique de cooccurrence des conditions sur "
        "tout l'historique disponible."
    )

    tickers = _load_universe()
    available = [t for t in tickers if (TICKERS_DIR / f"{t}.csv").exists()]
    missing = len(tickers) - len(available)

    col_t, col_info = st.columns([2, 3])
    with col_t:
        ticker = st.selectbox(
            f"Ticker ({len(available)} dispo)",
            available,
            key=f"{key_prefix}_ticker",
        )
    with col_info:
        if missing:
            st.warning(
                f"{missing} tickers absents (CSV non téléchargé). "
                f"Lance `refresh_tickers_from_ibkr.py` avec Gateway ouvert."
            )

    df = _load_ohlcv(ticker)
    if df is None or len(df) < 5:
        st.error(f"Pas de données pour {ticker}")
        return

    st.markdown("### 🎯 Conditions de cooccurrence")
    col_j0, col_j1 = st.columns(2)
    with col_j0:
        st.markdown(f"**Jour J0** — variation close J-1 → close J0")
        rng_j0 = st.slider(
            "Plage J0 (%)",
            min_value=-30.0, max_value=30.0,
            value=(-7.5, -5.0), step=0.1,
            key=f"{key_prefix}_rng_j0",
            help="2 curseurs : borne min et max de la variation J0 acceptée.",
        )
    with col_j1:
        st.markdown(f"**Jour J+1** — variation close J0 → close J+1")
        rng_j1 = st.slider(
            "Plage J+1 (%)",
            min_value=-30.0, max_value=30.0,
            value=(4.0, 8.3), step=0.1,
            key=f"{key_prefix}_rng_j1",
        )

    # Filtrage
    j0_min, j0_max = rng_j0
    j1_min, j1_max = rng_j1
    valid = df.dropna(subset=["var_j0_pct"]).copy()
    n_total = len(valid)

    mask_j0 = valid["var_j0_pct"].between(j0_min, j0_max)
    valid_j0 = valid[mask_j0].copy()
    n_j0 = len(valid_j0)

    valid_j0_with_j1 = valid_j0.dropna(subset=["var_j1_pct"]).copy()
    n_j0_eval = len(valid_j0_with_j1)
    mask_j1 = valid_j0_with_j1["var_j1_pct"].between(j1_min, j1_max)
    n_both = int(mask_j1.sum())

    st.markdown("### 📊 Résultats")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Sessions analysées", n_total)
    c2.metric("J0 conforme", n_j0,
              f"{n_j0/n_total*100:.1f}% du total" if n_total else "—")
    c3.metric("J0 + J+1 conformes", n_both,
              f"{n_both/n_j0_eval*100:.1f}% des J0 évalués" if n_j0_eval else "—")
    c4.metric("J+1 moyen | J0 ok",
              f"{valid_j0_with_j1['var_j1_pct'].mean():+.2f}%" if n_j0_eval else "—")
    c5.metric("J+1 médian | J0 ok",
              f"{valid_j0_with_j1['var_j1_pct'].median():+.2f}%" if n_j0_eval else "—")

    if n_both:
        st.markdown(f"### 📅 {n_both} cas où les 2 conditions sont remplies")
        winners = valid_j0_with_j1[mask_j1].copy()
        winners["date"] = winners["time"].dt.strftime("%Y-%m-%d")
        winners["var J0 %"] = winners["var_j0_pct"].round(2)
        winners["var J+1 %"] = winners["var_j1_pct"].round(2)
        out = winners[["date", "close", "var J0 %", "var J+1 %"]].rename(
            columns={"close": "close J0"}
        ).sort_values("date", ascending=False).reset_index(drop=True)
        st.dataframe(out, use_container_width=True, height=320, hide_index=True)
        st.download_button(
            "📥 Télécharger CSV",
            data=out.to_csv(index=False).encode("utf-8"),
            file_name=f"{ticker}_conditions_J0_{j0_min}à{j0_max}_J1_{j1_min}à{j1_max}.csv",
            mime="text/csv",
            key=f"{key_prefix}_download",
        )
    elif n_j0 > 0:
        st.info(f"{n_j0} jours respectent la condition J0, mais aucun n'a un J+1 dans ta plage.")
    else:
        st.info("Aucun jour ne respecte la condition J0 sur cet historique.")
