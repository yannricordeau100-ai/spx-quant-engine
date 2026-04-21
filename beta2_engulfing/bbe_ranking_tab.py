"""
Module partagé : rendu du classement BBE interactif.

Utilisé par :
  - bbe_ranking_app.py (app Streamlit standalone)
  - app_local.py (intégré comme expander dans l'app principale SPX Quant Engine)

La signature `render_bbe_ranking(key_prefix=...)` permet de réutiliser le
composant dans plusieurs contextes sans conflit de state.
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd
import streamlit as st

BASE = Path(__file__).parent
SIGNALS_CSV = BASE / "bbe_signals.csv"
META_CSV = BASE / "bbe_meta.csv"


@st.cache_data(show_spinner=False)
def _load_data():
    sig = pd.read_csv(SIGNALS_CSV)
    meta = pd.read_csv(META_CSV)
    return sig, meta


def _fmt_cap(x):
    if pd.isna(x):
        return "-"
    if x >= 1e9:
        return f"{x/1e9:.1f}B"
    if x >= 1e6:
        return f"{x/1e6:.0f}M"
    return f"{x:.0f}"


def _highlight_winrate(v):
    if pd.isna(v):
        return ""
    if v >= 70:
        return "background-color:#1b4332;color:white;font-weight:bold"
    if v >= 60:
        return "background-color:#2d6a4f;color:white;font-weight:bold"
    if v >= 50:
        return "background-color:#52b788;color:white"
    if v >= 40:
        return "background-color:#f9c74f"
    return "background-color:#e76f51;color:white"


def render_bbe_ranking(key_prefix: str = "bbe_rank") -> None:
    """Affiche le classement dynamique BBE — réutilisable partout."""
    if not SIGNALS_CSV.exists() or not META_CSV.exists():
        st.error(
            f"Données manquantes. Lance :\n\n"
            f"`cd {BASE} && python3 precompute_bbe_signals.py`"
        )
        return

    signals, meta = _load_data()

    st.caption(
        "Univers : 164 actions US, Beta>2, MCap 1-100 Mds$. "
        "BBE strict : corps > 1,1× corps J-1, volume J > volume J-1, "
        "earnings ±5j exclus. Période : 2021→aujourd'hui."
    )

    # ─── Contrôles ────────────────────────────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        threshold_pct = st.slider(
            "Seuil de validation (%)",
            min_value=-10.0, max_value=-0.1, value=-2.0, step=0.1,
            format="%.1f%%",
            key=f"{key_prefix}_threshold",
            help="Un BBE est 'gagnant' si le cours atteint ce seuil sous le close J0.",
        )
    with c2:
        horizon = st.slider(
            "Horizon (jours après le signal)",
            min_value=1, max_value=5, value=1, step=1,
            key=f"{key_prefix}_horizon",
            help="J+1 = lendemain seulement. J+5 = fenêtre de 5 jours.",
        )

    c3, c4, c5 = st.columns([2, 1, 2])
    with c3:
        mode_return = st.radio(
            "Base de calcul",
            ["Plus-bas atteint sur la fenêtre", "Close final (J+N)"],
            index=0,
            key=f"{key_prefix}_mode",
            help=(
                "Plus-bas atteint : validé dès que le low touche le seuil entre "
                "J+1 et J+N (simule un TP intraday).\n"
                "Close final : validé seulement si le close J+N est sous le seuil."
            ),
        )
    with c4:
        min_signals = st.number_input(
            "Min. signaux", min_value=0, max_value=50, value=10, step=1,
            key=f"{key_prefix}_minsig",
            help="Masque les tickers avec trop peu de signaux.",
        )
    with c5:
        sector_filter = st.multiselect(
            "Filtrer par secteur",
            sorted(meta["Sector"].dropna().unique()),
            default=[],
            key=f"{key_prefix}_sector",
        )

    if mode_return.startswith("Plus-bas"):
        ret_col = f"return_lowmin_j{horizon}_pct"
        mode_label = f"plus-bas atteint entre J+1 et J+{horizon}"
    else:
        ret_col = f"return_close_j{horizon}_pct"
        mode_label = f"close J+{horizon}"

    sig_h = signals.dropna(subset=[ret_col]).copy()
    grp = sig_h.groupby("ticker")[ret_col]
    stats = pd.DataFrame({
        "n_signals": grp.size(),
        "mean_ret_%": grp.mean().round(3),
        "median_ret_%": grp.median().round(3),
        "worst_%": grp.min().round(3),
        "best_%": grp.max().round(3),
    })
    stats["win_rate_%"] = (
        sig_h.groupby("ticker")[ret_col]
        .apply(lambda s: (s <= threshold_pct).mean() * 100)
        .round(1)
    )
    stats = stats.reset_index().rename(columns={"ticker": "Ticker"})
    stats = stats.merge(meta, on="Ticker", how="left")
    stats["MktCap"] = stats["MCap_USD"].apply(_fmt_cap)

    if min_signals > 0:
        stats = stats[stats["n_signals"] >= min_signals]
    if sector_filter:
        stats = stats[stats["Sector"].isin(sector_filter)]

    stats = stats.sort_values(
        ["win_rate_%", "n_signals"], ascending=[False, False]
    ).reset_index(drop=True)
    stats.insert(0, "#", stats.index + 1)

    st.markdown(
        f"**Seuil {threshold_pct:.1f}% · horizon J+{horizon} · base : {mode_label}**"
    )

    display = stats[[
        "#", "Ticker", "Company", "Sector", "MktCap",
        "n_signals", "win_rate_%", "mean_ret_%", "median_ret_%", "worst_%",
    ]].rename(columns={
        "n_signals": "N",
        "win_rate_%": "Win rate %",
        "mean_ret_%": "Moy. %",
        "median_ret_%": "Médiane %",
        "worst_%": "Pire %",
    })

    styled = display.style.applymap(_highlight_winrate, subset=["Win rate %"])
    st.dataframe(styled, use_container_width=True, height=560)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Tickers affichés", len(stats))
    k2.metric("Signaux totaux", int(stats["n_signals"].sum()) if len(stats) else 0)
    k3.metric(
        "Win rate moyen",
        f"{stats['win_rate_%'].mean():.1f}%" if len(stats) else "—",
    )
    k4.metric(
        "Win rate médian",
        f"{stats['win_rate_%'].median():.1f}%" if len(stats) else "—",
    )

    st.download_button(
        "📥 Télécharger le classement (CSV)",
        data=display.to_csv(index=False).encode("utf-8"),
        file_name=f"bbe_ranking_j{horizon}_{threshold_pct:.1f}pct_"
                  f"{'low' if mode_return.startswith('Plus-bas') else 'close'}.csv",
        mime="text/csv",
        key=f"{key_prefix}_download",
    )

    with st.expander("ℹ️ Définitions"):
        st.markdown("""
- **BBE strict** : bougie rouge dont le corps englobe celui de la bougie verte J-1, avec **volume J > volume J-1** et **corps J > 1,1 × corps J-1**, **open J ≥ close J-1** (gap up ou neutre, tolérance 0,1%).
- **Horizon (J+N)** : fenêtre de N jours de trading après le signal.
- **Plus-bas atteint** : rendement = (min low entre J+1 et J+N − close J0) / close J0. Un signal est validé dès qu'à un moment le cours a touché le seuil — pertinent pour un TP intraday.
- **Close final (J+N)** : rendement = (close J+N − close J0) / close J0. Plus restrictif.
- **Win rate %** : % des signaux BBE d'un ticker qui ont validé le seuil.
- **Exclusion earnings** : BBE à ±5j d'un earnings ignorés.
""")
