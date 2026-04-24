"""pead_ui.py — Streamlit UI pour l'onglet PEAD.

Module séparé pour garder app_local.py mince. Appelé depuis app_local.py via
`from pead_ui import render_pead_tab; render_pead_tab()`.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st


def _lazy_engine():
    """Lazy import pour éviter un coût de démarrage si l'onglet n'est pas ouvert."""
    from pead_engine import (
        load_universe, build_universe, backtest, backtest_summary,
        daily_scan, download_ohlcv_batch, build_earnings_batch,
        PEAD_DIR, DEFAULT_COMPRESSION_THRESHOLD, DEFAULT_SURPRISE_THRESHOLD,
    )
    return {
        "load_universe": load_universe,
        "build_universe": build_universe,
        "backtest": backtest,
        "backtest_summary": backtest_summary,
        "daily_scan": daily_scan,
        "download_ohlcv_batch": download_ohlcv_batch,
        "build_earnings_batch": build_earnings_batch,
        "PEAD_DIR": PEAD_DIR,
    }


def render_pead_tab() -> None:
    with st.expander("📈 PEAD — Backtest & Scan quotidien (Russell 1000 large caps)",
                     expanded=False):
        eng = _lazy_engine()

        # ── Stats univers ────────────────────────────────────────────────
        try:
            uni = eng["load_universe"]()
        except FileNotFoundError:
            st.warning("Univers PEAD non construit. Cliquer ci-dessous pour le créer.")
            uni = pd.DataFrame()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Tickers univers", len(uni) if not uni.empty else 0)
        with col2:
            if not uni.empty:
                med = uni["market_cap"].median() / 1e9
                st.metric("Market cap médian", f"${med:.1f}B")
        with col3:
            if not uni.empty:
                st.metric("Analystes médian", int(uni["n_analysts"].median()))
        with col4:
            tickers_csv = list((eng["PEAD_DIR"] / "tickers").glob("*.csv"))
            earnings_csv = list((eng["PEAD_DIR"] / "earnings").glob("*.csv"))
            st.metric("OHLCV / earnings", f"{len(tickers_csv)} / {len(earnings_csv)}")

        # ── Paramètres backtest (sliders) ────────────────────────────────
        st.markdown("### ⚙️ Paramètres du pattern")
        p1, p2, p3 = st.columns(3)
        with p1:
            cap_min = st.slider(
                "Market cap min ($B)", 1, 500, 20, step=1, key="pead_cap_min",
                help="Filtre univers large caps. Default $20B (exclut small/mid caps). "
                     "Changer → cliquer 'Rafraîchir univers + data'.",
            )
            cap_max = st.slider(
                "Market cap max ($B)", 10, 2000, 500, step=10, key="pead_cap_max",
                help="Default $500B (exclut MAG7 ≈ $1T+).",
            )
        with p2:
            compression_mode = st.selectbox(
                "Mode compression",
                options=["all", "avg", "median", "n_of_5"],
                index=1, key="pead_comp_mode",
                help=(
                    "all = chacun des 5 jours < seuil (spec stricte)  |  "
                    "avg = moyenne 5 jours < seuil  |  "
                    "median = médiane 5 jours < seuil  |  "
                    "n_of_5 = au moins N sur 5 jours < seuil"
                ),
            )
            compression_thr = st.slider(
                "Seuil compression (ratio range / range_20j)",
                0.3, 1.0, 0.7, step=0.05, key="pead_comp_thr",
            )
            if compression_mode == "n_of_5":
                n_of_5 = st.slider("N sur 5 jours", 1, 5, 4, key="pead_n_of_5")
            else:
                n_of_5 = 5
        with p3:
            surprise_thr_pct = st.slider(
                "Surprise minimum sur J (% vs Close J-1)",
                3, 20, 10, step=1, key="pead_surprise",
            )
            direction_filter = st.radio(
                "Directions tradées",
                options=["Long only", "Short only", "Les deux"],
                index=0, key="pead_direction",
                help=(
                    "Sur mid-caps, les shorts PEAD ont un WR < 30 %. "
                    "Par défaut long only."
                ),
            )

        st.markdown("### 🧪 Backtest historique")
        bt_col1, bt_col2 = st.columns([1, 1])
        with bt_col1:
            if st.button("▶️ Lancer backtest", key="pead_run_bt",
                         use_container_width=True):
                with st.spinner("Backtest en cours sur l'univers..."):
                    sigs = eng["backtest"](
                        compression_thr=compression_thr,
                        surprise_thr=surprise_thr_pct / 100.0,
                        compression_mode=compression_mode,
                        n_of_5=n_of_5,
                    )
                    # Filtre direction
                    if not sigs.empty:
                        if direction_filter == "Long only":
                            sigs = sigs[sigs["direction"] == "long"]
                        elif direction_filter == "Short only":
                            sigs = sigs[sigs["direction"] == "short"]
                    st.session_state["pead_bt_result"] = sigs
                    st.session_state["pead_bt_summary"] = eng["backtest_summary"](sigs)
                st.success(f"Terminé — {len(sigs)} signaux.")
        with bt_col2:
            if st.button("🔄 Rafraîchir univers + data", key="pead_rebuild",
                         help="Rebuild univers + download OHLCV + earnings (5-10 min)",
                         use_container_width=True):
                with st.spinner("Rebuild univers..."):
                    uni2 = eng["build_universe"](
                        market_cap_min=cap_min * 1_000_000_000,
                        market_cap_max=cap_max * 1_000_000_000,
                    )
                with st.spinner(f"Download OHLCV ({len(uni2)} tickers)..."):
                    eng["download_ohlcv_batch"](uni2["ticker"].tolist())
                with st.spinner(f"Fetch earnings ({len(uni2)} tickers)..."):
                    eng["build_earnings_batch"](uni2["ticker"].tolist())
                st.success("Univers + data à jour.")

        # Résultats backtest
        bt_summary = st.session_state.get("pead_bt_summary")
        bt_result = st.session_state.get("pead_bt_result")
        if bt_summary and bt_summary.get("n_signals", 0) > 0:
            s = bt_summary
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Signaux", s["n_signals"])
            c2.metric("Win rate", f"{s['win_rate_pct']}%")
            c3.metric("Avg PnL J+1→J+5", f"{s['avg_pnl_pct']}%")
            c4.metric("Médian PnL", f"{s['median_pnl_pct']}%")
            c5, c6, c7, c8 = st.columns(4)
            c5.metric("Longs", f"{s.get('n_long', 0)} (WR {s.get('long_win_rate_pct', 0)}%)")
            c6.metric("Shorts", f"{s.get('n_short', 0)} (WR {s.get('short_win_rate_pct', 0)}%)")
            c7.metric("Best trade", f"{s['best_pnl_pct']}%",
                      help=f"{s['best_trade']}")
            c8.metric("Worst trade", f"{s['worst_pnl_pct']}%",
                      help=f"{s['worst_trade']}")

            if isinstance(bt_result, pd.DataFrame) and not bt_result.empty:
                st.dataframe(bt_result, use_container_width=True, height=300)
                csv = bt_result.to_csv(index=False, sep=";").encode("utf-8")
                st.download_button("💾 Télécharger signaux CSV",
                                   data=csv, file_name="pead_backtest.csv",
                                   mime="text/csv")
        elif bt_summary:
            st.warning(
                f"Aucun signal trouvé avec ces paramètres "
                f"(mode={compression_mode}, seuil={compression_thr}, "
                f"surprise=±{surprise_thr_pct}%, direction={direction_filter}). "
                "Essaie de relâcher la compression (mode=avg, seuil=0.7) "
                "ou de baisser le seuil surprise à 5 %."
            )

        # ── Scan quotidien ───────────────────────────────────────────────
        st.markdown("### 🔍 Scan quotidien — earnings ±3j")
        sc1, sc2 = st.columns([1, 1])
        with sc1:
            dry_run = st.checkbox("Dry run (pas d'alerte Telegram/email)",
                                  value=True, key="pead_dry")
        with sc2:
            if st.button("📡 Lancer scan maintenant", key="pead_scan",
                         use_container_width=True):
                with st.spinner("Scan en cours (fetch earnings + Nasdaq timing)..."):
                    res = eng["daily_scan"](dry_run=dry_run)
                    st.session_state["pead_last_scan"] = res
                st.success(
                    f"{len(res['pre_earnings'])} pré-earnings | "
                    f"{len(res['signals'])} signaux"
                )

        scan_res = st.session_state.get("pead_last_scan")
        if scan_res:
            pe = scan_res.get("pre_earnings", [])
            sg = scan_res.get("signals", [])
            if pe:
                st.markdown("#### 🟡 Pré-earnings (compression détectée)")
                st.dataframe(pd.DataFrame(pe), use_container_width=True)
            if sg:
                st.markdown("#### 🟢🔴 Signaux déclenchés")
                st.dataframe(pd.DataFrame(sg), use_container_width=True)
            if not pe and not sg:
                st.info("Rien détecté dans la fenêtre ±3j avec les paramètres actuels.")

        # ── Fichiers disponibles ─────────────────────────────────────────
        with st.expander("📁 Fichiers disponibles", expanded=False):
            data_dir = eng["PEAD_DIR"]
            st.code(f"""Univers          : {data_dir / 'universe.csv'}
Tickers OHLCV    : {data_dir / 'tickers' / '*.csv'}  ({len(list((data_dir/'tickers').glob('*.csv')))} fichiers)
Earnings         : {data_dir / 'earnings' / '*.csv'}  ({len(list((data_dir/'earnings').glob('*.csv')))} fichiers)
Scans récents    : {data_dir / 'signals' / '*.json'}
Alertes log      : {data_dir / 'alerts.log'}""", language=None)
