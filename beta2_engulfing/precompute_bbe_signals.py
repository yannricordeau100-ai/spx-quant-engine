"""
Pré-calcule TOUS les signaux BBE de l'univers Beta>2 + leur rendement J+1.

Sortie : bbe_signals.csv (ticker, date, close_j0, close_j1, return_j1_%)
Cette table alimente l'app Streamlit interactive (classement dynamique
selon seuil de win-rate).

À relancer quand :
  - les CSV OHLCV sont rafraîchis (run generate_ticker_files.py avant)
  - l'univers change (relance beta_screen.py avant)
"""
from __future__ import annotations
import sys
from pathlib import Path

sys.path.insert(0, "/Users/yann/spx-quant-engine")
import pandas as pd
from ticker_analysis import (
    _find_ticker_csv, load_earnings_dates, detect_engulfing_strict,
)

SCREEN = Path("/Users/yann/spx-quant-engine/beta2_engulfing/beta_gt2_midlarge.csv")
OUT_SIGNALS = Path("/Users/yann/spx-quant-engine/beta2_engulfing/bbe_signals.csv")
OUT_META = Path("/Users/yann/spx-quant-engine/beta2_engulfing/bbe_meta.csv")

screen = pd.read_csv(SCREEN)
meta_cols = ["Ticker", "Company", "Sector", "MCap_USD"]
meta = screen[meta_cols].copy()
meta.to_csv(OUT_META, index=False)

tickers = sorted(screen["Ticker"].dropna().astype(str).unique())
all_signals = []

for t in tickers:
    p = _find_ticker_csv(t)
    if not p:
        continue
    df = pd.read_csv(p, sep=";")
    df.columns = [c.strip().lower() for c in df.columns]
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    for c in ("open", "high", "low", "close"):
        df[c] = pd.to_numeric(
            df[c].astype(str).str.replace(",", ".").str.replace(r"\s+", "", regex=True),
            errors="coerce",
        )
    df["volume"] = pd.to_numeric(
        df["volume"].astype(str).str.replace(r"\s+", "", regex=True), errors="coerce"
    )
    df = df.dropna(subset=["open", "close"]).reset_index(drop=True)
    if len(df) < 30:
        continue

    earn = load_earnings_dates(t)
    bear = detect_engulfing_strict(df, pattern="bearish", earnings_dates=earn)
    if len(bear) == 0:
        continue

    df_idx = df.set_index(df["time"].dt.normalize())
    dates_list = list(df_idx.index)

    HORIZONS = [1, 2, 3, 4, 5]
    for _, sig in bear.iterrows():
        d0 = pd.Timestamp(sig["date"]).normalize()
        if d0 not in df_idx.index:
            continue
        try:
            i0 = dates_list.index(d0)
        except ValueError:
            continue
        if i0 + 1 >= len(dates_list):
            continue  # pas au moins 1 jour après
        c0 = float(df_idx.loc[d0, "close"])
        if c0 <= 0:
            continue

        row = {
            "ticker": t,
            "date_j0": d0.strftime("%Y-%m-%d"),
            "close_j0": round(c0, 4),
        }
        # Pour chaque horizon H (1..5), on capture close_jH et low_cum_jH
        # (= plus bas atteint entre J+1 et J+H, inclus)
        min_low_running = None
        for h in HORIZONS:
            if i0 + h >= len(dates_list):
                row[f"close_j{h}"] = None
                row[f"return_close_j{h}_pct"] = None
                row[f"return_lowmin_j{h}_pct"] = None
                continue
            c_h = float(df_idx.iloc[i0 + h]["close"])
            low_h = float(df_idx.iloc[i0 + h]["low"])
            min_low_running = low_h if min_low_running is None else min(min_low_running, low_h)
            row[f"close_j{h}"] = round(c_h, 4)
            row[f"return_close_j{h}_pct"] = round((c_h - c0) / c0 * 100, 4)
            row[f"return_lowmin_j{h}_pct"] = round((min_low_running - c0) / c0 * 100, 4)
        all_signals.append(row)

sig_df = pd.DataFrame(all_signals)
sig_df.to_csv(OUT_SIGNALS, index=False)

# Stats globales
print(f"✅ {len(sig_df)} signaux BBE extraits sur {sig_df['ticker'].nunique()} tickers")
print(f"   Moyenne signaux/ticker : {sig_df.groupby('ticker').size().mean():.1f}")
print(f"   Export : {OUT_SIGNALS}")
print(f"   Métadonnées (cap, secteur) : {OUT_META}")
