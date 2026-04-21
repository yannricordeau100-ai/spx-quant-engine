"""
Classement des 164 tickers (Beta>2, MCap 1-100B$) par efficacité du
Bearish Engulfing (BBE) strict à J+1.

Métrique principale : rendement moyen J+1 après un signal BBE.
Plus c'est NÉGATIF, plus le BBE "fonctionne" (le marché descend le lendemain).

Utilise le même détecteur que l'app : ticker_analysis.detect_engulfing_strict
(confirmation volume, corps > 1.1×corps J-1, exclusion earnings ±5j).
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
OUT_CSV = Path("/Users/yann/spx-quant-engine/beta2_engulfing/bbe_ranking_j1.csv")

screen = pd.read_csv(SCREEN)
# Colonnes utiles : Ticker + Market Cap formaté ("12.34B") + MCap_USD numérique
mcap_map = dict(zip(screen["Ticker"], screen["Market Cap"]))
mcap_usd_map = dict(zip(screen["Ticker"], screen["MCap_USD"]))
company_map = dict(zip(screen["Ticker"], screen["Company"]))
sector_map = dict(zip(screen["Ticker"], screen["Sector"]))
tickers = sorted(screen["Ticker"].dropna().astype(str).unique())

def _load(t: str) -> pd.DataFrame | None:
    p = _find_ticker_csv(t)
    if not p:
        return None
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
    return df.dropna(subset=["open", "close"]).reset_index(drop=True)

rows = []
for t in tickers:
    df = _load(t)
    if df is None or len(df) < 30:
        continue
    earn = load_earnings_dates(t)
    bear = detect_engulfing_strict(df, pattern="bearish", earnings_dates=earn)
    n = len(bear)
    if n == 0:
        rows.append({
            "Ticker": t, "Company": company_map.get(t), "Sector": sector_map.get(t),
            "Market Cap": mcap_map.get(t), "MCap_USD": mcap_usd_map.get(t),
            "n_signals": 0, "j1_mean_%": None, "j1_median_%": None,
            "hit_rate_%": None, "j1_worst_%": None, "j1_best_%": None,
        })
        continue

    # Joindre J+1 close via merge_asof (chaque signal → close J+1 dans df)
    df_c = df[["time", "close"]].copy()
    # Index par date normalisée pour mapping
    close_by_date = dict(zip(df_c["time"].dt.normalize(), df_c["close"]))
    dates = df["time"].dt.normalize().tolist()

    j1_returns = []
    for _, sig in bear.iterrows():
        d0 = pd.Timestamp(sig["date"]).normalize()
        c0 = close_by_date.get(d0)
        if c0 is None or c0 <= 0:
            continue
        # Trouver le prochain jour de trading
        try:
            i0 = dates.index(d0)
        except ValueError:
            continue
        if i0 + 1 >= len(dates):
            continue
        c1 = close_by_date.get(dates[i0 + 1])
        if c1 is None:
            continue
        j1_returns.append((c1 - c0) / c0 * 100)

    if not j1_returns:
        rows.append({
            "Ticker": t, "Company": company_map.get(t), "Sector": sector_map.get(t),
            "Market Cap": mcap_map.get(t), "MCap_USD": mcap_usd_map.get(t),
            "n_signals": 0, "j1_mean_%": None, "j1_median_%": None,
            "hit_rate_%": None, "j1_worst_%": None, "j1_best_%": None,
        })
        continue

    s = pd.Series(j1_returns)
    hit_rate = (s < 0).mean() * 100  # % de J+1 vraiment baissiers
    rows.append({
        "Ticker": t,
        "Company": company_map.get(t),
        "Sector": sector_map.get(t),
        "Market Cap": mcap_map.get(t),
        "MCap_USD": mcap_usd_map.get(t),
        "n_signals": len(s),
        "j1_mean_%": round(s.mean(), 3),
        "j1_median_%": round(s.median(), 3),
        "hit_rate_%": round(hit_rate, 1),
        "j1_worst_%": round(s.min(), 3),
        "j1_best_%": round(s.max(), 3),
    })

out = pd.DataFrame(rows)
# Classement : meilleur BBE en premier = rendement J+1 le PLUS NÉGATIF
# Les tickers sans signal vont en fin
out["_rank_key"] = out["j1_mean_%"].fillna(9999)
out = out.sort_values(["_rank_key", "n_signals"], ascending=[True, False]).drop(columns="_rank_key")
out = out.reset_index(drop=True)
out.insert(0, "rank", out.index + 1)

out.to_csv(OUT_CSV, index=False)

# Affichage : uniquement les lignes avec signaux, colonnes demandées
show = out[out["n_signals"] > 0].copy()
print(f"\n=== BBE J+1 — {len(show)} tickers avec signaux (sur {len(out)} total) ===")
print(f"{'#':>3}  {'Ticker':<6} {'MktCap':>8}  {'n':>3}  {'J+1 μ%':>7}  {'hit%':>5}  Company")
print("-"*90)
for _, r in show.iterrows():
    print(f"{int(r['rank']):>3}  {r['Ticker']:<6} {str(r['Market Cap']):>8}  "
          f"{int(r['n_signals']):>3}  {r['j1_mean_%']:>7.3f}  {r['hit_rate_%']:>5.1f}  "
          f"{(r['Company'] or '')[:40]}")

print(f"\nCSV complet: {OUT_CSV}")
