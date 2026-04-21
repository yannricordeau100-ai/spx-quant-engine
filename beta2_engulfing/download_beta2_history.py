"""
Télécharge 5 ans d'OHLCV daily (depuis 2021-01-01) + earnings dates
pour les 164 tickers du screener Beta>2 / MCap 1-100B$.

Source: yfinance (gratuit, pas de clé API)
Sortie: ~/spx-quant-engine/data/beta2_universe/<TICKER>_daily.csv
Format colonnes: time;open;high;low;close;volume;earnings_date
Séparateur `;` + minuscules => cohérent avec les autres CSV daily du repo.
"""
from __future__ import annotations
import os, sys, time, json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
SCREEN_CSV = Path("/Users/yann/spx-quant-engine/beta2_engulfing/beta_gt2_midlarge.csv")
OUT_DIR = Path("/Users/yann/spx-quant-engine/data/beta2_universe")
START = "2021-01-01"
END = None  # aujourd'hui
MAX_WORKERS_EARNINGS = 12
# ---------------------------------------------------------------------------

OUT_DIR.mkdir(parents=True, exist_ok=True)

# 1. Liste des tickers ------------------------------------------------------
df_screen = pd.read_csv(SCREEN_CSV)
tickers = sorted(df_screen["Ticker"].dropna().astype(str).unique().tolist())
print(f"[1/4] {len(tickers)} tickers chargés")

# 2. Batch download OHLCV ---------------------------------------------------
print(f"[2/4] Téléchargement OHLCV 5Y (depuis {START})...")
t0 = time.time()
bulk = yf.download(
    tickers,
    start=START,
    end=END,
    interval="1d",
    group_by="ticker",
    auto_adjust=False,
    actions=False,
    progress=True,
    threads=True,
)
print(f"     OHLCV récupérés en {time.time()-t0:.1f}s")

# Normalise : si un seul ticker => df plat ; sinon MultiIndex (ticker, field)
def extract_ohlcv(tkr: str) -> pd.DataFrame | None:
    try:
        if isinstance(bulk.columns, pd.MultiIndex):
            if tkr not in bulk.columns.get_level_values(0):
                return None
            d = bulk[tkr].copy()
        else:
            d = bulk.copy()
        d = d.dropna(how="all")
        if d.empty:
            return None
        d = d.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })
        d = d[["open", "high", "low", "close", "volume"]]
        d.index = pd.to_datetime(d.index).tz_localize(None).normalize()
        d.index.name = "time"
        return d
    except Exception as e:
        print(f"   ! extract {tkr}: {e}")
        return None

# 3. Earnings dates (parallèle) --------------------------------------------
print(f"[3/4] Earnings dates (threads={MAX_WORKERS_EARNINGS})...")
t1 = time.time()

def fetch_earnings(tkr: str) -> tuple[str, set]:
    """Retourne l'ensemble des dates (YYYY-MM-DD) d'earnings >= 2020."""
    dates: set[str] = set()
    try:
        yt = yf.Ticker(tkr)
        # get_earnings_dates couvre passé + prochaines
        try:
            edf = yt.get_earnings_dates(limit=40)
        except Exception:
            edf = None
        if edf is not None and not edf.empty:
            idx = pd.to_datetime(edf.index, errors="coerce", utc=True)
            for ts in idx:
                if pd.isna(ts):
                    continue
                d = ts.tz_convert("America/New_York").date()
                if d.year >= 2020:
                    dates.add(d.isoformat())
        # Fallback: calendar (prochain earning seulement)
        try:
            cal = yt.calendar
            if isinstance(cal, dict):
                e = cal.get("Earnings Date")
                if e:
                    ee = e if isinstance(e, list) else [e]
                    for d in ee:
                        ts = pd.to_datetime(d, errors="coerce")
                        if not pd.isna(ts):
                            dates.add(ts.date().isoformat())
        except Exception:
            pass
    except Exception as e:
        print(f"   ! earnings {tkr}: {e}")
    return tkr, dates

earnings_map: dict[str, set[str]] = {}
with ThreadPoolExecutor(max_workers=MAX_WORKERS_EARNINGS) as ex:
    futs = [ex.submit(fetch_earnings, t) for t in tickers]
    done = 0
    for fu in as_completed(futs):
        tkr, s = fu.result()
        earnings_map[tkr] = s
        done += 1
        if done % 20 == 0 or done == len(tickers):
            print(f"     earnings {done}/{len(tickers)}")
print(f"     earnings récupérés en {time.time()-t1:.1f}s")

# 4. Fusion + écriture CSV --------------------------------------------------
print(f"[4/4] Écriture CSV dans {OUT_DIR}")
written, empty, failed = [], [], []
for tkr in tickers:
    d = extract_ohlcv(tkr)
    if d is None or d.empty:
        empty.append(tkr)
        continue
    # colonne earnings_date : vide sauf jours d'earnings (match par date)
    edates = earnings_map.get(tkr, set())
    d["earnings_date"] = d.index.strftime("%Y-%m-%d").where(
        d.index.strftime("%Y-%m-%d").isin(edates), ""
    )
    # Reset index en colonne 'time' string YYYY-MM-DD
    d_out = d.copy()
    d_out.insert(0, "time", d_out.index.strftime("%Y-%m-%d"))
    d_out = d_out.reset_index(drop=True)
    # Round floats pour lisibilité
    for c in ["open", "high", "low", "close"]:
        d_out[c] = d_out[c].astype(float).round(4)
    d_out["volume"] = d_out["volume"].astype("Int64")
    out_path = OUT_DIR / f"{tkr}_daily.csv"
    try:
        d_out.to_csv(out_path, sep=";", index=False)
        written.append(tkr)
    except Exception as e:
        failed.append((tkr, str(e)))

# Rapport -------------------------------------------------------------------
report = {
    "total_tickers": len(tickers),
    "written": len(written),
    "empty_or_missing": len(empty),
    "failed": len(failed),
    "empty_list": empty,
    "failed_list": failed,
    "output_dir": str(OUT_DIR),
    "start_date": START,
}
with open(OUT_DIR / "_download_report.json", "w") as f:
    json.dump(report, f, indent=2)

print("\n===== RAPPORT =====")
print(f"Écrits : {len(written)}/{len(tickers)}")
if empty:
    print(f"Vides/absents ({len(empty)}) : {empty[:15]}{'...' if len(empty)>15 else ''}")
if failed:
    print(f"Erreurs ({len(failed)}) : {failed[:5]}")
print(f"Dossier : {OUT_DIR}")
print(f"Rapport JSON : {OUT_DIR/'_download_report.json'}")
