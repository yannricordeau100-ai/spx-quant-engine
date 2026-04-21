"""
Génère 164 paires de fichiers (OHLCV + earnings) dans le format exact attendu
par spx-quant-engine/ticker_analysis.py et query_executor.py :

  <TICKER>.csv          : time;open;high;low;close;Volume  (reverse chrono)
  <TICKER>_earnings.csv : date;quarter;type

Destination : ~/spx-quant-engine/data/live_selected/tickers/
Source OHLCV réutilisée depuis : ~/spx-quant-engine/data/beta2_universe/
Source earnings (inclut dates futures) : yfinance.get_earnings_dates(limit=40)
"""
from __future__ import annotations
import json, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

# ---------------------------------------------------------------------------
SRC_OHLCV = Path("/Users/yann/spx-quant-engine/data/beta2_universe")
DST = Path("/Users/yann/spx-quant-engine/data/live_selected/tickers")
SCREEN_CSV = Path("/Users/yann/spx-quant-engine/beta2_engulfing/beta_gt2_midlarge.csv")
MAX_WORKERS = 12
# ---------------------------------------------------------------------------

DST.mkdir(parents=True, exist_ok=True)

tickers = sorted(pd.read_csv(SCREEN_CSV)["Ticker"].dropna().astype(str).unique())
print(f"[1/4] {len(tickers)} tickers à traiter")

# ---------- 1. OHLCV : reformatage depuis beta2_universe/ -----------------
print("[2/4] Reformatage OHLCV → format tickers/")

def write_ohlcv(tkr: str) -> tuple[str, int, str]:
    src = SRC_OHLCV / f"{tkr}_daily.csv"
    if not src.exists():
        return tkr, 0, "missing_source"
    d = pd.read_csv(src, sep=";", keep_default_na=False, dtype=str)
    if d.empty:
        return tkr, 0, "empty_source"

    # Colonnes finales : time;open;high;low;close;Volume (V majuscule)
    out = pd.DataFrame({
        "time": d["time"],
        "open": d["open"],
        "high": d["high"],
        "low": d["low"],
        "close": d["close"],
        "Volume": d["volume"],
    })
    # Reverse chronological (la convention de tickers/)
    out = out.iloc[::-1].reset_index(drop=True)
    dst = DST / f"{tkr}.csv"
    out.to_csv(dst, sep=";", index=False)
    return tkr, len(out), "ok"

ohlcv_results = []
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    for r in ex.map(write_ohlcv, tickers):
        ohlcv_results.append(r)
ok_ohlcv = [r for r in ohlcv_results if r[2] == "ok"]
print(f"     OHLCV écrits : {len(ok_ohlcv)}/{len(tickers)}")

# ---------- 2. Earnings via yfinance (parallèle) --------------------------
print(f"[3/4] Earnings dates (parallèle, workers={MAX_WORKERS})...")
t0 = time.time()

def fetch_earnings(tkr: str) -> tuple[str, list[tuple[str, str]]]:
    """Retourne liste triée de (date_iso, quarter_label). Inclut passé + futur."""
    rows: list[tuple[pd.Timestamp, str]] = []
    try:
        yt = yf.Ticker(tkr)
        try:
            edf = yt.get_earnings_dates(limit=40)
        except Exception:
            edf = None
        if edf is not None and not edf.empty:
            for ts in edf.index:
                ts = pd.to_datetime(ts, errors="coerce", utc=True)
                if pd.isna(ts):
                    continue
                d = ts.tz_convert("America/New_York").normalize().tz_localize(None)
                if d.year < 2020:
                    continue
                q = (d.month - 1) // 3 + 1
                rows.append((d, f"Q{q}-{d.year}"))
        # Fallback calendar (prochain earning)
        try:
            cal = yt.calendar
            if isinstance(cal, dict):
                e = cal.get("Earnings Date")
                if e:
                    ee = e if isinstance(e, list) else [e]
                    for dd in ee:
                        ts = pd.to_datetime(dd, errors="coerce")
                        if not pd.isna(ts):
                            d = ts.normalize()
                            q = (d.month - 1) // 3 + 1
                            rows.append((d, f"Q{q}-{d.year}"))
        except Exception:
            pass
    except Exception:
        pass

    # Dédupe par date
    seen = {}
    for d, q in rows:
        key = d.strftime("%Y-%m-%d")
        if key not in seen:
            seen[key] = q
    out = sorted(seen.items(), key=lambda x: x[0], reverse=True)  # reverse chrono
    return tkr, out

earnings_all: dict[str, list[tuple[str, str]]] = {}
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
    futs = {ex.submit(fetch_earnings, t): t for t in tickers}
    done = 0
    for fu in as_completed(futs):
        tkr, rows = fu.result()
        earnings_all[tkr] = rows
        done += 1
        if done % 30 == 0 or done == len(tickers):
            print(f"     earnings {done}/{len(tickers)}")
print(f"     earnings terminés en {time.time()-t0:.1f}s")

# ---------- 3. Écriture des <TICKER>_earnings.csv ------------------------
print("[4/4] Écriture fichiers _earnings.csv")
earn_stats = []
for tkr in tickers:
    rows = earnings_all.get(tkr, [])
    dst = DST / f"{tkr}_earnings.csv"
    if not rows:
        # On crée quand même le fichier avec le header (évite cas d'absence)
        dst.write_text("date;quarter;type\n")
        earn_stats.append((tkr, 0))
        continue
    lines = ["date;quarter;type"]
    for d, q in rows:
        lines.append(f"{d};{q};earnings")
    dst.write_text("\n".join(lines) + "\n")
    earn_stats.append((tkr, len(rows)))

# ---------- 4. Rapport ----------------------------------------------------
report = {
    "total_tickers": len(tickers),
    "ohlcv_ok": len(ok_ohlcv),
    "ohlcv_issues": [(t, s) for t, n, s in ohlcv_results if s != "ok"],
    "earnings_min": min(n for _, n in earn_stats),
    "earnings_max": max(n for _, n in earn_stats),
    "earnings_avg": round(sum(n for _, n in earn_stats) / len(earn_stats), 1),
    "earnings_zero": [t for t, n in earn_stats if n == 0],
    "dest_dir": str(DST),
}
(DST / "_generation_report.json").write_text(json.dumps(report, indent=2))
print("\n===== RAPPORT =====")
print(json.dumps(report, indent=2))
