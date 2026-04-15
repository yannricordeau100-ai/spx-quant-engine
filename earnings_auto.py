# earnings_auto.py — Auto-fetch earnings dates via yfinance

import pandas as pd
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data" / "live_selected"


def _should_fetch_earnings(ticker: str) -> bool:
    """True uniquement pour les vrais tickers société (pas indices/fondamentaux)."""
    from query_executor import _is_ticker_csv
    return _is_ticker_csv(f"{ticker}_daily")


def fetch_and_save_earnings(ticker: str, force: bool = False) -> bool:
    t = ticker.upper()
    path = DATA_DIR / f"{t}_earnings.csv"
    if path.exists() and not force:
        return True
    try:
        import yfinance as yf
        tk = yf.Ticker(t)
        cal = tk.earnings_dates
        if cal is None or cal.empty:
            return False
        cal = cal.reset_index()
        cal.columns = [c.strip().lower().replace(" ", "_") for c in cal.columns]
        date_col = cal.columns[0]
        cal["date"] = pd.to_datetime(cal[date_col], errors="coerce", utc=True)
        cal["date"] = cal["date"].dt.tz_localize(None)
        cal = cal.dropna(subset=["date"]).sort_values("date")
        out = cal[["date"]].copy()
        out["date"] = out["date"].dt.strftime("%Y-%m-%d")
        out.to_csv(path, sep=";", index=False)
        print(f"[earnings] {t}: {len(out)} dates → {path.name}", flush=True)
        return True
    except Exception as e:
        print(f"[earnings] {t}: {e}", flush=True)
        return False


def auto_fetch_missing(tickers: set) -> dict:
    results = {}
    for t in sorted(tickers):
        tu = t.upper()
        if not _should_fetch_earnings(tu):
            continue
        if not (DATA_DIR / f"{tu}_earnings.csv").exists():
            results[tu] = fetch_and_save_earnings(tu)
        else:
            results[tu] = True
    return results
