"""pead_engine.py — PEAD (Post-Earnings Announcement Drift) engine for app_local.py

Zone exclusive : data/pead/** (séparée de data/live_selected/tickers/** qui est
la zone de la conv BBE "Engulfing / Beta>2 universe"). Aucune écriture hors de
data/pead/** par ce module.

Modules:
- universe : build & filter Russell 1000 universe (mid-caps cibles pour PEAD)
- ohlcv    : download daily OHLCV via yfinance
- earnings : dates + timing (bmo/amc) via FMP /stable/ + Nasdaq fallback
- signals  : detect compression (J-5..J-1) + surprise (±10% on J)
- backtest : historical P&L Open J+1 → Close J+5
- alerts   : push Telegram + email via beta2_engulfing/notifiers.py
"""

from __future__ import annotations

import json
import os
import re
import time as _time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
PEAD_DIR = BASE_DIR / "data" / "pead"
PEAD_DIR.mkdir(parents=True, exist_ok=True)
(PEAD_DIR / "tickers").mkdir(exist_ok=True)
(PEAD_DIR / "earnings").mkdir(exist_ok=True)
(PEAD_DIR / "signals").mkdir(exist_ok=True)

load_dotenv(BASE_DIR / ".env")
FMP_API_KEY = os.environ.get("FMP_API_KEY", "eICYozEpbXVtGwOtAUaQErWxk3y7kNJG")

# ─── Paramètres par défaut (modifiables en UI pour plan B) ──────────────

DEFAULT_MARKET_CAP_MIN = 20_000_000_000    # $20B (large caps)
DEFAULT_MARKET_CAP_MAX = 500_000_000_000   # $500B (exclut MAG7)
DEFAULT_COMPRESSION_THRESHOLD = 0.7       # moyenne 5j < 70% du range moyen 20j
DEFAULT_COMPRESSION_MODE = "avg"          # "all" (spec stricte, rare) | "avg" (best WR)
DEFAULT_SURPRISE_THRESHOLD = 0.10         # ±10% on J vs J-1
DEFAULT_ENTRY_OFFSET = 1                  # Open J+1
DEFAULT_EXIT_OFFSET = 5                   # Close J+5
DEFAULT_DIRECTION_FILTER = "long"         # shorts ont WR < 30% sur le backtest

# Secteurs à exclure (typiquement immunes au PEAD : REITs, utilities, banks)
DEFAULT_SECTORS_EXCLUDED = {"Utilities", "Real Estate", "Financial Services"}
# Note : certaines conventions yfinance = "Financials", d'autres = "Financial Services"


# ─── 1. Univers : Russell 1000 → filtre mid-cap ──────────────────────────

IWB_HOLDINGS_URL = (
    "https://www.ishares.com/us/products/239707/ishares-russell-1000-etf/"
    "1467271812596.ajax?fileType=csv&fileName=IWB_holdings&dataType=fund"
)
IWB_CACHE = PEAD_DIR / "iwb_holdings_raw.csv"
UNIVERSE_CSV = PEAD_DIR / "universe.csv"


def fetch_russell_1000() -> pd.DataFrame:
    """Télécharge le CSV holdings IWB (Russell 1000 ETF) et le parse."""
    import requests

    r = requests.get(
        IWB_HOLDINGS_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=30,
    )
    r.raise_for_status()
    IWB_CACHE.write_bytes(r.content)

    # Le CSV a ~9 lignes de metadata avant le vrai header. On cherche "Ticker,Name,..."
    lines = IWB_CACHE.read_text(encoding="utf-8-sig").splitlines()
    header_idx = next(
        i for i, ln in enumerate(lines) if ln.strip().startswith("Ticker,Name,Sector")
    )
    df = pd.read_csv(
        IWB_CACHE, skiprows=header_idx, encoding="utf-8-sig",
        thousands=",",
    )
    # Clean : enlève lignes de total/cash/other et tickers non-equity
    df = df[df["Asset Class"] == "Equity"].copy()
    df = df[df["Ticker"].str.match(r"^[A-Z.]{1,6}$", na=False)].copy()
    df["Ticker"] = df["Ticker"].str.strip()
    return df.reset_index(drop=True)


def enrich_with_yfinance(tickers: list[str], max_workers: int = 8) -> pd.DataFrame:
    """Récupère market_cap + n_analysts + sector + beta pour chaque ticker.

    yfinance supporte les threads, on batch pour aller vite. ~1000 tickers en
    ~3-5 min typiquement.
    """
    import yfinance as yf
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _info_one(tk: str) -> dict:
        try:
            i = yf.Ticker(tk).info
            return {
                "ticker": tk,
                "name": i.get("shortName") or i.get("longName") or "",
                "sector": i.get("sector", ""),
                "industry": i.get("industry", ""),
                "market_cap": i.get("marketCap", 0) or 0,
                "n_analysts": i.get("numberOfAnalystOpinions", 0) or 0,
                "beta": i.get("beta", None),
                "avg_volume_3m": i.get("averageVolume", 0) or 0,
                "status": "ok",
            }
        except Exception as e:
            return {"ticker": tk, "status": f"err: {e}"}

    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_info_one, tk): tk for tk in tickers}
        for f in as_completed(futures):
            rows.append(f.result())
    return pd.DataFrame(rows)


def build_universe(
    market_cap_min: float = DEFAULT_MARKET_CAP_MIN,
    market_cap_max: float = DEFAULT_MARKET_CAP_MAX,
    exclude_sectors: set[str] | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Construit l'univers PEAD cible.

    1. Fetch Russell 1000 holdings (cached)
    2. Enrichi chaque ticker avec yfinance (market_cap, n_analysts, sector)
    3. Filtre market_cap ∈ [min, max]
    4. Exclut secteurs inaptes au PEAD (REITs, utilities, banks)
    5. Sauvegarde dans data/pead/universe.csv

    Retourne le DataFrame final trié par n_analysts ASC (les moins couvertes
    en premier = meilleurs candidats PEAD théoriques).
    """
    exclude_sectors = exclude_sectors or DEFAULT_SECTORS_EXCLUDED

    print(f"[pead.universe] Fetching Russell 1000 holdings...")
    r1k = fetch_russell_1000()
    print(f"  → {len(r1k)} tickers in Russell 1000")

    print(f"[pead.universe] Enriching via yfinance ({len(r1k)} tickers, ~3-5 min)...")
    t0 = _time.time()
    enriched = enrich_with_yfinance(r1k["Ticker"].tolist())
    print(f"  → done in {_time.time() - t0:.1f}s — "
          f"{(enriched['status'] == 'ok').sum()} OK, "
          f"{(enriched['status'] != 'ok').sum()} errors")

    # Merge IWB sector comme fallback
    enriched = enriched.merge(
        r1k[["Ticker", "Sector", "Weight (%)"]].rename(columns={"Ticker": "ticker"}),
        on="ticker", how="left",
    )
    enriched["sector_final"] = enriched["sector"].where(
        enriched["sector"].str.len() > 0, enriched["Sector"]
    )

    # Filtres
    before = len(enriched)
    uni = enriched[enriched["status"] == "ok"].copy()
    uni = uni[uni["market_cap"].between(market_cap_min, market_cap_max)]
    uni = uni[~uni["sector_final"].isin(exclude_sectors)]
    uni = uni.sort_values("n_analysts", ascending=True).reset_index(drop=True)

    print(f"[pead.universe] Filters:")
    print(f"  market_cap ∈ [${market_cap_min/1e9:.1f}B, ${market_cap_max/1e9:.1f}B] "
          f"+ exclude {exclude_sectors} → {len(uni)} / {before} retained")

    # Stats pour reporting
    print(f"\n[pead.universe] Stats de l'univers retenu :")
    print(f"  N tickers                : {len(uni)}")
    print(f"  market_cap médian        : ${uni['market_cap'].median()/1e9:.1f}B")
    print(f"  analystes moyen          : {uni['n_analysts'].mean():.1f}")
    print(f"  analystes médian         : {uni['n_analysts'].median():.0f}")
    print(f"  distribution analystes   : "
          f"min={uni['n_analysts'].min()}, "
          f"p25={uni['n_analysts'].quantile(0.25):.0f}, "
          f"p75={uni['n_analysts'].quantile(0.75):.0f}, "
          f"max={uni['n_analysts'].max()}")
    print(f"  secteurs top 5           :")
    for sec, n in uni["sector_final"].value_counts().head(5).items():
        print(f"    - {sec}: {n}")

    uni.to_csv(UNIVERSE_CSV, index=False)
    print(f"\n[pead.universe] Sauvegardé : {UNIVERSE_CSV}")
    return uni


def load_universe() -> pd.DataFrame:
    """Charge l'univers depuis le CSV. Build si absent."""
    if not UNIVERSE_CSV.exists():
        return build_universe()
    return pd.read_csv(UNIVERSE_CSV)


# ─── 2. Earnings : FMP (EPS actuals) + Nasdaq (timing bmo/amc) ──────────

FMP_EARNINGS_URL = "https://financialmodelingprep.com/stable/earnings-calendar"
NASDAQ_EARNINGS_URL = "https://api.nasdaq.com/api/calendar/earnings"
EARNINGS_DIR = PEAD_DIR / "earnings"


def fetch_earnings_range(from_date: str, to_date: str) -> pd.DataFrame:
    """FMP earnings-calendar pour une plage de dates. Retourne symbol, date,
    epsActual, epsEstimated, revenueActual, revenueEstimated."""
    import requests
    r = requests.get(
        FMP_EARNINGS_URL,
        params={"from": from_date, "to": to_date, "apikey": FMP_API_KEY},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data)


def fetch_nasdaq_timing(date_str: str) -> pd.DataFrame:
    """Nasdaq public API, retourne symbol + time (time-pre-market/time-after-hours/etc)
    pour une date donnée (format YYYY-MM-DD)."""
    import requests
    r = requests.get(
        NASDAQ_EARNINGS_URL,
        params={"date": date_str},
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    rows = data.get("data", {}).get("rows") or []
    if not rows:
        return pd.DataFrame(columns=["symbol", "time"])
    df = pd.DataFrame(rows)
    df["timing"] = df["time"].map({
        "time-pre-market": "bmo",
        "time-after-hours": "amc",
        "time-not-supplied": "unknown",
    }).fillna("unknown")
    return df[["symbol", "timing"]]


def _infer_timing_from_hour(ts: pd.Timestamp) -> str:
    """Déduit bmo/amc à partir de l'heure NY de l'annonce.
    - < 09:30 NY → bmo (before market open)
    - >= 16:00 NY → amc (after market close)
    - entre 09:30 et 16:00 → during (rare)
    """
    h = ts.hour + ts.minute / 60.0
    if h < 9.5:
        return "bmo"
    if h >= 16.0:
        return "amc"
    return "during"


def build_earnings_for_ticker(
    ticker: str,
    start_date: str = "2020-01-01",
    end_date: str | None = None,
    limit: int = 60,
) -> pd.DataFrame:
    """Earnings historiques via yfinance (gratuit, pas de quota).

    Saves to data/pead/earnings/<TICKER>.csv
    Format : date;timing;eps_actual;eps_estimated;surprise_pct
    """
    import yfinance as yf
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    try:
        df = yf.Ticker(ticker).get_earnings_dates(limit=limit)
    except Exception as e:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    df = df.rename(columns={
        "Earnings Date": "ts",
        "EPS Estimate": "eps_estimated",
        "Reported EPS": "eps_actual",
        "Surprise(%)": "surprise_pct",
    })
    df["ts"] = pd.to_datetime(df["ts"], utc=True).dt.tz_convert("America/New_York")
    df["date"] = df["ts"].dt.strftime("%Y-%m-%d")
    df["timing"] = df["ts"].apply(_infer_timing_from_hour)
    # Filtre fenêtre temporelle
    df = df[(df["date"] >= start_date) & (df["date"] <= end_date)].copy()
    if df.empty:
        return df
    cols = ["date", "timing", "eps_actual", "eps_estimated", "surprise_pct"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df.sort_values("date").reset_index(drop=True)
    out_path = EARNINGS_DIR / f"{ticker}.csv"
    df[cols].to_csv(out_path, index=False, sep=";")
    return df[cols]


def build_earnings_batch(tickers: list[str], max_workers: int = 8) -> dict[str, int]:
    """Fetch earnings en parallèle. Retourne {ticker: n_rows}."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = {}

    def _one(tk):
        try:
            df = build_earnings_for_ticker(tk)
            return tk, len(df)
        except Exception:
            return tk, -1

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_one, tk): tk for tk in tickers}
        done = 0
        for f in as_completed(futures):
            tk, n = f.result()
            results[tk] = n
            done += 1
            if done % 50 == 0:
                print(f"  [earnings] {done}/{len(tickers)} — last: {tk} ({n})")
    return results


def enrich_with_nasdaq_timing(earnings_df: pd.DataFrame) -> pd.DataFrame:
    """Pour chaque date unique dans earnings_df, fetch Nasdaq et merge timing.
    Plus efficace que 1 call par ticker."""
    out = earnings_df.copy()
    dates = sorted(pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d").unique())
    all_timing = []
    for i, dt in enumerate(dates):
        if i % 20 == 0:
            print(f"  [nasdaq timing] {i}/{len(dates)}")
        try:
            t = fetch_nasdaq_timing(dt)
            t["date"] = dt
            all_timing.append(t)
            _time.sleep(0.3)  # courtoisie Nasdaq
        except Exception as e:
            print(f"  [nasdaq timing] {dt} skip: {e}")
    if not all_timing:
        return out
    timing_df = pd.concat(all_timing, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    out = out.merge(
        timing_df.rename(columns={"symbol": "ticker"}),
        on=["ticker", "date"], how="left",
    )
    out["timing"] = out["timing_y"].fillna(out.get("timing_x", "unknown")).fillna("unknown")
    out = out.drop(columns=[c for c in ["timing_x", "timing_y"] if c in out.columns])
    return out


# ─── 3. OHLCV download via yfinance ─────────────────────────────────────

TICKERS_DIR = PEAD_DIR / "tickers"


def download_ohlcv(ticker: str, start: str = "2020-01-01",
                   end: str | None = None) -> pd.DataFrame:
    """Télécharge OHLCV daily via yfinance. Save CSV."""
    import yfinance as yf
    end = end or datetime.now().strftime("%Y-%m-%d")
    df = yf.Ticker(ticker).history(start=start, end=end, auto_adjust=False)
    if df.empty:
        return df
    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    cols = ["Date", "Open", "High", "Low", "Close", "Volume"]
    df = df[cols].rename(columns={c: c.lower() for c in cols})
    df = df.rename(columns={"date": "time"})
    out_path = TICKERS_DIR / f"{ticker}.csv"
    df.to_csv(out_path, index=False, sep=";")
    return df


def download_ohlcv_batch(tickers: list[str],
                          start: str = "2020-01-01",
                          max_workers: int = 8) -> dict[str, int]:
    """Télécharge OHLCV en parallèle. Retourne {ticker: n_rows}."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    results = {}

    def _one(tk):
        try:
            df = download_ohlcv(tk, start=start)
            return tk, len(df)
        except Exception as e:
            return tk, -1

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_one, tk): tk for tk in tickers}
        done = 0
        for f in as_completed(futures):
            tk, n = f.result()
            results[tk] = n
            done += 1
            if done % 50 == 0:
                print(f"  [ohlcv] {done}/{len(tickers)} — last: {tk} ({n} rows)")
    return results


# ─── 4. Signal detection : compression + surprise ─────────────────────

def _load_ohlcv(ticker: str) -> pd.DataFrame:
    p = TICKERS_DIR / f"{ticker}.csv"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, sep=";")
    df["time"] = pd.to_datetime(df["time"])
    return df.sort_values("time").reset_index(drop=True)


def _load_earnings(ticker: str) -> pd.DataFrame:
    p = EARNINGS_DIR / f"{ticker}.csv"
    if not p.exists():
        return pd.DataFrame()
    df = pd.read_csv(p, sep=";")
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def detect_signal_at(
    ohlcv: pd.DataFrame,
    earnings_date: pd.Timestamp,
    timing: str = "unknown",
    compression_thr: float = DEFAULT_COMPRESSION_THRESHOLD,
    surprise_thr: float = DEFAULT_SURPRISE_THRESHOLD,
    compression_mode: str = DEFAULT_COMPRESSION_MODE,   # "all" | "avg" | "median" | "n_of_5"
    n_of_5: int = 5,
) -> dict | None:
    """Detect PEAD signal for ONE earnings event. Retourne dict ou None.

    Convention :
    - timing='amc' → J = premier jour de bourse APRÈS earnings_date
    - timing='bmo' → J = premier jour de bourse EGAL OU APRÈS earnings_date
    - timing='unknown' → J = premier jour de bourse >= earnings_date (fallback amc
      par défaut si match exact disponible dans ohlcv)

    Conditions :
    1. Compression J-5..J-1 : pour CHAQUE des 5 jours, range < compression_thr × range_mean_20
    2. Surprise on J : Close_J > Close_J-1 × (1+surprise_thr)  [LONG]
                    or Close_J < Close_J-1 × (1-surprise_thr)  [SHORT]
    """
    ed = pd.Timestamp(earnings_date).normalize()

    # Détermine l'index J dans ohlcv
    # Pour BMO : J = même jour de trading que l'earnings
    # Pour AMC : J = jour de trading suivant
    if timing == "bmo":
        target = ed
    else:  # amc ou unknown
        target = ed + pd.Timedelta(days=1)

    # Trouve le J le plus proche (next trading day >= target)
    future = ohlcv[ohlcv["time"] >= target]
    if len(future) == 0:
        return None
    j_row = future.iloc[0]
    j_idx = future.index[0]

    # Besoin : J-20 à J+5 disponibles
    if j_idx < 20 or j_idx + 5 >= len(ohlcv):
        return None

    # Compression check J-5..J-1 (indices j_idx-5 to j_idx-1)
    window_pre = ohlcv.iloc[j_idx - 5: j_idx].copy()
    window_20 = ohlcv.iloc[j_idx - 20: j_idx].copy()
    window_20["range"] = window_20["high"] - window_20["low"]
    mean_range_20 = window_20["range"].mean()
    if mean_range_20 <= 0:
        return None

    window_pre["range"] = window_pre["high"] - window_pre["low"]
    window_pre["ratio"] = window_pre["range"] / mean_range_20
    if compression_mode == "all":
        compression_ok = bool((window_pre["ratio"] < compression_thr).all())
    elif compression_mode == "avg":
        compression_ok = bool(window_pre["ratio"].mean() < compression_thr)
    elif compression_mode == "median":
        compression_ok = bool(window_pre["ratio"].median() < compression_thr)
    elif compression_mode == "n_of_5":
        compression_ok = bool((window_pre["ratio"] < compression_thr).sum() >= n_of_5)
    else:
        compression_ok = False
    max_pre_ratio = float(window_pre["ratio"].max())
    avg_pre_ratio = float(window_pre["ratio"].mean())

    # Surprise on J
    close_j = float(j_row["close"])
    close_j_1 = float(ohlcv.iloc[j_idx - 1]["close"])
    if close_j_1 <= 0:
        return None
    var_j = (close_j - close_j_1) / close_j_1

    direction = None
    if var_j >= surprise_thr:
        direction = "long"
    elif var_j <= -surprise_thr:
        direction = "short"

    if direction is None or not compression_ok:
        return {
            "signal_valid": False,
            "compression_ok": compression_ok,
            "max_pre_ratio": max_pre_ratio,
            "var_j_pct": round(var_j * 100, 2),
            "j_date": j_row["time"].strftime("%Y-%m-%d"),
            "j_close": round(close_j, 2),
        }

    # Entry Open J+1, Exit Close J+5
    entry_row = ohlcv.iloc[j_idx + 1]
    exit_row = ohlcv.iloc[j_idx + 5]
    entry_open = float(entry_row["open"])
    exit_close = float(exit_row["close"])
    if entry_open <= 0:
        return None

    raw_ret = (exit_close - entry_open) / entry_open
    # Sens du trade : long = on gagne si monte ; short = on gagne si baisse
    pnl_pct = raw_ret * 100 if direction == "long" else -raw_ret * 100

    return {
        "signal_valid": True,
        "direction": direction,
        "compression_ok": True,
        "max_pre_ratio": round(max_pre_ratio, 3),
        "var_j_pct": round(var_j * 100, 2),
        "j_date": j_row["time"].strftime("%Y-%m-%d"),
        "j_close": round(close_j, 2),
        "entry_date": entry_row["time"].strftime("%Y-%m-%d"),
        "entry_open": round(entry_open, 2),
        "exit_date": exit_row["time"].strftime("%Y-%m-%d"),
        "exit_close": round(exit_close, 2),
        "raw_ret_pct": round(raw_ret * 100, 2),
        "pnl_pct": round(pnl_pct, 2),
    }


# ─── 5. Backtest historique ─────────────────────────────────────────────

def backtest(
    universe_df: pd.DataFrame | None = None,
    compression_thr: float = DEFAULT_COMPRESSION_THRESHOLD,
    surprise_thr: float = DEFAULT_SURPRISE_THRESHOLD,
    compression_mode: str = DEFAULT_COMPRESSION_MODE,
    n_of_5: int = 5,
) -> pd.DataFrame:
    """Itère sur tous les earnings historiques de l'univers, applique les
    filtres, accumule les signaux valides. Retourne un DataFrame trié."""
    uni = universe_df if universe_df is not None else load_universe()
    signals = []

    for i, row in uni.iterrows():
        ticker = row["ticker"]
        ohlcv = _load_ohlcv(ticker)
        if ohlcv.empty:
            continue
        earns = _load_earnings(ticker)
        if earns.empty:
            continue

        for _, e in earns.iterrows():
            res = detect_signal_at(
                ohlcv, e["date"], timing=e.get("timing", "unknown"),
                compression_thr=compression_thr, surprise_thr=surprise_thr,
                compression_mode=compression_mode, n_of_5=n_of_5,
            )
            if res and res.get("signal_valid"):
                signals.append({
                    "ticker": ticker,
                    "earnings_date": e["date"].strftime("%Y-%m-%d"),
                    "timing": e.get("timing", "unknown"),
                    **res,
                    "sector": row.get("sector_final") or row.get("sector"),
                    "market_cap_b": round(row.get("market_cap", 0) / 1e9, 2),
                    "n_analysts": row.get("n_analysts", 0),
                })

        if i % 50 == 0 and i > 0:
            print(f"  [backtest] {i}/{len(uni)} — signals so far: {len(signals)}")

    df = pd.DataFrame(signals)
    out_path = PEAD_DIR / "backtest_signals.csv"
    if not df.empty:
        df = df.sort_values("earnings_date").reset_index(drop=True)
        df.to_csv(out_path, index=False, sep=";")
    return df


def backtest_summary(signals_df: pd.DataFrame) -> dict:
    """Stats agrégées du backtest."""
    if signals_df.empty:
        return {"n_signals": 0}
    df = signals_df
    wins = df[df["pnl_pct"] > 0]
    longs = df[df["direction"] == "long"]
    shorts = df[df["direction"] == "short"]
    return {
        "n_signals": len(df),
        "n_long": len(longs),
        "n_short": len(shorts),
        "win_rate_pct": round(len(wins) / len(df) * 100, 1),
        "avg_pnl_pct": round(df["pnl_pct"].mean(), 2),
        "median_pnl_pct": round(df["pnl_pct"].median(), 2),
        "best_pnl_pct": round(df["pnl_pct"].max(), 2),
        "worst_pnl_pct": round(df["pnl_pct"].min(), 2),
        "best_trade": df.loc[df["pnl_pct"].idxmax(), ["ticker", "earnings_date", "pnl_pct"]].to_dict() if len(df) else None,
        "worst_trade": df.loc[df["pnl_pct"].idxmin(), ["ticker", "earnings_date", "pnl_pct"]].to_dict() if len(df) else None,
        "long_win_rate_pct": round((longs["pnl_pct"] > 0).mean() * 100, 1) if len(longs) else None,
        "short_win_rate_pct": round((shorts["pnl_pct"] > 0).mean() * 100, 1) if len(shorts) else None,
        "long_avg_pnl_pct": round(longs["pnl_pct"].mean(), 2) if len(longs) else None,
        "short_avg_pnl_pct": round(shorts["pnl_pct"].mean(), 2) if len(shorts) else None,
    }


# ─── 6. Alerting (réutilise beta2_engulfing/notifiers.py) ──────────────

def _import_notifiers():
    """Importe send_telegram + send_email depuis la conv BBE. Lazy pour éviter
    le coût d'import au boot de pead_engine."""
    import sys
    notif_dir = BASE_DIR / "beta2_engulfing"
    if str(notif_dir) not in sys.path:
        sys.path.insert(0, str(notif_dir))
    try:
        from notifiers import send_telegram, send_email  # type: ignore
        return send_telegram, send_email
    except Exception as e:
        print(f"[pead.alerts] import notifiers failed: {e}")
        return None, None


def send_pead_alert(
    kind: str,           # "pre_earnings" (compression détectée J-N) ou "signal" (surprise déclenchée)
    ticker: str,
    details: dict,
    dry_run: bool = False,
) -> dict:
    """Envoie une alerte Telegram + email pour un signal PEAD.

    details attendu:
      pre_earnings: {days_until, earnings_date, timing, market_cap_b, compression_ratio, n_analysts}
      signal: {direction, earnings_date, timing, var_j_pct, j_close, entry_date, entry_open, ...}
    """
    if kind == "pre_earnings":
        title = f"📈 PEAD pré-earnings — {ticker}"
        body_md = (
            f"*{ticker}* (mid-cap ${details.get('market_cap_b','?')}B, "
            f"{details.get('n_analysts','?')} analystes)\n"
            f"Earnings dans *{details.get('days_until','?')}j* "
            f"({details.get('earnings_date','?')}, {details.get('timing','?')}).\n"
            f"Compression pré-earnings détectée (ratio max {details.get('compression_ratio','?')}).\n"
            f"→ Surveiller le post-earnings pour un éventuel signal."
        )
    else:  # signal
        direction = details.get("direction", "?")
        emoji = "🟢" if direction == "long" else "🔴"
        title = f"{emoji} PEAD signal {direction.upper()} — {ticker}"
        body_md = (
            f"*{ticker}* — Signal PEAD *{direction.upper()}*\n"
            f"Earnings : {details.get('earnings_date','?')} ({details.get('timing','?')})\n"
            f"Surprise Close J vs J-1 : *{details.get('var_j_pct','?')}%*\n"
            f"Close J : ${details.get('j_close','?')}\n"
            f"*Entrée prévue* : Open {details.get('entry_date','?')}  "
            f"(≈ ${details.get('entry_open','?')})\n"
            f"*Sortie prévue* : Close {details.get('exit_date','?')}"
        )
    body_html = body_md.replace("\n", "<br>").replace("*", "<b>")  # simple md→html

    if dry_run:
        print(f"[pead.alerts DRY RUN] {title}\n{body_md}")
        return {"telegram": "skipped", "email": "skipped"}

    send_tg, send_mail = _import_notifiers()
    results = {}
    if send_tg:
        results["telegram"] = "ok" if send_tg(f"*{title}*\n\n{body_md}", markdown=True) else "fail"
    if send_mail:
        results["email"] = "ok" if send_mail(title, f"<h3>{title}</h3><p>{body_html}</p>") else "fail"
    # Log local
    log_path = PEAD_DIR / "alerts.log"
    with log_path.open("a") as f:
        f.write(f"{datetime.now().isoformat()} | {kind} | {ticker} | {results}\n")
    return results


# ─── 7. Daily scan ─────────────────────────────────────────────────────

def daily_scan(dry_run: bool = False) -> dict:
    """Scan quotidien : détecte pré-earnings J-3..J-1 (compression) et
    signaux earnings-day (J). Envoie les alertes.

    Retourne {"pre_earnings": [...], "signals": [...]}.
    """
    uni = load_universe()
    uni_set = set(uni["ticker"])
    today = datetime.now()

    # Earnings dans la fenêtre ±3 jours
    window_start = (today - timedelta(days=3)).strftime("%Y-%m-%d")
    window_end = (today + timedelta(days=3)).strftime("%Y-%m-%d")
    cal = fetch_earnings_range(window_start, window_end)
    cal = cal[cal["symbol"].isin(uni_set)].copy()

    # Enrichi timing via Nasdaq (un call par date unique)
    dates_unique = sorted(cal["date"].unique())
    timing_cache = {}
    for dt in dates_unique:
        try:
            t = fetch_nasdaq_timing(dt)
            for _, r in t.iterrows():
                timing_cache[(r["symbol"], dt)] = r["timing"]
        except Exception:
            pass

    pre_earnings_alerts = []
    signal_alerts = []

    for _, row in cal.iterrows():
        ticker = row["symbol"]
        earnings_date = pd.Timestamp(row["date"]).normalize()
        timing = timing_cache.get((ticker, row["date"]), "unknown")
        days_until = (earnings_date - pd.Timestamp(today).normalize()).days

        # Récup info univers
        uni_row = uni[uni["ticker"] == ticker].iloc[0]
        market_cap_b = round(uni_row.get("market_cap", 0) / 1e9, 2)
        n_analysts = int(uni_row.get("n_analysts", 0) or 0)

        ohlcv = _load_ohlcv(ticker)
        if ohlcv.empty:
            continue

        if days_until > 0:
            # Pré-earnings : check compression sur les derniers jours
            last_5 = ohlcv.tail(5).copy()
            last_20 = ohlcv.tail(20).copy()
            last_20["range"] = last_20["high"] - last_20["low"]
            mean_range = last_20["range"].mean()
            last_5["range"] = last_5["high"] - last_5["low"]
            last_5["ratio"] = last_5["range"] / mean_range
            max_ratio = float(last_5["ratio"].max())
            if (last_5["ratio"] < DEFAULT_COMPRESSION_THRESHOLD).all():
                details = {
                    "days_until": days_until,
                    "earnings_date": row["date"],
                    "timing": timing,
                    "market_cap_b": market_cap_b,
                    "compression_ratio": round(max_ratio, 3),
                    "n_analysts": n_analysts,
                }
                pre_earnings_alerts.append({"ticker": ticker, **details})
                send_pead_alert("pre_earnings", ticker, details, dry_run=dry_run)
        elif days_until >= -1:
            # Earnings day or J+0 : check signal
            res = detect_signal_at(ohlcv, earnings_date, timing=timing)
            if res and res.get("signal_valid"):
                details = {
                    "direction": res["direction"],
                    "earnings_date": row["date"],
                    "timing": timing,
                    "var_j_pct": res["var_j_pct"],
                    "j_close": res["j_close"],
                    "entry_date": res.get("entry_date", "?"),
                    "entry_open": res.get("entry_open", "?"),
                    "exit_date": res.get("exit_date", "?"),
                }
                signal_alerts.append({"ticker": ticker, **details})
                send_pead_alert("signal", ticker, details, dry_run=dry_run)

    # Save scan snapshot
    scan_result = {
        "timestamp": datetime.now().isoformat(),
        "pre_earnings": pre_earnings_alerts,
        "signals": signal_alerts,
    }
    scan_path = PEAD_DIR / "signals" / f"scan_{today.strftime('%Y%m%d')}.json"
    scan_path.write_text(json.dumps(scan_result, indent=2))
    print(f"[pead.daily_scan] {len(pre_earnings_alerts)} pré-earnings | "
          f"{len(signal_alerts)} signaux | dry_run={dry_run}")
    return scan_result


# ─── CLI ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    cmd = sys.argv[1] if len(sys.argv) > 1 else "universe"
    if cmd == "universe":
        build_universe()
    elif cmd == "ohlcv":
        uni = load_universe()
        print(f"[cli] Downloading OHLCV for {len(uni)} tickers...")
        t0 = _time.time()
        res = download_ohlcv_batch(uni["ticker"].tolist())
        ok = sum(1 for n in res.values() if n > 0)
        print(f"  done in {_time.time() - t0:.1f}s — {ok}/{len(res)} OK")
    elif cmd == "earnings":
        uni = load_universe()
        tickers = uni["ticker"].tolist()
        print(f"[cli] Fetching earnings via yfinance for {len(tickers)} tickers...")
        t0 = _time.time()
        res = build_earnings_batch(tickers)
        ok = sum(1 for n in res.values() if n > 0)
        total_events = sum(n for n in res.values() if n > 0)
        print(f"  done in {_time.time() - t0:.1f}s — {ok}/{len(tickers)} OK — "
              f"{total_events} earnings events total")
    elif cmd == "backtest":
        print("[cli] Running backtest on all earnings...")
        t0 = _time.time()
        sig = backtest()
        print(f"  → {len(sig)} signals in {_time.time() - t0:.1f}s")
        from pprint import pprint
        pprint(backtest_summary(sig))
    elif cmd == "scan":
        scan = daily_scan(dry_run=True)
        print("\n--- SCAN RESULT (dry_run) ---")
        from pprint import pprint
        pprint(scan)
    elif cmd == "upcoming":
        # Earnings dans les prochains 3 jours et passés 3 jours — pour alerting
        today = datetime.now()
        df = fetch_earnings_range(
            (today - timedelta(days=3)).strftime("%Y-%m-%d"),
            (today + timedelta(days=3)).strftime("%Y-%m-%d"),
        )
        uni_set = set(load_universe()["ticker"])
        df = df[df["symbol"].isin(uni_set)]
        print(f"Earnings ±3j dans l'univers PEAD : {len(df)}")
        print(df[["symbol", "date", "epsActual", "epsEstimated"]].to_string(index=False))
    else:
        print("Commandes : universe | ohlcv | earnings | upcoming")
