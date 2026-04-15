# ticker_analysis.py — Module d'analyse ticker (AAOI, AAPL, etc.)
# Route automatiquement les questions sur tickers individuels, sans sqlcoder.
# Utilisé par app_local.py via _compute_ticker_analysis()

import re
import math
from pathlib import Path

import pandas as pd
import numpy as np

DATA_DIR = Path(__file__).resolve().parent / "data" / "live_selected"
TICKERS_DIR = DATA_DIR / "tickers"

# ─── Fonctions BBE strictes ────────────────────────────────────────────────

def load_earnings_dates(ticker: str, data_dir: Path = None) -> set:
    """Charge les dates d'earnings depuis ticker_earnings.csv."""
    if data_dir is None:
        data_dir = DATA_DIR
    for search_dir in [data_dir, data_dir / "tickers", TICKERS_DIR]:
        for fname in [f"{ticker}_earnings.csv",
                      f"{ticker.upper()}_earnings.csv",
                      f"{ticker.lower()}_earnings.csv"]:
            p = search_dir / fname
            if p.exists():
                try:
                    # Auto-detect separator
                    first_line = p.read_text().split("\n")[0]
                    sep = ";" if ";" in first_line else ","
                    df = pd.read_csv(p, sep=sep)
                    date_col = next((c for c in df.columns
                                     if "date" in c.lower() or "time" in c.lower()),
                                    df.columns[0])
                    dates = pd.to_datetime(df[date_col].astype(str).str.strip(),
                                           format="mixed", errors="coerce").dropna()
                    return set(dates.dt.normalize())
                except Exception:
                    pass
    return set()


def detect_engulfing_strict(df: pd.DataFrame,
                            pattern: str = "bearish",
                            earnings_dates: set = None,
                            earnings_buffer_days: int = 5) -> pd.DataFrame:
    """
    Détection BBE stricte avec :
    - Confirmation volume (vol J > vol J-1)
    - Corps J > Corps J-1 × 1.1 (engulfing significatif)
    - Filtre earnings ±5j (si earnings_dates fourni)
    """
    df = df.copy().sort_values("time").reset_index(drop=True)

    df["body"] = (df["close"] - df["open"]).abs()
    df["is_green"] = df["close"] > df["open"]
    df["is_red"] = df["close"] < df["open"]
    has_vol = "volume" in df.columns

    results = []

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        curr = df.iloc[i]
        date = pd.Timestamp(curr["time"]).normalize() if "time" in curr.index else pd.Timestamp(curr.name).normalize()

        if earnings_dates and any(
            abs((date - e).days) <= earnings_buffer_days
            for e in earnings_dates
        ):
            continue

        if pattern == "bearish":
            if not (prev["is_green"] and curr["is_red"]):
                continue
            if curr["open"] < prev["close"] * 0.999:
                continue
            if curr["close"] >= prev["open"]:
                continue
            if curr["body"] < prev["body"] * 1.1:
                continue
            if has_vol and prev["volume"] > 0:
                if curr["volume"] < prev["volume"]:
                    continue
        elif pattern == "bullish":
            if not (prev["is_red"] and curr["is_green"]):
                continue
            if curr["open"] > prev["close"] * 1.001:
                continue
            if curr["close"] <= prev["open"]:
                continue
            if curr["body"] < prev["body"] * 1.1:
                continue
            if has_vol and prev["volume"] > 0:
                if curr["volume"] < prev["volume"]:
                    continue
        else:
            continue

        vol_ratio = (curr["volume"] / prev["volume"]
                     if has_vol and prev["volume"] > 0 else None)
        body_ratio = curr["body"] / prev["body"] if prev["body"] > 0 else None

        results.append({
            "date": date.strftime("%Y-%m-%d"),
            "open": round(float(curr["open"]), 2),
            "high": round(float(curr["high"]), 2),
            "low": round(float(curr["low"]), 2),
            "close": round(float(curr["close"]), 2),
            "var_j": round((curr["close"] / prev["close"] - 1) * 100, 2)
                     if prev["close"] > 0 else 0,
            "vol_ratio": round(vol_ratio, 2) if vol_ratio else None,
            "body_ratio": round(body_ratio, 2) if body_ratio else None,
            "confirmed_volume": (vol_ratio > 1) if vol_ratio else False,
            "prev_close": round(float(prev["close"]), 2),
            "prev_open": round(float(prev["open"]), 2),
        })

    return pd.DataFrame(results)


def _find_ticker_csv(ticker: str) -> Path | None:
    """Cherche le CSV ticker dans DATA_DIR et TICKERS_DIR."""
    for d in [DATA_DIR, TICKERS_DIR]:
        for pattern in [f"{ticker}.csv", f"{ticker.upper()}.csv",
                        f"{ticker.lower()}.csv",
                        f"{ticker}_daily.csv", f"{ticker.upper()}_daily.csv"]:
            p = d / pattern
            if p.exists():
                return p
    return None


# ─── Regex détection type de question ─────────────────────────────────────

_BBE_RE = re.compile(
    r"(bearish|bullish)\s*(engulfing|e\b|eng\b)",
    re.IGNORECASE
)

_FILTER_RE = re.compile(
    r"\b(perdu|perd|chut[eé]|baiss[eé]|gagn[eé]|mont[eé]|hauss[eé])"
    r"\s+(?:de\s+)?[+\-]?(\d+[\.,]?\d*)\s*%"
    r"\s*(ou\s+plus|ou\s+moins|\+|minimum|min)?",
    re.IGNORECASE,
)
_FILTER_UP_RE = re.compile(
    r"\b(?:plus\s+de|sup[eé]rieur[e]?\s+[àa]|au[- ]dessus\s+de)\s+(\d+[\.,]?\d*)\s*%",
    re.IGNORECASE,
)
_FILTER_DOWN_RE = re.compile(
    r"\b(?:moins\s+de|inf[eé]rieur[e]?\s+[àa]|en[- ]dessous\s+de)\s+(\d+[\.,]?\d*)\s*%",
    re.IGNORECASE,
)
_FILTER_ALL_RE = re.compile(
    r"\btoutes?\s+les\s+dates\b|\bjours?\s+où\b|\bquand\b.*%",
    re.IGNORECASE,
)
_STATS_RE = re.compile(
    r"\bcombien\s+de\s+fois\b|\ben\s+moyenne\b|\bquel\s+jour\b|\bfr[eé]quence\b",
    re.IGNORECASE,
)
_STATS_WEEKDAY_RE = re.compile(
    r"\bquel\s+jour\b|\bmeilleur\s+jour\b|\bpire\s+jour\b|\bjour\s+de\s+la\s+semaine\b"
    r"|\bpar\s+jour\b",
    re.IGNORECASE,
)
_STATS_MONTH_RE = re.compile(
    r"\bquel\s+mois\b|\bmeilleur\s+mois\b|\bpire\s+mois\b|\bpar\s+mois\b",
    re.IGNORECASE,
)
_COUNT_RE = re.compile(
    r"\bcombien\s+de\s+fois\b.*\b(baiss|perd|chut|mont|hauss|gagn)",
    re.IGNORECASE,
)
_NEXT_RE = re.compile(
    r"\blendemain\b|\bJ\+1\b|\bapr[eè]s\b|\bsuite\s+[aà]\b|\bpositif\b.*\blendemain\b"
    r"|\bn[eé]gatif\b.*\blendemain\b|\brebond\b",
    re.IGNORECASE,
)
_PERIOD_YEAR_RE = re.compile(r"\ben\s+(20\d{2})\b", re.IGNORECASE)
_PERIOD_SINCE_RE = re.compile(
    r"\bdepuis\s+(janvier|f[eé]vrier|mars|avril|mai|juin|juillet"
    r"|ao[uû]t|septembre|octobre|novembre|d[eé]cembre)(?:\s+(20\d{2}))?\b",
    re.IGNORECASE,
)
_PERIOD_Q_RE = re.compile(r"\b[QqTt]([1-4])\s*(20\d{2})?\b")
_THRESHOLD_RE = re.compile(r"[+\-]?(\d+[\.,]?\d*)\s*%")

_MONTH_MAP = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}
_MONTH_LABELS = {
    1: "Janvier", 2: "Février", 3: "Mars", 4: "Avril", 5: "Mai", 6: "Juin",
    7: "Juillet", 8: "Août", 9: "Septembre", 10: "Octobre", 11: "Novembre", 12: "Décembre",
}
_DOW_LABELS = {0: "Lundi", 1: "Mardi", 2: "Mercredi", 3: "Jeudi", 4: "Vendredi"}

# ─── Chargement données ──────────────────────────────────────────────────


def _to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str).str.replace(",", ".").str.replace(r"\s+", "", regex=True).str.strip(),
        errors="coerce",
    )


def _load_ticker_daily(ticker: str) -> pd.DataFrame | None:
    candidates = [
        DATA_DIR / f"{ticker.upper()}_daily.csv",
        DATA_DIR / f"{ticker.upper()}.csv",
        TICKERS_DIR / f"{ticker.upper()}_daily.csv",
        TICKERS_DIR / f"{ticker.upper()}.csv",
    ]
    for p in candidates:
        if p.exists():
            df = pd.read_csv(p, sep=";")
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
            df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
            for col in ("open", "close", "high", "low"):
                if col in df.columns:
                    df[col] = _to_numeric(df[col])
            if "volume" in df.columns:
                df["volume"] = _to_numeric(df["volume"])
            return df
    return None


def _load_earnings(ticker: str) -> pd.DataFrame | None:
    for search_dir in [DATA_DIR, TICKERS_DIR]:
        p = search_dir / f"{ticker.upper()}_earnings.csv"
        if p.exists():
            first_line = p.read_text().split("\n")[0]
            sep = ";" if ";" in first_line else ","
            df = pd.read_csv(p, sep=sep)
            df.columns = [c.strip().lower() for c in df.columns]
            date_col = next((c for c in df.columns if "date" in c), df.columns[0])
            df["date"] = pd.to_datetime(df[date_col].astype(str).str.strip(),
                                        format="mixed", errors="coerce")
            return df.dropna(subset=["date"])
    return None


def _prepare_daily(df: pd.DataFrame) -> pd.DataFrame:
    d = df.set_index("time").sort_index().copy()
    d["prev_close"] = d["close"].shift(1)
    d["var_pct"] = (d["close"] - d["prev_close"]) / d["prev_close"] * 100
    for h in range(1, 6):
        d[f"close_j{h}"] = d["close"].shift(-h)
        d[f"var_j{h}"] = (d[f"close_j{h}"] - d["close"]) / d["close"] * 100
    d["next_close"] = d["close_j1"]
    d["next_var"] = d["var_j1"]
    d["dow"] = d.index.dayofweek
    d["month"] = d.index.month
    d["quarter"] = d.index.quarter
    d["day_of_month"] = d.index.day
    d["year"] = d.index.year
    # Gap overnight
    if "open" in d.columns:
        d["gap_pct"] = (d["open"] - d["prev_close"]) / d["prev_close"] * 100
    # Volume metrics
    if "volume" in d.columns:
        d["vol_ma20"] = d["volume"].rolling(20).mean()
        d["vol_ratio"] = d["volume"] / d["vol_ma20"]
        d["vol_up3"] = (d["volume"] > d["volume"].shift(1)).astype(int)
        d["vol_up3"] = d["vol_up3"].rolling(3).sum()  # 3 = 3 jours consécutifs volume croissant
    # Volatility
    if "high" in d.columns and "low" in d.columns:
        d["range_pct"] = (d["high"] - d["low"]) / d["close"] * 100
        d["atr5"] = d["range_pct"].rolling(5).mean()
        d["atr20"] = d["range_pct"].rolling(20).mean()
        d["range_ma20"] = d["range_pct"].rolling(20).mean()
    # Position relative
    d["ma20"] = d["close"].rolling(20).mean()
    d["high_52w"] = d["close"].rolling(252, min_periods=20).max()
    d["low_52w"] = d["close"].rolling(252, min_periods=20).min()
    # Consecutive streaks
    up = (d["var_pct"] > 0).astype(int)
    dn = (d["var_pct"] < 0).astype(int)
    up_groups = up.ne(up.shift()).cumsum()
    dn_groups = dn.ne(dn.shift()).cumsum()
    d["consec_up"] = up.groupby(up_groups).cumsum()
    d["consec_dn"] = dn.groupby(dn_groups).cumsum()
    # Rolling extremes
    d["max_var_20"] = d["var_pct"].rolling(20).max()
    d["min_var_20"] = d["var_pct"].rolling(20).min()
    # Bougies japonaises
    if "open" in d.columns:
        d["candle_body"] = (d["close"] - d["open"]).abs()
        prev_open = d["open"].shift(1)
        prev_body = d["candle_body"].shift(1)
        prev_green = d["close"].shift(1) > prev_open
        prev_red = d["close"].shift(1) < prev_open
        curr_red = d["close"] < d["open"]
        curr_green = d["close"] > d["open"]
        d["bearish_engulfing"] = (
            prev_green & curr_red
            & (d["open"] >= d["close"].shift(1))
            & (d["close"] <= prev_open)
            & (d["candle_body"] > prev_body)
        )
        d["bullish_engulfing"] = (
            prev_red & curr_green
            & (d["open"] <= d["close"].shift(1))
            & (d["close"] >= prev_open)
            & (d["candle_body"] > prev_body)
        )
    # Low horizons pour validation engulfing
    if "low" in d.columns:
        for i in range(1, 6):
            d[f"low_j{i}"] = d["low"].shift(-i)
    return d


# ─── Détection période ───────────────────────────────────────────────────


def _apply_period(df: pd.DataFrame, query: str) -> pd.DataFrame:
    m = _PERIOD_YEAR_RE.search(query)
    if m:
        return df[df["year"] == int(m.group(1))]
    m = _PERIOD_Q_RE.search(query)
    if m:
        q = int(m.group(1))
        year = int(m.group(2)) if m.group(2) else df["year"].max()
        return df[(df["quarter"] == q) & (df["year"] == year)]
    m = _PERIOD_SINCE_RE.search(query)
    if m:
        month = _MONTH_MAP.get(m.group(1).lower().replace("é", "e").replace("û", "u"), 1)
        year = int(m.group(2)) if m.group(2) else df["year"].max()
        return df[df.index >= pd.Timestamp(year=year, month=month, day=1)]
    return df


# ─── Analyse principale ─────────────────────────────────────────────────


def analyze_ticker(ticker: str, query: str, context_dates: list[str] | None = None) -> dict:
    raw = _load_ticker_daily(ticker)
    if raw is None:
        return {"type": "TICKER_ANALYSIS", "ok": False,
                "error": f"Pas de CSV daily pour {ticker.upper()}."}

    df = _prepare_daily(raw)
    df_clean = df.dropna(subset=["prev_close", "var_pct"]).copy()
    df_period = _apply_period(df_clean, query)

    if context_dates:
        ctx_idx = pd.DatetimeIndex([pd.Timestamp(d) for d in context_dates])
        df_period = df_period[df_period.index.isin(ctx_idx)]

    # Handler STATS weekday
    if _STATS_WEEKDAY_RE.search(query):
        return _handle_stats_weekday(ticker, df_period, query)
    # Handler STATS monthly
    if _STATS_MONTH_RE.search(query):
        return _handle_stats_month(ticker, df_period, query)
    # Handler BBE dédié
    bbe_m = _BBE_RE.search(query)
    if bbe_m:
        pat = "bearish" if "bearish" in bbe_m.group(1).lower() else "bullish"
        earn_dates = load_earnings_dates(ticker)
        df_full = _prepare_daily(raw)
        df_bbe = detect_engulfing_strict(
            df_full.reset_index() if "time" not in df_full.columns else df_full.copy(),
            pattern=pat,
            earnings_dates=earn_dates
        )
        if not df_bbe.empty:
            df_bbe["date_ts"] = pd.to_datetime(df_bbe["date"])
            if not df_period.empty:
                dmin = df_period.index.min()
                dmax = df_period.index.max()
                df_bbe = df_bbe[
                    (df_bbe["date_ts"] >= dmin) &
                    (df_bbe["date_ts"] <= dmax)
                ]
        df_raw_full = df_full.reset_index() if "time" not in df_full.columns else df_full.copy()
        df_raw_full.columns = [c.strip().lower() for c in df_raw_full.columns]
        if "time" in df_raw_full.columns:
            df_raw_full["time"] = pd.to_datetime(df_raw_full["time"])
        td = sorted(df_raw_full["time"].dt.normalize().unique())
        tidx = {d: i for i, d in enumerate(td)}

        rows = []
        for _, sig in df_bbe.iterrows():
            sig_dt = pd.Timestamp(sig["date"]).normalize()
            idx = tidx.get(sig_dt)
            if idx is None:
                continue
            close_j = float(sig["close"])
            success = False
            best_perf = None
            for jj in range(1, 6):
                if idx + jj >= len(td):
                    break
                fr = df_raw_full[df_raw_full["time"].dt.normalize() == td[idx + jj]]
                if len(fr) == 0:
                    continue
                fc = float(fr.iloc[0]["close"])
                fl = float(fr.iloc[0]["low"]) if "low" in fr.columns else fc
                fh = float(fr.iloc[0]["high"]) if "high" in fr.columns else fc
                if pat == "bearish":
                    cand = min(fl, fc)
                    perf = (cand - close_j) / close_j * 100
                    if best_perf is None or perf < best_perf:
                        best_perf = perf
                    if cand < close_j:
                        success = True
                else:
                    cand = max(fh, fc)
                    perf = (cand - close_j) / close_j * 100
                    if best_perf is None or perf > best_perf:
                        best_perf = perf
                    if cand > close_j:
                        success = True
            rows.append({
                "date": sig["date"],
                "var_j": float(sig["var_j"]),
                "close": close_j,
                "vol_ratio": sig.get("vol_ratio"),
                "body_ratio": sig.get("body_ratio"),
                "best_move": round(best_perf, 2) if best_perf is not None else None,
                "success": success,
            })

        n_total = len(rows)
        n_success = sum(1 for r in rows if r["success"])
        n_fail = n_total - n_success
        taux = round(n_success / n_total * 100, 1) if n_total > 0 else 0

        return {
            "type": "TICKER_ANALYSIS", "ok": True,
            "ticker": ticker.upper(),
            "sub": "engulfing_analysis",
            "pattern": pat,
            "seuil": 0,
            "n_total": n_total,
            "n_success": n_success,
            "n_fail": n_fail,
            "taux": taux,
            "rows": rows,
            "earn_count": len(earn_dates),
            "conclusion": (
                f"{ticker.upper()} — {pat} engulfing : {n_total} signaux"
                f"{' (earnings ±5j exclus)' if earn_dates else ''}. "
                f"Win rate J+5 : {taux}% ({n_success}/{n_total})."
            )
        }

    # Handler count sans seuil
    cm = _COUNT_RE.search(query)
    if cm and not _THRESHOLD_RE.search(query):
        verb = cm.group(1).lower()
        is_neg = verb in ("baiss", "perd", "chut")
        count = int((df_period["var_pct"] < 0).sum()) if is_neg else int((df_period["var_pct"] > 0).sum())
        label = "jours négatifs" if is_neg else "jours positifs"
        total = len(df_period)
        pct = round(count / total * 100, 1) if total else 0
        return {
            "type": "TICKER_ANALYSIS", "ok": True, "ticker": ticker.upper(),
            "sub_type": "count",
            "metrics": {"n": count, "total": total, "pct": pct, "label": label,
                        "ticker": ticker.upper(), "period": _describe_period(query, df_period)},
            "next_day": {}, "distribution": [], "dates": [], "patterns": [],
            "conclusion": f"{ticker.upper()} : {count} {label} sur {total} séances ({pct}%).",
            "n": count,
        }

    # Détecter le seuil de filtrage
    fm = _FILTER_RE.search(query)
    fm_up = _FILTER_UP_RE.search(query)
    fm_down = _FILTER_DOWN_RE.search(query)
    thresholds = sorted(set(
        float(m.group(1).replace(",", ".")) for m in _THRESHOLD_RE.finditer(query)
    ))
    is_drop = True
    thr = 0
    if fm:
        verb = fm.group(1).lower()
        is_drop = verb in ("perdu", "perd", "chute", "chuté", "baisse", "baissé")
        thr = thresholds[0] if thresholds else 0
    elif fm_up:
        is_drop = False
        thr = float(fm_up.group(1).replace(",", "."))
    elif fm_down:
        is_drop = True
        thr = float(fm_down.group(1).replace(",", "."))

    has_next = bool(_NEXT_RE.search(query))

    if thr > 0:
        if is_drop:
            filtered = df_period[df_period["var_pct"] <= -thr].copy()
        else:
            filtered = df_period[df_period["var_pct"] >= thr].copy()
    elif _FILTER_ALL_RE.search(query) and thresholds:
        thr = thresholds[0]
        filtered = df_period[df_period["var_pct"].abs() >= thr].copy()
    else:
        filtered = df_period.copy()

    n = len(filtered)
    metrics = {
        "n": n, "ticker": ticker.upper(), "threshold": thr,
        "is_drop": is_drop, "period": _describe_period(query, df_period),
    }
    if n > 0:
        metrics["mean_var"] = round(float(filtered["var_pct"].mean()), 2)
        metrics["median_var"] = round(float(filtered["var_pct"].median()), 2)
        metrics["best"] = {"date": filtered["var_pct"].idxmax().strftime("%Y-%m-%d"),
                           "val": round(float(filtered["var_pct"].max()), 2)}
        metrics["worst"] = {"date": filtered["var_pct"].idxmin().strftime("%Y-%m-%d"),
                            "val": round(float(filtered["var_pct"].min()), 2)}

    next_day = {}
    if n > 0:
        filt_next = filtered.dropna(subset=["next_var"])
        if len(filt_next) > 0:
            next_day["n"] = len(filt_next)
            next_day["pct_positive"] = round(float((filt_next["next_var"] > 0).sum() / len(filt_next) * 100), 1)
            next_day["pct_negative"] = round(100 - next_day["pct_positive"], 1)
            next_day["mean_next"] = round(float(filt_next["next_var"].mean()), 2)
            next_day["median_next"] = round(float(filt_next["next_var"].median()), 2)

    distribution = _compute_distribution(filtered) if n > 0 else []

    dates_detail = []
    if n > 0:
        show_next = has_next or thr > 0
        for idx, row in filtered.iterrows():
            entry = {"date": idx.strftime("%Y-%m-%d"), "var": round(float(row["var_pct"]), 2)}
            if show_next and not pd.isna(row.get("next_var", float("nan"))):
                entry["next_var"] = round(float(row["next_var"]), 2)
            dates_detail.append(entry)

    # Patterns — teste sur TOUTES les données du ticker, pas juste filtered
    patterns = []
    if len(df_clean) >= 30:
        patterns = pattern_engine(df_clean, ticker)

    conclusion = _build_conclusion(ticker, thr, is_drop, metrics, next_day, patterns)

    return {
        "type": "TICKER_ANALYSIS", "ok": True, "ticker": ticker.upper(),
        "metrics": metrics, "next_day": next_day, "distribution": distribution,
        "dates": dates_detail, "patterns": patterns, "conclusion": conclusion, "n": n,
    }


# ─── Handlers STATS ──────────────────────────────────────────────────────


def _handle_stats_weekday(ticker: str, df: pd.DataFrame, query: str) -> dict:
    rows = []
    for dow in range(5):
        sub = df[df["dow"] == dow]
        if sub.empty:
            continue
        n = len(sub)
        rows.append({
            "jour": _DOW_LABELS[dow], "var_moy": round(float(sub["var_pct"].mean()), 3),
            "var_med": round(float(sub["var_pct"].median()), 3), "nb": n,
            "pct_positif": round(float((sub["var_pct"] > 0).sum() / n * 100), 1),
        })
    rows.sort(key=lambda r: r["var_moy"], reverse=True)
    best, worst = (rows[0], rows[-1]) if rows else (None, None)
    conclusion = ""
    if best and worst:
        conclusion = (f"Meilleur jour : {best['jour']} ({best['var_moy']:+.3f}%, {best['pct_positif']}% positif). "
                      f"Pire jour : {worst['jour']} ({worst['var_moy']:+.3f}%, {worst['pct_positif']}% positif).")
    return {
        "type": "TICKER_ANALYSIS", "ok": True, "ticker": ticker.upper(),
        "sub_type": "weekday",
        "metrics": {"n": len(df), "ticker": ticker.upper(), "period": _describe_period(query, df)},
        "weekday_stats": rows,
        "next_day": {}, "distribution": [], "dates": [], "patterns": [],
        "conclusion": conclusion, "n": len(df),
    }


def _handle_stats_month(ticker: str, df: pd.DataFrame, query: str) -> dict:
    rows = []
    for m in range(1, 13):
        sub = df[df["month"] == m]
        if sub.empty:
            continue
        n = len(sub)
        rows.append({
            "mois": _MONTH_LABELS.get(m, str(m)), "var_moy": round(float(sub["var_pct"].mean()), 3),
            "var_med": round(float(sub["var_pct"].median()), 3), "nb": n,
            "pct_positif": round(float((sub["var_pct"] > 0).sum() / n * 100), 1),
        })
    rows.sort(key=lambda r: r["var_moy"], reverse=True)
    best, worst = (rows[0], rows[-1]) if rows else (None, None)
    conclusion = ""
    if best and worst:
        conclusion = (f"Meilleur mois : {best['mois']} ({best['var_moy']:+.3f}%, {best['pct_positif']}% positif). "
                      f"Pire mois : {worst['mois']} ({worst['var_moy']:+.3f}%, {worst['pct_positif']}% positif).")
    return {
        "type": "TICKER_ANALYSIS", "ok": True, "ticker": ticker.upper(),
        "sub_type": "monthly",
        "metrics": {"n": len(df), "ticker": ticker.upper(), "period": _describe_period(query, df)},
        "monthly_stats": rows,
        "next_day": {}, "distribution": [], "dates": [], "patterns": [],
        "conclusion": conclusion, "n": len(df),
    }


def _describe_period(query: str, df: pd.DataFrame) -> str:
    m = _PERIOD_YEAR_RE.search(query)
    if m:
        return m.group(1)
    m = _PERIOD_Q_RE.search(query)
    if m:
        return f"Q{m.group(1)} {m.group(2) or ''}"
    if not df.empty:
        return f"{df.index.min().strftime('%Y-%m-%d')} → {df.index.max().strftime('%Y-%m-%d')}"
    return "toute la période"


# ─── Distribution par palier ─────────────────────────────────────────────


def _compute_distribution(filtered: pd.DataFrame) -> list[dict]:
    if filtered.empty:
        return []
    var = filtered["var_pct"]
    lo, hi = math.floor(var.min()), math.ceil(var.max())
    bins = list(range(lo, hi + 2))
    if len(bins) < 2:
        return []
    labels = [f"{bins[i]}%-{bins[i+1]}%" for i in range(len(bins) - 1)]
    cuts = pd.cut(var, bins=bins, labels=labels, right=False)
    counts = cuts.value_counts().sort_index()
    result = []
    for label, count in counts.items():
        if count == 0:
            continue
        subset = filtered[cuts == label]
        dom_month = int(subset["month"].mode().iloc[0]) if len(subset) > 0 else 0
        dom_dow = int(subset["dow"].mode().iloc[0]) if len(subset) > 0 else 0
        result.append({
            "palier": label, "count": int(count),
            "mois_dominant": _MONTH_LABELS.get(dom_month, str(dom_month)),
            "jour_dominant": _DOW_LABELS.get(dom_dow, str(dom_dow)),
        })
    return result


# ─── Pattern Engine v2 ───────────────────────────────────────────────────
# Teste sur TOUTES les données du ticker. Seuils : affichage ≥80%, actionnable ≥95%.
# Amplitude médiane ≥2%. Validation OOS 70/30 (min 20 IS, 6 OOS).


def pattern_engine(df: pd.DataFrame, ticker: str) -> list[dict]:
    """Teste toutes les conditions candidates sur le df complet du ticker."""
    candidates, earnings_excl = _build_candidates(df, ticker)
    n_total = len(df)
    split_idx = int(n_total * 0.7)
    df_is = df.iloc[:split_idx]
    df_oos = df.iloc[split_idx:]

    patterns = []
    for label, mask_fn in candidates:
        excl = earnings_excl if "engulfing" in label.lower() else set()
        _test_candidate(df_is, df_oos, label, mask_fn, patterns, excl)

    patterns.sort(key=lambda p: p["taux"], reverse=True)
    return patterns[:30]


def _build_candidates(df: pd.DataFrame, ticker: str) -> list[tuple]:
    """Construit la liste de (label, mask_function) à tester."""
    cands = []

    # A) Weekday
    for dow in range(5):
        cands.append((f"les {_DOW_LABELS[dow].lower()}s", lambda d, dw=dow: d["dow"] == dw))

    # B) Month
    for m in range(1, 13):
        cands.append((f"en {_MONTH_LABELS[m].lower()}", lambda d, mm=m: d["month"] == mm))

    # C) Quarter
    for q in range(1, 5):
        cands.append((f"au Q{q}", lambda d, qq=q: d["quarter"] == qq))

    # D) Position dans le mois
    cands.append(("début de mois (1-10)", lambda d: d["day_of_month"] <= 10))
    cands.append(("milieu de mois (11-20)", lambda d: (d["day_of_month"] > 10) & (d["day_of_month"] <= 20)))
    cands.append(("fin de mois (21+)", lambda d: d["day_of_month"] > 20))

    # E) Séquences consécutives (veille)
    for n in (2, 3, 4, 5):
        cands.append((f"après {n}j hausse consécutive", lambda d, nn=n: d["consec_up"].shift(1) >= nn))
        cands.append((f"après {n}j baisse consécutive", lambda d, nn=n: d["consec_dn"].shift(1) >= nn))

    # F) Après variation veille > X%
    for x in (3, 5, 7, 10):
        cands.append((f"après baisse ≥{x}% veille", lambda d, xx=x: d["var_pct"].shift(1) <= -xx))
        cands.append((f"après hausse ≥{x}% veille", lambda d, xx=x: d["var_pct"].shift(1) >= xx))

    # G) Après plus forte baisse/hausse 20j
    cands.append(("après plus forte baisse 20j",
                   lambda d: d["var_pct"].shift(1) == d["min_var_20"].shift(1)))
    cands.append(("après plus forte hausse 20j",
                   lambda d: d["var_pct"].shift(1) == d["max_var_20"].shift(1)))

    # H) Volume
    if "vol_ratio" in df.columns:
        cands.append(("volume > 1.5x moy 20j", lambda d: d["vol_ratio"] > 1.5))
        cands.append(("volume > 2x moy 20j", lambda d: d["vol_ratio"] > 2.0))
        cands.append(("volume < 0.5x moy 20j", lambda d: d["vol_ratio"] < 0.5))
        cands.append(("volume croissant 3j", lambda d: d["vol_up3"] >= 3))

    # I) Gaps overnight
    if "gap_pct" in df.columns:
        cands.append(("gap open > +2%", lambda d: d["gap_pct"] > 2))
        cands.append(("gap open < -2%", lambda d: d["gap_pct"] < -2))
        cands.append(("gap open > +5%", lambda d: d["gap_pct"] > 5))
        cands.append(("gap open < -5%", lambda d: d["gap_pct"] < -5))

    # J) Compression volatilité
    if "atr5" in df.columns and "atr20" in df.columns:
        cands.append(("ATR(5) < 0.5 × ATR(20)", lambda d: d["atr5"] < d["atr20"] * 0.5))
        cands.append(("range J < 50% range moy 20j", lambda d: d["range_pct"] < d["range_ma20"] * 0.5))

    # K) Position relative du cours
    if "ma20" in df.columns:
        cands.append(("cours < MA20", lambda d: d["close"] < d["ma20"]))
        cands.append(("cours > MA20", lambda d: d["close"] > d["ma20"]))
    if "low_52w" in df.columns:
        cands.append(("cours < 5% du plus bas 52s",
                       lambda d: (d["close"] - d["low_52w"]) / d["low_52w"] * 100 < 5))
        cands.append(("cours < 5% du plus haut 52s",
                       lambda d: (d["high_52w"] - d["close"]) / d["high_52w"] * 100 < 5))

    # L) Earnings (si fichier disponible)
    earn = _load_earnings(ticker)
    if earn is not None and len(earn) > 0:
        earn_dates = set(earn["date"].dt.normalize())
        # Pre-earnings J-5 à J-1
        pre_dates = set()
        for ed in earn_dates:
            for delta in range(1, 6):
                pre_dates.add(ed - pd.Timedelta(days=delta))
        cands.append(("J-5 à J-1 avant earnings", lambda d, pd_=pre_dates: d.index.normalize().isin(pd_)))
        # Post-earnings J+1 à J+3
        post_dates = set()
        for ed in earn_dates:
            for delta in range(1, 4):
                post_dates.add(ed + pd.Timedelta(days=delta))
        cands.append(("J+1 à J+3 après earnings", lambda d, pd_=post_dates: d.index.normalize().isin(pd_)))
        # Réaction earnings gap > 10%
        earn_next = set()
        for ed in earn_dates:
            earn_next.add(ed + pd.Timedelta(days=1))
            earn_next.add(ed)  # earnings day itself
        cands.append(("jour earnings gap > +10%",
                       lambda d, en=earn_next: d.index.normalize().isin(en) & (d.get("gap_pct", 0) > 10)))
        cands.append(("jour earnings gap < -10%",
                       lambda d, en=earn_next: d.index.normalize().isin(en) & (d.get("gap_pct", 0) < -10)))

    # M) Combinaisons (2 conditions, cap 50)
    combo_pairs = []
    if "vol_ratio" in df.columns:
        combo_pairs.append(("vol>1.5x + baisse>5%",
                            lambda d: (d["vol_ratio"] > 1.5) & (d["var_pct"] < -5)))
        combo_pairs.append(("vol<0.5x + hausse>3%",
                            lambda d: (d["vol_ratio"] < 0.5) & (d["var_pct"] > 3)))
        combo_pairs.append(("vol>2x + hausse>5%",
                            lambda d: (d["vol_ratio"] > 2.0) & (d["var_pct"] > 5)))
    if "gap_pct" in df.columns:
        combo_pairs.append(("gap<-2% + lundi",
                            lambda d: (d["gap_pct"] < -2) & (d["dow"] == 0)))
        combo_pairs.append(("gap>+2% + vendredi",
                            lambda d: (d["gap_pct"] > 2) & (d["dow"] == 4)))
    combo_pairs.append(("après 3j baisse + vol croissant",
                         lambda d: (d["consec_dn"].shift(1) >= 3) & (d.get("vol_up3", 0) >= 3)))
    cands.extend(combo_pairs)

    # N) Bougies japonaises — avec validation close OU low ≤ -2%
    if "bearish_engulfing" in df.columns:
        # Bearish engulfing brut
        cands.append(("bearish engulfing", lambda d: d["bearish_engulfing"] == True))
        # Bearish validé : au moins 1 close ou low dans J+1..J+5 ≤ close*0.98
        def _be_validated(d):
            be = d["bearish_engulfing"]
            thr = d["close"] * 0.98
            close_ok = False
            low_ok = False
            for i in range(1, 6):
                cc = f"close_j{i}"
                lc = f"low_j{i}"
                if cc in d.columns:
                    close_ok = close_ok | (d[cc] <= thr)
                if lc in d.columns:
                    low_ok = low_ok | (d[lc] <= thr)
            return be & (close_ok | low_ok)
        cands.append(("bearish engulfing validé (close/low ≤-2%)", _be_validated))

        # Bullish engulfing brut
        cands.append(("bullish engulfing", lambda d: d["bullish_engulfing"] == True))
        # Bullish validé
        def _bull_validated(d):
            bu = d["bullish_engulfing"]
            thr = d["close"] * 1.02
            close_ok = False
            for i in range(1, 6):
                cc = f"close_j{i}"
                if cc in d.columns:
                    close_ok = close_ok | (d[cc] >= thr)
            return bu & close_ok
        cands.append(("bullish engulfing validé (close ≥+2%)", _bull_validated))

        # Combos
        if "vol_ratio" in df.columns:
            cands.append(("bearish engulfing + vol>1.5x",
                          lambda d: (d["bearish_engulfing"]) & (d["vol_ratio"] > 1.5)))
            cands.append(("bullish engulfing + vol>1.5x",
                          lambda d: (d["bullish_engulfing"]) & (d["vol_ratio"] > 1.5)))
        cands.append(("bearish engulfing + après 3j hausse",
                       lambda d: (d["bearish_engulfing"]) & (d["consec_up"].shift(1) >= 3)))

    # Exclusion earnings ±5j pour les labels engulfing
    _earnings_exclusion = set()
    earn = _load_earnings(ticker)
    if earn is not None and len(earn) > 0:
        for ed in earn["date"].dt.normalize():
            for delta in range(-5, 6):
                _earnings_exclusion.add(ed + pd.Timedelta(days=delta))

    return cands, _earnings_exclusion


def _test_candidate(df_is: pd.DataFrame, df_oos: pd.DataFrame,
                    label: str, mask_fn, patterns: list,
                    earnings_excl: set | None = None):
    """Teste une condition sur IS, valide sur OOS."""
    try:
        mask_is = mask_fn(df_is)
    except Exception:
        return
    if not isinstance(mask_is, pd.Series):
        return
    sub_is = df_is[mask_is.fillna(False)].dropna(subset=["var_j1"])
    # Exclure ±5j autour des earnings pour les patterns engulfing
    if earnings_excl:
        sub_is = sub_is[~sub_is.index.normalize().isin(earnings_excl)]
    if len(sub_is) < 20:
        return

    for h in range(1, 6):
        col = f"var_j{h}"
        if col not in sub_is.columns:
            continue
        vals = sub_is[col].dropna()
        if len(vals) < 20:
            continue
        pct_pos = float((vals > 0).sum() / len(vals) * 100)
        pct_neg = float((vals < 0).sum() / len(vals) * 100)
        median_amp = float(vals.median())

        direction = None
        taux = 0
        if pct_pos >= 65 and median_amp >= 2:
            direction, taux = "hausse", pct_pos
        elif pct_neg >= 65 and abs(median_amp) >= 2:
            direction, taux = "baisse", pct_neg

        if direction is None:
            continue

        # OOS validation
        try:
            mask_oos = mask_fn(df_oos)
        except Exception:
            continue
        sub_oos = df_oos[mask_oos.fillna(False)].dropna(subset=[col]) if isinstance(mask_oos, pd.Series) else pd.DataFrame()
        if earnings_excl:
            sub_oos = sub_oos[~sub_oos.index.normalize().isin(earnings_excl)]
        n_oos = len(sub_oos)
        oos_pct = 0.0
        if n_oos >= 6:
            vals_oos = sub_oos[col].dropna()
            if direction == "hausse":
                oos_pct = float((vals_oos > 0).sum() / len(vals_oos) * 100) if len(vals_oos) > 0 else 0
            else:
                oos_pct = float((vals_oos < 0).sum() / len(vals_oos) * 100) if len(vals_oos) > 0 else 0
            # OOS flagged but not rejected — user sees all patterns
        elif n_oos > 0:
            oos_pct = -1  # trop peu, on garde mais flag

        oos_valid = n_oos >= 6 and oos_pct >= 50
        print(f"[pattern] {label:45s} J+{h} {direction:6s} IS: n={len(vals):3d} taux={taux:.1f}% "
              f"med={median_amp:+.2f}% | OOS: n={n_oos} pct={oos_pct:.1f}% {'OK' if oos_valid else 'FAIL'}", flush=True)

        patterns.append({
            "label": label, "horizon": f"J+{h}", "direction": direction,
            "taux": round(taux, 1), "n": len(vals),
            "median_amp": round(median_amp, 2),
            "mean_amp": round(float(vals.mean()), 2),
            "actionnable": taux >= 95 and oos_valid,
            "oos_valid": oos_valid,
            "n_oos": n_oos, "oos_pct": round(oos_pct, 1),
        })


# ─── Conclusion actionnable ──────────────────────────────────────────────


def _build_conclusion(ticker: str, thr: float, is_drop: bool,
                      metrics: dict, next_day: dict,
                      patterns: list[dict]) -> str:
    n = metrics["n"]
    if n == 0:
        return f"Aucune occurrence de {ticker.upper()} avec ce critère."
    parts = []
    direction = "perdu" if is_drop else "gagné"
    period = metrics.get("period", "")
    parts.append(f"{ticker.upper()} a {direction} ≥ {thr}% sur {n} séances ({period}).")
    if next_day:
        pct_pos = next_day.get("pct_positive", 0)
        mean_next = next_day.get("mean_next", 0)
        sign = "+" if mean_next >= 0 else ""
        if pct_pos >= 60:
            parts.append(f"Le lendemain est positif dans {pct_pos}% des cas (moy J+1 : {sign}{mean_next}%).")
            if pct_pos >= 70:
                parts.append(f"Signal : acheter call à la clôture de J ({sign}{mean_next}% dans {pct_pos}% des cas).")
        elif pct_pos <= 40:
            parts.append(f"Le lendemain est négatif dans {next_day.get('pct_negative', 0)}% des cas (moy J+1 : {sign}{mean_next}%).")
    if patterns:
        best = patterns[0]
        tag = "ACTIONNABLE" if best.get("actionnable") else "observé"
        parts.append(f"Pattern [{tag}] : {best['label']} → {best['direction']} sur {best['horizon']} "
                     f"dans {best['taux']}% des cas (n={best['n']}, amp. méd. {best['median_amp']:+.2f}%).")
    return " ".join(parts)
