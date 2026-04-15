# query_executor.py — Exécution pandas des questions classifiées

import re
from pathlib import Path

import pandas as pd
import numpy as np

DATA_DIR = Path(__file__).resolve().parent / "data" / "live_selected"
_CSV_CACHE: dict[str, pd.DataFrame] = {}

_FUNDAMENTAL_KW = {
    "vix", "skew", "vvix", "dxy", "spx", "spy", "qqq", "iwm",
    "gold", "oil", "silver", "copper",
    "dax", "ftse", "nikkei", "cac", "eurostoxx",
    "yield", "treasury", "bond", "tbill", "oanda",
    "put", "call", "ratio", "pcr", "equity", "index", "curve",
    "vix3m", "vix6m", "vix9d", "vix1d",
    "tick", "trin", "advance", "decline",
    "future", "correlation", "average", "range", "spread",
}


def _is_ticker_csv(stem: str) -> bool:
    """True si le CSV est un ticker individuel de société (AAOI, AAPL...)."""
    name = stem.lower()
    base = re.sub(r"_(daily|30min|5min|1min|1hour|4hour|weekly|monthly).*$", "", name)
    base = re.sub(r"[_\s,]+", "", base)
    for kw in _FUNDAMENTAL_KW:
        if kw in base:
            return False
    return len(base) <= 5 and bool(re.match(r"^[a-z]{1,5}$", base))


def _get_market_csvs(exclude_ticker: str = "") -> dict[str, str]:
    """Auto-detect all market CSVs (not tickers, not calendar, not intraday)."""
    result = {}
    excl_t = exclude_ticker.lower()
    _EXCLUDE = {"calendar", "option", "chain", "earning"}
    _INTRADAY = {"1min", "5min", "30min", "1hour", "4hour"}
    for f in sorted(DATA_DIR.glob("*.csv")):
        stem = f.stem
        name = stem.lower()
        if any(p in name for p in _EXCLUDE):
            continue
        if any(p in name for p in _INTRADAY):
            continue
        if _is_ticker_csv(stem):
            continue
        if excl_t and excl_t in name:
            continue
        col = "var_pct" if any(x in name for x in ("dxy", "gold", "spx", "spy", "qqq", "iwm", "oil", "dax", "ftse", "nikkei")) else "close"
        result[stem] = col
    return result


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
_DATE_TEXT_RE = re.compile(
    r"\b(?:le\s+)?(\d{1,2})\s+"
    r"(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre)"
    r"\s+(\d{4})\b", re.IGNORECASE)
_DATE_NUM_RE = re.compile(r"\b(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4}|\d{4}[/\-]\d{2}[/\-]\d{2})\b")


def _to_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(
        s.astype(str).str.replace(",", ".").str.replace(r"\s+", "", regex=True).str.strip(),
        errors="coerce")


def _load_daily(ticker: str) -> pd.DataFrame | None:
    tickers_dir = DATA_DIR / "tickers"
    candidates = [
        DATA_DIR / f"{ticker.upper()}_daily.csv",
        DATA_DIR / f"{ticker.upper()}.csv",
        tickers_dir / f"{ticker.upper()}_daily.csv",
        tickers_dir / f"{ticker.upper()}.csv",
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


def _load_earnings(ticker: str) -> set:
    tickers_dir = DATA_DIR / "tickers"
    for search_dir in [DATA_DIR, tickers_dir]:
        p = search_dir / f"{ticker.upper()}_earnings.csv"
        if p.exists():
            first_line = p.read_text().split("\n")[0]
            sep = ";" if ";" in first_line else ","
            df = pd.read_csv(p, sep=sep)
            df.columns = [c.strip().lower() for c in df.columns]
            date_col = next((c for c in df.columns if "date" in c), df.columns[0])
            df["date"] = pd.to_datetime(df[date_col].astype(str).str.strip(),
                                        format="mixed", errors="coerce")
            dates = set()
            for d in df["date"].dropna():
                for delta in range(-5, 6):
                    dates.add(d.normalize() + pd.Timedelta(days=delta))
            return dates
    return set()


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    d = df.set_index("time").sort_index().copy()
    d["prev_close"] = d["close"].shift(1)
    d["var_pct"] = (d["close"] - d["prev_close"]) / d["prev_close"] * 100
    d["next_close"] = d["close"].shift(-1)
    d["next_var"] = (d["next_close"] - d["close"]) / d["close"] * 100
    d["dow"] = d.index.dayofweek
    d["month"] = d.index.month
    d["year"] = d.index.year
    for i in range(1, 6):
        d[f"close_j{i}"] = d["close"].shift(-i)
        if "low" in d.columns:
            d[f"low_j{i}"] = d["low"].shift(-i)
    if "open" in d.columns:
        d["candle_body"] = (d["close"] - d["open"]).abs()
        prev_open = d["open"].shift(1)
        prev_body = d["candle_body"].shift(1)
        prev_green = d["close"].shift(1) > prev_open
        prev_red = d["close"].shift(1) < prev_open
        d["bearish_engulfing"] = (
            prev_green & (d["close"] < d["open"])
            & (d["open"] >= d["close"].shift(1))
            & (d["close"] <= prev_open)
            & (d["candle_body"] > prev_body))
        d["bullish_engulfing"] = (
            prev_red & (d["close"] > d["open"])
            & (d["open"] <= d["close"].shift(1))
            & (d["close"] >= prev_open)
            & (d["candle_body"] > prev_body))
    if "volume" in d.columns:
        d["vol_ma20"] = d["volume"].rolling(20).mean()
        d["vol_ratio"] = d["volume"] / d["vol_ma20"]
    return d


def _filter_period(df: pd.DataFrame, period: dict | None) -> pd.DataFrame:
    if not period:
        return df
    if "date_from" in period:
        df = df[df.index >= period["date_from"]]
    if "date_to" in period:
        df = df[df.index <= period["date_to"]]
    if "years" in period:
        return df[df["year"].isin(period["years"])]
    if "year" in period and "date_from" not in period:
        return df[df["year"] == period["year"]]
    return df


def _find_time_column(df: pd.DataFrame) -> str | None:
    for c in ("time", "date", "timestamp", "datetime", "index", "period"):
        if c in df.columns:
            return c
    for c in df.columns:
        try:
            sample = df[c].dropna().iloc[0] if len(df) > 0 else None
            if sample and pd.to_datetime(str(sample), errors="coerce") is not pd.NaT:
                return c
        except Exception:
            continue
    return None


def _load_csv_by_name(asset_name: str) -> pd.DataFrame | None:
    name = asset_name.upper()
    if name in _CSV_CACHE:
        return _CSV_CACHE[name]
    alias = {"VIX": "VIX_daily.csv", "SPX": "SPX_daily.csv", "DXY": "DXY_daily.csv",
             "GOLD": "Gold_daily.csv", "VVIX": "VVIX_daily.csv",
             "SKEW": "SKEW_INDEX_daily.csv", "VIX3M": "VIX3M_daily.csv",
             "VIX6M": "VIX6M_daily.csv"}
    candidates = [alias.get(name, ""), f"{name}_daily.csv", f"{name}.csv",
                  f"{asset_name}_daily.csv", f"{asset_name}.csv"]
    tickers_dir = DATA_DIR / "tickers"
    for c in candidates:
        if not c:
            continue
        # Search DATA_DIR first, then tickers/
        p = DATA_DIR / c
        if not p.exists() and tickers_dir.exists():
            p = tickers_dir / c
        if p.exists():
            with open(p, "r", encoding="utf-8", errors="ignore") as _f:
                _first = _f.readline()
            sep = "," if _first.count(",") > _first.count(";") else ";"
            df = pd.read_csv(p, sep=sep)
            df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
            tc = _find_time_column(df)
            if tc is None:
                continue
            df["time"] = pd.to_datetime(df[tc].astype(str).str.strip(), errors="coerce")
            df = df.dropna(subset=["time"]).set_index("time").sort_index()
            for col in ("open", "close", "high", "low"):
                if col in df.columns:
                    df[col] = _to_numeric(df[col])
            if "close" in df.columns:
                df["prev_close"] = df["close"].shift(1)
                df["var_pct"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100
            _CSV_CACHE[name] = df
            return df
    return None


def _apply_condition(df, cond_str):
    q = cond_str.lower().strip()
    # dépasse X, au-dessus de X
    if m := re.search(r"d[eé]passe\w*\s+(\d+[\.,]?\d*)", q):
        return df["close"] > float(m.group(1).replace(",", "."))
    if m := re.search(r"au[\s-]dessus\s+de\s+(\d+[\.,]?\d*)", q):
        return df["close"] > float(m.group(1).replace(",", "."))
    if m := re.search(r"en[\s-]dessous\s+de\s+(\d+[\.,]?\d*)", q):
        return df["close"] < float(m.group(1).replace(",", "."))
    # > X, < X (explicit operators)
    if m := re.search(r">\s*(\d+[\.,]?\d*)", q):
        return df["close"] > float(m.group(1).replace(",", "."))
    if m := re.search(r"<\s*(\d+[\.,]?\d*)", q):
        return df["close"] < float(m.group(1).replace(",", "."))
    # perd X%, baisse de X%, chute de X%
    if m := re.search(r"(perd|baisse|chut)\w*\s+(?:de\s+)?(\d+[\.,]?\d*)\s*%", q):
        return df["var_pct"] <= -float(m.group(2).replace(",", "."))
    if m := re.search(r"(baisse|perdu|chut)\w*\s+(?:de\s+)?(\d+[\.,]?\d*)", q):
        return df["var_pct"] <= -float(m.group(2).replace(",", "."))
    # monte de X%, hausse de X%, gagne X%
    if m := re.search(r"(monte|hausse|gagn)\w*\s+(?:de\s+)?(\d+[\.,]?\d*)\s*%", q):
        return df["var_pct"] >= float(m.group(2).replace(",", "."))
    if m := re.search(r"(hausse|gagn|mont)\w*\s+(?:de\s+)?(\d+[\.,]?\d*)", q):
        return df["var_pct"] >= float(m.group(2).replace(",", "."))
    # supérieur/inférieur à X
    if m := re.search(r"sup[eé]rieur\w*\s+[àa]\s+(\d+[\.,]?\d*)", q):
        return df["close"] > float(m.group(1).replace(",", "."))
    if m := re.search(r"inf[eé]rieur\w*\s+[àa]\s+(\d+[\.,]?\d*)", q):
        return df["close"] < float(m.group(1).replace(",", "."))
    return pd.Series(True, index=df.index)


def _parse_date(query: str) -> pd.Timestamp | None:
    m = _DATE_TEXT_RE.search(query)
    if m:
        day, ms, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        ms = ms.replace("é", "e").replace("û", "u").replace("ô", "o")
        month = _MONTH_MAP.get(ms)
        if month:
            try: return pd.Timestamp(year=year, month=month, day=day)
            except Exception: return None
    m2 = _DATE_NUM_RE.search(query)
    if m2:
        try: return pd.Timestamp(m2.group(1).replace("/", "-"))
        except Exception: return None
    return None


def _fmt_date(ts) -> str:
    return ts.strftime("%d/%m/%Y") if hasattr(ts, "strftime") else str(ts)


# ─── Main executor ───────────────────────────────────────────────────────

def execute_query(interpretation: dict, query: str) -> dict:
    cat = interpretation.get("category", "UNKNOWN")
    ticker = (interpretation.get("ticker") or "SPX").upper()
    period = interpretation.get("period")

    raw = _load_daily(ticker)
    if raw is None:
        return {"type": "INTERPRETED", "ok": False, "error": f"Pas de CSV daily pour {ticker}."}

    df = _prepare(raw)
    dc = df.dropna(subset=["prev_close", "var_pct"]).copy()
    dp = _filter_period(dc, period)

    try:
        if cat == "LOOKUP_DATE":
            return _exec_lookup_date(ticker, df, query, interpretation)
        if cat == "LOOKUP_BEST":
            return _exec_lookup_best(ticker, dp, dc, interpretation, period, query)
        if cat == "CANDLE_PATTERN":
            return _exec_candle_pattern(ticker, dp, dc, interpretation)
        if cat == "ENGULFING_MULTI_PERIOD":
            return _exec_engulfing_multi_period(ticker, dc, dp, interpretation, query)
        if cat == "ENGULFING_ANALYSIS":
            return _exec_engulfing_analysis(ticker, dc, dp, interpretation, query)
        if cat == "ENGULFING_FAILURE_ANALYSIS":
            return _exec_engulfing_failure(ticker, dc, interpretation, query)
        if cat == "EXPLAIN":
            return _exec_explain(ticker, dc, interpretation)
        if cat == "WEEKDAY_STATS":
            return _exec_weekday(ticker, dp)
        if cat == "MONTH_STATS":
            return _exec_month(ticker, dp)
        if cat == "ANNUAL_PERF":
            return _exec_annual_perf(ticker, df, period)
        if cat == "COUNT":
            return _exec_count(ticker, dp, query)
        if cat == "EXPLAIN_GENERAL":
            return _exec_explain_general(interpretation)
        if cat == "INTRADAY_ANALYSIS":
            return _exec_intraday(interpretation, query)
        if cat == "ML_PREDICT":
            interpretation["_query"] = query
            return _exec_ml(interpretation)
        if cat == "NEUTRAL_NEXT":
            return _exec_neutral_next(ticker, dp, interpretation, query)
        if cat == "STREAK_ANALYSIS":
            return _exec_streak(ticker, dp, interpretation.get("direction", "up"))
        if cat == "BIAS_ANALYSIS":
            return _exec_bias(ticker, dp, period)
        if cat == "CORRELATION_SCAN":
            return _exec_correlation_scan(ticker, dp, period)
        if cat == "CORRELATION":
            return _exec_correlation(ticker, interpretation)
        if cat == "MULTI_CONDITION":
            return _exec_multi_condition(interpretation, query)
        if cat == "MULTI_THRESHOLD":
            return _exec_multi_threshold(ticker, dp, interpretation)
        if cat == "FILTER_STATS":
            crit = interpretation.get("criterion")
            if interpretation.get("threshold") and crit in ("abs", "drop", "gain", "intraday_drop", "intraday_gain"):
                return _exec_filter_abs(ticker, dp, interpretation)
            return None
    except Exception as e:
        print(f"[executor] error in {cat}: {e}", flush=True)
        return None
    return None


# ─── Handlers ────────────────────────────────────────────────────────────

def _exec_lookup_date(ticker, df, query, interp):
    target = _parse_date(query)
    if target is None:
        return None
    field = interp.get("field", "close")
    if field not in ("open", "close", "high", "low", "var_pct", "volume"):
        field = "close"
    mask = df.index.normalize() == target.normalize()
    if not mask.any():
        return {"type": "INTERPRETED", "ok": False, "error": f"Pas de données pour {ticker} le {_fmt_date(target)}."}
    row = df[mask].iloc[0]
    if field not in row.index:
        return {"type": "INTERPRETED", "ok": False, "error": f"Colonne '{field}' absente pour {ticker}."}
    val = float(row[field])
    label_map = {"close": "Clôture", "open": "Open", "high": "Haut", "low": "Bas",
                 "var_pct": "Variation", "volume": "Volume"}
    unit = "%" if field == "var_pct" else ("" if field == "volume" else "pts")
    # Enrichment context
    ctx = {}
    if field != "var_pct" and "var_pct" in row.index and not pd.isna(row["var_pct"]):
        ctx["var_pct"] = round(float(row["var_pct"]), 2)
    if "vol_ratio" in row.index and not pd.isna(row.get("vol_ratio", np.nan)):
        ctx["volume_ratio"] = round(float(row["vol_ratio"]), 1)
    for pat in ("bearish_engulfing", "bullish_engulfing"):
        if pat in row.index and row[pat]:
            ctx["pattern"] = pat.replace("_", " ").title()
    vix_df = _load_csv_by_name("VIX")
    if vix_df is not None and "close" in vix_df.columns:
        tgt = target.normalize()
        if tgt in vix_df.index:
            ctx["vix"] = round(float(vix_df.loc[tgt, "close"]), 1)
    if ctx:
        return {"type": "INTERPRETED", "ok": True, "sub_type": "single_value_enriched",
                "label": f"{ticker} — {label_map.get(field, field)} du {_fmt_date(target)}",
                "value": val, "unit": unit, "context": ctx}
    return {"type": "INTERPRETED", "ok": True, "sub_type": "single_value",
            "label": f"{ticker} — {label_map.get(field, field)} du {_fmt_date(target)}",
            "value": val, "unit": unit}


def _exec_lookup_best(ticker, dp, dc, interp, period, query):
    if dp.empty:
        return {"type": "INTERPRETED", "ok": False, "error": "Aucune donnée pour cette période."}
    direction = interp.get("direction", "up")
    years = (period.get("years") if period and "years" in period
             else [period["year"]] if period and "year" in period
             else [None])
    results = []
    for year in years:
        sub = dp[dp["year"] == year] if year else dp
        if sub.empty:
            continue
        idx = sub["var_pct"].idxmin() if direction == "down" else sub["var_pct"].idxmax()
        val = float(sub.loc[idx, "var_pct"])
        close = float(sub.loc[idx, "close"])
        results.append({"year": year, "date": _fmt_date(idx), "var": round(val, 2), "close": round(close, 2)})
    if not results:
        return {"type": "INTERPRETED", "ok": False, "error": "Aucune donnée."}
    label = "pire jour" if direction == "down" else "meilleur jour"
    if len(results) == 1:
        r = results[0]
        ps = str(r["year"]) if r["year"] else "toute la période"
        return {"type": "INTERPRETED", "ok": True, "sub_type": "best_single",
                "ticker": ticker, "label": f"{'Pire' if direction == 'down' else 'Meilleur'} jour {ticker} ({ps})",
                "date": r["date"], "var": r["var"], "close": r["close"]}
    return {"type": "INTERPRETED", "ok": True, "sub_type": "best_multi",
            "ticker": ticker, "label": label, "direction": direction, "results": results}


def _exec_candle_pattern(ticker, dp, dc, interp):
    pattern = interp.get("pattern", "bearish_engulfing")
    criterion = interp.get("criterion", "all")
    data = dp if len(dp) < len(dc) else dc
    if pattern not in data.columns:
        return {"type": "INTERPRETED", "ok": False, "error": f"Pattern '{pattern}' non calculé."}
    matches = data[data[pattern] == True].copy()
    if matches.empty:
        return {"type": "INTERPRETED", "ok": False, "error": f"Aucun {pattern.replace('_', ' ')} pour {ticker}."}
    if criterion == "last":
        last = matches.iloc[-1]
        nv = round(float(last["next_var"]), 2) if not pd.isna(last.get("next_var", np.nan)) else None
        return {"type": "INTERPRETED", "ok": True, "sub_type": "pattern_last",
                "ticker": ticker, "pattern": pattern.replace("_", " "),
                "date": _fmt_date(last.name), "close": round(float(last["close"]), 2),
                "var": round(float(last["var_pct"]), 2), "next_var": nv, "n_total": len(matches)}
    if criterion == "count":
        return {"type": "INTERPRETED", "ok": True, "sub_type": "count",
                "ticker": ticker, "count": len(matches), "total": len(data),
                "pct": round(len(matches) / len(data) * 100, 1),
                "label": pattern.replace("_", " ")}
    rows = [{"date": _fmt_date(i), "var": round(float(r["var_pct"]), 2), "close": round(float(r["close"]), 2),
             **({"next_var": round(float(r["next_var"]), 2)} if not pd.isna(r.get("next_var", np.nan)) else {})}
            for i, r in matches.iterrows()]
    wn = [r for r in rows if "next_var" in r]
    pct_neg = round(sum(1 for r in wn if r["next_var"] < 0) / len(wn) * 100, 1) if wn else 0
    return {"type": "INTERPRETED", "ok": True, "sub_type": "pattern_all",
            "ticker": ticker, "pattern": pattern.replace("_", " "),
            "n": len(rows), "pct_neg_next": pct_neg, "dates": rows}


def _exec_engulfing_multi_period(ticker, dc, dp, interp, query):
    """BE analysis for multiple years side by side."""
    years = interp.get("years", [])
    pattern = interp.get("pattern", "bearish_engulfing")
    period_results = []
    for year in years:
        sub_interp = {**interp, "category": "ENGULFING_ANALYSIS",
                      "period": {"year": year}, "criterion": None}
        r = _exec_engulfing_analysis(ticker, dc, dp, sub_interp, query)
        if r and r.get("ok"):
            r["year"] = year
            period_results.append(r)
    return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_multi_period",
            "ticker": ticker, "pattern": pattern.replace("_", " "),
            "years": years, "period_results": period_results}


def _exec_engulfing_analysis(ticker, dc, dp, interp, query):
    """Analyse complète bearish/bullish engulfing."""
    q = query.lower()
    pattern = interp.get("pattern", "bearish_engulfing")
    is_bearish = "bearish" in pattern

    matches = dc[dc.get(pattern, False) == True].copy()
    earnings_excl = _load_earnings(ticker)
    if earnings_excl:
        matches = matches[~matches.index.normalize().isin(earnings_excl)]

    # Filter period if specified
    period = interp.get("period")
    if period:
        matches = _filter_period(matches, period)

    if matches.empty:
        return {"type": "INTERPRETED", "ok": False, "error": f"Aucun {pattern.replace('_', ' ')} pour {ticker}."}

    criterion = interp.get("criterion")

    # Volume threshold sub-type
    if criterion == "volume_threshold":
        return _exec_engulfing_vol_threshold(ticker, matches, dc, pattern, q)

    # Average performance sub-type
    if criterion == "avg_performance":
        return _exec_engulfing_avg_perf(ticker, matches, pattern)

    # Duration sub-type
    if criterion == "duration":
        return _exec_engulfing_duration(ticker, matches, pattern)

    # Extract N if "N derniers"
    m_n = re.search(r"(\d+)\s*derniers?|derniers?\s*(\d+)", q)
    n_last = int(m_n.group(1) or m_n.group(2)) if m_n else None

    # Extract custom threshold
    m_seuil = re.search(r"(?:limite|seuil).*?(\d+[\.,]?\d*)\s*%", q)
    if m_seuil:
        seuil = float(m_seuil.group(1).replace(",", "."))
    elif "bearish" in pattern:
        seuil = interp.get("be_seuil", 2.0)
    else:
        seuil = interp.get("bull_seuil", 2.0)

    # Compute success for each occurrence
    rows = []
    for idx, row in matches.iterrows():
        close_j = float(row["close"])
        threshold = close_j * (1 - seuil / 100) if is_bearish else close_j * (1 + seuil / 100)
        success = False
        best_move = 0.0
        for i in range(1, 6):
            cc = row.get(f"close_j{i}", np.nan)
            lc = row.get(f"low_j{i}", np.nan) if is_bearish else row.get(f"close_j{i}", np.nan)
            if is_bearish:
                if not pd.isna(cc) and cc < threshold:
                    success = True
                if not pd.isna(lc) and lc < threshold:
                    success = True
                if not pd.isna(lc):
                    best_move = max(best_move, (close_j - lc) / close_j * 100)
                if not pd.isna(cc):
                    best_move = max(best_move, (close_j - cc) / close_j * 100)
            else:
                if not pd.isna(cc) and cc > threshold:
                    success = True
                if not pd.isna(cc):
                    best_move = max(best_move, (cc - close_j) / close_j * 100)
        rows.append({
            "date": _fmt_date(idx), "var_j": round(float(row["var_pct"]), 2),
            "close": round(close_j, 2), "success": success,
            "best_move": round(best_move, 2),
        })

    # Apply N derniers
    if n_last:
        rows = rows[-n_last:]

    n_total = len(rows)
    n_success = sum(1 for r in rows if r["success"])
    n_fail = n_total - n_success
    taux = round(n_success / n_total * 100, 1) if n_total else 0

    # Sub-type detection
    # "quel % de baisse pour Y% de réussite"
    m_target = re.search(r"(\d+)\s*%\s*(?:de\s+)?(?:r[eé]ussite|succ[eè]s|taux)", q)
    if m_target and re.search(r"quel.*%.*(?:baisse|seuil)", q):
        target_rate = int(m_target.group(1))
        # Test thresholds 0.5% to 15%
        threshold_table = []
        for t in [x / 2 for x in range(1, 31)]:
            n_ok = 0
            for idx2, row2 in matches.iterrows():
                cj = float(row2["close"])
                th = cj * (1 - t / 100) if is_bearish else cj * (1 + t / 100)
                ok = False
                for i in range(1, 6):
                    cc = row2.get(f"close_j{i}", np.nan)
                    lc = row2.get(f"low_j{i}", np.nan) if is_bearish else cc
                    if is_bearish:
                        if (not pd.isna(cc) and cc <= th) or (not pd.isna(lc) and lc <= th):
                            ok = True; break
                    else:
                        if not pd.isna(cc) and cc >= th:
                            ok = True; break
                if ok:
                    n_ok += 1
            rate = round(n_ok / len(matches) * 100, 1) if len(matches) else 0
            threshold_table.append({"seuil": f"{t}%", "taux": rate, "n": n_ok})
            if rate >= target_rate and not any(x.get("_target_met") for x in threshold_table):
                threshold_table[-1]["_target_met"] = True
        return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_thresholds",
                "ticker": ticker, "pattern": pattern.replace("_", " "),
                "target_rate": target_rate, "table": threshold_table, "n_total": len(matches)}

    # VIX analysis
    if re.search(r"\bvix\b", q):
        vix_raw = _load_daily("VIX")
        if vix_raw is not None:
            vix_df = vix_raw.set_index("time").sort_index()
            for col in ("close",):
                if col in vix_df.columns:
                    vix_df[col] = _to_numeric(vix_df[col])
            vix_df.index = vix_df.index.normalize()
            vix_ranges = [(0, 15), (15, 20), (20, 25), (25, 30), (30, 100)]
            vix_table = []
            for lo, hi in vix_ranges:
                count_ok, count_tot = 0, 0
                for r in rows:
                    dt = pd.to_datetime(r["date"], dayfirst=True).normalize()
                    if dt in vix_df.index:
                        vix_val = float(vix_df.loc[dt, "close"]) if "close" in vix_df.columns else 0
                        if lo <= vix_val < hi:
                            count_tot += 1
                            if r["success"]:
                                count_ok += 1
                rate = round(count_ok / count_tot * 100, 1) if count_tot else 0
                vix_table.append({"vix_range": f"{lo}-{hi}", "n": count_tot, "n_success": count_ok, "taux": rate})
            return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_vix",
                    "ticker": ticker, "pattern": pattern.replace("_", " "),
                    "table": vix_table, "n_total": n_total}

    # "par année" / "chaque année" / multiple years
    by_year = bool(re.search(r"\bchaque\s+ann[eé]e\b|\bpar\s+ann[eé]e\b|\bann[eé]e\s+par\s+ann[eé]e\b", q))
    years_list = period.get("years") if period and "years" in period else None
    if by_year or years_list:
        year_rows = []
        all_years = years_list or sorted(set(matches.index.year))
        for yr in all_years:
            sub_y = [r for r in rows if pd.to_datetime(r["date"], dayfirst=True).year == yr]
            ny = len(sub_y)
            ns = sum(1 for r in sub_y if r["success"])
            nf = ny - ns
            year_rows.append({"Année": yr, "Occurrences": ny, "Succès": ns,
                              "Échecs": nf, "Taux %": round(ns / ny * 100, 1) if ny else 0})
        best_y = max(year_rows, key=lambda r: r["Taux %"]) if year_rows else None
        worst_y = min(year_rows, key=lambda r: r["Taux %"]) if year_rows else None
        conc = ""
        if best_y and worst_y:
            conc = (f"Meilleure année : {best_y['Année']} ({best_y['Taux %']}% sur {best_y['Occurrences']} cas) — "
                    f"Moins bonne : {worst_y['Année']} ({worst_y['Taux %']}% sur {worst_y['Occurrences']} cas).")
        return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_by_year",
                "ticker": ticker, "pattern": pattern.replace("_", " "),
                "seuil": seuil, "year_rows": year_rows,
                "total": {"n": n_total, "n_success": n_success, "n_fail": n_fail, "taux": taux},
                "dates_detail": rows,
                "conclusion": conc, "ticker_source": interp.get("ticker_source", "explicit")}

    # Default: success/fail analysis
    return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_analysis",
            "ticker": ticker, "pattern": pattern.replace("_", " "),
            "seuil": seuil, "n_total": n_total, "n_success": n_success,
            "n_fail": n_fail, "taux": taux,
            "rows": rows, "n_last": n_last,
            "ticker_source": interp.get("ticker_source", "explicit")}


def _exec_engulfing_failure(ticker, dc, interp, query):
    """Analyse des points communs aux échecs du BE."""
    pattern = interp.get("pattern", "bearish_engulfing")
    is_bearish = "bearish" in pattern
    seuil = 2.0

    matches = dc[dc.get(pattern, False) == True].copy()
    earnings_excl = _load_earnings(ticker)
    if earnings_excl:
        matches = matches[~matches.index.normalize().isin(earnings_excl)]

    # Compute success
    for idx, row in matches.iterrows():
        cj = float(row["close"])
        th = cj * (1 - seuil / 100) if is_bearish else cj * (1 + seuil / 100)
        ok = False
        for i in range(1, 6):
            cc = row.get(f"close_j{i}", np.nan)
            lc = row.get(f"low_j{i}", np.nan) if is_bearish else cc
            if is_bearish:
                if (not pd.isna(cc) and cc <= th) or (not pd.isna(lc) and lc <= th):
                    ok = True; break
            else:
                if not pd.isna(cc) and cc >= th:
                    ok = True; break
        matches.loc[idx, "success"] = ok

    successes = matches[matches["success"] == True]
    failures = matches[matches["success"] == False]
    if failures.empty:
        return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_failure_analysis",
                "ticker": ticker, "n_failures": 0, "correlations": {},
                "conclusion": "Aucun échec trouvé — le pattern a 100% de succès."}

    corr = {}
    signals_found = []
    fail_dates = failures.index.normalize()
    success_dates = successes.index.normalize()

    # Auto-detect ALL market CSVs
    csv_to_check = _get_market_csvs(exclude_ticker=ticker)
    print(f"[failure] {len(csv_to_check)} CSV marché trouvés pour croisement", flush=True)
    for name, col in csv_to_check.items():
        try:
            df_ext = _load_csv_by_name(name)
        except Exception:
            continue
        if df_ext is None:
            continue
        if col not in df_ext.columns:
            col = "close" if "close" in df_ext.columns else None
        if col is None:
            continue
        vals_f = df_ext[col].reindex(fail_dates, method="nearest", tolerance=pd.Timedelta("2D")).dropna()
        vals_s = df_ext[col].reindex(success_dates, method="nearest", tolerance=pd.Timedelta("2D")).dropna()
        if len(vals_f) < 2 or len(vals_s) < 2:
            continue
        mf, ms = float(vals_f.mean()), float(vals_s.mean())
        corr[f"{name} (échecs)"] = round(mf, 2)
        corr[f"{name} (succès)"] = round(ms, 2)
        diff = mf - ms
        std = float(vals_s.std()) if float(vals_s.std()) > 0 else 1
        if abs(diff) > std * 0.5:
            direction = "plus élevé" if diff > 0 else "plus bas"
            signals_found.append(f"{name} {direction} lors des échecs ({mf:.1f} vs {ms:.1f})")

    # Volume ratio
    if "vol_ratio" in failures.columns:
        vr_f = failures["vol_ratio"].dropna()
        vr_s = successes["vol_ratio"].dropna()
        if len(vr_f) > 0: corr["Volume ratio (échecs)"] = round(float(vr_f.mean()), 2)
        if len(vr_s) > 0: corr["Volume ratio (succès)"] = round(float(vr_s.mean()), 2)
        if len(vr_f) > 0 and float(vr_f.mean()) < 0.8:
            signals_found.append("Volume faible lors des échecs")

    # Jour / mois dominant
    if len(failures) > 0:
        dow_map = {0: "Lundi", 1: "Mardi", 2: "Mercredi", 3: "Jeudi", 4: "Vendredi"}
        corr["Jour dominant (échecs)"] = failures["dow"].map(dow_map).value_counts().index[0]
        corr["Mois dominant (échecs)"] = failures["month"].map(_MONTH_LABELS).value_counts().index[0]

    conc = " · ".join(signals_found) if signals_found else "Pas de corrélation dominante identifiée."

    return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_failure_analysis",
            "ticker": ticker, "n_failures": len(failures),
            "fail_dates": [d.strftime("%d/%m/%Y") for d in failures.index],
            "correlations": corr, "conclusion": conc}


def _exec_explain(ticker, dc, interp):
    """Explique la configuration du bearish/bullish engulfing."""
    pattern = interp.get("pattern", "bearish_engulfing")
    is_bearish = "bearish" in pattern
    name = "Bearish Engulfing" if is_bearish else "Bullish Engulfing"

    matches = dc[dc.get(pattern, False) == True] if pattern in dc.columns else pd.DataFrame()
    n = len(matches)
    derniere = matches.index[-1].strftime("%d/%m/%Y") if n > 0 else "N/A"

    lignes = [
        f"Détection : bougie J-1 {'verte' if is_bearish else 'rouge'} + bougie J {'rouge' if is_bearish else 'verte'} qui l'englobe (body J > body J-1)",
        f"Validation : au moins 1 close OU low parmi J+1..J+5 {'≤' if is_bearish else '≥'} close_J × {'0.98' if is_bearish else '1.02'} (seuil {'−' if is_bearish else '+'}2%)",
        "Exclusion : ±5 jours autour des earnings",
        f"Résultats sur {ticker} : {n} occurrences dans l'historique",
        f"Dernière occurrence : {derniere}",
    ]
    return {"type": "INTERPRETED", "ok": True, "sub_type": "text_explanation",
            "ticker": ticker, "pattern": name, "lignes": lignes}


def _exec_engulfing_vol_threshold(ticker, matches, dc, pattern, query):
    """Volume minimum pour X% de succès."""
    m_target = re.search(r"(\d+)\s*%", query)
    target_rate = int(m_target.group(1)) if m_target else 70
    seuil = 2.0
    is_bearish = "bearish" in pattern
    # Compute success on all matches
    for idx, row in matches.iterrows():
        cj = float(row["close"])
        th = cj * (1 - seuil / 100) if is_bearish else cj * (1 + seuil / 100)
        ok = False
        for i in range(1, 6):
            cc = row.get(f"close_j{i}", np.nan)
            lc = row.get(f"low_j{i}", np.nan) if is_bearish else cc
            if is_bearish and ((not pd.isna(cc) and cc <= th) or (not pd.isna(lc) and lc <= th)):
                ok = True; break
            if not is_bearish and not pd.isna(cc) and cc >= th:
                ok = True; break
        matches.loc[idx, "success"] = ok
    if "vol_ratio" not in matches.columns or matches["vol_ratio"].isna().all():
        return {"type": "INTERPRETED", "ok": False, "error": "Volume non disponible pour ce ticker."}
    quantiles = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
    results = []
    for q_pct in quantiles:
        q_val = float(matches["vol_ratio"].quantile(q_pct))
        sub = matches[matches["vol_ratio"] >= q_val]
        if len(sub) < 5:
            continue
        taux = round(float(sub["success"].mean() * 100), 1)
        results.append({"Vol ratio min": round(q_val, 2), "N occurrences": len(sub), "Taux succès %": taux})
    optimal = next((r for r in results if r["Taux succès %"] >= target_rate), None)
    conc = (f"Volume ratio ≥ {optimal['Vol ratio min']} → {optimal['Taux succès %']}% de succès ({optimal['N occurrences']} cas)"
            if optimal else f"Aucun seuil de volume ne donne {target_rate}% de succès.")
    return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_volume_threshold",
            "ticker": ticker, "target_rate": target_rate,
            "results": results, "optimal": optimal, "conclusion": conc}


def _exec_engulfing_duration(ticker, matches, pattern):
    """Durée moyenne de la baisse après un engulfing."""
    is_bearish = "bearish" in pattern
    for idx, row in matches.iterrows():
        cj = float(row["close"])
        days = 0
        for i in range(1, 11):
            fc = row.get(f"close_j{i}", np.nan)
            if pd.isna(fc):
                break
            if is_bearish and fc < cj:
                days = i
            elif not is_bearish and fc > cj:
                days = i
            else:
                break
        matches.loc[idx, "days_below"] = days
    vals = matches["days_below"].dropna()
    if vals.empty:
        return {"type": "INTERPRETED", "ok": False, "error": "Pas de données de durée."}
    med = round(float(vals.median()), 1)
    mean = round(float(vals.mean()), 1)
    dist = vals.value_counts().sort_index()
    name = pattern.replace("_", " ")
    return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_duration",
            "ticker": ticker, "pattern": name, "n": len(vals),
            "median_days": med, "mean_days": mean,
            "distribution": [{"jours": int(k), "count": int(v)} for k, v in dist.items()],
            "conclusion": f"Après un {name} sur {ticker}, la baisse dure en médiane {med} jour(s) (moyenne {mean})."}


def _exec_engulfing_avg_perf(ticker, matches, pattern):
    """Performance moyenne après un engulfing."""
    next_vars = matches["next_var"].dropna() if "next_var" in matches.columns else pd.Series(dtype=float)
    n = len(next_vars)
    if n == 0:
        return {"type": "INTERPRETED", "ok": False, "error": "Pas de données J+1 pour les engulfings."}
    avg = round(float(next_vars.mean()), 2)
    med = round(float(next_vars.median()), 2)
    pct_pos = round(float((next_vars > 0).mean() * 100), 1)
    name = pattern.replace("_", " ")
    return {"type": "INTERPRETED", "ok": True, "sub_type": "engulfing_avg_perf",
            "ticker": ticker, "pattern": name, "n": n,
            "avg_next": avg, "med_next": med, "pct_pos_next": pct_pos,
            "conclusion": f"Après un {name} sur {ticker}, la variation J+1 moyenne est {avg:+.2f}% (médiane {med:+.2f}%, {pct_pos}% positif, n={n})."}


def _exec_weekday(ticker, dp):
    rows = []
    for dow in range(5):
        sub = dp[dp["dow"] == dow]
        if sub.empty: continue
        n = len(sub)
        rows.append({"jour": _DOW_LABELS[dow], "var_moy": round(float(sub["var_pct"].mean()), 3),
                      "var_med": round(float(sub["var_pct"].median()), 3), "nb": n,
                      "pct_positif": round(float((sub["var_pct"] > 0).sum() / n * 100), 1)})
    rows.sort(key=lambda r: r["var_moy"], reverse=True)
    b, w = (rows[0], rows[-1]) if rows else (None, None)
    conc = f"Meilleur : {b['jour']} ({b['var_moy']:+.3f}%). Pire : {w['jour']} ({w['var_moy']:+.3f}%)." if b and w else ""
    return {"type": "INTERPRETED", "ok": True, "sub_type": "weekday",
            "ticker": ticker, "rows": rows, "conclusion": conc}


def _exec_month(ticker, dp):
    rows = []
    for m in range(1, 13):
        sub = dp[dp["month"] == m]
        if sub.empty: continue
        n = len(sub)
        rows.append({"mois": _MONTH_LABELS.get(m, str(m)), "var_moy": round(float(sub["var_pct"].mean()), 3),
                      "var_med": round(float(sub["var_pct"].median()), 3), "nb": n,
                      "pct_positif": round(float((sub["var_pct"] > 0).sum() / n * 100), 1)})
    rows.sort(key=lambda r: r["var_moy"], reverse=True)
    b, w = (rows[0], rows[-1]) if rows else (None, None)
    conc = f"Meilleur : {b['mois']} ({b['var_moy']:+.3f}%). Pire : {w['mois']} ({w['var_moy']:+.3f}%)." if b and w else ""
    return {"type": "INTERPRETED", "ok": True, "sub_type": "monthly",
            "ticker": ticker, "rows": rows, "conclusion": conc}


def _exec_annual_perf(ticker, df, period):
    if not period:
        return None
    # Relative period "depuis 2023" → filter then compute per year
    dp = _filter_period(df, period)
    if dp.empty:
        return None
    if "date_from" in period:
        # Group by year within the filtered range
        years = sorted(dp.index.year.unique())
    elif "years" in period:
        years = period["years"]
    elif "year" in period:
        years = [period["year"]]
    else:
        return None
    results = []
    for year in years:
        sub = dp[dp.index.year == year].dropna(subset=["close"])
        if sub.empty:
            continue
        fc, lc = float(sub["close"].iloc[0]), float(sub["close"].iloc[-1])
        results.append({"year": int(year), "perf": round((lc - fc) / fc * 100, 2),
                        "first_close": round(fc, 2), "last_close": round(lc, 2)})
    if len(results) == 1:
        r = results[0]
        return {"type": "INTERPRETED", "ok": True, "sub_type": "single_value",
                "label": f"Performance {ticker} {r['year']}", "value": r["perf"], "unit": "%"}
    if results:
        return {"type": "INTERPRETED", "ok": True, "sub_type": "annual_multi",
                "ticker": ticker, "results": results}
    return None


def _exec_count(ticker, dp, query):
    q = query.lower()
    is_neg = bool(re.search(r"\b(baiss|perd|chut|n[eé]gatif)\b", q))
    count = int((dp["var_pct"] < 0).sum()) if is_neg else int((dp["var_pct"] > 0).sum())
    label = "jours négatifs" if is_neg else "jours positifs"
    total = len(dp)
    return {"type": "INTERPRETED", "ok": True, "sub_type": "count",
            "ticker": ticker, "count": count, "total": total,
            "pct": round(count / total * 100, 1) if total else 0, "label": label}


def _exec_multi_threshold(ticker, dp, interp):
    thresholds = interp.get("threshold", [])
    direction = interp.get("direction", "down")
    results = []
    total = len(dp)
    best_thr, best_pct = None, 0
    for thr in thresholds:
        if direction == "down":
            mask = dp["var_pct"] <= -thr
            label = f"≤ -{thr}%"
        elif direction == "up":
            mask = dp["var_pct"] >= thr
            label = f"≥ +{thr}%"
        else:
            mask = dp["var_pct"].abs() >= thr
            label = f"|var| ≥ {thr}%"
        sub = dp[mask]
        n = len(sub)
        next_sub = sub.dropna(subset=["next_var"]) if "next_var" in sub.columns else pd.DataFrame()
        pct_j1 = round(float((next_sub["next_var"] > 0).mean() * 100), 1) if len(next_sub) > 0 else None
        var_j1 = round(float(next_sub["next_var"].mean()), 2) if len(next_sub) > 0 else None
        row = {"Seuil": label, "Occurrences": n,
               "% du total": round(n / total * 100, 1) if total else 0,
               "% positif J+1": pct_j1, "Var moy J+1": var_j1}
        results.append(row)
        if pct_j1 is not None and pct_j1 > best_pct:
            best_pct, best_thr = pct_j1, label
    conclusion = f"Le seuil {best_thr} maximise le % positif J+1 ({best_pct}%)" if best_thr else None
    return {"type": "INTERPRETED", "ok": True, "sub_type": "multi_threshold",
            "ticker": ticker, "results": results, "total": total, "conclusion": conclusion}


def _exec_filter_abs(ticker, dp, interp):
    criterion = interp.get("criterion", "abs")
    thr = interp.get("threshold", 5)

    # Choisir la colonne selon close-to-close vs open-to-close
    is_intraday = "intraday" in criterion
    if is_intraday and "open" in dp.columns and "close" in dp.columns:
        var_series = ((dp["close"] - dp["open"]) / dp["open"] * 100)
    else:
        var_series = dp["var_pct"]

    # Filtrer selon direction
    if criterion in ("drop", "intraday_drop"):
        mask = var_series <= -thr
    elif criterion in ("gain", "intraday_gain"):
        mask = var_series >= thr
    else:  # abs
        mask = var_series.abs() >= thr

    filtered = dp[mask].copy()
    filtered["_var_display"] = var_series[mask]
    n = len(filtered)
    variation_type = "open-to-close" if is_intraday else "close-to-close"

    if n == 0:
        return {"type": "INTERPRETED", "ok": True, "sub_type": "count",
                "ticker": ticker, "count": 0, "total": len(dp), "pct": 0,
                "label": f"jours avec variation ≥ {thr}% ({variation_type})",
                "variation_type": variation_type}
    fn = filtered.dropna(subset=["next_var"])
    pp = round(float((fn["next_var"] > 0).sum() / len(fn) * 100), 1) if len(fn) > 0 else 0
    mn = round(float(fn["next_var"].mean()), 2) if len(fn) > 0 else 0
    dates = [{"date": _fmt_date(i), "var": round(float(r["_var_display"]), 2),
              "next_var": round(float(r["next_var"]), 2) if not pd.isna(r.get("next_var", np.nan)) else None}
             for i, r in filtered.iterrows()]
    return {"type": "INTERPRETED", "ok": True, "sub_type": "filter_abs",
            "ticker": ticker, "n": n, "threshold": thr,
            "pct_positive_next": pp, "mean_next": mn, "dates": dates,
            "variation_type": variation_type}


def _exec_multi_condition(interp, query):
    """Multi-CSV: 'quand VIX > 25 et que AAOI baisse de 5%'."""
    q = query.lower()
    # Parse conditions from the query text
    parts = re.split(r"\bet\s+que?\b|\bET\b", query, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) < 2:
        return None
    part1, part2 = parts[0].strip(), parts[1].strip()
    # Detect assets in each part
    from query_interpreter import _detect_ticker, _TICKERS_KNOWN
    all_tickers = sorted(_TICKERS_KNOWN, key=len, reverse=True)
    asset1, asset2 = None, None
    for t in all_tickers:
        if re.search(rf"\b{re.escape(t)}\b", part1.lower()) and not asset1:
            asset1 = t.upper()
        if re.search(rf"\b{re.escape(t)}\b", part2.lower()) and not asset2:
            asset2 = t.upper()
    if not asset1 and not asset2:
        return None
    if not asset1:
        asset1 = asset2
    if not asset2:
        asset2 = asset1
    ticker = interp.get("ticker") or asset2 or "SPX"

    df1 = _load_csv_by_name(asset1)
    df2 = _load_csv_by_name(asset2) if asset2 != asset1 else df1
    if df1 is None or df2 is None:
        return {"type": "INTERPRETED", "ok": False, "error": f"CSV introuvable pour {asset1} ou {asset2}."}
    period = interp.get("period")
    if period:
        df1 = _filter_period(df1, period)
        df2 = _filter_period(df2, period)

    common = df1.index.intersection(df2.index)
    df1c, df2c = df1.loc[common], df2.loc[common]
    mask1 = _apply_condition(df1c, part1)
    mask2 = _apply_condition(df2c, part2)
    filtered = df2c[mask1 & mask2].copy()
    n = len(filtered)
    if n == 0:
        return {"type": "INTERPRETED", "ok": True, "sub_type": "multi_condition",
                "ticker": ticker, "asset_1": asset1, "asset_2": asset2,
                "n": 0, "pct_pos_next": 0, "mean_next": 0, "median_next": 0, "dates": []}
    if "var_pct" not in filtered.columns:
        filtered["var_pct"] = 0
    filtered["next_var"] = (df2c["close"].shift(-1) / df2c["close"] - 1).loc[filtered.index] * 100
    fn = filtered.dropna(subset=["next_var"])
    pp = round(float((fn["next_var"] > 0).mean() * 100), 1) if len(fn) > 0 else 0
    mn = round(float(fn["next_var"].mean()), 2) if len(fn) > 0 else 0
    med = round(float(fn["next_var"].median()), 2) if len(fn) > 0 else 0
    dates = [{"date": _fmt_date(i), "var_J": round(float(r.get("var_pct", 0)), 2),
              "next_var": round(float(r["next_var"]), 2) if not pd.isna(r.get("next_var", np.nan)) else None}
             for i, r in filtered.iterrows()]
    return {"type": "INTERPRETED", "ok": True, "sub_type": "multi_condition",
            "ticker": ticker, "asset_1": asset1, "asset_2": asset2,
            "cond_1": part1.strip(), "cond_2": part2.strip(),
            "n": n, "pct_pos_next": pp, "mean_next": mn, "median_next": med, "dates": dates}


def _exec_neutral_next(ticker, dp, interp, query):
    """Trouve le Nème jour J+1 le plus proche de 0% après baisse >= threshold."""
    threshold = float(interp.get("threshold", 5.0))
    rank = int(interp.get("rank", 1))
    filtered = dp[dp["var_pct"] <= -threshold].copy()
    if filtered.empty:
        return {"type": "INTERPRETED", "ok": False,
                "error": f"Aucun jour avec baisse ≥ {threshold}% pour {ticker}."}
    if "next_var" not in dp.columns:
        dp["next_var"] = dp["var_pct"].shift(-1)
    filtered["next_var"] = dp["next_var"].reindex(filtered.index)
    filtered = filtered.dropna(subset=["next_var"])
    if filtered.empty:
        return {"type": "INTERPRETED", "ok": False, "error": "Pas de données J+1."}
    filtered["abs_next"] = filtered["next_var"].abs()
    filtered = filtered.sort_values("abs_next")
    n_total = len(filtered)
    if rank > n_total:
        return {"type": "INTERPRETED", "ok": False,
                "error": f"Seulement {n_total} occurrences (rang {rank} demandé)."}
    row = filtered.iloc[rank - 1]
    date_str = _fmt_date(row.name)
    top5 = []
    for i, (idx, r) in enumerate(filtered.head(5).iterrows()):
        top5.append({"rank": i + 1, "date": _fmt_date(idx),
                      "var_J": round(float(r["var_pct"]), 2),
                      "var_J1": round(float(r["next_var"]), 2),
                      "abs_J1": round(float(r["abs_next"]), 2),
                      "close": round(float(r["close"]), 2) if "close" in r else None})
    rank_label = {1: "1er", 2: "2ème", 3: "3ème"}.get(rank, f"{rank}ème")
    return {
        "type": "INTERPRETED", "ok": True, "sub_type": "neutral_next",
        "ticker": ticker, "threshold": threshold, "rank": rank, "n_total": n_total,
        "date": date_str, "var_J": round(float(row["var_pct"]), 2),
        "var_J1": round(float(row["next_var"]), 2),
        "close": round(float(row["close"]), 2) if "close" in row else None,
        "top5": top5,
        "conclusion": (f"Parmi {n_total} jours avec {ticker} ≤ -{threshold}%, "
                        f"le {rank_label} plus neutre J+1 : {date_str} "
                        f"(var J={row['var_pct']:+.2f}%, var J+1={row['next_var']:+.2f}%)")
    }


def _exec_streak(ticker, dp, direction):
    streaks = []
    cur, start = 0, None
    for idx, row in dp.iterrows():
        match = row["var_pct"] > 0 if direction == "up" else row["var_pct"] < 0
        if match:
            if cur == 0:
                start = idx
            cur += 1
        else:
            if cur > 0:
                streaks.append({"start": start, "end": idx, "length": cur})
            cur = 0
    if cur > 0:
        streaks.append({"start": start, "end": dp.index[-1], "length": cur})
    streaks.sort(key=lambda s: s["length"], reverse=True)
    best = streaks[0] if streaks else None
    avg = round(sum(s["length"] for s in streaks) / len(streaks), 1) if streaks else 0
    top5 = [{"length": s["length"], "start": _fmt_date(s["start"]), "end": _fmt_date(s["end"])}
            for s in streaks[:5]]
    label = "haussière" if direction == "up" else "baissière"
    return {"type": "INTERPRETED", "ok": True, "sub_type": "streak_analysis",
            "ticker": ticker, "direction": direction,
            "best": {"length": best["length"], "start": _fmt_date(best["start"]),
                     "end": _fmt_date(best["end"])} if best else None,
            "top5": top5, "avg_streak": avg,
            "conclusion": f"Record {label} : {best['length']} jours du {_fmt_date(best['start'])} au {_fmt_date(best['end'])}." if best else "Aucune série trouvée."}


def _exec_bias(ticker, dp, period):
    if dp.empty:
        return {"type": "INTERPRETED", "ok": False, "error": "Aucune donnée."}
    n = len(dp)
    n_pos = int((dp["var_pct"] > 0).sum())
    n_neg = n - n_pos
    pct_pos = round(n_pos / n * 100, 1)
    mean_v = round(float(dp["var_pct"].mean()), 3)
    med_v = round(float(dp["var_pct"].median()), 3)
    skew_v = round(float(dp["var_pct"].skew()), 3)
    biais = "haussier" if pct_pos > 52 else "baissier" if pct_pos < 48 else "neutre"
    # Compare SPX
    spx_ctx = {}
    spx = _load_csv_by_name("SPX")
    if spx is not None:
        sp = _filter_period(spx, period) if period else spx
        if not sp.empty and "var_pct" in sp.columns:
            spx_ctx = {"pct_pos": round(float((sp["var_pct"] > 0).mean() * 100), 1),
                        "mean_var": round(float(sp["var_pct"].mean()), 3)}
    return {"type": "INTERPRETED", "ok": True, "sub_type": "bias_analysis",
            "ticker": ticker, "n": n, "n_pos": n_pos, "n_neg": n_neg,
            "pct_pos": pct_pos, "mean_var": mean_v, "median_var": med_v,
            "skew": skew_v, "biais": biais, "spx_context": spx_ctx}


def _exec_correlation_scan(ticker, dp, period):
    """Corrélation du ticker avec tous les CSV marché."""
    if dp.empty or "var_pct" not in dp.columns:
        return {"type": "INTERPRETED", "ok": False, "error": "Pas de données var_pct."}
    v1 = dp["var_pct"].dropna()
    csvs = _get_market_csvs(exclude_ticker=ticker)
    results = []
    for name, col in csvs.items():
        try:
            df_ext = _load_csv_by_name(name)
        except Exception:
            continue
        if df_ext is None or col not in df_ext.columns:
            # Try fallback column
            if df_ext is not None and "close" in df_ext.columns:
                col = "close"
            else:
                continue
        if period:
            df_ext = _filter_period(df_ext, period)
        v2 = df_ext[col].reindex(v1.index, method="nearest", tolerance=pd.Timedelta("2D")).dropna()
        common = v1.index.intersection(v2.index)
        if len(common) < 30:
            continue
        corr = round(float(v1.loc[common].corr(v2.loc[common])), 4)
        if abs(corr) > 0.05:
            label = name.replace("_daily", "").replace("_Daily", "")
            results.append({
                "Actif": label, "Corrélation": corr,
                "Force": "forte" if abs(corr) > 0.6 else "modérée" if abs(corr) > 0.3 else "faible",
                "Direction": "positive" if corr > 0 else "négative",
                "N jours": len(common),
            })
    results.sort(key=lambda r: abs(r["Corrélation"]), reverse=True)
    bp = next((r for r in results if r["Corrélation"] > 0), None)
    bn = next((r for r in results if r["Corrélation"] < 0), None)
    conc = ""
    if bp: conc += f"Corrél. positive max : {bp['Actif']} ({bp['Corrélation']:+.3f}). "
    if bn: conc += f"Corrél. négative max : {bn['Actif']} ({bn['Corrélation']:+.3f})."
    return {"type": "INTERPRETED", "ok": True, "sub_type": "correlation_scan",
            "ticker": ticker, "n_assets": len(results),
            "results": results[:20], "conclusion": conc}


def _exec_correlation(ticker, interp):
    t2 = interp.get("ticker_2", "SPX")
    period = interp.get("period")
    df1 = _load_csv_by_name(ticker)
    df2 = _load_csv_by_name(t2)
    if df1 is None or df2 is None:
        return {"type": "INTERPRETED", "ok": False, "error": f"CSV introuvable pour {ticker} ou {t2}."}
    if period:
        df1 = _filter_period(df1, period)
        df2 = _filter_period(df2, period)
    common = df1.index.intersection(df2.index)
    v1 = df1.loc[common, "var_pct"].dropna()
    v2 = df2.loc[common, "var_pct"].dropna()
    common2 = v1.index.intersection(v2.index)
    v1, v2 = v1.loc[common2], v2.loc[common2]
    if len(v1) < 20:
        return {"type": "INTERPRETED", "ok": False, "error": "Pas assez de données communes."}
    corr = round(float(v1.corr(v2)), 4)
    n = len(v1)
    return {"type": "INTERPRETED", "ok": True, "sub_type": "correlation",
            "ticker": ticker, "ticker_2": t2, "corr": corr, "n": n}


_EXPLANATIONS = {
    "VIX": "Le VIX (CBOE Volatility Index) mesure la volatilité implicite du SPX sur 30 jours.\nVIX élevé (>25) = peur du marché. VIX bas (<15) = complaisance.\nHistorique : <15 calme, 15-25 normal, >25 stress, >40 panique.\nRelation SPX : corrélation négative forte (-0.7 à -0.8).",
    "VVIX": "Le VVIX est la volatilité du VIX — la volatilité de la volatilité.\nVVIX élevé = les investisseurs s'attendent à des mouvements violents du VIX lui-même.\nUtile pour identifier les périodes de stress extrême ou de complaisance.",
    "SKEW": "Le SKEW Index mesure la demande de protection contre les baisses extrêmes du SPX.\nSKEW > 140 = forte demande de protection = anticipation de tail risk.\nSKEW < 100 = peu de protection = complaisance.\nUn SKEW élevé ne prédit pas une baisse mais indique une asymétrie du risque.",
    "engulfing": "Le Bearish Engulfing est un pattern en 2 bougies :\n1. Bougie verte (haussière)\n2. Bougie rouge plus grande qui l'englobe\nValidation ici : close OU low dans J+1..J+5 ≤ close_J × 0.98 (seuil -2%).\nExclusion : ±5j autour des earnings.",
    "put_call": "Le ratio Put/Call = options put / options call achetées.\nRatio élevé (>1.2) = protection massive = signal contrarian haussier.\nRatio bas (<0.7) = euphorie = risque de correction.\nIndicateur de sentiment contrarian.",
    "RSI": "Le RSI (Relative Strength Index) est un oscillateur entre 0 et 100.\nRSI > 70 = suracheté. RSI < 30 = survendu.\nLimite : peut rester en zone extrême longtemps en tendance forte.",
    "momentum": "Le momentum mesure la vitesse du mouvement des prix.\nMomentum 1m = (close - close il y a 21j) / close × 100.\nAcadémiquement validé (Jegadeesh & Titman 1993) : tendance à persister.",
    "corrélation": "Corrélation de Pearson : relation linéaire entre 2 actifs (-1 à +1).\n+1 = identiques, -1 = opposés, 0 = aucune relation.\nFort |r|>0.6, modéré 0.3-0.6, faible <0.3.\nAttention : corrélation ≠ causalité.",
}


def _exec_explain_general(interp):
    subject = interp.get("criterion", "général")
    text = _EXPLANATIONS.get(subject)
    if text:
        return {"type": "INTERPRETED", "ok": True, "sub_type": "text_explanation_general",
                "subject": subject, "text": text, "ticker": interp.get("ticker")}
    return None


def _exec_intraday(interp, query):
    try:
        from spx_intraday import (build_daily_sessions, find_intraday_patterns,
                                   find_best_intraday_time, analyze_overnight)
    except ImportError as e:
        return {"type": "INTERPRETED", "ok": False, "error": f"spx_intraday: {e}"}
    q = query.lower()
    horizon = interp.get("threshold")
    period = interp.get("period")

    if re.search(r"meilleur\w*\s+(?:moment|heure)|quand\s+acheter|quelle\s+heure", q):
        sessions = build_daily_sessions("SPY", "30min")
        if sessions.empty:
            return {"type": "INTERPRETED", "ok": False, "error": "Données intraday non disponibles."}
        if period:
            sessions = _filter_period(sessions, period)
        times = find_best_intraday_time(sessions)
        if not times:
            return {"type": "INTERPRETED", "ok": False, "error": "Pas assez de données."}
        b = times[0]
        return {"type": "INTERPRETED", "ok": True, "sub_type": "intraday_best_time",
                "ticker": "SPX/SPY", "results": times[:12], "best": b,
                "conclusion": f"Meilleure heure : {b['entry_time']} ({b['pct_positive']:.1f}% positif, moy {b['mean_ret']:+.3f}%)"}

    if re.search(r"overnight|nuit|futures?\s+spx", q):
        on = analyze_overnight()
        if on.empty:
            return {"type": "INTERPRETED", "ok": False, "error": "Données overnight non disponibles."}
        corr = round(float(on["overnight_ret"].corr(on["day_ret"])), 3)
        same = round(float(((on["overnight_direction"] == 1) == (on["day_ret"] > 0)).mean() * 100), 1)
        return {"type": "INTERPRETED", "ok": True, "sub_type": "intraday_overnight",
                "ticker": "SPX", "n": len(on), "corr_overnight_day": corr,
                "pct_same_direction": same,
                "mean_overnight_ret": round(float(on["overnight_ret"].mean()), 3),
                "conclusion": f"Corrélation overnight→jour : {corr:+.3f}. {same}% même direction."}

    # Conditional: gap or VIX condition
    if re.search(r"gap|quand\s+vix|si\s+vix|ouvre\s+en", q):
        sessions = build_daily_sessions("SPY", "30min")
        if not sessions.empty:
            if period:
                sessions = _filter_period(sessions, period)
            cond_fn = None
            cond_label = ""
            if re.search(r"gap\s+(haussier|positif|hausse)", q):
                cond_fn = lambda s: s["gap_pct"] > 0.5
                cond_label = "gap haussier (>0.5%)"
            elif re.search(r"gap\s+(baissier|n[eé]gatif|baisse)", q):
                cond_fn = lambda s: s["gap_pct"] < -0.5
                cond_label = "gap baissier (<-0.5%)"
            elif (m := re.search(r"gap\s*[>≥]?\s*(?:de\s+)?(\d+[\.,]?\d*)\s*%?", q)):
                t = float(m.group(1).replace(",", "."))
                cond_fn = lambda s, _t=t: s["gap_pct"].abs() > _t
                cond_label = f"gap > {t}%"
            if cond_fn:
                try:
                    filtered = sessions[cond_fn(sessions)]
                except Exception:
                    filtered = sessions
                n = len(filtered)
                hrs = []
                best_h, best_pct = "", 0
                for col in sorted(c for c in filtered.columns if c.startswith("ret_")):
                    vals = filtered[col].dropna()
                    if len(vals) < 5:
                        continue
                    pct = round(float((vals > 0).mean() * 100), 1)
                    label = col.replace("ret_", "").replace("min", " min").replace("close", "clôture")
                    hrs.append({"Horizon": label, "% positif": pct,
                                "Moy %": round(float(vals.mean()), 3),
                                "Médiane %": round(float(vals.median()), 3), "N": len(vals)})
                    if pct > best_pct:
                        best_pct, best_h = pct, label
                return {"type": "INTERPRETED", "ok": True, "sub_type": "intraday_conditional",
                        "ticker": "SPX/SPY", "condition": cond_label, "n_sessions": n,
                        "horizon_results": hrs, "best_horizon": best_h, "best_pct_positive": best_pct,
                        "conclusion": f"Sur {n} sessions '{cond_label}' : meilleur horizon = {best_h} ({best_pct:.1f}% positif)"}

    # Default: general horizon stats
    sessions = build_daily_sessions("SPY", "30min")
    if sessions.empty:
        return {"type": "INTERPRETED", "ok": False, "error": "Données non disponibles."}
    if period:
        sessions = _filter_period(sessions, period)
    col = f"ret_{horizon}min" if horizon and f"ret_{horizon}min" in sessions.columns else "ret_close"
    vals = sessions[col].dropna()
    if len(vals) < 10:
        return {"type": "INTERPRETED", "ok": False, "error": "Pas assez de données."}
    return {"type": "INTERPRETED", "ok": True, "sub_type": "intraday_general",
            "ticker": "SPX/SPY", "horizon": col, "n": len(vals),
            "mean": round(float(vals.mean()), 3), "median": round(float(vals.median()), 3),
            "pct_positive": round(float((vals > 0).mean() * 100), 1),
            "conclusion": f"SPX/SPY {col.replace('ret_','').replace('min',' min')} : {round(float((vals>0).mean()*100),1)}% positif, moy {round(float(vals.mean()),3):+.3f}%"}


def _exec_ml(interp):
    try:
        from spx_ml import get_or_train, predict_today
    except ImportError as e:
        return {"type": "INTERPRETED", "ok": False, "error": f"spx_ml: {e}"}
    q = interp.get("_query", "").lower()
    entry = "10h30" if "10h30" in q else ("10h00" if "10h" in q else "9h30")
    m_h = re.search(r"(\d+)\s*min", q)
    horizon = f"{m_h.group(1)}min" if m_h else "120min"
    trained = get_or_train(entry, horizon)
    if not trained.get("ok"):
        return {"type": "INTERPRETED", "ok": False, "error": trained.get("error", "Erreur ML.")}
    pred = predict_today(trained)
    return {"type": "INTERPRETED", "ok": True, "sub_type": "ml_amplitude",
            "ticker": "SPX", "entry_point": entry, "prediction": pred,
            "model_stats": {k: trained[k] for k in ("category_accuracy", "amplitude_mae", "n_train", "n_test", "best_model")
                            if k in trained},
            "top_features": trained.get("top_features", {}),
            "pred_distribution": trained.get("pred_distribution", {}),
            "test_distribution": trained.get("test_distribution", {}),
            "conclusion": f"[{entry}] {trained['best_model']} — Précision: {trained.get('category_accuracy', 0):.1f}% (test {trained.get('n_test', 0)}j)"}
