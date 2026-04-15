# query_interpreter.py — Classification sémantique des questions
# Regex fast path + Groq API (si GROQ_API_KEY) ou Ollama llama3.2:3b (fallback)

import json
import re
import urllib.request
import pathlib as _pathlib
import os as _os

# ─── Chargement .env sans dépendance ─────────────────────────────────────

_env = _pathlib.Path(__file__).parent / ".env"
if _env.exists():
    for _l in _env.read_text().splitlines():
        _l = _l.strip()
        if "=" in _l and not _l.startswith("#"):
            _k, _v = _l.split("=", 1)
            _os.environ.setdefault(_k.strip(), _v.strip())

# ─── API config ──────────────────────────────────────────────────────────

GROQ_API_KEY = _os.environ.get("GROQ_API_KEY")
GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = "llama-3.1-8b-instant"
OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "llama3.2:3b"

_SYSTEM_PROMPT = """\
Tu es un classificateur de questions financières. Réponds UNIQUEMENT en JSON valide.
Catégories : LOOKUP_DATE, LOOKUP_BEST, FILTER_STATS, COMPARE, CANDLE_PATTERN, ENGULFING_ANALYSIS, ENGULFING_FAILURE_ANALYSIS, EXPLAIN, WEEKDAY_STATS, MONTH_STATS, ANNUAL_PERF, COUNT, MULTI_THRESHOLD.
Format : {"category":"...","ticker":"AAOI","period":{"year":2024},"criterion":"var_pct_max","direction":"up","pattern":null,"field":"close","threshold":null,"output":"date_and_value"}
"""

# ─── Regex ───────────────────────────────────────────────────────────────

def _get_known_tickers() -> set[str]:
    """Auto-détecte les tickers depuis les CSV dans DATA_DIR et DATA_DIR/tickers/."""
    import pathlib as _p
    data_dir = _p.Path(__file__).parent / "data" / "live_selected"
    tickers_dir = data_dir / "tickers"
    _INDICES = {"spx", "spy", "qqq", "iwm", "vix", "vvix", "vix1d", "vix3m", "vix6m", "vix9d",
                "dxy", "gold", "dax", "dax40", "ftse", "ftse100", "nikkei", "nikkei225", "skew"}
    tickers = set(_INDICES)
    for scan_dir in [data_dir, tickers_dir]:
        if not scan_dir.exists():
            continue
        for f in scan_dir.glob("*.csv"):
            stem = f.stem.lower()
            base = re.sub(r"_(daily|30min|5min|1min|1hour|weekly).*$", "", stem)
            base = re.sub(r"[_\s,]+", "", base)
            if len(base) <= 5 and re.match(r"^[a-z]{1,5}$", base) and base not in _INDICES:
                tickers.add(base)
    return tickers

_TICKERS_KNOWN = _get_known_tickers()

_BEST_RE = re.compile(
    r"\b(meilleure?|pire|plus\s+forte?|plus\s+grosse?|record)\b"
    r".*\b(jour|journ[eé]e|date|séance|perf|hausse|baisse)"
    r"|\ble\s+plus\s+(baiss[eé]|mont[eé]|hauss[eé]|chut[eé])",
    re.IGNORECASE,
)
_LAST_RE = re.compile(r"\b(derni[eè]re?s?|last)\s+(\d+\s+)?(date|fois|jour)", re.IGNORECASE)
_PATTERN_RE = re.compile(
    r"bearish\s+engulfing|bearish\s+engulf|bullish\s+engulfing|bullish\s+engulf"
    r"|bearish\s+e\b|bullish\s+e\b"
    r"|baissier\s+englo\w+|haussier\s+englo\w+"
    r"|bougie\s+englo\w+|chandelier\s+englo\w+"
    r"|englo\w+\s+baissier|englo\w+\s+haussier"
    r"|enveloppant\s+baissier|enveloppant\s+haussier"
    r"|\bBE\b|\bB\.E\.\b|\bengulfing\b",
    re.IGNORECASE,
)
_ENGULFING_ANALYSIS_RE = re.compile(
    r"\b(march[eé]|fonctionn[eé]|r[eé]ussite|[eé]chec|taux|limite|seuil|volume\s+min"
    r"|vix.*engulf|engulf.*vix|pas\s+march[eé]|pas\s+fonctionn[eé]|succ[eè]s"
    r"|derniers?\s+\d+|(\d+)\s+derniers?|ratio|chaque\s+ann[eé]e|par\s+ann[eé]e"
    r"|ann[eé]e\s+par\s+ann[eé]e"
    r"|y\s+a[\s\-]+t[\s\-]+il|eu\s+des|a\s+eu|ont\s+eu|eu\s+un|il\s+y\s+a\s+eu"
    r"|a[\s\-]+t[\s\-]+il\s+eu|avait|y\s+avait"
    r"|montre[\s\-]moi|affiche|liste|tous\s+les|toutes\s+les"
    r"|valid[eé]s?|non\s+valid[eé]s?|tri|class[eé]|r[eé]parti"
    r"|depuis\s+le\s+d[eé]but|historique|complet)\b",
    re.IGNORECASE,
)
_MULTI_PERIOD_RE = re.compile(
    r"\b(idem|pareil|m[eê]me\s+chose|m[eê]me\s+question|aussi|puis)\b.*\b(en|sur|pour)\s+20\d{2}\b",
    re.IGNORECASE,
)
_FAILURE_RE = re.compile(
    r"\b(points?\s+communs?|pourquoi|qu.est.ce\s+qui|corr[eé]lation)\b"
    r".*\b([eé]checs?|pas\s+fonctionn|pas\s+march|rat[eé])\b",
    re.IGNORECASE,
)
_EXPLAIN_RE = re.compile(
    r"\b(setting|configuration|param[eè]tre|comment\s+(?:est|fonctionne|marche))\b"
    r".*\b(engulf|BE\b)", re.IGNORECASE,
)
_COMBIEN_PATTERN_RE = re.compile(r"\bcombien\s+de\s+(?:fois\s+)?(?:le\s+)?(?:bearish\s+)?(?:engulfing|BE)\b", re.IGNORECASE)
_WEEKDAY_RE = re.compile(
    r"\bquel\s+jour\b|\bmeilleur\s+jour\b|\bpire\s+jour\b|\bjour\s+de\s+la\s+semaine\b|\bpar\s+jour\b",
    re.IGNORECASE,
)
_MONTH_RE = re.compile(r"\bquel\s+mois\b|\bmeilleur\s+mois\b|\bpire\s+mois\b|\bpar\s+mois\b", re.IGNORECASE)
_PERF_RE = re.compile(r"\b(performance|perf)\b.*\b(20\d{2})\b", re.IGNORECASE)
_YEAR_RE = re.compile(r"\b(20\d{2})\b")
_LOOKUP_FIELD_RE = re.compile(
    r"\b(cl[oô]ture|close|open|ouverture|high|haut|low|bas|variation|volume)\b", re.IGNORECASE,
)
_DATE_TEXT_RE = re.compile(
    r"\b(\d{1,2})\s+(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre)\s+(\d{4})\b",
    re.IGNORECASE,
)
_DATE_NUM_RE = re.compile(r"\b(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\b")
_FILTER_VERB_RE = re.compile(
    r"\b(perdu|perd|chut[eé]|baiss[eé]|gagn[eé]|mont[eé]|hauss[eé]|plus\s+de|sup[eé]rieur"
    r"|boug[eé]|vari[eé])\b",
    re.IGNORECASE,
)
_COUNT_RE = re.compile(r"\bcombien\s+de\s+fois\b|\bnombre\s+de\s+fois\b", re.IGNORECASE)
_CORRELATION_RE = re.compile(r"\bcorr[eé]lation\b", re.IGNORECASE)
_INTRADAY_RE = re.compile(
    r"\b(intraday|intra.?day)\b"
    r"|\b(meilleur\s+moment|meilleure\s+heure|quand\s+acheter)\b"
    r"|\bovern?ight\b.*\b(spx|spy|futures?)\b"
    r"|\b(spx|spy)\b.*\bovern?ight\b"
    r"|\bgap\b.*\b(spx|spy|ouverture|haussier|baissier|positif|n[eé]gatif)\b"
    r"|\b(ouvre\s+en\s+gap|gap\s+d.ouverture|apr[eè]s\s+un\s+gap)\b"
    r"|\b(30\s+premi[eè]res?\s+min|premi[eè]res?\s+minutes?)\b", re.IGNORECASE)
_ML_RE = re.compile(
    r"\b(pr[eé]di[rct]|forecast|anticip|estim)\w*\b.*\b(spx|spy|march[eé]|ouverture)\b"
    r"|\b(spx|spy)\b.*\b(pr[eé]di|demain|aujourd.hui)\b"
    r"|\b(mod[eè]le|machine\s+learning|ml|xgboost|lightgbm)\b"
    r"|\bsignal\s+(aujourd|demain)\b", re.IGNORECASE)
_VS_RE = re.compile(r"\bvs\.?\b|\bversus\b|\bcomparer\b|\bdiff[eé]rence\s+entre\b", re.IGNORECASE)
_CORR_SCAN_RE = re.compile(
    r"\bplus\s+corr[eé]l[eé]\b|\bmieux\s+corr[eé]l[eé]\b"
    r"|\bcorr[eé]lation\s+avec\s+tous\b"
    r"|\btous\s+les\s+actifs\b.*\bcorr[eé]l\b"
    r"|\btoutes?\s+les?\s+corr[eé]lations?\b",
    re.IGNORECASE)
_MULTI_COND_RE = re.compile(
    r"\b(quand|lorsque|les\s+jours?\s+o[uù]|si|dans\s+les\s+cas)\b"
    r".*\b(et\s+que?|et\b|ET\b)",
    re.IGNORECASE)
_RELATIVE_PERIOD_RE = re.compile(
    r"\bdepuis\s+(\d+)\s+(mois|semaines?|ans?|jours?)\b"
    r"|\bsur\s+(?:les?\s+)?(\d+)\s+(?:derniers?\s+)?(mois|semaines?|ans?|jours?)\b"
    r"|\bdepuis\s+le\s+d[eé]but\s+de\s+l.ann[eé]e\b"
    r"|\bdepuis\s+(20\d{2})\b", re.IGNORECASE)


_TICKER_SYNONYMS = {
    "apple": "AAPL", "microsoft": "MSFT", "google": "GOOG",
    "alphabet": "GOOG", "amazon": "AMZN", "tesla": "TSLA",
    "nvidia": "NVDA", "meta": "META", "netflix": "NFLX",
    "reddit": "RDDT", "hood": "HOOD", "robinhood": "HOOD",
    "micron": "MU", "coherent": "COHR", "mercadolibre": "MELI",
    "applovin": "APP", "ondas": "ONDS", "iren": "IREN",
}

def _detect_ticker(query: str) -> str | None:
    q = re.sub(r'[?!.,;]', ' ', query).lower()
    # Check synonyms first (company names)
    for name, ticker in _TICKER_SYNONYMS.items():
        if re.search(rf"\b{re.escape(name)}\b", q):
            return ticker
    for t in sorted(_TICKERS_KNOWN, key=len, reverse=True):
        if re.search(rf"\b{re.escape(t)}\b", q):
            return t.upper()
    return None


def _detect_years(query: str) -> dict | None:
    # Détecter plage "de 2021 à 2025" ou "2021-2025"
    m_range = re.search(r"\b(20\d{2})\s*(?:à|a|-|jusqu'à|jusqu.à)\s*(20\d{2})\b", query, re.IGNORECASE)
    if m_range:
        y1, y2 = int(m_range.group(1)), int(m_range.group(2))
        if y1 < y2:
            years = list(range(y1, y2 + 1))
            return {"years": years}
    # Années individuelles
    years = sorted(set(int(y) for y in re.findall(r"\b(20\d{2})\b", query)))
    if not years:
        return None
    if len(years) == 1:
        return {"year": years[0]}
    return {"year": years[0], "years": years}


def _detect_thresholds(query: str) -> list[float]:
    return sorted(set(float(m.replace(",", ".")) for m in re.findall(r"(\d+[\.,]?\d*)\s*%", query)))


def _detect_period(query: str) -> dict | None:
    """Détecte périodes relatives ou années fixes."""
    import pandas as _pd
    q = query.lower()
    today = _pd.Timestamp.now().normalize()
    # Périodes relatives d'abord (car "depuis 2023" != "en 2023")
    if re.search(r"d[eé]but\s+de\s+l.ann[eé]e", q):
        return {"date_from": _pd.Timestamp(today.year, 1, 1), "date_to": today}
    m = re.search(r"depuis\s+(\d+)\s+(mois|semaines?|ans?|jours?)", q)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if "mois" in unit: return {"date_from": today - _pd.DateOffset(months=n), "date_to": today}
        if "semaine" in unit: return {"date_from": today - _pd.DateOffset(weeks=n), "date_to": today}
        if "an" in unit: return {"date_from": today - _pd.DateOffset(years=n), "date_to": today}
        if "jour" in unit: return {"date_from": today - _pd.DateOffset(days=n), "date_to": today}
    m = re.search(r"sur\s+(?:les?\s+)?(\d+)\s+(?:derniers?\s+)?(mois|semaines?|ans?|jours?)", q)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if "mois" in unit: return {"date_from": today - _pd.DateOffset(months=n), "date_to": today}
        if "semaine" in unit: return {"date_from": today - _pd.DateOffset(weeks=n), "date_to": today}
        if "an" in unit: return {"date_from": today - _pd.DateOffset(years=n), "date_to": today}
        if "jour" in unit: return {"date_from": today - _pd.DateOffset(days=n), "date_to": today}
    m = re.search(r"depuis\s+(20\d{2})\b", q)
    if m:
        return {"date_from": _pd.Timestamp(int(m.group(1)), 1, 1), "date_to": today}
    # Mois nommés : "en janvier et février 2026"
    _MMP = {"janvier":1,"février":2,"fevrier":2,"mars":3,"avril":4,"mai":5,"juin":6,
            "juillet":7,"août":8,"aout":8,"septembre":9,"octobre":10,"novembre":11,
            "décembre":12,"decembre":12}
    _mf = re.findall(r"\b(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre)\b", q)
    _yf = re.findall(r"\b(20\d{2})\b", q)
    if _mf and _yf:
        import calendar as _cal
        _yr = int(_yf[0])
        _mnums = sorted(set(_MMP.get(m.lower().replace("é","e").replace("û","u"), 0) for m in _mf) - {0})
        if _mnums:
            _ld = _cal.monthrange(_yr, _mnums[-1])[1]
            return {"date_from": _pd.Timestamp(_yr, _mnums[0], 1),
                    "date_to": _pd.Timestamp(_yr, _mnums[-1], _ld),
                    "months": _mnums, "year": _yr}
    # Fallback: fixed years
    return _detect_years(query)


def _has_date(query: str) -> bool:
    return bool(_DATE_TEXT_RE.search(query) or _DATE_NUM_RE.search(query))


def _is_bullish(q: str) -> bool:
    return bool(re.search(r"\bbullish\b|\bhaussier\b|\bbullish\s+e\b", q, re.IGNORECASE))


def _classify_regex(query: str) -> dict | None:
    q = query.lower()
    ticker = _detect_ticker(query)
    period = _detect_period(query)

    # EXPLAIN_GENERAL: "c'est quoi le VIX", "comment fonctionne le RSI"
    if re.search(r"\b(c.est\s+quoi|qu.est.ce\s+que|comment\s+fonctionne|explique|d[eé]fini)", q):
        subject = "général"
        if re.search(r"\bvix\b", q) and not re.search(r"\bvvix\b", q): subject = "VIX"
        elif re.search(r"\bvvix\b", q): subject = "VVIX"
        elif re.search(r"\bskew\b", q): subject = "SKEW"
        elif re.search(r"\bengulfing|bougie\b", q): subject = "engulfing"
        elif re.search(r"\bput.call|pcr\b", q): subject = "put_call"
        elif re.search(r"\brsi\b", q): subject = "RSI"
        elif re.search(r"\bmomentum\b", q): subject = "momentum"
        elif re.search(r"\bcorr[eé]l", q): subject = "corrélation"
        return {"category": "EXPLAIN_GENERAL", "ticker": ticker, "period": period,
                "criterion": subject, "direction": None, "pattern": None,
                "field": None, "threshold": None, "output": "text"}

    # LOOKUP_DATE
    if _LOOKUP_FIELD_RE.search(q) and _has_date(query):
        field = "close"
        if re.search(r"\b(open|ouverture)\b", q): field = "open"
        elif re.search(r"\b(high|haut)\b", q): field = "high"
        elif re.search(r"\b(low|bas)\b", q): field = "low"
        elif re.search(r"\bvari", q): field = "var_pct"
        elif re.search(r"\bvolume\b", q): field = "volume"
        return {"category": "LOOKUP_DATE", "ticker": ticker, "period": period,
                "criterion": None, "direction": None, "pattern": None,
                "field": field, "threshold": None, "output": "value"}

    # ML PREDICT
    if _ML_RE.search(q):
        m_h = re.search(r"(\d+)\s*(min|heure)", q)
        h = int(m_h.group(1)) * (1 if "min" in m_h.group(2) else 60) if m_h else 30
        return {"category": "ML_PREDICT", "ticker": "SPX", "period": period,
                "criterion": None, "direction": None, "pattern": None,
                "field": "ret", "threshold": h, "output": "prediction"}

    # INTRADAY
    if _INTRADAY_RE.search(q):
        m_h = re.search(r"(\d+)\s*(min|heure)", q)
        h = int(m_h.group(1)) * (1 if "min" in m_h.group(2) else 60) if m_h else None
        return {"category": "INTRADAY_ANALYSIS", "ticker": "SPX", "period": period,
                "criterion": None, "direction": None, "pattern": None,
                "field": "ret", "threshold": h, "output": "stats"}

    # CORRELATION_SCAN: "quel actif est le plus corrélé à AAOI"
    if _CORR_SCAN_RE.search(q):
        return {"category": "CORRELATION_SCAN", "ticker": ticker, "period": period,
                "criterion": None, "direction": None, "pattern": None,
                "field": "var_pct", "threshold": None, "output": "table"}

    # CORRELATION: "corrélation entre AAOI et SPX"
    if _CORRELATION_RE.search(q):
        tickers = []
        for t in sorted(_TICKERS_KNOWN, key=len, reverse=True):
            if re.search(rf"\b{re.escape(t)}\b", q) and t.upper() not in tickers:
                tickers.append(t.upper())
        if len(tickers) >= 2:
            return {"category": "CORRELATION", "ticker": tickers[0], "period": period,
                    "ticker_2": tickers[1], "criterion": None, "direction": None,
                    "pattern": None, "field": "var_pct", "threshold": None, "output": "value"}

    # MULTI_CONDITION: "quand VIX > 25 et AAOI baisse de 5%"
    if _MULTI_COND_RE.search(q):
        return {"category": "MULTI_CONDITION", "ticker": ticker, "period": period,
                "criterion": None, "direction": None, "pattern": None,
                "field": None, "threshold": None, "output": "stats_and_next"}

    # COMPARE
    if _VS_RE.search(q):
        return None

    # EXPLAIN engulfing
    if _EXPLAIN_RE.search(q):
        pattern = "bullish_engulfing" if _is_bullish(q) else "bearish_engulfing"
        return {"category": "EXPLAIN", "ticker": ticker, "period": period,
                "criterion": None, "direction": None, "pattern": pattern,
                "field": None, "threshold": None, "output": "text"}

    # ENGULFING_FAILURE_ANALYSIS
    pm = _PATTERN_RE.search(q)
    if pm and _FAILURE_RE.search(q):
        pattern = "bullish_engulfing" if _is_bullish(q) else "bearish_engulfing"
        return {"category": "ENGULFING_FAILURE_ANALYSIS", "ticker": ticker, "period": period,
                "criterion": None, "direction": None, "pattern": pattern,
                "field": None, "threshold": None, "output": "analysis"}

    # ENGULFING_MULTI_PERIOD: "BE 2025 idem en 2024 idem en 2023"
    if pm and _MULTI_PERIOD_RE.search(q):
        years_found = sorted(set(int(y) for y in re.findall(r"\b(20\d{2})\b", query)), reverse=True)
        if len(years_found) >= 2:
            pattern = "bullish_engulfing" if _is_bullish(q) else "bearish_engulfing"
            return {"category": "ENGULFING_MULTI_PERIOD", "ticker": ticker,
                    "pattern": pattern, "years": years_found,
                    "period": None, "criterion": None, "direction": None,
                    "field": None, "threshold": None, "output": "multi_period"}

    # ENGULFING_ANALYSIS (must check before plain CANDLE_PATTERN)
    if pm:
        pattern = "bullish_engulfing" if _is_bullish(q) else "bearish_engulfing"
        # Specific criterion detection (before general analysis gate)
        criterion = None
        if re.search(r"\bvolume\b.*\b(minimum|seuil|quel)\b|\b(quel|minimum)\b.*\bvolume\b", q):
            criterion = "volume_threshold"
        elif re.search(r"\b(performance|variation)\s+moyenne\b.*\bapr[eè]s\b|\bapr[eè]s\b.*\b(performance|variation)\s+moyenne\b", q):
            criterion = "avg_performance"
        elif re.search(r"\bdur[eé]e\b|\bcombien\s+de\s+jours\b.*\bbaisse\b|\bbaisse\b.*\bcombien\s+de\s+jours\b", q):
            criterion = "duration"
        if criterion or _ENGULFING_ANALYSIS_RE.search(q):
            return {"category": "ENGULFING_ANALYSIS", "ticker": ticker, "period": period,
                    "criterion": criterion, "direction": None, "pattern": pattern,
                    "field": None, "threshold": None, "output": "analysis"}

    # CANDLE_PATTERN: "combien de BE" / "combien de fois le BE"
    if _COMBIEN_PATTERN_RE.search(q):
        pattern = "bullish_engulfing" if _is_bullish(q) else "bearish_engulfing"
        # "combien de fois le BE a fonctionné" → ENGULFING_ANALYSIS
        if re.search(r"\b(fonctionn|march|succ|r[eé]ussi)\b", q):
            return {"category": "ENGULFING_ANALYSIS", "ticker": ticker, "period": period,
                    "criterion": None, "direction": None, "pattern": pattern,
                    "field": None, "threshold": None, "output": "analysis"}
        return {"category": "CANDLE_PATTERN", "ticker": ticker, "period": period,
                "criterion": "count", "direction": None, "pattern": pattern,
                "field": None, "threshold": None, "output": "count"}

    # CANDLE_PATTERN: dernière + pattern
    if pm and _LAST_RE.search(q):
        pattern = "bullish_engulfing" if _is_bullish(q) else "bearish_engulfing"
        return {"category": "CANDLE_PATTERN", "ticker": ticker, "period": period,
                "criterion": "last", "direction": None, "pattern": pattern,
                "field": None, "threshold": None, "output": "date_and_value"}

    # Bare pattern (e.g. "bearish E sur AAOI en 2025") → analyse complète
    if pm:
        pattern = "bullish_engulfing" if _is_bullish(q) else "bearish_engulfing"
        return {"category": "ENGULFING_ANALYSIS", "ticker": ticker, "period": period,
                "criterion": None, "direction": None, "pattern": pattern,
                "field": None, "threshold": None, "output": "analysis"}

    # WEEKDAY_STATS
    has_super = bool(re.search(r"\ble\s+plus\b|\bmaximum\b|\bminimum\b", q))
    if _WEEKDAY_RE.search(q) and not _BEST_RE.search(q) and not has_super:
        return {"category": "WEEKDAY_STATS", "ticker": ticker, "period": period,
                "criterion": "var_pct_max", "direction": None, "pattern": None,
                "field": "var_pct", "threshold": None, "output": "table"}

    # MONTH_STATS
    if _MONTH_RE.search(q):
        return {"category": "MONTH_STATS", "ticker": ticker, "period": period,
                "criterion": "var_pct_max", "direction": None, "pattern": None,
                "field": "var_pct", "threshold": None, "output": "table"}

    # STREAK_ANALYSIS: "plus long enchaînement", "record jours positifs"
    if re.search(r"\bplus\s+long\b.*\b(jours?\s+positifs?|jours?\s+n[eé]gatifs?|hausse|baisse|cons[eé]cutifs?)\b"
                 r"|\brecord\b.*\b(cons[eé]cutifs?|jours?\s+positifs?|jours?\s+n[eé]gatifs?)\b"
                 r"|\bencha[iî]nement\b.*\b(positifs?|n[eé]gatifs?|hausse|baisse)\b", q):
        direction = "down" if re.search(r"\bn[eé]gatif\b|\bbaisse\b", q) else "up"
        return {"category": "STREAK_ANALYSIS", "ticker": ticker, "period": period,
                "criterion": None, "direction": direction, "pattern": None,
                "field": "var_pct", "threshold": None, "output": "analysis"}

    # BIAS_ANALYSIS: "biais haussier/baissier", "penchant", "tendance générale"
    if re.search(r"\bbiais\b|\bpenchant\b|\btendan\w+\s+g[eé]n[eé]rale\b", q):
        return {"category": "BIAS_ANALYSIS", "ticker": ticker, "period": period,
                "criterion": None, "direction": None, "pattern": None,
                "field": None, "threshold": None, "output": "analysis"}

    # NEUTRAL_NEXT: "perf la plus neutre", "proche de 0%"
    if re.search(r"\bplus\s+neutre\b|\bproche\s+de\s+0\b|\bproche\s+de\s+z[eé]ro\b"
                 r"|\bla\s+plus\s+neutre\b|\bperf\s+neutre\b", q):
        rank = 1
        if re.search(r"\b2[eè]me\b|\bdeuxi[eè]me\b|\bsecond\b", q): rank = 2
        elif re.search(r"\b3[eè]me\b|\btroisi[eè]me\b", q): rank = 3
        m_thr = re.search(r"-\s*(\d+[\.,]?\d*)\s*%", q)
        thr = float(m_thr.group(1).replace(",", ".")) if m_thr else 5.0
        return {"category": "NEUTRAL_NEXT", "ticker": ticker, "period": period,
                "criterion": "closest_to_zero_next", "direction": "down",
                "pattern": None, "field": "var_pct", "threshold": thr,
                "output": "value", "rank": rank}

    # LOOKUP_BEST
    if _BEST_RE.search(q):
        direction = "down" if re.search(r"\bpire\b|\bbaiss[eé]\b|\bmoins\b|\bchut[eé]\b|\bperdu\b", q) else "up"
        return {"category": "LOOKUP_BEST", "ticker": ticker, "period": period,
                "criterion": "var_pct_max" if direction == "up" else "var_pct_min",
                "direction": direction, "pattern": None,
                "field": "var_pct", "threshold": None, "output": "date_and_value"}

    # ANNUAL_PERF
    if _PERF_RE.search(q):
        return {"category": "ANNUAL_PERF", "ticker": ticker, "period": period,
                "criterion": None, "direction": None, "pattern": None,
                "field": "close", "threshold": None, "output": "value"}

    # MULTI_THRESHOLD
    thresholds = _detect_thresholds(query)
    if len(thresholds) >= 2 and _FILTER_VERB_RE.search(q):
        direction = "down" if re.search(r"\b(perdu|perd|chut[eé]?|baiss[eé]?)\b", q) else "up"
        return {"category": "MULTI_THRESHOLD", "ticker": ticker, "period": period,
                "criterion": None, "direction": direction, "pattern": None,
                "field": "var_pct", "threshold": thresholds, "output": "table"}

    # FILTER_STATS (must check before COUNT — "combien de fois X a perdu N%" with threshold goes here)
    if _FILTER_VERB_RE.search(q) and re.search(r"\d+[\.,]?\d*\s*%", q):
        pass  # handled below, don't double-route
    elif _COUNT_RE.search(q):
        return {"category": "COUNT", "ticker": ticker, "period": period,
                "criterion": None, "direction": None, "pattern": None,
                "field": "var_pct", "threshold": None, "output": "value"}

    # FILTER_STATS
    if _FILTER_VERB_RE.search(q) and re.search(r"\d+[\.,]?\d*\s*%", q):
        direction = "down" if re.search(r"\b(perdu|perd|chut|baiss)\b", q) else "up"
        if re.search(r"\bboug[eé]\b", q):
            direction = "abs"
        m_thr = re.search(r"(\d+[\.,]?\d*)\s*%", q)
        thr = float(m_thr.group(1).replace(",", ".")) if m_thr else None
        # Détection open-to-close (intraday)
        is_intraday = bool(re.search(
            r"\b(open\s+to\s+close|open-to-close|intra.?day|ouverture\s+[aà]\s+cl[oô]ture)\b",
            q, re.IGNORECASE
        ))
        if is_intraday and direction == "down":
            criterion = "intraday_drop"
        elif is_intraday and direction == "up":
            criterion = "intraday_gain"
        elif direction == "down":
            criterion = "drop"
        elif direction == "abs":
            criterion = "abs"
        else:
            criterion = "gain"
        return {"category": "FILTER_STATS", "ticker": ticker, "period": period,
                "criterion": criterion,
                "direction": direction, "pattern": None,
                "field": "var_pct", "threshold": thr, "output": "stats_and_next"}

    return None


def _classify_llm(query: str) -> dict:
    try:
        if GROQ_API_KEY:
            return _classify_groq(query)
        return _classify_ollama(query)
    except Exception as e:
        print(f"[interpreter] LLM error: {e}", flush=True)
    return {"category": "UNKNOWN"}


def _classify_groq(query: str) -> dict:
    body = json.dumps({
        "model": GROQ_MODEL,
        "messages": [{"role": "system", "content": _SYSTEM_PROMPT}, {"role": "user", "content": query}],
        "temperature": 0.0, "max_tokens": 300,
    })
    try:
        import requests as _req
        resp = _req.post(GROQ_URL, data=body, headers={
            "Content-Type": "application/json", "Authorization": f"Bearer {GROQ_API_KEY}",
        }, timeout=15)
        data = resp.json()
    except ImportError:
        import ssl
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        req = urllib.request.Request(GROQ_URL, data=body.encode(), headers={
            "Content-Type": "application/json", "Authorization": f"Bearer {GROQ_API_KEY}",
        })
        with urllib.request.urlopen(req, timeout=15, context=ctx) as resp:
            data = json.loads(resp.read())
    if "error" in data:
        print(f"[interpreter] Groq error: {data['error']}", flush=True)
        return {"category": "UNKNOWN"}
    raw = data["choices"][0]["message"]["content"].strip()
    start, end = raw.find("{"), raw.rfind("}") + 1
    if start >= 0 and end > start:
        result = json.loads(raw[start:end])
        if "category" in result:
            if result.get("ticker"):
                result["ticker"] = result["ticker"].upper()
            print(f"[interpreter] Groq: {result['category']} | {query[:50]}", flush=True)
            return result
    return {"category": "UNKNOWN"}


def _classify_ollama(query: str) -> dict:
    prompt = f"{_SYSTEM_PROMPT}\n\nQ: {query}\n"
    payload = json.dumps({
        "model": OLLAMA_MODEL, "prompt": prompt, "stream": False,
        "options": {"temperature": 0.0, "num_predict": 300},
    }).encode()
    req = urllib.request.Request(OLLAMA_URL, data=payload,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    raw = data.get("response", "").strip()
    start, end = raw.find("{"), raw.rfind("}") + 1
    if start >= 0 and end > start:
        result = json.loads(raw[start:end])
        if "category" in result:
            if result.get("ticker"):
                result["ticker"] = result["ticker"].upper()
            print(f"[interpreter] Ollama: {result['category']} | {query[:50]}", flush=True)
            return result
    return {"category": "UNKNOWN"}


def interpret_query(query: str, active_ticker: str | None = None,
                    last_category: str | None = None,
                    last_params: dict | None = None) -> dict:
    # Follow-up contextuel (2 niveaux max)
    if last_category and last_params:
        q = query.lower()
        if last_category == "ENGULFING_ANALYSIS":
            if re.search(r"\b([eé]checs?|rat[eé]|pas\s+fonctionn|points?\s+communs?|pourquoi|corr[eé]l)\b", q):
                return _fill_ticker({"category": "ENGULFING_FAILURE_ANALYSIS",
                    "pattern": last_params.get("pattern", "bearish_engulfing"),
                    "period": None, "ticker": None}, active_ticker)
        if last_category == "LOOKUP_BEST":
            if re.search(r"\b(et\s+en|en)\s+\d{4}\b", q):
                return _fill_ticker({"category": "LOOKUP_BEST",
                    "period": _detect_period(query),
                    "direction": last_params.get("direction", "up"),
                    "criterion": last_params.get("criterion", "var_pct_max"),
                    "field": "var_pct", "output": "date_and_value",
                    "ticker": None}, active_ticker)

    result = _classify_regex(query)
    if result is not None:
        _fill_ticker(result, active_ticker)
        print(f"[interpreter] regex: {result['category']} | {query[:50]}", flush=True)
        return result
    result = _classify_llm(query)
    _fill_ticker(result, active_ticker)
    return result


def _fill_ticker(result: dict, active_ticker: str | None) -> dict:
    if not result.get("ticker"):
        if active_ticker:
            result["ticker"] = active_ticker.upper()
            result["ticker_source"] = "context"
        else:
            result["ticker"] = "SPX"
            result["ticker_source"] = "default"
    elif "ticker_source" not in result:
        result["ticker_source"] = "explicit"
    return result
