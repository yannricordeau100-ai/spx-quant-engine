"""
calendar_features.py — Features macro/calendar pour le SPX ML engine.

Colonnes produites (une par date) :
- is_opex          : bool — jour d'expiration options
- is_high_impact   : bool — au moins 1 événement high impact ce jour (US/EU)
- n_high_events    : int  — nombre d'événements high impact
- n_medium_events  : int  — nombre d'événements medium impact
- macro_surprise   : float — somme des (Actual - Estimate) pour events US high impact
                             normalisée par écart-type rolling 60j (z-score)
                             NaN si pas d'événement
- is_earnings_top2 : bool — jour d'earnings des top2 companies (col "Date earnings top2")
- is_low_activity  : bool — période de faible activité (vacances, etc.)
- is_fomc          : bool — jour FOMC (Interest Rate Decision US ou FOMC Minutes)
- is_cpi           : bool — jour CPI US (Inflation Rate YoY)
- is_nfp           : bool — jour NFP (Non Farm Payrolls)
- days_to_opex     : int  — jours jusqu'à la prochaine expiration options (0 si jour J)
- days_since_opex  : int  — jours depuis la dernière expiration options
"""

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data" / "live_selected"
CALENDAR_FILE = DATA_DIR / "calendar_events_daily.csv"

# Événements US high impact qui impactent le SPX directement
# Note : le CSV n'a pas de colonne Country, donc on cible des libellés
# unambigus (US-only) : FOMC, NFP, Initial Jobless Claims (US), ISM (US).
# CPI et Interest Rate Decision sont génériques (toutes banques centrales) :
# on les marque mais ils captureront aussi les events EU/JP/etc.
_US_HIGH_EVENTS = {
    "fomc": ["FOMC Minutes", "FOMC Press Conference", "Federal Funds Rate",
             "Fed Interest Rate Decision", "Fed Funds"],
    "cpi": ["Inflation Rate YoY", "Core Inflation Rate YoY", "CPI"],
    "nfp": ["Non Farm Payrolls", "Nonfarm Payrolls", "NFP"],
    "gdp": ["GDP Growth Rate QoQ", "GDP Growth Rate YoY"],
    "pce": ["Core PCE Price Index", "PCE Price Index"],
    "jobs": ["Initial Jobless Claims", "Continuing Jobless Claims"],
    "ism": ["ISM Manufacturing PMI", "ISM Services PMI", "ISM Non-Manufacturing"],
}

_ALL_US_KEY_EVENTS = [e for events in _US_HIGH_EVENTS.values() for e in events]


def _load_raw() -> pd.DataFrame | None:
    """Charge le CSV calendar brut."""
    if CALENDAR_FILE.exists():
        cal_path = CALENDAR_FILE
    else:
        # Fallback : ancien nom CALENDAR_EARNINGS_2021_2026_daily.csv
        alt = DATA_DIR / "CALENDAR_EARNINGS_2021_2026_daily.csv"
        if alt.exists():
            cal_path = alt
        else:
            return None

    try:
        df = pd.read_csv(cal_path, sep=";", encoding="utf-8-sig")
        df.columns = [c.strip() for c in df.columns]
        df["date"] = pd.to_datetime(
            df["Date"].astype(str).str.strip(),
            errors="coerce", utc=False
        ).dt.normalize()
        df = df.dropna(subset=["date"])
        return df
    except Exception as e:
        print(f"[calendar_features] Erreur chargement: {e}", flush=True)
        return None


def build_calendar_features(date_index: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    """
    Construit un DataFrame de features calendar indexé par date.

    Args:
        date_index: si fourni, réindexe sur ces dates (ffill pour jours manquants)
                    si None, retourne toutes les dates disponibles

    Returns:
        DataFrame avec colonnes décrites dans le module docstring.
        Retourne un DataFrame vide si le CSV est absent.
    """
    raw = _load_raw()
    if raw is None:
        print("[calendar_features] CSV absent — features calendar désactivées", flush=True)
        empty = pd.DataFrame()
        if date_index is not None:
            empty = pd.DataFrame(index=date_index)
        return empty

    dates = sorted(raw["date"].unique())
    rows = []

    for date in dates:
        day = raw[raw["date"] == date]

        opex_col = "Options Expiration"
        is_opex = bool(day[opex_col].notna().any()) if opex_col in day.columns else False

        earn_col = "Date earnings top2 companies"
        is_earn = bool(day[earn_col].notna().any()) if earn_col in day.columns else False

        low_col = "Low Activity Period"
        is_low = bool(day[low_col].notna().any()) if low_col in day.columns else False

        n_high = int((day["Impact"] == "high").sum())
        n_medium = int((day["Impact"] == "medium").sum())

        # Pour FOMC/CPI/NFP : restreindre aux événements high impact
        # (le CSV n'a pas de colonne Country, donc on approxime que high impact = US/majeur)
        high_events = day[day["Impact"] == "high"]["Event"].fillna("").str.lower().tolist()

        def _match(keywords):
            return any(k.lower() in ev for k in keywords for ev in high_events)

        is_fomc = _match(_US_HIGH_EVENTS["fomc"])
        is_cpi = _match(_US_HIGH_EVENTS["cpi"])
        is_nfp = _match(_US_HIGH_EVENTS["nfp"])
        is_gdp = _match(_US_HIGH_EVENTS["gdp"])
        is_pce = _match(_US_HIGH_EVENTS["pce"])

        us_high = day[
            (day["Impact"] == "high") &
            day["Event"].fillna("").apply(
                lambda e: any(k.lower() in e.lower() for k in _ALL_US_KEY_EVENTS)
            )
        ]
        if len(us_high) > 0 and us_high["Actual"].notna().any() and us_high["Estimate"].notna().any():
            actual = pd.to_numeric(us_high["Actual"], errors="coerce").fillna(0)
            estimate = pd.to_numeric(us_high["Estimate"], errors="coerce").fillna(0)
            surprise_raw = float((actual - estimate).sum())
        else:
            surprise_raw = np.nan

        rows.append({
            "date": date,
            "is_opex": is_opex,
            "is_high_impact": n_high > 0,
            "n_high_events": n_high,
            "n_medium_events": n_medium,
            "macro_surprise_raw": surprise_raw,
            "is_earnings_top2": is_earn,
            "is_low_activity": is_low,
            "is_fomc": is_fomc,
            "is_cpi": is_cpi,
            "is_nfp": is_nfp,
            "is_gdp": is_gdp,
            "is_pce": is_pce,
        })

    if not rows:
        return pd.DataFrame()

    feat = pd.DataFrame(rows).set_index("date").sort_index()

    valid = feat["macro_surprise_raw"].dropna()
    if len(valid) > 10:
        rolling_std = feat["macro_surprise_raw"].rolling(60, min_periods=5).std()
        rolling_mean = feat["macro_surprise_raw"].rolling(60, min_periods=5).mean()
        feat["macro_surprise"] = (feat["macro_surprise_raw"] - rolling_mean) / rolling_std.replace(0, np.nan)
    else:
        feat["macro_surprise"] = feat["macro_surprise_raw"]
    feat = feat.drop(columns=["macro_surprise_raw"])

    opex_dates = feat.index[feat["is_opex"]].tolist()
    if opex_dates:
        days_to = []
        days_since = []
        for d in feat.index:
            future = [o for o in opex_dates if o >= d]
            past = [o for o in opex_dates if o <= d]
            days_to.append((future[0] - d).days if future else np.nan)
            days_since.append((d - past[-1]).days if past else np.nan)
        feat["days_to_opex"] = days_to
        feat["days_since_opex"] = days_since
    else:
        feat["days_to_opex"] = np.nan
        feat["days_since_opex"] = np.nan

    if date_index is not None:
        feat = feat.reindex(date_index.normalize(), method="ffill")

    return feat


# Cache module-level pour éviter de recharger à chaque appel
_CACHE: pd.DataFrame | None = None


def get_calendar_features(date_index: pd.DatetimeIndex | None = None) -> pd.DataFrame:
    """Version cachée de build_calendar_features."""
    global _CACHE
    if _CACHE is None:
        _CACHE = build_calendar_features()
    if date_index is not None and not _CACHE.empty:
        return _CACHE.reindex(date_index.normalize(), method="ffill")
    return _CACHE if _CACHE is not None else pd.DataFrame()


def get_features_for_date(date) -> dict:
    """Retourne les features pour une date spécifique sous forme de dict."""
    feat = get_calendar_features()
    if feat.empty:
        return {}
    date_norm = pd.Timestamp(date).normalize()
    if date_norm in feat.index:
        row = feat.loc[date_norm]
        return {k: (bool(v) if isinstance(v, (bool, np.bool_)) else
                    (float(v) if pd.notna(v) else None))
                for k, v in row.items()}
    return {}
