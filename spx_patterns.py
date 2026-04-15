# spx_patterns.py — Patterns overnight SPX basés sur VIX, VVIX, SKEW, DXY, gaps

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data" / "live_selected"


def _to_num(s):
    return pd.to_numeric(s.astype(str).str.replace(",", ".").str.replace(r"\s+", "", regex=True), errors="coerce")


def _load(name):
    for fname in [f"{name}_daily.csv", f"{name}.csv"]:
        p = DATA_DIR / fname
        if p.exists():
            df = pd.read_csv(p, sep=";")
            df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
            df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
            df = df.dropna(subset=["time"]).set_index("time").sort_index()
            for col in ("open", "close", "high", "low"):
                if col in df.columns:
                    df[col] = _to_num(df[col])
            return df
    return None


def _build_signals():
    spx = _load("SPX")
    if spx is None:
        return pd.DataFrame()
    df = pd.DataFrame(index=spx.index)
    df["close"] = spx["close"]
    df["open"] = spx.get("open", pd.Series(dtype=float))
    df["overnight"] = (spx["open"].shift(-1) - spx["close"]) / spx["close"] * 100
    df["gap"] = (spx["open"] - spx["close"].shift(1)) / spx["close"].shift(1) * 100
    df["ma20"] = spx["close"].rolling(20).mean()
    df["mom_1m"] = (spx["close"] - spx["close"].shift(21)) / spx["close"].shift(21) * 100
    df["mom_3m"] = (spx["close"] - spx["close"].shift(63)) / spx["close"].shift(63) * 100
    df["mom_6m"] = (spx["close"] - spx["close"].shift(126)) / spx["close"].shift(126) * 100
    df["high_52w"] = spx["close"].rolling(252, min_periods=50).max()
    df["low_52w"] = spx["close"].rolling(252, min_periods=50).min()

    for name, col_name in [("VIX", "close"), ("VVIX", "close"), ("SKEW_INDEX", "close"), ("DXY", "close")]:
        ext = _load(name)
        if ext is not None and col_name in ext.columns:
            df[name.lower()] = _to_num(ext[col_name]).reindex(df.index, method="ffill")

    ratio = _load("VIX1D_VIX_ratio")
    if ratio is not None and "open" in ratio.columns:
        df["vix1d_ratio"] = _to_num(ratio["open"]).reindex(df.index, method="ffill")

    if "vvix" in df.columns:
        df["vvix_p90"] = df["vvix"].rolling(252, min_periods=50).quantile(0.90)
        df["vvix_p10"] = df["vvix"].rolling(252, min_periods=50).quantile(0.10)

    # Charger features calendar
    try:
        from calendar_features import get_calendar_features
        cal_feat = get_calendar_features(df.index)
        if not cal_feat.empty:
            for col in ["is_opex", "is_high_impact", "n_high_events",
                        "is_fomc", "is_cpi", "is_nfp", "is_earnings_top2",
                        "is_low_activity", "days_to_opex", "days_since_opex",
                        "macro_surprise"]:
                if col in cal_feat.columns:
                    df[col] = cal_feat[col].reindex(df.index, method="ffill")
    except Exception as e:
        print(f"[spx_patterns] calendar_features non disponible: {e}", flush=True)

    return df.dropna(subset=["overnight"])


def _test_signal(df, mask, label):
    sub = df[mask].dropna(subset=["overnight"])
    n = len(sub)
    if n < 20:
        return None
    split = int(n * 0.7)
    is_d, oos_d = sub.iloc[:split], sub.iloc[split:]
    taux_is = float((is_d["overnight"] > 0).mean() * 100)
    med = float(sub["overnight"].median())
    if abs(med) < 0.15 or taux_is < 55:
        return None
    taux_oos = float((oos_d["overnight"] > 0).mean() * 100) if len(oos_d) >= 6 else None
    if taux_oos is not None and taux_oos < 48:
        return None
    direction = "hausse" if med > 0 else "baisse"
    active = bool(mask.iloc[-1]) if len(mask) > 0 else False
    return {
        "label": label, "direction": direction,
        "taux_is": round(taux_is, 1),
        "taux_oos": round(taux_oos, 1) if taux_oos is not None else None,
        "n": n, "median_amp": round(med, 3),
        "actionnable": taux_is >= 70 and (taux_oos or 0) >= 55,
        "active_today": active,
    }


_all_patterns = []


def _compute_all():
    global _all_patterns
    df = _build_signals()
    if df.empty:
        return
    signals = {}
    if "vix" in df.columns:
        signals["VIX > 25"] = df["vix"] > 25
        signals["VIX > 20"] = df["vix"] > 20
        signals["VIX < 15"] = df["vix"] < 15
    if "vix1d_ratio" in df.columns and df["vix1d_ratio"].notna().any():
        signals["VIX1D/VIX > 1.20"] = df["vix1d_ratio"] > 1.20
        signals["VIX1D/VIX < 0.85"] = df["vix1d_ratio"] < 0.85
    if "vvix" in df.columns and "vvix_p90" in df.columns:
        signals["VVIX > P90"] = df["vvix"] > df["vvix_p90"]
        signals["VVIX < P10"] = df["vvix"] < df["vvix_p10"]
    if "skew_index" in df.columns:
        signals["SKEW > 140"] = df["skew_index"] > 140
        signals["SKEW < 100"] = df["skew_index"] < 100
    signals["Gap SPX < -1%"] = df["gap"] < -1.0
    signals["Gap SPX > +1%"] = df["gap"] > 1.0
    signals["SPX sous MA20"] = df["close"] < df["ma20"]
    if "mom_1m" in df.columns:
        signals["Momentum 1m positif"] = df["mom_1m"] > 0
        signals["Momentum 1m négatif"] = df["mom_1m"] < 0
    if "mom_3m" in df.columns:
        signals["Momentum 3m positif"] = df["mom_3m"] > 0
    if "mom_6m" in df.columns:
        signals["Momentum 6m positif"] = df["mom_6m"] > 0
    if "high_52w" in df.columns:
        signals["SPX near 52w high (<2%)"] = (df["high_52w"] - df["close"]) / df["high_52w"] < 0.02
    if "low_52w" in df.columns:
        signals["SPX near 52w low (<2%)"] = (df["close"] - df["low_52w"]) / df["low_52w"] < 0.02

    # Calendar / macro signals
    if "is_opex" in df.columns:
        signals["OpEx day"] = df["is_opex"].fillna(False).astype(bool)
        signals["Semaine OpEx (J-2 à J+2)"] = (
            (df.get("days_to_opex", pd.Series(999, index=df.index)) <= 2) |
            (df.get("days_since_opex", pd.Series(999, index=df.index)) <= 2)
        )
    if "is_fomc" in df.columns:
        signals["Jour FOMC"] = df["is_fomc"].fillna(False).astype(bool)
        signals["Veille FOMC"] = df["is_fomc"].fillna(False).shift(-1).fillna(False).astype(bool)
    if "is_cpi" in df.columns:
        signals["Jour CPI"] = df["is_cpi"].fillna(False).astype(bool)
    if "is_nfp" in df.columns:
        signals["Jour NFP"] = df["is_nfp"].fillna(False).astype(bool)
    if "is_high_impact" in df.columns:
        signals["Jour high impact macro"] = df["is_high_impact"].fillna(False).astype(bool)
        signals["Pas de macro high impact"] = ~df["is_high_impact"].fillna(False).astype(bool)
    if "is_earnings_top2" in df.columns:
        signals["Earnings top2 companies"] = df["is_earnings_top2"].fillna(False).astype(bool)
    if "is_low_activity" in df.columns:
        signals["Période faible activité"] = df["is_low_activity"].fillna(False).astype(bool)
    if "macro_surprise" in df.columns:
        signals["Surprise macro positive (z>1)"] = df["macro_surprise"].fillna(0) > 1.0
        signals["Surprise macro négative (z<-1)"] = df["macro_surprise"].fillna(0) < -1.0

    results = []
    for label, mask in signals.items():
        p = _test_signal(df, mask, label)
        if p:
            results.append(p)

    # Combos
    keys = list(signals.keys())
    combo_ct = 0
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            if combo_ct >= 30:
                break
            p = _test_signal(df, signals[keys[i]] & signals[keys[j]], f"{keys[i]} + {keys[j]}")
            if p:
                results.append(p)
                combo_ct += 1

    results.sort(key=lambda x: x["taux_is"], reverse=True)
    _all_patterns = results


try:
    _compute_all()
except Exception as e:
    print(f"[spx_patterns] init error: {e}", flush=True)


def find_active_patterns():
    return [p for p in _all_patterns if p.get("active_today")]


def get_all_patterns():
    return _all_patterns
