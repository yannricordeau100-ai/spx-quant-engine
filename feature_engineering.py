"""
feature_engineering.py — Construction exhaustive de toutes les features
pour le SPX Edge system.

Catégories de features produites :
1. Features brutes daily (toutes colonnes de tous CSV, shift J-1)
2. Features intraday J (OR30, VIX1D/VIX open, barres 5min/30min)
3. Features dérivées intra-CSV (momentum, z-score, percentile, accélération)
4. Features cross-asset (ratios, spreads, corrélations rolling)
5. Features temporelles (jour semaine, semaine OpEx, position dans le mois)
6. Features régime de marché (bull/bear, volatilité réalisée, trending)
"""

import gc
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import time as dtime

try:
    from cross_feature_library import (
        build_all_cross_features, get_priority_scores
    )
    _CROSS_LIB_AVAILABLE = True
except ImportError:
    _CROSS_LIB_AVAILABLE = False

DATA_DIR = Path(__file__).parent / "data" / "live_selected"

# ── Constantes ──────────────────────────────────────────────
ROLLING_WINDOWS = [3, 5, 10, 20, 60]
ZSCORE_WINDOWS  = [20, 60]
PCT_WINDOWS     = [252]
CORR_WINDOWS    = [10, 20]

# CSV à exclure (tickers individuels, options chains)
TICKER_EXCLUDE = {
    "aaoi", "aapl", "nvda", "msft", "tsla", "amzn",
    "googl", "meta", "jpm", "calendar", "earnings",
    "option", "chain",
}

# Assets clés pour les cross-ratios (ordre d'importance)
KEY_ASSETS = ["spx", "vix", "vix9d", "vix3m", "vix6m", "vvix", "skew",
              "vix1d_vix_ratio", "dxy", "gold", "iwm", "qqq", "spy",
              "us_10_years_bonds", "us_bonds_30_days_con",
              "advance_decline_rati", "spx_put_call_ratio",
              "equity_put_call_rati", "vix_put_call_ratio"]


def _load_csv(path: Path) -> pd.DataFrame | None:
    """Charge un CSV daily avec normalisation des colonnes."""
    try:
        df = pd.read_csv(path, sep=";")
        df.columns = [
            c.strip().lower()
             .replace(" ", "_").replace("#", "")
             .replace("-", "_").replace("(", "").replace(")", "")
             .strip("_")
            for c in df.columns
        ]
        tc = next((c for c in ("time", "date", "timestamp") if c in df.columns), None)
        if tc is None:
            return None
        df[tc] = pd.to_datetime(df[tc].astype(str).str.strip(), errors="coerce")
        df = df.dropna(subset=[tc]).set_index(tc).sort_index()
        df.index = df.index.normalize()
        for col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "."), errors="coerce"
            )
        return df
    except Exception as e:
        print(f"[feat_eng] Erreur {path.name}: {e}", flush=True)
        return None


def _load_intraday(symbol: str, freq: str) -> pd.DataFrame | None:
    """Charge un CSV intraday avec conversion Paris→NY."""
    for stem in [f"{symbol}_{freq}", f"{symbol.lower()}_{freq}"]:
        p = DATA_DIR / f"{stem}.csv"
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p, sep=";")
            df.columns = [
                c.strip().lower().replace(" ", "_").replace("#", "").strip()
                for c in df.columns
            ]
            df["time"] = pd.to_datetime(
                df["time"].astype(str).str.strip(), errors="coerce"
            )
            df = df.dropna(subset=["time"]).copy()
            if df["time"].dt.hour.median() >= 13:
                df["time"] = df["time"] - pd.Timedelta(hours=6)
            df = df.set_index("time").sort_index()
            for col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", "."), errors="coerce"
                )
            return df
        except Exception as e:
            print(f"[feat_eng] Erreur intraday {p.name}: {e}", flush=True)
    return None


def load_all_daily() -> dict[str, pd.DataFrame]:
    """Charge tous les CSV daily hors tickers/options."""
    result = {}
    for p in sorted(DATA_DIR.glob("*_daily.csv")):
        stem = p.stem.lower()
        short = stem.replace("_daily", "").replace("_index", "")
        if any(t in short for t in TICKER_EXCLUDE):
            continue
        df = _load_csv(p)
        if df is not None and len(df) >= 50:
            result[short] = df
    print(f"[feat_eng] {len(result)} CSV daily chargés", flush=True)
    return result


# ════════════════════════════════════════════════════════════
# CATÉGORIE 1 — Features brutes daily (J-1)
# ════════════════════════════════════════════════════════════

def build_raw_features(daily_csvs: dict, index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Toutes les colonnes de tous les CSV daily, shift(1) = J-1.
    Préfixe : {asset}_{colonne}
    """
    features = pd.DataFrame(index=index)
    n_feats = 0

    for name, df in daily_csvs.items():
        for col in df.columns:
            series = df[col].shift(1).reindex(index, method="ffill")
            if series.notna().sum() >= 30:
                feat_name = f"{name}_{col}"[:50]
                features[feat_name] = series
                n_feats += 1

    print(f"[feat_eng] Features brutes: {n_feats}", flush=True)
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 2 — Features intraday J (disponibles à l'entrée)
# ════════════════════════════════════════════════════════════

def build_intraday_features(index: pd.DatetimeIndex,
                            entry_point: str = "9h30") -> pd.DataFrame:
    """
    Features du jour J disponibles au moment de l'entrée.

    Pour 9h30 : uniquement données overnight (futures nuit)
    Pour 10h00 : + OR30 (high-low 9h30-10h00) + VIX1D/VIX open J
    Pour 10h30 : + barre 10h00
    """
    features = pd.DataFrame(index=index)

    # ── OR30 : high-low des 30 premières minutes ──────────────
    df_5min = _load_intraday("SPX", "5min")
    if df_5min is None:
        df_5min = _load_intraday("SPY", "5min")

    if df_5min is not None and "high" in df_5min.columns and "low" in df_5min.columns:
        for date in index:
            morning = df_5min[
                (df_5min.index.date == date.date()) &
                (df_5min.index.time >= dtime(9, 30)) &
                (df_5min.index.time < dtime(10, 0))
            ]
            if len(morning) >= 2:
                or30_high = float(morning["high"].max())
                or30_low = float(morning["low"].min())
                or30 = or30_high - or30_low
                open_j = float(morning.iloc[0].get("open", morning["high"].iloc[0]))

                features.loc[date, "or30_points"] = or30
                features.loc[date, "or30_pct"] = or30 / open_j * 100 if open_j > 0 else np.nan
                features.loc[date, "or30_high"] = or30_high
                features.loc[date, "or30_low"] = or30_low
                features.loc[date, "or30_open"] = open_j
                close_930 = float(morning.iloc[-1].get("close", open_j))
                features.loc[date, "or30_direction"] = 1.0 if close_930 > open_j else -1.0
                features.loc[date, "or30_ret_pct"] = (close_930 - open_j) / open_j * 100 if open_j > 0 else np.nan

        if "or30_points" in features.columns:
            print(f"[feat_eng] OR30: {features['or30_points'].notna().sum()} jours calculés", flush=True)
        gc.collect()

    # ── VIX open J et VIX1D/VIX open J (exact) ───────────────
    vix_daily = None
    for fname in DATA_DIR.glob("vix*.csv"):
        if "daily" in fname.stem.lower() and "put" not in fname.stem.lower() and "spx" not in fname.stem.lower() and "ratio" not in fname.stem.lower() and fname.stem.lower() in ("vix_daily",):
            vix_daily = _load_csv(fname)
            break
    if vix_daily is None:
        p = DATA_DIR / "VIX_daily.csv"
        if p.exists():
            vix_daily = _load_csv(p)

    if vix_daily is not None and "open" in vix_daily.columns:
        vix_open_j = vix_daily["open"].reindex(index, method="ffill")
        features["vix_open_j"] = vix_open_j

    vix1d_daily = None
    p = DATA_DIR / "VIX1D_VIX_ratio_daily.csv"
    if p.exists():
        vix1d_daily = _load_csv(p)

    if vix1d_daily is not None and "open" in vix1d_daily.columns:
        vix1d_open_j = vix1d_daily["open"].reindex(index, method="ffill")
        features["vix1d_vix_ratio_open_j"] = vix1d_open_j

    # ── Barre 10h00 (pour 10h30 seulement) ───────────────────
    if entry_point == "10h30":
        df_30min = _load_intraday("SPY", "30min")
        if df_30min is not None:
            for date in index:
                bar = df_30min[
                    (df_30min.index.date == date.date()) &
                    (df_30min.index.time == dtime(10, 0))
                ]
                if not bar.empty:
                    for col in ["open", "close", "high", "low", "volume", "rsi"]:
                        if col in bar.columns:
                            features.loc[date, f"bar_1000_{col}"] = float(bar.iloc[0][col])
                    if "open" in bar.columns and "close" in bar.columns:
                        o = float(bar.iloc[0]["open"])
                        c = float(bar.iloc[0]["close"])
                        if o > 0:
                            features.loc[date, "bar_1000_ret_pct"] = (c - o) / o * 100
            gc.collect()

    # ── Overnight futures ─────────────────────────────────────
    df_fut = _load_intraday("SPX_FUTURE", "30min")
    if df_fut is not None:
        for date in index:
            prev = date - pd.Timedelta(days=3)
            night = df_fut[
                ((df_fut.index.date >= prev.date()) &
                 (df_fut.index.date <= date.date())) &
                ((df_fut.index.time > dtime(16, 0)) |
                 (df_fut.index.time < dtime(9, 30)))
            ]
            if len(night) >= 2:
                o = float(night.iloc[0].get("open", night.iloc[0].get("close", np.nan)))
                c = float(night.iloc[-1].get("close", np.nan))
                if not np.isnan(o) and not np.isnan(c) and o > 0:
                    features.loc[date, "fut_overnight_ret_pct"] = (c - o) / o * 100
                if "high" in night.columns and "low" in night.columns:
                    features.loc[date, "fut_overnight_range_pct"] = (
                        float(night["high"].max()) - float(night["low"].min())
                    ) / max(float(night.iloc[0].get("close", 1)), 0.01) * 100
                if "volume" in night.columns:
                    features.loc[date, "fut_overnight_volume"] = float(night["volume"].sum())
                if "rsi" in night.columns and night["rsi"].notna().any():
                    features.loc[date, "fut_overnight_rsi"] = float(night["rsi"].iloc[-1])
        gc.collect()

    n_intra = features.shape[1]
    print(f"[feat_eng] Features intraday J: {n_intra}", flush=True)
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 3 — Features dérivées intra-CSV
# ════════════════════════════════════════════════════════════

def build_derived_features(daily_csvs: dict,
                           index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Pour chaque série importante : momentum, z-score, percentile,
    accélération (delta du delta), retournement.
    """
    features = pd.DataFrame(index=index)
    n = 0

    priority = {k: v for k, v in daily_csvs.items()
                if any(k.startswith(a) for a in KEY_ASSETS)}

    for name, df in priority.items():
        for col in ["close", "open", "high", "low", "rsi", "volume"]:
            if col not in df.columns:
                continue

            raw = df[col].shift(1).reindex(index, method="ffill")
            if raw.notna().sum() < 50:
                continue

            prefix = f"{name}_{col}"

            for w in [1, 3, 5, 10, 20]:
                mom = raw.pct_change(w) * 100
                if mom.notna().sum() >= 20:
                    features[f"{prefix}_mom{w}d"] = mom
                    n += 1

            for w in ZSCORE_WINDOWS:
                mu = raw.rolling(w, min_periods=w // 2).mean()
                sigma = raw.rolling(w, min_periods=w // 2).std()
                z = (raw - mu) / sigma.replace(0, np.nan)
                if z.notna().sum() >= 20:
                    features[f"{prefix}_z{w}"] = z
                    n += 1

            roll_pct = raw.rolling(252, min_periods=60).rank(pct=True) * 100
            if roll_pct.notna().sum() >= 20:
                features[f"{prefix}_pct252"] = roll_pct
                n += 1

            mom5 = raw.pct_change(5) * 100
            accel = mom5 - mom5.shift(5)
            if accel.notna().sum() >= 20:
                features[f"{prefix}_accel5"] = accel
                n += 1

            if col == "close":
                for w in [20, 50]:
                    ma = raw.rolling(w, min_periods=w // 2).mean()
                    dist = (raw - ma) / ma.replace(0, np.nan) * 100
                    if dist.notna().sum() >= 20:
                        features[f"{prefix}_distma{w}"] = dist
                        n += 1

                ret = raw.pct_change() * 100
                for w in [5, 10, 20]:
                    vol = ret.rolling(w, min_periods=w // 2).std()
                    if vol.notna().sum() >= 20:
                        features[f"{prefix}_vol{w}"] = vol
                        n += 1

    print(f"[feat_eng] Features dérivées intra-CSV: {n}", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 4 — Features cross-asset
# ════════════════════════════════════════════════════════════

def build_cross_features(daily_csvs: dict,
                         index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Ratios, spreads et corrélations entre assets.
    """
    features = pd.DataFrame(index=index)
    n = 0

    closes = {}
    for asset in KEY_ASSETS:
        for k, df in daily_csvs.items():
            if k.startswith(asset) and "close" in df.columns:
                s = df["close"].shift(1).reindex(index, method="ffill")
                if s.notna().sum() >= 50:
                    closes[asset] = s
                    break

    # ── Ratios A/B ──────────────────────────────────────────
    ratio_pairs = [
        ("vix", "vix3m"),
        ("vix9d", "vix"),
        ("vix9d", "vix3m"),
        ("vix", "vix6m"),
        ("vvix", "vix"),
        ("vix1d_vix_ratio", "vix"),
        ("skew", "vix"),
        ("dxy", "gold"),
        ("spx", "qqq"),
        ("spx", "iwm"),
        ("spx_put_call_ratio", "equity_put_call_rati"),
        ("vix_put_call_ratio", "spx_put_call_ratio"),
    ]

    for a, b in ratio_pairs:
        if a in closes and b in closes:
            ratio = closes[a] / closes[b].replace(0, np.nan)
            if ratio.notna().sum() >= 30:
                features[f"ratio_{a[:8]}_{b[:8]}"] = ratio
                z = (ratio - ratio.rolling(20).mean()) / ratio.rolling(20).std().replace(0, np.nan)
                features[f"ratio_{a[:8]}_{b[:8]}_z20"] = z
                n += 2

    # ── Spreads ──────────────────────────────────────────────
    spread_pairs = [
        ("vix9d", "vix"),
        ("vix3m", "vix"),
        ("vix6m", "vix3m"),
    ]

    for a, b in spread_pairs:
        if a in closes and b in closes:
            spread = closes[a] - closes[b]
            if spread.notna().sum() >= 30:
                features[f"spread_{a[:6]}_{b[:6]}"] = spread
                spread_z = (spread - spread.rolling(20).mean()) / spread.rolling(20).std().replace(0, np.nan)
                features[f"spread_{a[:6]}_{b[:6]}_z"] = spread_z
                n += 2

    # ── Corrélations rolling ────────────────────────────────
    corr_pairs = [
        ("spx", "gold"),
        ("spx", "dxy"),
        ("spx", "us_10_years_bonds"),
        ("vix", "spx"),
        ("gold", "dxy"),
    ]

    for a, b in corr_pairs:
        if a in closes and b in closes:
            ret_a = closes[a].pct_change()
            ret_b = closes[b].pct_change()
            for w in [10, 20]:
                corr = ret_a.rolling(w).corr(ret_b)
                if corr.notna().sum() >= 20:
                    features[f"corr_{a[:5]}_{b[:5]}_{w}d"] = corr
                    n += 1

    # ── Produits (interactions) ──────────────────────────────
    interaction_pairs = [
        ("vix", "skew"),
        ("vvix", "vix9d"),
    ]

    for a, b in interaction_pairs:
        if a in closes and b in closes:
            prod = closes[a] * closes[b]
            if prod.notna().sum() >= 30:
                features[f"interact_{a[:6]}_{b[:6]}"] = prod
                prod_z = (prod - prod.rolling(20).mean()) / prod.rolling(20).std().replace(0, np.nan)
                features[f"interact_{a[:6]}_{b[:6]}_z"] = prod_z
                n += 2

    # ── Performance relative cross-asset ────────────────────
    if "spx" in closes:
        spx_ret5 = closes["spx"].pct_change(5) * 100
        for peer in ["iwm", "qqq"]:
            if peer in closes:
                peer_ret5 = closes[peer].pct_change(5) * 100
                div = spx_ret5 - peer_ret5
                if div.notna().sum() >= 20:
                    features[f"spx_vs_{peer}_5d"] = div
                    n += 1

    print(f"[feat_eng] Features cross-asset: {n}", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 5 — Features temporelles et régimes
# ════════════════════════════════════════════════════════════

def build_temporal_features(daily_csvs: dict,
                            index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Features temporelles et régimes de marché.
    """
    features = pd.DataFrame(index=index)

    features["day_of_week"] = index.dayofweek
    features["month"] = index.month
    features["week_of_year"] = index.isocalendar().week.values.astype(float)
    features["day_of_month"] = index.day
    features["is_monday"] = (index.dayofweek == 0).astype(float)
    features["is_friday"] = (index.dayofweek == 4).astype(float)
    features["is_month_start"] = (index.day <= 5).astype(float)
    features["is_month_end"] = (index.day >= 25).astype(float)

    try:
        from calendar_features import get_calendar_features
        cal = get_calendar_features(index)
        if not cal.empty:
            for col in cal.columns:
                features[f"cal_{col}"] = cal[col].reindex(index, method="ffill")
    except Exception:
        pass

    spx_df = daily_csvs.get("spx", daily_csvs.get("spy", pd.DataFrame()))
    if "close" in spx_df.columns:
        spx = spx_df["close"].shift(1).reindex(index, method="ffill")

        for w in [5, 20, 60]:
            ma = spx.rolling(w, min_periods=w // 2).mean()
            features[f"spx_above_ma{w}"] = (spx > ma).astype(float)
            features[f"spx_dist_ma{w}_pct"] = (spx - ma) / ma.replace(0, np.nan) * 100

        ret = spx.pct_change()
        bull = (ret > 0).astype(int)
        streak = bull.groupby((bull != bull.shift()).cumsum()).cumcount() + 1
        streak_signed = streak * (bull * 2 - 1)
        features["spx_streak"] = streak_signed

        for w in [5, 20]:
            vol = ret.rolling(w, min_periods=w // 2).std() * 100
            features[f"spx_rvol_{w}d"] = vol

    vix_df = daily_csvs.get("vix", pd.DataFrame())
    if "close" in vix_df.columns:
        vix = vix_df["close"].shift(1).reindex(index, method="ffill")
        features["vix_regime_low"] = (vix < 15).astype(float)
        features["vix_regime_normal"] = ((vix >= 15) & (vix < 20)).astype(float)
        features["vix_regime_high"] = ((vix >= 20) & (vix < 30)).astype(float)
        features["vix_regime_crisis"] = (vix >= 30).astype(float)
        features["vix_mom1d"] = vix.pct_change(1) * 100
        features["vix_mom5d"] = vix.pct_change(5) * 100
        features["vix_spike"] = (vix > vix.shift(1) * 1.10).astype(float)
        features["vix_crush"] = (vix < vix.shift(1) * 0.90).astype(float)

    n = features.shape[1]
    print(f"[feat_eng] Features temporelles/régimes: {n}", flush=True)
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 6 — Lags temporels intra-CSV
# ════════════════════════════════════════════════════════════

def build_lag_features(daily_csvs: dict,
                       index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Catégorie 6 — Features temporelles intra-CSV.

    Pour chaque asset clé : lags explicites J-2/3/5/10, séquences,
    croisements MA, breakouts, et interactions cross-temporelles.
    """
    features = pd.DataFrame(index=index)
    n = 0

    lag_assets = {k: v for k, v in daily_csvs.items()
                  if any(k.startswith(a) for a in [
                      "vix", "vix9d", "vix3m", "vix6m", "vvix", "skew",
                      "spx", "spy", "qqq", "iwm", "dxy", "gold",
                      "advance_decline", "spx_put_call", "equity_put_call",
                      "vix_put_call", "us_10_years", "us_bonds_30"
                  ])}

    for name, df in lag_assets.items():
        for col in ["close", "rsi", "volume"]:
            if col not in df.columns:
                continue
            raw = df[col].shift(1).reindex(index, method="ffill")
            if raw.notna().sum() < 50:
                continue
            prefix = f"{name}_{col}"

            # Lags J-2 à J-10
            for lag in [2, 3, 5, 10]:
                lagged = df[col].shift(lag).reindex(index, method="ffill")
                if lagged.notna().sum() >= 30:
                    features[f"{prefix}_lag{lag}"] = lagged
                    n += 1

            # Différences entre lags
            for lag_a, lag_b in [(1, 3), (1, 5), (3, 10), (5, 10)]:
                s_a = df[col].shift(lag_a).reindex(index, method="ffill")
                s_b = df[col].shift(lag_b).reindex(index, method="ffill")
                diff = s_a - s_b
                if diff.notna().sum() >= 30 and col == "close":
                    features[f"{prefix}_diff_{lag_a}_{lag_b}"] = diff
                    n += 1

            # Streaks
            if col == "close":
                ret = raw.pct_change()
                direction = (ret > 0).astype(int)
                for streak_len in [2, 3, 5]:
                    roll_bull = direction.rolling(streak_len).sum()
                    roll_bear = (1 - direction).rolling(streak_len).sum()
                    features[f"{prefix}_streak_bull{streak_len}"] = (roll_bull == streak_len).astype(float)
                    features[f"{prefix}_streak_bear{streak_len}"] = (roll_bear == streak_len).astype(float)
                    n += 2

            # Croisements MA
            if col == "close":
                ma5 = raw.rolling(5, min_periods=3).mean()
                ma20 = raw.rolling(20, min_periods=10).mean()
                above_ma5_now = (raw > ma5).astype(int)
                above_ma5_prev = (raw.shift(1) > ma5.shift(1)).astype(int)
                features[f"{prefix}_cross_ma5_up"] = ((above_ma5_now == 1) & (above_ma5_prev == 0)).astype(float)
                features[f"{prefix}_cross_ma5_down"] = ((above_ma5_now == 0) & (above_ma5_prev == 1)).astype(float)
                above_ma20_now = (raw > ma20).astype(int)
                above_ma20_prev = (raw.shift(1) > ma20.shift(1)).astype(int)
                features[f"{prefix}_cross_ma20_up"] = ((above_ma20_now == 1) & (above_ma20_prev == 0)).astype(float)
                features[f"{prefix}_cross_ma20_down"] = ((above_ma20_now == 0) & (above_ma20_prev == 1)).astype(float)
                n += 4

                # Breakouts
                for w in [5, 10, 20]:
                    roll_max = raw.rolling(w, min_periods=w // 2).max()
                    roll_min = raw.rolling(w, min_periods=w // 2).min()
                    features[f"{prefix}_breakout_up{w}"] = (raw >= roll_max).astype(float)
                    features[f"{prefix}_breakout_down{w}"] = (raw <= roll_min).astype(float)
                    n += 2

    # Cross-temporel
    vix_close = daily_csvs.get("vix", pd.DataFrame()).get("close")
    spx_close = daily_csvs.get("spx", daily_csvs.get("spy", pd.DataFrame())).get("close")

    if vix_close is not None and spx_close is not None:
        vix_j1 = vix_close.shift(1).reindex(index, method="ffill")
        spx_m5 = spx_close.pct_change(5).shift(1).reindex(index, method="ffill") * 100
        spx_m10 = spx_close.pct_change(10).shift(1).reindex(index, method="ffill") * 100

        features["vix_j1_x_spx_m5"] = vix_j1 * spx_m5
        features["vix_j1_x_spx_m10"] = vix_j1 * spx_m10
        n += 2

        vix_up3 = (vix_close.shift(1) > vix_close.shift(4)).reindex(index, method="ffill").astype(float)
        spx_dn3 = (spx_close.shift(1) < spx_close.shift(4)).reindex(index, method="ffill").astype(float)
        features["vix_up3_and_spx_dn3"] = vix_up3 * spx_dn3
        n += 1

    skew_close = daily_csvs.get("skew", pd.DataFrame()).get("close")
    if skew_close is not None and vix_close is not None:
        skew_j1 = skew_close.shift(1).reindex(index, method="ffill")
        vix_m3 = vix_close.pct_change(3).shift(1).reindex(index, method="ffill") * 100
        features["skew_j1_x_vix_m3"] = skew_j1 * vix_m3
        n += 1

    print(f"[feat_eng] Features lags/temporel intra-CSV: {n}", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 7 — Microstructure / bougies
# ════════════════════════════════════════════════════════════

def build_microstructure_features(daily_csvs: dict,
                                  index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Catégorie 7 — Microstructure et signaux intra-journaliers.
    Body/shadow, gap, true range, ATR, position du close, séquences.
    """
    features = pd.DataFrame(index=index)
    n = 0

    priority = {k: v for k, v in daily_csvs.items()
                if any(k.startswith(a) for a in KEY_ASSETS)
                and all(c in v.columns for c in ["open", "high", "low", "close"])}

    for name, df in priority.items():
        o = df["open"].shift(1).reindex(index, method="ffill")
        h = df["high"].shift(1).reindex(index, method="ffill")
        l = df["low"].shift(1).reindex(index, method="ffill")
        c = df["close"].shift(1).reindex(index, method="ffill")
        c_prev = df["close"].shift(2).reindex(index, method="ffill")

        prefix = f"{name}_micro"

        body = (c - o).abs() / o.replace(0, np.nan) * 100
        features[f"{prefix}_body_pct"] = body
        n += 1

        features[f"{prefix}_body_dir"] = np.sign(c - o)
        n += 1

        upper_shadow = (h - c.where(c > o, o)) / h.replace(0, np.nan) * 100
        lower_shadow = (c.where(c < o, o) - l) / c.where(c < o, o).replace(0, np.nan) * 100
        features[f"{prefix}_upper_shadow"] = upper_shadow.clip(0)
        features[f"{prefix}_lower_shadow"] = lower_shadow.clip(0)
        n += 2

        range_hl = (h - l).replace(0, np.nan)
        features[f"{prefix}_close_position"] = (c - l) / range_hl
        n += 1

        gap = (o - c_prev) / c_prev.replace(0, np.nan) * 100
        features[f"{prefix}_gap_pct"] = gap
        features[f"{prefix}_gap_up"] = (gap > 0.1).astype(float)
        features[f"{prefix}_gap_down"] = (gap < -0.1).astype(float)
        n += 3

        tr1 = h - l
        tr2 = (h - c_prev).abs()
        tr3 = (l - c_prev).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        features[f"{prefix}_true_range"] = tr / c_prev.replace(0, np.nan) * 100
        n += 1

        for atr_w in [5, 14]:
            atr = tr.rolling(atr_w, min_periods=atr_w // 2).mean()
            features[f"{prefix}_atr{atr_w}"] = atr / c_prev.replace(0, np.nan) * 100
            n += 1

        features[f"{prefix}_body_ratio"] = (c - o).abs() / range_hl
        features[f"{prefix}_is_doji"] = ((c - o).abs() / range_hl < 0.1).astype(float)
        n += 2

        if "volume" in df.columns:
            vol = df["volume"].shift(1).reindex(index, method="ffill")
            features[f"{prefix}_effort_result"] = range_hl * vol / 1e6
            n += 1

        body_dir = np.sign(c - o)
        for seq_len in [2, 3]:
            roll_bull = (body_dir > 0).astype(int).rolling(seq_len).sum()
            roll_bear = (body_dir < 0).astype(int).rolling(seq_len).sum()
            features[f"{prefix}_bull_candles{seq_len}"] = (roll_bull == seq_len).astype(float)
            features[f"{prefix}_bear_candles{seq_len}"] = (roll_bear == seq_len).astype(float)
            n += 2

    print(f"[feat_eng] Features microstructure: {n}", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 8 — Transitions de régime
# ════════════════════════════════════════════════════════════

def build_regime_transition_features(daily_csvs: dict,
                                     index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Catégorie 8 — Détection de transitions de régime.
    Croisements VIX/SPX, compression vol, drawdown ATH, breadth.
    """
    features = pd.DataFrame(index=index)
    n = 0

    vix_df = daily_csvs.get("vix", pd.DataFrame())
    spx_df = daily_csvs.get("spx", daily_csvs.get("spy", pd.DataFrame()))
    vix9d_df = daily_csvs.get("vix9d", pd.DataFrame())
    vvix_df = daily_csvs.get("vvix", pd.DataFrame())
    skew_df = daily_csvs.get("skew", pd.DataFrame())
    ad_df = daily_csvs.get("advance_decline_rati", pd.DataFrame())

    # ── VIX ──
    if "close" in vix_df.columns:
        vix = vix_df["close"].shift(1).reindex(index, method="ffill")
        vix_prev = vix_df["close"].shift(2).reindex(index, method="ffill")

        for level in [15, 18, 20, 25, 30]:
            features[f"vix_cross_above_{level}"] = (
                (vix >= level) & (vix_prev < level)
            ).astype(float)
            features[f"vix_cross_below_{level}"] = (
                (vix < level) & (vix_prev >= level)
            ).astype(float)
            n += 2

        vix_vol5 = vix.rolling(5).std()
        vix_vol20 = vix.rolling(20).std()
        features["vix_compression"] = (vix_vol5 < vix_vol20 * 0.5).astype(float)
        features["vix_expansion"] = (vix_vol5 > vix_vol20 * 1.5).astype(float)
        n += 2

        if "open" in vix_df.columns and "high" in vix_df.columns:
            vix_open = vix_df["open"].shift(1).reindex(index, method="ffill")
            vix_high = vix_df["high"].shift(1).reindex(index, method="ffill")
            features["vix_spike_day"] = (
                (vix_high - vix_open) / vix_open.replace(0, np.nan) > 0.05
            ).astype(float)
            n += 1

        vix_ma20 = vix.rolling(20).mean()
        vix_std20 = vix.rolling(20).std()
        features["vix_zscore_20"] = (vix - vix_ma20) / vix_std20.replace(0, np.nan)
        features["vix_extreme_high"] = (features["vix_zscore_20"] > 2.0).astype(float)
        features["vix_extreme_low"] = (features["vix_zscore_20"] < -1.5).astype(float)
        n += 3

    # ── SPX ──
    if "close" in spx_df.columns:
        spx = spx_df["close"].shift(1).reindex(index, method="ffill")

        ath = spx.rolling(252, min_periods=60).max()
        features["spx_dist_ath_pct"] = (spx - ath) / ath.replace(0, np.nan) * 100
        features["spx_in_drawdown_5pct"] = (features["spx_dist_ath_pct"] < -5).astype(float)
        features["spx_in_drawdown_10pct"] = (features["spx_dist_ath_pct"] < -10).astype(float)
        n += 3

        dd = features["spx_dist_ath_pct"]
        features["spx_recovering"] = ((dd > dd.shift(5)) & (dd < -3)).astype(float)
        n += 1

        for w, threshold in [(5, 2.0), (20, 5.0), (60, 10.0)]:
            mom = spx.pct_change(w) * 100
            features[f"spx_strong_bull_{w}d"] = (mom > threshold).astype(float)
            features[f"spx_strong_bear_{w}d"] = (mom < -threshold).astype(float)
            n += 2

    # ── VIX term structure ──
    if "close" in vix9d_df.columns and "close" in vix_df.columns:
        vix9d = vix9d_df["close"].shift(1).reindex(index, method="ffill")
        vix = vix_df["close"].shift(1).reindex(index, method="ffill")
        spread = vix9d - vix

        features["vix_ts_inverted"] = (spread > 0).astype(float)
        features["vix_ts_deeply_inverted"] = (spread > 2).astype(float)

        spread_prev = spread.shift(1)
        features["vix_ts_inversion_new"] = (
            (spread > 0) & (spread_prev <= 0)
        ).astype(float)
        features["vix_ts_normalization_new"] = (
            (spread <= 0) & (spread_prev > 0)
        ).astype(float)
        n += 4

        spread_accel = spread - spread.shift(3)
        features["vix_ts_spread_accel"] = spread_accel
        n += 1

    # ── VVIX ──
    if "close" in vvix_df.columns:
        vvix = vvix_df["close"].shift(1).reindex(index, method="ffill")
        vvix_ma = vvix.rolling(20).mean()
        features["vvix_above_ma20"] = (vvix > vvix_ma).astype(float)
        vvix_prev = vvix_df["close"].shift(2).reindex(index, method="ffill")
        features["vvix_cross_above_ma20"] = (
            (vvix > vvix_ma) & (vvix_prev <= vvix_ma.shift(1))
        ).astype(float)
        n += 2

        features["vvix_spike_3d"] = (
            vvix > vvix.rolling(20).mean() + 2 * vvix.rolling(20).std()
        ).astype(float)
        n += 1

    # ── SKEW ──
    if "close" in skew_df.columns:
        skew = skew_df["close"].shift(1).reindex(index, method="ffill")
        features["skew_above_130"] = (skew > 130).astype(float)
        features["skew_above_140"] = (skew > 140).astype(float)
        features["skew_extreme"] = (skew > 150).astype(float)
        skew_ma20 = skew.rolling(20).mean()
        features["skew_above_ma20"] = (skew > skew_ma20).astype(float)
        skew_prev = skew_df["close"].shift(2).reindex(index, method="ffill")
        features["skew_cross_130_up"] = ((skew > 130) & (skew_prev <= 130)).astype(float)
        features["skew_cross_130_down"] = ((skew < 130) & (skew_prev >= 130)).astype(float)
        n += 6

    # ── Breadth ──
    if "close" in ad_df.columns:
        ad = ad_df["close"].shift(1).reindex(index, method="ffill")
        ad_ma10 = ad.rolling(10).mean()
        features["ad_above_ma10"] = (ad > ad_ma10).astype(float)
        features["ad_cross_ma10_up"] = (
            (ad > ad_ma10) & (ad.shift(1) <= ad_ma10.shift(1))
        ).astype(float)
        features["ad_cross_ma10_down"] = (
            (ad < ad_ma10) & (ad.shift(1) >= ad_ma10.shift(1))
        ).astype(float)
        ad_mom5 = ad.pct_change(5) * 100
        features["ad_strong_bull_5d"] = (ad_mom5 > 5).astype(float)
        features["ad_strong_bear_5d"] = (ad_mom5 < -5).astype(float)
        n += 5

    print(f"[feat_eng] Features transitions de régime: {n}", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 9 — Sentiment options
# ════════════════════════════════════════════════════════════

def build_options_sentiment_features(daily_csvs: dict,
                                     index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Catégorie 9 — Sentiment options et put/call avancé.
    """
    features = pd.DataFrame(index=index)
    n = 0

    pc_assets = {
        "spx_pc": "spx_put_call_ratio",
        "vix_pc": "vix_put_call_ratio",
        "equity_pc": "equity_put_call_rati",
        "iwm_pc": "iwm_put_call_ratio",
        "qqq_pc": "qqq_put_call_ratio",
    }

    series_cache = {}
    for alias, asset in pc_assets.items():
        for k, df in daily_csvs.items():
            if k.startswith(asset) and "close" in df.columns:
                s = df["close"].shift(1).reindex(index, method="ffill")
                if s.notna().sum() >= 50:
                    series_cache[alias] = s
                    break

    for alias, s in series_cache.items():
        pct10 = s.rolling(252, min_periods=60).quantile(0.10)
        pct90 = s.rolling(252, min_periods=60).quantile(0.90)
        features[f"{alias}_extreme_fear"] = (s > pct90).astype(float)
        features[f"{alias}_extreme_greed"] = (s < pct10).astype(float)
        n += 2

        for w in [1, 3, 5, 10]:
            features[f"{alias}_mom{w}d"] = s.pct_change(w) * 100
            n += 1

        features[f"{alias}_z20"] = (
            (s - s.rolling(20).mean()) / s.rolling(20).std().replace(0, np.nan)
        )
        features[f"{alias}_z60"] = (
            (s - s.rolling(60).mean()) / s.rolling(60).std().replace(0, np.nan)
        )
        n += 2

        features[f"{alias}_trending_fear"] = (
            s > s.rolling(5).mean()
        ).astype(float)
        n += 1

    if "spx_pc" in series_cache and "equity_pc" in series_cache:
        ratio = series_cache["spx_pc"] / series_cache["equity_pc"].replace(0, np.nan)
        features["spx_vs_equity_pc_ratio"] = ratio
        features["spx_vs_equity_pc_z20"] = (
            (ratio - ratio.rolling(20).mean()) / ratio.rolling(20).std().replace(0, np.nan)
        )
        n += 2

    if "vix_pc" in series_cache and "spx_pc" in series_cache:
        ratio2 = series_cache["vix_pc"] / series_cache["spx_pc"].replace(0, np.nan)
        features["vix_vs_spx_pc_ratio"] = ratio2
        n += 1

    if len(series_cache) >= 3:
        normalized = pd.DataFrame({
            alias: (s - s.rolling(60).mean()) / s.rolling(60).std().replace(0, np.nan)
            for alias, s in series_cache.items()
        })
        features["composite_fear_index"] = normalized.mean(axis=1)
        features["composite_fear_extreme"] = (
            features["composite_fear_index"] > 1.5
        ).astype(float)
        features["composite_greed_extreme"] = (
            features["composite_fear_index"] < -1.5
        ).astype(float)
        n += 3

    spx_df = daily_csvs.get("spx", pd.DataFrame())
    for col in ["iv_rank", "iv_percentile", "williams_vix_fix"]:
        candidates = [col, col.replace("_", " ")]
        for c in candidates:
            if c in spx_df.columns:
                s = spx_df[c].shift(1).reindex(index, method="ffill")
                if s.notna().sum() >= 30:
                    features[f"spx_{col}_level"] = s
                    features[f"spx_{col}_high"] = (
                        s > s.rolling(252, min_periods=60).quantile(0.80)
                    ).astype(float)
                    n += 2
                    break

    print(f"[feat_eng] Features sentiment options: {n}", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 10 — Inter-marchés
# ════════════════════════════════════════════════════════════

def build_intermarket_features(daily_csvs: dict,
                               index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Catégorie 10 — Relations inter-marchés avancées.
    Lead-lag, risk-on/off, rotation, yield curve.
    """
    features = pd.DataFrame(index=index)
    n = 0

    def _get_close(name: str):
        for k, df in daily_csvs.items():
            if k.startswith(name) and "close" in df.columns:
                s = df["close"].shift(1).reindex(index, method="ffill")
                return s if s.notna().sum() >= 50 else None
        return None

    def _get_ret(name: str, periods: int = 1):
        c = _get_close(name)
        return c.pct_change(periods) * 100 if c is not None else None

    spx_ret1 = _get_ret("spx", 1)
    spx_ret5 = _get_ret("spx", 5)
    nikkei_ret1 = _get_ret("nikkei225", 1)
    dax_ret1 = _get_ret("dax40", 1)
    gold_ret1 = _get_ret("gold", 1)
    dxy_ret1 = _get_ret("dxy", 1)
    bonds10_ret1 = _get_ret("us_10_years_bonds", 1)
    bonds30_ret1 = _get_ret("us_bonds_30_days_con", 1)
    iwm_ret1 = _get_ret("iwm", 1)
    qqq_ret1 = _get_ret("qqq", 1)

    if nikkei_ret1 is not None and spx_ret1 is not None:
        features["nikkei_lead_spx"] = nikkei_ret1
        features["nikkei_vs_spx_diverge"] = nikkei_ret1 - spx_ret1
        features["nikkei_strong_bull"] = (nikkei_ret1 > 1.5).astype(float)
        features["nikkei_strong_bear"] = (nikkei_ret1 < -1.5).astype(float)
        n += 4

    if dax_ret1 is not None and spx_ret1 is not None:
        features["dax_lead_spx"] = dax_ret1
        features["dax_vs_spx_diverge"] = dax_ret1 - spx_ret1
        n += 2

    risk_on_signals = []
    if spx_ret1 is not None:
        risk_on_signals.append((spx_ret1 > 0).astype(float))
    if gold_ret1 is not None:
        risk_on_signals.append((gold_ret1 < 0).astype(float))
        features["gold_safe_haven"] = (gold_ret1 > 1.0).astype(float)
        n += 1
    if dxy_ret1 is not None:
        risk_on_signals.append((dxy_ret1 < 0).astype(float))
        features["dxy_safe_haven"] = (dxy_ret1 > 0.5).astype(float)
        n += 1
    if bonds10_ret1 is not None:
        risk_on_signals.append((bonds10_ret1 > 0).astype(float))

    if len(risk_on_signals) >= 3:
        composite = pd.concat(risk_on_signals, axis=1).mean(axis=1)
        features["risk_on_composite"] = composite
        features["risk_on_strong"] = (composite > 0.75).astype(float)
        features["risk_off_strong"] = (composite < 0.25).astype(float)
        n += 3

    if iwm_ret1 is not None and qqq_ret1 is not None:
        features["small_vs_large_rotation"] = iwm_ret1 - qqq_ret1
        features["risk_appetite_rotation"] = (iwm_ret1 > qqq_ret1).astype(float)
        n += 2

    if spx_ret1 is not None and qqq_ret1 is not None:
        features["tech_vs_market"] = qqq_ret1 - spx_ret1
        n += 1

    if bonds10_ret1 is not None and spx_ret1 is not None:
        features["bonds_vs_spx_rotate"] = bonds10_ret1 - spx_ret1
        features["bonds_up_spx_down"] = (
            (bonds10_ret1 > 0) & (spx_ret1 < 0)
        ).astype(float)
        n += 2

    if spx_ret1 is not None and gold_ret1 is not None:
        for w in [5, 10, 20]:
            corr = spx_ret1.rolling(w).corr(gold_ret1)
            features[f"spx_gold_corr_{w}d"] = corr
            features[f"spx_gold_anticorr_{w}d"] = (corr < -0.5).astype(float)
            n += 2

    if spx_ret1 is not None and dxy_ret1 is not None:
        for w in [10, 20]:
            corr = spx_ret1.rolling(w).corr(dxy_ret1)
            features[f"spx_dxy_corr_{w}d"] = corr
            n += 1

    dxy_close = _get_close("dxy")
    if dxy_close is not None:
        dxy_ma20 = dxy_close.rolling(20).mean()
        features["dxy_above_ma20"] = (dxy_close > dxy_ma20).astype(float)
        features["dxy_above_100"] = (dxy_close > 100).astype(float)
        features["dxy_above_105"] = (dxy_close > 105).astype(float)
        for w in [5, 20]:
            features[f"dxy_mom{w}d"] = dxy_close.pct_change(w) * 100
        n += 6

    gold_close = _get_close("gold")
    if gold_close is not None:
        gold_ma20 = gold_close.rolling(20).mean()
        features["gold_above_ma20"] = (gold_close > gold_ma20).astype(float)
        gold_ath = gold_close.rolling(252, min_periods=60).max()
        features["gold_dist_ath"] = (gold_close - gold_ath) / gold_ath.replace(0, np.nan) * 100
        features["gold_near_ath"] = (features["gold_dist_ath"] > -3).astype(float)
        for w in [5, 20]:
            features[f"gold_mom{w}d"] = gold_close.pct_change(w) * 100
        n += 7

    bonds10 = _get_close("us_10_years_bonds")
    bonds30 = _get_close("us_bonds_30_days_con")
    if bonds10 is not None and bonds30 is not None:
        features["yield_curve_proxy"] = bonds30 - bonds10
        features["yield_curve_inverted"] = (bonds30 < bonds10).astype(float)
        spread_5d = (bonds30 - bonds10).pct_change(5) * 100
        features["yield_curve_steepening"] = (spread_5d > 2).astype(float)
        features["yield_curve_flattening"] = (spread_5d < -2).astype(float)
        n += 4

    print(f"[feat_eng] Features inter-marchés: {n}", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE 11 — Features intraday J-1
# ════════════════════════════════════════════════════════════

def build_intraday_jmoins1_features(index: pd.DatetimeIndex,
                                    entry_point: str) -> pd.DataFrame:
    """
    Features intraday calculées sur les données de J-1.
    Disponibles le matin de J avant l'ouverture.
    """
    features = pd.DataFrame(index=index)
    intraday_dir = Path(__file__).parent / "data" / "live_selected"
    n = 0

    # ── SPX 5min J-1 ──
    spx5_path = intraday_dir / "SPX_5min.csv"
    if spx5_path.exists():
        try:
            spx5 = pd.read_csv(spx5_path, sep=";")
            spx5["time"] = pd.to_datetime(
                spx5["time"].astype(str).str.strip(), errors="coerce"
            )
            spx5 = spx5.dropna(subset=["time"]).sort_values("time").set_index("time")
            if spx5.index.tz is not None:
                spx5.index = spx5.index.tz_localize(None)

            jm1_stats = {}

            for session_date in index:
                prev_date = session_date - pd.offsets.BDay(1)
                mask = (
                    (spx5.index.date == prev_date.date()) &
                    (spx5.index.time >= dtime(9, 30)) &
                    (spx5.index.time <= dtime(16, 0))
                )
                day_bars = spx5[mask]

                if len(day_bars) < 10:
                    jm1_stats[session_date] = {}
                    continue

                o = float(day_bars["open"].iloc[0])
                h = float(day_bars["high"].max())
                l = float(day_bars["low"].min())
                c = float(day_bars["close"].iloc[-1])

                amplitude = (h - l) / o * 100 if o > 0 else np.nan
                close_pos = (c - l) / (h - l) if (h - l) > 0 else np.nan

                last30_mask = day_bars.index.time >= dtime(15, 30)
                last30 = day_bars[last30_mask]
                last30_ret = np.nan
                if len(last30) >= 3:
                    o2 = float(last30["open"].iloc[0])
                    if o2 > 0:
                        last30_ret = (float(last30["close"].iloc[-1]) - o2) / o2 * 100

                first60_mask = (
                    (day_bars.index.time >= dtime(9, 30)) &
                    (day_bars.index.time <= dtime(10, 30))
                )
                first60 = day_bars[first60_mask]
                first60_ret = np.nan
                if len(first60) >= 6:
                    o3 = float(first60["open"].iloc[0])
                    if o3 > 0:
                        first60_ret = (float(first60["close"].iloc[-1]) - o3) / o3 * 100

                or30_mask = (
                    (day_bars.index.time >= dtime(9, 30)) &
                    (day_bars.index.time < dtime(10, 0))
                )
                or30_bars = day_bars[or30_mask]
                or30_range = np.nan
                or30_ratio = np.nan
                if len(or30_bars) >= 4:
                    or30_h = float(or30_bars["high"].max())
                    or30_l = float(or30_bars["low"].min())
                    or30_range = (or30_h - or30_l) / o * 100 if o > 0 else np.nan
                    if amplitude and amplitude > 0:
                        or30_ratio = or30_range / amplitude

                rsi_col = None
                for col in day_bars.columns:
                    if "rsi" in col.lower() and "ma" not in col.lower():
                        rsi_col = col
                        break
                rsi_close = float(day_bars[rsi_col].iloc[-1]) if rsi_col else np.nan

                wvf_col = None
                for col in day_bars.columns:
                    if "williams" in col.lower() or "vix fix" in col.lower():
                        wvf_col = col
                        break
                wvf_close = float(day_bars[wvf_col].iloc[-1]) if wvf_col else np.nan

                jm1_stats[session_date] = {
                    "amplitude": amplitude,
                    "close_pos": close_pos,
                    "last30_ret": last30_ret,
                    "first60_ret": first60_ret,
                    "or30_range": or30_range,
                    "or30_ratio": or30_ratio,
                    "rsi_close": rsi_close,
                    "wvf_close": wvf_close,
                }

            for feat in ["amplitude", "close_pos", "last30_ret",
                         "first60_ret", "or30_range", "or30_ratio",
                         "rsi_close", "wvf_close"]:
                vals = {d: s.get(feat, np.nan)
                        for d, s in jm1_stats.items()}
                col_name = f"spx5_jm1_{feat}"
                features[col_name] = pd.Series(vals).reindex(index)
                n += 1

            for feat in ["amplitude", "close_pos", "or30_range", "or30_ratio"]:
                col = f"spx5_jm1_{feat}"
                if col in features.columns:
                    mu = features[col].rolling(20, min_periods=10).mean()
                    sigma = features[col].rolling(20, min_periods=10).std()
                    features[f"{col}_z20"] = (features[col] - mu) / sigma.replace(0, np.nan)
                    n += 1

            features["spx5_jm1_close_top_third"] = (
                features["spx5_jm1_close_pos"] > 0.67
            ).astype(float)
            features["spx5_jm1_close_bottom_third"] = (
                features["spx5_jm1_close_pos"] < 0.33
            ).astype(float)
            n += 2

            print(f"[feat_eng] SPX 5min J-1: {n} features calculées",
                  flush=True)
        except Exception as e:
            print(f"[feat_eng] SPX 5min J-1 erreur: {e}", flush=True)

    # ── SPX FUTURE 30min — overnight J-1 ──
    fut30_path = intraday_dir / "SPX_FUTURE_30min.csv"
    if fut30_path.exists():
        try:
            fut30 = pd.read_csv(fut30_path, sep=";")
            fut30["time"] = pd.to_datetime(
                fut30["time"].astype(str).str.strip(), errors="coerce"
            )
            fut30 = fut30.dropna(subset=["time"]).sort_values("time").set_index("time")
            if fut30.index.tz is not None:
                fut30.index = fut30.index.tz_localize(None)

            n_fut = 0
            overnight_stats = {}

            for session_date in index:
                prev_date = session_date - pd.offsets.BDay(1)
                ov_mask = (
                    (fut30.index >= prev_date.replace(hour=18, minute=0)) &
                    (fut30.index < session_date.replace(hour=9, minute=30))
                )
                ov_bars = fut30[ov_mask]

                if len(ov_bars) < 3:
                    overnight_stats[session_date] = {}
                    continue

                ov_open = float(ov_bars["open"].iloc[0])
                ov_close = float(ov_bars["close"].iloc[-1])
                ov_high = float(ov_bars["high"].max())
                ov_low = float(ov_bars["low"].min())

                ov_ret = (ov_close - ov_open) / ov_open * 100 if ov_open > 0 else np.nan
                ov_range = (ov_high - ov_low) / ov_open * 100 if ov_open > 0 else np.nan

                premkt_mask = (
                    (fut30.index.date == session_date.date()) &
                    (fut30.index.time >= dtime(7, 30)) &
                    (fut30.index.time < dtime(9, 30))
                )
                premkt = fut30[premkt_mask]
                premkt_ret = np.nan
                if len(premkt) >= 2:
                    o4 = float(premkt["open"].iloc[0])
                    if o4 > 0:
                        premkt_ret = (float(premkt["close"].iloc[-1]) - o4) / o4 * 100

                ov_vol = float(ov_bars["volume"].sum()) if "volume" in ov_bars.columns else np.nan

                overnight_stats[session_date] = {
                    "ov_ret": ov_ret,
                    "ov_range": ov_range,
                    "premkt_ret": premkt_ret,
                    "ov_vol": ov_vol,
                }

            for feat in ["ov_ret", "ov_range", "premkt_ret", "ov_vol"]:
                vals = {d: s.get(feat, np.nan)
                        for d, s in overnight_stats.items()}
                col_name = f"fut_jm1_{feat}"
                features[col_name] = pd.Series(vals).reindex(index)
                n_fut += 1

            for feat in ["ov_ret", "ov_range", "premkt_ret"]:
                col = f"fut_jm1_{feat}"
                if col in features.columns:
                    mu = features[col].rolling(20, min_periods=10).mean()
                    sigma = features[col].rolling(20, min_periods=10).std()
                    features[f"{col}_z20"] = (features[col] - mu) / sigma.replace(0, np.nan)
                    n_fut += 1

            features["fut_jm1_gap_up"] = (
                features["fut_jm1_ov_ret"] > 0.3
            ).astype(float)
            features["fut_jm1_gap_down"] = (
                features["fut_jm1_ov_ret"] < -0.3
            ).astype(float)
            features["fut_jm1_large_overnight"] = (
                features["fut_jm1_ov_range"] >
                features["fut_jm1_ov_range"].rolling(20).quantile(0.8)
            ).astype(float)
            n_fut += 3

            print(f"[feat_eng] Futures 30min overnight: {n_fut} features",
                  flush=True)
            n += n_fut
        except Exception as e:
            print(f"[feat_eng] Futures 30min erreur: {e}", flush=True)

    # ── SPY 30min J-1 — volume profile et VWAP ──
    spy30_path = intraday_dir / "SPY_30min.csv"
    if spy30_path.exists():
        try:
            spy30 = pd.read_csv(spy30_path, sep=";")
            spy30["time"] = pd.to_datetime(
                spy30["time"].astype(str).str.strip(), errors="coerce"
            )
            spy30 = spy30.dropna(subset=["time"]).sort_values("time").set_index("time")
            if spy30.index.tz is not None:
                spy30.index = spy30.index.tz_localize(None)

            n_spy = 0
            spy_stats = {}

            for session_date in index:
                prev_date = session_date - pd.offsets.BDay(1)
                mask = (
                    (spy30.index.date == prev_date.date()) &
                    (spy30.index.time >= dtime(9, 30)) &
                    (spy30.index.time <= dtime(16, 0))
                )
                day_bars = spy30[mask]

                if len(day_bars) < 6:
                    spy_stats[session_date] = {}
                    continue

                morn_mask = day_bars.index.time < dtime(13, 0)
                aftn_mask = day_bars.index.time >= dtime(13, 0)

                vol_morn = np.nan
                vol_aftn = np.nan
                vol_ratio = np.nan

                if "volume" in day_bars.columns:
                    vol_morn = float(day_bars[morn_mask]["volume"].sum())
                    vol_aftn = float(day_bars[aftn_mask]["volume"].sum())
                    if vol_morn > 0:
                        vol_ratio = vol_aftn / vol_morn

                vwap_col = None
                for col in day_bars.columns:
                    if "vwap" in col.lower():
                        vwap_col = col
                        break

                close_vs_vwap = np.nan
                if vwap_col and len(day_bars) > 0:
                    vwap_last = float(day_bars[vwap_col].iloc[-1])
                    close_last = float(day_bars["close"].iloc[-1])
                    if vwap_last > 0:
                        close_vs_vwap = (close_last - vwap_last) / vwap_last * 100

                upper_col = None
                lower_col = None
                for col in day_bars.columns:
                    if "upper" in col.lower():
                        upper_col = col
                    if "lower" in col.lower():
                        lower_col = col

                bb_pos = np.nan
                if upper_col and lower_col:
                    upper = float(day_bars[upper_col].iloc[-1])
                    lower = float(day_bars[lower_col].iloc[-1])
                    close = float(day_bars["close"].iloc[-1])
                    if (upper - lower) > 0:
                        bb_pos = (close - lower) / (upper - lower)

                spy_stats[session_date] = {
                    "vol_ratio": vol_ratio,
                    "close_vs_vwap": close_vs_vwap,
                    "bb_pos": bb_pos,
                }

            for feat in ["vol_ratio", "close_vs_vwap", "bb_pos"]:
                vals = {d: s.get(feat, np.nan)
                        for d, s in spy_stats.items()}
                col_name = f"spy30_jm1_{feat}"
                features[col_name] = pd.Series(vals).reindex(index)
                n_spy += 1

            for feat in ["close_vs_vwap", "bb_pos"]:
                col = f"spy30_jm1_{feat}"
                if col in features.columns:
                    mu = features[col].rolling(20, min_periods=10).mean()
                    sigma = features[col].rolling(20, min_periods=10).std()
                    features[f"{col}_z20"] = (features[col] - mu) / sigma.replace(0, np.nan)
                    n_spy += 1

            print(f"[feat_eng] SPY 30min J-1: {n_spy} features", flush=True)
            n += n_spy
        except Exception as e:
            print(f"[feat_eng] SPY 30min J-1 erreur: {e}", flush=True)

    print(f"[feat_eng] Features intraday J-1 total: {n}", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# PIPELINE COMPLET
# ════════════════════════════════════════════════════════════

def build_all_features(index: pd.DatetimeIndex = None,
                       entry_point: str = "9h30",
                       nan_threshold: float = 0.40,
                       verbose: bool = True) -> pd.DataFrame:
    """
    Construit TOUTES les features en une passe.
    Si index est None, utilise toutes les dates disponibles dans les CSV daily.
    """
    print(f"\n[feat_eng] Construction features pour {entry_point}...", flush=True)

    daily_csvs = load_all_daily()

    if index is None:
        # Construire l'index depuis les dates min/max de tous les CSV
        all_dates_min = [df.index.min() for df in daily_csvs.values() if len(df) > 0]
        all_dates_max = [df.index.max() for df in daily_csvs.values() if len(df) > 0]
        if not all_dates_min:
            return pd.DataFrame()
        date_min = min(all_dates_min)
        date_max = max(all_dates_max)
        # Jours ouvrables US uniquement
        full_idx = pd.date_range(date_min, date_max, freq="B")
        # Filtrer : garder les jours où ≥50% des CSV ont des données
        coverage = pd.DataFrame(index=full_idx)
        for name, df in daily_csvs.items():
            coverage[name] = df.reindex(full_idx, method=None).iloc[:, 0].notna()
        n_csvs = coverage.shape[1]
        keep = coverage.sum(axis=1) >= (n_csvs * 0.5)
        index = full_idx[keep]
        print(f"[feat_eng] Index auto: {len(index)} jours "
              f"({date_min.date()} → {date_max.date()})", flush=True)

    f1 = build_raw_features(daily_csvs, index)
    f2 = build_intraday_features(index, entry_point)
    f2b = build_intraday_jmoins1_features(index, entry_point)
    f3 = build_derived_features(daily_csvs, index)
    if _CROSS_LIB_AVAILABLE:
        try:
            f4_base = build_cross_features(daily_csvs, index)
            f4_cross = build_all_cross_features(
                daily_csvs, index,
                intraday_features=f2,
                min_priority=2  # Garder priorité ≥ 2 uniquement
            )
            f4 = pd.concat([f4_base, f4_cross], axis=1)
            f4 = f4.loc[:, ~f4.columns.duplicated()]
        except Exception as _e:
            print(f"[feat_eng] cross_feature_library erreur: {_e}",
                  flush=True)
            f4 = build_cross_features(daily_csvs, index)
    else:
        f4 = build_cross_features(daily_csvs, index)
    f5 = build_temporal_features(daily_csvs, index)
    f6 = build_lag_features(daily_csvs, index)
    f7 = build_microstructure_features(daily_csvs, index)
    f8 = build_regime_transition_features(daily_csvs, index)
    f9 = build_options_sentiment_features(daily_csvs, index)
    f10 = build_intermarket_features(daily_csvs, index)

    all_feat = pd.concat([f1, f2, f2b, f3, f4, f5, f6, f7, f8, f9, f10], axis=1)
    all_feat = all_feat.loc[:, ~all_feat.columns.duplicated()]

    n_raw = all_feat.shape[1]

    nan_rate = all_feat.isnull().mean()
    all_feat = all_feat.loc[:, nan_rate <= nan_threshold]

    split = int(len(all_feat) * 0.70)
    for col in all_feat.columns:
        if all_feat[col].isnull().any():
            median_is = all_feat[col].iloc[:split].median()
            if not np.isnan(median_is):
                all_feat[col] = all_feat[col].fillna(median_is)

    all_feat = all_feat.dropna(axis=1, how="any")

    if verbose:
        print(f"[feat_eng] Total: {n_raw} → {all_feat.shape[1]} features "
              f"(après filtre NaN {nan_threshold:.0%})", flush=True)
        print(f"[feat_eng] Sessions: {len(all_feat)} | "
              f"Couverture: {all_feat.index.min().date()} → "
              f"{all_feat.index.max().date()}", flush=True)

    gc.collect()
    return all_feat


def build_all_features_extended(entry_point: str = "9h30",
                                nan_threshold: float = 0.40,
                                exclude_features: list = None) -> pd.DataFrame:
    """
    Variante de build_all_features() sur période maximale (2020-2026)
    sans les features nécessitant VIX1D.
    Permet d'avoir ~1481 sessions au lieu de 791.
    """
    if exclude_features is None:
        exclude_features = ["vix1d", "vix1d_vix"]

    all_feat = build_all_features(index=None,
                                  entry_point=entry_point,
                                  nan_threshold=nan_threshold)

    cols_to_drop = [c for c in all_feat.columns
                    if any(ex in c.lower() for ex in exclude_features)]
    all_feat = all_feat.drop(columns=cols_to_drop, errors="ignore")

    print(f"[feat_eng_ext] Features après exclusion VIX1D: {all_feat.shape[1]}",
          flush=True)
    print(f"[feat_eng_ext] Sessions: {all_feat.shape[0]}", flush=True)
    return all_feat
