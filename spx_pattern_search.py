"""
spx_pattern_search.py — Recherche exhaustive de patterns SPX actionnables.

Approche :
1. Sessions récupérées avec imputation NaN (plus de 335 → ~700+)
2. Recherche combinatoire de règles sur top features
3. Validation walk-forward 8 fenêtres
4. Export des patterns actionnables (précision OOS ≥ objectif)
"""

import os
os.environ["KMP_DUPLICATE_LIB_OK"] = "TRUE"
os.environ["OMP_NUM_THREADS"] = "1"

import gc
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from pathlib import Path
from itertools import combinations
from datetime import time as dtime

# Hériter des fonctions de base
from spx_ml import (
    build_sessions, _load_daily_all, _load_intraday,
    RIC_THRESHOLD, IC_THRESHOLD, IS_RATIO,
    ENTRY_POINTS, HORIZONS_MIN
)

try:
    from feature_engineering import build_all_features as _build_all_features
    _FEAT_ENG_AVAILABLE = True
except ImportError:
    _FEAT_ENG_AVAILABLE = False

# ── Constantes recherche patterns ──────────────────────────────
MIN_PRECISION_TARGET = 0.82   # précision OOS minimum pour "actionnable"
MIN_OCCURRENCES_OOS  = 6      # occurrences OOS minimum par pattern
MIN_OCCURRENCES_IS   = 15     # occurrences IS minimum
MAX_FEATURES_PER_RULE = 5     # max features dans une règle combinatoire
N_WALKFORWARD_WINDOWS = 8     # fenêtres walk-forward
MIN_WINDOWS_VALID    = 6      # fenêtres minimum où le pattern doit tenir
NAN_THRESHOLD        = 0.30   # max 30% NaN par feature pour la garder
IMPUTE_BY            = "median"  # médiane IS pour imputer NaN
N_TOP_FEATURES       = 50     # nombre de top features pour la recherche


def build_full_feature_matrix(entry_point: str = "9h30",
                              target_horizon: str = "360min") -> tuple:
    """
    Construit X (features complètes) et y (cible amplitude).
    Utilise feature_engineering.py si disponible.
    """
    sessions = build_sessions(entry_point)
    if sessions.empty:
        return None, None, None, []

    target_col = f"abs_ret_{target_horizon}_pct"
    if target_col not in sessions.columns:
        available = [c for c in sessions.columns
                     if c.startswith("abs_ret_") and c.endswith("_pct")]
        if not available:
            return None, None, None, []
        target_col = sorted(available)[-1]

    if _FEAT_ENG_AVAILABLE:
        X = _build_all_features(sessions.index, entry_point, nan_threshold=0.40)
    else:
        print("[feat_eng] feature_engineering non disponible — fallback", flush=True)
        return None, None, None, []

    if X.empty:
        return None, None, None, []

    y_amp = sessions[target_col].copy()

    def _cat(v):
        if pd.isna(v):
            return np.nan
        if v >= RIC_THRESHOLD:
            return 2
        if v <= IC_THRESHOLD:
            return 0
        return 1

    y_cat = y_amp.apply(_cat)

    common = X.index.intersection(y_amp.dropna().index)
    X = X.loc[common].copy()
    y_amp = y_amp.loc[common]
    y_cat = y_cat.loc[common]

    valid = X.notnull().all(axis=1) & y_cat.notna() & y_amp.notna()
    X, y_amp, y_cat = X[valid], y_amp[valid], y_cat[valid]

    print(f"[pattern_search/{entry_point}] Sessions: {len(X)} | "
          f"Features: {X.shape[1]} | Horizon: {target_col}", flush=True)
    print(f"[pattern_search/{entry_point}] FORT≥{RIC_THRESHOLD}%: "
          f"{(y_cat == 2).sum()} | FAIBLE≤{IC_THRESHOLD}%: "
          f"{(y_cat == 0).sum()} | INCERT: {(y_cat == 1).sum()}", flush=True)

    gc.collect()
    return X, y_amp, y_cat, list(X.columns)


def _OLD_build_full_feature_matrix(entry_point: str = "9h30",
                                   target_horizon: str = "360min") -> tuple:
    """[DEPRECATED] Ancienne version conservée pour référence."""
    sessions = build_sessions(entry_point)
    if sessions.empty:
        return None, None, None, []

    target_col = f"abs_ret_{target_horizon}_pct"
    if target_col not in sessions.columns:
        available = [c for c in sessions.columns
                     if c.startswith("abs_ret_") and c.endswith("_pct")]
        if not available:
            return None, None, None, []
        target_col = sorted(available)[-1]

    features = pd.DataFrame(index=sessions.index)

    # ── Daily features (shift J-1) ──
    daily_csvs = _load_daily_all()
    for name, df_d in daily_csvs.items():
        for col in df_d.columns:
            series = df_d[col].shift(1).reindex(features.index, method="ffill")
            feat_name = f"{name}_{col}"[:40]
            features[feat_name] = series

    # ── Overnight futures ──
    df_fut = _load_intraday("SPX_FUTURE", "30min")
    if df_fut is not None:
        for date in sessions.index:
            prev = date - pd.Timedelta(days=3)
            night = df_fut[
                ((df_fut.index.date >= prev.date()) &
                 (df_fut.index.date <= date.date())) &
                ((df_fut.index.time > dtime(16, 0)) |
                 (df_fut.index.time < dtime(9, 30)))
            ]
            if night.empty:
                continue
            try:
                o = float(night.iloc[0].get("open", night.iloc[0].get("close", np.nan)))
                c = float(night.iloc[-1].get("close", np.nan))
                if not (np.isnan(o) or np.isnan(c) or o == 0):
                    features.loc[date, "fut_overnight_ret_pct"] = (c - o) / o * 100
                if "high" in night.columns and "low" in night.columns:
                    h = float(night["high"].max())
                    l = float(night["low"].min())
                    ref = max(float(night.iloc[0].get("close", 1)), 0.01)
                    features.loc[date, "fut_overnight_range_pct"] = (h - l) / ref * 100
                if "volume" in night.columns:
                    features.loc[date, "fut_overnight_volume"] = float(night["volume"].sum())
                if "rsi" in night.columns and night["rsi"].notna().any():
                    features.loc[date, "fut_overnight_rsi_last"] = float(night["rsi"].iloc[-1])
            except Exception:
                continue
        gc.collect()

    # ── Calendar features ──
    try:
        from calendar_features import get_calendar_features
        cal = get_calendar_features(features.index)
        if not cal.empty:
            for col in cal.columns:
                features[f"cal_{col}"] = cal[col].reindex(features.index, method="ffill")
    except Exception:
        pass

    # ── Features dérivées ──
    vix = daily_csvs.get("vix", pd.DataFrame()).get("close")
    vix3m = daily_csvs.get("vix3m", pd.DataFrame()).get("close")
    vvix = daily_csvs.get("vvix", pd.DataFrame()).get("close")
    vix9d_df = daily_csvs.get("vix9d", pd.DataFrame())
    vix1d_ratio = daily_csvs.get("vix1d_vix_ratio", pd.DataFrame()).get("close")
    gold_df = daily_csvs.get("gold", pd.DataFrame())
    ad_df = daily_csvs.get("advance_decline_rati", pd.DataFrame())

    if vix is not None:
        vix_s = vix.shift(1).reindex(features.index, method="ffill")
        if vix3m is not None:
            vix3m_s = vix3m.shift(1).reindex(features.index, method="ffill")
            features["vix_term_structure"] = vix_s / vix3m_s.replace(0, np.nan)
        if vvix is not None:
            vvix_s = vvix.shift(1).reindex(features.index, method="ffill")
            features["vvix_vix_ratio"] = vvix_s / vix_s.replace(0, np.nan)
        spx_close = daily_csvs.get("spx", pd.DataFrame()).get("close")
        if spx_close is not None:
            spx_s = spx_close.shift(1).reindex(features.index, method="ffill")
            for w in [3, 5, 10, 20]:
                features[f"spx_mom_{w}d"] = spx_s.pct_change(w) * 100
                if w in [5, 20]:
                    features[f"spx_vol_{w}d"] = spx_s.pct_change().rolling(w).std() * 100
            ma20 = spx_s.rolling(20).mean()
            features["spx_dist_ma20"] = (spx_s - ma20) / ma20 * 100

    # VIX1D/VIX composite
    if vix1d_ratio is not None and vix is not None:
        vix1d_s = vix1d_ratio.shift(1).reindex(features.index, method="ffill")
        vix_s2 = vix.shift(1).reindex(features.index, method="ffill")
        features["vix1d_vix_ratio_close"] = vix1d_s
        features["vix1d_ratio_x_vix"] = vix1d_s * vix_s2
        features["vix1d_high_ratio_low_vix"] = (
            (vix1d_s > 0.6) & (vix_s2 < 20)
        ).astype(float)

    # VIX9D spread
    if "close" in vix9d_df.columns and vix is not None:
        vix9d_s = vix9d_df["close"].shift(1).reindex(features.index, method="ffill")
        vix_s3 = vix.shift(1).reindex(features.index, method="ffill")
        features["vix9d_vix_spread"] = vix9d_s - vix_s3
        features["vix9d_vix_ratio"] = vix9d_s / vix_s3.replace(0, np.nan)

    # Gold momentum
    if "close" in gold_df.columns:
        gold_s = gold_df["close"].shift(1).reindex(features.index, method="ffill")
        features["gold_mom3d"] = gold_s.pct_change(3) * 100
        features["gold_mom5d"] = gold_s.pct_change(5) * 100

    # A/D momentum
    if "close" in ad_df.columns:
        ad_s = ad_df["close"].shift(1).reindex(features.index, method="ffill")
        features["adv_decl_mom3d"] = ad_s.pct_change(3) * 100
        features["adv_decl_zscore"] = (
            (ad_s - ad_s.rolling(20).mean()) /
            (ad_s.rolling(20).std() + 1e-8)
        )

    # Put/call ratios momentum
    for pc_name in ["spx_put_call_ratio", "equity_put_call_rati", "vix_put_call_ratio"]:
        pc_df = daily_csvs.get(pc_name, pd.DataFrame())
        if "close" in pc_df.columns:
            pc_s = pc_df["close"].shift(1).reindex(features.index, method="ffill")
            features[f"{pc_name[:20]}_mom3d"] = pc_s.pct_change(3) * 100
            features[f"{pc_name[:20]}_zscore"] = (
                (pc_s - pc_s.rolling(20).mean()) /
                (pc_s.rolling(20).std() + 1e-8)
            )

    # Gap overnight
    features["gap_pct"] = sessions.get("gap_pct",
        pd.Series(dtype=float)).reindex(features.index)
    features["day_of_week"] = pd.Series(sessions.index.dayofweek, index=sessions.index)
    features["month"] = pd.Series(sessions.index.month, index=sessions.index)

    # ── Features intraday J (barre 9h30 du jour même) ──
    # Disponibles pour 10h00 et 10h30 seulement
    if entry_point in ("10h00", "10h30"):
        df_spy_intra = _load_intraday("SPY", "30min")
        if df_spy_intra is not None:
            for date in sessions.index:
                bar = df_spy_intra[
                    (df_spy_intra.index.date == date.date()) &
                    (df_spy_intra.index.time == dtime(9, 30))
                ]
                if bar.empty:
                    continue
                for col in ["open", "close", "high", "low", "volume", "rsi", "vwap"]:
                    if col in bar.columns:
                        features.loc[date, f"spy_930_{col}"] = float(bar.iloc[0][col])
                if "open" in bar.columns and "close" in bar.columns:
                    o = float(bar.iloc[0]["open"])
                    c = float(bar.iloc[0]["close"])
                    if o > 0:
                        features.loc[date, "spy_930_ret_pct"] = (c - o) / o * 100
                        features.loc[date, "spy_930_amp_pct"] = abs(c - o) / o * 100
            gc.collect()

        # Direction barre 9h30 : bullish/bearish/neutre
        if "spy_930_ret_pct" in features.columns:
            features["spy_930_bullish"] = (
                features["spy_930_ret_pct"] > 0.1
            ).astype(float)
            features["spy_930_bearish"] = (
                features["spy_930_ret_pct"] < -0.1
            ).astype(float)

        # Ecart SPY_930 vs VWAP J-1 (momentum relatif)
        spy_vwap_j1 = daily_csvs.get("spy", pd.DataFrame()).get("vwap")
        if spy_vwap_j1 is not None and "spy_930_close" in features.columns:
            vwap_s = spy_vwap_j1.shift(1).reindex(features.index, method="ffill")
            features["spy_930_vs_prev_vwap"] = (
                features["spy_930_close"] - vwap_s
            ) / vwap_s.replace(0, np.nan) * 100

        # Pour 10h30 : aussi la barre 10h00
        if entry_point == "10h30" and df_spy_intra is not None:
            for date in sessions.index:
                bar2 = df_spy_intra[
                    (df_spy_intra.index.date == date.date()) &
                    (df_spy_intra.index.time == dtime(10, 0))
                ]
                if bar2.empty:
                    continue
                for col in ["open", "close", "high", "low", "volume", "rsi"]:
                    if col in bar2.columns:
                        features.loc[date, f"spy_1000_{col}"] = float(bar2.iloc[0][col])
                if "open" in bar2.columns and "close" in bar2.columns:
                    o2 = float(bar2.iloc[0]["open"])
                    c2 = float(bar2.iloc[0]["close"])
                    if o2 > 0:
                        features.loc[date, "spy_1000_ret_pct"] = (c2 - o2) / o2 * 100
            gc.collect()

    # ── Cible ──
    y_amp = sessions[target_col].copy()

    def _cat(v):
        if pd.isna(v):
            return np.nan
        if v >= RIC_THRESHOLD:
            return 2
        if v <= IC_THRESHOLD:
            return 0
        return 1

    y_cat = y_amp.apply(_cat)

    # ── Alignement ──
    common = features.index.intersection(y_amp.dropna().index)
    X = features.loc[common].copy()
    y_amp = y_amp.loc[common]
    y_cat = y_cat.loc[common]

    # ── Drop features avec trop de NaN ──
    nan_rate = X.isnull().mean()
    X = X.loc[:, nan_rate <= NAN_THRESHOLD]

    # ── Imputation médiane IS (pas d'info future) ──
    split = int(len(X) * IS_RATIO)
    for col in X.columns:
        if X[col].isnull().any():
            median_is = X[col].iloc[:split].median()
            X[col] = X[col].fillna(median_is)

    # ── Filtre sessions valides ──
    valid = X.notnull().all(axis=1) & y_cat.notna() & y_amp.notna()
    X, y_amp, y_cat = X[valid], y_amp[valid], y_cat[valid]

    print(f"[pattern_search/{entry_point}] Sessions: {len(X)} "
          f"(vs ~335 avant imputation) | Features: {X.shape[1]}", flush=True)
    print(f"[pattern_search/{entry_point}] FORT: {(y_cat == 2).sum()} | "
          f"FAIBLE: {(y_cat == 0).sum()} | INCERT: {(y_cat == 1).sum()}", flush=True)

    gc.collect()
    return X, y_amp, y_cat, list(X.columns)


def build_filtered_feature_matrix(entry_point: str = "9h30",
                                  target_horizon: str = "360min",
                                  vix_open_max: float = None,
                                  vix_open_min: float = None) -> tuple:
    """
    Variante de build_full_feature_matrix avec filtre sur VIX open J.

    vix_open_max : garder seulement les jours où VIX open ≤ vix_open_max
    vix_open_min : garder seulement les jours où VIX open ≥ vix_open_min

    Note : VIX open J est une donnée du JOUR MÊME (pas J-1).
    C'est valide car à 9h30/10h00/10h30 le VIX open est déjà connu.
    """
    X, y_amp, y_cat, feat_names = build_full_feature_matrix(
        entry_point, target_horizon
    )
    if X is None:
        return None, None, None, []

    daily_csvs = _load_daily_all()
    vix_df = daily_csvs.get("vix", pd.DataFrame())

    if "open" not in vix_df.columns:
        print(f"[filtered] Colonne 'open' absente du CSV VIX — "
              f"utilisation de 'close' J-1 comme proxy", flush=True)
        vix_open = vix_df.get("close")
        if vix_open is not None:
            vix_open = vix_open.shift(1).reindex(X.index, method="ffill")
    else:
        # VIX open J : disponible le matin même
        vix_open = vix_df["open"].reindex(X.index, method="ffill")

    if vix_open is None or vix_open.empty:
        print(f"[filtered] VIX open non disponible — pas de filtre appliqué",
              flush=True)
        return X, y_amp, y_cat, feat_names

    mask = pd.Series(True, index=X.index)
    if vix_open_max is not None:
        mask = mask & (vix_open <= vix_open_max)
    if vix_open_min is not None:
        mask = mask & (vix_open >= vix_open_min)

    n_before = len(X)
    X = X[mask]
    y_amp = y_amp[mask]
    y_cat = y_cat[mask]
    n_after = len(X)

    regime = []
    if vix_open_max:
        regime.append(f"VIX_open ≤ {vix_open_max}")
    if vix_open_min:
        regime.append(f"VIX_open ≥ {vix_open_min}")
    regime_str = " & ".join(regime) if regime else "aucun"

    print(f"[filtered/{entry_point}] Filtre: {regime_str}", flush=True)
    print(f"[filtered/{entry_point}] Sessions: {n_before} → {n_after} "
          f"({n_before - n_after} exclues)", flush=True)
    print(f"[filtered/{entry_point}] FORT: {(y_cat == 2).sum()} | "
          f"FAIBLE: {(y_cat == 0).sum()} | INCERT: {(y_cat == 1).sum()}", flush=True)

    return X, y_amp, y_cat, feat_names


def build_extended_feature_matrix(entry_point: str,
                                  target_horizon: str,
                                  vix_open_max: float = None,
                                  vix_open_min: float = None):
    """
    Matrice features sur période étendue 2020-2026 sans VIX1D.
    """
    from feature_engineering import build_all_features_extended

    all_feat = build_all_features_extended(entry_point=entry_point)

    if all_feat is None or len(all_feat) < 50:
        return None, None, None, None

    daily_dir = Path(__file__).parent / "data" / "live_selected"
    vix_open = None
    for f in daily_dir.glob("VIX_daily.csv"):
        try:
            df = pd.read_csv(f, sep=";")
            df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
            df = df.dropna(subset=["time"]).sort_values("time").set_index("time")
            df.index = df.index.normalize()
            if "open" in df.columns:
                vix_open = df["open"].shift(1).reindex(all_feat.index, method="ffill")
                break
        except Exception:
            continue

    y_amp = None
    spx_daily_path = daily_dir / "SPX_daily.csv"
    if spx_daily_path.exists():
        try:
            spx_d = pd.read_csv(spx_daily_path, sep=";")
            spx_d["time"] = pd.to_datetime(
                spx_d["time"].astype(str).str.strip(), errors="coerce"
            )
            spx_d = spx_d.dropna(subset=["time"]).sort_values("time").set_index("time")
            spx_d.index = spx_d.index.normalize()
            if "open" in spx_d.columns and "close" in spx_d.columns:
                y_amp_raw = (spx_d["close"] - spx_d["open"]).abs() / spx_d["open"] * 100
                y_amp = y_amp_raw.reindex(all_feat.index, method="ffill")
                print(f"[ext_matrix] Cible : {target_horizon} proxy "
                      f"abs(close-open)/open*100 | "
                      f"médiane={y_amp.median():.3f}%", flush=True)
        except Exception as e:
            print(f"[ext_matrix] Erreur cible : {e}", flush=True)

    if y_amp is None:
        if spx_daily_path.exists():
            try:
                spx_d = pd.read_csv(spx_daily_path, sep=";")
                spx_d["time"] = pd.to_datetime(
                    spx_d["time"].astype(str).str.strip(), errors="coerce"
                )
                spx_d = spx_d.dropna(subset=["time"]).sort_values("time").set_index("time")
                spx_d.index = spx_d.index.normalize()
                if "high" in spx_d.columns and "low" in spx_d.columns:
                    y_amp_raw = (spx_d["high"] - spx_d["low"]) / spx_d["open"] * 100
                    y_amp = y_amp_raw.reindex(all_feat.index, method="ffill")
            except Exception:
                pass

    if y_amp is None:
        return None, None, None, None

    mask = pd.Series(True, index=all_feat.index)
    if vix_open is not None:
        if vix_open_max is not None:
            mask &= (vix_open <= vix_open_max)
        if vix_open_min is not None:
            mask &= (vix_open >= vix_open_min)

    X = all_feat[mask].dropna(axis=1, thresh=int(mask.sum() * 0.6))
    y = y_amp[mask].reindex(X.index)
    y_cat = pd.Series(
        [2 if v >= RIC_THRESHOLD else (0 if v <= IC_THRESHOLD else 1) for v in y],
        index=X.index
    )

    print(f"[ext_matrix] Sessions: {len(X)} | Features: {X.shape[1]}",
          flush=True)
    return X, y, y_cat, list(X.columns)


def run_extended_search(entry_point: str = "9h30",
                        target_horizon: str = "360min",
                        vix_open_max: float = None,
                        ric_threshold: float = None) -> dict:
    """
    Recherche de patterns sur période étendue 2020-2026 sans VIX1D.
    """
    import json as _json

    thr = ric_threshold if ric_threshold else RIC_THRESHOLD
    suffix = f"_ext_ric{int(thr * 100)}bps"
    if vix_open_max:
        suffix += f"_vix_le{int(vix_open_max)}"

    print(f"\n{'=' * 60}")
    print(f"RECHERCHE ÉTENDUE {entry_point}/{target_horizon}")
    print(f"RIC≥{thr}% | VIX≤{vix_open_max or 'tous'} | 2020-2026 sans VIX1D")
    print(f"{'=' * 60}\n")

    X, y_amp, y_cat, feat_names = build_extended_feature_matrix(
        entry_point, target_horizon,
        vix_open_max=vix_open_max
    )

    if X is None or len(X) < 50:
        print("[ext_search] Pas assez de sessions")
        return {"ok": False}

    y_bin = (y_amp >= thr).astype(int)
    n_pos = int(y_bin.sum())
    split = int(len(X) * IS_RATIO)

    print(f"[ext_search] Sessions: {len(X)} | RIC≥{thr}%: "
          f"{n_pos} ({n_pos / len(X) * 100:.1f}%)")

    top_features = get_top_features(
        X, y_bin.map({0: 0, 1: 2}).fillna(1), N_TOP_FEATURES
    )
    uni = search_univariate_rules(X, y_bin, top_features, split)
    combo = search_combinatorial_rules(
        X, y_bin, top_features, split, MAX_FEATURES_PER_RULE
    )

    all_rules = uni + combo
    actionable = [r for r in all_rules
                  if r.get("precision_oos", 0) >= MIN_PRECISION_TARGET * 100
                  and r.get("n_oos", 0) >= MIN_OCCURRENCES_OOS
                  and not r.get("leakage_suspect", False)]
    robust = [r for r in actionable if r.get("is_robust")]

    all_rules.sort(
        key=lambda r: (
            1 if r.get("precision_oos", 0) == 100.0 else 0,
            r.get("is_robust", False),
            r.get("precision_oos", 0),
            r.get("n_oos", 0)
        ),
        reverse=True
    )

    print(f"\n[ext_search] Actionnables: {len(actionable)} | "
          f"Robustes: {len(robust)}")
    for i, p in enumerate(all_rules[:5]):
        print(f"  #{i+1} OOS={p['precision_oos']:.1f}% "
              f"({p['n_oos']} occ) WF={p.get('n_windows_valid', 0)}/"
              f"{p.get('n_windows_tested', 0)} "
              f"{'[ROBUSTE]' if p.get('is_robust') else ''}")
        for c in p.get("conditions", []):
            sym = "≥" if c["direction"] == "above" else "≤"
            print(f"     {c['feature']} {sym} {c['threshold']}")

    result = {
        "ok": True,
        "type": "extended",
        "entry_point": entry_point,
        "target": target_horizon,
        "ric_threshold": thr,
        "n_sessions": len(X),
        "n_ric": n_pos,
        "n_actionable": len(actionable),
        "n_robust": len(robust),
        "all_patterns": all_rules[:50],
    }

    def _ser(obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, dict):
            return {k: _ser(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_ser(i) for i in obj]
        return obj

    out = Path(__file__).parent / "data" / \
          f"patterns_{entry_point}_{target_horizon}{suffix}.json"
    with open(out, "w") as f:
        _json.dump(_ser(result), f, indent=2)
    print(f"[ext_search] Sauvegardé: {out}")
    return result


def run_filtered_search(entry_point: str = "9h30",
                        target_horizon: str = "360min",
                        vix_open_max: float = None,
                        vix_open_min: float = None,
                        save_results: bool = True) -> dict:
    """
    Recherche de patterns sur un sous-univers filtré par VIX open.
    Même pipeline que run_full_search mais sur les sessions filtrées.
    """
    import json as _json

    filter_label = ""
    if vix_open_max:
        filter_label += f"_vix_le{int(vix_open_max)}"
    if vix_open_min:
        filter_label += f"_vix_ge{int(vix_open_min)}"

    print(f"\n{'=' * 60}", flush=True)
    print(f"RECHERCHE FILTRÉE {entry_point} / {target_horizon}", flush=True)
    if vix_open_max:
        print(f"CONDITION: VIX open ≤ {vix_open_max}", flush=True)
    if vix_open_min:
        print(f"CONDITION: VIX open ≥ {vix_open_min}", flush=True)
    print(f"{'=' * 60}\n", flush=True)

    X, y_amp, y_cat, feat_names = build_filtered_feature_matrix(
        entry_point, target_horizon, vix_open_max, vix_open_min
    )
    if X is None or len(X) < 25:
        return {"ok": False,
                "error": f"Pas assez de sessions après filtre ({len(X) if X is not None else 0})"}

    split = int(len(X) * IS_RATIO)
    y_bin = (y_cat == 2).astype(int)

    print(f"[filtered] Sélection top {N_TOP_FEATURES} features...", flush=True)
    top_features = get_top_features(X, y_cat, N_TOP_FEATURES)
    print(f"[filtered] Top 10: {top_features[:10]}", flush=True)

    print(f"[filtered] Règles univariées...", flush=True)
    uni_rules = search_univariate_rules(X, y_bin, top_features, split)

    print(f"[filtered] Règles combinatoires...", flush=True)
    combo_rules = search_combinatorial_rules(
        X, y_bin, top_features, split, MAX_FEATURES_PER_RULE
    )

    all_rules = uni_rules + combo_rules
    all_rules.sort(
        key=lambda r: (
            r.get("is_robust", False),
            r.get("n_windows_valid", 0),
            r.get("precision_oos", 0),
            r.get("per_quarter_oos", 0),
        ),
        reverse=True
    )

    actionable = [r for r in all_rules
                  if r.get("precision_oos", 0) >= MIN_PRECISION_TARGET * 100
                  and r.get("per_quarter_oos", 0) >= 3
                  and r.get("n_oos", 0) >= MIN_OCCURRENCES_OOS
                  and not r.get("leakage_suspect", False)]

    robust = [r for r in actionable if r.get("is_robust", False)]

    print(f"\n[filtered] RÉSULTATS:", flush=True)
    print(f"  Patterns actionnables: {len(actionable)}", flush=True)
    print(f"  Patterns robustes: {len(robust)}", flush=True)

    print(f"\nTOP 10 PATTERNS (VIX open {'≤' + str(vix_open_max) if vix_open_max else ''}"
          f"{'≥' + str(vix_open_min) if vix_open_min else ''}):", flush=True)
    for i, p in enumerate(all_rules[:10]):
        feats = " + ".join(p["features"])
        robust_flag = "[ROBUSTE]" if p.get("is_robust") else ""
        print(f"  #{i+1} [{p['type']}] {feats} {robust_flag}", flush=True)
        print(f"       IS={p['precision_is']:.1f}% ({p['n_is']} occ) | "
              f"OOS={p['precision_oos']:.1f}% ({p['n_oos']} occ) | "
              f"~{p['per_quarter_oos']:.1f}/trim | "
              f"WF={p.get('n_windows_valid', 0)}/{p.get('n_windows_tested', 0)}",
              flush=True)
        for cond in p.get("conditions", []):
            sym = "≥" if cond["direction"] == "above" else "≤"
            pct = cond.get("percentile", cond.get("percentile_is", "?"))
            print(f"         {cond['feature']} {sym} {cond['threshold']} (P{pct})",
                  flush=True)

    result = {
        "ok": True,
        "entry_point": entry_point,
        "target": target_horizon,
        "filter": {
            "vix_open_max": vix_open_max,
            "vix_open_min": vix_open_min,
            "label": filter_label,
        },
        "n_sessions": len(X),
        "n_sessions_is": split,
        "n_sessions_oos": len(X) - split,
        "n_features": X.shape[1],
        "n_actionable": len(actionable),
        "n_robust": len(robust),
        "all_patterns": all_rules[:50],
        "actionable_patterns": actionable,
        "robust_patterns": robust,
    }

    if save_results:
        out_dir = Path(__file__).parent / "data"
        out_dir.mkdir(exist_ok=True)
        fname = f"patterns_{entry_point}_{target_horizon}{filter_label}.json"
        out_path = out_dir / fname

        def _ser(obj):
            if isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            if isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            if isinstance(obj, np.bool_):
                return bool(obj)
            if isinstance(obj, dict):
                return {k: _ser(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_ser(i) for i in obj]
            return obj

        try:
            with open(out_path, "w") as f:
                _json.dump(_ser(result), f, indent=2)
            print(f"[filtered] Sauvegardé: {out_path}", flush=True)
        except Exception as e:
            print(f"[filtered] Erreur sauvegarde: {e}", flush=True)

    return result


def run_ic_search(entry_point: str = "9h30",
                  target_horizon: str = "360min",
                  vix_open_max: float = None,
                  ic_threshold_override: float = None) -> dict:
    """
    Recherche de patterns IC (amplitude ≤ seuil, marché calme).
    Cible : y_bin = 1 si abs_ret ≤ IC_THRESHOLD
    """
    import json as _json

    ic_thr = ic_threshold_override if ic_threshold_override else IC_THRESHOLD

    filter_label = f"_ic_{int(ic_thr * 100)}bps"
    if vix_open_max:
        filter_label += f"_vix_le{int(vix_open_max)}"

    print(f"\n{'=' * 60}", flush=True)
    print(f"RECHERCHE IC {entry_point} / {target_horizon}", flush=True)
    print(f"CIBLE: amplitude ≤ {ic_thr}% (marché calme)", flush=True)
    if vix_open_max:
        print(f"CONDITION: VIX open ≤ {vix_open_max}", flush=True)
    print(f"{'=' * 60}\n", flush=True)

    if vix_open_max:
        X, y_amp, y_cat, feat_names = build_filtered_feature_matrix(
            entry_point, target_horizon, vix_open_max=vix_open_max
        )
    else:
        X, y_amp, y_cat, feat_names = build_full_feature_matrix(
            entry_point, target_horizon
        )

    if X is None or len(X) < 25:
        return {"ok": False, "error": "Pas assez de sessions"}

    split = int(len(X) * IS_RATIO)

    # Cible IC : journée calme (amplitude ≤ ic_thr)
    y_bin_ic = (y_amp <= ic_thr).astype(int)

    n_ic = y_bin_ic.sum()
    print(f"[ic_search] Sessions IC (calme ≤{ic_thr}%): {n_ic}/{len(X)} "
          f"({n_ic / len(X) * 100:.1f}%)", flush=True)
    print(f"[ic_search] IS: {y_bin_ic.iloc[:split].sum()}/{split} IC", flush=True)
    print(f"[ic_search] OOS: {y_bin_ic.iloc[split:].sum()}/{len(X) - split} IC", flush=True)

    top_features = get_top_features(X, y_bin_ic.map({0: 0, 1: 2}).fillna(1), N_TOP_FEATURES)
    print(f"[ic_search] Top 10 features IC: {top_features[:10]}", flush=True)

    uni_rules = search_univariate_rules(X, y_bin_ic, top_features, split)
    combo_rules = search_combinatorial_rules(X, y_bin_ic, top_features, split, MAX_FEATURES_PER_RULE)

    all_rules = uni_rules + combo_rules
    all_rules.sort(
        key=lambda r: (r.get("is_robust", False), r.get("n_windows_valid", 0),
                       r.get("precision_oos", 0), r.get("per_quarter_oos", 0)),
        reverse=True
    )

    actionable = [r for r in all_rules
                  if r.get("precision_oos", 0) >= MIN_PRECISION_TARGET * 100
                  and r.get("per_quarter_oos", 0) >= 3
                  and r.get("n_oos", 0) >= MIN_OCCURRENCES_OOS
                  and not r.get("leakage_suspect", False)]
    robust = [r for r in actionable if r.get("is_robust", False)]

    print(f"\n[ic_search] RÉSULTATS IC:", flush=True)
    print(f"  Patterns actionnables: {len(actionable)}", flush=True)
    print(f"  Patterns robustes: {len(robust)}", flush=True)
    print(f"\nTOP 10 PATTERNS IC (amplitude ≤ {ic_thr}%):", flush=True)
    for i, p in enumerate(all_rules[:10]):
        feats = " + ".join(p["features"])
        robust_flag = "[ROBUSTE]" if p.get("is_robust") else ""
        print(f"  #{i+1} [{p['type']}] {feats} {robust_flag}", flush=True)
        print(f"       IS={p['precision_is']:.1f}% ({p['n_is']} occ) | "
              f"OOS={p['precision_oos']:.1f}% ({p['n_oos']} occ) | "
              f"~{p['per_quarter_oos']:.1f}/trim | "
              f"WF={p.get('n_windows_valid', 0)}/{p.get('n_windows_tested', 0)}",
              flush=True)
        for cond in p.get("conditions", []):
            sym = "≥" if cond["direction"] == "above" else "≤"
            pct = cond.get("percentile", cond.get("percentile_is", "?"))
            print(f"         {cond['feature']} {sym} {cond['threshold']} (P{pct})",
                  flush=True)

    result = {
        "ok": True,
        "type": "IC",
        "entry_point": entry_point,
        "target": target_horizon,
        "ic_threshold": ic_thr,
        "filter_vix_max": vix_open_max,
        "n_sessions": len(X),
        "n_sessions_ic": int(n_ic),
        "n_actionable": len(actionable),
        "n_robust": len(robust),
        "all_patterns": all_rules[:50],
        "actionable_patterns": actionable,
        "robust_patterns": robust,
    }

    out_dir = Path(__file__).parent / "data"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"patterns_{entry_point}_{target_horizon}{filter_label}.json"

    def _ser(obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, dict):
            return {k: _ser(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_ser(i) for i in obj]
        return obj

    try:
        with open(out_path, "w") as f:
            _json.dump(_ser(result), f, indent=2)
        print(f"[ic_search] Sauvegardé: {out_path}", flush=True)
    except Exception as e:
        print(f"[ic_search] Erreur sauvegarde: {e}", flush=True)

    return result


def run_grid_search(
    entry_points: list = None,
    horizons: list = None,
    ric_thresholds: list = None,
    vix_filters: list = None,
) -> dict:
    """
    Grille 4×4 : RIC threshold × VIX filter.
    Pour chaque cellule, trouve le meilleur pattern avec précision ≥ 90%.
    """
    import json as _json

    if entry_points is None:
        entry_points = ["9h30", "10h00"]
    if horizons is None:
        horizons = ["360min", "240min", "180min"]
    if ric_thresholds is None:
        ric_thresholds = [0.45, 0.40, 0.35, 0.30]
    if vix_filters is None:
        vix_filters = [22.0, 21.0, 20.0, 19.0]

    results = {}

    for ep in entry_points:
        results[ep] = {}
        for hz in horizons:
            results[ep][hz] = {}
            print(f"\n{'=' * 60}")
            print(f"GRILLE {ep}/{hz}")
            print(f"{'=' * 60}")

            for ric_thr in ric_thresholds:
                for vix_max in vix_filters:
                    cell_key = f"{int(ric_thr * 100)}bps_vix{int(vix_max)}"
                    print(f"\n[grid] {ep}/{hz} | RIC≥{ric_thr}% | VIX≤{vix_max}",
                          flush=True)

                    X, y_amp, y_cat, feat_names = build_filtered_feature_matrix(
                        ep, hz, vix_open_max=vix_max
                    )

                    if X is None or len(X) < 30:
                        results[ep][hz][cell_key] = {
                            "ok": False, "error": "Pas assez de sessions"
                        }
                        continue

                    y_bin = (y_amp >= ric_thr).astype(int)
                    n_pos = int(y_bin.sum())

                    print(f"[grid] Sessions: {len(X)} | RIC≥{ric_thr}%: "
                          f"{n_pos} ({n_pos / len(X) * 100:.1f}%)", flush=True)

                    if n_pos < 20:
                        results[ep][hz][cell_key] = {
                            "ok": False,
                            "error": f"Pas assez de sessions RIC ({n_pos})"
                        }
                        continue

                    split = int(len(X) * IS_RATIO)

                    top_features = get_top_features(
                        X, y_bin.map({0: 0, 1: 2}).fillna(1), N_TOP_FEATURES
                    )
                    uni_rules = search_univariate_rules(X, y_bin, top_features, split)
                    combo_rules = search_combinatorial_rules(
                        X, y_bin, top_features, split, MAX_FEATURES_PER_RULE
                    )

                    all_rules = uni_rules + combo_rules

                    actionable_90 = [
                        r for r in all_rules
                        if r.get("precision_oos", 0) >= 90.0
                        and r.get("n_oos", 0) >= MIN_OCCURRENCES_OOS
                        and r.get("per_quarter_oos", 0) >= 2
                        and not r.get("leakage_suspect", False)
                    ]
                    robust_90 = [r for r in actionable_90 if r.get("is_robust", False)]

                    best = None
                    if robust_90:
                        best = sorted(
                            robust_90,
                            key=lambda r: (r["precision_oos"], r["n_oos"]),
                            reverse=True
                        )[0]
                    elif actionable_90:
                        best = sorted(
                            actionable_90,
                            key=lambda r: (r["precision_oos"], r["n_oos"]),
                            reverse=True
                        )[0]

                    cell_result = {
                        "ok": True,
                        "ric_threshold": ric_thr,
                        "vix_max": vix_max,
                        "n_sessions": len(X),
                        "n_ric": n_pos,
                        "n_actionable_90": len(actionable_90),
                        "n_robust_90": len(robust_90),
                        "best_pattern": best,
                        "cell_status": (
                            "green" if (best and best.get("is_robust") and best.get("precision_oos", 0) >= 90)
                            else "yellow" if (best and best.get("precision_oos", 0) >= 87)
                            else "red"
                        ),
                        "best_oos": best.get("precision_oos", 0) if best else 0,
                        "best_wf": f"{best.get('n_windows_valid', 0)}/{best.get('n_windows_tested', 0)}" if best else "0/0",
                        "best_occ": best.get("n_oos", 0) if best else 0,
                    }

                    results[ep][hz][cell_key] = cell_result

                    status = "🟢" if cell_result["cell_status"] == "green" else \
                             "🟡" if cell_result["cell_status"] == "yellow" else "🔴"
                    print(f"[grid] {status} {cell_result['n_actionable_90']} "
                          f"patterns ≥90% | Best: {cell_result['best_oos']:.1f}% OOS / "
                          f"{cell_result['best_occ']} occ / WF {cell_result['best_wf']}",
                          flush=True)

    out_dir = Path(__file__).parent / "data"
    out_dir.mkdir(exist_ok=True)

    def _ser(obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, dict):
            return {k: _ser(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_ser(i) for i in obj]
        return obj

    out_path = out_dir / "grid_results.json"
    with open(out_path, "w") as f:
        _json.dump(_ser(results), f, indent=2)
    print(f"\n[grid] Résultats sauvegardés: {out_path}", flush=True)

    print("\n" + "=" * 60)
    print("RÉSUMÉ GRILLE")
    print("=" * 60)
    for ep in entry_points:
        for hz in horizons:
            print(f"\n{ep}/{hz}:")
            for ric_thr in ric_thresholds:
                row = f"  RIC≥{ric_thr:.2f}%  "
                for vix_max in vix_filters:
                    cell_key = f"{int(ric_thr * 100)}bps_vix{int(vix_max)}"
                    cell = results[ep][hz].get(cell_key, {})
                    if not cell.get("ok"):
                        row += "  ❌    "
                    else:
                        s = cell.get("cell_status", "red")
                        sym = "🟢" if s == "green" else "🟡" if s == "yellow" else "🔴"
                        row += f"  {sym}{cell.get('best_oos', 0):.0f}% "
                print(row)

    return results


def get_top_features(X: pd.DataFrame, y_cat: pd.Series,
                     n: int = 50) -> list[str]:
    """
    Sélectionne les N features les plus importantes via XGBoost
    sur IS uniquement (pas de data leakage).
    """
    split = int(len(X) * IS_RATIO)
    X_is = X.iloc[:split]
    y_is = (y_cat.iloc[:split] == 2).astype(int)

    try:
        import xgboost as xgb
        clf = xgb.XGBClassifier(
            n_estimators=150, max_depth=3, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.6,
            scale_pos_weight=(y_is == 0).sum() / max((y_is == 1).sum(), 1),
            use_label_encoder=False, eval_metric="logloss",
            random_state=42, n_jobs=1, verbosity=0
        )
        clf.fit(X_is, y_is)
        imp = pd.Series(clf.feature_importances_, index=X.columns)
        del clf
        gc.collect()
        return imp.sort_values(ascending=False).head(n).index.tolist()
    except Exception as e:
        print(f"[pattern_search] Erreur sélection features: {e}", flush=True)
        return list(X.columns[:n])


def _test_rule(mask: pd.Series, y_bin: pd.Series,
               split: int) -> dict:
    """
    Teste une règle (mask booléen) sur IS puis OOS.
    Retourne précision IS, OOS, n_is, n_oos.
    """
    mask_is = mask.iloc[:split]
    mask_oos = mask.iloc[split:]
    y_is = y_bin.iloc[:split]
    y_oos = y_bin.iloc[split:]

    n_is = int(mask_is.sum())
    n_oos = int(mask_oos.sum())

    if n_is < MIN_OCCURRENCES_IS or n_oos < MIN_OCCURRENCES_OOS:
        return None

    prec_is = float((y_is[mask_is] == 1).sum() / n_is * 100)
    prec_oos = float((y_oos[mask_oos] == 1).sum() / n_oos * 100)

    # Détecter leakage potentiel : IS très faible + OOS très élevé
    # avec peu d'occurrences OOS = potentiellement du bruit
    leakage_flag = (prec_oos - prec_is > 30) and n_oos < 20

    return {
        "n_is": n_is, "n_oos": n_oos,
        "precision_is": round(prec_is, 1),
        "precision_oos": round(prec_oos, 1),
        "leakage_suspect": leakage_flag,
    }


def _walkforward_validate(mask: pd.Series, y_bin: pd.Series,
                          n_windows: int = N_WALKFORWARD_WINDOWS,
                          min_valid: int = MIN_WINDOWS_VALID) -> dict:
    """
    Validation walk-forward sur N fenêtres temporelles.
    Un pattern est robuste s'il tient sur ≥ min_valid fenêtres.
    """
    n = len(mask)
    window_size = n // (n_windows + 1)

    precisions = []
    n_occs = []

    for i in range(1, n_windows + 1):
        train_end = i * window_size
        test_start = train_end
        test_end = min(train_end + window_size, n)

        if test_end - test_start < 5:
            continue

        mask_w = mask.iloc[test_start:test_end]
        y_w = y_bin.iloc[test_start:test_end]
        n_occ = int(mask_w.sum())

        if n_occ < 2:
            precisions.append(None)
            n_occs.append(n_occ)
            continue

        prec = float((y_w[mask_w] == 1).sum() / n_occ * 100)
        precisions.append(prec)
        n_occs.append(n_occ)

    valid_precs = [p for p in precisions if p is not None]
    n_valid_windows = sum(1 for p in valid_precs if p >= MIN_PRECISION_TARGET * 100)

    # Robuste si ≥ 75% des fenêtres testées valident le pattern
    min_valid_adaptive = max(2, int(len(valid_precs) * 0.75))

    return {
        "n_windows_tested": len(valid_precs),
        "n_windows_valid": n_valid_windows,
        "is_robust": n_valid_windows >= min_valid_adaptive,
        "mean_precision_wf": round(float(np.mean(valid_precs)), 1) if valid_precs else None,
        "std_precision_wf": round(float(np.std(valid_precs)), 1) if valid_precs else None,
        "precisions_by_window": [round(p, 1) if p is not None else None for p in precisions],
        "occs_by_window": n_occs,
    }


def search_univariate_rules(X: pd.DataFrame, y_bin: pd.Series,
                            top_features: list[str],
                            split: int) -> list[dict]:
    """
    Recherche de règles univariées sur top features.
    Pour chaque feature : teste percentiles 10/20/.../90 comme seuil.
    """
    results = []

    for feat in top_features:
        if feat not in X.columns:
            continue
        series = X[feat]
        # Skip features booléennes (quantile() ne supporte pas bool)
        if series.dtype == bool or str(series.dtype) == 'bool':
            continue
        # Convertir object/bool en float si nécessaire
        try:
            series = series.astype(float)
        except Exception:
            continue
        X_is_feat = series.iloc[:split]

        for pct in range(10, 91, 10):
            threshold = float(X_is_feat.quantile(pct / 100))

            for direction in ["above", "below"]:
                if direction == "above":
                    mask = series >= threshold
                else:
                    mask = series <= threshold

                stats = _test_rule(mask, y_bin, split)
                if stats is None:
                    continue
                # Skip patterns suspects de leakage
                if stats.get("leakage_suspect") and stats["n_oos"] < 20:
                    continue
                if stats["precision_oos"] < MIN_PRECISION_TARGET * 100:
                    continue

                wf = _walkforward_validate(mask, y_bin)

                _mask_oos = mask.iloc[split:]
                _oos_dates = [
                    str(d.date()) if hasattr(d, "date") else str(d)
                    for d in X.index[split:][_mask_oos.values].tolist()
                ]
                results.append({
                    "type": "univariate",
                    "features": [feat],
                    "conditions": [{
                        "feature": feat,
                        "direction": direction,
                        "threshold": round(threshold, 4),
                        "percentile_is": pct,
                    }],
                    **stats,
                    **wf,
                    "per_quarter_oos": round(stats["n_oos"] / max(len(X.iloc[split:]) / 63, 1), 1),
                    "oos_dates": _oos_dates,
                })

    results.sort(key=lambda r: (r["is_robust"], r["precision_oos"]), reverse=True)
    print(f"[pattern_search] Règles univariées: {len(results)} passent le filtre", flush=True)
    return results


def search_combinatorial_rules(X: pd.DataFrame, y_bin: pd.Series,
                               top_features: list[str],
                               split: int,
                               max_combo: int = MAX_FEATURES_PER_RULE) -> list[dict]:
    """
    Recherche exhaustive de règles combinatoires.
    Combine 2 à max_combo features avec leurs seuils IS optimaux.
    """
    best_thresholds = {}
    for feat in top_features:
        if feat not in X.columns:
            continue
        series = X[feat]
        if series.dtype == bool or str(series.dtype) == 'bool':
            continue
        try:
            series = series.astype(float)
        except Exception:
            continue
        X_is_feat = series.iloc[:split]

        best_local = None
        best_prec_is = 0

        for pct in range(10, 91, 10):
            thr = float(X_is_feat.quantile(pct / 100))
            for direction in ["above", "below"]:
                mask = (series >= thr) if direction == "above" else (series <= thr)
                mask_is = mask.iloc[:split]
                y_is = y_bin.iloc[:split]
                n = mask_is.sum()
                if n < MIN_OCCURRENCES_IS:
                    continue
                prec = float((y_is[mask_is] == 1).sum() / n * 100)
                if prec > best_prec_is:
                    best_prec_is = prec
                    best_local = {
                        "threshold": round(thr, 4),
                        "direction": direction,
                        "prec_is": round(prec, 1),
                        "percentile": pct,
                    }

        if best_local:
            best_thresholds[feat] = best_local

    results = []
    tested = 0

    # Combos 2 features
    for feat1, feat2 in combinations(list(best_thresholds.keys()), 2):
        t1 = best_thresholds[feat1]
        t2 = best_thresholds[feat2]

        m1 = (X[feat1] >= t1["threshold"]) if t1["direction"] == "above" else (X[feat1] <= t1["threshold"])
        m2 = (X[feat2] >= t2["threshold"]) if t2["direction"] == "above" else (X[feat2] <= t2["threshold"])

        for op, mask in [("AND", m1 & m2), ("OR", m1 | m2)]:
            tested += 1
            stats = _test_rule(mask, y_bin, split)
            if stats is None:
                continue
            if stats.get("leakage_suspect") and stats["n_oos"] < 20:
                continue
            if stats["precision_oos"] < MIN_PRECISION_TARGET * 100:
                continue

            wf = _walkforward_validate(mask, y_bin)

            _mask_oos = mask.iloc[split:]
            _oos_dates = [
                str(d.date()) if hasattr(d, "date") else str(d)
                for d in X.index[split:][_mask_oos.values].tolist()
            ]
            results.append({
                "type": f"combo2_{op}",
                "features": [feat1, feat2],
                "operator": op,
                "conditions": [
                    {"feature": feat1, **t1},
                    {"feature": feat2, **t2},
                ],
                **stats,
                **wf,
                "per_quarter_oos": round(stats["n_oos"] / max(len(X.iloc[split:]) / 63, 1), 1),
                "oos_dates": _oos_dates,
            })
        gc.collect()

    # Combos 3 features (sur les meilleures paires)
    if max_combo >= 3:
        top_pairs = sorted(
            [r for r in results if r["type"].startswith("combo2")],
            key=lambda r: r["precision_oos"], reverse=True
        )[:20]

        extra_feats = [f for f in list(best_thresholds.keys())
                       if f not in [p["features"][0] for p in top_pairs[:5]]
                       and f not in [p["features"][1] for p in top_pairs[:5]]][:15]

        for pair in top_pairs[:10]:
            f1, f2 = pair["features"]
            for feat3 in extra_feats:
                if feat3 in (f1, f2):
                    continue
                t3 = best_thresholds.get(feat3)
                if not t3:
                    continue
                m3 = (X[feat3] >= t3["threshold"]) if t3["direction"] == "above" else (X[feat3] <= t3["threshold"])

                t1 = best_thresholds[f1]
                t2 = best_thresholds[f2]
                m1 = (X[f1] >= t1["threshold"]) if t1["direction"] == "above" else (X[f1] <= t1["threshold"])
                m2 = (X[f2] >= t2["threshold"]) if t2["direction"] == "above" else (X[f2] <= t2["threshold"])
                mask_and = m1 & m2 & m3

                tested += 1
                stats = _test_rule(mask_and, y_bin, split)
                if stats is None:
                    continue
                if stats.get("leakage_suspect") and stats["n_oos"] < 20:
                    continue
                if stats["precision_oos"] < MIN_PRECISION_TARGET * 100:
                    continue

                wf = _walkforward_validate(mask_and, y_bin)
                _mask_oos = mask_and.iloc[split:]
                _oos_dates = [
                    str(d.date()) if hasattr(d, "date") else str(d)
                    for d in X.index[split:][_mask_oos.values].tolist()
                ]
                results.append({
                    "type": "combo3_AND",
                    "features": [f1, f2, feat3],
                    "operator": "AND",
                    "conditions": [
                        {"feature": f1, **t1},
                        {"feature": f2, **t2},
                        {"feature": feat3, **t3},
                    ],
                    **stats,
                    **wf,
                    "per_quarter_oos": round(stats["n_oos"] / max(len(X.iloc[split:]) / 63, 1), 1),
                    "oos_dates": _oos_dates,
                })
            gc.collect()

    # Combos 4 features : top 5 triplets + feature supplémentaire
    if max_combo >= 4:
        top_triplets = sorted(
            [r for r in results if r["type"] == "combo3_AND"],
            key=lambda r: r["precision_oos"], reverse=True
        )[:5]

        extra_feats4 = [f for f in list(best_thresholds.keys())
                        if f not in sum([t["features"] for t in top_triplets[:3]], [])][:10]

        for triplet in top_triplets[:3]:
            f1, f2, f3 = triplet["features"]
            t1 = best_thresholds.get(f1)
            t2 = best_thresholds.get(f2)
            t3 = best_thresholds.get(f3)
            if not (t1 and t2 and t3):
                continue
            m1 = (X[f1] >= t1["threshold"]) if t1["direction"] == "above" else (X[f1] <= t1["threshold"])
            m2 = (X[f2] >= t2["threshold"]) if t2["direction"] == "above" else (X[f2] <= t2["threshold"])
            m3 = (X[f3] >= t3["threshold"]) if t3["direction"] == "above" else (X[f3] <= t3["threshold"])

            for feat4 in extra_feats4:
                if feat4 in (f1, f2, f3):
                    continue
                t4 = best_thresholds.get(feat4)
                if not t4:
                    continue
                m4 = (X[feat4] >= t4["threshold"]) if t4["direction"] == "above" else (X[feat4] <= t4["threshold"])
                mask4 = m1 & m2 & m3 & m4

                tested += 1
                stats = _test_rule(mask4, y_bin, split)
                if stats is None:
                    continue
                if stats.get("leakage_suspect") and stats["n_oos"] < 20:
                    continue
                if stats["precision_oos"] < MIN_PRECISION_TARGET * 100:
                    continue

                wf = _walkforward_validate(mask4, y_bin)
                _mask_oos = mask4.iloc[split:]
                _oos_dates = [
                    str(d.date()) if hasattr(d, "date") else str(d)
                    for d in X.index[split:][_mask_oos.values].tolist()
                ]
                results.append({
                    "type": "combo4_AND",
                    "features": [f1, f2, f3, feat4],
                    "operator": "AND",
                    "conditions": [
                        {"feature": f1, **t1},
                        {"feature": f2, **t2},
                        {"feature": f3, **t3},
                        {"feature": feat4, **t4},
                    ],
                    **stats,
                    **wf,
                    "per_quarter_oos": round(
                        stats["n_oos"] / max(len(X.iloc[split:]) / 63, 1), 1
                    ),
                    "oos_dates": _oos_dates,
                })
            gc.collect()

    # Combos 5 features : top 3 quadruplets + feature supplémentaire
    if max_combo >= 5:
        top_quadruplets = sorted(
            [r for r in results if r["type"] == "combo4_AND"],
            key=lambda r: r["precision_oos"], reverse=True
        )[:3]

        extra_feats5 = [f for f in list(best_thresholds.keys())
                        if f not in sum([t["features"] for t in top_quadruplets[:2]], [])][:8]

        for quad in top_quadruplets[:2]:
            f1q, f2q, f3q, f4q = quad["features"]
            t1q = best_thresholds.get(f1q)
            t2q = best_thresholds.get(f2q)
            t3q = best_thresholds.get(f3q)
            t4q = best_thresholds.get(f4q)
            if not (t1q and t2q and t3q and t4q):
                continue
            m1q = (X[f1q] >= t1q["threshold"]) if t1q["direction"] == "above" else (X[f1q] <= t1q["threshold"])
            m2q = (X[f2q] >= t2q["threshold"]) if t2q["direction"] == "above" else (X[f2q] <= t2q["threshold"])
            m3q = (X[f3q] >= t3q["threshold"]) if t3q["direction"] == "above" else (X[f3q] <= t3q["threshold"])
            m4q = (X[f4q] >= t4q["threshold"]) if t4q["direction"] == "above" else (X[f4q] <= t4q["threshold"])

            for feat5 in extra_feats5:
                if feat5 in (f1q, f2q, f3q, f4q):
                    continue
                t5 = best_thresholds.get(feat5)
                if not t5:
                    continue
                m5 = (X[feat5] >= t5["threshold"]) if t5["direction"] == "above" else (X[feat5] <= t5["threshold"])
                mask5 = m1q & m2q & m3q & m4q & m5

                tested += 1
                stats = _test_rule(mask5, y_bin, split)
                if stats is None:
                    continue
                if stats.get("leakage_suspect") and stats["n_oos"] < 20:
                    continue
                if stats["precision_oos"] < MIN_PRECISION_TARGET * 100:
                    continue

                wf = _walkforward_validate(mask5, y_bin)
                _mask_oos = mask5.iloc[split:]
                _oos_dates = [
                    str(d.date()) if hasattr(d, "date") else str(d)
                    for d in X.index[split:][_mask_oos.values].tolist()
                ]
                results.append({
                    "type": "combo5_AND",
                    "features": [f1q, f2q, f3q, f4q, feat5],
                    "operator": "AND",
                    "conditions": [
                        {"feature": f1q, **t1q},
                        {"feature": f2q, **t2q},
                        {"feature": f3q, **t3q},
                        {"feature": f4q, **t4q},
                        {"feature": feat5, **t5},
                    ],
                    **stats,
                    **wf,
                    "per_quarter_oos": round(
                        stats["n_oos"] / max(len(X.iloc[split:]) / 63, 1), 1
                    ),
                    "oos_dates": _oos_dates,
                })
            gc.collect()

    print(f"[pattern_search] Combos testés: {tested} | "
          f"Patterns actionnables: {len(results)}", flush=True)
    results.sort(key=lambda r: (r["is_robust"], r["precision_oos"],
                                  r["per_quarter_oos"]), reverse=True)
    return results


def run_full_search(entry_point: str = "9h30",
                    target_horizon: str = "360min",
                    n_top_features: int = 30,
                    save_results: bool = True) -> dict:
    """
    Pipeline complet de recherche de patterns.
    """
    import json

    print(f"\n{'=' * 60}", flush=True)
    print(f"RECHERCHE PATTERNS {entry_point} / {target_horizon}", flush=True)
    print(f"{'=' * 60}\n", flush=True)

    # ── 1. Features ──────────────────────────────────────────
    X, y_amp, y_cat, feat_names = build_full_feature_matrix(
        entry_point, target_horizon
    )
    if X is None:
        return {"ok": False, "error": "Pas de données"}

    split = int(len(X) * IS_RATIO)
    y_bin = (y_cat == 2).astype(int)  # binaire FORT vs non-FORT

    # ── 2. Top features ──────────────────────────────────────
    print(f"[pattern_search] Sélection top {n_top_features} features...", flush=True)
    top_features = get_top_features(X, y_cat, n_top_features)
    print(f"[pattern_search] Top 10: {top_features[:10]}", flush=True)

    # ── 3. Recherche univariée ────────────────────────────────
    print(f"[pattern_search] Recherche règles univariées...", flush=True)
    uni_rules = search_univariate_rules(X, y_bin, top_features, split)

    # ── 4. Recherche combinatoire ─────────────────────────────
    print(f"[pattern_search] Recherche règles combinatoires...", flush=True)
    combo_rules = search_combinatorial_rules(
        X, y_bin, top_features, split, MAX_FEATURES_PER_RULE
    )

    # ── 5. Fusion et ranking final ────────────────────────────
    all_rules = uni_rules + combo_rules

    all_rules.sort(
        key=lambda r: (
            r.get("is_robust", False),
            r.get("n_windows_valid", 0),
            r.get("precision_oos", 0),
            r.get("per_quarter_oos", 0),
        ),
        reverse=True
    )

    actionable = [r for r in all_rules
                  if r.get("precision_oos", 0) >= MIN_PRECISION_TARGET * 100
                  and r.get("per_quarter_oos", 0) >= 3
                  and r.get("n_oos", 0) >= MIN_OCCURRENCES_OOS
                  and not r.get("leakage_suspect", False)]

    robust = [r for r in actionable if r.get("is_robust", False)]

    print(f"\n[pattern_search] RÉSULTATS FINAUX:", flush=True)
    print(f"  Patterns actionnables: {len(actionable)}", flush=True)
    print(f"  Patterns robustes (walk-forward): {len(robust)}", flush=True)

    print(f"\nTOP 10 PATTERNS:", flush=True)
    for i, p in enumerate(all_rules[:10]):
        feats = " + ".join(p["features"])
        op = p.get("operator", "")
        robust_flag = "[ROBUSTE]" if p.get("is_robust") else ""
        print(f"  #{i+1} [{p['type']}] {feats} {op}", flush=True)
        print(f"       IS={p['precision_is']:.1f}% ({p['n_is']} occ) | "
              f"OOS={p['precision_oos']:.1f}% ({p['n_oos']} occ) | "
              f"~{p['per_quarter_oos']:.1f}/trim | "
              f"WF={p.get('n_windows_valid', 0)}/{p.get('n_windows_tested', 0)} {robust_flag}",
              flush=True)
        for cond in p.get("conditions", []):
            direction_sym = "≥" if cond["direction"] == "above" else "≤"
            pct_info = f"(P{cond.get('percentile', cond.get('percentile_is', '?'))})"
            print(f"         {cond['feature']} {direction_sym} "
                  f"{cond['threshold']} {pct_info}", flush=True)

    # ── 6. Sauvegarde ─────────────────────────────────────────
    result = {
        "ok": True,
        "entry_point": entry_point,
        "target": target_horizon,
        "n_sessions": len(X),
        "n_sessions_is": split,
        "n_sessions_oos": len(X) - split,
        "n_features": X.shape[1],
        "top_features": top_features,
        "n_actionable": len(actionable),
        "n_robust": len(robust),
        "all_patterns": all_rules[:50],
        "actionable_patterns": actionable,
        "robust_patterns": robust,
    }

    if save_results:
        out_dir = Path(__file__).parent / "data"
        out_dir.mkdir(exist_ok=True)
        out_path = out_dir / f"patterns_{entry_point}_{target_horizon}.json"

        def _serialize(obj):
            if isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            if isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            if isinstance(obj, np.bool_):
                return bool(obj)
            if isinstance(obj, dict):
                return {k: _serialize(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_serialize(i) for i in obj]
            return obj

        try:
            with open(out_path, "w") as f:
                json.dump(_serialize(result), f, indent=2)
            print(f"[pattern_search] Résultats sauvegardés: {out_path}", flush=True)
        except Exception as e:
            print(f"[pattern_search] Erreur sauvegarde: {e}", flush=True)

    return result


def load_results(entry_point: str = "9h30",
                 target_horizon: str = "360min") -> dict | None:
    """Charge les résultats d'une recherche précédente."""
    import json
    p = Path(__file__).parent / "data" / f"patterns_{entry_point}_{target_horizon}.json"
    if not p.exists():
        return None
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return None


def validate_top_pattern(entry_point: str = "10h00",
                         target_horizon: str = "360min") -> dict:
    """
    Validation approfondie des top 5 patterns sur toute la période.
    Analyse : dates, régimes, distribution annuelle, amplitude moyenne.
    """
    import json as _json

    p_file = Path(__file__).parent / "data" / f"patterns_{entry_point}_{target_horizon}.json"
    if not p_file.exists():
        return {"ok": False, "error": f"Lancer run_full_search('{entry_point}', '{target_horizon}') d'abord"}

    with open(p_file) as f:
        saved = _json.load(f)

    top_patterns = saved.get("all_patterns", [])
    if not top_patterns:
        return {"ok": False, "error": "Pas de patterns sauvegardés"}

    X, y_amp, y_cat, _ = build_full_feature_matrix(entry_point, target_horizon)
    if X is None:
        return {"ok": False, "error": "Pas de données"}

    split = int(len(X) * IS_RATIO)
    y_bin = (y_cat == 2).astype(int)
    results = []

    for pat in top_patterns[:5]:
        conditions = pat.get("conditions", [])
        if not conditions:
            continue

        mask = pd.Series(True, index=X.index)
        for cond in conditions:
            feat = cond["feature"]
            if feat not in X.columns:
                continue
            thr = float(cond["threshold"])
            if cond["direction"] == "above":
                mask = mask & (X[feat] >= thr)
            else:
                mask = mask & (X[feat] <= thr)

        active_dates = X.index[mask]
        if len(active_dates) == 0:
            continue

        date_results = []
        for d in active_dates:
            loc = X.index.get_loc(d)
            period = "IS" if loc < split else "OOS"
            is_fort = bool(y_bin.loc[d] == 1)
            amp = float(y_amp.loc[d])
            date_results.append({
                "date": d.strftime("%Y-%m-%d"),
                "period": period,
                "is_fort": is_fort,
                "amplitude_pct": round(amp, 3),
                "year": d.year,
            })

        by_year = {}
        for dr in date_results:
            yr = str(dr["year"])
            if yr not in by_year:
                by_year[yr] = {"total": 0, "fort": 0, "precision": 0.0}
            by_year[yr]["total"] += 1
            if dr["is_fort"]:
                by_year[yr]["fort"] += 1
        for yr in by_year:
            t = by_year[yr]["total"]
            f = by_year[yr]["fort"]
            by_year[yr]["precision"] = round(f / t * 100, 1) if t > 0 else 0.0

        n_total = len(date_results)
        n_fort = sum(1 for dr in date_results if dr["is_fort"])
        prec_total = round(n_fort / n_total * 100, 1) if n_total > 0 else 0.0
        fort_amps = [dr["amplitude_pct"] for dr in date_results if dr["is_fort"]]
        mean_amp = round(float(np.mean(fort_amps)), 3) if fort_amps else 0.0
        p90_amp = round(float(np.percentile(fort_amps, 90)), 3) if fort_amps else 0.0

        print(f"\n{'─' * 55}", flush=True)
        print(f"Pattern: {' + '.join(pat['features'])}", flush=True)
        print(f"Total IS+OOS: {n_total} occ | FORT: {n_fort} | Précision: {prec_total}%", flush=True)
        print(f"Amplitude moy FORT: {mean_amp}% | P90: {p90_amp}%", flush=True)
        print("Distribution annuelle:", flush=True)
        for yr in sorted(by_year.keys()):
            d = by_year[yr]
            bar = "█" * int(d["precision"] / 10)
            print(f"  {yr}: {d['fort']:2d}/{d['total']:2d} = {d['precision']:5.1f}% {bar}", flush=True)

        results.append({
            "pattern": pat,
            "n_total": n_total,
            "n_fort": n_fort,
            "precision_total": prec_total,
            "mean_amp_fort": mean_amp,
            "p90_amp_fort": p90_amp,
            "by_year": by_year,
            "date_results": date_results,
        })

    return {
        "ok": True,
        "entry_point": entry_point,
        "target": target_horizon,
        "validations": results,
    }


def run_all_entries(target_horizon: str = "360min") -> dict:
    """Lance la recherche sur les 3 points d'entrée et compare les patterns."""
    all_results = {}
    for entry in ["9h30", "10h00", "10h30"]:
        print(f"\n{'=' * 60}", flush=True)
        print(f"ENTRY POINT: {entry}", flush=True)
        r = run_full_search(entry, target_horizon, save_results=True)
        all_results[entry] = r
        gc.collect()

    # Patterns communs aux 3 entry points
    print(f"\n{'=' * 60}", flush=True)
    print("PATTERNS COMMUNS AUX 3 ENTRY POINTS", flush=True)
    print(f"{'=' * 60}", flush=True)

    common_features = None
    for entry, r in all_results.items():
        if not r.get("ok"):
            continue
        top_patterns = r.get("all_patterns", [])[:10]
        feats_in_top = set()
        for p in top_patterns:
            for f in p.get("features", []):
                feats_in_top.add(f)
        if common_features is None:
            common_features = feats_in_top
        else:
            common_features &= feats_in_top

    if common_features:
        print(f"Features dans top 10 de tous les entry points:", flush=True)
        for f in sorted(common_features):
            print(f"  → {f}", flush=True)
    else:
        print("Pas de features communes aux 3 entry points", flush=True)

    return all_results


def run_multi_horizon(entry_point: str = "9h30") -> dict:
    """
    Lance la recherche sur plusieurs horizons pour trouver
    des patterns supplémentaires sans dégrader la précision.
    """
    horizons = ["180min", "240min", "360min"]
    all_results = {}

    for hz in horizons:
        print(f"\n{'=' * 50}", flush=True)
        print(f"HORIZON: {hz} | ENTRY: {entry_point}", flush=True)
        r = run_full_search(entry_point, hz, save_results=True)
        all_results[hz] = r
        gc.collect()

    print(f"\n{'=' * 50}", flush=True)
    print("PATTERNS STABLES SUR PLUSIEURS HORIZONS", flush=True)
    feature_counts = {}
    for hz, r in all_results.items():
        for pat in r.get("all_patterns", [])[:10]:
            for feat in pat.get("features", []):
                feature_counts[feat] = feature_counts.get(feat, 0) + 1

    stable = sorted(
        [(f, c) for f, c in feature_counts.items() if c >= 2],
        key=lambda x: x[1], reverse=True
    )
    print("Features stables (≥2 horizons dans top 10):", flush=True)
    for feat, count in stable[:10]:
        print(f"  {feat}: {count}/{len(horizons)} horizons", flush=True)

    return all_results


def check_today_signals(entry_point: str = "9h30",
                        target_horizon: str = "360min") -> dict:
    """
    Vérifie si les patterns actionnables sont actifs aujourd'hui.
    Utilise les données J-1 disponibles.
    """
    import json as _json

    p_file = Path(__file__).parent / "data" / \
        f"patterns_{entry_point}_{target_horizon}.json"
    if not p_file.exists():
        return {"ok": False, "error": "Lancer run_full_search d'abord"}

    with open(p_file) as f:
        saved = _json.load(f)

    X, y_amp, y_cat, _ = build_full_feature_matrix(entry_point, target_horizon)
    if X is None or X.empty:
        return {"ok": False, "error": "Pas de données"}

    today = X.index[-1]
    today_data = X.iloc[-1]

    active_patterns = []
    inactive_patterns = []

    for pat in saved.get("all_patterns", [])[:20]:
        if not pat.get("is_robust"):
            continue
        conditions = pat.get("conditions", [])
        all_met = True
        condition_details = []

        for cond in conditions:
            feat = cond["feature"]
            if feat not in today_data.index:
                all_met = False
                condition_details.append({
                    "feature": feat,
                    "threshold": cond["threshold"],
                    "direction": cond["direction"],
                    "current_value": None,
                    "met": False,
                })
                continue

            val = float(today_data[feat])
            thr = float(cond["threshold"])
            met = (val >= thr) if cond["direction"] == "above" else (val <= thr)
            if not met:
                all_met = False

            condition_details.append({
                "feature": feat,
                "threshold": round(thr, 4),
                "direction": cond["direction"],
                "current_value": round(val, 4),
                "met": met,
            })

        result = {
            "pattern": pat,
            "conditions_detail": condition_details,
            "all_conditions_met": all_met,
            "precision_oos": pat.get("precision_oos", 0),
            "per_quarter": pat.get("per_quarter_oos", 0),
            "n_conditions_met": sum(1 for c in condition_details if c["met"]),
            "n_conditions_total": len(condition_details),
        }

        if all_met:
            active_patterns.append(result)
        else:
            inactive_patterns.append(result)

    print(f"\n[check_today] Date analyse: {today.strftime('%d/%m/%Y')}", flush=True)
    print(f"[check_today] {entry_point}/{target_horizon}", flush=True)
    print(f"[check_today] Patterns actifs: {len(active_patterns)}", flush=True)
    print(f"[check_today] Patterns inactifs: {len(inactive_patterns)}", flush=True)

    for ap in active_patterns:
        print(f"\n  ACTIF: {' + '.join(ap['pattern']['features'])}", flush=True)
        print(f"     OOS={ap['precision_oos']:.1f}% | ~{ap['per_quarter']:.1f}/trim",
              flush=True)
        for c in ap["conditions_detail"]:
            sym = "≥" if c["direction"] == "above" else "≤"
            status = "✓" if c["met"] else "✗"
            print(f"     {status} {c['feature']} {sym} {c['threshold']} "
                  f"(actuel: {c['current_value']})", flush=True)

    return {
        "ok": True,
        "date": today.strftime("%Y-%m-%d"),
        "entry_point": entry_point,
        "target": target_horizon,
        "active_patterns": active_patterns,
        "inactive_patterns": inactive_patterns,
        "n_active": len(active_patterns),
    }


def compute_sensitivity(entry_point: str = "9h30",
                        target_horizon: str = "360min",
                        top_features: list[str] = None) -> dict:
    """
    Pour chaque feature clé, calcule la précision OOS
    à différents seuils (percentiles 10 à 90).
    """
    X, y_amp, y_cat, feat_names = build_full_feature_matrix(
        entry_point, target_horizon
    )
    if X is None:
        return {"ok": False, "error": "Pas de données"}

    split = int(len(X) * IS_RATIO)
    y_bin = (y_cat == 2).astype(int)

    if top_features is None:
        top_features = get_top_features(X, y_cat, 10)

    key_features = [
        "vix9d_vix_spread", "spx_williams_vix_fix",
        "vix9d_close", "vvix_rsi", "spx_vol_5d",
        "vix6m_high", "vix6m_open", "spx_mom_20d",
    ]
    features_to_test = list(dict.fromkeys(
        [f for f in key_features if f in X.columns] +
        [f for f in top_features if f in X.columns]
    ))[:12]

    sensitivity = {}

    for feat in features_to_test:
        series = X[feat]
        if series.dtype == bool:
            continue
        try:
            series = series.astype(float)
        except Exception:
            continue

        X_is_feat = series.iloc[:split]
        curve = []

        for pct in range(10, 91, 5):
            thr = float(X_is_feat.quantile(pct / 100))

            for direction in ["above", "below"]:
                mask = (series >= thr) if direction == "above" else (series <= thr)
                mask_oos = mask.iloc[split:]
                y_oos = y_bin.iloc[split:]
                n_oos = int(mask_oos.sum())
                if n_oos < 5:
                    continue
                prec_oos = float((y_oos[mask_oos] == 1).sum() / n_oos * 100)
                baseline = float(y_oos.mean() * 100)

                curve.append({
                    "percentile": pct,
                    "threshold": round(thr, 4),
                    "direction": direction,
                    "n_oos": n_oos,
                    "precision_oos": round(prec_oos, 1),
                    "lift": round(prec_oos - baseline, 1),
                })

        best_by_pct = {}
        for point in curve:
            pct = point["percentile"]
            if pct not in best_by_pct or point["lift"] > best_by_pct[pct]["lift"]:
                best_by_pct[pct] = point

        sensitivity[feat] = {
            "curve": sorted(best_by_pct.values(), key=lambda x: x["percentile"]),
            "baseline_oos": round(float(y_bin.iloc[split:].mean() * 100), 1),
        }

        best_point = max(best_by_pct.values(), key=lambda x: x["lift"])
        sensitivity[feat]["optimal"] = best_point
        sensitivity[feat]["margin_low"] = next(
            (p for p in sorted(best_by_pct.values(), key=lambda x: x["percentile"])
             if p["precision_oos"] >= best_point["precision_oos"] * 0.95),
            best_point
        )
        sensitivity[feat]["margin_high"] = next(
            (p for p in sorted(best_by_pct.values(),
                                key=lambda x: x["percentile"], reverse=True)
             if p["precision_oos"] >= best_point["precision_oos"] * 0.95),
            best_point
        )

    import json as _json
    out = Path(__file__).parent / "data" / \
        f"sensitivity_{entry_point}_{target_horizon}.json"
    try:
        def _ser(obj):
            if isinstance(obj, (np.integer, np.int64)):
                return int(obj)
            if isinstance(obj, (np.floating, np.float64)):
                return float(obj)
            if isinstance(obj, np.bool_):
                return bool(obj)
            if isinstance(obj, dict):
                return {k: _ser(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_ser(i) for i in obj]
            return obj
        with open(out, "w") as f:
            _json.dump(_ser(sensitivity), f, indent=2)
        print(f"[sensitivity] Sauvegardé: {out}", flush=True)
    except Exception as e:
        print(f"[sensitivity] Erreur sauvegarde: {e}", flush=True)

    return {"ok": True, "sensitivity": sensitivity,
            "entry_point": entry_point, "target": target_horizon}


def check_today_signals(
    entry_point: str = "9h30",
    vix_open_today: float = None,
    verbose: bool = True,
) -> dict:
    """
    Évalue tous les patterns prioritaires sur les données de J-1
    et retourne les signaux actifs + recommandation options.
    """
    import json as _json
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent))

    from feature_engineering import build_all_features

    signals = []
    all_patterns = []

    pat_dir = Path(__file__).parent / "data"
    pat_files = sorted(pat_dir.glob("patterns_*.json"))

    for pat_file in pat_files:
        try:
            with open(pat_file) as f:
                data = _json.load(f)
            pats = data.get("all_patterns", [])
            good = [p for p in pats
                    if p.get("precision_oos", 0) >= 90
                    and p.get("n_oos", 0) >= 6
                    and not p.get("leakage_suspect", False)]
            good.sort(
                key=lambda p: (
                    1 if p.get("precision_oos", 0) == 100.0 else 0,
                    p.get("is_robust", False),
                    p.get("precision_oos", 0),
                    p.get("n_oos", 0)
                ),
                reverse=True
            )
            for p in good[:5]:
                p["_source"] = pat_file.stem
                all_patterns.append(p)
        except Exception:
            continue

    if not all_patterns:
        return {"ok": False, "error": "Aucun pattern disponible"}

    try:
        all_feat = build_all_features(index=None, entry_point=entry_point)
        if all_feat is None or len(all_feat) == 0:
            return {"ok": False, "error": "Features non disponibles"}
        last_row = all_feat.iloc[-1]
        last_date = all_feat.index[-1]
    except Exception as e:
        return {"ok": False, "error": f"Erreur features : {e}"}

    if verbose:
        print(f"\n{'='*60}")
        print(f"CHECK SIGNAUX — {entry_point} | Données au {last_date.date()}")
        print(f"{'='*60}")

    for pat in all_patterns:
        conditions = pat.get("conditions", [])
        if not conditions:
            continue

        match = True
        for cond in conditions:
            feat = cond.get("feature")
            direction = cond.get("direction")
            threshold = cond.get("threshold")

            if feat not in last_row.index:
                match = False
                break

            val = float(last_row[feat])
            if direction == "above" and val < threshold:
                match = False
                break
            elif direction == "below" and val > threshold:
                match = False
                break

        if match:
            signal = {
                "source": pat["_source"],
                "oos_pct": pat.get("precision_oos", 0),
                "n_oos": pat.get("n_oos", 0),
                "is_robust": pat.get("is_robust", False),
                "conditions": conditions,
                "per_quarter": pat.get("per_quarter_oos", 0),
            }

            if vix_open_today is not None:
                try:
                    from options_validator import interpolate_gains, _verdict
                    gains = interpolate_gains(vix_open_today)
                    source = pat["_source"]
                    if "_ic_" in source:
                        best_strat = "IC ±40"
                        best_gain = gains["IC_pur40"]
                    else:
                        ric_g = gains["RIC_pur40"]
                        rib_g = gains["RIB_20_40"]
                        if ric_g >= rib_g:
                            best_strat = "RIC ±40"
                            best_gain = ric_g
                        else:
                            best_strat = "RIB ±20→±40"
                            best_gain = rib_g

                    signal["options_recommendation"] = {
                        "strategy": best_strat,
                        "max_gain_pts": best_gain,
                        "verdict": _verdict(best_gain),
                        "vix_used": vix_open_today,
                        "all_gains": gains,
                    }
                except Exception:
                    pass

            signals.append(signal)

            if verbose:
                rob = "✓ ROBUSTE" if signal["is_robust"] else ""
                print(f"\n🔴 SIGNAL ACTIF — {signal['source']} {rob}")
                print(f"   OOS={signal['oos_pct']:.0f}% / {signal['n_oos']} occ "
                      f"({signal['per_quarter']:.1f}/trim)")
                for c in conditions:
                    sym = "≥" if c["direction"] == "above" else "≤"
                    val = float(last_row.get(c["feature"], float("nan")))
                    print(f"   {c['feature']} {sym} {c['threshold']} "
                          f"(actuel: {val:.4f})")
                if "options_recommendation" in signal:
                    rec = signal["options_recommendation"]
                    print(f"   → {rec['strategy']} : {rec['verdict']} "
                          f"(max gain {rec['max_gain_pts']:.1f}pts "
                          f"@ VIX {rec['vix_used']:.1f})")

    if verbose:
        if not signals:
            print("\n✅ Aucun signal actif — pas de trade aujourd'hui")
        else:
            print(f"\n{'='*60}")
            print(f"RÉSUMÉ : {len(signals)} signal(s) actif(s)")

    return {
        "ok": True,
        "evaluation_date": str(last_date.date()),
        "entry_point": entry_point,
        "n_signals": len(signals),
        "signals": signals,
        "no_trade": len(signals) == 0,
    }


def run_validation_2025(
    entry_point: str = "9h30",
    horizon: str = "360min",
) -> dict:
    """
    Valide les patterns prioritaires sur 2025 uniquement.
    """
    import json as _json

    print(f"\n{'='*60}")
    print(f"VALIDATION 2025 — {entry_point}/{horizon}")
    print(f"{'='*60}")

    X, y_amp, y_cat, feat_names = build_full_feature_matrix(entry_point, horizon)
    if X is None:
        return {"ok": False, "error": "Features non disponibles"}

    split = int(len(X) * IS_RATIO)
    X_oos = X.iloc[split:]
    y_oos = y_amp.iloc[split:]

    mask_2025 = X_oos.index.year == 2025
    X_2025 = X_oos[mask_2025]
    y_2025 = y_oos[mask_2025]

    if len(X_2025) < 10:
        return {"ok": False, "error": f"Pas assez de sessions 2025 ({len(X_2025)})"}

    y_2025_bin = (y_2025 >= RIC_THRESHOLD).astype(int)

    print(f"Sessions 2025 : {len(X_2025)} | "
          f"RIC≥{RIC_THRESHOLD}% : {y_2025_bin.sum()} "
          f"({y_2025_bin.mean()*100:.1f}%)")

    pat_dir = Path(__file__).parent / "data"
    results_2025 = []

    for pat_file in sorted(pat_dir.glob(f"patterns_{entry_point}_{horizon}*.json")):
        try:
            with open(pat_file) as f:
                data = _json.load(f)
        except Exception:
            continue

        for pat in data.get("all_patterns", [])[:20]:
            if pat.get("precision_oos", 0) < 90:
                continue
            conditions = pat.get("conditions", [])
            if not conditions:
                continue

            mask = pd.Series(True, index=X_2025.index)
            for cond in conditions:
                feat = cond.get("feature")
                if feat not in X_2025.columns:
                    mask[:] = False
                    break
                if cond["direction"] == "above":
                    mask &= X_2025[feat] >= cond["threshold"]
                else:
                    mask &= X_2025[feat] <= cond["threshold"]

            n_trigger = int(mask.sum())
            if n_trigger < 2:
                continue

            precision = float(y_2025_bin[mask].mean() * 100)
            results_2025.append({
                "source": pat_file.stem,
                "oos_global": pat.get("precision_oos", 0),
                "n_trigger_2025": n_trigger,
                "precision_2025": round(precision, 1),
                "delta": round(precision - pat.get("precision_oos", 0), 1),
                "conditions": conditions,
                "is_robust": pat.get("is_robust", False),
            })

    results_2025.sort(key=lambda r: (r["precision_2025"], r["n_trigger_2025"]),
                      reverse=True)

    print(f"\nTOP patterns validés sur 2025 :")
    for r in results_2025[:10]:
        flag = "🟢" if r["precision_2025"] >= 90 else \
               "🟡" if r["precision_2025"] >= 80 else "🔴"
        print(f"{flag} {r['source'][:38]} OOS={r['oos_global']:.1f}% → "
              f"2025={r['precision_2025']:.1f}% ({r['delta']:+.1f}%) "
              f"N={r['n_trigger_2025']}")

    out_path = pat_dir / f"validation_2025_{entry_point}_{horizon}.json"
    with open(out_path, "w") as f:
        _json.dump(results_2025, f, indent=2)
    print(f"\nSauvegardé : {out_path}")

    return {
        "ok": True,
        "n_sessions_2025": len(X_2025),
        "results": results_2025,
    }
