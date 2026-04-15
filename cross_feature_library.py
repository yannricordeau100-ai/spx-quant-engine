"""
cross_feature_library.py — Bibliothèque de features cross-asset
pour le SPX Edge system.

Organisation par relations économiques connues :
1. Structure de termes de volatilité complète
2. Divergences momentum cross-asset
3. Flow options composite
4. Triangle refuge (Gold/DXY/Bonds)
5. Momentum cross-fréquence (daily × intraday)
6. Régimes composites multi-dimensionnels
"""

import gc
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from pathlib import Path

# Score de priorité théorique (1=faible, 3=fort)
PRIORITY_SCORES = {}


def _safe_ratio(a: pd.Series, b: pd.Series, min_b: float = 0.01) -> pd.Series:
    """Ratio sécurisé avec protection division par zéro."""
    return a / b.where(b.abs() > min_b, np.nan)


def _zscore(s: pd.Series, window: int) -> pd.Series:
    """Z-score rolling."""
    mu = s.rolling(window, min_periods=window // 2).mean()
    sigma = s.rolling(window, min_periods=window // 2).std()
    return (s - mu) / sigma.replace(0, np.nan)


def _pct_rank(s: pd.Series, window: int = 252) -> pd.Series:
    """Percentile dynamique rolling."""
    return s.rolling(window, min_periods=window // 3).rank(pct=True) * 100


# ════════════════════════════════════════════════════════════
# CATÉGORIE A — Structure de termes de volatilité complète
# ════════════════════════════════════════════════════════════

def build_vol_term_structure(daily_csvs: dict,
                             index: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Structure complète de la surface de volatilité.
    Tous les ratios calculés J-1.
    """
    features = pd.DataFrame(index=index)
    n = 0

    def _get(name, col="close"):
        for k, df in daily_csvs.items():
            if k.startswith(name) and col in df.columns:
                return df[col].shift(1).reindex(index, method="ffill")
        return None

    vix9d = _get("vix9d")
    vix = _get("vix")
    vix3m = _get("vix3m")
    vix6m = _get("vix6m")
    vvix = _get("vvix")
    skew = _get("skew")

    series = {
        "vix9d": vix9d, "vix": vix,
        "vix3m": vix3m, "vix6m": vix6m,
        "vvix": vvix, "skew": skew
    }
    series = {k: v for k, v in series.items() if v is not None}

    vix_pairs = [
        ("vix9d", "vix", 3),
        ("vix9d", "vix3m", 3),
        ("vix9d", "vix6m", 2),
        ("vix", "vix3m", 3),
        ("vix", "vix6m", 2),
        ("vix3m", "vix6m", 2),
    ]

    for a_name, b_name, priority in vix_pairs:
        if a_name not in series or b_name not in series:
            continue
        a, b = series[a_name], series[b_name]
        ratio = _safe_ratio(a, b)
        spread = a - b
        fname_ratio = f"vts_{a_name}_{b_name}_ratio"
        fname_spread = f"vts_{a_name}_{b_name}_spread"

        features[fname_ratio] = ratio
        features[fname_spread] = spread
        PRIORITY_SCORES[fname_ratio] = priority
        PRIORITY_SCORES[fname_spread] = priority
        n += 2

        for w in [20, 60]:
            fname_z = f"vts_{a_name}_{b_name}_ratio_z{w}"
            features[fname_z] = _zscore(ratio, w)
            PRIORITY_SCORES[fname_z] = priority
            n += 1

        spread_accel = spread - spread.shift(3)
        fname_accel = f"vts_{a_name}_{b_name}_spread_accel3"
        features[fname_accel] = spread_accel
        PRIORITY_SCORES[fname_accel] = priority - 1
        n += 1

        inverted = (ratio > 1).astype(float)
        inverted_new = ((inverted == 1) & (inverted.shift(1) == 0)).astype(float)
        features[f"vts_{a_name}_{b_name}_inverted"] = inverted
        features[f"vts_{a_name}_{b_name}_inversion_new"] = inverted_new
        PRIORITY_SCORES[f"vts_{a_name}_{b_name}_inverted"] = priority
        PRIORITY_SCORES[f"vts_{a_name}_{b_name}_inversion_new"] = priority
        n += 2

    if "vvix" in series and "vix" in series:
        vvix_vix_ratio = _safe_ratio(series["vvix"], series["vix"])
        features["vts_vvix_vix_ratio"] = vvix_vix_ratio
        features["vts_vvix_vix_ratio_z20"] = _zscore(vvix_vix_ratio, 20)
        features["vts_vvix_high"] = (
            series["vvix"] > series["vvix"].rolling(20).mean() +
            1.5 * series["vvix"].rolling(20).std()
        ).astype(float)
        PRIORITY_SCORES["vts_vvix_vix_ratio"] = 3
        PRIORITY_SCORES["vts_vvix_vix_ratio_z20"] = 3
        PRIORITY_SCORES["vts_vvix_high"] = 2
        n += 3

    if "skew" in series and "vix" in series:
        skew_vix_ratio = _safe_ratio(series["skew"], series["vix"])
        features["vts_skew_vix_ratio"] = skew_vix_ratio
        features["vts_skew_vix_ratio_z20"] = _zscore(skew_vix_ratio, 20)
        features["vts_skew_high_vix_low"] = (
            (series["skew"] > 130) & (series["vix"] < 18)
        ).astype(float)
        skew_mom3 = series["skew"].pct_change(3) * 100
        vix_mom3 = series["vix"].pct_change(3) * 100
        features["vts_skew_down_vix_up"] = (
            (skew_mom3 < -3) & (vix_mom3 > 5)
        ).astype(float)
        PRIORITY_SCORES["vts_skew_vix_ratio"] = 3
        PRIORITY_SCORES["vts_skew_vix_ratio_z20"] = 3
        PRIORITY_SCORES["vts_skew_high_vix_low"] = 2
        PRIORITY_SCORES["vts_skew_down_vix_up"] = 2
        n += 4

    vol_components = []
    if vix is not None:
        vol_components.append(_zscore(vix, 20))
    if vix9d is not None and vix is not None:
        vol_components.append((vix9d - vix).pipe(_zscore, 20))
    if vvix is not None:
        vol_components.append(_zscore(vvix, 20))
    if skew is not None:
        vol_components.append(_zscore(skew, 60))

    if len(vol_components) >= 3:
        vol_stress = pd.concat(vol_components, axis=1).mean(axis=1)
        features["vts_composite_stress"] = vol_stress
        features["vts_composite_stress_extreme"] = (vol_stress > 2.0).astype(float)
        features["vts_composite_stress_low"] = (vol_stress < -1.0).astype(float)
        PRIORITY_SCORES["vts_composite_stress"] = 3
        PRIORITY_SCORES["vts_composite_stress_extreme"] = 3
        PRIORITY_SCORES["vts_composite_stress_low"] = 2
        n += 3

    print(f"[cross_feat] Vol term structure: {n} features", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE B — Divergences momentum cross-asset
# ════════════════════════════════════════════════════════════

def build_momentum_divergences(daily_csvs: dict,
                               index: pd.DatetimeIndex) -> pd.DataFrame:
    """Divergences de momentum entre assets liés."""
    features = pd.DataFrame(index=index)
    n = 0

    def _get_ret(name, periods=1):
        for k, df in daily_csvs.items():
            if k.startswith(name) and "close" in df.columns:
                c = df["close"].shift(1).reindex(index, method="ffill")
                return c.pct_change(periods) * 100
        return None

    spx1 = _get_ret("spx", 1); spx3 = _get_ret("spx", 3)
    spx5 = _get_ret("spx", 5); spx10 = _get_ret("spx", 10)
    spx20 = _get_ret("spx", 20)
    iwm1 = _get_ret("iwm", 1); iwm3 = _get_ret("iwm", 3); iwm5 = _get_ret("iwm", 5)
    qqq1 = _get_ret("qqq", 1); qqq3 = _get_ret("qqq", 3); qqq5 = _get_ret("qqq", 5)
    dax1 = _get_ret("dax40", 1); dax5 = _get_ret("dax40", 5)
    nikkei1 = _get_ret("nikkei225", 1)
    gold1 = _get_ret("gold", 1); gold5 = _get_ret("gold", 5)
    bonds10_1 = _get_ret("us_10_years_bonds", 1)

    if spx1 is not None and iwm1 is not None:
        for window, ret_spx, ret_iwm in [
            (1, spx1, iwm1), (3, spx3, iwm3), (5, spx5, iwm5)
        ]:
            if ret_spx is None or ret_iwm is None:
                continue
            div = ret_spx - ret_iwm
            fname = f"div_spx_iwm_{window}d"
            features[fname] = div
            features[f"{fname}_z20"] = _zscore(div, 20)
            features[f"div_spx_up_iwm_down_{window}d"] = (
                (ret_spx > 0.5) & (ret_iwm < -0.5)
            ).astype(float)
            features[f"div_iwm_leads_spx_{window}d"] = (
                ret_iwm > ret_spx + 0.5
            ).astype(float)
            PRIORITY_SCORES[fname] = 3
            PRIORITY_SCORES[f"{fname}_z20"] = 3
            PRIORITY_SCORES[f"div_spx_up_iwm_down_{window}d"] = 2
            PRIORITY_SCORES[f"div_iwm_leads_spx_{window}d"] = 2
            n += 4

    if spx1 is not None and qqq1 is not None:
        for window, ret_spx, ret_qqq in [
            (1, spx1, qqq1), (3, spx3, qqq3), (5, spx5, qqq5)
        ]:
            if ret_spx is None or ret_qqq is None:
                continue
            div = ret_qqq - ret_spx
            fname = f"div_qqq_spx_{window}d"
            features[fname] = div
            features[f"{fname}_z20"] = _zscore(div, 20)
            PRIORITY_SCORES[fname] = 2
            PRIORITY_SCORES[f"{fname}_z20"] = 2
            n += 2

    if spx1 is not None and dax1 is not None:
        div_dax = spx1 - dax1
        features["div_spx_dax_1d"] = div_dax
        features["div_spx_dax_1d_z20"] = _zscore(div_dax, 20)
        features["div_dax_leads_spx"] = (dax1 > spx1 + 1.0).astype(float)
        PRIORITY_SCORES["div_spx_dax_1d"] = 2
        PRIORITY_SCORES["div_spx_dax_1d_z20"] = 2
        PRIORITY_SCORES["div_dax_leads_spx"] = 2
        n += 3

    if spx1 is not None and nikkei1 is not None:
        div_nk = nikkei1 - spx1
        features["div_nikkei_spx_1d"] = div_nk
        features["div_nikkei_spx_1d_z20"] = _zscore(div_nk, 20)
        features["div_nikkei_crash"] = (nikkei1 < -2.0).astype(float)
        features["div_nikkei_surge"] = (nikkei1 > 2.0).astype(float)
        PRIORITY_SCORES["div_nikkei_spx_1d"] = 3
        PRIORITY_SCORES["div_nikkei_spx_1d_z20"] = 3
        PRIORITY_SCORES["div_nikkei_crash"] = 3
        PRIORITY_SCORES["div_nikkei_surge"] = 2
        n += 4

    if spx1 is not None and gold1 is not None:
        for window, ret_spx, ret_gold in [(1, spx1, gold1), (5, spx5, gold5)]:
            if ret_spx is None or ret_gold is None:
                continue
            div = ret_spx - ret_gold
            fname = f"div_spx_gold_{window}d"
            features[fname] = div
            features[f"{fname}_z20"] = _zscore(div, 20)
            features[f"div_spx_gold_both_up_{window}d"] = (
                (ret_spx > 0.5) & (ret_gold > 0.5)
            ).astype(float)
            features[f"div_gold_up_spx_down_{window}d"] = (
                (ret_gold > 1.0) & (ret_spx < -0.5)
            ).astype(float)
            PRIORITY_SCORES[fname] = 2
            PRIORITY_SCORES[f"{fname}_z20"] = 2
            PRIORITY_SCORES[f"div_spx_gold_both_up_{window}d"] = 2
            PRIORITY_SCORES[f"div_gold_up_spx_down_{window}d"] = 3
            n += 4

    if spx1 is not None and bonds10_1 is not None:
        corr10d = spx1.rolling(10).corr(bonds10_1)
        corr20d = spx1.rolling(20).corr(bonds10_1)
        features["div_spx_bonds10_corr10d"] = corr10d
        features["div_spx_bonds10_corr20d"] = corr20d
        features["div_spx_bonds_positive_corr"] = (corr10d > 0.3).astype(float)
        features["div_spx_bonds_flight"] = (corr10d < -0.7).astype(float)
        PRIORITY_SCORES["div_spx_bonds10_corr10d"] = 3
        PRIORITY_SCORES["div_spx_bonds10_corr20d"] = 2
        PRIORITY_SCORES["div_spx_bonds_positive_corr"] = 2
        PRIORITY_SCORES["div_spx_bonds_flight"] = 3
        n += 4

    if spx1 is not None and iwm1 is not None and qqq1 is not None:
        all_down = ((spx1 < -0.5) & (iwm1 < -0.5) & (qqq1 < -0.5)).astype(float)
        spx_only_up = ((spx1 > 0.5) & (iwm1 < 0) & (qqq1 < 0)).astype(float)
        features["div_triple_all_down"] = all_down
        features["div_triple_spx_only_up"] = spx_only_up
        dispersion = pd.concat([spx1, iwm1, qqq1], axis=1).std(axis=1)
        features["div_triple_dispersion"] = dispersion
        features["div_triple_high_dispersion"] = (
            dispersion > dispersion.rolling(20).quantile(0.8)
        ).astype(float)
        PRIORITY_SCORES["div_triple_all_down"] = 3
        PRIORITY_SCORES["div_triple_spx_only_up"] = 2
        PRIORITY_SCORES["div_triple_dispersion"] = 2
        PRIORITY_SCORES["div_triple_high_dispersion"] = 2
        n += 4

    print(f"[cross_feat] Momentum divergences: {n} features", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE C — Flow options composite
# ════════════════════════════════════════════════════════════

def build_options_flow_composite(daily_csvs: dict,
                                 index: pd.DatetimeIndex) -> pd.DataFrame:
    """Composite du flow options sur tous les assets disponibles."""
    features = pd.DataFrame(index=index)
    n = 0

    pc_assets = {}
    for k, df in daily_csvs.items():
        for asset in ["spx_put_call", "vix_put_call", "equity_put_call",
                      "iwm_put_call", "qqq_put_call", "spy_put_call"]:
            if k.startswith(asset) and "close" in df.columns:
                s = df["close"].shift(1).reindex(index, method="ffill")
                if s.notna().sum() >= 100:
                    short = asset.replace("_put_call", "")
                    pc_assets[short] = s

    if len(pc_assets) < 2:
        print(f"[cross_feat] Options flow: insuffisant ({len(pc_assets)} assets)",
              flush=True)
        return features

    pc_pairs_priority = [
        ("spx", "equity", 3),
        ("vix", "spx", 3),
        ("iwm", "spx", 2),
        ("qqq", "spx", 2),
        ("spy", "spx", 2),
    ]

    for a_name, b_name, priority in pc_pairs_priority:
        if a_name not in pc_assets or b_name not in pc_assets:
            continue
        ratio = _safe_ratio(pc_assets[a_name], pc_assets[b_name])
        fname = f"pcf_{a_name}_{b_name}_ratio"
        features[fname] = ratio
        features[f"{fname}_z20"] = _zscore(ratio, 20)
        features[f"{fname}_pct252"] = _pct_rank(ratio, 252)
        PRIORITY_SCORES[fname] = priority
        PRIORITY_SCORES[f"{fname}_z20"] = priority
        PRIORITY_SCORES[f"{fname}_pct252"] = priority - 1
        n += 3

    if len(pc_assets) >= 3:
        pc_df = pd.DataFrame({
            name: _zscore(s, 20)
            for name, s in pc_assets.items()
        })
        pc_mean = pc_df.mean(axis=1)
        pc_std = pc_df.std(axis=1)
        features["pcf_composite_level"] = pc_mean
        features["pcf_composite_fear"] = (pc_mean > 1.5).astype(float)
        features["pcf_composite_greed"] = (pc_mean < -1.5).astype(float)
        features["pcf_dispersion"] = pc_std
        features["pcf_convergence_high"] = (
            (pc_mean > 1.0) & (pc_std < 0.5)
        ).astype(float)
        PRIORITY_SCORES["pcf_composite_level"] = 3
        PRIORITY_SCORES["pcf_composite_fear"] = 3
        PRIORITY_SCORES["pcf_composite_greed"] = 2
        PRIORITY_SCORES["pcf_dispersion"] = 2
        PRIORITY_SCORES["pcf_convergence_high"] = 3
        n += 5

    for name, s in pc_assets.items():
        for w in [1, 3, 5]:
            mom = s.pct_change(w) * 100
            fname = f"pcf_{name}_mom{w}d"
            features[fname] = mom
            PRIORITY_SCORES[fname] = 2
            n += 1
        accel = s.pct_change(1) - s.pct_change(1).shift(3)
        fname_accel = f"pcf_{name}_accel"
        features[fname_accel] = accel
        PRIORITY_SCORES[fname_accel] = 2
        n += 1

    vix_close = None
    for k, df in daily_csvs.items():
        if k.startswith("vix") and "close" in df.columns and "9d" not in k \
           and "3m" not in k and "6m" not in k and "put" not in k:
            vix_close = df["close"].shift(1).reindex(index, method="ffill")
            break

    if vix_close is not None and "spx" in pc_assets:
        spx_pc = pc_assets["spx"]
        features["pcf_spx_pc_x_vix"] = spx_pc * vix_close / 100
        features["pcf_double_fear"] = (
            (spx_pc > spx_pc.rolling(20).quantile(0.8)) &
            (vix_close > 20)
        ).astype(float)
        PRIORITY_SCORES["pcf_spx_pc_x_vix"] = 3
        PRIORITY_SCORES["pcf_double_fear"] = 3
        n += 2

    print(f"[cross_feat] Options flow composite: {n} features", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE D — Triangle refuge Gold/DXY/Bonds
# ════════════════════════════════════════════════════════════

def build_refuge_triangle(daily_csvs: dict,
                          index: pd.DatetimeIndex) -> pd.DataFrame:
    """Relations entre les trois actifs refuge."""
    features = pd.DataFrame(index=index)
    n = 0

    def _get_ret(name, periods=1):
        for k, df in daily_csvs.items():
            if k.startswith(name) and "close" in df.columns:
                c = df["close"].shift(1).reindex(index, method="ffill")
                return c.pct_change(periods) * 100
        return None

    def _get_close(name):
        for k, df in daily_csvs.items():
            if k.startswith(name) and "close" in df.columns:
                return df["close"].shift(1).reindex(index, method="ffill")
        return None

    gold1 = _get_ret("gold", 1); gold5 = _get_ret("gold", 5)
    dxy1 = _get_ret("dxy", 1); dxy5 = _get_ret("dxy", 5)
    bonds10_1 = _get_ret("us_10_years_bonds", 1)
    gold_c = _get_close("gold")
    dxy_c = _get_close("dxy")

    if gold1 is None or dxy1 is None:
        print("[cross_feat] Triangle refuge: données insuffisantes", flush=True)
        return features

    if bonds10_1 is not None:
        ftq = (
            (gold1 > 0.3) & (bonds10_1 > 0.1) & (dxy1 > 0.1)
        ).astype(float)
        features["refuge_ftq_all"] = ftq
        PRIORITY_SCORES["refuge_ftq_all"] = 3

        infla = (
            (gold1 > 0.5) & (bonds10_1 < -0.2) & (dxy1 > 0.2)
        ).astype(float)
        features["refuge_inflation_stress"] = infla
        PRIORITY_SCORES["refuge_inflation_stress"] = 3

        reces = (
            (bonds10_1 > 0.3) & (dxy1 < -0.2)
        ).astype(float)
        features["refuge_recession_signal"] = reces
        PRIORITY_SCORES["refuge_recession_signal"] = 2
        n += 3

    gold_dxy_ratio = _safe_ratio(
        gold_c, dxy_c
    ) if gold_c is not None and dxy_c is not None else None

    if gold_dxy_ratio is not None:
        features["refuge_gold_dxy_ratio"] = gold_dxy_ratio
        features["refuge_gold_dxy_ratio_z20"] = _zscore(gold_dxy_ratio, 20)
        features["refuge_gold_dxy_ratio_z60"] = _zscore(gold_dxy_ratio, 60)
        features["refuge_gold_dxy_ratio_pct252"] = _pct_rank(gold_dxy_ratio, 252)
        features["refuge_gold_dxy_extreme"] = (
            _zscore(gold_dxy_ratio, 60) > 2.0
        ).astype(float)
        PRIORITY_SCORES["refuge_gold_dxy_ratio"] = 2
        PRIORITY_SCORES["refuge_gold_dxy_ratio_z20"] = 2
        PRIORITY_SCORES["refuge_gold_dxy_ratio_z60"] = 2
        PRIORITY_SCORES["refuge_gold_dxy_ratio_pct252"] = 1
        PRIORITY_SCORES["refuge_gold_dxy_extreme"] = 3
        n += 5

    refuge_signals = []
    if gold1 is not None:
        refuge_signals.append((gold1 > 0.5).astype(float))
    if dxy1 is not None:
        refuge_signals.append((dxy1 > 0.3).astype(float))
    if bonds10_1 is not None:
        refuge_signals.append((bonds10_1 > 0.2).astype(float))

    if len(refuge_signals) >= 2:
        refuge_score = pd.concat(refuge_signals, axis=1).sum(axis=1)
        features["refuge_composite_score"] = refuge_score
        features["refuge_composite_max"] = (
            refuge_score == len(refuge_signals)
        ).astype(float)
        PRIORITY_SCORES["refuge_composite_score"] = 3
        PRIORITY_SCORES["refuge_composite_max"] = 3
        n += 2

    for name, ret5 in [("gold", gold5), ("dxy", dxy5)]:
        if ret5 is not None:
            fname = f"refuge_{name}_mom5d"
            features[fname] = ret5
            fname_strong = f"refuge_{name}_strong_move"
            features[fname_strong] = (ret5.abs() > 2.0).astype(float)
            PRIORITY_SCORES[fname] = 2
            PRIORITY_SCORES[fname_strong] = 2
            n += 2

    print(f"[cross_feat] Triangle refuge: {n} features", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE E — Momentum cross-fréquence
# ════════════════════════════════════════════════════════════

def build_cross_frequency_momentum(daily_csvs: dict,
                                   index: pd.DatetimeIndex,
                                   intraday_features: pd.DataFrame = None
                                   ) -> pd.DataFrame:
    """Cohérence ou divergence entre timeframes."""
    features = pd.DataFrame(index=index)
    n = 0

    def _get_close(name):
        for k, df in daily_csvs.items():
            if k.startswith(name) and "close" in df.columns:
                return df["close"].shift(1).reindex(index, method="ffill")
        return None

    spx_c = _get_close("spx")
    vix_c = _get_close("vix")

    if spx_c is not None:
        for w in [1, 3, 5, 10, 20]:
            mom = spx_c.pct_change(w) * 100
            if intraday_features is not None and "or30_direction" in intraday_features.columns:
                or30_dir = intraday_features["or30_direction"]
                coherent = (
                    (mom > 0) & (or30_dir > 0) |
                    (mom < 0) & (or30_dir < 0)
                ).astype(float)
                features[f"cfm_spx_or30_coherent_{w}d"] = coherent
                features[f"cfm_spx_or30_diverge_{w}d"] = (1 - coherent)
                PRIORITY_SCORES[f"cfm_spx_or30_coherent_{w}d"] = 3
                PRIORITY_SCORES[f"cfm_spx_or30_diverge_{w}d"] = 3
                n += 2

        mom1 = spx_c.pct_change(1) * 100
        mom5 = spx_c.pct_change(5) * 100
        mom20 = spx_c.pct_change(20) * 100

        features["cfm_spx_accel_1_5"] = mom1 - mom5 / 5
        features["cfm_spx_accel_5_20"] = mom5 / 5 - mom20 / 20
        features["cfm_spx_all_aligned_down"] = (
            (mom1 < 0) & (mom5 < 0) & (mom20 < 0)
        ).astype(float)
        features["cfm_spx_all_aligned_up"] = (
            (mom1 > 0) & (mom5 > 0) & (mom20 > 0)
        ).astype(float)
        PRIORITY_SCORES["cfm_spx_accel_1_5"] = 3
        PRIORITY_SCORES["cfm_spx_accel_5_20"] = 2
        PRIORITY_SCORES["cfm_spx_all_aligned_down"] = 3
        PRIORITY_SCORES["cfm_spx_all_aligned_up"] = 2
        n += 4

    if vix_c is not None:
        vix_mom1 = vix_c.pct_change(1) * 100
        vix_mom5 = vix_c.pct_change(5) * 100

        features["cfm_vix_accel"] = vix_mom1 - vix_mom5 / 5
        features["cfm_vix_spike_from_low"] = (
            (vix_mom1 > 5) & (vix_c < 18)
        ).astype(float)
        features["cfm_vix_crush_from_high"] = (
            (vix_mom1 < -5) & (vix_c > 25)
        ).astype(float)
        features["cfm_vix_trending_up_5d"] = (
            (vix_mom5 > 10) & (vix_mom1 > 0)
        ).astype(float)
        PRIORITY_SCORES["cfm_vix_accel"] = 3
        PRIORITY_SCORES["cfm_vix_spike_from_low"] = 3
        PRIORITY_SCORES["cfm_vix_crush_from_high"] = 3
        PRIORITY_SCORES["cfm_vix_trending_up_5d"] = 3
        n += 4

        vix_up = (vix_mom1 > 2).astype(float)
        vix_high = (vix_c > 20).astype(float)
        features["cfm_vix_quad_up_high"] = vix_up * vix_high
        features["cfm_vix_quad_up_low"] = vix_up * (1 - vix_high)
        features["cfm_vix_quad_down_high"] = (1 - vix_up) * vix_high
        features["cfm_vix_quad_down_low"] = (1 - vix_up) * (1 - vix_high)
        for q in ["up_high", "up_low", "down_high", "down_low"]:
            PRIORITY_SCORES[f"cfm_vix_quad_{q}"] = 3
        n += 4

    print(f"[cross_feat] Cross-frequency momentum: {n} features", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# CATÉGORIE F — Régimes composites multi-dimensionnels
# ════════════════════════════════════════════════════════════

def build_composite_regimes(daily_csvs: dict,
                            index: pd.DatetimeIndex) -> pd.DataFrame:
    """Détection de régimes de marché multi-dimensionnels."""
    features = pd.DataFrame(index=index)
    n = 0

    def _get(name, col="close"):
        for k, df in daily_csvs.items():
            if k.startswith(name) and col in df.columns:
                return df[col].shift(1).reindex(index, method="ffill")
        return None

    vix = _get("vix")
    vix9d = _get("vix9d")
    spx = _get("spx")
    vvix = _get("vvix")
    ad = _get("advance_decline_rati")
    spx_pc = _get("spx_put_call")

    stress_signals = []
    if vix is not None:
        stress_signals.append((vix > 20).astype(float))
        stress_signals.append(
            (vix > vix.rolling(20).mean() + vix.rolling(20).std()).astype(float)
        )
    if vix9d is not None and vix is not None:
        stress_signals.append((vix9d > vix).astype(float))
    if spx is not None:
        stress_signals.append(
            (spx < spx.rolling(20).mean()).astype(float)
        )
    if ad is not None:
        stress_signals.append(
            (ad < ad.rolling(10).mean()).astype(float)
        )
    if spx_pc is not None:
        stress_signals.append(
            (spx_pc > spx_pc.rolling(20).quantile(0.7)).astype(float)
        )

    if len(stress_signals) >= 3:
        stress_score = pd.concat(stress_signals, axis=1).sum(axis=1)
        features["regime_stress_score"] = stress_score
        features["regime_stress_max"] = (
            stress_score >= len(stress_signals) - 1
        ).astype(float)
        features["regime_stress_moderate"] = (
            (stress_score >= 2) & (stress_score < len(stress_signals) - 1)
        ).astype(float)
        features["regime_calm"] = (stress_score <= 1).astype(float)
        for k in ["regime_stress_score", "regime_stress_max",
                  "regime_stress_moderate", "regime_calm"]:
            PRIORITY_SCORES[k] = 3
        n += 4

    if vix is not None and vvix is not None:
        vix_z = _zscore(vix, 60)
        vvix_z = _zscore(vvix, 60)
        features["regime_vol_crisis"] = (
            (vix_z > 1.5) & (vvix_z > 1.5)
        ).astype(float)
        features["regime_vol_stress"] = (
            (vix_z > 0.5) & (vvix_z > 0.5) &
            ~((vix_z > 1.5) & (vvix_z > 1.5))
        ).astype(float)
        features["regime_vol_uncertainty"] = (
            (vix_z > 0.5) & (vvix_z <= 0.5) |
            (vix_z <= 0.5) & (vvix_z > 0.5)
        ).astype(float)
        features["regime_vol_calm"] = (
            (vix_z <= 0) & (vvix_z <= 0)
        ).astype(float)
        for k in ["regime_vol_crisis", "regime_vol_stress",
                  "regime_vol_uncertainty", "regime_vol_calm"]:
            PRIORITY_SCORES[k] = 3
        n += 4

    if spx is not None:
        mom5 = spx.pct_change(5) * 100
        mom20 = spx.pct_change(20) * 100
        features["regime_spx_bull_strong"] = (
            (mom5 > 2) & (mom20 > 5)
        ).astype(float)
        features["regime_spx_bull_weak"] = (
            (mom5 > 0) & (mom20 > 0) &
            ~((mom5 > 2) & (mom20 > 5))
        ).astype(float)
        features["regime_spx_bear_strong"] = (
            (mom5 < -2) & (mom20 < -5)
        ).astype(float)
        features["regime_spx_bear_weak"] = (
            (mom5 < 0) & (mom20 < 0) &
            ~((mom5 < -2) & (mom20 < -5))
        ).astype(float)
        for k in ["regime_spx_bull_strong", "regime_spx_bull_weak",
                  "regime_spx_bear_strong", "regime_spx_bear_weak"]:
            PRIORITY_SCORES[k] = 2
        n += 4

    print(f"[cross_feat] Régimes composites: {n} features", flush=True)
    gc.collect()
    return features


# ════════════════════════════════════════════════════════════
# PIPELINE + SCORING
# ════════════════════════════════════════════════════════════

def build_all_cross_features(daily_csvs: dict,
                             index: pd.DatetimeIndex,
                             intraday_features: pd.DataFrame = None,
                             min_priority: int = 1) -> pd.DataFrame:
    """
    Construit toutes les features cross-asset et les trie par importance.
    """
    print(f"\n[cross_feat] Construction features cross-asset...", flush=True)

    f_a = build_vol_term_structure(daily_csvs, index)
    f_b = build_momentum_divergences(daily_csvs, index)
    f_c = build_options_flow_composite(daily_csvs, index)
    f_d = build_refuge_triangle(daily_csvs, index)
    f_e = build_cross_frequency_momentum(daily_csvs, index, intraday_features)
    f_f = build_composite_regimes(daily_csvs, index)

    all_feat = pd.concat([f_a, f_b, f_c, f_d, f_e, f_f], axis=1)
    all_feat = all_feat.loc[:, ~all_feat.columns.duplicated()]

    if min_priority > 1:
        keep = [c for c in all_feat.columns
                if PRIORITY_SCORES.get(c, 1) >= min_priority]
        all_feat = all_feat[keep]

    total = all_feat.shape[1]
    by_priority = {
        p: sum(1 for c in all_feat.columns if PRIORITY_SCORES.get(c, 1) == p)
        for p in [1, 2, 3]
    }
    print(f"[cross_feat] Total: {total} features cross-asset", flush=True)
    print(f"[cross_feat] Priorité 3 (forte): {by_priority.get(3, 0)} | "
          f"Priorité 2 (moyenne): {by_priority.get(2, 0)} | "
          f"Priorité 1 (faible): {by_priority.get(1, 0)}", flush=True)

    gc.collect()
    return all_feat


def get_priority_scores() -> dict:
    """Retourne le dictionnaire des scores de priorité."""
    return PRIORITY_SCORES.copy()
