"""
options_validator.py — Validation économique des patterns via options chains 0DTE SPX.

Convention projet :
  RIC = CallAsk(K) + PutAsk(K) - CallBid(K+w) - PutBid(K-w)   [débit]
  IC  = CallBid(K) + PutBid(K) - CallAsk(K+w) - PutAsk(K-w)   [crédit]
  RIB = CallAsk(K+i) + PutAsk(K-i) - CallBid(K+o) - PutBid(K-o)  [débit, zone tampon ±i]
  IB  = CallBid(K+i) + PutBid(K-i) - CallAsk(K+o) - PutAsk(K-o)  [crédit, zone tampon ±i]

ATM = strike dont abs(CallDelta-0.5) + abs(abs(PutDelta)-0.5) est minimal.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Optional

# Chains disponibles : VIX level → filename (les fichiers utilisent des points)
CHAIN_FILES = {
    14.16: "VIX_14.16_option_chain_export.csv",
    15.09: "VIX_15.09_option_chain_full.csv",
    16.02: "VIX_16.02_option_chain_full.csv",
    16.30: "VIX_16.30_option_chain_full.csv",
    17.16: "VIX_17.16_option_chain_COMPLETE_WITH_DELTAS.csv",
    18.80: "VIX_18.80_option_chain_full.csv",
    19.04: "VIX_19.04_option_chain_full.csv",
    20.50: "VIX_20.50_option_chain_full.csv",
    22.36: "VIX_22.36_option_chain_full.csv",
    24.74: "VIX_24.74_option_chain_COMPLETE_WITH_DELTAS_REDO.csv",
    24.90: "VIX_24.90_option_chain_full.csv",
    25.91: "VIX_25.91_option_chain_full.csv",
    27.03: "VIX_27.03_option_chain_full.csv",
    30.80: "VIX_30.80_option_chain_full.csv",
}

# Seuils de rentabilité
MIN_GAIN_EXCELLENT = 8.0
MIN_GAIN_VIABLE    = 3.0
MIN_GAIN_MARGINAL  = 0.5

_CHAINS_CACHE: Dict[float, pd.DataFrame] = {}


def _load_chain(vix_level: float, data_dir: Path) -> Optional[pd.DataFrame]:
    if vix_level in _CHAINS_CACHE:
        return _CHAINS_CACHE[vix_level]
    fname = CHAIN_FILES.get(vix_level)
    if not fname:
        return None
    path = data_dir / fname
    if not path.exists():
        return None
    df = pd.read_csv(path, sep=';')
    df['atm_score'] = (df['CallDelta'] - 0.5).abs() + (df['PutDelta'].abs() - 0.5).abs()
    _CHAINS_CACHE[vix_level] = df
    return df


def _get_nearest_vix(vix: float) -> float:
    levels = sorted(CHAIN_FILES.keys())
    return min(levels, key=lambda x: abs(x - vix))


def _get_row(df: pd.DataFrame, strike: float) -> pd.Series:
    return df.iloc[(df['Strike'] - strike).abs().argsort()].iloc[0]


def calc_ric(df: pd.DataFrame, K: float, w: float) -> float:
    """RIC = CallAsk(K) + PutAsk(K) - CallBid(K+w) - PutBid(K-w)"""
    atm = _get_row(df, K)
    upper = _get_row(df, K + w)
    lower = _get_row(df, K - w)
    return round(atm['CallAsk'] + atm['PutAsk'] - upper['CallBid'] - lower['PutBid'], 2)


def calc_ic(df: pd.DataFrame, K: float, w: float) -> float:
    """IC = CallBid(K) + PutBid(K) - CallAsk(K+w) - PutAsk(K-w)"""
    atm = _get_row(df, K)
    upper = _get_row(df, K + w)
    lower = _get_row(df, K - w)
    return round(atm['CallBid'] + atm['PutBid'] - upper['CallAsk'] - lower['PutAsk'], 2)


def calc_rib(df: pd.DataFrame, K: float, inner_w: float, outer_w: float) -> float:
    """RIB = CallAsk(K+i) + PutAsk(K-i) - CallBid(K+o) - PutBid(K-o)"""
    inner_c = _get_row(df, K + inner_w)
    inner_p = _get_row(df, K - inner_w)
    outer_c = _get_row(df, K + outer_w)
    outer_p = _get_row(df, K - outer_w)
    return round(inner_c['CallAsk'] + inner_p['PutAsk'] - outer_c['CallBid'] - outer_p['PutBid'], 2)


def calc_ib(df: pd.DataFrame, K: float, inner_w: float, outer_w: float) -> float:
    """IB = CallBid(K+i) + PutBid(K-i) - CallAsk(K+o) - PutAsk(K-o)"""
    inner_c = _get_row(df, K + inner_w)
    inner_p = _get_row(df, K - inner_w)
    outer_c = _get_row(df, K + outer_w)
    outer_p = _get_row(df, K - outer_w)
    return round(inner_c['CallBid'] + inner_p['PutBid'] - outer_c['CallAsk'] - outer_p['PutAsk'], 2)


def _verdict(max_gain: float) -> str:
    if max_gain >= MIN_GAIN_EXCELLENT:
        return "✅✅ Excellent"
    if max_gain >= MIN_GAIN_VIABLE:
        return "✅ Viable"
    if max_gain >= MIN_GAIN_MARGINAL:
        return "🟡 Marginal"
    return "❌ Non rentable"


def validate_pattern(
    vix_level: float,
    data_dir: Path,
    wings: list = None,
    spot: float = 6500.0,
) -> dict:
    """Valide économiquement un pattern pour un niveau VIX donné."""
    if wings is None:
        wings = [
            ('pur', 30, None),
            ('pur', 40, None),
            ('zone', 20, 40),
            ('zone', 15, 40),
        ]

    nearest_vix = _get_nearest_vix(vix_level)
    df = _load_chain(nearest_vix, data_dir)

    if df is None:
        return {'ok': False, 'error': f'Chain non disponible pour VIX {vix_level}'}

    K = float(df.loc[df['atm_score'].idxmin(), 'Strike'])

    results = {
        'ok': True,
        'vix_requested': vix_level,
        'vix_chain_used': nearest_vix,
        'atm_strike': K,
        'configs': {},
        'best_strategy': None,
        'best_gain': -999,
    }

    for config in wings:
        style, w1, w2 = config

        if style == 'pur':
            w = w1
            ric = calc_ric(df, K, w)
            ic = calc_ic(df, K, w)
            ric_gain = round(w - ric, 2)
            ic_gain = round(ic, 2)

            label = f"pur±{w}"
            results['configs'][label] = {
                'style': 'pur',
                'wing': w,
                'RIC_debit': ric,
                'RIC_max_gain': ric_gain,
                'RIC_verdict': _verdict(ric_gain),
                'IC_credit': ic,
                'IC_max_gain': ic_gain,
                'IC_verdict': _verdict(ic_gain),
            }

            for strat, gain in [('RIC', ric_gain), ('IC', ic_gain)]:
                if gain > results['best_gain']:
                    results['best_gain'] = gain
                    results['best_strategy'] = f"{strat} {label}"

        elif style == 'zone':
            inner, outer = w1, w2
            rib = calc_rib(df, K, inner, outer)
            ib = calc_ib(df, K, inner, outer)
            rib_gain = round((outer - inner) - rib, 2)
            ib_gain = round(ib, 2)

            label = f"±{inner}→±{outer}"
            results['configs'][label] = {
                'style': 'zone',
                'inner_wing': inner,
                'outer_wing': outer,
                'zone_safe': f"K±{inner}pts",
                'RIB_debit': rib,
                'RIB_max_gain': rib_gain,
                'RIB_verdict': _verdict(rib_gain),
                'IB_credit': ib,
                'IB_max_gain': ib_gain,
                'IB_verdict': _verdict(ib_gain),
            }

            for strat, gain in [('RIB', rib_gain), ('IB', ib_gain)]:
                if gain > results['best_gain']:
                    results['best_gain'] = gain
                    results['best_strategy'] = f"{strat} {label}"

    return results


def validate_all_vix(data_dir: Path) -> dict:
    """Calcule les métriques pour tous les niveaux VIX disponibles."""
    return {vix: validate_pattern(vix, data_dir) for vix in sorted(CHAIN_FILES.keys())}


def get_economic_verdict_for_pattern(
    pattern_vix_range: tuple,
    strategy: str,
    data_dir: Path,
    wing: int = 40,
) -> str:
    """Verdict économique rapide pour un pattern donné."""
    vix_mid = (pattern_vix_range[0] + pattern_vix_range[1]) / 2
    result = validate_pattern(vix_mid, data_dir)

    if not result['ok']:
        return "❓ Données insuffisantes"

    for label, cfg in result['configs'].items():
        if strategy == 'RIC' and cfg['style'] == 'pur' and cfg.get('wing') == wing:
            g = cfg['RIC_max_gain']
            return f"{cfg['RIC_verdict']} (gain max {g:.1f}pts si SPX bouge ≥{wing}pts)"
        elif strategy == 'IC' and cfg['style'] == 'pur' and cfg.get('wing') == wing:
            g = cfg['IC_max_gain']
            return f"{cfg['IC_verdict']} (crédit {g:.1f}pts si SPX reste dans ±{wing}pts)"
        elif strategy == 'RIB' and cfg['style'] == 'zone':
            g = cfg['RIB_max_gain']
            return f"{cfg['RIB_verdict']} (gain max {g:.1f}pts)"
        elif strategy == 'IB' and cfg['style'] == 'zone':
            g = cfg['IB_max_gain']
            return f"{cfg['IB_verdict']} (crédit {g:.1f}pts)"

    return "❓ Config non trouvée"


# Données précalculées pour l'app
PRECOMPUTED = {
    14.16: {'RIC_pur30': 11.9, 'RIC_pur40': 20.2, 'IC_pur30': 17.4, 'IC_pur40': 19.2, 'RIB_20_40': 14.6, 'IB_20_40': 5.2},
    15.09: {'RIC_pur30': 9.2,  'RIC_pur40': 16.3, 'IC_pur30': 19.9, 'IC_pur40': 22.9, 'RIB_20_40': 12.1, 'IB_20_40': 7.3},
    16.02: {'RIC_pur30': 8.5,  'RIC_pur40': 15.9, 'IC_pur30': 20.9, 'IC_pur40': 23.6, 'RIB_20_40': 12.8, 'IB_20_40': 6.7},
    16.30: {'RIC_pur30': 10.7, 'RIC_pur40': 18.1, 'IC_pur30': 18.7, 'IC_pur40': 21.2, 'RIB_20_40': 13.0, 'IB_20_40': 6.6},
    17.16: {'RIC_pur30': 7.6,  'RIC_pur40': 13.5, 'IC_pur30': 21.6, 'IC_pur40': 25.8, 'RIB_20_40': 10.1, 'IB_20_40': 9.5},
    18.80: {'RIC_pur30': 6.7,  'RIC_pur40': 12.5, 'IC_pur30': 22.4, 'IC_pur40': 26.7, 'RIB_20_40': 9.5,  'IB_20_40': 9.7},
    19.04: {'RIC_pur30': 2.6,  'RIC_pur40': 6.4,  'IC_pur30': 24.8, 'IC_pur40': 31.0, 'RIB_20_40': 5.2,  'IB_20_40': 12.2},
    22.36: {'RIC_pur30': 6.6,  'RIC_pur40': 12.2, 'IC_pur30': 22.7, 'IC_pur40': 27.0, 'RIB_20_40': 9.5,  'IB_20_40': 9.7},
    24.74: {'RIC_pur30': -2.0, 'RIC_pur40': 0.1,  'IC_pur30': 28.7, 'IC_pur40': 36.6, 'RIB_20_40': 1.6,  'IB_20_40': 15.2},
    24.90: {'RIC_pur30': 3.8,  'RIC_pur40': 7.3,  'IC_pur30': 24.8, 'IC_pur40': 31.5, 'RIB_20_40': 5.7,  'IB_20_40': 13.2},
    25.91: {'RIC_pur30': 4.3,  'RIC_pur40': 8.1,  'IC_pur30': 24.7, 'IC_pur40': 30.9, 'RIB_20_40': 5.8,  'IB_20_40': 13.2},
    27.03: {'RIC_pur30': 0.4,  'RIC_pur40': -0.7, 'IC_pur30': 25.6, 'IC_pur40': 28.7, 'RIB_20_40': -1.7, 'IB_20_40': 9.6},
    30.80: {'RIC_pur30': 4.0,  'RIC_pur40': 8.1,  'IC_pur30': 25.2, 'IC_pur40': 31.0, 'RIB_20_40': 6.2,  'IB_20_40': 12.8},
}


def interpolate_gains(vix: float) -> dict:
    """Interpolation linéaire entre les niveaux VIX disponibles."""
    levels = sorted(PRECOMPUTED.keys())
    if vix <= levels[0]:
        return PRECOMPUTED[levels[0]]
    if vix >= levels[-1]:
        return PRECOMPUTED[levels[-1]]
    for i in range(len(levels) - 1):
        if levels[i] <= vix <= levels[i + 1]:
            lo, hi = levels[i], levels[i + 1]
            t = (vix - lo) / (hi - lo)
            result = {}
            for k in PRECOMPUTED[lo]:
                result[k] = round(PRECOMPUTED[lo][k] * (1 - t) + PRECOMPUTED[hi][k] * t, 2)
            return result
    return PRECOMPUTED[levels[-1]]


def simulate_pattern_economics(
    pattern_occurrences: list,
    daily_data_dir: Path,
    chain_data_dir: Path,
    structure_configs: list = None,
) -> dict:
    """
    Pour chaque occurrence historique d'un pattern, calcule le P&L
    réel de chaque structure options en utilisant le VIX open du jour
    et l'amplitude réelle du SPX.
    """
    if structure_configs is None:
        structure_configs = [
            ('RIC', 40, None),
            ('IC',  40, None),
            ('RIB', 20, 40),
            ('IB',  20, 40),
        ]

    vix_open_series = None
    for fname in ['VIX_daily.csv', 'vix_daily.csv']:
        p = daily_data_dir / fname
        if p.exists():
            df = pd.read_csv(p, sep=';')
            df['time'] = pd.to_datetime(df['time'].astype(str).str.strip(),
                                        errors='coerce')
            df = df.dropna(subset=['time']).set_index('time')
            df.index = df.index.normalize()
            if 'open' in df.columns:
                vix_open_series = df['open']
            break

    spx_df = None
    for fname in ['SPX_daily.csv', 'spx_daily.csv']:
        p = daily_data_dir / fname
        if p.exists():
            df = pd.read_csv(p, sep=';')
            df['time'] = pd.to_datetime(df['time'].astype(str).str.strip(),
                                        errors='coerce')
            df = df.dropna(subset=['time']).set_index('time')
            df.index = df.index.normalize()
            spx_df = df
            break

    results_by_structure = {
        cfg[0] + (f'_{cfg[1]}' if cfg[2] is None else f'_{cfg[1]}_{cfg[2]}'): []
        for cfg in structure_configs
    }

    session_details = []

    for session_date in pattern_occurrences:
        d = pd.Timestamp(session_date).normalize()

        vix_open = None
        if vix_open_series is not None and d in vix_open_series.index:
            vix_open = float(vix_open_series[d])

        if vix_open is None:
            continue

        spx_amplitude = None
        spx_open = None
        spx_close = None
        spx_high = None
        spx_low = None
        spx_move_abs = None
        if spx_df is not None and d in spx_df.index:
            row = spx_df.loc[d]
            if 'open' in spx_df.columns and 'close' in spx_df.columns:
                spx_open = float(row['open'])
                spx_close = float(row['close'])
                spx_high = float(row.get('high', spx_close))
                spx_low = float(row.get('low', spx_close))
                spx_amplitude = spx_high - spx_low
                spx_move_abs = abs(spx_close - spx_open)

        if spx_amplitude is None:
            continue

        gains = interpolate_gains(vix_open)

        session_row = {
            'date': d,
            'vix_open': vix_open,
            'spx_amplitude_hl': round(spx_amplitude, 1),
            'spx_move_abs': round(spx_move_abs, 1),
            'spx_open': spx_open,
            'spx_close': spx_close,
        }

        for cfg in structure_configs:
            strat, w1, w2 = cfg
            key = strat + (f'_{w1}' if w2 is None else f'_{w1}_{w2}')

            if strat == 'RIC':
                debit = gains.get('RIC_pur40', 0) if w1 == 40 \
                        else gains.get('RIC_pur30', 0)
                max_gain = w1 - debit
                # RIC : gain si le SPX se déplace nettement (close-open)
                move = spx_move_abs
                if move >= w1:
                    pnl = max_gain
                elif move >= w1 * 0.5:
                    frac = (move - w1 * 0.5) / (w1 * 0.5)
                    pnl = round(frac * max_gain - (1 - frac) * debit, 2)
                else:
                    pnl = round(-debit * (1 - move / (w1 * 0.5)) * 0.8, 2)
                    pnl = max(pnl, -debit)

            elif strat == 'IC':
                credit = gains.get('IC_pur40', 0) if w1 == 40 \
                         else gains.get('IC_pur30', 0)
                max_gain = credit
                # IC : perdant si HIGH-LOW dépasse l'aile
                move = spx_amplitude_hl
                if move <= w1 * 0.6:
                    pnl = max_gain
                elif move <= w1:
                    frac = (w1 - move) / (w1 * 0.4)
                    pnl = round(frac * max_gain, 2)
                else:
                    excess = move - w1
                    pnl = round(max_gain - excess * 1.2, 2)
                    pnl = max(pnl, -(w1 - max_gain))

            elif strat == 'RIB':
                inner, outer = w1, w2
                debit = gains.get('RIB_20_40', 0)
                wing_width = outer - inner
                max_gain = wing_width - debit
                # RIB : mouvement directionnel (close-open)
                move = spx_move_abs
                if move >= outer:
                    pnl = max_gain
                elif move >= inner:
                    frac = (move - inner) / (outer - inner)
                    pnl = round(frac * max_gain, 2)
                else:
                    pnl = round(-debit * (1 - move / inner) if inner > 0 else -debit, 2)
                    pnl = max(pnl, -debit)

            elif strat == 'IB':
                inner, outer = w1, w2
                credit = gains.get('IB_20_40', 0)
                max_gain = credit
                # IB : calme mesuré par HIGH-LOW
                move = spx_amplitude_hl
                if move <= inner:
                    pnl = max_gain
                elif move <= outer:
                    frac = (outer - move) / (outer - inner)
                    pnl = round(frac * max_gain, 2)
                else:
                    excess = move - outer
                    pnl = round(max_gain - excess, 2)
                    pnl = max(pnl, -(outer - max_gain))

            else:
                pnl = 0

            results_by_structure[key].append(pnl)
            session_row[f'pnl_{key}'] = round(pnl, 2)

        session_details.append(session_row)

    stats = {}
    for key, pnls in results_by_structure.items():
        if not pnls:
            continue
        arr = np.array(pnls)
        stats[key] = {
            'n_trades': len(arr),
            'win_rate': round(float((arr > 0).mean() * 100), 1),
            'avg_pnl': round(float(arr.mean()), 2),
            'total_pnl': round(float(arr.sum()), 2),
            'max_gain': round(float(arr.max()), 2),
            'max_loss': round(float(arr.min()), 2),
            'pnls': [round(float(x), 2) for x in arr],
        }

    return {
        'sessions': session_details,
        'stats': stats,
        'best_structure': max(stats, key=lambda k: stats[k]['avg_pnl']) if stats else None,
    }


def get_spx_amplitude_distribution(daily_data_dir: Path,
                                   vix_min: float = None,
                                   vix_max: float = None) -> dict:
    """
    Distribution des amplitudes SPX (high-low et close-open abs)
    selon le régime VIX.
    """
    vix_df = None
    spx_df = None

    for fname in ['VIX_daily.csv']:
        p = daily_data_dir / fname
        if p.exists():
            df = pd.read_csv(p, sep=';')
            df['time'] = pd.to_datetime(df['time'].astype(str).str.strip(),
                                        errors='coerce')
            df = df.dropna(subset=['time']).set_index('time')
            df.index = df.index.normalize()
            vix_df = df
            break

    for fname in ['SPX_daily.csv']:
        p = daily_data_dir / fname
        if p.exists():
            df = pd.read_csv(p, sep=';')
            df['time'] = pd.to_datetime(df['time'].astype(str).str.strip(),
                                        errors='coerce')
            df = df.dropna(subset=['time']).set_index('time')
            df.index = df.index.normalize()
            spx_df = df
            break

    if vix_df is None or spx_df is None:
        return {}

    merged = spx_df.copy()
    if 'open' in vix_df.columns:
        merged['vix_open'] = vix_df['open'].reindex(merged.index, method='ffill')

    if vix_min is not None:
        merged = merged[merged['vix_open'] >= vix_min]
    if vix_max is not None:
        merged = merged[merged['vix_open'] <= vix_max]

    if len(merged) == 0:
        return {}

    if 'high' in merged.columns and 'low' in merged.columns:
        hl = (merged['high'] - merged['low']).dropna()
    else:
        hl = pd.Series(dtype=float)

    if 'open' in merged.columns and 'close' in merged.columns:
        co = (merged['close'] - merged['open']).abs().dropna()
    else:
        co = pd.Series(dtype=float)

    def pct_above(series, threshold):
        if len(series) == 0:
            return 0
        return round(float((series >= threshold).mean() * 100), 1)

    thresholds = [10, 15, 20, 25, 30, 35, 40, 50, 60]

    return {
        'n_sessions': len(merged),
        'vix_range': (vix_min, vix_max),
        'hl_median': round(float(hl.median()), 1) if len(hl) > 0 else None,
        'hl_mean': round(float(hl.mean()), 1) if len(hl) > 0 else None,
        'co_median': round(float(co.median()), 1) if len(co) > 0 else None,
        'pct_hl_above': {t: pct_above(hl, t) for t in thresholds},
        'pct_co_above': {t: pct_above(co, t) for t in thresholds},
    }
