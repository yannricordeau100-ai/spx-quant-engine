"""pead_analysis_v2.py — Analyse adaptée aux contraintes :
- Min ~10 occurrences/an => N_min = 50 sur 5 ans
- Pas de filtre VIX trop restrictif (on teste large VIX ≥ 15 ou aucune VIX)
- Critères : cap_bucket, sector, month, weekday, year, analyst_bucket,
  surprise_bucket, beta_bucket, pre_trend_bucket + vix_bucket_broad

Usage : python3 pead_analysis_v2.py
"""

from __future__ import annotations

import json
from itertools import combinations
from pathlib import Path

import pandas as pd

from pead_analysis import _enrich_signal, _load_vix
from pead_engine import backtest, load_universe, PEAD_DIR


def _add_broad_vix(sig: pd.DataFrame) -> pd.DataFrame:
    """VIX bucket beaucoup plus large : 2 niveaux seulement."""
    if "vix_at_j" not in sig.columns:
        return sig
    def _broad(v):
        if v is None or pd.isna(v): return "unknown"
        return "vix_high_≥18" if v >= 18 else "vix_low_<18"
    sig["vix_broad"] = sig["vix_at_j"].apply(_broad)
    return sig


def analyze(sig: pd.DataFrame, col: str, min_n: int) -> pd.DataFrame:
    if sig.empty or col not in sig.columns:
        return pd.DataFrame()
    g = sig.groupby(col).agg(
        n=("pnl_pct", "size"),
        wr=("pnl_pct", lambda s: round((s > 0).mean() * 100, 1)),
        avg_pnl=("pnl_pct", lambda s: round(s.mean(), 2)),
    ).reset_index()
    g = g[g["n"] >= min_n].sort_values("wr", ascending=False)
    return g


def cross(sig: pd.DataFrame, cols: list[str], min_n: int, wr_thr: float) -> pd.DataFrame:
    if sig.empty or any(c not in sig.columns for c in cols):
        return pd.DataFrame()
    g = sig.groupby(cols).agg(
        n=("pnl_pct", "size"),
        wr=("pnl_pct", lambda s: round((s > 0).mean() * 100, 1)),
        avg_pnl=("pnl_pct", lambda s: round(s.mean(), 2)),
    ).reset_index()
    g = g[(g["n"] >= min_n) & (g["wr"] >= wr_thr)]
    return g.sort_values(["wr", "n", "avg_pnl"], ascending=[False, False, False])


def run(config_name: str, params: dict, n_year_min: int, years: int = 6,
        wr_thr: float = 75.0):
    """Run backtest + analysis for ONE config under constraints."""
    n_min = n_year_min * years
    print(f"\n{'='*80}\nCONFIG {config_name} — {params}")
    print(f"Contrainte : N_min = {n_min} ({n_year_min}/an × {years} ans)")
    print(f"{'='*80}")

    sig = backtest(**params)
    if sig.empty:
        print("→ aucun signal.")
        return None
    longs = sig[sig["direction"] == "long"].copy()
    print(f"Signaux Long totaux : {len(longs)} ({len(longs)/years:.1f}/an)")
    if len(longs) < n_min:
        print(f"⚠️  Moins de {n_min} signaux — config pas assez permissive.")

    uni = load_universe()
    vix = _load_vix()
    longs = _enrich_signal(longs, uni, vix)
    longs = _add_broad_vix(longs)

    from pead_engine import backtest_summary
    summary = backtest_summary(longs)
    print(f"Baseline : WR={summary.get('win_rate_pct')}% avg={summary.get('avg_pnl_pct')}%")

    # Critères de segmentation (pas de vix_regime strict)
    cols = [
        "cap_bucket", "sector", "month", "weekday", "year",
        "analyst_bucket", "surprise_bucket", "vix_broad",
        "beta_bucket", "pre_trend_bucket",
    ]
    cols = [c for c in cols if c in longs.columns]

    # Top univariate
    print(f"\n--- TOP univariate (N ≥ {n_min//5}, WR ≥ {wr_thr}%) ---")
    uni_min = max(5, n_min // 5)  # moins strict pour univarié (division par 5)
    for col in cols:
        r = analyze(longs, col, min_n=uni_min)
        r = r[r["wr"] >= wr_thr]
        if r.empty:
            continue
        print(f"\n  {col} :")
        print(r.head(5).to_string(index=False))

    # 2-way robustes (vrai N ≥ n_min)
    print(f"\n--- 2-way crosses (N ≥ {n_min}, WR ≥ {wr_thr}%) ---")
    found2 = []
    for pair in combinations(cols, 2):
        r = cross(longs, list(pair), min_n=n_min, wr_thr=wr_thr)
        if not r.empty:
            found2.append((pair, r))
            print(f"\n  {' × '.join(pair)} :")
            print(r.head(5).to_string(index=False))

    # 2-way moins stricts : N ≥ 10, pour voir les patterns même si statistiquement petits
    print(f"\n--- 2-way crosses (N ≥ 10 compromis, WR ≥ {wr_thr}%) ---")
    compromise_n = 10
    found2_comp = []
    for pair in combinations(cols, 2):
        r = cross(longs, list(pair), min_n=compromise_n, wr_thr=wr_thr)
        if not r.empty:
            found2_comp.append((pair, r))
    if found2_comp:
        all_rows = []
        for pair, r in found2_comp:
            for _, row in r.iterrows():
                all_rows.append({
                    "cols": " × ".join(pair),
                    **{c: row[c] for c in pair},
                    "n": row["n"], "wr": row["wr"], "avg_pnl": row["avg_pnl"],
                })
        df_all = pd.DataFrame(all_rows).sort_values(["n", "wr"], ascending=[False, False])
        print(df_all.head(20).to_string(index=False))

    # Setups élite ≥ 85%
    print(f"\n--- Elite setups (N ≥ 10, WR ≥ 85%) ---")
    elite_rows = []
    for pair in combinations(cols, 2):
        r = cross(longs, list(pair), min_n=10, wr_thr=85.0)
        for _, row in r.iterrows():
            elite_rows.append({
                "kind": "2-way", "cols": " × ".join(pair),
                **{c: row[c] for c in pair},
                "n": row["n"], "wr": row["wr"], "avg_pnl": row["avg_pnl"],
            })
    for triple in combinations(cols, 3):
        r = cross(longs, list(triple), min_n=10, wr_thr=85.0)
        for _, row in r.iterrows():
            elite_rows.append({
                "kind": "3-way", "cols": " × ".join(triple),
                **{c: row[c] for c in triple},
                "n": row["n"], "wr": row["wr"], "avg_pnl": row["avg_pnl"],
            })
    if elite_rows:
        df_el = pd.DataFrame(elite_rows).sort_values(["n", "wr"], ascending=[False, False])
        print(df_el.to_string(index=False))

    return {
        "config_name": config_name,
        "params": params,
        "n_long_total": len(longs),
        "signals_per_year": round(len(longs) / years, 1),
        "baseline_wr": summary.get("win_rate_pct"),
        "baseline_avg_pnl": summary.get("avg_pnl_pct"),
    }


if __name__ == "__main__":
    import sys
    # Par défaut : la config ciblée ~10-20 signaux/an
    # Config C = sweet spot (17.5/an, WR 61%, +2.06% — identifié au sweep)
    configs_to_run = [
        ("C_avg<0.75_±8%", {"compression_mode":"avg","compression_thr":0.75,"surprise_thr":0.08}),
    ]
    results = []
    for name, params in configs_to_run:
        r = run(name, params, n_year_min=10, years=6, wr_thr=75.0)
        if r:
            results.append(r)
    print("\n\n=== SYNTHÈSE ===")
    for r in results:
        print(f"  {r['config_name']}: {r['n_long_total']} longs ({r['signals_per_year']}/an) "
              f"WR={r['baseline_wr']}% avg={r['baseline_avg_pnl']}%")
