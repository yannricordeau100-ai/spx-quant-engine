"""pead_analysis.py — Exploration multi-critères des signaux PEAD.

Trois variations de paramètres, buckets par market cap / secteur / mois /
jour de semaine / année / n_analysts / VIX régime / taille surprise.
Croisements 2-way et 3-way pour identifier les setups WR ≥ 75 %.

Usage : python3 pead_analysis.py
Sortie : data/pead/analysis_report.json + affichage texte.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from itertools import combinations
from pathlib import Path

import pandas as pd

from pead_engine import (
    backtest, backtest_summary, load_universe, _load_ohlcv, PEAD_DIR,
)

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data" / "live_selected"


# ─── Variations de paramètres ────────────────────────────────────────────

VARIATIONS = [
    ("V1_current", {"compression_mode": "avg", "compression_thr": 0.7, "surprise_thr": 0.10}),
    ("V2_stricter", {"compression_mode": "avg", "compression_thr": 0.6, "surprise_thr": 0.10}),
    ("V3_looser", {"compression_mode": "avg", "compression_thr": 0.8, "surprise_thr": 0.07}),
]


# ─── Enrichissement des signaux avec contexte ────────────────────────────

def _load_vix() -> pd.DataFrame:
    p = DATA_DIR / "VIX_daily.csv"
    if not p.exists():
        return pd.DataFrame()
    # Le format est "time;open;high;low;close" ou csv normal — auto-detect
    df = pd.read_csv(p, sep=None, engine="python")
    df.columns = [c.strip().lower() for c in df.columns]
    if "time" not in df.columns:
        return pd.DataFrame()
    df["time"] = pd.to_datetime(df["time"])
    return df[["time", "close"]].rename(columns={"close": "vix_close"})


def _enrich_signal(sig: pd.DataFrame, uni: pd.DataFrame, vix: pd.DataFrame) -> pd.DataFrame:
    """Ajoute : sector, n_analysts, market_cap_bucket, month, weekday, year,
    vix_regime, surprise_bucket, cap_bucket_10b, cap_bucket_100b."""
    if sig.empty:
        return sig
    sig = sig.copy()
    sig["j_date_dt"] = pd.to_datetime(sig["j_date"])
    sig["month"] = sig["j_date_dt"].dt.month
    sig["weekday"] = sig["j_date_dt"].dt.day_name()
    sig["year"] = sig["j_date_dt"].dt.year

    # Cap buckets
    def _cap_bucket(mc_b: float) -> str:
        if pd.isna(mc_b):
            return "unknown"
        if mc_b < 100:
            low = int(mc_b // 10) * 10
            return f"[{low}-{low+10}B]"
        low = int(mc_b // 100) * 100
        return f"[{low}-{low+100}B]"

    sig["cap_bucket"] = sig["market_cap_b"].apply(_cap_bucket)

    # Analyst buckets
    def _ana_bucket(n: int | float) -> str:
        n = int(n or 0)
        if n == 0: return "0"
        if n <= 5: return "1-5"
        if n <= 10: return "6-10"
        if n <= 15: return "11-15"
        if n <= 20: return "16-20"
        if n <= 30: return "21-30"
        return "31+"

    sig["analyst_bucket"] = sig["n_analysts"].apply(_ana_bucket)

    # Surprise magnitude (abs of var_j)
    sig["surprise_abs"] = sig["var_j_pct"].abs()
    def _surprise_bucket(x):
        if x < 12: return "10-12%"
        if x < 15: return "12-15%"
        if x < 20: return "15-20%"
        if x < 30: return "20-30%"
        return "30%+"
    sig["surprise_bucket"] = sig["surprise_abs"].apply(_surprise_bucket)

    # Beta bucket (volatilité du ticker)
    uni_beta = uni.set_index("ticker")["beta"] if "beta" in uni.columns else pd.Series(dtype=float)
    sig["beta"] = sig["ticker"].map(uni_beta)
    def _beta_bucket(b):
        if b is None or pd.isna(b): return "unknown"
        if b < 0.8: return "low_<0.8"
        if b < 1.2: return "normal_0.8-1.2"
        if b < 1.6: return "high_1.2-1.6"
        return "very_high_1.6+"
    sig["beta_bucket"] = sig["beta"].apply(_beta_bucket)

    # Pré-tendance 20 jours (close J-1 vs close J-20)
    pre_trend = []
    for _, r in sig.iterrows():
        tk = r["ticker"]
        j_dt = r["j_date_dt"]
        # On lit l'OHLCV juste-en-temps
        from pead_engine import _load_ohlcv
        oh = _load_ohlcv(tk)
        if oh.empty:
            pre_trend.append(None); continue
        past = oh[oh["time"] < j_dt].tail(21)  # J-20 à J-1
        if len(past) < 20:
            pre_trend.append(None); continue
        c0 = float(past.iloc[0]["close"])
        c_last = float(past.iloc[-1]["close"])
        pre_trend.append((c_last - c0) / c0 * 100 if c0 > 0 else None)
    sig["pre_trend_20d_pct"] = pre_trend
    def _trend_bucket(t):
        if t is None or pd.isna(t): return "unknown"
        if t < -10: return "strong_down_<-10%"
        if t < -3: return "down_-10_to_-3%"
        if t < 3: return "flat_-3_to_+3%"
        if t < 10: return "up_+3_to_+10%"
        return "strong_up_+10%+"
    sig["pre_trend_bucket"] = sig["pre_trend_20d_pct"].apply(_trend_bucket)

    # VIX régime à J
    if not vix.empty:
        vix_m = vix.set_index("time")["vix_close"]
        def _vix_at(d):
            try:
                ts = pd.Timestamp(d)
                # chercher la date la plus proche ≤
                v = vix_m.loc[:ts]
                return float(v.iloc[-1]) if len(v) else None
            except Exception:
                return None
        sig["vix_at_j"] = sig["j_date_dt"].apply(_vix_at)
        def _vix_bucket(v):
            if v is None or pd.isna(v): return "unknown"
            if v < 15: return "calm_<15"
            if v < 20: return "normal_15-20"
            if v < 25: return "elevated_20-25"
            if v < 30: return "stressed_25-30"
            return "crisis_30+"
        sig["vix_regime"] = sig["vix_at_j"].apply(_vix_bucket)

    return sig


# ─── Analyse par bucket ──────────────────────────────────────────────────

def analyze_by_bucket(sig: pd.DataFrame, col: str, min_n: int = 3) -> pd.DataFrame:
    """Groupe les signaux par valeur de `col`, calcule WR et avg PnL.
    Filtre les buckets avec moins de `min_n` signaux."""
    if sig.empty or col not in sig.columns:
        return pd.DataFrame()
    g = sig.groupby(col).agg(
        n=("pnl_pct", "size"),
        wr=("pnl_pct", lambda s: round((s > 0).mean() * 100, 1)),
        avg_pnl=("pnl_pct", lambda s: round(s.mean(), 2)),
        median_pnl=("pnl_pct", lambda s: round(s.median(), 2)),
    ).reset_index()
    g = g[g["n"] >= min_n].sort_values("wr", ascending=False)
    return g


def cross_analysis(sig: pd.DataFrame, cols: list[str], min_n: int = 3,
                    wr_threshold: float = 75.0) -> pd.DataFrame:
    """Cross 2-way ou 3-way : groupe par combinaison de `cols`, retourne
    les buckets avec WR ≥ wr_threshold et N ≥ min_n."""
    if sig.empty or any(c not in sig.columns for c in cols):
        return pd.DataFrame()
    g = sig.groupby(cols).agg(
        n=("pnl_pct", "size"),
        wr=("pnl_pct", lambda s: round((s > 0).mean() * 100, 1)),
        avg_pnl=("pnl_pct", lambda s: round(s.mean(), 2)),
    ).reset_index()
    g = g[(g["n"] >= min_n) & (g["wr"] >= wr_threshold)]
    g = g.sort_values(["wr", "avg_pnl"], ascending=[False, False])
    return g


# ─── Pipeline principal ──────────────────────────────────────────────────

def run_full_analysis() -> dict:
    uni = load_universe()
    vix = _load_vix()

    print(f"[analysis] Universe: {len(uni)} tickers | VIX series: {len(vix)} days")
    print()

    report: dict = {
        "universe": {
            "n_tickers": len(uni),
            "market_cap_median_b": round(uni["market_cap"].median() / 1e9, 2),
            "sectors": uni["sector_final"].value_counts().to_dict() if "sector_final" in uni.columns else {},
        },
        "variations": {},
    }

    # Pour éviter re-backtest, on exécute chaque variation 1x et on enrichit
    variation_sigs: dict[str, pd.DataFrame] = {}
    for name, params in VARIATIONS:
        print(f"[analysis] Backtest {name}: {params}")
        t0 = time.time()
        sig = backtest(**params)
        # Filtre Long only (stratégie de trading validée)
        if not sig.empty:
            sig_long = sig[sig["direction"] == "long"].copy()
        else:
            sig_long = sig
        sig_long = _enrich_signal(sig_long, uni, vix)
        variation_sigs[name] = sig_long
        print(f"  → {len(sig_long)} long signals in {time.time()-t0:.1f}s")
        sig_long.to_csv(PEAD_DIR / f"signals_{name}.csv", index=False, sep=";")
        report["variations"][name] = {
            "params": params,
            "n_long": len(sig_long),
            "summary": backtest_summary(sig_long),
        }

    # Analyse par critère × variation
    analysis_cols = [
        "cap_bucket", "sector", "month", "weekday", "year",
        "analyst_bucket", "surprise_bucket", "vix_regime",
        "beta_bucket", "pre_trend_bucket",
    ]
    # Note: la colonne "sector" dans les signaux vient de uni["sector_final"] ou "sector"

    print("\n" + "=" * 80)
    print("ANALYSES UNIVARIÉES (par critère, pour chaque variation)")
    print("=" * 80)
    univariate: dict = {}
    for vname, sig in variation_sigs.items():
        if sig.empty:
            continue
        print(f"\n--- Variation {vname} (n={len(sig)} long) ---")
        univariate[vname] = {}
        for col in analysis_cols:
            if col not in sig.columns:
                continue
            r = analyze_by_bucket(sig, col, min_n=3)
            if r.empty:
                continue
            univariate[vname][col] = r.to_dict(orient="records")
            print(f"\n  Par {col}:")
            print(r.head(10).to_string(index=False))

    report["univariate"] = univariate

    # Extract best 3 buckets per criterion (variation V1 par défaut)
    best_per_criterion: dict = {}
    v1_sig = variation_sigs.get("V1_current", pd.DataFrame())
    # Si V1 a trop peu, prend V3
    eff_sig = v1_sig if len(v1_sig) >= 10 else variation_sigs.get("V3_looser", pd.DataFrame())
    print("\n" + "=" * 80)
    print(f"BEST BUCKETS PER CRITERION (variation {'V1' if len(v1_sig) >= 10 else 'V3'})")
    print("=" * 80)
    for col in analysis_cols:
        if col not in eff_sig.columns:
            continue
        r = analyze_by_bucket(eff_sig, col, min_n=3)
        if r.empty:
            continue
        top3 = r.head(3)
        best_per_criterion[col] = top3.to_dict(orient="records")
        print(f"\n  {col} — top 3:")
        print(top3.to_string(index=False))
    report["best_per_criterion"] = best_per_criterion

    # Crossings 2-way et 3-way pour V1 (variation courante, la plus représentative)
    print("\n" + "=" * 80)
    print("CROISEMENTS 2-way (WR ≥ 75 % & N ≥ 3)")
    print("=" * 80)
    crosses: dict = {"two_way": [], "three_way": []}
    base_sig = variation_sigs.get("V1_current", pd.DataFrame())
    if not base_sig.empty:
        pair_cols = [c for c in analysis_cols if c in base_sig.columns]
        for pair in combinations(pair_cols, 2):
            r = cross_analysis(base_sig, list(pair), min_n=3, wr_threshold=75.0)
            if not r.empty:
                print(f"\n  {' × '.join(pair)}:")
                print(r.head(10).to_string(index=False))
                for _, row in r.iterrows():
                    crosses["two_way"].append({
                        "cols": list(pair),
                        **row.to_dict(),
                    })

        print("\n" + "=" * 80)
        print("CROISEMENTS 3-way (WR ≥ 75 % & N ≥ 3)")
        print("=" * 80)
        for triple in combinations(pair_cols, 3):
            r = cross_analysis(base_sig, list(triple), min_n=3, wr_threshold=75.0)
            if not r.empty:
                print(f"\n  {' × '.join(triple)}:")
                print(r.head(5).to_string(index=False))
                for _, row in r.iterrows():
                    crosses["three_way"].append({
                        "cols": list(triple),
                        **row.to_dict(),
                    })

    report["crosses"] = crosses

    # Setups WR ≥ 85 %
    elite = [c for c in crosses["two_way"] + crosses["three_way"] if c.get("wr", 0) >= 85.0]
    report["elite_setups_85"] = elite

    out_path = PEAD_DIR / "analysis_report.json"
    # Conversion JSON (pandas Timestamp → str)
    def _json_default(o):
        if hasattr(o, "isoformat"):
            return o.isoformat()
        return str(o)
    out_path.write_text(json.dumps(report, indent=2, default=_json_default, ensure_ascii=False))
    print(f"\n[analysis] Rapport complet : {out_path}")
    return report


if __name__ == "__main__":
    run_full_analysis()
