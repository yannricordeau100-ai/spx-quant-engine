"""
patterns_v2.py — SPX Quant Engine v2.5.0
Module de détection automatique de patterns statistiques.

Usage (appelé depuis app_local.py) :
    from patterns_v2 import explore_patterns
    explore_patterns(["VIX", "VIX1D/VIX"], target="SPX", max_combos=1000)
    # → tourne en background (Thread daemon) et écrit data/patterns_results.json
"""

from __future__ import annotations

import json
import re
import threading
import time
from itertools import product
from pathlib import Path

import numpy as np
import pandas as pd
import pytz
from scipy.stats import binomtest

# ─── Paths & constantes ───────────────────────────────────────────────────

BASE_DIR  = Path(__file__).resolve().parent
DATA_DIR  = BASE_DIR / "data" / "live_selected"
OUT_FILE  = BASE_DIR / "data" / "patterns_results.json"

_TZ_PARIS = pytz.timezone("Europe/Paris")
_TZ_NY    = pytz.timezone("America/New_York")

# CSV en heure Paris (intraday)
_PARIS_INTRADAY = {
    "SPX_1min.csv","SPX_5min.csv","SPX_30min.csv",
    "SPY_1min.csv","SPY_30min.csv",
    "QQQ_1_min.csv","QQQ_30min.csv",
    "IWM_30_min.csv",
    "VIX1D_1min.csv","VIX1D_30min.csv",
    "SPX_FUTURE_1min.csv","SPX_FUTURE_5min.csv","SPX_FUTURE_30min.csv",
    "Gold_1hour.csv","oil_5min.csv","TICK_4hours.csv",
    "DAX40_daily.csv","FTSE100_daily.csv","NIKKEI225_daily.csv",
    "Gold_daily.csv","DXY_daily.csv","Yield_Curve_Spread_10Y_2Y.csv",
}

# Patterns de noms de fichier par actif (regex sur stem)
_ASSET_FILE_PATTERNS: dict[str, list[str]] = {
    "VIX":       [r"^VIX_daily\.csv$", r"^VIX_30min\.csv$"],
    "VIX1D/VIX": [r"^VIX1D_VIX_ratio_daily\.csv$"],
    "VIX1D":     [r"^VIX1D_1min\.csv$", r"^VIX1D_30min\.csv$"],
    "VVIX":      [r"^VVIX_daily\.csv$"],
    "VIX9D":     [r"^VIX9D_daily\.csv$"],
    "VIX3M":     [r"^VIX3M_daily\.csv$"],
    "SKEW":      [r"^SKEW_INDEX_daily\.csv$"],
    "DXY":       [r"^DXY_daily\.csv$"],
    "GOLD":      [r"^Gold_daily\.csv$"],
    "SPX":       [r"^SPX_daily\.csv$", r"^SPX_30min\.csv$"],
    "QQQ":       [r"^QQQ_daily\.csv$", r"^QQQ_30min\.csv$"],
    "SPY":       [r"^SPY_daily\.csv$", r"^SPY_30min\.csv$"],
}

# Nombre de percentiles candidats comme seuils (par direction)
N_THRESHOLDS = 8

# Horizons évalués en plus de open→close daily
HORIZONS = ["open_next", "30min", "60min", "120min", "close"]

MIN_OCCURRENCES = 30
P_VALUE_THRESHOLD = 0.05
TRAIN_RATIO = 0.70

# ─── Chargement CSV ───────────────────────────────────────────────────────

def _paris_to_ny(df: pd.DataFrame) -> pd.DataFrame:
    if df["time"].dt.tz is not None or df["time"].dt.hour.max() == 0:
        return df
    try:
        df = df.copy()
        df["time"] = (
            df["time"]
            .dt.tz_localize(_TZ_PARIS, ambiguous="NaT", nonexistent="NaT")
            .dt.tz_convert(_TZ_NY)
            .dt.tz_localize(None)
        )
    except Exception:
        pass
    return df


def _load_file(fname: str) -> pd.DataFrame | None:
    path = DATA_DIR / fname
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, sep=";")
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
        df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
        df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
        if fname in _PARIS_INTRADAY:
            df = _paris_to_ny(df)
        # Convertit toutes les colonnes numériques
        for col in df.columns:
            if col == "time":
                continue
            conv = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".").str.strip(), errors="coerce"
            )
            if conv.notna().mean() >= 0.3:
                df[col] = conv
        return df
    except Exception as e:
        print(f"[patterns] skip {fname}: {e}", flush=True)
        return None


def _discover_files(asset: str) -> list[tuple[str, str]]:
    """Retourne [(fname, freq)] pour l'actif donné."""
    patterns = _ASSET_FILE_PATTERNS.get(asset.upper(), [])
    if not patterns:
        # Tentative générique : cherche *ASSET*daily* ou *ASSET*
        key = asset.upper().replace("/", "_").replace("1D", "1D")
        patterns = [rf"(?i).*{re.escape(key)}.*\.csv$"]

    result = []
    for fname in sorted(DATA_DIR.glob("*.csv")):
        name = fname.name
        for pat in patterns:
            if re.match(pat, name, re.IGNORECASE):
                freq = ("daily" if "daily" in name.lower()
                        else "30min" if "30min" in name.lower()
                        else "1min"  if "1min"  in name.lower()
                        else "other")
                result.append((name, freq))
                break
    return result


# ─── Seuils candidats ─────────────────────────────────────────────────────

def _candidate_thresholds(series: pd.Series) -> list[float]:
    s = series.dropna()
    if len(s) < 30:
        return []
    pcts = np.linspace(10, 90, N_THRESHOLDS)
    raw = np.percentile(s, pcts)
    # Déduplique et arrondit à 2 décimales
    seen: set[float] = set()
    out: list[float] = []
    for v in raw:
        v = round(float(v), 2)
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


# ─── Horizons cibles pour le SPX ─────────────────────────────────────────

def _build_target_series(
    spx_daily: pd.DataFrame,
    spx_30min: pd.DataFrame | None,
) -> pd.DataFrame:
    """
    Construit un DataFrame indexé par date (normalisée) avec les colonnes :
      open_next   : open du lendemain
      30min       : prix 30min après ouverture
      60min       : prix 60min après ouverture (si dispo)
      120min      : prix 120min après ouverture (si dispo)
      close       : close du jour
    Toutes normalisées comme variation % depuis open du jour.
    """
    d = spx_daily.copy()
    d["date"] = d["time"].dt.normalize()
    d = d.set_index("date").sort_index()

    open_s  = d["open"]
    close_s = d["close"]

    result: dict[str, pd.Series] = {}

    # Variation open→close du jour
    result["close"] = (close_s - open_s) / open_s * 100

    # Open J+1 (overnight)
    result["open_next"] = (open_s.shift(-1) - close_s) / close_s * 100

    # Intraday depuis SPX_30min
    if spx_30min is not None:
        intra = spx_30min.copy()
        intra["date"] = intra["time"].dt.normalize()
        intra["minute"] = (
            intra["time"].dt.hour * 60 + intra["time"].dt.minute
        )
        # Ouverture NY = 9h30 = 570 min
        open_bar  = intra[intra["minute"] == 570].set_index("date")["open"]
        bar_30    = intra[intra["minute"] == 600].set_index("date")["close"]   # 10:00
        bar_60    = intra[intra["minute"] == 630].set_index("date")["close"]   # 10:30
        bar_120   = intra[intra["minute"] == 690].set_index("date")["close"]   # 11:30

        for col, bar in [("30min", bar_30), ("60min", bar_60), ("120min", bar_120)]:
            pct = (bar - open_bar) / open_bar * 100
            result[col] = pct

    return pd.DataFrame(result)


# ─── Test d'un pattern ────────────────────────────────────────────────────

def _test_single(
    cond_dates: set,
    target_df: pd.DataFrame,
    base_rate: float,
    horizon: str,
) -> dict | None:
    """
    Pour un ensemble de dates où la condition est vraie,
    teste si la direction SPX (hausse = 1 / baisse = 0) diffère significativement
    du base_rate via binomtest.
    Retourne un dict de stats ou None si pas significatif.
    """
    if horizon not in target_df.columns:
        return None
    pct = target_df.loc[target_df.index.isin(cond_dates), horizon].dropna()
    n = len(pct)
    if n < MIN_OCCURRENCES:
        return None
    bull = int((pct > 0).sum())
    p_val = binomtest(bull, n, p=base_rate, alternative="two-sided").pvalue
    if p_val >= P_VALUE_THRESHOLD:
        return None
    return {
        "n": n,
        "bull": bull,
        "pct_bull": round(bull / n * 100, 2),
        "mean_ret": round(float(pct.mean()), 4),
        "p_value": round(float(p_val), 6),
    }


# ─── Validation OOS ───────────────────────────────────────────────────────

def _validate_oos(
    cond_dates_sorted: list,
    target_df: pd.DataFrame,
    horizon: str,
    base_rate: float,
) -> dict | None:
    """Split temporel 70/30. Retourne stats OOS ou None si non confirmé."""
    n_train = int(len(cond_dates_sorted) * TRAIN_RATIO)
    oos_dates = set(cond_dates_sorted[n_train:])
    return _test_single(oos_dates, target_df, base_rate, horizon)


# ─── Pipeline principal ───────────────────────────────────────────────────

def explore_patterns(
    assets: list[str],
    target: str = "SPX",
    max_combos: int = 1000,
    session_state=None,          # st.session_state si appelé depuis Streamlit
) -> None:
    """
    Fonction principale. Doit être lancée dans un Thread daemon.
    Écrit les résultats dans data/patterns_results.json.
    Met session_state["patterns_ready"] = True quand terminé.
    """
    t_start = time.time()
    print(f"[patterns] Démarrage exploration — actifs={assets} target={target} max_combos={max_combos}", flush=True)

    # ── Chargement données cibles (SPX) ───────────────────────────────────
    spx_daily_files = _discover_files("SPX")
    spx_daily_df: pd.DataFrame | None = None
    spx_30min_df:  pd.DataFrame | None = None
    for fname, freq in spx_daily_files:
        df = _load_file(fname)
        if df is None:
            continue
        if freq == "daily" and spx_daily_df is None:
            spx_daily_df = df
        elif freq == "30min" and spx_30min_df is None:
            spx_30min_df = df

    if spx_daily_df is None:
        print("[patterns] SPX_daily.csv introuvable — abandon.", flush=True)
        return

    target_df = _build_target_series(spx_daily_df, spx_30min_df)
    # Base rate (% jours haussiers sur l'ensemble de l'historique)
    base_rate = float((target_df["close"].dropna() > 0).mean())
    print(f"[patterns] Base rate SPX close: {base_rate:.3f} ({len(target_df)} jours)", flush=True)

    # ── Chargement données conditions ─────────────────────────────────────
    cond_series_list: list[dict] = []
    for asset in assets:
        files = _discover_files(asset)
        if not files:
            print(f"[patterns] Aucun fichier trouvé pour {asset}", flush=True)
            continue
        for fname, freq in files:
            df = _load_file(fname)
            if df is None:
                continue
            # Colonne de valeur principale
            val_col = next(
                (c for c in ("close", "open") if c in df.columns),
                next((c for c in df.columns if c != "time"), None),
            )
            if val_col is None:
                continue
            s = df.set_index(df["time"].dt.normalize())[val_col].dropna()
            s = s[~s.index.duplicated(keep="last")]  # garde la dernière valeur du jour
            cond_series_list.append({
                "asset": asset, "file": fname, "freq": freq,
                "col": val_col, "series": s,
            })
            print(f"[patterns]   chargé {fname} ({len(s)} points, col={val_col})", flush=True)

    if not cond_series_list:
        print("[patterns] Aucune série de condition — abandon.", flush=True)
        return

    # ── Génération des combinaisons ───────────────────────────────────────
    combos: list[tuple] = []
    for cs in cond_series_list:
        thresholds = _candidate_thresholds(cs["series"])
        for thr, op in product(thresholds, [">", "<"]):
            combos.append((cs, thr, op))
            if len(combos) >= max_combos:
                break
        if len(combos) >= max_combos:
            break

    print(f"[patterns] {len(combos)} combinaisons à tester", flush=True)

    # ── Test de chaque combinaison ────────────────────────────────────────
    retained: list[dict] = []

    for i, (cs, thr, op) in enumerate(combos):
        s = cs["series"]
        if op == ">":
            cond_idx = s[s > thr].index
        else:
            cond_idx = s[s < thr].index

        cond_dates = set(cond_idx)
        cond_dates_sorted = sorted(cond_dates)

        for horizon in HORIZONS:
            stats = _test_single(cond_dates, target_df, base_rate, horizon)
            if stats is None:
                continue

            # Validation OOS
            oos = _validate_oos(cond_dates_sorted, target_df, horizon, base_rate)
            if oos is None:
                continue

            # Recherche conditions invalidantes (test inverse)
            inv_dates = set(
                s[s <= thr].index if op == ">" else s[s >= thr].index
            )
            inv_stats = _test_single(inv_dates, target_df, base_rate, horizon)
            invalidated_by = None
            if inv_stats and inv_stats["p_value"] < P_VALUE_THRESHOLD:
                direction_inv = "haussier" if inv_stats["pct_bull"] > 50 else "baissier"
                direction_main = "haussier" if stats["pct_bull"] > 50 else "baissier"
                if direction_inv != direction_main:
                    invalidated_by = {
                        "condition": f"{cs['asset']} {'<=' if op=='>' else '>='} {thr}",
                        "pct_bull": inv_stats["pct_bull"],
                        "p_value": inv_stats["p_value"],
                    }

            pattern = {
                "asset":       cs["asset"],
                "file":        cs["file"],
                "freq":        cs["freq"],
                "col":         cs["col"],
                "op":          op,
                "threshold":   thr,
                "horizon":     horizon,
                "is":          stats,
                "oos":         oos,
                "invalidated_by": invalidated_by,
                "condition_str": f"{cs['asset']} {op} {thr}",
            }
            retained.append(pattern)

        if (i + 1) % 50 == 0:
            print(f"[patterns] {i+1}/{len(combos)} testés — {len(retained)} retenus", flush=True)

    # ── Sauvegarde ────────────────────────────────────────────────────────
    elapsed = round(time.time() - t_start, 1)
    output = {
        "generated_at": pd.Timestamp.now().isoformat(),
        "assets": assets,
        "target": target,
        "max_combos": max_combos,
        "n_combos_tested": len(combos),
        "n_patterns": len(retained),
        "elapsed_sec": elapsed,
        "base_rate": round(base_rate, 4),
        "patterns": retained,
    }
    try:
        OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        OUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2))
        print(f"[patterns] {len(retained)} patterns sauvegardés dans {OUT_FILE} ({elapsed}s)", flush=True)
    except Exception as e:
        print(f"[patterns] Erreur sauvegarde : {e}", flush=True)

    # ── Notification via fichier flag (thread-safe) ───────────────────────
    try:
        from pathlib import Path as _Path
        import json as _j
        flag_path = _Path(__file__).parent / "data" / ".patterns_ready"
        flag_path.write_text(_j.dumps({"n_patterns": len(retained)}))
        print(f"[patterns] Flag écrit : {flag_path}", flush=True)
    except Exception as e:
        print(f"[patterns] Erreur flag : {e}", flush=True)


# ─── Lancement en arrière-plan ────────────────────────────────────────────

def launch_background(
    assets: list[str],
    target: str = "SPX",
    max_combos: int = 1000,
    session_state=None,
) -> threading.Thread:
    """Lance explore_patterns dans un Thread daemon et retourne le thread."""
    t = threading.Thread(
        target=explore_patterns,
        args=(assets, target, max_combos, session_state),
        daemon=True,
        name="patterns-explorer",
    )
    t.start()
    print(f"[patterns] Thread lancé (id={t.ident})", flush=True)
    return t
