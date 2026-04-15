"""
spx_ml.py — Prédiction amplitude SPX (RIC/IC) par ML.

Architecture :
- 3 points d'entrée indépendants : 9h30, 10h00, 10h30 NY
- Cible : abs(ret) en % depuis l'entrée sur horizon configuré
- 3 catégories : FORT (≥0.45%), FAIBLE (≤0.23%), INCERTAIN
- Validation IS/OOS chronologique stricte (70/30)
- Features : toutes colonnes daily (tous CSV hors tickers) +
             overnight futures + intraday (selon entry point) +
             calendar macro + features dérivées
"""

import gc
import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from pathlib import Path
from datetime import time as dtime

DATA_DIR = Path(__file__).parent / "data" / "live_selected"

# ── Constantes stratégiques (NE PAS MODIFIER) ──────────────────
RIC_THRESHOLD  = 0.45   # % minimum pour signal RIC
IC_THRESHOLD   = 0.23   # % maximum pour signal IC
IC_MIN_MINUTES = 120    # durée minimum IC
IS_RATIO       = 0.70   # ratio In-Sample
OOS_MIN_RATE   = 0.82   # taux OOS minimum pour "actionnable"
MIN_SAMPLES_IS = 20     # occurrences min en IS
MIN_SAMPLES_OOS = 10    # occurrences min en OOS

ENTRY_POINTS = {
    "9h30":  dtime(9,  30),
    "10h00": dtime(10,  0),
    "10h30": dtime(10, 30),
}

HORIZONS_MIN = [30, 60, 90, 120, 150, 180, 210, 240, 270, 300, 330, 360]

# ── Cache modèles ───────────────────────────────────────────────
_MODEL_CACHE: dict = {}


def _load_intraday(symbol: str, freq: str) -> pd.DataFrame | None:
    """Charge un CSV intraday, convertit Paris→NY si nécessaire."""
    for stem in [f"{symbol}_{freq}", f"{symbol.lower()}_{freq}"]:
        p = DATA_DIR / f"{stem}.csv"
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p, sep=";")
            df.columns = [c.strip().lower().replace(" ", "_").replace("#", "").strip()
                          for c in df.columns]
            df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
            df = df.dropna(subset=["time"]).copy()
            # Conversion Paris → NY (-6h si heure médiane ≥ 13h)
            if df["time"].dt.hour.median() >= 13:
                df["time"] = df["time"] - pd.Timedelta(hours=6)
            df = df.set_index("time").sort_index()
            for col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", "."), errors="coerce"
                )
            return df
        except Exception as e:
            print(f"[spx_ml] Erreur chargement {p}: {e}", flush=True)
    return None


def _load_daily_all() -> dict[str, pd.DataFrame]:
    """
    Charge tous les CSV daily (hors tickers individuels).
    Retourne dict {nom_court: DataFrame}.
    """
    TICKER_PATTERNS = ["aaoi", "aapl", "nvda", "msft", "tsla", "amzn",
                       "googl", "meta", "jpm", "calendar", "earnings"]

    result = {}
    for p in sorted(DATA_DIR.glob("*_daily.csv")):
        stem = p.stem.lower()
        if any(t in stem for t in TICKER_PATTERNS):
            continue
        try:
            df = pd.read_csv(p, sep=";")
            df.columns = [c.strip().lower().replace(" ", "_").replace("#", "").strip()
                          for c in df.columns]
            tc = next((c for c in ("time", "date", "timestamp") if c in df.columns), None)
            if tc is None:
                continue
            df[tc] = pd.to_datetime(df[tc].astype(str).str.strip(), errors="coerce")
            df = df.dropna(subset=[tc]).set_index(tc).sort_index()
            df.index = df.index.normalize()
            for col in df.columns:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", "."), errors="coerce"
                )
            short_name = stem.replace("_daily", "").replace("_index", "")[:20]
            result[short_name] = df
            print(f"[spx_ml] Chargé {short_name}: {len(df)} jours, {len(df.columns)} cols",
                  flush=True)
        except Exception as e:
            print(f"[spx_ml] Erreur {p.name}: {e}", flush=True)
    return result


def build_sessions(entry_point: str = "9h30") -> pd.DataFrame:
    """
    Construit un DataFrame sessions SPY/SPX avec amplitude cible.

    Colonnes cible :
    - abs_ret_{h}min_pct : amplitude absolue % depuis l'entrée
    - ric_ok_{h}min : bool, amplitude ≥ RIC_THRESHOLD
    - ic_ok_{h}min : bool, amplitude ≤ IC_THRESHOLD pendant IC_MIN_MINUTES
    """
    entry_time = ENTRY_POINTS.get(entry_point, dtime(9, 30))

    df = _load_intraday("SPY", "30min")
    if df is None:
        df = _load_intraday("SPX", "30min")
    if df is None:
        print(f"[spx_ml] Aucun CSV intraday SPY/SPX 30min trouvé", flush=True)
        return pd.DataFrame()

    trading = df[(df.index.time >= dtime(9, 30)) & (df.index.time <= dtime(16, 0))]
    sessions = {}

    for date in sorted(set(trading.index.date)):
        day = trading[trading.index.date == date].sort_index()
        if len(day) < 4:
            continue

        entry_bars = day[day.index.time >= entry_time]
        if entry_bars.empty:
            continue

        entry_bar = entry_bars.iloc[0]
        entry_price = float(entry_bar.get("open", entry_bar.get("close", np.nan)))
        if pd.isna(entry_price) or entry_price <= 0:
            continue
        entry_ts = entry_bars.index[0]

        session = {
            "date": pd.Timestamp(date),
            "entry_price": entry_price,
            "entry_close": float(entry_bar.get("close", entry_price)),
            "entry_volume": float(entry_bar.get("volume", np.nan)),
            "entry_rsi": float(entry_bar.get("rsi", np.nan)),
            "entry_vwap": float(entry_bar.get("vwap", np.nan)),
        }

        for h in HORIZONS_MIN:
            target_ts = entry_ts + pd.Timedelta(minutes=h)
            window = day[(day.index >= entry_ts) & (day.index <= target_ts)]
            if window.empty:
                continue

            price_h = float(window.iloc[-1].get("close", np.nan))
            if pd.isna(price_h):
                continue

            abs_ret_pct = abs((price_h - entry_price) / entry_price * 100)

            if "high" in window.columns and "low" in window.columns:
                max_amp = max(
                    abs(float(window["high"].max()) - entry_price) / entry_price * 100,
                    abs(float(window["low"].min()) - entry_price) / entry_price * 100
                )
            else:
                max_amp = abs_ret_pct

            lbl = f"{h}min"
            session[f"abs_ret_{lbl}_pct"] = round(abs_ret_pct, 4)
            session[f"ric_ok_{lbl}"] = abs_ret_pct >= RIC_THRESHOLD
            if h >= IC_MIN_MINUTES:
                session[f"ic_ok_{lbl}"] = max_amp <= IC_THRESHOLD

        sessions[date] = session

    if not sessions:
        return pd.DataFrame()

    result = pd.DataFrame(list(sessions.values())).set_index("date").sort_index()

    # Gap overnight depuis close J-1 16h00
    close_1600 = []
    for d in result.index:
        day_d = trading[trading.index.date == d.date()]
        bars_1600 = day_d[day_d.index.time == dtime(16, 0)]
        if not bars_1600.empty:
            close_1600.append(float(bars_1600.iloc[-1].get("close", np.nan)))
        elif not day_d.empty:
            close_1600.append(float(day_d.iloc[-1].get("close", np.nan)))
        else:
            close_1600.append(np.nan)

    result["close_1600"] = close_1600
    result["prev_close_1600"] = result["close_1600"].shift(1)
    result["gap_pct"] = (result["entry_price"] - result["prev_close_1600"]) / result["prev_close_1600"] * 100

    return result


def build_feature_matrix(entry_point: str = "9h30",
                         target_horizon: str = "120min") -> tuple:
    """
    Construit X (features), y_amp (amplitude %), y_cat (catégorie 0/1/2).

    Catégories :
    0 = FAIBLE  (≤ IC_THRESHOLD  = 0.23%)
    1 = INCERTAIN
    2 = FORT    (≥ RIC_THRESHOLD = 0.45%)

    Returns: (X, y_amp, y_cat) ou (None, None, None) si erreur.
    """
    sessions = build_sessions(entry_point)
    if sessions.empty:
        print(f"[spx_ml/{entry_point}] Pas de sessions disponibles", flush=True)
        return None, None, None

    target_col = f"abs_ret_{target_horizon}_pct"
    if target_col not in sessions.columns:
        available = [c for c in sessions.columns if c.startswith("abs_ret_") and c.endswith("_pct")]
        if not available:
            return None, None, None
        target_col = sorted(available)[0]
        print(f"[spx_ml/{entry_point}] Horizon {target_horizon} non disponible, "
              f"utilisation de {target_col}", flush=True)

    features = pd.DataFrame(index=sessions.index)

    # ── FEATURES DAILY (toutes colonnes, tous CSV, shift(1) = J-1) ──
    daily_csvs = _load_daily_all()
    for name, df_d in daily_csvs.items():
        for col in df_d.columns:
            series = df_d[col].shift(1).reindex(features.index, method="ffill")
            if series.notna().sum() > 30:
                feat_name = f"{name}_{col}"[:40]
                features[feat_name] = series

    # ── FEATURES OVERNIGHT FUTURES ──
    df_fut = _load_intraday("SPX_FUTURE", "30min")
    if df_fut is None:
        df_fut = _load_intraday("SPX_FUTURE", "1min")

    entry_time = ENTRY_POINTS[entry_point]

    if df_fut is not None:
        for date in sessions.index:
            prev_date = date - pd.Timedelta(days=3)
            night = df_fut[
                ((df_fut.index.date >= prev_date.date()) &
                 (df_fut.index.date <= date.date())) &
                ((df_fut.index.time > dtime(16, 0)) |
                 (df_fut.index.time < dtime(9, 30)))
            ]
            if night.empty:
                continue
            features.loc[date, "fut_overnight_ret_pct"] = (
                float(night.iloc[-1].get("close", np.nan)) -
                float(night.iloc[0].get("open", night.iloc[0].get("close", np.nan)))
            ) / max(float(night.iloc[0].get("open", 1)), 0.01) * 100
            if "high" in night.columns and "low" in night.columns:
                features.loc[date, "fut_overnight_range_pct"] = (
                    float(night["high"].max()) - float(night["low"].min())
                ) / max(float(night.iloc[0].get("close", 1)), 0.01) * 100
            if "volume" in night.columns:
                features.loc[date, "fut_overnight_volume"] = float(night["volume"].sum())
            if "rsi" in night.columns and night["rsi"].notna().any():
                features.loc[date, "fut_overnight_rsi_last"] = float(night["rsi"].iloc[-1])
        gc.collect()

    # ── FEATURES INTRADAY (10h00 et 10h30 ont accès aux barres précédentes) ──
    df_spy = _load_intraday("SPY", "30min")
    if df_spy is not None and entry_point in ("10h00", "10h30"):
        bar_time = dtime(9, 30)
        for date in sessions.index:
            bar = df_spy[
                (df_spy.index.date == date.date()) &
                (df_spy.index.time == bar_time)
            ]
            if bar.empty:
                continue
            for col in ["open", "close", "high", "low", "volume", "rsi", "vwap"]:
                if col in bar.columns:
                    val = float(bar.iloc[0][col])
                    features.loc[date, f"spy_930_{col}"] = val
            if "open" in bar.columns and "close" in bar.columns:
                o, c = float(bar.iloc[0]["open"]), float(bar.iloc[0]["close"])
                features.loc[date, "spy_930_bar_ret_pct"] = (c - o) / o * 100 if o else np.nan
                features.loc[date, "spy_930_bar_amp_pct"] = abs(c - o) / o * 100 if o else np.nan

        if entry_point == "10h30":
            bar_time2 = dtime(10, 0)
            for date in sessions.index:
                bar2 = df_spy[
                    (df_spy.index.date == date.date()) &
                    (df_spy.index.time == bar_time2)
                ]
                if bar2.empty:
                    continue
                for col in ["open", "close", "high", "low", "volume", "rsi"]:
                    if col in bar2.columns:
                        features.loc[date, f"spy_1000_{col}"] = float(bar2.iloc[0][col])
        gc.collect()

    # ── FEATURES CALENDAR ──
    try:
        from calendar_features import get_calendar_features
        cal = get_calendar_features(features.index)
        if not cal.empty:
            for col in cal.columns:
                features[f"cal_{col}"] = cal[col].reindex(features.index, method="ffill")
    except Exception as e:
        print(f"[spx_ml/{entry_point}] Calendar non disponible: {e}", flush=True)

    # ── FEATURES DÉRIVÉES ──
    features["gap_pct"] = sessions.get("gap_pct", pd.Series(dtype=float)).reindex(features.index)

    spx_close = daily_csvs.get("spx", daily_csvs.get("spx_daily", pd.DataFrame())).get("close")
    if spx_close is not None and len(spx_close) > 20:
        spx_close_shifted = spx_close.shift(1).reindex(features.index, method="ffill")
        for w in [3, 5, 10, 20]:
            features[f"spx_mom_{w}d"] = spx_close_shifted.pct_change(w) * 100
        features["spx_vol_5d"] = spx_close_shifted.pct_change().rolling(5).std() * 100
        features["spx_vol_20d"] = spx_close_shifted.pct_change().rolling(20).std() * 100
        ma20 = spx_close_shifted.rolling(20).mean()
        features["spx_dist_ma20"] = (spx_close_shifted - ma20) / ma20 * 100

    vix = daily_csvs.get("vix", pd.DataFrame()).get("close")
    vix3m = daily_csvs.get("vix3m", pd.DataFrame()).get("close")
    vvix = daily_csvs.get("vvix", pd.DataFrame()).get("close")

    if vix is not None and vix3m is not None:
        vix_s = vix.shift(1).reindex(features.index, method="ffill")
        vix3m_s = vix3m.shift(1).reindex(features.index, method="ffill")
        features["vix_term_structure"] = vix_s / vix3m_s.replace(0, np.nan)

    if vvix is not None and vix is not None:
        vvix_s = vvix.shift(1).reindex(features.index, method="ffill")
        vix_s2 = vix.shift(1).reindex(features.index, method="ffill")
        features["vvix_vix_ratio"] = vvix_s / vix_s2.replace(0, np.nan)

    # VIX1D/VIX ratio à l'open (signal fort identifié empiriquement)
    vix1d_ratio = daily_csvs.get("vix1d_vix_ratio", pd.DataFrame()).get("close")
    if vix1d_ratio is not None:
        vix1d_s = vix1d_ratio.shift(1).reindex(features.index, method="ffill")
        features["vix1d_vix_ratio_close"] = vix1d_s
        if vix is not None:
            vix_s3 = vix.shift(1).reindex(features.index, method="ffill")
            features["vix1d_ratio_x_vix"] = vix1d_s * vix_s3
            features["vix1d_high_ratio_low_vix"] = (
                (vix1d_s > 0.6) & (vix_s3 < 20)
            ).astype(float)

    # Momentum put/call ratios
    for pc_name in ["spx_put_call_ratio", "equity_put_call_rati",
                    "vix_put_call_ratio"]:
        pc_df = daily_csvs.get(pc_name, pd.DataFrame())
        if "close" in pc_df.columns:
            pc_s = pc_df["close"].shift(1).reindex(features.index, method="ffill")
            features[f"{pc_name[:20]}_mom3d"] = pc_s.pct_change(3) * 100
            features[f"{pc_name[:20]}_zscore"] = (
                (pc_s - pc_s.rolling(20).mean()) /
                (pc_s.rolling(20).std() + 1e-8)
            )

    # Spread VIX9D/VIX (court terme vs moyen terme)
    vix9d_df = daily_csvs.get("vix9d", pd.DataFrame())
    if "close" in vix9d_df.columns and vix is not None:
        vix9d_s = vix9d_df["close"].shift(1).reindex(features.index, method="ffill")
        vix_s4 = vix.shift(1).reindex(features.index, method="ffill")
        features["vix9d_vix_spread"] = vix9d_s - vix_s4
        features["vix9d_vix_ratio"] = vix9d_s / vix_s4.replace(0, np.nan)

    # Gold momentum (refuge → SPX inverse)
    gold_df = daily_csvs.get("gold", pd.DataFrame())
    if "close" in gold_df.columns:
        gold_s = gold_df["close"].shift(1).reindex(features.index, method="ffill")
        features["gold_mom3d"] = gold_s.pct_change(3) * 100
        features["gold_mom5d"] = gold_s.pct_change(5) * 100

    # Advance/Decline momentum
    ad_df = daily_csvs.get("advance_decline_rati", pd.DataFrame())
    if "close" in ad_df.columns:
        ad_s = ad_df["close"].shift(1).reindex(features.index, method="ffill")
        features["adv_decl_mom3d"] = ad_s.pct_change(3) * 100
        features["adv_decl_zscore"] = (
            (ad_s - ad_s.rolling(20).mean()) /
            (ad_s.rolling(20).std() + 1e-8)
        )

    features["day_of_week"] = pd.Series(sessions.index.dayofweek, index=sessions.index)
    features["month"] = pd.Series(sessions.index.month, index=sessions.index)
    features["week_of_year"] = pd.Series(sessions.index.isocalendar().week.values,
                                          index=sessions.index)

    # ── CIBLE ──
    y_amp = sessions[target_col].copy()

    def _categorize(v):
        if pd.isna(v):
            return np.nan
        if v >= RIC_THRESHOLD:
            return 2  # FORT
        if v <= IC_THRESHOLD:
            return 0  # FAIBLE
        return 1  # INCERTAIN

    y_cat = y_amp.apply(_categorize)

    # ── ALIGNEMENT ET NETTOYAGE ──
    common = features.index.intersection(y_amp.dropna().index)
    X = features.loc[common].copy()
    y_amp = y_amp.loc[common]
    y_cat = y_cat.loc[common]

    X = X.loc[:, X.isnull().mean() < 0.5]

    valid = X.notnull().all(axis=1) & y_cat.notna() & y_amp.notna()
    X, y_amp, y_cat = X[valid], y_amp[valid], y_cat[valid]

    print(f"[spx_ml/{entry_point}] Features: {X.shape[1]} | "
          f"Sessions: {X.shape[0]} | Horizon: {target_col}", flush=True)
    print(f"[spx_ml/{entry_point}] FORT≥{RIC_THRESHOLD}%: {(y_cat == 2).sum()} | "
          f"FAIBLE≤{IC_THRESHOLD}%: {(y_cat == 0).sum()} | "
          f"INCERTAIN: {(y_cat == 1).sum()}", flush=True)
    gc.collect()

    return X, y_amp, y_cat


def _model_factories():
    """Retourne les factories de modèles disponibles (XGBoost, LightGBM)."""
    factories = []
    try:
        import xgboost as xgb
        factories.append((
            "xgboost",
            lambda: xgb.XGBClassifier(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                use_label_encoder=False, eval_metric="mlogloss",
                random_state=42, n_jobs=2, verbosity=0
            ),
            lambda: xgb.XGBRegressor(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=42, n_jobs=2, verbosity=0
            )
        ))
    except ImportError:
        pass
    try:
        import lightgbm as lgb
        factories.append((
            "lightgbm",
            lambda: lgb.LGBMClassifier(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=42, n_jobs=2, verbose=-1
            ),
            lambda: lgb.LGBMRegressor(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=42, n_jobs=2, verbose=-1
            )
        ))
    except ImportError:
        pass
    return factories


def train_amplitude_model(entry_point: str = "9h30",
                          target_horizon: str = "120min") -> dict:
    """
    Entraîne XGBoost/LightGBM pour prédire l'amplitude SPX.
    Validation IS/OOS strictement chronologique.

    Returns dict avec :
    - ok, best_model, category_accuracy_is, category_accuracy_oos,
    - amplitude_mae, n_train, n_test, top_features,
    - pred_distribution, test_distribution, is_reliable
    """
    X, y_amp, y_cat = build_feature_matrix(entry_point, target_horizon)
    if X is None or len(X) < MIN_SAMPLES_IS + MIN_SAMPLES_OOS:
        return {"ok": False, "error": f"Pas assez de données ({len(X) if X is not None else 0} sessions)"}

    split = int(len(X) * IS_RATIO)
    X_is, X_oos = X.iloc[:split], X.iloc[split:]
    y_cat_is, y_cat_oos = y_cat.iloc[:split], y_cat.iloc[split:]
    y_amp_is, y_amp_oos = y_amp.iloc[:split], y_amp.iloc[split:]

    if len(X_oos) < MIN_SAMPLES_OOS:
        return {"ok": False, "error": f"OOS insuffisant ({len(X_oos)} sessions < {MIN_SAMPLES_OOS})"}

    results = {}

    for lib_name, make_clf, make_reg in _model_factories():
        try:
            clf = make_clf()
            clf.fit(X_is, y_cat_is)
            pred_is = clf.predict(X_is)
            pred_oos = clf.predict(X_oos)
            acc_is = float((pred_is == y_cat_is).mean() * 100)
            acc_oos = float((pred_oos == y_cat_oos).mean() * 100)

            reg = make_reg()
            reg.fit(X_is, y_amp_is)
            amp_pred_oos = reg.predict(X_oos)

            from sklearn.metrics import mean_absolute_error
            mae = float(mean_absolute_error(y_amp_oos, amp_pred_oos))

            try:
                imp = pd.Series(
                    clf.feature_importances_, index=X.columns
                ).sort_values(ascending=False)
            except Exception:
                imp = pd.Series(dtype=float)

            from collections import Counter
            dist_oos = Counter(pred_oos.tolist())
            dist_real = Counter(y_cat_oos.tolist())

            is_reliable = acc_oos >= OOS_MIN_RATE * 100

            results[lib_name] = {
                "category_accuracy_is": round(acc_is, 2),
                "category_accuracy_oos": round(acc_oos, 2),
                "amplitude_mae": round(mae, 4),
                "n_train": len(X_is),
                "n_test": len(X_oos),
                "is_reliable": is_reliable,
                "top_features": imp.head(15).round(4).to_dict(),
                "pred_distribution": {
                    "fort": int(dist_oos.get(2, 0)),
                    "incertain": int(dist_oos.get(1, 0)),
                    "faible": int(dist_oos.get(0, 0)),
                },
                "test_distribution": {
                    "fort": int(dist_real.get(2, 0)),
                    "incertain": int(dist_real.get(1, 0)),
                    "faible": int(dist_real.get(0, 0)),
                },
                "_clf": clf,
                "_reg": reg,
                "_feature_names": list(X.columns),
            }

            print(f"[spx_ml/{entry_point}/{lib_name}] "
                  f"IS={acc_is:.1f}% | OOS={acc_oos:.1f}% | MAE={mae:.4f}% | "
                  f"Fiable={is_reliable}", flush=True)
            gc.collect()

        except Exception as e:
            print(f"[spx_ml/{entry_point}/{lib_name}] Erreur: {e}", flush=True)

    if not results:
        return {"ok": False, "error": "Aucun modèle disponible (pip install xgboost lightgbm)"}

    best = max(results, key=lambda k: results[k]["category_accuracy_oos"])
    b = results[best]

    return {
        "ok": True,
        "entry_point": entry_point,
        "target": target_horizon,
        "best_model": best,
        "category_accuracy_is": b["category_accuracy_is"],
        "category_accuracy_oos": b["category_accuracy_oos"],
        # backward-compat alias pour _exec_ml (query_executor)
        "category_accuracy": b["category_accuracy_oos"],
        "amplitude_mae": b["amplitude_mae"],
        "n_train": b["n_train"],
        "n_test": b["n_test"],
        "is_reliable": b["is_reliable"],
        "top_features": b["top_features"],
        "pred_distribution": b["pred_distribution"],
        "test_distribution": b["test_distribution"],
        "all_models": {
            k: {"acc_is": v["category_accuracy_is"],
                "acc_oos": v["category_accuracy_oos"],
                "mae": v["amplitude_mae"],
                "reliable": v["is_reliable"]}
            for k, v in results.items()
        },
        "_models": results,
    }


def predict_today(trained: dict) -> dict:
    """
    Prédit l'amplitude pour aujourd'hui depuis le modèle entraîné.

    Returns dict avec amplitude_category, amplitude_pct, probabilities,
    ric_signal, ic_signal.
    """
    if not trained.get("ok"):
        return {"ok": False, "error": trained.get("error", "Modèle non entraîné")}

    entry_point = trained["entry_point"]
    target = trained["target"]

    X, _, _ = build_feature_matrix(entry_point, target)
    if X is None or X.empty:
        return {"ok": False, "error": "Pas de features pour aujourd'hui"}

    best_name = trained["best_model"]
    model_data = trained["_models"][best_name]
    clf = model_data["_clf"]
    reg = model_data["_reg"]
    feat_names = model_data["_feature_names"]

    X_today = X.iloc[[-1]].reindex(columns=feat_names, fill_value=0)

    cat = int(clf.predict(X_today)[0])
    amp = float(reg.predict(X_today)[0])

    try:
        proba = clf.predict_proba(X_today)[0]
        probas = {
            "faible": round(float(proba[0]) * 100, 1) if len(proba) > 0 else None,
            "incertain": round(float(proba[1]) * 100, 1) if len(proba) > 1 else None,
            "fort": round(float(proba[2]) * 100, 1) if len(proba) > 2 else None,
        }
    except Exception:
        probas = {}

    cat_label = {0: "FAIBLE", 1: "INCERTAIN", 2: "FORT"}

    return {
        "ok": True,
        "entry_point": entry_point,
        "target": target,
        "date": X.index[-1].strftime("%d/%m/%Y"),
        "amplitude_category": cat_label.get(cat, "?"),
        "amplitude_pct": round(amp, 3),
        "amplitude_pts": round(amp / 100 * 5500, 1),
        "probabilities": probas,
        "ric_signal": cat == 2,
        "ic_signal": cat == 0,
        "model": best_name,
        "is_reliable": trained.get("is_reliable", False),
    }


def get_or_train(entry_point: str = "9h30",
                 target: str = "120min") -> dict:
    """Retourne le modèle depuis le cache ou l'entraîne."""
    key = f"{entry_point}_{target}"
    if key not in _MODEL_CACHE:
        _MODEL_CACHE[key] = train_amplitude_model(entry_point, target)
    return _MODEL_CACHE[key]


def clear_cache():
    """Vide le cache des modèles (utile pour forcer un ré-entraînement)."""
    global _MODEL_CACHE
    _MODEL_CACHE.clear()
    gc.collect()
