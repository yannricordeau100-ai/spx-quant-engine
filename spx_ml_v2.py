"""
spx_ml_v2.py — Prédiction amplitude SPX v2 : Feature selection +
               XGBoost propre + LSTM PyTorch + Ensemble.

Dépend de spx_ml.py pour build_feature_matrix() et build_sessions().
Interface identique à spx_ml.py : get_or_train_v2(), predict_today_v2().

Constantes héritées de spx_ml.py (ne pas redéfinir) :
RIC_THRESHOLD = 0.45%, IC_THRESHOLD = 0.23%
IS_RATIO = 0.70, OOS_MIN_RATE = 0.82
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

# IMPORTANT : importer XGBoost AVANT PyTorch pour éviter le segfault libomp
try:
    import xgboost as _xgb_preload  # noqa: F401
except ImportError:
    pass
try:
    import lightgbm as _lgb_preload  # noqa: F401
except ImportError:
    pass

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset

# Hériter des constantes de spx_ml (mais pas _model_factories qui use n_jobs=2)
from spx_ml import (
    build_feature_matrix, build_sessions,
    RIC_THRESHOLD, IC_THRESHOLD, IS_RATIO, OOS_MIN_RATE,
    MIN_SAMPLES_IS, MIN_SAMPLES_OOS,
    ENTRY_POINTS,
)


def _model_factories():
    """Factories XGBoost/LightGBM avec n_jobs=1 (évite segfault libomp)."""
    factories = []
    try:
        import xgboost as xgb
        factories.append((
            "xgboost",
            lambda: xgb.XGBClassifier(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                use_label_encoder=False, eval_metric="mlogloss",
                random_state=42, n_jobs=1, verbosity=0
            ),
            lambda: xgb.XGBRegressor(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=42, n_jobs=1, verbosity=0
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
                random_state=42, n_jobs=1, verbose=-1
            ),
            lambda: lgb.LGBMRegressor(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                subsample=0.8, colsample_bytree=0.8,
                random_state=42, n_jobs=1, verbose=-1
            )
        ))
    except ImportError:
        pass
    return factories


# ── Device MPS/CPU ─────────────────────────────────────────────
def _get_device() -> torch.device:
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


# ── Constantes V2 ──────────────────────────────────────────────
N_TOP_FEATURES   = 25    # features retenues après sélection
SEQ_LEN          = 20    # fenêtre temporelle LSTM (jours)
LSTM_HIDDEN      = 64    # unités LSTM
LSTM_LAYERS      = 2     # couches LSTM
LSTM_DROPOUT     = 0.3   # dropout entre couches
LSTM_EPOCHS      = 80    # epochs max
LSTM_LR          = 1e-3  # learning rate
LSTM_BATCH       = 32    # batch size
LSTM_PATIENCE    = 12    # early stopping patience
DEFAULT_HORIZON      = "360min"   # horizon optimal identifié
DEFAULT_ENTRY        = "9h30"
PRECISION_THRESHOLD  = 0.70       # seuil proba pour signal FORT
ENSEMBLE_WEIGHT_XGB  = 0.45       # poids XGBoost dans l'ensemble
ENSEMBLE_WEIGHT_LSTM = 0.55       # poids LSTM dans l'ensemble

_CACHE_V2: dict = {}


# ══════════════════════════════════════════════════════════════
# ÉTAPE 1 — SÉLECTION DE FEATURES
# ══════════════════════════════════════════════════════════════

def select_features(X: pd.DataFrame,
                    y_cat: pd.Series,
                    n_top: int = N_TOP_FEATURES,
                    verbose: bool = True) -> list[str]:
    """
    Sélectionne les N features les plus importantes via XGBoost
    sur l'ensemble IS. Retourne la liste des noms de colonnes.

    Stratégie anti-overfitting :
    - Entraîner sur IS uniquement (70% chronologique)
    - Sélectionner par importance moyenne sur 5-fold TimeSeriesSplit
    - Exclure les features avec corrélation > 0.95 (redondance)
    """
    from sklearn.model_selection import TimeSeriesSplit

    split = int(len(X) * IS_RATIO)
    X_is = X.iloc[:split]
    y_is = y_cat.iloc[:split]

    importances = pd.Series(0.0, index=X.columns)
    n_valid_folds = 0

    try:
        import xgboost as xgb
        tscv = TimeSeriesSplit(n_splits=5)
        for fold_i, (tr, va) in enumerate(tscv.split(X_is)):
            if len(tr) < MIN_SAMPLES_IS or len(va) < 5:
                continue
            clf = xgb.XGBClassifier(
                n_estimators=100, max_depth=3, learning_rate=0.1,
                subsample=0.8, colsample_bytree=0.6,
                use_label_encoder=False, eval_metric="mlogloss",
                random_state=42 + fold_i, n_jobs=1, verbosity=0
            )
            clf.fit(X_is.iloc[tr], y_is.iloc[tr])
            imp = pd.Series(clf.feature_importances_, index=X.columns)
            importances += imp
            n_valid_folds += 1
            del clf
            gc.collect()

        if n_valid_folds == 0:
            clf = xgb.XGBClassifier(
                n_estimators=100, max_depth=3, random_state=42,
                use_label_encoder=False, eval_metric="mlogloss",
                verbosity=0
            )
            clf.fit(X_is, y_is)
            importances = pd.Series(clf.feature_importances_, index=X.columns)
            n_valid_folds = 1
            del clf
            gc.collect()

        importances = importances / n_valid_folds

    except ImportError:
        import lightgbm as lgb
        clf = lgb.LGBMClassifier(
            n_estimators=100, max_depth=3, random_state=42,
            n_jobs=1, verbose=-1
        )
        clf.fit(X_is, y_is)
        importances = pd.Series(clf.feature_importances_, index=X.columns)
        del clf
        gc.collect()

    importances = importances.sort_values(ascending=False)

    # Supprimer les features redondantes (corrélation > 0.95)
    top_candidates = importances.head(min(n_top * 3, len(importances))).index.tolist()
    X_candidates = X[top_candidates]

    corr_matrix = X_candidates.corr().abs()
    to_drop = set()
    for i in range(len(top_candidates)):
        if top_candidates[i] in to_drop:
            continue
        for j in range(i + 1, len(top_candidates)):
            if top_candidates[j] in to_drop:
                continue
            if corr_matrix.iloc[i, j] > 0.95:
                if importances[top_candidates[i]] >= importances[top_candidates[j]]:
                    to_drop.add(top_candidates[j])
                else:
                    to_drop.add(top_candidates[i])

    selected = [f for f in top_candidates if f not in to_drop][:n_top]

    if verbose:
        print(f"[spx_ml_v2] Sélection : {len(selected)} features retenues "
              f"(sur {len(X.columns)} candidates, {len(to_drop)} redondantes supprimées)",
              flush=True)
        print(f"[spx_ml_v2] Top 10 : {selected[:10]}", flush=True)

    return selected


# ══════════════════════════════════════════════════════════════
# ÉTAPE 2 — LSTM PyTorch
# ══════════════════════════════════════════════════════════════

class AmplitudeLSTM(nn.Module):
    """
    LSTM pour prédiction amplitude SPX.
    Input  : (batch, seq_len, n_features)
    Output : (batch, 3) — logits pour FAIBLE/INCERTAIN/FORT
    """
    def __init__(self, n_features: int, hidden: int = LSTM_HIDDEN,
                 n_layers: int = LSTM_LAYERS, dropout: float = LSTM_DROPOUT):
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=hidden,
            num_layers=n_layers,
            dropout=dropout if n_layers > 1 else 0.0,
            batch_first=True
        )
        self.norm = nn.LayerNorm(hidden)
        self.head = nn.Sequential(
            nn.Dropout(dropout),
            nn.Linear(hidden, 32),
            nn.GELU(),
            nn.Dropout(dropout * 0.5),
            nn.Linear(32, 3)
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        last = out[:, -1, :]
        last = self.norm(last)
        return self.head(last)


def _make_sequences(X: np.ndarray, y: np.ndarray,
                    seq_len: int = SEQ_LEN) -> tuple:
    """
    Construit des séquences glissantes pour le LSTM.
    X shape : (T, n_features)
    y shape : (T,)
    Returns : (X_seq, y_seq) de shape (T-seq_len, seq_len, n_features)
    """
    Xs, ys = [], []
    for i in range(seq_len, len(X)):
        Xs.append(X[i - seq_len:i])
        ys.append(y[i])
    return np.array(Xs, dtype=np.float32), np.array(ys, dtype=np.int64)


def train_lstm(X_is: np.ndarray, y_is: np.ndarray,
               X_oos: np.ndarray, y_oos: np.ndarray,
               n_features: int,
               device: torch.device,
               verbose: bool = True) -> dict:
    """
    Entraîne le LSTM avec early stopping sur la validation OOS.
    Retourne dict avec model, acc_is, acc_oos, history.
    """
    X_seq_is,  y_seq_is  = _make_sequences(X_is,  y_is,  SEQ_LEN)
    X_seq_oos, y_seq_oos = _make_sequences(
        np.concatenate([X_is[-SEQ_LEN:], X_oos]),
        np.concatenate([y_is[-SEQ_LEN:], y_oos]),
        SEQ_LEN
    )
    X_seq_oos = X_seq_oos[:len(X_oos)]
    y_seq_oos = y_seq_oos[:len(y_oos)]

    if len(X_seq_is) < 10:
        return {"ok": False, "error": "Pas assez de séquences IS pour LSTM"}

    Xt_is  = torch.tensor(X_seq_is,  dtype=torch.float32).to(device)
    yt_is  = torch.tensor(y_seq_is,  dtype=torch.long).to(device)
    Xt_oos = torch.tensor(X_seq_oos, dtype=torch.float32).to(device)
    yt_oos = torch.tensor(y_seq_oos, dtype=torch.long).to(device)

    ds = TensorDataset(Xt_is, yt_is)
    dl = DataLoader(ds, batch_size=LSTM_BATCH, shuffle=False)

    model = AmplitudeLSTM(n_features).to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=LSTM_LR,
                                  weight_decay=1e-4)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, patience=5, factor=0.5
    )

    # Poids classes pour déséquilibre
    class_counts = np.bincount(y_seq_is, minlength=3)
    class_weights = torch.tensor(
        1.0 / (class_counts + 1e-6), dtype=torch.float32
    ).to(device)
    criterion = nn.CrossEntropyLoss(weight=class_weights)

    best_oos_acc = 0.0
    best_state   = None
    patience_cnt = 0
    history      = []

    for epoch in range(LSTM_EPOCHS):
        model.train()
        train_loss = 0.0
        for xb, yb in dl:
            optimizer.zero_grad()
            logits = model(xb)
            loss   = criterion(logits, yb)
            loss.backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimizer.step()
            train_loss += loss.item()

        model.eval()
        with torch.no_grad():
            logits_oos = model(Xt_oos)
            pred_oos   = logits_oos.argmax(dim=1).cpu().numpy()
            acc_oos    = float((pred_oos == y_seq_oos).mean() * 100)

            logits_is  = model(Xt_is)
            pred_is    = logits_is.argmax(dim=1).cpu().numpy()
            acc_is     = float((pred_is == y_seq_is).mean() * 100)

        scheduler.step(1 - acc_oos / 100)
        history.append({"epoch": epoch, "acc_is": acc_is,
                         "acc_oos": acc_oos, "loss": train_loss})

        if acc_oos > best_oos_acc:
            best_oos_acc = acc_oos
            best_state   = {k: v.clone() for k, v in model.state_dict().items()}
            patience_cnt = 0
        else:
            patience_cnt += 1

        if patience_cnt >= LSTM_PATIENCE:
            if verbose:
                print(f"[LSTM] Early stop epoch {epoch} | "
                      f"Best OOS={best_oos_acc:.1f}%", flush=True)
            break

        if verbose and epoch % 20 == 0:
            print(f"[LSTM] Epoch {epoch:3d} | IS={acc_is:.1f}% | "
                  f"OOS={acc_oos:.1f}% | Loss={train_loss:.4f}", flush=True)

    if best_state:
        model.load_state_dict(best_state)

    model.eval()
    with torch.no_grad():
        pred_oos_final = model(Xt_oos).argmax(dim=1).cpu().numpy()
        pred_is_final  = model(Xt_is).argmax(dim=1).cpu().numpy()

    final_acc_oos = float((pred_oos_final == y_seq_oos).mean() * 100)
    final_acc_is  = float((pred_is_final  == y_seq_is).mean()  * 100)

    if verbose:
        print(f"[LSTM] Final IS={final_acc_is:.1f}% | "
              f"OOS={final_acc_oos:.1f}%", flush=True)

    return {
        "ok":      True,
        "model":   model,
        "acc_is":  round(final_acc_is,  2),
        "acc_oos": round(final_acc_oos, 2),
        "history": history,
        "n_seq_is":  len(X_seq_is),
        "n_seq_oos": len(X_seq_oos),
    }


# ══════════════════════════════════════════════════════════════
# ÉTAPE 3 — XGBoost sur features réduites
# ══════════════════════════════════════════════════════════════

def train_xgb_clean(X_is: pd.DataFrame, y_cat_is: pd.Series,
                    X_oos: pd.DataFrame, y_cat_oos: pd.Series,
                    y_amp_is: pd.Series, y_amp_oos: pd.Series) -> dict:
    """
    XGBoost propre sur features réduites avec cross-validation temporelle.
    """
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.metrics import mean_absolute_error

    results = {}

    for lib_name, make_clf, make_reg in _model_factories():
        try:
            clf = make_clf()
            clf.fit(X_is, y_cat_is)
            pred_is  = clf.predict(X_is)
            pred_oos = clf.predict(X_oos)
            acc_is   = float((pred_is  == y_cat_is).mean()  * 100)
            acc_oos  = float((pred_oos == y_cat_oos).mean() * 100)

            cv_accs = []
            tscv = TimeSeriesSplit(n_splits=5)
            for tr, va in tscv.split(X_is):
                if len(tr) < 15 or len(va) < 5:
                    continue
                cv_clf = make_clf()
                cv_clf.fit(X_is.iloc[tr], y_cat_is.iloc[tr])
                cv_accs.append(
                    float((cv_clf.predict(X_is.iloc[va]) == y_cat_is.iloc[va]).mean() * 100)
                )
                del cv_clf
            cv_mean = round(float(np.mean(cv_accs)), 2) if cv_accs else None
            cv_std  = round(float(np.std(cv_accs)),  2) if cv_accs else None

            reg = make_reg()
            reg.fit(X_is, y_amp_is)
            amp_oos = reg.predict(X_oos)
            mae = float(mean_absolute_error(y_amp_oos, amp_oos))

            try:
                proba_oos = clf.predict_proba(X_oos)
            except Exception:
                proba_oos = None

            print(f"[XGB/{lib_name}] IS={acc_is:.1f}% | OOS={acc_oos:.1f}% | "
                  f"CV={cv_mean}±{cv_std}% | MAE={mae:.4f}%", flush=True)

            results[lib_name] = {
                "clf": clf, "reg": reg,
                "acc_is": round(acc_is, 2), "acc_oos": round(acc_oos, 2),
                "cv_mean": cv_mean, "cv_std": cv_std,
                "mae": round(mae, 4),
                "proba_oos": proba_oos,
                "pred_oos": pred_oos,
            }
            gc.collect()
        except Exception as e:
            print(f"[XGB/{lib_name}] Erreur: {e}", flush=True)

    if not results:
        return {"ok": False, "error": "Aucun modèle XGB disponible"}

    best = max(results, key=lambda k: results[k]["acc_oos"])
    return {"ok": True, "best": best, "models": results}


# ══════════════════════════════════════════════════════════════
# ÉTAPE 3bis — MODÈLE BINAIRE FORT (précision optimisée)
# ══════════════════════════════════════════════════════════════

def train_binary_fort(X_is: pd.DataFrame, y_cat_is: pd.Series,
                      X_oos: pd.DataFrame, y_cat_oos: pd.Series,
                      y_amp_is: pd.Series, y_amp_oos: pd.Series,
                      precision_threshold: float = PRECISION_THRESHOLD) -> dict:
    """
    Modèle binaire FORT vs NON-FORT optimisé pour la précision.
    Objectif : maximiser precision(FORT) à seuil de probabilité élevé.
    """
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.model_selection import TimeSeriesSplit

    y_is_bin = (y_cat_is == 2).astype(int)
    y_oos_bin = (y_cat_oos == 2).astype(int)

    pos_weight = (y_is_bin == 0).sum() / max((y_is_bin == 1).sum(), 1)

    best_result = {}

    for lib_name, make_clf, make_reg in _model_factories():
        try:
            if lib_name == "xgboost":
                import xgboost as xgb
                clf_raw = xgb.XGBClassifier(
                    n_estimators=200, max_depth=3, learning_rate=0.05,
                    subsample=0.8, colsample_bytree=0.6,
                    scale_pos_weight=pos_weight,
                    use_label_encoder=False, eval_metric="logloss",
                    random_state=42, n_jobs=1, verbosity=0
                )
            else:
                import lightgbm as lgb
                clf_raw = lgb.LGBMClassifier(
                    n_estimators=200, max_depth=3, learning_rate=0.05,
                    subsample=0.8, colsample_bytree=0.6,
                    class_weight="balanced",
                    random_state=42, n_jobs=1, verbose=-1
                )

            clf_raw.fit(X_is, y_is_bin)

            try:
                clf = CalibratedClassifierCV(clf_raw, cv=3, method="isotonic")
                clf.fit(X_is, y_is_bin)
            except Exception:
                clf = clf_raw

            proba_oos = clf.predict_proba(X_oos)[:, 1]

            precision_curve = []
            for thr in np.arange(0.40, 0.96, 0.05):
                mask_oos = proba_oos >= thr
                n_pred = int(mask_oos.sum())
                if n_pred == 0:
                    break
                prec = float((y_oos_bin[mask_oos] == 1).sum() / n_pred * 100)
                per_q = n_pred / max(len(X_oos) / 63, 1)
                precision_curve.append({
                    "threshold": round(float(thr), 2),
                    "n_oos": n_pred,
                    "precision": round(prec, 1),
                    "per_quarter": round(per_q, 1),
                })

            cv_precisions = []
            tscv = TimeSeriesSplit(n_splits=5)
            for tr, va in tscv.split(X_is):
                if len(tr) < 20 or len(va) < 5:
                    continue
                if lib_name == "xgboost":
                    cv_clf = xgb.XGBClassifier(
                        n_estimators=100, max_depth=3, learning_rate=0.05,
                        scale_pos_weight=pos_weight,
                        use_label_encoder=False, eval_metric="logloss",
                        random_state=42, n_jobs=1, verbosity=0
                    )
                else:
                    cv_clf = lgb.LGBMClassifier(
                        n_estimators=100, max_depth=3, class_weight="balanced",
                        random_state=42, n_jobs=1, verbose=-1
                    )
                cv_clf.fit(X_is.iloc[tr], y_is_bin.iloc[tr])
                cv_proba = cv_clf.predict_proba(X_is.iloc[va])[:, 1]
                mask_va = cv_proba >= precision_threshold
                if mask_va.sum() > 0:
                    cv_p = float((y_is_bin.iloc[va][mask_va] == 1).sum() /
                                  mask_va.sum() * 100)
                    cv_precisions.append(cv_p)
                del cv_clf
                gc.collect()

            cv_prec_mean = round(float(np.mean(cv_precisions)), 1) if cv_precisions else None
            cv_prec_std = round(float(np.std(cv_precisions)), 1) if cv_precisions else None

            try:
                imp = pd.Series(
                    clf_raw.feature_importances_,
                    index=X_is.columns
                ).sort_values(ascending=False)
            except Exception:
                imp = pd.Series(dtype=float)

            print(f"[Binary/{lib_name}] CV précision@{precision_threshold:.0%}: "
                  f"{cv_prec_mean}±{cv_prec_std}%", flush=True)
            for row in precision_curve:
                print(f"  Seuil {row['threshold']:.0%}: {row['n_oos']:3d} OOS | "
                      f"Précision={row['precision']:.1f}% | "
                      f"~{row['per_quarter']:.1f}/trim", flush=True)

            best_result[lib_name] = {
                "clf": clf,
                "clf_raw": clf_raw,
                "precision_curve": precision_curve,
                "cv_prec_mean": cv_prec_mean,
                "cv_prec_std": cv_prec_std,
                "proba_oos": proba_oos,
                "top_features": imp.head(15).round(4).to_dict(),
            }
            gc.collect()
        except Exception as e:
            print(f"[Binary/{lib_name}] Erreur: {e}", flush=True)

    if not best_result:
        return {"ok": False, "error": "Aucun modèle binaire"}

    def _prec_at_70(r):
        curve = r.get("precision_curve", [])
        match = [c for c in curve if abs(c["threshold"] - 0.70) < 0.01]
        return match[0]["precision"] if match else 0

    best = max(best_result, key=lambda k: _prec_at_70(best_result[k]))

    return {
        "ok": True, "best": best, "models": best_result,
        "precision_threshold": precision_threshold,
    }


# ══════════════════════════════════════════════════════════════
# ÉTAPE 4 — ENSEMBLE
# ══════════════════════════════════════════════════════════════

def _ensemble_predict(xgb_proba: np.ndarray,
                      lstm_logits: np.ndarray,
                      w_xgb: float = ENSEMBLE_WEIGHT_XGB,
                      w_lstm: float = ENSEMBLE_WEIGHT_LSTM) -> tuple:
    """
    Combine les probabilités XGBoost et LSTM par moyenne pondérée.
    xgb_proba  : (N, 3) probabilités XGBoost
    lstm_logits: (N, 3) logits LSTM → convertis en probas via softmax
    Returns    : (preds (N,), combined_proba (N, 3))
    """
    lstm_exp   = np.exp(lstm_logits - lstm_logits.max(axis=1, keepdims=True))
    lstm_proba = lstm_exp / lstm_exp.sum(axis=1, keepdims=True)

    combined = w_xgb * xgb_proba + w_lstm * lstm_proba
    return combined.argmax(axis=1), combined


# ══════════════════════════════════════════════════════════════
# ÉTAPE 5 — PIPELINE COMPLET
# ══════════════════════════════════════════════════════════════

def train_v2(entry_point: str = "9h30",
             target_horizon: str = "120min",
             verbose: bool = True) -> dict:
    """
    Pipeline complet V2 :
    1. Build feature matrix (via spx_ml.py)
    2. Sélection features top-N anti-redondance
    3. XGBoost propre sur features réduites
    4. LSTM sur séquences 20j
    5. Ensemble pondéré
    """
    device = _get_device()
    print(f"[spx_ml_v2/{entry_point}] Device: {device}", flush=True)

    # ── 1. Features ──────────────────────────────────────────
    X, y_amp, y_cat = build_feature_matrix(entry_point, target_horizon)
    if X is None or len(X) < MIN_SAMPLES_IS + MIN_SAMPLES_OOS:
        n = len(X) if X is not None else 0
        return {"ok": False, "error": f"Pas assez de données ({n} sessions)"}

    split = int(len(X) * IS_RATIO)

    # ── 2. Sélection features ────────────────────────────────
    selected_features = select_features(X, y_cat, N_TOP_FEATURES, verbose)
    X_sel = X[selected_features]

    X_is,  X_oos  = X_sel.iloc[:split],  X_sel.iloc[split:]
    y_cat_is, y_cat_oos = y_cat.iloc[:split],  y_cat.iloc[split:]
    y_amp_is, y_amp_oos = y_amp.iloc[:split],  y_amp.iloc[split:]

    if verbose:
        print(f"[spx_ml_v2/{entry_point}] Split IS={len(X_is)} | OOS={len(X_oos)} | "
              f"Features={len(selected_features)}", flush=True)
        print(f"[spx_ml_v2/{entry_point}] IS — FORT:{(y_cat_is == 2).sum()} "
              f"INCERT:{(y_cat_is == 1).sum()} FAIBLE:{(y_cat_is == 0).sum()}", flush=True)
        print(f"[spx_ml_v2/{entry_point}] OOS — FORT:{(y_cat_oos == 2).sum()} "
              f"INCERT:{(y_cat_oos == 1).sum()} FAIBLE:{(y_cat_oos == 0).sum()}", flush=True)

    # ── 3. XGBoost propre ────────────────────────────────────
    print(f"[spx_ml_v2/{entry_point}] Entraînement XGBoost...", flush=True)
    xgb_result = train_xgb_clean(
        X_is, y_cat_is, X_oos, y_cat_oos, y_amp_is, y_amp_oos
    )
    if not xgb_result.get("ok"):
        return {"ok": False, "error": f"XGBoost: {xgb_result.get('error')}"}

    best_xgb_name = xgb_result["best"]
    best_xgb      = xgb_result["models"][best_xgb_name]

    # ── 4. LSTM ──────────────────────────────────────────────
    print(f"[spx_ml_v2/{entry_point}] Entraînement LSTM ({device})...", flush=True)

    X_np    = X_sel.values.astype(np.float32)
    mean_is = X_np[:split].mean(axis=0)
    std_is  = X_np[:split].std(axis=0) + 1e-8
    X_norm  = (X_np - mean_is) / std_is

    X_is_np  = X_norm[:split]
    X_oos_np = X_norm[split:]
    y_cat_np = y_cat.values.astype(np.int64)
    y_is_np  = y_cat_np[:split]
    y_oos_np = y_cat_np[split:]

    lstm_result = train_lstm(
        X_is_np, y_is_np, X_oos_np, y_oos_np,
        n_features=len(selected_features),
        device=device, verbose=verbose
    )

    lstm_ok = lstm_result.get("ok", False)
    if not lstm_ok:
        print(f"[spx_ml_v2/{entry_point}] LSTM échoué: {lstm_result.get('error')} "
              f"— utilisation XGBoost seul", flush=True)

    # ── 5. Ensemble sur OOS ──────────────────────────────────
    xgb_proba_oos = best_xgb.get("proba_oos")

    ensemble_acc_oos = None
    ensemble_preds   = None

    if lstm_ok and xgb_proba_oos is not None:
        lstm_model = lstm_result["model"]
        lstm_model.eval()

        X_seq_oos_np, y_seq_oos_np = _make_sequences(
            np.concatenate([X_is_np[-SEQ_LEN:], X_oos_np]),
            np.concatenate([y_is_np[-SEQ_LEN:],  y_oos_np]),
            SEQ_LEN
        )
        X_seq_oos_np = X_seq_oos_np[:len(X_oos_np)]
        y_seq_oos_np = y_seq_oos_np[:len(y_oos_np)]

        with torch.no_grad():
            Xt = torch.tensor(X_seq_oos_np, dtype=torch.float32).to(device)
            lstm_logits_oos = lstm_model(Xt).cpu().numpy()

        offset = len(X_oos_np) - len(X_seq_oos_np)
        xgb_proba_aligned = xgb_proba_oos[offset:]
        y_oos_aligned     = y_cat_oos.values[offset:]

        ensemble_preds, ensemble_proba = _ensemble_predict(
            xgb_proba_aligned, lstm_logits_oos
        )
        ensemble_acc_oos = float((ensemble_preds == y_oos_aligned).mean() * 100)

        print(f"[spx_ml_v2/{entry_point}] Ensemble OOS={ensemble_acc_oos:.1f}% | "
              f"XGB={best_xgb['acc_oos']:.1f}% | "
              f"LSTM={lstm_result['acc_oos']:.1f}%", flush=True)

    # ── Binary FORT model ─────────────────────────────────
    print(f"[spx_ml_v2/{entry_point}] Entraînement modèle binaire FORT...",
          flush=True)
    binary_result = train_binary_fort(
        X_is, y_cat_is, X_oos, y_cat_oos,
        y_amp_is, y_amp_oos,
        precision_threshold=PRECISION_THRESHOLD
    )
    binary_best = binary_result.get("best") if binary_result.get("ok") else None
    binary_data = binary_result.get("models", {}).get(binary_best, {}) if binary_best else {}

    # ── Résultat final ────────────────────────────────────────
    oos_scores = {
        "xgb":      best_xgb["acc_oos"],
        "lstm":     lstm_result.get("acc_oos", 0) if lstm_ok else 0,
        "ensemble": ensemble_acc_oos or 0,
    }
    best_method = max(oos_scores, key=oos_scores.get)
    best_oos    = oos_scores[best_method]

    is_reliable = best_oos >= OOS_MIN_RATE * 100

    print(f"[spx_ml_v2/{entry_point}] Meilleure méthode: {best_method} "
          f"OOS={best_oos:.1f}% | Fiable={is_reliable}", flush=True)

    try:
        clf_best = xgb_result["models"][best_xgb_name]["clf"]
        imp = pd.Series(
            clf_best.feature_importances_,
            index=selected_features
        ).sort_values(ascending=False)
        top_features = imp.head(15).round(4).to_dict()
    except Exception:
        top_features = {f: None for f in selected_features[:15]}

    from collections import Counter
    pred_xgb_oos = best_xgb["pred_oos"]
    dist_pred = Counter(pred_xgb_oos.tolist())
    dist_real = Counter(y_cat_oos.tolist())

    return {
        "ok":                    True,
        "entry_point":           entry_point,
        "target":                target_horizon,
        "best_model":            f"v2_{best_method}",
        "category_accuracy":     round(best_oos, 2),
        "category_accuracy_is":  round(best_xgb["acc_is"], 2),
        "category_accuracy_oos": round(best_oos, 2),
        "amplitude_mae":         best_xgb["mae"],
        "n_train":               len(X_is),
        "n_test":                len(X_oos),
        "is_reliable":           is_reliable,
        "top_features":          top_features,
        "selected_features":     selected_features,
        "pred_distribution": {
            "fort":      int(dist_pred.get(2, 0)),
            "incertain": int(dist_pred.get(1, 0)),
            "faible":    int(dist_pred.get(0, 0)),
        },
        "test_distribution": {
            "fort":      int(dist_real.get(2, 0)),
            "incertain": int(dist_real.get(1, 0)),
            "faible":    int(dist_real.get(0, 0)),
        },
        "v2": {
            "device":           str(device),
            "xgb_acc_oos":      best_xgb["acc_oos"],
            "xgb_cv_mean":      best_xgb.get("cv_mean"),
            "xgb_cv_std":       best_xgb.get("cv_std"),
            "lstm_ok":          lstm_ok,
            "lstm_acc_oos":     lstm_result.get("acc_oos") if lstm_ok else None,
            "ensemble_acc_oos": ensemble_acc_oos,
            "best_method":      best_method,
            "oos_scores":       oos_scores,
            "binary_ok":        binary_result.get("ok"),
            "binary_precision_curve": binary_data.get("precision_curve"),
            "binary_cv_prec":   binary_data.get("cv_prec_mean"),
            "binary_cv_prec_std": binary_data.get("cv_prec_std"),
            "binary_top_features": binary_data.get("top_features"),
        },
        "_xgb_clf":       xgb_result["models"][best_xgb_name]["clf"],
        "_xgb_reg":       xgb_result["models"][best_xgb_name]["reg"],
        "_lstm_model":    lstm_result.get("model") if lstm_ok else None,
        "_lstm_ok":       lstm_ok,
        "_device":        device,
        "_selected_feat": selected_features,
        "_mean_is":       mean_is,
        "_std_is":        std_is,
        "_best_method":   best_method,
        "_binary_clf":    binary_data.get("clf"),
        "_binary_lib":    binary_best,
    }


def predict_today_v2(trained: dict) -> dict:
    """
    Prédit l'amplitude pour aujourd'hui avec le modèle V2.
    Interface identique à predict_today() de spx_ml.py.
    """
    if not trained.get("ok"):
        return {"ok": False, "error": trained.get("error", "Modèle V2 non entraîné")}

    entry_point = trained["entry_point"]
    target      = trained["target"]

    X, _, _ = build_feature_matrix(entry_point, target)
    if X is None or X.empty:
        return {"ok": False, "error": "Pas de features pour aujourd'hui"}

    selected = trained["_selected_feat"]
    X_sel    = X[selected].iloc[[-1]]

    clf = trained["_xgb_clf"]
    reg = trained["_xgb_reg"]

    xgb_cat   = int(clf.predict(X_sel)[0])
    xgb_amp   = float(reg.predict(X_sel)[0])
    xgb_proba = clf.predict_proba(X_sel)[0] if hasattr(clf, "predict_proba") else None

    lstm_logits = None
    lstm_model  = trained.get("_lstm_model")

    if lstm_model is not None and trained.get("_lstm_ok"):
        device   = trained["_device"]
        mean_is  = trained["_mean_is"]
        std_is   = trained["_std_is"]

        X_all    = X[selected]
        X_np_all = (X_all.values.astype(np.float32) - mean_is) / std_is

        if len(X_np_all) >= SEQ_LEN:
            seq = X_np_all[-SEQ_LEN:]
            Xt  = torch.tensor(seq[np.newaxis], dtype=torch.float32).to(device)
            lstm_model.eval()
            with torch.no_grad():
                lstm_logits = lstm_model(Xt).cpu().numpy()[0]

    best_method = trained.get("_best_method", "xgb")

    if best_method == "ensemble" and xgb_proba is not None and lstm_logits is not None:
        preds, combined_proba = _ensemble_predict(
            xgb_proba[np.newaxis], lstm_logits[np.newaxis]
        )
        final_cat   = int(preds[0])
        final_proba = combined_proba[0]
    elif best_method == "lstm" and lstm_logits is not None:
        lstm_exp    = np.exp(lstm_logits - lstm_logits.max())
        final_proba = lstm_exp / lstm_exp.sum()
        final_cat   = int(final_proba.argmax())
    else:
        final_cat   = xgb_cat
        final_proba = xgb_proba if xgb_proba is not None else np.array([0., 0., 0.])

    cat_label = {0: "FAIBLE", 1: "INCERTAIN", 2: "FORT"}

    probas = {
        "faible":    round(float(final_proba[0]) * 100, 1) if len(final_proba) > 0 else None,
        "incertain": round(float(final_proba[1]) * 100, 1) if len(final_proba) > 1 else None,
        "fort":      round(float(final_proba[2]) * 100, 1) if len(final_proba) > 2 else None,
    }

    return {
        "ok":                 True,
        "entry_point":        entry_point,
        "target":             target,
        "date":               X.index[-1].strftime("%d/%m/%Y"),
        "amplitude_category": cat_label.get(final_cat, "?"),
        "amplitude_pct":      round(xgb_amp, 3),
        "amplitude_pts":      round(xgb_amp / 100 * 5500, 1),
        "probabilities":      probas,
        "ric_signal":         final_cat == 2,
        "ic_signal":          final_cat == 0,
        "model":              f"v2_{best_method}",
        "is_reliable":        trained.get("is_reliable", False),
        "v2_detail": {
            "xgb_cat":      cat_label.get(xgb_cat, "?"),
            "lstm_logits":  lstm_logits.tolist() if lstm_logits is not None else None,
            "best_method":  best_method,
        }
    }


def get_or_train_v2(entry_point: str = "9h30",
                    target: str = "120min") -> dict:
    """Retourne le modèle V2 depuis le cache ou l'entraîne."""
    key = f"v2_{entry_point}_{target}"
    if key not in _CACHE_V2:
        _CACHE_V2[key] = train_v2(entry_point, target)
    return _CACHE_V2[key]


def clear_cache_v2():
    """Vide le cache V2."""
    global _CACHE_V2
    _CACHE_V2.clear()
    gc.collect()
