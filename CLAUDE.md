# SPX QUANT ENGINE — CLAUDE CODE MEMORY
## Dernière mise à jour : 2026-04-14 — version v2.21.3

---

## 1. PROJET

**Nom** : SPX Quant Engine
**Fichier principal** : `app_local.py` — version **v2.21.3**
**Repo HF** (secondaire, pas prioritaire) : https://huggingface.co/spaces/TBQCH/spx-quant-engine
**Stack** : Python + Streamlit + pandas + DuckDB + Ollama (sqlcoder:latest) + PyTorch (MPS) + XGBoost + LightGBM
**Local** : ~/spx-quant-engine/
**Data** : ~/spx-quant-engine/data/live_selected/ (tous les CSV sont ici, à plat)
**UI locale** : `python3 -m streamlit run app_local.py --server.port 8503`

### Modules clés (v2.17.5)
- `app_local.py` — UI Streamlit + couches C1 (regex) et C2 (LLM+DuckDB) + onglet ML·Patterns SPX Edge
- `feature_engineering.py` — pipeline `build_all_features()` produisant **2731 features** par session
- `cross_feature_library.py` — 6 catégories de features cross-asset économiquement motivées (vol term structure, divergences momentum, options flow, refuge, cross-frequency, régimes composites) avec scoring de priorité théorique
- `spx_pattern_search.py` — recherche exhaustive de patterns combinatoires (univariate → combos 2/3/4/5 features), validation walk-forward, RIC + IC + filtres VIX
- `spx_ml.py` — version stable XGBoost/LightGBM (référence)
- `spx_ml_v2.py` — XGBoost + LSTM PyTorch + ensemble (mode binaire FORT)
- `calendar_features.py` — features macro depuis `calendar_events_daily.csv` (FOMC/CPI/NFP/OpEx)
- `query_interpreter.py` / `query_executor.py` — couche C1 regex (engulfing, lookups, filter_abs, etc.)
- `render_engine.py` — rendu HTML/Chart.js des résultats interprétés

### Composition des 2731 features (entry 9h30)
- 174 raw daily (toutes colonnes × tous CSV daily, shift J-1)
- 13 intraday J (OR30, VIX open J, VIX1D/VIX open ratio, overnight futures)
- 1154 dérivées intra-CSV (momentum 1/3/5/10/20j, z-score 20/60, percentile 252j, accélération, distance MA, vol réalisée)
- 224 cross-asset (44 base + **180 cross_feature_library priorité ≥ 2** : ratios VIX, divergences SPX/IWM/QQQ/Nikkei/Gold/Bonds, options flow composite, refuge triangle, cross-frequency, régimes)
- 39 temporal/régimes (jour semaine, mois, calendar FOMC/CPI/NFP, régimes VIX low/normal/high/crisis)
- 636 lags temporels intra-CSV (J-2/3/5/10, diffs entre lags, streaks bull/bear, croisements MA, breakouts)
- 380 microstructure (body, shadows, gap, true range, ATR5/14, doji, séquences candles)
- 40 transitions de régime (franchissements VIX, compression vol, drawdown ATH, breadth)
- 57 sentiment options (PC ratios extreme fear/greed, composite_fear_index, IV rank/percentile)
- 41 inter-marchés (lead-lag Nikkei/DAX, risk-on composite, yield curve, DXY/Gold)

### Nouveautés v2.21.3
- **Ollama bloqué pour tickers individuels** : si `_detect_individual_ticker()` trouve un ticker dans la question, Couche 2 (Ollama/DuckDB) n'est jamais appelée → réponse instantanée ou message d'erreur clair
- **16 synonymes de tickers** : apple→AAPL, google→GOOG, nvidia→NVDA, reddit→RDDT, robinhood→HOOD, applovin→APP, ondas→ONDS, iren→IREN, etc.
- **Toutes les questions < 1s** : 10/10 questions testées OK (max 0.36s pour Groq API sur questions macro, 0.01-0.03s pour tickers)

### Nouveautés v2.21.2
- **Auto-scan tickers/** : `_INDIVIDUAL_TICKERS` enrichi automatiquement depuis `TICKERS_DIR/*.csv` au démarrage (11 tickers : AAOI, AAPL, APP, COHR, GOOG, HOOD, IREN, MELI, MU, ONDS, RDDT)
- **Zéro Ollama pour tickers** : toutes les questions sur tickers individuels passent par `interpret_query` → `execute_query` (0.01s) ou `ticker_analysis` — jamais C2/Ollama
- **4 blocs métriques HTML** pour TICKER_ANALYSIS analyse filtrée : OCCURRENCES (#06b6d4), VAR MOYENNE, VAR MOY J+1, % POSITIF J+1 — fond #0d1117, border-radius 12px, bordure verte si taux succès élevé
- **Distribution par palier** : chart Altair bleu #1c71d8 avec labels paliers
- **Cache TTL 300s** (5 min au lieu de 1h)
- **Seuil BBE 2%** confirmé partout (session_state, interp, executor)

### Nouveautés v2.21.0 (stabilisation)
- **Routing corrigé pour engulfing** : `engulfing_analysis` et `engulfing_by_year` sont interceptés AVANT `dispatch_render` (render_engine) pour être rendus en Streamlit natif avec curseurs interactifs. Supporte les deux clés `sub_type` (query_executor) et `sub` (ticker_analysis).
- **Bloc `engulfing_by_year` robuste** : affiche TOUJOURS les données via `result["dates_detail"]` (données déjà calculées), recalcule seulement si J+ ≠ 5. Fallback garanti si le recalcul échoue.
- **Bloc `engulfing_analysis` vérifié** : curseurs J+1..5 + historique 3-60 mois + SPX J+1 pour les échecs (INTERPRETED et TICKER_ANALYSIS).
- **TICKER_ANALYSIS BBE handler** : détecte `sub="engulfing_analysis"` et affiche le rendu avec curseurs (même code que INTERPRETED).
- **11 tickers dans `data/live_selected/tickers/`** : AAOI, AAPL, APP, COHR, GOOG, HOOD, IREN, MELI, MU, ONDS, RDDT — tous avec earnings CSV.
- **Seuils sidebar** : `be_seuil` et `bull_seuil` defaults 2.0% dans la sidebar, mais le renderer `engulfing_by_year` utilise sa propre logique (direction uniquement, min(low,close) < close_J).
- **Vérification fonctionnelle** : "SPX quand VIX > 18" ✅ | "bearish engulfing AAOI en 2024 2025" ✅ | 20/20 régression ✅

### Nouveautés v2.20.0
- **Performance** : `@st.cache_data(ttl=3600)` sur `_cached_load_csv()` pour VIX et SPX daily — évite les relectures CSV à chaque rerun Streamlit
- **Routing AAOI** : `_get_known_tickers()` dans `query_interpreter.py` scanne `data/live_selected/tickers/` en plus de `data/live_selected/` — AAOI détecté comme ticker valide ✅
- **query_executor.py** : `_load_daily()` et `_load_earnings()` cherchent dans `tickers/` en plus de `live_selected/`. `_load_csv_by_name()` aussi.
- **Seuil BBE 0%** : `be_seuil` et `bull_seuil` defaults 2.0 → 0.0 dans `_exec_engulfing_analysis()`. Succès = `cc < close_j` (bearish strict) ou `cc > close_j` (bullish strict). Plus de threshold de 2% par défaut.
- **`_load_earnings()` dans ticker_analysis.py** : cherche dans `TICKERS_DIR`, auto-detect separator `;`/`,`, `format="mixed"` pour les dates

### Nouveautés v2.19.9 (final)
- **Handler BBE dédié dans `ticker_analysis.py`** : `_BBE_RE` regex + handler dans `analyze_ticker()` AVANT le count handler. Détecte "bearish engulfing", "bullish e", etc. Retourne `sub="engulfing_analysis"` avec rows contenant vol_ratio, body_ratio, best_move, success. Earnings ±5j exclus via `load_earnings_dates()`.
- **Renderer `engulfing_analysis` refondu dans `app_local.py`** : curseurs J+1..J+5 et historique 3-60 mois avec recalcul live. Le CSV ticker est rechargé, le win rate est recalculé selon la fenêtre J+ choisie. **SPX J+1 affiché pour les échecs** (charge SPX_daily.csv et affiche la variation SPX le lendemain de chaque échec). 6 derniers signaux en cards HTML.

### Nouveautés v2.19.9
- **Réponse `engulfing_analysis` interactive** : curseurs J+1..J+5 et historique 3-60 mois dans la réponse C1 elle-même. Le CSV du ticker est rechargé, `detect_engulfing_strict()` et `load_earnings_dates()` sont appelés, le win rate est recalculé en temps réel selon les paramètres choisis. Cards HTML avec Best J+1..J+{jend} + ✅/❌.
- **BBE perf_jend = meilleure perf J+1..J+jend** : bearish utilise min(low, close), bullish utilise max(high, close). Succès = au moins un jour dans la direction attendue.
- **Fix données futures tronquées** : `_df_bbe_full` conserve toutes les données pour le calcul J+1..J+5 même après cutoff.
- **Filtres avancés** : Volume > MA20 + RSI J-1 minimum (bearish en surachat)

### Nouveautés v2.19.5
- **Fenêtre BBE dynamique J+1 à J+5** : curseur `select_slider` pour choisir la fenêtre de validation (jours ouvrés — les CSV ne contiennent que des jours de marché, donc shift naturel correct)
- **Win rate calculé en temps réel** selon j_end sélectionné, affiché en grand au-dessus des cards (vert ≥60%, jaune ≥45%, rouge <45%)
- **Performance par signal** : chaque card BBE affiche la perf sur la fenêtre j_end + ✅/❌ succès/échec
- **Filtre VIX minimum** (bearish) : slider 0-30, exclut les signaux en marché trop calme
- **Filtres avancés** (expander ⚙️) : cours < MA20 (tendance baissière requise) + corps min % du range H-L + gap up/neutre à l'open
- **VIX daily chargé** une fois pour tous les tickers, utilisé dans le filtre bearish

### Nouveautés v2.19.4
- **BBE = Bearish/Bullish Engulfing** — terme officiel du projet
- **`detect_engulfing_strict()`** dans `ticker_analysis.py` : détection BBE avec confirmation volume (vol J > vol J-1) + corps J > corps J-1 × 1.1 + filtre earnings ±5j
- **`load_earnings_dates(ticker)`** : charge `ticker_earnings.csv` (auto-detect separator ;/,) depuis `live_selected/` ou `live_selected/tickers/`
- **`_find_ticker_csv(ticker)`** : cherche le CSV dans DATA_DIR et TICKERS_DIR
- **`TICKERS_DIR = data/live_selected/tickers/`** créé automatiquement au lancement, scanné par l'app pour les tickers individuels
- **Expander "🕯️ BBE — Analyse multi-ticker"** dans l'UI principale : multiselect jusqu'à 5 tickers, choix bearish/bullish/les deux, historique 3-36 mois, détection stricte inline avec vol_ratio/body_ratio par signal, 5 derniers signaux affichés en cards HTML colorées
- Auto-exclusion des indices/ratios du sélecteur de tickers (INDICES_EXCLUDE étendu)

### Nouveautés v2.19.3
- **`build_extended_feature_matrix` cible corrigée** : utilise `abs(close-open)/open*100` (amplitude directionnelle nette) au lieu de `abs(pct_change)` — plus cohérent avec la logique RIC/options
- **`_human_readable_feature()` refactorisée** : ~130 mappings par préfixe + nettoyage automatique de ~40 suffixes courants (z20, mom3d, pct252, distma, breakout, etc.) → couvre 99% des features générées
- **Onglet 📅 Validation 2025** : sélecteurs entry/horizon + bouton Valider → appelle `run_validation_2025()`, affiche les résultats avec 🟢/🟡/🔴 par précision 2025 et delta vs OOS global
- **`check_today_signals()` complet** dans `spx_pattern_search.py` : parcourt tous les patterns ≥90% OOS, évalue sur features J-1, recommande structure options si VIX connu
- **`run_validation_2025()`** : isole sessions 2025, recalcule précision par pattern, compare OOS global vs 2025
- **`run_tomorrow.sh` v2.19.3** : 5 batches (RIC VIX≤17, RIC VIX17-22, IC VIX≤25, grille, validation 2025)

### Nouveautés v2.19.2
- **P&L simulation corrigé** : RIC/RIB utilisent `abs(close-open)` (mouvement directionnel net), IC/IB utilisent `high-low` (amplitude max intraday adverse). Gains partiels modélisés entre 50-100% de l'aile pour RIC, entre 60-100% pour IC.
- **`check_today_signals(entry_point, vix_open_today)`** dans `spx_pattern_search.py` : évalue tous les patterns ≥90% OOS sur features J-1, retourne signaux actifs + recommandation structure options selon VIX (meilleure entre RIC±40 et RIB±20→40 pour RIC, IC±40 pour IC)
- **`run_validation_2025(entry_point, horizon)`** : isole les sessions 2025 de l'OOS et recalcule la précision de chaque pattern ≥90% — compare précision 2025 vs OOS global pour détecter la dégradation
- **Onglet "Signal aujourd'hui" refondu** : sélecteur entry_point + champ VIX open (optionnel) + bouton Vérifier → signaux actifs avec conditions traduites + recommandation options complète (6 structures affichées si VIX fourni)

### Nouveautés v2.19.1
- **`simulate_pattern_economics()`** dans `options_validator.py` : pour chaque date OOS d'un pattern, calcule le P&L réel des structures (RIC/IC/RIB/IB) en croisant VIX_open du jour (CSV daily) × amplitude SPX réelle (high-low + close-open) × primes options interpolées. Retourne stats globales (win_rate, avg_pnl, total_pnl, n_trades) + détail par session.
- **`get_spx_amplitude_distribution()`** : distribution des amplitudes SPX (high-low et close-open) par régime VIX (≤17, 17-22, ≥22), avec % de sessions où SPX bouge ≥ 10/15/20/25/30/35/40/50/60 pts.
- **`oos_dates`** ajouté dans chaque dict pattern (univariate + combo2/3/4/5) dans `spx_pattern_search.py` — permet de retrouver exactement les dates où le pattern s'est déclenché en OOS.
- **Onglet 💰 Options refondu** en simulateur interactif :
  - Section 1 : calculateur VIX → primes (avec slider VIX)
  - Section 2 : distribution amplitudes SPX par régime VIX (3 colonnes)
  - Section 3 : sélecteur de pattern parmi tous les fichiers `data/patterns_*.json` + sélecteur de structure (RIC/IC/RIB/IB) → P&L par session affiché avec emoji ✅/❌

### Nouveautés v2.19.0
- **`options_validator.py`** créé — validation économique des patterns via 14 chains options 0DTE SPX réelles
- **Convention projet documentée** :
  - `RIC = CallAsk(K) + PutAsk(K) - CallBid(K+w) - PutBid(K-w)` (débit, gain max si SPX bouge ≥w)
  - `IC = CallBid(K) + PutBid(K) - CallAsk(K+w) - PutAsk(K-w)` (crédit, gain max si SPX reste dans ±w)
  - `RIB / IB` = versions avec zone tampon (inner/outer wings)
- **`PRECOMPUTED` dict** : gains précalculés pour 13 niveaux VIX (14.16 → 30.80) × 6 stratégies
- **`interpolate_gains(vix)`** : interpolation linéaire pour VIX continu
- **Onglet 💰 Options** dans l'app : calculateur interactif avec slider VIX, badges de rentabilité, courbe gains vs VIX, validation économique des 6 patterns prioritaires
- **`run_tomorrow.sh`** créé — 4 batches RIC/IC/grille prêts à lancer

### Nouveautés v2.18.0 / v2.18.1
- **`run_grid_search()` opérationnel** dans `spx_pattern_search.py` — grille 4×4 (RIC 0.30/0.35/0.40/0.45 × VIX max 19/20/21/22), résultats sauvegardés dans `data/grid_results.json`
- **Panneau interactif "🎯 Grille RIC"** dans l'onglet ML : 3 sliders (RIC × VIX × Win rate) + résultats triés par fiabilité, contraintes structurelles détectées, vue d'ensemble matricielle
- **Tri amélioré dans `_show_patterns()`** : 100% OOS en premier, puis robuste WF, puis OOS%, puis occurrences
- **Index auto-étendu** : `build_all_features(index=None)` couvre 2020-04 → 2026-02 (1481 sessions au lieu de 791)
- **Features intraday J-1** (`build_intraday_jmoins1_features`) : 29 features depuis SPX_5min, SPX_FUTURE_30min, SPY_30min — OR30 ratio J-1, overnight futures (ov_ret/range/premkt), close-vs-VWAP SPY30, gap_up/down, large_overnight, z-scores 20j
- **`MIN_OCCURRENCES_OOS = 6`** (était 8) pour permettre des patterns avec moins d'occurrences mais plus d'historique
- **CSS sliders gris** uniforme pour cohérence visuelle

### 5 patterns prioritaires actuels (v2.17.5)
1. **Pattern A** — RIC / 9h30 / Tous régimes : 100% OOS / 11 occ / WF 4/6 — VTS VIX9j/VIX3m inversée + breadth dégradée
2. **Pattern B** — RIC / 9h30 / Tous régimes : 100% OOS / 10 occ / WF 3/4 — VIX open extrême + ratio VIX1j/VIX anormal
3. **Pattern C** — RIC / 9h30 / VIX 19-22 : 83.3% OOS / 12 occ / WF 6/8 — Hausse RSI put/call actions sur 5j
4. **Pattern D** — RIC / 10h00 / Tous régimes : 100% OOS / 9 occ / WF 3/4 — SPX en chute 5j + VIX 3 mois en hausse 10j + VIX high élevé
5. **Pattern IC-E** — IC / 10h00 / VIX ≤ 19 : 87.5% OOS / 8 occ / WF 4/6 — Décélération put/call SPX + interaction VVIX×VIX9j faible

**Objectif** : Moteur de recherche quant en langage naturel sur données boursières CSV.  
L'utilisateur pose une question en français ; le moteur répond avec stats + tableau exportable.

### Architecture deux couches

| Couche | Déclencheur | Technologie | Réponse |
|--------|------------|-------------|---------|
| **C1** | Question avec `ASSET OP SEUIL` ou filtre weekday/overnight/intraday/calendrier | Regex + pandas | Stats fréquence + variation + bar chart |
| **C2** | Toute question non reconnue par C1 | sqlcoder:latest (Ollama) + DuckDB | SQL → DataFrame → metric ou tableau |

---

## 2. RÈGLES ABSOLUES

- **Ne jamais faire de fallback silencieux** — si un dataset manque, erreur explicite, jamais de remplacement par un autre dataset
- **Ne jamais dupliquer les résultats** — si plusieurs CSV couvrent la même période, utiliser le canonique
- **Ne jamais modifier app_local.py partiellement** — toujours lire avant d'éditer, patch ciblé uniquement
- **Version tag visible** — toujours mettre le numéro de version discret sous le titre de l'UI
- **Pas de QCM** — réponse directe, pas de boutons Variation/Direction/Fréquence/Horizon
- **La virgule = le point pour les décimales** — 1,7 == 1.7 dans les questions
- **Questions sans "?"** — toujours interpréter comme une question implicite
- **Ne jamais remplacer un dataset manquant par un autre silencieusement**
- **Ne jamais casser une version qui fonctionne pour une feature non validée**

---

## 3. ARCHITECTURE DATA

### Dossier : data/live_selected/
Tous les CSV sont à plat dans ce dossier. Nouveaux CSV détectés automatiquement par `_build_dynamic_registry()`.

### Fuseaux horaires
- **Heure New York (ET)** : SPX, SPY, QQQ, IWM, VIX, VIX1D, VVIX, VIX3M, VIX9D, VIX6M, VX Futures, SPX Futures, SKEW, Put/Call ratios, Advance/Decline, VIX_SPX_OPEN
- **Heure Paris (CET/CEST)** : DAX40, FTSE100, NIKKEI225, Gold, DXY, OANDA (obligations), Yield Curve
- **Intemporel** : Options chains VIX (snapshots statiques), calendar_events

### CSV canoniques par actif
| Actif | Fichier daily | Notes |
|---|---|---|
| SPX | SPX_daily.csv | Open/Close NY |
| SPY | SPY_daily.csv | |
| QQQ | QQQ_daily.csv | |
| IWM | IWM_daily.csv | |
| VIX | VIX_daily.csv | |
| VIX1D/VIX ratio | VIX1D_VIX_ratio_daily.csv | CSV ratio calculé, priorité absolue |
| VVIX | VVIX_daily.csv | |
| VIX3M | VIX3M_daily.csv | |
| VIX9D | VIX9D_daily.csv | |
| VIX6M | VIX6M_daily.csv | |
| SKEW | SKEW_INDEX_daily.csv | |
| DXY | DXY_daily.csv | |
| Gold daily | Gold_daily.csv | |
| NIKKEI | NIKKEI225_daily.csv | |
| DAX | DAX40_daily.csv | |
| FTSE | FTSE100_daily.csv | |
| SPX Put/Call | SPX_Put_Call_Ratio_daily.csv | |
| QQQ Put/Call | QQQ_Put_Call_Ratio_daily.csv | |
| SPY Put/Call | SPY_Put_Call_Ratio_daily.csv | |
| IWM Put/Call | IWM_Put_Call_Ratio_daily.csv | |
| VIX Put/Call | VIX_Put_Call_Ratio_daily.csv | |
| Equity Put/Call | Equity_Put_Call_Ratio_daily.csv | |
| US 10Y | US_10_years_bonds_daily.csv | |
| US 2Y | OANDA_USB02YUSD, 1D.csv | |
| Yield Curve | Yield_Curve_Spread_10Y_2Y.csv | |
| VIX SPX Open | VIX_SPX_OPEN_daily.csv | VIX à 9h30 NY |
| Advance/Decline | advance_decline_ratio_net_ratio_put_call_daily.csv | |
| Calendar | calendar_events_daily.csv | CRITIQUE — voir section 6 |
| AAPL | AAPL.csv | Action individuelle |
| AAOI | AAOI.csv | Action individuelle |

### CSV intraday disponibles
- SPX : 1min, 5min, 30min
- SPY : 1min, 30min
- QQQ : 1min, 30min
- IWM : 30min
- VIX1D : 1min, 30min
- Gold : 1hour
- Oil : 5min
- TICK : 4hours
- SPX Future : 1min, 5min, 30min
- VX Future : VX1 daily, VX2 daily

---

## 4. LOGIQUE MÉTIER DES QUESTIONS

### Structure d'une question C1
```
[SUJET] quand [CONDITION] [OPÉRATEUR] [SEUIL] [FILTRE OPTIONNEL]
```
Exemples :
- "SPX quand VIX > 18"
- "SPX quand VIX1D/VIX > 1.2 les lundis"
- "QQQ quand DXY > 104 et VIX > 20"
- "SPX quand VIX > 18 le mardi et que l'ouverture est positive par rapport à la veille"

### Actifs SUJET (dont on veut la variation prix)
SPX, SPY, QQQ, IWM, AAPL, AAOI + tout nouveau ticker détecté automatiquement

### Actifs CONDITION (qui portent la condition numérique)
Tous les CSV daily + ratio VIX1D/VIX (priorité absolue sur VIX_daily si question contient "VIX1D/VIX")

### Réponse C1 standard (toujours)
1. **Fréquence** : X% des jours, N jours sur M total, période couverte
2. **Stats sujet** sur ces N jours :
   - Variation moyenne open→close
   - % jours haussiers / baissiers
   - Meilleur jour (date + %)
   - Pire jour (date + %)
3. **Bar chart** vert/rouge
4. **Tableau détaillé** exportable CSV

### Filtres C1 implémentés
- Jour de semaine : "les lundis", "le mardi", "les vendredis"
- Période : "sur 1 an", "depuis 2022", "en 2024"
- Overnight : "ouvre en positif/négatif par rapport à la clôture de la veille"
- Intraday : "entre l'ouverture et 30 min après" (CSV 30min)
- Multi-conditions ET : "quand VIX > 18 ET VIX1D/VIX > 1.2"
- Calendrier économique : publications emploi/NFP, CPI, FOMC, PMI, ISM, PCE, earnings + surprise positive/négative

---

## 5. SYNCHRO TEMPORELLE

### Référence universelle : heure New York (ET)
- "À l'ouverture" = 09:30 ET = Open du daily
- "30 min après l'ouverture" = 10:00 ET → utiliser CSV 30min
- "Dans la journée" = 09:30-16:00 ET
- "Prémarket" = toute période avant 09:30 ET

### CSV en heure Paris (à convertir vers NY)
SPX_1min, SPX_5min, SPX_30min, SPY_1min, SPY_30min, QQQ_1min, QQQ_30min,
IWM_30min, VIX1D_1min, VIX1D_30min, SPX_FUTURE_1min, SPX_FUTURE_5min,
SPX_FUTURE_30min, Gold_1hour, oil_5min, TICK_4hours

### CSV en heure NY (pas de conversion)
Tous les CSV daily, Calendar, VIX_SPX_OPEN_daily

### CSV en heure locale de leur bourse (pas de conversion, alignement sur date calendaire uniquement)
DAX40_daily (Frankfurt), FTSE100_daily (London), NIKKEI225_daily (Tokyo)

### Règles de conversion Paris → NY
Les CSV Paris utilisent l'heure locale Paris qui change automatiquement
(CET hiver = UTC+1, CEST été = UTC+2)

| Période | Heure Paris | Heure NY | Écart |
|---|---|---|---|
| Hiver normal (janv-mars, nov-déc) | 15:30 | 09:30 | -6h |
| Décalage USA été / FR hiver (~2 sem mars) | 14:30 | 09:30 | -5h |
| Été normal (avril-oct) | 15:30 | 09:30 | -6h |
| Décalage FR hiver / USA été (~1 sem nov) | 15:30 | 10:30 | -5h |

**Fonction de conversion à utiliser :**
```python
import pytz
from datetime import datetime

paris_tz = pytz.timezone('Europe/Paris')
ny_tz = pytz.timezone('America/New_York')

def paris_to_ny(dt_paris_naive):
    """Convertit datetime Paris naive → datetime NY naive"""
    dt_paris = paris_tz.localize(dt_paris_naive)
    dt_ny = dt_paris.astimezone(ny_tz)
    return dt_ny.replace(tzinfo=None)
```

### Règle spéciale TICK_4hours
Le TICK a 3 lignes par jour. La première ligne de chaque jour est décalée de +1h30
par rapport à l'ouverture réelle (bug de construction du CSV — espacement 4h fixe).

Correction : **ajouter 1h30 à la première ligne de chaque jour uniquement**

| Ligne | Heure dans fichier (Paris) | Heure NY réelle |
|---|---|---|
| 1ère (hiver) | 14:00 Paris | 09:30 NY ← corriger +1h30 |
| 1ère (décalage mars) | 13:00 Paris | 09:30 NY ← corriger +1h30 |
| 1ère (été) | 14:00 Paris | 09:30 NY ← corriger +1h30 |
| 2ème | 18:00 Paris hiver / 18:00 été | 12:00 NY ✅ |
| 3ème | 22:00 Paris hiver / 22:00 été | 16:00 NY ✅ |

"À l'ouverture" pour TICK = première ligne du jour (après correction = 09:30 NY)

---

## 6. CALENDRIER ÉCONOMIQUE (calendar_events_daily.csv)

Fichier CRITIQUE. Contient :
- Jours fériés US (marchés fermés)
- Jours tronqués (clôture anticipée)
- Publications économiques : date, heure, type, valeur estimée, valeur précédente
- Résultats de sociétés (susceptibles d'influencer le lendemain)

Utilisation :
- "Lors des publications emploi" → filtrer sur type contenant "emploi" / "NFP" / "jobless"
- "Meilleure qu'annoncé" → actual > estimate
- "Moins bonne qu'annoncé" → actual < estimate
- Toujours exclure les jours fériés des calculs sauf si question explicite

**Note** : `calendar_events_daily` est chargé en C1 (pandas) mais **pas encore dans DuckDB C2** — voir section 11 Limitations.

---

## 7. RÈGLES DE DÉPLOIEMENT

```bash
# app_local.py — usage local uniquement
python3 -m streamlit run app_local.py --server.port 8503

# Push HF (app.py, secondaire)
git add -A
git commit -m "description courte"
git push origin main
# HF rebuilde automatiquement (2-3 min)
# Vérifier sur : https://tbqch-spx-quant-engine.hf.space
```

- **Git LFS** est configuré pour les CSV (*.csv dans .gitattributes)
- **Branche** : main
- **app_local.py n'est PAS poussé sur HF** — fichier local uniquement

---

## 8. ROADMAP

### V1 — État au 2026-04-05

| Feature | Statut | Fichier |
|---------|--------|---------|
| Moteur de base : fréquence + variation SPX | ✅ FAIT | app_local.py + app.py |
| VIX1D/VIX ratio sans fallback silencieux | ✅ FAIT | app_local.py |
| Git LFS pour CSV | ✅ FAIT | .gitattributes |
| Filtres jour de semaine (lundi/mardi/…) | ✅ FAIT | app_local.py C1 |
| Filtre overnight (ouverture vs clôture J-1) | ✅ FAIT | app_local.py C1 |
| Fenêtres intraday (open → open+30min) | ✅ FAIT | app_local.py C1 |
| Multi-conditions ET | ✅ FAIT | app_local.py C1 |
| Calendrier économique (publications, surprise) | ✅ FAIT | app_local.py C1 |
| Auto-détection nouveaux CSV dans live_selected/ | ✅ FAIT | `_build_dynamic_registry()` |
| AAPL/AAOI comme sujet ET condition | ✅ FAIT | registre dynamique |
| Tous les Put/Call ratios comme condition | ✅ FAIT | registre dynamique |
| Historique persistant (JSON, 20 entrées) | ✅ FAIT | app_local.py UI |
| Question suivi / navigation sidebar | ✅ FAIT | app_local.py UI |
| Module IC/RIC (Iron Condor / Reverse IC) | ✅ FAIT | app_local.py |
| Couche 2 LLM (sqlcoder + DuckDB) | ✅ FAIT | app_local.py C2 |
| Questions sans condition numérique en C1 | 🔄 EN COURS | parse_query retourne None → C2 |
| calendar_events + CSV 1min dans DuckDB C2 | 🔄 EN COURS | skippés par `_SKIP_RE` actuellement |
| Multi-conditions OU | ❌ À FAIRE | — |

### V2 (PATTERNS — après V1 complète)
- [ ] Détection patterns statistiquement significatifs (p-value < 0.05, min 30 occurrences)
- [ ] XGBoost/LightGBM pour patterns conditionnels courts
- [ ] LSTM pour séquences temporelles longues
- [ ] Validation out-of-sample obligatoire
- [ ] Cross-CSV (même asset, fréquences différentes)
- [ ] Cross-assets (relations entre actifs différents)
- [ ] Millions de calculs : parallélisation nécessaire

### V3 (OPTIONS)
- [ ] Moteur Iron Condor / Reverse IC depuis option chains VIX
- [ ] Calcul à partir du delta 0.5 le plus proche
- [ ] Multi-CSV options simultanément
- [ ] Tailles d'aile variables

---

## 9. CE QU'IL NE FAUT PAS FAIRE

- Ne jamais utiliser SPX_Future comme proxy SPX
- Ne jamais utiliser VIX_daily quand la question dit "VIX1D/VIX"
- Ne jamais afficher des noms de CSV avec __ (double underscore) dans l'UI
- Ne jamais remplacer un dataset manquant par un autre silencieusement
- Ne jamais casser une version qui fonctionne pour une feature non validée
- Ne jamais faire `ollama stop` sans instruction explicite de Yann

---

## 10. ARCHITECTURE LLM LOCAL (app_local.py — Couche 2)

**Fichier** : `app_local.py` — CLI + UI Streamlit, local uniquement.

### Modèle
- **sqlcoder:latest** (7B, ~4.1 GB) — spécialisé SQL analytique
- Pull : `ollama pull sqlcoder:latest`
- Modèles écartés : llama3.2:3b (GROUP BY incorrect), phi3:mini (CTE imbriquées invalides)

### Chargement à la demande
- Ollama **ne démarre pas** au lancement de app_local.py
- Il démarre uniquement quand une question arrive en **Couche 2**
- Il s'arrête automatiquement après **60s d'inactivité** via `ollama stop sqlcoder:latest`
- Flag `_in_flight` : le timer ne stoppe pas le modèle pendant une génération longue
- `num_predict = 700`, timeout génération = 120s, timeout chargement = 90s

### Tables DuckDB chargées (47 tables au 2026-04-05)
Tous les CSV daily de `data/live_selected/` sauf :
- CSV sans colonne `time` parseable (OANDA, option chains, correlation CSVs)
- CSV intraday (1min, 5min) — trop volumineux, non chargés par défaut
- **spx_30min et vix1d_30min sont chargés** (utilisés dans les exemples few-shot)

### Few-shot prompt — 9 exemples (statiques)
1. Variation annuelle SPX (double sous-requête first/last close-open)
2. Meilleur jour de la semaine (CASE français + GROUP BY)
3. JOIN put/call ratio × VIX
4. COUNT jours de bourse par année
5. Range 30min (MAX(high)-MIN(low))
6. Performance par jour de la semaine (tous les jours)
7. Range conditionnel HAVING (sous-requête scalaire COUNT)
8. VVIX moyen par weekday (multi-jours IN)
9. VVIX moyen par weekday avec condition VIX (multi-jours + JOIN)

---

## 11. LIMITATIONS CONNUES C2 (sqlcoder + DuckDB)

| Limitation | Symptôme | Workaround |
|-----------|----------|-----------|
| GROUP BY omis dans CASE queries | Binder Error → retry avec LIMIT 1 | Le retry corrige mais perd les lignes multi-jours |
| Self-JOIN temporels complexes | SQL incorrect ou erreur | Reformuler en deux questions simples |
| calendar_events_daily non chargé dans DuckDB | Questions calendrier × intraday échouent en C2 | Couche 1 gère le calendrier pandas |
| CSV 1min non chargés dans DuckDB | Questions 1min échouent en C2 | Non résolu — données trop volumineuses |
| Timeout si sqlcoder prend > 120s | TimeoutError | Rare — questions très complexes uniquement |

---

## 12. CONTACT / DÉCISIONS

- Toutes les décisions de direction produit viennent de l'utilisateur (Yann)
- Si bloqué sur une décision fonctionnelle → écrire dans BLOCKED.md avec la question
- Si bloqué sur du code → essayer 2 approches différentes, documenter dans BLOCKED.md
- Ne jamais inventer une règle métier non confirmée par Yann
