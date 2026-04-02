# SPX QUANT ENGINE — CLAUDE CODE MEMORY
## Dernière mise à jour : 2026-04-02

---

## 1. PROJET

**Nom** : SPX Quant Engine  
**Repo HF** : https://huggingface.co/spaces/TBQCH/spx-quant-engine  
**URL app** : https://tbqch-spx-quant-engine.hf.space  
**Stack** : Python + Streamlit + pandas + numpy  
**Local** : ~/spx-quant-engine/  
**Data** : ~/spx-quant-engine/data/live_selected/ (tous les CSV sont ici, à plat)

**Objectif** : Moteur de recherche quant en langage naturel sur données boursières CSV.  
L'utilisateur pose une question en français, le moteur répond avec stats + tableau exportable.

---

## 2. RÈGLES ABSOLUES

- **Ne jamais faire de fallback silencieux** — si un dataset manque, erreur explicite, jamais de remplacement par un autre dataset
- **Ne jamais dupliquer les résultats** — si plusieurs CSV couvrent la même période, utiliser le canonique
- **Toujours donner le fichier app.py entier** — jamais de patch partiel
- **Push HF dès que validé localement** — sans demander confirmation
- **Version tag visible** — toujours mettre le numéro de version (ex: v2.0) discret en haut à droite de l'UI
- **Pas de QCM** — réponse directe, pas de boutons Variation/Direction/Fréquence/Horizon
- **La virgule = le point pour les décimales** — 1,7 == 1.7 dans les questions
- **Questions sans "?"** — toujours interpréter comme une question implicite

---

## 3. ARCHITECTURE DATA

### Dossier : data/live_selected/
Tous les CSV sont à plat dans ce dossier. Nouveaux CSV détectés automatiquement.

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

### Structure d'une question
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

### Réponse standard (toujours)
1. **Fréquence** : X% des jours, N jours sur M total, période couverte
2. **Stats sujet** sur ces N jours :
   - Variation moyenne open→close
   - % jours haussiers / baissiers
   - Meilleur jour (date + %)
   - Pire jour (date + %)
3. **Tableau détaillé** exportable CSV

### Filtres supportés v1
- Jour de semaine : "les lundis", "le mardi", "les vendredis"
- Période : "sur 1 an", "depuis 2022", "en 2024"
- Overnight : "ouvre en positif par rapport à la clôture de la veille"
- Intraday : "entre l'ouverture et 30 min après"
- Multi-conditions : "quand VIX > 18 ET VIX1D/VIX > 1.2"

---

## 5. SYNCHRO TEMPORELLE

- **Référence = heure New York (ET)**
- "À l'ouverture" = 9h30 ET = Open du daily
- "30 min après l'ouverture" = 10h00 ET → utiliser CSV 30min
- Changements d'heure EU/US gérés (décalage 1 semaine parfois)
- "Dans la journée" = 9h30-16h00 ET
- "Prémarket" = toute période avant 9h30 ET

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

---

## 7. RÈGLES DE DÉPLOIEMENT

```bash
# Workflow standard après toute modification
git add -A
git commit -m "description courte"
git push origin main
# HF rebuilde automatiquement (2-3 min)
# Vérifier sur : https://tbqch-spx-quant-engine.hf.space
```

- **Git LFS** est configuré pour les CSV (*.csv dans .gitattributes)
- **Branche** : main
- **Remote** : origin = https://huggingface.co/spaces/TBQCH/spx-quant-engine
- Tester localement avec `streamlit run app.py` avant push si modification majeure

---

## 8. ROADMAP

### V1 (EN COURS)
- [x] Moteur de base : fréquence + variation SPX
- [x] VIX1D/VIX ratio sans fallback silencieux
- [x] Git LFS pour CSV
- [x] Déploiement HF fonctionnel
- [ ] Filtres jour de semaine (lundi/mardi/...)
- [ ] Filtre overnight (ouverture vs clôture J-1)
- [ ] Fenêtres intraday (open → open+30min)
- [ ] Multi-conditions (ET / OU)
- [ ] Calendrier économique (publications emploi, surprise)
- [ ] Auto-détection nouveaux CSV dans live_selected/
- [ ] AAPL/AAOI comme sujet ET condition
- [ ] Tous les Put/Call ratios comme condition

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
- Ne jamais pusher sans avoir vérifié que app.py fait plus de 200 lignes (signe que le fichier est complet)
- Ne jamais remplacer un dataset manquant par un autre silencieusement
- Ne jamais casser une version qui fonctionne pour une feature non validée

---

## 10. CONTACT / DÉCISIONS

- Toutes les décisions de direction produit viennent de l'utilisateur (Yann)
- Si bloqué sur une décision fonctionnelle → écrire dans BLOCKED.md avec la question
- Si bloqué sur du code → essayer 2 approches différentes, documenter dans BLOCKED.md
- Ne jamais inventer une règle métier non confirmée par Yann
