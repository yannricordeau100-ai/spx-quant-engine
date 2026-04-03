# SPX Quant Engine — Session DONE
Date : 2026-04-03

---

## Features livrées

### 1. paris_to_ny (conversion horaire Paris → NY)
- Fonction `paris_to_ny(df)` dans `app.py`
- Appliquée automatiquement lors du `load_csv()` pour tous les fichiers exportés en heure Paris (CET/CEST) : DAX40, FTSE100, NIKKEI225, Gold, DXY, OANDA, Yield Curve
- Utilise `pytz` (ajouté à `requirements.txt`) pour localiser → convertir → dénaïviser
- No-op si les timestamps sont déjà date-only (pas d'heure intraday)

### 2. Correction TICK +1h30
- Fonction `tick_correction(df)` dans `app.py`
- Appliquée automatiquement sur `TICK_4hours.csv` lors du `load_csv()`
- Offset connu : les timestamps TICK sont décalés de +1h30 vs heure NY réelle

### 3. Filtre jour de semaine
- Détection dans la requête : "les lundis", "le mardi", "les vendredis", etc. (FR + EN)
- Exemple : `SPX quand VIX > 18 les vendredis`
- Filtre appliqué après l'intersection des conditions numériques
- Labels FR dans le titre de résultat : "les lundis", "les mardis", etc.

### 4. Filtre overnight
- Détection : "ouvre en positif", "ouverture positive", "ouverture en hausse", "gap up", "open supérieur"
- Logique : `open[J] > close[J-1]` sur SPX_daily.csv
- Exemple : `SPX quand VIX > 18 et ouverture positive`

### 5. Multi-conditions ET
- Split sur `\bET\b` ou `\bAND\b` (case-insensitive) dans la requête
- Intersection des dates filtrées pour chaque condition
- Exemple : `SPX quand VIX > 18 ET VIX1D/VIX > 1.2`
- Compatible avec weekday et overnight simultanément

### 6. Refactor parse_query
- Retourne désormais `{"subject", "conditions": [...], "weekday", "overnight"}` au lieu d'un tuple
- Architecture extensible pour futures conditions

---

## Commits de session

| SHA     | Message |
|---------|---------|
| bd347bb | v2.0 via Claude Code |
| 317f2f8 | add CLAUDE.md |
| f8bb0b2 | force HF restart |
| 5868927 | update CLAUDE.md: timezone rules complètes |
| 3687505 | force HF restart 2 |
| 8fce780 | trigger v2.0 deploy |
| df034ed | feat: paris_to_ny, TICK+1h30, filtre weekday, overnight, multi-conditions ET |

---

## Tests validés (python3 -c)

```
multi-ET:   {'subject': 'spx', 'conditions': [VIX>18, VIX1D/VIX>1.2], ...}
weekday:    {'subject': 'qqq', 'conditions': [VIX<20], 'weekday': 0, ...}
overnight:  {'subject': 'spx', 'conditions': [VIX>18], 'overnight': True, ...}
single:     {'subject': 'spx', 'conditions': [VIX1D/VIX>1.2], ...}
e2e stats:  n=8 jours, mean_var=+0.92%, pct_bull=50%
```

---

## État déploiement HF
- URL : https://tbqch-spx-quant-engine.hf.space
- Dernier commit pushé : df034ed
- Statut au moment de la session : BUILDING (HF build lent, ~5-10 min)
- Aucune erreur détectée

---

## Blockers
Aucun. Voir BLOCKED.md si besoin futur.

---

## Roadmap V1 restante (CLAUDE.md §8)
- [ ] Fenêtres intraday (open → open+30min)
- [ ] Multi-conditions OU
- [ ] Calendrier économique (publications emploi, surprise)
- [ ] Auto-détection nouveaux CSV dans live_selected/
- [ ] AAPL/AAOI comme sujet ET condition simultanément
