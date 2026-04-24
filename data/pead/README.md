# PEAD — Post-Earnings Announcement Drift

Module ajouté en v2.22.0 (2026-04-24). Zone exclusive `data/pead/**`, séparée de la zone BBE (`data/live_selected/tickers/**`).

## Univers

- **Russell 1000** (holdings iShares IWB, 1003 tickers)
- **Filtre** : market_cap ∈ [$20B, $500B] (large caps), exclut Utilities / Financial Services / Real Estate
- **Résultat** : 323 tickers, market_cap médian $50.7B, 22 analystes médian
- Fichier : `universe.csv`

## Pattern

1. **Compression pré-earnings J-5..J-1** : moyenne du ratio `(High-Low) / mean(High-Low,20j)` < 0.7 (default)
2. **Surprise earnings J** : Close_J vs Close_J-1 ≥ ±10 %
3. **Entry** : Open J+1 · **Exit** : Close J+5
4. **Filtre direction par défaut** : Long only (les shorts ont WR < 30 % sur backtest)

## Résultats backtest 2020-2026 (large caps, Long only)

| Config | N | Win rate | Avg PnL J+1→J+5 |
|---|---|---|---|
| **Avg<0.7 + 10 % + Long** (défaut) | **10** | **70 %** | **+2.3 %** |
| All<0.8 + 10 % + Long | 2 | 100 % | +5.1 % |
| Avg<0.7 + 5 % + Long | 37 | 54.1 % | +1.17 % |

La spec stricte initiale (`all < 0.5`) donne **0 signaux historiques** sur les 5046 earnings events de l'univers — pattern trop restrictif. Les paramètres sont configurables via sliders dans l'UI.

## Fichiers

| Chemin | Contenu |
|---|---|
| `universe.csv` | 323 tickers enrichis (market_cap, n_analysts, sector) |
| `tickers/<TICKER>.csv` | OHLCV daily 2020→aujourd'hui (323 fichiers) |
| `earnings/<TICKER>.csv` | date, timing (bmo/amc), eps_actual, eps_estimated, surprise_pct (210 fichiers) |
| `signals/scan_<YYYYMMDD>.json` | Résultats des scans quotidiens |
| `alerts.log` | Historique des alertes envoyées (Telegram + email) |

## Commandes CLI

```bash
python3 pead_engine.py universe   # rebuild univers
python3 pead_engine.py ohlcv      # download OHLCV (323 tickers)
python3 pead_engine.py earnings   # fetch earnings historique
python3 pead_engine.py backtest   # full backtest
python3 pead_engine.py scan       # scan quotidien (dry-run)
python3 pead_engine.py upcoming   # earnings ±3j
```

## Scan automatique (LaunchAgent macOS)

Installation :
```bash
cp pead_launchagent.plist ~/Library/LaunchAgents/com.yann.spxquant.pead.plist
launchctl load ~/Library/LaunchAgents/com.yann.spxquant.pead.plist
```

Programmé à :
- **22:05 Paris** (16:05 NY, 5 min après close US) → détecte earnings AMC
- **15:25 Paris** (9:25 NY, 5 min avant open US) → détecte earnings BMO

Désinstallation :
```bash
launchctl unload ~/Library/LaunchAgents/com.yann.spxquant.pead.plist
rm ~/Library/LaunchAgents/com.yann.spxquant.pead.plist
```

## Alerting

Réutilise `beta2_engulfing/notifiers.py` (Telegram + Resend email, déjà configuré par la conv BBE). Deux types d'alertes :
- **Pré-earnings** : compression détectée sur un ticker dont l'earnings est dans ≤ 3 jours
- **Signal** : surprise ±10 % détectée sur earnings day, avec recommandation entry/exit

## Split avec la conv BBE

Zone PEAD exclusive : tout sous `data/pead/**`, `pead_engine.py`, `pead_ui.py`, `pead_launchagent.plist`.

Zone BBE exclusive (ne pas toucher) : `ticker_analysis.py`, `data/live_selected/tickers/**`, `beta2_engulfing/**`.

Zone partagée (app_local.py) : patch minimal, un appel à `render_pead_tab()` uniquement.
