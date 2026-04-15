#!/bin/bash
# run_tomorrow.sh — v2.19.3 — 5 batches séquentiels
# Durée estimée : ~120 min total
# Lancer : bash ~/spx-quant-engine/run_tomorrow.sh

export KMP_DUPLICATE_LIB_OK=TRUE
export OMP_NUM_THREADS=1
cd ~/spx-quant-engine

echo "==================================================="
echo "BATCH 1/5 — RIC économiquement rentable (VIX ≤ 17)"
echo "Zone où RIC±40 est le plus rentable (max gain 14-20pts)"
echo "==================================================="
python3 -c "
import sys; sys.path.insert(0, '.')
from spx_pattern_search import run_filtered_search
for ep in ['9h30', '10h00']:
    for hz in ['360min', '240min', '180min']:
        run_filtered_search(ep, hz, vix_open_max=17.0)
" 2>&1 | grep -v "Chargé\|\[spx_ml\]\|\[cross_feat\]" \
       | tee results_ric_vix17_$(date +%Y%m%d_%H%M).txt

echo "==================================================="
echo "BATCH 2/5 — RIC VIX 17-22 (zone intermédiaire)"
echo "==================================================="
python3 -c "
import sys; sys.path.insert(0, '.')
from spx_pattern_search import run_filtered_search
for ep in ['9h30', '10h00']:
    run_filtered_search(ep, '360min', vix_open_min=17.0, vix_open_max=22.0)
    run_filtered_search(ep, '240min', vix_open_min=17.0, vix_open_max=22.0)
" 2>&1 | grep -v "Chargé\|\[spx_ml\]\|\[cross_feat\]" \
       | tee results_ric_vix17_22_$(date +%Y%m%d_%H%M).txt

echo "==================================================="
echo "BATCH 3/5 — IC sweet spot VIX 17-25"
echo "Zone où IC±40 rapporte 25-36pts de crédit"
echo "==================================================="
python3 -c "
import sys; sys.path.insert(0, '.')
from spx_pattern_search import run_ic_search
run_ic_search('10h00', '360min', vix_open_max=25.0)
run_ic_search('10h00', '360min', vix_open_max=22.0)
run_ic_search('10h00', '360min', vix_open_max=20.0)
run_ic_search('9h30',  '360min', vix_open_max=22.0)
" 2>&1 | grep -v "Chargé\|\[spx_ml\]\|\[cross_feat\]" \
       | tee results_ic_vix25_$(date +%Y%m%d_%H%M).txt

echo "==================================================="
echo "BATCH 4/5 — Grille complète mise à jour"
echo "==================================================="
python3 -c "
import sys; sys.path.insert(0, '.')
from spx_pattern_search import run_grid_search
run_grid_search(
    entry_points=['9h30', '10h00'],
    horizons=['360min', '240min'],
    ric_thresholds=[0.45, 0.40, 0.35, 0.30],
    vix_filters=[22.0, 21.0, 20.0, 19.0]
)
" 2>&1 | grep -v "Chargé\|\[spx_ml\]\|\[cross_feat\]" \
       | tee results_grid_$(date +%Y%m%d_%H%M).txt

echo "==================================================="
echo "BATCH 5/5 — Validation 2025 sur tous les horizons"
echo "==================================================="
python3 -c "
import sys; sys.path.insert(0, '.')
from spx_pattern_search import run_validation_2025
for ep in ['9h30', '10h00']:
    for hz in ['360min', '240min']:
        run_validation_2025(ep, hz)
" 2>&1 | grep -v "Chargé\|\[spx_ml\]\|\[cross_feat\]" \
       | tee results_val2025_$(date +%Y%m%d_%H%M).txt

echo ""
echo "==================================================="
echo "TOUS LES BATCHES TERMINÉS"
echo "Fichiers générés dans ~/spx-quant-engine/data/"
echo "==================================================="
