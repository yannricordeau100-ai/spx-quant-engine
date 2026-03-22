import json,sys
from pathlib import Path
import pandas as pd

APP_DIR=Path(__file__).resolve().parent
DATA_DIR=APP_DIR/"data_runtime"
CFG_DIR=APP_DIR/"config"
SEC_DIR=APP_DIR/"security"

required_files=[
    DATA_DIR/"pattern_query_registry.csv",
    DATA_DIR/"current_query_context.csv",
    DATA_DIR/"horizon_no_edge_status.csv",
    DATA_DIR/"strict_current_propagation_board.csv",
    CFG_DIR/"local_app_config.json",
    CFG_DIR/"ui_runtime_hints.json",
    CFG_DIR/"csv_ingestion_governance.json",
    SEC_DIR/"auth_config.json",
]

missing=[str(p) for p in required_files if not p.exists()]
if missing:
    print("FICHIERS MANQUANTS:")
    for m in missing: print(m)
    sys.exit(1)

reg=pd.read_csv(DATA_DIR/"pattern_query_registry.csv")
cur=pd.read_csv(DATA_DIR/"current_query_context.csv")
noe=pd.read_csv(DATA_DIR/"horizon_no_edge_status.csv")
scp=pd.read_csv(DATA_DIR/"strict_current_propagation_board.csv")

summary={
    "pattern_query_registry_rows":int(len(reg)),
    "current_query_context_rows":int(len(cur)),
    "horizon_no_edge_status_rows":int(len(noe)),
    "strict_current_propagation_board_rows":int(len(scp)),
    "current_actionable_matches":int(cur["is_current_actionable_repaired"].astype(str).str.lower().isin(["true","1","yes"]).sum()) if "is_current_actionable_repaired" in cur.columns else 0
}
print(json.dumps(summary,indent=2))
print("SMOKE_CHECK_OK")
