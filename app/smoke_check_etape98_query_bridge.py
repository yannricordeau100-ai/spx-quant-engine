import os,json,traceback
from runtime_query_bridge import run_query

BASE="/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/app_streamlit/data_runtime"
QUESTIONS=[
    "moyenne de gold_daily_ret quand has_macro_event = 1",
    "moyenne de dxy_ret quand macro_impact_high = 1",
    "montre les dates quand ratio_spx_put_call_value > 1",
]
out=[]
for q in QUESTIONS:
    r=run_query(BASE,q,preview_rows=5)
    if r.get("ok"):
        rr=r["result"]
        out.append({
            "question":q,
            "status":rr.get("status"),
            "answer_type":rr.get("answer_type"),
            "metric":rr.get("metric"),
            "value":rr.get("value"),
            "row_count_filtered":rr.get("row_count_filtered"),
            "preview_len":len(rr.get("preview",[])),
        })
    else:
        out.append({"question":q,"status":"ERROR","error":r.get("error")})
print("=== ETAPE98_LOCALHOST_SMOKE ===")
print(json.dumps(out,ensure_ascii=False,indent=2))
