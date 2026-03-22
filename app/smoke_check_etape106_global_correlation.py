import os,json
from runtime_query_bridge import run_query
APP_DIR=os.path.dirname(os.path.abspath(__file__))
BASE=os.path.join(APP_DIR,"data_runtime")
QUESTIONS=[
    "quels actifs ont été les plus corrélés dans les 7 derniers mois",
    "quelle est la corrélation entre les us bonds et le pétrole dans les 7 derniers mois",
    "quelle est la corrélation entre le spy pré-market et le spx dans les 7 derniers mois",
]
out=[]
for q in QUESTIONS:
    r=run_query(BASE,q,preview_rows=10)
    if r.get("ok"):
        rr=r["result"]
        out.append({"question":q,"status":rr.get("status"),"answer_type":rr.get("answer_type"),"value":rr.get("value"),"sample_size":rr.get("sample_size"),"preview_len":len(rr.get("preview",[])) if isinstance(rr.get("preview",[]),list) else 0})
    else:
        out.append({"question":q,"status":"ERROR","error":r.get("error")})
print("=== ETAPE106_LOCALHOST_SMOKE ===")
print(json.dumps(out,ensure_ascii=False,indent=2))
