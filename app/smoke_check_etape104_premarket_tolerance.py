import os,json
from runtime_query_bridge import run_query
APP_DIR=os.path.dirname(os.path.abspath(__file__))
BASE=os.path.join(APP_DIR,"data_runtime")
QUESTIONS=[
    "en générale, lors des 30 premières min d'ouverture du SPX, quelle est la performance absolue en pct chacun des mois de l'année",
    "lorsque le pré-market du SPY est contenu dans le range de la veille, à combien ouvre le SPX en moyenne",
]
out=[]
for q in QUESTIONS:
    r=run_query(BASE,q,preview_rows=12)
    if r.get("ok"):
        rr=r["result"]
        out.append({
            "question":q,
            "status":rr.get("status"),
            "answer_type":rr.get("answer_type"),
            "value":rr.get("value"),
            "sample_size":rr.get("sample_size"),
            "match_mode":rr.get("match_mode"),
            "table_len":len(rr.get("table",[])) if isinstance(rr.get("table",[]),list) else 0,
            "preview_len":len(rr.get("preview",[])),
        })
    else:
        out.append({"question":q,"status":"ERROR","error":r.get("error")})
print("=== ETAPE104_LOCALHOST_SMOKE ===")
print(json.dumps(out,ensure_ascii=False,indent=2))
