import os,json
from runtime_query_bridge import run_query

APP_DIR=os.path.dirname(os.path.abspath(__file__))
BASE=os.path.join(APP_DIR,"data_runtime")
QUESTIONS=[
    "en générale, lors des 30 premières min d'ouverture du SPX, quelle est la performance absolue en pct chacun des mois de l'année",
    "lorsque le pré-market du SPY est contenu dans le range de la veille, à combien ouvre le SPX en moyenne",
    "donne moi la moyenne du gold entre 15h et 16h les jours où le SPX ouvre à 0,22% ou plus",
    "si l'oil est au dessus de 60$ le matin à 9h alors quelle est la performance moyenne du IWM le lendemain",
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
            "condition_expr":rr.get("condition_expr"),
            "preview_len":len(rr.get("preview",[])),
            "table_head":rr.get("preview",[])[:5] if isinstance(rr.get("preview",[]),list) else [],
        })
    else:
        out.append({"question":q,"status":"ERROR","error":r.get("error")})
print("=== ETAPE103_LOCALHOST_SMOKE ===")
print(json.dumps(out,ensure_ascii=False,indent=2))
