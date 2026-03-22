import os,json
from runtime_query_bridge import run_query

APP_DIR=os.path.dirname(os.path.abspath(__file__))
BASE=os.path.join(APP_DIR,"data_runtime")
QUESTIONS=[
    "donne moi la moyenne du gold entre 15h et 16h les jours où le SPX ouvre à 0,22% ou plus",
    "lorsque le pré-market du SPY est contenu dans le range de la veille, à combien ouvre le SPX en moyenne",
    "si l'oil est au dessus de 60$ le matin à 9h alors quelle est la performance moyenne du IWM le lendemain",
    "quelle est la valeur des bonds US lorsque le dollar DXY est au dessus de 100",
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
            "temporal_parse":rr.get("temporal_parse"),
            "translated_query":rr.get("translated_query"),
        })
    else:
        out.append({"question":q,"status":"ERROR","error":r.get("error")})
print("=== ETAPE101_LOCALHOST_SMOKE ===")
print(json.dumps(out,ensure_ascii=False,indent=2))
