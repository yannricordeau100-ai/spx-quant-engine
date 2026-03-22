import os,json
from runtime_query_bridge import run_query

APP_DIR=os.path.dirname(os.path.abspath(__file__))
BASE=os.path.join(APP_DIR,"data_runtime")
QUESTIONS=[
    "quelle est la valeur moyenne de l'or les jours avec événement macro",
    "when dxy is above 100 what is the average us 10y value",
    "montre les dates quand le ratio put call spx est au dessus de 1",
    "donne moi la moyenne du gold entre 15h et 16h les jours où le spx ouvre à 0,22% ou plus",
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
            "translated_query":rr.get("translated_query"),
            "unsupported_fragments":rr.get("unsupported_fragments"),
            "unresolved_parts":rr.get("unresolved_parts"),
            "row_count_filtered":rr.get("row_count_filtered"),
            "preview_len":len(rr.get("preview",[])),
        })
    else:
        out.append({"question":q,"status":"ERROR","error":r.get("error")})
print("=== ETAPE99_LOCALHOST_SMOKE ===")
print(json.dumps(out,ensure_ascii=False,indent=2))
