import os,json
from runtime_query_bridge import run_query
APP_DIR=os.path.dirname(os.path.abspath(__file__))
BASE=os.path.join(APP_DIR,"data_runtime")
QUESTIONS=[
    "lorsque le pré-market du SPY est contenu dans le range de la veille, à combien ouvre le SPX en moyenne",
    "quelle a été la semaine en 2025 ou le yield curve a été le plus important",
    "quels ont été les jours de la semaine où le yield curve étaient le plus bas en 2020 2021 2024",
    "quand est ce que l'or et le pétrole ont été le plus corrélés dans les 7 derniers mois",
    "en générale, lors des 30 premières min d'ouverture du SPX, quelle est la performance absolue en pct chacun des mois de l'année",
    "quelle est la valeur du dax quand le dxy est au dessus de 100",
    "quelle est la value du range moyen 5 jours spx quand le range moyen 20 jours spx est au dessus de 40",
]
out=[]
for q in QUESTIONS:
    r=run_query(BASE,q,preview_rows=10)
    if r.get("ok"):
        rr=r["result"]
        out.append({
            "question":q,
            "status":rr.get("status"),
            "answer_type":rr.get("answer_type"),
            "value":rr.get("value"),
            "sample_size":rr.get("sample_size"),
            "preview_len":len(rr.get("preview",[])) if isinstance(rr.get("preview",[]),list) else 0,
            "table_len":len(rr.get("table",[])) if isinstance(rr.get("table",[]),list) else 0,
        })
    else:
        out.append({"question":q,"status":"ERROR","error":r.get("error")})
print("=== ETAPE105_LOCALHOST_SMOKE ===")
print(json.dumps(out,ensure_ascii=False,indent=2))
