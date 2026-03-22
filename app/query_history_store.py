import os, json, csv, pandas as pd

def append_history(history_jsonl_path, history_csv_path, category, question, result):
    os.makedirs(os.path.dirname(history_jsonl_path),exist_ok=True)
    now=pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    row={
        "ts_utc":now,
        "category":category,
        "question":question,
        "engine":result.get("engine"),
        "status":result.get("status"),
        "value":result.get("value"),
        "summary":result.get("summary"),
        "target_dataset":result.get("target_dataset"),
    }
    with open(history_jsonl_path,"a",encoding="utf-8") as f:
        f.write(json.dumps(row,ensure_ascii=False)+"\n")

    rows=[]
    if os.path.exists(history_csv_path):
        try:
            rows=pd.read_csv(history_csv_path).to_dict(orient="records")
        except Exception:
            rows=[]
    rows.append(row)
    rows=rows[-300:]
    keys=["ts_utc","category","question","engine","status","value","summary","target_dataset"]
    with open(history_csv_path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow(r)
