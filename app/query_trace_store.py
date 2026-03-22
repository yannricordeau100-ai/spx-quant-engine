import os, json, pandas as pd

def append_trace(last_json_path, history_jsonl_path, question, result):
    row={
        "ts_utc":pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "question":question,
        "engine":result.get("engine"),
        "status":result.get("status"),
        "value":result.get("value"),
        "summary":result.get("summary"),
        "source_files":result.get("source_files",[]),
        "source_file_names":result.get("source_file_names",[]),
        "source_paths":result.get("source_paths",[]),
    }
    with open(last_json_path,"w",encoding="utf-8") as f:
        json.dump(row,f,ensure_ascii=False,indent=2)
    with open(history_jsonl_path,"a",encoding="utf-8") as f:
        f.write(json.dumps(row,ensure_ascii=False)+"\n")

def load_last_trace(last_json_path):
    if not os.path.exists(last_json_path):
        return None
    try:
        with open(last_json_path,"r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None
