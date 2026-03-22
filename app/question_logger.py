import os,json,hashlib
from datetime import datetime,timezone
import pandas as pd

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def file_sha256(path):
    if not os.path.exists(path):
        return None
    raw=open(path,"rb").read()
    return hashlib.sha256(raw).hexdigest()

def append_jsonl(path,obj):
    with open(path,"a",encoding="utf-8") as f:
        f.write(json.dumps(obj,ensure_ascii=False)+"\n")

def read_jsonl(path):
    rows=[]
    if not os.path.exists(path):
        return rows
    with open(path,"r",encoding="utf-8",errors="replace") as f:
        for line in f:
            line=line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except:
                pass
    return rows

def dedupe_rows(rows):
    seen=set(); out=[]
    for r in rows:
        q=str(r.get("question","")).strip()
        key=(q,str(r.get("status","")),str(r.get("error","")))
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def save_csv(rows,path):
    import pandas as pd
    pd.DataFrame(rows).to_csv(path,index=False)

def log_unanswered(question,result,error,engine,jsonl_path,csv_path,bridge_hash):
    status=None
    if isinstance(result,dict):
        status=result.get("status")
    row={
        "ts_utc":utc_now(),
        "question":question,
        "engine":engine,
        "status":status,
        "error":error,
        "bridge_hash":bridge_hash,
        "needs_replay":True,
    }
    append_jsonl(jsonl_path,row)
    rows=dedupe_rows(read_jsonl(jsonl_path))
    save_csv(rows,csv_path)
    return row

def read_state(path):
    if not os.path.exists(path):
        return {}
    try:
        return json.load(open(path,"r",encoding="utf-8"))
    except:
        return {}

def write_state(path,obj):
    with open(path,"w",encoding="utf-8") as f:
        json.dump(obj,f,ensure_ascii=False,indent=2)

def replay_questions(questions,bridge_module,app_dir,preview_rows=5):
    rows=[]
    for q in questions:
        r=bridge_module.run_query(app_dir,q,preview_rows=preview_rows)
        rows.append({
            "question":q,
            "ok":r.get("ok"),
            "engine":None if not r.get("ok") else r["result"].get("engine"),
            "status":None if not r.get("ok") else r["result"].get("status"),
            "type":None if not r.get("ok") else r["result"].get("answer_type"),
            "value":None if not r.get("ok") else r["result"].get("value"),
            "error":r.get("error"),
        })
    return rows
