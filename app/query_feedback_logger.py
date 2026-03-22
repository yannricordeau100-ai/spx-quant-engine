import os,csv,json,uuid
from datetime import datetime,timezone
import pandas as pd

DEFAULT_COLUMNS=[
    "log_id",
    "logged_at_utc",
    "question",
    "status",
    "answer_type",
    "metric",
    "value",
    "translated_query",
    "translator_confidence",
    "assets_detected_json",
    "conditions_json",
    "unsupported_fragments_json",
    "unresolved_parts_json",
    "row_count_total",
    "row_count_filtered",
    "preview_len",
    "message",
    "user_feedback_status",
    "user_feedback_note",
]

def _ensure_file(path):
    os.makedirs(os.path.dirname(path),exist_ok=True)
    if not os.path.exists(path):
        pd.DataFrame(columns=DEFAULT_COLUMNS).to_csv(path,index=False)

def _to_json(x):
    try:
        return json.dumps(x if x is not None else [],ensure_ascii=False)
    except Exception:
        return "[]"

def append_feedback_log(path,question,result):
    _ensure_file(path)
    result=result or {}
    row={
        "log_id":str(uuid.uuid4()),
        "logged_at_utc":datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "question":question,
        "status":result.get("status"),
        "answer_type":result.get("answer_type"),
        "metric":result.get("metric"),
        "value":result.get("value"),
        "translated_query":result.get("translated_query"),
        "translator_confidence":result.get("translator_confidence"),
        "assets_detected_json":_to_json(result.get("assets_detected",[])),
        "conditions_json":_to_json(result.get("conditions",[])),
        "unsupported_fragments_json":_to_json(result.get("unsupported_fragments",[])),
        "unresolved_parts_json":_to_json(result.get("unresolved_parts",[])),
        "row_count_total":result.get("row_count_total"),
        "row_count_filtered":result.get("row_count_filtered"),
        "preview_len":len(result.get("preview",[]) or []),
        "message":result.get("message"),
        "user_feedback_status":"",
        "user_feedback_note":"",
    }
    df=pd.read_csv(path) if os.path.exists(path) and os.path.getsize(path)>0 else pd.DataFrame(columns=DEFAULT_COLUMNS)
    for c in DEFAULT_COLUMNS:
        if c not in df.columns:
            df[c]=""
    df=pd.concat([df,pd.DataFrame([row])],ignore_index=True)
    df=df[DEFAULT_COLUMNS]
    df.to_csv(path,index=False)
    return row

def load_feedback_log(path):
    _ensure_file(path)
    df=pd.read_csv(path)
    for c in DEFAULT_COLUMNS:
        if c not in df.columns:
            df[c]=""
    return df[DEFAULT_COLUMNS]

def export_feedback_log(src_path,dst_path):
    _ensure_file(src_path)
    df=load_feedback_log(src_path)
    os.makedirs(os.path.dirname(dst_path),exist_ok=True)
    df.to_csv(dst_path,index=False)
    return {"rows":int(len(df)),"dst_path":dst_path}
