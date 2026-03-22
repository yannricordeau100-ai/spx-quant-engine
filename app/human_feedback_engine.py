import os, re, json, unicodedata
import pandas as pd
from datetime import datetime, timezone

ROOT="/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
PROC=os.path.join(ROOT,"processed")
MASTER_CSV=os.path.join(PROC,"human_feedback_master.csv")
EXPORT_CSV=os.path.join(PROC,"feedback_export.csv")
OK_CSV=os.path.join(PROC,"human_feedback_ok.csv")
FALSE_CSV=os.path.join(PROC,"human_feedback_false.csv")
PASS_CSV=os.path.join(PROC,"human_feedback_pass.csv")
ANALYSIS_READY_JSON=os.path.join(PROC,"feedback_analysis_ready.json")
os.makedirs(PROC,exist_ok=True)

BASE_COLS=["timestamp","label","engine","question","question_norm","answer","app_version"]

def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def normalize_question(q):
    q=_strip_accents(str(q).lower()).strip()
    q=re.sub(r"\s+"," ",q)
    return q

def _empty_df():
    df=pd.DataFrame(columns=BASE_COLS)
    for c in BASE_COLS:
        if c not in df.columns:
            df[c]=""
    return df

def _load_df():
    if os.path.exists(MASTER_CSV):
        try:
            df=pd.read_csv(MASTER_CSV)
        except Exception:
            df=_empty_df()
    else:
        df=_empty_df()
    for c in BASE_COLS:
        if c not in df.columns:
            df[c]=""
    return df[BASE_COLS].copy()

def _write_csv_if_any(df, path):
    if df is None:
        return
    df.to_csv(path, index=False)

def _build_analysis_payload(df):
    if df is None or len(df)==0:
        return {
            "generated_at":datetime.now(timezone.utc).isoformat(),
            "n_total":0,
            "n_ok":0,
            "n_false":0,
            "n_pass":0,
            "latest_false":[],
            "latest_ok":[],
            "latest_pass":[],
            "by_engine":{},
            "by_app_version":{},
        }

    dfx=df.copy()
    dfx["label"]=dfx["label"].astype(str).str.upper().str.strip()
    dfx["engine"]=dfx["engine"].astype(str).str.strip()
    dfx["app_version"]=dfx["app_version"].astype(str).str.strip()

    by_engine={}
    try:
        g=dfx.groupby("engine", dropna=False)["label"].count().sort_values(ascending=False)
        for k,v in g.items():
            by_engine[str(k)] = int(v)
    except Exception:
        pass

    by_app_version={}
    try:
        g=dfx.groupby("app_version", dropna=False)["label"].count().sort_values(ascending=False)
        for k,v in g.items():
            by_app_version[str(k)] = int(v)
    except Exception:
        pass

    def latest_rows(label, n=20):
        sub=dfx[dfx["label"]==label].copy()
        if "timestamp" in sub.columns:
            try:
                sub=sub.sort_values("timestamp", ascending=False)
            except Exception:
                pass
        keep=["timestamp","engine","question","answer","app_version"]
        for c in keep:
            if c not in sub.columns:
                sub[c]=""
        return sub[keep].head(n).to_dict(orient="records")

    return {
        "generated_at":datetime.now(timezone.utc).isoformat(),
        "n_total":int(len(dfx)),
        "n_ok":int((dfx["label"]=="OK").sum()),
        "n_false":int((dfx["label"]=="FAUX").sum()),
        "n_pass":int((dfx["label"]=="PASSE").sum()),
        "latest_false":latest_rows("FAUX", 30),
        "latest_ok":latest_rows("OK", 30),
        "latest_pass":latest_rows("PASSE", 30),
        "by_engine":by_engine,
        "by_app_version":by_app_version,
    }

def _refresh_exports(df):
    _write_csv_if_any(df, MASTER_CSV)
    _write_csv_if_any(df, EXPORT_CSV)

    dfx=df.copy()
    dfx["label"]=dfx["label"].astype(str).str.upper().str.strip()

    _write_csv_if_any(dfx[dfx["label"]=="OK"].copy(), OK_CSV)
    _write_csv_if_any(dfx[dfx["label"]=="FAUX"].copy(), FALSE_CSV)
    _write_csv_if_any(dfx[dfx["label"]=="PASSE"].copy(), PASS_CSV)

    payload=_build_analysis_payload(dfx)
    with open(ANALYSIS_READY_JSON,"w",encoding="utf-8") as f:
        json.dump(payload,f,ensure_ascii=False,indent=2)

def save_feedback(label, question, answer, engine, app_version, extra=None):
    label=str(label or "").strip().upper()
    question=str(question or "").strip()
    answer=str(answer or "").strip()
    engine=str(engine or "").strip()
    app_version=str(app_version or "").strip()
    qn=normalize_question(question)

    df=_load_df()
    mask=(df["question_norm"].astype(str)==qn) & (df["app_version"].astype(str)==app_version)
    df=df.loc[~mask].copy()

    row={
        "timestamp":datetime.now(timezone.utc).isoformat(),
        "label":label,
        "engine":engine,
        "question":question,
        "question_norm":qn,
        "answer":answer,
        "app_version":app_version,
    }
    df=pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    _refresh_exports(df)
    return row

def load_feedback():
    df=_load_df()
    _refresh_exports(df)
    return df.to_dict(orient="records")

def export_feedback_csv():
    df=_load_df()
    _refresh_exports(df)
    return EXPORT_CSV if os.path.exists(EXPORT_CSV) else None

def get_analysis_ready_path():
    df=_load_df()
    _refresh_exports(df)
    return ANALYSIS_READY_JSON
