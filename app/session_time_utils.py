import re,unicodedata
import pandas as pd
import numpy as np

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _slug(s): return re.sub(r"[^a-z0-9]+","_",_nrm(s)).strip("_")

def detect_time_col(df):
    for cand in ["time","date","datetime","timestamp"]:
        for c in df.columns:
            if _slug(c)==cand:
                return c
    for c in df.columns:
        if any(x in _slug(c) for x in ["time","date","datetime","timestamp"]):
            return c
    if len(df.columns):
        c0=df.columns[0]
        dt=pd.to_datetime(df[c0],errors="coerce")
        if dt.notna().sum()>=max(5,min(len(df),20)//2):
            return c0
    return None

def first_match(cols,keys):
    sc={_slug(c):c for c in cols}
    for k in keys:
        if k in sc:
            return sc[k]
    for c in cols:
        if any(k in _slug(c) for k in keys):
            return c
    return None

def build_session_bounds(df):
    g=df.groupby("date_key",as_index=False).agg(
        session_open_dt=("__time__","min"),
        session_close_dt=("__time__","max"),
        session_open_hhmm=("time_hhmm","min"),
        session_close_hhmm=("time_hhmm","max"),
        row_count=("__time__","size"),
    ).sort_values("date_key").reset_index(drop=True)
    g["prev_session_close_dt"]=g["session_close_dt"].shift(1)
    g["prev_date_key"]=g["date_key"].shift(1)
    return g

def add_calendar_buckets(df):
    out=df.copy()
    out["month_num"]=out["__time__"].dt.month
    out["month_name"]=out["__time__"].dt.month_name()
    out["weekday_num"]=out["__time__"].dt.weekday
    out["weekday_name"]=out["__time__"].dt.day_name()
    return out
