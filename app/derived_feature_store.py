import os, json, csv, hashlib, pandas as pd

#1/30 helpers
def _sha(s):
    return hashlib.sha256(str(s).encode("utf-8")).hexdigest()

def _normalize_cols(cols):
    out=[]; seen={}
    for c in cols:
        c=str(c).strip()
        base="".join(ch.lower() if ch.isalnum() else "_" for ch in c).strip("_")
        if not base:
            base="col"
        k=base; i=2
        while k in seen:
            k=f"{base}_{i}"; i+=1
        seen[k]=1; out.append(k)
    return out

def _read_csv_flex(path):
    last=None
    for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
        for sep in (None,",",";","\t","|"):
            try:
                if sep is None:
                    df=pd.read_csv(path,sep=None,engine="python",encoding=enc,on_bad_lines="skip")
                else:
                    df=pd.read_csv(path,sep=sep,engine="python",encoding=enc,on_bad_lines="skip")
                if df is not None and df.shape[1]>=1:
                    df.columns=_normalize_cols(df.columns)
                    return df
            except Exception as e:
                last=e
    raise last

def _first_match(cols,candidates):
    s={str(c).lower():c for c in cols}
    for cand in candidates:
        if cand.lower() in s:
            return s[cand.lower()]
    for c in cols:
        lc=str(c).lower()
        for cand in candidates:
            if cand.lower() in lc:
                return c
    return None

def _parse_time_series(s,fmt=None):
    s=s.astype(str).str.strip()
    if fmt:
        try:
            dt=pd.to_datetime(s,errors="coerce",format=fmt)
            if int(dt.notna().sum())>0:
                return dt
        except Exception:
            pass
    try:
        return pd.to_datetime(s,errors="coerce")
    except Exception:
        return pd.to_datetime(pd.Series([None]*len(s)),errors="coerce")

#2/30 registry io
def load_registry(registry_json_path):
    if not os.path.exists(registry_json_path):
        return {"entries":[]}
    try:
        with open(registry_json_path,"r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"entries":[]}

def save_registry(registry_json_path, registry_csv_path, registry):
    with open(registry_json_path,"w",encoding="utf-8") as f:
        json.dump(registry,f,ensure_ascii=False,indent=2)
    rows=registry.get("entries",[])
    if rows:
        keys=sorted(set().union(*[set(r.keys()) for r in rows]))
        with open(registry_csv_path,"w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f,fieldnames=keys)
            w.writeheader()
            for r in rows:
                w.writerow(r)
    else:
        with open(registry_csv_path,"w",newline="",encoding="utf-8") as f:
            f.write("feature_key,feature_name,status\n")

#3/30 feature key
def build_feature_key(feature_name, params):
    return _sha(json.dumps({"feature_name":feature_name,"params":params},ensure_ascii=False,sort_keys=True))[:24]

#4/30 daily reader
def read_daily_close(path,time_format=None):
    df=_read_csv_flex(path)
    tcol=_first_match(df.columns,["time","datetime","date","timestamp"])
    ccol=_first_match(df.columns,["close"])
    if not tcol or not ccol:
        raise RuntimeError(f"DAILY_CLOSE_NOT_FOUND::{path}")
    df[tcol]=_parse_time_series(df[tcol],time_format)
    df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
    if df.empty:
        raise RuntimeError(f"NO_VALID_TIME_ROWS::{path}")
    out=df[[tcol,ccol]].copy()
    out.columns=["time","close"]
    out["close"]=pd.to_numeric(out["close"],errors="coerce")
    out=out[out["close"].notna()].copy()
    out["date_key"]=out["time"].dt.strftime("%Y-%m-%d")
    out=out.groupby("date_key",as_index=False).agg({"close":"last"})
    return out.sort_values("date_key").reset_index(drop=True)

#5/30 store result
def persist_feature_dataframe(root_dir, registry_json_path, registry_csv_path, feature_name, params, df, tail_rows=50):
    os.makedirs(root_dir,exist_ok=True)
    registry=load_registry(registry_json_path)
    key=build_feature_key(feature_name,params)
    csv_path=os.path.join(root_dir,f"{feature_name}__{key}.csv")
    keep=df.tail(int(tail_rows)).copy() if int(tail_rows)>0 else df.copy()
    keep.to_csv(csv_path,index=False)

    entry={
        "feature_key":key,
        "feature_name":feature_name,
        "status":"ready",
        "rows_total":int(len(df)),
        "rows_saved":int(len(keep)),
        "csv_path":csv_path,
        "updated_utc":pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "params_json":json.dumps(params,ensure_ascii=False,sort_keys=True),
    }

    entries=[x for x in registry.get("entries",[]) if x.get("feature_key")!=key]
    entries.append(entry)
    registry["entries"]=entries
    save_registry(registry_json_path,registry_csv_path,registry)
    return entry

#6/30 cache lookup
def lookup_cached_feature(registry_json_path, feature_name, params):
    registry=load_registry(registry_json_path)
    key=build_feature_key(feature_name,params)
    for e in registry.get("entries",[]):
        if e.get("feature_key")==key and e.get("feature_name")==feature_name and os.path.exists(e.get("csv_path","")):
            return e
    return None
