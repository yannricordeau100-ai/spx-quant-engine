import pandas as pd, numpy as np

POLARS_OK=True
DUCKDB_OK=True
try:
    import polars as pl
except Exception:
    POLARS_OK=False
try:
    import duckdb
except Exception:
    DUCKDB_OK=False

#1/32 helpers
def normalize_cols(cols):
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

def read_csv_flex(path):
    last=None
    for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
        for sep in (None,",",";","\t","|"):
            try:
                if sep is None:
                    df=pd.read_csv(path,sep=None,engine="python",encoding=enc,on_bad_lines="skip")
                else:
                    df=pd.read_csv(path,sep=sep,engine="python",encoding=enc,on_bad_lines="skip")
                if df is not None and df.shape[1]>=1:
                    df.columns=normalize_cols(df.columns)
                    return df
            except Exception as e:
                last=e
    raise last

def first_match(cols,candidates):
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

def parse_time_series(s,fmt=None):
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

def _safe_bool_series(s):
    return s.fillna(False).infer_objects(copy=False).astype(bool)

#2/32 readers
def read_intraday_window_table(path,time_format=None,target_hhmm=None):
    df=read_csv_flex(path)
    tcol=first_match(df.columns,["time","datetime","date","timestamp"])
    o=first_match(df.columns,["open"]); h=first_match(df.columns,["high"]); l=first_match(df.columns,["low"]); c=first_match(df.columns,["close"])
    if not all([tcol,o,h,l,c]):
        raise RuntimeError(f"INTRADAY_OHLC_NOT_FOUND::{path}")
    df[tcol]=parse_time_series(df[tcol],time_format)
    df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
    out=df[[tcol,o,h,l,c]].copy().rename(columns={tcol:"time",o:"open",h:"high",l:"low",c:"close"})
    for x in ["open","high","low","close"]:
        out[x]=pd.to_numeric(out[x],errors="coerce")
    out["date_key"]=out["time"].dt.strftime("%Y-%m-%d")
    out["hhmm"]=out["time"].dt.strftime("%H:%M")
    if target_hhmm:
        out=out[out["hhmm"]==target_hhmm].copy()
    if out.empty:
        return pd.DataFrame(columns=["date_key","window_open","window_high","window_low","window_close"])
    out=out.groupby("date_key",as_index=False).agg({"open":"first","high":"max","low":"min","close":"last"})
    return out.rename(columns={"open":"window_open","high":"window_high","low":"window_low","close":"window_close"})

def read_daily_target_table(path,time_format=None):
    df=read_csv_flex(path)
    tcol=first_match(df.columns,["time","datetime","date","timestamp"])
    o=first_match(df.columns,["open"]); h=first_match(df.columns,["high"]); l=first_match(df.columns,["low"]); c=first_match(df.columns,["close"])
    if not all([tcol,o,h,l,c]):
        raise RuntimeError(f"DAILY_OHLC_NOT_FOUND::{path}")
    df[tcol]=parse_time_series(df[tcol],time_format)
    df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
    df["date_key"]=df[tcol].dt.strftime("%Y-%m-%d")
    out=df.groupby("date_key",as_index=False).agg({o:"first",h:"max",l:"min",c:"last"}).rename(columns={o:"open",h:"high",l:"low",c:"close"})
    for x in ["open","high","low","close"]:
        out[x]=pd.to_numeric(out[x],errors="coerce")
    out=out.sort_values("date_key").reset_index(drop=True)
    out["prev_close"]=out["close"].shift(1)
    out["gap_open_vs_prev_close"]=(out["open"]-out["prev_close"])/out["prev_close"].replace(0,np.nan)
    out["ret_oc"]=(out["close"]-out["open"])/out["open"].replace(0,np.nan)
    out["next_close_ret"]=(out["close"].shift(-1)/out["close"])-1.0
    out["plus3d_ret"]=(out["close"].shift(-3)/out["close"])-1.0
    out["plus5d_ret"]=(out["close"].shift(-5)/out["close"])-1.0
    out["open_up_prob_proxy"]=(out["gap_open_vs_prev_close"]>0).astype(float)
    out["open_down_prob_proxy"]=(out["gap_open_vs_prev_close"]<0).astype(float)
    return out

def read_driver_daily_table(path,prefix,time_format=None):
    out=read_daily_target_table(path,time_format)[["date_key","close"]].copy()
    out[f"{prefix}_ret_1w_exante"]=(out["close"].shift(1)/out["close"].shift(6))-1.0
    out[f"{prefix}_ret_4w_exante"]=(out["close"].shift(1)/out["close"].shift(21))-1.0
    out[f"{prefix}_vol_5d_exante"]=out["close"].pct_change().rolling(5).std().shift(1)
    return out.drop(columns=["close"])

def read_calendar_table(path,time_format=None):
    df=read_csv_flex(path)
    tcol=first_match(df.columns,["time","datetime","date","timestamp"])
    if not tcol:
        return pd.DataFrame(columns=["date_key","is_macro_day","is_high_impact_day","is_quiet_day","has_macro_time"])
    df[tcol]=parse_time_series(df[tcol],time_format)
    df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
    df["date_key"]=df[tcol].dt.strftime("%Y-%m-%d")
    macro_col=first_match(df.columns,["macro_event","macro"])
    quiet_col=first_match(df.columns,["low_activity_period","low activity period"])
    impact_col=first_match(df.columns,["impact"])
    macro_time_col=first_match(df.columns,["macro_time","macro time"])
    out=pd.DataFrame({"date_key":df["date_key"]})
    out["is_macro_day"]=False if macro_col is None else df[macro_col].astype(str).str.strip().ne("").fillna(False).infer_objects(copy=False).astype(bool)
    out["is_quiet_day"]=False if quiet_col is None else df[quiet_col].astype(str).str.strip().ne("").fillna(False).infer_objects(copy=False).astype(bool)
    out["is_high_impact_day"]=False if impact_col is None else df[impact_col].astype(str).str.contains("high|fort|elev|élev",case=False,na=False)
    out["has_macro_time"]=False if macro_time_col is None else df[macro_time_col].astype(str).str.strip().ne("").fillna(False).infer_objects(copy=False).astype(bool)
    return out.groupby("date_key",as_index=False).agg({"is_macro_day":"max","is_quiet_day":"max","is_high_impact_day":"max","has_macro_time":"max"})

#3/32 windows
def hhmm_from_label(label):
    return {"at_open":"09:30","5m_after_open":"09:35","15m_after_open":"09:45","30m_after_open":"10:00","60m_after_open":"10:30"}.get(label)

#4/32 condition columns
def _bucket_series(s):
    x=pd.to_numeric(s,errors="coerce")
    try:
        return pd.qcut(x,q=2,labels=["low","high"],duplicates="drop").astype(str)
    except Exception:
        return pd.Series(["nan"]*len(s),index=s.index)

def _build_condition_columns(merged, driver_specs):
    asset_condition_map={}
    feature_dataset_parts=[]
    for spec in driver_specs:
        kind=spec.get("kind")
        name=spec.get("name")
        asset=spec.get("asset")
        if kind=="calendar":
            col=None
            if "is_macro_day" in merged.columns:
                col=f"{name}_macro"
                merged[col]=_safe_bool_series(merged["is_macro_day"]).map({True:"true",False:"false"})
            elif "is_high_impact_day" in merged.columns:
                col=f"{name}_impact"
                merged[col]=_safe_bool_series(merged["is_high_impact_day"]).map({True:"true",False:"false"})
            if col:
                asset_condition_map[asset]=col
                feature_dataset_parts.append(name)
        else:
            prefix=name.lower()
            chosen=None
            for src in [f"{prefix}_ret_1w_exante",f"{prefix}_ret_4w_exante",f"{prefix}_vol_5d_exante"]:
                if src in merged.columns:
                    c=f"{src}_bucket"
                    merged[c]=_bucket_series(merged[src])
                    chosen=c
                    break
            if chosen:
                asset_condition_map[asset]=chosen
                feature_dataset_parts.append(name)
    return merged, asset_condition_map, feature_dataset_parts

#5/32 tree evaluation
def _asset_mask(base, asset, asset_condition_map, chosen_values):
    col=asset_condition_map.get(asset)
    if not col or col not in base.columns:
        return pd.Series([True]*len(base),index=base.index)
    val=chosen_values.get(asset)
    if val is None:
        return pd.Series([True]*len(base),index=base.index)
    return base[col].astype(str)==str(val)

def _eval_tree(base, tree, asset_condition_map, chosen_values):
    if tree is None:
        return pd.Series([True]*len(base),index=base.index)
    t=tree.get("type")
    if t=="ASSET":
        return _asset_mask(base,tree.get("value"),asset_condition_map,chosen_values)
    if t=="NOT":
        return ~_eval_tree(base,tree.get("child"),asset_condition_map,chosen_values)
    if t=="AND":
        return _eval_tree(base,tree.get("left"),asset_condition_map,chosen_values) & _eval_tree(base,tree.get("right"),asset_condition_map,chosen_values)
    if t=="OR":
        return _eval_tree(base,tree.get("left"),asset_condition_map,chosen_values) | _eval_tree(base,tree.get("right"),asset_condition_map,chosen_values)
    return pd.Series([True]*len(base),index=base.index)

#6/32 enumerate assignments
def _top_values(base, col, n=2):
    vals=[x for x in base[col].astype(str).value_counts().index.tolist() if x!="nan"]
    return vals[:n]

def _enumerate_assignments(base, asset_condition_map, max_per_asset=2):
    assets=list(asset_condition_map.keys())
    options={}
    for a,col in asset_condition_map.items():
        options[a]=_top_values(base,col,max_per_asset)
    assignments=[{}]
    for a in assets:
        new=[]
        vals=options.get(a,[])
        if not vals:
            vals=[None]
        for prev in assignments:
            for v in vals:
                x=dict(prev); x[a]=v; new.append(x)
        assignments=new
    return assignments[:64]

def _assignment_label(assignment, asset_condition_map):
    parts=[]
    for a,v in assignment.items():
        col=asset_condition_map.get(a)
        if col:
            parts.append(f"{a}:{col}={v}")
    return " | ".join(parts)

#7/32 result row
def _agg_row(sub, target_dataset, semantics, feature_dataset, label, logic_form):
    return {
        "slice":"precedence_logic_engine",
        "case":"logical_tree_evaluation",
        "feature_dataset":feature_dataset,
        "target_dataset":target_dataset,
        "feature":"logic_tree",
        "bucket":label,
        "logic_form":logic_form,
        "sample_size":int(len(sub)),
        "gap_mean":None if sub["gap_open_vs_prev_close"].dropna().empty else float(sub["gap_open_vs_prev_close"].mean()),
        "intraday_mean":None if sub["ret_oc"].dropna().empty else float(sub["ret_oc"].mean()),
        "next_close_mean":None if sub["next_close_ret"].dropna().empty else float(sub["next_close_ret"].mean()),
        "plus3d_mean":None if sub["plus3d_ret"].dropna().empty else float(sub["plus3d_ret"].mean()),
        "plus5d_mean":None if sub["plus5d_ret"].dropna().empty else float(sub["plus5d_ret"].mean()),
        "open_up_prob":None if sub["open_up_prob_proxy"].dropna().empty else float(sub["open_up_prob_proxy"].mean()),
        "open_down_prob":None if sub["open_down_prob_proxy"].dropna().empty else float(sub["open_down_prob_proxy"].mean()),
        "timing_mode":semantics.get("timing_mode"),
        "target_window_label":semantics.get("target_window_label"),
        "cross_asset_count":semantics.get("cross_asset_count"),
        "performance_backend":"polars_duckdb_candidate",
    }

#8/32 main
def execute_heavy_scan(context):
    semantics=context["semantics"]
    target_daily_path=context["target_daily_path"]
    target_intraday_path=context.get("target_intraday_path")
    target_dataset=context["target_dataset"]
    time_registry=context["time_registry"]
    driver_specs=context.get("driver_specs",[])
    logic_tree=semantics.get("logic_tree")
    logic_forms=semantics.get("logic_forms",["IMPLICIT_AND"])

    reg=time_registry.get("path_index",{})
    tgt_fmt=reg.get(target_daily_path,{}).get("time_format")
    tgt=read_daily_target_table(target_daily_path,tgt_fmt)

    hhmm=hhmm_from_label(semantics.get("target_window_label"))
    if target_intraday_path and hhmm:
        intrafmt=reg.get(target_intraday_path,{}).get("time_format")
        snap=read_intraday_window_table(target_intraday_path,intrafmt,hhmm)
        if not snap.empty:
            tgt=tgt.merge(snap,on="date_key",how="left")
            tgt["gap_open_vs_prev_close"]=np.where(
                tgt["window_open"].notna(),
                (tgt["window_open"]-tgt["prev_close"])/tgt["prev_close"].replace(0,np.nan),
                tgt["gap_open_vs_prev_close"]
            )
            tgt["ret_oc"]=np.where(
                tgt["window_open"].notna() & tgt["window_close"].notna(),
                (tgt["window_close"]-tgt["window_open"])/tgt["window_open"].replace(0,np.nan),
                tgt["ret_oc"]
            )

    merged=tgt.copy()
    for spec in driver_specs:
        kind=spec.get("kind"); name=spec.get("name"); path=spec.get("path")
        if not path:
            continue
        if kind=="calendar":
            cal_fmt=reg.get(path,{}).get("time_format")
            cal=read_calendar_table(path,cal_fmt)
            merged=merged.merge(cal,on="date_key",how="left")
        else:
            prefix=name.lower()
            drv_fmt=reg.get(path,{}).get("time_format")
            drv=read_driver_daily_table(path,prefix,drv_fmt)
            merged=merged.merge(drv,on="date_key",how="left")

    merged, asset_condition_map, feature_dataset_parts=_build_condition_columns(merged,driver_specs)
    if len(asset_condition_map)==0:
        return []

    base=merged.copy()
    for c in asset_condition_map.values():
        base=base[base[c].astype(str).ne("nan")].copy()
    if base.empty:
        return []

    assignments=_enumerate_assignments(base,asset_condition_map,max_per_asset=2)
    out=[]
    for assignment in assignments:
        mask=_eval_tree(base,logic_tree,asset_condition_map,assignment)
        sub=base[mask].copy()
        if len(sub)<12:
            continue
        label=_assignment_label(assignment,asset_condition_map)
        out.append(_agg_row(
            sub=sub,
            target_dataset=target_dataset,
            semantics=semantics,
            feature_dataset="+".join(feature_dataset_parts),
            label=label,
            logic_form="|".join(logic_forms)
        ))
    out=sorted(out,key=lambda r:(r.get("sample_size",0),abs(r.get("plus5d_mean") or 0),abs(r.get("next_close_mean") or 0)),reverse=True)
    return out[:40]
