import os,json,re,unicodedata,pandas as pd,numpy as np

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _slug(s):
    return re.sub(r"[^a-z0-9]+","_",_nrm(s)).strip("_")

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

class TargetedEngineDiagnostic:
    #5/24 init
    def __init__(self, project_root, time_registry_path):
        self.project_root=project_root
        self.raw_root=os.path.join(project_root,"RAW_SOURCES")
        self.time_registry_path=time_registry_path
        self.time_registry=self._load_time_registry()
        self.dataset_map=self._build_dataset_map()

    #6/24 registry
    def _load_time_registry(self):
        if not os.path.exists(self.time_registry_path):
            return {"path_index":{}, "dataset_index":{}}
        try:
            with open(self.time_registry_path,"r",encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"path_index":{}, "dataset_index":{}}

    #7/24 datasets
    def _build_dataset_map(self):
        m={
            "spx_daily":os.path.join(self.raw_root,"SPX","SPX_daily.csv"),
            "vix_daily":os.path.join(self.raw_root,"VIX","VIX_daily.csv"),
            "dxy_daily":os.path.join(self.raw_root,"Dollar DXY","DXY_daily.csv"),
            "calendar_daily":os.path.join(self.raw_root,"Calendar","calendar_events_daily.csv"),
        }
        aau_dir=os.path.join(self.raw_root,"Autres Actions Upload")
        if os.path.isdir(aau_dir):
            for f in os.listdir(aau_dir):
                if f.lower().endswith(".csv"):
                    m[_slug(os.path.splitext(f)[0])]=os.path.join(aau_dir,f)
        return {k:v for k,v in m.items() if os.path.exists(v)}

    #8/24 generic readers
    def _read_csv_flex(self,path):
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

    def _detect_time_col(self,df):
        cols=list(df.columns)
        for c in ["time","datetime","date","timestamp"]:
            if c in cols:
                return c
        for c in cols:
            lc=str(c).lower()
            if "time" in lc or "date" in lc:
                return c
        return None

    def _first_match(self,cols,candidates):
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

    def _parse_time_series(self,s,path):
        s=s.astype(str).str.strip()
        reg=self.time_registry.get("path_index",{}).get(path,{})
        fmt=reg.get("time_format")
        if fmt:
            try:
                dt=pd.to_datetime(s,errors="coerce",format=fmt)
                if int(dt.notna().sum())>0:
                    return dt
            except Exception:
                pass
        formats=[
            "%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%Y-%m-%d",
            "%m/%d/%Y %H:%M:%S","%m/%d/%Y %H:%M","%m/%d/%Y",
            "%d/%m/%Y %H:%M:%S","%d/%m/%Y %H:%M","%d/%m/%Y",
            "%Y/%m/%d %H:%M:%S","%Y/%m/%d %H:%M","%Y/%m/%d"
        ]
        best=None; best_count=-1
        for fmt2 in formats:
            try:
                dt=pd.to_datetime(s,errors="coerce",format=fmt2)
                c=int(dt.notna().sum())
                if c>best_count:
                    best=dt; best_count=c
                if c==len(s):
                    return dt
            except Exception:
                pass
        try:
            dt=pd.to_datetime(s,errors="coerce")
            c=int(dt.notna().sum())
            if c>best_count:
                best=dt
        except Exception:
            pass
        if best is None:
            return pd.to_datetime(pd.Series([None]*len(s)),errors="coerce")
        return best

    #9/24 domain readers
    def read_daily_ohlc(self,path):
        df=self._read_csv_flex(path)
        tcol=self._detect_time_col(df)
        if tcol is None:
            raise RuntimeError(f"TIME_NOT_FOUND::{path}")
        df[tcol]=self._parse_time_series(df[tcol],path)
        before_rows=int(len(df))
        valid_rows=int(df[tcol].notna().sum())
        df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
        if df.empty:
            raise RuntimeError(f"NO_VALID_TIME_ROWS::{path}")
        df["date_key"]=df[tcol].dt.strftime("%Y-%m-%d")

        reg=self.time_registry.get("path_index",{}).get(path,{})
        o=reg.get("open_col") if reg.get("open_col") in df.columns else self._first_match(df.columns,["open"])
        h=reg.get("high_col") if reg.get("high_col") in df.columns else self._first_match(df.columns,["high"])
        l=reg.get("low_col") if reg.get("low_col") in df.columns else self._first_match(df.columns,["low"])
        c=reg.get("close_col") if reg.get("close_col") in df.columns else self._first_match(df.columns,["close"])
        if not all([o,h,l,c]):
            raise RuntimeError(f"OHLC_NOT_FOUND::{path}")

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
        out["ret_1w_exante"]=(out["close"].shift(1)/out["close"].shift(6))-1.0
        out["ret_2w_exante"]=(out["close"].shift(1)/out["close"].shift(11))-1.0
        out["ret_4w_exante"]=(out["close"].shift(1)/out["close"].shift(21))-1.0
        out["vol_5d_exante"]=out["ret_oc"].rolling(5).std().shift(1)
        out["open_up_vs_prev_close"]=(out["gap_open_vs_prev_close"]>0).astype(float)
        out["open_down_vs_prev_close"]=(out["gap_open_vs_prev_close"]<0).astype(float)
        meta={
            "raw_rows_before_time_filter":before_rows,
            "valid_time_rows":valid_rows,
            "daily_rows":int(len(out)),
            "time_col":tcol,
            "open_col":o,"high_col":h,"low_col":l,"close_col":c,
        }
        return out,meta

    def read_calendar(self,path):
        df=self._read_csv_flex(path)
        tcol=self._detect_time_col(df)
        if tcol is None:
            raise RuntimeError(f"TIME_NOT_FOUND::{path}")
        df[tcol]=self._parse_time_series(df[tcol],path)
        before_rows=int(len(df))
        valid_rows=int(df[tcol].notna().sum())
        df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
        if df.empty:
            raise RuntimeError(f"NO_VALID_TIME_ROWS::{path}")
        df["date_key"]=df[tcol].dt.strftime("%Y-%m-%d")
        macro_col=self._first_match(df.columns,["macro_event","macro"])
        quiet_col=self._first_match(df.columns,["low_activity_period","low activity period"])
        impact_col=self._first_match(df.columns,["impact"])
        macro_time_col=self._first_match(df.columns,["macro_time","macro time"])
        out=pd.DataFrame({"date_key":df["date_key"]})
        out["is_macro_day"]=False if macro_col is None else df[macro_col].astype(str).str.strip().ne("").fillna(False).infer_objects(copy=False).astype(bool)
        out["is_quiet_day"]=False if quiet_col is None else df[quiet_col].astype(str).str.strip().ne("").fillna(False).infer_objects(copy=False).astype(bool)
        out["is_high_impact_day"]=False if impact_col is None else df[impact_col].astype(str).str.contains("high|fort|élevé",case=False,na=False)
        out["has_macro_time"]=False if macro_time_col is None else df[macro_time_col].astype(str).str.strip().ne("").fillna(False).infer_objects(copy=False).astype(bool)
        out=out.groupby("date_key",as_index=False).agg({
            "is_macro_day":"max","is_quiet_day":"max","is_high_impact_day":"max","has_macro_time":"max"
        })
        meta={
            "raw_rows_before_time_filter":before_rows,
            "valid_time_rows":valid_rows,
            "daily_rows":int(len(out)),
            "time_col":tcol,
            "macro_col":macro_col,
            "quiet_col":quiet_col,
            "impact_col":impact_col,
            "macro_time_col":macro_time_col,
        }
        return out,meta

    #10/24 bucketing diagnostic
    def bucket_diagnostic(self, merged, feature_col, q=3):
        x=pd.to_numeric(merged[feature_col],errors="coerce")
        tmp=merged.copy()
        tmp["_feature"]=x
        tmp=tmp[tmp["_feature"].notna()].copy()
        info={
            "feature_col":feature_col,
            "rows_after_feature_notna":int(len(tmp)),
            "bucket_success":False,
            "bucket_counts":{},
            "rows_with_gap_notna":int(tmp["gap_open_vs_prev_close"].notna().sum()) if "gap_open_vs_prev_close" in tmp.columns else 0,
            "rows_with_ret1_notna":int(tmp["next_close_ret"].notna().sum()) if "next_close_ret" in tmp.columns else 0,
            "rows_with_ret3_notna":int(tmp["plus3d_ret"].notna().sum()) if "plus3d_ret" in tmp.columns else 0,
            "rows_with_ret5_notna":int(tmp["plus5d_ret"].notna().sum()) if "plus5d_ret" in tmp.columns else 0,
        }
        if len(tmp)<15:
            info["bucket_error"]="INSUFFICIENT_ROWS_FOR_BUCKETING"
            return info
        try:
            tmp["_bucket"]=pd.qcut(tmp["_feature"],q=q,labels=["low","mid","high"],duplicates="drop")
            counts=tmp["_bucket"].astype(str).value_counts(dropna=False).to_dict()
            info["bucket_success"]=True
            info["bucket_counts"]={str(k):int(v) for k,v in counts.items()}
        except Exception as e:
            info["bucket_error"]=repr(e)
        return info

    #11/24 cases
    def case_vix_to_spx(self):
        rows=[]; preview=[]
        try:
            spx,spx_meta=self.read_daily_ohlc(self.dataset_map["spx_daily"])
            vix,vix_meta=self.read_daily_ohlc(self.dataset_map["vix_daily"])
            feat=vix[["date_key","ret_1w_exante","ret_4w_exante","vol_5d_exante"]].copy()
            merged=spx.merge(feat,on="date_key",how="left")
            base={
                "case":"vix_to_spx",
                "target_rows":int(len(spx)),
                "feature_rows":int(len(feat)),
                "merged_rows":int(len(merged)),
                "spx_meta":spx_meta,
                "feature_meta":vix_meta,
            }
            for feature_col in ["ret_1w_exante","ret_4w_exante","vol_5d_exante"]:
                info=base.copy()
                info.update(self.bucket_diagnostic(merged,feature_col))
                rows.append(info)
            preview=merged[["date_key","gap_open_vs_prev_close","next_close_ret","plus3d_ret","plus5d_ret","ret_1w_exante","ret_4w_exante","vol_5d_exante"]].head(8).to_dict(orient="records")
        except Exception as e:
            rows.append({"case":"vix_to_spx","error":repr(e)})
        return rows,preview

    def case_dxy_to_spx(self):
        rows=[]; preview=[]
        try:
            spx,spx_meta=self.read_daily_ohlc(self.dataset_map["spx_daily"])
            dxy,dxy_meta=self.read_daily_ohlc(self.dataset_map["dxy_daily"])
            feat=dxy[["date_key","ret_1w_exante","ret_4w_exante"]].copy()
            merged=spx.merge(feat,on="date_key",how="left")
            base={
                "case":"dxy_to_spx",
                "target_rows":int(len(spx)),
                "feature_rows":int(len(feat)),
                "merged_rows":int(len(merged)),
                "spx_meta":spx_meta,
                "feature_meta":dxy_meta,
            }
            for feature_col in ["ret_1w_exante","ret_4w_exante"]:
                info=base.copy()
                info.update(self.bucket_diagnostic(merged,feature_col))
                rows.append(info)
            preview=merged[["date_key","gap_open_vs_prev_close","next_close_ret","plus3d_ret","plus5d_ret","ret_1w_exante","ret_4w_exante"]].head(8).to_dict(orient="records")
        except Exception as e:
            rows.append({"case":"dxy_to_spx","error":repr(e)})
        return rows,preview

    def case_calendar_to_spx(self):
        rows=[]; preview=[]
        try:
            spx,spx_meta=self.read_daily_ohlc(self.dataset_map["spx_daily"])
            cal,cal_meta=self.read_calendar(self.dataset_map["calendar_daily"])
            merged=spx.merge(cal,on="date_key",how="left")
            base={
                "case":"calendar_to_spx",
                "target_rows":int(len(spx)),
                "feature_rows":int(len(cal)),
                "merged_rows":int(len(merged)),
                "spx_meta":spx_meta,
                "feature_meta":cal_meta,
            }
            for feature_col in ["is_macro_day","is_high_impact_day","has_macro_time","is_quiet_day"]:
                info=base.copy()
                s=merged[feature_col].fillna(False).infer_objects(copy=False).astype(bool)
                info["feature_col"]=feature_col
                info["true_rows"]=int((s==True).sum())
                info["false_rows"]=int((s==False).sum())
                info["rows_with_gap_notna"]=int(merged["gap_open_vs_prev_close"].notna().sum())
                info["rows_with_ret1_notna"]=int(merged["next_close_ret"].notna().sum())
                info["rows_with_ret3_notna"]=int(merged["plus3d_ret"].notna().sum())
                info["rows_with_ret5_notna"]=int(merged["plus5d_ret"].notna().sum())
                rows.append(info)
            preview=merged[["date_key","gap_open_vs_prev_close","next_close_ret","plus3d_ret","plus5d_ret","is_macro_day","is_high_impact_day","has_macro_time","is_quiet_day"]].head(8).to_dict(orient="records")
        except Exception as e:
            rows.append({"case":"calendar_to_spx","error":repr(e)})
        return rows,preview

    def case_fundamental_to_aau(self):
        rows=[]; preview={}
        auto_keys=[k for k in self.dataset_map if k.startswith("auto_")]
        if not auto_keys:
            return [{"case":"fundamental_to_aau","error":"NO_AAU_DATASET_FOUND"}],preview
        target_key=sorted(auto_keys)[0]
        try:
            tgt,tgt_meta=self.read_daily_ohlc(self.dataset_map[target_key])
            rows_base={
                "case":"fundamental_to_aau",
                "target_key":target_key,
                "target_rows":int(len(tgt)),
                "target_meta":tgt_meta,
            }
            # VIX
            if "vix_daily" in self.dataset_map:
                vix,vix_meta=self.read_daily_ohlc(self.dataset_map["vix_daily"])
                merged=tgt.merge(vix[["date_key","ret_1w_exante","ret_4w_exante","vol_5d_exante"]],on="date_key",how="left")
                for feature_col in ["ret_1w_exante","ret_4w_exante","vol_5d_exante"]:
                    info=rows_base.copy()
                    info["feature_dataset"]="vix_daily"
                    info["feature_meta"]=vix_meta
                    info["merged_rows"]=int(len(merged))
                    info.update(self.bucket_diagnostic(merged,feature_col))
                    rows.append(info)
                preview["vix_daily"]=merged[["date_key","gap_open_vs_prev_close","next_close_ret","plus3d_ret","plus5d_ret","ret_1w_exante","ret_4w_exante","vol_5d_exante"]].head(8).to_dict(orient="records")
            # DXY
            if "dxy_daily" in self.dataset_map:
                dxy,dxy_meta=self.read_daily_ohlc(self.dataset_map["dxy_daily"])
                merged=tgt.merge(dxy[["date_key","ret_1w_exante","ret_4w_exante"]],on="date_key",how="left")
                for feature_col in ["ret_1w_exante","ret_4w_exante"]:
                    info=rows_base.copy()
                    info["feature_dataset"]="dxy_daily"
                    info["feature_meta"]=dxy_meta
                    info["merged_rows"]=int(len(merged))
                    info.update(self.bucket_diagnostic(merged,feature_col))
                    rows.append(info)
                preview["dxy_daily"]=merged[["date_key","gap_open_vs_prev_close","next_close_ret","plus3d_ret","plus5d_ret","ret_1w_exante","ret_4w_exante"]].head(8).to_dict(orient="records")
            # Calendar
            if "calendar_daily" in self.dataset_map:
                cal,cal_meta=self.read_calendar(self.dataset_map["calendar_daily"])
                merged=tgt.merge(cal,on="date_key",how="left")
                for feature_col in ["is_macro_day","is_high_impact_day","has_macro_time","is_quiet_day"]:
                    info=rows_base.copy()
                    info["feature_dataset"]="calendar_daily"
                    info["feature_meta"]=cal_meta
                    info["merged_rows"]=int(len(merged))
                    s=merged[feature_col].fillna(False).infer_objects(copy=False).astype(bool)
                    info["feature_col"]=feature_col
                    info["true_rows"]=int((s==True).sum())
                    info["false_rows"]=int((s==False).sum())
                    info["rows_with_gap_notna"]=int(merged["gap_open_vs_prev_close"].notna().sum())
                    info["rows_with_ret1_notna"]=int(merged["next_close_ret"].notna().sum())
                    info["rows_with_ret3_notna"]=int(merged["plus3d_ret"].notna().sum())
                    info["rows_with_ret5_notna"]=int(merged["plus5d_ret"].notna().sum())
                    rows.append(info)
                preview["calendar_daily"]=merged[["date_key","gap_open_vs_prev_close","next_close_ret","plus3d_ret","plus5d_ret","is_macro_day","is_high_impact_day","has_macro_time","is_quiet_day"]].head(8).to_dict(orient="records")
        except Exception as e:
            rows.append({"case":"fundamental_to_aau","target_key":target_key,"error":repr(e)})
        return rows,preview
