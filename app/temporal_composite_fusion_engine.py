import os,re,unicodedata,pandas as pd,numpy as np

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _slug(s): return re.sub(r"[^a-z0-9]+","_",_nrm(s)).strip("_")
def _normalize_cols(cols):
    out=[]; seen={}
    for c in cols:
        base=_slug(c) or "col"; k=base; i=2
        while k in seen:
            k=f"{base}_{i}"; i+=1
        seen[k]=1; out.append(k)
    return out

class TemporalCompositeFusionEngine:
    def __init__(self,source_root,source_config,asset_aliases,candle_synonyms,session_utils):
        self.source_root=source_root
        self.source_config=source_config
        self.asset_aliases=asset_aliases
        self.candle_synonyms=candle_synonyms
        self.session_utils=session_utils
        self.cache={}
        self.weekday_map={
            "lundi":0,"monday":0,"mardi":1,"tuesday":1,"mercredi":2,"wednesday":2,
            "jeudi":3,"thursday":3,"vendredi":4,"friday":4,"samedi":5,"saturday":5,"dimanche":6,"sunday":6,
        }

    def can_handle(self,q):
        nq=_nrm(q)
        temporal=any(x in nq for x in ["au bout de","after","minutes","minute","mins","min ","entre ","between ","premarket","pré-market","pre-market","premières","first 30","lendemain","next day"])
        composite=any(x in nq for x in [" uniquement "," only "," dans le cas ou "," dans le cas où "," si "," if "," et "," and "])
        free_seq=any(x in nq for x in ["bougie rouge","bougie verte","red candle","green candle","+","puis","then"])
        return temporal or (composite and free_seq) or ("seulement les" in nq) or ("only on" in nq)

    def _contains_any(self,q,arr): 
        nq=_nrm(q)
        return any(_nrm(x) in nq for x in arr)

    def _read_csv(self,path):
        last=None
        for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
            try:
                with open(path,"r",encoding=enc,errors="replace") as f:
                    lines=[x for x in f.read().splitlines() if str(x).strip()!=""]
                if not lines: continue
                header=lines[0]
                best_sep=None; best_n=1
                for sep in [",",";","\\t","|"]:
                    n=len(header.split(sep))
                    if n>best_n: best_n=n; best_sep=sep
                if best_sep and best_n>1:
                    cols=[x.strip() for x in header.split(best_sep)]
                    rows=[]
                    for line in lines[1:]:
                        parts=[x.strip() for x in line.split(best_sep)]
                        if len(parts)==len(cols): rows.append(parts)
                    if len(rows)>=max(10,int(len(lines)*0.4)):
                        df=pd.DataFrame(rows,columns=cols); df.columns=_normalize_cols(df.columns); return df
                for sep in (",",";","\\t","|",None):
                    try:
                        kw={"encoding":enc,"on_bad_lines":"skip"}
                        if sep is None: df=pd.read_csv(path,sep=None,engine="python",**kw)
                        else: df=pd.read_csv(path,sep=sep,engine="python",**kw)
                        if df is not None and df.shape[1]>=1:
                            df.columns=_normalize_cols(df.columns); return df
                    except Exception as e: last=e
            except Exception as e: last=e
        raise last

    def _resolve_assets(self,q):
        nq=_nrm(q); out=[]
        for k,aliases in self.asset_aliases.items():
            if any(_nrm(a) in nq for a in aliases): out.append(k)
        return out

    def _load(self,key):
        if key in self.cache: return self.cache[key]
        cfg=self.source_config[key]
        df=self._read_csv(cfg["path"])
        tcol=self.session_utils.detect_time_col(df)
        if tcol is None: raise RuntimeError(f"{key}_TIME_NOT_FOUND")
        s=df[tcol].astype(str)
        dt=pd.to_datetime(s,errors="coerce",format="%Y-%m-%d %H:%M:%S")
        if dt.notna().sum()==0: dt=pd.to_datetime(s,errors="coerce",format="%Y-%m-%d")
        if dt.notna().sum()==0: dt=pd.to_datetime(s,errors="coerce")
        df[tcol]=dt
        df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
        df["__time__"]=df[tcol]
        df["date_key"]=df["__time__"].dt.strftime("%Y-%m-%d")
        df["time_hhmm"]=df["__time__"].dt.strftime("%H:%M")
        df["year"]=df["__time__"].dt.year
        df["weekday_num"]=df["__time__"].dt.weekday
        df["weekday_name"]=df["__time__"].dt.day_name()
        self.cache[key]=df
        return df

    def _ohlc_cols(self,df):
        fm=self.session_utils.first_match
        return {"open":fm(df.columns,["open"]),"high":fm(df.columns,["high"]),"low":fm(df.columns,["low"]),"close":fm(df.columns,["close"])}

    def _value_col(self,df):
        fm=self.session_utils.first_match
        for cand in ["spread_10y_minus_2y","value","close","open","high","low","us_2y","spx_iwm_correlation_20d","spx_qqq_correlation_20d"]:
            c=fm(df.columns,[cand])
            if c: return c
        bad={"date_key","year","weekday_num","weekday_name","time_hhmm","__time__","month_num","month_name"}
        for c in df.columns:
            if c in bad: continue
            try:
                if pd.to_numeric(df[c],errors="coerce").notna().sum()>0: return c
            except: pass
        return None

    def _daily(self,key):
        df=self._load(key)
        if self.source_config[key]["kind"]=="ohlc":
            ohlc=self._ohlc_cols(df)
            agg={}
            if ohlc["open"]: agg[ohlc["open"]]="first"
            if ohlc["high"]: agg[ohlc["high"]]="max"
            if ohlc["low"]: agg[ohlc["low"]]="min"
            if ohlc["close"]: agg[ohlc["close"]]="last"
            out=df.groupby(["date_key","year","weekday_num","weekday_name"],as_index=False).agg(agg)
            ren={}
            if ohlc["open"]: ren[ohlc["open"]]="open"
            if ohlc["high"]: ren[ohlc["high"]]="high"
            if ohlc["low"]: ren[ohlc["low"]]="low"
            if ohlc["close"]: ren[ohlc["close"]]="close"
            out=out.rename(columns=ren)
            for c in ["open","high","low","close"]:
                if c in out.columns: out[c]=pd.to_numeric(out[c],errors="coerce")
            if "open" in out.columns and "close" in out.columns:
                out["ret"]=(out["close"]-out["open"])/out["open"].replace(0,np.nan)
                out["green_candle"]=out["close"]>out["open"]
                out["red_candle"]=out["close"]<out["open"]
                out["body_high"]=out[["open","close"]].max(axis=1)
                out["body_low"]=out[["open","close"]].min(axis=1)
                out["prev_open"]=out["open"].shift(1); out["prev_close"]=out["close"].shift(1)
                out["prev_body_high"]=out["body_high"].shift(1); out["prev_body_low"]=out["body_low"].shift(1)
                out["bearish_engulfing"]=(out["red_candle"])&(out["prev_close"]>out["prev_open"])&(out["body_high"]>=out["prev_body_high"])&(out["body_low"]<=out["prev_body_low"])
                out["bullish_engulfing"]=(out["green_candle"])&(out["prev_close"]<out["prev_open"])&(out["body_high"]>=out["prev_body_high"])&(out["body_low"]<=out["prev_body_low"])
            return out
        vcol=self._value_col(df)
        out=df.groupby(["date_key","year","weekday_num","weekday_name"],as_index=False).agg(value=(vcol,"last"))
        out["value"]=pd.to_numeric(out["value"],errors="coerce")
        return out

    def _parse_weekday(self,q):
        nq=_nrm(q)
        for k,v in self.weekday_map.items():
            if k in nq: return v
        return None

    def _parse_minutes_horizon(self,q):
        nq=_nrm(q)
        pats=[r"au bout de\s+(\d+)\s*(?:minute|min|minutes|mins)",r"after\s+(\d+)\s*(?:minute|min|minutes|mins)"]
        for p in pats:
            m=re.search(p,nq)
            if m: return int(m.group(1))
        return None

    def _parse_between(self,q):
        nq=_nrm(q)
        pats=[r"entre\s+(\d{1,2})(?:h|:)?(\d{0,2})?\s+et\s+(\d{1,2})(?:h|:)?(\d{0,2})?",r"between\s+(\d{1,2})(?::(\d{2}))?\s+(?:and|to)\s+(\d{1,2})(?::(\d{2}))?"]
        for p in pats:
            m=re.search(p,nq)
            if m:
                g=m.groups()
                h1=int(g[0]); m1=int(g[1] or 0); h2=int(g[2]); m2=int(g[3] or 0)
                return f"{h1:02d}:{m1:02d}",f"{h2:02d}:{m2:02d}"
        return None,None

    def _target_asset(self,q,assets):
        nq=_nrm(q)
        prefixes=["combien de jours est valable","how many days is","quelle est la moyenne de","average of","moyenne de","probabilite que","probabilité que","probability that","combien de fois","how many times","quelle est la performance de","what is the return of"]
        cuts=[" uniquement "," only "," dans le cas ou "," dans le cas où "," if "," si "," le tout "," uniquement les "," par an "," per year "," au bout de "," after "," entre "," between "]
        for pref in prefixes:
            if pref in nq:
                frag=nq.split(pref,1)[1]
                cut=len(frag)
                for tok in cuts:
                    p=frag.find(tok)
                    if p!=-1: cut=min(cut,p)
                frag=frag[:cut]
                for a,aliases in self.asset_aliases.items():
                    if any(_nrm(x) in frag for x in aliases): return a
        return assets[0] if assets else None

    def _target_metric_type(self,q,target_daily):
        nq=_nrm(q)
        if "probab" in nq or "chance" in nq:
            return "probability"
        if "combien" in nq or "count" in nq:
            return "count"
        if any(x in nq for x in ["performance","return","retour","rendement","variation","perf"]):
            return "ret" if "ret" in target_daily.columns else ("close" if "close" in target_daily.columns else "value")
        if "open" in nq or "ouvre" in nq or "ouverture" in nq:
            return "open" if "open" in target_daily.columns else "value"
        return "close" if "close" in target_daily.columns else ("value" if "value" in target_daily.columns else "count")

    def _cmp_parse(self,frag):
        f=_nrm(frag)
        keyword_map={
            "<=":["<=","inferieur ou egal a","inférieur ou égal à","less than or equal to"],
            ">=":[">=","superieur ou egal a","supérieur ou égal à","greater than or equal to"],
            "<":["<","inferieur a","inférieur à","below","less than"],
            ">":[">","superieur a","supérieur à","above","greater than","au dessus de"],
            "=":["=","egal a","égal à","equal to"]
        }
        for op,kws in keyword_map.items():
            pos=-1; hit=None
            for kw in kws:
                p=f.find(kw)
                if p!=-1 and (pos==-1 or p<pos): pos=p; hit=kw
            if hit is None: continue
            right=f[pos+len(hit):].strip()
            m=re.search(r"(-?\d+(?:[\.,]\d+)?)\s*%?",right)
            if m:
                raw=float(m.group(1).replace(",","."))
                is_pct=("%" in right) or any(x in f for x in ["pct","pourcent","percent","ret","performance","variation","rendement"])
                return op,(raw/100.0 if is_pct else raw),is_pct
        return None,None,None

    def _build_free_sequence(self,frag,df):
        nq=_nrm(frag)
        tokens=[]
        repl=nq.replace(" puis "," + ").replace(" then "," + ")
        parts=[x.strip() for x in repl.split("+") if x.strip()]
        for p in parts:
            if any(x in p for x in ["bougie rouge","red candle"]): tokens.append(("red_candle",1))
            elif any(x in p for x in ["bougie verte","green candle"]): tokens.append(("green_candle",1))
            elif any(x in p for x in ["2 bougies rouges","deux bougies rouges","two red candles"]): tokens.append(("red_candle",2))
            elif any(x in p for x in ["2 bougies vertes","deux bougies vertes","two green candles"]): tokens.append(("green_candle",2))
        if not tokens: return None
        expanded=[]
        for name,n in tokens:
            for _ in range(n): expanded.append(name)
        cond=pd.Series(True,index=df.index)
        L=len(expanded)
        for i,name in enumerate(expanded):
            cond=cond & df[name].shift(L-1-i).fillna(False).astype(bool)
        return cond

    def _condition_fragments(self,q):
        nq=_nrm(q)
        nq=nq.replace(" uniquement dans le cas ou "," && ").replace(" uniquement dans le cas où "," && ").replace(" only if "," && ")
        nq=nq.replace(" et "," && ").replace(" and "," && ").replace(" si "," && ").replace(" if "," && ")
        return [x.strip() for x in nq.split("&&") if x.strip()]

    def _daily_condition_from_fragment(self,frag,assets):
        candle_map={
            "bougie rouge":"red_candle","red candle":"red_candle",
            "bougie verte":"green_candle","green candle":"green_candle",
            "bearish engulfing":"bearish_engulfing","engulfing baissier":"bearish_engulfing",
            "bullish engulfing":"bullish_engulfing","engulfing haussier":"bullish_engulfing",
        }
        for asset in assets:
            if any(_nrm(a) in frag for a in self.asset_aliases.get(asset,[])):
                df=self._daily(asset).copy()
                seq=self._build_free_sequence(frag,df)
                if seq is not None:
                    tmp=df[["date_key"]].copy(); tmp[f"{asset}__cond"]=seq.fillna(False).astype(bool)
                    return asset,tmp,f"{asset}:free_sequence"
                for k,v in candle_map.items():
                    if _nrm(k) in frag and v in df.columns:
                        tmp=df[["date_key",v]].copy().rename(columns={v:f"{asset}__cond"})
                        tmp[f"{asset}__cond"]=tmp[f"{asset}__cond"].fillna(False).astype(bool)
                        return asset,tmp,f"{asset}:{v}"
                if "ret" in df.columns and any(x in frag for x in ["performance","return","retour","rendement","variation","perf"]):
                    metric="ret"
                elif "open" in df.columns and any(x in frag for x in ["open","ouvre","ouverture","opening"]):
                    metric="open"
                elif "high" in df.columns and "high" in frag:
                    metric="high"
                elif "low" in df.columns and "low" in frag:
                    metric="low"
                elif "value" in df.columns:
                    metric="value"
                else:
                    metric="close" if "close" in df.columns else ("value" if "value" in df.columns else None)
                op,val,is_pct=self._cmp_parse(frag)
                if metric is None or op is None: continue
                tmp=df[["date_key",metric]].copy().rename(columns={metric:f"{asset}__metric"})
                s=pd.to_numeric(tmp[f"{asset}__metric"],errors="coerce")
                if op=="<": cond=s<val
                elif op==">": cond=s>val
                elif op=="<=": cond=s<=val
                elif op==">=": cond=s>=val
                else: cond=np.isclose(s,val,equal_nan=False)
                tmp[f"{asset}__cond"]=cond.fillna(False)
                return asset,tmp,f"{asset}:{metric}{op}{val}"
        return None,None,None

    def _intraday_horizon_return(self,target_asset,minutes):
        df=self._load(target_asset).copy()
        ohlc=self._ohlc_cols(df)
        if ohlc["open"] is None or ohlc["close"] is None:
            return None
        df["open_num"]=pd.to_numeric(df[ohlc["open"]],errors="coerce")
        df["close_num"]=pd.to_numeric(df[ohlc["close"]],errors="coerce")
        df=df[df["open_num"].notna() & df["close_num"].notna()].copy()
        if df.empty: return None
        if target_asset in ["spx","spy","qqq"]:
            regular=df[(df["time_hhmm"]>="09:30")&(df["time_hhmm"]<="16:00")].copy()
            open_times=regular.groupby("date_key",as_index=False).agg(session_open=("__time__","min"))
            work=regular.merge(open_times,on="date_key",how="left")
        else:
            work=df.groupby("date_key",as_index=False).apply(lambda x:x).reset_index(drop=True)
            open_times=work.groupby("date_key",as_index=False).agg(session_open=("__time__","min"))
            work=work.merge(open_times,on="date_key",how="left")
        work["mins_from_open"]=(work["__time__"]-work["session_open"]).dt.total_seconds()/60.0
        starts=work[work["mins_from_open"]==0].copy()
        if starts.empty:
            starts=work.groupby("date_key",as_index=False).head(1).copy()
        work["future_time"]=work["__time__"]+pd.to_timedelta(minutes,unit="m")
        fut=work[["date_key","__time__","close_num"]].rename(columns={"__time__":"future_lookup_time","close_num":"future_close"})
        starts["future_lookup_time"]=starts["__time__"]+pd.to_timedelta(minutes,unit="m")
        merged=pd.merge_asof(starts.sort_values("future_lookup_time"),fut.sort_values("future_lookup_time"),on="future_lookup_time",by="date_key",direction="nearest",tolerance=pd.Timedelta(minutes=max(5,minutes)))
        merged["metric_ret_horizon"]=(pd.to_numeric(merged["future_close"],errors="coerce")-pd.to_numeric(merged["open_num"],errors="coerce"))/pd.to_numeric(merged["open_num"],errors="coerce").replace(0,np.nan)
        return merged[["date_key","metric_ret_horizon"]]

    def _intraday_between_mean(self,target_asset,start,end):
        df=self._load(target_asset).copy()
        ohlc=self._ohlc_cols(df)
        if ohlc["open"] is None or ohlc["close"] is None: return None
        df=df[(df["time_hhmm"]>=start)&(df["time_hhmm"]<=end)].copy()
        if df.empty: return None
        df["close_num"]=pd.to_numeric(df[ohlc["close"]],errors="coerce")
        return df.groupby("date_key",as_index=False).agg(metric_close=("close_num","mean"))

    def run(self,q,preview_rows=20):
        assets=self._resolve_assets(q)
        if not assets: return {"status":"NO_ASSET_RECOGNIZED","answer_type":"explanation"}
        target=self._target_asset(q,assets)
        if target is None: return {"status":"NO_TARGET_ASSET","answer_type":"explanation","assets":assets}
        base=self._daily(target).copy()
        if base.empty: return {"status":"TARGET_EMPTY","answer_type":"explanation","target":target}
        nq=_nrm(q)
        minute_h=self._parse_minutes_horizon(q)
        start,end=self._parse_between(q)
        if minute_h is not None:
            metric_df=self._intraday_horizon_return(target,minute_h)
            if metric_df is not None: base=base.merge(metric_df,on="date_key",how="left")
        elif start and end:
            metric_df=self._intraday_between_mean(target,start,end)
            if metric_df is not None: base=base.merge(metric_df,on="date_key",how="left")
        metric_type=self._target_metric_type(q,base)
        if metric_type=="probability":
            if "metric_ret_horizon" in base.columns:
                base["target_positive"]=pd.to_numeric(base["metric_ret_horizon"],errors="coerce")>0
            elif "ret" in base.columns:
                base["target_positive"]=pd.to_numeric(base["ret"],errors="coerce")>0
            else:
                return {"status":"TARGET_NO_RET_FOR_PROBABILITY","answer_type":"explanation","target":target}
            metric_col="target_positive"
        elif metric_type=="ret":
            metric_col="metric_ret_horizon" if "metric_ret_horizon" in base.columns else "ret"
        elif metric_type in ["open","close","value"]:
            metric_col=metric_type if metric_type in base.columns else ("metric_close" if "metric_close" in base.columns else None)
            if metric_col is None: return {"status":"TARGET_METRIC_NOT_AVAILABLE","answer_type":"explanation","target":target}
        else:
            metric_col="date_key"

        conditions=[]; parsed=[]
        for frag in self._condition_fragments(q):
            asset,tmp,desc=self._daily_condition_from_fragment(frag,assets)
            if tmp is not None:
                cond_col=[c for c in tmp.columns if c!="date_key"][0]
                tmp=tmp.rename(columns={cond_col:desc})
                conditions.append(tmp[["date_key",desc]])
                parsed.append(desc)

        merged=base.copy()
        for c in conditions: merged=merged.merge(c,on="date_key",how="left")
        cond_cols=[c for c in merged.columns if c not in base.columns]
        for c in cond_cols: merged[c]=merged[c].fillna(False).infer_objects(copy=False).astype(bool)
        mask=merged[cond_cols].all(axis=1) if cond_cols else pd.Series(True,index=merged.index)
        wd=self._parse_weekday(q)
        if wd is not None: mask=mask&(merged["weekday_num"]==wd)
        yrs=sorted({int(x) for x in re.findall(r"\b(20\d{2})\b",q)})
        if yrs: mask=mask&(merged["year"].isin(yrs))
        out=merged[mask].copy()

        if "par an" in nq or "per year" in nq:
            g=out.groupby("year",as_index=False).agg(count_days=("date_key","size"))
            return {"status":"OK","answer_type":"table","target_asset":target,"parsed_conditions":parsed,"sample_size":int(len(out)),"value":None,"table":g.to_dict("records"),"preview":g.head(preview_rows).to_dict("records")}
        if metric_type=="count":
            return {"status":"OK","answer_type":"count","target_asset":target,"parsed_conditions":parsed,"sample_size":int(len(out)),"value":int(len(out)),"preview":out[["date_key","year","weekday_name"]].head(preview_rows).to_dict("records")}
        if metric_type=="probability":
            return {"status":"OK","answer_type":"probability","target_asset":target,"parsed_conditions":parsed,"sample_size":int(len(out)),"value":None if out.empty else float(pd.to_numeric(out[metric_col],errors="coerce").mean()),"preview":out[["date_key","year","weekday_name",metric_col]].head(preview_rows).to_dict("records")}
        return {"status":"OK","answer_type":"mean","target_asset":target,"parsed_conditions":parsed,"sample_size":int(len(out)),"value":None if out.empty else float(pd.to_numeric(out[metric_col],errors="coerce").mean()),"preview":out[["date_key","year","weekday_name",metric_col]].head(preview_rows).to_dict("records")}
