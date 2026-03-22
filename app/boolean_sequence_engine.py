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

class BooleanSequenceEngine:
    def __init__(self,source_root,source_config,asset_aliases,candle_synonyms,session_utils):
        self.source_root=source_root
        self.source_config=source_config
        self.asset_aliases=asset_aliases
        self.candle_synonyms=candle_synonyms
        self.session_utils=session_utils
        self.cache={}
        self.weekday_map={"lundi":0,"monday":0,"mardi":1,"tuesday":1,"mercredi":2,"wednesday":2,"jeudi":3,"thursday":3,"vendredi":4,"friday":4,"samedi":5,"saturday":5,"dimanche":6,"sunday":6}

    def can_handle(self,q):
        nq=_nrm(q)
        has_bool=any(x in nq for x in ["(",")"," ou "," or "," et "," and "])
        has_candle=any(x in nq for x in ["bougie","candle","engulf","star","crow","soldier"])
        has_cmp=any(x in nq for x in ["<",">","<=",">=","inferieur","inférieur","superieur","supérieur","below","above"])
        has_horizon=any(x in nq for x in ["au bout de","after","minutes","minute","premieres","premières","between","entre"])
        return has_bool or (has_candle and has_cmp) or (has_horizon and has_bool)

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
        df=self._read_csv(self.source_config[key]["path"])
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
        bad={"date_key","time_hhmm","__time__","year","weekday_num","weekday_name","month_num","month_name"}
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
                out["prev_high"]=out["high"].shift(1) if "high" in out.columns else np.nan
                out["prev_low"]=out["low"].shift(1) if "low" in out.columns else np.nan
                out["prev_body_high"]=out["body_high"].shift(1); out["prev_body_low"]=out["body_low"].shift(1)
                out["bullish_engulfing"]=(out["green_candle"])&(out["prev_close"]<out["prev_open"])&(out["body_high"]>=out["prev_body_high"])&(out["body_low"]<=out["prev_body_low"])
                out["bearish_engulfing"]=(out["red_candle"])&(out["prev_close"]>out["prev_open"])&(out["body_high"]>=out["prev_body_high"])&(out["body_low"]<=out["prev_body_low"])
                out["three_black_crows"]=(out["red_candle"])&(out["red_candle"].shift(1))&(out["red_candle"].shift(2))
                out["three_white_soldiers"]=(out["green_candle"])&(out["green_candle"].shift(1))&(out["green_candle"].shift(2))
                out["morning_star"]=(out["green_candle"])&(out["red_candle"].shift(2))
                out["evening_star"]=(out["red_candle"])&(out["green_candle"].shift(2))
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
        for p in [r"au bout de\s+(\d+)\s*(?:minute|min|minutes|mins)",r"after\s+(\d+)\s*(?:minute|min|minutes|mins)"]:
            m=re.search(p,nq)
            if m: return int(m.group(1))
        return None

    def _target_asset(self,q,assets):
        nq=_nrm(q)
        prefixes=["quelle est la performance de","what is the return of","quelle est la moyenne de","average of","moyenne de","probabilite que","probabilité que","probability that","combien de jours est valable","how many days is","combien de fois","how many times"]
        cuts=[" uniquement "," only "," si "," if "," et "," and "," ou "," or "," au bout de "," after "," entre "," between "," le tout "," uniquement les "," par an "," per year "]
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

    def _metric_type(self,q,base):
        nq=_nrm(q)
        if "probab" in nq or "chance" in nq: return "probability"
        if "combien" in nq or "count" in nq: return "count"
        if any(x in nq for x in ["performance","return","retour","rendement","variation","perf"]): return "ret"
        if "open" in nq or "ouvre" in nq: return "open"
        return "close" if "close" in base.columns else ("value" if "value" in base.columns else "count")

    def _cmp_parse(self,frag):
        f=_nrm(frag)
        mapping={
            "<=":["<=","inferieur ou egal a","inférieur ou égal à","less than or equal to"],
            ">=":[">=","superieur ou egal a","supérieur ou égal à","greater than or equal to"],
            "<":["<","inferieur a","inférieur à","below","less than"],
            ">":[">","superieur a","supérieur à","above","greater than","au dessus de"],
            "=":["=","egal a","égal à","equal to"]
        }
        for op,kws in mapping.items():
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

    def _tokenize_bool(self,text):
        s=" "+_nrm(text)+" "
        s=s.replace("(", " ( ").replace(")", " ) ")
        s=s.replace(" uniquement dans le cas ou "," and ")
        s=s.replace(" uniquement dans le cas où "," and ")
        s=s.replace(" only if "," and ")
        s=s.replace(" et "," and ")
        s=s.replace(" ou "," or ")
        s=s.replace(" and "," AND ")
        s=s.replace(" or "," OR ")
        toks=[t for t in s.split() if t]
        return toks

    def _split_expr_terms(self,text):
        toks=self._tokenize_bool(text)
        parts=[]; cur=[]
        for t in toks:
            if t in ["AND","OR","(",")"]:
                if cur: parts.append(" ".join(cur)); cur=[]
                parts.append(t)
            else:
                cur.append(t)
        if cur: parts.append(" ".join(cur))
        return parts

    def _build_free_sequence(self,frag,df):
        nq=_nrm(frag)
        repl=nq.replace(" puis "," + ").replace(" then "," + ")
        parts=[x.strip() for x in repl.split("+") if x.strip()]
        expanded=[]
        for p in parts:
            if any(x in p for x in ["bougie rouge","red candle"]): expanded.append("red_candle")
            elif any(x in p for x in ["bougie verte","green candle"]): expanded.append("green_candle")
            elif any(x in p for x in ["2 bougies rouges","deux bougies rouges","two red candles"]): expanded+=["red_candle","red_candle"]
            elif any(x in p for x in ["2 bougies vertes","deux bougies vertes","two green candles"]): expanded+=["green_candle","green_candle"]
            elif any(x in p for x in ["3 bougies rouges","three red candles"]): expanded+=["red_candle","red_candle","red_candle"]
            elif any(x in p for x in ["3 bougies vertes","three green candles"]): expanded+=["green_candle","green_candle","green_candle"]
        if not expanded: return None
        cond=pd.Series(True,index=df.index)
        L=len(expanded)
        for i,name in enumerate(expanded):
            if name not in df.columns: return None
            cond=cond & df[name].shift(L-1-i).fillna(False).infer_objects(copy=False).astype(bool)
        return cond

    def _series_for_term(self,term,assets,base_dates):
        frag=_nrm(term)
        for asset in assets:
            if any(_nrm(a) in frag for a in self.asset_aliases.get(asset,[])):
                df=self._daily(asset).copy()
                seq=self._build_free_sequence(frag,df)
                if seq is not None:
                    out=df[["date_key"]].copy(); out["cond"]=seq
                    return out
                candle_map={
                    "bougie rouge":"red_candle","red candle":"red_candle","bougie verte":"green_candle","green candle":"green_candle",
                    "bearish engulfing":"bearish_engulfing","bullish engulfing":"bullish_engulfing","three black crows":"three_black_crows","three white soldiers":"three_white_soldiers",
                    "morning star":"morning_star","evening star":"evening_star"
                }
                for k,v in candle_map.items():
                    if _nrm(k) in frag and v in df.columns:
                        out=df[["date_key",v]].copy().rename(columns={v:"cond"})
                        return out
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
                out=df[["date_key",metric]].copy()
                s=pd.to_numeric(out[metric],errors="coerce")
                if op=="<": out["cond"]=s<val
                elif op==">": out["cond"]=s>val
                elif op=="<=": out["cond"]=s<=val
                elif op==">=": out["cond"]=s>=val
                else: out["cond"]=np.isclose(s,val,equal_nan=False)
                return out[["date_key","cond"]]
        out=base_dates.copy(); out["cond"]=False
        return out

    def _eval_boolean(self,text,assets,base_dates):
        parts=self._split_expr_terms(text)
        cond_map={}
        rebuilt=[]
        idx=0
        for p in parts:
            if p in ["AND","OR","(",")"]:
                rebuilt.append(p)
            else:
                key=f"T{idx}"
                cond_map[key]=self._series_for_term(p,assets,base_dates)
                rebuilt.append(key)
                idx+=1
        merged=base_dates.copy()
        for k,df in cond_map.items():
            merged=merged.merge(df.rename(columns={"cond":k}),on="date_key",how="left")
            merged[k]=merged[k].fillna(False).infer_objects(copy=False).astype(bool)
        # shunting-yard
        prec={"OR":1,"AND":2}
        output=[]; ops=[]
        for tok in rebuilt:
            if tok.startswith("T"): output.append(tok)
            elif tok in ["AND","OR"]:
                while ops and ops[-1] in prec and prec[ops[-1]]>=prec[tok]:
                    output.append(ops.pop())
                ops.append(tok)
            elif tok=="(":
                ops.append(tok)
            elif tok==")":
                while ops and ops[-1]!="(":
                    output.append(ops.pop())
                if ops and ops[-1]=="(": ops.pop()
        while ops: output.append(ops.pop())
        stack=[]
        for tok in output:
            if tok.startswith("T"):
                stack.append(merged[tok].astype(bool))
            else:
                b=stack.pop(); a=stack.pop()
                stack.append((a & b) if tok=="AND" else (a | b))
        return merged,(stack[-1] if stack else pd.Series(True,index=merged.index)),[p for p in parts if p not in ["AND","OR","(",")"]]

    def _intraday_horizon_return(self,target_asset,minutes):
        df=self._load(target_asset).copy()
        ohlc=self._ohlc_cols(df)
        if ohlc["open"] is None or ohlc["close"] is None: return None
        df["open_num"]=pd.to_numeric(df[ohlc["open"]],errors="coerce")
        df["close_num"]=pd.to_numeric(df[ohlc["close"]],errors="coerce")
        df=df[df["open_num"].notna() & df["close_num"].notna()].copy()
        if target_asset in ["spx","spy","qqq"]:
            df=df[(df["time_hhmm"]>="09:30")&(df["time_hhmm"]<="16:00")].copy()
        opens=df.groupby("date_key",as_index=False).agg(session_open=("__time__","min"))
        work=df.merge(opens,on="date_key",how="left")
        work["mins_from_open"]=(work["__time__"]-work["session_open"]).dt.total_seconds()/60.0
        starts=work[work["mins_from_open"]==0].copy()
        if starts.empty: starts=work.groupby("date_key",as_index=False).head(1).copy()
        fut=work[["date_key","__time__","close_num"]].rename(columns={"__time__":"future_lookup_time","close_num":"future_close"})
        starts["future_lookup_time"]=starts["__time__"]+pd.to_timedelta(minutes,unit="m")
        merged=pd.merge_asof(starts.sort_values("future_lookup_time"),fut.sort_values("future_lookup_time"),on="future_lookup_time",by="date_key",direction="nearest",tolerance=pd.Timedelta(minutes=max(5,minutes)))
        merged["metric_ret_horizon"]=(pd.to_numeric(merged["future_close"],errors="coerce")-pd.to_numeric(merged["open_num"],errors="coerce"))/pd.to_numeric(merged["open_num"],errors="coerce").replace(0,np.nan)
        return merged[["date_key","metric_ret_horizon"]]

    def _intraday_between_mean(self,target_asset,start,end):
        df=self._load(target_asset).copy()
        ohlc=self._ohlc_cols(df)
        if ohlc["close"] is None: return None
        df=df[(df["time_hhmm"]>=start)&(df["time_hhmm"]<=end)].copy()
        df["close_num"]=pd.to_numeric(df[ohlc["close"]],errors="coerce")
        return df.groupby("date_key",as_index=False).agg(metric_close=("close_num","mean"))

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

    def run(self,q,preview_rows=20):
        assets=self._resolve_assets(q)
        if not assets: return {"status":"NO_ASSET_RECOGNIZED","answer_type":"explanation"}
        target=self._target_asset(q,assets)
        if target is None: return {"status":"NO_TARGET_ASSET","answer_type":"explanation","assets":assets}
        base=self._daily(target).copy()
        if base.empty: return {"status":"TARGET_EMPTY","answer_type":"explanation","target":target}
        nq=_nrm(q)
        mins=self._parse_minutes_horizon(q)
        start,end=self._parse_between(q)
        if mins is not None:
            extra=self._intraday_horizon_return(target,mins)
            if extra is not None: base=base.merge(extra,on="date_key",how="left")
        elif start and end:
            extra=self._intraday_between_mean(target,start,end)
            if extra is not None: base=base.merge(extra,on="date_key",how="left")
        metric_type=self._metric_type(q,base)
        if metric_type=="probability":
            if "metric_ret_horizon" in base.columns: base["target_positive"]=pd.to_numeric(base["metric_ret_horizon"],errors="coerce")>0
            elif "ret" in base.columns: base["target_positive"]=pd.to_numeric(base["ret"],errors="coerce")>0
            else: return {"status":"TARGET_NO_RET_FOR_PROBABILITY","answer_type":"explanation","target":target}
            metric_col="target_positive"
        elif metric_type=="ret":
            metric_col="metric_ret_horizon" if "metric_ret_horizon" in base.columns else "ret"
        elif metric_type=="open":
            metric_col="open" if "open" in base.columns else None
            if metric_col is None: return {"status":"TARGET_OPEN_NOT_AVAILABLE","answer_type":"explanation","target":target}
        elif metric_type=="close":
            metric_col="metric_close" if "metric_close" in base.columns else ("close" if "close" in base.columns else None)
            if metric_col is None: return {"status":"TARGET_CLOSE_NOT_AVAILABLE","answer_type":"explanation","target":target}
        elif metric_type=="value":
            metric_col="value" if "value" in base.columns else None
            if metric_col is None: return {"status":"TARGET_VALUE_NOT_AVAILABLE","answer_type":"explanation","target":target}
        else:
            metric_col="date_key"
        base_dates=base[["date_key"]].drop_duplicates().copy()
        merged,mask,parsed=self._eval_boolean(q,assets,base_dates)
        out=base.merge(merged[["date_key"]].assign(__mask__=mask.values),on="date_key",how="left")
        out["__mask__"]=out["__mask__"].fillna(False).infer_objects(copy=False).astype(bool)
        wd=self._parse_weekday(q)
        if wd is not None: out["__mask__"]=out["__mask__"]&(out["weekday_num"]==wd)
        yrs=sorted({int(x) for x in re.findall(r"\b(20\d{2})\b",q)})
        if yrs: out["__mask__"]=out["__mask__"]&(out["year"].isin(yrs))
        out=out[out["__mask__"]].copy()
        if "par an" in nq or "per year" in nq:
            g=out.groupby("year",as_index=False).agg(count_days=("date_key","size"))
            return {"status":"OK","answer_type":"table","target_asset":target,"parsed_conditions":parsed,"sample_size":int(len(out)),"value":None,"table":g.to_dict("records"),"preview":g.head(preview_rows).to_dict("records")}
        if metric_type=="count":
            return {"status":"OK","answer_type":"count","target_asset":target,"parsed_conditions":parsed,"sample_size":int(len(out)),"value":int(len(out)),"preview":out[["date_key","year","weekday_name"]].head(preview_rows).to_dict("records")}
        if metric_type=="probability":
            return {"status":"OK","answer_type":"probability","target_asset":target,"parsed_conditions":parsed,"sample_size":int(len(out)),"value":None if out.empty else float(pd.to_numeric(out[metric_col],errors="coerce").mean()),"preview":out[["date_key","year","weekday_name",metric_col]].head(preview_rows).to_dict("records")}
        return {"status":"OK","answer_type":"mean","target_asset":target,"parsed_conditions":parsed,"sample_size":int(len(out)),"value":None if out.empty else float(pd.to_numeric(out[metric_col],errors="coerce").mean()),"preview":out[["date_key","year","weekday_name",metric_col]].head(preview_rows).to_dict("records")}
