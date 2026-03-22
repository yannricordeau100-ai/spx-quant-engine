import os,re,unicodedata,pandas as pd,numpy as np, importlib.util

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

def _load_exante():
    app_dir=os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
    p=os.path.join(app_dir,"exante_time_engine.py")
    spec=importlib.util.spec_from_file_location("exante_time_engine_cross",p)
    mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod

class CrossAssetConditionEngine:
    def __init__(self,source_config,asset_aliases,session_utils):
        self.source_config=source_config
        self.asset_aliases=asset_aliases
        self.session_utils=session_utils
        self.cache={}
        self.exante=_load_exante()

    def _read_csv(self,path):
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        last=None
        for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
            try:
                with open(path,"r",encoding=enc,errors="replace") as f:
                    lines=[x for x in f.read().splitlines() if str(x).strip()!=""]
                if not lines:
                    continue
                header=lines[0]
                best_sep=None; best_n=1
                for sep in [",",";","\\t","|"]:
                    n=len(header.split(sep))
                    if n>best_n:
                        best_n=n; best_sep=sep
                if best_sep and best_n>1:
                    cols=[x.strip() for x in header.split(best_sep)]
                    rows=[]
                    for line in lines[1:]:
                        parts=[x.strip() for x in line.split(best_sep)]
                        if len(parts)==len(cols):
                            rows.append(parts)
                    if len(rows)>=max(10,int(len(lines)*0.4)):
                        df=pd.DataFrame(rows,columns=cols)
                        df.columns=_normalize_cols(df.columns)
                        return df
                for sep in (",",";","\\t","|",None):
                    try:
                        kw={"encoding":enc,"on_bad_lines":"skip"}
                        if sep is None: df=pd.read_csv(path,sep=None,engine="python",**kw)
                        else: df=pd.read_csv(path,sep=sep,engine="python",**kw)
                        if df is not None and df.shape[1]>=1:
                            df.columns=_normalize_cols(df.columns)
                            return df
                    except Exception as e:
                        last=e
            except Exception as e:
                last=e
        raise last

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
        df["date_key"]=df[tcol].dt.strftime("%Y-%m-%d")
        df["year"]=df[tcol].dt.year
        fm=self.session_utils.first_match
        o=fm(df.columns,["open"]); h=fm(df.columns,["high"]); l=fm(df.columns,["low"]); c=fm(df.columns,["close"])
        ren={}
        if o: ren[o]="open"
        if h: ren[h]="high"
        if l: ren[l]="low"
        if c: ren[c]="close"
        df=df.rename(columns=ren)
        for x in ["open","high","low","close"]:
            if x in df.columns: df[x]=pd.to_numeric(df[x],errors="coerce")
        if "open" in df.columns and "close" in df.columns:
            df["ret"]=(df["close"]-df["open"])/df["open"].replace(0,np.nan)
            df["up"]=df["ret"]>0
            df["green_candle"]=df["close"]>df["open"]
            df["red_candle"]=df["close"]<df["open"]
        self.cache[key]=df
        return df

    def _resolve_assets(self,q):
        nq=_nrm(q); out=[]
        for k,aliases in self.asset_aliases.items():
            if any(_nrm(a) in nq for a in aliases):
                out.append(k)
        out=sorted(set(out))
        preferred=[]; seen=set()
        for k in out:
            base=re.sub(r"_(daily|30m|5m|1m|1h|4h)$","",k)
            if base in out and base not in seen:
                preferred.append(base); seen.add(base)
        for k in out:
            base=re.sub(r"_(daily|30m|5m|1m|1h|4h)$","",k)
            if base not in seen and k not in seen:
                preferred.append(k); seen.add(base); seen.add(k)
        return preferred if preferred else out

    def can_handle(self,q):
        nq=_nrm(q)
        assets=self._resolve_assets(q)
        if len(assets)<2: return False
        relation_words=["quand","when","si","if","lorsque","while"]
        threshold_words=[">","<","plus de","moins de","above","below","superieur","supérieur","inferieur","inférieur"]
        target_words=["performance","return","combien","how many","count","donne moi les dates","show dates","dates","monte","hausse","up","variation"]
        candle_words=["2 bougies rouges","deux bougies rouges","two red candles","2 bougies vertes","deux bougies vertes","two green candles","bougie rouge","bougie verte","red candle","green candle"]
        return any(x in nq for x in relation_words) and (any(x in nq for x in threshold_words+candle_words) and any(x in nq for x in target_words))

    def _cmp_parse(self,q):
        nq=_nrm(q)
        mapping=[
            (">=",[" >=",">=","superieur ou egal","supérieur ou égal","greater than or equal","at least"]),
            ("<=",[" <=","<=","inferieur ou egal","inférieur ou égal","less than or equal","at most"]),
            (">",[" > ",">","superieur a","supérieur à","above","greater than","plus de"]),
            ("<",[" < ","<","inferieur a","inférieur à","below","less than","moins de"]),
        ]
        for op,keys in mapping:
            pos=None; hit=None
            for k in keys:
                p=nq.find(k)
                if p!=-1 and (pos is None or p<pos):
                    pos=p; hit=k
            if hit is not None:
                right=nq[pos+len(hit):]
                m=re.search(r"(-?\d+(?:[\.,]\d+)?)\s*%?",right)
                if m:
                    raw=float(m.group(1).replace(",","."))
                    pct=("%" in right)
                    return op,raw,pct
        return None,None,None

    def _daily(self,key):
        df=self._load(key)
        kind=self.source_config[key].get("kind","ohlc")
        if kind!="ohlc":
            vcol="value" if "value" in df.columns else ("close" if "close" in df.columns else None)
            out=df.groupby(["date_key","year"],as_index=False).agg(value=(vcol,"last"))
            out["value"]=pd.to_numeric(out["value"],errors="coerce")
            return out
        agg={}
        for col,fn in [("open","first"),("high","max"),("low","min"),("close","last")]:
            if col in df.columns: agg[col]=fn
        out=df.groupby(["date_key","year"],as_index=False).agg(agg)
        if "open" in out.columns and "close" in out.columns:
            out["ret"]=(out["close"]-out["open"])/out["open"].replace(0,np.nan)
            out["up"]=out["ret"]>0
            out["green_candle"]=out["close"]>out["open"]
            out["red_candle"]=out["close"]<out["open"]
        return out

    def can_count_two_red(self,q):
        nq=_nrm(q)
        return any(x in nq for x in ["2 bougies rouges","deux bougies rouges","two red candles"])

    def run(self,q,preview_rows=20):
        nq=_nrm(q)
        assets=self._resolve_assets(q)
        if len(assets)<2:
            return {"status":"NEED_2_ASSETS","answer_type":"explanation"}

        target=assets[0]
        cond_assets=assets[1:]
        base=self._daily(target).copy()
        if "ret" not in base.columns and "value" not in base.columns:
            return {"status":"TARGET_METRIC_NOT_AVAILABLE","answer_type":"explanation","target":target}

        cond_mask=pd.Series(True,index=base.index)
        cond_details=[]
        op,val,pct=self._cmp_parse(q)

        for ca in cond_assets:
            cdf=self._daily(ca).copy()
            local=pd.Series(True,index=base.index)
            if self.can_count_two_red(q):
                cond_series=(cdf["red_candle"].fillna(False).astype(bool) & cdf["red_candle"].shift(1).fillna(False).astype(bool)) if "red_candle" in cdf.columns else pd.Series(False,index=cdf.index)
                tmp=cdf[["date_key"]].copy(); tmp[f"{ca}__cond"]=cond_series.values
                base=base.merge(tmp,on="date_key",how="left")
                local=local & base[f"{ca}__cond"].fillna(False).astype(bool)
                cond_details.append(f"{ca}:two_red_candles")
            elif "bougie rouge" in nq or "red candle" in nq:
                tmp=cdf[["date_key","red_candle"]].rename(columns={"red_candle":f"{ca}__red"})
                base=base.merge(tmp,on="date_key",how="left")
                local=local & base[f"{ca}__red"].fillna(False).astype(bool)
                cond_details.append(f"{ca}:red_candle")
            elif "bougie verte" in nq or "green candle" in nq:
                tmp=cdf[["date_key","green_candle"]].rename(columns={"green_candle":f"{ca}__green"})
                base=base.merge(tmp,on="date_key",how="left")
                local=local & base[f"{ca}__green"].fillna(False).astype(bool)
                cond_details.append(f"{ca}:green_candle")
            elif op is not None:
                metric_col="value" if "value" in cdf.columns else ("ret" if "ret" in cdf.columns else None)
                if metric_col is not None:
                    tmp=cdf[["date_key",metric_col]].rename(columns={metric_col:f"{ca}__metric"})
                    base=base.merge(tmp,on="date_key",how="left")
                    s=pd.to_numeric(base[f"{ca}__metric"],errors="coerce")
                    threshold=(val/100.0 if (pct and metric_col=="ret") else val)
                    if op==">": local=local & (s>threshold)
                    elif op=="<": local=local & (s<threshold)
                    elif op==">=": local=local & (s>=threshold)
                    elif op=="<=": local=local & (s<=threshold)
                    cond_details.append(f"{ca}:{metric_col}:{op}:{threshold}")
            cond_mask=cond_mask & local.fillna(False)

        out=base[cond_mask].copy()
        years=sorted({int(x) for x in re.findall(r"\b(20\d{2})\b",q)})
        if years:
            out=out[out["year"].isin(years)].copy()

        wants_dates=any(x in nq for x in ["dates","date","donne moi les dates","show dates","quelles dates","liste","list"])
        wants_count=any(x in nq for x in ["combien","how many","count"])
        target_up=(" monte " in f" {nq} ") or (" up " in f" {nq} ")

        if target_up and "ret" in out.columns:
            s=(pd.to_numeric(out["ret"],errors="coerce")>0)
            val_out=None if len(s)==0 else int(s.sum())
            preview=out[["date_key","ret"]].head(preview_rows).to_dict("records")
            return {"status":"OK" if len(out)>0 else "OK_NO_MATCHES","answer_type":"count","value":val_out if wants_count else None,"preview":preview,"target":target,"conditions":cond_details,"sample_size":int(len(out)),"exante_join":"daily_conservative"}

        if "ret" in out.columns:
            mean_val=None if out.empty else float(pd.to_numeric(out["ret"],errors="coerce").mean())
            if wants_dates and wants_count:
                return {"status":"OK" if len(out)>0 else "OK_NO_MATCHES","answer_type":"table","value":int(len(out)),"preview":out[["date_key","ret"]].head(preview_rows).to_dict("records"),"target":target,"conditions":cond_details,"sample_size":int(len(out)),"exante_join":"daily_conservative"}
            if wants_count:
                return {"status":"OK" if len(out)>0 else "OK_NO_MATCHES","answer_type":"count","value":int(len(out)),"preview":out[["date_key","ret"]].head(preview_rows).to_dict("records"),"target":target,"conditions":cond_details,"sample_size":int(len(out)),"exante_join":"daily_conservative"}
            return {"status":"OK" if len(out)>0 else "OK_NO_MATCHES","answer_type":"mean","value":mean_val,"preview":out[["date_key","ret"]].head(preview_rows).to_dict("records"),"target":target,"conditions":cond_details,"sample_size":int(len(out)),"exante_join":"daily_conservative"}

        return {"status":"NO_TARGET_RETURN","answer_type":"explanation","target":target,"conditions":cond_details}
