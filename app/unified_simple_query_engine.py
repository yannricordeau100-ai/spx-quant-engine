import os,re,unicodedata,pandas as pd,numpy as np, importlib.util

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
        base=_slug(c) or "col"; k=base; i=2
        while k in seen:
            k=f"{base}_{i}"; i+=1
        seen[k]=1; out.append(k)
    return out

def _load_exante():
    app_dir=os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
    p=os.path.join(app_dir,"exante_time_engine.py")
    spec=importlib.util.spec_from_file_location("exante_time_engine_runtime",p)
    mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod

class UnifiedSimpleQueryEngine:
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
                        if sep is None:
                            df=pd.read_csv(path,sep=None,engine="python",**kw)
                        else:
                            df=pd.read_csv(path,sep=sep,engine="python",**kw)
                        if df is not None and df.shape[1]>=1:
                            df.columns=_normalize_cols(df.columns)
                            return df
                    except Exception as e:
                        last=e
            except Exception as e:
                last=e
        raise last

    def _load(self,key):
        if key in self.cache:
            return self.cache[key]
        path=self.source_config[key]["path"]
        df=self._read_csv(path)
        tcol=self.session_utils.detect_time_col(df)
        if tcol is None:
            raise RuntimeError(f"{key}_TIME_NOT_FOUND")
        s=df[tcol].astype(str)
        dt=pd.to_datetime(s,errors="coerce",format="%Y-%m-%d %H:%M:%S")
        if dt.notna().sum()==0:
            dt=pd.to_datetime(s,errors="coerce",format="%Y-%m-%d")
        if dt.notna().sum()==0:
            dt=pd.to_datetime(s,errors="coerce")
        df[tcol]=dt
        df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
        df["date_key"]=df[tcol].dt.strftime("%Y-%m-%d")
        df["year"]=df[tcol].dt.year
        df["time_hhmm"]=df[tcol].dt.strftime("%H:%M")
        self.cache[key]=df
        return df

    def _resolve_assets(self,q):
        nq=_nrm(q); hits=[]
        for k,aliases in self.asset_aliases.items():
            if any(_nrm(a) in nq for a in aliases):
                hits.append(k)
        return sorted(set(hits))

    def can_handle(self,q):
        nq=_nrm(q)
        assets=self._resolve_assets(q)
        if not assets:
            return False
        has_action=any(x in nq for x in ["combien","how many","count","donne moi les dates","give me the dates","quelles dates","show dates","liste","list","moyenne","average","mean","variation"])
        has_price_words=any(x in nq for x in ["cloture","clôture","cloturé","clôturé","close","closed","performance","return","variation","rendement","open","high","low","price","cours","value","niveau"])
        has_thresh=any(x in nq for x in [">","<","superieur","supérieur","inferieur","inférieur","above","below","plus de","moins de","egal","égal","en absolue","absolute"])
        return has_action and (has_price_words or has_thresh)

    def _pick_best_asset(self,q,assets):
        nq=_nrm(q)
        if any(x in nq for x in ["30m","30 min","30min"]):
            thirty=[a for a in assets if a.endswith("_30m")]
            if thirty: return thirty[0]
        if any(x in nq for x in ["5m","5 min","5min"]):
            five=[a for a in assets if a.endswith("_5m")]
            if five: return five[0]
        if any(x in nq for x in ["daily","journalier","journaliere","journalière"]):
            daily=[a for a in assets if a.endswith("_daily")]
            if daily: return daily[0]
        base=[a for a in assets if not re.search(r"_(daily|30m|5m|1m|1h|4h)$",a)]
        if base: return base[0]
        return assets[0]

    def _parse_threshold(self,q):
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

    def _parse_window(self,q):
        nq=_nrm(q)
        m=re.search(r"entre\s+(\d{1,2})[:h](\d{2})\s+et\s+(\d{1,2})[:h](\d{2})", nq)
        if not m:
            return None
        h1,m1,h2,m2=map(int,m.groups())
        return (h1,m1),(h2,m2)

    def _parse_date_range(self,q):
        years=re.findall(r"\b(20\d{2})\b", q)
        if len(years)>=2:
            return None
        m=re.search(r"du\s+(\d{1,2})\s+([a-zéûîôà]+)\s+au\s+(\d{1,2})\s+([a-zéûîôà]+)", q.lower())
        if not m:
            return None
        return m.groups()

    def _metric(self,q,df):
        nq=_nrm(q)
        if "en absolue" in nq or "absolute" in nq:
            return "ret_abs" if "ret_abs" in df.columns else ("ret" if "ret" in df.columns else None)
        if any(x in nq for x in ["performance","return","variation","rendement"]) or "%" in nq or any(x in nq for x in ["a cloture a plus de","a clôturé à plus de","closed more than"]):
            return "ret" if "ret" in df.columns else ("close" if "close" in df.columns else "value")
        if any(x in nq for x in ["open","opening","ouverture"]):
            return "open" if "open" in df.columns else ("value" if "value" in df.columns else "close")
        if "high" in nq:
            return "high" if "high" in df.columns else ("value" if "value" in df.columns else "close")
        if "low" in nq:
            return "low" if "low" in df.columns else ("value" if "value" in df.columns else "close")
        if any(x in nq for x in ["value","niveau","cours","close","cloture","clôture"]):
            return "close" if "close" in df.columns else ("value" if "value" in df.columns else None)
        return "close" if "close" in df.columns else ("value" if "value" in df.columns else None)

    def _dailyize(self,asset_key):
        df=self._load(asset_key)
        fm=self.session_utils.first_match
        kind=self.source_config[asset_key].get("kind","ohlc")
        if kind!="ohlc":
            vcol=fm(df.columns,["spread_10y_minus_2y","value","close","open","high","low","us_2y"])
            if vcol is None:
                raise RuntimeError(f"{asset_key}_VALUE_NOT_FOUND")
            out=df.groupby(["date_key","year"],as_index=False).agg(value=(vcol,"last"))
            out["value"]=pd.to_numeric(out["value"],errors="coerce")
            return out

        o=fm(df.columns,["open"]); h=fm(df.columns,["high"]); l=fm(df.columns,["low"]); c=fm(df.columns,["close"])
        agg={}
        if o: agg[o]="first"
        if h: agg[h]="max"
        if l: agg[l]="min"
        if c: agg[c]="last"
        if not agg:
            raise RuntimeError(f"{asset_key}_OHLC_NOT_FOUND")
        out=df.groupby(["date_key","year"],as_index=False).agg(agg)
        ren={}
        if o: ren[o]="open"
        if h: ren[h]="high"
        if l: ren[l]="low"
        if c: ren[c]="close"
        out=out.rename(columns=ren)
        for x in ["open","high","low","close"]:
            if x in out.columns:
                out[x]=pd.to_numeric(out[x],errors="coerce")
        if "open" in out.columns and "close" in out.columns:
            out["ret"]=(out["close"]-out["open"])/out["open"].replace(0,np.nan)
            out["ret_abs"]=out["ret"].abs()
        return out

    def _intraday_prepare(self,asset_key):
        df=self._load(asset_key).copy()
        fm=self.session_utils.first_match
        o=fm(df.columns,["open"]); h=fm(df.columns,["high"]); l=fm(df.columns,["low"]); c=fm(df.columns,["close"])
        ren={}
        if o: ren[o]="open"
        if h: ren[h]="high"
        if l: ren[l]="low"
        if c: ren[c]="close"
        df=df.rename(columns=ren)
        for x in ["open","high","low","close"]:
            if x in df.columns:
                df[x]=pd.to_numeric(df[x],errors="coerce")
        if "open" in df.columns and "close" in df.columns:
            df["ret"]=(df["close"]-df["open"])/df["open"].replace(0,np.nan)
            df["ret_abs"]=df["ret"].abs()
        return df

    def run(self,q,preview_rows=20):
        assets=self._resolve_assets(q)
        if not assets:
            return {"status":"NO_ASSET_RECOGNIZED","answer_type":"explanation"}

        asset=self._pick_best_asset(q,assets)
        nq=_nrm(q)
        mode=self.exante.detect_query_mode(q)
        intraday_window=self._parse_window(q)

        # intraday window query
        if intraday_window:
            df=self._intraday_prepare(asset)
            if df.empty:
                return {"status":"EMPTY_ASSET","answer_type":"explanation","asset":asset}
            (h1,m1),(h2,m2)=intraday_window
            start_minutes=h1*60+m1
            end_minutes=h2*60+m2
            mins=df[df.columns[df.columns.get_loc("time_hhmm") if "time_hhmm" in df.columns else 0]] if False else None
            hhmm=df["time_hhmm"].astype(str)
            mm=hhmm.str.slice(0,2).astype(int)*60+hhmm.str.slice(3,5).astype(int)
            x=df[(mm>=start_minutes)&(mm<end_minutes)].copy()
            metric=self._metric(q,x)
            if metric is None or metric not in x.columns:
                return {"status":"METRIC_NOT_AVAILABLE","answer_type":"explanation","asset":asset,"metric":metric}
            wants_abs=("en absolue" in nq or "absolute" in nq)
            s=pd.to_numeric(x[metric],errors="coerce")
            val=None if x.empty else float(s.mean())
            return {"status":"OK" if len(x)>0 else "OK_NO_MATCHES","answer_type":"mean","asset":asset,"metric":metric,"sample_size":int(len(x)),"value":val,"preview":x[["date_key","time_hhmm",metric]].head(preview_rows).to_dict("records"),"exante_mode":mode}

        # daily/open style query
        df=self._dailyize(asset)

        years=sorted({int(x) for x in re.findall(r"\b(20\d{2})\b",q)})
        if years:
            df=df[df["year"].isin(years)].copy()

        metric=self._metric(q,df)
        if metric is None or metric not in df.columns:
            return {"status":"METRIC_NOT_AVAILABLE","answer_type":"explanation","asset":asset,"metric":metric}

        op,val,pct=self._parse_threshold(q)
        work=df.copy()
        s=pd.to_numeric(work[metric],errors="coerce")
        if val is not None:
            threshold=(val/100.0 if (pct and metric in ("ret","ret_abs")) else val)
            if op==">": work=work[s>threshold].copy()
            elif op=="<": work=work[s<threshold].copy()
            elif op==">=": work=work[s>=threshold].copy()
            elif op=="<=": work=work[s<=threshold].copy()

        wants_dates=any(x in nq for x in ["donne moi les dates","give me the dates","quelles dates","show dates","liste","list"])
        wants_count=any(x in nq for x in ["combien","how many","count"])
        wants_mean=any(x in nq for x in ["moyenne","average","mean","variation"])

        if wants_dates and wants_count:
            return {"status":"OK" if len(work)>0 else "OK_NO_MATCHES","answer_type":"table","asset":asset,"metric":metric,"sample_size":int(len(work)),"value":int(len(work)),"preview":work[["date_key","year",metric]].head(preview_rows).to_dict("records"),"exante_mode":mode}
        if wants_dates:
            return {"status":"OK" if len(work)>0 else "OK_NO_MATCHES","answer_type":"table","asset":asset,"metric":metric,"sample_size":int(len(work)),"value":None,"preview":work[["date_key","year",metric]].head(preview_rows).to_dict("records"),"exante_mode":mode}
        if wants_mean:
            return {"status":"OK" if len(work)>0 else "OK_NO_MATCHES","answer_type":"mean","asset":asset,"metric":metric,"sample_size":int(len(work)),"value":None if work.empty else float(pd.to_numeric(work[metric],errors="coerce").mean()),"preview":work[["date_key","year",metric]].head(preview_rows).to_dict("records"),"exante_mode":mode}
        return {"status":"OK" if len(work)>0 else "OK_NO_MATCHES","answer_type":"count","asset":asset,"metric":metric,"sample_size":int(len(work)),"value":int(len(work)),"preview":work[["date_key","year",metric]].head(preview_rows).to_dict("records"),"exante_mode":mode}
