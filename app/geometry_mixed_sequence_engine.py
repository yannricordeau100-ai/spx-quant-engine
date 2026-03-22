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
    spec=importlib.util.spec_from_file_location("exante_time_engine_geometry",p)
    mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod

class GeometryMixedSequenceEngine:
    def __init__(self,raw_root,source_config,asset_aliases,candle_pattern_synonyms,session_utils):
        self.raw_root=raw_root
        self.source_config=source_config
        self.asset_aliases=asset_aliases
        self.candle_pattern_synonyms=candle_pattern_synonyms
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
        preferred=[]; seen_base=set()
        for k in out:
            base=re.sub(r"_(daily|30m|5m|1m|1h|4h)$","",k)
            if base not in seen_base and base in out:
                preferred.append(base); seen_base.add(base)
            elif base not in seen_base and k==base:
                preferred.append(k); seen_base.add(base)
        for k in out:
            base=re.sub(r"_(daily|30m|5m|1m|1h|4h)$","",k)
            if base not in seen_base:
                preferred.append(k); seen_base.add(base)
        return preferred if preferred else out

    def can_handle(self,q):
        nq=_nrm(q)
        assets=self._resolve_assets(q)
        if not assets:
            return False
        geom=any(x in nq for x in ["recouvre","englobe","engulf","corps","body","wick","mèche","meche","full range"])
        mixed=any(x in nq for x in ["lendemain","next day","au bout de","after"])
        seq=any(x in nq for x in [
            "bougie rouge","bougie verte","red candle","green candle","+","puis","then",
            "bearish engulfing","bullish engulfing",
            "2 bougies rouges","deux bougies rouges","two red candles",
            "2 bougies vertes","deux bougies vertes","two green candles",
            "3 bougies rouges","3 bougies vertes"
        ])
        return geom or mixed or seq

    def _pick_asset(self,q,assets):
        nq=_nrm(q)
        if any(x in nq for x in ["30m","30 min","30min"]):
            v=[a for a in assets if a.endswith("_30m")]
            if v: return v[0]
        if any(x in nq for x in ["daily","journalier"]):
            v=[a for a in assets if a.endswith("_daily")]
            if v: return v[0]
        base=[a for a in assets if not re.search(r"_(daily|30m|5m|1m|1h|4h)$",a)]
        return base[0] if base else assets[0]

    def _seq_mask(self,df,q):
        nq=_nrm(q)
        full=df["_exante_full_candle"].fillna(False).astype(bool) if "_exante_full_candle" in df.columns else pd.Series(False,index=df.index)
        red=df["red_candle"].fillna(False).astype(bool) if "red_candle" in df.columns else pd.Series(False,index=df.index)
        green=df["green_candle"].fillna(False).astype(bool) if "green_candle" in df.columns else pd.Series(False,index=df.index)

        if any(x in nq for x in ["2 bougies rouges","deux bougies rouges","two red candles"]):
            return full & red & full.shift(1).fillna(False).astype(bool) & red.shift(1).fillna(False).astype(bool)
        if any(x in nq for x in ["2 bougies vertes","deux bougies vertes","two green candles"]):
            return full & green & full.shift(1).fillna(False).astype(bool) & green.shift(1).fillna(False).astype(bool)
        if "bougie rouge" in nq or "red candle" in nq:
            return full & red
        if "bougie verte" in nq or "green candle" in nq:
            return full & green
        return pd.Series(False,index=df.index)

    def run(self,q,preview_rows=20):
        assets=self._resolve_assets(q)
        if not assets:
            return {"status":"NO_ASSET_RECOGNIZED","answer_type":"explanation"}

        asset=self._pick_asset(q,assets)
        df=self._load(asset).copy()
        nq=_nrm(q)
        mode=self.exante.detect_query_mode(q)

        # conservative: only use fully closed candles for candle logic
        if "_30m" in asset or "_5m" in asset or "_1m" in asset or "_1h" in asset or "_4h" in asset or asset in ("spx","spy","qqq","iwm"):
            df["_exante_full_candle"]=True
        else:
            df["_exante_full_candle"]=True

        mask=self._seq_mask(df,q)
        out=df[mask].copy()

        years=sorted({int(x) for x in re.findall(r"\b(20\d{2})\b",q)})
        if years:
            out=out[out["year"].isin(years)].copy()

        if any(x in nq for x in ["lendemain","next day"]):
            if "ret" not in out.columns:
                return {"status":"NO_RETURN_COLUMN","answer_type":"explanation","asset":asset}
            next_ret=pd.to_numeric(df["ret"].shift(-1),errors="coerce")
            aligned=next_ret.loc[out.index]
            val=None if aligned.dropna().empty else float(aligned.dropna().mean())
            return {"status":"OK" if len(out)>0 else "OK_NO_MATCHES","answer_type":"mean","asset":asset,"value":val,"sample_size":int(len(out)),"preview":out[["date_key"]].head(preview_rows).to_dict("records"),"exante_mode":mode,"pattern_access":"full_candle_only"}

        wants_count=any(x in nq for x in ["combien","how many","count"])
        if wants_count:
            return {"status":"OK" if len(out)>0 else "OK_NO_MATCHES","answer_type":"count","asset":asset,"value":int(len(out)),"sample_size":int(len(out)),"preview":out[["date_key"]].head(preview_rows).to_dict("records"),"exante_mode":mode,"pattern_access":"full_candle_only"}

        return {"status":"OK" if len(out)>0 else "OK_NO_MATCHES","answer_type":"table","asset":asset,"value":None,"sample_size":int(len(out)),"preview":out[["date_key"]].head(preview_rows).to_dict("records"),"exante_mode":mode,"pattern_access":"full_candle_only"}
