import os,re,unicodedata,pandas as pd,numpy as np

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
        base=_slug(c) or "col"
        k=base; i=2
        while k in seen:
            k=f"{base}_{i}"; i+=1
        seen[k]=1
        out.append(k)
    return out

class CandlePatternEngine:
    def __init__(self,source_root,source_config,asset_aliases,synonyms,session_utils):
        self.source_root=source_root
        self.source_config=source_config
        self.asset_aliases=asset_aliases
        self.synonyms=synonyms
        self.session_utils=session_utils
        self.cache={}

    def can_handle(self,q):
        nq=_nrm(q)
        if any(x in nq for x in ["bougie","candlestick","candle","chandelier"]):
            return True
        for vals in self.synonyms.values():
            if isinstance(vals,list) and any(_nrm(v) in nq for v in vals):
                return True
        return False

    def _contains_any(self,q,keys):
        nq=_nrm(q)
        return any(_nrm(k) in nq for k in keys)

    def _resolve_asset(self,q):
        nq=_nrm(q)
        found=[]
        for k,aliases in self.asset_aliases.items():
            if any(_nrm(a) in nq for a in aliases):
                found.append(k)
        for k in found:
            if k in self.source_config and self.source_config[k]["kind"]=="ohlc":
                return k
        for fallback in ["spx","spy","qqq","iwm","gold","oil","dxy"]:
            if fallback in self.source_config:
                return fallback
        return None

    def _read_csv(self,path):
        last=None
        for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
            try:
                with open(path,"r",encoding=enc,errors="replace") as f:
                    lines=f.read().splitlines()
                lines=[x for x in lines if str(x).strip()!=""]
                if not lines:
                    continue
                header_line=lines[0]
                candidates=[",",";","\\t","|"]
                best_sep=None; best_cols=1
                for sep in candidates:
                    n=len(header_line.split(sep))
                    if n>best_cols:
                        best_cols=n; best_sep=sep
                if best_sep is not None and best_cols>1:
                    header=[x.strip() for x in header_line.split(best_sep)]
                    rows=[]
                    for line in lines[1:]:
                        parts=[x.strip() for x in line.split(best_sep)]
                        if len(parts)==len(header):
                            rows.append(parts)
                        elif len(parts)>len(header):
                            fixed=parts[:len(header)-1]+[best_sep.join(parts[len(header)-1:])]
                            if len(fixed)==len(header):
                                rows.append(fixed)
                    if len(rows)>=max(10,int(max(1,len(lines)-1)*0.5)):
                        df=pd.DataFrame(rows,columns=header)
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
        df["__time__"]=df[tcol]
        df["date_key"]=df["__time__"].dt.strftime("%Y-%m-%d")
        df["time_hhmm"]=df["__time__"].dt.strftime("%H:%M")
        self.cache[key]=df
        return df

    def _ohlc_cols(self,df):
        fm=self.session_utils.first_match
        return {
            "open":fm(df.columns,["open"]),
            "high":fm(df.columns,["high"]),
            "low":fm(df.columns,["low"]),
            "close":fm(df.columns,["close"]),
        }

    def _pattern_key(self,q):
        nq=_nrm(q)
        if self._contains_any(nq,self.synonyms.get("bearish_engulfing",[])): return "bearish_engulfing"
        if self._contains_any(nq,self.synonyms.get("bullish_engulfing",[])): return "bullish_engulfing"
        if self._contains_any(nq,self.synonyms.get("two_red_candles",[])) and self._contains_any(nq,self.synonyms.get("body_overlap_full",[])): return "two_red_body_engulf"
        if self._contains_any(nq,self.synonyms.get("two_red_candles",[])): return "two_red"
        if self._contains_any(nq,self.synonyms.get("two_green_candles",[])): return "two_green"
        if self._contains_any(nq,self.synonyms.get("doji",[])): return "doji"
        if self._contains_any(nq,self.synonyms.get("hammer",[])): return "hammer"
        if self._contains_any(nq,self.synonyms.get("shooting_star",[])): return "shooting_star"
        if self._contains_any(nq,self.synonyms.get("inside_bar",[])): return "inside_bar"
        if self._contains_any(nq,self.synonyms.get("outside_bar",[])): return "outside_bar"
        if self._contains_any(nq,self.synonyms.get("morning_star",[])): return "morning_star"
        if self._contains_any(nq,self.synonyms.get("evening_star",[])): return "evening_star"
        if self._contains_any(nq,self.synonyms.get("three_white_soldiers",[])): return "three_white_soldiers"
        if self._contains_any(nq,self.synonyms.get("three_black_crows",[])): return "three_black_crows"
        return None

    def _compute(self,df):
        ohlc=self._ohlc_cols(df)
        if any(v is None for v in ohlc.values()):
            return None, ohlc
        out=df.copy()
        out["open_num"]=pd.to_numeric(out[ohlc["open"]],errors="coerce")
        out["high_num"]=pd.to_numeric(out[ohlc["high"]],errors="coerce")
        out["low_num"]=pd.to_numeric(out[ohlc["low"]],errors="coerce")
        out["close_num"]=pd.to_numeric(out[ohlc["close"]],errors="coerce")
        out=out[out[["open_num","high_num","low_num","close_num"]].notna().all(axis=1)].copy()
        if out.empty:
            return out, ohlc
        out["body"]=(out["close_num"]-out["open_num"]).abs()
        out["range"]=(out["high_num"]-out["low_num"]).abs()
        out["body_high"]=out[["open_num","close_num"]].max(axis=1)
        out["body_low"]=out[["open_num","close_num"]].min(axis=1)
        out["upper_wick"]=out["high_num"]-out["body_high"]
        out["lower_wick"]=out["body_low"]-out["low_num"]
        out["is_green"]=out["close_num"]>out["open_num"]
        out["is_red"]=out["close_num"]<out["open_num"]
        out["prev_open"]=out["open_num"].shift(1)
        out["prev_close"]=out["close_num"].shift(1)
        out["prev_high"]=out["high_num"].shift(1)
        out["prev_low"]=out["low_num"].shift(1)
        out["prev_body_high"]=out["body_high"].shift(1)
        out["prev_body_low"]=out["body_low"].shift(1)
        out["body_pct_of_range"]=np.where(out["range"]>0,out["body"]/out["range"],np.nan)
        out["bearish_engulfing"]=(out["is_red"])&(out["prev_close"]>out["prev_open"])&(out["body_high"]>=out["prev_body_high"])&(out["body_low"]<=out["prev_body_low"])
        out["bullish_engulfing"]=(out["is_green"])&(out["prev_close"]<out["prev_open"])&(out["body_high"]>=out["prev_body_high"])&(out["body_low"]<=out["prev_body_low"])
        out["two_red"]=(out["is_red"])&(out["is_red"].shift(1))
        out["two_green"]=(out["is_green"])&(out["is_green"].shift(1))
        out["two_red_body_engulf"]=(out["is_red"])&(out["is_red"].shift(1))&(out["body_high"]>=out["prev_body_high"])&(out["body_low"]<=out["prev_body_low"])
        out["doji"]=out["body_pct_of_range"].fillna(1)<=0.10
        out["hammer"]=(out["lower_wick"]>=2.0*out["body"])&(out["upper_wick"]<=1.0*out["body"])
        out["shooting_star"]=(out["upper_wick"]>=2.0*out["body"])&(out["lower_wick"]<=1.0*out["body"])
        out["inside_bar"]=(out["high_num"]<=out["prev_high"])&(out["low_num"]>=out["prev_low"])
        out["outside_bar"]=(out["high_num"]>=out["prev_high"])&(out["low_num"]<=out["prev_low"])
        out["three_black_crows"]=(out["is_red"])&(out["is_red"].shift(1))&(out["is_red"].shift(2))
        out["three_white_soldiers"]=(out["is_green"])&(out["is_green"].shift(1))&(out["is_green"].shift(2))
        out["morning_star"]=(out["is_green"])&(out["is_red"].shift(2))
        out["evening_star"]=(out["is_red"])&(out["is_green"].shift(2))
        out["next_open"]=out["open_num"].shift(-1)
        out["next_close"]=out["close_num"].shift(-1)
        out["next_bar_ret"]=(out["next_close"]-out["next_open"])/out["next_open"].replace(0,np.nan)
        daily=out.groupby("date_key",as_index=False).agg(day_open=("open_num","first"),day_close=("close_num","last"))
        daily["next_day_open"]=daily["day_open"].shift(-1)
        daily["next_day_close"]=daily["day_close"].shift(-1)
        daily["next_day_ret"]=(daily["next_day_close"]-daily["next_day_open"])/daily["next_day_open"].replace(0,np.nan)
        out=out.merge(daily[["date_key","next_day_ret"]],on="date_key",how="left")
        return out, ohlc

    def run(self,q,preview_rows=20):
        asset=self._resolve_asset(q)
        if asset is None:
            return {"status":"NO_ASSET","answer_type":"explanation","message":"Aucun actif reconnu."}
        if asset not in self.source_config:
            return {"status":"ASSET_NOT_IN_SOURCE_CONFIG","answer_type":"explanation","asset":asset}
        pattern=self._pattern_key(q)
        if pattern is None:
            return {"status":"PATTERN_NOT_FOUND","answer_type":"explanation","asset":asset}
        df=self._load(asset)
        dfc,ohlc=self._compute(df)
        if dfc is None:
            return {"status":"NO_OHLC","answer_type":"explanation","asset":asset,"ohlc_detected":ohlc}
        if dfc.empty:
            return {"status":"NO_VALID_NUMERIC_OHLC","answer_type":"explanation","asset":asset}
        hits=dfc[dfc[pattern].fillna(False)].copy()
        wants_count=self._contains_any(q,self.synonyms.get("count",[]))
        wants_show=self._contains_any(q,self.synonyms.get("show",[]))
        wants_prob=self._contains_any(q,self.synonyms.get("probability",[]))
        wants_mean=self._contains_any(q,self.synonyms.get("mean",[]))
        wants_next_day=self._contains_any(q,self.synonyms.get("next_day",[]))
        wants_return=self._contains_any(q,self.synonyms.get("return",[]))
        horizon="next_day_ret" if wants_next_day else "next_bar_ret"
        if wants_prob:
            tmp=hits.copy()
            tmp["positive"]=pd.to_numeric(tmp[horizon],errors="coerce")>0
            return {"status":"OK","answer_type":"probability","asset":asset,"pattern":pattern,"value":None if tmp.empty else float(tmp["positive"].mean()),"sample_size":int(len(tmp)),"preview":tmp[["date_key","time_hhmm",horizon]].head(preview_rows).to_dict("records")}
        if wants_mean or wants_return:
            return {"status":"OK","answer_type":"mean","asset":asset,"pattern":pattern,"value":None if hits.empty else float(pd.to_numeric(hits[horizon],errors="coerce").mean()),"sample_size":int(len(hits)),"preview":hits[["date_key","time_hhmm",horizon]].head(preview_rows).to_dict("records")}
        if wants_count:
            return {"status":"OK","answer_type":"count","asset":asset,"pattern":pattern,"value":int(len(hits)),"sample_size":int(len(hits)),"preview":hits[["date_key","time_hhmm"]].head(preview_rows).to_dict("records")}
        if wants_show:
            return {"status":"OK","answer_type":"rows","asset":asset,"pattern":pattern,"sample_size":int(len(hits)),"preview":hits[["date_key","time_hhmm","open_num","high_num","low_num","close_num"]].head(preview_rows).to_dict("records")}
        return {"status":"OK","answer_type":"count","asset":asset,"pattern":pattern,"value":int(len(hits)),"sample_size":int(len(hits)),"preview":hits[["date_key","time_hhmm"]].head(preview_rows).to_dict("records")}
