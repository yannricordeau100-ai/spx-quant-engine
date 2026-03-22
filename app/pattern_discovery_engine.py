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
        base=_slug(c) or "col"; k=base; i=2
        while k in seen:
            k=f"{base}_{i}"; i+=1
        seen[k]=1; out.append(k)
    return out

class PatternDiscoveryEngine:
    def __init__(self, source_config, asset_aliases, session_utils):
        self.source_config=source_config
        self.asset_aliases=asset_aliases
        self.session_utils=session_utils
        self.cache={}

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
        df=self._read_csv(self.source_config[key]["path"])
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
        self.cache[key]=df
        return df

    def _resolve_assets(self,q):
        nq=_nrm(q); hits=[]
        for k,aliases in self.asset_aliases.items():
            if any(_nrm(a) in nq for a in aliases):
                hits.append(k)
        return sorted(set(hits))

    def _pick_best_asset(self,q,assets):
        nq=_nrm(q)
        if any(x in nq for x in ["30m","30 min","30min"]):
            v=[a for a in assets if a.endswith("_30m")]
            if v: return v[0]
        if any(x in nq for x in ["5m","5 min","5min"]):
            v=[a for a in assets if a.endswith("_5m")]
            if v: return v[0]
        v=[a for a in assets if a.endswith("_daily")]
        if v: return v[0]
        base=[a for a in assets if not re.search(r"_(daily|30m|5m|1m|1h|4h)$",a)]
        return base[0] if base else assets[0]

    def can_handle(self,q):
        nq=_nrm(q)
        discover_words=[
            "discovery automatique","auto discovery","decouvre","découvre",
            "trouve les meilleurs patterns","meilleurs patterns",
            "meilleurs signaux","best patterns","best signals",
            "signal miner","pattern miner","discover patterns","discover signals"
        ]
        assets=self._resolve_assets(q)
        return bool(assets) and any(x in nq for x in discover_words)

    def _dailyize(self,asset_key):
        df=self._load(asset_key)
        kind=self.source_config[asset_key].get("kind","ohlc")
        fm=self.session_utils.first_match
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
            out["green_candle"]=out["close"]>out["open"]
            out["red_candle"]=out["close"]<out["open"]
            out["body_pct"]=(out["close"]-out["open"]).abs()/out["open"].replace(0,np.nan)
            if "high" in out.columns and "low" in out.columns:
                out["range_pct"]=(out["high"]-out["low"])/out["open"].replace(0,np.nan)
        out["next_day_ret"]=pd.to_numeric(out["ret"].shift(-1),errors="coerce") if "ret" in out.columns else np.nan
        return out

    def _bool_shift(self, s, n):
        return s.shift(n).fillna(False).infer_objects(copy=False).astype(bool)

    def _candidate_masks(self,df):
        cands={}

        red=df["red_candle"].fillna(False).infer_objects(copy=False).astype(bool) if "red_candle" in df.columns else pd.Series(False,index=df.index,dtype=bool)
        green=df["green_candle"].fillna(False).infer_objects(copy=False).astype(bool) if "green_candle" in df.columns else pd.Series(False,index=df.index,dtype=bool)

        cands["1_green"]=green
        cands["1_red"]=red
        cands["2_green"]=green & self._bool_shift(green,1)
        cands["2_red"]=red & self._bool_shift(red,1)
        cands["3_green"]=green & self._bool_shift(green,1) & self._bool_shift(green,2)
        cands["3_red"]=red & self._bool_shift(red,1) & self._bool_shift(red,2)

        if "ret" in df.columns:
            cands["ret_gt_1pct"]=pd.to_numeric(df["ret"],errors="coerce")>0.01
            cands["ret_lt_m1pct"]=pd.to_numeric(df["ret"],errors="coerce")<-0.01
            cands["ret_abs_gt_1pct"]=pd.to_numeric(df["ret_abs"],errors="coerce")>0.01

        if "body_pct" in df.columns:
            cands["body_gt_1pct"]=pd.to_numeric(df["body_pct"],errors="coerce")>0.01

        if "range_pct" in df.columns:
            cands["range_gt_2pct"]=pd.to_numeric(df["range_pct"],errors="coerce")>0.02

        cands["red_red_green"]=self._bool_shift(red,2) & self._bool_shift(red,1) & green
        cands["green_green_red"]=self._bool_shift(green,2) & self._bool_shift(green,1) & red
        cands["red_green"]=self._bool_shift(red,1) & green
        cands["green_red"]=self._bool_shift(green,1) & red

        return cands

    def _score_row(self, sample, mean_next, hit_rate, baseline_hit):
        if sample is None or sample <= 0 or mean_next is None or pd.isna(mean_next):
            return None
        edge_hit=(hit_rate-baseline_hit) if (hit_rate is not None and baseline_hit is not None and not pd.isna(hit_rate) and not pd.isna(baseline_hit)) else 0.0
        return float(abs(mean_next)*10000.0*np.log1p(sample) + max(edge_hit,0)*100.0)

    def run(self,q,preview_rows=20):
        assets=self._resolve_assets(q)
        if not assets:
            return {"status":"NO_ASSET_RECOGNIZED","answer_type":"explanation"}

        asset=self._pick_best_asset(q,assets)
        df=self._dailyize(asset).copy()
        if "next_day_ret" not in df.columns:
            return {"status":"NO_NEXT_DAY_RETURN_AVAILABLE","answer_type":"explanation","asset":asset}

        years=sorted({int(x) for x in re.findall(r"\b(20\d{2})\b",q)})
        if years:
            df=df[df["year"].isin(years)].copy()

        df=df[df["next_day_ret"].notna()].copy()
        if df.empty:
            return {"status":"OK_NO_MATCHES","answer_type":"table","asset":asset,"value":0,"preview":[]}

        baseline_hit=float((pd.to_numeric(df["next_day_ret"],errors="coerce")>0).mean())
        cands=self._candidate_masks(df)

        rows=[]
        for name,mask in cands.items():
            mask=mask.fillna(False).infer_objects(copy=False).astype(bool)
            sub=df[mask].copy()
            n=int(len(sub))
            if n<3:
                continue
            mean_next=float(pd.to_numeric(sub["next_day_ret"],errors="coerce").mean())
            med_next=float(pd.to_numeric(sub["next_day_ret"],errors="coerce").median())
            hit=float((pd.to_numeric(sub["next_day_ret"],errors="coerce")>0).mean())
            edge_vs_baseline=float(hit-baseline_hit)
            score=self._score_row(n,mean_next,hit,baseline_hit)
            rows.append({
                "pattern":name,
                "sample_size":n,
                "mean_next_day_ret":mean_next,
                "median_next_day_ret":med_next,
                "hit_rate":hit,
                "baseline_hit_rate":baseline_hit,
                "edge_vs_baseline":edge_vs_baseline,
                "score":score
            })

        if not rows:
            return {"status":"OK_NO_MATCHES","answer_type":"table","asset":asset,"value":0,"preview":[]}

        out=pd.DataFrame(rows).sort_values(["score","sample_size"],ascending=[False,False]).reset_index(drop=True)
        preview=out.head(preview_rows).to_dict("records")

        return {
            "status":"OK",
            "answer_type":"table",
            "asset":asset,
            "discovery_mode":"conservative_exante_closed_bar_only",
            "candidate_count":int(len(out)),
            "value":int(len(out)),
            "preview":preview
        }
