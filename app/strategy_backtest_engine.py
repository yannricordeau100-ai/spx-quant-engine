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

class StrategyBacktestEngine:
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
        v=[a for a in assets if a.endswith("_daily")]
        if v: return v[0]
        base=[a for a in assets if not re.search(r"_(daily|30m|5m|1m|1h|4h)$",a)]
        return base[0] if base else assets[0]

    def can_handle(self,q):
        nq=_nrm(q)
        assets=self._resolve_assets(q)
        keys=[
            "backtest","teste la strategie","teste la stratégie","strategy test",
            "si j'achete","si j'achète","if i buy","if we buy",
            "apres 2 bougies rouges","après 2 bougies rouges",
            "apres 2 bougies vertes","après 2 bougies vertes",
            "next day strategy","trade setup"
        ]
        return bool(assets) and any(k in nq for k in keys)

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
            out["green_candle"]=out["close"]>out["open"]
            out["red_candle"]=out["close"]<out["open"]
            out["next_day_ret"]=pd.to_numeric(out["ret"].shift(-1),errors="coerce")
        return out

    def _bool_shift(self, s, n):
        return s.shift(n).fillna(False).infer_objects(copy=False).astype(bool)

    def _signal_mask(self,df,q):
        nq=_nrm(q)
        red=df["red_candle"].fillna(False).infer_objects(copy=False).astype(bool) if "red_candle" in df.columns else pd.Series(False,index=df.index,dtype=bool)
        green=df["green_candle"].fillna(False).infer_objects(copy=False).astype(bool) if "green_candle" in df.columns else pd.Series(False,index=df.index,dtype=bool)

        if any(x in nq for x in ["2 bougies rouges","deux bougies rouges","two red candles","apres 2 bougies rouges","après 2 bougies rouges"]):
            return red & self._bool_shift(red,1), "after_2_red"
        if any(x in nq for x in ["2 bougies vertes","deux bougies vertes","two green candles","apres 2 bougies vertes","après 2 bougies vertes"]):
            return green & self._bool_shift(green,1), "after_2_green"
        if "bougie rouge" in nq or "red candle" in nq:
            return red, "after_1_red"
        if "bougie verte" in nq or "green candle" in nq:
            return green, "after_1_green"
        return pd.Series(False,index=df.index,dtype=bool), "unknown_signal"

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

        mask, signal_name = self._signal_mask(df,q)
        trades=df[mask].copy()
        trades=trades[trades["next_day_ret"].notna()].copy()

        if trades.empty:
            return {
                "status":"OK_NO_MATCHES",
                "answer_type":"table",
                "asset":asset,
                "strategy_signal":signal_name,
                "value":0,
                "preview":[]
            }

        s=pd.to_numeric(trades["next_day_ret"],errors="coerce").dropna()
        equity=(1+s).cumprod()
        preview_cols=[c for c in ["date_key","ret","next_day_ret"] if c in trades.columns]

        result={
            "status":"OK",
            "answer_type":"table",
            "asset":asset,
            "strategy_signal":signal_name,
            "trade_count":int(len(s)),
            "mean_trade_ret":float(s.mean()),
            "median_trade_ret":float(s.median()),
            "hit_rate":float((s>0).mean()),
            "cumulative_return_proxy":float(equity.iloc[-1]-1.0) if len(equity)>0 else None,
            "value":int(len(s)),
            "preview":trades[preview_cols].head(preview_rows).to_dict("records")
        }
        return result
