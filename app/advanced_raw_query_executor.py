import os,re,unicodedata,itertools,pandas as pd,numpy as np

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

class AdvancedRawQueryExecutor:
    def __init__(self,source_root,source_config,asset_aliases,session_utils_module):
        self.source_root=source_root
        self.source_config=source_config
        self.asset_aliases=asset_aliases
        self.su=session_utils_module
        self.cache={}

    def _find_assets(self,text):
        nt=_nrm(text); out=[]
        for asset,aliases in self.asset_aliases.items():
            if any(_nrm(a) in nt for a in aliases):
                out.append(asset)
        return out

    def _canonical_pair_from_text(self,text):
        nt=_nrm(text)

        explicit_pairs = [
            ("spy_premarket","spx",[r"spy\s*(?:pre[- ]?market|premarket|pré[- ]?market)", r"\bspx\b"]),
            ("us10y","oil",[r"(?:us bonds|bond us|us bond|us bonds|taux 10 ans|us10y|10 year yield|treasury 10y)", r"(?:oil|petrole|pétrole|crude|wti)"]),
            ("us10y","gold",[r"(?:us bonds|bond us|us bond|us bonds|taux 10 ans|us10y|10 year yield|treasury 10y)", r"(?:gold|or|xau)"]),
            ("dxy","oil",[r"(?:dxy|dollar|dollar index|usd index|indice dollar)", r"(?:oil|petrole|pétrole|crude|wti)"]),
            ("dxy","gold",[r"(?:dxy|dollar|dollar index|usd index|indice dollar)", r"(?:gold|or|xau)"]),
            ("spy","spx",[r"\bspy\b", r"\bspx\b"]),
            ("qqq","spx",[r"\bqqq\b", r"\bspx\b"]),
            ("iwm","spx",[r"\biwm\b", r"\bspx\b"]),
        ]
        for a,b,pats in explicit_pairs:
            if re.search(pats[0],nt) and re.search(pats[1],nt):
                return a,b

        assets=self._find_assets(text)
        if len(assets)>=2:
            return assets[0],assets[1]
        return None,None

    def _is_trivial_pair(self,a,b):
        trivial_groups = [
            {"spx","spx_30m","spx_daily","spx_5d_avg_range","spx_20d_avg_range"},
            {"spy","spy_daily","spy_premarket"},
            {"qqq","qqq_30m","qqq_daily"},
            {"iwm","iwm_daily"},
            {"gold","gold_daily"},
        ]
        for g in trivial_groups:
            if a in g and b in g:
                return True
        return False

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

                best_sep=None
                best_cols=1
                for sep in candidates:
                    n=len([x for x in header_line.split(sep)])
                    if n>best_cols:
                        best_cols=n
                        best_sep=sep

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
        cfg=self.source_config[key]
        df=self._read_csv(cfg["path"])
        tcol=self.su.detect_time_col(df)
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
        df=self.su.add_calendar_buckets(df)
        self.cache[key]=(df,cfg)
        return self.cache[key]

    def _ohlc_cols(self,df):
        return {
            "open":self.su.first_match(df.columns,["open"]),
            "high":self.su.first_match(df.columns,["high"]),
            "low":self.su.first_match(df.columns,["low"]),
            "close":self.su.first_match(df.columns,["close"]),
        }

    def _value_col(self,df,preferred=None):
        cols=list(df.columns)
        if preferred and preferred in cols:
            return preferred
        preferred_keys=["value","spread_10y_minus_2y","spx_iwm_correlation_20d","spx_qqq_correlation_20d","close","open","high","low","us_2y"]
        for cand in preferred_keys:
            c=self.su.first_match(cols,[cand])
            if c:
                return c
        bad={"date_key","time_hhmm","month_name","weekday_name","__time__","month_num","weekday_num"}
        for c in cols:
            if c in bad:
                continue
            try:
                if pd.to_numeric(df[c],errors="coerce").notna().sum()>0:
                    return c
            except:
                pass
        return None

    def _infer_regular_bounds(self,df):
        hh=df["time_hhmm"].astype(str)
        candidates_open=hh[(hh>="09:00")&(hh<="10:30")]
        open_h=candidates_open.min() if len(candidates_open) else hh.min()
        candidates_close=hh[(hh>="15:00")&(hh<="17:00")]
        close_h=candidates_close.max() if len(candidates_close) else hh.max()
        return open_h,close_h

    def _attach_session_date(self,df):
        out=df.copy()
        open_h,close_h=self._infer_regular_bounds(out)
        d=pd.to_datetime(out["date_key"])
        t=out["time_hhmm"]
        session_date=np.where(t>close_h,(d+pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d"),d.dt.strftime("%Y-%m-%d"))
        out["session_date"]=session_date
        out["regular_open_hhmm"]=open_h
        out["regular_close_hhmm"]=close_h
        out["is_regular_session"]=(t>=open_h)&(t<=close_h)
        out["is_premarket_for_session"]=(out["session_date"]==out["date_key"])&(t<open_h)
        out["is_afterhours_for_next_session"]=(t>close_h)
        return out

    def _daily_summary(self,key):
        df,_=self._load(key)
        ohlc=self._ohlc_cols(df)
        agg={}; rename={}
        if ohlc["open"]:
            agg[ohlc["open"]]="first"; rename[ohlc["open"]]=f"{key}_open"
        if ohlc["high"]:
            agg[ohlc["high"]]="max"; rename[ohlc["high"]]=f"{key}_high"
        if ohlc["low"]:
            agg[ohlc["low"]]="min"; rename[ohlc["low"]]=f"{key}_low"
        if ohlc["close"]:
            agg[ohlc["close"]]="last"; rename[ohlc["close"]]=f"{key}_close"
        if not agg:
            vcol=self._value_col(df)
            if vcol:
                agg[vcol]="last"; rename[vcol]=f"{key}_value"
        dd=df.groupby("date_key",as_index=False).agg(agg).rename(columns=rename)
        return dd

    def _parse_threshold(self,text):
        nt=_nrm(text)
        m_pct=re.search(r"(?:au dessus de|above|over|greater than|hausse de|up by|ouvre a|ouvre à|opens at|open at)\s*(-?\d+(?:[\.,]\d+)?)\s*%",nt)
        if m_pct:
            return "pct",float(m_pct.group(1).replace(",", "."))/100.0
        m_abs=re.search(r"(?:au dessus de|above|over|greater than|>|a|à|at)\s*(-?\d+(?:[\.,]\d+)?)",nt)
        if m_abs:
            return "abs",float(m_abs.group(1).replace(",", "."))
        return None,None

    def _parse_years(self,text):
        yrs=re.findall(r"\b(20\d{2})\b",str(text))
        return sorted({int(y) for y in yrs})

    def _parse_month_window(self,text):
        nt=_nrm(text)
        m=re.search(r"(\d+)\s*derniers?\s*mois",nt)
        if m:
            return int(m.group(1))
        m=re.search(r"last\s*(\d+)\s*months?",nt)
        if m:
            return int(m.group(1))
        return None

    def _mean_word(self,text):
        nt=_nrm(text)
        return any(w in nt for w in ["moyenne","average","mean","avg","en général","en general","general"])

    def _series_daily_return(self,key):
        df,_=self._load(key)
        ohlc=self._ohlc_cols(df)
        if ohlc["close"]:
            daily=df.groupby("date_key",as_index=False).agg(value=(ohlc["close"],"last"))
        else:
            vcol=self._value_col(df)
            if vcol is None:
                return None
            daily=df.groupby("date_key",as_index=False).agg(value=(vcol,"last"))
        daily["value"]=pd.to_numeric(daily["value"],errors="coerce")
        daily["ret"]=daily["value"].pct_change()
        return daily[["date_key","ret","value"]]

    def _series_spy_true_premarket_return(self):
        df,_=self._load("spy")
        df=self._attach_session_date(df)
        ohlc=self._ohlc_cols(df)
        pm=df[df["is_premarket_for_session"]].copy()
        if pm.empty:
            return pd.DataFrame(columns=["date_key","ret","value"])
        g=pm.groupby("session_date",as_index=False).agg(pm_open=(ohlc["open"],"first"),pm_close=(ohlc["close"],"last")).rename(columns={"session_date":"date_key"})
        g["pm_open"]=pd.to_numeric(g["pm_open"],errors="coerce")
        g["pm_close"]=pd.to_numeric(g["pm_close"],errors="coerce")
        g["ret"]=(g["pm_close"]-g["pm_open"])/g["pm_open"].replace(0,np.nan)
        g["value"]=g["pm_close"]
        return g[["date_key","ret","value"]]

    def _build_global_correlation_catalog(self,months=7,exclude_trivial=True):
        series={}
        for key,cfg in self.source_config.items():
            try:
                if key=="spy":
                    s=self._series_spy_true_premarket_return()
                    if not s.empty:
                        series["spy_premarket"]=s[["date_key","ret"]].rename(columns={"ret":"spy_premarket"})
                s=self._series_daily_return(key)
                if s is None or s.empty or "ret" not in s.columns:
                    continue
                s=s[["date_key","ret"]].rename(columns={"ret":key})
                series[key]=s
            except Exception:
                continue

        keys=sorted(series.keys())
        rows=[]
        for a,b in itertools.combinations(keys,2):
            try:
                if exclude_trivial and self._is_trivial_pair(a,b):
                    continue
                m=series[a].merge(series[b],on="date_key",how="inner")
                if m.empty:
                    continue
                m["date"]=pd.to_datetime(m["date_key"],errors="coerce")
                max_date=m["date"].max()
                if pd.notna(max_date):
                    cutoff=max_date-pd.DateOffset(months=months)
                    m=m[m["date"]>=cutoff].copy()
                if len(m)<20:
                    continue
                corr=float(pd.to_numeric(m[a],errors="coerce").corr(pd.to_numeric(m[b],errors="coerce")))
                rows.append({
                    "asset_a":a,
                    "asset_b":b,
                    "corr":corr,
                    "sample_size":int(len(m)),
                    "date_from":None if m["date"].isna().all() else str(m["date"].min().date()),
                    "date_to":None if m["date"].isna().all() else str(m["date"].max().date())
                })
            except Exception:
                continue

        out=pd.DataFrame(rows)
        if len(out):
            out["abs_corr"]=out["corr"].abs()
            out=out.sort_values(["abs_corr","sample_size"],ascending=[False,False]).reset_index(drop=True)
        return out

    def _query_global_correlation(self,text,preview_rows=20):
        months=self._parse_month_window(text) or 7
        nt=_nrm(text)
        cat=self._build_global_correlation_catalog(months=months,exclude_trivial=True)
        if cat.empty:
            return {"status":"NO_CORRELATION_CATALOG","answer_type":"table","preview":[],"table":[],"sample_size":0}

        a,b=self._canonical_pair_from_text(text)
        if a is not None and b is not None:
            q=cat[((cat["asset_a"]==a)&(cat["asset_b"]==b))|((cat["asset_a"]==b)&(cat["asset_b"]==a))].copy()
            q=q.sort_values("abs_corr",ascending=False).reset_index(drop=True)
            return {
                "status":"OK",
                "answer_type":"table",
                "metric_kind":"pair_correlation",
                "value":None if q.empty else float(q.iloc[0]["corr"]),
                "sample_size":int(len(q)),
                "preview":q.head(preview_rows).to_dict("records"),
                "table":q.to_dict("records"),
                "best_pair":None if q.empty else q.iloc[0].to_dict(),
                "pair_requested":[a,b]
            }

        if "plus correl" in nt or "most correlated" in nt or "les plus corr" in nt:
            q=cat.sort_values("corr",ascending=False).reset_index(drop=True)
        elif "inverse" in nt or "negative" in nt or "négative" in nt or "moins corr" in nt:
            q=cat.sort_values("corr",ascending=True).reset_index(drop=True)
        else:
            q=cat.sort_values("abs_corr",ascending=False).reset_index(drop=True)

        return {
            "status":"OK",
            "answer_type":"table",
            "metric_kind":"global_correlation_catalog",
            "value":None if q.empty else float(q.iloc[0]["corr"]),
            "sample_size":int(len(q)),
            "preview":q.head(preview_rows).to_dict("records"),
            "table":q.to_dict("records"),
            "best_pair":None if q.empty else q.iloc[0].to_dict()
        }

    def _query_yield_curve_max_week(self,text,preview_rows=10):
        df,_=self._load("yield_curve_spread")
        vcol=self._value_col(df,"spread_10y_minus_2y")
        if vcol is None:
            return {"status":"VALUE_COL_NOT_FOUND","preview":[]}
        df[vcol]=pd.to_numeric(df[vcol],errors="coerce")
        yrs=self._parse_years(text)
        if yrs:
            df=df[df["__time__"].dt.year.isin(yrs)].copy()
        iso=df["__time__"].dt.isocalendar()
        df["iso_year"]=iso.year.astype(int)
        df["iso_week"]=iso.week.astype(int)
        g=df.groupby(["iso_year","iso_week"],as_index=False).agg(mean_value=(vcol,"mean"),min_value=(vcol,"min"),max_value=(vcol,"max"),sample_size=(vcol,"size"))
        g=g.sort_values(["mean_value","max_value"],ascending=False).reset_index(drop=True)
        top=g.head(1)
        return {"status":"OK","answer_type":"table","metric_kind":"yield_curve_week_rank","value":None if top.empty else float(top.iloc[0]["mean_value"]),"sample_size":int(len(g)),"preview":g.head(preview_rows).to_dict("records"),"table":g.to_dict("records"),"best_week":None if top.empty else top.iloc[0].to_dict()}

    def _query_yield_curve_lowest_weekday_by_year(self,text,preview_rows=20):
        df,_=self._load("yield_curve_spread")
        vcol=self._value_col(df,"spread_10y_minus_2y")
        if vcol is None:
            return {"status":"VALUE_COL_NOT_FOUND","preview":[]}
        df[vcol]=pd.to_numeric(df[vcol],errors="coerce")
        yrs=self._parse_years(text)
        if yrs:
            df=df[df["__time__"].dt.year.isin(yrs)].copy()
        df["year"]=df["__time__"].dt.year
        g=df.groupby(["year","weekday_num","weekday_name"],as_index=False).agg(mean_value=(vcol,"mean"),min_value=(vcol,"min"),sample_size=(vcol,"size"))
        best=g.sort_values(["year","mean_value"],ascending=[True,True]).groupby("year",as_index=False).head(1).sort_values("year")
        return {"status":"OK","answer_type":"table","metric_kind":"yield_curve_lowest_weekday_by_year","value":None,"sample_size":int(len(g)),"preview":best.head(preview_rows).to_dict("records"),"table":best.to_dict("records")}

    def _query_gold_oil_corr_recent(self,text,preview_rows=10):
        months=self._parse_month_window(text) or 7
        gold=self._series_daily_return("gold_daily")
        oil=self._series_daily_return("oil")
        if gold is None or oil is None:
            return {"status":"SERIES_BUILD_FAILED","preview":[]}
        m=gold.merge(oil,on="date_key",how="inner",suffixes=("_gold","_oil"))
        m["date"]=pd.to_datetime(m["date_key"],errors="coerce")
        m=m.sort_values("date").reset_index(drop=True)
        max_date=m["date"].max()
        if pd.notna(max_date):
            cutoff=max_date-pd.DateOffset(months=months)
            m=m[m["date"]>=cutoff].copy()
        m["corr_20d"]=m["ret_gold"].rolling(20).corr(m["ret_oil"])
        out=m.dropna(subset=["corr_20d"]).sort_values("corr_20d",ascending=False).reset_index(drop=True)
        return {"status":"OK","answer_type":"table","metric_kind":"gold_oil_rolling_corr_20d","value":None if out.empty else float(out.iloc[0]["corr_20d"]),"sample_size":int(len(out)),"preview":out[["date_key","corr_20d","ret_gold","ret_oil"]].head(preview_rows).to_dict("records"),"table":out[["date_key","corr_20d","ret_gold","ret_oil"]].to_dict("records"),"best_date":None if out.empty else str(out.iloc[0]["date_key"])}

    def _query_simple_value(self,text,preview_rows=10):
        assets=self._find_assets(text)
        if len(assets)<2:
            return {"status":"INSUFFICIENT_ASSET_RESOLUTION","preview":[]}
        cond_asset=assets[0]
        target_asset=assets[-1]
        kind,thr=self._parse_threshold(text)
        cond,_=self._load(cond_asset)
        tgt,_=self._load(target_asset)
        cond_col=self._value_col(cond) or self._ohlc_cols(cond)["close"]
        tgt_col=self._value_col(tgt) or self._ohlc_cols(tgt)["close"]
        cond_g=cond.groupby("date_key",as_index=False).agg(cond_value=(cond_col,"last"))
        tgt_g=tgt.groupby("date_key",as_index=False).agg(metric_value=(tgt_col,"last"))
        cond_g["cond_value"]=pd.to_numeric(cond_g["cond_value"],errors="coerce")
        tgt_g["metric_value"]=pd.to_numeric(tgt_g["metric_value"],errors="coerce")
        if kind=="pct":
            cond_g["cond_signal"]=cond_g["cond_value"].pct_change()
            cond_g=cond_g[cond_g["cond_signal"]>=thr].copy()
        elif thr is not None:
            cond_g=cond_g[cond_g["cond_value"]>=thr].copy()
        out=cond_g.merge(tgt_g,on="date_key",how="inner")
        return {"status":"OK","answer_type":"mean","source_asset":cond_asset,"target_asset":target_asset,"metric_kind":"value","value":None if out.empty else float(out["metric_value"].mean()),"sample_size":int(len(out)),"preview":out.head(preview_rows).to_dict("records")}

    def execute(self,text,preview_rows=10):
        nt=_nrm(text)
        if ("corrél" in nt or "correl" in nt or "correlation" in nt) and ("tous les actifs" in nt or "all assets" in nt or "plus corr" in nt or "most correlated" in nt or len(self._find_assets(text))>=2 or "pré-market" in nt or "premarket" in nt):
            return self._query_global_correlation(text,preview_rows)
        if ("yield curve" in nt or "courbe des taux" in nt or "spread 10y 2y" in nt) and ("semaine" in nt or "week" in nt) and ("plus important" in nt or "highest" in nt or "largest" in nt):
            return self._query_yield_curve_max_week(text,preview_rows)
        if ("yield curve" in nt or "courbe des taux" in nt or "spread 10y 2y" in nt) and ("jours de la semaine" in nt or "day of the week" in nt or "weekday" in nt) and ("plus bas" in nt or "lowest" in nt):
            return self._query_yield_curve_lowest_weekday_by_year(text,preview_rows)
        if ("or" in nt or "gold" in nt) and ("pétrole" in nt or "petrole" in nt or "oil" in nt) and ("corrél" in nt or "correl" in nt):
            return self._query_gold_oil_corr_recent(text,preview_rows)
        return self._query_simple_value(text,preview_rows)
