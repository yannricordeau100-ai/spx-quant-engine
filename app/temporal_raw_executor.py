import os,re,unicodedata,pandas as pd,numpy as np

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

class TemporalRawExecutor:
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

    def _load_asset_df(self,asset):
        if asset in self.cache:
            return self.cache[asset]
        cfg=self.source_config[asset]
        df=self._read_csv(cfg["path"])
        tcol=self.su.detect_time_col(df)
        if tcol is None:
            raise RuntimeError(f"{asset}_TIME_NOT_FOUND")
        df[tcol]=pd.to_datetime(df[tcol],errors="coerce")
        df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
        df["__time__"]=df[tcol]
        df["date_key"]=df["__time__"].dt.strftime("%Y-%m-%d")
        df["time_hhmm"]=df["__time__"].dt.strftime("%H:%M")
        df=self.su.add_calendar_buckets(df)
        self.cache[asset]=(df,cfg)
        return self.cache[asset]

    def _ohlc_cols(self,df):
        return {
            "open":self.su.first_match(df.columns,["open"]),
            "high":self.su.first_match(df.columns,["high"]),
            "low":self.su.first_match(df.columns,["low"]),
            "close":self.su.first_match(df.columns,["close"]),
        }

    def _parse_between(self,text):
        nt=_nrm(text)
        pats=[
            r"entre\s+(\d{1,2})(?:h|:)?(\d{0,2})?\s+et\s+(\d{1,2})(?:h|:)?(\d{0,2})?",
            r"between\s+(\d{1,2})(?::(\d{2}))?\s+(?:and|to)\s+(\d{1,2})(?::(\d{2}))?"
        ]
        for pat in pats:
            m=re.search(pat,nt)
            if m:
                g=m.groups()
                h1=int(g[0]); m1=int(g[1] or 0); h2=int(g[2]); m2=int(g[3] or 0)
                return f"{h1:02d}:{m1:02d}",f"{h2:02d}:{m2:02d}"
        return None,None

    def _parse_at(self,text):
        nt=_nrm(text)
        m=re.search(r"(?:a|à|at)\s+(\d{1,2})(?:h|:(\d{2}))",nt)
        if m:
            return f"{int(m.group(1)):02d}:{int(m.group(2) or 0):02d}"
        m=re.search(r"(\d{1,2})\s*am",nt)
        if m:
            h=int(m.group(1)); h=0 if h==12 else h
            return f"{h:02d}:00"
        m=re.search(r"(\d{1,2})\s*pm",nt)
        if m:
            h=int(m.group(1)); h=12 if h==12 else h+12
            return f"{h:02d}:00"
        return None

    def _parse_threshold(self,text):
        nt=_nrm(text)
        m_pct=re.search(r"(?:au dessus de|above|over|greater than|hausse de|up by|ouvre a|ouvre à|opens at|open at)\s*(-?\d+(?:[\.,]\d+)?)\s*%",nt)
        if m_pct:
            return "pct",float(m_pct.group(1).replace(",", "."))/100.0
        m_abs=re.search(r"(?:au dessus de|above|over|greater than|>|a|à|at)\s*(-?\d+(?:[\.,]\d+)?)",nt)
        if m_abs:
            return "abs",float(m_abs.group(1).replace(",", "."))
        return None,None

    def _mean_word(self,text):
        nt=_nrm(text)
        return any(w in nt for w in ["moyenne","average","mean","avg","en general","en général","general"])

    def _show_dates(self,text):
        nt=_nrm(text)
        return any(w in nt for w in ["montre","show","dates","quelles dates","which dates","liste","list"])

    def _is_previous_day_range_case(self,text):
        nt=_nrm(text)
        return ("premarket" in nt or "pre-market" in nt or "pre market" in nt or "pré-market" in nt) and ("veille" in nt or "previous day" in nt or "yesterday") and ("range" in nt or "ecart" in nt or "écart" in nt)

    def _is_next_day_case(self,text):
        nt=_nrm(text)
        return any(x in nt for x in ["lendemain","next day","following day","jour suivant"])

    def _is_month_aggregation_case(self,text):
        nt=_nrm(text)
        month_markers=["chacun des mois","chaque mois","par mois","each month","by month","months of the year","mois de l'annee","mois de l’année"]
        open_bucket_markers=["30 premieres min","30 premières min","30 premieres minutes","30 premières minutes","first 30 min","first 30 minutes","30 min d'ouverture","30 min d’ouverture","apres l'ouverture","après l’ouverture","opening 30 min"]
        return any(m in nt for m in month_markers) and any(m in nt for m in open_bucket_markers)

    def _target_metric_kind(self,text):
        nt=_nrm(text)
        if any(w in nt for w in ["performance","return","rendement","variation","perf"]):
            return "return"
        if any(w in nt for w in ["ouvre","open","opening","ouverture"]):
            return "open"
        return "close"

    def _daily_summary(self,asset):
        df,_=self._load_asset_df(asset)
        ohlc=self._ohlc_cols(df)
        agg={}
        if ohlc["open"]: agg[ohlc["open"]]="first"
        if ohlc["high"]: agg[ohlc["high"]]="max"
        if ohlc["low"]: agg[ohlc["low"]]="min"
        if ohlc["close"]: agg[ohlc["close"]]="last"
        rename={}
        if ohlc["open"]: rename[ohlc["open"]]=f"{asset}_open"
        if ohlc["high"]: rename[ohlc["high"]]=f"{asset}_high"
        if ohlc["low"]: rename[ohlc["low"]]=f"{asset}_low"
        if ohlc["close"]: rename[ohlc["close"]]=f"{asset}_close"
        dd=df.groupby("date_key",as_index=False).agg({k:v for k,v in agg.items()}).rename(columns=rename)
        return dd

    def _query_open_bucket_by_month(self,text,preview_rows=12):
        assets=self._find_assets(text)
        asset=assets[0] if assets else "spx"
        df,_=self._load_asset_df(asset)
        ohlc=self._ohlc_cols(df)
        if not ohlc["open"] or not ohlc["close"]:
            return {"status":"OHLC_NOT_AVAILABLE","message":"Colonnes open/close requises.","preview":[]}

        bounds=self.su.build_session_bounds(df)
        work=df.merge(bounds[["date_key","session_open_dt"]],on="date_key",how="left")
        work["mins_from_open"]=(work["__time__"]-work["session_open_dt"]).dt.total_seconds()/60.0
        bucket=work[(work["mins_from_open"]>=0)&(work["mins_from_open"]<30)].copy()

        per_day=bucket.groupby(["date_key","month_num","month_name"],as_index=False).agg(
            bucket_open=(ohlc["open"],"first"),
            bucket_close=(ohlc["close"],"last"),
            bucket_rows=("__time__","size"),
        )
        per_day["signed_perf_pct"]=(pd.to_numeric(per_day["bucket_close"],errors="coerce")-pd.to_numeric(per_day["bucket_open"],errors="coerce"))/pd.to_numeric(per_day["bucket_open"],errors="coerce").replace(0,np.nan)
        per_day["abs_perf_pct"]=per_day["signed_perf_pct"].abs()

        by_month=per_day.groupby(["month_num","month_name"],as_index=False).agg(
            mean_abs_perf_pct=("abs_perf_pct","mean"),
            mean_signed_perf_pct=("signed_perf_pct","mean"),
            sample_size=("date_key","size"),
        ).sort_values("month_num")
        preview=by_month.head(preview_rows).to_dict("records")
        return {
            "status":"OK",
            "answer_type":"table",
            "source_asset":asset,
            "condition_expr":"first_30_minutes_after_session_open grouped by month",
            "metric_kind":"abs_pct_by_month",
            "value":None,
            "sample_size":int(len(per_day)),
            "preview":preview,
            "table":by_month.to_dict("records"),
        }

    def _query_intraday_window(self,text,preview_rows=10):
        assets=self._find_assets(text)
        if len(assets)<2:
            return {"status":"INSUFFICIENT_ASSET_RESOLUTION","message":"Actifs insuffisants pour exécuter la fenêtre intraday.","preview":[]}
        metric_asset=assets[0]
        cond_asset=assets[1]

        start,end=self._parse_between(text)
        if not start or not end:
            return {"status":"WINDOW_PARSE_FAILED","message":"Fenêtre intraday non comprise.","preview":[]}

        kind,thr=self._parse_threshold(text)
        metric_kind=self._target_metric_kind(text)

        metric_df,_=self._load_asset_df(metric_asset)
        cond_df,_=self._load_asset_df(cond_asset)
        cond_ohlc=self._ohlc_cols(cond_df)
        metric_ohlc=self._ohlc_cols(metric_df)

        cond_daily=cond_df.groupby("date_key",as_index=False).agg({
            cond_ohlc["open"]:"first",
            cond_ohlc["close"]:"last"
        }).rename(columns={cond_ohlc["open"]:"cond_open",cond_ohlc["close"]:"cond_close"})
        if kind=="pct":
            cond_daily["cond_signal"]=(pd.to_numeric(cond_daily["cond_close"],errors="coerce")-pd.to_numeric(cond_daily["cond_open"],errors="coerce"))/pd.to_numeric(cond_daily["cond_open"],errors="coerce").replace(0,np.nan)
            cond_daily=cond_daily[cond_daily["cond_signal"]>=thr].copy()
            cond_expr=f"{cond_asset} daily_open_to_close_ret >= {thr}"
        else:
            cond_daily["cond_signal"]=pd.to_numeric(cond_daily["cond_open"],errors="coerce")
            cond_daily=cond_daily[cond_daily["cond_signal"]>=thr].copy()
            cond_expr=f"{cond_asset} open >= {thr}"

        win=metric_df[(metric_df["time_hhmm"]>=start)&(metric_df["time_hhmm"]<=end)].copy()
        if metric_kind=="return" and metric_ohlc["open"] and metric_ohlc["close"]:
            win["metric_value"]=(pd.to_numeric(win[metric_ohlc["close"]],errors="coerce")-pd.to_numeric(win[metric_ohlc["open"]],errors="coerce"))/pd.to_numeric(win[metric_ohlc["open"]],errors="coerce").replace(0,np.nan)
        elif metric_kind=="open" and metric_ohlc["open"]:
            win["metric_value"]=pd.to_numeric(win[metric_ohlc["open"]],errors="coerce")
        else:
            win["metric_value"]=pd.to_numeric(win[metric_ohlc["close"]],errors="coerce")
        metric_daily=win.groupby("date_key",as_index=False).agg(metric_value=("metric_value","mean"))

        merged=metric_daily.merge(cond_daily[["date_key","cond_signal"]],on="date_key",how="inner")
        preview=merged.head(preview_rows).to_dict("records")
        return {
            "status":"OK",
            "answer_type":"mean" if self._mean_word(text) else "rows",
            "source_asset":metric_asset,
            "condition_asset":cond_asset,
            "time_window":{"start":start,"end":end},
            "condition_expr":cond_expr,
            "metric_kind":metric_kind,
            "value":None if merged.empty else float(merged["metric_value"].mean()),
            "sample_size":int(len(merged)),
            "preview":preview,
        }

    def _query_premarket_previous_day(self,text,preview_rows=10):
        assets=self._find_assets(text)
        if "spy" not in assets or "spx" not in assets:
            return {"status":"ASSET_RESOLUTION_FAILED","message":"SPY et SPX requis pour ce cas.","preview":[]}

        spy_df,_=self._load_asset_df("spy")
        spx_df,_=self._load_asset_df("spx")
        spy_ohlc=self._ohlc_cols(spy_df)
        spx_ohlc=self._ohlc_cols(spx_df)

        spy_bounds=self.su.build_session_bounds(spy_df)
        pm_rows=[]
        spy_index=spy_df.sort_values("__time__").copy()
        for _,row in spy_bounds.iterrows():
            prev_close=row.get("prev_session_close_dt")
            curr_open=row.get("session_open_dt")
            dkey=row["date_key"]
            if pd.isna(prev_close) or pd.isna(curr_open):
                continue
            sub=spy_index[(spy_index["__time__"]>prev_close)&(spy_index["__time__"]<curr_open)].copy()
            if sub.empty:
                continue
            pm_high=pd.to_numeric(sub[spy_ohlc["high"]],errors="coerce").max()
            pm_low=pd.to_numeric(sub[spy_ohlc["low"]],errors="coerce").min()
            pm_rows.append({
                "date_key":dkey,
                "spy_pm_high":pm_high,
                "spy_pm_low":pm_low,
                "spy_pm_rows":int(len(sub)),
            })
        pm_daily=pd.DataFrame(pm_rows)
        if pm_daily.empty:
            return {
                "status":"OK",
                "answer_type":"mean",
                "source_asset":"spy",
                "target_asset":"spx",
                "condition_expr":"SPY premarket between previous session close and current session open inside previous day range",
                "metric_kind":"open",
                "value":None,
                "sample_size":0,
                "preview":[],
                "match_mode":"NO_PREMARKET_ROWS"
            }

        spy_daily=self._daily_summary("spy").sort_values("date_key").reset_index(drop=True)
        spy_daily["prev_high"]=pd.to_numeric(spy_daily["spy_high"],errors="coerce").shift(1)
        spy_daily["prev_low"]=pd.to_numeric(spy_daily["spy_low"],errors="coerce").shift(1)
        cond=pm_daily.merge(spy_daily[["date_key","prev_high","prev_low"]],on="date_key",how="inner")

        tol=0.001
        cond["prev_range"]=(pd.to_numeric(cond["prev_high"],errors="coerce")-pd.to_numeric(cond["prev_low"],errors="coerce")).abs()
        cond["tol_abs"]=cond["prev_range"].fillna(0)*tol
        cond["inside_strict"]=(pd.to_numeric(cond["spy_pm_high"],errors="coerce")<=pd.to_numeric(cond["prev_high"],errors="coerce")) & (pd.to_numeric(cond["spy_pm_low"],errors="coerce")>=pd.to_numeric(cond["prev_low"],errors="coerce"))
        cond["inside_touch"]=(pd.to_numeric(cond["spy_pm_high"],errors="coerce")<=pd.to_numeric(cond["prev_high"],errors="coerce")+pd.to_numeric(cond["tol_abs"],errors="coerce")) & (pd.to_numeric(cond["spy_pm_low"],errors="coerce")>=pd.to_numeric(cond["prev_low"],errors="coerce")-pd.to_numeric(cond["tol_abs"],errors="coerce"))

        chosen=cond[cond["inside_strict"]].copy()
        match_mode="STRICT"
        if chosen.empty:
            chosen=cond[cond["inside_touch"]].copy()
            match_mode="TOUCH_TOLERANT"

        spx_bounds=self.su.build_session_bounds(spx_df)
        spx_open=spx_df.merge(spx_bounds[["date_key","session_open_dt"]],on="date_key",how="left")
        spx_open=spx_open[spx_open["__time__"]==spx_open["session_open_dt"]].copy()
        spx_open["spx_open_val"]=pd.to_numeric(spx_open[spx_ohlc["open"]],errors="coerce")

        out=chosen.merge(spx_open[["date_key","spx_open_val"]],on="date_key",how="inner")
        return {
            "status":"OK",
            "answer_type":"mean" if self._mean_word(text) else "rows",
            "source_asset":"spy",
            "target_asset":"spx",
            "condition_expr":"SPY premarket between previous close and current open inside/touch previous day range",
            "metric_kind":"open",
            "value":None if out.empty else float(out["spx_open_val"].mean()),
            "sample_size":int(len(out)),
            "preview":out.head(preview_rows).to_dict("records"),
            "match_mode":match_mode,
            "strict_candidate_count":int(cond["inside_strict"].sum()) if len(cond) else 0,
            "touch_candidate_count":int(cond["inside_touch"].sum()) if len(cond) else 0,
        }

    def _query_point_in_time_next_day(self,text,preview_rows=10):
        assets=self._find_assets(text)
        if len(assets)<2:
            return {"status":"INSUFFICIENT_ASSET_RESOLUTION","message":"Actifs insuffisants.","preview":[]}
        cond_asset=assets[0]
        target_asset=assets[1]
        point=self._parse_at(text)
        if point is None and "morning" in _nrm(text):
            point="09:00"
        if point is None:
            return {"status":"POINT_TIME_PARSE_FAILED","message":"Heure ponctuelle non comprise.","preview":[]}

        kind,thr=self._parse_threshold(text)
        cond_df,_=self._load_asset_df(cond_asset)
        target_df,_=self._load_asset_df(target_asset)
        cond_ohlc=self._ohlc_cols(cond_df)

        cond_point=cond_df[cond_df["time_hhmm"]==point].copy()
        cond_point["cond_value"]=pd.to_numeric(cond_point[cond_ohlc["close"] or cond_ohlc["open"]],errors="coerce")
        if kind=="pct":
            return {"status":"POINT_TIME_PCT_NOT_SUPPORTED_YET","message":"Seuil pct ponctuel non encore exécuté.","preview":[]}
        cond_point=cond_point[cond_point["cond_value"]>=thr].copy()

        target_daily=self._daily_summary(target_asset).sort_values("date_key").reset_index(drop=True)
        target_daily["next_open"]=pd.to_numeric(target_daily.get(f"{target_asset}_open"),errors="coerce").shift(-1)
        target_daily["next_close"]=pd.to_numeric(target_daily.get(f"{target_asset}_close"),errors="coerce").shift(-1)
        target_daily["next_ret"]=(target_daily["next_close"]-target_daily["next_open"])/target_daily["next_open"].replace(0,np.nan)

        metric_kind=self._target_metric_kind(text)
        metric_col="next_ret" if metric_kind=="return" else ("next_open" if metric_kind=="open" else "next_close")
        out=cond_point.merge(target_daily[["date_key",metric_col]],on="date_key",how="inner").rename(columns={metric_col:"metric_value"})
        return {
            "status":"OK",
            "answer_type":"mean" if self._mean_word(text) else "rows",
            "source_asset":cond_asset,
            "target_asset":target_asset,
            "condition_expr":f"{cond_asset} at {point} >= {thr}",
            "metric_kind":metric_kind,
            "horizon":"next_day",
            "value":None if out.empty else float(pd.to_numeric(out["metric_value"],errors="coerce").mean()),
            "sample_size":int(len(out)),
            "preview":out.head(preview_rows).to_dict("records"),
        }

    def _query_simple_cross_asset(self,text,preview_rows=10):
        assets=self._find_assets(text)
        if len(assets)<2:
            return {"status":"INSUFFICIENT_ASSET_RESOLUTION","message":"Actifs insuffisants.","preview":[]}
        cond_asset=assets[0]
        target_asset=assets[-1]

        cond_daily=self._daily_summary(cond_asset)
        target_daily=self._daily_summary(target_asset)

        kind,thr=self._parse_threshold(text)
        if kind=="pct":
            cond_daily["cond_value"]=(pd.to_numeric(cond_daily[f"{cond_asset}_close"],errors="coerce")-pd.to_numeric(cond_daily[f"{cond_asset}_open"],errors="coerce"))/pd.to_numeric(cond_daily[f"{cond_asset}_open"],errors="coerce").replace(0,np.nan)
        else:
            cond_daily["cond_value"]=pd.to_numeric(cond_daily[f"{cond_asset}_close"],errors="coerce")

        if thr is None:
            return {"status":"THRESHOLD_NOT_FOUND","message":"Seuil non compris.","preview":[]}
        cond_daily=cond_daily[cond_daily["cond_value"]>=thr].copy()

        metric_kind=self._target_metric_kind(text)
        if metric_kind=="open":
            metric_col=f"{target_asset}_open"
        elif metric_kind=="return":
            target_daily["metric_value"]=(pd.to_numeric(target_daily[f"{target_asset}_close"],errors="coerce")-pd.to_numeric(target_daily[f"{target_asset}_open"],errors="coerce"))/pd.to_numeric(target_daily[f"{target_asset}_open"],errors="coerce").replace(0,np.nan)
            metric_col="metric_value"
        else:
            metric_col=f"{target_asset}_close"

        out=cond_daily.merge(target_daily[["date_key",metric_col]],on="date_key",how="inner").rename(columns={metric_col:"metric_value"})
        return {
            "status":"OK",
            "answer_type":"mean" if self._mean_word(text) else ("rows" if self._show_dates(text) else "mean"),
            "source_asset":cond_asset,
            "target_asset":target_asset,
            "condition_expr":f"{cond_asset} >= {thr}",
            "metric_kind":metric_kind,
            "value":None if out.empty else float(pd.to_numeric(out["metric_value"],errors="coerce").mean()),
            "sample_size":int(len(out)),
            "preview":out.head(preview_rows).to_dict("records"),
        }

    def execute(self,text,preview_rows=10):
        if self._is_month_aggregation_case(text):
            return self._query_open_bucket_by_month(text,preview_rows)
        if self._is_previous_day_range_case(text):
            return self._query_premarket_previous_day(text,preview_rows)
        nt=_nrm(text)
        if self._is_next_day_case(text) or self._parse_at(text) is not None or "morning" in nt:
            return self._query_point_in_time_next_day(text,preview_rows)
        start,end=self._parse_between(text)
        if start and end:
            return self._query_intraday_window(text,preview_rows)
        return self._query_simple_cross_asset(text,preview_rows)
