import os,re,unicodedata,pandas as pd,numpy as np,time,json

try:
    import polars as pl
except Exception:
    pl=None

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

class PerformanceDataLayer:
    def __init__(self, time_registry_path=None):
        self.polars_available = pl is not None
        self.time_registry_path=time_registry_path
        self.time_registry=self._load_time_registry()

    def _load_time_registry(self):
        if not self.time_registry_path or not os.path.exists(self.time_registry_path):
            return {"path_index":{}, "dataset_index":{}}
        try:
            with open(self.time_registry_path,"r",encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"path_index":{}, "dataset_index":{}}

    def detect_time_col(self,df):
        cols=list(df.columns)
        for c in ["time","datetime","date","timestamp"]:
            if c in cols:
                return c
        for c in cols:
            if "time" in c or "date" in c:
                return c
        return None

    def first_match(self,cols,candidates):
        s={str(c).lower():c for c in cols}
        for cand in candidates:
            if cand.lower() in s:
                return s[cand.lower()]
        for c in cols:
            lc=str(c).lower()
            for cand in candidates:
                if cand.lower() in lc:
                    return c
        return None

    def parse_time_series(self,s,path=None):
        s=s.astype(str).str.strip()

        # registry-first
        reg=None
        if path:
            reg=self.time_registry.get("path_index",{}).get(path)
        if reg and reg.get("time_format"):
            try:
                dt=pd.to_datetime(s,errors="coerce",format=reg["time_format"])
                if int(dt.notna().sum())>0:
                    return dt
            except Exception:
                pass

        # safe fallback exact formats only
        formats=[
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%m/%d/%Y %H:%M:%S",
            "%m/%d/%Y %H:%M",
            "%m/%d/%Y",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
        ]
        best=None
        best_count=-1
        for fmt in formats:
            try:
                dt=pd.to_datetime(s,errors="coerce",format=fmt)
                c=int(dt.notna().sum())
                if c>best_count:
                    best=dt
                    best_count=c
                if c==len(s):
                    return dt
            except Exception:
                pass

        # last resort generic, but only once
        try:
            dt=pd.to_datetime(s,errors="coerce")
            c=int(dt.notna().sum())
            if c>best_count:
                best=dt
                best_count=c
        except Exception:
            pass

        if best is None:
            return pd.to_datetime(pd.Series([None]*len(s)),errors="coerce")
        return best

    def read_csv_any_pandas(self,path):
        last=None
        for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
            try:
                with open(path,"r",encoding=enc,errors="replace") as f:
                    lines=[x for x in f.read().splitlines() if str(x).strip()!=""]
                if not lines:
                    continue
                header=lines[0]
                best_sep=None; best_n=1
                for sep in [",",";","\t","|"]:
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
                for sep in (",",";","\t","|",None):
                    try:
                        if sep is None:
                            df=pd.read_csv(path,sep=None,engine="python",encoding=enc,on_bad_lines="skip")
                        else:
                            df=pd.read_csv(path,sep=sep,engine="python",encoding=enc,on_bad_lines="skip")
                        if df is not None and df.shape[1]>=1:
                            df.columns=_normalize_cols(df.columns)
                            return df
                    except Exception as e:
                        last=e
            except Exception as e:
                last=e
        raise last

    def read_csv_any(self,path):
        if self.polars_available:
            try:
                for sep in [None,",",";","\t","|"]:
                    try:
                        if sep is None:
                            df=pl.read_csv(path,try_parse_dates=False,infer_schema_length=2000,ignore_errors=True)
                        else:
                            df=pl.read_csv(path,separator=sep,try_parse_dates=False,infer_schema_length=2000,ignore_errors=True)
                        if df is not None and df.width>=1:
                            pdf=df.to_pandas()
                            pdf.columns=_normalize_cols(pdf.columns)
                            return pdf
                    except Exception:
                        pass
            except Exception:
                pass
        return self.read_csv_any_pandas(path)

    def detect_ohlc_cols(self,df,path=None):
        reg=None
        if path:
            reg=self.time_registry.get("path_index",{}).get(path)

        if reg:
            o=reg.get("open_col")
            h=reg.get("high_col")
            l=reg.get("low_col")
            c=reg.get("close_col")
            if all([o,h,l,c]) and all(x in df.columns for x in [o,h,l,c]):
                return {"open":o,"high":h,"low":l,"close":c,"has_ohlc":True}

        o=self.first_match(df.columns,["open"])
        h=self.first_match(df.columns,["high"])
        l=self.first_match(df.columns,["low"])
        c=self.first_match(df.columns,["close"])
        return {"open":o,"high":h,"low":l,"close":c,"has_ohlc":all([o,h,l,c])}

    def read_daily_ohlc(self,path):
        df=self.read_csv_any(path)
        tcol=self.detect_time_col(df)
        if tcol is None:
            raise RuntimeError(f"TIME_NOT_FOUND::{path}")
        dt=self.parse_time_series(df[tcol],path=path)
        df[tcol]=dt
        df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
        if df.empty:
            raise RuntimeError(f"NO_VALID_TIME_ROWS::{path}")
        df["date_key"]=df[tcol].dt.strftime("%Y-%m-%d")

        cols=self.detect_ohlc_cols(df,path=path)
        if not cols["has_ohlc"]:
            raise RuntimeError(f"OHLC_NOT_FOUND::{path}")

        o,h,l,c=cols["open"],cols["high"],cols["low"],cols["close"]

        if self.polars_available:
            try:
                pldf=pl.from_pandas(df[[tcol,"date_key",o,h,l,c]].copy())
                out=pldf.group_by("date_key").agg([
                    pl.col(o).first().alias("open"),
                    pl.col(h).max().alias("high"),
                    pl.col(l).min().alias("low"),
                    pl.col(c).last().alias("close"),
                ]).sort("date_key").to_pandas()
            except Exception:
                out=df.groupby("date_key",as_index=False).agg({o:"first",h:"max",l:"min",c:"last"}).rename(columns={o:"open",h:"high",l:"low",c:"close"})
        else:
            out=df.groupby("date_key",as_index=False).agg({o:"first",h:"max",l:"min",c:"last"}).rename(columns={o:"open",h:"high",l:"low",c:"close"})

        for x in ["open","high","low","close"]:
            out[x]=pd.to_numeric(out[x],errors="coerce")
        out=out.sort_values("date_key").reset_index(drop=True)
        out["gap_open_vs_prev_close"]=(out["open"]-out["close"].shift(1))/out["close"].shift(1).replace(0,np.nan)
        out["ret_oc"]=(out["close"]-out["open"])/out["open"].replace(0,np.nan)
        out["range_intraday"]=(out["high"]-out["low"])/out["open"].replace(0,np.nan)
        out["next_close_ret"]=(out["close"].shift(-1)/out["close"])-1.0
        out["plus3d_ret"]=(out["close"].shift(-3)/out["close"])-1.0
        out["plus5d_ret"]=(out["close"].shift(-5)/out["close"])-1.0
        out["ret_1w_exante"]=(out["close"].shift(1)/out["close"].shift(6))-1.0
        out["ret_2w_exante"]=(out["close"].shift(1)/out["close"].shift(11))-1.0
        out["ret_4w_exante"]=(out["close"].shift(1)/out["close"].shift(21))-1.0
        out["vol_5d_exante"]=out["ret_oc"].rolling(5).std().shift(1)
        return out

    def read_intraday_open_slice(self,path):
        df=self.read_csv_any(path)
        tcol=self.detect_time_col(df)
        if tcol is None:
            raise RuntimeError(f"TIME_NOT_FOUND::{path}")
        dt=self.parse_time_series(df[tcol],path=path)
        df[tcol]=dt
        df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
        if df.empty:
            raise RuntimeError(f"NO_VALID_TIME_ROWS::{path}")

        cols=self.detect_ohlc_cols(df,path=path)
        if not cols["has_ohlc"]:
            raise RuntimeError(f"OHLC_NOT_FOUND::{path}")

        o,h,l,c=cols["open"],cols["high"],cols["low"],cols["close"]
        df["date_key"]=df[tcol].dt.strftime("%Y-%m-%d")
        df["hour_min"]=df[tcol].dt.strftime("%H:%M")

        if self.polars_available:
            try:
                pldf=pl.from_pandas(df[[tcol,"date_key","hour_min",o,h,l,c]].copy())
                first=pldf.sort(tcol).group_by("date_key").agg([
                    pl.col("hour_min").first().alias("hour_min"),
                    pl.col(o).first().alias("open"),
                    pl.col(h).first().alias("high"),
                    pl.col(l).first().alias("low"),
                    pl.col(c).first().alias("close"),
                ]).sort("date_key").to_pandas()
            except Exception:
                first=df.groupby("date_key",as_index=False).first().rename(columns={o:"open",h:"high",l:"low",c:"close"})
        else:
            first=df.groupby("date_key",as_index=False).first().rename(columns={o:"open",h:"high",l:"low",c:"close"})

        for x in ["open","high","low","close"]:
            first[x]=pd.to_numeric(first[x],errors="coerce")
        first["open_bar_ret"]=(first["close"]-first["open"])/first["open"].replace(0,np.nan)
        first["open_bar_range"]=(first["high"]-first["low"])/first["open"].replace(0,np.nan)
        first["open_bar_direction"]=np.where(first["close"]>first["open"],1,np.where(first["close"]<first["open"],-1,0))
        return first[["date_key","hour_min","open","high","low","close","open_bar_ret","open_bar_range","open_bar_direction"]].copy()

    def read_daily_binary_flags(self,path, macro_candidates=None, quiet_candidates=None):
        macro_candidates = macro_candidates or ["macro_event","macro"]
        quiet_candidates = quiet_candidates or ["low_activity_period","low activity period"]
        df=self.read_csv_any(path)
        tcol=self.detect_time_col(df)
        if tcol is None:
            raise RuntimeError(f"TIME_NOT_FOUND::{path}")
        dt=self.parse_time_series(df[tcol],path=path)
        df[tcol]=dt
        df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
        if df.empty:
            raise RuntimeError(f"NO_VALID_TIME_ROWS::{path}")
        df["date_key"]=df[tcol].dt.strftime("%Y-%m-%d")

        macro_col=self.first_match(df.columns,macro_candidates)
        quiet_col=self.first_match(df.columns,quiet_candidates)

        out=pd.DataFrame({"date_key":df["date_key"]})
        out["is_macro_day"]=False if macro_col is None else df[macro_col].astype(str).str.strip().ne("").fillna(False).infer_objects(copy=False).astype(bool)
        out["is_quiet_day"]=False if quiet_col is None else df[quiet_col].astype(str).str.strip().ne("").fillna(False).infer_objects(copy=False).astype(bool)
        out["macro_event_text"]="" if macro_col is None else df[macro_col].astype(str)
        out=out.groupby("date_key",as_index=False).agg({
            "is_macro_day":"max",
            "is_quiet_day":"max",
            "macro_event_text":"first"
        })
        return out

    def benchmark_pair(self,path_daily,path_intraday=None):
        out={"polars_available":self.polars_available}
        try:
            t0=time.perf_counter()
            d1=self.read_daily_ohlc(path_daily)
            out["daily_seconds"]=time.perf_counter()-t0
            out["daily_rows"]=int(len(d1))
            out["daily_error"]=None
        except Exception as e:
            out["daily_seconds"]=None
            out["daily_rows"]=None
            out["daily_error"]=repr(e)

        if path_intraday and os.path.exists(path_intraday):
            try:
                t1=time.perf_counter()
                d2=self.read_intraday_open_slice(path_intraday)
                out["intraday_seconds"]=time.perf_counter()-t1
                out["intraday_rows"]=int(len(d2))
                out["intraday_error"]=None
            except Exception as e:
                out["intraday_seconds"]=None
                out["intraday_rows"]=None
                out["intraday_error"]=repr(e)
        return out
