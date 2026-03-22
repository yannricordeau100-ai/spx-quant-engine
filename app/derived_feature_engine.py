import os, re, json, unicodedata, pandas as pd, importlib.util

#1/40 helpers
def _nrm(s):
    s="" if s is None else str(s)
    s=s.strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _load_module(path,name):
    spec=importlib.util.spec_from_file_location(name,path)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

#2/40 engine
class DerivedFeatureEngine:
    def __init__(self, project_root):
        self.project_root=project_root
        self.app_dir=os.path.join(project_root,"app_streamlit")
        self.processed=os.path.join(project_root,"processed")
        self.config=os.path.join(project_root,"config")
        self.store_dir=os.path.join(self.processed,"DERIVED_FEATURE_STORE","csv")
        self.registry_json=os.path.join(self.processed,"DERIVED_FEATURE_STORE","derived_feature_registry.json")
        self.registry_csv=os.path.join(self.processed,"DERIVED_FEATURE_STORE","derived_feature_registry.csv")
        self.cfg_path=os.path.join(self.config,"derived_feature_store_config.json")
        self.time_registry_path=os.path.join(self.processed,"ETAPE141_TIME_FORMAT_REGISTRY.json")
        self.canonical_registry_path=os.path.join(self.processed,"ETAPE170D_CANONICAL_SOURCE_REGISTRY.json")
        self.canonical_fingerprints_path=os.path.join(self.processed,"ETAPE170D_CANONICAL_SOURCE_FINGERPRINTS.json")
        self.store_mod=_load_module(os.path.join(self.app_dir,"derived_feature_store.py"),"derived_feature_store_runtime_170d")
        self.guard_mod=_load_module(os.path.join(self.app_dir,"source_guard.py"),"source_guard_runtime_170d")
        self.cfg=self._load_cfg()
        self.time_registry=self._load_time_registry()
        self.registry=self.guard_mod.load_registry(self.canonical_registry_path)
        self.fingerprints=self.guard_mod.load_fingerprints(self.canonical_fingerprints_path)

    #3/40 loaders
    def _load_cfg(self):
        if not os.path.exists(self.cfg_path):
            return {"default_tail_rows":50}
        try:
            with open(self.cfg_path,"r",encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"default_tail_rows":50}

    def _load_time_registry(self):
        if not os.path.exists(self.time_registry_path):
            return {"path_index":{}}
        try:
            with open(self.time_registry_path,"r",encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"path_index":{}}

    def _available(self):
        return sorted((self.registry.get("datasets",{}) or {}).keys())

    #4/40 asset mapping
    def _asset_to_dataset(self, name):
        n=_nrm(name)
        mapping={
            "spx":"spx_daily","spy":"spy_daily","qqq":"qqq_daily","iwm":"iwm_daily",
            "vix":"vix_daily","vvix":"vvix_daily","vix9d":"vix9d_daily","vix1d":"vix1d_best",
            "dxy":"dxy_daily","us10y":"us10y_daily","10 ans us":"us10y_daily",
            "calendar":"calendar_daily","or":"gold_daily","gold":"gold_daily","petrole":"oil_best","pétrole":"oil_best","oil":"oil_best",
            "tick":"tick_best"
        }
        return mapping.get(n)

    #5/40 parser
    def can_handle(self,q):
        nq=_nrm(q)
        return any(w in nq for w in ["ratio","spread","ecart","écart","/","delta","difference","diff","zscore","moyenne glissante","rolling"]) and any(x in nq for x in ["vix","spx","spy","qqq","iwm","dxy","us10y","or","gold","petrole","pétrole","oil","tick"])

    def _parse_years(self,q):
        nq=_nrm(q)
        m=re.search(r"(\d+)\s*(dernieres|dernières|last)\s*(annees|années|years)",nq)
        if m:
            return int(m.group(1))
        return 3

    def _parse_operation(self,q):
        nq=_nrm(q)
        if "zscore" in nq:
            return "zscore_ratio"
        if "moyenne glissante" in nq or "rolling" in nq:
            return "rolling_ratio_20"
        if "delta" in nq or "difference" in nq or "diff" in nq or "ecart" in nq or "écart" in nq or "spread" in nq:
            return "spread"
        if "ratio" in nq or "/" in nq:
            return "ratio"
        return "ratio"

    def _extract_assets(self,q):
        nq=_nrm(q)
        order=[]
        aliases=["vix1d","vix9d","vvix","vix","spx","spy","qqq","iwm","dxy","us10y","10 ans us","gold","or","oil","petrole","pétrole","tick"]
        for a in aliases:
            if a in nq:
                ds=self._asset_to_dataset(a)
                if ds and ds not in order:
                    order.append(ds)
        return order[:2]

    #6/40 read close
    def _meta(self, ds_key):
        meta=self.guard_mod.ensure_dataset_exists(self.registry,ds_key)
        meta=dict(meta)
        meta["dataset_key"]=ds_key
        return meta

    def _read_close(self, ds_key):
        meta=self._meta(ds_key)
        path=meta["path"]
        fmt=(self.time_registry.get("path_index",{}).get(path,{}) or {}).get("time_format")
        freq=(meta.get("freq_hint") or "")
        if str(freq)=="daily":
            return self.store_mod.read_daily_close(path,fmt), meta
        return self._derive_daily_close_from_intraday(path,fmt), meta

    def _derive_daily_close_from_intraday(self,path,fmt=None):
        df=self.store_mod._read_csv_flex(path)
        tcol=self.store_mod._first_match(df.columns,["time","datetime","date","timestamp"])
        ccol=self.store_mod._first_match(df.columns,["close"])
        if not tcol or not ccol:
            raise RuntimeError(f"INTRADAY_CLOSE_NOT_FOUND::{path}")
        df[tcol]=self.store_mod._parse_time_series(df[tcol],fmt)
        df=df[df[tcol].notna()].copy().sort_values(tcol).reset_index(drop=True)
        if df.empty:
            raise RuntimeError(f"NO_VALID_TIME_ROWS::{path}")
        out=df[[tcol,ccol]].copy()
        out.columns=["time","close"]
        out["close"]=pd.to_numeric(out["close"],errors="coerce")
        out=out[out["close"].notna()].copy()
        out["date_key"]=out["time"].dt.strftime("%Y-%m-%d")
        out=out.groupby("date_key",as_index=False).agg({"close":"last"})
        return out.sort_values("date_key").reset_index(drop=True)

    def _limit_years(self, df, years):
        if df.empty:
            return df
        df=df.copy()
        dt=pd.to_datetime(df["date_key"],errors="coerce")
        if dt.notna().sum()==0:
            return df
        max_dt=dt.max()
        cutoff=max_dt-pd.Timedelta(days=int(years*365.25))
        return df[dt>=cutoff].copy()

    #7/40 formulas
    def _compute_pair_feature(self, ds_a, ds_b, operation, years):
        a,a_meta=self._read_close(ds_a)
        b,b_meta=self._read_close(ds_b)
        a=a.rename(columns={"close":"a_close"})
        b=b.rename(columns={"close":"b_close"})
        m=a.merge(b,on="date_key",how="inner")
        m=self._limit_years(m,years)
        ratio=m["a_close"]/m["b_close"]
        if operation=="ratio":
            m["value"]=ratio
            fname=f"ratio__{ds_a}__over__{ds_b}"
        elif operation=="spread":
            m["value"]=m["a_close"]-m["b_close"]
            fname=f"spread__{ds_a}__minus__{ds_b}"
        elif operation=="rolling_ratio_20":
            m["value"]=ratio.rolling(20).mean()
            fname=f"rolling20_ratio__{ds_a}__over__{ds_b}"
        elif operation=="zscore_ratio":
            raw=ratio
            m["value"]=(raw-raw.rolling(20).mean())/raw.rolling(20).std()
            fname=f"zscore20_ratio__{ds_a}__over__{ds_b}"
        else:
            m["value"]=ratio
            fname=f"ratio__{ds_a}__over__{ds_b}"
        m["pct_change_1d"]=m["value"].pct_change()
        m["zscore_20"]=((m["value"]-m["value"].rolling(20).mean())/m["value"].rolling(20).std())
        return fname,m,[a_meta,b_meta]

    #8/40 main
    def run(self,q,preview_rows=20):
        assets=self._extract_assets(q)
        if len(assets)<2:
            return {"status":"NO_DERIVED_MATCH","answer_type":"table","value":0,"preview":[]}

        years=self._parse_years(q)
        operation=self._parse_operation(q)
        tail_rows=int(self.cfg.get("default_tail_rows",50))
        params={"assets":assets,"operation":operation,"years":years,"tail_rows":tail_rows}
        fname=f"{operation}__{assets[0]}__{assets[1]}__{years}y"

        missing=[x for x in assets if x not in (self.registry.get("datasets",{}) or {})]
        if missing:
            return {
                "status":"MISSING_DERIVED_SOURCE",
                "answer_type":"table",
                "value":0,
                "feature_name":fname,
                "cache_hit":False,
                "missing_datasets":missing,
                "available_datasets":self._available(),
                "preview":[]
            }

        cached=self.store_mod.lookup_cached_feature(self.registry_json,fname,params)
        if cached:
            df=pd.read_csv(cached["csv_path"])
            preview=df.tail(preview_rows).to_dict(orient="records")
            cached_total_rows=None
            if isinstance(cached,dict):
                cached_total_rows=cached.get("n_rows")
                if cached_total_rows is None:
                    cached_total_rows=cached.get("row_count")
                if cached_total_rows is None:
                    cached_total_rows=cached.get("full_row_count")
            if cached_total_rows is None:
                cached_total_rows=len(df)
            result={
                "status":"OK_CACHED",
                "answer_type":"table",
                "value":int(cached_total_rows),
                "feature_name":fname,
                "cache_hit":True,
                "cache_csv_path":cached["csv_path"],
                "available_datasets":self._available(),
                "preview":preview
            }
            src=[self._meta(assets[0]),self._meta(assets[1])]
            return self.guard_mod.attach_source_block(result,src)

        try:
            _,df,src=self._compute_pair_feature(assets[0],assets[1],operation,years)
        except Exception as e:
            return {
                "status":"DERIVED_COMPUTE_ERROR",
                "answer_type":"table",
                "value":0,
                "feature_name":fname,
                "cache_hit":False,
                "error_repr":repr(e),
                "available_datasets":self._available(),
                "preview":[]
            }

        entry=self.store_mod.persist_feature_dataframe(
            root_dir=self.store_dir,
            registry_json_path=self.registry_json,
            registry_csv_path=self.registry_csv,
            feature_name=fname,
            params=params,
            df=df,
            tail_rows=tail_rows
        )
        preview=df.tail(preview_rows).to_dict(orient="records")
        result={
            "status":"OK_COMPUTED_AND_STORED",
            "answer_type":"table",
            "value":int(len(df)),
            "feature_name":fname,
            "cache_hit":False,
            "cache_csv_path":entry["csv_path"],
            "available_datasets":self._available(),
            "preview":preview
        }
        return self.guard_mod.attach_source_block(result,src)
