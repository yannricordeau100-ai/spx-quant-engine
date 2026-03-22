import os,re,json,unicodedata,importlib.util,pandas as pd

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _slug(s):
    return re.sub(r"[^a-z0-9]+","_",_nrm(s)).strip("_")

class CustomPatternResearchEngine:
    def __init__(self, architecture_json_path):
        self.architecture_json_path=architecture_json_path
        self.project_root=os.path.dirname(os.path.dirname(self.architecture_json_path))
        self.raw_root=os.path.join(self.project_root,"RAW_SOURCES")
        self.processed=os.path.join(self.project_root,"processed")
        self.time_registry_path=os.path.join(self.processed,"ETAPE141_TIME_FORMAT_REGISTRY.json")
        self.result_memory_path=os.path.join(self.processed,"ETAPE163_LAST_CUSTOM_RESEARCH_MEMORY.json")
        self.priority_registry_path=os.path.join(self.processed,"ETAPE168A_DATA_PRIORITY_REGISTRY.json")
        self.semantics_path=os.path.join(self.project_root,"app_streamlit","nl_query_semantics.py")
        self.heavy_scan_path=os.path.join(self.project_root,"app_streamlit","heavy_scan_execution.py")
        self.time_registry=self._load_time_registry()
        self.dataset_map=self._build_dataset_map()
        self._sem=None
        self._heavy=None

    def _load_module(self,path,name):
        if not os.path.exists(path):
            return None
        spec=importlib.util.spec_from_file_location(name,path)
        mod=importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def _load_semantics(self):
        if self._sem is None:
            self._sem=self._load_module(self.semantics_path,"nl_query_semantics_runtime_166")
        return self._sem

    def _load_heavy(self):
        if self._heavy is None:
            self._heavy=self._load_module(self.heavy_scan_path,"heavy_scan_execution_runtime_166")
        return self._heavy

    def _load_time_registry(self):
        if not os.path.exists(self.time_registry_path):
            return {"path_index":{}, "dataset_index":{}}
        try:
            with open(self.time_registry_path,"r",encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"path_index":{}, "dataset_index":{}}

    def refresh(self):
        self.time_registry=self._load_time_registry()
        self.dataset_map=self._build_dataset_map()

    def can_handle(self,q):
        nq=_nrm(q)
        keys=["relation","pattern","impact","effet","lien","ouverture","cloture","clôture","gap","calendar","macro","vix","dxy","10 ans us","si "," alors ","et","ou","avec","sans","with","without","and","or","!","&&","||","et/ou","(",")"]
        return any(k in nq for k in keys)

    def _build_dataset_map(self):
        m={
            "spx_daily":os.path.join(self.raw_root,"SPX","SPX_daily.csv"),
            "spx_30m":os.path.join(self.raw_root,"SPX","SPX_30min.csv"),
            "spy_daily":os.path.join(self.raw_root,"SPY","SPY_daily.csv"),
            "spy_30m":os.path.join(self.raw_root,"SPY","SPY_30min.csv"),
            "qqq_daily":os.path.join(self.raw_root,"QQQ","QQQ_daily.csv"),
            "qqq_30m":os.path.join(self.raw_root,"QQQ","QQQ_30min.csv"),
            "iwm_daily":os.path.join(self.raw_root,"IWM","IWM_daily.csv"),
            "iwm_30m":os.path.join(self.raw_root,"IWM","IWM_30min.csv"),
            "vix_daily":os.path.join(self.raw_root,"VIX","VIX_daily.csv"),
            "vvix_daily":os.path.join(self.raw_root,"VVIX","VVIX_daily.csv"),
            "dxy_daily":os.path.join(self.raw_root,"Dollar DXY","DXY_daily.csv"),
            "us10y_daily":os.path.join(self.raw_root,"Bond US","US_10_years_bonds_daily.csv"),
            "calendar_daily":os.path.join(self.raw_root,"Calendar","calendar_events_daily.csv"),
        }
        aau_dir=os.path.join(self.raw_root,"Autres Actions Upload")
        if os.path.isdir(aau_dir):
            for f in os.listdir(aau_dir):
                if f.lower().endswith(".csv"):
                    m[_slug(os.path.splitext(f)[0])]=os.path.join(aau_dir,f)
        return {k:v for k,v in m.items() if os.path.exists(v)}

    def _asset_to_dataset_key(self, asset_code):
        direct={"SPX":"spx_daily","SPY":"spy_daily","QQQ":"qqq_daily","IWM":"iwm_daily","VIX":"vix_daily","VVIX":"vvix_daily","DXY":"dxy_daily","US10Y":"us10y_daily","CALENDAR":"calendar_daily"}
        if asset_code in direct and direct[asset_code] in self.dataset_map:
            return direct[asset_code]
        if asset_code in {"AAPL","NVDA","MSFT","AMZN","META","TSLA"}:
            slug=asset_code.lower()
            for k in self.dataset_map:
                if slug in k:
                    return k
        return None

    def _target_intraday_if_available(self,target_key):
        m={"spx_daily":"spx_30m","spy_daily":"spy_30m","qqq_daily":"qqq_30m","iwm_daily":"iwm_30m"}
        intrakey=m.get(target_key)
        if intrakey and intrakey in self.dataset_map:
            return intrakey
        return None

    def _parse_semantics(self,q):
        sem=self._load_semantics()
        if sem is None:
            return {}
        try:
            return sem.parse_query_semantics(q)
        except Exception:
            return {}

    def _resolve_target(self,q,semantics):
        target_asset=semantics.get("target_asset")
        if target_asset:
            k=self._asset_to_dataset_key(target_asset)
            if k:
                return k
        assets=semantics.get("assets_detected",[]) if isinstance(semantics,dict) else []
        for a in reversed(assets):
            k=self._asset_to_dataset_key(a)
            if k and k!="calendar_daily":
                return k
        return "spx_daily"

    def _resolve_driver_specs(self,semantics,target_key):
        specs=[]
        drivers=semantics.get("driver_assets",[]) or []
        if not drivers:
            assets=semantics.get("assets_detected",[])
            drivers=[a for a in assets if self._asset_to_dataset_key(a) and self._asset_to_dataset_key(a)!=target_key]
        for a in drivers:
            k=self._asset_to_dataset_key(a)
            if not k or k==target_key:
                continue
            kind="calendar" if a=="CALENDAR" else "asset"
            specs.append({"asset":a,"name":k,"path":self.dataset_map.get(k),"kind":kind})
        return specs[:3]

    def _performance_route_info(self,semantics):
        heavy=self._load_heavy()
        return {
            "performance_route_candidate":semantics.get("cross_asset_count",0)>=2,
            "polars_available":getattr(heavy,"POLARS_OK",None) if heavy else None,
            "duckdb_available":getattr(heavy,"DUCKDB_OK",None) if heavy else None,
            "reason":"precedence_logic_evaluation" if semantics.get("cross_asset_count",0)>=2 else "standard_runtime_path",
            "executed_heavy_scan":False,
            "heavy_scan_mode":None,
            "heavy_scan_status":None,
        }

    def _run_heavy(self,target_key,semantics):
        heavy=self._load_heavy()
        if heavy is None:
            return [], {"executed_heavy_scan":False,"heavy_scan_mode":None,"heavy_scan_status":"heavy_module_missing"}
        specs=self._resolve_driver_specs(semantics,target_key)
        if len(specs)==0:
            return [], {"executed_heavy_scan":False,"heavy_scan_mode":None,"heavy_scan_status":"no_driver_specs"}
        try:
            rows=heavy.execute_heavy_scan({
                "semantics":semantics,
                "target_daily_path":self.dataset_map[target_key],
                "target_intraday_path":self.dataset_map.get(self._target_intraday_if_available(target_key)),
                "target_dataset":target_key,
                "driver_specs":specs,
                "time_registry":self.time_registry,
            })
            return rows, {"executed_heavy_scan":True,"heavy_scan_mode":"precedence_logic_evaluation","heavy_scan_status":"ok"}
        except Exception as e:
            return [], {"executed_heavy_scan":True,"heavy_scan_mode":"precedence_logic_evaluation","heavy_scan_status":repr(e)}

def _load_priority_registry(self):
    if not os.path.exists(self.priority_registry_path):
        return {}
    try:
        with open(self.priority_registry_path,"r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _soft_priority_score(self, dataset_key):
    reg=self._load_priority_registry()
    scores=(reg.get("dataset_priority_scores") or {})
    return float(scores.get(dataset_key,0.95))

    def _universe_mode(self,target_key):
        if target_key in {"spx_daily","spy_daily","qqq_daily","iwm_daily"}:
            return "fundamentals_or_mixed"
        return "aau_conditioned_by_fundamentals"

    def run(self,q,preview_rows=20):
        self.refresh()
        semantics=self._parse_semantics(q)
        target_key=self._resolve_target(q,semantics)
        perf_info=self._performance_route_info(semantics)
        out,meta=self._run_heavy(target_key,semantics)
        perf_info.update(meta)

        def _score(row):
            vals=[]
            for k in ["plus5d_mean","plus3d_mean","next_close_mean","intraday_mean","gap_mean","open_up_prob","open_down_prob"]:
                v=row.get(k)
                try:
                    vals.append(abs(float(v)))
                except Exception:
                    pass
            return sum(vals)

        out=sorted(out,key=_score,reverse=True)[:preview_rows]
        result_payload={
            "status":"OK",
            "answer_type":"table",
            "value":int(len(out)),
            "universe_mode":self._universe_mode(target_key),
            "target_dataset":target_key,
            "strict_mode":"precedence_logic_v1",
            "semantics":semantics,
            "performance_route_info":perf_info,
            "preview":out
        }
        try:
            with open(self.result_memory_path,"w",encoding="utf-8") as f:
                json.dump({
                    "question":q,
                    "saved_at_utc":pd.Timestamp.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "target_dataset":result_payload.get("target_dataset"),
                    "universe_mode":result_payload.get("universe_mode"),
                    "strict_mode":result_payload.get("strict_mode"),
                    "semantics":result_payload.get("semantics",{}),
                    "performance_route_info":result_payload.get("performance_route_info",{}),
                    "preview":result_payload.get("preview",[]),
                    "value":result_payload.get("value")
                },f,ensure_ascii=False,indent=2)
        except Exception:
            pass
        return result_payload
