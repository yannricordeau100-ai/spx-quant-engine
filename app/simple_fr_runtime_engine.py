import os, re, json, unicodedata, pandas as pd

def _nrm(s):
    s="" if s is None else str(s)
    s=s.strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

MONTHS={
    "janvier":1,"fevrier":2,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"aout":8,"août":8,"septembre":9,"octobre":10,"novembre":11,"decembre":12,"décembre":12
}

class SimpleFRRuntimeEngine:
    def __init__(self, project_root):
        self.project_root=project_root
        self.processed=os.path.join(project_root,"processed")
        self.registry_path=os.path.join(self.processed,"ETAPE170D_CANONICAL_SOURCE_REGISTRY.json")
        self.registry=self._load_registry()

    def _load_registry(self):
        if not os.path.exists(self.registry_path):
            return {"datasets":{}}
        with open(self.registry_path,"r",encoding="utf-8") as f:
            return json.load(f)

    def _path(self, key):
        meta=(self.registry.get("datasets",{}) or {}).get(key)
        return None if not meta else meta.get("path")

    def can_handle(self, q):
        nq=_nrm(q)
        patterns=[
            "combien d annonces economiques","combien d'annonces economiques",
            "combien d annonces économiques","combien d'annonces économiques",
            "combien de seance","combien de séance","combien de fois",
            "ouvert en hausse","ouvert en baisse","ouverture en hausse","ouverture en baisse",
            "superieur a","supérieur à",">"
        ]
        return any(p in nq for p in patterns)

    def _read_csv_flex(self, path):
        last=None
        for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
            for sep in (None,",",";","\t","|"):
                try:
                    if sep is None:
                        df=pd.read_csv(path,sep=None,engine="python",encoding=enc,on_bad_lines="skip")
                    else:
                        df=pd.read_csv(path,sep=sep,engine="python",encoding=enc,on_bad_lines="skip")
                    if df is not None and df.shape[1]>=1:
                        break
                except Exception as e:
                    last=e
                    df=None
            if df is not None and df.shape[1]>=1:
                break
        if df is None:
            raise RuntimeError(repr(last) if last else f"READ_FAILED::{path}")
        cols=[]
        seen={}
        for c in df.columns:
            base="".join(ch.lower() if ch.isalnum() else "_" for ch in str(c)).strip("_")
            if not base:
                base="col"
            k=base
            i=2
            while k in seen:
                k=f"{base}_{i}"; i+=1
            seen[k]=1
            cols.append(k)
        df.columns=cols
        return df

    def _parse_time(self, s):
        return pd.to_datetime(s, errors="coerce")

    def _extract_year(self, nq):
        m=re.search(r"(20\d{2})", nq)
        return int(m.group(1)) if m else None

    def _extract_month(self, nq):
        for k,v in MONTHS.items():
            if k in nq:
                return v
        return None

    def _extract_threshold(self, nq):
        m=re.search(r"(dxy|vix|spx|spy|qqq|iwm|us10y|vix1d)\s*(?:superieur a|supérieur à|>)\s*(\d+(?:[\.,]\d+)?)", nq)
        if not m:
            return None,None
        asset=m.group(1).upper()
        val=float(m.group(2).replace(",","."))
        return asset,val

    def _calendar_count(self, nq):
        path=self._path("calendar_daily")
        if not path:
            return {"status":"MISSING_SOURCE","answer_type":"table","value":0,"preview":[]}
        df=self._read_csv_flex(path)
        tcol="time" if "time" in df.columns else None
        if not tcol:
            return {"status":"BAD_SOURCE","answer_type":"table","value":0,"preview":[]}
        dt=self._parse_time(df[tcol])
        df=df[dt.notna()].copy()
        df["dt"]=dt[dt.notna()]
        year=self._extract_year(nq)
        month=self._extract_month(nq)
        if year is not None:
            df=df[df["dt"].dt.year==year].copy()
        if month is not None:
            df=df[df["dt"].dt.month==month].copy()

        macro_cols=[c for c in df.columns if c in ["macro_event","macro_time_et","impact","actual","estimate","previous","earnings_major_companies","options_expiration","low_activity_period"]]
        if len(macro_cols)==0:
            count=int(len(df))
        else:
            count=int(df[macro_cols].notna().any(axis=1).sum())

        preview=df.head(20).to_dict(orient="records")
        return {
            "status":"OK",
            "answer_type":"table",
            "value":count,
            "summary":f"{count} ligne(s) calendar retenue(s)",
            "preview":preview,
            "source_files":[{"dataset_key":"calendar_daily","file_name":os.path.basename(path),"path":path,"rel_path":None,"freq_hint":"daily"}],
            "source_file_names":[os.path.basename(path)],
            "source_paths":[path],
        }

    def _asset_open_up_count(self, nq):
        asset,thr=self._extract_threshold(nq)
        if asset!="DXY":
            return None
        dxy_path=self._path("dxy_daily")
        spx_path=self._path("spx_daily")
        if not dxy_path or not spx_path:
            return {"status":"MISSING_SOURCE","answer_type":"table","value":0,"preview":[]}

        dxy=self._read_csv_flex(dxy_path)
        spx=self._read_csv_flex(spx_path)
        for df in (dxy,spx):
            if "time" not in df.columns:
                return {"status":"BAD_SOURCE","answer_type":"table","value":0,"preview":[]}

        dxy["dt"]=self._parse_time(dxy["time"])
        spx["dt"]=self._parse_time(spx["time"])
        dxy=dxy[dxy["dt"].notna()].copy()
        spx=spx[spx["dt"].notna()].copy()

        year=self._extract_year(nq)
        month=self._extract_month(nq)
        if year is not None:
            dxy=dxy[dxy["dt"].dt.year==year].copy()
            spx=spx[spx["dt"].dt.year==year].copy()
        if month is not None:
            dxy=dxy[dxy["dt"].dt.month==month].copy()
            spx=spx[spx["dt"].dt.month==month].copy()

        if "close" not in dxy.columns or "open" not in spx.columns or "close" not in spx.columns:
            return {"status":"BAD_SOURCE","answer_type":"table","value":0,"preview":[]}

        dxy["close"]=pd.to_numeric(dxy["close"],errors="coerce")
        spx["open"]=pd.to_numeric(spx["open"],errors="coerce")
        spx["close"]=pd.to_numeric(spx["close"],errors="coerce")

        dxy["date_key"]=dxy["dt"].dt.strftime("%Y-%m-%d")
        spx["date_key"]=spx["dt"].dt.strftime("%Y-%m-%d")

        merged=dxy[["date_key","close"]].rename(columns={"close":"dxy_close"}).merge(
            spx[["date_key","open","close"]].rename(columns={"close":"spx_close"}),
            on="date_key", how="inner"
        )
        merged=merged[(merged["dxy_close"].notna())&(merged["open"].notna())&(merged["spx_close"].notna())].copy()
        merged["spx_open_up"]=(merged["open"]>merged["spx_close"].shift(1)).astype(float)
        merged=merged[merged["spx_open_up"].notna()].copy()
        merged=merged[merged["dxy_close"]>thr].copy()

        count=int((merged["spx_open_up"]==1).sum())
        total=int(len(merged))
        preview=merged.head(20).to_dict(orient="records")
        return {
            "status":"OK",
            "answer_type":"table",
            "value":count,
            "summary":f"{count} séance(s) SPX ouverture haussière sur {total} cas retenus",
            "preview":preview,
            "source_files":[
                {"dataset_key":"dxy_daily","file_name":os.path.basename(dxy_path),"path":dxy_path,"rel_path":None,"freq_hint":"daily"},
                {"dataset_key":"spx_daily","file_name":os.path.basename(spx_path),"path":spx_path,"rel_path":None,"freq_hint":"daily"},
            ],
            "source_file_names":[os.path.basename(dxy_path),os.path.basename(spx_path)],
            "source_paths":[dxy_path,spx_path],
        }

    def run(self, q, preview_rows=20):
        nq=_nrm(q)

        if "annonces economiques" in nq or "annonces économiques" in nq:
            return self._calendar_count(nq)

        res=self._asset_open_up_count(nq)
        if res is not None:
            return res

        return {
            "status":"NO_SIMPLE_FR_MATCH",
            "answer_type":"table",
            "value":0,
            "preview":[]
        }
