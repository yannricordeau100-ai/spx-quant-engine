import os, re, json, unicodedata
import pandas as pd

MONTHS={
    "janvier":1,"fevrier":2,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"aout":8,"août":8,"septembre":9,"octobre":10,"novembre":11,"decembre":12,"décembre":12
}

FAMILY_KEYWORDS={
    "vix1d_best":["vix1d","vix 1d"],
    "vix9d_daily":["vix9d","vix 9d"],
    "vvix_daily":["vvix"],
    "vix_daily":["vix"],
    "spx_daily":["spx","s&p 500","s&p500","s&p"],
    "spy_daily":["spy"],
    "qqq_daily":["qqq"],
    "iwm_daily":["iwm"],
    "dxy_daily":["dxy","dollar index","dollar"],
    "us10y_daily":["us10y","10y","10 ans us","taux us 10 ans","10 years"],
    "gold_daily":["gold","or"],
    "oil_best":["oil","petrole","pétrole","crude"],
    "calendar_daily":["calendar","calendrier","macro","annonces economiques","annonces économiques","calendrier economique","calendrier économique"],
    "tick_best":["tick"],
}

def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _nrm(s):
    s="" if s is None else str(s)
    s=s.lower()
    s=_strip_accents(s)
    s=s.replace("’","'").replace("`","'")
    s=s.replace("cloturee","cloture").replace("cloturee","cloture")
    s=s.replace("cloture","cloture").replace("cloture","cloture")
    s=s.replace("cloture","cloture")
    s=s.replace("cloture","cloture")
    s=s.replace("cloturee","cloture").replace("cloturee","cloture")
    s=s.replace("cloture","cloture")
    s=s.replace("cloture","cloture")
    s=s.replace("cloture","cloture")
    s=s.replace("etait","etait")
    s=s.replace("superieure","superieur").replace("inferieure","inferieur")
    s=s.replace("jours de jours","jours de bourse")
    s=s.replace("jour de jours","jours de bourse")
    s=s.replace("jour de bourse","jours de bourse")
    s=s.replace("a quand remonte","quand remonte")
    s=s.replace("en dessous de","inferieur a")
    s=s.replace("au dessous de","inferieur a")
    s=s.replace("au-dessous de","inferieur a")
    s=s.replace("au dessus de","superieur a")
    s=s.replace("au-dessus de","superieur a")
    s=re.sub(r"[^a-z0-9%+<>=/.' -]+"," ",s)
    s=re.sub(r"\s+"," ",s).strip()
    return s

def _contains_term(nq, term):
    return re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", nq) is not None

def _read_csv_flex(path):
    last=None
    for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
        for sep in (None,",",";","\t","|"):
            try:
                if sep is None:
                    df=pd.read_csv(path,sep=None,engine="python",encoding=enc,on_bad_lines="skip")
                else:
                    df=pd.read_csv(path,sep=sep,engine="python",encoding=enc,on_bad_lines="skip")
                if df is not None and df.shape[1]>=1:
                    return df
            except Exception as e:
                last=e
    raise RuntimeError(repr(last) if last else f"CSV_READ_FAILED::{path}")

def _norm_cols(df):
    cols=[]; seen={}
    for c in df.columns:
        base="".join(ch.lower() if ch.isalnum() else "_" for ch in str(c)).strip("_")
        if not base:
            base="col"
        k=base; i=2
        while k in seen:
            k=f"{base}_{i}"; i+=1
        seen[k]=1
        cols.append(k)
    df=df.copy()
    df.columns=cols
    return df

def _find_col(df, candidates):
    cols=list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    for col in cols:
        low=str(col).lower()
        for c in candidates:
            if c in low:
                return col
    return None

def _coerce_time(df):
    tcol=_find_col(df,["time","date","datetime","timestamp"])
    if tcol is None:
        return df,None
    dt=pd.to_datetime(df[tcol],errors="coerce")
    out=df[dt.notna()].copy()
    if out.empty:
        return df,None
    out["_dt"]=dt[dt.notna()]
    out["date_key"]=out["_dt"].dt.strftime("%Y-%m-%d")
    out["year"]=out["_dt"].dt.year
    out["month"]=out["_dt"].dt.month
    out["day"]=out["_dt"].dt.day
    out["ym"]=out["_dt"].dt.strftime("%Y-%m")
    return out,tcol

def _extract_years(nq):
    return [int(x) for x in re.findall(r"(20\d{2})", nq)]

def _extract_month(nq):
    for k,v in MONTHS.items():
        if k in nq:
            return v
    return None

def _extract_between_dates(nq):
    patt=r"entre(?: le)? (\d{1,2}) ([a-z]+)(?: (\d{4}))? et(?: le)? (\d{1,2}) ([a-z]+)(?: (\d{4}))?"
    m=re.search(patt,nq)
    if not m:
        return None,None
    d1=int(m.group(1)); m1=MONTHS.get(m.group(2)); y1=int(m.group(3)) if m.group(3) else None
    d2=int(m.group(4)); m2=MONTHS.get(m.group(5)); y2=int(m.group(6)) if m.group(6) else y1
    if None in [m1,m2]:
        return None,None
    if y1 is None:
        years=_extract_years(nq)
        y1=years[0] if years else pd.Timestamp.utcnow().year
        y2=y2 or y1
    try:
        return pd.Timestamp(year=y1,month=m1,day=d1), pd.Timestamp(year=y2,month=m2,day=d2)
    except Exception:
        return None,None

def _extract_threshold(nq):
    m=re.search(r"(>=|<=|>|<|superieur a|inferieur a)\s*(-?\d+(?:[.,]\d+)?)",nq)
    if not m:
        return None
    op,val=m.group(1),float(m.group(2).replace(",","."))
    if op=="superieur a": op=">"
    if op=="inferieur a": op="<"
    return op,val

def _extract_percent_threshold(nq):
    m=re.search(r"(plus de|moins de|superieur a|inferieur a|>|<)\s*(\d+(?:[.,]\d+)?)\s*%",nq)
    if not m:
        return None
    raw,val=m.group(1),float(m.group(2).replace(",","."))
    return (">",val/100.0) if raw in ["plus de","superieur a",">"] else ("<",val/100.0)

def _wants_latest(nq):
    return any(x in nq for x in ["derniere date","derniere fois","plus recente","quand remonte","quand etait la derniere fois","derniere occurrence"])

def _wants_earliest(nq):
    return any(x in nq for x in ["plus ancienne","premiere date","plus vieille","date la plus ancienne","premiere occurrence"])

def _wants_count(nq):
    return any(x in nq for x in ["combien","nombre de","combien de fois"])

def _wants_same_month_frequency(nq):
    return ("meme mois" in nq) and any(x in nq for x in ["4 performances","4 jour","4 jours","quatre"])

def _count_requested(nq, default=1):
    nums=[int(x) for x in re.findall(r"\b(\d+)\b", nq)]
    small=[x for x in nums if x<100]
    return small[0] if small else default

def _calendar_like(df):
    cols=set(df.columns)
    hits=sum(1 for c in ["macro_event","macro_time_et","impact","actual","estimate","previous","earnings_major_companies","options_expiration","low_activity_period"] if c in cols)
    return hits>=2

class GenericQueryEngine:
    def __init__(self, project_root):
        self.project_root=project_root
        with open(os.path.join(project_root,"processed","ETAPE170D_CANONICAL_SOURCE_REGISTRY.json"),"r",encoding="utf-8") as f:
            self.registry=(json.load(f).get("datasets",{}) or {})

    def _meta(self, ds_key):
        meta=self.registry.get(ds_key)
        if not meta:
            raise RuntimeError(f"DATASET_NOT_FOUND::{ds_key}")
        path=meta.get("path")
        if not path or not os.path.exists(path):
            raise RuntimeError(f"DATASET_PATH_MISSING::{ds_key}")
        out=dict(meta)
        out["dataset_key"]=ds_key
        return out

    def _attach_sources(self, result, metas):
        src=[]
        for m in metas:
            src.append({
                "dataset_key":m.get("dataset_key"),
                "file_name":m.get("file_name"),
                "path":m.get("path"),
                "rel_path":m.get("rel_path"),
                "freq_hint":m.get("freq_hint"),
            })
        result["source_files"]=src
        result["source_file_names"]=[x["file_name"] for x in src]
        result["source_paths"]=[x["path"] for x in src]
        return result

    def _load_dataset(self, ds_key):
        meta=self._meta(ds_key)
        df=_read_csv_flex(meta["path"])
        df=_norm_cols(df)
        df,tcol=_coerce_time(df)
        return meta,df,tcol

    def _candidate_datasets(self, question):
        nq=_nrm(question)
        hits=[]
        for ds_key, terms in FAMILY_KEYWORDS.items():
            if ds_key not in self.registry:
                continue
            if any(_contains_term(nq,t) for t in terms):
                hits.append(ds_key)
        if _contains_term(nq,"vix") and not _contains_term(nq,"vvix"):
            hits=[x for x in hits if x!="vvix_daily"]
            if "vix_daily" in self.registry and "vix_daily" not in hits:
                hits.insert(0,"vix_daily")
        if _contains_term(nq,"vvix"):
            hits=[x for x in hits if x!="vix_daily"]
            if "vvix_daily" in self.registry and "vvix_daily" not in hits:
                hits.insert(0,"vvix_daily")
        if hits:
            dedup=[]; seen=set()
            for x in hits:
                if x not in seen:
                    seen.add(x); dedup.append(x)
            return dedup
        if any(x in nq for x in ["calendrier","calendar","annonces economiques","macro"]):
            return ["calendar_daily"] if "calendar_daily" in self.registry else []
        return [x for x in self.registry.keys() if x.endswith("_daily") or x.endswith("_best") or "calendar" in x]

    def _choose_numeric_col(self, df, nq):
        pref=[]
        if "cloture" in nq or "close" in nq:
            pref+=["close"]
        if "ouverture" in nq or "open" in nq:
            pref+=["open"]
        if "plus haut" in nq or "high" in nq:
            pref+=["high"]
        if "plus bas" in nq or "inferieur" in nq:
            pref+=["low","close"]
        if "vix1d/vix" in nq:
            pref+=["ratio","value"]
        for p in pref:
            c=_find_col(df,[p])
            if c is not None:
                return c
        for c in ["close","open","high","low","value","ratio"]:
            if c in df.columns:
                return c
        for c in df.columns:
            if c in ["year","month","day","ym","date_key","time","_dt"]:
                continue
            s=pd.to_numeric(df[c],errors="coerce")
            if s.notna().sum()>=max(5,int(len(df)*0.2) if len(df)>0 else 0):
                return c
        return None

    def can_handle(self, q):
        return len(_nrm(q))>0

    def _apply_time_filter(self, df, nq):
        out=df.copy()
        if "_dt" not in out.columns:
            return out
        years=_extract_years(nq)
        month=_extract_month(nq)
        s,e=_extract_between_dates(nq)
        if s is not None and e is not None:
            return out[(out["_dt"]>=s)&(out["_dt"]<=e)].copy()
        if len(years)==1 and "3 dernieres annees" not in nq and "sur les 3" not in nq:
            out=out[out["year"]==years[0]].copy()
        if month is not None:
            out=out[out["month"]==month].copy()
        return out

    def _answer_calendar(self, nq, meta, df):
        if "_dt" not in df.columns:
            return None
        dff=self._apply_time_filter(df,nq)
        if _wants_earliest(nq):
            row=dff.sort_values("_dt").head(1)
            if row.empty: return None
            dt=row["_dt"].iloc[0]
            return self._attach_sources({"status":"OK","answer_type":"answer","value":dt.strftime("%Y-%m-%d"),"answer":f"La date la plus ancienne est {dt.strftime('%Y-%m-%d')}.","preview":row.head(10).to_dict(orient="records")},[meta])
        if _wants_latest(nq):
            row=dff.sort_values("_dt").tail(1)
            if row.empty: return None
            dt=row["_dt"].iloc[0]
            return self._attach_sources({"status":"OK","answer_type":"answer","value":dt.strftime("%Y-%m-%d"),"answer":f"La date la plus récente est {dt.strftime('%Y-%m-%d')}.","preview":row.head(10).to_dict(orient="records")},[meta])
        if _wants_count(nq):
            macro_cols=[c for c in dff.columns if c in ["macro_event","macro_time_et","impact","actual","estimate","previous","earnings_major_companies","options_expiration","low_activity_period"]]
            count=int(dff[macro_cols].notna().any(axis=1).sum()) if macro_cols else int(len(dff))
            return self._attach_sources({"status":"OK","answer_type":"answer","value":count,"answer":f"{count} occurrence(s) retenue(s).","preview":dff.head(20).to_dict(orient="records")},[meta])
        return None

    def _answer_days_count(self, nq, meta, df):
        if "_dt" not in df.columns or not _wants_count(nq):
            return None
        if any(t in nq for t in ["jours de bourse","nombre de jours","combien de jours","jour de jours"]):
            dff=self._apply_time_filter(df,nq)
            count=int(dff["date_key"].nunique())
            return self._attach_sources({"status":"OK","answer_type":"answer","value":count,"answer":f"{count} jour(s) retenu(s).","preview":dff.head(20).to_dict(orient="records")},[meta])
        return None

    def _answer_same_month_perf(self, nq, meta, df):
        if "_dt" not in df.columns or not _wants_same_month_frequency(nq):
            return None
        close_col=_find_col(df,["close"])
        if close_col is None:
            return None
        dff=df.copy().sort_values("_dt")
        dff[close_col]=pd.to_numeric(dff[close_col],errors="coerce")
        dff=dff[dff[close_col].notna()].copy()
        dff["ret1"]=dff[close_col].pct_change()
        pct=_extract_percent_threshold(nq)
        if pct is None:
            return None
        op,val=pct
        dff["hit"]=(dff["ret1"]>val) if op==">" else (dff["ret1"]<val)
        agg=dff.groupby("ym",as_index=False)["hit"].sum().rename(columns={"hit":"n_hits"})
        need=_count_requested(nq,default=4)
        agg=agg[agg["n_hits"]>=need].copy()
        if agg.empty:
            return self._attach_sources({"status":"OK_EMPTY","answer_type":"answer","value":None,"answer":"Aucun mois correspondant trouvé.","preview":[]},[meta])
        ym=agg["ym"].iloc[-1]
        return self._attach_sources({"status":"OK","answer_type":"answer","value":ym,"answer":f"La dernière occurrence retenue remonte au mois {ym}.","preview":agg.tail(20).to_dict(orient="records")},[meta])

    def _answer_threshold_query(self, nq, meta, df):
        if "_dt" not in df.columns:
            return None
        th=_extract_threshold(nq)
        if th is None:
            return None
        op,val=th
        col=self._choose_numeric_col(df,nq)
        if col is None:
            return None
        dff=df.copy()
        dff[col]=pd.to_numeric(dff[col],errors="coerce")
        dff=dff[dff[col].notna()].copy()
        dff=self._apply_time_filter(dff,nq)
        if op==">": dff=dff[dff[col]>val].copy()
        elif op=="<": dff=dff[dff[col]<val].copy()
        elif op==">=": dff=dff[dff[col]>=val].copy()
        elif op=="<=": dff=dff[dff[col]<=val].copy()

        if dff.empty:
            return self._attach_sources({"status":"OK_EMPTY","answer_type":"answer","value":None,"answer":"Aucun cas correspondant trouvé.","preview":[]},[meta])

        if _wants_latest(nq):
            row=dff.sort_values("_dt").tail(1)
            dt=row["_dt"].iloc[0]; num=row[col].iloc[0]
            return self._attach_sources({"status":"OK","answer_type":"answer","value":dt.strftime("%Y-%m-%d"),"answer":f"La dernière date retenue est {dt.strftime('%Y-%m-%d')} ({col}={num:.4g}).","preview":row.head(10).to_dict(orient="records")},[meta])

        if _wants_earliest(nq):
            row=dff.sort_values("_dt").head(1)
            dt=row["_dt"].iloc[0]; num=row[col].iloc[0]
            return self._attach_sources({"status":"OK","answer_type":"answer","value":dt.strftime("%Y-%m-%d"),"answer":f"La première date retenue est {dt.strftime('%Y-%m-%d')} ({col}={num:.4g}).","preview":row.head(10).to_dict(orient="records")},[meta])

        if _wants_count(nq):
            count=int(len(dff))
            return self._attach_sources({"status":"OK","answer_type":"answer","value":count,"answer":f"{count} cas retenu(s).","preview":dff.head(20).to_dict(orient="records")},[meta])

        return None

    def _answer_derived_filtered_ratio(self, nq):
        if "vix1d/vix" not in nq and "vix1d / vix" not in nq:
            return None
        ratio_csv=os.path.join(self.project_root,"processed","DERIVED_FEATURE_STORE","csv","ratio__vix1d_best__vix_daily__3y__a1479448d9868fccca7c35de.csv")
        if not os.path.exists(ratio_csv):
            return None
        df=_read_csv_flex(ratio_csv)
        df=_norm_cols(df)
        df,tcol=_coerce_time(df)
        if "_dt" not in df.columns:
            return None
        val_col=self._choose_numeric_col(df,nq)
        if val_col is None:
            return None
        df[val_col]=pd.to_numeric(df[val_col],errors="coerce")
        dff=df[df[val_col].notna()].copy()
        dff=self._apply_time_filter(dff,nq)
        th=_extract_threshold(nq)
        if th is not None:
            op,val=th
            if op==">": dff=dff[dff[val_col]>val].copy()
            elif op=="<": dff=dff[dff[val_col]<val].copy()
            elif op==">=": dff=dff[dff[val_col]>=val].copy()
            elif op=="<=": dff=dff[dff[val_col]<=val].copy()

        src=[
            {"dataset_key":"vix1d_best","file_name":"VIX1D_30min.csv","path":self.registry.get("vix1d_best",{}).get("path"),"rel_path":self.registry.get("vix1d_best",{}).get("rel_path"),"freq_hint":"30min"},
            {"dataset_key":"vix_daily","file_name":self.registry.get("vix_daily",{}).get("file_name"),"path":self.registry.get("vix_daily",{}).get("path"),"rel_path":self.registry.get("vix_daily",{}).get("rel_path"),"freq_hint":"daily"},
        ]

        if _wants_count(nq):
            count=int(len(dff))
            return {"status":"OK","answer_type":"answer","value":count,"answer":f"{count} occurrence(s) retenue(s).","preview":dff.head(20).to_dict(orient="records"),"source_files":src,"source_file_names":[x["file_name"] for x in src],"source_paths":[x["path"] for x in src]}
        return None

    def run(self, q, preview_rows=20):
        nq=_nrm(q)

        res=self._answer_derived_filtered_ratio(nq)
        if res is not None:
            return res

        cands=self._candidate_datasets(q)
        for ds_key in cands:
            try:
                meta,df,tcol=self._load_dataset(ds_key)
            except Exception:
                continue

            if _calendar_like(df) or ds_key=="calendar_daily":
                res=self._answer_calendar(nq,meta,df)
                if res is not None:
                    return res

            res=self._answer_same_month_perf(nq,meta,df)
            if res is not None:
                return res

            res=self._answer_days_count(nq,meta,df)
            if res is not None:
                return res

            res=self._answer_threshold_query(nq,meta,df)
            if res is not None:
                return res

        return {"status":"NO_GENERIC_MATCH","answer_type":"answer","value":None,"answer":"Je n’ai pas encore su interpréter cette question de façon fiable.","preview":[]}
