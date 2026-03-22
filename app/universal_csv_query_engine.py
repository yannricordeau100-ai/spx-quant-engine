import os,re,json,unicodedata
import numpy as np,pandas as pd

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"[^a-z0-9]+"," ",s).strip()
    return s

def _slug(s): return _nrm(s).replace(" ","_")

def _canon(cols):
    out=[]; seen={}
    for c in cols:
        b=_slug(c) or "col"; k=b; i=2
        while k in seen:
            k=f"{b}_{i}"; i+=1
        seen[k]=1; out.append(k)
    return out

def _read(path):
    seps=[";",",","\\t","|",None]
    encs=["utf-8-sig","utf-8","cp1252","latin-1"]
    last=None
    for e in encs:
        for s in seps:
            try:
                kw={"encoding":e,"on_bad_lines":"skip"}
                if s is None: df=pd.read_csv(path,sep=None,engine="python",**kw)
                else: df=pd.read_csv(path,sep=s,engine="python",**kw)
                if df is not None and df.shape[1]>=1:
                    df.columns=_canon(df.columns)
                    return df
            except Exception as ex:
                last=ex
    raise last

class UniversalCSVQueryEngine:
    def __init__(self,base_dir):
        self.base_dir=base_dir
        self.catalog=pd.read_csv(os.path.join(base_dir,"ia_wide_source_catalog.csv"))
        self.cols=pd.read_csv(os.path.join(base_dir,"ia_wide_column_registry.csv"))
        self.ddict=pd.read_csv(os.path.join(base_dir,"ia_wide_dataset_dictionary.csv"))
        self.cache={}
    def _match_datasets(self,q):
        nq=_nrm(q); hits=[]
        for r in self.ddict.to_dict("records"):
            score=0
            try: aliases=json.loads(r.get("aliases_json","[]")) if isinstance(r.get("aliases_json"),str) else []
            except: aliases=[]
            try: tags=json.loads(r.get("tags_json","[]")) if isinstance(r.get("tags_json"),str) else []
            except: tags=[]
            for a in aliases+tags+[r.get("summary_text","")]:
                aa=_nrm(a)
                if aa and aa in nq: score+=max(1,len(aa.split()))
            if score>0: hits.append((score,r["dataset_key"]))
        if not hits:
            broad=[]
            for k in self.catalog[self.catalog["query_engine_eligible"]==True]["dataset_key"].tolist():
                if any(x in nq for x in ["market","marche","spx","spy","qqq","iwm","vix","calendar","macro","gold","oil","dxy","bond","ratio","corr","ric"]):
                    broad.append(k)
            return broad[:6]
        hits=sorted(hits,key=lambda x:(-x[0],x[1]))
        out=[]; seen=set()
        for _,k in hits:
            if k not in seen:
                seen.add(k); out.append(k)
        return out[:6]
    def _load_dataset(self,key):
        if key in self.cache: return self.cache[key]
        row=self.catalog[self.catalog["dataset_key"]==key].head(1)
        if row.empty: raise KeyError(key)
        path=row.iloc[0]["abs_path"]
        df=_read(path)
        self.cache[key]=df
        return df
    def _best_time_col(self,df):
        for c in df.columns:
            s=_slug(c)
            if s in {"time","date","datetime","timestamp","day"} or s.endswith("_time") or s.endswith("_date"):
                return c
        return None
    def _best_metric_col(self,q,keys):
        nq=_nrm(q); cands=[]
        for k in keys:
            sub=self.cols[self.cols["dataset_key"]==k]
            for c in sub["column_name"].tolist():
                cn=_nrm(c)
                if cn and cn in nq:
                    cands.append((len(cn),c))
        if cands:
            return sorted(cands,key=lambda x:-x[0])[0][1]
        prefs=["close","spot","price","last","open","high","low","value","actual","estimate","previous","impact"]
        for k in keys:
            sub=self.cols[self.cols["dataset_key"]==k]["column_name"].tolist()
            for p in prefs:
                for c in sub:
                    sc=_slug(c)
                    if sc==p or sc.endswith("_"+p):
                        return c
        return None
    def _join(self,keys):
        base=None; common=None
        for k in keys:
            df=self._load_dataset(k).copy()
            t=self._best_time_col(df)
            if t is None:
                df["__rowid__"]=np.arange(len(df)); t="__rowid__"
            else:
                try: df[t]=pd.to_datetime(df[t],errors="ignore")
                except: pass
            rename={c:f"{_slug(k)}__{c}" for c in df.columns if c!=t}
            df=df.rename(columns=rename)
            if base is None:
                base=df; common=t
            else:
                if t!=common: df=df.rename(columns={t:common})
                base=base.merge(df,on=common,how="inner")
        return base,common
    def _extract_conditions(self,q,df):
        nq=_nrm(q); conds=[]
        for c in df.columns:
            cs=_nrm(c)
            if not cs: continue
            m=re.search(rf"{re.escape(cs)}\s*(>=|<=|=|>|<)\s*(-?\d+(?:[.,]\d+)?)",nq)
            if m:
                conds.append((c,m.group(1),float(m.group(2).replace(",","."))))
        return conds
    def _apply_conditions(self,df,conds):
        out=df.copy()
        for c,op,v in conds:
            s=pd.to_numeric(out[c],errors="coerce")
            if op==">=": out=out[s>=v]
            elif op=="<=": out=out[s<=v]
            elif op==">": out=out[s>v]
            elif op=="<": out=out[s<v]
            elif op=="=": out=out[s==v]
        return out
    def answer(self,question,preview_rows=12):
        keys=self._match_datasets(question)
        if not keys:
            return {"status":"NO_DATASET_MATCH","message":"Aucun dataset candidat détecté.","matched_datasets":[]}
        df,jk=self._join(keys)
        conds=self._extract_conditions(question,df)
        filt=self._apply_conditions(df,conds) if conds else df
        nq=_nrm(question); metric=self._best_metric_col(question,keys)
        out={"status":"OK","matched_datasets":keys,"join_key":jk,"conditions":conds,"row_count_total":int(len(df)),"row_count_filtered":int(len(filt))}
        if any(x in nq for x in ["combien","how many","count","nombre de lignes","nb lignes","occur","occurrence"]):
            out["answer_type"]="count"; out["value"]=int(len(filt)); return out
        if metric and any(x in nq for x in ["moyenne","mean","average"]):
            s=pd.to_numeric(filt[metric],errors="coerce").dropna(); out["answer_type"]="mean"; out["metric"]=metric; out["value"]=None if s.empty else float(s.mean()); return out
        if metric and any(x in nq for x in ["mediane","median"]):
            s=pd.to_numeric(filt[metric],errors="coerce").dropna(); out["answer_type"]="median"; out["metric"]=metric; out["value"]=None if s.empty else float(s.median()); return out
        if metric and any(x in nq for x in ["min","minimum","plus bas"]):
            s=pd.to_numeric(filt[metric],errors="coerce").dropna(); out["answer_type"]="min"; out["metric"]=metric; out["value"]=None if s.empty else float(s.min()); return out
        if metric and any(x in nq for x in ["max","maximum","plus haut"]):
            s=pd.to_numeric(filt[metric],errors="coerce").dropna(); out["answer_type"]="max"; out["metric"]=metric; out["value"]=None if s.empty else float(s.max()); return out
        if metric and any(x in nq for x in ["std","ecart type","écart type","volatilite","volatilité"]):
            s=pd.to_numeric(filt[metric],errors="coerce").dropna(); out["answer_type"]="std"; out["metric"]=metric; out["value"]=None if s.empty else float(s.std()); return out
        if any(x in nq for x in ["montre","show","liste","list","dates","quelles dates","which dates","preview","apercu","aperçu"]):
            out["answer_type"]="rows"; out["preview"]=filt.head(preview_rows).to_dict("records"); return out
        out["answer_type"]="summary"
        out["preview"]=filt.head(preview_rows).to_dict("records")
        out["message"]="Question partiellement comprise. Datasets routés, jointure exécutée, conditions explicites appliquées."
        return out
