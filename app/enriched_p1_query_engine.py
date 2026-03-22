import os,re,json,unicodedata
import numpy as np,pandas as pd

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _safe(v):
    if isinstance(v,(np.integer,)): return int(v)
    if isinstance(v,(np.floating,)): return None if pd.isna(v) else float(v)
    if pd.isna(v): return None
    return v

def _records(df,n=12):
    if df is None or df.empty: return []
    return [{k:_safe(v) for k,v in r.items()} for r in df.head(n).to_dict("records")]

class EnrichedP1QueryEngine:
    def __init__(self,base_dir):
        self.base_dir=base_dir
        self.df=self._read(os.path.join(base_dir,"main_engine_enriched_p1.csv"))
        self.sem=self._read(os.path.join(base_dir,"runtime_query_semantic_dictionary.csv"))
        self.cols=list(self.df.columns)
    def _read(self,path):
        for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
            for sep in (",",";","\\t","|",None):
                try:
                    kw={"encoding":enc,"on_bad_lines":"skip"}
                    if sep is None: df=pd.read_csv(path,sep=None,engine="python",**kw)
                    else: df=pd.read_csv(path,sep=sep,engine="python",**kw)
                    if df is not None and df.shape[1]>=1:
                        cols=[]; seen={}
                        for c in df.columns:
                            base=re.sub(r"[^a-z0-9]+","_",_nrm(c)).strip("_") or "col"
                            k=base; i=2
                            while k in seen:
                                k=f"{base}_{i}"; i+=1
                            seen[k]=1; cols.append(k)
                        df.columns=cols
                        return df
                except: pass
        raise RuntimeError(path)
    def _resolve_columns(self,q):
        nq=_nrm(q); hits=[]
        for r in self.sem.to_dict("records"):
            try: aliases=json.loads(r.get("aliases_json","[]")) if isinstance(r.get("aliases_json"),str) else []
            except: aliases=[]
            score=0
            for a in aliases+[r.get("column_name","")]:
                aa=_nrm(a)
                if aa and aa in nq: score=max(score,len(aa))
            if score>0: hits.append((score,r["column_name"]))
        hits=sorted(hits,key=lambda x:(-x[0],x[1]))
        out=[]; seen=set()
        for _,c in hits:
            if c not in seen:
                seen.add(c); out.append(c)
        return out
    def _extract_conditions(self,q,columns):
        nq=_nrm(q); conds=[]
        for c in columns:
            cc=_nrm(c)
            m=re.search(rf"{re.escape(cc)}\s*(>=|<=|=|>|<)\s*(-?\d+(?:[.,]\d+)?)",nq)
            if m:
                conds.append((c,m.group(1),float(m.group(2).replace(",","."))))
                continue
            if any(x in nq for x in [cc+" = 1",cc+"==1",cc+" true",cc+" actif",cc+" yes"]):
                conds.append((c,"=",1.0))
        if not conds:
            direct_flags=["is_low_activity_day","is_options_expiration_day","is_earnings_top_day","has_macro_event","macro_impact_high","macro_impact_medium","macro_impact_low"]
            for c in direct_flags:
                if c in columns and _nrm(c) in nq:
                    conds.append((c,"=",1.0))
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
    def _pick_metric(self,q,resolved):
        nq=_nrm(q)
        numeric=[c for c in resolved if c in self.df.columns and pd.api.types.is_numeric_dtype(self.df[c])]
        if numeric: return numeric[0]
        defaults=["gold_daily_ret","oil_5m_ret","gold_close","rows_calendar_day"]
        for c in defaults:
            if c in self.df.columns and _nrm(c) in nq: return c
        for c in defaults:
            if c in self.df.columns: return c
        return None
    def answer(self,question,preview_rows=12):
        resolved=self._resolve_columns(question)
        conds=self._extract_conditions(question,resolved)
        filt=self._apply_conditions(self.df,conds) if conds else self.df
        nq=_nrm(question)
        metric=self._pick_metric(question,resolved)
        out={
            "status":"OK",
            "resolved_columns":resolved[:20],
            "conditions":conds,
            "row_count_total":int(len(self.df)),
            "row_count_filtered":int(len(filt))
        }
        if any(x in nq for x in ["combien","count","occur","occurrence","nombre de fois","how many"]):
            out["answer_type"]="count"; out["value"]=int(len(filt)); return out
        if metric and any(x in nq for x in ["moyenne","average","mean"]):
            s=pd.to_numeric(filt[metric],errors="coerce").dropna()
            out["answer_type"]="mean"; out["metric"]=metric; out["value"]=None if s.empty else float(s.mean()); return out
        if metric and any(x in nq for x in ["mediane","median"]):
            s=pd.to_numeric(filt[metric],errors="coerce").dropna()
            out["answer_type"]="median"; out["metric"]=metric; out["value"]=None if s.empty else float(s.median()); return out
        if metric and any(x in nq for x in ["min","minimum"]):
            s=pd.to_numeric(filt[metric],errors="coerce").dropna()
            out["answer_type"]="min"; out["metric"]=metric; out["value"]=None if s.empty else float(s.min()); return out
        if metric and any(x in nq for x in ["max","maximum"]):
            s=pd.to_numeric(filt[metric],errors="coerce").dropna()
            out["answer_type"]="max"; out["metric"]=metric; out["value"]=None if s.empty else float(s.max()); return out
        if any(x in nq for x in ["montre","show","liste","list","dates","quelles dates","which dates","preview","apercu","aperçu"]):
            keep=[c for c in ["date_key","time"]+resolved if c in filt.columns]
            view=filt[keep].copy() if keep else filt.copy()
            out["answer_type"]="rows"; out["preview"]=_records(view,preview_rows); return out
        out["answer_type"]="summary"
        keep=[c for c in ["date_key","time"]+resolved if c in filt.columns]
        view=filt[keep].copy() if keep else filt.copy()
        out["preview"]=_records(view,preview_rows)
        out["message"]="Question partiellement comprise. Colonnes résolues et conditions explicites appliquées."
        return out
