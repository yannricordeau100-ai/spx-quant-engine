import os,re,json,unicodedata,pandas as pd

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _slug(s): return re.sub(r"[^a-z0-9]+","_",_nrm(s)).strip("_")

class NaturalLanguageQueryTranslator:
    def __init__(self,base_dir,synonyms,preferred_columns,direct_condition_map):
        self.base_dir=base_dir
        self.synonyms=synonyms
        self.preferred_columns=preferred_columns
        self.direct_condition_map=direct_condition_map
        self.engine_cols=self._load_engine_cols()

    def _load_engine_cols(self):
        path=os.path.join(self.base_dir,"main_engine_enriched_p2.csv")
        for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
            for sep in (",",";","\\t","|",None):
                try:
                    kw={"encoding":enc,"on_bad_lines":"skip"}
                    if sep is None: df=pd.read_csv(path,sep=None,engine="python",**kw)
                    else: df=pd.read_csv(path,sep=sep,engine="python",**kw)
                    if df is not None and df.shape[1]>=1:
                        cols=[]; seen={}
                        for c in df.columns:
                            base=_slug(c) or "col"; k=base; i=2
                            while k in seen:
                                k=f"{base}_{i}"; i+=1
                            seen[k]=1; cols.append(k)
                        return cols
                except: pass
        return []

    def _contains_any(self,text,phrases):
        nt=_nrm(text)
        return any(_nrm(p) in nt for p in phrases)

    def _find_assets(self,text):
        nt=_nrm(text); found=[]
        for asset,aliases in self.synonyms["assets"].items():
            if any(_nrm(a) in nt for a in aliases):
                found.append(asset)
        return found

    def _find_mode(self,text):
        nt=_nrm(text)
        if any(_nrm(x) in nt for x in self.synonyms["concepts"]["rows"]): return "rows"
        if any(_nrm(x) in nt for x in self.synonyms["concepts"]["count"]): return "count"
        if any(_nrm(x) in nt for x in self.synonyms["concepts"]["mean"]): return "mean"
        if "quelle est la valeur" in nt or "what is the value" in nt or "quel est le prix" in nt or "what is the price" in nt:
            return "mean"
        return "mean"

    def _find_concept(self,text):
        nt=_nrm(text)
        if any(_nrm(x) in nt for x in self.synonyms["concepts"]["ret"]): return "ret"
        if any(_nrm(x) in nt for x in self.synonyms["concepts"]["range"]): return "range"
        if any(_nrm(x) in nt for x in self.synonyms["concepts"]["open"]): return "open"
        if any(_nrm(x) in nt for x in self.synonyms["concepts"]["close"]): return "close"
        return "close"

    def _pick_metric(self,assets,concept):
        for asset in assets:
            for col in self.preferred_columns.get((asset,concept),[]):
                if col in self.engine_cols:
                    return col,asset,concept
        # fallback close then ret
        for asset in assets:
            for alt_concept in ("close","ret","range","open"):
                for col in self.preferred_columns.get((asset,alt_concept),[]):
                    if col in self.engine_cols:
                        return col,asset,alt_concept
        return None,None,None

    def _direct_conditions(self,text):
        nt=_nrm(text); conds=[]; matched_labels=[]
        for expr,aliases in self.direct_condition_map:
            if any(_nrm(a) in nt for a in aliases):
                conds.append(expr)
                matched_labels.extend(aliases)
        return conds,matched_labels

    def _extract_numeric_condition(self,text,assets,default_concept="close"):
        nt=_nrm(text)
        ops=[
            (r"(?:au dessus de|above|greater than|superieur a|supérieur à|>\\s*)(-?\\d+(?:[\\.,]\\d+)?)",">"),
            (r"(?:en dessous de|below|less than|inferieur a|inférieur à|<\\s*)(-?\\d+(?:[\\.,]\\d+)?)","<"),
            (r"(?:egal a|égal à|equal to|=\\s*)(-?\\d+(?:[\\.,]\\d+)?)","="),
        ]
        for asset in assets:
            metric,_,metric_concept=self._pick_metric([asset],default_concept)
            if metric is None:
                continue
            for pat,op in ops:
                m=re.search(pat,nt)
                if m:
                    val=m.group(1).replace(",",".")
                    return f"{metric} {op} {val}",metric
        return None,None

    def _unsupported_fragments(self,text):
        nt=_nrm(text); hits=[]
        for frag in self.synonyms.get("time_fragments_unsupported",[]):
            if _nrm(frag) in nt:
                hits.append(frag)
        return sorted(set(hits))

    def translate(self,question):
        nt=_nrm(question)
        assets=self._find_assets(question)
        mode=self._find_mode(question)
        concept=self._find_concept(question)
        unsupported=self._unsupported_fragments(question)

        conds,_labels=self._direct_conditions(question)
        num_cond,num_metric=self._extract_numeric_condition(question,assets,default_concept="close")
        if num_cond:
            conds.append(num_cond)

        metric,metric_asset,metric_concept=self._pick_metric(assets,concept)

        unresolved=[]
        if not assets:
            unresolved.append("asset")
        if metric is None:
            unresolved.append("metric")

        translated=None
        if metric is not None:
            if mode=="mean":
                translated=f"moyenne de {metric}" + (f" quand {' et '.join(conds)}" if conds else "")
            elif mode=="count":
                translated=f"combien" + (f" quand {' et '.join(conds)}" if conds else "")
            elif mode=="rows":
                translated=f"montre les dates" + (f" quand {' et '.join(conds)}" if conds else "")
            else:
                translated=f"moyenne de {metric}" + (f" quand {' et '.join(conds)}" if conds else "")

        confidence="HIGH"
        if unresolved or unsupported:
            confidence="PARTIAL"

        return {
            "original_question":question,
            "normalized_question":nt,
            "mode":mode,
            "assets_detected":assets,
            "metric_concept":concept,
            "metric_column":metric,
            "metric_asset":metric_asset,
            "metric_concept_resolved":metric_concept,
            "conditions":conds,
            "unsupported_fragments":unsupported,
            "unresolved_parts":unresolved,
            "translated_query":translated,
            "confidence":confidence,
        }
