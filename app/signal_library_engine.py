import os,re,json,unicodedata

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

class SignalLibraryEngine:
    def __init__(self, registry_path):
        self.registry_path=registry_path
        self.registry=self._load_registry()

    def _load_registry(self):
        if not os.path.exists(self.registry_path):
            return {"signals":[]}
        with open(self.registry_path,"r",encoding="utf-8") as f:
            return json.load(f)

    def refresh(self):
        self.registry=self._load_registry()

    def can_handle(self,q):
        nq=_nrm(q)
        keys=[
            "liste les signaux","list signals","signal library","bibliotheque des signaux",
            "bibliothèque des signaux","quels signaux","what signals","montre les signaux"
        ]
        return any(k in nq for k in keys)

    def resolve_signal_ids(self,q):
        nq=_nrm(q)
        out=[]
        for sig in self.registry.get("signals",[]):
            aliases=[sig.get("signal_id","")] + sig.get("aliases",[])
            if any(_nrm(a) in nq for a in aliases if a):
                out.append(sig["signal_id"])
        return sorted(set(out))

    def run(self,q,preview_rows=20):
        self.refresh()
        signals=self.registry.get("signals",[])
        resolved=self.resolve_signal_ids(q)

        if resolved:
            rows=[s for s in signals if s.get("signal_id") in resolved]
            return {
                "status":"OK",
                "answer_type":"table",
                "value":int(len(rows)),
                "resolved_signal_ids":resolved,
                "preview":rows[:preview_rows]
            }

        rows=signals[:preview_rows]
        return {
            "status":"OK",
            "answer_type":"table",
            "value":int(len(signals)),
            "resolved_signal_ids":[],
            "preview":rows
        }
