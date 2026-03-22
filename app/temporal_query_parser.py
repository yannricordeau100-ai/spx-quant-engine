import os,re,json,unicodedata

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

class TemporalQueryParser:
    def __init__(self,synonyms):
        self.synonyms=synonyms

    def _find_assets(self,text):
        nt=_nrm(text); out=[]
        for asset,aliases in self.synonyms["assets"].items():
            if any(_nrm(a) in nt for a in aliases):
                out.append(asset)
        return out

    def _find_connectors(self,text):
        nt=_nrm(text)
        found=[]
        for label,vals in [
            ("AND",self.synonyms["connectors_and"]),
            ("OR",self.synonyms["connectors_or"]),
            ("IF",self.synonyms["if_then"]),
            ("THEN",self.synonyms["then"]),
        ]:
            if any(v.strip() in nt for v in vals):
                found.append(label)
        return found

    def _parse_between(self,text):
        nt=_nrm(text)
        out=[]
        for pat in self.synonyms["time_words"]["between"]:
            for m in re.finditer(pat,nt):
                g=m.groups()
                if len(g)>=4:
                    h1=int(g[0]); m1=int(g[1] or 0); h2=int(g[2]); m2=int(g[3] or 0)
                    out.append({"type":"between","start":f"{h1:02d}:{m1:02d}","end":f"{h2:02d}:{m2:02d}"})
        return out

    def _parse_at(self,text):
        nt=_nrm(text)
        out=[]
        for pat in self.synonyms["time_words"]["at"]:
            for m in re.finditer(pat,nt):
                gs=m.groups()
                if "pm" in pat:
                    h=int(gs[0]); h=12 if h==12 else h+12
                    out.append({"type":"at","time":f"{h:02d}:00"})
                elif "am" in pat:
                    h=int(gs[0]); h=0 if h==12 else h
                    out.append({"type":"at","time":f"{h:02d}:00"})
                else:
                    h=int(gs[0]); mm=int(gs[1] or 0)
                    out.append({"type":"at","time":f"{h:02d}:{mm:02d}"})
        return out

    def _parse_relative(self,text):
        nt=_nrm(text); out=[]
        for label,key in [
            ("premarket","premarket"),
            ("previous_day","previous_day"),
            ("next_day","next_day"),
            ("next_session","next_session"),
            ("morning","morning"),
        ]:
            for pat in self.synonyms["time_words"][key]:
                if re.search(pat,nt):
                    out.append(label); break
        return sorted(set(out))

    def _parse_horizons(self,text):
        nt=_nrm(text); out=[]
        for pat in self.synonyms["time_words"]["horizon_minutes"]:
            for m in re.finditer(pat,nt):
                out.append({"type":"minutes","value":int(m.group(1))})
        for pat in self.synonyms["time_words"]["horizon_bars"]:
            for m in re.finditer(pat,nt):
                out.append({"type":"bars","value":int(m.group(1))})
        return out

    def _split_if_then(self,text):
        nt=_nrm(text)
        blocks={"condition_block":nt,"target_block":None}
        for sep in [" alors "," then "]:
            if sep in nt:
                a,b=nt.split(sep,1)
                blocks["condition_block"]=a
                blocks["target_block"]=b
                break
        return blocks

    def parse(self,question):
        assets=self._find_assets(question)
        connectors=self._find_connectors(question)
        between=self._parse_between(question)
        at_times=self._parse_at(question)
        relative=self._parse_relative(question)
        horizons=self._parse_horizons(question)
        blocks=self._split_if_then(question)

        complexity_flags=[]
        if len(assets)>=2: complexity_flags.append("MULTI_ASSET")
        if between: complexity_flags.append("INTRADAY_WINDOW")
        if at_times: complexity_flags.append("POINT_IN_TIME")
        if relative: complexity_flags.append("RELATIVE_TIME")
        if horizons: complexity_flags.append("HORIZON")
        if "AND" in connectors or "OR" in connectors: complexity_flags.append("MULTI_CONDITION")
        if "IF" in connectors and "THEN" in connectors: complexity_flags.append("IF_THEN")

        executable_level="FOUNDATION_ONLY"
        if between or at_times or relative or horizons:
            executable_level="NEEDS_TEMPORAL_EXECUTOR"

        return {
            "question":question,
            "assets_detected":assets,
            "connectors_detected":connectors,
            "between_windows":between,
            "point_times":at_times,
            "relative_time_refs":relative,
            "horizons":horizons,
            "condition_block":blocks["condition_block"],
            "target_block":blocks["target_block"],
            "complexity_flags":complexity_flags,
            "executable_level":executable_level,
        }
