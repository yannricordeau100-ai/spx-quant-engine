import os, json, re, unicodedata, pandas as pd

#1/36 helpers
def _nrm(s):
    s="" if s is None else str(s)
    s=s.strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _to_df(preview):
    if isinstance(preview,list) and len(preview):
        return pd.DataFrame(preview)
    return pd.DataFrame()

def _safe_num(s):
    return pd.to_numeric(s,errors="coerce")

#2/36 memory
def load_memory(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path,"r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

#3/36 detector
def is_followup_question(q):
    nq=_nrm(q)
    patterns=[
        "sur ce resultat","sur ce résultat","dans ce resultat","dans ce résultat",
        "sur la branche","branche ","garde seulement","garde uniquement","retire ","sans ",
        "compare la branche","compare les branches","sur la cible precedente","sur la cible précédente",
        "sample >","sample >=","sample_size >","sample_size >=","n >","n >=",
        "j+5","j+3","lendemain","seulement les cas","uniquement les cas",
        "sur la meilleure ligne","sur la premiere","sur la première","sur la deuxieme","sur la deuxième",
        "sur la troisieme","sur la troisième","reprends la cible","reprend la cible",
        "not(","avec ","uniquement ","groupe ","group "
    ]
    return any(p in nq for p in patterns)

#4/36 extractors
def _extract_terms(nq):
    mapping=[
        ("not(calendar)","NOT(CALENDAR)"),("calendar","CALENDAR"),
        ("not(vix)","NOT(VIX)"),("vix","VIX"),
        ("not(dxy)","NOT(DXY)"),("dxy","DXY"),
        ("not(us10y)","NOT(US10Y)"),("us10y","US10Y"),
        ("vix1d","VIX1D"),("vix9d","VIX9D"),("vvix","VVIX"),
        ("spx","SPX"),("spy","SPY"),("qqq","QQQ"),("iwm","IWM"),
        ("aapl","AAPL"),("nvda","NVDA"),("msft","MSFT"),("amzn","AMZN"),("meta","META"),("tsla","TSLA"),
    ]
    out=[]
    for k,v in mapping:
        if k in nq:
            out.append(v)
    return list(dict.fromkeys(out))

def _extract_threshold(nq):
    m=re.search(r"(sample|sample_size|n)\s*(>=|>)\s*(\d+)",nq)
    if m:
        return (m.group(2), int(m.group(3)))
    return None

def _extract_metric(nq):
    mapping=[
        ("j+5","plus5d_mean"),
        ("j+3","plus3d_mean"),
        ("lendemain","next_close_mean"),
        ("gap","gap_mean"),
        ("intraday","intraday_mean"),
        ("ouverture haussiere","open_up_prob"),
        ("ouverture baissiere","open_down_prob")
    ]
    for k,v in mapping:
        if k in nq:
            return v
    return None

def _extract_line_indices(nq):
    idx=[]
    mapping=[("meilleure ligne",0),("premiere",0),("première",0),("deuxieme",1),("deuxième",1),("troisieme",2),("troisième",2)]
    for k,v in mapping:
        if k in nq:
            idx.append(v)
    return sorted(set(idx))

def _extract_branch_number(nq):
    m=re.search(r"branche\s*(\d+)",nq)
    if m:
        return int(m.group(1))
    m2=re.search(r"group(?:e)?\s*(\d+)",nq)
    if m2:
        return int(m2.group(1))
    return None

#5/36 sources
def _branch_source(df,memory):
    if "bucket" in df.columns:
        return df["bucket"].astype(str)
    branches=((memory or {}).get("semantics",{}) or {}).get("logic_tree_branches",[])
    if branches:
        joined=" | ".join(branches)
        return pd.Series([joined]*len(df),index=df.index)
    return pd.Series([""]*len(df),index=df.index)

def _memory_branches(memory):
    branches=((memory or {}).get("semantics",{}) or {}).get("logic_tree_branches",[])
    return [str(x) for x in branches]

#6/36 branch filtering
def _filter_by_terms(df,memory,terms,mode="keep"):
    if len(terms)==0 or df.empty:
        return df
    src=_branch_source(df,memory).str.upper()
    if mode=="keep":
        mask=pd.Series([False]*len(df),index=df.index)
        for t in terms:
            mask=mask | src.str.contains(re.escape(str(t).upper()),na=False)
    else:
        mask=pd.Series([True]*len(df),index=df.index)
        for t in terms:
            mask=mask & (~src.str.contains(re.escape(str(t).upper()),na=False))
    return df[mask].copy()

#7/36 numbered branch filtering
def _branch_term_from_number(memory, num):
    branches=_memory_branches(memory)
    idx=num-1
    if 0 <= idx < len(branches):
        return branches[idx]
    return None

#8/36 main
def run_followup(q,memory):
    if not memory:
        return {"status":"NO_MEMORY","answer_type":"table","value":0,"preview":[],"summary":"Aucune mémoire précédente disponible."}
    nq=_nrm(q)
    df=_to_df(memory.get("preview",[]))
    if df.empty:
        return {"status":"NO_PREVIEW_MEMORY","answer_type":"table","value":0,"preview":[],"summary":"La mémoire existe mais sans preview exploitable."}

    base=df.copy()
    summary_parts=[]

    # line selection
    idxs=_extract_line_indices(nq)
    if idxs:
        idxs=[i for i in idxs if i < len(base)]
        if idxs:
            base=base.iloc[idxs].copy()
            summary_parts.append(f"lignes={idxs}")

    # explicit branch number/group
    bn=_extract_branch_number(nq)
    if bn is not None:
        bt=_branch_term_from_number(memory,bn)
        if bt:
            before=len(base)
            base=_filter_by_terms(base,memory,[bt],mode="keep")
            summary_parts.append(f"branche_num={bn}")
            summary_parts.append(f"branche={bt}")
            summary_parts.append(f"avant={before}")

    # term filtering
    terms=_extract_terms(nq)
    if ("sur la branche" in nq) or ("garde seulement" in nq) or ("garde uniquement" in nq) or ("uniquement " in nq) or ("avec " in nq):
        before=len(base)
        base=_filter_by_terms(base,memory,terms,mode="keep")
        if terms:
            summary_parts.append("branches gardées="+",".join(terms))
            summary_parts.append(f"avant={before}")
    if ("retire " in nq) or ("sans " in nq):
        before=len(base)
        base=_filter_by_terms(base,memory,terms,mode="exclude")
        if terms:
            summary_parts.append("branches retirées="+",".join(terms))
            summary_parts.append(f"avant={before}")

    # target reuse
    if "cible precedente" in nq or "cible précédente" in nq or "reprends la cible" in nq or "reprend la cible" in nq:
        tgt=memory.get("target_dataset")
        if tgt is not None:
            summary_parts.append(f"cible={tgt}")

    # sample threshold
    th=_extract_threshold(nq)
    if th and "sample_size" in base.columns:
        op,val=th
        s=_safe_num(base["sample_size"])
        if op==">":
            base=base[s>val].copy()
        else:
            base=base[s>=val].copy()
        summary_parts.append(f"sample_size {op} {val}")

    requested_metric=_extract_metric(nq)
    compare_mode=("compare" in nq)

    preview=base.head(20).to_dict(orient="records")

    if requested_metric and requested_metric in base.columns:
        s=_safe_num(base[requested_metric])
        if s.notna().any():
            summary_parts.append(f"moyenne {requested_metric}={float(s.mean()):.4f}")

    if compare_mode:
        summary="comparaison demandée"
        if summary_parts:
            summary+=" | " + " | ".join(summary_parts)
        return {
            "status":"OK","answer_type":"table","value":int(len(base)),
            "target_dataset":memory.get("target_dataset"),
            "memory_source_question":memory.get("question"),
            "requested_metric":requested_metric,
            "preview":preview,
            "summary":summary,
            "available_branches":_memory_branches(memory)
        }

    if not summary_parts:
        summary_parts.append(f"{len(base)} ligne(s) retenue(s) sur mémoire précédente")
    else:
        summary_parts.insert(0,f"{len(base)} ligne(s) retenue(s)")

    return {
        "status":"OK","answer_type":"table","value":int(len(base)),
        "target_dataset":memory.get("target_dataset"),
        "memory_source_question":memory.get("question"),
        "requested_metric":requested_metric,
        "preview":preview,
        "summary":" | ".join(summary_parts),
        "available_branches":_memory_branches(memory)
    }
