import os, re, json, unicodedata
import pandas as pd

PROJECT_ROOT=r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
REGISTRY_PATH=r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/ETAPE197_ASSET_TIMEFRAME_REGISTRY.json"

SUPPORTED_ASSETS={
    "SPX":["spx","s&p 500","s&p500","s&p"],
    "SPY":["spy"],
    "QQQ":["qqq"],
    "IWM":["iwm"],
    "VIX":["vix"],
    "VVIX":["vvix"],
    "VIX9D":["vix9d","vix 9d"],
    "DXY":["dxy","dollar index","dollar"],
    "GOLD":["gold","or"],
}

UNSUPPORTED_COMMON=["aapl","msft","nvda","tsla","meta","amzn","googl","goog","nflx","amd","intc","adbe","crm","orcl"]

MONTHS={
    "janvier":1,"fevrier":2,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,"juillet":7,
    "aout":8,"août":8,"septembre":9,"octobre":10,"novembre":11,"decembre":12,"décembre":12
}

def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def _nrm(s):
    s=_strip_accents(str(s).lower())
    repl=[
        ("cloture","cloture"),("clôture","cloture"),("cloturé","cloture"),("clôturé","cloture"),
        ("a cloture","cloture"),("a clôturé","cloture"),
        ("au-dessous de","inferieur a"),("en dessous de","inferieur a"),("sous","inferieur a"),
        ("au-dessus de","superieur a"),("au dessus de","superieur a"),
        ("plus de","superieur a"),
    ]
    for a,b in repl:
        s=s.replace(a,b)
    s=re.sub(r"[^a-z0-9%+<>=/.' -]+"," ",s)
    s=re.sub(r"\s+"," ",s).strip()
    return s

def _contains_term(nq, term):
    return re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", nq) is not None

def _load_registry():
    if not os.path.exists(REGISTRY_PATH):
        return {}
    try:
        with open(REGISTRY_PATH,"r",encoding="utf-8") as f:
            raw=json.load(f)
        return raw.get("assets",{}) or {}
    except Exception:
        return {}

def _detect_assets_in_question(nq):
    found_supported=[]
    found_unsupported=[]
    for asset, aliases in SUPPORTED_ASSETS.items():
        for a in aliases:
            if _contains_term(nq,a):
                found_supported.append(asset)
                break
    for a in UNSUPPORTED_COMMON:
        if _contains_term(nq,a):
            found_unsupported.append(a.upper())
    return list(dict.fromkeys(found_supported)), list(dict.fromkeys(found_unsupported))

def _validate_assets_exist(assets):
    reg=_load_registry()
    missing=[]
    for asset in assets:
        arr=reg.get(asset,[]) or []
        valid=[x for x in arr if x.get("path") and os.path.exists(x.get("path"))]
        if not valid:
            missing.append(asset)
    return missing

def _pick_daily_entry(asset):
    reg=_load_registry()
    arr=reg.get(asset,[]) or []
    valid=[x for x in arr if x.get("path") and os.path.exists(x.get("path"))]
    if not valid:
        return None

    exact_pref={
        "VIX":["VIX_daily.csv"],
        "SPX":["SPX_daily.csv"],
        "SPY":["SPY_daily.csv"],
        "QQQ":["QQQ_daily.csv"],
        "IWM":["IWM_daily.csv"],
        "VVIX":["VVIX_daily.csv"],
        "VIX9D":["VIX9D_daily.csv"],
        "DXY":["DXY_daily.csv"],
        "GOLD":["Gold_daily.csv","GOLD_daily.csv"],
    }
    prefs=exact_pref.get(asset,[])
    for pref in prefs:
        for x in valid:
            if str(x.get("file_name","")) == pref:
                return x

    daily=[x for x in valid if (x.get("bar_minutes") in [1440,1440.0]) or ("daily" in str(x.get("file_name","")).lower())]
    if asset=="VIX":
        daily=[x for x in daily if str(x.get("file_name","")).lower()=="vix_daily.csv"]
    if daily:
        return sorted(daily,key=lambda x:(abs((x.get("bar_minutes") or 1440)-1440), str(x.get("file_name",""))))[0]
    return sorted(valid,key=lambda x:(abs((x.get("bar_minutes") or 999999)-1440), str(x.get("file_name",""))))[0]

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

def _find_time_col(df):
    for c in ["time","date","datetime","timestamp"]:
        if c in df.columns:
            return c
    for c in df.columns:
        if "time" in c or "date" in c:
            return c
    return None

def _find_close_col(df):
    prefs=["close","adj_close","close_last","last","price"]
    for c in prefs:
        if c in df.columns:
            return c
    for c in df.columns:
        if "close" in c:
            return c
    numeric=[]
    for c in df.columns:
        if c in ["time","date","datetime","timestamp"]:
            continue
        try:
            s=pd.to_numeric(df[c],errors="coerce")
            if s.notna().sum()>0:
                numeric.append(c)
        except Exception:
            pass
    return numeric[0] if numeric else None

def _parse_month_year(nq):
    month=None
    for k,v in MONTHS.items():
        if _contains_term(nq,k):
            month=v
            break
    m=re.search(r"\b(20\d{2})\b", nq)
    year=int(m.group(1)) if m else None
    return month, year

def _parse_condition(nq):
    m=re.search(r"(?:entre)\s+(-?\d+(?:\.\d+)?)\s+(?:et|a|à)\s+(-?\d+(?:\.\d+)?)", nq)
    if m:
        return ("between", float(m.group(1)), float(m.group(2)))
    m=re.search(r"(?:inferieur a|<)\s*(-?\d+(?:\.\d+)?)", nq)
    if m:
        return ("lt", float(m.group(1)), None)
    m=re.search(r"(?:superieur a|>)\s*(-?\d+(?:\.\d+)?)", nq)
    if m:
        return ("gt", float(m.group(1)), None)
    return (None,None,None)

def _question_is_count_like(nq):
    return any(x in nq for x in ["combien","nombre","nb ","nb de","fois"])

def can_handle(question):
    nq=_nrm(question)
    if not _question_is_count_like(nq):
        return False
    supported, unsupported=_detect_assets_in_question(nq)
    cond=_parse_condition(nq)
    return bool(supported or unsupported or cond[0] is not None)

def run(question, preview_rows=20):
    nq=_nrm(question)
    supported, unsupported=_detect_assets_in_question(nq)
    month, year=_parse_month_year(nq)
    cond_type, a, b=_parse_condition(nq)

    if unsupported:
        asset=unsupported[0]
        msg=f"Je n'ai pas de dataset canonique chargé pour {asset} dans cette app. Je ne dois donc pas répondre en le remplaçant par un autre actif."
        return {
            "status":"UNSUPPORTED_TARGET_ASSET",
            "engine":"count_threshold_engine",
            "value":None,
            "target_asset":asset,
            "answer_short":"Donnée indisponible",
            "answer_long":msg,
            "answer":msg,
            "source_file_names":[],
            "preview":[],
        }

    missing=_validate_assets_exist(supported)
    if missing:
        asset=missing[0]
        msg=f"Le dataset correspondant à {asset} n'existe pas dans la base."
        return {
            "status":"TARGET_DATASET_NOT_FOUND",
            "engine":"count_threshold_engine",
            "value":None,
            "target_asset":asset,
            "answer_short":"Dataset manquant",
            "answer_long":msg,
            "answer":msg,
            "source_file_names":[],
            "preview":[],
        }

    target_asset=supported[0] if supported else None
    if target_asset is None:
        msg="Je n'ai pas réussi à détecter clairement l'actif cible."
        return {
            "status":"NO_TARGET_ASSET",
            "engine":"count_threshold_engine",
            "value":None,
            "answer_short":"Actif manquant",
            "answer_long":msg,
            "answer":msg,
            "source_file_names":[],
            "preview":[],
        }

    entry=_pick_daily_entry(target_asset)
    if not entry:
        msg=f"Le dataset daily de {target_asset} est introuvable."
        return {
            "status":"TARGET_DATASET_NOT_FOUND",
            "engine":"count_threshold_engine",
            "value":None,
            "target_asset":target_asset,
            "answer_short":"Dataset introuvable",
            "answer_long":msg,
            "answer":msg,
            "source_file_names":[],
            "preview":[],
        }

    df=_read_csv_flex(entry["path"])
    df=_norm_cols(df)
    tcol=_find_time_col(df)
    ccol=_find_close_col(df)
    if tcol is None or ccol is None:
        msg="Le dataset ne permet pas de lire correctement la date et la clôture."
        return {
            "status":"BAD_DATASET",
            "engine":"count_threshold_engine",
            "value":None,
            "target_asset":target_asset,
            "answer_short":"Dataset invalide",
            "answer_long":msg,
            "answer":msg,
            "source_file_names":[entry.get("file_name")],
            "preview":[],
        }

    work=df.copy()
    work["timestamp"]=pd.to_datetime(work[tcol],errors="coerce")
    work["close_num"]=pd.to_numeric(work[ccol],errors="coerce")
    work=work[work["timestamp"].notna() & work["close_num"].notna()].copy()
    work["year"]=work["timestamp"].dt.year
    work["month"]=work["timestamp"].dt.month
    work["date"]=work["timestamp"].dt.strftime("%Y-%m-%d")

    if year is not None:
        work=work[work["year"]==year]
    if month is not None:
        work=work[work["month"]==month]

    if cond_type=="between":
        lo=min(a,b); hi=max(a,b)
        filt=work[(work["close_num"]>=lo) & (work["close_num"]<=hi)].copy()
        cond_txt=f"entre {lo:g} et {hi:g}"
        ranges=[[target_asset,lo,hi]]
    elif cond_type=="lt":
        filt=work[work["close_num"]<a].copy()
        cond_txt=f"en dessous de {a:g}"
        ranges=[[target_asset,None,a]]
    elif cond_type=="gt":
        filt=work[work["close_num"]>a].copy()
        cond_txt=f"au-dessus de {a:g}"
        ranges=[[target_asset,a,None]]
    else:
        filt=work.copy()
        cond_txt="dans le filtre demandé"
        ranges=[]

    count=int(len(filt))
    ctx=[]
    if month is not None:
        rev={v:k for k,v in MONTHS.items()}
        mois_txt=rev.get(month,f"mois {month}")
        ctx.append(mois_txt)
    if year is not None:
        ctx.append(str(year))
    ctx_txt=" en " + " ".join([x for x in ctx if x]).strip() if ctx else ""

    short=f"{count} fois"
    long_=f"{target_asset} a été {cond_txt} {count} fois{ctx_txt}."
    preview=[
        {"date":r["date"],"close":float(r["close_num"])}
        for _,r in filt[["date","close_num"]].head(preview_rows).iterrows()
    ]

    return {
        "status":"OK",
        "engine":"count_threshold_engine",
        "metric":"count",
        "value":count,
        "target_asset":target_asset,
        "target_dataset":entry.get("file_name"),
        "answer_short":short,
        "answer_long":long_,
        "answer":long_,
        "source_file_names":[entry.get("file_name")],
        "preview":preview,
        "conditions":[],
        "ranges":ranges,
        "display_context":{"month":month,"year":year,"cond_type":cond_type,"a":a,"b":b,"cond_txt":cond_txt},
    }
