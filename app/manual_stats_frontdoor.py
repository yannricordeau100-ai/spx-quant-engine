import os, re, json, time
import pandas as pd

LOCAL_BASE = r"/content/SPX_DATA_LINK"
LOCATOR_JSON = r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/csv_locator_index.json"
LOCATOR_DIAG_JSON = r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/csv_locator_diag.json"
STATE_JSON = r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/manual_stats_frontdoor_registry.json"
DIAG_JSON = r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/manual_stats_frontdoor_diag.json"
INVENTORY_JSON = r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/all_csv_inventory.json"
INVENTORY_DIAG_JSON = r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/all_csv_inventory_diag.json"

CACHE_SECONDS = 900

SCAN_ROOTS = [
    LOCAL_BASE,
    r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES",
    r"/content/drive/MyDrive/IA/VIX",
    r"/content/drive/MyDrive/IA",
]

DIR_EXCLUDES = {"MANIFESTS","__pycache__","runtime_diag"}
NAME_EXCLUDES = ["summary","registry","catalog","analytics","feedback","manifest","audit","overview","feature","decision","context","coverage","status","history"]
HARD_BAD_TICKERS = {"COPIE","US","VX"}

MONTHS = {"janvier":1,"fevrier":2,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,"juillet":7,"aout":8,"août":8,"septembre":9,"octobre":10,"novembre":11,"decembre":12,"décembre":12}
WEEKDAYS = {"lundi":0,"lundis":0,"mardi":1,"mardis":1,"mercredi":2,"mercredis":2,"jeudi":3,"jeudis":3,"vendredi":4,"vendredis":4}

# Alias VIX OPEN ajoutés:
# vix open, vix 9h30, vix a l ouverture, vix a l open, vix ouverture,
# vix open spx, vix ouverture spx, vix 9h30 cet, vix a l ouverture du spx,
# vix a l open du spx, vix opening, vix_9h30_cet_spx_opening
ALIASES = {
    "apple":"AAPL","aapl":"AAPL","spy":"SPY","spx":"SPX","qqq":"QQQ",
    "vix open spx":"VIX_9H30_CET_SPX_OPENING",
    "vix ouverture spx":"VIX_9H30_CET_SPX_OPENING",
    "vix a l ouverture du spx":"VIX_9H30_CET_SPX_OPENING",
    "vix a l open du spx":"VIX_9H30_CET_SPX_OPENING",
    "vix 9h30 cet":"VIX_9H30_CET_SPX_OPENING",
    "vix_9h30_cet_spx_opening":"VIX_9H30_CET_SPX_OPENING",
    "vix 9h30 cet spx opening":"VIX_9H30_CET_SPX_OPENING",
    "vix opening":"VIX_9H30_CET_SPX_OPENING",
    "vix openning":"VIX_9H30_CET_SPX_OPENING",
    "vix a l ouverture":"VIX_9H30_CET_SPX_OPENING",
    "vix a l open":"VIX_9H30_CET_SPX_OPENING",
    "vix ouverture":"VIX_9H30_CET_SPX_OPENING",
    "vix open":"VIX_9H30_CET_SPX_OPENING",
    "vix 9h30":"VIX_9H30_CET_SPX_OPENING",
    "vix":"VIX",
    "vix1d":"VIX1D","vix 1d":"VIX1D","vix_1d":"VIX1D",
    "vix3m":"VIX3M","vix6m":"VIX6M","vix9d":"VIX9D","vvix":"VVIX","vx1":"VX1","vx2":"VX2",
    "gold":"GOLD","dxy":"DXY","iwm":"IWM","aaoi":"AAOI","tick":"TICK",
    "ftse100":"FTSE100","nikkei225":"NIKKEI225","dax40":"DAX40","oil":"OIL"
}

MANUAL_PREFERRED_PATHS = {
    "VIX": r"/content/drive/MyDrive/IA/VIX/VIX_daily.csv",
    "VIX1D": r"/content/SPX_DATA_LINK/VOLATILITY/VIX1D_daily_generated.csv",
    "VIX_9H30_CET_SPX_OPENING": r"/content/drive/MyDrive/IA/VIX/VIX_9H30_CET_SPX_OPENING_daily.csv",
    "VIX3M": r"/content/drive/MyDrive/IA/VIX/VIX3M_daily.csv",
    "VIX6M": r"/content/drive/MyDrive/IA/VIX/VIX6M_daily.csv",
    "VIX9D": r"/content/drive/MyDrive/IA/VIX/VIX9D_daily.csv",
    "VVIX": r"/content/drive/MyDrive/IA/VIX/VVIX_daily.csv",
    "VX1": r"/content/drive/MyDrive/IA/VIX/VX_FUTURE _VX1_daily.csv",
    "VX2": r"/content/drive/MyDrive/IA/VIX/VX_FUTURE _VX2_daily.csv",
}

def _load_json(path, default):
    try:
        with open(path,"r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _save_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,"w",encoding="utf-8") as f:
        json.dump(obj,f,ensure_ascii=False,indent=2)

def _norm(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items():
        s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'")
    s=re.sub(r"[^a-z0-9%+/\-\. ]+"," ",s)
    s=re.sub(r"\s+"," ",s).strip()
    return s

def _detect_sep(path):
    try:
        with open(path,"r",encoding="utf-8",errors="ignore") as f:
            head="".join([f.readline() for _ in range(5)])
    except Exception:
        return ","
    scores={";":head.count(";"),",":head.count(","),"\t":head.count("\t")}
    best=max(scores,key=scores.get)
    return best if scores[best] > 0 else ","

def _to_num_series(s):
    x=s.astype(str)
    x=x.str.replace("\u202f","",regex=False).str.replace("\xa0","",regex=False).str.replace(" ","",regex=False).str.replace("'","",regex=False)
    sample="".join(x.dropna().astype(str).head(40).tolist())
    if "," in sample and "." in sample:
        if sample.rfind(",") > sample.rfind("."):
            x=x.str.replace(".","",regex=False).str.replace(",",".",regex=False)
        else:
            x=x.str.replace(",","",regex=False)
    else:
        x=x.str.replace(",",".",regex=False)
    return pd.to_numeric(x,errors="coerce")

def _path_norm(path):
    return str(path).lower().replace("\\","/")

def _looks_excluded(path):
    bn=os.path.basename(path.lower())
    parts=set(os.path.normpath(path).split(os.sep))
    if parts & DIR_EXCLUDES:
        return True
    return any(x in bn for x in NAME_EXCLUDES)

def _ticker_guess(path):
    p=_path_norm(path)
    b=os.path.basename(path).lower()
    parent=os.path.basename(os.path.dirname(path)).lower()
    full=parent + "/" + b

    if b.startswith("copie de spx") or "copie de spx" in b:
        return "SPX"
    if b.startswith("copie de aapl") or "copie de aapl" in b:
        return "AAPL"
    if b.startswith("copie de spy") or "copie de spy" in b:
        return "SPY"
    if b.startswith("copie de qqq") or "copie de qqq" in b:
        return "QQQ"
    if b.startswith("copie de "):
        return "COPIE"

    if "vx_future _vx1_daily.csv" in b or "vx_future_vx1_daily.csv" in b or b=="vx1_daily.csv":
        return "VX1"
    if "vx_future _vx2_daily.csv" in b or "vx_future_vx2_daily.csv" in b or b=="vx2_daily.csv":
        return "VX2"
    if "vix_9h30_cet_spx_opening" in b or "vix_9h30_cet_spx_opening" in full:
        return "VIX_9H30_CET_SPX_OPENING"
    if "vix1d" in b or "vix1d" in full or "vix_1d" in b or "vix_1d" in full:
        return "VIX1D"
    if re.search(r"(^|[^a-z0-9])vix3m([^a-z0-9]|$)", full):
        return "VIX3M"
    if re.search(r"(^|[^a-z0-9])vix6m([^a-z0-9]|$)", full):
        return "VIX6M"
    if re.search(r"(^|[^a-z0-9])vix9d([^a-z0-9]|$)", full):
        return "VIX9D"
    if re.search(r"(^|[^a-z0-9])vvix([^a-z0-9]|$)", full):
        return "VVIX"
    if re.search(r"(^|[^a-z0-9])vix([^a-z0-9]|$)", b) or re.search(r"(^|[^a-z0-9])vix([^a-z0-9]|$)", full):
        return "VIX"

    base=os.path.splitext(os.path.basename(path))[0].upper().replace(" ","_").replace("-","_")
    if base.startswith("COPIE_DE_SPX"):
        return "SPX"
    if base.startswith("COPIE_DE_AAPL"):
        return "AAPL"
    if base.startswith("COPIE_DE_SPY"):
        return "SPY"
    if base.startswith("COPIE_DE_QQQ"):
        return "QQQ"
    if base.startswith("COPIE_DE_"):
        return "COPIE"
    m=re.match(r"([A-Z][A-Z0-9_]{0,63})", base)
    return m.group(1) if m else base[:63]

def _load_csv(path):
    sep=_detect_sep(path)
    try:
        df=pd.read_csv(path,sep=sep,dtype=str)
    except Exception:
        try:
            df=pd.read_csv(path,sep=None,engine="python",dtype=str)
        except Exception:
            return None
    df.columns=[str(c).strip().lower() for c in df.columns]
    alias={"date":"time","datetime":"time","timestamp":"time","adjclose":"adj close","adj_close":"adj close","closing price":"close","closing_price":"close"}
    df=df.rename(columns={c:alias.get(c,c) for c in df.columns})
    value_col=None
    for c in ["close","open","adj close"]:
        if c in df.columns:
            value_col=c
            break
    if "time" not in df.columns or value_col is None:
        return None
    for c in ["open","high","low","close","volume","adj close"]:
        if c in df.columns:
            df[c]=_to_num_series(df[c])
    df["time"]=pd.to_datetime(df["time"],errors="coerce")
    df=df.dropna(subset=["time",value_col]).sort_values("time").copy()
    if df.empty:
        return None
    if "close" not in df.columns:
        df["close"]=df[value_col]
    else:
        df["close"]=df["close"].fillna(df[value_col])
    df["date"]=df["time"].dt.normalize()
    agg={}
    for c,a in [("open","first"),("high","max"),("low","min"),("close","last"),("volume","sum"),("adj close","last")]:
        if c in df.columns:
            agg[c]=a
    if "close" not in agg:
        agg["close"]="last"
    daily=df.groupby("date",as_index=False).agg(agg)
    daily["time"]=pd.to_datetime(daily["date"])
    daily=daily.sort_values("time").reset_index(drop=True)
    daily["year"]=daily["time"].dt.year
    daily["month"]=daily["time"].dt.month
    daily["weekday"]=daily["time"].dt.weekday
    daily["ret1d_pct"]=daily["close"].pct_change()*100.0
    daily["ret_h_1d_pct"]=(daily["close"].shift(-1)/daily["close"]-1.0)*100.0
    daily["ret_h_1m_pct"]=(daily["close"].shift(-21)/daily["close"]-1.0)*100.0
    return daily

def _score_row(row):
    p=_path_norm(row.get("path") or "")
    return (
                1 if "/content/drive/mydrive/ia/" in p else 0,
        1 if "/raw_sources/" in p else 0,
        0 if "_recovered_by_locator" in p else 1,
        row.get("rows",0),
        -len(row.get("file_name",""))
    )

def _scan_signature():
    files=[]
    for base in SCAN_ROOTS:
        if os.path.isdir(base):
            for root,dirs,fns in os.walk(base):
                dirs[:] = [d for d in dirs if d not in DIR_EXCLUDES]
                for fn in fns:
                    if fn.lower().endswith(".csv"):
                        p=os.path.join(root,fn)
                        if _looks_excluded(p):
                            continue
                        try:
                            st=os.stat(p)
                            files.append((p, int(st.st_mtime), int(st.st_size)))
                        except Exception:
                            pass
    files=sorted(files)
    return str(hash(tuple(files)))

def rebuild_inventory(force=False):
    old=_load_json(INVENTORY_JSON,{})
    oldd=_load_json(INVENTORY_DIAG_JSON,{})
    current_sig=_scan_signature()
    if (not force) and isinstance(old,dict) and old and oldd.get("scan_signature")==current_sig and (time.time()-float(oldd.get("built_at_ts",0) or 0) <= CACHE_SECONDS):
        return old

    inv={}
    total=0
    for base in SCAN_ROOTS:
        if os.path.isdir(base):
            for root,dirs,files in os.walk(base):
                dirs[:] = [d for d in dirs if d not in DIR_EXCLUDES]
                for fn in files:
                    if fn.lower().endswith(".csv"):
                        p=os.path.join(root,fn)
                        if _looks_excluded(p):
                            continue
                        total += 1
                        inv[p] = {
                            "path": p,
                            "file_name": os.path.basename(p),
                            "folder_name": os.path.basename(os.path.dirname(p)),
                            "ticker_guess": _ticker_guess(p),
                            "price_like": bool(_load_csv(p) is not None),
                        }
    diag={
        "built_at_ts": time.time(),
        "scan_signature": current_sig,
        "total_csv_seen": total,
        "inventory_count": len(inv),
        "price_like_count": sum(1 for v in inv.values() if v.get("price_like")),
        "non_price_like_count": sum(1 for v in inv.values() if not v.get("price_like")),
    }
    _save_json(INVENTORY_JSON, inv)
    _save_json(INVENTORY_DIAG_JSON, diag)
    return inv

def rebuild_locator(force=False):
    old=_load_json(LOCATOR_JSON,{})
    oldd=_load_json(LOCATOR_DIAG_JSON,{})
    current_sig=_scan_signature()
    if (not force) and isinstance(old,dict) and old and oldd.get("scan_signature")==current_sig and (time.time()-float(oldd.get("built_at_ts",0) or 0) <= CACHE_SECONDS):
        return old

    inv=rebuild_inventory(force=force)
    best={}
    existing=_load_json(LOCATOR_JSON,{})
    if isinstance(existing,dict):
        for t,row in existing.items():
            if isinstance(row,dict) and os.path.isfile(row.get("path","")) and t not in HARD_BAD_TICKERS:
                best[t]=row

    invalid=[]

    for t,p in MANUAL_PREFERRED_PATHS.items():
        if os.path.isfile(p):
            df=_load_csv(p)
            if df is not None and not df.empty:
                best[t]={
                    "ticker":t,
                    "path":p,
                    "original_path":p,
                    "file_name":os.path.basename(p),
                    "kind":"core",
                    "rows":int(len(df)),
                    "min_date":str(pd.to_datetime(df["date"].min()).date()),
                    "max_date":str(pd.to_datetime(df["date"].max()).date())
                }

    for p,meta in inv.items():
        if not meta.get("price_like"):
            continue
        t=meta.get("ticker_guess")
        if t in HARD_BAD_TICKERS:
            continue
        df=_load_csv(p)
        if df is None:
            if len(invalid)<120:
                invalid.append({"path":p,"ticker_guess":t,"reason":"load_fail"})
            continue
        row={
            "ticker":t,
            "path":p,
            "original_path":p,
            "file_name":os.path.basename(p),
            "kind":"aau" if "autres actions upload" in p.lower() else "core",
            "rows":int(len(df)),
            "min_date":str(pd.to_datetime(df["date"].min()).date()),
            "max_date":str(pd.to_datetime(df["date"].max()).date())
        }
        old_row=best.get(t)
        if old_row is None or _score_row(row) > _score_row(old_row):
            best[t]=row

    for bad in HARD_BAD_TICKERS:
        best.pop(bad,None)

    diag={
        "built_at_ts":time.time(),
        "scan_signature":current_sig,
        "index_count":len(best),
        "tickers":sorted(best.keys()),
        "aapl_present":"AAPL" in best,
        "spy_present":"SPY" in best,
        "vix_present":"VIX" in best,
        "vix1d_present":"VIX1D" in best,
        "vix_opening_present":"VIX_9H30_CET_SPX_OPENING" in best,
        "invalid_examples":invalid[:60]
    }
    _save_json(LOCATOR_JSON,best)
    _save_json(LOCATOR_DIAG_JSON,diag)
    return best

def build_registry(force=False):
    old=_load_json(STATE_JSON,{})
    oldd=_load_json(DIAG_JSON,{})
    current_sig=_scan_signature()
    if (not force) and isinstance(old,dict) and old and oldd.get("scan_signature")==current_sig and (time.time()-float(oldd.get("built_at_ts",0) or 0) <= CACHE_SECONDS):
        return old
    locator=rebuild_locator(force=force)
    reg={}
    invalid=[]
    for t,meta in sorted(locator.items()):
        p=(meta or {}).get("path")
        if not p or not os.path.isfile(p):
            continue
        df=_load_csv(p)
        if df is None or df.empty:
            if len(invalid)<120:
                invalid.append({"ticker":t,"path":p,"reason":"load_csv_none"})
            continue
        reg[t]={
            "ticker":t,
            "path":p,
            "original_path":(meta or {}).get("original_path",p),
            "file_name":(meta or {}).get("file_name",os.path.basename(p)),
            "kind":(meta or {}).get("kind","core"),
            "rows":int(len(df)),
            "min_date":str(pd.to_datetime(df["date"].min()).date()),
            "max_date":str(pd.to_datetime(df["date"].max()).date()),
            "frequency_label":"journalière"
        }
    for bad in HARD_BAD_TICKERS:
        reg.pop(bad,None)
    diag={
        "built_at_ts":time.time(),
        "scan_signature":current_sig,
        "valid_count":len(reg),
        "tickers":sorted(reg.keys()),
        "aapl_present":"AAPL" in reg,
        "spy_present":"SPY" in reg,
        "vix_present":"VIX" in reg,
        "vix1d_present":"VIX1D" in reg,
        "vix_opening_present":"VIX_9H30_CET_SPX_OPENING" in reg,
        "invalid_examples":invalid[:60]
    }
    _save_json(STATE_JSON,reg)
    _save_json(DIAG_JSON,diag)
    return reg

def force_refresh_all():
    rebuild_inventory(force=True)
    rebuild_locator(force=True)
    reg=build_registry(force=True)
    return {"ok":True,"valid_count":len(reg),"tickers":sorted(reg.keys())}

def get_registry_diagnostic():
    reg=build_registry(force=False)
    return {
        "valid_count":len(reg),
        "tickers":sorted(reg.keys()),
        "aapl_present":"AAPL" in reg,
        "spy_present":"SPY" in reg,
        "vix_present":"VIX" in reg,
        "vix1d_present":"VIX1D" in reg,
        "vix_opening_present":"VIX_9H30_CET_SPX_OPENING" in reg
    }

def get_inventory_diagnostic():
    inv=_load_json(INVENTORY_JSON,{})
    diag=_load_json(INVENTORY_DIAG_JSON,{})
    return {
        "inventory_count": len(inv),
        "total_csv_seen": diag.get("total_csv_seen", len(inv)),
        "price_like_count": diag.get("price_like_count", 0),
        "non_price_like_count": diag.get("non_price_like_count", 0),
    }

def get_live_supported_files_rows():
    reg=build_registry(force=False)
    rows=[]
    for t,meta in sorted(reg.items()):
        rows.append({
            "ticker":t,
            "file_name":meta.get("file_name"),
            "path":meta.get("path"),
            "kind":meta.get("kind"),
            "rows":meta.get("rows"),
            "min_date":meta.get("min_date"),
            "max_date":meta.get("max_date")
        })
    return rows

def get_all_inventory_rows():
    inv=rebuild_inventory(force=False)
    rows=[]
    for _,meta in sorted(inv.items(), key=lambda kv: (kv[1].get("folder_name",""), kv[1].get("file_name",""))):
        rows.append({
            "file_name": meta.get("file_name"),
            "folder_name": meta.get("folder_name"),
            "ticker_guess": meta.get("ticker_guess"),
            "price_like": meta.get("price_like"),
            "path": meta.get("path"),
        })
    return rows

def get_live_derived_files_rows():
    return []


def get_runtime_health_snapshot():
    reg=build_registry(force=False)
    inv=_load_json(INVENTORY_JSON,{})
    return {
        "registry_valid_count": len(reg),
        "inventory_count": len(inv),
        "aapl_present": "AAPL" in reg,
        "spy_present": "SPY" in reg,
        "vix_present": "VIX" in reg,
        "vix1d_present": "VIX1D" in reg,
        "vix_opening_present": "VIX_9H30_CET_SPX_OPENING" in reg,
    }


def _parse_years(nq):
    return sorted({int(y) for y in re.findall(r"\b(20\d{2})\b", nq)})

def _parse_month(nq):
    for k,v in MONTHS.items():
        if re.search(r"(?<![a-z0-9])" + re.escape(_norm(k)) + r"(?![a-z0-9])", nq):
            return v
    return None

def _parse_weekday(nq):
    for k,v in WEEKDAYS.items():
        if re.search(r"(?<![a-z0-9])" + re.escape(_norm(k)) + r"(?![a-z0-9])", nq):
            return v
    return None

def _fmt_pct(x):
    return ("%.2f%%" % x).replace(".", ",")

def _fmt_num(x, nd=4):
    try:
        return ((("%." + str(nd) + "f") % float(x))).replace(".", ",")
    except Exception:
        return str(x)

def _weekday_name(wd):
    mp={0:"lundi",1:"mardi",2:"mercredi",3:"jeudi",4:"vendredi"}
    return mp.get(wd)

def _filter_time(df, years=None, month=None, weekday=None):
    out=df.copy()
    years=years or []
    if years:
        out=out[out["year"].isin(years)]
    if month is not None:
        out=out[out["month"]==month]
    if weekday is not None:
        out=out[out["weekday"]==weekday]
    return out

def _extract_assets(question, reg):
    nq=_norm(question)
    hits=[]
    for alias,ticker in sorted(ALIASES.items(), key=lambda kv:(-len(kv[0]), kv[0])):
        alias_n=_norm(alias)
        if re.search(r"(?<![a-z0-9])" + re.escape(alias_n) + r"(?![a-z0-9])", nq):
            if ticker in reg and ticker not in hits:
                hits.append(ticker)
    for t in sorted(reg):
        if re.search(r"(?<![a-z0-9])" + re.escape(t.lower()) + r"(?![a-z0-9])", nq):
            if t not in hits:
                hits.append(t)
    return hits

def _result_error(t, reg):
    return {"ok":False,"error":"ASSET_NOT_AVAILABLE","detail":"Le ticker " + str(t) + " n'est pas disponible actuellement. Disponibles: " + ", ".join(sorted(reg.keys())[:30])}

def _is_relation_question(nq):
    keys=["relation","correlation","corrélation","corr","rapport","ratio","ecart","écart","spread","liaison","lien","pattern","structure","dependance","dépendance","relie","relié","compare les","entre "]
    return any(k in nq for k in keys)

def _is_too_complex_for_statistique(nq, assets):
    if len(assets) >= 2 and _is_relation_question(nq):
        return True
    if ("pattern" in nq or "corr" in nq or "correlation" in nq or "corrélation" in nq) and len(assets) >= 1:
        return True
    return False

def _vix1d_methodology_note():
    return (
        "Attention : la data VIX1D est récente et sa méthodologie a changé le 10 février 2025. "
        "Avant cette date, la lecture historique du VIX1D doit être utilisée avec prudence. "
        "Le plus simple est de comparer surtout les régimes, percentiles, ratios et écarts relatifs, "
        "plutôt que de considérer les niveaux anciens comme parfaitement équivalents aux niveaux récents. "
        "Je n'affirme pas ici un coefficient fixe universel de conversion, car ce serait potentiellement trompeur."
    )

def _relation_explainer(a1, a2, ret_corr, lvl_corr, ratio_mean, spread_mean, share_gt, p25, p50, p75):
    if pd.isna(ret_corr):
        corr_txt="relation courte peu exploitable"
    elif ret_corr >= 0.75:
        corr_txt="relation très forte"
    elif ret_corr >= 0.50:
        corr_txt="relation positive assez forte"
    elif ret_corr >= 0.25:
        corr_txt="relation positive modérée"
    elif ret_corr > -0.25:
        corr_txt="relation faible ou instable"
    else:
        corr_txt="relation inverse"

    line1 = f"En pratique, {a1} et {a2} ont ici une {corr_txt}."
    if share_gt >= 60:
        line2 = f"{a1} est souvent au-dessus de {a2}, ce qui indique une prime structurelle fréquente."
    elif share_gt <= 40:
        line2 = f"{a1} reste la plupart du temps sous {a2}, donc {a2} domine généralement le niveau observé."
    else:
        line2 = f"{a1} et {a2} s'alternent assez souvent, sans domination écrasante de l'un sur l'autre."

    line3 = (
        f"Le ratio moyen {a1}/{a2} vaut {_fmt_num(ratio_mean,3)}, avec des repères percentile 25/50/75 de "
        f"{_fmt_num(p25,3)} / {_fmt_num(p50,3)} / {_fmt_num(p75,3)}."
    )

    line4 = (
        "Quand un ratio vaut près de 1, cela signifie que les deux mesures sont proches l'une de l'autre. "
        "Quand il est nettement inférieur à 1, le numérateur est plus faible que le dénominateur. "
        "Quand il est nettement supérieur à 1, le numérateur devient dominant relativement à l'autre série."
    )

    line5 = (
        f"Le spread moyen {a1} - {a2} vaut {_fmt_num(spread_mean,3)}. "
        "Le spread permet de voir l'écart absolu entre les deux mesures, alors que le ratio donne une lecture relative."
    )

    line6 = (
        "Des professionnels peuvent suivre ratio et spread pour repérer des journées où le stress très court terme "
        "sur-réagit ou sous-réagit. Avec tes données, tu peux classer les journées par zones basses, médianes et hautes, "
        "puis observer comment ouvre le SPX dans chacun de ces régimes."
    )

    return [line1,line2,line3,line4,line5,line6]

def _advanced_examples(a1, a2, base):
    examples=[]
    if base.empty:
        return examples
    try:
        hi_ratio = base.sort_values("ratio", ascending=False).head(1)
        lo_ratio = base.sort_values("ratio", ascending=True).head(1)
        hi_spread = base.sort_values("spread", ascending=False).head(1)
        lo_spread = base.sort_values("spread", ascending=True).head(1)
        if not hi_ratio.empty:
            r=hi_ratio.iloc[0]
            examples.append(f"Exemple ratio élevé : le {str(r['date'].date())}, {a1}/{a2} vaut {_fmt_num(r['ratio'],3)}.")
        if not lo_ratio.empty:
            r=lo_ratio.iloc[0]
            examples.append(f"Exemple ratio faible : le {str(r['date'].date())}, {a1}/{a2} vaut {_fmt_num(r['ratio'],3)}.")
        if not hi_spread.empty:
            r=hi_spread.iloc[0]
            examples.append(f"Exemple spread positif : le {str(r['date'].date())}, le spread vaut {_fmt_num(r['spread'],2)}.")
        if not lo_spread.empty:
            r=lo_spread.iloc[0]
            examples.append(f"Exemple spread négatif : le {str(r['date'].date())}, le spread vaut {_fmt_num(r['spread'],2)}.")
    except Exception:
        pass
    return examples[:4]

def _advanced_opening_guidance(a1, a2, p25, p50, p75):
    return [
        f"Quand le ratio {a1}/{a2} est proche de 1, le très court terme ressemble davantage au repère comparé ; cela correspond souvent à une tension plus alignée entre les deux mesures.",
        f"Quand le ratio descend vers ou sous sa zone basse (autour du percentile 25 = {_fmt_num(p25,3)}), le marché price relativement moins de stress immédiat.",
        f"Quand le ratio remonte vers sa zone haute (autour du percentile 75 = {_fmt_num(p75,3)}), le stress court terme devient relativement plus important et il faut être plus attentif à l'ouverture du SPX, aux écarts rapides et aux journées nerveuses."
    ]

def _build_relation_result(a1, a2, df, years=None, month=None, weekday=None):
    base=df.copy()
    base["spread"]=base["close_a"]-base["close_b"]
    base["ratio"]=base["close_a"]/base["close_b"]
    ret_corr=base["ret1d_a"].corr(base["ret1d_b"])
    lvl_corr=base["close_a"].corr(base["close_b"])
    share_gt=float((base["close_a"]>base["close_b"]).mean())*100.0
    ratio_mean=float(base["ratio"].mean())
    spread_mean=float(base["spread"].mean())
    n=len(base)
    p25=float(base["ratio"].quantile(0.25))
    p50=float(base["ratio"].quantile(0.50))
    p75=float(base["ratio"].quantile(0.75))
    period_txt = f"de {str(pd.to_datetime(base['date'].min()).date())} à {str(pd.to_datetime(base['date'].max()).date())}"
    question_rewrite = f"Tu demandes comment {a1} et {a2} se comportent l'un par rapport à l'autre."
    key_message = (
        f"En résumé, {a1} suit globalement {a2} avec une corrélation journalière de {_fmt_num(ret_corr,4)}, "
        f"mais {a1} n'est au-dessus de {a2} que dans {_fmt_pct(share_gt)} des cas."
    )
    explain_lines = _relation_explainer(a1, a2, ret_corr, lvl_corr, ratio_mean, spread_mean, share_gt, p25, p50, p75)
    examples = _advanced_examples(a1, a2, base)
    opening_guidance = _advanced_opening_guidance(a1, a2, p25, p50, p75)

    extra_notes=[]
    if "VIX1D" in (a1, a2):
        extra_notes.append(_vix1d_methodology_note())

    answer_short=f"{a1}/{a2} : corr 1j {_fmt_num(ret_corr,4)} | ratio médian {_fmt_num(p50,3)}"
    answer_long=" ".join([question_rewrite, key_message] + explain_lines)

    preview=base[["date","close_a","close_b","spread","ratio","ret1d_a","ret1d_b"]].head(25).copy()
    preview["date"]=preview["date"].astype(str)
    preview=preview.fillna("")

    return {
        "ok":True,
        "result":{
            "engine":"advanced_pattern_relation",
            "status":"OK",
            "mode":"advanced_pattern",
            "metric":"relation_correlation",
            "target_asset":a1 + " vs " + a2,
            "analysis_type":"Relation / corrélation / ratio / spread",
            "assets_compared":[a1,a2],
            "period_used":period_txt,
            "question_rewrite":question_rewrite,
            "key_message":key_message,
            "explain_lines":explain_lines,
            "extra_notes":extra_notes,
            "examples":examples,
            "opening_guidance":opening_guidance,
            "answer_short":answer_short,
            "answer_long":answer_long,
            "source_file_names":[a1, a2],
            "preview":preview.to_dict("records"),
            "stats":{
                "count":int(n),
                "frequency_label":"journalière",
                "horizon_label":"Analyse de relation",
                "weekday_label":_weekday_name(weekday),
                "ret_corr":None if pd.isna(ret_corr) else float(ret_corr),
                "lvl_corr":None if pd.isna(lvl_corr) else float(lvl_corr),
                "ratio_mean":ratio_mean,
                "ratio_p25":p25,
                "ratio_p50":p50,
                "ratio_p75":p75,
                "spread_mean":spread_mean,
                "share_a_gt_b_pct":share_gt
            }
        }
    }

def execute_advanced_pattern(question):
    reg=build_registry(force=False)
    nq=_norm(question)
    assets=_extract_assets(question, reg)
    years=_parse_years(nq)
    month=_parse_month(nq)
    weekday=_parse_weekday(nq)

    if len(assets) < 2:
        return {"ok":False,"error":"ADVANCED_NEEDS_TWO_ASSETS","detail":"La recherche avancée a besoin d'au moins deux actifs dans la question pour étudier une relation ou un pattern."}

    a1,a2=assets[:2]
    if a1 not in reg:
        return _result_error(a1, reg)
    if a2 not in reg:
        return _result_error(a2, reg)

    d1=_load_csv(reg[a1]["path"])
    d2=_load_csv(reg[a2]["path"])
    if d1 is None:
        return _result_error(a1, reg)
    if d2 is None:
        return _result_error(a2, reg)

    x1=d1[["date","time","year","month","weekday","close","ret1d_pct"]].rename(columns={"close":"close_a","ret1d_pct":"ret1d_a"})
    x2=d2[["date","close","ret1d_pct"]].rename(columns={"close":"close_b","ret1d_pct":"ret1d_b"})
    df=x1.merge(x2,on="date",how="inner")
    df=_filter_time(df, years, month, weekday).dropna(subset=["close_a","close_b"])
    if df.empty:
        return {"ok":False,"error":"NO_MATCH_AFTER_FILTER","detail":"Aucune observation commune après filtres pour la recherche avancée."}

    return _build_relation_result(a1, a2, df, years=years, month=month, weekday=weekday)

def execute_manual_stats(question):
    reg=build_registry(force=False)
    nq=_norm(question)
    assets=_extract_assets(question, reg)
    years=_parse_years(nq)
    month=_parse_month(nq)
    weekday=_parse_weekday(nq)

    if _is_too_complex_for_statistique(nq, assets):
        return {"ok":False,"error":"TOO_COMPLEX_FOR_STATISTIQUE","detail":"Cette question relève d'une recherche avancée / relation / pattern. Passe en mode Avance."}

    def load_asset(t):
        ent=reg.get(t)
        if not ent:
            return None
        return _load_csv(ent["path"])

    if "comparaison" in nq or " vs " in nq:
        if len(assets)<2:
            return {"ok":False,"error":"NO_TWO_ASSETS","detail":"Comparaison impossible sans deux actifs."}
        a1,a2=assets[:2]
        d1,d2=load_asset(a1),load_asset(a2)
        if d1 is None:
            return _result_error(a1,reg)
        if d2 is None:
            return _result_error(a2,reg)
        z1=d1[["date","year","month","weekday","ret_h_1m_pct"]].rename(columns={"ret_h_1m_pct":"r1_pct"})
        z2=d2[["date","ret_h_1m_pct"]].rename(columns={"ret_h_1m_pct":"r2_pct"})
        df=z1.merge(z2,on="date",how="inner")
        df=_filter_time(df,years,month,weekday).dropna(subset=["r1_pct","r2_pct"])
        if df.empty:
            return {"ok":False,"error":"NO_MATCH_AFTER_FILTER","detail":"Aucun cas après filtres."}
        m1,m2=float(df["r1_pct"].mean()),float(df["r2_pct"].mean())
        leader=a1 if m1>=m2 else a2
        diff=abs(m1-m2)
        return {"ok":True,"result":{"engine":"statistical_engine","status":"OK","mode":"comparison","metric":"comparison","target_asset":leader,"answer_short":str(leader) + " surperformant de +" + _fmt_pct(diff),"answer_long":"Il y a eu " + str(len(df)) + " cas comparables entre " + str(pd.to_datetime(df['date'].min()).date()) + " et " + str(pd.to_datetime(df['date'].max()).date()) + ". En moyenne sur 1 mois dans ces conditions, " + str(a1) + " fait " + _fmt_pct(m1) + " et " + str(a2) + " " + _fmt_pct(m2) + ".","source_file_names":[reg[a1]["file_name"],reg[a2]["file_name"]],"preview":df[["date","r1_pct","r2_pct"]].head(20).assign(date=lambda x:x["date"].astype(str)).to_dict("records"),"stats":{"count":int(len(df)),"horizon_label":"1 mois","frequency_label":"journalière","weekday_label":_weekday_name(weekday)}}}

    if not assets:
        return {"ok":False,"error":"NO_ASSET","detail":"Je ne comprends pas cette question ou aucun actif exploitable n'a été détecté."}

    target=assets[0]
    df=load_asset(target)
    if df is None:
        return _result_error(target, reg)
    df=_filter_time(df,years,month,weekday)

    if "taux positif" in nq:
        s=df["ret_h_1d_pct"].dropna()
        if s.empty:
            return {"ok":False,"error":"NO_MATCH_AFTER_FILTER","detail":"Aucun cas après filtres."}
        rate=float((s>0).mean())*100.0
        return {"ok":True,"result":{"engine":"statistical_engine","status":"OK","mode":"positive_rate","metric":"taux_positif","target_asset":target,"answer_short":_fmt_pct(rate),"answer_long":"Il y a eu " + str(len(s)) + " cas entre " + str(pd.to_datetime(df.loc[s.index,'time'].min()).date()) + " et " + str(pd.to_datetime(df.loc[s.index,'time'].max()).date()) + ". Dans " + _fmt_pct(rate) + " des cas, " + str(target) + " est positif sur 1 jour.","source_file_names":[reg[target]["file_name"]],"preview":df.loc[s.index,["time","close","ret1d_pct"]].head(20).rename(columns={"time":"date"}).assign(date=lambda x:x["date"].astype(str)).to_dict("records"),"stats":{"count":int(len(s)),"horizon_label":"1 jour","frequency_label":"journalière","weekday_label":_weekday_name(weekday)}}}

    subset=df.copy()
    answer_long=str(target) + " a été retenu " + str(len(subset)) + " fois."
    thr_up=re.search(r"(?:augmente|monte|hausse) de ([0-9]+(?:[\\.,][0-9]+)?)%?(?: ou plus)?", nq)
    thr_dn=re.search(r"(?:baisse|recule|chute) de ([0-9]+(?:[\\.,][0-9]+)?)%?(?: ou plus| ou moins)?", nq)

    if re.search(r"(a ete en hausse|en hausse)", nq) and thr_up is None:
        subset=df[df["ret1d_pct"]>0].copy()
        answer_long=str(target) + " a été en hausse " + str(len(subset)) + " fois."
    elif re.search(r"(a ete en baisse|en baisse)", nq) and thr_dn is None:
        subset=df[df["ret1d_pct"]<0].copy()
        answer_long=str(target) + " a été en baisse " + str(len(subset)) + " fois."
    elif thr_up is not None:
        thr=float(thr_up.group(1).replace(",","."))
        subset=df[df["ret1d_pct"]>=thr].copy()
        answer_long=str(target) + " a augmenté d'au moins " + str(thr).replace(".", ",") + "% " + str(len(subset)) + " fois."
    elif thr_dn is not None:
        thr=float(thr_dn.group(1).replace(",","."))
        subset=df[df["ret1d_pct"]<=-thr].copy()
        answer_long=str(target) + " a baissé d'au moins " + str(thr).replace(".", ",") + "% " + str(len(subset)) + " fois."
    else:
        m=re.search(r"a ete en dessous de ([0-9]+(?:[\\.,][0-9]+)?)", nq)
        if m is not None:
            thr=float(m.group(1).replace(",","."))
            subset=df[df["close"]<thr].copy()
            answer_long=str(target) + " est resté en dessous de " + str(thr).replace(".", ",") + " " + str(len(subset)) + " fois."

    preview=subset[["time","close"]].head(20).rename(columns={"time":"date"}).copy()
    preview["date"]=preview["date"].astype(str)
    preview=preview.fillna("")
    return {"ok":True,"result":{"engine":"statistical_engine","status":"OK","mode":"count","metric":"count","target_asset":target,"answer_short":str(len(subset)) + " fois","answer_long":answer_long,"source_file_names":[reg[target]["file_name"]],"preview":preview.to_dict("records"),"stats":{"count":int(len(subset)),"frequency_label":"journalière","weekday_label":_weekday_name(weekday)}}}
