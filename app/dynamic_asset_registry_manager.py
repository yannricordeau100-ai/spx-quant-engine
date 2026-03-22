import os,re,json,unicodedata,shutil,hashlib

def nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def safe_slug(s):
    s=nrm(s).upper()
    s=s.replace(" ","_")
    s=re.sub(r"[^A-Z0-9_]+","_",s)
    s=re.sub(r"_+","_",s).strip("_")
    return s

def canonical_filename(fn):
    if not fn.lower().endswith(".csv"):
        return fn,None,None,None
    base=fn[:-4]

    # case 1: ticker only => default daily
    if re.match(r"^[A-Za-z0-9.\-_ ]+$", base, flags=re.I) and not re.search(r"_(daily|30min|1min|5min|1hour|4hours)$", base, flags=re.I):
        ticker=safe_slug(base)
        if not ticker:
            return fn,None,None,None
        out=f"{ticker}_daily.csv"
        asset_base=f"auto_{re.sub(r'[^a-z0-9]+','',ticker.lower())}"
        asset_key=asset_base
        return out,ticker,"daily",asset_key

    # case 2: ticker + frequency
    m=re.match(r"^(.*?)(daily|30min|1min|5min|1hour|4hours)$", base, flags=re.I)
    if m:
        left=m.group(1).rstrip(" _-")
        freq=m.group(2).lower()
    else:
        m2=re.match(r"^(.*?)_(daily|30min|1min|5min|1hour|4hours)$", base, flags=re.I)
        if not m2:
            return fn,None,None,None
        left=m2.group(1)
        freq=m2.group(2).lower()

    ticker=safe_slug(left)
    if not ticker:
        return fn,None,None,None
    out=f"{ticker}_{freq}.csv"
    freq_key={"daily":"daily","30min":"30m","1min":"1m","5min":"5m","1hour":"1h","4hours":"4h"}[freq]
    asset_base=f"auto_{re.sub(r'[^a-z0-9]+','',ticker.lower())}"
    asset_key=asset_base if freq_key=="daily" else f"{asset_base}_{freq_key}"
    return out,ticker,freq_key,asset_key

def folder_signature(upload_dir):
    items=[]
    if not os.path.isdir(upload_dir):
        return "missing"
    for fn in sorted(os.listdir(upload_dir)):
        p=os.path.join(upload_dir,fn)
        if os.path.isfile(p):
            st=os.stat(p)
            items.append(f"{fn}|{st.st_size}|{int(st.st_mtime)}")
    raw="\\n".join(items).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()

def rebuild_dynamic_registry(upload_dir,company_alias_json,registry_json,state_json):
    with open(company_alias_json,"r",encoding="utf-8") as f:
        company_aliases=json.load(f)

    rename_rows=[]
    for fn in sorted(os.listdir(upload_dir)):
        src=os.path.join(upload_dir,fn)
        if not os.path.isfile(src):
            continue
        canon,ticker,freq_key,asset_key=canonical_filename(fn)
        row={"original_file":fn,"canonical_file":canon,"ticker":ticker,"freq_key":freq_key,"asset_key":asset_key,"action":"ignored","exists_after":False}
        if canon is None:
            rename_rows.append(row)
            continue
        dst=os.path.join(upload_dir,canon)
        if os.path.abspath(src)==os.path.abspath(dst):
            row["action"]="kept"; row["exists_after"]=True
        else:
            if os.path.exists(dst):
                row["action"]="target_exists_skip"; row["exists_after"]=True
            else:
                shutil.move(src,dst); row["action"]="renamed"; row["exists_after"]=os.path.exists(dst)
        rename_rows.append(row)

    freq_map={"daily":"daily","30min":"30m","1min":"1m","5min":"5m","1hour":"1h","4hours":"4h"}
    pat=re.compile(r"^([A-Za-z0-9_]+)_(daily|30min|1min|5min|1hour|4hours)\.csv$",re.I)

    auto_rows=[]
    for fn in sorted(os.listdir(upload_dir)):
        path=os.path.join(upload_dir,fn)
        if not os.path.isfile(path):
            continue
        m=pat.match(fn)
        if not m:
            continue
        ticker_raw=m.group(1)
        freq_raw=m.group(2).lower()
        ticker=ticker_raw.upper()
        freq_key=freq_map[freq_raw]
        asset_base=f"auto_{re.sub(r'[^a-z0-9]+','',ticker.lower())}"
        asset_key=asset_base if freq_key=="daily" else f"{asset_base}_{freq_key}"
        aliases=[ticker.lower(),ticker]
        if freq_key=="daily":
            aliases += [f"{ticker.lower()} daily",f"{ticker} daily",f"{ticker.lower()} journalier",f"{ticker} journalier"]
        elif freq_key=="30m":
            aliases += [f"{ticker.lower()} 30m",f"{ticker} 30m",f"{ticker.lower()} 30 min",f"{ticker} 30 min",f"{ticker.lower()} 30min",f"{ticker} 30min"]
        aliases += company_aliases.get(ticker,[])
        auto_rows.append({"ticker":ticker,"freq_key":freq_key,"asset_base":asset_base,"asset_key":asset_key,"path":path,"aliases":sorted(set(aliases))})

    dynamic_config={}; dynamic_aliases={}; dynamic_summary={}
    for r in auto_rows:
        dynamic_config[r["asset_key"]]={"path":r["path"],"kind":"ohlc","family":"equity_upload"}
        dynamic_aliases[r["asset_key"]]=list(r["aliases"])
        base=r["asset_base"]; ticker=r["ticker"]
        dynamic_summary.setdefault(base,{"ticker":ticker,"freqs":{},"base_aliases":sorted(set([ticker.lower(),ticker] + company_aliases.get(ticker,[])))})
        dynamic_summary[base]["freqs"][r["freq_key"]]=r["asset_key"]

    for base,info in dynamic_summary.items():
        if "daily" in info["freqs"]:
            dynamic_config[base]=dynamic_config[info["freqs"]["daily"]]
        elif "30m" in info["freqs"]:
            dynamic_config[base]=dynamic_config[info["freqs"]["30m"]]
        elif "1m" in info["freqs"]:
            dynamic_config[base]=dynamic_config[info["freqs"]["1m"]]
        elif "5m" in info["freqs"]:
            dynamic_config[base]=dynamic_config[info["freqs"]["5m"]]
        else:
            dynamic_config[base]=dynamic_config[list(info["freqs"].values())[0]]
        dynamic_aliases[base]=info["base_aliases"]

    payload={"auto_assets":dynamic_summary,"dynamic_config":dynamic_config,"dynamic_aliases":dynamic_aliases}
    with open(registry_json,"w",encoding="utf-8") as f:
        json.dump(payload,f,ensure_ascii=False,indent=2)

    sig=folder_signature(upload_dir)
    with open(state_json,"w",encoding="utf-8") as f:
        json.dump({"signature":sig},f,ensure_ascii=False,indent=2)

    return {"signature":sig,"auto_assets_detected":len(dynamic_summary),"rebuilt":True,"rename_rows":rename_rows}

def ensure_dynamic_registry_current(upload_dir,company_alias_json,registry_json,state_json):
    current_sig=folder_signature(upload_dir)
    stored_sig=None
    if os.path.exists(state_json):
        try:
            with open(state_json,"r",encoding="utf-8") as f:
                stored_sig=json.load(f).get("signature")
        except:
            stored_sig=None
    needs_rebuild=(stored_sig!=current_sig) or (not os.path.exists(registry_json))
    if needs_rebuild:
        return rebuild_dynamic_registry(upload_dir,company_alias_json,registry_json,state_json)
    with open(registry_json,"r",encoding="utf-8") as f:
        reg=json.load(f)
    return {"signature":current_sig,"auto_assets_detected":len(reg.get("auto_assets",{})),"rebuilt":False,"rename_rows":[]}
