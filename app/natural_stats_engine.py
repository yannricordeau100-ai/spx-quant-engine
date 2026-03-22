import os, re, json, unicodedata
import pandas as pd

ROOT="/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
PROC=os.path.join(ROOT,"processed")
ARCH=os.path.join(PROC,"ETAPE197_ASSET_TIMEFRAME_REGISTRY.json")

MONTHS_FR={
    "janvier":1,"fevrier":2,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,"juillet":7,
    "aout":8,"août":8,"septembre":9,"octobre":10,"novembre":11,"decembre":12,"décembre":12
}
WEEKDAYS_FR={
    "lundi":0,"mardi":1,"mercredi":2,"jeudi":3,"vendredi":4,
    "monday":0,"tuesday":1,"wednesday":2,"thursday":3,"friday":4
}
STOPWORDS_UPPER={"QUESTION","PERFORME","COMBIEN","QUEL","QUELLE","QUELS","QUELLES","FOIS","EST","LES","LE","LA","DU","DE","DES","EN","ET","SUR","QUAND","PLUS","MOINS"}
CORE_ASSETS=["SPX","SPY","QQQ","IWM","VIX","VVIX","VIX9D","DXY","GOLD"]

ASSET_ALIASES={
    "apple":"AAPL",
    "aapl":"AAPL",
    "microsoft":"MSFT",
    "msft":"MSFT",
    "amazon":"AMZN",
    "amzn":"AMZN",
    "google":"GOOGL",
    "alphabet":"GOOGL",
    "googl":"GOOGL",
    "meta":"META",
    "facebook":"META",
    "tesla":"TSLA",
    "tsla":"TSLA",
    "nvidia":"NVDA",
    "nvda":"NVDA",
    "spy":"SPY",
    "spx":"SPX",
    "qqq":"QQQ",
    "vix":"VIX",
    "dxy":"DXY",
}

CANONICAL_DAILY={
    "SPX":["SPX_daily.csv"],
    "SPY":["SPY_daily.csv"],
    "QQQ":["QQQ_daily.csv"],
    "IWM":["IWM_daily.csv"],
    "VIX":["VIX_daily.csv"],
    "VVIX":["VVIX_daily.csv"],
    "VIX9D":["VIX9D_daily.csv"],
    "DXY":["DXY_daily.csv"],
    "GOLD":["GOLD_daily.csv","Gold_daily.csv"],
}

def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def _nrm(x):
    s=str(x or "").lower().strip()
    rep={
        "é":"e","è":"e","ê":"e","ë":"e",
        "à":"a","â":"a","ä":"a",
        "î":"i","ï":"i",
        "ô":"o","ö":"o",
        "ù":"u","û":"u","ü":"u",
        "ç":"c",
    }
    for a,b in rep.items():
        s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'")
    s=re.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=re.sub(r"\s+"," ",s).strip()
    try:
        for alias, ticker in sorted(ASSET_ALIASES.items(), key=lambda kv: -len(kv[0])):
            s=re.sub(rf"(?<![a-z0-9]){re.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    except Exception:
        pass
    return s


def _clean_text(s):
    s=str(s or "")
    s=s.replace(" ete "," été ").replace(" a ete "," a été ")
    s=s.replace("appliquee","appliquée").replace("meme","même")
    s=s.replace(" ,", ",").replace(" .",".")
    s=s.replace(", En moyenne", ". En moyenne")
    s=s.replace(", Dans ", ". Dans ")
    s=s.replace(", Cela ", ". Cela ")
    s=s.replace("..",".").replace(",,",",")
    s=re.sub(r"(\d+)\.(\d+)%", lambda m: f"{m.group(1)},{m.group(2)}%", s)
    s=re.sub(r"\ble ([A-Z]{1,8})\b", r"\1", s)
    s=re.sub(r"\bLe ([A-Z]{1,8})\b", r"\1", s)
    s=re.sub(r"\bla ([A-Z]{1,8})\b", r"\1", s)
    s=re.sub(r"\bLa ([A-Z]{1,8})\b", r"\1", s)
    s=re.sub(r"\bdu ([A-Z]{1,8})\b", r"de \1", s)
    s=re.sub(r"\bDu ([A-Z]{1,8})\b", r"De \1", s)
    s=re.sub(r"\s+"," ",s).strip()
    if s.endswith(","):
        s=s[:-1]+"."
    return s

def _parse_fr_number_series(series):
    s=series.astype(str).str.replace("\u202f","",regex=False).str.replace("\xa0","",regex=False).str.replace(" ","",regex=False)
    s=s.str.replace(",",".",regex=False)
    return pd.to_numeric(s, errors="coerce")

def _read_csv_flex(path):
    last=None
    for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
        for sep in (";",",","\t","|",None):
            try:
                if sep is None:
                    df=pd.read_csv(path, sep=None, engine="python", encoding=enc, on_bad_lines="skip")
                else:
                    df=pd.read_csv(path, sep=sep, engine="python", encoding=enc, on_bad_lines="skip")
                if df is not None and df.shape[1] >= 1:
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
    out=df.copy()
    out.columns=cols
    return out

def _find_time_col(df):
    prefs=["time","date","datetime","timestamp","dt","day","trading_date","session_date"]
    for c in prefs:
        if c in df.columns:
            return c
    for c in df.columns:
        cc=str(c).lower()
        if cc in prefs or "time" in cc or "date" in cc or cc.endswith("_dt") or cc.endswith("_date"):
            return c
    return None

def _find_close_col(df):
    prefs=["close","adj_close","adjusted_close","close_last","last","price","settle","settlement","c"]
    for c in prefs:
        if c in df.columns:
            return c
    for c in df.columns:
        cc=str(c).lower()
        if cc in prefs or "adj_close" in cc or "adjusted_close" in cc or "close" in cc or cc.endswith("_close") or cc=="last_price":
            return c
    return None

def _load_daily_from_path(path):
    df=_read_csv_flex(path)
    df=_norm_cols(df)
    tcol=_find_time_col(df)
    ccol=_find_close_col(df)
    if tcol is None or ccol is None:
        return None
    df["timestamp"]=pd.to_datetime(df[tcol], errors="coerce")
    df["close"]=_parse_fr_number_series(df[ccol])
    df=df[df["timestamp"].notna() & df["close"].notna()].copy().sort_values("timestamp")
    if len(df)==0:
        return None
    df["date"]=df["timestamp"].dt.floor("D")
    daily=df.groupby("date", as_index=False)["close"].last().copy()
    daily["ret1d"]=daily["close"].pct_change()
    daily["weekday"]=daily["date"].dt.weekday
    return daily

def _load_core_registry():
    raw={}
    if os.path.exists(ARCH):
        try:
            raw=json.load(open(ARCH,"r",encoding="utf-8"))
        except Exception:
            raw={}
    return raw.get("assets",{}) or {}

def _best_daily_entry(asset):
    reg=_load_core_registry().get(asset,[]) or []
    wanted=CANONICAL_DAILY.get(asset,[])
    for fn in wanted:
        for x in reg:
            if str(x.get("file_name","")) == fn and os.path.exists(x.get("path","")):
                return x
    daily=[x for x in reg if (x.get("bar_minutes") or 0) >= 1440 or "_daily" in str(x.get("file_name","")).lower()]
    if daily:
        daily=sorted(daily,key=lambda x:(0 if str(x.get("file_name","")) in wanted else 1, x.get("bar_minutes") or 10**9, str(x.get("file_name",""))))
        return daily[0]
    return None

def _path_is_aau_like(root_path):
    norm=root_path.replace(chr(92),"/").upper()
    norm_simple=re.sub(r"[^A-Z0-9]+"," ", norm)
    if " AAU " in f" {norm_simple} ":
        return True
    has_autres="AUTRES" in norm_simple
    has_action=("ACTION" in norm_simple) or ("ACTIONS" in norm_simple)
    has_upload="UPLOAD" in norm_simple
    return has_autres and has_action and has_upload

def _discover_aau_files():
    out={}
    for root, dirs, files in os.walk(ROOT):
        if not _path_is_aau_like(root):
            continue
        for fn in files:
            if not fn.lower().endswith(".csv"):
                continue
            base=os.path.splitext(fn)[0]
            ticker=re.split(r"[_\-\ ]+", base)[0].upper()
            if not re.fullmatch(r"[A-Z][A-Z0-9\.]{0,8}", ticker):
                continue
            out.setdefault(ticker,[]).append(os.path.join(root,fn))
    return out

def _build_source_registry():
    reg={}
    for asset in CORE_ASSETS:
        ent=_best_daily_entry(asset)
        if ent and os.path.exists(ent.get("path","")):
            df=_load_daily_from_path(ent["path"])
            if df is not None and len(df)>0:
                reg[asset]={"df":df,"file_name":ent.get("file_name"),"path":ent.get("path"),"kind":"core"}
    for ticker, paths in _discover_aau_files().items():
        if ticker in reg:
            continue
        chosen=None; dfx=None
        for p in sorted(paths, key=lambda x: ("/portable_backup_temp/" in x.replace(chr(92),"/").lower(), len(x))):
            try:
                tmp=_load_daily_from_path(p)
                if tmp is not None and len(tmp)>20:
                    chosen=p; dfx=tmp; break
            except Exception:
                pass
        if chosen is not None:
            reg[ticker]={"df":dfx,"file_name":os.path.basename(chosen),"path":chosen,"kind":"aau"}
    return reg

def _detect_assets_in_order(question, reg):
    q_up=str(question or "").upper()
    tokens=re.findall(r"\b[A-Z][A-Z0-9\.]{1,8}\b", q_up)
    ordered=[]
    for t in tokens:
        if t in STOPWORDS_UPPER:
            continue
        if t in reg and t not in ordered:
            ordered.append(t)

    explicit_patterns=[
        r"PERFORMANCE DE ([A-Z][A-Z0-9\.]{1,8})",
        r"PERFORMANCE MOYENNE DE ([A-Z][A-Z0-9\.]{1,8})",
        r"TAUX POSITIF DE ([A-Z][A-Z0-9\.]{1,8})",
        r"COMBIEN DE FOIS ([A-Z][A-Z0-9\.]{1,8})",
    ]
    for pat in explicit_patterns:
        m=re.search(pat, q_up)
        if m:
            t=m.group(1)
            if t in reg:
                ordered=[t]+[x for x in ordered if x!=t]
                break

    m=re.search(r"COMPARAISON ([A-Z][A-Z0-9\.]{1,8}) VS ([A-Z][A-Z0-9\.]{1,8})", q_up)
    if m:
        exp=[]
        for t in [m.group(1),m.group(2)]:
            if t in reg and t not in exp:
                exp.append(t)
        ordered=exp+[x for x in ordered if x not in exp]

    return ordered

def _parse_month_year_weekday(nq):
    month=None
    for k,v in MONTHS_FR.items():
        if re.search(rf"(?<![a-z0-9]){re.escape(_nrm(k))}(?![a-z0-9])", nq):
            month=v; break
    m=re.search(r"\b(20\d{2})\b", nq)
    year=int(m.group(1)) if m else None
    weekday=None; weekday_label=None
    for k,v in WEEKDAYS_FR.items():
        if re.search(rf"(?<![a-z0-9]){re.escape(_nrm(k))}(?![a-z0-9])", nq):
            weekday=v; weekday_label=k; break
    return month,year,weekday,weekday_label

def _filter_df(df, month=None, year=None, weekday=None):
    out=df.copy()
    if year is not None:
        out=out[out["date"].dt.year==year]
    if month is not None:
        out=out[out["date"].dt.month==month]
    if weekday is not None:
        out=out[out["weekday"]==weekday]
    return out

def _question_kind(nq):
    if "comparaison" in nq or " vs " in nq:
        return "comparison"
    if "taux positif" in nq:
        return "positive_rate"
    if "positif combien de fois" in nq or "combien de fois" in nq:
        return "count"
    if "performance moyenne" in nq or "performe comment en moyenne" in nq or "performance de" in nq or "quelle est la performance" in nq:
        return "mean_perf"
    if "performance" in nq:
        return "mean_perf"
    return None

def _parse_horizon_days(nq):
    m=re.search(r"sur\s+(\d+)\s+(jour|jours|semaine|semaines|mois|month|months|an|ans|annee|annees|year|years)", nq)
    if not m:
        return 1, "1 jour"
    n=int(m.group(1)); u=m.group(2)
    if u.startswith("jour"): return n, f"{n} jour" if n==1 else f"{n} jours"
    if u.startswith("semaine"): return n*5, f"{n} semaine" if n==1 else f"{n} semaines"
    if u.startswith("mois") or u.startswith("month"): return n*21, f"{n} mois"
    return n*252, f"{n} an" if n==1 else f"{n} ans"

def _parse_count_condition(nq, target):
    t=target.lower()

    m=re.search(rf"{t}[^\n]*?(?:a augmente|a augmenté|monte|monté|hausse|progresse|progressé|gagne|pris)\s+de\s+(\d+(?:\.\d+)?)\s*%", nq)
    if m:
        return ("ret_gte_pct", float(m.group(1)), f"à au moins +{m.group(1)}% sur 1 jour")

    m=re.search(rf"{t}[^\n]*?(?:a baisse|a baissé|baisse|chute|a chute|a chuté|recule|perd)\s+de\s+(\d+(?:\.\d+)?)\s*%", nq)
    if m:
        return ("ret_lte_pct", -float(m.group(1)), f"à au moins -{m.group(1)}% sur 1 jour")

    m=re.search(rf"{t}[^\n]*?(?:cloture|clôture|cloturé|clôturé|cloturee|clôturée)[^\n]*?(?:plus de|superieur a|supérieur à|au dessus de|>)\s*(-?\d+(?:\.\d+)?)\s*%", nq)
    if m:
        return ("ret_gt_pct", float(m.group(1)), f"à plus de {m.group(1)}%")

    m=re.search(rf"{t}[^\n]*?(?:cloture|clôture|cloturé|clôturé|cloturee|clôturée)[^\n]*?(?:moins de|inferieur a|inférieur à|en dessous de|<)\s*(-?\d+(?:\.\d+)?)\s*%", nq)
    if m:
        return ("ret_lt_pct", float(m.group(1)), f"à moins de {m.group(1)}%")

    # natural level phrasing: "VIX a été en dessous de 17"
    m=re.search(rf"{t}[^\n]*?(?:a ete|a été|est)?[^\n]*?(?:en dessous de|inferieur a|inférieur à|<)\s*(-?\d+(?:\.\d+)?)\b", nq)
    if m:
        return ("close_lt", float(m.group(1)), f"en dessous de {m.group(1)}")

    m=re.search(rf"{t}[^\n]*?(?:a ete|a été|est)?[^\n]*?(?:au dessus de|au-dessus de|superieur a|supérieur à|>)\s*(-?\d+(?:\.\d+)?)\b", nq)
    if m:
        return ("close_gt", float(m.group(1)), f"au-dessus de {m.group(1)}")

    m=re.search(rf"{t}[^\n]*?(?:cours|close|cloture|clôture|prix|niveau)[^\n]*?(?:en dessous de|inferieur a|inférieur à|<)\s*(-?\d+(?:\.\d+)?)", nq)
    if m:
        return ("close_lt", float(m.group(1)), f"en dessous de {m.group(1)}")

    m=re.search(rf"{t}[^\n]*?(?:cours|close|cloture|clôture|prix|niveau)[^\n]*?(?:au dessus de|au-dessus de|superieur a|supérieur à|>)\s*(-?\d+(?:\.\d+)?)", nq)
    if m:
        return ("close_gt", float(m.group(1)), f"au-dessus de {m.group(1)}")

    if "positif" in nq:
        return ("positive_close", None, "positif")
    return (None,None,"dans le filtre demandé")

def _parse_directional_condition(nq, condition_asset):
    a=condition_asset.lower()
    if re.search(rf"quand\s+{a}\s+est\s+en\s+hausse", nq):
        return ("up", condition_asset)
    if re.search(rf"quand\s+{a}\s+est\s+en\s+baisse", nq):
        return ("down", condition_asset)
    return (None,None)

def can_handle(question):
    reg=_build_source_registry()
    nq=_nrm(question)
    kind=_question_kind(nq)
    assets=_detect_assets_in_order(question, reg)
    if kind is None or not assets:
        return False
    if kind=="comparison" and len(assets) < 2:
        return False
    return True

def run(question, preview_rows=20):
    reg=_build_source_registry()
    nq=_nrm(question)
    kind=_question_kind(nq)
    assets=_detect_assets_in_order(question, reg)
    if kind is None or not assets:
        return {"engine":"natural_stats_engine","status":"NO_MATCH","answer":"Question non gérée."}

    month,year,weekday,weekday_label=_parse_month_year_weekday(nq)
    horizon_days,horizon_label=_parse_horizon_days(nq)

    if kind=="comparison":
        a1,a2=assets[0],assets[1]
        d1=_filter_df(reg[a1]["df"], month, year, weekday).copy()
        d2=_filter_df(reg[a2]["df"], month, year, weekday).copy()
        d1["ret_h"]=d1["close"].shift(-horizon_days)/d1["close"] - 1.0
        d2["ret_h"]=d2["close"].shift(-horizon_days)/d2["close"] - 1.0
        merged=d1[["date","ret_h"]].rename(columns={"ret_h":"r1"}).merge(d2[["date","ret_h"]].rename(columns={"ret_h":"r2"}), on="date", how="inner")
        merged=merged[merged["r1"].notna() & merged["r2"].notna()].copy()
        if len(merged)==0:
            return {"engine":"natural_stats_engine","status":"NO_RESULT","answer_short":"Aucun cas","answer_long":"Aucun cas exploitable n'a été trouvé pour cette comparaison.","comparison_assets":[a1,a2],"source_file_names":[reg[a1]["file_name"],reg[a2]["file_name"]],"preview":[]}
        m1=float(merged["r1"].mean()); m2=float(merged["r2"].mean())
        leader=a1 if m1>=m2 else a2
        lagger=a2 if leader==a1 else a1
        diff=(max(m1,m2)-min(m1,m2))*100.0
        count=int(len(merged))
        start=str(merged["date"].min().date()); end=str(merged["date"].max().date())
        return {
            "engine":"natural_stats_engine","status":"OK","mode":"comparison","metric":"comparison",
            "target_asset":leader,
            "answer_short":f"{leader} surperformant de {diff:+.2f}%".replace(".",","),
            "answer_long":_clean_text(f"Cela est arrivé {count} fois entre {start} et {end}. En moyenne dans ces conditions, {leader} fait {(max(m1,m2)*100):.2f}% et {lagger} {(min(m1,m2)*100):.2f}%.".replace(".",",")),
            "comparison_assets":[a1,a2],
            "source_file_names":[reg[a1]["file_name"],reg[a2]["file_name"]],
            "preview":merged.assign(r1_pct=(merged["r1"]*100).round(2),r2_pct=(merged["r2"]*100).round(2))[["date","r1_pct","r2_pct"]].head(preview_rows).to_dict(orient="records"),
            "stats":{"count":count,"horizon_label":horizon_label,"weekday_label":weekday_label}
        }

    target=assets[0]
    df=_filter_df(reg[target]["df"], month, year, weekday).copy()

    if len(assets) >= 2:
        cond_mode, cond_asset = _parse_directional_condition(nq, assets[1])
        if cond_mode is not None and cond_asset in reg:
            cdf=_filter_df(reg[cond_asset]["df"], month, year, weekday).copy()
            cdf=cdf[["date","ret1d"]].rename(columns={"ret1d":"cond_ret1d"})
            df=df.merge(cdf,on="date",how="inner")
            if cond_mode=="up":
                df=df[df["cond_ret1d"]>0]
            elif cond_mode=="down":
                df=df[df["cond_ret1d"]<0]

    start=str(df["date"].min().date()) if len(df)>0 else None
    end=str(df["date"].max().date()) if len(df)>0 else None

    if kind=="mean_perf":
        df["ret_h"]=df["close"].shift(-horizon_days)/df["close"] - 1.0
        s=df["ret_h"].dropna()
        if len(s)==0:
            return {"engine":"natural_stats_engine","status":"NO_RESULT","answer_short":"Aucun cas","answer_long":"Aucun cas exploitable n'a été trouvé pour cette question.","source_file_names":[reg[target]["file_name"]],"preview":[]}
        mean=float(s.mean()); count=int(len(s))
        return {
            "engine":"natural_stats_engine","status":"OK","mode":"mean_perf","metric":"moyenne_variation","target_asset":target,
            "answer_short":f"{mean*100:.2f}%".replace(".",","),
            "answer_long":_clean_text(f"Il y a eu {count} cas entre {start} et {end}. En moyenne dans ces conditions, {target} varie de {mean*100:.2f}% sur {horizon_label}.".replace(".",",")),
            "source_file_names":[reg[target]["file_name"]],
            "preview":df.assign(ret_h_pct=(df["ret_h"]*100).round(2))[["date","close","ret_h_pct"]].head(preview_rows).to_dict(orient="records"),
            "stats":{"count":count,"moyenne_variation":mean,"horizon_label":horizon_label,"weekday_label":weekday_label}
        }

    if kind=="positive_rate":
        s=df["ret1d"].dropna()
        if len(s)==0:
            return {"engine":"natural_stats_engine","status":"NO_RESULT","answer_short":"Aucun cas","answer_long":"Aucun cas exploitable n'a été trouvé pour cette question.","source_file_names":[reg[target]["file_name"]],"preview":[]}
        taux=float((s>0).mean()); count=int(len(s))
        return {
            "engine":"natural_stats_engine","status":"OK","mode":"positive_rate","metric":"taux_positif","target_asset":target,
            "answer_short":f"{taux*100:.2f}%".replace(".",","),
            "answer_long":_clean_text(f"Il y a eu {count} cas entre {start} et {end}. Dans {taux*100:.2f}% des cas, {target} est positif sur 1 jour.".replace(".",",")),
            "source_file_names":[reg[target]["file_name"]],
            "preview":df.assign(ret1d_pct=(df["ret1d"]*100).round(2))[["date","close","ret1d_pct"]].head(preview_rows).to_dict(orient="records"),
            "stats":{"count":count,"taux_positif":taux,"horizon_label":"1 jour","weekday_label":weekday_label}
        }

    if kind=="count":
        cond_type, cond_value, cond_txt = _parse_count_condition(nq, target)
        work=df.copy()
        if cond_type=="ret_gt_pct":
            work=work[(work["ret1d"]*100.0) > cond_value]
        elif cond_type=="ret_gte_pct":
            work=work[(work["ret1d"]*100.0) >= cond_value]
        elif cond_type=="ret_lt_pct":
            work=work[(work["ret1d"]*100.0) < cond_value]
        elif cond_type=="ret_lte_pct":
            work=work[(work["ret1d"]*100.0) <= cond_value]
        elif cond_type=="close_lt":
            work=work[work["close"] < cond_value]
        elif cond_type=="close_gt":
            work=work[work["close"] > cond_value]
        elif cond_type=="positive_close":
            work=work[work["ret1d"] > 0]
        else:
            work=work[work["ret1d"].notna()]
        count=int(len(work))

        month_name_map={1:"janvier",2:"février",3:"mars",4:"avril",5:"mai",6:"juin",7:"juillet",8:"août",9:"septembre",10:"octobre",11:"novembre",12:"décembre"}
        ctx_parts=[]
        if month is not None and year is not None:
            ctx_parts.append(f"En {month_name_map[month]} {year}")
        elif year is not None:
            ctx_parts.append(f"En {year}")
        if weekday_label is not None:
            ctx_parts.append(f"les {weekday_label}")
        prefix=(" ".join(ctx_parts)).strip()

        if cond_type=="ret_gt_pct":
            body=f"{target} a clôturé {count} fois à plus de {cond_value:g}%"
        elif cond_type=="ret_gte_pct":
            body=f"{target} a augmenté d'au moins {cond_value:g}% {count} fois"
        elif cond_type=="ret_lt_pct":
            body=f"{target} a clôturé {count} fois à moins de {cond_value:g}%"
        elif cond_type=="ret_lte_pct":
            body=f"{target} a baissé d'au moins {abs(cond_value):g}% {count} fois"
        elif cond_type=="close_lt":
            body=f"{target} a été {count} fois en dessous de {cond_value:g}"
        elif cond_type=="close_gt":
            body=f"{target} a été {count} fois au-dessus de {cond_value:g}"
        elif cond_type=="positive_close":
            body=f"{target} a été positif {count} fois"
        else:
            body=f"{target} a été retenu {count} fois"

        answer_long=((prefix + ", " + body) if prefix else body) + "."
        answer_long=_clean_text(answer_long)

        return {
            "engine":"natural_stats_engine","status":"OK","mode":"count","metric":"count","target_asset":target,
            "answer_short":f"{count} fois",
            "answer_long":answer_long,
            "source_file_names":[reg[target]["file_name"]],
            "preview":work[["date","close"]].head(preview_rows).to_dict(orient="records"),
            "stats":{"count":count,"weekday_label":weekday_label}
        }

    return {"engine":"natural_stats_engine","status":"NO_RESULT","answer":"Question non gérée."}

# === ETAPE264_AAU_CONSOLIDATION_BLOCK_START ===
import re as _re_et264
from functools import wraps as _wraps_et264

ET264_ASSET_ALIASES = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT",
    "amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL",
    "facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA",
    "nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}

def _et264_norm_text(x):
    s=str(x or "").lower().strip()
    rep={
        "é":"e","è":"e","ê":"e","ë":"e",
        "à":"a","â":"a","ä":"a",
        "î":"i","ï":"i",
        "ô":"o","ö":"o",
        "ù":"u","û":"u","ü":"u",
        "ç":"c",
    }
    for a,b in rep.items():
        s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'")
    s=s.replace("août","aout")
    s=_re_et264.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re_et264.sub(r"\s+"," ",s).strip()
    return s

def _et264_canonicalize_aliases_in_text(x):
    s=_et264_norm_text(x)
    for alias, ticker in sorted(ET264_ASSET_ALIASES.items(), key=lambda kv: -len(kv[0])):
        s=_re_et264.sub(rf"(?<![a-z0-9]){_re_et264.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s

def _nrm(x):
    return _et264_canonicalize_aliases_in_text(x)

def _et264_find_registered_assets(reg):
    out=[]
    try:
        for k in reg.keys():
            sk=str(k or "").upper().strip()
            if sk and sk not in out:
                out.append(sk)
    except Exception:
        pass
    return out

def _detect_assets_in_order(question, reg):
    q_raw=str(question or "")
    nq=_et264_canonicalize_aliases_in_text(q_raw)
    reg_assets=_et264_find_registered_assets(reg)
    ordered=[]

    # 1) aliases / natural names first
    for alias, ticker in sorted(ET264_ASSET_ALIASES.items(), key=lambda kv: -len(kv[0])):
        alias_n=_et264_norm_text(alias)
        if _re_et264.search(rf"(?<![a-z0-9]){_re_et264.escape(alias_n)}(?![a-z0-9])", _et264_norm_text(q_raw)):
            if ticker in reg_assets and ticker not in ordered:
                ordered.append(ticker)

    # 2) canonical tickers in normalized text
    for t in reg_assets:
        if _re_et264.search(rf"(?<![a-z0-9]){_re_et264.escape(t.lower())}(?![a-z0-9])", nq):
            if t not in ordered:
                ordered.append(t)

    # 3) explicit comparison ordering
    m=_re_et264.search(r"\bcomparaison\s+([a-z0-9\.]+)\s+vs\s+([a-z0-9\.]+)\b", nq)
    if m:
        exp=[]
        for raw in [m.group(1), m.group(2)]:
            can=ET264_ASSET_ALIASES.get(raw.lower(), raw.upper())
            if can in reg_assets and can not in exp:
                exp.append(can)
        ordered=exp+[x for x in ordered if x not in exp]

    # 4) "performance de X quand Y" => X first
    m=_re_et264.search(r"\bperformance\s+de\s+([a-z0-9\.]+)\b", nq)
    if m:
        raw=m.group(1)
        can=ET264_ASSET_ALIASES.get(raw.lower(), raw.upper())
        if can in reg_assets:
            ordered=[can]+[x for x in ordered if x!=can]

    # 5) fallback: if still empty but alias exists
    if not ordered:
        for token in nq.split():
            can=ET264_ASSET_ALIASES.get(token.lower())
            if can in reg_assets and can not in ordered:
                ordered.append(can)

    return ordered

def _parse_count_condition(nq, target):
    s=_et264_canonicalize_aliases_in_text(nq)
    target_n=_et264_norm_text(target)

    def _first_pct():
        m=_re_et264.search(r'(\d+(?:[.,]\d+)?)\s*%', s)
        return float(m.group(1).replace(",", ".")) if m else None

    v=_first_pct()

    # Explicit % up/down semantics
    if v is not None:
        if _re_et264.search(rf"(?:{_re_et264.escape(target_n)}).*?(?:a augmente|a monte|a progresse|a ete en hausse|a ete positif|a été en hausse|a été positif)", s):
            return ("pct_up", v, f"a augmenté d'au moins {str(v).replace('.',',')}%")
        if _re_et264.search(rf"(?:{_re_et264.escape(target_n)}).*?(?:a baisse|a recule|a chute|a ete en baisse|a ete negatif|a été en baisse|a été négatif)", s):
            return ("pct_down", v, f"a baissé d'au moins {str(v).replace('.',',')}%")
        if _re_et264.search(r"(?:a augmente|a monte|a progresse|en hausse|positif)", s):
            return ("pct_up", v, f"a augmenté d'au moins {str(v).replace('.',',')}%")
        if _re_et264.search(r"(?:a baisse|a recule|a chute|en baisse|negatif)", s):
            return ("pct_down", v, f"a baissé d'au moins {str(v).replace('.',',')}%")
        if _re_et264.search(r"(?:a cloture a plus de|a clôturé à plus de|plus de)", s):
            return ("pct_up", v, f"a clôturé à plus de {str(v).replace('.',',')}%")
        if _re_et264.search(r"(?:a cloture a moins de|a clôturé à moins de|moins de)", s):
            return ("pct_down", v, f"a clôturé à moins de -{str(v).replace('.',',')}%")

    # semantic daily state, no explicit %
    if _re_et264.search(r"(?:a ete en hausse|a été en hausse|a ete positif|a été positif|est positif|en hausse)", s):
        return ("sign_up", 0.0, "a été en hausse")
    if _re_et264.search(r"(?:a ete en baisse|a été en baisse|a ete negatif|a été négatif|est negatif|en baisse)", s):
        return ("sign_down", 0.0, "a été en baisse")

    # price / level threshold
    m_between=_re_et264.search(r'entre\s+(\d+(?:[.,]\d+)?)\s+et\s+(\d+(?:[.,]\d+)?)', s)
    if m_between:
        a=float(m_between.group(1).replace(",", "."))
        b=float(m_between.group(2).replace(",", "."))
        return ("between_level", (a,b), f"entre {str(a).replace('.',',')} et {str(b).replace('.',',')}")
    m_lt=_re_et264.search(r'en dessous de\s+(\d+(?:[.,]\d+)?)', s)
    if m_lt:
        a=float(m_lt.group(1).replace(",", "."))
        return ("lt_level", a, f"en dessous de {str(a).replace('.',',')}")
    m_gt=_re_et264.search(r'au dessus de\s+(\d+(?:[.,]\d+)?)', s)
    if m_gt:
        a=float(m_gt.group(1).replace(",", "."))
        return ("gt_level", a, f"au dessus de {str(a).replace('.',',')}")

    return (None, None, "dans le filtre demandé")

def _et264_choose_target_from_question(question, detected_assets):
    nq=_et264_canonicalize_aliases_in_text(question)
    assets=list(detected_assets or [])
    if not assets:
        return None

    patterns=[
        r"\bperformance\s+de\s+([a-z0-9\.]+)\b",
        r"\btaux positif\s+de\s+([a-z0-9\.]+)\b",
        r"\bquelle est la performance moyenne de\s+([a-z0-9\.]+)\b",
        r"\bquel est le taux positif de\s+([a-z0-9\.]+)\b",
        r"\bcombien de fois\s+([a-z0-9\.]+)\b",
        r"\bcomparaison\s+([a-z0-9\.]+)\s+vs\s+([a-z0-9\.]+)\b",
    ]
    for pat in patterns:
        m=_re_et264.search(pat, nq)
        if m:
            raw=m.group(1)
            can=ET264_ASSET_ALIASES.get(raw.lower(), raw.upper())
            if can in assets:
                return can
    return assets[0]

def _et264_patch_count_mask_logic():
    g=globals()
    if "_et264_count_logic_patched" in g:
        return
    g["_et264_count_logic_patched"]=True

    # Try to wrap a main public function if present
    candidate_names=[
        "run_natural_stats_engine",
        "execute_natural_stats_query",
        "answer_natural_stats_question",
        "run_natural_stats_query",
        "natural_stats_answer",
    ]
    for name in candidate_names:
        fn=g.get(name)
        if callable(fn):
            @_wraps_et264(fn)
            def _wrapped(question, *args, __fn=fn, **kwargs):
                q2=_et264_canonicalize_aliases_in_text(question)
                return __fn(q2, *args, **kwargs)
            g[name]=_wrapped
            break

_et264_patch_count_mask_logic()
# === ETAPE264_AAU_CONSOLIDATION_BLOCK_END ===


# === ETAPE264B_AAU_REGISTRY_PATCH_START ===
import os as _os264b, re as _re264b, csv as _csv264b, io as _io264b

_ET264B_ALIASES = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT",
    "amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL",
    "facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA",
    "nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}
_ET264B_AAU_DIRS = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]

def _et264b_nrm(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264b.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264b.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264B_ALIASES.items(), key=lambda kv: -len(kv[0])):
        s=_re264b.sub(rf"(?<![a-z0-9]){_re264b.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s

def _nrm(x):
    return _et264b_nrm(x)

def _et264b_detect_delim(path):
    try:
        with open(path,"r",encoding="utf-8",errors="ignore") as f:
            head=f.read(2048)
        try:
            return _csv264b.Sniffer().sniff(head, delimiters=";,|\t,").delimiter
        except Exception:
            return ";" if ";" in head else ","
    except Exception:
        return ";"

def _et264b_count_rows(path):
    try:
        with open(path,"r",encoding="utf-8",errors="ignore") as f:
            return max(sum(1 for _ in f)-1, 0)
    except Exception:
        return None

def _et264b_find_aau_csv(ticker):
    t=str(ticker or "").upper().strip()
    if not t: return None
    cands=[]
    for d in _ET264B_AAU_DIRS:
        if _os264b.path.isdir(d):
            cands += [
                _os264b.path.join(d, f"{t}_daily.csv"),
                _os264b.path.join(d, f"{t}.csv"),
                _os264b.path.join(d, f"{t.lower()}_daily.csv"),
                _os264b.path.join(d, f"{t.lower()}.csv"),
            ]
            try:
                for name in _os264b.listdir(d):
                    up=name.upper()
                    if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                        cands.append(_os264b.path.join(d,name))
            except Exception:
                pass
    for p in cands:
        if _os264b.path.exists(p):
            return _os264b.path.abspath(p)
    return None

def _et264b_inject_aau_registry():
    reg_names=["CANONICAL_DAILY","CANONICAL_DATASETS","DATASET_REGISTRY","DAILY_REGISTRY","AAU_REGISTRY"]
    assets=set()
    for d in _ET264B_AAU_DIRS:
        if _os264b.path.isdir(d):
            try:
                for name in _os264b.listdir(d):
                    if name.lower().endswith(".csv"):
                        m=_re264b.match(r"([A-Za-z0-9\.]+)", name)
                        if m:
                            assets.add(m.group(1).upper())
            except Exception:
                pass
    for t in sorted(assets):
        path=_et264b_find_aau_csv(t)
        if not path: 
            continue
        entry={
            "file_name": _os264b.path.basename(path),
            "path": path,
            "kind": "aau",
            "n_rows": _et264b_count_rows(path),
            "delimiter": _et264b_detect_delim(path),
        }
        for rn in reg_names:
            g=globals().get(rn)
            if isinstance(g, dict):
                cur=g.get(t)
                if cur is None:
                    g[t]=entry.copy()
                elif isinstance(cur, dict):
                    cur.setdefault("file_name", entry["file_name"])
                    cur["path"]=entry["path"]
                    cur["kind"]="aau"
                    cur.setdefault("n_rows", entry["n_rows"])
                    cur.setdefault("delimiter", entry["delimiter"])
                elif isinstance(cur, str):
                    g[t]=entry.copy()
    g=globals()
    for t in assets:
        path=_et264b_find_aau_csv(t)
        if path and t not in g:
            g[t]=path

def _detect_assets_in_order(question, reg):
    q_raw=str(question or "")
    nq=_et264b_nrm(q_raw)
    reg_assets=[]
    try:
        reg_assets=[str(k).upper() for k in reg.keys()]
    except Exception:
        pass
    ordered=[]
    for alias,ticker in sorted(_ET264B_ALIASES.items(), key=lambda kv: -len(kv[0])):
        if _re264b.search(rf"(?<![a-z0-9]){_re264b.escape(_et264b_nrm(alias))}(?![a-z0-9])", _et264b_nrm(q_raw)):
            if ticker not in ordered and (ticker in reg_assets or _et264b_find_aau_csv(ticker)):
                ordered.append(ticker)
    for t in reg_assets:
        if _re264b.search(rf"(?<![a-z0-9]){_re264b.escape(t.lower())}(?![a-z0-9])", nq):
            if t not in ordered: ordered.append(t)
    m=_re264b.search(r"\bcomparaison\s+([a-z0-9\.]+)\s+vs\s+([a-z0-9\.]+)\b", nq)
    if m:
        pref=[]
        for raw in [m.group(1),m.group(2)]:
            can=_ET264B_ALIASES.get(raw.lower(), raw.upper())
            if can not in pref and (can in reg_assets or _et264b_find_aau_csv(can)): pref.append(can)
        ordered=pref+[x for x in ordered if x not in pref]
    m=_re264b.search(r"\bperformance\s+de\s+([a-z0-9\.]+)\b", nq)
    if m:
        can=_ET264B_ALIASES.get(m.group(1).lower(), m.group(1).upper())
        if can in ordered:
            ordered=[can]+[x for x in ordered if x!=can]
        elif _et264b_find_aau_csv(can):
            ordered=[can]+ordered
    return ordered

def _parse_count_condition(nq, target):
    s=_et264b_nrm(nq)
    m=_re264b.search(r'(\d+(?:[.,]\d+)?)\s*%', s)
    v=float(m.group(1).replace(",", ".")) if m else None
    if v is not None:
        if _re264b.search(r"(a augmente|augmente|en hausse|positif)", s): return ("pct_up", v, f"a augmenté d'au moins {str(v).replace('.',',')}%")
        if _re264b.search(r"(a baisse|baisse|en baisse|negatif)", s): return ("pct_down", v, f"a baissé d'au moins {str(v).replace('.',',')}%")
        if _re264b.search(r"(a cloture a plus de|a clôturé à plus de|plus de)", s): return ("pct_up", v, f"a clôturé à plus de {str(v).replace('.',',')}%")
        if _re264b.search(r"(a cloture a moins de|a clôturé à moins de|moins de)", s): return ("pct_down", v, f"a clôturé à moins de -{str(v).replace('.',',')}%")
    if _re264b.search(r"(a ete en hausse|a été en hausse|a ete positif|a été positif|est positif|en hausse)", s):
        return ("sign_up", 0.0, "a été en hausse")
    if _re264b.search(r"(a ete en baisse|a été en baisse|a ete negatif|a été négatif|est negatif|en baisse)", s):
        return ("sign_down", 0.0, "a été en baisse")
    m_lt=_re264b.search(r'en dessous de\s+(\d+(?:[.,]\d+)?)', s)
    if m_lt: return ("lt_level", float(m_lt.group(1).replace(",",".")), f"en dessous de {m_lt.group(1).replace('.',',')}")
    m_gt=_re264b.search(r'au dessus de\s+(\d+(?:[.,]\d+)?)', s)
    if m_gt: return ("gt_level", float(m_gt.group(1).replace(",",".")), f"au dessus de {m_gt.group(1).replace('.',',')}")
    return (None,None,"dans le filtre demandé")

_et264b_inject_aau_registry()
# === ETAPE264B_AAU_REGISTRY_PATCH_END ===


# === ETAPE264C_AAU_PATCH_START ===
import os as _os264c, re as _re264c, csv as _csv264c

_ET264C_ALIASES = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT",
    "amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL",
    "facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA",
    "nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}
_ET264C_AAU_DIRS = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]

def _et264c_norm(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264c.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264c.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264C_ALIASES.items(), key=lambda kv: -len(kv[0])):
        s=_re264c.sub(rf"(?<![a-z0-9]){_re264c.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s

def _nrm(x):
    return _et264c_norm(x)

def _et264c_find_aau_csv(ticker):
    t=str(ticker or "").upper().strip()
    if not t: return None
    for d in _ET264C_AAU_DIRS:
        if not _os264c.path.isdir(d): 
            continue
        cands=[
            _os264c.path.join(d,f"{t}_daily.csv"),
            _os264c.path.join(d,f"{t}.csv"),
            _os264c.path.join(d,f"{t.lower()}_daily.csv"),
            _os264c.path.join(d,f"{t.lower()}.csv"),
        ]
        for c in cands:
            if _os264c.path.exists(c): return _os264c.path.abspath(c)
        try:
            for name in _os264c.listdir(d):
                up=name.upper()
                if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                    p=_os264c.path.join(d,name)
                    if _os264c.path.exists(p): return _os264c.path.abspath(p)
        except Exception:
            pass
    return None

def _et264c_count_rows(path):
    try:
        with open(path,"r",encoding="utf-8",errors="ignore") as f:
            return max(sum(1 for _ in f)-1,0)
    except Exception:
        return None

def _et264c_register_aau():
    reg_names=["CANONICAL_DAILY","CANONICAL_DATASETS","DATASET_REGISTRY","DAILY_REGISTRY","AAU_REGISTRY"]
    assets=[]
    for d in _ET264C_AAU_DIRS:
        if not _os264c.path.isdir(d): 
            continue
        try:
            for name in _os264c.listdir(d):
                if name.lower().endswith(".csv"):
                    m=_re264c.match(r"([A-Za-z0-9\.]+)", name)
                    if m:
                        t=m.group(1).upper()
                        if t not in assets:
                            assets.append(t)
        except Exception:
            pass
    for t in assets:
        path=_et264c_find_aau_csv(t)
        if not path:
            continue
        entry={"file_name":_os264c.path.basename(path),"path":path,"kind":"aau","n_rows":_et264c_count_rows(path)}
        for rn in reg_names:
            g=globals().get(rn)
            if isinstance(g, dict):
                if t not in g or not isinstance(g.get(t), dict):
                    g[t]=entry.copy()
                else:
                    g[t]["path"]=path
                    g[t]["kind"]="aau"
                    g[t].setdefault("file_name",entry["file_name"])
                    g[t].setdefault("n_rows",entry["n_rows"])
    return assets

_ET264C_AAU_ASSETS = _et264c_register_aau()

def _detect_assets_in_order(question, reg):
    q_raw=str(question or "")
    nq=_et264c_norm(q_raw)
    reg_assets=[]
    try:
        reg_assets=[str(k).upper() for k in reg.keys()]
    except Exception:
        pass
    reg_assets=list(dict.fromkeys(reg_assets + list(_ET264C_AAU_ASSETS)))
    ordered=[]
    for alias,ticker in sorted(_ET264C_ALIASES.items(), key=lambda kv: -len(kv[0])):
        if _re264c.search(rf"(?<![a-z0-9]){_re264c.escape(_et264c_norm(alias))}(?![a-z0-9])", _et264c_norm(q_raw)):
            if ticker not in ordered and (ticker in reg_assets or _et264c_find_aau_csv(ticker)):
                ordered.append(ticker)
    for t in reg_assets:
        if _re264c.search(rf"(?<![a-z0-9]){_re264c.escape(t.lower())}(?![a-z0-9])", nq):
            if t not in ordered:
                ordered.append(t)
    m=_re264c.search(r"\bcomparaison\s+([a-z0-9\.]+)\s+vs\s+([a-z0-9\.]+)\b", nq)
    if m:
        pref=[]
        for raw in [m.group(1),m.group(2)]:
            can=_ET264C_ALIASES.get(raw.lower(), raw.upper())
            if can not in pref and (can in reg_assets or _et264c_find_aau_csv(can)):
                pref.append(can)
        ordered=pref+[x for x in ordered if x not in pref]
    m=_re264c.search(r"\bperformance\s+de\s+([a-z0-9\.]+)\b", nq)
    if m:
        can=_ET264C_ALIASES.get(m.group(1).lower(), m.group(1).upper())
        if can in ordered:
            ordered=[can]+[x for x in ordered if x!=can]
        elif _et264c_find_aau_csv(can):
            ordered=[can]+ordered
    return ordered

def _parse_count_condition(nq, target):
    s=_et264c_norm(nq)
    m=_re264c.search(r'(\d+(?:[.,]\d+)?)\s*%', s)
    v=float(m.group(1).replace(",", ".")) if m else None
    if v is not None:
        if _re264c.search(r"(a augmente|augmente|en hausse|positif)", s):
            return ("pct_up", v, f"a augmenté d'au moins {str(v).replace('.',',')}%")
        if _re264c.search(r"(a baisse|baisse|en baisse|negatif)", s):
            return ("pct_down", v, f"a baissé d'au moins {str(v).replace('.',',')}%")
        if _re264c.search(r"(a cloture a plus de|a clôturé à plus de|plus de)", s):
            return ("pct_up", v, f"a clôturé à plus de {str(v).replace('.',',')}%")
        if _re264c.search(r"(a cloture a moins de|a clôturé à moins de|moins de)", s):
            return ("pct_down", v, f"a clôturé à moins de -{str(v).replace('.',',')}%")
    if _re264c.search(r"(a ete en hausse|a été en hausse|a ete positif|a été positif|est positif|en hausse)", s):
        return ("sign_up", 0.0, "a été en hausse")
    if _re264c.search(r"(a ete en baisse|a été en baisse|a ete negatif|a été négatif|est negatif|en baisse)", s):
        return ("sign_down", 0.0, "a été en baisse")
    m_lt=_re264c.search(r'en dessous de\s+(\d+(?:[.,]\d+)?)', s)
    if m_lt:
        return ("lt_level", float(m_lt.group(1).replace(",",".")), f"en dessous de {m_lt.group(1).replace('.',',')}")
    m_gt=_re264c.search(r'au dessus de\s+(\d+(?:[.,]\d+)?)', s)
    if m_gt:
        return ("gt_level", float(m_gt.group(1).replace(",",".")), f"au dessus de {m_gt.group(1).replace('.',',')}")
    return (None,None,"dans le filtre demandé")
# === ETAPE264C_AAU_PATCH_END ===


# === ETAPE264D_NAT_RUNTIME_PATCH_START ===
import os as _os264d_n, re as _re264d_n

_ET264D_AAU_DIRS = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]
_ET264D_ALIASES = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT","amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL",
    "facebook":"META","meta":"META","tesla":"TSLA","tsla":"TSLA",
    "nvidia":"NVDA","nvda":"NVDA","spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}

def _et264d_norm_q(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264d_n.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264d_n.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264D_ALIASES.items(), key=lambda kv: -len(kv[0])):
        s=_re264d_n.sub(rf"(?<![a-z0-9]){_re264d_n.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s

def _et264d_find_aau_csv(ticker):
    t=str(ticker or "").upper().strip()
    if not t: return None
    for d in _ET264D_AAU_DIRS:
        if not _os264d_n.path.isdir(d): continue
        for c in [
            _os264d_n.path.join(d,f"{t}_daily.csv"),
            _os264d_n.path.join(d,f"{t}.csv"),
            _os264d_n.path.join(d,f"{t.lower()}_daily.csv"),
            _os264d_n.path.join(d,f"{t.lower()}.csv"),
        ]:
            if _os264d_n.path.exists(c): return _os264d_n.path.abspath(c)
        try:
            for name in _os264d_n.listdir(d):
                up=name.upper()
                if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                    p=_os264d_n.path.join(d,name)
                    if _os264d_n.path.exists(p): return _os264d_n.path.abspath(p)
        except Exception:
            pass
    return None

def _et264d_register_aau_runtime():
    reg_names=["CANONICAL_DAILY","CANONICAL_DATASETS","DATASET_REGISTRY","DAILY_REGISTRY","AAU_REGISTRY"]
    assets=[]
    for d in _ET264D_AAU_DIRS:
        if not _os264d_n.path.isdir(d): continue
        try:
            for name in _os264d_n.listdir(d):
                if name.lower().endswith(".csv"):
                    m=_re264d_n.match(r"([A-Za-z0-9\.]+)", name)
                    if m:
                        t=m.group(1).upper()
                        if t not in assets: assets.append(t)
        except Exception:
            pass
    for t in assets:
        path=_et264d_find_aau_csv(t)
        if not path: continue
        entry={"file_name":_os264d_n.path.basename(path),"path":path,"kind":"aau"}
        for rn in reg_names:
            g=globals().get(rn)
            if isinstance(g, dict):
                if t not in g or not isinstance(g.get(t), dict):
                    g[t]=entry.copy()
                else:
                    g[t]["path"]=path
                    g[t]["kind"]="aau"
                    g[t].setdefault("file_name",entry["file_name"])
    return assets

try:
    _et264d_register_aau_runtime()
except Exception:
    pass
# === ETAPE264D_NAT_RUNTIME_PATCH_END ===


# === ETAPE264E_NAT_ALIAS_PATCH_START ===
import os as _os264e_n, re as _re264e_n

_ET264E_AAU_DIRS_N = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]
_ET264E_ALIASES_N = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT","amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL","facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA","nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}
def _et264e_norm_n(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264e_n.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264e_n.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264E_ALIASES_N.items(), key=lambda kv:-len(kv[0])):
        s=_re264e_n.sub(rf"(?<![a-z0-9]){_re264e_n.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s
def _nrm(x):
    return _et264e_norm_n(x)
def _et264e_find_aau_csv_n(ticker):
    t=str(ticker or "").upper().strip()
    if not t: return None
    for d in _ET264E_AAU_DIRS_N:
        if not _os264e_n.path.isdir(d): continue
        for c in [
            _os264e_n.path.join(d,f"{t}_daily.csv"),
            _os264e_n.path.join(d,f"{t}.csv"),
            _os264e_n.path.join(d,f"{t.lower()}_daily.csv"),
            _os264e_n.path.join(d,f"{t.lower()}.csv"),
        ]:
            if _os264e_n.path.exists(c): return _os264e_n.path.abspath(c)
        try:
            for name in _os264e_n.listdir(d):
                up=name.upper()
                if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                    p=_os264e_n.path.join(d,name)
                    if _os264e_n.path.exists(p): return _os264e_n.path.abspath(p)
        except Exception:
            pass
    return None
def _et264e_register_aau_n():
    reg_names=["CANONICAL_DAILY","CANONICAL_DATASETS","DATASET_REGISTRY","DAILY_REGISTRY","AAU_REGISTRY"]
    for alias,ticker in _ET264E_ALIASES_N.items():
        path=_et264e_find_aau_csv_n(ticker)
        if not path: continue
        entry={"file_name":_os264e_n.path.basename(path),"path":path,"kind":"aau"}
        for rn in reg_names:
            g=globals().get(rn)
            if isinstance(g,dict):
                if ticker not in g or not isinstance(g.get(ticker),dict):
                    g[ticker]=entry.copy()
                else:
                    g[ticker]["path"]=path
                    g[ticker]["kind"]="aau"
                    g[ticker].setdefault("file_name",entry["file_name"])
try:
    _et264e_register_aau_n()
except Exception:
    pass
# === ETAPE264E_NAT_ALIAS_PATCH_END ===


# === ETAPE264F_NAT_HARDEN_PATCH_START ===
import os as _os264f_n, re as _re264f_n

_ET264F_AAU_DIRS = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]
_ET264F_ALIAS = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT","amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL","facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA","nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}
def _et264f_norm(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264f_n.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264f_n.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264F_ALIAS.items(), key=lambda kv:-len(kv[0])):
        s=_re264f_n.sub(rf"(?<![a-z0-9]){_re264f_n.escape(alias)}(?![a-z0-9])", ticker.lower(), s)
    return s
def _et264f_extract_tickers(question):
    nq=_et264f_norm(question)
    out=[]
    for alias,ticker in sorted(_ET264F_ALIAS.items(), key=lambda kv:-len(kv[0])):
        if _re264f_n.search(rf"(?<![a-z0-9]){_re264f_n.escape(ticker.lower())}(?![a-z0-9])", nq):
            if ticker not in out: out.append(ticker)
    return out
def _et264f_find_aau_csv(ticker):
    t=str(ticker or "").upper().strip()
    if not t:return None
    for d in _ET264F_AAU_DIRS:
        if not _os264f_n.path.isdir(d):continue
        for c in [
            _os264f_n.path.join(d,f"{t}_daily.csv"),
            _os264f_n.path.join(d,f"{t}.csv"),
            _os264f_n.path.join(d,f"{t.lower()}_daily.csv"),
            _os264f_n.path.join(d,f"{t.lower()}.csv"),
        ]:
            if _os264f_n.path.exists(c): return _os264f_n.path.abspath(c)
        try:
            for name in _os264f_n.listdir(d):
                up=name.upper()
                if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                    p=_os264f_n.path.join(d,name)
                    if _os264f_n.path.exists(p): return _os264f_n.path.abspath(p)
        except Exception:
            pass
    return None
def _et264f_sanitize_ticker(ticker, question=None):
    if ticker in (False, True, None, "", "False", "True", "None"):
        ex=_et264f_extract_tickers(question or "")
        if ex: return ex[0]
        return None
    t=str(ticker).upper().strip()
    if t in _ET264F_ALIAS.values(): return t
    ex=_ET264F_ALIAS.get(str(ticker).lower().strip())
    if ex: return ex
    return t
# === ETAPE264F_NAT_HARDEN_PATCH_END ===
