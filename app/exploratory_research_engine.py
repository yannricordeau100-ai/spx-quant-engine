import os, re, json, unicodedata
import pandas as pd

PROJECT_ROOT=r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
REGISTRY_PATH=r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/ETAPE197_ASSET_TIMEFRAME_REGISTRY.json"

with open(REGISTRY_PATH,"r",encoding="utf-8") as f:
    _REG=(json.load(f).get("assets",{}) or {})

ASSET_ALIASES={
    "SPX":["spx","s&p 500","s&p500","s&p"],
    "SPY":["spy"],
    "QQQ":["qqq"],
    "IWM":["iwm"],
    "VIX":["vix"],
    "DXY":["dxy","dollar index","dollar"],
    "GOLD":["gold","or"],
}

MACRO_MAP={
    "cpi":["cpi","inflation"],
    "fomc":["fomc","fed","federal reserve"],
    "nfp":["nfp","nonfarm","non farm"],
    "earnings":["earnings","resultats","résultats","major companies"],
    "options_expiration":["options expiration","expiration","opex"],
}

def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def _nrm(s):
    s=_strip_accents(str(s).lower())
    repl=[
        ("au dessus de","superieur a"),("au-dessus de","superieur a"),
        ("en dessous de","inferieur a"),("au-dessous de","inferieur a"),
        ("lorsque","quand"),
        ("quel environnement","dans quel contexte"),
        ("meilleur niveau de vix","quel seuil de vix"),
        ("a partir de quel vix","quel seuil de vix"),
        ("quand spx performe le mieux","dans quel contexte spx performe le mieux"),
        ("quand spx monte le plus","dans quel contexte spx monte le plus"),
        ("surperforme le plus","surperforme"),
        ("edges","edge"),("meilleurs edges","meilleur edge"),
    ]
    for a,b in repl:
        s=s.replace(a,b)
    s=re.sub(r"[^a-z0-9%+<>=/.' -]+"," ",s)
    s=re.sub(r"\s+"," ",s).strip()
    return s

def _contains_term(nq, term):
    return re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", nq) is not None

def _detect_asset(nq, default="SPX"):
    for asset, aliases in ASSET_ALIASES.items():
        if any(_contains_term(nq,a) for a in aliases):
            return asset
    return default

def _detect_compare_assets(nq):
    found=[]
    for asset, aliases in ASSET_ALIASES.items():
        if any(_contains_term(nq,a) for a in aliases):
            found.append(asset)
    return list(dict.fromkeys(found))

def _best_entry_for_asset(asset, daily_only=False):
    arr=_REG.get(asset,[]) or []
    if not arr:
        return None
    valid=[x for x in arr if x.get("bar_minutes") is not None]
    if not valid:
        return arr[0]
    if daily_only:
        daily=[x for x in valid if x.get("bar_minutes")==1440]
        if daily:
            return daily[0]
    return sorted(valid, key=lambda x:(abs((x.get("bar_minutes") or 10**9)-1440), x.get("bar_minutes") or 10**9))[0]

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

def _load_daily_asset(asset):
    entry=_best_entry_for_asset(asset,daily_only=True)
    if entry is None:
        return None,None
    df=_read_csv_flex(entry["path"])
    df=_norm_cols(df)
    tcol=_find_time_col(df)
    if tcol is None:
        return None,None
    df["timestamp"]=pd.to_datetime(df[tcol],errors="coerce")
    df=df[df["timestamp"].notna()].copy().sort_values("timestamp")
    close_candidates=[c for c in df.columns if c in ["close","close_last","last","price","adj_close"]]
    if not close_candidates:
        close_candidates=[c for c in df.columns if "close" in c]
    if not close_candidates:
        num_cols=[c for c in df.columns if c not in [tcol,"timestamp"]]
        close_candidates=num_cols[:1]
    if not close_candidates:
        return None,None
    ccol=close_candidates[0]
    df["close"]=pd.to_numeric(df[ccol],errors="coerce")
    df=df[df["close"].notna()].copy()
    if "open" in df.columns:
        df["open"]=pd.to_numeric(df["open"],errors="coerce")
    else:
        df["open"]=df["close"].shift(1)
    df["ret_1d"]=df["close"].shift(-1)/df["close"]-1.0
    df["yyyymm"]=df["timestamp"].dt.to_period("M").astype(str)
    df["month"]=df["timestamp"].dt.month
    df["date_key"]=df["timestamp"].dt.floor("D")
    return df,entry

def _load_calendar():
    path=None
    for root,_,files in os.walk(PROJECT_ROOT):
        for f in files:
            if f.lower()=="calendar_events_daily.csv":
                path=os.path.join(root,f)
                break
        if path:
            break
    if path is None:
        return None,None
    df=_read_csv_flex(path)
    df=_norm_cols(df)
    tcol=_find_time_col(df)
    if tcol is None:
        return None,None
    df["timestamp"]=pd.to_datetime(df[tcol],errors="coerce")
    df=df[df["timestamp"].notna()].copy()
    df["date_key"]=df["timestamp"].dt.floor("D")
    text_cols=[c for c in df.columns if c not in [tcol,"timestamp","date_key"]]
    blob=df[text_cols].astype(str).agg(" | ".join, axis=1).str.lower().map(_strip_accents) if text_cols else pd.Series([""]*len(df),index=df.index)
    out=df[["date_key"]].copy()
    out["calendar_blob"]=blob
    out=out.groupby("date_key",as_index=False).agg({"calendar_blob":" | ".join})
    return out,{"file_name":"calendar_events_daily.csv","path":path}

def _detect_mode(nq):
    if any(x in nq for x in [
        "meilleur edge","meilleurs edge","edge","edges","top edge","top edges",
        "meilleur filtre","meilleurs filtres","quel filtre ameliore le plus","quel filtre améliore le plus",
        "quel est le meilleur edge","quels sont les meilleurs edges","quel est le meilleur filtre",
        "quel filtre améliore le plus le taux positif","quel filtre ameliore le plus le taux positif"
    ]):
        return "combinatorial_edge_scan"
    if any(x in nq for x in ["quel mois","meilleur mois","pire mois","saisonnalite","saisonnalité"]):
        return "month_scan"
    if any(x in nq for x in ["quel seuil de vix","quel niveau de vix","quel vix","meilleur niveau de vix","a partir de quel vix"]):
        return "vix_threshold_scan"
    if any(x in nq for x in ["quel evenement","quel événement","impact macro","macro impacte le plus","macro impacte","quel event"]):
        return "macro_scan"
    if any(x in nq for x in ["surperforme"]) and len(_detect_compare_assets(nq))>=2:
        return "pair_outperformance_scan"
    if any(x in nq for x in ["dans quel contexte","meilleur contexte","quel contexte","meilleur environnement","quel environnement"]):
        return "best_context_scan"
    return None

def can_handle(question):
    nq=_nrm(question)
    return _detect_mode(nq) is not None

def _summary_stats_from_series(s):
    s=pd.to_numeric(s,errors="coerce").dropna()
    if len(s)==0:
        return None
    return {
        "count":int(len(s)),
        "moyenne_variation":float(s.mean()),
        "taux_positif":float((s>0).mean()),
        "meilleure_variation":float(s.max()),
        "pire_variation":float(s.min()),
    }

def _display_pct(x):
    return "n.d." if x is None or pd.isna(x) else f"{x*100:.2f}%"

def _sort_rows(rows, nq):
    nq=nq.lower()
    if "pire mois" in nq or "pire" in nq:
        return sorted(rows,key=lambda x:(x["moyenne_variation"],x["taux_positif"],x["count"]))
    if "meilleur taux positif" in nq or "taux positif" in nq:
        return sorted(rows,key=lambda x:(x["taux_positif"],x["moyenne_variation"],x["count"]),reverse=True)
    if "plus de cas" in nq or "nombre de cas" in nq:
        return sorted(rows,key=lambda x:(x["count"],x["moyenne_variation"],x["taux_positif"]),reverse=True)
    return sorted(rows,key=lambda x:(x["moyenne_variation"],x["taux_positif"],x["count"]),reverse=True)

def _scan_all_months(asset, nq):
    df,entry=_load_daily_asset(asset)
    if df is None:
        return None
    rows=[]
    for period,sub in df.groupby("yyyymm"):
        stats=_summary_stats_from_series(sub["ret_1d"])
        if stats and stats["count"]>0:
            rows.append({
                "bucket":period,
                "count":stats["count"],
                "moyenne_variation":stats["moyenne_variation"],
                "taux_positif":stats["taux_positif"],
            })
    rows=_sort_rows(rows,nq)
    return rows,entry

def _scan_all_vix_thresholds(asset):
    target,target_entry=_load_daily_asset(asset)
    vix,vix_entry=_load_daily_asset("VIX")
    if target is None or vix is None:
        return None
    merged=target[["date_key","ret_1d"]].merge(vix[["date_key","close"]].rename(columns={"close":"vix_close"}),on="date_key",how="inner")
    merged=merged.dropna(subset=["ret_1d","vix_close"]).copy()
    if len(merged)==0:
        return None
    thresholds=sorted(pd.Series(merged["vix_close"]).dropna().astype(float).unique().tolist())
    rows=[]
    for thr in thresholds:
        sub=merged[merged["vix_close"]>=thr]
        stats=_summary_stats_from_series(sub["ret_1d"])
        if stats and stats["count"]>0:
            rows.append({
                "bucket":float(thr),
                "count":stats["count"],
                "moyenne_variation":stats["moyenne_variation"],
                "taux_positif":stats["taux_positif"],
            })
    rows=sorted(rows,key=lambda x:(x["moyenne_variation"],x["taux_positif"],x["count"]),reverse=True)
    return rows,[target_entry,vix_entry]

def _scan_macro_events(asset):
    target,target_entry=_load_daily_asset(asset)
    cal,cal_entry=_load_calendar()
    if target is None or cal is None:
        return None
    merged=target[["date_key","ret_1d"]].merge(cal,on="date_key",how="inner")
    merged=merged.dropna(subset=["ret_1d"]).copy()
    blob=merged["calendar_blob"].fillna("").astype(str).str.lower().map(_strip_accents)
    rows=[]
    for k,aliases in MACRO_MAP.items():
        mask=pd.Series(False,index=merged.index)
        for a in aliases:
            mask=mask | blob.str.contains(re.escape(_strip_accents(a)),regex=True)
        sub=merged[mask]
        stats=_summary_stats_from_series(sub["ret_1d"])
        if stats and stats["count"]>0:
            rows.append({
                "bucket":k,
                "count":stats["count"],
                "moyenne_variation":stats["moyenne_variation"],
                "taux_positif":stats["taux_positif"],
            })
    rows=sorted(rows,key=lambda x:(x["moyenne_variation"],x["taux_positif"],x["count"]),reverse=True)
    return rows,[target_entry,cal_entry]

def _scan_pair_outperformance(a1,a2):
    d1,e1=_load_daily_asset(a1)
    d2,e2=_load_daily_asset(a2)
    if d1 is None or d2 is None:
        return None
    merged=d1[["date_key","ret_1d","month"]].rename(columns={"ret_1d":"ret_a1"}).merge(
        d2[["date_key","ret_1d"]].rename(columns={"ret_1d":"ret_a2"}),on="date_key",how="inner"
    ).dropna()
    if len(merged)==0:
        return None
    merged["spread"]=merged["ret_a1"]-merged["ret_a2"]
    rows=[]
    base=_summary_stats_from_series(merged["spread"])
    if base and base["count"]>0:
        rows.append({
            "bucket":"global",
            "count":base["count"],
            "moyenne_variation":base["moyenne_variation"],
            "taux_positif":base["taux_positif"],
        })
    for m,sub in merged.groupby("month"):
        stats=_summary_stats_from_series(sub["spread"])
        if stats and stats["count"]>0:
            rows.append({
                "bucket":f"mois_{int(m)}",
                "count":stats["count"],
                "moyenne_variation":stats["moyenne_variation"],
                "taux_positif":stats["taux_positif"],
            })
    rows=sorted(rows,key=lambda x:(x["moyenne_variation"],x["taux_positif"],x["count"]),reverse=True)
    return rows,[e1,e2]

def _scan_best_context(asset):
    rows_all=[]
    used=[]
    r1=_scan_all_months(asset,"meilleur")
    if r1:
        rows,entry=r1
        used.append(entry)
        for x in rows:
            rows_all.append({"bucket":"mois:"+x["bucket"],"count":x["count"],"moyenne_variation":x["moyenne_variation"],"taux_positif":x["taux_positif"]})
    r2=_scan_all_vix_thresholds(asset)
    if r2:
        rows,entries=r2
        used.extend(entries)
        for x in rows[:3000]:
            rows_all.append({"bucket":"vix>="+str(round(float(x["bucket"]),4)),"count":x["count"],"moyenne_variation":x["moyenne_variation"],"taux_positif":x["taux_positif"]})
    r3=_scan_macro_events(asset)
    if r3:
        rows,entries=r3
        used.extend(entries)
        for x in rows:
            rows_all.append({"bucket":"macro:"+x["bucket"],"count":x["count"],"moyenne_variation":x["moyenne_variation"],"taux_positif":x["taux_positif"]})
    if not rows_all:
        return None
    rows_all=sorted(rows_all,key=lambda x:(x["moyenne_variation"],x["taux_positif"],x["count"]),reverse=True)
    uniq=[]; seen=set()
    for e in used:
        if e and e.get("file_name") not in seen:
            uniq.append(e); seen.add(e.get("file_name"))
    return rows_all,uniq

def _scan_dxy_direction(asset):
    target,target_entry=_load_daily_asset(asset)
    dxy,dxy_entry=_load_daily_asset("DXY")
    if target is None or dxy is None:
        return None
    dxy=dxy.copy()
    dxy["dxy_dir"]=dxy["close"].diff()
    merged=target[["date_key","ret_1d"]].merge(dxy[["date_key","dxy_dir"]],on="date_key",how="inner").dropna()
    if len(merged)==0:
        return None
    rows=[]
    for label,mask in [("dxy_en_hausse",merged["dxy_dir"]>0),("dxy_en_baisse",merged["dxy_dir"]<0)]:
        sub=merged[mask]
        stats=_summary_stats_from_series(sub["ret_1d"])
        if stats and stats["count"]>0:
            rows.append({
                "bucket":label,
                "count":stats["count"],
                "moyenne_variation":stats["moyenne_variation"],
                "taux_positif":stats["taux_positif"],
            })
    rows=sorted(rows,key=lambda x:(x["moyenne_variation"],x["taux_positif"],x["count"]),reverse=True)
    return rows,[target_entry,dxy_entry]

def _scan_combinatorial_edges(asset):
    rows_all=[]
    used=[]

    a=_scan_all_months(asset,"meilleur")
    if a:
        rows,entry=a
        used.append(entry)
        for x in rows:
            rows_all.append({"bucket":"mois:"+x["bucket"],"count":x["count"],"moyenne_variation":x["moyenne_variation"],"taux_positif":x["taux_positif"]})

    b=_scan_all_vix_thresholds(asset)
    if b:
        rows,entries=b
        used.extend(entries)
        top_vix=rows[:50]
        for x in top_vix:
            rows_all.append({"bucket":"vix>="+str(round(float(x["bucket"]),4)),"count":x["count"],"moyenne_variation":x["moyenne_variation"],"taux_positif":x["taux_positif"]})

    c=_scan_macro_events(asset)
    if c:
        rows,entries=c
        used.extend(entries)
        for x in rows:
            rows_all.append({"bucket":"macro:"+x["bucket"],"count":x["count"],"moyenne_variation":x["moyenne_variation"],"taux_positif":x["taux_positif"]})

    d=_scan_dxy_direction(asset)
    if d:
        rows,entries=d
        used.extend(entries)
        for x in rows:
            rows_all.append({"bucket":"dxy:"+x["bucket"],"count":x["count"],"moyenne_variation":x["moyenne_variation"],"taux_positif":x["taux_positif"]})

    # simple combinations
    target,target_entry=_load_daily_asset(asset)
    vix,vix_entry=_load_daily_asset("VIX")
    dxy,dxy_entry=_load_daily_asset("DXY")
    cal,cal_entry=_load_calendar()
    if target is not None:
        used.append(target_entry)
    if vix is not None:
        used.append(vix_entry)
    if dxy is not None:
        used.append(dxy_entry)
    if cal is not None:
        used.append(cal_entry)

    if target is not None and vix is not None:
        base=target[["date_key","ret_1d","month"]].merge(vix[["date_key","close"]].rename(columns={"close":"vix_close"}),on="date_key",how="inner")
        if dxy is not None:
            dxy2=dxy[["date_key","close"]].copy()
            dxy2["dxy_dir"]=dxy2["close"].diff()
            base=base.merge(dxy2[["date_key","dxy_dir"]],on="date_key",how="left")
        if cal is not None:
            base=base.merge(cal,on="date_key",how="left")
            base["calendar_blob"]=base["calendar_blob"].fillna("").astype(str).str.lower().map(_strip_accents)
        base=base.dropna(subset=["ret_1d","vix_close"]).copy()

        vix_levels=sorted(pd.Series(base["vix_close"]).dropna().astype(float).unique().tolist())
        # reduce combinatorial load but still meaningful
        candidate_thresholds=sorted(set(vix_levels[::max(1,len(vix_levels)//30)] + vix_levels[-10:] + vix_levels[:10]))
        candidate_thresholds=[float(x) for x in candidate_thresholds]

        combo_rows=[]
        for thr in candidate_thresholds:
            sub=base[base["vix_close"]>=thr]
            stats=_summary_stats_from_series(sub["ret_1d"])
            if stats and stats["count"]>=5:
                combo_rows.append({"bucket":f"combo:vix>={round(thr,4)}","count":stats["count"],"moyenne_variation":stats["moyenne_variation"],"taux_positif":stats["taux_positif"]})
            if "dxy_dir" in base.columns:
                sub2=base[(base["vix_close"]>=thr) & (base["dxy_dir"]<0)]
                stats2=_summary_stats_from_series(sub2["ret_1d"])
                if stats2 and stats2["count"]>=5:
                    combo_rows.append({"bucket":f"combo:vix>={round(thr,4)} & dxy_baisse","count":stats2["count"],"moyenne_variation":stats2["moyenne_variation"],"taux_positif":stats2["taux_positif"]})
            if "calendar_blob" in base.columns:
                sub3=base[(base["vix_close"]>=thr) & (base["calendar_blob"].str.contains("cpi",regex=True))]
                stats3=_summary_stats_from_series(sub3["ret_1d"])
                if stats3 and stats3["count"]>=5:
                    combo_rows.append({"bucket":f"combo:vix>={round(thr,4)} & cpi","count":stats3["count"],"moyenne_variation":stats3["moyenne_variation"],"taux_positif":stats3["taux_positif"]})
        rows_all.extend(combo_rows)

    rows_all=sorted(rows_all,key=lambda x:(x["moyenne_variation"],x["taux_positif"],x["count"]),reverse=True)

    uniq=[]; seen=set()
    for e in used:
        if e and e.get("file_name") not in seen:
            uniq.append(e); seen.add(e.get("file_name"))
    return rows_all,uniq

def run(question, preview_rows=20):
    nq=_nrm(question)
    mode=_detect_mode(nq)
    asset=_detect_asset(nq,"SPX")
    compare_assets=_detect_compare_assets(nq)

    if mode=="month_scan":
        out=_scan_all_months(asset,nq)
        if not out:
            return {"engine":"exploratory_research_engine","status":"NO_EXPLORATION_RESULT","answer":"Je n'ai pas pu scanner les mois pour cet actif."}
        rows,entry=out
        top=rows[0]
        label="Le pire mois observé" if "pire mois" in nq or "pire" in nq else "Le meilleur mois observé"
        return {
            "engine":"exploratory_research_engine",
            "status":"OK",
            "mode":"month_scan",
            "target_asset":asset,
            "value":top["bucket"],
            "answer":f"{label} depuis le début des CSV pour {asset} est {top['bucket']} avec une variation moyenne de {_display_pct(top['moyenne_variation'])} et un taux positif de {_display_pct(top['taux_positif'])}.",
            "summary":"Scan exploratoire sur tous les mois disponibles depuis le début de l'historique.",
            "ranking":rows[:preview_rows],
            "source_file_names":[entry.get("file_name")],
        }

    if mode=="vix_threshold_scan":
        out=_scan_all_vix_thresholds(asset)
        if not out:
            return {"engine":"exploratory_research_engine","status":"NO_EXPLORATION_RESULT","answer":"Je n'ai pas pu scanner les seuils de VIX pour cet actif."}
        rows,entries=out
        top=rows[0]
        return {
            "engine":"exploratory_research_engine",
            "status":"OK",
            "mode":"vix_threshold_scan",
            "target_asset":asset,
            "value":top["bucket"],
            "answer":f"Le meilleur seuil VIX observé pour {asset} est VIX >= {round(float(top['bucket']),4)} avec une variation moyenne de {_display_pct(top['moyenne_variation'])}, un taux positif de {_display_pct(top['taux_positif'])} et {top['count']} cas. Tous les niveaux VIX disponibles ont été testés.",
            "summary":"Scan exploratoire sur tous les niveaux VIX uniques exploitables du CSV daily.",
            "ranking":rows[:preview_rows],
            "source_file_names":[e.get("file_name") for e in entries if e],
        }

    if mode=="macro_scan":
        out=_scan_macro_events(asset)
        if not out:
            return {"engine":"exploratory_research_engine","status":"NO_EXPLORATION_RESULT","answer":"Je n'ai pas pu scanner les événements macro pour cet actif."}
        rows,entries=out
        top=rows[0]
        return {
            "engine":"exploratory_research_engine",
            "status":"OK",
            "mode":"macro_scan",
            "target_asset":asset,
            "value":top["bucket"],
            "answer":f"L'événement macro qui ressort le plus pour {asset} est {top['bucket']} avec une variation moyenne de {_display_pct(top['moyenne_variation'])} et un taux positif de {_display_pct(top['taux_positif'])}.",
            "summary":"Scan exploratoire sur les grandes familles d'événements macro détectées dans le calendrier économique.",
            "ranking":rows[:preview_rows],
            "source_file_names":[e.get("file_name") for e in entries if e],
        }

    if mode=="pair_outperformance_scan" and len(compare_assets)>=2:
        a1,a2=compare_assets[0],compare_assets[1]
        out=_scan_pair_outperformance(a1,a2)
        if not out:
            return {"engine":"exploratory_research_engine","status":"NO_EXPLORATION_RESULT","answer":"Je n'ai pas pu scanner la surperformance entre ces deux actifs."}
        rows,entries=out
        top=rows[0]
        return {
            "engine":"exploratory_research_engine",
            "status":"OK",
            "mode":"pair_outperformance_scan",
            "target_asset":a1,
            "comparison_assets":[a1,a2],
            "value":top["bucket"],
            "answer":f"Le contexte où {a1} surperforme le plus {a2} est {top['bucket']} avec un spread moyen de {_display_pct(top['moyenne_variation'])} et un taux de surperformance de {_display_pct(top['taux_positif'])}.",
            "summary":"Scan exploratoire de surperformance relative entre deux actifs.",
            "ranking":rows[:preview_rows],
            "source_file_names":[e.get("file_name") for e in entries if e],
        }

    if mode=="best_context_scan":
        out=_scan_best_context(asset)
        if not out:
            return {"engine":"exploratory_research_engine","status":"NO_EXPLORATION_RESULT","answer":"Je n'ai pas pu identifier de contexte dominant pour cet actif."}
        rows,entries=out
        top=rows[0]
        return {
            "engine":"exploratory_research_engine",
            "status":"OK",
            "mode":"best_context_scan",
            "target_asset":asset,
            "value":top["bucket"],
            "answer":f"Le meilleur contexte exploratoire détecté pour {asset} est {top['bucket']} avec une variation moyenne de {_display_pct(top['moyenne_variation'])} et un taux positif de {_display_pct(top['taux_positif'])}.",
            "summary":"Scan exploratoire agrégé sur mois, seuils VIX et événements macro.",
            "ranking":rows[:preview_rows],
            "source_file_names":[e.get("file_name") for e in entries if e],
        }

    if mode=="combinatorial_edge_scan":
        out=_scan_combinatorial_edges(asset)
        if not out:
            return {"engine":"exploratory_research_engine","status":"NO_EXPLORATION_RESULT","answer":"Je n'ai pas pu générer d'edges exploratoires pour cet actif."}
        rows,entries=out
        top=rows[0] if rows else None
        if not top:
            return {"engine":"exploratory_research_engine","status":"NO_EXPLORATION_RESULT","answer":"Aucun edge exploitable n'a été trouvé."}
        metric_focus="performance"
        if "taux positif" in nq:
            rows=sorted(rows,key=lambda x:(x["taux_positif"],x["moyenne_variation"],x["count"]),reverse=True)
            top=rows[0]
            metric_focus="taux_positif"
        elif "plus de cas" in nq or "nombre de cas" in nq:
            rows=sorted(rows,key=lambda x:(x["count"],x["moyenne_variation"],x["taux_positif"]),reverse=True)
            top=rows[0]
            metric_focus="count"

        if metric_focus=="taux_positif":
            answer=f"Le filtre exploratoire qui améliore le plus le taux positif pour {asset} est {top['bucket']} avec un taux positif de {_display_pct(top['taux_positif'])}, une variation moyenne de {_display_pct(top['moyenne_variation'])} et {top['count']} cas."
        elif metric_focus=="count":
            answer=f"Le filtre exploratoire qui génère le plus de cas pour {asset} est {top['bucket']} avec {top['count']} cas, une variation moyenne de {_display_pct(top['moyenne_variation'])} et un taux positif de {_display_pct(top['taux_positif'])}."
        else:
            answer=f"Le meilleur edge exploratoire détecté pour {asset} est {top['bucket']} avec une variation moyenne de {_display_pct(top['moyenne_variation'])}, un taux positif de {_display_pct(top['taux_positif'])} et {top['count']} cas."

        return {
            "engine":"exploratory_research_engine",
            "status":"OK",
            "mode":"combinatorial_edge_scan",
            "target_asset":asset,
            "value":top["bucket"],
            "metric_focus":metric_focus,
            "answer":answer,
            "summary":"Scan combinatoire automatique sur mois, seuils VIX, macro, direction DXY et quelques combinaisons simples.",
            "ranking":rows[:preview_rows],
            "source_file_names":[e.get("file_name") for e in entries if e],
        }

    return {
        "engine":"exploratory_research_engine",
        "status":"NO_EXPLORATION_RESULT",
        "answer":"Aucune structure exploratoire fiable n'a été reconnue."
    }
