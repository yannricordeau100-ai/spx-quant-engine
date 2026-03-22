import os, re, json, unicodedata
import pandas as pd
import numpy as np

PROJECT_ROOT=r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
REGISTRY_PATH=r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/ETAPE197_ASSET_TIMEFRAME_REGISTRY.json"

with open(REGISTRY_PATH,"r",encoding="utf-8") as f:
    _REG=(json.load(f).get("assets",{}) or {})

ASSET_ALIASES={
    "SPX":["spx","s&p 500","s&p500","s&p"],
    "SPY":["spy"],
    "QQQ":["qqq"],
    "IWM":["iwm"],
    "VIX1D":["vix1d","vix 1d"],
    "DXY":["dxy","dollar index","dollar"],
}

UNITS_TO_MIN={
    "minute":1,"minutes":1,"min":1,"mn":1,
    "heure":60,"heures":60,"hour":60,"hours":60,"h":60,
    "jour":1440,"jours":1440,"day":1440,"days":1440,"d":1440,
    "semaine":10080,"semaines":10080,"week":10080,"weeks":10080,
    "mois":43200,"month":43200,"months":43200,
    "an":525600,"ans":525600,"annee":525600,"annees":525600,"year":525600,"years":525600,
}

def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def _nrm(s):
    s=_strip_accents(str(s).lower())
    repl=[
        ("au dessus de","superieur a"),("au-dessus de","superieur a"),
        ("en dessous de","inferieur a"),("au-dessous de","inferieur a"),
        ("quel moment de la journee","quel moment de la journee"),
        ("a quel moment","quel moment"),
        ("meilleur moment","meilleur moment"),
        ("meilleur edge intraday","meilleur edge intraday"),
        ("intraday","intraday"),
        ("open","ouverture"),
        ("cloture","cloture"),
        ("fermeture","cloture"),
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
        if asset in ["VIX1D","DXY"]:
            continue
        if any(_contains_term(nq,a) for a in aliases):
            return asset
    return default

def _best_entry_for_asset(asset, prefer_intraday=True):
    arr=_REG.get(asset,[]) or []
    if not arr:
        return None
    valid=[x for x in arr if x.get("bar_minutes") is not None]
    if not valid:
        return arr[0]
    if prefer_intraday:
        intraday=[x for x in valid if 1 <= (x.get("bar_minutes") or 10**9) < 1440]
        if intraday:
            # prefer 30min first, then 1min, then nearest to 30
            intraday=sorted(intraday,key=lambda x:(abs((x.get("bar_minutes") or 10**9)-30), x.get("bar_minutes") or 10**9))
            return intraday[0]
    return sorted(valid,key=lambda x:(abs((x.get("bar_minutes") or 10**9)-1440), x.get("bar_minutes") or 10**9))[0]

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

def _load_series(asset, prefer_intraday=True):
    entry=_best_entry_for_asset(asset,prefer_intraday=prefer_intraday)
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
    df["date_key"]=df["timestamp"].dt.floor("D")
    df["hhmm"]=df["timestamp"].dt.strftime("%H:%M")
    df["minute_of_day"]=df["timestamp"].dt.hour*60+df["timestamp"].dt.minute
    bar_minutes=entry.get("bar_minutes") or None
    return (df,entry,bar_minutes)

def _parse_horizon_minutes(nq, default_bar_minutes):
    # examples: sur 30 minutes, sur 2 heures, sur 1 jour
    m=re.search(r"(?:sur|horizon|de)\s+(\d+)\s*(minute|minutes|min|heure|heures|hour|hours|jour|jours|day|days|semaine|semaines|week|weeks|mois|month|months|an|ans|annee|annees|year|years)", nq)
    if m:
        n=int(m.group(1))
        u=m.group(2)
        return max(1, n*UNITS_TO_MIN.get(u, default_bar_minutes or 30))
    return int(default_bar_minutes or 30)

def _detect_focus(nq):
    if any(x in nq for x in ["meilleur edge intraday","meilleur edge","meilleurs edges","meilleur moment","quel moment","a quel moment","a quelle heure","quelle heure"]):
        return "performance"
    if "taux positif" in nq:
        return "taux_positif"
    if "plus de cas" in nq or "nombre de cas" in nq:
        return "count"
    return "performance"

def _detect_filters(nq):
    return {
        "need_vix1d_up": any(x in nq for x in ["vix1d en hausse","vix1d monte","vix1d augmente"]),
        "need_vix1d_down": any(x in nq for x in ["vix1d en baisse","vix1d baisse"]),
        "need_dxy_up": any(x in nq for x in ["dxy en hausse","dxy monte","dxy augmente"]),
        "need_dxy_down": any(x in nq for x in ["dxy en baisse","dxy baisse"]),
        "session_open_focus": "ouverture" in nq,
        "session_close_focus": "cloture" in nq,
    }

def _is_intraday_wording(nq):
    triggers=[
        "intraday","meilleur edge intraday","meilleurs edges intraday","meilleur moment","quel moment","a quel moment","a quelle heure","quelle heure",
        "ouverture","cloture","dans la journee","dans la journée","au fil de la journee","au fil de la journée",
    ]
    return any(t in nq for t in triggers)

def can_handle(question):
    nq=_nrm(question)
    return _is_intraday_wording(nq)

def _display_pct(x):
    return "n.d." if x is None or pd.isna(x) else f"{x*100:.2f}%"

def _merge_daily_direction(base_df, daily_df, name):
    d=daily_df.copy()
    d["daily_dir"]=d["close"].diff()
    d=d[["date_key","daily_dir"]].rename(columns={"daily_dir":f"{name}_dir"})
    return base_df.merge(d,on="date_key",how="left")

def _merge_intraday_direction(base_df, intraday_df, name):
    x=intraday_df.copy()
    x["delta"]=x["close"].diff()
    x=x[["timestamp","delta"]].rename(columns={"delta":f"{name}_delta"})
    return base_df.merge(x,on="timestamp",how="left")

def _build_intraday_scan(asset, nq, preview_rows=12):
    target_pack=_load_series(asset, prefer_intraday=True)
    if not target_pack or target_pack[0] is None:
        return None
    target, target_entry, bar_minutes = target_pack
    if bar_minutes is None:
        return None

    horizon_minutes=_parse_horizon_minutes(nq, bar_minutes)
    steps=max(1, int(round(horizon_minutes / bar_minutes)))
    target["ret_fwd"]=target["close"].shift(-steps)/target["close"] - 1.0

    # reference series
    vix1d_pack=_load_series("VIX1D", prefer_intraday=True)
    dxy_pack=_load_series("DXY", prefer_intraday=False)

    df=target[["timestamp","date_key","hhmm","minute_of_day","close","ret_fwd"]].copy()

    if vix1d_pack and vix1d_pack[0] is not None:
        vix1d, vix1d_entry, _ = vix1d_pack
        # nearest exact timestamp merge first
        vix1d2=vix1d[["timestamp","close"]].copy()
        vix1d2["vix1d_delta"]=vix1d2["close"].diff()
        df=df.merge(vix1d2[["timestamp","vix1d_delta"]],on="timestamp",how="left")
    else:
        vix1d_entry=None

    if dxy_pack and dxy_pack[0] is not None:
        dxy, dxy_entry, _ = dxy_pack
        df=_merge_daily_direction(df,dxy,"dxy")
    else:
        dxy_entry=None

    filters=_detect_filters(nq)

    work=df.dropna(subset=["ret_fwd"]).copy()

    if filters["need_vix1d_up"] and "vix1d_delta" in work.columns:
        work=work[work["vix1d_delta"]>0]
    if filters["need_vix1d_down"] and "vix1d_delta" in work.columns:
        work=work[work["vix1d_delta"]<0]
    if filters["need_dxy_up"] and "dxy_dir" in work.columns:
        work=work[work["dxy_dir"]>0]
    if filters["need_dxy_down"] and "dxy_dir" in work.columns:
        work=work[work["dxy_dir"]<0]

    # reduce scan according to wording
    if filters["session_open_focus"]:
        work=work[(work["minute_of_day"]>=570) & (work["minute_of_day"]<=660)]  # 09:30-11:00
    if filters["session_close_focus"]:
        work=work[(work["minute_of_day"]>=840) & (work["minute_of_day"]<=960)]  # 14:00-16:00

    if len(work)==0:
        return {
            "engine":"intraday_edge_engine",
            "status":"NO_INTRADAY_RESULT",
            "answer":"Aucun cas intraday exploitable n'a été trouvé avec les filtres demandés.",
            "summary":"Le scan intraday n'a trouvé aucun cas valide.",
            "source_file_names":[x for x in [target_entry.get("file_name"), vix1d_entry.get("file_name") if vix1d_entry else None, dxy_entry.get("file_name") if dxy_entry else None] if x],
        }

    rows=[]
    for hhmm, sub in work.groupby("hhmm"):
        s=pd.to_numeric(sub["ret_fwd"],errors="coerce").dropna()
        if len(s) < 5:
            continue
        rows.append({
            "bucket":hhmm,
            "count":int(len(s)),
            "moyenne_variation":float(s.mean()),
            "taux_positif":float((s>0).mean()),
            "meilleure_variation":float(s.max()),
            "pire_variation":float(s.min()),
        })

    if not rows:
        return {
            "engine":"intraday_edge_engine",
            "status":"NO_INTRADAY_RESULT",
            "answer":"Aucun créneau intraday robuste n'a été trouvé.",
            "summary":"Le nombre de cas valides par créneau reste insuffisant.",
            "source_file_names":[x for x in [target_entry.get("file_name"), vix1d_entry.get("file_name") if vix1d_entry else None, dxy_entry.get("file_name") if dxy_entry else None] if x],
        }

    focus=_detect_focus(nq)
    if focus=="taux_positif":
        rows=sorted(rows,key=lambda x:(x["taux_positif"],x["moyenne_variation"],x["count"]),reverse=True)
    elif focus=="count":
        rows=sorted(rows,key=lambda x:(x["count"],x["moyenne_variation"],x["taux_positif"]),reverse=True)
    else:
        rows=sorted(rows,key=lambda x:(x["moyenne_variation"],x["taux_positif"],x["count"]),reverse=True)

    top=rows[0]
    filt_desc=[]
    if filters["need_vix1d_up"]:
        filt_desc.append("VIX1D en hausse")
    if filters["need_vix1d_down"]:
        filt_desc.append("VIX1D en baisse")
    if filters["need_dxy_up"]:
        filt_desc.append("DXY en hausse")
    if filters["need_dxy_down"]:
        filt_desc.append("DXY en baisse")
    if filters["session_open_focus"]:
        filt_desc.append("zone ouverture")
    if filters["session_close_focus"]:
        filt_desc.append("zone clôture")
    filt_txt=" | filtres: " + ", ".join(filt_desc) if filt_desc else ""

    if focus=="taux_positif":
        answer=f"Le meilleur moment intraday pour {asset} sur un horizon de {horizon_minutes} minute(s) est {top['bucket']} avec un taux positif de {_display_pct(top['taux_positif'])}, une variation moyenne de {_display_pct(top['moyenne_variation'])} et {top['count']} cas{filt_txt}."
    elif focus=="count":
        answer=f"Le créneau intraday le plus fréquent pour {asset} sur un horizon de {horizon_minutes} minute(s) est {top['bucket']} avec {top['count']} cas, une variation moyenne de {_display_pct(top['moyenne_variation'])} et un taux positif de {_display_pct(top['taux_positif'])}{filt_txt}."
    else:
        answer=f"Le meilleur edge intraday détecté pour {asset} sur un horizon de {horizon_minutes} minute(s) est {top['bucket']} avec une variation moyenne de {_display_pct(top['moyenne_variation'])}, un taux positif de {_display_pct(top['taux_positif'])} et {top['count']} cas{filt_txt}."

    sources=[target_entry.get("file_name")]
    if vix1d_entry:
        sources.append(vix1d_entry.get("file_name"))
    if dxy_entry:
        sources.append(dxy_entry.get("file_name"))

    return {
        "engine":"intraday_edge_engine",
        "status":"OK",
        "mode":"intraday_edge_scan",
        "target_asset":asset,
        "metric_focus":focus,
        "horizon_minutes":int(horizon_minutes),
        "bar_minutes":int(bar_minutes),
        "value":top["bucket"],
        "answer":answer,
        "summary":"Scan intraday par créneau horaire sur la série intraday cible, avec filtres optionnels VIX1D et DXY.",
        "ranking":rows[:preview_rows],
        "source_file_names":sources,
    }

def run(question, preview_rows=12):
    nq=_nrm(question)
    asset=_detect_asset(nq,"SPX")
    out=_build_intraday_scan(asset,nq,preview_rows=preview_rows)
    if out is None:
        return {
            "engine":"intraday_edge_engine",
            "status":"NO_INTRADAY_RESULT",
            "answer":"Je n'ai pas pu construire de scan intraday exploitable pour cette question."
        }
    return out
