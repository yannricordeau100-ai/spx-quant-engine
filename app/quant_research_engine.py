import os, re, json, unicodedata
import pandas as pd

ASSET_REGISTRY_PATH=r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/ETAPE197_ASSET_TIMEFRAME_REGISTRY.json"

with open(ASSET_REGISTRY_PATH,"r",encoding="utf-8") as f:
    _REG=(json.load(f).get("assets",{}) or {})

ASSET_ALIASES={
    "SPX":["spx","s&p 500","s&p500","s&p"],
    "SPY":["spy"],
    "QQQ":["qqq"],
    "IWM":["iwm"],
    "VIX":["vix"],
    "VVIX":["vvix"],
    "VIX1D":["vix1d","vix 1d"],
    "VIX9D":["vix9d","vix 9d"],
    "DXY":["dxy","dollar index","dollar"],
    "US10Y":["us10y","10 ans us","10y","taux us 10 ans"],
    "GOLD":["gold","or"],
    "OIL":["oil","petrole","pétrole"],
    "TICK":["tick"],
    "CALENDAR":["calendar","calendrier","macro","calendrier economique","annonces economiques","annonces économiques","cpi","fomc","nfp","inflation","resultats","résultats","earnings"],
}

DEFAULT_RANKING_BASKET=["SPX","SPY","QQQ","IWM"]

COMPARABLE_TARGET_ASSETS=set(["SPX","SPY","QQQ","IWM","GOLD","OIL","DXY","US10Y","VIX","VVIX","VIX1D","VIX9D"])

def _filter_comparison_assets(assets, nq):
    assets=[a for a in assets if a in COMPARABLE_TARGET_ASSETS and a != "CALENDAR"]
    # remove obvious context/filter-only calendar words if they leaked through aliases
    return list(dict.fromkeys(assets))

METRIC_ALIASES={
    "moyenne_variation":["performance","variation","rendement","retour","moyenne","variation moyenne","performance moyenne","evolution moyenne","évolution moyenne"],
    "taux_positif":["taux positif","winrate","taux de reussite","taux de réussite","part positive","part des cas positifs","proportion positive","pourcentage positif","quelle part","quelle proportion","quel pourcentage"],
    "count":["combien","nombre de cas","nombre d'occurrences","nombre de fois","nb de cas","combien de cas","combien de fois","quel nombre de cas","combien d'occurrences"],
    "meilleure_variation":["meilleure variation","meilleur move","best move","meilleure performance","plus forte hausse","variation maximale","hausse maximale"],
    "pire_variation":["pire variation","pire move","worst move","plus forte baisse","perte maximale brute","variation minimale","baisse maximale"],
    "count_directional_target":["en hausse","en baisse","positif","negative","négatif","negatif"],
}

MONTH_MAP={
    "janvier":1,"fevrier":2,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,
    "juillet":7,"aout":8,"août":8,"septembre":9,"octobre":10,"novembre":11,"decembre":12,"décembre":12
}

CALENDAR_KEYWORDS={
    "cpi":["cpi","inflation"],
    "fomc":["fomc","fed","federal reserve"],
    "nfp":["nfp","nonfarm","non farm"],
    "earnings":["earnings","resultats","résultats"],
    "options_expiration":["options expiration","expiration options","opex"],
    "macro":["macro","annonce economique","annonces economiques","annonce économique","annonces économiques"],
}

def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))

def _nrm(s):
    s="" if s is None else str(s)
    s=s.lower()
    s=_strip_accents(s)
    s=s.replace("’","'").replace("`","'")
    repl=[
        ("cloturee","cloture"),("cloture","cloture"),
        ("superieure","superieur"),("inferieure","inferieur"),
        ("a quand remonte","quand remonte"),
        ("au dessus de","superieur a"),("au-dessus de","superieur a"),
        ("au dessous de","inferieur a"),("au-dessous de","inferieur a"),("en dessous de","inferieur a"),
        ("lorsque","quand"),("si ","quand "),
        ("journee","jour"),("journees","jours"),
        ("haussiere","positif"),("baissiere","negatif"),
        ("est au-dessus de","superieur a"),("est au dessus de","superieur a"),
        ("est sous","inferieur a"),("est en dessous de","inferieur a"),
        ("versus","vs"),
    ]
    for a,b in repl:
        s=s.replace(a,b)
    s=re.sub(r"[^a-z0-9%+<>=/.' -]+"," ",s)
    s=re.sub(r"\s+"," ",s).strip()
    return s

def _contains_term(nq, term):
    return re.search(rf"(?<![a-z0-9]){re.escape(term)}(?![a-z0-9])", nq) is not None

def _detect_assets(nq):
    hits=[]
    for asset, arr in ASSET_ALIASES.items():
        if any(_contains_term(nq,t) for t in arr):
            hits.append(asset)
    return hits

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

def _load_dataset_entry(entry, label):
    df=_read_csv_flex(entry["path"])
    df=_norm_cols(df)
    tcol=_find_time_col(df)
    if tcol is None:
        raise RuntimeError(f"NO_TIME_COLUMN::{entry['file_name']}")
    df["timestamp"]=pd.to_datetime(df[tcol],errors="coerce")
    df=df[df["timestamp"].notna()].copy().sort_values("timestamp")
    rename={}
    for c in df.columns:
        if c in ["timestamp",tcol]:
            continue
        rename[c]=f"{c}_{label}"
    return df.rename(columns=rename)

def _best_entry_for_asset(asset, requested_minutes=None):
    arr=_REG.get(asset,[]) or []
    if not arr:
        return None
    valid=[x for x in arr if x.get("bar_minutes") is not None]
    if not valid:
        return arr[0]
    if requested_minutes is None:
        return valid[0]
    exact=[x for x in valid if x["bar_minutes"]==requested_minutes]
    if exact:
        return exact[0]
    finer=[x for x in valid if x["bar_minutes"]<=requested_minutes]
    if finer:
        return sorted(finer, key=lambda x:x["bar_minutes"], reverse=True)[0]
    return valid[0]

def _extract_thresholds(nq):
    thresholds=[]
    ranges=[]
    for asset, arr in ASSET_ALIASES.items():
        aliases=sorted(arr, key=len, reverse=True)
        found=False
        for alias in aliases:
            m_range=re.search(rf"{re.escape(alias)}\s+entre\s+(-?\d+(?:[.,]\d+)?)\s+et\s+(-?\d+(?:[.,]\d+)?)", nq)
            if m_range:
                low=float(m_range.group(1).replace(",","."))
                high=float(m_range.group(2).replace(",","."))
                ranges.append((asset,low,high))
                found=True
                break

            patterns=[
                rf"{re.escape(alias)}\s*(?:est\s+)?(>=|<=|>|<|superieur a|inferieur a)\s*(-?\d+(?:[.,]\d+)?)",
                rf"{re.escape(alias)}\s*(?:est\s+)?(?:au dessus de|au-dessus de|superieur a)\s*(-?\d+(?:[.,]\d+)?)",
                rf"{re.escape(alias)}\s*(?:est\s+)?(?:sous|inferieur a|en dessous de|au-dessous de)\s*(-?\d+(?:[.,]\d+)?)",
            ]
            for i,patt in enumerate(patterns):
                m=re.search(patt,nq)
                if m:
                    if i==0:
                        op,val=m.group(1),float(m.group(2).replace(",","."))
                        if op=="superieur a": op=">"
                        if op=="inferieur a": op="<"
                    elif i==1:
                        op,val=">",float(m.group(1).replace(",","."))
                    else:
                        op,val="<",float(m.group(1).replace(",","."))
                    thresholds.append((asset,op,val))
                    found=True
                    break
            if found:
                break
    return thresholds, ranges

def _extract_directional_conditions(nq):
    conds=[]
    for asset, arr in ASSET_ALIASES.items():
        aliases=sorted(arr, key=len, reverse=True)
        for alias in aliases:
            if re.search(rf"{re.escape(alias)}\s+(?:est\s+)?(?:positif|en hausse|hausse)", nq):
                conds.append((asset,"direction",">0"))
                break
            if re.search(rf"{re.escape(alias)}\s+(?:est\s+)?(?:negatif|négatif|en baisse|baisse)", nq):
                conds.append((asset,"direction","<0"))
                break
    return conds

def _extract_target_asset(nq, assets):
    for asset, aliases in ASSET_ALIASES.items():
        for alias in aliases:
            for patt in [
                rf"variation\s+de\s+{re.escape(alias)}",
                rf"variation\s+du\s+{re.escape(alias)}",
                rf"performance\s+de\s+{re.escape(alias)}",
                rf"performance\s+du\s+{re.escape(alias)}",
                rf"rendement\s+de\s+{re.escape(alias)}",
                rf"rendement\s+du\s+{re.escape(alias)}",
                rf"retour\s+de\s+{re.escape(alias)}",
                rf"retour\s+du\s+{re.escape(alias)}",
                rf"taux\s+positif\s+de\s+{re.escape(alias)}",
                rf"quelle\s+part\s+du\s+{re.escape(alias)}",
                rf"quelle\s+proportion\s+du\s+{re.escape(alias)}",
                rf"quel\s+pourcentage\s+du\s+{re.escape(alias)}",
                rf"{re.escape(alias)}\s+en\s+hausse",
                rf"{re.escape(alias)}\s+en\s+baisse",
                rf"{re.escape(alias)}\s+quand",
                rf"{re.escape(alias)}\s+sur\s+",
            ]:
                if re.search(patt,nq):
                    return asset
    assets=[a for a in assets if a != "CALENDAR"]
    if "SPX" in assets:
        return "SPX"
    return assets[0] if assets else "SPX"

def _extract_target_outcome_rule(nq, target_asset):
    for alias in ASSET_ALIASES.get(target_asset,[]):
        if re.search(rf"{re.escape(alias)}\s+(?:est\s+)?(?:en hausse|positif|hausse)", nq):
            return ">0"
        if re.search(rf"{re.escape(alias)}\s+(?:est\s+)?(?:en baisse|negatif|négatif|baisse)", nq):
            return "<0"
    if "quelle part" in nq or "quelle proportion" in nq or "quel pourcentage" in nq:
        return ">0"
    return None

def _extract_metric(nq, target_outcome_rule=None):
    if "plus de cas" in nq or "le plus de cas" in nq or "plus grand nombre de cas" in nq:
        return "count"
    priority=[
        ("meilleure_variation", METRIC_ALIASES.get("meilleure_variation",[])),
        ("pire_variation", METRIC_ALIASES.get("pire_variation",[])),
        ("taux_positif", METRIC_ALIASES.get("taux_positif",[])),
        ("count", METRIC_ALIASES.get("count",[])),
        ("moyenne_variation", METRIC_ALIASES.get("moyenne_variation",[])),
    ]
    for metric, arr in priority:
        if any(x in nq for x in arr):
            return metric
    if target_outcome_rule is not None:
        return "count_directional_target"
    return "moyenne_variation"

def _extract_horizon(nq, target_entry=None):
    def unit_to_minutes(v,u):
        u=u.lower()
        if u in ["min","minute","minutes"]: return v
        if u in ["h","heure","heures"]: return v*60
        if u in ["jour","jours","j"]: return v*1440
        if u in ["semaine","semaines"]: return v*10080
        if u in ["mois"]: return v*43200
        if u in ["an","ans","annee","annees"]: return v*525600
        return None

    mwin=re.search(r"(?:de|entre)\s+(\d+)\s*(min|minute|minutes|h|heure|heures|jour|jours|j|semaine|semaines|mois|an|ans|annee|annees)\s+(?:a|et)\s+(\d+)\s*(min|minute|minutes|h|heure|heures|jour|jours|j|semaine|semaines|mois|an|ans|annee|annees)", nq)
    if mwin:
        return {
            "mode":"window",
            "start_minutes":unit_to_minutes(int(mwin.group(1)),mwin.group(2)),
            "end_minutes":unit_to_minutes(int(mwin.group(3)),mwin.group(4)),
            "label":f"de {mwin.group(1)} {mwin.group(2)} a {mwin.group(3)} {mwin.group(4)}",
        }

    mh=re.search(r"(?:sur|a horizon de|horizon|sur l'horizon de|sur un horizon de)\s+(\d+)\s*(min|minutte|minute|minutes|h|heure|heures|jour|jours|j|semaine|semaines|mois|an|ans|annee|annees)", nq)
    if mh:
        return {
            "mode":"forward",
            "minutes":unit_to_minutes(int(mh.group(1)), mh.group(2)),
            "label":f"{mh.group(1)} {mh.group(2)}",
        }

    if "journalier" in nq:
        return {"mode":"forward","minutes":1440,"label":"1 jour"}
    if "mensuel" in nq:
        return {"mode":"forward","minutes":43200,"label":"1 mois"}
    if "hebdomadaire" in nq:
        return {"mode":"forward","minutes":10080,"label":"1 semaine"}
    if "minutier" in nq:
        return {"mode":"forward","minutes":1,"label":"1 minute"}

    default_minutes=target_entry.get("bar_minutes") if target_entry else 1440
    return {
        "mode":"forward",
        "minutes":default_minutes,
        "label":"1 barre"
    }

def _extract_month_filter(nq):
    for k,v in MONTH_MAP.items():
        if re.search(rf"(?<![a-z0-9]){re.escape(k)}(?![a-z0-9])", nq):
            return v
    return None

def _extract_year_filter(nq):
    years=[int(x) for x in re.findall(r"(?<!\d)(20\d{2})(?!\d)", nq)]
    return years[0] if years else None

def _extract_calendar_filter(nq):
    hits=[]
    for key, arr in CALENDAR_KEYWORDS.items():
        if any(a in nq for a in arr):
            hits.append(key)
    return hits

def _detect_compare_mode(nq, assets):
    if "classement" in nq or "classer" in nq or "ranking" in nq:
        return "ranking"
    if " vs " in f" {nq} " or " comparer " in f" {nq} " or "compare " in f" {nq} " or "comparaison" in nq:
        return "compare"
    if ("meilleur" in nq or "plus performant" in nq or "moins performant" in nq or "quel actif" in nq or "plus de cas" in nq or "meilleur taux positif" in nq) and len(assets)>=2:
        return "ranking"
    return None


def _extract_filter_assets_for_comparison(nq):
    filter_assets=set()
    thresholds, ranges=_extract_thresholds(nq)
    directional=_extract_directional_conditions(nq)
    for a,_,_ in thresholds:
        filter_assets.add(a)
    for a,_,_ in ranges:
        filter_assets.add(a)
    for a,_,_ in directional:
        filter_assets.add(a)
    if _extract_calendar_filter(nq):
        filter_assets.add("CALENDAR")
    return filter_assets

def _comparison_asset_list(nq, assets):
    assets=_filter_comparison_assets(assets, nq)
    filter_assets=_extract_filter_assets_for_comparison(nq)
    explicit_assets=[a for a in assets if a not in filter_assets]
    if len(explicit_assets) >= 2:
        return explicit_assets
    if "classement" in nq or "ranking" in nq or "quel actif" in nq:
        basket=[a for a in DEFAULT_RANKING_BASKET if a not in filter_assets]
        return basket
    return explicit_assets

def _load_calendar_context():
    entry=_best_entry_for_asset("CALENDAR",1440)
    if entry is None:
        return None,None
    df=_read_csv_flex(entry["path"])
    df=_norm_cols(df)
    tcol=_find_time_col(df)
    if tcol is None:
        return None,None
    df["timestamp"]=pd.to_datetime(df[tcol],errors="coerce")
    df=df[df["timestamp"].notna()].copy()
    df["date_key"]=df["timestamp"].dt.floor("D")
    df["weekday"]=df["timestamp"].dt.weekday
    text_cols=[c for c in df.columns if c not in ["timestamp","date_key",tcol]]
    if text_cols:
        blob=df[text_cols].astype(str).agg(" | ".join, axis=1).str.lower()
        blob=blob.map(_strip_accents)
    else:
        blob=pd.Series([""]*len(df), index=df.index)
    out=df[["date_key"]].copy()
    out["calendar_blob"]=blob
    out=out.groupby("date_key",as_index=False).agg({"calendar_blob":" | ".join})
    return out,entry

def _merge_conditions_to_target(target_df, cond_dfs):
    base=target_df.copy().sort_values("timestamp")
    for d in cond_dfs:
        dd=d.copy().sort_values("timestamp")
        base=pd.merge_asof(base, dd, on="timestamp", direction="backward")
    return base

def _apply_context_filters(df, thresholds, ranges, directional_conditions=None, month_filter=None, year_filter=None, calendar_filters=None):
    mask=pd.Series(True,index=df.index)
    for asset,op,val in thresholds:
        col=f"close_{asset.lower()}"
        if col not in df.columns:
            continue
        s=pd.to_numeric(df[col],errors="coerce")
        if op==">": mask&=(s>val)
        elif op=="<": mask&=(s<val)
        elif op==">=": mask&=(s>=val)
        elif op=="<=": mask&=(s<=val)

    for asset,low,high in ranges:
        col=f"close_{asset.lower()}"
        if col not in df.columns:
            continue
        s=pd.to_numeric(df[col],errors="coerce")
        mask&=(s>=low) & (s<=high)

    for item in (directional_conditions or []):
        asset, kind, rule = item
        close_col=f"close_{asset.lower()}"
        open_col=f"open_{asset.lower()}"
        if close_col not in df.columns:
            continue
        c=pd.to_numeric(df[close_col],errors="coerce")
        if open_col in df.columns:
            o=pd.to_numeric(df[open_col],errors="coerce")
            delta=c-o
        else:
            delta=c.diff()
        if rule==">0":
            mask&=(delta>0)
        elif rule=="<0":
            mask&=(delta<0)

    if month_filter is not None:
        mask&=(df["timestamp"].dt.month==month_filter)
    if year_filter is not None:
        mask&=(df["timestamp"].dt.year==year_filter)

    if calendar_filters:
        blob=df.get("calendar_blob")
        if blob is not None:
            blob=blob.fillna("").astype(str).str.lower().map(_strip_accents)
            cal_mask=pd.Series(True,index=df.index)
            for cf in calendar_filters:
                if cf=="cpi":
                    cal_mask&=blob.str.contains("cpi|inflation",regex=True)
                elif cf=="fomc":
                    cal_mask&=blob.str.contains("fomc|fed|federal reserve",regex=True)
                elif cf=="nfp":
                    cal_mask&=blob.str.contains("nfp|nonfarm|non farm",regex=True)
                elif cf=="earnings":
                    cal_mask&=blob.str.contains("earnings|resultats|major companies",regex=True)
                elif cf=="options_expiration":
                    cal_mask&=blob.str.contains("options expiration|expiration",regex=True)
                elif cf=="macro":
                    cal_mask&=(blob.str.len()>0)
            mask&=cal_mask
        else:
            mask&=False

    return mask

def _infer_horizon_bars(target_entry, horizon):
    bar_m=target_entry.get("bar_minutes") or 1440
    if horizon["mode"]=="forward":
        return max(1, int(round(horizon["minutes"]/bar_m)))
    start_b=max(0, int(round(horizon["start_minutes"]/bar_m)))
    end_b=max(start_b+1, int(round(horizon["end_minutes"]/bar_m)))
    return (start_b,end_b)

def _metric_label_from_minutes(minutes):
    if minutes <= 1: return "minutier"
    if minutes < 60: return "intraday"
    if minutes == 60: return "horaire"
    if minutes < 1440: return "multi-heures"
    if minutes == 1440: return "journalier"
    if minutes < 10080: return "multi-jours"
    if minutes == 10080: return "hebdomadaire"
    if minutes < 43200: return "multi-semaines"
    if minutes == 43200: return "mensuel"
    if minutes < 525600: return "multi-mois"
    return "annuel"

def _compute_forward_stats(df, target_asset, target_entry, horizon):
    label=target_asset.lower()
    close_col=f"close_{label}"
    if close_col not in df.columns:
        raise RuntimeError(f"MISSING_TARGET_CLOSE::{target_asset}")
    s=pd.to_numeric(df[close_col],errors="coerce")
    bar_minutes=target_entry.get("bar_minutes") or 1440

    if horizon["mode"]=="forward":
        bars=_infer_horizon_bars(target_entry,horizon)
        future=s.shift(-bars)
        ret=(future/s)-1.0
        metric_minutes=bars*bar_minutes
    else:
        start_b,end_b=_infer_horizon_bars(target_entry,horizon)
        start_px=s.shift(-start_b)
        end_px=s.shift(-end_b)
        ret=(end_px/start_px)-1.0
        metric_minutes=max(1,(end_b-start_b)*bar_minutes)

    r=ret.dropna()
    if len(r)==0:
        return {
            "count":0,
            "moyenne_variation":None,
            "taux_positif":None,
            "meilleure_variation":None,
            "pire_variation":None,
            "horizon_label":horizon["label"],
            "frequence_taux_positif":_metric_label_from_minutes(metric_minutes),
            "explication_taux_positif":"Aucun cas exploitable pour calculer le taux positif.",
            "_raw_returns":ret,
        }

    freq_label=_metric_label_from_minutes(metric_minutes)
    return {
        "count":int(len(r)),
        "moyenne_variation":float(r.mean()),
        "taux_positif":float((r>0).mean()),
        "meilleure_variation":float(r.max()),
        "pire_variation":float(r.min()),
        "horizon_label":horizon["label"],
        "frequence_taux_positif":freq_label,
        "explication_taux_positif":f"Le taux positif donne la part des cas ou la variation sur l'horizon {horizon['label']} est positive. Ici il s'agit d'une lecture {freq_label} basee sur la serie cible {target_entry.get('file_name')}.",
        "_raw_returns":ret,
    }

def _compute_target_directional_count(stats, rule):
    r=stats.get("_raw_returns")
    if r is None:
        return {"count_directional":0,"ratio_directional":None}
    rr=r.dropna()
    if len(rr)==0:
        return {"count_directional":0,"ratio_directional":None}
    if rule==">0":
        m=(rr>0)
    else:
        m=(rr<0)
    return {"count_directional":int(m.sum()),"ratio_directional":float(m.mean())}

def _context_text(month_filter, year_filter, calendar_filters):
    parts=[]
    if month_filter is not None:
        inv={v:k for k,v in MONTH_MAP.items()}
        parts.append(inv.get(month_filter,str(month_filter)))
    if year_filter is not None:
        parts.append(str(year_filter))
    if calendar_filters:
        parts.append(" / ".join(calendar_filters))
    if not parts:
        return ""
    return " | contexte: " + ", ".join(parts)

def _build_answer(metric, stats, target_asset, target_rule=None, context_text=""):
    if stats["count"]==0:
        return f"Aucun cas retenu pour {target_asset} avec les conditions demandées sur l'horizon {stats['horizon_label']}{context_text}."
    if metric=="count":
        return f"Pour {target_asset}, {stats['count']} cas ont été retenus sur l'horizon {stats['horizon_label']}{context_text}."
    if metric=="count_directional_target":
        extra=_compute_target_directional_count(stats,target_rule or ">0")
        direction_txt="en hausse" if (target_rule or ">0")==">0" else "en baisse"
        ratio=(extra["ratio_directional"] or 0.0)*100.0 if extra["ratio_directional"] is not None else None
        return f"Pour {target_asset}, {extra['count_directional']} cas sur {stats['count']} sont {direction_txt} sur l'horizon {stats['horizon_label']}, soit {ratio:.2f}%{context_text}."
    if metric=="taux_positif":
        tp=stats["taux_positif"]*100.0
        return f"Pour {target_asset}, le taux positif sur l'horizon {stats['horizon_label']} est de {tp:.2f}%. Il s'agit bien d'une lecture {stats['frequence_taux_positif']}{context_text}."
    if metric=="meilleure_variation":
        best=stats["meilleure_variation"]*100.0
        return f"Pour {target_asset}, la meilleure variation sur l'horizon {stats['horizon_label']} est de {best:.2f}%{context_text}."
    if metric=="pire_variation":
        worst=stats["pire_variation"]*100.0
        return f"Pour {target_asset}, la pire variation sur l'horizon {stats['horizon_label']} est de {worst:.2f}%{context_text}."
    mv=stats["moyenne_variation"]*100.0
    tp=stats["taux_positif"]*100.0
    return f"Pour {target_asset}, {stats['count']} cas ont ete retenus. La variation moyenne sur l'horizon {stats['horizon_label']} est de {mv:.2f}%. Le taux positif est de {tp:.2f}% et il est bien {stats['frequence_taux_positif']} dans ce contexte{context_text}."

def _metric_numeric_value(metric, stats, target_rule=None, nq=""):
    if metric=="count":
        return stats["count"]
    if metric=="count_directional_target":
        extra=_compute_target_directional_count(stats,target_rule or ">0")
        if "quelle part" in nq or "quelle proportion" in nq or "quel pourcentage" in nq:
            return extra["ratio_directional"]
        return extra["count_directional"]
    if metric=="taux_positif":
        return stats["taux_positif"]
    if metric=="meilleure_variation":
        return stats["meilleure_variation"]
    if metric=="pire_variation":
        return stats["pire_variation"]
    return stats["moyenne_variation"]

def _run_single_asset(question, target_asset, preview_rows=20):
    nq=_nrm(question)
    weekday_filter, weekday_label = _parse_weekday_from_nq(nq)
    target_entry=_best_entry_for_asset(target_asset, None)
    if target_entry is None:
        return {
            "status":"NO_QUANT_TARGET",
            "answer":"Aucun actif cible exploitable n'a ete trouve pour cette question."
        }

    target_rule=_extract_target_outcome_rule(nq, target_asset)
    metric=_extract_metric(nq, target_rule)
    horizon=_extract_horizon(nq, target_entry)
    requested_minutes=horizon.get("minutes") if horizon["mode"]=="forward" else horizon.get("end_minutes")

    target_entry=_best_entry_for_asset(target_asset, requested_minutes)
    target_df=_load_dataset_entry(target_entry, target_asset.lower())
    target_df["date_key"]=target_df["timestamp"].dt.floor("D")

    thresholds, ranges=_extract_thresholds(nq)
    directional_conditions=_extract_directional_conditions(nq)
    directional_conditions=[x for x in directional_conditions if x[0] != target_asset]

    month_filter=_extract_month_filter(nq)
    year_filter=_extract_year_filter(nq)
    calendar_filters=_extract_calendar_filter(nq)

    cond_assets=sorted(set(
        [a for a,_,_ in thresholds if a != target_asset] +
        [a for a,_,_ in ranges if a != target_asset] +
        [a for a,_,_ in directional_conditions if a != target_asset]
    ))

    cond_dfs=[]
    used_entries=[target_entry]
    for asset in cond_assets:
        entry=_best_entry_for_asset(asset, requested_minutes)
        if entry is None:
            continue
        d=_load_dataset_entry(entry, asset.lower())
        d["date_key"]=d["timestamp"].dt.floor("D")
        cond_dfs.append(d)
        used_entries.append(entry)

    merged=_merge_conditions_to_target(target_df, cond_dfs)

    if calendar_filters:
        cal_df, cal_entry = _load_calendar_context()
        if cal_df is not None:
            merged=merged.merge(cal_df,on="date_key",how="left")
            used_entries.append(cal_entry)

    mask=_apply_context_filters(
        merged,
        thresholds=thresholds,
        ranges=ranges,
        directional_conditions=directional_conditions,
        month_filter=month_filter,
        year_filter=year_filter,
        calendar_filters=calendar_filters
    )

    filtered=merged[mask].copy()
    stats=_compute_forward_stats(filtered, target_asset, target_entry, horizon)
    context_text=_context_text(month_filter, year_filter, calendar_filters)

    preview=[]
    if len(filtered)>0:
        keep_cols=[c for c in filtered.columns if c.startswith("timestamp") or c.startswith("close_") or c.startswith("open_") or c=="calendar_blob"]
        preview=filtered[keep_cols].head(preview_rows).to_dict(orient="records")

    value=_metric_numeric_value(metric, stats, target_rule, nq)

    return {
        "engine":"quant_research_engine",
        "status":"OK",
        "target_asset":target_asset,
        "metric":metric,
        "target_dataset":target_entry.get("file_name"),
        "conditions":thresholds,
        "ranges":ranges,
        "directional_conditions":directional_conditions,
        "target_outcome_rule":target_rule,
        "month_filter":month_filter,
        "year_filter":year_filter,
        "calendar_filters":calendar_filters,
        "horizon":horizon,
        "stats":{k:v for k,v in stats.items() if k != "_raw_returns"},
        "value":value,
        "answer":_build_answer(metric, stats, target_asset, target_rule, context_text),
        "summary":stats["explication_taux_positif"],
        "source_file_names":[x.get("file_name") for x in used_entries if x is not None],
        "source_paths":[x.get("path") for x in used_entries if x is not None],
        "preview":preview,
    }

def _comparison_answer(nq, metric, ranked, horizon_label):
    if not ranked:
        return "Aucun actif comparable n'a pu etre evalue."
    if len(ranked)==1:
        a=ranked[0]
        return f"Un seul actif comparable a ete retenu: {a['asset']}."

    if (" vs " in f" {nq} " or " comparer " in f" {nq} " or "compare " in f" {nq} " or "comparaison" in nq) and len(ranked)>=2:
        first=ranked[0]; second=ranked[1]
        if metric=="taux_positif":
            return f"Entre {first['asset']} et {second['asset']}, {first['asset']} ressort devant sur l'horizon {horizon_label} avec un taux positif de {first['display_value']} contre {second['display_value']}."
        return f"Entre {first['asset']} et {second['asset']}, {first['asset']} ressort devant sur l'horizon {horizon_label} avec {first['display_value']} contre {second['display_value']}."

    if "quel actif" in nq or "meilleur actif" in nq or "plus performant" in nq or "moins performant" in nq or "plus de cas" in nq or "plus grand nombre de cas" in nq:
        first=ranked[0]
        if metric=="taux_positif":
            return f"L'actif qui ressort devant est {first['asset']} sur l'horizon {horizon_label}, avec un taux positif de {first['display_value']}."
        if metric=="count":
            return f"L'actif qui ressort devant est {first['asset']} sur l'horizon {horizon_label}, avec {first['display_value']} cas."
        return f"L'actif qui ressort devant est {first['asset']} sur l'horizon {horizon_label}, avec {first['display_value']}."

    lines=[f"Classement sur l'horizon {horizon_label} :"]
    for i,row in enumerate(ranked,1):
        lines.append(f"{i}. {row['asset']} -> {row['display_value']}")
    return " ".join(lines)

def _display_value(metric, value):
    if value is None:
        return "n.d."
    if metric in ["moyenne_variation","taux_positif","meilleure_variation","pire_variation"]:
        return f"{value*100:.2f}%"
    return str(value)



def _parse_weekday_from_nq(nq):
    weekday_map={
        "lundi":0,"mardi":1,"mercredi":2,"jeudi":3,"vendredi":4,
        "monday":0,"tuesday":1,"wednesday":2,"thursday":3,"friday":4
    }
    for k,v in weekday_map.items():
        if re.search(rf"(?<![a-z0-9]){re.escape(k)}(?![a-z0-9])", nq):
            return v, k
    return None, None
def can_handle(question):
    nq=_nrm(question)
    weekday_filter, weekday_label = _parse_weekday_from_nq(nq)
    trigger_words=[
        "performance","variation","rendement","retour","taux positif","winrate","taux de reussite","taux de réussite",
        "moyenne","meilleure variation","pire variation","combien de cas","nombre de cas","plus de cas","le plus de cas","plus grand nombre de cas",
        "quelle part","quelle proportion","quel pourcentage","en hausse","en baisse","positif","negatif","négatif",
        "cpi","fomc","nfp","macro","resultats","résultats","earnings","janvier","fevrier","février","mars","avril","mai","juin","juillet","aout","août","septembre","octobre","novembre","decembre","décembre",
        "classement","classer","vs","comparaison","comparer","compare","meilleur actif","moins performant","plus performant","quel actif"
    ]
    return any(x in nq for x in trigger_words)

def run(question, preview_rows=20):
    nq=_nrm(question)
    weekday_filter, weekday_label = _parse_weekday_from_nq(nq)
    assets=_detect_assets(nq)
    compare_mode=_detect_compare_mode(nq, assets)
    compare_assets=_comparison_asset_list(nq, assets)
    if compare_mode and len(compare_assets) < 2:
        compare_assets=[a for a in DEFAULT_RANKING_BASKET if a not in _extract_filter_assets_for_comparison(nq)]

    if compare_mode and compare_assets:
        metric=_extract_metric(nq, None)
        rows=[]
        all_sources=[]
        horizon_label=None
        for asset in compare_assets:
            single=_run_single_asset(question, asset, preview_rows=0)
            if single.get("status") != "OK":
                continue
            val=single.get("value")
            if val is None:
                continue
            rows.append({
                "asset":asset,
                "metric":metric,
                "value":val,
                "display_value":_display_value(metric,val),
                "count":(single.get("stats") or {}).get("count"),
                "target_dataset":single.get("target_dataset"),
            })
            for s in (single.get("source_file_names") or []):
                if s not in all_sources:
                    all_sources.append(s)
            if horizon_label is None:
                horizon_label=((single.get("horizon") or {}).get("label"))

        reverse=True
        if metric=="pire_variation":
            reverse=False
        rows=sorted(rows, key=lambda x:(x["value"] is None, x["value"]), reverse=reverse)
        top_asset=rows[0]["asset"] if rows else None

        return {
            "engine":"quant_research_engine",
            "status":"OK",
            "mode":"comparison" if compare_mode=="compare" else "ranking",
            "metric":metric,
            "comparison_assets":compare_assets,
            "ranking":rows,
            "value":top_asset,
            "answer":_comparison_answer(nq, metric, rows, horizon_label or "n.d."),
            "summary":"Comparaison construite sur la meme logique de filtre quant appliquee a plusieurs actifs.",
            "source_file_names":all_sources,
            "source_paths":[],
            "preview":rows[:preview_rows],
        }

    if not assets:
        assets=["SPX"]
    target_asset=_extract_target_asset(nq, assets)
    return _run_single_asset(question, target_asset, preview_rows=preview_rows)
