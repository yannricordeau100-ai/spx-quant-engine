import os, re, json

ROOT="/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
PROC=os.path.join(ROOT,"processed")
REGISTRY=os.path.join(PROC,"ETAPE197_ASSET_TIMEFRAME_REGISTRY.json")

def _fmt_pct(x):
    try:
        return f"{float(x)*100:.2f}%".replace(".",",")
    except Exception:
        return None

def _fmt_pct_points(x):
    try:
        sign="+" if float(x) >= 0 else ""
        return f"{sign}{float(x)*100:.2f}%".replace(".",",")
    except Exception:
        return None

def _fr_decimal_in_text(s):
    s=str(s or "")
    s=re.sub(r"(\d+)\.(\d+)%", lambda m: f"{m.group(1)},{m.group(2)}%", s)
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
    s=_fr_decimal_in_text(s)
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

def _dataset_date_range_by_filename(filename):
    if not filename:
        return None, None
    path=None
    try:
        if os.path.exists(REGISTRY):
            raw=json.load(open(REGISTRY,"r",encoding="utf-8"))
            assets=raw.get("assets",{}) or {}
            for _, arr in assets.items():
                for x in (arr or []):
                    if str(x.get("file_name","")) == str(filename) and os.path.exists(x.get("path","")):
                        path=x.get("path")
                        break
                if path:
                    break
    except Exception:
        path=None
    if path is None or not os.path.exists(path):
        return None, None
    try:
        import pandas as pd
        for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
            for sep in (None,",",";","\t","|"):
                try:
                    if sep is None:
                        df=pd.read_csv(path, sep=None, engine="python", encoding=enc, on_bad_lines="skip")
                    else:
                        df=pd.read_csv(path, sep=sep, engine="python", encoding=enc, on_bad_lines="skip")
                    tcol=None
                    for c in df.columns:
                        cl=str(c).lower()
                        if cl in ["time","date","datetime","timestamp"] or "time" in cl or "date" in cl:
                            tcol=c
                            break
                    if tcol is None:
                        continue
                    s=pd.to_datetime(df[tcol], errors="coerce").dropna()
                    if len(s)==0:
                        continue
                    return str(s.min().date()), str(s.max().date())
                except Exception:
                    pass
    except Exception:
        pass
    return None, None

def _question_mentions_horizon(question):
    q=str(question or "").lower()
    m=re.search(r"sur\s+(\d+\s+(minute|minutes|heure|heures|jour|jours|semaine|semaines|mois|an|ans|annee|annees|month|months|year|years))", q)
    return m.group(1) if m else ""

def format_result(question, result, app_dir=None):
    if not isinstance(result, dict):
        return result

    engine=result.get("engine","")
    q=str(question or "")
    source_files=result.get("source_file_names") or []

    if engine in {"natural_stats_engine","aau_research_engine"}:
        if "answer_short" in result and isinstance(result["answer_short"], str):
            result["answer_short"]=_clean_text(result["answer_short"])
        if "answer_long" in result and isinstance(result["answer_long"], str):
            result["answer_long"]=_clean_text(result["answer_long"])
        return result

    if engine=="quant_research_engine":
        stats=result.get("stats") or {}
        target=result.get("target_asset") or result.get("asset") or ""
        horizon=stats.get("horizon_label") or _question_mentions_horizon(q)
        start,end=_dataset_date_range_by_filename(source_files[0] if source_files else None)

        ranking=result.get("ranking") or []
        if len(ranking) >= 2:
            try:
                a=ranking[0]; b=ranking[1]
                va=float(a.get("value")); vb=float(b.get("value"))
                diff=va-vb
                count=a.get("count")
                result["answer_short"]=f"{a.get('asset')} surperformant de {_fmt_pct_points(diff)}"
                av=str(a.get("display_value")).replace(".",",")
                bv=str(b.get("display_value")).replace(".",",")
                if count is not None and start and end:
                    result["answer_long"]=_clean_text(f"Cela est arrivé {count} fois entre {start} et {end}. En moyenne dans ces conditions, {a.get('asset')} fait {av} et {b.get('asset')} {bv}.")
                elif count is not None:
                    result["answer_long"]=_clean_text(f"Cela est arrivé {count} fois. En moyenne dans ces conditions, {a.get('asset')} fait {av} et {b.get('asset')} {bv}.")
                else:
                    result["answer_long"]=_clean_text(f"En moyenne dans ces conditions, {a.get('asset')} fait {av} et {b.get('asset')} {bv}.")
                return result
            except Exception:
                pass

        metric=result.get("metric")
        count=stats.get("count", result.get("count"))
        taux=stats.get("taux_positif")
        mean=stats.get("moyenne_variation")

        if metric=="taux_positif" and taux is not None:
            result["answer_short"]=_fmt_pct(taux)
            if count is not None and start and end:
                result["answer_long"]=_clean_text(f"Il y a eu {count} cas entre {start} et {end}. Dans {_fmt_pct(taux)} des cas, {target} est positif sur {horizon}.")
            elif count is not None:
                result["answer_long"]=_clean_text(f"Il y a eu {count} cas. Dans {_fmt_pct(taux)} des cas, {target} est positif sur {horizon}.")
            else:
                result["answer_long"]=_clean_text(f"Dans {_fmt_pct(taux)} des cas, {target} est positif sur {horizon}.")
            return result

        if mean is not None:
            result["answer_short"]=_fmt_pct(mean)
            if count is not None and start and end:
                result["answer_long"]=_clean_text(f"Il y a eu {count} cas entre {start} et {end}. En moyenne dans ces conditions, {target} varie de {_fmt_pct(mean)} sur {horizon}.")
            elif count is not None:
                result["answer_long"]=_clean_text(f"Il y a eu {count} cas. En moyenne dans ces conditions, {target} varie de {_fmt_pct(mean)} sur {horizon}.")
            else:
                result["answer_long"]=_clean_text(f"En moyenne dans ces conditions, {target} varie de {_fmt_pct(mean)} sur {horizon}.")
            return result

    if "answer" in result and isinstance(result["answer"],str):
        result["answer"]=_clean_text(result["answer"])
    if "answer_long" in result and isinstance(result["answer_long"],str):
        result["answer_long"]=_clean_text(result["answer_long"])
    return result
