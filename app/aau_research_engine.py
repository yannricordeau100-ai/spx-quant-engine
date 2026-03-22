import os, re, json, unicodedata
import pandas as pd

ROOT="/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
CORE_ASSETS={"SPX","SPY","QQQ","IWM","VIX","VVIX","VIX9D","DXY","GOLD"}

MONTHS_FR={
    "janvier":1,"fevrier":2,"février":2,"mars":3,"avril":4,"mai":5,"juin":6,"juillet":7,
    "aout":8,"août":8,"septembre":9,"octobre":10,"novembre":11,"decembre":12,"décembre":12
}
WEEKDAYS_FR={
    "lundi":0,"mardi":1,"mercredi":2,"jeudi":3,"vendredi":4,
    "monday":0,"tuesday":1,"wednesday":2,"thursday":3,"friday":4
}

def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def _nrm(s):
    s=_strip_accents(str(s).lower())
    s=re.sub(r"[^a-z0-9%+<>=/.' -]+"," ",s)
    s=re.sub(r"\s+"," ",s).strip()
    return s

def _read_csv_flex(path):
    last=None
    for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
        for sep in (None,",",";","\t","|"):
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
    for c in ["time","date","datetime","timestamp"]:
        if c in df.columns:
            return c
    for c in df.columns:
        cl=str(c).lower()
        if "time" in cl or "date" in cl:
            return c
    return None

def _find_close_col(df):
    prefs=["close","adj_close","close_last","last","price"]
    for c in prefs:
        if c in df.columns:
            return c
    for c in df.columns:
        if "close" in str(c).lower():
            return c
    return None

def _discover_aau_files():
    out={}
    for root, dirs, files in os.walk(ROOT):
        path_norm=root.replace("\\","/").upper()
        if "/AAU/" not in path_norm and not path_norm.endswith("/AAU"):
            continue
        for fn in files:
            if not fn.lower().endswith(".csv"):
                continue
            base=os.path.splitext(fn)[0]
            ticker=re.split(r"[_\- ]+", base)[0].upper()
            if not re.fullmatch(r"[A-Z][A-Z0-9\.]{0,8}", ticker):
                continue
            if ticker in CORE_ASSETS:
                continue
            out.setdefault(ticker, []).append(os.path.join(root, fn))
    return out

def _load_ticker_df(path):
    df=_read_csv_flex(path)
    df=_norm_cols(df)
    tcol=_find_time_col(df)
    ccol=_find_close_col(df)
    if tcol is None or ccol is None:
        return None
    df["timestamp"]=pd.to_datetime(df[tcol], errors="coerce")
    df["close"]=pd.to_numeric(df[ccol], errors="coerce")
    df=df[df["timestamp"].notna() & df["close"].notna()].copy().sort_values("timestamp")
    if len(df)==0:
        return None
    df["date"]=df["timestamp"].dt.floor("D")
    daily=df.groupby("date", as_index=False)["close"].last().copy()
    daily["ret1d"]=daily["close"].pct_change()
    daily["weekday"]=daily["date"].dt.weekday
    return daily

def _registry():
    discovered=_discover_aau_files()
    reg={}
    for ticker, paths in discovered.items():
        chosen=None
        chosen_df=None
        for p in sorted(paths):
            try:
                df=_load_ticker_df(p)
                if df is not None and len(df) >= 20:
                    chosen=p
                    chosen_df=df
                    break
            except Exception:
                pass
        if chosen is not None:
            reg[ticker]={"path":chosen,"df":chosen_df,"file_name":os.path.basename(chosen)}
    return reg

def _detect_tickers(question, reg):
    nq=_nrm(question)
    found=[]
    for ticker in reg.keys():
        if re.search(rf"(?<![a-z0-9]){re.escape(ticker.lower())}(?![a-z0-9])", nq):
            found.append(ticker)
    return list(dict.fromkeys(found))

def _parse_month_year(nq):
    month=None
    for k,v in MONTHS_FR.items():
        if re.search(rf"(?<![a-z0-9]){re.escape(_nrm(k))}(?![a-z0-9])", nq):
            month=v
            break
    m=re.search(r"\b(20\d{2})\b", nq)
    year=int(m.group(1)) if m else None
    return month, year

def _parse_weekday(nq):
    for k,v in WEEKDAYS_FR.items():
        if re.search(rf"(?<![a-z0-9]){re.escape(_nrm(k))}(?![a-z0-9])", nq):
            return v, k
    return None, None

def _parse_horizon_days(nq):
    m=re.search(r"sur\s+(\d+)\s+(jour|jours|semaine|semaines|mois|month|months|an|ans|annee|annees|year|years)", nq)
    if not m:
        return 1, "1 jour"
    n=int(m.group(1))
    u=m.group(2)
    if u.startswith("jour"):
        return n, f"{n} jour" if n==1 else f"{n} jours"
    if u.startswith("semaine"):
        return n*5, f"{n} semaine" if n==1 else f"{n} semaines"
    if u.startswith("mois") or u.startswith("month"):
        return n*21, f"{n} mois"
    return n*252, f"{n} an" if n==1 else f"{n} ans"

def _parse_threshold_fragment(nq, ticker):
    m=re.search(rf"{ticker.lower()}[^\n]*?(?:superieur a|au dessus de|>|plus de)\s*(-?\d+(?:\.\d+)?)\s*(%)?", nq)
    if m:
        return ("gt_pct" if m.group(2) else "gt", float(m.group(1)))
    m=re.search(rf"{ticker.lower()}[^\n]*?(?:inferieur a|en dessous de|<)\s*(-?\d+(?:\.\d+)?)\s*(%)?", nq)
    if m:
        return ("lt_pct" if m.group(2) else "lt", float(m.group(1)))
    m=re.search(rf"{ticker.lower()}[^\n]*?(?:entre)\s*(-?\d+(?:\.\d+)?)\s*(%)?\s*(?:et|a|à)\s*(-?\d+(?:\.\d+)?)\s*(%)?", nq)
    if m:
        pct=bool(m.group(2) or m.group(4))
        return ("between_pct" if pct else "between", float(m.group(1)), float(m.group(3)))
    m=re.search(rf"{ticker.lower()}[^\n]*?en hausse", nq)
    if m:
        return ("up", None)
    m=re.search(rf"{ticker.lower()}[^\n]*?en baisse", nq)
    if m:
        return ("down", None)
    return None

def _filter_df(df, month=None, year=None, weekday=None):
    out=df.copy()
    if year is not None:
        out=out[out["date"].dt.year==year]
    if month is not None:
        out=out[out["date"].dt.month==month]
    if weekday is not None:
        out=out[out["weekday"]==weekday]
    return out

def _apply_condition(base, cond_df, cond):
    work=base.merge(cond_df[["date","close","ret1d"]].rename(columns={"close":"cond_close","ret1d":"cond_ret1d"}), on="date", how="inner")
    if cond is None:
        return work
    kind=cond[0]
    if kind=="gt":
        return work[work["cond_close"] > cond[1]]
    if kind=="lt":
        return work[work["cond_close"] < cond[1]]
    if kind=="between":
        lo=min(cond[1],cond[2]); hi=max(cond[1],cond[2])
        return work[(work["cond_close"] >= lo) & (work["cond_close"] <= hi)]
    if kind=="gt_pct":
        return work[(work["cond_ret1d"]*100.0) > cond[1]]
    if kind=="lt_pct":
        return work[(work["cond_ret1d"]*100.0) < cond[1]]
    if kind=="between_pct":
        lo=min(cond[1],cond[2]); hi=max(cond[1],cond[2])
        return work[((work["cond_ret1d"]*100.0) >= lo) & ((work["cond_ret1d"]*100.0) <= hi)]
    if kind=="up":
        return work[work["cond_ret1d"] > 0]
    if kind=="down":
        return work[work["cond_ret1d"] < 0]
    return work

def _question_type(nq):
    if "comparaison" in nq or " vs " in nq:
        return "comparison"
    if "taux positif" in nq or "winrate" in nq:
        return "taux_positif"
    if "meilleure variation" in nq:
        return "best"
    if "pire variation" in nq:
        return "worst"
    if "combien de fois" in nq or "nombre de fois" in nq or "nombre de cas" in nq:
        return "count"
    return "performance"

def can_handle(question):
    reg=_registry()
    if not reg:
        return False
    tickers=_detect_tickers(question, reg)
    return len(tickers) >= 1

def run(question, preview_rows=20):
    nq=_nrm(question)
    reg=_registry()
    tickers=_detect_tickers(question, reg)
    if not tickers:
        return {"status":"NO_AAU_TICKER","engine":"aau_research_engine","answer":"Aucun ticker AAU détecté."}

    qtype=_question_type(nq)
    month, year=_parse_month_year(nq)
    weekday, weekday_label = _parse_weekday(nq)
    horizon_days, horizon_label=_parse_horizon_days(nq)

    if qtype=="comparison" and len(tickers) >= 2:
        a1, a2 = tickers[0], tickers[1]
        d1=_filter_df(reg[a1]["df"], month, year, weekday).copy()
        d2=_filter_df(reg[a2]["df"], month, year, weekday).copy()
        d1["ret_h"]=d1["close"].shift(-horizon_days)/d1["close"] - 1.0
        d2["ret_h"]=d2["close"].shift(-horizon_days)/d2["close"] - 1.0
        merged=d1[["date","ret_h"]].rename(columns={"ret_h":"ret1"}).merge(
            d2[["date","ret_h"]].rename(columns={"ret_h":"ret2"}), on="date", how="inner"
        )
        if len(tickers) >= 3:
            for cond_t in tickers[2:]:
                cond=_parse_threshold_fragment(nq, cond_t)
                merged=_apply_condition(merged, _filter_df(reg[cond_t]["df"], month, year, weekday), cond)

        merged=merged[merged["ret1"].notna() & merged["ret2"].notna()].copy()
        if len(merged)==0:
            return {
                "status":"NO_RESULT",
                "engine":"aau_research_engine",
                "answer_short":"Aucun cas",
                "answer_long":"Aucun cas exploitable n'a été trouvé pour cette comparaison AAU.",
                "comparison_assets":[a1,a2],
                "source_file_names":[reg[a1]["file_name"], reg[a2]["file_name"]],
                "preview":[]
            }
        m1=float(merged["ret1"].mean()); m2=float(merged["ret2"].mean())
        leader=a1 if m1>=m2 else a2
        lagger=a2 if leader==a1 else a1
        diff=max(m1,m2)-min(m1,m2)
        count=int(len(merged))
        return {
            "status":"OK",
            "engine":"aau_research_engine",
            "mode":"comparison",
            "metric":"moyenne_variation",
            "value":leader,
            "target_asset":leader,
            "comparison_assets":[a1,a2],
            "answer_short":f"{leader} surperformant de {diff*100:+.2f}%".replace(".",","),
            "answer_long":f"Cela est arrivé {count} fois. En moyenne dans ces conditions, le {leader} fait {(max(m1,m2)*100):.2f}% et le {lagger} {(min(m1,m2)*100):.2f}%.".replace(".",","),
            "source_file_names":[reg[a1]["file_name"], reg[a2]["file_name"]],
            "preview":merged.assign(ret1_pct=(merged["ret1"]*100).round(2), ret2_pct=(merged["ret2"]*100).round(2))[["date","ret1_pct","ret2_pct"]].head(preview_rows).to_dict(orient="records"),
            "stats":{"count":count,"horizon_label":horizon_label,"weekday_label":weekday_label},
        }

    target=tickers[0]
    df=_filter_df(reg[target]["df"], month, year, weekday).copy()
    df["ret_h"]=df["close"].shift(-horizon_days)/df["close"] - 1.0

    cond_tickers=tickers[1:]
    work=df.copy()
    source_files=[reg[target]["file_name"]]
    for ct in cond_tickers:
        cond=_parse_threshold_fragment(nq, ct)
        work=_apply_condition(work, _filter_df(reg[ct]["df"], month, year, weekday), cond)
        source_files.append(reg[ct]["file_name"])

    if qtype=="count":
        cond=_parse_threshold_fragment(nq, target)
        work=work[work["ret1d"].notna()].copy()
        if cond:
            kind=cond[0]
            if kind=="gt_pct":
                work=work[(work["ret1d"]*100.0) > cond[1]]
                cond_txt=f"au-dessus de {cond[1]:g}%"
            elif kind=="lt_pct":
                work=work[(work["ret1d"]*100.0) < cond[1]]
                cond_txt=f"en dessous de {cond[1]:g}%"
            elif kind=="between_pct":
                lo=min(cond[1],cond[2]); hi=max(cond[1],cond[2])
                work=work[((work["ret1d"]*100.0) >= lo) & ((work["ret1d"]*100.0) <= hi)]
                cond_txt=f"entre {lo:g}% et {hi:g}%"
            elif kind=="gt":
                work=work[work["close"] > cond[1]]
                cond_txt=f"au-dessus de {cond[1]:g}"
            elif kind=="lt":
                work=work[work["close"] < cond[1]]
                cond_txt=f"en dessous de {cond[1]:g}"
            elif kind=="between":
                lo=min(cond[1],cond[2]); hi=max(cond[1],cond[2])
                work=work[(work["close"] >= lo) & (work["close"] <= hi)]
                cond_txt=f"entre {lo:g} et {hi:g}"
            else:
                cond_txt="dans le filtre demandé"
        else:
            cond_txt="dans le filtre demandé"

        count=int(len(work))
        months={1:"janvier",2:"février",3:"mars",4:"avril",5:"mai",6:"juin",7:"juillet",8:"août",9:"septembre",10:"octobre",11:"novembre",12:"décembre"}
        ctx=[]
        if weekday_label is not None:
            ctx.append(f"les {weekday_label}")
        if month and year:
            ctx.append(f"en {months.get(month,month)} {year}")
        elif month:
            ctx.append(f"en {months.get(month,month)}")
        elif year:
            ctx.append(f"en {year}")
        prefix=(" ".join(ctx)).strip()

        prev=work.copy()
        if "ret1d" in prev.columns:
            prev["ret1d_pct"]=(prev["ret1d"]*100.0).round(2)
        keep=[c for c in ["date","close","ret1d_pct"] if c in prev.columns]
        return {
            "status":"OK",
            "engine":"aau_research_engine",
            "mode":"count",
            "metric":"count",
            "value":count,
            "target_asset":target,
            "target_dataset":reg[target]["file_name"],
            "answer_short":f"{count} fois",
            "answer_long":f"{target} a été {count} fois {cond_txt}" + (f" {prefix}" if prefix else "") + ".",
            "source_file_names":source_files,
            "preview":prev[keep].head(preview_rows).to_dict(orient="records"),
            "display_context":{"month":month,"year":year,"weekday_label":weekday_label}
        }

    work=work[work["ret_h"].notna()].copy()
    if len(work)==0:
        return {
            "status":"NO_RESULT",
            "engine":"aau_research_engine",
            "answer_short":"Aucun cas",
            "answer_long":"Aucun cas exploitable n'a été trouvé pour cette question AAU.",
            "target_asset":target,
            "source_file_names":source_files,
            "preview":[]
        }

    count=int(len(work))
    mean=float(work["ret_h"].mean())
    taux=float((work["ret_h"]>0).mean())
    best=float(work["ret_h"].max())
    worst=float(work["ret_h"].min())

    if qtype=="taux_positif":
        short=f"{taux*100:.2f}%".replace(".",",")
        long_=f"Dans {taux*100:.2f}% des cas, le {target} est positif sur {horizon_label}.".replace(".",",")
    elif qtype=="best":
        short=f"{best*100:.2f}%".replace(".",",")
        long_=f"La meilleure variation observée du {target} sur {horizon_label} est de {best*100:.2f}%.".replace(".",",")
    elif qtype=="worst":
        short=f"{worst*100:.2f}%".replace(".",",")
        long_=f"La pire variation observée du {target} sur {horizon_label} est de {worst*100:.2f}%.".replace(".",",")
    else:
        short=f"{mean*100:.2f}%".replace(".",",")
        long_=f"Il y a eu {count} cas. En moyenne dans ces conditions, le {target} varie de {mean*100:.2f}% sur {horizon_label}.".replace(".",",")

    prev=work.copy()
    prev["ret_h_pct"]=(prev["ret_h"]*100.0).round(2)
    keep=[c for c in ["date","close","ret_h_pct"] if c in prev.columns]

    return {
        "status":"OK",
        "engine":"aau_research_engine",
        "mode":"single_asset",
        "metric":"aau_metric",
        "value":count if qtype=="performance" else (taux if qtype=="taux_positif" else mean),
        "target_asset":target,
        "target_dataset":reg[target]["file_name"],
        "answer_short":short,
        "answer_long":long_,
        "source_file_names":source_files,
        "preview":prev[keep].head(preview_rows).to_dict(orient="records"),
        "stats":{"count":count,"moyenne_variation":mean,"taux_positif":taux,"meilleure_variation":best,"pire_variation":worst,"horizon_label":horizon_label,"weekday_label":weekday_label}
    }
