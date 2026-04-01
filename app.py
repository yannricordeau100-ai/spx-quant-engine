# SPX_QUANT_ENGINE | v2.0
import re
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

st.set_page_config(page_title="SPX Quant Engine", layout="wide")

VERSION = "v2.0"

BASE_DIR = Path(__file__).resolve().parent
DATA_ROOT = BASE_DIR / "data" / "live_selected"
APP_RUNTIME_DIR = BASE_DIR / "app_runtime"
APP_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
FEEDBACK_CSV = APP_RUNTIME_DIR / "question_feedback.csv"
DEFAULT_Q = "SPX quand VIX1D/VIX > 1.2"

MONTHS_FR = {1:"janvier",2:"février",3:"mars",4:"avril",5:"mai",6:"juin",
             7:"juillet",8:"août",9:"septembre",10:"octobre",11:"novembre",12:"décembre"}
WEEKDAYS_FR = {0:"lundi",1:"mardi",2:"mercredi",3:"jeudi",4:"vendredi",5:"samedi",6:"dimanche"}

def norm(t: Any) -> str:
    t = str(t).strip().lower()
    for k,v in {"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","î":"i","ï":"i",
                "ô":"o","ù":"u","û":"u","ü":"u","ç":"c","\u2018":"'","\u2019":"'",
                "≥":">=","≤":"<="}.items():
        t = t.replace(k,v)
    return re.sub(r"\s+"," ",t)

def fmt_date_fr(d):
    try:
        d = pd.to_datetime(d).date()
        return f"{WEEKDAYS_FR[d.weekday()]} {d.day} {MONTHS_FR[d.month]} {d.year}"
    except: return str(d)

def fmt_week_fr(yw):
    try:
        y,w = yw.split("-W"); y,w = int(y),int(w)
        s = date.fromisocalendar(y,w,1); e = date.fromisocalendar(y,w,5)
        l = f"du {WEEKDAYS_FR[s.weekday()]} {s.day} {MONTHS_FR[s.month]} {s.year} au {WEEKDAYS_FR[e.weekday()]} {e.day} {MONTHS_FR[e.month]} {e.year}"
        return l[:1].upper()+l[1:]
    except: return yw

def read_csv_any(path: Path):
    try:
        df = pd.read_csv(path, sep=None, engine="python")
        if df is not None and len(df.columns)>1: return df,"auto"
    except: pass
    for sep,label in [(";","sc"),(",","co"),("\t","tab"),("|","pipe")]:
        try:
            df = pd.read_csv(path,sep=sep)
            if df is not None and len(df.columns)>1: return df,label
        except: continue
    raise ValueError(f"Lecture impossible: {path}")

def guess_col(cols, keywords, exclude=None):
    exclude = exclude or []
    lower = {str(c).lower():c for c in cols}
    for k in keywords:
        if k in lower: return lower[k]
    for c in cols:
        cl = str(c).lower()
        if any(k in cl for k in keywords) and not any(e in cl for e in exclude):
            return c
    return None

def detect_freq(fn):
    n = norm(fn)
    for tag,freq in [("1min","1min"),("5min","5min"),("15min","15min"),("30min","30min"),
                     ("4h","4h"),("1hour","1h"),("hour","1h"),("daily","daily")]:
        if tag in n: return freq
    if n.endswith("_daily.csv") or ", 1d" in n: return "daily"
    return "unknown"

def detect_kind(fn):
    n = norm(fn)
    if "option_chain" in n: return "options"
    if "average_range" in n or "average range" in n: return "move_average"
    if "correlation" in n: return "correlation"
    if "vix1d_vix_ratio" in n or ("vix1d" in n and "ratio" in n): return "ratio"
    if "put_call" in n or "put call" in n: return "ratio"
    return "standard"

@st.cache_data(show_spinner=False)
def scan_catalog():
    rows = []
    if not DATA_ROOT.exists(): return pd.DataFrame()
    for f in sorted(DATA_ROOT.rglob("*.csv")):
        if "__macosx" in norm(str(f)): continue
        rows.append({"file":f.name,"path":str(f),"freq":detect_freq(f.name),"kind":detect_kind(f.name)})
    return pd.DataFrame(rows)

CATALOG = scan_catalog()

def get_file(fn):
    if CATALOG.empty: return None
    h = CATALOG[CATALOG["file"]==fn]
    return h.iloc[0].to_dict() if not h.empty else None

def find_ratio():
    h = get_file("VIX1D_VIX_ratio_daily.csv")
    if h: return h
    tmp = CATALOG[(CATALOG["kind"]=="ratio")&(CATALOG["freq"]=="daily")]
    tmp = tmp[tmp["file"].map(norm).str.contains("vix1d")]
    return tmp.iloc[0].to_dict() if not tmp.empty else None

@st.cache_data(show_spinner=False)
def load_value(path_str):
    path = Path(path_str)
    df,_ = read_csv_any(path)
    tc = guess_col(df.columns,["time","datetime","date","timestamp"])
    vc = guess_col(df.columns,["ratio","close","open","value","last"],exclude=["time","date","timestamp"])
    if tc is None or vc is None: raise Exception(f"Colonnes introuvables dans {path.name}")
    df[tc] = pd.to_datetime(df[tc],errors="coerce")
    df[vc] = pd.to_numeric(df[vc],errors="coerce")
    df = df.dropna(subset=[tc,vc]).sort_values(tc)
    out = df[[tc,vc]].rename(columns={tc:"time",vc:"value"})
    out["date"] = out["time"].dt.date
    return out, path.name

@st.cache_data(show_spinner=False)
def load_price(fn):
    h = get_file(fn)
    if h is None: raise FileNotFoundError(f"ERREUR : {fn} introuvable.")
    path = Path(h["path"]); df,_ = read_csv_any(path)
    tc = guess_col(df.columns,["time","datetime","date","timestamp"])
    oc = guess_col(df.columns,["open"])
    cc = guess_col(df.columns,["close"])
    if None in (tc,oc,cc): raise Exception(f"Colonnes introuvables dans {fn}")
    df[tc]=pd.to_datetime(df[tc],errors="coerce")
    df[oc]=pd.to_numeric(df[oc],errors="coerce")
    df[cc]=pd.to_numeric(df[cc],errors="coerce")
    df=df.dropna(subset=[tc,oc,cc]).sort_values(tc)
    out=df[[tc,oc,cc]].rename(columns={tc:"time",oc:"open",cc:"close"})
    out["date"]=out["time"].dt.date
    out["week"]=out["time"].dt.strftime("%G-W%V")
    return out, fn

# ── Dictionnaires actifs ─────────────────────────────────────────────────────
PRICE_ASSETS = {
    "spx":"SPX_daily.csv","spy":"SPY_daily.csv",
    "qqq":"QQQ_daily.csv","iwm":"IWM_daily.csv",
    "aapl":"AAPL.csv","aaoi":"AAOI.csv",
}
COND_ORDER = ["vix1d/vix","vix1d vix","vvix","vix3m","vix9d","vix6m",
              "skew","dxy","gold","nikkei","dax","ftse",
              "spx put call","qqq put call","spy put call","iwm put call",
              "vix put call","equity put call",
              "us 10y","us 2y","yield curve","vix spx open",
              "advance decline","vix"]
COND_FILES = {
    "vix1d/vix":None,"vix1d vix":None,
    "vix":"VIX_daily.csv","vvix":"VVIX_daily.csv",
    "vix3m":"VIX3M_daily.csv","vix9d":"VIX9D_daily.csv","vix6m":"VIX6M_daily.csv",
    "skew":"SKEW_INDEX_daily.csv","dxy":"DXY_daily.csv",
    "gold":"Gold_daily.csv",
    "nikkei":"NIKKEI225_daily.csv","dax":"DAX40_daily.csv","ftse":"FTSE100_daily.csv",
    "spx put call":"SPX_Put_Call_Ratio_daily.csv",
    "qqq put call":"QQQ_Put_Call_Ratio_daily.csv",
    "spy put call":"SPY_Put_Call_Ratio_daily.csv",
    "iwm put call":"IWM_Put_Call_Ratio_daily.csv",
    "vix put call":"VIX_Put_Call_Ratio_daily.csv",
    "equity put call":"Equity_Put_Call_Ratio_daily.csv",
    "us 10y":"US_10_years_bonds_daily.csv",
    "us 2y":"OANDA_USB02YUSD, 1D.csv",
    "yield curve":"Yield_Curve_Spread_10Y_2Y.csv",
    "vix spx open":"VIX_SPX_OPEN_daily.csv",
    "advance decline":"advance_decline_ratio_net_ratio_put_call_daily.csv",
}

def extract_threshold(q):
    qn = norm(q)
    for p in [r"(>=)\s*(\d+(?:[.,]\d+)?)",r"(<=)\s*(\d+(?:[.,]\d+)?)",
              r"(>)\s*(\d+(?:[.,]\d+)?)",r"(<)\s*(\d+(?:[.,]\d+)?)"]:
        m = re.search(p,qn)
        if m: return m.group(1),float(m.group(2).replace(",","."))
    return None,None

def parse_q(q):
    qn = norm(q)
    subject = next((a for a in PRICE_ASSETS if a in qn),None)
    cond = next((a for a in COND_ORDER if a in qn),None)
    op,thr = extract_threshold(q)
    if cond is None or op is None: return None
    return {"subject":subject,"cond":cond,"op":op,"thr":thr}

def load_cond(cond):
    if cond in ("vix1d/vix","vix1d vix"):
        h = find_ratio()
        if h is None: raise FileNotFoundError("VIX1D_VIX_ratio_daily.csv introuvable.")
        return load_value(h["path"])
    fn = COND_FILES.get(cond)
    if fn is None: raise FileNotFoundError(f"Actif '{cond}' non reconnu.")
    h = get_file(fn)
    if h is None: raise FileNotFoundError(f"ERREUR : {fn} introuvable.")
    return load_value(h["path"])

def apply_op(df,col,op,thr):
    m = {">":df[col]>thr,"<":df[col]<thr,">=":df[col]>=thr,"<=":df[col]<=thr}
    return df[m[op]].copy()

def build_answer(q):
    parsed = parse_q(q)
    if parsed is None:
        return "❓ Question non reconnue.\n\nExemples valides :\n- **SPX quand VIX > 18**\n- **SPX quand VIX1D/VIX > 1.2**\n- **QQQ quand DXY > 104**", pd.DataFrame()

    subject,cond,op,thr = parsed["subject"],parsed["cond"],parsed["op"],parsed["thr"]

    cond_df, cond_file = load_cond(cond)
    filtered = apply_op(cond_df,"value",op,thr)
    total = len(cond_df); match = len(filtered)
    pct = round(match/total*100,2) if total>0 else 0

    txt = (
        f"**{cond.upper()} {op} {thr} : {pct}% des jours**  "
        f"({match} jours sur {total})\n\n"
        f"Période : du {fmt_date_fr(cond_df['date'].min())} au {fmt_date_fr(cond_df['date'].max())}.\n\n"
        f"Dataset condition : `{cond_file}`"
    )

    if subject is None:
        exp = filtered[["date","value"]].copy()
        exp.columns=["Date",cond_file]
        exp["Date"]=pd.to_datetime(exp["Date"]).dt.strftime("%Y-%m-%d")
        exp.insert(0,"Question",""); 
        if not exp.empty: exp.iloc[0,0]=q
        return txt, exp

    # Charge actif sujet
    fn = PRICE_ASSETS[subject]
    subj_df, subj_file = load_price(fn)
    merged = pd.merge(
        filtered[["date","value"]].rename(columns={"value":"cond_value"}),
        subj_df[["date","open","close"]], on="date", how="inner"
    ).sort_values("date")

    if merged.empty:
        return txt+"\n\n⚠️ Aucune date commune entre les deux datasets.", pd.DataFrame()

    merged["var_pct"] = ((merged["close"]-merged["open"])/merged["open"])*100.0
    n = len(merged)
    avg = round(merged["var_pct"].mean(),3)
    pos = round((merged["var_pct"]>0).sum()/n*100,1)
    neg = round((merged["var_pct"]<0).sum()/n*100,1)
    best_day = merged.loc[merged["var_pct"].idxmax()]
    worst_day = merged.loc[merged["var_pct"].idxmin()]

    txt += (
        f"\n\n---\n**{subject.upper()} ces {n} jours :**\n\n"
        f"| Métrique | Valeur |\n|---|---|\n"
        f"| Variation moyenne open→close | **{avg:+.3f}%** |\n"
        f"| Jours haussiers | **{pos}%** |\n"
        f"| Jours baissiers | **{neg}%** |\n"
        f"| Meilleur jour | {fmt_date_fr(best_day['date'])} ({best_day['var_pct']:+.2f}%) |\n"
        f"| Pire jour | {fmt_date_fr(worst_day['date'])} ({worst_day['var_pct']:+.2f}%) |\n\n"
        f"Dataset : `{subj_file}`"
    )

    exp = merged[["date","open","close","var_pct","cond_value"]].copy()
    exp.columns=["Date",f"{subject.upper()} Open",f"{subject.upper()} Close",
                 f"{subject.upper()} Var%",f"{cond.upper()} Value"]
    exp["Date"]=pd.to_datetime(exp["Date"]).dt.strftime("%Y-%m-%d")
    exp.insert(0,"Question","")
    if not exp.empty: exp.iloc[0,0]=q
    return txt, exp

def is_weekly_drop(q):
    qn=norm(q)
    return ("spx" in qn and any(x in qn for x in ["baisse","baiss"])
            and bool(re.search(r"\d+(?:[.,]\d+)?\s*%",qn))
            and "fois" in qn and ("semaine" in qn or "week" in qn))

def build_weekly_drop(q):
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*%",q)
    thr = float(m.group(1).replace(",",".")) if m else 1.0
    m2 = re.search(r"(\d+)\s*fois",norm(q))
    times = int(m2.group(1)) if m2 else 2
    df,dname = load_price("SPX_daily.csv")
    df["ret"]=((df["close"]-df["open"])/df["open"])*100.0
    df["hit"]=df["ret"]<=-thr
    wk=df.groupby("week",as_index=False)["hit"].sum().rename(columns={"hit":"count"})
    wk=wk[wk["count"]>=times].copy()
    if wk.empty: return "Aucune semaine correspondant au critère.", pd.DataFrame()
    avg=round(len(wk)/df["week"].nunique()*52,2)
    wk["Semaine"]=wk["week"].apply(fmt_week_fr)
    wk=wk.sort_values("week",ascending=False)
    txt=f"**{fmt_week_fr(wk.iloc[0]['week'])}**\n\nEn moyenne **{avg} fois par an**.\n\nDataset : `{dname}`"
    exp=wk[["Semaine","week"]].copy(); exp.insert(0,"Question",""); exp.iloc[0,0]=q
    return txt, exp

def append_feedback(q,ans,kind,choice=""):
    row=pd.DataFrame([{"SOURCE":"SPX_QUANT_ENGINE_FEEDBACK_V1",
        "timestamp_utc":pd.Timestamp.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "question":q,"answer":ans,"type":kind,"choice":choice}])
    if FEEDBACK_CSV.exists(): row=pd.concat([pd.read_csv(FEEDBACK_CSV),row],ignore_index=True)
    row.to_csv(FEEDBACK_CSV,index=False)

# ── UI ───────────────────────────────────────────────────────────────────────
col_title, col_ver = st.columns([6,1])
with col_title: st.title("SPX Quant Engine")
with col_ver: st.markdown(f"<div style='text-align:right;color:#666;font-size:12px;padding-top:20px'>{VERSION}</div>", unsafe_allow_html=True)

q = st.text_input("Question", value=DEFAULT_Q, key="main_question")
resp_col, sig_col = st.columns([5.4,1.2])

with resp_col:
    st.markdown("## Réponse")
    txt,export_df = "",pd.DataFrame()
    try:
        if is_weekly_drop(q): txt,export_df = build_weekly_drop(q)
        else: txt,export_df = build_answer(q)
        st.markdown(txt)
    except Exception as e:
        st.error(f"Erreur : {e}")

with sig_col:
    st.markdown("## Signaler")
    if st.button("Réponse fausse"):
        append_feedback(q,txt,"false_answer"); st.success("Enregistré")
    if st.button("Question non gérée"):
        append_feedback(q,txt,"not_handled"); st.success("Enregistré")

if not export_df.empty:
    st.markdown("## Tableau résultat")
    st.dataframe(export_df,hide_index=True)
    st.download_button("Télécharger CSV",
        export_df.to_csv(index=False).encode("utf-8"),
        "resultat_spx_quant_engine.csv","text/csv")
