import os, sys, json, csv
from datetime import datetime, timezone

APP_DIR=os.path.dirname(os.path.abspath(__file__))
if APP_DIR not in sys.path:
    sys.path.insert(0, APP_DIR)

import streamlit as st
import pandas as pd
from ui_theme import inject_global_css, APP_VERSION
from manual_stats_frontdoor import (
    execute_manual_stats,
    execute_advanced_pattern,
    get_registry_diagnostic,
    get_inventory_diagnostic,
    get_live_supported_files_rows,
    get_all_inventory_rows,
    get_live_derived_files_rows,
    get_runtime_health_snapshot,
    force_refresh_all,
    build_registry,
)

ROOT=r"/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
PROC=os.path.join(ROOT,"processed")
FEEDBACK_CSV=os.path.join(PROC,"manual_feedback.csv")
APP_STATE_JSON=os.path.join(PROC,"app_state.json")
HISTORY_JSON=os.path.join(PROC,"app_history.json")
os.makedirs(PROC,exist_ok=True)

st.set_page_config(page_title="TheBestQuant",page_icon="📈",layout="wide",initial_sidebar_state="expanded")
inject_global_css()

def now_utc():
    return datetime.now(timezone.utc).isoformat()

def load_json(path, default):
    try:
        with open(path,"r",encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, obj):
    with open(path,"w",encoding="utf-8") as f:
        json.dump(obj,f,ensure_ascii=False,indent=2)

def append_feedback(question, verdict, result, mode_label):
    header=["ts_utc","mode","question","verdict","answer_short","answer_long","target_asset","engine","status"]
    row={
        "ts_utc":now_utc(),
        "mode":mode_label,
        "question":question,
        "verdict":verdict,
        "answer_short":(((result or {}).get("result") or {}).get("answer_short")),
        "answer_long":(((result or {}).get("result") or {}).get("answer_long")),
        "target_asset":(((result or {}).get("result") or {}).get("target_asset")),
        "engine":(((result or {}).get("result") or {}).get("engine")),
        "status":(((result or {}).get("result") or {}).get("status")),
    }
    exists=os.path.isfile(FEEDBACK_CSV)
    with open(FEEDBACK_CSV,"a",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=header)
        if not exists:
            w.writeheader()
        w.writerow(row)

def append_history(question, out, mode_label):
    hist=load_json(HISTORY_JSON, [])
    item={
        "ts_utc": now_utc(),
        "mode": mode_label,
        "question": question,
        "ok": bool((out or {}).get("ok")),
        "error": (out or {}).get("error"),
        "detail": (out or {}).get("detail"),
        "answer_short": (((out or {}).get("result") or {}).get("answer_short")),
        "answer_long": (((out or {}).get("result") or {}).get("answer_long")),
        "target_asset": (((out or {}).get("result") or {}).get("target_asset")),
        "engine": (((out or {}).get("result") or {}).get("engine")),
        "stats": (((out or {}).get("result") or {}).get("stats") or {}),
        "preview": (((out or {}).get("result") or {}).get("preview") or [])[:10],
        "assets_compared": (((out or {}).get("result") or {}).get("assets_compared") or []),
    }
    hist=[item]+hist
    hist=hist[:100]
    save_json(HISTORY_JSON,hist)

def humanize_columns(df, out=None):
    out_result=((out or {}).get("result") or {})
    assets=out_result.get("assets_compared") or []
    a=assets[0] if len(assets)>0 else "Actif A"
    b=assets[1] if len(assets)>1 else "Actif B"
    mp={
        "ret1d_pct":"Variation 1 jour",
        "r1_pct":"Variation actif 1",
        "r2_pct":"Variation actif 2",
        "close_a":f"Clôture {a}",
        "close_b":f"Clôture {b}",
        "ret1d_a":f"Var. 1j {a}",
        "ret1d_b":f"Var. 1j {b}",
        "spread":"Spread",
        "ratio":"Ratio",
        "date":"Date",
        "close":"Clôture"
    }
    x=df.copy().fillna("")
    for c in x.columns:
        try:
            if str(x[c].dtype) != "object":
                x[c]=x[c].round(3)
        except Exception:
            pass
    x.columns=[mp.get(c,c.replace("_"," ").strip().capitalize()) for c in x.columns]
    return x

if "registry_warmed" not in st.session_state:
    try:
        build_registry(force=False)
    except Exception:
        pass
    st.session_state.registry_warmed=True

if "mode" not in st.session_state:
    st.session_state.mode="Basique"
if "last_question" not in st.session_state:
    st.session_state.last_question=""
if "last_out" not in st.session_state:
    st.session_state.last_out=None

with st.sidebar:
    st.title("Navigation")
    st.caption("Version active : " + APP_VERSION)

st.title("TheBestQuant")
st.caption("Recherche statistique quant, claire, rapide, dark mode uniquement")

mode_col, qright = st.columns([0.28, 1.72], gap="medium")
with mode_col:
    st.session_state.mode=st.radio("Mode",["Basique","Avance"],index=0 if st.session_state.mode=="Basique" else 1)
with qright:
    pass

left,right=st.columns([1.75,0.85],gap="large")

with left:
    with st.form("query_form", clear_on_submit=False):
        q=st.text_input("Question",value=st.session_state.last_question,placeholder="Ex: combien de fois AAPL a ete en hausse en aout 2024")
        b1,b2=st.columns([0.3,0.7])
        run=b1.form_submit_button("Executer",use_container_width=True)
        clear=b2.form_submit_button("Effacer",use_container_width=True)

    c1,c2,c3,c4=st.columns(4)
    if c1.button("AAPL hausse août 2024",use_container_width=True):
        st.session_state.last_question="combien de fois AAPL a ete en hausse en aout 2024"; st.rerun()
    if c2.button("AAPL vs SPY 1 mois",use_container_width=True):
        st.session_state.last_question="comparaison AAPL vs SPY sur 1 mois"; st.rerun()
    if c3.button("VIX < 20 en 2024",use_container_width=True):
        st.session_state.last_question="combien de fois le VIX a ete en dessous de 20 en 2024"; st.rerun()
    if c4.button("VIX1D < 20 en 2024",use_container_width=True):
        st.session_state.last_question="combien de fois le VIX1D a ete en dessous de 20 en 2024"; st.rerun()

    if clear:
        st.session_state.last_question=""
        st.session_state.last_out=None
        st.rerun()

    if run:
        st.session_state.last_question=q
        if st.session_state.mode=="Avance":
            st.session_state.last_out=execute_advanced_pattern(q)
        else:
            st.session_state.last_out=execute_manual_stats(q)
        append_history(q, st.session_state.last_out, st.session_state.mode)
        save_json(APP_STATE_JSON,{"last_question":q,"updated_at":now_utc(),"version":APP_VERSION,"mode":st.session_state.mode})

    out=st.session_state.last_out
    if out is not None:
        if out.get("ok"):
            result=out["result"]
            st.subheader("Réponse")
            st.write(result.get("answer_short",""))

            if st.session_state.mode=="Avance":
                if result.get("question_rewrite"):
                    st.markdown('<div class="tq-note"><strong>Question reformulée :</strong> ' + str(result.get("question_rewrite")) + '</div>', unsafe_allow_html=True)
                if result.get("key_message"):
                    st.markdown('<div class="tq-note"><strong>Lecture rapide :</strong> ' + str(result.get("key_message")) + '</div>', unsafe_allow_html=True)

                ainfo1,ainfo2,ainfo3=st.columns(3)
                ainfo1.metric("Type d’analyse", result.get("analysis_type","-"))
                ainfo2.metric("Actifs comparés", " / ".join(result.get("assets_compared") or []))
                ainfo3.metric("Période utilisée", result.get("period_used","-"))

                st.markdown("**Explication simple**")
                for line in result.get("explain_lines") or []:
                    st.write("- " + str(line))

                if result.get("extra_notes"):
                    st.markdown("**Points de vigilance**")
                    for line in result.get("extra_notes") or []:
                        st.write("- " + str(line))

                if result.get("examples"):
                    st.markdown("**Exemples concrets**")
                    for line in result.get("examples") or []:
                        st.write("- " + str(line))

                if result.get("opening_guidance"):
                    st.markdown("**Lecture pour l'ouverture du SPX**")
                    for line in result.get("opening_guidance") or []:
                        st.write("- " + str(line))
            else:
                st.caption(result.get("answer_long",""))

            stats=result.get("stats") or {}
            s1,s2,s3=st.columns(3)
            s1.metric("Fréquence",stats.get("frequency_label","-"))
            s2.metric("Analyse",stats.get("horizon_label","-"))
            s3.metric("Cas",stats.get("count","-"))

            if st.session_state.mode=="Avance":
                p1,p2,p3=st.columns(3)
                p1.metric("Pct 25 ratio", "" if stats.get("ratio_p25") is None else str(round(float(stats.get("ratio_p25")),3)).replace(".",","))
                p2.metric("Pct 50 ratio", "" if stats.get("ratio_p50") is None else str(round(float(stats.get("ratio_p50")),3)).replace(".",","))
                p3.metric("Pct 75 ratio", "" if stats.get("ratio_p75") is None else str(round(float(stats.get("ratio_p75")),3)).replace(".",","))

            preview=(result.get("preview") or [])[:20]
            if preview:
                st.dataframe(humanize_columns(pd.DataFrame(preview), out=out),use_container_width=True,hide_index=True)

            srcs=result.get("source_file_names") or []
            if srcs:
                st.caption("Sources : " + " / ".join([str(x) for x in srcs[:8]]))

            f1,f2,f3=st.columns(3)
            if f1.button("OK",key="fb_ok",use_container_width=True):
                append_feedback(st.session_state.last_question,"OK",out,st.session_state.mode)
                st.success("Réponse enregistrée : OK")
            if f2.button("FAUSSE",key="fb_false",use_container_width=True):
                append_feedback(st.session_state.last_question,"FAUSSE",out,st.session_state.mode)
                st.warning("Réponse enregistrée : FAUSSE")
            if f3.button("PASSE",key="fb_pass",use_container_width=True):
                append_feedback(st.session_state.last_question,"PASSE",out,st.session_state.mode)
                st.info("Réponse enregistrée : PASSE")
        else:
            st.error(str(out.get("error")) + ": " + str(out.get("detail","Erreur inconnue")))

    st.subheader("Historique des recherches")
    hist=load_json(HISTORY_JSON, [])
    if hist:
        for item in hist[:20]:
            label=(item.get("ts_utc","")[:19].replace("T"," ") + " | " + str(item.get("mode","-")) + " | " + str(item.get("question","-")))
            with st.expander(label, expanded=False):
                st.caption("Question : " + str(item.get("question","-")))
                if item.get("ok"):
                    st.write(item.get("answer_short") or "")
                    st.caption(item.get("answer_long") or "")
                    pv=item.get("preview") or []
                    if pv:
                        st.dataframe(humanize_columns(pd.DataFrame(pv), out={"result": item}),use_container_width=True,hide_index=True)
                else:
                    st.error(str(item.get("error")) + ": " + str(item.get("detail","Erreur inconnue")))
    else:
        st.caption("Aucun historique pour l'instant.")

with right:
    if st.button("Actualiser les CSV", use_container_width=True):
        force_refresh_all()
        st.success("Inventaire et registry actualisés.")
        st.rerun()

    state=load_json(APP_STATE_JSON,{})
    diag=get_registry_diagnostic() or {}
    invdiag=get_inventory_diagnostic() or {}

    st.subheader("Moteur")
    st.write("Tickers price-like chargés :", diag.get("valid_count",0))
    st.write("Tous les CSV inventoriés :", invdiag.get("total_csv_seen",0))
    st.write(
        "AAPL / SPY / VIX / VIX1D / VIX open SPX :",
        "oui" if diag.get("aapl_present") else "non", "/",
        "oui" if diag.get("spy_present") else "non", "/",
        "oui" if diag.get("vix_present") else "non", "/",
        "oui" if diag.get("vix1d_present") else "non", "/",
        "oui" if diag.get("vix_opening_present") else "non"
    )


    health=get_runtime_health_snapshot() or {}
    st.subheader("Santé runtime")
    st.write("Registry :", health.get("registry_valid_count",0))
    st.write("Inventaire :", health.get("inventory_count",0))
    st.write("VIX / VIX1D / VIX open :", "oui" if health.get("vix_present") else "non", "/", "oui" if health.get("vix1d_present") else "non", "/", "oui" if health.get("vix_opening_present") else "non")

st.subheader("Utilisation")
    st.caption("Mode Basique = recherche statistique simple.")
    st.caption("Mode Avance = recherche relation / corrélation / pattern entre actifs.")
    st.caption("Inventaire des CSV vérifié automatiquement avec cache.")
    st.caption("Point ou virgule acceptés pour les décimales.")

with st.sidebar:
    st.subheader("Informations app")
    st.caption("Version active : " + APP_VERSION)
    st.caption("Mode : " + st.session_state.mode)
    if state.get("updated_at"):
        st.caption("Dernière mise à jour : " + str(state.get("updated_at")))

    st.subheader("Registry")
    st.caption("Tickers price-like : " + str(diag.get("valid_count",0)))
    st.caption("Tous les CSV inventoriés : " + str(invdiag.get("total_csv_seen",0)))

    st.subheader("Fichiers price-like utilisés")
    rows=get_live_supported_files_rows() or []
    if rows:
        for r in rows:
            st.caption(str(r.get("file_name","-")))
    else:
        st.caption("Aucun fichier exploitable.")

    st.subheader("Tous les CSV du cloud inventoriés")
    invrows=get_all_inventory_rows() or []
    if invrows:
        for r in invrows[:120]:
            tag="price-like" if r.get("price_like") else "aux"
            st.caption(f"{r.get('file_name','-')} [{tag}]")
    else:
        st.caption("Aucun CSV inventorié.")
