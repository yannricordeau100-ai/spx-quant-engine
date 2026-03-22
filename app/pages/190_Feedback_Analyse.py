import os, json, pandas as pd
import streamlit as st

ROOT="/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
PROC=os.path.join(ROOT,"processed")
ANALYSIS_JSON=os.path.join(PROC,"feedback_analysis_ready.json")
FALSE_CSV=os.path.join(PROC,"human_feedback_false.csv")
OK_CSV=os.path.join(PROC,"human_feedback_ok.csv")
PASS_CSV=os.path.join(PROC,"human_feedback_pass.csv")

st.set_page_config(page_title="Feedback analyse", layout="wide")
st.markdown("""
<style>
.et242-app-kicker{
    font-size:0.92rem;
    opacity:0.8;
    margin-top:-0.25rem;
    margin-bottom:1.0rem;
}
.et242-section-title{
    font-size:1.15rem;
    font-weight:700;
    margin-top:0.35rem;
    margin-bottom:0.55rem;
}
.et242-title{
    font-size:3.0rem;
    font-weight:800;
    line-height:1.15;
    margin-top:1.05rem;
    margin-bottom:0.85rem;
}
.et242-title-small{
    font-size:2.15rem;
    font-weight:800;
    line-height:1.20;
    margin-top:1.05rem;
    margin-bottom:0.85rem;
}
.et242-detail{
    font-size:1.03rem;
    line-height:1.62;
    margin-bottom:0.6rem;
}
.et242-card{
    padding:0.9rem 1rem;
    border:1px solid rgba(128,128,128,0.22);
    border-radius:14px;
    margin-bottom:0.8rem;
    background:rgba(127,127,127,0.05);
}
.et242-muted{
    opacity:0.78;
    font-size:0.95rem;
}
.et242-subsep{
    margin-top:0.9rem;
    margin-bottom:0.9rem;
}
.et243-app-kicker{
    font-size:0.92rem;
    opacity:0.8;
    margin-top:-0.25rem;
    margin-bottom:1.0rem;
}
.et243-section-title{
    font-size:1.15rem;
    font-weight:700;
    margin-top:0.35rem;
    margin-bottom:0.55rem;
}
.et243-title{
    font-size:3.0rem;
    font-weight:800;
    line-height:1.15;
    margin-top:1.05rem;
    margin-bottom:0.85rem;
}
.et243-title-small{
    font-size:2.15rem;
    font-weight:800;
    line-height:1.20;
    margin-top:1.05rem;
    margin-bottom:0.85rem;
}
.et243-detail{
    font-size:1.03rem;
    line-height:1.62;
    margin-bottom:0.6rem;
}
.et243-card{
    padding:0.9rem 1rem;
    border:1px solid rgba(128,128,128,0.22);
    border-radius:14px;
    margin-bottom:0.8rem;
    background:rgba(127,127,127,0.05);
}
.et243-muted{
    opacity:0.78;
    font-size:0.95rem;
}
.et245-footer{
    font-size:0.70rem;
    opacity:0.52;
    margin-top:2.5rem;
    margin-bottom:0.2rem;
    text-align:right;
}
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
.et242-app-kicker{
    font-size:0.92rem;
    opacity:0.8;
    margin-top:-0.25rem;
    margin-bottom:1.0rem;
}
.et242-section-title{
    font-size:1.15rem;
    font-weight:700;
    margin-top:0.35rem;
    margin-bottom:0.55rem;
}
.et242-title{
    font-size:3.0rem;
    font-weight:800;
    line-height:1.15;
    margin-top:1.05rem;
    margin-bottom:0.85rem;
}
.et242-title-small{
    font-size:2.15rem;
    font-weight:800;
    line-height:1.20;
    margin-top:1.05rem;
    margin-bottom:0.85rem;
}
.et242-detail{
    font-size:1.03rem;
    line-height:1.62;
    margin-bottom:0.6rem;
}
.et242-card{
    padding:0.9rem 1rem;
    border:1px solid rgba(128,128,128,0.22);
    border-radius:14px;
    margin-bottom:0.8rem;
    background:rgba(127,127,127,0.05);
}
.et242-muted{
    opacity:0.78;
    font-size:0.95rem;
}
.et242-subsep{
    margin-top:0.9rem;
    margin-bottom:0.9rem;
}
.et243-app-kicker{
    font-size:0.92rem;
    opacity:0.8;
    margin-top:-0.25rem;
    margin-bottom:1.0rem;
}
.et243-section-title{
    font-size:1.15rem;
    font-weight:700;
    margin-top:0.35rem;
    margin-bottom:0.55rem;
}
.et243-title{
    font-size:3.0rem;
    font-weight:800;
    line-height:1.15;
    margin-top:1.05rem;
    margin-bottom:0.85rem;
}
.et243-title-small{
    font-size:2.15rem;
    font-weight:800;
    line-height:1.20;
    margin-top:1.05rem;
    margin-bottom:0.85rem;
}
.et243-detail{
    font-size:1.03rem;
    line-height:1.62;
    margin-bottom:0.6rem;
}
.et243-card{
    padding:0.9rem 1rem;
    border:1px solid rgba(128,128,128,0.22);
    border-radius:14px;
    margin-bottom:0.8rem;
    background:rgba(127,127,127,0.05);
}
.et243-muted{
    opacity:0.78;
    font-size:0.95rem;
}
.et245-footer{
    font-size:0.74rem;
    opacity:0.58;
    margin-top:2.4rem;
    margin-bottom:0.25rem;
    text-align:right;
}
</style>
""", unsafe_allow_html=True)

st.title("Feedback analyse")
st.markdown("<div class='et242-app-kicker'>Vue rapide des réponses validées / invalidées pour préparer les prochaines corrections.</div>", unsafe_allow_html=True)

if not os.path.exists(ANALYSIS_JSON):
    st.warning("Le fichier d'analyse feedback n'existe pas encore.")
else:
    data=json.load(open(ANALYSIS_JSON,"r",encoding="utf-8"))
    c1,c2,c3,c4=st.columns(4)
    c1.metric("Total", data.get("n_total",0))
    c2.metric("OK", data.get("n_ok",0))
    c3.metric("FAUX", data.get("n_false",0))
    c4.metric("PASSE", data.get("n_pass",0))

    with st.expander("Résumé technique", expanded=False):
        st.json(data)

    st.markdown("<div class='et242-section-title'>Derniers FAUX</div>", unsafe_allow_html=True)
    if os.path.exists(FALSE_CSV):
        try:
            df_false=pd.read_csv(FALSE_CSV)
            if len(df_false)>0:
                st.dataframe(df_false, use_container_width=True)
            else:
                st.info("Aucun FAUX enregistré.")
        except Exception as e:
            st.error(f"Lecture FAUX impossible : {e}")
    else:
        st.info("Aucun FAUX enregistré.")

    st.markdown("<div class='et242-section-title'>Derniers OK</div>", unsafe_allow_html=True)
    if os.path.exists(OK_CSV):
        try:
            df_ok=pd.read_csv(OK_CSV)
            if len(df_ok)>0:
                st.dataframe(df_ok, use_container_width=True)
            else:
                st.info("Aucun OK enregistré.")
        except Exception as e:
            st.error(f"Lecture OK impossible : {e}")
    else:
        st.info("Aucun OK enregistré.")

    st.markdown("<div class='et242-section-title'>Derniers PASSE</div>", unsafe_allow_html=True)
    if os.path.exists(PASS_CSV):
        try:
            df_pass=pd.read_csv(PASS_CSV)
            if len(df_pass)>0:
                st.dataframe(df_pass, use_container_width=True)
            else:
                st.info("Aucun PASSE enregistré.")
        except Exception as e:
            st.error(f"Lecture PASSE impossible : {e}")
    else:
        st.info("Aucun PASSE enregistré.")

st.markdown(f"<div class=\'et245-footer\'>Version active : {APP_VERSION}</div>", unsafe_allow_html=True)
