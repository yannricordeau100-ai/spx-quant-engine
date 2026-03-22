import os
import hashlib
import importlib.util
import streamlit as st

APP_DIR=os.path.dirname(os.path.dirname(__file__))

def _load_module(path, name):
    spec=importlib.util.spec_from_file_location(name, path)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _file_sha(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for c in iter(lambda:f.read(1024*1024), b""):
            h.update(c)
    return h.hexdigest()[:12]

APP_VERSION="page-" + _file_sha(__file__)

engine=_load_module(os.path.join(APP_DIR,"open_prediction_engine.py"), "open_prediction_engine_page_242")

st.set_page_config(page_title="Prédiction open", layout="wide")
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


st.title("Prédiction open")
st.markdown("<div class='et242-app-kicker'>Version manuelle provisoire : tu saisis uniquement les valeurs réellement utilisées par le moteur avant le branchement live.</div>", unsafe_allow_html=True)

fields=engine.required_inputs()
preopen=[f for f in fields if f["phase"]=="preopen"]
open_phase=[f for f in fields if f["phase"]=="open"]

if "open_pred_inputs" not in st.session_state:
    st.session_state["open_pred_inputs"]={}

def _input_value(fid):
    return st.session_state["open_pred_inputs"].get(fid,"")

st.markdown("<div class='et242-section-title'>Données avant ouverture</div>", unsafe_allow_html=True)
st.caption("Ces valeurs servent à produire la prédiction principale juste avant 9h30, ou très proche de l'ouverture.")

for f in preopen:
    st.session_state["open_pred_inputs"][f["id"]] = st.text_input(
        f["label"],
        value=_input_value(f["id"]),
        key=f"open_pred_{f['id']}",
        placeholder="Saisis la valeur numérique"
    )

st.markdown("<div class='et242-subsep'></div>", unsafe_allow_html=True)
st.markdown("<div class='et242-section-title'>Confirmation 9h30</div>", unsafe_allow_html=True)
st.caption("Ces valeurs sont optionnelles. Elles servent à affiner la prédiction juste à 9h30 si tu les as.")

for f in open_phase:
    st.session_state["open_pred_inputs"][f["id"]] = st.text_input(
        f["label"],
        value=_input_value(f["id"]),
        key=f"open_pred_{f['id']}",
        placeholder="Optionnel"
    )

run = st.button("Calculer la prédiction", type="primary")

if run:
    result=engine.run_prediction(st.session_state["open_pred_inputs"])
    st.session_state["open_pred_result"]=result

if st.session_state.get("open_pred_result") is not None:
    result=st.session_state["open_pred_result"]

    if result.get("status") != "OK":
        st.error(result.get("answer_long","Erreur"))
        if result.get("missing_inputs"):
            st.warning("Champs manquants : " + ", ".join(result.get("missing_inputs")))
    else:
        title=str(result.get("answer_short",""))
        title_class="et242-title-small" if len(title)>120 else "et242-title"
        st.markdown(f"<div class='{title_class}'>{title}</div>", unsafe_allow_html=True)

        long_txt=str(result.get("answer_long",""))
        for sentence in [s.strip() for s in long_txt.split(". ") if s.strip()]:
            st.markdown(f"<div class='et242-detail'>{sentence if sentence.endswith('.') else sentence + '.'}</div>", unsafe_allow_html=True)

        c1,c2,c3=st.columns(3)
        c1.metric("Probabilité hausse", f"{result.get('prob_up',0)*100:.1f}%")
        c2.metric("Probabilité baisse", f"{result.get('prob_down',0)*100:.1f}%")
        c3.metric("Amplitude attendue", f"{result.get('expected_points',0):.1f} pts")

        st.markdown(f"<div class='et242-card'><strong>Bucket attendu :</strong> {result.get('expected_bucket','n.d.')} points</div>", unsafe_allow_html=True)

        with st.expander("Valeurs utilisées", expanded=False):
            st.json(result.get("used_inputs",{}))

        with st.expander("Variables calculées", expanded=False):
            st.json(result.get("derived_features",{}))

        with st.expander("Logique du moteur", expanded=False):
            st.json(result.get("reasoning_points",[]))

st.markdown(f"<div class=\'et245-footer\'>Version active : {APP_VERSION}</div>", unsafe_allow_html=True)
