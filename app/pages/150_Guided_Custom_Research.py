import os
import re
import hashlib
import importlib.util
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from human_feedback_engine import save_feedback, export_feedback_csv

APP_DIR=os.path.dirname(os.path.dirname(__file__))

def _load_module(path, name):
    spec=importlib.util.spec_from_file_location(name, path)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

bridge=_load_module(os.path.join(APP_DIR,"smart_query_executor.py"), "runtime_query_bridge_guided_243")

def _file_sha(path):
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for c in iter(lambda:f.read(1024*1024), b""):
            h.update(c)
    return h.hexdigest()[:12]

APP_VERSION="guided-" + _file_sha(__file__) + "-bridge-" + _file_sha(os.path.join(APP_DIR,"smart_query_executor.py"))

ASSETS=["SPX","SPY","QQQ","IWM","VIX","VVIX","VIX9D","DXY","GOLD"]
DEMANDE_MAP={
    "Combien de fois": "count",
    "Performance moyenne": "performance",
    "Taux positif": "taux_positif",
    "Meilleure variation": "meilleure_variation",
    "Pire variation": "pire_variation",
}
UNITS=["minutes","heures","jours","semaines","mois","années"]
MONTHS=["", "janvier","février","mars","avril","mai","juin","juillet","août","septembre","octobre","novembre","décembre"]

def _fmt_pct(x):
    try:
        return f"{float(x)*100:.2f}%".replace(".",",")
    except Exception:
        return None

def _split_sentences(txt):
    txt=str(txt or "").strip()
    if not txt:
        return []
    parts=re.split(r"(?<=[\.!?])\s+", txt)
    return [p.strip() for p in parts if p.strip()]

def _compose_count_detail(result):
    ctx=result.get("display_context") or {}
    asset=result.get("target_asset","")
    count=result.get("value")
    month=ctx.get("month")
    year=ctx.get("year")
    cond_type=ctx.get("cond_type")
    a=ctx.get("a")
    b=ctx.get("b")
    months={
        1:"janvier",2:"février",3:"mars",4:"avril",5:"mai",6:"juin",7:"juillet",
        8:"août",9:"septembre",10:"octobre",11:"novembre",12:"décembre"
    }
    ctx_prefix=""
    if month and year:
        ctx_prefix=f"En {months.get(month, month)} {year}, "
    elif month:
        ctx_prefix=f"En {months.get(month, month)}, "
    elif year:
        ctx_prefix=f"En {year}, "
    if cond_type=="between":
        lo=min(a,b); hi=max(a,b)
        return f"{ctx_prefix}le {asset} a été {int(count)} fois entre {lo:g} et {hi:g}."
    if cond_type=="lt":
        return f"{ctx_prefix}le {asset} a été {int(count)} fois en dessous de {a:g}."
    if cond_type=="gt":
        return f"{ctx_prefix}le {asset} a été {int(count)} fois au-dessus de {a:g}."
    return result.get("answer_long") or result.get("answer") or ""

def _compose_quant_detail(result):
    stats=result.get("stats") or {}
    asset=result.get("target_asset") or result.get("asset") or ""
    taux=stats.get("taux_positif")
    horizon=stats.get("horizon_label") or ""
    if taux is not None and asset and horizon:
        return f"Dans {_fmt_pct(taux)} des cas, le {asset} est positif sur {horizon}."
    return result.get("summary") or result.get("answer") or ""

def _result_text(result):
    engine=result.get("engine","")
    short=result.get("answer_short")
    long_=result.get("answer_long") or result.get("answer") or result.get("summary") or ""
    if engine=="count_threshold_engine":
        title=short or (f"{result.get('value')} fois" if result.get("value") is not None else "Réponse")
        detail=_compose_count_detail(result)
        return title, detail
    if engine in ["quant_research_engine","aau_research_engine"]:
        if short:
            return short, long_
        title=result.get("answer") or result.get("summary") or "Réponse"
        detail=_compose_quant_detail(result) if engine=="quant_research_engine" else long_
        return title, detail
    if short:
        return short, long_
    if result.get("answer"):
        return result.get("answer"), (result.get("summary") or "")
    if result.get("summary"):
        return result.get("summary"), ""
    return "Aucune réponse exploitable.", ""

def _result_table(result):
    preview=result.get("preview")
    ranking=result.get("ranking")
    if isinstance(preview, list) and len(preview) > 0:
        try:
            return pd.DataFrame(preview)
        except Exception:
            return None
    if isinstance(ranking, list) and len(ranking) > 0:
        try:
            return pd.DataFrame(ranking)
        except Exception:
            return None
    return None

def _logic_payload(result):
    keep={}
    for k in [
        "engine","status","mode","metric","metric_focus","value","target_asset","target_dataset",
        "source_file_names","conditions","ranges","horizon","stats","display_context"
    ]:
        if k in result:
            keep[k]=result.get(k)
    return keep

def _render_feedback(question, result):
    engine=result.get("engine","")
    _, detail=_result_text(result)
    stored_answer=detail or result.get("answer") or result.get("summary") or ""
    c1,c2,c3,c4=st.columns([1,1,1,2])
    if c1.button("OK", key=f"guided_fb_ok_{abs(hash((question,engine,APP_VERSION,'OK')))%10**10}"):
        save_feedback("OK", question, stored_answer, engine, APP_VERSION)
        st.success("Feedback OK enregistré.")
    if c2.button("FAUX", key=f"guided_fb_false_{abs(hash((question,engine,APP_VERSION,'FAUX')))%10**10}"):
        save_feedback("FAUX", question, stored_answer, engine, APP_VERSION)
        st.warning("Feedback FAUX enregistré.")
    if c3.button("PASSE", key=f"guided_fb_pass_{abs(hash((question,engine,APP_VERSION,'PASSE')))%10**10}"):
        save_feedback("PASSE", question, stored_answer, engine, APP_VERSION)
        st.info("Feedback PASSE enregistré.")
    csv_path=export_feedback_csv()
    if csv_path and os.path.exists(csv_path):
        with open(csv_path,"rb") as fb:
            c4.download_button(
                "Télécharger le CSV feedback",
                fb,
                file_name="feedback_export.csv",
                mime="text/csv",
                key=f"guided_fb_dl_{abs(hash((question,APP_VERSION)))%10**10}"
            )

def _render_paragraphs(detail):
    parts=_split_sentences(detail)
    if not parts:
        return
    html=[]
    for p in parts:
        html.append(f"<div class='et243-detail'>{p}</div>")
    st.markdown("".join(html), unsafe_allow_html=True)

def _render_result(question, out):
    if not isinstance(out, dict):
        st.error("Format de résultat invalide.")
        return
    if not out.get("ok"):
        st.error(out.get("error","Erreur inconnue"))
        return
    result=(out.get("result") or {})
    title, detail=_result_text(result)
    title_len=len(str(title or ""))
    title_class="et243-title-small" if title_len > 120 else "et243-title"

    st.markdown(f"<div class='{title_class}'>{title}</div>", unsafe_allow_html=True)
    if detail and detail.strip() and detail.strip()!=str(title).strip():
        _render_paragraphs(detail)

    _render_feedback(question, result)

    df=_result_table(result)
    if df is not None and len(df) > 0:
        st.markdown("<div class='et243-section-title'>Données utiles</div>", unsafe_allow_html=True)
        st.dataframe(df, use_container_width=True)

    logic=_logic_payload(result)
    if logic:
        with st.expander("Voir la logique", expanded=False):
            st.json(logic)

    with st.expander("Résultat brut", expanded=False):
        st.json(out)

def _build_question(demande_label, actif, h_n, h_unit, cond_asset, cond_mode, v1, v2, mois, annee):
    h_label=f"{int(h_n)} {h_unit}" if h_n else ""
    ctx=""
    if mois and annee:
        ctx=f" en {mois} {annee}"
    elif mois:
        ctx=f" en {mois}"
    elif annee:
        ctx=f" en {annee}"

    cond=""
    if cond_mode=="au-dessus de" and v1 not in ["", None]:
        cond=f" quand {cond_asset} > {v1}"
    elif cond_mode=="en dessous de" and v1 not in ["", None]:
        cond=f" quand {cond_asset} < {v1}"
    elif cond_mode=="entre" and v1 not in ["", None] and v2 not in ["", None]:
        cond=f" quand {cond_asset} entre {v1} et {v2}"
    elif cond_mode=="en hausse":
        cond=f" quand {cond_asset} en hausse"
    elif cond_mode=="en baisse":
        cond=f" quand {cond_asset} en baisse"

    if demande_label=="Combien de fois":
        if cond_mode=="au-dessus de":
            return f"combien de fois le {actif} a été au-dessus de {v1}{ctx}"
        if cond_mode=="en dessous de":
            return f"combien de fois le {actif} a été en dessous de {v1}{ctx}"
        if cond_mode=="entre":
            return f"combien de fois le {actif} a été entre {v1} et {v2}{ctx}"
        return f"combien de fois le {actif} a été observé{ctx}"

    if demande_label=="Performance moyenne":
        return f"performance du {actif} sur {h_label}{ctx}{cond}"
    if demande_label=="Taux positif":
        return f"taux positif {actif} sur {h_label}{ctx}{cond}"
    if demande_label=="Meilleure variation":
        return f"meilleure variation de {actif} sur {h_label}{ctx}{cond}"
    if demande_label=="Pire variation":
        return f"pire variation de {actif} sur {h_label}{ctx}{cond}"

    return f"performance du {actif} sur {h_label}{ctx}{cond}"

st.set_page_config(page_title="Construction assistée", layout="wide")
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


components.html("""
<script>
const doc = window.parent.document;
if (!window.parent.__et243ShortcutInstalled) {
  window.parent.__et243ShortcutInstalled = true;
  doc.addEventListener('keydown', function(e) {
    const isMac = navigator.platform.toUpperCase().indexOf('MAC') >= 0;
    const accel = isMac ? e.metaKey : e.ctrlKey;
    if (accel && e.key === 'Enter') {
      const buttons = Array.from(doc.querySelectorAll('button'));
      const btn = buttons.find(b => (b.innerText || '').trim() === 'Exécuter');
      if (btn) {
        e.preventDefault();
        btn.click();
      }
    }
  }, true);
}
</script>
""", height=0)

st.title("Construction assistée")

st.markdown("<div class='et243-section-title'>Paramètres simples</div>", unsafe_allow_html=True)
st.caption("Cette page t'aide à construire une question propre sans avoir à écrire toute la phrase manuellement.")

c1, c2 = st.columns(2)
with c1:
    actif = st.selectbox("Actif cible", ASSETS, index=0)
with c2:
    demande_label = st.selectbox("Type de demande", list(DEMANDE_MAP.keys()), index=0)

st.markdown("<div class='et243-section-title'>Horizon</div>", unsafe_allow_html=True)
h1, h2 = st.columns([1,1])
with h1:
    h_n = st.number_input("Nombre", min_value=1, max_value=5000, value=1, step=1)
with h2:
    h_unit = st.selectbox("Unité", UNITS, index=2)

st.markdown("<div class='et243-section-title'>Condition</div>", unsafe_allow_html=True)
k1, k2 = st.columns(2)
with k1:
    cond_asset = st.selectbox("Actif de condition", ASSETS, index=4 if "VIX" in ASSETS else 0)
with k2:
    cond_mode = st.selectbox("Condition", ["aucune","au-dessus de","en dessous de","entre","en hausse","en baisse"], index=0)

v1 = ""
v2 = ""
if cond_mode in ["au-dessus de","en dessous de"]:
    v1 = st.text_input("Valeur", value="")
elif cond_mode=="entre":
    a1, a2 = st.columns(2)
    with a1:
        v1 = st.text_input("Valeur basse", value="")
    with a2:
        v2 = st.text_input("Valeur haute", value="")

st.markdown("<div class='et243-section-title'>Contexte temps</div>", unsafe_allow_html=True)
t1, t2 = st.columns(2)
with t1:
    mois = st.selectbox("Mois", MONTHS, index=0)
with t2:
    annee = st.text_input("Année", value="")

question = _build_question(demande_label, actif, h_n, h_unit, cond_asset, cond_mode, v1, v2, mois, annee)
st.markdown("<div class='et243-section-title'>Question générée</div>", unsafe_allow_html=True)
st.markdown(f"<div class='et243-card'>{question}</div>", unsafe_allow_html=True)

run = st.button("Exécuter", type="primary")
if run:
    st.session_state["guided_last_question"] = question
    st.session_state["guided_last_out"] = bridge.run_query(APP_DIR, question, preview_rows=20)

if st.session_state.get("guided_last_out") is not None:
    _render_result(st.session_state.get("guided_last_question",""), st.session_state["guided_last_out"])

st.markdown(f"<div class=\'et245-footer\'>Version active : {APP_VERSION}</div>", unsafe_allow_html=True)
