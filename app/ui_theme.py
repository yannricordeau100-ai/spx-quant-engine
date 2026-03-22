APP_VERSION="etape268f_inventory_ui_alias_patch"

def inject_global_css():
    import streamlit as st
    st.markdown(
        """
        <style>
        :root{--bg:#0b1220;--panel:#0f1727;--text:#e8eefc;--muted:#9eb0cf;--line:#22324d;}
        html,body,[class*='css']{background:var(--bg)!important;color:var(--text)!important;}
        .stApp{background:var(--bg)!important;color:var(--text)!important;}
        section[data-testid='stSidebar']{background:var(--panel)!important;}
        .block-container{max-width:1450px;padding-top:0.85rem;padding-bottom:1rem;}
        .stTextInput input,.stTextArea textarea{background:#0c1422!important;color:var(--text)!important;border:1px solid var(--line)!important;}
        .stButton>button{border:1px solid var(--line)!important;}
        div[data-testid="stMetric"]{background:transparent!important;border:none!important;}
        header[data-testid="stHeader"]{background:transparent!important;height:0!important;}
        div[data-testid="stToolbar"]{display:none!important;}
        div[data-testid="stStatusWidget"]{display:none!important;}
        #MainMenu{visibility:hidden!important;}
        footer{visibility:hidden!important;}
        .stDeployButton{display:none!important;}
        div[data-testid="stDecoration"]{display:none!important;}
        .tq-note{padding:12px 14px;border:1px solid var(--line);background:#0f1727;border-radius:12px;margin-bottom:10px;}
        </style>
        """,
        unsafe_allow_html=True
    )

def sign_class_from_answer(answer_short):
    s=str(answer_short or "")
    if "-" in s:
        return "neg"
    if "+" in s:
        return "pos"
    return "neu"
