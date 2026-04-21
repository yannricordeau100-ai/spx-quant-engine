"""
App Streamlit STANDALONE — classement dynamique BBE sur l'univers Beta>2.

Lancement :
    cd ~/spx-quant-engine/beta2_engulfing
    python3 -m streamlit run bbe_ranking_app.py

Toute la logique vit dans bbe_ranking_tab.render_bbe_ranking() — partagée
avec l'intégration dans app_local.py (zéro duplication).
"""
from __future__ import annotations
import streamlit as st
from bbe_ranking_tab import render_bbe_ranking

st.set_page_config(page_title="BBE Ranking — Beta>2 Universe", layout="wide")
st.title("📉 Bearish Engulfing — Classement interactif")

render_bbe_ranking(key_prefix="bbe_standalone")
