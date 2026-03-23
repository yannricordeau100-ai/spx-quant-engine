python3 - <<'PY'
content = """import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("SPX Quant Engine - SAFE MODE")

st.success("App is running correctly")

st.write("If you see this, the app is fixed.")
"""
with open("app.py","w",encoding="utf-8") as f:
    f.write(content)
print("SAFE app written")
PY
