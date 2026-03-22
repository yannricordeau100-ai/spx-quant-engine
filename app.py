cd ~/spx-quant-engine
python3 - <<'PY'
content = """import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
st.title("TEST DRIVE CSV")

FILE_ID = "1HKmoeQEKaRZSOQarrU1Z6KI9Jv_cn6qd"

url = f"https://drive.google.com/uc?id={FILE_ID}"

st.write("URL:", url)

if st.button("Load test CSV"):
    try:
        df = pd.read_csv(url)
        st.success("Loaded from Drive")
        st.write("Shape:", df.shape)
        st.dataframe(df.head(50), width="stretch")
    except Exception as e:
        st.error(f"Error: {e}")
"""
with open("app.py","w") as f:
    f.write(content)
print("app test ready")
PY
