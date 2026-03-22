#!/usr/bin/env bash
set -e
cd "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/app_streamlit"
nohup python -m streamlit run "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/app_streamlit/app.py" --server.port 8501 --server.address 0.0.0.0 --server.headless true > "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/logs/etape173_streamlit.log" 2>&1 &
echo $! > "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/logs/etape173_streamlit.pid"
