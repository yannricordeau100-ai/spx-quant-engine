
import os
import time
import socket
import subprocess

APP = "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/app_streamlit"
MAIN_APP = "app.py"
PORT = 8501

STREAMLIT_LOG = "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/logs/streamlit.log"
PIDFILE = "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/logs/streamlit.pid"

def port_open():
    try:
        with socket.create_connection(("127.0.0.1", PORT), timeout=1.5):
            return True
    except:
        return False

def spawn():
    cmd = (
        'cd "' + APP + '" && '
        'nohup python -m streamlit run "' + MAIN_APP + '" '
        '--server.port ' + str(PORT) + ' '
        '--server.address 0.0.0.0 '
        '--server.headless true '
        '> "' + STREAMLIT_LOG + '" 2>&1 & echo $! > "' + PIDFILE + '"'
    )
    subprocess.run(["bash","-lc",cmd], check=False)

while True:
    if not port_open():
        spawn()
        time.sleep(8)
    time.sleep(15)
