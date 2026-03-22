import os,json
from query_feedback_logger import load_feedback_log, export_feedback_log

APP_DIR=os.path.dirname(os.path.abspath(__file__))
LOG_PATH=os.path.join(APP_DIR,"data_runtime","query_feedback_log.csv")
EXPORT_PATH=os.path.join(os.path.dirname(APP_DIR),"processed","ETAPE100_QUERY_FEEDBACK_LOG.csv")

df=load_feedback_log(LOG_PATH)
exp=export_feedback_log(LOG_PATH,EXPORT_PATH)

print("=== ETAPE100_LOCALHOST_SMOKE ===")
print(json.dumps({
    "log_exists":os.path.exists(LOG_PATH),
    "rows":int(len(df)),
    "export_exists":os.path.exists(EXPORT_PATH),
    "export_rows":int(exp["rows"]),
},ensure_ascii=False,indent=2))
