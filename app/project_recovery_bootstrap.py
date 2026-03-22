import os, json, hashlib, importlib.util
import pandas as pd

def sha256_file(path):
    if not os.path.exists(path) or not os.path.isfile(path):
        return None
    h=hashlib.sha256()
    with open(path,"rb") as f:
        for chunk in iter(lambda:f.read(1024*1024),b""):
            h.update(chunk)
    return h.hexdigest()

def project_recovery_report(project_root):
    app=os.path.join(project_root,"app_streamlit")
    processed=os.path.join(project_root,"processed")
    raw_root=os.path.join(project_root,"RAW_SOURCES")
    continuity=os.path.join(project_root,"CONTINUITY_PACK")
    state=os.path.join(continuity,"state")

    report={}
    report["project_root"]=project_root
    report["exists"]=os.path.isdir(project_root)

    core_paths={
        "app":app,
        "processed":processed,
        "raw_root":raw_root,
        "continuity_pack":continuity,
        "bridge":os.path.join(app,"runtime_query_bridge.py"),
        "unified_engine":os.path.join(app,"unified_simple_query_engine.py"),
        "geometry_engine":os.path.join(app,"geometry_mixed_sequence_engine.py"),
        "cross_engine":os.path.join(app,"cross_asset_condition_engine.py"),
        "dynamic_registry_manager":os.path.join(app,"dynamic_asset_registry_manager.py"),
        "question_logger":os.path.join(app,"question_logger.py"),
        "capability_registry":os.path.join(state,"CAPABILITY_REGISTRY.json"),
        "known_limitations":os.path.join(state,"KNOWN_LIMITATIONS.json"),
        "latest_zip_json":os.path.join(state,"LATEST_PORTABLE_ZIP.json"),
        "latest_zip_txt":os.path.join(state,"LATEST_PORTABLE_ZIP.txt"),
        "step_history":os.path.join(state,"STEP_HISTORY.csv"),
    }
    report["core_checks"]=[{"name":k,"path":v,"exists":os.path.exists(v),"sha256":sha256_file(v)} for k,v in core_paths.items()]

    raw_folders=[
        "SPX","SPY","QQQ","IWM","VIX","VVIX","Bond US","Dollar DXY","Or+pétrole",
        "Autres Index","TICK","5j et 20 j Move Average","Corrélations SPX:SPY QQQ IWM","Calendar","Autres Actions Upload"
    ]
    report["raw_folders"]=[]
    for folder in raw_folders:
        p=os.path.join(raw_root,folder)
        files=sorted(os.listdir(p)) if os.path.isdir(p) else []
        report["raw_folders"].append({
            "folder":folder,
            "exists":os.path.isdir(p),
            "file_count":len([x for x in files if os.path.isfile(os.path.join(p,x))]) if os.path.isdir(p) else None,
            "files_head":files[:10] if os.path.isdir(p) else None,
        })

    report["latest_zip"]=None
    latest_zip_json=os.path.join(state,"LATEST_PORTABLE_ZIP.json")
    if os.path.exists(latest_zip_json):
        try:
            report["latest_zip"]=json.load(open(latest_zip_json,"r",encoding="utf-8"))
        except:
            report["latest_zip"]={"error":"LATEST_ZIP_JSON_UNREADABLE"}

    unanswered=os.path.join(processed,"ETAPE126_UNANSWERED_QUESTIONS.csv")
    report["unanswered_exists"]=os.path.exists(unanswered)
    report["unanswered_count"]=None
    if os.path.exists(unanswered):
        try:
            report["unanswered_count"]=int(pd.read_csv(unanswered).shape[0])
        except:
            report["unanswered_count"]="unreadable"

    return report

def load_bridge(project_root):
    bridge_path=os.path.join(project_root,"app_streamlit","runtime_query_bridge.py")
    if not os.path.exists(bridge_path):
        raise FileNotFoundError(bridge_path)
    spec=importlib.util.spec_from_file_location("runtime_query_bridge_recovery",bridge_path)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def recovery_smoke(project_root, questions=None, preview_rows=5):
    if questions is None:
        questions=[
            "combien de fois SPX a clôturé à plus de 2%",
            "combien de fois AAPL a clôturé à plus de 2%",
            "quelle est la performance de AAPL quand VIX > 20",
        ]
    bridge=load_bridge(project_root)
    app=os.path.join(project_root,"app_streamlit")
    rows=[]
    for q in questions:
        r=bridge.run_query(app,q,preview_rows=preview_rows)
        rows.append({
            "question":q,
            "ok":r.get("ok"),
            "engine":None if not r.get("ok") else r["result"].get("engine"),
            "status":None if not r.get("ok") else r["result"].get("status"),
            "type":None if not r.get("ok") else r["result"].get("answer_type"),
            "value":None if not r.get("ok") else r["result"].get("value"),
            "error":r.get("error"),
        })
    return rows
