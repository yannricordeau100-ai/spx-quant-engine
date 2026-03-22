import os

def find_project_root(start_path=None):
    base=os.path.abspath(start_path or os.path.dirname(__file__))
    candidates=[
        base,
        os.path.abspath(os.path.join(base,"..")),
        os.path.abspath(os.path.join(base,"../..")),
    ]
    env_root=os.environ.get("SPX_PROJECT_ROOT")
    if env_root:
        candidates=[env_root]+candidates
    for cand in candidates:
        app_dir=os.path.join(cand,"app_streamlit")
        processed=os.path.join(cand,"processed")
        if os.path.isdir(app_dir) and os.path.isdir(processed):
            return cand
    return os.path.abspath(os.path.join(base,".."))

def repo_mode_root():
    return find_project_root(os.path.dirname(__file__))

def raw_sources_root():
    return os.path.join(repo_mode_root(),"RAW_SOURCES")

def processed_root():
    return os.path.join(repo_mode_root(),"processed")
