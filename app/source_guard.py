import os, json, hashlib, pandas as pd

def load_registry(path):
    if not os.path.exists(path):
        return {"datasets":{}}
    with open(path,"r",encoding="utf-8") as f:
        return json.load(f)

def load_fingerprints(path):
    if not os.path.exists(path):
        return []
    with open(path,"r",encoding="utf-8") as f:
        return json.load(f)

def get_dataset_meta(registry, key):
    return (registry.get("datasets",{}) or {}).get(key)

def ensure_dataset_exists(registry, key):
    meta=get_dataset_meta(registry,key)
    if meta is None:
        raise RuntimeError(f"CANONICAL_DATASET_MISSING::{key}")
    path=meta.get("path")
    if not path or not os.path.exists(path):
        raise RuntimeError(f"CANONICAL_DATASET_PATH_MISSING::{key}")
    return meta

def attach_source_block(result, metas):
    src=[]
    for m in metas:
        if not m:
            continue
        src.append({
            "dataset_key":m.get("dataset_key"),
            "file_name":m.get("file_name"),
            "path":m.get("path"),
            "rel_path":m.get("rel_path"),
            "freq_hint":m.get("freq_hint"),
        })
    result["source_files"]=src
    result["source_file_names"]=[x["file_name"] for x in src]
    result["source_paths"]=[x["path"] for x in src]
    return result
