
import pandas as pd
import json
import re

DATASET_MAP_PATH = "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/processed/ETAPE192_DATASET_INDEX.json"

with open(DATASET_MAP_PATH) as f:
    DATASETS = json.load(f)

DATASET_PATHS = {}

for d in DATASETS:

    name=d["file"].lower()

    if "spx_daily" in name:
        DATASET_PATHS["spx"]=d["path"]

    if "vix_daily" in name:
        DATASET_PATHS["vix"]=d["path"]

    if "dxy" in name:
        DATASET_PATHS["dxy"]=d["path"]

    if "gold" in name:
        DATASET_PATHS["gold"]=d["path"]


def load_df(path, label):

    df=pd.read_csv(path)

    if "date" in df.columns:
        df["timestamp"]=pd.to_datetime(df["date"])

    elif "time" in df.columns:
        df["timestamp"]=pd.to_datetime(df["time"])

    else:
        raise RuntimeError("NO_TIME_COLUMN")

    df=df.sort_values("timestamp")

    rename={}

    for c in df.columns:

        if c not in ["timestamp","date","time"]:

            rename[c]=f"{c}_{label}"

    df=df.rename(columns=rename)

    return df


def align_datasets(dfs):

    base=dfs[0]

    for d in dfs[1:]:

        base=pd.merge_asof(
            base,
            d,
            on="timestamp",
            direction="nearest"
        )

    return base


def parse_conditions(question):

    q=question.lower()

    conds=[]

    m=re.findall(r"vix\s*>\s*(\d+)",q)
    for v in m:
        conds.append(("vix_gt",float(v)))

    m=re.findall(r"dxy\s*>\s*(\d+)",q)
    for v in m:
        conds.append(("dxy_gt",float(v)))

    m=re.findall(r"gold\s*>\s*(\d+)",q)
    for v in m:
        conds.append(("gold_gt",float(v)))

    return conds


def run_query(question):

    dfs=[]

    if "spx" in DATASET_PATHS:
        dfs.append(load_df(DATASET_PATHS["spx"],"spx"))

    if "vix" in DATASET_PATHS:
        dfs.append(load_df(DATASET_PATHS["vix"],"vix"))

    if "dxy" in DATASET_PATHS:
        dfs.append(load_df(DATASET_PATHS["dxy"],"dxy"))

    if "gold" in DATASET_PATHS:
        dfs.append(load_df(DATASET_PATHS["gold"],"gold"))

    df=align_datasets(dfs)

    conds=parse_conditions(question)

    mask=pd.Series(True,index=df.index)

    for c,v in conds:

        if c=="vix_gt":
            mask&=(df["close_vix"]>v)

        if c=="dxy_gt":
            mask&=(df["close_dxy"]>v)

        if c=="gold_gt":
            mask&=(df["close_gold"]>v)

    r=df[mask]

    return {
        "engine":"multi_asset_multi_tf_engine",
        "status":"OK",
        "conditions":conds,
        "value":len(r),
        "answer":f"{len(r)} occurrences correspondant aux conditions"
    }
