
import pandas as pd
import json
import re

DATASET_MAP={
  "spx": "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Corre\u0301lations SPX:SPY QQQ IWM/SPX_QQQ_correlation_20days_daily.csv",
  "dxy": "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Dollar DXY/DXY_daily.csv",
  "vix": "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/VIX/VIX_daily.csv",
  "calendar": "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Calendar/calendar_events_daily.csv"
}

def load_dataset(key):

    if key not in DATASET_MAP:
        return None

    df=pd.read_csv(DATASET_MAP[key])

    if "date" in df.columns:
        df["date"]=pd.to_datetime(df["date"])

    return df


def query_cross_dataset(question):

    q=question.lower()

    spx=load_dataset("spx")
    vix=load_dataset("vix")
    dxy=load_dataset("dxy")

    if spx is None:
        return {"error":"SPX dataset missing"}

    if vix is not None:
        df=spx.merge(vix,on="date",suffixes=("_spx","_vix"))
    else:
        df=spx

    if dxy is not None:
        df=df.merge(dxy,on="date")

    m=re.search(r"vix.*>(\d+)",q)

    if m:

        thr=float(m.group(1))

        r=df[df["close_vix"]>thr]

        return {
            "engine":"cross_dataset_engine",
            "status":"OK",
            "value":len(r),
            "answer":f"{len(r)} jour(s) où VIX > {thr}",
        }

    m=re.search(r"dxy.*>(\d+)",q)

    if m:

        thr=float(m.group(1))

        r=df[df["close"]>thr]

        return {
            "engine":"cross_dataset_engine",
            "status":"OK",
            "value":len(r),
            "answer":f"{len(r)} jour(s) où DXY > {thr}",
        }

    return {
        "engine":"cross_dataset_engine",
        "status":"NO_MATCH"
    }
