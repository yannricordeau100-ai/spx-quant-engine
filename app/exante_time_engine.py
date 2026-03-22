from __future__ import annotations
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import pandas as pd
import re

# --------------------------------
# POLICY REGISTRY
# --------------------------------

TIMEZONE_REGISTRY = {
    "SPX":"America/New_York",
    "TICK":"America/New_York",
    "VIX1D":"America/New_York",
    "VIX_9H30":"America/New_York",
    "CALENDAR":"America/New_York",
    "GOLD":"Europe/Paris",
    "OIL":"Europe/Paris",
    "SPX_FUTURES":"Europe/Paris",
    "QQQ":"Europe/Paris",
    "IWM":"Europe/Paris",
    "SPY":"Europe/Paris",
    "DAX":"Europe/Berlin",
    "FTSE":"Europe/London",
    "NIKKEI":"Asia/Tokyo",
}

DST_REFERENCE_NEW_YORK = {"GOLD","OIL","SPX_FUTURES","QQQ","IWM","SPY"}

DEFAULT_SPX_OPEN = (9,30)  # New York
DEFAULT_QQQ_IWM_SPY_LOCAL_OPEN = (15,30)  # Paris representation of SPX-equivalent open in many contexts, not used blindly

# --------------------------------
# FREQUENCY
# --------------------------------

def frequency_to_minutes(freq):
    if freq is None:
        return None
    if isinstance(freq,(int,float)):
        return int(freq)
    s=str(freq).strip().lower().replace(" ","")
    mapping={
        "1m":1,"1min":1,"1minute":1,
        "5m":5,"5min":5,"5minute":5,
        "30m":30,"30min":30,"30minute":30,
        "1h":60,"1hour":60,"60m":60,
        "4h":240,"4hours":240,"240m":240,
        "1d":1440,"24h":1440,"daily":1440,"day":1440,
    }
    return mapping.get(s)

def line_end(ts, freq_minutes):
    return ts + timedelta(minutes=int(freq_minutes))

# --------------------------------
# QUERY MODES
# --------------------------------

def detect_query_mode(question: str) -> str:
    q=(question or "").lower()
    if "avant l'ouverture" in q or "before open" in q:
        return "before_open"
    if "à l'ouverture" in q or "a l'ouverture" in q or "at open" in q or "à l’open" in q:
        return "at_open"
    return "generic"

def is_open_related(question: str) -> bool:
    q=(question or "").lower()
    keys=["à l'ouverture","a l'ouverture","at open","before open","avant l'ouverture","ouverture des marchés","opening"]
    return any(k in q for k in keys)

# --------------------------------
# ACCESS RULES
# --------------------------------

def allowed_columns_intraday(ts, freq_minutes, T, mode="generic"):
    end=line_end(ts, freq_minutes)
    if end <= T:
        return ["open","high","low","close"], True
    if ts < T < end:
        return ["open"], False
    if ts == T:
        if mode == "at_open":
            return ["open"], False
        if mode == "before_open":
            return [], False
        return ["open"], False
    return [], False

def allowed_columns_daily_same_day(mode="generic"):
    if mode in ("at_open","generic"):
        return ["open"], False
    if mode == "before_open":
        return [], False
    return [], False

def apply_open_only_mask(row, allowed_cols):
    out=row.copy()
    for c in ["open","high","low","close"]:
        if c in out.index and c not in allowed_cols:
            out[c]=None
    return out

def candle_access_allowed(full_candle_flag: bool) -> bool:
    return bool(full_candle_flag)

# --------------------------------
# FILTER DATAFRAME EX-ANTE
# --------------------------------

def filter_exante_intraday(df, time_col, freq_minutes, T, mode="generic"):
    rows=[]
    for _, row in df.iterrows():
        ts=row[time_col]
        cols, full_candle = allowed_columns_intraday(ts, freq_minutes, T, mode=mode)
        if not cols:
            continue
        new_row=apply_open_only_mask(row, cols)
        new_row["_exante_allowed_cols"]="|".join(cols)
        new_row["_exante_full_candle"]=bool(full_candle)
        rows.append(new_row)
    return pd.DataFrame(rows)

def filter_exante_daily(df, time_col, T, mode="generic"):
    rows=[]
    T_date=T.date()
    for _, row in df.iterrows():
        ts=row[time_col]
        if pd.isna(ts):
            continue
        d=ts.date()
        if d < T_date:
            new_row=row.copy()
            new_row["_exante_allowed_cols"]="open|high|low|close"
            new_row["_exante_full_candle"]=True
            rows.append(new_row)
        elif d == T_date:
            cols, full_candle = allowed_columns_daily_same_day(mode=mode)
            if not cols:
                continue
            new_row=apply_open_only_mask(row, cols)
            new_row["_exante_allowed_cols"]="|".join(cols)
            new_row["_exante_full_candle"]=bool(full_candle)
            rows.append(new_row)
    return pd.DataFrame(rows)

# --------------------------------
# AS-OF JOINS
# --------------------------------

def asof_join_exante(left_df, right_df, time_col):
    return pd.merge_asof(
        left_df.sort_values(time_col),
        right_df.sort_values(time_col),
        on=time_col,
        direction="backward",
        allow_exact_matches=True
    )

# --------------------------------
# HELPER FOR WINDOW QUERIES
# --------------------------------

def inclusive_date_filter(df, time_col, start_date, end_date):
    x=df.copy()
    x[time_col]=pd.to_datetime(x[time_col], errors="coerce")
    return x[(x[time_col].dt.date >= pd.to_datetime(start_date).date()) & (x[time_col].dt.date <= pd.to_datetime(end_date).date())].copy()
