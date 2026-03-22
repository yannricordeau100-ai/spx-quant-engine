import os, json, re

ROOT="/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT"
PROC=os.path.join(ROOT,"processed")
ARCH=os.path.join(PROC,"ETAPE197_ASSET_TIMEFRAME_REGISTRY.json")

CORE_META={
    "SPX":{"type":"equity_index","category":"tradable_index"},
    "SPY":{"type":"equity_etf","category":"tradable_etf"},
    "QQQ":{"type":"equity_etf","category":"tradable_etf"},
    "IWM":{"type":"equity_etf","category":"tradable_etf"},
    "VIX":{"type":"volatility_index","category":"regime_variable"},
    "VVIX":{"type":"volatility_of_volatility_index","category":"regime_variable"},
    "VIX9D":{"type":"short_term_volatility_index","category":"regime_variable"},
    "DXY":{"type":"currency_index","category":"macro_variable"},
    "GOLD":{"type":"commodity_proxy","category":"macro_variable"},
}

def load_registry():
    raw={}
    if os.path.exists(ARCH):
        try:
            raw=json.load(open(ARCH,"r",encoding="utf-8"))
        except Exception:
            raw={}
    assets=(raw.get("assets",{}) or {})
    out={}
    for k,v in CORE_META.items():
        out[k]={**v,"datasets":assets.get(k,[])}
    return out

def discover_aau_tickers():
    out={}
    for root, dirs, files in os.walk(ROOT):
        norm=root.replace("\\","/").upper()
        if "/AAU/" not in norm and not norm.endswith("/AAU"):
            continue
        for fn in files:
            if not fn.lower().endswith(".csv"):
                continue
            base=os.path.splitext(fn)[0]
            ticker=re.split(r"[_\- ]+", base)[0].upper()
            if not re.fullmatch(r"[A-Z][A-Z0-9\.]{0,8}", ticker):
                continue
            if ticker in CORE_META:
                continue
            out.setdefault(ticker,[]).append(os.path.join(root,fn))
    return out
