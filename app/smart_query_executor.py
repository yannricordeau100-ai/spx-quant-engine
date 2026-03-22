import os, re, importlib.util

CORE_ASSETS={"SPX","SPY","QQQ","IWM","VIX","VVIX","VIX9D","DXY","GOLD"}
STOPWORDS={
    "LE","LA","LES","DE","DU","DES","ET","OU","EN","SUR","SOUS","DANS","PAR","POUR","AVEC",
    "VS","QUAND","COMBIEN","QUEL","QUELLE","QUELS","QUELLES","EST","A","AU","AUX","UNE","UN",
    "MOIS","JOUR","JOURS","SEMAINE","SEMAINES","AN","ANS","ANNEE","ANNEES","YEAR","YEARS",
    "MONTH","MONTHS","DAY","DAYS","WEEK","WEEKS","PLUS","MOINS","ENTRE","DESSUS","DESSOUS",
    "PERFORMANCE","MOYENNE","COMPARAISON","TAUX","POSITIF","CLOTURE","CLOTURÉ","CLOTURER","CLOTUREE",
    "VENDREDI","LUNDI","MARDI","MERCREDI","JEUDI","FRIDAY","MONDAY","TUESDAY","WEDNESDAY","THURSDAY",
    "JANVIER","FEVRIER","FÉVRIER","MARS","AVRIL","MAI","JUIN","JUILLET","AOUT","AOÛT","SEPTEMBRE","OCTOBRE","NOVEMBRE","DECEMBRE","DÉCEMBRE",
    "FOIS","CAS","ARRIVE","ARRIVÉ","CONDITIONS","QUESTION","PERFORME"
}

def _load_module(path, name):
    spec=importlib.util.spec_from_file_location(name, path)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def _question_upper_tokens(question):
    q=str(question or "")
    raw=re.findall(r"\b[A-Z][A-Z0-9\.]{1,8}\b", q.upper())
    out=[]
    for x in raw:
        if x in STOPWORDS:
            continue
        if re.fullmatch(r"20\d{2}", x):
            continue
        if len(x) <= 2 and x not in CORE_ASSETS:
            continue
        if x not in out:
            out.append(x)
    return out

def _known_tickers_from_natural_engine(app_dir):
    known=set(CORE_ASSETS)
    try:
        nat_path=os.path.join(app_dir,"natural_stats_engine.py")
        if os.path.exists(nat_path):
            nat=_load_module(nat_path,"natural_stats_engine_registry_runtime_257e")
            if hasattr(nat,"_build_source_registry"):
                reg=nat._build_source_registry()
                known.update(set(reg.keys()))
    except Exception:
        pass
    return known

def _should_block_unknown_ticker(question, app_dir):
    tokens=_question_upper_tokens(question)
    if not tokens:
        return None
    known=_known_tickers_from_natural_engine(app_dir)
    for t in tokens:
        if t in known:
            continue
        if re.fullmatch(r"[A-Z][A-Z0-9\.]{2,5}", t):
            return t
    for t in tokens:
        if t not in known:
            return t
    return None

def _apply_formatter(app_dir, question, out):
    try:
        if out.get("ok") and isinstance(out.get("result"), dict):
            formatter_path=os.path.join(app_dir,"answer_formatter.py")
            if os.path.exists(formatter_path):
                fmt=_load_module(formatter_path,"answer_formatter_runtime_257e")
                out["result"]=fmt.format_result(question, out["result"], app_dir=app_dir)
    except Exception as e:
        out["formatting_warning"]=repr(e)
    return out

def run_query(app_dir, question, preview_rows=20):
    # 1 natural stats
    try:
        nat_path=os.path.join(app_dir,"natural_stats_engine.py")
        if os.path.exists(nat_path):
            nat=_load_module(nat_path,"natural_stats_engine_runtime_257e")
            if nat.can_handle(question):
                out={"ok":True,"result":nat.run(question, preview_rows=preview_rows)}
                return _apply_formatter(app_dir, question, out)
    except Exception as e:
        return {"ok":False,"error":"NATURAL_STATS_ENGINE_RUNTIME_ERROR","detail":repr(e)}

    # 2 unknown ticker block only after consulting known tickers
    blocked=_should_block_unknown_ticker(question, app_dir)
    if blocked is not None:
        return {
            "ok":False,
            "error":"AAU_TICKER_NOT_AVAILABLE",
            "detail":f"Le ticker {blocked} est demandé, mais aucun CSV AAU exploitable n'est actuellement chargé pour lui."
        }

    # 3 AAU engine
    try:
        aau_path=os.path.join(app_dir,"aau_research_engine.py")
        if os.path.exists(aau_path):
            aau=_load_module(aau_path,"aau_research_engine_runtime_257e")
            if hasattr(aau,"can_handle") and aau.can_handle(question):
                out={"ok":True,"result":aau.run(question, preview_rows=preview_rows)}
                return _apply_formatter(app_dir, question, out)
    except Exception as e:
        return {"ok":False,"error":"AAU_ENGINE_RUNTIME_ERROR","detail":repr(e)}

    # 4 bridge fallback
    try:
        bridge_path=os.path.join(app_dir,"runtime_query_bridge.py")
        bridge=_load_module(bridge_path,"runtime_query_bridge_runtime_257e")
        out=bridge.run_query(app_dir, question, preview_rows=preview_rows)
        return _apply_formatter(app_dir, question, out)
    except Exception as e:
        return {"ok":False,"error":"RUNTIME_BRIDGE_ERROR","detail":repr(e)}

# === ETAPE264_EXECUTOR_AAU_ALIAS_BLOCK_START ===
import re as _re_exec264

ET264_EXEC_ASSET_ALIASES = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT",
    "amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL",
    "facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA",
    "nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}

def _et264_exec_norm(x):
    s=str(x or "").lower().strip()
    rep={
        "é":"e","è":"e","ê":"e","ë":"e",
        "à":"a","â":"a","ä":"a",
        "î":"i","ï":"i",
        "ô":"o","ö":"o",
        "ù":"u","û":"u","ü":"u",
        "ç":"c",
    }
    for a,b in rep.items():
        s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'")
    s=_re_exec264.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re_exec264.sub(r"\s+"," ",s).strip()
    for alias, ticker in sorted(ET264_EXEC_ASSET_ALIASES.items(), key=lambda kv: -len(kv[0])):
        s=_re_exec264.sub(rf"(?<![a-z0-9]){_re_exec264.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s

if "_should_block_unknown_ticker" in globals() and callable(_should_block_unknown_ticker):
    _et264_old_should_block_unknown_ticker = _should_block_unknown_ticker
    def _should_block_unknown_ticker(question, app_dir):
        q2=_et264_exec_norm(question)
        return _et264_old_should_block_unknown_ticker(q2, app_dir)

_candidate_exec_names = [
    "execute_smart_query",
    "smart_execute_query",
    "run_query",
    "execute_query",
]
for _name in _candidate_exec_names:
    _fn=globals().get(_name)
    if callable(_fn):
        def _make_wrap(__fn):
            def _wrapped(question, *args, **kwargs):
                q2=_et264_exec_norm(question)
                return __fn(q2, *args, **kwargs)
            return _wrapped
        globals()[_name]=_make_wrap(_fn)
        break
# === ETAPE264_EXECUTOR_AAU_ALIAS_BLOCK_END ===


# === ETAPE264B_EXECUTOR_BLOCKER_PATCH_START ===
import os as _os264be, re as _re264be

_ET264B_EXEC_ALIASES = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT",
    "amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL",
    "facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA",
    "nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}
_ET264B_EXEC_AAU_DIRS = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]

def _et264be_norm(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264be.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264be.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264B_EXEC_ALIASES.items(), key=lambda kv: -len(kv[0])):
        s=_re264be.sub(rf"(?<![a-z0-9]){_re264be.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s

def _et264be_has_aau_file(ticker):
    t=str(ticker or "").upper().strip()
    if not t: return False
    for d in _ET264B_EXEC_AAU_DIRS:
        if not _os264be.path.isdir(d): continue
        for cand in [
            _os264be.path.join(d, f"{t}_daily.csv"),
            _os264be.path.join(d, f"{t}.csv"),
            _os264be.path.join(d, f"{t.lower()}_daily.csv"),
            _os264be.path.join(d, f"{t.lower()}.csv"),
        ]:
            if _os264be.path.exists(cand): return True
        try:
            for name in _os264be.listdir(d):
                up=name.upper()
                if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                    return True
        except Exception:
            pass
    return False

if "_should_block_unknown_ticker" in globals() and callable(_should_block_unknown_ticker):
    _et264be_old_block=_should_block_unknown_ticker
    def _should_block_unknown_ticker(question, app_dir):
        q2=_et264be_norm(question)
        for alias,ticker in _ET264B_EXEC_ALIASES.items():
            if _re264be.search(rf"(?<![a-z0-9]){_re264be.escape(ticker.lower())}(?![a-z0-9])", q2):
                if _et264be_has_aau_file(ticker):
                    return False
        return _et264be_old_block(q2, app_dir)

for _name in ["execute_smart_query","smart_execute_query","run_query","execute_query"]:
    _fn=globals().get(_name)
    if callable(_fn):
        def _make_wrap(__fn):
            def _wrapped(question, *args, **kwargs):
                return __fn(_et264be_norm(question), *args, **kwargs)
            return _wrapped
        globals()[_name]=_make_wrap(_fn)
        break
# === ETAPE264B_EXECUTOR_BLOCKER_PATCH_END ===


# === ETAPE264C_EXEC_PATCH_START ===
import os as _os264ce, re as _re264ce

_ET264C_EXEC_ALIASES = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT",
    "amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL",
    "facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA",
    "nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}
_ET264C_EXEC_AAU_DIRS = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]

def _et264ce_norm(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264ce.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264ce.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264C_EXEC_ALIASES.items(), key=lambda kv: -len(kv[0])):
        s=_re264ce.sub(rf"(?<![a-z0-9]){_re264ce.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s

def _et264ce_has_aau_file(ticker):
    t=str(ticker or "").upper().strip()
    if not t: return False
    for d in _ET264C_EXEC_AAU_DIRS:
        if not _os264ce.path.isdir(d):
            continue
        for c in [
            _os264ce.path.join(d,f"{t}_daily.csv"),
            _os264ce.path.join(d,f"{t}.csv"),
            _os264ce.path.join(d,f"{t.lower()}_daily.csv"),
            _os264ce.path.join(d,f"{t.lower()}.csv"),
        ]:
            if _os264ce.path.exists(c): return True
        try:
            for name in _os264ce.listdir(d):
                up=name.upper()
                if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                    return True
        except Exception:
            pass
    return False

if "_should_block_unknown_ticker" in globals() and callable(_should_block_unknown_ticker):
    _et264ce_old_block = _should_block_unknown_ticker
    def _should_block_unknown_ticker(question, app_dir):
        q2=_et264ce_norm(question)
        # hard bypass if canonical/alias asset exists in AAU on disk
        for alias,ticker in _ET264C_EXEC_ALIASES.items():
            if _re264ce.search(rf"(?<![a-z0-9]){_re264ce.escape(ticker.lower())}(?![a-z0-9])", q2):
                if _et264ce_has_aau_file(ticker):
                    return False
        out=_et264ce_old_block(q2, app_dir)
        # sanitize legacy malformed returns that later become "ticker False"
        if out is False or out is True:
            return False
        if isinstance(out, tuple):
            try:
                if len(out)>=1 and (out[0] is False or out[0] is True):
                    return False
            except Exception:
                pass
        return out

for _name in ["execute_smart_query","smart_execute_query","run_query","execute_query"]:
    _fn=globals().get(_name)
    if callable(_fn):
        def _make_wrap(__fn):
            def _wrapped(question,*args,**kwargs):
                return __fn(_et264ce_norm(question),*args,**kwargs)
            return _wrapped
        globals()[_name]=_make_wrap(_fn)
        break
# === ETAPE264C_EXEC_PATCH_END ===


# === ETAPE264D_EXEC_RUNTIME_PATCH_START ===
import os as _os264d_e, re as _re264d_e, importlib as _importlib264d

_ET264D_AAU_DIRS_E = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]
_ET264D_ALIASES_E = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT","amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL",
    "facebook":"META","meta":"META","tesla":"TSLA","tsla":"TSLA",
    "nvidia":"NVDA","nvda":"NVDA","spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}

def _et264d_norm_exec(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264d_e.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264d_e.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264D_ALIASES_E.items(), key=lambda kv: -len(kv[0])):
        s=_re264d_e.sub(rf"(?<![a-z0-9]){_re264d_e.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s

def _et264d_find_aau_csv_exec(ticker):
    t=str(ticker or "").upper().strip()
    if not t: return None
    for d in _ET264D_AAU_DIRS_E:
        if not _os264d_e.path.isdir(d): continue
        for c in [
            _os264d_e.path.join(d,f"{t}_daily.csv"),
            _os264d_e.path.join(d,f"{t}.csv"),
            _os264d_e.path.join(d,f"{t.lower()}_daily.csv"),
            _os264d_e.path.join(d,f"{t.lower()}.csv"),
        ]:
            if _os264d_e.path.exists(c): return _os264d_e.path.abspath(c)
        try:
            for name in _os264d_e.listdir(d):
                up=name.upper()
                if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                    p=_os264d_e.path.join(d,name)
                    if _os264d_e.path.exists(p): return _os264d_e.path.abspath(p)
        except Exception:
            pass
    return None

def _et264d_q_has_aau_asset(q):
    nq=_et264d_norm_exec(q)
    seen=[]
    for _,ticker in _ET264D_ALIASES_E.items():
        if ticker in seen: 
            continue
        seen.append(ticker)
        if _re264d_e.search(rf"(?<![a-z0-9]){_re264d_e.escape(ticker.lower())}(?![a-z0-9])", nq):
            if _et264d_find_aau_csv_exec(ticker):
                return True
    return False

if "_should_block_unknown_ticker" in globals() and callable(_should_block_unknown_ticker):
    _et264d_old_block=_should_block_unknown_ticker
    def _should_block_unknown_ticker(question, app_dir):
        q2=_et264d_norm_exec(question)
        if _et264d_q_has_aau_asset(q2):
            return False
        out=_et264d_old_block(q2, app_dir)
        if out in (False, True, None):
            return False
        if isinstance(out, tuple):
            vals=list(out)
            if any(v is False for v in vals):
                return False
        return out

def _et264d_try_direct_natural(question):
    q2=_et264d_norm_exec(question)
    try:
        mod=_importlib264d.import_module("natural_stats_engine")
    except Exception:
        try:
            import sys as _sys264d, os as _os264d2
            app_dir=_os264d2.path.dirname(__file__)
            if app_dir not in _sys264d.path:
                _sys264d.path.insert(0, app_dir)
            mod=_importlib264d.import_module("natural_stats_engine")
        except Exception:
            return None
    for rn in ["_et264d_register_aau_runtime","_et264c_register_aau","_et264b_register_aau","_et264b_inject_aau_registry"]:
        f=getattr(mod,rn,None)
        if callable(f):
            try: f()
            except Exception: pass
    cand=[]
    for name in dir(mod):
        low=name.lower()
        obj=getattr(mod,name,None)
        if callable(obj) and any(k in low for k in ["natural","stats","answer","query","engine","run"]):
            cand.append(name)
    pref=[n for n in cand if any(k in n.lower() for k in ["run_natural","natural_stats","answer_natural","execute_natural"])] + cand
    tried=set()
    for name in pref:
        if name in tried: 
            continue
        tried.add(name)
        fn=getattr(mod,name,None)
        if not callable(fn):
            continue
        try:
            out=fn(q2)
            if isinstance(out, dict) and out.get("status")=="OK":
                return {"ok":True,"result":out}
            if isinstance(out, dict) and out.get("ok") is True:
                return out
        except TypeError:
            try:
                out=fn(question=q2)
                if isinstance(out, dict) and out.get("status")=="OK":
                    return {"ok":True,"result":out}
                if isinstance(out, dict) and out.get("ok") is True:
                    return out
            except Exception:
                pass
        except Exception:
            pass
    return None

def _et264d_wrap_fn(fn):
    def _wrapped(question,*args,**kwargs):
        q2=_et264d_norm_exec(question)
        out=fn(q2,*args,**kwargs)
        if isinstance(out, dict) and out.get("error")=="AAU_TICKER_NOT_AVAILABLE":
            if _et264d_q_has_aau_asset(q2):
                retry=_et264d_try_direct_natural(q2)
                if isinstance(retry, dict):
                    return retry
        return out
    return _wrapped

for _name in ["execute_smart_query","smart_execute_query","run_query","execute_query"]:
    _fn=globals().get(_name)
    if callable(_fn):
        globals()[_name]=_et264d_wrap_fn(_fn)

# sanitize legacy message interpolation
for _k,_v in list(globals().items()):
    if callable(_v) and _k.startswith("_et264d_"):
        continue
# === ETAPE264D_EXEC_RUNTIME_PATCH_END ===


# === ETAPE264E_FRONTDOOR_PATCH_START ===
import os as _os264e, re as _re264e, importlib as _importlib264e, sys as _sys264e

_ET264E_AAU_DIRS = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]
_ET264E_ALIASES = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT","amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL","facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA","nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}

def _et264e_norm(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264e.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264e.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264E_ALIASES.items(), key=lambda kv:-len(kv[0])):
        s=_re264e.sub(rf"(?<![a-z0-9]){_re264e.escape(alias.lower())}(?![a-z0-9])", ticker.lower(), s)
    return s

def _et264e_find_aau_csv(ticker):
    t=str(ticker or "").upper().strip()
    if not t: return None
    for d in _ET264E_AAU_DIRS:
        if not _os264e.path.isdir(d): continue
        for c in [
            _os264e.path.join(d,f"{t}_daily.csv"),
            _os264e.path.join(d,f"{t}.csv"),
            _os264e.path.join(d,f"{t.lower()}_daily.csv"),
            _os264e.path.join(d,f"{t.lower()}.csv"),
        ]:
            if _os264e.path.exists(c): return _os264e.path.abspath(c)
        try:
            for name in _os264e.listdir(d):
                up=name.upper()
                if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                    p=_os264e.path.join(d,name)
                    if _os264e.path.exists(p): return _os264e.path.abspath(p)
        except Exception:
            pass
    return None

def _et264e_has_aau_asset(q):
    nq=_et264e_norm(q)
    found=[]
    for _,ticker in _ET264E_ALIASES.items():
        if ticker in found: continue
        found.append(ticker)
        if _re264e.search(rf"(?<![a-z0-9]){_re264e.escape(ticker.lower())}(?![a-z0-9])", nq):
            if _et264e_find_aau_csv(ticker):
                return True
    return False

def _et264e_import_nat():
    app_dir=_os264e.path.dirname(__file__)
    if app_dir not in _sys264e.path:
        _sys264e.path.insert(0, app_dir)
    return _importlib264e.import_module("natural_stats_engine")

def _et264e_direct_natural(question):
    q2=_et264e_norm(question)
    try:
        mod=_et264e_import_nat()
    except Exception:
        return None
    for n in ["_et264e_register_aau_n","_et264d_register_aau_runtime","_et264c_register_aau","_et264b_inject_aau_registry"]:
        f=getattr(mod,n,None)
        if callable(f):
            try: f()
            except Exception: pass
    cand=[]
    for name in dir(mod):
        low=name.lower()
        fn=getattr(mod,name,None)
        if not callable(fn): 
            continue
        if any(k in low for k in ["natural","stats","answer","query","engine","run"]):
            cand.append(name)
    pref=[n for n in cand if any(k in n.lower() for k in ["run_natural","natural_stats","answer_natural","execute_natural"])] + cand
    tried=set()
    for name in pref:
        if name in tried: continue
        tried.add(name)
        fn=getattr(mod,name,None)
        try:
            out=fn(q2)
            if isinstance(out,dict):
                if out.get("status")=="OK":
                    return {"ok":True,"result":out}
                if out.get("ok") is True:
                    return out
        except TypeError:
            try:
                out=fn(question=q2)
                if isinstance(out,dict):
                    if out.get("status")=="OK":
                        return {"ok":True,"result":out}
                    if out.get("ok") is True:
                        return out
            except Exception:
                pass
        except Exception:
            pass
    return None

if "_should_block_unknown_ticker" in globals() and callable(_should_block_unknown_ticker):
    _et264e_old_should_block=_should_block_unknown_ticker
    def _should_block_unknown_ticker(question, app_dir):
        if _et264e_has_aau_asset(question):
            return False
        out=_et264e_old_should_block(_et264e_norm(question), app_dir)
        if out in (False,True,None):
            return False
        if isinstance(out,tuple) and any(v in (False,True,None) for v in out):
            return False
        return out

def _et264e_wrap(fn):
    def _wrapped(question,*args,**kwargs):
        q2=_et264e_norm(question)
        if _et264e_has_aau_asset(q2):
            direct=_et264e_direct_natural(q2)
            if isinstance(direct,dict):
                return direct
        out=fn(q2,*args,**kwargs)
        if isinstance(out,dict):
            if out.get("error")=="AAU_TICKER_NOT_AVAILABLE" or "ticker False" in str(out.get("detail","")):
                if _et264e_has_aau_asset(q2):
                    direct=_et264e_direct_natural(q2)
                    if isinstance(direct,dict):
                        return direct
        return out
    return _wrapped

for _nm in ["execute_smart_query","smart_execute_query","run_query","execute_query","answer_question","process_query"]:
    _fn=globals().get(_nm)
    if callable(_fn):
        globals()[_nm]=_et264e_wrap(_fn)
# === ETAPE264E_FRONTDOOR_PATCH_END ===


# === ETAPE264F_HARDEN_PATCH_START ===
import os as _os264f_e, re as _re264f_e, sys as _sys264f_e, importlib as _importlib264f_e

_ET264F_AAU_DIRS_E = [
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/RAW_SOURCES/Autres Actions Upload",
    "/content/drive/MyDrive/IA (ancien)/SPX_OPEN_ENGINE_PROJECT/exports/portable_backup_temp/RAW_SOURCES/Autres Actions Upload",
]
_ET264F_ALIAS_E = {
    "apple":"AAPL","aapl":"AAPL","apple inc":"AAPL",
    "microsoft":"MSFT","msft":"MSFT","amazon":"AMZN","amzn":"AMZN",
    "google":"GOOGL","alphabet":"GOOGL","googl":"GOOGL","facebook":"META","meta":"META",
    "tesla":"TSLA","tsla":"TSLA","nvidia":"NVDA","nvda":"NVDA",
    "spy":"SPY","spx":"SPX","qqq":"QQQ","vix":"VIX","dxy":"DXY",
}
def _et264f_norm_e(x):
    s=str(x or "").lower().strip()
    rep={"é":"e","è":"e","ê":"e","ë":"e","à":"a","â":"a","ä":"a","î":"i","ï":"i","ô":"o","ö":"o","ù":"u","û":"u","ü":"u","ç":"c"}
    for a,b in rep.items(): s=s.replace(a,b)
    s=s.replace("’","'").replace("`","'").replace("août","aout")
    s=_re264f_e.sub(r"[^a-z0-9%+\-\. ]+"," ",s)
    s=_re264f_e.sub(r"\s+"," ",s).strip()
    for alias,ticker in sorted(_ET264F_ALIAS_E.items(), key=lambda kv:-len(kv[0])):
        s=_re264f_e.sub(rf"(?<![a-z0-9]){_re264f_e.escape(alias)}(?![a-z0-9])", ticker.lower(), s)
    return s
def _et264f_extract_tickers_e(question):
    nq=_et264f_norm_e(question)
    out=[]
    for _,ticker in sorted(_ET264F_ALIAS_E.items(), key=lambda kv:-len(kv[0])):
        if _re264f_e.search(rf"(?<![a-z0-9]){_re264f_e.escape(ticker.lower())}(?![a-z0-9])", nq):
            if ticker not in out: out.append(ticker)
    return out
def _et264f_find_aau_csv_e(ticker):
    t=str(ticker or "").upper().strip()
    if not t:return None
    for d in _ET264F_AAU_DIRS_E:
        if not _os264f_e.path.isdir(d):continue
        for c in [
            _os264f_e.path.join(d,f"{t}_daily.csv"),
            _os264f_e.path.join(d,f"{t}.csv"),
            _os264f_e.path.join(d,f"{t.lower()}_daily.csv"),
            _os264f_e.path.join(d,f"{t.lower()}.csv"),
        ]:
            if _os264f_e.path.exists(c): return _os264f_e.path.abspath(c)
        try:
            for name in _os264f_e.listdir(d):
                up=name.upper()
                if up.startswith(t+"_") or up==t+".CSV" or up==t+"_DAILY.CSV":
                    p=_os264f_e.path.join(d,name)
                    if _os264f_e.path.exists(p): return _os264f_e.path.abspath(p)
        except Exception:
            pass
    return None
def _et264f_has_aau_asset_e(question):
    for t in _et264f_extract_tickers_e(question):
        if _et264f_find_aau_csv_e(t): return True
    return False
def _et264f_sanitize_ticker_e(ticker, question=None):
    if ticker in (False,True,None,"","False","True","None"):
        ex=_et264f_extract_tickers_e(question or "")
        if ex:return ex[0]
        return None
    t=str(ticker).upper().strip()
    if t in _ET264F_ALIAS_E.values(): return t
    tt=_ET264F_ALIAS_E.get(str(ticker).lower().strip())
    if tt:return tt
    return t
def _et264f_direct_natural_e(question):
    q2=_et264f_norm_e(question)
    app_dir=_os264f_e.path.dirname(__file__)
    if app_dir not in _sys264f_e.path:_sys264f_e.path.insert(0,app_dir)
    try:
        mod=_importlib264f_e.import_module("natural_stats_engine")
    except Exception:
        return None
    for n in ["_et264f_sanitize_ticker","_et264f_extract_tickers","_et264f_find_aau_csv","_et264e_register_aau_n","_et264d_register_aau_runtime","_et264c_register_aau"]:
        f=getattr(mod,n,None)
        if callable(f):
            try:f()
            except TypeError:pass
            except Exception:pass
    cand=[]
    for name in dir(mod):
        low=name.lower()
        fn=getattr(mod,name,None)
        if callable(fn) and any(k in low for k in ["natural","stats","answer","query","engine","run"]):
            cand.append(name)
    pref=[n for n in cand if any(k in n.lower() for k in ["run_natural","natural_stats","answer_natural","execute_natural"])] + cand
    done=set()
    for name in pref:
        if name in done:continue
        done.add(name)
        fn=getattr(mod,name,None)
        try:
            out=fn(q2)
            if isinstance(out,dict):
                if out.get("status")=="OK": return {"ok":True,"result":out}
                if out.get("ok") is True: return out
        except TypeError:
            try:
                out=fn(question=q2)
                if isinstance(out,dict):
                    if out.get("status")=="OK": return {"ok":True,"result":out}
                    if out.get("ok") is True: return out
            except Exception:
                pass
        except Exception:
            pass
    return None

if "_should_block_unknown_ticker" in globals() and callable(_should_block_unknown_ticker):
    _old_et264f_block=_should_block_unknown_ticker
    def _should_block_unknown_ticker(question, app_dir):
        if _et264f_has_aau_asset_e(question):
            return False
        out=_old_et264f_block(_et264f_norm_e(question), app_dir)
        if out in (False,True,None): return False
        if isinstance(out,tuple):
            vals=list(out)
            if any(v in (False,True,None,"False","True","None","") for v in vals):
                return False
        return out

def _et264f_wrap(fn):
    def _wrapped(question,*args,**kwargs):
        q2=_et264f_norm_e(question)
        if _et264f_has_aau_asset_e(q2):
            direct=_et264f_direct_natural_e(q2)
            if isinstance(direct,dict): return direct
        out=fn(q2,*args,**kwargs)
        if isinstance(out,dict):
            det=str(out.get("detail",""))
            err=str(out.get("error",""))
            if err=="AAU_TICKER_NOT_AVAILABLE" or "ticker False" in det or "ticker False" in str(out):
                if _et264f_has_aau_asset_e(q2):
                    direct=_et264f_direct_natural_e(q2)
                    if isinstance(direct,dict): return direct
        return out
    return _wrapped

for _nm in ["execute_smart_query","smart_execute_query","run_query","execute_query","answer_question","process_query"]:
    _fn=globals().get(_nm)
    if callable(_fn):
        globals()[_nm]=_et264f_wrap(_fn)
# === ETAPE264F_HARDEN_PATCH_END ===


# === ETAPE265_FRONTDOOR_PATCH_START ===
import importlib as _et265_importlib, sys as _et265_sys, os as _et265_os

def _et265_try_manual_frontdoor(question, app_dir):
    try:
        if app_dir not in _et265_sys.path:
            _et265_sys.path.insert(0, app_dir)
        mod=_et265_importlib.import_module("manual_stats_frontdoor")
        out=mod.execute_manual_stats(question)
        if isinstance(out,dict) and (out.get("ok") is True or out.get("error") in {"AAU_TICKER_NOT_AVAILABLE","NO_MATCH_AFTER_FILTER","UNSUPPORTED_OR_NO_ASSET"}):
            return out
    except Exception as e:
        return {"ok":False,"error":"MANUAL_FRONTDOOR_RUNTIME_ERROR","detail":repr(e)}
    return None

def _et265_wrap_frontdoor(fn):
    def _wrapped(question,*args,**kwargs):
        app_dir=_et265_os.path.dirname(__file__)
        direct=_et265_try_manual_frontdoor(question, app_dir)
        if isinstance(direct,dict) and direct.get("ok") is True:
            return direct
        if isinstance(direct,dict) and direct.get("error")=="AAU_TICKER_NOT_AVAILABLE":
            return direct
        out=fn(question,*args,**kwargs)
        if isinstance(out,dict) and out.get("error")=="AAU_TICKER_NOT_AVAILABLE":
            direct=_et265_try_manual_frontdoor(question, app_dir)
            if isinstance(direct,dict):
                return direct
        return out
    return _wrapped

for _nm in ["execute_smart_query","smart_execute_query","run_query","execute_query","answer_question","process_query"]:
    _fn=globals().get(_nm)
    if callable(_fn):
        globals()[_nm]=_et265_wrap_frontdoor(_fn)
# === ETAPE265_FRONTDOOR_PATCH_END ===
