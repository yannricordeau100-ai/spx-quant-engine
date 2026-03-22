import os, importlib.util

def _load_module(path, name):
    spec=importlib.util.spec_from_file_location(name, path)
    mod=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod

def _append_history_and_trace(project_root, question, result):
    try:
        hist_path=os.path.join(project_root,"app_streamlit","query_history_store.py")
        if os.path.exists(hist_path):
            hist_mod=_load_module(hist_path,"query_history_store_runtime_223")
            hist_jsonl=os.path.join(project_root,"processed","QUERY_HISTORY","query_history.jsonl")
            hist_csv=os.path.join(project_root,"processed","QUERY_HISTORY","query_history_index.csv")
            hist_mod.append_history(hist_jsonl,hist_csv,"classic_research",question,result)
    except Exception:
        pass
    try:
        trace_path=os.path.join(project_root,"app_streamlit","query_trace_store.py")
        if os.path.exists(trace_path):
            trace_mod=_load_module(trace_path,"query_trace_store_runtime_223")
            last_json=os.path.join(project_root,"processed","LAST_QUERY_SOURCE_TRACE.json")
            hist_jsonl=os.path.join(project_root,"processed","QUERY_SOURCE_TRACE_HISTORY.jsonl")
            trace_mod.append_trace(last_json,hist_jsonl,question,result)
    except Exception:
        pass

def _is_strong_new_question(q):
    q=(q or "").lower()
    triggers=[
        "combien","nombre","nb ","nb de","fois",
        "aapl","msft","nvda","tsla","meta","amzn","googl",
        "janvier","fevrier","février","mars","avril","mai","juin","juillet","aout","août","septembre","octobre","novembre","decembre","décembre",
        "entre ","superieur","supérieur","inferieur","inférieur","dessous","dessus",
        "cloture","clôture","cloturé","clôturé","close"
    ]
    return any(t in q for t in triggers)

def _polish_result(question, result):
    if not isinstance(result, dict):
        return result
    if result.get("value") is not None and any(x in (question or "").lower() for x in ["combien","nombre","nb ","nb de","fois"]):
        val=result.get("value")
        if result.get("answer_short") is None:
            result["answer_short"]=f"{val} fois"
    if result.get("answer_long") is None and result.get("answer"):
        result["answer_long"]=result.get("answer")
    if result.get("summary") is None:
        if result.get("engine")=="count_threshold_engine":
            result["summary"]="Réponse construite à partir du moteur de comptage et filtrage."
    return result

def run_query(app_dir, question, preview_rows=20):
    project_root=os.path.dirname(app_dir)

    count_threshold_engine_path=os.path.join(app_dir,"count_threshold_engine.py")
    followup_engine_path=os.path.join(app_dir,"result_followup_engine.py")
    followup_memory_path=os.path.join(project_root,"processed","ETAPE163_LAST_CUSTOM_RESEARCH_MEMORY.json")
    derived_engine_path=os.path.join(app_dir,"derived_feature_engine.py")
    simple_fr_engine_path=os.path.join(app_dir,"simple_fr_runtime_engine.py")
    exploratory_engine_path=os.path.join(app_dir,"exploratory_research_engine.py")
    market_reasoning_engine_path=os.path.join(app_dir,"market_reasoning_engine.py")
    intraday_edge_engine_path=os.path.join(app_dir,"intraday_edge_engine.py")
    quant_engine_path=os.path.join(app_dir,"quant_research_engine.py")
    generic_engine_path=os.path.join(app_dir,"generic_query_engine.py")
    custom_engine_path=os.path.join(app_dir,"custom_pattern_research_engine.py")
    architecture_json_path=os.path.join(project_root,"processed","ETAPE135_DUAL_UNIVERSE_ARCHITECTURE_REGISTRY.json")

    ql=str(question).lower()

    # 1) count threshold engine first
    try:
        if os.path.exists(count_threshold_engine_path):
            cmod=_load_module(count_threshold_engine_path,"count_threshold_engine_runtime_223")
            if cmod.can_handle(question):
                cres=cmod.run(question,preview_rows=preview_rows)
                result={"engine":"count_threshold_engine",**cres}
                result=_polish_result(question,result)
                _append_history_and_trace(project_root,question,result)
                return {"ok":True,"result":result}
    except Exception as e:
        return {"ok":False,"error":repr(e)}

    # 2) followup only if not a strong new question
    try:
        if os.path.exists(followup_engine_path):
            followup_engine=_load_module(followup_engine_path,"result_followup_engine_runtime_223")
            if (not _is_strong_new_question(question)) and followup_engine.is_followup_question(question):
                memory=followup_engine.load_memory(followup_memory_path)
                result={"engine":"result_followup_engine",**followup_engine.run_followup(question,memory)}
                result=_polish_result(question,result)
                _append_history_and_trace(project_root,question,result)
                return {"ok":True,"result":result}
    except Exception:
        pass

    # 3) derived
    try:
        if os.path.exists(derived_engine_path):
            dmod=_load_module(derived_engine_path,"derived_feature_engine_runtime_223")
            eng=dmod.DerivedFeatureEngine(project_root)
            if eng.can_handle(question):
                dres=eng.run(question,preview_rows=preview_rows)
                result={"engine":"derived_feature_engine",**dres}
                if result.get("answer") is None and result.get("value") is not None:
                    result["answer"]=f"Pour cette recherche dérivée, {result.get('value')} fois ont été retenues."
                result=_polish_result(question,result)
                _append_history_and_trace(project_root,question,result)
                return {"ok":True,"result":result}
    except Exception:
        pass

    # 4) market reasoning
    try:
        if os.path.exists(market_reasoning_engine_path):
            mmod=_load_module(market_reasoning_engine_path,"market_reasoning_engine_runtime_223")
            if hasattr(mmod,"can_handle") and mmod.can_handle(question):
                mres=mmod.run(question,preview_rows=preview_rows)
                result={"engine":"market_reasoning_engine",**mres}
                result=_polish_result(question,result)
                _append_history_and_trace(project_root,question,result)
                return {"ok":True,"result":result}
    except Exception:
        pass

    # 5) intraday
    try:
        if os.path.exists(intraday_edge_engine_path):
            imod=_load_module(intraday_edge_engine_path,"intraday_edge_engine_runtime_223")
            if hasattr(imod,"can_handle") and imod.can_handle(question):
                ires=imod.run(question,preview_rows=preview_rows)
                result={"engine":"intraday_edge_engine",**ires}
                result=_polish_result(question,result)
                _append_history_and_trace(project_root,question,result)
                return {"ok":True,"result":result}
    except Exception:
        pass

    # 6) exploratory
    try:
        if os.path.exists(exploratory_engine_path):
            emod=_load_module(exploratory_engine_path,"exploratory_research_engine_runtime_223")
            if hasattr(emod,"can_handle") and emod.can_handle(question):
                eres=emod.run(question,preview_rows=preview_rows)
                result={"engine":"exploratory_research_engine",**eres}
                result=_polish_result(question,result)
                _append_history_and_trace(project_root,question,result)
                return {"ok":True,"result":result}
    except Exception:
        pass

    # 7) quant
    try:
        if os.path.exists(quant_engine_path):
            qmod=_load_module(quant_engine_path,"quant_research_engine_runtime_223")
            if hasattr(qmod,"can_handle") and qmod.can_handle(question):
                qres=qmod.run(question,preview_rows=preview_rows)
                result={"engine":"quant_research_engine",**qres}
                result=_polish_result(question,result)
                _append_history_and_trace(project_root,question,result)
                return {"ok":True,"result":result}
    except Exception:
        pass

    # 8) simple_fr
    try:
        if os.path.exists(simple_fr_engine_path):
            smod=_load_module(simple_fr_engine_path,"simple_fr_runtime_engine_runtime_223")
            eng=smod.SimpleFRRuntimeEngine(project_root)
            if eng.can_handle(question):
                sres=eng.run(question,preview_rows=preview_rows)
                if sres.get("status") not in ["NO_SIMPLE_FR_MATCH","OK_EMPTY"]:
                    result={"engine":"simple_fr_runtime_engine",**sres}
                    result=_polish_result(question,result)
                    _append_history_and_trace(project_root,question,result)
                    return {"ok":True,"result":result}
    except Exception:
        pass

    # 9) generic
    try:
        if os.path.exists(generic_engine_path):
            gmod=_load_module(generic_engine_path,"generic_query_engine_runtime_223")
            eng=gmod.GenericQueryEngine(project_root)
            gres=eng.run(question,preview_rows=preview_rows)
            if gres.get("status")!="NO_GENERIC_MATCH":
                result={"engine":"generic_query_engine",**gres}
                result=_polish_result(question,result)
                _append_history_and_trace(project_root,question,result)
                return {"ok":True,"result":result}
    except Exception:
        pass

    # 10) custom
    try:
        if os.path.exists(custom_engine_path):
            cmod=_load_module(custom_engine_path,"custom_pattern_research_engine_runtime_223")
            eng=cmod.CustomPatternResearchEngine(architecture_json_path)
            if hasattr(eng,"can_handle") and eng.can_handle(question):
                cres=eng.run(question,preview_rows=preview_rows)
                result={"engine":"custom_pattern_research_engine",**cres}
                result=_polish_result(question,result)
                _append_history_and_trace(project_root,question,result)
                return {"ok":True,"result":result}
    except Exception:
        pass

    return {"ok":False,"error":"NO_RUNTIME_HANDLER_FOR_QUESTION"}
