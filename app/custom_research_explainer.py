import math

#1/20 helpers
def _to_float(x):
    try:
        if x is None:
            return None
        v=float(x)
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None

def _sample_size(row):
    try:
        return int(float(row.get("sample_size")))
    except Exception:
        return None

def _best_probability(row):
    up=_to_float(row.get("open_up_prob"))
    down=_to_float(row.get("open_down_prob"))
    if up is None and down is None:
        return None
    if down is None or (up is not None and up>=down):
        return {"label":"probabilité ouverture haussière","value":up,"side":"up"}
    return {"label":"probabilité ouverture baissière","value":down,"side":"down"}

def _direction_score(row):
    vals=[]
    for key in ["plus5d_mean","plus3d_mean","next_close_mean","intraday_mean","gap_mean"]:
        v=_to_float(row.get(key))
        if v is not None:
            vals.append(v)
    if not vals:
        return 0.0
    return float(sum(vals)/len(vals))

def _direction_label(row):
    s=_direction_score(row)
    if s>0:
        return "biais haussier"
    if s<0:
        return "biais baissier"
    return "neutre"

def _magnitude_score(row):
    vals=[]
    for key in ["plus5d_mean","plus3d_mean","next_close_mean","intraday_mean","gap_mean"]:
        v=_to_float(row.get(key))
        if v is not None:
            vals.append(abs(v))
    if not vals:
        return 0.0
    return float(sum(vals)/len(vals))

def _sample_score(n):
    if n is None:
        return 0.0
    if n>=300: return 1.0
    if n>=150: return 0.8
    if n>=80: return 0.6
    if n>=40: return 0.4
    if n>=20: return 0.25
    return 0.1

def _probability_score(row):
    prob=_best_probability(row)
    if prob is None:
        return 0.0
    v=_to_float(prob.get("value"))
    if v is None:
        return 0.0
    return abs(v-0.5)*2.0

def _confidence_components(row):
    n=_sample_size(row)
    sample_score=_sample_score(n)
    magnitude=min(1.0,_magnitude_score(row)/0.02)
    prob_score=_probability_score(row)
    total=0.5*sample_score+0.3*magnitude+0.2*prob_score
    if total>=0.75:
        label="forte"
    elif total>=0.5:
        label="moyenne"
    else:
        label="faible"
    flags=[]
    if n is None or n<40:
        flags.append("échantillon faible")
    if prob_score<0.1:
        flags.append("direction peu marquée")
    if magnitude<0.15:
        flags.append("effet faible")
    if _direction_label(row)=="neutre":
        flags.append("signal peu lisible")
    return {
        "sample_size":n,
        "sample_score":round(sample_score,4),
        "magnitude_score":round(magnitude,4),
        "probability_score":round(prob_score,4),
        "confidence_score":round(total,4),
        "confidence_label":label,
        "flags":flags
    }

def _row_priority_score(row):
    conf=_confidence_components(row)
    mag=_magnitude_score(row)
    return float(conf["confidence_score"]*0.6 + min(1.0,mag/0.02)*0.4)

def _case_family(row):
    case=str(row.get("case",""))
    feature_dataset=str(row.get("feature_dataset",""))
    target=str(row.get("target_dataset",""))
    text=" ".join([case,feature_dataset,target]).lower()
    if "calendar" in text or "macro" in text or "quiet" in text:
        return "calendar_macro"
    if "vix" in text or "vvix" in text:
        return "volatilité"
    if "dxy" in text or "dollar" in text:
        return "dollar"
    if target and target!="spx_daily":
        return "aau_conditionnée"
    return "général"

def _horizon_focus(row):
    p5=_to_float(row.get("plus5d_mean"))
    p3=_to_float(row.get("plus3d_mean"))
    n1=_to_float(row.get("next_close_mean"))
    mags=[
        ("J+5", abs(p5) if p5 is not None else -1),
        ("J+3", abs(p3) if p3 is not None else -1),
        ("lendemain", abs(n1) if n1 is not None else -1),
    ]
    best=max(mags,key=lambda x:x[1])
    return None if best[1] < 0 else best[0]

def _primary_signal_sentence(row):
    conf=_confidence_components(row)
    case=row.get("case","cas")
    feature=row.get("feature")
    bucket=row.get("bucket")
    direction=_direction_label(row)
    prob=_best_probability(row)
    horizon=_horizon_focus(row)
    parts=[str(case)]
    if feature:
        parts.append(str(feature))
    if bucket is not None:
        parts.append(f"bucket={bucket}")
    txt=" | ".join(parts)
    tail=[direction]
    if horizon:
        tail.append(f"horizon dominant={horizon}")
    if prob and prob.get("value") is not None:
        tail.append(f"{prob['label']}={prob['value']:.3f}")
    tail.append(f"confiance={conf['confidence_label']} ({conf['confidence_score']:.2f})")
    n=conf["sample_size"]
    if n is not None:
        tail.append(f"n={n}")
    return txt + " | " + " | ".join(tail)

def _secondary_signal_sentence(row):
    conf=_confidence_components(row)
    direction=_direction_label(row)
    nxt=_to_float(row.get("next_close_mean"))
    p3=_to_float(row.get("plus3d_mean"))
    p5=_to_float(row.get("plus5d_mean"))
    nums=[]
    if nxt is not None: nums.append(f"lendemain={nxt:.4f}")
    if p3 is not None: nums.append(f"J+3={p3:.4f}")
    if p5 is not None: nums.append(f"J+5={p5:.4f}")
    flags=conf["flags"][:2]
    txt=f"{direction} | confiance={conf['confidence_label']} ({conf['confidence_score']:.2f})"
    if nums:
        txt+=" | " + " | ".join(nums)
    if flags:
        txt+=" | flags=" + ", ".join(flags)
    return txt

#2/20 main explain
def explain_result(result_dict, top_n=3):
    if not isinstance(result_dict,dict):
        return {
            "summary":"résultat non exploitable",
            "top_rows":[],
            "bullets":[],
            "confidence_overview":{"global_label":"faible","global_score":0.0,"flags":["résultat non exploitable"]},
            "primary_reading":None,
            "secondary_readings":[],
            "agreement_label":"indéterminé",
        }

    preview=result_dict.get("preview",[])
    if not isinstance(preview,list) or len(preview)==0:
        return {
            "summary":"aucune ligne exploitable retournée par le moteur",
            "top_rows":[],
            "bullets":["Le moteur n'a retourné aucune preview exploitable."],
            "confidence_overview":{"global_label":"faible","global_score":0.0,"flags":["aucune donnée"]},
            "primary_reading":None,
            "secondary_readings":[],
            "agreement_label":"indéterminé",
        }

    ranked=sorted(preview,key=_row_priority_score,reverse=True)
    top_rows=ranked[:top_n]
    conf_rows=[_confidence_components(r) for r in top_rows]
    directions=[_direction_label(r) for r in top_rows]
    families=[_case_family(r) for r in top_rows]

    avg_conf=sum(r["confidence_score"] for r in conf_rows)/len(conf_rows) if conf_rows else 0.0
    if avg_conf>=0.75:
        global_label="forte"
    elif avg_conf>=0.5:
        global_label="moyenne"
    else:
        global_label="faible"

    if directions.count("biais haussier")==len(directions):
        agreement="alignement haussier"
    elif directions.count("biais baissier")==len(directions):
        agreement="alignement baissier"
    elif "biais haussier" in directions and "biais baissier" in directions:
        agreement="signaux divergents"
    else:
        agreement="signaux partiellement alignés"

    flags=[]
    for c in conf_rows:
        for f in c["flags"]:
            if f not in flags:
                flags.append(f)

    primary=top_rows[0] if top_rows else None
    primary_reading=None if primary is None else _primary_signal_sentence(primary)
    secondary=[]
    for row in top_rows[1:]:
        secondary.append(_secondary_signal_sentence(row))

    bullets=[]
    if primary_reading:
        bullets.append("Signal principal | " + primary_reading)
    for i,s in enumerate(secondary,1):
        bullets.append(f"Signal secondaire {i} | {s}")

    fam_text=", ".join(sorted(set(families))) if families else "n/a"
    value=result_dict.get("value",len(preview))
    summary=(
        f"{value} ligne(s) retournée(s), {len(top_rows)} mise(s) en avant, "
        f"{agreement}, confiance globale {global_label} ({avg_conf:.2f}), "
        f"famille(s) dominante(s): {fam_text}."
    )

    return {
        "summary":summary,
        "top_rows":top_rows,
        "bullets":bullets,
        "confidence_overview":{
            "global_score":round(avg_conf,4),
            "global_label":global_label,
            "flags":flags[:6]
        },
        "primary_reading":primary_reading,
        "secondary_readings":secondary,
        "agreement_label":agreement,
    }
