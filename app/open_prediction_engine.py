import math

def required_inputs():
    return [
        {"id":"spy_prev_close","label":"SPY à la clôture la veille","phase":"preopen","required":True},
        {"id":"spy_928","label":"SPY à 9h28 ET","phase":"preopen","required":True},
        {"id":"qqq_prev_close","label":"QQQ à la clôture la veille","phase":"preopen","required":True},
        {"id":"qqq_928","label":"QQQ à 9h28 ET","phase":"preopen","required":True},
        {"id":"vix_prev_close","label":"VIX à la clôture la veille","phase":"preopen","required":True},
        {"id":"vix_928","label":"VIX à 9h28 ET","phase":"preopen","required":True},

        {"id":"spy_930","label":"SPY à 9h30 ET","phase":"open","required":False},
        {"id":"qqq_930","label":"QQQ à 9h30 ET","phase":"open","required":False},
        {"id":"vix_930","label":"VIX à 9h30 ET","phase":"open","required":False},
    ]

def _to_float(v):
    if v is None:
        return None
    if isinstance(v,(int,float)):
        return float(v)
    s=str(v).strip().replace(",",".")
    if not s:
        return None
    return float(s)

def _pct(a,b):
    a=_to_float(a); b=_to_float(b)
    if a is None or b is None or a==0:
        return None
    return (b/a - 1.0) * 100.0

def _sigmoid(x):
    return 1.0/(1.0+math.exp(-x))

def _bucket(points):
    if points < 10:
        return "0–10"
    if points < 25:
        return "10–25"
    if points < 40:
        return "25–40"
    return "40+"

def run_prediction(raw_inputs):
    vals={}
    for f in required_inputs():
        vals[f["id"]]=_to_float(raw_inputs.get(f["id"]))

    missing=[f["label"] for f in required_inputs() if f["required"] and vals.get(f["id"]) is None]
    if missing:
        return {
            "status":"MISSING_REQUIRED_INPUTS",
            "answer_short":"Données manquantes",
            "answer_long":"Il manque des valeurs obligatoires pour calculer la prédiction open.",
            "missing_inputs":missing,
            "used_inputs":vals,
        }

    spy_gap_928=_pct(vals["spy_prev_close"], vals["spy_928"])
    qqq_gap_928=_pct(vals["qqq_prev_close"], vals["qqq_928"])
    vix_gap_928=_pct(vals["vix_prev_close"], vals["vix_928"])

    spy_gap_930=_pct(vals["spy_prev_close"], vals["spy_930"]) if vals.get("spy_930") is not None else None
    qqq_gap_930=_pct(vals["qqq_prev_close"], vals["qqq_930"]) if vals.get("qqq_930") is not None else None
    vix_gap_930=_pct(vals["vix_prev_close"], vals["vix_930"]) if vals.get("vix_930") is not None else None

    # Score directionnel pré-open
    score = 0.0
    score += 1.40 * spy_gap_928
    score += 0.90 * qqq_gap_928
    score -= 0.35 * vix_gap_928

    reasoning=[]

    reasoning.append({"theme":"spy_preopen","value_pct":round(spy_gap_928,4),"impact":"positif" if spy_gap_928>=0 else "négatif"})
    reasoning.append({"theme":"qqq_preopen","value_pct":round(qqq_gap_928,4),"impact":"positif" if qqq_gap_928>=0 else "négatif"})
    reasoning.append({"theme":"vix_preopen","value_pct":round(vix_gap_928,4),"impact":"négatif pour le sens haussier" if vix_gap_928>=0 else "positif pour le sens haussier"})

    open_confirmation_used=False
    if spy_gap_930 is not None or qqq_gap_930 is not None or vix_gap_930 is not None:
        open_confirmation_used=True
        score += 1.10 * (spy_gap_930 if spy_gap_930 is not None else spy_gap_928)
        score += 0.70 * (qqq_gap_930 if qqq_gap_930 is not None else qqq_gap_928)
        score -= 0.30 * (vix_gap_930 if vix_gap_930 is not None else vix_gap_928)

        reasoning.append({"theme":"confirmation_930","spy_930_pct":spy_gap_930,"qqq_930_pct":qqq_gap_930,"vix_930_pct":vix_gap_930})

    prob_up = _sigmoid(score / 1.8)
    prob_down = 1.0 - prob_up
    direction = "hausse" if prob_up >= 0.5 else "baisse"
    prob_direction = prob_up if direction=="hausse" else prob_down

    # amplitude attendue en points SPX
    base_amp = 4.0
    base_amp += 9.0 * abs(spy_gap_928)
    base_amp += 6.0 * abs(qqq_gap_928)
    base_amp += 1.5 * abs(vix_gap_928)

    if open_confirmation_used:
        base_amp += 7.0 * abs((spy_gap_930 if spy_gap_930 is not None else spy_gap_928) - spy_gap_928)
        base_amp += 4.0 * abs((qqq_gap_930 if qqq_gap_930 is not None else qqq_gap_928) - qqq_gap_928)
        base_amp += 1.0 * abs((vix_gap_930 if vix_gap_930 is not None else vix_gap_928) - vix_gap_928)

    expected_points = max(0.0, min(80.0, base_amp))
    bucket = _bucket(expected_points)

    short = f"{direction.capitalize()} {prob_direction*100:.1f}% | {expected_points:.1f} pts | bucket {bucket}"
    long_1 = f"Probabilité principale à l'open : {direction} {prob_direction*100:.1f}%."
    long_2 = f"Amplitude attendue : {expected_points:.1f} points SPX."
    long_3 = f"Bucket attendu : {bucket} points."
    long_4 = "La confirmation 9h30 a été utilisée." if open_confirmation_used else "Prédiction basée uniquement sur les données avant ouverture."

    return {
        "status":"OK",
        "mode":"manual_open_prediction",
        "direction":direction,
        "prob_up":prob_up,
        "prob_down":prob_down,
        "prob_direction":prob_direction,
        "expected_points":expected_points,
        "expected_bucket":bucket,
        "answer_short":short,
        "answer_long":" ".join([long_1,long_2,long_3,long_4]),
        "used_inputs":vals,
        "derived_features":{
            "spy_gap_928_pct":spy_gap_928,
            "qqq_gap_928_pct":qqq_gap_928,
            "vix_gap_928_pct":vix_gap_928,
            "spy_gap_930_pct":spy_gap_930,
            "qqq_gap_930_pct":qqq_gap_930,
            "vix_gap_930_pct":vix_gap_930,
            "open_confirmation_used":open_confirmation_used,
            "score":score,
        },
        "reasoning_points":reasoning,
    }
