import re, unicodedata

#1/33 helpers
def _nrm(s):
    s="" if s is None else str(s)
    s=s.strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _contains_any(text, arr):
    return any(x in text for x in arr)

#2/33 assets and aliases
ASSET_ALIASES={
    "SPX":[["spx"],["s&p","500"],["s&p500"]],
    "SPY":[["spy"]],
    "QQQ":[["qqq"]],
    "IWM":[["iwm"]],
    "VIX":[["vix"]],
    "VVIX":[["vvix"]],
    "DXY":[["dxy"],["dollar","index"]],
    "US10Y":[["10","ans","us"],["10y"],["us10y"],["taux","us","10","ans"],["us","10","years"]],
    "CALENDAR":[["calendar"],["macro"],["calendar","macro"],["macro","calendar"],["quiet","day"],["low","activity"]],
    "AAPL":[["aapl"],["apple"]],
    "NVDA":[["nvda"],["nvidia"]],
    "MSFT":[["msft"],["microsoft"]],
    "AMZN":[["amzn"],["amazon"]],
    "META":[["meta"]],
    "TSLA":[["tsla"],["tesla"]],
}

LOGIC_WORDS={
    "AND":{"and","et","with","avec","&&","&","+"},
    "OR":{"or","ou","et/ou","and/or","||","|"},
    "NOT":{"not","without","sans","sauf","but","!","minus"},
}

STOPWORDS={"si","alors","est","ce","que","comme","cela","tel","quel","dans","les","prochains","jours","du","de","la","le","des","a","à","l","d","apres","après","ouverture","cloture","clôture","demain","lendemain"}

#3/33 time semantics
def parse_market_time_semantics(q):
    nq=_nrm(q)
    out={"market_anchor":None,"timing_mode":None,"minutes_after_open":None,"target_window_label":None,"close_reference":False}
    if _contains_any(nq,["avant l'ouverture","avant ouverture","before open","pre-open"]):
        out["market_anchor"]="open"; out["timing_mode"]="before_open"; out["target_window_label"]="before_open"
        return out
    if _contains_any(nq,["a l'ouverture","a l ouverture","à l'ouverture","à l ouverture","ouverture des marches","ouverture du marche"]):
        out["market_anchor"]="open"; out["timing_mode"]="at_open"; out["target_window_label"]="at_open"
    m=re.search(r"(\d+)\s*(min|minute|minutes)\s*(apres|après|after)",nq)
    if m:
        mins=int(m.group(1))
        out["market_anchor"]="open"; out["timing_mode"]="minutes_after_open"; out["minutes_after_open"]=mins; out["target_window_label"]=f"{mins}m_after_open"
        return out
    m2=re.search(r"(\d+)\s*(h|heure|heures)\s*(apres|après|after)",nq)
    if m2:
        mins=int(m2.group(1))*60
        out["market_anchor"]="open"; out["timing_mode"]="minutes_after_open"; out["minutes_after_open"]=mins; out["target_window_label"]=f"{mins}m_after_open"
        return out
    if _contains_any(nq,["d'ici la cloture","d'ici la clôture","jusqu'a la cloture","jusqu'a la clôture","to close","by the close"]):
        out["market_anchor"]="close"; out["timing_mode"]="to_close"; out["target_window_label"]="to_close"; out["close_reference"]=True
        return out
    if out["timing_mode"] is None and _contains_any(nq,["ouverture","open"]):
        out["market_anchor"]="open"; out["timing_mode"]="at_open"; out["target_window_label"]="at_open"
    return out

#4/33 horizons
def parse_horizon_semantics(q):
    nq=_nrm(q)
    horizons=[]
    mapping=[
        ("next_day",["lendemain","demain","next day","tomorrow"]),
        ("j3",["j+3","a j+3","a j 3","3 jours","3 days"]),
        ("j5",["j+5","a j+5","a j 5","5 jours","5 days"]),
        ("next_days",["prochains jours","next days","dans les prochains jours"]),
        ("to_close",["d'ici la cloture","d'ici la clôture","to close","by the close"]),
    ]
    for code,keys in mapping:
        if any(k in nq for k in keys):
            horizons.append(code)
    if not horizons:
        if "ouverture" in nq or "open" in nq:
            horizons=["at_open"]
        else:
            horizons=["next_days_unspecified"]
    return {"horizons":horizons,"primary_horizon":horizons[0] if horizons else None}

#5/33 tokenizer
def _tokenize(q):
    txt=_nrm(q)
    txt=txt.replace("(", " ( ").replace(")", " ) ")
    txt=txt.replace("&&"," && ").replace("||"," || ").replace("!"," ! ").replace("&"," & ").replace("|"," | ").replace("+"," + ")
    txt=re.sub(r"\s+"," ",txt).strip()
    toks=txt.split(" ")
    return [t for t in toks if t!=""]

#6/33 asset matcher
def _match_asset(tokens, i):
    best=None
    best_len=0
    for asset,alias_lists in ASSET_ALIASES.items():
        for alias in alias_lists:
            n=len(alias)
            if n<=0 or i+n>len(tokens):
                continue
            if tokens[i:i+n]==alias and n>best_len:
                best=(asset,n)
                best_len=n
    return best

#7/33 logic matcher
def _match_logic(tok):
    for op,variants in LOGIC_WORDS.items():
        if tok in variants:
            return op
    return None

#8/33 cross-asset detection
def parse_cross_asset_semantics(q):
    toks=_tokenize(q)
    assets=[]
    i=0
    while i<len(toks):
        hit=_match_asset(toks,i)
        if hit is not None:
            asset,n=hit
            assets.append(asset)
            i+=n
            continue
        i+=1
    assets=list(dict.fromkeys(assets))
    return {
        "assets_detected":assets,
        "cross_asset_count":len(assets),
        "is_cross_asset":len(assets)>=2,
        "is_three_way_or_more":len(assets)>=3,
        "is_four_way_or_more":len(assets)>=4,
        "calendar_involved":"CALENDAR" in assets,
    }

#9/33 question type
def parse_question_type(q):
    nq=_nrm(q)
    if _contains_any(nq,["si "," alors ","et/ou","and/or","with","without","sans ","avec "," or "," and ","(",")","not "]):
        return "conditional_multi_asset"
    if _contains_any(nq,["relation","lien","correlation","rapport"]):
        return "relation"
    if _contains_any(nq,["pattern","schema","schéma","setup"]):
        return "pattern"
    if _contains_any(nq,["ouverture","open","cloture","clôture"]):
        return "market_window"
    return "generic_custom_research"

#10/33 target split
def split_assets_target_drivers(q, assets):
    nq=_nrm(q)
    if not assets:
        return {"target_asset":None,"driver_assets":[]}
    if " alors " in (" "+nq+" "):
        right=nq.split(" alors ",1)[1]
        for a in assets:
            if a.lower() in right.lower():
                return {"target_asset":a,"driver_assets":[x for x in assets if x!=a]}
    if "est ce que" in nq:
        right=nq.split("est ce que",1)[1]
        for a in assets:
            if a.lower() in right.lower():
                return {"target_asset":a,"driver_assets":[x for x in assets if x!=a]}
    non_calendar=[a for a in assets if a!="CALENDAR"]
    if non_calendar:
        target=non_calendar[-1]
        return {"target_asset":target,"driver_assets":[x for x in assets if x!=target]}
    return {"target_asset":assets[-1],"driver_assets":assets[:-1]}

#11/33 logic tokenization with dedup-safe asset extraction
def tokenize_logic_expression(q, driver_assets):
    toks=_tokenize(q)
    allowed=set(driver_assets or [])
    out=[]
    i=0
    while i<len(toks):
        tok=toks[i]
        if tok=="(":
            out.append("("); i+=1; continue
        if tok==")":
            out.append(")"); i+=1; continue
        op=_match_logic(tok)
        if op is not None:
            if not out and op in {"AND","OR"}:
                i+=1
                continue
            out.append(op); i+=1; continue
        hit=_match_asset(toks,i)
        if hit is not None:
            asset,n=hit
            if (not allowed) or (asset in allowed):
                if not out:
                    out.append(("ASSET",asset))
                else:
                    prev=out[-1]
                    if isinstance(prev,tuple) and prev[0]=="ASSET" and prev[1]==asset:
                        pass
                    elif prev=="NOT":
                        out.append(("ASSET",asset))
                    elif prev==")":
                        out.append("AND"); out.append(("ASSET",asset))
                    elif isinstance(prev,tuple) and prev[0]=="ASSET":
                        out.append("AND"); out.append(("ASSET",asset))
                    else:
                        out.append(("ASSET",asset))
            i+=n
            continue
        i+=1

    # cleanup repeated operators / malformed edges
    cleaned=[]
    for tok in out:
        if not cleaned:
            if tok in {"AND","OR"}:
                continue
            cleaned.append(tok)
            continue
        prev=cleaned[-1]
        if tok in {"AND","OR"} and prev in {"AND","OR","NOT","("}:
            continue
        if tok=="NOT" and prev not in {"AND","OR","(","NOT"}:
            cleaned.append("AND")
        if tok==")" and prev in {"AND","OR","NOT","("}:
            continue
        cleaned.append(tok)

    while cleaned and cleaned[-1] in {"AND","OR","NOT"}:
        cleaned.pop()

    return cleaned

#12/33 shunting-yard
PRECEDENCE={"NOT":3,"AND":2,"OR":1}
ASSOC={"NOT":"right","AND":"left","OR":"left"}

def to_postfix(tokens):
    out=[]; stack=[]
    for tok in tokens:
        if isinstance(tok,tuple) and tok[0]=="ASSET":
            out.append(tok)
        elif tok in PRECEDENCE:
            while stack and stack[-1] in PRECEDENCE:
                top=stack[-1]
                if (ASSOC[tok]=="left" and PRECEDENCE[tok] <= PRECEDENCE[top]) or (ASSOC[tok]=="right" and PRECEDENCE[tok] < PRECEDENCE[top]):
                    out.append(stack.pop())
                else:
                    break
            stack.append(tok)
        elif tok=="(":
            stack.append(tok)
        elif tok==")":
            while stack and stack[-1]!="(":
                out.append(stack.pop())
            if stack and stack[-1]=="(":
                stack.pop()
    while stack:
        if stack[-1]!="(":
            out.append(stack.pop())
        else:
            stack.pop()
    return out

#13/33 postfix -> tree
def postfix_to_tree(postfix):
    st=[]
    for tok in postfix:
        if isinstance(tok,tuple) and tok[0]=="ASSET":
            st.append({"type":"ASSET","value":tok[1]})
        elif tok=="NOT":
            if len(st)>=1:
                a=st.pop()
                st.append({"type":"NOT","child":a})
        elif tok in {"AND","OR"}:
            if len(st)>=2:
                b=st.pop(); a=st.pop()
                st.append({"type":tok,"left":a,"right":b})
    return st[-1] if st else None

#14/33 tree dedup / simplify
def _tree_key(node):
    if node is None:
        return "NONE"
    t=node.get("type")
    if t=="ASSET":
        return f"A:{node.get('value')}"
    if t=="NOT":
        return f"N({ _tree_key(node.get('child')) })"
    if t in {"AND","OR"}:
        l=_tree_key(node.get("left"))
        r=_tree_key(node.get("right"))
        if l>r:
            l,r=r,l
        return f"{t}({l},{r})"
    return str(node)

def simplify_logic_tree(node):
    if node is None:
        return None
    t=node.get("type")
    if t=="ASSET":
        return node
    if t=="NOT":
        child=simplify_logic_tree(node.get("child"))
        if child is None:
            return None
        if child.get("type")=="NOT":
            return simplify_logic_tree(child.get("child"))
        return {"type":"NOT","child":child}
    if t in {"AND","OR"}:
        left=simplify_logic_tree(node.get("left"))
        right=simplify_logic_tree(node.get("right"))
        if left is None:
            return right
        if right is None:
            return left
        if _tree_key(left)==_tree_key(right):
            return left
        # A AND NOT(A) or A OR A-like cases not fully reduced here, keep safe
        if _tree_key(left)>_tree_key(right):
            left,right=right,left
        return {"type":t,"left":left,"right":right}
    return node

#15/33 operators/forms
def detect_logic_operators(tokens):
    ops=[]
    for tok in tokens:
        if tok in {"AND","OR","NOT"}:
            ops.append(tok)
    return sorted(set(ops))

def detect_logic_forms(tokens):
    ops=detect_logic_operators(tokens)
    has_and="AND" in ops; has_or="OR" in ops; has_not="NOT" in ops
    if has_and and has_or and has_not: return ["AND_OR_NOT"]
    if has_and and has_or: return ["AND_OR"]
    if has_and and has_not: return ["AND_NOT"]
    if has_or and has_not: return ["OR_NOT"]
    if has_and: return ["AND"]
    if has_or: return ["OR"]
    if has_not: return ["NOT"]
    return ["IMPLICIT_AND"]

#16/33 branch listing for follow-up
def _list_branches(node):
    if node is None:
        return []
    t=node.get("type")
    if t=="ASSET":
        return [node.get("value")]
    if t=="NOT":
        return [f"NOT({x})" for x in _list_branches(node.get("child"))]
    if t in {"AND","OR"}:
        return _list_branches(node.get("left")) + _list_branches(node.get("right"))
    return []

#17/33 main
def parse_query_semantics(q):
    out={}
    cross=parse_cross_asset_semantics(q)
    out["question_type"]=parse_question_type(q)
    out.update(parse_market_time_semantics(q))
    out.update(parse_horizon_semantics(q))
    out.update(cross)
    out.update(split_assets_target_drivers(q,cross.get("assets_detected",[])))
    tokens=tokenize_logic_expression(q,out.get("driver_assets",[]))
    postfix=to_postfix(tokens)
    tree=postfix_to_tree(postfix)
    tree=simplify_logic_tree(tree)
    out["logic_tokens"]=tokens
    out["logic_postfix"]=postfix
    out["logic_tree"]=tree
    out["logic_tree_branches"]=_list_branches(tree)
    out["logic_operators"]=detect_logic_operators(tokens)
    out["logic_forms"]=detect_logic_forms(tokens)
    out["parser_fix_version"]="ETAPE166B"
    return out
