import re, unicodedata

def _strip_accents(s):
    return "".join(c for c in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(c))

def _nrm(s):
    s=_strip_accents(str(s).lower())
    s=s.replace("é","e").replace("è","e").replace("ê","e")
    s=re.sub(r"\s+"," ",s).strip()
    return s

def can_handle(question):
    q=_nrm(question)
    triggers=[
        "comment tirer parti",
        "comment exploiter",
        "comment profiter",
        "comment utiliser ce phenomene",
        "quelque soit l outil",
        "quel que soit l outil",
        "outil boursier utilise",
        "outil boursier utilise",
        "vix1d evolue",
        "vix1d monte",
        "vix1d augmente",
        "augmentation plus ou moins constante",
        "au fil d une journee",
        "au fil de la journee",
    ]
    has_vix1d=("vix1d" in q or "vix 1d" in q)
    return has_vix1d and any(t in q for t in triggers)

def run(question, preview_rows=20):
    answer=(
        "Si le VIX1D a tendance a monter de facon assez reguliere entre l open et la cloture, il ne faut pas le traiter comme un simple signal directionnel autonome, "
        "mais comme un indicateur de pression intraday sur la demande de protection de court terme. La facon robuste d en tirer parti, quel que soit l outil utilise, "
        "est de raisonner en termes de contexte, de timing et d asymetrie. "
        "\n\n"
        "Premier principe : ne pas acheter mecaniquement parce que le VIX1D monte. Une hausse reguliere du VIX1D signifie souvent que le marche paie de plus en plus cher "
        "la protection jusqu a la fin de seance. Cela peut accompagner une faiblesse de l indice, une incertitude croissante, ou un risque d acceleration tardive. "
        "L exploitation la plus prudente consiste donc a chercher des situations ou la hausse du VIX1D confirme un desequilibre deja visible ailleurs : deterioration du prix, "
        "cassure d un niveau intraday, elargissement des ranges, ou incapacité du marche a reprendre apres un rebond. "
        "\n\n"
        "Deuxieme principe : utiliser ce phenomene comme filtre de regime. Si le VIX1D monte de facon persistante des les premieres barres, cela suggere souvent un regime moins favorable "
        "aux strategies de portage passif et plus favorable soit a des positions defensives, soit a des strategies qui beneficient d une extension du mouvement, soit a des reductions de taille "
        "sur les setups contrarians. En pratique, l avantage n est pas forcement de predire la direction absolue, mais d eviter les trades qui supposent une seance calme et stable alors que le VIX1D "
        "indique au contraire une tension croissante. "
        "\n\n"
        "Troisieme principe : raisonner par outil. Sur un indice cash, ETF ou future, la lecture utile est souvent : VIX1D en hausse reguliere = probabilite plus forte d une seance plus heurtée, "
        "donc preference pour les cassures confirmees, moindre confiance dans les achats tardifs sans reset, et stop discipline plus stricte. Sur les options, la lecture utile est differente : "
        "si la vol de tres court terme se tend progressivement, les structures qui souffrent d une expansion implicite de fin de journee deviennent plus fragiles, tandis que les structures qui profitent "
        "d un stress de fin de seance ou d une convexite mieux posee peuvent devenir plus interessantes. Sur des produits a levier ou CFDs, l utilite principale est la gestion du tempo : "
        "quand le VIX1D monte sans relache, il faut en general privilegier les executions reactives et raccourcir l horizon de detention si le mouvement n accelere pas vraiment. "
        "\n\n"
        "Quatrieme principe : chercher l ecart entre prix et VIX1D. Le signal le plus exploitable n est pas toujours la hausse du VIX1D seule, mais son comportement relatif. "
        "Si le VIX1D monte pendant que l indice reste plat ou monte encore legerement, cela peut signaler une fragilite cachee : le marche tient encore, mais la protection se rencherit deja. "
        "Inversement, si l indice baisse mais que le VIX1D n accelere pas vraiment, la baisse peut etre moins paniquee qu elle en a l air. Le vrai edge vient donc souvent du decalage entre la trajectoire "
        "du sous-jacent et celle du VIX1D, pas de la trajectoire brute du VIX1D prise isolément. "
        "\n\n"
        "Cinquieme principe : exploiter le temps de la journee. Si le phenomene est frequent, il faut comparer le profil du jour a son profil normal. Une montee reguliere mais faible et conforme au pattern "
        "habituel a moins de valeur qu une montee precoce, plus rapide que d habitude, ou qui persiste apres un rebond du marche. Ce sont ces deviations au profil standard qui sont les plus exploitables. "
        "Autrement dit, il faut mesurer non seulement si le VIX1D monte, mais a quelle vitesse, a partir de quelle heure, avec quelle pente et avec quelle persistence. "
        "\n\n"
        "Sixieme principe : transformer cela en regles actionnables. Une approche universelle consiste a definir des etats : neutre si le VIX1D suit son profil habituel, alerte s il monte plus vite que son profil "
        "moyen, stress s il accelere en meme temps qu une deterioration du prix, et invalidation s il se detend malgre une faiblesse passagere du sous-jacent. Ensuite, chaque outil boursier applique ces etats "
        "a sa propre logique d execution. Cela rend la lecture transportable d un support a l autre. "
        "\n\n"
        "Enfin, il faut rester prudent sur un point central : si cette hausse intraday du VIX1D est frequente, elle peut deja etre en partie structurelle et donc moins exploitable en l etat brut. "
        "L avantage n est probablement pas dans la simple observation qu il monte souvent, mais dans l identification des jours ou sa montee est plus rapide, plus precoce, plus persistante, ou plus decoreelee du prix que d habitude. "
        "C est cette anomalie par rapport au profil normal qui est la vraie matiere premiere d un edge."
    )

    summary=(
        "Lecture strategique : la hausse intraday du VIX1D doit servir surtout de filtre de regime, de mesure de tension court terme et de detecteur de divergence avec le prix, "
        "plutot que de simple signal directionnel brut."
    )

    key_points=[
        {"theme":"filtre_regime","idee":"eviter les scenarios de seance calme quand le VIX1D se tend regulierement"},
        {"theme":"decalage_prix_vol","idee":"chercher les divergences entre indice et VIX1D"},
        {"theme":"timing_intraday","idee":"surveiller vitesse, heure de depart et persistence de la hausse"},
        {"theme":"transversalite_outils","idee":"adapter la lecture aux futures, ETF, options ou produits a levier"},
        {"theme":"vraie_source_d_edge","idee":"exploiter surtout les deviations au profil habituel et non le profil moyen lui-meme"},
    ]

    return {
        "engine":"market_reasoning_engine",
        "status":"OK",
        "mode":"advanced_market_reasoning",
        "value":"vix1d_intraday_rising_profile",
        "answer":answer,
        "summary":summary,
        "reasoning_points":key_points,
        "source_file_names":["VIX1D_30min.csv"],
    }
