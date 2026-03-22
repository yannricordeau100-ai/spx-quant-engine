
import pandas as pd
import numpy as np

def rare_edge_scan(df,features,target):

    edges=[]

    for col in features:

        vals=df[col].dropna().unique()

        for v in vals:

            sub=df[df[col]==v]

            if len(sub)<20:
                continue

            freq=len(sub)/len(df)
            ret=sub[target].mean()

            rarity=1-freq
            score=rarity*abs(ret)

            edges.append({
                "feature":col,
                "value":v,
                "freq":freq,
                "mean_return":ret,
                "edge_score":score
            })

    edges=sorted(edges,key=lambda x:x["edge_score"],reverse=True)

    return edges[:50]
