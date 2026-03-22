import os,re,unicodedata,pandas as pd,numpy as np
from collections import Counter

def _nrm(s):
    s="" if s is None else str(s)
    s=s.replace("\\\\","/").strip().lower()
    s="".join(c for c in unicodedata.normalize("NFKD",s) if not unicodedata.combining(c))
    s=re.sub(r"\s+"," ",s)
    return s

def _slug(s):
    return re.sub(r"[^a-z0-9]+","_",_nrm(s)).strip("_")

def _normalize_cols(cols):
    out=[]; seen={}
    for c in cols:
        base=_slug(c) or "col"; k=base; i=2
        while k in seen:
            k=f"{base}_{i}"; i+=1
        seen[k]=1; out.append(k)
    return out

class DataValidatorEngine:
    def __init__(self, source_config, asset_aliases, session_utils):
        self.source_config=source_config
        self.asset_aliases=asset_aliases
        self.session_utils=session_utils

    def can_handle(self,q):
        nq=_nrm(q)
        keys=[
            "valide ce csv","validate this csv","validator","validation fichier","validation csv",
            "controle qualite","contrôle qualité","quality check","quality control",
            "verifie ce fichier","vérifie ce fichier","check this file"
        ]
        return any(k in nq for k in keys)

    def _read_csv_any(self,path):
        last=None
        for enc in ("utf-8-sig","utf-8","cp1252","latin-1"):
            try:
                with open(path,"r",encoding=enc,errors="replace") as f:
                    lines=[x for x in f.read().splitlines() if str(x).strip()!=""]
                if not lines:
                    continue
                header=lines[0]
                best_sep=None; best_n=1
                for sep in [",",";","\\t","|"]:
                    n=len(header.split(sep))
                    if n>best_n:
                        best_n=n; best_sep=sep
                if best_sep and best_n>1:
                    cols=[x.strip() for x in header.split(best_sep)]
                    rows=[]
                    for line in lines[1:]:
                        parts=[x.strip() for x in line.split(best_sep)]
                        if len(parts)==len(cols):
                            rows.append(parts)
                    if len(rows)>=max(10,int(len(lines)*0.4)):
                        df=pd.DataFrame(rows,columns=cols)
                        df.columns=_normalize_cols(df.columns)
                        return df, enc, best_sep
                for sep in (",",";","\\t","|",None):
                    try:
                        kw={"encoding":enc,"on_bad_lines":"skip"}
                        if sep is None:
                            df=pd.read_csv(path,sep=None,engine="python",**kw)
                        else:
                            df=pd.read_csv(path,sep=sep,engine="python",**kw)
                        if df is not None and df.shape[1]>=1:
                            df.columns=_normalize_cols(df.columns)
                            return df, enc, ("auto" if sep is None else sep)
                    except Exception as e:
                        last=e
            except Exception as e:
                last=e
        raise last

    def _dominant_minutes(self,ts):
        if len(ts)<2:
            return None
        diffs=((ts.sort_values().diff().dropna().dt.total_seconds()/60.0).round(6)).tolist()
        diffs=[x for x in diffs if x>0]
        if not diffs:
            return None
        c=Counter(diffs)
        return c.most_common(1)[0][0]

    def _quality_flags(self,df):
        tcol=self.session_utils.detect_time_col(df)
        open_col=self.session_utils.first_match(df.columns,["open"])
        high_col=self.session_utils.first_match(df.columns,["high"])
        low_col=self.session_utils.first_match(df.columns,["low"])
        close_col=self.session_utils.first_match(df.columns,["close"])

        out={}
        out["time_col"]=tcol
        out["open_col"]=open_col
        out["high_col"]=high_col
        out["low_col"]=low_col
        out["close_col"]=close_col
        out["has_ohlc"]=all([open_col,high_col,low_col,close_col])

        if tcol is None:
            out["time_parse_ok"]=False
            out["sorted_ok"]=False
            out["duplicate_timestamps"]=None
            out["dominant_freq_minutes"]=None
            return out

        s=df[tcol].astype(str)
        dt=pd.to_datetime(s,errors="coerce",format="%Y-%m-%d %H:%M:%S")
        if dt.notna().sum()==0:
            dt=pd.to_datetime(s,errors="coerce",format="%Y-%m-%d")
        if dt.notna().sum()==0:
            dt=pd.to_datetime(s,errors="coerce")
        out["time_parse_ok"]=bool(dt.notna().sum()>0)

        x=dt.dropna()
        if len(x)==0:
            out["sorted_ok"]=False
            out["duplicate_timestamps"]=None
            out["dominant_freq_minutes"]=None
            return out

        out["sorted_ok"]=bool(x.is_monotonic_increasing)
        out["duplicate_timestamps"]=int(x.duplicated().sum())
        out["dominant_freq_minutes"]=self._dominant_minutes(x)

        if out["has_ohlc"]:
            tmp=df.copy()
            tmp[open_col]=pd.to_numeric(tmp[open_col],errors="coerce")
            tmp[high_col]=pd.to_numeric(tmp[high_col],errors="coerce")
            tmp[low_col]=pd.to_numeric(tmp[low_col],errors="coerce")
            tmp[close_col]=pd.to_numeric(tmp[close_col],errors="coerce")
            valid_num=tmp[[open_col,high_col,low_col,close_col]].notna().all(axis=1)
            ohlc_bad=((tmp[high_col] < tmp[[open_col,close_col,low_col]].max(axis=1)) | (tmp[low_col] > tmp[[open_col,close_col,high_col]].min(axis=1)))
            out["ohlc_complete_rows"]=int(valid_num.sum())
            out["ohlc_bad_rows"]=int((valid_num & ohlc_bad).sum())
        else:
            out["ohlc_complete_rows"]=None
            out["ohlc_bad_rows"]=None

        return out

    def run(self,q,preview_rows=20):
        nq=_nrm(q)

        upload_dir=os.path.join(os.path.dirname(os.path.dirname(__file__)) if "__file__" in globals() else "", "RAW_SOURCES", "Autres Actions Upload")
        candidates=[]

        m=re.search(r'([A-Z]{1,10}(?:_[A-Z0-9]+)*)\.csv', q)
        if m:
            candidates.append(m.group(1)+".csv")

        if "aapl" in nq:
            candidates += ["AAPL_daily.csv","AAPL.csv"]

        if os.path.isdir(upload_dir):
            for f in sorted(os.listdir(upload_dir)):
                if f.lower().endswith(".csv"):
                    candidates.append(f)

        candidates=list(dict.fromkeys(candidates))
        if not candidates:
            return {"status":"NO_CSV_CANDIDATE_FOUND","answer_type":"explanation"}

        chosen=None
        for f in candidates:
            p=os.path.join(upload_dir,f)
            if os.path.exists(p):
                chosen=p
                break

        if chosen is None:
            return {"status":"CSV_NOT_FOUND_IN_DROPZONE","answer_type":"explanation","candidates":candidates}

        df, enc, sep = self._read_csv_any(chosen)
        flags=self._quality_flags(df)

        issues=[]
        if flags["time_col"] is None:
            issues.append("missing_time_col")
        if flags["time_parse_ok"] is False:
            issues.append("time_parse_failed")
        if flags["sorted_ok"] is False:
            issues.append("timestamps_not_sorted")
        if flags["duplicate_timestamps"] not in (None,0):
            issues.append("duplicate_timestamps")
        if flags["ohlc_bad_rows"] not in (None,0):
            issues.append("ohlc_bad_rows")

        status="OK" if len(issues)==0 else "OK_WITH_ISSUES"
        preview=df.head(preview_rows).to_dict("records")

        return {
            "status":status,
            "answer_type":"table",
            "file_path":chosen,
            "rows":int(len(df)),
            "cols":int(df.shape[1]),
            "encoding":enc,
            "separator":sep,
            "issues":issues,
            "summary":flags,
            "value":int(len(issues)),
            "preview":preview
        }
