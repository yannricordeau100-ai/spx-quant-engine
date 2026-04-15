# spx_intraday.py — Sessions intraday SPX/SPY, amplitude, RIC/IC

import pandas as pd
import numpy as np
import gc
from pathlib import Path
from datetime import time as dtime

DATA_DIR = Path(__file__).parent / "data" / "live_selected"

RIC_THRESHOLD = 0.45
IC_THRESHOLD = 0.23
ENTRY_POINTS = {"9h30": dtime(9, 30), "10h00": dtime(10, 0), "10h30": dtime(10, 30)}
HORIZONS_MIN = [30, 60, 90, 120, 150, 180, 240, 300, 390]

_CACHE: dict = {}


def _to_num(s):
    return pd.to_numeric(s.astype(str).str.replace(",", ".").str.strip(), errors="coerce")


def _load_intraday(symbol: str, freq: str) -> pd.DataFrame | None:
    key = f"{symbol}_{freq}"
    if key in _CACHE:
        return _CACHE[key]
    for p in [DATA_DIR / f"{symbol}_{freq}.csv", DATA_DIR / f"{symbol.upper()}_{freq}.csv"]:
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p, sep=";")
            df.columns = [c.strip().lower().replace(" ", "_").replace("#", "").strip() for c in df.columns]
            df["time"] = pd.to_datetime(df["time"].astype(str).str.strip(), errors="coerce")
            df = df.dropna(subset=["time"]).copy()
            if df["time"].dt.hour.median() >= 13:
                df["time"] = df["time"] - pd.Timedelta(hours=6)
            df = df.set_index("time").sort_index()
            for col in df.columns:
                df[col] = _to_num(df[col])
            _CACHE[key] = df
            return df
        except Exception as e:
            print(f"[intraday] load error {p.name}: {e}", flush=True)
    return None


def _trading(df, start=dtime(9, 30), end=dtime(16, 0)):
    return df[(df.index.time >= start) & (df.index.time <= end)]


def build_sessions_from_entry(entry_point: str = "9h30", symbol: str = "SPY",
                               freq: str = "30min") -> pd.DataFrame:
    entry_t = ENTRY_POINTS.get(entry_point, dtime(9, 30))
    df = _load_intraday(symbol, freq)
    if df is None:
        return pd.DataFrame()
    df_tr = _trading(df)
    sessions = []
    for date, grp in df_tr.groupby(df_tr.index.date):
        grp = grp.sort_index()
        entry_bars = grp[grp.index.time >= entry_t]
        if entry_bars.empty:
            continue
        ep = float(entry_bars.iloc[0]["open"])
        entry_ts = entry_bars.index[0]
        row = {"date": pd.Timestamp(date), "entry_point": entry_point, "entry_price": ep}
        for h in HORIZONS_MIN:
            tgt = entry_ts + pd.Timedelta(minutes=h)
            win = grp[(grp.index >= entry_ts) & (grp.index <= tgt)]
            if win.empty:
                continue
            price = float(win.iloc[-1]["close"])
            abs_pct = abs((price - ep) / ep * 100)
            abs_pts = abs(price - ep)
            lbl = f"{h}min" if h < 390 else "close"
            row[f"abs_ret_{lbl}_pct"] = round(abs_pct, 4)
            row[f"abs_ret_{lbl}_pts"] = round(abs_pts, 2)
            row[f"ret_{lbl}_pct"] = round((price - ep) / ep * 100, 4)
            row[f"ric_ok_{lbl}"] = abs_pct >= RIC_THRESHOLD
            if h >= 120 and "high" in win.columns and "low" in win.columns:
                mx = max(abs(float(win["high"].max()) - ep), abs(float(win["low"].min()) - ep)) / ep * 100
                row[f"ic_ok_{lbl}"] = mx <= IC_THRESHOLD
        sessions.append(row)
    if not sessions:
        return pd.DataFrame()
    r = pd.DataFrame(sessions).set_index("date").sort_index()
    r["prev_close"] = r["entry_price"].shift(1)
    r["gap_pct"] = (r["entry_price"] - r["prev_close"]) / r["prev_close"] * 100
    gc.collect()
    return r


def analyze_amplitude_stats(sessions: pd.DataFrame, entry_point: str) -> dict:
    if sessions.empty:
        return {"entry_point": entry_point, "n_sessions": 0, "rows": []}
    rows = []
    for h in HORIZONS_MIN:
        lbl = f"{h}min" if h < 390 else "close"
        col_pct = f"abs_ret_{lbl}_pct"
        col_pts = f"abs_ret_{lbl}_pts"
        ric = f"ric_ok_{lbl}"
        ic = f"ic_ok_{lbl}"
        if col_pct not in sessions.columns:
            continue
        vals = sessions[col_pct].dropna()
        if len(vals) < 10:
            continue
        row = {"Horizon": lbl, "N": len(vals),
               "Amp. moy (%)": round(float(vals.mean()), 3),
               "Amp. méd (%)": round(float(vals.median()), 3)}
        if col_pts in sessions.columns:
            row["Amp. moy (pts)"] = round(float(sessions[col_pts].dropna().mean()), 1)
        if ric in sessions.columns:
            row[f"≥{RIC_THRESHOLD}% (RIC)"] = f"{sessions[ric].mean() * 100:.1f}%"
        if ic in sessions.columns:
            row[f"≤{IC_THRESHOLD}% (IC)"] = f"{sessions[ic].mean() * 100:.1f}%"
        rows.append(row)
    return {"entry_point": entry_point, "n_sessions": len(sessions), "rows": rows}


def get_intraday_features_for_entry(entry_point: str, dates: pd.DatetimeIndex) -> pd.DataFrame:
    features = pd.DataFrame(index=dates)
    df_fut = _load_intraday("SPX_FUTURE", "30min")
    df_spy = _load_intraday("SPY", "30min")

    # ── OVERNIGHT FUTURES (all entry points) ──
    if df_fut is not None:
        for date in dates:
            prev = date - pd.Timedelta(days=3)
            night = df_fut[
                (df_fut.index.date >= prev.date()) & (df_fut.index.date <= date.date()) &
                ((df_fut.index.time > dtime(16, 0)) | (df_fut.index.time <= dtime(9, 30)))
            ]
            night = night[night.index < pd.Timestamp(date) + pd.Timedelta(hours=9, minutes=31)]
            if night.empty or len(night) < 2:
                continue
            on_o, on_c = float(night.iloc[0]["open"]), float(night.iloc[-1]["close"])
            if on_o > 0:
                on_ret = (on_c - on_o) / on_o * 100
                features.loc[date, "fut_on_ret_pct"] = on_ret
                features.loc[date, "fut_on_dir"] = 1 if on_ret > 0 else -1
                features.loc[date, "fut_on_abs_ret"] = abs(on_ret)
            if "high" in night.columns and "low" in night.columns:
                h, l = float(night["high"].max()), float(night["low"].min())
                if l > 0:
                    features.loc[date, "fut_on_range_pct"] = (h - l) / l * 100
            if "volume" in night.columns:
                features.loc[date, "fut_on_volume"] = float(night["volume"].sum())
            if "rsi" in night.columns:
                rv = night["rsi"].dropna()
                if not rv.empty:
                    features.loc[date, "fut_on_rsi_last"] = float(rv.iloc[-1])
            lb = night.iloc[-1]
            features.loc[date, "fut_last_close"] = float(lb["close"])
            if float(lb["open"]) > 0:
                features.loc[date, "fut_last_bar_ret"] = (float(lb["close"]) - float(lb["open"])) / float(lb["open"]) * 100
            mid = len(night) // 2
            if mid > 0:
                f_ret = (float(night.iloc[mid - 1]["close"]) - float(night.iloc[0]["open"])) / float(night.iloc[0]["open"]) * 100
                s_ret = (float(night.iloc[-1]["close"]) - float(night.iloc[mid]["open"])) / float(night.iloc[mid]["open"]) * 100
                features.loc[date, "fut_on_first_half"] = f_ret
                features.loc[date, "fut_on_second_half"] = s_ret
                features.loc[date, "fut_on_accel"] = s_ret - f_ret
            features.loc[date, "fut_on_n_bars"] = len(night)
        gc.collect()

    # ── BARRES INTRADAY SPY ──
    bars_to_load = []
    if entry_point == "10h00":
        bars_to_load = [dtime(9, 30)]
    elif entry_point == "10h30":
        bars_to_load = [dtime(9, 30), dtime(10, 0)]

    if df_spy is not None and bars_to_load:
        df_tr = _trading(df_spy)
        for bt in bars_to_load:
            tl = f"{bt.hour}h{bt.minute:02d}" if bt.minute else f"{bt.hour}h"
            for date in dates:
                bar = df_tr[(df_tr.index.date == date.date()) & (df_tr.index.time == bt)]
                if bar.empty:
                    continue
                b = bar.iloc[0]
                for col in bar.columns:
                    cn = col.lower().strip().replace(" ", "_").replace("#", "").replace("-", "_").strip("_")
                    if cn in ("time", "date"):
                        continue
                    if pd.notna(b[col]):
                        features.loc[date, f"spy_{tl}_{cn}"[:45]] = float(b[col])
                if "open" in bar.columns and "close" in bar.columns:
                    o, c = float(b["open"]), float(b["close"])
                    if o > 0:
                        features.loc[date, f"spy_{tl}_bar_amp"] = abs(c - o) / o * 100
                        features.loc[date, f"spy_{tl}_bar_dir"] = 1 if c > o else -1
                    if "high" in bar.columns and "low" in bar.columns:
                        h, l = float(b["high"]), float(b["low"])
                        if h > l:
                            features.loc[date, f"spy_{tl}_close_pos"] = (c - l) / (h - l)

        if entry_point == "10h30":
            for date in dates:
                win = df_tr[(df_tr.index.date == date.date()) &
                            (df_tr.index.time >= dtime(9, 30)) &
                            (df_tr.index.time <= dtime(10, 0))]
                if len(win) < 2:
                    continue
                o930, c1000 = float(win.iloc[0]["open"]), float(win.iloc[-1]["close"])
                if o930 > 0:
                    features.loc[date, "spy_cum_ret"] = (c1000 - o930) / o930 * 100
                    features.loc[date, "spy_cum_amp"] = abs(c1000 - o930) / o930 * 100
        gc.collect()

    n_on = sum(1 for c in features.columns if "fut_" in c)
    n_spy = sum(1 for c in features.columns if "spy_" in c)
    print(f"[intraday_features/{entry_point}] overnight:{n_on} spy:{n_spy} total:{len(features.columns)}", flush=True)
    return features


def find_best_entry_time(symbol: str = "SPY") -> list:
    sessions = build_sessions_from_entry("9h30", symbol)
    if sessions.empty:
        return []
    results = []
    for h in HORIZONS_MIN:
        lbl = f"{h}min" if h < 390 else "close"
        col = f"abs_ret_{lbl}_pct"
        if col not in sessions.columns:
            continue
        vals = sessions[col].dropna()
        if len(vals) < 10:
            continue
        results.append({
            "horizon": lbl, "amp_moy_pct": round(float(vals.mean()), 3),
            "amp_med_pct": round(float(vals.median()), 3),
            "ric_pct": round(float((vals >= RIC_THRESHOLD).mean() * 100), 1),
            "n": len(vals),
        })
    results.sort(key=lambda r: r["amp_moy_pct"], reverse=True)
    return results


def analyze_overnight(days_back=None):
    df_fut = _load_intraday("SPX_FUTURE", "30min")
    df_spy = _load_intraday("SPY", "30min")
    if df_fut is None or df_spy is None:
        return pd.DataFrame()
    on = df_fut[(df_fut.index.time > dtime(16, 0)) | (df_fut.index.time < dtime(9, 30))]
    spy_tr = _trading(df_spy)
    rows = []
    for date, grp in on.groupby(on.index.date):
        nxt = pd.Timestamp(date) + pd.Timedelta(days=1)
        nxt_d = spy_tr[spy_tr.index.date == nxt.date()]
        if nxt_d.empty or len(grp) < 2:
            continue
        on_ret = (float(grp.iloc[-1]["close"]) - float(grp.iloc[0]["open"])) / float(grp.iloc[0]["open"]) * 100
        d_op = float(nxt_d.iloc[0]["open"])
        d_cl = float(nxt_d.iloc[-1]["close"])
        rows.append({"date": nxt, "overnight_ret": round(on_ret, 3),
                      "overnight_direction": 1 if on_ret > 0 else -1,
                      "day_ret": round((d_cl - d_op) / d_op * 100, 3)})
    gc.collect()
    if not rows:
        return pd.DataFrame()
    df_out = pd.DataFrame(rows).set_index("date").sort_index()
    return df_out.iloc[-days_back:] if days_back else df_out
