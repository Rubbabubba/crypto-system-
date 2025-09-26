# strategies/c1.py — v1.9.2
# Crypto analog of S1 (VWAP + EMA slope + sigma band) with stateless exits.
# /scan route signature: run(df_map, params, positions) -> List[{symbol, action, reason}]
#
# ENTRY (flat):
#   Reversion: crossed up VWAP AND ema_slope >= min
#   OR
#   Trend: close > VWAP AND ema_slope >= min AND "recent touch" of lower VWAP sigma band
#
# EXIT (have):
#   EMA cross-down on bar close (close <= EMA)
#
# Notes:
#   • Stateless (no persisted trail). Clean for /scan.
#   • Defaults tuned for 5Min, but timeframe is controlled by your router/bar fetcher.

from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd
import numpy as np

NAME = "c1"
VERSION = "1.9.2"

# ---------- helpers ----------
def _ok_df(df: pd.DataFrame) -> bool:
    need = {"open","high","low","close","volume"}
    return isinstance(df, pd.DataFrame) and need.issubset(df.columns) and len(df) >= 60

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=int(n), adjust=False).mean()

def _vwap(df: pd.DataFrame) -> pd.Series:
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].replace(0, np.nan)
    vwap = (tp * vol).cumsum() / vol.cumsum()
    return vwap.fillna(method="ffill").fillna(df["close"])

def _zscore(series: pd.Series, window: int) -> pd.Series:
    r = series.rolling(window)
    m = r.mean()
    sd = r.std(ddof=0)
    return (series - m) / sd.replace(0, np.nan)

def _cross_dn(x_prev, x_now, y_prev, y_now) -> bool:
    return (x_prev > y_prev) and (x_now <= y_now)

def _in_pos(positions: Dict[str, float], sym: str) -> bool:
    try:
        return float(positions.get(sym, 0.0)) > 0.0
    except Exception:
        return False

# ---------- main ----------
def run(df_map: Dict[str, pd.DataFrame], params: Dict[str, Any], positions: Dict[str, float]) -> List[Dict[str, Any]]:
    ema_len         = int(params.get("ema_len", 20))
    ema_slope_min   = float(params.get("ema_slope_min", 0.0))
    vwap_sigma      = float(params.get("vwap_sigma", 1.0))
    band_lookback   = int(params.get("band_lookback", 10))
    # Everything else (timeframe, notional) is handled by the router/engine.

    out: List[Dict[str, Any]] = []

    for sym, df in df_map.items():
        if not _ok_df(df):
            out.append({"symbol": sym, "action": "flat", "reason": "insufficient_data"})
            continue

        df = df.sort_index()
        if df.tail(2).isna().any().any():
            out.append({"symbol": sym, "action": "flat", "reason": "nan_tail"})
            continue

        # Indicators
        ema = _ema(df["close"], ema_len)
        ema_slope = ema - ema.shift(1)
        vwap = _vwap(df)
        dev = df["close"] - vwap
        z = _zscore(dev, max(20, ema_len))
        lower_touch = z <= -abs(vwap_sigma)

        # Compute recent "touch" state from the last window (stateless)
        idx_last = len(df) - 1
        start = max(0, idx_last - band_lookback)
        touched_recent = bool(lower_touch.iloc[start:idx_last+1].any())

        # Crosses / comparisons (prev vs now)
        c_prev, c_now = df["close"].iloc[-2], df["close"].iloc[-1]
        vw_prev, vw_now = vwap.iloc[-2], vwap.iloc[-1]
        ema_prev, ema_now = ema.iloc[-2], ema.iloc[-1]
        slope_now = ema_slope.iloc[-1]

        crossed_up = (c_prev < vw_prev) and (c_now > vw_now)
        ema_slope_ok = (slope_now >= ema_slope_min)
        trend_ok = (c_now > vw_now) and ema_slope_ok and touched_recent

        have = _in_pos(positions, sym)

        # Exits first
        if have:
            if _cross_dn(c_prev, c_now, ema_prev, ema_now):
                out.append({"symbol": sym, "action": "sell", "reason": "ema_cross_down"})
                continue
            out.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        # Entries
        if (crossed_up and ema_slope_ok) or trend_ok:
            reason = "entry_reversion" if (crossed_up and ema_slope_ok) else "entry_trend"
            out.append({"symbol": sym, "action": "buy", "reason": reason})
        else:
            why = []
            if not (crossed_up or trend_ok): why.append("no_vwap_signal")
            if not ema_slope_ok: why.append("slope_fail")
            out.append({"symbol": sym, "action": "flat", "reason": " & ".join(why) if why else "no_signal"})

    return out
