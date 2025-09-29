# strategies/c4.py — v1.9.4
# Volume “bubble” breakout. Entry on break above bubble high with volume context; exit on failed breakout or ATR stop.
from __future__ import annotations
from typing import Dict, Any, List
import numpy as np
import pandas as pd

NAME = "c4"
VERSION = "1.9.4"

NEED_COLS = {"open","high","low","close","volume"}

def _ok_df(df: pd.DataFrame, need_len: int = 120) -> bool:
    return (
        isinstance(df, pd.DataFrame) and
        NEED_COLS.issubset(df.columns) and
        len(df) >= need_len and
        not df.tail(2).isna().any().any()
    )

def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h-l), (h-prev_c).abs(), (l-prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(int(n)).mean()

def _in_pos(positions: Dict[str, float], sym: str) -> bool:
    try: return float(positions.get(sym, 0.0)) > 0.0
    except Exception: return False

def run(df_map: Dict[str, pd.DataFrame], params: Dict[str, Any], positions: Dict[str, float]) -> List[Dict[str, Any]]:
    bars_per_bubble = int(params.get("bars_per_bubble", 60))
    min_total_vol   = float(params.get("min_total_vol", 0.0))
    min_delta_frac  = float(params.get("min_delta_frac", 0.30))  # bubble range vs ATR(atr_len)
    atr_len         = int(params.get("atr_len", 14))
    atr_mult_stop   = float(params.get("atr_mult_stop", 1.0))

    out: List[Dict[str, Any]] = []
    need_len = max(bars_per_bubble + 5, atr_len + 5, 120)

    for sym, df in df_map.items():
        if not _ok_df(df, need_len=need_len):
            out.append({"symbol": sym, "action": "flat", "reason": "insufficient_data"})
            continue

        df = df.sort_index()
        c = df["close"]
        hwin = df["high"].rolling(bars_per_bubble)
        lwin = df["low"].rolling(bars_per_bubble)
        vwin = df["volume"].rolling(bars_per_bubble)

        bubble_high = hwin.max()
        bubble_low  = lwin.min()
        bubble_vol  = vwin.sum()

        atr = _atr(df, atr_len)

        bh = float(bubble_high.iloc[-2])  # prior bar bubble high
        bl = float(bubble_low.iloc[-2])
        bv = float(bubble_vol.iloc[-2])
        atr_now = float(atr.iloc[-1])

        c_prev, c_now = float(c.iloc[-2]), float(c.iloc[-1])

        have = _in_pos(positions, sym)

        # Bubble must be "meaningful" vs ATR
        bubble_range = max(1e-9, bh - bl)
        meaningful = (atr_now > 0) and ((bubble_range / atr_now) >= min_delta_frac)

        if have:
            # Exit if failed breakout: close back below prior bubble high; or ATR stop from bubble low
            stop_line = bl - atr_mult_stop * atr_now
            if (c_now < bh) or (atr_mult_stop > 0 and c_now < stop_line):
                reason = "failed_breakout" if (c_now < bh) else "atr_stop"
                out.append({"symbol": sym, "action": "sell", "reason": reason})
                continue
            out.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        # Entry: break above bubble high + volume context + meaningful bubble
        if meaningful and (bv >= min_total_vol) and (c_prev <= bh) and (c_now > bh):
            out.append({"symbol": sym, "action": "buy", "reason": "bubble_breakout"})
        else:
            out.append({"symbol": sym, "action": "flat", "reason": "no_signal"})

    return out
