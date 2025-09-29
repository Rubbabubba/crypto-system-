# strategies/c5.py — v1.9.4
# Highest-high breakout with EMA/ATR context. Exit on failed breakout or close<EMA.
from __future__ import annotations
from typing import Dict, Any, List
import numpy as np
import pandas as pd

NAME = "c5"
VERSION = "1.9.4"

NEED_COLS = {"open","high","low","close","volume"}

def _ok_df(df: pd.DataFrame, need_len: int = 100) -> bool:
    return (
        isinstance(df, pd.DataFrame) and
        NEED_COLS.issubset(df.columns) and
        len(df) >= need_len and
        not df.tail(2).isna().any().any()
    )

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=int(n), adjust=False).mean()

def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h-l), (h-prev_c).abs(), (l-prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(int(n)).mean()

def _in_pos(positions: Dict[str, float], sym: str) -> bool:
    try: return float(positions.get(sym, 0.0)) > 0.0
    except Exception: return False

def run(df_map: Dict[str, pd.DataFrame], params: Dict[str, Any], positions: Dict[str, float]) -> List[Dict[str, Any]]:
    breakout_len = int(params.get("breakout_len", 20))
    ema_len      = int(params.get("ema_len", 20))
    atr_len      = int(params.get("atr_len", 14))
    k_fail       = float(params.get("k_fail", 0.0))  # percentage pullback tolerated after breakout (0..0.2)

    out: List[Dict[str, Any]] = []

    need_len = max(breakout_len + 5, ema_len + 5, atr_len + 5, 100)

    for sym, df in df_map.items():
        if not _ok_df(df, need_len=need_len):
            out.append({"symbol": sym, "action": "flat", "reason": "insufficient_data"})
            continue

        df = df.sort_index()
        c = df["close"]
        ema = _ema(c, ema_len)
        atr = _atr(df, atr_len)

        # Prior highest high
        hh_prev = df["high"].rolling(breakout_len).max().iloc[-2]
        c_prev, c_now = float(c.iloc[-2]), float(c.iloc[-1])
        ema_now = float(ema.iloc[-1])

        have = _in_pos(positions, sym)

        if have:
            # Exit if failed breakout (price back below hh_prev by k_fail fraction) or close below EMA
            fail_line = hh_prev * (1 - abs(k_fail))
            if (k_fail > 0 and c_now < fail_line) or (c_now < ema_now):
                reason = "failed_breakout" if (k_fail > 0 and c_now < fail_line) else "ema_break"
                out.append({"symbol": sym, "action": "sell", "reason": reason})
                continue
            out.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        # Entry: breakout + above EMA
        if (c_prev <= hh_prev) and (c_now > hh_prev) and (c_now > ema_now):
            out.append({"symbol": sym, "action": "buy", "reason": "hh_breakout"})
        else:
            out.append({"symbol": sym, "action": "flat", "reason": "no_signal"})

    return out
