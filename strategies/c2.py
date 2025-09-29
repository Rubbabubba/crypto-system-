# strategies/c2.py — v1.9.4
# Close vs EMA trend filter + ATR context. Entry: close>EMA & slope>=0. Exit: close<EMA OR ATR breach.
from __future__ import annotations
from typing import Dict, Any, List
import numpy as np
import pandas as pd

NAME = "c2"
VERSION = "1.9.4"

NEED_COLS = {"open","high","low","close","volume"}

def _ok_df(df: pd.DataFrame, need_len: int = 60) -> bool:
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
    ema_len  = int(params.get("ema_len", 50))
    atr_len  = int(params.get("atr_len", 14))
    atr_mult = float(params.get("atr_mult", 1.0))

    out: List[Dict[str, Any]] = []

    need_len = max(ema_len + 5, atr_len + 5, 60)

    for sym, df in df_map.items():
        if not _ok_df(df, need_len=need_len):
            out.append({"symbol": sym, "action": "flat", "reason": "insufficient_data"})
            continue

        df = df.sort_index()
        ema = _ema(df["close"], ema_len)
        atr = _atr(df, atr_len)

        c_prev, c_now = df["close"].iloc[-2], df["close"].iloc[-1]
        ema_prev, ema_now = ema.iloc[-2], ema.iloc[-1]
        atr_now = float(atr.iloc[-1])

        slope_now = float(ema_now - ema_prev)

        have = _in_pos(positions, sym)

        # Stop line below EMA
        stop_line = ema_now - atr_mult * atr_now

        if have:
            # Exit if price under EMA OR under stop_line
            if (c_now < ema_now) or (atr_mult > 0 and c_now < stop_line):
                reason = "ema_break" if (c_now < ema_now) else "atr_stop"
                out.append({"symbol": sym, "action": "sell", "reason": reason})
                continue
            out.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        # Entry: close above EMA and slope non-negative
        if (c_now > ema_now) and (slope_now >= 0):
            out.append({"symbol": sym, "action": "buy", "reason": "close>ema_slope_up"})
        else:
            out.append({"symbol": sym, "action": "flat", "reason": "no_signal"})

    return out
