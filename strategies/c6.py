# strategies/c6.py — v1.9.4
# EMA fast/slow (MACD-style) + higher-high confirmation. Exit on EMA cross-down or ATR stop vs slow EMA.
from __future__ import annotations
from typing import Dict, Any, List
import numpy as np
import pandas as pd

NAME = "c6"
VERSION = "1.9.4"

NEED_COLS = {"open","high","low","close","volume"}

def _ok_df(df: pd.DataFrame, need_len: int = 80) -> bool:
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
    ema_fast_len   = int(params.get("ema_fast_len", 12))
    ema_slow_len   = int(params.get("ema_slow_len", 26))
    confirm_hh_len = int(params.get("confirm_hh_len", 10))
    atr_len        = int(params.get("atr_len", 14))
    atr_mult_stop  = float(params.get("atr_mult_stop", 1.0))

    out: List[Dict[str, Any]] = []

    need_len = max(ema_slow_len + confirm_hh_len + 5, atr_len + 5, 80)

    for sym, df in df_map.items():
        if not _ok_df(df, need_len=need_len):
            out.append({"symbol": sym, "action": "flat", "reason": "insufficient_data"})
            continue

        df = df.sort_index()
        c = df["close"]
        h = df["high"]

        f = _ema(c, ema_fast_len)
        s = _ema(c, ema_slow_len)
        atr = _atr(df, atr_len)

        f_prev, f_now = float(f.iloc[-2]), float(f.iloc[-1])
        s_prev, s_now = float(s.iloc[-2]), float(s.iloc[-1])
        c_now = float(c.iloc[-1])
        atr_now = float(atr.iloc[-1])

        have = _in_pos(positions, sym)
        stop_line = s_now - atr_mult_stop * atr_now

        # Higher-high confirmation vs last N bars (excluding current)
        hh_prev = float(h.rolling(confirm_hh_len).max().iloc[-2])

        if have:
            if (f_prev > s_prev and f_now <= s_now) or (atr_mult_stop > 0 and c_now < stop_line):
                reason = "ema_cross_down" if (f_prev > s_prev and f_now <= s_now) else "atr_stop"
                out.append({"symbol": sym, "action": "sell", "reason": reason})
                continue
            out.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        # Entry: fast>slow and close makes a higher-high than prior window
        if (f_now > s_now) and (c_now > hh_prev):
            out.append({"symbol": sym, "action": "buy", "reason": "ema_trend_hh_confirm"})
        else:
            out.append({"symbol": sym, "action": "flat", "reason": "no_signal"})

    return out
