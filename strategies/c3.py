# strategies/c3.py — v1.9.4
# MA crossover (default EMA 13/34). Exit on cross-down or ATR stop below slow MA.
from __future__ import annotations
from typing import Dict, Any, List
import numpy as np
import pandas as pd

NAME = "c3"
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

def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(int(n)).mean()

def _atr(df: pd.DataFrame, n: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h-l), (h-prev_c).abs(), (l-prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(int(n)).mean()

def _in_pos(positions: Dict[str, float], sym: str) -> bool:
    try: return float(positions.get(sym, 0.0)) > 0.0
    except Exception: return False

def run(df_map: Dict[str, pd.DataFrame], params: Dict[str, Any], positions: Dict[str, float]) -> List[Dict[str, Any]]:
    ma1_type = str(params.get("ma_fast_type", params.get("ma1_type", "EMA"))).upper()
    ma2_type = str(params.get("ma_slow_type", params.get("ma2_type", "EMA"))).upper()
    ma_fast  = int(params.get("ma_fast", params.get("ma1_len", 13)))
    ma_slow  = int(params.get("ma_slow", params.get("ma2_len", 34)))
    atr_len  = int(params.get("atr_len", 14))
    atr_mult = float(params.get("atr_mult", 1.0))

    out: List[Dict[str, Any]] = []

    need_len = max(ma_slow + 5, atr_len + 5, 60)

    def _ma(series: pd.Series, t: str, n: int) -> pd.Series:
        return _ema(series, n) if t == "EMA" else _sma(series, n)

    for sym, df in df_map.items():
        if not _ok_df(df, need_len=need_len):
            out.append({"symbol": sym, "action": "flat", "reason": "insufficient_data"})
            continue

        df = df.sort_index()
        f = _ma(df["close"], ma1_type, ma_fast)
        s = _ma(df["close"], ma2_type, ma_slow)
        atr = _atr(df, atr_len)

        f_prev, f_now = f.iloc[-2], f.iloc[-1]
        s_prev, s_now = s.iloc[-2], s.iloc[-1]
        c_now = df["close"].iloc[-1]
        atr_now = float(atr.iloc[-1])

        have = _in_pos(positions, sym)
        stop_line = s_now - atr_mult * atr_now

        if have:
            if (f_prev > s_prev and f_now <= s_now) or (atr_mult > 0 and c_now < stop_line):
                reason = "ma_cross_down" if (f_prev > s_prev and f_now <= s_now) else "atr_stop"
                out.append({"symbol": sym, "action": "sell", "reason": reason})
                continue
            out.append({"symbol": sym, "action": "flat", "reason": "hold_in_pos"})
            continue

        if f_prev <= s_prev and f_now > s_now:
            out.append({"symbol": sym, "action": "buy", "reason": "ma_cross_up"})
        else:
            out.append({"symbol": sym, "action": "flat", "reason": "no_signal"})

    return out
