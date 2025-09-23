# strategies/c6.py — v1.8.8
import pandas as pd

NAME = "c6"
VERSION = "1.8.8"

def _ema(s, n): return s.ewm(span=n, adjust=False).mean()

def run(df_map, params, positions):
    ef = int(params.get("ema_fast_len", 12))
    es = int(params.get("ema_slow_len", 26))
    sg = int(params.get("signal_len", 9))
    results = []
    for sym, df in df_map.items():
        if df is None or len(df) < es + sg + 2:
            continue
        c = df["close"]
        fast = _ema(c, ef)
        slow = _ema(c, es)
        macd = fast - slow
        signal = _ema(macd, sg)
        m0, s0 = macd.iloc[-1], signal.iloc[-1]
        have = float(positions.get(sym, 0.0)) > 0.0

        if not have and m0 > s0:
            results.append({"symbol": sym, "action": "buy", "reason": "macd_cross_up"})
        elif have and m0 < s0:
            results.append({"symbol": sym, "action": "sell", "reason": "macd_cross_down"})
        else:
            results.append({"symbol": sym, "action": "flat", "reason": "no_signal"})
    return results
