# strategies/c3.py — v1.8.8
import pandas as pd

NAME = "c3"
VERSION = "1.8.8"

def run(df_map, params, positions):
    ma_fast = int(params.get("ma_fast", 12))
    ma_slow = int(params.get("ma_slow", 26))
    results = []
    for sym, df in df_map.items():
        if df is None or len(df) < ma_slow + 2:
            continue
        c = df["close"]
        f = c.rolling(ma_fast).mean()
        s = c.rolling(ma_slow).mean()
        f0, s0 = f.iloc[-1], s.iloc[-1]
        have = float(positions.get(sym, 0.0)) > 0.0

        if not have and f0 > s0:
            results.append({"symbol": sym, "action": "buy", "reason": "sma_cross_up"})
        elif have and f0 < s0:
            results.append({"symbol": sym, "action": "sell", "reason": "sma_cross_down"})
        else:
            results.append({"symbol": sym, "action": "flat", "reason": "no_signal"})
    return results
