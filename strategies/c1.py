# strategies/c1.py — v1.8.8
import pandas as pd

NAME = "c1"
VERSION = "1.8.8"

def _ema(s, n):
    return s.ewm(span=n, adjust=False).mean()

def run(df_map, params, positions):
    ema_len = int(params.get("ema_len", 20))
    results = []
    for sym, df in df_map.items():
        if df is None or df.empty:
            continue
        close = df["close"]
        ema = _ema(close, ema_len)
        c0, e0 = close.iloc[-1], ema.iloc[-1]
        have = float(positions.get(sym, 0.0)) > 0.0

        if not have and c0 > e0:
            results.append({"symbol": sym, "action": "buy", "reason": f"close>{ema_len}EMA"})
        elif have and c0 < e0:
            results.append({"symbol": sym, "action": "sell", "reason": f"close<{ema_len}EMA"})
        else:
            results.append({"symbol": sym, "action": "flat", "reason": "no_signal"})
    return results
