# strategies/c5.py — v1.8.8
import pandas as pd

NAME = "c5"
VERSION = "1.8.8"

def _ema(s, n): return s.ewm(span=n, adjust=False).mean()

def run(df_map, params, positions):
    n = int(params.get("breakout_len", 20))
    ema_len = int(params.get("ema_len", 20))
    results = []
    for sym, df in df_map.items():
        if df is None or len(df) < max(n, ema_len) + 2:
            continue
        hh = df["high"].rolling(n).max().iloc[-2]
        c0 = df["close"].iloc[-1]
        e0 = _ema(df["close"], ema_len).iloc[-1]
        have = float(positions.get(sym, 0.0)) > 0.0

        if not have and c0 > hh:
            results.append({"symbol": sym, "action": "buy", "reason": f"breakout_{n}"})
        elif have and c0 < e0:
            results.append({"symbol": sym, "action": "sell", "reason": f"exit_ema{ema_len}"})
        else:
            results.append({"symbol": sym, "action": "flat", "reason": "no_signal"})
    return results
