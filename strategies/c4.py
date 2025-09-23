# c4.py — v1.8.7
import pandas as pd
import numpy as np

NAME = "c4"
VERSION = "1.8.7"

def _rsi(s, n=14):
    d = s.diff()
    up = (d.where(d>0, 0)).rolling(n).mean()
    dn = (-d.where(d<0, 0)).rolling(n).mean()
    rs = up / (dn.replace(0, np.nan))
    rsi = 100 - (100/(1+rs))
    return rsi.fillna(50)

def run(df_map, params, positions):
    n = int(params.get("rsi_len", 14))
    buy_th = float(params.get("buy_th", 30))
    sell_th = float(params.get("sell_th", 55))
    results = []
    for sym, df in df_map.items():
        if df is None or len(df) < n + 2:
            continue
        r = _rsi(df["close"], n)
        r0 = r.iloc[-1]
        have = float(positions.get(sym, 0.0)) > 0.0

        if not have and r0 < buy_th:
            results.append({"symbol": sym, "action": "buy", "reason": f"rsi<{buy_th}"})
        elif have and r0 > sell_th:
            results.append({"symbol": sym, "action": "sell", "reason": f"rsi>{sell_th}"})
        else:
            results.append({"symbol": sym, "action": "flat", "reason": "no_signal"})
    return results
