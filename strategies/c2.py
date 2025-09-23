# strategies/c2.py — v1.8.8
import pandas as pd

NAME = "c2"
VERSION = "1.8.8"

def _ema(s, n): return s.ewm(span=n, adjust=False).mean()

def _atr(df, n=14):
    h, l, c = df["high"], df["low"], df["close"]
    hl = (h - l).abs()
    hc = (h - c.shift(1)).abs()
    lc = (l - c.shift(1)).abs()
    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def run(df_map, params, positions):
    ema_len = int(params.get("ema_len", 20))
    atr_len = int(params.get("atr_len", 14))
    atr_mult = float(params.get("atr_mult", 1.0))
    results = []
    for sym, df in df_map.items():
        if df is None or len(df) < max(ema_len, atr_len) + 2:
            continue
        c = df["close"]; e = _ema(c, ema_len); a = _atr(df, atr_len)
        c0, e0, a0 = c.iloc[-1], e.iloc[-1], a.iloc[-1]
        have = float(positions.get(sym, 0.0)) > 0.0

        if not have and c0 > e0:
            results.append({"symbol": sym, "action": "buy", "reason": "close_above_ema"})
        elif have and c0 < (e0 - atr_mult*a0):
            results.append({"symbol": sym, "action": "sell", "reason": "atr_stop"})
        else:
            results.append({"symbol": sym, "action": "flat", "reason": "no_signal"})
    return results
