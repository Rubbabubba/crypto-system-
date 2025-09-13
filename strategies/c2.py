# strategies/c2.py
import os, numpy as np, pandas as pd

__version__ = "1.0.0"

def atr(df: pd.DataFrame, length: int = 14):
    hi_lo = df["high"] - df["low"]
    hi_pc = (df["high"] - df["close"].shift()).abs()
    lo_pc = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([hi_lo, hi_pc, lo_pc], axis=1).max(axis=1)
    return tr.rolling(length).mean()

def run(market, broker, symbols: list, params: dict, dry: bool, pwrite):
    tf = params.get("C2_TIMEFRAME", "15Min")
    look = int(params.get("C2_LOOKBACK", 60))
    atr_len = int(params.get("C2_ATR_LEN", 14))
    k = float(params.get("C2_BREAK_K", 1.2))  # breakout threshold in ATRs
    notional = float(params.get("ORDER_NOTIONAL", 25))
    results = []

    for sym in symbols:
        df = market.get_bars(sym, timeframe=tf, limit=max(look, atr_len) + 5)
        if df.empty or len(df) < look + 2:
            results.append({"symbol": sym, "status": "no_data"})
            continue

        df["atr"] = atr(df, atr_len)
        last = df.iloc[-1]
        hh = df["high"].tail(look).max()
        ll = df["low"].tail(look).min()

        action, reason = "hold", None
        long_break = last["close"] > hh and (last["close"] - df["close"].iloc[-2]) > k * last["atr"]
        short_break = last["close"] < ll and (df["close"].iloc[-2] - last["close"]) > k * last["atr"]

        # Long-only version for now (set short later if desired)
        if long_break:
            action, reason = "buy", f"Breakout above {look}-bar high with {k}*ATR thrust"

        if action == "buy":
            resp = broker.place_order_notional(sym, "buy", notional, dry=dry)
            pwrite(sym, "C2", "BUY", notional, reason, dry, extra=resp)

        results.append({"symbol": sym, "action": action, "reason": reason,
                        "close": float(last["close"]), "atr": float(last["atr"]), "HH": float(hh), "LL": float(ll)})
    return results
