# strategies/c1.py
import os, math, time
import numpy as np
import pandas as pd

__version__ = "1.0.0"

def ema(series: pd.Series, length: int):
    return series.ewm(span=length, adjust=False).mean()

def rsi(series: pd.Series, length: int = 14):
    delta = series.diff()
    up = (delta.clip(lower=0)).ewm(alpha=1/length, adjust=False).mean()
    down = (-delta.clip(upper=0)).ewm(alpha=1/length, adjust=False).mean()
    rs = up / (down.replace(0, np.nan))
    return 100 - (100 / (1 + rs))

def run(market, broker, symbols: list, params: dict, dry: bool, pwrite):
    """
    Logic:
      Long bias when close > EMA(EMA_LEN).
      Entry on RSI crossing up above RSI_BUY from below.
      Exit when RSI > RSI_SELL or close < EMA.
    Trades are simplified to market notional orders.
    """
    tf = params.get("C1_TIMEFRAME", "5Min")
    ema_len = int(params.get("C1_EMA_LEN", 20))
    rsi_len = int(params.get("C1_RSI_LEN", 14))
    rsi_buy = float(params.get("C1_RSI_BUY", 38))
    rsi_sell = float(params.get("C1_RSI_SELL", 62))
    notional = float(params.get("ORDER_NOTIONAL", 25))
    results = []

    for sym in symbols:
        bars = market.get_bars(sym, timeframe=tf, limit=max(ema_len, rsi_len) + 50)
        if bars.empty or len(bars) < rsi_len + 10:
            results.append({"symbol": sym, "status": "no_data"})
            continue

        bars["ema"] = ema(bars["close"], ema_len)
        bars["rsi"] = rsi(bars["close"], rsi_len)
        last = bars.iloc[-1]
        prev = bars.iloc[-2]

        action = "hold"
        reason = None

        if last["close"] > last["ema"] and prev["rsi"] < rsi_buy <= last["rsi"]:
            action = "buy"
            reason = f"RSI cross up {prev['rsi']:.1f}->{last['rsi']:.1f} above {rsi_buy} & close>EMA"
        elif last["rsi"] > rsi_sell or last["close"] < last["ema"]:
            action = "flat"
            reason = f"RSI>{rsi_sell} or close<EMA"

        if action == "buy":
            resp = broker.place_order_notional(sym, "buy", notional, dry=dry)
            pwrite(sym, "C1", "BUY", notional, reason, dry, extra=resp)
        elif action == "flat":
            # Simple: cancel all outstanding & let risk rails manage exits or manual flatting.
            resp = broker.cancel_all() if not dry else {"dry_run": True, "action": "cancel_all"}
            pwrite(sym, "C1", "FLAT", 0, reason, dry, extra=resp)

        results.append({"symbol": sym, "action": action, "reason": reason, "close": float(last['close']),
                        "ema": float(last['ema']), "rsi": float(last['rsi'])})
    return results
