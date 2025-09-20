# strategies/c3.py
import numpy as np
import pandas as pd

def _sma(series: pd.Series, n: int):
    return series.rolling(n).mean()

def _atr(df: pd.DataFrame, atr_len: int):
    h, l, c = df['high'], df['low'], df['close']
    prev_c = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(atr_len).mean()

def run(symbol: str,
        df: pd.DataFrame,
        params: dict,
        dry: bool,
        notional: float | None):
    ma_fast = int(params.get("ma_fast", 20))
    ma_slow = int(params.get("ma_slow", 50))
    atr_len = int(params.get("atr_len", 14))

    need = max(ma_fast, ma_slow, atr_len) + 2
    if df is None or len(df) < need:
        return {"symbol": symbol, "action": "flat", "reason": "insufficient_bars"}

    df = df.copy()
    df["ma1"] = _sma(df["close"], ma_fast)
    df["ma2"] = _sma(df["close"], ma_slow)
    df["atr"] = _atr(df, atr_len)

    last = df.iloc[-1]
    close = float(last["close"])
    ma1   = float(last["ma1"])
    ma2   = float(last["ma2"])
    atr   = float(last["atr"])

    action = "flat"
    reason = "no_signal"

    # simple cross logic
    prev = df.iloc[-2]
    prev_ma1 = float(prev["ma1"])
    prev_ma2 = float(prev["ma2"])

    crossed_up   = prev_ma1 <= prev_ma2 and ma1 > ma2
    crossed_down = prev_ma1 >= prev_ma2 and ma1 < ma2

    if crossed_up:
        action = "buy"
        reason = "ma_cross_up"
    elif crossed_down:
        action = "sell"
        reason = "ma_cross_down"

    return {
        "symbol": symbol,
        "action": action,
        "reason": reason,
        "close": close,
        "ma1": ma1,
        "ma2": ma2,
        "atr": atr,
        "order_id": None
    }
