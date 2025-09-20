# strategies/c2.py
import numpy as np
import pandas as pd

def _ema(series: pd.Series, span: int):
    return series.ewm(span=span, adjust=False).mean()

def _atr(df: pd.DataFrame, atr_len: int):
    # Expect columns: open, high, low, close
    h, l, c = df['high'], df['low'], df['close']
    prev_c = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(atr_len).mean()

def run(symbol: str,
        df: pd.DataFrame,
        params: dict,
        dry: bool,
        notional: float | None):
    """
    Returns a dict (required by the app).
    df must contain time-indexed ohlc columns: open, high, low, close.
    """
    # Defaults (override via query params)
    ema_len   = int(params.get("ema_len", 34))
    hh_len    = int(params.get("hh_len", 34))
    atr_len   = int(params.get("atr_len", 14))
    atr_mult  = float(params.get("atr_mult", 0.5))  # sensitivity

    if df is None or len(df) < max(ema_len, hh_len, atr_len) + 2:
        return {"symbol": symbol, "action": "flat", "reason": "insufficient_bars"}

    df = df.copy()
    df["ema"] = _ema(df["close"], ema_len)
    df["hh"]  = df["high"].rolling(hh_len).max()
    df["atr"] = _atr(df, atr_len)

    last = df.iloc[-1]
    close = float(last["close"])
    ema   = float(last["ema"])
    hh    = float(last["hh"])
    atr   = float(last["atr"])

    action = "flat"
    reason = "no_signal"

    # Example signals (tweak to taste):
    # breakout long if close > hh and close > ema
    if close > hh and close > ema and np.isfinite(atr):
        action = "buy"
        reason = "breakout_long"
    # mean reversion flatten: close far below ema by ATR multiple
    elif close < ema - atr_mult * atr:
        action = "flat"
        reason = "below_ema_atr"

    return {
        "symbol": symbol,
        "action": action,
        "reason": reason,
        "close": close,
        "ema": ema,
        "hh": hh,
        "atr": atr,
        "order_id": None  # app may fill this when placing orders
    }
