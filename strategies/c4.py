# strategies/c4.py
import numpy as np
import pandas as pd

def _ema(series: pd.Series, span: int):
    return series.ewm(span=span, adjust=False).mean()

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
    ema_len  = int(params.get("ema_len", 34))
    hh_len   = int(params.get("hh_len", 21))
    atr_len  = int(params.get("atr_len", 14))
    delta_n  = int(params.get("delta_len", 5))   # lookback for close change
    atr_mult = float(params.get("atr_mult", 0.5))

    need = max(ema_len, hh_len, atr_len, delta_n) + 2
    if df is None or len(df) < need:
        return {"symbol": symbol, "action": "flat", "reason": "insufficient_bars"}

    df = df.copy()
    df["ema"] = _ema(df["close"], ema_len)
    df["hh"]  = df["high"].rolling(hh_len).max()
    df["atr"] = _atr(df, atr_len)

    # delta fraction vs ATR (or vs price)
    df["delta"] = df["close"] - df["close"].shift(delta_n)
    # normalize by ATR to keep it scale-aware
    df["delta_frac"] = (df["delta"] / df["atr"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    last = df.iloc[-1]
    close = float(last["close"])
    ema   = float(last["ema"])
    hh    = float(last["hh"])
    atr   = float(last["atr"])
    delta_frac = float(last["delta_frac"])

    action = "flat"
    reason = "no_signal"

    if close > hh and close > ema and delta_frac > atr_mult:
        action = "buy"
        reason = "breakout_momentum"
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
        "delta_frac": delta_frac,
        "order_id": None
    }
