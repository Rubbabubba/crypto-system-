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

def _extract_ctx(args, kwargs):
    dry = kwargs.get("dry", args[0] if len(args) > 0 else True)
    notional = kwargs.get("notional", args[1] if len(args) > 1 else None)
    return bool(dry), notional

def run(symbol: str,
        df: pd.DataFrame,
        params: dict,
        *args,
        **kwargs):
    """
    EMA + HH with delta fraction (vs ATR). Always returns a dict.
    Accepts dry/notional flexibly.
    """
    dry, notional = _extract_ctx(args, kwargs)

    ema_len  = int(params.get("ema_len", 34))
    hh_len   = int(params.get("hh_len", 21))
    atr_len  = int(params.get("atr_len", 14))
    delta_n  = int(params.get("delta_len", 5))
    atr_mult = float(params.get("atr_mult", 0.5))

    need = max(ema_len, hh_len, atr_len, delta_n) + 2
    if df is None or len(df) < need:
        return {"symbol": symbol, "action": "flat", "reason": "insufficient_bars", "order_id": None}

    df = df.copy()
    df["ema"] = _ema(df["close"], ema_len)
    df["hh"]  = df["high"].rolling(hh_len).max()
    df["atr"] = _atr(df, atr_len)

    df["delta"] = df["close"] - df["close"].shift(delta_n)
    df["delta_frac"] = (df["delta"] / df["atr"]).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    last = df.iloc[-1]
    close = float(last["close"])
    ema   = float(last["ema"])
    hh    = float(last["hh"])
    atr   = float(last["atr"]) if np.isfinite(last["atr"]) else np.nan
    delta_frac = float(last["delta_frac"])

    action = "flat"
    reason = "no_signal"

    if np.isfinite(atr) and close > hh and close > ema and delta_frac > atr_mult:
        action = "buy"
        reason = "breakout_momentum"
    elif np.isfinite(atr) and close < ema - atr_mult * atr:
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
        "order_id": None,
        "dry": dry,
        "notional": notional
    }
