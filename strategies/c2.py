# strategies/c2.py
from __future__ import annotations
import math
from typing import Dict, Any
import pandas as pd

from .utils import last, nz, ensure_df_has, len_ok, safe_float

__version__ = "1.1.1"
VERSION = (1, 1, 1)

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.ewm(span=length, adjust=False).mean()

def run(symbol: str, df: pd.DataFrame, cfg: Dict[str, Any], place_order, log, **kwargs):
    """
    Donchian breakout above HH + k*ATR with EMA trend filter.
    Env knobs:
      C2_LOOKBACK (20), C2_BREAK_K (0.5), C2_ATR_LEN (14),
      C2_EMA_TREND_LEN (50), ORDER_NOTIONAL (25), C2_MIN_BARS
    kwargs: dry, force, now (ignored safely)
    """
    out = {"symbol": symbol, "action": "flat"}
    try:
        dry = bool(kwargs.get("dry", False))

        need_cols = ["close", "high", "low"]
        if not ensure_df_has(df, need_cols):
            out.update({"reason": "missing_cols", "need": need_cols})
            return out

        n = int(cfg.get("C2_LOOKBACK", 20))
        k = safe_float(cfg.get("C2_BREAK_K", 0.5), 0.5)
        atr_len = int(cfg.get("C2_ATR_LEN", 14))
        ema_len = int(cfg.get("C2_EMA_TREND_LEN", 50))
        notional = safe_float(cfg.get("ORDER_NOTIONAL", 25), 25.0)
        min_bars = int(cfg.get("C2_MIN_BARS", max(100, n + ema_len + atr_len + 2)))

        if not len_ok(df, min_bars):
            out.update({"reason": "not_enough_bars", "have": len(df), "need": min_bars})
            return out

        close_s = df["close"].astype(float)
        high_s = df["high"].astype(float)

        hh_s = high_s.rolling(n, min_periods=n).max()
        atr_s = atr(df, atr_len)
        ema_s = ema(close_s, ema_len)

        close = nz(last(close_s))
        hh = nz(last(hh_s))
        atr_v = nz(last(atr_s))
        ema_v = nz(last(ema_s))

        if any(math.isnan(x) for x in [close, hh, atr_v, ema_v]):
            out["reason"] = "nan_indicators"
            return out

        breakout_up = close > (hh + k * atr_v)
        trend_ok = close > ema_v
        enter_long = bool(breakout_up and trend_ok)

        if enter_long and notional > 0:
            if dry:
                out.update({
                    "action": "paper_buy",
                    "close": round(close, 4),
                    "hh": round(hh, 4),
                    "atr": round(atr_v, 6),
                    "ema": round(ema_v, 4),
                    "reason": "dry_run_breakout_trend_ok"
                })
            else:
                oid = place_order(symbol, "buy", notional=notional)
                out.update({
                    "action": "buy",
                    "order_id": oid,
                    "close": round(close, 4),
                    "hh": round(hh, 4),
                    "atr": round(atr_v, 6),
                    "ema": round(ema_v, 4),
                    "reason": "donchian_breakout_trend_ok"
                })
        else:
            out.update({
                "action": "flat",
                "close": round(close, 4),
                "hh": round(hh, 4),
                "atr": round(atr_v, 6),
                "ema": round(ema_v, 4),
                "reason": "no_signal"
            })
        return out

    except Exception as e:
        out.update({
            "action": "error",
            "error": f"{type(e).__name__}: {e}",
            "ctx": {
                "nrows": int(len(df) if isinstance(df, pd.DataFrame) else 0),
                "cols": list(df.columns) if isinstance(df, pd.DataFrame) else [],
                "params": {
                    "C2_LOOKBACK": cfg.get("C2_LOOKBACK"),
                    "C2_BREAK_K": cfg.get("C2_BREAK_K"),
                    "C2_ATR_LEN": cfg.get("C2_ATR_LEN"),
                    "C2_EMA_TREND_LEN": cfg.get("C2_EMA_TREND_LEN"),
                    "ORDER_NOTIONAL": cfg.get("ORDER_NOTIONAL"),
                },
            },
        })
        return out
