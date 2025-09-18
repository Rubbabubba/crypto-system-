# strategies/c1.py
from __future__ import annotations
import math
from typing import Dict, Any
import pandas as pd

from .utils import last, nz, ensure_df_has, len_ok, safe_float

__version__ = "1.1.1"
VERSION = (1, 1, 1)

# ---- basic indicators (no external deps) ----
def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    roll_up = up.ewm(alpha=1/length, adjust=False).mean()
    roll_down = down.ewm(alpha=1/length, adjust=False).mean()
    rs = roll_up / (roll_down.replace(0, 1e-12))
    return 100 - (100 / (1 + rs))

def run(symbol: str, df: pd.DataFrame, cfg: Dict[str, Any], place_order, log, **kwargs):
    """
    RSI pullback above EMA. Long-only by default.
    Env knobs:
      C1_RSI_LEN (14), C1_EMA_LEN (21), C1_RSI_BUY (48),
      C1_CLOSE_ABOVE_EMA_EQ (0/1), ORDER_NOTIONAL (25), C1_MIN_BARS (>=50)
    kwargs: dry, force, now (ignored safely)
    """
    out = {"symbol": symbol, "action": "flat"}
    try:
        dry = bool(kwargs.get("dry", False))

        need_cols = ["close"]
        if not ensure_df_has(df, need_cols):
            out.update({"reason": "missing_cols", "need": need_cols})
            return out

        rsi_len = int(cfg.get("C1_RSI_LEN", 14))
        ema_len = int(cfg.get("C1_EMA_LEN", 21))
        min_bars = int(cfg.get("C1_MIN_BARS", max(50, ema_len + rsi_len + 2)))
        rsi_buy = safe_float(cfg.get("C1_RSI_BUY", 48), 48.0)
        allow_eq = str(cfg.get("C1_CLOSE_ABOVE_EMA_EQ", "1")) == "1"
        notional = safe_float(cfg.get("ORDER_NOTIONAL", 25), 25.0)

        if not len_ok(df, min_bars):
            out.update({"reason": "not_enough_bars", "have": len(df), "need": min_bars})
            return out

        close_s = df["close"].astype(float)
        ema_s = ema(close_s, ema_len)
        rsi_s = rsi(close_s, rsi_len)

        close = nz(last(close_s))
        ema_v = nz(last(ema_s))
        rsi_v = nz(last(rsi_s))

        if any(math.isnan(x) for x in [close, ema_v, rsi_v]):
            out["reason"] = "nan_indicators"
            return out

        above_ema = (close > ema_v) or (allow_eq and abs(close - ema_v) < 1e-12)
        pullback_ok = rsi_v < rsi_buy
        enter_long = bool(above_ema and pullback_ok)

        if enter_long and notional > 0:
            if dry:
                out.update({
                    "action": "paper_buy",
                    "close": round(close, 4),
                    "rsi": round(rsi_v, 2),
                    "ema": round(ema_v, 2),
                    "reason": "dry_run_enter_long_rsi_pullback"
                })
            else:
                oid = place_order(symbol, "buy", notional=notional)
                out.update({
                    "action": "buy",
                    "order_id": oid,
                    "close": round(close, 4),
                    "rsi": round(rsi_v, 2),
                    "ema": round(ema_v, 2),
                    "reason": "enter_long_rsi_pullback"
                })
        else:
            out.update({
                "action": "flat",
                "close": round(close, 4),
                "rsi": round(rsi_v, 2),
                "ema": round(ema_v, 2),
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
                    "C1_RSI_LEN": cfg.get("C1_RSI_LEN"),
                    "C1_EMA_LEN": cfg.get("C1_EMA_LEN"),
                    "C1_RSI_BUY": cfg.get("C1_RSI_BUY"),
                    "ORDER_NOTIONAL": cfg.get("ORDER_NOTIONAL"),
                },
            },
        })
        return out
