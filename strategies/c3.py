# strategies/c3.py
from __future__ import annotations
import math
from typing import Dict, Any
import pandas as pd

from .utils import last, nz, ensure_df_has, len_ok, safe_float

__version__ = "1.1.0"

def ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()

def sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length, min_periods=length).mean()

MA_TYPES = {
    "EMA": ema,
    "SMA": sma,
}

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

def run(symbol: str, df: pd.DataFrame, cfg: Dict[str, Any], place_order, log):
    """
    Fast/slow MA cross with ATR-based stops/targets (long-only by default).
    Env knobs:
      C3_MA1_TYPE(EMA), C3_MA2_TYPE(EMA), C3_MA1_LEN(13), C3_MA2_LEN(34),
      C3_ATR_LEN(14), C3_RISK_M(0.8), C3_RR_MULT(1.0),
      C3_USE_LIMIT(1), C3_TRAIL_ON(1), C3_TRAIL_ATR_MULT(1.2), C3_RR_EXIT_FRAC(0.5),
      ORDER_NOTIONAL(25), C3_MIN_BARS
    """
    out = {"symbol": symbol, "action": "flat"}
    try:
        need_cols = ["close", "high", "low"]
        if not ensure_df_has(df, need_cols):
            out.update({"reason": "missing_cols", "need": need_cols})
            return out

        ma1_type = str(cfg.get("C3_MA1_TYPE", "EMA")).upper()
        ma2_type = str(cfg.get("C3_MA2_TYPE", "EMA")).upper()
        ma1_len = int(cfg.get("C3_MA1_LEN", 13))
        ma2_len = int(cfg.get("C3_MA2_LEN", 34))
        atr_len = int(cfg.get("C3_ATR_LEN", 14))
        risk_m = safe_float(cfg.get("C3_RISK_M", 0.8), 0.8)
        rr_mult = safe_float(cfg.get("C3_RR_MULT", 1.0), 1.0)
        use_limit = str(cfg.get("C3_USE_LIMIT", "1")) == "1"
        trail_on = str(cfg.get("C3_TRAIL_ON", "1")) == "1"
        trail_mult = safe_float(cfg.get("C3_TRAIL_ATR_MULT", 1.2), 1.2)
        rr_exit_frac = safe_float(cfg.get("C3_RR_EXIT_FRAC", 0.5), 0.5)
        notional = safe_float(cfg.get("ORDER_NOTIONAL", 25), 25.0)

        min_bars = int(cfg.get("C3_MIN_BARS", max(150, ma2_len + atr_len + 5)))
        if not len_ok(df, min_bars):
            out.update({"reason": "not_enough_bars", "have": len(df), "need": min_bars})
            return out

        close_s = df["close"].astype(float)
        ma1_fn = MA_TYPES.get(ma1_type, ema)
        ma2_fn = MA_TYPES.get(ma2_type, ema)

        ma1_s = ma1_fn(close_s, ma1_len)
        ma2_s = ma2_fn(close_s, ma2_len)
        atr_s = atr(df, atr_len)

        # Cross detection uses previous & current scalars
        ma1_prev = nz(last(ma1_s.iloc[-2]))
        ma2_prev = nz(last(ma2_s.iloc[-2]))
        ma1_now = nz(last(ma1_s))
        ma2_now = nz(last(ma2_s))
        close = nz(last(close_s))
        atr_v = nz(last(atr_s))

        if any(math.isnan(x) for x in [ma1_prev, ma2_prev, ma1_now, ma2_now, close, atr_v]):
            out["reason"] = "nan_indicators"
            return out

        cross_up = (ma1_prev <= ma2_prev) and (ma1_now > ma2_now)

        if cross_up and notional > 0:
            # Risk model (simple): stop = close - risk_m * ATR, target = close + rr_mult * (close - stop)
            stop = close - risk_m * atr_v
            risk = max(1e-12, close - stop)
            target = close + rr_mult * risk

            oid = place_order(symbol, "buy", notional=notional,
                              risk_model={
                                  "stop": round(stop, 6),
                                  "target": round(target, 6),
                                  "trail_on": trail_on,
                                  "trail_atr_mult": trail_mult,
                                  "rr_exit_frac": rr_exit_frac,
                              },
                              limit_price=(round(target, 6) if use_limit else None))

            out.update({
                "action": "buy",
                "order_id": oid,
                "close": round(close, 4),
                "ma1": round(ma1_now, 4),
                "ma2": round(ma2_now, 4),
                "atr": round(atr_v, 6),
                "stop": round(stop, 6),
                "target": round(target, 6),
                "reason": "ma_cross_up",
            })
        else:
            out.update({
                "action": "flat",
                "close": round(close, 4),
                "ma1": round(ma1_now, 4),
                "ma2": round(ma2_now, 4),
                "atr": round(atr_v, 6),
                "reason": "no_signal",
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
                    "C3_MA1_TYPE": cfg.get("C3_MA1_TYPE"),
                    "C3_MA2_TYPE": cfg.get("C3_MA2_TYPE"),
                    "C3_MA1_LEN": cfg.get("C3_MA1_LEN"),
                    "C3_MA2_LEN": cfg.get("C3_MA2_LEN"),
                    "C3_ATR_LEN": cfg.get("C3_ATR_LEN"),
                    "ORDER_NOTIONAL": cfg.get("ORDER_NOTIONAL"),
                },
            },
        })
        return out
