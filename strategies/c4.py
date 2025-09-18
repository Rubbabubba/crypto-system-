# strategies/c4.py
from __future__ import annotations
import math
from typing import Dict, Any
import pandas as pd
import numpy as np

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

def _estimated_buy_sell_volume(df: pd.DataFrame) -> pd.DataFrame:
    o = df["open"].astype(float)
    h = df["high"].astype(float)
    l = df["low"].astype(float)
    c = df["close"].astype(float)
    v = df["volume"].fillna(0.0).astype(float)

    bar_top = h - np.maximum(o, c)
    bar_bot = np.minimum(o, c) - l
    bar_rng = (h - l).clip(lower=1e-12)
    bull = (c - o) > 0

    buy_rng = np.where(bull, bar_rng, bar_top + bar_bot)
    sell_rng = np.where(bull, bar_top + bar_bot, bar_rng)
    tot_rng = (bar_rng + bar_top + bar_bot).clip(lower=1e-12)

    buy_vol = np.round((buy_rng / tot_rng) * v, 6)
    sell_vol = np.round((sell_rng / tot_rng) * v, 6)

    out = pd.DataFrame(index=df.index)
    out["buy_vol"] = buy_vol
    out["sell_vol"] = sell_vol
    out["delta_vol"] = buy_vol - sell_vol
    out["tot_vol"] = buy_vol + sell_vol
    return out

def run(symbol: str, df: pd.DataFrame, cfg: Dict[str, Any], place_order, log, **kwargs):
    """
    Volume-delta window + small ATR breakout with trend filter.
    Env knobs:
      C4_DELTA_WIN(20), C4_MIN_DELTA_FRAC(0.25),
      C4_BREAK_K_ATR(0.25), C4_ATR_LEN(14),
      C4_TREND_EMA_LEN(50), ORDER_NOTIONAL(25), C4_MIN_BARS
    kwargs: dry, force, now (ignored safely)
    """
    out = {"symbol": symbol, "action": "flat"}
    try:
        dry = bool(kwargs.get("dry", False))

        need_cols = ["open", "high", "low", "close", "volume"]
        if not ensure_df_has(df, need_cols):
            out.update({"reason": "missing_cols", "need": need_cols})
            return out

        delta_win = int(cfg.get("C4_DELTA_WIN", 20))
        min_delta_frac = safe_float(cfg.get("C4_MIN_DELTA_FRAC", 0.25), 0.25)
        k_atr = safe_float(cfg.get("C4_BREAK_K_ATR", 0.25), 0.25)
        atr_len = int(cfg.get("C4_ATR_LEN", 14))
        trend_ema_len = int(cfg.get("C4_TREND_EMA_LEN", 50))
        notional = safe_float(cfg.get("ORDER_NOTIONAL", 25), 25.0)
        min_bars = int(cfg.get("C4_MIN_BARS", max(150, delta_win + trend_ema_len + atr_len + 5)))

        if not len_ok(df, min_bars):
            out.update({"reason": "not_enough_bars", "have": len(df), "need": min_bars})
            return out

        close_s = df["close"].astype(float)
        high_s = df["high"].astype(float)
        vol_parts = _estimated_buy_sell_volume(df)
        atr_s = atr(df, atr_len)
        ema_trend = ema(close_s, trend_ema_len)

        delta_roll = vol_parts["delta_vol"].rolling(delta_win, min_periods=delta_win).sum()
        total_roll = vol_parts["tot_vol"].rolling(delta_win, min_periods=delta_win).sum()

        close = nz(last(close_s))
        hh = nz(last(high_s.rolling(delta_win, min_periods=delta_win).max()))
        atr_v = nz(last(atr_s))
        ema_v = nz(last(ema_trend))
        delta_sum = nz(last(delta_roll))
        total_sum = nz(last(total_roll), 0.0)

        if any(math.isnan(x) for x in [close, hh, atr_v, ema_v, delta_sum]) or total_sum <= 0:
            out["reason"] = "nan_or_zero_denominator"
            return out

        delta_frac = abs(delta_sum) / max(1e-12, total_sum)
        skew_ok = delta_frac >= min_delta_frac
        breakout_up = close > (hh + k_atr * atr_v)
        trend_ok = close > ema_v
        enter_long = bool(skew_ok and breakout_up and trend_ok)

        if enter_long and notional > 0:
            if dry:
                out.update({
                    "action": "paper_buy",
                    "close": round(close, 4),
                    "hh": round(hh, 4),
                    "atr": round(atr_v, 6),
                    "ema": round(ema_v, 4),
                    "delta_frac": round(delta_frac, 4),
                    "reason": "dry_run_delta_skew_breakout_trend_ok"
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
                    "delta_frac": round(delta_frac, 4),
                    "reason": "delta_skew_breakout_trend_ok"
                })
        else:
            out.update({
                "action": "flat",
                "close": round(close, 4),
                "hh": round(hh, 4),
                "atr": round(atr_v, 6),
                "ema": round(ema_v, 4),
                "delta_frac": round(delta_frac, 4),
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
                    "C4_DELTA_WIN": cfg.get("C4_DELTA_WIN"),
                    "C4_MIN_DELTA_FRAC": cfg.get("C4_MIN_DELTA_FRAC"),
                    "C4_BREAK_K_ATR": cfg.get("C4_BREAK_K_ATR"),
                    "C4_ATR_LEN": cfg.get("C4_ATR_LEN"),
                    "C4_TREND_EMA_LEN": cfg.get("C4_TREND_EMA_LEN"),
                    "ORDER_NOTIONAL": cfg.get("ORDER_NOTIONAL"),
                },
            },
        })
        return out
