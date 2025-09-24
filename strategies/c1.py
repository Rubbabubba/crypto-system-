# strategies/c1.py
# Version: 1.9.0-crypto-s1
# Purpose: Crypto analog of S1 (VWAP + EMA20 slope + sigma band) with explicit exits.
#
# Entries (Either-mode logic):
#   • Reversion: close crosses up through VWAP and EMA20 slope >= min_slope
#   • Trend:     close > VWAP, EMA20 slope >= min_slope, and a recent "touch" of the lower VWAP sigma band
#
# Exits (choose-first):
#   • EMA cross-down: close <= EMA20 (on bar close)
#   • ATR trailing stop: trail updates upward only; sell if close <= trail
#
# Notes:
#   • Works on bar-close signals. Evaluate with the exact timeframe you intend (default 5Min).
#   • Cooldown (if set) only suppresses NEW entries, never exits.
#   • State carries entry_price and trail_price; your engine should persist state per (symbol, strategy).
#
# Environment Overrides (all optional):
#   C1_TIMEFRAME          default: "5Min"
#   C1_EMA_LEN            default: 20
#   C1_EMA_SLOPE_MIN     default: 0.0          (non-negative slope)
#   C1_VWAP_SIGMA        default: 1.0          (band width as stdev multiple)
#   C1_BAND_LOOKBACK     default: 10           (bars to look back for a band "touch")
#   C1_ATR_LEN           default: 14
#   C1_ATR_STOP_MULT     default: 1.0
#   C1_COOLDOWN_SEC      default: 0            (blocks entries only)
#   AUTORUN_NOTIONAL / NOTIONAL_DEFAULT  (fallback sizing)
#
# Adapter contract (return value):
#   {
#     "action": "buy" | "sell" | "hold",
#     "symbol": str,
#     "reason": str,
#     "timeframe": str,
#     "notional": float | None,
#     "state": dict,   # persist this between calls per symbol
#   }
#
# Required input (typical):
#   symbol: "BTC/USD"
#   df: pandas.DataFrame with columns ["open","high","low","close","volume"], indexed by timestamp ascending
#   position: dict or None (expects position.get("qty", 0) > 0 for in-position)
#   state: dict or None; we will read/write keys: entry_price, trail_price, last_entry_ts
#
# Safe defaults allow drop-in evaluation in paper mode.

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Dict, Any, Optional

import numpy as np
import pandas as pd


# ---------- Utilities ----------

def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()

def _atr(df: pd.DataFrame, length: int) -> pd.Series:
    # df must have high, low, close
    high = df["high"]
    low = df["low"]
    close = df["close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(length).mean()

def _vwap(df: pd.DataFrame) -> pd.Series:
    # Typical intraday VWAP analog for crypto bars
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    vol = df["volume"].replace(0, np.nan)  # avoid division by zero
    pv = tp * vol
    cum_pv = pv.cumsum()
    cum_v = vol.cumsum()
    vwap = cum_pv / cum_v
    return vwap.fillna(method="ffill").fillna(df["close"])

def _zscore(series: pd.Series, window: int) -> pd.Series:
    rolling = series.rolling(window)
    mean = rolling.mean()
    std = rolling.std(ddof=0)
    return (series - mean) / (std.replace(0, np.nan))

def _get_env_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, default))
    except Exception:
        return default

def _get_env_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, default))
    except Exception:
        return default

def _get_env_str(key: str, default: str) -> str:
    v = os.getenv(key)
    return v if v else default

def _in_position(position: Optional[Dict[str, Any]]) -> bool:
    if not position:
        return False
    try:
        return float(position.get("qty", 0)) > 0
    except Exception:
        return False


# ---------- Parameters ----------

@dataclass
class Params:
    timeframe: str = _get_env_str("C1_TIMEFRAME", "5Min")
    ema_len: int = _get_env_int("C1_EMA_LEN", 20)
    ema_slope_min: float = _get_env_float("C1_EMA_SLOPE_MIN", 0.0)
    vwap_sigma: float = _get_env_float("C1_VWAP_SIGMA", 1.0)
    band_lookback: int = _get_env_int("C1_BAND_LOOKBACK", 10)
    atr_len: int = _get_env_int("C1_ATR_LEN", 14)
    atr_stop_mult: float = _get_env_float("C1_ATR_STOP_MULT", 1.0)
    cooldown_sec: int = _get_env_int("C1_COOLDOWN_SEC", 0)
    notional: Optional[float] = None  # filled from AUTORUN_NOTIONAL/NOTIONAL_DEFAULT if None

    @staticmethod
    def with_env_fallbacks(**overrides) -> "Params":
        p = Params()
        for k, v in overrides.items():
            setattr(p, k, v)
        if p.notional is None:
            # prefer AUTORUN_NOTIONAL, fallback to NOTIONAL_DEFAULT
            n = os.getenv("AUTORUN_NOTIONAL") or os.getenv("NOTIONAL_DEFAULT")
            if n:
                try:
                    p.notional = float(n)
                except Exception:
                    p.notional = None
        return p


# ---------- Core Logic ----------

def _compute_indicators(df: pd.DataFrame, p: Params) -> pd.DataFrame:
    out = df.copy()

    # EMA and slope (difference over 1 bar; adjust if you prefer longer slope window)
    out["ema"] = _ema(out["close"], p.ema_len)
    out["ema_slope"] = out["ema"] - out["ema"].shift(1)

    # VWAP and sigma band using z-score of (close - vwap)
    out["vwap"] = _vwap(out)
    dev = out["close"] - out["vwap"]
    # 20-bar zscore by default (can be tuned via ema_len or a new param if desired)
    z = _zscore(dev, max(20, p.ema_len))
    out["vw_z"] = z

    # Define lower band threshold as -vwap_sigma (below VWAP)
    out["lower_band_touch"] = (out["vw_z"] <= -abs(p.vwap_sigma))

    # ATR for stops
    out["atr"] = _atr(out, p.atr_len)

    return out


def _entry_signal(row_prev: pd.Series, row: pd.Series, p: Params, now_ts: pd.Timestamp,
                  state: Dict[str, Any]) -> (bool, str):
    """
    Either-mode entry:
      A) Reversion: crossed up through VWAP & ema_slope >= min
      B) Trend: close > vwap & ema_slope >= min & recent touch of lower band
    """
    # Cooldown for entries (never blocks exits)
    last_entry_ts = state.get("last_entry_ts")
    if p.cooldown_sec and last_entry_ts:
        if (now_ts.value // 10**9) - int(last_entry_ts) < p.cooldown_sec:
            return False, "cooldown"

    crossed_up = (row_prev["close"] < row_prev["vwap"]) and (row["close"] > row["vwap"])
    ema_slope_ok = row["ema_slope"] >= p.ema_slope_min

    reversion_ok = crossed_up and ema_slope_ok

    # Trend leg requires a "recent touch" of the lower band
    touched_recent = bool(state.get("touched_recent", False))

    trend_ok = (row["close"] > row["vwap"]) and ema_slope_ok and touched_recent

    if reversion_ok or trend_ok:
        return True, "entry:reversion" if reversion_ok else "entry:trend"
    return False, "no-entry"


def _update_band_touch_state(df: pd.DataFrame, idx: int, p: Params, state: Dict[str, Any]) -> None:
    """
    Track whether we've seen a lower-band touch within the recent lookback window.
    """
    start = max(0, idx - p.band_lookback)
    window = df.iloc[start:idx+1]
    state["touched_recent"] = bool(window["lower_band_touch"].any())


def _exit_conditions(row_prev: pd.Series, row: pd.Series, p: Params,
                     in_pos: bool, state: Dict[str, Any]) -> (bool, str):
    if not in_pos:
        return False, "flat"

    # 1) EMA cross-down exit on close
    crossdown = (row_prev["close"] > row_prev["ema"]) and (row["close"] <= row["ema"])
    if crossdown:
        return True, "exit:ema-crossdown"

    # 2) ATR trailing exit
    trail = state.get("trail_price")
    if trail is not None and row["close"] <= trail:
        return True, "exit:atr-trail"

    return False, "hold"


def _update_trail_on_bar_close(row: pd.Series, p: Params, state: Dict[str, Any]) -> None:
    """
    Set or update a trailing stop: trail = close - ATR * mult, only ratchets upward.
    """
    if p.atr_stop_mult <= 0:
        return
    if pd.isna(row["atr"]):
        return
    proposed = float(row["close"] - p.atr_stop_mult * row["atr"])
    prev = state.get("trail_price")
    if prev is None:
        state["trail_price"] = proposed
    else:
        state["trail_price"] = max(prev, proposed)


def _on_entry_bookkeeping(row: pd.Series, state: Dict[str, Any]) -> None:
    state["entry_price"] = float(row["close"])
    state["last_entry_ts"] = int(time.time())
    # Initialize trail right away (so the next bar will have a valid trail check)
    state["trail_price"] = None  # set on next bar close by _update_trail_on_bar_close


# ---------- Public API ----------

def signal(symbol: str,
           df: pd.DataFrame,
           position: Optional[Dict[str, Any]] = None,
           state: Optional[Dict[str, Any]] = None,
           params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Core strategy entry/exit signal.
    """
    if state is None:
        state = {}

    # Merge params with env fallbacks
    p = Params.with_env_fallbacks(**(params or {}))

    # Basic validations
    needed = {"open", "high", "low", "close", "volume"}
    if not isinstance(df, pd.DataFrame) or not needed.issubset(df.columns):
        return {
            "action": "hold",
            "symbol": symbol,
            "reason": "insufficient-data",
            "timeframe": p.timeframe,
            "notional": p.notional,
            "state": state,
        }
    if len(df) < max(50, p.ema_len + 5, p.atr_len + 5):
        return {
            "action": "hold",
            "symbol": symbol,
            "reason": "warmup",
            "timeframe": p.timeframe,
            "notional": p.notional,
            "state": state,
        }

    df = _compute_indicators(df.copy(), p)
    # Ensure strictly ascending index and no NaNs in the last two rows we use
    df = df.sort_index()
    if df.isna().tail(5).any().any():
        df = df.fillna(method="ffill").fillna(method="bfill")

    # Latest two bars (previous and current) for cross logic
    row_prev = df.iloc[-2]
    row = df.iloc[-1]
    now_ts = df.index[-1]

    # Maintain recent band-touch state
    _update_band_touch_state(df, len(df) - 1, p, state)

    in_pos = _in_position(position)

    # Update/ratchet trailing stop on bar close (even if flat, so it initializes after entry)
    _update_trail_on_bar_close(row, p, state)

    # Exits take priority when in position
    do_exit, why = _exit_conditions(row_prev, row, p, in_pos, state)
    if do_exit:
        # Clear entry/trail on exit; leave touched_recent state intact
        state.pop("entry_price", None)
        state.pop("trail_price", None)
        return {
            "action": "sell",
            "symbol": symbol,
            "reason": why,
            "timeframe": p.timeframe,
            "notional": None,  # sell-all; engine should compute qty from position
            "state": state,
        }

    # Entries if flat
    if not in_pos:
        do_entry, why = _entry_signal(row_prev, row, p, now_ts, state)
        if do_entry:
            _on_entry_bookkeeping(row, state)
            return {
                "action": "buy",
                "symbol": symbol,
                "reason": why,
                "timeframe": p.timeframe,
                "notional": p.notional,
                "state": state,
            }

    # Otherwise hold
    return {
        "action": "hold",
        "symbol": symbol,
        "reason": "no-signal",
        "timeframe": p.timeframe,
        "notional": p.notional,
        "state": state,
    }


# ---------- Adapter (optional) ----------
# If your app expects call signatures like:
#   evaluate(symbol, bars_df, position, state, **kwargs) -> dict
# or
#   run(symbol, bars_df, position, state, **kwargs) -> dict
# provide thin wrappers below. Adjust names to your app's dispatcher.

def evaluate(symbol: str,
             bars_df: pd.DataFrame,
             position: Optional[Dict[str, Any]] = None,
             state: Optional[Dict[str, Any]] = None,
             **kwargs) -> Dict[str, Any]:
    """
    Compatibility wrapper for engines calling `evaluate(...)`.
    """
    return signal(symbol, bars_df, position=position, state=state, params=kwargs or None)


def run(symbol: str,
        bars_df: pd.DataFrame,
        position: Optional[Dict[str, Any]] = None,
        state: Optional[Dict[str, Any]] = None,
        **kwargs) -> Dict[str, Any]:
    """
    Compatibility wrapper for engines calling `run(...)`.
    """
    return signal(symbol, bars_df, position=position, state=state, params=kwargs or None)