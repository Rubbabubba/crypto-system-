#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
vol_filter.py — Volatility/ATR filters
Build: v2.0.0 (2025-10-11, America/Chicago)

Purpose
- Lightweight volatility gates you can call from strategies or the scan bridge.

Inputs
- Accepts bars as list[dict] with keys {t,o,h,l,c,v} OR a pandas DataFrame
  with columns ['open','high','low','close','volume'] and a datetime index
  or a 'ts'/'t' column.

Public API
- atr_value(bars, n=14) -> float
- atr_pct(bars, n=14, use_close=True) -> float
- volatility_regime(bars, n=14, slope_lookback=10) -> dict
    Returns: {'atr': float, 'atr_pct': float, 'rising': bool, 'slope': float}
- passes_volatility(bars, min_atr=None, min_atr_pct=None, n=14, slope_lookback=10) -> bool
"""

from __future__ import annotations

__version__ = "2.0.0"

from typing import Any, Dict, Optional, Iterable, Union
import numpy as np
import pandas as pd

from utils_volatility import to_df, atr_from_bars, atr_series_from_df

__all__ = [
    "atr_value",
    "atr_pct",
    "volatility_regime",
    "passes_volatility",
]

# -----------------------------------------------------------------------------
# Core helpers
# -----------------------------------------------------------------------------
def _as_df(bars: Union[pd.DataFrame, Iterable[dict]]) -> pd.DataFrame:
    return to_df(bars)

def atr_value(bars: Union[pd.DataFrame, Iterable[dict]], n: int = 14) -> float:
    """Return the latest ATR(n) value (float), or NaN if unavailable."""
    return float(atr_from_bars(bars, n=n, return_series=False))

def atr_pct(bars: Union[pd.DataFrame, Iterable[dict]], n: int = 14, use_close: bool = True) -> float:
    """
    Return ATR% = 100 * ATR / ref_price, where ref_price is the latest close (default)
    or HL2 if use_close=False.
    """
    df = _as_df(bars)
    if df.empty:
        return float("nan")
    atr = atr_value(df, n=n)
    ref = float(df["close"].iloc[-1]) if use_close else float((df["high"].iloc[-1] + df["low"].iloc[-1]) / 2.0)
    if ref <= 0 or np.isnan(ref) or np.isnan(atr):
        return float("nan")
    return 100.0 * atr / ref

def _slope(series: pd.Series) -> float:
    """
    Return simple linear slope over the series index order (unitless; per step).
    Uses numpy.polyfit on [0..len-1] vs values.
    """
    y = series.astype(float).values
    x = np.arange(len(y), dtype=float)
    if len(y) < 2:
        return 0.0
    m, _ = np.polyfit(x, y, 1)
    return float(m)

# -----------------------------------------------------------------------------
# Regime & gating
# -----------------------------------------------------------------------------
def volatility_regime(
    bars: Union[pd.DataFrame, Iterable[dict]],
    n: int = 14,
    slope_lookback: int = 10,
) -> Dict[str, float | bool]:
    """
    Compute a simple volatility regime snapshot:
      - atr: latest ATR(n)
      - atr_pct: ATR as % of price
      - rising: True if ATR is trending up over the last `slope_lookback`
      - slope: raw slope of ATR over the last `slope_lookback`
    """
    df = _as_df(bars)
    if df.empty:
        return {"atr": float("nan"), "atr_pct": float("nan"), "rising": False, "slope": 0.0}

    atr_series = atr_series_from_df(df, n=n).dropna()
    if atr_series.empty:
        return {"atr": float("nan"), "atr_pct": float("nan"), "rising": False, "slope": 0.0}

    look = max(int(slope_lookback), 2)
    tail = atr_series.tail(look)
    slope = _slope(tail)
    atr_val = float(atr_series.iloc[-1])
    apct = atr_pct(df, n=n, use_close=True)

    return {"atr": atr_val, "atr_pct": apct, "rising": bool(slope > 0), "slope": slope}

def passes_volatility(
    bars: Union[pd.DataFrame, Iterable[dict]],
    min_atr: Optional[float] = None,
    min_atr_pct: Optional[float] = None,
    n: int = 14,
    slope_lookback: int = 10,
) -> bool:
    """
    Decide if the instrument has "enough" volatility:
      - If min_atr is set, require ATR >= min_atr
      - If min_atr_pct is set, require ATR% >= min_atr_pct
      - Require rising ATR trend over the last `slope_lookback` bars

    Any unset threshold is ignored.
    """
    snap = volatility_regime(bars, n=n, slope_lookback=slope_lookback)

    if min_atr is not None:
        if not (isinstance(snap["atr"], float) and snap["atr"] >= float(min_atr)):
            return False

    if min_atr_pct is not None:
        apct = snap["atr_pct"]
        if not (isinstance(apct, float) and apct >= float(min_atr_pct)):
            return False

    if not snap["rising"]:
        return False

    return True
