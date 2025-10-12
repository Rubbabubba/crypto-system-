#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
utils_volatility.py — ATR & volatility helpers
Build: v2.0.0 (2025-10-11, America/Chicago)

Supports:
- Bars as list[dict]: [{t,o,h,l,c,v}] (epoch seconds in 't', floats in o/h/l/c/v)
- Bars as pandas.DataFrame with columns: ['open','high','low','close','volume'] (ts index or 'ts'/'t' column)

Public API
- to_df(bars) -> pd.DataFrame
- true_range_df(df) -> pd.Series
- atr_series_from_df(df, n=14) -> pd.Series
- atr_from_bars(bars, n=14, return_series=False) -> float | pd.Series
- hl2_series(df) -> pd.Series
"""

from __future__ import annotations

__version__ = "2.0.0"

from typing import Any, Iterable, Union
import pandas as pd
import numpy as np

__all__ = [
    "to_df",
    "true_range_df",
    "atr_series_from_df",
    "atr_from_bars",
    "hl2_series",
]

# -----------------------------------------------------------------------------
# Normalization
# -----------------------------------------------------------------------------
def to_df(bars: Union[pd.DataFrame, Iterable[dict]]) -> pd.DataFrame:
    """
    Convert bars into a normalized DataFrame with UTC timestamp index and columns:
    ['open','high','low','close','volume']
    """
    if isinstance(bars, pd.DataFrame):
        df = bars.copy()
        # time column inference
        if "ts" in df.columns:
            idx = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            df = df.drop(columns=[c for c in ["ts", "t", "time"] if c in df.columns])
            df.index = idx
        elif "t" in df.columns:
            idx = pd.to_datetime(df["t"], unit="s", utc=True, errors="coerce")
            df = df.drop(columns=[c for c in ["ts", "t", "time"] if c in df.columns])
            df.index = idx
        elif "time" in df.columns:
            idx = pd.to_datetime(df["time"], utc=True, errors="coerce")
            df = df.drop(columns=[c for c in ["ts", "t", "time"] if c in df.columns])
            df.index = idx
        elif not isinstance(df.index, pd.DatetimeIndex):
            # try to coerce index
            df.index = pd.to_datetime(df.index, utc=True, errors="coerce")

        # rename o/h/l/c/v if present
        rename_map = {"o":"open","h":"high","l":"low","c":"close","v":"volume","vol":"volume"}
        df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

        # ensure columns
        for col in ["open","high","low","close","volume"]:
            if col not in df.columns:
                df[col] = np.nan

        return df[["open","high","low","close","volume"]].sort_index()

    # Iterable[dict]
    rows = list(bars or [])
    if not rows:
        return pd.DataFrame(columns=["open","high","low","close","volume"])

    df = pd.DataFrame(rows)
    # timestamps
    if "ts" in df.columns:
        idx = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    elif "t" in df.columns:
        idx = pd.to_datetime(df["t"], unit="s", utc=True, errors="coerce")
    elif "time" in df.columns:
        idx = pd.to_datetime(df["time"], utc=True, errors="coerce")
    else:
        idx = pd.to_datetime(df.index, utc=True, errors="coerce")

    df.index = idx
    # rename
    rename_map = {"o":"open","h":"high","l":"low","c":"close","v":"volume","vol":"volume"}
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    for col in ["open","high","low","close","volume"]:
        if col not in df.columns:
            df[col] = np.nan

    return df[["open","high","low","close","volume"]].sort_index()


# -----------------------------------------------------------------------------
# Core calculations
# -----------------------------------------------------------------------------
def true_range_df(df: pd.DataFrame) -> pd.Series:
    """
    Wilder's True Range as a Series aligned to df.index.
    TR = max( high - low, abs(high - prev_close), abs(low - prev_close) )
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)

    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr


def atr_series_from_df(df: pd.DataFrame, n: int = 14) -> pd.Series:
    """
    Wilder's ATR(n) as a Series (RMA of TR over window n).
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)

    n = max(int(n), 1)
    tr = true_range_df(df)

    # Wilder's RMA: initial value = mean(TR[0:n]), then recursive smoothing
    atr = tr.ewm(alpha=1.0/n, adjust=False, min_periods=n).mean()
    return atr


def atr_from_bars(bars: Union[pd.DataFrame, Iterable[dict]], n: int = 14, return_series: bool = False) -> Union[float, pd.Series]:
    """
    Convenience wrapper:
    - Accepts list-of-dicts or DataFrame.
    - Computes ATR(n) series; returns the last value by default, or the full Series when return_series=True.
    """
    df = to_df(bars)
    if df.empty:
        return pd.Series(dtype=float) if return_series else float("nan")

    atr = atr_series_from_df(df, n=n)
    return atr if return_series else (float(atr.iloc[-1]) if len(atr) else float("nan"))


def hl2_series(df: pd.DataFrame) -> pd.Series:
    """
    (high + low) / 2 as a Series.
    """
    if df is None or df.empty:
        return pd.Series(dtype=float)
    return (df["high"].astype(float) + df["low"].astype(float)) / 2.0
