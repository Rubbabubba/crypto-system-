from __future__ import annotations

import logging
from typing import Optional, Literal

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Optional broker import (never fatal)
try:
    import broker as br  # if you have a local broker helper module
except Exception as e:
    logger.debug(f"[utils_volatility] broker import skipped: {e}")
    br = None


# -----------------------------
# Basic indicators & utilities
# -----------------------------

def sma(series: pd.Series, window: int) -> pd.Series:
    """Simple Moving Average."""
    s = _to_series(series)
    return s.rolling(window=window, min_periods=window).mean()


def ema(series: pd.Series, span: int) -> pd.Series:
    """Exponential Moving Average (alpha=2/(span+1))."""
    s = _to_series(series)
    return s.ewm(span=span, adjust=False, min_periods=span).mean()


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Wilder's RSI, period in bars."""
    s = _to_series(series)
    delta = s.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)

    # Wilder's smoothing via ewm(alpha=1/period)
    gain = up.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    loss = down.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()

    rs = gain / loss.replace(0.0, np.nan)
    out = 100.0 - (100.0 / (1.0 + rs))
    return out.fillna(50.0)


def pct_change(series: pd.Series) -> pd.Series:
    """Percent change in % units."""
    s = _to_series(series)
    return s.pct_change() * 100.0


# -----------------------------
# True Range, ATR, ATR percent
# -----------------------------

def true_range(high: pd.Series, low: pd.Series, close: pd.Series) -> pd.Series:
    """True range series from H, L, C (C is current close; previous close is close.shift(1))."""
    h = _to_series(high)
    l = _to_series(low)
    c_prev = _to_series(close).shift(1)

    tr1 = h - l
    tr2 = (h - c_prev).abs()
    tr3 = (l - c_prev).abs()

    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr


def atr(
    df: pd.DataFrame,
    period: int = 14,
    method: Literal["ema", "sma"] = "ema",
    hi_col: str = "high",
    lo_col: str = "low",
    cl_col: str = "close",
) -> pd.Series:
    """
    Average True Range (Wilder-style by default using EMA).
    Expects df with columns: high, low, close (name overrideable).
    """
    _require_cols(df, [hi_col, lo_col, cl_col], name="atr")
    tr = true_range(df[hi_col], df[lo_col], df[cl_col])
    if method == "ema":
        out = tr.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    else:
        out = tr.rolling(window=period, min_periods=period).mean()
    return out


def atr_pct(
    df: pd.DataFrame,
    period: int = 14,
    method: Literal["ema", "sma"] = "ema",
    hi_col: str = "high",
    lo_col: str = "low",
    cl_col: str = "close",
) -> pd.Series:
    """
    ATR as a percent of price (close), i.e., 100 * ATR / close.
    """
    _require_cols(df, [hi_col, lo_col, cl_col], name="atr_pct")
    a = atr(df, period=period, method=method, hi_col=hi_col, lo_col=lo_col, cl_col=cl_col)
    return (a / df[cl_col].replace(0.0, np.nan)) * 100.0


# -----------------------------
# Rolling volatility (stdev)
# -----------------------------

def rolling_vol(
    series: pd.Series,
    window: int = 20,
    pct_inputs: bool = False,
    annualize: bool = False,
    periods_per_year: int = 365,  # daily by default; set 365*24 for hourly
) -> pd.Series:
    """
    Rolling volatility (standard deviation). If pct_inputs=False, converts to % via pct_change first.
    If annualize=True, multiplies by sqrt(periods_per_year).
    """
    s = _to_series(series)
    returns = s if pct_inputs else s.pct_change() * 100.0
    vol = returns.rolling(window=window, min_periods=window).std()
    if annualize:
        vol = vol * np.sqrt(periods_per_year)
    return vol


# -----------------------------
# Expected move helpers
# -----------------------------

def expected_move_from_atr_pct(atr_pct_value: float, k: float = 1.0) -> float:
    """
    Turn an ATR% into a single-trade expected move in % (linear scale).
    For quick guards, k=1.0 is fine; you can tune k from backtests.
    """
    try:
        return float(atr_pct_value) * float(k)
    except Exception:
        return np.nan


def edge_vs_fee_ok(
    expected_move_pct: Optional[float],
    fee_rate_pct: float = 0.26,
    multiple: float = 3.0,
) -> bool:
    """
    Fee guard: require expected_move_pct >= multiple * fee_rate_pct.
    If expected_move_pct is None, returns True (skip check).
    """
    if expected_move_pct is None:
        return True
    try:
        return float(expected_move_pct) >= (float(multiple) * float(fee_rate_pct))
    except Exception:
        return False


# -----------------------------
# Convenience wrappers used by strategies (non-IO)
# -----------------------------

def get_atr_pct(
    df: pd.DataFrame,
    period: int = 14,
    method: Literal["ema", "sma"] = "ema",
    hi_col: str = "high",
    lo_col: str = "low",
    cl_col: str = "close",
) -> float:
    """
    Returns the latest ATR% value from a dataframe of OHLC.
    """
    series = atr_pct(df, period=period, method=method, hi_col=hi_col, lo_col=lo_col, cl_col=cl_col)
    try:
        return float(series.iloc[-1])
    except Exception:
        return np.nan


def last(series: pd.Series, default: float = np.nan) -> float:
    """Return the last value of a Series as float."""
    try:
        return float(_to_series(series).iloc[-1])
    except Exception:
        return default


# -----------------------------
# Internal helpers
# -----------------------------

def _to_series(x) -> pd.Series:
    if isinstance(x, pd.Series):
        return x
    if isinstance(x, (list, tuple, np.ndarray)):
        return pd.Series(x)
    # If scalar, broadcast to length-1 series
    return pd.Series([x])


def _require_cols(df: pd.DataFrame, cols: list[str], name: str = ""):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"[utils_volatility.{name}] missing columns: {missing}. "
                       f"Have: {list(df.columns)}")
