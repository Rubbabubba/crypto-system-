#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
utils.py — General helpers (no broker/symbol logic)
Build: v2.0.0 (2025-10-11, America/Chicago)

Exports (stable)
- last(x, default=None) -> scalar|None
- as_bool(x, default=False) -> bool
- nz(x, default=0.0) -> float
- ensure_df_has(df, cols: list[str]) -> bool
- len_ok(df, need: int) -> bool
- safe_float(v, default=0.0) -> float

Notes
- Any symbol mapping or timeframe normalization is now handled in `symbol_map.py`.
- Keep this module broker-agnostic and side-effect free.
"""

from __future__ import annotations

__version__ = "2.0.0"

from typing import Any, Optional, Dict, List, Iterable, Union
import numpy as np
import pandas as pd


__all__ = ["last", "as_bool", "nz", "ensure_df_has", "len_ok", "safe_float"]


# -----------------------------------------------------------------------------
# Scalars & coercion
# -----------------------------------------------------------------------------
def last(x: Any, default: Optional[float] = None) -> Optional[float]:
    """
    Return the last scalar value from a pandas Series/Index/array/list/tuple; otherwise default.
    """
    try:
        if x is None:
            return default
        if isinstance(x, (pd.Series, pd.Index)):
            return x.iloc[-1] if len(x) else default
        if isinstance(x, (list, tuple, np.ndarray)):
            return x[-1] if len(x) else default
        # scalar already
        return float(x)
    except Exception:
        return default


def as_bool(x: Any, default: bool = False) -> bool:
    """
    Best-effort bool parser for env/config inputs.
    Accepts: True/False/1/0/"1"/"0"/"true"/"false"/"yes"/"no"/None.
    """
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    if isinstance(x, (int, float)):
        return x != 0
    s = str(x).strip().lower()
    if s in ("1", "true", "t", "yes", "y", "on"):
        return True
    if s in ("0", "false", "f", "no", "n", "off", ""):
        return False
    return default


def nz(x: Any, default: float = 0.0) -> float:
    """
    Null-to-zero (or default) conversion for numeric fields.
    """
    try:
        if x is None or (isinstance(x, float) and np.isnan(x)):
            return default
        return float(x)
    except Exception:
        return default


def safe_float(v: Any, default: float = 0.0) -> float:
    """
    Safely cast to float with default on error.
    """
    try:
        return float(v)
    except Exception:
        return default


# -----------------------------------------------------------------------------
# DataFrame guards
# -----------------------------------------------------------------------------
def ensure_df_has(df: pd.DataFrame, cols: List[str]) -> bool:
    """
    Return True if df is non-empty and contains all required columns.
    """
    if df is None or df is pd.NA:
        return False
    try:
        if df.empty:
            return False
    except Exception:
        return False
    try:
        return all(c in df.columns for c in cols)
    except Exception:
        return False


def len_ok(df: pd.DataFrame, need: int) -> bool:
    """
    Return True if df has at least `need` rows.
    """
    try:
        return df is not None and hasattr(df, "__len__") and len(df) >= int(need)
    except Exception:
        return False
