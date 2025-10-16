# strategies/utils.py
from __future__ import annotations
from typing import Any, Optional, Dict, List
import numpy as np
import pandas as pd

__all__ = ["last", "as_bool", "nz", "ensure_df_has", "len_ok", "safe_float"]

def last(x: Any, default: Optional[float] = None) -> Optional[float]:
    """Return the last scalar value from a pandas Series/array/list, else default."""
    try:
        if x is None:
            return default
        if isinstance(x, (pd.Series, pd.Index)):
            return x.iloc[-1] if len(x) else default
        if isinstance(x, (list, tuple, np.ndarray)):
            return x[-1] if len(x) else default
        return x  # assume scalar
    except Exception:
        return default

def as_bool(x: Any) -> bool:
    """Safely convert Series/array to a single bool."""
    if isinstance(x, pd.Series):
        return bool(x.iloc[-1]) if len(x) else False
    if isinstance(x, (list, tuple, np.ndarray)):
        return bool(np.asarray(x).any())
    return bool(x)

def nz(x: Optional[float], fill: float = 0.0) -> float:
    """Replace NaN/None with fill."""
    try:
        if x is None:
            return fill
        return fill if (isinstance(x, float) and np.isnan(x)) else float(x)
    except Exception:
        return fill

def ensure_df_has(df: pd.DataFrame, cols: List[str]) -> bool:
    """Quick guard for required columns."""
    if df is None or df.empty:
        return False
    return all(c in df.columns for c in cols)

def len_ok(df: pd.DataFrame, need: int) -> bool:
    return df is not None and len(df) >= need

def safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default
