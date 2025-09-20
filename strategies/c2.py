# strategies/c2.py
from __future__ import annotations
import math
from typing import Any, Dict, Iterable, List, Mapping
import numpy as np
import pandas as pd


# ---------- helpers ----------
def _to_param_dict(p: Any) -> Dict[str, Any]:
    """
    Normalize params into a dict. Supports:
      - dict
      - list like ["ema_len=34", {"limit": 600}]
      - None
    """
    if isinstance(p, Mapping):
        return dict(p)
    out: Dict[str, Any] = {}
    if isinstance(p, Iterable) and not isinstance(p, (str, bytes)):
        for item in p:
            if isinstance(item, Mapping):
                out.update(item)
            elif isinstance(item, str) and "=" in item:
                k, v = item.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def _as_int(d: Mapping[str, Any], key: str, default: int) -> int:
    v = d.get(key, default)
    try:
        return int(v)
    except Exception:
        return default


def _as_float(d: Mapping[str, Any], key: str, default: float) -> float:
    v = d.get(key, default)
    try:
        return float(v)
    except Exception:
        return default


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _atr(df: pd.DataFrame, atr_len: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(atr_len).mean()


def _extract_trade_ctx(pdict: Mapping[str, Any], kwargs: Dict[str, Any]):
    # dry/notional may appear in kwargs or params
    dry = bool(kwargs.get("dry", pdict.get("dry", True)))
    notional = kwargs.get("notional", pdict.get("notional"))
    try:
        notional = float(notional) if notional is not None else None
    except Exception:
        notional = None
    return dry, notional


# ---------- strategy ----------
def run(market, broker, symbols: Iterable[str], params, *args, **kwargs) -> List[Dict[str, Any]]:
    """
    C2: EMA + Highest-High + ATR breakout filter.
    Router calls: run(market, broker, symbols, params, dry=..., log=...)
    Returns: list[dict] one dict per symbol.
    """
    pdict = _to_param_dict(params)
    dry, notional = _extract_trade_ctx(pdict, kwargs)

    # Inputs / defaults
    timeframe = str(pdict.get("timeframe", "5Min"))
    limit = _as_int(pdict, "limit", 600)
    ema_len = _as_int(pdict, "ema_len", 34)
    hh_len = _as_int(pdict, "hh_len", 34)
    atr_len = _as_int(pdict, "atr_len", 14)
    atr_mult = _as_float(pdict, "atr_mult", 0.5)

    need = max(ema_len, hh_len, atr_len) + 2
    if limit < need:
        limit = need

    # Fetch candles (expects dict[symbol] -> DataFrame)
    bars: Dict[str, pd.DataFrame] = market.candles(symbols, timeframe=timeframe, limit=limit)

    results: List[Dict[str, Any]] = []
    for sym in symbols:
        df = bars.get(sym)
        if df is None or len(df) < need:
            results.append({
                "symbol": sym, "action": "flat", "reason": "insufficient_bars",
                "order_id": None
            })
            continue

        df = df.copy()
        df["ema"] = _ema(df["close"], ema_len)
        df["hh"] = df["high"].rolling(hh_len).max()
        df["atr"] = _atr(df, atr_len)

        last = df.iloc[-1]
        close = float(last["close"])
        ema = float(last["ema"])
        hh = float(last["hh"])
        atr = float(last["atr"]) if math.isfinite(float(last["atr"])) else float("nan")

        action = "flat"
        reason = "no_signal"

        if np.isfinite(atr) and close > hh and close > ema:
            action = "buy"
            reason = "breakout_long"
        elif np.isfinite(atr) and close < ema - atr_mult * atr:
            action = "flat"
            reason = "below_ema_atr"

        results.append({
            "symbol": sym,
            "action": action,
            "reason": reason,
            "close": close,
            "ema": ema,
            "hh": hh,
            "atr": atr,
            "order_id": None,
            "dry": dry,
            "notional": notional
        })

    return results
