# strategies/c3.py
from __future__ import annotations
import math
from typing import Any, Dict, Iterable, List, Mapping
import numpy as np
import pandas as pd


# ---------- helpers ----------
def _to_param_dict(p: Any) -> Dict[str, Any]:
    if isinstance(p, dict):
        return dict(p)
    out: Dict[str, Any] = {}
    if isinstance(p, Iterable) and not isinstance(p, (str, bytes)):
        for item in p:
            if isinstance(item, dict):
                out.update(item)
            elif isinstance(item, str) and "=" in item:
                k, v = item.split("=", 1)
                out[k.strip()] = v.strip()
    return out


def _as_int(d: Mapping[str, Any], key: str, default: int) -> int:
    try:
        return int(d.get(key, default))
    except Exception:
        return default


def _sma(series: pd.Series, n: int) -> pd.Series:
    return series.rolling(n).mean()


def _atr(df: pd.DataFrame, atr_len: int) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev_c = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev_c).abs(), (l - prev_c).abs()], axis=1).max(axis=1)
    return tr.rolling(atr_len).mean()


def _extract_trade_ctx(pdict: Mapping[str, Any], kwargs: Dict[str, Any]):
    dry = bool(kwargs.get("dry", pdict.get("dry", True)))
    notional = kwargs.get("notional", pdict.get("notional"))
    try:
        notional = float(notional) if notional is not None else None
    except Exception:
        notional = None
    return dry, notional


# ---------- strategy ----------
def run(market, broker, symbols: Iterable[str], params, *args, **kwargs) -> Dict[str, Any]:
    """
    C3: MA( fast/slow ) cross with ATR context.
    Returns dict with 'ok' and 'results'.
    """
    pdict = _to_param_dict(params)
    dry, notional = _extract_trade_ctx(pdict, kwargs)

    timeframe = str(pdict.get("timeframe", "5Min"))
    limit = _as_int(pdict, "limit", 600)
    ma_fast = _as_int(pdict, "ma_fast", 20)
    ma_slow = _as_int(pdict, "ma_slow", 50)
    atr_len = _as_int(pdict, "atr_len", 14)

    need = max(ma_fast, ma_slow, atr_len) + 2
    if limit < need:
        limit = need

    bars: Dict[str, pd.DataFrame] = market.candles(symbols, timeframe=timeframe, limit=limit)

    results: List[Dict[str, Any]] = []
    for sym in symbols:
        df = bars.get(sym)
        if df is None or len(df) < need:
            results.append({"symbol": sym, "action": "flat", "reason": "insufficient_bars", "order_id": None})
            continue

        df = df.copy()
        df["ma1"] = _sma(df["close"], ma_fast)
        df["ma2"] = _sma(df["close"], ma_slow)
        df["atr"] = _atr(df, atr_len)

        last = df.iloc[-1]
        prev = df.iloc[-2]

        close = float(last["close"])
        ma1 = float(last["ma1"])
        ma2 = float(last["ma2"])
        atr = float(last["atr"]) if math.isfinite(float(last["atr"])) else float("nan")

        prev_ma1 = float(prev["ma1"])
        prev_ma2 = float(prev["ma2"])

        crossed_up = prev_ma1 <= prev_ma2 and ma1 > ma2
        crossed_down = prev_ma1 >= prev_ma2 and ma1 < ma2

        action = "flat"
        reason = "no_signal"
        if crossed_up:
            action = "buy"
            reason = "ma_cross_up"
        elif crossed_down:
            action = "sell"
            reason = "ma_cross_down"

        results.append({
            "symbol": sym,
            "action": action,
            "reason": reason,
            "close": close,
            "ma1": ma1,
            "ma2": ma2,
            "atr": atr,
            "order_id": None,
            "dry": dry,
            "notional": notional
        })

    return {
        "ok": True,
        "strategy": "c3",
        "dry": dry,
        "results": results
    }
