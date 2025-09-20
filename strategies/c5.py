# strategies/c5.py
# Version: 1.7.3
# Percent breakout above recent high with ATR filter.

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd


def _atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    h, l, c = df["high"], df["low"], df["close"]
    prev = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - prev).abs(), (l - prev).abs()], axis=1).max(axis=1)
    return tr.ewm(alpha=1/length, adjust=False).mean()


def run(market, broker, symbols: List[str], params: Dict[str, Any], *, dry: bool, log):
    tf = str(params.get("timeframe", "5Min"))
    limit = int(params.get("limit", 600))
    lookback = int(params.get("lookback", 50))
    pct = float(params.get("pct", 0.002))     # 0.2% over hh
    atr_mult = float(params.get("atr_mult", 0.0))
    notional = float(params.get("notional", 0.0))

    results: List[Dict[str, Any]] = []
    try:
        data = market.candles(symbols, timeframe=tf, limit=limit)
    except Exception as e:
        return {"ok": False, "strategy": "c5", "dry": dry, "error": f"candles fetch failed: {e}", "results": []}

    for sym in symbols:
        df = data.get(sym)
        if df is None or len(df) < max(lookback + 5, 30):
            results.append({"symbol": sym, "action": "error", "error": "no_data"})
            continue

        close = df["close"].astype(float)
        high = df["high"].astype(float)
        atr = _atr(df, 14)
        c = float(close.iloc[-1])
        hh = float(high.rolling(lookback).max().iloc[-2])  # prior-window high
        threshold = hh * (1.0 + pct)

        passes_atr = True
        if atr_mult > 0:
            passes_atr = (c - hh) >= (atr_mult * float(atr.iloc[-1]))

        action = "flat"
        reason = "no_signal"
        order_id: Any = None

        if c >= threshold and passes_atr:
            action = "buy"
            reason = "breakout"
            if not dry and notional > 0 and broker is not None:
                try:
                    order_id = broker.paper_buy(sym, notional=notional)
                except Exception as oe:
                    order_id = {"status": "order_error", "error": str(oe)}
            else:
                order_id = {"status": "paper_buy", "notional": notional}

        results.append({
            "symbol": sym,
            "action": action,
            "reason": reason,
            "close": round(c, 6),
            "hh": round(hh, 6),
            "atr": float(atr.iloc[-1]),
            "order_id": order_id,
        })

    return {"ok": True, "strategy": "c5", "dry": dry, "results": results}
