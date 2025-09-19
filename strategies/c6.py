"""
Strategy C6 — Trend-follow with ATR breakout & trailing logic (scan-style)
Version: 1.0.1
- Uses market.candles(...) (or market.get_bars via compat shim)
- Accepts: timeframe, limit, ma_len, atr_len, atr_mult
"""

from __future__ import annotations
from typing import Dict, Any, List
import pandas as pd
import numpy as np

DEFAULTS = dict(
    timeframe="5Min",
    limit=600,
    ma_len=50,
    atr_len=14,
    atr_mult=2.0,
)

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _atr(h: pd.Series, l: pd.Series, c: pd.Series, n: int) -> pd.Series:
    high_low = (h - l).abs()
    high_close = (h - c.shift()).abs()
    low_close = (l - c.shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()

def _try_buy(broker, symbol: str, *, notional: float | None, qty: float | None):
    if notional is not None:
        if hasattr(broker, "market_buy_notional"):
            return broker.market_buy_notional(symbol, notional=notional)
        if hasattr(broker, "submit_order"):
            return broker.submit_order(symbol=symbol, side="buy", type="market",
                                       notional=notional, time_in_force="gtc")
    if qty is not None:
        if hasattr(broker, "market_buy"):
            return broker.market_buy(symbol, qty=qty)
        if hasattr(broker, "submit_order"):
            return broker.submit_order(symbol=symbol, side="buy", type="market",
                                       qty=qty, time_in_force="gtc")
    return {"status": "no_broker_method"}

def run(market, broker, symbols: List[str], params: Dict[str, Any], *, dry: bool, pwrite=print, log=print):
    cfg = {**DEFAULTS, **(params or {})}
    tf = cfg["timeframe"]
    limit = int(cfg["limit"])
    ma_len = int(cfg["ma_len"])
    atr_len = int(cfg["atr_len"])
    atr_mult = float(cfg["atr_mult"])
    notional = float(cfg.get("notional")) if cfg.get("notional") else None
    qty = float(cfg.get("qty")) if cfg.get("qty") else None

    if hasattr(market, "candles"):
        data = market.candles(symbols=symbols, timeframe=tf, limit=limit)
    else:
        data = market.get_bars(symbols=symbols, timeframe=tf, limit=limit)

    out = []
    for sym in symbols:
        df = data.get(sym)
        if df is None or len(df) == 0:
            out.append({"symbol": sym, "action": "error", "error": "no_data"})
            continue

        h = df["high"].astype(float)
        l = df["low"].astype(float)
        c = df["close"].astype(float)

        ma = _ema(c, ma_len)
        atr = _atr(h, l, c, atr_len)

        last_close = float(c.iloc[-1])
        last_ma = float(ma.iloc[-1])
        last_atr = float(atr.iloc[-1]) if not np.isnan(atr.iloc[-1]) else None

        # Simple breakout logic: price above MA + ATR band
        upper_band = last_ma + atr_mult * (last_atr if last_atr is not None else 0.0)
        lower_band = last_ma - atr_mult * (last_atr if last_atr is not None else 0.0)

        action = "flat"
        reason = "no_signal"
        order_id = None

        if last_atr is not None:
            if last_close > upper_band:
                action = "buy"
                reason = "trend_breakout_up"
                if not dry:
                    order_id = _try_buy(broker, sym, notional=notional, qty=qty)
            elif last_close < lower_band:
                action = "flat"  # in this simplified scan we exit via c5’s sell rule or a separate exit strategy
                reason = "trend_breakdown"

        out.append({
            "symbol": sym,
            "action": action,
            "reason": reason,
            "close": last_close,
            "ma": round(last_ma, 4),
            "atr": round(last_atr, 6) if last_atr is not None else None,
            "upper": round(upper_band, 4) if last_atr is not None else None,
            "lower": round(lower_band, 4) if last_atr is not None else None,
            "order_id": order_id if not dry else None,
        })

    return {"ok": True, "force": False, "dry": dry, "results": out, "strategy": "c6"}
