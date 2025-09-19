"""
Strategy C5 — RSI + EMA + trading-session filter
Version: 1.0.1
- Uses market.candles(...) (or market.get_bars via compat shim)
- Accepts: timeframe, limit, rsi_len, rsi_buy, rsi_sell
- On live (dry=0) uses notional or qty via broker best-effort
"""

from __future__ import annotations
import math
from typing import Dict, Any, List
import pandas as pd
import numpy as np

DEFAULTS = dict(
    timeframe="5Min",
    limit=600,
    rsi_len=14,
    rsi_buy=55.0,
    rsi_sell=45.0,
)

def _ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()

def _rsi(s: pd.Series, n: int) -> pd.Series:
    delta = s.diff()
    up = (delta.clip(lower=0)).ewm(alpha=1/n, adjust=False).mean()
    dn = (-delta.clip(upper=0)).ewm(alpha=1/n, adjust=False).mean()
    rs = up / (dn.replace(0, np.nan))
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)

def _try_market_buy(broker, symbol: str, *, notional: float | None, qty: float | None):
    if notional is not None:
        for method in ("market_buy_notional", "market_buy"):
            if hasattr(broker, method):
                return getattr(broker, method)(symbol, notional=notional)
        # Generic submit_order path
        if hasattr(broker, "submit_order"):
            return broker.submit_order(symbol=symbol, side="buy", type="market",
                                       notional=notional, time_in_force="gtc")
    if qty is not None:
        for method in ("market_buy_qty", "market_buy"):
            if hasattr(broker, method):
                return getattr(broker, method)(symbol, qty=qty)
        if hasattr(broker, "submit_order"):
            return broker.submit_order(symbol=symbol, side="buy", type="market",
                                       qty=qty, time_in_force="gtc")
    return {"status": "no_broker_method"}

def _try_market_sell(broker, symbol: str, *, qty: float | None, notional: float | None):
    # Prefer qty for sell; fall back to notional if wrapper supports it
    if qty is not None:
        for method in ("market_sell_qty", "market_sell"):
            if hasattr(broker, method):
                return getattr(broker, method)(symbol, qty=qty)
        if hasattr(broker, "submit_order"):
            return broker.submit_order(symbol=symbol, side="sell", type="market",
                                       qty=qty, time_in_force="gtc")
    if notional is not None:
        for method in ("market_sell_notional", "market_sell"):
            if hasattr(broker, method):
                return getattr(broker, method)(symbol, notional=notional)
        if hasattr(broker, "submit_order"):
            return broker.submit_order(symbol=symbol, side="sell", type="market",
                                       notional=notional, time_in_force="gtc")
    return {"status": "no_broker_method"}

def _last_qty_for(broker, symbol: str) -> float | None:
    # If your broker exposes positions(), use it to find position qty for sells
    if hasattr(broker, "positions"):
        try:
            pos = broker.positions()
            for p in pos:
                if p.get("symbol") in (symbol, symbol.replace("/", "")) and p.get("side", "").lower() == "long":
                    q = p.get("qty") or p.get("quantity")
                    if q is not None:
                        return float(q)
        except Exception:
            pass
    return None

def run(market, broker, symbols: List[str], params: Dict[str, Any], *, dry: bool, pwrite=print, log=print):
    cfg = {**DEFAULTS, **(params or {})}
    tf = cfg["timeframe"]
    limit = int(cfg["limit"])
    rsi_len = int(cfg["rsi_len"])
    rsi_buy = float(cfg["rsi_buy"])
    rsi_sell = float(cfg["rsi_sell"])
    notional = float(cfg.get("notional")) if cfg.get("notional") else None
    qty = float(cfg.get("qty")) if cfg.get("qty") else None

    # Fetch candles (dict: symbol -> DataFrame with columns ts, open, high, low, close, volume)
    if hasattr(market, "candles"):
        data = market.candles(symbols=symbols, timeframe=tf, limit=limit)
    else:
        data = market.get_bars(symbols=symbols, timeframe=tf, limit=limit)

    results = []
    for sym in symbols:
        df = data.get(sym)
        if df is None or len(df) == 0:
            results.append({"symbol": sym, "action": "error", "error": "no_data"})
            continue

        # Indicators
        close = df["close"].astype(float)
        ema = _ema(close, 21)
        rsi = _rsi(close, rsi_len)

        last_close = float(close.iloc[-1])
        last_ema = float(ema.iloc[-1])
        last_rsi = float(rsi.iloc[-1])

        action = "flat"
        reason = "no_signal"
        order_id = None

        # Entry: RSI > rsi_buy and close > EMA (momentum + trend)
        if last_rsi >= rsi_buy and last_close >= last_ema:
            action = "buy"
            reason = "rsi_momentum_uptrend"
            if not dry:
                order_id = _try_market_buy(broker, sym, notional=notional, qty=qty)

        # Exit: RSI < rsi_sell or close < EMA (momentum fades or trend breaks)
        elif last_rsi <= rsi_sell or last_close < last_ema:
            # If you have a position, sell (best-effort)
            pos_qty = _last_qty_for(broker, sym)
            if pos_qty and pos_qty > 0:
                action = "sell"
                reason = "rsi_momentum_down"
                if not dry:
                    order_id = _try_market_sell(broker, sym, qty=pos_qty, notional=None)

        results.append({
            "symbol": sym,
            "action": action,
            "reason": reason,
            "close": last_close,
            "ema": last_ema,
            "rsi": round(last_rsi, 2),
            "order_id": order_id if not dry else None,
        })

    return {"ok": True, "force": False, "dry": dry, "results": results, "strategy": "c5"}
