#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
br_router.py — broker selection shim
Build: v2.0.0 (2025-10-11)

Behavior
- Prefer Kraken when:
  * BROKER=kraken  (case-insensitive), OR
  * both KRAKEN_KEY and KRAKEN_SECRET are present, OR
  * KRAKEN_TRADING=1/true
- Otherwise fall back to Alpaca (legacy 'broker' module).

Exports
- Wildcard re-exports of the selected broker adapter so existing imports like:
    from br_router import get_bars, last_price, market_notional, orders, positions
  continue to work unchanged.

Notes
- This file only routes; all HTTP/API logic lives in the adapter modules.
"""
__version__ = "2.0.0"

import os

BROKER_ENV = (os.getenv("BROKER", "kraken") or "").lower()
HAS_KRAKEN_CREDS = bool(os.getenv("KRAKEN_KEY") and os.getenv("KRAKEN_SECRET"))
KRAKEN_FLAG = os.getenv("KRAKEN_TRADING", "0") in ("1", "true", "True")

USE_KRAKEN = (BROKER_ENV == "kraken") or HAS_KRAKEN_CREDS or KRAKEN_FLAG

if USE_KRAKEN:
    from broker_kraken import *  # noqa: F401,F403
    ACTIVE_BROKER_MODULE = "broker_kraken"
else:
    from broker import *  # Alpaca fallback; must exist if selected  # noqa: F401,F403
    ACTIVE_BROKER_MODULE = "broker"


def _resolve_price(symbol, preload=None):
    try:
        from broker_kraken import get_ticker_price
        px = get_ticker_price(symbol)
        if px:
            return float(px)
    except Exception:
        pass
    try:
        if preload and preload.get(symbol) and preload[symbol].get("tf_5Min"):
            bars = preload[symbol]["tf_5Min"]
            if len(bars) > 0:
                last = bars[-1]
                return float(last["close"] if isinstance(last, dict) else last.close)
    except Exception:
        pass
    try:
        import book
        px = getattr(book, "get_last_price")(symbol)
        if px:
            return float(px)
    except Exception:
        pass
    return None


def _normalize_symbol(symbol: str) -> str:
    sym = symbol.replace("-", "/").upper()
    try:
        import symbol_map
        if hasattr(symbol_map, "to_broker"):
            return symbol_map.to_broker(sym)
    except Exception:
        pass
    return sym


def market_notional(symbol: str, requested_notional: float, side: str = "buy", preload=None):
    sym = _normalize_symbol(symbol)
    price = _resolve_price(sym, preload=preload)
    if not price or price <= 0:
        return {"ok": False, "error": f"no price available for {sym}"}
    min_order = float(os.getenv("MIN_ORDER_NOTIONAL_USD", "10"))
    max_equity_pct = float(os.getenv("MAX_EQUITY_PCT", "0.25"))
    quote_ccy = "USD" if "/USD" in sym or sym.endswith("USD") else os.getenv("QUOTE_CCY", "USD")
    free_quote = None
    try:
        from broker_kraken import get_free_balance_quote
        free_quote = float(get_free_balance_quote(quote_ccy)) if get_free_balance_quote else None
    except Exception:
        free_quote = None
    cap_by_equity = (free_quote * max_equity_pct) if (free_quote is not None) else requested_notional
    final_notional = min(max(requested_notional, min_order), cap_by_equity)
    if free_quote is not None and final_notional > free_quote:
        final_notional = max(min_order, free_quote * 0.98)
    if final_notional < min_order:
        return {"ok": False, "error": f"notional {final_notional:.2f} below exchange minimum {min_order:.2f}"}
    qty = final_notional / price
    try:
        from broker_kraken import market_order
        resp = market_order(sym, side=side, qty=qty, price_hint=price)
        if isinstance(resp, dict) and resp.get("ok"):
            return {"ok": True, "final_notional": final_notional, "price_used": price, "reason": "sized_by_request_cap_equity"}
        err = resp.get("error") if isinstance(resp, dict) else "broker_error"
        return {"ok": False, "error": err, "final_notional": final_notional, "price_used": price}
    except Exception as e:
        return {"ok": False, "error": str(e), "final_notional": final_notional, "price_used": price}
