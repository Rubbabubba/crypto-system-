import logging
logger = logging.getLogger(__name__)
import requests
import importlib
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


# Common Kraken public pair codes for USD
_KRAKEN_PUBLIC_MAP = {
    "BTC/USD": "XXBTZUSD",
    "XBT/USD": "XXBTZUSD",
    "ETH/USD": "XETHZUSD",
    "SOL/USD": "SOLUSD",
    "LTC/USD": "XLTCZUSD",
    "BCH/USD": "BCHUSD",
    "XRP/USD": "XXRPZUSD",
    "DOGE/USD": "XDGUSD",
    "AVAX/USD": "AVAXUSD",
    "LINK/USD": "LINKUSD",
}


def market_notional(symbol, requested_notional, side, preload=None):
    """Router wrapper that guarantees a price via _resolve_price and adapts to broker signature.
    """
    price = _resolve_price(symbol, preload=preload)
    if price is None:
        raise ValueError(f"no price available for {symbol}")
    import importlib
    mod = importlib.import_module(ACTIVE_BROKER_MODULE)
    # Try broker's market_notional with price kw
    if hasattr(mod, "market_notional"):
        try:
            return mod.market_notional(symbol, requested_notional, side, price=price)
        except TypeError:
            try:
                return mod.market_notional(symbol, requested_notional, side)
            except Exception:
                pass
    # Fallback: compute qty and use market(...)
    qty = float(requested_notional) / float(price)
    if hasattr(mod, "market"):
        return mod.market(symbol, side, qty)
    # Last resort: place limit at price if available
    if hasattr(mod, "limit"):
        return mod.limit(symbol, side, qty, price=price)
    raise RuntimeError("Active broker does not expose market_notional/market/limit")
