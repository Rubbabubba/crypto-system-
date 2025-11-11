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
