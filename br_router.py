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
import logging
logger = logging.getLogger(__name__)

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


def _normalize_symbol(symbol: str) -> str:
    sym = symbol.replace("-", "/").upper().strip()
    try:
        import symbol_map
        if hasattr(symbol_map, "to_broker"):
            mapped = symbol_map.to_broker(sym)
            if mapped:
                return mapped
    except Exception:
        pass
    return sym


def _kraken_variants(sym: str):
    s = sym.upper().replace("-", "/").strip()
    # hard-prioritize Kraken BTC aliases first
    if s.startswith("BTC") or s.startswith("XBT"):
        ordered = ["XBT/USD", "XBTUSD", "XXBTZUSD", "BTC/USD", "BTCUSD"]
    else:
        ordered = [s, s.replace("/", "")]
    # de-dupe, keep order
    seen, out = set(), []
    for a in ordered:
        if a not in seen:
            seen.add(a); out.append(a)
    return out


def _resolve_price(symbol, preload=None):
    candidates = _kraken_variants(symbol)

    # 1) Broker ticker (primary)
    try:
        from broker_kraken import get_ticker_price
    except Exception:
        get_ticker_price = None
    if get_ticker_price:
        for c in candidates:
            try:
                px = get_ticker_price(c)
                if px:
                    logger.debug(f"price resolver: broker OK {c} -> {px}")
                    return float(px)
            except Exception as e:
                logger.debug(f"price resolver: broker fail {c}: {e}")

    # 2) Preloaded bars (works with several shapes)
    try:
        if preload:
            # A) preload[sym]['tf_5Min'] or ['5Min']
            for c in candidates:
                d = preload.get(c) if isinstance(preload, dict) else None
                if isinstance(d, dict):
                    bars = d.get("tf_5Min") or d.get("5Min")
                    if bars:
                        last = bars[-1]
                        close = last["close"] if isinstance(last, dict) else getattr(last, "close", None)
                        if close is not None:
                            logger.debug(f"price resolver: preload A {c} -> {close}")
                            return float(close)
            # B) preload['5Min'][sym]
            if isinstance(preload, dict) and isinstance(preload.get("5Min"), dict):
                for c in candidates:
                    b = preload["5Min"].get(c)
                    if b:
                        last = b[-1]
                        close = last["close"] if isinstance(last, dict) else getattr(last, "close", None)
                        if close is not None:
                            logger.debug(f"price resolver: preload B {c} -> {close}")
                            return float(close)
    except Exception as e:
        logger.debug(f"price resolver: preload exception {e}")

    # 3) Book fallbacks
    try:
        import book
        for fn in ("get_last_price", "get_last_close"):
            if hasattr(book, fn):
                px = getattr(book, fn)(symbol)
                if px:
                    logger.debug(f"price resolver: book {fn} {symbol} -> {px}")
                    return float(px)
        for fn in ("get_bars", "load_bars"):
            if hasattr(book, fn):
                bars = getattr(book, fn)(symbol, tf="5Min", limit=1)
                if bars:
                    last = bars[-1] if isinstance(bars, (list, tuple)) else list(bars)[-1]
                    close = last["close"] if isinstance(last, dict) else getattr(last, "close", None)
                    if close is not None:
                        logger.debug(f"price resolver: book {fn} last close {symbol} -> {close}")
                        return float(close)
    except Exception as e:
        logger.debug(f"price resolver: book exception {e}")

    logger.debug(f"price resolver: NO PRICE for {symbol} (candidates={candidates})")
    return None
