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
    s = sym.upper()
    alts = set([s])
    base, quote = (s.split("/") + [""])[:2] if "/" in s else (s[:-3], s[-3:])
    if base == "BTC":
        base_alts = ["BTC", "XBT"]
    else:
        base_alts = [base]
    quote_alts = [quote or "USD"]
    for b in base_alts:
        for q in quote_alts:
            alts.add(f"{b}/{q}")
            alts.add(f"{b}{q}")
    more = set()
    for a in list(alts):
        if a.endswith("USD"):
            more.add(a.replace("USD", "ZUSD"))
        if a.startswith("BTC") or a.startswith("XBT"):
            more.add(a.replace("BTC", "XBT"))
    alts |= more
    return list(alts)


def _resolve_price(symbol, preload=None):
    try:
        from broker_kraken import get_ticker_price
    except Exception:
        get_ticker_price = None
    candidates = _kraken_variants(_normalize_symbol(symbol))
    for c in candidates:
        try:
            if get_ticker_price:
                px = get_ticker_price(c)
                if px:
                    return float(px)
        except Exception:
            pass
    try:
        if preload:
            for c in candidates:
                d = preload.get(c) if isinstance(preload, dict) else None
                if d and isinstance(d, dict):
                    bars = d.get("tf_5Min") or d.get("5Min")
                    if bars and len(bars) > 0:
                        last = bars[-1]
                        val = last['close'] if isinstance(last, dict) else getattr(last, 'close', None)
                        if val is not None:
                            return float(val)
            if isinstance(preload, dict) and '5Min' in preload and isinstance(preload['5Min'], dict):
                for c in candidates:
                    b = preload['5Min'].get(c)
                    if b and len(b) > 0:
                        last = b[-1]
                        val = last['close'] if isinstance(last, dict) else getattr(last, 'close', None)
                        if val is not None:
                            return float(val)
    except Exception:
        pass
    try:
        import book
        for fn in ('get_last_price', 'get_last_close'):
            if hasattr(book, fn):
                px = getattr(book, fn)(symbol)
                if px:
                    return float(px)
        for fn in ('get_bars', 'load_bars'):
            if hasattr(book, fn):
                bars = getattr(book, fn)(symbol, tf='5Min', limit=1)
                if bars and len(bars) > 0:
                    last = bars[-1] if isinstance(bars, (list, tuple)) else list(bars)[-1]
                    if isinstance(last, dict) and 'close' in last and last['close'] is not None:
                        return float(last['close'])
                    close = getattr(last, 'close', None)
                    if close is not None:
                        return float(close)
    except Exception:
        pass
    return None
