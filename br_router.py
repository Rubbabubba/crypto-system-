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


# ====== APPENDED BY PATCH v2_7_9 ======
import logging as _brlog
import importlib as _brimp
_brlogger = _brlog.getLogger(__name__)
try:
    import requests as _brreq
except Exception:
    _brreq = None

# Canonical Kraken public pair map (USD)
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

def _kraken_variants(sym: str):
    s = (sym or "").upper().replace("-", "/").strip()
    if s.startswith("BTC") or s.startswith("XBT"):
        ordered = ["XBT/USD", "XBTUSD", "XXBTZUSD", "BTC/USD", "BTCUSD"]
    else:
        ordered = [s, s.replace("/", "")]
    seen, out = set(), []
    for a in ordered:
        if a not in seen:
            seen.add(a); out.append(a)
    return out

def _resolve_price(symbol, preload=None):
    # 1) broker direct (if available)
    try:
        from broker_kraken import get_ticker_price as _br_get_px
    except Exception:
        _br_get_px = None
    if _br_get_px:
        for c in _kraken_variants(symbol):
            try:
                px = _br_get_px(c)
                if px:
                    _brlogger.debug(f"price resolver: broker OK {c} -> {px}")
                    return float(px)
            except Exception as e:
                _brlogger.debug(f"price resolver: broker fail {c}: {e}")

    # 2) preload shapes
    try:
        if isinstance(preload, dict):
            for c in _kraken_variants(symbol):
                d = preload.get(c)
                if isinstance(d, dict):
                    bars = d.get("tf_5Min") or d.get("5Min")
                    if bars:
                        last = bars[-1]
                        close = last["close"] if isinstance(last, dict) else getattr(last, "close", None)
                        if close is not None:
                            _brlogger.debug(f"price resolver: preload A {c} -> {close}")
                            return float(close)
            five = preload.get("5Min")
            if isinstance(five, dict):
                for c in _kraken_variants(symbol):
                    seq = five.get(c)
                    if seq:
                        last = seq[-1]
                        close = last["close"] if isinstance(last, dict) else getattr(last, "close", None)
                        if close is not None:
                            _brlogger.debug(f"price resolver: preload B {c} -> {close}")
                            return float(close)
    except Exception as e:
        _brlogger.debug(f"price resolver: preload exception {e}")

    # 3) book helpers
    try:
        import book as _brbook
        for fn in ("get_last_price","get_last_close","last_price"):
            if hasattr(_brbook, fn):
                px = getattr(_brbook, fn)(symbol)
                if px:
                    _brlogger.debug(f"price resolver: book {fn} {symbol} -> {px}")
                    return float(px)
        for fn in ("get_bars","load_bars","fetch_bars"):
            if hasattr(_brbook, fn):
                try:
                    bars = getattr(_brbook, fn)(symbol, tf="5Min", limit=1)
                except TypeError:
                    bars = getattr(_brbook, fn)(symbol, timeframe="5Min", limit=1)
                if bars:
                    last = bars[-1] if isinstance(bars, (list, tuple)) else list(bars)[-1]
                    close = last["close"] if isinstance(last, dict) else getattr(last, "close", None)
                    if close is not None:
                        _brlogger.debug(f"price resolver: book {fn} {symbol} -> {close}")
                        return float(close)
    except Exception as e:
        _brlogger.debug(f"price resolver: book exception {e}")

    # 4) Kraken public
    try:
        if _brreq is not None:
            pair = _KRAKEN_PUBLIC_MAP.get(symbol.upper().replace("-", "/"))
            if pair:
                r = _brreq.get("https://api.kraken.com/0/public/Ticker", params={"pair": pair}, timeout=3)
                j = r.json()
                if j.get("error") == [] and "result" in j and j["result"]:
                    data = list(j["result"].values())[0]
                    px = float(data["c"][0])
                    _brlogger.debug(f"price resolver: Kraken public {pair} -> {px}")
                    return px
    except Exception as e:
        _brlogger.debug(f"price resolver: kraken public fallback exception {e}")

    _brlogger.debug(f"price resolver: NO PRICE for {symbol}")
    return None

def market_notional(symbol, requested_notional=None, side=None, preload=None, **kwargs):
    \"\"\"Router wrapper that guarantees a price via _resolve_price and adapts to broker signature.
    Unknown kwargs (e.g., strategy) are ignored.
    \"\"\"
    price = _resolve_price(symbol, preload=preload)
    if price is None:
        raise ValueError(f"no price available for {symbol}")
    # pick active broker module
    try:
        modname = globals().get("ACTIVE_BROKER_MODULE") or "broker_kraken"
        _mod = _brimp.import_module(modname)
    except Exception:
        import broker_kraken as _mod

    # Determine requested notional default if not provided
    import os as _bros
    req = requested_notional if requested_notional is not None else float(_bros.getenv("SCHED_NOTIONAL", "25"))

    # Try broker's market_notional with optional price
    if hasattr(_mod, "market_notional"):
        try:
            return _mod.market_notional(symbol, req, side, price=price)
        except TypeError:
            try:
                return _mod.market_notional(symbol, req, side)
            except Exception:
                pass

    # Fallback: compute qty and call market(...)
    qty = float(req) / float(price)
    if hasattr(_mod, "market"):
        return _mod.market(symbol, side, qty)
    if hasattr(_mod, "limit"):
        return _mod.limit(symbol, side, qty, price=price)
    raise RuntimeError("Active broker does not expose market_notional/market/limit")
# ====== END PATCH v2_7_9 ======
