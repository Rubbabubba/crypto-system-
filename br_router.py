# br_router.py — clean router with robust price resolution and safe market_notional wrapper
# This file is self-contained to avoid syntax issues (no line continuations).

import os
import logging
import importlib

try:
    import requests
except Exception:
    requests = None  # public fallback disabled if requests is unavailable

# Keep broker exports available (as before)
from broker_kraken import *  # noqa: F401,F403

ACTIVE_BROKER_MODULE = os.getenv("ACTIVE_BROKER_MODULE", "broker_kraken")
logger = logging.getLogger(__name__)

# Kraken canonical public pair map (USD) for public fallback only
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
    out = []
    seen = set()
    for a in ordered:
        if a not in seen:
            seen.add(a)
            out.append(a)
    return out

def _resolve_price(symbol, preload=None):
    # 1) broker direct (preferred)
    try:
        from broker_kraken import get_ticker_price as _get_px
    except Exception:
        _get_px = None
    if _get_px:
        for c in _kraken_variants(symbol):
            try:
                px = _get_px(c)
                if px:
                    logger.debug(f"price resolver: broker OK {c} -> {px}")
                    return float(px)
            except Exception as e:
                logger.debug(f"price resolver: broker fail {c}: {e}")

    # 2) preload bars
    try:
        if isinstance(preload, dict):
            # shape A: preload[sym]['tf_5Min'|'5Min']
            for c in _kraken_variants(symbol):
                d = preload.get(c)
                if isinstance(d, dict):
                    bars = d.get("tf_5Min") or d.get("5Min")
                    if bars:
                        last = bars[-1]
                        close = last["close"] if isinstance(last, dict) else getattr(last, "close", None)
                        if close is not None:
                            logger.debug(f"price resolver: preload A {c} -> {close}")
                            return float(close)
            # shape B: preload['5Min'][sym]
            five = preload.get("5Min")
            if isinstance(five, dict):
                for c in _kraken_variants(symbol):
                    seq = five.get(c)
                    if seq:
                        last = seq[-1]
                        close = last["close"] if isinstance(last, dict) else getattr(last, "close", None)
                        if close is not None:
                            logger.debug(f"price resolver: preload B {c} -> {close}")
                            return float(close)
    except Exception as e:
        logger.debug(f"price resolver: preload exception {e}")

    # 3) book helpers
    try:
        import book as _book
        for fn in ("get_last_price", "get_last_close", "last_price"):
            if hasattr(_book, fn):
                try:
                    px = getattr(_book, fn)(symbol)
                except TypeError:
                    px = None
                if px:
                    logger.debug(f"price resolver: book {fn} {symbol} -> {px}")
                    return float(px)
        for fn in ("get_bars", "load_bars", "fetch_bars"):
            if hasattr(_book, fn):
                try:
                    bars = getattr(_book, fn)(symbol, tf="5Min", limit=1)
                except TypeError:
                    try:
                        bars = getattr(_book, fn)(symbol, timeframe="5Min", limit=1)
                    except Exception:
                        bars = None
                if bars:
                    last = bars[-1] if isinstance(bars, (list, tuple)) else list(bars)[-1]
                    close = last["close"] if isinstance(last, dict) else getattr(last, "close", None)
                    if close is not None:
                        logger.debug(f"price resolver: book {fn} {symbol} -> {close}")
                        return float(close)
    except Exception as e:
        logger.debug(f"price resolver: book exception {e}")

    # 4) Kraken public fallback
    try:
        if requests is not None:
            pair = _KRAKEN_PUBLIC_MAP.get((symbol or "").upper().replace("-", "/"))
            if pair:
                r = requests.get("https://api.kraken.com/0/public/Ticker", params={"pair": pair}, timeout=3)
                j = r.json()
                if j.get("error") == [] and "result" in j and j["result"]:
                    data = list(j["result"].values())[0]
                    px = float(data["c"][0])
                    logger.debug(f"price resolver: Kraken public {pair} -> {px}")
                    return px
    except Exception as e:
        logger.debug(f"price resolver: kraken public fallback exception {e}")

    logger.debug(f"price resolver: NO PRICE for {symbol}")
    return None

def market_notional(symbol, requested_notional=None, side=None, preload=None, **kwargs):
    """Router wrapper that guarantees a price and adapts to the active broker.
    Unknown kwargs (e.g., strategy) are accepted and ignored here.
    """
    price = _resolve_price(symbol, preload=preload)
    if price is None:
        raise ValueError(f"no price available for {symbol}")
    try:
        mod = importlib.import_module(ACTIVE_BROKER_MODULE)
    except Exception:
        import broker_kraken as mod

    req = requested_notional if requested_notional is not None else float(os.getenv("SCHED_NOTIONAL", "25"))

    # Preferred: broker market_notional with price kw
    if hasattr(mod, "market_notional"):
        try:
            return mod.market_notional(symbol, req, side, price=price)
        except TypeError:
            try:
                return mod.market_notional(symbol, req, side)
            except Exception:
                pass

    # Fallback: qty-based market order
    qty = float(req) / float(price)
    if hasattr(mod, "market"):
        return mod.market(symbol, side, qty)

    if hasattr(mod, "limit"):
        return mod.limit(symbol, side, qty, price=price)

    raise RuntimeError("Active broker does not expose market_notional/market/limit")
