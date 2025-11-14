<<<<<<< HEAD
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
=======
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


  keep working without change.


"""





import os


import importlib


from types import ModuleType





# Default broker module names


BROKER_KRAKEN = "broker_kraken"


BROKER_ALPACA = "broker"





ACTIVE_BROKER_NAME = None


ACTIVE_BROKER_MODULE: ModuleType | None = None








def _env_true(name: str) -> bool:


    v = os.environ.get(name, "").strip().lower()


    return v in {"1", "true", "yes", "y", "on"}








def _select_broker_name() -> str:


    # 1) Explicit override


    explicit = os.environ.get("BROKER", "").strip().lower()


    if explicit == "kraken":


        return BROKER_KRAKEN


    if explicit == "alpaca":


        return BROKER_ALPACA





    # 2) Kraken trading flags / keys


    has_kraken_keys = bool(


        os.environ.get("KRAKEN_KEY") and os.environ.get("KRAKEN_SECRET")


    )


    if has_kraken_keys or _env_true("KRAKEN_TRADING"):


        return BROKER_KRAKEN





    # 3) Fallback


    return BROKER_ALPACA








def _load_broker_module(name: str) -> ModuleType:


    try:


        return importlib.import_module(name)


    except Exception as exc:  # pragma: no cover - startup failure


        raise RuntimeError(f"failed to import broker module '{name}': {exc}") from exc








def _wire_exports(mod: ModuleType) -> None:


    """


    Re-export everything from the chosen broker module into this module's


    global namespace so callers can keep using:





        from br_router import get_bars, last_price, market_notional, ...





    without caring which concrete broker adapter is active.


    """


    g = globals()


    skip = {


        "__name__",


        "__file__",


        "__package__",


        "__loader__",


        "__spec__",


        "__builtins__",


    }


    for attr in dir(mod):


        if attr.startswith("_") and attr not in {"__all__"}:


            continue


        if attr in skip:


            continue


        g[attr] = getattr(mod, attr)








def init_broker() -> ModuleType:


    """


    Initialize the active broker module and wire its exports.





    Returns the active broker module instance.


    """


    global ACTIVE_BROKER_NAME, ACTIVE_BROKER_MODULE





    if ACTIVE_BROKER_MODULE is not None:


        return ACTIVE_BROKER_MODULE





    name = _select_broker_name()


    mod = _load_broker_module(name)


    _wire_exports(mod)





    ACTIVE_BROKER_NAME = name


    ACTIVE_BROKER_MODULE = mod


    return mod








# Initialize at import time so br_router is ready for use.


init_broker()





# ==== BEGIN PATCH v1.1.0 (router: price + caps + kwargs) =========================================


from typing import Optional, Dict, Any





try:


    import symbol_map as _symmap


except Exception:  # optional


    _symmap = None





try:


    import broker_kraken as _brk


except Exception:  # optional


    _brk = None








def _env_float(name: str, default: float) -> float:


    # Helper: read float env with default


    try:


        return float(os.environ.get(name, str(default)))


    except Exception:


        return default








def _resolve_price(symbol: str) -> Optional[float]:


    # Best-effort price resolver for a symbol.


    # 1) Direct ticker from broker_kraken


    try:


        if _brk is not None and hasattr(_brk, "ticker"):


            p = _brk.ticker(symbol)


            if isinstance(p, (int, float)) and p > 0:


                return float(p)


    except Exception:


        pass





    # 2) Symbol-map to Kraken format, then ticker


    try:


        if _symmap is not None and hasattr(_symmap, "to_kraken"):


            ksym = _symmap.to_kraken(symbol) or symbol


        else:


            ksym = symbol


        if _brk is not None and hasattr(_brk, "ticker"):


            p = _brk.ticker(ksym)


            if isinstance(p, (int, float)) and p > 0:


                return float(p)


    except Exception:


        pass





    # 3) Last cached price


    try:


        if _brk is not None and hasattr(_brk, "last_price"):


            p = _brk.last_price(symbol)


            if isinstance(p, (int, float)) and p > 0:


                return float(p)


    except Exception:


        pass





    # 4) 1-minute close as fallback


    try:


        if _brk is not None and hasattr(_brk, "ohlcv_close"):


            p = _brk.ohlcv_close(symbol, tf="1Min")


            if isinstance(p, (int, float)) and p > 0:


                return float(p)


    except Exception:


        pass





    return None








def _cap_notional(notional: float) -> float:


    # Apply MAX_NOTIONAL_PER_ORDER cap; never return <= 0.


    max_per = _env_float("MAX_NOTIONAL_PER_ORDER", 100.0)


    try:


        n = float(notional)


    except Exception:


        n = 0.0


    if n > max_per:


        n = max_per


    if n <= 0.0:


        n = 1e-8


    return n








# Preserve any existing market_notional so we can call through


try:


    _old_market_notional = market_notional  # type: ignore[name-defined]


except Exception:


    _old_market_notional = None  # type: ignore[assignment]








def market_notional(


    symbol: str,


    side: str,


    notional: float,


    price: Optional[float] = None,


    **kwargs: Any,


) -> Dict[str, Any]:


    """Safer market_notional wrapper."""


    # Cap notional


    capped = _cap_notional(notional)





    # Resolve price


    px = price if isinstance(price, (int, float)) and price > 0 else _resolve_price(symbol)


    if px is None or px <= 0.0:


        return {"ok": False, "error": "no price available for %s" % symbol}





    # If there was an original implementation, call through


    if _old_market_notional is not None:


        try:


            return _old_market_notional(


                symbol=symbol,


                side=side,


                notional=capped,


                price=px,


                **kwargs,


            )  # type: ignore[misc]


        except TypeError:


            # Legacy signature: (symbol, side, notional)


            try:


                return _old_market_notional(symbol, side, capped)  # type: ignore[misc]


            except Exception as exc:


                return {"ok": False, "error": "legacy market_notional failed: %s" % (exc,)}


        except Exception as exc:


            return {"ok": False, "error": "market_notional failed: %s" % (exc,)}





    # Fallback: call broker_kraken.market_value if available


    if _brk is not None and hasattr(_brk, "market_value"):


        try:


            order = _brk.market_value(symbol, side, capped)


            return {"ok": True, "order": order}


        except Exception as exc:


            return {"ok": False, "error": "broker market_value failed: %s" % (exc,)}





    return {"ok": False, "error": "no routing backend available"}





# ==== END PATCH v1.1.0 ===========================================================================
>>>>>>> aaad261e04c14004f76931029016af8c5533c084
