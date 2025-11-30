#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
br_router.py — broker selection shim

Build: v2.1.0 (2025-11-29)

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

# ==== BEGIN PATCH v1.2.0 (router: price + caps + kwargs + strategy passthru) =====================

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
    """
    Best-effort price resolver for a symbol.

    Tries, in order:
    - broker_kraken.ticker(symbol)
    - ticker(mapped_symbol) via symbol_map.to_kraken
    - broker_kraken.last_price(symbol)
    - broker_kraken.ohlcv_close(symbol, tf="1Min")
    """
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
    """
    Apply MAX_NOTIONAL_PER_ORDER cap; never return <= 0.
    This is a per-order cap, orthogonal to per-symbol risk_caps in risk.json.
    """
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
    """
    Safer market_notional wrapper.

    Responsibilities:
    - Cap per-order notional via MAX_NOTIONAL_PER_ORDER.
    - Resolve a best-effort price if not provided.
    - Forward all kwargs (including 'strategy') to the underlying broker
      adapter so strategy tagging via userref/comment works end-to-end.
    """
    # Cap notional
    capped = _cap_notional(notional)

    # Resolve price
    px = price if isinstance(price, (int, float)) and price > 0 else _resolve_price(symbol)
    if px is None or px <= 0.0:
        return {"ok": False, "error": "no price available for %s" % symbol}

    # If there was an original implementation, call through
    if _old_market_notional is not None:
        try:
            # Preferred modern signature
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
                return {
                    "ok": False,
                    "error": "legacy market_notional failed: %s" % (exc,),
                }
        except Exception as exc:
            return {"ok": False, "error": "market_notional failed: %s" % (exc,)}

    # No original implementation exported; call Kraken adapter directly if present.
    # First prefer broker_kraken.market_notional if it exists.
    if _brk is not None and hasattr(_brk, "market_notional"):
        try:
            order = _brk.market_notional(
                symbol=symbol,
                side=side,
                notional=capped,
                price=px,
                **kwargs,
            )
            return {"ok": True, "order": order}
        except Exception as exc:
            return {
                "ok": False,
                "error": "broker market_notional failed: %s" % (exc,),
            }

    # Fallback: call broker_kraken.market_value if available
    if _brk is not None and hasattr(_brk, "market_value"):
        try:
            # market_value may ignore extra kwargs, but we still pass them (strategy included)
            order = _brk.market_value(symbol, side, capped, **kwargs)
            return {"ok": True, "order": order}
        except Exception as exc:
            return {
                "ok": False,
                "error": "broker market_value failed: %s" % (exc,),
            }

    return {"ok": False, "error": "no routing backend available"}


# ==== END PATCH v1.2.0 =======================================================================
