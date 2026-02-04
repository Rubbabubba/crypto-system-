"""
symbol_map.py — minimal symbol + timeframe normalization for Kraken.

Keeps the deployment self-contained (no missing imports on Render).

Accepted symbol formats:
- "BTC/USD" (preferred in env allowlist)
- "BTCUSD" (TradingView / exchange-style)
- "KRAKEN:BTCUSD" (TradingView tickerid)
- "BTC-USD", "BTC_USD"

Exports:
- normalize_symbol(): returns UI allowlist form like "BTC/USD"
- to_kraken(): returns Kraken pair code like "XBTUSD"
- from_kraken(): returns UI symbol "BTC/USD"
- tf_to_kraken(): timeframe normalization to minutes

This is intentionally small and opinionated.
"""

from __future__ import annotations

import re
from typing import Optional


_BASE_MAP = {
    "BTC": "XBT",
    "XBT": "XBT",
    "ETH": "ETH",
    "SOL": "SOL",
    "LINK": "LINK",
    "XRP": "XRP",
    "ADA": "ADA",
    "AVAX": "AVAX",
    "DOT": "DOT",
    "LTC": "LTC",
    "BCH": "BCH",
    "NEAR": "NEAR",
    "SUI": "SUI",
}

_QUOTE_MAP = {
    "USD": "USD",
    "USDT": "USDT",
    "USDC": "USDC",
    "EUR": "EUR",
}


def _strip_tv_prefix(sym: str) -> str:
    s = (sym or "").strip().upper()
    if ":" in s:
        s = s.split(":", 1)[1]
    return s


def _normalize_delims(sym: str) -> str:
    s = (sym or "").strip().upper()
    s = _strip_tv_prefix(s)
    s = s.replace("-", "/").replace("_", "/")
    return s


def normalize_symbol(symbol: str) -> str:
    """
    Normalize any accepted symbol format into UI allowlist form: "BASE/QUOTE" (e.g., "BTC/USD").
    - Converts "BTCUSD" -> "BTC/USD"
    - Converts "KRAKEN:BTCUSD" -> "BTC/USD"
    - Converts "XBTUSD" -> "BTC/USD" (UI uses BTC)
    """
    s = _normalize_delims(symbol)
    if not s:
        return s

    # Already BASE/QUOTE
    if "/" in s:
        base, quote = s.split("/", 1)
    else:
        # BTCUSD / XBTUSD -> split by known quote suffix
        m = re.match(r"^([A-Z0-9]{2,6})(USD|USDT|USDC|EUR)$", s)
        if not m:
            raise ValueError(f"unsupported symbol format: {symbol}")
        base, quote = m.group(1), m.group(2)

    # UI uses BTC not XBT
    if base == "XBT":
        base = "BTC"

    # sanity normalize quotes
    quote = _QUOTE_MAP.get(quote, quote)

    return f"{base}/{quote}"


def to_kraken(symbol: str) -> str:
    """Convert UI symbol (or accepted formats) to Kraken pair code used in endpoints."""
    ui = normalize_symbol(symbol)
    base, quote = ui.split("/", 1)

    # Kraken uses XBT
    base = _BASE_MAP.get(base, base)
    quote = _QUOTE_MAP.get(quote, quote)

    return f"{base}{quote}"


def from_kraken(pair: str) -> str:
    """Convert Kraken pair code to UI symbol in BASE/QUOTE form."""
    p = (pair or "").strip().upper()
    if not p:
        return p

    # Strip common Kraken asset prefixes like XXBTZUSD -> XBTUSD
    p = re.sub(r"^[XZ]+", "", p)
    p = re.sub(r"[XZ]+", "", p)

    # Try to split by known quotes
    for q in sorted(_QUOTE_MAP.values(), key=len, reverse=True):
        if p.endswith(q):
            base = p[: -len(q)]
            quote = q
            if base == "XBT":
                base = "BTC"
            return f"{base}/{quote}"

    return p


def tf_to_kraken(timeframe: str) -> Optional[int]:
    """Return Kraken OHLC interval (minutes) for a timeframe string."""
    tf = (timeframe or "").strip()
    if not tf:
        return None

    tfu = tf.upper()

    # common TradingView ...Min
    m = re.match(r"^(\d+)\s*MIN$", tfu)
    if m:
        return int(m.group(1))

    m = re.match(r"^(\d+)\s*M$", tfu)
    if m:
        return int(m.group(1))

    if tfu.endswith("MIN"):
        try:
            return int(tfu[:-3])
        except Exception:
            return None

    # hours
    if tfu in ("1H", "1HR", "60"):
        return 60
    if tfu in ("4H", "240"):
        return 240

    # plain integer minutes
    if tf.isdigit():
        return int(tf)

    return None
