# broker_kraken.py
"""
Kraken broker adapter for:
- account checks (stubbed for parity),
- bars (OHLC) retrieval,
- order placement (spot market buy/sell by notional) — minimal example.

Requires KRAKEN_KEY / KRAKEN_SECRET for trading (if you wire that next),
but bars use the public API and should work without keys.
"""

from __future__ import annotations
import time
import hashlib
import hmac
import base64
import os
from typing import Dict, List, Any

import requests

from strategies.symbol_map import to_kraken, tf_to_kraken

KRAKEN_API = os.getenv("KRAKEN_API", "https://api.kraken.com")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-system/kraken-adapter"})

def _http_public(path: str, params: Dict[str, Any] | None = None) -> Any:
    url = f"{KRAKEN_API}{path}"
    r = SESSION.get(url, params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(f"Kraken error: {data['error']}")
    return data["result"]

# ---- Bars (OHLC) ----

def get_bars(symbols: List[str], timeframe: str, limit: int) -> Dict[str, List[Dict[str, Any]]]:
    """
    Returns { "BTC/USD": [ {t,o,h,l,c,v}, ... ], ... } where t is epoch seconds.
    Uses Kraken /0/public/OHLC with:
      pair = altname (e.g., XBTUSD)
      interval = minutes (e.g., 5 for 5Min)
    Note: Kraken returns up to 720 points depending on interval. We slice to `limit`.
    """
    interval = tf_to_kraken(timeframe)
    out: Dict[str, List[Dict[str, Any]]] = {}

    for sym in symbols:
        kpair = to_kraken(sym)  # e.g., BTC/USD → XBTUSD
        result = _http_public("/0/public/OHLC", {"pair": kpair, "interval": interval})
        # The OHLC payload uses the pair key; take first (only) list
        ohlc_key = next(iter(result.keys()))
        rows = result[ohlc_key]
        # rows: [ [time, open, high, low, close, vwap, volume, count], ... ]
        # Map to our app’s bar schema
        bars = []
        for row in rows[-limit:]:
            t, o, h, l, c, vwap, vol, cnt = row
            bars.append({
                "t": int(t),
                "o": float(o),
                "h": float(h),
                "l": float(l),
                "c": float(c),
                "v": float(vol),
            })
        out[sym] = bars

    return out

# ---- Minimal order API (stub/placeholder) ----
# You can flesh this out after bars are confirmed working.

KRAKEN_KEY = os.getenv("KRAKEN_KEY", "")
KRAKEN_SECRET = os.getenv("KRAKEN_SECRET", "")

def _sign(path: str, data: Dict[str, Any]) -> Dict[str, str]:
    # Kraken private endpoint signing
    if not (KRAKEN_KEY and KRAKEN_SECRET):
        raise RuntimeError("Kraken trading requires KRAKEN_KEY and KRAKEN_SECRET")
    nonce = str(int(time.time() * 1000))
    data = {"nonce": nonce, **data}
    postdata = "&".join(f"{k}={v}" for k, v in data.items())
    message = (data["nonce"] + postdata).encode()
    sha256 = hashlib.sha256(message).digest()
    decoded = base64.b64decode(KRAKEN_SECRET)
    mac = hmac.new(decoded, (path.encode() + sha256), hashlib.sha512)
    sig = base64.b64encode(mac.digest()).decode()
    return {"API-Key": KRAKEN_KEY, "API-Sign": sig}, data

def place_order(symbol: str, side: str, notional: float, client_order_id: str,
                time_in_force: str = "GTC", type: str = "market") -> Dict[str, Any]:
    """
    Minimal market notional order. Kraken needs volume, not notional; you’ll generally
    need a price to convert notional→volume. For now this raises to avoid accidental sends.
    """
    raise NotImplementedError("Trading wiring to Kraken notional orders is pending.")

# Parity stubs used by your app:
def get_positions() -> Dict[str, Any]:
    return {}

def get_account() -> Dict[str, Any]:
    return {"broker": "kraken", "trading_enabled": bool(KRAKEN_KEY and KRAKEN_SECRET)}
