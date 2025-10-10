# broker_kraken.py
"""
Kraken broker adapter:
- Public OHLC bars (already working)
- Market orders by notional (opt-in via KRAKEN_TRADING=1)
- Tiny helpers for account/positions parity

ENV:
  BROKER=kraken
  KRAKEN_API=https://api.kraken.com    (optional; default shown)
  KRAKEN_KEY=...                       (required for trading)
  KRAKEN_SECRET=...                    (required for trading; base64 as given by Kraken)
  KRAKEN_TRADING=1                     (ONLY when you’re ready to send live orders)

NOTE: Order placement converts notional→volume using latest price, snaps to lot
      precision, and enforces Kraken’s ordermin when available.
"""

from __future__ import annotations
import os, time, hmac, hashlib, base64
from typing import Dict, List, Any, Tuple

import requests

from strategies.symbol_map import to_kraken, tf_to_kraken

# ---------- Config / Session ----------
KRAKEN_API = os.getenv("KRAKEN_API", "https://api.kraken.com")
KRAKEN_KEY = os.getenv("KRAKEN_KEY", "")
KRAKEN_SECRET = os.getenv("KRAKEN_SECRET", "")
KRAKEN_TRADING = os.getenv("KRAKEN_TRADING", "0") == "1"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-system/kraken-adapter"})

def _http_public(path: str, params: Dict[str, Any] | None = None) -> Any:
    r = SESSION.get(f"{KRAKEN_API}{path}", params=params, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(f"Kraken error: {data['error']}")
    return data["result"]

def _http_private(path: str, data: Dict[str, Any]) -> Any:
    if not (KRAKEN_KEY and KRAKEN_SECRET):
        raise RuntimeError("Kraken trading requires KRAKEN_KEY and KRAKEN_SECRET")
    url = f"{KRAKEN_API}{path}"
    nonce = str(int(time.time() * 1000))
    payload = {"nonce": nonce, **data}

    # Build message for signature
    postdata = "&".join(f"{k}={v}" for k, v in payload.items())
    message = (nonce + postdata).encode()
    sha256 = hashlib.sha256(message).digest()
    secret = base64.b64decode(KRAKEN_SECRET)
    mac = hmac.new(secret, path.encode() + sha256, hashlib.sha512)
    sig = base64.b64encode(mac.digest()).decode()

    headers = {
        "API-Key": KRAKEN_KEY,
        "API-Sign": sig,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    r = SESSION.post(url, data=payload, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    if data.get("error"):
        raise RuntimeError(f"Kraken error: {data['error']}")
    return data["result"]

# ---------- Bars (OHLC) ----------

def get_bars(symbols: List[str], timeframe: str, limit: int) -> Dict[str, List[Dict[str, Any]]]:
    interval = tf_to_kraken(timeframe)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for sym in symbols:
        kpair = to_kraken(sym)  # e.g., BTC/USD -> XBTUSD
        result = _http_public("/0/public/OHLC", {"pair": kpair, "interval": interval})
        ohlc_key = next(iter(result.keys()))
        rows = result[ohlc_key]
        bars = []
        for row in rows[-limit:]:
            t, o, h, l, c, vwap, vol, cnt = row
            bars.append({"t": int(t), "o": float(o), "h": float(h), "l": float(l), "c": float(c), "v": float(vol)})
        out[sym] = bars
    return out

# ---------- Ticker / Pair meta ----------

def _get_last_price_alt(altname: str) -> float:
    res = _http_public("/0/public/Ticker", {"pair": altname})
    k = next(iter(res.keys()))
    last = float(res[k]["c"][0])  # last trade price
    return last

def _get_pair_meta_alt(altname: str) -> Tuple[int, float | None]:
    """
    Returns (lot_decimals, ordermin) for altname (e.g., XBTUSD).
    ordermin may be None if Kraken doesn’t provide it for the pair.
    """
    res = _http_public("/0/public/AssetPairs", None)
    # Search by altname or by key
    for key, meta in res.items():
        if (meta.get("altname") or "").upper() == altname.upper() or key.upper() == altname.upper():
            lot_decimals = int(meta.get("lot_decimals", 8))
            # Kraken sometimes provides "ordermin" (string) in pair info; if absent, None
            ordermin = meta.get("ordermin")
            try:
                ordermin_f = float(ordermin) if ordermin is not None else None
            except Exception:
                ordermin_f = None
            return lot_decimals, ordermin_f
    return 8, None

def _snap_volume(vol: float, lot_decimals: int) -> float:
    fmt = "{:0." + str(lot_decimals) + "f}"
    snapped = float(fmt.format(vol))
    # Avoid formatting to 0 if min positive value is needed
    return snapped

# ---------- Orders ----------

def place_order(symbol: str, side: str, notional: float, client_order_id: str,
                time_in_force: str = "GTC", type: str = "market") -> Dict[str, Any]:
    """
    Market order by notional USD. Converts to base 'volume' using last price.
    - Only executes if KRAKEN_TRADING=1
    - Uses AddOrder private endpoint
    - Sets 'userref' to your client_order_id (Kraken int; we hash to a 31-bit int)
    """
    if not KRAKEN_TRADING:
        raise RuntimeError("Kraken trading disabled (set KRAKEN_TRADING=1 to enable)")

    if type.lower() != "market":
        raise ValueError("Only market orders are supported in this adapter.")
    s = side.lower()
    if s not in ("buy", "sell"):
        raise ValueError("side must be 'buy' or 'sell'")

    alt = to_kraken(symbol)             # e.g., XBTUSD
    price = _get_last_price_alt(alt)    # USD per base
    lot_decimals, ordermin = _get_pair_meta_alt(alt)

    if price <= 0:
        raise RuntimeError(f"Invalid price for {alt}: {price}")

    volume = notional / price
    volume = _snap_volume(volume, lot_decimals)

    if ordermin is not None and volume < ordermin:
        raise RuntimeError(f"Volume {volume} below Kraken ordermin {ordermin} for {alt}. "
                           f"Increase notional or choose a cheaper asset.")

    # Kraken's 'userref' must be 32-bit signed int. Make a stable small int from client_order_id.
    userref = abs(hash(client_order_id)) % 2147483647

    payload = {
        "pair": alt,
        "type": s,                # buy/sell
        "ordertype": "market",
        "volume": str(volume),    # as string
        "userref": userref,
        # TIF: Kraken supports 'timeinforce': GTC/IOC/GTD; keep GTC by default.
        "timeinforce": "GTC" if (time_in_force or "").upper() == "GTC" else "IOC",
        # Optional: "validate": True   # dry-run on Kraken side (no fill); set if you want.
    }
    res = _http_private("/0/private/AddOrder", payload)
    return {"broker": "kraken", "pair": alt, "side": s, "notional": notional, "volume": volume, "result": res}

# ---------- Parity stubs ----------

def get_positions() -> Dict[str, Any]:
    # Full positions would require parsing private ledgers/balances.
    # Keep empty for parity with your app's use.
    return {}

def get_account() -> Dict[str, Any]:
    return {
        "broker": "kraken",
        "trading_enabled": KRAKEN_TRADING and bool(KRAKEN_KEY and KRAKEN_SECRET),
    }
