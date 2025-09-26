# broker.py — Alpaca (paper) crypto helper used by app.py
import os
import time
import json
from typing import Dict, List, Any
import requests

# ---- ENV ----
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID") or os.getenv("APCA_API_KEY_ID") or ""
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY") or ""
trading_base      = os.getenv("ALPACA_TRADE_HOST", "https://paper-api.alpaca.markets").rstrip("/")
data_base         = os.getenv("ALPACA_DATA_HOST",  "https://data.alpaca.markets").rstrip("/")

# ---- HTTP session ----
_s = requests.Session()
_s.headers.update({
    "APCA-API-KEY-ID": ALPACA_KEY_ID,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    "Content-Type": "application/json",
})

def _get(url: str, params: Dict[str, Any] = None):
    r = _s.get(url, params=params, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {url} [{r.status_code}]: {r.text}")
    return r.json()

def _post(url: str, payload: Dict[str, Any]):
    r = _s.post(url, data=json.dumps(payload), timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"POST {url} [{r.status_code}]: {r.text}")
    return r.json()

# --------------------------------------------------------------------
# Market Data (Crypto, US venue) — v1beta3 endpoints
# Docs: https://docs.alpaca.markets/reference/crypto-bars
# --------------------------------------------------------------------

def get_bars(symbols: List[str], timeframe: str = "1Min", limit: int = 600, merge: bool = False) -> Dict[str, Any]:
    """
    Returns: { "BTC/USD": [ {t,o,h,l,c,v}, ... ], "ETH/USD": [...] }
    Time is ISO8601 (UTC). Columns use o/h/l/c/v/t to minimize payload; app normalizer maps them.
    """
    if not symbols:
        return {}
    # Alpaca crypto bars endpoint supports multiple symbols
    url = f"{data_base}/v1beta3/crypto/us/bars"
    params = {
        "symbols": ",".join(symbols),
        "timeframe": timeframe,
        "limit": int(limit),
        # you can add "feed": "sip" if your plan supports
    }
    j = _get(url, params=params)
    out: Dict[str, Any] = {}
    # Response shape: { "bars": { "BTC/USD": [ { "t": "...", "o": "...", ... }, ... ], ... } }
    bars_by_sym = j.get("bars") or {}
    for sym in symbols:
        arr = bars_by_sym.get(sym) or []
        # Keep only needed keys and coerce types lightly; app’s normalizer will finish the job.
        cleaned = []
        for b in arr:
            cleaned.append({
                "t": b.get("t"),
                "o": float(b.get("o", 0)),
                "h": float(b.get("h", 0)),
                "l": float(b.get("l", 0)),
                "c": float(b.get("c", 0)),
                "v": float(b.get("v", 0)),
            })
        out[sym] = cleaned
    return out

def last_trade_map(symbols: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Returns: { "BTC/USD": {"price": 12345.67}, ... }
    """
    if not symbols:
        return {}
    url = f"{data_base}/v1beta3/crypto/us/trades/latest"
    params = {"symbols": ",".join(symbols)}
    j = _get(url, params=params)
    # shape: { "trades": { "BTC/USD": { "p": price, "t": "...", ... }, ... } }
    trades = j.get("trades") or {}
    out = {}
    for sym in symbols:
        p = None
        if sym in trades:
            p = trades[sym].get("p")
        out[sym] = {"price": float(p) if p is not None else 0.0}
    return out

# --------------------------------------------------------------------
# Trading (paper) — v2 endpoints
# Docs: https://docs.alpaca.markets/reference/submitorder
# --------------------------------------------------------------------

def list_positions() -> List[Dict[str, Any]]:
    url = f"{trading_base}/v2/positions"
    try:
        j = _get(url)
        # Normalize symbol key for downstream
        for p in j:
            if "symbol" not in p and "asset_symbol" in p:
                p["symbol"] = p["asset_symbol"]
        return j
    except RuntimeError as e:
        # No positions returns 200 with empty array, errors bubble up
        raise e

def list_orders(status: str = "all", limit: int = 200) -> List[Dict[str, Any]]:
    url = f"{trading_base}/v2/orders"
    params = {"status": status, "limit": int(limit), "direction": "desc", "nested": "true"}
    j = _get(url, params=params)
    return j if isinstance(j, list) else j.get("orders", [])

def submit_order(symbol: str, side: str, notional: float, client_order_id: str,
                 type: str = "market", time_in_force: str = "gtc") -> Dict[str, Any]:
    """
    Places a crypto market order by notional. Alpaca infers asset_class from symbol (e.g. BTC/USD).
    """
    url = f"{trading_base}/v2/orders"
    payload = {
        "symbol": symbol,
        "side": side.lower(),
        "type": type,
        "time_in_force": time_in_force,
        "notional": float(notional),
        "client_order_id": client_order_id,
    }
    return _post(url, payload)
