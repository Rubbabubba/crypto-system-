# broker_kraken.py — Kraken adapter (resilient imports)
import os, time, hmac, hashlib, base64, requests, urllib.parse as up
from typing import Dict, Any, List, Iterable

# ---- Resilient imports for symbol map (supports both layouts) ----
try:
    # Most repos use a package folder: strategies/symbol_map.py
    from strategies.symbol_map import KRAKEN_PAIR_MAP, to_kraken, tf_to_kraken
except Exception:
    # Flat layout fallback: symbol_map.py at project root
    from symbol_map import KRAKEN_PAIR_MAP, to_kraken, tf_to_kraken  # type: ignore

KRAKEN = os.getenv("KRAKEN_BASE", "https://api.kraken.com")
KEY     = os.getenv("KRAKEN_KEY", os.getenv("KRAKEN_API_KEY",""))
SECRET  = os.getenv("KRAKEN_SECRET", os.getenv("KRAKEN_API_SECRET",""))

# Build inverse map for pretty symbols, e.g., XBTUSD -> BTC/USD
try:
    _INV = {v: k for k, v in (KRAKEN_PAIR_MAP or {}).items()}
except Exception:
    _INV = {}

def from_kraken(pair: str) -> str:
    """Best-effort inverse mapping from Kraken pair to UI symbol."""
    if not pair:
        return pair
    if pair in _INV:
        return _INV[pair]
    # Heuristic: 'ETHUSD' -> 'ETH/USD'
    if "/" not in pair and len(pair) >= 6 and pair.endswith("USD"):
        return pair[:-3] + "/USD"
    return pair

def _raise_for_api_error(j: Dict[str,Any]):
    if j is None:
        raise RuntimeError("Kraken: empty response")
    if isinstance(j, dict) and j.get("error"):
        raise RuntimeError(f"Kraken error: {j['error']}")

def _public(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{KRAKEN}{path}"
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    j = r.json()
    _raise_for_api_error(j)
    return j.get("result") or {}

def _sign(path: str, data: Dict[str, Any]) -> Dict[str,str]:
    """Kraken auth: API-Key; API-Sign = HMAC-SHA512(secret, path + SHA256(nonce+postdata))"""
    postdata = up.urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode()
    message = path.encode() + hashlib.sha256(encoded).digest()
    # Kraken secrets are base64-encoded; if yours are raw, this will still raise clearly
    secret_bytes = base64.b64decode(SECRET) if SECRET else b""
    mac = hmac.new(secret_bytes, message, hashlib.sha512)
    return {"API-Key": KEY, "API-Sign": base64.b64encode(mac.digest()).decode()}

def _private(path: str, data: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{KRAKEN}{path}"
    payload = {"nonce": int(time.time()*1000), **(data or {})}
    headers = _sign(path, payload)
    r = requests.post(url, data=payload, headers=headers, timeout=20)
    r.raise_for_status()
    j = r.json()
    _raise_for_api_error(j)
    return j.get("result") or {}

# ---------------------------------------------------------------------------
# Market data
# ---------------------------------------------------------------------------
def get_bars(symbol: str, timeframe: str = "5min", limit: int = 200) -> List[Dict[str, Any]]:
    """Return list of bar dicts: {t,o,h,l,c,v,vw} using Kraken /OHLC."""
    pair = to_kraken(symbol)
    interval = tf_to_kraken(timeframe)
    res = _public("/0/public/OHLC", {"pair": pair, "interval": interval})
    rows = None
    for k, v in res.items():
        if isinstance(v, list):
            rows = v
            break
    if not rows:
        return []
    out = []
    for row in rows[-limit:]:
        t,o,h,l,c,vw,vol,_ = row
        out.append({
            "t": int(t),
            "o": float(o),
            "h": float(h),
            "l": float(l),
            "c": float(c),
            "v": float(vol),
            "vw": float(vw) if vw not in (None,"") else float(c),
        })
    return out

def last_trade_map(symbols: Iterable[str]):
    """Return { 'BTC/USD': {'price': 12345.67}, ... } via /Ticker."""
    if isinstance(symbols, str):
        symbols = [symbols]
    pairs = [to_kraken(s) for s in symbols]
    res = _public("/0/public/Ticker", {"pair": ",".join(pairs)})
    out = {}
    for k, v in res.items():
        price = float((v.get("c") or ["0"])[0])
        ui = from_kraken(k)
        out[ui] = {"price": price}
    return out

def last_price(symbol: str) -> float:
    return (last_trade_map([symbol]).get(symbol) or {}).get("price", 0.0)

# ---------------------------------------------------------------------------
# Trading
# ---------------------------------------------------------------------------
def _cid_to_userref(cid: str) -> int:
    # Kraken userref must be 32-bit signed int; derive deterministically
    if not cid:
        return int(time.time())
    h = hashlib.sha256(cid.encode()).digest()
    n = int.from_bytes(h[:4], "big") & 0x7FFFFFFF
    return n or int(time.time())

def place_order(symbol: str, side: str, notional: float, client_order_id: str = "") -> Dict[str, Any]:
    """Market order by notional (USD). Computes volume using last price."""
    side = side.lower()
    assert side in ("buy","sell"), "side must be buy/sell"
    pair = to_kraken(symbol)
    px = last_price(symbol)
    if px <= 0:
        px = 1.0
    volume = max(notional / px, 1e-6)
    data = {
        "pair": pair,
        "type": side,
        "ordertype": "market",
        "volume": f"{volume:.8f}",
        "userref": _cid_to_userref(client_order_id),
        "oflags": "viqc",  # volume in quote currency calc at cost
    }
    res = _private("/0/private/AddOrder", data)
    return {
        "ok": True,
        "txid": (res.get("txid") or [None])[0],
        "descr": res.get("descr"),
        "client_order_id": client_order_id,
        "symbol": symbol,
        "side": side,
        "notional": notional,
    }

# Alias used by the optional _scan_bridge enhancement
def submit_order(**kwargs):
    return place_order(kwargs.get("symbol"), kwargs.get("side"), float(kwargs.get("notional", 0)), kwargs.get("client_order_id",""))

def list_orders(status: str = "all", limit: int = 100) -> List[Dict[str, Any]]:
    """Return mixed open+closed orders in a common shape the app expects."""
    items: List[Dict[str, Any]] = []
    if status in ("all","open"):
        oo = _private("/0/private/OpenOrders", {})
        for _, o in (oo.get("open") or {}).items():
            d = o.get("descr", {})
            items.append({
                "id": o.get("userref") or o.get("refid"),
                "symbol": from_kraken(d.get("pair","")),
                "side": d.get("type","").lower(),
                "status": "open",
                "submitted_at": o.get("opentm"),
                "price": float(d.get("price","0") or 0),
                "filled_qty": 0.0,
                "filled_avg_price": 0.0,
                "client_order_id": o.get("userref") or "",
                "notional": 0.0,
            })
    if status in ("all","closed"):
        co = _private("/0/private/ClosedOrders", {"trades": True})
        for _, o in (co.get("closed") or {}).items():
            d = o.get("descr", {})
            items.append({
                "id": o.get("userref") or o.get("refid"),
                "symbol": from_kraken(d.get("pair","")),
                "side": d.get("type","").lower(),
                "status": "filled" if o.get("status") == "closed" else (o.get("status") or ""),
                "filled_at": o.get("closetm"),
                "price": float(o.get("price","0") or 0),
                "filled_qty": float(o.get("vol_exec","0") or 0),
                "filled_avg_price": float(o.get("price","0") or 0),
                "client_order_id": o.get("userref") or "",
                "notional": 0.0,
            })
    return items[:limit]

def list_positions() -> List[Dict[str, Any]]:
    """Approximate position list built from balances; app tracks realized P&L elsewhere."""
    bals = _private("/0/private/Balance", {}) or {}
    out: List[Dict[str, Any]] = []
    for sym, amt in bals.items():
        if sym in ("ZUSD", "USD") or sym.endswith(".S"):
            continue
        qty = float(amt or 0)
        if qty <= 0:
            continue
        base = sym.replace("X","").replace("Z","")
        pair = f"{base}USD"
        out.append({
            "symbol": from_kraken(pair),
            "qty": qty,
            "avg_entry_price": 0.0,
        })
    return out
