# broker_kraken.py
import os, time, hmac, hashlib, base64, requests, urllib.parse as up
from typing import Dict, Any, List, Tuple
from strategies.symbol_map import to_kraken, tf_to_kraken

KRAKEN = os.getenv("KRAKEN_BASE", "https://api.kraken.com")
KEY = os.getenv("KRAKEN_KEY","")
SECRET = os.getenv("KRAKEN_SECRET","")

def _sign(path: str, data: Dict[str, Any]) -> Dict[str,str]:
    # Kraken auth: API-Key header and API-Sign = HMAC-SHA512(secret, path + SHA256(nonce+postdata))
    postdata = up.urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode()
    message = path.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(SECRET), message, hashlib.sha512)
    return {"API-Key": KEY, "API-Sign": base64.b64encode(mac.digest()).decode()}

def _private(path: str, payload: Dict[str, Any]) -> Any:
    payload = {"nonce": int(time.time()*1000), **payload}
    headers = _sign(path, payload)
    r = requests.post(KRAKEN + path, headers=headers, data=payload, timeout=30)
    j = r.json()
    if j.get("error"):
        raise RuntimeError(f"Kraken error: {j['error']}")
    return j["result"]

def _public(path: str, params: Dict[str, Any]) -> Any:
    r = requests.get(KRAKEN + path, params=params, timeout=30)
    j = r.json()
    if j.get("error"):
        raise RuntimeError(f"Kraken error: {j['error']}")
    return j["result"]

# ---- Surface roughly matching broker.py ----
def get_bars(symbol: str, timeframe="5Min", limit=360) -> Dict[str, List[Dict[str, Any]]]:
    pair = to_kraken(symbol)
    interval = tf_to_kraken(timeframe)
    res = _public("/0/public/OHLC", {"pair": pair, "interval": interval})
    rows = res[pair]
    # normalize to your existing bar schema
    out = []
    for t,o,h,l,c,v,_ct in rows[-limit:]:
        out.append({"t": int(t), "o": float(o), "h": float(h), "l": float(l), "c": float(c), "v": float(v)})
    return {symbol: out}

def place_order(symbol: str, side: str, notional: float, client_order_id: str,
                time_in_force="GTC", type="market") -> Any:
    pair = to_kraken(symbol)
    # Market order sized in quote currency → need volume in base.
    # Fetch last price to convert notional (USD) → volume (base)
    px = _public("/0/public/Ticker", {"pair": pair})[pair]["c"][0]
    vol = round(notional / float(px), 8)
    params = {
        "ordertype": "market",
        "type": "buy" if side=="buy" else "sell",
        "pair": pair,
        "volume": str(vol),
        "userref": client_order_id[:32],  # Kraken limit
        "timeinforce": "GTC" if time_in_force.upper()=="GTC" else "IOC"
    }
    return _private("/0/private/AddOrder", params)

def list_positions() -> List[Dict[str, Any]]:
    # Emulate “positions” via balances for base assets; avg price best-effort via recent trades
    bals = _private("/0/private/Balance", {})
    result = []
    for sym in list(bals.keys()):
        if sym in ("ZUSD","USD"):  # skip cash
            continue
        amt = float(bals[sym])
        if amt <= 0: 
            continue
        # try to find a USD pair symbol like XBT → XBTUSD; map back to BTC/USD for UI
        if sym.endswith(".S"):  # staking wrappers etc — skip
            continue
        base = sym.replace("X","").replace("Z","")  # rough
        pair = f"{base}USD"
        ui = symbol_from_kraken(pair)  # implement inverse mapper if you want pretty names
        result.append({"symbol": ui, "qty": amt, "avg_entry": 0.0})  # avg_entry: tracked in your PnL book if needed
    return result
