# broker.py — Alpaca Paper broker glue for crypto
from __future__ import annotations
import os, requests
from typing import List, Dict, Any

# --- Config from env ---
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID","")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY","")
ALPACA_PAPER      = os.getenv("ALPACA_PAPER","1") in ("1","true","True")

DEFAULT_TRADE_HOST = "https://paper-api.alpaca.markets" if ALPACA_PAPER else "https://api.alpaca.markets"
trading_base = os.getenv("ALPACA_TRADE_HOST", DEFAULT_TRADE_HOST)
data_base    = os.getenv("ALPACA_DATA_HOST",  "https://data.alpaca.markets")

HEADERS = {
    "APCA-API-KEY-ID": ALPACA_KEY_ID,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}

def _trading_symbol(sym_slash: str) -> str:
    return sym_slash.replace("/", "")  # BTC/USD -> BTCUSD

def _data_symbol(sym_slash: str) -> str:
    return sym_slash  # v1beta3 uses BTC/USD

def _http(method: str, url: str, **kw):
    r = requests.request(method, url, headers=HEADERS, timeout=30, **kw)
    ct = (r.headers.get("content-type") or "")
    if r.status_code >= 400:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text}")
    if r.text and "application/json" in ct:
        return r.json()
    return {}

# ---------- Orders / Positions ----------
def submit_order(symbol: str, side: str, notional: float, client_order_id: str,
                 time_in_force: str = "gtc", type: str = "market") -> Dict[str,Any]:
    url = f"{trading_base}/v2/orders"
    payload = {
        "symbol": _trading_symbol(symbol),
        "side": side,
        "type": type,
        "time_in_force": time_in_force,
        "notional": round(float(notional), 2),
        "client_order_id": client_order_id,
    }
    return _http("POST", url, json=payload)

# Back-compat alias for strategies that call br.place_order(...)
def place_order(symbol: str, side: str, notional: float, client_order_id: str,
                time_in_force: str = "gtc", type: str = "market") -> Dict[str,Any]:
    return submit_order(symbol, side, notional, client_order_id, time_in_force, type)

def list_orders(status: str = "all", limit: int = 200) -> List[Dict[str,Any]]:
    url = f"{trading_base}/v2/orders"
    params = {"status": status, "limit": int(limit)}
    return _http("GET", url, params=params)

def list_positions() -> List[Dict[str,Any]]:
    url = f"{trading_base}/v2/positions"
    return _http("GET", url)

# ---------- Market data ----------
def get_bars(symbols, timeframe: str = "5Min", limit: int = 600, merge: bool = False):
    """Return dict[symbol] -> list of bars ({o,h,l,c,v,t}) using v1beta3 crypto bars."""
    if isinstance(symbols, str):
        symbols = [symbols]
    syms = ",".join([_data_symbol(s) for s in symbols])
    url = f"{data_base}/v1beta3/crypto/us/bars"
    params = {"timeframe": timeframe, "symbols": syms, "limit": int(limit)}
    j = _http("GET", url, params=params) or {}
    data = j.get("bars") or {}
    out: Dict[str, Any] = {}
    for s in symbols:
        rows = []
        for r in data.get(s, []):
            rows.append({
                "o": r.get("o"),
                "h": r.get("h"),
                "l": r.get("l"),
                "c": r.get("c"),
                "v": r.get("v"),
                "t": r.get("t") or r.get("Timestamp") or r.get("timestamp"),
            })
        out[s] = rows
    return out

def last_trade_map(symbols) -> Dict[str,Any]:
    if isinstance(symbols, str):
        symbols = [symbols]
    syms = ",".join([_data_symbol(s) for s in symbols])
    url = f"{data_base}/v1beta3/crypto/us/latest/trades"
    j = _http("GET", url, params={"symbols": syms}) or {}
    data = j.get("trades") or {}
    out = {}
    for s in symbols:
        row = data.get(s) or {}
        out[s] = {"price": float(row.get("p") or 0.0)}
    return out

__all__ = [
    "submit_order","place_order","list_orders","list_positions",
    "get_bars","last_trade_map"
]
