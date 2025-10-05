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
    """
    Return dict[symbol_slash] -> list of bars ({o,h,l,c,v,t}) from Alpaca v1beta3 crypto.

    Improvements:
    - Adds robust key mapping (slash / no-slash).
    - Supplies a 'start' timestamp based on requested lookback.
    - Paginates with 'page_token' until 'limit' is reached (or no more data).
    - Sorts bars ascending by timestamp.

    Uses env:
      FETCH_PAGE_LIMIT (fallback 1000)
      OVERSHOOT_MULT   (fallback 2.0) to ensure enough lookback time window
    """
    from datetime import datetime, timedelta, timezone

    if isinstance(symbols, str):
        symbols = [symbols]

    # Normalize request symbols
    req_syms_slash = [s.strip().upper() for s in symbols]
    req_syms_noslash = [s.replace("/", "") for s in req_syms_slash]
    syms_param = ",".join(req_syms_slash)

    # --- compute a reasonable start time window based on limit & TF ---
    tf = timeframe.strip()
    # crude minutes-per-bar map
    tf_minutes = 1
    if tf.endswith("Min"):
        try:
            tf_minutes = int(tf.replace("Min", ""))
        except Exception:
            tf_minutes = 5
    elif tf in ("1H", "1Hour", "1Hour", "1h"):
        tf_minutes = 60
    elif tf in ("5m","1m","15m","30m"):  # if someone passes lowercase
        tf_minutes = int(tf.replace("m",""))
    else:
        # default to 5 minutes if unknown
        tf_minutes = 5

    overshoot = float(os.getenv("OVERSHOOT_MULT", "2.0"))  # widen window to be safe
    lookback_minutes = int(limit * tf_minutes * overshoot)
    start_dt = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    start_iso = start_dt.isoformat(timespec="seconds").replace("+00:00", "Z")

    per_page = int(os.getenv("FETCH_PAGE_LIMIT", "1000"))
    if per_page <= 0:
        per_page = 1000

    url = f"{data_base}/v1beta3/crypto/us/bars"

    # Collect pages
    collected: Dict[str, list] = {s: [] for s in req_syms_slash}
    page_token = None

    while True:
        params = {
            "timeframe": timeframe,
            "symbols": syms_param,
            "limit": min(per_page, limit),
            "start": start_iso,
            # You can also add "end" if you want a fixed window; we allow up to "now".
            # "sort": "asc"  # default may be desc; we sort afterwards anyway
        }
        if page_token:
            params["page_token"] = page_token

        j = _http("GET", url, params=params) or {}
        data = j.get("bars") or {}
        page_token = j.get("next_page_token")

        # Map slash/no-slash keys from response into our slash-form keys
        def _rows_for(k: str):
            return data.get(k) or data.get(k.replace("/", "")) or []

        # Append rows
        for s_slash, s_plain in zip(req_syms_slash, req_syms_noslash):
            rows_src = _rows_for(s_slash) or _rows_for(s_plain)
            if rows_src:
                dest = collected[s_slash]
                for r in rows_src:
                    dest.append({
                        "o": r.get("o"),
                        "h": r.get("h"),
                        "l": r.get("l"),
                        "c": r.get("c"),
                        "v": r.get("v"),
                        "t": r.get("t") or r.get("Timestamp") or r.get("timestamp"),
                    })
                # truncate to requested limit in case server returns extra
                if len(dest) >= limit:
                    collected[s_slash] = dest[:limit]

        # stop conditions: no more pages, or everyone reached limit
        have_all = all(len(collected[s]) >= limit for s in req_syms_slash)
        if have_all or not page_token:
            break

    # final sort (ascending) and ensure presence for each requested key
    out: Dict[str, list] = {}
    for s_slash in req_syms_slash:
        rows = collected.get(s_slash, [])
        rows.sort(key=lambda x: x.get("t") or "")
        out[s_slash] = rows[:limit]
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
