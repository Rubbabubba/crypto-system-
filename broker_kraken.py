#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
broker_kraken.py — Kraken adapter
Build: v2.0.0 (2025-10-12, America/Chicago)

Public API used by app/strategies:
- get_bars(symbol: str, timeframe: str = "5Min", limit: int = 300) -> List[{t,o,h,l,c,v}]
- last_price(symbol: str) -> float
- last_trade_map(symbols: list[str]) -> dict[UI_SYMBOL, {"price": float}]
- market_notional(symbol: str, side: "buy"|"sell", notional: float) -> dict
- orders() -> Any
- positions() -> list[dict]
- trades_history(count: int = 20) -> dict  # recent fills
"""

from __future__ import annotations

__version__ = "2.0.0"

import os
import re
import time
import math
import hmac
import base64
import hashlib
import threading
from typing import Any, Dict, List, Optional

def _userref_for_strategy(strategy: Optional[str]) -> int:
    try:
        if isinstance(strategy, str) and re.fullmatch(r"c[1-9]", strategy.strip().lower()):
            return int(strategy.strip().lower()[1])
    except Exception:
        pass
    import time
    minute = int(time.time() // 60)
    h = hash((strategy or 'x', minute))
    return int(h & 0x7fffffff)


import requests

# ---------------------------------------------------------------------------
# Robust local import of symbol_map helpers (works in flat or packaged repo)
# ---------------------------------------------------------------------------
try:
    from symbol_map import to_kraken, from_kraken, tf_to_kraken
except ModuleNotFoundError:
    import sys
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from symbol_map import to_kraken, from_kraken, tf_to_kraken  # type: ignore

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_BASE = os.getenv("KRAKEN_BASE", "https://api.kraken.com")
TIMEOUT = float(os.getenv("KRAKEN_TIMEOUT", "10"))        # seconds
MIN_DELAY = float(os.getenv("KRAKEN_MIN_DELAY", "0.35"))  # seconds between calls (simple gate)
MAX_RETRIES = int(os.getenv("KRAKEN_MAX_RETRIES", "4"))
BACKOFF_BASE = float(os.getenv("KRAKEN_BACKOFF_BASE", "0.8"))

API_KEY = os.getenv("KRAKEN_KEY", "")
API_SECRET = os.getenv("KRAKEN_SECRET", "")

SESSION = requests.Session()
_GATE_LOCK = threading.Lock()
_LAST_CALL = 0.0

# ---------------------------------------------------------------------------
# Rate gate
# ---------------------------------------------------------------------------
def _rate_gate():
    global _LAST_CALL
    with _GATE_LOCK:
        now = time.time()
        wait = MIN_DELAY - (now - _LAST_CALL)
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL = time.time()

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _pub(path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    url = f"{API_BASE}/0/public/{path}"
    for attempt in range(MAX_RETRIES):
        try:
            _rate_gate()
            r = SESSION.get(url, params=params or {}, timeout=TIMEOUT)
            if r.status_code == 429:
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("error"):
                if attempt < MAX_RETRIES - 1:
                    time.sleep(BACKOFF_BASE * (2 ** attempt))
                    continue
                raise RuntimeError(f"Kraken public error: {data['error']}")
            return data.get("result", {})
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue
            raise RuntimeError(f"HTTP public error: {e}") from e
    return {}

def _sign(urlpath: str, data: Dict[str, Any]) -> Dict[str, str]:
    postdata = "&".join(f"{k}={data[k]}" for k in data)
    message = (str(data["nonce"]) + postdata).encode()
    sha256 = hashlib.sha256(message).digest()
    mac = hmac.new(base64.b64decode(API_SECRET), urlpath.encode() + sha256, hashlib.sha512)
    sig = base64.b64encode(mac.digest()).decode()
    return {"API-Key": API_KEY, "API-Sign": sig}

def _priv(path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if not API_KEY or not API_SECRET:
        raise RuntimeError("Missing KRAKEN_KEY / KRAKEN_SECRET for private call")
    urlpath = f"/0/private/{path}"
    url = f"{API_BASE}{urlpath}"
    for attempt in range(MAX_RETRIES):
        try:
            _rate_gate()
            payload = dict(params or {})
            payload["nonce"] = str(int(time.time() * 1000))
            headers = _sign(urlpath, payload)
            r = SESSION.post(url, data=payload, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("error"):
                errtxt = ";".join(data["error"])
                transient = any(code in errtxt for code in (
                    "EGeneral:Internal error",
                    "EAPI:Rate limit exceeded",
                    "EService:Unavailable",
                    "EService:Timeout",
                ))
                if transient and attempt < MAX_RETRIES - 1:
                    time.sleep(BACKOFF_BASE * (2 ** attempt))
                    continue
                raise RuntimeError(f"Kraken private error: {errtxt}")
            return data.get("result", {})
        except requests.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue
            raise RuntimeError(f"HTTP private error: {e}") from e
    return {}

# ---------------------------------------------------------------------------
# Kraken key → UI symbol heuristics (for robustness)
# ---------------------------------------------------------------------------
_KNOWN_QUOTES = ("USD", "USDT", "USDC", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF")

_BASE_MAP = {
    "XBT": "BTC", "XXBT": "BTC",
    "ETH": "ETH", "XETH": "ETH",
    "ETC": "ETC", "XETC": "ETC",
    "XDG": "DOGE", "DOGE": "DOGE",
    "XRP": "XRP", "XXRP": "XRP",
    "LTC": "LTC", "XLTC": "LTC",
    "BCH": "BCH", "XBCH": "BCH",
    "SOL": "SOL",
    "ADA": "ADA",
    "AVAX": "AVAX",
    "LINK": "LINK",
    # add more as needed
}

def _normalize_base(b: str) -> str:
    b = b.upper()
    if len(b) > 1 and b[0] == "X":
        b2 = b[1:]
        if b2 in _BASE_MAP:
            return _BASE_MAP[b2]
    return _BASE_MAP.get(b, b)

def _normalize_quote(q: str) -> str:
    q = q.upper()
    if len(q) > 1 and q[0] == "Z":
        q = q[1:]
    if q in ("USD", "USDT", "USDC"):
        return "USD"
    return q

def _kraken_key_to_ui_pair(k: str) -> Optional[str]:
    """
    Convert Kraken key (e.g., 'XXBTZUSD' or 'XETHZUSD' or 'XBTUSD') to UI 'BTCUSD'/'ETHUSD'.
    IMPORTANT: check 'Z'+fiat suffix BEFORE plain fiat to handle '...ZUSD' correctly.
    """
    K = k.upper()
    for q in _KNOWN_QUOTES:
        if K.endswith("Z" + q):
            base = K[: -(len(q) + 1)]
            return _normalize_base(base) + _normalize_quote("Z" + q)
        if K.endswith(q):
            base = K[: -len(q)]
            return _normalize_base(base) + _normalize_quote(q)
    try:
        return from_kraken(k).upper()
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Public endpoints
# ---------------------------------------------------------------------------
def last_trade_map(symbols: List[str]) -> Dict[str, Dict[str, float]]:
    """
    Return { UI_SYMBOL: {"price": float} } for each requested symbol.
    Works even if Kraken responds with keys like 'XXBTZUSD' by normalizing them.
    """
    if not symbols:
        return {}
    req_syms = [s.upper() for s in symbols]
    want_pair = {ui: to_kraken(ui) for ui in req_syms}  # e.g., BTCUSD -> XBTUSD

    pairs = ",".join(want_pair.values())
    res = _pub("Ticker", {"pair": pairs}) or {}

    k_price: Dict[str, float] = {}
    for k, v in res.items():
        try:
            px = float((v or {}).get("c", ["0"])[0])
        except Exception:
            px = 0.0
        k_price[k.upper()] = px

    out: Dict[str, Dict[str, float]] = {ui: {"price": 0.0} for ui in req_syms}

    for ui in req_syms:
        target_alt = want_pair[ui].upper()  # e.g., XBTUSD
        px = None
        if target_alt in k_price:
            px = k_price[target_alt]
        else:
            for kk, vv in k_price.items():
                ui_guess = _kraken_key_to_ui_pair(kk)
                if ui_guess == ui:
                    px = vv
                    break
        out[ui] = {"price": float(px) if (px is not None and math.isfinite(px)) else 0.0}

    return out

def last_price(symbol: str) -> float:
    mp = last_trade_map([symbol])
    try:
        return float((mp.get(symbol.upper()) or {}).get("price", 0.0))
    except Exception:
        return 0.0

def get_bars(symbol: str, timeframe: str = "5Min", limit: int = 300) -> List[Dict[str, Any]]:
    """
    Returns list of bars: [{t,o,h,l,c,v}] (epoch seconds; newest last)
    Robustly selects the right series even if Kraken uses 'XXBTZUSD' keys.
    """
    pair = to_kraken(symbol)                   # e.g., BTCUSD -> XBTUSD
    interval = int(tf_to_kraken(timeframe) or 5)
    res = _pub("OHLC", {"pair": pair, "interval": interval}) or {}

    series = None
    ui_target = symbol.upper()
    for key, val in res.items():
        if not isinstance(val, list):
            continue
        if key.upper() == pair.upper():
            series = val
            break
        ui_guess = _kraken_key_to_ui_pair(key)
        if ui_guess == ui_target:
            series = val
    if series is None:
        for key, val in res.items():
            if isinstance(val, list) and val and isinstance(val[0], list):
                series = val
                break

    if not series:
        return []

    out: List[Dict[str, Any]] = []
    for row in series[-int(limit):]:
        try:
            t = int(row[0])
            o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4])
            v = float(row[6])  # base volume
            out.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": v})
        except Exception:
            continue

    out.sort(key=lambda x: x["t"])
    return out

# ---------------------------------------------------------------------------
# Private endpoints
# ---------------------------------------------------------------------------
def _round_qty(q: float) -> float:
    return float(f"{q:.8f}")

def _userref(symbol: str, side: str, notional: float) -> int:
    minute = int(time.time() // 60)
    h = hash((symbol.upper(), side.lower(), round(float(notional), 4), minute))
    return int(h & 0x7FFFFFFF)

def _ensure_price(symbol: str) -> float:
    p = last_price(symbol)
    if not p or not math.isfinite(p) or p <= 0:
        raise RuntimeError(f"no price available for {symbol}")
    return p

def market_notional(symbol: str, side: str, notional: float, strategy: Optional[str] = None) -> Dict[str, Any]:
    """
    Market order by USD notional:
      volume(base) = notional(quote USD) / last_price
    Returns: { pair, side, notional, volume, txid, descr, result }
    """
    side = side.lower().strip()
    if side not in ("buy", "sell"):
        raise ValueError("side must be 'buy' or 'sell'")

    ui = symbol.upper()
    pair = to_kraken(ui)
    px = _ensure_price(ui)
    volume = _round_qty(float(notional) / px)
    if volume <= 0:
        raise ValueError("computed volume <= 0")

    payload = {
        "pair": pair,
        "type": "buy" if side == "buy" else "sell",
        "ordertype": "market",
        "volume": f"{volume:.8f
        \"userref\": str(_userref_for_strategy(strategy)),
    }",
        "userref": str(_userref(ui, side, float(notional))),
    }
    res = _priv("AddOrder", payload)

    txid = None
    descr = None
    try:
        txid = (res.get("txid") or [None])[0]
        descr = (res.get("descr") or {}).get("order")
    except Exception:
        pass

    return {
        "pair": pair,
        "side": side,
        "notional": float(notional),
        "volume": volume,
        "txid": txid,
        "descr": descr,
        "result": res,
    }

def orders() -> Any:
    try:
        return _priv("OpenOrders", {})
    except Exception as e:
        return {"error": str(e)}

def positions() -> List[Dict[str, Any]]:
    """
    Normalize Kraken balance keys:
    - Strip '.F' suffix (e.g., 'SOL.F' -> 'SOL')
    - Map 'XXBT'/'XBT'->'BTC', 'XETH'->'ETH', 'ZUSD'->'USD', etc.
    """
    out: List[Dict[str, Any]] = []
    try:
        bal = _priv("Balance", {})  # {"ZUSD":"123.45","XXBT":"0.01","SOL.F":"0.12",...}
        for k, v in (bal or {}).items():
            try:
                qty = float(v)
            except Exception:
                qty = 0.0
            if qty <= 0:
                continue
            asset = k.upper()
            if asset.endswith(".F"):  # futures/ledger suffix seen for some assets
                asset = asset[:-2]
            # common maps
            if asset in ("ZUSD", "USD"):
                asset = "USD"
            elif asset in ("XXBT", "XBT"):
                asset = "BTC"
            elif asset in ("XETH", "ETH"):
                asset = "ETH"
            elif asset in ("XDG", "DOGE"):
                asset = "DOGE"
            elif asset in ("XXRP", "XRP"):
                asset = "XRP"
            elif asset in ("XLTC", "LTC"):
                asset = "LTC"
            elif asset in ("XBCH", "BCH"):
                asset = "BCH"
            # pass-through for others like SOL, ADA, AVAX, LINK
            out.append({"asset": asset, "qty": qty})
    except Exception as e:
        out.append({"error": str(e)})
    return out

def trades_history(count: int = 20) -> Dict[str, Any]:
    """
    Return recent trades (fills). Pass-through of Kraken's TradesHistory, normalized to a list.
    """
    try:
        res = _priv("TradesHistory", {"type": "all", "ofs": 0})
        trades = list((res.get("trades") or {}).items())  # [(txid, {...}), ...]
        trades.sort(key=lambda kv: float(kv[1].get("time", 0)), reverse=True)
        items = []
        for tid, t in trades[: max(1, int(count))]:
            items.append({
                "txid": tid,
                "pair": t.get("pair"),
                "type": t.get("type"),
                "ordertype": t.get("ordertype"),
                "price": float(t.get("price", 0) or 0),
                "vol": float(t.get("vol", 0) or 0),
                "time": t.get("time"),
                "fee": float(t.get("fee", 0) or 0),
                "cost": float(t.get("cost", 0) or 0),
            })
        return {"ok": True, "trades": items}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# --- Added helper: trade_details -------------------------------------------------
def trade_details(ids):
    """
    Accepts a mixed list of Kraken order ids (start with 'O') and trade ids (start with 'T').
    Returns a dict keyed by BOTH order and trade ids, each containing as many of:
    ordertxid, descr, userref, price, vol, fee, cost, filled_ts.
    """
    out = {}
    try:
        client = globals().get("_client") or globals().get("client") or None
        if not client:
            return out
        ids = [i for i in (ids or []) if i]
        if not ids:
            return out

        trade_ids = [i for i in ids if str(i).startswith("T")]
        order_ids = [i for i in ids if str(i).startswith("O")]

        # 1) Pull orders; collect their trade ids
        orders = {}
        if order_ids:
            try:
                qor = client.request("QueryOrders", {"txid": ",".join(order_ids)})
                orders = (qor.get("result") or {}) if isinstance(qor, dict) else {}
            except Exception:
                orders = {}

            for o in (orders or {}).values():
                for tid in (o.get("trades") or []):
                    if tid and tid not in trade_ids:
                        trade_ids.append(tid)

        # 2) Pull trades (includes those discovered from orders)
        tr_res = {}
        if trade_ids:
            try:
                qtr = client.request("QueryTrades", {"txid": ",".join(trade_ids)})
                tr_res = (qtr.get("result") or {}) if isinstance(qtr, dict) else {}
            except Exception:
                tr_res = {}

        # 3) Build rows for each trade id
        for tid, t in (tr_res or {}).items():
            if not t:
                continue
            row = {}
            row["ordertxid"] = t.get("ordertxid")
            # numeric fields (best-effort)
            for k_src, k_dst in [("price", "price"), ("vol", "vol"), ("fee", "fee"), ("cost", "cost")]:
                v = t.get(k_src)
                try:
                    row[k_dst] = float(v) if v is not None else None
                except Exception:
                    row[k_dst] = None
            # Kraken "time" is epoch seconds
            try:
                row["filled_ts"] = float(t.get("time")) if t.get("time") is not None else None
            except Exception:
                row["filled_ts"] = None
            out[tid] = row

        # 4) Build/augment rows for each order id
        for oid, o in (orders or {}).items():
            row = out.get(oid, {})
            desc_blob = o.get("descr") or {}
            if isinstance(desc_blob, dict):
                row["descr"] = desc_blob.get("order") or ""
            else:
                row["descr"] = desc_blob or ""
            if "userref" in o and o["userref"] is not None:
                row["userref"] = o["userref"]

            # If the order lists trades, copy the latest trade's monetized fields
            tr_list = o.get("trades") or []
            if tr_list:
                last_tid = tr_list[-1]
                t = (tr_res or {}).get(last_tid) or {}
                for k_src, k_dst in [("price", "price"), ("vol", "vol"), ("fee", "fee"), ("cost", "cost")]:
                    v = t.get(k_src)
                    try:
                        row[k_dst] = float(v) if v is not None else row.get(k_dst)
                    except Exception:
                        pass
                try:
                    row["filled_ts"] = float(t.get("time")) if t.get("time") is not None else row.get("filled_ts")
                except Exception:
                    pass

            out[oid] = row

        return out
    except Exception:
        return out
