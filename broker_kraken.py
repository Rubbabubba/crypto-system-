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

Key behaviors:
- Robust mapping: handles Kraken keys like 'XXBTZUSD'/'XETHZUSD' and maps them to UI symbols 'BTCUSD'/'ETHUSD'
- Rate-limit gate + retries/backoff for transient errors
- Idempotency via userref for market orders
"""

from __future__ import annotations

__version__ = "2.0.0"

import os
import time
import math
import hmac
import base64
import hashlib
import threading
from typing import Any, Dict, List, Optional

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
TIMEOUT = float(os.getenv("KRAKEN_TIMEOUT", "10"))      # seconds
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
    "XBT": "BTC",
    "XXBT": "BTC",
    "ETH": "ETH",
    "XETH": "ETH",
    "ETC": "ETC",
    "XETC": "ETC",
    "XDG": "DOGE",
    "DOGE": "DOGE",
    "XRP": "XRP",
    "XXRP": "XRP",
    "LTC": "LTC",
    "XLTC": "LTC",
    "BCH": "BCH",
    "XBCH": "BCH",
    "SOL": "SOL",
    "ADA": "ADA",
    "AVAX": "AVAX",
    "LINK": "LINK",
    # add others as needed; unknowns fall through
}

def _normalize_base(b: str) -> str:
    b = b.upper()
    # strip a single leading X if present (Kraken convention for crypto)
    if len(b) > 1 and b[0] == "X":
        b2 = b[1:]
        if b2 in _BASE_MAP:
            return _BASE_MAP[b2]
    return _BASE_MAP.get(b, b)

def _normalize_quote(q: str) -> str:
    q = q.upper()
    # strip leading Z for fiat (Kraken convention)
    if len(q) > 1 and q[0] == "Z":
        q = q[1:]
    # normalize to USD (your system expects USD)
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
        # 1) handle 'Z'+fiat (Kraken fiat convention) FIRST
        if K.endswith("Z" + q):
            base = K[: -(len(q) + 1)]
            return _normalize_base(base) + _normalize_quote("Z" + q)
        # 2) plain fiat suffix (e.g., 'XBTUSD')
        if K.endswith(q):
            base = K[: -len(q)]
            return _normalize_base(base) + _normalize_quote(q)
    # 3) final fallback via symbol_map
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

    # Kraken supports comma-joined pairs
    pairs = ",".join(want_pair.values())
    res = _pub("Ticker", {"pair": pairs}) or {}

    # Ingest all returned keys → price
    k_price: Dict[str, float] = {}
    for k, v in res.items():
        try:
            px = float((v or {}).get("c", ["0"])[0])
        except Exception:
            px = 0.0
        k_price[k.upper()] = px

    out: Dict[str, Dict[str, float]] = {ui: {"price": 0.0} for ui in req_syms}

    # Resolve for each requested UI symbol
    for ui in req_syms:
        target_alt = want_pair[ui].upper()  # e.g., XBTUSD
        px = None

        # 1) exact altname key
        if target_alt in k_price:
            px = k_price[target_alt]
        else:
            # 2) normalize every returned key to UI and compare
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
    # Prefer exact altname key; fallback: any key that normalizes to our UI symbol
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
            # don't break immediately; try to find exact altname first, else keep this
    if series is None:
        # final fallback: pick the first list-looking value
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

def market_notional(symbol: str, side: str, notional: float) -> Dict[str, Any]:
    """
    Market order by USD notional:
      volume(base) = notional(quote USD) / last_price
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
        "volume": f"{volume:.8f}",
        "userref": str(_userref(ui, side, float(notional))),
    }
    res = _priv("AddOrder", payload)
    return {"pair": pair, "side": side, "notional": float(notional), "volume": volume, "result": res}

def orders() -> Any:
    try:
        return _priv("OpenOrders", {})
    except Exception as e:
        return {"error": str(e)}

def positions() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    try:
        bal = _priv("Balance", {})  # {"ZUSD":"123.45","XXBT":"0.01",...}
        for k, v in (bal or {}).items():
            try:
                qty = float(v)
            except Exception:
                qty = 0.0
            if qty <= 0:
                continue
            asset = (
                "USD" if k.upper() in ("ZUSD", "USD") else
                "BTC" if k.upper() in ("XXBT", "XBT") else
                "ETH" if k.upper() in ("XETH", "ETH") else
                "SOL" if k.upper() == "SOL" else
                "ADA" if k.upper() == "ADA" else
                "DOGE" if k.upper() in ("XDG", "DOGE") else
                "XRP" if k.upper() in ("XXRP", "XRP") else
                "LTC" if k.upper() in ("XLTC", "LTC") else
                "BCH" if k.upper() in ("XBCH", "BCH") else
                k
            )
            out.append({"asset": asset, "qty": qty})
    except Exception as e:
        out.append({"error": str(e)})
    return out
