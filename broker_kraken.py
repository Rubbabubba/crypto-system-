#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
broker_kraken.py — Kraken adapter
Build: v2.0.0 (2025-10-11, America/Chicago)

Public API (used by app.py/strategies)
- get_bars(symbol: str, timeframe: str = "5Min", limit: int = 300) -> List[dict{t,o,h,l,c,v}]
- last_price(symbol: str) -> float
- last_trade_map(symbols: list[str]) -> dict[str, {"price": float}]
- market_notional(symbol: str, side: "buy"|"sell", notional: float) -> dict
- orders() -> dict | list
- positions() -> list[dict]

Notes
- Requires env: KRAKEN_KEY, KRAKEN_SECRET for private endpoints
- Timeframes map via symbol_map.tf_to_kraken()
- Symbols map via symbol_map.to_kraken() / from_kraken()
- Idempotency: userref ensures “at-most-once” semantics on replays
- Basic rate-limit gate + retries with exponential backoff for transient errors
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

from symbol_map import to_kraken, from_kraken, tf_to_kraken

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
API_BASE = os.getenv("KRAKEN_BASE", "https://api.kraken.com")
TIMEOUT = float(os.getenv("KRAKEN_TIMEOUT", "10"))  # seconds
# conservative gate; Kraken uses "weights" but 0.35s pause avoids most 429s
MIN_DELAY = float(os.getenv("KRAKEN_MIN_DELAY", "0.35"))
MAX_RETRIES = int(os.getenv("KRAKEN_MAX_RETRIES", "4"))
BACKOFF_BASE = float(os.getenv("KRAKEN_BACKOFF_BASE", "0.8"))  # seconds

API_KEY = os.getenv("KRAKEN_KEY", "")
API_SECRET = os.getenv("KRAKEN_SECRET", "")

SESSION = requests.Session()
_GATE_LOCK = threading.Lock()
_LAST_CALL = 0.0

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _rate_gate():
    global _LAST_CALL
    with _GATE_LOCK:
        now = time.time()
        wait = MIN_DELAY - (now - _LAST_CALL)
        if wait > 0:
            time.sleep(wait)
        _LAST_CALL = time.time()

def _pub(path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    url = f"{API_BASE}/0/public/{path}"
    for attempt in range(MAX_RETRIES):
        try:
            _rate_gate()
            r = SESSION.get(url, params=params or {}, timeout=TIMEOUT)
            if r.status_code == 429:  # rate limited
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("error"):
                # Kraken error codes are strings like "EGeneral:Internal error"
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
    """
    Kraken private signature.
    urlpath starts with '/0/private/...'
    """
    postdata = "&".join(f"{k}={data[k]}" for k in data)
    # nonce + hash
    message = (str(data["nonce"]) + postdata).encode()
    sha256 = hashlib.sha256(message).digest()
    # sign
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
            # Kraken requires a monotonically increasing nonce
            payload["nonce"] = str(int(time.time() * 1000))
            headers = _sign(urlpath, payload)
            r = SESSION.post(url, data=payload, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("error"):
                # transient/internal/rate-limit -> retry
                errtxt = ";".join(data["error"])
                transient = any(
                    code in errtxt
                    for code in (
                        "EGeneral:Internal error",
                        "EAPI:Rate limit exceeded",
                        "EService:Unavailable",
                        "EService:Timeout",
                    )
                )
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

def _round_qty(q: float) -> float:
    # 8 decimals is generally safe; real precision is per-asset (could enhance later)
    return float(f"{q:.8f}")

def _userref(symbol: str, side: str, notional: float) -> int:
    """
    Stable-ish idempotency key: hash of (symbol, side, notional, minute-slot).
    Kraken userref accepts signed 32-bit ints; constrain via modulo.
    """
    minute = int(time.time() // 60)
    h = hash((symbol.upper(), side.lower(), round(float(notional), 4), minute))
    # fit to 31-bit signed range
    return int(h & 0x7FFFFFFF)

# -----------------------------------------------------------------------------
# Public endpoints
# -----------------------------------------------------------------------------
def last_trade_map(symbols: List[str]) -> Dict[str, Dict[str, float]]:
    if not symbols:
        return {}
    pairs = [to_kraken(s) for s in symbols]
    # Kraken ticker supports comma-delimited list in 'pair'
    res = _pub("Ticker", {"pair": ",".join(pairs)}) or {}
    out: Dict[str, Dict[str, float]] = {}
    for k, v in res.items():
        ui = from_kraken(k)
        try:
            price = float((v or {}).get("c", ["0"])[0])
        except Exception:
            price = 0.0
        out[ui] = {"price": price}
    # Ensure all keys present
    for s in symbols:
        out.setdefault(s.upper(), {"price": float(out.get(s.upper(), {}).get("price", 0.0))})
    return out

def last_price(symbol: str) -> float:
    mp = last_trade_map([symbol])
    return float((mp.get(symbol.upper()) or {}).get("price", 0.0))

def get_bars(symbol: str, timeframe: str = "5Min", limit: int = 300) -> List[Dict[str, Any]]:
    """
    Returns list of bars: [{t,o,h,l,c,v}] (epoch seconds; newest last)
    """
    pair = to_kraken(symbol)
    interval = int(tf_to_kraken(timeframe) or 5)  # minutes
    res = _pub("OHLC", {"pair": pair, "interval": interval}) or {}
    series = None
    # Kraken returns { "XBTUSD": [ [ts, open, high, low, close, vwap, volume, count], ... ] }
    for key, val in res.items():
        # pick the array matching our pair; sometimes key is altname like "XXBTZUSD" vs "XBTUSD"
        if key.upper().endswith(pair.upper()) or key.upper() == pair.upper():
            series = val
            break
        # fallback to first list-looking value
        if series is None and isinstance(val, list) and val and isinstance(val[0], list):
            series = val
    if not series:
        return []
    # Kraken’s series sometimes includes the last (still-open) candle; we’ll keep it
    out: List[Dict[str, Any]] = []
    for row in series[-int(limit):]:
        try:
            t = int(row[0])
            o = float(row[1]); h = float(row[2]); l = float(row[3]); c = float(row[4])
            v = float(row[6])  # row[6] is "volume" in base units
            out.append({"t": t, "o": o, "h": h, "l": l, "c": c, "v": v})
        except Exception:
            continue
    # Ascending by time
    out.sort(key=lambda x: x["t"])
    return out

# -----------------------------------------------------------------------------
# Private endpoints
# -----------------------------------------------------------------------------
def _ensure_price(symbol: str) -> float:
    p = last_price(symbol)
    if not p or not math.isfinite(p) or p <= 0:
        raise RuntimeError(f"no price available for {symbol}")
    return p

def market_notional(symbol: str, side: str, notional: float) -> Dict[str, Any]:
    """
    Market order by USD notional:
      volume(base) = notional(quote USD) / last_price(quote)
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
        # Optional flags: "viqc" (volume in quote) not supported on spot; we compute volume ourselves
        # "timeinforce": "IOC" / "GTC" could be added; Kraken defaults are fine for market
    }
    res = _priv("AddOrder", payload)
    # Typical response: {"descr":{"order":"buy 0.0001 XBTUSD market"}, "txid":["OXXXXXXXXX"]}
    return {"pair": pair, "side": side, "notional": float(notional), "volume": volume, "result": res}

def orders() -> Any:
    """
    Open orders snapshot (pass-through of Kraken shape with minimal normalization).
    """
    try:
        res = _priv("OpenOrders", {})
        # res: {"open": {"OXXXX":{"descr":{...},"vol":"...","vol_exec":"...","status":"open",...}}, "count":1}
        return res
    except Exception as e:
        return {"error": str(e)}

def positions() -> List[Dict[str, Any]]:
    """
    Kraken spot doesn't expose 'positions' like futures; we emulate spot “holdings”
    via open orders is not correct. Return empty or balances if needed.
    For now, return balances for core assets as a convenience.
    """
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
            # Map a few common Kraken asset codes to UI-ish tickers
            asset = (
                "USD" if k.upper().endswith("USD") or k.upper() == "ZUSD" else
                "BTC" if k.upper() in ("XXBT", "XBT") else
                "ETH" if k.upper() in ("XETH", "ETH") else
                k
            )
            out.append({"asset": asset, "qty": qty})
    except Exception as e:
        out.append({"error": str(e)})
    return out
