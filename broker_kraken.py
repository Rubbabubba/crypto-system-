#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
broker_kraken.py — Kraken adapter
Build: v2.1.0 (2025-10-12, America/Chicago)

Public API used by app/strategies:
- get_bars(symbol: str, timeframe: str = "5Min", limit: int = 300) -> List[{t,o,h,l,c,v}]
- last_price(symbol: str) -> float
- last_trade_map(symbols: list[str]) -> dict[UI_SYMBOL, {"price": float}]
- market_notional(symbol: str, side: "buy"|"sell", notional: float, price: float|None = None, strategy: str|None = None, **kwargs) -> dict
- orders() -> Any
- positions() -> list[dict]
- trades_history(count: int = 20) -> dict  # recent fills
"""

from __future__ import annotations

__version__ = "2.1.0"

import os
import re
import time
import threading

# Patch 3: robust monotonic nonce generator (Kraken requires strictly increasing nonces per API key)
_nonce_lock = threading.Lock()
_last_nonce_ms = 0

def _next_nonce_ms() -> int:
    """Return a strictly increasing millisecond nonce (process-wide)."""
    global _last_nonce_ms
    now = time.time_ns() // 1_000_000  # ms
    with _nonce_lock:
        if now <= _last_nonce_ms:
            now = _last_nonce_ms + 1
        _last_nonce_ms = now
        return now
import math
import hmac
import base64
import hashlib
import threading
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def _load_strategy_to_userref() -> Dict[str, int]:
    """Load mapping from strategy name -> Kraken userref (int).

    Supports both of these JSON shapes in policy_config/userref_map.json:

      1. {"c1": 101, "c2": 102, ...}
      2. {"101": "c1", "102": "c2", ...}
    """
    cfg_path = Path(os.getenv("POLICY_CFG_DIR", "policy_config")) / "userref_map.json"
    try:
        with cfg_path.open("r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        return {}

    if not isinstance(cfg, dict) or not cfg:
        return {}

    mapping: Dict[str, int] = {}

    first_value = next(iter(cfg.values()))
    if isinstance(first_value, int):
        # Shape 1: strategy -> int userref
        for strat, ref in cfg.items():
            try:
                mapping[str(strat)] = int(ref)
            except Exception:
                continue
    else:
        # Shape 2: userref string -> strategy
        for ref, strat in cfg.items():
            try:
                mapping[str(strat)] = int(ref)
            except Exception:
                continue

    return mapping


_STRATEGY_TO_USERREF: Dict[str, int] = _load_strategy_to_userref()


def _userref_for_strategy(strategy: Optional[str]) -> int:
    """Resolve a stable Kraken userref for a given strategy.

    We require a non-empty strategy tag for all orders so fills can be
    reliably attributed back to strategy. Prefer explicit mappings from
    policy_config/userref_map.json; otherwise fall back to a deterministic
    stable hash of the strategy string.
    """
    if not isinstance(strategy, str) or not strategy.strip():
        raise ValueError("missing strategy tag for order (strategy is required)")
    key = strategy.strip()
    if key in _STRATEGY_TO_USERREF:
        return int(_STRATEGY_TO_USERREF[key])

    # Stable deterministic fallback (no time component)
    h = hash(key)
    return int(h & 0x7FFFFFFF)

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
# Order cooldown latch (authoritative gateway)
# ---------------------------------------------------------------------------
_ORDER_LATCH_LOCK = threading.Lock()
_ORDER_LATCH: Dict[str, Dict[str, Any]] = {}  # KRAKEN_PAIR (e.g. SUIUSD/XBTUSD) -> {side, ts, strategy}

def _cooldown_seconds(name: str, default: int = 0) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return default

def _cooldown_check_and_latch(strategy: str, kraken_pair: str, side: str) -> None:
    """Raise if an order should be blocked by cooldown/min-hold; otherwise latch immediately."""
    same_side = _cooldown_seconds('SCHED_COOLDOWN_SAME_SIDE_SECONDS', 0)
    flip_side = _cooldown_seconds('SCHED_COOLDOWN_FLIP_SECONDS', 0)
    min_hold  = _cooldown_seconds('SCHED_MIN_HOLD_SECONDS', 0)
    if same_side <= 0 and flip_side <= 0 and min_hold <= 0:
        return

    now = time.time()
    key = str(kraken_pair or "").upper()  # IMPORTANT: latch on the exchange pair string

    with _ORDER_LATCH_LOCK:
        prev = _ORDER_LATCH.get(key)
        if prev is not None:
            last_side = str(prev.get('side', '')).lower()
            last_ts = float(prev.get('ts', 0.0) or 0.0)
            dt = now - last_ts

            if last_side == side and same_side > 0 and dt < same_side:
                raise RuntimeError(f'cooldown_same_side strat={strategy} pair={key} dt={dt:.1f}s < {same_side}s')

            if last_side and last_side != side:
                if flip_side > 0 and dt < flip_side:
                    raise RuntimeError(f'cooldown_flip strat={strategy} pair={key} {last_side}->{side} dt={dt:.1f}s < {flip_side}s')
                if min_hold > 0 and dt < min_hold:
                    raise RuntimeError(f'min_hold strat={strategy} pair={key} {last_side}->{side} dt={dt:.1f}s < {min_hold}s')

        # latch immediately (authoritative gateway). If AddOrder fails, caller rolls back.
        _ORDER_LATCH[key] = {'side': side, 'ts': now, 'strategy': strategy}

def _cooldown_rollback(strategy: str, kraken_pair: str, side: str) -> None:
    key = str(kraken_pair or "").upper()
    with _ORDER_LATCH_LOCK:
        prev = _ORDER_LATCH.get(key)
        if not prev:
            return
        if str(prev.get('side','')).lower() != side:
            return
        # Only roll back if the latch was set by this same strategy; otherwise leave it.
        if str(prev.get('strategy','')) == strategy:
            _ORDER_LATCH.pop(key, None)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_BASE = os.getenv("KRAKEN_BASE", "https://api.kraken.com")
TIMEOUT = float(os.getenv("KRAKEN_TIMEOUT", "10"))        # seconds
MIN_DELAY = float(os.getenv("KRAKEN_MIN_DELAY", "0.35"))  # seconds between calls (simple gate)
MAX_RETRIES = int(os.getenv("KRAKEN_MAX_RETRIES", "4"))
BACKOFF_BASE = float(os.getenv("KRAKEN_BACKOFF_BASE", "0.8"))

# Keep broker env resolution in sync with app.py.
# We accept both naming schemes so either set works:
# - New:    KRAKEN_API_KEY / KRAKEN_API_SECRET
# - Legacy: KRAKEN_KEY / KRAKEN_SECRET
# - Also optional "_1" suffixed variants used in some deployments.
_KRAKEN_KEY_NAMES = (
    "KRAKEN_API_KEY",
    "KRAKEN_KEY",
    "KRAKEN_KEY_1",
    "KRAKEN_API_KEY_1",
)
_KRAKEN_SECRET_NAMES = (
    "KRAKEN_API_SECRET",
    "KRAKEN_SECRET",
    "KRAKEN_SECRET_1",
    "KRAKEN_API_SECRET_1",
)

def _get_env_first(*names: str):
    for n in names:
        v = os.getenv(n)
        if v:
            return v, n
    return "", ""

def _get_kraken_creds():
    key, key_name = _get_env_first(*_KRAKEN_KEY_NAMES)
    sec, sec_name = _get_env_first(*_KRAKEN_SECRET_NAMES)
    return key, sec, key_name, sec_name

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
    api_key, api_secret, _, _ = _get_kraken_creds()
    postdata = "&".join(f"{k}={data[k]}" for k in data)
    message = (str(data["nonce"]) + postdata).encode()
    sha256 = hashlib.sha256(message).digest()
    mac = hmac.new(base64.b64decode(api_secret), urlpath.encode() + sha256, hashlib.sha512)
    sig = base64.b64encode(mac.digest()).decode()
    return {"API-Key": api_key, "API-Sign": sig}

def _priv(path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
    api_key, api_secret, key_name, sec_name = _get_kraken_creds()
    if not api_key or not api_secret:
        raise RuntimeError(
            "Missing Kraken API creds. Set one of: "
            f"{_KRAKEN_KEY_NAMES} and {_KRAKEN_SECRET_NAMES}. "
            f"Detected key_env='{key_name}' secret_env='{sec_name}'."
        )
    urlpath = f"/0/private/{path}"
    url = f"{API_BASE}{urlpath}"
    for attempt in range(MAX_RETRIES):
        try:
            _rate_gate()
            payload = dict(params or {})
            payload["nonce"] = str(_next_nonce_ms())
            headers = _sign(urlpath, payload)
            r = SESSION.post(url, data=payload, headers=headers, timeout=TIMEOUT)
            if r.status_code == 429:
                time.sleep(BACKOFF_BASE * (2 ** attempt))
                continue
            r.raise_for_status()
            data = r.json()
            if data.get("error"):
                errtxt = ";".join(data["error"])
                # Kraken sometimes returns invalid nonce when requests are too close together; retry with fresh nonce
                if "EAPI:Invalid nonce" in errtxt or "EGeneral:Invalid arguments" in errtxt and "nonce" in errtxt.lower():
                    time.sleep(0.25 * (attempt + 1))
                    continue