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
import math
import hmac
import base64
import hashlib
import threading
import json
import logging
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


def _format_price(x: float, decimals: int) -> str:
    """Format Kraken price string with correct per-pair precision.

    Kraken rejects prices with more decimals than `pair_decimals` for the pair.
    We truncate (not round) to avoid inadvertently crossing boundaries.
    """
    try:
        d = max(0, int(decimals))
    except Exception:
        d = 0
    px = _truncate_to_decimals(float(x), d)
    if d <= 0:
        return str(int(px))
    return f"{px:.{d}f}"


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

logger = logging.getLogger("broker_kraken")


_BALANCE_BOOTSTRAP_PATH = Path(os.getenv("BALANCE_BOOTSTRAP_SNAPSHOT_PATH", "/var/data/kraken_balance_bootstrap.json"))

def _write_balance_bootstrap_event(payload: Dict[str, Any]) -> None:
    try:
        _BALANCE_BOOTSTRAP_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _BALANCE_BOOTSTRAP_PATH.with_suffix(_BALANCE_BOOTSTRAP_PATH.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(_BALANCE_BOOTSTRAP_PATH)
    except Exception:
        return

def _record_balance_bootstrap_event(*, status: str, result: Any = None, errors: Any = None, exception: Any = None, cooldown_remaining: float | None = None) -> None:
    try:
        payload: Dict[str, Any] = {
            "utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "status": str(status or "unknown"),
            "result_type": type(result).__name__ if result is not None else None,
            "result_is_dict": isinstance(result, dict),
            "result_keys": sorted(list(result.keys()))[:100] if isinstance(result, dict) else [],
            "result_size": len(result) if isinstance(result, dict) else None,
            "sample": {},
            "errors": list(errors or []),
            "exception": str(exception) if exception else None,
            "cooldown_remaining": float(cooldown_remaining or 0.0),
        }
        if isinstance(result, dict):
            sample = {}
            for k in sorted(result.keys())[:20]:
                try:
                    v = result.get(k)
                    if isinstance(v, (str, int, float, bool)) or v is None:
                        sample[str(k)] = v
                    else:
                        sample[str(k)] = str(v)
                except Exception:
                    continue
            payload["sample"] = sample
            # bootstrap-specific hints
            usd_keys = [k for k in result.keys() if str(k).upper() in ("USD","ZUSD","USDT","USDC")]
            payload["usd_keys"] = usd_keys
            try:
                payload["usd_values"] = {str(k): result.get(k) for k in usd_keys}
            except Exception:
                payload["usd_values"] = {}
        _write_balance_bootstrap_event(payload)
    except Exception:
        return

def read_balance_bootstrap_event() -> Dict[str, Any]:
    try:
        if not _BALANCE_BOOTSTRAP_PATH.exists():
            return {}
        return json.loads(_BALANCE_BOOTSTRAP_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}

_NONCE_LOCK = threading.Lock()
_LAST_NONCE = 0

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
# Balance helpers (spot-safe preflight)
# ---------------------------------------------------------------------------
_ASSET_CODE_OVERRIDES = {
    "BTC": "XXBT",
    "XBT": "XXBT",
    "ETH": "XETH",
    "USD": "ZUSD",
    "USDT": "USDT",
    "USDC": "USDC",
}

# Balance cache (seconds) to prevent repeated private Balance calls within a single tick
_BAL_CACHE: Dict[str, Any] = {"ts": 0.0, "bal": {}}
_OPEN_ORDERS_CACHE: Dict[str, Any] = {"ts": 0.0, "open": {}}
_BAL_FETCH_LOCK = threading.Lock()
_OPEN_ORDERS_FETCH_LOCK = threading.Lock()
_PRIVATE_COOLDOWN_UNTIL: Dict[str, float] = {"Balance": 0.0, "OpenOrders": 0.0}
_PRIVATE_COOLDOWN_REASON: Dict[str, str] = {"Balance": "", "OpenOrders": ""}

def _private_endpoint_cooldown_remaining(path: str) -> float:
    try:
        return max(0.0, float(_PRIVATE_COOLDOWN_UNTIL.get(str(path or ""), 0.0) or 0.0) - time.time())
    except Exception:
        return 0.0

def _set_private_endpoint_cooldown(path: str, *, reason: str, seconds: float) -> None:
    p = str(path or "")
    if not p:
        return
    try:
        sec = max(0.0, float(seconds or 0.0))
    except Exception:
        sec = 0.0
    until = time.time() + sec if sec > 0 else 0.0
    _PRIVATE_COOLDOWN_UNTIL[p] = until
    _PRIVATE_COOLDOWN_REASON[p] = str(reason or "")

def _clear_private_endpoint_cooldown(path: str) -> None:
    p = str(path or "")
    if not p:
        return
    _PRIVATE_COOLDOWN_UNTIL[p] = 0.0
    _PRIVATE_COOLDOWN_REASON[p] = ""

def _private_endpoint_cooldown_reason(path: str) -> str:
    return str(_PRIVATE_COOLDOWN_REASON.get(str(path or ""), "") or "")

def _private_endpoint_cooldown_seconds(path: str) -> float:
    env_key = f"KRAKEN_{str(path or "").upper()}_COOLDOWN_SEC"
    try:
        val = float(os.getenv(env_key, "") or 0.0)
    except Exception:
        val = 0.0
    if val > 0:
        return val
    try:
        return float(os.getenv("KRAKEN_PRIVATE_COOLDOWN_SEC", "15") or 15.0)
    except Exception:
        return 15.0

def _asset_code_candidates(ui_asset: str) -> list:
    a = (ui_asset or "").upper().strip()
    cands = []
    if not a:
        return cands
    if a in _ASSET_CODE_OVERRIDES:
        cands.append(_ASSET_CODE_OVERRIDES[a])
    # common patterns in Kraken balances
    cands.extend([a, f"X{a}", f"Z{a}", f"XX{a}", f"ZZ{a}"])
    # de-dupe, preserve order
    out = []
    seen = set()
    for x in cands:
        if x and x not in seen:
            out.append(x); seen.add(x)
    return out

def _get_balance_float(bal: dict, ui_asset: str) -> float:
    for k in _asset_code_candidates(ui_asset):
        if k in bal:
            try:
                return float(bal[k])
            except Exception:
                continue
    return 0.0


def _fetch_balances() -> dict:
    """Fetch Kraken balances (private Balance), with cache, singleflight, and grace."""
    ttl_env = os.getenv("KRAKEN_BALANCE_TTL_SEC", os.getenv("BALANCE_CACHE_TTL_SEC", "3.0"))
    grace_env = os.getenv("KRAKEN_BALANCE_STALE_GRACE_SEC", os.getenv("BALANCE_STALE_GRACE_SEC", "900"))
    try:
        ttl = float(ttl_env or 3.0)
    except Exception:
        ttl = 3.0
    try:
        stale_grace = float(grace_env or 900.0)
    except Exception:
        stale_grace = 900.0

    now = time.time()
    try:
        ts = float(_BAL_CACHE.get("ts") or 0.0)
    except Exception:
        ts = 0.0
    cached_bal = _BAL_CACHE.get("bal") if isinstance(_BAL_CACHE.get("bal"), dict) else {}

    if ttl > 0 and (now - ts) < ttl and isinstance(cached_bal, dict):
        return cached_bal  # type: ignore[return-value]

    with _BAL_FETCH_LOCK:
        now = time.time()
        try:
            ts = float(_BAL_CACHE.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        cached_bal = _BAL_CACHE.get("bal") if isinstance(_BAL_CACHE.get("bal"), dict) else {}
        if ttl > 0 and (now - ts) < ttl and isinstance(cached_bal, dict):
            return cached_bal  # type: ignore[return-value]

        cooldown_remaining = _private_endpoint_cooldown_remaining("Balance")
        if cooldown_remaining > 0 and isinstance(cached_bal, dict) and cached_bal and (now - ts) <= stale_grace:
            return cached_bal  # type: ignore[return-value]
        if cooldown_remaining > 0:
            try:
                _record_balance_bootstrap_event(status="cooldown_blocked", result=cached_bal if isinstance(cached_bal, dict) else {}, cooldown_remaining=cooldown_remaining)
            except Exception:
                pass

        try:
            bal = _priv("Balance", {}) or {}
            if not isinstance(bal, dict):
                bal = {}
            _BAL_CACHE["ts"] = time.time()
            _BAL_CACHE["bal"] = bal
            _clear_private_endpoint_cooldown("Balance")
            return bal
        except Exception as e:
            try:
                _record_balance_bootstrap_event(status="exception", exception=e, cooldown_remaining=_private_endpoint_cooldown_remaining("Balance"))
            except Exception:
                pass
            if isinstance(cached_bal, dict) and cached_bal and (now - ts) <= stale_grace:
                return cached_bal  # type: ignore[return-value]
            raise


# ---------------------------------------------------------------------------
# Pair metadata (ordermin / lot_decimals) for minimum-volume + precision guards
# ---------------------------------------------------------------------------
_PAIR_META_LOCK = threading.Lock()
_PAIR_META_CACHE: Dict[str, Dict[str, Any]] = {}  # pair -> {"ts": float, "ordermin": float, "lot_decimals": int}

def _pair_meta(pair: str) -> Dict[str, Any]:
    """Return Kraken pair metadata for order sizing guards.

    Uses public AssetPairs and caches results. Values:
      - ordermin: minimum base volume for orders
      - lot_decimals: max decimals allowed for volume
    """
    key = str(pair or "").strip().upper()
    if not key:
        return {"ordermin": 0.0, "lot_decimals": 8, "pair_decimals": 8}

    ttl = float(os.getenv("KRAKEN_PAIR_META_TTL_SEC", "3600") or 3600)
    now = time.time()

    with _PAIR_META_LOCK:
        cached = _PAIR_META_CACHE.get(key)
        if cached and ttl > 0:
            try:
                if (now - float(cached.get("ts") or 0.0)) < ttl:
                    return {"ordermin": float(cached.get("ordermin") or 0.0),
                            "lot_decimals": int(cached.get("lot_decimals") or 8),
                            "pair_decimals": int(cached.get("pair_decimals") or 8)}
            except Exception:
                pass

    try:
        res = _pub("AssetPairs", {"pair": key}) or {}
    except Exception:
        res = {}

    ordermin = 0.0
    lot_decimals = 8
    pair_decimals = 8
    if isinstance(res, dict) and res:
        try:
            v = next(iter(res.values()))
            if isinstance(v, dict):
                ordermin = float(v.get("ordermin") or 0.0)
                lot_decimals = int(v.get("lot_decimals") or 8)
                pair_decimals = int(v.get("pair_decimals") or 8)
        except Exception:
            pass

    meta = {"ts": now, "ordermin": float(ordermin), "lot_decimals": int(lot_decimals), "pair_decimals": int(pair_decimals)}
    with _PAIR_META_LOCK:
        _PAIR_META_CACHE[key] = meta

    return {"ordermin": meta["ordermin"], "lot_decimals": meta["lot_decimals"], "pair_decimals": meta["pair_decimals"]}

def _truncate_to_decimals(x: float, decimals: int) -> float:
    """Truncate (not round) to a fixed number of decimals."""
    try:
        d = max(0, int(decimals))
    except Exception:
        d = 0
    if not math.isfinite(float(x)):
        return 0.0
    if d <= 0:
        return float(int(x))
    factor = 10 ** d
    return math.floor(float(x) * factor) / factor

def _format_volume(x: float, decimals: int) -> str:
    try:
        d = max(0, int(decimals))
    except Exception:
        d = 8
    return f"{float(x):.{d}f}"


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

def _cooldown_check(strategy: str, kraken_pair: str, side: str) -> None:
    """Raise if an order should be blocked by cooldown/min-hold.

    IMPORTANT: This does *not* latch. We only latch after a successful AddOrder,
    so failed preflights (balance/min-volume/etc.) do not poison the cooldown gate.
    """
    same_side = _cooldown_seconds('SCHED_COOLDOWN_SAME_SIDE_SECONDS', 0)
    flip_side = _cooldown_seconds('SCHED_COOLDOWN_FLIP_SECONDS', 0)
    min_hold  = _cooldown_seconds('SCHED_MIN_HOLD_SECONDS', 0)
    if same_side <= 0 and flip_side <= 0 and min_hold <= 0:
        return

    now = time.time()
    key = str(kraken_pair or "").upper()

    with _ORDER_LATCH_LOCK:
        prev = _ORDER_LATCH.get(key)
        if prev is None:
            return

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


def _cooldown_latch(strategy: str, kraken_pair: str, side: str) -> None:
    """Latch a successful order for cooldown checks."""
    key = str(kraken_pair or "").upper()
    if not key:
        return
    now = time.time()
    with _ORDER_LATCH_LOCK:
        _ORDER_LATCH[key] = {'side': side, 'ts': now, 'strategy': strategy}

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
API_BASE = os.getenv("KRAKEN_BASE", "https://api.kraken.com")

# Backward-compatible alias.
# Some parts of the codebase (and older deployments) reference KRAKEN_API_URL.
# Keep this defined so private/public requests don't crash with NameError.
KRAKEN_API_URL = API_BASE
TIMEOUT = float(os.getenv("KRAKEN_TIMEOUT", "10"))        # seconds
# Some code paths reference KRAKEN_TIMEOUT_SEC; keep a compatible alias.
KRAKEN_TIMEOUT_SEC = float(os.getenv("KRAKEN_TIMEOUT_SEC", str(TIMEOUT)))
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
    if not api_key or not api_secret:
        raise RuntimeError("Missing Kraken API credentials (KRAKEN_API_KEY/KRAKEN_API_SECRET)")
    postdata = "&".join(f"{k}={data[k]}" for k in data)
    message = (str(data["nonce"]) + postdata).encode()
    sha256 = hashlib.sha256(message).digest()
    mac = hmac.new(base64.b64decode(api_secret), urlpath.encode() + sha256, hashlib.sha512)
    sig = base64.b64encode(mac.digest()).decode()
    return {"API-Key": api_key, "API-Sign": sig}


def _headers(path: str, data: Dict[str, Any]) -> Dict[str, str]:
    """Return Kraken private REST auth headers."""
    urlpath = f"/0/private/{path}"
    return _sign(urlpath, data)

def _priv(path: str, data: dict | None = None) -> dict:
    """Kraken private REST call with monotonic nonce + light retries.

    Kraken requires strictly increasing nonces per API key. Under concurrency,
    time-based nonces can collide causing EAPI:Invalid nonce.
    Kraken can also return EAPI:Rate limit exceeded if we poll too aggressively.
    """
    data = dict(data or {})
    url = f"{KRAKEN_API_URL}/0/private/{path}"

    managed_path = str(path or "")
    if managed_path in ("Balance", "OpenOrders"):
        remaining = _private_endpoint_cooldown_remaining(managed_path)
        if remaining > 0:
            reason = _private_endpoint_cooldown_reason(managed_path)
            raise RuntimeError(f"Kraken private call cooldown active: {managed_path} remaining={round(remaining, 3)}s reason={reason}")

    # retry a handful of times for nonce/rate limit errors
    last_err: Exception | None = None
    for attempt in range(1, 6):
        try:
            # Monotonic nonce (per-process)
            with _NONCE_LOCK:
                global _LAST_NONCE
                n = int(time.time() * 1000)
                if n <= _LAST_NONCE:
                    n = _LAST_NONCE + 1
                _LAST_NONCE = n
            data["nonce"] = str(n)

            headers = _headers(path, data)
            r = requests.post(url, data=data, headers=headers, timeout=KRAKEN_TIMEOUT_SEC)
            payload = r.json()

            errors = payload.get("error") or []
            if errors:
                # Log full error list; keep request params minimal to avoid leaking secrets
                safe_keys = {k: data.get(k) for k in ("pair","type","ordertype","volume","price","price2","leverage","oflags","timeinforce") if k in data}
                logger.error("kraken._priv ERROR: path=%s status=%s errors=%s req=%s", path, r.status_code, errors, safe_keys)

                # Retry specific transient errors
                transient = any(("Rate limit exceeded" in e) or ("Invalid nonce" in e) for e in errors)
                if transient and managed_path in ("Balance", "OpenOrders"):
                    _set_private_endpoint_cooldown(
                        managed_path,
                        reason="; ".join([str(e) for e in errors if e]),
                        seconds=_private_endpoint_cooldown_seconds(managed_path),
                    )
                if managed_path == "Balance":
                    try:
                        _record_balance_bootstrap_event(
                            status="error",
                            result=payload.get("result"),
                            errors=errors,
                            cooldown_remaining=_private_endpoint_cooldown_remaining("Balance"),
                        )
                    except Exception:
                        pass
                if transient and attempt < 5:
                    # Exponential-ish backoff with jitter
                    sleep_s = min(8.0, 0.4 * (2 ** (attempt - 1))) + (0.05 * attempt)
                    time.sleep(sleep_s)
                    continue

                raise RuntimeError(f"Kraken private call failed: {path} errors={errors}")

            result = payload.get("result") or {}

            if managed_path == "Balance":
                try:
                    _record_balance_bootstrap_event(status="success", result=result, cooldown_remaining=0.0)
                except Exception:
                    pass

            if managed_path in ("Balance", "OpenOrders"):
                _clear_private_endpoint_cooldown(managed_path)

            if path == "AddOrder":
                txid = None
                try:
                    txid = result.get("txid")
                except Exception:
                    txid = None
                logger.info("kraken AddOrder ok: txid=%s pair=%s type=%s ordertype=%s vol=%s",
                            txid, data.get("pair"), data.get("type"), data.get("ordertype"), data.get("volume"))

            return result

        except Exception as e:
            last_err = e
            # For network/JSON parse issues, brief retry
            if attempt < 5:
                time.sleep(min(2.0, 0.3 * attempt))
                continue
            raise

    # unreachable
    raise last_err  # type: ignore

def close_notional_for_qty(symbol: str, qty: float) -> float:
    """
    Helper for position-aware scheduler: compute notional needed to close `qty` at current price.
    """
    px = last_price(symbol)
    if px <= 0:
        raise RuntimeError(f"last_price for {symbol} is <= 0")
    return abs(qty) * px


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
    Convert Kraken key (e.g., 'XXBTZUSD' or 'XETHZUSD' or 'XBTUSD')
    to UI 'BTC/USD', 'ETH/USD', etc.

    IMPORTANT: we normalize to the slash style the rest of the system uses.
    """
    K = k.upper()
    for q in _KNOWN_QUOTES:
        # Handle ...ZUSD, ...ZEUR, etc. first
        if K.endswith("Z" + q):
            base = K[: -(len(q) + 1)]
            return f"{_normalize_base(base)}/{_normalize_quote('Z' + q)}"
        if K.endswith(q):
            base = K[: -len(q)]
            return f"{_normalize_base(base)}/{_normalize_quote(q)}"
    try:
        # Fall back to symbol_map.from_kraken, but make sure we return slash style
        ui = from_kraken(k)
        if isinstance(ui, str) and "/" in ui:
            return ui.upper()
        if isinstance(ui, str):
            # If something like 'BTCUSD' came back, insert a slash before quote
            for q in _KNOWN_QUOTES:
                if ui.upper().endswith(q):
                    b = ui[:-len(q)]
                    return f"{b.upper()}/{q}"
        return None
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


def _best_bid_ask(pair: str) -> tuple[float | None, float | None]:
    """Best-effort best bid/ask for a Kraken pair via public Depth."""
    try:
        ob = _pub("Depth", {"pair": str(pair), "count": 1}) or {}
    except Exception:
        ob = {}
    if not isinstance(ob, dict) or not ob:
        return None, None
    try:
        book = next(iter(ob.values()))
    except Exception:
        return None, None
    if not isinstance(book, dict):
        return None, None
    bid = None
    ask = None
    try:
        bids = book.get("bids") or []
        if bids and isinstance(bids[0], (list, tuple)) and bids[0]:
            bid = float(bids[0][0])
    except Exception:
        bid = None
    try:
        asks = book.get("asks") or []
        if asks and isinstance(asks[0], (list, tuple)) and asks[0]:
            ask = float(asks[0][0])
    except Exception:
        ask = None
    return bid, ask


def _query_order(txid: str) -> dict:
    """Query a single Kraken order (best-effort)."""
    if not txid:
        return {}
    try:
        res = _priv("QueryOrders", {"txid": str(txid)}) or {}
    except Exception:
        return {}
    if not isinstance(res, dict):
        return {}
    # res is mapping txid -> order
    try:
        return res.get(str(txid)) or next(iter(res.values()))
    except Exception:
        return {}




def _query_trades(txids: list[str]) -> dict:
    """Return QueryTrades rows keyed by txid (best-effort)."""
    ids = [str(x).strip() for x in (txids or []) if str(x).strip()]
    if not ids:
        return {}
    try:
        res = _priv("QueryTrades", {"txid": ",".join(ids)}) or {}
        return res if isinstance(res, dict) else {}
    except Exception:
        return {}


def _reconcile_order_fill(txid: str, *, timeout_sec: int = 8) -> dict:
    """Best-effort reconciliation for an order using QueryOrders + QueryTrades.

    Returns a dict with status, vol_exec, fee, cost, avg_price, trade_ids, filled.
    """
    deadline = time.time() + max(1, int(timeout_sec))
    last_status = None
    last_row: dict[str, Any] = {}
    trade_ids: list[str] = []
    while time.time() <= deadline:
        od = _query_order(str(txid)) or {}
        last_row = od if isinstance(od, dict) else {}
        last_status = str(last_row.get("status") or "").lower()
        trade_ids = [str(t) for t in (last_row.get("trades") or []) if str(t)]
        try:
            vol_exec = float(last_row.get("vol_exec") or 0.0)
        except Exception:
            vol_exec = 0.0
        if last_status in ("closed", "canceled", "cancelled", "expired") or trade_ids or vol_exec > 0:
            break
        time.sleep(1.0)

    vol_exec = 0.0
    fee = 0.0
    cost = 0.0
    avg_price = None
    filled_ts = None
    trades = _query_trades(trade_ids) if trade_ids else {}
    if trades:
        for _, t in trades.items():
            try:
                vol_exec += float(t.get("vol") or 0.0)
            except Exception:
                pass
            try:
                fee += float(t.get("fee") or 0.0)
            except Exception:
                pass
            try:
                cost += float(t.get("cost") or 0.0)
            except Exception:
                pass
            try:
                t_ts = float(t.get("time")) if t.get("time") is not None else None
            except Exception:
                t_ts = None
            if t_ts is not None:
                filled_ts = max(float(filled_ts or 0.0), float(t_ts))
        if vol_exec > 0:
            avg_price = cost / vol_exec
    else:
        try:
            vol_exec = float(last_row.get("vol_exec") or 0.0)
        except Exception:
            vol_exec = 0.0
        descr_price = None
        try:
            descr_price = float(last_row.get("price") or 0.0)
        except Exception:
            descr_price = None
        avg_price = descr_price if descr_price and descr_price > 0 else None
        try:
            fee = float(last_row.get("fee") or 0.0)
        except Exception:
            fee = 0.0
        try:
            cost = float(last_row.get("cost") or 0.0)
        except Exception:
            cost = 0.0

    return {
        "status": last_status or None,
        "vol_exec": float(vol_exec),
        "fee": float(fee),
        "cost": float(cost),
        "avg_price": float(avg_price) if avg_price is not None else None,
        "trade_ids": trade_ids,
        "filled_ts": filled_ts,
        "filled": bool((last_status == "closed") or (float(vol_exec) > 0.0)),
    }


def stop_limit_notional(
    symbol: str,
    *,
    notional: float,
    stop_price: float,
    strategy: str,
    limit_buffer_pct: float | None = None,
    timeout_sec: int | None = None,
    market_fallback: bool | None = None,
) -> Dict[str, Any]:
    """Submit a stop-loss-limit SELL for spot holdings, then reconcile / fallback."""
    ui = symbol.upper()
    pair = to_kraken(ui)
    strat_tag = (strategy or '').strip()
    if not strat_tag:
        raise ValueError('missing strategy tag for order (strategy is required)')

    _cooldown_check(strat_tag, pair, 'sell')

    trigger_px = float(stop_price or 0.0)
    if trigger_px <= 0:
        return {"ok": False, "error": "stop_limit_notional failed: invalid stop_price"}

    try:
        bal = _fetch_balances()
    except Exception as e:
        return {"ok": False, "error": f"stop_limit_notional failed: balance fetch failed: {e}"}

    base = (ui.split('/', 1)[0] if '/' in ui else ui).strip().upper()
    avail = float(_get_balance_float(bal, base) or 0.0)
    if avail <= 0:
        return {"ok": False, "error": f"stop_limit_notional failed: insufficient {base} balance (avail={avail})"}

    px_for_size = _ensure_price(ui)
    volume = min(float(notional) / float(px_for_size), float(avail))

    meta = _pair_meta(pair)
    ordermin = float(meta.get("ordermin") or 0.0)
    lot_decimals = int(meta.get("lot_decimals") or 8)
    pair_decimals = int(meta.get("pair_decimals") or 8)

    volume = _truncate_to_decimals(float(volume), lot_decimals)
    if volume <= 0:
        return {"ok": False, "error": "stop_limit_notional failed: volume <= 0 after precision truncation"}
    if ordermin > 0.0 and volume < ordermin:
        if avail >= ordermin:
            volume = _truncate_to_decimals(float(ordermin), lot_decimals)
        else:
            return {"ok": False, "error": f"stop_limit_notional failed: dust_below_min_volume:{volume}<{ordermin} (pair={pair}) (avail={avail})"}

    buff = float(limit_buffer_pct) if limit_buffer_pct is not None else float(os.getenv("STOP_LIMIT_BUFFER_PCT", "0.01") or 0.01)
    wait_sec = int(timeout_sec) if timeout_sec is not None else int(float(os.getenv("STOP_LIMIT_TIMEOUT_SEC", "60") or 60))
    fallback = bool(market_fallback) if market_fallback is not None else (os.getenv("MARKET_FALLBACK", "1").strip().lower() in ("1","true","yes","on"))

    limit_px = max(0.0, float(trigger_px) * (1.0 - max(0.0, float(buff))))
    if limit_px <= 0:
        return {"ok": False, "error": "stop_limit_notional failed: invalid limit price"}

    payload = {
        "pair": pair,
        "type": "sell",
        "ordertype": "stop-loss-limit",
        "price": _format_price(float(trigger_px), pair_decimals),
        "price2": _format_price(float(limit_px), pair_decimals),
        "volume": _format_volume(volume, lot_decimals),
        "userref": str(_userref_for_strategy(strategy)),
    }
    try:
        res = _priv("AddOrder", payload)
    except Exception as e:
        return {"ok": False, "error": f"stop_limit_notional failed: {e}"}

    txid = None
    descr = None
    try:
        txid = (res.get("txid") or [None])[0]
        descr = (res.get("descr") or {}).get("order")
    except Exception:
        pass

    recon = _reconcile_order_fill(str(txid), timeout_sec=max(2, wait_sec)) if txid else {}
    remaining_volume = max(0.0, float(volume) - float(recon.get("vol_exec") or 0.0))
    remaining_notional = remaining_volume * float(px_for_size)

    status = str(recon.get("status") or "").lower()
    if status != "closed" and txid:
        try:
            _priv("CancelOrder", {"txid": str(txid)})
        except Exception:
            pass

    if float(recon.get("vol_exec") or 0.0) > 0.0 and remaining_volume <= 0.0:
        _cooldown_latch(strat_tag, pair, 'sell')
        return {
            "ok": True,
            "filled": True,
            "execution": "stop_loss_limit",
            "pair": pair,
            "side": "sell",
            "notional": float(notional),
            "volume": float(volume),
            "trigger_price": float(trigger_px),
            "limit_price": float(limit_px),
            "txid": txid,
            "descr": descr,
            "reconciled": recon,
            "result": res,
        }

    if fallback and remaining_volume > 0.0:
        mkt = market_notional(
            symbol=symbol,
            side='sell',
            notional=max(remaining_notional, 0.0),
            price=px_for_size,
            strategy=strategy,
        )
        if mkt.get("ok"):
            try:
                mkt["stop_limit_first"] = {
                    "txid": txid,
                    "trigger_price": float(trigger_px),
                    "limit_price": float(limit_px),
                    "reconciled": recon,
                    "partial_volume": float(recon.get("vol_exec") or 0.0),
                    "remaining_volume": float(remaining_volume),
                }
            except Exception:
                pass
        return mkt

    return {
        "ok": bool(float(recon.get("vol_exec") or 0.0) > 0.0),
        "filled": bool(float(recon.get("vol_exec") or 0.0) > 0.0),
        "execution": "stop_loss_limit",
        "pair": pair,
        "side": "sell",
        "notional": float(notional),
        "volume": float(volume),
        "trigger_price": float(trigger_px),
        "limit_price": float(limit_px),
        "txid": txid,
        "descr": descr,
        "reconciled": recon,
        "result": res,
        "error": None if float(recon.get("vol_exec") or 0.0) > 0.0 else "stop_limit_not_filled",
    }

def market_notional(
    symbol: str,
    side: str,
    notional: float,
    price: Optional[float] = None,
    strategy: Optional[str] = None,
    exec_mode_override: Optional[str] = None,
    post_offset_override: Optional[float] = None,
    chase_sec_override: Optional[int] = None,
    chase_steps_override: Optional[int] = None,
    market_fallback_override: Optional[bool] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """
    Market order by USD notional:
      volume(base) = notional(quote USD) / price

    Accepts an optional `price` (so callers like br_router can reuse a
    resolved price) and an optional `strategy` (used to derive a stable
    Kraken userref for correlation / journaling).
    """
    side = side.lower().strip()
    if side not in ("buy", "sell"):
        raise ValueError("side must be 'buy' or 'sell'")

    # Normalize & build kraken pair first
    ui = symbol.upper()
    pair = to_kraken(ui)  # must match what Kraken sees (e.g., SUIUSD / XBTUSD)

    # Strategy tag is required for attribution/logging
    strat_tag = (strategy or '').strip()
    if not strat_tag:
        raise ValueError('missing strategy tag for order (strategy is required)')

    # Authoritative cooldown/min-hold gateway (keyed on KRAKEN pair)
    _cooldown_check(strat_tag, pair, side)

    # Use caller-supplied price if valid; otherwise fall back to last_price.
    if isinstance(price, (int, float)) and math.isfinite(float(price)) and float(price) > 0:
        px = float(price)
    else:
        px = _ensure_price(ui)
    # Compute base volume from USD notional.
    volume = float(notional) / px

    avail_for_sell: Optional[float] = None  # only set for spot SELL preflight

    # Spot-safe preflight: cap sell volume to available base balance so we don't hit
    # EOrder:Insufficient funds when our paper position size differs from exchange.
    # Note: we only cap SELL on spot. For BUY (spending quote), we leave as-is for now.
    if side.lower() == "sell":
        try:
            bal = _fetch_balances()
        except Exception as e:
            return {"ok": False, "error": f"market_notional failed: balance fetch failed: {e}"}

        base = (ui.split('/', 1)[0] if '/' in ui else ui).strip().upper()
        avail = float(_get_balance_float(bal, base) or 0.0)
        avail_for_sell = avail
        # If we don't have the asset, nothing to sell.
        if avail <= 0:
            return {"ok": False, "error": f"market_notional failed: insufficient {base} balance (avail={avail})"}

        # volume is in BASE units for Kraken spot orders
        volume = min(volume, avail)
        # If the cap reduces volume to ~0, bail.
        if float(volume) <= 0:
            return {"ok": False, "error": f"market_notional failed: sell volume <= 0 after balance cap (avail={avail})"}

    # Enforce Kraken per-pair minimum volume + precision (prevents AddOrder volume minimum errors).
    meta = _pair_meta(pair)
    ordermin = float(meta.get("ordermin") or 0.0)
    lot_decimals = int(meta.get("lot_decimals") or 8)
    pair_decimals = int(meta.get("pair_decimals") or 8)

    # Truncate to allowed decimals (Kraken rejects or rounds unexpectedly otherwise).
    volume = _truncate_to_decimals(float(volume), lot_decimals)

    if volume <= 0.0:
        return {"ok": False, "error": "market_notional failed: volume <= 0 after precision truncation"}

    if ordermin > 0.0 and float(volume) < ordermin:
        # SELL handling:
        # - If we *do* have enough available base to meet Kraken minimum, bump to minimum.
        # - If we *do not* have enough (true dust), return a distinct error for callers to quarantine.
        if side.lower() == "sell" and avail_for_sell is not None:
            if float(avail_for_sell) >= ordermin:
                volume = _truncate_to_decimals(float(ordermin), lot_decimals)
            else:
                return {"ok": False, "error": f"market_notional failed: dust_below_min_volume:{volume}<{ordermin} (pair={pair}) (avail={avail_for_sell})"}
        else:
            return {"ok": False, "error": f"market_notional failed: below_min_volume:{volume}<{ordermin} (pair={pair})"}

    
    exec_mode = (str(exec_mode_override).strip().lower() if exec_mode_override is not None else (os.getenv("EXECUTION_MODE", "market") or "market").strip().lower())
    post_offset = float(post_offset_override) if post_offset_override is not None else float(os.getenv("POST_ONLY_OFFSET_PCT", "0.0002") or 0.0002)
    aggressive_cross_pct = float(os.getenv("LIMIT_AGGRESSIVE_CROSS_PCT", "0.0005") or 0.0005)
    aggressive_reconcile_sec = int(float(os.getenv("LIMIT_AGGRESSIVE_RECONCILE_SEC", "3") or 3))
    aggressive_use_ioc = (os.getenv("LIMIT_AGGRESSIVE_USE_IOC", "1").strip().lower() in ("1", "true", "yes", "on"))
    chase_sec = int(chase_sec_override) if chase_sec_override is not None else int(float(os.getenv("LIMIT_CHASE_SEC", "10") or 10))
    chase_steps = int(chase_steps_override) if chase_steps_override is not None else int(float(os.getenv("LIMIT_CHASE_STEPS", "1") or 1))
    if market_fallback_override is not None:
        market_fallback = bool(market_fallback_override)
    else:
        market_fallback = (os.getenv("MARKET_FALLBACK", "1").strip().lower() in ("1","true","yes","on"))
    # Guardrail: maker_first without market fallback must allow time/steps for a maker fill.
    if exec_mode == "maker_first" and not market_fallback:
        min_wait = int(float(os.getenv("MAKER_MIN_WAIT_SEC", "5") or 5))
        if chase_sec < min_wait:
            chase_sec = min_wait
        if chase_steps < 1:
            chase_steps = 1


    def _place_market(*, volume_override: Optional[float] = None, notional_override: Optional[float] = None) -> Dict[str, Any]:
        volume_to_send = float(volume_override) if volume_override is not None else float(volume)
        notional_to_report = float(notional_override) if notional_override is not None else float(notional)
        if volume_to_send <= 0:
            return {"ok": False, "error": "market_notional failed: remaining volume <= 0 for market fallback"}
        payload_mkt = {
            "pair": pair,
            "type": "buy" if side == "buy" else "sell",
            "ordertype": "market",
            "volume": _format_volume(volume_to_send, lot_decimals),
            "userref": str(_userref_for_strategy(strategy) if strategy is not None else _userref(ui, side, float(notional_to_report))),
        }
        try:
            res_m = _priv("AddOrder", payload_mkt)
        except Exception as e:
            return {"ok": False, "error": f"market_notional failed: {e}"}

        txid_m = None
        descr_m = None
        try:
            txid_m = (res_m.get("txid") or [None])[0]
            descr_m = (res_m.get("descr") or {}).get("order")
        except Exception:
            pass

        recon = _reconcile_order_fill(str(txid_m), timeout_sec=int(float(os.getenv("ORDER_RECONCILE_TIMEOUT_SEC", "8") or 8))) if txid_m else {}
        if bool(recon.get("filled")):
            _cooldown_latch(strat_tag, pair, side)
        return {
            "ok": bool(recon.get("filled")) if txid_m else True,
            "filled": bool(recon.get("filled")) if txid_m else True,
            "execution": "market",
            "pair": pair,
            "side": side,
            "notional": float(notional_to_report),
            "volume": float(volume_to_send),
            "txid": txid_m,
            "descr": descr_m,
            "reconciled": recon,
            "result": res_m,
            "error": None if (bool(recon.get("filled")) if txid_m else True) else "market_order_not_filled",
        }

    def _place_post_only_once(limit_px: float) -> Dict[str, Any]:
        payload_lim = {
            "pair": pair,
            "type": "buy" if side == "buy" else "sell",
            "ordertype": "limit",
            "price": _format_price(float(limit_px), pair_decimals),
            "volume": _format_volume(volume, lot_decimals),
            "oflags": "post",
            "userref": str(_userref_for_strategy(strategy) if strategy is not None else _userref(ui, side, float(notional))),
        }
        try:
            res_l = _priv("AddOrder", payload_lim)
        except Exception as e:
            return {"ok": False, "error": f"maker_first failed: {e}"}

        txid_l = None
        descr_l = None
        try:
            txid_l = (res_l.get("txid") or [None])[0]
            descr_l = (res_l.get("descr") or {}).get("order")
        except Exception:
            pass

        # Wait for fill
        start = time.time()
        filled = False
        status = None
        vol_exec = 0.0
        while txid_l and (time.time() - start) < max(1, chase_sec):
            od = _query_order(str(txid_l)) or {}
            status = od.get("status")
            try:
                vol_exec = float(od.get("vol_exec") or 0.0)
            except Exception:
                vol_exec = 0.0
            if status == "closed" and vol_exec > 0:
                filled = True
                break
            time.sleep(1.0)

        if filled:
            recon = _reconcile_order_fill(str(txid_l), timeout_sec=int(float(os.getenv("ORDER_RECONCILE_TIMEOUT_SEC", "8") or 8))) if txid_l else {}
            _cooldown_latch(strat_tag, pair, side)
            return {
                "ok": True,
                "filled": True,
                "execution": "post_only_limit",
                "pair": pair,
                "side": side,
                "notional": float(notional),
                "volume": float(volume),
                "limit_price": float(limit_px),
                "txid": txid_l,
                "descr": descr_l,
                "status": status,
                "vol_exec": vol_exec,
                "reconciled": recon,
                "result": res_l,
            }

        # Not filled within chase_sec: cancel the order.
        try:
            _priv("CancelOrder", {"txid": str(txid_l)})
        except Exception:
            pass

        return {
            "ok": False,
            "filled": False,
            "execution": "post_only_limit",
            "pair": pair,
            "side": side,
            "notional": float(notional),
            "volume": float(volume),
            "limit_price": float(limit_px),
            "txid": txid_l,
            "descr": descr_l,
            "status": status,
            "vol_exec": vol_exec,
            "error": "maker_not_filled",
        }

    def _place_aggressive_limit_once(limit_px: float) -> Dict[str, Any]:
        payload_lim = {
            "pair": pair,
            "type": "buy" if side == "buy" else "sell",
            "ordertype": "limit",
            "price": _format_price(float(limit_px), pair_decimals),
            "volume": _format_volume(volume, lot_decimals),
            "userref": str(_userref_for_strategy(strategy) if strategy is not None else _userref(ui, side, float(notional))),
        }
        if aggressive_use_ioc:
            payload_lim["timeinforce"] = "IOC"
        try:
            res_l = _priv("AddOrder", payload_lim)
        except Exception as e:
            return {"ok": False, "error": f"limit_aggressive failed: {e}"}

        txid_l = None
        descr_l = None
        try:
            txid_l = (res_l.get("txid") or [None])[0]
            descr_l = (res_l.get("descr") or {}).get("order")
        except Exception:
            pass

        recon = _reconcile_order_fill(str(txid_l), timeout_sec=max(1, aggressive_reconcile_sec)) if txid_l else {}
        status = str(recon.get("status") or "").lower() if txid_l else None
        try:
            vol_exec = float(recon.get("vol_exec") or 0.0)
        except Exception:
            vol_exec = 0.0
        filled = bool(recon.get("filled")) or (status == "closed" and vol_exec > 0)
        if filled:
            _cooldown_latch(strat_tag, pair, side)
            return {
                "ok": True,
                "filled": True,
                "execution": "limit_aggressive",
                "pair": pair,
                "side": side,
                "notional": float(notional),
                "volume": float(volume),
                "limit_price": float(limit_px),
                "txid": txid_l,
                "descr": descr_l,
                "status": status,
                "vol_exec": vol_exec,
                "reconciled": recon,
                "result": res_l,
            }
        return {
            "ok": False,
            "filled": False,
            "execution": "limit_aggressive",
            "pair": pair,
            "side": side,
            "notional": float(notional),
            "volume": float(volume),
            "limit_price": float(limit_px),
            "txid": txid_l,
            "descr": descr_l,
            "status": status,
            "vol_exec": vol_exec,
            "reconciled": recon,
            "result": res_l,
            "error": "aggressive_limit_not_filled",
        }

    # Aggressive limit execution: cross the spread with a marketable IOC limit, then fallback to market.
    if exec_mode == "limit_aggressive":
        bid, ask = _best_bid_ask(pair)
        if side == "buy":
            ref = ask if (ask and ask > 0) else px
            limit_px = float(ref) * (1.0 + max(0.0, float(aggressive_cross_pct)))
        else:
            ref = bid if (bid and bid > 0) else px
            limit_px = float(ref) * (1.0 - max(0.0, float(aggressive_cross_pct)))
        out = _place_aggressive_limit_once(limit_px=float(limit_px))
        if out.get("ok"):
            return out
        if market_fallback:
            remaining_volume = float(volume)
            remaining_notional = float(notional)
            try:
                vol_exec = float(out.get("vol_exec") or 0.0)
            except Exception:
                vol_exec = 0.0
            if vol_exec > 0:
                remaining_volume = max(0.0, float(volume) - vol_exec)
                remaining_notional = max(0.0, remaining_volume * float(px))
            mkt = _place_market(volume_override=remaining_volume, notional_override=remaining_notional)
            mkt["aggressive_limit_first"] = {
                "attempt_failed": True,
                "first_attempt": out,
                "fallback_remaining_volume": remaining_volume,
                "fallback_remaining_notional": remaining_notional,
            }
            return mkt
        return out

    # Maker-first execution: try post-only limit (optionally chase), then fallback to market.
    if exec_mode == "maker_first":
        last_err = None
        for step in range(max(1, chase_steps)):
            bid, ask = _best_bid_ask(pair)
            if side == "buy":
                ref = bid if (bid and bid > 0) else px
                limit_px = float(ref) * (1.0 - float(post_offset))
            else:
                ref = ask if (ask and ask > 0) else px
                limit_px = float(ref) * (1.0 + float(post_offset))

            out = _place_post_only_once(limit_px=float(limit_px))
            if out.get("ok"):
                return out
            last_err = out

        if market_fallback:
            remaining_volume = float(volume)
            remaining_notional = float(notional)
            if last_err:
                try:
                    vol_exec = float(last_err.get("vol_exec") or 0.0)
                except Exception:
                    vol_exec = 0.0
                if vol_exec > 0:
                    remaining_volume = max(0.0, float(volume) - vol_exec)
                    remaining_notional = max(0.0, remaining_volume * float(px))
            mkt = _place_market(volume_override=remaining_volume, notional_override=remaining_notional)
            # Preserve context about maker attempts
            if last_err:
                mkt["maker_first"] = {
                    "attempt_failed": True,
                    "last_error": last_err,
                    "fallback_remaining_volume": remaining_volume,
                    "fallback_remaining_notional": remaining_notional,
                }
            return mkt

        # No fallback: return last maker attempt details
        return last_err or {"ok": False, "error": "maker_first_failed"}

    # Default: market
    return _place_market()


def limit_notional(
    symbol: str,
    side: str,
    notional: float,
    limit_price: float,
    price: Optional[float] = None,
    strategy: Optional[str] = None,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Limit order by USD notional.

    This is primarily used for stop exits: when our stop triggers, we submit a
    SELL limit slightly below the stop to reduce slippage vs a pure market exit.

    NOTE: We do not wait for fills here. Callers should reconcile balances and
    fall back to a market exit if the limit hasn't filled within a timeout.
    """
    side = side.lower().strip()
    if side not in ("buy", "sell"):
        raise ValueError("side must be 'buy' or 'sell'")

    if not isinstance(limit_price, (int, float)) or not math.isfinite(float(limit_price)) or float(limit_price) <= 0:
        raise ValueError("limit_price must be a positive number")

    ui = symbol.upper()
    pair = to_kraken(ui)

    strat_tag = (strategy or "").strip()
    if not strat_tag:
        raise ValueError("missing strategy tag for order (strategy is required)")

    # Cooldown gate (keyed on Kraken pair)
    _cooldown_check(strat_tag, pair, side)

    # Determine reference price for sizing (use passed in price or last_price)
    if isinstance(price, (int, float)) and math.isfinite(float(price)) and float(price) > 0:
        px = float(price)
    else:
        px = _ensure_price(ui)

    volume = float(notional) / px

    avail_for_sell: Optional[float] = None
    if side == "sell":
        try:
            bal = _fetch_balances()
        except Exception as e:
            return {"ok": False, "error": f"limit_notional failed: balance fetch failed: {e}"}

        base = (ui.split('/', 1)[0] if '/' in ui else ui).strip().upper()
        avail = float(_get_balance_float(bal, base) or 0.0)
        avail_for_sell = avail
        if avail <= 0:
            return {"ok": False, "error": f"limit_notional failed: insufficient {base} balance (avail={avail})"}

        volume = min(volume, avail)
        if float(volume) <= 0:
            return {"ok": False, "error": f"limit_notional failed: sell volume <= 0 after balance cap (avail={avail})"}

    meta = _pair_meta(pair)
    ordermin = float(meta.get("ordermin") or 0.0)
    lot_decimals = int(meta.get("lot_decimals") or 8)
    pair_decimals = int(meta.get("pair_decimals") or 8)

    volume = _truncate_to_decimals(float(volume), lot_decimals)
    if volume <= 0.0:
        return {"ok": False, "error": "limit_notional failed: volume <= 0 after precision truncation"}

    if ordermin > 0.0 and float(volume) < ordermin:
        if side == "sell" and avail_for_sell is not None:
            if float(avail_for_sell) >= ordermin:
                volume = _truncate_to_decimals(float(ordermin), lot_decimals)
            else:
                return {"ok": False, "error": f"limit_notional failed: dust_below_min_volume:{volume}<{ordermin} (pair={pair}) (avail={avail_for_sell})"}
        else:
            return {"ok": False, "error": f"limit_notional failed: below_min_volume:{volume}<{ordermin} (pair={pair})"}

    payload = {
        "pair": pair,
        "type": "buy" if side == "buy" else "sell",
        "ordertype": "limit",
        "price": _format_price(limit_price, pair_decimals),
        "volume": _format_volume(volume, lot_decimals),
        "userref": str(
            _userref_for_strategy(strategy) if strategy is not None else _userref(ui, side, float(notional))
        ),
    }

    try:
        res = _priv("AddOrder", payload)
    except Exception as e:
        return {"ok": False, "error": f"limit_notional failed: {e}"}

    txid = None
    descr = None
    try:
        txid = (res.get("txid") or [None])[0]
        descr = (res.get("descr") or {}).get("order")
    except Exception:
        pass

    _cooldown_latch(strat_tag, pair, side)
    return {
        "pair": pair,
        "side": side,
        "notional": float(notional),
        "volume": volume,
        "limit_price": float(limit_price),
        "txid": txid,
        "descr": descr,
        "result": res,
    }



def cancel_order(txid: str) -> Dict[str, Any]:
    """Best-effort cancel by txid."""
    t = str(txid or "").strip()
    if not t:
        return {"ok": False, "error": "missing txid"}
    try:
        res = _priv("CancelOrder", {"txid": t})
        return {"ok": True, "txid": t, "result": res}
    except Exception as e:
        return {"ok": False, "txid": t, "error": str(e)}

def _fetch_open_orders() -> dict:
    ttl_env = os.getenv("KRAKEN_OPEN_ORDERS_TTL_SEC", os.getenv("BROKER_STATE_CACHE_TTL_SEC", "180"))
    grace_env = os.getenv("KRAKEN_OPEN_ORDERS_STALE_GRACE_SEC", os.getenv("BROKER_STATE_CACHE_GRACE_SEC", "1800"))
    try:
        ttl = float(ttl_env or 180.0)
    except Exception:
        ttl = 180.0
    try:
        stale_grace = float(grace_env or 1800.0)
    except Exception:
        stale_grace = 1800.0

    now = time.time()
    try:
        ts = float(_OPEN_ORDERS_CACHE.get("ts") or 0.0)
    except Exception:
        ts = 0.0
    cached_open = _OPEN_ORDERS_CACHE.get("open") if isinstance(_OPEN_ORDERS_CACHE.get("open"), dict) else {}

    if ttl > 0 and (now - ts) < ttl and isinstance(cached_open, dict):
        return cached_open  # type: ignore[return-value]

    with _OPEN_ORDERS_FETCH_LOCK:
        now = time.time()
        try:
            ts = float(_OPEN_ORDERS_CACHE.get("ts") or 0.0)
        except Exception:
            ts = 0.0
        cached_open = _OPEN_ORDERS_CACHE.get("open") if isinstance(_OPEN_ORDERS_CACHE.get("open"), dict) else {}
        if ttl > 0 and (now - ts) < ttl and isinstance(cached_open, dict):
            return cached_open  # type: ignore[return-value]

        cooldown_remaining = _private_endpoint_cooldown_remaining("OpenOrders")
        if cooldown_remaining > 0 and isinstance(cached_open, dict) and (cached_open or ts > 0) and (now - ts) <= stale_grace:
            return cached_open  # type: ignore[return-value]

        try:
            open_orders = _priv("OpenOrders", {}) or {}
            if not isinstance(open_orders, dict):
                open_orders = {}
            _OPEN_ORDERS_CACHE["ts"] = time.time()
            _OPEN_ORDERS_CACHE["open"] = open_orders
            _clear_private_endpoint_cooldown("OpenOrders")
            return open_orders
        except Exception:
            if isinstance(cached_open, dict) and (cached_open or ts > 0) and (now - ts) <= stale_grace:
                return cached_open  # type: ignore[return-value]
            raise

def orders() -> Any:
    try:
        return _fetch_open_orders()
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
        bal = _fetch_balances()  # {"ZUSD":"123.45","XXBT":"0.01","SOL.F":"0.12",...}
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


def trades_history_since(*, since_ts: float, pair: str | None = None, limit: int = 250) -> list[dict]:
    """Return a flat list of trades from Kraken TradesHistory starting at `since_ts`.

    Some parts of the app expect this helper (used by /trades/refresh). We keep it
    in broker_kraken so app.py can call it directly.
    """
    since_ts_i = int(since_ts)
    out: list[dict] = []
    ofs = 0
    # Kraken TradesHistory uses offset-based pagination.
    while len(out) < max(1, int(limit)):
        res = _priv("TradesHistory", {"type": "all", "trades": True, "start": since_ts_i, "ofs": int(ofs)})
        trades = (res.get("trades") or {}) if isinstance(res, dict) else {}
        if not isinstance(trades, dict) or not trades:
            break

        # trades is dict: {txid: {...}}
        batch_n = 0
        for txid, t in trades.items():
            if not isinstance(t, dict):
                continue
            item = dict(t)
            item.setdefault("txid", txid)
            if pair and item.get("pair") != pair:
                continue
            out.append(item)
            batch_n += 1
            if len(out) >= max(1, int(limit)):
                break

        if batch_n <= 0:
            break
        ofs += batch_n

    return out[: max(1, int(limit))]

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

            # Propagate order userref down to each trade id for attribution
            tr_list = o.get("trades") or []
            if tr_list and "userref" in row and row.get("userref") is not None:
                for _tid in tr_list:
                    if not _tid:
                        continue
                    trow = out.get(_tid, {})
                    if trow.get("userref") is None:
                        trow["userref"] = row.get("userref")
                    out[_tid] = trow

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
