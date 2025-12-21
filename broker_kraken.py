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
_KRAKEN_API = API_BASE  # alias for legacy references
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

def _priv(path: str, data: Dict[str, Any], timeout: float = 30.0) -> Dict[str, Any]:
    """Kraken private endpoint helper.

    IMPORTANT: Kraken returns HTTP 200 even when the call failed. The failure is
    encoded in the JSON payload: {"error":[...]}.

    This helper logs the endpoint + errors and raises if errors are present.
    """
    urlpath = f"/0/private/{path}"
    url = f"{API_BASE}{urlpath}"

    # Kraken requires an always-increasing nonce (int). Milliseconds since epoch is fine.
    data = dict(data or {})
    data["nonce"] = int(time.time() * 1000)

    headers = {"User-Agent": "crypto-system-api/kraken"}
    headers.update(_sign(urlpath, data))
    headers["Content-Type"] = "application/x-www-form-urlencoded"

    # NOTE: requests encodes dict -> form body by default
    r = requests.post(url, data=data, headers=headers, timeout=timeout)

    # Rate limit
    if r.status_code == 429:
        raise RuntimeError(f"Kraken rate limited (HTTP 429) on {path}")

    try:
        payload = r.json()
    except Exception as e:
        logger.error("kraken._priv non-json response: path=%s status=%s body=%r", path, r.status_code, r.text[:500])
        raise

    errors = payload.get("error") or []
    if errors:
        # Log full error list; keep request params minimal to avoid leaking secrets
        safe_keys = {k: data.get(k) for k in ("pair","type","ordertype","volume","price","price2","leverage","oflags","timeinforce") if k in data}
        logger.error("kraken._priv ERROR: path=%s status=%s errors=%s req=%s", path, r.status_code, errors, safe_keys)
        raise RuntimeError(f"Kraken private call failed: {path} errors={errors}")

    result = payload.get("result")
    # Useful for confirming trades are actually being placed:
    if path == "AddOrder":
        txid = None
        try:
            txid = (result or {}).get("txid")
        except Exception:
            txid = None
        logger.info("kraken AddOrder ok: txid=%s pair=%s type=%s ordertype=%s vol=%s",
                    txid, data.get("pair"), data.get("type"), data.get("ordertype"), data.get("volume"))

    return result or {}
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

def market_notional(
    symbol: str,
    side: str,
    notional: float,
    price: Optional[float] = None,
    strategy: Optional[str] = None,
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
    _cooldown_check_and_latch(strat_tag, pair, side)

    # Use caller-supplied price if valid; otherwise fall back to last_price.
    if isinstance(price, (int, float)) and math.isfinite(float(price)) and float(price) > 0:
        px = float(price)
    else:
        px = _ensure_price(ui)

    volume = _round_qty(float(notional) / px)
    if volume <= 0:
        raise ValueError("computed volume <= 0")

    payload = {
        "pair": pair,
        "type": "buy" if side == "buy" else "sell",
        "ordertype": "market",
        "volume": f"{volume:.8f}",
        "userref": str(
            _userref_for_strategy(strategy)
            if strategy is not None
            else _userref(ui, side, float(notional))
        ),
    }

    try:
        res = _priv("AddOrder", payload)
    except Exception:
        _cooldown_rollback(strat_tag, pair, side)  # rollback MUST use the same key as latch
        raise

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
