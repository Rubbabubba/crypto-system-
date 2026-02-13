from __future__ import annotations



import logging

log = logging.getLogger("crypto_light")
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, List, Tuple
from uuid import uuid4

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .broker import balances_by_asset as _balances_by_asset
from .broker import base_asset as _base_asset
from .broker import last_price as _last_price
from .broker import market_notional as _market_notional
from .broker import get_bars as _get_bars
from .config import load_settings
from .models import WebhookPayload, WorkerExitPayload, WorkerScanPayload
from .risk import compute_brackets
from .state import InMemoryState, TradePlan
from .symbol_map import normalize_symbol

settings = load_settings()

# Keep a normalized set for fast membership checks.
ALLOWED_SYMBOLS = set(getattr(settings, 'allowed_symbols', []) or [])

state = InMemoryState()
app = FastAPI(title="Crypto Light", version="1.0.3")


def _require_worker_secret(provided: str | None) -> tuple[bool, str | None]:
    """Validate worker secret for /worker/* endpoints.

    Behavior:
    - If WORKER_SECRET is not configured (empty), allow requests.
    - If configured, require the caller to pass the same secret.
    """
    expected = (settings.worker_secret or "").strip()
    if not expected:
        return True, None

    provided = (provided or "").strip()
    if not provided:
        return False, "missing worker_secret"
    if provided != expected:
        return False, "invalid worker_secret"
    return True, None


# ---------- Entry engine (optional; replaces TradingView) ----------
ENTRY_ENGINE_ENABLED = (os.getenv("ENTRY_ENGINE_ENABLED", "1").strip().lower() in ("1", "true", "yes", "on"))
ENTRY_ENGINE_STRATEGIES = {s.strip().lower() for s in os.getenv("ENTRY_ENGINE_STRATEGIES", "rb1,tc1").split(",") if s.strip()}
ENTRY_ENGINE_TIMEFRAME = os.getenv("ENTRY_ENGINE_TIMEFRAME", "5Min").strip() or "5Min"   # must match broker get_bars
ENTRY_ENGINE_LIMIT_BARS = int(float(os.getenv("ENTRY_ENGINE_LIMIT_BARS", "300") or 300))

# RB1 params (5m)
RB1_LOOKBACK_BARS = int(float(os.getenv("RB1_LOOKBACK_BARS", "48") or 48))
RB1_BREAKOUT_BUFFER_PCT = float(os.getenv("RB1_BREAKOUT_BUFFER_PCT", "0.0005") or 0.0005)  # 0.05%

# RB1 near-breakout params (optional)
# If RB1_NEAR_BREAKOUT_PCT > 0, we will also fire when price is within this percent below breakout level
# and momentum is positive (close > prev_close) unless RB1_NEAR_REQUIRE_UP=0.
RB1_NEAR_BREAKOUT_PCT = float(os.getenv("RB1_NEAR_BREAKOUT_PCT", "0") or 0)  # e.g. 0.0015 = 0.15%
RB1_NEAR_REQUIRE_UP = int(float(os.getenv("RB1_NEAR_REQUIRE_UP", "1") or 1)) == 1
RB1_NEAR_UP_MODE = (os.getenv("RB1_NEAR_UP_MODE", "gt") or "gt").strip().lower()  # gt (default) or ge

# TC1 params
TC1_LTF_EMA = int(float(os.getenv("TC1_LTF_EMA", "20") or 20))
TC1_HTF_TIMEFRAME = os.getenv("TC1_HTF_TIMEFRAME", "60Min").strip() or "60Min"
TC1_HTF_LIMIT_BARS = int(float(os.getenv("TC1_HTF_LIMIT_BARS", "300") or 300))
TC1_HTF_FAST = int(float(os.getenv("TC1_HTF_FAST", "50") or 50))
TC1_HTF_SLOW = int(float(os.getenv("TC1_HTF_SLOW", "200") or 200))
TC1_RECLAIM_BUFFER_PCT = float(os.getenv("TC1_RECLAIM_BUFFER_PCT", "0.0005") or 0.0005)  # 0.05%

# TC1 trend-soften epsilon (optional)
# If >0, allow HTF uptrend when EMA_fast is within epsilon below EMA_slow (still requires EMA_fast rising).
TC1_TREND_SOFTEN_EPSILON = float(os.getenv("TC1_TREND_SOFTEN_EPSILON", "0") or 0)  # e.g. 0.002 = 0.2%


# ---------- Scanner config (soft allow) ----------
SCANNER_URL = os.getenv("SCANNER_URL", "").strip()
SCANNER_REFRESH_SEC = int(float(os.getenv("SCANNER_REFRESH_SEC", "300") or 300))
SCANNER_SOFT_ALLOW = (os.getenv("SCANNER_SOFT_ALLOW", "1").strip().lower() in ("1", "true", "yes", "on"))
SCANNER_TIMEOUT_SEC = float(os.getenv("SCANNER_TIMEOUT_SEC", "10") or 10)

# ---------- Universe normalization / safety constraints ----------
SCANNER_DRIVEN_UNIVERSE = (os.getenv('SCANNER_DRIVEN_UNIVERSE', '1').strip().lower() in ('1','true','yes','on'))
SCANNER_TARGET_N = int(float(os.getenv('SCANNER_TARGET_N', os.getenv('TOP_N', '5')) or 5))
UNIVERSE_USD_ONLY = (os.getenv('UNIVERSE_USD_ONLY', '0').strip().lower() in ('1','true','yes','on'))
UNIVERSE_PREFER_USD_FOR_STABLES = (os.getenv('UNIVERSE_PREFER_USD_FOR_STABLES', '0').strip().lower() in ('1','true','yes','on'))
FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS = (os.getenv('FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS', '0').strip().lower() in ('1','true','yes','on'))

MAX_OPEN_POSITIONS = int(float(os.getenv('MAX_OPEN_POSITIONS', '2') or 2))
MAX_ENTRIES_PER_SCAN = int(float(os.getenv('MAX_ENTRIES_PER_SCAN', '1') or 1))
MAX_ENTRIES_PER_DAY = int(float(os.getenv('MAX_ENTRIES_PER_DAY', '5') or 5))

_SCANNER_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "active_symbols": set(),   # type: Set[str]
    "ok": False,
    "last_error": None,
    "raw": None,
}


def _log_event(level: str, event: Dict[str, Any]) -> None:
    try:
        state.record_event(event)
    except Exception:
        pass
    try:
        print(json.dumps(event, default=str))
    except Exception:
        print(str(event))





def get_positions() -> list[dict]:
    """Return open positions derived from Kraken balances + live USD prices.

    - Uses normalized balances from `balances_by_asset()` (e.g., "BTC", "ADA").
    - Prices are fetched per-symbol via `last_price("ASSET/USD")`.
    - Ignores *dust* positions below `settings.min_position_notional_usd` to prevent
      repeated exit/entry churn around exchange minimums.

    Return format:
      [{"symbol": "BTC/USD", "asset": "BTC", "qty": 0.01, "price": 65000, "notional_usd": 650}]
    """
    try:
        balances = _balances_by_asset() or {}
    except Exception as e:
        log.exception("balances_by_asset failed: %s", e)
        return []

    positions: list[dict] = []
    for asset, qty in balances.items():
        asset_u = str(asset).upper().strip()
        if asset_u == "USD":
            continue

        try:
            qty_f = float(qty or 0.0)
        except Exception:
            continue
        if qty_f <= 0:
            continue

        sym = f"{asset_u}/USD"
        try:
            px = float(_last_price(sym) or 0.0)
        except Exception:
            px = 0.0
        if px <= 0:
            continue

        notional = qty_f * px
        if notional < float(getattr(settings, "min_position_notional_usd", 0.0) or 0.0):
            continue

        positions.append(
            {
                "symbol": sym,
                "asset": asset_u,
                "qty": qty_f,
                "price": px,
                "notional_usd": notional,
            }
        )

    positions.sort(key=lambda x: float(x.get("notional_usd", 0.0) or 0.0), reverse=True)
    return positions

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _utc_date_str(now: Optional[datetime] = None) -> str:
    n = now or datetime.now(timezone.utc)
    return n.strftime("%Y-%m-%d")


def _is_after_utc_hhmm(now: datetime, hhmm_utc: str) -> bool:
    if not hhmm_utc:
        return False
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return False
    return (now.hour, now.minute) >= (h, m)


def _ema(values: list[float], period: int) -> list[float]:
    """Simple EMA series; returns list same length as values."""
    if period <= 1 or not values:
        return values[:]
    k = 2.0 / (period + 1.0)
    out: list[float] = []
    ema = float(values[0])
    out.append(ema)
    for v in values[1:]:
        ema = (float(v) * k) + (ema * (1.0 - k))
        out.append(ema)
    return out


def _rb1_long_signal(symbol: str) -> tuple[bool, dict]:
    """True breakout: close crosses above prior N-bar high (edge-trigger)."""
    bars = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, RB1_LOOKBACK_BARS + 5))
    if not bars or len(bars) < (RB1_LOOKBACK_BARS + 3):
        return False, {"reason": "insufficient_bars", "bars": len(bars) if bars else 0}

    highs = [float(b["h"]) for b in bars]
    closes = [float(b["c"]) for b in bars]
    ts = [int(float(b["t"])) for b in bars]

    # prior range high excludes current bar
    look = highs[-(RB1_LOOKBACK_BARS+1):-1]
    range_high = max(look)
    prev_close = closes[-2]
    cur_close = closes[-1]

    level = range_high * (1.0 + RB1_BREAKOUT_BUFFER_PCT)

    # Primary breakout (edge-trigger)
    breakout = (prev_close <= level) and (cur_close > level)

    # Optional near-breakout: allow early entry when close is within X% of the level.
    near = False
    near_threshold = None
    dist_pct = None
    if RB1_NEAR_BREAKOUT_PCT and RB1_NEAR_BREAKOUT_PCT > 0:
        near_threshold = level * (1.0 - RB1_NEAR_BREAKOUT_PCT)
        dist_pct = (level - cur_close) / level if level else None
        momentum_ok = ((cur_close >= prev_close) if RB1_NEAR_UP_MODE == 'ge' else (cur_close > prev_close)) if RB1_NEAR_REQUIRE_UP else True
        # Still require we haven't already broken out in prior bar to avoid repeated triggers.
        near = (prev_close < level) and (cur_close >= near_threshold) and momentum_ok

    fired = breakout or near

    meta = {
        "range_high": range_high,
        "level": level,
        "prev_close": prev_close,
        "close": cur_close,
        "bar_ts": ts[-1],
        "breakout": breakout,
        "near": near,
        "near_pct": RB1_NEAR_BREAKOUT_PCT,
        "near_threshold": near_threshold,
        "dist_to_level_pct": dist_pct,
        "require_up": RB1_NEAR_REQUIRE_UP,
        "up_mode": RB1_NEAR_UP_MODE,
    }
    return fired, meta


def _tc1_long_signal(symbol: str) -> tuple[bool, dict]:
    """Trend pullback continuation:
    - HTF uptrend: EMA_fast > EMA_slow AND EMA_fast rising
    - LTF reclaim: close crosses back above LTF EMA after being below
    """
    # HTF trend
    htf = _get_bars(symbol, timeframe=TC1_HTF_TIMEFRAME, limit=max(TC1_HTF_LIMIT_BARS, TC1_HTF_SLOW + 5))
    if not htf or len(htf) < (TC1_HTF_SLOW + 3):
        return False, {"reason": "insufficient_htf_bars", "bars": len(htf) if htf else 0}

    htf_closes = [float(b["c"]) for b in htf]
    ema_fast = _ema(htf_closes, TC1_HTF_FAST)
    ema_slow = _ema(htf_closes, TC1_HTF_SLOW)
    raw_uptrend = (ema_fast[-1] > ema_slow[-1]) and (ema_fast[-1] > ema_fast[-2])
    eps = TC1_TREND_SOFTEN_EPSILON if TC1_TREND_SOFTEN_EPSILON and TC1_TREND_SOFTEN_EPSILON > 0 else 0.0
    softened = (ema_fast[-1] >= (ema_slow[-1] * (1.0 - eps))) and (ema_fast[-1] > ema_fast[-2])
    uptrend = softened

    # LTF reclaim
    ltf = _get_bars(symbol, timeframe=ENTRY_ENGINE_TIMEFRAME, limit=max(ENTRY_ENGINE_LIMIT_BARS, TC1_LTF_EMA + 10))
    if not ltf or len(ltf) < (TC1_LTF_EMA + 5):
        return False, {"reason": "insufficient_ltf_bars", "bars": len(ltf) if ltf else 0, "uptrend": uptrend}

    ltf_closes = [float(b["c"]) for b in ltf]
    ltf_ts = [int(float(b["t"])) for b in ltf]
    ltf_ema = _ema(ltf_closes, TC1_LTF_EMA)

    prev_close = ltf_closes[-2]
    cur_close = ltf_closes[-1]
    prev_ema = ltf_ema[-2]
    cur_ema = ltf_ema[-1]

    buffer = cur_ema * TC1_RECLAIM_BUFFER_PCT
    fired = uptrend and (prev_close < (prev_ema - buffer)) and (cur_close > (cur_ema + buffer))

    meta = {
        "uptrend": uptrend,
        "uptrend_raw": raw_uptrend,
        "trend_soften_epsilon": eps,
        "htf_fast": ema_fast[-1],
        "htf_fast_prev": ema_fast[-2],
        "htf_slow": ema_slow[-1],
        "ltf_ema": cur_ema,
        "prev_close": prev_close,
        "close": cur_close,
        "bar_ts": ltf_ts[-1],
    }
    return fired, meta



def _within_minutes_after_utc_hhmm(now: datetime, hhmm_utc: str, window_min: int) -> bool:
    if not hhmm_utc:
        return False
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return False
    start = h * 60 + m
    cur = now.hour * 60 + now.minute
    return start <= cur < (start + max(1, int(window_min)))


def _is_flatten_time(now: datetime, hhmm_utc: str) -> bool:
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        h, m = 23, 55
    return (now.hour, now.minute) >= (h, m)


def _validate_symbol(symbol: str) -> str:
    sym = normalize_symbol(symbol)
    if sym not in settings.allowed_symbols:
        raise HTTPException(status_code=400, detail=f"symbol_not_allowed: {sym}")
    return sym


def _has_position(symbol: str) -> tuple[bool, float]:
    bal = _balances_by_asset()
    base = _base_asset(symbol)
    qty = float(bal.get(base, 0.0) or 0.0)
    if qty <= 0:
        return False, 0.0
    px = float(_last_price(symbol))
    return True, float(qty * px)


# ---------- Scanner helpers ----------
def _scanner_should_refresh() -> bool:
    if not SCANNER_URL:
        return False
    return (time.time() - float(_SCANNER_CACHE["ts"] or 0.0)) >= float(SCANNER_REFRESH_SEC)


def _scanner_fetch_active_symbols() -> list[str]:
    """Fetch scanner / knows how to normalize symbols and apply dedup/filters.
    Returns a list of *normalized* symbols (e.g. BTC/USD) suitable for allow-checks.
    Never raises; returns [] on failure.
    """
    try:
        url = settings.scanner_url
        if not url:
            return []
        r = requests.get(url, timeout=settings.scanner_timeout_sec)
        r.raise_for_status()
        data = r.json()
        syms = data.get("active_symbols") or []
        out: list[str] = []
        for s in syms:
            try:
                out.append(normalize_symbol(str(s)))
            except Exception:
                continue
        # de-dupe preserve order
        seen=set()
        dedup=[]
        for s in out:
            if s in seen:
                continue
            seen.add(s)
            dedup.append(s)
        return dedup
    except Exception:
        return []



def _scanner_fetch_active_symbols_and_meta() -> tuple[bool, str | None, dict, list[str]]:
    """Fetch active symbols + include scanner meta/diagnostics.

    Returns: (ok, reason, meta, symbols)
    - ok: True if HTTP 200 and JSON parsed
    - reason: short string when ok=False
    - meta: dict of useful fields (status_code, url, last_error, last_refresh_utc, etc)
    - symbols: list[str] of normalized symbols (e.g., 'ETH/USD')

    Never raises.
    """
    url = (SCANNER_URL or os.getenv("SCANNER_URL", "").strip())
    meta: dict = {"scanner_url": url}
    if not url:
        return False, "missing_scanner_url", meta, []

    try:
        r = requests.get(url, timeout=SCANNER_TIMEOUT_SEC)
        meta["status_code"] = r.status_code
        try:
            meta["elapsed_ms"] = int(getattr(getattr(r, "elapsed", None), "total_seconds", lambda: 0)() * 1000)
        except Exception:
            meta["elapsed_ms"] = None

        if r.status_code != 200:
            meta["body_snippet"] = (r.text or "")[:500]
            return False, f"http_{r.status_code}", meta, []

        data = r.json() if r.content else {}
        meta["scanner_ok"] = bool(data.get("ok", True))
        meta["last_error"] = data.get("last_error")
        meta["last_refresh_utc"] = data.get("last_refresh_utc")
        if isinstance(data.get("meta"), dict):
            meta["scanner_meta"] = data.get("meta")

        raw_syms = data.get("active_symbols")
        if not isinstance(raw_syms, list):
            return False, "invalid_active_symbols", meta, []

        clean: list[str] = []
        for s in raw_syms:
            if not isinstance(s, str):
                continue
            try:
                clean.append(normalize_symbol(s))
            except Exception:
                continue

        # de-dupe preserve order
        seen: set[str] = set()
        dedup: list[str] = []
        for s in clean:
            if s in seen:
                continue
            seen.add(s)
            dedup.append(s)

        return True, None, meta, dedup

    except Exception as e:
        meta["error"] = f"{type(e).__name__}: {e}"
        return False, "exception", meta, []
def _build_universe(payload, scanner_syms: list[str]) -> list[str]:
    """Build the scan universe safely.

    Priority:
      1) Explicit payload.symbols (if provided and non-empty)
      2) Scanner symbols (scanner_syms)  <-- primary mode
      3) Fallback to settings.allowed_symbols

    Optional behaviors (env-driven):
      - SCANNER_TARGET_N: cap universe size from scanner (default 5)
      - UNIVERSE_USD_ONLY: keep only */USD
      - UNIVERSE_PREFER_USD_FOR_STABLES: when USD-only, convert BASE/USDT or BASE/USDC -> BASE/USD
        *only if BASE/USD exists in scanner list*, else drop it.
      - FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS: intersect universe with ALLOWED_SYMBOLS (default off)
    """
    try:
        requested = getattr(payload, "symbols", None) or []
        force_scan = bool(getattr(payload, "force_scan", False))
    except Exception:
        requested, force_scan = [], False

    def _norm_list(items):
        out: list[str] = []
        seen: set[str] = set()
        for s in items or []:
            try:
                sym = normalize_symbol(str(s))
            except Exception:
                continue
            if sym in seen:
                continue
            seen.add(sym)
            out.append(sym)
        return out

    requested_norm = _norm_list(requested)
    scanner_norm = _norm_list(scanner_syms)

    # Build raw universe
    if requested_norm:
        universe = requested_norm
    elif scanner_norm:
        universe = scanner_norm[: max(1, int(SCANNER_TARGET_N or 5))] if SCANNER_DRIVEN_UNIVERSE else scanner_norm
    else:
        universe = _norm_list(getattr(settings, "allowed_symbols", []) or [])

    # USD-only filtering / conversion
    if UNIVERSE_USD_ONLY:
        scanner_set = set(scanner_norm)
        out: list[str] = []
        for s in universe:
            try:
                base, quote = s.split("/", 1)
            except Exception:
                continue
            if quote == "USD":
                out.append(s)
                continue
            if UNIVERSE_PREFER_USD_FOR_STABLES and quote in ("USDT", "USDC"):
                cand = f"{base}/USD"
                if cand in scanner_set:
                    out.append(cand)
                # else: drop (don't silently trade a different quote)
        # de-dupe preserve order
        universe = list(dict.fromkeys(out))

    # Allowed-symbol filtering (off by default for scanner-driven universe)
    allowed = set(getattr(settings, "allowed_symbols", []) or [])
    if allowed and FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS and not (force_scan and SCANNER_SOFT_ALLOW):
        universe = [s for s in universe if s in allowed]

    # Stable fallback: if universe empty but allowed configured, use allowed
    if not universe and allowed:
        universe = list(dict.fromkeys(list(allowed)))

    return universe


def _scanner_refresh() -> None:
    if not SCANNER_URL:
        return

    try:
        r = requests.get(SCANNER_URL, timeout=SCANNER_TIMEOUT_SEC)
        r.raise_for_status()
        j = r.json()

        # Expected: {"ok": true, "active_symbols": ["SOL/USD", ...], ...}
        active = j.get("active_symbols") or j.get("active_coins") or []
        active_norm = set()

        for s in active:
            try:
                active_norm.add(normalize_symbol(str(s)))
            except Exception:
                continue

        _SCANNER_CACHE["ts"] = time.time()
        _SCANNER_CACHE["active_symbols"] = active_norm
        _SCANNER_CACHE["ok"] = bool(j.get("ok", True)) and len(active_norm) > 0
        _SCANNER_CACHE["last_error"] = None
        _SCANNER_CACHE["raw"] = j

    except Exception as e:
        _SCANNER_CACHE["ts"] = time.time()
        _SCANNER_CACHE["ok"] = False
        _SCANNER_CACHE["last_error"] = str(e)


def _symbol_allowed_by_scanner(symbol: str) -> tuple[bool, str, Dict[str, Any]]:
    """
    Returns (allowed, reason, meta)
    Soft allow:
      - If scanner is down OR returns empty => allow (fallback)
      - If scanner is healthy => allow only if symbol is in active set
    """
    if not SCANNER_URL:
        return True, "scanner_disabled", {"scanner_url": ""}

    if _scanner_should_refresh():
        _scanner_refresh()

    ok = bool(_SCANNER_CACHE.get("ok"))
    active = _SCANNER_CACHE.get("active_symbols") or set()
    err = _SCANNER_CACHE.get("last_error")

    meta = {
        "scanner_ok": ok,
        "scanner_error": err,
        "scanner_active_count": len(active),
        "scanner_soft_allow": SCANNER_SOFT_ALLOW,
    }

    if not ok or len(active) == 0:
        if SCANNER_SOFT_ALLOW:
            return True, "scanner_fallback_allow", meta
        return False, "scanner_unavailable_block", meta

    if symbol in active:
        return True, "scanner_active_allow", meta

    return False, "not_in_active_set", meta


@app.get("/health")
def health():
    scanner_ok, scanner_reason, scanner_meta, scanner_syms = _scanner_fetch_active_symbols_and_meta()
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "scanner_url": SCANNER_URL or None,
        "scanner_ok": scanner_ok and (len(scanner_syms) > 0),
        "scanner_reason": scanner_reason,
        "scanner_active_count": len(scanner_syms),
        "scanner_last_refresh_utc": (scanner_meta or {}).get("last_refresh_utc"),
        "scanner_last_error": (scanner_meta or {}).get("last_error"),
        "scanner_soft_allow": SCANNER_SOFT_ALLOW,
    }


@app.get("/telemetry")
def telemetry(limit: int = 100):
    lim = max(1, min(int(limit), 500))
    return {"ok": True, "count": len(state.telemetry), "items": state.telemetry[-lim:]}



def _execute_long_entry(
    *,
    symbol: str,
    strategy: str,
    signal_name: str | None,
    signal_id: str | None,
    notional: float | None,
    source: str,
    req_id: str,
    client_ip: str | None = None,
    extra: dict | None = None,
):
    def ignored(reason: str, **extra_fields):
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": source,
            "status": "ignored",
            "reason": reason,
            **(extra_fields or {}),
        }
        _log_event("warning", evt)
        return {"ok": True, "ignored": True, "reason": reason, **(extra_fields or {})}

    now = datetime.now(timezone.utc)
    utc_date = _utc_date_str(now)
    state.reset_daily_counters_if_needed(utc_date)

    if signal_id and state.seen_recent_signal(signal_id, int(settings.signal_dedupe_ttl_sec)):
        return ignored("duplicate_signal_id", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    if not bool(settings.trading_enabled):
        return ignored("trading_disabled", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    # daily loss cap
    if state.daily_pnl_usd <= -float(settings.max_daily_loss_usd):
        return ignored("max_daily_loss_reached", symbol=symbol, strategy=strategy, daily_pnl_usd=state.daily_pnl_usd)

    # cooldown
    if not state.can_enter(symbol, int(settings.entry_cooldown_sec)):
        return ignored("entry_cooldown", symbol=symbol, cooldown_sec=int(settings.entry_cooldown_sec))

    # max trades per day per symbol
    if not state.can_trade_symbol_today(symbol, int(settings.max_trades_per_symbol_per_day)):
        return ignored("max_trades_per_symbol_per_day", symbol=symbol, max_per_day=int(settings.max_trades_per_symbol_per_day))

    # one position per symbol
    has_pos, pos_notional = _has_position(symbol)
    if has_pos:
        return ignored("position_already_open", symbol=symbol, position_notional_usd=pos_notional, strategy=strategy)

    _notional = float(notional or settings.default_notional_usd)
    if _notional < float(settings.min_order_notional_usd):
        return ignored("notional_below_minimum", symbol=symbol, notional_usd=_notional, min_notional_usd=float(settings.min_order_notional_usd))

    px = float(_last_price(symbol))
    stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)

    # Exposure caps (0 disables)
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)
    if max_total or max_symbol:
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)
        if max_total and (total_exposure + _notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, _notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd"}
        if max_symbol and (sym_exposure + _notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, _notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd"}
    # Exposure caps (0 disables)
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)
    if max_total or max_symbol:
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)
        if max_total and (total_exposure + _notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, _notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd"}
        if max_symbol and (sym_exposure + _notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, _notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd"}
    res = _market_notional(symbol=symbol, side="buy", notional=_notional, strategy=strategy, price=px)

    evt = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "req_id": req_id,
        "kind": source,
        "status": "executed",
        "action": "buy",
        "symbol": symbol,
        "strategy": strategy,
        "signal": signal_name,
        "signal_id": signal_id,
        "price": px,
        "notional_usd": _notional,
        "stop": float(stop_price),
        "take": float(take_price),
        "order": res,
        "client_ip": client_ip,
    }
    if extra:
        evt.update(extra)
    _log_event("info", evt)

    if res.get("ok"):
        state.mark_enter(symbol)
        state.plans[symbol] = TradePlan(
            symbol=symbol,
            strategy=strategy,
            side="buy",
            entry_price=float(px),
            stop_price=float(stop_price),
            take_price=float(take_price),
            notional_usd=float(_notional),
            opened_ts=time.time(),
        )
        return {"ok": True, "executed": True, "symbol": symbol, "strategy": strategy, "price": px, "stop": float(stop_price), "take": float(take_price)}

    # Do NOT create a plan or cooldown on failed orders (e.g., insufficient funds).
    return {"ok": False, "executed": False, "symbol": symbol, "strategy": strategy, "price": px, "stop": float(stop_price), "take": float(take_price), "error": res.get("error") or "order_failed"}


@app.post("/webhook")
def webhook(payload: WebhookPayload, request: Request):
    req_id = request.headers.get("x-request-id") or str(uuid4())
    client_ip = getattr(request.client, "host", None)

    def ignored(reason: str, **extra):
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": "webhook",
            "status": "ignored",
            "reason": reason,
            "client_ip": client_ip,
            **extra,
        }
        _log_event("warning", evt)
        return {"ok": True, "ignored": True, "reason": reason, **extra}

    # Secret
    if not settings.webhook_secret or payload.secret != settings.webhook_secret:
        _log_event(
            "warning",
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "req_id": req_id,
                "kind": "webhook",
                "status": "rejected",
                "reason": "invalid_secret",
                "client_ip": client_ip,
            },
        )
        raise HTTPException(status_code=401, detail="invalid secret")

    # Symbol allowlist (static env allowlist)
    symbol = _validate_symbol(payload.symbol)

    # Scanner gate (dynamic top5) — soft allow enabled by env
    allowed, allow_reason, allow_meta = _symbol_allowed_by_scanner(symbol)
    if not allowed:
        return ignored(
            allow_reason,
            symbol=symbol,
            strategy=str(payload.strategy),
            signal=(payload.signal or None),
            signal_id=(payload.signal_id or None),
            **allow_meta,
        )

    side = str(payload.side).lower().strip()
    strategy = str(payload.strategy).strip()
    signal_name = (payload.signal or "").strip() or None
    signal_id = (payload.signal_id or "").strip() or None

    if side != "buy":
        return ignored("long_only_mode", symbol=symbol, side=side, strategy=strategy)

    now = datetime.now(timezone.utc)
    utc_date = _utc_date_str(now)
    state.reset_daily_counters_if_needed(utc_date)

    # Idempotency / TV retries
    if signal_id and state.seen_recent_signal(signal_id, int(settings.signal_dedupe_ttl_sec)):
        return ignored(
            "duplicate_signal_id",
            symbol=symbol,
            strategy=strategy,
            signal=signal_name,
            signal_id=signal_id,
            ttl_sec=int(settings.signal_dedupe_ttl_sec),
        )

    # Master kill switch
    if not bool(settings.trading_enabled):
        return ignored("trading_disabled", symbol=symbol, strategy=strategy, signal=signal_name, signal_id=signal_id)

    # 24/7 by default; cutoff enforced only if set
    if settings.no_new_entries_after_utc and _is_after_utc_hhmm(now, settings.no_new_entries_after_utc):
        return ignored(
            "entries_disabled_after_cutoff",
            symbol=symbol,
            strategy=strategy,
            signal=signal_name,
            signal_id=signal_id,
            cutoff_utc=settings.no_new_entries_after_utc,
            utc=now.isoformat(),
        )

    # Optional short flatten window block (off by default)
    if settings.enforce_daily_flatten and settings.block_entries_after_flatten:
        if _within_minutes_after_utc_hhmm(now, settings.daily_flatten_time_utc, window_min=3):
            return ignored(
                "entries_blocked_during_flatten_window",
                symbol=symbol,
                strategy=strategy,
                signal=signal_name,
                signal_id=signal_id,
                flatten_utc=settings.daily_flatten_time_utc,
                utc=now.isoformat(),
            )

    # Entry cooldown
    if not state.can_enter(symbol, int(settings.entry_cooldown_sec)):
        return ignored(
            "entry_cooldown_active",
            symbol=symbol,
            strategy=strategy,
            signal=signal_name,
            signal_id=signal_id,
            cooldown_sec=int(settings.entry_cooldown_sec),
        )

    # Max trades/day per symbol
    trades = int(state.trades_today_by_symbol.get(symbol, 0) or 0)
    if trades >= int(settings.max_trades_per_symbol_per_day):
        return ignored(
            "max_trades_reached",
            symbol=symbol,
            strategy=strategy,
            trades_today=trades,
            max_trades=int(settings.max_trades_per_symbol_per_day),
        )

    # One position per symbol
    has_pos, pos_notional = _has_position(symbol)
    if has_pos:
        return ignored("position_already_open", symbol=symbol, strategy=strategy, position_notional_usd=pos_notional)

    # Notional
    notional = float(payload.notional_usd or settings.default_notional_usd)
    if notional < float(settings.min_order_notional_usd):
        return ignored(
            "notional_below_minimum",
            symbol=symbol,
            strategy=strategy,
            notional_usd=notional,
            min_notional_usd=float(settings.min_order_notional_usd),
        )

    # Execute BUY
    px = float(_last_price(symbol))
    stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)

    # Exposure caps (0 disables)
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)
    if max_total or max_symbol:
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)
        if max_total and (total_exposure + notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd"}
        if max_symbol and (sym_exposure + notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd"}
    # Exposure caps (0 disables)
    max_total = float(getattr(settings, "max_total_exposure_usd", 0.0) or 0.0)
    max_symbol = float(getattr(settings, "max_symbol_exposure_usd", 0.0) or 0.0)
    if max_total or max_symbol:
        positions = get_positions()
        total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
        sym_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions if p.get("symbol") == symbol)
        if max_total and (total_exposure + notional) > max_total:
            log.info("Skip entry %s: total exposure cap (%.2f + %.2f > %.2f)", symbol, total_exposure, notional, max_total)
            return {"ok": True, "skipped": True, "reason": "max_total_exposure_usd"}
        if max_symbol and (sym_exposure + notional) > max_symbol:
            log.info("Skip entry %s: symbol exposure cap (%.2f + %.2f > %.2f)", symbol, sym_exposure, notional, max_symbol)
            return {"ok": True, "skipped": True, "reason": "max_symbol_exposure_usd"}
    res = _market_notional(symbol=symbol, side="buy", notional=notional, strategy=strategy, price=px)

    _log_event(
        "info",
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": "webhook",
            "status": "executed",
            "action": "buy",
            "symbol": symbol,
            "strategy": strategy,
            "signal": signal_name,
            "signal_id": signal_id,
            "price": px,
            "notional_usd": notional,
            "stop": float(stop_price),
            "take": float(take_price),
            "scanner_gate": {"allowed": True, "reason": allow_reason, **allow_meta},
            "client_ip": client_ip,
        },
    )

    state.mark_enter(symbol)
    state.plans[symbol] = TradePlan(
        symbol=symbol,
        side="buy",
        notional_usd=notional,
        entry_price=px,
        stop_price=stop_price,
        take_price=take_price,
        strategy=strategy,
        opened_ts=now.timestamp(),
    )
    n = state.inc_trade(symbol)

    return {
        "ok": True,
        "action": "buy",
        "symbol": symbol,
        "price": px,
        "stop": stop_price,
        "take": take_price,
        "trade_count_today": n,
        "signal": signal_name,
        "signal_id": signal_id,
        "scanner_gate": {"reason": allow_reason, **allow_meta},
        "result": res,
    }


@app.post("/worker/exit")
def worker_exit(payload: WorkerExitPayload):
    # Worker secret
    if settings.worker_secret:
        if not payload.worker_secret or payload.worker_secret != settings.worker_secret:
            raise HTTPException(status_code=401, detail="invalid worker secret")

    try:
        now = datetime.now(timezone.utc)
        utc_date = _utc_date_str(now)

        did_flatten = False
        if settings.enforce_daily_flatten and _is_flatten_time(now, settings.daily_flatten_time_utc) and state.last_flatten_utc_date != utc_date:
            did_flatten = True
            state.last_flatten_utc_date = utc_date

        exits = []
        bal = _balances_by_asset()

        for symbol in settings.allowed_symbols:
            base = _base_asset(symbol)
            qty = float(bal.get(base, 0.0) or 0.0)
            if qty <= 0:
                state.plans.pop(symbol, None)
                state.clear_pending_exit(symbol)
                continue

            plan = state.plans.get(symbol)
            entry_px = float(plan.entry_price) if plan else float(_last_price(symbol))
            stop_px, take_px = compute_brackets(entry_px, settings.stop_pct, settings.take_pct)

            if not plan:
                plan = TradePlan(
                    symbol=symbol,
                    side="buy",
                    notional_usd=float(settings.default_notional_usd),
                    entry_price=entry_px,
                    stop_price=stop_px,
                    take_price=take_px,
                    strategy="unknown",
                    opened_ts=now.timestamp(),
                )
                state.plans[symbol] = plan

            px = float(_last_price(symbol))
            reason = None

            if did_flatten:
                reason = "daily_flatten"
            elif px <= float(plan.stop_price):
                reason = "stop"
            elif px >= float(plan.take_price):
                reason = "take"

            if not reason:
                continue

            if not state.can_exit(symbol, int(settings.exit_cooldown_sec)):
                continue

            notional_exit = max(float(settings.exit_min_notional_usd), float(plan.notional_usd))
            res = _market_notional(symbol=symbol, side="sell", notional=notional_exit, strategy=plan.strategy, price=px)

            state.mark_exit(symbol)
            state.set_pending_exit(symbol, reason=reason, txid=None)

            evt = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "kind": "exit",
                "status": "executed",
                "symbol": symbol,
                "strategy": plan.strategy,
                "reason": reason,
                "price": px,
                "notional_usd": notional_exit,
            }
            _log_event("info", evt)
            exits.append({"symbol": symbol, "reason": reason, "price": px, "result": res})

        return {"ok": True, "utc": now.isoformat(), "did_flatten": did_flatten, "exits": exits}

    except HTTPException:
        raise
    except Exception as e:
        _log_event(
            "error",
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "kind": "worker_exit",
                "status": "error",
                "error": str(e),
            },
        )
        return JSONResponse(status_code=200, content={"ok": False, "error": str(e)})



def _entry_signals_for_symbol(symbol: str) -> tuple[dict, dict]:
    """Return (signals, debug) for the given symbol.

    signals: {"rb1": bool, "tc1": bool}
    debug:   {"rb1": {...}, "tc1": {...}}
    """
    signals: dict = {}
    debug: dict = {}

    rb1_fired, rb1_meta = _rb1_long_signal(symbol)
    signals["rb1"] = bool(rb1_fired)
    debug["rb1"] = rb1_meta

    tc1_fired, tc1_meta = _tc1_long_signal(symbol)
    signals["tc1"] = bool(tc1_fired)
    debug["tc1"] = tc1_meta

    return signals, debug



def place_entry(symbol: str, *, strategy: str, req_id: str | None = None, client_ip: str | None = None, notional: float | None = None):
    """
    Wrapper used by /worker/scan_entries to execute an entry in live mode.

    Returns a tuple: (ok, reason, meta)
      - ok: bool
      - reason: str | None (present when ok=False or executed=False)
      - meta: dict (full structured execution result)
    """
    rid = req_id or str(uuid4())
    res = _execute_long_entry(
        symbol=symbol,
        strategy=strategy,
        signal_name=strategy,
        signal_id=None,
        notional=notional,
        source="scan_entries",
        req_id=rid,
        client_ip=client_ip,
        extra={"strategy": strategy},
    )
    # _execute_long_entry returns a dict like:
    #   {"ok": True, "executed": True|False, "reason": "..."} or {"ok": False, "error": "..."}
    ok = bool(res.get("ok", False))
    reason = res.get("reason") or res.get("error")
    # If ok but not executed, treat as not-ok for scan_entries "placed" reporting.
    if ok and not res.get("executed", False):
        return False, reason or "not_executed", res
    return ok, reason, res



def _count_open_positions(open_set: set[str]) -> int:
    """Count open positions safely.

    IMPORTANT: We should not count in-memory "plans" as open positions when we have a live
    broker snapshot, or we can deadlock trading after a restart (stale plans).
    """
    return len(open_set or set())
    utc_date = _utc_date_str(now)
    try:
        state.reset_daily_counters_if_needed(utc_date)
    except Exception:
        pass

    # Global / config safety checks
    if not getattr(settings, "trading_enabled", True):
        return False, "trading_disabled", {}

    # Daily entry cap (global)
    try:
        total_today = int(getattr(state, "trades_today_total", 0) or 0)
        if MAX_ENTRIES_PER_DAY > 0 and total_today >= int(MAX_ENTRIES_PER_DAY):
            return False, "max_entries_per_day_reached", {"entries_today": total_today, "max_entries_per_day": MAX_ENTRIES_PER_DAY}
    except Exception:
        pass

    # Time-of-day discipline
    try:
        if _is_after_utc_hhmm(now, getattr(settings, "no_new_entries_after_utc", "") or ""):
            return False, "blocked_by_time_window", {"no_new_entries_after_utc": getattr(settings, "no_new_entries_after_utc", "")}
    except Exception:
        pass

    # Per-symbol trade frequency cap
    try:
        count_for_symbol = int(getattr(state, "trades_today_by_symbol", {}).get(symbol, 0) or 0)
        if getattr(settings, "max_trades_per_symbol_per_day", 999) and count_for_symbol >= int(getattr(settings, "max_trades_per_symbol_per_day", 999)):
            return False, "max_trades_per_symbol_per_day_reached", {"trades_today": count_for_symbol, "max": getattr(settings, "max_trades_per_symbol_per_day", 999)}
    except Exception:
        pass

    # Cooldown
    try:
        if not state.can_enter(symbol, getattr(settings, "entry_cooldown_sec", 0)):
            return False, "cooldown_active", {"cooldown_sec": getattr(settings, "entry_cooldown_sec", 0)}
    except Exception:
        pass

    # Execute via existing engine
    try:
        ok, reason, meta = _execute_long_entry(symbol=symbol, strategy=strategy, signal_name=strategy)
        if ok:
            try:
                state.mark_enter(symbol)
                state.inc_trade(symbol)
                # global counter (added in state.py patch)
                if hasattr(state, "inc_trade_total"):
                    state.inc_trade_total()
            except Exception:
                pass
        return bool(ok), str(reason), meta or {}
    except Exception as e:
        return False, f"exception:{type(e).__name__}", {"error": str(e)}




@app.post("/worker/reset_plans")
def worker_reset_plans(payload: WorkerScanPayload):
    """Admin endpoint to clear stale in-memory plans.
    Uses the same worker secret gate as scan_entries/exit.
    NOTE: In-memory state is per-instance; this clears only the current instance.
    """
    # Worker secret
    if settings.worker_secret:
        if not payload.worker_secret or payload.worker_secret != settings.worker_secret:
            raise HTTPException(status_code=401, detail="invalid worker secret")

    cleared = len(state.plans)
    state.plans.clear()
    return {"ok": True, "utc": utc_now_iso(), "cleared_plans": cleared}

@app.post("/worker/scan_entries")
def scan_entries(payload: WorkerScanPayload):
    """
    Scan the current universe for entry signals (RB1/TC1), optionally executing orders.

    This endpoint ALWAYS returns a rich diagnostics payload so you can see exactly why
    you got 0 entries (no signals, already in position, symbol not allowed, etc.).
    """
    ok, reason = _require_worker_secret(payload.worker_secret)
    if not ok:
        return JSONResponse(status_code=401, content={"ok": False, "utc": utc_now_iso(), "error": reason})

    # 1) Scanner → symbols (soft allowlist) + meta
    scanner_ok, scanner_reason, scanner_meta, scanner_syms = _scanner_fetch_active_symbols_and_meta()

    # 2) Universe (explicit symbols > allowed list (+ scanner if soft allow))
    universe = _build_universe(payload, scanner_syms)

    # 3) Snapshot positions once
    positions = get_positions()
    open_set = {p["symbol"] for p in positions if p.get("qty", 0) > 0}
    open_positions_count = _count_open_positions(open_set)

    # 4) Evaluate signals + build diagnostics
    per_symbol: Dict[str, Any] = {}
    candidates: List[Tuple[str, str]] = []  # (symbol, strategy)
    for sym in universe:
        d: Dict[str, Any] = {
            "symbol": sym,
            "in_position": sym in open_set,
            "signals": {},
            "eligible": False,
            "skip": [],
        }

        if sym in open_set:
            d["skip"].append("already_in_position")
            per_symbol[sym] = d
            continue

        if open_positions_count >= MAX_OPEN_POSITIONS:
            d["skip"].append("max_open_positions_reached")
            d["max_open_positions"] = MAX_OPEN_POSITIONS
            per_symbol[sym] = d
            continue

        try:
            fired, sig_debug = _entry_signals_for_symbol(sym)  # (signals, debug)
        except Exception as e:
            d["skip"].append(f"signal_error:{type(e).__name__}")
            d["signal_error"] = str(e)
            per_symbol[sym] = d
            continue

        d["signals"] = fired
        d["signal_debug"] = sig_debug

        fired_strats = [k for k, v in fired.items() if v]
        if not fired_strats:
            d["skip"].append("no_signal")
            per_symbol[sym] = d
            continue

        # If both fire, prefer RB1 (breakout) over TC1 (trend continuation)
        strategy = "rb1" if fired.get("rb1") else fired_strats[0]
        d["eligible"] = True
        d["chosen_strategy"] = strategy
        # per-scan entry cap
        if MAX_ENTRIES_PER_SCAN > 0 and len(candidates) >= MAX_ENTRIES_PER_SCAN:
            d["eligible"] = False
            d["skip"].append("max_entries_per_scan_reached")
            per_symbol[sym] = d
            continue
        candidates.append((sym, strategy))
        per_symbol[sym] = d

    # 5) Execute (or dry-run)
    results: List[Dict[str, Any]] = []
    for sym, strategy in candidates:
        if payload.dry_run:
            results.append({"symbol": sym, "strategy": strategy, "status": "dry_run"})
            continue

        ok2, reason2, meta2 = place_entry(sym, strategy=strategy)
        results.append(
            {
                "symbol": sym,
                "strategy": strategy,
                "ok": ok2,
                "status": "executed" if ok2 else "skipped",
                "reason": reason2,
                "meta": meta2,
            }
        )

    # 6) Return diagnostics
    return {
        "ok": True,
        "utc": utc_now_iso(),
        "universe_count": len(universe),
        "scanner": {
            "ok": scanner_ok,
            "reason": scanner_reason,
            "active_count": len(scanner_syms),
            "last_refresh_utc": (scanner_meta or {}).get("last_refresh_utc"),
            "last_error": (scanner_meta or {}).get("last_error"),
            "active_symbols": scanner_syms,
        },
        "results": results,
        "diagnostics": {
            "request": {
                "dry_run": payload.dry_run,
                "force_scan": getattr(payload, "force_scan", False),
                "symbols_provided": payload.symbols or [],
            },
            "config": {
                "scanner_url": SCANNER_URL,
                "scanner_soft_allow": SCANNER_SOFT_ALLOW,
                "allowed_symbols_count": len(ALLOWED_SYMBOLS),
                "allowed_symbols_sample": sorted(list(ALLOWED_SYMBOLS))[:50],
                "scanner_driven_universe": SCANNER_DRIVEN_UNIVERSE,
                "scanner_target_n": SCANNER_TARGET_N,
                "universe_usd_only": UNIVERSE_USD_ONLY,
                "prefer_usd_for_stables": UNIVERSE_PREFER_USD_FOR_STABLES,
                "filter_universe_by_allowed_symbols": FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS,
                "max_open_positions": MAX_OPEN_POSITIONS,
                "max_entries_per_scan": MAX_ENTRIES_PER_SCAN,
                "max_entries_per_day": MAX_ENTRIES_PER_DAY,
            },
            "positions_count": len(positions),
            "plans_count": len(getattr(state, "plans", {}) or {}),
            "open_positions_count": open_positions_count,
            "positions_open": sorted(list(open_set))[:50],
            "candidates": [{"symbol": s, "strategy": st} for (s, st) in candidates],
            "per_symbol": per_symbol,
        },
    }

@app.get("/dashboard")
def dashboard():
    # Dashboard is a JSON snapshot. Keep it resilient: failures here should not break the whole service.
    try:
        positions = get_positions()
    except Exception:
        positions = []

    # Safe serialization for plans/state (TradePlan is a dataclass)
    open_plans = []
    try:
        from dataclasses import asdict, is_dataclass
        for p in getattr(state, "plans", {}).values():
            open_plans.append(asdict(p) if is_dataclass(p) else p)
    except Exception:
        open_plans = []

    # Telemetry buffer is best-effort and may be absent/non-serializable
    telemetry_out = None
    try:
        telemetry_out = list(getattr(state, "telemetry", []) or [])
    except Exception:
        telemetry_out = None

    return {
        "ok": True,
        "utc": utc_now_iso(),
        "settings": {
            "max_open_positions": getattr(settings, "max_open_positions", None),
            "max_entries_per_day": getattr(settings, "max_entries_per_day", None),
            "max_entries_per_scan": getattr(settings, "max_entries_per_scan", None),
            "scanner_url": getattr(settings, "scanner_url", None),
        },
        "open_positions": positions,
        "open_plans": open_plans,
        "telemetry": telemetry_out,
    }

    t = None
    try:
        t = telemetry.snapshot()  # type: ignore[attr-defined]
    except Exception:
        try:
            t = telemetry.to_dict()  # type: ignore[attr-defined]
        except Exception:
            t = None

    return {
        "ok": True,
        "time": utc_now_iso(),
        "mode": getattr(settings, "mode", None),
        "allowed_symbols": getattr(settings, "allowed_symbols", None),
        "positions": positions,
        "open_plans": list(plans.values()) if isinstance(plans, dict) else None,
        "telemetry": t,
    }


@app.get("/performance")
def performance():
    """Very simple P&L proxy using current positions (mark-to-market).

    If you're logging fills elsewhere, you can swap this for realized P&L.
    """
    positions = get_positions()
    total_exposure = sum(float(p.get("notional_usd", 0.0) or 0.0) for p in positions)
    return {
        "ok": True,
        "time": utc_now_iso(),
        "open_positions": positions,
        "total_open_notional_usd": total_exposure,
    }
