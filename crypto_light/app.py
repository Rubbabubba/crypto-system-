from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set
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
app = FastAPI(title="Crypto Light", version="1.0.2")


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

# TC1 params
TC1_LTF_EMA = int(float(os.getenv("TC1_LTF_EMA", "20") or 20))
TC1_HTF_TIMEFRAME = os.getenv("TC1_HTF_TIMEFRAME", "60Min").strip() or "60Min"
TC1_HTF_LIMIT_BARS = int(float(os.getenv("TC1_HTF_LIMIT_BARS", "300") or 300))
TC1_HTF_FAST = int(float(os.getenv("TC1_HTF_FAST", "50") or 50))
TC1_HTF_SLOW = int(float(os.getenv("TC1_HTF_SLOW", "200") or 200))
TC1_RECLAIM_BUFFER_PCT = float(os.getenv("TC1_RECLAIM_BUFFER_PCT", "0.0005") or 0.0005)  # 0.05%


# ---------- Scanner config (soft allow) ----------
SCANNER_URL = os.getenv("SCANNER_URL", "").strip()
SCANNER_REFRESH_SEC = int(float(os.getenv("SCANNER_REFRESH_SEC", "300") or 300))
SCANNER_SOFT_ALLOW = (os.getenv("SCANNER_SOFT_ALLOW", "1").strip().lower() in ("1", "true", "yes", "on"))
SCANNER_TIMEOUT_SEC = float(os.getenv("SCANNER_TIMEOUT_SEC", "10") or 10)

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
    """Return currently-open positions.

    This 'crypto_light' build doesn't integrate with a broker/exchange yet.
    The scanner uses positions only to avoid emitting entry signals for symbols
    you already hold. For now we default to an empty list.

    Expected return format (when implemented):
        [{"symbol": "BTCUSD", "qty": 0.25}, ...]
    """
    return []

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
    fired = (prev_close <= level) and (cur_close > level)

    meta = {"range_high": range_high, "level": level, "prev_close": prev_close, "close": cur_close, "bar_ts": ts[-1]}
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
    uptrend = (ema_fast[-1] > ema_slow[-1]) and (ema_fast[-1] > ema_fast[-2])

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
        "htf_fast": ema_fast[-1],
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
    - symbols: list[str] of normalized symbols (e.g., 'ETH/USDT')

    Never raises.
    """
    url = getattr(settings, 'scanner_url', None) or os.getenv('SCANNER_URL', '').strip()
    meta: dict = {'scanner_url': url}
    if not url:
        return False, 'missing_scanner_url', meta, []
    try:
        r = requests.get(url, timeout=10)
        meta['status_code'] = r.status_code
        meta['elapsed_ms'] = int(getattr(getattr(r, 'elapsed', None), 'total_seconds', lambda: 0)() * 1000)
        if r.status_code != 200:
            # try to include short body for diagnostics
            body = (r.text or '')
            meta['body_snippet'] = body[:500]
            return False, f'http_{r.status_code}', meta, []
        data = r.json()
        # expected keys: ok, active_symbols, meta, last_error, last_refresh_utc
        meta['scanner_ok'] = bool(data.get('ok'))
        meta['last_error'] = data.get('last_error')
        meta['last_refresh_utc'] = data.get('last_refresh_utc')
        if isinstance(data.get('meta'), dict):
            meta['scanner_meta'] = data.get('meta')
        syms = data.get('active_symbols')
        if not isinstance(syms, list):
            return False, 'invalid_active_symbols', meta, []
        # normalize/clean
        clean: list[str] = []
        for s in syms:
            if not isinstance(s, str):
                continue
            s2 = s.strip().upper()
            if not s2 or '/' not in s2:
                continue
            clean.append(s2)
        return True, None, meta, clean
    except Exception as e:
        meta['error'] = f'{type(e).__name__}: {e}'
        return False, 'exception', meta, []
def _build_universe(payload, scanner_syms: list[str]) -> list[str]:
    """Build the scan universe safely.

    Priority:
      1) Explicit payload.symbols (if provided and non-empty)
      2) Scanner symbols (scanner_syms)
      3) Fallback to settings.allowed_symbols

    Applies allow-list unless payload.force_scan is truthy AND SCANNER_SOFT_ALLOW is enabled.
    Never raises.
    """
    try:
        requested = getattr(payload, "symbols", None) or []
        force_scan = bool(getattr(payload, "force_scan", False))
    except Exception:
        requested, force_scan = [], False

    def _norm_list(items):
        out = []
        seen = set()
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

    if requested_norm:
        universe = requested_norm
    elif scanner_norm:
        universe = scanner_norm
    else:
        universe = _norm_list(getattr(settings, "allowed_symbols", []) or [])

    allowed = set(getattr(settings, "allowed_symbols", []) or [])
    if allowed and not (force_scan and SCANNER_SOFT_ALLOW):
        universe = [s for s in universe if s in allowed]

    if not universe and allowed:
        # stable de-dupe
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
    return {
        "ok": True,
        "utc": datetime.now(timezone.utc).isoformat(),
        "scanner_url": SCANNER_URL or None,
        "scanner_ok": bool(_SCANNER_CACHE.get("ok")),
        "scanner_active_count": len(_SCANNER_CACHE.get("active_symbols") or set()),
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

    state.mark_enter(symbol)
    state.plans[symbol] = TradePlan(
        symbol=symbol,
        strategy=strategy,
        side="buy",
        entry_price=float(px),
        stop=float(stop_price),
        take=float(take_price),
        notional_usd=float(_notional),
        opened_ts=time.time(),
    )

    return {"ok": True, "executed": True, "symbol": symbol, "strategy": strategy, "price": px, "stop": float(stop_price), "take": float(take_price)}

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
    universe = payload.get("universe") or scanner_syms or []
    universe = [str(s).upper().strip() for s in universe if str(s).strip()]
    # Deduplicate while preserving order
    seen = set()
    universe = [s for s in universe if not (s in seen or seen.add(s))]

    # 3) Snapshot positions once
    positions = get_positions()
    open_set = {p["symbol"] for p in positions if p.get("qty", 0) > 0}

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

        try:
            fired = _entry_signals_for_symbol(sym)  # {"rb1": bool, "tc1": bool, ...}
        except Exception as e:
            d["skip"].append(f"signal_error:{type(e).__name__}")
            d["signal_error"] = str(e)
            per_symbol[sym] = d
            continue

        d["signals"] = fired

        fired_strats = [k for k, v in fired.items() if v]
        if not fired_strats:
            d["skip"].append("no_signal")
            per_symbol[sym] = d
            continue

        # If both fire, prefer RB1 (breakout) over TC1 (trend continuation)
        strategy = "rb1" if fired.get("rb1") else fired_strats[0]
        d["eligible"] = True
        d["chosen_strategy"] = strategy
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
            },
            "positions_count": len(positions),
            "positions_open": sorted(list(open_set))[:50],
            "candidates": [{"symbol": s, "strategy": st} for (s, st) in candidates],
            "per_symbol": per_symbol,
        },
    }
