from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set, Tuple
from uuid import uuid4

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .broker import balances_by_asset as _balances_by_asset
from .broker import base_asset as _base_asset
from .broker import last_price as _last_price
from .broker import market_notional as _market_notional
from .config import load_settings
from .models import WebhookPayload, WorkerExitPayload
from .risk import compute_brackets
from .state import InMemoryState, TradePlan
from .symbol_map import normalize_symbol

settings = load_settings()
state = InMemoryState()
app = FastAPI(title="Crypto Light", version="1.1.0")

# ---------- Scanner config ----------
SCANNER_URL = os.getenv("SCANNER_URL", "").strip()
SCANNER_REFRESH_SEC = int(float(os.getenv("SCANNER_REFRESH_SEC", "300") or 300))
SCANNER_SOFT_ALLOW = (os.getenv("SCANNER_SOFT_ALLOW", "1").strip().lower() in ("1", "true", "yes", "on"))
SCANNER_TIMEOUT_SEC = float(os.getenv("SCANNER_TIMEOUT_SEC", "10") or 10)

# Expansion behavior:
# - If true and scanner is healthy: allow ANY base that scanner selects (do not intersect with ALLOWED_SYMBOLS)
ALLOW_SCANNER_NEW_SYMBOLS = os.getenv("ALLOW_SCANNER_NEW_SYMBOLS", "1").strip().lower() in ("1", "true", "yes", "on")

# If true: bypass ALLOWED_SYMBOLS entirely (still gated by scanner if scanner is healthy)
ALLOW_ANY_SYMBOL = os.getenv("ALLOW_ANY_SYMBOL", "0").strip().lower() in ("1", "true", "yes", "on")

_SCANNER_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "ok": False,
    "last_error": None,
    "raw": None,
    # new format
    "active_bases": set(),             # type: Set[str]    e.g., {"ETH","ADA"}
    "best_symbol_by_base": {},         # type: Dict[str,str] e.g., {"ETH":"ETH/USDT"}
    # backwards compat
    "active_symbols": set(),           # type: Set[str]    e.g., {"ETH/USDT","ADA/USD"}
}


def _log_event(level: str, event: Dict[str, Any]) -> None:
    event = dict(event)
    event.setdefault("level", level)
    event.setdefault("utc", datetime.now(timezone.utc).isoformat())
    try:
        state.record_event(event)
    except Exception:
        pass
    try:
        print(json.dumps(event, default=str))
    except Exception:
        print(str(event))


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


def _normalize_allowed_symbols(raw: list[str]) -> Set[str]:
    out: Set[str] = set()
    for s in raw or []:
        try:
            out.add(normalize_symbol(s))
        except Exception:
            continue
    return out


_ALLOWED_SYMBOLS: Set[str] = _normalize_allowed_symbols(settings.allowed_symbols)


def _base_from_symbol(ui_symbol: str) -> str:
    base, _quote = ui_symbol.split("/", 1)
    return base.strip().upper()


def _scanner_should_refresh() -> bool:
    if not SCANNER_URL:
        return False
    return (time.time() - float(_SCANNER_CACHE["ts"] or 0.0)) >= float(SCANNER_REFRESH_SEC)


def _scanner_refresh() -> None:
    if not SCANNER_URL:
        return

    try:
        r = requests.get(SCANNER_URL, timeout=SCANNER_TIMEOUT_SEC)
        r.raise_for_status()
        j = r.json()

        active_bases: Set[str] = set()
        best_symbol_by_base: Dict[str, str] = {}
        active_symbols: Set[str] = set()

        # NEW expected (scanner v2): {"active_bases":[...], "best_symbol_by_base":{...}}
        if isinstance(j.get("active_bases"), list):
            for b in j.get("active_bases") or []:
                if not b:
                    continue
                active_bases.add(str(b).strip().upper())

        if isinstance(j.get("best_symbol_by_base"), dict):
            for b, sym in (j.get("best_symbol_by_base") or {}).items():
                if not b or not sym:
                    continue
                b_up = str(b).strip().upper()
                try:
                    best_symbol_by_base[b_up] = normalize_symbol(str(sym))
                except Exception:
                    continue

        # BACK-COMPAT: older scanner: {"active_symbols":[...]}
        # If provided, we will derive bases and normalize symbols.
        if isinstance(j.get("active_symbols"), list) or isinstance(j.get("active_coins"), list):
            raw_syms = j.get("active_symbols") or j.get("active_coins") or []
            for s in raw_syms:
                try:
                    ui = normalize_symbol(str(s))
                    active_symbols.add(ui)
                    active_bases.add(_base_from_symbol(ui))
                except Exception:
                    continue

        ok = bool(j.get("ok", True))
        # Treat "healthy" only if we have bases (new) or symbols (old)
        healthy = ok and (len(active_bases) > 0 or len(active_symbols) > 0)

        _SCANNER_CACHE["ts"] = time.time()
        _SCANNER_CACHE["ok"] = bool(healthy)
        _SCANNER_CACHE["last_error"] = None
        _SCANNER_CACHE["raw"] = j
        _SCANNER_CACHE["active_bases"] = active_bases
        _SCANNER_CACHE["best_symbol_by_base"] = best_symbol_by_base
        _SCANNER_CACHE["active_symbols"] = active_symbols

    except Exception as e:
        _SCANNER_CACHE["ts"] = time.time()
        _SCANNER_CACHE["ok"] = False
        _SCANNER_CACHE["last_error"] = str(e)


def _scanner_meta() -> Dict[str, Any]:
    ok = bool(_SCANNER_CACHE.get("ok"))
    err = _SCANNER_CACHE.get("last_error")
    bases = _SCANNER_CACHE.get("active_bases") or set()
    syms = _SCANNER_CACHE.get("active_symbols") or set()
    return {
        "scanner_url": SCANNER_URL,
        "scanner_ok": ok,
        "scanner_error": err,
        "scanner_active_base_count": len(bases),
        "scanner_active_symbol_count": len(syms),
        "scanner_soft_allow": SCANNER_SOFT_ALLOW,
    }


def _base_allowed_by_scanner(base: str) -> Tuple[bool, str, Dict[str, Any]]:
    """
    Base-level gating:
      - If scanner disabled => allow
      - If scanner down/empty => allow if soft_allow else block
      - If scanner healthy => allow only if base in active_bases
    """
    if not SCANNER_URL:
        return True, "scanner_disabled", _scanner_meta()

    if _scanner_should_refresh():
        _scanner_refresh()

    meta = _scanner_meta()
    ok = bool(_SCANNER_CACHE.get("ok"))
    bases: Set[str] = _SCANNER_CACHE.get("active_bases") or set()

    if not ok or len(bases) == 0:
        if SCANNER_SOFT_ALLOW:
            return True, "scanner_fallback_allow", meta
        return False, "scanner_unavailable_block", meta

    if base in bases:
        return True, "scanner_active_allow", meta

    return False, "base_not_in_active_set", meta


def _resolve_symbol_scanner_quote(base: str, requested_ui_symbol: str) -> Tuple[str, Dict[str, Any]]:
    """
    scanner_quote behavior:
      - If scanner provides best_symbol_by_base[BASE], execute that UI symbol.
      - Else execute the requested symbol.
    """
    chosen = requested_ui_symbol
    info: Dict[str, Any] = {"requested_symbol": requested_ui_symbol}

    best_map: Dict[str, str] = _SCANNER_CACHE.get("best_symbol_by_base") or {}
    best = best_map.get(base)
    if best:
        chosen = best
        info["scanner_best_symbol"] = best

    info["executed_symbol"] = chosen
    info["used_scanner_quote"] = bool(best)
    return chosen, info


def _allowed_by_static_allowlist(ui_symbol: str) -> Tuple[bool, str]:
    """
    Static allowlist check (from ALLOWED_SYMBOLS).
    Can be bypassed with ALLOW_ANY_SYMBOL or (scanner healthy + ALLOW_SCANNER_NEW_SYMBOLS).
    """
    if ALLOW_ANY_SYMBOL:
        return True, "allow_any_symbol_enabled"

    # If allowlist empty, treat as allow-all (useful if you intentionally clear ALLOWED_SYMBOLS)
    if len(_ALLOWED_SYMBOLS) == 0:
        return True, "allowlist_empty_allow_all"

    if ui_symbol in _ALLOWED_SYMBOLS:
        return True, "allowlist_allow"

    return False, "not_in_allowed_symbols"


@app.post("/webhook")
async def webhook(req: Request):
    """
    TradingView webhook entry.
    Payload example:
    {
      "secret":"...",
      "symbol":"BTC/USD",
      "side":"buy",
      "strategy":"rb1",
      "signal":"range_breakout_edge",
      "signal_id":"BTCUSD_5m_rb1_long"
    }
    """
    rid = str(uuid4())
    now = datetime.now(timezone.utc)

    try:
        body = await req.body()
        payload = WebhookPayload.model_validate_json(body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"invalid_payload: {e}")

    # Secret check
    if payload.secret != settings.webhook_secret:
        _log_event("WARN", {"event": "webhook_ignored", "rid": rid, "reason": "bad_secret"})
        raise HTTPException(status_code=401, detail="unauthorized")

    if not settings.trading_enabled:
        _log_event("INFO", {"event": "webhook_ignored", "rid": rid, "reason": "trading_disabled"})
        return {"ok": True, "ignored": True, "reason": "trading_disabled"}

    # Normalize symbol (incoming may be BTCUSD, KRAKEN:BTCUSD, BTC/USD, etc.)
    try:
        requested_ui_symbol = normalize_symbol(payload.symbol)
    except Exception as e:
        _log_event("WARN", {"event": "webhook_ignored", "rid": rid, "reason": "bad_symbol", "symbol": payload.symbol, "err": str(e)})
        return {"ok": True, "ignored": True, "reason": "bad_symbol"}

    base = _base_from_symbol(requested_ui_symbol)

    # Scanner base gating
    allowed_scanner, scanner_reason, scanner_meta = _base_allowed_by_scanner(base)
    if not allowed_scanner:
        _log_event("INFO", {
            "event": "webhook_ignored",
            "rid": rid,
            "reason": scanner_reason,
            "base": base,
            "symbol": requested_ui_symbol,
            "scanner": scanner_meta,
        })
        return {"ok": True, "ignored": True, "reason": scanner_reason, "scanner": scanner_meta}

    # Decide which symbol to execute (scanner_quote behavior)
    exec_ui_symbol, exec_sym_meta = _resolve_symbol_scanner_quote(base, requested_ui_symbol)

    # Static allowlist behavior:
    # - If scanner is healthy and ALLOW_SCANNER_NEW_SYMBOLS => do NOT intersect with ALLOWED_SYMBOLS
    # - Else apply allowlist unless ALLOW_ANY_SYMBOL is enabled
    scanner_is_healthy = bool(_SCANNER_CACHE.get("ok")) and len((_SCANNER_CACHE.get("active_bases") or set())) > 0
    if not (scanner_is_healthy and ALLOW_SCANNER_NEW_SYMBOLS):
        allow_ok, allow_reason = _allowed_by_static_allowlist(exec_ui_symbol)
        if not allow_ok:
            _log_event("INFO", {
                "event": "webhook_ignored",
                "rid": rid,
                "reason": allow_reason,
                "symbol": exec_ui_symbol,
                "requested_symbol": requested_ui_symbol,
                "base": base,
            })
            return {"ok": True, "ignored": True, "reason": allow_reason}

    # No-new-entries cutoff ("" means 24/7)
    if payload.side.lower() == "buy" and settings.no_new_entries_after_utc:
        if _is_after_utc_hhmm(now, settings.no_new_entries_after_utc):
            _log_event("INFO", {"event": "webhook_ignored", "rid": rid, "reason": "no_new_entries_after_cutoff", "cutoff": settings.no_new_entries_after_utc})
            return {"ok": True, "ignored": True, "reason": "no_new_entries_after_cutoff"}

    # Daily flatten window block (optional)
    if payload.side.lower() == "buy" and settings.enforce_daily_flatten and settings.block_entries_after_flatten:
        if _is_after_utc_hhmm(now, settings.daily_flatten_time_utc):
            _log_event("INFO", {"event": "webhook_ignored", "rid": rid, "reason": "blocked_after_flatten_time", "flatten_time": settings.daily_flatten_time_utc})
            return {"ok": True, "ignored": True, "reason": "blocked_after_flatten_time"}

    # One position per symbol (base-level protection still via state positions)
    if payload.side.lower() == "buy" and state.has_open_position(exec_ui_symbol):
        _log_event("INFO", {"event": "webhook_ignored", "rid": rid, "reason": "position_already_open", "symbol": exec_ui_symbol})
        return {"ok": True, "ignored": True, "reason": "position_already_open"}

    # Cooldown per symbol
    if payload.side.lower() == "buy":
        ok_cd, remaining = state.entry_cooldown_ok(exec_ui_symbol, settings.entry_cooldown_sec)
        if not ok_cd:
            _log_event("INFO", {"event": "webhook_ignored", "rid": rid, "reason": "entry_cooldown", "symbol": exec_ui_symbol, "remaining_sec": remaining})
            return {"ok": True, "ignored": True, "reason": "entry_cooldown", "remaining_sec": remaining}

    # Per-day trade cap (default 999)
    if payload.side.lower() == "buy":
        day = _utc_date_str(now)
        if not state.trades_per_day_ok(exec_ui_symbol, day, settings.max_trades_per_symbol_per_day):
            _log_event("INFO", {"event": "webhook_ignored", "rid": rid, "reason": "max_trades_per_day", "symbol": exec_ui_symbol, "day": day})
            return {"ok": True, "ignored": True, "reason": "max_trades_per_day"}

    # Dedupe by signal_id (anti-spam)
    if payload.signal_id:
        if not state.signal_dedupe_ok(payload.signal_id, settings.signal_dedupe_ttl_sec):
            _log_event("INFO", {"event": "webhook_ignored", "rid": rid, "reason": "signal_dedupe", "signal_id": payload.signal_id})
            return {"ok": True, "ignored": True, "reason": "signal_dedupe"}

    # Build trade plan (stop/take)
    try:
        px = float(_last_price(exec_ui_symbol))
        brackets = compute_brackets(
            side=payload.side.lower(),
            entry_price=px,
            stop_pct=settings.stop_pct,
            take_pct=settings.take_pct,
        )
    except Exception as e:
        _log_event("ERROR", {"event": "webhook_error", "rid": rid, "reason": "price_or_brackets_failed", "symbol": exec_ui_symbol, "err": str(e)})
        return JSONResponse(status_code=500, content={"ok": False, "error": "price_or_brackets_failed"})

    # Place market order for default notional
    try:
        order = _market_notional(
            symbol=exec_ui_symbol,
            side=payload.side.lower(),
            notional=float(settings.default_notional_usd),
            strategy=payload.strategy,
            price=None,
        )
    except Exception as e:
        _log_event("ERROR", {"event": "webhook_error", "rid": rid, "reason": "order_failed", "symbol": exec_ui_symbol, "err": str(e)})
        return JSONResponse(status_code=500, content={"ok": False, "error": "order_failed", "detail": str(e)})

    # Track plan in memory
    plan = TradePlan(
        symbol=exec_ui_symbol,
        side=payload.side.lower(),
        strategy=payload.strategy,
        signal=payload.signal,
        signal_id=payload.signal_id,
        entry_time_utc=now.isoformat(),
        entry_price=px,
        stop_price=brackets["stop"],
        take_price=brackets["take"],
        qty=float(order.get("qty") or 0.0),
        order_id=str(order.get("order_id") or ""),
    )
    state.set_plan(exec_ui_symbol, plan)

    # Mark trade count + cooldown
    day = _utc_date_str(now)
    if payload.side.lower() == "buy":
        state.bump_trade_count(exec_ui_symbol, day)
        state.set_last_entry(exec_ui_symbol, time.time())

    _log_event("INFO", {
        "event": "webhook_executed",
        "rid": rid,
        "symbol": exec_ui_symbol,
        "requested_symbol": requested_ui_symbol,
        "base": base,
        "side": payload.side.lower(),
        "strategy": payload.strategy,
        "signal": payload.signal,
        "signal_id": payload.signal_id,
        "order": order,
        "plan": plan.model_dump(),
        "scanner": scanner_meta,
        "scanner_quote": exec_sym_meta,
    })

    return {
        "ok": True,
        "executed": True,
        "symbol": exec_ui_symbol,
        "requested_symbol": requested_ui_symbol,
        "scanner": scanner_meta,
        "scanner_quote": exec_sym_meta,
        "order": order,
        "plan": plan.model_dump(),
    }


@app.post("/worker/exit")
async def worker_exit(req: Request):
    """
    Background exit runner calls this endpoint.
    """
    rid = str(uuid4())
    try:
        body = await req.body()
        payload = WorkerExitPayload.model_validate_json(body)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"invalid_payload: {e}")

    if payload.worker_secret != settings.worker_secret:
        raise HTTPException(status_code=401, detail="unauthorized")

    # Delegate to existing state logic
    try:
        res = state.run_exit_cycle(
            balances_by_asset=_balances_by_asset,
            base_asset=_base_asset,
            last_price=_last_price,
            market_notional=_market_notional,
            settings=settings,
        )
        return {"ok": True, "rid": rid, "result": res}
    except Exception as e:
        _log_event("ERROR", {"event": "worker_exit_error", "rid": rid, "err": str(e)})
        return {"ok": False, "rid": rid, "error": str(e)}


@app.get("/telemetry")
def telemetry():
    return {"ok": True, "events": state.get_events(limit=250)}


@app.get("/health")
def health():
    # light scanner refresh for visibility only
    if SCANNER_URL and _scanner_should_refresh():
        _scanner_refresh()

    return {
        "ok": True,
        "utc": datetime.now(timezone.utc).isoformat(),
        "scanner": _scanner_meta(),
        "allow_any_symbol": ALLOW_ANY_SYMBOL,
        "allow_scanner_new_symbols": ALLOW_SCANNER_NEW_SYMBOLS,
        "allowed_symbols_count": len(_ALLOWED_SYMBOLS),
    }
