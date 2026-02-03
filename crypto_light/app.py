from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from .broker_kraken import balances_by_asset as _balances_by_asset
from .broker_kraken import base_asset as _base_asset
from .broker_kraken import last_price as _last_price
from .broker_kraken import market_notional as _market_notional
from .config import load_settings
from .models import WebhookPayload, WorkerExitPayload
from .risk import compute_brackets
from .state import InMemoryState, TradePlan
from .symbol_map import normalize_symbol

settings = load_settings()
state = InMemoryState()
app = FastAPI(title="Crypto Light", version="1.0.0")


def _log_event(level: str, event: Dict[str, Any]) -> None:
    """Emit structured JSON logs + store best-effort telemetry in memory."""
    try:
        state.record_event(event)
    except Exception:
        pass

    try:
        msg = json.dumps(event, default=str)
    except Exception:
        msg = str(event)

    lvl = (level or "").lower()
    if lvl in ("warning", "warn"):
        print(msg)
    elif lvl in ("error",):
        print(msg)
    else:
        print(msg)


def _utc_date_str(now: Optional[datetime] = None) -> str:
    n = now or datetime.now(timezone.utc)
    return n.strftime("%Y-%m-%d")


def _is_flatten_time(now: datetime, hhmm_utc: str) -> bool:
    # hhmm_utc like "23:55"
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        h, m = 23, 55
    return (now.hour, now.minute) >= (h, m)


def _is_after_utc_hhmm(now: datetime, hhmm_utc: str) -> bool:
    """Return True if now (UTC) is after or equal to HH:MM (UTC)."""
    if not hhmm_utc:
        return False
    try:
        hh, mm = hhmm_utc.split(":", 1)
        h = int(hh)
        m = int(mm)
    except Exception:
        return False
    return (now.hour, now.minute) >= (h, m)


def _within_minutes_after_utc_hhmm(now: datetime, hhmm_utc: str, window_min: int) -> bool:
    """True if now is within [HH:MM, HH:MM+window) UTC."""
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


@app.get("/health")
def health():
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/telemetry")
def telemetry(limit: int = 100):
    lim = max(1, min(int(limit), 500))
    return {"ok": True, "count": len(state.telemetry), "items": state.telemetry[-lim:]}


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

    # Security
    if not settings.webhook_secret or payload.secret != settings.webhook_secret:
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": "webhook",
            "status": "rejected",
            "reason": "invalid_secret",
            "client_ip": client_ip,
        }
        _log_event("warning", evt)
        raise HTTPException(status_code=401, detail="invalid secret")

    try:
        symbol = _validate_symbol(payload.symbol)
    except HTTPException as e:
        evt = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "req_id": req_id,
            "kind": "webhook",
            "status": "rejected",
            "reason": "symbol_not_allowed",
            "client_ip": client_ip,
            "symbol": str(payload.symbol),
            "detail": getattr(e, "detail", None),
        }
        _log_event("warning", evt)
        raise

    side = str(payload.side).lower().strip()
    strategy = str(payload.strategy).strip()

    if side not in ("buy", "sell"):
        return ignored("invalid_side", symbol=symbol, side=side, strategy=strategy)

    if not strategy:
        return ignored("missing_strategy", symbol=symbol, side=side)

    now = datetime.now(timezone.utc)
    utc_date = _utc_date_str(now)
    state.reset_daily_counters_if_needed(utc_date)

    # Idempotency / anti-retry: ignore repeated signal_id for a short TTL
    signal_id = (payload.signal_id or "").strip()
    signal_name = (payload.signal or "").strip()
    if signal_id and state.seen_recent_signal(signal_id, int(settings.signal_dedupe_ttl_sec)):
        return ignored(
            "duplicate_signal_id",
            symbol=symbol,
            side=side,
            strategy=strategy,
            signal=signal_name or None,
            signal_id=signal_id,
            ttl_sec=int(settings.signal_dedupe_ttl_sec),
        )

    # Master kill switch
    if side == "buy" and not bool(settings.trading_enabled):
        return ignored(
            "trading_disabled",
            symbol=symbol,
            side=side,
            strategy=strategy,
            signal=signal_name or None,
            signal_id=signal_id or None,
        )

    # 24/7 by default. If NO_NEW_ENTRIES_AFTER_UTC is set (HH:MM), enforce it.
    if side == "buy" and settings.no_new_entries_after_utc and _is_after_utc_hhmm(now, settings.no_new_entries_after_utc):
        return ignored(
            "entries_disabled_after_cutoff",
            symbol=symbol,
            side=side,
            strategy=strategy,
            signal=signal_name or None,
            signal_id=signal_id or None,
            cutoff_utc=settings.no_new_entries_after_utc,
            utc=now.isoformat(),
        )

    # Daily flatten is optional. If enabled, you can block entries for a short window
    # around flatten to avoid immediate re-entries while the worker is flattening.
    if side == "buy" and settings.enforce_daily_flatten and settings.block_entries_after_flatten:
        if _within_minutes_after_utc_hhmm(now, settings.daily_flatten_time_utc, window_min=3):
            return ignored(
                "entries_blocked_during_flatten_window",
                symbol=symbol,
                side=side,
                strategy=strategy,
                signal=signal_name or None,
                signal_id=signal_id or None,
                flatten_utc=settings.daily_flatten_time_utc,
                utc=now.isoformat(),
            )

    # Entry cooldown (per symbol)
    if side == "buy" and not state.can_enter(symbol, int(settings.entry_cooldown_sec)):
        return ignored(
            "entry_cooldown_active",
            symbol=symbol,
            side=side,
            strategy=strategy,
            signal=signal_name or None,
            signal_id=signal_id or None,
            cooldown_sec=int(settings.entry_cooldown_sec),
        )

    # Trade frequency guard
    trades = int(state.trades_today_by_symbol.get(symbol, 0) or 0)
    if side == "buy" and trades >= int(settings.max_trades_per_symbol_per_day):
        return ignored(
            "max_trades_reached",
            symbol=symbol,
            side=side,
            strategy=strategy,
            trades_today=trades,
            max_trades=int(settings.max_trades_per_symbol_per_day),
        )

    # One position per symbol (spot long only)
    has_pos, _pos_notional = _has_position(symbol)
    if side == "buy" and has_pos:
        return ignored(
            "position_already_open",
            symbol=symbol,
            side=side,
            strategy=strategy,
            position_notional_usd=_pos_notional,
        )

    notional = float(payload.notional_usd or settings.default_notional_usd)
    if notional < float(settings.min_order_notional_usd):
        return ignored(
            "notional_below_minimum",
            symbol=symbol,
            side=side,
            strategy=strategy,
            notional_usd=notional,
            min_notional_usd=float(settings.min_order_notional_usd),
        )

    # Execute
    px = float(_last_price(symbol))

    if side == "sell":
        res = _market_notional(symbol=symbol, side="sell", notional=notional, strategy=strategy, price=px)
        _log_event(
            "info",
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "req_id": req_id,
                "kind": "webhook",
                "status": "executed",
                "action": "sell",
                "symbol": symbol,
                "strategy": strategy,
                "signal": (payload.signal or None),
                "signal_id": (payload.signal_id or None),
                "side": side,
                "price": px,
                "notional_usd": notional,
                "client_ip": client_ip,
            },
        )
        return {"ok": True, "action": "sell", "symbol": symbol, "price": px, "result": res}

    # BUY entry
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
            "signal": (payload.signal or None),
            "signal_id": (payload.signal_id or None),
            "side": side,
            "price": px,
            "notional_usd": notional,
            "stop": float(stop_price),
            "take": float(take_price),
            "client_ip": client_ip,
        },
    )

    # Mark entry for cooldown
    state.mark_enter(symbol)

    # Track plan in-memory (nice-to-have; broker remains source of truth)
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
        "signal": payload.signal,
        "signal_id": payload.signal_id,
        "result": res,
    }


@app.post("/worker/exit")
def worker_exit(payload: WorkerExitPayload):
    # Security
    if settings.worker_secret:
        if not payload.worker_secret or payload.worker_secret != settings.worker_secret:
            raise HTTPException(status_code=401, detail="invalid worker secret")

    try:
        now = datetime.now(timezone.utc)
        utc_date = _utc_date_str(now)

        # Daily flatten: once per UTC date (optional)
        did_flatten = False
        if settings.enforce_daily_flatten and _is_flatten_time(now, settings.daily_flatten_time_utc) and state.last_flatten_utc_date != utc_date:
            did_flatten = True
            state.last_flatten_utc_date = utc_date

        exits = []

        # Reconcile balances
        bal = _balances_by_asset()

        for symbol in settings.allowed_symbols:
            base = _base_asset(symbol)
            qty = float(bal.get(base, 0.0) or 0.0)
            if qty <= 0:
                state.plans.pop(symbol, None)
                state.clear_pending_exit(symbol)
                continue

            has_plan = symbol in state.plans
            plan = state.plans.get(symbol)

            # Determine entry price for brackets
            entry_px = float(plan.entry_price) if plan else float(_last_price(symbol))
            stop_px, take_px = compute_brackets(entry_px, settings.stop_pct, settings.take_pct)

            # Use plan if present; otherwise create an inferred plan
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

            # Exit cooldown
            if not state.can_exit(symbol, int(settings.exit_cooldown_sec)):
                continue

            # Avoid re-submitting multiple exits while a pending exit is working
            pending = state.pending_exits.get(symbol)
            if pending:
                # If pending is too old, let broker_kraken handle timeout / fallback
                pass

            # Execute exit: sell by notional (broker caps to available base)
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
