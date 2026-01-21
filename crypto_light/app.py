from __future__ import annotations

import logging
from datetime import datetime, timezone
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request

from .config import load_settings
from .models import WebhookPayload, WorkerExitPayload
from .state import InMemoryState, TradePlan
from . import broker
from .risk import compute_brackets

settings = load_settings()
logging.basicConfig(level=getattr(logging, settings.log_level.upper(), logging.INFO))
log = logging.getLogger("crypto_light")

app = FastAPI(title="Crypto System - Light", version="0.1.0")
state = InMemoryState()


def _log_event(level: str, event: dict) -> None:
    """Unified telemetry: in-memory buffer + structured log line."""
    state.record_event(event)
    msg = f"telemetry {event}"  # keep it simple; Render log search is easy
    if level == "warning":
        log.warning(msg)
    elif level == "error":
        log.error(msg)
    else:
        log.info(msg)


def _utc_date_str(now: datetime | None = None) -> str:
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


def _validate_symbol(symbol: str) -> str:
    sym = str(symbol).upper().strip()
    if sym not in set(settings.allowed_symbols):
        raise HTTPException(status_code=400, detail=f"symbol not allowed: {sym}")
    return sym


def _has_position(symbol: str) -> tuple[bool, float]:
    bal = broker.balances_by_asset()
    base = broker.base_asset(symbol)
    qty = float(bal.get(base, 0.0) or 0.0)
    if qty <= 0:
        return False, 0.0
    try:
        px = broker.last_price(symbol)
        notional = qty * px
    except Exception:
        notional = 0.0
    # only count as a position if it's above exit_min_notional_usd
    return notional >= float(settings.exit_min_notional_usd), notional


@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "crypto-system-light",
        "allowed_symbols": settings.allowed_symbols,
        "daily_flatten_time_utc": settings.daily_flatten_time_utc,
    }


@app.get("/telemetry")
def telemetry(limit: int = 100):
    """Best-effort recent events (non-persistent)."""
    lim = max(1, min(int(limit), 500))
    return {"ok": True, "count": min(lim, len(state.telemetry)), "events": state.telemetry[-lim:]}


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

    # Discipline: hard block new entries after flatten time
    if _is_flatten_time(now, settings.daily_flatten_time_utc) and side == "buy":
        return ignored(
            "entries_disabled_after_flatten_time",
            symbol=symbol,
            side=side,
            strategy=strategy,
            utc=now.isoformat(),
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
    px = float(broker.last_price(symbol))

    if side == "sell":
        # Sell means: close existing spot position (sell by notional; broker caps to available base)
        res = broker.market_notional(symbol=symbol, side="sell", notional=notional, strategy=strategy, price=px)
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
                "side": side,
                "price": px,
                "notional_usd": notional,
                "client_ip": client_ip,
            },
        )
        return {"ok": True, "action": "sell", "symbol": symbol, "price": px, "result": res}

    # BUY entry
    stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)
    res = broker.market_notional(symbol=symbol, side="buy", notional=notional, strategy=strategy, price=px)

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
            "side": side,
            "price": px,
            "notional_usd": notional,
            "stop": float(stop_price),
            "take": float(take_price),
            "client_ip": client_ip,
        },
    )

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

        # Daily flatten: once per UTC date
        did_flatten = False
        if _is_flatten_time(now, settings.daily_flatten_time_utc) and state.last_flatten_utc_date != utc_date:
            did_flatten = True
            state.last_flatten_utc_date = utc_date

        exits = []

        # Reconcile current balances
        bal = broker.balances_by_asset()

        for symbol in settings.allowed_symbols:
            base = broker.base_asset(symbol)
            qty = float(bal.get(base, 0.0) or 0.0)
            if qty <= 0:
                # clean up stale plan
                state.clear_pending_exit(symbol)
                state.plans.pop(symbol, None)
                continue

            px = float(broker.last_price(symbol))
            notional = qty * px
            if notional < float(settings.exit_min_notional_usd):
                # if we had a pending exit and we're now effectively flat, clear it
                if symbol in state.pending_exits:
                    state.clear_pending_exit(symbol)
                    state.plans.pop(symbol, None)
                continue

            # Pending exit workflow: if we've already submitted a limit exit,
            # give it time to fill. If it hasn't filled within a timeout, cancel
            # (best-effort) and fall back to a market exit for the remaining size.
            pending = state.pending_exits.get(symbol)
            if pending:
                age = float(now.timestamp()) - float(pending.get("ts") or now.timestamp())
                if age >= float(settings.stop_limit_timeout_sec):
                    txid = pending.get("txid")
                    cancel_res = None
                    if txid:
                        cancel_res = broker.cancel_order(str(txid))
                    res = broker.market_notional(symbol=symbol, side="sell", notional=notional, strategy="stop_fallback", price=px)
                    state.mark_exit(symbol)
                    state.clear_pending_exit(symbol)
                    state.plans.pop(symbol, None)
                    exits.append({"symbol": symbol, "reason": "stop_fallback_market", "notional": notional, "price": px, "result": res})
                    _log_event(
                        "info",
                        {
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "kind": "exit",
                            "status": "executed",
                            "reason": "stop_fallback_market",
                            "symbol": symbol,
                            "price": px,
                            "notional_usd": notional,
                            "txid": txid,
                            "cancel": cancel_res,
                        },
                    )
                # either way, don't submit more exits this tick
                continue

            # If daily flatten: exit everything
            if did_flatten:
                if not state.can_exit(symbol, settings.exit_cooldown_sec):
                    continue
                res = broker.market_notional(symbol=symbol, side="sell", notional=notional, strategy="flatten", price=px)
                state.mark_exit(symbol)
                state.plans.pop(symbol, None)
                exits.append({"symbol": symbol, "reason": "daily_flatten", "notional": notional, "price": px, "result": res})
                _log_event(
                    "info",
                    {
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "kind": "exit",
                        "status": "executed",
                        "reason": "daily_flatten",
                        "symbol": symbol,
                        "price": px,
                        "notional_usd": notional,
                    },
                )
                continue

            # Otherwise: bracket exits (only if we have an in-memory plan)
            plan = state.plans.get(symbol)
            if not plan:
                continue

            if not state.can_exit(symbol, settings.exit_cooldown_sec):
                continue

            if px <= float(plan.stop_price):
                # Stop triggered: prefer a limit sell slightly below the stop to cap slippage.
                # If it doesn't fill within STOP_LIMIT_TIMEOUT_SEC, we'll cancel (best-effort)
                # and market out the remaining size on the next worker tick.
                stop_price = float(plan.stop_price)
                buffer_pct = float(settings.stop_limit_buffer_pct)
                limit_price = stop_price * (1.0 - buffer_pct / 100.0)

                res = broker.limit_notional(
                    symbol=symbol,
                    side="sell",
                    notional=notional,
                    limit_price=limit_price,
                    strategy=plan.strategy,
                    price=px,
                )

                txid = None
                try:
                    txid = res.get("txid")
                except Exception:
                    txid = None

                if res.get("ok") is False or ("error" in res and res.get("error")):
                    # If Kraken rejects the limit (rare but possible), fall back immediately.
                    res2 = broker.market_notional(symbol=symbol, side="sell", notional=notional, strategy=plan.strategy, price=px)
                    state.mark_exit(symbol)
                    state.plans.pop(symbol, None)
                    exits.append({"symbol": symbol, "reason": "stop_market_fallback", "entry": plan.entry_price, "price": px, "result": res2})
                    _log_event(
                        "info",
                        {
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "kind": "exit",
                            "status": "executed",
                            "reason": "stop_market_fallback",
                            "symbol": symbol,
                            "entry": float(plan.entry_price),
                            "price": px,
                            "stop": stop_price,
                            "limit": float(limit_price),
                            "notional_usd": notional,
                            "strategy": plan.strategy,
                            "limit_result": res,
                            "market_result": res2,
                        },
                    )
                else:
                    # Track pending exit; reconcile balances on subsequent ticks.
                    state.set_pending_exit(symbol, reason="stop_limit", txid=txid)
                    state.mark_exit(symbol)
                    exits.append({"symbol": symbol, "reason": "stop_limit_placed", "entry": plan.entry_price, "price": px, "limit": limit_price, "txid": txid, "result": res})
                    _log_event(
                        "info",
                        {
                            "ts": datetime.now(timezone.utc).isoformat(),
                            "kind": "exit",
                            "status": "placed",
                            "reason": "stop_limit",
                            "symbol": symbol,
                            "entry": float(plan.entry_price),
                            "price": px,
                            "stop": stop_price,
                            "limit": float(limit_price),
                            "notional_usd": notional,
                            "strategy": plan.strategy,
                            "txid": txid,
                        },
                    )
            elif px >= float(plan.take_price):
                res = broker.market_notional(symbol=symbol, side="sell", notional=notional, strategy=plan.strategy, price=px)
                state.mark_exit(symbol)
                state.plans.pop(symbol, None)
                exits.append({"symbol": symbol, "reason": "take", "entry": plan.entry_price, "price": px, "result": res})
                _log_event(
                    "info",
                    {
                        "ts": datetime.now(timezone.utc).isoformat(),
                        "kind": "exit",
                        "status": "executed",
                        "reason": "take",
                        "symbol": symbol,
                        "entry": float(plan.entry_price),
                        "price": px,
                        "take": float(plan.take_price),
                        "notional_usd": notional,
                        "strategy": plan.strategy,
                    },
                )

        return {"ok": True, "utc": now.isoformat(), "did_flatten": did_flatten, "exits": exits}
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
        return {"ok": False, "error": str(e)}
