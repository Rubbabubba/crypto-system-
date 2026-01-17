from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException

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


@app.post("/webhook")
def webhook(payload: WebhookPayload):
    # Security
    if not settings.webhook_secret or payload.secret != settings.webhook_secret:
        raise HTTPException(status_code=401, detail="invalid secret")

    symbol = _validate_symbol(payload.symbol)
    side = str(payload.side).lower().strip()
    strategy = str(payload.strategy).strip()

    if side not in ("buy", "sell"):
        raise HTTPException(status_code=400, detail="side must be buy or sell")

    if not strategy:
        raise HTTPException(status_code=400, detail="strategy is required")

    now = datetime.now(timezone.utc)
    utc_date = _utc_date_str(now)
    state.reset_daily_counters_if_needed(utc_date)

    # Discipline: hard block new entries after flatten time
    if _is_flatten_time(now, settings.daily_flatten_time_utc) and side == "buy":
        raise HTTPException(status_code=400, detail="entries disabled after daily flatten time")

    # Trade frequency guard
    trades = int(state.trades_today_by_symbol.get(symbol, 0) or 0)
    if side == "buy" and trades >= int(settings.max_trades_per_symbol_per_day):
        raise HTTPException(status_code=400, detail=f"max trades reached for {symbol} today")

    # One position per symbol (spot long only)
    has_pos, _pos_notional = _has_position(symbol)
    if side == "buy" and has_pos:
        raise HTTPException(status_code=400, detail=f"position already open for {symbol}")

    notional = float(payload.notional_usd or settings.default_notional_usd)
    if notional < float(settings.min_order_notional_usd):
        raise HTTPException(status_code=400, detail=f"notional below minimum: {notional}")

    # Execute
    px = float(broker.last_price(symbol))

    if side == "sell":
        # Sell means: close existing spot position (sell by notional; broker caps to available base)
        res = broker.market_notional(symbol=symbol, side="sell", notional=notional, strategy=strategy, price=px)
        return {"ok": True, "action": "sell", "symbol": symbol, "price": px, "result": res}

    # BUY entry
    stop_price, take_price = compute_brackets(px, settings.stop_pct, settings.take_pct)
    res = broker.market_notional(symbol=symbol, side="buy", notional=notional, strategy=strategy, price=px)

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
            state.plans.pop(symbol, None)
            continue

        px = float(broker.last_price(symbol))
        notional = qty * px
        if notional < float(settings.exit_min_notional_usd):
            continue

        # If daily flatten: exit everything
        if did_flatten:
            if not state.can_exit(symbol, settings.exit_cooldown_sec):
                continue
            res = broker.market_notional(symbol=symbol, side="sell", notional=notional, strategy="flatten", price=px)
            state.mark_exit(symbol)
            state.plans.pop(symbol, None)
            exits.append({"symbol": symbol, "reason": "daily_flatten", "notional": notional, "price": px, "result": res})
            continue

        # Otherwise: bracket exits (only if we have an in-memory plan)
        plan = state.plans.get(symbol)
        if not plan:
            continue

        if not state.can_exit(symbol, settings.exit_cooldown_sec):
            continue

        if px <= float(plan.stop_price):
            res = broker.market_notional(symbol=symbol, side="sell", notional=notional, strategy=plan.strategy, price=px)
            state.mark_exit(symbol)
            state.plans.pop(symbol, None)
            exits.append({"symbol": symbol, "reason": "stop", "entry": plan.entry_price, "price": px, "result": res})
        elif px >= float(plan.take_price):
            res = broker.market_notional(symbol=symbol, side="sell", notional=notional, strategy=plan.strategy, price=px)
            state.mark_exit(symbol)
            state.plans.pop(symbol, None)
            exits.append({"symbol": symbol, "reason": "take", "entry": plan.entry_price, "price": px, "result": res})

    return {"ok": True, "utc": now.isoformat(), "did_flatten": did_flatten, "exits": exits}
