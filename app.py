#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto System – FastAPI service
- Live strategy scans (c1..c6) via adapter
- Guardrails (cooldowns, loss streak, EMA alignment, spread/edge checks)
- Exit nanny (TP / no-cross)
- In-memory realized PnL attribution per strategy
- Simple dashboard and analytics endpoints (no backtest bleed-through)
Version: 2025.10.06-crypto-v3b
"""

import asyncio
import csv
import io
import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from math import isfinite
from typing import Any, Dict, List, Optional, Tuple

import importlib
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse


# ---------- Small helpers ----------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _parse_ts_any(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _order_is_fill_like(o: dict) -> bool:
    st = (o.get("status") or "").lower()
    fq = float(o.get("filled_qty") or 0.0)
    fp = float(o.get("filled_avg_price") or 0.0)
    return (st in ("filled", "partially_filled", "done")) or (fq > 0 and fp > 0)

def _sym_to_slash(s: str) -> str:
    if not s:
        return s
    s = s.upper()
    return s if "/" in s else (s[:-3] + "/" + s[-3:] if len(s) > 3 else s)

def _extract_strategy_from_order(o: dict) -> str:
    s = (o.get("strategy") or "").strip().lower()
    if s:
        return s
    coid = (o.get("client_order_id") or o.get("clientOrderId") or "").strip()
    if coid:
        token = coid.split("-")[0].strip().lower()
        if token:
            return token
    for k in ("tag", "subtag", "strategy_tag", "algo"):
        v = (o.get(k) or "").strip().lower()
        if v:
            return v
    return "unknown"

def _fetch_filled_orders_last_hours(hours: int) -> list:
    import broker as br
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    raw = br.list_orders(status="all", limit=1000) or []
    out = []
    for o in raw:
        if not _order_is_fill_like(o):
            continue
        t = _parse_ts_any(
            o.get("filled_at")
            or o.get("updated_at")
            or o.get("submitted_at")
            or o.get("created_at")
        )
        if t and t >= since:
            out.append(o)
    out.sort(
        key=lambda r: _parse_ts_any(
            r.get("filled_at")
            or r.get("updated_at")
            or r.get("submitted_at")
            or r.get("created_at")
        )
        or datetime.min.replace(tzinfo=timezone.utc)
    )
    return out

def _normalize_trade_row(o: dict) -> dict:
    sym = _sym_to_slash(o.get("symbol") or o.get("asset_symbol") or "")
    side = (o.get("side") or "").lower()
    qty = float(o.get("filled_qty") or o.get("qty") or o.get("quantity") or o.get("size") or 0.0)
    px = float(o.get("filled_avg_price") or o.get("price") or o.get("limit_price") or o.get("avg_price") or 0.0)
    coid = o.get("client_order_id") or o.get("clientOrderId") or ""
    ts = o.get("filled_at") or o.get("updated_at") or o.get("submitted_at") or o.get("created_at")
    return {
        "id": o.get("id") or coid,
        "symbol": sym,
        "side": side,
        "qty": qty,
        "price": px,
        "strategy": _extract_strategy_from_order(o),
        "time": (_parse_ts_any(ts) or datetime.now(timezone.utc)).isoformat(),
        "status": (o.get("status") or "").lower(),
        "client_order_id": coid,
        "notional": float(o.get("notional") or 0.0),
    }

# --- Daily stop (global & per-strategy) ---------------------------------------
_DISABLED_STRATS: set[str] = set()
_DAILY_PNL: dict[str, float] = defaultdict(float)  # sid -> pnl today
_DAILY_TOTAL: float = 0.0
_LAST_DAY: Optional[str] = None

DAILY_STOP_GLOBAL_USD = float(os.getenv("DAILY_STOP_GLOBAL_USD", "-100.0"))
DAILY_STOP_PER_STRAT_USD = float(os.getenv("DAILY_STOP_PER_STRAT_USD", "-50.0"))

def _daykey() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _roll_day_if_needed():
    global _LAST_DAY, _DAILY_PNL, _DAILY_TOTAL, _DISABLED_STRATS
    dk = _daykey()
    if _LAST_DAY != dk:
        _LAST_DAY = dk
        _DAILY_PNL.clear()
        _DAILY_TOTAL = 0.0
        _DISABLED_STRATS.clear()

# In the scheduler loop, skip disabled strategies:
#   for strat in STRATEGIES:
#       _roll_day_if_needed()
#       if strat in _DISABLED_STRATS: 
#           continue
#       ...

# At the end of _push_orders, after computing per-order pnl 'pnl' and strategy 'sname':
#   _roll_day_if_needed()
#   _DAILY_PNL[sname] += float(pnl or 0.0)
#   _DAILY_TOTAL += float(pnl or 0.0)
#   if _DAILY_PNL[sname] <= DAILY_STOP_PER_STRAT_USD:
#       _DISABLED_STRATS.add(sname)
#       log.warning("Daily stop (per-strategy) hit for %s; disabling until UTC rollover", sname)
#   if _DAILY_TOTAL <= DAILY_STOP_GLOBAL_USD:
#       _DISABLED_STRATS.update(STRATEGIES)
#       log.error("Daily stop (global) hit; disabling all strategies until UTC rollover")


# --- Normalize + persist orders to the ring store ---------------------------
from datetime import timezone

def _normalize_order(o: dict, strategy: str) -> dict:
    """Map adapter/broker orders into a canonical shape the metrics expect."""
    now = dt.datetime.now(timezone.utc).isoformat()
    # Accept several common key spellings; default sensible zeros for PnL until closed
    sym = o.get("symbol") or o.get("asset") or o.get("sym") or o.get("ticker")
    side = (o.get("side") or "").lower()  # "buy"/"sell"
    qnty = o.get("qty") or o.get("quantity") or o.get("size") or 0
    notional = o.get("notional") or o.get("value") or 0.0
    status = (o.get("status") or "open").lower()  # open/filled/closed/canceled
    pnl = o.get("pnl") or o.get("realized_pnl") or 0.0
    fee = o.get("fee") if "fee" in o else (o.get("fees") or 0.0)
    price = o.get("price") or o.get("avg_price") or o.get("limit_price") or 0.0
    opened_at = o.get("filled_at") or o.get("submitted_at") or o.get("created_at") or now
    closed_at = o.get("closed_at") or None
    oid = o.get("id") or o.get("order_id") or f"{strategy}-{sym}-{opened_at}"

    return {
        "id": oid,
        "symbol": sym,
        "side": side,
        "qty": float(qnty) if qnty else 0.0,
        "price": float(price) if price else 0.0,
        "notional": float(notional) if notional else 0.0,
        "strategy": strategy,
        "status": status,            # open | filled | closed | canceled
        "pnl": float(pnl),
        "fee": float(fee),
        "opened_at": opened_at,
        "closed_at": closed_at,
        # Optional passthroughs for later analysis:
        "time_in_force": o.get("time_in_force"),
        "type": o.get("type") or o.get("order_type"),
        "broker": o.get("broker") or "alpaca",
        "raw": o,                    # keep original shape for debugging
    }

def _append_orders(orders: list[dict], strategy: str) -> int:
    """Append normalized orders to the global ring; return count appended."""
    if not orders:
        return 0
    count = 0
    with _store_lock:
        ring = _get_store("orders")
        seen = {r.get("id") for r in ring if isinstance(r, dict)}
        for o in orders:
            try:
                norm = _normalize_order(o, strategy)
                if norm["id"] in seen:
                    continue
                ring.append(norm)
                seen.add(norm["id"])
                count += 1
        # Trim if you keep a cap (optional):
        cap = SETTINGS.get("orders_ring_cap", 5000)
        if cap and len(ring) > cap:
            del ring[:len(ring) - cap]
    return count


# ---------- Analytics: FIFO realized PnL attribution ----------
def _compute_strategy_metrics(
    orders: List[dict],
    hours: int = 12,
    tz: timezone = timezone.utc,
) -> dict:
    """
    Realizes P&L FIFO per SYMBOL, attributes P&L to the strategy that OPENED the lot.
    Also tracks who CLOSED each lot.
    """
    now = datetime.now(tz)
    cutoff = now - timedelta(hours=hours)

    # sanitize input fills
    fills = []
    for o in orders:
        if (o.get("status") or "").lower() != "filled":
            continue
        try:
            ts = datetime.fromisoformat((o["time"]).replace("Z", "+00:00"))
        except Exception:
            continue
        if ts < cutoff:
            continue
        side = (o.get("side") or "").lower()
        if side not in ("buy", "sell"):
            continue
        price = float(o.get("price", 0.0))
        qty = float(o.get("qty", 0.0))
        if qty <= 0 or price <= 0:
            continue
        fills.append(
            {
                "time": ts,
                "strategy": str(o.get("strategy") or ""),
                "symbol": str(o.get("symbol") or ""),
                "side": side,
                "qty": qty,
                "price": price,
                "id": o.get("id"),
                "client_order_id": o.get("client_order_id"),
                "notional": float(o.get("notional", qty * price)),
            }
        )
    fills.sort(key=lambda x: x["time"])

    # FIFO book per symbol
    book: Dict[str, deque] = defaultdict(deque)

    def S() -> dict:
        return dict(
            trades=0,
            wins=0,
            losses=0,
            gross_profit=0.0,
            gross_loss=0.0,
            net_pnl=0.0,
            profit_factor=None,
            win_rate=None,
            avg_trade=None,
            volume=0.0,
        )

    per_open_strategy: Dict[str, dict] = defaultdict(S)
    per_open_strategy_sym: Dict[str, dict] = defaultdict(S)
    per_close_strategy: Dict[str, dict] = defaultdict(S)
    realized_trades: List[dict] = []

    def _bump(stats: dict, pnl: float, notional: float):
        stats["trades"] += 1
        stats["net_pnl"] += pnl
        if pnl >= 0:
            stats["wins"] += 1
            stats["gross_profit"] += pnl
        else:
            stats["losses"] += 1
            stats["gross_loss"] += -pnl
        stats["volume"] += notional

    for f in fills:
        sym = f["symbol"]
        strat = f["strategy"] or "unknown"
        if f["side"] == "buy":
            book[sym].append(
                {
                    "qty": f["qty"],
                    "price": f["price"],
                    "opened_by": strat,
                    "time": f["time"],
                    "open_id": f.get("id"),
                }
            )
        else:  # sell
            qty_to_close = f["qty"]
            sell_price = f["price"]
            while qty_to_close > 1e-12 and book[sym]:
                lot = book[sym][0]
                use_qty = min(qty_to_close, lot["qty"])
                pnl = (sell_price - lot["price"]) * use_qty
                notional = use_qty * sell_price
                opened_by = lot["opened_by"]

                _bump(per_open_strategy[opened_by], pnl, notional)
                _bump(per_open_strategy_sym[f"{opened_by}::{sym}"], pnl, notional)
                _bump(per_close_strategy[strat], pnl, notional)

                realized_trades.append(
                    {
                        "id": f.get("id"),
                        "symbol": sym,
                        "side": "sell",
                        "qty": use_qty,
                        "price": sell_price,
                        "strategy_opened": opened_by,
                        "strategy_closed": strat,
                        "time": f["time"].isoformat(),
                        "status": "filled",
                        "client_order_id": f.get("client_order_id"),
                        "notional": notional,
                        "realized_pnl": pnl,
                    }
                )

                lot["qty"] -= use_qty
                qty_to_close -= use_qty
                if lot["qty"] <= 1e-12:
                    book[sym].popleft()

    def _finalize(stats: dict):
        gp, gl, t = stats["gross_profit"], stats["gross_loss"], stats["trades"]
        if gl > 0:
            stats["profit_factor"] = gp / gl
        elif gp > 0:
            stats["profit_factor"] = float("inf")
        else:
            stats["profit_factor"] = None
        if t > 0:
            stats["win_rate"] = stats["wins"] / t
            stats["avg_trade"] = stats["net_pnl"] / t
        else:
            stats["win_rate"] = None
            stats["avg_trade"] = None

    for d in (per_open_strategy, per_open_strategy_sym, per_close_strategy):
        for k in d:
            _finalize(d[k])

    return {
        "window_hours": hours,
        "summary_per_strategy": dict(per_open_strategy),
        "detail_per_strategy_symbol": dict(per_open_strategy_sym),
        "summary_closed_by": dict(per_close_strategy),
        "realized_trades": realized_trades,
        "count_orders_considered": len(fills),
    }


# --- STRATEGY GUARD STATE ----------------------------------------------------
class StrategyGuard:
    def __init__(self):
        self.cooldown_until_bar: Dict[str, int] = {}  # symbol -> bar_index
        self.closes_per_hour: Dict[str, deque] = defaultdict(lambda: deque())
        self.loss_streak: Dict[str, int] = defaultdict(int)  # strategy -> count
        self.last_bar_index: int = 0

    def on_bar(self, bar_index: int):
        self.last_bar_index = bar_index

    def on_realized(self, symbol: str, opener: str, closer: str, pnl: float, when: datetime):
        if pnl < 0:
            self.cooldown_until_bar[symbol] = max(
                self.cooldown_until_bar.get(symbol, 0),
                self.last_bar_index + GUARDS["cooldown_bars_after_loss"],
            )
            self.loss_streak[opener] = self.loss_streak.get(opener, 0) + 1
        else:
            self.loss_streak[opener] = 0

        dq = self.closes_per_hour[symbol]
        dq.append(when)
        cutoff = when - timedelta(hours=1)
        while dq and dq[0] < cutoff:
            dq.popleft()

    def can_open(
        self,
        strategy: str,
        symbol: str,
        edge_bps_val: float,
        spread_bps: float,
        price_above_ema: bool,
    ) -> Tuple[bool, str]:
        if not GUARDS["enable"]:
            return True, "guards_disabled"

        if self.last_bar_index < self.cooldown_until_bar.get(symbol, -1):
            return False, f"cooldown_active_until_bar_{self.cooldown_until_bar[symbol]}"

        if self.loss_streak.get(strategy, 0) >= GUARDS["max_consecutive_losses_per_strategy"]:
            return False, f"loss_streak_{self.loss_streak[strategy]}"

        if edge_bps_val < GUARDS["min_edge_bps"]:
            return False, f"edge_bps_{edge_bps_val:.2f}_lt_min_{GUARDS['min_edge_bps']}"
        if edge_bps_val < GUARDS["min_edge_vs_spread_x"] * spread_bps:
            return False, f"edge_vs_spread_{edge_bps_val:.2f}/{spread_bps:.2f}"
        if not price_above_ema:
            return False, "ema_alignment_fail"

        return True, "ok"

    def can_close_now(self, symbol: str, when: datetime) -> Tuple[bool, str]:
        dq = self.closes_per_hour[symbol]
        cutoff = when - timedelta(hours=1)
        while dq and dq[0] < cutoff:
            dq.popleft()
        if len(dq) >= GUARDS["max_closes_per_symbol_per_hour"]:
            return False, f"closes_per_hour_cap_{len(dq)}"
        return True, "ok"


GUARD = StrategyGuard()


def _tf_seconds(tf: str) -> int:
    t = tf.lower()
    if t in ("1min", "1m"):
        return 60
    if t in ("5min", "5m"):
        return 300
    if t in ("15min", "15m"):
        return 900
    if t in ("1h", "60min"):
        return 3600
    return 300  # default 5m

def bar_index_now(timeframe: str = "5Min") -> int:
    now = datetime.now(timezone.utc)
    return int(now.timestamp() // _tf_seconds(timeframe))


# -----------------------------------------------------------------------------
# Logging & Config
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s:app:%(message)s")
log = logging.getLogger("app")

SCHEDULE_SECONDS = int(os.getenv("SCHEDULE_SECONDS", os.getenv("SCHEDULER_INTERVAL_SEC", "60")))
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "300"))
DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "5Min")
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", os.getenv("ORDER_NOTIONAL", "25")))
DEFAULT_SYMBOLS = os.getenv("DEFAULT_SYMBOLS", "BTC/USD,ETH/USD").split(",")

TRADING_ENABLED = os.getenv("TRADING_ENABLED", "1") in ("1", "true", "True")
APP_VERSION = os.getenv("APP_VERSION", "2025.10.06-crypto-v3b")
STRATEGIES = [s.strip() for s in os.getenv("STRATEGY_LIST", "c1,c2,c3,c4,c5,c6").split(",") if s.strip()]

GUARDS = {
    "enable": True,
    "cooldown_bars_after_loss": 6,
    "max_closes_per_symbol_per_hour": 4,
    "max_consecutive_losses_per_strategy": 3,
    "consec_lookback_hours": 2,
    "min_edge_bps": 5.0,
    "min_edge_vs_spread_x": 2.0,
    "ema_fast": 20,
    "ema_slow": 50,
    "breakeven_trigger_bps": 8.0,
    "time_bail_bars": 8,
    "tp_target_bps": 12.0,
    "no_cross_exit": True,
}

# --- EMA / PRICE UTILITIES ---------------------------------------------------
def _bps(pct: float) -> float:
    return pct * 1e4

@lru_cache(maxsize=4096)
def _ema(series_key: tuple, prices: tuple[float, ...], period: int) -> float:
    k = 2.0 / (period + 1.0)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema

def ema_value(symbol: str, timeframe: str, closes: List[float], period: int) -> float:
    if not closes:
        return float("nan")
    key = (symbol, timeframe, len(closes), period)
    return _ema(key, tuple(closes), period)

def _price_above_ema_fast(symbol: str, timeframe: str, closes: List[float]) -> bool:
    try:
        ema = ema_value(symbol, timeframe, closes, GUARDS["ema_fast"])
        last = closes[-1] if closes else float("nan")
        return isfinite(ema) and isfinite(last) and (last >= ema)
    except Exception:
        return True  # fail-open if we can't compute

def _spread_bps(last: float, bid: float = None, ask: float = None) -> float:
    if bid and ask and bid > 0 and ask > bid:
        mid = 0.5 * (bid + ask)
        return _bps((ask - bid) / mid)
    return 4.0


# -----------------------------------------------------------------------------
# Strategy adapter
# -----------------------------------------------------------------------------
class RealStrategiesAdapter:
    def __init__(self):
        self._mods: Dict[str, Any] = {}

    def _get_mod(self, name: str):
        if name in self._mods:
            return self._mods[name]
        try:
            mod = importlib.import_module(f"strategies.{name}")
            log.info("adapter: imported strategies.%s", name)
        except Exception:
            mod = importlib.import_module(name)  # fallback if top-level
            log.info("adapter: imported %s (top-level)", name)
        self._mods[name] = mod
        return mod

    def scan(self, req: Dict[str, Any], _contexts: Dict[str, Any]) -> List[Dict[str, Any]]:
        strat = (req.get("strategy") or "").lower()
        if not strat:
            log.warning("adapter: missing 'strategy' in req")
            return []

        tf = req.get("timeframe") or DEFAULT_TIMEFRAME
        lim = int(req.get("limit") or DEFAULT_LIMIT)
        notional = float(req.get("notional") or DEFAULT_NOTIONAL)
        symbols = req.get("symbols") or []
        if isinstance(symbols, str):
            symbols = [s.strip() for s in symbols.split(",") if s.strip()]
        else:
            symbols = [s.strip() for s in symbols]
        dry = bool(req.get("dry", False))
        raw = req.get("raw") or {}

        try:
            mod = self._get_mod(strat)
            log.info(
                "adapter: calling %s.run_scan syms=%s tf=%s lim=%s notional=%s dry=%s",
                strat,
                ",".join(symbols),
                tf,
                lim,
                notional,
                int(dry),
            )
            result = mod.run_scan(symbols, tf, lim, notional, dry, raw)

            # normalize return
            orders: List[Dict[str, Any]] = []
            if isinstance(result, dict):
                if isinstance(result.get("placed"), list):
                    orders = result["placed"]
                elif isinstance(result.get("orders"), list):
                    orders = result["orders"]
            elif isinstance(result, list):
                orders = result

            log.info("adapter: %s produced %d order(s)", strat, len(orders))
            if len(orders) == 0 and isinstance(result, dict):
                log.info("adapter: %s raw result keys=%s", strat, list(result.keys()))
            return orders or []
        except Exception as e:
            log.exception("adapter: scan failed for %s: %s", strat, e)
            return []


class StrategyBook:
    def __init__(self):
        self._impl = RealStrategiesAdapter()

    def scan(self, req: Dict[str, Any], contexts: Optional[Dict[str, Any]] = None):
        return self._impl.scan(req, contexts or {})


# --- Position/PnL state ------------------------------------------------------
_positions_state: Dict[str, Dict[str, float]] = {}  # symbol -> {"qty": float, "avg_price": float}

def _get_last_price(symbol_slash: str) -> float:
    try:
        import broker as br
        pmap = br.last_trade_map([symbol_slash])
        return float(pmap.get(symbol_slash, {}).get("price") or 0.0)
    except Exception:
        return 0.0

def _normalize_order(o: dict) -> dict:
    coid = o.get("client_order_id") or o.get("clientOrderId") or o.get("client_orderid") or ""
    oid = o.get("id") or o.get("order_id") or coid or ""
    raw_sym = o.get("symbol") or o.get("Symbol") or o.get("asset_symbol") or ""
    sym = _sym_to_slash(raw_sym)
    side = (o.get("side") or o.get("Side") or o.get("order_side") or "").lower()
    status = (o.get("status") or o.get("Status") or "").lower()

    qty = o.get("filled_qty") or o.get("qty") or o.get("quantity") or o.get("size") or o.get("filled_quantity") or 0
    price = o.get("filled_avg_price") or o.get("price") or o.get("limit_price") or o.get("avg_price") or 0
    notional = o.get("notional") or o.get("notional_value") or 0

    try:
        qty = float(qty)
    except Exception:
        qty = 0.0
    try:
        price = float(price)
    except Exception:
        price = 0.0
    try:
        notional = float(notional)
    except Exception:
        notional = 0.0

    if price <= 0:
        price = _get_last_price(sym)
    if qty <= 0 and notional and price:
        qty = round(notional / price, 8)

    ts = (
        o.get("filled_at")
        or o.get("updated_at")
        or o.get("submitted_at")
        or o.get("created_at")
        or o.get("timestamp")
        or _now_iso()
    )

    strategy = (
        o.get("strategy")
        or _extract_strategy_from_order(o)
        or o.get("tag")
        or o.get("subtag")
        or "unknown"
    ).lower()

    pnl = o.get("pnl") or 0.0
    try:
        pnl = float(pnl)
    except Exception:
        pnl = 0.0

    return {
        "id": oid,
        "symbol": sym,
        "side": side,
        "qty": qty,
        "price": price,
        "strategy": strategy,
        "pnl": pnl,
        "time": ts,
        "client_order_id": coid,
        "status": status,
        "notional": notional,
    }

def _apply_realized_pnl(row: dict) -> float:
    sym = row.get("symbol")
    side = (row.get("side") or "").lower()
    qty = float(row.get("qty") or 0)
    price = float(row.get("price") or 0)
    if qty <= 0 or price <= 0:
        return 0.0

    pos = _positions_state.setdefault(sym, {"qty": 0.0, "avg_price": 0.0})
    realized = 0.0

    if side == "buy":
        new_qty = pos["qty"] + qty
        if new_qty > 0:
            pos["avg_price"] = ((pos["qty"] * pos["avg_price"]) + (qty * price)) / new_qty
        pos["qty"] = new_qty
    elif side == "sell":
        sell_qty = min(qty, max(pos["qty"], 0.0))
        if sell_qty > 0:
            realized = (price - pos["avg_price"]) * sell_qty
            pos["qty"] = pos["qty"] - sell_qty
    return realized

def _recalc_equity() -> float:
    if not _positions_state:
        return 0.0
    syms = [s for s, p in _positions_state.items() if p.get("qty", 0) > 0]
    if not syms:
        return 0.0
    try:
        import broker as br
        pmap = br.last_trade_map(syms)
    except Exception:
        pmap = {}
    eq = 0.0
    for s, p in _positions_state.items():
        q = float(p.get("qty") or 0)
        if q <= 0:
            continue
        last = float((pmap.get(s, {}) or {}).get("price") or 0.0)
        if last > 0:
            eq += q * last
    return eq

def _get_store():
    """Unified store accessor for metrics routes."""
    try:
        return getattr(app.state, "store", None) or {}
    except Exception:
        return {}

def _safe_get(key: str, default=None):
    """Return data from known sources (in-memory globals, app.state.store)."""
    # 1. app.state.store
    store = _get_store()
    if isinstance(store, dict) and key in store:
        return store[key]
    # 2. internal globals
    if key == "orders":
        return _orders_ring
    if key == "attribution":
        return _attribution
    if key == "summary":
        return _summary
    # 3. fallback
    return default or []

# -----------------------------------------------------------------------------
# App + state
# -----------------------------------------------------------------------------
app = FastAPI(title="Crypto System")

_orders_ring: List[Dict[str, Any]] = []
_attribution: Dict[str, Any] = {"by_strategy": {}, "updated_at": None}
_summary: Dict[str, Any] = {"equity": 0.0, "pnl_day": 0.0, "pnl_week": 0.0, "pnl_month": 0.0, "updated_at": None}

def _push_orders(orders: List[Dict[str, Any]]):
    global _orders_ring, _attribution, _summary

    if not orders:
        ts = _now_iso()
        _summary["updated_at"] = ts
        _attribution["updated_at"] = ts
        return

    realized_total = 0.0

    for o in orders:
        row = _normalize_order(o)

        try:
            r = _apply_realized_pnl(row)
        except Exception:
            r = 0.0

        try:
            if (row.get("side") == "sell") and r != 0.0:
                GUARD.on_realized(
                    symbol=row.get("symbol"),
                    opener=(row.get("strategy") or "unknown"),
                    closer=(row.get("strategy") or "unknown"),
                    pnl=float(r),
                    when=datetime.now(timezone.utc),
                )
        except Exception:
            log.exception("guard on_realized failed")

        row["pnl"] = float(row.get("pnl") or 0.0) + float(r or 0.0)
        realized_total += float(r or 0.0)

        _orders_ring.append(row)
        if len(_orders_ring) > 1000:
            _orders_ring = _orders_ring[-1000:]

        strat = (row.get("strategy") or "unknown").lower()
        _attribution["by_strategy"][strat] = _attribution["by_strategy"].get(strat, 0.0) + float(r or 0.0)

    ts = _now_iso()
    _attribution["updated_at"] = ts
    _summary["updated_at"] = ts

    _summary["pnl_day"] = float(_summary.get("pnl_day") or 0.0) + realized_total

    try:
        _summary["equity"] = _recalc_equity()
    except Exception:
        pass


async def _exit_nanny(symbols: List[str], timeframe: str):
    try:
        import broker as br
        bars_map = br.get_bars(symbols, timeframe=timeframe, limit=max(GUARDS["ema_slow"], 60)) or {}
        last_trade_map = br.last_trade_map(symbols) or {}
    except Exception:
        bars_map, last_trade_map = {}, {}

    for sym, pos in list(_positions_state.items()):
        qty = float(pos.get("qty") or 0.0)
        if qty <= 0:
            continue

        closes = [
            float(b.get("c") or b.get("close") or 0.0)
            for b in (bars_map.get(sym) or [])
            if float(b.get("c") or b.get("close") or 0.0) > 0
        ]
        if not closes:
            continue

        last = float((last_trade_map.get(sym, {}) or {}).get("price") or closes[-1] or 0.0)
        if last <= 0:
            continue

        entry = float(pos.get("avg_price") or 0.0)
        if entry <= 0:
            continue

        up_bps = _bps((last - entry) / entry)
        if up_bps >= GUARDS["breakeven_trigger_bps"]:
            log.info("exit_nanny: %s hit breakeven trigger (%.1f bps)", sym, up_bps)

        # TP target
        if up_bps >= GUARDS["tp_target_bps"]:
            ok, why = GUARD.can_close_now(sym, datetime.now(timezone.utc))
            if ok and TRADING_ENABLED:
                try:
                    br.submit_order(symbol=sym, side="sell", notional=min(DEFAULT_NOTIONAL, qty * last))
                    log.info("exit_nanny: TP sell placed for %s", sym)
                    continue
                except Exception:
                    log.exception("exit_nanny TP submit failed")

        # No-cross exit (below slow EMA)
        try:
            slow_ema = ema_value(sym, timeframe, closes, GUARDS["ema_slow"])
            crossed = isfinite(slow_ema) and (last < slow_ema)
        except Exception:
            crossed = False

        if crossed and TRADING_ENABLED:
            ok, why = GUARD.can_close_now(sym, datetime.now(timezone.utc))
            if ok:
                try:
                    br.submit_order(symbol=sym, side="sell", notional=min(DEFAULT_NOTIONAL, qty * last))
                    log.info("exit_nanny: no-cross sell placed for %s", sym)
                except Exception:
                    log.exception("exit_nanny no-cross submit failed")


# -----------------------------------------------------------------------------
# Scan bridge
# -----------------------------------------------------------------------------
async def _scan_bridge(strat: str, req: Dict[str, Any], dry: bool = False) -> List[Dict[str, Any]]:
    req = dict(req or {})
    req.setdefault("strategy", strat)
    req.setdefault("timeframe", req.get("tf") or DEFAULT_TIMEFRAME)
    req.setdefault("limit", req.get("limit") or DEFAULT_LIMIT)
    req.setdefault("notional", req.get("notional") or DEFAULT_NOTIONAL)

    syms = req.get("symbols") or DEFAULT_SYMBOLS
    if isinstance(syms, str):
        syms = [s.strip() for s in syms.split(",") if s.strip()]
    req["symbols"] = syms
    req["dry"] = dry
    req["raw"] = dict(req)

    sb = StrategyBook()
    orders = sb.scan(req, {"one": {"timeframe": req["timeframe"], "symbols": syms, "notional": req["notional"]}})

    # Guard filter on proposed orders
    try:
        import broker as br
        bars_map = br.get_bars(req["symbols"], timeframe=req["timeframe"], limit=60) or {}
    except Exception:
        bars_map = {}

    filtered: List[Dict[str, Any]] = []
    now_dt = datetime.now(timezone.utc)

    for o in (orders or []):
        side = (o.get("side") or "").lower()
        sym = _sym_to_slash(o.get("symbol") or "")
        sname = (req.get("strategy") or "").lower()

        closes = [
            float(b.get("c") or b.get("close") or 0.0)
            for b in (bars_map.get(sym) or [])
            if float(b.get("c") or b.get("close") or 0.0) > 0
        ]
        px = float(o.get("price") or 0.0)
        if px <= 0 and closes:
            px = closes[-1]

        edge_bps_val = float(o.get("edge_bps") or o.get("edge") or 0.0)
        spr_bps = _spread_bps(px)
        ema_ok = _price_above_ema_fast(sym, req["timeframe"], closes)

        if side == "buy":
            ok, why = GUARD.can_open(sname, sym, edge_bps_val, spr_bps, ema_ok)
            if not ok:
                log.info("guard: DROP OPEN %s %s by %s (%s)", sym, side, sname, why)
                continue
        if side == "sell":
            ok, why = GUARD.can_close_now(sym, now_dt)
            if not ok:
                log.info("guard: DROP CLOSE %s by %s (%s)", sym, sname, why)
                continue

        filtered.append(o)

    if not filtered:
        return []
    if isinstance(filtered, dict):
        return filtered.get("orders") or filtered.get("placed") or []
    return list(filtered)


# -----------------------------------------------------------------------------
# Background scheduler
# -----------------------------------------------------------------------------
_scheduler_task: Optional[asyncio.Task] = None
_running = False

async def _scheduler_loop():
    global _running
    _running = True
    try:
        while _running:
            dry_flag = (not TRADING_ENABLED)
            log.info("Scheduler tick: running all strategies (dry=%d)", int(dry_flag))
            for strat in STRATEGIES:
                try:
                    GUARD.on_bar(bar_index_now(DEFAULT_TIMEFRAME))
                except Exception:
                    log.exception("guard tick failed")
                try:
                    orders = await _scan_bridge(
                        strat,
                        {
                            "timeframe": DEFAULT_TIMEFRAME,
                            "symbols": DEFAULT_SYMBOLS,
                            "limit": DEFAULT_LIMIT,
                            "notional": DEFAULT_NOTIONAL,
                        },
                        dry=dry_flag,
                    )
                except Exception:
                    log.exception("scan error %s", strat)
                    orders = []
                try:
                    _push_orders(orders)
                except Exception:
                    log.exception("push orders error")
                try:
                    await _exit_nanny(DEFAULT_SYMBOLS, DEFAULT_TIMEFRAME)
                except Exception:
                    log.exception("exit nanny failed")
            await asyncio.sleep(SCHEDULE_SECONDS)
    finally:
        _running = False

@app.on_event("startup")
async def _startup():
    global _scheduler_task
    log.info("App startup; scheduler interval is %ss", SCHEDULE_SECONDS)
    if _scheduler_task is None:
        _scheduler_task = asyncio.create_task(_scheduler_loop())

@app.on_event("shutdown")
async def _shutdown():
    global _running, _scheduler_task
    log.info("Shutting down app; scheduler will stop.")
    _running = False
    try:
        if _scheduler_task:
            await asyncio.wait_for(_scheduler_task, timeout=5.0)
    except Exception:
        pass


# --- Safe state accessors for metrics ----------------------------------------
from collections import deque

def _orders_source():
    """
    Returns an iterable of normalized order dicts regardless of where you keep them.
    Supports:
      - old global STATE = {"orders": deque([...])}
      - app.state.store["orders"]
      - global RING.orders (deque/list)
    Falls back to empty list.
    """
    # 1) legacy global STATE
    try:
        if "STATE" in globals():
            gstate = globals()["STATE"]
            if isinstance(gstate, dict) and "orders" in gstate:
                return gstate["orders"]
    except Exception:
        pass

    # 2) FastAPI state store
    try:
        from fastapi import FastAPI  # already imported elsewhere
        if "app" in globals():
            store = getattr(app.state, "store", None)
            if isinstance(store, dict) and "orders" in store:
                return store["orders"]
    except Exception:
        pass

    # 3) ring-style object
    try:
        if "RING" in globals():
            ring = globals()["RING"]
            if hasattr(ring, "orders"):
                return ring.orders
    except Exception:
        pass

    # 4) nothing found
    return []

def _as_iter(iterable):
    try:
        return list(iterable)
    except Exception:
        return []

def _strategy_from_order(o: dict) -> str:
    # tolerate different shapes
    if not isinstance(o, dict):
        return "unknown"
    sid = o.get("strategy") or o.get("strategy_id") or o.get("tag") or ""
    if isinstance(sid, str) and sid.strip():
        return sid
    # sometimes embedded in client_order_id
    coid = o.get("client_order_id") or o.get("clientOrderId")
    if isinstance(coid, str) and ":" in coid:
        return coid.split(":", 1)[0]
    return "unknown"

def _pnl_from_order(o: dict) -> float:
    # prefer realized PnL if present; else signed notional approximation
    if not isinstance(o, dict):
        return 0.0
    if "realized_pnl" in o and o["realized_pnl"] is not None:
        try:
            return float(o["realized_pnl"])
        except Exception:
            pass
    # fallback: fill value * side sign (very rough)
    qty = float(o.get("filled_qty") or o.get("qty") or 0) or 0.0
    price = float(o.get("fill_price") or o.get("avg_fill_price") or o.get("price") or 0) or 0.0
    side = (o.get("side") or "").lower()
    sign = 1.0 if side == "sell" else -1.0 if side == "buy" else 0.0
    return sign * qty * price * 0.0  # default to 0 because this is not realized; adjust if you want proxy PnL

def _compute_strategy_metrics(orders_iterable):
    """
    Produces a per-strategy scorecard dict with conservative defaults.
    Expects orders to be closed/fill-complete for realized metrics.
    """
    import math, statistics, datetime as dt

    orders = [o for o in _as_iter(orders_iterable) if isinstance(o, dict)]
    buckets = {}  # sid -> list of orders
    for o in orders:
        sid = _strategy_from_order(o)
        buckets.setdefault(sid, []).append(o)

    scorecard = {}
    for sid, items in buckets.items():
        n = len(items)
        rets = []
        wins = 0
        losses = 0
        gross_pnl = 0.0
        fees = 0.0

        for o in items:
            r = _pnl_from_order(o)
            gross_pnl += r
            # fees if present
            fee = o.get("fees") or o.get("commission") or 0.0
            try:
                fees += float(fee or 0.0)
            except Exception:
                pass
            if r > 0:
                wins += 1
            elif r < 0:
                losses += 1
            # per-trade return (if notional present)
            notional = o.get("notional") or (float(o.get("filled_qty") or 0) * float(o.get("avg_fill_price") or o.get("price") or 0))
            try:
                notional = float(notional or 0.0)
            except Exception:
                notional = 0.0
            if notional > 0:
                rets.append(r / notional)

        win_rate = wins / n if n else 0.0
        avg_ret = statistics.mean(rets) if rets else 0.0
        std_ret = statistics.pstdev(rets) if len(rets) > 1 else 0.0
        # simple Sharpe-like (per-trade), avoid div-by-zero
        sharpe_like = (avg_ret / std_ret) if std_ret > 0 else (float("inf") if avg_ret > 0 else 0.0)

        scorecard[sid] = {
            "trades": n,
            "wins": wins,
            "losses": losses,
            "win_rate": round(win_rate, 4),
            "gross_pnl": round(gross_pnl, 2),
            "fees": round(fees, 2),
            "net_pnl": round(gross_pnl - fees, 2),
            "avg_ret": round(avg_ret, 6),
            "std_ret": round(std_ret, 6),
            "sharpe_like": (round(sharpe_like, 4) if math.isfinite(sharpe_like) else "inf"),
        }
    return scorecard

# -----------------------------------------------------------------------------
# Dashboard HTML
# -----------------------------------------------------------------------------
_DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Crypto System Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root{
      --bg:#0b1220; --panel:#111a2b; --ink:#e6edf3; --muted:#a6b3c2;
      --accent:#5dd4a3; --accent2:#66a3ff; --red:#ff6b6b; --chip:#1a2336; --chip-br:#26324a;
    }
    *{box-sizing:border-box}
    body{ margin:0; font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial; background:linear-gradient(160deg,#0b1220 0%,#0e172a 100%); color:var(--ink);}
    header{ padding:20px 24px; border-bottom:1px solid #16233b; display:flex; align-items:center; gap:12px }
    .badge{ font-size:12px; background:var(--chip); border:1px solid var(--chip-br); color:var(--muted); padding:4px 8px; border-radius:999px;}
    main{ padding:24px; max-width:1200px; margin:0 auto }
    .grid{ display:grid; grid-template-columns:repeat(12,1fr); gap:16px }
    .card{ background:var(--panel); border:1px solid #1a2740; border-radius:14px; padding:16px; box-shadow:0 10px 24px rgba(0,0,0,.2)}
    .span-4{grid-column:span 4} .span-6{grid-column:span 6} .span-8{grid-column:span 8} .span-12{grid-column:span 12}
    h1{font-size:18px;margin:0} .muted{color:var(--muted)} .row{display:flex;align-items:center;justify-content:space-between;gap:12px}
    .kpi{font-size:28px;font-weight:700} .good{color:var(--accent)} .bad{color:var(--red)}
    table{width:100%;border-collapse:collapse;font-size:14px} th,td{padding:8px;border-bottom:1px solid #1a2740;text-align:left} th{color:#9db0c9;font-weight:600}
    .chips{display:flex;flex-wrap:wrap;gap:8px} .chip{ background:var(--chip); border:1px solid var(--chip-br); padding:6px 10px; border-radius:999px; font-size:12px; color:#c7d2e3 }
    a.btn{ display:inline-block; padding:8px 12px; border-radius:10px; text-decoration:none; background:var(--accent2); color:#0b1220; font-weight:700; border:1px solid #2a3f6b }
    footer{ padding:28px; color:var(--muted); text-align:center }
    @media (max-width: 900px){ .span-4,.span-6,.span-8{grid-column:span 12} }
    code{ background:#0d1628; border:1px solid #1a2740; padding:2px 6px; border-radius:6px }
  </style>
  <script>
    async function loadSummary(){
      const r = await fetch('/pnl/summary');
      const d = await r.json();
      document.getElementById('eq').textContent = Number(d.equity || 0).toFixed(2);
      document.getElementById('pnl_day').textContent = Number(d.pnl_day || 0).toFixed(2);
      document.getElementById('pnl_week').textContent = Number(d.pnl_week || 0).toFixed(2);
      document.getElementById('pnl_month').textContent = Number(d.pnl_month || 0).toFixed(2);
      document.getElementById('updated').textContent = d.updated_at ? new Date(d.updated_at).toLocaleString() : '-';
    }
    async function loadAttribution(){
      const r = await fetch('/orders/attribution');
      const d = await r.json();
      const tbody = document.getElementById('attr_body');
      tbody.innerHTML = '';
      if (d.by_strategy){
        for (const [k,v] of Object.entries(d.by_strategy)){
          const tr = document.createElement('tr');
          tr.innerHTML = `<td>${k}</td><td>${Number(v).toFixed(2)}</td>`;
          tbody.appendChild(tr);
        }
      }
      document.getElementById('attr_updated').textContent = d.updated_at ? new Date(d.updated_at).toLocaleString() : '-';
    }
    async function loadOrders(){
      const r = await fetch('/orders/recent?limit=50');
      const d = await r.json();
      const tbody = document.getElementById('orders_body');
      tbody.innerHTML = '';
      (d.orders || []).forEach(o => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${o.id ?? ''}</td>
          <td>${o.symbol ?? ''}</td>
          <td>${o.side ?? ''}</td>
          <td>${o.qty ?? ''}</td>
          <td>${o.px ?? ''}</td>
          <td>${o.strategy ?? ''}</td>
          <td>${o.pnl ?? 0}</td>
          <td>${o.ts ? new Date(o.ts*1000).toLocaleString() : ''}</td>
        `;
        tbody.appendChild(tr);
      });
    }
    async function refreshAll(){ await Promise.all([loadSummary(), loadAttribution(), loadOrders()]); }
    setInterval(refreshAll, 15000); window.addEventListener('load', refreshAll);
  </script>
</head>
<body>
  <header class="row">
    <h1>Crypto System</h1>
    <span class="badge">Live</span>
    <span class="badge">Scheduler: <code>active</code></span>
    <div style="margin-left:auto" class="chips">
      <span class="chip">TF: <code id="tf_chip">auto</code></span>
      <span class="chip">Tick: <code>{SCHEDULE_SECONDS}s</code></span>
    </div>
  </header>

  <main>
    <div class="grid">
      <section class="card span-8">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">P&L Summary</h2>
          <a class="btn" href="#" onclick="refreshAll();return false;">Refresh</a>
        </div>
        <div class="grid" style="grid-template-columns:repeat(12,1fr);gap:12px">
          <div class="span-4"><div class="muted">Equity</div><div class="kpi" id="eq">0.00</div></div>
          <div class="span-4"><div class="muted">PnL (Day)</div><div class="kpi good" id="pnl_day">0.00</div></div>
          <div class="span-4"><div class="muted">PnL (Week)</div><div class="kpi" id="pnl_week">0.00</div></div>
          <div class="span-4"><div class="muted">PnL (Month)</div><div class="kpi" id="pnl_month">0.00</div></div>
          <div class="span-8"><div class="muted">Last Updated</div><div id="updated" class="kpi" style="font-size:16px">-</div></div>
        </div>
      </section>

      <section class="card span-4">
        <div class="row" style="margin-bottom:8px"><h2 style="margin:0;font-size:16px">Attribution</h2></div>
        <table><thead><tr><th>Strategy</th><th>PnL</th></tr></thead><tbody id="attr_body"></tbody></table>
        <div class="muted" style="margin-top:8px">Updated: <span id="attr_updated">-</span></div>
      </section>

      <section class="card span-12">
        <div class="row" style="margin-bottom:8px"><h2 style="margin:0;font-size:16px">Recent Orders</h2></div>
        <table>
          <thead><tr><th>ID</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Price</th><th>Strategy</th><th>PnL</th><th>Time</th></tr></thead>
          <tbody id="orders_body"></tbody>
        </table>
      </section>
    </div>
  </main>

  <footer>Built with FastAPI • Tick interval: {SCHEDULE_SECONDS}s</footer>
  <script>document.getElementById('tf_chip').textContent = "{DEFAULT_TIMEFRAME}";</script>
</body>
</html>
""".replace("{SCHEDULE_SECONDS}", str(SCHEDULE_SECONDS)).replace("{DEFAULT_TIMEFRAME}", DEFAULT_TIMEFRAME)


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/dashboard", status_code=307)

@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    return HTMLResponse(_DASHBOARD_HTML)

@app.get("/healthz", include_in_schema=False)
def healthz():
    return {"ok": True, "version": APP_VERSION, "time": _now_iso()}

@app.get("/pnl/summary", response_class=JSONResponse)
async def pnl_summary():
    return JSONResponse(_summary)

@app.get("/orders/recent", response_class=JSONResponse)
async def orders_recent(limit: int = Query(100, ge=1, le=1000)):
    items = _orders_ring[-limit:] if _orders_ring else []
    mapped = []
    for o in items:
        ts_iso = o.get("time") or ""
        try:
            ts_epoch = int(datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp())
        except Exception:
            ts_epoch = None
        mapped.append(
            {
                "id": o.get("id"),
                "symbol": o.get("symbol"),
                "side": o.get("side"),
                "qty": o.get("qty"),
                "px": o.get("price"),
                "strategy": o.get("strategy"),
                "pnl": o.get("pnl"),
                "ts": ts_epoch,
            }
        )
    return JSONResponse({"orders": mapped, "updated_at": datetime.now(timezone.utc).isoformat()})

@app.get("/orders/attribution", response_class=JSONResponse)
async def orders_attribution():
    return JSONResponse(_attribution)

@app.get("/analytics/trades")
async def analytics_trades(hours: int = 12):
    orders = _fetch_filled_orders_last_hours(int(hours))
    rows = [_normalize_trade_row(o) for o in orders]
    metrics = _compute_strategy_metrics(rows, hours=int(hours))
    return {
        "window_hours": metrics["window_hours"],
        "summary_per_strategy": metrics["summary_per_strategy"],
        "detail_per_strategy_symbol": metrics["detail_per_strategy_symbol"],
        "summary_closed_by": metrics["summary_closed_by"],
        "realized_trades": metrics["realized_trades"],
        "count_orders_considered": metrics["count_orders_considered"],
    }

@app.get("/analytics/trades.csv", response_class=PlainTextResponse)
async def analytics_trades_csv(hours: int = 12):
    orders = _fetch_filled_orders_last_hours(int(hours))
    rows = [_normalize_trade_row(o) for o in orders]
    buf = io.StringIO()
    w = csv.DictWriter(
        buf,
        fieldnames=[
            "time",
            "strategy",
            "symbol",
            "side",
            "qty",
            "price",
            "id",
            "client_order_id",
            "status",
            "notional",
        ],
    )
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()

@app.get("/diag/orders_raw")
async def diag_orders_raw(status: str = "all", limit: int = 25):
    import broker as br
    data = br.list_orders(status=status, limit=limit) or []
    return {"status": status, "limit": limit, "orders": data}

@app.get("/diag/bars")
async def diag_bars(symbols: str = "BTC/USD,ETH/USD", tf: str = "5Min", limit: int = 360):
    import broker as br
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    bars = br.get_bars(syms, timeframe=tf, limit=int(limit))
    return {"timeframe": tf, "limit": limit, "counts": {k: len(v) for k, v in bars.items()}, "keys": list(bars.keys())}

@app.get("/diag/imports")
async def diag_imports():
    out = {}
    for name in [s.strip() for s in os.getenv("STRATEGY_LIST", "c1,c2,c3,c4,c5,c6").split(",") if s.strip()]:
        try:
            try:
                importlib.import_module(f"strategies.{name}")
                out[name] = f"ok: strategies.{name}"
            except Exception:
                importlib.import_module(name)
                out[name] = f"ok: top-level {name}"
        except Exception as e:
            out[name] = f"ERROR: {e}"
    return out

@app.get("/diag/scan")
async def diag_scan(
    strategy: str,
    symbols: str = "BTC/USD,ETH/USD",
    tf: Optional[str] = None,
    limit: Optional[int] = None,
    notional: Optional[float] = None,
    dry: int = 1,
):
    tf = tf or DEFAULT_TIMEFRAME
    limit = limit or DEFAULT_LIMIT
    notional = notional or DEFAULT_NOTIONAL
    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    req = {
        "strategy": strategy,
        "timeframe": tf,
        "limit": limit,
        "notional": notional,
        "symbols": syms,
        "dry": bool(dry),
        "raw": {},
    }
    sb = StrategyBook()
    orders = sb.scan(req, {"one": {"timeframe": tf, "symbols": syms, "notional": notional}})
    return {"args": req, "orders_count": len(orders or []), "orders": orders}

@app.get("/diag/alpaca")
async def diag_alpaca():
    try:
        import broker as br
        pos = br.list_positions()
        bars = br.get_bars(["BTC/USD", "ETH/USD"], timeframe=DEFAULT_TIMEFRAME, limit=3)
        return {
            "positions_len": len(pos if isinstance(pos, list) else []),
            "bars_keys": list(bars.keys()),
            "bars_len_each": {k: len(v) for k, v in bars.items()},
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/fliptest", response_class=JSONResponse)
def metrics_fliptest(hours: int = 168):
    """Flip-test per strategy by negating realized PnL over last N hours."""
    orders = _fetch_filled_orders_last_hours(int(hours))
    # Reuse the normalization already in app
    rows = [_normalize_trade_row(o) for o in orders]
    per = defaultdict(list)
    for r in rows:
        per[(r.get("strategy") or "unknown").lower()].append(r)

    out = {}
    for sid, items in per.items():
        pnl = [float(x.get("pnl") or 0.0) for x in items]
        gp = sum(x for x in pnl if x > 0); gl = -sum(x for x in pnl if x < 0)
        gp_r = sum(-x for x in pnl if x < 0); gl_r = -sum(-x for x in pnl if -x < 0)  # reversed
        n = len(pnl)
        out[sid] = {
            "N": n,
            "PF": (gp / gl) if gl > 0 else (float("inf") if gp > 0 else 0.0),
            "Expectancy": (sum(pnl) / n) if n else 0.0,
            "PF_rev": (gp_r / gl_r) if gl_r > 0 else (float("inf") if gp_r > 0 else 0.0),
            "Expectancy_rev": ((-sum(pnl)) / n) if n else 0.0,
        }
    return out

@app.get("/metrics/scorecard", response_class=JSONResponse)
def metrics_scorecard():
    """
    JSON scorecard across all strategies, using the current in-memory orders.
    Safe against missing global STATE or empty stores.
    """
    import statistics, math

    try:
        orders = _safe_get("orders", [])
        if not orders:
            return {"ok": True, "data": {}, "note": "no orders yet"}

        # --- compute metrics ---
        buckets = {}
        for o in orders:
            strat = (o.get("strategy") or "unknown").lower()
            buckets.setdefault(strat, []).append(o)

        data = {}
        for sname, items in buckets.items():
            n = len(items)
            wins = losses = 0
            gross_pnl = fees = 0.0
            rets = []

            for o in items:
                pnl = float(o.get("pnl") or 0.0)
                gross_pnl += pnl
                if pnl > 0:
                    wins += 1
                elif pnl < 0:
                    losses += 1
                fee = float(o.get("fee") or o.get("fees") or 0.0)
                fees += fee
                notional = float(o.get("notional") or 0.0)
                if notional > 0:
                    rets.append(pnl / notional)

            win_rate = wins / n if n else 0.0
            avg_ret = statistics.mean(rets) if rets else 0.0
            std_ret = statistics.pstdev(rets) if len(rets) > 1 else 0.0
            sharpe_like = (avg_ret / std_ret) if std_ret > 0 else (float("inf") if avg_ret > 0 else 0.0)

            data[sname] = {
                "trades": n,
                "wins": wins,
                "losses": losses,
                "win_rate": round(win_rate, 4),
                "gross_pnl": round(gross_pnl, 2),
                "fees": round(fees, 2),
                "net_pnl": round(gross_pnl - fees, 2),
                "avg_ret": round(avg_ret, 6),
                "std_ret": round(std_ret, 6),
                "sharpe_like": (round(sharpe_like, 4) if math.isfinite(sharpe_like) else "inf"),
            }

        return {"ok": True, "data": data, "count_strategies": len(data)}

    except Exception as e:
        log.exception("metrics_scorecard failed: %s", e)
        return JSONResponse(content={"ok": False, "error": str(e)}, status_code=500)


@app.get("/metrics/scorecard.csv", response_class=PlainTextResponse)
def metrics_scorecard_csv():
    """
    CSV scorecard export, resilient against missing data.
    """
    import io, csv, math

    try:
        orders = _safe_get("orders", [])
        if not orders:
            return PlainTextResponse("strategy,trades,wins,losses,win_rate,gross_pnl,fees,net_pnl,avg_ret,std_ret,sharpe_like\n")

        # reuse JSON computation
        buckets = {}
        for o in orders:
            strat = (o.get("strategy") or "unknown").lower()
            buckets.setdefault(strat, []).append(o)

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["strategy", "trades", "wins", "losses", "win_rate", "gross_pnl", "fees", "net_pnl", "avg_ret", "std_ret", "sharpe_like"])

        import statistics
        for sname, items in buckets.items():
            n = len(items)
            wins = losses = 0
            gross_pnl = fees = 0.0
            rets = []
            for o in items:
                pnl = float(o.get("pnl") or 0.0)
                gross_pnl += pnl
                if pnl > 0:
                    wins += 1
                elif pnl < 0:
                    losses += 1
                fee = float(o.get("fee") or o.get("fees") or 0.0)
                fees += fee
                notional = float(o.get("notional") or 0.0)
                if notional > 0:
                    rets.append(pnl / notional)
            win_rate = wins / n if n else 0.0
            avg_ret = statistics.mean(rets) if rets else 0.0
            std_ret = statistics.pstdev(rets) if len(rets) > 1 else 0.0
            sharpe_like = (avg_ret / std_ret) if std_ret > 0 else (float("inf") if avg_ret > 0 else 0.0)
            writer.writerow([
                sname,
                n,
                wins,
                losses,
                round(win_rate, 4),
                round(gross_pnl, 2),
                round(fees, 2),
                round(gross_pnl - fees, 2),
                round(avg_ret, 6),
                round(std_ret, 6),
                round(sharpe_like, 4) if math.isfinite(sharpe_like) else "inf",
            ])
        return PlainTextResponse(buf.getvalue(), media_type="text/csv")
    except Exception as e:
        log.exception("metrics_scorecard_csv failed: %s", e)
        return PlainTextResponse("error," + str(e), media_type="text/csv", status_code=500)

@app.post("/init/positions")
async def init_positions():
    """Seed the in-memory positions state from Alpaca current positions."""
    try:
        import broker as br
        pos = br.list_positions() or []
        global _positions_state
        _positions_state = {}
        for p in pos:
            sym = _sym_to_slash(p.get("symbol") or p.get("Symbol") or "")
            try:
                qty = float(
                    p.get("qty")
                    or p.get("quantity")
                    or p.get("qty_available")
                    or p.get("size")
                    or p.get("Qty")
                    or 0.0
                )
            except Exception:
                qty = 0.0
            avg = float(p.get("avg_entry_price") or p.get("average_entry_price") or p.get("avg_price") or 0.0)
            if sym and qty and avg:
                _positions_state[sym] = {"qty": qty, "avg_price": avg}
        _summary["equity"] = _recalc_equity()
        ts = _now_iso()
        _summary["updated_at"] = ts
        _attribution["updated_at"] = ts
        return {"ok": True, "positions": _positions_state}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/init/backfill")
async def init_backfill(days: Optional[int] = None, status: str = "closed"):
    """
    Replay recent *filled* orders into P&L/attribution without importing backtest-only data.
    """
    try:
        import broker as br
        lookback_days = int(days or os.getenv("INIT_BACKFILL_DAYS", "7"))
        after = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        raws = br.list_orders(status=status, limit=1000) or []
        selected = []
        for r in raws:
            ts = r.get("filled_at") or r.get("updated_at") or r.get("submitted_at") or r.get("created_at")
            when = _parse_ts_any(ts) if isinstance(ts, str) else None
            if when and when >= after:
                selected.append(r)
        orders = []
        for r in selected:
            st = (r.get("status") or "").lower()
            filled_px = float(r.get("filled_avg_price") or 0.0)
            filled_qty = float(r.get("filled_qty") or 0.0)
            if st in ("filled", "partially_filled", "done") or (filled_px > 0 and filled_qty > 0):
                orders.append(r)
        _push_orders(orders)
        return {"ok": True, "considered": len(raws), "selected": len(selected), "replayed": len(orders)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn  # type: ignore
    port = int(os.getenv("PORT", "10000"))
    log.info("Launching Uvicorn on 0.0.0.0:%d (version %s)", port, APP_VERSION)
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
