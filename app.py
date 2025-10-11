#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto System – FastAPI service (Render-safe)
Version: 2025.10.11-kraken-switch-b

v3e adds:
- Strategy runtime controls (/strategies, /strategies/enable, /disable, /reload)
- Adapter .reload(name) support + scheduler uses ACTIVE_STRATEGIES set
- Optional universe.py integration (UniverseBuilder) with cache-like adapter
- Keeps v3d improvements: exit nanny client_order_id, multi-coin universe, guards, etc.
"""

import asyncio
import csv
import io
import json
import logging
import os

# --- Broker selection (Alpaca vs Kraken) ---
BROKER = os.getenv("BROKER","alpaca").lower()
if BROKER == "kraken":
    # using selected broker module as br_kraken as br
else:
    # using selected broker module as br
import time
import importlib
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from math import isfinite
from statistics import median
from typing import Any, Dict, List, Optional

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
    if not s: return s
    s = s.upper()
    # Accept already-slashed (BTC/USD) or plain (BTCUSD)
    return s if "/" in s else (s[:-3] + "/" + s[-3:] if len(s) > 3 else s)

def _extract_strategy_from_order(o: dict) -> str:
    s = (o.get("strategy") or "").strip().lower()
    if s: return s
    coid = (o.get("client_order_id") or o.get("clientOrderId") or "").strip()
    if coid:
        token = coid.split("-")[0].strip().lower()
        if token: return token
    for k in ("tag", "subtag", "strategy_tag", "algo"):
        v = (o.get(k) or "").strip().lower()
        if v: return v
    return "unknown"

# -----------------------------------------------------------------------------
# Environment & Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s:app:%(message)s")
log = logging.getLogger("app")

APP_VERSION = os.getenv("APP_VERSION", "2025.10.11-kraken-switch-b")
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "1") in ("1", "true", "True")
SCHEDULE_SECONDS = int(os.getenv("SCHEDULE_SECONDS", os.getenv("SCHEDULER_INTERVAL_SEC", "60")))
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "300"))
DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "5Min")
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", os.getenv("ORDER_NOTIONAL", "25")))
STRATEGIES = [s.strip() for s in os.getenv("STRATEGY_LIST", "c1,c2,c3,c4,c5,c6").split(",") if s.strip()]

# If DEFAULT_SYMBOLS env is set, we honor it; else we’ll build a universe.
_default_symbols_env = [s.strip().upper() for s in os.getenv("DEFAULT_SYMBOLS", "").split(",") if s.strip()]

# Guards
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

# Daily stop scaffolding (optional; expand as needed)
_DISABLED_STRATS: set[str] = set()
_DAILY_PNL: dict[str, float] = defaultdict(float)
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

# --- EMA / PRICE UTILITIES ---------------------------------------------------
def _tf_seconds(tf: str) -> int:
    t = tf.lower()
    if t in ("1min", "1m"): return 60
    if t in ("5min", "5m"): return 300
    if t in ("15min", "15m"): return 900
    if t in ("1h", "60min"): return 3600
    return 300

@lru_cache(maxsize=4096)
def _ema(series_key: tuple, prices: tuple[float, ...], period: int) -> float:
    k = 2.0 / (period + 1.0)
    ema = prices[0]
    for p in prices[1:]:
        ema = p * k + ema * (1 - k)
    return ema

def ema_value(symbol: str, timeframe: str, closes: List[float], period: int) -> float:
    if not closes: return float("nan")
    key = (symbol, timeframe, len(closes), period)
    return _ema(key, tuple(closes), period)

def _price_above_ema_fast(symbol: str, timeframe: str, closes: List[float]) -> bool:
    try:
        ema = ema_value(symbol, timeframe, closes, GUARDS["ema_fast"])
        last = closes[-1] if closes else float("nan")
        return isfinite(ema) and isfinite(last) and (last >= ema)
    except Exception:
        return True  # fail-open

def _bps(pct: float) -> float:
    return pct * 1e4

def _spread_bps(last: float, bid: float = None, ask: float = None) -> float:
    if bid and ask and bid > 0 and ask > bid:
        mid = 0.5 * (bid + ask)
        return _bps((ask - bid) / mid)
    return 4.0

# -----------------------------------------------------------------------------
# Symbol Universe (multi-coin) ------------------------------------------------
# -----------------------------------------------------------------------------
_CURRENT_SYMBOLS: List[str] = []

def _builtin_candidates() -> List[str]:
    # Focus on symbols commonly available on Alpaca Crypto.
    # You can extend via CANDIDATE_SYMBOLS env.
    base = [
        "BTC/USD","ETH/USD","SOL/USD","DOGE/USD","ADA/USD","AVAX/USD","LTC/USD",
        "BCH/USD","LINK/USD","DOT/USD","MATIC/USD","XRP/USD","TRX/USD","ATOM/USD",
        "FIL/USD","NEAR/USD","APT/USD","ARB/USD","OP/USD","ETC/USD","ALGO/USD",
        "SUI/USD","AAVE/USD","UNI/USD","GRT/USD"
    ]
    extra = [s.strip().upper() for s in os.getenv("CANDIDATE_SYMBOLS","").split(",") if s.strip()]
    # Allow removing with EXCLUDE_SYMBOLS
    excl = set([s.strip().upper() for s in os.getenv("EXCLUDE_SYMBOLS","").split(",") if s.strip()])
    out = [s for s in (base + extra) if s not in excl]
    # Dedup & stable order
    seen = set(); final=[]
    for s in out:
        if s not in seen:
            final.append(s); seen.add(s)
    return final

def _try_import_marketcrypto():
    try:
        return importlib.import_module("services.market_crypto")
    except Exception:
        try:
            return importlib.import_module("market_crypto")
        except Exception:
            return None

def _build_universe_from_bars(timeframe: str, limit: int, max_symbols: int = 24) -> List[str]:
    """
    Pulls bars for a wide candidate set and keeps coins that returned enough rows.
    Uses universe.py's UniverseBuilder if available; otherwise falls back to a bar-count filter.
    """
    candidates = _builtin_candidates()
    mod = _try_import_marketcrypto()
    if not mod:
        log.warning("Universe: services.market_crypto not available; using candidates as-is.")
        return candidates[:max_symbols]

    try:
        MarketCrypto = getattr(mod, "MarketCrypto")
        mc = MarketCrypto.from_env()

        # ----- Try universe.py integration first -----
        try:
            import universe as uni
            cfg_cls = getattr(uni, "UniverseConfig", None)
            builder_cls = getattr(uni, "UniverseBuilder", None)
            if cfg_cls and builder_cls:
                cfg = cfg_cls()
                builder = builder_cls(cfg)
                frames = mc.candles(candidates, timeframe=timeframe, limit=min(limit, 600)) or {}

                class _Cache:
                    def __init__(self, _frames): self._f = _frames
                    def rows(self, sym, tf):
                        df = self._f.get(sym) or None
                        return int(getattr(df, "shape", [0,0])[0]) if df is not None else 0

                builder.refresh_from_cache_like_source(candidates, _Cache(frames))
                if getattr(builder, "symbols", None):
                    return builder.symbols[:max_symbols]
        except Exception:
            # If universe.py isn't present or errors, fall through to bar-count filter
            pass

        # ----- Fallback: simple row-count filter -----
        frames = mc.candles(candidates, timeframe=timeframe, limit=min(limit, 600)) or {}
        rows_ok = []
        for s, df in (frames or {}).items():
            try:
                n = int(getattr(df, "shape", [0,0])[0] or 0)
                if n >= max(150, int(limit*0.9)):  # enough history
                    rows_ok.append(s)
            except Exception:
                pass
        if not rows_ok:
            log.warning("Universe: no symbols passed rows filter; falling back to candidates.")
            return candidates[:max_symbols]
        return rows_ok[:max_symbols]
    except Exception as e:
        log.exception("Universe build failed; using candidates. %s", e)
        return candidates[:max_symbols]

def _load_universe_initial() -> List[str]:
    if _default_symbols_env:
        return _default_symbols_env
    return _build_universe_from_bars(DEFAULT_TIMEFRAME, DEFAULT_LIMIT)

def _set_current_symbols(syms: List[str]):
    global _CURRENT_SYMBOLS
    _CURRENT_SYMBOLS = [s.strip().upper() for s in syms if s and isinstance(s, str)]

# -----------------------------------------------------------------------------
# Strategy adapter
# -----------------------------------------------------------------------------
class RealStrategiesAdapter:
    def __init__(self):
        self._mods: Dict[str, Any] = {}

    def _get_mod(self, name: str):
        if name in self._mods: return self._mods[name]
        try:
            mod = importlib.import_module(f"strategies.{name}")
            log.info("adapter: imported strategies.%s", name)
        except Exception:
            mod = importlib.import_module(name)  # fallback if top-level
            log.info("adapter: imported %s (top-level)", name)
        self._mods[name] = mod
        return mod

    def reload(self, name: str) -> bool:
        """Hot-reload a single strategy module if already imported."""
        if name in self._mods:
            try:
                self._mods[name] = importlib.reload(self._mods[name])
                log.info("adapter: reloaded %s", name)
                return True
            except Exception as e:
                log.exception("adapter: reload failed for %s: %s", name, e)
                return False
        # If not imported yet, next scan will import it.
        return True

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
            # Each strategy implements run_scan(symbols, timeframe, limit, notional, dry, raw)
            result = mod.run_scan(symbols, tf, lim, notional, dry, raw)

            orders: List[Dict[str, Any]] = []
            if isinstance(result, dict):
                if isinstance(result.get("placed"), list):
                    orders = result["placed"]
                elif isinstance(result.get("orders"), list):
                    orders = result["orders"]
            elif isinstance(result, list):
                orders = result
            log.info("adapter: %s produced %d order(s)", strat, len(orders))
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
    # using selected broker module as br
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

    try: qty = float(qty)
    except Exception: qty = 0.0
    try: price = float(price)
    except Exception: price = 0.0
    try: notional = float(notional)
    except Exception: notional = 0.0

    if price <= 0: price = _get_last_price(sym)
    if qty <= 0 and notional and price: qty = round(notional / max(price,1e-9), 8)

    ts = (
        o.get("filled_at")
        or o.get("updated_at")
        or o.get("submitted_at")
        or o.get("created_at")
        or o.get("timestamp")
        or _now_iso()
    )

    strategy = (
        o.get("strategy") or _extract_strategy_from_order(o) or o.get("tag") or o.get("subtag") or "unknown"
    ).lower()

    pnl = o.get("pnl") or 0.0
    try: pnl = float(pnl)
    except Exception: pnl = 0.0

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
    if qty <= 0 or price <= 0: return 0.0
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
    if not _positions_state: return 0.0
    syms = [s for s, p in _positions_state.items() if p.get("qty", 0) > 0]
    if not syms: return 0.0
    try:
    # using selected broker module as br
        pmap = br.last_trade_map(syms)
    except Exception:
        pmap = {}
    eq = 0.0
    for s, p in _positions_state.items():
        q = float(p.get("qty") or 0)
        if q <= 0: continue
        last = float((pmap.get(s, {}) or {}).get("price") or 0.0)
        if last > 0: eq += q * last
    return eq

# -----------------------------------------------------------------------------
# App + state
# -----------------------------------------------------------------------------
app = FastAPI(title="Crypto System")

# Universe at startup
_set_current_symbols(_load_universe_initial())

# Ring buffer for recent orders/events (cap via env)
ORDERS_RING_CAP = int(os.getenv("ORDERS_RING_CAP", "1000"))
_orders_ring: deque = deque(maxlen=max(100, ORDERS_RING_CAP))

_attribution: Dict[str, Any] = {"by_strategy": {}, "updated_at": None}
_summary: Dict[str, Any] = {"equity": 0.0, "pnl_day": 0.0, "pnl_week": 0.0, "pnl_month": 0.0, "updated_at": None}

# ---- Strategy runtime toggles ----
ACTIVE_STRATEGIES = set(STRATEGIES)  # scheduler reads from this at runtime

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
        row["pnl"] = float(row.get("pnl") or 0.0) + float(r or 0.0)
        realized_total += float(r or 0.0)
        row["ts"] = time.time()
        _orders_ring.append(row)
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
    # using selected broker module as br
        bars_map = br.get_bars(symbols, timeframe=timeframe, limit=max(GUARDS["ema_slow"], 60)) or {}
        last_trade_map = br.last_trade_map(symbols) or {}
    except Exception:
        bars_map, last_trade_map = {}, {}
    for sym, pos in list(_positions_state.items()):
        qty = float(pos.get("qty") or 0.0)
        if qty <= 0: continue
        closes = [
            float(b.get("c") or b.get("close") or 0.0)
            for b in (bars_map.get(sym) or [])
            if float(b.get("c") or b.get("close") or 0.0) > 0
        ]
        if not closes: continue
        last = float((last_trade_map.get(sym, {}) or {}).get("price") or closes[-1] or 0.0)
        if last <= 0: continue
        entry = float(pos.get("avg_price") or 0.0)
        if entry <= 0: continue
        up_bps = _bps((last - entry) / entry)

        # Take profit
        if up_bps >= GUARDS["tp_target_bps"] and TRADING_ENABLED:
            try:
    # using selected broker module as br
                coid = f"nanny-tp-{sym.replace('/','')}-{int(time.time())}"
                br.submit_order(symbol=sym, side="sell", notional=min(DEFAULT_NOTIONAL, qty * last), client_order_id=coid)
                log.info("exit_nanny: TP sell placed for %s", sym)
                continue
            except Exception:
                log.exception("exit_nanny TP submit failed")

        # No-cross exit below slow EMA
        try:
            slow_ema = ema_value(sym, timeframe, closes, GUARDS["ema_slow"])
            crossed = isfinite(slow_ema) and (last < slow_ema)
        except Exception:
            crossed = False
        if crossed and TRADING_ENABLED:
            try:
    # using selected broker module as br
                coid = f"nanny-x-{sym.replace('/','')}-{int(time.time())}"
                br.submit_order(symbol=sym, side="sell", notional=min(DEFAULT_NOTIONAL, qty * last), client_order_id=coid)
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

    syms = req.get("symbols") or _CURRENT_SYMBOLS
    if isinstance(syms, str):
        syms = [s.strip() for s in syms.split(",") if s.strip()]
    req["symbols"] = syms
    req["dry"] = dry
    req["raw"] = dict(req)

    sb = StrategyBook()
    orders = sb.scan(req, {"one": {"timeframe": req["timeframe"], "symbols": syms, "notional": req["notional"]}})

    # Basic guard: EMA/spread sanity for BUYs
    try:
    # using selected broker module as br
        bars_map = br.get_bars(req["symbols"], timeframe=req["timeframe"], limit=60) or {}
    except Exception:
        bars_map = {}

    filtered: List[Dict[str, Any]] = []
    for o in (orders or []):
        side = (o.get("side") or o.get("order_side") or "").lower()
        sym = _sym_to_slash(o.get("symbol") or "")
        sname = (req.get("strategy") or "").lower()
        closes = [
            float(b.get("c") or b.get("close") or 0.0)
            for b in (bars_map.get(sym) or [])
            if float(b.get("c") or b.get("close") or 0.0) > 0
        ]
        px = float(o.get("price") or o.get("px") or 0.0)
        if px <= 0 and closes: px = closes[-1]
        edge_bps_val = float(o.get("edge_bps") or o.get("edge") or 0.0)
        spr_bps = _spread_bps(px)
        ema_ok = _price_above_ema_fast(sym, req["timeframe"], closes)
        if side == "buy":
            if not ema_ok or edge_bps_val < GUARDS["min_edge_bps"] or edge_bps_val < GUARDS["min_edge_vs_spread_x"] * spr_bps:
                log.info("guard: drop open %s by %s (ema_ok=%s edge=%.2f spread=%.2f)", sym, sname, ema_ok, edge_bps_val, spr_bps)
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
    dry_flag = (not TRADING_ENABLED)
    try:
        while _running:
            _roll_day_if_needed()
            # Use the runtime ACTIVE_STRATEGIES set
            for strat in list(ACTIVE_STRATEGIES):
                if strat in _DISABLED_STRATS:  # daily-stop gating hook
                    continue
                try:
                    orders = await _scan_bridge(
                        strat,
                        {
                            "timeframe": DEFAULT_TIMEFRAME,
                            "symbols": _CURRENT_SYMBOLS,
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
                    await _exit_nanny(_CURRENT_SYMBOLS, DEFAULT_TIMEFRAME)
                except Exception:
                    log.exception("exit nanny failed")
            log.info("Scheduler tick complete (dry=%d symbols=%d) — sleeping %ss", int(dry_flag), len(_CURRENT_SYMBOLS), SCHEDULE_SECONDS)
            await asyncio.sleep(SCHEDULE_SECONDS)
    finally:
        _running = False

@app.on_event("startup")
async def _startup():
    global _scheduler_task
    log.info("App startup; scheduler interval is %ss; strategies=%s; symbols=%d", SCHEDULE_SECONDS, ",".join(sorted(ACTIVE_STRATEGIES)), len(_CURRENT_SYMBOLS))
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
      const r = await fetch('/pnl/summary'); const d = await r.json();
      document.getElementById('eq').textContent = Number(d.equity || 0).toFixed(2);
      document.getElementById('pnl_day').textContent = Number(d.pnl_day || 0).toFixed(2);
      document.getElementById('pnl_week').textContent = Number(d.pnl_week || 0).toFixed(2);
      document.getElementById('pnl_month').textContent = Number(d.pnl_month || 0).toFixed(2);
      document.getElementById('updated').textContent = d.updated_at ? new Date(d.updated_at).toLocaleString() : '-';
    }
    async function loadAttribution(){
      const r = await fetch('/orders/attribution'); const d = await r.json();
      const tbody = document.getElementById('attr_body'); tbody.innerHTML = '';
      if (d.by_strategy){ for (const [k,v] of Object.entries(d.by_strategy)){
          const tr = document.createElement('tr'); tr.innerHTML = `<td>${k}</td><td>${Number(v).toFixed(2)}</td>`; tbody.appendChild(tr);
      }}
      document.getElementById('attr_updated').textContent = d.updated_at ? new Date(d.updated_at).toLocaleString() : '-';
    }
    async function loadOrders(){
      const r = await fetch('/orders/recent?limit=50'); const d = await r.json();
      const tbody = document.getElementById('orders_body'); tbody.innerHTML = '';
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
    async function loadUniverse(){
      const r = await fetch('/universe'); const d = await r.json();
      document.getElementById('sym_count').textContent = (d.symbols || []).length;
    }
    async function refreshAll(){ await Promise.all([loadSummary(), loadAttribution(), loadOrders(), loadUniverse()]); }
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
      <span class="chip">Symbols: <code id="sym_count">0</code></span>
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
    return {"ok": True, "version": APP_VERSION, "time": _now_iso(), "broker": BROKER}

# ---- Universe endpoints ----
@app.get("/universe", response_class=JSONResponse)
def get_universe():
    return {"symbols": _CURRENT_SYMBOLS, "count": len(_CURRENT_SYMBOLS), "timeframe": DEFAULT_TIMEFRAME}

@app.post("/universe/refresh", response_class=JSONResponse)
def refresh_universe(max_symbols: int = 24):
    syms = _build_universe_from_bars(DEFAULT_TIMEFRAME, DEFAULT_LIMIT, max_symbols=max_symbols)
    _set_current_symbols(syms)
    return {"ok": True, "symbols": _CURRENT_SYMBOLS, "count": len(_CURRENT_SYMBOLS)}

# ---- Strategy management endpoints ----
@app.get("/strategies", response_class=JSONResponse)
def strategies_list():
    names = sorted(set(STRATEGIES) | ACTIVE_STRATEGIES)
    out = [{"name": n, "enabled": (n in ACTIVE_STRATEGIES)} for n in names]
    return {"strategies": out}

@app.post("/strategies/enable", response_class=JSONResponse)
def strategies_enable(name: str):
    ACTIVE_STRATEGIES.add(name)
    return {"ok": True, "enabled": sorted(ACTIVE_STRATEGIES)}

@app.post("/strategies/disable", response_class=JSONResponse)
def strategies_disable(name: str):
    ACTIVE_STRATEGIES.discard(name)
    return {"ok": True, "enabled": sorted(ACTIVE_STRATEGIES)}

@app.post("/strategies/reload", response_class=JSONResponse)
def strategies_reload(name: str = ""):
    sb = StrategyBook()
    if name:
        ok = sb._impl.reload(name)
        return {"ok": ok, "reloaded": [name] if ok else []}
    reloaded = []
    for k in list(sb._impl._mods.keys()):
        if sb._impl.reload(k):
            reloaded.append(k)
    return {"ok": True, "reloaded": reloaded}

# ---- PnL & orders endpoints ----
@app.get("/pnl/summary", response_class=JSONResponse)
async def pnl_summary():
    return JSONResponse(_summary)

@app.get("/orders/recent", response_class=JSONResponse)
async def orders_recent(limit: int = Query(100, ge=1, le=1000)):
    items = list(_orders_ring)[-limit:] if _orders_ring else []
    mapped = []
    for o in items:
        ts_iso = o.get("time") or ""
        try:
            ts_epoch = int(datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp())
        except Exception:
            ts_epoch = int(o.get("ts") or time.time())
        mapped.append(
            {
                "id": o.get("id"),
                "symbol": o.get("symbol"),
                "side": o.get("side"),
                "qty": o.get("qty"),
                "px": o.get("price") or o.get("px"),
                "strategy": o.get("strategy"),
                "pnl": o.get("pnl"),
                "ts": ts_epoch,
            }
        )
    return JSONResponse({"orders": mapped, "updated_at": datetime.now(timezone.utc).isoformat()})

@app.get("/orders/attribution", response_class=JSONResponse)
async def orders_attribution():
    return JSONResponse(_attribution)

# ---- Analytics & diagnostics ----
@app.get("/analytics/trades", response_class=JSONResponse)
def analytics_trades(hours: int = 168):
    now = time.time()
    cutoff = now - (hours * 3600)
    items = [r for r in list(_orders_ring) if float(r.get("ts", now)) >= cutoff]
    out = []
    for r in items:
        out.append({
            "ts": float(r.get("ts", now)),
            "strategy": (r.get("strategy") or "unknown").lower(),
            "symbol": r.get("symbol") or r.get("sym") or "",
            "side": r.get("side") or "",
            "qty": r.get("qty") or r.get("quantity") or 0,
            "notional": r.get("notional") or 0.0,
            "pnl": float(r.get("pnl") or 0.0),
            "closed": bool(r.get("closed") or False),
        })
    return JSONResponse({"ok": True, "count": len(out), "rows": out})

@app.get("/analytics/trades.csv", response_class=PlainTextResponse)
async def analytics_trades_csv(hours: int = 12):
    orders = _fetch_filled_orders_last_hours(int(hours))
    rows = [_normalize_trade_row(o) for o in orders]
    buf = io.StringIO()
    w = csv.DictWriter(
        buf,
        fieldnames=[
            "time","strategy","symbol","side","qty","price","id","client_order_id","status","notional",
        ],
    )
    w.writeheader()
    for r in rows: w.writerow(r)
    return buf.getvalue()

def _fetch_filled_orders_last_hours(hours: int) -> list:
    # using selected broker module as br
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    raw = br.list_orders(status="all", limit=1000) or []
    out = []
    for o in raw:
        if not _order_is_fill_like(o): continue
        t = _parse_ts_any(
            o.get("filled_at") or o.get("updated_at") or o.get("submitted_at") or o.get("created_at")
        )
        if t and t >= since: out.append(o)
    out.sort(
        key=lambda r: _parse_ts_any(
            r.get("filled_at") or r.get("updated_at") or r.get("submitted_at") or r.get("created_at")
        ) or datetime.min.replace(tzinfo=timezone.utc)
    )
    return out

@app.get("/diag/orders_raw")
async def diag_orders_raw(status: str = "all", limit: int = 25):
    # using selected broker module as br
    data = br.list_orders(status=status, limit=limit) or []
    return {"status": status, "limit": limit, "orders": data}

@app.get("/diag/bars")
async def diag_bars(symbols: str = "", tf: str = "", limit: int = 360):
    # using selected broker module as br
    syms = [s.strip().upper() for s in (symbols or ",".join(_CURRENT_SYMBOLS)).split(",") if s.strip()]
    bars = br.get_bars(syms, timeframe=(tf or DEFAULT_TIMEFRAME), limit=int(limit))
    return {"timeframe": (tf or DEFAULT_TIMEFRAME), "limit": limit, "counts": {k: len(v) for k, v in bars.items()}, "keys": list(bars.keys())}

@app.get("/diag/imports")
async def diag_imports():
    out = {}
    for name in sorted(set(STRATEGIES) | ACTIVE_STRATEGIES):
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
    symbols: str = "",
    tf: Optional[str] = None,
    limit: Optional[int] = None,
    notional: Optional[float] = None,
    dry: int = 1,
):
    tf = tf or DEFAULT_TIMEFRAME
    limit = limit or DEFAULT_LIMIT
    notional = notional or DEFAULT_NOTIONAL
    syms = [s.strip() for s in (symbols or ",".join(_CURRENT_SYMBOLS)).split(",") if s.strip()]
    req = {"strategy": strategy, "timeframe": tf, "limit": limit, "notional": notional, "symbols": syms, "dry": bool(dry), "raw": {}}
    sb = StrategyBook()
    orders = sb.scan(req, {"one": {"timeframe": tf, "symbols": syms, "notional": notional}})
    return {"args": req, "orders_count": len(orders or []), "orders": orders}

@app.get("/diag/alpaca")
async def diag_alpaca():
    try:
    # using selected broker module as br
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
    now = time.time(); cutoff = now - hours*3600
    rows = [r for r in list(_orders_ring) if float(r.get("ts", now)) >= cutoff]
    per = defaultdict(list)
    for r in rows:
        per[(r.get("strategy") or "unknown").lower()].append(float(r.get("pnl") or 0.0))
    out = {}
    for sid, pnl in per.items():
        n = len(pnl)
        gp = sum(x for x in pnl if x > 0); gl = -sum(x for x in pnl if x < 0)
        pf = (gp / gl) if gl > 0 else (float("inf") if gp > 0 else 0.0)
        gp_r = sum(-x for x in pnl if x < 0); gl_r = -sum(-x for x in pnl if -x < 0)
        pf_r = (gp_r / gl_r) if gl_r > 0 else (float("inf") if gp_r > 0 else 0.0)
        out[sid] = {"N": n, "PF": round(pf, 3), "Expectancy": round((sum(pnl)/n) if n else 0.0, 4),
                    "PF_rev": round(pf_r, 3), "Expectancy_rev": round(((-sum(pnl))/n) if n else 0.0, 4)}
    return JSONResponse({"ok": True, "data": out})

@app.get("/metrics/scorecard", response_class=JSONResponse)
def metrics_scorecard(hours: int = 168, last_n_trades: int = 0):
    now = time.time()
    rows = list(_orders_ring)
    if hours and hours > 0:
        cutoff = now - (hours * 3600)
        rows = [r for r in rows if float(r.get("ts", now)) >= cutoff]
    per = defaultdict(list)
    for r in rows:
        sid = (r.get("strategy") or "unknown").lower()
        per[sid].append(r)
    if last_n_trades and last_n_trades > 0:
        per = {k: v[-last_n_trades:] for k, v in per.items()}
    out = {}
    for sid, items in per.items():
        pnl = [float(x.get("pnl") or 0.0) for x in items if x.get("pnl") is not None]
        n = len(items)
        wins = sum(1 for x in pnl if x > 0)
        losses = sum(1 for x in pnl if x < 0)
        gp = sum(x for x in pnl if x > 0); gl = -sum(x for x in pnl if x < 0)
        net = gp - gl
        win_rate = (wins / max(wins + losses, 1)) if (wins + losses) else 0.0
        exp = (net / n) if n else 0.0
        med = median(pnl) if pnl else 0.0
        pf = (gp / gl) if gl > 0 else (float("inf") if gp > 0 else 0.0)
        # simple running drawdown
        cum = 0.0; peak = 0.0; maxdd = 0.0
        for x in pnl:
            cum += x; peak = max(peak, cum); maxdd = min(maxdd, cum - peak)
        out[sid] = {
            "trades": n, "wins": wins, "losses": losses, "win_rate": round(win_rate, 4),
            "gross_pnl": round(gp, 2), "fees": 0.0, "net_pnl": round(net, 2),
            "avg_ret": 0.0, "std_ret": 0.0, "sharpe_like": 0.0,
            "median_pnl": round(med, 2), "max_drawdown": round(maxdd, 2),
        }
    return JSONResponse({"ok": True, "data": out, "count_strategies": len(out)})

@app.post("/init/positions")
async def init_positions():
    try:
    # using selected broker module as br
        pos = br.list_positions() or []
        global _positions_state
        _positions_state = {}
        for p in pos:
            sym = _sym_to_slash(p.get("symbol") or p.get("Symbol") or "")
            try:
                qty = float(p.get("qty") or p.get("quantity") or p.get("qty_available") or p.get("size") or p.get("Qty") or 0.0)
            except Exception:
                qty = 0.0
            avg = float(p.get("avg_entry_price") or p.get("average_entry_price") or p.get("avg_price") or 0.0)
            if sym and qty and avg:
                _positions_state[sym] = {"qty": qty, "avg_price": avg}
        _summary["equity"] = _recalc_equity()
        ts = _now_iso(); _summary["updated_at"] = ts; _attribution["updated_at"] = ts
        return {"ok": True, "positions": _positions_state}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/init/backfill")
async def init_backfill(days: Optional[int] = None, status: str = "closed"):
    try:
    # using selected broker module as br
        lookback_days = int(days or os.getenv("INIT_BACKFILL_DAYS", "7"))
        after = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        raws = br.list_orders(status=status, limit=1000) or []
        selected = []
        for r in raws:
            ts = r.get("filled_at") or r.get("updated_at") or r.get("submitted_at") or r.get("created_at")
            when = _parse_ts_any(ts) if isinstance(ts, str) else None
            if when and when >= after: selected.append(r)
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
