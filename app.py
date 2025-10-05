#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import time
import csv, io
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from datetime import datetime, timezone


# ---------- Analytics helpers ----------
def _parse_ts(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z","+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def _order_is_fill_like(o: dict) -> bool:
    st = (o.get("status") or "").lower()
    fq = float(o.get("filled_qty") or 0.0)
    fp = float(o.get("filled_avg_price") or 0.0)
    return (st in ("filled","partially_filled","done")) or (fq > 0 and fp > 0)

def _extract_strategy_from_order(o: dict) -> str:
    s = (o.get("strategy") or "").strip().lower()
    if s:
        return s
    coid = (o.get("client_order_id") or o.get("clientOrderId") or "").strip()
    if coid:
        token = coid.split("-")[0].strip().lower()
        if token:
            return token
    for k in ("tag","subtag","strategy_tag","algo"):
        v = (o.get(k) or "").strip().lower()
        if v:
            return v
    return "unknown"

def _norm_symbol(s: str) -> str:
    if not s: return s
    s = s.upper()
    return s if "/" in s else (s[:-3] + "/" + s[-3:] if len(s) > 3 else s)

def _sym_to_slash(s: str) -> str:
    if not s:
        return s
    s = s.upper()
    return s if "/" in s else (s[:-3] + "/" + s[-3:] if len(s) > 3 else s)

def _extract_strategy(coid: str, fallback: str = "") -> str:
    if not coid:
        return fallback
    # common pattern: c2-<timestamp>-btcusd or similar
    part = coid.split("-")[0].lower()
    return part if part else (fallback or "unknown")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _fetch_filled_orders_last_hours(hours: int) -> list:
    import broker as br
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    raw = br.list_orders(status="all", limit=1000) or []
    out = []
    for o in raw:
        if not _order_is_fill_like(o):
            continue
        t = _parse_ts(o.get("filled_at") or o.get("updated_at") or o.get("submitted_at") or o.get("created_at"))
        if t and t >= since:
            out.append(o)
    out.sort(key=lambda r: _parse_ts(r.get("filled_at") or r.get("updated_at") or r.get("submitted_at") or r.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc))
    return out

def _normalize_trade_row(o: dict) -> dict:
    sym = _norm_symbol(o.get("symbol") or o.get("asset_symbol") or "")
    side = (o.get("side") or "").lower()
    qty  = float(o.get("filled_qty") or o.get("qty") or o.get("quantity") or o.get("size") or 0.0)
    px   = float(o.get("filled_avg_price") or o.get("price") or o.get("limit_price") or o.get("avg_price") or 0.0)
    coid = o.get("client_order_id") or o.get("clientOrderId") or ""
    ts   = o.get("filled_at") or o.get("updated_at") or o.get("submitted_at") or o.get("created_at")
    return {
        "id": o.get("id") or coid,
        "symbol": sym,
        "side": side,
        "qty": qty,
        "price": px,
        "strategy": _extract_strategy_from_order(o),
        "time": (_parse_ts(ts) or datetime.now(timezone.utc)).isoformat(),
        "status": (o.get("status") or "").lower(),
        "client_order_id": coid,
        "notional": float(o.get("notional") or 0.0),
    }

def _compute_strategy_metrics(rows: list) -> dict:
    books = defaultdict(lambda: deque())  # (strategy,symbol) -> deque of [qty, price]
    stats = defaultdict(lambda: {
        "trades": 0, "wins": 0, "losses": 0,
        "gross_profit": 0.0, "gross_loss": 0.0,
        "net_pnl": 0.0, "volume": 0.0
    })
    realized_rows = []

    for r in rows:
        key = (r["strategy"], r["symbol"])
        side, qty, px = r["side"], float(r["qty"] or 0), float(r["price"] or 0)
        if qty <= 0 or px <= 0:
            continue

        if side == "buy":
            books[key].append([qty, px])
            stats[key]["volume"] += qty * px
        elif side == "sell":
            remaining = qty
            trade_realized = 0.0
            stats[key]["volume"] += qty * px
            while remaining > 0 and books[key]:
                lot_qty, lot_px = books[key][0]
                use = min(remaining, lot_qty)
                pnl = (px - lot_px) * use
                trade_realized += pnl
                lot_qty -= use
                remaining -= use
                if lot_qty <= 1e-12:
                    books[key].popleft()
                else:
                    books[key][0][0] = lot_qty
            if abs(trade_realized) > 0:
                sk = stats[key]
                sk["trades"] += 1
                sk["net_pnl"] += trade_realized
                if trade_realized >= 0:
                    sk["wins"] += 1
                    sk["gross_profit"] += trade_realized
                else:
                    sk["losses"] += 1
                    sk["gross_loss"] += -trade_realized
                realized_rows.append({**r, "realized_pnl": trade_realized})

    per_strategy = defaultdict(lambda: {
        "trades": 0, "wins": 0, "losses": 0, "gross_profit": 0.0,
        "gross_loss": 0.0, "net_pnl": 0.0, "profit_factor": None,
        "win_rate": None, "avg_trade": None, "volume": 0.0
    })
    for (strategy, _symbol), s in stats.items():
        agg = per_strategy[strategy]
        for k in ("trades","wins","losses","gross_profit","gross_loss","net_pnl","volume"):
            agg[k] += s[k]

    for strategy, s in per_strategy.items():
        t = s["trades"]
        gp, gl = s["gross_profit"], s["gross_loss"]
        s["profit_factor"] = (gp / gl) if gl > 0 else (None if gp == 0 else float("inf"))
        s["win_rate"] = (s["wins"] / t) if t > 0 else None
        s["avg_trade"] = (s["net_pnl"] / t) if t > 0 else None

    return {"per_strategy": per_strategy, "per_pair": stats, "realized_rows": realized_rows}

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s:app:%(message)s")
log = logging.getLogger("app")

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
# Use the env knob you’re already setting on Render
SCHEDULE_SECONDS = int(os.getenv("SCHEDULE_SECONDS", os.getenv("SCHEDULER_INTERVAL_SEC", "60")))

DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "300"))
DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "5Min")
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", os.getenv("ORDER_NOTIONAL", "25")))
DEFAULT_SYMBOLS = os.getenv("DEFAULT_SYMBOLS", "BTC/USD,ETH/USD").split(",")

TRADING_ENABLED = os.getenv("TRADING_ENABLED", "1") in ("1","true","True")
APP_VERSION = os.getenv("APP_VERSION", "2025.10.04-crypto-v2")

STRATEGIES = [s.strip() for s in os.getenv("STRATEGY_LIST", "c1,c2,c3,c4,c5,c6").split(",") if s.strip()]

# -----------------------------------------------------------------------------
# Strategy adapter (real c1..c6.run_scan)
# -----------------------------------------------------------------------------
import importlib

class RealStrategiesAdapter:
    def __init__(self):
        self._mods: Dict[str, Any] = {}

    def _get_mod(self, name: str):
        if name in self._mods:
            return self._mods[name]
        try:
            mod = importlib.import_module(f"strategies.{name}")
            logging.getLogger("app").info("adapter: imported strategies.%s", name)
        except Exception:
            mod = importlib.import_module(name)  # fallback if in top-level
            logging.getLogger("app").info("adapter: imported %s (top-level)", name)
        self._mods[name] = mod
        return mod

    def scan(self, req: Dict[str, Any], _contexts: Dict[str, Any]) -> List[Dict[str, Any]]:
        log = logging.getLogger("app")
        strat = (req.get("strategy") or "").lower()
        if not strat:
            log.warning("adapter: missing 'strategy' in req")
            return []

        tf = req.get("timeframe") or os.getenv("DEFAULT_TIMEFRAME", "5Min")
        lim = int(req.get("limit") or int(os.getenv("DEFAULT_LIMIT", "300")))
        notional = float(req.get("notional") or float(os.getenv("DEFAULT_NOTIONAL", "25")))
        symbols = req.get("symbols") or []
        if isinstance(symbols, str):
            symbols = [s.strip() for s in symbols.split(",") if s.strip()]
        else:
            symbols = [s.strip() for s in symbols]
        dry = bool(req.get("dry", False))
        raw = req.get("raw") or {}

        try:
            mod = self._get_mod(strat)
            log.info("adapter: calling %s.run_scan syms=%s tf=%s lim=%s notional=%s dry=%s",
                     strat, ",".join(symbols), tf, lim, notional, int(dry))
            result = mod.run_scan(symbols, tf, lim, notional, dry, raw)

            # normalize
            orders: List[Dict[str, Any]] = []
            if isinstance(result, dict):
                if isinstance(result.get("placed"), list):
                    orders = result["placed"]
                elif isinstance(result.get("orders"), list):
                    orders = result["orders"]
            elif isinstance(result, list):
                orders = result

            log.info("adapter: %s produced %d order(s)", strat, len(orders))
            if len(orders) == 0:
                # helpful breadcrumb for debugging strategy returns
                log.info("adapter: %s raw result keys=%s type=%s", strat, list(result.keys()) if isinstance(result, dict) else "-", type(result).__name__)
            return orders or []
        except Exception as e:
            log.exception("adapter: scan failed for %s: %s", strat, e)
            return []

# keep a tiny facade so downstream code doesn’t change
class StrategyBook:
    def __init__(self):
        self._impl = RealStrategiesAdapter()

    def scan(self, req: Dict[str, Any], contexts: Optional[Dict[str, Any]] = None):
        return self._impl.scan(req, contexts or {})


_positions_state = {}  # symbol -> {"qty": float, "avg_price": float}

def _get_last_price(symbol_slash: str) -> float:
    try:
        import broker as br
        pmap = br.last_trade_map([symbol_slash])
        px = float(pmap.get(symbol_slash, {}).get("price") or 0.0)
        return px
    except Exception:
        return 0.0

def _normalize_order(o: dict) -> dict:
    """
    Populate columns: id, symbol, side, qty, price, strategy, pnl, time
    and keep legacy keys for compatibility.
    """
    coid = o.get("client_order_id") or o.get("clientOrderId") or o.get("client_orderid") or ""
    oid  = o.get("id") or o.get("order_id") or coid or ""
    raw_sym = o.get("symbol") or o.get("Symbol") or o.get("asset_symbol") or ""
    sym  = _sym_to_slash(raw_sym)
    side = (o.get("side") or o.get("Side") or o.get("order_side") or "").lower()
    status = (o.get("status") or o.get("Status") or "").lower()

    # qty / price / notional with many fallbacks
    qty = (
        o.get("filled_qty") or o.get("qty") or o.get("quantity") or
        o.get("size") or o.get("filled_quantity") or 0
    )
    price = (
        o.get("filled_avg_price") or o.get("price") or
        o.get("limit_price") or o.get("avg_price") or 0
    )
    notional = o.get("notional") or o.get("notional_value") or 0

    try: qty = float(qty)
    except Exception: qty = 0.0
    try: price = float(price)
    except Exception: price = 0.0
    try: notional = float(notional)
    except Exception: notional = 0.0

    if price <= 0:
        # fall back to latest trade to display *something*
        price = _get_last_price(sym)
    if qty <= 0 and notional and price:
        qty = round(notional / price, 8)

    # timestamps: prefer filled_at > updated_at > submitted/created
    ts = (
        o.get("filled_at") or o.get("updated_at") or
        o.get("submitted_at") or o.get("created_at") or
        o.get("timestamp")
    )
    ts = ts or _now_iso()

    # Strategy extraction
    strategy = (o.get("strategy") or _extract_strategy(coid, "") or o.get("tag") or o.get("subtag") or "")

    # pnl (if provided by upstream; else computed later)
    pnl = o.get("pnl") or 0.0
    try: pnl = float(pnl)
    except Exception: pnl = 0.0

    return {
        "id": oid, "symbol": sym, "side": side, "qty": qty, "price": price,
        "strategy": (strategy or "unknown").lower(), "pnl": pnl, "time": ts,
        "client_order_id": coid, "status": status, "notional": notional,
    }

def _apply_realized_pnl(row: dict) -> float:
    """
    Update a tiny in-memory position book and return realized P&L for this fill.
    Assumes 'qty' and 'price' are set on the normalized row.
    Only computes when we have both qty>0 and price>0.
    """
    sym = row.get("symbol")
    side = (row.get("side") or "").lower()
    qty = float(row.get("qty") or 0)
    price = float(row.get("price") or 0)
    status = row.get("status") or ""
    # If the order isn’t filled yet, skip PnL math—just display the row
    # If your Alpaca webhook/refresh later adds fill info, this will catch it then.
    if qty <= 0 or price <= 0:
        return 0.0

    pos = _positions_state.setdefault(sym, {"qty": 0.0, "avg_price": 0.0})
    realized = 0.0

    if side == "buy":
        # new avg = (old_cost + new_cost) / new_qty
        new_qty = pos["qty"] + qty
        if new_qty > 0:
            pos["avg_price"] = ((pos["qty"] * pos["avg_price"]) + (qty * price)) / new_qty
        pos["qty"] = new_qty

    elif side == "sell":
        # realized PnL on the sold size at current avg
        sell_qty = min(qty, max(pos["qty"], 0.0))
        if sell_qty > 0:
            realized = (price - pos["avg_price"]) * sell_qty
            pos["qty"] = pos["qty"] - sell_qty
            # keep avg the same for remaining qty; if flat, leave last avg
    return realized

def _recalc_equity() -> float:
    """Mark-to-market equity of open positions using last trade prices."""
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

# -----------------------------------------------------------------------------
# App + state
# -----------------------------------------------------------------------------
app = FastAPI(title="Crypto System")

_orders_ring: List[Dict[str, Any]] = []
_attribution: Dict[str, Any] = {"by_strategy": {}, "updated_at": None}
_summary: Dict[str, Any] = {"equity": 0.0, "pnl_day": 0.0, "pnl_week": 0.0, "pnl_month": 0.0, "updated_at": None}

def _push_orders(orders: List[Dict[str, Any]]):
    """
    - Normalize each incoming order so dashboard columns are populated.
    - Compute realized P&L on sells; update attribution & summary.
    - Keep ring buffer compatible with your existing /orders/recent endpoint.
    """
    global _orders_ring, _attribution, _summary

    if not orders:
        # still bump the timestamp so "Last Updated" moves
        ts = _now_iso()
        _summary["updated_at"] = ts
        _attribution["updated_at"] = ts
        return

    realized_total = 0.0

    for o in orders:
        row = _normalize_order(o)

        # compute realized pnl if qty/price are known (typically when filled data is present)
        try:
            r = _apply_realized_pnl(row)
        except Exception:
            r = 0.0
        row["pnl"] = float(row.get("pnl") or 0.0) + float(r or 0.0)
        realized_total += float(r or 0.0)

        # ring buffer
        _orders_ring.append(row)
        if len(_orders_ring) > 1000:
            _orders_ring = _orders_ring[-1000:]

        # attribution by strategy (realized only)
        strat = (row.get("strategy") or "unknown").lower()
        _attribution["by_strategy"][strat] = _attribution["by_strategy"].get(strat, 0.0) + float(r or 0.0)

    # summary updates
    ts = _now_iso()
    _attribution["updated_at"] = ts
    _summary["updated_at"] = ts

    # realized PnL goes into the day bucket (simple running total from app start)
    _summary["pnl_day"] = float(_summary.get("pnl_day") or 0.0) + realized_total
    # week/month rollups could be added later if you want date-aware buckets

    # equity: mark-to-market of open positions (approx)
    try:
        _summary["equity"] = _recalc_equity()
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Scan bridge (unchanged external behavior)
# -----------------------------------------------------------------------------
async def _scan_bridge(strat: str, req: Dict[str, Any], dry: bool = False) -> List[Dict[str, Any]]:
    req = dict(req or {})
    req.setdefault("strategy", strat)
    req.setdefault("timeframe", req.get("tf") or DEFAULT_TIMEFRAME)
    req.setdefault("limit", req.get("limit") or DEFAULT_LIMIT)
    req.setdefault("notional", req.get("notional") or DEFAULT_NOTIONAL)
    # normalize symbols
    syms = req.get("symbols")
    if not syms:
        syms = DEFAULT_SYMBOLS
    if isinstance(syms, str):
        syms = [s.strip() for s in syms.split(",") if s.strip()]
    req["symbols"] = syms
    req["dry"] = dry
    # passthrough original payload for strategies that read extra knobs
    req["raw"] = dict(req)

    sb = StrategyBook()
    orders = sb.scan(req, {"one": {"timeframe": req["timeframe"], "symbols": syms, "notional": req["notional"]}})
    # normalize
    if not orders:
        return []
    if isinstance(orders, dict):
        return orders.get("orders") or orders.get("placed") or []
    return list(orders)

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

# -----------------------------------------------------------------------------
# HTML (full inline page)
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
      --bg:#0b1220;
      --panel:#111a2b;
      --ink:#e6edf3;
      --muted:#a6b3c2;
      --accent:#5dd4a3;
      --accent2:#66a3ff;
      --red:#ff6b6b;
      --chip:#1a2336;
      --chip-br:#26324a;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
      background: linear-gradient(160deg, #0b1220 0%, #0e172a 100%);
      color:var(--ink);
    }
    header{
      padding:20px 24px;
      border-bottom:1px solid #16233b;
      display:flex;align-items:center;gap:12px;
    }
    .badge{
      font-size:12px;
      background:var(--chip);
      border:1px solid var(--chip-br);
      color:var(--muted);
      padding:4px 8px;border-radius:999px;
    }
    main{padding:24px;max-width:1200px;margin:0 auto}
    .grid{
      display:grid;
      grid-template-columns: repeat(12, 1fr);
      gap:16px;
    }
    .card{
      background:var(--panel);
      border:1px solid #1a2740;
      border-radius:14px;
      padding:16px;
      box-shadow: 0 10px 24px rgba(0,0,0,.2);
    }
    .span-4{grid-column: span 4}
    .span-6{grid-column: span 6}
    .span-8{grid-column: span 8}
    .span-12{grid-column: span 12}
    h1{font-size:18px;margin:0}
    .muted{color:var(--muted)}
    .row{display:flex;align-items:center;justify-content:space-between;gap:12px}
    .kpi{font-size:28px;font-weight:700}
    .good{color:var(--accent)}
    .bad{color:var(--red)}
    table{width:100%;border-collapse:collapse;font-size:14px}
    th,td{padding:8px;border-bottom:1px solid #1a2740;text-align:left}
    th{color:#9db0c9;font-weight:600}
    .chips{display:flex;flex-wrap:wrap;gap:8px}
    .chip{
      background:var(--chip);
      border:1px solid var(--chip-br);
      padding:6px 10px;border-radius:999px;font-size:12px;color:#c7d2e3;
    }
    a.btn{
      display:inline-block;padding:8px 12px;border-radius:10px;text-decoration:none;
      background:var(--accent2);color:#0b1220;font-weight:700;border:1px solid #2a3f6b;
    }
    footer{padding:28px;color:var(--muted);text-align:center}
    @media (max-width: 900px){
      .span-4,.span-6,.span-8{grid-column: span 12}
    }
    code{
      background:#0d1628;border:1px solid #1a2740;padding:2px 6px;border-radius:6px
    }
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
    async function refreshAll(){
      await Promise.all([loadSummary(), loadAttribution(), loadOrders()]);
    }
    setInterval(refreshAll, 15000);
    window.addEventListener('load', refreshAll);
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
          <div class="span-4">
            <div class="muted">Equity</div>
            <div class="kpi" id="eq">0.00</div>
          </div>
          <div class="span-4">
            <div class="muted">PnL (Day)</div>
            <div class="kpi good" id="pnl_day">0.00</div>
          </div>
          <div class="span-4">
            <div class="muted">PnL (Week)</div>
            <div class="kpi" id="pnl_week">0.00</div>
          </div>
          <div class="span-4">
            <div class="muted">PnL (Month)</div>
            <div class="kpi" id="pnl_month">0.00</div>
          </div>
          <div class="span-8">
            <div class="muted">Last Updated</div>
            <div id="updated" class="kpi" style="font-size:16px">-</div>
          </div>
        </div>
      </section>

      <section class="card span-4">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">Attribution</h2>
        </div>
        <table>
          <thead><tr><th>Strategy</th><th>PnL</th></tr></thead>
          <tbody id="attr_body"></tbody>
        </table>
        <div class="muted" style="margin-top:8px">Updated: <span id="attr_updated">-</span></div>
      </section>

      <section class="card span-12">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">Recent Orders</h2>
        </div>
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Price</th>
              <th>Strategy</th><th>PnL</th><th>Time</th>
            </tr>
          </thead>
          <tbody id="orders_body"></tbody>
        </table>
      </section>
    </div>
  </main>

  <footer>
    Built with FastAPI • Tick interval: {SCHEDULE_SECONDS}s
  </footer>

  <script>
    document.getElementById('tf_chip').textContent = "{DEFAULT_TIMEFRAME}";
  </script>
</body>
</html>
""".replace("{SCHEDULE_SECONDS}", str(SCHEDULE_SECONDS)).replace("{DEFAULT_TIMEFRAME}", DEFAULT_TIMEFRAME)

# -----------------------------------------------------------------------------
# Routes (unchanged)
# -----------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/dashboard", status_code=307)

@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    return HTMLResponse(_DASHBOARD_HTML)

@app.get("/pnl/summary", response_class=JSONResponse)
async def pnl_summary():
    return JSONResponse(_summary)

@app.get("/orders/recent", response_class=JSONResponse)
async def orders_recent(limit: int = Query(100, ge=1, le=1000)):
    items = _orders_ring[-limit:] if _orders_ring else []
    return JSONResponse({"orders": items, "updated_at": datetime.now(timezone.utc).isoformat()})

@app.get("/orders/attribution", response_class=JSONResponse)
async def orders_attribution():
    return JSONResponse(_attribution)

from fastapi.responses import PlainTextResponse

@app.get("/analytics/trades")
async def analytics_trades(hours: int = 12):
    orders = _fetch_filled_orders_last_hours(int(hours))
    rows = [_normalize_trade_row(o) for o in orders]
    metrics = _compute_strategy_metrics(rows)
    per_strategy = {k: dict(v) for k, v in metrics["per_strategy"].items()}
    per_pair = {f"{k[0]}::{k[1]}": dict(v) for k, v in metrics["per_pair"].items()}
    return {
        "window_hours": hours,
        "summary_per_strategy": per_strategy,
        "detail_per_strategy_symbol": per_pair,
        "realized_trades": metrics["realized_rows"],
        "count_orders_considered": len(rows)
    }

@app.get("/analytics/trades.csv", response_class=PlainTextResponse)
async def analytics_trades_csv(hours: int = 12):
    orders = _fetch_filled_orders_last_hours(int(hours))
    rows = [_normalize_trade_row(o) for o in orders]
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["time","strategy","symbol","side","qty","price","id","client_order_id","status","notional"])
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()

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
                qty = float(p.get("qty") or p.get("quantity") or p.get("qty_available") or p.get("size") or 0.0)
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
async def init_backfill(days: int = None, status: str = "closed"):
    """
    Pull recent filled orders and replay them into the P&L/attribution.
    - days: lookback (defaults to INIT_BACKFILL_DAYS or 7)
    - status: 'closed' or 'all' (Alpaca v2)
    """
    try:
        import broker as br
        lookback_days = int(days or os.getenv("INIT_BACKFILL_DAYS", "7"))
        after = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        raws = br.list_orders(status=status, limit=1000) or []
        selected = []
        for r in raws:
            ts = r.get("filled_at") or r.get("updated_at") or r.get("submitted_at") or r.get("created_at")
            try:
                when = datetime.fromisoformat(ts.replace("Z","+00:00")) if isinstance(ts, str) else None
            except Exception:
                when = None
            if when and when >= after:
                selected.append(r)

        orders = []
        for r in selected:
            st = (r.get("status") or "").lower()
            filled_px = float(r.get("filled_avg_price") or 0.0)
            filled_qty = float(r.get("filled_qty") or 0.0)
            if st in ("filled","partially_filled","done") or (filled_px > 0 and filled_qty > 0):
                orders.append(r)

        _push_orders(orders)
        return {"ok": True, "considered": len(raws), "selected": len(selected), "replayed": len(orders)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/diag/orders_raw")
async def diag_orders_raw(status: str = "all", limit: int = 25):
    import broker as br
    data = br.list_orders(status=status, limit=limit) or []
    return {"status": status, "limit": limit, "orders": data}


@app.get("/healthz", response_class=JSONResponse, include_in_schema=False)
async def healthz():
    return JSONResponse({"ok": True, "ts": time.time(), "version": APP_VERSION})
    
from fastapi import HTTPException

@app.get("/diag/bars")
async def diag_bars(symbols: str = "BTC/USD,ETH/USD", tf: str = "5Min", limit: int = 360):
    import broker as br
    syms = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    bars = br.get_bars(syms, timeframe=tf, limit=int(limit))
    return {
        "timeframe": tf,
        "limit": limit,
        "counts": {k: len(v) for k, v in bars.items()},
        "keys": list(bars.keys())
    }

@app.get("/diag/imports")
async def diag_imports():
    out = {}
    for name in [s.strip() for s in os.getenv("STRATEGY_LIST", "c1,c2,c3,c4,c5,c6").split(",") if s.strip()]:
        try:
            import importlib
            try:
                importlib.import_module(f"strategies.{name}")
                out[name] = "ok: strategies.%s" % name
            except Exception:
                importlib.import_module(name)
                out[name] = "ok: top-level %s" % name
        except Exception as e:
            out[name] = f"ERROR: {e}"
    return out

@app.get("/diag/scan")
async def diag_scan(strategy: str, symbols: str = "BTC/USD,ETH/USD",
                    tf: str = None, limit: int = None, notional: float = None, dry: int = 1):
    tf = tf or os.getenv("DEFAULT_TIMEFRAME", "5Min")
    limit = limit or int(os.getenv("DEFAULT_LIMIT", "300"))
    notional = notional or float(os.getenv("DEFAULT_NOTIONAL", "25"))
    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    req = {
        "strategy": strategy,
        "timeframe": tf,
        "limit": limit,
        "notional": notional,
        "symbols": syms,
        "dry": bool(dry),
        "raw": {}
    }
    sb = StrategyBook()
    orders = sb.scan(req, {"one": {"timeframe": tf, "symbols": syms, "notional": notional}})
    return {"args": req, "orders_count": len(orders or []), "orders": orders}

@app.get("/diag/alpaca")
async def diag_alpaca():
    try:
        import broker as br
        pos = br.list_positions()
        bars = br.get_bars(["BTC/USD","ETH/USD"], timeframe=os.getenv("DEFAULT_TIMEFRAME","5Min"), limit=3)
        return {"positions_len": len(pos if isinstance(pos, list) else []),
                "bars_keys": list(bars.keys()),
                "bars_len_each": {k: len(v) for k, v in bars.items()}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from fastapi import HTTPException
from datetime import datetime, timedelta, timezone

def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")

@app.post("/init/positions")
async def init_positions():
    """Seed the in-memory positions state from Alpaca current positions."""
    try:
        import broker as br
        pos = br.list_positions() or []
        # Reset local state
        global _positions_state
        _positions_state = {}
        for p in pos:
            sym = _sym_to_slash(p.get("symbol") or p.get("Symbol") or "")
            qty = float(p.get("qty") or p.get("quantity") or p.get("qty_available") or p.get("qty_i") or p.get("Qty") or 0.0)
            try:
                # Alpaca uses string qty; average_entry_price for crypto
                qty = float(p.get("qty") or p.get("quantity") or p.get("qty_available") or p.get("qty_i") or p.get("Qty") or p.get("size") or 0.0)
            except Exception:
                pass
            avg = float(p.get("avg_entry_price") or p.get("average_entry_price") or p.get("avg_price") or 0.0)
            if sym and qty and avg:
                _positions_state[sym] = {"qty": qty, "avg_price": avg}
        # refresh equity
        _summary["equity"] = _recalc_equity()
        ts = _now_iso()
        _summary["updated_at"] = ts
        _attribution["updated_at"] = ts
        return {"ok": True, "positions": _positions_state}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/init/backfill")
async def init_backfill(days: int = None, status: str = "closed"):
    """
    Pull recent filled orders and replay them into the P&L/attribution.
    - days: lookback (defaults to INIT_BACKFILL_DAYS or 7)
    - status: 'closed' or 'all' (Alpaca v2)
    """
    try:
        import broker as br
        lookback_days = int(days or os.getenv("INIT_BACKFILL_DAYS", "7"))
        after = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        # broker.list_orders supports status/limit; we add simple 'after' filtering here
        raws = br.list_orders(status=status, limit=1000) or []
        # filter by 'filled_at' or 'updated_at' >= after
        selected = []
        for r in raws:
            ts = r.get("filled_at") or r.get("updated_at") or r.get("submitted_at") or r.get("created_at")
            try:
                when = datetime.fromisoformat(ts.replace("Z","+00:00")) if isinstance(ts, str) else None
            except Exception:
                when = None
            if when and when >= after:
                selected.append(r)

        # Normalize & push so P&L/Attribution update
        # We only push FILLEDs (or anything with filled_avg_price>0)
        orders = []
        for r in selected:
            st = (r.get("status") or "").lower()
            filled_px = float(r.get("filled_avg_price") or 0.0)
            filled_qty = float(r.get("filled_qty") or 0.0)
            if st in ("filled","partially_filled","done") or (filled_px > 0 and filled_qty > 0):
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
    log.info("Launching Uvicorn on 0.0.0.0:%d", port)
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
