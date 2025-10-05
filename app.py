#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from datetime import datetime, timezone

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
    Produce a row with the columns your dashboard shows, while keeping legacy keys too.
    Returns a dict with at least: id, symbol, side, qty, price, strategy, pnl, time
    """
    # raw fields, various naming possibilities
    coid = o.get("client_order_id") or o.get("clientOrderId") or ""
    oid  = o.get("id") or coid or ""
    sym  = _sym_to_slash(o.get("symbol") or o.get("Symbol") or "")
    side = (o.get("side") or o.get("Side") or "").lower()
    status = o.get("status") or o.get("Status") or ""
    # quantities & prices
    qty = o.get("qty") or o.get("quantity") or o.get("filled_qty") or o.get("filled_quantity") or 0
    price = o.get("filled_avg_price") or o.get("price") or o.get("limit_price") or 0
    notional = o.get("notional") or 0
    # convert numerics
    try: qty = float(qty)
    except Exception: qty = 0.0
    try: price = float(price)
    except Exception: price = 0.0
    try: notional = float(notional)
    except Exception: notional = 0.0

    # if no price, fetch a last trade to fill the display
    if price <= 0:
        price = _get_last_price(sym)

    # if no qty but we have a notional + price, estimate qty for display
    if qty <= 0 and notional and price:
        qty = round(notional / price, 8)

    # strategy column
    strategy = (o.get("strategy") or _extract_strategy(coid, ""))

    # time column: prefer submitted/filled timestamps if present
    ts = o.get("filled_at") or o.get("submitted_at") or o.get("timestamp") or _now_iso()

    # default pnl per-order (we’ll compute realized PnL below on sells)
    pnl = o.get("pnl") or 0.0
    try: pnl = float(pnl)
    except Exception: pnl = 0.0

    row = {
        # columns your UI shows
        "id": oid,
        "symbol": sym,
        "side": side,
        "qty": qty,
        "price": price,
        "strategy": strategy,
        "pnl": pnl,
        "time": ts,

        # keep legacy keys so older UI rows still render something
        "client_order_id": coid,
        "status": status,
        "notional": notional,
    }
    return row

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


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn  # type: ignore
    port = int(os.getenv("PORT", "10000"))
    log.info("Launching Uvicorn on 0.0.0.0:%d", port)
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
