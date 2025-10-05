#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
SCHEDULE_SECONDS = int(os.getenv("SCHEDULE_SECONDS", "60"))
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "100"))
DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "1h")
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", "0"))
DEFAULT_SYMBOLS = os.getenv("DEFAULT_SYMBOLS", "")  # comma list
APP_VERSION = os.getenv('APP_VERSION', '2025.10.04-crypto-v2')
TRADING_ENABLED = os.getenv('TRADING_ENABLED','1') in ('1','true','True')

# Strategies to iterate on each tick (match your logs)
STRATEGIES = ["c1", "c2", "c3", "c4", "c5", "c6"]

# -----------------------------------------------------------------------------
# StrategyBook facade (now backed by real strategies)
# -----------------------------------------------------------------------------
_real_SB = None

# Real strategies adapter: dispatch to c1..c6.run_scan and return placed orders
import importlib

class _RealStrategiesAdapter:
    def __init__(self):
        self._cache = {}

    def _load(self, name: str):
        if name in self._cache:
            return self._cache[name]
        mod = None
        # try strategies.cN first, then flat cN
        try:
            mod = importlib.import_module(f"strategies.{name}")
        except Exception:
            mod = importlib.import_module(name)
        self._cache[name] = mod
        return mod

    def scan(self, req, contexts):
        strat = (req.get("strategy") or "").lower()
        if not strat:
            return []
        tf = req.get("timeframe")
        lim = int(req.get("limit") or 300)
        notional = float(req.get("notional") or 0)
        syms = req.get("symbols") or []
        if isinstance(syms, str):
            syms = [s.strip() for s in syms.split(",") if s.strip()]
        dry = bool(req.get("dry", False))
        extra = req.get("raw") or {}
        try:
            mod = self._load(strat)
            res = mod.run_scan(syms, tf, lim, notional, dry, extra)
            # Prefer 'placed' array if present
            if isinstance(res, dict) and isinstance(res.get("placed"), list):
                return res["placed"]
            # else allow returning list directly
            if isinstance(res, list):
                return res
            return []
        except Exception as e:
            log.error("Real adapter scan failed for %s: %s", strat, e)
            return []

_real_SB = _RealStrategiesAdapter

class _FallbackStrategyBook:
    """No-op fallback so the service boots even without the real book."""
    def scan(self, req: Dict[str, Any], contexts: Dict[str, Any]):
        return []

def _ensure_ctx_shape(ctx_map: Optional[Dict[str, Any]], tf: str, symbols: List[str], notional: float) -> Dict[str, Any]:
    """Make sure contexts has at least 'one' and 'default' with basic fields."""
    ctx_map = ctx_map or {}
    if not isinstance(ctx_map, dict):
        ctx_map = {}
    base = {"timeframe": tf, "symbols": symbols, "notional": notional}
    one = ctx_map.get("one") if isinstance(ctx_map.get("one"), dict) else {}
    default = ctx_map.get("default") if isinstance(ctx_map.get("default"), dict) else {}
    one = {**base, **one}
    default = {**base, **(default or one)}
    ctx_map["one"] = one
    ctx_map["default"] = default
    return ctx_map

class StrategyBook:  # compatibility facade
    """
    Facade that wraps an implementation which understands scan(req, contexts).
    Also repairs missing req/context shapes for older strategies on the fly.
    """
    def __init__(self):
        try:
            self._impl = _real_SB() if _real_SB else _FallbackStrategyBook()
        except Exception as e:
            log.warning("StrategyBook impl init failed: %s; using fallback.", e)
            self._impl = _FallbackStrategyBook()
        self._warned: Dict[str, bool] = {}

    def _once_warn(self, strategy: str, msg: str):
        if not self._warned.get(strategy):
            log.warning(msg)
            self._warned[strategy] = True

    def scan(self, req: Dict[str, Any], contexts: Optional[Dict[str, Any]] = None):
        # keep the same normalization semantics you already had
        strategy = req.get("strategy") or "unknown"
        tf = req.get("timeframe") or DEFAULT_TIMEFRAME
        symbols = req.get("symbols") or [s.strip() for s in DEFAULT_SYMBOLS.split(",") if s.strip()]
        notional = float(req.get("notional") or DEFAULT_NOTIONAL)

        contexts = _ensure_ctx_shape(contexts, tf, symbols, notional)
        req = dict(req)
        req.setdefault("one", contexts["one"])
        req.setdefault("default", contexts["default"])
        req.setdefault("contexts", contexts)

        try:
            return self._impl.scan(req, contexts)
        except KeyError as ke:
            if str(ke).strip("'\"") in ("one", "default"):
                self._once_warn(strategy, "scan: strategy '{}' accessed missing key 'one'; repairing payload and retrying once.".format(strategy))
                # Rebuild shapes then retry once
                contexts = _ensure_ctx_shape(contexts, tf, symbols, notional)
                req["one"] = contexts["one"]
                req["default"] = contexts["default"]
                req["contexts"] = contexts
                try:
                    return self._impl.scan(req, contexts)
                except Exception as e2:
                    self._once_warn(strategy, f"scan retry still failed for '{strategy}': {e2}")
                    return []
            else:
                self._once_warn(strategy, f"scan raised KeyError for '{strategy}': {ke}")
                return []
        except TypeError as te:
            # As a last resort, attempt a legacy (req-only) call if the impl accepts it
            self._once_warn(strategy, f"scan TypeError for '{strategy}' with (req, contexts): {te}. Trying (req) only.")
            try:
                return self._impl.scan(req)  # type: ignore[arg-type]
            except Exception as e2:
                self._once_warn(strategy, f"scan (req) also failed for '{strategy}': {e2}")
                return []
        except Exception as e:
            self._once_warn(strategy, f"scan failed for '{strategy}': {e}")
            return []

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(title="Crypto System")

# ... (all your existing state stores, ring buffers, and helpers stay unchanged)

# -----------------------------------------------------------------------------
# Helpers to prepare contexts (unchanged)
# -----------------------------------------------------------------------------
def _prepare_contexts(req: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accepts a variety of incoming shapes (one/default, contexts map, list, or scalar tag)
    and guarantees `one` and `default` entries with timeframe/symbols/notional backfilled.
    """
    tf = req.get("timeframe") or DEFAULT_TIMEFRAME
    symbols = req.get("symbols")
    if not symbols:
        symbols = [s.strip() for s in DEFAULT_SYMBOLS.split(",") if s.strip()]
    notional = float(req.get("notional") or DEFAULT_NOTIONAL)

    base_ctx = {"timeframe": tf, "symbols": symbols, "notional": notional}
    ctx_map: Dict[str, Any] = {}

    provided = req.get("contexts") or req.get("context") or req.get("ctx") or {}
    if isinstance(provided, dict):
        ctx_map.update({k: (v or {}) for k, v in provided.items()})
    elif isinstance(provided, list) and provided:
        first = provided[0] if isinstance(provided[0], dict) else {}
        ctx_map["list"] = [(c if isinstance(c, dict) else {}) for c in provided]
        ctx_map["one"] = {**base_ctx, **first}
    elif isinstance(provided, list) and not provided:
        pass
    elif isinstance(provided, (str, int, float)):
        ctx_map["one"] = {**base_ctx, "tag": str(provided)}

    if "one" not in ctx_map:
        if "default" in ctx_map and isinstance(ctx_map["default"], dict):
            ctx_map["one"] = {**base_ctx, **ctx_map["default"]}
        else:
            ctx_map["one"] = dict(base_ctx)

    if "default" not in ctx_map:
        ctx_map["default"] = dict(ctx_map["one"])

    for k in ("one", "default"):
        ctx_k = ctx_map.get(k) or {}
        ctx_k.setdefault("timeframe", tf)
        ctx_k.setdefault("symbols", symbols)
        ctx_k.setdefault("notional", notional)
        ctx_map[k] = ctx_k

    return ctx_map

# -----------------------------------------------------------------------------
# Scan bridge (unchanged shape)
# -----------------------------------------------------------------------------
async def _scan_bridge(strat: str, req: Dict[str, Any], dry: bool = False):
    """
    Normalizes inputs and calls StrategyBook.scan in a way that keeps
    older strategies happy. Guarantees both:
      - contexts['one'] and contexts['default'] exist
      - req['one'], req['default'], and req['contexts'] exist
    """
    req = req or {}
    tf = req.get("timeframe") or req.get("tf") or DEFAULT_TIMEFRAME
    lim = int(req.get("limit") or DEFAULT_LIMIT)
    notional = req.get("notional") or DEFAULT_NOTIONAL

    # Normalize symbols from req or env
    symbols_field = req.get("symbols") or ([req.get("symbol")] if req.get("symbol") else None)
    if symbols_field is None:
        env_syms = [s.strip() for s in DEFAULT_SYMBOLS.split(",") if s.strip()]
        symbols = env_syms
    elif isinstance(symbols_field, str):
        symbols = [symbols_field]
    else:
        symbols = list(symbols_field)

    # Build safe contexts
    ctx_map = _prepare_contexts(req)

    # ----------------- YOUR PREVIOUS PATCH (kept verbatim) -----------------
    # Minimal, safe req payload your strategies can rely on
    compact_req = {
        # canonical fields
        "strategy": strat,
        "timeframe": tf,
        "limit": lim,
        "notional": notional,
        "symbols": symbols,
        "dry": dry,

        # <-- NEW: mirror contexts into req so strategies that expect req['one']
        # or req['default'] keep working. Also include req['contexts'].
        "one": ctx_map.get("one"),
        "default": ctx_map.get("default"),
        "contexts": ctx_map,

        # passthrough for anything else the caller provided
        "raw": req,
    }
    # ----------------------------------------------------------------------

    inst = StrategyBook()

    result: Any = inst.scan(compact_req, ctx_map)

    # Normalize output
    if not result:
        return []
    if isinstance(result, dict):
        if "orders" in result and isinstance(result["orders"], list):
            return result["orders"]
        return [result]
    if isinstance(result, list):
        return result
    return [result]

# -----------------------------------------------------------------------------
# Background scheduler
# -----------------------------------------------------------------------------
_scheduler_task: Optional[asyncio.Task] = None
_scheduler_running = False

async def _scheduler_loop():
    global _scheduler_running
    _scheduler_running = True
    try:
        while _scheduler_running:
            dry_flag = (not TRADING_ENABLED)
            log.info(f"Scheduler tick: running all strategies (dry={int(dry_flag)})")
            for strat in STRATEGIES:
                req = {
                    "timeframe": DEFAULT_TIMEFRAME,
                    "symbols": [s.strip() for s in DEFAULT_SYMBOLS.split(",") if s.strip()],
                    "limit": DEFAULT_LIMIT,
                    "notional": DEFAULT_NOTIONAL,
                    # callers can push their own contexts here later
                }
                orders: List[Dict[str, Any]] = []
                try:
                    orders = await _scan_bridge(strat, req, dry=dry_flag)
                except Exception as e:
                    log.exception("scan error %s: %s", strat, e)
                    orders = []

                # Push to in-memory ring (your existing code continues here)
                try:
                    _push_orders(orders)
                except Exception:  # pragma: no cover
                    pass

            await asyncio.sleep(SCHEDULE_SECONDS)
    finally:
        _scheduler_running = False

@app.on_event("startup")
async def _startup():
    global _scheduler_task
    log.info("App startup; scheduler interval is %ss", SCHEDULE_SECONDS)
    if _scheduler_task is None:
        _scheduler_task = asyncio.create_task(_scheduler_loop())

@app.on_event("shutdown")
async def _shutdown():
    global _scheduler_running, _scheduler_task
    log.info("Shutting down app; scheduler will stop.")
    _scheduler_running = False
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
    /* ... (all your existing styles remain unchanged) ... */
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
      const r = await fetch('/orders/recent?limit=100');
      const d = await r.json();
      const tbody = document.getElementById('orders_body');
      tbody.innerHTML = '';
      if (Array.isArray(d.orders)){
        for (const o of d.orders){
          const tr = document.createElement('tr');
          const coid = o.client_order_id || o.clientOrderId || '';
          tr.innerHTML = `
            <td>${o.symbol || ''}</td>
            <td>${o.side || ''}</td>
            <td>${o.notional ?? ''}</td>
            <td>${coid}</td>
            <td>${o.status || ''}</td>
          `;
          tbody.appendChild(tr);
        }
      }
      document.getElementById('orders_updated').textContent = d.updated_at ? new Date(d.updated_at).toLocaleString() : '-';
    }
    async function refreshAll(){
      await Promise.all([loadSummary(), loadAttribution(), loadOrders()]);
    }
    document.addEventListener('DOMContentLoaded', refreshAll);
    setInterval(refreshAll, 5000);
  </script>
</head>
<body>
  <header>
      <span class="badge">v{APP_VERSION}</span>
      <span class="badge">Tick: <code>{SCHEDULE_SECONDS}s</code></span>
      <span class="badge">Default TF: <code>{DEFAULT_TIMEFRAME}</code></span>
  </header>

  <main>
    <!-- (your full existing dashboard sections remain unchanged) -->
    <section class="card">
      <div class="row" style="margin-bottom:8px">
        <h2 style="margin:0;font-size:16px">Key Metrics</h2>
      </div>
      <div class="grid">
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
      <div class="muted" id="attr_updated" style="margin-top:8px">-</div>
    </section>

    <section class="card span-8">
      <div class="row" style="margin-bottom:8px">
        <h2 style="margin:0;font-size:16px">Recent Orders</h2>
      </div>
      <table>
        <thead><tr><th>Symbol</th><th>Side</th><th>Notional</th><th>Client ID</th><th>Status</th></tr></thead>
        <tbody id="orders_body"></tbody>
      </table>
      <div class="muted" id="orders_updated" style="margin-top:8px">-</div>
    </section>
  </main>

  <footer style="padding:16px 24px;color:#92a0b3">
    Built with FastAPI • Tick interval: {SCHEDULE_SECONDS}s
  </footer>

</body>
</html>
""".replace("{SCHEDULE_SECONDS}", str(SCHEDULE_SECONDS)).replace("{DEFAULT_TIMEFRAME}", DEFAULT_TIMEFRAME).replace("{APP_VERSION}", APP_VERSION)

# -----------------------------------------------------------------------------
# Routes (unchanged)
# -----------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/dashboard", status_code=307)

@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard():
    return HTMLResponse(_DASHBOARD_HTML)

# (Your existing PnL state, ring buffer, and endpoints remain)
_orders_ring: List[Dict[str, Any]] = []
_attribution: Dict[str, Any] = {"by_strategy": {}, "updated_at": None}
_summary: Dict[str, Any] = {"equity": 0.0, "pnl_day": 0.0, "pnl_week": 0.0, "pnl_month": 0.0, "updated_at": None}

def _push_orders(orders: List[Dict[str, Any]]):
    global _orders_ring, _attribution, _summary
    if not orders:
        return
    for o in orders:
        _orders_ring.append(o)
        if len(_orders_ring) > 1000:
            _orders_ring = _orders_ring[-1000:]
        strat = (o.get("strategy") or "").lower() or (o.get("client_order_id") or "").split("-")[0]
        _attribution["by_strategy"][strat] = _attribution["by_strategy"].get(strat, 0.0) + float(o.get("pnl", 0) or 0.0)
    ts = datetime.now(timezone.utc).isoformat()
    _attribution["updated_at"] = ts
    _summary["updated_at"] = ts

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
    return JSONResponse({"ok": True, "ts": time.time()})

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn  # type: ignore
    port = int(os.getenv("PORT", "10000"))
    log.info("Launching Uvicorn on 0.0.0.0:%d", port)
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
