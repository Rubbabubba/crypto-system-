#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from statistics import mean
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
DEFAULT_WINDOW_HOURS = float(os.getenv("DEFAULT_WINDOW_HOURS", "4"))

# Strategies to iterate on each tick (match your logs)
STRATEGIES = ["c1", "c2", "c3", "c4", "c5", "c6"]

# -----------------------------------------------------------------------------
# StrategyBook import + compatibility wrapper
# -----------------------------------------------------------------------------
_real_SB = None
try:
    # Adjust this to your project layout if needed
    from strategy_book import StrategyBook as _ImportedStrategyBook  # type: ignore
    _real_SB = _ImportedStrategyBook
except Exception as e:
    log.warning("Could not import StrategyBook from strategy_book: %s", e)

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
    Wraps the real StrategyBook (if present) and calls its scan in a safe way.
    It:
      * Guarantees req['one'], req['default'], req['contexts']
      * Guarantees contexts['one'], contexts['default']
      * Tries a couple of call shapes to satisfy older signatures
      * Quiets repeated warnings per strategy
    """

    def __init__(self):
        self._impl = _real_SB() if _real_SB else _FallbackStrategyBook()
        self._warned: Dict[str, bool] = {}

    def _once_warn(self, strategy: str, msg: str):
        if not self._warned.get(strategy):
            log.warning(msg)
            self._warned[strategy] = True

    def scan(self, req: Dict[str, Any], contexts: Dict[str, Any]):
        strategy = req.get("strategy", "unknown")
        tf = req.get("timeframe") or DEFAULT_TIMEFRAME
        symbols = req.get("symbols") or []
        notional = req.get("notional") or DEFAULT_NOTIONAL

        # Always harden shapes before first attempt
        contexts = _ensure_ctx_shape(contexts, tf, symbols, notional)

        # Ensure req mirrors contexts (so req['one'] exists even if strategy reads from req)
        req.setdefault("one", contexts.get("one"))
        req.setdefault("default", contexts.get("default"))
        req.setdefault("contexts", contexts)

        # Try the canonical shape first
        try:
            return self._impl.scan(req, contexts)
        except KeyError as ke:
            # Common issue: KeyError('one')
            if str(ke) in ("'one'", "one"):
                self._once_warn(strategy, f"scan: strategy '{strategy}' accessed missing key 'one'; repairing payload and retrying once.")
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

# -----------------------------------------------------------------------------
# In-memory “DB” for demo endpoints
# -----------------------------------------------------------------------------
_orders_ring: List[Dict[str, Any]] = []
_MAX_ORDERS = 1000

_pnl_summary = {
    "equity": 0.0,
    "pnl_day": 0.0,
    "pnl_week": 0.0,
    "pnl_month": 0.0,
    "updated_at": datetime.now(timezone.utc).toisoformat(),
}

_attribution = {
    "by_strategy": {s: 0.0 for s in STRATEGIES},
    "updated_at": datetime.now(timezone.utc).toisoformat(),
}

def _push_orders(new_orders: List[Dict[str, Any]]):
    if not new_orders:
        return
    for o in new_orders:
        o.setdefault("ts", time.time())
        _orders_ring.append(o)
    if len(_orders_ring) > _MAX_ORDERS:
        del _orders_ring[: len(_orders_ring) - _MAX_ORDERS]

def _touch_metrics(orders: List[Dict[str, Any]], strategy: str):
    if not orders:
        return
    pnl_bump = sum(float(o.get("pnl", 0.0)) for o in orders)
    _pnl_summary["pnl_day"] += pnl_bump
    _pnl_summary["equity"] += pnl_bump
    _pnl_summary["updated_at"] = datetime.now(timezone.utc).toisoformat()

    _attribution["by_strategy"][strategy] = _attribution["by_strategy"].get(strategy, 0.0) + pnl_bump
    _attribution["updated_at"] = datetime.now(timezone.utc).toisoformat()

# -----------------------------------------------------------------------------
# Contexts
# -----------------------------------------------------------------------------
def _prepare_contexts(req: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a robust contexts map that *always* contains:
      - 'one': a single context dict
      - 'default': same as 'one' unless overridden
    and also preserves any user-provided contexts.
    Supports incoming contexts as dict, list, or None.
    """
    tf = req.get("timeframe") or req.get("tf") or DEFAULT_TIMEFRAME

    symbols_raw = req.get("symbols") or req.get("symbol") or []
    if isinstance(symbols_raw, str):
        if symbols_raw.strip():
            symbols = [s.strip() for s in symbols_raw.split(",")]
        else:
            symbols = []
    elif isinstance(symbols_raw, list):
        symbols = symbols_raw
    else:
        env_syms = [s.strip() for s in DEFAULT_SYMBOLS.split(",") if s.strip()]
        symbols = env_syms

    notional = req.get("notional") or req.get("size") or DEFAULT_NOTIONAL

    base_ctx = {
        "timeframe": tf,
        "symbols": symbols,
        "notional": notional,
    }

    provided = req.get("contexts")
    ctx_map: Dict[str, Any] = {}

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
# Hardened scan bridge (keeps YOUR PATCH verbatim)
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
            log.info("Scheduler tick: running all strategies (dry=0)")
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
                    orders = await _scan_bridge(strat, req, dry=False)
                except Exception as e:
                    # Shouldn’t happen now, but keep it contained
                    log.error("scan bridge failed for '%s': %s", strat, e)
                    orders = []

                if orders:
                    # Stamp strategy name if not present
                    for o in orders:
                        o.setdefault("strategy", strat)
                    _push_orders(orders)
                    _touch_metrics(orders, strat)
            await asyncio.sleep(SCHEDULE_SECONDS)
    finally:
        log.info("Scheduler stopped.")

# -----------------------------------------------------------------------------
# Startup / Shutdown
# -----------------------------------------------------------------------------
@app.on_event("startup")
async def _startup():
    global _scheduler_task
    log.info("Starting app; scheduler will start.")
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
# Helpers for windowed analysis
# -----------------------------------------------------------------------------
def _filter_window_orders(window_seconds: float) -> Tuple[List[Dict[str, Any]], float, float]:
    now = time.time()
    start = now - window_seconds
    items = [o for o in _orders_ring if float(o.get("ts", 0)) >= start]
    return items, start, now

def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

# -----------------------------------------------------------------------------
# HTML (full inline page) — restored and extended
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
      --amber:#ffb86b;
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
    main{padding:24px;max-width:1400px;margin:0 auto}
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
    .span-3{grid-column: span 3}
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
    .warn{color:var(--amber)}
    table{width:100%;border-collapse:collapse;font-size:14px}
    th,td{padding:8px;border-bottom:1px solid #1a2740;text-align:left}
    th{color:#9db0c9;font-weight:600}
    .chips{display:flex;flex-wrap:wrap;gap:8px}
    .chip{
      background:var(--chip);
      border:1px solid var(--chip-br);
      padding:6px 10px;border-radius:999px;font-size:12px;color:#c7d2e3;
    }
    a.btn, button.btn{
      display:inline-block;padding:8px 12px;border-radius:10px;text-decoration:none;
      background:var(--accent2);color:#0b1220;font-weight:700;border:1px solid #2a3f6b;
      cursor:pointer;
    }
    select, input[type=number]{
      background:#0d1628;border:1px solid #1a2740;color:var(--ink);
      border-radius:8px;padding:6px 8px;min-width:80px;
    }
    footer{padding:28px;color:var(--muted);text-align:center}
    @media (max-width: 1100px){
      .span-3,.span-4,.span-6,.span-8{grid-column: span 12}
    }
    code{
      background:#0d1628;border:1px solid #1a2740;padding:2px 6px;border-radius:6px
    }
    .pill{
      padding:3px 8px;border-radius:999px;font-size:12px;border:1px solid #1a2740;
      display:inline-block
    }
    .pill.win{background:rgba(93,212,163,.1);border-color:#275b48;color:var(--accent)}
    .pill.loss{background:rgba(255,107,107,.08);border-color:#5b2736;color:var(--red)}
    .pill.draw{background:rgba(255,184,107,.08);border-color:#5b4a27;color:var(--amber)}
  </style>
  <script>
    let windowHours = {DEFAULT_WINDOW_HOURS};

    function money(x){ return Number(x||0).toFixed(2) }
    function pct(x){ return (Number(x||0)*100).toFixed(1)+'%' }

    async function loadSummary(){
      const r = await fetch('/pnl/summary');
      const d = await r.json();
      document.getElementById('eq').textContent = money(d.equity);
      document.getElementById('pnl_day').textContent = money(d.pnl_day);
      document.getElementById('pnl_week').textContent = money(d.pnl_week);
      document.getElementById('pnl_month').textContent = money(d.pnl_month);
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
          tr.innerHTML = `<td>${k}</td><td>${money(v)}</td>`;
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
          <td>${money(o.pnl)}</td>
          <td>${o.ts ? new Date(o.ts*1000).toLocaleString() : ''}</td>
        `;
        tbody.appendChild(tr);
      });
    }

    async function loadScoreboard(){
      const r = await fetch('/orders/scoreboard?window_hours='+windowHours);
      const d = await r.json();
      document.getElementById('win_window_from').textContent = new Date(d.from_ts*1000).toLocaleString();
      document.getElementById('win_window_to').textContent = new Date(d.to_ts*1000).toLocaleString();

      const tbody = document.getElementById('score_body');
      tbody.innerHTML = '';
      (d.strategies || []).forEach(s => {
        const cls = s.gross_pnl >= 0 ? 'good' : 'bad';
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${s.strategy}</td>
          <td>${s.trades}</td>
          <td>${s.wins}</td>
          <td>${s.losses}</td>
          <td>${pct(s.win_rate)}</td>
          <td class="${cls}">${money(s.gross_pnl)}</td>
          <td>${money(s.avg_pnl)}</td>
          <td>${s.last_trade_ts ? new Date(s.last_trade_ts*1000).toLocaleTimeString() : '-'}</td>
        `;
        tbody.appendChild(tr);
      });
    }

    async function loadWindowTrades(){
      const r = await fetch('/orders/window?window_hours='+windowHours);
      const d = await r.json();
      const tbody = document.getElementById('win_trades_body');
      tbody.innerHTML = '';
      (d.orders || []).forEach(o => {
        const pnl = Number(o.pnl || 0);
        const tag = pnl > 0 ? 'win' : (pnl < 0 ? 'loss' : 'draw');
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${o.ts ? new Date(o.ts*1000).toLocaleTimeString() : ''}</td>
          <td>${o.id ?? ''}</td>
          <td>${o.symbol ?? ''}</td>
          <td>${o.side ?? ''}</td>
          <td>${o.qty ?? ''}</td>
          <td>${o.px ?? ''}</td>
          <td>${o.strategy ?? ''}</td>
          <td>${money(o.pnl)}</td>
          <td><span class="pill ${tag}">${tag.toUpperCase()}</span></td>
        `;
        tbody.appendChild(tr);
      });
      document.getElementById('win_trades_count').textContent = d.count ?? 0;
    }

    async function loadInsights(){
      const r = await fetch('/orders/insights?window_hours='+windowHours);
      const d = await r.json();
      const ul = document.getElementById('insights_list');
      ul.innerHTML = '';
      (d.insights || []).forEach(x => {
        const li = document.createElement('li');
        li.innerHTML = `<strong>${x.strategy}:</strong> ${x.message}`;
        ul.appendChild(li);
      });
      document.getElementById('insights_updated').textContent = new Date().toLocaleTimeString();
    }

    async function refreshAll(){
      await Promise.all([loadSummary(), loadAttribution(), loadOrders(), loadScoreboard(), loadWindowTrades(), loadInsights()]);
    }

    function onWindowChange(sel){
      const v = Number(sel.value);
      if (!isNaN(v) && v > 0){ windowHours = v; refreshAll(); }
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
      <span class="chip">Window:
        <select onchange="onWindowChange(this)">
          <option value="1">1h</option>
          <option value="2">2h</option>
          <option value="4" selected>4h</option>
          <option value="8">8h</option>
          <option value="24">24h</option>
        </select>
      </span>
    </div>
  </header>

  <main>
    <div class="grid">
      <!-- Summary -->
      <section class="card span-8">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">P&L Summary</h2>
          <button class="btn" onclick="refreshAll()">Refresh</button>
        </div>
        <div class="grid" style="grid-template-columns:repeat(12,1fr);gap:12px">
          <div class="span-3">
            <div class="muted">Equity</div>
            <div class="kpi" id="eq">0.00</div>
          </div>
          <div class="span-3">
            <div class="muted">PnL (Day)</div>
            <div class="kpi good" id="pnl_day">0.00</div>
          </div>
          <div class="span-3">
            <div class="muted">PnL (Week)</div>
            <div class="kpi" id="pnl_week">0.00</div>
          </div>
          <div class="span-3">
            <div class="muted">PnL (Month)</div>
            <div class="kpi" id="pnl_month">0.00</div>
          </div>
          <div class="span-12" style="margin-top:6px">
            <div class="muted">Last Updated</div>
            <div id="updated" class="kpi" style="font-size:16px">-</div>
          </div>
        </div>
      </section>

      <!-- Attribution -->
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

      <!-- Recent Orders -->
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

      <!-- Strategy Performance (Window) -->
      <section class="card span-8">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">Strategy Performance (window)</h2>
          <div class="muted">From <span id="win_window_from">-</span> to <span id="win_window_to">-</span></div>
        </div>
        <table>
          <thead>
            <tr>
              <th>Strategy</th><th>Trades</th><th>Wins</th><th>Losses</th>
              <th>Win%</th><th>Gross PnL</th><th>Avg PnL</th><th>Last Trade</th>
            </tr>
          </thead>
          <tbody id="score_body"></tbody>
        </table>
      </section>

      <!-- Window Trades -->
      <section class="card span-4">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">Trades in Window</h2>
          <div class="muted"><span id="win_trades_count">0</span> trades</div>
        </div>
        <div style="max-height:300px;overflow:auto;border:1px solid #1a2740;border-radius:8px">
          <table>
            <thead>
              <tr>
                <th>Time</th><th>ID</th><th>Sym</th><th>Side</th><th>Qty</th><th>Px</th><th>Strat</th><th>PnL</th><th>Result</th>
              </tr>
            </thead>
            <tbody id="win_trades_body"></tbody>
          </table>
        </div>
      </section>

      <!-- Insights -->
      <section class="card span-12">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">Insights & Triage</h2>
          <div class="muted">Updated: <span id="insights_updated">-</span></div>
        </div>
        <ul id="insights_list" style="margin:0 0 0 18px;padding:0;line-height:1.6"></ul>
        <div class="muted" style="margin-top:8px">
          Heuristics: flags low win-rate & negative PnL; suggests reducing size, pausing, or reviewing entry/exit rules.
        </div>
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
""".replace("{SCHEDULE_SECONDS}", str(SCHEDULE_SECONDS)).replace("{DEFAULT_TIMEFRAME}", DEFAULT_TIMEFRAME).replace("{DEFAULT_WINDOW_HOURS}", str(DEFAULT_WINDOW_HOURS))

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def root():
    return RedirectResponse(url="/dashboard", status_code=307)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(content=_DASHBOARD_HTML, status_code=200)

@app.get("/pnl/summary", response_class=JSONResponse)
async def pnl_summary():
    return JSONResponse(_pnl_summary)

@app.get("/orders/recent", response_class=JSONResponse)
async def orders_recent(limit: int = Query(DEFAULT_LIMIT, ge=1, le=1000)):
    items = list(reversed(_orders_ring[-limit:]))
    return JSONResponse({"orders": items, "count": len(items)})

@app.get("/orders/attribution", response_class=JSONResponse)
async def orders_attribution():
    return JSONResponse(_attribution)

# ---------- New: Windowed performance endpoints ----------

@app.get("/orders/window", response_class=JSONResponse)
async def orders_window(window_hours: float = Query(DEFAULT_WINDOW_HOURS, ge=0.1, le=48.0)):
    orders, start, now = _filter_window_orders(window_hours * 3600.0)
    # sort newest first
    orders_sorted = sorted(orders, key=lambda o: float(o.get("ts", 0)), reverse=True)
    return JSONResponse({"orders": orders_sorted, "count": len(orders_sorted), "from_ts": start, "to_ts": now})

@app.get("/orders/scoreboard", response_class=JSONResponse)
async def orders_scoreboard(window_hours: float = Query(DEFAULT_WINDOW_HOURS, ge=0.1, le=48.0)):
    orders, start, now = _filter_window_orders(window_hours * 3600.0)
    by: Dict[str, List[Dict[str, Any]]] = {}
    for o in orders:
        strat = (o.get("strategy") or "unknown")
        by.setdefault(strat, []).append(o)

    rows = []
    for strat, items in by.items():
        pnls = [_safe_float(o.get("pnl", 0.0)) for o in items]
        wins = sum(1 for p in pnls if p > 0)
        losses = sum(1 for p in pnls if p < 0)
        trades = len(items)
        gross = sum(pnls)
        avg = (mean(pnls) if trades else 0.0)
        win_rate = (wins / trades) if trades else 0.0
        last_ts = max(float(o.get("ts", 0)) for o in items) if items else 0.0
        rows.append({
            "strategy": strat,
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "gross_pnl": gross,
            "avg_pnl": avg,
            "last_trade_ts": last_ts,
        })

    # include any strategies with no trades to keep the table complete
    for s in STRATEGIES:
        if not any(r["strategy"] == s for r in rows):
            rows.append({
                "strategy": s, "trades": 0, "wins": 0, "losses": 0,
                "win_rate": 0.0, "gross_pnl": 0.0, "avg_pnl": 0.0, "last_trade_ts": 0.0
            })

    # sort by gross pnl desc
    rows.sort(key=lambda r: r["gross_pnl"], reverse=True)
    return JSONResponse({"strategies": rows, "from_ts": start, "to_ts": now})

@app.get("/orders/insights", response_class=JSONResponse)
async def orders_insights(window_hours: float = Query(DEFAULT_WINDOW_HOURS, ge=0.1, le=48.0)):
    sb = (await orders_scoreboard(window_hours)).body
    data = json.loads(sb.decode("utf-8")) if isinstance(sb, (bytes, bytearray)) else sb
    strategies = data.get("strategies", [])
    insights: List[Dict[str, str]] = []

    for r in strategies:
        s = r["strategy"]
        trades = r["trades"]
        wins = r["wins"]
        losses = r["losses"]
        win_rate = r["win_rate"]
        gross = r["gross_pnl"]
        avg = r["avg_pnl"]

        # Basic heuristics
        if trades >= 3 and gross < 0 and win_rate < 0.4:
            insights.append({"strategy": s, "message": "Underperforming: low win-rate & negative PnL. Consider pausing, reducing size, or revisiting signals."})
        elif trades >= 3 and gross > 0 and win_rate >= 0.6:
            insights.append({"strategy": s, "message": "Healthy: positive PnL with decent win-rate. Consider carefully scaling or widening limits."})
        elif trades >= 3 and abs(avg) < 1e-9:
            insights.append({"strategy": s, "message": "Flat average PnL. Review TP/SL or fees/slippage; signals may need sharpening."})
        elif trades == 0:
            insights.append({"strategy": s, "message": "No trades in the window — confirm triggers, market filters, and symbol/timeframe coverage."})

    return JSONResponse({"insights": insights})

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
