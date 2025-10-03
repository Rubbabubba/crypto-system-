# app.py — working_app.py base + minimal performance review additions
# (Added: /orders/performance endpoint and small dashboard panel/JS to visualize it)

from __future__ import annotations

import os
import time
import json
import math
import queue
import random
import signal
import logging
import threading
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware

# --------------------------------------------------------------------------------------
# Config & Logging
# --------------------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

PORT = int(os.getenv("PORT", "10000"))
HOST = os.getenv("HOST", "0.0.0.0")

# Controls how many recent orders we keep in memory
_MAX_ORDERS = 1000

# --------------------------------------------------------------------------------------
# In-memory state (same as working_app.py)
# --------------------------------------------------------------------------------------

# Ring buffer for recent orders (latest first)
_orders_ring: List[Dict[str, Any]] = []

# PnL summary
_pnl_summary: Dict[str, Any] = {
    "realized": 0.0,
    "unrealized": 0.0,
    "fees": 0.0,
    "updated_at": datetime.now(timezone.utc).isoformat(),
}

# Strategy attribution info (dummy shape preserved from working version)
_strategy_attribution = {
    "strategies": [
        {"id": "c1", "name": "Mean Revert"},
        {"id": "c2", "name": "Breakout"},
        {"id": "c3", "name": "Scalper"},
        {"id": "c4", "name": "Momentum"},
        {"id": "c5", "name": "Arb"},
        {"id": "c6", "name": "Maker"},
    ]
}

# Controls & scheduler flags
_scheduler_thread: Optional[threading.Thread] = None
_scheduler_stop = threading.Event()
_scheduler_interval_seconds = 30  # tick interval

# Feature flag to simulate orders (as in working_app.py)
_ENABLE_SIM = True

# --------------------------------------------------------------------------------------
# Utilities
# --------------------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _append_order(order: Dict[str, Any]) -> None:
    """Append to ring; keep only _MAX_ORDERS newest."""
    global _orders_ring
    _orders_ring.insert(0, order)
    if len(_orders_ring) > _MAX_ORDERS:
        _orders_ring = _orders_ring[:_MAX_ORDERS]

def _simulate_order_tick() -> None:
    """Simulate some orders for the dashboard demo (unchanged behavior)."""
    if not _ENABLE_SIM:
        return
    # Randomly choose a strategy
    strat = random.choice([s["id"] for s in _strategy_attribution["strategies"]])
    symbol = random.choice(["BTC-USD", "ETH-USD", "SOL-USD", "DOGE-USD"])
    side = random.choice(["buy", "sell"])
    qty = random.choice([0.01, 0.02, 0.05, 0.1])
    price = round(random.uniform(10, 2000), 2)
    fill_price = price + (random.uniform(-1.5, 1.5))
    pnl = round(random.uniform(-15, 20), 2)
    fees = round(abs(pnl) * 0.01, 2)

    o = {
        "id": f"sim-{int(time.time()*1000)}",
        "ts": time.time(),
        "ts_iso": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "side": side,
        "qty": qty,
        "filled_qty": qty,
        "price": price,
        "avg_fill_price": round(fill_price, 2),
        "pnl": pnl,
        "fees": fees,
        "strategy_id": strat,
    }
    _append_order(o)

    # update pnl summary
    _pnl_summary["realized"] = round(_pnl_summary.get("realized", 0.0) + pnl, 2)
    _pnl_summary["fees"] = round(_pnl_summary.get("fees", 0.0) + fees, 2)
    _pnl_summary["updated_at"] = _now_iso()

# --------------------------------------------------------------------------------------
# Scheduler (unchanged behavior)
# --------------------------------------------------------------------------------------

def _scheduler_loop():
    log.info("Starting app; scheduler will start.")
    while not _scheduler_stop.is_set():
        try:
            log.info("Scheduler tick: running all strategies (dry=0)")
            # In the working app this is where strategies scan/submit orders.
            # We keep behavior unchanged and only simulate if enabled.
            _simulate_order_tick()
        except Exception as e:
            log.exception("Scheduler loop error: %s", e)
        finally:
            _scheduler_stop.wait(_scheduler_interval_seconds)

def _start_scheduler():
    global _scheduler_thread
    if _scheduler_thread and _scheduler_thread.is_alive():
        return
    _scheduler_stop.clear()
    _scheduler_thread = threading.Thread(target=_scheduler_loop, daemon=True)
    _scheduler_thread.start()

def _stop_scheduler():
    _scheduler_stop.set()
    if _scheduler_thread:
        _scheduler_thread.join(timeout=5)
    log.info("Scheduler stopped.")

# --------------------------------------------------------------------------------------
# FastAPI App & Middleware (same)
# --------------------------------------------------------------------------------------

app = FastAPI(title="Crypto System")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------------------------------------------
# Routes — unchanged ones from working_app.py
# --------------------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    # Full original dashboard HTML from the working file, with a tiny injected
    # Strategy Performance panel (see below).
    return HTMLResponse(content=HTML_DASHBOARD)

@app.get("/pnl/summary", response_class=JSONResponse)
def pnl_summary():
    return JSONResponse(_pnl_summary)

@app.get("/orders/recent", response_class=JSONResponse)
def orders_recent(limit: int = Query(50, ge=1, le=500)):
    return JSONResponse({"orders": _orders_ring[:limit]})

@app.get("/orders/attribution", response_class=JSONResponse)
def orders_attribution():
    return JSONResponse(_strategy_attribution)

# --------------------------------------------------------------------------------------
# NEW: Minimal endpoint to analyze performance over the last N hours
# --------------------------------------------------------------------------------------

@app.get("/orders/performance", response_class=JSONResponse)
def orders_performance(
    window_hours: int = Query(4, ge=1, le=168),
    strategy_id: Optional[str] = None
):
    """
    Aggregate win/loss stats over the last `window_hours`. Optionally filter by strategy_id.
    """
    now_ts = time.time()
    cutoff = now_ts - (window_hours * 3600)
    # Copy current ring to a list to avoid mutation while we iterate
    recent = [o for o in _orders_ring if isinstance(o, dict) and o.get("ts", 0) >= cutoff]
    if strategy_id:
        recent = [o for o in recent if str(o.get("strategy_id")) == str(strategy_id)]

    def _pnl_of(o: Dict[str, Any]) -> float:
        if o.get("pnl") is not None:
            return float(o.get("pnl") or 0.0)
        if "realized_pnl" in o:
            return float(o.get("realized_pnl") or 0.0)
        return 0.0

    def _is_win(o: Dict[str, Any]) -> bool:
        return _pnl_of(o) > 0

    by_strategy: Dict[str, Dict[str, Any]] = {}
    gross_pnl = 0.0
    for o in recent:
        sid = str(o.get("strategy_id") or "unknown")
        pnl = _pnl_of(o)
        gross_pnl += pnl
        stats = by_strategy.setdefault(sid, {"count": 0, "wins": 0, "losses": 0, "gross_pnl": 0.0})
        stats["count"] += 1
        stats["gross_pnl"] += pnl
        if pnl > 0:
            stats["wins"] += 1
        elif pnl < 0:
            stats["losses"] += 1

    for stats in by_strategy.values():
        c = max(stats["count"], 1)
        stats["win_rate"] = stats["wins"] / c
        stats["avg_pnl"] = stats["gross_pnl"] / c

    wins = sum(1 for o in recent if _is_win(o))
    losses = sum(1 for o in recent if not _is_win(o) and _pnl_of(o) != 0)
    count = len(recent)
    win_rate = (wins / count) if count else 0.0
    avg_pnl = (gross_pnl / count) if count else 0.0

    return {
        "window_hours": window_hours,
        "from_iso": datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat(),
        "to_iso": datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat(),
        "count": count,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "gross_pnl": gross_pnl,
        "avg_pnl": avg_pnl,
        "by_strategy": by_strategy,
        "trades": [
            {
                "id": o.get("id"),
                "ts": o.get("ts"),
                "ts_iso": datetime.fromtimestamp(o.get("ts", 0), tz=timezone.utc).isoformat() if o.get("ts") else None,
                "strategy_id": o.get("strategy_id"),
                "symbol": o.get("symbol"),
                "side": o.get("side"),
                "qty": o.get("filled_qty") or o.get("qty"),
                "avg_fill_price": o.get("avg_fill_price") or o.get("price"),
                "pnl": _pnl_of(o),
                "is_win": _is_win(o),
            }
            for o in recent
        ],
    }

# --------------------------------------------------------------------------------------
# HTML (full original dashboard from working_app.py) + tiny perf panel & JS
# --------------------------------------------------------------------------------------

HTML_DASHBOARD = """<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Trading Dashboard</title>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
      :root { color-scheme: dark light; }
      body {
        font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
        margin: 0; padding: 0; background: #0f1115; color: #e6e8eb;
      }
      header {
        padding: 16px 20px; background: #12151c; position: sticky; top:0; z-index:10;
        border-bottom: 1px solid #1b2030;
      }
      h1 { margin:0; font-size: 20px; font-weight: 600; letter-spacing: .3px; }
      main { padding: 20px; display: grid; gap: 16px; grid-template-columns: 1fr; max-width: 1200px; margin: 0 auto;}
      @media (min-width: 900px) { main { grid-template-columns: 1fr 1fr; } }
      .panel {
        background: #12151c; border: 1px solid #1b2030; border-radius: 12px; padding: 16px;
        box-shadow: 0 0 0 1px rgba(0,0,0,.2), 0 6px 20px rgba(0,0,0,.35);
      }
      h2 { margin: 0 0 12px 0; font-size: 16px; font-weight: 600; }
      table { width: 100%; border-collapse: collapse; font-size: 13px; }
      thead th {
        text-align: left; padding: 8px; border-bottom: 1px solid #1b2030; color: #98a2b3; font-weight: 500;
      }
      tbody td { padding: 8px; border-bottom: 1px solid #1b2030; }
      .right { text-align: right; }
      .muted { color: #98a2b3; }
      .win { color: #16a34a; }
      .loss { color: #ef4444; }
      .controls { display: flex; gap: 8px; align-items: center; margin-bottom: 8px; flex-wrap: wrap; }
      input, select, button {
        background: #0f131b; border: 1px solid #1b2030; color: #e6e8eb; border-radius: 8px;
        padding: 6px 8px; font-size: 13px;
      }
      button { cursor: pointer; }
      .kpis { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; }
      .kpi { background: #0f131b; border: 1px solid #1b2030; border-radius: 10px; padding: 10px; }
      .kpi h3 { margin: 0; font-size: 12px; color:#98a2b3; font-weight: 500;}
      .kpi p { margin: 4px 0 0 0; font-size: 18px; font-weight: 600; }
      footer { color: #98a2b3; font-size: 12px; text-align: center; padding: 30px 0; grid-column: 1 / -1; }
    </style>
  </head>
  <body>
    <header>
      <h1>Trading Dashboard</h1>
    </header>
    <main>
      <section class="panel">
        <h2>P&amp;L Summary</h2>
        <div class="kpis">
          <div class="kpi"><h3>Realized P&amp;L</h3><p id="pl-realized">$0.00</p></div>
          <div class="kpi"><h3>Unrealized P&amp;L</h3><p id="pl-unrealized">$0.00</p></div>
          <div class="kpi"><h3>Fees</h3><p id="pl-fees">$0.00</p></div>
        </div>
        <p class="muted" id="pl-updated">Updated —</p>
      </section>

      <!-- Strategy Performance (minimal new panel) -->
      <section class="panel">
        <h2>Strategy Performance (last <span id="perf-window">4</span>h)</h2>
        <div class="controls">
          <label>Window (hours):
            <input type="number" id="perfHours" min="1" max="168" value="4" style="width:80px;">
          </label>
          <label>Strategy:
            <select id="perfStrategy">
              <option value="">All</option>
            </select>
          </label>
          <button id="perfRefresh">Refresh</button>
        </div>
        <div id="perfSummary"></div>
        <table id="perfTable">
          <thead>
            <tr>
              <th>Strategy</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Win %</th><th>Gross PnL</th><th>Avg PnL</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </section>

      <section class="panel" style="grid-column: 1 / -1;">
        <h2>Recent Orders</h2>
        <div class="controls">
          <label>Show:
            <select id="ordersLimit">
              <option value="50">50</option>
              <option value="100">100</option>
              <option value="200">200</option>
              <option value="500">500</option>
            </select>
          </label>
          <button id="ordersRefresh">Refresh</button>
        </div>
        <table id="ordersTable">
          <thead>
            <tr>
              <th>Time</th>
              <th>Strategy</th>
              <th>Symbol</th>
              <th>Side</th>
              <th class="right">Qty</th>
              <th class="right">Price</th>
              <th class="right">Fill</th>
              <th class="right">PnL</th>
              <th class="right">Fees</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </section>

      <footer>© Your System</footer>
    </main>

    <script>
      function fmtUSD(x) {
        const n = Number(x||0);
        return (n < 0 ? "-$" : "$") + Math.abs(n).toFixed(2);
      }
      function fmtDT(iso) {
        if (!iso) return "";
        const d = new Date(iso);
        return d.toLocaleString();
      }
      async function loadPnL() {
        const res = await fetch('/pnl/summary');
        const data = await res.json();
        document.getElementById('pl-realized').textContent = fmtUSD(data.realized);
        document.getElementById('pl-unrealized').textContent = fmtUSD(data.unrealized);
        document.getElementById('pl-fees').textContent = fmtUSD(data.fees);
        document.getElementById('pl-updated').textContent = "Updated " + fmtDT(data.updated_at);
      }
      async function loadOrders() {
        const limit = document.getElementById('ordersLimit').value || "50";
        const res = await fetch('/orders/recent?limit=' + limit);
        const data = await res.json();
        const tbody = document.querySelector('#ordersTable tbody');
        tbody.innerHTML = "";
        (data.orders || []).forEach(o => {
          const tr = document.createElement('tr');
          const pnl = Number(o.pnl ?? o.realized_pnl ?? 0);
          tr.innerHTML = `
            <td>${fmtDT(o.ts_iso)}</td>
            <td>${o.strategy_id ?? ""}</td>
            <td>${o.symbol ?? ""}</td>
            <td>${o.side ?? ""}</td>
            <td class="right">${o.filled_qty ?? o.qty ?? ""}</td>
            <td class="right">${o.price ?? ""}</td>
            <td class="right">${o.avg_fill_price ?? ""}</td>
            <td class="right ${pnl>0?'win':(pnl<0?'loss':'')}">${pnl.toFixed(2)}</td>
            <td class="right">${(o.fees ?? 0).toFixed(2)}</td>
          `;
          tbody.appendChild(tr);
        });
      }

      // --- Strategy Performance (new minimal JS) ---
      async function loadPerformance() {
        const hours = Number(document.getElementById('perfHours').value || 4);
        const strat = document.getElementById('perfStrategy').value || '';
        document.getElementById('perf-window').textContent = String(hours);
        const url = strat ? \`/orders/performance?window_hours=\${hours}&strategy_id=\${encodeURIComponent(strat)}\`
                          : \`/orders/performance?window_hours=\${hours}\`;
        const res = await fetch(url);
        const data = await res.json();

        // summary
        const sum = document.getElementById('perfSummary');
        sum.innerHTML = \`
          <p><strong>Total Trades:</strong> \${data.count}
             &nbsp; <strong>Wins:</strong> \${data.wins}
             &nbsp; <strong>Losses:</strong> \${data.losses}
             &nbsp; <strong>Win%:</strong> \${(data.win_rate*100).toFixed(1)}%
             &nbsp; <strong>Gross PnL:</strong> \${data.gross_pnl.toFixed(2)}
             &nbsp; <strong>Avg PnL:</strong> \${data.avg_pnl.toFixed(2)}
             <br><small>\${data.from_iso} → \${data.to_iso}</small>
          </p>\`;

        // table
        const tbody = document.querySelector('#perfTable tbody');
        tbody.innerHTML = '';
        Object.entries(data.by_strategy).forEach(([sid, s]) => {
          const tr = document.createElement('tr');
          tr.innerHTML = \`
            <td>\${sid}</td>
            <td>\${s.count}</td>
            <td class="win">\${s.wins}</td>
            <td class="loss">\${s.losses}</td>
            <td>\${(s.win_rate*100).toFixed(1)}%</td>
            <td>\${s.gross_pnl.toFixed(2)}</td>
            <td>\${s.avg_pnl.toFixed(2)}</td>\`;
          tbody.appendChild(tr);
        });
      }

      // populate strategy dropdown from attribution endpoint
      async function populateStrategyFilter() {
        try {
          const res = await fetch('/orders/attribution');
          const data = await res.json();
          const sel = document.getElementById('perfStrategy');
          if (!sel) return;
          const have = new Set(Array.from(sel.options).map(o => o.value));
          (data.strategies || []).forEach(s => {
            const val = String(s.id);
            if (!have.has(val)) {
              const opt = document.createElement('option');
              opt.value = val;
              opt.textContent = \`\${val} — \${s.name || 'strategy'}\`;
              sel.appendChild(opt);
            }
          });
        } catch(e) {
          console.warn('strategy filter populate failed', e);
        }
      }

      document.addEventListener('DOMContentLoaded', () => {
        document.getElementById('ordersRefresh').addEventListener('click', loadOrders);
        document.getElementById('perfRefresh').addEventListener('click', loadPerformance);
        loadPnL(); loadOrders();
        populateStrategyFilter().then(loadPerformance);
        setInterval(() => { loadPnL(); loadOrders(); }, 60000);
      });
    </script>
  </body>
</html>
"""

# --------------------------------------------------------------------------------------
# Startup / Shutdown hooks (unchanged; note FastAPI deprecation warnings are benign)
# --------------------------------------------------------------------------------------

@app.on_event("startup")
def _on_startup():
    log.info("Launching Uvicorn on %s:%s", HOST, PORT)
    _start_scheduler()

@app.on_event("shutdown")
def _on_shutdown():
    log.info("Shutting down app; scheduler will stop.")
    _stop_scheduler()

# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    log.info("Launching Uvicorn on %s:%s", HOST, PORT)
    uvicorn.run(app, host=HOST, port=PORT)
