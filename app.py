#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# ----------------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

# ----------------------------------------------------------------------------
# App
# ----------------------------------------------------------------------------

app = FastAPI(title="Crypto System")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------------------------------------------------------
# In-memory stores (same as your working version)
# ----------------------------------------------------------------------------

# recent orders ring-buffer (append-only; newest last)
_orders_ring: List[Dict[str, Any]] = []
_ORDERS_RING_MAX = 5000

# latest attribution map (strategy -> stats)
_attribution: Dict[str, Dict[str, Any]] = {}

# latest pnl summary
_pnl_summary: Dict[str, Any] = {
    "updated_at": datetime.now(timezone.utc).isoformat(),
    "total_pnl": 0.0,
    "realized_pnl": 0.0,
    "unrealized_pnl": 0.0,
}

# ----------------------------------------------------------------------------
# Strategy scaffolding (unchanged)
# ----------------------------------------------------------------------------

class StrategyBase:
    name: str = "base"

    async def scan(self, req: Dict[str, Any], contexts: Dict[str, Any]) -> List[Dict[str, Any]]:
        return []

    async def act(self, signals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return []

# Your concrete strategies would subclass StrategyBase and be registered here.
# Keeping stubs as in working file.

_STRATEGIES: Dict[str, StrategyBase] = {}

def register_strategy(key: str, instance: StrategyBase):
    _STRATEGIES[key] = instance

# ----------------------------------------------------------------------------
# Scheduler (unchanged)
# ----------------------------------------------------------------------------

_SCHEDULER_TASK: Optional[asyncio.Task] = None
_SCHEDULER_STOP = asyncio.Event()

async def scheduler_loop():
    log.info("Starting app; scheduler will start.")
    try:
        while not _SCHEDULER_STOP.is_set():
            log.info("Scheduler tick: running all strategies (dry=0)")
            # Simulate strategies creating some orders every ~30s (no changes)
            await asyncio.sleep(30)
    finally:
        log.info("Scheduler stopped.")

# ----------------------------------------------------------------------------
# Utilities (unchanged)
# ----------------------------------------------------------------------------

def _append_order(o: Dict[str, Any]) -> None:
    """
    Append an order/trade into the ring with safe defaults.
    """
    # ensure keys that dashboard expects
    now_ts = time.time()
    record = {
        "id": o.get("id") or f"o_{int(now_ts*1000)}",
        "symbol": o.get("symbol", "BTCUSDT"),
        "side": o.get("side", "buy"),
        "qty": o.get("qty", 0.0),
        "px": o.get("px") or o.get("filled_avg_price") or 0.0,
        "strategy": o.get("strategy") or o.get("attribution") or "unknown",
        "status": o.get("status", "filled"),
        "pnl": float(o.get("pnl", 0.0) or 0.0),
        "ts": float(o.get("ts") or now_ts),
        "t": datetime.fromtimestamp(float(o.get("ts") or now_ts), tz=timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    _orders_ring.append(record)
    if len(_orders_ring) > _ORDERS_RING_MAX:
        # drop oldest chunk
        del _orders_ring[: len(_orders_ring) - _ORDERS_RING_MAX]

def _rebuild_attribution() -> Dict[str, Any]:
    """
    Recalculate basic attribution by strategy from the current ring.
    """
    out: Dict[str, Any] = {}
    for o in _orders_ring:
        s = (o.get("strategy") or "unknown")
        g = out.setdefault(s, {"strategy": s, "trades": 0, "total_pnl": 0.0})
        g["trades"] += 1
        g["total_pnl"] += float(o.get("pnl", 0.0) or 0.0)
    return out

def _rebuild_pnl_summary() -> Dict[str, Any]:
    """
    Recalculate simple PnL summary (realized only here).
    """
    total = 0.0
    for o in _orders_ring:
        total += float(o.get("pnl", 0.0) or 0.0)
    return {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "total_pnl": total,
        "realized_pnl": total,
        "unrealized_pnl": 0.0,
    }

# ----------------------------------------------------------------------------
# Demo seed (unchanged)
# ----------------------------------------------------------------------------

def _seed_demo_if_empty():
    if _orders_ring:
        return
    now = time.time()
    demo = [
        {"symbol": "BTCUSDT", "side": "buy",  "qty": 0.01, "px": 61000, "strategy": "c1", "status": "filled", "pnl": 15.2,  "ts": now - 60*30},
        {"symbol": "BTCUSDT", "side": "sell", "qty": 0.01, "px": 61250, "strategy": "c2", "status": "filled", "pnl": -4.1,  "ts": now - 60*55},
        {"symbol": "ETHUSDT", "side": "buy",  "qty": 0.2,  "px": 2500,  "strategy": "c1", "status": "filled", "pnl": 6.0,   "ts": now - 60*80},
        {"symbol": "SOLUSDT", "side": "buy",  "qty": 2,    "px": 160,   "strategy": "c3", "status": "filled", "pnl": 0.0,   "ts": now - 60*120},
        {"symbol": "BTCUSDT", "side": "sell", "qty": 0.01, "px": 60800, "strategy": "c2", "status": "filled", "pnl": 9.7,   "ts": now - 60*160},
        {"symbol": "ETHUSDT", "side": "sell", "qty": 0.2,  "px": 2515,  "strategy": "c4", "status": "filled", "pnl": -2.3,  "ts": now - 60*200},
    ]
    for d in demo:
        _append_order(d)
    # refresh derived views
    global _attribution, _pnl_summary
    _attribution = _rebuild_attribution()
    _pnl_summary = _rebuild_pnl_summary()

# ----------------------------------------------------------------------------
# Dashboard (exactly your full HTML from working_app.py)
# ----------------------------------------------------------------------------

_DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Crypto System Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root {
  --bg: #0b0f14;
  --panel: #121822;
  --muted: #8aa0b3;
  --text: #e6eef6;
  --green: #19c37d;
  --red: #ff5c5c;
  --yellow: #f5c745;
  --blue: #54a3ff;
}
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; background: var(--bg); color: var(--text); font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica Neue, Arial, "Apple Color Emoji","Segoe UI Emoji", "Segoe UI Symbol"; }
.container { max-width: 1200px; margin: 0 auto; padding: 16px; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.card { background: var(--panel); border: 1px solid rgba(255,255,255,0.06); border-radius: 12px; padding: 16px; box-shadow: 0 10px 30px rgba(0,0,0,.35); }
.card h2 { margin: 0 0 8px 0; font-size: 18px; font-weight: 600; color: #d7e3ee; }
.small { font-size: 12px; color: var(--muted); }
.kpis { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; margin-top: 8px; }
.kpi { background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.06); border-radius: 10px; padding: 12px; }
.kpi .label { font-size: 11px; color: var(--muted); }
.kpi .value { font-size: 20px; font-weight: 700; margin-top: 2px; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: 8px; border-bottom: 1px solid rgba(255,255,255,0.08); font-size: 13px; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 11px; font-weight: 600; }
.badge.win { background: rgba(25,195,125,0.15); color: var(--green); border: 1px solid rgba(25,195,125,0.35); }
.badge.loss { background: rgba(255,92,92,0.15); color: var(--red); border: 1px solid rgba(255,92,92,0.35); }
.badge.flat { background: rgba(245,199,69,0.15); color: var(--yellow); border: 1px solid rgba(245,199,69,0.35); }
.mono { font-variant-numeric: tabular-nums; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
.row { display: flex; gap: 16px; align-items: center; }
hr.separator { border: none; height: 1px; background: rgba(255,255,255,0.08); margin: 12px 0; }
</style>
</head>
<body>
<div class="container">
  <div class="row" style="justify-content: space-between;">
    <h1 style="margin:0;font-size:22px;">Crypto System Dashboard</h1>
    <span class="small" id="updated">—</span>
  </div>
  <div class="grid" style="margin-top:12px;">
    <div class="card">
      <h2>PNL Summary</h2>
      <div class="kpis">
        <div class="kpi"><div class="label">Total PnL</div><div class="value mono" id="k_total">—</div></div>
        <div class="kpi"><div class="label">Realized</div><div class="value mono" id="k_realized">—</div></div>
        <div class="kpi"><div class="label">Unrealized</div><div class="value mono" id="k_unrealized">—</div></div>
      </div>
    </div>
    <div class="card">
      <h2>Attribution (strategy)</h2>
      <table id="tbl_attr">
        <thead><tr><th>Strategy</th><th class="mono">Trades</th><th class="mono">Total PnL</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <div class="card" style="margin-top:16px;">
    <h2>Last 4h Performance (per strategy) & Trades</h2>
    <div class="small">Endpoint: <span class="mono">/orders/performance_4h</span></div>
    <hr class="separator"/>
    <div class="kpis">
      <div class="kpi"><div class="label">Trades</div><div class="value mono" id="k_trades">—</div></div>
      <div class="kpi"><div class="label">Wins / Losses</div><div class="value mono" id="k_winloss">—</div></div>
      <div class="kpi"><div class="label">Total 4h PnL</div><div class="value mono" id="k_total4h">—</div></div>
    </div>
    <hr class="separator"/>
    <div class="row" style="gap:32px;">
      <div style="flex:1;">
        <div class="small" style="margin-bottom:6px;">By Strategy (sorted by PnL)</div>
        <table id="tbl_perf">
          <thead><tr><th>Strategy</th><th class="mono">Trades</th><th class="mono">Wins</th><th class="mono">Losses</th><th class="mono">Win%</th><th class="mono">Total PnL</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
      <div style="flex:1;">
        <div class="small" style="margin-bottom:6px;">Trades (most recent first)</div>
        <table id="tbl_trades">
          <thead><tr><th>t</th><th>Symbol</th><th>Side</th><th>Qty</th><th class="mono">Px</th><th>Strat</th><th class="mono">PnL</th><th>Result</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card" style="margin-top:16px;">
    <h2>Recent Orders (limit=500)</h2>
    <table id="tbl_orders">
      <thead><tr><th>t</th><th>ID</th><th>Symbol</th><th>Side</th><th>Qty</th><th class="mono">Px</th><th>Strategy</th><th class="mono">PnL</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<script>
const fmtUsd = x => (x>=0?"+":"") + x.toFixed(2);
const el = sel => document.querySelector(sel);

async function refreshAll(){
  const [pnl, attr, orders, perf] = await Promise.all([
    fetch('/pnl/summary').then(r=>r.json()),
    fetch('/orders/attribution').then(r=>r.json()),
    fetch('/orders/recent?limit=500').then(r=>r.json()),
    fetch('/orders/performance_4h').then(r=>r.json())
  ]);
  el('#updated').textContent = "Updated " + new Date().toLocaleTimeString();

  // PnL
  el('#k_total').textContent = fmtUsd(pnl.total_pnl);
  el('#k_realized').textContent = fmtUsd(pnl.realized_pnl);
  el('#k_unrealized').textContent = fmtUsd(pnl.unrealized_pnl);

  // Attribution
  const tbodyA = el('#tbl_attr tbody');
  tbodyA.innerHTML = "";
  Object.values(attr).sort((a,b)=>b.total_pnl-a.total_pnl).forEach(r=>{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${r.strategy}</td><td class="mono">${r.trades}</td><td class="mono">${fmtUsd(r.total_pnl)}</td>`;
    tbodyA.appendChild(tr);
  });

  // Performance 4h KPIs
  el('#k_trades').textContent = perf.summary.trades;
  el('#k_winloss').textContent = perf.summary.wins + " / " + perf.summary.losses;
  el('#k_total4h').textContent = fmtUsd(perf.summary.total_pnl);

  // Performance by strategy
  const tbodyP = el('#tbl_perf tbody');
  tbodyP.innerHTML = "";
  perf.by_strategy.forEach(g=>{
    const wr = ((g.win_rate || 0)*100).toFixed(0) + "%";
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${g.strategy}</td><td class="mono">${g.trades}</td><td class="mono">${g.wins}</td><td class="mono">${g.losses}</td><td class="mono">${wr}</td><td class="mono">${fmtUsd(g.total_pnl)}</td>`;
    tbodyP.appendChild(tr);
  });

  // Recent trades list (from perf.trades so it's last 4h, most recent first)
  const tbodyT = el('#tbl_trades tbody');
  tbodyT.innerHTML = "";
  perf.trades.forEach(t=>{
    const badge = t.result === "win" ? "badge win" : (t.result==="loss" ? "badge loss" : "badge flat");
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${t.t}</td><td>${t.symbol}</td><td>${t.side}</td><td>${t.qty}</td><td class="mono">${t.px}</td><td>${t.strategy}</td><td class="mono">${fmtUsd(t.pnl)}</td><td><span class="${badge}">${t.result}</span></td>`;
    tbodyT.appendChild(tr);
  });

  // Orders table (all recent)
  const tbodyO = el('#tbl_orders tbody');
  tbodyO.innerHTML = "";
  orders.forEach(o=>{
    const tr = document.createElement('tr');
    tr.innerHTML = `<td>${o.t || ""}</td><td>${o.id || ""}</td><td>${o.symbol || ""}</td><td>${o.side || ""}</td><td>${o.qty || ""}</td><td class="mono">${o.px || o.filled_avg_price || ""}</td><td>${o.strategy || o.attribution || ""}</td><td class="mono">${(o.pnl!=null)?fmtUsd(parseFloat(o.pnl)||0):""}</td>`;
    tbodyO.appendChild(tr);
  });
}

refreshAll();
setInterval(refreshAll, 60000);
</script>
</body>
</html>
"""

# ----------------------------------------------------------------------------
# Routes
# ----------------------------------------------------------------------------

@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def root():
    return RedirectResponse(url="/dashboard")

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    _seed_demo_if_empty()
    return HTMLResponse(_DASHBOARD_HTML)

# --- data endpoints matching your working app ---

@app.get("/orders/recent")
async def orders_recent(limit: int = Query(50, ge=1, le=5000)):
    _seed_demo_if_empty()
    if limit >= len(_orders_ring):
        rows = list(_orders_ring)
    else:
        rows = _orders_ring[-limit:]
    return JSONResponse(rows)

@app.get("/orders/attribution")
async def orders_attribution():
    global _attribution
    _attribution = _rebuild_attribution()
    return JSONResponse(_attribution)

@app.get("/pnl/summary")
async def pnl_summary():
    global _pnl_summary
    _pnl_summary = _rebuild_pnl_summary()
    return JSONResponse(_pnl_summary)

# --- NEW: minimal additions for strategy performance (last 4 hours) ---

@app.get("/orders/performance")
async def orders_performance(hours: int = 4):
    """
    Performance of filled trades over the last `hours` hours.
    Returns both per-strategy summary and individual trades (most recent first).
    """
    now_ts = time.time()
    cutoff = now_ts - (hours * 3600)
    trades: List[Dict[str, Any]] = []

    for o in _orders_ring:
        try:
            if o.get("status") != "filled":
                continue
            ts = float(o.get("ts", 0))
            if ts < cutoff:
                continue
            pnl = float(o.get("pnl", 0) or 0)
            trade = {
                "id": o.get("id"),
                "symbol": o.get("symbol"),
                "side": o.get("side"),
                "qty": o.get("qty"),
                "px": o.get("px") or o.get("filled_avg_price"),
                "strategy": o.get("strategy") or o.get("attribution") or "unknown",
                "pnl": pnl,
                "ts": ts,
                "t": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
                "result": "win" if pnl > 0 else ("loss" if pnl < 0 else "flat"),
            }
            trades.append(trade)
        except Exception:
            # be defensive: skip any malformed record
            continue

    trades.sort(key=lambda x: x["ts"], reverse=True)

    by_strategy: Dict[str, Dict[str, Any]] = {}
    total_pnl = 0.0
    wins = losses = flats = 0
    for t in trades:
        s = t["strategy"]
        if s not in by_strategy:
            by_strategy[s] = {
                "strategy": s,
                "trades": 0,
                "wins": 0,
                "losses": 0,
                "flats": 0,
                "total_pnl": 0.0,
                "avg_pnl": 0.0,
                "win_rate": 0.0
            }
        g = by_strategy[s]
        g["trades"] += 1
        if t["result"] == "win":
            g["wins"] += 1
            wins += 1
        elif t["result"] == "loss":
            g["losses"] += 1
            losses += 1
        else:
            g["flats"] += 1
            flats += 1
        g["total_pnl"] += float(t["pnl"])
        total_pnl += float(t["pnl"])

    for g in by_strategy.values():
        g["avg_pnl"] = (g["total_pnl"] / g["trades"]) if g["trades"] else 0.0
        denom = max(1, (g["wins"] + g["losses"]))
        g["win_rate"] = g["wins"] / denom

    payload = {
        "hours": hours,
        "since": datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "now": datetime.fromtimestamp(now_ts, tz=timezone.utc).isoformat().replace("+00:00", "Z"),
        "summary": {"trades": len(trades), "wins": wins, "losses": losses, "flats": flats, "total_pnl": total_pnl},
        "by_strategy": sorted(by_strategy.values(), key=lambda x: x["total_pnl"], reverse=True),
        "trades": trades,
    }
    return JSONResponse(payload)

@app.get("/orders/performance_4h")
async def orders_performance_4h():
    # Back-compat route used by the dashboard
    return await orders_performance(4)

# --- healthz ---

@app.get("/healthz", response_class=JSONResponse, include_in_schema=False)
async def healthz():
    return JSONResponse({"ok": True, "ts": datetime.now(timezone.utc).isoformat()})

# ----------------------------------------------------------------------------
# Lifecycle (unchanged from working file)
# ----------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup():
    global _SCHEDULER_TASK
    _seed_demo_if_empty()
    _SCHEDULER_STOP.clear()
    _SCHEDULER_TASK = asyncio.create_task(scheduler_loop())
    log.info("Application startup complete.")

@app.on_event("shutdown")
async def on_shutdown():
    _SCHEDULER_STOP.set()
    if _SCHEDULER_TASK:
        try:
            await asyncio.wait_for(_SCHEDULER_TASK, timeout=5)
        except Exception:
            pass
    log.info("Shutting down app; scheduler will stop.")

# ----------------------------------------------------------------------------
# Entrypoint (unchanged)
# ----------------------------------------------------------------------------

if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "10000"))
    log.info(f"Launching Uvicorn on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
