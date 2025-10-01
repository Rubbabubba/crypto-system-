# app.py
import os
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple

import httpx
from fastapi import FastAPI, Query, Body, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

app = FastAPI(title="Crypto Trading System")

# -----------------------------
# Configuration / Defaults
# -----------------------------
DEFAULT_UNIVERSE: List[str] = [
    "BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD","ADA/USD",
    "AVAX/USD","LINK/USD","TON/USD","TRX/USD","BCH/USD","LTC/USD",
    "APT/USD","ARB/USD","SUI/USD","OP/USD","MATIC/USD","NEAR/USD","ATOM/USD"
]
STRATS_ALL: List[str] = ["c1","c2","c3","c4","c5","c6"]

# Built-in background scheduler (Option B)
SCHED_ENABLED = os.getenv("SCHED_ENABLED", "0") == "1"
SCHED_INTERVAL_SEC = int(os.getenv("SCHED_INTERVAL_SEC", "300"))

# Alpaca
ALPACA_KEY = os.getenv("ALPACA_API_KEY_ID", "")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET_KEY", "")
ALPACA_PAPER = os.getenv("ALPACA_PAPER", "1") == "1"
ALPACA_BASE = "https://paper-api.alpaca.markets" if ALPACA_PAPER else "https://api.alpaca.markets"
ALPACA_DATA = "https://data.alpaca.markets"
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "1") == "1"  # master toggle

# App Meta
APP_VERSION = os.getenv("APP_VERSION", "2025.10.01")
STARTED_AT = datetime.now(timezone.utc).isoformat()

# In-memory order log (so /orders/recent keeps working even if you don’t have a DB)
ORDER_LOG: List[Dict[str, Any]] = []  # appended when we "place" orders from scans
ORDER_LOG_MAX = 2000

# -----------------------------
# Strategies Adapter
# -----------------------------
"""
We try to call into your existing strategies package WITHOUT enforcing a specific API.
This adapter will check (in order):

1) strategies.run_scan(...)
2) strategies.StrategyBook().scan(...)
3) strategies.<strat>.scan(...)

Your current dashboard logs show /scan is returning 200 with fields like topk & min_score,
so this should be compatible as long as one of the above exists.
"""

_StrategyCache: Dict[str, Any] = {}
_strategy_run_fn = None
_strategy_book = None

def _import_strategies():
    global _strategy_run_fn, _strategy_book
    if _strategy_run_fn or _strategy_book:
        return
    try:
        import strategies  # type: ignore
    except Exception as e:
        logger.warning("strategies import failed: %s", e)
        return

    # 1) Prefer run_scan function if present
    if hasattr(strategies, "run_scan") and callable(getattr(strategies, "run_scan")):
        _strategy_run_fn = getattr(strategies, "run_scan")
        logger.info("Using strategies.run_scan adapter")
        return

    # 2) Try StrategyBook
    try:
        SB = getattr(strategies, "StrategyBook", None)
        if SB:
            _strategy_book = SB()
            logger.info("Using strategies.StrategyBook().scan adapter")
            return
    except Exception as e:
        logger.warning("StrategyBook init failed: %s", e)

def _get_module_for_strat(strat: str):
    """Load strategies.<strat> lazily if needed."""
    if strat in _StrategyCache:
        return _StrategyCache[strat]
    try:
        import importlib
        mod = importlib.import_module(f"strategies.{strat}")
        _StrategyCache[strat] = mod
        return mod
    except Exception as e:
        logger.warning("No module strategies.%s (%s)", strat, e)
        return None

async def run_scan_adapter(
    strat: str,
    timeframe: str,
    limit: int,
    notional: float,
    symbols: List[str],
    dry: bool,
    topk: int = 2,
    min_score: float = 0.10
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Returns (placed_orders, meta_info)
    placed_orders: list of dicts for any orders we actually attempted
    meta_info: arbitrary metadata (scores, candidate signals, etc.)
    """
    _import_strategies()

    # 1) strategies.run_scan
    if _strategy_run_fn:
        try:
            res = await _maybe_await(_strategy_run_fn(
                strat=strat,
                timeframe=timeframe,
                limit=limit,
                notional=notional,
                symbols=symbols,
                dry=dry,
                topk=topk,
                min_score=min_score
            ))
            # Expect a dict or tuple
            if isinstance(res, tuple) and len(res) == 2:
                return res[0], res[1]  # (placed, meta)
            if isinstance(res, dict):
                return res.get("placed", []), res
        except Exception as e:
            logger.exception("strategies.run_scan error: %s", e)

    # 2) StrategyBook().scan
    if _strategy_book and hasattr(_strategy_book, "scan"):
        try:
            res = await _maybe_await(_strategy_book.scan(
                strat=strat,
                timeframe=timeframe,
                limit=limit,
                notional=notional,
                symbols=symbols,
                dry=dry,
                topk=topk,
                min_score=min_score
            ))
            if isinstance(res, tuple) and len(res) == 2:
                return res[0], res[1]
            if isinstance(res, dict):
                return res.get("placed", []), res
        except Exception as e:
            logger.exception("StrategyBook.scan error: %s", e)

    # 3) strategies.<strat>.scan
    mod = _get_module_for_strat(strat)
    if mod and hasattr(mod, "scan") and callable(getattr(mod, "scan")):
        try:
            res = await _maybe_await(mod.scan(
                timeframe=timeframe,
                limit=limit,
                notional=notional,
                symbols=symbols,
                dry=dry,
                topk=topk,
                min_score=min_score
            ))
            if isinstance(res, tuple) and len(res) == 2:
                return res[0], res[1]
            if isinstance(res, dict):
                return res.get("placed", []), res
        except Exception as e:
            logger.exception("%s.scan error: %s", strat, e)

    # Fallback: no strategy available
    logger.warning("No strategy adapter handled %s; returning flat", strat)
    return [], {"reason": "no_strategy_adapter"}

async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x

# -----------------------------
# Alpaca Helpers (safe no-ops)
# -----------------------------
def alpaca_headers() -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
        "Content-Type": "application/json",
    }

async def alpaca_positions() -> List[Dict[str, Any]]:
    if not (ALPACA_KEY and ALPACA_SECRET):
        return []
    url = f"{ALPACA_BASE}/v2/positions"
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.get(url, headers=alpaca_headers())
    if r.status_code == 200:
        return r.json()
    return []

def _append_order_log(row: Dict[str, Any]):
    ORDER_LOG.append(row)
    if len(ORDER_LOG) > ORDER_LOG_MAX:
        del ORDER_LOG[: len(ORDER_LOG) - ORDER_LOG_MAX]

# -----------------------------
# API Models
# -----------------------------
class ScanResponse(BaseModel):
    strat: str
    placed: int
    placed_sample: List[Dict[str, Any]]
    meta: Dict[str, Any]
    dry: bool

# -----------------------------
# Routes
# -----------------------------
@app.get("/", include_in_schema=False)
def root_redirect():
    return RedirectResponse(url="/dashboard", status_code=307)

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

@app.get("/health")
def health():
    return {
        "ok": True,
        "version": APP_VERSION,
        "started_at": STARTED_AT,
        "scheduler": {"enabled": SCHED_ENABLED, "interval_sec": SCHED_INTERVAL_SEC},
        "alpaca_paper": ALPACA_PAPER,
        "trading_enabled": TRADING_ENABLED,
    }

@app.get("/universe")
def universe():
    return {"universe": DEFAULT_UNIVERSE, "count": len(DEFAULT_UNIVERSE)}

@app.get("/v2/positions")
async def positions():
    # Mirror the path your frontend expects
    return JSONResponse(await alpaca_positions())

@app.get("/orders/attribution")
def orders_attr():
    # Keep whatever shape your UI expects; we expose a compact snapshot
    return {
        "source": "app-order-log",
        "count": len(ORDER_LOG),
        "last": ORDER_LOG[-10:],
    }

@app.get("/orders/recent")
def orders_recent(status: str = "all", limit: int = 200):
    # Simple, local recorder. If you already store orders elsewhere, you can replace this.
    lim = max(1, min(limit, 1000))
    return {"orders": ORDER_LOG[-lim:]}

@app.get("/pnl/summary")
def pnl_summary():
    # Placeholder; extend to read from your storage if you track realized/unrealized PnL.
    # We at least return 200 OK so your UI stays happy.
    return {"realized": 0.0, "unrealized": 0.0, "since": STARTED_AT}

@app.post("/scan/{strat}", response_model=ScanResponse)
async def scan_endpoint(
    strat: str,
    timeframe: str = Query("5Min"),
    limit: int = Query(360, ge=50, le=10_000),
    dry: int = Query(1, ge=0, le=1),
    symbols: Optional[str] = Query(None),
    notional: float = Query(25, gt=0),
    topk: int = Query(2, ge=1, le=50),
    min_score: float = Query(0.10, ge=0.0, le=1.0),
):
    if strat not in STRATS_ALL:
        return JSONResponse(
            status_code=400,
            content={"error": f"Unknown strategy '{strat}'", "allowed": STRATS_ALL},
        )
    syms = symbols.split(",") if symbols else list(DEFAULT_UNIVERSE)

    placed, meta = await run_scan_adapter(
        strat=strat,
        timeframe=timeframe,
        limit=limit,
        notional=notional,
        symbols=syms,
        dry=bool(dry),
        topk=topk,
        min_score=min_score,
    )

    # Record orders locally (so /orders/recent shows something)
    utcnow = datetime.now(timezone.utc).isoformat()
    for p in placed or []:
        _append_order_log({
            "ts": utcnow,
            "strat": strat,
            "symbol": p.get("symbol"),
            "side": p.get("side"),
            "qty": p.get("qty"),
            "notional": p.get("notional"),
            "status": p.get("status", "submitted"),
            "dry": bool(dry),
            "meta": {"timeframe": timeframe, "limit": limit},
        })

    sample = (placed or [])[:10]
    return {
        "strat": strat,
        "placed": len(placed or []),
        "placed_sample": sample,
        "meta": meta or {},
        "dry": bool(dry),
    }

# -----------------------------
# (Optional) /cron/runall is handy even with Option B
# -----------------------------
@app.post("/cron/runall")
async def cron_run_all(
    dry: int = Query(0, ge=0, le=1),
    timeframe: str = Query("5Min"),
    limit: int = Query(360, ge=100, le=5000),
    notional: float = Query(25, gt=0),
    symbols: Optional[str] = Query(None),
    topk: int = Query(2, ge=1, le=10),
    min_score: float = Query(0.10, ge=0.0, le=1.0)
):
    syms = symbols.split(",") if symbols else list(DEFAULT_UNIVERSE)
    results = []
    for s in STRATS_ALL:
        try:
            placed, meta = await run_scan_adapter(
                strat=s,
                timeframe=timeframe,
                limit=limit,
                notional=notional,
                symbols=syms,
                dry=bool(dry),
                topk=topk,
                min_score=min_score
            )
            results.append({"strat": s, "placed": len(placed or []), "meta": meta})
            await asyncio.sleep(1.0)
        except Exception as e:
            logger.exception("runall error for %s", s)
            results.append({"strat": s, "error": str(e)})
            await asyncio.sleep(0.5)
    return {"ok": True, "results": results}

# -----------------------------
# Background Scheduler (Option B)
# -----------------------------
_bg_task: asyncio.Task | None = None
async def scheduler_loop():
    interval = max(30, SCHED_INTERVAL_SEC)  # sanity floor
    while True:
        try:
            logger.info("Scheduler tick: running all strategies (dry=0)")
            await cron_run_all(
                dry=0,
                timeframe="5Min",
                limit=360,
                notional=25,
                symbols=None,
                topk=2,
                min_score=0.10
            )
        except Exception:
            logger.exception("scheduler_loop error")
        await asyncio.sleep(interval)

@app.on_event("startup")
async def _startup():
    # Small delay to let server finish booting
    await asyncio.sleep(1.0)
    if SCHED_ENABLED:
        global _bg_task
        _bg_task = asyncio.create_task(scheduler_loop())
        logger.info("Background scheduler started (interval=%ss)", SCHED_INTERVAL_SEC)
    else:
        logger.info("Background scheduler disabled (SCHED_ENABLED != 1)")

@app.on_event("shutdown")
async def _shutdown():
    global _bg_task
    if _bg_task:
        _bg_task.cancel()
        _bg_task = None

# -----------------------------
# Inline HTML (dashboard)
# -----------------------------
DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Crypto Trading System</title>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <style>
    :root { color-scheme: light dark; }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
    h1 { margin: 0 0 12px; font-size: 22px; }
    .row { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin-bottom: 12px; }
    label { font-size: 14px; color: #666; margin-right: 6px; }
    input, select { padding: 6px 8px; font-size: 14px; }
    button { padding: 8px 12px; font-size: 14px; cursor: pointer; }
    .card { border: 1px solid #ddd; padding: 12px; border-radius: 8px; margin-top: 16px; }
    code { background: rgba(0,0,0,0.06); padding: 2px 4px; border-radius: 4px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 8px; border-bottom: 1px solid #eee; text-align: left; }
    .muted { color: #888; }
    .ok { color: #0a7; }
    .bad { color: #d33; }
  </style>
</head>
<body>
  <h1>Crypto Trading System</h1>
  <div class="row">
    <span class="muted">Version: <code id="ver">...</code></span>
    <span class="muted">Scheduler: <code id="sched">...</code></span>
    <span class="muted">Alpaca: <code id="alp">...</code></span>
  </div>

  <div class="card">
    <div class="row">
      <label>Timeframe</label>
      <select id="tf">
        <option>1Min</option>
        <option selected>5Min</option>
        <option>15Min</option>
      </select>

      <label>Limit</label>
      <input id="lim" type="number" value="360" min="50" max="10000" style="width:100px"/>

      <label>Notional</label>
      <input id="notional" type="number" value="25" min="1" step="1" style="width:100px"/>

      <label>Dry</label>
      <select id="dry">
        <option value="1">Yes</option>
        <option value="0" selected>No</option>
      </select>

      <label>TopK</label>
      <input id="topk" type="number" value="2" min="1" max="50" style="width:80px"/>

      <label>Min Score</label>
      <input id="minscore" type="number" value="0.10" min="0" max="1" step="0.01" style="width:100px"/>

      <button onclick="runAll()">Run All</button>
    </div>
    <div class="row">
      <label>Symbols (CSV)</label>
      <input id="symbols" type="text" style="flex:1"
        placeholder="Leave blank to use default universe"/>
    </div>
    <div id="status" class="muted"></div>
    <div id="results"></div>
  </div>

  <div class="card">
    <h3>Recent Orders</h3>
    <div id="orders"></div>
  </div>

  <script>
    async function loadHealth() {
      const r = await fetch('/health'); const j = await r.json();
      document.getElementById('ver').textContent = j.version || 'n/a';
      const sch = j.scheduler?.enabled ? 'enabled @ '+j.scheduler.interval_sec+'s' : 'disabled';
      document.getElementById('sched').textContent = sch;
      const alp = (j.alpaca_paper===true?'paper':'live') + ', trading='+(j.trading_enabled?'on':'off');
      document.getElementById('alp').textContent = alp;
    }

    async function runAll() {
      const tf = document.getElementById('tf').value;
      const lim = document.getElementById('lim').value;
      const notional = document.getElementById('notional').value;
      const dry = document.getElementById('dry').value;
      const topk = document.getElementById('topk').value;
      const minscore = document.getElementById('minscore').value;
      const syms = document.getElementById('symbols').value.trim();

      const params = new URLSearchParams({
        timeframe: tf, limit: lim, dry: dry, notional: notional,
        topk: topk, min_score: minscore
      });
      if (syms) { params.set('symbols', syms); }

      setStatus('Running scans...');
      const out = [];
      for (const s of ["c1","c2","c3","c4","c5","c6"]) {
        const url = `/scan/${encodeURIComponent(s)}?`+params.toString();
        const r = await fetch(url, {method:'POST'});
        const j = await r.json();
        out.push(j);
      }
      renderResults(out);
      setStatus('Done.');
      refreshOrders();
    }

    function setStatus(msg) {
      document.getElementById('status').textContent = msg;
    }

    function renderResults(items) {
      const el = document.getElementById('results');
      if (!Array.isArray(items) || items.length === 0) {
        el.innerHTML = '<div class="muted">No results.</div>'; return;
      }
      let html = '<table><thead><tr><th>Strat</th><th>Placed</th><th>Sample</th><th>Meta keys</th><th>Dry</th></tr></thead><tbody>';
      for (const it of items) {
        const keys = it.meta ? Object.keys(it.meta).slice(0,5).join(', ') : '';
        const sample = (it.placed_sample||[]).slice(0,3).map(p => (p.symbol||'?')+' '+(p.side||'?')+' x'+(p.qty||p.notional||'?')).join(' | ');
        html += `<tr>
          <td><code>${it.strat}</code></td>
          <td class="${it.placed>0?'ok':'muted'}">${it.placed}</td>
          <td>${sample||'<span class="muted">-</span>'}</td>
          <td class="muted">${keys||'-'}</td>
          <td>${it.dry ? 'Yes' : 'No'}</td>
        </tr>`;
      }
      html += '</tbody></table>';
      el.innerHTML = html;
    }

    async function refreshOrders() {
      const r = await fetch('/orders/recent?status=all&limit=200');
      const j = await r.json();
      const el = document.getElementById('orders');
      const rows = j.orders || [];
      if (!rows.length) { el.innerHTML = '<div class="muted">No orders yet.</div>'; return; }
      let html = '<table><thead><tr><th>Time (UTC)</th><th>Strat</th><th>Symbol</th><th>Side</th><th>Qty/Notional</th><th>Status</th><th>Dry</th></tr></thead><tbody>';
      for (const o of rows.slice(-50).reverse()) {
        html += `<tr>
          <td>${o.ts || ''}</td>
          <td><code>${o.strat||''}</code></td>
          <td>${o.symbol||''}</td>
          <td>${o.side||''}</td>
          <td>${o.qty ?? o.notional ?? ''}</td>
          <td>${o.status||''}</td>
          <td>${o.dry ? 'Yes' : 'No'}</td>
        </tr>`;
      }
      html += '</tbody></table>';
      el.innerHTML = html;
    }

    loadHealth();
    refreshOrders();
    setInterval(refreshOrders, 10000);
  </script>
</body>
</html>
"""

# -----------------------------
# Local dev entrypoint
# -----------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
