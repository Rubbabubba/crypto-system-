# app.py  — Crypto Control Plane (v0.9.8)
# - FastAPI service with HTML dashboard (overall P&L, by-strategy, calendar heatmap, recent orders)
# - Background autoscheduler (optional via env)
# - Scan bridge aligned to StrategyBook.scan(strategy, contexts) signature
# - Simple in-memory order/P&L bookkeeping
#
# Env:
#   PORT (Render injects; defaults to 10000)
#   AUTO_SCAN_ENABLED=1              # turn on background scan loop
#   AUTO_SCAN_EVERY=60               # seconds between full book scans
#   DEFAULT_TF=5Min
#   DEFAULT_LIMIT=360
#   DEFAULT_NOTIONAL=25
#   DEFAULT_SYMBOLS=BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD,ADA/USD,TON/USD,TRX/USD,APT/USD,ARB/USD,SUI/USD,OP/USD,MATIC/USD,NEAR/USD,ATOM/USD

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional
from string import Template

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse
from pydantic import BaseModel

# ---------------------- Logging ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

APP_VERSION = "v0.9.8"

# ---------------------- StrategyBook Loader ----------------------
try:
    import strategies as _strategies_mod
except Exception as e:
    log.error("Failed to import strategies module: %s", e)
    _strategies_mod = None

class StrategyBookProxy:
    def __init__(self) -> None:
        if _strategies_mod is None:
            raise RuntimeError("strategies module not available")
        cls = getattr(_strategies_mod, "StrategyBook", None)
        if cls is None:
            raise RuntimeError("strategies.StrategyBook missing")
        self._impl = cls()
        log.info("Loaded StrategyBook from %s.%s", _strategies_mod.__name__, "StrategyBook")

    def scan(self, *args, **kwargs):
        return self._impl.scan(*args, **kwargs)

_strategy_book = StrategyBookProxy()

# ---------------------- Config / Defaults ----------------------
def _env_flag(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip() not in ("0", "false", "False", "")

AUTO_SCAN_ENABLED = _env_flag("AUTO_SCAN_ENABLED", False)
AUTO_SCAN_EVERY = int(os.getenv("AUTO_SCAN_EVERY", "60"))

DEFAULT_TF = os.getenv("DEFAULT_TF", "5Min")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "360"))
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", "25"))
DEFAULT_SYMBOLS = os.getenv(
    "DEFAULT_SYMBOLS",
    "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD,ADA/USD,TON/USD,TRX/USD,APT/USD,ARB/USD,SUI/USD,OP/USD,MATIC/USD,NEAR/USD,ATOM/USD",
)

# ---------------------- In-Memory State ----------------------
_orders: List[Dict[str, Any]] = []  # append-only recent orders (capped)
_ORDERS_CAP = 1000

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _truncate_orders():
    global _orders
    if len(_orders) > _ORDERS_CAP:
        _orders = _orders[-_ORDERS_CAP:]

def _safe_float(x, default=0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def _coerce_order(o: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize an order dict coming from StrategyBook.
    Expected keys (best effort): ts, strategy, symbol, side, qty, price, pnl
    """
    out = {}
    out["ts"] = o.get("ts") or o.get("timestamp") or _now_utc_iso()
    out["strategy"] = o.get("strategy") or o.get("strat") or "unknown"
    out["symbol"] = o.get("symbol") or o.get("sym") or "?"
    out["side"] = (o.get("side") or "").upper() or "?"
    out["qty"] = _safe_float(o.get("qty") or o.get("quantity") or 0)
    out["price"] = _safe_float(o.get("price") or 0)
    out["pnl"] = _safe_float(o.get("pnl") or o.get("pnl_realized") or 0)
    return out

def _append_orders(orders: List[Dict[str, Any]]) -> None:
    for o in orders:
        if isinstance(o, dict):
            _orders.append(_coerce_order(o))
    _truncate_orders()

def _pnl_summary() -> Dict[str, Any]:
    total = 0.0
    by_strat: Dict[str, float] = {}
    by_day: Dict[str, float] = {}
    for o in _orders:
        pnl = _safe_float(o.get("pnl", 0))
        s = o.get("strategy", "unknown")
        ts = o.get("ts")
        total += pnl
        by_strat[s] = by_strat.get(s, 0.0) + pnl
        try:
            d = ts[:10]
        except Exception:
            d = date.today().isoformat()
        by_day[d] = by_day.get(d, 0.0) + pnl
    days_sorted = sorted(by_day.keys())
    return {
        "version": APP_VERSION,
        "updated_at": _now_utc_iso(),
        "pnl_total": round(total, 2),
        "pnl_by_strategy": {k: round(v, 2) for k, v in by_strat.items()},
        "pnl_calendar": [{"day": d, "pnl": round(by_day[d], 2)} for d in days_sorted],
        "orders_count": len(_orders),
    }

# ---------------------- Scan Bridge ----------------------
async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x

async def _scan_bridge(
    strat: str,
    req: Dict[str, Any],
    dry: int = 0,
) -> List[Dict[str, Any]]:
    """
    Align with StrategyBook.scan(strategy, contexts)
    contexts: dict[str, dict] — allow multiple named contexts; we provide "default".
    """
    tf = req.get("timeframe", DEFAULT_TF)
    lim = int(req.get("limit", DEFAULT_LIMIT))
    notional = float(req.get("notional", DEFAULT_NOTIONAL))
    symbols = req.get("symbols", DEFAULT_SYMBOLS)

    ctx_map = {
        "default": {
            "timeframe": tf,
            "limit": lim,
            "notional": notional,
            "symbols": symbols,
            "dry": dry,
        }
    }

    attempts = [
        (strat, ctx_map),                            # expected signature
        (strat, json.dumps(ctx_map)),               # if implementation wants a JSON string
        (strat, {"timeframe": tf, "limit": lim, "notional": notional, "symbols": symbols, "dry": dry}),  # flat dict
    ]

    last_error: Optional[Exception] = None
    for i, args in enumerate(attempts, start=1):
        try:
            res = await _maybe_await(_strategy_book.scan(*args))
            if isinstance(res, dict) and "orders" in res:
                return list(res.get("orders") or [])
            if isinstance(res, list):
                return res
            if isinstance(res, dict) and "data" in res and isinstance(res["data"], list):
                return res["data"]
            raise TypeError(f"unexpected return type: {type(res)}")
        except Exception as e:
            last_error = e
            log.warning("scan variant #%d failed: %s", i, e)

    raise TypeError(f"No compatible StrategyBook.scan signature (strategy, contexts). Last error: {last_error}")

# ---------------------- FastAPI ----------------------
app = FastAPI(title="Crypto System Control Plane", version=APP_VERSION)

class ScanBody(BaseModel):
    timeframe: Optional[str] = None
    limit: Optional[int] = None
    notional: Optional[float] = None
    symbols: Optional[str] = None
    dry: Optional[int] = 0

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard", status_code=307)

@app.get("/healthz")
def healthz():
    return PlainTextResponse("ok\n")

@app.get("/orders/attribution")
def attribution():
    return {
        "app_version": APP_VERSION,
        "auto_scan_enabled": AUTO_SCAN_ENABLED,
        "auto_scan_every": AUTO_SCAN_EVERY,
        "orders_cached": len(_orders),
        "defaults": {
            "timeframe": DEFAULT_TF,
            "limit": DEFAULT_LIMIT,
            "notional": DEFAULT_NOTIONAL,
            "symbols": DEFAULT_SYMBOLS,
        },
        "now": _now_utc_iso(),
    }

@app.get("/orders/recent")
def orders_recent(limit: int = 50):
    if limit <= 0:
        limit = 1
    return {"orders": _orders[-limit:]}

@app.get("/pnl/summary")
def pnl_summary():
    return _pnl_summary()

@app.post("/scan/{strat}")
async def scan_strat(
    strat: str,
    request: Request,
    timeframe: Optional[str] = None,
    limit: Optional[int] = None,
    notional: Optional[float] = None,
    symbols: Optional[str] = None,
    dry: int = 0,
    body: Optional[ScanBody] = None,
):
    req: Dict[str, Any] = {
        "timeframe": timeframe or (body.timeframe if body and body.timeframe else DEFAULT_TF),
        "limit": int(limit if limit is not None else (body.limit if body and body.limit is not None else DEFAULT_LIMIT)),
        "notional": float(notional if notional is not None else (body.notional if body and body.notional is not None else DEFAULT_NOTIONAL)),
        "symbols": symbols or (body.symbols if body and body.symbols else DEFAULT_SYMBOLS),
    }
    dry_val = dry if dry is not None else (body.dry if body and body.dry is not None else 0)

    try:
        orders = await _scan_bridge(strat, req, dry=dry_val)
    except Exception as e:
        log.error("StrategyBook.scan error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    if isinstance(orders, list) and dry_val == 0:
        _append_orders(orders)

    return {"ok": True, "strategy": strat, "request": req, "dry": dry_val, "orders": orders}

@app.post("/scan/all")
async def scan_all(
    timeframe: Optional[str] = None,
    limit: Optional[int] = None,
    notional: Optional[float] = None,
    symbols: Optional[str] = None,
    dry: int = 0,
    body: Optional[ScanBody] = None,
):
    req: Dict[str, Any] = {
        "timeframe": timeframe or (body.timeframe if body and body.timeframe else DEFAULT_TF),
        "limit": int(limit if limit is not None else (body.limit if body and body.limit is not None else DEFAULT_LIMIT)),
        "notional": float(notional if notional is not None else (body.notional if body and body.notional is not None else DEFAULT_NOTIONAL)),
        "symbols": symbols or (body.symbols if body and body.symbols else DEFAULT_SYMBOLS),
    }
    dry_val = dry if dry is not None else (body.dry if body and body.dry is not None else 0)

    results = []
    for strat in ("c1", "c2", "c3", "c4", "c5", "c6"):
        try:
            orders = await _scan_bridge(strat, req, dry=dry_val)
        except Exception as e:
            log.error("StrategyBook.scan error: %s", e, exc_info=True)
            orders = []
        if isinstance(orders, list) and dry_val == 0:
            _append_orders(orders)
        results.append({"strategy": strat, "count": len(orders), "orders": orders})
    return {"ok": True, "request": req, "dry": dry_val, "results": results}

# ---------------------- Dashboard (HTML) ----------------------
_DASHBOARD_HTML = Template("""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Crypto System Dashboard ($version)</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root { --bg:#0d1117; --card:#161b22; --ink:#c9d1d9; --muted:#8b949e; --pos:#2ea043; --neg:#f85149; --accent:#58a6ff; --grid:#30363d; }
  * { box-sizing: border-box; }
  body { margin:0; font-family: ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial; background: var(--bg); color: var(--ink); }
  header { padding:16px 20px; border-bottom:1px solid var(--grid); display:flex; justify-content:space-between; align-items:center; }
  header h1 { margin:0; font-size:18px; }
  header .meta { color: var(--muted); font-size:12px; }
  .wrap { padding: 20px; display:grid; gap:16px; grid-template-columns: repeat(12, 1fr); }
  .card { background:var(--card); border:1px solid var(--grid); border-radius:12px; padding:16px; }
  .span-4 { grid-column: span 4; }
  .span-6 { grid-column: span 6; }
  .span-8 { grid-column: span 8; }
  .span-12 { grid-column: span 12; }
  .kpi { display:flex; gap:14px; align-items:baseline; }
  .kpi .big { font-size:36px; font-weight:700; }
  .kpi .label { color:var(--muted); font-size:12px; }
  .pill { display:inline-block; padding:2px 8px; border-radius:999px; border:1px solid var(--grid); font-size:12px; color:var(--muted); }
  table { width:100%; border-collapse: collapse; }
  th, td { padding:8px 10px; border-bottom:1px solid var(--grid); font-size:13px; text-align:left; }
  th { color:var(--muted); font-weight:600; }
  .right { text-align:right; }
  .pos { color: var(--pos); }
  .neg { color: var(--neg); }
  .heat { display:grid; grid-template-columns: repeat(14, 1fr); gap:3px; }
  .heat .cell { height:14px; border-radius:3px; background:#20262e; }
  .muted { color: var(--muted); }
  .row { display:flex; gap:10px; align-items:center; flex-wrap: wrap; }
  .btn { background:#21262d; border:1px solid var(--grid); border-radius:8px; color:#c9d1d9; padding:8px 12px; font-size:13px; cursor:pointer; }
  .btn:hover { border-color:#8b949e; }
  .small { font-size:12px; }
  .mono { font-family: ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace; }
  @media(max-width: 960px){ .span-6,.span-8,.span-4 { grid-column: span 12; } }
</style>
</head>
<body>
<header>
  <h1>Crypto System — Dashboard <span class="pill">$version</span></h1>
  <div class="meta">Updated <span id="updatedAt">—</span></div>
</header>

<div class="wrap">
  <div class="card span-4">
    <div class="kpi">
      <div class="big" id="pnlTotal">—</div>
    </div>
    <div class="label">Total Realized P&L</div>
    <div class="small muted" id="ordersCount">— orders</div>
    <div class="row" style="margin-top:12px;">
      <button class="btn" id="btnRunAll">Run All (dry=0)</button>
      <button class="btn" id="btnRunDry">Run All (dry=1)</button>
    </div>
    <div class="small muted" style="margin-top:8px;">
      <span>TF:</span> <span class="mono" id="tf">—</span>&nbsp;&nbsp;
      <span>Limit:</span> <span class="mono" id="lim">—</span>&nbsp;&nbsp;
      <span>Notional:</span> <span class="mono" id="notional">—</span>
    </div>
  </div>

  <div class="card span-8">
    <h3 style="margin:0 0 10px 0;">P&L by Strategy</h3>
    <table id="tblByStrat">
      <thead><tr><th>Strategy</th><th class="right">P&L</th></tr></thead>
      <tbody></tbody>
    </table>
  </div>

  <div class="card span-12">
    <h3 style="margin:0 0 10px 0;">Calendar P&L</h3>
    <div id="calendarLegend" class="small muted" style="margin-bottom:8px;">Green=profit, Red=loss. Hover for values.</div>
    <div class="heat" id="heat"></div>
  </div>

  <div class="card span-12">
    <div class="row" style="justify-content:space-between;">
      <h3 style="margin:0;">Recent Orders</h3>
      <div class="small muted">auto-refreshing</div>
    </div>
    <table id="tblOrders">
      <thead>
        <tr>
          <th>Time</th><th>Strategy</th><th>Symbol</th><th>Side</th>
          <th class="right">Qty</th><th class="right">Price</th><th class="right">P&L</th>
        </tr>
      </thead>
      <tbody></tbody>
    </table>
  </div>
</div>

<script>
(function(){
  function fmtMoney(x){
    var n = Number(x || 0);
    var s = n.toFixed(2);
    if (n > 0) return '<span class="pos">+' + s + '</span>';
    if (n < 0) return '<span class="neg">' + s + '</span>';
    return s;
  }
  function esc(t){
    return String(t || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
  function setText(id, val){
    var el = document.getElementById(id);
    if (el) el.textContent = val;
  }
  function renderByStrat(map){
    var rows = '';
    var keys = Object.keys(map || {}).sort();
    for (var i=0;i<keys.length;i++){
      var k = keys[i]; var v = map[k] || 0;
      rows += '<tr><td class="mono">' + esc(k) + '</td><td class="right">' + fmtMoney(v) + '</td></tr>';
    }
    if (rows === '') rows = '<tr><td colspan="2" class="muted">No data yet</td></tr>';
    var tbody = document.querySelector('#tblByStrat tbody');
    if (tbody) tbody.innerHTML = rows;
  }
  function renderCalendar(arr){
    var heat = document.getElementById('heat');
    heat.innerHTML = '';
    if (!arr || arr.length === 0){ heat.innerHTML = '<div class="muted small">No P&L yet</div>'; return; }
    var minV=0, maxV=0;
    for (var i=0;i<arr.length;i++){ var p=Number(arr[i].pnl||0); if (p<minV) minV=p; if (p>maxV) maxV=p; }
    function colorFor(v){
      var p = Number(v||0);
      if (p===0) return '#20262e';
      if (p>0){
        var t = Math.min(1.0, p/(maxV||1));
        var g = Math.floor(40 + t*140);
        return 'rgb(40,' + (g+50) + ',70)';
      }else{
        var t2 = Math.min(1.0, Math.abs(p)/(Math.abs(minV)||1));
        var r = Math.floor(60 + t2*150);
        return 'rgb(' + (r+70) + ',40,40)';
      }
    }
    for (var j=0;j<arr.length;j++){
      var d = arr[j].day; var pv = Number(arr[j].pnl||0);
      var cell = document.createElement('div');
      cell.className = 'cell';
      cell.title = d + '  ' + pv.toFixed(2);
      cell.style.backgroundColor = colorFor(pv);
      heat.appendChild(cell);
    }
  }
  function renderOrders(list){
    var rows = '';
    for (var i=0;i<(list||[]).length;i++){
      var o = list[i];
      rows += '<tr>'
        + '<td class="mono">' + esc((o.ts||'').replace('T',' ').replace('Z','')) + '</td>'
        + '<td class="mono">' + esc(o.strategy||'?') + '</td>'
        + '<td class="mono">' + esc(o.symbol||'?') + '</td>'
        + '<td>' + esc(o.side||'?') + '</td>'
        + '<td class="right">' + esc((o.qty||0).toFixed ? o.qty.toFixed(4) : String(o.qty||0)) + '</td>'
        + '<td class="right">' + esc((o.price||0).toFixed ? o.price.toFixed(4) : String(o.price||0)) + '</td>'
        + '<td class="right">' + fmtMoney(o.pnl||0) + '</td>'
        + '</tr>';
    }
    if (rows === '') rows = '<tr><td colspan="7" class="muted">No orders yet</td></tr>';
    var tbody = document.querySelector('#tblOrders tbody');
    if (tbody) tbody.innerHTML = rows;
  }

  async function fetchJSON(url, opts){
    try{
      var r = await fetch(url, opts || {});
      if (!r.ok) return null;
      return await r.json();
    }catch(e){ return null; }
  }

  async function refresh(){
    var pnls = await fetchJSON('/pnl/summary');
    var recent = await fetchJSON('/orders/recent?limit=50');
    if (pnls){
      document.getElementById('updatedAt').textContent = new Date().toLocaleTimeString();
      document.getElementById('pnlTotal').innerHTML = fmtMoney(pnls.pnl_total);
      document.getElementById('ordersCount').textContent = (pnls.orders_count||0) + ' orders';
      renderByStrat(pnls.pnl_by_strategy || {});
      renderCalendar(pnls.pnl_calendar || []);
    }
    if (recent && recent.orders) renderOrders(recent.orders);
  }

  async function init(){
    var attr = await fetchJSON('/orders/attribution');
    if (attr && attr.defaults){
      document.getElementById('tf').textContent = attr.defaults.timeframe || '—';
      document.getElementById('lim').textContent = String(attr.defaults.limit || '—');
      document.getElementById('notional').textContent = String(attr.defaults.notional || '—');
    }
    await refresh();
    setInterval(refresh, 10000);

    document.getElementById('btnRunAll').addEventListener('click', async function(){
      var r = await fetchJSON('/scan/all?dry=0', {method:'POST'});
      await refresh();
      alert(r && r.ok ? 'Ran all (dry=0)' : 'Run failed');
    });
    document.getElementById('btnRunDry').addEventListener('click', async function(){
      var r = await fetchJSON('/scan/all?dry=1', {method:'POST'});
      alert(r && r.ok ? 'Ran all (dry=1)' : 'Run failed');
    });
  }

  window.addEventListener('load', init);
})();
</script>
</body>
</html>
""")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    html = _DASHBOARD_HTML.substitute(version=APP_VERSION)
    return HTMLResponse(html)

# ---------------------- Background Autoscheduler ----------------------
_scheduler_task: Optional[asyncio.Task] = None
_scheduler_stop = asyncio.Event()

async def _run_one_scan(strat: str, req: Dict[str, Any], dry: int = 0) -> None:
    try:
        orders = await _scan_bridge(strat, req, dry=dry)
        if isinstance(orders, list) and dry == 0:
            _append_orders(orders)
    except Exception:
        log.error("StrategyBook.scan error", exc_info=True)

async def _scheduler_loop():
    log.info("Scheduler loop started; every %ss", AUTO_SCAN_EVERY)
    while not _scheduler_stop.is_set():
        log.info("Scheduler tick: running all strategies (dry=0)")
        req = {"timeframe": DEFAULT_TF, "limit": DEFAULT_LIMIT, "notional": DEFAULT_NOTIONAL, "symbols": DEFAULT_SYMBOLS}
        for strat in ("c1", "c2", "c3", "c4", "c5", "c6"):
            await _run_one_scan(strat, req, dry=0)
            await asyncio.sleep(1.0)
        try:
            await asyncio.wait_for(_scheduler_stop.wait(), timeout=AUTO_SCAN_EVERY)
        except asyncio.TimeoutError:
            pass

@app.on_event("startup")
async def on_startup():
    if AUTO_SCAN_ENABLED:
        log.info("Auto scheduler enabled (every %ss)", AUTO_SCAN_EVERY)
        global _scheduler_task
        _scheduler_stop.clear()
        _scheduler_task = asyncio.create_task(_scheduler_loop())
    else:
        log.info("Auto scheduler disabled")

@app.on_event("shutdown")
async def on_shutdown():
    log.info("Shutting down app; scheduler will stop.")
    _scheduler_stop.set()
    t = globals().get("_scheduler_task")
    if t:
        try:
            await asyncio.wait_for(t, timeout=5)
        except Exception:
            pass

# ---------------------- Main ----------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, log_level="info")
