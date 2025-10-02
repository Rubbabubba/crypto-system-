# app.py
import os
import sys
import asyncio
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse

# ---------------------- App Metadata ----------------------
APP_NAME = "Crypto System Control Plane"
APP_VERSION = "1.8.0"

# ---------------------- Logging ----------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("app")

# ---------------------- Defaults ----------------------
DEFAULT_TF = "1h"
DEFAULT_LIMIT = 200
DEFAULT_NOTIONAL = 10_000.0
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEFAULT_STRATEGIES = ("c1", "c2", "c3", "c4", "c5", "c6")
SCHEDULER_INTERVAL_SEC = int(os.getenv("SCHEDULER_INTERVAL_SEC", "60"))
ORDERS_MAX = int(os.getenv("ORDERS_MAX", "1000"))

# ---------------------- In-Memory Store ----------------------
# A very simple in-memory “db” for orders.
_orders_lock = asyncio.Lock()
_orders: List[Dict[str, Any]] = []

def _append_orders(orders: List[Dict[str, Any]]) -> None:
    # keep list bounded
    if not orders:
        return
    capped = []
    ts = datetime.now(timezone.utc).isoformat()
    for o in orders:
        if "ts" not in o:
            o["ts"] = ts
        capped.append(o)
    _orders.extend(capped)
    if len(_orders) > ORDERS_MAX:
        del _orders[: len(_orders) - ORDERS_MAX]

# ---------------------- StrategyBook Shim ----------------------
"""
We’ll try to import your real StrategyBook. If not found, we expose a no-op
fallback that returns an empty list (so the app runs without crashing).
Expected signature (from your logs):
    class StrategyBook:
        def scan(self, req, contexts): -> List[Dict]
"""
StrategyBook: Any = None
_strategy_book_source = "fallback"

try:
    # Try common import paths. You can adjust if your module lives elsewhere.
    from strategy_book import StrategyBook as _SB  # type: ignore
    StrategyBook = _SB
    _strategy_book_source = "strategy_book.StrategyBook"
except Exception:
    try:
        from strategies import StrategyBook as _SB  # type: ignore
        StrategyBook = _SB
        _strategy_book_source = "strategies.StrategyBook"
    except Exception:
        class StrategyBook:  # type: ignore
            def scan(self, req, contexts):
                # Fallback: no-op scan (keeps the service healthy even w/o code)
                return []
        _strategy_book_source = "fallback(no-op)"

log.info("Using StrategyBook from: %s", _strategy_book_source)

# ---------------------- Scan Bridge ----------------------
async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x

def _build_contexts(
    timeframe: str,
    limit: int,
    notional: float,
    symbols: List[str],
    dry: int,
) -> Dict[str, Dict[str, Any]]:
    """
    Build a contexts dict that is both explicit and backwards-friendly.
    Your logs suggested some code was poking at keys like 'one' or iterating items,
    so we include both a 'one' entry and a 'default' entry with the same payload.
    """
    base = {
        "timeframe": timeframe,
        "limit": limit,
        "notional": notional,
        "symbols": symbols,
        "dry": dry,
    }
    return {
        "one": dict(base),        # some code seemed to index 'one'
        "default": dict(base),    # others might iterate or expect 'default'
    }

async def _scan_bridge(
    strat: str,
    req: Dict[str, Any],
    dry: int = 0,
) -> List[Dict[str, Any]]:
    """
    Call StrategyBook().scan(req, contexts) exactly as your logs imply.
    We pass the *whole* contexts dict (not just contexts['one']) to avoid
    “string indices” errors seen earlier.
    """
    tf: str = req.get("timeframe", DEFAULT_TF)
    lim: int = int(req.get("limit", DEFAULT_LIMIT))
    notional: float = float(req.get("notional", DEFAULT_NOTIONAL))
    symbols: List[str] = req.get("symbols", DEFAULT_SYMBOLS)

    ctx_map = _build_contexts(tf, lim, notional, symbols, dry)

    # Minimal, safe req payload your strategies can rely on
    compact_req = {
        "strategy": strat,
        "timeframe": tf,
        "limit": lim,
        "notional": notional,
        "symbols": symbols,
        "dry": dry,
        # passthrough for anything else
        "raw": req,
    }

    inst = StrategyBook()
    attempts: List[Tuple[str, Any]] = []

    # Single, canonical call (instance method)
    try:
        result = inst.scan(compact_req, ctx_map)
        result = await _maybe_await(result)
        if isinstance(result, list):
            return result
        # If a strategy returns a non-list, normalize to list
        if result is None:
            return []
        return [result]
    except Exception as e:
        attempts.append(("inst.scan(req, contexts)", e))

    # Final: give up but log what we tried
    for name, e in attempts:
        log.warning("scan attempt failed (%s): %s", name, e)
    log.error(
        "All scan attempts failed for strategy '%s'. Returning empty list. Last error: %s",
        strat,
        attempts[-1][1] if attempts else "unknown",
    )
    return []

# ---------------------- FastAPI ----------------------
app = FastAPI(title=APP_NAME, version=APP_VERSION)

# CORS for dashboard XHR
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------- Scheduler ----------------------
_scheduler_task: Optional[asyncio.Task] = None
_scheduler_stop = asyncio.Event()

async def _scheduler_loop():
    """
    Background loop: every SCHEDULER_INTERVAL_SEC, run scans for all strategies
    with dry=0. Append any returned orders to memory.
    """
    log.info("Scheduler started (interval=%ss).", SCHEDULER_INTERVAL_SEC)
    try:
        while not _scheduler_stop.is_set():
            start = time.time()
            log.info("Scheduler tick: running all strategies (dry=0)")
            req = {
                "timeframe": DEFAULT_TF,
                "limit": DEFAULT_LIMIT,
                "notional": DEFAULT_NOTIONAL,
                "symbols": DEFAULT_SYMBOLS,
            }
            for strat in DEFAULT_STRATEGIES:
                try:
                    orders = await _scan_bridge(strat, req, dry=0)
                except Exception as e:
                    log.error("Scheduler scan error [%s]: %s", strat, e, exc_info=True)
                    orders = []
                if isinstance(orders, list) and orders:
                    async with _orders_lock:
                        _append_orders(orders)
            elapsed = max(0.0, SCHEDULER_INTERVAL_SEC - (time.time() - start))
            try:
                await asyncio.wait_for(_scheduler_stop.wait(), timeout=elapsed)
            except asyncio.TimeoutError:
                pass
    finally:
        log.info("Scheduler stopped.")

@app.on_event("startup")
async def _on_startup():
    global _scheduler_task
    log.info("Starting up app; scheduler will start.")
    _scheduler_stop.clear()
    loop = asyncio.get_event_loop()
    _scheduler_task = loop.create_task(_scheduler_loop())

@app.on_event("shutdown")
async def _on_shutdown():
    log.info("Shutting down app; scheduler will stop.")
    _scheduler_stop.set()
    if _scheduler_task:
        try:
            await _scheduler_task
        except Exception:
            pass

# ---------------------- Helpers: PnL & Attribution ----------------------
def _compute_pnl_summary() -> Dict[str, Any]:
    """
    Very basic PnL roll-up from in-memory _orders.
    Expects order dicts to have at least: 'strategy', 'side', 'qty', 'price'
    """
    total_realized = 0.0
    per_symbol: Dict[str, float] = {}
    per_strategy: Dict[str, float] = {}

    for o in _orders:
        sym = o.get("symbol", "UNKNOWN")
        strat = o.get("strategy", "UNKNOWN")
        side = (o.get("side") or "").lower()
        qty = float(o.get("qty", 0))
        px = float(o.get("price", 0))

        # super-naive: buys negative PnL (cost), sells positive (proceeds)
        sign = 1.0 if side in ("sell", "short", "close") else -1.0
        pnl = sign * qty * px

        total_realized += pnl
        per_symbol[sym] = per_symbol.get(sym, 0.0) + pnl
        per_strategy[strat] = per_strategy.get(strat, 0.0) + pnl

    return {
        "total_realized": round(total_realized, 2),
        "per_symbol": {k: round(v, 2) for k, v in per_symbol.items()},
        "per_strategy": {k: round(v, 2) for k, v in per_strategy.items()},
        "count_orders": len(_orders),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

def _compute_attribution() -> Dict[str, Any]:
    by_strategy: Dict[str, Dict[str, Any]] = {}
    for o in _orders:
        strat = o.get("strategy", "UNKNOWN")
        if strat not in by_strategy:
            by_strategy[strat] = {"orders": 0, "notional": 0.0, "symbols": {}}
        by_strategy[strat]["orders"] += 1
        notional = float(o.get("qty", 0)) * float(o.get("price", 0))
        by_strategy[strat]["notional"] += notional
        sym = o.get("symbol", "UNKNOWN")
        by_strategy[strat]["symbols"][sym] = by_strategy[strat]["symbols"].get(sym, 0) + 1

    for strat, row in by_strategy.items():
        row["notional"] = round(row["notional"], 2)
    return {"by_strategy": by_strategy, "updated_at": datetime.now(timezone.utc).isoformat()}

# ---------------------- Routes: HTML ----------------------
@app.api_route("/", methods=["GET", "HEAD"])
def root():
    return RedirectResponse(url="/dashboard", status_code=307)

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    # Self-contained HTML (no external assets) — includes lightweight styling and JS polling.
    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>{APP_NAME} — Dashboard v{APP_VERSION}</title>
<style>
  :root {{
    --bg: #0f1220;
    --panel: #171a2b;
    --text: #e9ecf1;
    --muted: #9aa4b2;
    --accent: #5ee6a8;
    --accent2: #8ab4ff;
    --bad: #ff8a7a;
    --good: #7dffa1;
    --chip: #22263a;
    --border: #2a2e45;
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 0;
    font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Arial, "Apple Color Emoji", "Segoe UI Emoji";
    background: radial-gradient(1000px 600px at 10% -10%, #1a1f35 0%, #0f1220 35%, #0f1220 100%);
    color: var(--text);
  }}
  header {{
    position: sticky; top: 0; z-index: 5;
    background: rgba(15,18,32,0.7); backdrop-filter: blur(8px);
    border-bottom: 1px solid var(--border);
    padding: 16px 20px;
    display: grid; grid-template-columns: 1fr auto; gap: 12px; align-items: center;
  }}
  .brand {{
    font-weight: 700; letter-spacing: .3px; font-size: 16px; color: var(--text);
  }}
  .brand small {{ color: var(--muted); font-weight: 600; margin-left: 6px; }}
  .controls > * {{ margin-left: 8px; }}
  button, .chip {{
    border: 1px solid var(--border); background: var(--chip); color: var(--text);
    padding: 8px 12px; border-radius: 8px; font-weight: 600; cursor: pointer;
  }}
  button:hover {{ border-color: var(--accent2); }}
  main {{ padding: 20px; max-width: 1200px; margin: 0 auto; }}
  .grid {{
    display: grid; gap: 16px;
    grid-template-columns: repeat(12, 1fr);
  }}
  .card {{
    background: linear-gradient(0deg, rgba(255,255,255,0.02), rgba(255,255,255,0.02)), var(--panel);
    border: 1px solid var(--border);
    border-radius: 14px; padding: 14px;
    box-shadow: 0 10px 30px rgba(0,0,0,0.20), inset 0 1px 0 rgba(255,255,255,0.03);
  }}
  .span-4 {{ grid-column: span 4; min-width: 0; }}
  .span-8 {{ grid-column: span 8; min-width: 0; }}
  .span-12 {{ grid-column: span 12; min-width: 0; }}
  h2 {{ margin: 0 0 8px 0; font-size: 14px; color: var(--muted); font-weight: 700; letter-spacing: .3px; }}
  .stat {{
    font-size: 28px; font-weight: 800; margin: 4px 0 8px 0;
  }}
  .green {{ color: var(--good); }}
  .red {{ color: var(--bad); }}
  table {{ width: 100%; border-collapse: collapse; }}
  th, td {{ text-align: left; padding: 8px 10px; border-bottom: 1px dashed var(--border); }}
  th {{ font-size: 12px; color: var(--muted); font-weight: 700; }}
  td {{ font-size: 13px; }}
  .pill {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #1f2338; border: 1px solid var(--border); font-size: 12px; color: var(--accent2); }}
  .muted {{ color: var(--muted); }}
  .right {{ text-align: right; }}
  .fade {{ opacity: .85; }}
  .footer {{ margin-top: 16px; color: var(--muted); font-size: 12px; }}
  @media (max-width: 980px) {{
    .span-4 {{ grid-column: span 12; }}
    .span-8 {{ grid-column: span 12; }}
  }}
</style>
</head>
<body>
<header>
  <div class="brand">{APP_NAME} <small>v{APP_VERSION}</small></div>
  <div class="controls">
    <button id="run-all">Run All Now</button>
    <span class="chip" id="status-chip">idle</span>
  </div>
</header>

<main class="grid">
  <section class="card span-4" id="pnl-card">
    <h2>Realized PnL</h2>
    <div class="stat" id="pnl-total">—</div>
    <div class="muted fade" id="pnl-updated">Updated: —</div>
    <div style="margin-top:12px">
      <h3 class="muted" style="font-size:12px;margin:0 0 6px 0;">By Strategy</h3>
      <div id="pnl-by-strategy" class="fade"></div>
    </div>
  </section>

  <section class="card span-8">
    <h2>Recent Orders</h2>
    <table id="orders-table">
      <thead>
        <tr>
          <th>Time</th>
          <th>Strategy</th>
          <th>Symbol</th>
          <th>Side</th>
          <th class="right">Qty</th>
          <th class="right">Price</th>
          <th class="right">Notional</th>
        </tr>
      </thead>
      <tbody id="orders-body"></tbody>
    </table>
  </section>

  <section class="card span-12">
    <h2>Attribution</h2>
    <table id="attr-table">
      <thead>
        <tr>
          <th>Strategy</th>
          <th class="right">Orders</th>
          <th class="right">Notional</th>
          <th>Symbols</th>
        </tr>
      </thead>
      <tbody id="attr-body"></tbody>
    </table>
  </section>

  <section class="card span-12 footer">
    Scheduler interval: {SCHEDULER_INTERVAL_SEC}s · Strategies: {", ".join(DEFAULT_STRATEGIES)}
  </section>
</main>

<script>
const fmt = (n) => (typeof n === 'number' ? n.toLocaleString(undefined, {{maximumFractionDigits: 2}}) : n);

async function fetchJSON(url) {{
  const r = await fetch(url, {{cache: 'no-store'}});
  if (!r.ok) throw new Error('HTTP ' + r.status);
  return await r.json();
}}

function renderPnL(data) {{
  const total = data.total_realized || 0;
  const cls = total >= 0 ? 'green' : 'red';
  document.getElementById('pnl-total').innerHTML = `<span class="${{cls}}">$ ${{fmt(total)}}</span>`;
  document.getElementById('pnl-updated').textContent = 'Updated: ' + (data.updated_at || '—');

  const byStrat = data.per_strategy || {{}};
  const parts = Object.entries(byStrat)
    .sort((a,b) => Math.abs(b[1]) - Math.abs(a[1]))
    .map(([k,v]) => `<div class="fade"><span class="pill">${{k}}</span> &nbsp; $ {{fmt(v)}}</div>`);
  document.getElementById('pnl-by-strategy').innerHTML = parts.join('') || '<span class="muted">No data</span>';
}}

function renderOrders(data) {{
  const rows = (data.orders || []).map(o => {{
    const notional = (parseFloat(o.qty||0) * parseFloat(o.price||0)) || 0;
    const side = (o.side||'').toUpperCase();
    const t = o.ts ? new Date(o.ts).toLocaleString() : '—';
    return `<tr>
      <td>${{t}}</td>
      <td><span class="pill">${{o.strategy||'—'}}</span></td>
      <td>${{o.symbol||'—'}}</td>
      <td>${{side}}</td>
      <td class="right">${{fmt(parseFloat(o.qty||0))}}</td>
      <td class="right">${{fmt(parseFloat(o.price||0))}}</td>
      <td class="right">${{fmt(notional)}}</td>
    </tr>`;
  }});
  document.getElementById('orders-body').innerHTML = rows.join('') || `<tr><td colspan="7" class="muted">No orders yet</td></tr>`;
}}

function renderAttr(data) {{
  const rows = Object.entries(data.by_strategy || {{}}).map(([k, v]) => {{
    const syms = Object.entries(v.symbols||{{}})
      .map(([s,c]) => `<span class="pill">${{s}} × ${{c}}</span>`)
      .join(' ');
    return `<tr>
      <td><span class="pill">${{k}}</span></td>
      <td class="right">${{v.orders || 0}}</td>
      <td class="right">$ {{fmt(v.notional || 0)}}</td>
      <td>${{syms || '<span class="muted">—</span>'}}</td>
    </tr>`;
  }});
  document.getElementById('attr-body').innerHTML = rows.join('') || `<tr><td colspan="4" class="muted">No data</td></tr>`;
}}

async function refreshAll() {{
  try {{
    document.getElementById('status-chip').textContent = 'refreshing…';
    const [pnl, rec, attr] = await Promise.all([
      fetchJSON('/pnl/summary'),
      fetchJSON('/orders/recent?limit=50'),
      fetchJSON('/orders/attribution'),
    ]);
    renderPnL(pnl);
    renderOrders(rec);
    renderAttr(attr);
    document.getElementById('status-chip').textContent = 'ok';
  }} catch (e) {{
    console.error(e);
    document.getElementById('status-chip').textContent = 'error';
  }}
}}

async function runAllNow() {{
  try {{
    document.getElementById('status-chip').textContent = 'scanning…';
    const r = await fetch('/scan/all', {{method:'POST'}});
    await refreshAll();
    document.getElementById('status-chip').textContent = 'ok';
  }} catch (e) {{
    console.error(e);
    document.getElementById('status-chip').textContent = 'error';
  }}
}}

document.getElementById('run-all').addEventListener('click', runAllNow);
refreshAll();
setInterval(refreshAll, 6000);
</script>
</body>
</html>
"""
    return HTMLResponse(content=html)

# ---------------------- Routes: JSON APIs ----------------------
@app.get("/pnl/summary", response_class=JSONResponse)
async def pnl_summary():
    async with _orders_lock:
        data = _compute_pnl_summary()
    return JSONResponse(data)

@app.get("/orders/recent", response_class=JSONResponse)
async def orders_recent(limit: int = Query(50, ge=1, le=ORDERS_MAX)):
    async with _orders_lock:
        out = list(reversed(_orders[-limit:]))  # newest first
    return JSONResponse({"orders": out})

@app.get("/orders/attribution", response_class=JSONResponse)
async def orders_attribution():
    async with _orders_lock:
        data = _compute_attribution()
    return JSONResponse(data)

# Quick health
@app.get("/healthz", response_class=PlainTextResponse)
def health():
    return PlainTextResponse("ok")

# ---------------------- Routes: Triggers ----------------------
@app.post("/scan/{strategy}", response_class=JSONResponse)
async def run_one(strategy: str, dry: int = Query(0, ge=0, le=1)):
    req = {
        "timeframe": DEFAULT_TF,
        "limit": DEFAULT_LIMIT,
        "notional": DEFAULT_NOTIONAL,
        "symbols": DEFAULT_SYMBOLS,
    }
    try:
        orders = await _scan_bridge(strategy, req, dry=dry)
    except Exception as e:
        log.error("StrategyBook.scan error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    if isinstance(orders, list) and dry == 0:
        async with _orders_lock:
            _append_orders(orders)
    return {"ok": True, "strategy": strategy, "dry": dry, "orders": orders}

@app.post("/scan/all", response_class=JSONResponse)
async def run_all(dry: int = Query(0, ge=0, le=1)):
    req = {
        "timeframe": DEFAULT_TF,
        "limit": DEFAULT_LIMIT,
        "notional": DEFAULT_NOTIONAL,
        "symbols": DEFAULT_SYMBOLS,
    }
    results = []
    for strat in DEFAULT_STRATEGIES:
        try:
            orders = await _scan_bridge(strat, req, dry=dry)
        except Exception as e:
            log.error("StrategyBook.scan error [%s]: %s", strat, e, exc_info=True)
            orders = []
        if isinstance(orders, list) and dry == 0 and orders:
            async with _orders_lock:
                _append_orders(orders)
        results.append({"strategy": strat, "count": len(orders), "orders": orders})
    return {"ok": True, "dry": dry, "results": results}

# ---------------------- Main ----------------------
if __name__ == "__main__":
    # Allow `python app.py` to Just Work on Render/Heroku/etc.
    port = int(os.getenv("PORT", "10000"))
    host = os.getenv("HOST", "0.0.0.0")
    try:
        import uvicorn  # type: ignore
        uvicorn.run("app:app", host=host, port=port, reload=False, log_level="info")
    except Exception as e:
        log.error("Failed to start uvicorn: %s", e, exc_info=True)
        # Fallback: simple ASGI server if uvicorn missing (unlikely on Render)
        from starlette.responses import PlainTextResponse as _Plain
        async def _fallback_scope(scope, receive, send):
            if scope["type"] == "http":
                res = _Plain("Server failed to start (uvicorn missing).", status_code=500)
                await res(scope, receive, send)
        import anyio
        anyio.run(lambda: None)