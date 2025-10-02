# app.py
# Crypto System Control Plane
# Full FastAPI app with scheduler, scan bridge, and rich dashboard.
# Version bumped.

from __future__ import annotations

import os
import sys
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

# ------------------------------------------------------------------------------
# Version / Constants
# ------------------------------------------------------------------------------
APP_VERSION = "1.5.0"  # bumped

DEFAULT_TF = "1h"
DEFAULT_LIMIT = 200
DEFAULT_NOTIONAL = 1000.0
DEFAULT_SYMBOLS: List[str] = ["BTCUSDT", "ETHUSDT"]

ORDERS_MAX_BUFFER = 10000
DEFAULT_SCHED_INTERVAL_SEC = int(os.getenv("SCHED_INTERVAL_SEC", "60"))
AUTO_SCHEDULER = os.getenv("AUTO_SCHEDULER", "0")  # "1" to enable
SCHED_STRATS = tuple((os.getenv("SCHED_STRATS") or "c1,c2,c3,c4,c5,c6").split(","))

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
log = logging.getLogger("app")
if not log.handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

# Marker log seen in your traces
log.info("Loaded StrategyBook from strategies.StrategyBook")

# ------------------------------------------------------------------------------
# StrategyBook import (with safe stub)
# ------------------------------------------------------------------------------
try:
    from strategies import StrategyBook  # type: ignore
except Exception as e:
    log.exception("Failed to import StrategyBook; using a stub. Error: %s", e)

    class StrategyBook:  # type: ignore
        """
        Minimal stub for local testing:
        scan(strategy: str, contexts: List[dict]) -> List[dict]
        """
        @staticmethod
        def scan(strategy: str, contexts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            ctx = contexts[0] if contexts else {}
            symbols = ctx.get("symbols") or DEFAULT_SYMBOLS
            now = datetime.now(timezone.utc).isoformat()
            orders = []
            for sym in symbols:
                orders.append({
                    "id": f"{strategy}-{sym}-{int(datetime.now().timestamp())}",
                    "ts": now,
                    "strategy": strategy,
                    "symbol": sym,
                    "side": "buy",
                    "qty": round((ctx.get("notional", 1000.0) / 1000.0), 6),
                    "price": 100.0,
                    "notional": float(ctx.get("notional", 1000.0)),
                    "timeframe": ctx.get("timeframe", DEFAULT_TF),
                })
            return orders

# ------------------------------------------------------------------------------
# In-memory order buffer
# ------------------------------------------------------------------------------
_orders: List[Dict[str, Any]] = []  # newest last

def _append_orders(orders: List[Dict[str, Any]]) -> None:
    global _orders
    if not orders:
        return
    _orders.extend(orders)
    if len(_orders) > ORDERS_MAX_BUFFER:
        _orders = _orders[-ORDERS_MAX_BUFFER:]

def _recent_orders(limit: int = 50) -> List[Dict[str, Any]]:
    return _orders[-limit:] if limit > 0 else []

def _pnl_summary() -> Dict[str, Any]:
    """
    Very simple demo PnL summary; replace with real logic as needed.
    """
    total_notional = 0.0
    buys = sells = 0.0
    by_strategy: Dict[str, Dict[str, float]] = {}
    by_symbol: Dict[str, Dict[str, float]] = {}

    for o in _orders:
        notional = float(o.get("notional", 0.0))
        total_notional += notional
        side = str(o.get("side"))
        if side == "buy":
            buys += notional
        elif side == "sell":
            sells += notional

        strat = str(o.get("strategy", "unknown"))
        sym = str(o.get("symbol", "UNK"))
        by_strategy.setdefault(strat, {"count": 0, "notional": 0.0})
        by_strategy[strat]["count"] += 1
        by_strategy[strat]["notional"] += notional

        by_symbol.setdefault(sym, {"count": 0, "notional": 0.0})
        by_symbol[sym]["count"] += 1
        by_symbol[sym]["notional"] += notional

    return {
        "total_notional": round(total_notional, 2),
        "buys": round(buys, 2),
        "sells": round(sells, 2),
        "net_flow": round(buys - sells, 2),
        "strategies": by_strategy,
        "symbols": by_symbol,
        "count": len(_orders),
        "asOf": datetime.now(timezone.utc).isoformat(),
        "version": APP_VERSION,
    }

# ------------------------------------------------------------------------------
# Scan Bridge
# ------------------------------------------------------------------------------
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
    Canonical bridge to StrategyBook.scan(strategy, contexts)
    - contexts is a *list* of dicts (we pass a single default context).
    """
    tf = req.get("timeframe", DEFAULT_TF)
    lim = int(req.get("limit", DEFAULT_LIMIT))
    notional = float(req.get("notional", DEFAULT_NOTIONAL))
    symbols = req.get("symbols", DEFAULT_SYMBOLS)

    # Normalize CSV strings into list
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]

    contexts = [{
        "timeframe": tf,
        "limit": lim,
        "notional": notional,
        "symbols": symbols,
        "dry": dry,
    }]

    # Single canonical call (no kwargs/positional experiments)
    res = StrategyBook.scan(strat, contexts)
    orders = await _maybe_await(res)

    if orders is None:
        return []
    if not isinstance(orders, list):
        orders = [orders]
    return orders

# ------------------------------------------------------------------------------
# FastAPI App + CORS
# ------------------------------------------------------------------------------
app = FastAPI(title="Crypto System Control Plane", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------------------
# HTML Dashboard (rich)
# ------------------------------------------------------------------------------
_DASHBOARD_HTML = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Crypto System Control Plane</title>
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <style>
    :root {{
      --bg: #0b1220;
      --card: #101a2b;
      --muted: #7b8ba1;
      --text: #e4edf7;
      --accent: #37c0ff;
      --accent-2: #4ade80;
      --warn: #f59e0b;
      --danger: #ef4444;
      --border: #13223a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; background: var(--bg); color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
    }}
    header {{
      position: sticky; top: 0; background: rgba(11,18,32,0.85);
      border-bottom: 1px solid var(--border); backdrop-filter: blur(6px);
      display: flex; align-items: center; justify-content: space-between;
      padding: 14px 22px;
    }}
    header .title {{
      font-weight: 700; letter-spacing: 0.3px; display:flex; gap:10px; align-items:center;
    }}
    .badge {{ font-size: 12px; color: var(--muted); padding: 2px 8px; border:1px solid var(--border); border-radius: 999px; }}
    main {{ padding: 24px; max-width: 1200px; margin: 0 auto; }}
    .grid {{
      display: grid; gap: 16px;
      grid-template-columns: repeat(12, 1fr);
    }}
    .card {{
      background: linear-gradient(180deg, rgba(25,40,70,0.35), rgba(23,36,63,0.25));
      border: 1px solid var(--border); border-radius: 14px; padding: 16px;
    }}
    .card h3 {{ margin: 0 0 10px 0; font-size: 16px; color: #cfe1ff; }}
    .row {{ display: flex; gap: 10px; flex-wrap: wrap; align-items: center; }}
    label {{ font-size: 12px; color: var(--muted); }}
    input, select {{
      background: #0e1727; color: var(--text); border: 1px solid var(--border);
      padding: 8px 10px; border-radius: 10px; outline: none;
    }}
    input:focus, select:focus {{ border-color: var(--accent); }}
    button {{
      background: #13223a; color: var(--text); border: 1px solid var(--border);
      padding: 10px 14px; border-radius: 10px; cursor: pointer;
    }}
    button.primary {{ border-color: #155e75; background: #0b3d4f; }}
    button.primary:hover {{ background: #0e4c63; }}
    button.ghost:hover {{ border-color: var(--accent); }}
    table {{
      width: 100%; border-collapse: collapse; font-size: 13px;
    }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid var(--border); }}
    th {{ text-align: left; color: var(--muted); font-weight: 600; }}
    .pill {{ padding: 2px 8px; border: 1px solid var(--border); border-radius: 999px; font-size: 12px; color: var(--muted); }}
    .muted {{ color: var(--muted); }}
    .kpi {{ font-size: 22px; font-weight: 700; }}
    .kpi-row {{ display:flex; gap: 16px; }}
    .kpi .label {{ font-size: 12px; color: var(--muted); font-weight: 500; }}
    .col-4 {{ grid-column: span 4; }}
    .col-6 {{ grid-column: span 6; }}
    .col-8 {{ grid-column: span 8; }}
    .col-12 {{ grid-column: span 12; }}
    @media (max-width: 920px) {{
      .col-4, .col-6, .col-8 {{ grid-column: span 12; }}
    }}
    .footer {{ margin-top: 20px; color: var(--muted); font-size: 12px; text-align:center; }}
    code {{ background:#0d1625; border:1px solid var(--border); padding:2px 6px; border-radius:6px; }}
  </style>
</head>
<body>
  <header>
    <div class="title">
      <span>⚡</span>
      <span>Crypto System Control Plane</span>
      <span class="badge">v {APP_VERSION}</span>
    </div>
    <div class="row">
      <span class="muted">Scheduler:</span>
      <span class="pill" id="sched-pill">{'Enabled' if AUTO_SCHEDULER == '1' else 'Disabled'}</span>
      <span class="pill">Interval: {DEFAULT_SCHED_INTERVAL_SEC}s</span>
      <span class="pill">Strats: {", ".join(SCHED_STRATS)}</span>
    </div>
  </header>

  <main>
    <div class="grid">
      <div class="card col-12">
        <h3>Quick Scan</h3>
        <div class="row" style="gap:12px; margin-bottom: 10px;">
          <label>Strategy</label>
          <select id="scan-strategy">
            {"".join(f'<option value="{s}">{s}</option>' for s in SCHED_STRATS)}
          </select>

          <label>Timeframe</label>
          <select id="tf">
            <option>1m</option><option>5m</option><option>15m</option>
            <option>1h</option><option selected>4h</option><option>1d</option>
          </select>

          <label>Limit</label>
          <input id="limit" type="number" min="1" max="5000" value="{DEFAULT_LIMIT}" />

          <label>Notional</label>
          <input id="notional" type="number" min="1" step="1" value="{int(DEFAULT_NOTIONAL)}" />

          <label>Symbols (CSV)</label>
          <input id="symbols" type="text" value="{",".join(DEFAULT_SYMBOLS)}" style="min-width:320px;" />

          <label>Dry</label>
          <select id="dry"><option value="1">1</option><option value="0" selected>0</option></select>

          <button class="primary" id="btn-scan-one">Run Scan (One)</button>
          <button class="ghost" id="btn-scan-all">Run Scan (All)</button>
        </div>
        <div id="scan-result" class="muted"></div>
      </div>

      <div class="card col-6">
        <h3>PnL Summary</h3>
        <div class="kpi-row">
          <div class="kpi">Total: <span id="k-total">-</span></div>
          <div class="kpi">Buys: <span id="k-buys">-</span></div>
          <div class="kpi">Sells: <span id="k-sells">-</span></div>
          <div class="kpi">Net: <span id="k-net">-</span></div>
        </div>
        <div class="muted" id="k-asof" style="margin-top:6px;">as of -</div>
        <div style="margin-top:12px;">
          <h4 class="muted" style="margin:0 0 6px 0;">By Strategy</h4>
          <table id="tbl-strat">
            <thead><tr><th>Strategy</th><th>Count</th><th>Notional</th></tr></thead>
            <tbody></tbody>
          </table>
        </div>
      </div>

      <div class="card col-6">
        <h3>Recent Orders</h3>
        <div class="row" style="margin-bottom:10px;">
          <label>Limit</label>
          <input id="recent-limit" type="number" min="1" max="500" value="50" />
          <button id="btn-refresh-orders">Refresh</button>
        </div>
        <table id="tbl-orders">
          <thead>
            <tr>
              <th>Time</th><th>Strategy</th><th>Symbol</th><th>Side</th>
              <th>Qty</th><th>Price</th><th>Notional</th><th>TF</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>

      <div class="card col-12">
        <h3>Attribution</h3>
        <table id="tbl-attr">
          <thead><tr><th>Strategy</th><th>Order Count</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>

      <div class="card col-12">
        <h3>API Endpoints</h3>
        <ul>
          <li>GET <code>/orders/attribution</code></li>
          <li>GET <code>/orders/recent?limit=50</code></li>
          <li>GET <code>/pnl/summary</code></li>
          <li>POST <code>/scan/&lt;strat&gt;</code> — JSON body: <code>{{"timeframe","limit","notional","symbols"}}</code></li>
          <li>POST <code>/scan/all</code> — JSON body: <code>{{"timeframe","limit","notional","symbols"}}</code></li>
        </ul>
        <p class="muted">Try: <code>curl -X POST /scan/c1 -H "Content-Type: application/json" -d '{{"symbols":["BTCUSDT","ETHUSDT"]}}'</code></p>
      </div>
    </div>

    <div class="footer">
      &copy; {datetime.now().year} • Control Plane • Build <code>{APP_VERSION}</code>
    </div>
  </main>

  <script>
    const fmt = new Intl.NumberFormat(undefined, {{ maximumFractionDigits: 2 }});
    const el = (id) => document.getElementById(id);

    async function fetchJSON(url, opts) {{
      const r = await fetch(url, opts);
      if (!r.ok) throw new Error(await r.text());
      return await r.json();
    }}

    // PnL Summary
    async function loadSummary() {{
      try {{
        const data = await fetchJSON("/pnl/summary");
        el("k-total").textContent = fmt.format(data.total_notional);
        el("k-buys").textContent = fmt.format(data.buys);
        el("k-sells").textContent = fmt.format(data.sells);
        el("k-net").textContent = fmt.format(data.net_flow);
        el("k-asof").textContent = "as of " + new Date(data.asOf).toLocaleString();

        const tb = el("tbl-strat").querySelector("tbody");
        tb.innerHTML = "";
        const entries = Object.entries(data.strategies || {{}}).sort((a,b) => b[1].notional - a[1].notional);
        for (const [name, v] of entries) {{
          const tr = document.createElement("tr");
          tr.innerHTML = `<td>${{name}}</td><td>${{v.count}}</td><td>${{fmt.format(v.notional)}}</td>`;
          tb.appendChild(tr);
        }}
      }} catch (e) {{
        console.error("summary error", e);
      }}
    }}

    // Recent Orders
    async function loadOrders() {{
      try {{
        const limit = Number(el("recent-limit").value) || 50;
        const data = await fetchJSON(`/orders/recent?limit=${{limit}}`);
        const tb = el("tbl-orders").querySelector("tbody");
        tb.innerHTML = "";
        for (const o of data.slice().reverse()) {{
          const tr = document.createElement("tr");
          tr.innerHTML = `
            <td>${{new Date(o.ts || o.time || Date.now()).toLocaleString()}}</td>
            <td>${{o.strategy || ""}}</td>
            <td>${{o.symbol || ""}}</td>
            <td>${{o.side || ""}}</td>
            <td>${{o.qty ?? ""}}</td>
            <td>${{o.price ?? ""}}</td>
            <td>${{fmt.format(o.notional ?? 0)}}</td>
            <td>${{o.timeframe || ""}}</td>
          `;
          tb.appendChild(tr);
        }}
      }} catch (e) {{
        console.error("orders error", e);
      }}
    }}

    // Attribution
    async function loadAttribution() {{
      try {{
        const data = await fetchJSON("/orders/attribution");
        const tb = el("tbl-attr").querySelector("tbody");
        tb.innerHTML = "";
        const entries = Object.entries(data.counts || {{}}).sort((a,b) => b[1] - a[1]);
        for (const [name, count] of entries) {{
          const tr = document.createElement("tr");
          tr.innerHTML = `<td>${{name}}</td><td>${{count}}</td>`;
          tb.appendChild(tr);
        }}
      }} catch (e) {{
        console.error("attr error", e);
      }}
    }}

    // Scan actions
    async function runScanOne() {{
      const strat = el("scan-strategy").value;
      const body = {{
        timeframe: el("tf").value,
        limit: Number(el("limit").value) || {DEFAULT_LIMIT},
        notional: Number(el("notional").value) || {int(DEFAULT_NOTIONAL)},
        symbols: el("symbols").value.split(",").map(s => s.trim()).filter(Boolean)
      }};
      const dry = el("dry").value;
      el("scan-result").textContent = "Running scan for " + strat + "...";
      try {{
        const res = await fetchJSON(`/scan/${{encodeURIComponent(strat)}}?dry=${{dry}}`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(body)
        }});
        el("scan-result").textContent = `OK: ${{
          res.orders?.length ?? (res.results?.reduce((a,x)=>a+x.count,0) || 0)
        }} orders`;
        // refresh panels
        loadSummary();
        loadOrders();
        loadAttribution();
      }} catch (e) {{
        el("scan-result").textContent = "Error: " + e.message;
      }}
    }}

    async function runScanAll() {{
      const body = {{
        timeframe: el("tf").value,
        limit: Number(el("limit").value) || {DEFAULT_LIMIT},
        notional: Number(el("notional").value) || {int(DEFAULT_NOTIONAL)},
        symbols: el("symbols").value.split(",").map(s => s.trim()).filter(Boolean)
      }};
      const dry = el("dry").value;
      el("scan-result").textContent = "Running scan for ALL strategies...";
      try {{
        const res = await fetchJSON(`/scan/all?dry=${{dry}}`, {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(body)
        }});
        const total = (res.results || []).reduce((a, x) => a + (x.count || 0), 0);
        el("scan-result").textContent = `OK: ${{total}} orders across ${{(res.results||[]).length}} strategies`;
        loadSummary();
        loadOrders();
        loadAttribution();
      }} catch (e) {{
        el("scan-result").textContent = "Error: " + e.message;
      }}
    }}

    // Wire up
    document.addEventListener("DOMContentLoaded", () => {{
      el("btn-scan-one").addEventListener("click", runScanOne);
      el("btn-scan-all").addEventListener("click", runScanAll);
      el("btn-refresh-orders").addEventListener("click", () => {{
        loadOrders();
        loadAttribution();
        loadSummary();
      }});
      // initial load
      loadSummary();
      loadOrders();
      loadAttribution();
      // auto refresh every 10s for panels
      setInterval(() => {{
        loadSummary();
        loadAttribution();
      }}, 10000);
    }});
  </script>
</body>
</html>
""".strip()

# ------------------------------------------------------------------------------
# Basic Routes
# ------------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/dashboard")

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard() -> str:
    return _DASHBOARD_HTML

@app.get("/orders/attribution")
async def orders_attribution() -> Dict[str, Any]:
    by_strategy: Dict[str, int] = {}
    for o in _orders:
        s = str(o.get("strategy", "unknown"))
        by_strategy[s] = by_strategy.get(s, 0) + 1
    return {
        "ok": True,
        "asOf": datetime.now(timezone.utc).isoformat(),
        "counts": by_strategy,
        "version": APP_VERSION,
    }

@app.get("/orders/recent")
async def orders_recent(limit: int = 50) -> List[Dict[str, Any]]:
    return _recent_orders(limit=limit)

@app.get("/pnl/summary")
async def pnl_summary() -> Dict[str, Any]:
    return _pnl_summary()

# ------------------------------------------------------------------------------
# Scan APIs
# ------------------------------------------------------------------------------
@app.post("/scan/{strat}")
async def scan_one(strat: str, req: Dict[str, Any], dry: int = 0) -> Dict[str, Any]:
    dry_val = int(dry)
    try:
        orders = await _scan_bridge(strat, req, dry=dry_val)
    except Exception as e:
        log.error("StrategyBook.scan error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    if isinstance(orders, list) and dry_val == 0:
        _append_orders(orders)

    return {
        "ok": True,
        "strategy": strat,
        "request": req,
        "dry": dry_val,
        "orders": orders,
    }

@app.post("/scan/all")
async def scan_all(req: Dict[str, Any], dry: int = 0) -> Dict[str, Any]:
    dry_val = int(dry)
    results: List[Dict[str, Any]] = []
    for strat in SCHED_STRATS:
        try:
            orders = await _scan_bridge(strat, req, dry=dry_val)
        except Exception as e:
            log.error("StrategyBook.scan error: %s", e, exc_info=True)
            orders = []
        if isinstance(orders, list) and dry_val == 0:
            _append_orders(orders)
        results.append({"strategy": strat, "count": len(orders), "orders": orders})
    return {"ok": True, "request": req, "dry": dry_val, "results": results}

# ------------------------------------------------------------------------------
# Scheduler
# ------------------------------------------------------------------------------
_scheduler_task: Optional[asyncio.Task] = None
_shutdown_event = asyncio.Event()

async def _scheduler_loop() -> None:
    dry = 0
    req: Dict[str, Any] = {
        "timeframe": DEFAULT_TF,
        "limit": DEFAULT_LIMIT,
        "notional": DEFAULT_NOTIONAL,
        "symbols": DEFAULT_SYMBOLS,
    }
    while not _shutdown_event.is_set():
        try:
            log.info("Scheduler tick: running all strategies (dry=%s)", dry)
            for strat in SCHED_STRATS:
                try:
                    orders = await _scan_bridge(strat, req, dry=dry)
                except Exception as e:
                    log.error("StrategyBook.scan error: %s", e, exc_info=True)
                    orders = []
                if isinstance(orders, list) and dry == 0:
                    _append_orders(orders)
        except Exception as e:
            log.error("Scheduler loop error: %s", e, exc_info=True)
        await asyncio.wait(
            [asyncio.create_task(_shutdown_event.wait())],
            timeout=DEFAULT_SCHED_INTERVAL_SEC,
        )

# ------------------------------------------------------------------------------
# Lifecycle
# ------------------------------------------------------------------------------
@app.on_event("startup")
async def on_startup() -> None:
    enabled = AUTO_SCHEDULER == "1"
    log.info("Auto scheduler %s", "enabled" if enabled else "disabled")
    if enabled:
        global _scheduler_task
        _scheduler_task = asyncio.create_task(_scheduler_loop())

@app.on_event("shutdown")
async def on_shutdown() -> None:
    log.info("Shutting down app; scheduler will stop.")
    _shutdown_event.set()
    if _scheduler_task:
        try:
            await asyncio.wait_for(_scheduler_task, timeout=5)
        except Exception:
            pass

# ------------------------------------------------------------------------------
# Error handlers
# ------------------------------------------------------------------------------
@app.exception_handler(HTTPException)
async def http_ex_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})

@app.exception_handler(Exception)
async def unhandled_ex_handler(request: Request, exc: Exception):
    log.error("Unhandled error: %s", exc, exc_info=True)
    return JSONResponse(status_code=500, content={"ok": False, "detail": str(exc)})

# ------------------------------------------------------------------------------
# Entrypoint
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "10000"))
    log.info("Uvicorn starting on 0.0.0.0:%d", port)
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=bool(os.getenv("RELOAD")))
