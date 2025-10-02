#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import asyncio
import logging
import inspect
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel

# ---------------------- App metadata ----------------------
APP_NAME = "Crypto System Control Plane"
APP_VERSION = "1.7.3"  # bumped

# ---------------------- Logging ----------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

# ---------------------- Strategy imports (optional) ----------------------
StrategyBook = None
try:
    # Should define StrategyBook with some .scan variant
    from strategies import StrategyBook as _SB  # type: ignore
    StrategyBook = _SB
    log.info("Loaded StrategyBook from strategies module.")
except Exception as e:
    log.warning("Could not import StrategyBook: %s", e)

# ---------------------- Defaults & Globals ----------------------
DEFAULT_TF = "1h"
DEFAULT_LIMIT = 200
DEFAULT_NOTIONAL = 1000.0
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
DEFAULT_DRY = 0

SCHEDULER_INTERVAL_SEC = int(os.environ.get("SCHEDULER_INTERVAL_SEC", "60"))
STRATEGIES: Tuple[str, ...] = ("c1", "c2", "c3", "c4", "c5", "c6")

# naive in-memory store
ORDERS_STORE: List[Dict[str, Any]] = []
ATTR_STORE: Dict[str, Dict[str, Union[int, float]]] = {}
PNL_SUMMARY: Dict[str, Any] = {
    "pnl": 0.0,
    "usd": 0.0,
    "wins": 0,
    "losses": 0,
    "count": 0,
    "updated_at": None,
}


def _append_orders(orders: List[Dict[str, Any]]) -> None:
    """Append orders to in-memory store and update attribution & pnl summary."""
    global ORDERS_STORE, ATTR_STORE, PNL_SUMMARY
    if not isinstance(orders, list):
        return
    now = datetime.now(timezone.utc).isoformat()
    for o in orders:
        if isinstance(o, dict):
            o.setdefault("ts", now)
            ORDERS_STORE.append(o)
            strat = o.get("strategy", "unknown")
            pnl = float(o.get("pnl", 0.0))
            ATTR_STORE.setdefault(strat, {"count": 0, "pnl": 0.0})
            ATTR_STORE[strat]["count"] = int(ATTR_STORE[strat]["count"]) + 1
            ATTR_STORE[strat]["pnl"] = float(ATTR_STORE[strat]["pnl"]) + pnl

            # update pnl summary
            PNL_SUMMARY["pnl"] = float(PNL_SUMMARY.get("pnl", 0.0)) + pnl
            PNL_SUMMARY["usd"] = PNL_SUMMARY["pnl"]  # alias
            PNL_SUMMARY["count"] = int(PNL_SUMMARY.get("count", 0)) + 1
            if pnl >= 0:
                PNL_SUMMARY["wins"] = int(PNL_SUMMARY.get("wins", 0)) + 1
            else:
                PNL_SUMMARY["losses"] = int(PNL_SUMMARY.get("losses", 0)) + 1

    PNL_SUMMARY["updated_at"] = now


# ---------------------- Scan Bridge ----------------------
async def _maybe_await(x):
    if inspect.isasyncgen(x):
        return [item async for item in x]  # type: ignore[misc]
    if asyncio.iscoroutine(x):
        return await x
    if inspect.isgenerator(x):
        return list(x)  # type: ignore[misc]
    return x


def _normalize_orders(res: Any) -> List[Dict[str, Any]]:
    """Coerce different return styles into List[Dict]."""
    if res is None:
        return []
    if isinstance(res, tuple) and res:
        res = res[0]  # (orders, meta)
    if isinstance(res, dict):
        if "orders" in res and isinstance(res["orders"], list):
            res = res["orders"]
        else:
            return [res]  # single-order dict
    if isinstance(res, list):
        return [x for x in res if isinstance(x, dict)]
    return []


def _build_req_and_contexts(base_req: Dict[str, Any], dry: int):
    """Create a permissive 'req' and a few flavors of 'contexts' to satisfy different StrategyBooks."""
    # permissive req (no exotic keys)
    tf = base_req.get("timeframe", DEFAULT_TF)
    lim = int(base_req.get("limit", DEFAULT_LIMIT))
    notional = float(base_req.get("notional", DEFAULT_NOTIONAL))
    symbols = base_req.get("symbols", DEFAULT_SYMBOLS)

    # A compact 'req' that many codebases expect
    req_compact = {
        "timeframe": tf,
        "limit": lim,
        "notional": notional,
        "symbols": symbols,
        "dry": dry,
    }

    # Some implementations index contexts['one'] (your logs show KeyError: 'one')
    ctx = {
        "timeframe": tf,
        "limit": lim,
        "notional": notional,
        "symbols": symbols,
        "dry": dry,
    }

    # Variants
    ctx_map_one = {"one": dict(ctx)}
    ctx_map_default = {"default": dict(ctx)}
    ctx_map_both = {"one": dict(ctx), "default": dict(ctx)}
    ctx_plain = dict(ctx)  # in case they expect a non-nested dict

    return req_compact, {
        "one": ctx_map_one,
        "default": ctx_map_default,
        "both": ctx_map_both,
        "plain": ctx_plain,
    }


async def _scan_bridge(
    strat: str,
    req: Dict[str, Any],
    dry: int = 0,
) -> List[Dict[str, Any]]:
    """
    Universal adapter to whatever StrategyBook.scan expects.
    Tries multiple signatures & context shapes. Never raises; returns [] on failure.
    """
    if StrategyBook is None:
        log.warning("StrategyBook missing; skipping scan for %s", strat)
        return []

    req_compact, ctxs = _build_req_and_contexts(req, dry)

    # Collect callables (instance first, then class)
    callables: List[Tuple[str, Any]] = []
    try:
        inst = StrategyBook()  # type: ignore[call-arg]
        if hasattr(inst, "scan"):
            callables.append(("inst_scan", getattr(inst, "scan")))
    except Exception as e:
        log.debug("No instance StrategyBook(): %s", e)
    try:
        callables.append(("class_scan", getattr(StrategyBook, "scan")))
    except Exception as e:
        log.debug("No class StrategyBook.scan: %s", e)

    attempts: List[Tuple[str, Tuple[Any, tuple, dict]]] = []

    # Based on your logs, the most promising shapes are:
    #   scan(strategy, req, contexts)
    #   scan(req, contexts)
    #   scan(strategy, contexts)
    # And 'contexts' wants a map with key 'one'
    for tag, fn in callables:
        # Preferred: both keys available
        attempts.extend([
            (f"{tag}(strategy, req, ctx_both)", (fn, (strat, req_compact, ctxs["both"]), {})),
            (f"{tag}(strategy, req, ctx_one)", (fn, (strat, req_compact, ctxs["one"]), {})),
            (f"{tag}(req, ctx_both)", (fn, (req_compact, ctxs["both"]), {})),
            (f"{tag}(req, ctx_one)", (fn, (req_compact, ctxs["one"]), {})),
            (f"{tag}(strategy, ctx_both)", (fn, (strat, ctxs["both"]), {})),
            (f"{tag}(strategy, ctx_one)", (fn, (strat, ctxs["one"]), {})),
            # Fallbacks for code that uses a plain dict for contexts, or wants default-only:
            (f"{tag}(strategy, req, ctx_default)", (fn, (strat, req_compact, ctxs["default"]), {})),
            (f"{tag}(req, ctx_default)", (fn, (req_compact, ctxs["default"]), {})),
            (f"{tag}(strategy, ctx_default)", (fn, (strat, ctxs["default"]), {})),
            (f"{tag}(strategy, req, ctx_plain)", (fn, (strat, req_compact, ctxs["plain"]), {})),
            (f"{tag}(req, ctx_plain)", (fn, (req_compact, ctxs["plain"]), {})),
            (f"{tag}(strategy, ctx_plain)", (fn, (strat, ctxs["plain"]), {})),
            # Very old shapes:
            (f"{tag}(strategy)", (fn, (strat,), {})),
            (f"{tag}(req)", (fn, (req_compact,), {})),
            (f"{tag}()", (fn, (), {})),
        ])

    last_error: Optional[Exception] = None

    for label, (fn, args, kwargs) in attempts:
        try:
            # Attempt to call; do not try kwargs with 'timeframe' etc (your logs show kwarg rejection)
            raw = fn(*args, **kwargs)  # type: ignore[misc]
            res = await _maybe_await(raw)
            orders = _normalize_orders(res)
            if isinstance(orders, list):
                return orders
        except Exception as e:
            last_error = e
            msg = str(e)
            if len(msg) > 180:
                msg = msg[:180] + "…"
            log.warning("scan attempt failed (%s): %s", label, msg)

    log.error(
        "All scan attempts failed for strategy '%s'. Returning empty list. Last error: %s",
        strat,
        last_error,
    )
    return []


async def _run_one_scan(strat: str, req: Dict[str, Any], dry: int) -> List[Dict[str, Any]]:
    orders = await _scan_bridge(strat, req, dry=dry)
    return orders if isinstance(orders, list) else []


async def _scheduler_loop():
    """
    Periodically scans all strategies and appends orders to in-memory store.
    Safe to run as a background task.
    """
    req: Dict[str, Any] = {}
    dry_val = int(os.environ.get("SCHEDULER_DRY", str(DEFAULT_DRY)))

    log.info("Scheduler tick: running all strategies (dry=%d)", dry_val)
    for strat in STRATEGIES:
        try:
            orders = await _scan_bridge(strat, req, dry=dry_val)
        except Exception as e:
            log.error("StrategyBook.scan unexpected error: %s", e, exc_info=True)
            orders = []
        if isinstance(orders, list) and dry_val == 0:
            _append_orders(orders)


# ---------------------- FastAPI ----------------------
app = FastAPI(title=APP_NAME, version=APP_VERSION)


@app.on_event("startup")
async def _on_startup():
    log.info("Application startup complete.")
    if StrategyBook is None:
        log.warning("No StrategyBook available, skipping scheduler.")
        return

    async def runner():
        await asyncio.sleep(2)
        while True:
            try:
                await _scheduler_loop()
            except Exception as e:
                log.error("Scheduler loop error: %s", e, exc_info=True)
            await asyncio.sleep(SCHEDULER_INTERVAL_SEC)

    asyncio.create_task(runner())


# ---------------------- Models ----------------------
class Order(BaseModel):
    id: str
    strategy: str
    symbol: str
    side: str  # BUY/SELL
    qty: float
    px: float
    pnl: float = 0.0
    ts: Optional[str] = None


# ---------------------- Routes ----------------------
@app.head("/", include_in_schema=False)
async def head_root():
    # Health checks (HEAD /). Return 200 OK.
    return Response(status_code=200)


@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/dashboard", status_code=307)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    # Inline HTML dashboard (complete)
    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{APP_NAME} — Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root {{
  --bg: #0b0f14;
  --card: #121821;
  --text: #e6eef9;
  --muted: #a7b3c6;
  --accent: #3aa0ff;
  --good: #2ecc71;
  --bad: #ff5c5c;
  --chip: #1a2230;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0;
  padding: 0;
  background: var(--bg);
  color: var(--text);
  font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
}}
.header {{
  padding: 20px 24px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  position: sticky;
  top: 0;
  background: linear-gradient(to bottom, rgba(11,15,20,.95), rgba(11,15,20,.85));
  backdrop-filter: blur(6px);
  border-bottom: 1px solid #1b2330;
  z-index: 10;
}}
.title {{
  display: flex;
  gap: 12px;
  align-items: center;
  font-weight: 700;
  letter-spacing: .2px;
}}
.badge {{
  background: var(--chip);
  padding: 4px 10px;
  border-radius: 999px;
  font-size: 12px;
  color: var(--muted);
  border: 1px solid #223046;
}}
.grid {{
  display: grid;
  grid-template-columns: 1.2fr .8fr;
  gap: 16px;
  padding: 16px;
}}
.card {{
  background: var(--card);
  border: 1px solid #1b2330;
  border-radius: 14px;
  overflow: hidden;
}}
.card .head {{
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 16px;
  border-bottom: 1px solid #1b2330;
}}
.card .body {{
  padding: 12px 16px 16px 16px;
}}
.kpis {{
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 10px;
}}
.kpi {{
  background: #0f1520;
  border: 1px solid #1b2330;
  border-radius: 10px;
  padding: 12px;
}}
.kpi .label {{ color: var(--muted); font-size: 12px; }}
.kpi .value {{ margin-top: 6px; font-size: 20px; font-weight: 700; }}
.kpi .delta {{ margin-top: 4px; font-size: 12px; }}
.delta.pos {{ color: var(--good); }}
.delta.neg {{ color: var(--bad); }}
.table {{
  width: 100%;
  border-collapse: collapse;
  font-size: 14px;
}}
.table th, .table td {{
  padding: 10px 8px;
  border-bottom: 1px solid #1b2330;
  text-align: left;
  white-space: nowrap;
}}
.table th {{ color: var(--muted); font-weight: 600; font-size: 12px; }}
.chips {{ display: flex; gap: 8px; flex-wrap: wrap; }}
.chip {{
  background: var(--chip);
  border: 1px solid #223046;
  color: var(--muted);
  padding: 6px 10px;
  border-radius: 999px;
  font-size: 12px;
}}
.footer {{
  color: var(--muted);
  font-size: 12px;
  padding: 24px 16px 40px;
  text-align: center;
}}
.code {{
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
  color: #c8d8f0;
  background: #0f1520;
  padding: 10px 12px;
  border: 1px solid #1b2330;
  border-radius: 8px;
}}
.btn {{
  appearance: none;
  background: transparent;
  color: var(--accent);
  border: 1px solid #294466;
  padding: 8px 12px;
  border-radius: 10px;
  cursor: pointer;
}}
.btn:hover {{ background: #0f1520; }}
@media (max-width: 980px) {{
  .grid {{ grid-template-columns: 1fr; }}
  .kpis {{ grid-template-columns: repeat(2, 1fr); }}
}}
</style>
</head>
<body>
  <div class="header">
    <div class="title">
      <svg width="22" height="22" viewBox="0 0 24 24" fill="none"><path d="M3 12h6l3 7 3-14 3 7h3" stroke="#3aa0ff" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
      <div>{APP_NAME}</div>
      <span class="badge">v{APP_VERSION}</span>
    </div>
    <div class="chips">
      <span class="chip" id="scheduler-status">scheduler: checking…</span>
      <button class="btn" onclick="refreshAll()">Refresh</button>
    </div>
  </div>

  <div class="grid">
    <div class="card">
      <div class="head">
        <div>Performance</div>
        <div class="chips">
          <span class="chip">1m</span>
          <span class="chip">15m</span>
          <span class="chip">1h</span>
          <span class="chip">4h</span>
        </div>
      </div>
      <div class="body">
        <div class="kpis">
          <div class="kpi">
            <div class="label">Total PnL (USD)</div>
            <div class="value" id="kpi-pnl">–</div>
            <div class="delta" id="kpi-pnl-delta">updated: –</div>
          </div>
          <div class="kpi">
            <div class="label">Trades</div>
            <div class="value" id="kpi-count">–</div>
            <div class="delta"><span id="kpi-wins">–</span> wins • <span id="kpi-losses">–</span> losses</div>
          </div>
          <div class="kpi">
            <div class="label">Win Rate</div>
            <div class="value" id="kpi-winrate">–</div>
            <div class="delta">all time</div>
          </div>
          <div class="kpi">
            <div class="label">Strategies Active</div>
            <div class="value" id="kpi-strats">–</div>
            <div class="delta" id="kpi-strats-note">loaded from StrategyBook</div>
          </div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="head">
        <div>Attribution</div>
        <div class="chips" id="attr-chips"></div>
      </div>
      <div class="body">
        <table class="table" id="attr-table">
          <thead>
            <tr>
              <th>Strategy</th>
              <th>Trades</th>
              <th>PnL</th>
            </tr>
          </thead>
          <tbody id="attr-tbody"></tbody>
        </table>
      </div>
    </div>

    <div class="card" style="grid-column: 1 / -1;">
      <div class="head">
        <div>Recent Orders</div>
        <div class="chips">
          <span class="chip">/orders/recent?limit=50</span>
        </div>
      </div>
      <div class="body">
        <table class="table" id="orders-table">
          <thead>
            <tr>
              <th>Time</th>
              <th>ID</th>
              <th>Strategy</th>
              <th>Symbol</th>
              <th>Side</th>
              <th>Qty</th>
              <th>Price</th>
              <th>PnL</th>
            </tr>
          </thead>
          <tbody id="orders-tbody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="footer">
    <div>Endpoints:
      <code class="code">GET /pnl/summary</code>,
      <code class="code">GET /orders/recent?limit=50</code>,
      <code class="code">GET /orders/attribution</code>
    </div>
  </div>

<script>
async function jsonGet(url) {{
  const res = await fetch(url, {{ headers: {{ "accept": "application/json" }} }});
  if (!res.ok) throw new Error("HTTP " + res.status);
  return res.json();
}}

function fmt(n, decimals=2) {{
  if (n === null || n === undefined || isNaN(n)) return "–";
  return Number(n).toLocaleString(undefined, {{ minimumFractionDigits: decimals, maximumFractionDigits: decimals }});
}}

async function loadPnl() {{
  try {{
    const d = await jsonGet("/pnl/summary");
    document.getElementById("kpi-pnl").textContent = "$" + fmt(d.usd || d.pnl);
    document.getElementById("kpi-count").textContent = fmt(d.count, 0);
    document.getElementById("kpi-wins").textContent = fmt(d.wins, 0);
    document.getElementById("kpi-losses").textContent = fmt(d.losses, 0);
    const total = (d.wins || 0) + (d.losses || 0);
    const wr = total > 0 ? (100 * (d.wins || 0) / total) : 0;
    document.getElementById("kpi-winrate").textContent = fmt(wr, 1) + "%";
    document.getElementById("kpi-pnl-delta").textContent = "updated: " + (d.updated_at || "–");
  }} catch (e) {{
    console.error(e);
  }}
}}

async function loadAttr() {{
  try {{
    const d = await jsonGet("/orders/attribution");
    const tbody = document.getElementById("attr-tbody");
    tbody.innerHTML = "";
    const chips = document.getElementById("attr-chips");
    chips.innerHTML = "";
    const keys = Object.keys(d).sort();
    document.getElementById("kpi-strats").textContent = keys.length;
    for (const k of keys) {{
      const row = d[k];
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${{k}}</td>
        <td>${{row.count || 0}}</td>
        <td>${{Number(row.pnl || 0).toLocaleString(undefined, {{ minimumFractionDigits: 2, maximumFractionDigits: 2 }})}}</td>
      `;
      tbody.appendChild(tr);

      const chip = document.createElement("span");
      chip.className = "chip";
      chip.textContent = `${{k}} • ${{fmt(row.pnl || 0)}}`;
      chips.appendChild(chip);
    }}
    document.getElementById("kpi-strats-note").textContent = "attribution counters";
  }} catch (e) {{
    console.error(e);
  }}
}}

async function loadOrders() {{
  try {{
    const d = await jsonGet("/orders/recent?limit=50");
    const tbody = document.getElementById("orders-tbody");
    tbody.innerHTML = "";
    for (const o of d) {{
      const pnl = Number(o.pnl || 0);
      const cls = pnl >= 0 ? "pos" : "neg";
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td>${{o.ts || ""}}</td>
        <td>${{o.id || ""}}</td>
        <td>${{o.strategy || ""}}</td>
        <td>${{o.symbol || ""}}</td>
        <td>${{o.side || ""}}</td>
        <td>${{fmt(o.qty)}}</td>
        <td>${{fmt(o.px)}}</td>
        <td class="${{cls}}">${{fmt(pnl)}}</td>
      `;
      tbody.appendChild(tr);
    }}
  }} catch (e) {{
    console.error(e);
  }}
}}

async function loadSchedulerStatus() {{
  try {{
    const d = await jsonGet("/status");
    const el = document.getElementById("scheduler-status");
    if (d.strategy_book_loaded) {{
      el.textContent = "scheduler: active";
      el.style.color = "#2ecc71";
    }} else {{
      el.textContent = "scheduler: idle (no StrategyBook)";
      el.style.color = "#ffcc66";
    }}
  }} catch (e) {{
    console.error(e);
  }}
}}

async function refreshAll() {{
  await Promise.all([loadPnl(), loadAttr(), loadOrders(), loadSchedulerStatus()]);
}}

window.addEventListener("load", refreshAll);
</script>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=200)


@app.get("/status")
async def status():
    return {
        "ok": True,
        "app": APP_NAME,
        "version": APP_VERSION,
        "strategy_book_loaded": StrategyBook is not None,
        "scheduler_interval_sec": SCHEDULER_INTERVAL_SEC,
        "strategies": list(STRATEGIES),
    }


@app.get("/pnl/summary")
async def pnl_summary():
    return PNL_SUMMARY


@app.get("/orders/recent")
async def orders_recent(limit: int = Query(50, ge=1, le=500)):
    if not ORDERS_STORE:
        return []
    return ORDERS_STORE[-limit:][::-1]


@app.get("/orders/attribution")
async def orders_attribution():
    return ATTR_STORE


@app.post("/scan/{strategy}")
async def scan_one(strategy: str, req: Dict[str, Any] = {}):
    dry_val = int(req.get("dry", DEFAULT_DRY))
    try:
        orders = await _run_one_scan(strategy, req, dry=dry_val)
    except Exception as e:
        log.error("StrategyBook.scan error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    if isinstance(orders, list) and dry_val == 0:
        _append_orders(orders)
    return {"ok": True, "strategy": strategy, "request": req, "dry": dry_val, "orders": orders}


@app.post("/scan-all")
async def scan_all(req: Dict[str, Any] = {}):
    dry_val = int(req.get("dry", DEFAULT_DRY))
    results = []
    for strat in STRATEGIES:
        try:
            orders = await _scan_bridge(strat, req, dry=dry_val)
        except Exception as e:
            log.error("StrategyBook.scan error: %s", e, exc_info=True)
            orders = []
        if isinstance(orders, list) and dry_val == 0:
            _append_orders(orders)
        results.append({"strategy": strat, "count": len(orders) if isinstance(orders, list) else 0, "orders": orders})
    return {"ok": True, "request": req, "dry": dry_val, "results": results}


# ---------------------- Dev server ----------------------
def _infer_host_port() -> Tuple[str, int]:
    # Render uses PORT env var; default to 10000 for local parity with your logs
    host = os.environ.get("HOST", "0.0.0.0")
    try:
        port = int(os.environ.get("PORT", "10000"))
    except Exception:
        port = 10000
    return host, port


if __name__ == "__main__":
    host, port = _infer_host_port()
    log.info("Starting %s v%s on %s:%d", APP_NAME, APP_VERSION, host, port)
    try:
        import uvicorn  # type: ignore
        uvicorn.run(
            "app:app",
            host=host,
            port=port,
            reload=bool(int(os.environ.get("RELOAD", "0"))),
            lifespan="on",
        )
    except Exception as e:
        log.exception("Failed to start Uvicorn: %s", e)
        sys.exit(1)