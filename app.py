# app.py
# FastAPI control plane for crypto system
# Version bumped
APP_VERSION = "1.2.0"

import asyncio
import inspect
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel

# ---------------------- Logging ----------------------
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

# ---------------------- StrategyBook import ----------------------
# Expect a module providing StrategyBook with a .scan method.
# We don't implement strategies here; we *adapt* to whatever signature is present.
StrategyBook = None  # type: ignore[assignment]
try:
    # You should have a package/module on your image that exposes StrategyBook
    from strategies import StrategyBook as ImportedStrategyBook  # type: ignore
    StrategyBook = ImportedStrategyBook
except Exception as e:  # pragma: no cover
    log.warning("No external StrategyBook import detected: %s", e)

# ---------------------- Defaults ----------------------
DEFAULT_TF = os.environ.get("DEFAULT_TIMEFRAME", "1h")
DEFAULT_LIMIT = int(os.environ.get("DEFAULT_LIMIT", "300"))
DEFAULT_NOTIONAL = float(os.environ.get("DEFAULT_NOTIONAL", "1000"))
DEFAULT_SYMBOLS = os.environ.get("DEFAULT_SYMBOLS", "BTCUSDT,ETHUSDT")

SCHEDULER_INTERVAL_SEC = int(os.environ.get("SCHEDULER_INTERVAL_SEC", "60"))
SCAN_STRATEGIES: Tuple[str, ...] = tuple(
    (os.environ.get("SCAN_STRATEGIES") or "c1,c2,c3,c4,c5,c6").split(",")
)

# ---------------------- In-memory stores ----------------------
_ORDERS: List[Dict[str, Any]] = []  # append-only order log
_PNL_SUMMARY: Dict[str, Any] = {"total_pnl": 0.0, "by_symbol": {}, "asof": None}

# ---------------------- Helpers ----------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def _maybe_await(x):
    if inspect.isawaitable(x):
        return await x
    return x

def _coerce_symbols(symbols: Union[str, List[str], Tuple[str, ...], None]) -> List[str]:
    if symbols is None:
        symbols = DEFAULT_SYMBOLS
    if isinstance(symbols, (list, tuple)):
        return [s.strip() for s in symbols if s and str(s).strip()]
    # string path
    return [s.strip() for s in str(symbols).split(",") if s.strip()]

def _normalize_req(req: Dict[str, Any]) -> Dict[str, Any]:
    # Never mutate the original dict the caller hands us.
    r = dict(req or {})
    r["timeframe"] = str(r.get("timeframe", DEFAULT_TF))
    try:
        r["limit"] = int(r.get("limit", DEFAULT_LIMIT))
    except Exception:
        r["limit"] = DEFAULT_LIMIT
    try:
        r["notional"] = float(r.get("notional", DEFAULT_NOTIONAL))
    except Exception:
        r["notional"] = DEFAULT_NOTIONAL
    r["symbols"] = _coerce_symbols(r.get("symbols", DEFAULT_SYMBOLS))
    return r

def _build_context_map(req: Dict[str, Any], dry: int) -> Dict[str, Dict[str, Any]]:
    r = _normalize_req(req)
    ctx = {
        "timeframe": r["timeframe"],
        "limit": r["limit"],
        "notional": r["notional"],
        "symbols": r["symbols"],
        "dry": int(dry),
    }
    return {"default": ctx}

# ---------------------- Scan Bridge ----------------------
async def _scan_bridge(
    strat: str,
    req: Dict[str, Any],
    dry: int = 0,
) -> List[Dict[str, Any]]:
    """
    Call StrategyBook.scan with best-effort signature adaptation.

    We support these common signatures (sync or async):
      - scan(strategy: str, contexts: dict[str, dict]) -> list
      - scan(*, strategy: str, contexts: dict[str, dict]) -> list
      - scan(strategy: str, ctx: dict) -> list
      - scan(*, strategy: str, **ctx) -> list
      - scan(contexts: dict[str, dict]) -> list
      - scan(ctx: dict) -> list
      - scan(**ctx) -> list

    Returns a list of order dicts (possibly empty).
    Raises a TypeError if no variant matched.
    """
    if StrategyBook is None:
        raise TypeError("StrategyBook is not available on this deployment")

    # Build normalized contexts
    contexts = _build_context_map(req, dry)
    single_ctx = contexts["default"]

    # Obtain scan function (staticmethod/classmethod/instance ok)
    scan_fn = getattr(StrategyBook, "scan", None)
    if scan_fn is None:
        raise TypeError("StrategyBook.scan is missing")

    # Introspect to help choose safer variants
    try:
        sig = inspect.signature(scan_fn)
        params = sig.parameters
    except Exception:
        sig = None
        params = {}

    # Try variants in a deterministic order; keep last error for transparency
    last_error: Optional[Exception] = None
    variant_idx = 0

    async def _try(call, *args, **kwargs):
        nonlocal last_error, variant_idx
        try:
            variant_idx += 1
            res = await _maybe_await(call(*args, **kwargs))
            # Ensure list of dicts is returned
            if res is None:
                return []
            if isinstance(res, dict):
                # Some implementations return {"orders": [...]}
                if "orders" in res and isinstance(res["orders"], list):
                    return list(res["orders"])
                # or a single order dict
                return [res]
            if isinstance(res, list):
                return res
            # Anything else: coerce to empty list rather than explode
            return []
        except Exception as e:
            last_error = e
            log.warning("scan variant #%d failed: %s", variant_idx, e)
            return None

    # Preferred explicit kwargs if present in signature
    if "strategy" in params and "contexts" in params:
        out = await _try(scan_fn, strategy=strat, contexts=contexts)
        if out is not None:
            return out

    # Positional (strategy, contexts)
    out = await _try(scan_fn, strat, contexts)
    if out is not None:
        return out

    # Keyword (strategy, ctx)
    if "strategy" in params and "ctx" in params:
        out = await _try(scan_fn, strategy=strat, ctx=single_ctx)
        if out is not None:
            return out

    # Positional (strategy, ctx)
    out = await _try(scan_fn, strat, single_ctx)
    if out is not None:
        return out

    # Keyword expansion: scan(strategy=..., **ctx)
    out = await _try(scan_fn, strategy=strat, **single_ctx)
    if out is not None:
        return out

    # contexts-only
    if "contexts" in params:
        out = await _try(scan_fn, contexts=contexts)
        if out is not None:
            return out

    # ctx-only
    if "ctx" in params:
        out = await _try(scan_fn, ctx=single_ctx)
        if out is not None:
            return out

    # pure kwargs (**ctx)
    out = await _try(scan_fn, **single_ctx)
    if out is not None:
        return out

    raise TypeError(
        f"No compatible StrategyBook.scan signature (strategy, contexts). Last error: {last_error}"
    )

# ---------------------- Order utils ----------------------
def _append_orders(orders: List[Dict[str, Any]]) -> None:
    global _ORDERS, _PNL_SUMMARY
    ts = _now_iso()
    for o in (orders or []):
        if isinstance(o, dict):
            o.setdefault("ts", ts)
            _ORDERS.append(o)
            # Very naive PnL update if present
            sym = o.get("symbol") or o.get("asset") or "UNKNOWN"
            pnl = float(o.get("pnl", 0.0))
            _PNL_SUMMARY["total_pnl"] = float(_PNL_SUMMARY.get("total_pnl", 0.0)) + pnl
            _PNL_SUMMARY.setdefault("by_symbol", {})
            _PNL_SUMMARY["by_symbol"][sym] = float(
                _PNL_SUMMARY["by_symbol"].get(sym, 0.0) + pnl
            )
    _PNL_SUMMARY["asof"] = ts

# ---------------------- FastAPI ----------------------
app = FastAPI(title="Crypto System Control Plane", version=APP_VERSION)

# ---------------------- Scheduler ----------------------
async def _run_one_scan(strat: str, req: Dict[str, Any], dry: int) -> List[Dict[str, Any]]:
    try:
        orders = await _scan_bridge(strat, req, dry=dry)
        return orders or []
    except Exception as e:
        log.error("StrategyBook.scan error: %s", e, exc_info=True)
        return []

async def _scheduler_loop():
    # Default request baseline; StrategyBook can override via implementation
    base_req = {
        "timeframe": DEFAULT_TF,
        "limit": DEFAULT_LIMIT,
        "notional": DEFAULT_NOTIONAL,
        "symbols": _coerce_symbols(DEFAULT_SYMBOLS),
    }
    log.info("Scheduler tick: running all strategies (dry=0)")
    tasks = [asyncio.create_task(_run_one_scan(s, base_req, dry=0)) for s in SCAN_STRATEGIES]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    total_added = 0
    for res in results:
        if isinstance(res, list):
            _append_orders(res)
            total_added += len(res)
    if total_added:
        log.info("Scheduler appended %d orders", total_added)

@app.on_event("startup")
async def _on_startup():
    log.info("Application startup complete.")
    # Fire-and-forget interval scheduler
    async def runner():
        # small initial delay to let app settle
        await asyncio.sleep(2)
        while True:
            try:
                await _scheduler_loop()
            except Exception as e:
                log.error("Scheduler loop error: %s", e, exc_info=True)
            await asyncio.sleep(SCHEDULER_INTERVAL_SEC)
    asyncio.create_task(runner())

# ---------------------- Models ----------------------
class ScanRequest(BaseModel):
    timeframe: Optional[str] = None
    limit: Optional[int] = None
    notional: Optional[float] = None
    symbols: Optional[Union[str, List[str]]] = None
    dry: Optional[int] = 0

# ---------------------- HTML (Dashboard) ----------------------
_DASHBOARD_HTML = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Crypto Control Plane · v{APP_VERSION}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root {{
      --bg: #0b0e14;
      --card: #111623;
      --text: #e6e6e6;
      --muted: #9aa4b2;
      --accent: #7dd3fc;
      --good: #22c55e;
      --bad: #ef4444;
      --warn: #f59e0b;
      --border: #1f2937;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; background: var(--bg); color: var(--text); font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
    }}
    header {{
      padding: 18px 24px; border-bottom: 1px solid var(--border); background: #0c1220;
      display:flex; align-items:center; gap:12px;
      position: sticky; top:0;
    }}
    h1 {{ margin: 0; font-size: 18px; letter-spacing: .3px; }}
    .tag {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas; color: var(--muted); }}
    main {{ padding: 20px; display: grid; gap: 16px; grid-template-columns: 1fr; max-width: 1200px; margin: 0 auto; }}
    .grid {{ display:grid; gap:16px; grid-template-columns: repeat(12, 1fr); }}
    .card {{
      background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 16px;
      box-shadow: 0 10px 24px rgba(0,0,0,.35);
    }}
    .span-4 {{ grid-column: span 4; }}
    .span-8 {{ grid-column: span 8; }}
    .row {{ display:flex; align-items:center; justify-content: space-between; gap:12px; }}
    .kpi {{ font-size: 28px; font-weight: 700; }}
    .muted {{ color: var(--muted); font-size: 13px; }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ padding: 10px 8px; border-bottom: 1px solid var(--border); font-size: 14px; text-align: left; }}
    th {{ color: var(--muted); font-weight: 600; }}
    .pill {{ display:inline-block; padding: 4px 8px; border-radius: 999px; font-size: 12px; border:1px solid var(--border); background:#0e172a; color:var(--text); }}
    .good {{ color: var(--good); }} .bad {{ color: var(--bad); }} .warn {{ color: var(--warn); }}
    button, select, input {{
      background:#0d1323; color:var(--text); border:1px solid var(--border); border-radius: 10px; padding:8px 10px;
      font: inherit;
    }}
    button:hover {{ border-color:#334155; cursor:pointer; }}
    .controls {{ display:flex; gap:8px; flex-wrap: wrap; }}
    @media (max-width: 900px) {{
      .span-4, .span-8 {{ grid-column: span 12; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>Crypto System Control Plane <span class="tag">v{APP_VERSION}</span></h1>
  </header>
  <main>
    <div class="grid">
      <section class="card span-4">
        <div class="row">
          <div>
            <div class="muted">Total PnL</div>
            <div id="kpi-pnl" class="kpi">—</div>
          </div>
          <div class="muted" id="kpi-asof">—</div>
        </div>
        <div class="muted" style="margin-top:8px" id="kpi-breakdown">—</div>
      </section>

      <section class="card span-8">
        <div class="row" style="margin-bottom:8px">
          <div class="muted">Recent Orders</div>
          <div class="controls">
            <select id="limit">
              <option>25</option>
              <option selected>50</option>
              <option>100</option>
            </select>
            <button id="refresh">Refresh</button>
            <button id="scanNow">Run Scan</button>
          </div>
        </div>
        <table id="orders">
          <thead>
            <tr>
              <th>Time</th><th>Strategy</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Px</th><th>PnL</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </section>
    </div>
  </main>
  <script>
    async function jget(url) {{
      const r = await fetch(url, {{ headers: {{ "Accept": "application/json" }} }});
      if (!r.ok) throw new Error("HTTP " + r.status);
      return await r.json();
    }}

    async function loadPnL() {{
      const d = await jget("/pnl/summary");
      const pnl = (d.total_pnl ?? 0).toFixed(2);
      const asof = d.asof || "—";
      document.getElementById("kpi-pnl").textContent = (pnl >= 0 ? "+" : "") + pnl;
      document.getElementById("kpi-pnl").className = "kpi " + (pnl >= 0 ? "good" : "bad");
      document.getElementById("kpi-asof").textContent = asof;
      const parts = [];
      if (d.by_symbol) {{
        for (const [k, v] of Object.entries(d.by_symbol)) {{
          parts.push(k + ": " + Number(v).toFixed(2));
        }}
      }}
      document.getElementById("kpi-breakdown").textContent = parts.length ? parts.join("  ·  ") : "—";
    }}

    function td(text) {{
      const el = document.createElement("td");
      el.textContent = text;
      return el;
    }}

    function fmt(x) {{
      if (x === null || x === undefined) return "—";
      if (typeof x === "number") return x.toString();
      return String(x);
    }}

    async function loadOrders() {{
      const lim = document.getElementById("limit").value;
      const d = await jget("/orders/recent?limit=" + encodeURIComponent(lim));
      const tb = document.querySelector("#orders tbody");
      tb.innerHTML = "";
      for (const o of d.orders || []) {{
        const tr = document.createElement("tr");
        tr.appendChild(td(o.ts || "—"));
        tr.appendChild(td(o.strategy || o.strat || "—"));
        tr.appendChild(td(o.symbol || o.asset || "—"));
        tr.appendChild(td(o.side || "—"));
        tr.appendChild(td(o.qty !== undefined ? o.qty : "—"));
        tr.appendChild(td(o.price !== undefined ? o.price : "—"));
        const pnl = Number(o.pnl || 0);
        const t = td(pnl.toFixed(2));
        t.className = pnl >= 0 ? "good" : "bad";
        tr.appendChild(t);
        tb.appendChild(tr);
      }}
    }}

    async function runScan() {{
      try {{
        const body = {{ timeframe: "{DEFAULT_TF}", limit: {DEFAULT_LIMIT}, notional: {DEFAULT_NOTIONAL}, symbols: "{DEFAULT_SYMBOLS}" }};
        const r = await fetch("/scan/run", {{
          method: "POST",
          headers: {{ "Content-Type": "application/json" }},
          body: JSON.stringify(body)
        }});
        const d = await r.json();
        if (!r.ok) throw new Error(d.detail || ("HTTP " + r.status));
        await loadPnL();
        await loadOrders();
        alert("Scan complete");
      }} catch (e) {{
        alert("Scan failed: " + e);
      }}
    }}

    document.getElementById("refresh").addEventListener("click", () => {{ loadPnL(); loadOrders(); }});
    document.getElementById("scanNow").addEventListener("click", runScan);

    // initial
    loadPnL().then(loadOrders);
  </script>
</body>
</html>
"""

# ---------------------- Routes: HTML ----------------------
@app.get("/", response_class=RedirectResponse)
async def root():
    return RedirectResponse(url="/dashboard", status_code=307)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(content=_DASHBOARD_HTML)

# ---------------------- Routes: JSON APIs ----------------------
@app.get("/pnl/summary")
async def pnl_summary():
    return JSONResponse(_PNL_SUMMARY)

@app.get("/orders/recent")
async def orders_recent(limit: int = Query(50, ge=1, le=1000)):
    # return newest-first
    data = list(reversed(_ORDERS[-limit:]))
    return JSONResponse({"orders": data, "count": len(data)})

@app.get("/orders/attribution")
async def orders_attr():
    by_strat: Dict[str, float] = {}
    for o in _ORDERS:
        s = o.get("strategy") or o.get("strat") or "UNKNOWN"
        by_strat[s] = by_strat.get(s, 0.0) + float(o.get("pnl", 0.0))
    return JSONResponse({"by_strategy": by_strat, "asof": _now_iso()})

# Manual trigger for a scan run across all configured strategies
@app.post("/scan/run")
async def scan_run(req: ScanRequest):
    dry_val = int(req.dry or 0)
    # Build a dict from the pydantic model (only provided fields)
    inbound: Dict[str, Any] = {
        k: v for k, v in req.model_dump().items() if v is not None and k != "dry"
    }
    results: List[Dict[str, Any]] = []
    for strat in SCAN_STRATEGIES:
        try:
            orders = await _scan_bridge(strat, inbound, dry=dry_val)
        except Exception as e:
            log.error("StrategyBook.scan error: %s", e, exc_info=True)
            orders = []
        if isinstance(orders, list) and dry_val == 0:
            _append_orders(orders)
        results.append({"strategy": strat, "count": len(orders), "orders": orders})
    return {"ok": True, "request": inbound, "dry": dry_val, "results": results}

# Legacy single-strategy entrypoint (if you prefer /scan?strategy=c1)
@app.post("/scan")
async def scan(req: ScanRequest, strategy: str = Query(..., alias="strategy")):
    dry_val = int(req.dry or 0)
    inbound: Dict[str, Any] = {
        k: v for k, v in req.model_dump().items() if v is not None and k != "dry"
    }
    try:
        orders = await _scan_bridge(strategy, inbound, dry=dry_val)
    except Exception as e:
        log.error("StrategyBook.scan error: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    if isinstance(orders, list) and dry_val == 0:
        _append_orders(orders)
    return {"ok": True, "strategy": strategy, "request": inbound, "dry": dry_val, "orders": orders}
