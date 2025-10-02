# app.py  (v3.6.0)
# A single-file FastAPI app with:
# - Flexible StrategyBook.scan adapter
# - Internal scheduler (option B)
# - Dashboard HTML (P&L total, by strategy, calendar heatmap)
# - Compatibility endpoints: /scan/{c1..c6}, /scan/all, /orders/*, /pnl/summary, /v2/positions, /health, /dashboard
#
# Env vars you can set on Render:
#   AUTO_SCAN_ENABLED=1           -> enable internal scheduler (default: 0)
#   AUTO_SCAN_EVERY=60            -> seconds between automatic full scans (default: 60)
#   DEFAULT_TIMEFRAME=5Min
#   DEFAULT_LIMIT=360
#   DEFAULT_NOTIONAL=25
#   DEFAULT_SYMBOLS=BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD,ADA/USD,TON/USD,TRX/USD,APT/USD,ARB/USD,SUI/USD,OP/USD,MATIC/USD,NEAR/USD,ATOM/USD
#   PORT (Render provides automatically)
#
# Notes:
# - This app *does not* depend on httpx. Requests out to exchanges/brokers should be done in your StrategyBook code.
# - The StrategyBook adapter below tries multiple calling conventions and avoids passing 'dry' into typed models.

import os
import json
import time
import asyncio
import logging
import traceback
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional, Union

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

APP_VERSION = "3.6.0"

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

# -----------------------------------------------------------------------------
# Optional: import your StrategyBook implementation
# -----------------------------------------------------------------------------
_strategy_book = None
ScanRequest = None  # optional typed request model

def _try_import_strategy_book():
    global _strategy_book, ScanRequest
    candidates = [
        # ("module", "class_name", "scan_request_class_name")
        ("strategy_book", "StrategyBook", "ScanRequest"),
        ("strategies", "StrategyBook", "ScanRequest"),
        ("app_strategy", "StrategyBook", "ScanRequest"),
    ]
    for mod, cls, scan_req in candidates:
        try:
            m = __import__(mod, fromlist=[cls, scan_req])
            _strategy_book = getattr(m, cls, None)
            ScanRequest = getattr(m, scan_req, None)
            if callable(_strategy_book):
                # If it's a class, instantiate a singleton
                _strategy_book = _strategy_book()
            if _strategy_book is not None:
                log.info("Loaded StrategyBook from %s.%s", mod, cls)
                return
        except Exception:
            continue
    log.warning("StrategyBook not found; scans will return 'flat' unless your code provides it.")

_try_import_strategy_book()

# -----------------------------------------------------------------------------
# Defaults & helpers
# -----------------------------------------------------------------------------
DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "5Min")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "360"))
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", "25"))

DEFAULT_SYMBOLS_RAW = os.getenv(
    "DEFAULT_SYMBOLS",
    "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD,ADA/USD,TON/USD,TRX/USD,APT/USD,ARB/USD,SUI/USD,OP/USD,MATIC/USD,NEAR/USD,ATOM/USD",
)

def _parse_symbols(sym_param: Optional[str]) -> List[str]:
    raw = (sym_param or DEFAULT_SYMBOLS_RAW).strip()
    # Convert URL-encoded slashes back
    raw = raw.replace("%2F", "/")
    parts = [s.strip() for s in raw.split(",") if s.strip()]
    return parts

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x

# -----------------------------------------------------------------------------
# StrategyBook scan bridge (flexible signature adapter)
# -----------------------------------------------------------------------------
def _mk_scan_payload(
    timeframe: str,
    limit: int,
    symbols: List[str],
    notional: Optional[float] = None,
    topk: Optional[int] = None,
    min_score: Optional[float] = None,
) -> Dict[str, Any]:
    """Canonical dict for scanning (no framework-specific fields)."""
    return {
        "timeframe": str(timeframe),
        "limit": int(limit),
        "symbols": symbols,
        "notional": notional,
        "topk": topk,
        "min_score": min_score,
    }

def _variants_for_scan(strat: str, base: Dict[str, Any], dry: int) -> List[Dict[str, Any]]:
    """Produce multiple payload shapes commonly seen in adapters."""
    execute = bool(dry == 0)
    live = execute  # alias

    clean = {k: v for k, v in base.items() if v is not None}

    # kwargs with execute/live
    kw = dict(clean); kw.update({"execute": execute, "live": live})
    # kwargs with named strat
    kw_named = dict(kw); kw_named["strat"] = strat
    # dict + strategy
    dict_with_strategy = dict(kw); dict_with_strategy["strategy"] = strat
    # minimal variants
    kw_min = dict(clean)
    dict_with_strategy_min = dict(clean); dict_with_strategy_min["strategy"] = strat

    return [
        {"call": ("kwargs2",), "kwargs": {"strat": strat, **kw}},                 # scan(strat=?, timeframe=..., ...)
        {"call": ("args1_kwargs", strat), "kwargs": kw},                          # scan(strat, timeframe=..., ...)
        {"call": ("args1_dict1", strat), "args": (clean,)},                       # scan(strat, { ... })
        {"call": ("args1_dict1_json", strat), "args": (json.dumps(clean),)},      # scan(strat, "{...json...}")
        {"call": ("args0_dict_strategy",), "args": (dict_with_strategy,)},        # scan({strategy:..., ...})
        {"call": ("args0_json_strategy",), "args": (json.dumps(dict_with_strategy),)},  # scan("{...}")
        {"call": ("kwargs2_named",), "kwargs": kw_named},                         # scan(strat=..., timeframe=..., ...)
        {"call": ("kwargs_min",), "kwargs": {"strat": strat, **kw_min}},
        {"call": ("args0_dict_strategy_min",), "args": (dict_with_strategy_min,)},
    ]

async def _scan_bridge(strat: str, req_dict: Dict[str, Any], dry: int = 1) -> Any:
    """Try multiple StrategyBook.scan(...) signatures safely."""
    if _strategy_book is None:
        raise RuntimeError("StrategyBook not available")

    # 0) Try ScanRequest (if available), *without* passing 'dry'
    if ScanRequest is not None:
        try:
            sr_payload = {k: v for k, v in req_dict.items() if k in {"timeframe","limit","symbols","notional","topk","min_score"}}
            model = ScanRequest(**sr_payload)  # type: ignore
            return await _maybe_await(_strategy_book.scan(strat, model))  # type: ignore
        except TypeError as te:
            log.warning("scan(strat, ScanRequest) failed: %s", te)
        except Exception:
            log.exception("scan(strat, ScanRequest) crashed")

    variants = _variants_for_scan(strat, req_dict, dry)

    for v in variants:
        try:
            call = v["call"][0]
            if call in ("kwargs2", "kwargs2_named", "kwargs_min"):
                return await _maybe_await(_strategy_book.scan(**v["kwargs"]))  # type: ignore
            if call == "args1_kwargs":
                arg1 = v["call"][1]
                return await _maybe_await(_strategy_book.scan(arg1, **v["kwargs"]))  # type: ignore
            if call in ("args1_dict1", "args1_dict1_json"):
                arg1 = v["call"][1]
                return await _maybe_await(_strategy_book.scan(arg1, *v["args"]))  # type: ignore
            if call in ("args0_dict_strategy", "args0_json_strategy", "args0_dict_strategy_min"):
                return await _maybe_await(_strategy_book.scan(*v["args"]))  # type: ignore
        except TypeError as te:
            log.warning("scan variant %s failed: %s", v["call"], te)
        except Exception:
            log.exception("scan variant %s crashed", v["call"])

    raise TypeError("No compatible StrategyBook.scan signature (exhausted variants)")

# -----------------------------------------------------------------------------
# In-memory order log & PnL (for the dashboard)
# -----------------------------------------------------------------------------
# If your StrategyBook already persists orders elsewhere, you can adapt the
# /orders/recent handler to read from there. For now, we keep an in-memory list.
_order_log: List[Dict[str, Any]] = []
_positions: Dict[str, Any] = {}  # simple shape

def _append_orders_from_scan(strat: str, scan_result: Any):
    """
    Try to extract 'orders' from a scan result and push into the in-memory log.
    Expected order fields (best-effort): ts, strat, symbol, side, qty, price, pnl
    """
    try:
        if not scan_result:
            return
        if isinstance(scan_result, dict):
            orders = []
            if "orders" in scan_result and isinstance(scan_result["orders"], list):
                orders = scan_result["orders"]
            elif "result" in scan_result and isinstance(scan_result["result"], dict) and isinstance(scan_result["result"].get("orders"), list):
                orders = scan_result["result"]["orders"]
            for od in orders:
                od_copy = dict(od)
                od_copy.setdefault("ts", _utc_now_iso())
                od_copy.setdefault("strat", strat)
                od_copy.setdefault("pnl", 0.0)
                _order_log.append(od_copy)
    except Exception:
        log.exception("Failed to harvest orders from scan result for %s", strat)

def _pnl_summary() -> Dict[str, Any]:
    total = 0.0
    by_strat: Dict[str, float] = {}
    by_day: Dict[str, float] = {}
    for od in _order_log:
        pnl = float(od.get("pnl", 0.0))
        total += pnl
        s = str(od.get("strat", "unknown"))
        by_strat[s] = by_strat.get(s, 0.0) + pnl
        # Day key
        try:
            ts = od.get("ts")
            d = datetime.fromisoformat(ts.replace("Z", "+00:00")) if isinstance(ts, str) else datetime.now(timezone.utc)
        except Exception:
            d = datetime.now(timezone.utc)
        key = d.strftime("%Y-%m-%d")
        by_day[key] = by_day.get(key, 0.0) + pnl

    # Calendar range (last 90 days)
    today = datetime.now(timezone.utc).date()
    days = [(today.fromordinal(today.toordinal() - i)).isoformat() for i in range(0, 90)]
    days.reverse()
    cal = [{"date": d, "pnl": round(by_day.get(d, 0.0), 2)} for d in days]

    return {
        "version": APP_VERSION,
        "total_pnl": round(total, 2),
        "by_strategy": {k: round(v, 2) for k, v in by_strat.items()},
        "calendar": cal,
        "orders_count": len(_order_log),
    }

# -----------------------------------------------------------------------------
# FastAPI app & middleware
# -----------------------------------------------------------------------------
app = FastAPI(title="Crypto System", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

STRATEGY_LIST = ["c1", "c2", "c3", "c4", "c5", "c6"]

# -----------------------------------------------------------------------------
# HTML dashboard
# -----------------------------------------------------------------------------
_DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Crypto System Dashboard · __VERSION__</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root { --bg:#0b0e14; --panel:#121826; --muted:#8fa3bf; --text:#e4ecfa; --pos:#17c964; --neg:#f31260; --accent:#3b82f6; }
    html, body { margin:0; padding:0; background:var(--bg); color:var(--text); font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji"; }
    .wrap { max-width:1100px; margin:32px auto; padding:0 16px; }
    .title { font-size:22px; font-weight:700; opacity:.9; display:flex; gap:10px; align-items:center; }
    .grid { display:grid; grid-template-columns: repeat(12, 1fr); gap:16px; margin-top:16px; }
    .card { background:var(--panel); border-radius:14px; padding:16px; box-shadow: 0 4px 20px rgba(0,0,0,.25); }
    .card h3 { margin:0 0 8px 0; font-size:14px; font-weight:600; color:var(--muted); letter-spacing:.3px; }
    .big { font-size:28px; font-weight:800; }
    .pos { color:var(--pos); }
    .neg { color:var(--neg); }
    .row { display:flex; align-items:center; justify-content:space-between; gap:12px; }
    .muted { color:var(--muted); font-size:12px; }
    .pill { background:rgba(59,130,246,.15); color:#cfe1ff; padding:4px 8px; border-radius:999px; font-size:12px; }
    .list { display:flex; flex-direction:column; gap:10px; max-height:320px; overflow:auto; }
    .order { display:flex; justify-content:space-between; align-items:center; padding:10px 12px; background:rgba(255,255,255,.03); border-radius:10px; }
    .order .sym { font-weight:700; }
    .order .side { font-size:12px; color:var(--muted); }
    .order .pnl { font-weight:800; }
    .heat { display:grid; grid-template-columns: repeat(15, 1fr); gap:3px; }
    .cell { width:100%; aspect-ratio:1/1; border-radius:4px; background:#20283b; }
    .cell[data-v] { opacity:.95; }
    .legend { display:flex; gap:8px; align-items:center; margin-top:6px; }
    .legend .box { width:16px; height:16px; border-radius:3px; }
    .footer { margin-top:18px; display:flex; justify-content:space-between; align-items:center; color:var(--muted); font-size:12px; }
    a { color:#cfe1ff; text-decoration:none; }
    .kpis { display:grid; grid-template-columns: repeat(3, 1fr); gap:12px; }
    @media (max-width:900px){ .grid { grid-template-columns: repeat(6, 1fr); } .heat{ grid-template-columns: repeat(10, 1fr);} }
    @media (max-width:600px){ .grid { grid-template-columns: repeat(1, 1fr); } .kpis{ grid-template-columns: repeat(1, 1fr);} }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="row">
      <div class="title">Crypto System Dashboard <span class="pill">v __VERSION__</span></div>
      <div class="muted" id="ts"></div>
    </div>

    <div class="grid">
      <div class="card" style="grid-column: span 4;">
        <h3>Total P&L</h3>
        <div id="total" class="big">—</div>
        <div class="muted" id="ordersCount">— orders</div>
      </div>
      <div class="card" style="grid-column: span 8;">
        <h3>P&L by Strategy</h3>
        <div id="byStrat" class="kpis"></div>
      </div>

      <div class="card" style="grid-column: span 12;">
        <h3>Calendar P&L (last ~90 days)</h3>
        <div id="calendar" class="heat"></div>
        <div class="legend">
          <div class="box" style="background:#22304d"></div><div class="muted">0</div>
          <div class="box" style="background:#335e3f"></div><div class="muted">small +</div>
          <div class="box" style="background:#17c964"></div><div class="muted">large +</div>
          <div class="box" style="background:#4b1f2a"></div><div class="muted">small −</div>
          <div class="box" style="background:#f31260"></div><div class="muted">large −</div>
        </div>
      </div>

      <div class="card" style="grid-column: span 12;">
        <h3>Recent Orders</h3>
        <div id="orders" class="list"></div>
      </div>
    </div>

    <div class="footer">
      <div>Backend: FastAPI · <a href="/pnl/summary">/pnl/summary</a> · <a href="/orders/recent?limit=50">/orders/recent</a></div>
      <div>© __YEAR__</div>
    </div>
  </div>

<script>
const $ = (q) => document.querySelector(q);
const fmt = (x) => {
  if (x === null || x === undefined) return "—";
  const f = Number(x);
  if (Number.isNaN(f)) return String(x);
  return (f >= 0 ? "+" : "") + f.toFixed(2);
};
const colorFor = (v) => {
  if (!v) return "#22304d";
  const n = Number(v);
  if (Number.isNaN(n) || n === 0) return "#22304d";
  if (n > 0) {
    if (n < 5) return "#335e3f";
    if (n < 20) return "#23985b";
    return "#17c964";
  } else {
    const a = Math.abs(n);
    if (a < 5) return "#4b1f2a";
    if (a < 20) return "#c52554";
    return "#f31260";
  }
};

async function loadAll() {
  try {
    const pnl = await fetch("/pnl/summary").then(r => r.json());
    const orders = await fetch("/orders/recent?limit=50").then(r => r.json());

    $("#ts").textContent = new Date().toLocaleString();

    // total pnl
    const tot = pnl.total_pnl ?? 0;
    const el = $("#total");
    el.textContent = fmt(tot);
    el.className = "big " + (tot >= 0 ? "pos" : "neg");

    // orders count
    $("#ordersCount").textContent = (pnl.orders_count ?? 0) + " orders";

    // by strategy
    const bs = $("#byStrat");
    bs.innerHTML = "";
    const map = pnl.by_strategy || {};
    const keys = Object.keys(map).sort();
    if (keys.length === 0) {
      bs.innerHTML = "<div class='muted'>No strategy P&L yet.</div>";
    } else {
      keys.forEach(k => {
        const v = Number(map[k] || 0);
        const d = document.createElement("div");
        d.className = "card";
        d.innerHTML = "<div class='row'><div class='muted'>"+k+"</div><div class='" + (v>=0?"pos":"neg") + "'>"+fmt(v)+"</div></div>";
        bs.appendChild(d);
      });
    }

    // calendar
    const cal = $("#calendar");
    cal.innerHTML = "";
    (pnl.calendar || []).forEach(c => {
      const cell = document.createElement("div");
      cell.className = "cell";
      const v = Number(c.pnl || 0);
      cell.style.background = colorFor(v);
      cell.setAttribute("title", c.date + " · " + fmt(v));
      if (v !== 0) cell.dataset.v = "1";
      cal.appendChild(cell);
    });

    // orders list
    const ol = $("#orders");
    ol.innerHTML = "";
    (orders || []).forEach(o => {
      const row = document.createElement("div");
      row.className = "order";
      const left = document.createElement("div");
      left.innerHTML = "<div class='sym'>" + (o.symbol || "—") + "</div><div class='side'>" + (o.side || "—") + " · " + (o.qty || "—") + " @ " + (o.price||"—") + "</div>";
      const right = document.createElement("div");
      const pv = Number(o.pnl || 0);
      right.className = "pnl " + (pv >= 0 ? "pos" : "neg");
      right.textContent = fmt(pv);
      row.appendChild(left);
      row.appendChild(right);
      ol.appendChild(row);
    });

  } catch (e) {
    console.error(e);
  }
}
loadAll();
setInterval(loadAll, 10000);
</script>

</body>
</html>
"""

def _dashboard_html() -> str:
    return (
        _DASHBOARD_HTML
        .replace("__VERSION__", APP_VERSION)
        .replace("__YEAR__", str(datetime.now().year))
    )

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/dashboard")

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(_dashboard_html())

@app.get("/health")
async def health():
    return PlainTextResponse("ok", status_code=200)

@app.get("/orders/attribution")
async def orders_attr():
    return JSONResponse({
        "service": "crypto-system",
        "version": APP_VERSION,
        "ts": _utc_now_iso(),
        "auto_scan_enabled": bool(int(os.getenv("AUTO_SCAN_ENABLED", "0"))),
        "auto_scan_every": int(os.getenv("AUTO_SCAN_EVERY", "60")),
    })

@app.get("/orders/recent")
async def orders_recent(limit: int = Query(50, ge=1, le=1000)):
    return JSONResponse(list(reversed(_order_log[-limit:])))

@app.get("/v2/positions")
async def positions():
    return JSONResponse(_positions)

@app.get("/pnl/summary")
async def pnl_summary():
    return JSONResponse(_pnl_summary())

# --- SCAN endpoints -----------------------------------------------------------
async def _run_one_scan(strat: str, dry: int, timeframe: str, limit: int,
                        symbols: List[str], notional: Optional[float],
                        topk: Optional[int], min_score: Optional[float]) -> Dict[str, Any]:
    req = _mk_scan_payload(
        timeframe=timeframe,
        limit=limit,
        symbols=symbols,
        notional=notional,
        topk=topk,
        min_score=min_score,
    )
    if _strategy_book is None:
        log.warning("No strategy adapter handled %s; returning flat", strat)
        return {"status": "ok", "strat": strat, "result": {"orders": []}, "ts": _utc_now_iso()}

    try:
        res = await _scan_bridge(strat, req, dry=dry)
        _append_orders_from_scan(strat, res)
        return {"status": "ok", "strat": strat, "result": res, "ts": _utc_now_iso()}
    except Exception as e:
        log.exception("StrategyBook.scan error")
        return {"status": "error", "strat": strat, "error": str(e), "ts": _utc_now_iso()}

@app.post("/scan/{strat}")
async def scan_one(
    strat: str,
    dry: int = Query(1, ge=0, le=1),
    timeframe: str = Query(DEFAULT_TIMEFRAME),
    limit: int = Query(DEFAULT_LIMIT, ge=1),
    notional: Optional[float] = Query(DEFAULT_NOTIONAL),
    symbols: Optional[str] = Query(None),
    topk: Optional[int] = Query(None),
    min_score: Optional[float] = Query(None),
):
    syms = _parse_symbols(symbols)
    return JSONResponse(await _run_one_scan(strat, dry, timeframe, limit, syms, notional, topk, min_score))

@app.post("/scan/all")
async def scan_all(
    dry: int = Query(1, ge=0, le=1),
    timeframe: str = Query(DEFAULT_TIMEFRAME),
    limit: int = Query(DEFAULT_LIMIT, ge=1),
    notional: Optional[float] = Query(DEFAULT_NOTIONAL),
    symbols: Optional[str] = Query(None),
    topk: Optional[int] = Query(None),
    min_score: Optional[float] = Query(None),
):
    syms = _parse_symbols(symbols)
    out = []
    for s in STRATEGY_LIST:
        out.append(await _run_one_scan(s, dry, timeframe, limit, syms, notional, topk, min_score))
        await asyncio.sleep(1.0)  # small gap
    return JSONResponse(out)

# -----------------------------------------------------------------------------
# Internal scheduler (Option B)
# -----------------------------------------------------------------------------
async def _scheduler_loop():
    every = max(10, int(os.getenv("AUTO_SCAN_EVERY", "60")))
    while True:
        try:
            if int(os.getenv("AUTO_SCAN_ENABLED", "0")) == 1:
                log.info("Scheduler tick: running all strategies (dry=0)")
                # run through all strategies sequentially with defaults
                syms = _parse_symbols(None)
                for s in STRATEGY_LIST:
                    # dry=0 to execute live if your StrategyBook honors it via execute/live flags
                    await _run_one_scan(
                        strat=s,
                        dry=0,
                        timeframe=DEFAULT_TIMEFRAME,
                        limit=DEFAULT_LIMIT,
                        symbols=syms,
                        notional=DEFAULT_NOTIONAL,
                        topk=None,
                        min_score=None,
                    )
                    await asyncio.sleep(1.0)
            await asyncio.sleep(every)
        except Exception:
            log.exception("Scheduler loop crashed; recovering in %ss", every)
            await asyncio.sleep(every)

@app.on_event("startup")
async def on_startup():
    # kick off scheduler if enabled
    if int(os.getenv("AUTO_SCAN_ENABLED", "0")) == 1:
        asyncio.create_task(_scheduler_loop())
        log.info("Auto scheduler enabled (every %ss)", int(os.getenv("AUTO_SCAN_EVERY", "60")))
    else:
        log.info("Auto scheduler disabled")

# -----------------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, log_level="info")
