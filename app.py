# app.py  (v3.7.0)
# Single-file FastAPI service with:
# - Robust StrategyBook scan adapter (kwargs-only; no typed model)
# - Internal scheduler (Option B) via FastAPI lifespan
# - Full dashboard HTML (Total P&L, By-Strategy P&L, Calendar Heatmap, Recent Orders)
# - Endpoints: /scan/{c1..c6}, /scan/all, /orders/recent, /orders/attribution, /pnl/summary,
#              /v2/positions, /health, /dashboard
#
# Env (Render):
#   AUTO_SCAN_ENABLED=1
#   AUTO_SCAN_EVERY=60
#   DEFAULT_TIMEFRAME=5Min
#   DEFAULT_LIMIT=360
#   DEFAULT_NOTIONAL=25
#   DEFAULT_SYMBOLS=BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD,ADA/USD,TON/USD,TRX/USD,APT/USD,ARB/USD,SUI/USD,OP/USD,MATIC/USD,NEAR/USD,ATOM/USD
#   PORT (Render provides automatically)

import os
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

APP_VERSION = "3.7.0"

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

# -----------------------------------------------------------------------------
# StrategyBook loader (optional)
# -----------------------------------------------------------------------------
_strategy_book = None

def _try_import_strategy_book():
    global _strategy_book
    candidates = [
        ("strategies", "StrategyBook"),
        ("strategy_book", "StrategyBook"),
        ("app_strategy", "StrategyBook"),
    ]
    for mod, cls in candidates:
        try:
            m = __import__(mod, fromlist=[cls])
            sb = getattr(m, cls, None)
            if sb is None:
                continue
            _strategy_book = sb() if callable(sb) else sb
            log.info("Loaded StrategyBook from %s.%s", mod, cls)
            return
        except Exception:
            continue
    log.warning("StrategyBook not found; scans will return 'flat'.")

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
    raw = raw.replace("%2F", "/")
    return [s.strip() for s in raw.split(",") if s.strip()]

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        return await x
    return x

# -----------------------------------------------------------------------------
# Scan adapter (kwargs-only; no typed model; no dict/json positional calls)
# -----------------------------------------------------------------------------
def _mk_scan_payload(
    timeframe: str,
    limit: int,
    symbols: List[str],
    notional: Optional[float] = None,
    topk: Optional[int] = None,
    min_score: Optional[float] = None,
) -> Dict[str, Any]:
    return {
        "timeframe": str(timeframe),
        "limit": int(limit),
        "symbols": symbols,
        "notional": notional,
        "topk": topk,
        "min_score": min_score,
    }

async def _scan_bridge(strat: str, req: Dict[str, Any], dry: int = 1) -> Any:
    """
    Try common kwargs calling styles only — avoids dict/json positional variants
    that caused 'string indices must be integers' in some StrategyBook impls.
    We DO NOT pass 'dry' anywhere; we derive execute/live from it when present.
    """
    if _strategy_book is None:
        raise RuntimeError("StrategyBook not available")

    clean = {k: v for k, v in req.items() if v is not None}
    execute = bool(dry == 0)
    live = execute

    variants = [
        # scan(strat=..., timeframe=..., ..., execute=..., live=...)
        {"kwargs": {"strat": strat, **clean, "execute": execute, "live": live}},
        # scan(strategy=..., timeframe=..., ...)
        {"kwargs": {"strategy": strat, **clean, "execute": execute, "live": live}},
        # scan(strat, timeframe=..., ...)
        {"args": (strat,), "kwargs": {**clean, "execute": execute, "live": live}},
        # scan(strategy=..., timeframe=..., ...) without execute/live
        {"kwargs": {"strategy": strat, **clean}},
        # scan(strat=..., timeframe=..., ...) without execute/live
        {"kwargs": {"strat": strat, **clean}},
        # scan(strat, timeframe=..., ...) without execute/live
        {"args": (strat,), "kwargs": {**clean}},
    ]

    last_error = None
    for i, v in enumerate(variants, 1):
        try:
            if "args" in v:
                return await _maybe_await(_strategy_book.scan(*v["args"], **v["kwargs"]))  # type: ignore
            else:
                return await _maybe_await(_strategy_book.scan(**v["kwargs"]))  # type: ignore
        except TypeError as te:
            log.warning("scan kwargs-variant #%d failed: %s", i, te)
            last_error = te
        except Exception as ex:
            log.exception("scan kwargs-variant #%d crashed", i)
            last_error = ex
    raise TypeError(f"No compatible StrategyBook.scan signature (kwargs-only). Last error: {last_error}")

# -----------------------------------------------------------------------------
# In-memory orders & PnL
# -----------------------------------------------------------------------------
_order_log: List[Dict[str, Any]] = []
_positions: Dict[str, Any] = {}

def _append_orders_from_scan(strat: str, scan_result: Any):
    try:
        if not scan_result:
            return
        orders = []
        if isinstance(scan_result, dict):
            if isinstance(scan_result.get("orders"), list):
                orders = scan_result["orders"]
            elif isinstance(scan_result.get("result"), dict) and isinstance(scan_result["result"].get("orders"), list):
                orders = scan_result["result"]["orders"]

        for od in orders:
            if not isinstance(od, dict):
                continue
            rec = dict(od)
            rec.setdefault("ts", _utc_now_iso())
            rec.setdefault("strat", strat)
            rec.setdefault("pnl", 0.0)
            _order_log.append(rec)
    except Exception:
        log.exception("Failed to harvest orders from scan result (%s)", strat)

def _pnl_summary() -> Dict[str, Any]:
    total = 0.0
    by_strat: Dict[str, float] = {}
    by_day: Dict[str, float] = {}
    for od in _order_log:
        pnl = float(od.get("pnl", 0.0))
        total += pnl
        s = str(od.get("strat", "unknown"))
        by_strat[s] = by_strat.get(s, 0.0) + pnl

        ts = od.get("ts")
        try:
            d = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
        except Exception:
            d = datetime.now(timezone.utc)
        key = d.strftime("%Y-%m-%d")
        by_day[key] = by_day.get(key, 0.0) + pnl

    # last 90 days
    days = []
    today = datetime.now(timezone.utc).date()
    for i in range(89, -1, -1):
        days.append((today.fromordinal(today.toordinal() - i)).isoformat())

    calendar = [{"date": d, "pnl": round(by_day.get(d, 0.0), 2)} for d in days]

    return {
        "version": APP_VERSION,
        "total_pnl": round(total, 2),
        "by_strategy": {k: round(v, 2) for k, v in by_strat.items()},
        "calendar": calendar,
        "orders_count": len(_order_log),
    }

# -----------------------------------------------------------------------------
# FastAPI app + CORS
# -----------------------------------------------------------------------------
STRATEGY_LIST = ["c1", "c2", "c3", "c4", "c5", "c6"]

# Internal scheduler (Option B)
_scheduler_task: Optional[asyncio.Task] = None

async def _scheduler_loop():
    every = max(10, int(os.getenv("AUTO_SCAN_EVERY", "60")))
    while True:
        try:
            if int(os.getenv("AUTO_SCAN_ENABLED", "0")) == 1:
                log.info("Scheduler tick: running all strategies (dry=0)")
                syms = _parse_symbols(None)
                for s in STRATEGY_LIST:
                    try:
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
                    except Exception:
                        log.exception("Scheduled scan failed for %s", s)
                    await asyncio.sleep(1.0)
            await asyncio.sleep(every)
        except Exception:
            log.exception("Scheduler loop crashed; retrying in %ss", every)
            await asyncio.sleep(every)

@asynccontextmanager
async def lifespan(app_: FastAPI):
    global _scheduler_task
    if int(os.getenv("AUTO_SCAN_ENABLED", "0")) == 1:
        _scheduler_task = asyncio.create_task(_scheduler_loop())
        log.info("Auto scheduler enabled (every %ss)", int(os.getenv("AUTO_SCAN_EVERY", "60")))
    else:
        log.info("Auto scheduler disabled")
    try:
        yield
    finally:
        if _scheduler_task:
            log.info("Shutting down app; scheduler will stop.")
            _scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await _scheduler_task

import contextlib
app = FastAPI(title="Crypto System", version=APP_VERSION, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# Dashboard HTML (plain string; no f-strings / no Template)
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
    return _DASHBOARD_HTML.replace("__VERSION__", APP_VERSION).replace("__YEAR__", str(datetime.now().year))

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse("/dashboard")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
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

# --- SCAN helpers -------------------------------------------------------------
async def _run_one_scan(
    strat: str,
    dry: int,
    timeframe: str,
    limit: int,
    symbols: List[str],
    notional: Optional[float],
    topk: Optional[int],
    min_score: Optional[float],
) -> Dict[str, Any]:
    req = _mk_scan_payload(timeframe, limit, symbols, notional, topk, min_score)
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
        await asyncio.sleep(1.0)
    return JSONResponse(out)

# -----------------------------------------------------------------------------
# Uvicorn entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, log_level="info")
