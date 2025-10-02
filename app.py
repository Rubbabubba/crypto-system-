# app.py — v3.5.3
# - Full dashboard: Total P&L, by-strategy P&L, calendar P&L, positions, recent orders
# - Built-in scheduler (Option B) controlled by env vars
# - Robust StrategyBook.scan bridge (dict → JSON → ScanRequest)
# - Renders HTML with string.Template (no f-strings around JS/CSS)

import os
import json
import time
import threading
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel
from string import Template

# ===== Logging =====
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# ===== Strategies package (your code) =====
try:
    from strategies import StrategyBook, ScanRequest, ScanResult  # type: ignore
except Exception as e:
    log.error("Failed importing strategies: %s", e)
    StrategyBook = None  # type: ignore

    class ScanRequest(BaseModel):  # minimal shim
        timeframe: str
        limit: int
        dry: int = 1
        symbols: List[str]
        notional: Optional[float] = None
        topk: Optional[int] = None
        min_score: Optional[float] = None

    class ScanResult(BaseModel):  # minimal shim
        symbol: str
        action: str
        reason: str
        side: Optional[str] = None
        qty: Optional[float] = None
        notional: Optional[float] = None
        status: Optional[str] = None

# ===== App =====
app = FastAPI(title="Crypto System", version="3.5.3")

# ===== Global StrategyBook =====
_strategy_book = None
if StrategyBook is not None:
    try:
        _strategy_book = StrategyBook()
        log.info("StrategyBook initialized")
    except Exception as e:
        log.exception("Failed to init StrategyBook: %s", e)

# ===== Env / Defaults =====
ENABLE_SCHEDULER = os.getenv("ENABLE_SCHEDULER", "0") == "1"
SCHEDULER_INTERVAL_SEC = int(os.getenv("SCHEDULER_INTERVAL_SEC", "60"))
STRATEGY_LIST = [s.strip() for s in os.getenv("STRATEGY_LIST", "c1,c2,c3,c4,c5,c6").split(",") if s.strip()]

DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "5Min")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "360"))
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", "25"))
DEFAULT_SYMBOLS = [s.strip() for s in os.getenv("DEFAULT_SYMBOLS", "BTC/USD,ETH/USD").split(",") if s.strip()]

# ===== Helpers =====
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def _maybe_await(x):
    if hasattr(x, "__await__"):
        import asyncio
        return asyncio.get_event_loop().run_until_complete(x)
    return x

def _mk_scan_dict(
    timeframe: str,
    limit: int,
    dry: int,
    symbols: List[str],
    notional: Optional[float] = None,
    topk: Optional[int] = None,
    min_score: Optional[float] = None,
) -> Dict[str, Any]:
    return {
        "timeframe": timeframe,
        "limit": int(limit),
        "dry": int(dry),
        "symbols": symbols,
        "notional": notional,
        "topk": topk,
        "min_score": min_score,
    }

def _safe_call_orders_recent(limit: int, status: str) -> List[Dict[str, Any]]:
    try:
        if hasattr(_strategy_book, "orders_recent"):
            return _strategy_book.orders_recent(limit=limit, status=status)  # type: ignore
        if hasattr(_strategy_book, "broker") and hasattr(_strategy_book.broker, "orders_recent"):
            return _strategy_book.broker.orders_recent(limit=limit, status=status)  # type: ignore
    except Exception:
        log.exception("orders_recent failed")
    return []

def _safe_call_positions() -> List[Dict[str, Any]]:
    try:
        if hasattr(_strategy_book, "positions"):
            return _strategy_book.positions()  # type: ignore
        if hasattr(_strategy_book, "broker") and hasattr(_strategy_book.broker, "positions"):
            return _strategy_book.broker.positions()  # type: ignore
    except Exception:
        log.exception("positions failed")
    return []

def _safe_call_pnl_summary() -> Dict[str, Any]:
    try:
        if hasattr(_strategy_book, "pnl_summary"):
            return _strategy_book.pnl_summary()  # type: ignore
        if hasattr(_strategy_book, "analytics") and hasattr(_strategy_book.analytics, "pnl_summary"):
            return _strategy_book.analytics.pnl_summary()  # type: ignore
    except Exception:
        log.exception("pnl_summary failed")

    today = datetime.now().date()
    cal = [{"date": (today - timedelta(days=i)).isoformat(), "realized": 0.0} for i in range(0, 14)][::-1]
    return {
        "asof": _iso_now(),
        "total": {"realized": 0.0, "unrealized": 0.0},
        "by_strategy": {s: {"realized": 0.0, "unrealized": 0.0} for s in STRATEGY_LIST},
        "calendar": cal,
    }

def _safe_call_universe() -> Dict[str, Any]:
    return {
        "core": ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD","AVAX/USD","LINK/USD","BCH/USD","LTC/USD"],
        "alts": ["ADA/USD","TON/USD","TRX/USD","APT/USD","ARB/USD","SUI/USD","OP/USD","MATIC/USD","NEAR/USD","ATOM/USD"]
    }

# ---------- Robust bridge to StrategyBook.scan ----------
def _scan_bridge(strat: str, req_dict: Dict[str, Any]):
    if _strategy_book is None:
        raise RuntimeError("StrategyBook not available")

    # 1) dict
    try:
        return _maybe_await(_strategy_book.scan(strat, req_dict))  # type: ignore
    except TypeError as te1:
        log.warning("scan(strat, dict) failed: %s", te1)
    except Exception:
        log.exception("scan(strat, dict) crashed")

    # 2) JSON string
    try:
        return _maybe_await(_strategy_book.scan(strat, json.dumps(req_dict)))  # type: ignore
    except TypeError as te2:
        log.warning("scan(strat, json) failed: %s", te2)
    except Exception:
        log.exception("scan(strat, json) crashed")

    # 3) ScanRequest model (if available)
    try:
        if ScanRequest is not None:
            model = ScanRequest(**req_dict)  # type: ignore
            return _maybe_await(_strategy_book.scan(strat, model))  # type: ignore
    except TypeError as te3:
        log.warning("scan(strat, ScanRequest) failed: %s", te3)
    except Exception:
        log.exception("scan(strat, ScanRequest) crashed")

    raise TypeError("No compatible StrategyBook.scan signature (tried dict, json, ScanRequest)")

# ===== Scheduler (Option B) =====
_stop_flag = False

def _scheduler_loop():
    global _stop_flag
    if _strategy_book is None:
        log.warning("Scheduler: StrategyBook is not available; exiting scheduler thread.")
        return
    while not _stop_flag and ENABLE_SCHEDULER:
        try:
            log.info("Scheduler tick: running all strategies (dry=0)")
            req = _mk_scan_dict(
                timeframe=DEFAULT_TIMEFRAME,
                limit=DEFAULT_LIMIT,
                dry=0,
                symbols=DEFAULT_SYMBOLS,
                notional=DEFAULT_NOTIONAL,
                topk=None,
                min_score=None,
            )
            for s in STRATEGY_LIST:
                try:
                    _scan_bridge(s, req)
                except Exception as e:
                    log.error("StrategyBook.scan error: %s", e)
                    log.warning("No strategy adapter handled %s; returning flat", s)
                time.sleep(1.0)
        except Exception:
            log.exception("Scheduler loop crashed once; continuing.")
        for _ in range(SCHEDULER_INTERVAL_SEC):
            if _stop_flag:
                break
            time.sleep(1)

if ENABLE_SCHEDULER:
    t = threading.Thread(target=_scheduler_loop, name="scheduler", daemon=True)
    t.start()
    log.info("Scheduler thread started (interval=%ss)", SCHEDULER_INTERVAL_SEC)

# ===== Routes =====
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard")

@app.get("/health")
def health():
    return {"status": "ok", "time": _iso_now(), "scheduler": ENABLE_SCHEDULER}

# ---- Dashboard (rendered with string.Template, safe for JS/CSS) ----
_DASHBOARD_TMPL = Template(r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Crypto System – Dashboard (v$version)</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root {
      --bg:#0b0e12; --panel:#12161c; --muted:#9aa4ad; --text:#e7edf3; --accent:#24a0ed; --pos:#19c37d; --neg:#ff4d4f;
    }
    * { box-sizing:border-box; }
    body {
      margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, Noto Sans, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
      background: var(--bg); color: var(--text);
    }
    header {
      padding:16px 20px; display:flex; align-items:center; gap:16px; border-bottom:1px solid #1b2027; position:sticky; top:0; background:rgba(11,14,18,.9); backdrop-filter: blur(6px);
    }
    h1 { font-size:18px; margin:0; font-weight:600; letter-spacing:.3px; }
    .badge { font-size:12px; color:#fff; background:#2b3340; padding:4px 8px; border-radius:999px; }
    .tag { font-size:12px; color:#cfd6dd; background:#1a212b; padding:4px 8px; border-radius:6px; border:1px solid #2a323d; }
    main { padding:20px; max-width:1200px; margin:0 auto; }
    .grid { display:grid; grid-template-columns: repeat(12, 1fr); gap:16px; }
    .card { grid-column: span 12; background:var(--panel); border:1px solid #1b2027; border-radius:14px; padding:16px; }
    @media (min-width: 900px) {
      .span4 { grid-column: span 4; }
      .span6 { grid-column: span 6; }
      .span8 { grid-column: span 8; }
      .span12 { grid-column: span 12; }
      .span3 { grid-column: span 3; }
    }
    .kpis { display:flex; gap:16px; flex-wrap:wrap; }
    .kpi { background:#0f141a; border:1px solid #1b2027; border-radius:12px; padding:12px 14px; min-width:180px; }
    .kpi h3 { font-size:12px; margin:0; color:#9aa4ad; font-weight:500; }
    .kpi .v { font-size:22px; margin-top:6px; font-weight:700; }
    .green { color: var(--pos); }
    .red { color: var(--neg); }
    table { width:100%; border-collapse: collapse; }
    th, td { text-align:left; padding:8px; border-bottom:1px solid #1b2027; font-size:14px; }
    th { color:#c2cbd3; font-weight:600; }
    .controls { display:flex; gap:10px; flex-wrap:wrap; }
    input, select, button {
      background:#0f141a; color:#e7edf3; border:1px solid #273140; border-radius:10px; padding:8px 10px; font-size:14px;
    }
    button.primary { background:var(--accent); color:#00121f; border-color: transparent; font-weight:700; }
    .calendar { display:grid; grid-template-columns: repeat(14, 1fr); gap:6px; }
    .cell { height:36px; border-radius:6px; background:#0f141a; display:flex; align-items:center; justify-content:center; font-size:12px; border:1px solid #1b2027; }
    .pos { background: rgba(25,195,125,.1); border-color: rgba(25,195,125,.3); }
    .neg { background: rgba(255,77,79,.12); border-color: rgba(255,77,79,.35); }
    .small { font-size:12px; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; }
  </style>
</head>
<body>
  <header>
    <h1>Crypto Trading System <span class="badge">v$version</span></h1>
    <span id="sched" class="tag">Scheduler: …</span>
    <span id="asof" class="tag">as of —</span>
  </header>

  <main>
    <div class="grid">
      <section class="card span12">
        <div class="kpis">
          <div class="kpi">
            <h3>Total Realized P&L</h3>
            <div id="kpi-realized" class="v">—</div>
          </div>
          <div class="kpi">
            <h3>Total Unrealized P&L</h3>
            <div id="kpi-unrealized" class="v">—</div>
          </div>
          <div class="kpi">
            <h3>Open Positions</h3>
            <div id="kpi-positions" class="v">—</div>
          </div>
          <div class="kpi">
            <h3>Recent Orders (24h)</h3>
            <div id="kpi-orders" class="v">—</div>
          </div>
        </div>
      </section>

      <section class="card span8">
        <h3>P&L by Strategy</h3>
        <table id="tbl-strat">
          <thead>
            <tr><th>Strategy</th><th class="mono">Realized</th><th class="mono">Unrealized</th></tr>
          </thead>
          <tbody></tbody>
        </table>
      </section>

      <section class="card span4">
        <h3>Calendar P&L (Realized)</h3>
        <div id="cal" class="calendar"></div>
        <div class="small" id="cal-hint" style="margin-top:6px; color:#9aa4ad;"></div>
      </section>

      <section class="card span6">
        <h3>Open Positions</h3>
        <table id="tbl-pos">
          <thead>
            <tr><th>Symbol</th><th>Side</th><th class="mono">Qty</th><th class="mono">Avg</th><th class="mono">Unrealized</th></tr>
          </thead>
          <tbody></tbody>
        </table>
      </section>

      <section class="card span6">
        <h3>Recent Orders</h3>
        <table id="tbl-orders">
          <thead>
            <tr><th>Time</th><th>Symbol</th><th>Side</th><th class="mono">Qty</th><th>Status</th><th>Strategy</th></tr>
          </thead>
          <tbody></tbody>
        </table>
      </section>

      <section class="card span12">
        <h3>Manual Scan (optional)</h3>
        <div class="controls" style="margin-bottom:10px;">
          <label>Timeframe <input id="tf" value="$default_timeframe"/></label>
          <label>Limit <input id="lim" type="number" value="$default_limit"/></label>
          <label>Notional <input id="notional" type="number" value="$default_notional"/></label>
          <label>Symbols <input id="syms" size="80" value="$default_symbols"/></label>
          <label>Dry?
            <select id="dry">
              <option value="1">1 (no orders)</option>
              <option value="0">0 (live)</option>
            </select>
          </label>
          <button class="primary" onclick="scanAll()">Run All</button>
        </div>
        <div class="controls">
          $buttons_html
        </div>
        <pre id="scan-log" class="mono small" style="margin-top:12px; background:#0f141a; border:1px solid #1b2027; padding:10px; border-radius:8px; max-height:220px; overflow:auto;"></pre>
      </section>
    </div>
  </main>

<script>
const fmt = (x) => {
  if (x === null || x === undefined) return "—";
  const s = Number(x);
  if (!isFinite(s)) return "—";
  const str = s.toFixed(2);
  return (s > 0 ? '<span class="green">+'+str+'</span>' : (s < 0 ? '<span class="red">'+str+'</span>' : str));
};

async function loadAll(){
  try {
    const health = await (await fetch('/health')).json();
    document.getElementById('sched').textContent = 'Scheduler: ' + (health.scheduler ? 'ON' : 'OFF');
  } catch(e) {}

  try {
    const pnl = await (await fetch('/pnl/summary')).json();
    document.getElementById('asof').textContent = 'as of ' + (pnl.asof || '');
    const realized = pnl.total?.realized ?? 0;
    const unreal = pnl.total?.unrealized ?? 0;
    document.getElementById('kpi-realized').innerHTML = fmt(realized);
    document.getElementById('kpi-unrealized').innerHTML = fmt(unreal);

    const tbody = document.querySelector('#tbl-strat tbody');
    tbody.innerHTML = '';
    const bys = pnl.by_strategy || {};
    Object.keys(bys).sort().forEach(k => {
      const r = bys[k]?.realized ?? 0;
      const u = bys[k]?.unrealized ?? 0;
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${k}</td><td class="mono">${fmt(r)}</td><td class="mono">${fmt(u)}</td>`;
      tbody.appendChild(tr);
    });

    const calDiv = document.getElementById('cal');
    calDiv.innerHTML = '';
    const cal = pnl.calendar || [];
    cal.forEach(d => {
      const v = Number(d.realized || 0);
      const cls = v > 0 ? 'cell pos' : (v < 0 ? 'cell neg' : 'cell');
      const el = document.createElement('div');
      el.className = cls;
      el.title = `${d.date}  realized: ${v.toFixed(2)}`;
      el.textContent = (v>0?'+':'') + v.toFixed(0);
      calDiv.appendChild(el);
    });
    document.getElementById('cal-hint').textContent = 'Last ' + cal.length + ' trading days';

  } catch(e) {
    console.error(e);
  }

  try {
    const pos = await (await fetch('/v2/positions')).json();
    document.getElementById('kpi-positions').innerHTML = (pos.length || 0);
    const tbody = document.querySelector('#tbl-pos tbody');
    tbody.innerHTML = '';
    pos.forEach(p => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${p.symbol||'-'}</td><td>${p.side||'-'}</td><td class="mono">${p.qty||'-'}</td><td class="mono">${p.avg_price||'-'}</td><td class="mono">${fmt(p.unrealized_pl||0)}</td>`;
      tbody.appendChild(tr);
    });
  } catch(e) {}

  try {
    const ords = await (await fetch('/orders/recent?status=all&limit=200')).json();
    document.getElementById('kpi-orders').innerHTML = (ords.length || 0);
    const tbody = document.querySelector('#tbl-orders tbody');
    tbody.innerHTML = '';
    ords.slice(0,200).forEach(o => {
      const tr = document.createElement('tr');
      const ts = o.submitted_at || o.created_at || o.time || '';
      tr.innerHTML = `<td>${ts}</td><td>${o.symbol||'-'}</td><td>${o.side||'-'}</td><td class="mono">${o.qty||o.notional||'-'}</td><td>${o.status||'-'}</td><td>${o.strategy||o.tag||'-'}</td>`;
      tbody.appendChild(tr);
    });
  } catch(e) {
    console.error(e);
  }
}

async function scanOne(strat){
  const tf = document.getElementById('tf').value;
  const lim = document.getElementById('lim').value;
  const notional = document.getElementById('notional').value;
  const syms = document.getElementById('syms').value;
  const dry = document.getElementById('dry').value;
  const url = `/scan/${strat}?dry=${dry}&timeframe=${encodeURIComponent(tf)}&limit=${lim}&notional=${notional}&symbols=${encodeURIComponent(syms)}`;
  const t0 = Date.now();
  const r = await fetch(url, {method:'POST'});
  const txt = await r.text();
  const ms = Date.now()-t0;
  const logEl = document.getElementById('scan-log');
  logEl.textContent = `[${new Date().toISOString()}] POST ${url}  ${r.status} (${ms}ms)\n${txt}\n\n` + logEl.textContent;
  loadAll();
}

async function scanAll(){
  const tf = document.getElementById('tf').value;
  const lim = document.getElementById('lim').value;
  const notional = document.getElementById('notional').value;
  const syms = document.getElementById('syms').value;
  const dry = document.getElementById('dry').value;
  const url = `/scan/all?dry=${dry}&timeframe=${encodeURIComponent(tf)}&limit=${lim}&notional=${notional}&symbols=${encodeURIComponent(syms)}`;
  const t0 = Date.now();
  const r = await fetch(url, {method:'POST'});
  const txt = await r.text();
  const ms = Date.now()-t0;
  const logEl = document.getElementById('scan-log');
  logEl.textContent = `[${new Date().toISOString()}] POST ${url}  ${r.status} (${ms}ms)\n${txt}\n\n` + logEl.textContent;
  loadAll();
}

window.scanOne = scanOne;
window.scanAll = scanAll;

loadAll();
setInterval(loadAll, 60000);
</script>

</body>
</html>
""")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    buttons_html = "".join([f"<button onclick=\"scanOne('{s}')\">{s}</button>" for s in STRATEGY_LIST])
    html = _DASHBOARD_TMPL.substitute(
        version=app.version,
        default_timeframe=DEFAULT_TIMEFRAME,
        default_limit=str(DEFAULT_LIMIT),
        default_notional=str(DEFAULT_NOTIONAL),
        default_symbols=",".join(DEFAULT_SYMBOLS),
        buttons_html=buttons_html,
    )
    return HTMLResponse(html)

# ======= API: Universe =======
@app.get("/universe")
def universe():
    return JSONResponse(_safe_call_universe())

# ======= API: Orders / Positions / P&L =======
@app.get("/orders/recent")
def orders_recent(limit: int = Query(100, ge=1, le=500), status: str = Query("all")):
    data = _safe_call_orders_recent(limit=limit, status=status)
    return JSONResponse(data)

@app.get("/v2/positions")
def positions():
    data = _safe_call_positions()
    return JSONResponse(data)

@app.get("/pnl/summary")
def pnl_summary():
    data = _safe_call_pnl_summary()
    return JSONResponse(data)

@app.get("/orders/attribution")
def orders_attr():
    return JSONResponse({"ok": True, "time": _iso_now(), "v": app.version})

# ======= API: Scanning =======
@app.post("/scan/{strat}")
def scan_strat(
    strat: str,
    dry: int = Query(1),
    timeframe: str = Query(DEFAULT_TIMEFRAME),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=5000),
    notional: float = Query(DEFAULT_NOTIONAL),
    symbols: str = Query(",".join(DEFAULT_SYMBOLS)),
    topk: Optional[int] = Query(None, ge=1, le=25),
    min_score: Optional[float] = Query(None),
):
    if _strategy_book is None:
        return JSONResponse({"error": "StrategyBook not available"}, status_code=500)

    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    req = _mk_scan_dict(
        timeframe=timeframe,
        limit=limit,
        dry=dry,
        symbols=syms,
        notional=notional,
        topk=topk,
        min_score=min_score,
    )
    try:
        res = _scan_bridge(strat, req)
        if isinstance(res, list):
            payload = [r.dict() if hasattr(r, "dict") else r for r in res]
        else:
            payload = res.dict() if hasattr(res, "dict") else res
        return JSONResponse({"ok": True, "results": payload})
    except Exception as e:
        log.error("scan_strat failed: %s", e)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@app.post("/scan/all")
def scan_all(
    dry: int = Query(1),
    timeframe: str = Query(DEFAULT_TIMEFRAME),
    limit: int = Query(DEFAULT_LIMIT, ge=1, le=5000),
    notional: float = Query(DEFAULT_NOTIONAL),
    symbols: str = Query(",".join(DEFAULT_SYMBOLS)),
    topk: Optional[int] = Query(None, ge=1, le=25),
    min_score: Optional[float] = Query(None),
):
    if _strategy_book is None:
        return JSONResponse({"error": "StrategyBook not available"}, status_code=500)

    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    req = _mk_scan_dict(
        timeframe=timeframe,
        limit=limit,
        dry=dry,
        symbols=syms,
        notional=notional,
        topk=topk,
        min_score=min_score,
    )
    out = []
    for strat in STRATEGY_LIST:
        try:
            res = _scan_bridge(strat, req)
            if isinstance(res, list):
                payload = [r.dict() if hasattr(r, "dict") else r for r in res]
            else:
                payload = res.dict() if hasattr(res, "dict") else res
            out.append({"strat": strat, "results": payload})
        except Exception as e:
            log.error("scan_all %s failed: %s", strat, e)
            out.append({"strat": strat, "error": str(e)})
        time.sleep(0.5)
    return JSONResponse({"ok": True, "runs": out})

# ===== Graceful shutdown =====
@app.on_event("shutdown")
def _shutdown():
    global _stop_flag
    _stop_flag = True
    log.info("Shutting down app; scheduler will stop.")

# ===== Local dev =====
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
