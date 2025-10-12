#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto System – FastAPI service (Render-safe)
Build: v2.0.0 (2025-10-11)

Routes
- GET  /                     -> Dashboard
- GET  /health               -> Service health
- GET  /diag/broker          -> Active broker diagnostics
- GET  /version              -> App version
- GET  /config               -> Defaults, symbols, strategies, params
- POST /scan/{strategy}      -> Run a scan for c1..c6; optional dry=true
- GET  /bars/{symbol}        -> Recent bars
- GET  /price/{symbol}       -> Last price
- GET  /positions            -> Spot positions summary
- GET  /orders               -> Open orders
- POST /order/market         -> Place market order by notional
- GET  /scheduler/start      -> Start dry-run scheduler loop (requires SCHED_ON=1)
- GET  /scheduler/stop       -> Stop scheduler loop

Notes (v2.0.0)
- Demo StrategyBook removed; app dispatches directly to strategies c1..c6.
- Kraken-first broker routing; Alpaca remains legacy fallback via 'broker'.
- Dashboard HTML expanded with live calls, symbol/strategy pickers, and actions.
"""
__version__ = "2.0.0"

import asyncio
import os
import sys
import logging
import threading, time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, HTMLResponse



# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("app")

# -----------------------------------------------------------------------------
# Broker selection (Kraken by default; Alpaca legacy fallback)
# -----------------------------------------------------------------------------
BROKER = os.getenv("BROKER", "kraken").lower()
USING_KRAKEN = BROKER == "kraken" or (os.getenv("KRAKEN_KEY") and os.getenv("KRAKEN_SECRET"))

if USING_KRAKEN:
    import broker_kraken as br  # Kraken adapter
    ACTIVE_BROKER_MODULE = "broker_kraken"
else:
    import broker as br  # Alpaca adapter (legacy)
    ACTIVE_BROKER_MODULE = "broker"

# Live trading enablement – broker-specific flag
TRADING_ENABLED_BASE = os.getenv("TRADING_ENABLED", "1") in ("1", "true", "True")
if USING_KRAKEN:
    TRADING_ENABLED = TRADING_ENABLED_BASE and (os.getenv("KRAKEN_TRADING", "0") in ("1", "true", "True"))
else:
    TRADING_ENABLED = TRADING_ENABLED_BASE and (os.getenv("ALPACA_TRADING", "0") in ("1", "true", "True"))

# -----------------------------------------------------------------------------
# Globals & defaults
# -----------------------------------------------------------------------------
APP_VERSION = os.getenv("APP_VERSION", __version__)
SERVICE_NAME = os.getenv("SERVICE_NAME", "Crypto System")

DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "5Min")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "300"))
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", os.getenv("ORDER_NOTIONAL", "25")))

# Symbol universe
try:
    from universe import load_universe_from_env
    _CURRENT_SYMBOLS = load_universe_from_env()
except Exception:
    _CURRENT_SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD", "ADAUSD", "DOGEUSD"]

# Strategy defaults (optimized params to retain)
DEFAULT_STRAT_PARAMS: Dict[str, Dict[str, Any]] = {
    "c1": {"ema_n": 20, "vwap_pull": 0.0020, "min_vol": 0.0},
    "c2": {"ema_n": 50, "exit_k": 0.997, "min_atr": 0.0},
    "c3": {"ch_n": 55, "break_k": 1.0005, "fail_k": 0.997, "min_atr": 0.0},
    "c4": {"ema_fast": 12, "ema_slow": 26, "sig": 9, "atr_n": 14, "atr_mult": 1.2, "min_vol": 0.0},
    "c5": {"lookback": 20, "band_k": 1.0010, "exit_k": 0.9990, "min_vol": 0.0},
    "c6": {"atr_n": 14, "atr_mult": 1.2, "ema_n": 20, "exit_k": 0.997, "min_vol": 0.0},
}
ACTIVE_STRATEGIES = list(DEFAULT_STRAT_PARAMS.keys())
_DISABLED_STRATS = set()

# -----------------------------------------------------------------------------
# Import strategies and build dispatcher
# -----------------------------------------------------------------------------
import c1, c2, c3, c4, c5, c6

STRAT_DISPATCH = {
    "c1": c1.scan,
    "c2": c2.scan,
    "c3": c3.scan,
    "c4": c4.scan,
    "c5": c5.scan,
    "c6": c6.scan,
}

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _merge_raw(strat: str, raw_in: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = dict(DEFAULT_STRAT_PARAMS.get(strat, {}))
    if raw_in:
        base.update({k: raw_in[k] for k in raw_in})
    return base

# -----------------------------------------------------------------------------
# FastAPI app
# -----------------------------------------------------------------------------
app = FastAPI(title=SERVICE_NAME, version=APP_VERSION)

@app.get("/health")
def health():
    return {
        "ok": True,
        "service": SERVICE_NAME,
        "version": APP_VERSION,
        "broker": ("kraken" if USING_KRAKEN else "alpaca"),
        "trading": TRADING_ENABLED,
        "time": utc_now(),
        "symbols": _CURRENT_SYMBOLS,
        "strategies": ACTIVE_STRATEGIES,
    }

@app.get("/diag/broker")
def diag_broker():
    return {"broker_module": ACTIVE_BROKER_MODULE, "using_kraken": USING_KRAKEN, "trading": TRADING_ENABLED}

@app.get("/version")
def version():
    return {"version": APP_VERSION}

@app.get("/config")
def config():
    return {
        "DEFAULT_TIMEFRAME": DEFAULT_TIMEFRAME,
        "DEFAULT_LIMIT": DEFAULT_LIMIT,
        "DEFAULT_NOTIONAL": DEFAULT_NOTIONAL,
        "SYMBOLS": _CURRENT_SYMBOLS,
        "STRATEGIES": ACTIVE_STRATEGIES,
        "PARAMS": DEFAULT_STRAT_PARAMS,
    }

# -----------------------------------------------------------------------------
# Request model
# -----------------------------------------------------------------------------
from pydantic import BaseModel
from typing import TypedDict

class ScanRequestModel(BaseModel):
    symbols: Optional[List[str]] = None
    timeframe: Optional[str] = None
    limit: Optional[int] = None
    notional: Optional[float] = None
    raw: Optional[Dict[str, Any]] = None
    dry: Optional[bool] = None

class ScanRequest(TypedDict, total=False):
    strategy: str
    timeframe: str
    limit: int
    notional: float
    symbols: List[str]
    raw: Dict[str, Any]

# -----------------------------------------------------------------------------
# Scan bridge — dispatch to the chosen strategy module
# -----------------------------------------------------------------------------
async def _scan_bridge(strat: str, req: Dict[str, Any], *args, **kwargs) -> List[Dict[str, Any]]:
    dry_arg = kwargs.get("dry", None)
    if dry_arg is None and args:
        dry_arg = bool(args[0])
    is_dry = bool(dry_arg) or (not TRADING_ENABLED)

    req = dict(req or {})
    strat = (strat or "").lower()
    if strat not in STRAT_DISPATCH:
        raise HTTPException(status_code=400, detail=f"Unknown strategy '{strat}'")

    req.setdefault("strategy", strat)
    req.setdefault("timeframe", req.get("tf") or DEFAULT_TIMEFRAME)
    req.setdefault("limit", req.get("limit") or DEFAULT_LIMIT)
    req.setdefault("notional", req.get("notional") or DEFAULT_NOTIONAL)

    syms = req.get("symbols") or _CURRENT_SYMBOLS
    if isinstance(syms, str):
        syms = [s.strip() for s in syms.split(",") if s.strip()]
    syms = [s.upper() for s in syms]
    req["symbols"] = syms

    req["raw"] = _merge_raw(strat, dict(req.get("raw") or {}))
    ctx = {"timeframe": req["timeframe"], "symbols": syms, "notional": req["notional"]}

    orders = STRAT_DISPATCH[strat](req, ctx) or []

    placed: List[Dict[str, Any]] = []
    for o in orders:
        sym = (o.get("symbol") or syms[0]).upper()
        side = (o.get("side") or "buy").lower()
        notional = float(o.get("notional") or req["notional"])
        if is_dry:
            placed.append({**o, "symbol": sym, "side": side, "notional": notional, "dry": True})
            continue
        try:
            res = br.market_notional(sym, side, notional)
            placed.append({**o, "symbol": sym, "side": side, "notional": notional, "order": res})
        except Exception as e:
            placed.append({**o, "symbol": sym, "side": side, "notional": notional, "error": str(e)})
    return placed

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.post("/scan/{strategy}")
async def scan(strategy: str, model: ScanRequestModel):
    try:
        body = model.model_dump() if model else {}
        orders = await _scan_bridge(strategy, body, dry=(body.get("dry") in (True, 1, "1", "true", "True")))
        return {"ok": True, "orders": orders, "strategy": strategy, "version": APP_VERSION, "time": utc_now()}
    except HTTPException:
        raise
    except Exception as e:
        log.exception("scan error")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/bars/{symbol}")
def bars(symbol: str, timeframe: str = DEFAULT_TIMEFRAME, limit: int = 200):
    try:
        out = br.get_bars(symbol.upper(), timeframe=timeframe, limit=limit)
        return {"ok": True, "symbol": symbol.upper(), "timeframe": timeframe, "limit": limit, "bars": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/price/{symbol}")
def price(symbol: str):
    try:
        p = br.last_price(symbol.upper())
        return {"ok": True, "symbol": symbol.upper(), "price": p}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/positions")
def positions():
    try:
        pos = br.positions()
        return {"ok": True, "positions": pos}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders")
def orders():
    try:
        out = br.orders()
        return {"ok": True, "orders": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@app.get("/fills")
def fills():
    try:
        data = br.trades_history(20)
        return {"ok": True, **data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/order/market")
async def order_market(request: Request):
    try:
        body = await request.json()
        symbol = (body.get("symbol") or "BTCUSD").upper()
        side = (body.get("side") or "buy").lower()
        notional = float(body.get("notional") or DEFAULT_NOTIONAL)
        if not TRADING_ENABLED:
            return {"ok": True, "dry": True, "symbol": symbol, "side": side, "notional": notional}
        res = br.market_notional(symbol, side, notional)
        return {"ok": True, "order": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# Dashboard HTML (expanded; placeholders replaced at runtime)
# -----------------------------------------------------------------------------
DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>{SERVICE_NAME} — Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
:root {
  --bg:#0b0d10; --card:#11161a; --ink:#e6edf3; --muted:#9fb0c0; --border:#1e242c;
  --accent:#8ab4ff; --ok:#6bdc6b; --warn:#ffcf5a; --err:#ff7d7d;
}
* { box-sizing:border-box; }
body { font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif; background:var(--bg); color:var(--ink); margin:0; }
.header { display:flex; align-items:center; justify-content:space-between; padding:12px 16px; border-bottom:1px solid #222; }
.badge { padding:3px 8px; border-radius:12px; font-size:12px; border:1px solid var(--border); background:#0f141a; }
.badge.kraken { background:#163; color:#b5f5b5; border-color:#1e5033; }
.badge.alpaca { background:#322; color:#f5b5b5; border-color:#503333; }
.grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(330px, 1fr)); grid-gap:12px; padding:12px; }
.card { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:12px; }
h2 { margin:0 0 8px 0; font-size:16px; }
label { font-size:12px; color:var(--muted); }
input, select, button, textarea { font:inherit; background:#0b1117; color:var(--ink); border:1px solid var(--border); border-radius:8px; padding:8px; }
button { cursor:pointer; }
a { color:var(--accent); text-decoration:none; margin-left:8px; }
a:hover { text-decoration:underline; }
pre { background:#0b1117; padding:10px; border-radius:8px; overflow:auto; }
.table { width:100%; border-collapse:collapse; font-size:13px; }
.table th, .table td { border-bottom:1px solid var(--border); padding:6px 8px; text-align:left; vertical-align:middle; }
.kv { display:grid; grid-template-columns:160px 1fr; grid-gap:6px; font-size:13px; align-items:center; }
.mono { font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace; }
.small { font-size:12px; color:var(--muted); }
hr { border:none; border-top:1px solid var(--border); margin:8px 0; }
svg.spark { width:120px; height:28px; }
svg.spark path.line { fill:none; stroke:var(--accent); stroke-width:1.5; }
svg.spark rect.bg { fill:#0b1117; }
svg.spark path.fill { fill:rgba(138,180,255,0.12); stroke:none; }
</style>
</head>
<body>
<div class="header">
  <div>
    <strong>{SERVICE_NAME}</strong>
    <span class="badge {BROKER_BADGE}">broker: {BROKER_TEXT}</span>
    <span class="badge">v{APP_VERSION}</span>
  </div>
  <div class="small">Now: <span id="now"></span></div>
</div>

<div class="grid">

  <div class="card">
    <h2>Service</h2>
    <div class="kv">
      <div>Health</div>
      <div>
        <button onclick="callJson('/health')">GET /health</button>
        <a class="small" href="/health" target="_blank" rel="noopener">open</a>
      </div>

      <div>Broker</div>
      <div>
        <button onclick="callJson('/diag/broker')">GET /diag/broker</button>
        <a class="small" href="/diag/broker" target="_blank" rel="noopener">open</a>
      </div>

      <div>Version</div>
      <div>
        <button onclick="callJson('/version')">GET /version</button>
        <a class="small" href="/version" target="_blank" rel="noopener">open</a>
      </div>

      <div>Config</div>
      <div>
        <button onclick="loadConfig()">GET /config</button>
        <a class="small" href="/config" target="_blank" rel="noopener">open</a>
      </div>
      <div>
        <button onclick="callJson('/fills')">GET /fills</button>
        <a class="small" href="/fills" target="_blank" rel="noopener">open</a>
      </div>
    </div>
    <hr/>
    <div class="small">Symbols: <span id="cfg_symbols" class="mono"></span></div>
    <div class="small">Strategies: <span id="cfg_strats" class="mono"></span></div>
  </div>

  <div class="card">
    <h2>Quick Scan (dry run)</h2>
    <div style="display:grid; grid-template-columns:1fr 1fr; grid-gap:8px;">
      <div>
        <label>Strategy</label>
        <select id="scan_strat">
          <option>c1</option><option>c2</option><option>c3</option>
          <option>c4</option><option>c5</option><option>c6</option>
        </select>
      </div>
      <div>
        <label>Timeframe</label>
        <input id="scan_tf" value="5Min"/>
      </div>
      <div>
        <label>Symbols (comma sep)</label>
        <input id="scan_syms" placeholder="BTCUSD,ETHUSD"/>
      </div>
      <div>
        <label>Notional (USD)</label>
        <input id="scan_notional" value="25"/>
      </div>
      <div>
        <label>Limit (bars)</label>
        <input id="scan_limit" value="300"/>
      </div>
      <div style="display:flex; align-items:flex-end;">
        <button onclick="runScan()">POST /scan&lt;strat&gt;</button>
      </div>
    </div>
    <hr/>
    <pre id="scan_out" class="mono small">// orders will appear here</pre>
  </div>

  <div class="card">
    <h2>Live Prices <span class="small">(30s auto-refresh)</span></h2>
    <div class="small">From /price/&lt;symbol&gt; for each configured symbol.</div>
    <table class="table" id="px_table">
      <thead>
        <tr><th>Symbol</th><th>Price</th><th>Spark</th><th>Updated</th></tr>
      </thead>
      <tbody id="px_tbody"></tbody>
    </table>
    <div style="margin-top:8px;">
      <button onclick="refreshPrices(true)">Refresh now</button>
    </div>
  </div>

  <div class="card">
    <h2>Price & Bars</h2>
    <div style="display:grid; grid-template-columns:1fr 1fr; grid-gap:8px;">
      <div>
        <label>Symbol</label>
        <input id="bars_sym" value="BTCUSD"/>
      </div>
      <div>
        <label>Timeframe</label>
        <input id="bars_tf" value="5Min"/>
      </div>
      <div>
        <label>Limit</label>
        <input id="bars_limit" value="60"/>
      </div>
      <div style="display:flex; align-items:flex-end;">
        <button onclick="fetchBars()">GET /bars&lt;symbol&gt;</button>
        <a class="small" href="/bars/BTCUSD?timeframe=5Min&limit=60" target="_blank" rel="noopener">open</a>
      </div>
    </div>
    <hr/>
    <div style="display:flex; gap:8px;">
      <button onclick="fetchPrice()">GET /price&lt;symbol&gt;</button>
      <a class="small" href="/price/BTCUSD" target="_blank" rel="noopener">open</a>
      <button onclick="callJson('/orders')">GET /orders</button>
      <a class="small" href="/orders" target="_blank" rel="noopener">open</a>
      <button onclick="callJson('/positions')">GET /positions</button>
      <a class="small" href="/positions" target="_blank" rel="noopener">open</a>
    </div>
    <hr/>
    <pre id="bars_out" class="mono small">// bars/prices will appear here</pre>
  </div>

  <div class="card">
    <h2>Place Market Order</h2>
    <div class="small">Live trading must be enabled (TRADING_ENABLED & KRAKEN_TRADING).</div>
    <div style="display:grid; grid-template-columns:1fr 1fr; grid-gap:8px; margin-top:6px;">
      <div>
        <label>Symbol</label>
        <input id="ord_sym" value="BTCUSD"/>
      </div>
      <div>
        <label>Side</label>
        <select id="ord_side"><option>buy</option><option>sell</option></select>
      </div>
      <div>
        <label>Notional (USD)</label>
        <input id="ord_notional" value="25"/>
      </div>
      <div style="display:flex; align-items:flex-end;">
        <button onclick="placeOrder()">POST /order/market</button>
      </div>
    </div>
    <hr/>
    <pre id="ord_out" class="mono small">// order result will appear here</pre>
  </div>

  <div class="card">
    <h2>Scheduler</h2>
    <div style="display:flex; gap:8px;">
      <button onclick="callJson('/scheduler/start')">/scheduler/start</button>
      <button onclick="callJson('/scheduler/stop')">/scheduler/stop</button>
    </div>
    <hr/>
    <pre id="sched_out" class="mono small">// scheduler responses here</pre>
  </div>

  <div class="card">
    <h2>Console</h2>
    <pre id="console" class="mono small">// responses will stream here</pre>
  </div>

</div>

<script>
function nowISO() { return new Date().toISOString(); }
function setNow() { document.getElementById('now').textContent = nowISO(); }
setNow(); setInterval(setNow, 1000);

function println(id, txt) {
  const el = document.getElementById(id);
  el.textContent = (el.textContent ? el.textContent + "\\n" : "") + txt;
  el.scrollTop = el.scrollHeight;
}

async function callJson(path) {
  try {
    const r = await fetch(path);
    const j = await r.json();
    println('console', `[${nowISO()}] GET ${path}\\n` + JSON.stringify(j, null, 2));
    if (path === '/config') {
      document.getElementById('cfg_symbols').textContent = (j.SYMBOLS || []).join(',');
      document.getElementById('cfg_strats').textContent = (j.STRATEGIES || []).join(',');
      buildPriceRows(j.SYMBOLS || []);
    }
    if (path.startsWith('/scheduler')) {
      document.getElementById('sched_out').textContent = JSON.stringify(j, null, 2);
    }
    return j;
  } catch (e) {
    println('console', `[${nowISO()}] ERROR GET ${path}: ` + e);
  }
}

async function loadConfig() { return await callJson('/config'); }

async function runScan() {
  const strat = document.getElementById('scan_strat').value;
  const tf = document.getElementById('scan_tf').value;
  const syms = document.getElementById('scan_syms').value || document.getElementById('cfg_symbols').textContent;
  const notional = parseFloat(document.getElementById('scan_notional').value || '25');
  const limit = parseInt(document.getElementById('scan_limit').value || '300');
  const body = {
    symbols: (syms ? syms.split(',').map(s => s.trim()).filter(Boolean) : []),
    timeframe: tf, limit: limit, notional: notional, dry: true
  };
  try {
    const r = await fetch(`/scan/${strat}`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    document.getElementById('scan_out').textContent = JSON.stringify(j, null, 2);
    println('console', `[${nowISO()}] POST /scan/${strat}\\n` + JSON.stringify(j, null, 2));
  } catch (e) {
    println('console', `[${nowISO()}] ERROR POST /scan/${strat}: ` + e);
  }
}

async function fetchBars() {
  const sym = document.getElementById('bars_sym').value;
  const tf = document.getElementById('bars_tf').value;
  const limit = document.getElementById('bars_limit').value;
  try {
    const r = await fetch(`/bars/${sym}?timeframe=${encodeURIComponent(tf)}&limit=${encodeURIComponent(limit)}`);
    const j = await r.json();
    document.getElementById('bars_out').textContent = JSON.stringify(j, null, 2);
    println('console', `[${nowISO()}] GET /bars/${sym}\\n` + JSON.stringify(j, null, 2));
  } catch (e) {
    println('console', `[${nowISO()}] ERROR GET /bars/${sym}: ` + e);
  }
}

async function fetchPrice() {
  const sym = document.getElementById('bars_sym').value;
  try {
    const r = await fetch(`/price/${sym}`);
    const j = await r.json();
    document.getElementById('bars_out').textContent = JSON.stringify(j, null, 2);
    println('console', `[${nowISO()}] GET /price/${sym}\\n` + JSON.stringify(j, null, 2));
  } catch (e) {
    println('console', `[${nowISO()}] ERROR GET /price/${sym}: ` + e);
  }
}

/* Live Prices + Sparklines */
let PX_SYMBOLS = [];
const PX_SERIES = {};   // symbol -> array of numbers
const MAX_POINTS = 50;  // ≈25 min at 30s

function buildPriceRows(symbols) {
  PX_SYMBOLS = (symbols || []).map(s => String(s).toUpperCase());
  const tbody = document.getElementById('px_tbody');
  tbody.innerHTML = '';
  PX_SYMBOLS.forEach(sym => {
    PX_SERIES[sym] = PX_SERIES[sym] || [];
    const tr = document.createElement('tr');
    tr.innerHTML =
      `<td class="mono">${sym}</td>` +
      `<td id="px_${sym}" class="mono">—</td>` +
      `<td><svg class="spark" viewBox="0 0 120 28" id="px_svg_${sym}">` +
      `<rect class="bg" x="0" y="0" width="120" height="28"></rect>` +
      `<path class="fill" id="px_fill_${sym}" d=""></path>` +
      `<path class="line" id="px_line_${sym}" d=""></path>` +
      `</svg></td>` +
      `<td id="px_t_${sym}" class="small">—</td>`;
    tbody.appendChild(tr);
  });
}

function computePath(points, w=120, h=28, pad=2) {
  if (!points || points.length === 0) return { line:'', fill:'' };
  const n = points.length;
  const min = Math.min.apply(null, points);
  const max = Math.max.apply(null, points);
  const rng = (max - min) || 1e-9;
  const innerW = w - pad*2;
  const innerH = h - pad*2;
  const x = i => pad + (i/(n-1))*innerW;
  const y = v => pad + innerH - ((v - min)/rng)*innerH;
  let d = `M ${x(0).toFixed(2)} ${y(points[0]).toFixed(2)}`;
  for (let i = 1; i < n; i++) d += ` L ${x(i).toFixed(2)} ${y(points[i]).toFixed(2)}`;
  let df = d + ` L ${x(n-1).toFixed(2)} ${(h-pad).toFixed(2)} L ${x(0).toFixed(2)} ${(h-pad).toFixed(2)} Z`;
  return { line: d, fill: df };
}

function renderSparkline(sym) {
  const pts = PX_SERIES[sym] || [];
  const { line, fill } = computePath(pts);
  const lineEl = document.getElementById(`px_line_${sym}`);
  const fillEl = document.getElementById(`px_fill_${sym}`);
  if (lineEl) lineEl.setAttribute('d', line || '');
  if (fillEl) fillEl.setAttribute('d', fill || '');
}

async function refreshPrices(forceLog) {
  if (!PX_SYMBOLS.length) {
    await loadConfig();
  }
  for (const sym of PX_SYMBOLS) {
    try {
      const r = await fetch(`/price/${sym}`);
      const j = await r.json();
      const px = (j && j.price != null) ? Number(j.price) : null;
      if (px != null && isFinite(px)) {
        const arr = (PX_SERIES[sym] = (PX_SERIES[sym] || []));
        arr.push(px);
        if (arr.length > MAX_POINTS) arr.splice(0, arr.length - MAX_POINTS);
        renderSparkline(sym);
      }
      document.getElementById(`px_${sym}`).textContent = (px == null ? '—' : String(px));
      document.getElementById(`px_t_${sym}`).textContent = nowISO();
      if (forceLog) println('console', `[${nowISO()}] price ${sym} -> ${px}`);
    } catch (e) {
      println('console', `[${nowISO()}] ERROR price ${sym}: ` + e);
    }
  }
}

window.addEventListener('load', async () => {
  await loadConfig();
  await refreshPrices(true);
  setInterval(refreshPrices, 30000);
});

/* Orders */
async function placeOrder() {
  const sym = document.getElementById('ord_sym').value;
  const side = document.getElementById('ord_side').value;
  const notional = parseFloat(document.getElementById('ord_notional').value || '25');
  const body = { symbol: sym, side: side, notional: notional };
  try {
    const r = await fetch('/order/market', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
    const j = await r.json();
    document.getElementById('ord_out').textContent = JSON.stringify(j, null, 2);
    println('console', `[${nowISO()}] POST /order/market\\n` + JSON.stringify(j, null, 2));
  } catch (e) {
    println('console', `[${nowISO()}] ERROR POST /order/market: ` + e);
  }
}
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def root():
    broker_badge = "kraken" if USING_KRAKEN else "alpaca"
    broker_text = "kraken" if USING_KRAKEN else "alpaca"
    html = (
        DASHBOARD_HTML
        .replace("{SERVICE_NAME}", SERVICE_NAME)
        .replace("{APP_VERSION}", APP_VERSION)
        .replace("{BROKER_BADGE}", broker_badge)
        .replace("{BROKER_TEXT}", broker_text)
    )
    return HTMLResponse(content=html, status_code=200)
    
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_alias():
    return root()


# -----------------------------------------------------------------------------
# Automatic strategy scheduler (opt-in via env SCHED_ON=1)
# -----------------------------------------------------------------------------
_RUNNING = False

def _sched_config():
    """
    Read scheduler config from env on each pass so changes take effect without restart.
    """
    # strategies: explicit list or fall back to ACTIVE_STRATEGIES
    raw = os.getenv("SCHED_STRATS", "")
    strategies = [s.strip() for s in raw.split(",") if s.strip()] or list(ACTIVE_STRATEGIES)

    timeframe = os.getenv("SCHED_TIMEFRAME", DEFAULT_TIMEFRAME)
    try:
        limit = int(os.getenv("SCHED_LIMIT", str(DEFAULT_LIMIT)))
    except Exception:
        limit = DEFAULT_LIMIT

    try:
        notional = float(os.getenv("SCHED_NOTIONAL", str(DEFAULT_NOTIONAL)))
    except Exception:
        notional = DEFAULT_NOTIONAL

    try:
        sleep_s = int(os.getenv("SCHED_SLEEP", "30"))  # default 30s
    except Exception:
        sleep_s = 30

    dry_env = os.getenv("SCHED_DRY", "1").lower() in ("1", "true", "yes", "y")
    trading_flags = (
        os.getenv("TRADING_ENABLED", "0").lower() in ("1", "true", "yes", "y")
        and os.getenv("KRAKEN_TRADING", "0").lower() in ("1", "true", "yes", "y")
    )
    # if trading flags are not both enabled, force dry regardless of SCHED_DRY
    dry = dry_env or (not trading_flags)

    return {
        "strategies": strategies,
        "timeframe": timeframe,
        "limit": limit,
        "notional": notional,
        "sleep_s": sleep_s,
        "dry": dry,
        "trading_flags": trading_flags,
    }

async def _loop():
    """
    Runs forever (until /scheduler/stop). Each pass:
      - reads latest env config
      - scans allowed strategies over _CURRENT_SYMBOLS
      - places live orders when dry=False (and trading flags allow)
    """
    global _RUNNING
    _RUNNING = True
    log.info("Scheduler started (v%s, broker=%s)", APP_VERSION, ("kraken" if USING_KRAKEN else "alpaca"))
    try:
        while _RUNNING:
            cfg = _sched_config()
            syms = list(_CURRENT_SYMBOLS)  # configured symbols (from /config/universe)
            # only run strategies that are currently active and not explicitly disabled
            run_strats = [s for s in cfg["strategies"] if s in ACTIVE_STRATEGIES and s not in _DISABLED_STRATS]

            log.debug(
                "Scheduler pass: strats=%s tf=%s limit=%s notional=%s dry=%s symbols=%s",
                ",".join(run_strats), cfg["timeframe"], cfg["limit"], cfg["notional"], cfg["dry"], ",".join(syms)
            )

            for strat in run_strats:
                if not _RUNNING:
                    break
                try:
                    await _scan_bridge(
                        strat,
                        {
                            "timeframe": cfg["timeframe"],
                            "limit": cfg["limit"],
                            "notional": cfg["notional"],
                            "symbols": syms,
                        },
                        dry=cfg["dry"],
                    )
                except Exception as e:
                    log.warning("Scheduler scan error (%s): %s", strat, e)

            # sleep (interruptible by stop)
            total = max(1, int(cfg["sleep_s"]))
            for _ in range(total):
                if not _RUNNING:
                    break
                await asyncio.sleep(1)
    finally:
        log.info("Scheduler stopped")

@app.get("/scheduler/start")
async def scheduler_start():
    # Keep your original gate: require SCHED_ON to be enabled to start via API
    if os.getenv("SCHED_ON", "0").lower() not in ("1", "true", "yes", "y"):
        return {"ok": False, "why": "SCHED_ON env not enabled"}
    if _RUNNING:
        return {"ok": True, "already": True}
    asyncio.create_task(_loop())
    return {"ok": True, "started": True, "config": _sched_config()}

@app.get("/scheduler/stop")
async def scheduler_stop():
    global _RUNNING
    _RUNNING = False
    return {"ok": True, "stopping": True}
    
@app.on_event("startup")
async def _maybe_autostart_scheduler():
    if os.getenv("SCHED_ON", "0").lower() in ("1", "true", "yes", "y"):
        # fire-and-forget; the loop itself reads env each pass
        asyncio.create_task(_loop())
        log.info("Scheduler autostart: enabled by SCHED_ON")


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn  # type: ignore
    port = int(os.getenv("PORT", "10000"))
    log.info(
        "Launching Uvicorn on 0.0.0.0:%d (version %s, broker=%s, trading=%s)",
        port, APP_VERSION, ("kraken" if USING_KRAKEN else "alpaca"), TRADING_ENABLED
    )
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
