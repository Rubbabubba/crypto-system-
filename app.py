#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto System – FastAPI service (Render/Kraken)
Build: v2.0.0 (2025-10-12)

Routes
- GET  /                     -> Dashboard
- GET  /dashboard            -> alias
- GET  /health               -> Service health (+ scheduler_running)
- GET  /diag/broker          -> Active broker diagnostics
- GET  /version              -> App version
- GET  /config               -> Defaults, symbols, strategies, params
- POST /scan/{strategy}      -> Run a scan for c1..c6; optional dry=true
- GET  /bars/{symbol}        -> Recent bars
- GET  /price/{symbol}       -> Last price
- GET  /positions            -> Spot positions summary
- GET  /orders               -> Open orders
- GET  /fills                -> Recent fills (via broker)
- POST /order/market         -> Place market order by notional (USD)
Scheduler
- GET  /scheduler/start      -> Start loop (requires SCHED_ON=1)
- GET  /scheduler/stop       -> Stop loop
- GET  /scheduler/status     -> status + live config
Journal & PnL
- POST /journal/sync         -> sync journal with fills (by txid)
- GET  /journal              -> list journal rows
- GET  /pnl/summary          -> total + per-strategy + per-symbol P&L
- GET  /pnl/strategies       -> per-strategy P&L
- GET  /pnl/symbols          -> per-symbol P&L

Notes (v2.0.0)
- Kraken-first broker routing; Alpaca remains legacy fallback.
- HTML dashboard extended with live price table + sparklines and a small P&L card.
- No HTML functionality removed; all existing routes kept.
"""

from __future__ import annotations

__version__ = "2.0.0"

import asyncio
import os
import sys
import json
import math
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TypedDict
from pathlib import Path
import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Body
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
# Per-order notional (USD). For scheduler override use SCHED_NOTIONAL.
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", os.getenv("ORDER_NOTIONAL", "25")))

# Symbol universe
try:
    from universe import load_universe_from_env
    _CURRENT_SYMBOLS = load_universe_from_env()
except Exception:
    _CURRENT_SYMBOLS = ["BTCUSD", "ETHUSD", "SOLUSD", "DOGEUSD", "XRPUSD", "AVAXUSD", "LINKUSD", "BCHUSD", "LTCUSD"]

# Strategy defaults (optimized params retained)
DEFAULT_STRAT_PARAMS: Dict[str, Dict[str, Any]] = {
    "c1": {"ema_n": 20, "vwap_pull": 0.0020, "min_vol": 0.0},
    "c2": {"ema_n": 50, "exit_k": 0.997, "min_atr": 0.0},
    "c3": {"ch_n": 55, "break_k": 1.0005, "fail_k": 0.997, "min_atr": 0.0},
    "c4": {"ema_fast": 12, "ema_slow": 26, "sig": 9, "atr_n": 14, "atr_mult": 1.2, "min_vol": 0.0},
    "c5": {"lookback": 20, "band_k": 1.0010, "exit_k": 0.9990, "min_vol": 0.0},
    "c6": {"atr_n": 14, "atr_mult": 1.2, "ema_n": 20, "exit_k": 0.997, "min_vol": 0.0},
}
ACTIVE_STRATEGIES = list(DEFAULT_STRAT_PARAMS.keys())
_DISABLED_STRATS: set[str] = set()

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

def _to_list(x: Any) -> List[str]:
    if x is None: return []
    if isinstance(x, list): return [str(s).upper() for s in x]
    if isinstance(x, str): return [s.strip().upper() for s in x.split(",") if s.strip()]
    return [str(x).upper()]
    
JOURNAL_PATH = Path(os.getenv("JOURNAL_PATH", "journal.json"))

def _load_journal():
    if not JOURNAL_PATH.exists():
        return {"ok": True, "rows": [], "count": 0}
    try:
        import json
        j = json.loads(JOURNAL_PATH.read_text())
        rows = j.get("rows", [])
        return {"ok": True, "rows": rows, "count": len(rows)}
    except Exception as e:
        return {"ok": False, "why": f"read journal: {e}", "rows": [], "count": 0}

def _save_journal(rows):
    import json
    out = {"ok": True, "rows": rows, "count": len(rows)}
    JOURNAL_PATH.write_text(json.dumps(out))

# -----------------------------------------------------------------------------
# FastAPI app
# -----------------------------------------------------------------------------
app = FastAPI(title=SERVICE_NAME, version=APP_VERSION)

# -----------------------------------------------------------------------------
# Core info routes
# -----------------------------------------------------------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "service": SERVICE_NAME,
        "version": APP_VERSION,
        "broker": ("kraken" if USING_KRAKEN else "alpaca"),
        "trading": TRADING_ENABLED,
        "scheduler_running": _RUNNING,
        "time": utc_now(),
        "symbols": _CURRENT_SYMBOLS,
        "strategies": ACTIVE_STRATEGIES,
    }

@app.get("/diag/broker")
def diag_broker():
    return {"broker_module": ACTIVE_BROKER_MODULE, "using_kraken": USING_KRAKEN, "trading": TRADING_ENABLED}

@app.get("/version")
def version():
    return {"version": APP_VERSION, "time": utc_now()}

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
# Scan bridge — dispatch to the chosen strategy module
# -----------------------------------------------------------------------------
class ScanRequestModel(TypedDict, total=False):
    symbols: List[str]
    timeframe: str
    limit: int
    notional: float
    raw: Dict[str, Any]
    dry: bool

async def _scan_bridge(strat: str, req: Dict[str, Any], *, dry: Optional[bool] = None) -> Dict[str, Any]:
    strat = (strat or "").lower()
    fn = STRAT_DISPATCH.get(strat)
    if not fn:
        raise HTTPException(status_code=400, detail=f"Unknown strategy '{strat}'")

    is_dry = bool(dry) if dry is not None else (not TRADING_ENABLED)

    timeframe = req.get("timeframe") or DEFAULT_TIMEFRAME
    limit = int(req.get("limit") or DEFAULT_LIMIT)
    notional = float(req.get("notional") or DEFAULT_NOTIONAL)
    symbols = _to_list(req.get("symbols")) or _CURRENT_SYMBOLS
    raw = _merge_raw(strat, dict(req.get("raw") or {}))
    ctx = {"timeframe": timeframe, "symbols": symbols, "notional": notional}

    # Strategy returns [{"symbol","side",...}]
    orders = fn({"strategy": strat, "timeframe": timeframe, "limit": limit, "notional": notional, "symbols": symbols, "raw": raw}, ctx) or []

    placed: List[Dict[str, Any]] = []
    if not is_dry:
        for o in orders:
            sym = (o.get("symbol") or symbols[0]).upper()
            side = (o.get("side") or "buy").lower()
            notional_o = float(o.get("notional") or notional)
            try:
                res = br.market_notional(sym, side, notional_o)
                placed.append({**o, "symbol": sym, "side": side, "notional": notional_o, "order": res})
                # journal attribution
                try:
                    _journal_append({
                        "ts": int(time.time()),
                        "symbol": sym, "side": side, "notional": notional_o,
                        "strategy": strat, "txid": res.get("txid"), "descr": res.get("descr"),
                        "price": None, "vol": None, "fee": None, "cost": None, "filled_ts": None,
                    })
                except Exception as je:
                    log.warning("journal add failed: %s", je)
            except Exception as e:
                placed.append({**o, "symbol": sym, "side": side, "notional": notional_o, "error": str(e)})
    else:
        # dry run echoes actionable intents
        for o in orders:
            sym = (o.get("symbol") or symbols[0]).upper()
            side = (o.get("side") or "buy").lower()
            notional_o = float(o.get("notional") or notional)
            placed.append({**o, "symbol": sym, "side": side, "notional": notional_o, "dry": True})

    return {"ok": True, "orders": orders, "placed": placed, "strategy": strat, "version": APP_VERSION, "time": utc_now()}

@app.post("/scan/{strategy}")
async def scan(strategy: str, model: Dict[str, Any] = Body(...)):
    try:
        body = dict(model or {})
        is_dry = body.get("dry")
        res = await _scan_bridge(strategy, body, dry=(is_dry in (True, 1, "1", "true", "True")))
        return res
    except HTTPException:
        raise
    except Exception as e:
        log.exception("scan error")
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# Market data & account
# -----------------------------------------------------------------------------
@app.get("/bars/{symbol}")
def bars(symbol: str, timeframe: str = DEFAULT_TIMEFRAME, limit: int = 200):
    try:
        out = br.get_bars(symbol.upper(), timeframe=timeframe, limit=int(limit))
        return {"ok": True, "symbol": symbol.upper(), "timeframe": timeframe, "limit": int(limit), "bars": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/price/{symbol}")
def price(symbol: str):
    try:
        p = br.last_price(symbol.upper())
        return {"ok": True, "symbol": symbol.upper(), "price": float(p)}
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
        data = br.trades_history(50)
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
        # journal manual
        try:
            _journal_append({
                "ts": int(time.time()),
                "symbol": symbol, "side": side, "notional": notional,
                "strategy": "manual", "txid": res.get("txid"), "descr": res.get("descr"),
                "price": None, "vol": None, "fee": None, "cost": None, "filled_ts": None,
            })
        except Exception as je:
            log.warning("journal add (manual) failed: %s", je)
        return {"ok": True, "order": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
        
@app.post("/reconcile/fills")
async def reconcile_fills():
    """
    Backfill journal rows that are missing price/vol/fee/cost using Kraken trades history.
    Matches by txid. Idempotent.
    """
    j = _load_journal()
    if not j.get("ok"):
        return j

    rows = j["rows"]
    # Build lookup of txids that need hydration
    wanted = {r["txid"] for r in rows if r.get("txid") and (r.get("price") is None or r.get("vol") is None)}
    if not wanted:
        return {"ok": True, "updated": 0, "why": "no missing fills"}

    try:
        # Ask broker for recent fills; must include price, vol, fee, cost, time, pair, txid
        fills = br.fills()  # you already have this; returns list of dicts
    except Exception as e:
        return {"ok": False, "why": f"broker fills error: {e}"}

    by_txid = {f.get("txid"): f for f in fills if f.get("txid")}
    updated = 0
    for r in rows:
        tx = r.get("txid")
        if not tx or tx not in wanted:
            continue
        f = by_txid.get(tx)
        if not f:
            continue
        # Normalize and copy into journal row
        r["price"] = float(f.get("price") or 0.0)
        r["vol"]   = float(f.get("vol")   or 0.0)
        r["fee"]   = float(f.get("fee")   or 0.0)
        r["cost"]  = float(f.get("cost")  or 0.0)
        r["filled_ts"] = int(f.get("time") or time.time())
        updated += 1

    if updated:
        _save_journal(rows)
    return {"ok": True, "updated": updated}
    
@app.get("/pnl")
async def pnl_summary():
    """
    Compute realized P&L per strategy and overall, using FIFO matching per (strategy, symbol).
    Requires journal rows to have price/vol/fee/cost.
    """
    j = _load_journal()
    if not j.get("ok"):
        return j
    rows = [r for r in j["rows"] if r.get("price") is not None and r.get("vol") is not None]

    # Group by (strategy, symbol), then FIFO match buys to sells
    from collections import defaultdict, deque

    groups = defaultdict(list)
    for r in rows:
        key = (r.get("strategy","?"), r.get("symbol","?"))
        groups[key].append(r)
    # sort by filled_ts/ts for stable matching
    for key in groups:
        groups[key].sort(key=lambda x: (x.get("filled_ts") or x.get("ts") or 0))

    report = {}
    total_real = 0.0
    total_fees = 0.0
    total_trades = 0
    total_wins = 0

    for key, gr in groups.items():
        buys = deque()
        realized = 0.0
        fees = 0.0
        wins = 0
        trades = 0

        for r in gr:
            side = r.get("side")
            price = float(r["price"])
            vol   = float(r["vol"])
            fee   = float(r.get("fee") or 0.0)
            cost  = float(r.get("cost") or 0.0)
            ts    = r.get("filled_ts") or r.get("ts") or 0
            if side == "buy":
                buys.append({"px": price, "vol": vol, "fee": fee, "ts": ts})
            elif side == "sell":
                # match against open buys FIFO
                sell_vol = vol
                sell_px  = price
                trade_pnl = 0.0
                while sell_vol > 1e-12 and buys:
                    lot = buys[0]
                    use = min(sell_vol, lot["vol"])
                    trade_pnl += (sell_px - lot["px"]) * use
                    lot["vol"] -= use
                    sell_vol  -= use
                    if lot["vol"] <= 1e-12:
                        buys.popleft()
                # realized P&L for this sell (fees subtract)
                realized += trade_pnl - fee
                trades += 1
                if trade_pnl > 0:
                    wins += 1
            # accumulate fees always
            fees += fee

        strat, sym = key
        report.setdefault(strat, [])
        report[strat].append({
            "symbol": sym,
            "realized": round(realized, 2),
            "fees": round(fees, 2),
            "trades": trades,
            "win_rate": (round(100.0 * wins / trades, 1) if trades else 0.0),
        })
        total_real += realized
        total_fees += fees
        total_trades += trades
        total_wins += wins

    overall = {
        "realized": round(total_real, 2),
        "fees": round(total_fees, 2),
        "trades": total_trades,
        "win_rate": (round(100.0 * total_wins / total_trades, 1) if total_trades else 0.0),
    }
    return {"ok": True, "by_strategy": report, "overall": overall}

# -----------------------------------------------------------------------------
# Dashboard HTML (placeholders replaced at runtime; no f-strings)
# Adds a small P&L card using /pnl/summary
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
svg.spark { width:120px; height:28; }
svg.spark path.line { fill:none; stroke:var(--accent); stroke-width:1.5; }
svg.spark rect.bg { fill:#0b1117; }
svg.spark path.fill { fill:rgba(138,180,255,0.12); stroke:none; }
.good { color: var(--ok); } .bad { color: var(--err); }
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

      <div>Fills</div>
      <div>
        <button onclick="callJson('/fills')">GET /fills</button>
        <a class="small" href="/fills" target="_blank" rel="noopener">open</a>
      </div>

      <div>Scheduler</div>
      <div>
        <button onclick="callJson('/scheduler/status')">GET /scheduler/status</button>
        <a class="small" href="/scheduler/status" target="_blank" rel="noopener">open</a>
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
      <button onclick="callJson('/scheduler/status')">/scheduler/status</button>
    </div>
    <hr/>
    <pre id="sched_out" class="mono small">// scheduler responses here</pre>
  </div>

  <div class="card">
    <h2>P&amp;L</h2>
    <div class="small">Realized + Unrealized (MTM) with fees; live from /pnl/summary</div>
    <div id="pnl_time" class="small"></div>
    <table class="table" id="pnl_strat_tbl">
      <thead><tr><th>Strategy</th><th>Realized</th><th>Unrealized</th><th>Fees</th><th>Equity</th></tr></thead>
      <tbody></tbody>
    </table>
    <div class="small" style="margin-top:6px;">Total: <span id="pnl_total" class="mono"></span></div>
    <div style="margin-top:8px;">
      <button onclick="refreshPNL()">Refresh P&amp;L</button>
      <button onclick="callJson('/journal/sync')">Sync Fills</button>
      <a class="small" href="/pnl/summary" target="_blank" rel="noopener">open</a>
      <a class="small" href="/journal" target="_blank" rel="noopener">journal</a>
    </div>
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

/* Quick scan */
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

async function refreshPnl() {
  try {
    const r = await fetch('/pnl');
    const j = await r.json();
    if (!j.ok) return;
    const o = j.overall || {};
    const by = j.by_strategy || {};
    let rows = '';
    for (const strat of Object.keys(by).sort()) {
      const realized = by[strat].reduce((a,b)=>a+(b.realized||0),0);
      const fees = by[strat].reduce((a,b)=>a+(b.fees||0),0);
      rows += `<tr><td>${strat.toUpperCase()}</td><td>$${realized.toFixed(2)}</td><td>$${fees.toFixed(2)}</td></tr>`;
    }
    const html = `
      <div class="card">
        <h3>P&L (Realized)</h3>
        <p><b>Total:</b> $${(o.realized||0).toFixed(2)} &nbsp; <b>Fees:</b> $${(o.fees||0).toFixed(2)} &nbsp; <b>Win%:</b> ${(o.win_rate||0).toFixed(1)}%</p>
        <table><thead><tr><th>Strategy</th><th>P&L</th><th>Fees</th></tr></thead><tbody>${rows}</tbody></table>
      </div>`;
    document.getElementById('pnlCard').innerHTML = html;
  } catch (e) { /* ignore */ }
}
setInterval(refreshPnl, 5000);
refreshPnl();


/* Bars & price */
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
  setInterval(refreshPNL, 30000);
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

/* P&L card */
function fmt(x){ const v = Number(x||0); const s = (v>=0?'+':''); return s + (Math.round(v*100)/100).toFixed(2); }
async function refreshPNL() {
  try {
    const r = await fetch('/pnl/summary');
    const j = await r.json();
    const tbody = document.querySelector('#pnl_strat_tbl tbody');
    tbody.innerHTML = '';
    (j.per_strategy || []).forEach(row => {
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${row.strategy}</td>`
        + `<td class="mono ${row.realized>=0?'good':'bad'}">${fmt(row.realized)}</td>`
        + `<td class="mono ${row.unrealized>=0?'good':'bad'}">${fmt(row.unrealized)}</td>`
        + `<td class="mono">${fmt(-row.fees)}</td>`
        + `<td class="mono ${row.equity>=0?'good':'bad'}">${fmt(row.equity)}</td>`;
      tbody.appendChild(tr);
    });
    const t = j.total || {};
    document.getElementById('pnl_total').textContent =
      `Realized ${fmt(t.realized)}  |  Unrealized ${fmt(t.unrealized)}  |  Fees ${fmt(-(t.fees||0))}  |  Equity ${fmt(t.equity)}`;
    document.getElementById('pnl_time').textContent = 'As of ' + (j.time || new Date().toISOString());
    println('console', `[${nowISO()}] GET /pnl/summary\\n` + JSON.stringify(j, null, 2));
  } catch (e) {
    println('console', `[${nowISO()}] ERROR /pnl/summary: ` + e);
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
# Scheduler (auto, env-driven; INFO logs per pass)
# -----------------------------------------------------------------------------
_RUNNING = False

def _sched_config() -> Dict[str, Any]:
    raw = os.getenv("SCHED_STRATS", "")
    strategies = [s.strip().lower() for s in raw.split(",") if s.strip()] or [s.lower() for s in ACTIVE_STRATEGIES]
    timeframe = os.getenv("SCHED_TIMEFRAME", DEFAULT_TIMEFRAME)
    try: limit = int(os.getenv("SCHED_LIMIT", str(DEFAULT_LIMIT)))
    except: limit = DEFAULT_LIMIT
    try: notional = float(os.getenv("SCHED_NOTIONAL", str(DEFAULT_NOTIONAL)))
    except: notional = DEFAULT_NOTIONAL
    try: sleep_s = int(os.getenv("SCHED_SLEEP", "30"))
    except: sleep_s = 30
    dry_env = os.getenv("SCHED_DRY", "1").lower() in ("1", "true", "yes", "y")
    trading_flags = TRADING_ENABLED and (os.getenv("KRAKEN_TRADING", "0").lower() in ("1","true","yes","y"))
    dry = dry_env or (not trading_flags)
    return {
        "strategies": strategies, "timeframe": timeframe, "limit": limit, "notional": notional,
        "sleep_s": sleep_s, "dry": dry, "trading_flags": trading_flags
    }

async def _loop():
    global _RUNNING
    _RUNNING = True
    log.info("Scheduler started (v%s, broker=%s)", APP_VERSION, ("kraken" if USING_KRAKEN else "alpaca"))
    try:
        while _RUNNING:
            cfg = _sched_config()
            syms = list(_CURRENT_SYMBOLS)
            run_strats = [s for s in cfg["strategies"] if s in [x.lower() for x in ACTIVE_STRATEGIES] and s not in [x.lower() for x in _DISABLED_STRATS]]
            log.info("Scheduler pass: strats=%s tf=%s limit=%s notional=%s dry=%s symbols=%s",
                     ",".join(run_strats), cfg["timeframe"], cfg["limit"], cfg["notional"], cfg["dry"], ",".join(syms))
            for strat in run_strats:
                if not _RUNNING: break
                try:
                    await _scan_bridge(strat, {
                        "timeframe": cfg["timeframe"], "limit": cfg["limit"],
                        "notional": cfg["notional"], "symbols": syms
                    }, dry=cfg["dry"])
                except Exception as e:
                    log.warning("Scheduler scan error (%s): %s", strat, e)
            total = max(1, int(cfg["sleep_s"]))
            for _ in range(total):
                if not _RUNNING: break
                await asyncio.sleep(1)
    finally:
        log.info("Scheduler stopped")

@app.get("/scheduler/start")
async def scheduler_start():
    if os.getenv("SCHED_ON", "0").lower() not in ("1","true","yes","y"):
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

@app.get("/scheduler/status")
async def scheduler_status():
    return {"ok": True, "running": _RUNNING, "config": _sched_config()}

@app.on_event("startup")
async def _maybe_autostart_scheduler():
    if os.getenv("SCHED_ON", "0").lower() in ("1","true","yes","y"):
        asyncio.create_task(_loop())
        log.info("Scheduler autostart: enabled by SCHED_ON")

# -----------------------------------------------------------------------------
# Journal & P&L
# -----------------------------------------------------------------------------
_JOURNAL_LOCK = threading.Lock()
_JOURNAL_PATH = os.getenv("JOURNAL_PATH", "./journal_v2.jsonl")
_JOURNAL: List[Dict[str, Any]] = []

def _journal_load():
    global _JOURNAL
    try:
        rows = []
        with open(_JOURNAL_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                rows.append(json.loads(line))
        with _JOURNAL_LOCK:
            _JOURNAL = rows
        log.info("journal: loaded %d rows", len(rows))
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning("journal load error: %s", e)

def _journal_append(row: dict):
    with _JOURNAL_LOCK:
        _JOURNAL.append(row)
        try:
            with open(_JOURNAL_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
        except Exception as e:
            log.warning("journal persist error: %s", e)

@app.on_event("startup")
async def _journal_on_start():
    _journal_load()

def _sync_journal_with_fills(max_trades: int = 200) -> dict:
    try:
        fills = br.trades_history(max_trades)
        if not fills.get("ok"):
            return {"ok": False, "error": fills.get("error", "unknown")}
        by_txid = {t["txid"]: t for t in fills.get("trades", []) if t.get("txid")}
        updated = 0
        with _JOURNAL_LOCK:
            for row in _JOURNAL:
                tx = row.get("txid")
                if tx and tx in by_txid:
                    t = by_txid[tx]
                    sym_guess = t.get("pair") or ""
                    try:
                        from symbol_map import from_kraken as _from
                        sym_ui = (_from(sym_guess) or sym_guess).upper()
                    except Exception:
                        sym_ui = sym_guess.upper()
                    row["symbol"] = row.get("symbol") or sym_ui
                    row["price"] = float(t.get("price") or 0.0)
                    row["vol"] = float(t.get("vol") or 0.0)
                    row["fee"] = float(t.get("fee") or 0.0)
                    row["cost"] = float(t.get("cost") or 0.0)
                    row["filled_ts"] = float(t.get("time") or 0.0)
                    updated += 1
        return {"ok": True, "updated": updated, "count": len(_JOURNAL)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.post("/journal/sync")
def journal_sync():
    return _sync_journal_with_fills(200)

@app.get("/journal")
def journal_list(limit: int = 100):
    with _JOURNAL_LOCK:
        rows = list(_JOURNAL[-int(limit):])
    return {"ok": True, "rows": rows, "count": len(_JOURNAL)}

def _prices_for(symbols: List[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for s in symbols:
        try:
            out[s] = float(br.last_price(s))
        except Exception:
            out[s] = 0.0
    return out

def _pnl_calc(now_prices: Dict[str, float]) -> Dict[str, Any]:
    from collections import defaultdict, deque
    with _JOURNAL_LOCK:
        trades = [r for r in _JOURNAL if r.get("price") and r.get("vol")]
    trades.sort(key=lambda r: r.get("filled_ts") or r.get("ts") or 0)

    lots = defaultdict(lambda: deque())  # (strategy,symbol) -> deque of [qty, cost_px]
    stats = defaultdict(lambda: {"realized": 0.0, "fees": 0.0, "qty": 0.0, "avg_cost": 0.0})

    for r in trades:
        strat = r.get("strategy") or "unknown"
        sym = (r.get("symbol") or "").upper()
        side = r.get("side")
        px = float(r.get("price") or 0.0)
        vol = float(r.get("vol") or 0.0)
        fee = float(r.get("fee") or 0.0)
        key = (strat, sym)
        if side == "buy":
            lots[key].append([vol, px])
            st = stats[key]
            st["qty"] += vol
            prev_qty = max(1e-9, st["qty"] - vol)
            st["avg_cost"] = ((st["avg_cost"] * prev_qty) + px * vol) / max(1e-9, st["qty"])
            st["fees"] += fee
        elif side == "sell":
            remain = vol
            realized = 0.0
            while remain > 1e-12 and lots[key]:
                q, cpx = lots[key][0]
                take = min(q, remain)
                realized += (px - cpx) * take
                q -= take; remain -= take
                if q <= 1e-12: lots[key].popleft()
                else: lots[key][0][0] = q
            st = stats[key]
            st["qty"] -= vol
            st["realized"] += realized
            st["fees"] += fee

    out_strat, out_sym = {}, {}
    total = {"realized": 0.0, "unrealized": 0.0, "fees": 0.0, "equity": 0.0}
    for key, st in stats.items():
        strat, sym = key
        mkt = float(now_prices.get(sym) or 0.0)
        unreal = 0.0
        if mkt > 0 and lots[key]:
            for q, cpx in lots[key]:
                unreal += (mkt - cpx) * q
        equity = st["realized"] + unreal - st["fees"]

        # aggregate per-strategy
        srow = out_strat.get(strat, {"strategy": strat, "realized": 0.0, "unrealized": 0.0, "fees": 0.0, "equity": 0.0})
        srow["realized"] += st["realized"]; srow["unrealized"] += unreal; srow["fees"] += st["fees"]; srow["equity"] += equity
        out_strat[strat] = srow

        # aggregate per-symbol
        yrow = out_sym.get(sym, {"symbol": sym, "realized": 0.0, "unrealized": 0.0, "fees": 0.0, "equity": 0.0})
        yrow["realized"] += st["realized"]; yrow["unrealized"] += unreal; yrow["fees"] += st["fees"]; yrow["equity"] += equity
        out_sym[sym] = yrow

        total["realized"] += st["realized"]; total["unrealized"] += unreal; total["fees"] += st["fees"]; total["equity"] += equity

    return {
        "ok": True,
        "time": utc_now(),
        "total": total,
        "per_strategy": sorted(out_strat.values(), key=lambda r: r["equity"], reverse=True),
        "per_symbol": sorted(out_sym.values(), key=lambda r: r["equity"], reverse=True),
    }

@app.get("/pnl/summary")
def pnl_summary():
    try:
        # collect symbols that appear in journal; fallback to configured symbols
        with _JOURNAL_LOCK:
            syms = sorted({(r.get("symbol") or "").upper() for r in _JOURNAL if r.get("symbol")}) or list(_CURRENT_SYMBOLS)
        prices = _prices_for(syms)
        return _pnl_calc(prices)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pnl/strategies")
def pnl_strategies():
    return pnl_summary().get("per_strategy", [])

@app.get("/pnl/symbols")
def pnl_symbols():
    return pnl_summary().get("per_symbol", [])

@app.post("/pnl/reset")
def pnl_reset():
    try:
        with _JOURNAL_LOCK:
            _JOURNAL.clear()
        try:
            if os.path.exists(_JOURNAL_PATH):
                os.remove(_JOURNAL_PATH)
        except Exception:
            pass
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn  # type: ignore
    port = int(os.getenv("PORT", "10000"))
    log.info(
        "Launching Uvicorn on 0.0.0.0:%d (version %s, broker=%s, trading=%s)",
        port, __version__, ("kraken" if USING_KRAKEN else "alpaca"), TRADING_ENABLED
    )
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
