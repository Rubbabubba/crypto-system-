#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto System – FastAPI service (Render-safe)
Version: 2025.10.11-crypto-v3g

What’s new in v3g vs baseline:
- Strong broker selection (default Kraken; auto-detect via KRAKEN_*).
- Live trading requires broker-specific flag (KRAKEN_TRADING=1).
- New /diag/broker endpoint to prove we are NOT hitting Alpaca.
- Loud startup logging; scheduler banner includes broker name.
- Full dashboard HTML retained.
"""

import asyncio
import csv
import io
import json
import math
import os
import random
import statistics as stats
import sys
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd  # type: ignore
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from pydantic import BaseModel

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
import logging

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

# -----------------------------------------------------------------------------
# Broker selection (Kraken-first)
# -----------------------------------------------------------------------------
BROKER = os.getenv("BROKER", "kraken").lower()
USING_KRAKEN = BROKER == "kraken" or (
    os.getenv("KRAKEN_KEY") and os.getenv("KRAKEN_SECRET")
)
if USING_KRAKEN:
    import broker_kraken as br  # your Kraken adapter
    ACTIVE_BROKER_MODULE = "broker_kraken"
else:
    import broker as br  # Alpaca adapter (legacy)
    ACTIVE_BROKER_MODULE = "broker"

# Live trading enablement – broker-specific
TRADING_ENABLED_BASE = os.getenv("TRADING_ENABLED", "1") in ("1", "true", "True")
if USING_KRAKEN:
    TRADING_ENABLED = TRADING_ENABLED_BASE and (os.getenv("KRAKEN_TRADING", "0") in ("1", "true", "True"))
else:
    TRADING_ENABLED = TRADING_ENABLED_BASE and (os.getenv("ALPACA_TRADING", "0") in ("1", "true", "True"))

# -----------------------------------------------------------------------------
# Globals & defaults
# -----------------------------------------------------------------------------
APP_VERSION = os.getenv("APP_VERSION", "2025.10.06-baseline-a")
SERVICE_NAME = os.getenv("SERVICE_NAME", "Crypto System")

# Safe, non-self-referential defaults (avoid NameError)
DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "5Min")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "300"))
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", os.getenv("ORDER_NOTIONAL", "25")))

DEFAULT_UNIVERSE = os.getenv(
    "DEFAULT_UNIVERSE",
    "BTC/USD,ETH/USD,SOL/USD,ADA/USD,DOGE/USD,XRP/USD,AVAX/USD,LTC/USD,LINK/USD,DOT/USD,MATIC/USD,BCH/USD"
)

SCHEDULER_INTERVAL_SEC = int(os.getenv("SCHEDULER_INTERVAL_SEC", "60"))
RECENT_ORDERS_LIMIT = int(os.getenv("RECENT_ORDERS_LIMIT", "200"))
MAX_ORDERS_RING = int(os.getenv("MAX_ORDERS_RING", "1500"))

# -----------------------------------------------------------------------------
# Strategy book adapter
# -----------------------------------------------------------------------------
from strategy_book import StrategyBook  # local helper that dispatches to c1..c6

# Active strategies
STRATEGY_LIST = os.getenv("STRATEGY_LIST", "c1,c2,c3,c4,c5,c6")
ACTIVE_STRATEGIES = [s.strip() for s in STRATEGY_LIST.split(",") if s.strip()]

# Symbol universe
_CURRENT_SYMBOLS: List[str] = [s.strip() for s in DEFAULT_UNIVERSE.split(",") if s.strip()]

# -----------------------------------------------------------------------------
# Presentation helpers / math
# -----------------------------------------------------------------------------
def _fmt(n: Any) -> str:
    try:
        f = float(n)
    except Exception:
        return str(n)
    if abs(f) >= 1000:
        return f"{f:,.2f}"
    if abs(f) >= 1:
        return f"{f:.2f}"
    return f"{f:.6f}"

def _pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return 100.0 * (a / b - 1.0)

def _sym_to_slash(s: str) -> str:
    s = (s or "").replace("-", "/").replace("_", "/").upper()
    if "/" not in s and len(s) > 3 and s.endswith("USD"):
        return f"{s[:-3]}/USD"
    return s

def _median(xs: List[float]) -> float:
    xs = [x for x in xs if x is not None]
    return stats.median(xs) if xs else 0.0

def _spread_bps(px: float, best_bid: Optional[float]=None, best_ask: Optional[float]=None) -> float:
    # best-effort spread estimate using last price if no book available
    if best_bid and best_ask and best_ask > 0 and best_bid > 0:
        return 1e4 * (best_ask - best_bid) / ((best_ask + best_bid) / 2.0)
    if px <= 0:
        return 0.0
    # assume 10 bps typical crypto spread if unknown (tiny)
    return 10.0

def _price_above_ema_fast(symbol: str, timeframe: str, closes: List[float]) -> bool:
    # super light EMA(20) check for buy guard
    if len(closes) < 20:
        return True
    k = 2.0 / (20.0 + 1.0)
    ema = closes[-20]
    for v in closes[-19:]:
        ema = v * k + ema * (1.0 - k)
    last = closes[-1]
    return last >= ema

# -----------------------------------------------------------------------------
# Risk & guard rails
# -----------------------------------------------------------------------------
GUARDS = {
    "min_edge_bps": 4.0,
    "min_edge_vs_spread_x": 1.2,
    "breakeven_trigger_bps": 8.0,
    "time_bail_bars": 8,
    "tp_target_bps": 12.0,
    "no_cross_exit": True,
}

# ---- Tuned per-strategy default params (merged into each scan call) ----
DEFAULT_STRAT_PARAMS = {
    "c1": {"ema_n": 20, "vwap_pull": 0.0020, "min_vol": 0.0},
    "c2": {"ema_n": 50, "exit_k": 0.997, "min_atr": 0.0},
    "c3": {"ch_n": 55, "break_k": 1.0005, "fail_k": 0.997, "min_atr": 0.0},
    "c4": {"ema_fast": 12, "ema_slow": 26, "atr_k": 2.0, "min_atr": 0.0},
    "c5": {"band_n": 20, "band_k": 1.0005, "exit_k": 0.998, "min_atr": 0.0},
    "c6": {"ema_n": 34, "exit_k": 0.998, "min_atr": 0.0},
}

_DISABLED_STRATS: set[str] = set()
_DAILY_PNL: dict[str, float] = defaultdict(float)
_DAILY_TOTAL: float = 0.0
_LAST_DAY: Optional[str] = None
DAILY_STOP_GLOBAL_USD = float(os.getenv("DAILY_STOP_GLOBAL_USD", "-100.0"))
DAILY_STOP_PER_STRAT_USD = float(os.getenv("DAILY_STOP_PER_STRAT_USD", "-50.0"))

def _daykey() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _roll_day_if_needed():
    global _LAST_DAY, _DAILY_PNL, _DAILY_TOTAL
    k = _daykey()
    if _LAST_DAY != k:
        _DAILY_PNL = defaultdict(float)
        _DAILY_TOTAL = 0.0
        _LAST_DAY = k
        log.info("Rolled P&L day to %s", k)

# -----------------------------------------------------------------------------
# App
# -----------------------------------------------------------------------------
app = FastAPI(title=SERVICE_NAME)

# -----------------------------------------------------------------------------
# In-memory rings
# -----------------------------------------------------------------------------
_orders_ring: deque = deque(maxlen=MAX_ORDERS_RING)  # orders and intents for dashboard
_attribution: Dict[str, float] = defaultdict(float)  # by strategy
_equity_cache: Dict[str, float] = {}  # last prices per symbol

# -----------------------------------------------------------------------------
# Startup banner
# -----------------------------------------------------------------------------
log.info(
    "Startup: version=%s broker=%s module=%s trading_enabled_base=%s trading_enabled_final=%s",
    APP_VERSION, ("kraken" if USING_KRAKEN else "alpaca"), ACTIVE_BROKER_MODULE, TRADING_ENABLED_BASE, TRADING_ENABLED
)

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/healthz")
def healthz():
    return {"ok": True, "version": APP_VERSION, "time": datetime.now(timezone.utc).isoformat()}

@app.get("/diag/broker")
def diag_broker():
    return {
        "ok": True,
        "info": {
            "selected": ("kraken" if USING_KRAKEN else "alpaca"),
            "module": ACTIVE_BROKER_MODULE,
            "trading_enabled_base": TRADING_ENABLED_BASE,
            "broker_live_flag": os.getenv("KRAKEN_TRADING", "0") if USING_KRAKEN else os.getenv("ALPACA_TRADING", "0"),
            "trading_enabled_final": TRADING_ENABLED,
        },
        "probe": {"BTC/USD": 1},
    }

@app.get("/universe")
def get_universe():
    return {"symbols": _CURRENT_SYMBOLS, "count": len(_CURRENT_SYMBOLS)}

@app.post("/universe/set")
async def set_universe(req: Dict[str, Any]):
    syms = req.get("symbols") or []
    if isinstance(syms, str):
        syms = [s.strip() for s in syms.split(",") if s.strip()]
    if not syms:
        raise HTTPException(400, "no symbols")
    global _CURRENT_SYMBOLS
    _CURRENT_SYMBOLS = syms
    return {"ok": True, "count": len(_CURRENT_SYMBOLS)}

@app.get("/version")
def version():
    return {"version": APP_VERSION}

@app.get("/config")
def config():
    return {
        "DEFAULT_TIMEFRAME": DEFAULT_TIMEFRAME,
        "DEFAULT_LIMIT": DEFAULT_LIMIT,
        "DEFAULT_NOTIONAL": DEFAULT_NOTIONAL,
        "STRATEGY_LIST": ACTIVE_STRATEGIES,
        "TRADING_ENABLED": TRADING_ENABLED,
        "BROKER": ("kraken" if USING_KRAKEN else "alpaca"),
    }

# -----------------------------------------------------------------------------
# P&L + attribution
# -----------------------------------------------------------------------------
def _update_equity(symbols: Iterable[str]) -> None:
    try:
        mp = br.last_trade_map(list(symbols))
        for k, v in (mp or {}).items():
            _equity_cache[k] = float(v.get("price") or 0.0)
    except Exception:
        log.exception("equity update failed")

def _push_orders(strategy: str, orders: List[Dict[str, Any]]) -> None:
    ts = datetime.now(timezone.utc).isoformat()
    total_notional = 0.0
    for o in (orders or []):
        o2 = dict(o)
        o2["strategy"] = strategy
        o2["ts"] = ts
        _orders_ring.append(o2)
        total_notional += float(o2.get("notional") or 0.0)
    if total_notional:
        _attribution[strategy] += total_notional

@app.get("/orders/recent")
def orders_recent(limit: int = 50):
    items = list(_orders_ring)[-limit:]
    return list(reversed(items))

@app.get("/orders/attribution")
def orders_attr():
    return {"by_strategy": dict(_attribution), "total_notional": sum(_attribution.values())}

@app.get("/pnl/summary")
def pnl_summary():
    # Best-effort equity calc from last price * approx position qty (if any)
    try:
        poss = br.list_positions() or []
    except Exception:
        poss = []
    symbols = [p.get("symbol") for p in poss if p.get("symbol")]
    _update_equity(symbols)
    equity = 0.0
    for p in poss:
        sym = p.get("symbol"); qty = float(p.get("qty") or 0.0)
        px = _equity_cache.get(sym, 0.0)
        equity += qty * px
    return {
        "equity": equity,
        "day": _DAILY_TOTAL,
        "by_strategy": dict(_DAILY_PNL),
        "ts": datetime.now(timezone.utc).isoformat(),
    }

# -----------------------------------------------------------------------------
# Scan bridge
# -----------------------------------------------------------------------------
# --- REPLACE the function header + initial normalization lines ---
async def _scan_bridge(strat: str, req: Dict[str, Any], *args, **kwargs) -> List[Dict[str, Any]]:
    """
    Bridge a strategy scan into placed orders + attribution.
    Accepts 'dry' via keyword or positional; forces dry when TRADING_ENABLED is False.
    """
    # Accept dry either as kwarg or first positional
    dry_arg = kwargs.get("dry", None)
    if dry_arg is None and args:
        dry_arg = bool(args[0])
    is_dry = bool(dry_arg) or (not TRADING_ENABLED)

    # normalize basic request
    req = dict(req or {})
    req.setdefault("strategy", strat)
    req.setdefault("timeframe", req.get("tf") or DEFAULT_TIMEFRAME)
    req.setdefault("limit", req.get("limit") or DEFAULT_LIMIT)
    req.setdefault("notional", req.get("notional") or DEFAULT_NOTIONAL)

    syms = req.get("symbols") or _CURRENT_SYMBOLS
    if isinstance(syms, str):
        syms = [s.strip() for s in syms.split(",") if s.strip()]
    req["symbols"] = syms

    # merge tuned defaults into raw; allow req['raw'] to override defaults
    raw_in = dict(req.get("raw") or {})
    raw_merged = dict(DEFAULT_STRAT_PARAMS.get(strat, {}))
    raw_merged.update(raw_in)
    req["raw"] = raw_merged

    # force dry if trading disabled
    is_dry = dry or (not TRADING_ENABLED)

    sb = StrategyBook()
    orders = sb.scan(req, {"one": {"timeframe": req["timeframe"], "symbols": syms, "notional": req["notional"]}})

    # Basic guard: EMA/spread sanity for BUYs
    try:
        bars_map = br.get_bars(req["symbols"], timeframe=req["timeframe"], limit=60) or {}
    except Exception:
        bars_map = {}

    filtered: List[Dict[str, Any]] = []
    for o in (orders or []):
        side = (o.get("side") or o.get("order_side") or "").lower()
        sym = _sym_to_slash(o.get("symbol") or "")
        sname = (req.get("strategy") or "").lower()
        closes = [
            float(b.get("c") or b.get("close") or 0.0)
            for b in (bars_map.get(sym) or [])
            if float(b.get("c") or b.get("close") or 0.0) > 0
        ]
        px = float(o.get("price") or o.get("px") or 0.0)
        if px <= 0 and closes:
            px = closes[-1]
        edge_bps_val = float(o.get("edge_bps") or o.get("edge") or 0.0)
        spr_bps = _spread_bps(px)
        ema_ok = _price_above_ema_fast(sym, req["timeframe"], closes)
        if side == "buy":
            if not ema_ok or edge_bps_val < GUARDS["min_edge_bps"] or edge_bps_val < GUARDS["min_edge_vs_spread_x"] * spr_bps:
                log.info("guard: drop open %s by %s (ema_ok=%s edge=%.2f spread=%.2f)", sym, sname, ema_ok, edge_bps_val, spr_bps)
                continue
        filtered.append({**o, "symbol": sym, "side": side})

    if not filtered:
        return []

    # Optional execution + attribution
    placed: List[Dict[str, Any]] = []
    if not is_dry:
        for o in filtered:
            try:
                sym = _sym_to_slash(o.get("symbol") or "")
                side = (o.get("side") or "").lower()
                if side not in ("buy", "sell"):
                    continue
                cid = o.get("client_order_id") or f"{strat}-{int(time.time())}-{sym.replace('/','')}"
                notional = float(o.get("notional") or req["notional"])
                br.submit_order(symbol=sym, side=side, notional=notional, client_order_id=cid)
                placed.append({**o, "client_order_id": cid})
            except Exception:
                log.exception("submit failed")
    to_push = placed if (placed and not is_dry) else filtered
    if to_push:
        _push_orders(strat, to_push)
    return to_push

# -----------------------------------------------------------------------------
# Scheduler
# -----------------------------------------------------------------------------
_running = False

async def _scheduler_loop():
    global _running
    _running = True
    dry_flag = (not TRADING_ENABLED)
    try:
        while _running:
            _roll_day_if_needed()
            for strat in list(ACTIVE_STRATEGIES):
                if strat in _DISABLED_STRATS:
                    continue
                try:
                    orders = await _scan_bridge(
                        strat,
                        {
                            "timeframe": DEFAULT_TIMEFRAME,
                            "limit": DEFAULT_LIMIT,
                            "notional": DEFAULT_NOTIONAL,
                            "symbols": _CURRENT_SYMBOLS,
                        },
                        dry=dry_flag,
                    )
                    if orders:
                        log.info("scan %s → %d orders", strat, len(orders))
                except Exception:
                    log.exception("scan error %s", strat)
            log.info("Scheduler tick complete (dry=%s symbols=%d broker=%s) — sleeping %ss", int(dry_flag), len(_CURRENT_SYMBOLS), ("kraken" if USING_KRAKEN else "alpaca"), SCHEDULER_INTERVAL_SEC)
            await asyncio.sleep(SCHEDULER_INTERVAL_SEC)
    finally:
        _running = False

@app.post("/scheduler/start")
async def scheduler_start():
    if _running:
        return {"ok": True, "running": True}
    asyncio.create_task(_scheduler_loop())
    return {"ok": True, "running": True}

@app.post("/scheduler/stop")
async def scheduler_stop():
    global _running
    _running = False
    return {"ok": True, "running": False}

# -----------------------------------------------------------------------------
# Orders passthrough endpoints (for testing)
# -----------------------------------------------------------------------------
@app.post("/order/submit")
async def order_submit(req: Dict[str, Any]):
    sym = _sym_to_slash(req.get("symbol") or "")
    side = (req.get("side") or "").lower()
    cid = req.get("client_order_id") or f"manual-{int(time.time())}-{sym.replace('/','')}"
    notional = float(req.get("notional") or DEFAULT_NOTIONAL)
    if side not in ("buy", "sell"):
        raise HTTPException(400, "side must be buy/sell")
    if not sym:
        raise HTTPException(400, "symbol required")
    if not TRADING_ENABLED:
        return {"ok": True, "dry": True, "message": "TRADING_ENABLED=0"}
    try:
        br.submit_order(symbol=sym, side=side, notional=notional, client_order_id=cid)
        _push_orders("manual", [{"symbol": sym, "side": side, "notional": notional, "client_order_id": cid}])
        return {"ok": True, "cid": cid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/positions")
def positions():
    try:
        return br.list_positions() or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders")
def orders(status: str = "all", limit: int = 100):
    try:
        return br.list_orders(status=status, limit=limit) or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -----------------------------------------------------------------------------
# HTML (full inline page)
# -----------------------------------------------------------------------------
_DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Crypto System Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root{
      --bg:#0b1220;
      --panel:#111a2b;
      --ink:#e6edf3;
      --muted:#a6b3c2;
      --accent:#5dd4a3;
      --accent2:#66a3ff;
      --red:#ff6b6b;
      --chip:#1a2336;
      --chip-br:#26324a;
    }
    *{box-sizing:border-box}
    body{
      margin:0;
      font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji","Segoe UI Emoji";
      background: linear-gradient(160deg, #0b1220 0%, #0e172a 100%);
      color:var(--ink);
    }
    header{
      padding:20px 24px;
      border-bottom:1px solid #16233b;
      display:flex;align-items:center;gap:12px;
    }
    .badge{
      font-size:12px;
      background:var(--chip);
      border:1px solid var(--chip-br);
      color:var(--muted);
      padding:4px 8px;border-radius:999px;
    }
    main{padding:24px;max-width:1200px;margin:0 auto}
    .grid{
      display:grid;
      grid-template-columns: repeat(12, 1fr);
      gap:16px;
    }
    .card{
      background:var(--panel);
      border:1px solid #1a2740;
      border-radius:14px;
      padding:16px;
      box-shadow: 0 10px 24px rgba(0,0,0,.2);
    }
    .span-4{grid-column: span 4}
    .span-6{grid-column: span 6}
    .span-8{grid-column: span 8}
    .span-12{grid-column: span 12}
    h1{font-size:18px;margin:0}
    .muted{color:var(--muted)}
    .row{display:flex;align-items:center;justify-content:space-between;gap:12px}
    .kpi{font-size:28px;font-weight:700}
    .good{color:var(--accent)}
    .bad{color:var(--red)}
    table{width:100%;border-collapse:collapse;font-size:14px}
    th,td{padding:8px;border-bottom:1px solid #1a2740;text-align:left}
    th{color:#9db0c9;font-weight:600}
    .chips{display:flex;flex-wrap:wrap;gap:8px}
    .chip{
      background:var(--chip);
      border:1px solid var(--chip-br);
      padding:6px 10px;border-radius:999px;font-size:12px;color:#c7d2e3;
    }
    a.btn{
      display:inline-block;padding:8px 12px;border-radius:10px;text-decoration:none;
      background:var(--accent2);color:#0b1220;font-weight:700;border:1px solid #2a3f6b;
    }
    footer{padding:28px;color:var(--muted);text-align:center}
    @media (max-width: 900px){
      .span-4,.span-6,.span-8{grid-column: span 12}
    }
    code{
      background:#0d1628;border:1px solid #1a2740;padding:2px 6px;border-radius:6px
    }
  </style>
  <script>
    async function loadSummary(){
      const r = await fetch('/pnl/summary');
      const d = await r.json();
      document.getElementById('eq').textContent = Number(d.equity || 0).toFixed(2);
      document.getElementById('pnl_day').textContent = Number(d.pnl_day || 0).toFixed(2);
      document.getElementById('pnl_week').textContent = Number(d.pnl_week || 0).toFixed(2);
      document.getElementById('pnl_month').textContent = Number(d.pnl_month || 0).toFixed(2);
      document.getElementById('updated').textContent = d.updated_at ? new Date(d.updated_at).toLocaleString() : '-';
    }
    async function loadAttribution(){
      const r = await fetch('/orders/attribution');
      const d = await r.json();
      const tbody = document.getElementById('attr_body');
      tbody.innerHTML = '';
      if (d.by_strategy){
        for (const [k,v] of Object.entries(d.by_strategy)){
          const tr = document.createElement('tr');
          tr.innerHTML = `<td>${k}</td><td>${Number(v).toFixed(2)}</td>`;
          tbody.appendChild(tr);
        }
      }
      document.getElementById('attr_updated').textContent = d.updated_at ? new Date(d.updated_at).toLocaleString() : '-';
    }
    async function loadOrders(){
      const r = await fetch('/orders/recent?limit=50');
      const d = await r.json();
      const tbody = document.getElementById('orders_body');
      tbody.innerHTML = '';
      (d.orders || []).forEach(o => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${o.id ?? ''}</td>
          <td>${o.symbol ?? ''}</td>
          <td>${o.side ?? ''}</td>
          <td>${o.qty ?? ''}</td>
          <td>${o.px ?? ''}</td>
          <td>${o.strategy ?? ''}</td>
          <td>${o.pnl ?? 0}</td>
          <td>${o.ts ? new Date(o.ts*1000).toLocaleString() : ''}</td>
        `;
        tbody.appendChild(tr);
      });
    }
    async function refreshAll(){
      await Promise.all([loadSummary(), loadAttribution(), loadOrders()]);
    }
    setInterval(refreshAll, 15000);
    window.addEventListener('load', refreshAll);
  </script>
</head>
<body>
  <header class="row">
    <h1>Crypto System</h1>
    <span class="badge">Live</span>
    <span class="badge">Scheduler: <code>active</code></span>
    <div style="margin-left:auto" class="chips">
      <span class="chip">TF: <code id="tf_chip">auto</code></span>
      <span class="chip">Tick: <code>{SCHEDULE_SECONDS}s</code></span>
    </div>
  </header>

  <main>
    <div class="grid">
      <section class="card span-8">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">P&L Summary</h2>
          <a class="btn" href="#" onclick="refreshAll();return false;">Refresh</a>
        </div>
        <div class="grid" style="grid-template-columns:repeat(12,1fr);gap:12px">
          <div class="span-4">
            <div class="muted">Equity</div>
            <div class="kpi" id="eq">0.00</div>
          </div>
          <div class="span-4">
            <div class="muted">PnL (Day)</div>
            <div class="kpi good" id="pnl_day">0.00</div>
          </div>
          <div class="span-4">
            <div class="muted">PnL (Week)</div>
            <div class="kpi" id="pnl_week">0.00</div>
          </div>
          <div class="span-4">
            <div class="muted">PnL (Month)</div>
            <div class="kpi" id="pnl_month">0.00</div>
          </div>
          <div class="span-8">
            <div class="muted">Last Updated</div>
            <div id="updated" class="kpi" style="font-size:16px">-</div>
          </div>
        </div>
      </section>

      <section class="card span-4">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">Attribution</h2>
        </div>
        <table>
          <thead><tr><th>Strategy</th><th>PnL</th></tr></thead>
          <tbody id="attr_body"></tbody>
        </table>
        <div class="muted" style="margin-top:8px">Updated: <span id="attr_updated">-</span></div>
      </section>

      <section class="card span-12">
        <div class="row" style="margin-bottom:8px">
          <h2 style="margin:0;font-size:16px">Recent Orders</h2>
        </div>
        <table>
          <thead>
            <tr>
              <th>ID</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Price</th>
              <th>Strategy</th><th>PnL</th><th>Time</th>
            </tr>
          </thead>
          <tbody id="orders_body"></tbody>
        </table>
      </section>
    </div>
  </main>

  <footer>
    Built with FastAPI • Tick interval: {SCHEDULE_SECONDS}s
  </footer>

  <script>
    document.getElementById('tf_chip').textContent = "{DEFAULT_TIMEFRAME}";
  </script>
</body>
</html>
""".replace("{SCHEDULE_SECONDS}", str(SCHEDULE_SECONDS)).replace("{DEFAULT_TIMEFRAME}", DEFAULT_TIMEFRAME)


@app.get("/", response_class=HTMLResponse)
def root():
    return HTMLResponse(content=DASHBOARD_HTML, status_code=200)

# -----------------------------------------------------------------------------
# Legacy/utility routes preserved
# -----------------------------------------------------------------------------
@app.get("/diag/ping")
def ping():
    return {"ok": True, "pong": int(time.time())}

@app.get("/diag/routes")
def routes_list():
    routes = []
    for r in app.router.routes:
        routes.append({"path": getattr(r, "path", None), "name": getattr(r, "name", None), "methods": list(getattr(r, "methods", []) or [])})
    return routes

@app.post("/init/backfill")
async def init_backfill(req: Dict[str, Any]):
    """
    Best-effort "read recent" to warm attribution and equity.
    """
    try:
        limit = int(req.get("limit") or 50)
        orders = br.list_orders(status="all", limit=limit) or []
        _push_orders("backfill", orders)
        symbols = list({o.get("symbol") for o in orders if o.get("symbol")})
        _update_equity(symbols)
        return {"ok": True, "considered": len(orders), "symbols": symbols}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/scan/once")
async def scan_once(req: Dict[str, Any]):
    try:
        strat = (req.get("strategy") or ACTIVE_STRATEGIES[0]).lower()
        orders = await _scan_bridge(strat, req, dry=(not TRADING_ENABLED))
        return {"ok": True, "orders": orders}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/replay")
async def replay(req: Dict[str, Any]):
    """
    Accepts a CSV of orders to push into ring for attribution testing.
    """
    try:
        csv_text = req.get("csv") or ""
        f = io.StringIO(csv_text)
        rdr = csv.DictReader(f)
        raws = list(rdr)
        selected = []
        orders: List[Dict[str, Any]] = []
        for r in raws:
            strat = (r.get("strategy") or "replay").lower()
            sym = _sym_to_slash(r.get("symbol") or "")
            side = (r.get("side") or "").lower()
            notional = float(r.get("notional") or 0.0)
            cid = r.get("client_order_id") or f"replay-{int(time.time())}-{sym.replace('/','')}"
            if side not in ("buy", "sell") or not sym or notional <= 0:
                continue
            o = {"symbol": sym, "side": side, "notional": notional, "client_order_id": cid}
            selected.append(o)
            orders.append(o)
        if orders:
            _push_orders("replay", orders)
        return {"ok": True, "considered": len(raws), "selected": len(selected), "replayed": len(orders)}
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
        port, APP_VERSION, BROKER, TRADING_ENABLED
    )
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
