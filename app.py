# app.py — Crypto Trading System "everything" app
# Version: 1.10.0 (2025-09-30)
#
# What’s in this file
# - Full Flask app with HTML dashboard embedded (mobile-friendly)
# - Relative fetch() URLs (no template-literals leaking into Python)
# - /scan/c1..c6 -> calls strategies.c1..c6.run_scan(...)
# - /v2/positions, /orders/recent, /pnl/summary, /orders/attribution, /calendar
# - /config/symbols, /diag/candles, /health
# - Light-weight Alpaca proxy using env vars (paper/live)
#
# Expected ENV (redact secrets in logs!)
#   ALPACA_KEY_ID, ALPACA_SECRET_KEY
#   ALPACA_TRADE_HOST   (e.g. https://paper-api.alpaca.markets)
#   ALPACA_DATA_HOST    (e.g. https://data.alpaca.markets)
#   CRYPTO_EXCHANGE=alpaca
#   TZ=UTC, LOG_LEVEL=INFO
#   SYMBOLS=BTC/USD,ETH/USD,SOL/USD,...
#   AUTORUN_ENABLED=true|false, AUTORUN_INTERVAL_SECS=60, AUTORUN_TIMEFRAME=1Min, AUTORUN_LIMIT=600, AUTORUN_NOTIONAL=5
#
# Notes
# - If Alpaca creds are missing, API endpoints return empty/defaults but still 200.
# - P&L summary:
#     realized: computed from fills (activities) within window
#     unrealized: from current positions
# - Strategy attribution: computed by looking at client_order_id prefixes (c1..c6-*)
# - Calendar: produces a simple monthly grid with last 30 days of activity markers
#
# License: MIT

from __future__ import annotations

import os
import json
import time
import math
import threading
import datetime as dt
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from services.market_crypto import MarketCrypto
import broker as br


import requests
from flask import Flask, request, jsonify, Response

# --------------------------------------------
# App / config
# --------------------------------------------
APP_VERSION = "1.9.2"
app = Flask(__name__)

def env_str(name: str, default: str="") -> str:
    v = os.environ.get(name)
    return v if v is not None and v != "" else default

def env_bool(name: str, default: bool=False) -> bool:
    v = os.environ.get(name)
    if v is None: return default
    return str(v).lower() in ("1","true","yes","y","on")

def env_int(name: str, default: int=0) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except:
        return default

def env_float(name: str, default: float=0.0) -> float:
    try:
        return float(os.environ.get(name, str(default)))
    except:
        return default

TZ = env_str("TZ","UTC")
LOG_LEVEL = env_str("LOG_LEVEL","INFO")

# Symbols config storage (in-memory; you can wire to a DB if you prefer)
DEFAULT_SYMBOLS = [s.strip() for s in env_str("SYMBOLS","BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD").split(",") if s.strip()]
_symbols_store = list(DEFAULT_SYMBOLS)

# Alpaca endpoints
ALPACA_TRADE_HOST = env_str("ALPACA_TRADE_HOST","https://paper-api.alpaca.markets")
ALPACA_DATA_HOST  = env_str("ALPACA_DATA_HOST","https://data.alpaca.markets")
ALPACA_KEY_ID     = env_str("ALPACA_KEY_ID","")
ALPACA_SECRET_KEY = env_str("ALPACA_SECRET_KEY","")

def alpaca_headers() -> Dict[str,str]:
    if not (ALPACA_KEY_ID and ALPACA_SECRET_KEY):
        return {}
    return {
        "APCA-API-KEY-ID": ALPACA_KEY_ID,
        "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

# --------------------------------------------
# Candles (data) helper
# --------------------------------------------

def fetch_crypto_bars(symbol: str, timeframe: str, limit: int=600) -> List[Dict[str,Any]]:
    """
    Returns a list of bars with keys: t,o,h,l,c,v using MarketCrypto (Alpaca v1beta3).
    timeframe: e.g. "1Min","5Min","1Hour","1Day"
    """
    try:
        market = MarketCrypto.from_env()
        data = market.candles([symbol], timeframe=timeframe, limit=limit)
        df = data.get(symbol)
        if df is None or len(df.index) == 0:
            return []
        # Convert DataFrame rows to list of dicts matching prior shape
        out = []
        for _, row in df.iterrows():
            out.append({
                "t": str(row.get("ts")),
                "o": float(row.get("open") or 0),
                "h": float(row.get("high") or 0),
                "l": float(row.get("low") or 0),
                "c": float(row.get("close") or 0),
                "v": float(row.get("volume") or 0),
            })
        return out
    except Exception:
        return []

# --------------------------------------------
# Strategies loading
# --------------------------------------------
# Expect each strategies.cx to expose: run_scan(symbols, timeframe, limit, notional, dry, extra)
def _load_strategy(modname: str):
    # Lazy import so app starts even if a module is missing temporarily
    import importlib
    return importlib.import_module(modname)

STRAT_MODULES = {
    "c1": "strategies.c1",
    "c2": "strategies.c2",
    "c3": "strategies.c3",
    "c4": "strategies.c4",
    "c5": "strategies.c5",
    "c6": "strategies.c6",
}

# --------------------------------------------
# Broker helpers (positions, orders, activities)
# --------------------------------------------
def alpaca_ok() -> bool:
    return bool(ALPACA_KEY_ID and ALPACA_SECRET_KEY and ALPACA_TRADE_HOST.startswith("http"))


def get_positions() -> List[Dict[str,Any]]:
    try:
        return br.list_positions()
    except Exception:
        return []



def get_recent_orders(status: str="all", limit: int=200) -> List[Dict[str,Any]]:
    try:
        return br.list_orders(status=status, limit=limit)
    except Exception:
        return []


def get_fill_activities(after_iso: Optional[str]=None) -> List[Dict[str,Any]]:
    """Realized P&L approximated via FILL activities."""
    if not alpaca_ok():
        return []
    try:
        url = f"{ALPACA_TRADE_HOST}/v2/account/activities/FILL"
        params = {}
        if after_iso:
            params["after"] = after_iso
        r = requests.get(url, headers=alpaca_headers(), params=params, timeout=20)
        if r.status_code != 200:
            return []
        return r.json()
    except Exception:
        return []


def compute_pnl_summary(days: int=14) -> Dict[str,Any]:
    """
    Realized P&L from FILL activities (average-cost method); Unrealized from current positions.
    """
    since = (dt.datetime.utcnow() - dt.timedelta(days=days)).replace(tzinfo=dt.timezone.utc).isoformat()
    fills = get_fill_activities(since)
    # Normalize fills and sort ascending by time
    def _ts(f):
        for k in ("transaction_time","timestamp","processed_at","order_submitted_at","date"):
            if f.get(k):
                return f[k]
        return ""
    fills = sorted(fills, key=_ts)

    realized = 0.0
    # Track running position and avg cost per symbol
    pos: Dict[str, Dict[str,float]] = {}  # {sym: {"qty": q, "avg": avg}}
    for f in fills:
        try:
            sym = f.get("symbol") or f.get("asset_symbol") or ""
            side = (f.get("side") or f.get("order_side") or "").lower()
            px = float(f.get("price") or f.get("price_per_share") or f.get("p") or 0)
            qty = float(f.get("qty") or f.get("quantity") or f.get("q") or 0)
            if not sym or qty <= 0 or px <= 0:
                continue
            # For crypto, Alpaca activities usually use trading symbol (BTCUSD). For consistency, keep as-is.
            s = sym
            entry = pos.get(s, {"qty": 0.0, "avg": 0.0})
            if side in ("buy","buying_power_decrease"):
                # new weighted average cost
                new_qty = entry["qty"] + qty
                entry["avg"] = (entry["avg"] * entry["qty"] + px * qty) / new_qty if new_qty > 0 else entry["avg"]
                entry["qty"] = new_qty
                pos[s] = entry
            elif side in ("sell","selling_power_increase"):
                sell_qty = min(qty, entry["qty"]) if entry["qty"] > 0 else qty
                realized += (px - entry["avg"]) * sell_qty
                entry["qty"] = max(0.0, entry["qty"] - sell_qty)
                pos[s] = entry
        except Exception:
            continue

    # Unrealized from positions snapshot
    positions = get_positions()
    unreal = 0.0
    for p in positions:
        try:
            mv = float(p.get("market_value","0") or 0)
            cb = float(p.get("cost_basis","0") or 0)
            unreal += mv - cb
        except Exception:
            pass
    return {
        "window_days": days,
        "realized": realized,
        "unrealized": unreal,
        "total": realized + unreal,
        "as_of": now_utc_iso(),
    }


def attribute_by_strategy(days: int=30) -> Dict[str,Any]:
    """Group recent orders by client_order_id prefix c1..c6 to approximate strat attribution."""
    orders = get_recent_orders(status="all", limit=500)
    buckets: Dict[str, Dict[str,Any]] = {}
    for o in orders:
        coid = (o.get("client_order_id") or "").lower()
        strat = None
        for s in ("c1","c2","c3","c4","c5","c6"):
            if coid.startswith(s+"-") or coid.startswith(s+"_"):
                strat = s
                break
        if not strat:
            strat = "other"
        b = buckets.setdefault(strat, {"count":0, "symbols":set()})
        b["count"] += 1
        sym = o.get("symbol") or o.get("asset_symbol") or ""
        if sym: b["symbols"].add(sym)
    # Convert sets
    for k,v in buckets.items():
        v["symbols"] = sorted(list(v["symbols"]))
    return {"days": days, "buckets": buckets, "as_of": now_utc_iso()}

# --------------------------------------------
# Autorun loop (optional)
# --------------------------------------------
AUTORUN_ENABLED = env_bool("AUTORUN_ENABLED", False)
AUTORUN_INTERVAL_SECS = env_int("AUTORUN_INTERVAL_SECS", 60)
AUTORUN_TIMEFRAME = env_str("AUTORUN_TIMEFRAME","1Min")
AUTORUN_LIMIT = env_int("AUTORUN_LIMIT", 600)
AUTORUN_NOTIONAL = env_float("AUTORUN_NOTIONAL", 5.0)

def autorun_loop():
    while True:
        try:
            symbols = list(_symbols_store)
            for key, modname in STRAT_MODULES.items():
                try:
                    mod = _load_strategy(modname)
                    res = mod.run_scan(
                        symbols=symbols,
                        timeframe=AUTORUN_TIMEFRAME,
                        limit=AUTORUN_LIMIT,
                        notional=AUTORUN_NOTIONAL,
                        dry=False,
                        extra={}
                    )
                    app.logger.info(f"autorun {key}: {res}")
                    time.sleep(0.25)
                except Exception as e:
                    app.logger.exception(f"autorun {key} error: {e}")
            time.sleep(max(5, AUTORUN_INTERVAL_SECS))
        except Exception as e:
            app.logger.exception(f"autorun outer error: {e}")
            time.sleep(10)

if AUTORUN_ENABLED:
    threading.Thread(target=autorun_loop, daemon=True).start()

# --------------------------------------------
# Routes: health, config, diagnostics
# --------------------------------------------
@app.route("/health")
def health():
    return jsonify({
        "ok": True,
        "time": now_utc_iso(),
        "version": APP_VERSION,
        "alpaca_connected": alpaca_ok(),
        "symbols": _symbols_store
    })

@app.route("/config/symbols", methods=["GET","POST"])
def config_symbols():
    global _symbols_store
    if request.method == "POST":
        js = request.get_json(force=True, silent=True) or {}
        syms = js.get("symbols")
        if isinstance(syms, list) and all(isinstance(s,str) for s in syms):
            _symbols_store = [s.strip() for s in syms if s.strip()]
        return jsonify({"ok": True, "symbols": _symbols_store})
    return jsonify({"symbols": _symbols_store})

@app.route("/diag/candles")
def diag_candles():
    symbols = request.args.get("symbols","").split(",")
    tf = request.args.get("tf","5Min")
    limit = int(request.args.get("limit","600") or "600")
    out_meta = {}
    rows = {}
    for s in symbols:
        s = s.strip()
        if not s: continue
        bars = fetch_crypto_bars(s, tf, limit)
        rows[s] = len(bars)
        out_meta[s] = {
            "rows": len(bars),
            "last_ts": bars[-1]["t"] if bars else None
        }
    return jsonify({"rows": rows, "meta": out_meta})

# --------------------------------------------
# Routes: strategies
# --------------------------------------------
def _comma_list(qs: str) -> List[str]:
    return [x.strip() for x in qs.split(",") if x.strip()]

def _scan_common(strat_key: str):
    modname = STRAT_MODULES[strat_key]
    mod = _load_strategy(modname)
    symbols = _comma_list(request.args.get("symbols","") or ",".join(_symbols_store))
    timeframe = request.args.get("timeframe","5Min")
    limit = int(request.args.get("limit","600") or "600")
    notional = float(request.args.get("notional","0") or "0")
    dry = bool(int(request.args.get("dry","1") or "1"))
    extra = {}  # you can parse extra query params here
    res = mod.run_scan(symbols=symbols, timeframe=timeframe, limit=limit, notional=notional, dry=dry, extra=extra)
    # normalize response structure
    return jsonify({
        "ok": True,
        "dry": dry,
        "results": res.get("results",[]),
        "placed": res.get("placed",[]),
        "version": APP_VERSION
    })

@app.route("/scan/c1", methods=["POST"])
def scan_c1(): return _scan_common("c1")

@app.route("/scan/c2", methods=["POST"])
def scan_c2(): return _scan_common("c2")

@app.route("/scan/c3", methods=["POST"])
def scan_c3(): return _scan_common("c3")

@app.route("/scan/c4", methods=["POST"])
def scan_c4(): return _scan_common("c4")

@app.route("/scan/c5", methods=["POST"])
def scan_c5(): return _scan_common("c5")

@app.route("/scan/c6", methods=["POST"])
def scan_c6(): return _scan_common("c6")

# --------------------------------------------
# Routes: positions / orders / pnl / attribution / calendar
# --------------------------------------------
@app.route("/v2/positions")
def route_positions():
    return jsonify(get_positions())

@app.route("/orders/recent")
def route_orders_recent():
    status = request.args.get("status","all")
    limit = int(request.args.get("limit","200") or "200")
    return jsonify(get_recent_orders(status=status, limit=limit))

@app.route("/pnl/summary")
def route_pnl_summary():
    # default 14 days; you can change with ?days=7 or 30 from the UI toggles
    days = int(request.args.get("days","14") or "14")
    return jsonify(compute_pnl_summary(days=days))

@app.route("/orders/attribution")
def route_orders_attr():
    days = int(request.args.get("days","30") or "30")
    return jsonify(attribute_by_strategy(days=days))

@app.route("/calendar")
def route_calendar():
    """
    Very simple event feed: returns last 30 days of activity markers using orders timestamps.
    """
    days = int(request.args.get("days","30") or "30")
    since = dt.datetime.utcnow() - dt.timedelta(days=days)
    orders = get_recent_orders(status="all", limit=500)
    by_day: Dict[str,int] = {}
    for o in orders:
        ts = o.get("submitted_at") or o.get("created_at") or o.get("updated_at") or None
        if not ts: continue
        try:
            # Normalize to date (UTC)
            d = ts[:10]  # YYYY-MM-DD
            by_day[d] = by_day.get(d,0) + 1
        except:
            pass
    items = [{"date": d, "count": c} for d,c in sorted(by_day.items())]
    return jsonify({"days": days, "items": items, "as_of": now_utc_iso()})

# --------------------------------------------
# Dashboard (HTML) — mobile-friendly, relative URLs only
# --------------------------------------------
DASH_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Crypto System Dashboard · v1.9.2</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg: #0b1020;
      --panel: #131a33;
      --panel-2: #0f152b;
      --text: #e5ecff;
      --muted: #9fb2ff;
      --accent: #5aa2ff;
      --accent-2: #94f3d3;
      --red: #ff6b6b;
      --green: #2ee6a6;
      --yellow: #ffd166;
      --grid: 12px;
      --radius: 14px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; padding: 0;
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Inter, "Helvetica Neue", Arial, "Apple Color Emoji", "Segoe UI Emoji";
      color: var(--text);
      background: radial-gradient(1200px 800px at 10% -20%, #132046 0%, #0b1020 35%) no-repeat, var(--bg);
    }
    a { color: var(--accent); text-decoration: none; }
    .wrap { max-width: 1200px; margin: 0 auto; padding: 16px; }
    header {
      display: flex; flex-wrap: wrap; align-items: center; gap: 10px;
      padding: 12px 8px 8px;
    }
    header .title { font-weight: 700; font-size: 18px; letter-spacing: 0.3px; }
    header .meta { color: var(--muted); font-size: 12px; margin-left: auto; }
    .grid {
      display: grid; gap: 12px;
      grid-template-columns: repeat(12, 1fr);
    }
    .card {
      background: linear-gradient(180deg, rgba(255,255,255,0.02), rgba(255,255,255,0.01));
      border: 1px solid rgba(148, 179, 255, 0.12);
      border-radius: var(--radius);
      padding: 14px;
      box-shadow: 0 8px 20px rgba(0,0,0,0.15), inset 0 1px 0 rgba(255,255,255,0.06);
    }
    .card h3 { margin: 0 0 8px 0; font-size: 14px; color: var(--muted); font-weight: 600; }
    .kvs { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; }
    .kv { background: var(--panel-2); border-radius: 12px; padding: 10px; border: 1px solid rgba(148,179,255,0.10); }
    .kv .lbl { color: var(--muted); font-size: 12px; }
    .kv .val { font-weight: 700; font-size: 18px; }
    .pill { display:inline-block; padding:4px 8px; border-radius:20px; font-size: 11px; background:#0c1b3a; border:1px solid rgba(148,179,255,.15); color: var(--muted); }
    .list { display:flex; flex-direction: column; gap:8px; }
    .row { display:flex; align-items:center; justify-content: space-between; gap: 10px; padding:8px 10px; background: var(--panel-2); border-radius: 10px; border: 1px solid rgba(148,179,255,0.10); }
    .row .l { display:flex; align-items:center; gap:8px; }
    .row .r { color: var(--muted); font-size: 12px; }
    .gain { color: var(--green); }
    .loss { color: var(--red); }
    .tabs { display:flex; gap:6px; flex-wrap: wrap; }
    .tab { padding:6px 10px; border-radius: 20px; background: #101839; border:1px solid rgba(148,179,255,.14); color: var(--muted); font-size: 12px; cursor: pointer; }
    .tab.active { color:#051622; background: var(--accent-2); border-color: rgba(148,179,255,.0); font-weight:700; }
    .muted { color: var(--muted); }

    /* Layout */
    .col-span-12 { grid-column: span 12; }
    .col-span-8  { grid-column: span 8; }
    .col-span-6  { grid-column: span 6; }
    .col-span-4  { grid-column: span 4; }
    .col-span-3  { grid-column: span 3; }
    .col-span-2  { grid-column: span 2; }

    @media (max-width: 980px) {
      .grid { grid-template-columns: repeat(6, 1fr); }
      .col-span-8 { grid-column: span 6; }
      .col-span-6 { grid-column: span 6; }
      .col-span-4 { grid-column: span 6; }
      .col-span-3 { grid-column: span 3; }
      .col-span-2 { grid-column: span 3; }
    }
    @media (max-width: 640px) {
      .grid { grid-template-columns: repeat(2, 1fr); }
      .col-span-8,.col-span-6,.col-span-4,.col-span-3,.col-span-2 { grid-column: span 2; }
      .kvs { grid-template-columns: repeat(2, minmax(0,1fr)); }
    }

    /* Calendar */
    .cal {
      display:grid; gap:8px;
      grid-template-columns: repeat(7, minmax(0,1fr));
    }
    .cal .cell {
      background: var(--panel-2);
      border: 1px solid rgba(148,179,255,.10);
      min-height: 70px;
      border-radius: 10px;
      padding: 6px;
      display:flex; flex-direction:column; justify-content:space-between;
    }
    .cal .d { font-size:11px; color: var(--muted);}
    .dotbar { height:8px; border-radius:4px; background:#0e1d3d; border:1px solid rgba(148,179,255,.08); overflow:hidden;}
    .dotbar .f { height:100%; background: linear-gradient(90deg, var(--accent), var(--accent-2)); width:0%; }

    /* Tables */
    table { width:100%; border-collapse: collapse; }
    th, td { text-align:left; font-size:12px; padding:8px 6px; border-bottom: 1px solid rgba(148,179,255,.08); }
    th { color: var(--muted); font-weight:600; }
  </style>
</head>
<body>
  <div class="wrap">
    <header>
      <div class="title">Crypto Trading System <span class="pill">v1.9.2</span></div>
      <div class="meta" id="meta">loading…</div>
    </header>

    <div class="grid">
      <!-- Summary -->
      <section class="card col-span-12">
        <h3>Summary</h3>
        <div class="kvs" id="summary">
          <div class="kv"><div class="lbl">Positions</div><div class="val" id="sum-pos">–</div></div>
          <div class="kv"><div class="lbl">Unrealized P&L</div><div class="val" id="sum-unr">–</div></div>
          <div class="kv"><div class="lbl">Realized (window)</div><div class="val" id="sum-real">–</div></div>
        </div>
      </section>

      <!-- P&L -->
      <section class="card col-span-8">
        <div style="display:flex; align-items:center; justify-content:space-between;">
          <h3>P&L</h3>
          <div class="tabs">
            <div class="tab" data-days="1">1D</div>
            <div class="tab active" data-days="14">14D</div>
            <div class="tab" data-days="30">30D</div>
          </div>
        </div>
        <div class="list" id="pnlList">
          <div class="row"><div class="l"><span class="pill">Total</span></div><div class="r" id="pnl-total">–</div></div>
          <div class="row"><div class="l"><span class="pill">Unrealized</span></div><div class="r" id="pnl-unr">–</div></div>
          <div class="row"><div class="l"><span class="pill">Realized</span></div><div class="r" id="pnl-real">–</div></div>
          <div class="muted" style="font-size:12px; margin-top:6px;">Realized from fills within window; Unrealized from current positions.</div>
        </div>
      </section>

      <!-- Strategies Attribution -->
      <section class="card col-span-4">
        <h3>Strategies (P&L / volume by prefix)</h3>
        <div id="strats-list" class="list">
          <div class="muted">loading…</div>
        </div>
      </section>

      <!-- Positions -->
      <section class="card col-span-8">
        <h3>Open Positions</h3>
        <div id="pos-box" class="list">
          <div class="muted">loading…</div>
        </div>
      </section>

      <!-- Recent Orders -->
      <section class="card col-span-4">
        <h3>Recent Orders</h3>
        <div class="list" id="orders-box">
          <div class="muted">loading…</div>
        </div>
      </section>

      <!-- Calendar -->
      <section class="card col-span-12">
        <h3>Activity Calendar (last 30 days)</h3>
        <div class="cal" id="cal"></div>
      </section>
    </div>
  </div>

  <script>
    const fmtMoney = (x) => {
      if (x === null || x === undefined || isNaN(x)) return "–";
      const s = Number(x).toFixed(2);
      if (x > 0) return `<span class="gain">+$${s}</span>`;
      if (x < 0) return `<span class="loss">-$${Math.abs(x).toFixed(2)}</span>`;
      return `$${s}`;
    };
    const fmtNum = (x) => (x === null || x === undefined) ? "–" : String(x);
    const el = (id) => document.getElementById(id);

    async function fetchJSON(url) {
      const r = await fetch(url);
      if (!r.ok) throw new Error(`${r.status}`);
      return await r.json();
    }

    async function loadMeta() {
      try {
        const h = await fetchJSON('/health');
        el('meta').innerHTML = `v${h.version} · ${new Date(h.time).toLocaleString()} · Alpaca: ${h.alpaca_connected ? 'ok' : 'off'}`;
      } catch(e) {
        el('meta').innerHTML = 'health: error';
      }
    }

    async function loadPositions() {
      const box = el('pos-box');
      try {
        const ps = await fetchJSON('/v2/positions');
        el('sum-pos').innerHTML = fmtNum(ps.length);
        if (!ps.length) { box.innerHTML = '<div class="muted">No open positions.</div>'; return; }
        box.innerHTML = '';
        ps.forEach(p => {
          const side = (p.side || 'long').toUpperCase();
          const mv = parseFloat(p.market_value || '0');
          const cb = parseFloat(p.cost_basis || '0');
          const unr = mv - cb;
          const div = document.createElement('div');
          div.className = 'row';
          div.innerHTML = `
            <div class="l">
              <span class="pill">${side}</span>
              <strong>${p.symbol || p.asset_symbol || ''}</strong>
              <span class="muted">qty ${p.qty}</span>
            </div>
            <div class="r">MV ${fmtMoney(mv)} · CB ${fmtMoney(cb)} · ${fmtMoney(unr)}</div>
          `;
          box.appendChild(div);
        });
      } catch(e) {
        box.innerHTML = '<div class="muted">positions: error</div>';
      }
    }

    async function loadPNL(days=14) {
      // tabs UI
      document.querySelectorAll('.tab').forEach(t => {
        t.classList.toggle('active', Number(t.dataset.days) === Number(days));
      });
      try {
        const p = await fetchJSON(`/pnl/summary?days=${days}`);
        el('pnl-total').innerHTML = fmtMoney(p.total);
        el('pnl-unr').innerHTML = fmtMoney(p.unrealized);
        el('pnl-real').innerHTML = fmtMoney(p.realized);
        el('sum-unr').innerHTML = fmtMoney(p.unrealized);
        el('sum-real').innerHTML = fmtMoney(p.realized);
      } catch(e) {
        el('pnl-total').innerHTML = 'err';
        el('pnl-unr').innerHTML = 'err';
        el('pnl-real').innerHTML = 'err';
      }
    }

    async function loadAttribution() {
      const box = el('strats-list');
      try {
        const js = await fetchJSON('/orders/attribution?days=30');
        const buckets = js.buckets || {};
        box.innerHTML = '';
        const keys = Object.keys(buckets).sort();
        if (!keys.length) {
          box.innerHTML = '<div class="muted">No recent orders found.</div>';
          return;
        }
        keys.forEach(k => {
          const b = buckets[k];
          const div = document.createElement('div');
          div.className = 'row';
          div.innerHTML = `
            <div class="l"><span class="pill">${k.toUpperCase()}</span> <strong>${b.count} orders</strong></div>
            <div class="r">${(b.symbols || []).join(', ')}</div>
          `;
          box.appendChild(div);
        });
      } catch(e) {
        box.innerHTML = '<div class="muted">attribution: error</div>';
      }
    }

    async function loadOrders() {
      const box = el('orders-box');
      try {
        const os = await fetchJSON('/orders/recent?status=all&limit=200');
        if (!os.length) { box.innerHTML = '<div class="muted">No orders.</div>'; return; }
        box.innerHTML = '';
        os.slice(0,18).forEach(o => {
          const div = document.createElement('div');
          div.className = 'row';
          const coid = (o.client_order_id || '').slice(0, 18);
          div.innerHTML = `
            <div class="l"><span class="pill">${(o.side||'').toUpperCase()}</span> <strong>${o.symbol||''}</strong> <span class="muted">${o.status||''}</span></div>
            <div class="r">${coid}</div>
          `;
          box.appendChild(div);
        });
      } catch(e) {
        box.innerHTML = '<div class="muted">orders: error</div>';
      }
    }

    function monthGrid(items) {
      // items: [{date:'YYYY-MM-DD', count:n}]
      // make a 5-week grid from today backwards ~30 days
      const today = new Date();
      const map = {};
      items.forEach(x => map[x.date] = x.count);
      const root = document.getElementById('cal');
      root.innerHTML = '';
      // compute the last 35 days
      for (let i=34; i>=0; i--) {
        const d = new Date(today.getTime() - i*24*60*60*1000);
        const iso = d.toISOString().slice(0,10);
        const n = map[iso] || 0;
        const pct = Math.min(100, n*10); // simple fill scale
        const cell = document.createElement('div');
        cell.className = 'cell';
        cell.innerHTML = `
          <div class="d">${iso}</div>
          <div class="dotbar"><div class="f" style="width:${pct}%"></div></div>
        `;
        root.appendChild(cell);
      }
    }

    async function loadCalendar() {
      try {
        const js = await fetchJSON('/calendar?days=30');
        monthGrid(js.items || []);
      } catch(e) {
        document.getElementById('cal').innerHTML = '<div class="muted">calendar: error</div>';
      }
    }

    // Tabs handlers
    document.addEventListener('click', (ev) => {
      const t = ev.target.closest('.tab');
      if (t) {
        const days = Number(t.dataset.days || 14);
        loadPNL(days);
      }
    });

    // Initial load + refresh every ~30s
    (async function init() {
      await loadMeta();
      await loadPNL(14);
      await loadAttribution();
      await loadPositions();
      await loadOrders();
      await loadCalendar();
      setInterval(() => { loadMeta(); loadPositions(); loadOrders(); }, 30000);
    })();
  </script>
</body>
</html>
"""

@app.route("/")
def home():
    return Response(DASH_HTML, mimetype="text/html")

# --------------------------------------------
# Main
# --------------------------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT","10000"))
    print(f"Starting Crypto System {APP_VERSION} on port {port}  (Alpaca OK: {alpaca_ok()})")
    app.run(host="0.0.0.0", port=port)
