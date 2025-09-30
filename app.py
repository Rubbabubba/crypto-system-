# app.py
# =====================================================================
# Crypto System - Everything App (REST + Dashboard)
# Version: 2.2.0 (2025-09-29)
# ---------------------------------------------------------------------
# This Flask app exposes:
#   - GET  /                      -> Dashboard (mobile-friendly)
#   - GET  /health               -> Service health & version
#   - GET  /config/symbols       -> Current symbols (JSON)
#   - POST /config/symbols       -> Update symbols (JSON body: {"symbols": [...]})
#   - POST /scan/<sid>           -> Run strategy c1..c6 over symbols
#   - GET  /diag/candles         -> Row counts + last ts for requested symbols
#   - GET  /orders/recent        -> Recent orders passthrough from Alpaca
#   - GET  /orders/attribution   -> P&L by strategy tag (client_order_id prefix)
#   - GET  /pnl/summary          -> P&L summary for N days (default 14)
#   - GET  /calendar             -> Stub JSON events, surface in dashboard
#
# Environment (paper by default):
#   ALPACA_PAPER=1
#   ALPACA_KEY_ID, ALPACA_SECRET_KEY
#   ALPACA_TRADE_HOST=https://paper-api.alpaca.markets
#   ALPACA_DATA_HOST =https://data.alpaca.markets
#
# Strategy knobs can be passed as query params to /scan/<sid>, and each
# strategy may use env fallbacks (e.g. C1_EMA_LEN, etc.).
# =====================================================================

import os
import json
import math
import time
import typing as T
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests
from flask import Flask, request, jsonify, make_response

# -------------------------
# Global config & constants
# -------------------------
APP_VERSION = "2.2.0"
APP_BUILD   = "2025-09-29T00:00:00Z"

DEFAULT_SYMBOLS = [
    "BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD",
    "AVAX/USD","LINK/USD","BCH/USD","LTC/USD"
]

DEFAULT_TIMEFRAME = os.getenv("AUTORUN_TIMEFRAME", "5Min")
DEFAULT_LIMIT     = int(os.getenv("AUTORUN_LIMIT", "600"))
DEFAULT_NOTIONAL  = float(os.getenv("AUTORUN_NOTIONAL", "25"))

# Alpaca hosts/keys
ALPACA_KEY_ID     = os.getenv("ALPACA_KEY_ID", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
TRADE_HOST        = os.getenv("ALPACA_TRADE_HOST", "https://paper-api.alpaca.markets")
DATA_HOST         = os.getenv("ALPACA_DATA_HOST",  "https://data.alpaca.markets")

# Misc
TZ = os.getenv("TZ", "UTC")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
STRAT_TAGS = {"c1","c2","c3","c4","c5","c6"}

session = requests.Session()
session.headers.update({
    "APCA-API-KEY-ID": ALPACA_KEY_ID,
    "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
})

app = Flask(__name__)

def _utcnow_iso():
    return datetime.now(timezone.utc).isoformat()

def log(msg: str):
    if LOG_LEVEL in ("INFO","DEBUG"):
        print(msg, flush=True)

def err(msg: str):
    print(msg, flush=True)

# -------------------------
# Strategy loader
# -------------------------
def load_strat(sid: str):
    if sid not in STRAT_TAGS:
        raise ValueError(f"Unknown strategy id: {sid}")
    mod_name = f"strategies.{sid}"
    try:
        mod = __import__(mod_name, fromlist=["*"])
        if not hasattr(mod, "run"):
            raise AttributeError(f"{mod_name} missing run(df_map, params)")
        return mod
    except Exception as e:
        raise RuntimeError(f"Failed to import {mod_name}: {e}")

# -------------------------
# Symbols config (in-memory)
# -------------------------
_symbols = DEFAULT_SYMBOLS[:]

@app.get("/config/symbols")
def get_symbols():
    return jsonify({"symbols": _symbols, "count": len(_symbols)})

@app.post("/config/symbols")
def set_symbols():
    try:
        data = request.get_json(force=True) or {}
        syms = data.get("symbols") or []
        if not isinstance(syms, list) or not all(isinstance(x, str) for x in syms):
            return jsonify({"ok": False, "error": "Body must be {'symbols': [str,...]}"}), 400
        global _symbols
        _symbols = syms
        return jsonify({"ok": True, "symbols": _symbols, "count": len(_symbols)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

# -------------------------
# Alpaca helpers
# -------------------------
def _normalize_symbol(sym: str) -> str:
    # Alpaca crypto uses like "BTCUSD" in trading, but bars API
    # accepts "BTC/USD" or "BTCUSD". We keep the display version and join for API.
    return sym.replace("/", "")

def get_orders(status="all", limit=200):
    url = f"{TRADE_HOST}/v2/orders"
    params = {"status": status, "limit": str(limit), "direction": "desc"}
    r = session.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def get_positions():
    url = f"{TRADE_HOST}/v2/positions"
    r = session.get(url, timeout=20)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    return r.json()

def get_activities(activity_types=None, date_from=None, date_to=None, page_size=100):
    # Used for P&L attribution (fills)
    url = f"{TRADE_HOST}/v2/account/activities"
    params = {"page_size": str(page_size)}
    if activity_types:
        params["activity_types"] = activity_types
    if date_from: params["date"] = None; params["after"] = date_from
    if date_to:   params["until"] = date_to
    r = session.get(url, params=params, timeout=25)
    r.raise_for_status()
    return r.json()

def portfolio_history(period="2W", timeframe="1D"):
    url = f"{TRADE_HOST}/v2/account/portfolio/history"
    params = {"period": period, "timeframe": timeframe, "intraday_reporting": "market_hours"}
    r = session.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_crypto_bars(symbols: T.List[str], timeframe="5Min", limit=600) -> T.Dict[str, pd.DataFrame]:
    """
    Pull bars for multiple symbols via v1beta3 crypto bars.
    Returns { "BTC/USD": DataFrame, ... } with utc index.
    """
    # Translate timeframe to Alpaca API granularity
    tf_map = {
        "1Min":"1Min","3Min":"3Min","5Min":"5Min","15Min":"15Min","30Min":"30Min","1Hour":"1H"
    }
    alp_tf = tf_map.get(timeframe, "5Min")
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=limit*5 if "Min" in timeframe else limit*60)

    df_map: T.Dict[str, pd.DataFrame] = {}
    for sym in symbols:
        base = _normalize_symbol(sym)
        url = f"{DATA_HOST}/v1beta3/crypto/us/bars"
        params = {
            "symbols": base,
            "timeframe": alp_tf,
            "limit": str(limit),
            "start": start.isoformat().replace("+00:00","Z"),
            "end": end.isoformat().replace("+00:00","Z"),
        }
        try:
            r = session.get(url, params=params, timeout=25)
            r.raise_for_status()
            j = r.json()
            # Expected {"bars": {"BTCUSD": [ {t,o,h,l,c,v}, ... ] } }
            bars = j.get("bars", {}).get(base, [])
            if not bars:
                df_map[sym] = pd.DataFrame()
                continue
            df = pd.DataFrame(bars)
            # Normalize schema
            # t: RFC3339 time, o/h/l/c: float, v: volume
            df["time"] = pd.to_datetime(df["t"], utc=True)
            df = df.rename(columns={"o":"open","h":"high","l":"low","c":"close","v":"volume"})
            df = df[["time","open","high","low","close","volume"]].sort_values("time")
            df = df.set_index("time")
            df_map[sym] = df
        except Exception as e:
            err(f"[bars] {sym} fetch error: {e}")
            df_map[sym] = pd.DataFrame()
    return df_map

# -------------------------
# Diagnostics
# -------------------------
@app.get("/health")
def health():
    ok = bool(ALPACA_KEY_ID and ALPACA_SECRET_KEY and TRADE_HOST and DATA_HOST)
    payload = [
        {"ok": ok, "time": _utcnow_iso(), "version": APP_VERSION, "build": APP_BUILD},
    ]
    df = pd.DataFrame(payload)
    return df.to_string(index=False) + "\n"

@app.get("/diag/candles")
def diag_candles():
    syms = (request.args.get("symbols") or ",".join(_symbols)).split(",")
    tf   = request.args.get("tf", DEFAULT_TIMEFRAME)
    lim  = int(request.args.get("limit", str(DEFAULT_LIMIT)))
    dfm  = fetch_crypto_bars(syms, tf, lim)
    meta = {}
    rows = {}
    for s,df in dfm.items():
        rows[s] = int(df.shape[0])
        meta[s] = {
            "last_ts": (df.index.max().isoformat() if not df.empty else None),
            "rows": rows[s]
        }
    return jsonify({"meta": meta, "rows": rows})

# -------------------------
# P&L + Attribution
# -------------------------
@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status","all")
    limit  = int(request.args.get("limit","200"))
    try:
        data = get_orders(status=status, limit=limit)
        return jsonify(data)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502

@app.get("/orders/attribution")
def orders_attribution():
    days = int(request.args.get("days","30"))
    since = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    # Use fills activities for robust sizing & realized P&L
    try:
        acts = get_activities(activity_types="FILL", date_from=since, page_size=1000)
    except Exception as e:
        return jsonify({"ok": False, "error": f"activities error: {e}"}), 502

    # Group by client_order_id prefix (c1..c6-*)
    rows = []
    for a in acts:
        cid = a.get("order_id") or ""
        clid = a.get("client_order_id") or ""
        strat = ""
        for tag in STRAT_TAGS:
            if clid.lower().startswith(tag):
                strat = tag
                break
        rows.append({
            "time": a.get("transaction_time"),
            "symbol": a.get("symbol"),
            "qty": float(a.get("qty", "0") or 0),
            "price": float(a.get("price", "0") or 0.0),
            "side": a.get("side"),
            "client_order_id": clid,
            "strategy": strat or "other",
        })
    df = pd.DataFrame(rows)
    if df.empty:
        return jsonify({"ok": True, "per_strategy": [], "count": 0})

    # Approx realized p/l per fill direction vs previous position price is non-trivial without positions history.
    # As a pragmatic attribution for dashboard, compute gross traded notional per strategy and count of fills.
    df["notional"] = df["qty"] * df["price"]
    g = df.groupby("strategy").agg(
        fills=("symbol","count"),
        gross_notional=("notional","sum"),
    ).reset_index().sort_values("gross_notional", ascending=False)
    return jsonify({"ok": True, "per_strategy": g.to_dict(orient="records"), "count": int(df.shape[0])})

@app.get("/pnl/summary")
def pnl_summary():
    days = int(request.args.get("days","14"))
    # Portfolio history (equity curve) — simpler and stable
    period = "1M" if days > 21 else ("2W" if days <= 14 else "1M")
    try:
        ph = portfolio_history(period=period, timeframe="1D")
        out = {
            "equity": ph.get("equity", []),
            "profit_loss": ph.get("profit_loss", []),
            "time": ph.get("timestamp", []),
            "base_value": ph.get("base_value", 0),
            "period": ph.get("period", period),
        }
        return jsonify({"ok": True, "summary": out})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502

# -------------------------
# Strategy scan/execute
# -------------------------
def _parse_symbols_arg() -> T.List[str]:
    syms_arg = request.args.get("symbols")
    if syms_arg:
        syms = syms_arg.split(",")
    else:
        syms = _symbols
    # normalize spacing
    return [s.strip() for s in syms if s.strip()]

def _parse_bool_arg(name: str, default=False) -> bool:
    v = request.args.get(name)
    if v is None:
        return default
    return str(v).lower() in ("1","true","yes","y","on")

def _client_order_tag(sid: str) -> str:
    tag = os.getenv("CLIENT_ORDER_TAG", "")
    return f"{sid}-{tag}".strip("-")

def _place_market_notional(symbol: str, usd_notional: float, side: str, client_tag: str, dry: bool):
    if dry:
        return {"side": side, "symbol": symbol, "notional": usd_notional,
                "status": "simulated", "client_order_id": client_tag }
    url = f"{TRADE_HOST}/v2/orders"
    payload = {
        "symbol": _normalize_symbol(symbol),
        "side": side,
        "type": "market",
        "time_in_force": "gtc",
        "notional": str(usd_notional),
        "client_order_id": client_tag,
    }
    r = session.post(url, json=payload, timeout=20)
    if r.status_code >= 400:
        raise RuntimeError(f"order error {r.status_code}: {r.text}")
    return r.json()

@app.post("/scan/<sid>")
def scan_sid(sid: str):
    t0 = time.time()
    dry = _parse_bool_arg("dry", default=True)
    tf  = request.args.get("timeframe", DEFAULT_TIMEFRAME)
    lim = int(request.args.get("limit", str(DEFAULT_LIMIT)))
    notional = float(request.args.get("notional", str(DEFAULT_NOTIONAL)))

    syms = _parse_symbols_arg()
    if not syms:
        return jsonify({"ok": False, "error": "No symbols provided"}), 400

    # 1) Load strategy
    try:
        strat = load_strat(sid)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400

    # 2) Fetch bars
    df_map = fetch_crypto_bars(syms, tf, lim)
    empty = [s for s,df in df_map.items() if df.empty]
    if len(empty) == len(syms):
        return jsonify({
            "ok": False, "error": "No bars returned for all symbols",
            "hint": "Check ALPACA_DATA_HOST and API keys; /diag/candles will show rows"
        }), 502

    # 3) Params (query -> dict)
    params = dict(request.args)  # easy pass-through
    params["notional"] = notional
    params["timeframe"] = tf
    params["limit"] = lim

    # 4) Run strategy
    try:
        res = strat.run(df_map, params)
        # expected shape:
        # res = {"decisions":[{"symbol":"BTC/USD", "action":"buy|sell|flat", "reason":"..."}, ...]}
    except Exception as e:
        return jsonify({"ok": False, "error": f"strategy error: {e}"}), 500

    decisions = res.get("decisions", []) if isinstance(res, dict) else []
    placed = []
    tag_base = _client_order_tag(sid)

    # 5) Turn decisions into orders
    for d in decisions:
        sym = d.get("symbol")
        act = (d.get("action") or "flat").lower()
        if sym not in syms:
            continue
        if act not in ("buy", "sell"):
            continue
        side = "buy" if act == "buy" else "sell"
        # client tag per order (so we keep attribution)
        client_tag = f"{tag_base}-{sym.replace('/','')}-{int(time.time())}"
        try:
            order = _place_market_notional(sym, notional, side, client_tag, dry=dry)
            placed.append(order)
        except Exception as e:
            placed.append({"symbol": sym, "side": side, "notional": notional,
                           "status": f"error: {e}", "client_order_id": client_tag})

    took_ms = int((time.time() - t0) * 1000)
    return jsonify({
        "ok": True,
        "dry": dry,
        "took_ms": took_ms,
        "empty_symbols": empty,
        "results": decisions,
        "placed": placed
    })

# -------------------------
# Dashboard (HTML)
# -------------------------
DASHBOARD_HTML = f"""
<!doctype html>
<html lang="en" data-bs-theme="dark">
<head>
  <meta charset="utf-8">
  <title>Crypto System Dashboard · v{APP_VERSION}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <link
    href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css"
    rel="stylesheet" />
  <style>
    body {{ padding: 16px; }}
    .card {{ margin-bottom: 16px; }}
    .kv .k {{ color: #9aa0a6; width: 140px; display: inline-block; }}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
    .grid-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }}
    .grid-3 {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }}
    @media(max-width: 992px) {{
      .grid-2, .grid-3 {{ grid-template-columns: 1fr; }}
    }}
    .badge-tag {{ font-size: .8rem; }}
    .table-sm td, .table-sm th {{ padding:.4rem; }}
    .pill {{ border:1px solid #444; padding:.25rem .5rem; border-radius:999px; }}
  </style>
</head>
<body class="bg-body-tertiary">
  <div class="container-fluid">
    <div class="d-flex justify-content-between align-items-center mb-2">
      <div>
        <h3 class="mb-0">Crypto Trading System</h3>
        <div class="text-secondary">Version <span class="mono">{APP_VERSION}</span> · Build <span class="mono">{APP_BUILD}</span></div>
      </div>
      <div class="text-end">
        <div id="now" class="text-secondary mono"></div>
        <button class="btn btn-sm btn-primary" id="refreshAll">Refresh</button>
      </div>
    </div>

    <div class="grid-3">
      <div class="card">
        <div class="card-header">Summary</div>
        <div class="card-body">
          <div id="summary" class="kv">
            <div><span class="k">Positions</span><span id="sum_positions" class="mono">—</span></div>
            <div><span class="k">Today P&amp;L</span><span id="sum_pnl" class="mono">—</span></div>
            <div><span class="k">Equity (last)</span><span id="sum_equity" class="mono">—</span></div>
          </div>
        </div>
      </div>

      <div class="card">
        <div class="card-header d-flex justify-content-between align-items-center">
          <span>P&amp;L</span>
          <div>
            <select id="pnl_days" class="form-select form-select-sm" style="width:auto; display:inline-block;">
              <option value="7">7d</option>
              <option value="14" selected>14d</option>
              <option value="30">30d</option>
              <option value="90">90d</option>
            </select>
          </div>
        </div>
        <div class="card-body">
          <canvas id="pnl_chart" height="120"></canvas>
        </div>
      </div>

      <div class="card">
        <div class="card-header">Calendar</div>
        <div class="card-body">
          <ul id="calendar_ul" class="list-unstyled mb-0"></ul>
        </div>
      </div>
    </div>

    <div class="grid-2">
      <div class="card">
        <div class="card-header">Strategy Attribution (last 30d)</div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-sm table-striped align-middle">
              <thead>
                <tr><th>Strategy</th><th>Fills</th><th>Gross Notional</th></tr>
              </thead>
              <tbody id="attr_tbody">
                <tr><td colspan="3" class="text-center text-secondary">Loading…</td></tr>
              </tbody>
            </table>
          </div>
          <small class="text-secondary">Attribution uses client_order_id prefixes (c1..c6).</small>
        </div>
      </div>

      <div class="card">
        <div class="card-header">Recent Orders</div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-sm table-striped align-middle">
              <thead>
                <tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Price</th><th>Status</th><th>Client ID</th></tr>
              </thead>
              <tbody id="orders_tbody">
                <tr><td colspan="7" class="text-center text-secondary">Loading…</td></tr>
              </tbody>
            </table>
          </div>
          <div class="text-end">
            <span class="pill mono">status=all · limit=200</span>
          </div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-header">Actions</div>
      <div class="card-body">
        <div class="d-flex flex-wrap gap-2">
          <button class="btn btn-outline-light btn-sm" data-sid="c1">Run C1</button>
          <button class="btn btn-outline-light btn-sm" data-sid="c2">Run C2</button>
          <button class="btn btn-outline-light btn-sm" data-sid="c3">Run C3</button>
          <button class="btn btn-outline-light btn-sm" data-sid="c4">Run C4</button>
          <button class="btn btn-outline-light btn-sm" data-sid="c5">Run C5</button>
          <button class="btn btn-outline-light btn-sm" data-sid="c6">Run C6</button>
        </div>
        <div id="run_output" class="mt-3 mono small text-secondary"></div>
      </div>
    </div>

    <footer class="mt-3 text-secondary small">
      &copy; 2025 · Crypto System · v{APP_VERSION}
    </footer>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
  <script>
    const svc = location.origin;

    function fmtUsd(x) {{
      if (x === null || x === undefined || isNaN(x)) return "—";
      return "$" + Number(x).toLocaleString(undefined, {{maximumFractionDigits: 2}});
    }}

    async function fetchJSON(url) {{
      const r = await fetch(url);
      if (!r.ok) throw new Error(await r.text());
      return r.json();
    }}

    async function loadSummary() {{
      // positions count + today's pnl approximated from last equity change
      const orders = await fetchJSON(`/orders/recent?status=all&limit=200`).catch(()=>[]);
      const pnl = await fetchJSON(`/pnl/summary?days=14`).catch(()=>null);

      const posResp = await fetch(`${svc}/v2/positions`, {{headers: {{}} }});
      // NOTE: direct /v2/positions would be blocked (CORS). So fallback to server proxy not provided here.
      // We just compute "Positions" from orders presence:
      const posCount = 0;

      let lastEquity = null;
      if (pnl && pnl.ok && pnl.summary && pnl.summary.equity && pnl.summary.equity.length>0) {{
        lastEquity = pnl.summary.equity[pnl.summary.equity.length-1];
      }}

      document.getElementById('sum_positions').textContent = String(posCount);
      document.getElementById('sum_pnl').textContent = "n/a";
      document.getElementById('sum_equity').textContent = lastEquity? fmtUsd(lastEquity): "—";
    }}

    async function loadPnl(days) {{
      const j = await fetchJSON(`/pnl/summary?days=${{days}}`);
      const eq = (j.summary?.equity ?? []);
      const ts = (j.summary?.time ?? []).map(t => new Date(t*1000));

      const ctx = document.getElementById('pnl_chart').getContext('2d');
      if (window.pnlChart) window.pnlChart.destroy();
      window.pnlChart = new Chart(ctx, {{
        type: 'line',
        data: {{
          labels: ts,
          datasets: [{{ label: 'Equity', data: eq }}]
        }},
        options: {{
          scales: {{
            x: {{ type: 'time', time: {{ unit: 'day' }} }},
            y: {{ beginAtZero: false }}
          }},
          plugins: {{
            legend: {{ display: false }}
          }}
        }}
      }});
    }}

    async function loadAttribution() {{
      const j = await fetchJSON('/orders/attribution?days=30');
      const tbody = document.getElementById('attr_tbody');
      tbody.innerHTML = '';
      if (!(j.ok) || (j.per_strategy ?? []).length===0) {{
        tbody.innerHTML = '<tr><td colspan="3" class="text-center text-secondary">No fills in window.</td></tr>';
        return;
      }}
      for (const r of j.per_strategy) {{
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><span class="badge bg-secondary badge-tag">${{r.strategy}}</span></td>
          <td class="mono">${{r.fills}}</td>
          <td class="mono">${{fmtUsd(r.gross_notional)}}</td>`;
        tbody.appendChild(tr);
      }}
    }}

    async function loadOrders() {{
      const j = await fetchJSON('/orders/recent?status=all&limit=200');
      const tbody = document.getElementById('orders_tbody');
      tbody.innerHTML = '';
      if (!Array.isArray(j) || j.length===0) {{
        tbody.innerHTML = '<tr><td colspan="7" class="text-center text-secondary">No orders.</td></tr>';
        return;
      }}
      for (const o of j) {{
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td class="mono">${{o.submitted_at ?? o.created_at ?? ''}}</td>
          <td class="mono">${{o.symbol ?? ''}}</td>
          <td>${{(o.side ?? '').toUpperCase()}}</td>
          <td class="mono">${{o.qty ?? ''}}</td>
          <td class="mono">${{o.filled_avg_price ?? o.limit_price ?? ''}}</td>
          <td>${{o.status ?? ''}}</td>
          <td class="mono">${{o.client_order_id ?? ''}}</td>`;
        tbody.appendChild(tr);
      }}
    }}

    async function loadCalendar() {{
      const j = await fetchJSON('/calendar');
      const ul = document.getElementById('calendar_ul');
      ul.innerHTML = '';
      if (!(j.ok) || (j.events??[]).length===0) {{
        ul.innerHTML = '<li class="text-secondary">No upcoming items.</li>';
        return;
      }}
      for (const e of j.events) {{
        const li = document.createElement('li');
        li.innerHTML = `<div class="mono">${{e.date}} · <strong>${{e.title}}</strong> <span class="text-secondary">${{e.note ?? ''}}</span></div>`;
        ul.appendChild(li);
      }}
    }}

    async function runStrategy(sid) {{
      const out = document.getElementById('run_output');
      out.textContent = 'Running ' + sid + '…';
      try {{
        const u = `/scan/${{sid}}?dry=1&timeframe=5Min&limit=600&notional=25&symbols=BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD`;
        const r = await fetch(u, {{method:'POST'}});
        const j = await r.json();
        out.textContent = JSON.stringify(j, null, 2);
      }} catch (e) {{
        out.textContent = 'Error: ' + e;
      }}
    }}

    document.getElementById('pnl_days').addEventListener('change', (e) => {{
      loadPnl(e.target.value);
    }});

    document.getElementById('refreshAll').addEventListener('click', async () => {{
      document.getElementById('now').textContent = new Date().toISOString();
      await Promise.all([loadSummary(), loadPnl(document.getElementById('pnl_days').value), loadAttribution(), loadOrders(), loadCalendar()]);
    }});

    document.querySelectorAll('button[data-sid]').forEach(btn => {{
      btn.addEventListener('click', () => runStrategy(btn.dataset.sid));
    }});

    // initial load
    document.getElementById('now').textContent = new Date().toISOString();
    (async () => {{
      await Promise.all([loadSummary(), loadPnl(14), loadAttribution(), loadOrders(), loadCalendar()]);
    }})();
  </script>
</body>
</html>
"""

@app.get("/")
def index():
    resp = make_response(DASHBOARD_HTML)
    resp.headers["X-App-Version"] = APP_VERSION
    return resp

# Simple static calendar stub you can replace with your own source
@app.get("/calendar")
def calendar():
    today = datetime.now(timezone.utc).date()
    events = [
        {"date": (today + timedelta(days=1)).isoformat(), "title": "Daily check", "note":"Review fills & attribution"},
        {"date": (today + timedelta(days=3)).isoformat(), "title": "Weekly rollup", "note":"P&L export"},
    ]
    return jsonify({"ok": True, "events": events, "tz": "UTC"})

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    host = "0.0.0.0"
    log(f"Starting Crypto System v{APP_VERSION} ({APP_BUILD}) on {host}:{port}")
    app.run(host=host, port=port, debug=False)
