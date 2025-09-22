# app.py — Crypto System API + Dashboard (rich UI)
# Version: 1.8.6
# - Fix: escaped JS template expressions in dashboard HTML (no f-string parsing issues)
# - Full HTML dashboard with P&L cards, calendar heatmap, per-strategy attribution,
#   positions, recent orders, symbols editor, and one-click scan runners.
# - Per-strategy version reporting in /health/versions
# - /admin/reload (dev-gated via ALLOW_ADMIN_RELOAD=1) to hot-reload c1..c6
# - /config/symbols (GET/POST) persisted in ./config/symbols.json
# - /pnl/summary and /pnl/daily (calendar heatmap backend)
# - Keeps legacy response shapes (orders/recent wraps { value, Count }, positions = plain array)

import os
import json
import importlib
import traceback
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Any, List, Tuple

import requests
from flask import Flask, request, jsonify, Response

APP_VERSION = "1.8.6"

# --------------------------------
# Environment / Config
# --------------------------------
ALPACA_KEY    = os.getenv("APCA_API_KEY_ID", "")
ALPACA_SECRET = os.getenv("APCA_API_SECRET_KEY", "")
TRADING_BASE  = os.getenv("TRADING_BASE", "https://paper-api.alpaca.markets")
DATA_BASE     = os.getenv("DATA_BASE",    "https://data.alpaca.markets")
EXCHANGE_NAME = os.getenv("EXCHANGE",     "alpaca")
ALLOW_RELOAD  = os.getenv("ALLOW_ADMIN_RELOAD", "0") in ("1", "true", "True")

DEFAULT_TIMEFRAME = "1Min"

# Persisted default symbols for scans / cron
CONFIG_DIR = os.getenv("CONFIG_DIR", "./config")
SYMBOLS_PATH = os.path.join(CONFIG_DIR, "symbols.json")
os.makedirs(CONFIG_DIR, exist_ok=True)

def _alpaca_headers() -> Dict[str, str]:
    return {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": f"crypto-system/{APP_VERSION}"
    }

def _jsonify_ok(d: Dict[str, Any]) -> Response:
    return jsonify({"ok": True, **d})

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _iso(d: datetime) -> str:
    if not d.tzinfo:
        d = d.replace(tzinfo=timezone.utc)
    return d.isoformat()

def _safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0

# --------------------------------
# Strategy import / version report
# --------------------------------
def _strategy_versions() -> Dict[str, str]:
    versions = {}
    for name in ("c1", "c2", "c3", "c4", "c5", "c6"):
        try:
            mod = importlib.import_module(name)
            versions[name] = getattr(mod, "__version__", "unknown")
        except Exception:
            versions[name] = "unavailable"
    return versions

def _reload_strategies() -> Dict[str, str]:
    out = {}
    for name in ("c1", "c2", "c3", "c4", "c5", "c6"):
        try:
            if name in list(importlib.sys.modules.keys()):
                importlib.reload(importlib.sys.modules[name])
            else:
                importlib.import_module(name)
            mod = importlib.import_module(name)
            out[name] = getattr(mod, "__version__", "unknown")
        except Exception as e:
            out[name] = f"error: {e}"
    return out

# --------------------------------
# Alpaca helpers
# --------------------------------
def _alpaca_get(url: str, params: Dict[str, Any] = None) -> Any:
    r = requests.get(url, headers=_alpaca_headers(), params=params or {}, timeout=30)
    r.raise_for_status()
    return r.json()

def _alpaca_post(url: str, body: Dict[str, Any]) -> Any:
    r = requests.post(url, headers=_alpaca_headers(), data=json.dumps(body), timeout=30)
    r.raise_for_status()
    return r.json()

# Orders / positions
def _list_orders(status="all", limit=50) -> List[Dict[str, Any]]:
    url = f"{TRADING_BASE}/v2/orders"
    params = {
        "status": status,
        "limit": limit,
        "direction": "desc",
        "nested": "false",
        "asset_class": "crypto",
    }
    data = _alpaca_get(url, params)
    if isinstance(data, dict) and "orders" in data:
        return data.get("orders", [])
    return data if isinstance(data, list) else []

def _list_positions() -> List[Dict[str, Any]]:
    url = f"{TRADING_BASE}/v2/positions"
    params = {"asset_class": "crypto"}
    data = _alpaca_get(url, params)
    return data if isinstance(data, list) else data.get("positions", [])

# Market data bars
def _get_bars(symbols: List[str], timeframe: str, limit: int) -> Dict[str, List[Dict[str, Any]]]:
    # Alpaca v1beta3 crypto bars (US)
    url = f"{DATA_BASE}/v1beta3/crypto/us/bars"
    params = {
        "symbols": ",".join(s.replace("/", "") for s in symbols),  # e.g. BTCUSD
        "timeframe": timeframe,
        "limit": limit
    }
    data = _alpaca_get(url, params)
    # Normalize to { "BTC/USD": [bars...] }
    out: Dict[str, List[Dict[str, Any]]] = {}
    bars = data.get("bars", {})
    for k, arr in bars.items():
        # map BTCUSD -> BTC/USD
        sym = f"{k[:-3]}/{k[-3:]}" if len(k) > 3 else k
        out[sym] = arr
    return out

# --------------------------------
# Flask
# --------------------------------
app = Flask(__name__)

# Root / minimal status
@app.get("/")
def root():
    html = f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Crypto System</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 2rem; }}
    code, pre {{ background:#111; color:#0f0; padding:0.5rem 0.75rem; border-radius:6px; display:block; }}
    .kvs span {{ display:inline-block; min-width: 180px; }}
    a.button {{ display:inline-block; padding:8px 12px; background:#0ea5e9; color:#fff; border-radius:8px; text-decoration:none; }}
  </style>
</head>
<body>
  <h1>Crypto System</h1>
  <div class="kvs">
    <div><span>app:</span><b>{APP_VERSION}</b></div>
    <div><span>exchange:</span><b>{EXCHANGE_NAME}</b></div>
    <div><span>trading_base:</span><b>{TRADING_BASE}</b></div>
    <div><span>data_base:</span><b>{DATA_BASE}</b></div>
  </div>
  <p><a class="button" href="/dashboard">Open Dashboard</a></p>
  <h2>Health</h2>
  <pre>GET /health/versions</pre>
  <h2>Routes</h2>
  <pre>GET /routes</pre>
</body>
</html>
"""
    return Response(html, mimetype="text/html")

# -----------------------------
# Health / routes
# -----------------------------
@app.get("/health/versions")
def health_versions():
    try:
        systems = _strategy_versions()
        return jsonify({
            "app": APP_VERSION,
            "exchange": EXCHANGE_NAME,
            "trading_base": TRADING_BASE,
            "data_base": DATA_BASE,
            "systems": systems
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500

@app.get("/routes")
def routes():
    items = []
    for r in app.url_map.iter_rules():
        items.append({
            "endpoint": r.endpoint,
            "methods": sorted([m for m in r.methods if m not in ("HEAD","OPTIONS")]),
            "rule": str(r)
        })
    return jsonify({"ok": True, "routes": items})

# -----------------------------
# Diagnostics
# -----------------------------
@app.get("/diag/gate")
def diag_gate():
    try:
        _ = _list_orders(status="all", limit=1)
        return _jsonify_ok({"gate": "ok"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/diag/crypto")
def diag_crypto():
    return _jsonify_ok({
        "exchange": EXCHANGE_NAME,
        "trading_base": TRADING_BASE,
        "data_base": DATA_BASE,
        "app": APP_VERSION
    })

@app.get("/diag/candles")
def diag_candles():
    # ?symbols=BTC/USD,ETH/USD&tf=1Min&limit=1000
    symbols = request.args.get("symbols", "BTC/USD,ETH/USD").split(",")
    tf = request.args.get("tf", DEFAULT_TIMEFRAME)
    limit = int(request.args.get("limit", "1000"))
    try:
        bars = _get_bars(symbols, tf, limit)
        rows = {s: (len(bars.get(s, [])) if bars.get(s) else 0) for s in symbols}
        return jsonify({"ok": True, "rows": rows, "tf": tf, "limit": limit})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500

# -----------------------------
# Orders / Positions
# -----------------------------
@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status", "all")
    limit = int(request.args.get("limit", "50"))
    try:
        arr = _list_orders(status=status, limit=limit)
        return jsonify({"value": arr, "Count": len(arr)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500

@app.get("/positions")
def positions():
    try:
        arr = _list_positions()
        return jsonify(arr)  # plain array
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500

# -----------------------------
# Orders Attribution (strategy tag from client_order_id)
# -----------------------------
@app.get("/orders/attribution")
def orders_attribution():
    # /orders/attribution?days=1
    days = int(request.args.get("days", "1"))
    limit = int(request.args.get("limit", "1000"))
    cutoff = _now_utc() - timedelta(days=days)
    arr = _list_orders(status="all", limit=limit)
    per = {}
    total = 0
    wins = 0
    losses = 0
    realized_total = 0.0

    def _parse_dt(o):
        for k in ("filled_at", "completed_at", "submitted_at", "created_at"):
            t = o.get(k)
            if t:
                try:
                    return datetime.fromisoformat(t.replace("Z", "+00:00")).astimezone(timezone.utc)
                except Exception:
                    pass
        return None

    def _get_realized(o):
        for k in ("realized_pnl","pnl","realizedPnL","realized"):
            if k in o and o[k] is not None:
                try:
                    return float(o[k])
                except Exception:
                    return 0.0
        return 0.0

    for o in arr:
        dt = _parse_dt(o)
        if not dt or dt < cutoff:
            continue
        sid = str(o.get("client_order_id") or "")
        strat = "unknown"
        if "-" in sid:
            strat = sid.split("-", 1)[0] or "unknown"
        bucket = per.setdefault(strat, {"count":0,"wins":0,"losses":0,"realized":0.0})
        bucket["count"] += 1
        total += 1
        rpnl = _get_realized(o)
        if rpnl > 0:
            bucket["wins"] += 1
            wins += 1
        elif rpnl < 0:
            bucket["losses"] += 1
            losses += 1
        bucket["realized"] += rpnl
        realized_total += rpnl

    return _jsonify_ok({
        "days": days,
        "cutoff_utc": _iso(cutoff),
        "per_strategy": per,
        "totals": {
            "count": total,
            "wins": wins,
            "losses": losses,
            "realized": round(realized_total, 2)
        }
    })

# -----------------------------
# P&L Summary + Daily series
# -----------------------------
def _compute_unrealized_from_positions() -> float:
    try:
        pos = _list_positions()
        total = 0.0
        for p in pos:
            upnl = p.get("unrealized_pl") or p.get("unrealized_intraday_pl") or p.get("unrealizedPnl")
            total += _safe_float(upnl)
        return round(total, 2)
    except Exception:
        return 0.0

@app.get("/pnl/summary")
def pnl_summary():
    now = _now_utc()
    arr = _list_orders(status="all", limit=3000)

    def win_loss_realized_since(cut: datetime) -> Tuple[float, int, int, int]:
        wins = losses = count = 0
        total = 0.0
        for o in arr:
            t = o.get("filled_at") or o.get("completed_at") or o.get("submitted_at") or o.get("created_at")
            if not t:
                continue
            try:
                dt = datetime.fromisoformat(t.replace("Z","+00:00")).astimezone(timezone.utc)
            except Exception:
                continue
            if dt < cut:
                continue
            rpnl = 0.0
            for k in ("realized_pnl","pnl","realizedPnL","realized"):
                if k in o and o[k] is not None:
                    try: rpnl = float(o[k]); break
                    except: pass
            if rpnl > 0: wins += 1
            elif rpnl < 0: losses += 1
            total += rpnl
            count += 1
        return (round(total,2), wins, losses, count)

    start_today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    start_week  = start_today - timedelta(days=start_today.weekday())  # Monday 00:00 UTC
    start_month = datetime(now.year, now.month, 1, tzinfo=timezone.utc)

    r_today, w_today, l_today, c_today = win_loss_realized_since(start_today)
    r_week,  w_week,  l_week,  c_week  = win_loss_realized_since(start_week)
    r_month, w_month, l_month, c_month = win_loss_realized_since(start_month)

    unreal = _compute_unrealized_from_positions()

    cut_30d = now - timedelta(days=30)
    r_30, w_30, l_30, c_30 = win_loss_realized_since(cut_30d)
    wr_30 = (100.0 * w_30 / c_30) if c_30 > 0 else 0.0

    return jsonify({
        "as_of_utc": _iso(now),
        "realized": {
            "today": r_today,
            "week": r_week,
            "month": r_month
        },
        "unrealized": round(unreal, 2),
        "trades": {
            "count_30d": c_30,
            "wins_30d": w_30,
            "losses_30d": l_30,
            "win_rate_30d": round(wr_30, 2)
        },
        "version": APP_VERSION
    })

@app.get("/pnl/daily")
def pnl_daily():
    days = int(request.args.get("days", "84"))
    end = _now_utc().date()
    start = end - timedelta(days=days-1)
    arr = _list_orders(status="all", limit=4000)

    buckets: Dict[date, float] = { (start + timedelta(days=i)): 0.0 for i in range(days) }

    for o in arr:
        t = o.get("filled_at") or o.get("completed_at") or o.get("submitted_at") or o.get("created_at")
        if not t:
            continue
        try:
            dt = datetime.fromisoformat(t.replace("Z","+00:00")).astimezone(timezone.utc)
        except Exception:
            continue
        d = dt.date()
        if d < start or d > end:
            continue
        rpnl = 0.0
        for k in ("realized_pnl","pnl","realizedPnL","realized"):
            if k in o and o[k] is not None:
                try: rpnl = float(o[k]); break
                except: pass
        buckets[d] = buckets.get(d, 0.0) + rpnl

    series = [{"date": d.isoformat(), "pnl": round(v, 2)} for d, v in sorted(buckets.items())]
    return jsonify({
        "ok": True,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": days,
        "series": series,
        "version": APP_VERSION
    })

# -----------------------------
# Config: symbols GET/POST
# -----------------------------
def _load_symbols() -> List[str]:
    try:
        with open(SYMBOLS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        syms = data.get("symbols", [])
        return [s for s in syms if isinstance(s, str)]
    except Exception:
        return []

def _save_symbols(symbols: List[str]) -> None:
    payload = {"symbols": symbols, "updated_utc": _iso(_now_utc()), "version": APP_VERSION}
    with open(SYMBOLS_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

@app.route("/config/symbols", methods=["GET","POST","OPTIONS"])
def config_symbols():
    if request.method == "GET":
        syms = _load_symbols()
        return _jsonify_ok({"symbols": syms, "count": len(syms), "version": APP_VERSION})
    if request.method == "POST":
        body = request.get_json(silent=True) or {}
        syms = body.get("symbols") or []
        if not isinstance(syms, list):
            return jsonify({"ok": False, "error": "symbols must be a list of strings"}), 400
        cleaned = []
        for s in syms:
            s = str(s).strip().upper()
            if s and "/" in s:
                cleaned.append(s)
        _save_symbols(cleaned)
        return _jsonify_ok({"symbols": cleaned, "count": len(cleaned), "version": APP_VERSION})
    return jsonify({"ok": True})

# -----------------------------
# Scan endpoint shim (kept)
# -----------------------------
def _load_scan_module(name: str):
    mod = importlib.import_module(name)
    if not hasattr(mod, "scan"):
        raise RuntimeError(f"{name}.scan(...) not found")
    return mod

@app.route("/scan/<name>", methods=["POST","OPTIONS"])
def scan_named(name: str):
    name = name.lower().strip()
    if name not in ("c1","c2","c3","c4","c5","c6"):
        return jsonify({"ok": False, "error": f"unknown strategy '{name}'"}), 404

    params = dict(request.args)
    body = request.get_json(silent=True) or {}
    if isinstance(body, dict):
        params.update({k:str(v) for k,v in body.items() if k not in params})

    tf = params.get("timeframe", DEFAULT_TIMEFRAME)
    limit = int(params.get("limit", "600"))
    dry = params.get("dry", params.get("paper", "1")) in ("1","true","True","yes")
    notional = float(params.get("notional", "0") or 0)

    syms = _load_symbols()
    if not syms:
        syms = ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD","AVAX/USD","LINK/USD","BCH/USD","LTC/USD"]

    try:
        bars = _get_bars(syms, tf, limit)
        mod = _load_scan_module(name)
        results = mod.scan(symbols=syms, timeframe=tf, limit=limit, dry=dry, notional=notional, candles=bars, **params)
        return jsonify({"ok": True, "dry": dry, "results": results, "strategy": name})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc(), "strategy": name}), 500

# -----------------------------
# Rich Dashboard (no f-string; we replace a token instead)
# -----------------------------
@app.get("/dashboard")
def dashboard():
    html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Crypto Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root {
      --bg: #0b1220;
      --panel: #0f172a;
      --muted: #94a3b8;
      --text: #e2e8f0;
      --accent: #38bdf8;
      --green: #10b981;
      --red: #ef4444;
      --yellow: #f59e0b;
      --border: #1f2a44;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: radial-gradient(1200px 800px at 30% -10%, #12233f, transparent), var(--bg);
      color: var(--text);
      font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
    }
    header {
      display: flex; align-items: center; justify-content: space-between;
      padding: 16px 20px; border-bottom: 1px solid var(--border);
      position: sticky; top: 0; background: rgba(11,18,32,0.8); backdrop-filter: blur(8px); z-index: 10;
    }
    h1 { margin: 0; font-size: 20px; letter-spacing: .5px; }
    .tag { color: var(--muted); font-size: 12px; }
    main { padding: 20px; max-width: 1280px; margin: 0 auto; }
    .cards {
      display: grid; grid-template-columns: repeat(6, minmax(160px, 1fr)); gap: 12px;
    }
    .card {
      background: linear-gradient(180deg, #0f172a, #0c1426);
      border: 1px solid var(--border); border-radius: 12px; padding: 12px;
    }
    .k { color: var(--muted); font-size: 12px; }
    .v { font-size: 22px; font-weight: 700; margin-top: 6px; }
    .grid2 {
      display: grid; grid-template-columns: 2fr 1fr; gap: 16px; margin-top: 16px;
    }
    .panel { background: var(--panel); border: 1px solid var(--border); border-radius: 12px; padding: 12px; }
    .panel h3 { margin: 0 0 10px 0; font-size: 16px; }
    .calendar { display: grid; grid-template-columns: repeat(14, 12px); gap: 4px; }
    .cell { width: 12px; height: 12px; border-radius: 3px; background: #334155; }
    .cell.pos { background: var(--green); }
    .cell.neg { background: var(--red); }
    .cell.zero { background: #64748b; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th, td { padding: 8px; border-bottom: 1px solid var(--border); }
    th { text-align: left; color: var(--muted); font-weight: 500; }
    td.mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; }
    .controls input, .controls select, .controls button, .controls textarea {
      background: #0b1220; color: var(--text); border: 1px solid var(--border); border-radius: 8px; padding: 8px;
      font-size: 12px;
    }
    .controls button.primary { background: #0ea5e9; border-color: #0284c7; }
    .pill { display:inline-block; padding:2px 8px; border-radius:999px; font-size:11px; background:#0b1220; border:1px solid var(--border); color:var(--muted); }
    .ok { color: var(--green); } .bad { color: var(--red); } .warn { color: var(--yellow); }
    .right { text-align: right; }
    .footer { color: var(--muted); font-size: 12px; margin-top: 20px; text-align: right; }
    @media (max-width: 1100px) {
      .cards { grid-template-columns: repeat(2, minmax(160px, 1fr)); }
      .grid2 { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
<header>
  <div>
    <h1>Crypto Dashboard <span class="tag">v__APP_VERSION__</span></h1>
    <div class="tag" id="env"></div>
  </div>
  <div class="row controls">
    <select id="scanStrategy">
      <option value="c1">c1</option><option value="c2">c2</option><option value="c3">c3</option>
      <option value="c4">c4</option><option value="c5">c5</option><option value="c6">c6</option>
    </select>
    <select id="tf">
      <option>1Min</option><option>5Min</option><option>15Min</option>
    </select>
    <input id="limit" type="number" value="600" min="50" step="50" style="width:90px"/>
    <input id="notional" type="number" value="5" min="0" step="1" style="width:90px"/>
    <button class="primary" id="runScan">Run Scan</button>
  </div>
</header>
<main>
  <section class="cards" id="cards"></section>

  <div class="grid2">
    <div class="panel">
      <h3>Daily Realized P&L (Calendar)</h3>
      <div id="cal" class="calendar"></div>
      <div class="tag" id="calRange"></div>
    </div>

    <div class="panel">
      <h3>Per-Strategy Attribution (last <span id="attrDays">1</span>d)</h3>
      <div class="row controls" style="margin-bottom:8px;">
        <input id="attrDaysInput" type="number" value="1" min="1" max="30" style="width:80px"/>
        <button id="refreshAttr">Refresh</button>
      </div>
      <table id="attrTable">
        <thead><tr><th>Strategy</th><th>Orders</th><th>Wins</th><th>Losses</th><th class="right">Realized</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <div class="grid2" style="margin-top:16px;">
    <div class="panel">
      <h3>Positions</h3>
      <table id="posTable">
        <thead><tr>
          <th>Symbol</th><th class="right">Qty</th><th class="right">Avg Entry</th><th class="right">Mkt Value</th><th class="right">Unrealized</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>

    <div class="panel">
      <h3>Recent Orders</h3>
      <div class="row controls" style="margin-bottom:8px;">
        <select id="orderStatus">
          <option value="all" selected>all</option>
          <option value="filled">filled</option>
          <option value="open">open</option>
          <option value="closed">closed</option>
        </select>
        <input id="orderLimit" type="number" value="50" min="10" step="10" style="width:90px"/>
        <button id="refreshOrders">Refresh</button>
      </div>
      <table id="ordTable">
        <thead><tr>
          <th>Time (UTC)</th><th>Symbol</th><th>Side</th><th>Status</th><th>Tag</th><th class="right">Notional</th><th class="right">Qty</th><th class="right">Fill Px</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <div class="panel" style="margin-top:16px;">
    <h3>Symbols (Default Scan List)</h3>
    <div class="row controls" style="margin-bottom:8px;">
      <textarea id="symbols" rows="3" style="width:100%" placeholder="One per line, e.g. BTC/USD"></textarea>
    </div>
    <div class="row controls">
      <button id="loadSymbols">Load</button>
      <button class="primary" id="saveSymbols">Save</button>
      <span id="symbolsMsg" class="tag"></span>
    </div>
  </div>

  <div class="footer" id="sys"></div>
</main>

<script>
async function getJSON(u) { const r = await fetch(u); return r.json(); }
async function postJSON(u, body) {
  const r = await fetch(u, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : null
  });
  return r.json();
}

function fmt2(n) { if (n===null||n===undefined||isNaN(n)) return "0.00"; return Number(n).toFixed(2); }
function clsSign(n) { if (n>0) return 'ok'; if (n<0) return 'bad'; return 'warn'; }
function stratFromCOID(coid) {
  if (!coid) return 'unknown';
  const i = coid.indexOf('-');
  if (i<=0) return 'unknown';
  return coid.slice(0,i);
}

async function loadEnv() {
  const h = await getJSON('/health/versions');
  document.getElementById('env').textContent = `exchange=${h.exchange}  trading_base=${h.trading_base}  data_base=${h.data_base}`;
  const systems = h.systems || {};
  const s = Object.entries(systems).map(([k,v]) => `${k}: ${v}`).join('  ·  ');
  document.getElementById('sys').textContent = `systems → ${s}`;
}

async function loadCards() {
  const s = await getJSON('/pnl/summary');
  const cards = [
    {k:'Realized Today', v: fmt2(s.realized.today)},
    {k:'Realized Week',  v: fmt2(s.realized.week)},
    {k:'Realized Month', v: fmt2(s.realized.month)},
    {k:'Unrealized P&L', v: fmt2(s.unrealized)},
    {k:'Win Rate (30d)', v: (s.trades.win_rate_30d||0).toFixed(2) + '%'},
    {k:'Trades (30d)',   v: s.trades.count_30d||0},
  ];
  const el = document.getElementById('cards');
  el.innerHTML = cards.map(c => `
    <div class="card">
      <div class="k">${c.k}</div>
      <div class="v ${clsSign(parseFloat(c.v))}">${c.v}</div>
    </div>
  ).join('');
}

async function loadCalendar(days=120) {
  const d = await getJSON('/pnl/daily?days=' + days);
  const el = document.getElementById('cal');
  const cells = d.series.map(x => {
    const v = Number(x.pnl||0);
    const c = v>0 ? 'pos' : (v<0 ? 'neg' : 'zero');
    return `<div class="cell ${c}" title="${x.date} → ${fmt2(v)}"></div>`;
  }).join('');
  el.innerHTML = cells;
  document.getElementById('calRange').textContent = `${d.start} → ${d.end}`;
}

async function loadAttribution(days) {
  const d = await getJSON('/orders/attribution?days=' + days);
  document.getElementById('attrDays').textContent = days;
  const tb = document.querySelector('#attrTable tbody');
  const rows = Object.entries(d.per_strategy || {}).map(([k,v]) => `
    <tr>
      <td>${k}</td>
      <td class="mono">${v.count||0}</td>
      <td class="mono ok">${v.wins||0}</td>
      <td class="mono bad">${v.losses||0}</td>
      <td class="mono right ${clsSign(v.realized||0)}">${fmt2(v.realized||0)}</td>
    </tr>
  `).join('');
  tb.innerHTML = rows || '<tr><td colspan="5" class="tag">No orders in window.</td></tr>';
}

async function loadPositions() {
  const arr = await getJSON('/positions');
  const tb = document.querySelector('#posTable tbody');
  const rows = (arr||[]).map(p => `
    <tr>
      <td>${p.symbol||p.asset_symbol||''}</td>
      <td class="mono right">${fmt2(p.qty||p.quantity||0)}</td>
      <td class="mono right">${fmt2(p.avg_entry_price||0)}</td>
      <td class="mono right">${fmt2(p.market_value||0)}</td>
      <td class="mono right ${clsSign(p.unrealized_pl||p.unrealizedPnl||0)}">${fmt2(p.unrealized_pl||p.unrealizedPnl||0)}</td>
    </tr>
  `).join('');
  tb.innerHTML = rows || '<tr><td colspan="5" class="tag">No positions.</td></tr>';
}

async function loadOrders() {
  const status = document.getElementById('orderStatus').value;
  const limit  = document.getElementById('orderLimit').value;
  const data = await getJSON(`/orders/recent?status=${encodeURIComponent(status)}&limit=${encodeURIComponent(limit)}`);
  const arr = data.value || data || [];
  const tb = document.querySelector('#ordTable tbody');
  const rows = (arr||[]).map(o => `
    <tr>
      <td class="mono">${o.filled_at || o.submitted_at || o.created_at || ''}</td>
      <td>${o.symbol||o.asset_symbol||''}</td>
      <td>${o.side||''}</td>
      <td>${o.status||''}</td>
      <td><span class="pill">${stratFromCOID(o.client_order_id)}</span></td>
      <td class="mono right">${fmt2(o.notional||0)}</td>
      <td class="mono right">${fmt2(o.filled_qty||o.qty||0)}</td>
      <td class="mono right">${fmt2(o.filled_avg_price||0)}</td>
    </tr>
  `).join('');
  tb.innerHTML = rows || '<tr><td colspan="8" class="tag">No recent orders.</td></tr>';
}

async function loadSymbols() {
  const d = await getJSON('/config/symbols');
  const box = document.getElementById('symbols');
  const list = (d.symbols||[]).join('\\n');
  box.value = list;
  document.getElementById('symbolsMsg').textContent = `Loaded ${d.count||0} symbols`;
}

async function saveSymbols() {
  const raw = document.getElementById('symbols').value||'';
  const arr = raw.split(/\\n|,|\\s+/).map(s => s.trim()).filter(Boolean);
  const cleaned = [];
  for (const s of arr) {
    const u = s.toUpperCase();
    if (u.includes('/')) cleaned.push(u);
  }
  const res = await postJSON('/config/symbols', { symbols: cleaned });
  document.getElementById('symbolsMsg').textContent = res.ok ? `Saved ${res.count||0} symbols` : (res.error||'Error');
  return cleaned;
}

async function runScan() {
  const strat = document.getElementById('scanStrategy').value;
  const tf = document.getElementById('tf').value;
  const limit = document.getElementById('limit').value;
  const notional = document.getElementById('notional').value;
  const url = `/scan/${encodeURIComponent(strat)}?dry=0&timeframe=${encodeURIComponent(tf)}&limit=${encodeURIComponent(limit)}&notional=${encodeURIComponent(notional)}`;
  const res = await postJSON(url, null);
  alert(res.ok ? `Scan ${strat} queued: ${res.results?.length||0} results` : `Error: ${res.error||'unknown'}`);
  await Promise.all([loadOrders(), loadPositions(), loadCards()]);
}

document.getElementById('refreshAttr').addEventListener('click', () => {
  const d = parseInt(document.getElementById('attrDaysInput').value||'1',10);
  loadAttribution(Math.max(1, Math.min(30, d)));
});
document.getElementById('refreshOrders').addEventListener('click', loadOrders);
document.getElementById('loadSymbols').addEventListener('click', loadSymbols);
document.getElementById('saveSymbols').addEventListener('click', saveSymbols);
document.getElementById('runScan').addEventListener('click', runScan);

(async function init(){
  await loadEnv();
  await loadCards();
  await loadCalendar(120);
  await loadAttribution(1);
  await loadPositions();
  await loadOrders();
  await loadSymbols();
})();
</script>
</body>
</html>
"""
    return Response(html.replace("__APP_VERSION__", APP_VERSION), mimetype="text/html")

# -----------------------------
# Admin: reload strategies (dev only)
# -----------------------------
@app.post("/admin/reload")
def admin_reload():
    if not ALLOW_RELOAD:
        return jsonify({"ok": False, "error": "reload disabled; set ALLOW_ADMIN_RELOAD=1"}), 403
    try:
        versions = _reload_strategies()
        return _jsonify_ok({"reloaded": versions})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "trace": traceback.format_exc()}), 500

# --------------------------------
# Static passthrough (optional)
# --------------------------------
@app.get("/static/<path:filename>")
def static_files(filename: str):
    try:
        with open(os.path.join("static", filename), "rb") as f:
            b = f.read()
        return Response(b)
    except Exception:
        return Response("Not found", status=404)

# --------------------------------
# Main
# --------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
