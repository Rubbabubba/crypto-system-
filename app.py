# app.py — Crypto Trading System API + Dashboard (v1.8.6)
# - Loads strategies c1..c6 dynamically
# - Exposes POST /scan/<name>
# - Health, routes, positions, orders, pnl stubs wired to your existing services
# - Serves a simple HTML dashboard at /

from __future__ import annotations
import os, time, json, math, traceback, logging, importlib
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

from flask import Flask, request, jsonify, Response

APP_VERSION = os.getenv("APP_VERSION", "1.8.6")
EXCHANGE_NAME = os.getenv("EXCHANGE", "alpaca")
TRADING_BASE = os.getenv("TRADING_BASE", "https://paper-api.alpaca.markets")
DATA_BASE = os.getenv("DATA_BASE", "https://data.alpaca.markets")

# Symbols cache (GET/POST /config/symbols)
_SYMBOLS: List[str] = [
    "BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD",
    "AVAX/USD","LINK/USD","BCH/USD","LTC/USD",
]

# --- Logging ---
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# --- Light-weight broker/data adapters (expect your real ones in services/*) ---
# You can swap these with your concrete implementations.

class Broker:
    def __init__(self):
        pass

    # NOTE: For attribution, we always pass client_order_id (coid)
    def place_order(self, symbol: str, side: str, notional: float,
                    client_order_id: str, tif: str = "gtc", order_type: str = "market") -> Dict[str, Any]:
        # TODO: replace with your services/exchange_exec.py actual submission
        # Here we mimic a broker echo response
        now = datetime.now(timezone.utc).isoformat()
        return dict(
            id=os.urandom(8).hex(),
            status="filled",
            filled_at=now,
            filled_avg_price=None,
            filled_qty=None,
            symbol=symbol,
            side=side,
            notional=notional,
            client_order_id=client_order_id,
            created_at=now,
            submitted_at=now,
        )

    def recent_orders(self, status: str = "all", limit: int = 50) -> List[Dict[str, Any]]:
        # TODO: wire to your order store; server previously returned { value:[...], Count:N }
        # We return an array for simplicity
        return []

    def positions(self) -> List[Dict[str, Any]]:
        # TODO: wire to your real positions endpoint
        return []

    def pnl_summary(self) -> Dict[str, Any]:
        # TODO: replace with real computation
        return {
            "as_of_utc": datetime.now(timezone.utc).isoformat(),
            "realized": {"today": 0.0, "week": 0.0, "month": 0.0},
            "unrealized": 0.0,
            "trades": {"count_30d": 0, "wins_30d": 0, "losses_30d": 0, "win_rate_30d": 0.0},
            "version": APP_VERSION,
        }

    def pnl_daily(self, days: int = 7) -> Dict[str, Any]:
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=days-1)
        return {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "days": days,
            "series": [{"date": (start+timedelta(days=i)).isoformat(), "pnl": 0.0} for i in range(days)],
            "version": APP_VERSION,
        }

BROKER = Broker()

# Simple OHLCV store using Alpaca Crypto v2 bars (replace with your existing data layer)
import requests

def fetch_bars(symbol: str, timeframe: str = "1Min", limit: int = 600) -> List[Dict[str, Any]]:
    # Minimal, public-like fetcher; in your real code, use your auth + services/diag path
    # Here we purposely try your existing diag route first if present
    svc = os.getenv("SERVICE_URL")
    if svc:
        try:
            u = f"{svc}/diag/candles?symbols={requests.utils.quote(symbol)}&tf={timeframe}&limit={limit}"
            r = requests.get(u, timeout=10)
            if r.ok:
                js = r.json()
                # Expect rows keyed by symbol or a flat list under data
                if isinstance(js, dict) and "data" in js:
                    return js["data"]
        except Exception:
            pass
    # Fallback: return empty (strategies will produce flat/no_signal)
    return []

# --- Strategy loader ---

def load_strategies() -> Dict[str, Any]:
    systems: Dict[str, Any] = {}
    for name in ("c1","c2","c3","c4","c5","c6"):
        try:
            mod = importlib.import_module(name)
            sysobj = getattr(mod, "system", None)
            if not sysobj:
                raise RuntimeError(f"{name} missing `system`")
            systems[name] = sysobj
            log.info("Loaded %s v%s", name, getattr(sysobj, "version", "?"))
        except Exception:
            log.error("Failed loading %s:\n%s", name, traceback.format_exc())
    return systems

SYSTEMS = load_strategies()

# --- Flask ---
app = Flask(__name__)

@app.get("/")
def dashboard():
    html = """
<!DOCTYPE html>
<html>
<head>
  <meta charset=\"utf-8\"/>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>
  <title>Crypto Trading System — Dashboard</title>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;padding:24px;background:#0b0f14;color:#e7f0ff}
    h1{font-weight:700;margin:0 0 8px}
    .row{display:flex;gap:16px;flex-wrap:wrap}
    .card{background:#111826;border:1px solid #1f2a3a;border-radius:14px;padding:16px;flex:1;min-width:320px}
    button{background:#2a72ff;color:white;border:none;border-radius:10px;padding:10px 14px;cursor:pointer}
    button:disabled{opacity:0.6}
    table{width:100%;border-collapse:collapse;margin-top:8px;font-size:14px}
    th,td{border-bottom:1px solid #1f2a3a;padding:8px;text-align:left}
    .pill{display:inline-block;padding:2px 8px;border-radius:999px;background:#1f2a3a}
    .ok{color:#7dff9b}
    .err{color:#ff8a7d}
    .muted{opacity:.8}
    code{background:#0b1320;padding:2px 6px;border-radius:6px}
  </style>
</head>
<body>
  <h1>Crypto Trading System <span class=\"muted\">(v<span id=\"appv\"></span>)</span></h1>
  <div class=\"row\">
    <div class=\"card\">
      <h3>Health</h3>
      <div id=\"health\"></div>
    </div>
    <div class=\"card\">
      <h3>Symbols</h3>
      <div>
        <input id=\"symbols\" style=\"width:100%\" placeholder=\"comma-separated e.g. BTC/USD,ETH/USD,...\"/>
        <div style=\"margin-top:8px\"><button id=\"saveSymbols\">Save Symbols</button></div>
      </div>
    </div>
  </div>

  <div class=\"row\" style=\"margin-top:16px\">
    <div class=\"card\">
      <h3>Run Scan</h3>
      <div style=\"display:flex;gap:8px;flex-wrap:wrap\">
        <select id=\"strat\">
          <option>c1</option><option>c2</option><option>c3</option>
          <option>c4</option><option>c5</option><option>c6</option>
        </select>
        <select id=\"tf\"><option>1Min</option><option>5Min</option><option>15Min</option></select>
        <input id=\"limit\" type=\"number\" value=\"600\" min=\"50\" max=\"2000\"/>
        <label><input id=\"dry\" type=\"checkbox\" checked/> Dry</label>
        <input id=\"notional\" type=\"number\" step=\"0.01\" value=\"5\"/>
        <button id=\"run\">Run Scan</button>
      </div>
      <div id=\"scanOut\" style=\"margin-top:8px\"></div>
    </div>
    <div class=\"card\">
      <h3>P&amp;L</h3>
      <div id=\"pnl\"></div>
    </div>
  </div>

  <div class=\"card\" style=\"margin-top:16px\">
    <h3>Orders Attribution (1d)</h3>
    <div id=\"attr\"></div>
  </div>

<script>
async function j(u, opt={}){ const r = await fetch(u, opt); if(!r.ok) throw new Error(r.status+" "+u); return await r.json(); }
function q(id){ return document.getElementById(id); }
function fmt(n){ return (n!=null)? (typeof n==="number"? n.toFixed(2): n): "" }

async function loadHealth(){
  const d = await j('/health/versions');
  q('appv').textContent = d.app;
  const s = d.systems || {};
  let rows = Object.keys(s).sort().map(k=>`<tr><td>${k}</td><td>${s[k]}</td></tr>`).join('');
  if(!rows) rows = '<tr><td colspan="2" class="muted">no systems</td></tr>';
  q('health').innerHTML = `
    <div>exchange: <code>${d.exchange||''}</code></div>
    <div>data_base: <code>${d.data_base||''}</code></div>
    <div>trading_base: <code>${d.trading_base||''}</code></div>
    <table><thead><tr><th>strategy</th><th>version</th></tr></thead><tbody>${rows}</tbody></table>`;
}

async function loadSymbols(){
  try{
    const r = await j('/config/symbols');
    const syms = r.symbols||[];
    q('symbols').value = syms.join(',');
  }catch(e){ q('symbols').placeholder='(failed to load)'; }
}

q('saveSymbols').onclick = async ()=>{
  const arr = q('symbols').value.split(',').map(s=>s.trim()).filter(Boolean);
  const res = await j('/config/symbols', {method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify({symbols:arr})});
  alert('Saved '+(res.count||arr.length)+' symbols');
};

q('run').onclick = async ()=>{
  const strat = q('strat').value;
  const tf = q('tf').value;
  const limit = q('limit').value;
  const dry = q('dry').checked? 1: 0;
  const notional = q('notional').value;
  const params = new URLSearchParams({dry, timeframe:tf, limit, notional});
  const res = await j(`/scan/${encodeURIComponent(strat)}?`+params.toString(), {method:'POST'});
  const rows = (res.results||[]).slice(0,30).map(r=>`<tr>
      <td>${r.symbol||''}</td><td>${r.action||''}</td><td>${r.reason||''}</td>
      <td>${fmt(r.close)}</td><td>${fmt(r.ema||r.ema_fast||r.ma1||'')}</td><td>${fmt(r.atr)}</td>
    </tr>`).join('');
  q('scanOut').innerHTML = `<div class="pill">ok: ${res.ok}</div> <div class="pill">dry: ${res.dry}</div>
    <table><thead><tr><th>symbol</th><th>action</th><th>reason</th><th>close</th><th>ma/ema</th><th>ATR</th></tr></thead><tbody>${rows||'<tr><td colspan="6" class="muted">no results</td></tr>'}</tbody></table>`;
  loadAttr();
};

async function loadPnl(){
  try{
    const s = await j('/pnl/summary');
    q('pnl').innerHTML = `<div>as_of_utc: <code>${s.as_of_utc||''}</code></div>
      <div>realized today: <b class="ok">${fmt(s.realized?.today)}</b>, week: ${fmt(s.realized?.week)}, month: ${fmt(s.realized?.month)}</div>
      <div>unrealized: <b class="${(s.unrealized||0)>=0?'ok':'err'}">${fmt(s.unrealized)}</b></div>`;
  }catch(e){ q('pnl').innerHTML = '<span class="muted">pnl unavailable</span>'; }
}

async function loadAttr(){
  try{
    const a = await j('/orders/attribution?days=1');
    const m = a.per_strategy||{}; const keys = Object.keys(m).sort();
    const rows = keys.map(k=>{const v=m[k]||{}; return `<tr><td>${k}</td><td>${v.count||0}</td><td>${v.wins||0}</td><td>${v.losses||0}</td><td>${fmt(v.realized||0)}</td></tr>`}).join('');
    q('attr').innerHTML = `<table><thead><tr><th>strategy</th><th>count</th><th>wins</th><th>losses</th><th>realized</th></tr></thead><tbody>${rows||'<tr><td colspan="5" class="muted">no data</td></tr>'}</tbody></table>`;
  }catch(e){ q('attr').innerHTML = '<span class="muted">attribution unavailable</span>'; }
}

loadHealth(); loadSymbols(); loadPnl(); loadAttr();
</script>
</body>
</html>
    """
    return Response(html, mimetype="text/html")

# --- API: health/routes ---
@app.get("/health/versions")
def health_versions():
    systems_state = {}
    for name in ("c1","c2","c3","c4","c5","c6"):
        v = SYSTEMS.get(name)
        systems_state[name] = getattr(v, "version", "unavailable") if v else "unavailable"
    return dict(
        app=APP_VERSION,
        exchange=EXCHANGE_NAME,
        trading_base=TRADING_BASE,
        data_base=DATA_BASE,
        systems=systems_state,
    )

@app.get("/routes")
def routes_list():
    rt = []
    for r in app.url_map.iter_rules():
        rt.append(dict(rule=str(r), methods=sorted(list(r.methods))))
    return {"routes": rt}

# --- API: symbols (persist in memory; swap to your storage if needed) ---
@app.get("/config/symbols")
def get_symbols():
    return {"ok": True, "symbols": _SYMBOLS}

@app.post("/config/symbols")
def set_symbols():
    global _SYMBOLS
    js = request.get_json(silent=True) or {}
    syms = js.get("symbols") or []
    _SYMBOLS = [s for s in syms if isinstance(s, str) and s]
    return {"ok": True, "count": len(_SYMBOLS), "symbols": _SYMBOLS, "version": APP_VERSION}

# --- API: positions / pnl / orders passthroughs ---
@app.get("/positions")
def api_positions():
    try:
        return jsonify(BROKER.positions())
    except Exception:
        log.error("positions error:\n%s", traceback.format_exc())
        return ("", 500)

@app.get("/pnl/summary")
def api_pnl_summary():
    try:
        return jsonify(BROKER.pnl_summary())
    except Exception:
        log.error("pnl summary error:\n%s", traceback.format_exc())
        return ("", 500)

@app.get("/pnl/daily")
def api_pnl_daily():
    try:
        days = int(request.args.get("days", 7))
        return jsonify(BROKER.pnl_daily(days))
    except Exception:
        log.error("pnl daily error:\n%s", traceback.format_exc())
        return ("", 500)

@app.get("/orders/recent")
def api_orders_recent():
    try:
        status = request.args.get("status", "all")
        limit = int(request.args.get("limit", 50))
        arr = BROKER.recent_orders(status=status, limit=limit)
        # For maximum compatibility, return both array and {value,Count}
        return jsonify({"value": arr, "Count": len(arr)})
    except Exception:
        log.error("orders recent error:\n%s", traceback.format_exc())
        return ("", 500)

@app.get("/orders/attribution")
def api_orders_attr():
    try:
        # TODO: implement from your database of orders. Here we return a stable empty object
        return jsonify({"per_strategy": {}, "ok": True})
    except Exception:
        return jsonify({"per_strategy": {}, "ok": False})

# --- API: diag/candles (optional convenience passthrough for client scripts) ---
@app.get("/diag/candles")
def api_diag_candles():
    try:
        syms = request.args.get("symbols", "")
        timeframe = request.args.get("tf", "1Min")
        limit = int(request.args.get("limit", 600))
        rows = {}
        for s in syms.split(','):
            s = s.strip()
            if not s: continue
            bars = fetch_bars(s, timeframe, limit)
            rows[s] = len(bars)
        return jsonify({"rows": rows, "ok": True})
    except Exception:
        log.error("diag/candles error:\n%s", traceback.format_exc())
        return ("", 500)

# --- API: scans ---
@app.post("/scan/<name>")
def api_scan(name: str):
    sysobj = SYSTEMS.get(name)
    if not sysobj:
        return jsonify({"ok": False, "error": f"unknown_strategy:{name}"}), 404

    params = request.args.to_dict()
    dry = str(params.pop("dry", "1")).lower() in ("1","true","yes")
    timeframe = params.pop("timeframe", "1Min")
    limit = int(params.pop("limit", 600))
    notional = float(params.pop("notional", 0) or 0)
    symbols = list(_SYMBOLS)

    try:
        results = sysobj.scan(
            get_bars=fetch_bars,
            symbols=symbols,
            timeframe=timeframe,
            limit=limit,
            params=params,
            notional=notional,
            dry=dry,
            tag=name,
            broker=BROKER,
        )
        return jsonify({"ok": True, "dry": dry, "results": results})
    except Exception:
        log.error("scan %s error:\n%s", name, traceback.format_exc())
        return jsonify({"ok": False, "error": "exception"}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)