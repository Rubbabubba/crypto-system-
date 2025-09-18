# app.py
from __future__ import annotations
import os, json, time, importlib
from typing import Any, Dict, List, Callable
from datetime import datetime, timezone

from flask import Flask, request, jsonify, Response, redirect

# ----------------------------
# App / Env
# ----------------------------
APP_VERSION = os.environ.get("APP_VERSION", "1.6.0")
SYSTEM_NAME = "crypto"

CRYPTO_EXCHANGE = os.environ.get("CRYPTO_EXCHANGE", "alpaca")
CRYPTO_SYMBOLS  = [s.strip() for s in os.environ.get("CRYPTO_SYMBOLS", "BTC/USD,ETH/USD,SOL/USD,DOGE/USD").split(",") if s.strip()]

ALPACA_TRADING_BASE = os.environ.get("CRYPTO_TRADING_BASE") or os.environ.get("ALPACA_TRADING_BASE") or "https://paper-api.alpaca.markets/v2"
ALPACA_DATA_BASE    = os.environ.get("CRYPTO_DATA_BASE")    or os.environ.get("ALPACA_DATA_BASE")    or "https://data.alpaca.markets/v1beta3/crypto/us"

API_KEY    = os.environ.get("CRYPTO_API_KEY") or os.environ.get("APCA_API_KEY_ID")
API_SECRET = os.environ.get("CRYPTO_API_SECRET") or os.environ.get("APCA_API_SECRET_KEY")

# ----------------------------
# Imports: Market & Broker
# ----------------------------
# Market data service (your repo file)
try:
    from services.market_crypto import MarketCrypto  # must expose MarketCrypto.from_env()
except Exception:
    MarketCrypto = None  # type: ignore

# Execution service (new file we added earlier)
try:
    from services.exchange_exec import ExchangeExec  # must expose ExchangeExec.from_env()
except Exception:
    ExchangeExec = None  # type: ignore

# Instantiate shared services
def _make_market():
    if MarketCrypto is None:
        raise RuntimeError("services.market_crypto.MarketCrypto not found. Ensure services/market_crypto.py exists.")
    return MarketCrypto.from_env()

def _make_broker():
    if ExchangeExec is None:
        raise RuntimeError("services.exchange_exec.ExchangeExec not found. Ensure services/exchange_exec.py exists.")
    return ExchangeExec.from_env()

market = _make_market()
broker = _make_broker()

# ----------------------------
# Flask
# ----------------------------
app = Flask(__name__)

# ----------------------------
# Helpers
# ----------------------------
def _ok(data: Dict[str, Any], headers: Dict[str, str] | None = None):
    resp = jsonify(data)
    if headers:
        for k, v in headers.items():
            resp.headers[k] = v
    return resp, 200

def _err(msg: str, code: int = 400):
    return jsonify({"ok": False, "error": msg}), code

def _bool(val: Any, default: bool=False) -> bool:
    if val is None:
        return default
    s = str(val).strip().lower()
    return s in ("1","true","yes","y","on")

def _float(val: Any, default: float | None=None):
    try:
        return float(val)
    except Exception:
        return default

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _strategy_module(name: str):
    """Import a strategy module from strategies.<name>"""
    return importlib.import_module(f"strategies.{name}")

def _mod_version(mod_name: str) -> str:
    try:
        m = importlib.import_module(mod_name)
        return getattr(m, "__version__", "unknown")
    except Exception:
        return "error"

def _collect_params_from_request(prefix: str = "param.") -> Dict[str, Any]:
    """
    Accepts query items like ?param.C1_RSI_BUY=50 and turns into {"C1_RSI_BUY": "50"}.
    Also accepts JSON body with {"params": {...}}
    """
    params: Dict[str, Any] = {}
    # querystring
    for k, v in request.args.items():
        if k.startswith(prefix):
            params[k[len(prefix):]] = v
    # body
    if request.is_json:
        body = request.get_json(silent=True) or {}
        if isinstance(body, dict) and "params" in body and isinstance(body["params"], dict):
            params.update(body["params"])
    return params

def _inline_logger(tag: str) -> Callable[[str], None]:
    def log_fn(msg: str):
        app.logger.info("[%s] %s", tag, msg)
    return log_fn

# ----------------------------
# Strategy runner
# ----------------------------
def _run_strategy_direct(tag: str, mod, symbols: List[str], params: Dict[str, Any], dry: bool):
    """
    Calls strategy.run(market, broker, symbols, params, dry=<bool>, log=<callable>, pwrite=<fn>)
    and returns the module's results list (or an error).
    """
    pbuf: List[str] = []
    def pwrite(s: str):  # for strategies that want to 'print'
        pbuf.append(str(s))

    try:
        results = mod.run(market, broker, symbols, params, dry=dry, log=_inline_logger(tag), pwrite=pwrite)
        return {"ok": True, "results": results, "prints": pbuf}
    except TypeError as te:
        # Older strategies may not accept 'log'/'pwrite'. Retry more permissive signatures.
        try:
            results = mod.run(market, broker, symbols, params, dry=dry, pwrite=pwrite)  # type: ignore
            return {"ok": True, "results": results, "prints": pbuf}
        except Exception as e2:
            app.logger.error("[%s] inline run failed: %s", tag, e2, exc_info=True)
            return {"ok": False, "error": str(e2)}
    except Exception as e:
        app.logger.error("[%s] inline run failed: %s", tag, e, exc_info=True)
        return {"ok": False, "error": str(e)}

# ----------------------------
# Routes: UI
# ----------------------------
DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Crypto Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root {
    --bg:#0b0f14; --panel:#121821; --text:#e6edf3; --muted:#8aa0b4; --ok:#2ecc71; --warn:#f1c40f; --err:#e74c3c; --chip:#1b2430; --accent:#4aa3ff;
  }
  *{box-sizing:border-box} body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial;background:var(--bg);color:var(--text)}
  header{padding:16px 20px;background:linear-gradient(180deg,#0e131a 0%,#0b0f14 100%);border-bottom:1px solid #1a2330;display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}
  h1{margin:0;font-size:18px;letter-spacing:.4px;font-weight:600}
  .muted{color:var(--muted)} .grid{display:grid;gap:16px;padding:16px;grid-template-columns:repeat(auto-fill,minmax(320px,1fr))}
  .card{background:var(--panel);border:1px solid #1a2330;border-radius:12px;padding:16px}
  .card h2{margin:0 0 12px;font-size:16px;letter-spacing:.3px}
  .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
  .chips{display:flex;gap:8px;flex-wrap:wrap}
  .chip{background:var(--chip);border:1px solid #1f2a38;color:var(--text);border-radius:999px;padding:6px 10px;font-size:12px}
  .ok{color:var(--ok)} .warn{color:var(--warn)} .err{color:var(--err)}
  button,.btn{cursor:pointer;background:#162335;color:var(--text);border:1px solid #233248;padding:8px 12px;border-radius:8px;font-size:13px}
  button:hover,.btn:hover{background:#1b2a40}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{padding:8px;border-bottom:1px solid #1a2330;text-align:left}
  th{color:var(--muted);font-weight:500}
  .mono{font-family:ui-monospace,Menlo,Consolas,monospace}.right{text-align:right}.small{font-size:12px;color:var(--muted)}
  .notice{background:#0f1520;border:1px solid #203049;padding:10px 12px;border-radius:10px;font-size:13px}
</style>
</head>
<body>
<header>
  <div>
    <h1>Crypto Dashboard <span class="small muted mono" id="appVersion"></span></h1>
    <div class="small muted">Symbols: <span id="symList"></span></div>
  </div>
  <div class="row">
    <button onclick="refreshAll()">Refresh</button>
    <a class="btn" href="/health/versions">Versions</a>
    <a class="btn" href="/diag/crypto">Account</a>
  </div>
</header>

<div class="grid">
  <div class="card">
    <h2>Quick Actions</h2>
    <div class="row">
      <button onclick="triggerScan('c1')">Scan C1</button>
      <button onclick="triggerScan('c2')">Scan C2</button>
      <button onclick="triggerScan('c3')">Scan C3</button>
      <button onclick="triggerScan('c4')">Scan C4</button>
    </div>
    <div class="small muted" id="scanResult" style="margin-top:10px;"></div>
    <div class="chips" style="margin-top:10px;">
      <span class="chip">/scan/c1</span><span class="chip">/scan/c2</span><span class="chip">/scan/c3</span><span class="chip">/scan/c4</span>
    </div>
  </div>

  <div class="card" style="grid-column:1 / -1;">
    <h2>Recent Orders</h2>
    <div class="row" style="margin-bottom:8px;">
      <button onclick="loadOrders('all')">All</button>
      <button onclick="loadOrders('open')">Open</button>
      <button onclick="loadOrders('closed')">Closed</button>
    </div>
    <div id="ordersTable">Loading…</div>
  </div>

  <div class="card" style="grid-column:1 / -1;">
    <h2>Positions</h2>
    <div id="positionsTable">Loading…</div>
  </div>
</div>

<script>
async function jfetch(url, opts={}) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error("HTTP "+r.status);
  const ct = r.headers.get("content-type") || "";
  return ct.includes("application/json") ? r.json() : r.text();
}
function esc(s){ return (s==null?"":String(s)).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]); }

async function refreshMeta(){
  try{
    const v = await jfetch('/health/versions');
    document.getElementById('appVersion').textContent = "v"+esc(v.app);
    document.getElementById('symList').textContent = (await jfetch('/health')).symbols.join(', ');
  }catch(e){}
}

async function triggerScan(which){
  try{
    const res = await jfetch(`/scan/${which}?dry=0`, {method:'POST'});
    document.getElementById('scanResult').textContent = JSON.stringify(res);
    loadOrders('all');
  }catch(e){ document.getElementById('scanResult').textContent = 'Scan failed'; }
}

async function loadOrders(status='all'){
  try{
    const rows = await jfetch(`/orders/recent?status=${encodeURIComponent(status)}&limit=200`);
    const arr = Array.isArray(rows)?rows:[];
    if(arr.length===0){ document.getElementById('ordersTable').innerHTML = '<div class="muted">No orders</div>'; return; }
    let html = '<table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Status</th><th class="right">Filled</th></tr></thead><tbody>';
    for(const o of arr){
      html += `<tr>
        <td class="mono small">${esc(o.submitted_at || o.created_at || '')}</td>
        <td class="mono">${esc(o.symbol || '')}</td>
        <td>${esc(o.side || '')}</td>
        <td>${esc(o.qty || o.notional || '')}</td>
        <td>${esc(o.status || '')}</td>
        <td class="right">${esc(o.filled_qty || '0')}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    document.getElementById('ordersTable').innerHTML = html;
  }catch(e){ document.getElementById('ordersTable').innerHTML = '<div class="err">Failed to load orders</div>'; }
}

async function loadPositions(){
  try{
    const rows = await jfetch('/positions');
    const arr = Array.isArray(rows)?rows:[];
    if(arr.length===0){ document.getElementById('positionsTable').innerHTML='<div class="muted">No positions</div>'; return; }
    let html = '<table><thead><tr><th>Symbol</th><th>Side</th><th>Qty</th><th class="right">Market Value</th><th class="right">Unrealized P/L</th></tr></thead><tbody>';
    for(const p of arr){
      html += `<tr>
        <td class="mono">${esc(p.symbol||'')}</td>
        <td>${esc(p.side||'')}</td>
        <td>${esc(p.qty||'')}</td>
        <td class="right mono">$${esc(p.market_value||'0')}</td>
        <td class="right mono">$${esc(p.unrealized_pl||'0')}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    document.getElementById('positionsTable').innerHTML = html;
  }catch(e){ document.getElementById('positionsTable').innerHTML = '<div class="err">Failed to load positions</div>'; }
}

function refreshAll(){ refreshMeta(); loadOrders('all'); loadPositions(); }
window.addEventListener('load', () => { refreshAll(); setInterval(refreshMeta, 30000); });
</script>
</body>
</html>
"""

@app.get("/dashboard")
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html")

@app.get("/")
def index_root():
    return redirect("/dashboard")

# ----------------------------
# Health & Versions
# ----------------------------
@app.get("/health")
def health():
    return _ok({"ok": True, "system": SYSTEM_NAME, "symbols": CRYPTO_SYMBOLS})

@app.get("/health/versions")
def health_versions():
    systems = {
        "c1": {"version": _mod_version("strategies.c1")},
        "c2": {"version": _mod_version("strategies.c2")},
        "c3": {"version": _mod_version("strategies.c3")},
        "c4": {"version": _mod_version("strategies.c4")},
    }
    body = {"app": APP_VERSION, "exchange": CRYPTO_EXCHANGE, "systems": systems}
    headers = {
        "x-c1-version": systems["c1"]["version"],
        "x-c2-version": systems["c2"]["version"],
        "x-c3-version": systems["c3"]["version"],
        "x-c4-version": systems["c4"]["version"],
    }
    return _ok(body, headers)

# ----------------------------
# Diag: Crypto (Alpaca)
# ----------------------------
import requests

def _alpaca_headers():
    return {
        "APCA-API-KEY-ID": API_KEY or "",
        "APCA-API-SECRET-KEY": API_SECRET or "",
        "Content-Type": "application/json",
    }

@app.get("/diag/crypto")
def diag_crypto():
    try:
        r = requests.get(f"{ALPACA_TRADING_BASE}/account", headers=_alpaca_headers(), timeout=20)
        sample = r.json() if r.headers.get("content-type","").startswith("application/json") else {"status_code": r.status_code, "text": r.text}
        return _ok({
            "ok": r.status_code < 300,
            "exchange": CRYPTO_EXCHANGE,
            "api_key_present": bool(API_KEY),
            "trading_base": ALPACA_TRADING_BASE,
            "data_base": ALPACA_DATA_BASE,
            "symbols": CRYPTO_SYMBOLS,
            "account_sample": sample,
            "error": None if r.status_code < 300 else f"HTTP {r.status_code}",
        })
    except Exception as e:
        return _ok({
            "ok": False, "exchange": CRYPTO_EXCHANGE, "api_key_present": bool(API_KEY),
            "trading_base": ALPACA_TRADING_BASE, "data_base": ALPACA_DATA_BASE,
            "symbols": CRYPTO_SYMBOLS, "error": str(e)
        })

# ----------------------------
# Orders & Positions (Alpaca)
# ----------------------------
@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status", "all")
    limit  = int(request.args.get("limit", "50"))
    params = {"status": status, "limit": str(limit), "direction": "desc", "nested": "true", "symbols": None}
    # Restrict to crypto
    params["asset_class"] = "crypto"
    r = requests.get(f"{ALPACA_TRADING_BASE}/orders", headers=_alpaca_headers(), params=params, timeout=20)
    try:
        data = r.json()
    except Exception:
        data = []
    # ensure list
    data = data if isinstance(data, list) else []
    return _ok(data)

@app.get("/positions")
def positions():
    r = requests.get(f"{ALPACA_TRADING_BASE}/positions", headers=_alpaca_headers(), timeout=20)
    try:
        rows = r.json()
    except Exception:
        rows = []
    # Only crypto (a bit defensive)
    out = []
    for p in rows if isinstance(rows, list) else []:
        if str(p.get("asset_class","")).lower() == "crypto" or "/" in str(p.get("symbol","")):
            out.append({
                "symbol": p.get("symbol"),
                "side": "long" if float(p.get("qty",0))>=0 else "short",
                "qty": p.get("qty"),
                "market_value": p.get("market_value"),
                "unrealized_pl": p.get("unrealized_pl"),
                "asset_class": p.get("asset_class"),
            })
    return _ok(out)

# ----------------------------
# Scans: C1–C4
# ----------------------------
def _scan(name: str):
    dry   = _bool(request.args.get("dry"), default=False)
    force = _bool(request.args.get("force"), default=False)
    params = _collect_params_from_request()

    try:
        mod = _strategy_module(name)
    except Exception as e:
        return _err(f"strategy import failed: {e}", 500)

    tag = name
    res = _run_strategy_direct(tag, mod, CRYPTO_SYMBOLS, params, dry=dry)
    if not res.get("ok"):
        return jsonify({"ok": False, "strategy": name, "dry": dry, "force": force, "error": res.get("error")}), 200

    payload = {
        "ok": True,
        "strategy": name,
        "dry": dry,
        "force": force,
        "results": res.get("results", []),
    }
    # Expose strategy module version in header
    ver = getattr(mod, "__version__", "unknown")
    return _ok(payload, {"x-strategy-version": ver})

@app.post("/scan/c1")
def scan_c1(): return _scan("c1")

@app.post("/scan/c2")
def scan_c2(): return _scan("c2")

@app.post("/scan/c3")
def scan_c3(): return _scan("c3")

@app.post("/scan/c4")
def scan_c4(): return _scan("c4")

# ----------------------------
# Signals panel (optional JSON)
# ----------------------------
@app.get("/signals")
def signals():
    out: Dict[str, Any] = {}
    for name in ("c1","c2","c3","c4"):
        try:
            mod = _strategy_module(name)
            getter = getattr(mod, "signals", None)
            out[name] = getter() if callable(getter) else {}
        except Exception:
            out[name] = {}
    return _ok(out)

# ----------------------------
# Inline diag
# ----------------------------
@app.get("/diag/inline")
def diag_inline():
    return _ok({
        "app": APP_VERSION,
        "exchange": CRYPTO_EXCHANGE,
        "symbols": CRYPTO_SYMBOLS,
        "server_time": _now_iso(),
    })

# ----------------------------
# Main
# ----------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
