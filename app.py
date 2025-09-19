#!/usr/bin/env python3
# app.py — Crypto System API
# Version: 1.7.8

import os
import json
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional

from flask import Flask, request, jsonify, Response, redirect

APP_VERSION = "1.7.8"
app = Flask(__name__)

# ----------------------------- utils -----------------------------
def _bool(x: Any, default=False) -> bool:
    if x is None:
        return default
    if isinstance(x, bool):
        return x
    s = str(x).strip().lower()
    return s in ("1", "true", "t", "yes", "y", "on")

def _int(x: Any, default: int) -> int:
    try:
        return int(x)
    except Exception:
        return default

def _now_iso() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()

def _tf_norm(tf: str) -> str:
    if not tf:
        return "5Min"
    s = str(tf).strip()
    m = {
        "1m":"1Min","3m":"3Min","5m":"5Min","15m":"15Min","30m":"30Min",
        "60m":"60Min","1h":"1Hour","1H":"1Hour","1d":"1Day","1D":"1Day",
        "5min":"5Min","15min":"15Min","30min":"30Min","60min":"60Min",
        "1Min":"1Min","3Min":"3Min","5Min":"5Min","15Min":"15Min","30Min":"30Min","60Min":"60Min",
        "1Hour":"1Hour","1Day":"1Day"
    }
    return m.get(s, s)

# ----------------------------- imports -----------------------------
try:
    from services.exchange_exec import ExchangeExec
except Exception:
    ExchangeExec = None  # type: ignore

try:
    from services.market_crypto import MarketCrypto
except Exception:
    MarketCrypto = None  # type: ignore

import requests
import pandas as pd

# strategies c1..c4
_strat_modules: Dict[str, Any] = {}
for _name in ("c1", "c2", "c3", "c4"):
    try:
        _strat_modules[_name] = __import__(f"strategies.{_name}", fromlist=["*"])
    except Exception:
        _strat_modules[_name] = None

# ----------------------------- market/broker -----------------------------
def _make_broker():
    if ExchangeExec is None:
        return None
    return ExchangeExec.from_env()

def _make_market():
    if MarketCrypto is None:
        return None
    try:
        return MarketCrypto()
    except Exception:
        return None

broker = _make_broker()
_underlying_market = _make_market()

# ----------------------------- HTTP helpers -----------------------------
def _alpaca_base() -> str:
    return os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets").rstrip("/")

def _alpaca_headers() -> Dict[str, str]:
    h: Dict[str,str] = {}
    key = os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY_ID")
    sec = os.getenv("APCA_API_SECRET_KEY") or os.getenv("ALPACA_API_SECRET_KEY")
    if key and sec:
        h["APCA-API-KEY-ID"] = key
        h["APCA-API-SECRET-KEY"] = sec
    return h

def _rename_bars_df(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "t":"ts","o":"open","h":"high","l":"low","c":"close","v":"volume",
        "timestamp":"ts","open":"open","high":"high","low":"low","close":"close","volume":"volume"
    }
    df = df.rename(columns=rename_map)
    cols = [c for c in ["ts","open","high","low","close","volume"] if c in df.columns]
    return df[cols] if cols else df

def _http_candles_multi(symbols: List[str], timeframe: str, limit: int
) -> Tuple[Dict[str,pd.DataFrame], List[str], Optional[str], Optional[str]]:
    """Batch fetch; returns map + attempts/url/error (no per-symbol retries here)."""
    attempts: List[str] = []
    last_url: Optional[str] = None
    last_error: Optional[str] = None
    out: Dict[str, pd.DataFrame] = {s: pd.DataFrame() for s in symbols}

    base = _alpaca_base()
    tf = _tf_norm(timeframe)
    url = f"{base}/v1beta3/crypto/us/bars"
    params = {"symbols": ",".join(symbols), "timeframe": tf, "limit": str(limit)}
    try:
        r = requests.get(url, params=params, headers=_alpaca_headers(), timeout=12)
        last_url = r.url
        attempts.append(last_url or url)
        if r.status_code != 200:
            last_error = f"HTTP {r.status_code}: {r.text[:300]}"
            return out, attempts, last_url, last_error
        data = r.json() or {}
        bars_map = data.get("bars", {})
        for s in symbols:
            rows = bars_map.get(s, [])
            if rows:
                out[s] = _rename_bars_df(pd.DataFrame(rows))
        return out, attempts, last_url, None
    except Exception as e:
        last_error = f"{type(e).__name__}: {e}"
        return out, attempts, last_url, last_error

def _http_candles_single(symbol: str, timeframe: str, limit: int
) -> Tuple[pd.DataFrame, str, Optional[str]]:
    """Single-symbol fetch; returns df, url, error."""
    base = _alpaca_base()
    tf = _tf_norm(timeframe)
    url = f"{base}/v1beta3/crypto/us/bars"
    params = {"symbols": symbol, "timeframe": tf, "limit": str(limit)}
    try:
        r = requests.get(url, params=params, headers=_alpaca_headers(), timeout=12)
        final_url = r.url
        if r.status_code != 200:
            return pd.DataFrame(), final_url, f"HTTP {r.status_code}: {r.text[:300]}"
        data = r.json() or {}
        bars_map = data.get("bars", {})
        rows = bars_map.get(symbol, [])
        if rows:
            return _rename_bars_df(pd.DataFrame(rows)), final_url, None
        return pd.DataFrame(), final_url, None
    except Exception as e:
        return pd.DataFrame(), url, f"{type(e).__name__}: {e}"

# ----------------------------- safe market wrapper -----------------------------
class MarketProxy:
    """Facade around MarketCrypto with HTTP fallback (batch + per-symbol)."""
    def __init__(self, inner):
        self.inner = inner
        self.symbols = getattr(inner, "symbols", ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"])
        self.last_bars_url: Optional[str] = None
        self.last_error: Optional[str] = None
        self.last_attempts: List[str] = []

    def _inner_try(self, symbols: List[str], timeframe: str, limit: int):
        attempts: List[str] = []
        url = None
        err = None
        out: Dict[str, pd.DataFrame] = {s: pd.DataFrame() for s in symbols}
        if not hasattr(self.inner, "candles"):
            return out, attempts, url, "inner: no candles()"
        try:
            rv = self.inner.candles(symbols=symbols, timeframe=timeframe, limit=limit, return_debug=True)
            if isinstance(rv, tuple) and len(rv) == 4:
                maybe_map, attempts, url, err = rv
                if isinstance(maybe_map, dict):
                    for s in symbols:
                        v = maybe_map.get(s)
                        if isinstance(v, pd.DataFrame):
                            out[s] = v
                elif isinstance(maybe_map, pd.DataFrame) and symbols:
                    out[symbols[0]] = maybe_map
            elif isinstance(rv, dict):
                for s in symbols:
                    v = rv.get(s)
                    if isinstance(v, pd.DataFrame):
                        out[s] = v
            elif isinstance(rv, pd.DataFrame) and symbols:
                out[symbols[0]] = rv
        except Exception as e:
            err = f"inner: {type(e).__name__}: {e}"
        return out, attempts, url, err

    def candles(self, symbols: List[str], timeframe: str, limit: int, return_debug: bool = False):
        tf = _tf_norm(timeframe)
        m1, a1, u1, e1 = self._inner_try(symbols, tf, limit)
        has_any = any((not df.empty) for df in m1.values())

        attempts = list(a1)
        last_url = u1
        last_error = e1

        # 1) Batch fallback
        if not has_any:
            m2, a2, u2, e2 = _http_candles_multi(symbols, tf, limit)
            for s in symbols:
                if m1.get(s) is None or m1[s].empty:
                    m1[s] = m2.get(s, pd.DataFrame())
            attempts.extend(a2)
            last_url = u2 or last_url
            last_error = e2 or last_error

        # 2) Per-symbol fallback for any that are still empty
        empties = [s for s in symbols if m1.get(s) is None or m1[s].empty]
        for s in empties:
            df, url_s, err_s = _http_candles_single(s, tf, limit)
            if url_s:
                attempts.append(url_s)
                last_url = url_s
            if err_s and not last_error:
                last_error = err_s
            if not df.empty:
                m1[s] = df

        self.last_attempts = attempts
        self.last_bars_url = last_url
        self.last_error = last_error

        if return_debug:
            return m1, attempts, last_url, last_error
        return m1

# Use proxy if underlying exists
market = MarketProxy(_underlying_market) if _underlying_market else None

# ----------------------------- scan params normalizer -----------------------------
def _norm_scan_params(args) -> Dict[str, Any]:
    p: Dict[str, Any] = {}
    tf = args.get("timeframe") or args.get("tf") or args.get("param.timeframe")
    if tf:
        p["timeframe"] = str(tf)
    lim = args.get("limit") or args.get("param.limit")
    if lim:
        p["limit"] = _int(lim, 300)
    syms = args.get("symbols")
    if syms:
        toks = [s.strip() for s in syms.split(",") if s.strip()]
        if toks:
            p["symbols"] = toks
    for k, v in list(args.items()):
        if k.startswith("param.") and k not in ("param.timeframe", "param.limit"):
            p[k[6:]] = v
    return p

# ----------------------------- gate -----------------------------
GATE_ON = _bool(os.getenv("CRYPTO_GATE_ON", "true"), True)
GATE_REASON = os.getenv("CRYPTO_GATE_REASON", "")
def _gate_state() -> Dict[str, Any]:
    clock = {"is_open": True, "next_open": None, "next_close": None, "source": "crypto-24x7"}
    return {"gate_on": GATE_ON, "decision": "open" if GATE_ON else "closed",
            "reason": GATE_REASON or ("24/7 crypto" if GATE_ON else "manually disabled"),
            "clock": clock, "ts": _now_iso()}

# ----------------------------- versions -----------------------------
def _strategy_versions() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for name, mod in _strat_modules.items():
        if mod is None:
            out[name] = {"version": ""}
        else:
            out[name] = {"version": getattr(mod, "STRATEGY_VERSION", "") or ""}
    return out

@app.get("/health")
def health():
    syms = []
    try:
        syms = market.symbols if market and getattr(market, "symbols", None) else ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"]
    except Exception:
        syms = ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"]
    return jsonify({"ok": True, "system": "crypto", "symbols": syms})

@app.get("/health/versions")
def health_versions():
    versions = _strategy_versions()
    resp = {
        "app": APP_VERSION,
        "exchange": getattr(broker, "exchange_name", "alpaca") if broker else "unknown",
        "systems": versions,
    }
    headers = {
        "x-app-version": APP_VERSION,
        "x-c1-version": versions.get("c1",{}).get("version",""),
        "x-c2-version": versions.get("c2",{}).get("version",""),
        "x-c3-version": versions.get("c3",{}).get("version",""),
        "x-c4-version": versions.get("c4",{}).get("version",""),
    }
    return (jsonify(resp), 200, headers)

# ----------------------------- diagnostics -----------------------------
@app.get("/diag/crypto")
def diag_crypto():
    info: Dict[str, Any] = {
        "ok": True,
        "exchange": getattr(broker, "exchange_name", "alpaca") if broker else "unknown",
        "trading_base": getattr(broker, "trading_base", None) if broker else None,
        "data_base_env": os.getenv("ALPACA_DATA_BASE") or "https://data.alpaca.markets",
        "effective_bars_url": getattr(market, "last_bars_url", None) if market else None,
        "last_data_error": getattr(market, "last_error", None) if market else None,
        "api_key_present": bool(os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY_ID")),
        "symbols": getattr(market, "symbols", ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"]),
        "last_attempts": getattr(market, "last_attempts", []),
    }
    acct_sample = None
    acct_err = None
    if broker and hasattr(broker, "account_sample"):
        try:
            acct_sample = broker.account_sample()
        except Exception as e:
            acct_err = str(e)
    info["account_sample"] = acct_sample
    info["account_error"] = acct_err
    return jsonify(info)

@app.get("/diag/candles")
def diag_candles():
    symbols = request.args.get("symbols", "BTC/USD,ETH/USD,SOL/USD,DOGE/USD")
    limit = _int(request.args.get("limit", 3), 3)
    tf = request.args.get("tf") or request.args.get("timeframe") or "5m"
    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    rows_map: Dict[str, int] = {s: 0 for s in syms}
    attempts: List[str] = []
    last_url = None
    last_error = None

    df_map: Dict[str, Any] = {s: pd.DataFrame() for s in syms}
    try:
        if market:
            df_map, attempts, last_url, last_error = market.candles(
                symbols=syms, timeframe=tf, limit=limit, return_debug=True
            )
            for s in syms:
                df = df_map.get(s)
                rows_map[s] = int(getattr(df, "shape", [0])[0]) if df is not None else 0
    except Exception as e:
        last_error = f"{type(e).__name__}: {e}"

    return jsonify({
        "symbols": syms,
        "timeframe": tf,
        "limit": limit,
        "rows": rows_map,
        "last_attempts": attempts,
        "last_url": last_url,
        "last_error": last_error,
    })

@app.get("/diag/gate")
def diag_gate():
    clock = {"is_open": True, "next_open": "", "next_close": "", "source": "crypto-24x7"}
    return jsonify({"gate_on": True, "decision": "open", "reason": "24/7 crypto", "clock": clock, "ts": _now_iso()})

# ----------------------------- scans -----------------------------
def _pwrite(msg: str):
    try:
        print(msg, flush=True)
    except Exception:
        pass

def _log(col: Dict[str, Any]):
    try:
        print(json.dumps(col), flush=True)
    except Exception:
        pass

def _run_strategy_direct(name: str, dry: bool, force: bool, params: Dict[str, Any]) -> Tuple[bool, List[Dict[str, Any]]]:
    mod = _strat_modules.get(name)
    if not mod:
        return False, [{"action": "error", "error": f"strategy {name} unavailable"}]
    if not market or not broker:
        return False, [{"action": "error", "error": "market/broker unavailable"}]

    try:
        symbols = params.pop("symbols", getattr(market, "symbols", ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"]))
        res = mod.run(market, broker, symbols, params, dry=dry, pwrite=_pwrite, log=_log)  # type: ignore
        results = res if isinstance(res, list) else (res or [])
        return True, results
    except TypeError:
        try:
            symbols = params.pop("symbols", getattr(market, "symbols", ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"]))
            res = mod.run(market, broker, symbols, params, dry=dry, pwrite=_pwrite)  # type: ignore
            results = res if isinstance(res, list) else (res or [])
            return True, results
        except Exception as e2:
            return False, [{"action": "error", "error": str(e2)}]
    except Exception as e:
        return False, [{"action": "error", "error": str(e)}]

def _scan_response(name: str, dry: bool, force: bool, params: Dict[str, Any]):
    ok, results = _run_strategy_direct(name, dry, force, params)
    headers = {
        "x-app-version": APP_VERSION,
        "x-strategy-version": getattr(_strat_modules.get(name), "STRATEGY_VERSION", "") if _strat_modules.get(name) else "",
    }
    body = {"strategy": name, "ok": ok, "dry": dry, "force": force, "results": results}
    return (jsonify(body), 200, headers)

@app.post("/scan/<name>")
def scan(name: str):
    n = name.lower()
    if n not in _strat_modules:
        return jsonify({"ok": False, "dry": _bool(request.args.get("dry"), True),
                        "force": False, "strategy": n, "error": "unknown strategy"}), 400
    dry = _bool(request.args.get("dry"), True)
    force = _bool(request.args.get("force"), False)
    p = _norm_scan_params(request.args)
    return _scan_response(n, dry, force, p)

# ----------------------------- orders/positions/signals -----------------------------
@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status", "all")
    limit = _int(request.args.get("limit", 50), 50)
    rows: List[Dict[str, Any]] = []
    try:
        if broker and hasattr(broker, "orders_recent"):
            rows = broker.orders_recent(status=status, limit=limit)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify(rows)

@app.get("/positions")
def positions():
    try:
        if broker and hasattr(broker, "positions"):
            return jsonify(broker.positions())
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
    return jsonify([])

@app.get("/signals")
def signals():
    try:
        if hasattr(market, "recent_signals"):
            return jsonify(getattr(market, "recent_signals"))
    except Exception:
        pass
    return jsonify([])

# ----------------------------- dashboard -----------------------------
DASHBOARD_HTML = """<!doctype html><html lang="en"><head>
<meta charset="utf-8"><title>Crypto Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root { --bg:#0b0f14; --panel:#121821; --text:#e6edf3; --muted:#8aa0b4; --ok:#2ecc71; --warn:#f1c40f; --err:#e74c3c; --chip:#1b2430; }
  * { box-sizing:border-box; } body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, Arial; background:var(--bg); color:var(--text);}
  header { padding:16px 20px; background: linear-gradient(180deg,#0e131a 0%,#0b0f14 100%); border-bottom:1px solid #1a2330; display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap; }
  h1 { margin:0; font-size:18px; letter-spacing:.4px; font-weight:600; } .muted { color: var(--muted); }
  .grid { display:grid; gap:16px; padding:16px; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); }
  .card { background: var(--panel); border:1px solid #1a2330; border-radius:12px; padding:16px; }
  .card h2 { margin:0 0 12px; font-size:16px; letter-spacing:.3px; }
  .row { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .chips { display:flex; gap:8px; flex-wrap:wrap; } .chip { background: var(--chip); border:1px solid #1f2a38; color: var(--text); border-radius:999px; padding:6px 10px; font-size:12px; }
  .ok { color: var(--ok); } .warn { color: var(--warn); } .err { color: var(--err); }
  button, .btn { cursor:pointer; background:#162335; color:var(--text); border:1px solid #233248; padding:8px 12px; border-radius:8px; font-size:13px; }
  button:hover, .btn:hover { background:#1b2a40; } table { width:100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding:8px; border-bottom:1px solid #1a2330; text-align:left; } th { color: var(--muted); font-weight:500; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Courier New", monospace; } .right { text-align: right; } .small { font-size: 12px; color: var(--muted); }
</style>
</head><body>
<header>
  <div><h1>Crypto Dashboard <span class="small muted mono" id="appVersion"></span></h1>
  <div class="small muted">Gate <strong id="gateState">checking…</strong></div></div>
  <div class="row">
    <button onclick="refreshAll()">Refresh</button>
    <a class="btn" href="/health/versions">Versions</a>
    <a class="btn" href="/diag/crypto">Crypto</a>
    <a class="btn" href="/diag/candles?symbols=BTC/USD,ETH/USD,SOL/USD,DOGE/USD&limit=3&tf=5m">Candles</a>
  </div>
</header>
<div class="grid">
  <div class="card"><h2>Gate</h2><div id="gateCard">Loading…</div></div>
  <div class="card"><h2>Quick Scans</h2><div class="row">
    <button onclick="triggerScan('c1')">Scan C1</button>
    <button onclick="triggerScan('c2')">Scan C2</button>
    <button onclick="triggerScan('c3')">Scan C3</button>
    <button onclick="triggerScan('c4')">Scan C4</button>
  </div><div class="small muted" id="scanResult" style="margin-top:10px;"></div></div>
  <div class="card" style="grid-column: 1 / -1;"><h2>Recent Orders</h2><div id="ordersTable">Loading…</div></div>
  <div class="card" style="grid-column: 1 / -1;"><h2>Positions</h2><div id="positionsTable">Loading…</div></div>
</div>
<script>
async function jfetch(u,o={}){const r=await fetch(u,o);if(!r.ok)throw new Error("HTTP "+r.status);const ct=r.headers.get("content-type")||"";return ct.includes("application/json")?r.json():r.text();}
function esc(s){return (s==null?"":String(s)).replace(/[&<>\"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]));}
async function refreshGate(){try{const g=await jfetch('/diag/gate');const v=await jfetch('/health/versions');document.getElementById('appVersion').textContent="v"+esc(v.app);const open=g.decision==='open';document.getElementById('gateState').textContent=open?'OPEN':(g.decision||'closed');document.getElementById('gateState').className=open?'ok':'warn';document.getElementById('gateCard').innerHTML=`<div><strong>Gate:</strong> ${esc(g.gate_on?'on':'off')} — <strong>Decision:</strong> ${esc(g.decision)} — ${esc(g.reason||'')}</div>`;}catch(e){document.getElementById('gateCard').innerHTML='<span class="err">Failed to load gate</span>';}}
async function loadOrders(){try{const rows=await jfetch('/orders/recent?status=all&limit=200');const a=Array.isArray(rows)?rows:[];if(a.length===0){document.getElementById('ordersTable').innerHTML='<div class="muted">No orders</div>';return;}let h='<table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Type</th><th>Status</th><th class="right">Filled</th></tr></thead><tbody>';for(const o of a.slice(0,200)){h+=`<tr><td class="mono small">${esc(o.submitted_at||o.created_at||'')}</td><td class="mono">${esc(o.symbol||'')}</td><td>${esc(o.side||'')}</td><td>${esc(o.qty||'')}</td><td>${esc(o.type||o.order_type||'')}</td><td>${esc(o.status||'')}</td><td class="right">${esc(o.filled_qty||'0')}</td></tr>`;}h+='</tbody></table>';document.getElementById('ordersTable').innerHTML=h;}catch(e){document.getElementById('ordersTable').innerHTML='<div class="err">Failed to load orders</div>';}}
async function loadPositions(){try{const rows=await jfetch('/positions');const a=Array.isArray(rows)?rows:[];if(a.length===0){document.getElementById('positionsTable').innerHTML='<div class="muted">No positions</div>';return;}let h='<table><thead><tr><th>Symbol</th><th>Side</th><th>Qty</th><th class="right">Market Value</th><th class="right">Unrealized P/L</th></tr></thead><tbody>';for(const p of a){h+=`<tr><td class="mono">${esc(p.symbol||p.asset_id||'')}</td><td>${esc(p.side||'')}</td><td>${esc(p.qty||'')}</td><td class="right mono">$${esc(p.market_value||'0')}</td><td class="right mono">$${esc(p.unrealized_pl||'0')}</td></tr>`;}h+='</tbody></table>';document.getElementById('positionsTable').innerHTML=h;}catch(e){document.getElementById('positionsTable').innerHTML='<div class="err">Failed to load positions</div>';}}
async function triggerScan(w){try{const res=await jfetch(`/scan/${w}?dry=1&timeframe=5Min&limit=600`,{method:'POST'});document.getElementById('scanResult').textContent=JSON.stringify(res);}catch(e){document.getElementById('scanResult').textContent='Scan failed';}}
function refreshAll(){refreshGate();loadOrders();loadPositions();}
window.addEventListener('load',()=>{refreshAll();setInterval(refreshGate,30000);});
</script>
</body></html>
"""

@app.get("/dashboard")
def dashboard():
    return Response(DASHBOARD_HTML, mimetype="text/html")

@app.get("/")
def index_root():
    return redirect("/dashboard")

# ----------------------------- run -----------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
