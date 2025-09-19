#!/usr/bin/env python3
# app.py — Crypto System API
# Version: 1.7.2

import os
import json
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional

from flask import Flask, request, jsonify, Response, redirect

APP_VERSION = "1.7.2"
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

# ----------------------------- imports -----------------------------
try:
    from services.exchange_exec import ExchangeExec
except Exception:
    ExchangeExec = None  # type: ignore

try:
    from services.market_crypto import MarketCrypto
except Exception:
    MarketCrypto = None  # type: ignore

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
    return MarketCrypto()

broker = _make_broker()
_underlying_market = _make_market()

# ----------------------------- safe market wrapper -----------------------------
class MarketProxy:
    """
    Defensive facade around your MarketCrypto to normalize `candles(...)`:
    - Always supports multi-symbol by looping per symbol.
    - Normalizes return_debug outputs into a dict + debug bundle.
    """
    def __init__(self, inner):
        self.inner = inner
        # bubble through common attrs if present
        self.symbols = getattr(inner, "symbols", ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"])
        self.last_bars_url = None
        self.last_error = None

    def _one(
        self, symbol: str, timeframe: str, limit: int, return_debug: bool
    ):
        # Try inner.candles for a single symbol list [symbol]
        if not hasattr(self.inner, "candles"):
            raise RuntimeError("market.candles not available")
        try:
            if return_debug:
                # could be (df_map, attempts, last_url, last_error) OR
                # df_map only OR (df, attempts, last_url, last_error)
                rv = self.inner.candles(symbols=[symbol], timeframe=timeframe, limit=limit, return_debug=True)
            else:
                rv = self.inner.candles(symbols=[symbol], timeframe=timeframe, limit=limit)
        except TypeError:
            # fallback without keywords
            if return_debug:
                rv = self.inner.candles([symbol], timeframe, limit, True)
            else:
                rv = self.inner.candles([symbol], timeframe, limit)

        # Normalize per-symbol output
        df_map: Dict[str, Any] = {}
        attempts: List[str] = []
        last_url: Optional[str] = None
        last_error: Optional[str] = None

        # Case A: dict mapping
        if isinstance(rv, dict):
            df_map = rv
        # Case B: tuple length 4: (df_map or df, attempts, last_url, last_error)
        elif isinstance(rv, tuple) and len(rv) == 4:
            maybe_map, attempts, last_url, last_error = rv
            if isinstance(maybe_map, dict):
                df_map = maybe_map
            else:
                # assume it's a single DataFrame
                df_map = {symbol: maybe_map}
        # Case C: single DF
        else:
            df_map = {symbol: rv}

        # Track last values for /diag/crypto
        self.last_bars_url = last_url or self.last_bars_url
        self.last_error = last_error

        if return_debug:
            return df_map, attempts, last_url, last_error
        return df_map

    def candles(
        self,
        symbols: List[str],
        timeframe: str,
        limit: int,
        return_debug: bool = False
    ):
        all_map: Dict[str, Any] = {}
        all_attempts: List[str] = []
        last_url: Optional[str] = None
        last_error: Optional[str] = None

        for s in symbols:
            if return_debug:
                m, att, url, err = self._one(s, timeframe, limit, True)
                all_map.update(m or {})
                if isinstance(att, list):
                    all_attempts.extend(att)
                if url:
                    last_url = url
                if err:
                    last_error = err
            else:
                m = self._one(s, timeframe, limit, False)
                all_map.update(m or {})

        if return_debug:
            return all_map, all_attempts, last_url, last_error
        return all_map

# Use proxy everywhere strategies/UI need market access
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
    return {
        "gate_on": GATE_ON,
        "decision": "open" if GATE_ON else "closed",
        "reason": GATE_REASON or ("24/7 crypto" if GATE_ON else "manually disabled"),
        "clock": clock,
        "ts": _now_iso(),
    }

# ----------------------------- versions -----------------------------
def _strategy_versions() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    for name, mod in _strat_modules.items():
        if mod is None:
            out[name] = {"version": ""}
        else:
            ver = getattr(mod, "STRATEGY_VERSION", "")
            out[name] = {"version": ver or ""}
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
    resp = {
        "app": APP_VERSION,
        "exchange": getattr(broker, "exchange_name", "alpaca") if broker else "unknown",
        "systems": _strategy_versions(),
    }
    headers = {
        "x-app-version": APP_VERSION,
        "x-c1-version": _strategy_versions().get("c1",{}).get("version",""),
        "x-c2-version": _strategy_versions().get("c2",{}).get("version",""),
        "x-c3-version": _strategy_versions().get("c3",{}).get("version",""),
        "x-c4-version": _strategy_versions().get("c4",{}).get("version",""),
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
        "api_key_present": _bool(os.getenv("APCA_API_KEY_ID") or os.getenv("ALPACA_API_KEY_ID") or "", False),
        "symbols": getattr(market, "symbols", ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"]),
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
    last_attempts: List[str] = []
    last_url = None
    last_error = None
    rows_map: Dict[str, int] = {s: 0 for s in syms}

    if not market:
        return jsonify({
            "symbols": syms, "timeframe": tf, "limit": limit,
            "rows": rows_map, "last_attempts": last_attempts,
            "last_url": last_url, "last_error": "no market"
        })

    try:
        df_map, last_attempts, last_url, last_error = market.candles(
            symbols=syms, timeframe=tf, limit=limit, return_debug=True
        )
        for s in syms:
            df = df_map.get(s)
            try:
                rows_map[s] = int(getattr(df, "shape", [0])[0]) if df is not None else 0
            except Exception:
                rows_map[s] = 0
    except Exception as e:
        last_error = f"{type(e).__name__}: {e}"

    return jsonify({
        "symbols": syms,
        "timeframe": tf,
        "limit": limit,
        "rows": rows_map,
        "last_attempts": last_attempts,
        "last_url": last_url,
        "last_error": last_error,
    })

@app.get("/diag/gate")
def diag_gate():
    return jsonify(_gate_state())

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

    # Pass the proxy market that guarantees multi-symbol candles
    mkt = market
    try:
        symbols = params.pop("symbols", getattr(mkt, "symbols", ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"]))
        res = mod.run(mkt, broker, symbols, params, dry=dry, pwrite=_pwrite, log=_log)  # type: ignore
        results = res if isinstance(res, list) else (res or [])
        return True, results
    except TypeError:
        try:
            symbols = params.pop("symbols", getattr(mkt, "symbols", ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD"]))
            res = mod.run(mkt, broker, symbols, params, dry=dry, pwrite=_pwrite)  # type: ignore
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
DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Crypto Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root { --bg:#0b0f14; --panel:#121821; --text:#e6edf3; --muted:#8aa0b4; --ok:#2ecc71; --warn:#f1c40f; --err:#e74c3c; --chip:#1b2430; }
  * { box-sizing:border-box; }
  body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial; background:var(--bg); color:var(--text);}
  header { padding:16px 20px; background: linear-gradient(180deg,#0e131a 0%,#0b0f14 100%); border-bottom:1px solid #1a2330; display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap; }
  h1 { margin:0; font-size:18px; letter-spacing:.4px; font-weight:600; }
  .muted { color: var(--muted); }
  .grid { display:grid; gap:16px; padding:16px; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); }
  .card { background: var(--panel); border:1px solid #1a2330; border-radius:12px; padding:16px; }
  .card h2 { margin:0 0 12px; font-size:16px; letter-spacing:.3px; }
  .row { display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
  .chips { display:flex; gap:8px; flex-wrap:wrap; }
  .chip { background: var(--chip); border:1px solid #1f2a38; color: var(--text); border-radius:999px; padding:6px 10px; font-size:12px; }
  .ok { color: var(--ok); } .warn { color: var(--warn); } .err { color: var(--err); }
  button, .btn { cursor:pointer; background:#162335; color:var(--text); border:1px solid #233248; padding:8px 12px; border-radius:8px; font-size:13px; }
  button:hover, .btn:hover { background:#1b2a40; }
  table { width:100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding:8px; border-bottom:1px solid #1a2330; text-align:left; }
  th { color: var(--muted); font-weight:500; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Courier New", monospace; }
  .right { text-align: right; }
  .small { font-size: 12px; color: var(--muted); }
  .notice { background:#0f1520; border:1px solid #203049; padding:10px 12px; border-radius:10px; font-size:13px; }
</style>
</head>
<body>
<header>
  <div>
    <h1>Crypto Dashboard <span class="small muted mono" id="appVersion"></span></h1>
    <div class="small muted">Gate <strong id="gateState">checking…</strong></div>
  </div>
  <div class="row">
    <button onclick="refreshAll()">Refresh</button>
    <a class="btn" href="/health/versions">Versions</a>
    <a class="btn" href="/diag/crypto">Crypto</a>
    <a class="btn" href="/diag/candles?symbols=BTC/USD,ETH/USD,SOL/USD,DOGE/USD&limit=3&tf=5m">Candles</a>
  </div>
</header>

<div class="grid">
  <div class="card">
    <h2>Gate</h2>
    <div id="gateCard" class="notice">Loading…</div>
    <div class="chips" style="margin-top:10px;">
      <span class="chip">/diag/gate</span>
      <span class="chip">24/7 crypto</span>
    </div>
  </div>

  <div class="card">
    <h2>Quick Scans</h2>
    <div class="row">
      <button onclick="triggerScan('c1')">Scan C1</button>
      <button onclick="triggerScan('c2')">Scan C2</button>
      <button onclick="triggerScan('c3')">Scan C3</button>
      <button onclick="triggerScan('c4')">Scan C4</button>
    </div>
    <div class="small muted" id="scanResult" style="margin-top:10px;"></div>
  </div>

  <div class="card" style="grid-column: 1 / -1;">
    <h2>Recent Orders</h2>
    <div id="ordersTable">Loading…</div>
  </div>

  <div class="card" style="grid-column: 1 / -1;">
    <h2>Positions</h2>
    <div id="positionsTable">Loading…</div>
  </div>
</div>

<script>
async function jfetch(url, opts={}) {
  const r = await fetch(url, opts);
  if (!r.ok) throw new Error("HTTP "+r.status);
  const ct = r.headers.get("content-type") || "";
  if (ct.includes("application/json")) return r.json();
  return r.text();
}
function esc(s){ return (s==null?"":String(s)).replace(/[&<>\"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]); }

async function refreshGate() {
  try {
    const gate = await jfetch('/diag/gate');
    const v = await jfetch('/health/versions');
    document.getElementById('appVersion').textContent = "v" + esc(v.app);
    const open = gate.decision === 'open';
    document.getElementById('gateState').textContent = open ? 'OPEN' : (gate.decision || 'closed');
    document.getElementById('gateState').className = open ? 'ok' : 'warn';
    const clk = gate.clock || {};
    const lines = [
      `<div><strong>Gate:</strong> ${esc(gate.gate_on ? 'on' : 'off')} — <strong>Decision:</strong> ${esc(gate.decision)} — ${esc(gate.reason||'')}</div>`,
      `<div><strong>Clock:</strong> is_open=${esc(clk.is_open)} · next_open=${esc(clk.next_open)} · next_close=${esc(clk.next_close)}</div>`
    ];
    document.getElementById('gateCard').innerHTML = lines.join('');
  } catch (e) {
    document.getElementById('gateCard').innerHTML = `<span class="err">Failed to load gate</span>`;
  }
}

async function loadOrders() {
  try {
    const rows = await jfetch('/orders/recent?status=all&limit=200');
    const arr = Array.isArray(rows) ? rows : [];
    if (arr.length === 0) { document.getElementById('ordersTable').innerHTML = '<div class="muted">No orders</div>'; return; }
    let html = '<table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Type</th><th>Status</th><th class="right">Filled</th></tr></thead><tbody>';
    for (const o of arr.slice(0,200)) {
      html += `<tr>
        <td class="mono small">${esc(o.submitted_at || o.created_at || '')}</td>
        <td class="mono">${esc(o.symbol || '')}</td>
        <td>${esc(o.side || '')}</td>
        <td>${esc(o.qty || '')}</td>
        <td>${esc(o.type || o.order_type || '')}</td>
        <td>${esc(o.status || '')}</td>
        <td class="right">${esc(o.filled_qty || '0')}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    document.getElementById('ordersTable').innerHTML = html;
  } catch(e) {
    document.getElementById('ordersTable').innerHTML = `<div class="err">Failed to load orders</div>`;
  }
}

async function loadPositions() {
  try {
    const rows = await jfetch('/positions');
    const arr = Array.isArray(rows) ? rows : [];
    if (arr.length === 0) { document.getElementById('positionsTable').innerHTML = '<div class="muted">No positions</div>'; return; }
    let html = '<table><thead><tr><th>Symbol</th><th>Side</th><th>Qty</th><th class="right">Market Value</th><th class="right">Unrealized P/L</th></tr></thead><tbody>';
    for (const p of arr) {
      html += `<tr>
        <td class="mono">${esc(p.symbol || p.asset_id || '')}</td>
        <td>${esc(p.side || '')}</td>
        <td>${esc(p.qty || '')}</td>
        <td class="right mono">$${esc(p.market_value || '0')}</td>
        <td class="right mono">$${esc(p.unrealized_pl || '0')}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    document.getElementById('positionsTable').innerHTML = html;
  } catch(e) {
    document.getElementById('positionsTable').innerHTML = `<div class="err">Failed to load positions</div>`;
  }
}

async function triggerScan(which) {
  try {
    const url = `/scan/${which}?dry=1&timeframe=5Min&limit=600`;
    const res = await jfetch(url, { method: 'POST' });
    document.getElementById('scanResult').textContent = JSON.stringify(res);
  } catch(e) {
    document.getElementById('scanResult').textContent = 'Scan failed';
  }
}

function refreshAll() {
  refreshGate();
  loadOrders();
  loadPositions();
}
window.addEventListener('load', () => {
  refreshAll();
  setInterval(refreshGate, 30000);
});
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

# ----------------------------- run -----------------------------
if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
