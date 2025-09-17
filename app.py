# app.py
import os
import json
import csv
import time
import threading
import datetime as dt
from collections import defaultdict

from flask import Flask, request, jsonify, make_response, Response, redirect
from dotenv import load_dotenv

# Services & strategies
from services.market_crypto import MarketCrypto
from services.exchange_exec import ExchangeExec
import strategies.c1 as strat_c1
import strategies.c2 as strat_c2

# =============================================================================
# App metadata / versioning
# =============================================================================
__app_version__ = "1.2.0"   # HTML dashboard + orders/positions + inline scheduler & ops

# =============================================================================
# Boot
# =============================================================================
load_dotenv()

app = Flask(__name__)
os.makedirs("logs", exist_ok=True)
os.makedirs("storage", exist_ok=True)

# In-memory ticks for inline scheduler status
_last_ticks = {"c1": None, "c2": None}

# =============================================================================
# Helpers & ENV accessors
# =============================================================================
def env_map():
    keys = [
        "CRYPTO_EXCHANGE", "CRYPTO_TRADING_BASE_URL", "CRYPTO_DATA_BASE_URL",
        "CRYPTO_SYMBOLS", "DAILY_TARGET", "DAILY_STOP", "ORDER_NOTIONAL",
        "C1_TIMEFRAME", "C1_RSI_LEN", "C1_EMA_LEN", "C1_RSI_BUY", "C1_RSI_SELL",
        "C2_TIMEFRAME", "C2_LOOKBACK", "C2_ATR_LEN", "C2_BREAK_K",
        "CRON_INLINE", "C1_EVERY_SEC", "C1_OFFSET_SEC", "C2_EVERY_SEC", "C2_OFFSET_SEC",
        "CRON_DRY",
    ]
    return {k: os.getenv(k) for k in keys}

def get_symbols():
    raw = os.getenv("CRYPTO_SYMBOLS", "BTC/USD,ETH/USD")
    return [s.strip() for s in raw.split(",") if s.strip()]

def _now_utc_ts() -> int:
    return int(time.time())

def _today_bounds_utc():
    # UTC day bounds for consistent rollups
    now = dt.datetime.utcnow()
    start = dt.datetime(year=now.year, month=now.month, day=now.day, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())

def pwrite(symbol, system, side, notional, note, dry, extra=None):
    """
    Append an execution intent/record to the crypto journal.
    Note: This logs actions (signals/requests). Realized P&L should be computed from fills/positions.
    """
    path = "storage/crypto_ledger.csv"
    exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["ts","symbol","system","side","notional_usd","note","dry_run","extra"])
        w.writerow([_now_utc_ts(), symbol, system, side, round(float(notional), 2), note or "", int(dry), json.dumps(extra or {})])

def mk_services():
    market = MarketCrypto()
    broker = ExchangeExec()
    return market, broker

# Simple Alpaca request helper using the broker session/base (avoids modifying services file)
def _alpaca_req(method: str, path: str, **kw):
    _, broker = mk_services()
    url = f"{broker.base}{path}"
    r = broker.session.request(method.upper(), url, timeout=(5, 20), **kw)
    r.raise_for_status()
    # Some Alpaca endpoints can return empty bodies on 204; handle gracefully
    try:
        return r.json()
    except Exception:
        return {"status": r.status_code}

# =============================================================================
# Optional: token gate for /scan/* (set CRON_TOKEN to enable)
# =============================================================================
def _require_cron_token_if_set():
    tok = os.getenv("CRON_TOKEN")
    if not tok:
        return None
    if request.args.get("token") != tok:
        return jsonify({"ok": False, "error": "forbidden"}), 403
    return None

# =============================================================================
# Strategy runners (HTTP + inline)
# =============================================================================
def _run_strategy_http(mod, name: str):
    """
    HTTP entry: wraps strategy, returns JSON + x-strategy-version header.
    """
    dry = request.args.get("dry", "1") == "1"
    force = request.args.get("force", "0") == "1"
    symbols = get_symbols()
    params = {**env_map(), "ORDER_NOTIONAL": os.getenv("ORDER_NOTIONAL", os.getenv("ORDER_SIZE", 25))}
    market, broker = mk_services()

    try:
        results = mod.run(market, broker, symbols, params, dry=dry, pwrite=pwrite)
        payload = {"ok": True, "strategy": name, "dry": dry, "force": force, "results": results}
        status = 200
    except Exception as e:
        payload = {"ok": False, "strategy": name, "dry": dry, "force": force, "error": str(e)}
        status = 502

    resp = make_response(jsonify(payload), status)
    resp.headers["x-strategy-version"] = getattr(mod, "__version__", "0")
    resp.headers["x-app-version"] = __app_version__
    return resp

def _run_strategy_direct(mod, name: str, dry: bool):
    """
    Background runner (inline scheduler) – no HTTP response, logs only.
    """
    global _last_ticks
    _last_ticks[name] = _now_utc_ts()
    symbols = get_symbols()
    params = {**env_map(), "ORDER_NOTIONAL": os.getenv("ORDER_NOTIONAL", os.getenv("ORDER_SIZE", 25))}
    market, broker = mk_services()
    try:
        results = mod.run(market, broker, symbols, params, dry=dry, pwrite=pwrite)
        app.logger.info({"ts": _now_utc_ts(), "name": name, "dry": dry, "ok": True, "n": len(results)})
    except Exception as e:
        app.logger.exception(f"[{name}] inline run failed: {e}")

# =============================================================================
# Inline scheduler
# =============================================================================
def _start_inline_scheduler():
    enabled = os.getenv("CRON_INLINE", "0") == "1"
    if not enabled:
        app.logger.info("Inline scheduler disabled (set CRON_INLINE=1 to enable).")
        return

    # Cadence (seconds)
    c1_every = int(os.getenv("C1_EVERY_SEC", 5 * 60))
    c1_offset = int(os.getenv("C1_OFFSET_SEC", 0))
    c2_every = int(os.getenv("C2_EVERY_SEC", 15 * 60))
    c2_offset = int(os.getenv("C2_OFFSET_SEC", 60))
    dry = os.getenv("CRON_DRY", "0") == "1"

    def runner(every, offset, fn, label):
        def loop():
            if offset > 0:
                time.sleep(offset)
            while True:
                t0 = time.time()
                try:
                    fn()
                except Exception as e:
                    app.logger.exception(f"[{label}] exception: {e}")
                elapsed = time.time() - t0
                sleep_s = max(1, every - int(elapsed))
                time.sleep(sleep_s)

        th = threading.Thread(target=loop, name=f"inline-{label}", daemon=True)
        th.start()
        return th

    app.logger.info({
        "msg": "Starting inline scheduler",
        "C1_EVERY_SEC": c1_every, "C1_OFFSET_SEC": c1_offset,
        "C2_EVERY_SEC": c2_every, "C2_OFFSET_SEC": c2_offset,
        "CRON_DRY": int(dry),
    })

    runner(c1_every, c1_offset, lambda: _run_strategy_direct(strat_c1, "c1", dry), "c1")
    runner(c2_every, c2_offset, lambda: _run_strategy_direct(strat_c2, "c2", dry), "c2")

# Start scheduler at boot
_start_inline_scheduler()

# =============================================================================
# Health / Diag
# =============================================================================
@app.get("/health")
def health():
    return jsonify({"ok": True, "system": "crypto", "symbols": get_symbols()})

@app.get("/health/versions")
def health_versions():
    data = {
        "app": __app_version__,
        "exchange": os.getenv("CRYPTO_EXCHANGE", "alpaca"),
        "systems": {
            "c1": {"version": strat_c1.__version__},
            "c2": {"version": strat_c2.__version__},
        },
    }
    resp = jsonify(data)
    resp.headers["x-app-version"] = __app_version__
    resp.headers["x-c1-version"] = strat_c1.__version__
    resp.headers["x-c2-version"] = strat_c2.__version__
    return resp

@app.get("/diag/crypto")
def diag_crypto():
    acct = None
    ok = True
    err = None
    try:
        _, broker = mk_services()
        acct = broker.get_account()
    except Exception as e:
        ok = False
        err = str(e)
    payload = {
        "ok": ok,
        "exchange": os.getenv("CRYPTO_EXCHANGE"),
        "trading_base": os.getenv("CRYPTO_TRADING_BASE_URL"),
        "data_base": os.getenv("CRYPTO_DATA_BASE_URL"),
        "api_key_present": bool(os.getenv("CRYPTO_API_KEY")),
        "symbols": get_symbols(),
        "account_sample": acct,
        "error": err,
    }
    resp = jsonify(payload)
    resp.headers["x-app-version"] = __app_version__
    return resp

@app.get("/diag/inline")
def diag_inline():
    return jsonify({
        "enabled": os.getenv("CRON_INLINE", "0") == "1",
        "c1_every_sec": int(os.getenv("C1_EVERY_SEC", 300)),
        "c1_offset_sec": int(os.getenv("C1_OFFSET_SEC", 0)),
        "c2_every_sec": int(os.getenv("C2_EVERY_SEC", 900)),
        "c2_offset_sec": int(os.getenv("C2_OFFSET_SEC", 60)),
        "last_ticks": _last_ticks,
        "dry": os.getenv("CRON_DRY", "0") == "1",
    })

# 24/7 market gate/clock for parity with equities UI
@app.get("/diag/gate")
def diag_gate():
    return jsonify({
        "gate_on": True,
        "decision": "open",  # crypto 24/7
        "note": "Crypto trades 24/7; gate forced open.",
    })

@app.get("/diag/clock")
def diag_clock():
    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
    return jsonify({
        "is_open": True,
        "now_utc": now,
        "next_open": None,
        "next_close": None
    })

# =============================================================================
# Ops: P&L & limits
# =============================================================================
def _read_ledger_rows():
    path = "storage/crypto_ledger.csv"
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                row["ts"] = int(row["ts"])
                row["notional_usd"] = float(row["notional_usd"])
                row["dry_run"] = bool(int(row["dry_run"]))
            except Exception:
                pass
            rows.append(row)
    return rows

def _summarize_today_activity():
    start_ts, end_ts = _today_bounds_utc()
    by_sys = defaultdict(lambda: {"count": 0, "buy_notional": 0.0, "flat_count": 0, "other_count": 0})
    total = {"count": 0, "buy_notional": 0.0, "flat_count": 0, "other_count": 0}
    rows = _read_ledger_rows()
    for row in rows:
        if not (start_ts <= row["ts"] < end_ts):
            continue
        if row.get("dry_run"):
            continue
        sysname = row.get("system") or "NA"
        side = (row.get("side") or "").upper()
        by_sys[sysname]["count"] += 1
        total["count"] += 1
        if side == "BUY":
            by_sys[sysname]["buy_notional"] += row.get("notional_usd", 0.0)
            total["buy_notional"] += row.get("notional_usd", 0.0)
        elif side == "FLAT":
            by_sys[sysname]["flat_count"] += 1
            total["flat_count"] += 1
        else:
            by_sys[sysname]["other_count"] += 1
            total["other_count"] += 1
    return {"by_system": by_sys, "totals": total}

@app.get("/pnl/daily")
def pnl_daily():
    """
    Returns:
      - today's journal activity summary (counts/notional)
      - live account snapshot
      - current open positions (from Alpaca)
    """
    summary = _summarize_today_activity()
    acct = None
    positions = None
    ok = True
    err = None
    try:
        _, broker = mk_services()
        acct = broker.get_account()
        positions = broker.get_positions()
    except Exception as e:
        ok = False
        err = str(e)

    return jsonify({
        "ok": ok,
        "error": err,
        "app_version": __app_version__,
        "today_activity": summary,
        "account": acct,
        "positions": positions,
    })

@app.get("/health/limits")
def health_limits():
    """
    Shows today's activity vs target/stop rails.
    Uses journal count/notional as activity proxy and account equity for context.
    """
    daily_target = float(os.getenv("DAILY_TARGET", "0") or 0)
    daily_stop = float(os.getenv("DAILY_STOP", "0") or 0)
    summary = _summarize_today_activity()
    acct = None
    ok = True
    err = None
    try:
        _, broker = mk_services()
        acct = broker.get_account()
    except Exception as e:
        ok = False
        err = str(e)

    payload = {
        "ok": ok,
        "error": err,
        "app_version": __app_version__,
        "rails": {"daily_target": daily_target, "daily_stop": daily_stop},
        "today": summary,
        "account_equity": acct.get("equity") if isinstance(acct, dict) else None,
        "note": "This endpoint compares journal activity to rails; realized P&L requires fills."
    }
    return jsonify(payload)

# =============================================================================
# Orders / Positions (for dashboard)
# =============================================================================
@app.get("/positions")
def positions():
    try:
        _, broker = mk_services()
        data = broker.get_positions()
        # Return array (some SDKs return dict); enforce list for UI
        if isinstance(data, dict):
            data = data.get("positions") or []
        return jsonify(data)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502

@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status", "all")
    limit = int(request.args.get("limit", "200"))
    params = {"limit": str(limit)}
    if status and status != "all":
        params["status"] = status
    try:
        data = _alpaca_req("GET", "/orders", params=params)
        # Ensure list
        if isinstance(data, dict) and "orders" in data:
            data = data["orders"]
        return jsonify(data)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 502

# =============================================================================
# Scan routes (token-gated if CRON_TOKEN set)
# =============================================================================
@app.post("/scan/c1")
def scan_c1():
    guard = _require_cron_token_if_set()
    if guard:
        return guard
    return _run_strategy_http(strat_c1, "c1")

@app.post("/scan/c2")
def scan_c2():
    guard = _require_cron_token_if_set()
    if guard:
        return guard
    return _run_strategy_http(strat_c2, "c2")

# =============================================================================
# Dashboard (inline HTML) — Crypto
# =============================================================================
DASHBOARD_HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Crypto Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root {
    --bg: #0b0f14;
    --panel: #121821;
    --text: #e6edf3;
    --muted: #8aa0b4;
    --ok: #2ecc71;
    --warn: #f1c40f;
    --err: #e74c3c;
    --accent: #4aa3ff;
    --chip: #1b2430;
  }
  * { box-sizing: border-box; }
  body { margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", "Apple Color Emoji", "Segoe UI Emoji"; background: var(--bg); color: var(--text); }
  header { padding: 16px 20px; background: linear-gradient(180deg, #0e131a 0%, #0b0f14 100%); border-bottom: 1px solid #1a2330; display: flex; align-items: center; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
  h1 { margin: 0; font-size: 18px; letter-spacing: .4px; font-weight: 600; }
  .muted { color: var(--muted); }
  .grid { display: grid; gap: 16px; padding: 16px; grid-template-columns: repeat(auto-fill, minmax(320px, 1fr)); }
  .card { background: var(--panel); border: 1px solid #1a2330; border-radius: 12px; padding: 16px; }
  .card h2 { margin: 0 0 12px; font-size: 16px; letter-spacing: .3px; }
  .row { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
  .chips { display: flex; gap: 8px; flex-wrap: wrap; }
  .chip { background: var(--chip); border: 1px solid #1f2a38; color: var(--text); border-radius: 999px; padding: 6px 10px; font-size: 12px; }
  .ok { color: var(--ok); }
  .warn { color: var(--warn); }
  .err { color: var(--err); }
  button, .btn { cursor: pointer; background: #162335; color: var(--text); border: 1px solid #233248; padding: 8px 12px; border-radius: 8px; font-size: 13px; }
  button:hover, .btn:hover { background: #1b2a40; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 8px; border-bottom: 1px solid #1a2330; text-align: left; }
  th { color: var(--muted); font-weight: 500; }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
  .right { text-align: right; }
  .small { font-size: 12px; color: var(--muted); }
  .notice { background: #0f1520; border: 1px solid #203049; padding: 10px 12px; border-radius: 10px; font-size: 13px; }
</style>
</head>
<body>
<header>
  <div>
    <h1>Crypto Dashboard <span class="small muted mono" id="appVersion"></span></h1>
    <div class="small muted">Live execution is <strong id="gateState">checking…</strong></div>
  </div>
  <div class="row">
    <button onclick="refreshAll()">Refresh</button>
    <a class="btn" href="/health/versions">Versions</a>
    <a class="btn" href="/diag/crypto">Account</a>
    <a class="btn" href="/pnl/daily">P&L</a>
  </div>
</header>

<div class="grid">
  <div class="card">
    <h2>Market Gate</h2>
    <div id="gateCard" class="notice">Loading…</div>
    <div class="chips" style="margin-top:10px;">
      <span class="chip">/diag/gate</span>
      <span class="chip">/diag/inline</span>
    </div>
  </div>

  <div class="card">
    <h2>Quick Actions</h2>
    <div class="row">
      <button onclick="triggerScan('C1')">Scan C1</button>
      <button onclick="triggerScan('C2')">Scan C2</button>
    </div>
    <div class="small muted" id="scanResult" style="margin-top:10px;"></div>
  </div>

  <div class="card" style="grid-column: 1 / -1;">
    <h2>Recent Orders</h2>
    <div class="row" style="margin-bottom:8px;">
      <button onclick="loadOrders('all')">All</button>
      <button onclick="loadOrders('open')">Open</button>
      <button onclick="loadOrders('closed')">Closed</button>
    </div>
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

function esc(s){ return (s==null?"":String(s)).replace(/[&<>"']/g, m => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }

async function refreshGate() {
  try {
    const gate = await jfetch('/diag/gate');
    const v = await jfetch('/health/versions');
    document.getElementById('appVersion').textContent = "v" + esc(v.app);
    const open = (gate.decision || '').toLowerCase() === 'open';
    document.getElementById('gateState').textContent = open ? 'OPEN' : (gate.decision || 'closed');
    document.getElementById('gateState').className = open ? 'ok' : 'warn';

    const inline = await jfetch('/diag/inline');
    const lines = [
      `<div><strong>Gate:</strong> ${esc(gate.gate_on ? 'on' : 'off')} — <strong>Decision:</strong> ${esc(gate.decision)}</div>`,
      `<div><strong>Inline:</strong> ${inline.enabled ? 'enabled' : 'disabled'} · dry=${inline.dry ? '1' : '0'}</div>`,
      `<div class="small muted mono">C1 every ${esc(inline.c1_every_sec)}s (last: ${esc(inline.last_ticks.c1)}), C2 every ${esc(inline.c2_every_sec)}s (last: ${esc(inline.last_ticks.c2)})</div>`
    ];
    document.getElementById('gateCard').innerHTML = lines.join('');
  } catch (e) {
    document.getElementById('gateCard').innerHTML = `<span class="err">Failed to load gate</span>`;
  }
}

async function loadOrders(status='all') {
  try {
    const rows = await jfetch(`/orders/recent?status=${encodeURIComponent(status)}&limit=200`);
    const arr = Array.isArray(rows) ? rows : [];
    if (arr.length === 0) {
      document.getElementById('ordersTable').innerHTML = '<div class="muted">No orders</div>';
      return;
    }
    let html = '<table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Type</th><th>Status</th><th class="right">Filled</th></tr></thead><tbody>';
    for (const o of arr.slice(0,200)) {
      html += `<tr>
        <td class="mono small">${esc(o.submitted_at || o.created_at || '')}</td>
        <td class="mono">${esc(o.symbol || '')}</td>
        <td>${esc(o.side || '')}</td>
        <td>${esc(o.qty || o.notional || '')}</td>
        <td>${esc(o.type || '')}</td>
        <td>${esc(o.status || '')}</td>
        <td class="right">${esc(o.filled_qty || '0')}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    document.getElementById('ordersTable').innerHTML = html;
  } catch (e) {
    document.getElementById('ordersTable').innerHTML = `<div class="err">Failed to load orders</div>`;
  }
}

async function loadPositions() {
  try {
    const rows = await jfetch('/positions');
    const arr = Array.isArray(rows) ? rows : [];
    if (arr.length === 0) {
      document.getElementById('positionsTable').innerHTML = '<div class="muted">No positions</div>';
      return;
    }
    let html = '<table><thead><tr><th>Symbol</th><th>Side</th><th>Qty</th><th class="right">Market Value</th><th class="right">Unrealized P/L</th></tr></thead><tbody>';
    for (const p of arr) {
      html += `<tr>
        <td class="mono">${esc(p.symbol || '')}</td>
        <td>${esc(p.side || '')}</td>
        <td>${esc(p.qty || p.quantity || '')}</td>
        <td class="right mono">$${esc(p.market_value || '0')}</td>
        <td class="right mono">$${esc(p.unrealized_pl || p.unrealized_plpc || '0')}</td>
      </tr>`;
    }
    html += '</tbody></table>';
    document.getElementById('positionsTable').innerHTML = html;
  } catch (e) {
    document.getElementById('positionsTable').innerHTML = `<div class="err">Failed to load positions</div>`;
  }
}

async function triggerScan(which) {
  try {
    const url = which === 'C1' ? '/scan/c1?dry=0' : '/scan/c2?dry=0';
    const res = await jfetch(url, { method: 'POST' });
    document.getElementById('scanResult').textContent = JSON.stringify(res);
    loadOrders('all');
  } catch (e) {
    document.getElementById('scanResult').textContent = 'Scan failed';
  }
}

function refreshAll() {
  refreshGate();
  loadOrders('all');
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
    # Redirect root to dashboard for convenience
    return redirect("/dashboard", code=302)

# =============================================================================
# Entrypoint
# =============================================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
