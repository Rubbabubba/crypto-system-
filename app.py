# app.py
# Version: 1.7.1
from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from flask import Flask, jsonify, request, redirect, Response

# --- Strategy imports (ensure these files exist) ---
# Your existing strategies:
#   strategies/c1.py, strategies/c2.py, strategies/c3.py, strategies/c4.py
# New strategies we added:
#   strategies/c5.py, strategies/c6.py
try:
    import strategies.c1 as c1
except Exception:
    c1 = None
try:
    import strategies.c2 as c2
except Exception:
    c2 = None
try:
    import strategies.c3 as c3
except Exception:
    c3 = None
try:
    import strategies.c4 as c4
except Exception:
    c4 = None
try:
    import strategies.c5 as c5
except Exception:
    c5 = None
try:
    import strategies.c6 as c6
except Exception:
    c6 = None

# --- Services (market + broker) ---
# Expecting:
#   services/market_crypto.py -> class MarketCrypto with a .make() or similar factory
#   services/exchange_exec.py -> class ExchangeExec with .from_env()
def _make_market():
    try:
        from services.market_crypto import MarketCrypto
        # Prefer a modern factory if you have it; fall back to a simple constructor.
        if hasattr(MarketCrypto, "make"):
            return MarketCrypto.make()
        if hasattr(MarketCrypto, "from_env"):
            return MarketCrypto.from_env()
        return MarketCrypto()
    except Exception as e:
        print(f"[boot] Market init failed: {e}")
        return None

def _make_broker():
    try:
        from services.exchange_exec import ExchangeExec
        if hasattr(ExchangeExec, "from_env"):
            return ExchangeExec.from_env()
        return ExchangeExec()
    except Exception as e:
        print(f"[boot] Broker init failed: {e}")
        return None

app = Flask(__name__)
market = _make_market()
broker = _make_broker()

APP_VERSION = "1.7.1"
DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD"]

# ---------- Helpers ----------
def _ok(payload: Dict[str, Any], code: int = 200) -> Tuple[Response, int]:
    return jsonify(payload), code

def _parse_symbols_arg(raw: str | None) -> List[str]:
    if not raw:
        return list(DEFAULT_SYMBOLS)
    # allow comma or space separated; trim items
    parts = [p.strip() for p in raw.replace(" ", ",").split(",") if p.strip()]
    return parts or list(DEFAULT_SYMBOLS)

def _parse_params(args) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in args.items():
        if k.lower() in ("dry", "force", "symbols"):
            continue
        # attempt numeric cast
        try:
            if v.lower() in ("true", "false"):
                out[k] = (v.lower() == "true")
            elif "." in v:
                out[k] = float(v)
            else:
                out[k] = int(v)
        except Exception:
            out[k] = v
    return out

def _parse_bool(s: str | None, default: bool) -> bool:
    if s is None:
        return default
    return str(s).lower() in ("1", "true", "yes", "y", "on")

def _run_strategy_direct(mod, strat_name: str):
    """Uniform runner to call strategy.run(...) with parsed args and safe error handling."""
    symbols = _parse_symbols_arg(request.args.get("symbols"))
    params = _parse_params(request.args)
    dry = _parse_bool(request.args.get("dry"), True)

    def log(*a, **kw):
        print(*a, **kw)

    if mod is None:
        return _ok({"ok": False, "strategy": strat_name, "error": "strategy_module_missing"})

    try:
        result = mod.run(market, broker, symbols, params, dry, log, print)
        # Flatten a bit to keep old consumers happy
        payload = {"ok": True, "strategy": strat_name}
        if isinstance(result, dict):
            payload.update(result)
        return _ok(payload)
    except Exception as e:
        return _ok({"ok": False, "strategy": strat_name, "error": str(e)})

def _module_version(mod) -> str:
    # Try a few sources to produce a friendly version string.
    for attr in ("__version__", "VERSION", "version"):
        v = getattr(mod, attr, None) if mod else None
        if isinstance(v, str) and v.strip():
            return v
    # Look in top-level __doc__ for a version-like token
    doc = getattr(mod, "__doc__", "") if mod else ""
    if isinstance(doc, str):
        for token in doc.split():
            if token.lower().startswith("version"):
                return token
    # Fallback
    return "1.x"

# ---------- Basic routes ----------
@app.get("/")
def root_redirect():
    return redirect("/dashboard", code=302)

@app.get("/dashboard")
def dashboard_view():
    """Return a full HTML dashboard with quick controls for scans and live orders."""
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Crypto System Dashboard · v{APP_VERSION}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {{
      --bg: #0e1117;
      --card: #161b22;
      --ink: #e6edf3;
      --muted: #9da7b3;
      --accent: #2f81f7;
      --ok: #1f6feb;
      --warn: #f2cc60;
      --err: #ff6b6b;
      --good: #2ea043;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font: 14px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, "Noto Sans", "Apple Color Emoji", "Segoe UI Emoji";
    }}
    header {{
      display:flex; align-items:center; justify-content: space-between;
      padding: 16px 20px; border-bottom: 1px solid #202634;
      position: sticky; top: 0; background: rgba(14,17,23,0.9); backdrop-filter: blur(6px);
    }}
    header h1 {{ margin: 0; font-size: 18px; }}
    .grid {{
      display: grid; grid-template-columns: repeat(12, 1fr); gap: 16px; padding: 16px;
    }}
    .card {{
      background: var(--card); border: 1px solid #222a39; border-radius: 14px; padding: 16px;
    }}
    .span-4 {{ grid-column: span 4; }}
    .span-6 {{ grid-column: span 6; }}
    .span-8 {{ grid-column: span 8; }}
    .span-12 {{ grid-column: span 12; }}
    h2 {{ margin-top: 0; font-size: 16px; }}
    label {{ color: var(--muted); font-size: 12px; display:block; margin-bottom:4px; }}
    input, select {{
      width: 100%; padding: 8px 10px; border-radius: 10px; border: 1px solid #2a3242;
      background: #0c111b; color: var(--ink);
    }}
    .row {{ display:flex; gap: 10px; }}
    .row > div {{ flex: 1; }}
    button {{
      padding: 10px 14px; border-radius: 10px; border: 1px solid #2a3242; background: #0c111b; color: var(--ink);
      cursor: pointer; transition: all .15s ease;
    }}
    button.primary {{ background: var(--accent); border-color: var(--accent); color: white; }}
    button.good {{ background: var(--good); border-color: var(--good); color: #fff; }}
    button.warn {{ background: var(--warn); border-color: var(--warn); color: #111; }}
    button:hover {{ filter: brightness(1.1); transform: translateY(-1px); }}
    pre {{
      background: #0b0f16; border: 1px solid #202634; color: #b7c1cc;
      padding: 12px; border-radius: 12px; overflow:auto; max-height: 420px; white-space: pre-wrap;
    }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ padding: 8px 10px; border-bottom: 1px solid #243048; text-align:left; }}
    .pill {{ display:inline-flex; padding:2px 8px; border-radius:999px; font-size:12px; border:1px solid #2a3242; color:#c8d2de; }}
    .muted {{ color: var(--muted);}}
    .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; }}
    .small {{ font-size: 12px; }}
  </style>
</head>
<body>
  <header>
    <h1>Crypto Trading System <span class="muted">· v{APP_VERSION}</span></h1>
    <div class="row">
      <button id="btn-refresh" title="Refresh panels">Refresh</button>
      <a href="/health/versions" target="_blank"><button>Health · Versions</button></a>
    </div>
  </header>

  <section class="grid">
    <div class="card span-6">
      <h2>Scan Runner</h2>
      <div class="row">
        <div>
          <label>Strategy</label>
          <select id="strat">
            <option value="c1">c1</option>
            <option value="c2">c2</option>
            <option value="c3">c3</option>
            <option value="c4">c4</option>
            <option value="c5">c5</option>
            <option value="c6">c6</option>
          </select>
        </div>
        <div>
          <label>Dry Run</label>
          <select id="dry">
            <option value="1" selected>Yes (no orders)</option>
            <option value="0">No (live orders)</option>
          </select>
        </div>
      </div>
      <div class="row" style="margin-top:10px;">
        <div><label>Symbols (comma-separated)</label><input id="symbols" value="BTC/USD,ETH/USD,SOL/USD,DOGE/USD"/></div>
      </div>
      <div class="row" style="margin-top:10px;">
        <div><label>Timeframe</label><input id="timeframe" value="5Min"/></div>
        <div><label>Limit</label><input id="limit" value="600"/></div>
      </div>
      <div class="row" style="margin-top:10px;">
        <div><label>Notional ($)</label><input id="notional" placeholder="e.g. 25"/></div>
        <div><label>Qty</label><input id="qty" placeholder="optional"/></div>
      </div>
      <div class="row" style="margin-top:10px;">
        <div><label>Extra Params (key=value, one per line)</label>
          <textarea id="extra" rows="6" style="width:100%; background:#0c111b; color:#e6edf3; border:1px solid #2a3242; border-radius:10px;" placeholder="rsi_len=9&#10;rsi_buy=60&#10;rsi_sell=40"></textarea>
        </div>
      </div>
      <div class="row" style="margin-top:12px;">
        <button class="primary" id="btn-run">Run Scan</button>
        <button class="good" id="btn-run-c1-live">$25 Quick · c1 LIVE</button>
        <button class="warn" id="btn-clear">Clear Output</button>
      </div>
      <div style="margin-top:12px;">
        <label>Response</label>
        <pre id="out">—</pre>
      </div>
    </div>

    <div class="card span-6">
      <h2>Health</h2>
      <div id="health-container">
        <div class="muted small">loading…</div>
      </div>
      <h2 style="margin-top:18px;">Recent Orders</h2>
      <div class="row">
        <div><label class="small">Status</label>
          <select id="orders-status">
            <option value="all" selected>all</option>
            <option value="open">open</option>
            <option value="closed">closed</option>
            <option value="canceled">canceled</option>
            <option value="filled">filled</option>
          </select>
        </div>
        <div><label class="small">Limit</label><input id="orders-limit" value="50"/></div>
        <div style="align-self:flex-end;"><button id="btn-orders">Refresh Orders</button></div>
      </div>
      <pre id="orders">—</pre>

      <h2 style="margin-top:18px;">Positions</h2>
      <div class="row">
        <div style="align-self:flex-end;"><button id="btn-positions">Refresh Positions</button></div>
      </div>
      <pre id="positions">—</pre>
    </div>

    <div class="card span-12">
      <h2>Candles Probe</h2>
      <div class="row">
        <div><label>Symbols</label><input id="probe-symbols" value="BTC/USD,ETH/USD,SOL/USD,DOGE/USD"/></div>
        <div><label>TF</label><input id="probe-tf" value="5m"/></div>
        <div><label>Limit</label><input id="probe-limit" value="3"/></div>
        <div style="align-self:flex-end;"><button id="btn-probe">Probe</button></div>
      </div>
      <pre id="probe">—</pre>
    </div>
  </section>

  <script>
  const $ = (id) => document.getElementById(id);
  function kvToQuery(kv) {{
    const params = [];
    for (const [k,v] of Object.entries(kv)) {{
      if (v === undefined || v === null || String(v).trim() === "") continue;
      params.push(encodeURIComponent(k) + "=" + encodeURIComponent(String(v)));
    }}
    return params.length ? ("?" + params.join("&")) : "";
  }}

  async function runScan(preset) {{
    const strat = preset?.strat || $("strat").value;
    const dry = preset?.dry ?? $("dry").value;
    const symbols = preset?.symbols || $("symbols").value;
    const timeframe = preset?.timeframe || $("timeframe").value;
    const limit = preset?.limit || $("limit").value;
    const notional = preset?.notional ?? $("notional").value;
    const qty = preset?.qty ?? $("qty").value;

    // parse extra k=v per line
    const extra = {{}};
    const raw = $("extra").value || "";
    raw.split(/\\r?\\n/).forEach(line => {{
      const s = line.trim();
      if (!s || s.startsWith("#")) return;
      const eq = s.indexOf("=");
      if (eq > 0) {{
        const k = s.slice(0,eq).trim();
        const v = s.slice(eq+1).trim();
        if (k) extra[k] = v;
      }}
    }});

    const kv = Object.assign({{
      dry, symbols, timeframe, limit, notional, qty
    }}, extra);

    const url = "/scan/" + strat + kvToQuery(kv);
    $("out").textContent = "POST " + url + "\\n…";
    try {{
      const res = await fetch(url, {{ method:"POST" }});
      const j = await res.json();
      $("out").textContent = JSON.stringify(j, null, 2);
    }} catch (e) {{
      $("out").textContent = "ERROR: " + e;
    }}
  }}

  async function refreshHealth() {{
    try {{
      const v = await fetch("/health/versions").then(r => r.json());
      const html = `
        <table>
          <tr><th>App</th><th>Exchange</th><th>Systems</th></tr>
          <tr>
            <td class="mono">${{v.app}}</td>
            <td class="mono">${{v.exchange}}</td>
            <td><pre class="small mono" style="max-height:160px;">${{JSON.stringify(v.systems, null, 2)}}</pre></td>
          </tr>
        </table>`;
      $("health-container").innerHTML = html;
    }} catch (e) {{
      $("health-container").innerHTML = `<div class="muted small">error: ${{e}}</div>`;
    }}
  }}

  async function refreshOrders() {{
    const status = $("orders-status").value || "all";
    const limit = $("orders-limit").value || "50";
    $("orders").textContent = "Loading…";
    try {{
      const j = await fetch(`/orders/recent?status=${{encodeURIComponent(status)}}&limit=${{encodeURIComponent(limit)}}`).then(r => r.json());
      $("orders").textContent = JSON.stringify(j, null, 2);
    }} catch (e) {{
      $("orders").textContent = "ERROR: " + e;
    }}
  }}

  async function refreshPositions() {{
    $("positions").textContent = "Loading…";
    try {{
      const j = await fetch("/positions").then(r => r.json());
      $("positions").textContent = JSON.stringify(j, null, 2);
    }} catch (e) {{
      $("positions").textContent = "ERROR: " + e;
    }}
  }}

  async function probeCandles() {{
    const symbols = $("probe-symbols").value;
    const tf = $("probe-tf").value;
    const limit = $("probe-limit").value;
    $("probe").textContent = "Loading…";
    try {{
      const j = await fetch(`/diag/candles?symbols=${{encodeURIComponent(symbols)}}&tf=${{encodeURIComponent(tf)}}&limit=${{encodeURIComponent(limit)}}`).then(r => r.json());
      $("probe").textContent = JSON.stringify(j, null, 2);
    }} catch (e) {{
      $("probe").textContent = "ERROR: " + e;
    }}
  }}

  $("btn-run").addEventListener("click", () => runScan());
  $("btn-run-c1-live").addEventListener("click", () => runScan({{
    strat:"c1", dry:"0", timeframe:"5Min", limit:"600", notional:"25"
  }}));
  $("btn-clear").addEventListener("click", () => $("out").textContent = "—");
  $("btn-refresh").addEventListener("click", () => {{ refreshHealth(); refreshOrders(); refreshPositions(); }});
  $("btn-orders").addEventListener("click", refreshOrders);
  $("btn-positions").addEventListener("click", refreshPositions);
  $("btn-probe").addEventListener("click", probeCandles);

  // initial
  refreshHealth(); refreshOrders(); refreshPositions();
  </script>
</body>
</html>
"""
    return Response(html, mimetype="text/html")

# ---------- Strategy endpoints ----------
# Keep separate functions to avoid endpoint name collisions.
@app.post("/scan/c1")
def scan_c1():
    return _run_strategy_direct(c1, "c1")

@app.post("/scan/c2")
def scan_c2():
    return _run_strategy_direct(c2, "c2")

@app.post("/scan/c3")
def scan_c3():
    return _run_strategy_direct(c3, "c3")

@app.post("/scan/c4")
def scan_c4():
    return _run_strategy_direct(c4, "c4")

@app.post("/scan/c5")
def scan_c5():
    return _run_strategy_direct(c5, "c5")

@app.post("/scan/c6")
def scan_c6():
    return _run_strategy_direct(c6, "c6")

# ---------- Health ----------
@app.get("/health/versions")
def health_versions():
    systems = {
        "c1": _module_version(c1) if c1 else "",
        "c2": _module_version(c2) if c2 else "",
        "c3": _module_version(c3) if c3 else "",
        "c4": _module_version(c4) if c4 else "",
        "c5": _module_version(c5) if c5 else "1.0.0",
        "c6": _module_version(c6) if c6 else "1.0.0",
    }
    return _ok({"app": APP_VERSION, "exchange": "alpaca", "systems": systems})

# ---------- Diag: crypto / candles / gate (minimal, but useful) ----------
@app.get("/diag/crypto")
def diag_crypto():
    out: Dict[str, Any] = {
        "ok": True,
        "exchange": "alpaca",
        "trading_base": getattr(broker, "trading_base", ""),
        "data_base_env": os.environ.get("ALPACA_DATA_BASE", "https://data.alpaca.markets"),
        "symbols": DEFAULT_SYMBOLS,
        "api_key_present": bool(os.environ.get("APCA_API_KEY_ID") or os.environ.get("ALPACA_KEY_ID")),
    }
    # Try probing account (optional)
    try:
        if hasattr(broker, "get_account"):
            acct = broker.get_account()
            # keep it short
            out["account_sample"] = {k: acct.get(k) for k in list(acct)[:24]} if isinstance(acct, dict) else str(acct)[:1000]
    except Exception as e:
        out["account_error"] = str(e)
    return _ok(out)

@app.get("/diag/candles")
def diag_candles():
    symbols = _parse_symbols_arg(request.args.get("symbols"))
    tf = request.args.get("tf", "5m")
    limit = int(request.args.get("limit", "3"))
    out = {
        "symbols": symbols,
        "timeframe": tf,
        "limit": limit,
        "rows": {},
        "last_error": "",
    }
    try:
        if market is None or not hasattr(market, "get_bars"):
            out["last_error"] = "market_missing_or_no_get_bars"
            return _ok(out)
        data = market.get_bars(symbols=symbols, timeframe=tf, limit=limit)  # dict
        for s in symbols:
            df = data.get(s)
            try:
                out["rows"][s] = int(len(df)) if df is not None else 0
            except Exception:
                out["rows"][s] = 0
    except Exception as e:
        out["last_error"] = str(e)
    return _ok(out)

@app.get("/diag/gate")
def diag_gate():
    # For crypto, markets are 24/7; keep simple
    return _ok({
        "gate_on": True,
        "decision": "open",
        "reason": "24/7 crypto",
        "clock": {"is_open": True, "next_open": "", "next_close": "", "source": "crypto-24x7"},
        "ts": datetime.now(timezone.utc).isoformat()
    })

# ---------- Orders / Positions ----------
@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status", "all")
    limit = int(request.args.get("limit", "50"))
    out: Dict[str, Any] = {"status": status, "limit": limit, "orders": []}
    try:
        if broker and hasattr(broker, "list_orders"):
            orders = broker.list_orders(status=status, limit=limit)
            # Ensure JSON serializable
            if isinstance(orders, list):
                out["orders"] = orders
            else:
                out["orders"] = [orders]
        else:
            out["error"] = "no_broker_list_orders"
    except Exception as e:
        out["error"] = str(e)
    return _ok(out)

@app.get("/positions")
def positions_get():
    out: Dict[str, Any] = {"positions": []}
    try:
        if broker and hasattr(broker, "list_positions"):
            poss = broker.list_positions()
            if isinstance(poss, list):
                out["positions"] = poss
            else:
                out["positions"] = [poss]
        else:
            out["error"] = "no_broker_list_positions"
    except Exception as e:
        out["error"] = str(e)
    return _ok(out)

# ---------- Signals (optional placeholder to prevent 404 on UI links) ----------
@app.get("/signals")
def signals_get():
    # If you have a signals store, populate here.
    return _ok({"ok": True, "signals": [], "ts": datetime.now(timezone.utc).isoformat()})

# ---------- Main ----------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port)
