# app.py
# Crypto Trading System - Flask Service
# Version: 1.7.1 (2025-09-19)
#
# - Single-file Flask app with embedded dashboard HTML
# - Safe bootstrapping for market/broker (never None)
# - Uniform scan runner for strategies c1..c6 (kwargs-compatible)
# - Candles diagnostics with batch + per-symbol fallbacks
# - Simple positions & orders viewers
#
# Environment (typical):
#   ALPACA_KEY_ID, ALPACA_SECRET_KEY
#   ALPACA_PAPER=True
#   ALPACA_DATA_BASE=https://data.alpaca.markets
#   ALPACA_TRADING_BASE=https://paper-api.alpaca.markets/v2
#
# Routes (selection):
#   GET  /                      -> /dashboard
#   GET  /dashboard             -> Embedded UI
#   GET  /health/versions       -> app + exchange + strategies list
#   GET  /diag/gate             -> trading gate (crypto 24/7)
#   GET  /diag/candles          -> candles fetch debug
#   GET  /positions             -> open positions
#   GET  /orders/recent         -> recent orders
#   POST /scan/<strategy>       -> run c1..c6 (dry=1 for dry-run)
#
# Example (dry):
#   curl -sS -X POST "https://<host>/scan/c1?dry=1&timeframe=5Min&limit=600&symbols=BTC/USD,ETH/USD,SOL/USD,DOGE/USD"
#
# Example (live small notional):
#   curl -sS -X POST "https://<host>/scan/c1?dry=0&timeframe=5Min&limit=600&notional=25&symbols=BTC/USD,ETH/USD,SOL/USD,DOGE/USD"

from __future__ import annotations

import importlib
import json
import os
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, Response, jsonify, redirect, request

# Optional .env support (silent if not present)
try:
    from dotenv import load_dotenv

    load_dotenv()
except Exception:
    pass

APP_VERSION = "1.7.1"

# --- Service wrappers (your project modules) ---
# We only import symbols we actually reference here.
try:
    from services.market_crypto import MarketCrypto
except Exception as e:
    # Provide a helpful error early if module missing
    print("ERROR: failed to import services.market_crypto.MarketCrypto:", e, file=sys.stderr)
    MarketCrypto = None  # type: ignore

try:
    from services.exchange_exec import ExchangeExec
except Exception as e:
    print("ERROR: failed to import services.exchange_exec.ExchangeExec:", e, file=sys.stderr)
    ExchangeExec = None  # type: ignore


# --- Flask app ---
app = Flask(__name__)

# --- Globals (lazy-init via _ensure_context) ---
market = None
broker = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_context():
    """
    (Re)create market/broker instances exactly once per process,
    and ensure they are never None when used.
    """
    global market, broker

    # Market (data provider)
    if market is None:
        if MarketCrypto is None:
            raise RuntimeError("MarketCrypto class unavailable (import failed).")
        if hasattr(MarketCrypto, "from_env") and callable(getattr(MarketCrypto, "from_env")):
            market = MarketCrypto.from_env()
        else:
            # Fallback to direct constructor if your class expects env vars inside __init__
            market = MarketCrypto()

    # Broker (execution)
    if broker is None:
        if ExchangeExec is None:
            raise RuntimeError("ExchangeExec class unavailable (import failed).")
        if hasattr(ExchangeExec, "from_env") and callable(getattr(ExchangeExec, "from_env")):
            broker = ExchangeExec.from_env()
        else:
            broker = ExchangeExec()

    return market, broker


def _ok_json(payload: Dict[str, Any], status: int = 200) -> Response:
    return app.response_class(
        response=json.dumps(payload, default=str),
        status=status,
        mimetype="application/json",
    )


def _err_json(message: str, status: int = 200, **extra) -> Response:
    payload = {"ok": False, "error": message}
    payload.update(extra)
    return _ok_json(payload, status=status)


def _parse_bool(val: Optional[str], default: bool = False) -> bool:
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "t", "yes", "y"}


def _parse_symbols() -> List[str]:
    # Accept either `symbols=` or `symbol=` (comma-separated)
    s = request.args.get("symbols") or request.args.get("symbol")
    if not s:
        # Default universe
        return ["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD"]
    # split on commas, trim
    return [x.strip() for x in s.split(",") if x.strip()]


def _collect_params() -> Dict[str, Any]:
    """
    Collect all query args into a dictionary, with a few normalizations.
    Strategy modules can cherry-pick what they need.
    """
    params: Dict[str, Any] = {}
    # Normalize synonyms: timeframe/limit appear commonly
    if "timeframe" in request.args:
        params["timeframe"] = request.args.get("timeframe")
    elif "tf" in request.args:
        params["timeframe"] = request.args.get("tf")

    if "limit" in request.args:
        try:
            params["limit"] = int(request.args.get("limit", "600"))
        except Exception:
            params["limit"] = 600

    # Pass through notional/qty if present (for broker sizing)
    if "notional" in request.args:
        try:
            params["notional"] = float(request.args.get("notional"))
        except Exception:
            pass
    if "qty" in request.args:
        try:
            params["qty"] = float(request.args.get("qty"))
        except Exception:
            pass

    # Generic pass-through (everything else)
    for k, v in request.args.items():
        if k in {"dry", "symbols", "symbol", "timeframe", "tf", "limit", "notional", "qty"}:
            continue
        params[k] = v
    return params


def _load_strategy_module(name: str):
    """
    Import strategies dynamically from strategies.<name>.
    Only allow known names.
    """
    allowed = {"c1", "c2", "c3", "c4", "c5", "c6"}
    if name not in allowed:
        raise ValueError(f"Unknown strategy '{name}'. Allowed: {sorted(allowed)}")
    return importlib.import_module(f"strategies.{name}")


def _pwrite_factory() -> Tuple[List[str], Any]:
    """
    Returns a capture list and a writer callable to collect log lines.
    """
    lines: List[str] = []

    def pwrite(*args, **kwargs):
        msg = " ".join(str(a) for a in args)
        lines.append(msg)

    return lines, pwrite


def _run_strategy_direct(name: str, dry: bool) -> Dict[str, Any]:
    """
    Core runner used by /scan/<strategy> routes.
    Always ensures context; always calls with kwargs so older strategies
    that don't accept certain arguments will ignore them.
    """
    mkt, brk = _ensure_context()
    params = _collect_params()
    symbols = _parse_symbols()

    log_lines, pwrite = _pwrite_factory()

    try:
        mod = _load_strategy_module(name)

        # Use kwargs so strategies with older signatures won't crash.
        # Expected baseline: run(market, broker, symbols, params, **kwargs)
        results = mod.run(
            mkt,
            brk,
            symbols,
            params,
            dry=dry,
            pwrite=pwrite,
            log=None,
            notional=params.get("notional"),
            qty=params.get("qty"),
        )

        out = {
            "ok": True,
            "force": False,
            "dry": dry,
            "strategy": name,
            "results": results,
        }
        if log_lines:
            out["log"] = log_lines[-200:]  # cap
        return out

    except Exception as e:
        tb = traceback.format_exc()
        return {
            "ok": False,
            "force": False,
            "dry": dry,
            "strategy": name,
            "error": str(e),
            "trace": tb,
            "log": log_lines[-200:] if log_lines else [],
        }


# -------------------------
# Routes
# -------------------------

@app.get("/")
def root():
    return redirect("/dashboard", code=302)


@app.get("/dashboard")
def dashboard():
    # Minimal, responsive dashboard; no external assets.
    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Crypto System Dashboard • v{APP_VERSION}</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root {{
  --bg: #0f1115; --card:#161a22; --muted:#8591a3; --text:#e8ecf1; --acc:#2dd4bf; --err:#ef4444; --ok:#22c55e;
  --code:#0b1220; --chip:#1e2430;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.4 system-ui,Segoe UI,Roboto,Inter,Arial; }}
header {{
  padding:16px 20px; border-bottom:1px solid #232938;
  display:flex; align-items:center; justify-content:space-between;
}}
h1 {{ margin:0; font-size:18px; }}
small.muted {{ color:var(--muted); }}
.container {{ padding:16px; max-width:1200px; margin:0 auto; }}
.grid {{ display:grid; gap:14px; grid-template-columns: repeat(auto-fit, minmax(280px,1fr)); }}
.card {{ background:var(--card); border:1px solid #232938; border-radius:12px; padding:14px; }}
.card h2 {{ margin:0 0 10px; font-size:16px; }}
.row {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; }}
input, select {{ background:#0c1018; color:var(--text); border:1px solid #2a3344; border-radius:8px; padding:8px 10px; }}
input[type="text"]{{ width:100%; }}
button {{
  background:var(--acc); color:#04211e; border:none; padding:8px 12px; border-radius:8px; cursor:pointer; font-weight:600;
}}
button.secondary {{ background:#2a3344; color:var(--text); }}
pre {{
  background:var(--code); color:#cfdaea; border:1px solid #22314d; border-radius:10px; padding:10px; overflow:auto; max-height:260px;
}}
.badge {{ display:inline-block; padding:2px 6px; border-radius:999px; background:var(--chip); color:#c4cedb; font-size:12px; }}
.kv {{ display:grid; grid-template-columns:140px 1fr; gap:6px; font-family:ui-monospace,Consolas,Monaco,monospace; }}
.kv b {{ color:#a2acc0; }}
hr.div {{ border:0; border-top:1px solid #242b3b; margin:10px 0; }}
footer {{ color:var(--muted); text-align:center; padding:12px; }}
</style>
</head>
<body>
<header>
  <h1>Crypto System <small class="muted">v{APP_VERSION}</small></h1>
  <div class="row">
    <span class="badge" id="gate-badge">gate: ...</span>
    <span class="badge" id="vers-badge">versions: ...</span>
  </div>
</header>

<div class="container">
  <div class="grid">
    <div class="card">
      <h2>Scan Runner</h2>
      <div class="row" style="gap:6px;">
        <label>Strategy</label>
        <select id="strat">
          <option>c1</option>
          <option>c2</option>
          <option>c3</option>
          <option>c4</option>
          <option>c5</option>
          <option>c6</option>
        </select>
        <label>Dry</label>
        <select id="dry">
          <option value="1">1 (dry)</option>
          <option value="0">0 (live)</option>
        </select>
      </div>
      <div class="row" style="margin-top:8px;">
        <input id="symbols" type="text" placeholder="symbols (comma-separated)" value="BTC/USD,ETH/USD,SOL/USD,DOGE/USD">
      </div>
      <div class="row" style="margin-top:8px;">
        <input id="timeframe" type="text" placeholder="timeframe" value="5Min">
        <input id="limit" type="text" placeholder="limit" value="600">
        <input id="notional" type="text" placeholder="notional (optional)">
        <input id="qty" type="text" placeholder="qty (optional)">
      </div>
      <div class="row" style="margin-top:8px;">
        <button onclick="runScan()">Run Scan</button>
        <button class="secondary" onclick="runAllDry()">Run C1–C6 (dry)</button>
      </div>
      <pre id="scan-out">{}</pre>
    </div>

    <div class="card">
      <h2>Candles Diagnostics</h2>
      <div class="row" style="gap:6px;">
        <input id="cand-syms" type="text" value="BTC/USD,ETH/USD,SOL/USD,DOGE/USD">
        <input id="cand-tf" type="text" value="5m">
        <input id="cand-limit" type="text" value="3">
        <button onclick="loadCandles()">Fetch</button>
      </div>
      <pre id="cand-out">{}</pre>
    </div>

    <div class="card">
      <h2>Positions</h2>
      <div class="row">
        <button onclick="loadPositions()">Refresh</button>
      </div>
      <pre id="pos-out">[]</pre>
    </div>

    <div class="card">
      <h2>Orders (recent)</h2>
      <div class="row" style="gap:6px;">
        <input id="ord-status" type="text" value="all">
        <input id="ord-limit" type="text" value="50">
        <button onclick="loadOrders()">Refresh</button>
      </div>
      <pre id="ord-out">[]</pre>
    </div>
  </div>

  <hr class="div">
  <div class="kv">
    <b>Time (UTC)</b><span id="utc-now">...</span>
    <b>Data Base</b><span id="data-base">...</span>
    <b>Trading Base</b><span id="trade-base">...</span>
  </div>
</div>

<footer>© {datetime.utcnow().year} Crypto System</footer>

<script>
async function jget(url) {{
  const r = await fetch(url);
  return await r.json();
}}
async function jpost(url) {{
  const r = await fetch(url, {{ method: "POST" }});
  return await r.json();
}}

function enc(v) {{ return encodeURIComponent(v); }}
function setText(id, val) {{ document.getElementById(id).textContent = typeof val === "string" ? val : JSON.stringify(val, null, 2); }}

async function hydrateTop() {{
  try {{
    const gate = await jget("/diag/gate");
    document.getElementById("gate-badge").textContent = "gate: " + gate.decision;
  }} catch {{ document.getElementById("gate-badge").textContent = "gate: n/a"; }}

  try {{
    const vers = await jget("/health/versions");
    document.getElementById("vers-badge").textContent = "versions: " + vers.app;
    setText("data-base", vers.data_base || "(n/a)");
    setText("trade-base", vers.trading_base || "(n/a)");
  }} catch {{
    document.getElementById("vers-badge").textContent = "versions: n/a";
  }}

  setText("utc-now", new Date().toISOString());
}}

async function runScan() {{
  const strat = document.getElementById("strat").value;
  const dry = document.getElementById("dry").value;
  const syms = document.getElementById("symbols").value;
  const tf = document.getElementById("timeframe").value;
  const limit = document.getElementById("limit").value;
  const notional = document.getElementById("notional").value;
  const qty = document.getElementById("qty").value;

  let url = `/scan/${strat}?dry=${enc(dry)}&symbols=${enc(syms)}&timeframe=${enc(tf)}&limit=${enc(limit)}`;
  if (notional) url += `&notional=${enc(notional)}`;
  if (qty) url += `&qty=${enc(qty)}`;

  const res = await jpost(url);
  setText("scan-out", res);
}}

async function runAllDry() {{
  const tf = document.getElementById("timeframe").value;
  const limit = document.getElementById("limit").value;
  const syms = document.getElementById("symbols").value;
  const names = ["c1","c2","c3","c4","c5","c6"];
  const out = {{}};
  for (const n of names) {{
    const url = `/scan/${n}?dry=1&symbols=${enc(syms)}&timeframe=${enc(tf)}&limit=${enc(limit)}`;
    out[n] = await jpost(url);
  }}
  setText("scan-out", out);
}}

async function loadCandles() {{
  const syms = document.getElementById("cand-syms").value;
  const tf = document.getElementById("cand-tf").value;
  const limit = document.getElementById("cand-limit").value;
  const url = `/diag/candles?symbols=${enc(syms)}&tf=${enc(tf)}&limit=${enc(limit)}`;
  const res = await jget(url);
  setText("cand-out", res);
}}

async function loadPositions() {{
  const res = await jget("/positions");
  setText("pos-out", res);
}}

async function loadOrders() {{
  const status = document.getElementById("ord-status").value;
  const limit = document.getElementById("ord-limit").value;
  const url = `/orders/recent?status=${enc(status)}&limit=${enc(limit)}`;
  const res = await jget(url);
  setText("ord-out", res);
}}

hydrateTop();
</script>
</body>
</html>"""
    return html


@app.get("/health/versions")
def health_versions():
    try:
        mkt, brk = _ensure_context()
    except Exception:
        mkt = None
        brk = None

    systems = {"c1": "", "c2": "", "c3": "", "c4": "", "c5": "", "c6": ""}
    payload = {
        "app": APP_VERSION,
        "exchange": getattr(brk, "name", "alpaca") if brk else "alpaca",
        "systems": systems,
    }

    # Convenience: surface base URLs if the wrappers expose them
    try:
        payload["data_base"] = getattr(mkt, "data_base", None) or getattr(mkt, "bars_base", None)
    except Exception:
        pass
    try:
        payload["trading_base"] = getattr(brk, "trading_base", None)
    except Exception:
        pass

    return _ok_json(payload)


@app.get("/diag/gate")
def diag_gate():
    payload = {
        "gate_on": True,
        "decision": "open",
        "reason": "24/7 crypto",
        "clock": {"is_open": True, "next_open": "", "next_close": "", "source": "crypto-24x7"},
        "ts": _now_iso(),
    }
    return _ok_json(payload)


@app.get("/diag/candles")
def diag_candles():
    """
    Fetch candles with a resilient path:
      1) try batch query if wrapper supports it
      2) per-symbol fallback if needed
    Return debug of attempts/urls/errors and row counts.
    """
    try:
        mkt, _ = _ensure_context()
    except Exception as e:
        return _err_json(f"market init failed: {e}")

    symbols = _parse_symbols()
    tf = request.args.get("tf") or request.args.get("timeframe") or "5m"
    try:
        limit = int(request.args.get("limit", "3"))
    except Exception:
        limit = 3

    attempts: List[str] = []
    rows_map: Dict[str, int] = {s: 0 for s in symbols}
    last_url: str = ""
    last_err: str = ""

    def _rows(df_like) -> int:
        # Accept pandas DataFrame or list-like
        try:
            import pandas as pd  # type: ignore
            if isinstance(df_like, pd.DataFrame):
                return int(df_like.shape[0])
        except Exception:
            pass
        # Alpaca SDK sometimes returns objects with .df or .data
        if hasattr(df_like, "df"):
            try:
                return int(getattr(df_like, "df").shape[0])
            except Exception:
                pass
        if hasattr(df_like, "data"):
            try:
                data = getattr(df_like, "data")
                return len(data) if hasattr(data, "__len__") else 0
            except Exception:
                pass
        # List of dicts
        if isinstance(df_like, (list, tuple)):
            return len(df_like)
        # Fallback
        try:
            # If object has 'empty' attr (like DataFrame)
            if getattr(df_like, "empty", False) is True:
                return 0
        except Exception:
            pass
        return 0

    # Batch attempt, if MarketCrypto exposes a multi-fetch (candles for multiple symbols).
    try:
        if hasattr(mkt, "candles"):
            last_url = ""
            attempts.append("batch")
            data = mkt.candles(symbols, timeframe=tf, limit=limit)  # expected dict: sym -> df
            if isinstance(data, dict):
                for s in symbols:
                    rows_map[s] = _rows(data.get(s))
            else:
                # If a single object returned for batch (unlikely), count once for first symbol
                n = _rows(data)
                if symbols:
                    rows_map[symbols[0]] = n
        else:
            last_err = "MarketCrypto missing .candles()"
    except Exception as e:
        last_err = f"batch error: {e}"

    # Per-symbol fallback for any that remain zero
    for s in symbols:
        if rows_map[s] > 0:
            continue
        try:
            attempts.append(s)
            one = None
            if hasattr(mkt, "candles"):
                one = mkt.candles([s], timeframe=tf, limit=limit)
                if isinstance(one, dict):
                    one = one.get(s)
            elif hasattr(mkt, "get_bars"):  # legacy signatures
                one = mkt.get_bars(s, timeframe=tf, limit=limit)
            rows_map[s] = _rows(one)
        except Exception as e:
            last_err = f"{last_err} | {s} error: {e}"

    payload = {
        "symbols": symbols,
        "timeframe": tf,
        "limit": limit,
        "last_attempts": attempts,
        "last_url": last_url,
        "last_error": last_err.strip(),
        "rows": rows_map,
    }
    return _ok_json(payload)


@app.get("/positions")
def positions():
    try:
        _, brk = _ensure_context()
        # Try common method names
        if hasattr(brk, "positions"):
            data = brk.positions()
        elif hasattr(brk, "get_positions"):
            data = brk.get_positions()
        else:
            data = []
        return _ok_json(data if isinstance(data, (list, dict)) else {"data": data})
    except Exception as e:
        return _err_json(str(e))


@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status", "all")
    try:
        limit = int(request.args.get("limit", "50"))
    except Exception:
        limit = 50
    try:
        _, brk = _ensure_context()
        if hasattr(brk, "orders_recent"):
            data = brk.orders_recent(status=status, limit=limit)
        elif hasattr(brk, "recent_orders"):
            data = brk.recent_orders(status=status, limit=limit)
        else:
            data = []
        return _ok_json(data if isinstance(data, (list, dict)) else {"data": data})
    except Exception as e:
        return _err_json(str(e))


# Unified scan endpoint family
@app.post("/scan/<name>")
def scan_name(name: str):
    dry = _parse_bool(request.args.get("dry"), default=True)  # default to dry=1 if omitted
    out = _run_strategy_direct(name, dry=dry)
    return _ok_json(out)


# -------------------------
# Entrypoint
# -------------------------

if __name__ == "__main__":
    # Bind host/port if present in env (Render uses PORT)
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "10000"))
    app.run(host=host, port=port)
