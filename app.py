# app.py — Crypto System API + Dashboard
# Version: 1.7.3
# - Adds strong env validation with readable errors
# - Exposes /scan/<name> for c1..c6
# - Robust /diag/candles using Alpaca v1beta3 (us feed)
# - Full HTML dashboard (no f-strings used; safe with braces)
# - /routes to list URL map

import os
import json
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

from flask import Flask, request, jsonify, redirect, Response

# Optional: load .env if present (local runs)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

APP_VERSION = "1.7.3"

# ---- Imports for your services ----
# These are expected to exist in your repo as previously discussed.
# MarketCrypto must offer: from_env(), candles(symbols, timeframe, limit) -> Dict[str, DataFrame]
# ExchangeExec must offer: from_env(), paper_buy/sell/notional, recent_orders(), positions()
try:
    from services.market_crypto import MarketCrypto
    from services.exchange_exec import ExchangeExec
except Exception as e:
    # Let the app still boot so /health/versions can show errors
    MarketCrypto = None  # type: ignore
    ExchangeExec = None  # type: ignore
    _IMPORT_ERROR = str(e)
else:
    _IMPORT_ERROR = None

app = Flask(__name__)

# ---------------------------
# Helpers
# ---------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _ok(data: Dict[str, Any]) -> Response:
    return jsonify(data)

def _err(msg: str, extra: Dict[str, Any] | None = None, code: int = 200) -> Response:
    payload = {"ok": False, "error": msg}
    if extra:
        payload.update(extra)
    return app.response_class(
        response=json.dumps(payload, default=str, ensure_ascii=False, indent=None),
        status=code,
        mimetype="application/json",
    )

def _need_env(*keys: str) -> Tuple[bool, str | None]:
    missing = [k for k in keys if not os.getenv(k)]
    if missing:
        return False, f"Missing {', '.join(missing)} in environment."
    return True, None

def _build_market() -> Tuple[Any | None, str | None]:
    """
    Create MarketCrypto from env with helpful error messages.
    """
    if _IMPORT_ERROR:
        return None, f"Import error: { _IMPORT_ERROR }"

    ok_env, msg = _need_env("ALPACA_KEY_ID", "ALPACA_SECRET_KEY")
    if not ok_env:
        return None, msg

    try:
        market = MarketCrypto.from_env()
        return market, None
    except Exception as e:
        return None, f"market init failed: {e}"

def _build_broker() -> Tuple[Any | None, str | None]:
    if _IMPORT_ERROR:
        return None, f"Import error: { _IMPORT_ERROR }"
    ok_env, msg = _need_env("ALPACA_KEY_ID", "ALPACA_SECRET_KEY")
    if not ok_env:
        return None, msg
    try:
        broker = ExchangeExec.from_env()
        return broker, None
    except Exception as e:
        return None, f"broker init failed: {e}"

def _parse_symbols(arg: str | None, default: List[str]) -> List[str]:
    if not arg:
        return default
    # Allow separators: comma or space
    parts = [p.strip() for p in arg.replace(" ", ",").split(",") if p.strip()]
    return parts or default

def _bool_arg(name: str, default: bool) -> bool:
    v = request.args.get(name)
    if v is None:
        return default
    return str(v).strip() not in ("0", "false", "False", "")

def _int_arg(name: str, default: int) -> int:
    v = request.args.get(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default

def _float_arg(name: str, default: float) -> float:
    v = request.args.get(name)
    try:
        return float(v) if v is not None else default
    except Exception:
        return default

def _str_arg(name: str, default: str) -> str:
    v = request.args.get(name)
    return v if v is not None else default

DEFAULT_SYMBOLS = ["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD"]

# --------------------------------
# Health & Info
# --------------------------------

@app.get("/")
def root():
    return redirect("/dashboard", code=302)

@app.get("/health/versions")
def health_versions():
    data_base = os.getenv("ALPACA_DATA_HOST", "")
    trading_base = os.getenv("ALPACA_TRADE_HOST", "")
    return _ok({
        "app": APP_VERSION,
        "exchange": "alpaca",
        "systems": {"c1": "", "c2": "", "c3": "", "c4": "", "c5": "", "c6": ""},
        "data_base": data_base,
        "trading_base": trading_base
    })

@app.get("/routes")
def routes():
    items = []
    for rule in app.url_map.iter_rules():
        items.append({
            "endpoint": rule.endpoint,
            "methods": sorted(list(rule.methods)),
            "rule": str(rule)
        })
    return _ok({"ok": True, "routes": items})

# --------------------------------
# Diag
# --------------------------------

@app.get("/diag/gate")
def diag_gate():
    # Crypto is 24/7
    return _ok({
        "gate_on": True,
        "decision": "open",
        "reason": "24/7 crypto",
        "clock": {"is_open": True, "next_open": "", "next_close": "", "source": "crypto-24x7"},
        "ts": _now_utc_iso()
    })

@app.get("/diag/crypto")
def diag_crypto():
    market, merr = _build_market()
    broker, berr = _build_broker()
    if merr:
        return _ok({
            "ok": True,
            "exchange": "alpaca",
            "data_base_env": os.getenv("ALPACA_DATA_HOST", ""),
            "trading_base": os.getenv("ALPACA_TRADE_HOST", ""),
            "api_key_present": bool(os.getenv("ALPACA_KEY_ID")),
            "error": merr,
            "symbols": DEFAULT_SYMBOLS
        })
    base = getattr(market, "data_base", os.getenv("ALPACA_DATA_HOST", ""))
    tbase = getattr(broker, "trading_base", os.getenv("ALPACA_TRADE_HOST", "")) if broker else os.getenv("ALPACA_TRADE_HOST", "")
    account_sample = {}
    try:
        if broker:
            account_sample = broker.sample_account()
    except Exception as e:
        return _ok({
            "ok": True,
            "exchange": "alpaca",
            "data_base_env": os.getenv("ALPACA_DATA_HOST", ""),
            "trading_base": tbase,
            "api_key_present": True,
            "account_error": str(e),
            "symbols": DEFAULT_SYMBOLS
        })
    return _ok({
        "ok": True,
        "exchange": "alpaca",
        "data_base": base,
        "trading_base": tbase,
        "api_key_present": True,
        "account_sample": account_sample,
        "symbols": DEFAULT_SYMBOLS
    })

@app.get("/diag/candles")
def diag_candles():
    symbols = _parse_symbols(request.args.get("symbols"), DEFAULT_SYMBOLS)
    tf = _str_arg("tf", _str_arg("timeframe", "5Min"))
    limit = _int_arg("limit", 3)

    market, merr = _build_market()
    if merr:
        return _ok({"ok": False, "error": merr})

    attempts = []
    last_url = ""
    last_error = ""
    rows: Dict[str, int] = {s: 0 for s in symbols}

    try:
        # Try batch first (MarketCrypto should batch on its own)
        attempts.append(f"{getattr(market, 'data_base', '')}/v1beta3/crypto/us/bars?symbols={','.join([s.replace('/', '%2F') for s in symbols])}&timeframe={tf}&limit={limit}")
        data = market.candles(symbols, timeframe=tf, limit=limit)  # expected Dict[str, DataFrame]
        for s in symbols:
            df = data.get(s)
            rows[s] = int(getattr(df, "shape", [0, 0])[0]) if df is not None else 0
        last_url = attempts[-1]
    except Exception as e:
        last_error = str(e)
        # fallback per symbol
        for s in symbols:
            try:
                url = f"{getattr(market, 'data_base', '')}/v1beta3/crypto/us/bars?symbols={s.replace('/', '%2F')}&timeframe={tf}&limit={limit}"
                attempts.append(url)
                d1 = market.candles([s], timeframe=tf, limit=limit)
                df = d1.get(s)
                rows[s] = int(getattr(df, "shape", [0, 0])[0]) if df is not None else 0
                last_url = url
            except Exception as e1:
                last_error = f"{last_error} | {s} -> {e1}"

    return _ok({
        "symbols": symbols,
        "timeframe": tf,
        "limit": limit,
        "rows": rows,
        "last_attempts": attempts,
        "last_url": last_url,
        "last_error": last_error
    })

# --------------------------------
# Orders / Positions
# --------------------------------

@app.get("/orders/recent")
def orders_recent():
    broker, berr = _build_broker()
    if berr:
        return _err(berr)
    status = _str_arg("status", "all")
    limit = _int_arg("limit", 50)
    try:
        items = broker.recent_orders(status=status, limit=limit)
        return _ok(items)
    except Exception as e:
        return _err(str(e))

@app.get("/positions")
def positions():
    broker, berr = _build_broker()
    if berr:
        return _err(berr)
    try:
        items = broker.positions()
        return _ok(items)
    except Exception as e:
        return _err(str(e))

# --------------------------------
# Strategy runner (C1–C6)
# --------------------------------

def _run_strategy_inline(name: str, dry: bool, symbols: List[str], params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expected module: strategies.c{name} with run(market, broker, symbols, params, *, dry, log)
    Returns: dict with "results":[...]
    """
    # Import lazily so app can boot even if strategy has syntax errors
    try:
        mod = __import__(f"strategies.{name}", fromlist=["*"])
    except Exception as e:
        return {"ok": False, "strategy": name, "error": f"import error: {e}"}

    market, merr = _build_market()
    if merr:
        return {"ok": False, "strategy": name, "error": merr}

    broker, berr = _build_broker()
    if berr and not dry:
        # For live, broker must be up. For dry, we can pass None.
        return {"ok": False, "strategy": name, "error": berr}

    def pwrite(msg: str):
        # lightweight logger to stdout (captured by Render logs)
        print(msg, flush=True)

    def log(**kw):
        pwrite(json.dumps(kw))

    try:
        # All strategies should accept dry=, log= kw only (positional: market, broker, symbols, params)
        out = mod.run(market, broker, symbols, params, dry=dry, log=log)
        if not isinstance(out, dict):
            return {"ok": False, "strategy": name, "error": "strategy returned non-dict"}
        out.setdefault("ok", True)
        out.setdefault("strategy", name)
        out.setdefault("dry", dry)
        return out
    except TypeError as te:
        # Typical signature mismatch
        return {"ok": False, "strategy": name, "error": f"{te}"}
    except Exception as e:
        return {"ok": False, "strategy": name, "error": f"{e}", "trace": traceback.format_exc(limit=3)}

@app.post("/scan/<name>")
def scan_named(name: str):
    # support c1..c6 only
    if name not in {"c1", "c2", "c3", "c4", "c5", "c6"}:
        return _err(f"unknown strategy: {name}", code=404)

    dry = _bool_arg("dry", True)
    symbols = _parse_symbols(request.args.get("symbols"), DEFAULT_SYMBOLS)

    # common params
    params: Dict[str, Any] = {
        "timeframe": _str_arg("timeframe", "5Min"),
        "limit": _int_arg("limit", 600),
        "notional": _float_arg("notional", 0.0),  # live only
    }

    # pass any extra query params through under params
    # (e.g., rsi_len=9&rsi_buy=60&rsi_sell=40)
    for k, v in request.args.items():
        if k in {"dry", "symbols", "timeframe", "limit", "notional"}:
            continue
        params[k] = v

    out = _run_strategy_inline(name, dry=dry, symbols=symbols, params=params)
    # Flask will jsonify dict already
    return _ok(out)

# --------------------------------
# Dashboard (full HTML)
# --------------------------------

@app.get("/dashboard")
def dashboard():
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Crypto System Dashboard</title>
<style>
:root {
  --bg: #0b0f14;
  --card: #111822;
  --muted: #8aa0b8;
  --text: #eaf2ff;
  --accent: #5eead4;
  --warn: #f59e0b;
  --err: #ef4444;
  --ok: #22c55e;
  --link: #93c5fd;
}
* { box-sizing: border-box; }
body {
  margin: 0; padding: 24px; background: linear-gradient(180deg,#0b0f14,#0a1018 50%,#0b0f14);
  color: var(--text); font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Apple Color Emoji", "Segoe UI Emoji";
}
h1 { margin: 0 0 12px 0; font-size: 24px; font-weight: 700; letter-spacing: .2px; }
p.mono { color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 12px; }
.card {
  background: linear-gradient(180deg, #111822, #0e151f); border: 1px solid #1d2838;
  border-radius: 16px; padding: 16px; box-shadow: 0 10px 24px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.02);
}
.grid { display: grid; gap: 16px; grid-template-columns: repeat(12, 1fr); }
.col-4 { grid-column: span 4; }
.col-8 { grid-column: span 8; }
label { font-size: 12px; color: var(--muted); display: block; margin: 0 0 6px; }
input, select {
  width: 100%; background: #0a0f16; color: var(--text); border: 1px solid #1e293b; border-radius: 10px; padding: 10px 12px;
}
.row { display: grid; gap: 12px; grid-template-columns: repeat(4, 1fr); }
button {
  background: linear-gradient(180deg, #1f2937, #111827); border: 1px solid #2b374a; color: var(--text);
  padding: 10px 14px; border-radius: 10px; cursor: pointer; font-weight: 600;
}
button.primary { background: linear-gradient(180deg, #0ea5e9, #0284c7); border: 1px solid #1e40af; }
button.warn { background: linear-gradient(180deg, #f59e0b, #b45309); border: 1px solid #b45309; }
button:disabled { opacity: .6; cursor: not-allowed; }
pre {
  background: #0a0f16; color: #cbd5e1; border: 1px solid #1e293b; border-radius: 10px; padding: 12px;
  white-space: pre-wrap; word-break: break-word; font-size: 12px;
}
hr { border: 0; border-top: 1px solid #1f2937; margin: 16px 0; }
.kv { display: grid; grid-template-columns: 140px 1fr; row-gap: 6px; }
.kv div { padding: 3px 0; color: var(--muted); }
.kv div b { color: var(--text); }
a, a:visited { color: var(--link); text-decoration: none; }
.badge { display: inline-block; padding: 3px 8px; border: 1px solid #2b374a; border-radius: 999px; background: #0a0f16; color: var(--muted); font-size: 11px; }
</style>
</head>
<body>
  <div class="grid">
    <div class="col-8">
      <div class="card">
        <h1>Crypto Scanner</h1>
        <p class="mono">Version: 1.7.3 &nbsp; • &nbsp; Exchange: Alpaca (paper-friendly)</p>
        <div class="row">
          <div>
            <label>Strategy</label>
            <select id="strategy">
              <option value="c1">c1</option>
              <option value="c2">c2</option>
              <option value="c3">c3</option>
              <option value="c4">c4</option>
              <option value="c5">c5</option>
              <option value="c6">c6</option>
            </select>
          </div>
          <div>
            <label>Symbols (comma separated)</label>
            <input id="symbols" value="BTC/USD,ETH/USD,SOL/USD,DOGE/USD" />
          </div>
          <div>
            <label>Timeframe</label>
            <input id="timeframe" value="5Min" />
          </div>
          <div>
            <label>Limit</label>
            <input id="limit" value="600" />
          </div>
        </div>
        <div class="row" style="margin-top:12px">
          <div>
            <label>Dry Run</label>
            <select id="dry">
              <option value="1">1 (no orders)</option>
              <option value="0">0 (live)</option>
            </select>
          </div>
          <div>
            <label>Notional (live)</label>
            <input id="notional" value="25" />
          </div>
          <div>
            <label>Extra Params (k=v&k2=v2)</label>
            <input id="xtra" placeholder="rsi_len=9&rsi_buy=60&rsi_sell=40" />
          </div>
          <div style="display:flex; align-items:flex-end; gap:8px;">
            <button class="primary" id="runBtn">Run Scan</button>
            <button id="pingBtn">Ping Candles</button>
          </div>
        </div>
        <hr/>
        <div id="out" class="mono">Output will appear here…</div>
      </div>
    </div>

    <div class="col-4">
      <div class="card">
        <h2 style="margin:0 0 8px">Health</h2>
        <div id="health" class="kv mono">
          <div>App</div><div><span class="badge">loading…</span></div>
          <div>Data base</div><div><span class="badge">loading…</span></div>
          <div>Trade base</div><div><span class="badge">loading…</span></div>
        </div>
        <hr/>
        <h3 style="margin:0 0 8px">Gate</h3>
        <div id="gate" class="kv mono">
          <div>Status</div><div><span class="badge">loading…</span></div>
          <div>Reason</div><div><span class="badge">loading…</span></div>
          <div>TS</div><div><span class="badge">loading…</span></div>
        </div>
        <hr/>
        <a href="/routes" target="_blank">See all routes</a>
      </div>
    </div>
  </div>

<script>
async function jget(url) {
  const r = await fetch(url, { method: "GET" });
  return await r.json();
}
async function jpost(url) {
  const r = await fetch(url, { method: "POST" });
  return await r.json();
}
function qs(id){ return document.getElementById(id); }
function show(o){ qs("out").innerHTML = "<pre>"+JSON.stringify(o, null, 2)+"</pre>"; }

async function refreshHealth(){
  try {
    const hv = await jget("/health/versions");
    const h = qs("health");
    h.innerHTML = `
      <div>App</div><div><b>${hv.app||"-"}</b></div>
      <div>Data base</div><div><b>${hv.data_base||"-"}</b></div>
      <div>Trade base</div><div><b>${hv.trading_base||"-"}</b></div>
    `;
  } catch(e) {}
}
async function refreshGate(){
  try {
    const g = await jget("/diag/gate");
    const el = qs("gate");
    el.innerHTML = `
      <div>Status</div><div><b>${g.decision||"-"}</b></div>
      <div>Reason</div><div><b>${g.reason||"-"}</b></div>
      <div>TS</div><div><b>${g.ts||"-"}</b></div>
    `;
  } catch(e) {}
}

qs("runBtn").addEventListener("click", async () => {
  const name = qs("strategy").value;
  const dry = qs("dry").value;
  const symbols = encodeURIComponent(qs("symbols").value);
  const tf = encodeURIComponent(qs("timeframe").value);
  const limit = encodeURIComponent(qs("limit").value);
  const notional = encodeURIComponent(qs("notional").value || "0");
  const xtra = qs("xtra").value.trim();
  let url = `/scan/${name}?dry=${dry}&symbols=${symbols}&timeframe=${tf}&limit=${limit}`;
  if (notional && notional !== "0") url += `&notional=${notional}`;
  if (xtra) url += `&${xtra}`;
  show({ request: url });
  const out = await jpost(url);
  show(out);
});

qs("pingBtn").addEventListener("click", async () => {
  const tf = encodeURIComponent(qs("timeframe").value || "5Min");
  const limit = encodeURIComponent(qs("limit").value || "3");
  const symbols = encodeURIComponent(qs("symbols").value);
  const url = `/diag/candles?symbols=${symbols}&limit=${limit}&tf=${tf}`;
  show({ request: url });
  const out = await jget(url);
  show(out);
});

refreshHealth();
refreshGate();
</script>
</body>
</html>
"""
    return Response(html, mimetype="text/html")

# --------------------------------
# Main
# --------------------------------

if __name__ == "__main__":
    # Bind 0.0.0.0 for Render
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
