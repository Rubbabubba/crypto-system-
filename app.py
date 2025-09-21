# app.py — Crypto System API + Dashboard
# Version: 1.8.0
# - Adds /pnl/summary and /pnl/daily (KPI cards + calendar heatmap data)
# - Adds /config/symbols (GET/POST) with JSON-file persistence + DEFAULT_SYMBOLS env override
# - Dashboard now includes: KPI overview cards, P&L calendar heatmap, Symbols editor (Load/Save)
# - scan/<name> auto-falls back to saved symbols when none are passed
# - Preserves all existing routes, helpers, and scanner UI from 1.7.3
#
# NOTE: Render disk is ephemeral; for durable symbols use Redis/DB later.
#       SYMBOLS_PATH (env) controls where symbols.json is stored (default: ./data/symbols.json)

import os
import json
import traceback
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Any, List, Tuple
from collections import defaultdict

from flask import Flask, request, jsonify, redirect, Response, send_from_directory

# Optional: load .env if present (local runs)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

APP_VERSION = "1.8.0"

# ---- Imports for your services ----
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

app = Flask(__name__, static_folder="static", static_url_path="/static")

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

# --------------------------------
# Symbols persistence (NEW)
# --------------------------------

SYMBOLS_PATH = os.getenv("SYMBOLS_PATH", "./data/symbols.json")
os.makedirs(os.path.dirname(SYMBOLS_PATH), exist_ok=True)

# Extended default list (can be overridden with env DEFAULT_SYMBOLS=CSV)
_DEFAULTS_BUILTIN = [
    "BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD",
    "XRP/USD", "ADA/USD", "AVAX/USD", "LINK/USD",
    "TON/USD", "TRX/USD", "BCH/USD", "LTC/USD",
    "APT/USD", "ARB/USD", "SUI/USD", "OP/USD", "MATIC/USD",
]
_env_defaults = os.getenv("DEFAULT_SYMBOLS", "").strip()
if _env_defaults:
    DEFAULT_SYMBOLS = [s.strip().upper() for s in _env_defaults.split(",") if s.strip()]
else:
    DEFAULT_SYMBOLS = _DEFAULTS_BUILTIN[:]

def _symbols_load() -> List[str]:
    if os.path.exists(SYMBOLS_PATH):
        try:
            with open(SYMBOLS_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and isinstance(data.get("symbols"), list):
                    return [str(s).upper() for s in data["symbols"]]
                if isinstance(data, list):
                    return [str(s).upper() for s in data]
        except Exception:
            pass
    return DEFAULT_SYMBOLS[:]

def _symbols_save(symbols: List[str]) -> List[str]:
    cleaned = [str(s).strip().upper() for s in symbols if isinstance(s, str) and s.strip()]
    with open(SYMBOLS_PATH, "w", encoding="utf-8") as f:
        json.dump({"symbols": cleaned}, f, indent=2)
    return cleaned

@app.get("/config/symbols")
def config_symbols_get():
    return _ok({"version": APP_VERSION, "symbols": _symbols_load()})

@app.post("/config/symbols")
def config_symbols_post():
    payload = request.get_json(silent=True) or {}
    symbols = payload.get("symbols")
    if not isinstance(symbols, list) or not symbols:
        return _err('Provide JSON with non-empty list: {"symbols": ["BTC/USD", ...]}', code=400)
    for s in symbols:
        if not isinstance(s, str) or "/" not in s:
            return _err(f"Invalid symbol '{s}'. Expect like 'BTC/USD'.", code=400)
    saved = _symbols_save(symbols)
    return _ok({"ok": True, "symbols": saved, "count": len(saved), "version": APP_VERSION})

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
    base = getattr(market, "data_base", os.getenv("ALPACA_DATA_HOST", "")) if market else os.getenv("ALPACA_DATA_HOST", "")
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
    symbols = _parse_symbols(request.args.get("symbols"), _symbols_load())
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
    limit = _int_arg("limit", 200)  # slight bump to help daily agg
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
# P&L aggregation (NEW)
# --------------------------------

def _parse_dt(s: str | None):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        try:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def _order_is_filled(o: Dict[str, Any]) -> bool:
    st = str(o.get("status", "")).lower()
    if st in ("filled", "closed", "done", "complete", "executed"):
        return True
    return bool(_parse_dt(o.get("filled_at")))

def _order_dt(o: Dict[str, Any]):
    for k in ("filled_at", "completed_at", "submitted_at", "created_at", "timestamp"):
        dt = _parse_dt(o.get(k))
        if dt:
            return dt
    return None

def _realized_from_order(o: Dict[str, Any]) -> float:
    for k in ("realized_pnl", "pnl", "realizedPnL", "realized"):
        v = o.get(k)
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            try:
                return float(v)
            except Exception:
                pass
    return 0.0

def _aggregate_daily_realized(orders: List[Dict[str, Any]], start_date: date, end_date: date) -> Dict[str, float]:
    day_pnl: Dict[str, float] = defaultdict(float)
    for o in orders:
        if not _order_is_filled(o):
            continue
        dt = _order_dt(o)
        if not dt:
            continue
        d = dt.astimezone(timezone.utc).date()
        if d < start_date or d > end_date:
            continue
        day_pnl[d.isoformat()] += _realized_from_order(o)

    out: Dict[str, float] = {}
    cur = start_date
    while cur <= end_date:
        out[cur.isoformat()] = float(day_pnl.get(cur.isoformat(), 0.0))
        cur += timedelta(days=1)
    return out

def _compute_summary(orders: List[Dict[str, Any]], positions: List[Dict[str, Any]]) -> Dict[str, Any]:
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    start_of_week = today - timedelta(days=today.weekday())  # Monday
    start_of_month = today.replace(day=1)

    daily_90 = _aggregate_daily_realized(orders, start_date=today - timedelta(days=89), end_date=today)
    realized_today = daily_90.get(today.isoformat(), 0.0)
    realized_week = sum(v for d, v in daily_90.items() if datetime.fromisoformat(d).date() >= start_of_week)
    realized_month = sum(v for d, v in daily_90.items() if datetime.fromisoformat(d).date() >= start_of_month)

    unreal = 0.0
    for p in positions:
        got = False
        v = p.get("unrealized_pl")
        if isinstance(v, (int, float, str)):
            try:
                unreal += float(v); got = True
            except Exception:
                pass
        if not got:
            plpc = p.get("unrealized_plpc")
            try:
                if plpc is not None:
                    plpc = float(plpc)
                    mv = float(p.get("market_value", 0.0))
                    cb = mv / (1.0 + plpc) if (1.0 + plpc) != 0 else 0.0
                    unreal += (mv - cb); got = True
            except Exception:
                pass
        if not got:
            try:
                mv = p.get("market_value"); cb = p.get("cost_basis")
                if mv is not None and cb is not None:
                    unreal += float(mv) - float(cb)
            except Exception:
                pass

    cutoff = today - timedelta(days=30)
    wins = losses = trades = 0
    for o in orders:
        if not _order_is_filled(o):
            continue
        dt = _order_dt(o)
        if not dt or dt.date() < cutoff:
            continue
        pnl = _realized_from_order(o)
        trades += 1
        if pnl > 1e-9: wins += 1
        elif pnl < -1e-9: losses += 1

    win_rate = (wins / trades) * 100.0 if trades > 0 else 0.0

    return {
        "version": APP_VERSION,
        "as_of_utc": now_utc.isoformat(),
        "realized": {
            "today": round(realized_today, 2),
            "week": round(realized_week, 2),
            "month": round(realized_month, 2),
        },
        "unrealized": round(unreal, 2),
        "trades": {
            "count_30d": trades,
            "wins_30d": wins,
            "losses_30d": losses,
            "win_rate_30d": round(win_rate, 2),
        },
    }

@app.get("/pnl/daily")
def pnl_daily():
    try:
        days = int(request.args.get("days", 180))
        days = max(1, min(370, days))
    except Exception:
        days = 180
    end_str = request.args.get("end")
    if end_str:
        try:
            end_date = datetime.fromisoformat(end_str).date()
        except Exception:
            end_date = datetime.now(timezone.utc).date()
    else:
        end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days - 1)

    broker, berr = _build_broker()
    if berr:
        return _err(berr, code=500)
    # Pull a healthy window of recent orders for the aggregation
    try:
        orders = broker.recent_orders(status="all", limit=1000)
    except Exception as e:
        return _err(str(e), code=500)

    daily = _aggregate_daily_realized(orders, start_date=start_date, end_date=end_date)
    series = [{"date": d, "pnl": round(v, 2)} for d, v in daily.items()]
    return _ok({"version": APP_VERSION, "start": start_date.isoformat(), "end": end_date.isoformat(), "days": days, "series": series})

@app.get("/pnl/summary")
def pnl_summary():
    broker, berr = _build_broker()
    if berr:
        return _err(berr, code=500)
    try:
        orders = broker.recent_orders(status="all", limit=1000)
        pos = broker.positions()
    except Exception as e:
        return _err(str(e), code=500)
    summary = _compute_summary(orders, pos)
    return _ok(summary)

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

    # Use saved defaults if symbols not provided explicitly
    raw_symbols = request.args.get("symbols")
    default_syms = _symbols_load()
    symbols = _parse_symbols(raw_symbols, default_syms)

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
    return _ok(out)

# --------------------------------
# Dashboard (full HTML) — scanner + NEW P&L + symbols editor
# --------------------------------

@app.get("/dashboard")
def dashboard():
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Crypto System Dashboard</title>
<style>
:root {{
  --bg: #0b0f14;
  --card: #111822;
  --muted: #8aa0b8;
  --text: #eaf2ff;
  --accent: #5eead4;
  --warn: #f59e0b;
  --err: #ef4444;
  --ok: #22c55e;
  --link: #93c5fd;
  --pos: #2bbf6a;
  --neg: #e24a4a;
  --flat: #384658;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0; padding: 24px; background: linear-gradient(180deg,#0b0f14,#0a1018 50%,#0b0f14);
  color: var(--text); font-family: Inter, ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, "Apple Color Emoji", "Segoe UI Emoji";
}}
h1 {{ margin: 0 0 12px 0; font-size: 24px; font-weight: 700; letter-spacing: .2px; }}
h2 {{ margin: 0 0 10px 0; font-size: 18px; font-weight: 700; }}
p.mono {{ color: var(--muted); font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace; font-size: 12px; }}
.card {{
  background: linear-gradient(180deg, #111822, #0e151f); border: 1px solid #1d2838;
  border-radius: 16px; padding: 16px; box-shadow: 0 10px 24px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.02);
}}
.grid {{ display: grid; gap: 16px; grid-template-columns: repeat(12, 1fr); }}
.col-4 {{ grid-column: span 4; }}
.col-8 {{ grid-column: span 8; }}
.col-12 {{ grid-column: span 12; }}
label {{ font-size: 12px; color: var(--muted); display: block; margin: 0 0 6px; }}
input, select, textarea {{
  width: 100%; background: #0a0f16; color: var(--text); border: 1px solid #1e293b; border-radius: 10px; padding: 10px 12px;
}}
.row4 {{ display: grid; gap: 12px; grid-template-columns: repeat(4, 1fr); }}
.row2 {{ display: grid; gap: 12px; grid-template-columns: repeat(2, 1fr); }}
button {{
  background: linear-gradient(180deg, #1f2937, #111827); border: 1px solid #2b374a; color: var(--text);
  padding: 10px 14px; border-radius: 10px; cursor: pointer; font-weight: 600;
}}
button.primary {{ background: linear-gradient(180deg, #0ea5e9, #0284c7); border: 1px solid #1e40af; }}
button.warn {{ background: linear-gradient(180deg, #f59e0b, #b45309); border: 1px solid #b45309; }}
button:disabled {{ opacity: .6; cursor: not-allowed; }}
pre {{
  background: #0a0f16; color: #cbd5e1; border: 1px solid #1e293b; border-radius: 10px; padding: 12px;
  white-space: pre-wrap; word-break: break-word; font-size: 12px;
}}
hr {{ border: 0; border-top: 1px solid #1f2937; margin: 16px 0; }}
.kv {{ display: grid; grid-template-columns: 140px 1fr; row-gap: 6px; }}
.kv div {{ padding: 3px 0; color: var(--muted); }}
.kv div b {{ color: var(--text); }}
a, a:visited {{ color: var(--link); text-decoration: none; }}
.badge {{ display: inline-block; padding: 3px 8px; border: 1px solid #2b374a; border-radius: 999px; background: #0a0f16; color: var(--muted); font-size: 11px; }}
.kpis {{ display:grid; grid-template-columns: repeat(5,1fr); gap:12px; }}
.kpi h3 {{ margin:0; font-size:12px; color: var(--muted); }}
.kpi .val {{ margin-top:6px; font-size:22px; font-weight:700; }}
.hm-grid {{ margin-top: 10px; display: grid; grid-template-columns: repeat(53, 12px); gap: 3px; }}
.hm-cell {{ width:12px; height:12px; border-radius:2px; background: var(--flat); }}
.hm-cell.pos {{ background: var(--pos); }}
.hm-cell.neg {{ background: var(--neg); }}
.hm-cell.flat {{ background: var(--flat); }}
</style>
</head>
<body>
  <div class="grid">

    <!-- LEFT: Scanner (existing) -->
    <div class="col-8">
      <div class="card">
        <h1>Crypto Scanner</h1>
        <p class="mono">Version: {APP_VERSION} &nbsp; • &nbsp; Exchange: Alpaca (paper-friendly)</p>
        <div class="row4">
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
            <input id="symbols" placeholder="BTC/USD,ETH/USD,SOL/USD,DOGE/USD" />
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
        <div class="row4" style="margin-top:12px">
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

    <!-- RIGHT: Health + Symbols -->
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
        <h3 style="margin:0 0 8px">Symbols</h3>
        <div class="row2">
          <div>
            <label>Default Symbols (cron uses this)</label>
            <textarea id="symbox" rows="8" placeholder="BTC/USD, ETH/USD, ..."></textarea>
          </div>
          <div>
            <button id="symLoad" style="margin-top:22px">Load</button>
            <button id="symSave" style="margin-top:8px">Save</button>
            <div id="symMsg" class="mono" style="margin-top:8px"></div>
            <a href="/config/symbols" target="_blank" class="mono">GET /config/symbols</a>
          </div>
        </div>
        <hr/>
        <a href="/routes" target="_blank">See all routes</a>
      </div>
    </div>

    <!-- FULL-WIDTH: P&L section -->
    <div class="col-12">
      <div class="card">
        <h2 style="margin:0 0 8px">P&amp;L Overview</h2>
        <div class="kpis">
          <div class="kpi"><h3>Realized Today</h3><div id="kpi-today" class="val">—</div></div>
          <div class="kpi"><h3>Realized Week</h3><div id="kpi-week" class="val">—</div></div>
          <div class="kpi"><h3>Realized Month</h3><div id="kpi-month" class="val">—</div></div>
          <div class="kpi"><h3>Unrealized P&amp;L</h3><div id="kpi-unreal" class="val">—</div></div>
          <div class="kpi">
            <h3>Win Rate / Trades (30d)</h3>
            <div class="val" id="kpi-winrate">—</div>
            <div class="mono" id="kpi-trades" style="font-size:12px; color:var(--muted)">—</div>
          </div>
        </div>
        <hr/>
        <div class="row2">
          <div>
            <label>Days</label>
            <input id="days" type="number" value="180" min="30" max="370"/>
          </div>
          <div style="display:flex; align-items:flex-end; gap:8px;">
            <button id="hmRefresh">Refresh Heatmap</button>
          </div>
        </div>
        <div id="hmRange" class="mono" style="margin-top:8px; color:var(--muted)">—</div>
        <div id="hm" class="hm-grid" title="Calendar heatmap"></div>
      </div>
    </div>

  </div>

<script>
async function jget(url) {{
  const r = await fetch(url, {{ method: "GET" }});
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}}
async function jpost(url) {{
  const r = await fetch(url, {{ method: "POST" }});
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}}
function qs(id){{ return document.getElementById(id); }}
function show(o){{ qs("out").innerHTML = "<pre>"+JSON.stringify(o, null, 2)+"</pre>"; }}

// Health
async function refreshHealth(){{
  try {{
    const hv = await jget("/health/versions");
    const h = qs("health");
    h.innerHTML = `
      <div>App</div><div><b>${{hv.app||"-"}}</b></div>
      <div>Data base</div><div><b>${{hv.data_base||"-"}}</b></div>
      <div>Trade base</div><div><b>${{hv.trading_base||"-"}}</b></div>
    `;
  }} catch(e) {{}}
}}
async function refreshGate(){{
  try {{
    const g = await jget("/diag/gate");
    const el = qs("gate");
    el.innerHTML = `
      <div>Status</div><div><b>${{g.decision||"-"}}</b></div>
      <div>Reason</div><div><b>${{g.reason||"-"}}</b></div>
      <div>TS</div><div><b>${{g.ts||"-"}}</b></div>
    `;
  }} catch(e) {{}}
}}

// Scanner
qs("runBtn").addEventListener("click", async () => {{
  const name = qs("strategy").value;
  const dry = qs("dry").value;
  const symbols = encodeURIComponent(qs("symbols").value);
  const tf = encodeURIComponent(qs("timeframe").value);
  const limit = encodeURIComponent(qs("limit").value);
  const notional = encodeURIComponent(qs("notional").value || "0");
  const xtra = qs("xtra").value.trim();
  let url = `/scan/${{name}}?dry=${{dry}}&symbols=${{symbols}}&timeframe=${{tf}}&limit=${{limit}}`;
  if (notional && notional !== "0") url += `&notional=${{notional}}`;
  if (xtra) url += `&${{xtra}}`;
  show({{ request: url }});
  const out = await jpost(url);
  show(out);
}});
qs("pingBtn").addEventListener("click", async () => {{
  const tf = encodeURIComponent(qs("timeframe").value || "5Min");
  const limit = encodeURIComponent(qs("limit").value || "3");
  const symbols = encodeURIComponent(qs("symbols").value);
  const url = `/diag/candles?symbols=${{symbols}}&limit=${{limit}}&tf=${{tf}}`;
  show({{ request: url }});
  const out = await jget(url);
  show(out);
}});

// Symbols editor
async function loadSyms() {{
  const d = await jget("/config/symbols");
  qs("symbox").value = (d.symbols || []).join(", ");
  qs("symMsg").textContent = "Loaded " + (d.symbols?.length || 0) + " symbols.";
}}
async function saveSyms() {{
  const raw = qs("symbox").value || "";
  const list = raw.split(",").map(s => s.trim()).filter(Boolean);
  const r = await fetch("/config/symbols", {{
    method: "POST", headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ symbols: list }})
  }});
  const d = await r.json();
  if (r.ok) {{
    qs("symMsg").textContent = "Saved " + (d.count || 0) + " symbols.";
  }} else {{
    qs("symMsg").textContent = "Error: " + (d.error || "unknown");
  }}
}}
qs("symLoad").addEventListener("click", loadSyms);
qs("symSave").addEventListener("click", saveSyms);

// KPI cards + Heatmap
const fmtMoney = (n) => {{
  if (n === null || n === undefined || isNaN(n)) return "—";
  const sign = n < 0 ? "-" : "";
  const v = Math.abs(n);
  return sign + "$" + v.toLocaleString(undefined, {{maximumFractionDigits: 2}});
}};
function repaintKPIs(s) {{
  qs("kpi-today").textContent  = fmtMoney(s?.realized?.today);
  qs("kpi-week").textContent   = fmtMoney(s?.realized?.week);
  qs("kpi-month").textContent  = fmtMoney(s?.realized?.month);
  qs("kpi-unreal").textContent = fmtMoney(s?.unrealized ?? 0);
  const wr = s?.trades?.win_rate_30d ?? 0;
  qs("kpi-winrate").textContent = (isNaN(wr) ? "—" : wr.toFixed(2) + "%");
  const t = s?.trades?.count_30d ?? 0;
  const w = s?.trades?.wins_30d ?? 0;
  const l = s?.trades?.losses_30d ?? 0;
  qs("kpi-trades").textContent = `${{t}} trades · ${{w}}W/${{l}}L (30d)`;
}}
function colorFor(v) {{
  if (v > 0.00001) return "pos";
  if (v < -0.00001) return "neg";
  return "flat";
}}
function buildHeatmap(series) {{
  const hm = qs("hm");
  hm.innerHTML = "";
  for (const dp of series) {{
    const cell = document.createElement("div");
    cell.className = "hm-cell " + colorFor(dp.pnl);
    cell.title = `${{dp.date}} — $${{dp.pnl.toFixed(2)}}`;
    hm.appendChild(cell);
  }}
}}
async function refreshSummary() {{
  const s = await jget("/pnl/summary");
  repaintKPIs(s);
}}
async function refreshHeatmap() {{
  const days = Math.max(30, Math.min(370, parseInt(qs("days").value || "180")));
  const d = await jget(`/pnl/daily?days=${{days}}`);
  qs("hmRange").textContent = `${{d.start}} to ${{d.end}} · ${{d.days}} days`;
  buildHeatmap(d.series);
}}
qs("hmRefresh").addEventListener("click", refreshHeatmap);

// Init
(async function init() {{
  try {{
    await Promise.all([refreshHealth(), refreshGate(), loadSyms(), refreshSummary(), refreshHeatmap()]);
  }} catch (e) {{
    console.error(e);
  }}
}})();
</script>
</body>
</html>
"""
    return Response(html, mimetype="text/html")

# Static (Flask serves /static automatically; explicit route for clarity)
@app.get("/static/<path:filename>")
def serve_static(filename):
    return send_from_directory(app.static_folder, filename)

# --------------------------------
# Main
# --------------------------------

if __name__ == "__main__":
    # Bind 0.0.0.0 for Render
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
