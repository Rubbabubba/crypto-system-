# app.py — Crypto System API + Dashboard
# Version: 1.8.1
# - NEW: /pnl/summary (KPI cards) and /pnl/daily (calendar heatmap)
# - NEW: /orders/attribution and /pnl/trades with strategy inference
# - NEW: /config/symbols (GET/POST) persisted to symbols.json
# - /scan/<name> now passes client_tag so orders can set client_order_id
# - Dashboard: KPI cards, daily heatmap, per-strategy P&L, symbols load/save
# - Backward compatible with existing MarketCrypto / ExchangeExec services

import os
import json
import math
import traceback
from datetime import datetime, timezone, timedelta, date
from typing import Dict, Any, List, Tuple

from flask import Flask, request, jsonify, redirect, Response

# Optional: load .env if present (local runs)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

APP_VERSION = "1.8.1"

# ---- Imports for your services ----
# MarketCrypto must offer: from_env(), candles(symbols, timeframe, limit) -> Dict[str, DataFrame]
# ExchangeExec must offer: from_env(), paper_buy/sell/notional, recent_orders(status,limit), positions()
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

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _now_utc_iso() -> str:
    return _now_utc().isoformat()

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

# ---------------------------
# Symbols config (persisted)
# ---------------------------

DEFAULT_SYMBOLS_ENV = os.getenv("DEFAULT_SYMBOLS", "")
DEFAULT_SYMBOLS = (
    [s.strip() for s in DEFAULT_SYMBOLS_ENV.split(",") if s.strip()]
    or ["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD"]
)
SYMBOLS_PATH = os.getenv("SYMBOLS_PATH", "./data/symbols.json")

def _ensure_dir_for(path: str):
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)

def _load_symbols_file() -> List[str]:
    try:
        with open(SYMBOLS_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict) and "symbols" in data and isinstance(data["symbols"], list):
                return [str(x) for x in data["symbols"]]
            if isinstance(data, list):
                return [str(x) for x in data]
    except Exception:
        pass
    return DEFAULT_SYMBOLS

def _save_symbols_file(symbols: List[str]) -> None:
    _ensure_dir_for(SYMBOLS_PATH)
    with open(SYMBOLS_PATH, "w", encoding="utf-8") as f:
        json.dump({"symbols": symbols}, f, ensure_ascii=False)

def _current_symbols() -> List[str]:
    return _load_symbols_file()

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
# Config: symbols
# --------------------------------

@app.get("/config/symbols")
def get_config_symbols():
    syms = _current_symbols()
    return _ok({"ok": True, "version": APP_VERSION, "count": len(syms), "symbols": syms})

@app.post("/config/symbols")
def post_config_symbols():
    try:
        body = request.get_json(force=True, silent=True) or {}
        symbols = body.get("symbols") or []
        if not isinstance(symbols, list):
            return _err("symbols must be a list")
        # normalize/unique, preserve order
        seen = set()
        cleaned: List[str] = []
        for s in symbols:
            si = str(s).strip()
            if not si or si in seen:
                continue
            seen.add(si)
            cleaned.append(si)
        _save_symbols_file(cleaned)
        return _ok({"ok": True, "version": APP_VERSION, "count": len(cleaned), "symbols": cleaned})
    except Exception as e:
        return _err(str(e))

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
            "symbols": _current_symbols()
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
            "symbols": _current_symbols()
        })
    return _ok({
        "ok": True,
        "exchange": "alpaca",
        "data_base": base,
        "trading_base": tbase,
        "api_key_present": True,
        "account_sample": account_sample,
        "symbols": _current_symbols()
    })

@app.get("/diag/candles")
def diag_candles():
    symbols = _parse_symbols(request.args.get("symbols"), _current_symbols())
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
        attempts.append(f"{getattr(market, 'data_base', '')}/v1beta3/crypto/us/bars?symbols={','.join([s.replace('/', '%2F') for s in symbols])}&timeframe={tf}&limit={limit}")
        data = market.candles(symbols, timeframe=tf, limit=limit)
        for s in symbols:
            df = data.get(s)
            rows[s] = int(getattr(df, "shape", [0, 0])[0]) if df is not None else 0
        last_url = attempts[-1]
    except Exception as e:
        last_error = str(e)
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
# Strategy runner (C1–C6) + client_tag propagation
# --------------------------------

def _run_strategy_inline(name: str, dry: bool, symbols: List[str], params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expected module: strategies.c{name} with run(market, broker, symbols, params, *, dry, log)
    Returns: dict with "results":[...]
    """
    try:
        mod = __import__(f"strategies.{name}", fromlist=["*"])
    except Exception as e:
        return {"ok": False, "strategy": name, "error": f"import error: {e}"}

    market, merr = _build_market()
    if merr:
        return {"ok": False, "strategy": name, "error": merr}

    broker, berr = _build_broker()
    if berr and not dry:
        return {"ok": False, "strategy": name, "error": berr}

    def pwrite(msg: str):
        print(msg, flush=True)
    def log(**kw):
        pwrite(json.dumps(kw))

    try:
        out = mod.run(market, broker, symbols, params, dry=dry, log=log)
        if not isinstance(out, dict):
            return {"ok": False, "strategy": name, "error": "strategy returned non-dict"}
        out.setdefault("ok", True)
        out.setdefault("strategy", name)
        out.setdefault("dry", dry)
        return out
    except TypeError as te:
        return {"ok": False, "strategy": name, "error": f"{te}"}
    except Exception as e:
        return {"ok": False, "strategy": name, "error": f"{e}", "trace": traceback.format_exc(limit=3)}

@app.post("/scan/<name>")
def scan_named(name: str):
    if name not in {"c1", "c2", "c3", "c4", "c5", "c6"}:
        return _err(f"unknown strategy: {name}", code=404)

    dry = _bool_arg("dry", True)
    symbols = _parse_symbols(request.args.get("symbols"), _current_symbols())

    # common params
    params: Dict[str, Any] = {
        "timeframe": _str_arg("timeframe", "5Min"),
        "limit": _int_arg("limit", 600),
        "notional": _float_arg("notional", 0.0),  # live only
        # >>> IMPORTANT for attribution: pass client_tag
        "client_tag": _str_arg("client_tag", name)
    }
    # pass extra query params through into params
    for k, v in request.args.items():
        if k in {"dry", "symbols", "timeframe", "limit", "notional", "client_tag"}:
            continue
        params[k] = v

    # Strategies should forward params["client_tag"] into broker.submit_order()
    # as client_order_id=f"{client_tag}-{symbol}-{int(time.time())}" for perfect attribution.
    out = _run_strategy_inline(name, dry=dry, symbols=symbols, params=params)
    return _ok(out)

# --------------------------------
# P&L utilities (normalize orders, compute realized/unrealized)
# --------------------------------

def _parse_dt(any_ts) -> datetime | None:
    if not any_ts:
        return None
    try:
        # Accept both with and without tz
        dt = datetime.fromisoformat(str(any_ts).replace("Z", "+00:00"))
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        try:
            return datetime.strptime(str(any_ts), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
        except Exception:
            return None

def _get_filled_time(o: Dict[str, Any]) -> datetime | None:
    for k in ("filled_at", "completed_at", "submitted_at", "created_at", "timestamp"):
        dt = _parse_dt(o.get(k))
        if dt:
            return dt
    return None

def _get_realized(o: Dict[str, Any]) -> float:
    for k in ("realized_pnl", "pnl", "realizedPnL", "realized"):
        v = o.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    return 0.0

def _is_filled(o: Dict[str, Any]) -> bool:
    s = str(o.get("status", "")).lower()
    if s in {"filled", "closed", "done", "complete", "executed"}:
        return True
    return o.get("filled_at") is not None

def _infer_strategy_from_coid(coid: str | None) -> str:
    if not coid:
        return "unknown"
    cid = str(coid).lower()
    # Patterns like c1-..., c2_..., c3..., allow hyphen/underscore separators
    if cid.startswith("c1"): return "c1"
    if cid.startswith("c2"): return "c2"
    if cid.startswith("c3"): return "c3"
    if cid.startswith("c4"): return "c4"
    if cid.startswith("c5"): return "c5"
    if cid.startswith("c6"): return "c6"
    return "unknown"

def _normalize_orders(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for o in items:
        if not isinstance(o, dict):
            continue
        if not _is_filled(o):
            continue
        t = _get_filled_time(o)
        if not t:
            continue
        sym = o.get("symbol") or o.get("asset") or ""
        side = (o.get("side") or "").lower()
        qty = o.get("qty") or o.get("quantity") or 0
        price = o.get("filled_avg_price") or o.get("price") or o.get("avg_execution_price") or None
        pnl = _get_realized(o)
        coid = o.get("client_order_id") or o.get("clientOrderId") or ""
        strat = _infer_strategy_from_coid(coid)
        out.append({
            "time": t.isoformat(),
            "symbol": sym,
            "side": side,
            "qty": qty,
            "price": price,
            "realized_pnl": pnl,
            "client_order_id": coid,
            "strategy": strat,
            "id": o.get("id") or o.get("order_id") or ""
        })
    # Sort asc by time
    out.sort(key=lambda r: r["time"])
    return out

def _fetch_orders(limit: int = 1000) -> List[Dict[str, Any]]:
    broker, berr = _build_broker()
    if berr:
        raise RuntimeError(berr)
    items = broker.recent_orders(status="all", limit=limit)
    if isinstance(items, dict) and "items" in items:
        items = items["items"]  # tolerate paginated shapes
    if not isinstance(items, list):
        raise RuntimeError("unexpected orders shape")
    return items

def _fetch_positions() -> List[Dict[str, Any]]:
    broker, berr = _build_broker()
    if berr:
        raise RuntimeError(berr)
    items = broker.positions()
    if not isinstance(items, list):
        return []
    return items

def _unrealized_from_positions(positions: List[Dict[str, Any]]) -> float:
    tot = 0.0
    for p in positions:
        for k in ("unrealized_pl", "unrealized_pnl", "unrealized", "unrealized_plpc_value"):
            v = p.get(k)
            if v is not None:
                try: 
                    tot += float(v)
                    break
                except Exception:
                    pass
    return float(round(tot, 2))

# --------------------------------
# P&L endpoints
# --------------------------------

@app.get("/pnl/summary")
def pnl_summary():
    try:
        orders = _normalize_orders(_fetch_orders(limit=_int_arg("limit", 1000)))
        pos = _fetch_positions()
    except Exception as e:
        return _err(str(e))

    now = _now_utc()
    today = date.fromtimestamp(now.timestamp())
    # Week: Monday..today (UTC)
    week_start = (today - timedelta(days=today.weekday()))
    month_start = today.replace(day=1)

    def realized_in_range(start_d: date, end_d: date | None = None) -> float:
        sdt = datetime.combine(start_d, datetime.min.time(), tzinfo=timezone.utc)
        edt = (datetime.combine(end_d, datetime.max.time(), tzinfo=timezone.utc) if end_d
               else datetime.combine(today, datetime.max.time(), tzinfo=timezone.utc))
        tot = 0.0
        for o in orders:
            t = _parse_dt(o["time"])
            if not t:
                continue
            if sdt <= t <= edt:
                tot += float(o["realized_pnl"])
        return float(round(tot, 2))

    realized_today = realized_in_range(today)
    realized_week = realized_in_range(week_start)
    realized_month = realized_in_range(month_start)

    # Win rate / count (last 30d)
    since = now - timedelta(days=30)
    trades_30 = [o for o in orders if (_parse_dt(o["time"]) or now) >= since]
    wins = sum(1 for o in trades_30 if float(o["realized_pnl"]) > 0)
    losses = sum(1 for o in trades_30 if float(o["realized_pnl"]) < 0)
    count_30 = len(trades_30)
    wr = float(round((wins / count_30 * 100.0), 2)) if count_30 else 0.0

    out = {
        "version": APP_VERSION,
        "as_of_utc": _now_utc_iso(),
        "realized": {"today": realized_today, "week": realized_week, "month": realized_month},
        "unrealized": _unrealized_from_positions(pos),
        "trades": {"count_30d": count_30, "wins_30d": wins, "losses_30d": losses, "win_rate_30d": wr},
    }
    return _ok(out)

@app.get("/pnl/daily")
def pnl_daily():
    days = _int_arg("days", 30)
    days = max(1, min(days, 365))
    try:
        orders = _normalize_orders(_fetch_orders(limit=_int_arg("limit", 2000)))
    except Exception as e:
        return _err(str(e))

    # Aggregate by UTC date
    agg: Dict[str, float] = {}
    for o in orders:
        t = _parse_dt(o["time"])
        if not t:
            continue
        d = t.date().isoformat()
        agg[d] = agg.get(d, 0.0) + float(o["realized_pnl"])

    # Build series for the last N days (including today)
    end = _now_utc().date()
    start = end - timedelta(days=days - 1)
    series = []
    cur = start
    while cur <= end:
        s = cur.isoformat()
        series.append({"date": s, "pnl": float(round(agg.get(s, 0.0), 2))})
        cur += timedelta(days=1)

    return _ok({"version": APP_VERSION, "start": start.isoformat(), "end": end.isoformat(), "days": days, "series": series})

@app.get("/orders/attribution")
def orders_attribution():
    days = _int_arg("days", 2)
    limit = _int_arg("limit", 2000)
    try:
        orders = _normalize_orders(_fetch_orders(limit=limit))
    except Exception as e:
        return _err(str(e))

    since = _now_utc() - timedelta(days=days)
    orders = [o for o in orders if (_parse_dt(o["time"]) or _now_utc()) >= since]

    # by-strategy totals
    per = {}
    for o in orders:
        st = o["strategy"]
        d = per.setdefault(st, {"count": 0, "wins": 0, "losses": 0, "realized": 0.0})
        d["count"] += 1
        pnl = float(o["realized_pnl"])
        if pnl > 0: d["wins"] += 1
        if pnl < 0: d["losses"] += 1
        d["realized"] = float(round(d["realized"] + pnl, 2))

    overall = {"count": sum(d["count"] for d in per.values()),
               "wins": sum(d["wins"] for d in per.values()),
               "losses": sum(d["losses"] for d in per.values()),
               "realized": float(round(sum(d["realized"] for d in per.values()), 2))}
    return _ok({"version": APP_VERSION, "days": days, "overall": overall, "per_strategy": per, "orders": orders})

@app.get("/pnl/trades")
def pnl_trades():
    days = _int_arg("days", 2)
    limit = _int_arg("limit", 2000)
    try:
        orders = _normalize_orders(_fetch_orders(limit=limit))
    except Exception as e:
        return _err(str(e))
    since = _now_utc() - timedelta(days=days)
    orders = [o for o in orders if (_parse_dt(o["time"]) or _now_utc()) >= since]
    return _ok({"version": APP_VERSION, "days": days, "trades": orders})

# --------------------------------
# Dashboard (full HTML) — adds KPI cards, heatmap, per-strategy P&L, symbols save/load
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
  --grey: #334155;
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
.col-12 { grid-column: span 12; }
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

/* KPI cards */
.kpis { display: grid; gap: 12px; grid-template-columns: repeat(5, 1fr); margin-top: 12px; }
.kpi { background: #0a0f16; border: 1px solid #1e293b; border-radius: 12px; padding: 12px; }
.kpi .v { font-size: 18px; font-weight: 700; }
.kpi small { color: var(--muted); }

/* Heatmap */
.hm { display: grid; grid-template-columns: repeat(15, 1fr); gap: 4px; }
.cell { width: 100%; padding-top: 100%; position: relative; border-radius: 4px; border: 1px solid #1f2937; }
.cell > span { position: absolute; inset: 0; font-size: 0; }

/* Strategy table */
table { width: 100%; border-collapse: collapse; font-size: 12px; }
th, td { text-align: right; padding: 8px; border-bottom: 1px solid #1f2937; }
th:first-child, td:first-child { text-align: left; }
.pos { color: var(--ok); } .neg { color: var(--err); } .muted { color: var(--muted); }
</style>
</head>
<body>
  <div class="grid">
    <div class="col-8">
      <div class="card">
        <h1>Crypto Scanner</h1>
        <p class="mono">Version: 1.8.1 &nbsp; • &nbsp; Exchange: Alpaca (paper-friendly)</p>
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
            <label>Symbols</label>
            <input id="symbols" value="" placeholder="Loaded from /config/symbols…" />
            <button id="saveSyms" style="margin-top:6px;width:100%">Save Symbols</button>
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
        <h3 style="margin:4px 0 8px">P&L Overview</h3>
        <div class="kpis" id="kpis">
          <div class="kpi"><div class="v">—</div><small>Realized Today</small></div>
          <div class="kpi"><div class="v">—</div><small>Realized Week</small></div>
          <div class="kpi"><div class="v">—</div><small>Realized Month</small></div>
          <div class="kpi"><div class="v">—</div><small>Unrealized</small></div>
          <div class="kpi"><div class="v">—</div><small>Win rate (30d)</small></div>
        </div>
        <hr/>
        <h3 style="margin:4px 0 8px">Daily P&L (30d)</h3>
        <div id="heatmap" class="hm"></div>
        <hr/>
        <h3 style="margin:4px 0 8px">P&L by Strategy (last 2d)</h3>
        <table id="stratTbl">
          <thead><tr><th>Strategy</th><th>Trades</th><th>Wins</th><th>Losses</th><th>Realized</th></tr></thead>
          <tbody><tr><td colspan="5" class="muted">Loading…</td></tr></tbody>
        </table>
        <hr/>
        <a href="/routes" target="_blank">See all routes</a>
      </div>
    </div>
  </div>

<script>
async function jget(url){ const r = await fetch(url, { method:"GET" }); return await r.json(); }
async function jpost(url, body){ const r = await fetch(url, { method:"POST", headers:{"Content-Type":"application/json"}, body: body ? JSON.stringify(body) : null }); return await r.json(); }
function qs(id){ return document.getElementById(id); }
function show(o){ qs("out").innerHTML = "<pre>"+JSON.stringify(o, null, 2)+"</pre>"; }
function fmt(v){ const n = Number(v||0); const s = n.toFixed(2); return (n>0? "<span class='pos'>+"+s+"</span>" : n<0? "<span class='neg'>"+s+"</span>" : "<span class='muted'>"+s+"</span>"); }
function pct(v){ return (Number(v||0)).toFixed(2) + "%"; }

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

async function refreshKpis(){
  try {
    const s = await jget("/pnl/summary");
    const k = qs("kpis");
    k.innerHTML = "";
    const cards = [
      {label:"Realized Today", v: fmt(s.realized?.today)},
      {label:"Realized Week", v: fmt(s.realized?.week)},
      {label:"Realized Month", v: fmt(s.realized?.month)},
      {label:"Unrealized", v: fmt(s.unrealized)},
      {label:"Win rate (30d)", v: (s.trades?.count_30d? pct(s.trades?.win_rate_30d): "<span class='muted'>0.00%</span>")}
    ];
    for (const c of cards) {
      const d = document.createElement("div");
      d.className = "kpi";
      d.innerHTML = `<div class="v">${c.v}</div><small>${c.label}</small>`;
      k.appendChild(d);
    }
  } catch(e) {}
}

function cellColor(v){
  if (v === 0) return "var(--grey)";
  if (v > 0) return "var(--ok)";
  return "var(--err)";
}
async function refreshHeatmap(){
  try {
    const d = await jget("/pnl/daily?days=30");
    const hm = qs("heatmap");
    hm.innerHTML = "";
    (d.series||[]).forEach(x => {
      const c = document.createElement("div");
      c.className = "cell";
      const bg = cellColor(Number(x.pnl||0));
      c.style.background = bg;
      c.title = `${x.date}: ${Number(x.pnl||0).toFixed(2)}`;
      c.innerHTML = "<span></span>";
      hm.appendChild(c);
    });
  } catch(e) {}
}

async function refreshStrategies(){
  try {
    const a = await jget("/orders/attribution?days=2");
    const tb = qs("stratTbl").querySelector("tbody");
    tb.innerHTML = "";
    const per = a.per_strategy || {};
    const keys = Object.keys(per).sort();
    if (keys.length === 0) {
      tb.innerHTML = `<tr><td colspan="5" class="muted">No filled trades in window.</td></tr>`;
      return;
    }
    for (const k of keys) {
      const r = per[k];
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${k}</td><td>${r.count||0}</td><td>${r.wins||0}</td><td>${r.losses||0}</td><td>${fmt(r.realized||0)}</td>`;
      tb.appendChild(tr);
    }
    // Overall row
    const o = a.overall || {};
    const tr = document.createElement("tr");
    tr.innerHTML = `<td><b>Total</b></td><td><b>${o.count||0}</b></td><td><b>${o.wins||0}</b></td><td><b>${o.losses||0}</b></td><td><b>${fmt(o.realized||0)}</b></td>`;
    tb.appendChild(tr);
  } catch(e) {}
}

async function loadSymbolsIntoInput(){
  try {
    const s = await jget("/config/symbols");
    qs("symbols").value = (s.symbols||[]).join(",");
  } catch(e) {
    qs("symbols").value = "BTC/USD,ETH/USD,SOL/USD,DOGE/USD";
  }
}

qs("saveSyms").addEventListener("click", async () => {
  const raw = qs("symbols").value;
  const syms = raw.split(",").map(s=>s.trim()).filter(Boolean);
  const res = await jpost("/config/symbols", {symbols: syms});
  alert(`Saved ${res.count||0} symbols`);
});

qs("runBtn").addEventListener("click", async () => {
  const name = qs("strategy").value;
  const dry = qs("dry").value;
  const symbols = encodeURIComponent(qs("symbols").value);
  const tf = encodeURIComponent(qs("timeframe").value);
  const limit = encodeURIComponent(qs("limit").value);
  const notional = encodeURIComponent(qs("notional").value || "0");
  const xtra = qs("xtra").value.trim();
  let url = `/scan/${name}?dry=${dry}&symbols=${symbols}&timeframe=${tf}&limit=${limit}&client_tag=${name}`;
  if (notional && notional !== "0") url += `&notional=${notional}`;
  if (xtra) url += `&${xtra}`;
  show({ request: url });
  const out = await jpost(url);
  show(out);
  // Light refresh after scans
  refreshKpis(); refreshHeatmap(); refreshStrategies();
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

(async function init(){
  await loadSymbolsIntoInput();
  await refreshHealth();
  await refreshKpis();
  await refreshHeatmap();
  await refreshStrategies();
})();
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
