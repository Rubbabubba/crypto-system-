import os
import json
import math
import time
import traceback
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque

import numpy as np
import pandas as pd
from flask import Flask, request, jsonify, Response

# ---- Optional: reduce pandas chained-assign warnings
pd.options.mode.chained_assignment = None  # be tame

# ==== Broker wrapper (your module) ====
# Expecting functions (any subset; all calls are guarded):
# - get_account()
# - get_positions()
# - get_orders(status:str="all", limit:int=200)  OR list_orders(...)
# - place_order(symbol, side, notional=None, qty=None, client_order_id=None)
# - get_candles(symbol, timeframe, limit)
# - get_calendar(days:int=30)
#
# If your broker exposes slightly different names, the safe_* helpers below
# will try alternatives and default to empty results instead of crashing.
try:
    import broker
except Exception:
    broker = None  # App still runs for the dashboard shell / diagnostics


# ======================================
# Flask app
# ======================================
app = Flask(__name__)
INCLUDE_TRACE = os.getenv("DEBUG_TRACE_ERRORS", "0") == "1"

# ---------- JSON error handler ----------
@app.errorhandler(Exception)
def handle_any_error(e):
    if INCLUDE_TRACE:
        return jsonify({"ok": False, "error": str(e), "traceback": traceback.format_exc()}), 500
    return ("", 500)


# ======================================
# Helpers
# ======================================

def utcnow():
    return datetime.now(timezone.utc)

def as_utc(ts):
    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return utcnow()
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    if isinstance(ts, datetime):
        return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    return utcnow()

def safe_account():
    if not broker:
        return {}
    for fn in ("get_account", "account", "getAccount"):
        f = getattr(broker, fn, None)
        if f:
            try:
                acc = f()
                return acc or {}
            except Exception:
                return {}
    return {}

def safe_positions():
    if not broker:
        return []
    for fn in ("get_positions", "positions", "list_positions"):
        f = getattr(broker, fn, None)
        if f:
            try:
                pos = f()
                if isinstance(pos, dict) and "positions" in pos:
                    return pos["positions"]
                return pos or []
            except Exception:
                return []
    return []

def safe_orders(status="all", limit=200):
    if not broker:
        return []
    # Try common broker wrapper names
    for name in ("get_orders", "orders", "list_orders", "getOrders"):
        f = getattr(broker, name, None)
        if f:
            try:
                return f(status=status, limit=limit)  # preferred signature
            except TypeError:
                try:
                    return f(status, limit)  # positional fallback
                except Exception:
                    pass
            except Exception:
                pass
    return []

def safe_calendar(days=30):
    if not broker:
        return []
    for fn in ("get_calendar", "calendar", "getCalendar"):
        f = getattr(broker, fn, None)
        if f:
            try:
                return f(days=days)
            except TypeError:
                try:
                    return f(days)
                except Exception:
                    pass
            except Exception:
                pass
    return []

def safe_place_order(**kwargs):
    if not broker:
        return {"ok": False, "error": "no_broker"}
    for fn in ("place_order", "submit_order", "order"):
        f = getattr(broker, fn, None)
        if f:
            try:
                return f(**kwargs) or {}
            except Exception as e:
                return {"ok": False, "error": str(e)}
    return {"ok": False, "error": "no_place_order_method"}

def safe_candles(symbol, timeframe, limit):
    if not broker:
        return pd.DataFrame()
    for fn in ("get_candles", "candles", "getBars", "get_bars"):
        f = getattr(broker, fn, None)
        if f:
            try:
                df = f(symbol, timeframe, limit)
                # Accept dict-like or list; convert to DataFrame
                if isinstance(df, dict):
                    df = pd.DataFrame(df)
                if not isinstance(df, pd.DataFrame):
                    df = pd.DataFrame(df or [])
                return df
            except Exception:
                return pd.DataFrame()
    return pd.DataFrame()

def normalize_candles(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    Normalize broker candle schema into: ts, open, high, low, close, volume, vwap
    Fill essential columns; ffill/bfill gaps (avoid deprecated fillna(method=...)).
    """
    if df is None or len(df) == 0:
        return None

    # Standardize column names to lower
    cols_lower = {c.lower(): c for c in df.columns}
    df = df.rename(columns={orig: orig.lower() for orig in df.columns})

    # Map common short names
    mapping = {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "vw": "vwap"}
    for k, v in mapping.items():
        if k in df.columns and v not in df.columns:
            df = df.rename(columns={k: v})

    # Time column
    t_candidates = ["ts", "t", "timestamp", "time"]
    tcol = next((c for c in t_candidates if c in df.columns), None)
    if not tcol:
        # create synthetic monotonic timestamps if missing
        df["ts"] = pd.date_range(end=utcnow(), periods=len(df), freq="min")
    else:
        df["ts"] = pd.to_datetime(df[tcol], utc=True, errors="coerce")
        df.drop(columns=[c for c in t_candidates if c in df.columns and c != "ts"], inplace=True, errors="ignore")

    # Ensure essential columns exist
    for c in ["open", "high", "low", "close", "volume", "vwap"]:
        if c not in df.columns:
            df[c] = np.nan

    # Coerce numeric
    for c in ["open", "high", "low", "close", "volume", "vwap"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # Approximate vwap if missing
    if df["vwap"].isna().all():
        tp = (df["high"] + df["low"] + df["close"]) / 3.0
        df["vwap"] = tp

    # FFill/BFill to clean small holes; don't manufacture long histories though
    df[["open", "high", "low", "close", "volume", "vwap"]] = \
        df[["open", "high", "low", "close", "volume", "vwap"]].ffill().bfill()

    # Drop totally NA rows on essential price columns
    df = df.dropna(subset=["close"])

    # Sort by time just in case
    df = df.sort_values("ts").reset_index(drop=True)
    return df if len(df) else None


def parse_strategy_from_client_id(cid: str | None) -> str:
    """Try to read 'c1', 'c2', ... prefix from client_order_id."""
    if not cid:
        return "unknown"
    cid_low = cid.lower()
    for name in ("c1", "c2", "c3", "c4", "c5", "c6"):
        if cid_low.startswith(name):
            return name
    # sometimes "c1-" or "c1_" appears not at position 0
    for name in ("c1", "c2", "c3", "c4", "c5", "c6"):
        if cid_low.find(name + "-") >= 0 or cid_low.find(name + "_") >= 0:
            return name
    return "unknown"


def extract_fills(orders: list[dict], since_days: int = 30) -> list[dict]:
    """
    Convert orders -> normalized fills (time, symbol, side, qty, price, client_id).
    Only include filled/partially_filled; filter by recent days.
    """
    cutoff = utcnow() - timedelta(days=since_days)
    fills = []
    for o in orders or []:
        status = (o.get("status") or "").lower()
        if status not in ("filled", "partially_filled", "done_for_day", "closed"):
            continue

        filled_qty = o.get("filled_qty") or o.get("qty") or o.get("quantity") or "0"
        try:
            qty = float(filled_qty)
        except Exception:
            # Some APIs return crypto qty as string decimal
            try:
                qty = float(str(filled_qty))
            except Exception:
                qty = 0.0
        if qty <= 0:
            continue

        price = o.get("filled_avg_price") or o.get("avg_fill_price") or o.get("price") or o.get("limit_price")
        try:
            price = float(price)
        except Exception:
            price = None

        sym = o.get("symbol") or o.get("asset_symbol") or ""
        side = (o.get("side") or "").lower()
        cid = o.get("client_order_id") or o.get("client_orderid") or o.get("client_id")

        t = o.get("filled_at") or o.get("updated_at") or o.get("submitted_at") or o.get("timestamp")
        t = as_utc(t)

        if t < cutoff:
            continue

        fills.append({
            "time": t,
            "symbol": sym,
            "side": side,
            "qty": qty,
            "price": price,
            "client_id": cid,
            "strategy": parse_strategy_from_client_id(cid),
        })
    # Sort fills chronologically
    fills.sort(key=lambda x: x["time"])
    return fills


def fifo_realized_pnl(fills: list[dict]) -> dict:
    """
    Naive FIFO realized P&L per symbol & overall.
    Assumes long-only (buys add, sells reduce). Ignores fees.
    """
    by_symbol = defaultdict(lambda: {"pos_qty": 0.0, "layers": deque(), "realized": 0.0})
    for f in fills:
        sym = f["symbol"]
        qty = f["qty"]
        price = f["price"] or 0.0
        side = f["side"]
        book = by_symbol[sym]

        if side == "buy":
            # add a cost layer
            book["layers"].append({"qty": qty, "price": price})
            book["pos_qty"] += qty
        elif side == "sell":
            remain = qty
            while remain > 0 and book["layers"]:
                layer = book["layers"][0]
                take = min(layer["qty"], remain)
                pnl = (price - layer["price"]) * take
                book["realized"] += pnl
                layer["qty"] -= take
                remain -= take
                if layer["qty"] <= 1e-12:
                    book["layers"].popleft()
            book["pos_qty"] = max(0.0, book["pos_qty"] - qty)

    total = sum(v["realized"] for v in by_symbol.values())
    return {"by_symbol": by_symbol, "total": total}


def pnl_by_strategy(fills: list[dict]) -> dict:
    """
    Group realized P&L and counts by strategy (from client_order_id prefix).
    """
    # Compute realized PnL at symbol level first (FIFO), then attribute by strategy
    # For strategy attribution, we’ll simply bucket each SELL’s realized amount to its strategy tag.
    # If buys/sells mix between strategies, this stays approximate (common in lightweight systems).
    strat_stats = defaultdict(lambda: {"realized": 0.0, "buys": 0, "sells": 0, "orders": 0})
    # Build running book per symbol to compute realized per SELL event
    books = defaultdict(lambda: deque())
    for f in fills:
        sym = f["symbol"]
        price = f["price"] or 0.0
        qty = f["qty"]
        strat = f["strategy"]
        side = f["side"]
        if side == "buy":
            strat_stats[strat]["orders"] += 1
            strat_stats[strat]["buys"] += 1
            books[sym].append({"qty": qty, "price": price})
        elif side == "sell":
            strat_stats[strat]["orders"] += 1
            strat_stats[strat]["sells"] += 1
            remain = qty
            realized = 0.0
            while remain > 0 and books[sym]:
                layer = books[sym][0]
                take = min(layer["qty"], remain)
                realized += (price - layer["price"]) * take
                layer["qty"] -= take
                remain -= take
                if layer["qty"] <= 1e-12:
                    books[sym].popleft()
            strat_stats[strat]["realized"] += realized

    # Ensure all C1..C6 appear
    for name in ("c1", "c2", "c3", "c4", "c5", "c6"):
        strat_stats.setdefault(name, {"realized": 0.0, "buys": 0, "sells": 0, "orders": 0})

    total = sum(v["realized"] for v in strat_stats.values())
    return {"by_strategy": strat_stats, "total": total}


def get_env_list(name: str, default_csv: str) -> list[str]:
    env_val = os.getenv(name, default_csv)
    return [s.strip() for s in env_val.split(",") if s.strip()]


# ======================================
# Symbols config
# ======================================

# In-memory override (persist to env if you want to make permanent)
_CONFIG_SYMBOLS = get_env_list("SYMBOLS", "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD")

@app.get("/config/symbols")
def get_symbols():
    return jsonify({"symbols": _CONFIG_SYMBOLS})

@app.post("/config/symbols")
def set_symbols():
    data = request.json or {}
    symbols = data.get("symbols") or request.args.get("symbols")
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]
    if not symbols:
        return jsonify({"ok": False, "error": "no_symbols"}), 400
    global _CONFIG_SYMBOLS
    _CONFIG_SYMBOLS = symbols
    return jsonify({"ok": True, "symbols": _CONFIG_SYMBOLS})


# ======================================
# Diagnostics
# ======================================

@app.get("/diag/candles")
def diag_candles():
    symbols = request.args.get("symbols") or ",".join(_CONFIG_SYMBOLS)
    timeframe = request.args.get("tf", os.getenv("AUTORUN_TIMEFRAME", "5Min"))
    limit = int(request.args.get("limit", os.getenv("AUTORUN_LIMIT", "600")))
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]

    rows = {}
    meta = {}
    for sym in symbols:
        df = safe_candles(sym, timeframe, limit)
        n = int(df.shape[0]) if isinstance(df, pd.DataFrame) else 0
        rows[sym] = n
        if n:
            try:
                nd = normalize_candles(df)
                last_ts = nd["ts"].iloc[-1].isoformat() if nd is not None and len(nd) else None
            except Exception:
                last_ts = None
        else:
            last_ts = None
        meta[sym] = {"rows": n, "last_ts": last_ts}
    return jsonify({"rows": rows, "meta": meta})


# ======================================
# SCAN endpoint (C1..C6)
# ======================================

def load_strategy(name: str):
    """Dynamically import strategies.cX"""
    modname = f"strategies.{name}"
    try:
        mod = __import__(modname, fromlist=["*"])
        run_fn = getattr(mod, "run", None)
        return run_fn
    except Exception:
        return None

@app.post("/scan/<name>")
def scan(name):
    # Query params (with env fallbacks)
    args = request.args
    dry = str(args.get("dry", "1")).lower() in ("1", "true", "yes")
    timeframe = args.get("timeframe", os.getenv("AUTORUN_TIMEFRAME", "5Min"))
    limit = int(args.get("limit", os.getenv("AUTORUN_LIMIT", "600")))
    notional = float(args.get("notional", os.getenv("AUTORUN_NOTIONAL", "25")))
    symbols = args.get("symbols") or ",".join(_CONFIG_SYMBOLS)
    if isinstance(symbols, str):
        symbols = [s.strip() for s in symbols.split(",") if s.strip()]

    run_fn = load_strategy(name)
    if not run_fn:
        return jsonify({"ok": False, "error": f"strategy {name} not found"}), 404

    placed = []
    results = []

    for sym in symbols:
        try:
            raw = safe_candles(sym, timeframe, limit)
            nd = normalize_candles(raw)
            # Require enough bars for common indicators (ATR/MAs)
            min_bars = int(os.getenv("MIN_BARS_REQUIRED", "50"))
            if nd is None or len(nd) < min_bars:
                results.append({"symbol": sym, "action": "flat", "reason": "no_or_short_candles"})
                continue

            # Strategy contract: run(df, symbol, notional, dry, broker, **kwargs) -> dict
            decision = run_fn(df=nd, symbol=sym, notional=notional, dry=dry, broker=broker)
            if not isinstance(decision, dict):
                results.append({"symbol": sym, "action": "flat", "reason": "invalid_decision"})
                continue

            # Standardize decision
            act = decision.get("action", "flat")
            reason = decision.get("reason", "ok")
            orders = decision.get("placed") or []

            # If the strategy prefers this app to place orders, handle here (only when live)
            if (not dry) and act in ("buy", "sell") and not orders:
                cid = decision.get("client_order_id") or f"{name}-{sym.replace('/','')}-{int(time.time())}"
                side = "buy" if act == "buy" else "sell"
                res = safe_place_order(symbol=sym.replace("/", ""),
                                       side=side,
                                       notional=notional,
                                       client_order_id=cid)
                if res:
                    orders = [res]

            if orders:
                placed.extend(orders)
            else:
                results.append({"symbol": sym, "action": act, "reason": reason})

        except Exception as e:
            # Never allow a symbol crash to 500 the whole request
            if INCLUDE_TRACE:
                app.logger.exception(f"scan {name} {sym} crashed")
            results.append({"symbol": sym, "action": "flat", "reason": f"error:{e.__class__.__name__}"})

    return jsonify({"ok": True, "dry": dry, "placed": placed, "results": results})


# ======================================
# Orders & PnL endpoints
# ======================================

@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status", "all")
    limit = int(request.args.get("limit", "200"))
    data = safe_orders(status=status, limit=limit)
    # Always JSON (even if 3rd party lib returns objects)
    try:
        return jsonify(data)
    except Exception:
        # best effort to JSON-ify
        def _coerce(x):
            if isinstance(x, dict):
                return x
            try:
                return json.loads(json.dumps(x, default=str))
            except Exception:
                return {"value": str(x)}
        return jsonify([_coerce(o) for o in (data or [])])

@app.get("/orders/attribution")
def orders_attribution():
    days = int(request.args.get("days", "30"))
    orders = safe_orders(status="all", limit=1000)
    fills = extract_fills(orders, since_days=days)
    stats = pnl_by_strategy(fills)
    out = []
    for strat, s in sorted(stats["by_strategy"].items()):
        out.append({
            "strategy": strat.upper(),
            "orders": s["orders"],
            "buys": s["buys"],
            "sells": s["sells"],
            "realized_pnl": round(s["realized"], 2),
        })
    return jsonify({"days": days, "total_realized": round(stats["total"], 2), "by_strategy": out})

@app.get("/pnl/summary")
def pnl_summary():
    days = int(request.args.get("days", "30"))
    orders = safe_orders(status="all", limit=2000)
    fills = extract_fills(orders, since_days=days)
    # Realized
    fifo = fifo_realized_pnl(fills)
    realized_total = round(fifo["total"], 2)

    # Account snapshot
    acct = safe_account()
    equity = float(acct.get("equity") or acct.get("portfolio_value") or 0.0)
    cash = float(acct.get("cash") or acct.get("buying_power") or 0.0)

    # Open P&L from positions (rough: (current_price - avg) * qty)
    positions = safe_positions()
    open_pnl = 0.0
    for p in positions or []:
        try:
            qty = float(p.get("qty") or p.get("quantity") or 0.0)
            ap = float(p.get("avg_entry_price") or 0.0)
            cp = float(p.get("current_price") or 0.0)
            open_pnl += (cp - ap) * qty
        except Exception:
            pass

    return jsonify({
        "days": days,
        "realized_total": realized_total,
        "open_pnl": round(open_pnl, 2),
        "equity": round(equity, 2),
        "cash": round(cash, 2),
        "positions_count": len(positions or []),
    })


# ======================================
# Calendar
# ======================================

@app.get("/calendar")
def calendar():
    days = int(request.args.get("days", "30"))
    cal = safe_calendar(days=days) or []
    # Normalize minimal fields
    out = []
    for c in cal:
        try:
            date = c.get("date") or c.get("calendar_date") or c.get("time") or c.get("timestamp")
        except AttributeError:
            date = None
        out.append({
            "date": str(date) if date else None,
            "open": c.get("open"),
            "close": c.get("close"),
            "session": c.get("session"),
            "notes": c.get("notes") or c.get("comment"),
        })
    return jsonify({"days": days, "events": out})


# ======================================
# Root: responsive dashboard (HTML)
# ======================================

DASH_HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Crypto System Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg: #0b1020;
      --card: #121832;
      --muted: #8ca0ff;
      --text: #e7ecff;
      --danger: #ff6b6b;
      --ok: #22c55e;
      --warn: #f7c948;
      --grid: 12px;
      --radius: 14px;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, "Apple Color Emoji", "Segoe UI Emoji";
      background: linear-gradient(180deg, #0b1020, #0a0f1e 40%, #080d1a);
      color: var(--text);
    }
    header {
      padding: 18px var(--grid);
      border-bottom: 1px solid rgba(255,255,255,0.06);
      display: flex; align-items: center; gap: 10px; justify-content: space-between;
      position: sticky; top: 0; backdrop-filter: blur(6px);
      background: rgba(11,16,32,0.6);
    }
    .brand { font-weight: 700; letter-spacing: .4px; }
    .container { padding: 18px; max-width: 1200px; margin: 0 auto; }

    .cards { display: grid; grid-template-columns: repeat(4, 1fr); gap: 12px; }
    .card {
      background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.02));
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: var(--radius);
      padding: 14px;
      box-shadow: 0 8px 20px rgba(0,0,0,0.25);
    }
    .card h3 { margin: 0 0 6px 0; font-size: 0.95rem; font-weight: 600; color: var(--muted); }
    .stat { font-size: 1.6rem; font-weight: 700; }

    .row { display: grid; grid-template-columns: 1.2fr .8fr; gap: 12px; margin-top: 12px; }
    .section { background: rgba(255,255,255,0.03); border: 1px solid rgba(255,255,255,0.08); border-radius: var(--radius); padding: 14px; }

    .toolbar { display: flex; gap: 8px; align-items: center; margin-bottom: 12px; flex-wrap: wrap; }
    select, button {
      background: rgba(255,255,255,0.08); color: var(--text); border: 1px solid rgba(255,255,255,0.15);
      padding: 8px 10px; border-radius: 10px; font-weight: 600;
    }
    button.primary { background: #3246ff; border-color: #4356ff; }

    table { width: 100%; border-collapse: collapse; }
    th, td { padding: 8px 10px; border-bottom: 1px solid rgba(255,255,255,0.06); font-size: 0.92rem; }
    th { color: var(--muted); text-align: left; }

    .pill { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: .85rem; }
    .pill.ok { background: rgba(34,197,94,.15); color: #7bf2a4; }
    .pill.bad { background: rgba(255,107,107,.15); color: #ff9c9c; }
    .pill.warn { background: rgba(247,201,72,.15); color: #f7e2a4; }

    /* Mobile */
    @media (max-width: 960px) {
      .cards { grid-template-columns: repeat(2, 1fr); }
      .row { grid-template-columns: 1fr; }
    }
    @media (max-width: 560px) {
      .cards { grid-template-columns: 1fr; }
      header { flex-direction: column; align-items: flex-start; gap: 8px; }
    }
  </style>
</head>
<body>
  <header>
    <div class="brand">⚡ Crypto Trading System — Dashboard</div>
    <div class="toolbar">
      <label for="days">P&amp;L Window</label>
      <select id="days">
        <option value="7">7d</option>
        <option value="14">14d</option>
        <option value="30" selected>30d</option>
        <option value="60">60d</option>
        <option value="90">90d</option>
      </select>
      <button class="primary" id="refresh">Refresh</button>
    </div>
  </header>

  <div class="container">
    <!-- Summary -->
    <div class="cards" id="summary">
      <div class="card"><h3>Equity</h3><div class="stat" id="equity">—</div></div>
      <div class="card"><h3>Cash</h3><div class="stat" id="cash">—</div></div>
      <div class="card"><h3>Open P&amp;L</h3><div class="stat" id="open_pnl">—</div></div>
      <div class="card"><h3>Positions</h3><div class="stat" id="pos_count">—</div></div>
    </div>

    <!-- PnL + Strategies -->
    <div class="row">
      <div class="section">
        <div class="toolbar"><strong>P&amp;L Summary</strong></div>
        <div class="cards" style="grid-template-columns: repeat(3, 1fr);">
          <div class="card"><h3>Realized (window)</h3><div class="stat" id="realized_total">—</div></div>
          <div class="card"><h3>Open P&amp;L (now)</h3><div class="stat" id="open_pnl_2">—</div></div>
          <div class="card"><h3>Window</h3><div class="stat" id="days_lbl">—</div></div>
        </div>
      </div>

      <div class="section">
        <div class="toolbar"><strong>Calendar (next 30d)</strong></div>
        <table id="cal_tbl">
          <thead><tr><th>Date</th><th>Open</th><th>Close</th><th>Session</th><th>Notes</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>
    </div>

    <!-- Strategy Attribution -->
    <div class="section" style="margin-top:12px;">
      <div class="toolbar"><strong>Per-Strategy P&amp;L</strong></div>
      <table id="strat_tbl">
        <thead><tr><th>Strategy</th><th>Orders</th><th>Buys</th><th>Sells</th><th>Realized P&amp;L</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>

    <!-- Recent Orders -->
    <div class="section" style="margin-top:12px;">
      <div class="toolbar"><strong>Recent Orders</strong></div>
      <table id="orders_tbl">
        <thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Avg Price</th><th>Status</th><th>Client ID</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </div>

  <script>
    const $ = (id) => document.getElementById(id);
    const fmtUSD = (x) => {
      if (x === null || x === undefined || isNaN(x)) return "—";
      return new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 2 }).format(x);
    }

    async function getJSON(url) {
      const r = await fetch(url, { cache: "no-store" });
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }

    async function loadSummary(days) {
      const data = await getJSON(`/pnl/summary?days=${days}`);
      $('equity').innerText = fmtUSD(data.equity);
      $('cash').innerText = fmtUSD(data.cash);
      $('open_pnl').innerText = fmtUSD(data.open_pnl);
      $('pos_count').innerText = (data.positions_count ?? 0);

      $('realized_total').innerText = fmtUSD(data.realized_total);
      $('open_pnl_2').innerText = fmtUSD(data.open_pnl);
      $('days_lbl').innerText = data.days + " days";
    }

    async function loadAttribution(days) {
      const data = await getJSON(`/orders/attribution?days=${days}`);
      const tb = $('#strat_tbl').querySelector('tbody');
      tb.innerHTML = "";
      (data.by_strategy || []).forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${row.strategy}</td>
          <td>${row.orders}</td>
          <td>${row.buys}</td>
          <td>${row.sells}</td>
          <td>${fmtUSD(row.realized_pnl)}</td>`;
        tb.appendChild(tr);
      });
    }

    async function loadOrders() {
      const rows = await getJSON(`/orders/recent?status=all&limit=200`);
      const tb = $('#orders_tbl').querySelector('tbody');
      tb.innerHTML = "";
      (rows || []).forEach(o => {
        const t = o.filled_at || o.updated_at || o.submitted_at || "";
        const sym = o.symbol || o.asset_symbol || "";
        const side = (o.side || "").toUpperCase();
        const qty = o.filled_qty || o.qty || o.quantity || "";
        const avg = o.filled_avg_price || o.avg_fill_price || "";
        const st = (o.status || "").toUpperCase();
        const cid = o.client_order_id || o.client_id || "";
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${t}</td><td>${sym}</td><td>${side}</td>
          <td>${qty}</td><td>${avg}</td><td>${st}</td><td>${cid}</td>`;
        tb.appendChild(tr);
      });
    }

    async function loadCalendar() {
      const data = await getJSON(`/calendar?days=30`);
      const tb = $('#cal_tbl').querySelector('tbody');
      tb.innerHTML = "";
      (data.events || []).forEach(e => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${e.date || ""}</td>
          <td>${e.open ?? ""}</td>
          <td>${e.close ?? ""}</td>
          <td>${e.session ?? ""}</td>
          <td>${e.notes ?? ""}</td>`;
        tb.appendChild(tr);
      });
    }

    async function refreshAll() {
      const days = parseInt($('days').value, 10);
      await Promise.all([
        loadSummary(days),
        loadAttribution(days),
        loadOrders(),
        loadCalendar()
      ]);
    }

    $('refresh').addEventListener('click', refreshAll);
    document.addEventListener('DOMContentLoaded', refreshAll);
  </script>
</body>
</html>
"""

@app.get("/")
def index():
    return Response(DASH_HTML, mimetype="text/html")


# ======================================
# Health
# ======================================

@app.get("/health")
def health():
    return jsonify({"ok": True, "time": utcnow().isoformat()})


# ======================================
# Entrypoint
# ======================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    # In Render you typically run with debug off. Keep threaded True for concurrent requests.
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
