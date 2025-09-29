# app.py — v1.9.6 “everything + seeded PnL”
# - Realized P&L via FIFO across full history
# - If earlier BUYs are missing, seed FIFO from current positions (avg_entry_price)
# - Monthly Calendar, Per-Strategy attribution, Summary, Data Check
# - Mobile-friendly dashboard
# - Unrealized fallback: sum(market_value - cost_basis) when last prices unavailable

import os
import re
import time
import json
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque
from typing import Any, Iterable

import pandas as pd
from flask import Flask, request, jsonify, render_template_string

APP_VERSION = "1.9.6"
UTC = timezone.utc

# ---------- Bars normalizer ----------
def bars_to_df(bars: Any) -> pd.DataFrame:
    if isinstance(bars, pd.DataFrame):
        df = bars.copy()
    elif isinstance(bars, Iterable) and not isinstance(bars, (str, bytes, dict)):
        df = pd.DataFrame(list(bars))
    elif isinstance(bars, dict):
        df = pd.DataFrame(bars)
    else:
        return pd.DataFrame(columns=["open","high","low","close","volume"])

    col_maps = [
        {"o":"open","h":"high","l":"low","c":"close","v":"volume","t":"timestamp"},
        {"open":"open","high":"high","low":"low","close":"close","volume":"volume","timestamp":"timestamp"},
        {"Open":"open","High":"high","Low":"low","Close":"close","Volume":"volume","Timestamp":"timestamp"},
    ]
    lower = {str(c).lower(): c for c in df.columns}
    for cmap in col_maps:
        if all(k in lower for k in cmap.keys()):
            df = df.rename(columns={ lower[k]: v for k, v in cmap.items() if k in lower })
            break

    for col in ("open","high","low","close","volume"):
        if col not in df.columns:
            df[col] = pd.NA

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
        df = df.sort_values("timestamp").reset_index(drop=True).set_index("timestamp")
    else:
        df = df.reset_index(drop=True)

    for col in ("open","high","low","close","volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df[["open","high","low","close","volume"]]

# ---------- Broker ----------
import broker  # must exist as broker.py (your wrapper around Alpaca)

# ---------- Utilities ----------
def _safe_float(x, default=0.0):
    try: return float(x)
    except Exception: return default

def _now_utc():
    return datetime.now(tz=UTC).replace(microsecond=0)

def _norm_sym_slash(sym: str) -> str:
    if not sym: return sym
    s = str(sym).replace(" ", "")
    if "/" in s: return s
    for q in ("USD","USDT","USDC"):
        if s.endswith(q):
            base = s[:-len(q)]
            if base:
                return f"{base}/{q}"
    return s

# ---------- Symbols persistence ----------
_SYMBOLS_FILE = os.environ.get("SYMBOLS_FILE","symbols.txt")

def get_symbols():
    env = os.environ.get("SYMBOLS","")
    if env.strip():
        return [s.strip() for s in env.split(",") if s.strip()]
    if os.path.exists(_SYMBOLS_FILE):
        try:
            with open(_SYMBOLS_FILE,"r") as f:
                return [ln.strip() for ln in f if ln.strip()]
        except Exception:
            pass
    return ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD","AVAX/USD","LINK/USD","BCH/USD","LTC/USD"]

def set_symbols(symbols):
    try:
        with open(_SYMBOLS_FILE,"w") as f:
            for s in symbols:
                f.write(s+"\n")
    except Exception:
        pass

# ---------- Strategy loader ----------
def load_strategies():
    tried = []
    modules = []
    try:
        base_pkg = "strategies"
        for i in range(1, 7):
            name = f"c{i}"
            mod = __import__(f"{base_pkg}.{name}", fromlist=[name])
            modules.append(mod)
            tried.append(f"{base_pkg}.{name}")
    except Exception:
        modules = []
        tried.clear()
        for i in range(1, 7):
            name = f"c{i}"
            try:
                mod = __import__(name)
                modules.append(mod)
                tried.append(name)
            except Exception:
                pass

    valid = [m for m in modules if getattr(m, "NAME", None) and hasattr(m, "run")]
    return valid, tried

STRATS, _TRIED = load_strategies()
STRAT_MAP = {m.NAME: m for m in STRATS}

# ---------- P&L (FIFO) ----------
def _pick_time(o):
    for k in ("filled_at","completed_at","submitted_at","created_at","timestamp"):
        v = o.get(k)
        if v:
            try:
                return datetime.fromisoformat(str(v).replace("Z","+00:00")).astimezone(UTC)
            except Exception:
                pass
    return None

def _extract_order(o):
    sym  = o.get("symbol") or o.get("asset_symbol") or ""
    side = (o.get("side") or "").lower()
    qty  = abs(_safe_float(o.get("qty") or o.get("quantity") or o.get("filled_qty"), 0.0))
    notional = _safe_float(o.get("notional"), 0.0)
    price = _safe_float(o.get("filled_avg_price") or (notional/qty if qty>0 else 0.0), 0.0)
    coid = (o.get("client_order_id") or "")
    strat = "unknown"
    m = re.match(r"(c[1-6])[-_]", coid, re.I)
    if m: strat = m.group(1).lower()
    return sym, side, qty, price, strat

def compute_fifo_all_with_seed(orders, positions):
    """
    Build FIFO matches. If FIFO would start with a SELL and we have no earlier BUYs
    in history, we seed inventory from the *current* position average price so the
    realized P&L is meaningful (approximate) instead of zero.
    """
    # 1) Collect all orders with times
    rows = []
    for o in (orders or []):
        t = _pick_time(o)
        if not t: 
            continue
        rows.append((t,o))
    rows.sort(key=lambda x: x[0])

    # 2) Build a seed lot per symbol at the earliest order time
    seed_by_sym = {}
    for p in (positions or []):
        sym = p.get("symbol") or p.get("asset_symbol")
        q   = _safe_float(p.get("qty") or p.get("quantity"))
        px  = _safe_float(p.get("avg_entry_price"))
        if sym and q>0 and px>0:
            seed_by_sym[sym] = (q, px)

    inv = defaultdict(deque)  # sym -> deque([qty, px, strat])
    legs = []

    # Pre-seed just once (if needed) right before we process the first order
    earliest = rows[0][0] - timedelta(seconds=1) if rows else _now_utc()

    # 3) Walk orders and match FIFO, seeding if required
    for t, o in rows:
        sym, side, qty, price, strat = _extract_order(o)
        if qty <= 0 or price <= 0 or side not in ("buy","sell"):
            continue

        if side == "buy":
            inv[sym].append([qty, price, strat])
            continue

        # SELL path:
        # If there is no inventory and we have a position seed, inject it *once*
        if not inv[sym] and sym in seed_by_sym:
            sq, spx = seed_by_sym.pop(sym)  # only once
            # Use a neutral strat for seed
            inv[sym].append([sq, spx, "seed"])

        qsell = qty
        while qsell > 1e-12 and inv[sym]:
            q0, px0, strat0 = inv[sym][0]
            take = min(qsell, q0)
            pnl = (price - px0) * take
            legs.append({
                "time": t, "symbol": sym, "pnl": pnl, "qty": take,
                "buy_px": px0, "sell_px": price, "strat": strat0 or strat
            })
            qsell -= take
            q0 -= take
            if q0 <= 1e-12:
                inv[sym].popleft()
            else:
                inv[sym][0][0] = q0
        # If still leftover qsell and no inventory, we cannot price it → ignored.

    return legs

def realized_window_from_legs(legs, start_dt, end_dt):
    total = 0.0
    by_day = defaultdict(float)
    by_strat = defaultdict(lambda: {"count":0,"wins":0,"losses":0,"realized":0.0})
    for leg in legs:
        t = leg["time"]
        if (start_dt and t < start_dt) or (end_dt and t > end_dt):
            continue
        pnl = float(leg["pnl"])
        total += pnl
        d = t.strftime("%Y-%m-%d")
        by_day[d] += pnl
        s = leg.get("strat","unknown") or "unknown"
        by_strat[s]["count"] += 1
        if pnl > 0: by_strat[s]["wins"] += 1
        if pnl < 0: by_strat[s]["losses"] += 1
        by_strat[s]["realized"] += pnl
    return total, by_day, by_strat

# ---------- Flask ----------
app = Flask(__name__)

@app.get("/health/versions")
def health_versions():
    systems = {m.NAME: getattr(m, "VERSION", "unknown") for m in STRATS}
    return jsonify({
        "app": APP_VERSION,
        "exchange": "alpaca",
        "trading_base": broker.trading_base,
        "data_base": broker.data_base,
        "systems": systems,
        "loaded_from": _TRIED,
    })

@app.get("/routes")
def list_routes():
    rv = [str(r) for r in app.url_map.iter_rules()]
    return jsonify({"routes": rv})

@app.get("/config/symbols")
def get_config_symbols():
    return jsonify({"ok": True, "symbols": get_symbols(), "version": APP_VERSION})

@app.post("/config/symbols")
def post_config_symbols():
    payload = request.get_json(force=True) or {}
    symbols = payload.get("symbols") or []
    if not symbols:
        return jsonify({"ok": False, "error": "no symbols"}), 400
    set_symbols(symbols)
    return jsonify({"ok": True, "symbols": get_symbols(), "count": len(get_symbols()), "version": APP_VERSION})

@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status","all")
    limit  = int(request.args.get("limit","200"))
    rows = broker.list_orders(status=status, limit=limit)
    return jsonify({"value": rows, "Count": len(rows)})

@app.get("/positions")
def positions():
    rows = broker.list_positions()
    return jsonify({"value": rows, "Count": len(rows)})

@app.get("/orders/attribution")
def orders_attribution():
    days = int(request.args.get("days","30"))
    end = _now_utc()
    start = (end - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    orders = broker.list_orders(status="all", limit=4000)
    positions = broker.list_positions()
    legs = compute_fifo_all_with_seed(orders, positions)
    _, _, by_strat = realized_window_from_legs(legs, start, end)
    for name in ["c1","c2","c3","c4","c5","c6","seed","unknown"]:
        by_strat.setdefault(name, {"count":0,"wins":0,"losses":0,"realized":0.0})
    return jsonify({"per_strategy": by_strat, "start": start.isoformat(), "end": end.isoformat(), "version": APP_VERSION})

@app.get("/pnl/summary")
def pnl_summary():
    now = _now_utc()
    today_start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    wk_start = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
    mo_start = datetime(now.year, now.month, 1, tzinfo=UTC)

    orders = broker.list_orders(status="all", limit=4000)
    positions = broker.list_positions()
    legs = compute_fifo_all_with_seed(orders, positions)

    def realized_between(a,b):
        t, _, _ = realized_window_from_legs(legs, a, b)
        return t

    realized_today = realized_between(today_start, now)
    realized_week  = realized_between(wk_start, now)
    realized_month = realized_between(mo_start, now)

    # Unrealized:
    unrealized = 0.0
    # Preferred: last prices; Fallback: market_value - cost_basis
    try:
        syms = [p.get("symbol") or p.get("asset_symbol") for p in positions]
        last = {}
        try:
            last = broker.last_trade_map(syms) or {}
        except Exception:
            last = {}
        if last:
            for p in positions:
                sym = p.get("symbol") or p.get("asset_symbol")
                qty = _safe_float(p.get("qty") or p.get("quantity"))
                px  = _safe_float(p.get("avg_entry_price"))
                last_px = _safe_float((last.get(sym) or {}).get("price"))
                if qty and last_px:
                    unrealized += (last_px - px) * qty
        else:
            # Fallback using Alpaca position fields
            for p in positions:
                mv = _safe_float(p.get("market_value"))
                cb = _safe_float(p.get("cost_basis"))
                if mv or cb:
                    unrealized += (mv - cb)
    except Exception:
        pass

    return jsonify({
        "as_of_utc": now.isoformat(),
        "realized": {"today": realized_today, "week": realized_week, "month": realized_month},
        "unrealized": unrealized,
    })

@app.get("/pnl/daily")
def pnl_daily():
    days = int(request.args.get("days","14"))
    end = _now_utc()
    start = (end - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)

    orders = broker.list_orders(status="all", limit=4000)
    positions = broker.list_positions()
    legs = compute_fifo_all_with_seed(orders, positions)

    _, by_day, _ = realized_window_from_legs(legs, start, end)
    series = [{"date": d, "pnl": pnl} for d, pnl in sorted(by_day.items())]
    return jsonify({"series": series, "as_of_utc": end.isoformat()})

@app.get("/pnl/calendar_month")
def pnl_calendar_month():
    """
    Returns a full month grid of realized P&L (sell legs only) computed with full-history FIFO pairing plus position seeding.
    Query: ?month=YYYY-MM  (default = current month)
    """
    q = request.args.get("month","")
    now = _now_utc()
    try:
        if q:
            y, m = [int(x) for x in q.split("-")]
            first = datetime(y, m, 1, tzinfo=UTC)
        else:
            first = datetime(now.year, now.month, 1, tzinfo=UTC)
    except Exception:
        first = datetime(now.year, now.month, 1, tzinfo=UTC)

    # start Monday grid and cover 6 weeks
    start = first - timedelta(days=(first.weekday() % 7))
    end   = start + timedelta(days=6*7) - timedelta(seconds=1)

    orders = broker.list_orders(status="all", limit=4000)
    positions = broker.list_positions()
    legs = compute_fifo_all_with_seed(orders, positions)

    _, by_day, _ = realized_window_from_legs(legs, start, end)

    days = []
    d = start
    while d <= end:
        key = d.strftime("%Y-%m-%d")
        days.append({"date": key, "pnl": by_day.get(key, 0.0),
                     "in_month": d.month == first.month})
        d += timedelta(days=1)

    return jsonify({
        "month": first.strftime("%Y-%m"),
        "start": start.strftime("%Y-%m-%d"),
        "end": end.strftime("%Y-%m-%d"),
        "days": days
    })

@app.get("/diag/candles")
def diag_candles():
    syms = request.args.get("symbols","")
    tf   = request.args.get("tf","1Min")
    limit= int(request.args.get("limit","100"))
    rows = {}
    symbols = [s.strip() for s in syms.split(",") if s.strip()] or get_symbols()
    df_map = broker.get_bars(symbols, timeframe=tf, limit=limit, merge=False)
    for s, df in df_map.items():
        rows[s] = int(len(df)) if df is not None else 0
    return jsonify({"rows": rows})

@app.post("/scan/<name>")
def scan(name):
    name = name.lower()
    if name not in STRAT_MAP:
        return jsonify({"ok": False, "error": f"unknown strategy '{name}'"}), 404

    dry = request.args.get("dry","1") != "0"
    timeframe = request.args.get("timeframe","1Min")
    limit = int(request.args.get("limit","600"))
    notional = _safe_float(request.args.get("notional","5"), 5.0)
    params = {k:v for k,v in request.args.items() if k not in ("dry","timeframe","limit","notional","symbols")}
    symbols = get_symbols()
    if "symbols" in request.args:
        symbols = [s.strip() for s in request.args.get("symbols","").split(",") if s.strip()]

    # positions normalized
    pos = broker.list_positions()
    long_map = {
        _norm_sym_slash(p.get("symbol") or p.get("asset_symbol")): _safe_float(p.get("qty") or p.get("quantity"))
        for p in pos
    }

    df_map_raw = broker.get_bars(symbols, timeframe=timeframe, limit=limit, merge=False)
    df_map = {sym: bars_to_df(b) for sym, b in df_map_raw.items()}

    strat = STRAT_MAP[name]
    results = strat.run(df_map, params, long_map)

    placed = []
    if not dry:
        now_epoch = int(time.time())
        for r in results:
            symbol = r.get("symbol"); action = r.get("action")
            if not symbol or action not in ("buy","sell"): 
                continue
            qty_have = long_map.get(symbol, 0.0)
            if action == "buy" and qty_have > 0: 
                continue
            if action == "sell" and qty_have <= 0: 
                continue
            coid = f"{name}-{symbol.replace('/','')}-{now_epoch}"
            o = broker.submit_order(symbol=symbol, side=action, notional=notional,
                                    client_order_id=coid, time_in_force="gtc", type="market")
            placed.append(o)

    return jsonify({"ok": True, "dry": dry, "results": results, "placed": placed})

# ---------- Autorun loop ----------
_last_bar_ts = {}

def autorun_worker():
    enabled = os.environ.get("AUTORUN_ENABLED","false").lower() == "true"
    if not enabled: return
    interval = int(os.environ.get("AUTORUN_INTERVAL_SECS","60"))
    timeframe = os.environ.get("AUTORUN_TIMEFRAME","1Min")
    limit = int(os.environ.get("AUTORUN_LIMIT","600"))
    notional = _safe_float(os.environ.get("AUTORUN_NOTIONAL","5"), 5.0)

    while True:
        try:
            symbols = get_symbols()
            pos = broker.list_positions()
            long_map = {
                _norm_sym_slash(p.get("symbol") or p.get("asset_symbol")): _safe_float(p.get("qty") or p.get("quantity"))
                for p in pos
            }
            df_map_raw = broker.get_bars(symbols, timeframe=timeframe, limit=limit, merge=False)
            df_map = {sym: bars_to_df(b) for sym, b in df_map_raw.items()}

            latest_by_symbol = {}
            for sym, df in df_map.items():
                if df is None or df.empty: 
                    continue
                tlast = pd.to_datetime(df.index[-1]).to_pydatetime().replace(tzinfo=UTC)
                latest_by_symbol[sym] = tlast

            now_epoch = int(time.time())
            for strat in STRATS:
                try:
                    results = strat.run(df_map, {}, long_map)
                except Exception:
                    continue
                for r in (results or []):
                    symbol = r.get("symbol"); action = r.get("action")
                    if action not in ("buy","sell"): 
                        continue
                    if symbol not in latest_by_symbol: 
                        continue
                    key = (strat.NAME, symbol, timeframe)
                    if _last_bar_ts.get(key) == latest_by_symbol[symbol]:
                        continue
                    qty_have = long_map.get(symbol, 0.0)
                    if action == "buy" and qty_have > 0: 
                        continue
                    if action == "sell" and qty_have <= 0: 
                        continue
                    coid = f"{strat.NAME}-{symbol.replace('/','')}-{now_epoch}"
                    broker.submit_order(symbol=symbol, side=action, notional=notional,
                                        client_order_id=coid, time_in_force="gtc", type="market")
                    _last_bar_ts[key] = latest_by_symbol[symbol]
        except Exception:
            pass
        time.sleep(interval)

# ---------- Dashboard HTML ----------
DASH_HTML = r"""{% raw %}
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Crypto System — v{{version}}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg:#0b0f14; --panel:#111723; --muted:#93a4ba; --text:#eaf2ff; --green:#22c55e; --red:#ef4444; --accent:#60a5fa;
      --chip:#1b2433; --border:#1f2a3a; --row:#0f1520; --row2:#0c111b;
    }
    *{box-sizing:border-box}
    html,body{margin:0;background:var(--bg);color:var(--text);font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif}
    h1,h2,h3{margin:0 0 10px}
    .wrap{max-width:1200px;margin:0 auto;padding:18px}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
    .grid-3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px}
    .panel{background:var(--panel);border:1px solid var(--border);border-radius:16px;padding:14px;box-shadow:0 6px 24px rgba(0,0,0,.22)}
    .muted{color:var(--muted)}
    .pill{display:inline-flex;align-items:center;gap:8px;background:var(--chip);border:1px solid var(--border);border-radius:999px;padding:6px 10px;font-size:12px}
    .kpi{display:flex;align-items:baseline;gap:10px}
    .big{font-size:28px;font-weight:700}
    .pos{color:var(--green)} .neg{color:var(--red)}
    table{width:100%;border-collapse:collapse}
    th,td{padding:10px;border-bottom:1px solid var(--border);vertical-align:middle}
    thead th{position:sticky;top:0;background:var(--panel);z-index:1}
    tbody tr:nth-child(odd){background:var(--row)}
    tbody tr:nth-child(even){background:var(--row2)}
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace}
    .form-row{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:8px}
    input,select{padding:8px 10px;border-radius:8px;border:1px solid var(--border);background:#0f1624;color:var(--text)}
    .btn{padding:8px 12px;border-radius:8px;border:1px solid var(--border);background:#0f1624;color:var(--text);cursor:pointer}
    .btn:hover{background:#152039}
    /* Monthly calendar */
    .cal-wrap{display:grid;grid-template-columns:repeat(7,1fr);gap:6px}
    .cal-day{border:1px solid var(--border);border-radius:12px;padding:8px;background:#0d1526;min-height:74px;display:flex;flex-direction:column;justify-content:space-between}
    .cal-day.mute{opacity:.45}
    .cal-date{font-size:12px;color:#9fb1c9}
    .cal-pnl{font-size:16px}
    .nav{display:flex;gap:8px;align-items:center}
    /* Mobile */
    @media (max-width: 860px){
      .grid, .grid-3{grid-template-columns:1fr}
      .wrap{padding:12px}
      .big{font-size:24px}
      .cal-day{min-height:64px}
    }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Crypto System <span class="pill">v{{version}}</span></h1>

    <div class="grid" style="margin-top:10px">
      <div class="panel">
        <h3>Summary</h3>
        <div class="kpi"><span class="muted">Today</span> <span id="pToday" class="big mono">—</span></div>
        <div class="kpi"><span class="muted">Week</span>  <span id="pWeek"  class="big mono">—</span></div>
        <div class="kpi"><span class="muted">Month</span> <span id="pMonth" class="big mono">—</span></div>
        <div class="kpi"><span class="muted">Unrealized</span> <span id="pUnreal" class="big mono">—</span></div>
        <div class="muted" style="margin-top:6px"><span id="asOf" class="mono"></span></div>
      </div>

      <div class="panel">
        <div class="nav" style="justify-content:space-between">
          <h3>Calendar (monthly realized)</h3>
          <div class="nav">
            <button class="btn" onclick="shiftMonth(-1)">◀</button>
            <span id="calLabel" class="pill">—</span>
            <button class="btn" onclick="shiftMonth(1)">▶</button>
          </div>
        </div>
        <div class="cal-wrap" style="margin-top:10px">
          <div class="muted" style="grid-column: span 7">Mon Tue Wed Thu Fri Sat Sun</div>
          <div id="calGrid" style="grid-column: span 7; display:grid; grid-template-columns:repeat(7,1fr); gap:6px"></div>
        </div>
      </div>
    </div>

    <div class="grid-3" style="margin-top:14px">
      <div class="panel">
        <h3>Strategies (server)</h3>
        <div id="strats">Loading…</div>
      </div>

      <div class="panel">
        <h3>Run Scan</h3>
        <div class="form-row">
          <select id="strategy">
            <option value="c1">C1</option><option value="c2">C2</option><option value="c3">C3</option>
            <option value="c4">C4</option><option value="c5">C5</option><option value="c6">C6</option>
          </select>
          <input id="symbols" value="BTC/USD" />
          <input id="tf" value="5Min" />
          <input id="limit" type="number" value="300" />
          <input id="notional" type="number" value="25" />
          <label class="pill"><input id="dry" type="checkbox" checked /> dry</label>
          <button class="btn" onclick="runScan()">Run</button>
        </div>
        <table>
          <thead><tr><th>Symbol</th><th>Action</th><th>Reason</th></tr></thead>
          <tbody id="scanRows"><tr><td colspan="3" class="muted">Awaiting scan…</td></tr></tbody>
        </table>
      </div>

      <div class="panel">
        <h3>P&amp;L by Strategy (last 30d)</h3>
        <div class="form-row">
          <input id="attrDays" type="number" value="30" />
          <button class="btn" onclick="loadAttr()">Refresh</button>
        </div>
        <table>
          <thead><tr><th>Strategy</th><th class="mono">Trades</th><th class="mono">Wins</th><th class="mono">Losses</th><th class="mono">Realized</th></tr></thead>
          <tbody id="attrRows"><tr><td colspan="5" class="muted">Loading…</td></tr></tbody>
        </table>
        <div class="muted" id="attrWindow"></div>
      </div>
    </div>

    <div class="panel" style="margin-top:14px">
      <h3>Recent fills</h3>
      <table>
        <thead>
          <tr><th>Time</th><th>Side</th><th>Symbol</th><th class="mono">Qty</th><th class="mono">Price</th><th class="mono">Notional</th><th>Client ID</th><th>Status</th></tr>
        </thead>
        <tbody id="fills"><tr><td colspan="8" class="muted">Loading…</td></tr></tbody>
      </table>
    </div>

    <div class="panel" style="margin-top:14px">
      <h3>Data Check</h3>
      <div class="form-row">
        <input id="dcSymbols" value="BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD" size="80"/>
        <input id="dcTf" value="5Min" />
        <input id="dcLimit" type="number" value="600" />
        <button class="btn" onclick="runDataCheck()">Check</button>
      </div>
      <table>
        <thead><tr><th>Symbol</th><th>Rows</th></tr></thead>
        <tbody id="dcRows"><tr><td colspan="2" class="muted">Awaiting check…</td></tr></tbody>
      </table>
    </div>

    <footer class="muted" style="text-align:center;padding:18px">Exchange: Alpaca · Data & symbols normalized · v{{version}}</footer>
  </div>

  <script>
    const $ = sel => document.querySelector(sel);
    const fmtUsd = x => (x>=0?'+':'') + (x||0).toLocaleString(undefined,{style:'currency',currency:'USD',maximumFractionDigits:2});
    const parseDate = s => { try { return new Date(s); } catch { return null; } };
    const preferTime = (o,keys)=>{ for(const k of keys) if(o && o[k]) return o[k]; return null; };

    // --- Summary ---
    async function loadSummary(){
      const r = await fetch('/pnl/summary'); const j = await r.json();
      const M=j.realized||{};
      $('#pToday').textContent = fmtUsd(M.today||0); $('#pToday').className='big mono '+((M.today||0)>=0?'pos':'neg');
      $('#pWeek').textContent  = fmtUsd(M.week||0);  $('#pWeek').className='big mono '+((M.week||0)>=0?'pos':'neg');
      $('#pMonth').textContent = fmtUsd(M.month||0); $('#pMonth').className='big mono '+((M.month||0)>=0?'pos':'neg');
      $('#pUnreal').textContent= fmtUsd(j.unrealized||0); $('#pUnreal').className='big mono '+((j.unrealized||0)>=0?'pos':'neg');
      $('#asOf').textContent = `as of ${j.as_of_utc||''}`;
    }

    // --- Monthly calendar ---
    let calCursor = new Date(); // current month
    const ym = d => `${d.getUTCFullYear()}-${String(d.getUTCMonth()+1).padStart(2,'0')}`;

    function shiftMonth(delta){
      calCursor.setUTCMonth(calCursor.getUTCMonth()+delta);
      loadCalendar();
    }

    async function loadCalendar(){
      const label = ym(calCursor);
      $('#calLabel').textContent = label;
      const r = await fetch(`/pnl/calendar_month?month=${label}`);
      const j = await r.json();
      const grid = $('#calGrid'); grid.innerHTML = '';
      for(const d of (j.days||[])){
        const el = document.createElement('div');
        el.className = 'cal-day'+(d.in_month?'':' mute');
        const dt = document.createElement('div'); dt.className='cal-date'; dt.textContent = d.date.slice(-2);
        const pv = document.createElement('div'); const pnl=+d.pnl||0; pv.className='cal-pnl mono '+(pnl>=0?'pos':'neg'); pv.textContent = fmtUsd(pnl);
        el.appendChild(dt); el.appendChild(pv);
        grid.appendChild(el);
      }
    }

    // --- Strategies list ---
    async function loadStrats(){
      try{
        const hv = await (await fetch('/health/versions')).json();
        const keys = Object.keys(hv.systems||{}).sort();
        const rows = keys.map(k=>`<tr><td>${k.toUpperCase()}</td><td>v${hv.systems[k]}</td></tr>`).join('');
        const tbl = `<table><thead><tr><th>Strategy</th><th>Version</th></tr></thead><tbody>${rows}</tbody></table>`;
        $('#strats').innerHTML = tbl;
      }catch{ $('#strats').textContent='Failed to load'; }
    }

    // --- P&L by Strategy ---
    async function loadAttr(){
      const days = +$('#attrDays').value || 30;
      const r = await fetch(`/orders/attribution?days=${days}`);
      const j = await r.json(); const m = j.per_strategy||{};
      const names = Object.keys(m).sort();
      const rows = names.map(k=>{
        const v=m[k]||{}; const rzd=+v.realized||0;
        return `<tr><td>${k.toUpperCase()}</td><td class="mono">${v.count||0}</td><td class="mono">${v.wins||0}</td><td class="mono">${v.losses||0}</td><td class="mono ${rzd>=0?'pos':'neg'}">${fmtUsd(rzd)}</td></tr>`;
      }).join('');
      $('#attrRows').innerHTML = rows || `<tr><td colspan="5" class="muted">No data.</td></tr>`;
      $('#attrWindow').textContent = `${j.start||''} — ${j.end||''}`;
    }

    // --- Scan ---
    async function runScan(){
      const name=$('#strategy').value, symbols=encodeURIComponent($('#symbols').value||'BTC/USD');
      const tf=encodeURIComponent($('#tf').value||'5Min'); const limit=+($('#limit').value||300);
      const notional=+($('#notional').value||25); const dry=$('#dry').checked?1:0;
      const url=`/scan/${name}?dry=${dry}&symbols=${symbols}&timeframe=${tf}&limit=${limit}&notional=${notional}`;
      const r=await fetch(url,{method:'POST'}); const j=await r.json();
      const rows=(j.results||[]).map(r=>`<tr><td>${r.symbol}</td><td>${(r.action||'').toUpperCase()}</td><td>${r.reason||''}</td></tr>`).join('');
      $('#scanRows').innerHTML = rows || `<tr><td colspan="3" class="muted">No results.</td></tr>`;
    }

    // --- Fills ---
    function coalesceSymbol(o){ return o.symbol || o.asset_symbol || ''; }
    function pickQty(o){ return +((o.qty||o.quantity||o.filled_qty)||0); }
    function pickPx(o){ return +(o.filled_avg_price||0); }
    function pickNotional(o){ const q=pickQty(o), px=pickPx(o); return +(o.notional || (q*px)); }
    function pickTime(o){ const s = preferTime(o, ['filled_at','completed_at','submitted_at','created_at','timestamp']); return parseDate(s); }
    async function loadFills(){
      const r=await fetch('/orders/recent?status=all&limit=200'); let j=await r.json();
      let arr=Array.isArray(j)?j:(Array.isArray(j.value)?j.value:[]); arr=arr.filter(o=>o&&(o.status||o.side));
      const rows=arr.map(o=>{
        const ts=pickTime(o), dt=ts?ts.toISOString().replace('T',' ').replace('Z',''):'';
        const side=(o.side||'').toUpperCase(), sym=coalesceSymbol(o), qty=pickQty(o), px=pickPx(o), notional=pickNotional(o);
        const coid=o.client_order_id||'', status=(o.status||'').toUpperCase();
        return `<tr><td class="mono">${dt}</td><td>${side}</td><td>${sym}</td><td class="mono">${qty.toFixed(6)}</td><td class="mono">${px?px.toFixed(6):''}</td><td class="mono">${notional?notional.toFixed(2):''}</td><td class="mono">${coid}</td><td>${status}</td></tr>`;
      }).join('');
      $('#fills').innerHTML = rows || `<tr><td colspan="8" class="muted">No recent orders.</td></tr>`;
    }

    // --- Data check ---
    async function runDataCheck(){
      const sy=encodeURIComponent($('#dcSymbols').value), tf=encodeURIComponent($('#dcTf').value), lim=$('#dcLimit').value;
      const r=await fetch(`/diag/candles?symbols=${sy}&tf=${tf}&limit=${lim}`); const j=await r.json();
      const rows=Object.entries(j.rows||{}).map(([s,n])=>`<tr><td>${s}</td><td class="mono">${n}</td></tr>`).join('');
      $('#dcRows').innerHTML = rows || `<tr><td colspan="2" class="muted">No data.</td></tr>`;
    }

    async function boot(){
      await Promise.all([loadSummary(), loadStrats(), loadAttr(), loadFills(), loadCalendar()]);
      setInterval(loadSummary, 20000);
      setInterval(loadFills, 20000);
      setInterval(loadAttr, 30000);
    }
    boot();
  </script>
</body>
</html>
{% endraw %}"""

@app.get("/")
def index():
    return render_template_string(DASH_HTML, version=APP_VERSION)

# ---------- Start autorun if enabled ----------
def start_background():
    try:
        if os.environ.get("AUTORUN_ENABLED","false").lower() == "true":
            t = threading.Thread(target=autorun_worker, daemon=True)
            t.start()
    except Exception:
        pass

if __name__ == "__main__":
    start_background()
    port = int(os.environ.get("PORT","10000"))
    app.run(host="0.0.0.0", port=port)
