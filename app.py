# app.py  — v1.8.9
import os
import re
import glob
import time
import json
import importlib
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque

import pandas as pd
from flask import Flask, request, jsonify, render_template_string

APP_VERSION = "1.8.9"
UTC = timezone.utc

# ---------- Broker ----------
from services.exchange_exec import Broker as AlpacaBroker

broker = AlpacaBroker(
    api_key=os.environ.get("APCA_API_KEY_ID", ""),
    secret_key=os.environ.get("APCA_API_SECRET_KEY", ""),
    trading_base=os.environ.get("APCA_API_BASE_URL", "https://paper-api.alpaca.markets"),
    data_base=os.environ.get("DATA_API_BASE_URL", "https://data.alpaca.markets"),
)

# ---------- Symbols / config ----------
DEFAULT_SYMBOLS = [
    "BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD","AVAX/USD","LINK/USD","BCH/USD","LTC/USD"
]
_symbols = None

def get_symbols():
    global _symbols
    if _symbols is None:
        env_syms = os.environ.get("SYMBOLS", "")
        if env_syms.strip():
            _symbols = [s.strip() for s in env_syms.split(",") if s.strip()]
        else:
            _symbols = DEFAULT_SYMBOLS[:]
    return _symbols

def set_symbols(new_list):
    global _symbols
    _symbols = list(dict.fromkeys([s.strip() for s in new_list if s.strip()]))

# ---------- Strategy loader (robust) ----------
# We support:
#  - strategies/c1.py ... c6.py  (preferred)
#  - c1.py ... c6.py at repo root (fallback)
def load_strategies():
    modules = []
    tried = []

    # Preferred: strategies package
    try:
        base_pkg = "strategies"
        for i in range(1, 7):
            name = f"c{i}"
            mod = importlib.import_module(f"{base_pkg}.{name}")
            modules.append(mod)
            tried.append(f"{base_pkg}.{name}")
    except Exception:
        # Fallback: plain modules in root
        modules = []
        tried.clear()
        for i in range(1, 7):
            name = f"c{i}"
            try:
                mod = importlib.import_module(name)
                modules.append(mod)
                tried.append(name)
            except Exception:
                pass

    # keep only modules that declare NAME and VERSION
    valid = []
    for m in modules:
        if getattr(m, "NAME", None) and hasattr(m, "run"):
            valid.append(m)
    return valid, tried

STRATS, _TRIED = load_strategies()
STRAT_MAP = {m.NAME: m for m in STRATS}

# ---------- Flask ----------
app = Flask(__name__)

# ---------- Helpers ----------
def _now_utc(): return datetime.now(tz=UTC)

def _parse_dt_utc(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(UTC)
    except Exception:
        return None

def _safe_float(x, default=0.0):
    try: return float(x)
    except Exception: return default

def strat_from_coid(coid):
    if not coid or "-" not in coid: return "unknown"
    return coid.split("-",1)[0].lower()

def compute_realized_fifo(orders, start_dt=None, end_dt=None):
    filled = []
    for o in orders:
        st = (o.get("status") or "").lower()
        if st != "filled": 
            continue
        ft = _parse_dt_utc(o.get("filled_at") or o.get("submitted_at") or o.get("created_at"))
        if not ft: 
            continue
        if start_dt and ft < start_dt: 
            continue
        if end_dt and ft > end_dt:
            continue
        qty = _safe_float(o.get("filled_qty") or o.get("qty"))
        px  = _safe_float(o.get("filled_avg_price") or o.get("avg_price"))
        sym = o.get("symbol") or o.get("asset_symbol")
        side = (o.get("side") or "").lower()
        if not sym or qty <= 0 or px <= 0: 
            continue
        filled.append({"t":ft,"symbol":sym,"side":side,"qty":qty,"px":px,"coid":o.get("client_order_id")})

    realized_total = 0.0
    realized_by_day = defaultdict(float)
    realized_by_strat = defaultdict(lambda: {"count":0,"wins":0,"losses":0,"realized":0.0})
    inv = defaultdict(lambda: deque())  # symbol -> deque of [qty, px]

    for row in sorted(filled, key=lambda r: r["t"]):
        d = row["t"].date()
        sym, side, q, px = row["symbol"], row["side"], row["qty"], row["px"]
        strat = strat_from_coid(row.get("coid"))

        if side == "buy":
            inv[sym].append([q, px])
            continue

        pnl = 0.0
        qty = q
        while qty > 0 and inv[sym]:
            q0, px0 = inv[sym][0]
            take = min(qty, q0)
            pnl += (px - px0) * take
            q0 -= take
            qty -= take
            if q0 <= 1e-12:
                inv[sym].popleft()
            else:
                inv[sym][0][0] = q0

        realized_total += pnl
        realized_by_day[d] += pnl
        realized_by_strat[strat]["count"] += 1
        if pnl > 0: realized_by_strat[strat]["wins"] += 1
        if pnl < 0: realized_by_strat[strat]["losses"] += 1
        realized_by_strat[strat]["realized"] += pnl

    return realized_total, realized_by_day, realized_by_strat

def positions_unrealized(positions):
    total = 0.0
    for p in positions or []:
        u = p.get("unrealized_pl")
        if u is not None:
            total += _safe_float(u)
        else:
            mv = _safe_float(p.get("market_value"))
            qty = _safe_float(p.get("qty") or p.get("quantity"))
            avg = _safe_float(p.get("avg_entry_price"))
            if qty and avg and mv:
                total += (mv - qty*avg)
    return total

# ---------- Routes ----------
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
    try:
        payload = request.get_json(force=True) or {}
        symbols = payload.get("symbols") or []
        if not symbols:
            return jsonify({"ok": False, "error": "no symbols"}), 400
        set_symbols(symbols)
        return jsonify({"ok": True, "symbols": get_symbols(), "count": len(get_symbols()), "version": APP_VERSION})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/orders/recent")
def orders_recent():
    status = request.args.get("status","all")
    limit = int(request.args.get("limit","50"))
    try:
        rows = broker.list_orders(status=status, limit=limit)
        return jsonify({"value": rows, "Count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/positions")
def positions():
    try:
        rows = broker.list_positions()
        return jsonify(rows)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/orders/attribution")
def orders_attribution():
    days = int(request.args.get("days","1"))
    end = _now_utc()
    start = (end - timedelta(days=days)).replace(microsecond=0)
    try:
        orders = broker.list_orders(status="all", limit=2000)
        _, _, by_strat = compute_realized_fifo(orders, start_dt=start, end_dt=end)
        for name in ["c1","c2","c3","c4","c5","c6","unknown"]:
            by_strat.setdefault(name, {"count":0,"wins":0,"losses":0,"realized":0.0})
        return jsonify({"per_strategy": by_strat, "version": APP_VERSION})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/pnl/summary")
def pnl_summary():
    try:
        now = _now_utc()
        orders = broker.list_orders(status="all", limit=4000)
        today_start = datetime(now.year, now.month, now.day, tzinfo=UTC)
        wk_start = (now - timedelta(days=7)).replace(hour=0, minute=0, second=0, microsecond=0)
        mo_start = (now - timedelta(days=31)).replace(hour=0, minute=0, second=0, microsecond=0)

        def realized_between(a,b):
            r, _, _ = compute_realized_fifo(orders, start_dt=a, end_dt=b)
            return r

        realized_today = realized_between(today_start, now)
        realized_week  = realized_between(wk_start, now)
        realized_month = realized_between(mo_start, now)

        pos = broker.list_positions()
        unreal = positions_unrealized(pos)

        start30 = now - timedelta(days=30)
        _, _, attr30 = compute_realized_fifo(orders, start_dt=start30, end_dt=now)
        trade_count = sum(v["count"] for v in attr30.values())
        wins = sum(v["wins"] for v in attr30.values())
        losses = sum(v["losses"] for v in attr30.values())
        wr = (wins / trade_count * 100.0) if trade_count else 0.0

        return jsonify({
            "as_of_utc": now.isoformat(),
            "realized": {"today": round(realized_today,2), "week": round(realized_week,2), "month": round(realized_month,2)},
            "unrealized": round(unreal,2),
            "trades": {"count_30d": trade_count, "wins_30d": wins, "losses_30d": losses, "win_rate_30d": round(wr,2)},
            "version": APP_VERSION
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/pnl/daily")
def pnl_daily():
    days = int(request.args.get("days","30"))
    end = _now_utc()
    start = (end - timedelta(days=days)).replace(microsecond=0)
    try:
        orders = broker.list_orders(status="all", limit=8000)
        _, by_day, _ = compute_realized_fifo(orders, start_dt=start, end_dt=end)
        series = []
        d = start.date()
        while d <= end.date():
            series.append({"date": d.isoformat(), "pnl": round(by_day.get(d, 0.0),2)})
            d = d + timedelta(days=1)
        return jsonify({"start": start.date().isoformat(), "end": end.date().isoformat(), "days": days, "series": series, "version": APP_VERSION})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/diag/candles")
def diag_candles():
    syms = request.args.get("symbols","")
    tf   = request.args.get("tf","1Min")
    limit= int(request.args.get("limit","100"))
    rows = {}
    try:
        symbols = [s.strip() for s in syms.split(",") if s.strip()] or get_symbols()
        df_map = broker.get_bars(symbols, timeframe=tf, limit=limit, merge=False)
        for s, df in df_map.items():
            rows[s] = int(len(df)) if df is not None else 0
        return jsonify({"rows": rows})
    except Exception as e:
        for s in [s.strip() for s in syms.split(",") if s.strip()] or get_symbols():
            rows[s] = 0
        return jsonify({"rows": rows, "error": str(e)})

@app.post("/scan/<name>")
def scan(name):
    name = name.lower()
    if name not in STRAT_MAP:
        return jsonify({"ok": False, "error": f"unknown strategy '{name}'"}), 404

    dry = request.args.get("dry","1") != "0"
    timeframe = request.args.get("timeframe","1Min")
    limit = int(request.args.get("limit","600"))
    notional = _safe_float(request.args.get("notional","5"), 5.0)
    params = {k:v for k,v in request.args.items() if k not in ("dry","timeframe","limit","notional")}
    symbols = get_symbols()

    try:
        pos = broker.list_positions()
        long_map = { (p.get("symbol") or p.get("asset_symbol")): _safe_float(p.get("qty") or p.get("quantity")) for p in pos }

        df_map = broker.get_bars(symbols, timeframe=timeframe, limit=limit, merge=False)
        strat = STRAT_MAP[name]
        results = strat.run(df_map, params, long_map)

        placed = []
        if not dry:
            now_epoch = int(time.time())
            for r in results:
                action = (r.get("action") or "").lower()
                symbol = r.get("symbol")
                if action not in ("buy","sell") or not symbol:
                    continue
                qty_have = long_map.get(symbol, 0.0)
                if action == "buy" and qty_have > 0:  # avoid rebuy
                    continue
                if action == "sell" and qty_have <= 0:
                    continue
                coid = f"{name}-{symbol.replace('/','')}-{now_epoch}"
                o = broker.submit_order(symbol=symbol, side=action, notional=notional,
                                        client_order_id=coid, time_in_force="gtc", type="market")
                placed.append(o)

        return jsonify({"ok": True, "dry": dry, "results": results, "placed": placed})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# ---------- Background automation ----------
_last_bar_ts = {}  # (strategy, symbol, timeframe) -> last bar time processed

def autorun_worker():
    enabled = os.environ.get("AUTORUN_ENABLED","false").lower() == "true"
    if not enabled:
        return
    interval = int(os.environ.get("AUTORUN_INTERVAL_SECS","60"))
    timeframe = os.environ.get("AUTORUN_TIMEFRAME","1Min")
    limit = int(os.environ.get("AUTORUN_LIMIT","600"))
    notional = _safe_float(os.environ.get("AUTORUN_NOTIONAL","5"), 5.0)

    while True:
        try:
            symbols = get_symbols()
            pos = broker.list_positions()
            long_map = { (p.get("symbol") or p.get("asset_symbol")): _safe_float(p.get("qty") or p.get("quantity")) for p in pos }

            df_map = broker.get_bars(symbols, timeframe=timeframe, limit=limit, merge=False)

            latest_by_symbol = {}
            for sym, df in df_map.items():
                if df is None or df.empty: 
                    continue
                tlast = pd.to_datetime(df.index[-1]).to_pydatetime().replace(tzinfo=UTC)
                latest_by_symbol[sym] = tlast

            for strat in STRATS:
                results = strat.run(df_map, {}, long_map)
                now_epoch = int(time.time())
                for r in results:
                    action = (r.get("action") or "").lower()
                    symbol = r.get("symbol")
                    if action not in ("buy","sell") or not symbol:
                        continue
                    if symbol not in latest_by_symbol:
                        continue
                    key = (strat.NAME, symbol, timeframe)
                    last_t = _last_bar_ts.get(key)
                    if last_t == latest_by_symbol[symbol]:
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

threading.Thread(target=autorun_worker, daemon=True).start()

# ---------- Dashboard (HTML) ----------
DASH_HTML = r"""{% raw %}
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Crypto System Dashboard — v1.8.9</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {
      --bg: #0b0f14; --panel:#111723; --muted:#8091a7; --text:#eaf2ff; --green:#2dcc70; --red:#ff5c5c; --accent:#5aa5ff;
      --tableRow:#0f1520; --tableAlt:#0c111b; --chip:#1b2433; --border:#1e2a3a;
    }
    html,body{background:var(--bg);color:var(--text);font-family:Inter,system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0}
    h1,h2,h3{margin:0 0 8px}
    .wrap{max-width:1200px;margin:0 auto;padding:24px}
    .grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}
    .panel{background:var(--panel);border:1px solid var(--border);border-radius:16px;padding:16px;box-shadow:0 6px 24px rgba(0,0,0,.25)}
    .muted{color:var(--muted)}
    .pill{display:inline-flex;align-items:center;gap:8px;background:var(--chip);border:1px solid var(--border);border-radius:999px;padding:6px 10px;font-size:12px}
    .kpi{display:flex;align-items:baseline;gap:8px}
    .kpi .big{font-size:28px;font-weight:700}
    .kpi .delta{font-size:13px}
    .pos{color:var(--green)} .neg{color:var(--red)}
    table{width:100%;border-collapse:collapse}
    th,td{padding:10px;border-bottom:1px solid var(--border);vertical-align:middle}
    thead th{position:sticky;top:0;background:var(--panel);z-index:1}
    tbody tr:nth-child(odd){background:var(--tableRow)}
    tbody tr:nth-child(even){background:var(--tableAlt)}
    .right{text-align:right}
    .mono{font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;}
    .small{font-size:12px}
    .badge{display:inline-block;padding:2px 8px;border-radius:8px;font-size:11px;border:1px solid var(--border);background:#0e1523}
    .toolbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:8px 0 0}
    .calendar{display:grid;grid-template-columns:repeat(7,1fr);gap:8px}
    .day{background:var(--tableAlt);border:1px solid var(--border);border-radius:12px;padding:10px;min-height:70px}
    .day .d{font-size:11px;color:var(--muted)}
    .day .p{margin-top:6px;font-weight:600}
    a{color:var(--accent);text-decoration:none} a:hover{text-decoration:underline}
    .hint{margin-top:8px}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Crypto System <span class="pill">Dashboard <span id="appVersion" class="mono">v1.8.9</span></span></h1>

    <div class="grid" style="grid-template-columns: 1.1fr .9fr;">
      <!-- P&L Summary -->
      <div class="panel">
        <h2>P&amp;L Snapshot</h2>
        <div class="toolbar small">
          <span id="asOf" class="muted">as of —</span>
        </div>
        <div class="grid" style="grid-template-columns: repeat(3,1fr); margin-top:10px;">
          <div class="kpi"><span class="big" id="pnlToday">—</span><span class="delta muted">today</span></div>
          <div class="kpi"><span class="big" id="pnlWeek">—</span><span class="delta muted">week</span></div>
          <div class="kpi"><span class="big" id="pnlMonth">—</span><span class="delta muted">month</span></div>
        </div>
        <div class="hint small muted">Unrealized: <span id="unrealized" class="mono">—</span></div>
      </div>

      <!-- Calendar P&L -->
      <div class="panel">
        <h2>Daily P&amp;L (last 14d)</h2>
        <div class="calendar" id="cal"></div>
      </div>
    </div>

    <!-- Recent Fills Table -->
    <div class="panel" style="margin-top:16px;">
      <h2>Recent Fills</h2>
      <div class="toolbar small">
        <span class="muted">Source: <code>/orders/recent?status=all&limit=200</code></span>
        <span class="muted">•</span>
        <span class="muted">Auto-refresh: 20s</span>
      </div>
      <div style="overflow:auto; max-height: 520px; margin-top:10px;">
        <table>
          <thead>
            <tr>
              <th style="min-width:280px;">Description</th>
              <th>Type</th>
              <th class="right">Qty</th>
              <th class="right">Amount</th>
              <th class="right">Date</th>
            </tr>
          </thead>
          <tbody id="fills"></tbody>
        </table>
      </div>
      <div class="hint small muted">Shows both buys and sells. Amount uses order notional: negative for buys, positive for sells.</div>
    </div>

    <div class="small muted" style="margin-top:12px;">
      Need raw JSON? Try <a href="/orders/recent?status=all&limit=20" target="_blank">/orders/recent</a>,
      <a href="/pnl/summary" target="_blank">/pnl/summary</a>, <a href="/pnl/daily?days=14" target="_blank">/pnl/daily</a>.
    </div>
  </div>

  <script>
    // --- helpers ---
    const $ = sel => document.querySelector(sel);
    const fmtUsd = n => {
      if (n === null || n === undefined || isNaN(n)) return "—";
      try { return new Intl.NumberFormat(undefined,{style:'currency',currency:'USD',maximumFractionDigits:2}).format(n); }
      catch { return `$${(+n).toFixed(2)}`; }
    };
    const parseNum = (v) => { if (v===null||v===undefined) return null; const x = +v; return isNaN(x)? null : x; };
    const parseDate = (s) => { try { return new Date(s); } catch { return null; } };
    const preferTime = (o, keys) => { for (const k of keys) if (o && o[k]) return o[k]; return null; };

    // --- P&L summary ---
    async function loadSummary() {
      const r = await fetch('/pnl/summary');
      const j = await r.json();
      $('#asOf').textContent = `as of ${j.as_of_utc || ''}`;
      const month = j.realized?.month ?? 0;
      const week  = j.realized?.week ?? 0;
      const today = j.realized?.today ?? 0;
      const unreal = j.unrealized ?? 0;
      $('#pnlToday').textContent = fmtUsd(today);
      $('#pnlWeek').textContent  = fmtUsd(week);
      $('#pnlMonth').textContent = fmtUsd(month);
      $('#unrealized').textContent = fmtUsd(unreal);
      $('#pnlToday').className = 'big ' + (today>=0?'pos':'neg');
      $('#pnlWeek').className  = 'big ' + (week>=0?'pos':'neg');
      $('#pnlMonth').className = 'big ' + (month>=0?'pos':'neg');
    }

    // --- Calendar 14d ---
    async function loadDaily() {
      const r = await fetch('/pnl/daily?days=14');
      const j = await r.json();
      const el = $('#cal'); el.innerHTML = '';
      const series = j.series || [];
      for (const d of series) {
        const pnl = +d.pnl || 0;
        const box = document.createElement('div');
        box.className = 'day';
        const date = document.createElement('div'); date.className='d'; date.textContent = d.date;
        const pv = document.createElement('div'); pv.className='p mono ' + (pnl>=0?'pos':'neg'); pv.textContent = fmtUsd(pnl);
        box.appendChild(date); box.appendChild(pv);
        el.appendChild(box);
      }
    }

    // --- Recent fills from /orders/recent ---
    function coalesceSymbol(o){ return o.symbol || o.asset_symbol || ''; }
    function sideToSign(side){ return (String(side||'').toLowerCase()==='sell') ? +1 : -1; }

    function mkDesc(side, qty, sym) {
      const s = (String(side||'').toLowerCase()==='sell')?'Sell':'Buy';
      const q = qty!=null ? (+qty).toFixed(8).replace(/0+$/,'').replace(/\.$/,'') : '0';
      return `${s} ${q} ${sym}`;
    }

    function typeFromStatus(status){
      const s = String(status||'').toLowerCase();
      if (s==='filled') return 'FILL';
      if (s==='partially_filled') return 'PARTIAL';
      return (status||'').toString().toUpperCase();
    }

    function pickQty(o){
      const cands = [o.filled_qty, o.qty, o.quantity];
      for (const c of cands) if (c!==undefined && c!==null && !isNaN(+c)) return +c;
      return null;
    }

    function pickNotional(o){
      const cands = [o.notional, o.amount, o.order_notional];
      for (const c of cands) if (c!==undefined && c!==null && !isNaN(+c)) return +c;
      return null;
    }

    function pickTime(o){
      const s = preferTime(o, ['filled_at','completed_at','submitted_at','created_at','timestamp']);
      return parseDate(s);
    }

    async function loadFills() {
      const r = await fetch('/orders/recent?status=all&limit=200');
      let j = await r.json();
      // Flatten shape: either { value:[...] } or just [...]
      let arr = Array.isArray(j) ? j : (Array.isArray(j.value) ? j.value : []);
      // Filter to orders that are meaningful to display (filled or buy/sell intents)
      arr = arr.filter(o => o && (o.status || o.side));
      // Map → table rows
      const rows = arr.map(o => {
        const sym = coalesceSymbol(o);
        const qty = pickQty(o);
        const side = (o.side || '').toLowerCase();
        const notional = pickNotional(o);
        const dt = pickTime(o);
        const desc = mkDesc(side, qty, sym);
        const typ = typeFromStatus(o.status);
        // Amount: buy is negative cash outflow; sell positive inflow
        const amt = (notional!=null) ? sideToSign(side) * Math.abs(+notional) : null;
        return {
          desc, type: typ, qty, amt,
          date: dt ? dt : null,
          sortKey: dt ? dt.getTime() : 0
        };
      }).sort((a,b)=> b.sortKey - a.sortKey);

      // Render
      const tbody = $('#fills'); tbody.innerHTML = '';
      for (const r of rows) {
        const tr = document.createElement('tr');
        const d = document.createElement('td'); d.textContent = r.desc; d.className='mono';
        const t = document.createElement('td'); t.innerHTML = `<span class="badge">${r.type}</span>`;
        const q = document.createElement('td'); q.className='right mono'; q.textContent = (r.qty!=null)? r.qty.toFixed(8).replace(/0+$/,'').replace(/\.$/,'') : '—';
        const a = document.createElement('td'); a.className='right mono ' + (r.amt>=0?'pos':'neg'); a.textContent = (r.amt!=null)? fmtUsd(r.amt) : '—';
        const dt = document.createElement('td'); dt.className='right small muted mono'; dt.textContent = r.date ? r.date.toLocaleString() : '—';
        tr.appendChild(d); tr.appendChild(t); tr.appendChild(q); tr.appendChild(a); tr.appendChild(dt);
        tbody.appendChild(tr);
      }
    }

    async function boot(){
      // Version stamp (optional, we just show what’s in the title unless /health/versions says otherwise)
      try {
        const hv = await (await fetch('/health/versions')).json();
        if (hv?.app) { document.getElementById('appVersion').textContent = `v${hv.app}`; }
      } catch {}
      await Promise.all([loadSummary(), loadDaily(), loadFills()]);
      setInterval(loadSummary, 20000);
      setInterval(loadDaily, 60000);
      setInterval(loadFills, 20000);
    }
    boot();
  </script>
</body>
</html>
{% endraw %}"""

@app.get("/")
def index():
    return render_template_string(DASH_HTML)

if __name__ == "__main__":
    port = int(os.environ.get("PORT","10000"))
    app.run(host="0.0.0.0", port=port)
