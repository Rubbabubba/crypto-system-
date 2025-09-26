# app.py  — v1.8.9 (restored) + normalization + strategy attribution panel
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

# ---------- Bars normalizer (maps columns to open/high/low/close/volume) ----------
from typing import Any, Iterable
def bars_to_df(bars: Any) -> pd.DataFrame:
    """Accepts list[dict] / dict / DataFrame and returns a DataFrame with
    columns: open, high, low, close, volume, ascending index (uses 'timestamp' if present)"""
    if isinstance(bars, pd.DataFrame):
        df = bars.copy()
    elif isinstance(bars, Iterable) and not isinstance(bars, (str, bytes, dict)):
        df = pd.DataFrame(list(bars))
    elif isinstance(bars, dict):
        df = pd.DataFrame(bars)
    else:
        return pd.DataFrame(columns=["open","high","low","close","volume"])

    col_maps = [
        {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume", "t": "timestamp"},
        {"open": "open", "high": "high", "low": "low", "close": "close", "volume": "volume", "timestamp": "timestamp"},
        {"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume", "Timestamp": "timestamp"},
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

# ---------- Broker glue ----------
import broker  # your existing wrapper; must expose the used methods below

# ---------- Utilities ----------
def _safe_float(x, default=0.0):
    try: return float(x)
    except Exception: return default

def _now_utc():
    return datetime.now(tz=UTC).replace(microsecond=0)

# Symbols config (persisted via env or file)
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
    # default seed
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

# ---------- P&L / FIFO realization ----------
def compute_realized_fifo(orders, start_dt=None, end_dt=None):
    """Compute realized P&L FIFO using buy/sell orders within [start_dt, end_dt].
       Returns: (realized_total, realized_by_day, realized_by_strat)"""
    def pick_time(o):
        for k in ("filled_at","completed_at","submitted_at","created_at","timestamp"):
            v = o.get(k)
            if v: 
                try: return datetime.fromisoformat(str(v).replace("Z","+00:00")).astimezone(UTC)
                except Exception: pass
        return None

    inv = defaultdict(deque)   # sym -> deque of [qty, price, strat]
    realized_total = 0.0
    realized_by_day = defaultdict(float)  # 'YYYY-MM-DD' -> pnl
    realized_by_strat = defaultdict(lambda: {"count":0,"wins":0,"losses":0,"realized":0.0})

    # sort by time
    rows = []
    for o in (orders or []):
        t = pick_time(o)
        if not t: continue
        if start_dt and t < start_dt: continue
        if end_dt and t > end_dt: continue
        rows.append((t,o))
    rows.sort(key=lambda x: x[0])

    for t, o in rows:
        sym = o.get("symbol") or o.get("asset_symbol") or ""
        side = (o.get("side") or "").lower()
        qty  = abs(_safe_float(o.get("qty") or o.get("quantity") or o.get("filled_qty"), 0.0))
        notional = _safe_float(o.get("notional"), 0.0)
        price = _safe_float(o.get("filled_avg_price") or (notional/qty if qty>0 else 0.0), 0.0)
        strat = "unknown"
        coid = (o.get("client_order_id") or "")
        m = re.match(r"(c[1-6])[-_]", coid, re.I)
        if m: strat = m.group(1).lower()

        if qty <= 0 or price <= 0: 
            continue

        if side == "buy":
            inv[sym].append([qty, price, strat])
            continue

        if side != "sell":
            continue

        # FIFO match sells
        qsell = qty
        d = t.strftime("%Y-%m-%d")
        pnl = 0.0
        while qsell > 1e-12 and inv[sym]:
            q0, px0, strat0 = inv[sym][0]
            take = min(qsell, q0)
            pnl += (price - px0) * take
            qsell -= take
            q0 -= take
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
    limit  = int(request.args.get("limit","200"))
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

        # unrealized via positions * last
        unrealized = 0.0
        try:
            pos = broker.list_positions()
            last = broker.last_trade_map([p.get("symbol") or p.get("asset_symbol") for p in pos])
            for p in pos:
                sym = p.get("symbol") or p.get("asset_symbol")
                qty = _safe_float(p.get("qty") or p.get("quantity"))
                px  = _safe_float(p.get("avg_entry_price"))
                last_px = _safe_float((last.get(sym) or {}).get("price"))
                if qty and last_px:
                    unrealized += (last_px - px) * qty
        except Exception:
            pass

        return jsonify({
            "as_of_utc": now.isoformat(),
            "realized": {"today": realized_today, "week": realized_week, "month": realized_month},
            "unrealized": unrealized,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/pnl/daily")
def pnl_daily():
    days = int(request.args.get("days","14"))
    try:
        end = _now_utc()
        start = (end - timedelta(days=days)).replace(microsecond=0)
        orders = broker.list_orders(status="all", limit=4000)
        _, by_day, _ = compute_realized_fifo(orders, start_dt=start, end_dt=end)
        series = [{"date": d, "pnl": pnl} for d, pnl in sorted(by_day.items())]
        return jsonify({"series": series, "as_of_utc": end.isoformat()})
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
        # 🔒 normalize bars to the format strategies expect
        df_map = {sym: bars_to_df(b) for sym, b in df_map.items()}

        strat = STRAT_MAP[name]
        results = strat.run(df_map, params, long_map)

        placed = []
        if not dry:
            now_epoch = int(time.time())
            for r in results:
                symbol = r.get("symbol")
                action = r.get("action")
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
    except Exception as e:
        # Log in your hosting environment for traceback; return message here
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
            # 🔒 normalize here too
            df_map = {sym: bars_to_df(b) for sym, b in df_map.items()}

            latest_by_symbol = {}
            for sym, df in df_map.items():
                if df is None or df.empty: 
                    continue
                try:
                    tlast = pd.to_datetime(df.index[-1]).to_pydatetime().replace(tzinfo=UTC)
                except Exception:
                    tlast = None
                if tlast:
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
    html,body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,system-ui,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0}
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
    .mono{font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,"Liberation Mono","Courier New",monospace}
    .calendar{display:grid;grid-template-columns:repeat(7,1fr);gap:8px}
    .day{background:#0c1220;border:1px solid var(--border);border-radius:12px;padding:10px}
    .d{font-size:12px;color:#a7b1c2;margin-bottom:6px}
    .p{font-size:16px}
    footer{color:#9aa6b2;text-align:center;padding:16px}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Crypto System <span id="appVersion" class="pill">v1.8.9</span></h1>

    <div class="grid" style="margin-top:12px">
      <div class="panel">
        <h3>Summary</h3>
        <div class="kpi"><span class="muted">Today</span> <span id="pnlToday" class="big mono">—</span></div>
        <div class="kpi"><span class="muted">Week</span>  <span id="pnlWeek"  class="big mono">—</span></div>
        <div class="kpi"><span class="muted">Month</span> <span id="pnlMonth" class="big mono">—</span></div>
        <div class="kpi"><span class="muted">Unrealized</span> <span id="unrealized" class="big mono">—</span></div>
        <div class="muted" style="margin-top:6px"><span id="asOf" class="mono"></span></div>
      </div>

      <div class="panel">
        <h3>P&amp;L (last 14 days)</h3>
        <div id="cal" class="calendar"></div>
      </div>
    </div>

    <!-- New: Strategies (server) -->
    <div class="panel" style="margin-top:16px">
      <h3>Strategies (server)</h3>
      <div id="strats" class="muted">Loading…</div>
    </div>

    <div class="panel" style="margin-top:16px">
      <h3>Recent fills</h3>
      <table>
        <thead>
          <tr><th>Time</th><th>Side</th><th>Symbol</th><th class="mono">Qty</th><th class="mono">Price</th><th class="mono">Notional</th><th>Client ID</th><th>Status</th></tr>
        </thead>
        <tbody id="fills">
          <tr><td colspan="8" class="muted">Loading…</td></tr>
        </tbody>
      </table>
    </div>

    <footer>
      <div class="muted">Exchange: Alpaca &middot; (we just show what’s in the title unless /health/versions says otherwise)</div>
    </footer>
  </div>

  <script>
    const $ = (sel) => document.querySelector(sel);
    const fmtUsd = (x) => (x>=0?'+':'') + (x||0).toLocaleString(undefined,{style:'currency',currency:'USD',maximumFractionDigits:2});
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

    // --- Strategies (NAME + VERSION) ---
    async function loadStrats() {
      try {
        const hv = await (await fetch('/health/versions')).json();
        if (hv?.app) { document.getElementById('appVersion').textContent = `v${hv.app}`; }
        const keys = Object.keys(hv.systems || {}).sort();
        const rows = keys.map(k => `<tr><td>${k.toUpperCase()}</td><td>v${hv.systems[k]}</td></tr>`).join('');
        const tbl = `<table><thead><tr><th>Strategy</th><th>Version</th></tr></thead><tbody>${rows}</tbody></table>`;
        document.getElementById('strats').innerHTML = tbl;
      } catch (e) {
        document.getElementById('strats').textContent = 'Failed to load strategies';
      }
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
    function sideToSign(side){ return (side||'').toLowerCase()==='sell' ? '-' : '+'; }
    function pickQty(o){ return +((o.qty || o.quantity || o.filled_qty) ?? 0); }
    function pickNotional(o){ return +(o.notional ?? (pickQty(o) * (+o.filled_avg_price || 0))); }
    function pickPx(o){ return +(o.filled_avg_price || 0); }

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
        const px = pickPx(o);
        const ts = pickTime(o);
        const dt = ts ? ts.toISOString().replace('T',' ').replace('Z','') : '';
        const coid = o.client_order_id || '';
        const status = (o.status || '').toUpperCase();
        const sign = sideToSign(side);
        return `<tr>
          <td class="mono">${dt}</td>
          <td>${side.toUpperCase()}</td>
          <td>${sym}</td>
          <td class="mono">${sign}${qty.toFixed(6)}</td>
          <td class="mono">${px ? px.toFixed(6): ''}</td>
          <td class="mono">${notional ? notional.toFixed(2) : ''}</td>
          <td class="mono">${coid}</td>
          <td>${status}</td>
        </tr>`;
      }).join('');
      document.getElementById('fills').innerHTML = rows || `<tr><td colspan="8" class="muted">No recent orders.</td></tr>`;
    }

    async function boot() {
      // (we just show what’s in the title unless /health/versions says otherwise)
      try {
        const hv = await (await fetch('/health/versions')).json();
        if (hv?.app) { document.getElementById('appVersion').textContent = `v${hv.app}`; }
      } catch {}
      await Promise.all([loadSummary(), loadDaily(), loadFills(), loadStrats()]);
      setInterval(loadSummary, 20000);
      setInterval(loadDaily, 60000);
      setInterval(loadFills, 20000);
      setInterval(loadStrats, 60000);
    }
    boot();
  </script>
</body>
</html>
{% endraw %}"""

@app.get("/")
def index():
    return render_template_string(DASH_HTML)

# ---- start background loop if enabled (good for single-process dev) ----
def start_background():
    try:
        enabled = os.environ.get("AUTORUN_ENABLED","false").lower() == "true"
        if not enabled:
            return
        t = threading.Thread(target=autorun_worker, daemon=True)
        t.start()
    except Exception:
        pass

if __name__ == "__main__":
    start_background()
    port = int(os.environ.get("PORT","10000"))
    app.run(host="0.0.0.0", port=port)
