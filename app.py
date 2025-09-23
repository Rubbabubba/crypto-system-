# app.py  — v1.8.7
import os
import time
import json
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict, deque

import pandas as pd
from flask import Flask, request, jsonify, Response, render_template_string

# ---- Versions ----
APP_VERSION = "1.8.7"

# ---- Broker (Alpaca paper) ----
from services.exchange_exec import Broker as AlpacaBroker

broker = AlpacaBroker(
    api_key=os.environ.get("APCA_API_KEY_ID", ""),
    secret_key=os.environ.get("APCA_API_SECRET_KEY", ""),
    trading_base=os.environ.get("APCA_API_BASE_URL", "https://paper-api.alpaca.markets"),
    data_base=os.environ.get("DATA_API_BASE_URL", "https://data.alpaca.markets"),
)

# ---- Symbols / config ----
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

# ---- Load strategies (each exposes: NAME, VERSION, run(df_map, params, positions)) ----
import c1, c2, c3, c4, c5, c6
STRATS = [c1, c2, c3, c4, c5, c6]
STRAT_MAP = {m.NAME: m for m in STRATS}

# ---- Flask ----
app = Flask(__name__)

# ---- Helpers ----
UTC = timezone.utc

def _now_utc():
    return datetime.now(tz=UTC)

def _parse_dt_utc(s):
    if not s:
        return None
    try:
        # Accept "Z" and offsets
        return datetime.fromisoformat(s.replace("Z","+00:00")).astimezone(UTC)
    except Exception:
        return None

def _safe_float(x, default=0.0):
    try:
        return float(x)
    except Exception:
        return default

def strat_from_coid(coid):
    if not coid or "-" not in coid:
        return "unknown"
    return coid.split("-",1)[0].lower()

def compute_realized_fifo(orders, start_dt=None, end_dt=None):
    """
    Compute realized P&L by symbol with a simple FIFO over filled orders.
    Returns (realized_total, realized_by_day: dict[date->float], realized_by_strat: dict[strat->summary])
    """
    # Filter by time window and 'filled'
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

    # FIFO per symbol
    realized_total = 0.0
    realized_by_day = defaultdict(float)
    realized_by_strat = defaultdict(lambda: {"count":0,"wins":0,"losses":0,"realized":0.0})

    inv = defaultdict(lambda: deque())  # symbol -> deque of (qty,px)
    for row in sorted(filled, key=lambda r: r["t"]):
        d = row["t"].date()
        sym, side, q, px = row["symbol"], row["side"], row["qty"], row["px"]
        strat = strat_from_coid(row.get("coid"))

        if side == "buy":
            inv[sym].append([q, px])
            # no realized P&L yet
            continue

        # side == sell
        qty = q
        pnl = 0.0
        while qty > 0 and inv[sym]:
            q0, px0 = inv[sym][0]
            take = min(qty, q0)
            pnl += (px - px0) * take
            q0 -= take
            qty -= take
            if q0 <= 1e-12:  # consumed
                inv[sym].popleft()
            else:
                inv[sym][0][0] = q0
        # If inventory empty and still qty left, treat remaining as opened short then closed immediately (zero cost) — but we’ll ignore for crypto long-only flow.

        realized_total += pnl
        realized_by_day[d] += pnl
        # attribute to strategy if present
        realized_by_strat[strat]["count"] += 1
        if pnl > 0: realized_by_strat[strat]["wins"] += 1
        if pnl < 0: realized_by_strat[strat]["losses"] += 1
        realized_by_strat[strat]["realized"] += pnl

    return realized_total, realized_by_day, realized_by_strat

def positions_unrealized(positions):
    total = 0.0
    for p in positions or []:
        # prefer broker-provided unrealized_pl; fallback to (market_value - cost)
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

# ---- Routes ----

@app.get("/health/versions")
def health_versions():
    systems = {m.NAME: getattr(m, "VERSION", "unknown") for m in STRATS}
    return jsonify({
        "app": APP_VERSION,
        "exchange": "alpaca",
        "trading_base": broker.trading_base,
        "data_base": broker.data_base,
        "systems": systems,
    })

@app.get("/routes")
def list_routes():
    rv = []
    for r in app.url_map.iter_rules():
        rv.append(str(r))
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
    # ?days=1
    days = int(request.args.get("days","1"))
    end = _now_utc()
    start = (end - timedelta(days=days)).replace(microsecond=0)
    try:
        orders = broker.list_orders(status="all", limit=1000)  # most recent
        _, _, by_strat = compute_realized_fifo(orders, start_dt=start, end_dt=end)
        # ensure all strats appear (even if 0)
        for name in ["c1","c2","c3","c4","c5","c6","unknown"]:
            by_strat.setdefault(name, {"count":0,"wins":0,"losses":0,"realized":0.0})
        return jsonify({"per_strategy": by_strat, "version": APP_VERSION})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.get("/pnl/summary")
def pnl_summary():
    try:
        now = _now_utc()
        orders = broker.list_orders(status="all", limit=2000)  # recent window
        # daily/week/month realized using FIFO
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

        # trades summary (30d)
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
    # returns per-day realized over last N days (overall portfolio)
    days = int(request.args.get("days","30"))
    end = _now_utc()
    start = (end - timedelta(days=days)).replace(microsecond=0)
    try:
        orders = broker.list_orders(status="all", limit=5000)
        _, by_day, _ = compute_realized_fifo(orders, start_dt=start, end_dt=end)
        # build series for each day inclusive
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
    # lightweight probe: return row counts per symbol
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
        # never 500 this endpoint; return zeros
        for s in [s.strip() for s in syms.split(",") if s.strip()] or get_symbols():
            rows[s] = 0
        return jsonify({"rows": rows, "error": str(e)})

# ---- SCAN endpoint (kept for API completeness; dashboard no longer uses it) ----
@app.post("/scan/<name>")
def scan(name):
    name = name.lower()
    if name not in STRAT_MAP:
        return jsonify({"ok": False, "error": f"unknown strategy '{name}'"}), 404

    dry = request.args.get("dry","1") != "0"
    timeframe = request.args.get("timeframe","1Min")
    limit = int(request.args.get("limit","600"))
    notional = _safe_float(request.args.get("notional","5"), 5.0)

    # collect extra params for the strategy
    params = {k:v for k,v in request.args.items() if k not in ("dry","timeframe","limit","notional")}
    symbols = get_symbols()

    try:
        pos = broker.list_positions()
        long_map = { (p.get("symbol") or p.get("asset_symbol")): _safe_float(p.get("qty") or p.get("quantity")) for p in pos }

        df_map = broker.get_bars(symbols, timeframe=timeframe, limit=limit, merge=False)
        strat = STRAT_MAP[name]
        results = strat.run(df_map, params, long_map)

        # place orders if needed
        placed = []
        if not dry:
            now_epoch = int(time.time())
            for r in results:
                action = (r.get("action") or "").lower()
                symbol = r.get("symbol")
                if action not in ("buy","sell") or not symbol:
                    continue
                # skip duplicate/inverted actions based on position
                qty_have = long_map.get(symbol, 0.0)
                if action == "buy" and qty_have > 0:  # already long
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

# ---- Background automation (runs all strategies every N seconds) ----
_last_bar_ts = {}  # (symbol, timeframe) -> last bar timestamp we processed

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

            # Dedupe per latest bar time, so we don’t spam per cycle
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
                    # dedupe on (symbol, timeframe) last bar
                    key = (strat.NAME, symbol, timeframe)
                    last_t = _last_bar_ts.get(key)
                    if last_t == latest_by_symbol[symbol]:
                        continue
                    # Position guard
                    qty_have = long_map.get(symbol, 0.0)
                    if action == "buy" and qty_have > 0:
                        continue
                    if action == "sell" and qty_have <= 0:
                        continue
                    # place paper order
                    coid = f"{strat.NAME}-{symbol.replace('/','')}-{now_epoch}"
                    broker.submit_order(symbol=symbol, side=action, notional=notional,
                                        client_order_id=coid, time_in_force="gtc", type="market")
                    _last_bar_ts[key] = latest_by_symbol[symbol]

        except Exception as e:
            # keep running no matter what
            pass
        time.sleep(interval)

# Start background worker
threading.Thread(target=autorun_worker, daemon=True).start()

# ---- Dashboard (snapshot only, no manual scan buttons) ----
DASH_HTML = r"""{% raw %}
<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Crypto System — Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet"/>
<style>
  :root { --bg:#0f1320; --card:#151a2c; --text:#e6eefc; --muted:#9db2d7; --green:#1db954; --red:#ff4d4f; --amber:#ffb020; }
  *{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:Inter,system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial}
  .wrap{max-width:1100px;margin:32px auto;padding:0 16px}
  h1{margin:0 0 8px;font-size:26px}
  .sub{color:var(--muted);margin-bottom:24px}
  .grid{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}
  .card{background:var(--card);border-radius:14px;padding:16px;box-shadow:0 8px 24px rgba(0,0,0,.25)}
  .span-4{grid-column:span 4} .span-8{grid-column:span 8} .span-12{grid-column:span 12}
  .kpi{display:flex;align-items:center;justify-content:space-between}
  .kpi .v{font-size:28px;font-weight:700}
  .kpi .l{color:var(--muted);font-size:12px}
  .row{display:flex;gap:16px;flex-wrap:wrap}
  .pill{padding:6px 10px;border-radius:9999px;background:#0e1425;color:var(--muted);font-size:12px}
  table{width:100%;border-collapse:collapse}
  th,td{padding:8px 6px;border-bottom:1px solid rgba(255,255,255,.06);font-size:14px}
  th{color:var(--muted);text-align:left}
  .pos{color:var(--green);font-weight:600}
  .neg{color:var(--red);font-weight:600}
  .calendar{display:grid;grid-template-columns:repeat(7,1fr);gap:8px}
  .cell{background:#10162a;border-radius:10px;padding:10px;min-height:70px;display:flex;flex-direction:column;justify-content:space-between}
  .date{font-size:11px;color:var(--muted)}
  .pn{font-size:14px;font-weight:700}
  .g{color:var(--green)} .r{color:var(--red)} .z{color:var(--muted)}
  .foot{color:var(--muted);font-size:12px;margin-top:8px}
</style>
</head>
<body>
<div class="wrap">
  <h1>Crypto System — Dashboard</h1>
  <div class="sub">Snapshot of current and past performance. Automated paper trading enabled via Alpaca.</div>

  <div class="grid">
    <div class="card span-4">
      <div class="kpi"><div class="l">Unrealized</div><div id="k_unr" class="v z">—</div></div>
      <div class="row" style="margin-top:8px">
        <div class="pill">Today: <span id="k_rt">—</span></div>
        <div class="pill">Week: <span id="k_rw">—</span></div>
        <div class="pill">Month: <span id="k_rm">—</span></div>
      </div>
      <div class="foot" id="asof">—</div>
    </div>

    <div class="card span-8">
      <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
        <div style="font-weight:700">Per-Strategy (last 24h)</div>
        <div class="pill" id="appv">—</div>
      </div>
      <table id="tbl_attr">
        <thead><tr><th>Strategy</th><th>Count</th><th>Wins</th><th>Losses</th><th>Realized</th></tr></thead>
        <tbody></tbody>
      </table>
    </div>

    <div class="card span-12">
      <div style="font-weight:700;margin-bottom:8px">Daily Realized P&L (last 30 days)</div>
      <div id="cal" class="calendar"></div>
    </div>
  </div>
</div>

<script>
function fmt(x){ if(x===null||x===undefined) return "—"; const s = Number(x).toFixed(2); const n = Number(x); const cls = n>0?'pos':(n<0?'neg':'z'); return `<span class="${cls}">${s}</span>`; }
function clsPn(x){ const n=Number(x); return n>0?'g':(n<0?'r':'z'); }

async function fetchJSON(u){ const r = await fetch(u); if(!r.ok) throw new Error(await r.text()); return r.json(); }

async function loadAll(){
  try{
    const [sum, attr, daily, hv] = await Promise.all([
      fetchJSON('/pnl/summary'),
      fetchJSON('/orders/attribution?days=1'),
      fetchJSON('/pnl/daily?days=30'),
      fetchJSON('/health/versions')
    ]);

    // KPIs
    document.getElementById('k_unr').innerHTML = fmt(sum.unrealized);
    document.getElementById('k_rt').innerText = (sum.realized?.today ?? 0).toFixed(2);
    document.getElementById('k_rw').innerText = (sum.realized?.week ?? 0).toFixed(2);
    document.getElementById('k_rm').innerText = (sum.realized?.month ?? 0).toFixed(2);
    document.getElementById('asof').innerText = `as of ${sum.as_of_utc}`;
    document.getElementById('appv').innerText = `app ${hv.app}`;

    // Per-strategy
    const tb = document.querySelector('#tbl_attr tbody');
    tb.innerHTML = '';
    const keys = ['c1','c2','c3','c4','c5','c6','unknown'];
    keys.forEach(k=>{
      const v = (attr.per_strategy||{})[k] || {};
      const tr = document.createElement('tr');
      tr.innerHTML = `<td>${k}</td><td>${v.count||0}</td><td>${v.wins||0}</td><td>${v.losses||0}</td><td>${(v.realized||0).toFixed(2)}</td>`;
      tb.appendChild(tr);
    });

    // Calendar
    const cal = document.getElementById('cal'); cal.innerHTML = '';
    (daily.series||[]).forEach(d=>{
      const div = document.createElement('div');
      div.className = 'cell';
      div.innerHTML = `<div class="date">${d.date}</div><div class="pn ${clsPn(d.pnl)}">${Number(d.pnl).toFixed(2)}</div>`;
      cal.appendChild(div);
    });

  }catch(e){
    console.error(e);
  }
}
loadAll();
setInterval(loadAll, 60000); // refresh every minute
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
