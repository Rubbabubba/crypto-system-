#!/usr/bin/env python3
# app.py — Crypto System v1.7.3 (v1.7.2 + perf_4h + tiles for c1..c6)
# - Preserves previous routes:
#     /                (dashboard)
#     /orders/recent
#     /orders/attribution
#     /pnl/summary
# - Adds:
#     /orders/performance_4h      <-- NEW (used by dashboard tiles/tables)
#     /strategies/run?key=c1..c6  <-- Lightweight “Run now” action
#
# Notes:
# * Reads orders from TRADES_JSON_PATH (if present) or falls back to in-memory sample.
# * “Strategy” is taken from order["strategy"] or first tag in order["tags"].
# * PnL: uses order.get("realized_pnl") if present; otherwise infers
#   sign by side (SELL positive, BUY negative) for crude aggregation.

import json
import os
import threading
import time
from collections import defaultdict, Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from flask import Flask, Response, jsonify, request

APP_VERSION = "v1.7.3"

# --------------------------------------------
# Utilities
# --------------------------------------------

def now_utc():
    return datetime.now(timezone.utc)

def to_iso(dt: datetime) -> str:
    # Render RFC3339-ish timestamps cleanly
    return dt.astimezone(timezone.utc).isoformat()

def parse_dt(s: str) -> datetime | None:
    try:
        # accept ISO or seconds/ms epoch
        if s.isdigit():
            # epoch seconds
            return datetime.fromtimestamp(int(s), tz=timezone.utc)
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def coalesce_strategy(o: Dict[str, Any]) -> str:
    # preferred
    s = o.get("strategy")
    if s:
        return str(s)
    # sometimes orders have tags: ["c2", ...]
    tags = o.get("tags") or []
    if isinstance(tags, list) and tags:
        return str(tags[0])
    # context.strategy (some systems)
    ctx = o.get("context") or {}
    if isinstance(ctx, dict):
        s2 = ctx.get("strategy")
        if s2:
            return str(s2)
    return "unknown"

def order_ts(o: Dict[str, Any]) -> datetime | None:
    # try typical fields
    for k in ("filled_at", "submitted_at", "timestamp", "ts", "created_at"):
        if k in o and o[k]:
            dt = parse_dt(str(o[k]))
            if dt:
                return dt
    return None

def order_side(o: Dict[str, Any]) -> str:
    s = (o.get("side") or "").lower()
    if s in ("buy", "sell"):
        return s
    # try type
    t = (o.get("type") or "").lower()
    if t in ("buy", "sell"):
        return t
    return "buy"

def order_qty(o: Dict[str, Any]) -> float:
    for k in ("qty", "quantity", "filled_qty", "filled_quantity", "size"):
        v = o.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    return 0.0

def order_price(o: Dict[str, Any]) -> float:
    for k in ("filled_avg_price", "price", "avg_price", "execution_price"):
        v = o.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    return 0.0

def order_pnl(o: Dict[str, Any]) -> float:
    # If realized PnL provided, prefer that
    rpnl = o.get("realized_pnl")
    if isinstance(rpnl, (int, float)):
        return float(rpnl)

    # Fallback heuristic (very rough):
    # Treat SELL as positive notional, BUY as negative notional.
    # This is not exact PnL — but gives directionality in absence of fills.
    side = order_side(o)
    qty = order_qty(o)
    px = order_price(o)
    notional = qty * px
    if side == "sell":
        return abs(notional)
    return -abs(notional)

def within_last_hours(dt: datetime | None, hours: int) -> bool:
    if not dt:
        return False
    return dt >= (now_utc() - timedelta(hours=hours))

# --------------------------------------------
# Data source
# --------------------------------------------

TRADES_JSON_PATH = os.environ.get("TRADES_JSON_PATH", "/mnt/data/trades.json")

def _load_orders_from_file() -> List[Dict[str, Any]]:
    if not TRADES_JSON_PATH or not os.path.exists(TRADES_JSON_PATH):
        return []
    try:
        with open(TRADES_JSON_PATH, "r") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            # some APIs wrap as {"orders":[...]}
            if isinstance(data, dict):
                if "orders" in data and isinstance(data["orders"], list):
                    return data["orders"]
        return []
    except Exception:
        return []

_SAMPLE_ORDERS = [
    # Minimal mock if no file is present
    {
        "id": "m1",
        "symbol": "BTCUSD",
        "side": "buy",
        "qty": 0.01,
        "price": 60000.0,
        "filled_at": to_iso(now_utc() - timedelta(minutes=30)),
        "strategy": "c1",
        "realized_pnl": -1.2,
    },
    {
        "id": "m2",
        "symbol": "BTCUSD",
        "side": "sell",
        "qty": 0.01,
        "price": 60300.0,
        "filled_at": to_iso(now_utc() - timedelta(minutes=20)),
        "strategy": "c1",
        "realized_pnl": 2.7,
    },
    {
        "id": "m3",
        "symbol": "ETHUSD",
        "side": "sell",
        "qty": 0.1,
        "price": 2600.0,
        "filled_at": to_iso(now_utc() - timedelta(hours=2)),
        "strategy": "c5",
        "realized_pnl": 5.0,
    },
    {
        "id": "m4",
        "symbol": "SOLUSD",
        "side": "buy",
        "qty": 1.0,
        "price": 150.0,
        "filled_at": to_iso(now_utc() - timedelta(hours=3, minutes=40)),
        "strategy": "c6",
        "realized_pnl": -3.0,
    },
]

def get_orders(limit: int = 200) -> List[Dict[str, Any]]:
    data = _load_orders_from_file()
    if not data:
        data = _SAMPLE_ORDERS
    # normalize timestamps to ISO
    for o in data:
        dt = order_ts(o)
        if dt:
            o["_ts"] = to_iso(dt)
    # Sort newest -> oldest
    data_sorted = sorted(data, key=lambda x: parse_dt(x.get("_ts") or x.get("filled_at") or x.get("submitted_at") or x.get("created_at") or x.get("timestamp") or ""), reverse=True)
    return data_sorted[:limit]

# --------------------------------------------
# Aggregations
# --------------------------------------------

def summarize_attribution(orders: List[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {"by_strategy": [], "totals": {}}
    by = defaultdict(list)
    for o in orders:
        by[coalesce_strategy(o)].append(o)

    league = []
    total_pnl = 0.0
    total_orders = 0
    for s, group in by.items():
        wins = 0
        losses = 0
        pnl_sum = 0.0
        for g in group:
            pnl = order_pnl(g)
            pnl_sum += pnl
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
        total_pnl += pnl_sum
        total_orders += len(group)
        league.append({
            "strategy": s,
            "orders": len(group),
            "wins": wins,
            "losses": losses,
            "pnl": round(pnl_sum, 6),
        })

    league.sort(key=lambda r: r["pnl"], reverse=True)
    out["by_strategy"] = league
    out["totals"] = {
        "orders": total_orders,
        "pnl": round(total_pnl, 6),
        "strategies": len(league),
        "updated_at": to_iso(now_utc()),
    }
    return out

def summarize_performance_4h(orders: List[Dict[str, Any]]) -> Dict[str, Any]:
    cutoff = now_utc() - timedelta(hours=4)
    filtered = [o for o in orders if (dt := order_ts(o)) and dt >= cutoff]
    by = defaultdict(list)
    for o in filtered:
        by[coalesce_strategy(o)].append(o)

    cards = []
    for s, group in by.items():
        wins = sum(1 for g in group if order_pnl(g) > 0)
        losses = sum(1 for g in group if order_pnl(g) < 0)
        pnl_sum = round(sum(order_pnl(g) for g in group), 6)
        cards.append({
            "strategy": s,
            "orders": len(group),
            "wins": wins,
            "losses": losses,
            "pnl": pnl_sum,
        })
    cards.sort(key=lambda r: r["strategy"])
    return {
        "updated_at": to_iso(now_utc()),
        "count": len(filtered),
        "by_strategy": cards,
        "cutoff": to_iso(cutoff),
    }

def summarize_pnl(orders: List[Dict[str, Any]]) -> Dict[str, Any]:
    # simple rollups: today & last_24h
    now = now_utc()
    start_today = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    last24 = now - timedelta(hours=24)

    def bucket(after_dt):
        pnl = 0.0
        n = 0
        for o in orders:
            dt = order_ts(o)
            if dt and dt >= after_dt:
                pnl += order_pnl(o)
                n += 1
        return {"orders": n, "pnl": round(pnl, 6)}

    return {
        "version": APP_VERSION,
        "updated_at": to_iso(now),
        "today": bucket(start_today),
        "last_24h": bucket(last24),
        "lifetime": {
            "orders": len(orders),
            "pnl": round(sum(order_pnl(o) for o in orders), 6)
        }
    }

# --------------------------------------------
# Flask app
# --------------------------------------------

app = Flask(__name__)

# ----------------- HTML ---------------------

DASH_HTML = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Crypto System {APP_VERSION}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    :root {{
      --bg:#0c1118; --panel:#111826; --ink:#e5e7eb; --muted:#9ca3af; --accent:#60a5fa; --win:#16a34a; --loss:#dc2626; --warn:#f59e0b;
    }}
    html,body{{margin:0;padding:0;background:var(--bg);color:var(--ink);font-family:Inter,system-ui,Segoe UI,Arial,sans-serif}}
    .wrap{{max-width:1180px;margin:0 auto;padding:18px}}
    h1{{font-size:22px; margin:0 0 8px 0}}
    .sub{{color:var(--muted);font-size:12px;margin-bottom:16px}}
    .grid{{display:grid;grid-template-columns:repeat(12,1fr);gap:12px}}
    .card{{background:var(--panel);border-radius:10px;padding:12px}}
    .card h3{{margin:0 0 8px 0;font-size:14px}}
    .kpi{{display:flex;gap:14px;flex-wrap:wrap}}
    .pill{{display:inline-flex;align-items:center;padding:4px 8px;border-radius:999px;font-size:12px;background:#0b1220;border:1px solid #1f2a44;color:var(--muted)}
    .pill.good{{border-color:#224a2a;color:#a7f3d0}}
    .pill.bad{{border-color:#4a1f28;color:#fecaca}}
    .pill.warn{{border-color:#4a3a1f;color:#fde68a}}
    table{{width:100%;border-collapse:collapse;font-size:12px}}
    th,td{{padding:8px;border-bottom:1px solid #1f2937}}
    th{{text-align:left;color:var(--muted);font-weight:600}}
    .right{{text-align:right}}
    .btn{{background:transparent;border:1px solid #24324a;color:var(--ink);padding:6px 8px;border-radius:8px;cursor:pointer;font-size:12px}}
    .btn:hover{{background:#0f1729}}
    .row{{display:flex;gap:12px;align-items:center;flex-wrap:wrap}}
    .tile{{background:#0c1424;border:1px solid #1b2a45;border-radius:10px;padding:10px;display:flex;flex-direction:column;gap:10px}}
    .tile .hdr{{display:flex;justify-content:space-between;align-items:center}}
    .tile .name{{font-weight:600}}
    .green{{color:var(--win)}} .red{{color:var(--loss)}}
    .mono{{font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace}}
    .muted{{color:var(--muted)}}
    canvas{{width:100%;height:160px;background:transparent}}
    .foot{{margin-top:12px;color:var(--muted);font-size:11px}}
    @media (max-width:900px) {{
      .grid{{grid-template-columns: repeat(6,1fr)}}
    }}
    @media (max-width:640px) {{
      .grid{{grid-template-columns: repeat(2,1fr)}}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Crypto System <span class="muted">{APP_VERSION}</span></h1>
    <div class="sub">Live overview · <span id="updated"></span></div>

    <!-- Row: Strategy tiles (c1..c6) -->
    <div class="grid" style="margin-bottom:12px">
      <!-- 12 columns: each tile spans 2 cols => 6 tiles -->
      <div class="card tile" style="grid-column: span 2" id="tile-c1">
        <div class="hdr"><div class="name">Strategy c1</div><span class="pill" id="pill-c1">idle</span></div>
        <div class="row"><button class="btn" onclick="runNow('c1')">Run now</button><div class="muted" id="tile-c1-meta"></div></div>
      </div>
      <div class="card tile" style="grid-column: span 2" id="tile-c2">
        <div class="hdr"><div class="name">Strategy c2</div><span class="pill" id="pill-c2">idle</span></div>
        <div class="row"><button class="btn" onclick="runNow('c2')">Run now</button><div class="muted" id="tile-c2-meta"></div></div>
      </div>
      <div class="card tile" style="grid-column: span 2" id="tile-c3">
        <div class="hdr"><div class="name">Strategy c3</div><span class="pill" id="pill-c3">idle</span></div>
        <div class="row"><button class="btn" onclick="runNow('c3')">Run now</button><div class="muted" id="tile-c3-meta"></div></div>
      </div>
      <div class="card tile" style="grid-column: span 2" id="tile-c4">
        <div class="hdr"><div class="name">Strategy c4</div><span class="pill" id="pill-c4">idle</span></div>
        <div class="row"><button class="btn" onclick="runNow('c4')">Run now</button><div class="muted" id="tile-c4-meta"></div></div>
      </div>
      <div class="card tile" style="grid-column: span 2" id="tile-c5">
        <div class="hdr"><div class="name">Strategy c5</div><span class="pill" id="pill-c5">idle</span></div>
        <div class="row"><button class="btn" onclick="runNow('c5')">Run now</button><div class="muted" id="tile-c5-meta"></div></div>
      </div>
      <div class="card tile" style="grid-column: span 2" id="tile-c6">
        <div class="hdr"><div class="name">Strategy c6</div><span class="pill" id="pill-c6">idle</span></div>
        <div class="row"><button class="btn" onclick="runNow('c6')">Run now</button><div class="muted" id="tile-c6-meta"></div></div>
      </div>
    </div>

    <div class="grid">
      <div class="card" style="grid-column: span 8">
        <h3>Performance — Last 4 Hours (by strategy)</h3>
        <div class="kpi" id="perf4h-cards"></div>
        <div class="foot">Cutoff: <span id="perf_cutoff"></span></div>
      </div>
      <div class="card" style="grid-column: span 4">
        <h3>Attribution (lifetime)</h3>
        <div id="attr_table"></div>
      </div>

      <div class="card" style="grid-column: span 12">
        <h3>Recent Orders</h3>
        <div id="orders_table"></div>
      </div>
    </div>

    <div class="foot">Auto-refreshes every 60s · <span id="clock"></span></div>
  </div>

<script>
const fmt = (n, d=2) => {
  if (n === null || n === undefined || Number.isNaN(n)) return "—";
  return Number(n).toLocaleString(undefined, {maximumFractionDigits:d});
};
function setPill(id, text, cls) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = text;
  el.className = "pill " + (cls||"");
}

async function runNow(key) {{
  setPill("pill-"+key, "running…", "warn");
  try {{
    const r = await fetch(`/strategies/run?key=${{encodeURIComponent(key)}}`, {{ method: "POST" }});
    const j = await r.json();
    if (j.ok) setPill("pill-"+key, "done", "good");
    else setPill("pill-"+key, "error", "bad");
  }} catch (e) {{
    setPill("pill-"+key, "error", "bad");
  }}
  setTimeout(()=>setPill("pill-"+key, "idle", ""), 4000);
}}

function renderAttr(data) {{
  const rows = data.by_strategy.map(r => `
    <tr>
      <td class="mono">${{r.strategy}}</td>
      <td class="right mono">${{fmt(r.orders,0)}}</td>
      <td class="right mono green">${{fmt(r.wins,0)}}</td>
      <td class="right mono red">${{fmt(r.losses,0)}}</td>
      <td class="right mono" style="color:${{r.pnl>=0?'#16a34a':'#dc2626'}}">${{fmt(r.pnl, 4)}}</td>
    </tr>`).join("");
  const html = `
    <table>
      <thead><tr><th>Strategy</th><th class="right">Orders</th><th class="right">Wins</th><th class="right">Losses</th><th class="right">PnL</th></tr></thead>
      <tbody>${{rows}}</tbody>
    </table>`;
  document.getElementById("attr_table").innerHTML = html;
}}

function renderPerf4h(data) {{
  document.getElementById("perf_cutoff").textContent = data.cutoff || "";
  const cards = (data.by_strategy||[]).map(r => `
    <div class="tile" style="min-width:160px">
      <div class="hdr"><div class="name mono">${{r.strategy}}</div><span class="pill">${{fmt(r.orders,0)}} orders</span></div>
      <div class="row"><span class="green mono">W ${{fmt(r.wins,0)}}</span><span class="red mono">L ${{fmt(r.losses,0)}}</span></div>
      <div class="mono" style="font-size:13px;color:${{r.pnl>=0?'#16a34a':'#dc2626'}}">PnL ${{fmt(r.pnl,4)}}</div>
    </div>
  `).join("");
  document.getElementById("perf4h-cards").innerHTML = cards || "<div class='muted'>No orders in last 4 hours.</div>";
}}

function renderOrders(data) {{
  const rows = data.map(o => `
    <tr>
      <td class="mono">${{o._ts || o.filled_at || o.submitted_at || ''}}</td>
      <td class="mono">${{o.symbol || ''}}</td>
      <td class="mono">${{(o.side||'').toUpperCase()}}</td>
      <td class="right mono">${{fmt(o.qty||o.quantity||o.filled_qty, 6)}}</td>
      <td class="right mono">${{fmt(o.filled_avg_price||o.price||o.avg_price, 6)}}</td>
      <td class="mono">${{(o.strategy || (o.tags && o.tags[0]) || (o.context && o.context.strategy) || 'unknown')}}</td>
      <td class="right mono">${{fmt(o.realized_pnl, 6)}}</td>
    </tr>`).join("");
  const html = `
    <table>
      <thead>
        <tr><th>Time</th><th>Symbol</th><th>Side</th><th class="right">Qty</th><th class="right">Price</th><th>Strategy</th><th class="right">Realized PnL</th></tr>
      </thead>
      <tbody>${{rows}}</tbody>
    </table>`;
  document.getElementById("orders_table").innerHTML = html;
}}

async function refreshAll() {{
  try {{
    const [recent, attr, pnl, perf4h] = await Promise.all([
      fetch('/orders/recent?limit=500').then(r=>r.json()),
      fetch('/orders/attribution').then(r=>r.json()),
      fetch('/pnl/summary').then(r=>r.json()),
      fetch('/orders/performance_4h').then(r=>r.json())
    ]);
    renderOrders(recent);
    renderAttr(attr);
    renderPerf4h(perf4h);
    document.getElementById('updated').textContent = pnl.updated_at || '';
  }} catch(e) {{
    console.error(e);
  }}
}}
setInterval(()=>{{
  document.getElementById('clock').textContent = new Date().toLocaleTimeString();
}}, 1000);

refreshAll();
setInterval(refreshAll, 60000);
</script>
</body>
</html>
"""

# ----------------- ROUTES -------------------

@app.route("/")
def dashboard():
    return Response(DASH_HTML, mimetype="text/html")

@app.get("/orders/recent")
def orders_recent():
    try:
        limit = int(request.args.get("limit", 200))
    except Exception:
        limit = 200
    return jsonify(get_orders(limit=limit))

@app.get("/orders/attribution")
def orders_attribution():
    days = request.args.get("days")
    orders = get_orders(limit=2000)
    if days:
        try:
            d = int(days)
            cutoff = now_utc() - timedelta(days=d)
            orders = [o for o in orders if (dt := order_ts(o)) and dt >= cutoff]
        except Exception:
            pass
    return jsonify(summarize_attribution(orders))

@app.get("/orders/performance_4h")
def orders_performance_4h():
    # NEW: used by dashboard “Performance — Last 4 Hours”
    orders = get_orders(limit=2000)
    return jsonify(summarize_performance_4h(orders))

@app.get("/pnl/summary")
def pnl_summary():
    orders = get_orders(limit=2000)
    return jsonify(summarize_pnl(orders))

@app.post("/strategies/run")
def strategies_run():
    # lightweight stub that marks a strategy "run" — you can wire to real worker if needed
    key = (request.args.get("key") or "").strip()
    ok = key in {"c1","c2","c3","c4","c5","c6"}
    status = {"ok": ok, "key": key, "ts": to_iso(now_utc())}
    return jsonify(status), (200 if ok else 400)

# --------------------------------------------
# Optional: tiny scheduler heartbeat (logs)
# --------------------------------------------

SCHED_ENABLED = os.environ.get("SCHED_ENABLED", "1") not in ("0", "false", "False")
SCHED_INTERVAL = int(os.environ.get("SCHED_INTERVAL_SECS", "30"))

def _heartbeat():
    while True:
        try:
            time.sleep(SCHED_INTERVAL)
            print(f"{to_iso(now_utc())} INFO:app:Scheduler tick: running all strategies (dry=0)")
        except Exception:
            time.sleep(SCHED_INTERVAL)

if SCHED_ENABLED:
    threading.Thread(target=_heartbeat, daemon=True).start()

# --------------------------------------------
# Main
# --------------------------------------------

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    print(f"{to_iso(now_utc())} INFO:app:Starting Crypto System {APP_VERSION} on :{port}")
    app.run(host="0.0.0.0", port=port)