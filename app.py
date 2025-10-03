#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import math
import threading
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

# --------------------------------------------------------------------------------------
# Optional strategy book import (kept graceful)
# --------------------------------------------------------------------------------------
StrategyBook = None
try:
    from strategy_book import StrategyBook as _SB  # type: ignore
    StrategyBook = _SB
except Exception as e:
    print(f"{datetime.now(timezone.utc).isoformat()} WARNING:app:Could not import StrategyBook from strategy_book: {e}")

# --------------------------------------------------------------------------------------
# Simple in-memory store for orders
# --------------------------------------------------------------------------------------

class OrderStore:
    def __init__(self):
        self._lock = threading.RLock()
        self._orders: List[Dict[str, Any]] = []

    def add_order(self, order: Dict[str, Any]) -> None:
        with self._lock:
            self._orders.append(order)

    def recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            return list(sorted(self._orders, key=lambda x: x.get("timestamp", ""), reverse=True))[:limit]

    def all(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._orders)

    def seed_from_file(self, path: str) -> None:
        if not os.path.exists(path):
            return
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for row in data:
                    if "timestamp" in row and isinstance(row["timestamp"], str):
                        try:
                            ts = row["timestamp"]
                            if isinstance(ts, str) and ts.endswith("Z"):
                                row["_ts_dt"] = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                            else:
                                row["_ts_dt"] = datetime.fromisoformat(ts)
                        except Exception:
                            row["_ts_dt"] = datetime.now(timezone.utc)
                    else:
                        row["_ts_dt"] = datetime.now(timezone.utc)
                    self.add_order(row)
        except Exception as e:
            print(f"{datetime.now(timezone.utc).isoformat()} WARNING:app:Failed to seed orders from {path}: {e}")

ORDERS = OrderStore()
ATTACHED_TRADES = os.environ.get("ATTACHED_TRADES_PATH", "/mnt/data/trades.json")
ORDERS.seed_from_file(ATTACHED_TRADES)

# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: datetime) -> str:
    # Always RFC3339-ish with Z for UTC
    return dt.isoformat().replace("+00:00", "Z")

def parse_iso(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts.replace("Z", "+00:00")
    return datetime.fromisoformat(ts)

def pnl_for_order(order: Dict[str, Any]) -> float:
    if "pnl" in order and isinstance(order["pnl"], (int, float)):
        return float(order["pnl"])
    qty = float(order.get("qty", 0) or 0)
    side = str(order.get("side", "")).lower()
    price_in = float(order.get("price_in", order.get("avg_fill_price", 0)) or 0)
    price_out = float(order.get("price_out", order.get("close_price", 0)) or 0)
    if qty == 0 or price_out == 0:
        return 0.0
    delta = price_out - price_in
    if side in ("sell", "short"):
        delta = -delta
    return qty * delta

def within_last_hours(ts_iso: str, hours: int) -> bool:
    try:
        dt = parse_iso(ts_iso)
        return dt >= now_utc() - timedelta(hours=hours)
    except Exception:
        return False

def group_by_strategy(orders: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for o in orders:
        strat = str(o.get("strategy") or o.get("strat") or "unknown")
        out.setdefault(strat, []).append(o)
    return out

def strategy_stats_last_n_hours(orders: List[Dict[str, Any]], hours: int = 4) -> Dict[str, Any]:
    window_orders = [o for o in orders if within_last_hours(str(o.get("timestamp") or o.get("time") or iso(now_utc())), hours)]
    by_strat = group_by_strategy(window_orders)
    result: Dict[str, Any] = {}
    for strat, rows in by_strat.items():
        wins, losses, pnls = 0, 0, []
        for r in rows:
            p = pnl_for_order(r)
            pnls.append(p)
            if p > 0:
                wins += 1
            elif p < 0:
                losses += 1
        result[strat] = {
            "trades": len(rows),
            "wins": wins,
            "losses": losses,
            "win_rate": (wins / len(rows)) if rows else 0.0,
            "total_pnl": sum(pnls),
            "avg_pnl": (sum(pnls) / len(pnls)) if pnls else 0.0,
            "orders": rows,
        }
    all_pnls = [pnl_for_order(o) for o in window_orders]
    result["_ALL"] = {
        "trades": len(window_orders),
        "wins": sum(1 for p in all_pnls if p > 0),
        "losses": sum(1 for p in all_pnls if p < 0),
        "win_rate": (sum(1 for p in all_pnls if p > 0) / len(all_pnls)) if all_pnls else 0.0,
        "total_pnl": sum(all_pnls),
        "avg_pnl": (sum(all_pnls) / len(all_pnls)) if all_pnls else 0.0,
        "orders": window_orders,
    }
    return result

def attribution(orders: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_strat = group_by_strategy(orders)
    out: Dict[str, Any] = {}
    for strat, rows in by_strat.items():
        strat_pnl = sum(pnl_for_order(o) for o in rows)
        out[strat] = {
            "trades": len(rows),
            "pnl": strat_pnl,
            "win_rate": (sum(1 for o in rows if pnl_for_order(o) > 0) / len(rows)) if rows else 0.0,
            "avg_pnl": (strat_pnl / len(rows)) if rows else 0.0,
        }
    return out

# --------------------------------------------------------------------------------------
# Background scheduler
# --------------------------------------------------------------------------------------

SCHEDULER_ENABLED = os.environ.get("SCHEDULER_ENABLED", "1") != "0"
SCHEDULER_INTERVAL_SEC = int(os.environ.get("SCHEDULER_INTERVAL_SEC", "60"))
DEFAULT_SYMBOLS = os.environ.get("DEFAULT_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
DEFAULT_TIMEFRAME = os.environ.get("DEFAULT_TIMEFRAME", "1m")
DEFAULT_LIMIT = int(os.environ.get("DEFAULT_LIMIT", "200"))
DEFAULT_NOTIONAL = float(os.environ.get("DEFAULT_NOTIONAL", "100"))

def _contexts_as_list(ctx_by_name: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert a name->dict mapping into a list of dicts (with 'name'),
    so StrategyBook implementations that iterate contexts work.
    """
    out: List[Dict[str, Any]] = []
    for name, val in ctx_by_name.items():
        if isinstance(val, dict):
            d = {"name": name, **val}
        else:
            d = {"name": name, "value": val}
        out.append(d)
    return out

def _try_scan(book, strat: str, req: Dict[str, Any], ctx_by_name: Dict[str, Any], ctx_list: List[Dict[str, Any]]):
    """
    Try multiple StrategyBook.scan signatures until one works.
    Return a list of intents or [].
    """
    attempts = [
        ("scan(req, ctx_list)",           lambda: book.scan(req, ctx_list)),
        ("scan(req, ctx_by_name)",        lambda: book.scan(req, ctx_by_name)),
        ("scan(strat, req, ctx_list)",    lambda: book.scan(strat, req, ctx_list)),
        ("scan(strat, req, ctx_by_name)", lambda: book.scan(strat, req, ctx_by_name)),
        ("scan(strat, ctx_list)",         lambda: book.scan(strat, ctx_list)),
        ("scan(strat, ctx_by_name)",      lambda: book.scan(strat, ctx_by_name)),
    ]
    last_err = None
    for label, fn in attempts:
        try:
            res = fn()
            # Normalize: must be list-like
            if res is None:
                return []
            if isinstance(res, dict):
                # some books return {'intents': [...]}
                if "intents" in res and isinstance(res["intents"], list):
                    return res["intents"]
                # or a single intent dict
                return [res]
            if not isinstance(res, list):
                # single non-dict item? wrap
                return [res]
            return res
        except Exception as e:
            last_err = (label, e)
            continue
    if last_err:
        label, e = last_err
        print(f"{iso(now_utc())} WARNING:app:scan attempts exhausted; last='{label}' error: {e}")
    return []

def run_all_strategies_once(dry: bool = True) -> None:
    started = now_utc()
    print(f"{iso(started)} INFO:app:Scheduler tick: running all strategies (dry={1 if dry else 0})")

    if StrategyBook is None:
        return

    strategies = ["c1", "c2", "c3", "c4", "c5", "c6"]

    ctx_by_name: Dict[str, Any] = {
        "one": {"tf": DEFAULT_TIMEFRAME, "limit": DEFAULT_LIMIT},
        "default": {"notional": DEFAULT_NOTIONAL},
        "alts": {"universe": DEFAULT_SYMBOLS},
    }
    ctx_list = _contexts_as_list(ctx_by_name)

    book = StrategyBook()  # type: ignore
    for strat in strategies:
        try:
            tf = DEFAULT_TIMEFRAME
            lim = DEFAULT_LIMIT
            notional = DEFAULT_NOTIONAL
            symbols = DEFAULT_SYMBOLS

            req: Dict[str, Any] = {
                "strategy": strat,
                "timeframe": tf,
                "limit": lim,
                "notional": notional,
                "symbols": symbols,
                "dry": dry,
                # Provide both forms so downstream code can choose
                "contexts": ctx_by_name,        # by-name dict
                "contexts_list": ctx_list,      # iterable list
                "one": ctx_by_name.get("one"),
                "default": ctx_by_name.get("default"),
            }

            intents = _try_scan(book, strat, req, ctx_by_name, ctx_list)
            if not intents:
                continue

            for intent in intents:
                # normalize to a dict
                intent = intent or {}
                if not isinstance(intent, dict):
                    intent = {"_value": intent}
                o = {
                    "id": intent.get("id") or f"{strat}-{int(time.time()*1000)}",
                    "strategy": strat,
                    "symbol": intent.get("symbol") or "BTCUSDT",
                    "side": intent.get("side") or "buy",
                    "qty": intent.get("qty") or 0,
                    "price_in": intent.get("price") or intent.get("price_in") or 0.0,
                    "price_out": intent.get("price_out") or 0.0,
                    "timestamp": iso(now_utc()),
                    "meta": intent,
                }
                if "pnl" in intent:
                    try:
                        o["pnl"] = float(intent["pnl"])
                    except Exception:
                        pass
                ORDERS.add_order(o)

        except Exception as e:
            print(f"{iso(now_utc())} WARNING:app:scan attempt failed (inst.scan(...)): {e}")
            print(f"{iso(now_utc())} ERROR:app:All scan attempts failed for strategy '{strat}'. Returning empty list. Last error: {e}")

def scheduler_loop():
    while True:
        try:
            run_all_strategies_once(dry=False)
        except Exception as e:
            print(f"{iso(now_utc())} ERROR:app:Scheduler loop error: {e}")
        time.sleep(SCHEDULER_INTERVAL_SEC)

# --------------------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------------------

app = FastAPI(title="Crypto System", version="1.0.0")

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard")

@app.get("/orders/recent")
def get_recent_orders(limit: int = Query(50, ge=1, le=500)):
    return JSONResponse(ORDERS.recent(limit))

@app.get("/pnl/summary")
def pnl_summary():
    orders = ORDERS.all()
    by_strat = group_by_strategy(orders)
    payload = {"updated_at": iso(now_utc()), "strategies": {}}
    total = 0.0
    for strat, rows in by_strat.items():
        spnl = sum(pnl_for_order(o) for o in rows)
        total += spnl
        payload["strategies"][strat] = {
            "trades": len(rows),
            "pnl": spnl,
            "win_rate": (sum(1 for o in rows if pnl_for_order(o) > 0) / len(rows)) if rows else 0.0,
        }
    payload["total_pnl"] = total
    return JSONResponse(payload)

@app.get("/orders/attribution")
def orders_attribution():
    return JSONResponse(attribution(ORDERS.all()))

@app.get("/orders/performance_4h")
def orders_performance_4h():
    return JSONResponse(strategy_stats_last_n_hours(ORDERS.all(), hours=4))

@app.post("/evaluate")
async def evaluate_orders(body: Dict[str, Any]):
    items = body if isinstance(body, list) else body.get("orders") or []
    count = 0
    for row in items:
        try:
            if "timestamp" not in row:
                row["timestamp"] = iso(now_utc())
            ORDERS.add_order(row)
            count += 1
        except Exception:
            continue
    return JSONResponse({"ok": True, "ingested": count})

# --------------------------------------------------------------------------------------
# Dashboard HTML (full page with 4h performance)
# --------------------------------------------------------------------------------------

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
<meta http-equiv="Pragma" content="no-cache" />
<meta http-equiv="Expires" content="0" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Crypto System Dashboard</title>
<style>
  :root {
    --bg: #0b0f14;
    --panel: #121821;
    --panel-2: #0e141c;
    --text: #e6eef8;
    --muted: #97a6b9;
    --pos: #00d084;
    --neg: #ff5a5f;
    --accent: #66b2ff;
    --chip: #1e2937;
    --border: #1a2230;
    --warn: #ffcc00;
  }
  * { box-sizing: border-box; }
  html, body { height: 100%; }
  body {
    margin: 0; background: var(--bg); color: var(--text);
    font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, Apple Color Emoji, Segoe UI Emoji;
    line-height: 1.35;
  }
  a { color: var(--accent); text-decoration: none; }
  header {
    position: sticky; top: 0; z-index: 10;
    background: linear-gradient(180deg, rgba(10,14,20,.95), rgba(10,14,20,.85) 60%, rgba(10,14,20,0));
    backdrop-filter: blur(8px);
    border-bottom: 1px solid var(--border);
  }
  .wrap { max-width: 1200px; margin: 0 auto; padding: 16px; }
  .hstack { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .spacer { flex: 1; }
  .chip { background: var(--chip); border: 1px solid var(--border); padding: 6px 10px; border-radius: 999px; color: var(--muted); font-size: 12px; }
  .grid { display: grid; grid-template-columns: repeat(12, 1fr); gap: 16px; }
  .card {
    background: linear-gradient(180deg, var(--panel), var(--panel-2));
    border: 1px solid var(--border); border-radius: 12px; padding: 16px;
  }
  .span-4 { grid-column: span 4; }
  .span-8 { grid-column: span 8; }
  .span-12 { grid-column: span 12; }
  h1, h2, h3 { margin: 0 0 8px 0; }
  h1 { font-size: 20px; }
  h2 { font-size: 16px; color: var(--muted); }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th, td { padding: 8px; border-bottom: 1px solid var(--border); text-align: left; }
  th { color: var(--muted); font-weight: 600; }
  tr:last-child td { border-bottom: none; }
  .pos { color: var(--pos); }
  .neg { color: var(--neg); }
  .muted { color: var(--muted); }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
  .tag { display:inline-block; padding: 2px 8px; border-radius: 999px; background: #162130; border:1px solid var(--border); color: var(--muted); font-size: 12px; }
  .kpi { font-size: 28px; font-weight: 700; }
  .sub { color: var(--muted); font-size: 12px; }
  .small { font-size: 12px; }
  .right { text-align: right; }
</style>
</head>
<body>
  <header>
    <div class="wrap hstack">
      <h1>🚀 Crypto System</h1>
      <span class="chip">Live</span>
      <div class="spacer"></div>
      <span class="chip">Auto-refresh <span id="refreshStatus" class="muted">(on)</span></span>
      <a class="chip" href="/orders/recent?limit=500" target="_blank">API: recent</a>
      <a class="chip" href="/pnl/summary" target="_blank">API: pnl</a>
      <a class="chip" href="/orders/attribution" target="_blank">API: attribution</a>
      <a class="chip" href="/orders/performance_4h" target="_blank">API: perf 4h</a>
    </div>
  </header>

  <main class="wrap" style="padding-top: 16px; padding-bottom: 64px;">
    <section class="grid">
      <div class="card span-4">
        <h2>Total P&L</h2>
        <div id="kpiTotal" class="kpi mono">—</div>
        <div class="sub" id="lastUpdated">updated —</div>
      </div>
      <div class="card span-4">
        <h2>Win Rate (All)</h2>
        <div id="kpiWinRate" class="kpi mono">—</div>
        <div class="sub" id="kpiTrades">— trades</div>
      </div>
      <div class="card span-4">
        <h2>Last 4h P&L</h2>
        <div id="kpi4h" class="kpi mono">—</div>
        <div class="sub" id="kpi4hTrades">— trades (— wins / — losses)</div>
      </div>

      <div class="card span-8">
        <h2>Per-Strategy Attribution (All Time)</h2>
        <table id="tblAttrib">
          <thead>
            <tr>
              <th>Strategy</th>
              <th class="right">Trades</th>
              <th class="right">Win rate</th>
              <th class="right">Avg P&L</th>
              <th class="right">Total P&L</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>

      <div class="card span-4">
        <h2>Strategies (Last 4 Hours)</h2>
        <table id="tbl4h">
          <thead>
            <tr>
              <th>Strategy</th>
              <th class="right">Trades</th>
              <th class="right">Win %</th>
              <th class="right">P&L</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>

      <div class="card span-12">
        <h2>Recent Trades (Last 4 Hours)</h2>
        <table id="tblTrades">
          <thead>
            <tr>
              <th>Time (UTC)</th>
              <th>Strategy</th>
              <th>Symbol</th>
              <th>Side</th>
              <th class="right">Qty</th>
              <th class="right mono">PnL</th>
              <th class="right mono">In → Out</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
        <div class="small muted" style="margin-top:8px;">
          Tip: Click the API chips in the header to inspect raw JSON responses.
        </div>
      </div>
    </section>
  </main>

<script>
const fmtPNL = (v) => {
  const n = Number(v||0);
  const cls = n >= 0 ? 'pos' : 'neg';
  const sign = n > 0 ? '+' : '';
  return `<span class="${cls}">${sign}${n.toFixed(2)}</span>`;
};
const pct = (x) => isFinite(x) ? (x*100).toFixed(1) + '%' : '—';

async function fetchJSON(url) {
  const r = await fetch(url, {cache:'no-store'});
  return r.json();
}

async function refresh() {
  try {
    const [attrib, pnl, perf4h, recent] = await Promise.all([
      fetchJSON('/orders/attribution'),
      fetchJSON('/pnl/summary'),
      fetchJSON('/orders/performance_4h'),
      fetchJSON('/orders/recent?limit=500'),
    ]);

    const total = pnl.total_pnl || 0;
    document.getElementById('kpiTotal').innerHTML = fmtPNL(total);
    document.getElementById('lastUpdated').textContent = 'updated ' + (pnl.updated_at || '—');

    let allTrades = 0, allWins = 0;
    Object.values(attrib).forEach(row => {
      allTrades += row.trades || 0;
      if (row.trades) allWins += Math.round((row.win_rate||0) * row.trades);
    });
    const wr = allTrades ? (allWins/allTrades) : 0;
    document.getElementById('kpiWinRate').textContent = pct(wr);
    document.getElementById('kpiTrades').textContent = `${allTrades} trades`;

    const agg = perf4h._ALL || {};
    document.getElementById('kpi4h').innerHTML = fmtPNL(agg.total_pnl || 0);
    document.getElementById('kpi4hTrades').textContent =
      `${(agg.trades||0)} trades (${agg.wins||0} wins / ${agg.losses||0} losses)`;

    const attribBody = document.querySelector('#tblAttrib tbody');
    attribBody.innerHTML = '';
    Object.entries(attrib).sort((a,b)=> (b[1].pnl||0) - (a[1].pnl||0)).forEach(([k,v])=>{
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><span class="tag">${k}</span></td>
        <td class="right">${v.trades||0}</td>
        <td class="right">${pct(v.win_rate||0)}</td>
        <td class="right mono">${(v.avg_pnl||0).toFixed(2)}</td>
        <td class="right mono">${fmtPNL(v.pnl||0)}</td>
      `;
      attribBody.appendChild(tr);
    });

    const body4h = document.querySelector('#tbl4h tbody');
    body4h.innerHTML = '';
    Object.entries(perf4h)
      .filter(([k,_])=> k !== '_ALL')
      .sort((a,b)=> (b[1].total_pnl||0) - (a[1].total_pnl||0))
      .forEach(([k,v])=>{
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><span class="tag">${k}</span></td>
          <td class="right">${v.trades||0}</td>
          <td class="right">${pct(v.win_rate||0)}</td>
          <td class="right mono">${fmtPNL(v.total_pnl||0)}</td>
        `;
        body4h.appendChild(tr);
      });

    const tbody = document.querySelector('#tblTrades tbody');
    tbody.innerHTML = '';
    (recent || [])
      .filter(o => {
        const ts = (o.timestamp || o.time || '');
        if (!ts) return false;
        try {
          const t = new Date(ts);
          return t >= new Date(Date.now() - 4*60*60*1000);
        } catch(e){ return false; }
      })
      .sort((a,b)=> String(b.timestamp||'').localeCompare(String(a.timestamp||'')))
      .slice(0, 200)
      .forEach(o=>{
        const tr = document.createElement('tr');
        const inPrice = Number(o.price_in || o.avg_fill_price || 0);
        const outPrice = Number(o.price_out || o.close_price || 0);
        const side = (o.side||'').toUpperCase();
        tr.innerHTML = `
          <td class="mono">${o.timestamp || '—'}</td>
          <td><span class="tag">${o.strategy || o.strat || '—'}</span></td>
          <td>${o.symbol || '—'}</td>
          <td>${side || '—'}</td>
          <td class="right">${Number(o.qty||0).toFixed(4)}</td>
          <td class="right mono">${fmtPNL(o.pnl || 0)}</td>
          <td class="right mono">${inPrice?inPrice.toFixed(4):'—'} → ${outPrice?outPrice.toFixed(4):'—'}</td>
        `;
        tbody.appendChild(tr);
      });

  } catch (e) {
    console.error(e);
    document.getElementById('refreshStatus').textContent = '(error)';
  }
}

let timer = null;
function start() {
  refresh();
  timer = setInterval(refresh, 15000);
}
document.addEventListener('visibilitychange', ()=>{
  if (document.hidden) {
    document.getElementById('refreshStatus').textContent = '(paused)';
  } else {
    document.getElementById('refreshStatus').textContent = '(on)';
    refresh();
  }
});
start();
</script>

</body>
</html>
"""

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

# --------------------------------------------------------------------------------------
# FastAPI lifespan hooks (legacy on_event kept for parity with your logs)
# --------------------------------------------------------------------------------------

@app.on_event("startup")
def _on_startup():
    print(f"{iso(now_utc())} INFO:app:Starting app; scheduler will start.")
    if SCHEDULER_ENABLED:
        t = threading.Thread(target=scheduler_loop, name="scheduler", daemon=True)
        t.start()

@app.on_event("shutdown")
def _on_shutdown():
    print(f"{iso(now_utc())} INFO:app:Shutting down app; scheduler will stop.")

# --------------------------------------------------------------------------------------
# Local run (Render auto-runs `python app.py`)
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "10000"))
    print(f"{iso(now_utc())} INFO:app:Launching Uvicorn on {host}:{port}")
    uvicorn.run("app:app", host=host, port=port, reload=False, access_log=True)
