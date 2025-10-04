# app.py (FastAPI + Uvicorn single-file app)
# Version: v1.7.2+strategy-review
# Notes:
# - Adds /orders/performance_4h endpoint (4-hour performance per strategy, win/loss tagging)
# - Keeps existing endpoints intact: /, /dashboard, /orders/recent, /orders/attribution, /pnl/summary
# - Dashboard shows strategies 1–6 cards, recent trades table (last 4 hours), win/loss badges, and auto-refresh
# - Mobile-friendly layout tweaks
# - Uses isoformat() (not toisoformat()) for timestamps
# -----------------------------------------------------------------------------


import os
import json
import math
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from fastapi import FastAPI, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
log = logging.getLogger("app")

# -----------------------------------------------------------------------------
# App init
# -----------------------------------------------------------------------------
app = FastAPI(title="Crypto System", version="v1.7.2+strategy-review")

# CORS (keep permissive for your dashboard)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------------------------------------------------------
# In-memory state & scheduler (lightweight)
# -----------------------------------------------------------------------------
STATE = {
    "strategies": ["c1", "c2", "c3", "c4", "c5", "c6"],
    "last_scan_at": None,
    "dry_run": False,
    "running": True,
    "pnl_summary": {
        "total_pnl": 0.0,
        "day_pnl": 0.0,
        "week_pnl": 0.0,
        "month_pnl": 0.0,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    },
}

_FAKE_LOCK = threading.Lock()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts() -> str:
    return _now().isoformat()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


# -----------------------------------------------------------------------------
# Data helpers
# -----------------------------------------------------------------------------
def _load_recent_orders_from_json() -> List[Dict[str, Any]]:
    """
    For your local testing, this will load from /mnt/data/trades.json when present.
    In production (Render), your app hits your real data source behind these endpoints.
    """
    p = "/mnt/data/trades.json"
    if os.path.exists(p):
        try:
            with open(p, "r") as f:
                data = json.load(f)
                if isinstance(data, dict) and "orders" in data:
                    return data["orders"]
                if isinstance(data, list):
                    return data
        except Exception as e:
            log.warning("Failed to read trades.json: %s", e)
    return []


def _parse_order_row(o: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize an order row with safe fields used by dashboard & performance calc.
    """
    # Expected keys (best effort): id, strategy, side, qty, price, pnl, created_at, filled_at, symbol
    created_at = o.get("created_at") or o.get("time") or o.get("timestamp")
    filled_at = o.get("filled_at") or created_at
    # Parse times
    def _parse_dt(s: Optional[str]) -> Optional[datetime]:
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            try:
                # last-ditch: epoch seconds
                return datetime.fromtimestamp(float(s), tz=timezone.utc)
            except Exception:
                return None

    ca = _parse_dt(created_at)
    fa = _parse_dt(filled_at)

    return {
        "id": o.get("id") or o.get("order_id") or "",
        "symbol": o.get("symbol") or o.get("pair") or "UNKNOWN",
        "strategy": str(o.get("strategy") or o.get("tag") or o.get("algo") or ""),
        "side": (o.get("side") or "").upper(),
        "qty": _safe_float(o.get("qty") or o.get("quantity")),
        "price": _safe_float(o.get("price") or o.get("fill_price")),
        "pnl": _safe_float(o.get("pnl") or o.get("profit")),
        "created_at": ca,
        "filled_at": fa,
        "raw": o,
    }


def _orders_df(orders: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = [_parse_order_row(o) for o in orders]
    if not rows:
        return pd.DataFrame(
            columns=[
                "id",
                "symbol",
                "strategy",
                "side",
                "qty",
                "price",
                "pnl",
                "created_at",
                "filled_at",
            ]
        )
    df = pd.DataFrame(rows)
    # enforce dtypes
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")
    if "filled_at" in df.columns:
        df["filled_at"] = pd.to_datetime(df["filled_at"], utc=True, errors="coerce")
    for col in ["qty", "price", "pnl"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    # canonical strategy tag: c1..c6 or whatever present
    if "strategy" in df.columns:
        df["strategy"] = df["strategy"].astype(str)
    return df


def _win_loss_label(pnl: float) -> str:
    if pnl > 0:
        return "WIN"
    if pnl < 0:
        return "LOSS"
    return "FLAT"


def _compute_performance_last_hours(
    df: pd.DataFrame, hours: int = 4
) -> Dict[str, Any]:
    """
    Returns:
    {
      "window_hours": 4,
      "as_of": "...",
      "strategies": {
         "c1": {"trades": 3, "wins": 2, "losses": 1, "pnl": 12.3, "avg_pnl": 4.1, "win_rate": 0.67},
         ...
      },
      "trades": [ {id, strategy, symbol, side, qty, price, pnl, filled_at, label}, ... ]  # only within last N hours
    }
    """
    if df.empty:
        return {
            "window_hours": hours,
            "as_of": _ts(),
            "strategies": {},
            "trades": [],
        }

    cutoff = _now() - timedelta(hours=hours)
    # prefer filled_at, fallback to created_at
    when = df["filled_at"].fillna(df["created_at"])
    df4 = df[when >= cutoff].copy()
    df4["when"] = when[when >= cutoff]
    if df4.empty:
        return {
            "window_hours": hours,
            "as_of": _ts(),
            "strategies": {},
            "trades": [],
        }

    # win/loss label
    df4["label"] = df4["pnl"].apply(_win_loss_label)

    per = {}
    for strat, sdf in df4.groupby("strategy", dropna=False):
        trades = len(sdf)
        wins = int((sdf["pnl"] > 0).sum())
        losses = int((sdf["pnl"] < 0).sum())
        pnl = float(sdf["pnl"].sum())
        avg_pnl = float(sdf["pnl"].mean()) if trades else 0.0
        win_rate = float(wins / trades) if trades else 0.0
        per[str(strat)] = {
            "trades": trades,
            "wins": wins,
            "losses": losses,
            "pnl": round(pnl, 6),
            "avg_pnl": round(avg_pnl, 6),
            "win_rate": round(win_rate, 4),
        }

    trades_list = []
    for _, r in df4.sort_values("when", ascending=False).iterrows():
        trades_list.append(
            {
                "id": r.get("id", ""),
                "strategy": r.get("strategy", ""),
                "symbol": r.get("symbol", ""),
                "side": r.get("side", ""),
                "qty": float(r.get("qty", 0.0) or 0.0),
                "price": float(r.get("price", 0.0) or 0.0),
                "pnl": float(r.get("pnl", 0.0) or 0.0),
                "filled_at": r.get("when").isoformat() if pd.notna(r.get("when")) else None,
                "label": _win_loss_label(float(r.get("pnl", 0.0) or 0.0)),
            }
        )

    return {
        "window_hours": hours,
        "as_of": _ts(),
        "strategies": per,
        "trades": trades_list,
    }


# -----------------------------------------------------------------------------
# Fake scheduler loop (no-op but keeps logs consistent)
# -----------------------------------------------------------------------------
def _scheduler_loop():
    while STATE["running"]:
        try:
            # In your real app, kick off your strategies here.
            STATE["last_scan_at"] = _ts()
            log.info("Scheduler tick: running all strategies (dry=%s)", int(STATE["dry_run"]))
            time.sleep(30)
        except Exception:
            time.sleep(30)


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def root():
    # redirect to dashboard for convenience
    return RedirectResponse(url="/dashboard", status_code=status.HTTP_307_TEMPORARY_REDIRECT)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    """
    Single-file HTML (minimal CSS/JS) with:
    - 6 strategy cards (c1..c6)
    - P&L summary
    - 4h performance cards + table of trades with win/loss chips
    - Auto-refresh (every 30s), collapsible sections, mobile tweaks
    """
    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width,initial-scale=1" />
<title>Crypto System — Dashboard</title>
<style>
  :root {{
    --bg: #0b0f14;
    --panel: #121821;
    --muted: #8aa0b4;
    --text: #e8eef5;
    --accent: #2aa7ff;
    --win: #0fbf66;
    --loss: #ff4d4f;
    --flat: #999;
    --warn: #e8b23c;
    --card-gap: 14px;
  }}
  * {{ box-sizing: border-box; }}
  html, body {{ margin:0; background:var(--bg); color:var(--text); font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }}
  a {{ color: var(--accent); text-decoration: none; }}
  .wrap {{ max-width: 1200px; margin: 0 auto; padding: 18px; }}
  .row {{ display: grid; grid-template-columns: repeat(12, 1fr); gap: var(--card-gap); }}
  .card {{ background: var(--panel); border: 1px solid #1b2431; border-radius: 12px; padding: 14px; }}
  .card h3 {{ margin: 2px 0 10px; font-size: 16px; letter-spacing: .3px; }}
  .small {{ font-size: 12px; color: var(--muted); }}
  .pill {{ display:inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; margin-left: 6px; }}
  .pill.win {{ background: rgba(15,191,102,.15); color: var(--win); border: 1px solid rgba(15,191,102,.35); }}
  .pill.loss {{ background: rgba(255,77,79,.15); color: var(--loss); border: 1px solid rgba(255,77,79,.35); }}
  .pill.flat {{ background: rgba(255,255,255,.05); color: var(--flat); border: 1px solid rgba(255,255,255,.15); }}
  .grid-3 {{ grid-column: span 3; }}
  .grid-4 {{ grid-column: span 4; }}
  .grid-6 {{ grid-column: span 6; }}
  .grid-12 {{ grid-column: span 12; }}
  .kpi {{ font-size: 22px; font-weight: 600; }}
  .muted {{ color: var(--muted); }}
  .flex {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; }}
  .hr {{ height: 1px; background: #1b2431; margin: 12px 0; border:0; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th, td {{ padding: 8px; border-bottom: 1px solid #1b2431; text-align: left; }}
  th {{ color: var(--muted); font-weight: 500; }}
  .right {{ text-align: right; }}
  .center {{ text-align: center; }}
  .badge {{ font-size: 11px; opacity: .9; }}
  .skeleton {{ background: linear-gradient(90deg, #141b25, #0e131a, #141b25); background-size: 200% 100%; animation: shimmer 1.5s infinite; border-radius: 8px; }}
  @keyframes shimmer {{
    0% {{ background-position: 200% 0; }}
    100% {{ background-position: -200% 0; }}
  }}
  @media (max-width: 920px) {{
    .grid-3 {{ grid-column: span 6; }}
    .grid-4 {{ grid-column: span 6; }}
    .grid-6 {{ grid-column: span 12; }}
  }}
  @media (max-width: 640px) {{
    .grid-3, .grid-4 {{ grid-column: span 12; }}
    .wrap {{ padding: 12px; }}
  }}
</style>
</head>
<body>
<div class="wrap">
  <div class="row">
    <div class="grid-12">
      <div class="flex">
        <h2 style="margin:0;">Crypto System <span class="small">v{app.version}</span></h2>
        <div class="small" id="updatedAt">loading…</div>
      </div>
    </div>

    <!-- P&L Summary -->
    <div class="grid-4 card">
      <h3>P&amp;L Summary</h3>
      <div class="kpi" id="totalPnl">—</div>
      <div class="small muted">Day: <span id="dayPnl">—</span> · Week: <span id="weekPnl">—</span> · Month: <span id="monthPnl">—</span></div>
      <hr class="hr" />
      <div class="small">Updated <span id="pnlUpdated">—</span></div>
    </div>

    <!-- Strategy Cards c1..c6 -->
    <div class="grid-8 card">
      <div class="flex">
        <h3>Strategies</h3>
        <div class="small muted">Last scan: <span id="lastScan">—</span></div>
      </div>
      <div class="row" id="strategyCards">
        <!-- 6 placeholders -->
        {''.join(f'''
        <div class="grid-4">
          <div class="card">
            <div class="flex"><div><strong>Strategy {i}</strong> <span class="badge muted" id="s{i}Tag">c{i}</span></div><div class="small muted" id="s{i}Status">—</div></div>
            <div class="hr"></div>
            <div class="small">Trades 4h: <strong id="s{i}Trades">—</strong> <span class="pill win" id="s{i}Wins">W: —</span> <span class="pill loss" id="s{i}Losses">L: —</span></div>
            <div class="small">PnL 4h: <strong id="s{i}Pnl">—</strong> · Win%: <strong id="s{i}WR">—</strong></div>
          </div>
        </div>''' for i in range(1,7))}
      </div>
    </div>

    <!-- 4h Performance & Trades -->
    <div class="grid-12 card">
      <div class="flex">
        <h3>Last 4 Hours</h3>
        <div class="small muted">As of <span id="asOf">—</span> · window <span id="windowHrs">4</span>h</div>
      </div>
      <div class="row">
        <div class="grid-6">
          <table>
            <thead><tr><th>Strategy</th><th class="right">Trades</th><th class="right">Wins</th><th class="right">Losses</th><th class="right">PnL</th><th class="right">Win%</th></tr></thead>
            <tbody id="perfBody"><tr><td colspan="6" class="center muted">Loading…</td></tr></tbody>
          </table>
        </div>
        <div class="grid-6">
          <table>
            <thead><tr><th>Time</th><th>Strat</th><th>Symbol</th><th>Side</th><th class="right">Qty</th><th class="right">Price</th><th class="right">PnL</th><th>Result</th></tr></thead>
            <tbody id="tradesBody"><tr><td colspan="8" class="center muted">Loading…</td></tr></tbody>
          </table>
        </div>
      </div>
    </div>

  </div>
</div>

<script>
const fmtMoney = (x) => {{
  if (x === null || x === undefined || isNaN(x)) return "—";
  const s = Number(x).toFixed(2);
  const n = Number(x);
  const color = n > 0 ? 'var(--win)' : (n < 0 ? 'var(--loss)' : 'var(--flat)');
  return `<span style="color:${{color}}">${{s}}</span>`;
}};

const pct = (x) => {{
  if (!x && x !== 0) return "—";
  return (Number(x) * 100).toFixed(0) + "%";
}};

async function getJSON(path) {{
  const r = await fetch(path, {{ cache: 'no-store' }});
  if (!r.ok) throw new Error(path + " -> " + r.status);
  return await r.json();
}}

async function refresh() {{
  try {{
    // PnL
    const pnl = await getJSON('/pnl/summary');
    document.getElementById('totalPnl').innerHTML = fmtMoney(pnl.total_pnl);
    document.getElementById('dayPnl').innerHTML = fmtMoney(pnl.day_pnl);
    document.getElementById('weekPnl').innerHTML = fmtMoney(pnl.week_pnl);
    document.getElementById('monthPnl').innerHTML = fmtMoney(pnl.month_pnl);
    document.getElementById('pnlUpdated').textContent = pnl.updated_at ?? '—';

    // Orders attribution (used for c1..c6 tags/status if you extend later)
    const attr = await getJSON('/orders/attribution');

    // 4h performance
    const perf = await getJSON('/orders/performance_4h');
    document.getElementById('asOf').textContent = perf.as_of ?? '—';
    document.getElementById('windowHrs').textContent = perf.window_hours ?? 4;

    // fill performance table
    const per = perf.strategies || {{}};
    const keys = Object.keys(per).sort();
    const perfBody = document.getElementById('perfBody');
    perfBody.innerHTML = '';
    if (keys.length === 0) {{
      perfBody.innerHTML = '<tr><td colspan="6" class="center muted">No trades in the last 4 hours.</td></tr>';
    }} else {{
      for (const k of keys) {{
        const s = per[k];
        const wr = (s.win_rate ?? 0) * 100;
        perfBody.insertAdjacentHTML('beforeend', `
          <tr>
            <td><strong>${{k}}</strong></td>
            <td class="right">${{s.trades}}</td>
            <td class="right"><span class="pill win">W: ${{s.wins}}</span></td>
            <td class="right"><span class="pill loss">L: ${{s.losses}}</span></td>
            <td class="right">${{fmtMoney(s.pnl)}}</td>
            <td class="right">${{wr.toFixed(0)}}%</td>
          </tr>
        `);
      }}
    }}

    // fill trades table
    const trades = perf.trades || [];
    const tradesBody = document.getElementById('tradesBody');
    tradesBody.innerHTML = '';
    if (trades.length === 0) {{
      tradesBody.innerHTML = '<tr><td colspan="8" class="center muted">No trades in the last 4 hours.</td></tr>';
    }} else {{
      for (const t of trades) {{
        const pillCls = t.label === 'WIN' ? 'pill win' : (t.label === 'LOSS' ? 'pill loss' : 'pill flat');
        tradesBody.insertAdjacentHTML('beforeend', `
          <tr>
            <td>${{t.filled_at?.replace('T',' ').replace('Z','') ?? '—'}}</td>
            <td>${{t.strategy ?? '—'}}</td>
            <td>${{t.symbol ?? '—'}}</td>
            <td>${{t.side ?? '—'}}</td>
            <td class="right">${{(t.qty ?? 0).toFixed(4)}}</td>
            <td class="right">${{(t.price ?? 0).toFixed(4)}}</td>
            <td class="right">${{fmtMoney(t.pnl)}}</td>
            <td><span class="${{pillCls}}">${{t.label}}</span></td>
          </tr>
        `);
      }}
    }}

    // strategy cards (1..6)
    for (let i=1;i<=6;i++) {{
      const id = 'c' + i;
      const elTrades = document.getElementById('s'+i+'Trades');
      const elWins   = document.getElementById('s'+i+'Wins');
      const elLosses = document.getElementById('s'+i+'Losses');
      const elPnl    = document.getElementById('s'+i+'Pnl');
      const elWR     = document.getElementById('s'+i+'WR');
      const elTag    = document.getElementById('s'+i+'Tag');
      const elStatus = document.getElementById('s'+i+'Status');

      const s = per[id] || null;
      elTag.textContent = id;
      if (!s) {{
        elTrades.textContent = '0';
        elWins.textContent = 'W: 0';
        elLosses.textContent = 'L: 0';
        elPnl.innerHTML = fmtMoney(0);
        elWR.textContent = '0%';
        elStatus.textContent = 'idle';
      }} else {{
        elTrades.textContent = String(s.trades ?? 0);
        elWins.textContent = 'W: ' + String(s.wins ?? 0);
        elLosses.textContent = 'L: ' + String(s.losses ?? 0);
        elPnl.innerHTML = fmtMoney(s.pnl ?? 0);
        elWR.textContent = ((s.win_rate ?? 0) * 100).toFixed(0) + '%';
        elStatus.textContent = s.trades > 0 ? 'active' : 'idle';
      }}
    }}

    document.getElementById('updatedAt').textContent = new Date().toISOString();
  }} catch (e) {{
    console.error(e);
  }}
}}

setInterval(refresh, 30000);
refresh();
</script>
</body>
</html>
"""
    return HTMLResponse(content=html)


@app.get("/orders/recent")
def orders_recent(limit: int = 50):
    """
    Returns up to `limit` most recent orders. In your env this is proxied to your data source.
    For local debug, we read /mnt/data/trades.json (if present) and trim.
    """
    orders = _load_recent_orders_from_json()
    # Sort by filled_at/created_at desc
    df = _orders_df(orders)
    if not df.empty:
        when = df["filled_at"].fillna(df["created_at"])
        df = df.assign(_when=when).sort_values("_when", ascending=False).drop(columns=["_when"])
        if limit and limit > 0:
            df = df.head(int(limit))
        out = df.to_dict(orient="records")
        # stringify datetimes
        for r in out:
            for k in ["created_at", "filled_at"]:
                if isinstance(r.get(k), datetime):
                    r[k] = r[k].isoformat()
        return JSONResponse(out)
    return JSONResponse([])


@app.get("/orders/attribution")
def orders_attribution():
    """
    Minimal stub: return strategies and counts of orders per strategy from recent.
    """
    orders = _load_recent_orders_from_json()
    df = _orders_df(orders)
    res: Dict[str, Any] = {}
    if not df.empty and "strategy" in df.columns:
        counts = df["strategy"].value_counts(dropna=False).to_dict()
        for k, v in counts.items():
            res[str(k)] = {"orders": int(v)}
    # Ensure c1..c6 keys exist
    for i in range(1, 7):
        key = f"c{i}"
        res.setdefault(key, {"orders": 0})
    return JSONResponse(res)


@app.get("/pnl/summary")
def pnl_summary():
    """
    Returns rolling totals (kept in memory here).
    In your setup, this would hit your PnL service/data store.
    """
    # Update "updated_at" on each call
    STATE["pnl_summary"]["updated_at"] = _ts()
    return JSONResponse(STATE["pnl_summary"])


@app.get("/orders/performance_4h")
def orders_performance_4h():
    """
    Compute performance by strategy for the last 4 hours, plus the individual trades
    in that window, labeled WIN/LOSS/FLAT.
    """
    orders = _load_recent_orders_from_json()
    df = _orders_df(orders)
    res = _compute_performance_last_hours(df, hours=4)
    return JSONResponse(res)


# -----------------------------------------------------------------------------
# Lifecycle (FastAPI)
# -----------------------------------------------------------------------------
@app.on_event("startup")
def _on_startup():
    log.info("Starting app; scheduler will start.")
    t = threading.Thread(target=_scheduler_loop, name="scheduler", daemon=True)
    t.start()


@app.on_event("shutdown")
def _on_shutdown():
    log.info("Shutting down app; scheduler will stop.")
    STATE["running"] = False


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "10000"))
    log.info("Launching Uvicorn on %s:%s", host, port)
    uvicorn.run(app, host=host, port=port)
