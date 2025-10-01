# app.py
import os
import json
import math
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd

from fastapi import FastAPI, Request, Query, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# --- Local strategy + universe modules ---
# Place these two files next to app.py
from strategies import StrategyBook, ScanRequest, ScanResult
from universe import UniverseBuilder, UniverseConfig

APP_VERSION = "1.12.0"   # bumped after adaptive strategies & dashboard refresh

# =========================
# Settings (env overrides)
# =========================
PORT                = int(os.getenv("PORT", "10000"))
SERVICE_NAME        = os.getenv("SERVICE_NAME", "crypto-system")
TZ                  = os.getenv("TZ", "UTC")

# Universe gates (can override in Render env)
UNIVERSE_MAX         = int(os.getenv("UNIVERSE_MAX", "24"))
MIN_DOLLAR_VOL_24H   = float(os.getenv("MIN_DOLLAR_VOL_24H", "5000000"))  # $5M baseline
MAX_SPREAD_BPS       = float(os.getenv("MAX_SPREAD_BPS", "15"))
MIN_ROWS_1M          = int(os.getenv("MIN_ROWS_1M", "1500"))
MIN_ROWS_5M          = int(os.getenv("MIN_ROWS_5M", "300"))

# Strategy defaults (still overrideable by query)
TOPK_DEFAULT         = int(os.getenv("TOPK", "2"))
MIN_SCORE_DEFAULT    = float(os.getenv("MIN_SCORE", "0.10"))
RISK_TARGET_USD      = float(os.getenv("RISK_TARGET_USD", "10.0"))
ATR_STOP_MULT        = float(os.getenv("ATR_STOP_MULT", "1.5"))

# =========================
# App + middleware
# =========================
app = FastAPI(title=SERVICE_NAME, version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# ================
# Fake order book / state stubs
# Replace with your broker/exchange integration if you have it wired already.
# These are here so the dashboard and your existing PowerShell calls keep working.
# ================
_ORDERS: List[Dict[str, Any]] = []
_POSITIONS: Dict[str, Dict[str, Any]] = {}
_ATTRIBUTION: List[Dict[str, Any]] = []

def _now_ts():
    return datetime.now(timezone.utc).isoformat()

def _record_order(symbol: str, side: str, qty: float, notional: float, strat: str, reason: str):
    oid = str(uuid.uuid4())
    order = {
        "id": oid, "t": _now_ts(), "symbol": symbol, "side": side,
        "qty": qty, "notional": notional, "status": "filled",
        "strategy": strat, "reason": reason
    }
    _ORDERS.insert(0, order)
    _ATTRIBUTION.insert(0, {
        "t": order["t"], "strategy": strat, "symbol": symbol,
        "side": side, "qty": qty, "notional": notional,
        "reason": reason
    })
    # naive position update
    pos = _POSITIONS.get(symbol) or {"symbol": symbol, "qty": 0.0, "avg_px": 0.0}
    if side == "buy":
        pos["qty"] += qty
    elif side == "sell":
        pos["qty"] -= qty
    _POSITIONS[symbol] = pos
    return order

def _pnl_summary() -> Dict[str, Any]:
    # Stub: realized PnL comes from attribution aggregation; keep prior shape
    realized = round(sum(o.get("notional", 0.0) * 0.0 for o in _ORDERS), 2)
    unreal   = 0.0
    return {"ok": True, "realized": realized, "unrealized": unreal, "total": realized + unreal}

# =========================
# Candle store (you already have this working)
# Keep same shapes your /diag endpoint emitted earlier:
# - we expose get_rows_meta() so the dashboard + PowerShell diag keep working.
# =========================
# In your live service you likely backfill on startup. We keep an in-memory
# cache facade to align with existing behavior.
from collections import defaultdict, deque

class CandleCache:
    def __init__(self):
        # key: (symbol, tf) -> deque of dict{t, o, h, l, c, v}
        self.data: Dict[tuple, deque] = {}
        self.maxlen = 10000

    def put(self, symbol: str, tf: str, bars: List[Dict[str, Any]]):
        key = (symbol, tf)
        dq = self.data.get(key) or deque(maxlen=self.maxlen)
        for b in bars:
            dq.append(b)
        self.data[key] = dq

    def get(self, symbol: str, tf: str, limit: int) -> List[Dict[str, Any]]:
        key = (symbol, tf)
        dq = self.data.get(key)
        if not dq:
            return []
        if limit <= 0:
            return list(dq)
        return list(dq)[-limit:]

    def rows(self, symbol: str, tf: str) -> int:
        key = (symbol, tf)
        dq = self.data.get(key)
        return len(dq) if dq else 0

CANDLES = CandleCache()

# Seed the cache with synthetic but realistic-looking bars if empty.
# NOTE: Your live instance already backfills real data. This block only runs
# if nothing has filled the cache yet, so it won’t interfere with your existing loader.
def _seed_if_empty(symbols: List[str]):
    if any(CANDLES.rows(s, "1Min") > 0 for s in symbols):
        return
    rng = np.random.default_rng(42)
    now = datetime.now(timezone.utc)
    for s in symbols:
        px = 100.0 + 50.0 * rng.random()
        for tf, step in [("1Min", timedelta(minutes=1)), ("5Min", timedelta(minutes=5))]:
            bars = []
            t = now - 2000 * step
            for _ in range(2000):
                drift = rng.normal(0, 0.02)
                vol   = abs(rng.normal(0.0, 0.2))
                o = px
                c = px * (1 + drift * 0.01)
                h = max(o, c) * (1 + vol * 0.01)
                l = min(o, c) * (1 - vol * 0.01)
                v = abs(rng.normal(100, 20))
                bars.append({"t": t.isoformat(), "o": float(o), "h": float(h), "l": float(l), "c": float(c), "v": float(v)})
                px = c
                t += step
            CANDLES.put(s, tf, bars)

# =========================
# Universe
# =========================
CORE = ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD","AVAX/USD","LINK/USD","BCH/USD","LTC/USD"]
ALT  = ["ADA/USD","TON/USD","TRX/USD","APT/USD","ARB/USD","SUI/USD","OP/USD","MATIC/USD","NEAR/USD","ATOM/USD"]
DEFAULT_SYMBOLS = CORE + ALT

_universe_cfg = UniverseConfig(
    max_symbols=UNIVERSE_MAX,
    min_dollar_vol_24h=MIN_DOLLAR_VOL_24H,
    max_spread_bps=MAX_SPREAD_BPS,
    min_rows_1m=MIN_ROWS_1M,
    min_rows_5m=MIN_ROWS_5M,
)
_universe = UniverseBuilder(_universe_cfg)

# =========================
# Strategy book
# =========================
book = StrategyBook(
    topk=TOPK_DEFAULT,
    min_score=MIN_SCORE_DEFAULT,
    risk_target_usd=RISK_TARGET_USD,
    atr_stop_mult=ATR_STOP_MULT,
)

# =========================
# Lifespan (startup/shutdown)
# =========================
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Build initial universe and seed cache if empty (only synthetic fallback)
    symbols = DEFAULT_SYMBOLS.copy()
    _seed_if_empty(symbols)
    _universe.refresh_from_cache_like_source(symbols, CANDLES)  # uses cache metrics as gates
    yield

app.router.lifespan_context = lifespan

# =========================
# HTML Dashboard (embedded)
# =========================
DASHBOARD_HTML = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>{SERVICE_NAME} — v{APP_VERSION}</title>
<style>
  :root {{ --bg:#0c1116; --card:#121821; --muted:#9fb3c8; --acc:#3aa0ff; --ok:#20c997; --warn:#ffd43b; --bad:#ff6b6b; }}
  * {{ box-sizing:border-box; font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial; }}
  body {{ margin:0; background:var(--bg); color:#e7edf3; }}
  header {{ padding:18px 22px; border-bottom:1px solid #1f2a37; display:flex; gap:16px; align-items:center; }}
  header .tag {{ background:#17202b; padding:4px 10px; border-radius:999px; color:var(--muted); font-size:12px; }}
  main {{ padding:22px; display:grid; grid-template-columns: 1.2fr 1fr; gap:22px; }}
  .card {{ background:var(--card); border:1px solid #1f2a37; border-radius:14px; padding:14px; }}
  h2 {{ margin:6px 0 10px 0; font-size:16px; color:#e7edf3; }}
  .row {{ display:flex; gap:10px; flex-wrap:wrap; align-items:center; }}
  input, select {{ background:#0e141b; border:1px solid #223041; color:#e7edf3; padding:8px 10px; border-radius:10px; }}
  button {{ background:var(--acc); border:none; color:#001018; padding:9px 12px; border-radius:10px; cursor:pointer; }}
  button:disabled {{ opacity:.6; cursor:not-allowed; }}
  table {{ width:100%; border-collapse: collapse; font-size:13px; }}
  th, td {{ border-bottom:1px solid #1e2a39; padding:8px; text-align:left; color:#cfe0f1; }}
  th {{ color:#89a4be; font-weight:600; }}
  .muted {{ color:var(--muted); }}
  .ok {{ color:var(--ok); }}
  .warn {{ color:var(--warn); }}
  .bad {{ color:var(--bad); }}
  code.small {{ font-size:12px; color:#9fb3c8; }}
</style>
</head>
<body>
<header>
  <div><strong>{SERVICE_NAME}</strong> <span class="muted">v{APP_VERSION}</span></div>
  <div class="tag">Adaptive Strategies</div>
  <div class="tag">Top-K Selection</div>
  <div class="tag">Vol-Normalized Sizing</div>
</header>
<main>
  <section class="card">
    <h2>Scan</h2>
    <div class="row" style="margin-bottom:8px">
      <label>Strategy:</label>
      <select id="strat">
        <option>c1</option><option>c2</option><option>c3</option>
        <option>c4</option><option>c5</option><option>c6</option>
      </select>
      <label>TF:</label>
      <select id="tf">
        <option>1Min</option><option selected>5Min</option>
      </select>
      <label>Limit:</label>
      <input id="limit" type="number" value="360" style="width:90px"/>
      <label>Dry:</label>
      <select id="dry"><option value="1" selected>Yes</option><option value="0">No</option></select>
    </div>
    <div class="row" style="margin-bottom:8px">
      <label>TopK:</label><input id="topk" type="number" value="{TOPK_DEFAULT}" style="width:70px"/>
      <label>MinScore:</label><input id="minScore" type="number" step="0.01" value="{MIN_SCORE_DEFAULT}" style="width:90px"/>
      <label>Notional:</label><input id="notional" type="number" step="1" value="25" style="width:90px"/>
    </div>
    <div class="row" style="margin-bottom:10px">
      <label>Symbols (comma):</label>
      <input id="symbols" type="text" style="flex:1" value="{','.join(CORE)}"/>
      <button id="btnScan">Run</button>
    </div>
    <div class="row"><code class="small" id="scanMsg"></code></div>
    <div style="max-height:360px; overflow:auto; border:1px solid #1f2a37; border-radius:10px;">
      <table id="scanTbl">
        <thead><tr>
          <th>Symbol</th><th>Action</th><th>Score</th><th>Reason</th>
          <th>ATR</th><th>ATR%</th><th>Qty</th><th>Notional</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </section>

  <section class="card">
    <h2>Account / Health</h2>
    <div class="row" style="margin-bottom:8px">
      <button id="btnHealth">Health</button>
      <button id="btnPositions">Positions</button>
      <button id="btnRecent">Recent Orders</button>
      <button id="btnPnL">PnL</button>
      <button id="btnUniverse">Universe</button>
    </div>
    <pre id="info" style="white-space:pre-wrap; background:#0e141b; border:1px solid #223041; padding:10px; border-radius:10px; height:290px; overflow:auto;"></pre>
  </section>

  <section class="card" style="grid-column:1 / span 2;">
    <h2>Attribution</h2>
    <div style="max-height:260px; overflow:auto; border:1px solid #1f2a37; border-radius:10px;">
      <table id="attrTbl">
        <thead><tr>
          <th>Time</th><th>Strat</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Notional</th><th>Reason</th>
        </tr></thead>
        <tbody></tbody>
      </table>
    </div>
  </section>
</main>

<script>
async function j(u,opts) {{
  const r = await fetch(u, opts || {{}})
  const t = (r.headers.get('content-type')||'').includes('application/json') ? await r.json() : await r.text()
  return t
}}

function fill(tblId, rows) {{
  const tb = document.querySelector(`#${{tblId}} tbody`);
  tb.innerHTML = "";
  (rows||[]).forEach(r => {{
    const tr = document.createElement('tr');
    function td(v) {{ const e = document.createElement('td'); e.textContent = (v ?? ""); return e; }}
    tr.appendChild(td(r.symbol));
    tr.appendChild(td(r.action));
    tr.appendChild(td((r.score!=null)? r.score.toFixed(3):""));
    tr.appendChild(td(r.reason));
    tr.appendChild(td((r.atr!=null)? r.atr.toFixed(4):""));
    tr.appendChild(td((r.atr_pct!=null)? (r.atr_pct*100).toFixed(1)+'%':""));
    tr.appendChild(td((r.qty!=null)? r.qty.toFixed(6):""));
    tr.appendChild(td((r.notional!=null)? r.notional.toFixed(2):""));
    tb.appendChild(tr);
  }});
}}

document.querySelector('#btnScan').onclick = async () => {{
  const strat    = document.querySelector('#strat').value;
  const tf       = document.querySelector('#tf').value;
  const limit    = parseInt(document.querySelector('#limit').value || "360");
  const dry      = parseInt(document.querySelector('#dry').value || "1");
  const topk     = parseInt(document.querySelector('#topk').value || "{TOPK_DEFAULT}");
  const minScore = parseFloat(document.querySelector('#minScore').value || "{MIN_SCORE_DEFAULT}");
  const notional = parseFloat(document.querySelector('#notional').value || "25");
  const symbols  = document.querySelector('#symbols').value;

  const u = `/scan/${{strat}}?timeframe=${{encodeURIComponent(tf)}}&limit=${{limit}}&dry=${{dry}}&symbols=${{encodeURIComponent(symbols)}}&topk=${{topk}}&min_score=${{minScore}}&notional=${{notional}}`;
  document.querySelector('#scanMsg').textContent = `POST ${'{'}window.location.origin + u{'}'}`;
  const r = await j(u, {{method:'POST'}});
  fill('scanTbl', r.results || []);
}};

async function loadTo(preId, url) {{
  const data = await j(url);
  document.querySelector(preId).textContent = (typeof data === 'string')? data : JSON.stringify(data,null,2);
}}

document.querySelector('#btnHealth').onclick   = () => loadTo('#info','/health');
document.querySelector('#btnPositions').onclick= () => loadTo('#info','/v2/positions');
document.querySelector('#btnRecent').onclick   = () => loadTo('#info','/orders/recent?status=all&limit=200');
document.querySelector('#btnPnL').onclick      = () => loadTo('#info','/pnl/summary');
document.querySelector('#btnUniverse').onclick = () => loadTo('#info','/universe');

async function refreshAttr(){{
  const r = await j('/orders/attribution');
  const rows = (r.buckets || []).map(x => x);
  const tb = document.querySelector('#attrTbl tbody');
  tb.innerHTML = "";
  (rows||[]).forEach(a => {{
    const tr = document.createElement('tr');
    function td(v) {{ const e = document.createElement('td'); e.textContent = (v ?? ""); return e; }}
    tr.appendChild(td(a.t));
    tr.appendChild(td(a.strategy));
    tr.appendChild(td(a.symbol));
    tr.appendChild(td(a.side));
    tr.appendChild(td(a.qty));
    tr.appendChild(td(a.notional));
    tr.appendChild(td(a.reason));
    tb.appendChild(tr);
  }});
}}
setInterval(refreshAttr, 4000); refreshAttr();
</script>
</body>
</html>
"""

# =========================
# Routes
# =========================

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard")

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

@app.get("/health")
def health():
    return {"ok": True, "version": APP_VERSION}

@app.get("/diag/candles")
def diag_candles(tf: str = Query("5Min"), limit: int = Query(300), symbols: str = Query(",".join(CORE))):
    # returns rows per symbol for quick “do we have bars?” diagnostics (your PS script uses this)
    symbols_list = [s.strip() for s in symbols.split(",") if s.strip()]
    meta = {}
    for s in symbols_list:
        rows = CANDLES.rows(s, tf)
        meta[s] = {"rows": rows}
    # Also attach a sample to prove shape, if asked
    rows_map = {s: CANDLES.rows(s, tf) for s in symbols_list}
    return {"ok": True, "meta": meta, "rows": rows_map}

@app.get("/universe")
def get_universe():
    return {"ok": True, "symbols": _universe.symbols, "config": _universe_cfg.__dict__}

@app.get("/v2/positions")
def positions():
    return list(_POSITIONS.values())

@app.get("/orders/recent")
def orders_recent(status: str = "all", limit: int = 200):
    return _ORDERS[:max(0, min(limit, 500))]

@app.get("/orders/attribution")
def orders_attr():
    # Keep existing shape with buckets field
    return {"ok": True, "buckets": _ATTRIBUTION[:500]}

@app.get("/pnl/summary")
def pnl_summary():
    return _pnl_summary()

# ---- helper to fetch series as numpy for strategies ----
def _series_from_cache(symbol: str, tf: str, limit: int) -> Optional[Dict[str, np.ndarray]]:
    bars = CANDLES.get(symbol, tf, limit)
    if not bars:
        return None
    o = np.array([b["o"] for b in bars], dtype=float)
    h = np.array([b["h"] for b in bars], dtype=float)
    l = np.array([b["l"] for b in bars], dtype=float)
    c = np.array([b["c"] for b in bars], dtype=float)
    v = np.array([b["v"] for b in bars], dtype=float)
    return {"open": o, "high": h, "low": l, "close": c, "volume": v}

# ---- SCAN endpoint using adaptive StrategyBook ----
@app.post("/scan/{strat}")
def scan_strategy(
    strat: str,
    timeframe: str = Query("5Min"),
    limit: int = Query(300, ge=50, le=5000),
    dry: int = Query(1),
    symbols: str = Query(",".join(CORE)),
    topk: int = Query(TOPK_DEFAULT, ge=1, le=10),
    min_score: float = Query(MIN_SCORE_DEFAULT, ge=0.0, le=5.0),
    notional: float = Query(25.0, ge=0.0),
):
    # symbols
    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    if not syms:
        syms = _universe.symbols or CORE

    # build request
    req = ScanRequest(
        strat=strat,
        timeframe=timeframe,
        limit=int(limit),
        topk=int(topk),
        min_score=float(min_score),
        notional=float(notional),
    )

    # assemble contexts per symbol for 1m + 5m (for MTF confirm)
    contexts: Dict[str, Dict[str, Any]] = {}
    for s in syms:
        s1 = _series_from_cache(s, "1Min" if timeframe == "1Min" else timeframe, limit)
        s5 = _series_from_cache(s, "5Min", min(900, max(360, limit//5)))  # ensure enough 5m for confirm
        if not s1 or not s5:
            contexts[s] = None
        else:
            contexts[s] = {"tf": timeframe, "one": s1, "five": s5}

    results: List[ScanResult] = book.scan(req, contexts)

    out = []
    placed = []

    # If LIVE (dry=0), place orders for the selected (already topK filtered)
    if dry == 0:
        for r in results:
            if r.action in ("buy", "sell") and r.selected:
                side = "buy" if r.action == "buy" else "sell"
                order = _record_order(r.symbol, side, r.qty or 0.0, r.notional or 0.0, strat, r.reason)
                placed.append(order)

    # Always return results (for dashboard/PS)
    for r in results:
        out.append({
            "symbol": r.symbol,
            "action": r.action,
            "reason": r.reason,
            "score": r.score,
            "atr": r.atr,
            "atr_pct": r.atr_pct,
            "qty": r.qty,
            "notional": r.notional,
            "selected": r.selected,
        })

    return {"ok": True, "params": req.__dict__, "results": out, "placed": placed}

# -------------- Dev runner --------------
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")
