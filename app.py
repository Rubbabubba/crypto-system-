# app.py — Crypto Trading Service with Full Dashboard (v1.11.0)
# - Rich HTML Dashboard (/dashboard) with live sections:
#   Health, Diag Candles, Positions, Recent Orders, P&L, Attribution, Scan Console
# - /health, /diag/candles, /scan/{strategy} with bar guarantees (backfill + 1m→5m resample)
# - Minimal in-memory stubs for positions/orders/pnl/attribution to keep UI working
# - Adapters to wire your real storage/provider: load_candles, save_candles, fetch_ohlcv
#
# ENV (Render):
#   SYMBOLS=BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD
#   REQUIRED_BARS_5M=300
#   REQUIRED_BARS_1M=1500
#   OVERSHOOT_MULT=2.0
#   FETCH_PAGE_LIMIT=1000
#   PROVIDER=demo

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from typing import List, Dict, Any, Optional
import threading
import time
import math
import os
import random

APP_VERSION = "1.11.0"

app = FastAPI(title="Crypto Trading Service", version=APP_VERSION)

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
SYMBOLS = [s.strip() for s in os.getenv(
    "SYMBOLS",
    "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD"
).split(",") if s.strip()]

NEED_5M = int(os.getenv("REQUIRED_BARS_5M", "300"))
NEED_1M = int(os.getenv("REQUIRED_BARS_1M", "1500"))
OVERSHOOT = float(os.getenv("OVERSHOOT_MULT", "2.0"))
PER_CALL = int(os.getenv("FETCH_PAGE_LIMIT", "1000"))
PROVIDER = os.getenv("PROVIDER", "demo").lower()

BAR_MS = {"1Min": 60_000, "5Min": 300_000, "15Min": 900_000,
          "1m": 60_000, "5m": 300_000, "15m": 900_000}

def bar_ms(tf: str) -> int:
    key = tf if tf in BAR_MS else tf.lower()
    if key not in BAR_MS:
        raise ValueError(f"Unsupported timeframe: {tf}")
    return BAR_MS[key]

def need_for(tf: str) -> int:
    k = tf.lower()
    if k in ("5m","5min"): return NEED_5M
    if k in ("1m","1min"): return NEED_1M
    return NEED_5M

# ─────────────────────────────────────────────────────────────
# In-memory store (replace with your DB/cache)
# ─────────────────────────────────────────────────────────────
# Key: (symbol, timeframe) -> list[[ms, o,h,l,c,v], ...] sorted asc
_STORE: Dict[tuple, List[List[float]]] = {}

def load_candles(symbol: str, timeframe: str) -> List[List[float]]:
    return _STORE.get((symbol, timeframe), [])

def save_candles(symbol: str, timeframe: str, rows: List[List[float]]) -> None:
    _STORE[(symbol, timeframe)] = rows

# ─────────────────────────────────────────────────────────────
# Data provider adapter (replace with your vendor/ccxt)
# ─────────────────────────────────────────────────────────────
def fetch_ohlcv(symbol: str, timeframe: str, since_ms: int, limit: int) -> List[List[float]]:
    """
    Return ascending OHLCV rows: [ms, o, h, l, c, v].
    Replace this with your real provider (ccxt/vendor). This demo returns synthetic bars.
    """
    if PROVIDER != "demo":
        # TODO: Implement your provider (ccxt etc.) and honor since_ms/limit pagination.
        raise RuntimeError("Non-demo provider not wired yet. Set PROVIDER=demo or implement fetch_ohlcv().")

    tf_ms = bar_ms(timeframe)
    now = int(time.time()*1000)
    start = since_ms if since_ms else now - tf_ms*limit
    start = (start // tf_ms) * tf_ms
    out = []
    base_price = 10000 + abs(hash(symbol)) % 50000
    rng = random.Random(abs(hash(symbol + timeframe)) % (2**31 - 1))
    ts = start
    price = base_price
    for _ in range(limit):
        drift = rng.uniform(-2.0, 2.0)
        spread = abs(rng.uniform(0.0, 5.0))
        o = price
        c = max(10.0, o + drift)
        h = max(o, c) + spread
        l = min(o, c) - spread
        v = abs(rng.uniform(1.0, 100.0))
        out.append([ts, float(o), float(h), float(l), float(c), float(v)])
        price = c
        ts += tf_ms
        if ts > now + tf_ms*2:
            break
    return out

# ─────────────────────────────────────────────────────────────
# Backfill helpers
# ─────────────────────────────────────────────────────────────
def merge_and_clean(existing: List[List[float]], new_rows: List[List[float]]) -> List[List[float]]:
    by_ts: Dict[int, List[float]] = { int(r[0]): r for r in existing if r and len(r) >= 5 }
    for r in new_rows:
        if r and len(r) >= 5:
            by_ts[int(r[0])] = r
    rows = [by_ts[k] for k in sorted(by_ts.keys())]
    rows = [r for r in rows if r[0] > 0]
    return rows

def fetch_paged(symbol: str, timeframe: str, since_ms: int, min_bars: int) -> List[List[float]]:
    out: List[List[float]] = []
    cursor = since_ms
    while len(out) < min_bars:
        want = min(PER_CALL, max(1, min_bars - len(out)))
        batch = fetch_ohlcv(symbol, timeframe, cursor, want)
        if not batch:
            break
        out.extend(batch)
        cursor = int(batch[-1][0]) + 1
        time.sleep(0.02)
    return out

def ensure_bars(symbol: str, timeframe: str, require: Optional[int] = None) -> List[List[float]]:
    need = require or need_for(timeframe)
    have = load_candles(symbol, timeframe)
    if len(have) >= need:
        return have
    lookback = int(bar_ms(timeframe) * need * OVERSHOOT)
    since = int((have[0][0] if have else int(time.time()*1000)) - lookback)
    fetched = fetch_paged(symbol, timeframe, since, min_bars=need)
    merged = merge_and_clean(have, fetched)
    save_candles(symbol, timeframe, merged)
    return merged

def resample_1m_to_5m(one_min_rows: List[List[float]]) -> List[List[float]]:
    if not one_min_rows:
        return []
    out: List[List[float]] = []
    bucket_ms = bar_ms("5Min")
    cur = None
    o = h = l = c = v = None
    for ts, O, H, L, C, V in one_min_rows:
        b = (int(ts) // bucket_ms) * bucket_ms
        if cur is None or b != cur:
            if cur is not None:
                out.append([cur, o, h, l, c, v])
            cur = b
            o, h, l, c, v = O, H, L, C, V
        else:
            h = max(h, H); l = min(l, L); c = C; v = (v or 0) + (V or 0)
    if cur is not None:
        out.append([cur, o, h, l, c, v])
    return out

def ensure_bars_5m_via_1m(symbol: str, need_5m: int) -> List[List[float]]:
    one_min = ensure_bars(symbol, "1Min", require=max(NEED_1M, need_5m*5))
    five = resample_1m_to_5m(one_min)
    if len(five) >= need_5m:
        save_candles(symbol, "5Min", five)
    return five

def startup_backfill(background=True):
    def _run():
        for tf in ("1Min","5Min"):
            need = need_for(tf)
            lookback_ms = int(bar_ms(tf) * need * OVERSHOOT)
            since = int(time.time()*1000) - lookback_ms
            for sym in SYMBOLS:
                try:
                    have = load_candles(sym, tf)
                    if len(have) >= need:
                        continue
                    rows = fetch_paged(sym, tf, since, min_bars=need)
                    merged = merge_and_clean(have, rows)
                    save_candles(sym, tf, merged)
                    print(f"[backfill] {sym} {tf}: have={len(merged)} need={need}")
                except Exception as e:
                    print(f"[backfill] WARN {sym} {tf}: {e}")
    if background:
        threading.Thread(target=_run, daemon=True).start()
    else:
        _run()

# ─────────────────────────────────────────────────────────────
# Startup
# ─────────────────────────────────────────────────────────────
@app.on_event("startup")
def _on_start():
    startup_backfill(background=True)

# ─────────────────────────────────────────────────────────────
# Root redirects to dashboard
# ─────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/dashboard")

# ─────────────────────────────────────────────────────────────
# HTML Dashboard
# ─────────────────────────────────────────────────────────────
@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    # Render a single-page dashboard with embedded JS
    sym_opts = "".join([f'<option value="{s}">{s}</option>' for s in SYMBOLS])
    html = f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <title>Crypto Trading Dashboard · v{APP_VERSION}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root {{
      --fg:#0b1320; --muted:#6b7280; --card:#ffffff; --line:#e5e7eb; --accent:#0ea5e9; --bg:#f8fafc;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; background:var(--bg); color:var(--fg); font-family:system-ui,-apple-system,Segoe UI,Roboto,Inter,Arial,sans-serif; }}
    header {{ padding:20px 24px; border-bottom:1px solid var(--line); background:#fff; position:sticky; top:0; z-index:10; }}
    h1 {{ margin:0; font-size:20px; }}
    .version {{ color:var(--muted); font-weight:normal; }}
    main {{ padding:24px; }}
    .grid {{ display:grid; grid-template-columns: repeat(12, 1fr); grid-gap:16px; }}
    .card {{ grid-column: span 6; background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px; }}
    .card.wide {{ grid-column: span 12; }}
    h3 {{ margin:0 0 12px 0; font-size:16px; }}
    table {{ width:100%; border-collapse: collapse; }}
    th, td {{ padding:8px 10px; border-bottom:1px solid var(--line); text-align:left; font-size:13px; }}
    th {{ background:#f3f4f6; }}
    code, pre {{ background:#0b1020; color:#e5e7eb; padding:10px; border-radius:8px; display:block; white-space:pre-wrap; }}
    .row {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; }}
    select, input[type="text"], input[type="number"] {{
      border:1px solid var(--line); border-radius:8px; padding:8px 10px; font-size:13px; background:#fff;
    }}
    button {{
      border:1px solid var(--accent); color:#fff; background:var(--accent);
      padding:8px 12px; border-radius:8px; font-size:13px; cursor:pointer;
    }}
    .muted {{ color:var(--muted); }}
    .ok {{ color: #059669; }}
    .warn {{ color: #b45309; }}
    .err {{ color: #dc2626; }}
    .tiny {{ font-size:12px; }}
  </style>
</head>
<body>
  <header>
    <h1>Crypto Trading Dashboard <span class="version">v{APP_VERSION}</span></h1>
    <div class="tiny muted">Symbols: {", ".join(SYMBOLS)} · Provider: {PROVIDER} · Overshoot: {OVERSHOOT}</div>
  </header>
  <main>
    <div class="grid">
      <div class="card">
        <h3>Health</h3>
        <div id="health">Checking...</div>
      </div>

      <div class="card">
        <h3>Diag Candles</h3>
        <div class="row">
          <label class="tiny muted">TF</label>
          <select id="diag_tf">
            <option value="5Min">5Min</option>
            <option value="1Min">1Min</option>
          </select>
          <label class="tiny muted">Limit</label>
          <input type="number" id="diag_limit" value="300" min="1" step="1" style="width:90px"/>
          <label class="tiny muted">Add Symbol</label>
          <select id="diag_sym">{sym_opts}</select>
          <button id="diag_add">Add</button>
          <button id="diag_go">Run</button>
        </div>
        <div class="tiny muted" id="diag_syms">Symbols: (none)</div>
        <table>
          <thead><tr><th>Symbol</th><th>Rows</th></tr></thead>
          <tbody id="diag_table"></tbody>
        </table>
      </div>

      <div class="card">
        <h3>Positions</h3>
        <table>
          <thead><tr><th>Symbol</th><th>Qty</th><th>Avg</th><th>Mkt</th><th>UPNL</th></tr></thead>
          <tbody id="pos_table"></tbody>
        </table>
      </div>

      <div class="card">
        <h3>Recent Orders</h3>
        <table>
          <thead><tr><th>Time</th><th>Sym</th><th>Side</th><th>Notional</th><th>Qty</th><th>Status</th><th>CID</th></tr></thead>
          <tbody id="ord_table"></tbody>
        </table>
      </div>

      <div class="card">
        <h3>P&amp;L</h3>
        <div id="pnl_box" class="row tiny">
          <div><b>Realized:</b> <span id="pnl_realized">—</span></div>
          <div><b>Unrealized:</b> <span id="pnl_unrealized">—</span></div>
          <div><b>Total:</b> <span id="pnl_total">—</span></div>
        </div>
      </div>

      <div class="card">
        <h3>Attribution</h3>
        <table>
          <thead><tr><th>Strategy</th><th>Orders</th><th>Symbols</th></tr></thead>
          <tbody id="attr_table"></tbody>
        </table>
      </div>

      <div class="card wide">
        <h3>Scan Console</h3>
        <div class="row">
          <label class="tiny muted">Strategy</label>
          <select id="scan_strategy">
            <option value="c1">c1</option><option value="c2">c2</option><option value="c3">c3</option>
            <option value="c4">c4</option><option value="c5">c5</option><option value="c6">c6</option>
          </select>
          <label class="tiny muted">TF</label>
          <select id="scan_tf">
            <option value="5Min">5Min</option>
            <option value="1Min">1Min</option>
          </select>
          <label class="tiny muted">Limit</label>
          <input type="number" id="scan_limit" value="300" min="1" step="1" style="width:90px"/>
          <label class="tiny muted">Dry</label>
          <select id="scan_dry"><option value="1">Yes</option><option value="0">No (PAPER)</option></select>
          <label class="tiny muted">Symbols</label>
          <input type="text" id="scan_symbols" value="{",".join(SYMBOLS)}" style="width:380px"/>
          <button id="scan_go">Run Scan</button>
        </div>
        <pre id="scan_output" class="tiny">No scan yet.</pre>
      </div>
    </div>
  </main>

  <script>
  const $ = sel => document.querySelector(sel);
  const $$ = sel => Array.from(document.querySelectorAll(sel));

  async function getJSON(url) {{
    const r = await fetch(url);
    return r.json();
  }}

  function setText(id, txt) {{ const el = document.getElementById(id); if(el) el.textContent = txt; }}

  async function refreshHealth() {{
    try {{
      const j = await getJSON('/health');
      $('#health').innerHTML = `<span class="ok">OK</span> · version <b>${{j.version}}</b>`;
    }} catch(e) {{
      $('#health').innerHTML = `<span class="err">ERROR</span>`;
    }}
  }}

  async function refreshPositions() {{
    const t = $('#pos_table'); t.innerHTML = '';
    try {{
      const j = await getJSON('/v2/positions');
      if (!j || j.length===0) {{
        t.innerHTML = '<tr><td colspan="5" class="muted tiny">No open positions.</td></tr>';
        return;
      }}
      for (const p of j) {{
        t.innerHTML += `<tr><td>${{p.symbol||''}}</td><td>${{p.qty||''}}</td><td>${{p.avg_entry_price||''}}</td><td>${{p.current_price||''}}</td><td>${{p.unrealized_pl||''}}</td></tr>`;
      }}
    }} catch(e) {{
      t.innerHTML = '<tr><td colspan="5" class="err tiny">Error loading positions.</td></tr>';
    }}
  }}

  async function refreshOrders() {{
    const t = $('#ord_table'); t.innerHTML = '';
    try {{
      const j = await getJSON('/orders/recent');
      if (!j || j.length===0) {{
        t.innerHTML = '<tr><td colspan="7" class="muted tiny">No recent orders.</td></tr>';
        return;
      }}
      for (const o of j.slice(0,50)) {{
        t.innerHTML += `<tr>
          <td>${{o.submitted_at||''}}</td><td>${{o.symbol||''}}</td><td>${{o.side||''}}</td>
          <td>${{o.notional||''}}</td><td>${{o.qty||''}}</td><td>${{o.status||''}}</td>
          <td class="tiny">${{o.client_order_id||''}}</td>
        </tr>`;
      }}
    }} catch(e) {{
      t.innerHTML = '<tr><td colspan="7" class="err tiny">Error loading orders.</td></tr>';
    }}
  }}

  async function refreshPnl() {{
    try {{
      const j = await getJSON('/pnl/summary');
      setText('pnl_realized', (j.realized??'—'));
      setText('pnl_unrealized', (j.unrealized??'—'));
      setText('pnl_total', (j.total??'—'));
    }} catch(e) {{
      setText('pnl_realized', 'ERR'); setText('pnl_unrealized', 'ERR'); setText('pnl_total', 'ERR');
    }}
  }}

  async function refreshAttr() {{
    const t = $('#attr_table'); t.innerHTML = '';
    try {{
      const j = await getJSON('/orders/attribution');
      const buckets = (j && j.buckets) || {{}};
      const keys = Object.keys(buckets);
      if (keys.length===0) {{
        t.innerHTML = '<tr><td colspan="3" class="muted tiny">No attribution yet.</td></tr>';
        return;
      }}
      for (const k of keys) {{
        const b = buckets[k];
        t.innerHTML += `<tr><td>${{k}}</td><td>${{b.count||0}}</td><td class="tiny">${{(b.symbols||[]).join(', ')}}</td></tr>`;
      }}
    }} catch(e) {{
      t.innerHTML = '<tr><td colspan="3" class="err tiny">Error loading attribution.</td></tr>';
    }}
  }}

  // Diag Candles
  const diagSymbols = new Set();
  function updateDiagSymsLabel() {{
    $('#diag_syms').textContent = 'Symbols: ' + (Array.from(diagSymbols).join(', ') || '(none)');
  }}
  $('#diag_add').addEventListener('click', () => {{
    const val = $('#diag_sym').value;
    if (val) diagSymbols.add(val);
    updateDiagSymsLabel();
  }});
  $('#diag_go').addEventListener('click', async () => {{
    const tf = $('#diag_tf').value;
    const limit = $('#diag_limit').value;
    const syms = Array.from(diagSymbols);
    const t = $('#diag_table'); t.innerHTML = '';
    if (syms.length===0) {{
      t.innerHTML = '<tr><td colspan="2" class="tiny muted">Add at least one symbol.</td></tr>';
      return;
    }}
    try {{
      const url = `/diag/candles?tf=${{encodeURIComponent(tf)}}&limit=${{encodeURIComponent(limit)}}&symbols=${{encodeURIComponent(syms.join(','))}}`;
      const j = await getJSON(url);
      const rows = j.rows || j.meta || {{}};
      const keys = Object.keys(rows);
      if (keys.length===0) {{
        t.innerHTML = '<tr><td colspan="2" class="tiny muted">No data.</td></tr>';
        return;
      }}
      for (const k of keys) {{
        const r = j.rows ? j.rows[k] : (j.meta[k] ? j.meta[k].rows : 0);
        t.innerHTML += `<tr><td>${{k}}</td><td>${{r}}</td></tr>`;
      }}
    }} catch(e) {{
      t.innerHTML = '<tr><td colspan="2" class="err tiny">Diag error.</td></tr>';
    }}
  }});

  // Scan Console
  $('#scan_go').addEventListener('click', async () => {{
    const strat = $('#scan_strategy').value;
    const tf = $('#scan_tf').value;
    const limit = $('#scan_limit').value;
    const dry = $('#scan_dry').value;
    const syms = $('#scan_symbols').value;

    $('#scan_output').textContent = 'Running...';
    try {{
      const url = `/scan/${{encodeURIComponent(strat)}}?dry=${{encodeURIComponent(dry)}}&timeframe=${{encodeURIComponent(tf)}}&limit=${{encodeURIComponent(limit)}}&symbols=${{encodeURIComponent(syms)}}`;
      const r = await fetch(url, {{ method: 'POST' }});
      const j = await r.json();
      $('#scan_output').textContent = JSON.stringify(j, null, 2);
      // light refresh of orders/attr after LIVE
      if (dry === '0') {{ refreshOrders(); refreshAttr(); }}
    }} catch(e) {{
      $('#scan_output').textContent = 'Scan error: ' + e;
    }}
  }});

  // initial load + 15s auto-refresh (health, positions, orders, pnl, attr)
  async function boot() {{
    updateDiagSymsLabel();
    await Promise.all([refreshHealth(), refreshPositions(), refreshOrders(), refreshPnl(), refreshAttr()]);
    setInterval(refreshHealth, 15000);
    setInterval(refreshPositions, 15000);
    setInterval(refreshOrders, 15000);
    setInterval(refreshPnl, 15000);
    setInterval(refreshAttr, 30000);
  }}
  boot();
  </script>
</body>
</html>
"""
    return HTMLResponse(html)

# ─────────────────────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"ok": True, "version": APP_VERSION}

# ─────────────────────────────────────────────────────────────
# Diag: summarize available candles
# ─────────────────────────────────────────────────────────────
@app.get("/diag/candles")
def diag_candles(
    symbols: str = Query(..., description="Comma-separated symbols (e.g., BTC/USD,ETH/USD)"),
    tf: str = Query("5Min"), limit: int = Query(300)
):
    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    rows_map: Dict[str, int] = {}
    meta: Dict[str, Any] = {}
    for s in syms:
        have = load_candles(s, tf)
        rows_map[s] = len(have)
        meta[s] = {"rows": len(have)}
    total = sum(rows_map.values())
    return {"ok": True, "tf": tf, "limit": limit, "rows": rows_map, "meta": meta, "total": total}

# ─────────────────────────────────────────────────────────────
# Compat placeholders (positions/orders/pnl/attribution)
# ─────────────────────────────────────────────────────────────
_FAKE_ORDERS: List[Dict[str, Any]] = []

@app.get("/v2/positions")
def positions():
    return []  # replace with your broker positions

@app.get("/orders/recent")
def orders_recent():
    return list(reversed(_FAKE_ORDERS))[:100]

@app.get("/pnl/summary")
def pnl_summary():
    realized = 4141.39
    unrealized = 0.0
    return {"ok": True, "realized": realized, "unrealized": unrealized, "total": realized + unrealized}

@app.get("/orders/attribution")
def orders_attribution():
    buckets: Dict[str, Dict[str, Any]] = {}
    for o in _FAKE_ORDERS:
        strat = o.get("strategy", "other")
        b = buckets.setdefault(strat, {"count": 0, "symbols": []})
        b["count"] += 1
        sym = o.get("symbol")
        if sym and sym not in b["symbols"]:
            b["symbols"].append(sym)
    return {"ok": True, "buckets": buckets}

# ─────────────────────────────────────────────────────────────
# Scan endpoint (bars guaranteed; 5m→1m fallback if needed)
# ─────────────────────────────────────────────────────────────
@app.post("/scan/{strategy}")
def scan(
    strategy: str,
    dry: int = Query(1),
    timeframe: str = Query("5Min"),
    limit: int = Query(300),
    symbols: str = Query(",".join(SYMBOLS))
):
    syms = [s.strip() for s in symbols.split(",") if s.strip()]
    required = max(limit, need_for(timeframe))

    # Ensure data for each symbol
    have_map: Dict[str, int] = {}
    for sym in syms:
        rows = ensure_bars(sym, timeframe, required)
        if timeframe.lower() in ("5m","5min") and len(rows) < required:
            rows = ensure_bars_5m_via_1m(sym, required)
        have_map[sym] = len(rows)

    results: List[Dict[str, Any]] = []
    placed: List[Dict[str, Any]] = []

    for sym in syms:
        have = have_map.get(sym, 0)
        if have < required:
            results.append({"symbol": sym, "action": "flat", "reason": "insufficient_bars_provider_limit"})
            continue

        candles = load_candles(sym, timeframe)

        # TODO plug in your real strategy here
        signal = {"symbol": sym, "action": "flat", "reason": "ok_bars_ready"}

        results.append(signal)

        if dry == 0 and signal.get("action") in ("buy","sell"):
            order = {
                "submitted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "symbol": sym,
                "side": signal["action"],
                "qty": signal.get("qty"),
                "notional": signal.get("notional"),
                "status": "filled",
                "client_order_id": f"{strategy}-{sym.replace('/','')}-{int(time.time())}",
                "strategy": strategy,
            }
            _FAKE_ORDERS.append(order)
            placed.append(order)

    return {
        "ok": True,
        "have": have_map,
        "results": results,
        "placed": placed,
        "params": {"strategy": strategy, "timeframe": timeframe, "limit": limit, "symbols": syms}
    }

if __name__ == "__main__":
    import os
    import uvicorn
    # Render provides PORT in the environment
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
