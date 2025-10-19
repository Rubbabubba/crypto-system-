
from __future__ import annotations

import os
import json
import math
import time
from pathlib import Path
from typing import Dict, List, Tuple, Any

import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware

# --------------------------------------------------------------------------------------
# Configuration & Policy files (kept under ./policy_config unless POLICY_CFG_DIR is set)
# --------------------------------------------------------------------------------------

POLICY_CFG_DIR = os.getenv("POLICY_CFG_DIR", str(Path(__file__).parent / "policy_config"))
WINDOWS_PATH   = Path(POLICY_CFG_DIR) / "windows.json"
WL_PATH        = Path(POLICY_CFG_DIR) / "whitelist.json"
BL_PATH        = Path(POLICY_CFG_DIR) / "blacklist.json"

def _load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[policy] failed to read {path}: {e}")
    return default

def _get_config() -> Dict[str, Any]:
    """
    Fetch /config from this service to avoid relying on globals.
    The app already exposes GET /config; we reuse it.
    """
    base = os.getenv("PUBLIC_BASE_URL", "http://127.0.0.1:10000")
    try:
        r = requests.get(f"{base}/config", timeout=3)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[policy] /config fetch failed: {e}")
        # last-resort defaults so /policy/eligible still responds
        return {
            "SYMBOLS": ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD","AVAX/USD","LINK/USD","BCH/USD","LTC/USD"],
            "STRATEGIES": ["c1","c2","c3","c4","c5","c6"]
        }

def _now_utc():
    import datetime as _dt
    return _dt.datetime.utcnow()

def _inside_window(win: Dict[str, Any], now) -> bool:
    # Example window schema: {"days":[1,2,3,4,5], "hours":[14,15,16], "tz":"UTC"}
    days  = win.get("days")   # 0=Mon .. 6=Sun
    hours = win.get("hours")
    if days is not None and (now.weekday() not in days):
        return False
    if hours is not None and (now.hour not in hours):
        return False
    return True

def _guard_reason(ok: bool, reason: str) -> str:
    return reason if not ok else "ok"

# --------------------------------------------------------------------------------------
# FastAPI app
# --------------------------------------------------------------------------------------

app = FastAPI(title="Crypto System", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------------------------------------------
# New: /policy/eligible (kept same path)
# --------------------------------------------------------------------------------------

@app.get("/policy/eligible")
def policy_eligible():
    cfg = _get_config()
    symbols: List[str]    = cfg.get("SYMBOLS", [])
    strategies: List[str] = cfg.get("STRATEGIES", [])

    windows = _load_json(WINDOWS_PATH, default={})              # { "c1": {"BTC/USD": {days:[],hours:[]}, ...}, ... }
    whitelist = _load_json(WL_PATH, default={})                 # { "c1": ["BTC/USD", ...], "c2": ["ETH/USD", ...], ... }
    blacklist = _load_json(BL_PATH, default={})                 # { "BTC/USD": ["c1","c2"], ... } (optional)

    now = _now_utc()

    eligible: List[Dict[str, Any]] = []
    blocked:  List[Dict[str, Any]] = []

    for strat in strategies:
        wl_for_strat = set(whitelist.get(strat, symbols))  # default allow all symbols if no whitelist provided
        for sym in symbols:
            # Check whitelist / blacklist
            if sym not in wl_for_strat:
                blocked.append({"strategy": strat, "symbol": sym, "reason": "not_in_strategy_whitelist"})
                continue
            if sym in blacklist and strat in set(blacklist.get(sym, [])):
                blocked.append({"strategy": strat, "symbol": sym, "reason": "strategy_blacklisted"})
                continue

            # Check windows
            strat_windows = windows.get(strat, {})
            sym_window = strat_windows.get(sym) or strat_windows.get(sym.replace("/", ""))  # tolerate BTC/USD vs BTCUSD
            if sym_window:
                ok = _inside_window(sym_window, now)
                if not ok:
                    blocked.append({"strategy": strat, "symbol": sym, "reason": f"outside_window hour={now.hour} dow={['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][now.weekday()]}" })
                    continue

            # If we made it here, this pair is tradable now
            eligible.append({"strategy": strat, "symbol": sym})

    return {"ok": True, "time": now.isoformat() + "Z", "eligible": eligible, "blocked": blocked}

# --------------------------------------------------------------------------------------
# Dashboard (kept at /dashboard). We remove noisy sections and add "Tradable Now".
# No changes to other routes.
# --------------------------------------------------------------------------------------

DASHBOARD_HTML = """\
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Crypto System</title>
  <style>
    body { font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 0; color: #111; background:#0b0e11; }
    header { padding: 16px 20px; background: #0f172a; color: #f8fafc; display:flex; align-items:center; gap:16px; flex-wrap:wrap;}
    .pill { background:#1e293b; padding:6px 10px; border-radius:999px; font-size:12px; color:#e2e8f0;}
    main { padding: 18px; max-width: 1200px; margin: 0 auto; }
    h2 { margin: 18px 0 10px; color:#e2e8f0; }
    .cards { display:grid; grid-template-columns: repeat(auto-fill, minmax(220px,1fr)); gap:12px; }
    .card { background:#111827; border:1px solid #1f2937; border-radius:10px; padding:12px; color:#e5e7eb; }
    .muted { color:#94a3b8; font-size:12px; }
    table { width:100%; border-collapse: collapse; color:#e5e7eb; background:#111827; border:1px solid #1f2937; border-radius:10px; overflow:hidden; }
    th, td { text-align:left; padding:8px 10px; border-bottom:1px solid #1f2937; }
    th { background:#0f172a; color:#cbd5e1; }
    .ok { color:#22c55e; }
    .bad { color:#ef4444; }
    .warn { color:#f59e0b; }
    .grid { display: grid; grid-template-columns: repeat(auto-fill,minmax(180px,1fr)); gap:10px; }
    .mini { font-size:12px; }
  </style>
</head>
<body>
  <header>
    <div><strong>Crypto System</strong></div>
    <div id="status" class="pill">loading…</div>
    <div class="pill"><span id="now"></span></div>
  </header>

  <main>
    <section>
      <h2>PnL (Summary)</h2>
      <div class="cards" id="pnl-cards">
        <div class="card"><div class="muted">Realized</div><div id="p_realized">$0</div></div>
        <div class="card"><div class="muted">Unrealized</div><div id="p_unrealized">$0</div></div>
        <div class="card"><div class="muted">Fees</div><div id="p_fees">$0</div></div>
        <div class="card"><div class="muted">Equity</div><div id="p_equity">$0</div></div>
      </div>
    </section>

    <section>
      <h2>Tradable Now</h2>
      <div class="grid" id="tradable"></div>
      <div class="mini muted" id="tradable-note"></div>
    </section>

    <section>
      <h2>Prices</h2>
      <div class="grid" id="quotes"></div>
    </section>
  </main>

<script>
const base = location.origin;

function fmtUSD(n){ try { return n.toLocaleString(undefined,{style:'currency',currency:'USD'}); } catch{ return '$'+n }; }
function el(tag, cls){ const d=document.createElement(tag); if(cls) d.className=cls; return d; }

async function loadConfig(){
  const r = await fetch(base+'/config'); return await r.json();
}
async function loadPnL(){
  const r = await fetch(base+'/pnl/summary'); return await r.json();
}
async function loadEligible(){
  const r = await fetch(base+'/policy/eligible'); return await r.json();
}
async function loadPrice(sym){
  const r = await fetch(base+'/price/'+encodeURIComponent(sym)); return await r.json();
}

async function refresh(){
  document.getElementById('now').textContent = new Date().toLocaleString();
  // PnL
  try {
    const pnl = await loadPnL();
    const t = pnl.total || {realized:0,unrealized:0,fees:0,equity:0};
    document.getElementById('p_realized').textContent  = fmtUSD(t.realized||0);
    document.getElementById('p_unrealized').textContent= fmtUSD(t.unrealized||0);
    document.getElementById('p_fees').textContent      = fmtUSD(t.fees||0);
    document.getElementById('p_equity').textContent    = fmtUSD(t.equity||0);
  } catch(e){
    console.error(e);
  }

  // Tradable Now
  const trad = document.getElementById('tradable');
  trad.innerHTML='';
  try {
    const e = await loadEligible();
    (e.eligible||[]).forEach(row => {
      const c = el('div','card');
      c.innerHTML = '<div class="muted">'+row.strategy+'</div><div><strong>'+row.symbol+'</strong></div>';
      trad.appendChild(c);
    });
    document.getElementById('tradable-note').textContent = (e.blocked||[]).length + ' blocked by policy';
  } catch(e) {
    console.error(e);
  }

  // Quotes (best-effort)
  try {
    const cfg = await loadConfig();
    const syms = cfg.SYMBOLS||[];
    const q = document.getElementById('quotes'); q.innerHTML='';
    for(const s of syms){
      try {
        const r = await loadPrice(s);
        const c = el('div','card');
        c.innerHTML = '<div class="muted">'+s+'</div><div><strong>'+ (r.price ?? '—') +'</strong></div>';
        q.appendChild(c);
      } catch {}
    }
  } catch(e) {}
}

setInterval(refresh, 15000);
refresh();
</script>
</body>
</html>
"""

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

# -----------------------------
# Health
# -----------------------------
@app.get("/healthz")
def healthz():
    return {"ok": True, "policy_dir": POLICY_CFG_DIR}
