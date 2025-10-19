
from __future__ import annotations

import os
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional

import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

POLICY_CFG_DIR = os.getenv("POLICY_CFG_DIR", str(Path(__file__).parent / "policy_config"))
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")

def _load_json_file(p: Path) -> Any:
    if not p.exists():
        return None
    try:
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def load_policy(policy_dir: str | Path) -> Dict[str, Any]:
    d = Path(policy_dir)
    return {
        "whitelist": _load_json_file(d / "whitelist.json") or {},
        "blacklist": _load_json_file(d / "blacklist.json") or {},
        "windows":   _load_json_file(d / "windows.json") or {},
    }

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

_DOW_MAP = {"Mon":0,"Tue":1,"Wed":2,"Thu":3,"Fri":4,"Sat":5,"Sun":6}

def _in_window(now: datetime, window: Dict[str, Any]) -> bool:
    if not window:
        return True
    if "days" in window:
        dow = now.weekday()
        allowed_days = {_DOW_MAP.get(d, -1) for d in window["days"]}
        if dow not in allowed_days:
            return False
    if "hours" in window:
        if now.hour not in set(window["hours"]):
            return False
    return True

def guard_allows(strategy: str, symbol: str, policy: Dict[str, Any], now: Optional[datetime] = None):
    now = now or _now_utc()
    wl = policy.get("whitelist") or {}
    bl = policy.get("blacklist") or {}
    win = policy.get("windows") or {}

    sym_norm = symbol.replace("-", "/").replace("\\", "/")

    bl_syms = set((bl.get("symbols") or []))
    bl_strats = set((bl.get("strategies") or []))
    if sym_norm in bl_syms:
        return False, "blacklist_symbol"
    if strategy in bl_strats:
        return False, "blacklist_strategy"

    wl_syms = set((wl.get("symbols") or []))
    wl_strats = set((wl.get("strategies") or []))
    if wl and wl_syms and sym_norm not in wl_syms:
        return False, "not_in_symbol_whitelist"
    if wl and wl_strats and strategy not in wl_strats:
        return False, "not_in_strategy_whitelist"

    sym_win = (win.get("symbols") or {}).get(sym_norm) or {}
    if sym_win and not _in_window(now, sym_win):
        reason = f"outside_window hour={now.hour:02d} dow={['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][now.weekday()]}"
        return False, reason

    strat_win = (win.get("strategies") or {}).get(strategy) or {}
    if strat_win and not _in_window(now, strat_win):
        reason = f"outside_window hour={now.hour:02d} dow={['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][now.weekday()]}"
        return False, reason

    return True, ""

app = FastAPI(title="Crypto System")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def _base_url_from_request(request: Request) -> str:
    if PUBLIC_BASE_URL:
        return PUBLIC_BASE_URL.rstrip("/")
    return str(request.base_url).rstrip("/")

def get_config(request: Request) -> Dict[str, Any]:
    base = _base_url_from_request(request)
    try:
        r = requests.get(f"{base}/config", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}

@app.get("/policy/eligible", response_class=JSONResponse)
def policy_eligible(request: Request):
    cfg = get_config(request)
    symbols = cfg.get("SYMBOLS") or cfg.get("symbols") or []
    strategies = cfg.get("STRATEGIES") or cfg.get("strategies") or []

    policy = load_policy(POLICY_CFG_DIR)
    now = _now_utc()

    eligible = []
    blocked = []

    for sym in symbols:
        for strat in strategies:
            ok, reason = guard_allows(strat, sym, policy, now=now)
            if ok:
                eligible.append({"strategy": strat, "symbol": sym})
            else:
                blocked.append({"strategy": strat, "symbol": sym, "reason": reason})

    return {"ok": True, "time": now.isoformat(), "eligible": eligible, "blocked": blocked}

DASHBOARD_HTML = """
<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'/>
  <meta name='viewport' content='width=device-width, initial-scale=1'/>
  <title>Crypto System - Dashboard</title>
  <style>
    :root { color-scheme: dark; }
    body { margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; background:#0b0f14; color:#d8e1ea; }
    header { padding: 16px 20px; border-bottom: 1px solid #121a22; background:#0d131a; }
    h1 { margin:0; font-size: 18px; letter-spacing: .3px; color:#e8f1fa; }
    main { padding: 18px 20px 80px; display:grid; gap:18px; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); }
    .card { background:#0f1620; border:1px solid #14202b; border-radius:12px; padding:14px; }
    .card h2 { margin:0 0 10px 0; font-size:14px; color:#96b2c9; text-transform:uppercase; letter-spacing:.8px;}
    .grid { display:grid; gap:10px; }
    .row { display:flex; justify-content:space-between; gap:10px; font-variant-numeric: tabular-nums; }
    .muted { color:#7f97ab; }
    .pill { display:inline-block; padding:2px 8px; border-radius:999px; background:#0c1b2a; border:1px solid #1c2c3a; color:#b8d4ee; font-size:12px; margin:2px 6px 2px 0; }
    .ok { background:#0c2a1a; border-color:#18442c; color:#a5e7c4; }
  </style>
</head>
<body>
  <header><h1>Crypto System</h1></header>
  <main>
    <section class='card'>
      <h2>PNL Summary</h2>
      <div id='pnl' class='grid'>
        <div class='row'><span class='muted'>Realized</span><span class='mono' id='p_realized'>…</span></div>
        <div class='row'><span class='muted'>Unrealized</span><span class='mono' id='p_unrealized'>…</span></div>
        <div class='row'><span class='muted'>Fees</span><span class='mono' id='p_fees'>…</span></div>
        <div class='row'><span class='muted'>Equity</span><span class='mono' id='p_equity'>…</span></div>
      </div>
    </section>

    <section class='card'>
      <h2>Tradable Now</h2>
      <div id='tradable'>
        <div class='muted'>Loading eligibility…</div>
      </div>
    </section>

    <section class='card'>
      <h2>Prices</h2>
      <div id='prices' class='grid'></div>
    </section>
  </main>

<script>
const fmt = new Intl.NumberFormat(undefined, {maximumFractionDigits: 2});
const base = location.origin;

async function loadPNL() {
  try {
    const r = await fetch(base + "/pnl/summary");
    const j = await r.json();
    const t = j.total || {};
    document.getElementById("p_realized").textContent = fmt.format(t.realized || 0);
    document.getElementById("p_unrealized").textContent = fmt.format(t.unrealized || 0);
    document.getElementById("p_fees").textContent = fmt.format(t.fees || 0);
    document.getElementById("p_equity").textContent = fmt.format(t.equity || 0);
  } catch(e) {
    document.getElementById("pnl").innerHTML = '<div class="muted">Error loading PnL</div>';
  }
}

async function loadConfig() {
  const r = await fetch(base + "/config");
  return await r.json();
}

async function loadEligible() {
  try {
    const r = await fetch(base + "/policy/eligible");
    const j = await r.json();
    const wrap = document.getElementById("tradable");
    const ok = j.eligible || [];
    const blocked = j.blocked || [];

    if (ok.length === 0) {
      wrap.innerHTML = '<div class="muted">No (strategy, coin) pairs currently tradable under policy.</div>';
      return;
    }
    const pills = ok.map(x => '<span class="pill ok">' + x.strategy + ' · ' + x.symbol + '</span>').join('');
    const blockedCount = blocked.length ? '<div class="muted" style="margin-top:8px">Blocked: ' + blocked.length + '</div>' : '';
    wrap.innerHTML = pills + blockedCount;
  } catch(e) {
    document.getElementById("tradable").innerHTML = '<div class="muted">Error loading eligibility</div>';
  }
}

async function loadPrices() {
  try {
    const cfg = await loadConfig();
    const syms = cfg.SYMBOLS || cfg.symbols || [];
    const container = document.getElementById("prices");
    container.innerHTML = '';
    for (const s of syms) {
      try {
        const r = await fetch(base + "/price/" + encodeURIComponent(s));
        const j = await r.json();
        const p = j.price;
        const div = document.createElement('div');
        div.className = 'row';
        div.innerHTML = '<span>'+ s +'</span><span class="mono">'+ (p !== undefined ? fmt.format(p) : '—') +'</span>';
        container.appendChild(div);
      } catch(e) {
        const div = document.createElement('div');
        div.className = 'row';
        div.innerHTML = '<span>'+ s +'</span><span class="mono">—</span>';
        container.appendChild(div);
      }
    }
  } catch(e) {
    document.getElementById("prices").innerHTML = '<div class="muted">Error loading prices</div>';
  }
}

loadPNL();
loadEligible();
loadPrices();

setInterval(loadPNL, 15000);
setInterval(loadEligible, 30000);
setInterval(loadPrices, 45000);
</script>
</body>
</html>
"""

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(_):
    return HTMLResponse(content=DASHBOARD_HTML, status_code=200)

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    print(f"Launching Uvicorn on 0.0.0.0:{port} (policy dir={POLICY_CFG_DIR})")
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
