from pathlib import Path
import json
import os
from typing import Dict, List, Optional, Tuple
from datetime import datetime, time
import zoneinfo

import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

app = FastAPI(title="Crypto System (Policy Dashboard)")  # keep name 'app' for uvicorn app:app

# --- Configuration of policy directory ---
POLICY_CFG_DIR = os.getenv("POLICY_CFG_DIR")
if not POLICY_CFG_DIR:
    POLICY_CFG_DIR = str((Path(__file__).parent / "policy_config").resolve())
POLICY_DIR_PATH = Path(POLICY_CFG_DIR)

# --- Helpers to call our own API base ---
def base_url_from_request(request: Request) -> str:
    env_url = os.getenv("PUBLIC_BASE_URL")
    if env_url:
        return env_url.rstrip("/")
    return str(request.base_url).rstrip("/")

def get_json(url: str, timeout: float = 10.0) -> dict:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

# --- Policy loading ---
def _safe_load_json(path: Path) -> dict:
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        pass
    return {}

def load_policy(dir_path: Path) -> Tuple[dict, dict, dict]:
    whitelist = _safe_load_json(dir_path / "whitelist.json")
    windows = _safe_load_json(dir_path / "windows.json")
    blacklist = _safe_load_json(dir_path / "blacklist.json")
    return whitelist or {}, windows or {}, blacklist or {}

# --- Guard checks ---
DOW_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

def _in_whitelist(whitelist: dict, strategy: str, symbol: str) -> bool:
    if not whitelist:
        return True
    if strategy in whitelist and isinstance(whitelist[strategy], list):
        return symbol in whitelist[strategy]
    syms = set(whitelist.get("symbols", []) if isinstance(whitelist.get("symbols"), list) else [])
    strats = set(whitelist.get("strategies", []) if isinstance(whitelist.get("strategies"), list) else [])
    if syms and strats:
        return symbol in syms and strategy in strats
    if syms and not strats:
        return symbol in syms
    if strats and not syms:
        return strategy in strats
    return True

def _in_blacklist(blacklist: dict, strategy: str, symbol: str) -> bool:
    if not blacklist:
        return False
    if strategy in blacklist and isinstance(blacklist[strategy], list):
        return symbol in blacklist[strategy]
    syms = set(blacklist.get("symbols", []) if isinstance(blacklist.get("symbols"), list) else [])
    strats = set(blacklist.get("strategies", []) if isinstance(blacklist.get("strategies"), list) else [])
    if symbol in syms:
        return True
    if strategy in strats:
        return True
    return False

def _within_window(windows: dict, strategy: str, symbol: str, now_utc: datetime) -> bool:
    if not windows:
        return True
    entry = None
    if strategy in windows and isinstance(windows[strategy], dict):
        entry = windows[strategy]
    elif "default" in windows and isinstance(windows["default"], dict):
        entry = windows["default"]
    else:
        return True
    start_s = entry.get("start", "00:00")
    end_s = entry.get("end", "23:59")
    tz_name = entry.get("tz", "UTC")
    days = entry.get("days", DOW_NAMES)
    try:
        tz = zoneinfo.ZoneInfo(tz_name)
    except Exception:
        tz = zoneinfo.ZoneInfo("UTC")
    local_now = now_utc.astimezone(tz)
    dow = DOW_NAMES[local_now.weekday()]
    if days and dow not in days:
        return False
    try:
        hh, mm = map(int, start_s.split(":"))
        s = time(hh, mm)
        hh2, mm2 = map(int, end_s.split(":"))
        e = time(hh2, mm2)
    except Exception:
        return True
    if s <= e:
        return s <= local_now.time() <= e
    else:
        return local_now.time() >= s or local_now.time() <= e

def guard_allows(whitelist: dict, windows: dict, blacklist: dict, strategy: str, symbol: str, now_utc: Optional[datetime] = None) -> bool:
    if now_utc is None:
        now_utc = datetime.utcnow().replace(tzinfo=zoneinfo.ZoneInfo("UTC"))
    if _in_blacklist(blacklist, strategy, symbol):
        return False
    if not _in_whitelist(whitelist, strategy, symbol):
        return False
    if not _within_window(windows, strategy, symbol, now_utc):
        return False
    return True

# ---- Routes ----

@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return """<html><head><title>Crypto System</title></head>
<body style="font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial;">
<h2>Crypto System service</h2>
<ul>
  <li><a href="/dashboard">Dashboard</a></li>
  <li><a href="/policy/eligible">Policy Eligible (JSON)</a></li>
</ul>
</body></html>"""

@app.get("/policy/eligible")
def policy_eligible(request: Request):
    base = base_url_from_request(request)
    try:
        cfg = get_json(f"{base}/config")
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Failed to fetch /config: {e}"}, status_code=500)

    symbols = cfg.get("SYMBOLS") or cfg.get("symbols") or []
    strategies = cfg.get("STRATEGIES") or cfg.get("strategies") or []

    wl, win, bl = load_policy(POLICY_DIR_PATH)
    now_utc = datetime.utcnow().replace(tzinfo=zoneinfo.ZoneInfo("UTC"))

    eligible = []
    for sym in symbols:
        sym_norm = sym.replace("/", "")
        for strat in strategies:
            if guard_allows(wl, win, bl, strat, sym_norm, now_utc):
                eligible.append({"strategy": strat, "symbol": sym})

    return {"ok": True, "count": len(eligible), "eligible": eligible, "time": now_utc.isoformat()}

def _format_money(n: Optional[float]) -> str:
    if n is None:
        return "—"
    try:
        return f"${n:,.2f}"
    except Exception:
        return str(n)

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    base = base_url_from_request(request)

    cfg = {}
    try:
        cfg = get_json(f"{base}/config")
    except Exception:
        pass
    symbols = cfg.get("SYMBOLS") or []
    strategies = cfg.get("STRATEGIES") or []

    pnl = {}
    try:
        pnl = get_json(f"{base}/pnl/summary")
    except Exception:
        pnl = {"ok": False}

    elig = {}
    try:
        elig = get_json(f"{base}/policy/eligible")
    except Exception:
        elig = {"ok": False}

    prices_rows = []
    for s in symbols[:12]:
        try:
            pr = get_json(f"{base}/price/{s}")
            price = pr.get("price")
            prices_rows.append(f"<tr><td>{s}</td><td style='text-align:right'>{price:,}</td></tr>")
        except Exception:
            prices_rows.append(f"<tr><td>{s}</td><td style='text-align:right'>—</td></tr>")

    total = (pnl.get("total") or {}) if isinstance(pnl, dict) else {}
    realized = _format_money(total.get("realized"))
    unrealized = _format_money(total.get("unrealized"))
    fees = _format_money(total.get("fees"))
    equity = _format_money(total.get("equity"))

    elig_rows = ""
    if elig.get("ok") and isinstance(elig.get("eligible"), list) and elig["eligible"]:
        for row in elig["eligible"][:50]:
            elig_rows += f"<tr><td>{row.get('strategy')}</td><td>{row.get('symbol')}</td></tr>"
    else:
        elig_rows = "<tr><td colspan='2' style='opacity:.7'>None right now</td></tr>"

    html = f"""
<!doctype html>
<html>
<head>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Dashboard</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 16px; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 12px; }}
    .card {{ border: 1px solid #e4e4e7; border-radius: 10px; padding: 12px; box-shadow: 0 1px 2px rgba(0,0,0,.04); }}
    h2 {{ margin: 0 0 8px; font-size: 18px; }}
    table {{ width: 100%; border-collapse: collapse; }}
    td, th {{ padding: 6px; border-bottom: 1px solid #f0f0f2; text-align: left; }}
    .muted {{ color: #666; font-size: 12px; }}
  </style>
</head>
<body>
  <h1 style="font-size:20px;margin:4px 0 12px;">Crypto System</h1>
  <div class="grid">
    <div class="card">
      <h2>P&L</h2>
      <table>
        <tr><td>Realized</td><td style="text-align:right">{realized}</td></tr>
        <tr><td>Unrealized</td><td style="text-align:right">{unrealized}</td></tr>
        <tr><td>Fees</td><td style="text-align:right">{fees}</td></tr>
        <tr><td>Equity</td><td style="text-align:right">{equity}</td></tr>
      </table>
      <div class="muted">From <code>/pnl/summary</code></div>
    </div>

    <div class="card">
      <h2>Tradable Now</h2>
      <table>
        <tr><th>Strategy</th><th>Symbol</th></tr>
        {elig_rows}
      </table>
      <div class="muted">From <code>/policy/eligible</code> using rules in <code>{POLICY_DIR_PATH}</code></div>
    </div>

    <div class="card">
      <h2>Prices</h2>
      <table>
        <tr><th>Symbol</th><th style="text-align:right">Last</th></tr>
        {"".join(prices_rows)}
      </table>
      <div class="muted">Live pulls from <code>/price/&lt;symbol&gt;</code></div>
    </div>
  </div>
</body>
</html>
"""
    return HTMLResponse(content=html)

@app.get("/healthz")
def healthz():
    return PlainTextResponse("ok")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    print(f"Launching Uvicorn on 0.0.0.0:{port} (policy dir={POLICY_DIR_PATH})")
    uvicorn.run(app, host="0.0.0.0", port=port, reload=False)
