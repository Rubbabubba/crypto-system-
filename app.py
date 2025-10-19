
# app.py (policy-aware dashboard fix)
# NOTE: This version focuses on fixing /policy/eligible and the dashboard's "What can trade now"
# by safely resolving SYMBOLS/STRATEGIES from policy_config and importing guard from policy.guard.
from __future__ import annotations

import os
import json
import datetime as dt
from pathlib import Path
from typing import List, Dict, Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse

# ---- Policy / guard imports ----
try:
    from policy.guard import load_policy, guard_allows  # repo layout: policy/guard.py
except Exception as e:
    # Keep importing errors visible but allow server to start
    load_policy = None
    guard_allows = None

app = FastAPI()

# ---- Resolve config dir (your repo has policy_config/) ----
POLICY_CFG_DIR = Path(os.getenv("POLICY_CFG_DIR", Path(__file__).parent / "policy_config")).resolve()

# ---- Helper: read whitelist + params to infer symbols/strategies ----
def _load_json(p: Path, default: Any) -> Any:
    try:
        return json.loads(p.read_text())
    except Exception:
        return default

def infer_symbols_strategies() -> tuple[list[str], list[str]]:
    wl = _load_json(POLICY_CFG_DIR / "whitelist.json", {})
    params = _load_json(POLICY_CFG_DIR / "params.json", {})
    # strategies from whitelist keys or params keys
    strats = set()
    if isinstance(wl, dict):
        for k, v in wl.items():
            if isinstance(v, dict):
                strats.update(v.get("strategies", []))
            # some repos use {strategy: [symbols]}
            if isinstance(v, list) and k.startswith("c"):
                strats.add(k)
    if isinstance(params, dict):
        strats.update(list(params.keys()))
    if not strats:
        strats.update(["c1","c2","c3","c4","c5","c6"])

    # symbols from whitelist values
    syms = set()
    if isinstance(wl, dict):
        # support both shapes:
        #  1) {"BTC/USD": {"strategies":["c1","c2"]}, ...}
        #  2) {"c1": ["BTC/USD","ETH/USD"], ...}
        for k, v in wl.items():
            if isinstance(v, dict) and "strategies" in v:
                syms.add(k)
            elif isinstance(v, list) and k.startswith("c"):
                for s in v:
                    syms.add(s)
    if not syms:
        # very safe fallback
        syms.update(["BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD","AVAX/USD","LINK/USD","BCH/USD","LTC/USD"])
    return sorted(syms), sorted(strats)

SYMBOLS, STRATEGIES = infer_symbols_strategies()

# cache policy object
_POLICY = None
def _get_policy():
    global _POLICY
    if _POLICY is None and load_policy is not None:
        _POLICY = load_policy(str(POLICY_CFG_DIR))
    return _POLICY

# ---- NEW: policy eligibility endpoint used by the dashboard ----
@app.get("/policy/eligible")
def policy_eligible() -> Dict[str, Any]:
    now = dt.datetime.utcnow()
    policy = _get_policy()
    out: List[Dict[str, Any]] = []
    # If guard functions missing, return a clear message instead of 500
    if policy is None or guard_allows is None:
        return {
            "ok": False,
            "error": "policy.guard not available",
            "policy_cfg_dir": str(POLICY_CFG_DIR),
            "symbols": SYMBOLS,
            "strategies": STRATEGIES
        }
    for strat in STRATEGIES:
        for sym in SYMBOLS:
            allowed, reason = guard_allows(policy, strat, sym, now)
            out.append({
                "strategy": strat,
                "symbol": sym,
                "allowed": bool(allowed),
                "reason": reason,
            })
    return {"ok": True, "as_of": now.isoformat() + "Z", "results": out, "symbols": SYMBOLS, "strategies": STRATEGIES}

# ---- Optional: minimal dashboard section renderer (keeps your other routes intact) ----
@app.get("/policy/eligible/table", response_class=HTMLResponse)
def eligible_table():
    data = policy_eligible()
    if not data.get("ok"):
        return HTMLResponse(f"<pre>{json.dumps(data, indent=2)}</pre>")
    rows = []
    for r in data["results"]:
        badge = "✅" if r["allowed"] else "⛔"
        rows.append(f"<tr><td>{r['strategy']}</td><td>{r['symbol']}</td><td>{badge}</td><td>{r['reason']}</td></tr>")
    html = f"""
    <div style="font-family: ui-sans-serif, system-ui; color: #e5e7eb">
      <h3>What can trade now</h3>
      <table border="1" cellpadding="6" cellspacing="0">
        <thead><tr><th>Strategy</th><th>Symbol</th><th>Status</th><th>Reason</th></tr></thead>
        <tbody>
          {''.join(rows) if rows else '<tr><td colspan="4">No entries</td></tr>'}
        </tbody>
      </table>
      <p style="opacity:.7">as of {data['as_of']}</p>
    </div>
    """
    return HTMLResponse(html)

# ---- Keep your existing run style ----
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    print(f"Launching Uvicorn on 0.0.0.0:{port} (policy_cfg_dir={POLICY_CFG_DIR})")
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
