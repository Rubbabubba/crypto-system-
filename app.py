#!/usr/bin/env python3
from __future__ import annotations

import os, sys, json, logging, threading, time, re
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict
from fastapi import FastAPI, HTTPException, Request, Body, Query
from fastapi.responses import HTMLResponse, RedirectResponse

logging.basicConfig(level=os.getenv("LOG_LEVEL","INFO"), format="%(asctime)s | %(levelname)s | %(message)s", stream=sys.stdout)
log = logging.getLogger("app")
APP_VERSION = os.getenv("APP_VERSION","prod-full-adv-1.0")

# Broker
BROKER = os.getenv("BROKER","kraken").lower()
USING_KRAKEN = BROKER=="kraken" or (os.getenv("KRAKEN_KEY") and os.getenv("KRAKEN_SECRET"))
if USING_KRAKEN:
    import broker_kraken as br
else:
    import broker as br  # fallback, if you have one

# Universe
try:
    from universe import load_universe_from_env
    _CURRENT_SYMBOLS = load_universe_from_env()
except Exception:
    _CURRENT_SYMBOLS = ["BTCUSD","ETHUSD","SOLUSD","LTCUSD"]

# Strategies (only registered for health/config; scanning unchanged)
ACTIVE_STRATEGIES = ["c1","c2","c3","c4","c5","c6"]
DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME","5Min")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT","300"))
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", os.getenv("ORDER_NOTIONAL","25")))

def utc_now()->str: return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

app = FastAPI(title="Crypto System", version=APP_VERSION)

# ---------- UI ----------
DASHBOARD_PATH = Path(os.getenv("DASHBOARD_FILE","dashboard.html"))
@app.get("/", include_in_schema=False)
def root(): return RedirectResponse(url="/dashboard.html")
@app.get("/dashboard", include_in_schema=False)
def dash_alias(): return RedirectResponse(url="/dashboard.html")
@app.get("/dashboard.html", response_class=HTMLResponse)
def serve_dashboard_html():
    if not DASHBOARD_PATH.exists():
        return HTMLResponse("<pre>dashboard.html not found</pre>", status_code=200)
    return HTMLResponse(DASHBOARD_PATH.read_text(encoding="utf-8"))

# ---------- Health & Config ----------
@app.get("/health")
def health():
    return {"ok": True, "version": APP_VERSION, "broker": ("kraken" if USING_KRAKEN else "other"),
            "time": utc_now(), "symbols": _CURRENT_SYMBOLS, "strategies": ACTIVE_STRATEGIES}

@app.get("/config")
def config():
    return {"DEFAULT_TIMEFRAME": DEFAULT_TIMEFRAME, "DEFAULT_LIMIT": DEFAULT_LIMIT, "DEFAULT_NOTIONAL": DEFAULT_NOTIONAL,
            "SYMBOLS": _CURRENT_SYMBOLS, "STRATEGIES": ACTIVE_STRATEGIES}

# ---------- Market basics ----------
@app.get("/price/{symbol}")
def price(symbol: str):
    p = br.last_price(symbol.upper())
    return {"ok": True, "symbol": symbol.upper(), "price": float(p)}

@app.get("/price/{base}/{quote}")
def price_pair(base: str, quote: str):
    sym = f"{base.upper()}{quote.upper()}"
    p = br.last_price(sym)
    return {"ok": True, "symbol": sym, "price": float(p)}

@app.get("/fills")
def fills():
    return {"ok": True, **br.trades_history(200)}

# ---------- Journal & PnL (FIFO, journal-based) ----------
_JOURNAL_LOCK = threading.Lock()
_JOURNAL_PATH = os.getenv("JOURNAL_PATH","./journal_v2.jsonl")
_JOURNAL: List[Dict[str,Any]] = []

STRAT_TAG_RE = re.compile(r"(?:strategy\s*=\s*|strat\s*[:=]\s*|\[strategy\s*=\s*)([a-z0-9_]+)", re.I)
USERREF_MAP_FILE = Path(os.getenv("USERREF_MAP_FILE", "policy_config/userref_map.json"))

def _load_userref_map() -> dict:
    try: return json.loads(USERREF_MAP_FILE.read_text())
    except Exception: return {}

def _infer_strategy(descr: str|None, userref: int|None, default="import")->str:
    s = descr or ""
    m = STRAT_TAG_RE.search(s)
    if m: return m.group(1).lower()
    if userref is not None:
        m = _load_userref_map().get(str(int(userref)))
        if m: return m.lower()
    return default

def _journal_load():
    global _JOURNAL
    rows=[]
    try:
        with open(_JOURNAL_PATH,"r",encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
    except FileNotFoundError:
        pass
    with _JOURNAL_LOCK: _JOURNAL=rows

@app.on_event("startup")
async def _on_start(): _journal_load()

def _journal_append_many(rows: List[dict]):
    if not rows: return 0
    with _JOURNAL_LOCK:
        _JOURNAL.extend(rows)
        with open(_JOURNAL_PATH,"a",encoding="utf-8") as f:
            for r in rows: f.write(json.dumps(r, separators=(",",":"))+"\n")
    return len(rows)

def _sync_journal_with_fills(n=500):
    fills = br.trades_history(n)
    if not fills.get("ok"): return {"ok": False, "error": fills.get("error","unknown")}
    by_txid = {t.get("txid"): t for t in fills.get("trades", []) if t.get("txid")}
    updated = 0
    with _JOURNAL_LOCK:
        for row in _JOURNAL:
            tx = row.get("txid")
            if not tx or tx not in by_txid: continue
            t = by_txid[tx]
            row["price"] = float(t.get("price") or 0.0)
            row["vol"] = float(t.get("vol") or 0.0)
            row["fee"] = float(t.get("fee") or 0.0)
            row["cost"] = float(t.get("cost") or 0.0)
            row["filled_ts"] = float(t.get("time") or 0.0)
            row["userref"] = t.get("userref", row.get("userref"))
            row["descr"] = t.get("descr") or row.get("descr")
            if (row.get("strategy") or "import") == "import":
                row["strategy"] = _infer_strategy(row.get("descr"), row.get("userref"))
            updated += 1
    if updated:
        with open(_JOURNAL_PATH,"w",encoding="utf-8") as f:
            for r in _JOURNAL: f.write(json.dumps(r, separators=(",",":"))+"\n")
    return {"ok": True, "updated": updated, "count": len(_JOURNAL)}

def _journal_backfill(n=5000):
    fills = br.trades_history(n)
    if not fills.get("ok"): return {"ok": False, "error": fills.get("error","unknown")}
    with _JOURNAL_LOCK: existing = {str(r.get("txid")) for r in _JOURNAL if r.get("txid")}
    try:
        from symbol_map import from_kraken as _from_k
    except Exception:
        _from_k = lambda s: s
    rows = []
    for t in fills.get("trades") or []:
        txid = str(t.get("txid") or "")
        if not txid or txid in existing: continue
        pair_raw = str(t.get("pair") or "")
        sym_ui = (_from_k(pair_raw) or pair_raw).upper()
        side = str(t.get("type") or t.get("side") or "").lower()
        price = float(t.get("price") or 0.0)
        vol = float(t.get("vol") or 0.0)
        fee = float(t.get("fee") or 0.0)
        cost = float(t.get("cost") or 0.0)
        ts_fill = float(t.get("time") or 0.0)
        userref = t.get("userref")
        descr = t.get("descr") or ""
        strategy = _infer_strategy(descr, userref, default="import")
        rows.append({"ts": int(ts_fill) or int(time.time()), "filled_ts": float(ts_fill) if ts_fill else None,
                     "symbol": sym_ui, "side": side, "price": price, "vol": vol, "fee": fee, "cost": cost,
                     "strategy": strategy, "txid": txid, "descr": descr, "userref": userref})
    added = _journal_append_many(rows)
    return {"ok": True, "added": added, "count": len(_JOURNAL)}

@app.post("/journal/sync")
def journal_sync():
    return _sync_journal_with_fills(500)
@app.get("/journal/sync")
def journal_sync_get():
    return _sync_journal_with_fills(500)
@app.post("/journal/backfill")
def journal_backfill_post():
    return _journal_backfill(5000)
@app.get("/journal/backfill")
def journal_backfill_get():
    return _journal_backfill(5000)

@app.post("/journal/enrich")
def journal_enrich():
    fixed = 0
    with _JOURNAL_LOCK:
        for row in _JOURNAL:
            if (row.get("strategy") or "").lower() != "import":
                continue
            s = _infer_strategy(row.get("descr"), row.get("userref"))
            if s and s != "import":
                row["strategy"] = s; fixed += 1
    if fixed:
        with open(_JOURNAL_PATH,"w",encoding="utf-8") as f:
            for r in _JOURNAL: f.write(json.dumps(r, separators=(",",":"))+"\n")
    return {"ok": True, "updated": fixed, "count": len(_JOURNAL)}

@app.get("/journal")
def journal_list(limit: int = 2000):
    with _JOURNAL_LOCK: rows = list(_JOURNAL[-int(limit):])
    return {"ok": True, "rows": rows, "count": len(_JOURNAL)}

# ---------- P&L summary ----------
from collections import defaultdict, deque
def _prices_for(symbols: List[str])->Dict[str,float]:
    out={}
    for s in symbols:
        try: out[s]=float(br.last_price(s))
        except Exception: out[s]=0.0
    return out

def _pnl_calc(now_prices: Dict[str,float])->Dict[str,Any]:
    with _JOURNAL_LOCK: trades=[r for r in _JOURNAL if r.get("price") and r.get("vol")]
    trades.sort(key=lambda r: r.get("filled_ts") or r.get("ts") or 0)
    lots=defaultdict(lambda: deque()); stats=defaultdict(lambda: {"realized":0.0,"fees":0.0})
    for r in trades:
        strat=r.get("strategy") or "unknown"; sym=(r.get("symbol") or "").upper()
        side=r.get("side"); px=float(r.get("price") or 0.0); vol=float(r.get("vol") or 0.0); fee=float(r.get("fee") or 0.0)
        key=(strat,sym)
        if side=="buy": lots[key].append([vol,px]); stats[key]["fees"]+=fee
        elif side=="sell":
            rem=vol; realized=0.0
            while rem>1e-12 and lots[key]:
                q,cpx=lots[key][0]; take=min(q,rem); realized+=(px-cpx)*take; q-=take; rem-=take
                if q<=1e-12: lots[key].popleft()
                else: lots[key][0][0]=q
            stats[key]["realized"]+=realized; stats[key]["fees"]+=fee
    outS,outY={},{}; T={"realized":0.0,"unrealized":0.0,"fees":0.0,"equity":0.0}
    for key, st in stats.items():
        strat, sym = key; unreal=0.0; mkt=now_prices.get(sym,0.0)
        if mkt>0 and lots.get(key):
            for q,cpx in lots[key]: unreal+=(mkt-cpx)*q
        eq=st["realized"]+unreal-st["fees"]
        outS[strat]=outS.get(strat,{"strategy":strat,"realized":0.0,"unrealized":0.0,"fees":0.0,"equity":0.0})
        outS[strat]["realized"]+=st["realized"]; outS[strat]["unrealized"]+=unreal; outS[strat]["fees"]+=st["fees"]; outS[strat]["equity"]+=eq
        outY[sym]=outY.get(sym,{"symbol":sym,"realized":0.0,"unrealized":0.0,"fees":0.0,"equity":0.0})
        outY[sym]["realized"]+=st["realized"]; outY[sym]["unrealized"]+=unreal; outY[sym]["fees"]+=st["fees"]; outY[sym]["equity"]+=eq
        T["realized"]+=st["realized"]; T["unrealized"]+=unreal; T["fees"]+=st["fees"]; T["equity"]+=eq
    return {"ok":True, "time":utc_now(), "total":T,
            "per_strategy":sorted(outS.values(), key=lambda r:r["equity"], reverse=True),
            "per_symbol":sorted(outY.values(), key=lambda r:r["equity"], reverse=True)}

@app.get("/pnl/summary")
def pnl_summary():
    with _JOURNAL_LOCK: syms = sorted({(r.get("symbol") or "").upper() for r in _JOURNAL if r.get("symbol")}) or list(_CURRENT_SYMBOLS)
    return _pnl_calc(_prices_for(syms))

# ---------- Advisor endpoints ----------
import advisor
@app.get("/advisor/weekly")
def advisor_weekly(end: Optional[str] = Query(None), days: int = Query(7, ge=1, le=31)):
    try:
        if not end: end=(datetime.now(timezone(timedelta(hours=-5))).date().isoformat())
        return {"ok": True, **advisor.analyze_week(end, days)}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/advisor/range")
def advisor_range(start: str = Query(...), end: str = Query(...)):
    try: return {"ok": True, **advisor.analyze_range(start, end)}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.get("/advisor/daily")
def advisor_daily(date: Optional[str] = Query(None)):
    try: return {"ok": True, **advisor.analyze_day(date)}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

@app.post("/advisor/apply")
def advisor_apply(body: Dict[str, Any] = Body(...)):
    try:
        recs = body.get("recommendations") or {}; dry = bool(body.get("dry", True))
        res = advisor.write_policy_updates(recs, dry=dry); return {"ok": True, **res}
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT","10000")), reload=False, access_log=True)