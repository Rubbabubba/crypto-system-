
# app.py — Crypto System API (version 1.12.3)
# Drop-in, production-ready FastAPI app with journal + P&L + price routes.
# - /price/{base}/{quote} and /prices (batch)
# - /journal/sync, /journal/backfill, /journal/enrich, /journal/enrich/deep
#   (uses broker_kraken.trade_details(txids) when available)
# - /journal (list), /journal/sanity
# - /fills (from journal rows with fill fields), /pnl/summary
# - /policy snapshot and /scheduler/run (safe stubs)
# - No inline dashboard HTML — serve dashboard.html yourself (e.g., /static)
#
# Designed to run on Render/Heroku with PORT env var or default 10000.
#
# Python 3.10+

from __future__ import annotations
import os
import json
import time
import math
import logging
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# ------------------------------------------------------------
# App / Logging
# ------------------------------------------------------------
VERSION = "1.12.3"
PORT = int(os.environ.get("PORT", "10000"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("crypto-system-api")
log.info("Starting crypto-system-api v%s on 0.0.0.0:%d", VERSION, PORT)

app = FastAPI(title="crypto-system-api", version=VERSION)

# ------------------------------------------------------------
# Journal store (file-based, robust to corruption)
# ------------------------------------------------------------
DATA_DIR = os.environ.get("DATA_DIR", "/mnt/data")
os.makedirs(DATA_DIR, exist_ok=True)
JOURNAL_PATH = os.path.join(DATA_DIR, "journal.json")

def _load_json_safe(path: str) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        log.warning("Failed to read %s (%s). Using empty list.", path, e)
        return []

def _atomic_write(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)

class JournalStore:
    def __init__(self, path: str):
        self.path = path
        self._cache: List[Dict[str, Any]] = _load_json_safe(path)

    def rows(self) -> List[Dict[str, Any]]:
        return list(self._cache)

    def save(self) -> None:
        _atomic_write(self.path, self._cache)

    def upsert_rows(self, new_rows: List[Dict[str, Any]]) -> int:
        """Idempotent upsert by txid if present; else append if unique (ts+symbol+side)."""
        if not new_rows:
            return 0
        idx: Dict[str, int] = {}
        for i, r in enumerate(self._cache):
            key = r.get("txid") or f"{r.get('ts')}|{r.get('symbol')}|{r.get('side')}"
            if key:
                idx[key] = i
        updated = 0
        for r in new_rows:
            key = r.get("txid") or f"{r.get('ts')}|{r.get('symbol')}|{r.get('side')}"
            if key in idx:
                self._cache[idx[key]].update(r)
                updated += 1
            else:
                self._cache.append(r)
                updated += 1
        self.save()
        return updated

store = JournalStore(JOURNAL_PATH)

# ------------------------------------------------------------
# Kraken helpers (public prices + optional private trade_details)
# ------------------------------------------------------------
KRAKEN_ALIAS = {
    "BTC": "XBT",  # Kraken uses XBTUSD not BTCUSD
}
def _kpair(symbol: str) -> Optional[str]:
    # Accept forms: "BTC/USD" or "BTCUSD"
    s = symbol.replace("/", "").upper()
    if len(s) < 6:
        return None
    base = s[:-3]
    quote = s[-3:]
    base = KRAKEN_ALIAS.get(base, base)
    return base + quote

_price_cache: Dict[str, Tuple[float, float]] = {}  # symbol -> (price, ts)

def kraken_price(symbol: str) -> Optional[float]:
    now = time.time()
    # 10s cache
    if symbol in _price_cache:
        px, ts = _price_cache[symbol]
        if now - ts < 10:
            return px
    pair = _kpair(symbol)
    if not pair:
        return None
    try:
        url = f"https://api.kraken.com/0/public/Ticker?pair={pair}"
        r = requests.get(url, timeout=10)
        j = r.json()
        result = j.get("result") or {}
        # Kraken sometimes remaps pair names; take first key
        if result:
            k = next(iter(result))
            data = result[k]
            last = data.get("c", [None])[0]
            if last:
                px = float(last)
                _price_cache[symbol] = (px, now)
                return px
    except Exception as e:
        log.warning("kraken_price error for %s: %s", symbol, e)
    return None

def _try_import_trade_details():
    try:
        import broker_kraken  # type: ignore
        if hasattr(broker_kraken, "trade_details"):
            return broker_kraken.trade_details
    except Exception as e:
        log.info("broker_kraken.trade_details not available: %s", e)
    return None

trade_details_fn = _try_import_trade_details()

# ------------------------------------------------------------
# Models
# ------------------------------------------------------------
class SyncBody(BaseModel):
    since_hours: int = 720
    limit: int = 2000

class EnrichBody(BaseModel):
    limit: int = 2000

class SchedulerBody(BaseModel):
    dry: bool = True
    tf: str = "5Min"
    strats: str = "c1,c2,c3,c4,c5,c6"
    symbols: str = "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD"
    limit: int = 300
    notional: float = 25.0

# ------------------------------------------------------------
# Utility
# ------------------------------------------------------------
def _list_routes() -> List[str]:
    return sorted({f"{list(r.methods)[0]} {r.path}" for r in app.routes if getattr(r, 'methods', None)})

def _symbolize(base: str, quote: str) -> str:
    return f"{base.upper()}/{quote.upper()}"

def _compute_pnl_summary(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    # Very simple realized P&L: sum(signed cost) and track by symbol/strategy
    realized = 0.0
    by_symbol: Dict[str, Dict[str, float]] = {}
    for r in rows:
        cost = r.get("cost")
        side = (r.get("side") or "").lower()
        sym = r.get("symbol") or "UNK"
        strat = r.get("strategy") or "UNK"
        if isinstance(cost, (int, float)) and cost is not None:
            signed = cost if side == "sell" else -cost
            realized += signed
            d = by_symbol.setdefault(sym, {"realized": 0.0})
            d["realized"] += signed
    return {"realized": round(realized, 2), "by_symbol": by_symbol}

def _light_enrich_row(r: Dict[str, Any]) -> None:
    # Parse 'descr' like "buy 0.01 BTCUSD @ market" to fill price/vol when missing (best effort)
    descr = r.get("descr") or ""
    if not descr or (r.get("price") and r.get("vol")):
        return
    try:
        parts = descr.split()
        if len(parts) >= 3:
            # e.g., buy 1.27877 AVAXUSD @ market
            side = parts[0].lower()
            asset = parts[2]  # AVAXUSD
            # price may not be in descr; leave None
            r.setdefault("side", side)
            if asset.endswith("USD"):
                base = asset[:-3]
                r.setdefault("symbol", f"{base}/USD")
    except Exception:
        pass

def _enrich_with_trade_details(rows: List[Dict[str, Any]]) -> int:
    if not trade_details_fn:
        return 0
    txids = [r.get("txid") for r in rows if r.get("txid")]
    if not txids:
        return 0
    try:
        details = trade_details_fn(txids) or {}
    except Exception as e:
        log.warning("trade_details call failed: %s", e)
        return 0
    updated = 0
    for r in rows:
        txid = r.get("txid")
        if not txid or txid not in details:
            continue
        d = details[txid] or {}
        changed = False
        if "descr" in d and d["descr"] and r.get("descr") != d["descr"]:
            r["descr"] = d["descr"]; changed = True
        if "userref" in d and d.get("userref") is not None and r.get("userref") != d["userref"]:
            r["userref"] = d["userref"]; changed = True
        if changed:
            updated += 1
    if updated:
        store.save()
    return updated

# ------------------------------------------------------------
# Routes
# ------------------------------------------------------------
@app.get("/", summary="API root; shows version and routes")
def root():
    return {
        "ok": True,
        "version": VERSION,
        "routes": _list_routes(),
    }

@app.get("/health", summary="Health check")
def health():
    return {"ok": True, "version": VERSION}

@app.get("/routes", summary="List routes")
def routes():
    return {"ok": True, "routes": _list_routes()}

# ---------- Prices ----------
@app.get("/price/{base}/{quote}", summary="Get latest price for BASE/QUOTE (Kraken public API)")
def get_price(base: str, quote: str):
    symbol = _symbolize(base, quote)
    px = kraken_price(symbol)
    if px is None:
        raise HTTPException(404, detail=f"Price not found for {symbol}")
    return {"ok": True, "symbol": symbol, "price": px}

@app.post("/prices", summary="Batch prices")
def get_prices(payload: Dict[str, List[str]] = Body(...)):
    symbols = payload.get("symbols") or []
    out = {}
    for s in symbols:
        px = kraken_price(s)
        if px is not None:
            out[s] = px
    return {"ok": True, "prices": out}

# ---------- Journal ----------
@app.post("/journal/sync", summary="Sync recent trades into journal (stub; idempotent)")
def journal_sync(body: SyncBody):
    # In a real system, you'd fetch from broker here.
    # This stub just returns ok and leaves existing journal untouched.
    return {"ok": True, "updated": 0, "count": len(store.rows())}

@app.post("/journal/backfill", summary="Backfill older trades (stub; idempotent)")
def journal_backfill(body: SyncBody):
    return {"ok": True, "updated": 0, "count": len(store.rows())}

@app.post("/journal/enrich", summary="Light enrich journal rows (descr parse + trade_details)")
def journal_enrich(body: EnrichBody):
    rows = store.rows()
    rows_sorted = sorted(rows, key=lambda r: r.get("ts") or 0, reverse=True)[:body.limit]
    # light enrich
    for r in rows_sorted:
        _light_enrich_row(r)
    # optional broker trade_details
    updated = _enrich_with_trade_details(rows_sorted)
    # save
    store.save()
    return {"ok": True, "updated": updated, "checked": len(rows_sorted)}

@app.post("/journal/enrich/deep", summary="Deep enrich (light + trade_details + fill prices)")
def journal_enrich_deep(body: EnrichBody):
    rows = store.rows()
    rows_sorted = sorted(rows, key=lambda r: r.get("ts") or 0, reverse=True)[:body.limit]
    # light enrich first
    for r in rows_sorted:
        _light_enrich_row(r)
    # trade_details pass
    updated = _enrich_with_trade_details(rows_sorted)
    # attach current prices for rows missing price (read-only hint mkt_price)
    for r in rows_sorted:
        if not r.get("price") and r.get("symbol"):
            px = kraken_price(r["symbol"])
            if px:
                r["mkt_price"] = px
                updated += 1
    store.save()
    return {"ok": True, "updated": updated, "checked": len(rows_sorted)}

@app.get("/journal", summary="List journal rows")
def get_journal(limit: int = Query(200, ge=1, le=10000)):
    rows = sorted(store.rows(), key=lambda r: r.get("ts") or 0, reverse=True)[:limit]
    return {"ok": True, "rows": rows, "count": len(rows)}

@app.post("/journal/sanity", summary="Sanity check a sample of journal rows")
def journal_sanity(body: EnrichBody = Body(default=EnrichBody())):
    rows = sorted(store.rows(), key=lambda r: r.get("ts") or 0, reverse=True)[:body.limit]
    problems = []
    for r in rows:
        if not r.get("symbol") or not r.get("side"):
            problems.append({"txid": r.get("txid"), "issue": "missing symbol/side"})
    return {"ok": True, "problems": problems, "checked": len(rows)}

@app.get("/fills", summary="Returns journal rows that look like filled trades")
def fills():
    rows = [r for r in store.rows() if r.get("side") in ("buy", "sell")]
    return {"ok": True, "rows": rows, "count": len(rows)}

# ---------- P&L / KPIs ----------
@app.get("/pnl/summary", summary="Realized P&L summary (simple)")
def pnl_summary():
    rows = store.rows()
    return {"ok": True, "date": time.strftime("%Y-%m-%d"), **_compute_pnl_summary(rows)}

# ---------- Policy / Scheduler ----------
POLICY_SNAPSHOT = {
    "windows": {
        "c1": {"days": ["Mon","Tue","Wed","Thu","Fri"], "hours": [13,14,15,16,17,18,19,20]},
        "c2": {"days": ["Mon","Tue","Wed","Thu","Fri"], "hours": [6,7,8,19,20,21,22,23]},
        "c3": {"days": ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], "hours": [7,8,9,18,19,20,21,22]},
        "c4": {"days": ["Mon","Tue","Wed","Thu","Fri"], "hours": [10,11,12,13,14,20,21]},
        "c5": {"days": ["Mon","Tue","Wed","Thu","Fri"], "hours": [0,1,2,18,19,20]},
        "c6": {"days": ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], "hours": [1,2,3,20,21,22]},
    },
    "whitelist": {
        "c1": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c2": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c3": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c4": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c5": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c6": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
    },
    "risk": {
        "fee_rate_pct": 0.26,
        "edge_multiple_vs_fee": 3.0,
        "atr_floor_pct": {"tier1": 0.6, "tier2": 0.9, "tier3": 1.2},
        "tiers": {"tier1": ["BTCUSD","ETHUSD","SOLUSD"], "tier2": ["XRPUSD","ADAUSD","DOGEUSD","LTCUSD","BCHUSD","AVAXUSD","LINKUSD"], "tier3": []},
        "symbol_mutex_minutes": 60,
        "cooldown_minutes_after_exit_for_mr": 30,
        "mr_strategies": ["c1"],
        "avoid_pairs": [],
    },
}

@app.get("/policy", summary="Policy snapshot")
def policy():
    return {"ok": True, "policy": POLICY_SNAPSHOT}

@app.post("/scheduler/run", summary="Scheduler dry-run stub")
def scheduler_run(body: SchedulerBody):
    equity = float(os.environ.get("EQUITY_USD", "22.20"))
    risk_pct = 0.05
    notional = max(min(body.notional, 250.0), 25.0)
    log.info("Sizing: equity=%.2f USD, risk_pct=%.4f -> notional=%.2f (25.00..250.00)", equity, risk_pct, notional)
    log.info("Scheduler pass: strats=%s tf=%s limit=%d notional=%.1f dry=%s symbols=%s",
             body.strats, body.tf, body.limit, notional, body.dry, body.symbols)
    return {"ok": True, "scheduled": 0, "dry": body.dry, "notional": notional}

# ------------------------------------------------------------
# Entrypoint (Render runs `python app.py`)
# ------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
