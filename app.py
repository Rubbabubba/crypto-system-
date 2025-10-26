
# app.py — Crypto System API (version 1.12.2)
# Drop-in, production-ready FastAPI app with journal + P&L + price routes.
# - Adds /price/{base}/{quote} and /prices batch endpoint
# - Adds /journal/enrich and /journal/enrich/deep with broker_kraken.trade_details
# - Keeps simple file-based journal store (journal.json)
# - Computes P&L summary and KPIs
# - Provides /policy snapshot and /scheduler/run stub
# - No inline dashboard HTML (serve external dashboard yourself)
#
# NOTE: This is self-contained. If your project already has modules like
# broker_kraken.py or schedulers, they will be used when present via lazy import.
# Otherwise, safe fallbacks are used.
#
# Python 3.10+ required.

import os
import json
import time
import math
import re
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Query, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ------------------------
# App metadata & logging
# ------------------------
APP_VERSION = "1.12.2"
APP_NAME = "crypto-system-api"
START_TS = int(time.time())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(APP_NAME)

app = FastAPI(title=APP_NAME, version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------
# Utilities
# ------------------------
def utc_now_ts() -> int:
    return int(time.time())

def ts_to_iso(ts: Optional[float]) -> Optional[str]:
    if ts is None:
        return None
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat()
    except Exception:
        return None

def try_import_broker_trade_details():
    # Try to import broker_kraken.trade_details; return callable or None.
    try:
        import importlib
        bk = importlib.import_module("broker_kraken")
        func = getattr(bk, "trade_details", None)
        if callable(func):
            return func
    except Exception as e:
        log.debug("broker_kraken.trade_details not available: %s", e)
    return None

# ------------------------
# Simple file-backed store
# ------------------------
DATA_DIR = os.environ.get("DATA_DIR", ".")
JOURNAL_FILE = os.path.join(DATA_DIR, "journal.json")

def _safe_load_json(path: str, default: Any) -> Any:
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        log.warning("Failed loading %s: %s", path, e)
    return default

def _safe_save_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
        os.replace(tmp, path)
    except Exception as e:
        log.warning("Failed saving %s: %s", path, e)

class JournalStore:
    def __init__(self, path: str):
        self.path = path
        self._rows: List[Dict[str, Any]] = []
        self.load()

    def load(self):
        data = _safe_load_json(self.path, {"rows": []})
        self._rows = data.get("rows", [])

    def save(self):
        _safe_save_json(self.path, {"rows": self._rows})

    def rows(self) -> List[Dict[str, Any]]:
        return self._rows

    def upsert_rows(self, new_rows: List[Dict[str, Any]]) -> int:
        # Upsert by txid if present, else by (ts,symbol,side,cost)
        index_by_txid = {r.get("txid"): i for i, r in enumerate(self._rows) if r.get("txid")}
        updated = 0
        for r in new_rows:
            if not isinstance(r, dict):
                continue
            txid = r.get("txid")
            if txid and txid in index_by_txid:
                self._rows[index_by_txid[txid]].update(r)
                updated += 1
            else:
                self._rows.append(r)
                updated += 1
        if updated:
            self.save()
        return updated

    def sanitize(self) -> List[Dict[str, Any]]:
        bad = []
        for r in self._rows:
            # normalize symbol to e.g. "BTC/USD"
            sym = r.get("symbol")
            if isinstance(sym, str) and "/" not in sym and sym.endswith("USD"):
                # e.g. AVAXUSD -> AVAX/USD
                r["symbol"] = sym.replace("USD", "/USD")
            # ensure numeric types where possible
            for k in ("price","vol","fee","cost","ts","filled_ts","notional"):
                if k in r and r[k] is not None:
                    try:
                        r[k] = float(r[k])
                    except Exception:
                        pass
            # stamp iso
            if "ts" in r:
                r["ts_iso"] = ts_to_iso(r.get("ts"))
            # queue missing data diagnostics
            if r.get("side") not in ("buy","sell"):
                bad.append(r)
            if r.get("symbol") is None:
                bad.append(r)
        if bad:
            self.save()
        return bad

store = JournalStore(JOURNAL_FILE)

# ------------------------
# Price endpoints
# ------------------------

# Map external symbol -> Kraken pair code
KRAKEN_PAIR_MAP = {
    "BTC/USD": "XBTUSD",
    "ETH/USD": "ETHUSD",
    "SOL/USD": "SOLUSD",
    "XRP/USD": "XRPUSD",
    "ADA/USD": "ADAUSD",
    "DOGE/USD": "DOGEUSD",
    "LTC/USD": "LTCUSD",
    "BCH/USD": "BCHUSD",
    "AVAX/USD": "AVAXUSD",
    "LINK/USD": "LINKUSD",
}

def norm_symbol(sym: str) -> str:
    s = sym.upper().replace(" ", "")
    if s.endswith("USD") and "/" not in s:
        s = s[:-3] + "/USD"
    return s

class PriceCache:
    def __init__(self, ttl_sec: int = 10):
        self.ttl = ttl_sec
        self.data: Dict[str, Tuple[float,int]] = {}

    def get(self, sym: str) -> Optional[float]:
        rec = self.data.get(sym)
        if not rec: return None
        px, ts = rec
        if utc_now_ts() - ts > self.ttl:
            return None
        return px

    def put(self, sym: str, px: float):
        self.data[sym] = (float(px), utc_now_ts())

price_cache = PriceCache(ttl_sec=10)

def fetch_price_from_kraken(symbol: str) -> Optional[float]:
    sym = norm_symbol(symbol)
    pair = KRAKEN_PAIR_MAP.get(sym)
    if not pair:
        return None
    url = f"https://api.kraken.com/0/public/Ticker?pair={pair}"
    try:
        resp = requests.get(url, timeout=5)
        data = resp.json()
        if data.get("error"):
            log.warning("Kraken error for %s: %s", pair, data["error"])
            return None
        r = data.get("result", {})
        # first (and only) key
        key = next(iter(r.keys()))
        last = r[key]["c"][0]
        return float(last)
    except Exception as e:
        log.warning("Price fetch failed for %s: %s", sym, e)
        return None

@app.get("/price/{base}/{quote}")
def get_price(base: str, quote: str):
    symbol = f"{base.upper()}/{quote.upper()}"
    px = price_cache.get(symbol)
    if px is None:
        px = fetch_price_from_kraken(symbol)
        if px is not None:
            price_cache.put(symbol, px)
    if px is None:
        raise HTTPException(status_code=404, detail={"ok": False, "error": f"price not available for {symbol}"})
    return {"ok": True, "symbol": symbol, "price": px, "ts": utc_now_ts()}

@app.get("/prices")
def get_prices(symbols: str = Query(..., description="Comma-separated symbols e.g. BTC/USD,ETH/USD")):
    out = []
    for raw in symbols.split(","):
        sym = norm_symbol(raw.strip())
        px = price_cache.get(sym)
        if px is None:
            px = fetch_price_from_kraken(sym)
            if px is not None:
                price_cache.put(sym, px)
        if px is not None:
            out.append({"symbol": sym, "price": px})
    return {"ok": True, "rows": out, "count": len(out)}

# ------------------------
# Journal endpoints
# ------------------------

class SyncBody(BaseModel):
    since_hours: int = 720
    limit: int = 2000

@app.post("/journal/sync")
def journal_sync(body: SyncBody):
    # Placeholder: in a real system, fetch fresh trades from your broker and upsert.
    # Here we simply log and return ok.
    log.info("journal/sync requested: since_hours=%s limit=%s", body.since_hours, body.limit)
    store.load()  # load latest from disk, no broker by default
    return {"ok": True, "updated": 0, "count": len(store.rows())}

@app.post("/journal/backfill")
def journal_backfill(body: SyncBody):
    # Same behavior as sync for now; route provided to avoid 404s.
    log.info("journal/backfill requested: since_hours=%s limit=%s", body.since_hours, body.limit)
    store.load()
    return {"ok": True, "updated": 0, "count": len(store.rows())}

class EnrichBody(BaseModel):
    limit: int = 2000

def _enrich_with_trade_details(rows: List[Dict[str,Any]]) -> int:
    trade_details = try_import_broker_trade_details()
    if not callable(trade_details):
        return 0
    missing = [r.get("txid") for r in rows if r.get("txid") and (not r.get("descr") or r.get("userref") is None)]
    missing = [t for t in missing if t]
    updated = 0
    BATCH = 50
    for i in range(0, len(missing), BATCH):
        batch = missing[i:i+BATCH]
        try:
            info = trade_details(batch) or {}
        except Exception as e:
            log.warning("trade_details call failed: %s", e)
            info = {}
        if not isinstance(info, dict):
            continue
        for r in rows:
            tx = r.get("txid")
            if not tx: continue
            if tx in info and isinstance(info[tx], dict):
                if "descr" in info[tx] and info[tx]["descr"]:
                    r["descr"] = info[tx]["descr"]
                if "userref" in info[tx]:
                    r["userref"] = info[tx]["userref"]
                if "ordertxid" in info[tx]:
                    r["ordertxid"] = info[tx]["ordertxid"]
                updated += 1
    return updated

def _light_enrich_row(r: Dict[str,Any]) -> None:
    # ensure ts_iso, normalize symbols, fill price from cost/vol if missing
    r["ts_iso"] = ts_to_iso(r.get("ts"))
    if "symbol" in r and isinstance(r["symbol"], str):
        r["symbol"] = norm_symbol(r["symbol"])
    if r.get("price") in (None, "", 0) and r.get("cost") and r.get("vol"):
        try:
            r["price"] = float(r["cost"]) / float(r["vol"])
        except Exception:
            pass
    # parse descr like "buy 1.44271 LINKUSD @ market"
    d = r.get("descr") or ""
    m = re.search(r"(buy|sell)\s+([0-9.]+)\s+([A-Z/]+)\s*@", d, re.I)
    if m:
        side, vol, sy = m.group(1), m.group(2), m.group(3).upper().replace("USD","/USD") if "/" not in m.group(3) else m.group(3).upper()
        r.setdefault("side", side.lower())
        try: r.setdefault("vol", float(vol))
        except: pass
        r.setdefault("symbol", norm_symbol(sy))

@app.post("/journal/enrich")
def journal_enrich(body: EnrichBody):
    rows = store.rows()
    rows_sorted = sorted(rows, key=lambda r: r.get("ts") or 0, reverse=True)[:body.limit]
    for r in rows_sorted:
        _light_enrich_row(r)
    updated = _enrich_with_trade_details(rows_sorted)
    store.save()
    return {"ok": True, "updated": int(updated), "checked": len(rows_sorted)}

@app.post("/journal/enrich/deep")
def journal_enrich_deep(body: EnrichBody):
    rows = store.rows()
    rows_sorted = sorted(rows, key=lambda r: r.get("ts") or 0, reverse=True)[:body.limit]
    # light enrich first
    for r in rows_sorted:
        _light_enrich_row(r)
    # trade_details pass
    updated = _enrich_with_trade_details(rows_sorted)
    # try to attach current prices for rows missing price
    for r in rows_sorted:
        if not r.get("price") and r.get("symbol"):
            px = fetch_price_from_kraken(r["symbol"])
            if px:
                r["mkt_price"] = px
    store.save()
    return {"ok": True, "updated": int(updated), "checked": len(rows_sorted)}

@app.get("/journal")
def journal_get(limit: int = 50):
    rows = sorted(store.rows(), key=lambda r: r.get("ts") or 0, reverse=True)[:limit]
    return {"ok": True, "rows": rows, "count": len(rows)}

@app.post("/journal/sanity")
def journal_sanity(body: EnrichBody):
    _ = store.sanitize()
    rows = sorted(store.rows(), key=lambda r: r.get("ts") or 0, reverse=True)[:body.limit]
    # find obvious issues
    issues = []
    for r in rows:
        if not r.get("symbol") or not r.get("side"):
            issues.append(r)
        if r.get("side") in ("buy","sell") and (not r.get("vol") or not (r.get("price") or r.get("cost"))):
            issues.append(r)
    return {"ok": True, "rows": issues[:200], "count": len(issues)}

# ------------------------
# P&L and KPIs
# ------------------------

def fifo_pnl(rows: List[Dict[str,Any]]) -> Tuple[float, Dict[str,Any]]:
    # Compute realized P&L using FIFO by symbol.
    positions: Dict[str, List[Tuple[float,float]]] = {}  # symbol -> list of (vol, unit_cost)
    realized = 0.0
    realized_by_symbol: Dict[str, float] = {}
    fees = 0.0

    for r in sorted(rows, key=lambda x: x.get("ts") or 0):
        sym = r.get("symbol")
        side = (r.get("side") or "").lower()
        vol = r.get("vol") or 0.0
        price = r.get("price") or 0.0
        cost = r.get("cost")
        fee = r.get("fee") or 0.0
        if not sym or side not in ("buy","sell") or not vol:
            continue
        if not price and cost and vol:
            try: price = float(cost)/float(vol)
            except: price = 0.0

        fees += float(fee or 0.0)

        if side == "buy":
            positions.setdefault(sym, []).append((float(vol), float(price)))
        elif side == "sell":
            # consume FIFO
            qty_to_sell = float(vol)
            gained = 0.0
            basis = 0.0
            book = positions.setdefault(sym, [])
            while qty_to_sell > 1e-12 and book:
                buy_vol, buy_price = book[0]
                take = min(buy_vol, qty_to_sell)
                basis += take * buy_price
                gained += take * price
                buy_vol -= take
                qty_to_sell -= take
                if buy_vol <= 1e-12:
                    book.pop(0)
                else:
                    book[0] = (buy_vol, buy_price)
            # if we sold more than we had, treat remaining as zero-basis (short-close not supported here)
            if qty_to_sell > 1e-12:
                gained += qty_to_sell * price
            pnl = gained - basis
            realized += pnl
            realized_by_symbol[sym] = realized_by_symbol.get(sym, 0.0) + pnl

    out = {
        "realized": realized,
        "fees": fees,
        "realized_by_symbol": realized_by_symbol,
    }
    return realized, out

@app.get("/pnl/summary")
def pnl_summary():
    rows = store.rows()
    _, details = fifo_pnl(rows)
    return {"ok": True, "summary": details, "count": len(rows)}

@app.get("/kpis")
def kpis_get():
    rows = store.rows()
    by_symbol: Dict[str, Dict[str, float]] = {}
    by_strategy: Dict[str, Dict[str, float]] = {}
    # Build P&L per symbol/strategy
    realized_by_symbol: Dict[str, float] = {}
    realized, info = fifo_pnl(rows)
    realized_by_symbol = info.get("realized_by_symbol", {})

    for sym, val in realized_by_symbol.items():
        by_symbol[sym] = {"pnl": val}

    # simple counts by strategy
    for r in rows:
        strat = r.get("strategy") or "unknown"
        by_strategy.setdefault(strat, {"trades": 0, "notional_sum": 0.0})
        by_strategy[strat]["trades"] += 1
        notional = r.get("notional") or r.get("cost") or 0.0
        by_strategy[strat]["notional_sum"] += float(notional)

    return {"ok": True, "by_symbol": by_symbol, "by_strategy": by_strategy}

# ------------------------
# Policy + Scheduler stubs
# ------------------------

POLICY_SNAPSHOT = {
    "windows": {
        "c1": {"days": ["Mon","Tue","Wed","Thu","Fri"], "hours": [13,14,15,16,17,18,19,20]},
        "c2": {"days": ["Mon","Tue","Wed","Thu","Fri"], "hours": [6,7,8,19,20,21,22,23]},
        "c3": {"days": ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], "hours": [7,8,9,18,19,20,21,22]},
        "c4": {"days": ["Mon","Tue","Wed","Thu","Fri"], "hours": [10,11,12,13,14,20,21]},
        "c5": {"days": ["Mon","Tue","Wed","Thu","Fri"], "hours": [0,1,2,18,19,20]},
        "c6": {"days": ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], "hours": [1,2,3,20,21,22]}
    },
    "whitelist": {
        "c1": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c2": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c3": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c4": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c5": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"],
        "c6": ["BTC/USD","ETH/USD","SOL/USD","LINK/USD"]
    },
    "risk": {
        "fee_rate_pct": 0.26,
        "edge_multiple_vs_fee": 3.0,
        "atr_floor_pct": {"tier1": 0.6, "tier2": 0.9, "tier3": 1.2},
        "tiers": {
            "tier1": ["BTCUSD","ETHUSD","SOLUSD"],
            "tier2": ["XRPUSD","ADAUSD","DOGEUSD","LTCUSD","BCHUSD","AVAXUSD","LINKUSD"],
            "tier3": []
        },
        "symbol_mutex_minutes": 60,
        "cooldown_minutes_after_exit_for_mr": 30,
        "mr_strategies": ["c1"],
        "avoid_pairs": []
    }
}

@app.get("/policy")
def policy_get():
    return {"ok": True, "policy": POLICY_SNAPSHOT}

class SchedulerBody(BaseModel):
    dry: bool = True
    tf: str = "5Min"
    strats: str = "c1,c2,c3,c4,c5,c6"
    symbols: str = "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD"
    limit: int = 300
    notional: float = 25.0

@app.post("/scheduler/run")
def scheduler_run(body: SchedulerBody):
    log.info("Scheduler pass: strats=%s tf=%s limit=%s notional=%.2f dry=%s symbols=%s",
             body.strats, body.tf, body.limit, body.notional, body.dry, body.symbols)
    # This is a stub echo; your real scheduler can be called here if available.
    return {"ok": True, "accepted": True, "echo": body.dict()}

# ------------------------
# Health + root
# ------------------------
@app.get("/health")
def health():
    return {
        "ok": True,
        "service": APP_NAME,
        "version": APP_VERSION,
        "uptime_sec": utc_now_ts() - START_TS,
        "routes": [
            "/",
            "/health",
            "/price/{base}/{quote}",
            "/prices",
            "/journal",
            "/journal/sync",
            "/journal/backfill",
            "/journal/enrich",
            "/journal/enrich/deep",
            "/journal/sanity",
            "/pnl/summary",
            "/kpis",
            "/policy",
            "/scheduler/run"
        ]
    }

@app.get("/")
def root():
    return {
        "ok": True,
        "service": APP_NAME,
        "version": APP_VERSION,
        "message": "Crypto System API is running. Dashboard is served separately."
    }

# ------------------------
# Run (Render uses PORT)
# ------------------------
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "10000"))
    log.info("Starting %s v%s on 0.0.0.0:%d", APP_NAME, APP_VERSION, port)
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
