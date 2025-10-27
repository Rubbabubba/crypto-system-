"""
crypto-system-api (app.py) — v2.1.0
------------------------------------
Full drop-in FastAPI app.

# Routes (human overview)
#   GET   /                      -> root info/version
#   GET   /health                -> service heartbeat
#   GET   /routes                -> list available routes (helper)
#   GET   /policy                -> example whitelist policy
#   GET   /price/{base}/{quote}  -> public spot price via Kraken (normalized)
#   GET   /dashboard             -> serves ./static/dashboard.html if present
#   [Static] /static/*           -> static assets if ./static exists
#   GET   /debug/config          -> shows env-detection + time (no secrets)
#
#   GET   /journal               -> peek journal rows (limit=, offset=)
#   POST  /journal/backfill      -> pull long history from Kraken (since_hours, limit)
#   POST  /journal/sync          -> pull recent history from Kraken (since_hours, limit)
#   POST  /journal/enrich        -> no-op enrich (OK shape for tests)
#   POST  /journal/enrich/deep   -> no-op deep enrich
#   POST  /journal/sanity        -> light checks over stored rows
#
#   GET   /pnl/summary           -> tiny PnL-style rollup from journal
#   GET   /kpis                  -> basic counters
#
#   POST  /scheduler/run         -> stub scheduler (dry-run supported)

# Notes
# - Kraken credentials: accepts either naming scheme:
#       KRAKEN_API_KEY   or  KRAKEN_KEY
#       KRAKEN_API_SECRET or KRAKEN_SECRET
# - Data dir: respects DATA_DIR (default ./data). Falls back to temp if not writeable.
# - Pair normalization: BTC->XBT, and common USD pairs mapped to Kraken altnames for public/private calls.
"""

import base64
import datetime as dt
import hashlib
import hmac
import json
import logging
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import Body, FastAPI, HTTPException, Query
__version__ = '2.3.0'
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# --------------------------------------------------------------------------------------
# Version / Logging
# --------------------------------------------------------------------------------------

APP_VERSION = "1.12.6"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("crypto-system-api")

# --------------------------------------------------------------------------------------
# Data Directory (avoid unwritable /mnt/data on some hosts)
# --------------------------------------------------------------------------------------

def _pick_data_dir() -> Path:
    candidate = os.getenv("DATA_DIR", "./data")
    p = Path(candidate)
    try:
        p.mkdir(parents=True, exist_ok=True)
        # probe writeability
        test = p / ".wtest"
        test.write_text("ok", encoding="utf-8")
        test.unlink(missing_ok=True)
        return p
    except Exception:
        # fallback to a temp-like local folder
        p = Path("./_data_fallback")
        p.mkdir(parents=True, exist_ok=True)
        return p

DATA_DIR = _pick_data_dir()
DB_PATH = DATA_DIR / "journal.db"

# --------------------------------------------------------------------------------------
# Kraken credentials & normalization helpers
# --------------------------------------------------------------------------------------

def _get_env_first(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None

def _kraken_creds() -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """Return (api_key, api_secret, key_name_used, secret_name_used)."""
    key_name = "KRAKEN_API_KEY" if os.getenv("KRAKEN_API_KEY") else ("KRAKEN_KEY" if os.getenv("KRAKEN_KEY") else None)
    sec_name = "KRAKEN_API_SECRET" if os.getenv("KRAKEN_API_SECRET") else ("KRAKEN_SECRET" if os.getenv("KRAKEN_SECRET") else None)
    key = _get_env_first("KRAKEN_API_KEY", "KRAKEN_KEY")
    sec = _get_env_first("KRAKEN_API_SECRET", "KRAKEN_SECRET")
    return key, sec, key_name, sec_name

# Public/private altname mapping (expand as needed)
# Note: Kraken uses XBT for Bitcoin.
_BASE_ALT = {
    "BTC": "XBT",
    "XBT": "XBT",
    "ETH": "ETH",
    "SOL": "SOL",
    "DOGE": "DOGE",  # legacy XDG existed historically
    "XRP": "XRP",
    "AVAX": "AVAX",
    "LINK": "LINK",
    "BCH": "BCH",
    "LTC": "LTC",
}
# inverse mapping for rendering to app-style
_BASE_INV = {v: k for k, v in _BASE_ALT.items()}

def to_kraken_alt_pair(base: str, quote: str) -> str:
    """App 'BTC/USD' -> Kraken 'XBTUSD' altnames."""
    b = _BASE_ALT.get(base.upper(), base.upper())
    q = quote.upper()
    return f"{b}{q}"

def from_kraken_pair_to_app(sym: str) -> str:
    """
    Kraken pair to app 'BASE/QUOTE'.
    Handles both altnames like 'XBTUSD' and some legacy like 'XXBTZUSD' by stripping leading X/Z.
    """
    s = sym.upper()
    # legacy style sometimes like XXBTZUSD -> strip leading X/Z prefixes
    if len(s) > 6 and s.count("/") == 0:
        # heuristic: try to isolate base(3-4)+quote(3)
        # Strip leading X/Z on base and possible Z on quote
        # Examples: XXBTZUSD -> XBTUSD, XETHZEUR -> ETHEUR
        s = s.lstrip("XZ")
        s = s.replace("ZUSD", "USD").replace("ZEUR", "EUR").replace("ZUSDT", "USDT")
    if "/" in s:
        parts = s.split("/")
        b_raw, q = parts[0], parts[1]
    else:
        # assume last 3-4 are quote, rest base
        if s.endswith("USDT"):
            q = "USDT"; b_raw = s[:-4]
        elif s.endswith("USD"):
            q = "USD"; b_raw = s[:-3]
        elif s.endswith("EUR"):
            q = "EUR"; b_raw = s[:-3]
        else:
            # fallback: treat last 3 chars as quote
            q = s[-3:]; b_raw = s[:-3]
    b = _BASE_INV.get(b_raw, b_raw)
    return f"{b}/{q}"

# --------------------------------------------------------------------------------------
# Kraken API client (public & private)
# --------------------------------------------------------------------------------------

KRAKEN_API_BASE = "https://api.kraken.com"

def kraken_public_ticker(alt_pair: str) -> Dict[str, Any]:
    url = f"{KRAKEN_API_BASE}/0/public/Ticker"
    r = requests.get(url, params={"pair": alt_pair}, timeout=15)
    r.raise_for_status()
    return r.json()

def _kraken_sign(path: str, data: Dict[str, Any], secret_b64: str) -> str:
    # per Kraken docs: API-Sign = HMAC-SHA512(path + SHA256(nonce+postdata)) using base64-decoded secret
    postdata = "&".join([f"{k}={data[k]}" for k in data])
    message = (str(data["nonce"]) + postdata).encode()
    sha256 = hashlib.sha256(message).digest()
    mac = hmac.new(base64.b64decode(secret_b64), (path.encode() + sha256), hashlib.sha512)
    return base64.b64encode(mac.digest()).decode()

def kraken_private(method: str, data: Dict[str, Any], key: str, secret_b64: str) -> Dict[str, Any]:
    path = f"/0/private/{method}"
    url = f"{KRAKEN_API_BASE}{path}"
    data = dict(data) if data else {}
    data["nonce"] = int(time.time() * 1000)
    headers = {
        "API-Key": key,
        "API-Sign": _kraken_sign(path, data, secret_b64),
        "Content-Type": "application/x-www-form-urlencoded",
    }
    r = requests.post(url, headers=headers, data=data, timeout=30)
    r.raise_for_status()
    return r.json()

# --------------------------------------------------------------------------------------
# SQLite Journal store
# --------------------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            txid TEXT PRIMARY KEY,
            ts REAL,
            pair TEXT,
            symbol TEXT,
            side TEXT,
            price REAL,
            volume REAL,
            cost REAL,
            fee REAL,
            strategy TEXT,
            raw JSON
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts)")
    return conn

def insert_trades(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn = _db()
    cur = conn.cursor()
    ins = """
        INSERT OR IGNORE INTO trades
        (txid, ts, pair, symbol, side, price, volume, cost, fee, raw)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    count = 0
    for r in rows:
        try:
            cur.execute(ins, (
                r.get("txid"),
                float(r.get("ts", 0)),
                r.get("pair"),
                r.get("symbol"),
                r.get("side"),
                float(r.get("price", 0) or 0),
                float(r.get("volume", 0) or 0),
                float(r.get("cost", 0) or 0),
                float(r.get("fee", 0) or 0),
                json.dumps(r, separators=(",", ":")),
            ))
            count += cur.rowcount
        except Exception as e:
            log.warning(f"insert skip txid={r.get('txid')}: {e}")
    conn.commit()
    conn.close()
    return count

def fetch_rows(limit: int = 25, offset: int = 0) -> List[Dict[str, Any]]:
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT txid, ts, pair, symbol, side, price, volume, cost, fee, raw FROM trades ORDER BY ts DESC LIMIT ? OFFSET ?", (limit, offset))
    out = []
    for row in cur.fetchall():
        txid, ts_, pair, symbol, side, price, volume, cost, fee, raw = row
        try:
            raw_obj = json.loads(raw) if raw else None
        except Exception:
            raw_obj = None
        out.append({
            "txid": txid, "ts": ts_, "pair": pair, "symbol": symbol, "side": side,
            "price": price, "volume": volume, "cost": cost, "fee": fee, "raw": raw_obj
        })
    conn.close()
    return out

def count_rows() -> int:
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM trades")
    n = cur.fetchone()[0] or 0
    conn.close()
    return int(n)

# --------------------------------------------------------------------------------------
# FastAPI app & static
# --------------------------------------------------------------------------------------

app = FastAPI(title="crypto-system-api", version=APP_VERSION)

# CORS (relaxed; tighten if needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Static mount if ./static exists
STATIC_DIR = Path("./static")
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# --------------------------------------------------------------------------------------
# Models
# --------------------------------------------------------------------------------------

class PriceResponse(BaseModel):
    ok: bool
    symbol: str
    price: float
    source: str = "kraken"

# --------------------------------------------------------------------------------------
# Utility
# --------------------------------------------------------------------------------------

def hours_to_start_ts(hours: int) -> int:
    now = int(time.time())
    if not hours or hours <= 0:
        return 0
    return now - int(hours * 3600)

def _kraken_error_str(resp: Dict[str, Any]) -> Optional[str]:
    try:
        errs = resp.get("error") or []
        if errs:
            return "; ".join(errs)
    except Exception:
        pass
    return None

# --------------------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------------------


def price_ticker(base: str, quote: str) -> Dict[str, Any]:
    """
    Fetch last price for base/quote from Kraken public API via our existing kraken_public helper.
    Returns {"pair": "BASE/QUOTE", "price": float}
    """
    pair = f"{base}/{quote}"
    try:
        sym = f"{base}{quote}".replace("/", "").upper()
        # Kraken wants e.g. XXBTZUSD for BTC/USD - but assume we have a kraken_public wrapper that accepts 'pair' and maps.
        data = kraken_public("Ticker", {"pair": f"{base}/{quote}"})
        res = data.get("result") or {}
        # pick first value
        if res:
            first = next(iter(res.values()))
            # 'c' -> last trade [price, lot]
            last = first.get("c", [None])[0]
            return {"pair": pair, "price": float(last) if last is not None else 0.0}
    except Exception as e:
        log.warning(f"price_ticker failed for {pair}: {e}")
    return {"pair": pair, "price": 0.0}

@app.get("/")
def root():
    return {"ok": True, "name": "crypto-system-api", "version": APP_VERSION}

@app.get("/health")
def health():
    return {"ok": True, "version": APP_VERSION, "ts": int(time.time())}

@app.get("/routes")
def routes():
    # helper enumerating registered routes
    paths = sorted({r.path for r in app.routes})
    return {"routes": paths, "count": len(paths)}

@app.get("/dashboard")
def dashboard():
    # Serve ./static/dashboard.html if present; else redirect to root.
    dash = STATIC_DIR / "dashboard.html"
    if dash.exists():
        return FileResponse(str(dash))
    return RedirectResponse(url="/")

# ---- Policy (sample whitelist) -------------------------------------------------------

@app.get("/policy")
def policy():
    return {
        "ok": True,
        "whitelist": {
            "core": ["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD", "XRP/USD"],
            "alts": ["AVAX/USD", "LINK/USD", "BCH/USD", "LTC/USD"],
        },
    }

# ---- Price ---------------------------------------------------------------------------

@app.get("/price/{base}/{quote}", response_model=PriceResponse)
def price(base: str, quote: str):
    sym_app = f"{base.upper()}/{quote.upper()}"
    alt = to_kraken_alt_pair(base, quote)  # e.g., BTC/USD -> XBTUSD
    try:
        data = kraken_public_ticker(alt)
        err = _kraken_error_str(data)
        if err:
            raise HTTPException(status_code=502, detail=f"Kraken error: {err}")
        result = data.get("result") or {}
        if not result:
            raise HTTPException(status_code=502, detail="No ticker data")
        # the only key is the pair, unknown exact spelling, pick first
        k = next(iter(result.keys()))
        last_trade = result[k]["c"][0]  # 'c' -> last trade price [price, lot]
        px = float(last_trade)
        return PriceResponse(ok=True, symbol=sym_app, price=px)
    except requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"HTTP error from Kraken: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Price error: {e}")

# ---- Debug config (env detection — no secrets) ---------------------------------------

@app.get("/config")
def get_config():
    symbols = [s.strip() for s in os.getenv("SYMBOLS","").split(",") if s.strip()]
    strategies = [s.strip() for s in os.getenv("SCHED_STRATS","").split(",") if s.strip()]
    return {
        "ok": True,
        "service": os.getenv("SERVICE_NAME", "Crypto System"),
        "version": APP_VERSION,
        "symbols": symbols,
        "strategies": strategies,
        "timeframe": os.getenv("SCHED_TIMEFRAME", os.getenv("DEFAULT_TIMEFRAME","5Min")),
        "notional": float(os.getenv("SCHED_NOTIONAL", os.getenv("DEFAULT_NOTIONAL","25") or 25)),
        "limit": int(os.getenv("SCHED_LIMIT", os.getenv("DEFAULT_LIMIT","300") or 300)),
        "trading_enabled": bool(int(os.getenv("TRADING_ENABLED","1") or "1")),
        "tz": os.getenv("TZ","America/Chicago")
    }
@app.get("/debug/config")
def debug_config():
    key, sec, key_name, sec_name = _kraken_creds()
    now = int(time.time())
    return {
        "ok": True,
        "kraken_env_detected": {
            "API_KEY_present": bool(key),
            "API_KEY_used_name": key_name,
            "API_SECRET_present": bool(sec),
            "API_SECRET_used_name": sec_name,
        },
        "data_dir": str(DATA_DIR),
        "db_path": str(DB_PATH),
        "time": {"now_ts": now, "iso": dt.datetime.utcfromtimestamp(now).isoformat() + "Z"},
    }

# ---- Journal endpoints ---------------------------------------------------------------

def _pull_trades_from_kraken(since_hours: int, hard_limit: int) -> Tuple[int, int, Optional[str]]:
    """
    Pull trades via private TradesHistory and insert into SQLite.
    Returns (inserted, seen_count, last_error)
    """
    key, sec, _, _ = _kraken_creds()
    if not key or not sec:
        return (0, 0, "Missing Kraken API credentials")

    start_ts = hours_to_start_ts(since_hours)
    inserted = 0
    seen = 0
    last_error = None
    ofs = 0
    page_size = 50  # Kraken paginates with 'ofs'; response includes 'count'

    try:
        while True:
            payload = {"type": "all", "trades": True, "ofs": ofs}
            if start_ts > 0:
                payload["start"] = start_ts  # unix seconds

            resp = kraken_private("TradesHistory", payload, key, sec)
            err = _kraken_error_str(resp)
            if err:
                last_error = err
                break

            result = resp.get("result") or {}
            trades = result.get("trades") or {}
            total_count = int(result.get("count") or 0)

            rows = []
            for txid, t in trades.items():
                # t keys: ordertxid, pair, time, type, ordertype, price, cost, fee, vol, margin, misc, posstatus, cprice, ccost, cfee, cvol, cmargin, net, trades
                pair_raw = t.get("pair") or ""
                symbol = from_kraken_pair_to_app(pair_raw)
                rows.append({
                    "txid": txid,
                    "ts": float(t.get("time", 0)),
                    "pair": pair_raw,
                    "symbol": symbol,
                    "side": t.get("type"),
                    "price": float(t.get("price") or 0),
                    "volume": float(t.get("vol") or 0),
                    "cost": float(t.get("cost") or 0),
                    "fee": float(t.get("fee") or 0),
                    "strategy": None,
                    "raw": t,
                })

            seen += len(rows)
            inserted += insert_trades(rows)

            # stop conditions
            ofs += page_size
            if seen >= hard_limit:
                break
            if ofs >= total_count:
                break

            # slight delay to be nice to API
            time.sleep(0.2)

    except requests.HTTPError as e:
        last_error = f"HTTP {e}"
    except Exception as e:
        last_error = str(e)
        log.exception("Error during TradesHistory loop")

    return (inserted, seen, last_error)

@app.get("/journal")
def journal_peek(limit: int = Query(25, ge=1, le=1000), offset: int = Query(0, ge=0)):

    rows = fetch_rows(limit=limit, offset=offset)
    return {"ok": True, "count": count_rows(), "rows": rows}
@app.post("/journal/attach")
def journal_attach(payload: Dict[str, Any] = Body(...)):
    txid = payload.get("txid")
    strategy = payload.get("strategy")
    if not txid or not strategy:
        raise HTTPException(status_code=400, detail="txid and strategy required")
    conn = _db()
    cur = conn.cursor()
    cur.execute("UPDATE trades SET strategy=? WHERE txid=?", (str(strategy), str(txid)))
    conn.commit()
    return {"ok": True, "updated": cur.rowcount}

@app.get("/fills")
def get_fills(limit: int = Query(100, ge=1, le=1000)):
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT txid, ts, symbol, side, price, volume, fee, strategy FROM trades ORDER BY ts DESC LIMIT ?", (limit,))
    rows = [dict(txid=r[0], ts=r[1], symbol=r[2], side=r[3], price=r[4], volume=r[5], fee=r[6], strategy=r[7]) for r in cur.fetchall()]
    return {"ok": True, "rows": rows, "count": len(rows)}
    rows = fetch_rows(limit=limit, offset=offset)
    return {"ok": True, "count": count_rows(), "rows": rows}

@app.post("/journal/backfill")
def journal_backfill(payload: Dict[str, Any] = Body(...)):
    since_hours = int(payload.get("since_hours", 24 * 365))
    limit = int(payload.get("limit", 100000))
    start_ts = hours_to_start_ts(since_hours)

    inserted, seen, last_error = _pull_trades_from_kraken(since_hours, limit)
    return {
        "ok": True,
        "updated": inserted,
        "count": seen,
        "debug": {
            "creds_present": all(_kraken_creds()[:2]),
            "since_hours": since_hours,
            "start_ts": start_ts,
            "start_iso": dt.datetime.utcfromtimestamp(start_ts).isoformat() + "Z" if start_ts else None,
            "limit": limit,
            "last_error": last_error,
        },
    }

@app.post("/journal/sync")
def journal_sync(payload: Dict[str, Any] = Body(...)):
    since_hours = int(payload.get("since_hours", 24 * 90))
    limit = int(payload.get("limit", 50000))
    start_ts = hours_to_start_ts(since_hours)

    inserted, seen, last_error = _pull_trades_from_kraken(since_hours, limit)
    return {
        "ok": True,
        "updated": inserted,
        "count": seen,
        "debug": {
            "creds_present": all(_kraken_creds()[:2]),
            "since_hours": since_hours,
            "start_ts": start_ts,
            "start_iso": dt.datetime.utcfromtimestamp(start_ts).isoformat() + "Z" if start_ts else None,
            "limit": limit,
        }
    }

def _fetch_prices(symbols: List[str]) -> Dict[str, float]:
    prices = {}
    for sym in symbols:
        try:
            base, quote = sym.split("/")
            data = price_ticker(base, quote)
            prices[sym] = float(data.get("price", 0.0))
        except Exception:
            prices[sym] = 0.0
    return prices

def _compute_fifo_pnl(rows: List[Dict[str, Any]], prices: Dict[str, float]):
    # rows: must contain symbol, side, price, volume, fee, strategy
    per = {}  # (strategy, symbol) -> dict
    for r in sorted(rows, key=lambda x: x.get("ts", 0)):
        strat = (r.get("strategy") or "misc")
        sym = r.get("symbol")
        side = (r.get("side") or "").lower()
        price = float(r.get("price") or 0.0)
        vol = float(r.get("volume") or 0.0)
        fee = float(r.get("fee") or 0.0)
        key = (strat, sym)
        d = per.setdefault(key, {"qty":0.0, "avg":0.0, "realized":0.0, "fees":0.0})
        d["fees"] += fee
        if side == "buy":
            new_qty = d["qty"] + vol
            if new_qty > 0:
                d["avg"] = (d["qty"]*d["avg"] + vol*price) / new_qty
            d["qty"] = new_qty
        elif side == "sell":
            close = min(vol, d["qty"])
            if close > 0:
                d["realized"] += (price - d["avg"]) * close
                d["qty"] -= close
        else:
            continue
    per_strategy = {}
    per_symbol = {}
    total = {"realized":0.0, "unrealized":0.0, "fees":0.0, "equity":0.0}
    out_per_strategy = []
    out_per_symbol = []
    for (strat, sym), d in per.items():
        mkt = prices.get(sym, 0.0)
        unreal = (mkt - d["avg"]) * d["qty"] if d["qty"] != 0 else 0.0
        realized = d["realized"]
        fees = d["fees"]
        equity = realized + unreal - fees
        S = per_strategy.setdefault(strat, {"realized":0.0,"unrealized":0.0,"fees":0.0,"equity":0.0})
        for k,v in [("realized",realized),("unrealized",unreal),("fees",fees),("equity",equity)]:
            S[k] += v
        P = per_symbol.setdefault(sym, {"realized":0.0,"unrealized":0.0,"fees":0.0,"equity":0.0})
        for k,v in [("realized",realized),("unrealized",unreal),("fees",fees),("equity",equity)]:
            P[k] += v
        total["realized"] += realized
        total["unrealized"] += unreal
        total["fees"] += fees
        total["equity"] += equity
    for strat, vals in per_strategy.items():
        out_per_strategy.append({"strategy": strat, **vals})
    for sym, vals in per_symbol.items():
        out_per_symbol.append({"symbol": sym, **vals})
    return {"total": total, "per_strategy": out_per_strategy, "per_symbol": out_per_symbol}

DAILY_JSON = os.path.join(DATA_DIR, "daily.json")

@app.get("/advisor/daily")
def advisor_daily():
    try:
        with open(DAILY_JSON, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        data = {"date": dt.date.today().isoformat(), "notes": "", "recommendations": []}
    return {"ok": True, **data}

@app.post("/advisor/apply")
def advisor_apply(payload: Dict[str, Any] = Body(...)):
    data = {
        "date": payload.get("date") or dt.date.today().isoformat(),
        "notes": payload.get("notes") or "",
        "recommendations": payload.get("recommendations") or []
    }
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(DAILY_JSON, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2)
    return {"ok": True, "saved": True, **data}

@app.get("/journal/counts")
def journal_counts():
    conn = _db()
    cur = conn.cursor()
    total = cur.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
    unlabeled = cur.execute("SELECT COUNT(*) FROM trades WHERE strategy IS NULL OR TRIM(strategy)=''").fetchone()[0]
    labeled = total - unlabeled
    per_strategy = []
    for row in cur.execute("SELECT COALESCE(NULLIF(TRIM(strategy), ''), 'unlabeled') AS s, COUNT(*) FROM trades GROUP BY s ORDER BY COUNT(*) DESC").fetchall():
        per_strategy.append({"strategy": row[0], "count": row[1]})
    conn.close()
    return {"ok": True, "total": total, "labeled": labeled, "unlabeled": unlabeled, "per_strategy": per_strategy}


@app.post("/journal/enrich")
def journal_enrich(payload: Dict[str, Any] = Body(...)):
    batch_size = int(payload.get("batch_size", 20) or 20)
    max_rows   = int(payload.get("max_rows", 2000) or 2000)
    apply_rules = bool(payload.get("apply_rules", True))

    conn = _db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT txid, ts, symbol, raw FROM trades WHERE strategy IS NULL OR TRIM(strategy)='' LIMIT ?", (max_rows,))
        rows = cur.fetchall()
    finally:
        conn.close()

    def _j(x):
        try:
            return json.loads(x) if isinstance(x, str) else (x or {})
        except Exception:
            return {}

    order_to_trades = {}
    trade_info = {}
    for txid, ts, symbol, raw in rows:
        j = _j(raw)
        otx = j.get("ordertxid") or j.get("order_txid") or j.get("ordertx")
        if isinstance(otx, list): otx = otx[0] if otx else None
        if otx:
            order_to_trades.setdefault(str(otx), []).append(str(txid))
        trade_info[str(txid)] = {"ts": ts, "symbol": symbol, "raw": j, "ordertxid": str(otx) if otx else None}

    all_ids = list(order_to_trades.keys())
    key, sec, _, _ = _kraken_creds()
    orders_meta = {}
    if key and sec and all_ids:
        for i in range(0, len(all_ids), batch_size):
            chunk = all_ids[i:i+batch_size]
            try:
                resp = kraken_private("QueryOrdersInfo", {"txid": ",".join(chunk)}, key, sec)
                res = (resp.get("result") or {}) if isinstance(resp, dict) else {}
                if isinstance(res, dict):
                    orders_meta.update(res)
            except Exception as e:
                log.warning(f"QueryOrdersInfo failed: {e}")

    import re as _re, datetime as _dt, pytz
    def infer_order_strat(o):
        if not isinstance(o, dict): return None
        u = o.get("userref")
        if isinstance(u, str) and _re.fullmatch(r"c[1-9]", u.lower()): return u.lower()
        if isinstance(u, int) and 1 <= u <= 9: return f"c{u}"
        d = o.get("descr") or {}
        for val in d.values():
            if not isinstance(val, str): continue
            m = _re.search(r"\b(c[1-9])\b", val.lower())
            if m: return m.group(1)
            m = _re.search(r"strat\s*[:=]\s*(c[1-9])", val.lower())
            if m: return m.group(1)
        return None

    # rules
    whitelist = {}
    windows = {}
    try:
        whitelist = json.load(open(DATA_DIR / "whitelist.json", "r", encoding="utf-8"))
    except Exception:
        try:
            whitelist = json.load(open("whitelist.json", "r", encoding="utf-8"))
        except Exception:
            pass
    try:
        windows = json.load(open(DATA_DIR / "windows.json", "r", encoding="utf-8"))
    except Exception:
        try:
            windows = json.load(open("windows.json", "r", encoding="utf-8"))
        except Exception:
            pass

    tzname = os.getenv("TZ", "America/Chicago")
    try:
        tz = pytz.timezone(tzname)
    except Exception:
        import pytz as _p; tz = _p.UTC

    def allowed_by_rules(strat, symbol, ts):
        syms = whitelist.get(strat)
        if syms and ("*" not in syms) and (symbol not in syms): return False
        win = windows.get(strat) or {}
        days = set([d[:3].title() for d in (win.get("days") or [])])
        hours = set(win.get("hours") or [])
        if not days and not hours: return True
        try:
            t = _dt.datetime.fromtimestamp(float(ts), tz)
            if days and t.strftime("%a") not in days: return False
            if hours and t.hour not in set(int(h) for h in hours): return False
            return True
        except Exception:
            return False

    to_update = {}
    api_labeled = rules_labeled = ambiguous = missing_order = 0

    for oid, lst in order_to_trades.items():
        strat = infer_order_strat(orders_meta.get(oid) or {})
        if strat:
            for tx in lst: to_update[tx] = strat
            api_labeled += len(lst)
        else:
            missing_order += len(lst)

    if apply_rules and (whitelist or windows):
        all_strats = list(set(list(whitelist.keys()) + list(windows.keys())))
        for txid, info in trade_info.items():
            if txid in to_update: continue
            sym, ts = info.get("symbol"), info.get("ts")
            if not sym or ts is None: continue
            cands = [s for s in all_strats if allowed_by_rules(s, sym, ts)]
            if len(cands) == 1:
                to_update[txid] = cands[0]; rules_labeled += 1
            elif len(cands) != 1:
                ambiguous += 1

    updated = 0
    if to_update:
        conn = _db(); cur = conn.cursor()
        for txid, strat in to_update.items():
            try:
                cur.execute("UPDATE trades SET strategy=? WHERE txid=?", (str(strat), str(txid)))
                updated += cur.rowcount
            except Exception as e:
                log.warning(f"update failed txid={txid}: {e}")
        conn.commit(); conn.close()

    return {
        "ok": True,
        "scanned": len(rows),
        "orders_checked": len(all_ids),
        "updated": updated,
        "api_labeled": api_labeled,
        "rules_labeled": rules_labeled,
        "ambiguous": ambiguous,
        "missing_order": missing_order,
        "apply_rules": apply_rules,
        "batch_size": batch_size,
        "max_rows": max_rows,
    }

@app.get("/pnl/summary")
def pnl_summary():
    conn = _db()
    cur = conn.cursor()
    cur.execute("SELECT ts, symbol, side, price, volume, fee, COALESCE(strategy, 'misc') as strategy FROM trades")
    rows = [
        {"ts": r[0], "symbol": r[1], "side": r[2], "price": r[3], "volume": r[4], "fee": r[5], "strategy": r[6]} for r in cur.fetchall()
    ]
    symbols = sorted({r["symbol"] for r in rows})
    prices = _fetch_prices(symbols)
    roll = _compute_fifo_pnl(rows, prices)
    roll.update({"ok": True, "counts": {"journal_rows": len(rows)}})
    return roll

@app.get("/kpis")
def kpis():
    return {
        "ok": True,
        "counts": {
            "journal_rows": count_rows(),
        }
    }


_SCHED_ENABLED = bool(int(os.getenv("SCHED_ON", os.getenv("SCHED_ENABLED","1") or "1")))
@app.get("/scheduler/status")
def scheduler_status():
    return { "ok": True, "enabled": _SCHED_ENABLED, "interval_secs": int(os.getenv("SCHED_SLEEP","30") or "30") }

@app.post("/scheduler/start")
def scheduler_start():
    global _SCHED_ENABLED
    _SCHED_ENABLED = True
    return {"ok": True, "enabled": True}

@app.post("/scheduler/stop")
def scheduler_stop():
    global _SCHED_ENABLED
    _SCHED_ENABLED = False
    return {"ok": True, "enabled": False}

# ---- Scheduler (stub) ----------------------------------------------------------------

@app.post("/scheduler/run")
def scheduler_run(payload: Dict[str, Any] = Body(...)):
    dry = bool(payload.get("dry", True))
    tf = payload.get("tf", "5Min")
    strats = str(payload.get("strats", "c1,c2,c3"))
    symbols_csv = str(payload.get("symbols", "BTC/USD,ETH/USD"))
    limit = int(payload.get("limit", 300))
    notional = float(payload.get("notional", 25.0))

    msg = f"Scheduler pass: strats={strats} tf={tf} limit={limit} notional={notional} dry={dry} symbols={symbols_csv}"
    log.info(msg)
    actions = []
    return {"ok": True, "message": msg, "actions": actions}

# --------------------------------------------------------------------------------------
# Startup log
# --------------------------------------------------------------------------------------


def _ensure_strategy_column():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
        # Check if column exists
        cur.execute("PRAGMA table_info(trades)")
        cols = [r[1] for r in cur.fetchall()]
        if "strategy" not in cols:
            cur.execute("ALTER TABLE trades ADD COLUMN strategy TEXT")
            conn.commit()
        cur.execute("CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy)")
        conn.commit()
    finally:
        conn.close()

@app.on_event("startup")
def _startup():
    log.info(f"Starting crypto-system-api v{APP_VERSION} on 0.0.0.0:{os.getenv('PORT','10000')}")
    # ensure DB created
    _ = _db()
    _ensure_strategy_column()
    log.info(f"Data dir: {DATA_DIR} | DB: {DB_PATH}")

# --------------------------------------------------------------------------------------
# Entrypoint (for local runs; Render uses 'python app.py')
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)