"""
crypto-system-api (app.py) — v2.4.0
------------------------------------
Full drop-in FastAPI app.

# Routes (human overview — synced with FastAPI app)
#   GET   /                        -> root info / basic status
#   GET   /health                  -> service heartbeat
#   GET   /routes                  -> list available routes (helper)
#   GET   /dashboard               -> HTML dashboard UI
#   GET   /policy                  -> example policy payload (stub)
#   GET   /price/{base}/{quote}    -> live price via Kraken (normalized pair)
#   GET   /config                  -> app config (symbols, strategies, version, etc.)
#   GET   /debug/config            -> env detection + time (no secrets)
#
#   GET   /journal                 -> peek journal rows (limit, offset)
#   POST  /journal/attach          -> attach/update labels/notes for a row
#   POST  /journal/backfill        -> full backfill from broker fills
#   POST  /journal/sync            -> incremental sync from broker fills
#   GET   /journal/counts          -> counts by strategy / unlabeled
#   POST  /journal/enrich          -> no-op enrich placeholder
#
#   GET   /fills                   -> recent raw broker fills (via broker_kraken)
#
#   GET   /advisor/daily           -> advisor summary + recommendations
#   POST  /advisor/apply           -> apply advisor recommendations to policy_config
#
#   GET   /debug/log/test          -> simple log/test endpoint
#   GET   /debug/kraken            -> basic Kraken connectivity test
#   GET   /debug/db                -> simple DB/journal sanity checks
#   GET   /debug/kraken/trades     -> recent trades/fills from Kraken (variant 1)
#   GET   /debug/kraken/trades2    -> recent trades/fills from Kraken (variant 2)
#   GET   /debug/env               -> safe dump of non-secret env/config
#
#   GET   /pnl/summary             -> P&L summary (possibly using journal fallback)
#   GET   /pnl/fifo                -> FIFO P&L breakdown
#   GET   /pnl/by_strategy         -> P&L grouped by strategy
#   GET   /pnl/by_symbol           -> P&L grouped by symbol
#   GET   /pnl/combined            -> combined P&L view
#
#   GET   /kpis                    -> key performance indicators summary
#
#   GET   /scheduler/status        -> scheduler status (enabled, thread, last run)
#   POST  /scheduler/start         -> start background scheduler
#   POST  /scheduler/stop          -> stop background scheduler
#   GET   /scheduler/last          -> last scheduler run summary
#   POST  /scheduler/run           -> run scheduler once with optional overrides
#
#   [Static] /static/*             -> static assets if ./static mounted
"""

import base64
import datetime as dt

# --- datetime import compatibility shim ---
# Some parts of the code use `datetime.utcnow()` (class) while others use `datetime.datetime.utcnow()` (module).
# If `from datetime import datetime` shadowed the module, rebind `datetime` back to the module.
try:
    _ = datetime.datetime  # OK if 'datetime' is the module
except Exception:
    import importlib as _importlib
    datetime = _importlib.import_module("datetime")
# --- end shim ---
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
import threading
import time
from symbol_map import KRAKEN_PAIR_MAP


# Routes:
#   - /
#   - /health
#   - /routes
#   - /dashboard
#   - /policy
#   - /config
#   - /debug/config
#
#   - /price/{base}/{quote}
#
#   - /journal
#   - /journal/attach
#   - /journal/backfill
#   - /journal/sync
#   - /journal/counts
#   - /journal/enrich
#
#   - /fills
#
#   - /advisor/daily
#   - /advisor/apply
#
#   - /debug/log/test
#   - /debug/kraken
#   - /debug/db
#   - /debug/kraken/trades
#   - /debug/kraken/trades2
#   - /debug/env
#
#   - /pnl/summary
#   - /pnl/fifo
#   - /pnl/by_strategy
#   - /pnl/by_symbol
#   - /pnl/combined
#
#   - /kpis
#
#   - /scheduler/status
#   - /scheduler/start
#   - /scheduler/stop
#   - /scheduler/last
#   - /scheduler/run
#
#   - /scan/all


import requests
from fastapi import Body, FastAPI, HTTPException, Query

# --- logging baseline for Render stdout ---
import logging, sys, os as _os
LOG_LEVEL = _os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger("crypto-system")
log.info("Logging initialized at level %s", LOG_LEVEL)
__version__ = '2.3.4'



from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# --------------------------------------------------------------------------------------
# Version / Logging
# --------------------------------------------------------------------------------------

APP_VERSION = "1.12.9"

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
# Scheduler globals
# --------------------------------------------------------------------------------------
_SCHED_ENABLED = bool(int(os.getenv("SCHED_ON", os.getenv("SCHED_ENABLED", "1") or "1")))
_SCHED_SLEEP = int(os.getenv("SCHED_SLEEP", "30") or "30")
_SCHED_THREAD = None  # type: Optional[threading.Thread]
_SCHED_TICKS = 0
_SCHED_LAST = {}
_SCHED_LAST_LOCK = threading.Lock()
  # number of background scheduler passes completed


# --------------------------------------------------------------------------------------
# Kraken credentials & normalization helpers
# --------------------------------------------------------------------------------------

def _get_env_first(*names: str) -> Optional[str]:
    for n in names:
        v = os.getenv(n)
        if v:
            return v
    return None


def _kraken_creds():
    key = os.getenv("KRAKEN_API_KEY") or os.getenv("KRAKEN_KEY") or ""
    sec = os.getenv("KRAKEN_API_SECRET") or os.getenv("KRAKEN_SECRET") or ""
    user = os.getenv("KRAKEN_USER") or ""
    pwd  = os.getenv("KRAKEN_PASS") or ""
    used = "KRAKEN_API_KEY/SECRET" if os.getenv("KRAKEN_API_KEY") or os.getenv("KRAKEN_API_SECRET") else            ("KRAKEN_KEY/SECRET" if os.getenv("KRAKEN_KEY") or os.getenv("KRAKEN_SECRET") else "none")
    try:
        log.info("_kraken_creds: using pair=%s; key_len=%d sec_len=%d", used, len(key), len(sec))
    except Exception:
        pass
    return key, sec, user, pwd


# ------------------------------------------------------------------------------
# Symbol normalization: Kraken pair -> app UI symbol
# ------------------------------------------------------------------------------
_REV_KRAKEN_PAIR_MAP = {v.upper(): k for k, v in KRAKEN_PAIR_MAP.items()}

def from_kraken_pair_to_app(pair_raw: str) -> str:
    """
    Convert Kraken pair strings (e.g. 'XBTUSD', 'ETHUSD') into UI symbols
    (e.g. 'BTC/USD', 'ETH/USD').

    Uses symbol_map.KRAKEN_PAIR_MAP as the source of truth and falls back
    to a simple 'BASE/USD' rule with XBT->BTC if needed.
    """
    if not pair_raw:
        return ""

    s = str(pair_raw).upper()

    # 1) Exact match from our configured map (recommended pairs)
    if s in _REV_KRAKEN_PAIR_MAP:
        return _REV_KRAKEN_PAIR_MAP[s]

    # 2) Generic USD pairs (e.g. XBTUSD, ETHUSD, SOLUSD, etc.)
    if s.endswith("USD"):
        base = s[:-3]
        if base == "XBT":
            base = "BTC"
        return f"{base}/USD"

    # 3) Fallback – just return the raw pair if we don't know it
    return pair_raw

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


# --- Kraken public REST helper (no auth) ---
import requests as _rq

# --- internal helper to open the journal DB safely (idempotent) ---
def _get_db_conn():
    import os, sqlite3
    data_dir = os.getenv("DATA_DIR", "/var/data")
    os.makedirs(data_dir, exist_ok=True)
    db_path = os.path.join(data_dir, "journal.db")
    conn = sqlite3.connect(db_path, check_same_thread=False)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        conn.commit()
    except Exception:
        pass
    return conn
# --- end helper ---


def kraken_public(endpoint: str, params: dict) -> dict:
    """
    Minimal public API caller for Kraken.
    Example: kraken_public("Time", {}), kraken_public("Ticker", {"pair":"BTC/USD"})
    """
    url = f"https://api.kraken.com/0/public/{endpoint}"
    try:
        r = _rq.get(url, params=params or {}, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.warning("kraken_public error for %s: %s", endpoint, e)
        return {"error": [str(e)], "result": {}}

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
    # Prefer ./static/dashboard.html if present; else fall back to repo root.
    dash = STATIC_DIR / "dashboard.html"
    if not dash.exists():
        alt = Path(__file__).resolve().parent / "dashboard.html"
        if alt.exists():
            dash = alt

    if dash.exists():
        return FileResponse(str(dash))

    # If we still don't have a dashboard file, just go to root
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
def journal_attach(payload: dict = Body(default=None)):
    """
    Attach strategy labels to trades.

    Accepts either:
      - {"strategy": "c3", "txid": "T..."}         # single
      - {"strategy": "c3", "txids": ["T...","T..."]} # bulk

    Returns: {"ok": true, "updated": <int>}
    """
    strategy = (payload or {}).get("strategy")
    txid     = (payload or {}).get("txid")
    txids    = (payload or {}).get("txids")

    if not strategy or (not txid and not txids):
        raise HTTPException(status_code=400, detail="txid or txids and strategy required")

    targets = []
    if txid:
        targets.append(str(txid))
    if isinstance(txids, list):
        targets.extend([str(t) for t in txids if t])

    if not targets:
        return {"ok": True, "updated": 0}

    updated = 0
    conn = _get_db_conn()
    try:
        cur = conn.cursor()
        for t in targets:
            cur.execute("UPDATE trades SET strategy = ? WHERE txid = ?", (strategy, t))
            updated += cur.rowcount
        conn.commit()
    finally:
        try: conn.close()
        except: pass

    return {"ok": True, "updated": int(updated)}

@app.get("/fills")
def get_fills(limit: int = 50, offset: int = 0):
    conn = _get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT txid, ts, pair, symbol, side, price, volume, fee, cost, strategy
            FROM trades
            ORDER BY ts DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )
        rows = [
            {
                "txid": r[0], "ts": r[1], "pair": r[2], "symbol": r[3], "side": r[4],
                "price": r[5], "volume": r[6], "fee": r[7], "cost": r[8], "strategy": r[9]
            }
            for r in cur.fetchall()
        ]
        return {"ok": True, "rows": rows}
    finally:
        conn.close()

@app.post("/journal/backfill")
def journal_backfill(payload: Dict[str, Any] = Body(default=None)):
    payload = payload or {}
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
def journal_sync(payload: Dict[str, Any] = Body(default=None)):
    """
    Pull trades from Kraken TradesHistory and upsert into sqlite.

    Payload:
      - since_hours: int (default 72)
      - limit: int (default 50000) maximum rows to write this call
      - dry_run: bool (optional) if true, do not write

    Strategy:
      1) ofs paging
      2) if plateau or rate-limited, fallback to time-cursor paging
      Both phases use retry with exponential backoff on 'EAPI:Rate limit exceeded'
    """
    import time, json as _json, math as _math

    payload     = payload or {}
    dry_run     = bool(payload.get("dry_run", False))
    since_hours = int(payload.get("since_hours", 72) or 72)
    hard_limit  = int(payload.get("limit", 50000) or 50000)

    key, sec, *_ = _kraken_creds()
    if not (key and sec):
        return {"ok": False, "error": "missing_credentials"}

    # pacing + retries from env
    min_delay   = float(os.getenv("KRAKEN_MIN_DELAY", "0.35") or 0.35)   # base delay between calls
    max_retries = int(os.getenv("KRAKEN_MAX_RETRIES", "6") or 6)         # backoff retries per call

    def _sleep_base():
        time.sleep(min_delay)

    def _kraken_call(payload):
        """TradesHistory with backoff on rate limit"""
        delay = min_delay
        for attempt in range(max_retries + 1):
            resp = kraken_private("TradesHistory", payload, key, sec)
            if not (isinstance(resp, dict) and resp.get("error")):
                return resp
            errs = resp.get("error") or []
            is_rl = any("rate limit" in str(e).lower() for e in errs)
            if not is_rl:
                # non rate-limit error: return as-is
                return resp
            # rate limited -> exponential backoff with jitter
            time.sleep(delay)
            delay = min(delay * 1.7, 8.0)
        return resp  # return last resp (still error)

    start_ts0 = int(time.time() - since_hours * 3600)

    UPSERT_SQL = """
    INSERT INTO trades (txid, ts, symbol, side, price, volume, fee, strategy, raw)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(txid) DO UPDATE SET
        ts      = excluded.ts,
        symbol  = excluded.symbol,
        side    = excluded.side,
        price   = excluded.price,
        volume  = excluded.volume,
        fee     = excluded.fee,
        strategy= COALESCE(trades.strategy, excluded.strategy),
        raw     = excluded.raw
    """

    def flush_batch(batch):
        if dry_run or not batch:
            return 0
        conn = _db(); cur = conn.cursor()
        cur.executemany(UPSERT_SQL, batch)
        conn.commit()
        n = len(batch)
        conn.close()
        return n

    def map_row(txid, t):
        try:
            ts = float(t.get('time')) if t.get('time') is not None else None
        except Exception:
            ts = None
        pair_raw = t.get('pair') or ''
        symbol = from_kraken_pair_to_app(pair_raw)
        side = t.get('type')
        try:
            price = float(t.get('price')) if t.get('price') is not None else None
        except Exception:
            price = None
        try:
            volume = float(t.get('vol')) if t.get('vol') is not None else None
        except Exception:
            volume = None
        try:
            fee = float(t.get('fee')) if t.get('fee') is not None else None
        except Exception:
            fee = None
        raw = _json.dumps(t, separators=(',', ':'), ensure_ascii=False)
        return (str(txid), ts, symbol, side, price, volume, fee, None, raw)

    total_writes = 0
    total_pulled = 0
    pages        = 0
    kraken_count = None
    last_seen_ts = None
    notes        = []

    # ---------- Phase A: ofs paging ----------
    start_ts = start_ts0
    ofs      = 0
    plateau  = False

    while True:
        _sleep_base()
        resp = _kraken_call({"start": start_ts, "ofs": ofs})
        if isinstance(resp, dict) and resp.get("error"):
            notes.append({"phase":"ofs","ofs":ofs,"error":resp.get("error")})
            plateau = True
            break

        res    = (resp.get("result") or {}) if isinstance(resp, dict) else {}
        trades = res.get("trades") or {}
        kraken_count = res.get("count", kraken_count)
        keys = list(trades.keys())
        if not keys:
            break

        pages += 1
        total_pulled += len(keys)

        batch = [map_row(txid, trades[txid]) for txid in keys]
        total_writes += flush_batch(batch)

        # track max ts for cursor fallback
        for txid in keys:
            t = trades[txid]
            try:
                ts = float(t.get("time")) if t.get("time") is not None else None
                if ts is not None:
                    if last_seen_ts is None or ts > last_seen_ts:
                        last_seen_ts = ts
            except:
                pass

        ofs += len(keys)

        if hard_limit and total_writes >= hard_limit:
            break

        # if we’ve pulled a significant chunk but kraken_count indicates more, fallback
        if total_pulled >= 600 and (kraken_count or 0) and total_pulled < (kraken_count or 999999):
            plateau = True
            break
        if pages > 300:
            plateau = True
            break

    # ---------- Phase B: time-cursor fallback ----------
    if plateau:
        cursor = (last_seen_ts or start_ts0)
        # slight backoff to avoid missing trades with identical timestamps
        cursor = max((cursor - 0.5) if cursor else start_ts0, 0.0)
        cursor_pages = 0

        while True:
            _sleep_base()
            payload = {"start": int(_math.floor(cursor)), "ofs": 0}
            resp = _kraken_call(payload)
            if isinstance(resp, dict) and resp.get("error"):
                notes.append({"phase":"cursor","cursor":cursor,"error":resp.get("error")})
                break

            res    = (resp.get("result") or {}) if isinstance(resp, dict) else {}
            trades = res.get("trades") or {}
            keys   = list(trades.keys())
            if not keys:
                break

            # sort by time so we can advance cursor deterministically
            keys.sort(key=lambda k: trades[k].get("time") or 0)
            pages        += 1
            cursor_pages += 1
            total_pulled += len(keys)

            batch = [map_row(txid, trades[txid]) for txid in keys]
            total_writes += flush_batch(batch)

            max_ts = max([trades[k].get("time") or cursor for k in keys])
            cursor = float(max_ts) + 0.5  # nudge ahead

            if hard_limit and total_writes >= hard_limit:
                break
            if cursor_pages > 600:
                notes.append({"phase":"cursor","stopped":"cursor_pages_guard"})
                break

    # Count rows post-write
    try:
        conn = _db(); cur = conn.cursor()
        total_rows = cur.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        conn.close()
    except Exception as e:
        total_rows = None
        notes.append({"post_count_error": str(e)})

    return {
        "ok": True,
        "dry_run": dry_run,
        "count": total_writes,
        "debug": {
            "creds_present": True,
            "since_hours": since_hours,
            "start_ts": start_ts0,
            "start_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_ts0)),
            "pages": pages,
            "pulled": total_pulled,
            "kraken_count": kraken_count,
            "hard_limit": hard_limit,
            "post_total_rows": total_rows,
            "notes": notes
        }
    }

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
def advisor_apply(payload: Dict[str, Any] = Body(default=None)):
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



@app.get("/price/{base}/{quote}")
def price_endpoint(base: str, quote: str):
    data = price_ticker(base, quote)
    return {"ok": True, "pair": data.get("pair"), "price": data.get("price")}

@app.get("/debug/log/test")
def debug_log_test():
    log.debug("debug: hello from /debug/log/test")
    log.info("info: hello from /debug/log/test")
    log.warning("warn: hello from /debug/log/test")
    return {"ok": True, "logged": True}

@app.get("/debug/kraken")
def debug_kraken():
    key, sec, *_ = _kraken_creds()
    out = {"ok": True, "public": None, "private": None, "creds_present": bool(key and sec)}
    try:
        r = kraken_public("Time", {})
        out["public"] = {"ok": True, "result_keys": list((r or {}).keys())[:4]}
        log.info("/debug/kraken public ok: keys=%s", out["public"]["result_keys"])
    except Exception as e:
        out["public"] = {"ok": False, "error": str(e)}
        log.warning("/debug/kraken public err: %s", e)
    if key and sec:
        try:
            r = kraken_private("Balance", {}, key, sec)
            out["private"] = {"ok": True, "result_keys": list((r.get("result") or {}).keys())[:1]}
            log.info("/debug/kraken private ok")
        except Exception as e:
            out["private"] = {"ok": False, "error": str(e)}
            log.warning("/debug/kraken private err: %s", e)
    else:
        out["private"] = {"ok": False, "error": "no_creds_in_env"}
    return out


@app.get("/debug/db")
def debug_db():
    """
    Show DB path, file existence/size, and per-table counts to diagnose empty state.
    """
    import os, sqlite3, time
    info = {"ok": True}
    try:
        # Try to introspect the connection the app uses
        conn = _db()
        cur = conn.cursor()
        # PRAGMA database_list returns file path for main DB
        try:
            rows = cur.execute("PRAGMA database_list").fetchall()
            info["database_list"] = [{"seq": r[0], "name": r[1], "file": r[2]} for r in rows]
            dbfile = rows[0][2] if rows else ""
            if dbfile and os.path.exists(dbfile):
                st = os.stat(dbfile)
                info["db_file"] = {"path": dbfile, "exists": True, "size_bytes": st.st_size, "mtime": st.st_mtime}
            else:
                info["db_file"] = {"path": dbfile, "exists": False}
        except Exception as e:
            info["database_list_error"] = str(e)
        # List tables
        try:
            tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            info["tables"] = tables
            counts = {}
            for t in tables:
                try:
                    c = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                    counts[t] = c
                except Exception as e:
                    counts[t] = f"err: {e}"
            info["counts"] = counts
        except Exception as e:
            info["tables_error"] = str(e)
        conn.close()
    except Exception as e:
        info["ok"] = False
        info["error"] = str(e)
    return info


@app.get("/debug/kraken/trades")
def debug_kraken_trades(since_hours: int = 720, limit: int = 50000):
    """
    Diagnostic: pull Kraken TradesHistory (paginated) without DB writes.
    """
    import time
    key, sec, *_ = _kraken_creds()
    out = {"ok": True, "creds_present": bool(key and sec), "pages": 0, "pulled": 0, "count": None,
           "first_ts": None, "last_ts": None, "sample_txids": []}
    if not (key and sec):
        out["ok"] = False
        out["error"] = "no_creds"
        return out
    start_ts = int(time.time() - since_hours * 3600)
    ofs = 0
    sample = []
    first_ts = None
    last_ts = None
    total_seen = 0
    total_count = None
    while True:
        try:
            resp = kraken_private("TradesHistory", {"start": start_ts, "ofs": ofs}, key, sec)
            res = resp.get("result") or {}
            trades = res.get("trades") or {}
            total_count = res.get("count", total_count)
            keys = list(trades.keys())
            if not keys:
                break
            for tx in keys[:10]:
                if len(sample) < 20:
                    sample.append(tx)
            for tx in keys:
                t = trades[tx].get("time")
                if t is not None:
                    if first_ts is None or t < first_ts: first_ts = t
                    if last_ts is None or t > last_ts: last_ts = t
            n = len(keys)
            total_seen += n
            out["pages"] += 1
            if total_count is not None and total_seen >= total_count:
                break
            ofs += n
            if out["pages"] > 60:
                break
        except Exception as e:
            out["ok"] = False
            out["error"] = str(e)
            break
    out["pulled"] = total_seen
    out["count"] = total_count
    out["first_ts"] = first_ts
    out["last_ts"] = last_ts
    out["sample_txids"] = sample
    return out

@app.get("/debug/kraken/trades2")
def debug_kraken_trades2(since_hours: int = 200000, mode: str = "cursor", max_pages: int = 300):
    """
    Diagnostic: list how many we can pull by mode.
      - mode=ofs: plain ofs pagination
      - mode=cursor: advance start by last_seen_ts
    """
    import time, math as _math
    key, sec, *_ = _kraken_creds()
    out = {"ok": True, "creds_present": bool(key and sec), "mode": mode,
           "pages": 0, "pulled": 0, "count": None, "first_ts": None, "last_ts": None, "errors": []}
    if not (key and sec):
        out["ok"] = False; out["error"] = "no_creds"; return out

    start_ts = int(time.time() - since_hours * 3600)
    min_delay = float(os.getenv("KRAKEN_MIN_DELAY", "0.35") or 0.35)

    def upd(ts):
        if ts is None: return
        if out["first_ts"] is None or ts < out["first_ts"]: out["first_ts"] = ts
        if out["last_ts"]  is None or ts > out["last_ts"]:  out["last_ts"]  = ts

    if mode == "ofs":
        ofs = 0
        while out["pages"] < max_pages:
            time.sleep(min_delay)
            resp = kraken_private("TradesHistory", {"start": start_ts, "ofs": ofs}, key, sec)
            if isinstance(resp, dict) and resp.get("error"):
                out["errors"].append({"ofs":ofs,"error":resp.get("error")}); break
            res = (resp.get("result") or {}) if isinstance(resp, dict) else {}
            out["count"] = res.get("count", out["count"])
            trades = res.get("trades") or {}
            keys = list(trades.keys())
            if not keys: break
            out["pages"] += 1; out["pulled"] += len(keys)
            for k in keys: upd(trades[k].get("time"))
            ofs += len(keys)
    else:
        cursor = start_ts
        pages = 0
        while pages < max_pages:
            time.sleep(min_delay)
            resp = kraken_private("TradesHistory", {"start": int(_math.floor(cursor)), "ofs": 0}, key, sec)
            if isinstance(resp, dict) and resp.get("error"):
                out["errors"].append({"cursor":cursor,"error":resp.get("error")}); break
            res = (resp.get("result") or {}) if isinstance(resp, dict) else {}
            out["count"] = res.get("count", out["count"])
            trades = res.get("trades") or {}
            keys = list(trades.keys())
            if not keys: break
            # sort by time to move cursor forward
            keys.sort(key=lambda k: trades[k].get("time") or 0)
            pages += 1; out["pages"] += 1; out["pulled"] += len(keys)
            for k in keys: upd(trades[k].get("time"))
            cursor = (float(trades[keys[-1]].get("time") or cursor) + 0.5)

    return out

@app.get("/pnl/summary")
def pnl_summary():
    # Fresh recompute from DB; include 'misc' only if unlabeled rows exist
    try:
        conn = _db() if '_db' in globals() else db() if 'db' in globals() else None
        if conn is None:
            import sqlite3
            import os as _os
            _db_path = _os.getenv("DB_PATH", "data/journal.db")
            conn = sqlite3.connect(_db_path)
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM trades WHERE strategy IS NULL OR strategy=''")
        unlabeled_count = cur.fetchone()[0]

        cur.execute("""
            SELECT symbol,
                   SUM(CASE WHEN side='sell' THEN price*volume ELSE -price*volume END) AS realized,
                   SUM(COALESCE(fee,0)) AS fees
              FROM trades
          GROUP BY symbol
        """)
        sym_rows = cur.fetchall()

        cur.execute("""
            SELECT CASE WHEN strategy IS NULL OR strategy='' THEN 'misc' ELSE strategy END AS strategy,
                   SUM(CASE WHEN side='sell' THEN price*volume ELSE -price*volume END) AS realized,
                   SUM(COALESCE(fee,0)) AS fees
              FROM trades
          GROUP BY CASE WHEN strategy IS NULL OR strategy='' THEN 'misc' ELSE strategy END
        """)
        strat_rows = cur.fetchall()

        if not unlabeled_count:
            strat_rows = [r for r in strat_rows if r[0] != 'misc']

        total_realized = float(sum((r[1] or 0.0) for r in sym_rows))
        total_fees     = float(sum((r[2] or 0.0) for r in sym_rows))
        total_unreal   = 0.0
        total_equity   = total_realized + total_unreal - total_fees

        per_symbol = [{"symbol": str(r[0]),
                       "realized": float(r[1] or 0.0),
                       "unrealized": 0.0,
                       "fees": float(r[2] or 0.0),
                       "equity": float((r[1] or 0.0) - (r[2] or 0.0))} for r in sym_rows]

        per_strategy = [{"strategy": str(r[0]),
                         "realized": float(r[1] or 0.0),
                         "unrealized": 0.0,
                         "fees": float(r[2] or 0.0),
                         "equity": float((r[1] or 0.0) - (r[2] or 0.0))} for r in strat_rows]

        cur.execute("SELECT COUNT(*) FROM trades")
        journal_rows = int(cur.fetchone()[0])

        conn.close()
        return {"total": {"realized": total_realized,
                          "unrealized": total_unreal,
                          "fees": total_fees,
                          "equity": total_equity},
                "per_strategy": per_strategy,
                "per_symbol": per_symbol,
                "ok": True,
                "counts": {"journal_rows": journal_rows}}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@app.get("/pnl/fifo")
def pnl_fifo():
    """Accounting-grade FIFO realized/unrealized/fees by (strategy, symbol)."""
    rows = fetch_rows(limit=1_000_000, offset=0)
    fills = []
    for r in rows:
        side = (r.get("side") or "").lower()
        if side not in ("buy", "sell"):
            continue
        try:
            fills.append({
                "t": float(r.get("ts") or 0),
                "sym": str(r.get("symbol") or "").upper(),
                "side": side,
                "price": float(r.get("price") or 0),
                "vol": float(r.get("volume") or 0),
                "fee": float(r.get("fee") or 0),
                "strategy": (r.get("strategy") or "misc").lower()
            })
        except Exception:
            pass
    fills.sort(key=lambda x: x["t"])
    lots = {}
    stat = {}
    def key(s, y): return f"{s}||{y}"
    for f in fills:
        k = key(f["strategy"], f["sym"])
        lots.setdefault(k, [])
        stat.setdefault(k, {"realized": 0.0, "fees": 0.0, "qty": 0.0})
        if f["side"] == "buy":
            lots[k].append({"q": f["vol"], "px": f["price"]})
            stat[k]["qty"] += f["vol"]
            stat[k]["fees"] += f["fee"]
        else:
            rem = f["vol"]; realized = 0.0
            L = lots[k]
            while rem > 1e-12 and L:
                head = L[0]
                take = min(head["q"], rem)
                realized += (f["price"] - head["px"]) * take
                head["q"] -= take
                rem -= take
                if head["q"] <= 1e-12:
                    L.pop(0)
            stat[k]["qty"] -= f["vol"]
            stat[k]["realized"] += realized
            stat[k]["fees"] += f["fee"]

    symbols = sorted({f["sym"] for f in fills})
    px_map = {}
    for s in symbols:
        try:
            base, quote = (s[:-3], "USD") if s.endswith("USD") else (s, "USD")
            pr = price_ticker(base, quote)
            px_map[s] = float(pr.get("price") or 0)
        except Exception:
            px_map[s] = 0.0

    per_strategy, per_symbol = {}, {}
    Treal = Tunreal = Tfees = 0.0
    for k, S in stat.items():
        strat, sym = k.split("||")
        unreal = 0.0
        mkt = px_map.get(sym) or 0.0
        for lot in lots.get(k, []):
            if mkt > 0:
                unreal += (mkt - lot["px"]) * lot["q"]
        equity = S["realized"] + unreal - S["fees"]

        ps = per_strategy.setdefault(strat, {"strategy": strat, "realized": 0.0, "unrealized": 0.0, "fees": 0.0, "equity": 0.0})
        ps["realized"] += S["realized"]; ps["unrealized"] += unreal; ps["fees"] += S["fees"]; ps["equity"] += equity

        py = per_symbol.setdefault(sym, {"symbol": sym, "realized": 0.0, "unrealized": 0.0, "fees": 0.0, "equity": 0.0})
        py["realized"] += S["realized"]; py["unrealized"] += unreal; py["fees"] += S["fees"]; py["equity"] += equity

        Treal += S["realized"]; Tunreal += unreal; Tfees += S["fees"]

    return {
        "ok": True,
        "total": {"realized": Treal, "unrealized": Tunreal, "fees": Tfees, "equity": Treal + Tunreal - Tfees},
        "per_strategy": sorted(per_strategy.values(), key=lambda r: r["equity"], reverse=True),
        "per_symbol": sorted(per_symbol.values(), key=lambda r: r["equity"], reverse=True)
    }

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
    symbols = os.getenv("SYMBOLS", os.getenv("DEFAULT_SYMBOLS", "BTC/USD,ETH/USD")).strip()
    strats = os.getenv("SCHED_STRATS", "c1,c2,c3,c4,c5,c6").strip()
    timeframe = os.getenv("SCHED_TIMEFRAME", os.getenv("DEFAULT_TIMEFRAME", "5Min")).strip()
    limit = int(os.getenv("SCHED_LIMIT", os.getenv("DEFAULT_LIMIT", "300") or 300))
    notional = float(os.getenv("SCHED_NOTIONAL", os.getenv("DEFAULT_NOTIONAL", "25") or 25))
    guard_enabled = bool(int(os.getenv("TRADING_ENABLED", "1") or 1))
    window = os.getenv("TRADING_WINDOW", os.getenv("WINDOW", "live")).strip()
    return {
        "ok": True,
        "enabled": _SCHED_ENABLED,
        "interval_secs": int(os.getenv("SCHED_SLEEP", "30") or 30),
        "symbols": symbols,
        "strats": strats,
        "timeframe": timeframe,
        "limit": limit,
        "notional": notional,
        "guard_enabled": guard_enabled,
        "window": window,
        "ticks": _SCHED_TICKS,
    }


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


@app.get("/scheduler/last")
def scheduler_last():
    """
    Returns the parameters of the most recent scheduler run (manual or background).
    """
    global _SCHED_LAST
    if not _SCHED_LAST:
        raise HTTPException(status_code=404, detail="No last scheduler payload yet")
    with _SCHED_LAST_LOCK:
        return dict(_SCHED_LAST)

# --------------------------------------------------------------------------------------
# Background scheduler loop
# --------------------------------------------------------------------------------------
def _scheduler_loop():
    """Background loop honoring _SCHED_ENABLED and _SCHED_SLEEP.
    Builds payload from env so Render env toggles work without redeploy.
    """
    global _SCHED_ENABLED
    tick = 0
    global _SCHED_TICKS
    while True:
        try:
            if _SCHED_ENABLED:
                # Build payload from env on each pass
                payload = {
                    "tf": os.getenv("SCHED_TIMEFRAME", os.getenv("DEFAULT_TIMEFRAME","5Min") or "5Min"),
                    "strats": os.getenv("SCHED_STRATS", "c1,c2,c3,c4,c5,c6"),
                    "symbols": os.getenv("SYMBOLS","BTC/USD,ETH/USD"),
                    "limit": int(os.getenv("SCHED_LIMIT", os.getenv("DEFAULT_LIMIT","300") or "300")),
                    "notional": float(os.getenv("SCHED_NOTIONAL", os.getenv("DEFAULT_NOTIONAL","25") or "25")),
                    "dry_run": str(os.getenv("SCHED_DRY","0")).lower() in ("1","true","yes"),
                }
                try:
                    with _SCHED_LAST_LOCK:
                        _SCHED_LAST = dict(payload) | {"ts": datetime.datetime.utcnow().isoformat() + "Z"}
                except Exception as e:
                    log.warning("could not set _SCHED_LAST (loop): %s", e)

                # call the same function our route uses
                _ = scheduler_run(payload)
                tick += 1
                _SCHED_TICKS = tick
                log.info("scheduler tick #%s ok", tick)
        except Exception as e:
            log.exception("scheduler loop error: %s", e)
        # sleep regardless to prevent tight loop if disabled
        time.sleep(_SCHED_SLEEP)
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
    global _SCHED_THREAD
    if _SCHED_ENABLED and _SCHED_THREAD is None:
        _SCHED_THREAD = threading.Thread(target=_scheduler_loop, daemon=True, name="scheduler")
        _SCHED_THREAD.start()
        log.info("Scheduler thread started: sleep=%s enabled=%s", _SCHED_SLEEP, _SCHED_ENABLED)
    else:
        log.info("Scheduler thread not started: enabled=%s existing=%s", _SCHED_ENABLED, bool(_SCHED_THREAD))

# --------------------------------------------------------------------------------------
# Entrypoint (for local runs; Render uses 'python app.py')
# --------------------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "10000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)

@app.post("/journal/enrich")
def journal_enrich(payload: Dict[str, Any] = Body(default=None)):
    dry_run = bool(payload.get("dry_run", False))
    batch_size = int(payload.get("batch_size", 40) or 40)
    max_rows   = int(payload.get("max_rows", 5000) or 5000)
    apply_rules = bool(payload.get("apply_rules", True))
    log.info("enrich start: dry_run=%s batch=%s max_rows=%s apply_rules=%s", dry_run, batch_size, max_rows, apply_rules)

    conn = _db(); cur = conn.cursor()
    try:
        cur.execute("SELECT txid, ts, symbol, raw FROM trades WHERE strategy IS NULL OR TRIM(strategy)='' LIMIT ?", (max_rows,))
        rows = cur.fetchall()
    finally:
        conn.close()

    scanned = len(rows)
    if scanned == 0:
        return {"ok": True, "scanned": 0, "orders_checked": 0, "updated": 0,
                "api_labeled": 0, "rules_labeled": 0, "ambiguous": 0, "missing_order": 0,
                "apply_rules": apply_rules, "batch_size": batch_size, "max_rows": max_rows}

    def _j(x):
        try:
            return json.loads(x) if isinstance(x, str) else (x or {})
        except Exception:
            return {}

    trade_info = {}
    trade_ids = []
    for txid, ts, symbol, raw in rows:
        txid = str(txid)
        trade_ids.append(txid)
        trade_info[txid] = {"ts": ts, "symbol": symbol, "raw": _j(raw)}
    log.info("unlabeled scanned=%d trade_ids=%d", scanned, len(trade_ids))

    key, sec, *_ = _kraken_creds()

    # Stage A: trades -> order ids
    trade_meta = {}
    order_to_trades = {}
    if key and sec and trade_ids:
        for i in range(0, len(trade_ids), batch_size):
            chunk = trade_ids[i:i+batch_size]
            try:
                resp = kraken_private("QueryTrades", {"txid": ",".join(chunk)}, key, sec)
                res = (resp.get("result") or {}) if isinstance(resp, dict) else {}
                if isinstance(res, dict):
                    trade_meta.update(res)
            except Exception as e:
                log.warning("QueryTrades failed for %d txids: %s", len(chunk), e)
        for txid in trade_ids:
            meta = trade_meta.get(txid, {})
            otx = meta.get("ordertxid")
            if isinstance(otx, list):
                otx = otx[0] if otx else None
            if otx:
                order_to_trades.setdefault(str(otx), []).append(txid)
    else:
        log.warning("journal_enrich: Kraken creds missing or no trade_ids; skipping API enrichment")

    all_order_ids = list(order_to_trades.keys())
    log.info("order_to_trades size=%d (unique orders)", len(all_order_ids))

    # Stage B: orders -> strategy
    orders_meta = {}
    if key and sec and all_order_ids:
        for i in range(0, len(all_order_ids), batch_size):
            chunk = all_order_ids[i:i+batch_size]
            try:
                resp = kraken_private("QueryOrdersInfo", {"txid": ",".join(chunk)}, key, sec)
                res = (resp.get("result") or {}) if isinstance(resp, dict) else {}
                if isinstance(res, dict):
                    orders_meta.update(res)
            except Exception as e:
                log.warning("QueryOrdersInfo failed for %d orders: %s", len(chunk), e)
    log.info("orders_meta fetched=%d", len(orders_meta))

    import re as _re, datetime as _dt, pytz, json as _json
    def infer_from_order(o):
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

    # Load rules
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

    for oid, txs in order_to_trades.items():
        strat = infer_from_order(orders_meta.get(oid) or {})
        if strat:
            for tx in txs: to_update[tx] = strat
            api_labeled += len(txs)
        else:
            missing_order += len(txs)

    if apply_rules and (whitelist or windows):
        all_strats = list(set(list(whitelist.keys()) + list(windows.keys())))
        for txid, info in trade_info.items():
            if txid in to_update: continue
            sym, ts = info.get("symbol"), info.get("ts")
            if not sym or ts is None: continue
            cands = [s for s in all_strats if allowed_by_rules(s, sym, ts)]
            if len(cands) == 1:
                to_update[txid] = cands[0]; rules_labeled += 1
            else:
                ambiguous += 1

    if dry_run:
        sample = dict(list(to_update.items())[:10])
        return {"ok": True, "dry_run": True, "scanned": scanned, "orders_checked": len(all_order_ids),
                "to_update_count": len(to_update), "sample_updates": sample}

    updated = 0
    if to_update:
        conn = _db(); cur = conn.cursor()
        for txid, strat in to_update.items():
            try:
                cur.execute("UPDATE trades SET strategy=? WHERE txid=?", (str(strat), str(txid)))
                updated += cur.rowcount
            except Exception as e:
                log.warning("update failed txid=%s: %s", txid, e)
        conn.commit(); conn.close()

    return {
        "ok": True,
        "scanned": scanned,
        "orders_checked": len(all_order_ids),
        "updated": updated,
        "api_labeled": api_labeled,
        "rules_labeled": rules_labeled,
        "ambiguous": ambiguous,
        "missing_order": missing_order,
        "apply_rules": apply_rules,
        "batch_size": batch_size,
        "max_rows": max_rows,
    }

@app.get("/debug/env")
def debug_env():
    keys = [
        "APP_VERSION","SCHED_DRY","SCHED_ON","SCHED_TIMEFRAME","SCHED_LIMIT","SCHED_NOTIONAL",
        "BROKER","TRADING_ENABLED","TZ","PORT","DEFAULT_LIMIT","DEFAULT_NOTIONAL","DEFAULT_TIMEFRAME"
    ]
    env = {k: os.getenv(k) for k in keys}
    return {"ok": True, "env": env}

# ---- Scan all (no orders) ------------------------------------------------------------
@app.get("/scan/all")
def scan_all(tf: str = "5Min", symbols: str = "BTC/USD,ETH/USD", strats: str = "c1,c2,c3",
             limit: int = 300, notional: float = 25.0):
    """
    Preload bars for requested symbols/timeframe(s), run each strategy scan, and return intents
    without placing orders. Useful for diagnosing "why no action?".
    """
    try:
        import br_router as br
    except Exception as e:
        return {"ok": False, "error": f"import br_router failed: {e}"}
    try:
        from book import StrategyBook, ScanRequest
    except Exception as e:
        return {"ok": False, "error": f"import book failed: {e}"}

    syms = [s.strip().upper() for s in (symbols or "").split(",") if s.strip()]
    mods = [m.strip().lower() for m in (strats or "").split(",") if m.strip()]
    tf = str(tf or "5Min")
    limit = int(limit or 300)
    notional = float(notional or 25.0)

    # Env-tunable book params (defaults)
    topk = int(os.getenv("BOOK_TOPK", "2") or 2)
    min_score = float(os.getenv("BOOK_MIN_SCORE", "0.07") or 0.07)
    atr_stop_mult = float(os.getenv("ATR_STOP_MULT", "1.0") or 1.0)

    # Preload bars: 1Min and main tf
    bars_cache = {}
    for sym in syms:
        try:
            one = br.get_bars(sym, timeframe="1Min", limit=limit)
            five = br.get_bars(sym, timeframe=tf, limit=limit)
            def series(bars, key):
                return [row.get(key) for row in (bars or [])]
            bars_cache[sym] = {
                "one":  {"close": series(one,"c"), "high": series(one,"h"), "low": series(one,"l")},
                "five": {"close": series(five,"c"), "high": series(five,"h"), "low": series(five,"l")},
            }
        except Exception as e:
            bars_cache[sym] = {"error": str(e)}

    out = {"ok": True, "tf": tf, "symbols": syms, "strats": mods, "results": {}}
    for mod in mods:
        req = ScanRequest(strat=mod, timeframe=tf, limit=limit, topk=topk, min_score=min_score, notional=notional)
        book = StrategyBook(topk=topk, min_score=min_score, atr_stop_mult=atr_stop_mult)
        try:
            res = book.scan(req, bars_cache)
            out["results"][mod] = [r.__dict__ for r in res]
        except Exception as e:
            out["results"][mod] = [{"error": str(e)}]
    return out


@app.post("/scheduler/run")
def scheduler_run(payload: Dict[str, Any] = Body(default=None)):
    """
    Real scheduler: fetch bars once, run StrategyBook on requested strats/symbols,
    and (optionally) place market orders via br_router.market_notional.
    """
    payload = payload or {}
    try:
        import br_router as br
    except Exception as e:
        msg = f"import br_router failed: {e}"
        log.warning(msg)
        return {"ok": False, "message": msg, "actions": []}
    try:
        from book import StrategyBook, ScanRequest
    except Exception as e:
        msg = f"import book failed: {e}"
        log.warning(msg)
        return {"ok": False, "message": msg, "actions": []}

    # Resolve inputs
    def _resolve_dry(_p):
        _env = str(os.getenv("SCHED_DRY", "1")).lower() in ("1","true","yes")
        try:
            if _p is None: return _env
            if isinstance(_p, dict):
                v = _p.get("dry_run", None)
            else:
                v = None
            return _env if v is None else bool(v)
        except Exception:
            return _env

    dry = _resolve_dry(payload)
    tf = str(payload.get("tf", os.getenv("SCHED_TIMEFRAME", "5Min")))
    strats = [s.strip().lower() for s in str(payload.get("strats", os.getenv("SCHED_STRATS","c1,c2,c3"))).split(",") if s.strip()]
    symbols_csv = str(payload.get("symbols", os.getenv("SYMBOLS","BTC/USD,ETH/USD")))
    syms = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
    limit = int(payload.get("limit", int(os.getenv("SCHED_LIMIT", "300") or 300)))
    notional = float(payload.get("notional", float(os.getenv("SCHED_NOTIONAL", "25") or 25)))
    msg = f"Scheduler pass: strats={','.join(strats)} tf={tf} limit={limit} notional={notional} dry={dry} symbols={symbols_csv}"
    log.info(msg)

    # Record last-run
    try:
        with _SCHED_LAST_LOCK:
            _SCHED_LAST.clear()
            _SCHED_LAST.update({
                "tf": tf, "strats": ",".join(strats), "symbols": symbols_csv,
                "limit": limit, "notional": notional, "dry_run": dry,
                "ts": dt.datetime.utcnow().isoformat() + "Z",
            })
    except Exception as e:
        log.warning("could not set _SCHED_LAST: %s", e)

    # Env-tunable book params
    topk = int(os.getenv("BOOK_TOPK", "2") or 2)
    min_score = float(os.getenv("BOOK_MIN_SCORE", "0.07") or 0.07)
    atr_stop_mult = float(os.getenv("ATR_STOP_MULT", "1.0") or 1.0)

    # Fee/edge guard envs
    taker_fee_bps = float(os.getenv("KRAKEN_TAKER_FEE_BPS", "26") or 26.0)  # 0.26% typical
    fee_multiple = float(os.getenv("EDGE_MULTIPLE_VS_FEE", "2.0") or 2.0)   # require edge >= 2x fee
    min_notional = float(os.getenv("MIN_ORDER_NOTIONAL_USD", "10") or 10.0) # skip under min

    # Preload bars once
    contexts = {}
    for sym in syms:
        try:
            one = br.get_bars(sym, timeframe="1Min", limit=limit)
            five = br.get_bars(sym, timeframe=tf, limit=limit)
            def series(bars, key):
                return [row.get(key) for row in (bars or [])]
            contexts[sym] = {
                "one":  {"close": series(one,"c"), "high": series(one,"h"), "low": series(one,"l")},
                "five": {"close": series(five,"c"), "high": series(five,"h"), "low": series(five,"l")},
            }
        except Exception as e:
            contexts[sym] = None
            log.warning("bars preload error for %s: %s", sym, e)

    actions = []
    telemetry = []

    for strat in strats:
        book = StrategyBook(topk=topk, min_score=min_score, atr_stop_mult=atr_stop_mult)
        req = ScanRequest(strat=strat, timeframe=tf, limit=limit, topk=topk, min_score=min_score, notional=notional)
        try:
            res = book.scan(req, contexts)
        except Exception as e:
            telemetry.append({"strategy": strat, "error": str(e)})
            continue

        for r in res:
            edge_pct = float(r.atr_pct or 0.0) * 100.0  # rank pct 0..1 → 0..100 proxy
            fee_pct = taker_fee_bps / 10000.0
            guard_ok = True
            guard_reason = None
            if r.action not in ("buy","sell"):
                guard_ok = False; guard_reason = r.reason or "flat"
            elif r.notional < max(min_notional, 0.0):
                guard_ok = False; guard_reason = f"notional_below_min:{r.notional:.2f}"
            elif edge_pct < (fee_multiple * fee_pct * 100.0):
                guard_ok = False; guard_reason = f"edge_vs_fee_low:{edge_pct:.3f}pct"

            telemetry.append({
                "strategy": strat, "symbol": r.symbol, "raw_action": r.action, "reason": r.reason,
                "score": r.score, "atr": r.atr, "atr_pct": r.atr_pct,
                "qty": r.qty, "notional": r.notional,
                "guard_ok": guard_ok, "guard_reason": guard_reason
            })

            if guard_ok and r.action in ("buy","sell"):
                act = {
                    "symbol": r.symbol, "side": r.action, "strategy": strat,
                    "notional": float(r.notional if r.notional > 0 else notional),
                }
                if dry:
                    act["status"] = "dry_ok"
                else:
                    try:
                        resp = br.market_notional(r.symbol, r.action, act["notional"], strategy=strat)
                        act["status"] = "live_ok"
                        act["broker"] = resp
                    except Exception as e:
                        act["status"] = "live_err"
                        act["error"] = str(e)
                actions.append(act)

    return {"ok": True, "message": msg, "actions": actions, "telemetry": telemetry}

# ==== BEGIN PATCH v1.0.0: P&L aggregation endpoints =============================================
from typing import Optional, Any, List, Dict, Tuple
import sqlite3 as _sqlite3
import os as _os
from datetime import datetime as _dt

_JOURNAL_DB_PATH = "/var/data/journal.db"

def _pnl__parse_bool(v: Optional[str], default: bool=True) -> bool:
    if v is None:
        return default
    s = str(v).strip().lower()
    return s in ("1","true","t","yes","y","on")

def _pnl__parse_ts(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        s2 = s.replace("Z","").strip()
        # Accept 'YYYY-MM-DD' or ISO with time
        if len(s2) == 10 and s2.count("-")==2:
            s2 = s2 + "T00:00:00"
        _dt.fromisoformat(s2)  # validate
        return s2
    except Exception:
        return None

def _pnl__connect() -> _sqlite3.Connection:
    if not _os.path.exists(_JOURNAL_DB_PATH):
        raise FileNotFoundError(f"journal db not found at: {_JOURNAL_DB_PATH}")
    con = _sqlite3.connect(_JOURNAL_DB_PATH)
    con.row_factory = _sqlite3.Row
    return con

def _pnl__detect_table(con: _sqlite3.Connection) -> str:
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    names = [r[0] for r in cur.fetchall()]
    # Preferred names first
    for nm in ("journal","trades","fills","orders","executions"):
        if nm in names:
            return nm
    # Fallback: first table with strategy+symbol+timestamp
    for nm in names:
        try:
            cols = {r[1] for r in con.execute(f"PRAGMA table_info({nm})").fetchall()}
            if {"timestamp","strategy","symbol"}.issubset(cols):
                return nm
        except Exception:
            pass
    return names[0] if names else ""

def _pnl__columns(con: _sqlite3.Connection, table: str) -> List[str]:
    return [r[1] for r in con.execute(f"PRAGMA table_info({table})")]

def _pnl__where(start_iso: Optional[str], end_iso: Optional[str]) -> Tuple[str, List[Any]]:
    w, p = [], []
    if start_iso:
        w.append("timestamp >= ?")
        p.append(start_iso)
    if end_iso:
        w.append("timestamp <= ?")
        p.append(end_iso)
    return ((" WHERE " + " AND ".join(w)) if w else ""), p

def _pnl__build_sql(table: str, group_fields: List[str], have_cols: List[str], realized_only: bool) -> str:
    realized = "COALESCE(SUM(realized_pnl),0.0)" if "realized_pnl" in have_cols else "0.0"
    fees     = "COALESCE(SUM(fee),0.0)" if "fee" in have_cols else "0.0"
    unreal   = "COALESCE(SUM(unrealized_pnl),0.0)" if (not realized_only and "unrealized_pnl" in have_cols) else "0.0"
    equity   = f"({realized} + {unreal} - {fees})"
    sel_grp  = ", ".join(group_fields) if group_fields else "'_all' AS group_key"
    grp_by   = (" GROUP BY " + ", ".join(group_fields)) if group_fields else ""
    return f"""
        SELECT
            {sel_grp},
            {realized} AS realized_pnl,
            {unreal} AS unrealized_pnl,
            {fees} AS fees,
            {equity} AS equity,
            COUNT(*) AS trades
        FROM {table}{{WHERE}}
        {grp_by}
        ORDER BY equity DESC
    """

def _pnl__agg(group_fields: List[str], start: Optional[str], end: Optional[str], realized_only: bool):
    s = _pnl__parse_ts(start)
    e = _pnl__parse_ts(end)
    with _pnl__connect() as con:
        table = _pnl__detect_table(con)
        if not table:
            return {"ok": False, "error": "no tables found in journal"}
        have = _pnl__columns(con, table)
        where, params = _pnl__where(s, e)
        sql = _pnl__build_sql(table, group_fields, have, realized_only).replace("{WHERE}", where)
        rows = [dict(r) for r in con.execute(sql, params).fetchall()]
        return {"ok": True, "table": table, "start": s, "end": e, "realized_only": realized_only, "count": len(rows), "rows": rows}

@app.get("/pnl/by_strategy")
def pnl_by_strategy(start: Optional[str] = None, end: Optional[str] = None,
                    realized_only: Optional[str] = "true", tz: Optional[str] = "UTC"):
    try:
        return _pnl__agg(["strategy"], start, end, _pnl__parse_bool(realized_only, True))
    except Exception as e:
        return {"ok": False, "error": f"/pnl/by_strategy failed: {e.__class__.__name__}: {e}"}

@app.get("/pnl/by_symbol")
def pnl_by_symbol(start: Optional[str] = None, end: Optional[str] = None,
                  realized_only: Optional[str] = "true", tz: Optional[str] = "UTC"):
    try:
        return _pnl__agg(["symbol"], start, end, _pnl__parse_bool(realized_only, True))
    except Exception as e:
        return {"ok": False, "error": f"/pnl/by_symbol failed: {e.__class__.__name__}: {e}"}

@app.get("/pnl/combined")
def pnl_combined(start: Optional[str] = None, end: Optional[str] = None,
                 realized_only: Optional[str] = "true", tz: Optional[str] = "UTC"):
    try:
        return _pnl__agg(["strategy","symbol"], start, end, _pnl__parse_bool(realized_only, True))
    except Exception as e:
        return {"ok": False, "error": f"/pnl/combined failed: {e.__class__.__name__}: {e}"}
# ==== END PATCH v1.0.0 ===========================================================================
