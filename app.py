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
import threading
import broker_kraken
import requests
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from symbol_map import KRAKEN_PAIR_MAP, to_kraken
from position_engine import PositionEngine, Fill
from position_manager import load_net_positions, Position
from risk_engine import RiskEngine
from strategy_api import PositionSnapshot
from typing import Dict, Any, List
from strategy_api import PositionSnapshot
from scheduler_core import SchedulerConfig, SchedulerResult, StrategyBook, ScanRequest, ScanResult,run_scheduler_once
from risk_engine import RiskEngine
from fastapi import Body, FastAPI, HTTPException, Query



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
# Risk config loader (policy_config/risk.json)
# --------------------------------------------------------------------------------------
def load_risk_config() -> dict:
    """
    Load structured risk config from policy_config/risk.json.
    Returns {} on any error so callers can safely default.
    """
    try:
        cfg_dir = os.getenv("POLICY_CFG_DIR", "policy_config")
        path = Path(cfg_dir) / "risk.json"
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.warning("load_risk_config failed: %s", e)
        return {}

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
    sym_app = f"{base.upper()}/{quote.upper()}"        # e.g. AVAX/USD
    alt = to_kraken(sym_app)                           # e.g. XBTUSD, AVAXUSD, etc.
    try:
        data = kraken_public_ticker(alt)
        err = _kraken_error_str(data)
        if err:
            raise HTTPException(status_code=502, detail=f"Kraken error: {err}")
        result = data.get("result") or {}
        if not result:
            raise HTTPException(status_code=502, detail="No ticker data")
        k = next(iter(result.keys()))                  # the only key is the pair
        last_trade = result[k]["c"][0]                 # 'c' -> last trade price [price, lot]
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

@app.post("/debug/strategy_scan")
def debug_strategy_scan(payload: Dict[str, Any] = Body(default=None)):
    """
    Explain why a given strategy did or did not want to trade a symbol
    on the most recent bar.

    Returns:
      - position snapshot (qty, avg_price)
      - raw scan fields (action, reason, score, atr_pct, notional, selected)
      - entry gating flags (is_flat, scan_selected, etc.)
    """
    payload = payload or {}

    strat = str(payload.get("strategy") or "").strip().lower()
    symbol = str(payload.get("symbol") or "").strip().upper()

    if not strat or not symbol:
        return {
            "ok": False,
            "error": "Both 'strategy' and 'symbol' are required, e.g. c3 / ADA/USD",
        }

    tf = str(payload.get("tf") or os.getenv("TF", "5Min"))
    limit = int(payload.get("limit", int(os.getenv("SCHED_LIMIT", "300") or 300)))
    notional = float(payload.get("notional", float(os.getenv("SCHED_NOTIONAL", "25") or 25.0)))

    # Load positions + risk config
    positions = _load_open_positions_from_trades(use_strategy_col=True)
    risk_cfg = load_risk_config() or {}

    # Preload bars using the same format as the main scheduler
    try:
        import br_router as br  # type: ignore[import]
    except Exception as e:
        return {
            "ok": False,
            "error": f"failed to import br_router: {e}",
        }

    def _safe_series(bars, key: str):
        vals = []
        if isinstance(bars, list):
            for row in bars:
                if isinstance(row, dict) and key in row:
                    vals.append(row[key])
        return vals

    contexts: Dict[str, Any] = {}
    telemetry: List[Dict[str, Any]] = []

    try:
        one = br.get_bars(symbol, timeframe="1Min", limit=limit)
        five = br.get_bars(symbol, timeframe=tf, limit=limit)

        if not one or not five:
            contexts[symbol] = None
            telemetry.append(
                {
                    "symbol": symbol,
                    "stage": "preload_bars",
                    "ok": False,
                    "reason": "no_bars",
                }
            )
        else:
            # IMPORTANT: match the real scheduler preload:
            # broker returns keys like "c","h","l" (close, high, low)
            contexts[symbol] = {
                "one": {
                    "close": _safe_series(one, "c"),
                    "high":  _safe_series(one, "h"),
                    "low":   _safe_series(one, "l"),
                },
                "five": {
                    "close": _safe_series(five, "c"),
                    "high":  _safe_series(five, "h"),
                    "low":   _safe_series(five, "l"),
                },
            }
    except Exception as e:
        contexts[symbol] = None
        telemetry.append(
            {
                "symbol": symbol,
                "stage": "preload_bars",
                "ok": False,
                "error": f"{e.__class__.__name__}: {e}",
            }
        )

    # Build a minimal SchedulerConfig (we only care about this one symbol/strat)
    cfg = SchedulerConfig(
        now=dt.datetime.utcnow(),
        timeframe=tf,
        limit=limit,
        symbols=[symbol],
        strats=[strat],
        notional=notional,
        positions=positions,
        contexts=contexts,
        risk_cfg=risk_cfg,
    )

    # Run the book's scan for this strategy
    book = StrategyBook()
    sreq = ScanRequest(
        strat=strat,
        timeframe=tf,
        limit=limit,
        topk=book.topk,
        min_score=book.min_score,
        notional=notional,
    )

    scans: List[ScanResult] = book.scan(sreq, cfg.contexts) or []

    # Get the scan result for this symbol (if any)
    scan = None
    for r in scans:
        if isinstance(r, ScanResult) and getattr(r, "symbol", None) == symbol:
            scan = r
            break

    # Position snapshot for this strategy/symbol
    pos_obj = positions.get((symbol, strat))
    qty = float(getattr(pos_obj, "qty", 0.0) or 0.0) if pos_obj is not None else 0.0
    avg_price = getattr(pos_obj, "avg_price", None) if pos_obj is not None else None

    out: Dict[str, Any] = {
        "ok": True,
        "strategy": strat,
        "symbol": symbol,
        "tf": tf,
        "limit": limit,
        "notional": notional,
        "position": {
            "qty": qty,
            "avg_price": avg_price,
        },
        "telemetry": telemetry,
    }

    if scan is None:
        out["scan"] = {"reason": "no_scan_result"}
        out["entry_gate"] = {
            "is_flat": abs(qty) < 1e-10,
            "scan_selected": None,
            "scan_action": None,
            "scan_notional_positive": None,
            "would_emit_entry": False,
        }
        return out

    # Raw scan fields
    out["scan"] = {
        "action": scan.action,
        "reason": scan.reason,
        "score": float(getattr(scan, "score", 0.0) or 0.0),
        "atr_pct": float(getattr(scan, "atr_pct", 0.0) or 0.0),
        "notional": float(getattr(scan, "notional", 0.0) or 0.0),
        "selected": bool(getattr(scan, "selected", False)),
    }

    # Entry gating logic (mirrors entry_signal checks, but simplified)
    is_flat = abs(qty) < 1e-10
    scan_selected = bool(getattr(scan, "selected", False))
    scan_action = getattr(scan, "action", None)
    scan_notional_positive = float(getattr(scan, "notional", 0.0) or 0.0) > 0.0

    would_emit_entry = (
        is_flat
        and scan_selected
        and scan_action in ("buy", "sell")
        and scan_notional_positive
    )

    out["entry_gate"] = {
        "is_flat": is_flat,
        "scan_selected": scan_selected,
        "scan_action": scan_action,
        "scan_notional_positive": scan_notional_positive,
        "would_emit_entry": would_emit_entry,
    }

    return out

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

def _compute_realized_pnl(
    con,
    symbol: Optional[str] = None,
    strategy: Optional[str] = None,
    since_ts: Optional[int] = None,
    until_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Compute a very simple realized PnL summary from the trades table.

    Assumes 'trades' has:
      - symbol (TEXT)
      - strategy (TEXT, may be NULL)
      - side ('buy'/'sell')
      - price (REAL)
      - volume (REAL)
      - fee (REAL)
      - ts (INTEGER, unix seconds)

    This uses a naive running-average method for entries and exits
    (not strict FIFO) but is consistent and easy to reason about.
    """
    cur = con.cursor()
    clauses = []
    params: List[Any] = []

    if symbol:
        clauses.append("symbol = ?")
        params.append(symbol)
    if strategy:
        clauses.append("strategy = ?")
        params.append(strategy)
    if since_ts is not None:
        clauses.append("ts >= ?")
        params.append(since_ts)
    if until_ts is not None:
        clauses.append("ts <= ?")
        params.append(until_ts)

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    cur.execute(
        f"""
        SELECT symbol, strategy, side, price, volume, fee, ts
        FROM trades
        {where_sql}
        ORDER BY ts ASC
        """,
        tuple(params),
    )

    # simple running-average model per (symbol,strategy)
    state: Dict[Tuple[str, str], Dict[str, float]] = {}
    realized_total = 0.0
    fees_total = 0.0
    trade_count = 0

    for sym, strat, side, price, vol, fee, ts in cur.fetchall():
        strat = strat or "misc"
        key = (sym, strat)
        price = float(price or 0.0)
        vol = float(vol or 0.0)
        fee = float(fee or 0.0)

        s = state.get(key) or {"qty": 0.0, "avg_price": 0.0}
        qty = s["qty"]
        avg_price = s["avg_price"]

        if side == "buy":
            # if currently short, this reduces/flip the short
            if qty <= 0:
                # realized pnl if you're closing short
                closed_qty = min(-qty, vol) if qty < 0 else 0.0
                if closed_qty > 0:
                    # short: entry at avg_price, exit at price
                    pnl = (avg_price - price) * closed_qty
                    realized_total += pnl
                # update net position
                new_qty = qty + vol
                if new_qty > 0:
                    # now net long; compute new avg_price for long portion
                    # combine residual short pnl implicitly via running sum
                    new_cost = price * max(new_qty - max(-qty, 0.0), 0.0)
                    avg_price = new_cost / new_qty if new_qty != 0 else avg_price
                qty = new_qty
            else:
                # add to long
                new_qty = qty + vol
                new_cost = qty * avg_price + vol * price
                avg_price = new_cost / new_qty if new_qty != 0 else avg_price
                qty = new_qty

        elif side == "sell":
            if qty >= 0:
                # closing/flip long
                closed_qty = min(qty, vol) if qty > 0 else 0.0
                if closed_qty > 0:
                    pnl = (price - avg_price) * closed_qty
                    realized_total += pnl
                new_qty = qty - vol
                if new_qty < 0:
                    # now net short; avg_price for short is entry price
                    avg_price = price
                qty = new_qty
            else:
                # add to short
                new_qty = qty - vol
                new_cost = (-qty) * avg_price + vol * price
                avg_price = new_cost / (-new_qty) if new_qty != 0 else avg_price
                qty = new_qty

        fees_total += fee
        trade_count += 1

        s["qty"] = qty
        s["avg_price"] = avg_price
        state[key] = s

    net_realized = realized_total - fees_total

    return {
        "realized_gross": realized_total,
        "fees": fees_total,
        "realized_net": net_realized,
        "trade_count": trade_count,
    }

@app.get("/pnl/realized_summary")
def pnl_realized_summary(
    symbol: Optional[str] = Query(None),
    strategy: Optional[str] = Query(None),
    since_ts: Optional[int] = Query(None),
    until_ts: Optional[int] = Query(None),
):
    con = _db()
    try:
        summary = _compute_realized_pnl(
            con,
            symbol=symbol,
            strategy=strategy,
            since_ts=since_ts,
            until_ts=until_ts,
        )
    finally:
        con.close()
    return summary


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
    
@app.get("/debug/kraken/positions")
def debug_kraken_positions(
    use_strategy: bool = True,
    include_legacy: bool = False,
):
    """
    Cross-check live Kraken balances vs journal-derived positions.

    - Kraken side: broker_kraken.positions() -> [{"asset": "AVAX", "qty": 1.23}, ...]
    - Journal side: load_net_positions(...) from trades table.
      * use_strategy=True  -> uses (symbol,strategy), then aggregates per asset
      * include_legacy=False -> ignores strategy='legacy' (old history after reset)

    Returns:
      {
        "ok": true,
        "kraken": [...],
        "journal_by_asset": [...],
        "diff": [...]
      }
    """
    try:
        # --- Kraken side: live balances -----------------------------------
        try:
            kraken_raw = broker_kraken.positions()  # [{"asset": "AVAX","qty": 1.23}, ...] or [{"error": "..."}]
        except Exception as e:
            kraken_raw = [{"error": f"positions_error:{e}"}]

        kraken_map = {}
        kraken_list = []
        for row in kraken_raw:
            asset = str(row.get("asset", "")).upper()
            if not asset:
                # pass through errors or unknown shapes
                kraken_list.append(row)
                continue
            try:
                qty = float(row.get("qty", 0.0) or 0.0)
            except Exception:
                qty = 0.0
            if qty == 0.0:
                continue
            kraken_map[asset] = kraken_map.get(asset, 0.0) + qty
            kraken_list.append({"asset": asset, "qty": qty})

        # --- Journal side: load positions from trades ---------------------
        con = _db()
        try:
            # PnL helper detects the correct table; fallback to "trades"
            try:
                table = _pnl__detect_table(con)
            except Exception:
                table = "trades"

            pos_dict = load_net_positions(
                con,
                table=table,
                use_strategy_col=bool(use_strategy),
            )
        finally:
            con.close()

        journal_agg: Dict[str, float] = {}
        journal_details: Dict[str, list] = {}

        for (symbol, strategy), pos in pos_dict.items():
            if not include_legacy and str(strategy).strip().lower() == "legacy":
                # Ignore legacy history when include_legacy=False
                continue

            sym = str(symbol or "").upper()
            if "/" in sym:
                asset = sym.split("/", 1)[0].strip()
            else:
                # Fallback: treat full symbol as asset (e.g. "AVAXUSD" -> "AVAXUSD")
                asset = sym

            qty = float(pos.qty or 0.0)
            if abs(qty) < 1e-12:
                continue

            journal_agg[asset] = journal_agg.get(asset, 0.0) + qty
            journal_details.setdefault(asset, []).append({
                "symbol": sym,
                "strategy": strategy,
                "qty": qty,
                "side": "long" if qty > 0 else ("short" if qty < 0 else "flat"),
            })

        journal_list = []
        for asset, total_qty in journal_agg.items():
            journal_list.append({
                "asset": asset,
                "qty": total_qty,
                "positions": journal_details.get(asset, []),
            })

        # --- Diff: kraken_qty - journal_qty --------------------------------
        all_assets = set(kraken_map.keys()) | set(journal_agg.keys())
        diff_list = []
        for asset in sorted(all_assets):
            kqty = float(kraken_map.get(asset, 0.0))
            jq = float(journal_agg.get(asset, 0.0))
            diff_list.append({
                "asset": asset,
                "kraken_qty": kqty,
                "journal_qty": jq,
                "delta": kqty - jq,
            })

        return {
            "ok": True,
            "use_strategy": bool(use_strategy),
            "include_legacy": bool(include_legacy),
            "kraken": kraken_list,
            "journal_by_asset": sorted(journal_list, key=lambda x: x["asset"]),
            "diff": diff_list,
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}
    
@app.get("/debug/global_policy")
def debug_global_policy():
    """
    Shows the active global policy configuration as the scheduler
    actually sees it — after loading risk.json and environment variables.

    Includes:
    - daily flatten logic
    - per-symbol risk caps
    - profit-lock parameters
    - loss-zone parameters
    - timezone + computed current local time
    """
    try:
        # Load risk config directly from policy_config/risk.json
        _risk_cfg = load_risk_config()

        daily_flat = (_risk_cfg.get("daily_flatten") or {})
        risk_caps = (_risk_cfg.get("risk_caps") or {})
        profit_lock = (_risk_cfg.get("profit_lock") or {})
        loss_zone = (_risk_cfg.get("loss_zone") or {})

        # Compute local time using TZ
        try:
            from zoneinfo import ZoneInfo
            _tzname = os.getenv("TZ", "UTC")
            _now_local = dt.datetime.now(ZoneInfo(_tzname))
        except Exception:
            _tzname = "UTC"
            _now_local = dt.datetime.utcnow()

        # Compute flatten_mode flag just like scheduler_run
        flatten_mode = False
        try:
            if daily_flat.get("enabled"):
                fh = int(daily_flat.get("flatten_hour_local", 23))
                fm = int(daily_flat.get("flatten_minute_local", 0))
                if (_now_local.hour, _now_local.minute) >= (fh, fm):
                    flatten_mode = True
        except Exception:
            flatten_mode = False

        return {
            "ok": True,
            "timezone": _tzname,
            "now_local": _now_local.isoformat(),
            "daily_flatten": daily_flat,
            "flatten_mode_active_now": flatten_mode,
            "risk_caps": risk_caps,
            "profit_lock": profit_lock,
            "loss_zone": loss_zone,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
    

@app.get("/debug/positions")
def debug_positions():
    """
    Show current open positions as seen by the system, including:

    - qty / avg_price from trades table
    - last_price (via _last_price_safe)
    - unrealized_pct (via RiskEngine.compute_unrealized_pct)
    - per-symbol caps from risk.json (max_notional / max_units)

    This is the single source of truth for exposures + unrealized P&L.
    """
    positions = _load_open_positions_from_trades(use_strategy_col=True)
    risk_cfg = load_risk_config() or {}
    risk_engine = RiskEngine(risk_cfg)

    out = []

    for (symbol, strategy), pm_pos in positions.items():
        snap = PositionSnapshot(
            symbol=symbol,
            strategy=strategy,
            qty=float(getattr(pm_pos, "qty", 0.0) or 0.0),
            avg_price=getattr(pm_pos, "avg_price", None),
            unrealized_pct=None,
        )

        last_px = _last_price_safe(symbol)
        try:
            unrealized_pct = risk_engine.compute_unrealized_pct(
                snap,
                last_price_fn=_last_price_safe,
            )
        except Exception:
            unrealized_pct = None

        snap.unrealized_pct = unrealized_pct

        # symbol caps (already time-of-day adjusted)
        max_notional, max_units = risk_engine.symbol_caps(symbol)

        out.append(
            {
                "symbol": symbol,
                "strategy": strategy,
                "qty": snap.qty,
                "avg_price": snap.avg_price,
                "last_price": last_px,
                "unrealized_pct": unrealized_pct,
                "max_notional_cap": max_notional,
                "max_units_cap": max_units,
            }
        )

    return {
        "positions": out,
    }

@app.get("/debug/trades_sample")
def debug_trades_sample(
    symbol: str = Query(None, description="Optional symbol filter, e.g. ADA/USD"),
    limit: int = Query(10, ge=1, le=200, description="Max rows to return"),
):
    """
    Show a small sample of rows from the trades table so we can inspect
    price / volume / cost / fee and spot scaling issues.
    """
    con = _db()
    try:
        cur = con.cursor()
        if symbol:
            cur.execute(
                """
                SELECT txid, ts, pair, symbol, side, price, volume, cost, fee
                FROM trades
                WHERE symbol = ?
                ORDER BY ts DESC
                LIMIT ?
                """,
                (symbol, limit),
            )
        else:
            cur.execute(
                """
                SELECT txid, ts, pair, symbol, side, price, volume, cost, fee
                FROM trades
                ORDER BY ts DESC
                LIMIT ?
                """,
                (limit,),
            )
        rows = cur.fetchall()
    finally:
        con.close()

    out = []
    for r in rows:
        txid, ts, pair, sym, side, price, volume, cost, fee = r
        out.append(
            {
                "txid": txid,
                "ts": ts,
                "pair": pair,
                "symbol": sym,
                "side": side,
                "price": float(price) if price is not None else None,
                "volume": float(volume) if volume is not None else None,
                "cost": float(cost) if cost is not None else None,
                "fee": float(fee) if fee is not None else None,
            }
        )

    return {"rows": out}
        
# --------------------------------------------------------------------------------------
# Scheduler globals
# --------------------------------------------------------------------------------------

_SCHED_ENABLED = bool(int(os.getenv("SCHED_ON", os.getenv("SCHED_ENABLED", "1") or "1")))
_SCHED_SLEEP = float(os.getenv("SCHED_SLEEP", "30") or 30)
_SCHED_TICKS = 0
_SCHED_LAST: Dict[str, Any] = {}
_SCHED_LAST_LOCK = threading.Lock()
_SCHED_THREAD: Optional[threading.Thread] = None


# --------------------------------------------------------------------------------------
# Scheduler status + controls
# --------------------------------------------------------------------------------------

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

@app.post("/scheduler/core_debug")
def scheduler_core_debug(payload: Dict[str, Any] = Body(default=None)):
    """
    Run scheduler_core.run_scheduler_once with the current config,
    but DO NOT route anything to the broker.

    Returns:
      - config (resolved from payload + env)
      - positions
      - raw intents from scheduler_core (before caps/guard/loss-zone)
      - scheduler_core telemetry
    """
    payload = payload or {}

    def _env_bool(key: str, default: bool) -> bool:
        v = os.getenv(key)
        if v is None:
            return default
        return str(v).lower() in ("1", "true", "yes", "on")

    tf = str(payload.get("tf", os.getenv("SCHED_TIMEFRAME", "5Min")))
    strats_csv = str(payload.get("strats", os.getenv("SCHED_STRATS", "c1,c2,c3,c4,c5,c6")))
    strats = [s.strip().lower() for s in strats_csv.split(",") if s.strip()]

    symbols_csv = str(payload.get("symbols", os.getenv("SYMBOLS", "BTC/USD,ETH/USD")))
    syms = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]

    limit = int(payload.get("limit", int(os.getenv("SCHED_LIMIT", "300") or 300)))
    notional = float(payload.get("notional", float(os.getenv("SCHED_NOTIONAL", "25") or 25.0)))

    config_snapshot = {
        "tf": tf,
        "strats_raw": strats_csv,
        "strats": strats,
        "symbols_raw": symbols_csv,
        "symbols": syms,
        "limit": limit,
        "notional": notional,
    }

    # positions + risk_cfg
    positions = _load_open_positions_from_trades(use_strategy_col=True)
    risk_cfg = load_risk_config() or {}
    risk_engine = RiskEngine(risk_cfg)

    # preload bars (same as v2)
    try:
        import br_router as br  # type: ignore[import]
    except Exception as e:
        return {
            "ok": False,
            "error": f"failed to import br_router: {e}",
            "config": config_snapshot,
        }

    contexts: Dict[str, Any] = {}
    telemetry: List[Dict[str, Any]] = []

    def _safe_series(bars, key: str):
        vals = []
        if isinstance(bars, list):
            for row in bars:
                if isinstance(row, dict) and key in row:
                    vals.append(row[key])
        return vals

    for sym in syms:
        try:
            one = br.get_bars(sym, timeframe="1Min", limit=limit)
            multi = br.get_bars(sym, timeframe=tf, limit=limit)

            if not one or not multi:
                contexts[sym] = None
                telemetry.append(
                    {
                        "symbol": sym,
                        "stage": "preload_bars",
                        "ok": False,
                        "reason": "no_bars",
                    }
                )
                continue

            contexts[sym] = {
                "one": {
                    "open": _safe_series(one, "open"),
                    "high": _safe_series(one, "high"),
                    "low": _safe_series(one, "low"),
                    "close": _safe_series(one, "close"),
                    "volume": _safe_series(one, "volume"),
                    "ts": _safe_series(one, "ts"),
                },
                "five": {
                    "open": _safe_series(multi, "open"),
                    "high": _safe_series(multi, "high"),
                    "low": _safe_series(multi, "low"),
                    "close": _safe_series(multi, "close"),
                    "volume": _safe_series(multi, "volume"),
                    "ts": _safe_series(multi, "ts"),
                },
            }
        except Exception as e:
            contexts[sym] = None
            telemetry.append(
                {
                    "symbol": sym,
                    "stage": "preload_bars",
                    "ok": False,
                    "error": f"{e.__class__.__name__}: {e}",
                }
            )

    now = dt.datetime.utcnow()
    cfg = SchedulerConfig(
        now=now,
        timeframe=tf,
        limit=limit,
        symbols=syms,
        strats=strats,
        notional=notional,
        positions=positions,
        contexts=contexts,
        risk_cfg=risk_cfg,
    )

    result: SchedulerResult = run_scheduler_once(cfg, last_price_fn=_last_price_safe)

    telemetry.extend(result.telemetry)

    intents_out = []
    for it in result.intents:
        intents_out.append(
            {
                "strategy": it.strategy,
                "symbol": it.symbol,
                "kind": it.kind,
                "side": it.side,
                "notional": it.notional,
                "reason": it.reason,
                "meta": it.meta,
            }
        )

    pos_out = []
    for (sym, strat), pm_pos in positions.items():
        pos_out.append(
            {
                "symbol": sym,
                "strategy": strat,
                "qty": float(getattr(pm_pos, "qty", 0.0) or 0.0),
                "avg_price": getattr(pm_pos, "avg_price", None),
            }
        )

    return {
        "ok": True,
        "config": config_snapshot,
        "positions": pos_out,
        "intents": intents_out,
        "telemetry": telemetry,
    }

@app.get("/scheduler/risk_debug")
def scheduler_risk_debug(
    symbol: Optional[str] = Query(None),
    strategy: Optional[str] = Query(None),
):
    """
    For each open position (optionally filtered by symbol/strategy),
    show:

    - unrealized_pct
    - symbol caps
    - global exit decision (if any)
    """
    positions = _load_open_positions_from_trades(use_strategy_col=True)
    risk_cfg = load_risk_config() or {}
    risk_engine = RiskEngine(risk_cfg)

    now = dt.datetime.utcnow()

    out = []
    for (sym, strat), pm_pos in positions.items():
        if symbol and sym != symbol:
            continue
        if strategy and strat != strategy:
            continue

        snap = PositionSnapshot(
            symbol=sym,
            strategy=strat,
            qty=float(getattr(pm_pos, "qty", 0.0) or 0.0),
            avg_price=getattr(pm_pos, "avg_price", None),
            unrealized_pct=None,
        )
        try:
            upnl = risk_engine.compute_unrealized_pct(snap, last_price_fn=_last_price_safe)
        except Exception:
            upnl = None

        snap.unrealized_pct = upnl
        max_notional, max_units = risk_engine.symbol_caps(sym)
        reason = risk_engine.apply_global_exit_rules(
            pos=snap,
            unrealized_pct=upnl,
            atr_pct=None,
            now=now,
        )

        out.append(
            {
                "symbol": sym,
                "strategy": strat,
                "qty": snap.qty,
                "avg_price": snap.avg_price,
                "unrealized_pct": upnl,
                "max_notional_cap": max_notional,
                "max_units_cap": max_units,
                "global_exit_reason": reason,
            }
        )

    return {"positions": out}


# --------------------------------------------------------------------------------------
# Background scheduler loop
# --------------------------------------------------------------------------------------

def _scheduler_loop():
    """Background loop honoring _SCHED_ENABLED and _SCHED_SLEEP.
    Builds payload from env so Render env toggles work without redeploy.
    """
    global _SCHED_ENABLED, _SCHED_TICKS
    tick = 0
    while True:
        try:
            if _SCHED_ENABLED:
                payload = {
                    "tf": os.getenv("SCHED_TIMEFRAME", os.getenv("DEFAULT_TIMEFRAME", "5Min") or "5Min"),
                    "strats": os.getenv("SCHED_STRATS", "c1,c2,c3,c4,c5,c6"),
                    "symbols": os.getenv("SYMBOLS", "BTC/USD,ETH/USD"),
                    "limit": int(os.getenv("SCHED_LIMIT", os.getenv("DEFAULT_LIMIT", "300") or "300")),
                    "notional": float(os.getenv("SCHED_NOTIONAL", os.getenv("DEFAULT_NOTIONAL", "25") or "25")),
                    "dry_run": str(os.getenv("SCHED_DRY", "0")).lower() in ("1", "true", "yes"),
                }

                # v2 payload: map dry_run -> dry
                payload_v2 = {
                    "tf": payload["tf"],
                    "strats": payload["strats"],
                    "symbols": payload["symbols"],
                    "limit": payload["limit"],
                    "notional": payload["notional"],
                    "dry": payload["dry_run"],
                }

                try:
                    # Keep /debug/scheduler status intact
                    with _SCHED_LAST_LOCK:
                        _SCHED_LAST.clear()
                        _SCHED_LAST.update(payload | {"ts": dt.datetime.utcnow().isoformat() + "Z"})
                except Exception as e:
                    log.warning("could not set _SCHED_LAST (loop): %s", e)

                # >>> new brain <<<
                _ = scheduler_run_v2(payload_v2)

                tick += 1
                _SCHED_TICKS = tick
                log.info("scheduler v2 tick #%s ok", tick)
        except Exception as e:
            log.exception("scheduler loop error: %s", e)
        time.sleep(_SCHED_SLEEP)

# --------------------------------------------------------------------------------------
# Startup hook (ensure DB + scheduler thread)
# --------------------------------------------------------------------------------------

def _ensure_strategy_column():
    conn = sqlite3.connect(DB_PATH)
    try:
        cur = conn.cursor()
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
# Price helper used by scheduler
# --------------------------------------------------------------------------------------

def _last_price_safe(symbol: str) -> float:
    """
    Best-effort last price lookup that works whether we're using Kraken directly
    or only have br_router available. Returns 0.0 on any failure.
    """
    try:
        import broker_kraken as _bk  # type: ignore[import]
    except Exception:
        try:
            import br_router as _bk  # type: ignore[import]
        except Exception:
            return 0.0

    try:
        if hasattr(_bk, "last_price"):
            px = _bk.last_price(symbol)
            return float(px or 0.0)
    except Exception:
        return 0.0
    return 0.0


# --------------------------------------------------------------------------------------
# Scheduler run (manual + background)
# --------------------------------------------------------------------------------------

@app.post("/scheduler/run")
def scheduler_run(payload: Dict[str, Any] = Body(default=None)):
    """
    Real scheduler: fetch bars once, run StrategyBook on requested strats/symbols,
    and (optionally) place market orders via br_router.market_notional.
    """
    actions: List[Dict[str, Any]] = []
    telemetry: List[Dict[str, Any]] = []

    # small helpers for super-safe config access
    def _cfg_get(d: Any, key: str, default: Any = None) -> Any:
        return d.get(key, default) if isinstance(d, dict) else default

    def _cfg_dict(x: Any) -> Dict[str, Any]:
        return x if isinstance(x, dict) else {}

    try:
        payload = payload or {}

        # --- imports that can fail cleanly ----------------------------------
        try:
            import br_router as br
        except Exception as e:
            msg = f"import br_router failed: {e}"
            log.warning(msg)
            return {"ok": False, "message": msg, "actions": actions, "telemetry": telemetry}
        try:
            from book import StrategyBook, ScanRequest
        except Exception as e:
            msg = f"import book failed: {e}"
            log.warning(msg)
            return {"ok": False, "message": msg, "actions": actions, "telemetry": telemetry}

        # --- resolve inputs -------------------------------------------------
        def _resolve_dry(_p):
            _env = str(os.getenv("SCHED_DRY", "1")).lower() in ("1", "true", "yes")
            try:
                if _p is None:
                    return _env
                if isinstance(_p, dict):
                    v = _p.get("dry_run", None)
                else:
                    v = None
                return _env if v is None else bool(v)
            except Exception:
                return _env

        dry = _resolve_dry(payload)
        tf = str(payload.get("tf", os.getenv("SCHED_TIMEFRAME", "5Min")))
        strats = [
            s.strip().lower()
            for s in str(payload.get("strats", os.getenv("SCHED_STRATS", "c1,c2,c3"))).split(",")
            if s.strip()
        ]
        symbols_csv = str(payload.get("symbols", os.getenv("SYMBOLS", "BTC/USD,ETH/USD")))
        syms = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
        limit = int(payload.get("limit", int(os.getenv("SCHED_LIMIT", "300") or 300)))
        notional = float(payload.get("notional", float(os.getenv("SCHED_NOTIONAL", "25") or 25)))
        msg = (
            f"Scheduler pass: strats={','.join(strats)} tf={tf} "
            f"limit={limit} notional={notional} dry={dry} symbols={symbols_csv}"
        )
        log.info(msg)

        # Load open positions keyed by (symbol, strategy).
        positions = _load_open_positions_from_trades(use_strategy_col=True)

        # --- risk config ----------------------------------------------------
        # Load global risk policy config
        try:
            _risk_cfg = load_risk_config()
        except Exception:
            _risk_cfg = {}

        # Extra safety: normalize to dict in case something weird slips through
        if not isinstance(_risk_cfg, dict):
            try:
                import json as _json
                if isinstance(_risk_cfg, str):
                    _risk_cfg = _json.loads(_risk_cfg)
                    if not isinstance(_risk_cfg, dict):
                        _risk_cfg = {}
                else:
                    _risk_cfg = {}
            except Exception:
                _risk_cfg = {}

        daily_flat_cfg  = _cfg_dict(_cfg_get(_risk_cfg, "daily_flatten", {}))
        risk_caps_cfg   = _cfg_dict(_cfg_get(_risk_cfg, "risk_caps", {}))
        profit_lock_cfg = _cfg_dict(_cfg_get(_risk_cfg, "profit_lock", {}))
        loss_zone_cfg   = _cfg_dict(_cfg_get(_risk_cfg, "loss_zone", {}))
        time_mult_cfg   = _cfg_dict(_cfg_get(_risk_cfg, "time_multipliers", {}))
        atr_floor_cfg   = _cfg_dict(_cfg_get(_risk_cfg, "atr_floor_pct", {}))
        tiers_cfg       = _cfg_dict(_cfg_get(_risk_cfg, "tiers", {}))

        # Day vs night time multiplier for risk caps
        time_mult = 1.0
        try:
            day_night_cfg = _cfg_dict(_cfg_get(time_mult_cfg, "day_night", {}))
            if day_night_cfg:
                try:
                    from zoneinfo import ZoneInfo as _ZInfo
                    _tz_mult = os.getenv("TZ", "UTC")
                    _now_local_mult = dt.datetime.now(_ZInfo(_tz_mult))
                except Exception:
                    _now_local_mult = dt.datetime.utcnow()

                day_start = int(day_night_cfg.get("day_start_hour_local", 6))
                night_start = int(day_night_cfg.get("night_start_hour_local", 19))
                day_multiplier = float(day_night_cfg.get("day_multiplier", 1.0))
                night_multiplier = float(day_night_cfg.get("night_multiplier", 1.0))
                hour = _now_local_mult.hour

                if day_start <= hour < night_start:
                    time_mult = day_multiplier
                else:
                    time_mult = night_multiplier
        except Exception:
            time_mult = 1.0

        # Daily flatten window
        flatten_mode = False
        try:
            if daily_flat_cfg.get("enabled"):
                try:
                    from zoneinfo import ZoneInfo as _ZInfo
                    _tz = os.getenv("TZ", "UTC")
                    _now_local = dt.datetime.now(_ZInfo(_tz))
                except Exception:
                    _now_local = dt.datetime.utcnow()
                _fh = int(daily_flat_cfg.get("flatten_hour_local", 23))
                _fm = int(daily_flat_cfg.get("flatten_minute_local", 0))
                if (_now_local.hour, _now_local.minute) >= (_fh, _fm):
                    flatten_mode = True
        except Exception:
            flatten_mode = False

        # Record last-run
        try:
            with _SCHED_LAST_LOCK:
                _SCHED_LAST.clear()
                _SCHED_LAST.update(
                    {
                        "tf": tf,
                        "strats": ",".join(strats),
                        "symbols": symbols_csv,
                        "limit": limit,
                        "notional": notional,
                        "dry_run": dry,
                        "ts": dt.datetime.utcnow().isoformat() + "Z",
                    }
                )
        except Exception as e:
            log.warning("could not set _SCHED_LAST: %s", e)

        # Env-tunable book params
        topk = int(os.getenv("BOOK_TOPK", "2") or 2)
        min_score = float(os.getenv("BOOK_MIN_SCORE", "0.07") or 0.07)
        atr_stop_mult = float(os.getenv("ATR_STOP_MULT", "1.0") or 1.0)

        # Fee/edge guard envs
        taker_fee_bps = float(os.getenv("KRAKEN_TAKER_FEE_BPS", "26") or 26.0)
        fee_multiple = float(os.getenv("EDGE_MULTIPLE_VS_FEE", "2.0") or 2.0)
        min_notional = float(os.getenv("MIN_ORDER_NOTIONAL_USD", "10") or 10.0)

        # --- preload bars once (defensive) ----------------------------------
        contexts: Dict[str, Any] = {}

        def _safe_series(bars, key: str):
            vals = []
            if isinstance(bars, list):
                for row in bars:
                    if isinstance(row, dict) and key in row:
                        vals.append(row[key])
            return vals

        for sym in syms:
            try:
                one = br.get_bars(sym, timeframe="1Min", limit=limit)
                five = br.get_bars(sym, timeframe=tf, limit=limit)
                contexts[sym] = {
                    "one": {
                        "close": _safe_series(one, "c"),
                        "high": _safe_series(one, "h"),
                        "low": _safe_series(one, "l"),
                    },
                    "five": {
                        "close": _safe_series(five, "c"),
                        "high": _safe_series(five, "h"),
                        "low": _safe_series(five, "l"),
                    },
                }
            except Exception as e:
                contexts[sym] = {
                    "one": {"close": [], "high": [], "low": []},
                    "five": {"close": [], "high": [], "low": []},
                    "error": str(e),
                }
                log.warning("bars preload error for %s: %s", sym, e)

        # --- main strategy loop ---------------------------------------------
        for strat in strats:
            book = StrategyBook(topk=topk, min_score=min_score, atr_stop_mult=atr_stop_mult)
            req = ScanRequest(
                strat=strat,
                timeframe=tf,
                limit=limit,
                topk=topk,
                min_score=min_score,
                notional=notional,
            )
            try:
                res = book.scan(req, contexts)
            except Exception as e:
                telemetry.append({"strategy": strat, "error": str(e)})
                continue

            for r in res:
                try:
                    edge_pct = float(r.atr_pct or 0.0) * 100.0
                    fee_pct = taker_fee_bps / 10000.0
                    guard_ok = True
                    guard_reason = None
                    if r.action not in ("buy", "sell"):
                        guard_ok = False
                        guard_reason = r.reason or "flat"
                    elif r.notional < max(min_notional, 0.0):
                        guard_ok = False
                        guard_reason = f"notional_below_min:{r.notional:.2f}"
                    elif edge_pct < (fee_multiple * fee_pct * 100.0):
                        guard_ok = False
                        guard_reason = f"edge_vs_fee_low:{edge_pct:.3f}pct"

                    telemetry.append(
                        {
                            "strategy": strat,
                            "symbol": r.symbol,
                            "raw_action": r.action,
                            "reason": r.reason,
                            "score": r.score,
                            "atr": r.atr,
                            "atr_pct": r.atr_pct,
                            "qty": r.qty,
                            "notional": r.notional,
                            "guard_ok": guard_ok,
                            "guard_reason": guard_reason,
                        }
                    )

                    if not guard_ok:
                        continue

                    sym = r.symbol
                    pos = _position_for(sym, strat, positions)
                    current_qty = float(pos.qty) if pos is not None else 0.0

                    # Unrealized PnL %
                    unrealized_pct = None
                    if pos is not None and pos.avg_price and abs(pos.qty) > 1e-10:
                        _px = _last_price_safe(sym)
                        try:
                            _avg = float(pos.avg_price or 0.0)
                        except Exception:
                            _avg = 0.0
                        if _px > 0.0 and _avg > 0.0:
                            if pos.qty > 0:
                                unrealized_pct = (_px - _avg) / _avg * 100.0
                            else:
                                unrealized_pct = (_avg - _px) / _avg * 100.0

                    # Loss-zone no-rebuy
                    if unrealized_pct is not None:
                        _lz_cfg = _cfg_dict(loss_zone_cfg)
                        _lz = _cfg_get(_lz_cfg, "no_rebuy_below_pct", None)
                        try:
                            _lz = float(_lz) if _lz is not None else None
                        except Exception:
                            _lz = None
                        if (
                            _lz is not None
                            and unrealized_pct <= _lz
                            and current_qty > 0
                            and r.action == "buy"
                        ):
                            guard_ok = False
                            guard_reason = f"loss_zone_block:{unrealized_pct:.2f}pct"
                            telemetry.append(
                                {
                                    "strategy": strat,
                                    "symbol": sym,
                                    "raw_action": r.action,
                                    "reason": r.reason,
                                    "loss_zone": True,
                                    "unrealized_pct": unrealized_pct,
                                    "loss_zone_threshold": _lz,
                                }
                            )
                            continue

                    # Helper to send an order
                    def _send(symbol: str, side: str, notional_value: float, intent: str) -> None:
                        try:
                            if intent.startswith("open"):
                                _rcfg = _cfg_dict(risk_caps_cfg)
                                _max_notional_map = _cfg_dict(
                                    _cfg_get(_rcfg, "max_notional_per_symbol", {})
                                )
                                _max_units_map = _cfg_dict(
                                    _cfg_get(_rcfg, "max_units_per_symbol", {})
                                )
                                _sym_key = symbol.upper()
                                _sym_norm = "".join(ch for ch in _sym_key if ch.isalnum())
                                _cap_notional = _max_notional_map.get(
                                    _sym_key,
                                    _max_notional_map.get(
                                        _sym_norm, _max_notional_map.get("default")
                                    ),
                                )
                                if _cap_notional is not None:
                                    try:
                                        _cap_notional = float(_cap_notional) * float(
                                            time_mult or 1.0
                                        )
                                    except Exception:
                                        _cap_notional = float(_cap_notional)
                                _cap_units = _max_units_map.get(
                                    _sym_key,
                                    _max_units_map.get(
                                        _sym_norm, _max_units_map.get("default")
                                    ),
                                )
                                if _cap_notional is not None or _cap_units is not None:
                                    _pos_here = _position_for(symbol, strat, positions)
                                    _qty_here = float(_pos_here.qty) if _pos_here is not None else 0.0
                                    _px_here = _last_price_safe(symbol)
                                    _cur_notional = abs(_qty_here) * _px_here
                                    if _cap_notional is not None and _px_here > 0.0:
                                        _max_additional = float(_cap_notional) - float(
                                            _cur_notional
                                        )
                                        if _max_additional <= 0:
                                            actions.append(
                                                {
                                                    "symbol": symbol,
                                                    "side": side,
                                                    "strategy": strat,
                                                    "notional": 0.0,
                                                    "intent": intent,
                                                    "status": "blocked_cap",
                                                    "reason": f"cap_notional:{_cur_notional:.2f}>={_cap_notional}",
                                                }
                                            )
                                            return
                                        if notional_value > _max_additional:
                                            notional_value = _max_additional
                                    if _cap_units is not None and _px_here > 0.0:
                                        _max_units_total = float(_cap_units)
                                        _max_units_add = _max_units_total - abs(_qty_here)
                                        if _max_units_add <= 0:
                                            actions.append(
                                                {
                                                    "symbol": symbol,
                                                    "side": side,
                                                    "strategy": strat,
                                                    "notional": 0.0,
                                                    "intent": intent,
                                                    "status": "blocked_cap",
                                                    "reason": f"cap_units:{abs(_qty_here):.6f}>={_max_units_total}",
                                                }
                                            )
                                            return
                                        _max_notional_units = _max_units_add * _px_here
                                        if notional_value > _max_notional_units:
                                            notional_value = _max_notional_units
                            if notional_value <= 0:
                                actions.append(
                                    {
                                        "symbol": symbol,
                                        "side": side,
                                        "strategy": strat,
                                        "notional": 0.0,
                                        "intent": intent,
                                        "status": "blocked_cap",
                                        "reason": "notional_after_caps<=0",
                                    }
                                )
                                return
                        except Exception:
                            # Fail-open on caps errors
                            pass

                        act = {
                            "symbol": symbol,
                            "side": side,
                            "strategy": strat,
                            "notional": float(notional_value),
                            "intent": intent,
                        }
                        if dry:
                            act["status"] = "dry_ok"
                        else:
                            try:
                                resp = br.market_notional(symbol, side, notional_value, strategy=strat)
                                act["status"] = "live_ok"
                                act["broker"] = resp
                            except Exception as e:
                                act["status"] = "live_err"
                                act["error"] = str(e)
                        actions.append(act)

                    # Notional from qty
                    def _notional_for_qty(symbol: str, qty: float) -> float:
                        px = _last_price_safe(symbol)
                        return abs(qty) * px if px > 0 else 0.0

                    # Start from raw action, then apply flatten + PnL/ATR-based overrides
                    desired = r.action

                    if flatten_mode:
                        log.info("[sched] daily_flatten active -> flatten %s for %s", sym, strat)
                        desired = "flat"

                    # Profit-lock
                    if unrealized_pct is not None:
                        _tp_cfg = _cfg_dict(profit_lock_cfg)
                        _tp = _cfg_get(_tp_cfg, "take_profit_pct", None)
                        try:
                            _tp = float(_tp) if _tp is not None else None
                        except Exception:
                            _tp = None

                        if _tp is not None and unrealized_pct >= _tp:
                            desired = "flat"
                            log.info(
                                "[sched] profit_lock_flatten: strat=%s sym=%s upnl=%.2f tp=%.2f",
                                strat,
                                sym,
                                unrealized_pct,
                                _tp,
                            )
                            telemetry.append(
                                {
                                    "strategy": strat,
                                    "symbol": sym,
                                    "raw_action": r.action,
                                    "reason": r.reason,
                                    "unrealized_pct": unrealized_pct,
                                    "take_profit_pct": _tp,
                                    "exit_reason": "profit_lock_flatten",
                                }
                            )

                    # Stop-loss
                    if unrealized_pct is not None:
                        _sl_cfg = _cfg_dict(loss_zone_cfg)
                        _sl = _cfg_get(_sl_cfg, "stop_loss_pct", _cfg_get(_sl_cfg, "no_rebuy_below_pct"))
                        try:
                            _sl = float(_sl) if _sl is not None else None
                        except Exception:
                            _sl = None

                        if _sl is not None and unrealized_pct <= _sl:
                            desired = "flat"
                            log.info(
                                "[sched] stop_loss_flatten: strat=%s sym=%s upnl=%.2f sl=%.2f",
                                strat,
                                sym,
                                unrealized_pct,
                                _sl,
                            )
                            telemetry.append(
                                {
                                    "strategy": strat,
                                    "symbol": sym,
                                    "raw_action": r.action,
                                    "reason": r.reason,
                                    "unrealized_pct": unrealized_pct,
                                    "stop_loss_pct": _sl,
                                    "exit_reason": "stop_loss_flatten",
                                }
                            )

                    # ATR reversal
                    try:
                        atr_val = float(getattr(r, "atr_pct", 0.0) or 0.0)

                        sym_norm = sym.replace("/", "").replace("-", "")
                        tier_name = None
                        for tname, symbols_list in _cfg_dict(tiers_cfg).items():
                            # symbols_list can be list OR string; normalize to list
                            if isinstance(symbols_list, str):
                                symbols_iter = [symbols_list]
                            else:
                                symbols_iter = list(symbols_list) if symbols_list is not None else []
                            if sym_norm in symbols_iter:
                                tier_name = tname
                                break

                        atr_floor = None
                        if tier_name is not None:
                            atr_floor = _cfg_get(atr_floor_cfg, tier_name, None)

                        if atr_floor is not None and atr_val < float(atr_floor) and abs(current_qty) > 1e-10:
                            if desired != "flat":
                                desired = "flat"
                                log.info(
                                    "[sched] atr_reversal_flatten: strat=%s sym=%s atr_pct=%.4f floor=%.4f",
                                    strat,
                                    sym,
                                    atr_val,
                                    float(atr_floor),
                                )
                                telemetry.append(
                                    {
                                        "strategy": strat,
                                        "symbol": sym,
                                        "raw_action": r.action,
                                        "reason": r.reason,
                                        "unrealized_pct": unrealized_pct,
                                        "atr_pct": atr_val,
                                        "atr_floor": float(atr_floor),
                                        "exit_reason": "atr_reversal_flatten",
                                    }
                                )
                    except Exception:
                        pass

                    # Case 0: Already flat
                    if abs(current_qty) < 1e-10:
                        # Long-only guard: do NOT open a fresh short when flat.
                        # Instead, log telemetry so we can see that we skipped it.
                        if desired == "sell":
                            telemetry.append(
                                {
                                    "symbol": sym,
                                    "strategy": strat,
                                    "kind": "entry_skip",
                                    "side": "sell",
                                    "reason": "long_only_blocked_sell_entry",
                                    "source": "scheduler_v2",
                                    "scan_score": float(getattr(r, "score", 0.0) or 0.0),
                                    "scan_atr_pct": float(getattr(r, "atr_pct", 0.0) or 0.0),
                                    "scan_notional": float(
                                        getattr(r, "notional", 0.0) or 0.0
                                    ),
                                }
                            )
                            # Do not send any order in this case
                            continue

                        # Normal long-only entry: open a BUY when flat
                        if desired == "buy":
                            target_notional = float(
                                r.notional if r.notional and r.notional > 0 else notional
                            )
                            _send(sym, "buy", target_notional, intent="open_buy")

                        # In all other cases while flat, do nothing.
                        continue


                    # Case 1: Currently long
                    if current_qty > 0:
                        if desired == "buy":
                            continue
                        close_notional = _notional_for_qty(sym, current_qty)
                        if close_notional <= 0:
                            continue

                        if desired == "flat":
                            _send(sym, "sell", close_notional, intent="close_long")
                            continue
                        elif desired == "sell":
                            _send(sym, "sell", close_notional, intent="flip_close_long")
                            target_notional = float(
                                r.notional if r.notional and r.notional > 0 else notional
                            )
                            _send(sym, "sell", target_notional, intent="flip_open_short")
                            continue

                    # Case 2: Currently short
                    if current_qty < 0:
                        if desired == "sell":
                            continue
                        close_notional = _notional_for_qty(sym, current_qty)
                        if close_notional <= 0:
                            continue

                        if desired == "flat":
                            _send(sym, "buy", close_notional, intent="close_short")
                            continue
                        elif desired == "buy":
                            _send(sym, "buy", close_notional, intent="flip_close_short")
                            target_notional = float(
                                r.notional if r.notional and r.notional > 0 else notional
                            )
                            _send(sym, "buy", target_notional, intent="flip_open_long")
                            continue

                except Exception as inner_e:
                    # Belt-and-suspenders: don't let a single symbol/strategy blow up the whole run
                    log.exception("scheduler_run per-row error: %s", inner_e)
                    telemetry.append({
                        "strategy": strat,
                        "symbol": getattr(r, "symbol", None),
                        "error": str(inner_e),
                        "stage": "row_dispatch",
                    })
                    continue

        return {"ok": True, "message": msg, "actions": actions, "telemetry": telemetry}

    except Exception as e:
        log.exception("scheduler_run error: %s", e)
        return {
            "ok": False,
            "message": "scheduler_run_failed",
            "error": str(e),
            "actions": actions,
            "telemetry": telemetry,
        }
        
# --------------------------------------------------------------------------------------
# Scheduler v2: uses scheduler_core + risk_engine + br_router
# --------------------------------------------------------------------------------------

@app.post("/scheduler/v2/run")
def scheduler_run_v2(payload: Dict[str, Any] = Body(default=None)):
    """
    Scheduler v2:

    - Uses scheduler_core + RiskEngine to produce explicit OrderIntents
      (entries + exits + per-strategy + global exits).
    - Applies per-symbol caps + loss-zone no-rebuy for ENTRY/SCALE intents.
    - Routes surviving intents through br_router.market_notional when dry=False.

    This does NOT remove the legacy /scheduler/run endpoint. Use this side-by-side
    for testing until you're happy to flip over.
    """
    actions: List[Dict[str, Any]] = []
    telemetry: List[Dict[str, Any]] = []

    payload = payload or {}

    # ------------------------------------------------------------------
    # Helper: env bool
    # ------------------------------------------------------------------
    def _env_bool(key: str, default: bool) -> bool:
        v = os.getenv(key)
        if v is None:
            return default
        return str(v).lower() in ("1", "true", "yes", "on")

    # Dry-run flag: payload.dry overrides SCHED_DRY (default True)
    dry = payload.get("dry", None)
    if dry is None:
        dry = _env_bool("SCHED_DRY", True)
    dry = bool(dry)

    # ------------------------------------------------------------------
    # Minimum order notional (USD) to avoid dust orders
    # ------------------------------------------------------------------
    MIN_NOTIONAL_USD = float(os.getenv("MIN_ORDER_NOTIONAL_USD", "5.0") or 5.0)

    # ------------------------------------------------------------------
    # Resolve basic scheduler config from payload + env
    # ------------------------------------------------------------------
    tf = str(payload.get("tf", os.getenv("SCHED_TIMEFRAME", "5Min")))
    strats_csv = str(payload.get("strats", os.getenv("SCHED_STRATS", "c1,c2,c3,c4,c5,c6")))
    strats = [s.strip().lower() for s in strats_csv.split(",") if s.strip()]

    symbols_csv = str(payload.get("symbols", os.getenv("SYMBOLS", "BTC/USD,ETH/USD")))
    syms = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]

    limit = int(payload.get("limit", int(os.getenv("SCHED_LIMIT", "300") or 300)))
    notional = float(payload.get("notional", float(os.getenv("SCHED_NOTIONAL", "25") or 25.0)))

    config_snapshot = {
        "tf": tf,
        "strats_raw": strats_csv,
        "strats": strats,
        "symbols_raw": symbols_csv,
        "symbols": syms,
        "limit": limit,
        "notional": notional,
        "dry": bool(dry),
    }

    log.info(
        "Scheduler v2: strats=%s tf=%s limit=%s notional=%s dry=%s symbols=%s",
        ",".join(strats),
        tf,
        limit,
        notional,
        dry,
        symbols_csv,
    )

    # ------------------------------------------------------------------
    # Imports that can fail without killing the API entirely
    # ------------------------------------------------------------------
    try:
        import br_router as br  # type: ignore[import]
    except Exception as e:
        log.error("scheduler_v2: failed to import br_router: %s", e)
        if not dry:
            return {"ok": False, "error": f"failed to import br_router: {e}", "config": config_snapshot}
        br = None  # dry mode can still show intents

    try:
        from policy.guard import guard_allows  # type: ignore[import]
    except Exception:
        guard_allows = None  # optional; strategies already use guard internally

    # ------------------------------------------------------------------
    # Load positions & risk config
    # ------------------------------------------------------------------
    positions = _load_open_positions_from_trades(use_strategy_col=True)
    risk_cfg = load_risk_config() or {}
    risk_engine = RiskEngine(risk_cfg)

    # ------------------------------------------------------------------
    # Preload bar contexts once (similar to legacy scheduler_run)
    # ------------------------------------------------------------------
    contexts: Dict[str, Any] = {}

    def _safe_series(bars, key: str):
        vals = []
        if isinstance(bars, list):
            for row in bars:
                if isinstance(row, dict) and key in row:
                    vals.append(row[key])
        return vals

    for sym in syms:
        try:
            # For now, hard-code 1m + tf (e.g. 5Min) like your existing path
            one = br.get_bars(sym, timeframe="1Min", limit=limit) if br is not None else []
            multi = br.get_bars(sym, timeframe=tf, limit=limit) if br is not None else []

            if not one or not multi:
                contexts[sym] = None
                telemetry.append(
                    {
                        "symbol": sym,
                        "stage": "preload_bars",
                        "ok": False,
                        "reason": "no_bars",
                    }
                )
                continue

            contexts[sym] = {
                "one": {
                    "open": _safe_series(one, "open"),
                    "high": _safe_series(one, "high"),
                    "low": _safe_series(one, "low"),
                    "close": _safe_series(one, "close"),
                    "volume": _safe_series(one, "volume"),
                    "ts": _safe_series(one, "ts"),
                },
                "five": {  # we keep the key name 'five' even if tf != 5Min
                    "open": _safe_series(multi, "open"),
                    "high": _safe_series(multi, "high"),
                    "low": _safe_series(multi, "low"),
                    "close": _safe_series(multi, "close"),
                    "volume": _safe_series(multi, "volume"),
                    "ts": _safe_series(multi, "ts"),
                },
            }
        except Exception as e:
            contexts[sym] = None
            telemetry.append(
                {
                    "symbol": sym,
                    "stage": "preload_bars",
                    "ok": False,
                    "error": f"{e.__class__.__name__}: {e}",
                }
            )

    # ------------------------------------------------------------------
    # Run scheduler_core (strategies + per-strat + global exits)
    # ------------------------------------------------------------------
    now = dt.datetime.utcnow()
    cfg = SchedulerConfig(
        now=now,
        timeframe=tf,
        limit=limit,
        symbols=syms,
        strats=strats,
        notional=notional,
        positions=positions,
        contexts=contexts,
        risk_cfg=risk_cfg,
    )

    result: SchedulerResult = run_scheduler_once(cfg, last_price_fn=_last_price_safe)
    telemetry.extend(result.telemetry)

    # ------------------------------------------------------------------
    # Deduplicate intents: exits take precedence over entries on same (sym,strat)
    # ------------------------------------------------------------------
    exit_kinds = {"exit", "take_profit", "stop_loss"}
    entry_kinds = {"entry", "scale"}

    # Priority for exits: stop_loss > take_profit > exit
    def _exit_priority(kind: str) -> int:
        if kind == "stop_loss":
            return 3
        if kind == "take_profit":
            return 2
        return 1  # generic exit

    best_exit: Dict[tuple, Any] = {}
    entries: List[Any] = []

    for intent in result.intents:
        key = (intent.symbol, intent.strategy)

        if intent.kind in exit_kinds:
            prev = best_exit.get(key)
            if prev is None or _exit_priority(intent.kind) > _exit_priority(prev.kind):
                best_exit[key] = intent
        elif intent.kind in entry_kinds:
            entries.append(intent)
        else:
            # unknown kind: just pass through as a generic action
            entries.append(intent)

    # Keep entries only if there is *no* exit for that (symbol,strategy)
    final_intents: List[Any] = list(best_exit.values())
    for intent in entries:
        key = (intent.symbol, intent.strategy)
        if key in best_exit:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": intent.kind,
                    "side": intent.side,
                    "reason": "dropped_entry_due_to_exit_same_pass",
                    "source": "scheduler_v2",
                }
            )
            continue
        final_intents.append(intent)

    # ------------------------------------------------------------------
    # Apply guard + per-symbol caps + loss-zone no-rebuy; then route
    # ------------------------------------------------------------------
    for intent in final_intents:
        key = (intent.symbol, intent.strategy)
        pm_pos = positions.get(key)

        snap = PositionSnapshot(
            symbol=intent.symbol,   
            strategy=intent.strategy,
            qty=float(getattr(pm_pos, "qty", 0.0) or 0.0),
            avg_price=getattr(pm_pos, "avg_price", None),
            unrealized_pct=None,
        )

        # Fill unrealized_pct here so loss-zone + any extra logic
        # see the exact same P&L % that scheduler_core used.
        try:
            snap.unrealized_pct = risk_engine.compute_unrealized_pct(
                snap,
                last_price_fn=_last_price_safe,
            )
        except Exception:
            snap.unrealized_pct = None

        guard_allowed = True
        guard_reason = "ok"
        if guard_allows is not None and intent.kind in entry_kinds:
            try:
                guard_allowed, guard_reason = guard_allows(intent.strategy, intent.symbol, now=now)
            except Exception as e:
                guard_allowed = False
                guard_reason = f"guard_exception:{e}"

        if not guard_allowed:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": intent.kind,
                    "side": intent.side,
                    "reason": guard_reason,
                    "source": "guard_allows",
                }
            )
            continue

        # Decide final notional to send
        final_notional: float = 0.0
        side = intent.side

        # ----- ENTRY / SCALE: enforce caps & loss-zone --------------------------------
        if intent.kind in entry_kinds:
            if intent.notional is None or intent.notional <= 0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": "entry_without_notional",
                        "source": "scheduler_v2",
                    }
                )
                continue

            allowed_cap, adjusted_notional, cap_reason = risk_engine.enforce_symbol_cap(
                symbol=intent.symbol,
                strat=intent.strategy,
                pos=snap,
                notional_value=float(intent.notional),
                last_price_fn=_last_price_safe,
                now=now,
            )
            if not allowed_cap or adjusted_notional <= 0.0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": cap_reason or "blocked_by_symbol_cap",
                        "source": "risk_engine.enforce_symbol_cap",
                    }
                )
                continue

            final_notional = float(adjusted_notional)

            # Loss-zone no-rebuy below threshold (if we have unrealized_pct wired)
            if risk_engine.is_loss_zone_norebuy_block(
                unrealized_pct=snap.unrealized_pct,
                is_entry_side=True,
            ):
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": "loss_zone_no_rebuy_below",
                        "source": "risk_engine.is_loss_zone_norebuy_block",
                    }
                )
                continue

        # ----- EXIT / TP / SL: compute notional from position if missing -------------
        else:
            qty_here = float(getattr(pm_pos, "qty", 0.0) or 0.0)
            if abs(qty_here) < 1e-10:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": "no_position_to_exit",
                        "source": "scheduler_v2",
                    }
                )
                continue

            px = _last_price_safe(intent.symbol)
            if px <= 0.0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": intent.kind,
                        "side": intent.side,
                        "reason": "no_price_for_exit",
                        "source": "scheduler_v2",
                    }
                )
                continue

            if intent.notional is not None and intent.notional > 0:
                final_notional = float(intent.notional)
            else:
                final_notional = abs(qty_here) * px  # flatten full position

        # If we reached here, we have a valid final_notional
        if final_notional <= 0:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": intent.kind,
                    "side": intent.side,
                    "reason": "non_positive_final_notional",
                    "source": "scheduler_v2",
                }
            )
            continue

                # Ignore dust for entries + TP/SL; still allow generic exits (e.g. daily_flatten)
        if final_notional < MIN_NOTIONAL_USD and intent.kind in {"entry", "scale", "take_profit", "stop_loss"}:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": intent.kind,
                    "side": intent.side,
                    "reason": f"below_min_notional:{final_notional:.4f}<{MIN_NOTIONAL_USD}",
                    "source": "scheduler_v2",
                }
            )
            continue

        action_record: Dict[str, Any] = {
            "symbol": intent.symbol,
            "strategy": intent.strategy,
            "side": side,
            "kind": intent.kind,
            "notional": final_notional,
            "reason": intent.reason,
            "dry": bool(dry),
        }

        if dry or br is None:
            action_record["status"] = "skipped_dry_run"
            actions.append(action_record)
            continue

        # ------------------------------------------------------------------
        # Send to broker via br_router.market_notional
        # ------------------------------------------------------------------
        try:
            resp = br.market_notional(
                symbol=intent.symbol,
                side=side,
                notional=final_notional,
                strategy=intent.strategy,
            )
            action_record["status"] = "sent"
            action_record["response"] = resp
        except Exception as e:
            action_record["status"] = "error"
            action_record["error"] = f"{e.__class__.__name__}: {e}"

        actions.append(action_record)

    # ------------------------------------------------------------------
    # FINAL RETURN — avoid null/None responses
    # ------------------------------------------------------------------
    return {
        "ok": True,
        "dry": bool(dry),
        "config": config_snapshot,
        "actions": actions,
        "telemetry": telemetry,
    }

# ---- New core debug endpoint) ------------------------------------------------------------        
        
@app.post("/scheduler/core_debug")
def scheduler_core_debug(payload: Dict[str, Any] = Body(default=None)):
    """
    Runs the new scheduler_core once and returns the raw OrderIntents,
    WITHOUT sending any orders to the broker. Safe for inspection.
    """
    # Reuse the same config parsing you already do in scheduler_run:
    # - resolve timeframe, symbols, strats, limit, notional, dry from
    #   env + payload
    # - load positions via _load_open_positions_from_trades(use_strategy_col=True)
    # - load risk_cfg via load_risk_config()
    # - preload contexts dict exactly like scheduler_run does

    # Pseudocode sketch (you’ll adapt from your existing scheduler_run):
    now = dt.datetime.utcnow()
    positions = _load_open_positions_from_trades(use_strategy_col=True)
    risk_cfg = load_risk_config() or {}
    contexts = { ... }  # the same structure you currently pass to StrategyBook

    cfg = SchedulerConfig(
        now=now,
        timeframe=tf,
        limit=limit,
        symbols=symbols,
        strats=strats,
        notional=notional,
        positions=positions,
        contexts=contexts,
        risk_cfg=risk_cfg,
    )

    result = run_scheduler_once(cfg)

    # Convert OrderIntents to plain dicts for JSON
    intents_as_dicts = [
        {
            "strategy": i.strategy,
            "symbol": i.symbol,
            "side": i.side,
            "kind": i.kind,
            "notional": i.notional,
            "reason": i.reason,
            "meta": i.meta,
        }
        for i in result.intents
    ]

    return {
        "intents": intents_as_dicts,
        "telemetry": result.telemetry,
    }
    
@app.post("/scheduler/core_debug_risk")
def scheduler_core_debug_risk(payload: Dict[str, Any] = Body(default=None)):
    """
    Debug-only: run scheduler_core + RiskEngine, but DO NOT send orders.

    This lets us see per-strategy OrderIntents and which ones risk caps
    or global exits would flatten or block.
    """
    payload = payload or {}

    # 1) Reuse existing helpers in scheduler_run to resolve tf, strats, symbols, etc.
    tf = str(payload.get("tf", os.getenv("SCHED_TIMEFRAME", "5Min")))
    strats_csv = str(payload.get("strats", os.getenv("SCHED_STRATS", "c1")))
    strats = [s.strip().lower() for s in strats_csv.split(",") if s.strip()]
    notional = float(payload.get("notional", os.getenv("SCHED_NOTIONAL", "40")) or 40.0)
    limit = int(payload.get("limit", os.getenv("SCHED_LIMIT", "300")) or 300)

    # Same symbols logic you already use in scheduler_run
    symbols_csv = payload.get("symbols", os.getenv("SYMBOLS", "BTC/USD,ETH/USD"))
    if isinstance(symbols_csv, str):
        symbols = [s.strip().upper() for s in symbols_csv.split(",") if s.strip()]
    else:
        symbols = [str(s).strip().upper() for s in symbols_csv or []]

    # 2) Load positions & risk_cfg exactly as scheduler_run does
    positions = _load_open_positions_from_trades(use_strategy_col=True)
    risk_cfg = load_risk_config() or {}

    # 3) Build contexts exactly like scheduler_run (reuse your existing code)
    contexts = _preload_contexts(tf=tf, symbols=symbols, limit=limit)
    # ^ if you don't have _preload_contexts yet, this is the same bar-loading
    #   logic that's currently inside scheduler_run before StrategyBook.scan.

    now = dt.datetime.utcnow()
    cfg = SchedulerConfig(
        now=now,
        timeframe=tf,
        limit=limit,
        symbols=symbols,
        strats=strats,
        notional=notional,
        positions=positions,
        contexts=contexts,
        risk_cfg=risk_cfg,
    )

    # 4) Run pure strategy logic
    sched_result = run_scheduler_once(cfg)

    # 5) Apply RiskEngine to see what would be flattened/blocked
    re_engine = RiskEngine(risk_cfg)
    intents_after_risk: List[Dict[str, Any]] = []
    for intent in sched_result.intents:
        key = (intent.symbol, intent.strategy)
        pm_pos = positions.get(key)
        snap = PositionSnapshot(
            symbol=intent.symbol,
            strategy=intent.strategy,
            qty=float(getattr(pm_pos, "qty", 0.0) or 0.0),
            avg_price=getattr(pm_pos, "avg_price", None),
            unrealized_pct=None,  # optional: compute via RiskEngine later
        )

        # example: check symbol caps for ENTRY / SCALE
        cap_reason = None
        allowed = True
        adjusted_notional = intent.notional
        if intent.kind in ("entry", "scale") and intent.notional is not None:
            def _last_price(sym: str) -> float:
                return float(_last_price_safe(sym) or 0.0)

            allowed, adjusted_notional, cap_reason = re_engine.enforce_symbol_cap(
                symbol=intent.symbol,
                strat=intent.strategy,
                pos=snap,
                notional_value=float(intent.notional),
                last_price_fn=_last_price,
                now=now,
            )

        intents_after_risk.append(
            {
                "strategy": intent.strategy,
                "symbol": intent.symbol,
                "side": intent.side,
                "kind": intent.kind,
                "original_notional": intent.notional,
                "adjusted_notional": adjusted_notional,
                "cap_allowed": allowed,
                "cap_reason": cap_reason,
                "reason": intent.reason,
                "meta": intent.meta,
            }
        )

    return {
        "intents": intents_after_risk,
        "telemetry": sched_result.telemetry,
        "risk_raw": risk_cfg,
    }

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

def _pnl__load_fills(start: Optional[str], end: Optional[str]) -> List[Fill]:
    """Load raw fills from the trades table and adapt them into Fill objects."""
    s = _pnl__parse_ts(start)
    e = _pnl__parse_ts(end)
    where_sql, params = _pnl__where(s, e)

    con = _pnl__connect()
    try:
        cur = con.cursor()
        sql = f"""
            SELECT txid, ts, symbol, side, price, volume, fee,
                   COALESCE(NULLIF(TRIM(strategy), ''), 'misc') AS strategy
            FROM trades
            {where_sql}
            ORDER BY ts ASC
        """
        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        con.close()

    fills: List[Fill] = []
    for txid, ts_val, symbol, side, price, volume, fee, strategy in rows:
        try:
            fills.append(
                Fill(
                    ts=float(ts_val or 0.0),
                    symbol=str(symbol or ""),
                    side=str(side or ""),
                    qty=float(volume or 0.0),
                    price=float(price or 0.0),
                    fee=float(fee or 0.0),
                    strategy=str(strategy or "misc"),
                    txid=str(txid or ""),
                )
            )
        except Exception:
            # Skip bad rows rather than failing the whole request
            continue
    return fills


def _pnl__price_lookup(symbol: str) -> float:
    """Safe wrapper around broker_kraken.last_price for unrealized PnL."""
    try:
        return float(broker_kraken.last_price(symbol) or 0.0)
    except Exception:
        return 0.0

def _pnl__where(start_iso: Optional[str], end_iso: Optional[str]) -> Tuple[str, List[Any]]:
    """
    Build a WHERE clause over the trades table using ISO date strings.

    start_iso / end_iso are ISO strings like '2025-11-16' or
    '2025-11-16T00:00:00' (already validated/normalized by _pnl__parse_ts).

    We store trade time in 'ts' as a Unix timestamp (seconds since epoch),
    so we compare against strftime('%s', ?) which converts the ISO string
    to epoch seconds inside SQLite.
    """
    where_parts: List[str] = []
    params: List[Any] = []

    if start_iso:
        # ts >= epoch_seconds(start_iso)
        where_parts.append("ts >= strftime('%s', ?)")
        params.append(start_iso)

    if end_iso:
        # ts <= epoch_seconds(end_iso)
        where_parts.append("ts <= strftime('%s', ?)")
        params.append(end_iso)

    where_sql = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return where_sql, params

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

def _load_open_positions_from_trades(use_strategy_col: bool = False) -> Dict[Tuple[str, str], Position]:
    """
    Load net positions from the trades/journal table, using the same DB the PnL uses.
    For positions, we treat the 'trades' table as the single source of truth
    whenever it exists, because that's where TradesHistory imports write.

    Returns a dict keyed by (symbol, strategy).
    """
    con = _db()
    try:
        table = "trades"
        try:
            cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
            names = {r[0] for r in cur.fetchall()}
            if "trades" not in names:
                # Fall back to the generic detector if no 'trades' table
                table = _pnl__detect_table(con)
        except Exception:
            table = _pnl__detect_table(con)

        return load_net_positions(con, table=table, use_strategy_col=use_strategy_col)
    finally:
        con.close()

def _position_for(sym, strat, positions):
    positions = _normalize_positions(positions)
    key = f"{sym}|{strat}"
    return positions.get(key)
    
import json

def _normalize_positions(positions):
    """
    Ensure positions is a dict mapping 'SYMBOL|STRAT' -> position dict.
    Accepts dict, JSON string, list, or None and normalizes.
    """
    if isinstance(positions, dict):
        return positions

    if positions is None:
        return {}

    # If it's JSON text
    if isinstance(positions, str):
        positions = positions.strip()
        if not positions:
            return {}
        try:
            loaded = json.loads(positions)
        except Exception:
            return {}  # bad JSON; treat as no positions

        # If the JSON is already a mapping
        if isinstance(loaded, dict):
            return loaded

        # If it's a list of position objects, build a mapping
        if isinstance(loaded, list):
            mapping = {}
            for p in loaded:
                try:
                    sym = p.get("symbol")
                    strat = p.get("strategy") or p.get("strategy_id")
                    if sym and strat:
                        key = f"{sym}|{strat}"
                        mapping[key] = p
                except Exception:
                    continue
            return mapping

        return {}

    # If it’s some other weird type, just fail-safe
    if isinstance(positions, list):
        mapping = {}
        for p in positions:
            try:
                sym = p.get("symbol")
                strat = p.get("strategy") or p.get("strategy_id")
                if sym and strat:
                    key = f"{sym}|{strat}"
                    mapping[key] = p
            except Exception:
                continue
        return mapping

    return {}

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

# ---- BEGIN PATCH v2.0.0: position-aware PnL engine endpoints -------------------------------

@app.get("/pnl2/summary")
def pnl2_summary(start: Optional[str] = None, end: Optional[str] = None):
    """Position-aware PnL summary based on raw fills in the trades table."""
    try:
        fills = _pnl__load_fills(start, end)
        engine = PositionEngine()
        engine.process_fills(fills)
        snap = engine.snapshot(price_lookup=_pnl__price_lookup)
        return {"ok": True, **snap, "start": _pnl__parse_ts(start), "end": _pnl__parse_ts(end)}
    except Exception as e:
        return {"ok": False, "error": f"/pnl2/summary failed: {e.__class__.__name__}: {e}"}


@app.get("/pnl2/by_strategy")
def pnl2_by_strategy(start: Optional[str] = None, end: Optional[str] = None):
    """Position-aware PnL grouped by strategy."""
    try:
        fills = _pnl__load_fills(start, end)
        engine = PositionEngine()
        engine.process_fills(fills)
        snap = engine.snapshot(price_lookup=_pnl__price_lookup)
        rows = []
        for strat, row in snap["per_strategy"].items():
            r = {"strategy": strat}
            r.update(row)
            rows.append(r)
        return {
            "ok": True,
            "table": "trades",
            "start": _pnl__parse_ts(start),
            "end": _pnl__parse_ts(end),
            "grouping": "strategy",
            "count": len(rows),
            "rows": rows,
        }
    except Exception as e:
        return {"ok": False, "error": f"/pnl2/by_strategy failed: {e.__class__.__name__}: {e}"}


@app.get("/pnl2/by_symbol")
def pnl2_by_symbol(start: Optional[str] = None, end: Optional[str] = None):
    """Position-aware PnL grouped by symbol."""
    try:
        fills = _pnl__load_fills(start, end)
        engine = PositionEngine()
        engine.process_fills(fills)
        snap = engine.snapshot(price_lookup=_pnl__price_lookup)
        rows = []
        for sym, row in snap["per_symbol"].items():
            r = {"symbol": sym}
            r.update(row)
            rows.append(r)
        return {
            "ok": True,
            "table": "trades",
            "start": _pnl__parse_ts(start),
            "end": _pnl__parse_ts(end),
            "grouping": "symbol",
            "count": len(rows),
            "rows": rows,
        }
    except Exception as e:
        return {"ok": False, "error": f"/pnl2/by_symbol failed: {e.__class__.__name__}: {e}"}

# ---- END PATCH v2.0.0 -----------------------------------------------------------------------