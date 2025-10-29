"""
crypto-system-api (app.py) — v2.3.6
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

# Routes:
#  - /
#  - /health
#  - /routes
#  - /dashboard
#  - /policy
#  - /config
#  - /debug/config
#  - /journal
#  - /journal/attach
#  - /fills
#  - /journal/backfill
#  - /journal/sync
#  - /advisor/daily
#  - /advisor/apply
#  - /journal/counts
#  - /journal/enrich
#  - /price/{base}/{quote}
#  - /debug/log/test
#  - /debug/kraken
#  - /debug/db
#  - /debug/kraken/trades
#  - /pnl/summary
#  - /kpis
#  - /scheduler/status
#  - /scheduler/start
#  - /scheduler/stop
#  - /scheduler/run

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
    """
    Pull trades from Kraken TradesHistory and upsert into sqlite.

    Payload:
      - since_hours: int (default 72)
      - limit: int (default 50000) maximum rows to write this call
      - dry_run: bool (optional) if true, do not write

    Behavior:
      - Paginates with 'ofs' until Kraken's 'count' reached or 'limit' cap hit
      - Upsert on txid; preserves existing 'strategy' if already labeled
      - Stores: txid, ts, symbol (pair), side (type), price, volume (vol), fee, raw (json)
    """
    import time, json as _json
    dry_run = bool(payload.get("dry_run", False))
    since_hours = int(payload.get("since_hours", 72) or 72)
    hard_limit = int(payload.get("limit", 50000) or 50000)

    key, sec, *_ = _kraken_creds()
    if not (key and sec):
        return {"ok": False, "error": "missing_credentials"}

    start_ts = int(time.time() - since_hours * 3600)
    ofs = 0
    pulled = 0
    pages = 0
    kraken_count = None

    # Accumulate and write in batches
    batch = []
    wrote_rows = 0

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
        -- keep existing strategy if present; otherwise take new one (which is NULL for sync)
        strategy= COALESCE(trades.strategy, excluded.strategy),
        raw     = excluded.raw
    """

    def flush_batch():
        nonlocal batch, wrote_rows
        if dry_run or not batch:
            batch.clear()
            return
        conn = _db(); cur = conn.cursor()
        cur.executemany(UPSERT_SQL, batch)
        conn.commit()
        wrote_rows += len(batch)
        conn.close()
        batch.clear()

    while True:
        try:
            resp = kraken_private("TradesHistory", {"start": start_ts, "ofs": ofs}, key, sec)
            res = resp.get("result") or {}
            trades = res.get("trades") or {}
            kraken_count = res.get("count", kraken_count)
            keys = list(trades.keys())
        except Exception as e:
            log.warning("TradesHistory page error ofs=%s: %s", ofs, e)
            break

        if not keys:
            break

        pages += 1
        pulled += len(keys)

        for txid in keys:
            t = trades.get(txid) or {}
            # Map fields with safe coercion
            try: ts = float(t.get("time")) if t.get("time") is not None else None
            except: ts = None
            symbol = t.get("pair")
            side   = t.get("type")
            try: price = float(t.get("price")) if t.get("price") is not None else None
            except: price = None
            try: volume = float(t.get("vol")) if t.get("vol") is not None else None
            except: volume = None
            try: fee    = float(t.get("fee")) if t.get("fee") is not None else None
            except: fee = None

            raw = _json.dumps(t, separators=(",", ":"), ensure_ascii=False)
            # We do NOT set a strategy during sync (leave as None so enrich can label)
            batch.append((str(txid), ts, symbol, side, price, volume, fee, None, raw))

            # Flush in chunks and respect limit
            if len(batch) >= 1000:
                flush_batch()
                if hard_limit and wrote_rows >= hard_limit:
                    break

        if hard_limit and wrote_rows >= hard_limit:
            break

        ofs += len(keys)
        # stop if we've paged through everything Kraken says exists
        if kraken_count is not None and ofs >= kraken_count:
            break
        # safety guard for runaway pagination
        if pages > 200:
            break

    # final flush
    flush_batch()

    # Count rows post-write for sanity
    try:
        conn = _db(); cur = conn.cursor()
        total_rows = cur.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        conn.close()
    except Exception as e:
        total_rows = None
        log.warning("journal_sync count error: %s", e)

    return {
        "ok": True,
        "dry_run": dry_run,
        "count": wrote_rows,
        "debug": {
            "creds_present": True,
            "since_hours": since_hours,
            "start_ts": start_ts,
            "start_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_ts)),
            "pages": pages,
            "pulled": pulled,
            "kraken_count": kraken_count,
            "hard_limit": hard_limit,
            "post_total_rows": total_rows
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
    """
    Label unlabeled trades with strategy using Kraken metadata + optional rules.
    - Authoritative path: (raw.ordertxid if present) OR QueryTrades -> ordertxid
    - Then QueryOrdersInfo -> infer strategy via userref/descr
    - Fallback rules: whitelist.json + windows.json (optional)
    - Safe: dry_run support; preserves existing non-empty strategies
    """
    import json, re as _re, datetime as _dt
    import pytz

    dry_run     = bool(payload.get("dry_run", False))
    batch_size  = int(payload.get("batch_size", 40) or 40)
    max_rows    = int(payload.get("max_rows", 5000) or 5000)
    apply_rules = bool(payload.get("apply_rules", True))

    # 1) load unlabeled trades
    conn = _db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT txid, ts, symbol, raw
            FROM trades
            WHERE strategy IS NULL OR TRIM(strategy)=''
            LIMIT ?
        """, (max_rows,))
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
    trade_ids  = []
    txid_to_order = {}

    for txid, ts, symbol, raw in rows:
        txid = str(txid)
        meta = _j(raw)
        trade_info[txid] = {"ts": ts, "symbol": symbol, "raw": meta}
        trade_ids.append(txid)
        # Some Kraken trade rows already contain ordertxid in the trade body:
        otx = meta.get("ordertxid")
        if isinstance(otx, list): 
            otx = otx[0] if otx else None
        if otx:
            txid_to_order[txid] = str(otx)

    log.info("enrich: scanned=%d unlabeled=%d with_raw_ordertxid=%d", scanned, len(trade_ids), len(txid_to_order))

    key, sec, *_ = _kraken_creds()

    # 2) For trades missing ordertxid, fetch via QueryTrades
    if key and sec:
        missing = [t for t in trade_ids if t not in txid_to_order]
        for i in range(0, len(missing), batch_size):
            chunk = missing[i:i+batch_size]
            try:
                resp = kraken_private("QueryTrades", {"txid": ",".join(chunk)}, key, sec)
                res = (resp.get("result") or {}) if isinstance(resp, dict) else {}
                for t in chunk:
                    m = res.get(t) or {}
                    otx = m.get("ordertxid")
                    if isinstance(otx, list): otx = otx[0] if otx else None
                    if otx:
                        txid_to_order[t] = str(otx)
            except Exception as e:
                log.warning("QueryTrades failed for %d txids: %s", len(chunk), e)
    else:
        log.warning("enrich: missing creds; skipping QueryTrades/QueryOrdersInfo")

    order_ids = sorted(set(txid_to_order.values()))
    orders_meta = {}

    # 3) QueryOrdersInfo -> infer strategy from userref / descr
    if key and sec and order_ids:
        for i in range(0, len(order_ids), batch_size):
            chunk = order_ids[i:i+batch_size]
            try:
                resp = kraken_private("QueryOrdersInfo", {"txid": ",".join(chunk)}, key, sec)
                res = (resp.get("result") or {}) if isinstance(resp, dict) else {}
                orders_meta.update(res)
            except Exception as e:
                log.warning("QueryOrdersInfo failed for %d orders: %s", len(chunk), e)

    def infer_from_order(o: Dict[str, Any]):
        if not isinstance(o, dict): return None
        u = o.get("userref")
        # userref can be an int or str like "c3"
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

    # 4) optional fallback rules
    whitelist, windows = {}, {}
    try: whitelist = json.load(open("whitelist.json", "r", encoding="utf-8"))
    except Exception: pass
    try: windows = json.load(open("windows.json", "r", encoding="utf-8"))
    except Exception: pass

    tzname = os.getenv("TZ", "America/Chicago")
    try: tz = pytz.timezone(tzname)
    except Exception: 
        tz = pytz.UTC

    def allowed_by_rules(strat, symbol, ts):
        syms = whitelist.get(strat)
        if syms and ("*" not in syms) and (symbol not in syms): return False
        win = windows.get(strat) or {}
        days  = set([d[:3].title() for d in (win.get("days") or [])])
        hours = set(win.get("hours") or [])
        if not days and not hours: return True
        try:
            t = _dt.datetime.fromtimestamp(float(ts), tz)
            if days  and t.strftime("%a") not in days: return False
            if hours and t.hour not in set(int(h) for h in hours): return False
            return True
        except Exception:
            return False

    # 5) decide labels
    to_update = {}
    api_labeled = rules_labeled = ambiguous = missing_order = 0

    for txid, oid in txid_to_order.items():
        strat = infer_from_order(orders_meta.get(oid) or {})
        if strat:
            to_update[txid] = strat
            api_labeled += 1
        else:
            missing_order += 1

    if apply_rules and (whitelist or windows):
        all_strats = sorted(set(list(whitelist.keys()) + list(windows.keys())))
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
        return {"ok": True, "dry_run": True, "scanned": scanned, "orders_checked": len(order_ids),
                "to_update_count": len(to_update), "sample_updates": sample}

    # 6) write updates (preserve existing non-empty strategy)
    updated = 0
    if to_update:
        conn = _db(); cur = conn.cursor()
        for txid, strat in to_update.items():
            try:
                cur.execute("""
                    UPDATE trades
                       SET strategy = ?
                     WHERE txid     = ?
                       AND (strategy IS NULL OR TRIM(strategy)='')
                """, (str(strat), str(txid)))
                updated += cur.rowcount
            except Exception as e:
                log.warning("update failed txid=%s: %s", txid, e)
        conn.commit(); conn.close()

    return {
        "ok": True,
        "scanned": scanned,
        "orders_checked": len(order_ids),
        "updated": updated,
        "api_labeled": api_labeled,
        "rules_labeled": rules_labeled,
        "ambiguous": ambiguous,
        "missing_order": missing_order,
        "apply_rules": apply_rules,
        "batch_size": batch_size,
        "max_rows": max_rows,
    }

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

@app.get("/pnl/summary")
def pnl_summary():
    """
    Compute realized/unrealized, fees, equity by strategy and by symbol.
    Safe on empty/unlabeled journals.
    """
    import json
    conn = _db(); cur = conn.cursor()

    # Pull minimal fields
    try:
        cur.execute("SELECT symbol, side, price, volume, fee, strategy, raw FROM trades")
        rows = cur.fetchall()
    except Exception as e:
        conn.close()
        return {"ok": False, "error": f"db_read: {e}"}

    conn.close()

    # group by symbol FIFO to estimate realized P&L; unrealized = 0 for journal-only view
    from collections import defaultdict, deque

    lots = defaultdict(deque)  # symbol -> queue of (qty, price)
    realized = defaultdict(float)
    fees     = defaultdict(float)

    def add_fee(sym, f):
        try: fees[sym] += float(f or 0)
        except: pass

    for symbol, side, price, volume, fee, strategy, raw in rows:
        if not symbol or price is None or volume is None:
            add_fee(symbol or "UNKNOWN", fee)
            continue
        qty  = float(volume)
        prc  = float(price)
        sym  = str(symbol)

        # side: 'buy' adds to inventory; 'sell' reduces FIFO and realizes P&L
        if (side or "").lower() == "buy":
            lots[sym].append([qty, prc])
        elif (side or "").lower() == "sell":
            remain = qty
            while remain > 1e-12 and lots[sym]:
                lot_qty, lot_price = lots[sym][0]
                take = min(remain, lot_qty)
                realized[sym] += (prc - lot_price) * take
                lot_qty -= take
                remain  -= take
                if lot_qty <= 1e-12:
                    lots[sym].popleft()
                else:
                    lots[sym][0][0] = lot_qty
        add_fee(sym, fee)

    # pack totals
    total_realized = sum(realized.values())
    total_fees     = sum(fees.values())
    total_unreal   = 0.0
    total_equity   = total_realized + total_unreal - total_fees

    # per strategy = aggregate over rows (using realized-by-symbol split)
    # If you want exact realized per strategy, you’ll need per-trade pairing; for now, split realized to 'misc' if unlabeled.
    per_strategy = defaultdict(lambda: {"realized":0.0,"unrealized":0.0,"fees":0.0,"equity":0.0})
    per_symbol   = []

    # fees per symbol already tracked; distribute to strategy buckets by row fees
    for symbol, side, price, volume, fee, strategy, raw in rows:
        strat = (str(strategy).strip() if strategy else "misc")
        per_strategy[strat]["fees"] += float(fee or 0)

    for sym in sorted(set([r[0] for r in rows if r[0]])):
        r = float(realized.get(sym, 0.0))
        f = float(fees.get(sym, 0.0))
        per_symbol.append({"symbol": sym, "realized": r, "unrealized": 0.0, "fees": f, "equity": r - f})

    # Put all realized into 'misc' unless you later compute per-strategy pairing
    per_strategy["misc"]["realized"]   += total_realized
    per_strategy["misc"]["unrealized"] += 0.0
    per_strategy["misc"]["equity"]     += total_realized - per_strategy["misc"]["fees"]

    return {
        "total": {
            "realized": total_realized,
            "unrealized": total_unreal,
            "fees": total_fees,
            "equity": total_equity
        },
        "per_strategy": [
            {"strategy": k, **v} for k, v in sorted(per_strategy.items(), key=lambda x: x[0])
        ],
        "per_symbol": per_symbol,
        "ok": True,
        "counts": {
            "journal_rows": len(rows)
        }
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

@app.post("/journal/enrich")
def journal_enrich(payload: Dict[str, Any] = Body(...)):
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
