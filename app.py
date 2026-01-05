"""
crypto-system-api (app.py) — v2.4.2
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
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from symbol_map import KRAKEN_PAIR_MAP, to_kraken
from position_engine import PositionEngine, Fill
from position_manager import load_net_positions, Position
from risk_engine import RiskEngine
from strategy_api import PositionSnapshot
from strategy_api import PositionSnapshot
from scheduler_core import SchedulerConfig, SchedulerResult, StrategyBook, ScanRequest, ScanResult,run_scheduler_once
from risk_engine import RiskEngine
from fastapi import Body, FastAPI, HTTPException, Query
from advisor_v2 import advisor_summary


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
logger = log  # alias for older patches
log.info("Logging initialized at level %s", LOG_LEVEL)
__version__ = '2.3.4'



from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# --------------------------------------------------------------------------------------
# Version / Logging
# --------------------------------------------------------------------------------------

APP_VERSION = "3.0.1"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("crypto-system-api")


# ------------------------------------------------------------------------------
# Scheduler anti-churn latch (in-memory, updates immediately on send)
# This avoids relying on journal/trades DB latency to enforce cooldowns.
# ------------------------------------------------------------------------------
_LAST_ACTION_LATCH = {}  # (symbol, strategy) -> {ts, side, kind}
_LAST_ACTION_LATCH_LOCK = threading.Lock()

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
# Whitelist config loader (policy_config/whitelist.json)
# --------------------------------------------------------------------------------------
def load_whitelist_config() -> dict:
    """
    Load per-strategy whitelist mapping from policy_config/whitelist.json.

    Expected format:
      { "c1": ["BTC/USD", "ETH/USD"], "c2": ["SOL/USD", ...], ... }

    Returns {} on any error so callers can safely default.
    """
    try:
        cfg_dir = os.getenv("POLICY_CFG_DIR", "policy_config")
        path = Path(cfg_dir) / "whitelist.json"
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return {}
        # Normalize: lower strategy keys, upper symbol values
        out = {}
        for k, v in raw.items():
            if not k:
                continue
            strat = str(k).strip().lower()
            if not strat:
                continue
            if isinstance(v, (list, tuple)):
                syms = [str(s).strip().upper() for s in v if str(s).strip()]
            else:
                syms = []
            out[strat] = sorted(set(syms))
        return out
    except Exception as e:
        log.warning("load_whitelist_config failed: %s", e)
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
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DATA_DIR / "journal.db"
# --- Journal sync watermark cursor (persisted on disk) ---
JOURNAL_CURSOR_PATH = DATA_DIR / "journal_cursor.json"
JOURNAL_SAFETY_BUFFER_SECONDS = int(os.getenv("JOURNAL_SAFETY_BUFFER_SECONDS", "3600"))
# Reconcile tolerances (avoid false mismatches from Kraken rounding/precision)
LEDGER_FEE_EPS = float(os.getenv("LEDGER_FEE_EPS", "0.0002"))
LEDGER_USD_EPS = float(os.getenv("LEDGER_USD_EPS", "0.0002"))


def _read_journal_cursor() -> dict:
    """Best-effort read of journal cursor file. Returns {} on any error."""
    try:
        if JOURNAL_CURSOR_PATH.exists():
            return json.loads(JOURNAL_CURSOR_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _write_journal_cursor(last_seen_ts: int, extra: dict | None = None) -> None:
    """Atomic-ish best-effort write of journal cursor file. Never raises."""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_seen_ts": int(last_seen_ts),
            "updated_at": int(time.time()),
            "updated_iso": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }
        if extra:
            try:
                payload.update(extra)
            except Exception:
                pass
        tmp = JOURNAL_CURSOR_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        tmp.replace(JOURNAL_CURSOR_PATH)
    except Exception:
        pass

# ----------------------------------------------------------------------
# Journal v2: append-only JSONL log written directly from scheduler_v2
# ----------------------------------------------------------------------
JOURNAL_V2_PATH = DATA_DIR / "journal_v2.jsonl"
JOURNAL_V2_MAX_BYTES = int(os.getenv("JOURNAL_V2_MAX_BYTES", str(5 * 1024 * 1024)))  # 5 MB default
_JOURNAL_V2_LOCK = threading.Lock()


# --- Ledgers sync watermark cursor (persisted on disk) ---
LEDGER_CURSOR_PATH = DATA_DIR / "ledger_cursor.json"

def _read_ledger_cursor() -> dict:
    """Best-effort read of ledger cursor file. Returns {} on any error."""
    try:
        if LEDGER_CURSOR_PATH.exists():
            return json.loads(LEDGER_CURSOR_PATH.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _write_ledger_cursor(last_seen_ts: int, extra: dict | None = None) -> None:
    """Atomic-ish best-effort write of ledger cursor file. Never raises."""
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        payload = {
            "last_seen_ts": int(last_seen_ts),
            "updated_at": int(time.time()),
            "updated_iso": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        }
        if extra:
            try:
                payload.update(extra)
            except Exception:
                pass
        tmp = LEDGER_CURSOR_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        os.replace(str(tmp), str(LEDGER_CURSOR_PATH))
    except Exception:
        pass
def append_journal_v2(event: Dict[str, Any]) -> None:
    """
    Append a single JSON event to journal_v2.jsonl with very defensive
    error handling. Rotates the file when it grows beyond JOURNAL_V2_MAX_BYTES.

    This MUST NEVER raise; failures are silently ignored so the scheduler
    and API continue to run even if disk is unhappy.
    """
    # Best-effort ensure data dir exists
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    path = JOURNAL_V2_PATH

    # Size-based rotation
    try:
        if path.exists() and path.stat().st_size > JOURNAL_V2_MAX_BYTES:
            ts = int(time.time())
            rotated = path.with_name(f"{path.name}.{ts}")
            try:
                path.rename(rotated)
            except Exception:
                # If rename fails, just keep appending to current file
                pass
    except Exception:
        # If stat/exists fails, ignore and try writing anyway
        pass

    # Serialize safely
    try:
        line = json.dumps(event, sort_keys=False, default=str)
    except Exception:
        # Never let bad event data kill the logger
        return

    # Append with a lock
    try:
        with _JOURNAL_V2_LOCK:
            with path.open("a", encoding="utf-8") as f:
                f.write(line)
                f.write("\n")
    except Exception:
        # Swallow all I/O errors
        return

def _load_attrib_from_journal_v2(max_lines: int = 250_000) -> Dict[str, Dict[str, str]]:
    """
    Build a mapping: ordertxid -> {"intent_id": ..., "strategy": ...}
    from append-only journal_v2.jsonl. Safe + best-effort.
    """
    path = JOURNAL_V2_PATH
    if not path.exists():
        return {}

    m: Dict[str, Dict[str, str]] = {}
    try:
        with path.open("r", encoding="utf-8") as fh:
            # tail-ish behavior: read all, but cap lines to avoid OOM on huge files
            lines = fh.readlines()[-max_lines:]
        for line in lines:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except Exception:
                continue

            # Common fields we expect in events (top-level OR inside response/result)
            ordertxid = ev.get("ordertxid") or ev.get("order_txid") or ev.get("orderId") or ev.get("txid")
            userref   = ev.get("userref")

            resp = ev.get("response")
            if (not ordertxid or userref is None) and isinstance(resp, dict):
                # try inside response itself
                otx2, ur2 = _extract_ordertxid_userref_from_resp(resp)
                if not ordertxid and otx2:
                    ordertxid = otx2
                if userref is None and ur2 is not None:
                    userref = ur2

            intent_id = ev.get("intent_id")
            strategy  = ev.get("strategy")

            # If not present at top-level, try to extract from broker response payload
            if not ordertxid:
                resp = ev.get("response")
                try:
                    if isinstance(resp, dict):
                        ordertxid = resp.get("ordertxid") or resp.get("order_txid") or resp.get("orderId")
                        if not ordertxid:
                            r = resp.get("result") if isinstance(resp.get("result"), dict) else None
                            tx = (r.get("txid") if r else None)
                            if isinstance(tx, list) and tx:
                                ordertxid = tx[0]
                            elif isinstance(tx, str):
                                ordertxid = tx
                        if not ordertxid:
                            tx = resp.get("txid")
                            if isinstance(tx, list) and tx:
                                ordertxid = tx[0]
                            elif isinstance(tx, str):
                                ordertxid = tx
                except Exception:
                    pass

            if ordertxid and (intent_id or strategy):
                rec = m.setdefault(str(ordertxid), {})
                if intent_id and "intent_id" not in rec:
                    rec["intent_id"] = str(intent_id)
                if strategy and "strategy" not in rec:
                    rec["strategy"] = str(strategy)
    except Exception as e:
        log.warning("attrib load from journal_v2 failed: %s", e)

    return m


def reconcile_trade_attribution() -> Dict[str, Any]:
    """
    Apply journal_v2 attribution to trades table using ordertxid.
    Safe, idempotent, and fast enough for a sync call.
    """
    attrib = _load_attrib_from_journal_v2()

    # --- Step C: symbol normalization (canonical) ---
    # Legacy runs may have written non-canonical symbols like "XETHZ/USD" or "XLTCZ/USD".
    # Normalize stored trades.symbol to canonical UI form (e.g., "ETH/USD") using Kraken pair codes.
    try:
        conn = _db()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT DISTINCT pair, symbol
            FROM trades
            WHERE symbol IS NOT NULL
              AND symbol LIKE '%/%'
              AND (symbol LIKE 'X%/%' OR symbol LIKE 'Z%/%' OR symbol LIKE '%Z/%')
            LIMIT 5000
            """
        )
        fixes = []
        for pair_raw, sym_raw in cur.fetchall():
            pair_raw = pair_raw or ""
            sym_raw  = sym_raw or ""
            canon = from_kraken_pair_to_app(pair_raw) or from_kraken_pair_to_app(sym_raw) or sym_raw
            if canon and canon != sym_raw:
                fixes.append((canon, sym_raw))
        if fixes:
            cur.executemany("UPDATE trades SET symbol = ? WHERE symbol = ?", fixes)
            conn.commit()
        try:
            conn.close()
        except Exception:
            pass
    except Exception:
        pass

    # Best-effort fallback: if journal_v2 attribution is missing (or journal_v2 file isn't present),
    # build an attribution map from existing trades that already have strategy/intent_id.
    # This lets us backfill newly-pulled Kraken trades that arrive without attribution.
    if not attrib:
        try:
            conn = _db()
            cur = conn.cursor()
            cur.execute("""
                SELECT ordertxid, intent_id, strategy
                FROM trades
                WHERE ordertxid IS NOT NULL
                  AND (intent_id IS NOT NULL OR strategy IS NOT NULL)
            """)
            for otx, iid, strat in cur.fetchall():
                if not otx:
                    continue
                rec = attrib.setdefault(str(otx), {})
                if iid and "intent_id" not in rec:
                    rec["intent_id"] = str(iid)
                if strat and "strategy" not in rec:
                    rec["strategy"] = str(strat)
        except Exception as e:
            log.warning("attrib fallback from trades table failed: %s", e)
        finally:
            try:
                conn.close()
            except Exception:
                pass

    if not attrib:
        return {"ok": True, "updated": 0, "note": "no_attrib_found"}

    conn = _db()
    cur = conn.cursor()
    updated = 0

    try:
        for ordertxid, rec in attrib.items():
            intent_id = rec.get("intent_id")
            strategy  = rec.get("strategy")

            if not (intent_id or strategy):
                continue

            cur.execute("""
                UPDATE trades
                SET
                    intent_id = COALESCE(intent_id, ?),
                    strategy  = COALESCE(strategy,  ?)
                WHERE ordertxid = ?
            """, (intent_id, strategy, ordertxid))

            updated += cur.rowcount

        conn.commit()
    finally:
        try:
            conn.close()
        except Exception:
            pass

    return {"ok": True, "updated": int(updated), "attrib_keys": int(len(attrib))}


# Telemetry JSONL log (one row per scheduler event)
TELEMETRY_PATH = DATA_DIR / "telemetry_v2.jsonl"

def _append_telemetry_rows(rows: List[Dict[str, Any]]) -> None:
    """
    Append scheduler_v2 telemetry rows to a JSONL file on disk.

    Each row is a plain dict. We add a UTC timestamp if not already present.
    """
    if not rows:
        return

    import time
    ts = time.time()

    try:
        TELEMETRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(TELEMETRY_PATH, "a", encoding="utf-8") as fh:
            for r in rows:
                if "ts" not in r:
                    r = dict(r)
                    r["ts"] = ts
                fh.write(json.dumps(r) + "\n")
    except Exception as e:
        # Telemetry logging must never break the scheduler
        log.warning("failed to append telemetry rows: %s", e)
        
def _extract_ordertxid_userref_from_resp(resp: Any) -> Tuple[Optional[str], Optional[int]]:
    """
    Best-effort: pull ordertxid + userref out of br_router.market_notional response.
    Handles shapes like:
      - {"ordertxid": "...", "userref": 123}
      - {"txid": "..."} or {"txid": ["..."]}
      - {"result": {"txid": ["..."]}}
      - {"result": {"ordertxid": "..."}}
    """
    if resp is None:
        return (None, None)

    # Sometimes response is not a dict
    if not isinstance(resp, dict):
        return (None, None)

    # candidates to search
    cands = [resp]
    # br_router.market_notional commonly wraps broker response under 'order'
    if isinstance(resp.get("order"), dict):
        cands.append(resp["order"])
        if isinstance(resp["order"].get("result"), dict):
            cands.append(resp["order"]["result"])
    if isinstance(resp.get("result"), dict):
        cands.append(resp["result"])
    if isinstance(resp.get("data"), dict):
        cands.append(resp["data"])

    ordertxid: Optional[str] = None
    userref: Optional[int] = None

    for d in cands:
        if not isinstance(d, dict):
            continue

        # Kraken often returns "txid" as the order tx id in order placement calls
        otx = d.get("ordertxid") or d.get("order_txid") or d.get("txid") or d.get("orderId")
        if isinstance(otx, list) and otx:
            otx = otx[0]
        if otx and ordertxid is None:
            ordertxid = str(otx)

        ur = d.get("userref") or d.get("user_ref")
        if ur is not None and userref is None:
            try:
                userref = int(ur)
            except Exception:
                userref = None

    return (ordertxid, userref)


def _intent_id(intent: Any) -> Optional[str]:
    """Read the stable intent id you set in intent.meta."""
    try:
        meta = getattr(intent, "meta", None)
        if isinstance(meta, dict):
            v = meta.get("intent_id")
            return str(v) if v else None
    except Exception:
        pass
    return None


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

def _get_env_first(*names: str, return_name: bool = False):
    """Return the first non-empty env var among `names`.

    If return_name=False (default): returns the value or None.
    If return_name=True: returns a tuple (value_or_None, env_name_used_or_empty).
    """
    for n in names:
        v = os.getenv(n)
        if v:
            return (v, n) if return_name else v
    return (None, "") if return_name else None


def _kraken_creds():
    key, key_name = _get_env_first(
        "KRAKEN_API_KEY", "KRAKEN_KEY", "KRAKEN_KEY_1", "KRAKEN_API_KEY_1",
        return_name=True
    )
    sec, sec_name = _get_env_first(
        "KRAKEN_API_SECRET", "KRAKEN_SECRET", "KRAKEN_SECRET_1", "KRAKEN_API_SECRET_1",
        return_name=True
    )

    key = key or ""
    sec = sec or ""

    used = f"{key_name or '∅'}/{sec_name or '∅'}"
    try:
        log.info(
            f"_kraken_creds: using pair={key_name}/{sec_name}; "
            f"key_len={len(key) if key else 0} sec_len={len(sec) if sec else 0}"
        )
    except Exception:
        pass
    return key, sec, key_name, sec_name


# ------------------------------------------------------------------------------
# Symbol normalization: Kraken pair -> app UI symbol
# ------------------------------------------------------------------------------
_REV_KRAKEN_PAIR_MAP = {v.upper(): k for k, v in KRAKEN_PAIR_MAP.items()}

def from_kraken_pair_to_app(pair_raw: str) -> str:
    """
    Convert Kraken pair strings into canonical UI symbols (e.g. 'BTC/USD').

    Handles:
      - configured altname mappings (symbol_map.KRAKEN_PAIR_MAP) when available
      - common Kraken pair codes like 'XBTUSD', 'ETHUSD'
      - Kraken "wrapped" pair codes like 'XXBTZUSD', 'XETHZUSD', 'XLTCZUSD', 'XXRPZUSD', etc.
      - previously-buggy forms like 'XETHZ/USD', 'XLTCZ/USD' (older fallback artifacts)

    This function is intentionally defensive: it never throws.
    """
    if not pair_raw:
        return ""

    s = str(pair_raw).upper().strip()

    # Helper: clean Kraken asset codes.
    def _kraken_clean_base(a: str) -> str:
        a = (a or "").upper().strip()
        if not a:
            return ""

        # Strip common leading wrappers: XXBT -> XBT, XETH -> ETH, ZUSD -> USD, etc.
        while len(a) >= 4 and a[0] in "XZ" and a[1] in "XZ":
            a = a[1:]

        # Strip single leading wrapper if present (XETH -> ETH, XLTC -> LTC, XXRP -> XRP)
        if len(a) >= 4 and a[0] in "XZ":
            a = a[1:]

        # Some wrapped bases include a trailing Z (XETHZ, XLTCZ, XXBTZ, XXRPZ)
        if len(a) >= 4 and a.endswith("Z"):
            a = a[:-1]

        # Canonicalize edge-cases
        if a == "XBT":
            a = "BTC"
        if a == "RP":
            a = "XRP"

        return a

    # Normalize any legacy artifact that already contains a slash, like 'XETHZ/USD'
    if "/" in s:
        try:
            base_raw, quote_raw = s.split("/", 1)
            base = base_raw.strip()
            quote = quote_raw.strip()
            if quote == "USD":
                base = _kraken_clean_base(base)
                if base:
                    return f"{base}/USD"
        except Exception:
            pass

    # 1) Exact match from our configured map (recommended pairs)
    try:
        if s in _REV_KRAKEN_PAIR_MAP:
            return _REV_KRAKEN_PAIR_MAP[s]
    except Exception:
        # mapping may not be loaded in some minimal configs
        pass

    # 2) Wrapped USD pairs often end with 'ZUSD' (e.g. XXBTZUSD, XETHZUSD)
    if s.endswith("ZUSD") and len(s) > 4:
        base_raw = s[:-4]
        base = _kraken_clean_base(base_raw)
        if base:
            return f"{base}/USD"

    # 3) Simple USD pairs end with 'USD' (e.g. XBTUSD, ETHUSD, ADAUSD)
    if s.endswith("USD") and len(s) > 3:
        base_raw = s[:-3]
        base = _kraken_clean_base(base_raw)
        if base:
            return f"{base}/USD"

    # 4) Fallback – just return the raw pair if we don't know it
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

# --- Kraken Nonce Guard (patch #2.4) ----------------------------------------
# Kraken private endpoints require a strictly increasing nonce per API key.
# Time-based nonces can collide during fast loops or concurrent requests, causing:
#   EAPI:Invalid nonce
#
# We serialize private calls and persist the last nonce to DATA_DIR so restarts
# can't regress the sequence.

_KRAKEN_NONCE_LOCK = threading.Lock()
_KRAKEN_NONCE_FILE = DATA_DIR / "kraken_nonce.json"
_KRAKEN_NONCE_STATE = {"last": 0}

def _load_kraken_nonce_state() -> None:
    try:
        if _KRAKEN_NONCE_FILE.exists():
            obj = json.loads(_KRAKEN_NONCE_FILE.read_text(encoding="utf-8") or "{}")
            _KRAKEN_NONCE_STATE["last"] = int(obj.get("last", 0) or 0)
    except Exception:
        # best-effort
        pass

def _save_kraken_nonce_state(last_nonce: int) -> None:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = _KRAKEN_NONCE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({"last": int(last_nonce), "updated_at": int(time.time())}), encoding="utf-8")
        os.replace(tmp, _KRAKEN_NONCE_FILE)
    except Exception:
        pass

def _next_kraken_nonce() -> int:
    # Must be called under _KRAKEN_NONCE_LOCK
    now = int(time.time() * 1000)
    last = int(_KRAKEN_NONCE_STATE.get("last", 0) or 0)
    n = max(now, last + 1)
    _KRAKEN_NONCE_STATE["last"] = n
    _save_kraken_nonce_state(n)
    return n

# initialize nonce state on import
_load_kraken_nonce_state()

def kraken_private(method: str, data: Dict[str, Any], key: str, secret_b64: str) -> Dict[str, Any]:
    """Kraken private API call with nonce serialization + retry on Invalid nonce."""
    path = f"/0/private/{method}"
    url = f"{KRAKEN_API_BASE}{path}"
    payload = dict(data) if data else {}

    # Serialize requests to ensure monotonic nonce ordering per key.
    with _KRAKEN_NONCE_LOCK:
        # Small bounded retry for transient Invalid nonce responses.
        for attempt in range(6):
            payload["nonce"] = _next_kraken_nonce()
            headers = {
                "API-Key": key,
                "API-Sign": _kraken_sign(path, payload, secret_b64),
                "Content-Type": "application/x-www-form-urlencoded",
            }

            r = requests.post(url, headers=headers, data=payload, timeout=30)
            r.raise_for_status()
            out = r.json()

            errs = out.get("error") or []
            if any("Invalid nonce" in str(e) for e in errs):
                # backoff slightly and retry with a bumped nonce
                time.sleep(0.6 + 0.4 * attempt)
                continue

            return out

    # If we got here, we exhausted retries while still seeing Invalid nonce.
    return {"error": ["EAPI:Invalid nonce (retries exhausted)"], "result": None}

# --------------------------------------------------------------------------------------
# SQLite Journal store
# --------------------------------------------------------------------------------------


# --- Open Orders Guard (patch #2) -------------------------------------------
# Prevent duplicate/stacked entries when an order is still open on Kraken.
# We treat *any* open order on the same Kraken pair as a block for new entries.

_OPEN_ORDERS_CACHE = {"ts": 0.0, "pairs": set()}

def _parse_bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")

def kraken_open_orders_pairs_cached(ttl_sec: float = 20.0) -> set:
    """Return a set of Kraken pair strings with open orders (cached briefly)."""
    now = time.time()
    try:
        ts = float(_OPEN_ORDERS_CACHE.get("ts") or 0.0)
    except Exception:
        ts = 0.0

    pairs_obj = _OPEN_ORDERS_CACHE.get("pairs")
    if (now - ts) < float(ttl_sec) and isinstance(pairs_obj, set):
        return pairs_obj

    pairs = set()
    try:
        key, secret_b64, _, _ = _kraken_creds()
        resp = kraken_private("OpenOrders", {"trades": True}, key, secret_b64)
        result = (resp or {}).get("result") or {}
        open_orders = result.get("open") or {}
        for _txid, od in (open_orders or {}).items():
            descr = (od or {}).get("descr") or {}
            pair = descr.get("pair")
            if pair:
                pairs.add(str(pair))
    except Exception as e:
        # Fail open: if we can't fetch open orders, don't block trading.
        log.warning("OpenOrders guard: failed to fetch open orders: %s", e)

    _OPEN_ORDERS_CACHE["ts"] = now
    _OPEN_ORDERS_CACHE["pairs"] = pairs
    return pairs

# ---------------------------------------------------------------------------

def _db() -> sqlite3.Connection:
    # One connection per call (safe for FastAPI); always point at DATA_DIR/DB_PATH
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass

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

            -- attribution fields (persist across redeploy)
            userref INTEGER,
            ordertxid TEXT,
            intent_id TEXT,
            strategy TEXT,
            inserted_at REAL,

            raw JSON
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ledgers (
            txid TEXT PRIMARY KEY,
            refid TEXT,
            ts REAL,
            type TEXT,
            subtype TEXT,
            aclass TEXT,
            subclass TEXT,
            asset TEXT,
            wallet TEXT,
            amount REAL,
            fee REAL,
            balance REAL,
            raw TEXT,
            inserted_at REAL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ledgers_refid ON ledgers(refid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ledgers_ts ON ledgers(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ledgers_asset ON ledgers(asset)")

    # Lightweight migrations for existing DBs (safe if columns already exist)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA table_info(trades)")
        cols = {r[1] for r in cur.fetchall()}

        migrations = [
            ("userref",     "ALTER TABLE trades ADD COLUMN userref INTEGER"),
            ("ordertxid",   "ALTER TABLE trades ADD COLUMN ordertxid TEXT"),
            ("intent_id",   "ALTER TABLE trades ADD COLUMN intent_id TEXT"),
            ("inserted_at", "ALTER TABLE trades ADD COLUMN inserted_at REAL"),
            ("pair",        "ALTER TABLE trades ADD COLUMN pair TEXT"),
            ("cost",        "ALTER TABLE trades ADD COLUMN cost REAL"),
            ("strategy",    "ALTER TABLE trades ADD COLUMN strategy TEXT"),
        ]
        for col_name, sql in migrations:
            if col_name not in cols:
                try:
                    conn.execute(sql)
                except Exception:
                    pass
    except Exception:
        pass

    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ordertxid ON trades(ordertxid)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_intent_id ON trades(intent_id)")
    # --- Advisor v2 Learning Engine (Phase 2) -----------------------------------------
    # Persist "blocked / skipped" events so Advisor can learn from what DIDN'T execute.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS blocked_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL,
            strategy TEXT,
            symbol TEXT,
            kind TEXT,
            side TEXT,
            status TEXT,
            reason TEXT,
            error TEXT,
            notional REAL,
            meta_json TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_blocked_events_ts ON blocked_events(ts)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_blocked_events_reason ON blocked_events(reason)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_blocked_events_symbol ON blocked_events(symbol)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_blocked_events_strategy ON blocked_events(strategy)")

    return conn

def insert_trades(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    conn = _db()
    cur = conn.cursor()

    ins = """
    INSERT OR IGNORE INTO trades
    (txid, ts, pair, symbol, side, price, volume, cost, fee, userref, ordertxid, intent_id, strategy, raw)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """


    count = 0
    now_ts = time.time()

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
                int(r.get("userref") or 0),
                (r.get("ordertxid") or None),
                (r.get("intent_id") or None),
                (r.get("strategy") or None),
                json.dumps(r.get("raw") if isinstance(r.get("raw"), dict) else r, separators=(",", ":")),
            ))

            count += cur.rowcount
        except Exception as e:
            log.warning(f"insert skip txid={r.get('txid')}: {e}")

    conn.commit()
    conn.close()
    return count


def insert_ledgers(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Insert ledgers into sqlite with true idempotency.
    - Uses INSERT OR IGNORE so overlap replays do not churn the DB.
    - Performs a lightweight UPDATE only if key fields are currently NULL/empty (backfill).
    Returns: {"inserted": int, "updated": int}
    """
    if not rows:
        return {"inserted": 0, "updated": 0}

    conn = _db()
    cur = conn.cursor()

    ins = """
    INSERT OR IGNORE INTO ledgers
    (txid, refid, ts, type, subtype, aclass, subclass, asset, wallet, amount, fee, balance, raw, inserted_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    upd = """
    UPDATE ledgers SET
        refid = COALESCE(?, refid),
        ts = COALESCE(?, ts),
        type = COALESCE(?, type),
        subtype = COALESCE(?, subtype),
        aclass = COALESCE(?, aclass),
        subclass = COALESCE(?, subclass),
        asset = COALESCE(?, asset),
        wallet = COALESCE(?, wallet),
        amount = COALESCE(?, amount),
        fee = COALESCE(?, fee),
        balance = COALESCE(?, balance),
        raw = COALESCE(?, raw)
    WHERE txid = ?
      AND (
        refid IS NULL OR type IS NULL OR asset IS NULL OR raw IS NULL OR raw = ''
      )
    """

    now = time.time()
    inserted = 0
    updated = 0

    for r in rows:
        try:
            txid = str(r.get("txid") or "")
            refid = (r.get("refid") or None)
            ts = float(r.get("ts", 0) or 0)
            typ = (r.get("type") or None)
            subtype = (r.get("subtype") or None)
            aclass = (r.get("aclass") or None)
            subclass = (r.get("subclass") or None)
            asset = (r.get("asset") or None)
            wallet = (r.get("wallet") or None)
            amount = float(r.get("amount", 0) or 0)
            fee = float(r.get("fee", 0) or 0)
            balance = float(r.get("balance", 0) or 0)
            raw = json.dumps(r.get("raw") if isinstance(r.get("raw"), dict) else r, separators=(",", ":"))

            cur.execute(ins, (txid, refid, ts, typ, subtype, aclass, subclass, asset, wallet, amount, fee, balance, raw, now))
            if cur.rowcount == 1:
                inserted += 1
            else:
                # Optional backfill for older rows with missing metadata; does not churn every run.
                cur.execute(upd, (refid, ts, typ, subtype, aclass, subclass, asset, wallet, amount, fee, balance, raw, txid))
                if cur.rowcount == 1:
                    updated += 1
        except Exception as e:
            log.warning(f"insert ledger skip txid={r.get('txid')}: {e}")

    conn.commit()
    conn.close()
    return {"inserted": int(inserted), "updated": int(updated)}

def fetch_rows(limit: int = 25, offset: int = 0) -> List[Dict[str, Any]]:
    conn = _db()
    cur = conn.cursor()

    cur.execute("""
        SELECT
            txid, ts, pair, symbol, side, price, volume, cost, fee,
            userref, ordertxid, intent_id, strategy, inserted_at, raw
        FROM trades
        ORDER BY ts DESC
        LIMIT ? OFFSET ?
    """, (limit, offset))

    out = []
    for row in cur.fetchall():
        (txid, ts_, pair, symbol, side, price, volume, cost, fee,
         userref, ordertxid, intent_id, strategy, inserted_at, raw) = row

        try:
            raw_obj = json.loads(raw) if raw else None
        except Exception:
            raw_obj = None

        out.append({
            "txid": txid,
            "ts": ts_,
            "pair": pair,
            "symbol": symbol,
            "side": side,
            "price": price,
            "volume": volume,
            "cost": cost,
            "fee": fee,
            "userref": userref,
            "ordertxid": ordertxid,
            "intent_id": intent_id,
            "strategy": strategy,
            "inserted_at": inserted_at,
            "raw": raw_obj,
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
    
@app.on_event("startup")
def _startup_reconcile():
    try:
        log.info("startup: data_dir=%s db=%s journal_v2=%s", str(DATA_DIR), str(DB_PATH), str(JOURNAL_V2_PATH))
        res = reconcile_trade_attribution()
        log.info("startup: reconcile_trade_attribution => %s", res)
    except Exception as e:
        log.warning("startup reconcile failed: %s", e)

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
    
def _extract_ordertxid_and_userref(resp: Any) -> Tuple[Optional[str], Optional[int]]:
    """
    Best-effort extraction of ordertxid + userref from whatever the broker/execution layer returns.
    Handles common shapes:
      - {"ok": True, "txid": "..."} or {"txid": ["..."]}
      - {"result": {"txid": ["..."]}}
      - {"order_txid": "..."} / {"ordertxid": "..."}
    """
    try:
        if resp is None:
            return (None, None)

        # Sometimes response is a string
        if isinstance(resp, str):
            return (None, None)

        # Sometimes response is a list (rare)
        if isinstance(resp, list):
            # try first dict in list
            for x in resp:
                if isinstance(x, dict):
                    otx, ur = _extract_ordertxid_and_userref(x)
                    if otx or ur is not None:
                        return (otx, ur)
            return (None, None)

        if not isinstance(resp, dict):
            return (None, None)

        # unwrap common nesting
        candidate_dicts = [resp]
        if isinstance(resp.get("result"), dict):
            candidate_dicts.append(resp["result"])
        if isinstance(resp.get("data"), dict):
            candidate_dicts.append(resp["data"])

        ordertxid: Optional[str] = None
        userref: Optional[int] = None

        for d in candidate_dicts:
            # order txid keys
            val = (
                d.get("ordertxid")
                or d.get("order_txid")
                or d.get("orderTxid")
                or d.get("txid")     # Kraken often returns txid for order placement
            )
            if isinstance(val, list) and val:
                val = val[0]
            if val and ordertxid is None:
                ordertxid = str(val)

            # userref keys
            ur = d.get("userref") or d.get("user_ref")
            if ur is not None and userref is None:
                try:
                    userref = int(ur)
                except Exception:
                    userref = None

        return (ordertxid, userref)

    except Exception:
        return (None, None)


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
    try:
        key, sec, key_name, sec_name = _kraken_creds()
        now = int(time.time())
    except Exception as e:
        # Make this endpoint diagnostic-only; it should never hard-500.
        return {"ok": False, "error": f"debug_config_failed: {e}"}
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
                    "ordertxid": str(t.get("ordertxid") or ""),
                    "userref": int(t.get("userref") or 0),
                    "intent_id": None,
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
    

@app.get("/journal/ledgers")
def journal_ledgers_peek(
    limit: int = Query(25, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    type: Optional[str] = Query(None, description="Optional ledger type filter, e.g. trade/deposit/withdrawal"),
    asset: Optional[str] = Query(None, description="Optional asset filter, e.g. ZUSD, XXBT, ETH"),
    refid: Optional[str] = Query(None, description="Optional refid (often trade txid)"),
):
    """
    Inspect ingested Kraken ledger rows (for debugging + reconciliation).
    """
    conn = _db()
    cur = conn.cursor()

    where = []
    args: List[Any] = []
    if type:
        where.append("type = ?")
        args.append(type)
    if asset:
        where.append("asset LIKE ?")
        args.append(f"%{asset}%")
    if refid:
        where.append("refid = ?")
        args.append(refid)

    wsql = ("WHERE " + " AND ".join(where)) if where else ""
    cur.execute(f"SELECT COUNT(*) FROM ledgers {wsql}", tuple(args))
    total = int(cur.fetchone()[0] or 0)

    cur.execute(f"""
        SELECT txid, refid, ts, type, subtype, asset, amount, fee, balance
        FROM ledgers
        {wsql}
        ORDER BY ts DESC
        LIMIT ? OFFSET ?
    """, tuple(args + [int(limit), int(offset)]))
    rows = []
    for r in (cur.fetchall() or []):
        rows.append({
            "txid": r[0],
            "refid": r[1],
            "ts": float(r[2] or 0),
            "type": r[3],
            "subtype": r[4],
            "asset": r[5],
            "amount": float(r[6] or 0),
            "fee": float(r[7] or 0),
            "balance": float(r[8] or 0),
        })
    conn.close()
    return {"ok": True, "count": total, "rows": rows}


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
            SELECT txid, ts, pair, symbol, side, price, volume, fee, cost, strategy, userref, ordertxid, intent_id
            FROM trades
            ORDER BY ts DESC
            LIMIT ? OFFSET ?
            """,
            (limit, offset),
        )
        rows = [
            {
                "txid": r[0], "ts": r[1], "pair": r[2], "symbol": r[3], "side": r[4],
                "price": r[5], "volume": r[6], "fee": r[7], "cost": r[8], "strategy": r[9],
                "userref": r[10], "ordertxid": r[11], "intent_id": r[12]
            }
            for r in cur.fetchall()
        ]
        return {"ok": True, "rows": rows}
    finally:
        conn.close()
        
        
@app.get("/attrib/debug")
def attrib_debug(limit: int = 80):
    path = JOURNAL_V2_PATH
    if not path.exists():
        return {"ok": False, "error": "journal_v2_missing", "path": str(path)}

    out = []
    with path.open("r", encoding="utf-8") as fh:
        lines = fh.readlines()[-limit:]

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except Exception:
            continue

        out.append({
            "t": ev.get("t") or ev.get("type"),
            "ts": ev.get("ts") or ev.get("time"),
            "ordertxid": ev.get("ordertxid") or ev.get("order_txid") or ev.get("orderId"),
            "userref": ev.get("userref"),
            "intent_id": ev.get("intent_id"),
            "strategy": ev.get("strategy"),
        })

    return {"ok": True, "path": str(path), "rows": out}

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
    try:
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

        # mode controls how the start window is chosen. Default keeps existing behavior (cursor-based incremental).
        # - mode=cursor   : use durable cursor if present, otherwise fallback to since_hours window
        # - mode=backfill : ignore durable cursor for this run (safe because upserts are idempotent)
        # advance_cursor (default True) controls whether we advance the durable cursor after a successful run.
        mode = str(payload.get("mode", "cursor") or "cursor").lower()
        advance_cursor = bool(payload.get("advance_cursor", True))

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

        cursor_blob = _read_journal_cursor()
        cursor_ts = cursor_blob.get("last_seen_ts")
        now_ts = int(time.time())
        fallback_ts = now_ts - int(since_hours) * 3600

        # mode=backfill ignores the durable cursor for this run (safe because upserts are idempotent).
        # By default it DOES advance the durable cursor (advance_cursor=True) after a successful run.
        if mode == "backfill":
            start_ts0 = fallback_ts
            cursor_source = "backfill"
            cursor_ts_effective = 0
        else:
            if cursor_ts:
                # Best practice: always fetch since (last_seen_ts - safety buffer)
                start_ts0 = max(0, int(cursor_ts) - JOURNAL_SAFETY_BUFFER_SECONDS)
                cursor_source = "cursor"
            else:
                start_ts0 = fallback_ts
                cursor_source = "since_hours"
            cursor_ts_effective = int(cursor_ts or 0)


        UPSERT_SQL = """
        INSERT INTO trades (
            txid, ts, pair, symbol, side, price, volume, cost, fee,
            userref, ordertxid, intent_id, strategy, inserted_at, raw
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?)
        ON CONFLICT(txid) DO UPDATE SET
            ts        = excluded.ts,
            pair      = COALESCE(excluded.pair, trades.pair),
            symbol    = excluded.symbol,
            side      = excluded.side,
            price     = excluded.price,
            volume    = excluded.volume,
            cost      = COALESCE(excluded.cost, trades.cost),
            fee       = excluded.fee,
            userref   = COALESCE(excluded.userref, trades.userref),
            ordertxid = COALESCE(excluded.ordertxid, trades.ordertxid),
            intent_id = COALESCE(trades.intent_id, excluded.intent_id),
            strategy  = COALESCE(trades.strategy, excluded.strategy),
            inserted_at = COALESCE(trades.inserted_at, excluded.inserted_at),
            raw       = excluded.raw
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
                ts = float(t.get("time")) if t.get("time") is not None else None
            except Exception:
                ts = None

            pair_raw = t.get("pair") or ""
            symbol = from_kraken_pair_to_app(pair_raw)
            side = t.get("type")

            def _f(x):
                try:
                    return float(x) if x is not None else None
                except Exception:
                    return None

            price  = _f(t.get("price"))
            volume = _f(t.get("vol"))
            cost   = _f(t.get("cost"))
            fee    = _f(t.get("fee"))

            # Kraken TradesHistory includes ordertxid
            ordertxid = t.get("ordertxid")

            # userref usually not present in TradesHistory; keep if it is (or if you add it later)
            userref = t.get("userref")

            raw = _json.dumps(t, separators=(",", ":"), ensure_ascii=False, default=str)
            inserted_at = time.time()

            return (
                str(txid), ts, pair_raw, symbol, side, price, volume, cost, fee,
                (int(userref) if userref is not None else None),
                (str(ordertxid) if ordertxid else None),
                None,  # intent_id will be reconciled from journal_v2
                None,  # strategy will be reconciled from journal_v2 (or manual attach)
                inserted_at,
                raw
            )


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
        
        # Persist watermark cursor so redeploys don't lose attribution window.
        # In backfill mode, do not advance the durable cursor unless advance_cursor=true.
        candidate_ts = int(last_seen_ts or 0)
        base_ts      = int(cursor_ts_effective or 0)
        if mode == "backfill" and (not advance_cursor):
            new_cursor_ts = 0
        else:
            new_cursor_ts = max(base_ts, candidate_ts)

        debug = {
            "creds_present": True,
            "since_hours": since_hours,
            "cursor_source": cursor_source,
            "mode": mode,
            "advance_cursor": advance_cursor,
            "cursor_file_ts": cursor_ts,
            "start_ts": start_ts0,
            "start_iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(start_ts0)),
            "pages": pages,
            "pulled": total_pulled,
            "kraken_count": kraken_count,
            "hard_limit": hard_limit,
            "post_total_rows": total_rows,
            "notes": notes
        }

        # Reconcile attribution before advancing the watermark so a failed run never "skips" history.
        if dry_run:
            recon = {"ok": True, "skipped": True, "note": "dry_run"}
        else:
            try:
                recon = reconcile_trade_attribution()
            except Exception as e:
                return {
                    "ok": False,
                    "dry_run": dry_run,
                    "count": total_writes,
                    "reconcile": {"ok": False, "error": str(e)},
                    "debug": debug,
                }

        # Advance cursor only after successful write + successful reconciliation (and never on dry_run)
        if (not dry_run) and (recon.get("ok") is True) and new_cursor_ts:
            _write_journal_cursor(new_cursor_ts, {
                "last_start_ts": int(start_ts0),
                "last_run_pulled": int(total_pulled),
                "last_run_pages": int(pages),
                "cursor_source": cursor_source,
            })

        return {
            "ok": True,
            "dry_run": dry_run,
            "count": total_writes,
            "reconcile": recon,
            "debug": debug
        }

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        try:
            log.exception('journal_sync failed')
        except Exception:
            pass
        return {"ok": False, "error": str(e), "trace": tb}

DAILY_JSON = os.path.join(DATA_DIR, "daily.json")

@app.post("/journal/ledgers/sync")
def journal_ledgers_sync(payload: Dict[str, Any] = Body(default=None)):
    """Incremental sync of Kraken Ledgers into sqlite, with durable cursor."""
    try:
        import time, json as _json

        payload     = payload or {}
        dry_run     = bool(payload.get("dry_run", False))
        since_hours = int(payload.get("since_hours", 72) or 72)
        hard_limit  = int(payload.get("limit", 50000) or 50000)

        key, sec, *_ = _kraken_creds()
        if not (key and sec):
            return {"ok": False, "error": "missing_credentials"}

        min_delay   = float(os.getenv("KRAKEN_MIN_DELAY", "0.35") or 0.35)
        max_retries = int(os.getenv("KRAKEN_MAX_RETRIES", "6") or 6)
        timeout     = float(os.getenv("KRAKEN_TIMEOUT", "10") or 10)

        mode = str(payload.get("mode", "cursor") or "cursor").lower()
        advance_cursor = bool(payload.get("advance_cursor", False))

        cursor_blob = _read_ledger_cursor()
        cursor_ts = cursor_blob.get("last_seen_ts")
        now_ts = int(time.time())
        fallback_ts = now_ts - int(since_hours) * 3600

        # mode=backfill ignores the durable cursor for this run (safe because upserts are idempotent).
        # By default it does NOT advance the durable cursor unless advance_cursor=true.
        if mode == "backfill":
            start_ts0 = fallback_ts
            cursor_source = "backfill"
            cursor_ts_effective = 0
        else:
            if cursor_ts:
                start_ts0 = max(0, int(cursor_ts) - JOURNAL_SAFETY_BUFFER_SECONDS)
                cursor_source = "cursor"
            else:
                start_ts0 = fallback_ts
                cursor_source = "fallback"
            cursor_ts_effective = int(cursor_ts or 0)

        # Kraken paging
        ofs = 0
        seen_total = 0
        pulled_total = 0
        inserted_total = 0
        updated_total = 0
        pages = 0
        max_seen_ts = int(cursor_ts_effective) if cursor_ts_effective else 0

        def call_ledgers(payload: Dict[str, Any]) -> Dict[str, Any]:
            delay = min_delay
            last = None
            for _ in range(max_retries):
                time.sleep(min_delay)
                try:
                    resp = kraken_private("Ledgers", payload, key, sec)
                except Exception as e:
                    resp = {"error": [str(e)]}
                last = resp
                errs = (resp or {}).get("error") or []
                if not errs:
                    return resp
                if any("Rate limit" in str(e) for e in errs):
                    time.sleep(delay)
                    delay = min(delay * 1.7, 8.0)
                    continue
                return resp
            return last or {"error": ["unknown_error"]}

        # Phase 1: ofs paging on start window
        max_pages = int(payload.get("max_pages", os.getenv("KRAKEN_MAX_PAGES", "500")) or 500)
        while inserted_total < hard_limit:
            payload = {"start": int(start_ts0), "ofs": int(ofs)}
            resp = call_ledgers(payload)
            errs = (resp or {}).get("error") or []
            if errs:
                # bail out safely; do not advance cursor
                return {"ok": False, "dry_run": dry_run, "error": errs, "debug": {"cursor_source": cursor_source, "start_ts": start_ts0, "ofs": ofs}}

            result = (resp or {}).get("result") or {}
            ledger_map = result.get("ledger") or {}
            count = int(result.get("count") or 0)
            pages += 1

            rows = []
            for txid, v in (ledger_map or {}).items():
                try:
                    ts = float(v.get("time") or 0)
                except Exception:
                    ts = 0.0
                if ts > max_seen_ts:
                    max_seen_ts = int(ts)
                rows.append({
                    "txid": str(txid),
                    "refid": v.get("refid"),
                    "ts": ts,
                    "type": v.get("type"),
                    "subtype": v.get("subtype"),
                    "aclass": v.get("aclass"),
                    "subclass": v.get("subclass"),
                    "asset": v.get("asset"),
                    "wallet": v.get("wallet"),
                    "amount": v.get("amount"),
                    "fee": v.get("fee"),
                    "balance": v.get("balance"),
                    "raw": v,
                })

            seen_total += len(rows)
            pulled_total += len(rows)

            if not dry_run and rows:
                ins_res = insert_ledgers(rows)
                inserted_total += int(ins_res.get('inserted', 0))
                updated_total += int(ins_res.get('updated', 0))
# Kraken uses ofs; stop when fewer than page_size-ish returned
            if not ledger_map or len(ledger_map) == 0:
                break

            ofs += len(ledger_map)

            # stop when we've reached Kraken's reported count (if provided)
            if count and ofs >= count:
                break

            # safety: cap pages to avoid runaway loops
            if pages >= max_pages:
                break

            # safety: don't spin forever
            if ofs > 200000:
                break

        # write cursor only if we observed newer data AND not dry_run
        if (not dry_run) and max_seen_ts and (int(max_seen_ts) > int(cursor_ts_effective)) and (mode != "backfill" or advance_cursor):
            _write_ledger_cursor(int(max_seen_ts), {"source": "kraken"})

        return {
            "ok": True,
            "dry_run": dry_run,
            "count": int(pulled_total),
            "inserted": int(inserted_total),
            "updated": int(updated_total),
            "debug": {
                "cursor_source": cursor_source,
                "cursor_file_ts": int(cursor_ts or 0),
                "start_ts": int(start_ts0),
                "start_iso": datetime.datetime.utcfromtimestamp(int(start_ts0)).replace(microsecond=0).isoformat() + "Z" if start_ts0 else None,
                "pages": int(pages),
                "pulled": int(pulled_total),
                "kraken_count": int(seen_total),
                "hard_limit": int(hard_limit),
                "max_seen_ts": int(max_seen_ts or 0),
            }
        }
    except Exception as e:
        log.exception("journal_ledgers_sync failed")
        return {"ok": False, "error": str(e)}


@app.get("/journal/reconcile/ledgers-trades")
def journal_reconcile_ledgers_trades(limit: int = 50):
    """Cross-check invariants between trades and ledgers (Kraken)."""
    try:
        conn = _db()
        cur = conn.cursor()

        # counts
        cur.execute("SELECT COUNT(*) FROM trades")
        trades_n = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(*) FROM ledgers")
        ledgers_n = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(*) FROM ledgers WHERE type='trade'")
        ledgers_trade_n = int(cur.fetchone()[0] or 0)
        # distinct trade refids present in ledgers (one trade => typically 2 ledger rows)
        cur.execute('''
            SELECT COUNT(DISTINCT l.refid)
            FROM ledgers l
            JOIN trades t ON t.txid = l.refid
            WHERE l.type='trade' AND l.refid IS NOT NULL
        ''')
        ledgers_trade_refids_n = int(cur.fetchone()[0] or 0)

        # non-trade ledgers (staking, transfers, etc.)
        cur.execute("SELECT COUNT(*) FROM ledgers WHERE type IS NULL OR type <> 'trade'")
        ledgers_non_trade_n = int(cur.fetchone()[0] or 0)


        # trades missing any trade-ledger rows
        cur.execute("""
            SELECT COUNT(*) FROM trades t
            WHERE NOT EXISTS (
                SELECT 1 FROM ledgers l
                WHERE l.refid = t.txid AND l.type='trade'
            )
        """)
        trades_missing_ledgers = int(cur.fetchone()[0] or 0)

        # trade-ledgers missing a trade row
        cur.execute("""
            SELECT COUNT(*) FROM ledgers l
            WHERE l.type='trade' AND l.refid IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM trades t WHERE t.txid = l.refid)
        """)
        ledgers_missing_trades = int(cur.fetchone()[0] or 0)

        # distinct trade refids in ledgers that do not exist in trades
        cur.execute('''
            SELECT COUNT(DISTINCT l.refid)
            FROM ledgers l
            WHERE l.type='trade' AND l.refid IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM trades t WHERE t.txid = l.refid)
        ''')
        ledgers_missing_trades_distinct_n = int(cur.fetchone()[0] or 0)

        # fee mismatches (asset-aware): Kraken may charge fees in base asset (e.g., DOT) or quote (e.g., ZUSD).
        # We estimate the fee-in-USD from ledger rows by using the implied trade price from the ledger legs:
        #   implied_px_usd = abs(usd_amount) / abs(base_amount)
        # Then:
        #   fee_usd_est = usd_fee + (base_fee * implied_px_usd)  (plus any other fee assets we can convert similarly)
        #
        # This reconciles cases like:
        #   trade_fee=0.23992 USD, ledger_fee=0.11767129 DOT, implied_px≈2.038 USD/DOT => 0.11767129*2.038≈0.2399 USD
        cur.execute("""
            SELECT t.txid, COALESCE(t.fee,0) AS trade_fee
            FROM trades t
            WHERE EXISTS (SELECT 1 FROM ledgers l WHERE l.refid = t.txid AND l.type='trade')
        """)
        trade_fee_rows = cur.fetchall() or []

        def _is_usd_asset(a: str) -> bool:
            a = (a or "").upper()
            return ("USD" in a) or (a in {"ZUSD", "USD"})

        fee_mismatches = []
        for (txid, trade_fee) in trade_fee_rows:
            cur.execute("""
                SELECT asset, COALESCE(amount,0) AS amount, COALESCE(fee,0) AS fee
                FROM ledgers
                WHERE refid = ? AND type='trade'
            """, (txid,))
            lrows = cur.fetchall() or []

            # Aggregate ledger legs
            fee_by_asset = {}
            amt_by_asset = {}
            for asset, amount, fee in lrows:
                asset = asset or ""
                fee_by_asset[asset] = float(fee_by_asset.get(asset, 0.0)) + float(fee or 0.0)
                amt_by_asset[asset] = float(amt_by_asset.get(asset, 0.0)) + float(amount or 0.0)

            # Identify implied price using USD leg vs non-USD leg(s).
            usd_amt = sum(abs(v) for a, v in amt_by_asset.items() if _is_usd_asset(a))
            non_usd_assets = [a for a in amt_by_asset.keys() if not _is_usd_asset(a)]
            base_amt = sum(abs(amt_by_asset[a]) for a in non_usd_assets)

            implied_px = (usd_amt / base_amt) if (usd_amt and base_amt) else None

            # Convert fees to USD estimate
            fee_usd_est = 0.0
            fee_usd_direct = sum(float(v) for a, v in fee_by_asset.items() if _is_usd_asset(a))
            fee_usd_est += fee_usd_direct

            # If fee is charged in a non-USD asset and we have implied price, convert it.
            fee_converted = {}
            if implied_px:
                for a, v in fee_by_asset.items():
                    if _is_usd_asset(a):
                        continue
                    # best-effort: treat non-USD fee asset as base leg for conversion
                    fee_converted[a] = float(v) * float(implied_px)
                    fee_usd_est += fee_converted[a]

            trade_fee_f = float(trade_fee or 0.0)
            if abs(trade_fee_f - fee_usd_est) > float(LEDGER_FEE_EPS):
                fee_mismatches.append({
                    "txid": txid,
                    "trade_fee": trade_fee_f,
                    "ledger_fee_usd_est": float(fee_usd_est),
                    "ledger_fee_by_asset": {k: float(v) for k, v in fee_by_asset.items() if abs(float(v)) > 0},
                    "implied_px_usd": float(implied_px) if implied_px else None,
                })

        # cost mismatches (compare trade.cost vs abs(sum USD ledger amounts) when asset USD present)

        # cost mismatches (compare trade.cost vs abs(sum USD ledger amounts) when asset USD present)
        cur.execute("""
            WITH usd AS (
              SELECT refid, ABS(SUM(COALESCE(amount,0))) AS usd_amt
              FROM ledgers
              WHERE type='trade' AND refid IS NOT NULL AND asset LIKE '%USD%'
              GROUP BY refid
            )
            SELECT t.txid, COALESCE(t.cost,0) AS trade_cost, COALESCE(usd.usd_amt,0) AS ledger_usd
            FROM trades t
            JOIN usd ON usd.refid = t.txid
            WHERE ABS(COALESCE(t.cost,0) - COALESCE(usd.usd_amt,0)) > ?
            ORDER BY ABS(COALESCE(t.cost,0) - COALESCE(usd.usd_amt,0)) DESC
            LIMIT ?
        """, (float(LEDGER_USD_EPS), int(limit)))
        cost_mismatches = [
            {"txid": r[0], "trade_cost": float(r[1] or 0), "ledger_usd": float(r[2] or 0)}
            for r in (cur.fetchall() or [])
        ]

        conn.close()
        return {
            "ok": True,
            "counts": {
                "trades": trades_n,
                "ledgers": ledgers_n,
                "ledgers_trade": ledgers_trade_n,
                "ledgers_trade_refids": ledgers_trade_refids_n,
                "ledgers_non_trade": ledgers_non_trade_n,
                "trades_missing_ledgers": trades_missing_ledgers,
                "ledgers_missing_trades": ledgers_missing_trades,
                "ledgers_missing_trades_distinct": ledgers_missing_trades_distinct_n,
                "fee_mismatch": len(fee_mismatches),
                "cost_mismatch": len(cost_mismatches),
                "coverage_pct": (float(ledgers_trade_refids_n) / float(trades_n) if trades_n else 0.0),
                "fee_eps": float(LEDGER_FEE_EPS),
                "usd_eps": float(LEDGER_USD_EPS),
            },
            "samples": {
                "fee_mismatches": fee_mismatches,
                "cost_mismatches": cost_mismatches,
            },
            "notes": ([f"Ledger coverage is low: {ledgers_trade_n}/{trades_n} trades have matching ledgers. Consider running /journal/ledgers/sync with a larger since_hours to backfill."] if (trades_n and ledgers_trade_n < trades_n) else [])
        }
    except Exception as e:
        log.exception("journal_reconcile_ledgers_trades failed")
        return {"ok": False, "error": str(e)}
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
    
@app.get("/advisor/v2/summary")
def advisor_v2_summary(limit: int = Query(1000, ge=1, le=10000)):
    """
    High-level Advisor summary over recent journal_v2 entries.
    This does NOT place orders; it's read-only analytics.
    """
    try:
        payload = advisor_summary(limit=limit)
        return payload
    except Exception as exc:
        return {
            "ok": False,
            "error": "advisor_v2_failed",
            "detail": str(exc),
        }


@app.get("/advisor/v2/report")
def advisor_v2_report(
    hours: int = Query(24, ge=1, le=24*30),
    days: Optional[int] = Query(None, ge=1, le=365),
    bucket_hours: int = Query(4, ge=1, le=12),
    write_file: bool = Query(True),
    markdown: bool = Query(False),
):
    """
    Full Advisor v2 attribution report.

    Reads (last N hours/days):
      - journal.db (trades table)
      - journal_v2.jsonl (broker actions)
      - telemetry_v2.jsonl (guard/risk/skip reasons)

    Returns JSON by default, or Markdown if markdown=true.
    """
    try:
        from advisor_v2 import generate_report, to_markdown
        rep = generate_report(hours=hours, days=days, bucket_hours=bucket_hours, write_file=write_file)
        if markdown:
            return {"ok": True, "markdown": to_markdown(rep)}
        return rep
    except Exception as exc:
        return {"ok": False, "error": "advisor_v2_report_failed", "detail": str(exc)}

@app.get("/advisor/v2/suggestions")
def advisor_v2_suggestions(
    hours: int = Query(24, ge=1, le=24*30),
    days: Optional[int] = Query(None, ge=1, le=365),
    top_n: int = Query(10, ge=1, le=50),
    min_closed: int = Query(3, ge=0, le=100),
):
    """
    Advisor v2 suggestions endpoint used by the dashboard.

    This is a thin wrapper around advisor_v2.advisor_suggestions and
    never places orders. It only reads journal/telemetry/trades and
    returns ranked (strategy, symbol) pairs.
    """
    try:
        from advisor_v2 import advisor_suggestions
        payload = advisor_suggestions(
            hours=hours,
            days=days,
            top_n=top_n,
            min_closed=min_closed,
        )
        return payload
    except Exception as exc:
        return {
            "ok": False,
            "error": "advisor_v2_suggestions_failed",
            "detail": str(exc),
        }




# --------------------------------------------------------------------------------------
# Advisor v2 Learning Engine — Phase 2 (Observer)
# --------------------------------------------------------------------------------------

def _is_blocked_action(a: Dict[str, Any]) -> bool:
    """Return True if an action represents a blocked/skipped trade/exit."""
    status = str(a.get("status") or "").lower()
    err = str(a.get("error") or "").lower()
    # Anything explicitly marked skipped/blocked counts.
    if status.startswith("skipped") or "blocked" in status:
        return True
    # Common failure modes also count.
    if "insufficient" in err or "below_min" in err or "min_notional" in err:
        return True
    return False

def _blocked_reason(a: Dict[str, Any]) -> str:
    """Normalize a reason string for grouping in Advisor reports."""
    status = str(a.get("status") or "").strip()
    if status:
        return status
    reason = str(a.get("reason") or "").strip()
    if reason:
        return reason
    err = str(a.get("error") or "").strip()
    return err[:160] if err else "blocked"

def _insert_blocked_events(events: List[Dict[str, Any]]) -> int:
    if not events:
        return 0
    ts = time.time()
    conn = _journal_db_connect()
    try:
        rows = []
        for e in events:
            rows.append((
                float(e.get("ts") or ts),
                str(e.get("strategy") or ""),
                str(e.get("symbol") or ""),
                str(e.get("kind") or ""),
                str(e.get("side") or ""),
                str(e.get("status") or ""),
                str(e.get("reason") or ""),
                str(e.get("error") or ""),
                float(e.get("notional") or 0.0),
                json.dumps(e.get("meta") or {}, ensure_ascii=False),
            ))
        conn.executemany(
            "INSERT INTO blocked_events (ts,strategy,symbol,kind,side,status,reason,error,notional,meta_json) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        conn.commit()
        return len(rows)
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.post("/advisor/v2/ingest_run")
def advisor_v2_ingest_run(payload: Dict[str, Any] = Body(default=None)) -> Dict[str, Any]:
    """
    Ingest a /scheduler/v2/run response and persist blocked/skipped events.

    This endpoint is intentionally "observer-only": it never trades.
    """
    payload = payload or {}
    actions = payload.get("actions") or []
    blocked = []
    for a in actions:
        if not isinstance(a, dict):
            continue
        if _is_blocked_action(a):
            blocked.append({
                "ts": time.time(),
                "strategy": a.get("strategy"),
                "symbol": a.get("symbol"),
                "kind": a.get("kind"),
                "side": a.get("side"),
                "status": a.get("status"),
                "reason": _blocked_reason(a),
                "error": a.get("error"),
                "notional": a.get("notional"),
                "meta": {
                    "intent_id": a.get("intent_id"),
                    "dry": payload.get("dry"),
                    "tf": (payload.get("config") or {}).get("tf"),
                },
            })
    n = _insert_blocked_events(blocked)
    return {"ok": True, "ingested": n, "blocked_found": len(blocked)}

@app.get("/advisor/v2/blocked_summary")
def advisor_v2_blocked_summary(hours: int = Query(24, ge=1, le=720), top_n: int = Query(25, ge=1, le=200)) -> Dict[str, Any]:
    """Aggregated view of blocked/skipped events for the last N hours."""
    since_ts = time.time() - (hours * 3600)
    conn = _journal_db_connect()
    try:
        cur = conn.cursor()
        by_reason = []
        for r in cur.execute(
            "SELECT COALESCE(NULLIF(TRIM(reason),''),'unknown') AS r, COUNT(*) "
            "FROM blocked_events WHERE ts >= ? GROUP BY r ORDER BY COUNT(*) DESC LIMIT ?",
            (since_ts, int(top_n)),
        ).fetchall():
            by_reason.append({"reason": r[0], "count": int(r[1])})
        by_symbol = []
        for r in cur.execute(
            "SELECT COALESCE(NULLIF(TRIM(symbol),''),'') AS s, COUNT(*) "
            "FROM blocked_events WHERE ts >= ? GROUP BY s ORDER BY COUNT(*) DESC LIMIT ?",
            (since_ts, int(top_n)),
        ).fetchall():
            by_symbol.append({"symbol": r[0], "count": int(r[1])})
        by_strategy = []
        for r in cur.execute(
            "SELECT COALESCE(NULLIF(TRIM(strategy),''),'') AS s, COUNT(*) "
            "FROM blocked_events WHERE ts >= ? GROUP BY s ORDER BY COUNT(*) DESC LIMIT ?",
            (since_ts, int(top_n)),
        ).fetchall():
            by_strategy.append({"strategy": r[0], "count": int(r[1])})
        total = cur.execute("SELECT COUNT(*) FROM blocked_events WHERE ts >= ?", (since_ts,)).fetchone()[0]
        return {"ok": True, "hours": hours, "total": int(total), "by_reason": by_reason, "by_symbol": by_symbol, "by_strategy": by_strategy}
    finally:
        try:
            conn.close()
        except Exception:
            pass

@app.get("/advisor/v2/blocked_detail")
def advisor_v2_blocked_detail(hours: int = Query(24, ge=1, le=720), limit: int = Query(200, ge=1, le=5000)) -> Dict[str, Any]:
    """Recent blocked/skipped event rows (for drilling into root causes)."""
    since_ts = time.time() - (hours * 3600)
    conn = _journal_db_connect()
    try:
        cur = conn.cursor()
        rows = []
        for r in cur.execute(
            "SELECT ts,strategy,symbol,kind,side,status,reason,error,notional,meta_json "
            "FROM blocked_events WHERE ts >= ? ORDER BY ts DESC LIMIT ?",
            (since_ts, int(limit)),
        ).fetchall():
            rows.append({
                "ts": float(r[0] or 0),
                "strategy": r[1],
                "symbol": r[2],
                "kind": r[3],
                "side": r[4],
                "status": r[5],
                "reason": r[6],
                "error": r[7],
                "notional": float(r[8] or 0.0),
                "meta": json.loads(r[9] or "{}") if (r[9] or "").strip() else {},
            })
        return {"ok": True, "hours": hours, "count": len(rows), "rows": rows}
    finally:
        try:
            conn.close()
        except Exception:
            pass
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
    
@app.get("/journal/v2/review")
def journal_v2_review(
    limit: int = Query(2000, ge=1, le=100000),
) -> Dict[str, Any]:
    """
    Lightweight summary over the append-only journal_v2.jsonl file.

    - Reads up to `limit` most recent events.
    - Aggregates by strategy and (strategy, symbol).
    - Ready for Advisor v2 to consume (we'll add P&L fields later).
    """
    path = JOURNAL_V2_PATH
    if not path.exists():
        return {
            "ok": True,
            "count": 0,
            "per_strategy": [],
            "per_pair": [],
        }

    rows: List[Dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    evt = json.loads(line)
                except Exception:
                    continue
                rows.append(evt)
    except Exception as e:
        return {
            "ok": False,
            "error": f"failed_to_read_journal_v2: {e.__class__.__name__}: {e}",
        }

    # Keep only the last `limit` events
    if len(rows) > limit:
        rows = rows[-limit:]

    per_strategy: Dict[str, Dict[str, Any]] = {}
    per_pair: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for evt in rows:
        strat = str(evt.get("strategy") or "unknown")
        sym = str(evt.get("symbol") or "unknown")
        status = str(evt.get("status") or "unknown")
        side = str(evt.get("side") or "unknown")
        kind = str(evt.get("kind") or "unknown")
        dry = bool(evt.get("dry") or False)

        # Placeholder for future P&L; currently zero until we enrich events
        pnl = float(evt.get("pnl", 0.0) or 0.0)

        # --- per strategy ---
        s_rec = per_strategy.setdefault(
            strat,
            {
                "strategy": strat,
                "count": 0,
                "sent": 0,
                "error": 0,
                "dry": 0,
                "buy": 0,
                "sell": 0,
                "pnl_sum": 0.0,
            },
        )
        s_rec["count"] += 1
        if status == "sent":
            s_rec["sent"] += 1
        if status == "error":
            s_rec["error"] += 1
        if dry:
            s_rec["dry"] += 1
        if side == "buy":
            s_rec["buy"] += 1
        if side == "sell":
            s_rec["sell"] += 1
        s_rec["pnl_sum"] += pnl

        # --- per (strategy, symbol) ---
        key = (strat, sym)
        p_rec = per_pair.setdefault(
            key,
            {
                "strategy": strat,
                "symbol": sym,
                "count": 0,
                "sent": 0,
                "error": 0,
                "buy": 0,
                "sell": 0,
                "pnl_sum": 0.0,
            },
        )
        p_rec["count"] += 1
        if status == "sent":
            p_rec["sent"] += 1
        if status == "error":
            p_rec["error"] += 1
        if side == "buy":
            p_rec["buy"] += 1
        if side == "sell":
            p_rec["sell"] += 1
        p_rec["pnl_sum"] += pnl

    return {
        "ok": True,
        "count": len(rows),
        "per_strategy": sorted(per_strategy.values(), key=lambda r: r["strategy"]),
        "per_pair": sorted(per_pair.values(), key=lambda r: (r["strategy"], r["symbol"])),
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

    # Load positions + risk config, then normalize positions to dict[(symbol,strat)] -> Position
    raw_positions = _load_open_positions_from_trades(use_strategy_col=True)

    # Live Kraken balances map (base asset -> available qty). Used to prevent phantom SELL exits.
    try:
        _kr_pos_raw = broker_kraken.positions() or []
    except Exception:
        _kr_pos_raw = []
    _live_bal: Dict[str, float] = {}
    for _r in _kr_pos_raw:
        try:
            _a = str(_r.get("asset", "") or "").upper()
            _q = _r.get("avail", None)
            if _q is None:
                _q = _r.get("qty", 0.0)
            _qf = float(_q)
        except Exception:
            continue
        if _a in ("XBT", "XXBT"):
            _a = "BTC"
        _live_bal[_a] = _live_bal.get(_a, 0.0) + _qf

    def _base_asset_from_symbol(_sym: str) -> str:
        _s = str(_sym or "")
        if "/" in _s:
            return _s.split("/")[0].upper()
        return _s.upper()

    risk_cfg = load_risk_config() or {}

    positions: Dict[Tuple[str, str], Position] = {}
    if isinstance(raw_positions, dict):
        positions = raw_positions
    elif isinstance(raw_positions, list):
        _pos_map: Dict[Tuple[str, str], Position] = {}
        for p in raw_positions:
            try:
                sym = (getattr(p, "symbol", "") or "").strip()
                sname = (getattr(p, "strategy", "") or "").strip().lower()
                if not sym:
                    continue
                key = (sym, sname or "default")
                qty = float(getattr(p, "qty", 0.0) or 0.0)
                if abs(qty) > 1e-12:
                    _pos_map[key] = p
                elif key in _pos_map:
                    _pos_map.pop(key, None)
            except Exception:
                continue
        positions = _pos_map

    # Defensive: scheduler_core requires dict positions
    if not isinstance(positions, dict):
        raise TypeError(f"debug_strategy_scan positions must be dict, got {type(positions)}")

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
            contexts[symbol] = {
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

    if scan is not None:
        out["scan"] = {
            "action": getattr(scan, "action", None),
            "reason": getattr(scan, "reason", None),
            "score": getattr(scan, "score", None),
            "atr_pct": getattr(scan, "atr_pct", None),
            "notional": getattr(scan, "notional", None),
            "selected": getattr(scan, "selected", None),
        }
    else:
        out["scan"] = None

    # Derive some entry-gating flags similar to scheduler_v2
    is_flat = abs(qty) < 1e-12
    scan_selected = bool(out["scan"] and out["scan"]["selected"])
    scan_action = out["scan"]["action"] if out["scan"] else None
    scan_notional_positive = (out["scan"]["notional"] or 0.0) > 0.0 if out["scan"] else False

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
def journal_enrich(
    apply_rules: bool = Query(True),
    batch_size: int = Query(200, ge=1, le=2000),
    max_rows: int = Query(5000, ge=1, le=20000),
    dry_run: bool = Query(False),
):
    """
    Enrich unlabeled trades in the local journal using the Kraken `userref`
    field and `policy_config/userref_map.json`.

    This is a simpler, more robust attribution pass that:
    - reads unlabeled rows from the `trades` table;
    - parses the raw Kraken trade JSON for `userref`;
    - maps `userref -> strategy` via userref_map.json;
    - writes the inferred strategy back to `trades.strategy`.

    NOTE:
    - We no longer rely on Kraken's QueryTrades / QueryOrdersInfo here.
    - Only trades that were placed with a `userref` that appears in
      userref_map.json will be labeled.
    """

    log.info(
        "journal_enrich (v2): apply_rules=%s dry_run=%s batch_size=%s max_rows=%s",
        apply_rules,
        dry_run,
        batch_size,
        max_rows,
    )

    conn = _db()
    cur = conn.cursor()

    # ---------------------------------------------------------
    # Step 1: Load unlabeled trades from the local journal
    # ---------------------------------------------------------
    rows: list[dict] = []
    cur.execute(
        "SELECT txid, ts, symbol, raw "
        "FROM trades "
        "WHERE strategy IS NULL OR TRIM(strategy) = '' "
        "ORDER BY ts DESC "
        "LIMIT ?",
        (max_rows,),
    )
    for txid, ts, symbol, raw in cur.fetchall():
        rows.append(
            {
                "txid": txid,
                "ts": ts,
                "symbol": symbol,
                "raw": raw,
            }
        )

    scanned = len(rows)
    if scanned == 0:
        log.info("journal_enrich (v2): nothing to do, 0 unlabeled rows")
        return JSONResponse(
            {
                "ok": True,
                "scanned": 0,
                "orders_checked": 0,
                "updated": 0,
                "api_labeled": 0,
                "rules_labeled": 0,
                "ambiguous": 0,
                "missing_order": 0,
                "apply_rules": apply_rules,
                "batch_size": batch_size,
                "max_rows": max_rows,
            }
        )

    # ---------------------------------------------------------
    # Step 2: Load userref -> strategy mapping from policy_config
    # ---------------------------------------------------------
    from pathlib import Path
    import json

    cfg_dir = _os.getenv("POLICY_CONFIG_DIR", "policy_config")
    userref_path = Path(cfg_dir) / "userref_map.json"
    userref_to_strategy: dict[int, str] = {}

    try:
        if userref_path.exists():
            with userref_path.open("r", encoding="utf-8") as f:
                raw_map = json.load(f)

            if isinstance(raw_map, dict):
                # Current repo shape is: { "101": "c1", "102": "c2", ..., "1": "manual", ... }
                for k, v in raw_map.items():
                    try:
                        ref = int(k)
                    except Exception:
                        continue
                    strat = str(v).strip()
                    if strat:
                        userref_to_strategy[ref] = strat
        else:
            log.warning(
                "journal_enrich (v2): userref_map.json not found at %s; "
                "no attribution will be possible from userref.",
                userref_path,
            )
    except Exception as e:
        log.exception("journal_enrich (v2): failed to load userref_map.json: %s", e)

    # ---------------------------------------------------------
    # Step 3: Extract userref from raw JSON and propose updates
    # ---------------------------------------------------------
    def _extract_userref(obj):
        """Recursively search a JSON-like object for a `userref` field."""
        if isinstance(obj, dict):
            if "userref" in obj:
                try:
                    return int(obj["userref"])
                except Exception:
                    pass
            for val in obj.values():
                ref = _extract_userref(val)
                if ref is not None:
                    return ref
        elif isinstance(obj, (list, tuple)):
            for val in obj:
                ref = _extract_userref(val)
                if ref is not None:
                    return ref
        return None

    updates: dict[tuple[str, float], str] = {}  # key: (txid, ts) -> strategy
    rules_labeled = 0

    if not userref_to_strategy:
        log.warning(
            "journal_enrich (v2): userref_to_strategy map is empty; "
            "no rows will be labeled this pass."
        )
    else:
        for row in rows:
            txid = row["txid"]
            ts = row["ts"]
            raw = row.get("raw")
            if not raw:
                continue

            try:
                blob = json.loads(raw)
            except Exception:
                # raw may be NULL or not JSON for legacy rows
                continue

            ref = _extract_userref(blob)
            if ref is None:
                continue

            strat = userref_to_strategy.get(ref)
            if not strat:
                continue

            key = (txid, ts)
            if key not in updates:
                updates[key] = strat
                rules_labeled += 1

    # ---------------------------------------------------------
    # Step 4: Apply updates (or report what would be done)
    # ---------------------------------------------------------
    updated = 0
    orders_checked = 0
    api_labeled = 0
    ambiguous = 0
    missing_order = 0

    if not updates:
        log.info("journal_enrich (v2): no strategies inferred from userref.")
    elif dry_run:
        log.info(
            "journal_enrich (v2): DRY RUN, would update %d rows", len(updates)
        )
    else:
        log.info(
            "journal_enrich (v2): applying %d updates to trades.strategy",
            len(updates),
        )
        try:
            for (txid, ts), strat in updates.items():
                cur.execute(
                    "UPDATE trades "
                    "SET strategy = ? "
                    "WHERE txid = ? AND ts = ?",
                    (strat, txid, ts),
                )
                updated += cur.rowcount
            conn.commit()
        except Exception:
            log.exception("journal_enrich (v2): failed while updating trades")
            conn.rollback()

    # ---------------------------------------------------------
    # Step 5: Return summary for the UI / CLI
    # ---------------------------------------------------------
    return JSONResponse(
        {
            "ok": True,
            "scanned": scanned,
            "orders_checked": orders_checked,
            "updated": updated,
            "api_labeled": api_labeled,
            "rules_labeled": rules_labeled,
            "ambiguous": ambiguous,
            "missing_order": missing_order,
            "apply_rules": apply_rules,
            "batch_size": batch_size,
            "max_rows": max_rows,
        }
    )

@app.get("/debug/env")
def debug_env():
    keys = [
        "APP_VERSION","SCHED_DRY","SCHED_ON","SCHED_TIMEFRAME","SCHED_LIMIT","SCHED_NOTIONAL",
        "BROKER","TRADING_ENABLED","TZ","PORT","DEFAULT_LIMIT","DEFAULT_NOTIONAL","DEFAULT_TIMEFRAME"
    ]
    env = {k: os.getenv(k) for k in keys}
    return {"ok": True, "env": env}
    

@app.get("/debug/kraken/value")
def debug_kraken_value():
    """Live Kraken portfolio value (best-effort)."""
    try:
        try:
            bals = broker_kraken.positions()  # [{"asset":..,"qty":..}, ...]
        except Exception as e:
            return {"ok": False, "error": f"positions_error:{e}"}

        breakdown = []
        total = 0.0
        for row in bals:
            asset = str(row.get("asset","")).upper()
            if not asset:
                continue
            qty = float(row.get("qty", 0) or 0)
            if qty == 0:
                continue
            px = 1.0
            if asset != "USD":
                try:
                    px = float(broker_kraken.last_price(f"{asset}USD") or 0) or 0.0
                except Exception:
                    px = 0.0
            val = qty * px
            breakdown.append({"asset": asset, "qty": qty, "px": px, "value": val})
            total += val
        breakdown.sort(key=lambda r: (0 if r["asset"]=="USD" else 1, r["asset"]))
        return {"ok": True, "total_usd": total, "breakdown": breakdown}
    except Exception as e:
        return {"ok": False, "error": f"debug_kraken_value_error:{e}"}

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
def debug_positions(include_strategy: bool = True, include_legacy: bool = False):
    """
    Show current open positions as seen by the system, including:

    - qty / avg_price from trades table
    - last_price (via _last_price_safe)
    - unrealized_pct (via RiskEngine.compute_unrealized_pct)
    - per-symbol caps from risk.json (max_notional / max_units)

    This is the single source of truth for exposures + unrealized P&L.
    """
    raw_positions = _load_open_positions_from_trades(
        use_strategy_col=include_strategy,
        include_legacy=include_legacy,
    )

    # Normalize to the dict shape expected everywhere else:
    #   (symbol, strategy) -> Position
    positions: Dict[Tuple[str, str], Position] = {}

    if isinstance(raw_positions, dict):
        positions = raw_positions
    elif isinstance(raw_positions, list):
        _pos_map: Dict[Tuple[str, str], Position] = {}
        for p in raw_positions:
            try:
                sym = (getattr(p, "symbol", "") or "").strip()
                strat = (getattr(p, "strategy", "") or "").strip().lower()
                if not sym:
                    continue
                # Fallback labels when strategy col is empty or we’re not
                # including per-strategy splits.
                if not strat:
                    strat = "default" if include_strategy else "misc"
                key = (sym, strat)
                qty = float(getattr(p, "qty", 0.0) or 0.0)
                if abs(qty) > 1e-12:
                    _pos_map[key] = p
                elif key in _pos_map:
                    _pos_map.pop(key, None)
            except Exception:
                continue
        positions = _pos_map

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

    # ------------------------------------------------------------------
    # Policy-aligned universe: restrict scheduler symbols to whitelist + risk policy
    # (prevents scanning/trading paused pairs even if SYMBOLS env includes them).
    # ------------------------------------------------------------------
    try:
        wl_map = load_whitelist_config() or {}
        rk_cfg = load_risk_config() or {}
        # Union of whitelisted symbols for selected strategies
        wl_union = set()
        for st in strats:
            for sym in (wl_map.get(st) or []):
                wl_union.add(str(sym).strip().upper())

        # Risk policy guards
        avoid_pairs = set(str(x).strip().upper() for x in (rk_cfg.get("avoid_pairs") or []) if str(x).strip())
        caps_map = (((rk_cfg.get("risk_caps") or {}).get("max_notional_per_symbol")) or {}) if isinstance(rk_cfg, dict) else {}
        default_cap = float((((rk_cfg.get("risk_caps") or {}).get("default")) or 0.0)) if isinstance(rk_cfg, dict) else 0.0

        def _pair_key(s: str) -> str:
            return str(s).strip().upper().replace("/", "")

        def _cap_for(sym: str) -> float:
            k = _pair_key(sym)
            v = caps_map.get(k)
            try:
                return float(v) if v is not None else float(default_cap or 0.0)
            except Exception:
                return float(default_cap or 0.0)

        filtered = []
        for sym in syms:
            k = _pair_key(sym)
            if wl_union and sym not in wl_union:
                continue
            if k in avoid_pairs:
                continue
            cap = _cap_for(sym)
            if cap <= 0:
                continue
            filtered.append(sym)

        if filtered:
            syms = filtered
    except Exception:
        pass

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
    positions = _reconcile_positions_with_kraken(positions)
    # Normalize positions for scheduler_core: it expects a dict keyed by (symbol, strategy)
    # _load_open_positions_from_trades returns a List[Position] for debug friendliness.
    if isinstance(positions, list):
        _pos_map: Dict[Tuple[str, str], Position] = {}
        for p in positions:
            try:
                sym = (getattr(p, "symbol", "") or "").strip()
                strat = (getattr(p, "strategy", "") or "").strip()
                if not sym:
                    continue
                _pos_map[(sym, strat)] = p
            except Exception:
                continue
        positions = _pos_map
    elif not isinstance(positions, dict):
        positions = {}
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
    # Defensive: scheduler_core requires dict positions
    if not isinstance(positions, dict):
        raise TypeError(f"scheduler_run_v2 positions must be dict, got {type(positions)}")

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
    # Normalize positions for scheduler_core: it expects a dict keyed by (symbol, strategy)
    # _load_open_positions_from_trades returns a List[Position] for debug friendliness.
    if isinstance(positions, list):
        _pos_map: Dict[Tuple[str, str], Position] = {}
        for p in positions:
            try:
                sym = (getattr(p, "symbol", "") or "").strip()
                strat = (getattr(p, "strategy", "") or "").strip()
                if not sym:
                    continue
                _pos_map[(sym, strat)] = p
            except Exception:
                continue
        positions = _pos_map
    elif not isinstance(positions, dict):
        positions = {}
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

# ------------------------------------------------------------------
# Last-price helper for risk calculation + exits
# ------------------------------------------------------------------
def _last_price_safe(symbol: str) -> float:
    """
    Best-effort last price lookup used by debug and risk endpoints.

    Tries, in order:
      1) Global `contexts` (if present) 5m closes
      2) br_router helpers (get_last_price, get_ticker)
      3) br_router.get_bars("5Min")

    On any failure, returns 0.0 instead of raising, so that callers like
    /debug/positions and /scheduler/core_debug_risk never crash purely due
    to price lookup issues.
    """
    # 1) Try cached 5m bars from global contexts, if available
    try:
        ctxs = globals().get("contexts") or {}
        if isinstance(ctxs, dict):
            ctx = ctxs.get(symbol)
        else:
            ctx = None
        if ctx:
            five = ctx.get("five") or {}
            closes = five.get("close") or []
            if closes:
                return float(closes[-1])
    except Exception:
        # contexts missing or malformed; fall through to broker lookups
        pass

    # 2) Fallback: ask broker router for a last price
    br = None
    try:
        import br_router as br_mod  # type: ignore[import]
        br = br_mod
    except Exception:
        br = None

    if br is not None:
        # Prefer a dedicated helper if you have it
        try:
            if hasattr(br, "get_last_price"):
                px = br.get_last_price(symbol)
                if px is not None:
                    return float(px)
        except Exception:
            pass

        # Otherwise try ticker-like helpers if present
        try:
            if hasattr(br, "get_ticker"):
                t = br.get_ticker(symbol)
                if isinstance(t, dict):
                    for k in ("last", "price", "c", "close"):
                        v = t.get(k)
                        if v is not None:
                            return float(v)
        except Exception:
            pass

        # Finally, try OHLC bars as a last resort
        try:
            if hasattr(br, "get_bars"):
                bars = br.get_bars(symbol, timeframe="5Min", limit=1)
                if isinstance(bars, list) and bars:
                    row = bars[-1]
                    if isinstance(row, dict):
                        for k in ("c", "close", "price", "last"):
                            v = row.get(k)
                            if v is not None:
                                return float(v)
        except Exception:
            pass

    # Absolute fallback: unknown price
    return 0.0
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

        # ------------------------------------------------------------------
        # Policy-aligned universe: restrict scheduler symbols to whitelist + risk policy
        # (prevents scanning/trading paused pairs even if SYMBOLS env includes them).
        # ------------------------------------------------------------------
        try:
            wl_map = load_whitelist_config() or {}
            rk_cfg = load_risk_config() or {}

            # Union of whitelisted symbols for selected strategies (if configured)
            wl_union: set[str] = set()
            for st in strats:
                for sym in (wl_map.get(st) or []):
                    s = str(sym).strip().upper()
                    if s:
                        wl_union.add(s)

            # Risk policy guards
            avoid_pairs = set(
                str(x).strip().upper()
                for x in ((rk_cfg.get("avoid_pairs") or []) if isinstance(rk_cfg, dict) else [])
                if str(x).strip()
            )

            risk_caps = (rk_cfg.get("risk_caps") or {}) if isinstance(rk_cfg, dict) else {}
            caps_map = (risk_caps.get("max_notional_per_symbol") or {}) if isinstance(risk_caps, dict) else {}
            default_cap = float(risk_caps.get("default") or 0.0) if isinstance(risk_caps, dict) else 0.0

            def _pair_key(s: str) -> str:
                return str(s).strip().upper().replace("/", "")

            def _cap_for(sym: str) -> float:
                k = _pair_key(sym)
                v = caps_map.get(k) if isinstance(caps_map, dict) else None
                try:
                    return float(v) if v is not None else float(default_cap or 0.0)
                except Exception:
                    return float(default_cap or 0.0)

            filtered: List[str] = []
            for sym in syms:
                su = str(sym).strip().upper()
                if not su:
                    continue
                if wl_union and su not in wl_union:
                    continue
                if su in avoid_pairs or _pair_key(su) in avoid_pairs:
                    continue
                if _cap_for(su) <= 0:
                    continue
                filtered.append(su)

            if filtered:
                syms = filtered
        except Exception:
            pass
        limit = int(payload.get("limit", int(os.getenv("SCHED_LIMIT", "300") or 300)))
        notional = float(payload.get("notional", float(os.getenv("SCHED_NOTIONAL", "25") or 25)))

        # --- whitelist enforcement (per-strategy universe) -------------------
        enforce_wl = str(os.getenv("ENFORCE_STRAT_WHITELIST", "0")).lower() in ("1", "true", "yes")
        wl_map: Dict[str, List[str]] = {}
        if enforce_wl:
            wl_map = load_whitelist_config() or {}
        if enforce_wl and wl_map:
            wl_union: set[str] = set()
            for _st in strats:
                _allowed = wl_map.get(_st, [])
                if _allowed:
                    wl_union |= set(_allowed)
            if wl_union:
                syms = sorted(wl_union)
                symbols_csv = ",".join(syms)
                log.info("Whitelist enforced: symbols now=%s", symbols_csv)
        msg = (
            f"Scheduler pass: strats={','.join(strats)} tf={tf} "
            f"limit={limit} notional={notional} dry={dry} symbols={symbols_csv}"
        )
        log.info(msg)

        # Load open positions keyed by (symbol, strategy).
        positions = _load_open_positions_from_trades(use_strategy_col=True)
        # Normalize positions for scheduler_core: it expects a dict keyed by (symbol, strategy)
        # _load_open_positions_from_trades returns a List[Position] for debug friendliness.
        if isinstance(positions, list):
            _pos_map: Dict[Tuple[str, str], Position] = {}
            for p in positions:
                try:
                    sym = getattr(p, 'symbol', None) or getattr(p, 'sym', None)
                    strat = getattr(p, 'strategy', None) or getattr(p, 'strat', None)
                    if not sym or not strat:
                        continue
                    _pos_map[(sym, strat)] = p
                except Exception:
                    continue
            positions = _pos_map
        elif not isinstance(positions, dict):
            positions = {}

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
                _ctx = contexts
                if enforce_wl and wl_map:
                    _allowed = set(wl_map.get(strat, []) or [])
                    if _allowed:
                        _ctx = {s: contexts[s] for s in contexts.keys() if s in _allowed}
                res = book.scan(req, _ctx)
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

    # small helpers for super-safe config access
    def _cfg_get(d: Any, key: str, default: Any = None) -> Any:
        return d.get(key, default) if isinstance(d, dict) else default

    # ------------------------------------------------------------------
    # Helper: env bool
    # ------------------------------------------------------------------
    def _env_bool(key: str, default: bool) -> bool:
        v = os.getenv(key)
        if v is None:
            return default
        return str(v).lower() in ("1", "true", "yes", "on")

    payload = payload or {}
    
    # ---------------------------------------------------------------
    # Helpers: intent_id + broker response extraction (attribution)
    # ---------------------------------------------------------------
    def _ensure_intent_id(intent_obj: Any) -> Optional[str]:
        try:
            meta = getattr(intent_obj, "meta", None)
            if not isinstance(meta, dict):
                meta = {}
                try:
                    setattr(intent_obj, "meta", meta)
                except Exception:
                    pass
            iid = meta.get("intent_id")
            if not iid:
                iid = uuid.uuid4().hex
                meta["intent_id"] = iid
            return str(iid)
        except Exception:
            return None
            
    def _canon_symbol(sym: str) -> str:
        s = (sym or "").strip().upper()

        # Common Kraken asset-code wrappers you’re seeing in fills:
        # XLTCZ/USD -> LTC/USD
        # XXRPZ/USD -> XRP/USD
        # Keep USD, normalize base.
        if "/" in s:
            base, quote = s.split("/", 1)
            # Strip leading X/Z wrappers often used by Kraken asset codes
            while len(base) > 3 and base[0] in "XZ":
                base = base[1:]
            while len(quote) > 3 and quote[0] in "XZ":
                quote = quote[1:]
            return f"{base}/{quote}"

        return s


    def _extract_ordertxid_userref(resp: Any) -> Tuple[Optional[str], Optional[str]]:
        """
        Best-effort extraction for Kraken-style responses.
        Returns (ordertxid, userref).
        """
        ordertxid = None
        userref = None
        try:
            if isinstance(resp, dict):
                # direct keys (if you already normalize somewhere)
                ordertxid = resp.get("ordertxid") or resp.get("order_txid") or resp.get("orderId")
                userref = resp.get("userref") or resp.get("user_ref")

                # Kraken common: {"result":{"txid":["..."]}}
                if not ordertxid:
                    r = resp.get("result") if isinstance(resp.get("result"), dict) else None
                    tx = (r.get("txid") if r else None)
                    if isinstance(tx, list) and tx:
                        ordertxid = tx[0]
                    elif isinstance(tx, str):
                        ordertxid = tx

                # Sometimes: top-level "txid"
                if not ordertxid:
                    tx = resp.get("txid")
                    if isinstance(tx, list) and tx:
                        ordertxid = tx[0]
                    elif isinstance(tx, str):
                        ordertxid = tx
        except Exception:
            pass

        return (str(ordertxid) if ordertxid else None, str(userref) if userref else None)


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

    # ------------------------------------------------------------------
    # Policy-aligned universe: restrict scheduler symbols to whitelist + risk policy
    # (prevents scanning/trading paused pairs even if SYMBOLS env includes them).
    # ------------------------------------------------------------------
    try:
        wl_map = load_whitelist_config() or {}
        rk_cfg = load_risk_config() or {}
        # Union of whitelisted symbols for selected strategies
        wl_union = set()
        for st in strats:
            for sym in (wl_map.get(st) or []):
                wl_union.add(str(sym).strip().upper())

        # Risk policy guards
        avoid_pairs = set(str(x).strip().upper() for x in (rk_cfg.get("avoid_pairs") or []) if str(x).strip())
        caps_map = (((rk_cfg.get("risk_caps") or {}).get("max_notional_per_symbol")) or {}) if isinstance(rk_cfg, dict) else {}
        default_cap = float((((rk_cfg.get("risk_caps") or {}).get("default")) or 0.0)) if isinstance(rk_cfg, dict) else 0.0

        def _pair_key(s: str) -> str:
            return str(s).strip().upper().replace("/", "")

        def _cap_for(sym: str) -> float:
            k = _pair_key(sym)
            v = caps_map.get(k)
            try:
                return float(v) if v is not None else float(default_cap or 0.0)
            except Exception:
                return float(default_cap or 0.0)

        filtered = []
        for sym in syms:
            k = _pair_key(sym)
            if wl_union and sym not in wl_union:
                continue
            if k in avoid_pairs:
                continue
            cap = _cap_for(sym)
            if cap <= 0:
                continue
            filtered.append(sym)

        if filtered:
            syms = filtered
    except Exception:
        pass

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
        ",".join(syms),
    )

    # ------------------------------------------------------------------
    # Load broker router + guard
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

def _reconcile_positions_with_kraken(pos_list: Any) -> Any:
    """Return journal positions filtered/scaled to live Kraken balances."""
    if not isinstance(pos_list, list):
        return pos_list
    try:
        kr_raw = broker_kraken.positions() or []
    except Exception:
        kr_raw = []
    kr_map: Dict[str, float] = {}
    for r in kr_raw:
        try:
            a = str(r.get("asset","")).upper().strip()
            if not a:
                continue
            kr_map[a] = float(r.get("qty",0) or 0)
        except Exception:
            continue

    by_asset: Dict[str, List[Any]] = {}
    for p in pos_list:
        sym = (getattr(p, "symbol", "") or "").strip()
        asset = sym.split("/")[0].upper().strip() if "/" in sym else sym.upper().strip()[:4]
        by_asset.setdefault(asset, []).append(p)

    out: List[Any] = []
    for asset, plist in by_asset.items():
        kqty = float(kr_map.get(asset, 0) or 0)
        if kqty <= 0:
            # Kraken doesn't hold it -> no exits should be generated for it
            continue
        jtot = 0.0
        for p in plist:
            try:
                jtot += float(getattr(p, "qty", 0) or 0)
            except Exception:
                pass
        if jtot <= 0:
            continue
        scale = kqty / jtot
        for p in plist:
            try:
                p.qty = float(getattr(p, "qty", 0) or 0) * scale
            except Exception:
                pass
            out.append(p)
    return out

    # Load positions & risk config
    # ------------------------------------------------------------------
    positions = _load_open_positions_from_trades(use_strategy_col=True)
    # Normalize positions for scheduler_core: it expects a dict keyed by (symbol, strategy)
    # _load_open_positions_from_trades returns a List[Position] for debug friendliness.
    if isinstance(positions, list):
        _pos_map: Dict[Tuple[str, str], Position] = {}
        for p in positions:
            try:
                sym = (getattr(p, "symbol", "") or "").strip()
                strat = (getattr(p, "strategy", "") or "").strip()
                if not sym:
                    continue
                _pos_map[(sym, strat)] = p
            except Exception:
                continue
        positions = _pos_map
    elif not isinstance(positions, dict):
        positions = {}
    risk_cfg = load_risk_config() or {}
    risk_engine = RiskEngine(risk_cfg)

    # ------------------------------------------------------------------
    # Preload bar contexts once (match /debug/strategy_scan + StrategyBook)
    # ------------------------------------------------------------------
    contexts: Dict[str, Any] = {}

    def _safe_series(bars, key: str):
        vals = []
        if isinstance(bars, list):
            for row in bars:
                if isinstance(row, dict) and key in row:
                    vals.append(row[key])
        return vals

    def _normalize_symbol_for_bars(sym: str) -> str:
            # If br_router has a normalizer, use it
            try:
                if br is not None and hasattr(br, "normalize_symbol"):
                    return str(br.normalize_symbol(sym))
            except Exception:
                pass
    
            # Minimal Kraken altname fixups (common offenders)
            if sym == "XLTCZ/USD":
                return "LTC/USD"
            if sym == "XXRPZ/USD":
                return "XRP/USD"
            return sym
    
    
    for sym in syms:
        sym_can = _canon_symbol(sym)
        try:
            bars_sym = _normalize_symbol_for_bars(sym)
            
            one  = br.get_bars(bars_sym, timeframe="1Min", limit=limit)
            five = br.get_bars(bars_sym, timeframe=tf,     limit=limit)


            if not one or not five:
                contexts[sym_can] = None
                telemetry.append({"symbol": sym, "stage": "preload_bars", "ok": False, "reason": "no_bars"})
            else:
                contexts[sym_can] = {
                    "one":  {"close": _safe_series(one, "c"),  "high": _safe_series(one, "h"),  "low": _safe_series(one, "l")},
                    "five": {"close": _safe_series(five, "c"), "high": _safe_series(five, "h"), "low": _safe_series(five, "l")},
                }
        except Exception as e:
            contexts[sym_can] = None
            telemetry.append({"symbol": sym, "stage": "preload_bars", "ok": False, "error": f"{e.__class__.__name__}: {e}"})

    


    # ------------------------------------------------------------------
    # Last-price helper for risk calculation + exits
    # ------------------------------------------------------------------
    def _last_price_safe(symbol: str) -> float:
        ctx = contexts.get(symbol)
        if not ctx:
            return 0.0
        five = ctx.get("five") or {}
        closes = five.get("close") or []
        if not closes:
            return 0.0
        try:
            return float(closes[-1])
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Build SchedulerConfig and run scheduler_core once
    # ------------------------------------------------------------------
    now = dt.datetime.utcnow()
    # Defensive: scheduler_core requires dict positions
    if not isinstance(positions, dict):
        raise TypeError(f"scheduler_run_v2 positions must be dict, got {type(positions)}")

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
    # Helper: exit priority (used for deduplication)
    # ------------------------------------------------------------------
    def _exit_priority(kind: str) -> int:
        """
        Higher number = higher priority exit.
        stop_loss > exit > take_profit
        """
        if not kind:
            return 0
        k = str(kind).strip().lower()
        if k == "stop_loss":
            return 3
        if k == "exit":
            return 2
        if k == "take_profit":
            return 1
        return 0

    # ------------------------------------------------------------------
    # Deduplicate intents: exits take precedence over entries on same (sym,strat)
    # Canonicalize (kind/symbol/strategy/side) so keys don't drift.
    # ------------------------------------------------------------------
    exit_kinds = {"exit", "take_profit", "stop_loss"}
    entry_kinds = {"entry", "scale"}

    best_exit: Dict[Tuple[str, str], Any] = {}
    entries: List[Any] = []

    for intent in result.intents:
        # Canonicalize fields defensively
        k_kind = (str(getattr(intent, "kind", "") or "")).strip().lower()
        k_sym = _canon_symbol(str(getattr(intent, "symbol", "") or ""))
        k_str = (str(getattr(intent, "strategy", "") or "")).strip().lower()
        k_side = (str(getattr(intent, "side", "") or "")).strip().lower()

        # Write back canonical values so downstream uses consistent values
        try:
            intent.kind = k_kind
        except Exception:
            pass
        try:
            intent.symbol = k_sym
        except Exception:
            pass
        try:
            intent.strategy = k_str
        except Exception:
            pass
        try:
            intent.side = k_side
        except Exception:
            pass

        key = (k_sym, k_str)

        if k_kind in exit_kinds:
            prev = best_exit.get(key)
            if prev is None or _exit_priority(k_kind) > _exit_priority(getattr(prev, "kind", "")):
                best_exit[key] = intent
        elif k_kind in entry_kinds:
            entries.append(intent)
        else:
            # unknown kind: just pass through as a generic action
            entries.append(intent)

    # Combine exits + entries, making sure no entry is emitted on same pass
    # when an exit exists for that (symbol,strategy).
    final_intents: List[Any] = []

    # First, add exits
    for _, intent in best_exit.items():
        final_intents.append(intent)

    # Then, add entries only where no exit exists
    for intent in entries:
        key = (getattr(intent, "symbol", None), getattr(intent, "strategy", None))
        if key in best_exit:
            telemetry.append(
                {
                    "symbol": getattr(intent, "symbol", None),
                    "strategy": getattr(intent, "strategy", None),
                    "kind": getattr(intent, "kind", None),
                    "side": getattr(intent, "side", None),
                    "reason": "dropped_entry_due_to_exit_same_pass",
                    "source": "scheduler_v2",
                }
            )
            continue
        final_intents.append(intent)

    # ------------------------------------------------------------------
    # GLOBAL RISK LATCH:
    # If we are emitting any global flatten/stop-loss intents, block ALL entries
    # this pass. This is stop-the-bleed behavior.
    # ------------------------------------------------------------------
    global_risk_active = False
    try:
        for it in final_intents:
            r = (str(getattr(it, "reason", "") or "")).lower()
            src = (str(getattr(it, "source", "") or "")).lower()
            if ("global_daily_flatten" in r) or ("global_stop_loss" in r) or ("global_risk_engine" in src):
                global_risk_active = True
                break
    except Exception:
        global_risk_active = False

    
    # ------------------------------------------------------------------
    # GLOBAL SYMBOL GATE:
    # Allow at most ONE action per *symbol* per scheduler pass, even if multiple
    # strategies emit intents for the same symbol. This prevents burst trading
    # (e.g., two different strategies both buying SUI in the same tick).
    #
    # Selection rules (per symbol):
    #   1) Prefer exits over entries
    #   2) Prefer stop_loss > exit > take_profit > entry/scale > other
    #   3) If tie, keep the first encountered (stable)
    # ------------------------------------------------------------------
    def _symbol_kind_priority(kind: str) -> int:
        k = (str(kind or "")).strip().lower()
        if k == "stop_loss":
            return 50
        if k == "exit":
            return 40
        if k == "take_profit":
            return 30
        if k in ("entry", "scale"):
            return 10
        return 0

    best_by_symbol: Dict[str, Any] = {}
    dropped_by_symbol: List[Any] = []

    try:
        for it in list(final_intents):
            sym_k = _canon_symbol(str(getattr(it, "symbol", "") or ""))
            kind_k = (str(getattr(it, "kind", "") or "")).strip().lower()
            pr = _symbol_kind_priority(kind_k)

            prev = best_by_symbol.get(sym_k)
            if prev is None:
                best_by_symbol[sym_k] = it
                continue

            prev_kind = (str(getattr(prev, "kind", "") or "")).strip().lower()
            prev_pr = _symbol_kind_priority(prev_kind)

            if pr > prev_pr:
                dropped_by_symbol.append(prev)
                best_by_symbol[sym_k] = it
            else:
                dropped_by_symbol.append(it)

        if dropped_by_symbol:
            for it in dropped_by_symbol:
                telemetry.append(
                    {
                        "symbol": getattr(it, "symbol", None),
                        "strategy": getattr(it, "strategy", None),
                        "kind": getattr(it, "kind", None),
                        "side": getattr(it, "side", None),
                        "reason": "blocked_by_global_symbol_gate",
                        "source": "scheduler_v2",
                    }
                )

        # Replace final_intents with gated list (stable order: original order filtered)
        gated: List[Any] = []
        keep_ids = set(id(v) for v in best_by_symbol.values())
        for it in final_intents:
            if id(it) in keep_ids:
                gated.append(it)
        final_intents = gated
    except Exception as e:
        telemetry.append({"stage": "global_symbol_gate", "ok": False, "error": f"{e.__class__.__name__}: {e}"})
# ------------------------------------------------------------------
    # Apply guard + per-symbol caps + loss-zone no-rebuy; then route
    # Also: stop-the-bleed duplicate action latch (1 action per (sym,strat) per run)
    # ------------------------------------------------------------------
    sent_keys: set = set()

    for intent in final_intents:
        # Ensure every intent has a stable id for attribution
        intent_id = _ensure_intent_id(intent)

        # Normalize side again (belt + suspenders)
        side = (str(getattr(intent, "side", "") or "")).strip().lower()
        try:
            intent.side = side
        except Exception:
            pass

        # Force strategy label for truly missing strategies (global/system intents)
        try:
            if not getattr(intent, "strategy", None):
                intent.strategy = "global"
        except Exception:
            pass

        # SELL-exit gate: never attempt to sell an asset that Kraken reports as 0 available.
        # This prevents "avail=0.0" churn when the journal thinks a position exists but Kraken does not.
        kind = (str(getattr(intent, "kind", "") or "")).strip().lower()
        if side == "sell" and kind in exit_kinds:
            base = _base_asset_from_symbol(getattr(intent, "symbol", ""))
            if float(_live_bal.get(base, 0.0)) <= 0.0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": kind,
                        "side": side,
                        "reason": f"no_live_balance:{base}",
                        "source": "scheduler_v2",
                    }
                )
                actions.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "intent_id": intent_id,
                        "side": side,
                        "kind": kind,
                        "notional": float(getattr(intent, "notional", 0.0) or 0.0),
                        "reason": getattr(intent, "reason", None),
                        "dry": bool(dry),
                        "status": "skipped_no_live_balance",
                        "error": f"no_live_balance:{base}",
                    }
                )
                continue

        # Canonicalize symbol for consistent position/context access
        try:
            intent.symbol = _canon_symbol(str(getattr(intent, "symbol", "") or ""))
        except Exception:
            pass

        # Global risk latch: block entries immediately
        if global_risk_active and getattr(intent, "kind", "") in entry_kinds:
            telemetry.append(
                {
                    "symbol": getattr(intent, "symbol", None),
                    "strategy": getattr(intent, "strategy", None),
                    "kind": getattr(intent, "kind", None),
                    "side": side,
                    "reason": "blocked_entry_due_to_global_risk_active",
                    "source": "scheduler_v2",
                }
            )
            continue

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
        if guard_allows is not None and getattr(intent, "kind", "") in entry_kinds:
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
                    "kind": getattr(intent, "kind", None),
                    "side": side,
                    "reason": guard_reason,
                    "source": "guard_allows",
                }
            )
            continue

        # Advisory-only guard warnings (allow but log)
        if guard_reason and guard_reason != "ok":
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": getattr(intent, "kind", None),
                    "side": side,
                    "reason": f"guard_warn:{guard_reason}",
                    "source": "guard_allows",
                }
            )

        # Decide final notional to send
        final_notional: float = 0.0

        # ----- ENTRY / SCALE: use intent.notional (after caps) --------------------
        if getattr(intent, "kind", "") in entry_kinds:
            if getattr(intent, "notional", None) is None or float(getattr(intent, "notional", 0.0) or 0.0) <= 0.0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": getattr(intent, "kind", None),
                        "side": side,
                        "reason": "entry_without_notional",
                        "source": "scheduler_v2",
                    }
                )
                continue

            allowed_cap, adjusted_notional, cap_reason = risk_engine.enforce_symbol_cap(
                symbol=intent.symbol,
                strat=intent.strategy,
                pos=snap,
                notional_value=float(getattr(intent, "notional", 0.0)),
                last_price_fn=_last_price_safe,
                now=now,
            )
            if not allowed_cap or float(adjusted_notional or 0.0) <= 0.0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": getattr(intent, "kind", None),
                        "side": side,
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
                        "kind": getattr(intent, "kind", None),
                        "side": side,
                        "reason": "loss_zone_no_rebuy_below",
                        "source": "risk_engine.is_loss_zone_norebuy_block",
                    }
                )
                continue

        # ----- EXIT / TP / SL: cap exit to actual position value; never sell w/o qty -----
        else:
            qty_here = float(getattr(pm_pos, "qty", 0.0) or 0.0)
            px = _last_price_safe(intent.symbol)
            if px <= 0.0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": getattr(intent, "kind", None),
                        "side": side,
                        "reason": "no_price_for_exit",
                        "source": "scheduler_v2",
                    }
                )
                continue

            # If broker side is SELL, we must have something to sell
            if side == "sell" and qty_here <= 0.0:
                telemetry.append(
                    {
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": getattr(intent, "kind", None),
                        "side": side,
                        "reason": "exit_skipped_no_position_to_sell",
                        "source": "scheduler_v2",
                    }
                )
                continue

            # Choose exit size (use intent.notional if provided, otherwise flatten)
            if getattr(intent, "notional", None) is not None and float(getattr(intent, "notional", 0.0) or 0.0) > 0.0:
                final_notional = float(getattr(intent, "notional", 0.0))
            else:
                final_notional = abs(qty_here) * px  # flatten full position

            # Cap SELL exits so we never try to sell more than position value
            if side == "sell":
                max_notional = abs(qty_here) * px * 0.995  # buffer for fees/rounding
                final_notional = min(final_notional, max_notional)

        # Valid final_notional?
        if final_notional <= 0.0:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": getattr(intent, "kind", None),
                    "side": side,
                    "reason": "non_positive_final_notional",
                    "source": "scheduler_v2",
                }
            )
            continue

        # Ignore dust for entries + TP/SL; still allow generic exits (e.g. daily_flatten)
        if final_notional < MIN_NOTIONAL_USD and getattr(intent, "kind", "") in {"entry", "scale"}:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": getattr(intent, "kind", None),
                    "side": side,
                    "reason": f"below_min_notional:{final_notional:.4f}<{MIN_NOTIONAL_USD}",
                    "source": "scheduler_v2",
                }
            )
            continue

        # Stop-the-bleed: one action per (symbol,strategy) per run
        send_key = (intent.symbol, intent.strategy)
        if send_key in sent_keys:
            telemetry.append(
                {
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": getattr(intent, "kind", None),
                    "side": side,
                    "reason": "blocked_duplicate_action_same_run",
                    "source": "scheduler_v2",
                }
            )
            continue
        sent_keys.add(send_key)

        action_record: Dict[str, Any] = {
            "symbol": intent.symbol,
            "strategy": intent.strategy,
            "intent_id": intent_id,
            "side": side,
            "kind": getattr(intent, "kind", None),
            "notional": final_notional,
            "reason": getattr(intent, "reason", None),
            "dry": bool(dry),
        }

        if dry or br is None:
            action_record["status"] = "skipped_dry_run"
            try:
                append_journal_v2(
                    {
                        "ts": time.time(),
                        "source": "scheduler_v2",
                        "intent_id": intent_id,
                        "ordertxid": None,
                        "userref": None,
                        "symbol": intent.symbol,
                        "strategy": intent.strategy,
                        "kind": getattr(intent, "kind", None),
                        "side": side,
                        "notional": final_notional,
                        "reason": getattr(intent, "reason", None),
                        "dry": bool(dry),
                        "status": action_record.get("status"),
                        "response": action_record.get("response"),
                        "error": action_record.get("error"),
                    }
                )
            except Exception:
                pass

            actions.append(action_record)
            continue

        # ------------------------------------------------------------------
        # Send to broker via br_router.market_notional
        # ------------------------------------------------------------------

        # ------------------------------------------------------------------
        # Stop-the-bleed cooldown latch (prevents rapid churn)
        # NOTE: enforced using in-memory latch so it applies immediately.
        # ------------------------------------------------------------------
        try:
            cooldown_same = int(os.getenv("SCHED_COOLDOWN_SAME_SIDE_SECONDS", "0") or 0)
            cooldown_flip = int(os.getenv("SCHED_COOLDOWN_FLIP_SECONDS", "0") or 0)
            min_hold_seconds = int(os.getenv("SCHED_MIN_HOLD_SECONDS", "0") or 0)
            kind_l = str(getattr(intent, "kind", "") or "").strip().lower()

            # Stop-loss bypasses cooldown/min-hold (safety first)
            if kind_l != "stop_loss" and (cooldown_same > 0 or cooldown_flip > 0 or (min_hold_seconds > 0 and kind_l in ("entry", "scale", "scale_in", "add"))):
                key = (intent.symbol, intent.strategy)
                now = time.time()
                with _LAST_ACTION_LATCH_LOCK:
                    last = _LAST_ACTION_LATCH.get(key)

                if last:
                    last_ts = float(last.get("ts") or 0.0)
                    last_side = str(last.get("side") or "").strip().lower()
                    age = now - last_ts

                    if cooldown_same > 0 and last_side == side and age < cooldown_same:
                        action_record["status"] = "skipped_cooldown_same_side"
                        action_record["error"] = f"cooldown_same_side:{age:.1f}s<{cooldown_same}s"
                        try: log.info("scheduler_v2: %s %s %s blocked (%s)", intent.strategy, intent.symbol, side, action_record["error"])
                        except Exception: pass
                        actions.append(action_record)
                        continue

                    if cooldown_flip > 0 and last_side and last_side != side and age < cooldown_flip:
                        action_record["status"] = "skipped_cooldown_flip"
                        action_record["error"] = f"cooldown_flip:{age:.1f}s<{cooldown_flip}s"
                        try: log.info("scheduler_v2: %s %s %s blocked (%s)", intent.strategy, intent.symbol, side, action_record["error"])
                        except Exception: pass
                        actions.append(action_record)
                        continue

                    if min_hold_seconds > 0 and kind_l in ("entry", "scale", "scale_in", "add") and age < min_hold_seconds:
                        action_record["status"] = "skipped_min_hold"
                        action_record["error"] = f"min_hold:{age:.1f}s<{min_hold_seconds}s"
                        try: log.info("scheduler_v2: %s %s %s blocked (%s)", intent.strategy, intent.symbol, side, action_record["error"])
                        except Exception: pass
                        actions.append(action_record)
                        continue
        except Exception:
            pass
        try:
            # Open-orders guard (patch #2): optionally skip entries for pairs that already have an open order.
            if _parse_bool_env("OPENORDER_GUARD", True) and not cfg.get("dry"):
                try:
                    ttl = float(os.getenv("OPENORDER_GUARD_TTL", "20"))
                except Exception:
                    ttl = 20.0
                try:
                    kpair = to_kraken(intent.symbol)
                except Exception:
                    kpair = None
                if kpair:
                    open_pairs = kraken_open_orders_pairs_cached(ttl_sec=ttl)
                    if kpair in open_pairs:
                        log.info("OpenOrders guard: skip %s (pair=%s) because open order exists", intent.symbol, kpair)
                        telemetry.append({"t": "skip_open_order", "symbol": intent.symbol, "pair": kpair, "strat": intent.strat})
                        continue

            resp = br.market_notional(
                symbol=intent.symbol,
                side=side,
                notional=final_notional,
                strategy=intent.strategy,
            )
            action_record["status"] = "sent"
            action_record["response"] = resp


            # Update anti-churn latch immediately on send
            try:
                with _LAST_ACTION_LATCH_LOCK:
                    _LAST_ACTION_LATCH[(intent.symbol, intent.strategy)] = {
                        "ts": time.time(),
                        "side": side,
                        "kind": str(getattr(intent, "kind", None) or ""),
                    }
            except Exception:
                pass
            otx, ur = _extract_ordertxid_userref(resp)
            if otx:
                action_record["ordertxid"] = otx
            if ur:
                action_record["userref"] = ur

        except Exception as e:
            log.error("scheduler_v2: broker error for %s %s: %s", intent.symbol, side, e)
            action_record["status"] = "error"
            action_record["error"] = f"{e.__class__.__name__}: {e}"

        # Journal v2: record every executed (or failed) broker call
        try:
            append_journal_v2(
                {
                    "ts": time.time(),
                    "source": "scheduler_v2",
                    "intent_id": intent_id,
                    "ordertxid": action_record.get("ordertxid"),
                    "userref": action_record.get("userref"),
                    "symbol": intent.symbol,
                    "strategy": intent.strategy,
                    "kind": getattr(intent, "kind", None),
                    "side": side,
                    "notional": final_notional,
                    "reason": getattr(intent, "reason", None),
                    "dry": bool(dry),
                    "status": action_record.get("status"),
                    "response": action_record.get("response"),
                    "error": action_record.get("error"),
                }
            )
        except Exception:
            pass

        actions.append(action_record)



    # ------------------------------------------------------------------
    # Build per-(strategy,symbol) universe summary for debugging
    # ------------------------------------------------------------------
    universe_map: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def _uni(symbol: str, strategy: str) -> Dict[str, Any]:
        key = (symbol, strategy)
        if key not in universe_map:
            universe_map[key] = {
                "symbol": symbol,
                "strategy": strategy,
                "has_context": bool(contexts.get(symbol)),
                "had_scan": False,
                "had_entry_intent": False,
                "had_exit_intent": False,
                "blocked_by_guard": False,
                "blocked_by_cap": False,
                "blocked_by_loss_zone": False,
                "below_min_notional": False,
                "actions_sent": 0,
                "actions_dry": 0,
                "reasons": [],
            }
        return universe_map[key]

    # Prime universe with all (strat, symbol) pairs from config
    for strat in strats:
        for sym in syms:
            _uni(sym, strat)

    # Fold telemetry into universe_map
    for row in telemetry:
        sym = row.get("symbol")
        strat = row.get("strategy")
        if not sym or not strat:
            continue
        u = _uni(sym, strat)
        kind = row.get("kind")
        source = row.get("source") or ""
        reason = row.get("reason")

        if kind in {"entry", "scale", "entry_skip"}:
            u["had_scan"] = True
            if kind in {"entry", "scale"}:
                u["had_entry_intent"] = True

        if kind in {"exit", "take_profit", "stop_loss", "exit_skip"}:
            u["had_scan"] = True
            if kind in {"exit", "take_profit", "stop_loss"}:
                u["had_exit_intent"] = True

        if source == "guard_allows":
            u["blocked_by_guard"] = True
        if source == "risk_engine.enforce_symbol_cap":
            u["blocked_by_cap"] = True
        if source == "risk_engine.is_loss_zone_norebuy_block":
            u["blocked_by_loss_zone"] = True
        if isinstance(reason, str) and reason.startswith("below_min_notional:"):
            u["below_min_notional"] = True

        if reason:
            tag = f"{source}:{reason}" if source else str(reason)
            u["reasons"].append(tag)

    # Fold actions into universe_map
    for act in actions:
        sym = act.get("symbol")
        strat = act.get("strategy")
        if not sym or not strat:
            continue
        u = _uni(sym, strat)
        status = act.get("status")
        if status == "sent":
            u["actions_sent"] += 1
        elif status == "skipped_dry_run":
            u["actions_dry"] += 1

    # Finalize universe list (dedupe reasons)
    universe: List[Dict[str, Any]] = []
    for (sym, strat), u in universe_map.items():
        seen_reasons = set()
        uniq_reasons: List[str] = []
        for r in u.get("reasons", []):
            if r not in seen_reasons:
                uniq_reasons.append(r)
                seen_reasons.add(r)
        u["reasons"] = uniq_reasons
        universe.append(u)

    # ------------------------------------------------------------------
    # FINAL RETURN — log telemetry, then respond
    # ------------------------------------------------------------------
    try:
        # Log telemetry for offline Advisor v2 analysis.
        # We log regardless of dry/live, but the rows themselves include any
        # dry/run flags you added at the source.
        _append_telemetry_rows(telemetry)
    except Exception as e:
        # Never let telemetry logging break the API
        log.warning("scheduler_v2: failed to log telemetry: %s", e)

    return {
        "ok": True,
        "dry": bool(dry),
        "config": config_snapshot,
        "actions": actions,
        "telemetry": telemetry,
        "universe": universe,
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
    # Normalize positions for scheduler_core: it expects a dict keyed by (symbol, strategy)
    # _load_open_positions_from_trades returns a List[Position] for debug friendliness.
    if isinstance(positions, list):
        _pos_map: Dict[Tuple[str, str], Position] = {}
        for p in positions:
            try:
                sym = (getattr(p, "symbol", "") or "").strip()
                strat = (getattr(p, "strategy", "") or "").strip()
                if not sym:
                    continue
                _pos_map[(sym, strat)] = p
            except Exception:
                continue
        positions = _pos_map
    elif not isinstance(positions, dict):
        positions = {}
    risk_cfg = load_risk_config() or {}
    contexts = { ... }  # the same structure you currently pass to StrategyBook

    # Defensive: scheduler_core requires dict positions
    if not isinstance(positions, dict):
        raise TypeError(f"scheduler_run_v2 positions must be dict, got {type(positions)}")

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

def _preload_contexts(tf: str, symbols: List[str], limit: int) -> Dict[str, Any]:
    """
    Helper for /scheduler/core_debug_risk:
    Preload bar contexts (1Min + tf) for each symbol into a dictionary
    matching the shape expected by SchedulerConfig/StrategyBook.

    This is a read-only helper for debug/risk surfaces and does not send orders.
    """
    ctxs: Dict[str, Any] = {}
    try:
        import br_router as br  # type: ignore[import]
    except Exception:
        # If bar router is unavailable, return an empty context map.
        return ctxs

    def _safe_series(bars, key: str):
        vals = []
        if isinstance(bars, list):
            for row in bars:
                if isinstance(row, dict) and key in row:
                    vals.append(row[key])
        return vals

    def _normalize_symbol_for_bars(sym: str) -> str:
        s = str(sym or "")
        if "/" in s:
            base, quote = s.split("/", 1)
            return f"{base}{quote}"
        return s

    def _canon_symbol(sym: str) -> str:
        s = str(sym or "").upper()

        # Prefer router-level normalizer if present
        try:
            if hasattr(br, "normalize_symbol"):
                return str(br.normalize_symbol(s))
        except Exception:
            pass

        # Minimal Kraken-style altnames -> canonical spot symbols
        if s == "XBT/USD":
            return "BTC/USD"
        if s == "XLTCZ/USD":
            return "LTC/USD"
        if s == "XXRPZ/USD":
            return "XRP/USD"
        return s

    for sym in symbols or []:
        sym_can = _canon_symbol(sym)
        try:
            bars_sym = _normalize_symbol_for_bars(sym)
            one = br.get_bars(bars_sym, timeframe="1Min", limit=limit)
            five = br.get_bars(bars_sym, timeframe=tf, limit=limit)

            if not one or not five:
                ctxs[sym_can] = None
            else:
                ctxs[sym_can] = {
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
        except Exception:
            ctxs[sym_can] = None

    return ctxs

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
    # Normalize positions for scheduler_core: it expects a dict keyed by (symbol, strategy)
    # _load_open_positions_from_trades returns a List[Position] for debug friendliness.
    if isinstance(positions, list):
        _pos_map: Dict[Tuple[str, str], Position] = {}
        for p in positions:
            try:
                sym = (getattr(p, "symbol", "") or "").strip()
                strat = (getattr(p, "strategy", "") or "").strip()
                if not sym:
                    continue
                _pos_map[(sym, strat)] = p
            except Exception:
                continue
        positions = _pos_map
    elif not isinstance(positions, dict):
        positions = {}
    risk_cfg = load_risk_config() or {}

    # 3) Build contexts exactly like scheduler_run (reuse your existing code)
    contexts = _preload_contexts(tf=tf, symbols=symbols, limit=limit)
    # ^ if you don't have _preload_contexts yet, this is the same bar-loading
    #   logic that's currently inside scheduler_run before StrategyBook.scan.

    now = dt.datetime.utcnow()
    # Defensive: scheduler_core requires dict positions
    if not isinstance(positions, dict):
        raise TypeError(f"scheduler_run_v2 positions must be dict, got {type(positions)}")

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
                "one":  {"close": series(one,"c"), "high": series(one,"h"), "low": series(one,"l"), "volume": series(one, "v")},
                "five": {"close": series(five,"c"), "high": series(five,"h"), "low": series(five,"l"), "volume": series(five, "v")},
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

def _load_open_positions_from_trades(use_strategy_col: bool = True, include_legacy: bool = False) -> List[Position]:
    """Compute open (net) positions from the local `trades` table.

    This is a lightweight, deterministic view that does *not* depend on Kraken positions.
    It is used by /debug/positions and any journal sanity checks.

    Notes:
    - Long-only: buys add, sells reduce. If sells exceed current qty we zero the position.
    - Average price is maintained as a weighted average of *buys* for the remaining qty.
    """
    import sqlite3

    if not DB_PATH:
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        # Ensure table exists
        names = set(r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall())
        if "trades" not in names:
            return []

        rows = conn.execute(
            """
            SELECT ts, symbol, side, price, volume, COALESCE(strategy, '') AS strategy
            FROM trades
            ORDER BY ts ASC
            """
        ).fetchall()

        state: Dict[Tuple[str, str], Dict[str, float]] = {}
        for r in rows:
            sym = (r["symbol"] or "").strip()
            if not sym:
                continue
            strat = (r["strategy"] or "").strip() if use_strategy_col else "misc"
            side = (r["side"] or "").strip().lower()
            price = float(r["price"] or 0.0)
            vol = float(r["volume"] or 0.0)
            if vol <= 0:
                continue

            k = (strat or "misc", sym)
            st = state.setdefault(k, {"qty": 0.0, "avg": 0.0})

            qty = st["qty"]
            avg = st["avg"]

            if side == "buy":
                new_qty = qty + vol
                if new_qty > 0 and price > 0:
                    st["avg"] = ((avg * qty) + (price * vol)) / new_qty
                st["qty"] = new_qty
            elif side == "sell":
                # Reduce position; if we oversell, flatten.
                if vol >= qty:
                    st["qty"] = 0.0
                    st["avg"] = 0.0
                else:
                    st["qty"] = qty - vol
            else:
                # Unknown side; ignore.
                continue

        out: List[Position] = []
        for (strat, sym), st in state.items():
            if st["qty"] and st["qty"] > 0:
                out.append(Position(symbol=sym, strategy=strat, qty=st["qty"], avg_price=st["avg"]))

        out.sort(key=lambda p: (p.strategy or "", p.symbol or ""))
        return out
    finally:
        conn.close()
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
        
@app.get("/pnl2/daily")
def pnl2_daily(year: int, month: int):
    """
    Return daily realized P&L for a given month.
    year: 2025, month: 12 (1-based)
    """
    from datetime import datetime, timedelta

    try:
        # Days in month: go to the first of next month and step back a day
        if month == 12:
            next_month = datetime(year + 1, 1, 1)
        else:
            next_month = datetime(year, month + 1, 1)
        days_in_month = (next_month - timedelta(days=1)).day

        rows = []

        for day in range(1, days_in_month + 1):
            start_dt = datetime(year, month, day, 0, 0, 0)
            end_dt   = datetime(year, month, day, 23, 59, 59)

            # IMPORTANT: use same format as rest of PnL code ("YYYY-MM-DD HH:MM:SS")
            start = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            end   = end_dt.strftime("%Y-%m-%d %H:%M:%S")

            fills = _pnl__load_fills(start, end)
            engine_day = PositionEngine()
            engine_day.process_fills(fills)
            snap = engine_day.snapshot()
            realized = snap["total"]["realized"]

            rows.append({"day": day, "realized": realized})

        return {"ok": True, "year": year, "month": month, "rows": rows}

    except Exception as e:
        return {
            "ok": False,
            "error": f"/pnl2/daily failed: {e.__class__.__name__}: {e}",
            "year": year,
            "month": month,
            "rows": [],
        }

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