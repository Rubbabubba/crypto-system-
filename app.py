# app.py
import os
import json
import csv
import time
import threading
import datetime as dt
from collections import defaultdict

from flask import Flask, request, jsonify, make_response
from dotenv import load_dotenv

# Services & strategies
from services.market_crypto import MarketCrypto
from services.exchange_exec import ExchangeExec
import strategies.c1 as strat_c1
import strategies.c2 as strat_c2

# =============================================================================
# App metadata / versioning
# =============================================================================
__app_version__ = "1.1.0"   # Inline scheduler + ops endpoints

# =============================================================================
# Boot
# =============================================================================
load_dotenv()

app = Flask(__name__)
os.makedirs("logs", exist_ok=True)
os.makedirs("storage", exist_ok=True)

# In-memory ticks for inline scheduler status
_last_ticks = {"c1": None, "c2": None}

# =============================================================================
# Helpers & ENV accessors
# =============================================================================
def env_map():
    keys = [
        "CRYPTO_EXCHANGE", "CRYPTO_TRADING_BASE_URL", "CRYPTO_DATA_BASE_URL",
        "CRYPTO_SYMBOLS", "DAILY_TARGET", "DAILY_STOP", "ORDER_NOTIONAL",
        "C1_TIMEFRAME", "C1_RSI_LEN", "C1_EMA_LEN", "C1_RSI_BUY", "C1_RSI_SELL",
        "C2_TIMEFRAME", "C2_LOOKBACK", "C2_ATR_LEN", "C2_BREAK_K",
        "CRON_INLINE", "C1_EVERY_SEC", "C1_OFFSET_SEC", "C2_EVERY_SEC", "C2_OFFSET_SEC",
        "CRON_DRY",
    ]
    return {k: os.getenv(k) for k in keys}

def get_symbols():
    raw = os.getenv("CRYPTO_SYMBOLS", "BTC/USD,ETH/USD")
    return [s.strip() for s in raw.split(",") if s.strip()]

def _now_utc_ts() -> int:
    return int(time.time())

def _today_bounds_utc():
    # Compute UTC day bounds so it's deterministic across server regions
    now = dt.datetime.utcnow()
    start = dt.datetime(year=now.year, month=now.month, day=now.day, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())

def pwrite(symbol, system, side, notional, note, dry, extra=None):
    """
    Append an execution intent/record to the crypto journal.
    Note: This logs actions (signals/requests). Realized P&L should be computed from fills/positions.
    """
    path = "storage/crypto_ledger.csv"
    exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["ts","symbol","system","side","notional_usd","note","dry_run","extra"])
        w.writerow([_now_utc_ts(), symbol, system, side, round(float(notional), 2), note or "", int(dry), json.dumps(extra or {})])

def mk_services():
    market = MarketCrypto()
    broker = ExchangeExec()
    return market, broker

# =============================================================================
# Optional: token gate for /scan/* (set CRON_TOKEN to enable)
# =============================================================================
def _require_cron_token_if_set():
    tok = os.getenv("CRON_TOKEN")
    if not tok:
        return None
    if request.args.get("token") != tok:
        return jsonify({"ok": False, "error": "forbidden"}), 403
    return None

# =============================================================================
# Strategy runners (HTTP + inline)
# =============================================================================
def _run_strategy_http(mod, name: str):
    """
    HTTP entry: wraps strategy, returns JSON + x-strategy-version header.
    """
    dry = request.args.get("dry", "1") == "1"
    force = request.args.get("force", "0") == "1"
    symbols = get_symbols()
    params = {**env_map(), "ORDER_NOTIONAL": os.getenv("ORDER_NOTIONAL", os.getenv("ORDER_SIZE", 25))}
    market, broker = mk_services()

    try:
        results = mod.run(market, broker, symbols, params, dry=dry, pwrite=pwrite)
        payload = {"ok": True, "strategy": name, "dry": dry, "force": force, "results": results}
        status = 200
    except Exception as e:
        payload = {"ok": False, "strategy": name, "dry": dry, "force": force, "error": str(e)}
        status = 502

    resp = make_response(jsonify(payload), status)
    resp.headers["x-strategy-version"] = getattr(mod, "__version__", "0")
    resp.headers["x-app-version"] = __app_version__
    return resp

def _run_strategy_direct(mod, name: str, dry: bool):
    """
    Background runner (inline scheduler) – no HTTP response, logs only.
    """
    global _last_ticks
    _last_ticks[name] = _now_utc_ts()
    symbols = get_symbols()
    params = {**env_map(), "ORDER_NOTIONAL": os.getenv("ORDER_NOTIONAL", os.getenv("ORDER_SIZE", 25))}
    market, broker = mk_services()
    try:
        results = mod.run(market, broker, symbols, params, dry=dry, pwrite=pwrite)
        app.logger.info({"ts": _now_utc_ts(), "name": name, "dry": dry, "ok": True, "n": len(results)})
    except Exception as e:
        app.logger.exception(f"[{name}] inline run failed: {e}")

# =============================================================================
# Inline scheduler
# =============================================================================
def _start_inline_scheduler():
    enabled = os.getenv("CRON_INLINE", "0") == "1"
    if not enabled:
        app.logger.info("Inline scheduler disabled (set CRON_INLINE=1 to enable).")
        return

    # Cadence (seconds)
    c1_every = int(os.getenv("C1_EVERY_SEC", 5 * 60))
    c1_offset = int(os.getenv("C1_OFFSET_SEC", 0))
    c2_every = int(os.getenv("C2_EVERY_SEC", 15 * 60))
    c2_offset = int(os.getenv("C2_OFFSET_SEC", 60))
    dry = os.getenv("CRON_DRY", "0") == "1"

    def runner(every, offset, fn, label):
        def loop():
            if offset > 0:
                time.sleep(offset)
            while True:
                t0 = time.time()
                try:
                    fn()
                except Exception as e:
                    app.logger.exception(f"[{label}] exception: {e}")
                elapsed = time.time() - t0
                sleep_s = max(1, every - int(elapsed))
                time.sleep(sleep_s)

        th = threading.Thread(target=loop, name=f"inline-{label}", daemon=True)
        th.start()
        return th

    app.logger.info({
        "msg": "Starting inline scheduler",
        "C1_EVERY_SEC": c1_every, "C1_OFFSET_SEC": c1_offset,
        "C2_EVERY_SEC": c2_every, "C2_OFFSET_SEC": c2_offset,
        "CRON_DRY": int(dry),
    })

    runner(c1_every, c1_offset, lambda: _run_strategy_direct(strat_c1, "c1", dry), "c1")
    runner(c2_every, c2_offset, lambda: _run_strategy_direct(strat_c2, "c2", dry), "c2")

# Start scheduler at boot
_start_inline_scheduler()

# =============================================================================
# Health / Diag
# =============================================================================
@app.get("/health")
def health():
    return jsonify({"ok": True, "system": "crypto", "symbols": get_symbols()})

@app.get("/health/versions")
def health_versions():
    data = {
        "app_version": __app_version__,
        "exchange": os.getenv("CRYPTO_EXCHANGE", "alpaca"),
        "systems": {
            "c1": {"version": strat_c1.__version__},
            "c2": {"version": strat_c2.__version__},
        },
    }
    resp = jsonify(data)
    resp.headers["x-app-version"] = __app_version__
    resp.headers["x-c1-version"] = strat_c1.__version__
    resp.headers["x-c2-version"] = strat_c2.__version__
    return resp

@app.get("/diag/crypto")
def diag_crypto():
    acct = None
    ok = True
    err = None
    try:
        _, broker = mk_services()
        acct = broker.get_account()
    except Exception as e:
        ok = False
        err = str(e)
    payload = {
        "ok": ok,
        "exchange": os.getenv("CRYPTO_EXCHANGE"),
        "trading_base": os.getenv("CRYPTO_TRADING_BASE_URL"),
        "data_base": os.getenv("CRYPTO_DATA_BASE_URL"),
        "api_key_present": bool(os.getenv("CRYPTO_API_KEY")),
        "symbols": get_symbols(),
        "account_sample": acct,
        "error": err,
    }
    resp = jsonify(payload)
    resp.headers["x-app-version"] = __app_version__
    return resp

@app.get("/diag/inline")
def diag_inline():
    return jsonify({
        "enabled": os.getenv("CRON_INLINE", "0") == "1",
        "c1_every_sec": int(os.getenv("C1_EVERY_SEC", 300)),
        "c1_offset_sec": int(os.getenv("C1_OFFSET_SEC", 0)),
        "c2_every_sec": int(os.getenv("C2_EVERY_SEC", 900)),
        "c2_offset_sec": int(os.getenv("C2_OFFSET_SEC", 60)),
        "last_ticks": _last_ticks,
        "dry": os.getenv("CRON_DRY", "0") == "1",
    })

# =============================================================================
# Ops: P&L & limits
# =============================================================================
def _read_ledger_rows():
    path = "storage/crypto_ledger.csv"
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, "r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                row["ts"] = int(row["ts"])
                row["notional_usd"] = float(row["notional_usd"])
                row["dry_run"] = bool(int(row["dry_run"]))
            except Exception:
                pass
            rows.append(row)
    return rows

def _summarize_today_activity():
    start_ts, end_ts = _today_bounds_utc()
    by_sys = defaultdict(lambda: {"count": 0, "buy_notional": 0.0, "flat_count": 0, "other_count": 0})
    total = {"count": 0, "buy_notional": 0.0, "flat_count": 0, "other_count": 0}
    rows = _read_ledger_rows()
    for row in rows:
        if not (start_ts <= row["ts"] < end_ts):
            continue
        if row.get("dry_run"):
            continue
        sysname = row.get("system") or "NA"
        side = (row.get("side") or "").upper()
        by_sys[sysname]["count"] += 1
        total["count"] += 1
        if side == "BUY":
            by_sys[sysname]["buy_notional"] += row.get("notional_usd", 0.0)
            total["buy_notional"] += row.get("notional_usd", 0.0)
        elif side == "FLAT":
            by_sys[sysname]["flat_count"] += 1
            total["flat_count"] += 1
        else:
            by_sys[sysname]["other_count"] += 1
            total["other_count"] += 1
    return {"by_system": by_sys, "totals": total}

@app.get("/pnl/daily")
def pnl_daily():
    """
    Returns:
      - today's journal activity summary (counts/notional)
      - live account snapshot
      - current open positions (from Alpaca) including unrealized provided by broker
    Note: Realized P&L should be computed from fills; this endpoint summarizes activity + live state.
    """
    summary = _summarize_today_activity()
    acct = None
    positions = None
    ok = True
    err = None
    try:
        _, broker = mk_services()
        acct = broker.get_account()
        positions = broker.get_positions()
    except Exception as e:
        ok = False
        err = str(e)

    return jsonify({
        "ok": ok,
        "error": err,
        "app_version": __app_version__,
        "today_activity": summary,
        "account": acct,
        "positions": positions,
    })

@app.get("/health/limits")
def health_limits():
    """
    Shows today's activity vs target/stop rails.
    Uses journal count/notional as activity proxy and account equity for context.
    """
    daily_target = float(os.getenv("DAILY_TARGET", "0") or 0)
    daily_stop = float(os.getenv("DAILY_STOP", "0") or 0)
    summary = _summarize_today_activity()
    acct = None
    ok = True
    err = None
    try:
        _, broker = mk_services()
        acct = broker.get_account()
    except Exception as e:
        ok = False
        err = str(e)

    # We don't compute realized P&L here (fills required). Provide activity + equity.
    payload = {
        "ok": ok,
        "error": err,
        "app_version": __app_version__,
        "rails": {"daily_target": daily_target, "daily_stop": daily_stop},
        "today": summary,
        "account_equity": acct.get("equity") if isinstance(acct, dict) else None,
        "note": "This endpoint compares journal activity to rails; realized P&L requires fills."
    }
    return jsonify(payload)

# =============================================================================
# Scan routes (token-gated if CRON_TOKEN set)
# =============================================================================
@app.post("/scan/c1")
def scan_c1():
    guard = _require_cron_token_if_set()
    if guard:
        return guard
    return _run_strategy_http(strat_c1, "c1")

@app.post("/scan/c2")
def scan_c2():
    guard = _require_cron_token_if_set()
    if guard:
        return guard
    return _run_strategy_http(strat_c2, "c2")

# =============================================================================
# Entrypoint
# =============================================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
