#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crypto System – FastAPI service (Render/Kraken)
Build: v2.0.1 — Dashboard externalized + policy & price endpoint fixes

Changes in 2.0.1
- Added SYMBOLS alias to avoid NameError in older routes.
- Added GET /price/{base}/{quote} so calls like /price/BTC/USD work.
- Restored /policy/eligible using guard.py if present; safe fallback otherwise.
- Dashboard HTML still loaded from external dashboard.html (DASHBOARD_PATH optional).
"""

from __future__ import annotations

__version__ = "2.0.1"

import asyncio
import os
import sys
import json
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TypedDict

import pandas as pd
from fastapi import FastAPI, HTTPException, Request, Body
from fastapi.responses import JSONResponse, HTMLResponse

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("app")

BROKER = os.getenv("BROKER", "kraken").lower()
USING_KRAKEN = BROKER == "kraken" or (os.getenv("KRAKEN_KEY") and os.getenv("KRAKEN_SECRET"))

if USING_KRAKEN:
    import broker_kraken as br  # Kraken adapter
    ACTIVE_BROKER_MODULE = "broker_kraken"
else:
    import broker as br  # Alpaca adapter (legacy)
    ACTIVE_BROKER_MODULE = "broker"

TRADING_ENABLED_BASE = os.getenv("TRADING_ENABLED", "1") in ("1", "true", "True")
if USING_KRAKEN:
    TRADING_ENABLED = TRADING_ENABLED_BASE and (os.getenv("KRAKEN_TRADING", "0") in ("1", "true", "True"))
else:
    TRADING_ENABLED = TRADING_ENABLED_BASE and (os.getenv("ALPACA_TRADING", "0") in ("1", "true", "True"))

APP_VERSION = os.getenv("APP_VERSION", __version__)
SERVICE_NAME = os.getenv("SERVICE_NAME", "Crypto System")

DEFAULT_TIMEFRAME = os.getenv("DEFAULT_TIMEFRAME", "5Min")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "300"))
DEFAULT_NOTIONAL = float(os.getenv("DEFAULT_NOTIONAL", os.getenv("ORDER_NOTIONAL", "25")))

RISK_PCT = float(os.getenv("RISK_PCT", "0.05"))
NOTIONAL_MIN = float(os.getenv("NOTIONAL_MIN", "25"))
NOTIONAL_MAX = float(os.getenv("NOTIONAL_MAX", "250"))
SCHED_AUTO_SIZE = os.getenv("SCHED_AUTO_SIZE", "1").lower() in ("1","true","yes","y")

try:
    from universe import load_universe_from_env
    _CURRENT_SYMBOLS = load_universe_from_env()
except Exception:
    _CURRENT_SYMBOLS = ["BTC/USD","ETH/USD","SOL/USD","DOGE/USD","XRP/USD","AVAX/USD","LINK/USD","BCH/USD","LTC/USD"]

# Compatibility alias for older code paths:
SYMBOLS = _CURRENT_SYMBOLS

DEFAULT_STRAT_PARAMS: Dict[str, Dict[str, Any]] = {
    "c1": {"ema_n": 20, "vwap_pull": 0.0020, "min_vol": 0.0},
    "c2": {"ema_n": 50, "exit_k": 0.997, "min_atr": 0.0},
    "c3": {"ch_n": 55, "break_k": 1.0005, "fail_k": 0.997, "min_atr": 0.0},
    "c4": {"ema_fast": 12, "ema_slow": 26, "sig": 9, "atr_n": 14, "atr_mult": 1.2, "min_vol": 0.0},
    "c5": {"lookback": 20, "band_k": 1.0010, "exit_k": 0.9990, "min_vol": 0.0},
    "c6": {"atr_n": 14, "atr_mult": 1.2, "ema_n": 20, "exit_k": 0.997, "min_vol": 0.0},
}
ACTIVE_STRATEGIES = list(DEFAULT_STRAT_PARAMS.keys())
_DISABLED_STRATS: set[str] = set()

import c1, c2, c3, c4, c5, c6

STRAT_DISPATCH = {
    "c1": c1.scan,
    "c2": c2.scan,
    "c3": c3.scan,
    "c4": c4.scan,
    "c5": c5.scan,
    "c6": c6.scan,
}

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _merge_raw(strat: str, raw_in: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    base = dict(DEFAULT_STRAT_PARAMS.get(strat, {}))
    if raw_in:
        base.update({k: raw_in[k] for k in raw_in})
    return base

def _to_list(x: Any) -> List[str]:
    if x is None: return []
    if isinstance(x, list): return [str(s).upper() for s in x]
    if isinstance(x, str): return [s.strip().upper() for s in x.split(",") if s.strip()]
    return [str(x).upper()]

def _normalize_asset_code(asset: str) -> str:
    if not asset:
        return ""
    base = asset.split(".")[0].upper()
    if base in ("XBT", "XXBT"):
        return "BTC"
    if base.startswith("X") and len(base) in (3,4):
        return base[1:]
    return base

def _account_equity_usd() -> float:
    try:
        pos = br.positions()
    except Exception:
        return 0.0
    base_to_sym = {s.split("/")[0].upper(): s for s in _CURRENT_SYMBOLS if s.upper().endswith("/USD")}
    cash_usd = 0.0
    equity = 0.0
    for p in (pos or []):
        asset = str(p.get("asset") or "").upper()
        qty = float(p.get("qty") or 0.0)
        if asset == "USD":
            cash_usd += qty
            continue
        base = _normalize_asset_code(asset)
        sym = base_to_sym.get(base)
        if not sym or qty == 0.0:
            continue
        try:
            px = float(br.last_price(sym))
        except Exception:
            px = 0.0
        equity += qty * px
    return cash_usd + equity

def _sized_notional_from_equity(equity_usd: float) -> float:
    raw = float(equity_usd) * float(RISK_PCT)
    sized = max(NOTIONAL_MIN, min(NOTIONAL_MAX, raw))
    return round(sized, 2)

app = FastAPI(title=SERVICE_NAME, version=APP_VERSION)

@app.get("/health")
def health():
    return {
        "ok": True,
        "service": SERVICE_NAME,
        "version": APP_VERSION,
        "broker": ("kraken" if USING_KRAKEN else "alpaca"),
        "trading": TRADING_ENABLED,
        "scheduler_running": _RUNNING,
        "time": utc_now(),
        "symbols": _CURRENT_SYMBOLS,
        "strategies": ACTIVE_STRATEGIES,
    }

@app.get("/diag/broker")
def diag_broker():
    return {"broker_module": ACTIVE_BROKER_MODULE, "using_kraken": USING_KRAKEN, "trading": TRADING_ENABLED}

@app.get("/version")
def version():
    return {"version": APP_VERSION, "time": utc_now()}

@app.get("/config")
def config():
    return {
        "DEFAULT_TIMEFRAME": DEFAULT_TIMEFRAME,
        "DEFAULT_LIMIT": DEFAULT_LIMIT,
        "DEFAULT_NOTIONAL": DEFAULT_NOTIONAL,
        "SIZING": {
            "RISK_PCT": RISK_PCT,
            "NOTIONAL_MIN": NOTIONAL_MIN,
            "NOTIONAL_MAX": NOTIONAL_MAX,
            "SCHED_AUTO_SIZE": SCHED_AUTO_SIZE,
        },
        "SYMBOLS": _CURRENT_SYMBOLS,
        "STRATEGIES": ACTIVE_STRATEGIES,
        "PARAMS": DEFAULT_STRAT_PARAMS,
    }

class ScanRequestModel(TypedDict, total=False):
    symbols: List[str]
    timeframe: str
    limit: int
    notional: float
    raw: Dict[str, Any]
    dry: bool

async def _scan_bridge(strat: str, req: Dict[str, Any], *, dry: Optional[bool] = None) -> Dict[str, Any]:
    strat = (strat or "").lower()
    fn = STRAT_DISPATCH.get(strat)
    if not fn:
        raise HTTPException(status_code=400, detail=f"Unknown strategy '{strat}'")

    is_dry = bool(dry) if dry is not None else (not TRADING_ENABLED)

    timeframe = req.get("timeframe") or DEFAULT_TIMEFRAME
    limit = int(req.get("limit") or DEFAULT_LIMIT)
    notional = float(req.get("notional") or DEFAULT_NOTIONAL)
    symbols = _to_list(req.get("symbols")) or [s.upper().replace("USD","/USD") if "USD" in s and "/" not in s else s.upper() for s in _CURRENT_SYMBOLS]
    raw = _merge_raw(strat, dict(req.get("raw") or {}))
    ctx = {"timeframe": timeframe, "symbols": symbols, "notional": notional}

    orders = fn({"strategy": strat, "timeframe": timeframe, "limit": limit, "notional": notional, "symbols": symbols, "raw": raw}, ctx) or []

    placed: List[Dict[str, Any]] = []
    if not is_dry:
        for o in orders:
            sym = (o.get("symbol") or symbols[0]).upper()
            side = (o.get("side") or "buy").lower()
            notional_o = float(o.get("notional") or notional)
            try:
                res = br.market_notional(sym, side, notional_o)
                placed.append({**o, "symbol": sym, "side": side, "notional": notional_o, "order": res})
            except Exception as e:
                placed.append({**o, "symbol": sym, "side": side, "notional": notional_o, "error": str(e)})
    else:
        for o in orders:
            sym = (o.get("symbol") or symbols[0]).upper()
            side = (o.get("side") or "buy").lower()
            notional_o = float(o.get("notional") or notional)
            placed.append({**o, "symbol": sym, "side": side, "notional": notional_o, "dry": True})

    return {"ok": True, "orders": orders, "placed": placed, "strategy": strat, "version": APP_VERSION, "time": utc_now()}

@app.post("/scan/{strategy}")
async def scan(strategy: str, model: Dict[str, Any] = Body(...)):
    try:
        body = dict(model or {})
        is_dry = body.get("dry")
        res = await _scan_bridge(strategy, body, dry=(is_dry in (True, 1, "1", "true", "True")))
        return res
    except HTTPException:
        raise
    except Exception as e:
        log.exception("scan error")
        raise HTTPException(status_code=500, detail=str(e))

# ------------------------------- Market Data ---------------------------------

@app.get("/bars/{symbol}")
def bars(symbol: str, timeframe: str = DEFAULT_TIMEFRAME, limit: int = 200):
    try:
        out = br.get_bars(symbol.upper(), timeframe=timeframe, limit=int(limit))
        return {"ok": True, "symbol": symbol.upper(), "timeframe": timeframe, "limit": int(limit), "bars": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Original endpoint kept for compatibility: /price/BTCUSD or /price/BTC/USD (the latter will 404 here)
@app.get("/price/{symbol}")
def price(symbol: str):
    try:
        p = br.last_price(symbol.upper())
        return {"ok": True, "symbol": symbol.upper(), "price": float(p)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# New endpoint to support /price/BTC/USD pattern used by the dashboard
@app.get("/price/{base}/{quote}")
def price_pair(base: str, quote: str):
    try:
        # prefer slash form e.g., BTC/USD to match Kraken adapter expectations
        symbol = f"{base.upper()}/{quote.upper()}"
        p = br.last_price(symbol)
        return {"ok": True, "symbol": symbol, "price": float(p)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/positions")
def positions():
    try:
        pos = br.positions()
        return {"ok": True, "positions": pos}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders")
def orders():
    try:
        out = br.orders()
        return {"ok": True, "orders": out}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/fills")
def fills():
    try:
        data = br.trades_history(200)
        return {"ok": True, **data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/order/market")
async def order_market(request: Request):
    try:
        body = await request.json()
        symbol = (body.get("symbol") or "BTC/USD").upper()
        side = (body.get("side") or "buy").lower()
        notional = float(body.get("notional") or DEFAULT_NOTIONAL)
        if not TRADING_ENABLED:
            return {"ok": True, "dry": True, "symbol": symbol, "side": side, "notional": notional}
        res = br.market_notional(symbol, side, notional)
        return {"ok": True, "order": res}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ----------------------------- Dashboard HTML --------------------------------

def _read_dashboard_template() -> str:
    path = os.getenv("DASHBOARD_PATH", "./dashboard.html")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return f"<h1>Dashboard file not found</h1><p>Expected at: {path}</p>"
    except Exception as e:
        return f"<h1>Dashboard error</h1><pre>{e}</pre>"

def _render_dashboard_html() -> str:
    broker_badge = "kraken" if USING_KRAKEN else "alpaca"
    broker_text = "kraken" if USING_KRAKEN else "alpaca"
    html = _read_dashboard_template()
    html = (html
            .replace("{SERVICE_NAME}", SERVICE_NAME)
            .replace("{APP_VERSION}", APP_VERSION)
            .replace("{BROKER_BADGE}", broker_badge)
            .replace("{BROKER_TEXT}", broker_text))
    return html

@app.get("/", response_class=HTMLResponse)
def root():
    return HTMLResponse(content=_render_dashboard_html(), status_code=200)

@app.get("/dashboard", response_class=HTMLResponse)
def dashboard_alias():
    return root()

# ------------------------------- Scheduler -----------------------------------
_RUNNING = False

def _sched_config() -> Dict[str, Any]:
    raw = os.getenv("SCHED_STRATS", "")
    strategies = [s.strip().lower() for s in raw.split(",") if s.strip()] or [s.lower() for s in ACTIVE_STRATEGIES]
    timeframe = os.getenv("SCHED_TIMEFRAME", DEFAULT_TIMEFRAME)
    try: limit = int(os.getenv("SCHED_LIMIT", str(DEFAULT_LIMIT)))
    except: limit = DEFAULT_LIMIT
    try: notional = float(os.getenv("SCHED_NOTIONAL", str(DEFAULT_NOTIONAL)))
    except: notional = DEFAULT_NOTIONAL
    try: sleep_s = int(os.getenv("SCHED_SLEEP", "30"))
    except: sleep_s = 30
    dry_env = os.getenv("SCHED_DRY", "1").lower() in ("1", "true", "yes", "y")
    trading_flags = TRADING_ENABLED and (os.getenv("KRAKEN_TRADING", "0").lower() in ("1","true","yes","y"))
    dry = dry_env or (not trading_flags)
    return {
        "strategies": strategies,
        "timeframe": timeframe,
        "limit": limit,
        "notional": notional,
        "sleep_s": sleep_s,
        "dry": dry,
        "trading_flags": trading_flags
    }

async def _loop():
    global _RUNNING
    _RUNNING = True
    log.info("Scheduler started (v%s, broker=%s)", APP_VERSION, ("kraken" if USING_KRAKEN else "alpaca"))
    try:
        while _RUNNING:
            cfg = _sched_config()
            if SCHED_AUTO_SIZE:
                try:
                    eq = _account_equity_usd()
                    dyn_notional = _sized_notional_from_equity(eq)
                    cfg["notional"] = dyn_notional
                    log.info("Sizing: equity=%.2f USD, risk_pct=%.4f -> notional=%.2f (%.2f..%.2f)",
                             eq, RISK_PCT, dyn_notional, NOTIONAL_MIN, NOTIONAL_MAX)
                except Exception as e:
                    log.warning("Sizing error; using static notional %.2f: %s", cfg["notional"], e)
            syms = list(_CURRENT_SYMBOLS)
            run_strats = [s for s in cfg["strategies"] if s in [x.lower() for x in ACTIVE_STRATEGIES] and s not in [x.lower() for x in _DISABLED_STRATS]]
            log.info("Scheduler pass: strats=%s tf=%s limit=%s notional=%s dry=%s symbols=%s",
                     ",".join(run_strats), cfg["timeframe"], cfg["limit"], cfg["notional"], cfg["dry"], ",".join(syms))
            for strat in run_strats:
                if not _RUNNING: break
                try:
                    await _scan_bridge(strat, {
                        "timeframe": cfg["timeframe"], "limit": cfg["limit"],
                        "notional": cfg["notional"], "symbols": syms
                    }, dry=cfg["dry"])
                except Exception as e:
                    log.warning("Scheduler scan error (%s): %s", strat, e)
            total = max(1, int(cfg["sleep_s"]))
            for _ in range(total):
                if not _RUNNING: break
                await asyncio.sleep(1)
    finally:
        log.info("Scheduler stopped")

@app.get("/scheduler/start")
async def scheduler_start():
    if os.getenv("SCHED_ON", "0").lower() not in ("1","true","yes","y"):
        return {"ok": False, "why": "SCHED_ON env not enabled"}
    if _RUNNING:
        return {"ok": True, "already": True, "config": _sched_config()}
    asyncio.create_task(_loop())
    return {"ok": True, "started": True, "config": _sched_config()}

@app.get("/scheduler/stop")
async def scheduler_stop():
    global _RUNNING
    _RUNNING = False
    return {"ok": True, "stopping": True}

@app.get("/scheduler/status")
async def scheduler_status():
    return {"ok": True, "running": _RUNNING, "config": _sched_config()}

@app.on_event("startup")
async def _maybe_autostart_scheduler():
    if os.getenv("SCHED_ON", "0").lower() in ("1","true","yes","y"):
        asyncio.create_task(_loop())
        log.info("Scheduler autostart: enabled by SCHED_ON")

# ------------------------------ Policy/Guard ---------------------------------

def _coerce_slash(sym: str) -> str:
    sym = sym.upper().replace(" ", "")
    if "/" not in sym and sym.endswith("USD"):
        return sym[:-3] + "/USD"
    return sym

@app.get("/policy/eligible")
def policy_eligible():
    """
    Returns eligibility of each symbol based on guard module if present.
    Fallback: allow True for all symbols.
    """
    syms = [_coerce_slash(s) for s in _CURRENT_SYMBOLS]
    try:
        import guard  # local policy helper using policy_config/*
        now_ts = time.time()
        out = []
        for sym in syms:
            try:
                ok, reason = guard.is_trade_allowed("dashboard", sym, now_ts=now_ts)
                out.append({"symbol": sym, "allowed": bool(ok), "reason": str(reason)})
            except Exception as e:
                out.append({"symbol": sym, "allowed": False, "reason": f"guard_error:{e}"})
        return {"ok": True, "symbols": out, "time": utc_now()}
    except Exception:
        # Safe fallback if guard isn't available or errors
        return {"ok": True, "symbols": [{"symbol": s, "allowed": True, "reason": "fallback"} for s in syms], "time": utc_now()}

# ----------------------------- Journal & P&L ---------------------------------
_JOURNAL_LOCK = threading.Lock()
_JOURNAL_PATH = os.getenv("JOURNAL_PATH", "./journal_v2.jsonl")
_JOURNAL: List[Dict[str, Any]] = []

def _journal_load():
    global _JOURNAL
    try:
        rows = []
        with open(_JOURNAL_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                rows.append(json.loads(line))
        with _JOURNAL_LOCK:
            _JOURNAL = rows
        log.info("journal: loaded %d rows from %s", len(rows), _JOURNAL_PATH)
    except FileNotFoundError:
        pass
    except Exception as e:
        log.warning("journal load error: %s", e)

def _journal_append(row: dict):
    with _JOURNAL_LOCK:
        _JOURNAL.append(row)
        try:
            with open(_JOURNAL_PATH, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, separators=(",", ":")) + "\n")
        except Exception as e:
            log.warning("journal persist error: %s", e)

@app.on_event("startup")
async def _journal_on_start():
    _journal_load()

def _prices_for(symbols: List[str]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for s in symbols:
        try:
            out[s] = float(br.last_price(s))
        except Exception:
            out[s] = 0.0
    return out

def _pnl_calc(now_prices: Dict[str, float]) -> Dict[str, Any]:
    from collections import defaultdict, deque
    with _JOURNAL_LOCK:
        trades = [r for r in _JOURNAL if r.get("price") and r.get("vol")]
    trades.sort(key=lambda r: r.get("filled_ts") or r.get("ts") or 0)

    lots = defaultdict(lambda: deque())
    stats = defaultdict(lambda: {"realized": 0.0, "fees": 0.0, "qty": 0.0, "avg_cost": 0.0})

    for r in trades:
        strat = r.get("strategy") or "unknown"
        sym = (r.get("symbol") or "").upper()
        side = r.get("side")
        px = float(r.get("price") or 0.0)
        vol = float(r.get("vol") or 0.0)
        fee = float(r.get("fee") or 0.0)
        key = (strat, sym)
        if side == "buy":
            lots[key].append([vol, px])
            st = stats[key]
            st["qty"] += vol
            prev_qty = max(1e-9, st["qty"] - vol)
            st["avg_cost"] = ((st["avg_cost"] * prev_qty) + px * vol) / max(1e-9, st["qty"])
            st["fees"] += fee
        elif side == "sell":
            remain = vol
            realized = 0.0
            while remain > 1e-12 and lots[key]:
                q, cpx = lots[key][0]
                take = min(q, remain)
                realized += (px - cpx) * take
                q -= take; remain -= take
                if q <= 1e-12: lots[key].popleft()
                else: lots[key][0][0] = q
            st = stats[key]
            st["qty"] -= vol
            st["realized"] += realized
            st["fees"] += fee

    out_strat, out_sym = {}, {}
    total = {"realized": 0.0, "unrealized": 0.0, "fees": 0.0, "equity": 0.0}
    for key, st in stats.items():
        strat, sym = key
        mkt = float(now_prices.get(sym) or 0.0)
        unreal = 0.0
        if mkt > 0 and lots[key]:
            for q, cpx in lots[key]:
                unreal += (mkt - cpx) * q
        equity = st["realized"] + unreal - st["fees"]

        srow = out_strat.get(strat, {"strategy": strat, "realized": 0.0, "unrealized": 0.0, "fees": 0.0, "equity": 0.0})
        srow["realized"] += st["realized"]; srow["unrealized"] += unreal; srow["fees"] += st["fees"]; srow["equity"] += equity
        out_strat[strat] = srow

        yrow = out_sym.get(sym, {"symbol": sym, "realized": 0.0, "unrealized": 0.0, "fees": 0.0, "equity": 0.0})
        yrow["realized"] += st["realized"]; yrow["unrealized"] += unreal; yrow["fees"] += st["fees"]; yrow["equity"] += equity
        out_sym[sym] = yrow

        total["realized"] += st["realized"]; total["unrealized"] += unreal; total["fees"] += st["fees"]; total["equity"] += equity

    return {
        "ok": True,
        "time": utc_now(),
        "total": total,
        "per_strategy": sorted(out_strat.values(), key=lambda r: r["equity"], reverse=True),
        "per_symbol": sorted(out_sym.values(), key=lambda r: r["equity"], reverse=True),
    }

@app.get("/pnl/summary")
def pnl_summary():
    try:
        with _JOURNAL_LOCK:
            syms = sorted({(r.get("symbol") or "").upper() for r in _JOURNAL if r.get("symbol")}) or list(_CURRENT_SYMBOLS)
        prices = _prices_for(syms)
        return _pnl_calc(prices)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pnl/strategies")
def pnl_strategies():
    try:
        return pnl_summary().get("per_strategy", [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/pnl/symbols")
def pnl_symbols():
    try:
        return pnl_summary().get("per_symbol", [])
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/pnl/reset")
def pnl_reset():
    try:
        with _JOURNAL_LOCK:
            _JOURNAL.clear()
        try:
            if os.path.exists(_JOURNAL_PATH):
                os.remove(_JOURNAL_PATH)
        except Exception:
            pass
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn  # type: ignore
    port = int(os.getenv("PORT", "10000"))
    log.info(
        "Launching Uvicorn on 0.0.0.0:%d (version %s, broker=%s, trading=%s)",
        port, __version__, ("kraken" if USING_KRAKEN else "alpaca"), TRADING_ENABLED
    )
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False, access_log=True)
