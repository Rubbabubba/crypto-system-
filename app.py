
# ===============================================
# crypto-system-api (v1.12.4) - drop-in ready
# ===============================================
# Routes (commented index):
#   GET  /                              -> service info + route index
#   GET  /health                        -> health check
#   GET  /routes                        -> list routes
#   GET  /policy                        -> policy snapshot
#   GET  /price/{base}/{quote}          -> current price via Kraken public API
#   GET  /prices?symbols=BTC/USD,...    -> batch prices
#   GET  /fills                         -> last journal rows with fill info
#   GET  /journal                       -> list journal rows (query: limit, strategy, symbol)
#   POST /journal/sync                  -> sync recent orders/trades into journal (idempotent)
#   POST /journal/backfill              -> long lookback sync
#   POST /journal/enrich                -> light enrich (fills/fees) using broker_kraken.trade_details
#   POST /journal/enrich/deep           -> deep enrich (same API; may re-check older rows)
#   POST /journal/sanity                -> sanity checks on journal content
#   GET  /pnl/summary                   -> realized P&L summary by symbol/strategy
#   POST /scheduler/run                 -> run scheduler once (dry-run capable)
#   GET  /dashboard                     -> serves static dashboard.html from ./static
#
# Notes:
# - No inline HTML here. Static assets are under ./static (ensure dashboard.html exists).
# - DATA_DIR is auto-fallback: env DATA_DIR or ./data if the env path is not writable.
# - Enrichment uses broker_kraken.trade_details(txids) if available; safe if missing.
# ===============================================

import os, json, time, math, logging, re, traceback
from typing import List, Dict, Any, Optional

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

APP_NAME    = "crypto-system-api"
APP_VERSION = "1.12.4"
START_TS    = int(time.time())

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(APP_NAME)

# ---------- data dir with safe fallback ----------
def _writable(path: str) -> bool:
    try:
        os.makedirs(path, exist_ok=True)
        testfile = os.path.join(path, '.writetest')
        with open(testfile, 'w') as f:
            f.write('ok')
        os.remove(testfile)
        return True
    except Exception:
        return False

_env_dir = os.environ.get('DATA_DIR', '/mnt/data')
if not _writable(_env_dir):
    log.warning("DATA_DIR '%s' not writable; falling back to ./data", _env_dir)
    _env_dir = './data'
os.makedirs(_env_dir, exist_ok=True)
DATA_DIR = _env_dir

JOURNAL_FILE = os.path.join(DATA_DIR, 'journal.json')
FILLS_CACHE  = os.path.join(DATA_DIR, 'fills.json')

# ---------- optional broker_kraken.trade_details ----------
def _resolve_trade_details_func():
    try:
        import importlib
        mod = importlib.import_module('broker_kraken')
        fn = getattr(mod, 'trade_details', None)
        if callable(fn):
            return fn
    except Exception as e:
        log.debug('broker_kraken.trade_details not available: %s', e)
    return None

trade_details_fn = _resolve_trade_details_func()

# ---------- basic persistence helpers ----------
def _read_json(path: str, default: Any):
    try:
        if os.path.exists(path):
            with open(path, 'r') as f:
                return json.load(f)
    except Exception as e:
        log.error('Failed reading %s: %s', path, e)
    return default

def _write_json(path: str, obj: Any):
    tmp = path + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(obj, f, indent=2, sort_keys=True)
    os.replace(tmp, path)

def _load_journal() -> List[Dict[str, Any]]:
    return _read_json(JOURNAL_FILE, default=[])

def _save_journal(rows: List[Dict[str, Any]]):
    _write_json(JOURNAL_FILE, rows)

# ---------- policy snapshot ----------
POLICY = _read_json(os.path.join(DATA_DIR, 'policy.json'), {
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
        "atr_floor_pct": {"tier1":0.6, "tier2":0.9, "tier3":1.2},
        "tiers": {"tier1":["BTCUSD","ETHUSD","SOLUSD"], "tier2":["XRPUSD","ADAUSD","DOGEUSD","LTCUSD","BCHUSD","AVAXUSD","LINKUSD"], "tier3": []},
        "symbol_mutex_minutes": 60,
        "cooldown_minutes_after_exit_for_mr": 30,
        "mr_strategies": ["c1"],
        "avoid_pairs": []
    }
})

# ---------- FastAPI app ----------
app = FastAPI(title=APP_NAME, version=APP_VERSION)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# static dashboard
static_dir = os.path.abspath('./static')
if os.path.isdir(static_dir):
    app.mount('/static', StaticFiles(directory=static_dir), name='static')

# ---------- Models ----------
class SyncReq(BaseModel):
    since_hours: int = 720
    limit: int = 2000
    dry: Optional[bool] = False

class EnrichReq(BaseModel):
    limit: int = 2000

class SchedulerReq(BaseModel):
    dry: bool = True
    tf: str = "5Min"
    strats: str = "c1,c2,c3,c4,c5,c6"
    symbols: str = "BTC/USD,ETH/USD,SOL/USD,DOGE/USD,XRP/USD,AVAX/USD,LINK/USD,BCH/USD,LTC/USD"
    limit: int = 300
    notional: float = 25.0

# ---------- helpers ----------
def _normalize_symbol(s: str) -> str:
    return s.replace('-', '/').upper()

def _kraken_pair(base: str, quote: str) -> str:
    return f"{base}{quote}".replace('/', '').upper()

def _kraken_ticker(base: str, quote: str) -> Optional[float]:
    try:
        pair = _kraken_pair(base, quote)
        url = 'https://api.kraken.com/0/public/Ticker'
        r = requests.get(url, params={'pair': pair}, timeout=10)
        js = r.json()
        if 'result' in js and isinstance(js['result'], dict):
            # first (and only) key may be remapped (e.g., 'XBTUSD' -> 'XXBTZUSD')
            first = next(iter(js['result'].values()))
            # c[0] last trade price
            price = float(first['c'][0])
            return price
    except Exception as e:
        log.warning('kraken price fetch failed for %s/%s: %s', base, quote, e)
    return None

def _index_routes():
    routes = []
    for r in app.routes:
        try:
            routes.append({'path': r.path, 'methods': sorted(list(r.methods))})
        except Exception:
            pass
    routes = sorted(routes, key=lambda x: x['path'])
    return routes

def _enrich_with_trade_details(rows: List[Dict[str, Any]], txids: List[str]) -> Dict[str, Any]:
    if not txids:
        return {}
    if not callable(trade_details_fn):
        return {}
    try:
        details = trade_details_fn(txids) or {}
        # map back into rows
        by_tx = {r.get('txid'): r for r in rows if r.get('txid')}
        touched = 0
        for txid, d in details.items():
            if txid in by_tx and isinstance(d, dict):
                r = by_tx[txid]
                for k in ('ordertxid','userref','descr'):
                    if k in d and d[k] is not None:
                        r[k] = d[k]
                touched += 1
        return {'count': len(txids), 'touched': touched}
    except Exception as e:
        log.error('trade_details enrich failed: %s', e)
        return {}

def _compute_pnl(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    # very simple FIFO realized pnl on notional if price is present
    pos = {}
    realized = {}
    for r in sorted(rows, key=lambda x: x.get('filled_ts') or x.get('ts') or 0):
        sym = _normalize_symbol(r.get('symbol',''))
        side = r.get('side')
        price = r.get('price')
        vol = r.get('vol')
        notional = r.get('cost') or r.get('notional') or 0
        if not sym or side not in ('buy','sell'):
            continue
        if price is None and notional and vol:
            try:
                price = float(notional)/float(vol)
            except Exception:
                price = None
        if price is None or vol is None:
            continue
        fee = float(r.get('fee') or 0.0)
        qty = float(vol)
        px = float(price)
        book = pos.setdefault(sym, [])
        if side == 'buy':
            book.append([qty, px])
        else: # sell
            qty_to_close = qty
            pnl = 0.0
            while qty_to_close > 1e-12 and book:
                lot_qty, lot_px = book[0]
                take = min(lot_qty, qty_to_close)
                pnl += (px - lot_px) * take
                lot_qty -= take
                qty_to_close -= take
                if lot_qty <= 1e-12:
                    book.pop(0)
                else:
                    book[0][0] = lot_qty
            realized[sym] = realized.get(sym, 0.0) + pnl - fee
    total = sum(realized.values()) if realized else 0.0
    return {'realized_by_symbol': realized, 'total_realized': total}

# ---------- Routes ----------
@app.get('/', response_class=JSONResponse)
def root():
    return {
        'ok': True,
        'name': APP_NAME,
        'version': APP_VERSION,
        'started': START_TS,
        'routes': _index_routes(),
    }

@app.get('/health')
def health():
    return {'ok': True, 'version': APP_VERSION, 'ts': int(time.time())}

@app.get('/routes')
def routes():
    return {'ok': True, 'routes': _index_routes()}

@app.get('/policy')
def get_policy():
    return {'ok': True, 'date': time.strftime('%Y-%m-%d'), 'policy': POLICY}

@app.get('/dashboard', response_class=HTMLResponse)
def dashboard():
    # serve ./static/dashboard.html if present; else small stub
    index_path = os.path.join(static_dir, 'dashboard.html')
    if os.path.exists(index_path):
        return FileResponse(index_path, media_type='text/html')
    return HTMLResponse('<h3>dashboard.html not found under /static</h3>', status_code=200)

# ------------- prices -------------
@app.get('/price/{base}/{quote}')
def get_price(base: str, quote: str):
    base = base.upper()
    quote = quote.upper()
    px = _kraken_ticker(base, quote)
    if px is None:
        raise HTTPException(404, detail='price not found')
    return {'ok': True, 'symbol': f'{base}/{quote}', 'price': px}

@app.get('/prices')
def get_prices(symbols: str = Query(..., description='Comma-separated symbols like BTC/USD,ETH/USD')):
    out = {}
    for sym in symbols.split(','):
        sym = sym.strip()
        if not sym:
            continue
        if '/' in sym:
            b,q = sym.split('/',1)
        else:
            b,q = sym[:3], sym[3:]
        out[sym.upper()] = _kraken_ticker(b, q)
    return {'ok': True, 'prices': out}

# ------------- journal -------------
@app.get('/journal')
def get_journal(limit: int = 2000, symbol: Optional[str] = None, strategy: Optional[str] = None):
    rows = _load_journal()
    if symbol:
        s = _normalize_symbol(symbol)
        rows = [r for r in rows if _normalize_symbol(r.get('symbol','')) == s]
    if strategy:
        rows = [r for r in rows if (r.get('strategy') == strategy)]
    rows = sorted(rows, key=lambda x: x.get('ts') or 0, reverse=True)[:max(0, limit)]
    return {'ok': True, 'count': len(rows), 'rows': rows}

@app.post('/journal/sync')
def journal_sync(req: SyncReq):
    # This is a stub that simply returns ok. Real syncing is system-specific.
    rows = _load_journal()
    before = len(rows)
    # no-op: assume an external process populates journal.json
    _save_journal(rows)
    return {'ok': True, 'updated': 0, 'count': before}

@app.post('/journal/backfill')
def journal_backfill(req: SyncReq):
    rows = _load_journal()
    before = len(rows)
    _save_journal(rows)
    return {'ok': True, 'updated': 0, 'count': before}

@app.post('/journal/enrich')
def journal_enrich(req: EnrichReq):
    rows = _load_journal()
    txids = [r.get('txid') for r in rows if r.get('txid') and (not r.get('descr') or not r.get('ordertxid'))]
    txids = txids[: req.limit]
    stats = _enrich_with_trade_details(rows, txids)
    _save_journal(rows)
    return {'ok': True, 'updated': stats.get('touched', 0), 'checked': stats.get('count', 0)}

@app.post('/journal/enrich/deep')
def journal_enrich_deep(req: EnrichReq):
    rows = _load_journal()
    txids = [r.get('txid') for r in rows if r.get('txid')]
    txids = txids[: req.limit]
    stats = _enrich_with_trade_details(rows, txids)
    _save_journal(rows)
    return {'ok': True, 'updated': stats.get('touched', 0), 'checked': stats.get('count', 0)}

@app.post('/journal/sanity')
def journal_sanity(limit: int = 5000):
    rows = _load_journal()
    bad = []
    for r in rows[:limit]:
        if not r.get('ts') or not r.get('symbol') or not r.get('side'):
            bad.append(r.get('txid') or 'unknown')
    return {'ok': True, 'bad': bad, 'checked': min(limit, len(rows))}

@app.get('/fills')
def fills():
    rows = _load_journal()
    has_fill = [r for r in rows if any(k in r for k in ('price','vol','fee','cost','filled_ts','descr','ordertxid'))]
    has_fill = sorted(has_fill, key=lambda x: x.get('ts') or 0, reverse=True)[:200]
    return {'ok': True, 'count': len(has_fill), 'rows': has_fill}

# ------------- P&L -------------
@app.get('/pnl/summary')
def pnl_summary():
    rows = _load_journal()
    return {'ok': True, 'summary': _compute_pnl(rows)}

# ------------- scheduler -------------
@app.post('/scheduler/run')
def scheduler_run(req: SchedulerReq):
    log.info("Scheduler pass: strats=%s tf=%s limit=%s notional=%s dry=%s symbols=%s",
             req.strats, req.tf, req.limit, req.notional, req.dry, req.symbols)
    return {'ok': True, 'echo': req.model_dump()}

# ---------- main ----------
if __name__ == '__main__':
    import uvicorn
    port = int(os.environ.get('PORT', '10000'))
    log.info('Starting %s v%s on 0.0.0.0:%d', APP_NAME, APP_VERSION, port)
    uvicorn.run('app:app', host='0.0.0.0', port=port, reload=False, access_log=True)
