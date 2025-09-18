# app.py
import os
import json
import csv
import time
import threading
import datetime as dt
from collections import defaultdict
from typing import List, Dict, Any

from flask import Flask, request, jsonify, make_response, Response, redirect
from dotenv import load_dotenv

from services.market_crypto import MarketCrypto
from services.exchange_exec import ExchangeExec
import strategies.c1 as strat_c1
import strategies.c2 as strat_c2
import strategies.c3 as strat_c3
import strategies.c4 as strat_c4   # <-- NEW

__app_version__ = "1.5.0"   # add C4 (Volume Bubbles Breakout)

load_dotenv()
app = Flask(__name__)
os.makedirs("logs", exist_ok=True)
os.makedirs("storage", exist_ok=True)

_last_ticks = {"c1": None, "c2": None, "c3": None, "c4": None}

# ----------------- ENV helpers -----------------
def env_map():
    keys = [
        "CRYPTO_EXCHANGE", "CRYPTO_TRADING_BASE_URL", "CRYPTO_DATA_BASE_URL",
        "CRYPTO_SYMBOLS", "DAILY_TARGET", "DAILY_STOP", "ORDER_NOTIONAL",
        # C1
        "C1_TIMEFRAME","C1_RSI_LEN","C1_EMA_LEN","C1_RSI_BUY","C1_RSI_SELL",
        "C1_HTF_TIMEFRAME","C1_CLOSE_ABOVE_EMA_EQ","C1_REGIME","C1_ATR_LEN",
        "C1_ATR_STOP_MULT","C1_ATR_TP_MULT","C1_COOLDOWN_SEC","C1_MAX_SPREAD_PCT",
        "C1_MIN_ATR_USD","C1_MAX_POSITIONS",
        # C2
        "C2_TIMEFRAME","C2_LOOKBACK","C2_ATR_LEN","C2_BREAK_K","C2_MIN_RANGE_PCT",
        "C2_HTF_TIMEFRAME","C2_EMA_TREND_LEN","C2_ATR_STOP_MULT","C2_ATR_TP_MULT",
        "C2_COOLDOWN_SEC","C2_MAX_POSITIONS",
        # C3
        "C3_TIMEFRAME","C3_MA1_TYPE","C3_MA2_TYPE","C3_MA1_LEN","C3_MA2_LEN",
        "C3_ATR_LEN","C3_RISK_M","C3_SWING_LOOKBACK","C3_USE_LIMIT","C3_RR_MULT",
        "C3_TRAIL_ON","C3_TRAIL_ATR_MULT","C3_TRAIL_SOURCE","C3_RR_EXIT_FRAC",
        "C3_ALLOW_SHORTS","C3_COOLDOWN_SEC","C3_MAX_POSITIONS","C3_USE_SESSION",
        "C3_SESSION_IGN_GMT6",
        # C4 (new)
        "C4_TIMEFRAME","C4_BUBBLE_TF","C4_LOOKBACK_BUBBLES","C4_MIN_TOTAL_VOL",
        "C4_MIN_DELTA_FRAC","C4_BREAK_K_ATR","C4_ATR_LEN","C4_ATR_STOP_MULT",
        "C4_USE_LIMIT","C4_RR_MULT","C4_TRAIL_ON","C4_TRAIL_ATR_MULT",
        "C4_RR_EXIT_FRAC","C4_ALLOW_SHORTS","C4_COOLDOWN_SEC","C4_MAX_POSITIONS",
        # inline cron
        "CRON_INLINE","C1_EVERY_SEC","C1_OFFSET_SEC","C2_EVERY_SEC","C2_OFFSET_SEC",
        "C3_EVERY_SEC","C3_OFFSET_SEC","C4_EVERY_SEC","C4_OFFSET_SEC","CRON_DRY",
    ]
    return {k: os.getenv(k) for k in keys}

def get_symbols():
    raw = os.getenv("CRYPTO_SYMBOLS", "BTC/USD,ETH/USD")
    return [s.strip() for s in raw.split(",") if s.strip()]

def _now_utc_ts() -> int: return int(time.time())

def _today_bounds_utc():
    now = dt.datetime.utcnow()
    start = dt.datetime(year=now.year, month=now.month, day=now.day, tzinfo=dt.timezone.utc)
    end = start + dt.timedelta(days=1)
    return int(start.timestamp()), int(end.timestamp())

def pwrite(symbol, system, side, notional, note, dry, extra=None):
    path = "storage/crypto_ledger.csv"
    exists = os.path.exists(path)
    with open(path,"a",newline="") as f:
        w=csv.writer(f)
        if not exists:
            w.writerow(["ts","symbol","system","side","notional_usd","note","dry_run","extra"])
        w.writerow([_now_utc_ts(),symbol,system,side,round(float(notional),2),note or "",int(dry),json.dumps(extra or {})])

def mk_services():
    return MarketCrypto(), ExchangeExec()

def _load_json(path, default):
    try:
        if os.path.exists(path):
            with open(path,"r") as f: return json.load(f)
    except Exception: pass
    return default

# --- tiny indicators for signals snapshots ---
def _ema(v,l):
    if l<=1 or not v: return v[:]
    k=2.0/(l+1.0); out=[]; e=v[0]
    for x in v:
        e=x*k+e*(1-k); out.append(e)
    return out

def _rsi(values, length):
    if length<=1 or len(values)<length+1: return [50.0]*len(values)
    gains=[];losses=[]
    for i in range(1,len(values)):
        ch=values[i]-values[i-1]
        gains.append(max(ch,0.0)); losses.append(max(-ch,0.0))
    ag=sum(gains[:length])/length; al=sum(losses[:length])/length
    rsis=[50.0]*length
    for i in range(length,len(gains)):
        ag=(ag*(length-1)+gains[i])/length
        al=(al*(length-1)+losses[i])/length
        rs=99.0 if al==0 else ag/al
        rsis.append(100-(100/(1+rs)))
    while len(rsis)<len(values): rsis.append(rsis[-1])
    return rsis

def _true_ranges(h,l,c):
    trs=[]; pc=None
    for i in range(len(c)):
        hi,lo,cl=h[i],l[i],c[i]
        tr=hi-lo if pc is None else max(hi-lo,abs(hi-pc),abs(lo-pc))
        trs.append(max(tr,0.0)); pc=cl
    return trs

def _atr(h,l,c,len_):
    trs=_true_ranges(h,l,c)
    return _ema(trs,len_) if len_>1 else trs

# --------------- token gate ---------------
def _require_cron_token_if_set():
    tok=os.getenv("CRON_TOKEN")
    if not tok: return None
    if request.args.get("token") != tok:
        return jsonify({"ok":False,"error":"forbidden"}),403
    return None

# --------------- run wrappers ---------------
def _run_strategy_http(mod, name: str):
    dry = request.args.get("dry","1")=="1"
    force = request.args.get("force","0")=="1"
    symbols = get_symbols()
    params = {**env_map(), "ORDER_NOTIONAL": os.getenv("ORDER_NOTIONAL", os.getenv("ORDER_SIZE",25))}
    market, broker = mk_services()
    try:
        results = mod.run(market, broker, symbols, params, dry=dry, pwrite=pwrite)
        payload={"ok":True,"strategy":name,"dry":dry,"force":force,"results":results}
        status=200
    except Exception as e:
        payload={"ok":False,"strategy":name,"dry":dry,"force":force,"error":str(e)}
        status=502
    resp=make_response(jsonify(payload),status)
    resp.headers["x-strategy-version"]=getattr(mod,"__version__","0")
    resp.headers["x-app-version"]=__app_version__
    return resp

def _run_strategy_direct(mod, name: str, dry: bool):
    _last_ticks[name]=_now_utc_ts()
    symbols=get_symbols()
    params={**env_map(), "ORDER_NOTIONAL": os.getenv("ORDER_NOTIONAL", os.getenv("ORDER_SIZE",25))}
    market, broker = mk_services()
    try:
        mod.run(market, broker, symbols, params, dry=dry, pwrite=pwrite)
    except Exception as e:
        app.logger.exception(f"[{name}] inline run failed: {e}")

# --------------- inline scheduler ---------------
def _start_inline_scheduler():
    enabled=os.getenv("CRON_INLINE","0")=="1"
    if not enabled:
        app.logger.info("Inline scheduler disabled (CRON_INLINE=1 to enable).")
        return
    c1_every=int(os.getenv("C1_EVERY_SEC",300)); c1_offset=int(os.getenv("C1_OFFSET_SEC",0))
    c2_every=int(os.getenv("C2_EVERY_SEC",900)); c2_offset=int(os.getenv("C2_OFFSET_SEC",60))
    c3_every=int(os.getenv("C3_EVERY_SEC",600)); c3_offset=int(os.getenv("C3_OFFSET_SEC",30))
    c4_every=int(os.getenv("C4_EVERY_SEC",600)); c4_offset=int(os.getenv("C4_OFFSET_SEC",45))
    dry=os.getenv("CRON_DRY","0")=="1"

    def runner(every, offset, fn, label):
        def loop():
            if offset>0: time.sleep(offset)
            while True:
                t0=time.time()
                try: fn()
                except Exception as e: app.logger.exception(f"[{label}] {e}")
                sleep_s = max(1, every - int(time.time()-t0))
                time.sleep(sleep_s)
        th=threading.Thread(target=loop,name=f"inline-{label}",daemon=True); th.start()

    app.logger.info({"msg":"Starting inline scheduler",
                     "C1_EVERY_SEC":c1_every,"C2_EVERY_SEC":c2_every,
                     "C3_EVERY_SEC":c3_every,"C4_EVERY_SEC":c4_every,
                     "CRON_DRY":int(dry)})
    runner(c1_every,c1_offset,lambda:_run_strategy_direct(strat_c1,"c1",dry),"c1")
    runner(c2_every,c2_offset,lambda:_run_strategy_direct(strat_c2,"c2",dry),"c2")
    runner(c3_every,c3_offset,lambda:_run_strategy_direct(strat_c3,"c3",dry),"c3")
    runner(c4_every,c4_offset,lambda:_run_strategy_direct(strat_c4,"c4",dry),"c4")

_start_inline_scheduler()

# --------------- health/diag ---------------
@app.get("/health")
def health(): return jsonify({"ok":True,"system":"crypto","symbols":get_symbols()})

@app.get("/health/versions")
def health_versions():
    data={"app":__app_version__,
          "exchange":os.getenv("CRYPTO_EXCHANGE","alpaca"),
          "systems":{"c1":{"version":strat_c1.__version__},
                     "c2":{"version":strat_c2.__version__},
                     "c3":{"version":strat_c3.__version__},
                     "c4":{"version":strat_c4.__version__}}}
    resp=jsonify(data)
    resp.headers["x-app-version"]=__app_version__
    resp.headers["x-c1-version"]=strat_c1.__version__
    resp.headers["x-c2-version"]=strat_c2.__version__
    resp.headers["x-c3-version"]=strat_c3.__version__
    resp.headers["x-c4-version"]=strat_c4.__version__
    return resp

@app.get("/diag/crypto")
def diag_crypto():
    acct=None; ok=True; err=None
    try:
        _, broker = mk_services()
        acct = broker.get_account()
    except Exception as e:
        ok=False; err=str(e)
    resp=jsonify({"ok":ok,"exchange":os.getenv("CRYPTO_EXCHANGE"),
                  "trading_base":os.getenv("CRYPTO_TRADING_BASE_URL"),
                  "data_base":os.getenv("CRYPTO_DATA_BASE_URL"),
                  "api_key_present":bool(os.getenv("CRYPTO_API_KEY")),
                  "symbols":get_symbols(),"account_sample":acct,"error":err})
    resp.headers["x-app-version"]=__app_version__
    return resp

@app.get("/diag/inline")
def diag_inline():
    return jsonify({"enabled":os.getenv("CRON_INLINE","0")=="1",
                    "c1_every_sec":int(os.getenv("C1_EVERY_SEC",300)),
                    "c2_every_sec":int(os.getenv("C2_EVERY_SEC",900)),
                    "c3_every_sec":int(os.getenv("C3_EVERY_SEC",600)),
                    "c4_every_sec":int(os.getenv("C4_EVERY_SEC",600)),
                    "last_ticks":_last_ticks,
                    "dry":os.getenv("CRON_DRY","0")=="1"})

@app.get("/diag/gate")
def diag_gate(): return jsonify({"gate_on":True,"decision":"open","note":"Crypto 24/7"})

@app.get("/diag/clock")
def diag_clock():
    now=dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
    return jsonify({"is_open":True,"now_utc":now,"next_open":None,"next_close":None})

# --------------- PnL & rails ---------------
def _read_ledger_rows():
    p="storage/crypto_ledger.csv"
    if not os.path.exists(p): return []
    rows=[]
    with open(p,"r",newline="") as f:
        r=csv.DictReader(f)
        for row in r:
            if not row or "ts" not in row or not row["ts"]: continue
            try: row["ts"]=int(row["ts"])
            except: continue
            try: row["notional_usd"]=float(row.get("notional_usd",0) or 0)
            except: row["notional_usd"]=0.0
            try: row["dry_run"]=bool(int(row.get("dry_run",0) or 0))
            except: row["dry_run"]=False
            rows.append(row)
    return rows

def _today_summary():
    start_ts,end_ts=_today_bounds_utc()
    by=defaultdict(lambda:{"count":0,"buy_notional":0.0,"flat_count":0,"sell_count":0,"other_count":0})
    tot={"count":0,"buy_notional":0.0,"flat_count":0,"sell_count":0,"other_count":0}
    for row in _read_ledger_rows():
        if not (start_ts<=row["ts"]<end_ts): continue
        if row.get("dry_run"): continue
        sys=row.get("system") or "NA"; side=(row.get("side") or "").upper()
        by[sys]["count"]+=1; tot["count"]+=1
        if side=="BUY":
            by[sys]["buy_notional"]+=row["notional_usd"]; tot["buy_notional"]+=row["notional_usd"]
        elif side in ("SELL","FLAT","EXIT"):
            by[sys]["sell_count"]+=1; tot["sell_count"]+=1
        else:
            by[sys]["other_count"]+=1; tot["other_count"]+=1
    return {"by_system":by,"totals":tot}

@app.get("/pnl/daily")
def pnl_daily():
    summary=_today_summary(); acct=None; positions=None; ok=True; err=None
    try:
        _, broker = mk_services()
        acct=broker.get_account(); positions=broker.get_positions()
    except Exception as e:
        ok=False; err=str(e)
    return jsonify({"ok":ok,"error":err,"app_version":__app_version__,
                    "today_activity":summary,"account":acct,"positions":positions})

@app.get("/health/limits")
def health_limits():
    daily_target=float(os.getenv("DAILY_TARGET", "0") or 0)
    daily_stop=float(os.getenv("DAILY_STOP", "0") or 0)
    summary=_today_summary(); acct=None; ok=True; err=None
    try:
        _, broker = mk_services(); acct=broker.get_account()
    except Exception as e:
        ok=False; err=str(e)
    return jsonify({"ok":ok,"error":err,"app_version":__app_version__,
                    "rails":{"daily_target":daily_target,"daily_stop":daily_stop},
                    "today":summary,
                    "account_equity":acct.get("equity") if isinstance(acct,dict) else None})

# --------------- orders/positions ---------------
def _alpaca_req(method, path, **kw):
    _, broker = mk_services()
    url=f"{broker.base}{path}"
    r=broker.session.request(method.upper(),url,timeout=(5,20),**kw)
    r.raise_for_status()
    try: return r.json()
    except: return {"status": r.status_code}

@app.get("/positions")
def positions():
    try:
        _, broker = mk_services()
        data=broker.get_positions()
        if isinstance(data,dict): data=data.get("positions") or []
        return jsonify(data)
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}),502

@app.get("/orders/recent")
def orders_recent():
    status=request.args.get("status","all"); limit=int(request.args.get("limit","200"))
    params={"limit":str(limit)}
    if status and status!="all": params["status"]=status
    try:
        data=_alpaca_req("GET","/orders",params=params)
        if isinstance(data,dict) and "orders" in data: data=data["orders"]
        return jsonify(data)
    except Exception as e:
        return jsonify({"ok":False,"error":str(e)}),502

# --------------- Signals (C1/C2/C3 existing + C4 new) ---------------
@app.get("/signals")
def signals():
    symbols=get_symbols(); mkt,_=mk_services()
    out_c1=[]; out_c2=[]; out_c3=[]; out_c4=[]

    # ---- C1 snapshot (unchanged) ----
    C1_TIMEFRAME=os.getenv("C1_TIMEFRAME","5Min")
    C1_HTF=os.getenv("C1_HTF_TIMEFRAME","1Hour")
    C1_EMA_LEN=int(os.getenv("C1_EMA_LEN","50"))
    C1_RSI_LEN=int(os.getenv("C1_RSI_LEN","14"))
    C1_RSI_BUY=float(os.getenv("C1_RSI_BUY","42"))
    C1_ALLOW_EQ=os.getenv("C1_CLOSE_ABOVE_EMA_EQ","0")=="1"
    C1_REGIME=os.getenv("C1_REGIME","up").lower()
    C1_ATR_LEN=int(os.getenv("C1_ATR_LEN","14"))
    C1_MIN_ATR_USD=float(os.getenv("C1_MIN_ATR_USD","0.25"))

    # ---- C2 snapshot (unchanged) ----
    C2_TIMEFRAME=os.getenv("C2_TIMEFRAME","5Min")
    C2_LOOKBACK=int(os.getenv("C2_LOOKBACK","20"))
    C2_ATR_LEN=int(os.getenv("C2_ATR_LEN","14"))
    C2_HTF=os.getenv("C2_HTF_TIMEFRAME","1Hour")
    C2_EMA_TREND_LEN=int(os.getenv("C2_EMA_TREND_LEN","100"))
    C2_BREAK_K=float(os.getenv("C2_BREAK_K","1.0"))
    C2_MIN_RANGE_PCT=float(os.getenv("C2_MIN_RANGE_PCT","0.5"))

    # ---- C3 snapshot (unchanged) ----
    C3_TF=os.getenv("C3_TIMEFRAME","5Min")
    C3_MA1_TYPE=os.getenv("C3_MA1_TYPE","EMA")
    C3_MA2_TYPE=os.getenv("C3_MA2_TYPE","EMA")
    C3_MA1_LEN=int(os.getenv("C3_MA1_LEN","21"))
    C3_MA2_LEN=int(os.getenv("C3_MA2_LEN","50"))
    C3_ATR_LEN=int(os.getenv("C3_ATR_LEN","14"))
    C3_RISK_M=float(os.getenv("C3_RISK_M","1.0"))
    C3_SWING_LB=int(os.getenv("C3_SWING_LOOKBACK","5"))
    C3_USE_LIMIT=os.getenv("C3_USE_LIMIT","1")=="1"
    C3_RR=float(os.getenv("C3_RR_MULT","1.0"))

    # ---- C4 snapshot (new) ----
    C4_TF=os.getenv("C4_TIMEFRAME","5Min")
    C4_BTF=os.getenv("C4_BUBBLE_TF","1Hour")
    C4_LB=int(os.getenv("C4_LOOKBACK_BUBBLES","24"))
    C4_MIN_T=float(os.getenv("C4_MIN_TOTAL_VOL","0"))
    C4_MIN_DF=float(os.getenv("C4_MIN_DELTA_FRAC","0.30"))
    C4_ATR_LEN=int(os.getenv("C4_ATR_LEN","14"))
    C4_BREAK_K=float(os.getenv("C4_BREAK_K_ATR","0.0"))

    # helpers
    for sym in symbols:
        # C1
        try:
            b=mkt.get_bars(sym, C1_TIMEFRAME, limit=300) or []
            if len(b)>=60:
                c=[float(x["close"]) for x in b]
                h=[float(x["high"]) for x in b]
                l=[float(x["low"])  for x in b]
                last=c[-1]; e=_ema(c, C1_EMA_LEN)[-1]; r=_rsi(c, C1_RSI_LEN)[-1]; a=_atr(h,l,c,C1_ATR_LEN)[-1]
                htf=mkt.get_bars(sym, C1_HTF, limit=200) or []
                regime_up=True
                if len(htf)>20:
                    hc=[float(x["close"]) for x in htf]
                    he=_ema(hc, 200)[-1] if len(hc)>=200 else _ema(hc, max(20,len(hc)//2))[-1]
                    regime_up = hc[-1] >= he
                ready=True; reason=[]
                if a < C1_MIN_ATR_USD: ready=False; reason.append("ATR low")
                above = last >= e if C1_ALLOW_EQ else last > e
                if not above: ready=False; reason.append("close<EMA")
                if r >= C1_RSI_BUY: ready=False; reason.append(f"RSI≥{C1_RSI_BUY}")
                if C1_REGIME=="up" and not regime_up: ready=False; reason.append("regime down")
                if C1_REGIME=="down" and regime_up: ready=False; reason.append("regime up")
                out_c1.append({"symbol":sym,"close":last,"ema":e,"rsi":r,"atr":a,"regime":"up" if regime_up else "down",
                               "ready":ready,"reason":", ".join(reason) if not ready else ""})
            else:
                out_c1.append({"symbol":sym,"status":"no_data"})
        except Exception as ex:
            out_c1.append({"symbol":sym,"error":str(ex)})

        # C2
        try:
            b=mkt.get_bars(sym,C2_TIMEFRAME,limit=300) or []
            if len(b)>=max(50,C2_LOOKBACK+5):
                c=[float(x["close"]) for x in b]
                h=[float(x["high"]) for x in b]
                l=[float(x["low"])  for x in b]
                last=c[-1]; at=_atr(h,l,c,C2_ATR_LEN)[-1]
                hh=max(h[-C2_LOOKBACK:]); ll=min(l[-C2_LOOKBACK:])
                range_pct=(hh-ll)/max(1e-9,(hh+ll)/2)*100.0
                htf=mkt.get_bars(sym,C2_HTF,limit=200) or []
                trend_up=True
                if len(htf)>=C2_EMA_TREND_LEN:
                    hc=[float(x["close"]) for x in htf]
                    et=_ema(hc,C2_EMA_TREND_LEN)[-1]
                    trend_up = hc[-1] >= et
                thresh = hh + C2_BREAK_K*at
                ready=True; reason=[]
                if not trend_up: ready=False; reason.append("trend down")
                if range_pct < C2_MIN_RANGE_PCT: ready=False; reason.append("range low")
                if last <= thresh: ready=False; reason.append("no_break")
                out_c2.append({"symbol":sym,"close":last,"hh":hh,"ll":ll,"atr":at,"range_pct":round(range_pct,3),
                               "thresh":thresh,"trend_up":trend_up,"ready":ready,"reason":", ".join(reason) if not ready else ""})
            else:
                out_c2.append({"symbol":sym,"status":"no_data"})
        except Exception as ex:
            out_c2.append({"symbol":sym,"error":str(ex)})

        # C3
        try:
            b=mkt.get_bars(sym, C3_TF, limit=300) or []
            need = max(C3_MA1_LEN,C3_MA2_LEN,C3_ATR_LEN,C3_SWING_LB)+5
            if len(b)>=need:
                c=[float(x["close"]) for x in b]
                h=[float(x["high"]) for x in b]
                l=[float(x["low"])  for x in b]
                last=c[-1]
                # quick EMA only here (signals view)
                m1=_ema(c,C3_MA1_LEN)[-1]; m2=_ema(c,C3_MA2_LEN)[-1]
                at=_atr(h,l,c,C3_ATR_LEN)[-1]
                ll=min(l[-C3_SWING_LB:])
                stop=ll - at*float(C3_RISK_M)
                risk=max(1e-9, last - stop)
                target = last + float(C3_RR)*risk if C3_USE_LIMIT else None
                cross_up = ( _ema(c,C3_MA1_LEN)[-2] <= _ema(c,C3_MA2_LEN)[-2]) and (m1 > m2)
                ready=cross_up; reason="" if ready else "no_cross"
                out_c3.append({"symbol":sym,"close":last,"ma1":m1,"ma2":m2,"atr":at,"stop":stop,"target":target,
                               "ready":ready,"reason":reason})
            else:
                out_c3.append({"symbol":sym,"status":"no_data"})
        except Exception as ex:
            out_c3.append({"symbol":sym,"error":str(ex)})

        # C4
        try:
            base=mkt.get_bars(sym, C4_TF, limit=300) or []
            bub =mkt.get_bars(sym, C4_BTF, limit=max(30,C4_LB)) or []
            if len(base)>=max(50,C4_ATR_LEN+5) and len(bub)>=3:
                c=[float(x["close"]) for x in base]
                h=[float(x["high"]) for x in base]
                l=[float(x["low"])  for x in base]
                last=c[-1]; at=_atr(h,l,c,C4_ATR_LEN)[-1]

                # compute last completed bubble totals (simple: use penultimate bubble)
                bb=bub[-2]
                o=float(bb["open"]); hi=float(bb["high"]); lo=float(bb["low"]); cl=float(bb["close"]); vol=float(bb.get("volume",0))
                # split vol (same heuristic)
                barTop = hi - max(o, cl)
                barBot = min(o, cl) - lo
                barRng = hi - lo
                bull   = (cl - o) > 0
                buyRng = barRng if bull else (barTop + barBot)
                sellRng= (barTop + barBot) if bull else barRng
                totalR = barRng + barTop + barBot
                if totalR>0 and vol>0:
                    buy = (buyRng/totalR)*vol; sell=(sellRng/totalR)*vol
                else:
                    buy = sell = 0.0
                total = buy + sell; delta = buy - sell
                frac = abs(delta)/total if total>0 else 0.0
                thresh = hi + C4_BREAK_K*at
                ready = (total>=C4_MIN_T) and (frac>=C4_MIN_DF) and (last>thresh)
                out_c4.append({"symbol":sym,"close":last,"bubble_hi":hi,"bubble_lo":lo,"total":total,"delta_frac":round(frac,3),
                               "atr":at,"thresh":thresh,"ready":ready,
                               "reason":"" if ready else "thresholds/break not met"})
            else:
                out_c4.append({"symbol":sym,"status":"no_data"})
        except Exception as ex:
            out_c4.append({"symbol":sym,"error":str(ex)})

    return jsonify({"symbols":symbols,"c1":out_c1,"c2":out_c2,"c3":out_c3,"c4":out_c4})

# --------------- scan routes ---------------
@app.post("/scan/c1")
def scan_c1():
    guard=_require_cron_token_if_set()
    if guard: return guard
    return _run_strategy_http(strat_c1,"c1")

@app.post("/scan/c2")
def scan_c2():
    guard=_require_cron_token_if_set()
    if guard: return guard
    return _run_strategy_http(strat_c2,"c2")

@app.post("/scan/c3")
def scan_c3():
    guard=_require_cron_token_if_set()
    if guard: return guard
    return _run_strategy_http(strat_c3,"c3")

@app.post("/scan/c4")
def scan_c4():
    guard=_require_cron_token_if_set()
    if guard: return guard
    return _run_strategy_http(strat_c4,"c4")

# --------------- dashboard (C4 added) ---------------
DASHBOARD_HTML = """
<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><title>Crypto Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1" />
<style>
  :root { --bg:#0b0f14; --panel:#121821; --text:#e6edf3; --muted:#8aa0b4; --ok:#2ecc71; --warn:#f1c40f; --err:#e74c3c; --chip:#1b2430; }
  *{box-sizing:border-box} body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial}
  body{background:var(--bg);color:var(--text)}
  header{padding:16px 20px;background:linear-gradient(180deg,#0e131a 0%,#0b0f14 100%);border-bottom:1px solid #1a2330;display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap}
  h1{margin:0;font-size:18px;letter-spacing:.4px;font-weight:600}
  .muted{color:var(--muted)}
  .grid{display:grid;gap:16px;padding:16px;grid-template-columns:repeat(auto-fill,minmax(320px,1fr))}
  .card{background:var(--panel);border:1px solid #1a2330;border-radius:12px;padding:16px}
  .card h2{margin:0 0 12px;font-size:16px;letter-spacing:.3px}
  .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
  .chip{background:var(--chip);border:1px solid #1f2a38;color:var(--text);border-radius:999px;padding:6px 10px;font-size:12px}
  .ok{color:var(--ok)} .warn{color:var(--warn)} .err{color:var(--err)}
  button,.btn{cursor:pointer;background:#162335;color:var(--text);border:1px solid #233248;padding:8px 12px;border-radius:8px;font-size:13px}
  button:hover,.btn:hover{background:#1b2a40}
  table{width:100%;border-collapse:collapse;font-size:13px}
  th,td{padding:8px;border-bottom:1px solid #1a2330;text-align:left}
  th{color:var(--muted);font-weight:500}
  .mono{font-family:ui-monospace,Menlo,Consolas,monospace}
  .right{text-align:right}
  .small{font-size:12px;color:var(--muted)}
</style>
</head>
<body>
<header>
  <div>
    <h1>Crypto Dashboard <span class="small muted mono" id="appVersion"></span></h1>
    <div class="small muted">Live execution is <strong id="gateState">checking…</strong></div>
  </div>
  <div class="row">
    <button onclick="refreshAll()">Refresh</button>
    <a class="btn" href="/health/versions">Versions</a>
    <a class="btn" href="/diag/crypto">Account</a>
    <a class="btn" href="/pnl/daily">P&L</a>
  </div>
</header>

<div class="grid">
  <div class="card">
    <h2>Market & Inline</h2>
    <div id="gateCard" class="small">Loading…</div>
    <div class="row" style="margin-top:8px;">
      <span class="chip">/diag/gate</span>
      <span class="chip">/diag/inline</span>
      <span class="chip">/signals</span>
    </div>
  </div>

  <div class="card">
    <h2>Quick Actions</h2>
    <div class="row">
      <button onclick="triggerScan('C1')">Scan C1</button>
      <button onclick="triggerScan('C2')">Scan C2</button>
      <button onclick="triggerScan('C3')">Scan C3</button>
      <button onclick="triggerScan('C4')">Scan C4</button>
    </div>
    <div class="small muted" id="scanResult" style="margin-top:10px;"></div>
  </div>

  <div class="card" style="grid-column:1 / -1;">
    <h2>Signals</h2>
    <div id="signalsTable">Loading…</div>
  </div>

  <div class="card" style="grid-column:1 / -1;">
    <h2>Recent Orders</h2>
    <div class="row" style="margin-bottom:8px;">
      <button onclick="loadOrders('all')">All</button>
      <button onclick="loadOrders('open')">Open</button>
      <button onclick="loadOrders('closed')">Closed</button>
    </div>
    <div id="ordersTable">Loading…</div>
  </div>

  <div class="card" style="grid-column:1 / -1;">
    <h2>Positions</h2>
    <div id="positionsTable">Loading…</div>
  </div>
</div>

<script>
async function jfetch(u,o={}){const r=await fetch(u,o); if(!r.ok) throw new Error("HTTP "+r.status); const ct=r.headers.get("content-type")||""; return ct.includes("application/json")? r.json(): r.text();}
function esc(s){return (s==null?"":String(s)).replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]))}

async function refreshGate(){
  try{
    const gate=await jfetch('/diag/gate'); const v=await jfetch('/health/versions'); const inline=await jfetch('/diag/inline');
    document.getElementById('appVersion').textContent="v"+esc(v.app);
    const open=(gate.decision||'').toLowerCase()==='open';
    document.getElementById('gateState').textContent=open?'OPEN':(gate.decision||'closed');
    document.getElementById('gateState').className=open?'ok':'warn';
    const lines=[
      `<div><strong>Gate:</strong> ${esc(gate.decision)}</div>`,
      `<div><strong>Inline:</strong> ${inline.enabled?'enabled':'disabled'} · dry=${inline.dry?'1':'0'}</div>`,
      `<div class="small mono">C1:${esc(inline.c1_every_sec)}s · C2:${esc(inline.c2_every_sec)}s · C3:${esc(inline.c3_every_sec)}s · C4:${esc(inline.c4_every_sec)}s</div>`
    ];
    document.getElementById('gateCard').innerHTML=lines.join('');
  }catch(e){document.getElementById('gateCard').innerHTML='<span class="err">Failed</span>';}
}

async function loadSignals(){
  try{
    const s=await jfetch('/signals');
    function tableC1(a){let h='<h3 style="margin:8px 0">C1 — Mean-Revert</h3><table><thead><tr><th>Symbol</th><th class="right">Close</th><th class="right">EMA</th><th class="right">RSI</th><th class="right">ATR</th><th>Regime</th><th>Ready</th><th>Reason</th></tr></thead><tbody>';
      for(const r of a){ if(r.status){h+=`<tr><td colspan="8" class="muted">${esc(r.symbol)} — ${esc(r.status)}</td></tr>`;continue;}
        h+=`<tr><td class="mono">${esc(r.symbol)}</td><td class="right mono">${esc(r.close)}</td><td class="right mono">${esc(r.ema)}</td><td class="right mono">${esc(r.rsi)}</td><td class="right mono">${esc(r.atr)}</td><td>${esc(r.regime||'')}</td><td>${r.ready?'<span class="ok">yes</span>':'no'}</td><td class="small muted">${esc(r.reason||'')}</td></tr>`;}
      return h+'</tbody></table>'; }
    function tableC2(a){let h='<h3 style="margin:16px 0 8px;">C2 — Breakout</h3><table><thead><tr><th>Symbol</th><th class="right">Close</th><th class="right">HH</th><th class="right">ATR</th><th class="right">Thresh</th><th>Trend</th><th>Ready</th><th>Reason</th></tr></thead><tbody>';
      for(const r of a){ if(r.status){h+=`<tr><td colspan="8" class="muted">${esc(r.symbol)} — ${esc(r.status)}</td></tr>`;continue;}
        h+=`<tr><td class="mono">${esc(r.symbol)}</td><td class="right mono">${esc(r.close)}</td><td class="right mono">${esc(r.hh)}</td><td class="right mono">${esc(r.atr)}</td><td class="right mono">${esc(r.thresh)}</td><td>${r.trend_up?'up':'down'}</td><td>${r.ready?'<span class="ok">yes</span>':'no'}</td><td class="small muted">${esc(r.reason||'')}</td></tr>`;}
      return h+'</tbody></table>'; }
    function tableC3(a){let h='<h3 style="margin:16px 0 8px;">C3 — MA Cross + ATR</h3><table><thead><tr><th>Symbol</th><th class="right">Close</th><th class="right">MA1</th><th class="right">MA2</th><th class="right">ATR</th><th class="right">Stop</th><th class="right">Target</th><th>Ready</th><th>Reason</th></tr></thead><tbody>';
      for(const r of a){ if(r.status){h+=`<tr><td colspan="9" class="muted">${esc(r.symbol)} — ${esc(r.status)}</td></tr>`;continue;}
        h+=`<tr><td class="mono">${esc(r.symbol)}</td><td class="right mono">${esc(r.close)}</td><td class="right mono">${esc(r.ma1)}</td><td class="right mono">${esc(r.ma2)}</td><td class="right mono">${esc(r.atr)}</td><td class="right mono">${esc(r.stop)}</td><td class="right mono">${r.target!=null?esc(r.target):'-'}</td><td>${r.ready?'<span class="ok">yes</span>':'no'}</td><td class="small muted">${esc(r.reason||'')}</td></tr>`;}
      return h+'</tbody></table>'; }
    function tableC4(a){let h='<h3 style="margin:16px 0 8px;">C4 — Volume Bubble Breakout</h3><table><thead><tr><th>Symbol</th><th class="right">Close</th><th class="right">Bubble High</th><th class="right">Bubble Low</th><th class="right">Total Vol</th><th class="right">Δ/Total</th><th class="right">ATR</th><th class="right">Thresh</th><th>Ready</th><th>Reason</th></tr></thead><tbody>';
      for(const r of a){ if(r.status){h+=`<tr><td colspan="10" class="muted">${esc(r.symbol)} — ${esc(r.status)}</td></tr>`;continue;}
        h+=`<tr><td class="mono">${esc(r.symbol)}</td><td class="right mono">${esc(r.close)}</td><td class="right mono">${esc(r.bubble_hi)}</td><td class="right mono">${esc(r.bubble_lo)}</td><td class="right mono">${esc(r.total)}</td><td class="right mono">${esc(r.delta_frac)}</td><td class="right mono">${esc(r.atr)}</td><td class="right mono">${esc(r.thresh)}</td><td>${r.ready?'<span class="ok">yes</span>':'no'}</td><td class="small muted">${esc(r.reason||'')}</td></tr>`;}
      return h+'</tbody></table>'; }
    let html=tableC1(s.c1||[])+tableC2(s.c2||[])+tableC3(s.c3||[])+tableC4(s.c4||[]);
    document.getElementById('signalsTable').innerHTML=html;
  }catch(e){document.getElementById('signalsTable').innerHTML='<div class="err">Failed to load signals</div>';}
}

async function loadOrders(status='all'){
  try{
    const rows=await jfetch(`/orders/recent?status=${encodeURIComponent(status)}&limit=200`);
    const arr=Array.isArray(rows)?rows:[];
    if(arr.length===0){document.getElementById('ordersTable').innerHTML='<div class="muted">No orders</div>';return;}
    let html='<table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Qty/Notional</th><th>Type</th><th>Status</th><th class="right">Filled</th></tr></thead><tbody>';
    for(const o of arr.slice(0,200)){
      html+=`<tr><td class="mono small">${esc(o.submitted_at||o.created_at||'')}</td><td class="mono">${esc(o.symbol||'')}</td><td>${esc(o.side||'')}</td><td>${esc(o.qty||o.notional||'')}</td><td>${esc(o.type||'')}</td><td>${esc(o.status||'')}</td><td class="right">${esc(o.filled_qty||'0')}</td></tr>`;
    }
    html+='</tbody></table>'; document.getElementById('ordersTable').innerHTML=html;
  }catch(e){document.getElementById('ordersTable').innerHTML='<div class="err">Failed to load orders</div>';}
}

async function loadPositions(){
  try{
    const rows=await jfetch('/positions'); const arr=Array.isArray(rows)?rows:[];
    if(arr.length===0){document.getElementById('positionsTable').innerHTML='<div class="muted">No positions</div>';return;}
    let html='<table><thead><tr><th>Symbol</th><th>Side</th><th>Qty</th><th class="right">Mkt Value</th><th class="right">Unreal P/L</th></tr></thead><tbody>';
    for(const p of arr){
      html+=`<tr><td class="mono">${esc(p.symbol||'')}</td><td>${esc(p.side||'')}</td><td>${esc(p.qty||p.quantity||'')}</td><td class="right mono">$${esc(p.market_value||'0')}</td><td class="right mono">$${esc(p.unrealized_pl||p.unrealized_plpc||'0')}</td></tr>`;
    }
    html+='</tbody></table>'; document.getElementById('positionsTable').innerHTML=html;
  }catch(e){document.getElementById('positionsTable').innerHTML='<div class="err">Failed to load positions</div>';}
}

async function triggerScan(which){
  try{
    const url= which==='C1'?'/scan/c1?dry=0': which==='C2'?'/scan/c2?dry=0': which==='C3'?'/scan/c3?dry=0':'/scan/c4?dry=0';
    const res=await jfetch(url,{method:'POST'}); document.getElementById('scanResult').textContent=JSON.stringify(res);
    loadOrders('all'); loadPositions();
  }catch(e){document.getElementById('scanResult').textContent='Scan failed';}
}

function refreshAll(){ refreshGate(); loadSignals(); loadOrders('all'); loadPositions(); }
window.addEventListener('load',()=>{ refreshAll(); setInterval(refreshGate,30000); setInterval(loadSignals,60000); });
</script>
</body></html>
"""

@app.get("/dashboard")
def dashboard(): return Response(DASHBOARD_HTML, mimetype="text/html")

@app.get("/")
def index_root(): return redirect("/dashboard", code=302)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
