# app.py
import os, json, csv, time
from flask import Flask, request, jsonify, make_response
from dotenv import load_dotenv
from services.market_crypto import MarketCrypto
from services.exchange_exec import ExchangeExec
import strategies.c1 as strat_c1
import strategies.c2 as strat_c2

load_dotenv()

app = Flask(__name__)
os.makedirs("logs", exist_ok=True)
os.makedirs("storage", exist_ok=True)

def env_map():
    return {k: os.getenv(k) for k in [
        "CRYPTO_EXCHANGE","CRYPTO_TRADING_BASE_URL","CRYPTO_DATA_BASE_URL",
        "CRYPTO_SYMBOLS","DAILY_TARGET","DAILY_STOP","ORDER_NOTIONAL",
        "C1_TIMEFRAME","C1_RSI_LEN","C1_EMA_LEN","C1_RSI_BUY","C1_RSI_SELL",
        "C2_TIMEFRAME","C2_LOOKBACK","C2_ATR_LEN","C2_BREAK_K"
    ]}

def get_symbols():
    raw = os.getenv("CRYPTO_SYMBOLS", "BTC/USD,ETH/USD")
    return [s.strip() for s in raw.split(",") if s.strip()]

def pwrite(symbol, system, side, notional, note, dry, extra=None):
    # Append to P&L journal as an execution note (realized P&L occurs once fills are fetched; this logs intent/result).
    path = "storage/crypto_ledger.csv"
    exists = os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["ts","symbol","system","side","notional_usd","note","dry_run","extra"])
        w.writerow([int(time.time()), symbol, system, side, round(float(notional),2), note or "", int(dry), json.dumps(extra or {})])

def mk_services():
    market = MarketCrypto()
    broker = ExchangeExec()
    return market, broker

@app.get("/health/versions")
def health_versions():
    data = {
        "systems": {
            "c1": {"version": strat_c1.__version__},
            "c2": {"version": strat_c2.__version__},
        },
        "exchange": os.getenv("CRYPTO_EXCHANGE", "alpaca"),
    }
    resp = jsonify(data)
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
        # scrub sensitive
        acct.pop("crypto_status", None)  # optional noisy fields
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
        "error": err
    }
    return jsonify(payload)

def _run_strategy(mod, name: str):
    dry = request.args.get("dry", "1") == "1"
    force = request.args.get("force", "0") == "1"
    symbols = get_symbols()
    params = {**env_map(), "ORDER_NOTIONAL": os.getenv("ORDER_NOTIONAL", os.getenv("ORDER_SIZE", 25))}
    market, broker = mk_services()

    # Crypto runs 24/7; no market-hours gate needed.
    results = mod.run(market, broker, symbols, params, dry=dry, pwrite=pwrite)

    resp = make_response(jsonify({"strategy": name, "dry": dry, "force": force, "results": results}))
    resp.headers["x-strategy-version"] = getattr(mod, "__version__", "0")
    return resp

@app.post("/scan/c1")
def scan_c1():
    return _run_strategy(strat_c1, "c1")

@app.post("/scan/c2")
def scan_c2():
    return _run_strategy(strat_c2, "c2")

@app.get("/health")
def health():
    return jsonify({"ok": True, "system": "crypto", "symbols": get_symbols()})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
