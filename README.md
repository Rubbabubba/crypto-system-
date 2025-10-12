# Crypto Trading Bots — v2.0.0

Broker: **Kraken** (Texas-friendly).  
Runtime: **Python 3.11**.  
Service: **FastAPI + Uvicorn**.  
Strategies: **c1..c6** wired directly (no demo engine).

## Features
- REST API with routes for scanning, prices, bars, orders, positions, and a scheduler.
- Kraken-first execution and market data via `broker_kraken.py`.
- Symbol normalization (`symbol_map.py`) and timeframe adapters.
- Expanded dashboard at `/` with quick actions, live prices, and sparklines.
- Deterministic dependencies (`requirements.txt`) and slim container (`Dockerfile`).

---

## Quick Start (Local)

1) Create your `.env` from the template:
```bash
cp .env.example .env
# edit .env -> set KRAKEN_KEY / KRAKEN_SECRET, turn TRADING_ENABLED/KRAKEN_TRADING to 1 for live
