# Crypto System - Light (Equities-inspired)

This is the minimal, boring crypto automation service:

- TradingView Alerts -> `/webhook`
- Background worker -> `/worker/exit` every 30s
- Kraken spot via `broker_kraken.market_notional()`
- One position per symbol
- Deterministic bracket exits (stop/take)
- Daily forced flatten at `DAILY_FLATTEN_TIME_UTC` (default 23:55 UTC)

## Env vars (minimum)

### Required
- `WEBHOOK_SECRET`
- `WORKER_SECRET` (recommended)
- `KRAKEN_API_KEY`, `KRAKEN_API_SECRET` (see broker_kraken.py)

### Suggested
- `ALLOWED_SYMBOLS=BTC/USD,ETH/USD,SOL/USD`
- `DEFAULT_NOTIONAL_USD=50`
- `STOP_PCT=0.01`
- `TAKE_PCT=0.02`
- `DAILY_FLATTEN_TIME_UTC=23:55`
- `MAX_TRADES_PER_SYMBOL_PER_DAY=3`

## TradingView alert payload

```json
{
  "secret": "...",
  "symbol": "BTC/USD",
  "side": "buy",
  "strategy": "rb1",
  "signal": "range_breakout_v1",
  "notional_usd": 50
}
```
