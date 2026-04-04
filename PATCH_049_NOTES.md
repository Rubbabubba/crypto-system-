# Patch 049

This patch is intentionally narrow. It does **not** touch lifecycle, journal, reconciliation, or broker state machinery.

## Code changes
- Removed hidden hard floor on `TC0_MIN_ATR_PCT`
- Removed hidden hard floor on `TC0_BREAKOUT_BUFFER_PCT`
- Removed hidden forced minimum on `TC0_MAX_HOLD_SEC`
- Added `ALLOW_SCANNER_NEW_SYMBOLS` support to universe construction
- Added soft-tier profitability gate
- Added ATR-based expected-move multipliers:
  - `RB1_EXPECTED_MOVE_ATR_MULT` default `1.35`
  - `TC0_EXPECTED_MOVE_ATR_MULT` default `1.75`

## Recommended env changes
### main system
- `ALLOW_SCANNER_NEW_SYMBOLS=1`
- `SCANNER_DRIVEN_UNIVERSE=1`
- `FILTER_UNIVERSE_BY_ALLOWED_SYMBOLS=0`
- `SCANNER_SOFT_ALLOW=1`
- `TC0_MIN_ATR_PCT=0.00045`
- `RB1_MIN_ATR_PCT=0.00070`
- `PROFIT_FILTER_MIN_EXPECTED_MOVE_BPS=16`
- `PROFIT_FILTER_MIN_MOVE_TO_COST_MULT=0.30`
- `PROFIT_FILTER_SOFT_PASS_ENABLED=1`
- `PROFIT_FILTER_SOFT_PASS_MIN_EXPECTED_MOVE_BPS=12`
- `PROFIT_FILTER_SOFT_PASS_MIN_MOVE_TO_COST_MULT=0.20`

### scanner
- `BTC_ONLY_ALIGNMENT_ENABLED=0`
- `SCANNER_EMIT_ONLY_SYMBOLS=0`
- clear `SCANNER_FORCE_EMIT_SYMBOLS`
- optionally reduce `MIN_24H_RANGE_PCT` from `0.04` to `0.025`
- optionally reduce `FALLBACK_MIN_24H_RANGE_PCT` from `0.015` to `0.012`
