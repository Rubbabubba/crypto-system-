# patch-047-profitability-filter-bugfix-and-min-edge-enforcement

Changes:
- Fix profitability filter NameError by removing dependency on undefined globals and using dedicated env-backed fee/slippage inputs.
- Add controlled min-edge enforcement:
  - `PROFIT_FILTER_MIN_MOVE_TO_COST_MULT=0.45`
  - `PROFIT_FILTER_MIN_EXPECTED_MOVE_BPS=28.0`
  - `RB1_MAX_DIST_TO_LEVEL_PCT=0.0045`
  - `RB1_MIN_ATR_PCT=0.0018`
- Add `DISABLE_ADOPTED_STRATEGY` env flag plumbing for legacy/future compatibility without changing current journal behavior.
- Preserve all persistence, reconciliation, journaling, and scanner behavior.
