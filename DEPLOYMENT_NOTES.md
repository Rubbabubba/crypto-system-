# patch-046-controlled-profitability-filter-recalibration

Changes:
- Recalibrate profitability gate to stop overblocking while preserving fee awareness.
- New defaults:
  - `PROFIT_FILTER_MIN_MOVE_TO_COST_MULT=0.35`
  - `PROFIT_FILTER_MIN_EXPECTED_MOVE_BPS=20.0`
  - `PROFIT_FILTER_USE_LIVE_SPREAD=0`
  - `PROFIT_FILTER_COST_FLOOR_BPS=68.0`
  - `RB1_MAX_DIST_TO_LEVEL_PCT=0.0050`
  - `RB1_MIN_ATR_PCT=0.0015`
- Preserve momentum, ATR, journaling, persistence, and reconciliation behavior.
