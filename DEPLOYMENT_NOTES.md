# patch-048-universe-quality-expansion-adopted-hard-disable-invalid-active-symbols-fix

Changes:
- Controlled universe-quality expansion via env-backed allowlist augmentation.
- `DISABLE_ADOPTED_STRATEGY` now defaults to enabled for live entry suppression.
- Normalize stale `invalid_active_symbols` scan payload reporting when compatibility fallback confirms admissible active symbols.
- Preserve journaling, persistence, reconciliation, sizing, and worker behavior.


## Patch 050
- Removes hidden TC0 hard floors.
- Adds soft-pass profitability tier and ATR-based expected move multipliers.
- Fixes path-B admission so scanner-driven new symbols are not silently re-filtered to the static pilot basket unless explicitly allowlisted.
- Fixes /diagnostics/blocked_trades 500 and adds summary counts.
