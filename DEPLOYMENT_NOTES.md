# patch-036-readiness-compatibility-hotfix

Changes:
- Hotfix Patch 035 readiness/compatibility regression caused by local rebinding of `ALLOWED_SYMBOLS`.
- Preserve the contract-safe 7-symbol admitted live set without mutating global allowlist state.
- Restore `/compatibility` and `/ready` endpoint stability.
- Preserve scanner behavior, tc0 + rb1 strategy behavior, lifecycle, exits, journaling, worker routing, and sizing.
