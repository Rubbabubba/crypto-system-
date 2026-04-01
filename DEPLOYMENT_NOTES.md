# patch-041-backfill-strategy-mapping-unmatched-sell-recovery

Changes:
- Preserve backfilled strategy attribution using lifecycle trade plans and lifecycle intent tables before falling back to adopted.
- Add unmatched sell recovery by finding nearest prior broker buy when an exact prior buy match is unavailable.
- Preserve Patch 040 journal persistence and lifecycle-backed provenance fixes.
- No scanner behavior changes beyond paired build identity.
