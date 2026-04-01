# patch-039-reconciled-strategy-attribution-preservation

Changes:
- Preserve original strategy attribution for reconciled/backfilled closes by resolving strategy provenance from open trade, meta, matching journal rows, and trade plan before falling back to adopted.
- Repair recent reconciled rows wrongly labeled as adopted when strategy provenance exists.
- Preserve all Patch 038 qty truth / exit dedupe / P&L truth behavior.
- No scanner behavior changes.
