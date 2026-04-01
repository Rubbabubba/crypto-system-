# patch-042-journal-strategy-rewrite-backfill-rehydration-lifecycle-history

Changes:
- Add journal strategy rewrite pass that rewrites already-journaled open/closed trades from lifecycle history.
- Add unmatched backfill rehydration from broker history to reconstruct missing closes when lifecycle-provenanced strategy can be resolved.
- Preserve Patch 040 journal persistence and Patch 041 lifecycle/intents lookup logic.
- No scanner behavior changes beyond paired build identity.
