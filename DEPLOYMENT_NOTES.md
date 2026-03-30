# patch-037-rb1-dedupe-repeat-entry-tolerance-tuning

Changes:
- Tune RB1 repeat-entry tolerance by bypassing state signal-id dedupe for RB1 only.
- Preserve lifecycle signal fingerprint dedupe and workflow locking as the active anti-duplication controls for RB1.
- Leave TC0 and all non-RB1 strategies on the existing signal-id dedupe path.
- No changes to sizing, exposure, exits, journaling, worker routing, scanner alignment, or lifecycle state.
