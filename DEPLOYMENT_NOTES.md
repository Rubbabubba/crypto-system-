# patch-044-backfill-family-specific-provenance-mapping

Changes:
- Add family-specific provenance mapping for backfilled trade families, especially reconciled_fill_backfill rows.
- Resolve strategy using table-specific ranking across lifecycle sources, favoring txid-linked and time-adjacent matches.
- Preserve journal persistence, P&L truth, and existing explicit provenance bridge behavior.
- No scanner behavior changes beyond paired build identity.
