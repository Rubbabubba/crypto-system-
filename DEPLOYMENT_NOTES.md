# patch-040-journal-persistence-lifecycle-strategy-provenance-fix

Changes:
- Force trade journal persistence default to `/var/data/trade_journal.sqlite3` when `TRADE_JOURNAL_DB_PATH` is not provided.
- Fix reconciled strategy provenance lookups to read `trade_plans` from the lifecycle DB, not the journal DB.
- Preserve Patch 038 qty truth / P&L truth and Patch 039 strategy attribution behavior.
- No scanner behavior changes beyond paired build identity.
