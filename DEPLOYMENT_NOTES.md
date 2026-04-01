# patch-043-explicit-journal-lifecycle-provenance-bridge-forced-strategy-rewrite

Changes:
- Add explicit journal↔lifecycle provenance bridge that inspects actual lifecycle tables/columns and resolves strategy by txid bridge first, then symbol/timestamp proximity.
- Add forced rewrite pass for already-journaled adopted rows in both closed_trades and open_trades.
- Preserve journal persistence and P&L truth fixes.
- No scanner behavior changes beyond paired build identity.
