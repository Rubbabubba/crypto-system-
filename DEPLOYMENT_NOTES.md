# patch-038-reconciled-exit-dedupe-qty-truth-pnl-truth-fix

Changes:
- Fix reconciled exit quantity/cost/fee truth for partial residual closes so tiny residual quantities cannot inherit full-fill cash value.
- Deduplicate reconciled close journaling by symbol + broker exit txid, preferring the pre-existing closed row and suppressing duplicate reconciled inserts.
- Add startup repair pass to correct recent reconciled exit P&L truth and remove duplicate reconciled exit rows from the trade journal.
- Preserve scanner behavior, entry logic, worker loops, lifecycle coordination, sizing mode, and risk gates.
