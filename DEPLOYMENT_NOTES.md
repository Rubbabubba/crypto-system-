# Patch 020 Deployment Notes

This hotfix repairs the Patch 019 lifecycle-policy regression.

## Fixes
- defines `PENDING_EXIT_TTL_SEC` for worker exit flow
- restores `_normalize_plan_lifecycle_policy` and related lifecycle helpers
- prevents `/worker/exit_diagnostics` and `/diagnostics/holdings_truth` from crashing
- preserves Patch 018 economic balance truth and Patch 019 adopted-plan lifecycle intent

## Post-deploy checks
- `/worker/exit_diagnostics`
- `/diagnostics/holdings_truth`
- `/performance`
- `/diagnostics/account_truth`
