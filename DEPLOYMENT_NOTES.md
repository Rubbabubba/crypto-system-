# Patch 019 — Adopted Position Lifecycle Control

This drop-in patch builds surgically on Patch 018.

## Main service changes
- gives adopted positions an explicit lifecycle policy with time-exit control
- adds adopted-plan policy normalization so legacy adopted plans with `max_hold_sec = 0` are upgraded in place
- applies adopted lifecycle policy consistently in:
  - `/worker/exit`
  - `/worker/exit_diagnostics`
  - `/diagnostics/holdings_truth`
- surfaces adopted plan origin, policy source, and time-exit eligibility in diagnostics

## Default adopted policy
- `ADOPTED_TIME_EXIT_ENABLED=1`
- `ADOPTED_MAX_HOLD_SEC` defaults to `MAX_HOLD_SEC` when set, otherwise `7200`

## Expected post-deploy checks
- `/worker/exit_diagnostics`
- `/diagnostics/holdings_truth`
- `/performance`

## Expected truth
- adopted positions no longer sit indefinitely with `max_hold_sec = 0`
- diagnostics show adopted origin and lifecycle policy clearly
- time-exit eligibility is visible and actionable
