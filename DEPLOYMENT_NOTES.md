# Patch 018 — Economic Balance Truth / De-dup Fix

This drop-in patch builds surgically on Patch 017.

## Main service changes
- separates visibility balances from economic balances
- stops summing duplicate broker inventory across parsed balances and positions API
- uses one selected economic quantity per canonical asset for:
  - /performance
  - /diagnostics/account_truth
  - holdings qualification
  - exit sizing and open-position truth
- adds source agreement details to:
  - /diagnostics/account_truth
  - /diagnostics/holdings_truth

## Selection rule
- canonicalize aliases like BTC/XXBT and USD/ZUSD within each source
- choose a single economic quantity per asset using max(parsed, positions_api) when both exist
- keep both sources visible for diagnostics without double-counting them

## Expected post-deploy checks
- /diagnostics/account_truth
- /performance
- /diagnostics/holdings_truth
- /worker/exit_diagnostics

## Expected truth
- USD no longer doubled
- BTC qty matches live broker holding once
- position notional aligns with adopted plan / live holdings
